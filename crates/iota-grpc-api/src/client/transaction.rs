// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use anyhow::anyhow;
use futures::{Stream, StreamExt};
use iota_json_rpc_types::IotaTransactionBlockEffects;
use iota_types::{
    base_types::{IotaAddress, ObjectID, TransactionDigest},
    digests::ObjectDigest,
};
use serde_json::{Value as JsonValue, json};
use tonic::transport::Channel;

use crate::transactions::{
    Transaction, TransactionStreamRequest, transaction_service_client::TransactionServiceClient,
};

/// Dedicated client for transaction-related gRPC operations.
///
/// This client handles all transaction service interactions including streaming
/// transactions with filtering capabilities.
#[derive(Clone)]
pub struct TransactionClient {
    client: TransactionServiceClient<Channel>,
}

impl TransactionClient {
    /// Create a new TransactionClient from a shared gRPC channel.
    pub(super) fn new(channel: Channel) -> Self {
        Self {
            client: TransactionServiceClient::new(channel),
        }
    }

    /// Stream transaction effects with automatic BCS deserialization and
    /// filtering.
    ///
    /// # Arguments
    /// * `filter` - Transaction filter to apply to the stream
    ///
    /// # Returns
    /// A stream of IOTA transaction effects that match the specified filter
    pub async fn stream_transactions(
        &mut self,
        filter: crate::transactions::TransactionFilter,
    ) -> Result<impl Stream<Item = Result<IotaTransactionBlockEffects, tonic::Status>>, tonic::Status>
    {
        let request = TransactionStreamRequest {
            filter: Some(filter),
        };
        let response = self.client.stream_transactions(request).await?;
        let stream = response.into_inner();

        // Transform the stream to deserialize Transaction protobuf messages
        // into native IotaTransactionBlockEffects
        Ok(stream.map(|transaction_result| {
            transaction_result.and_then(|transaction| {
                Self::deserialize_transaction(&transaction).map_err(|e| {
                    tonic::Status::internal(format!("Failed to deserialize transaction: {e}"))
                })
            })
        }))
    }

    /// Deserialize versioned transaction effects from structured proto fields
    /// using JSON.
    fn deserialize_transaction(tx: &Transaction) -> anyhow::Result<IotaTransactionBlockEffects> {
        match tx.message_version.as_str() {
            "v1" | "" => {
                // Empty string for backward compatibility during transition
                if let Some(crate::transactions::transaction::Effects::V1(v1_data)) = &tx.effects {
                    let json_effects = Self::v1_proto_to_json(v1_data)?;
                    serde_json::from_value(json_effects)
                        .map_err(|e| anyhow!("Failed to deserialize V1 effects from JSON: {e}"))
                } else {
                    Err(anyhow!("Missing V1 effects data"))
                }
            }
            // Future versions:
            // "v2" => { ... }
            version => Err(anyhow!("Unsupported message version: {}", version)),
        }
    }

    /// Convert V1 proto Transaction to JSON matching
    /// IotaTransactionBlockEffectsV1 structure
    fn v1_proto_to_json(
        tx: &crate::transactions::TransactionEffectsV1,
    ) -> anyhow::Result<JsonValue> {
        // Extract and convert transaction digest
        let digest = tx
            .transaction_digest
            .as_ref()
            .ok_or_else(|| anyhow!("Missing transaction digest"))?;
        let transaction_digest = TransactionDigest::new(
            digest
                .digest
                .clone()
                .try_into()
                .map_err(|_| anyhow!("Invalid transaction digest length"))?,
        )
        .to_string();

        // Extract and convert execution status
        let status = tx
            .status
            .as_ref()
            .ok_or_else(|| anyhow!("Missing transaction status"))?;

        let status_json = match &status.status {
            Some(crate::transactions::execution_status::Status::Success(_)) => {
                json!({ "status": "success" })
            }
            Some(crate::transactions::execution_status::Status::Failure(failure)) => {
                json!({ "status": "failure", "error": failure.error })
            }
            None => return Err(anyhow!("Missing execution status variant")),
        };

        // Extract and convert gas cost summary
        let gas_used = tx
            .gas_used
            .as_ref()
            .ok_or_else(|| anyhow!("Missing gas cost summary"))?;

        let gas_used_json = json!({
            "computationCost": gas_used.computation_cost.to_string(),
            "computationCostBurned": gas_used.computation_cost_burned.to_string(),
            "storageCost": gas_used.storage_cost.to_string(),
            "storageRebate": gas_used.storage_rebate.to_string(),
            "nonRefundableStorageFee": gas_used.non_refundable_storage_fee.to_string()
        });

        // Convert modified_at_versions
        let modified_at_versions = tx
            .modified_at_versions
            .iter()
            .map(|mv| -> anyhow::Result<JsonValue> {
                let object_id = mv
                    .object_id
                    .as_ref()
                    .ok_or_else(|| anyhow!("Missing object ID in modified_at_versions"))?;

                let bytes: [u8; 32] = object_id
                    .address
                    .clone()
                    .try_into()
                    .map_err(|_| anyhow!("Invalid object ID length"))?;
                let object_id = ObjectID::from_bytes(bytes)?;
                Ok(json!({
                    "objectId": object_id.to_string(),
                    "sequenceNumber": mv.sequence_number.to_string()
                }))
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Convert shared objects
        let shared_objects = tx
            .shared_objects
            .iter()
            .map(Self::object_ref_to_json)
            .collect::<Result<Vec<_>, _>>()?;

        // Convert created objects
        let created = tx
            .created
            .iter()
            .map(Self::owned_object_ref_to_json)
            .collect::<Result<Vec<_>, _>>()?;

        // Convert mutated objects
        let mutated = tx
            .mutated
            .iter()
            .map(Self::owned_object_ref_to_json)
            .collect::<Result<Vec<_>, _>>()?;

        // Convert unwrapped objects
        let unwrapped = tx
            .unwrapped
            .iter()
            .map(Self::owned_object_ref_to_json)
            .collect::<Result<Vec<_>, _>>()?;

        // Convert deleted objects
        let deleted = tx
            .deleted
            .iter()
            .map(Self::object_ref_to_json)
            .collect::<Result<Vec<_>, _>>()?;

        // Convert unwrapped_then_deleted objects
        let unwrapped_then_deleted = tx
            .unwrapped_then_deleted
            .iter()
            .map(Self::object_ref_to_json)
            .collect::<Result<Vec<_>, _>>()?;

        // Convert wrapped objects
        let wrapped = tx
            .wrapped
            .iter()
            .map(Self::object_ref_to_json)
            .collect::<Result<Vec<_>, _>>()?;

        // Convert gas object
        let gas_object = tx
            .gas_object
            .as_ref()
            .ok_or_else(|| anyhow!("Missing gas object"))?;
        let gas_object_json = Self::owned_object_ref_to_json(gas_object)?;

        // Convert events digest (optional)
        let events_digest = tx
            .events_digest
            .as_ref()
            .map(|digest| -> anyhow::Result<String> {
                // Format as hex manually since TransactionEventsDigest doesn't implement
                // Display
                let hex_string = digest
                    .digest
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>();
                Ok(format!("0x{hex_string}"))
            })
            .transpose()?;

        // Convert dependencies
        let dependencies = tx
            .dependencies
            .iter()
            .map(|digest| -> anyhow::Result<String> {
                let transaction_digest = TransactionDigest::new(
                    digest
                        .digest
                        .clone()
                        .try_into()
                        .map_err(|_| anyhow!("Invalid dependency digest length"))?,
                );
                Ok(transaction_digest.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Build the complete JSON object matching IotaTransactionBlockEffectsV1
        // structure
        Ok(json!({
            "messageVersion": "v1",
            "status": status_json,
            "executedEpoch": tx.executed_epoch.to_string(),
            "gasUsed": gas_used_json,
            "modifiedAtVersions": modified_at_versions,
            "sharedObjects": shared_objects,
            "transactionDigest": transaction_digest,
            "created": created,
            "mutated": mutated,
            "unwrapped": unwrapped,
            "deleted": deleted,
            "unwrappedThenDeleted": unwrapped_then_deleted,
            "wrapped": wrapped,
            "gasObject": gas_object_json,
            "eventsDigest": events_digest,
            "dependencies": dependencies
        }))
    }

    /// Convert proto ObjectRef to JSON
    fn object_ref_to_json(obj_ref: &crate::common::ObjectRef) -> anyhow::Result<JsonValue> {
        let object_id = obj_ref
            .object_id
            .as_ref()
            .ok_or_else(|| anyhow!("Missing object ID"))?;
        let digest = obj_ref
            .digest
            .as_ref()
            .ok_or_else(|| anyhow!("Missing object digest"))?;

        let bytes: [u8; 32] = object_id
            .address
            .clone()
            .try_into()
            .map_err(|_| anyhow!("Invalid object ID length"))?;
        let object_id = ObjectID::from_bytes(bytes)?;
        let object_digest = ObjectDigest::try_from(digest.digest.as_slice())
            .map_err(|e| anyhow!("Invalid object digest: {e}"))?;

        Ok(json!({
            "objectId": object_id.to_string(),
            "version": obj_ref.version,
            "digest": object_digest.to_string()
        }))
    }

    /// Convert proto OwnedObjectRef to JSON
    fn owned_object_ref_to_json(
        owned_ref: &crate::transactions::OwnedObjectRef,
    ) -> anyhow::Result<JsonValue> {
        let reference = owned_ref
            .reference
            .as_ref()
            .ok_or_else(|| anyhow!("Missing object reference"))?;
        let owner = owned_ref
            .owner
            .as_ref()
            .ok_or_else(|| anyhow!("Missing owner"))?;

        let reference_json = Self::object_ref_to_json(reference)?;
        let owner_json = Self::owner_to_json(owner)?;

        Ok(json!({
            "owner": owner_json,
            "reference": reference_json
        }))
    }

    /// Convert proto Owner to JSON
    fn owner_to_json(owner: &crate::transactions::Owner) -> anyhow::Result<JsonValue> {
        match &owner.owner {
            Some(crate::transactions::owner::Owner::AddressOwner(addr)) => {
                // address_owner directly contains a common.Address
                let bytes: [u8; 32] = addr
                    .address
                    .clone()
                    .try_into()
                    .map_err(|_| anyhow!("Invalid address length"))?;
                let iota_address = IotaAddress::from_bytes(bytes)?;
                Ok(json!({
                    "AddressOwner": iota_address.to_string()
                }))
            }
            Some(crate::transactions::owner::Owner::ObjectOwner(obj_owner)) => {
                // object_owner contains an ObjectOwner message with an address field
                let address = obj_owner
                    .address
                    .as_ref()
                    .ok_or_else(|| anyhow!("Missing object owner address"))?;
                let bytes: [u8; 32] = address
                    .address
                    .clone()
                    .try_into()
                    .map_err(|_| anyhow!("Invalid object owner address length"))?;
                let object_id = ObjectID::from_bytes(bytes)?;
                Ok(json!({
                    "ObjectOwner": object_id.to_string()
                }))
            }
            Some(crate::transactions::owner::Owner::Shared(shared)) => Ok(json!({
                "Shared": {
                    "initial_shared_version": shared.initial_shared_version
                }
            })),
            Some(crate::transactions::owner::Owner::Immutable(_)) => Ok(json!("Immutable")),
            None => Err(anyhow!("Missing owner variant")),
        }
    }
}
