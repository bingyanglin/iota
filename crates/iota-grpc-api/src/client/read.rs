// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use iota_types::{
    base_types::{IotaAddress, ObjectDigest, ObjectID, SequenceNumber, TransactionDigest},
    object::{Data, Object, ObjectInner, ObjectRead, Owner},
};
use move_core_types::annotated_value::MoveStructLayout;
use tonic::{Status, transport::Channel};

use crate::{
    common::{
        Address, Digest, ObjectRef as ProtoObjectRef, TransactionResponse,
        TransactionResponseOptions,
    },
    read::{
        ContainsTransactionRequest, GetObjectRequest, GetTransactionRequest, exists,
        get_object_response, read_service_client::ReadServiceClient,
    },
};

/// Dedicated client for read-related gRPC operations.
#[derive(Clone)]
pub struct ReadClient {
    client: ReadServiceClient<Channel>,
}

impl ReadClient {
    /// Create a new ReadClient from a shared gRPC channel.
    pub(super) fn new(channel: Channel) -> Self {
        Self {
            client: ReadServiceClient::new(channel),
        }
    }

    /// Get an object by ID.
    ///
    /// # Arguments
    /// * `object_id` - The object ID to retrieve
    ///
    /// # Returns
    /// Result containing ObjectRead (Exists/NotExists/Deleted)
    pub async fn get_object(&mut self, object_id: ObjectID) -> Result<ObjectRead, Status> {
        // Convert to gRPC request format
        let grpc_request = GetObjectRequest {
            object_id: Some(Address {
                address: object_id.into_bytes().to_vec(),
            }),
        };

        // Make gRPC call
        let response = self.client.get_object(grpc_request).await?;
        let grpc_response = response.into_inner();

        // Convert proto response to ObjectRead
        let result = grpc_response
            .result
            .ok_or_else(|| Status::internal("Missing result in response"))?;

        convert_proto_to_object_read(result)
    }

    /// Check if a transaction exists.
    ///
    /// # Arguments
    /// * `digest` - The transaction digest to check
    ///
    /// # Returns
    /// Result containing whether the transaction exists
    pub async fn contains_transaction(
        &mut self,
        digest: TransactionDigest,
    ) -> Result<bool, Status> {
        let grpc_request = ContainsTransactionRequest {
            digest: Some(Digest {
                digest: digest.into_inner().to_vec(),
            }),
        };

        let response = self.client.contains_transaction(grpc_request).await?;
        Ok(response.into_inner().exists)
    }

    /// Get a transaction by digest.
    ///
    /// # Arguments
    /// * `digest` - The transaction digest to retrieve
    /// * `options` - Options for what data to include in the response
    ///
    /// # Returns
    /// Result containing the transaction response
    pub async fn get_transaction(
        &mut self,
        digest: TransactionDigest,
        options: Option<TransactionResponseOptions>,
    ) -> Result<TransactionResponse, Status> {
        let grpc_request = GetTransactionRequest {
            digest: Some(Digest {
                digest: digest.into_inner().to_vec(),
            }),
            options,
        };

        let response = self.client.get_transaction(grpc_request).await?;
        Ok(response.into_inner())
    }
}

/// Convert proto response to ObjectRead
fn convert_proto_to_object_read(result: get_object_response::Result) -> Result<ObjectRead, Status> {
    match result {
        get_object_response::Result::Exists(exists) => {
            // Parse ObjectRef
            let object_ref = parse_object_ref(&exists.object_ref, "object_ref")?;

            // Parse Object from flattened fields
            let object = convert_flattened_proto_to_object(&exists)?;

            // Parse layout
            let layout = exists
                .move_structure_layout
                .as_ref()
                .map(|bcs_data| {
                    bcs::from_bytes::<MoveStructLayout>(&bcs_data.data).map_err(|e| {
                        Status::internal(format!("Failed to deserialize layout from BCS: {e}"))
                    })
                })
                .transpose()?;

            Ok(ObjectRead::Exists(object_ref, object, layout))
        }
        get_object_response::Result::NotExists(not_exists) => {
            let object_id = parse_object_id(&not_exists.object_id, "object_id")?;
            Ok(ObjectRead::NotExists(object_id))
        }
        get_object_response::Result::Deleted(deleted) => {
            let object_ref = parse_object_ref(&deleted.object_ref, "object_ref")?;
            Ok(ObjectRead::Deleted(object_ref))
        }
    }
}

/// Convert flattened proto Exists message to core Object
fn convert_flattened_proto_to_object(exists: &crate::read::Exists) -> Result<Object, Status> {
    // Parse data from BCS
    let data_bcs = exists
        .data
        .as_ref()
        .ok_or_else(|| Status::internal("Missing data in response"))?;
    let data = bcs::from_bytes::<Data>(&data_bcs.data)
        .map_err(|e| Status::internal(format!("Failed to deserialize data from BCS: {e}")))?;

    // Parse owner from oneof
    let owner = convert_flattened_owner(&exists.owner)?;

    // Parse previous_transaction
    let previous_transaction =
        parse_transaction_digest(&exists.previous_transaction, "previous_transaction")?;

    Ok(Object::from(ObjectInner {
        data,
        owner,
        previous_transaction,
        storage_rebate: exists.storage_rebate,
    }))
}

/// Convert flattened proto owner oneof to core Owner
fn convert_flattened_owner(proto_owner: &Option<exists::Owner>) -> Result<Owner, Status> {
    let owner_oneof = proto_owner
        .as_ref()
        .ok_or_else(|| Status::internal("Missing owner in response"))?;

    match owner_oneof {
        exists::Owner::AddressOwner(addr) => {
            let address = parse_iota_address(&Some(addr.clone()), "address_owner")?;
            Ok(Owner::AddressOwner(address))
        }
        exists::Owner::ObjectOwner(addr) => {
            let address = parse_iota_address(&Some(addr.clone()), "object_owner")?;
            Ok(Owner::ObjectOwner(address))
        }
        exists::Owner::Shared(shared) => Ok(Owner::Shared {
            initial_shared_version: SequenceNumber::from_u64(shared.initial_shared_version),
        }),
        exists::Owner::Immutable(_) => Ok(Owner::Immutable),
    }
}

// Helper functions
fn parse_object_id(address: &Option<Address>, field_name: &str) -> Result<ObjectID, Status> {
    let address = address
        .as_ref()
        .ok_or_else(|| Status::internal(format!("Missing {field_name}")))?;

    if address.address.len() != 32 {
        return Err(Status::internal(format!(
            "{field_name} must be 32 bytes, got {}",
            address.address.len()
        )));
    }

    ObjectID::from_bytes(&address.address)
        .map_err(|e| Status::internal(format!("Invalid {field_name}: {e}")))
}

/// Parse ObjectRef from proto
fn parse_object_ref(
    proto_ref: &Option<ProtoObjectRef>,
    field_name: &str,
) -> Result<(ObjectID, SequenceNumber, ObjectDigest), Status> {
    let proto_ref = proto_ref
        .as_ref()
        .ok_or_else(|| Status::internal(format!("Missing {field_name}")))?;

    let object_id = parse_object_id(&proto_ref.object_id, "object_id")?;
    let version = SequenceNumber::from_u64(proto_ref.version);
    let digest = parse_digest(&proto_ref.digest, "digest")?;

    Ok((object_id, version, digest))
}

fn parse_digest(digest: &Option<Digest>, field_name: &str) -> Result<ObjectDigest, Status> {
    let digest = digest
        .as_ref()
        .ok_or_else(|| Status::internal(format!("Missing {field_name}")))?;

    ObjectDigest::try_from(digest.digest.as_slice())
        .map_err(|e| Status::internal(format!("Invalid {field_name}: {e}")))
}

fn parse_transaction_digest(
    digest: &Option<Digest>,
    field_name: &str,
) -> Result<TransactionDigest, Status> {
    let digest = digest
        .as_ref()
        .ok_or_else(|| Status::internal(format!("Missing {field_name}")))?;

    TransactionDigest::try_from(digest.digest.as_slice())
        .map_err(|e| Status::internal(format!("Invalid {field_name}: {e}")))
}

fn parse_iota_address(address: &Option<Address>, field_name: &str) -> Result<IotaAddress, Status> {
    let address = address
        .as_ref()
        .ok_or_else(|| Status::internal(format!("Missing {field_name}")))?;

    IotaAddress::from_bytes(&address.address)
        .map_err(|e| Status::internal(format!("Invalid {field_name}: {e}")))
}
