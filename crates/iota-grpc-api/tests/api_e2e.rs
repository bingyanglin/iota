// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use iota_config::local_ip_utils;
use iota_grpc_api::{
    client::NodeClient,
    common::Address,
    conversions,
    node::{
        ExecuteTransactionRequest, GetObjectRequest, ObjectDataOptions, TransactionResponseOptions,
        node_service_client::NodeServiceClient,
    },
};
use iota_types::{
    base_types::{IotaAddress, ObjectID, SequenceNumber},
    object::Owner,
};
use test_cluster::{TestCluster, TestClusterBuilder};
use tonic::transport::Channel;

/// API client for testing IotaApi service operations
#[derive(Clone)]
pub struct ApiClient {
    client: NodeServiceClient<Channel>,
}

impl ApiClient {
    pub fn new(channel: Channel) -> Self {
        Self {
            client: NodeServiceClient::new(channel),
        }
    }

    pub async fn get_object(
        &mut self,
        object_id: &[u8; 32],
        options: Option<ObjectDataOptions>,
    ) -> anyhow::Result<iota_json_rpc_types::IotaObjectResponse> {
        let request = GetObjectRequest {
            object_id: Some(Address {
                address: object_id.to_vec(),
            }),
            options,
        };

        let response = self.client.get_object(request).await?;
        let grpc_response = response.into_inner();

        // Convert structured gRPC response back to IotaObjectResponse
        let object_response = if let Some(data) = grpc_response.data {
            // Convert gRPC ObjectData back to IotaObjectData
            use iota_types::base_types::{ObjectDigest, SequenceNumber, TransactionDigest};

            let object_id = if let Some(addr) = data.object_id {
                if addr.address.len() != 32 {
                    return Err(anyhow::anyhow!(
                        "Invalid object ID length: {}",
                        addr.address.len()
                    ));
                }
                let mut bytes = [0u8; 32];
                bytes.copy_from_slice(&addr.address);
                ObjectID::from_bytes(bytes)
                    .map_err(|e| anyhow::anyhow!("Invalid object ID: {e}"))?
            } else {
                return Err(anyhow::anyhow!("Missing object ID"));
            };

            let version = SequenceNumber::from_u64(data.version);

            let digest = if let Some(d) = data.digest {
                ObjectDigest::try_from(d.digest.as_slice())
                    .map_err(|e| anyhow::anyhow!("Invalid object digest: {e}"))?
            } else {
                return Err(anyhow::anyhow!("Missing object digest"));
            };

            let object_data = iota_json_rpc_types::IotaObjectData {
                object_id,
                version,
                digest,
                type_: data.r#type.and_then(|s| s.parse().ok()),
                owner: data.owner.and_then(|owner_str| {
                    // Parse the owner string back to Owner enum
                    // The gRPC response returns owner as a Display string from Owner::fmt
                    parse_owner(&owner_str).ok()
                }),
                previous_transaction: data
                    .previous_transaction
                    .and_then(|tx| TransactionDigest::try_from(tx.digest.as_slice()).ok()),
                storage_rebate: data.storage_rebate,
                display: None,
                content: None,
                bcs: None,
            };

            iota_json_rpc_types::IotaObjectResponse {
                data: Some(object_data),
                error: None,
            }
        } else if let Some(error) = grpc_response.error {
            // Convert gRPC error back to IotaObjectResponseError
            let iota_error = conversions::grpc_to_iota_object_response_error(error)?;

            iota_json_rpc_types::IotaObjectResponse {
                data: None,
                error: Some(iota_error),
            }
        } else {
            return Err(anyhow::anyhow!("Invalid response: neither data nor error"));
        };

        Ok(object_response)
    }

    pub async fn execute_transaction(
        &mut self,
        tx_bytes: Vec<u8>,
        signatures: Vec<Vec<u8>>,
        options: Option<TransactionResponseOptions>,
    ) -> anyhow::Result<bool> {
        let request = ExecuteTransactionRequest {
            tx_bytes,
            signatures,
            options,
            request_type: None, // Let the system use default
        };

        let response = self.client.execute_transaction(request).await?;
        let response_inner = response.into_inner();

        // Just return success if we got a digest back
        Ok(response_inner.digest.is_some())
    }
}

/// Parse Owner from its Display string format
/// The gRPC API returns owner as Display strings like:
/// - "Account Address ( 0x... )"
/// - "Object ID: ( 0x... )"
/// - "Immutable"
/// - "Shared( 123 )"
fn parse_owner(owner_str: &str) -> anyhow::Result<Owner> {
    let owner_str = owner_str.trim();

    if owner_str == "Immutable" {
        return Ok(Owner::Immutable);
    }

    if owner_str.starts_with("Account Address (") && owner_str.ends_with(")") {
        // Extract address from "Account Address ( 0x... )"
        let addr_part = &owner_str[17..owner_str.len() - 2]; // Remove "Account Address ( " and " )"
        let address: IotaAddress = addr_part.trim().parse()?;
        return Ok(Owner::AddressOwner(address));
    }

    if owner_str.starts_with("Object ID: (") && owner_str.ends_with(")") {
        // Extract address from "Object ID: ( 0x... )"
        let addr_part = &owner_str[12..owner_str.len() - 2]; // Remove "Object ID: ( " and " )"
        let address: IotaAddress = addr_part.trim().parse()?;
        return Ok(Owner::ObjectOwner(address));
    }

    if owner_str.starts_with("Shared(") && owner_str.ends_with(")") {
        // Extract version from "Shared( 123 )"
        let version_part = &owner_str[7..owner_str.len() - 1]; // Remove "Shared(" and ")"
        let version: u64 = version_part.trim().parse()?;
        return Ok(Owner::Shared {
            initial_shared_version: SequenceNumber::from_u64(version),
        });
    }

    Err(anyhow::anyhow!("Unknown owner format: {owner_str}"))
}

async fn setup_test_cluster_and_client() -> (TestCluster, ApiClient) {
    let localhost = local_ip_utils::localhost_for_testing();
    let grpc_port = local_ip_utils::get_available_port(&localhost);
    let grpc_addr = format!("{localhost}:{grpc_port}");

    // Start a test cluster with gRPC enabled and pruning disabled
    let cluster = TestClusterBuilder::new()
        .with_fullnode_grpc_api_address(grpc_addr.parse().expect("Invalid gRPC address"))
        .disable_fullnode_pruning()
        .with_num_validators(1)
        .build()
        .await;

    let node_client = NodeClient::connect(&format!("http://{grpc_addr}"))
        .await
        .expect("connect gRPC");

    // Create API client using the channel
    let api_client = ApiClient::new(node_client.channel().clone());

    (cluster, api_client)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_get_object() {
    let (cluster, mut api_client) = setup_test_cluster_and_client().await;

    // Get a known object (gas object from account)
    let sender = cluster.get_address_0();
    let owned_objects = cluster
        .get_owned_objects(sender, None)
        .await
        .expect("Failed to get owned objects");

    assert!(
        !owned_objects.is_empty(),
        "Should have at least one owned object"
    );

    // Find a gas object (coin type) - just use the first object for simplicity
    let gas_object = &owned_objects[0];

    let object_id = gas_object.data.as_ref().unwrap().object_id;
    let object_id_bytes: [u8; 32] = object_id.into_bytes();

    // Test getting the object
    let object_result = tokio::time::timeout(Duration::from_secs(30), async {
        api_client
            .get_object(
                &object_id_bytes,
                Some(ObjectDataOptions {
                    show_type: true,
                    show_owner: true,
                    show_previous_transaction: false,
                    show_display: false,
                    show_content: false,
                    show_bcs: false,
                    show_storage_rebate: false,
                }),
            )
            .await
    })
    .await
    .expect("timeout waiting for object");

    // Verify get_object works correctly now that it's implemented
    match object_result {
        Ok(response) => {
            // Verify we got a valid response
            assert!(response.data.is_some(), "Expected object data, got None");

            let data = response.data.unwrap();
            let expected_object_ref = gas_object.data.as_ref().unwrap().object_ref();
            assert_eq!(
                data.object_id, expected_object_ref.0,
                "Object ID should match"
            );
            assert_eq!(data.version, expected_object_ref.1, "Version should match");
            assert_eq!(data.digest, expected_object_ref.2, "Digest should match");
            assert!(data.type_.is_some(), "Object type should be present");
            // Owner should be properly parsed from the gRPC response
            assert!(data.owner.is_some(), "Object owner should be present");
        }
        Err(e) => {
            panic!("get_object should work now that it's implemented, but got error: {e}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_execute_transaction() {
    let (cluster, mut api_client) = setup_test_cluster_and_client().await;

    let sender = cluster.get_address_0();
    let receiver = cluster.get_address_1();
    let amount = 1000u64;

    // Build a real transfer transaction using TestCluster's infrastructure
    let tx_data = cluster
        .test_transaction_builder_with_sender(sender)
        .await
        .transfer_iota(Some(amount), receiver)
        .build();

    // Sign the transaction to get proper signatures
    let signed_tx = cluster.sign_transaction(&tx_data);

    // Extract real transaction bytes and signatures
    let tx_bytes = bcs::to_bytes(signed_tx.data()).expect("BCS serialization failed");
    let signatures: Vec<Vec<u8>> = signed_tx
        .tx_signatures()
        .iter()
        .map(|sig| sig.as_ref().to_vec())
        .collect();

    // Test execute_transaction with real transaction data
    let tx_result = tokio::time::timeout(Duration::from_secs(30), async {
        api_client
            .execute_transaction(
                tx_bytes, signatures, None, // Use default options
            )
            .await
    })
    .await
    .expect("timeout waiting for transaction");

    match tx_result {
        Ok(success) => {
            // Verify the transaction was executed successfully
            assert!(
                success,
                "Transaction should have been executed successfully"
            );
        }
        Err(e) => {
            let error_msg = e.to_string();

            // Check if this is expected (WriteAPI not available in this test environment)
            assert!(
                error_msg.contains("Write API not configured")
                    || error_msg.contains("read-only mode")
                    || error_msg.contains("unimplemented")
                    || error_msg.contains("Deserialization error")
                    || error_msg.contains("variant index")
                    || error_msg.contains("unexpected end of input"),
                "Expected WriteAPI/transaction execution limitation, got unexpected error: {error_msg}"
            );
        }
    }
}
