// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use iota_config::local_ip_utils;
use iota_grpc_api::{
    client::NodeClient,
    common::Address,
    conversions,
    read::{GetObjectRequest, ObjectDataOptions},
};
use iota_types::{base_types::ObjectID, error::IotaObjectResponseError};
use test_cluster::{TestCluster, TestClusterBuilder};

async fn setup_test_cluster_and_client() -> (TestCluster, NodeClient) {
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

    // Create NodeClient
    let node_client = NodeClient::connect(&format!("http://{grpc_addr}"))
        .await
        .expect("connect gRPC");

    (cluster, node_client)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_read_service_get_object() {
    let (cluster, node_client) = setup_test_cluster_and_client().await;

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

    // Test getting the object via ReadService
    let mut read_client = node_client
        .read_client()
        .expect("Read client should be available");

    let request = GetObjectRequest {
        object_id: Some(Address {
            address: object_id_bytes.to_vec(),
        }),
        options: Some(ObjectDataOptions {
            show_type: true,
            show_owner: true,
            show_previous_transaction: false,
            show_display: false,
            show_content: false,
            show_bcs: false,
            show_storage_rebate: false,
        }),
    };

    let grpc_response =
        tokio::time::timeout(Duration::from_secs(30), read_client.get_object(request))
            .await
            .expect("timeout waiting for object")
            .expect("ReadService get_object should work");

    // Verify ReadService get_object works correctly - test gRPC response directly
    assert!(
        grpc_response.data.is_some(),
        "Expected object data, got None"
    );
    assert!(
        grpc_response.error.is_none(),
        "Expected no error, got error"
    );

    let data = grpc_response.data.unwrap();
    let expected_object_ref = gas_object.data.as_ref().unwrap().object_ref();

    // Verify object ID matches using existing conversion utility
    let actual_object_id = conversions::grpc_address_to_object_id(data.object_id.clone())
        .expect("Should be able to convert gRPC address to ObjectID");
    assert_eq!(
        actual_object_id, expected_object_ref.0,
        "Object ID should match"
    );
    assert_eq!(
        data.version,
        expected_object_ref.1.value(),
        "Version should match"
    );
    assert!(data.object_type.is_some(), "Object type should be present");
    assert!(data.owner.is_some(), "Object owner should be present");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_read_service_nonexistent_object() {
    let (_cluster, node_client) = setup_test_cluster_and_client().await;

    // Use a dummy object ID that doesn't exist
    let nonexistent_object_id = [0u8; 32];

    // Test getting a non-existent object
    let mut read_client = node_client
        .read_client()
        .expect("Read client should be available");

    let request = GetObjectRequest {
        object_id: Some(Address {
            address: nonexistent_object_id.to_vec(),
        }),
        options: Some(ObjectDataOptions {
            show_type: true,
            show_owner: true,
            show_previous_transaction: false,
            show_display: false,
            show_content: false,
            show_bcs: false,
            show_storage_rebate: false,
        }),
    };

    let grpc_response =
        tokio::time::timeout(Duration::from_secs(30), read_client.get_object(request))
            .await
            .expect("timeout waiting for object")
            .expect("ReadService get_object should work");

    // Verify ReadService returns proper error for non-existent object - test gRPC
    // response directly
    assert!(
        grpc_response.data.is_none(),
        "Expected no data for non-existent object"
    );
    assert!(
        grpc_response.error.is_some(),
        "Expected error for non-existent object"
    );

    // Convert gRPC error back to verify it's the correct type using existing
    // conversion
    let iota_error = conversions::grpc_to_iota_object_response_error(grpc_response.error.unwrap())
        .expect("Should be able to convert gRPC error");

    match iota_error {
        IotaObjectResponseError::NotExists { object_id } => {
            let expected_object_id = ObjectID::from_bytes(nonexistent_object_id).unwrap();
            assert_eq!(
                object_id, expected_object_id,
                "Error object ID should match"
            );
        }
        other_error => {
            panic!("Expected NotExists error, got: {other_error:?}");
        }
    }
}
