// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use iota_grpc_api::client::ReadClient;
use iota_json_rpc_types::IotaObjectDataOptions;
use iota_types::{base_types::ObjectID, error::IotaObjectResponseError};
use test_cluster::TestCluster;

mod utils;
use utils::setup_test_cluster_and_client;

async fn setup_test_cluster_and_read_client() -> (TestCluster, ReadClient) {
    let (cluster, node_client) = setup_test_cluster_and_client().await;

    let read_client = node_client
        .read_client()
        .expect("Read client should be available");

    (cluster, read_client)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_read_service_get_object() {
    let (cluster, mut read_client) = setup_test_cluster_and_read_client().await;

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

    let options = Some(IotaObjectDataOptions {
        show_type: true,
        show_owner: true,
        show_previous_transaction: false,
        show_display: false,
        show_content: false,
        show_bcs: false,
        show_storage_rebate: false,
    });

    let response = tokio::time::timeout(
        Duration::from_secs(30),
        read_client.get_object(object_id, options),
    )
    .await
    .expect("timeout waiting for object")
    .expect("ReadService get_object should work");

    // Verify ReadService get_object works correctly
    assert!(response.data.is_some(), "Should have data");
    assert!(response.error.is_none(), "Should not have error");
    let iota_object_data = response.data.unwrap();

    let expected_object_ref = gas_object.data.as_ref().unwrap().object_ref();

    // Verify object matches expected data
    assert_eq!(
        iota_object_data.object_id, expected_object_ref.0,
        "Object ID should match"
    );
    assert_eq!(
        iota_object_data.version, expected_object_ref.1,
        "Version should match"
    );
    assert!(
        iota_object_data.type_.is_some(),
        "Object type should be present"
    );
    assert!(
        iota_object_data.owner.is_some(),
        "Object owner should be present"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_read_service_nonexistent_object() {
    let (_cluster, mut read_client) = setup_test_cluster_and_read_client().await;

    // Use a dummy object ID that doesn't exist
    let nonexistent_object_id = [0u8; 32];

    let nonexistent_id = ObjectID::from_bytes(nonexistent_object_id).unwrap();
    let options = Some(IotaObjectDataOptions {
        show_type: true,
        show_owner: true,
        show_previous_transaction: false,
        show_display: false,
        show_content: false,
        show_bcs: false,
        show_storage_rebate: false,
    });

    let response = tokio::time::timeout(
        Duration::from_secs(30),
        read_client.get_object(nonexistent_id, options),
    )
    .await
    .expect("timeout waiting for object")
    .expect("ReadService get_object should work");

    // Verify ReadService returns proper error for non-existent object
    assert!(response.data.is_none(), "Should not have data");
    assert!(response.error.is_some(), "Should have error");
    let iota_error = response.error.unwrap();

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
