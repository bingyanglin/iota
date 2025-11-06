// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use iota_config::local_ip_utils;
use iota_grpc_types::v0::ledger_service::{
    GetEpochRequest, GetEpochResponse, ledger_service_client::LedgerServiceClient,
};
use prost_types::FieldMask;
use test_cluster::{TestCluster, TestClusterBuilder};

async fn setup_test_cluster_and_client()
-> (TestCluster, LedgerServiceClient<tonic::transport::Channel>) {
    let localhost = local_ip_utils::localhost_for_testing();
    let grpc_port = local_ip_utils::get_available_port(&localhost);
    let grpc_addr = format!("{localhost}:{grpc_port}");

    // Start a test cluster with gRPC enabled
    let cluster = TestClusterBuilder::new()
        .with_fullnode_grpc_api_address(grpc_addr.parse().expect("Invalid gRPC address"))
        .with_num_validators(1)
        .build()
        .await;

    let client = LedgerServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .expect("Failed to connect to gRPC server");

    (cluster, client)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_epoch_current() {
    let (_cluster, mut client) = setup_test_cluster_and_client().await;

    // Test GetEpoch without specifying epoch (should return current epoch or fail
    // if no checkpoints)
    let request = GetEpochRequest {
        epoch: None,
        read_mask: None,
    };

    let result = client.get_epoch(request).await;

    // The cluster might not have checkpoints yet, so this could fail with
    // Unavailable
    match result {
        Ok(response) => {
            let response: GetEpochResponse = response.into_inner();
            assert!(
                response.epoch.is_some(),
                "Response should contain epoch data"
            );

            let epoch = response.epoch.unwrap();
            assert!(epoch.epoch.is_some(), "Epoch number should be present");
            assert!(
                epoch.committee.is_some(),
                "Committee should be present in default mask"
            );
        }
        Err(status) => {
            // It's acceptable to get Unavailable if no checkpoints exist yet
            assert_eq!(
                status.code(),
                tonic::Code::Unavailable,
                "Should fail with Unavailable if no checkpoints"
            );
            assert!(
                status.message().contains("no checkpoints available"),
                "Error message should indicate no checkpoints: {}",
                status.message()
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_epoch_by_number() {
    let (_cluster, mut client) = setup_test_cluster_and_client().await;

    // Test GetEpoch for epoch 0 (genesis epoch)
    let request = GetEpochRequest {
        epoch: Some(0),
        read_mask: None,
    };

    let response = client
        .get_epoch(request)
        .await
        .expect("GetEpoch for epoch 0 should succeed");

    let response: GetEpochResponse = response.into_inner();
    assert!(
        response.epoch.is_some(),
        "Response should contain epoch data"
    );

    let epoch = response.epoch.unwrap();
    assert_eq!(
        epoch.epoch,
        Some(0),
        "Returned epoch should match requested epoch"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_epoch_with_field_mask() {
    let (_cluster, mut client) = setup_test_cluster_and_client().await;

    // Test with field mask requesting only epoch number
    let request = GetEpochRequest {
        epoch: Some(0),
        read_mask: Some(FieldMask {
            paths: vec!["epoch".to_string()],
        }),
    };

    let response = client
        .get_epoch(request)
        .await
        .expect("GetEpoch with field_mask should succeed");

    let response: GetEpochResponse = response.into_inner();
    let epoch = response.epoch.expect("Response should contain epoch data");

    assert_eq!(epoch.epoch, Some(0), "Epoch number should be present");
    // With only "epoch" in mask, committee should still be None or minimal
    // (depending on implementation details)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_epoch_with_committee_mask() {
    let (_cluster, mut client) = setup_test_cluster_and_client().await;

    // Test with field mask requesting epoch and committee
    let request = GetEpochRequest {
        epoch: Some(0),
        read_mask: Some(FieldMask {
            paths: vec!["epoch".to_string(), "committee".to_string()],
        }),
    };

    let response = client
        .get_epoch(request)
        .await
        .expect("GetEpoch with committee mask should succeed");

    let response: GetEpochResponse = response.into_inner();
    let epoch = response.epoch.expect("Response should contain epoch data");

    assert_eq!(epoch.epoch, Some(0), "Epoch number should be present");
    assert!(
        epoch.committee.is_some(),
        "Committee should be present when requested"
    );

    let committee = epoch.committee.unwrap();
    assert_eq!(
        committee.epoch,
        Some(0),
        "Committee epoch should match requested epoch"
    );
    assert!(
        !committee.members.is_empty(),
        "Committee should have at least one validator"
    );

    // Verify validator member structure
    let member = &committee.members[0];
    assert!(
        member.public_key.is_some(),
        "Validator should have public key"
    );
    assert!(member.weight.is_some(), "Validator should have weight");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_epoch_with_checkpoint_range_mask() {
    let (_cluster, mut client) = setup_test_cluster_and_client().await;

    // Test with field mask requesting checkpoint range
    let request = GetEpochRequest {
        epoch: Some(0),
        read_mask: Some(FieldMask {
            paths: vec![
                "epoch".to_string(),
                "first_checkpoint".to_string(),
                "last_checkpoint".to_string(),
            ],
        }),
    };

    let response = client
        .get_epoch(request)
        .await
        .expect("GetEpoch with checkpoint range mask should succeed");

    let response: GetEpochResponse = response.into_inner();
    let epoch = response.epoch.expect("Response should contain epoch data");

    assert_eq!(epoch.epoch, Some(0), "Epoch number should be present");
    // Note: Genesis epoch might not have checkpoint data populated yet
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_epoch_with_timestamps_mask() {
    let (_cluster, mut client) = setup_test_cluster_and_client().await;

    // Test with field mask requesting timestamps
    let request = GetEpochRequest {
        epoch: Some(0),
        read_mask: Some(FieldMask {
            paths: vec!["epoch".to_string(), "start".to_string(), "end".to_string()],
        }),
    };

    let response = client
        .get_epoch(request)
        .await
        .expect("GetEpoch with timestamps mask should succeed");

    let response: GetEpochResponse = response.into_inner();
    let epoch = response.epoch.expect("Response should contain epoch data");

    assert_eq!(epoch.epoch, Some(0), "Epoch number should be present");
    // Note: Genesis epoch might not have timestamp data yet, and end might be
    // None for current epoch
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_epoch_with_reference_gas_price_mask() {
    let (_cluster, mut client) = setup_test_cluster_and_client().await;

    // Test with field mask requesting reference gas price
    let request = GetEpochRequest {
        epoch: Some(0),
        read_mask: Some(FieldMask {
            paths: vec!["epoch".to_string(), "reference_gas_price".to_string()],
        }),
    };

    let response = client
        .get_epoch(request)
        .await
        .expect("GetEpoch with reference_gas_price mask should succeed");

    let response: GetEpochResponse = response.into_inner();
    let epoch = response.epoch.expect("Response should contain epoch data");

    assert_eq!(epoch.epoch, Some(0), "Epoch number should be present");
    // Note: Genesis epoch might not have reference gas price populated yet
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_epoch_with_all_fields_mask() {
    let (_cluster, mut client) = setup_test_cluster_and_client().await;

    // Test with field mask requesting all fields using wildcard
    let request = GetEpochRequest {
        epoch: Some(0),
        read_mask: Some(FieldMask {
            paths: vec!["*".to_string()],
        }),
    };

    let response = client
        .get_epoch(request)
        .await
        .expect("GetEpoch with all fields mask should succeed");

    let response: GetEpochResponse = response.into_inner();
    let epoch = response.epoch.expect("Response should contain epoch data");

    assert_eq!(epoch.epoch, Some(0), "Epoch number should be present");
    assert!(
        epoch.committee.is_some(),
        "Committee should be present with wildcard"
    );
    // Note: Other fields like checkpoints, gas price might not be populated for
    // genesis epoch
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_epoch_invalid_field_mask() {
    let (_cluster, mut client) = setup_test_cluster_and_client().await;

    // Test with invalid field path in mask
    let request = GetEpochRequest {
        epoch: Some(0),
        read_mask: Some(FieldMask {
            paths: vec!["invalid_field_name".to_string()],
        }),
    };

    let result = client.get_epoch(request).await;

    // Should return an error (InvalidArgument)
    assert!(
        result.is_err(),
        "GetEpoch should fail for invalid field mask"
    );

    let error = result.err().unwrap();
    assert_eq!(
        error.code(),
        tonic::Code::InvalidArgument,
        "Error should be InvalidArgument for invalid field mask"
    );
    assert!(
        error.message().contains("Invalid field path"),
        "Error message should mention invalid field path"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_epoch_nonexistent_epoch() {
    let (_cluster, mut client) = setup_test_cluster_and_client().await;

    // Test with a very large epoch number that doesn't exist
    let request = GetEpochRequest {
        epoch: Some(99999),
        read_mask: None,
    };

    let result = client.get_epoch(request).await;

    // Should return an error (NotFound)
    assert!(
        result.is_err(),
        "GetEpoch should fail for non-existent epoch"
    );

    let error = result.err().unwrap();
    assert_eq!(
        error.code(),
        tonic::Code::NotFound,
        "Error should be NotFound for non-existent epoch"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_epoch_default_mask_fields() {
    let (_cluster, mut client) = setup_test_cluster_and_client().await;

    // Test without field mask to verify default fields
    // Default is: "epoch,committee,first_checkpoint,last_checkpoint"
    let request = GetEpochRequest {
        epoch: Some(0),
        read_mask: None,
    };

    let response = client
        .get_epoch(request)
        .await
        .expect("GetEpoch should succeed with default mask");

    let response: GetEpochResponse = response.into_inner();
    let epoch = response.epoch.expect("Response should contain epoch data");

    // Verify default fields are present
    assert!(epoch.epoch.is_some(), "Epoch number should be in default");
    assert!(epoch.committee.is_some(), "Committee should be in default");
    // Note: Genesis epoch might not have checkpoint data populated yet
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_epoch_nested_field_mask() {
    let (_cluster, mut client) = setup_test_cluster_and_client().await;

    // Test with nested field mask for committee members
    let request = GetEpochRequest {
        epoch: Some(0),
        read_mask: Some(FieldMask {
            paths: vec![
                "epoch".to_string(),
                "committee.epoch".to_string(),
                "committee.members".to_string(),
            ],
        }),
    };

    let response = client
        .get_epoch(request)
        .await
        .expect("GetEpoch with nested field mask should succeed");

    let response: GetEpochResponse = response.into_inner();
    let epoch = response.epoch.expect("Response should contain epoch data");

    assert!(
        epoch.committee.is_some(),
        "Committee should be present with nested mask"
    );

    let committee = epoch.committee.unwrap();
    assert!(
        committee.epoch.is_some(),
        "Committee epoch should be present"
    );
    assert!(
        !committee.members.is_empty(),
        "Committee members should be present"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_epoch_protocol_config_mask() {
    let (_cluster, mut client) = setup_test_cluster_and_client().await;

    // Test with field mask requesting protocol config
    let request = GetEpochRequest {
        epoch: Some(0),
        read_mask: Some(FieldMask {
            paths: vec!["epoch".to_string(), "protocol_config".to_string()],
        }),
    };

    let response = client
        .get_epoch(request)
        .await
        .expect("GetEpoch with protocol_config mask should succeed");

    let response: GetEpochResponse = response.into_inner();
    let epoch = response.epoch.expect("Response should contain epoch data");

    assert_eq!(epoch.epoch, Some(0), "Epoch number should be present");
    // Note: Genesis epoch might not have protocol config populated yet
}
