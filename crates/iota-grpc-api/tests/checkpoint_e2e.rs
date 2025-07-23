// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use futures::StreamExt;
use iota_grpc_api::{
    client::GrpcNodeClient,
    node::{CheckpointStreamRequest, node_service_client::NodeServiceClient},
};
use test_cluster::TestClusterBuilder;
use tonic::Request;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_stream_checkpoints() {
    // Pick a port for gRPC
    let grpc_port = 50055u16;
    let grpc_addr = format!("127.0.0.1:{grpc_port}");

    // Start a test cluster with gRPC enabled and pruning disabled
    let cluster = TestClusterBuilder::new()
        .with_fullnode_grpc_api_address(grpc_addr.parse().expect("Invalid gRPC address"))
        .disable_fullnode_pruning()
        .with_num_validators(1)
        .build()
        .await;

    // Wait for checkpoint 2 to be available
    println!("Waiting for checkpoint 2");
    cluster.wait_for_checkpoint(2, None).await;

    println!("Connecting to gRPC at {grpc_addr}");
    let mut client = NodeServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .expect("connect gRPC");
    println!("Connected to gRPC!");

    // Request all checkpoints
    println!("Sending gRPC stream request");
    let request = Request::new(CheckpointStreamRequest {
        start_sequence_number: None,
        end_sequence_number: None,
        full: None,
    });
    let mut stream = client
        .stream_checkpoints(request)
        .await
        .expect("gRPC call")
        .into_inner();
    println!("Starting to stream checkpoints");

    // Only collect the first 20 checkpoints to avoid hanging
    let mut indices = Vec::new();
    let mut count = 0;
    while let Some(res) = stream.next().await {
        match res {
            Ok(cp) => {
                println!("[gRPC] Received checkpoint: {cp:?}");
                indices.push(cp.sequence_number);
                count += 1;
                if count >= 20 {
                    break;
                }
            }
            Err(e) => {
                panic!("Error streaming checkpoint: {e:?}");
            }
        }
    }
    // There should be at least 20 checkpoints
    assert!(indices.len() >= 20, "Should stream at least 20 checkpoints");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_get_epoch_first_checkpoint_sequence_number() {
    // Pick a port for gRPC
    let grpc_port = 50058u16;
    let grpc_addr = format!("127.0.0.1:{grpc_port}");

    // Start a test cluster with gRPC enabled and pruning disabled
    let cluster = TestClusterBuilder::new()
        .with_fullnode_grpc_api_address(grpc_addr.parse().expect("Invalid gRPC address"))
        .disable_fullnode_pruning()
        .with_num_validators(1)
        .build()
        .await;

    let sender = cluster.get_address_0();
    let receiver = cluster.get_address_1();

    // Wait for 2 new checkpoint to be available
    cluster.wait_for_checkpoint(2, None).await;
    println!("Checkpoint 2 available, forcing new epoch");

    // Advance to a new epoch
    cluster.force_new_epoch().await;
    cluster.transfer_iota_must_exceed(sender, receiver, 1).await;
    let current_epoch = cluster
        .fullnode_handle
        .iota_node
        .with(|node| node.state().epoch_store_for_testing().epoch());
    println!("Current epoch after force_new_epoch: {current_epoch}");

    // Wait for 3 new checkpoints in the new epoch
    cluster.wait_for_checkpoint(3, None).await;
    cluster.force_new_epoch().await;
    cluster.transfer_iota_must_exceed(sender, receiver, 1).await;
    let current_epoch = cluster
        .fullnode_handle
        .iota_node
        .with(|node| node.state().epoch_store_for_testing().epoch());
    println!("Current epoch after force_new_epoch: {current_epoch}");

    // Connect to the gRPC endpoint
    let mut client = GrpcNodeClient::connect(&format!("http://{grpc_addr}"))
        .await
        .expect("connect gRPC");

    // List all checkpoints and their epochs using the gRPC stream
    println!("[gRPC] Listing all checkpoints and their epochs via gRPC stream");
    let mut stream = client
        .stream_checkpoints(Some(0), None, Some(false))
        .await
        .expect("gRPC stream");
    let mut all_indices = vec![];
    let mut all_epochs = vec![];
    while let Some(res) = stream.next().await {
        match res {
            Ok(cp) => match GrpcNodeClient::deserialize_checkpoint(&cp) {
                Ok(iota_grpc_api::client::CheckpointContent::Summary(summary)) => {
                    if let Some(v1_summary) = summary.as_v1() {
                        let epoch = v1_summary.data().epoch;
                        println!(
                            "Checkpoint sequence_number: {}, epoch: {}",
                            cp.sequence_number, epoch
                        );
                        all_indices.push(cp.sequence_number);
                        all_epochs.push(epoch);
                        if cp.sequence_number > 50 {
                            break;
                        }
                    }
                }
                Ok(iota_grpc_api::client::CheckpointContent::Data(_)) => {
                    panic!(
                        "Expected checkpoint summary but received data at sequence_number {}",
                        cp.sequence_number
                    );
                }
                Err(e) => {
                    panic!(
                        "Failed to deserialize checkpoint at sequence_number {}: {:?}",
                        cp.sequence_number, e
                    );
                }
            },
            Err(e) => {
                panic!("gRPC stream error: {e:?}");
            }
        }
    }

    // Query for the first checkpoint of epoch 0 (should be 0)
    let first_0 = client
        .get_epoch_first_checkpoint_sequence_number(0)
        .await
        .expect("gRPC call");
    println!("[gRPC] First checkpoint of epoch 0: {first_0}");
    assert_eq!(first_0, 0, "First checkpoint of epoch 0 should be 0");

    // Query for the first checkpoint of epoch 1 (should be >= 2)
    let first_1 = client
        .get_epoch_first_checkpoint_sequence_number(1)
        .await
        .expect("gRPC call");
    println!("[gRPC] First checkpoint of epoch 1: {first_1}");
    assert!(
        first_1 >= 2,
        "First checkpoint of epoch 1 should be >= 2, got {first_1}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_stream_full_checkpoint_data() {
    let grpc_port = 50059u16;
    let grpc_addr = format!("127.0.0.1:{grpc_port}");

    // Start a test cluster with gRPC enabled and pruning disabled
    let cluster = TestClusterBuilder::new()
        .with_fullnode_grpc_api_address(grpc_addr.parse().expect("Invalid gRPC address"))
        .disable_fullnode_pruning()
        .with_num_validators(1)
        .build()
        .await;
    cluster.wait_for_checkpoint(2, None).await;
    let mut client = GrpcNodeClient::connect(&format!("http://{grpc_addr}"))
        .await
        .unwrap();
    let mut stream = client
        .stream_checkpoints(None, Some(2), Some(true))
        .await
        .unwrap();
    if let Some(Ok(cp)) = stream.next().await {
        let checkpoint_data = match GrpcNodeClient::deserialize_checkpoint(&cp)
            .expect("Failed to deserialize checkpoint")
        {
            iota_grpc_api::client::CheckpointContent::Data(data) => data,
            iota_grpc_api::client::CheckpointContent::Summary(_) => {
                panic!("Expected data, got summary")
            }
        };
        if let Some(v1_data) = checkpoint_data.as_v1() {
            assert_eq!(v1_data.checkpoint_summary.sequence_number, 2);
        } else {
            panic!("Expected V1 checkpoint data");
        }
    } else {
        panic!("No checkpoint data returned");
    }
}
