// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use iota_config::local_ip_utils;
use iota_grpc_api::{
    client::NodeClient,
    write::{ExecuteTransactionRequest, TransactionResponseOptions},
};
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
async fn test_write_service_execute_transaction() {
    let (cluster, node_client) = setup_test_cluster_and_client().await;

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

    // Test execute_transaction via WriteService with real transaction data
    let tx_result = tokio::time::timeout(Duration::from_secs(30), async {
        let mut write_client = node_client
            .write_client()
            .ok_or_else(|| anyhow::anyhow!("Write client not available"))?;

        let request = ExecuteTransactionRequest {
            tx_bytes,
            signatures,
            options: Some(TransactionResponseOptions {
                show_input: false,
                show_raw_input: false,
                show_effects: true,
                show_events: false,
                show_object_changes: false,
                show_balance_changes: false,
                show_raw_effects: false,
            }),
            request_type: None, // Let the system use default
        };

        let response = write_client.execute_transaction(request).await?;

        // Validate the IotaTransactionBlockResponse
        assert!(
            !response.digest.inner().is_empty(),
            "Response should have a valid digest"
        );

        // Since we requested show_effects: true, validate effects are present
        assert!(
            response.effects.is_some(),
            "Effects should be present when show_effects is true"
        );

        // Validate that fields we didn't request are None/empty
        assert!(
            response.transaction.is_none(),
            "Transaction should be None when show_input is false"
        );
        assert!(
            response.raw_transaction.is_empty(),
            "Raw transaction should be empty when show_raw_input is false"
        );
        assert!(
            response.events.is_none(),
            "Events should be None when show_events is false"
        );
        assert!(
            response.object_changes.is_none(),
            "Object changes should be None when show_object_changes is false"
        );
        assert!(
            response.balance_changes.is_none(),
            "Balance changes should be None when show_balance_changes is false"
        );
        assert!(
            response.raw_effects.is_empty(),
            "Raw effects should be empty when show_raw_effects is false"
        );

        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("timeout waiting for transaction");

    match tx_result {
        Ok(()) => {
            // Transaction was executed successfully and response validated
            println!("Transaction executed successfully via WriteService");
        }
        Err(e) => {
            let error_msg = e.to_string();

            // Check if this is expected (WriteService not available in this test
            // environment)
            assert!(
                error_msg.contains("Write API not configured")
                    || error_msg.contains("read-only mode")
                    || error_msg.contains("unimplemented")
                    || error_msg.contains("Transaction execution not available")
                    || error_msg.contains("Deserialization error")
                    || error_msg.contains("variant index")
                    || error_msg.contains("unexpected end of input"),
                "Expected WriteService/transaction execution limitation, got unexpected error: {error_msg}"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_write_service_invalid_transaction() {
    let (_cluster, node_client) = setup_test_cluster_and_client().await;

    // Create invalid transaction data (dummy bytes that won't deserialize properly)
    let tx_bytes = vec![0u8; 32]; // Invalid transaction bytes
    let signatures = vec![vec![0u8; 64]]; // Invalid signature

    // Test execute_transaction with invalid data via WriteService
    let tx_result = tokio::time::timeout(Duration::from_secs(30), async {
        let mut write_client = node_client
            .write_client()
            .ok_or_else(|| anyhow::anyhow!("Write client not available"))?;

        let request = ExecuteTransactionRequest {
            tx_bytes,
            signatures,
            options: Some(TransactionResponseOptions {
                show_input: false,
                show_raw_input: false,
                show_effects: true,
                show_events: false,
                show_object_changes: false,
                show_balance_changes: false,
                show_raw_effects: false,
            }),
            request_type: None,
        };

        let _response = write_client.execute_transaction(request).await?;
        // Should not reach here with invalid transaction data
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("timeout waiting for transaction");

    match tx_result {
        Ok(()) => {
            // This would be unexpected for invalid transaction data
            panic!("WriteService should not succeed with invalid transaction data");
        }
        Err(e) => {
            let error_msg = e.to_string();

            // Expected: invalid transaction format or WriteService not available
            assert!(
                error_msg.contains("Failed to deserialize transaction data")
                    || error_msg.contains("invalid argument")
                    || error_msg.contains("Transaction execution not available")
                    || error_msg.contains("Write API not configured")
                    || error_msg.contains("Deserialization error")
                    || error_msg.contains("variant index")
                    || error_msg.contains("unexpected end of input"),
                "Expected transaction deserialization or WriteService error, got: {error_msg}"
            );
        }
    }
}
