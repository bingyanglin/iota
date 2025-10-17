// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use iota_grpc_api::{
    client::WriteClient,
    common::{BcsData, TransactionResponseOptions},
    write::ExecuteTransactionRequest,
};
use iota_types::{
    effects::TransactionEffectsAPI,
    transaction::{TransactionData, TransactionDataAPI},
};
use test_cluster::TestCluster;

mod utils;
use utils::setup_test_cluster_and_client;

async fn setup_test_cluster_and_write_client() -> (TestCluster, WriteClient) {
    let (cluster, node_client) = setup_test_cluster_and_client().await;

    let write_client = node_client
        .write_client()
        .expect("Write client should be available");

    (cluster, write_client)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_write_service_execute_transaction() {
    let (cluster, mut write_client) = setup_test_cluster_and_write_client().await;

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
    let tx_bytes =
        bcs::to_bytes(&signed_tx.data().intent_message().value).expect("BCS serialization failed");
    let signatures: Vec<Vec<u8>> = signed_tx
        .tx_signatures()
        .iter()
        .map(|sig| sig.as_ref().to_vec())
        .collect();

    // Test execute_transaction via WriteService with real transaction data
    let tx_result = tokio::time::timeout(Duration::from_secs(30), async {
        let request = ExecuteTransactionRequest {
            tx_bytes: Some(BcsData { data: tx_bytes }),
            signatures: signatures
                .into_iter()
                .map(|data| BcsData { data })
                .collect(),
            options: Some(TransactionResponseOptions {
                show_input: false,
                show_raw_input: false,
                show_effects: true,
                show_events: false,
                show_object_changes: false,
                show_balance_changes: false,
                show_raw_effects: false,
            }),
            request_type: None, // Uses default: WaitForEffectsCert
        };

        let response = write_client.execute_transaction(request).await?;

        // Validate the ExecuteTransactionResponse
        assert!(response.digest.is_some(), "Response should have a digest");
        assert!(
            !response.digest.as_ref().unwrap().digest.is_empty(),
            "Digest should not be empty"
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
            response.raw_transaction.is_none(),
            "Raw transaction should be None when show_raw_input is false"
        );
        assert!(
            response.events.is_none(),
            "Events should be None when show_events is false"
        );
        assert!(
            response.object_changes.is_empty(),
            "Object changes should be empty when show_object_changes is false"
        );
        assert!(
            response.balance_changes.is_empty(),
            "Balance changes should be empty when show_balance_changes is false"
        );
        assert!(
            response.raw_effects.is_none(),
            "Raw effects should be None when show_raw_effects is false"
        );

        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("timeout waiting for transaction");

    match tx_result {
        Ok(()) => {}
        Err(e) => {
            panic!("WriteService transaction execution failed: {e}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_write_service_invalid_transaction() {
    let (_cluster, mut write_client) = setup_test_cluster_and_write_client().await;

    // Create invalid transaction data (dummy bytes that won't deserialize properly)
    let tx_bytes = vec![0u8; 32]; // Invalid transaction bytes
    let signatures = vec![vec![0u8; 64]]; // Invalid signature

    // Test execute_transaction with invalid data via WriteService
    let tx_result = tokio::time::timeout(Duration::from_secs(30), async {
        let request = ExecuteTransactionRequest {
            tx_bytes: Some(BcsData { data: tx_bytes }),
            signatures: signatures
                .into_iter()
                .map(|data| BcsData { data })
                .collect(),
            options: Some(TransactionResponseOptions {
                show_input: false,
                show_raw_input: false,
                show_effects: true,
                show_events: false,
                show_object_changes: false,
                show_balance_changes: false,
                show_raw_effects: false,
            }),
            request_type: Some(1), // WaitForLocalExecution
        };

        let _response = write_client.execute_transaction(request).await?;
        // Should not reach here with invalid transaction data
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("timeout waiting for transaction");

    if let Ok(()) = tx_result {
        // This would be unexpected for invalid transaction data
        panic!("WriteService should not succeed with invalid transaction data");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_transaction_data_bcs_deserialization() {
    let (cluster, mut write_client) = setup_test_cluster_and_write_client().await;

    let sender = cluster.get_address_0();
    let receiver = cluster.get_address_1();
    let amount = 1000u64;

    // Build and sign transaction
    let tx_data = cluster
        .test_transaction_builder_with_sender(sender)
        .await
        .transfer_iota(Some(amount), receiver)
        .build();

    let signed_tx = cluster.sign_transaction(&tx_data);
    let tx_bytes =
        bcs::to_bytes(&signed_tx.data().intent_message().value).expect("BCS serialization failed");
    let signatures: Vec<Vec<u8>> = signed_tx
        .tx_signatures()
        .iter()
        .map(|sig| sig.as_ref().to_vec())
        .collect();

    // Execute transaction
    let request = ExecuteTransactionRequest {
        tx_bytes: Some(BcsData { data: tx_bytes }),
        signatures: signatures
            .into_iter()
            .map(|data| BcsData { data })
            .collect(),
        options: Some(TransactionResponseOptions {
            show_input: true,
            show_raw_input: true,
            show_effects: false,
            show_events: false,
            show_object_changes: false,
            show_balance_changes: false,
            show_raw_effects: false,
        }),
        request_type: None,
    };
    let response = write_client
        .execute_transaction(request)
        .await
        .expect("gRPC call should succeed");

    // Verify TransactionData can be deserialized from BCS
    assert!(
        response.transaction.is_some(),
        "TransactionData should be present when show_input is true"
    );

    let bcs_data = response.transaction.as_ref().unwrap();
    let deserialized_tx_data: TransactionData = bcs::from_bytes(&bcs_data.data)
        .expect("Should be able to deserialize TransactionData from BCS");

    // Verify the deserialized data matches original
    assert_eq!(deserialized_tx_data.sender(), sender, "Sender should match");
    assert_eq!(
        deserialized_tx_data.gas_budget(),
        tx_data.gas_budget(),
        "Gas budget should match"
    );

    // Verify raw_transaction can also be deserialized
    assert!(
        response.raw_transaction.is_some(),
        "Raw transaction should be present when show_raw_input is true"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_write_service_dry_run_transaction() {
    let (cluster, mut write_client) = setup_test_cluster_and_write_client().await;

    let sender = cluster.get_address_0();
    let receiver = cluster.get_address_1();
    let amount = 1000u64;

    // Build transaction data
    let tx_data = cluster
        .test_transaction_builder_with_sender(sender)
        .await
        .transfer_iota(Some(amount), receiver)
        .build();

    // Serialize transaction data for dry run
    let tx_bytes = bcs::to_bytes(&tx_data).expect("BCS serialization failed");

    // Test dry_run_transaction
    let response = tokio::time::timeout(
        Duration::from_secs(30),
        write_client.dry_run_transaction(tx_bytes),
    )
    .await
    .expect("timeout waiting for dry_run_transaction")
    .expect("dry_run_transaction should succeed");

    // Validate the response structure
    assert!(
        response.effects.is_some(),
        "Effects should be present in dry run response"
    );
    assert!(
        response.input.is_some(),
        "Input should be present in dry run response"
    );

    // Validate effects can be deserialized
    let effects_data = response.effects.as_ref().unwrap();
    let effects: iota_types::effects::TransactionEffects =
        bcs::from_bytes(&effects_data.data).expect("Effects should deserialize from BCS");
    assert!(
        effects.status().is_ok(),
        "Transaction should execute successfully"
    );

    // Validate object_changes for transfer transaction
    assert!(
        !response.object_changes.is_empty(),
        "Object changes should be present for transfer transaction"
    );

    // Validate balance_changes for transfer transaction
    assert!(
        !response.balance_changes.is_empty(),
        "Balance changes should be present for transfer transaction"
    );

    // Validate input transaction data
    let input = response.input.as_ref().unwrap();
    assert!(
        input.transaction.is_some(),
        "Input should have transaction data"
    );
    let input_tx_data: iota_types::transaction::TransactionData =
        bcs::from_bytes(&input.transaction.as_ref().unwrap().data)
            .expect("Input transaction data should deserialize from BCS");
    assert_eq!(input_tx_data.sender(), sender, "Sender should match");

    // Validate execution_error_source is None for successful transaction
    assert!(
        response.execution_error_source.is_none(),
        "execution_error_source should be None for successful transaction"
    );

    // Validate events field exists (may be empty for simple transfers that don't
    // emit events) The events field is a Vec, not Option, so it always exists
    // For a simple IOTA transfer, no custom events are emitted
    assert!(
        response.events.is_empty(),
        "Simple transfer should not emit custom events"
    );

    // Note: suggested_gas_price may or may not be present depending on
    // congestion
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_write_service_dry_run_transaction_with_failure() {
    let (cluster, mut write_client) = setup_test_cluster_and_write_client().await;

    let sender = cluster.get_address_0();
    let receiver = cluster.get_address_1();

    // Create a transaction that will pass validation but fail during execution
    // We'll use a very large transfer amount that exceeds the sender's balance
    // First get one of the sender's gas coins
    let _gas_objects = cluster
        .get_owned_objects(sender, None)
        .await
        .expect("Failed to get owned objects");

    // Build a transaction with a very large amount that will cause
    // InsufficientCoinBalance error This will pass basic validation but fail
    // during execution
    let tx_data = cluster
        .test_transaction_builder_with_sender(sender)
        .await
        .transfer_iota(Some(u64::MAX), receiver) // Transfer max amount - will fail
        .build();

    let tx_bytes = bcs::to_bytes(&tx_data).expect("BCS serialization failed");

    // Test dry_run_transaction with failing transaction
    let response = tokio::time::timeout(
        Duration::from_secs(30),
        write_client.dry_run_transaction(tx_bytes),
    )
    .await
    .expect("timeout waiting for dry_run_transaction")
    .expect("dry_run_transaction should succeed even with failing transaction");

    // Validate effects show failure
    let effects_data = response.effects.as_ref().unwrap();
    let effects: iota_types::effects::TransactionEffects =
        bcs::from_bytes(&effects_data.data).expect("Effects should deserialize from BCS");

    // The transaction should fail due to insufficient balance
    assert!(
        effects.status().is_err(),
        "Transaction should fail with insufficient balance"
    );

    // Validate execution_error_source is present for failed transaction
    assert!(
        response.execution_error_source.is_some(),
        "execution_error_source should be present for failed transaction"
    );

    let error_source = response.execution_error_source.as_ref().unwrap();
    assert!(
        !error_source.is_empty(),
        "execution_error_source should not be empty"
    );
    // The error should mention insufficient balance or coin
    assert!(
        error_source.to_lowercase().contains("insufficient")
            || error_source.to_lowercase().contains("coin"),
        "Error should mention insufficient balance or coin issue, got: {error_source}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_write_service_dev_inspect_transaction() {
    let (cluster, mut write_client) = setup_test_cluster_and_write_client().await;

    let sender = cluster.get_address_0();

    // Build a simple move call for dev inspect
    let tx_data = cluster
        .test_transaction_builder_with_sender(sender)
        .await
        .transfer_iota(Some(1000), cluster.get_address_1())
        .build();

    // Extract the transaction kind (not the full TransactionData)
    let tx_kind_bytes = bcs::to_bytes(tx_data.kind()).expect("BCS serialization failed");

    // Test dev_inspect_transaction
    let response = tokio::time::timeout(
        Duration::from_secs(30),
        write_client.dev_inspect_transaction(iota_grpc_api::write::DevInspectTransactionRequest {
            sender: Some(iota_grpc_api::common::Address {
                address: sender.to_vec(),
            }),
            tx_bytes: Some(BcsData {
                data: tx_kind_bytes,
            }),
            gas_price: None,
            additional_args: None,
        }),
    )
    .await
    .expect("timeout waiting for dev_inspect_transaction")
    .expect("dev_inspect_transaction should succeed");

    // Validate the response structure
    assert!(
        response.effects.is_some(),
        "Effects should be present in dev inspect response"
    );

    // DevInspect returns BCS-encoded iota-types TransactionEffects (core type)
    // This matches the protobuf definition and is consistent with other gRPC APIs
    let effects_data = response.effects.as_ref().unwrap();
    assert!(
        !effects_data.data.is_empty(),
        "Effects data should not be empty"
    );

    // Validate error field is None for successful execution
    assert!(
        response.error.is_none(),
        "Error should be None for successful dev inspect"
    );

    // Validate txn_data based on additional_args.show_txn_data
    // Since we didn't set show_txn_data=true, txn_data should be None
    assert!(
        response.txn_data.is_none(),
        "txn_data should be None when show_txn_data is not set"
    );

    // Validate events field exists
    assert!(
        response.events.is_empty(),
        "Simple transfer should not emit custom events"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_write_service_dev_inspect_with_show_txn_data() {
    let (cluster, mut write_client) = setup_test_cluster_and_write_client().await;

    let sender = cluster.get_address_0();

    // Build a transaction for dev inspect
    let tx_data = cluster
        .test_transaction_builder_with_sender(sender)
        .await
        .transfer_iota(Some(1000), cluster.get_address_1())
        .build();

    let tx_kind_bytes = bcs::to_bytes(tx_data.kind()).expect("BCS serialization failed");

    // Test dev_inspect_transaction with show_txn_data=true
    let response = tokio::time::timeout(
        Duration::from_secs(30),
        write_client.dev_inspect_transaction(iota_grpc_api::write::DevInspectTransactionRequest {
            sender: Some(iota_grpc_api::common::Address {
                address: sender.to_vec(),
            }),
            tx_bytes: Some(BcsData {
                data: tx_kind_bytes,
            }),
            gas_price: None,
            additional_args: Some(iota_grpc_api::write::DevInspectArgs {
                gas_sponsor: None,
                gas_budget: None,
                gas_objects: vec![],
                skip_checks: Some(false),
                show_txn_data: Some(true), // Request txn_data
            }),
        }),
    )
    .await
    .expect("timeout waiting for dev_inspect_transaction")
    .expect("dev_inspect_transaction should succeed");

    // Validate txn_data is present when show_txn_data=true
    assert!(
        response.txn_data.is_some(),
        "txn_data should be present when show_txn_data=true"
    );

    // Validate txn_data can be deserialized
    let txn_data_bcs = response.txn_data.as_ref().unwrap();
    let txn_data: iota_types::transaction::TransactionData =
        bcs::from_bytes(&txn_data_bcs.data).expect("txn_data should deserialize from BCS");

    assert_eq!(txn_data.sender(), sender, "Sender should match");
}
