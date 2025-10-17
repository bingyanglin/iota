// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use iota_grpc_api::{client::ReadClient, common::TransactionResponseOptions};
use iota_types::{
    base_types::{ObjectID, TransactionDigest},
    object::ObjectRead,
    transaction::TransactionDataAPI,
};
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

    let object_read =
        tokio::time::timeout(Duration::from_secs(30), read_client.get_object(object_id))
            .await
            .expect("timeout waiting for object")
            .expect("ReadService get_object should work");

    // Verify ObjectRead::Exists variant
    match object_read {
        ObjectRead::Exists(object_ref, object, _layout) => {
            let expected_object_ref = gas_object.data.as_ref().unwrap().object_ref();

            assert_eq!(
                object_ref.0, expected_object_ref.0,
                "Object ID should match"
            );
            assert_eq!(object_ref.1, expected_object_ref.1, "Version should match");
            assert_eq!(
                object.owner(),
                &iota_types::object::Owner::AddressOwner(sender)
            );
        }
        _ => panic!("Expected ObjectRead::Exists, got {object_read:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_read_service_nonexistent_object() {
    let (_cluster, mut read_client) = setup_test_cluster_and_read_client().await;

    // Use a dummy object ID that doesn't exist
    let nonexistent_id = ObjectID::from_bytes([0u8; 32]).unwrap();

    let object_read = tokio::time::timeout(
        Duration::from_secs(30),
        read_client.get_object(nonexistent_id),
    )
    .await
    .expect("timeout waiting for object")
    .expect("ReadService get_object should work");

    // Verify ObjectRead::NotExists variant
    match object_read {
        ObjectRead::NotExists(object_id) => {
            assert_eq!(object_id, nonexistent_id, "Object ID should match");
        }
        _ => panic!("Expected ObjectRead::NotExists, got {object_read:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_read_service_contains_transaction_exists() {
    let (cluster, mut read_client) = setup_test_cluster_and_read_client().await;

    // Execute a transaction to get a real digest
    let sender = cluster.get_address_0();
    let receiver = cluster.get_address_1();
    let amount = 1000u64;

    let tx_data = cluster
        .test_transaction_builder_with_sender(sender)
        .await
        .transfer_iota(Some(amount), receiver)
        .build();

    // Sign and execute the transaction through the cluster
    let signed_tx = cluster.sign_transaction(&tx_data);
    let transaction_response = cluster.execute_transaction(signed_tx).await;
    let tx_digest = transaction_response.digest;

    // Test contains_transaction with existing transaction
    let exists = tokio::time::timeout(
        Duration::from_secs(30),
        read_client.contains_transaction(tx_digest),
    )
    .await
    .expect("timeout waiting for contains_transaction")
    .expect("contains_transaction should succeed");

    assert!(
        exists,
        "Transaction should exist after being executed: {tx_digest}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_read_service_contains_transaction_not_exists() {
    let (_cluster, mut read_client) = setup_test_cluster_and_read_client().await;

    // Use a dummy transaction digest that doesn't exist
    let nonexistent_digest = TransactionDigest::new([0u8; 32]);

    // Test contains_transaction with non-existent transaction
    let exists = tokio::time::timeout(
        Duration::from_secs(30),
        read_client.contains_transaction(nonexistent_digest),
    )
    .await
    .expect("timeout waiting for contains_transaction")
    .expect("contains_transaction should succeed");

    assert!(
        !exists,
        "Transaction should not exist: {nonexistent_digest}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_read_service_get_transaction_all_options_enabled() {
    let (cluster, mut read_client) = setup_test_cluster_and_read_client().await;

    // Execute a transaction to get a real digest
    let sender = cluster.get_address_0();
    let receiver = cluster.get_address_1();
    let amount = 1000u64;

    let tx_data = cluster
        .test_transaction_builder_with_sender(sender)
        .await
        .transfer_iota(Some(amount), receiver)
        .build();

    let signed_tx = cluster.sign_transaction(&tx_data);
    let transaction_response = cluster.execute_transaction(signed_tx).await;
    let tx_digest = transaction_response.digest;

    // Test get_transaction with all options enabled to verify all fields are
    // returned
    let response = tokio::time::timeout(
        Duration::from_secs(30),
        read_client.get_transaction(
            tx_digest,
            Some(TransactionResponseOptions {
                show_input: true,
                show_raw_input: true,
                show_effects: true,
                show_events: true,
                show_object_changes: true,
                show_balance_changes: true,
                show_raw_effects: true,
            }),
        ),
    )
    .await
    .expect("timeout waiting for get_transaction")
    .expect("get_transaction should succeed");

    // Validate the response
    assert!(response.digest.is_some(), "Response should have a digest");
    assert_eq!(
        response.digest.as_ref().unwrap().digest,
        tx_digest.into_inner().to_vec(),
        "Digest should match"
    );

    // Validate all requested fields are present
    assert!(
        response.transaction.is_some(),
        "Transaction should be present when show_input is true"
    );
    assert!(
        response.raw_transaction.is_some(),
        "Raw transaction should be present when show_raw_input is true"
    );
    assert!(
        response.effects.is_some(),
        "Effects should be present when show_effects is true"
    );
    // Events may be empty for simple transactions, but the field should exist
    assert!(
        response.events.is_some(),
        "Events field should be present when show_events is true"
    );
    // Object changes and balance changes should exist for transfer transactions
    assert!(
        !response.object_changes.is_empty(),
        "Object changes should be present for transfer transaction"
    );
    assert!(
        !response.balance_changes.is_empty(),
        "Balance changes should be present for transfer transaction"
    );
    assert!(
        response.raw_effects.is_some(),
        "Raw effects should be present when show_raw_effects is true"
    );
    assert!(
        response.checkpoint.is_some(),
        "Checkpoint should be present for executed transaction"
    );
    assert!(
        response.timestamp_ms.is_some(),
        "Timestamp should be present for executed transaction"
    );

    // Validate BCS deserialization
    let tx_data: iota_types::transaction::TransactionData =
        bcs::from_bytes(&response.transaction.unwrap().data)
            .expect("Should deserialize TransactionData from BCS");
    assert_eq!(tx_data.sender(), sender, "Sender should match");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_read_service_get_transaction_all_options_disabled() {
    let (cluster, mut read_client) = setup_test_cluster_and_read_client().await;

    // Execute a transaction to get a real digest
    let sender = cluster.get_address_0();
    let receiver = cluster.get_address_1();
    let amount = 1000u64;

    let tx_data = cluster
        .test_transaction_builder_with_sender(sender)
        .await
        .transfer_iota(Some(amount), receiver)
        .build();

    let signed_tx = cluster.sign_transaction(&tx_data);
    let transaction_response = cluster.execute_transaction(signed_tx).await;
    let tx_digest = transaction_response.digest;

    // Test get_transaction with all options disabled to verify optional fields are
    // not returned
    let response = tokio::time::timeout(
        Duration::from_secs(30),
        read_client.get_transaction(
            tx_digest,
            Some(TransactionResponseOptions {
                show_input: false,
                show_raw_input: false,
                show_effects: false,
                show_events: false,
                show_object_changes: false,
                show_balance_changes: false,
                show_raw_effects: false,
            }),
        ),
    )
    .await
    .expect("timeout waiting for get_transaction")
    .expect("get_transaction should succeed");

    // Validate only digest is present, everything else is None/empty
    assert!(response.digest.is_some(), "Digest should always be present");
    assert_eq!(
        response.digest.as_ref().unwrap().digest,
        tx_digest.into_inner().to_vec(),
        "Digest should match"
    );

    // All optional fields should be None/empty when options are false
    assert!(
        response.transaction.is_none(),
        "Transaction should be None when show_input is false"
    );
    assert!(
        response.raw_transaction.is_none(),
        "Raw transaction should be None when show_raw_input is false"
    );
    assert!(
        response.effects.is_none(),
        "Effects should be None when show_effects is false"
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

    // Checkpoint and timestamp should still be present
    assert!(
        response.checkpoint.is_some(),
        "Checkpoint should be present for executed transaction"
    );
    assert!(
        response.timestamp_ms.is_some(),
        "Timestamp should be present for executed transaction"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_read_service_get_transaction_not_found() {
    let (_cluster, mut read_client) = setup_test_cluster_and_read_client().await;

    // Use a non-existent transaction digest
    let nonexistent_digest = TransactionDigest::new([0u8; 32]);

    // Test get_transaction with non-existent digest
    let result = tokio::time::timeout(
        Duration::from_secs(30),
        read_client.get_transaction(
            nonexistent_digest,
            Some(TransactionResponseOptions {
                show_input: true,
                show_raw_input: false,
                show_effects: true,
                show_events: false,
                show_object_changes: false,
                show_balance_changes: false,
                show_raw_effects: false,
            }),
        ),
    )
    .await
    .expect("timeout waiting for get_transaction");

    // Should return NotFound error
    match result {
        Err(status) if status.code() == tonic::Code::NotFound => {
            // Expected - transaction not found
            assert!(
                status.message().contains("not found"),
                "Error message should mention 'not found'"
            );
        }
        Err(e) => panic!("Expected NotFound error, got: {e}"),
        Ok(_) => panic!("Expected NotFound error, but got success"),
    }
}
