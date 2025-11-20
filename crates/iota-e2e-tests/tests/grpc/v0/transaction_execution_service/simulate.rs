// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use iota_grpc_types::v0::{
    bcs::BcsData,
    transaction::Transaction as ProtoTransaction,
    transaction_execution_service::{
        SimulateTransactionRequest,
        transaction_execution_service_client::TransactionExecutionServiceClient,
    },
};
use iota_macros::sim_test;
use iota_types::transaction::{TransactionData, TransactionDataAPI};
use test_cluster::TestClusterBuilder;

#[sim_test]
async fn simulate_transaction_service_available() {
    let test_cluster = TestClusterBuilder::new()
        .with_fullnode_enable_grpc_api(true)
        .build()
        .await;

    // Wait for at least one checkpoint
    test_cluster.wait_for_checkpoint(1, None).await;

    // Test that we can connect to the TransactionExecutionService
    let client = TransactionExecutionServiceClient::connect(test_cluster.grpc_url()).await;

    // The service should be available
    assert!(
        client.is_ok(),
        "TransactionExecutionService should be available"
    );
}

#[sim_test]
async fn simulate_transaction_simple_transfer() {
    let test_cluster = TestClusterBuilder::new()
        .with_fullnode_enable_grpc_api(true)
        .build()
        .await;

    // Wait for at least one checkpoint
    test_cluster.wait_for_checkpoint(1, None).await;

    let mut client = TransactionExecutionServiceClient::connect(test_cluster.grpc_url())
        .await
        .unwrap();

    let recipient = iota_types::base_types::IotaAddress::random_for_testing_only();

    let (sender, mut gas) = test_cluster.wallet.get_one_account().await.unwrap();
    gas.sort_by_key(|object_ref| object_ref.0);
    let obj_to_send = gas.first().unwrap();
    let gas_obj = gas.last().unwrap();

    // Build a simple transfer transaction
    let tx_data = TransactionData::new_transfer(
        recipient,
        *obj_to_send,
        sender,
        *gas_obj,
        1_000_000, // gas budget
        1000,      // gas price
    );

    // Create the simulation request with BCS
    let transaction = ProtoTransaction {
        bcs: Some(BcsData {
            data: bcs::to_bytes(&tx_data).unwrap().into(),
        }),
        ..Default::default()
    };

    let request = SimulateTransactionRequest {
        transaction: Some(transaction),
        tx_checks: vec![],
        estimate_gas_budget: None,
        read_mask: None,
    };

    // Simulate the transaction
    let response = client
        .simulate_transaction(request)
        .await
        .unwrap()
        .into_inner();

    // Verify we got a response with effects
    let executed_transaction = response.transaction.unwrap();
    assert!(executed_transaction.effects.is_some());
}

#[sim_test]
async fn simulate_transaction_with_gas_estimation() {
    let test_cluster = TestClusterBuilder::new()
        .with_fullnode_enable_grpc_api(true)
        .build()
        .await;

    // Wait for at least one checkpoint
    test_cluster.wait_for_checkpoint(1, None).await;

    let mut client = TransactionExecutionServiceClient::connect(test_cluster.grpc_url())
        .await
        .unwrap();

    let recipient = iota_types::base_types::IotaAddress::random_for_testing_only();

    let (sender, mut gas) = test_cluster.wallet.get_one_account().await.unwrap();
    gas.sort_by_key(|object_ref| object_ref.0);
    let obj_to_send = gas.first().unwrap();
    let gas_obj = gas.last().unwrap();

    // Build a simple transfer transaction with a very high gas budget
    let tx_data = TransactionData::new_transfer(
        recipient,
        *obj_to_send,
        sender,
        *gas_obj,
        1_000_000_000, // very high gas budget
        1000,          // gas price
    );

    // Create the simulation request with gas estimation enabled
    let transaction = ProtoTransaction {
        bcs: Some(BcsData {
            data: bcs::to_bytes(&tx_data).unwrap().into(),
        }),
        ..Default::default()
    };

    let request = SimulateTransactionRequest {
        transaction: Some(transaction),
        tx_checks: vec![],
        estimate_gas_budget: Some(true),
        read_mask: None,
    };

    // Simulate the transaction
    let response = client
        .simulate_transaction(request)
        .await
        .unwrap()
        .into_inner();

    // Verify we got a response with effects
    let executed_transaction = response.transaction.unwrap();
    assert!(executed_transaction.effects.is_some());

    // Verify the returned transaction has an estimated budget
    // (it should be lower than the original 1_000_000_000)
    if let Some(tx_proto) = executed_transaction.transaction {
        if let Some(bcs_data) = tx_proto.bcs {
            let returned_tx: TransactionData = bcs::from_bytes(&bcs_data.data).unwrap();
            // The estimated budget should be much less than 1 billion
            assert!(returned_tx.gas_data().budget < 1_000_000_000);
            // But should be positive
            assert!(returned_tx.gas_data().budget > 0);
        }
    }
}
