// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use futures::StreamExt;
use iota_grpc_types::{
    field::{FieldMask, FieldMaskUtil},
    v0::ledger_service::{
        GetTransactionsRequest, TransactionRequest, ledger_service_client::LedgerServiceClient,
    },
};
use iota_macros::sim_test;
use iota_test_transaction_builder::TestTransactionBuilder;
use test_cluster::TestClusterBuilder;

#[sim_test]
async fn get_transactions() {
    let test_cluster = TestClusterBuilder::new()
        .with_fullnode_enable_grpc_api(true)
        .build()
        .await;

    test_cluster.wait_for_checkpoint(1, None).await;

    // Create a test transaction to have something to query
    let (sender, gas) = test_cluster
        .wallet
        .get_one_gas_object()
        .await
        .unwrap()
        .unwrap();
    let rgp = test_cluster.get_reference_gas_price().await;
    let transaction_data = TestTransactionBuilder::new(sender, gas, rgp)
        .transfer_iota(None, sender)
        .build();
    let signed_transaction = test_cluster.wallet.sign_transaction(&transaction_data);
    let transaction_digest = *signed_transaction.digest();
    test_cluster
        .wallet
        .execute_transaction_may_fail(signed_transaction)
        .await
        .unwrap();

    let mut client = LedgerServiceClient::connect(test_cluster.grpc_url())
        .await
        .unwrap();

    // Test 1: Request with no provided read_mask (should default to "digest")
    let mut stream = client
        .get_transactions(GetTransactionsRequest::new(vec![TransactionRequest::new(
            transaction_digest.inner().to_vec(),
        )]))
        .await
        .unwrap()
        .into_inner();

    let response = stream.next().await.unwrap().unwrap();
    let transaction = response.transactions[0].transaction();

    // These fields default to being read
    assert_eq!(
        transaction.digest.as_ref().map(|d| d.digest.as_ref()),
        Some(transaction_digest.inner().as_ref())
    );

    // while these fields default to not being read
    assert!(transaction.transaction.is_none());
    assert!(transaction.signatures.is_none());
    assert!(transaction.effects.is_none());
    assert!(transaction.events.is_none());
    assert!(transaction.checkpoint.is_none());
    assert!(transaction.timestamp.is_none());

    // Ensure stream is complete
    assert!(stream.next().await.is_none());

    // Test 2: Request all fields
    let mut stream = client
        .get_transactions(
            GetTransactionsRequest::new(vec![TransactionRequest::new(
                transaction_digest.inner().to_vec(),
            )])
            .with_read_mask(FieldMask::from_paths([
                "digest",
                "transaction",
                "signatures",
                "effects",
                "events",
                "checkpoint",
                "timestamp",
            ])),
        )
        .await
        .unwrap()
        .into_inner();

    let response = stream.next().await.unwrap().unwrap();
    let transaction = response.transactions[0].transaction();

    assert_eq!(
        transaction.digest.as_ref().map(|d| d.digest.as_ref()),
        Some(transaction_digest.inner().as_ref())
    );
    assert!(transaction.transaction.is_some());
    assert!(transaction.signatures.is_some());
    assert!(transaction.effects.is_some());
    // Note: events may be None if the transaction doesn't emit any events
    assert!(transaction.checkpoint.is_some());
    assert!(transaction.timestamp.is_some());

    // Ensure stream is complete
    assert!(stream.next().await.is_none());
}
