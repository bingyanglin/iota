// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use iota_grpc_types::{
    field::FieldMaskUtil,
    v0::{
        bcs::BcsData,
        signatures::{UserSignature, UserSignatures},
        transaction::Transaction as ProtoTransaction,
        transaction_execution_service::{
            ExecuteTransactionRequest, ExecuteTransactionResponse,
            transaction_execution_service_client::TransactionExecutionServiceClient,
        },
    },
};
use iota_macros::sim_test;
use iota_test_transaction_builder::make_transfer_iota_transaction;
use prost_types::FieldMask;
use test_cluster::TestClusterBuilder;

use crate::{impl_field_presence_checker, utils::assert_field_presence};

// Generate the FieldPresenceChecker implementation for
// ExecuteTransactionResponse
impl_field_presence_checker!(ExecuteTransactionResponse, {
    "transaction" => transaction,
});

async fn assert_execute_transaction_request(
    client: &mut TransactionExecutionServiceClient<tonic::transport::Channel>,
    transaction: ProtoTransaction,
    signatures: UserSignatures,
    read_mask: Option<FieldMask>,
    expected_fields: &[&str],
    scenario: &str,
) -> ExecuteTransactionResponse {
    let response = client
        .execute_transaction(ExecuteTransactionRequest {
            transaction: Some(transaction),
            signatures: Some(signatures),
            read_mask,
        })
        .await
        .unwrap()
        .into_inner();

    assert_field_presence(&response, expected_fields, scenario);
    response
}

#[sim_test]
async fn execute_transaction_transfer() {
    let test_cluster = TestClusterBuilder::new()
        .with_fullnode_enable_grpc_api(true)
        .build()
        .await;

    let mut client = TransactionExecutionServiceClient::connect(test_cluster.grpc_url())
        .await
        .unwrap();

    let recipient = iota_types::base_types::IotaAddress::random_for_testing_only();
    let amount = 9;

    let txn =
        make_transfer_iota_transaction(&test_cluster.wallet, Some(recipient), Some(amount)).await;

    let transaction = ProtoTransaction {
        bcs: Some(BcsData {
            data: bcs::to_bytes(txn.transaction_data()).unwrap().into(),
        }),
        ..Default::default()
    };

    let signatures = UserSignatures {
        signatures: txn
            .tx_signatures()
            .iter()
            .map(|s| UserSignature {
                bcs: Some(BcsData {
                    data: s.as_ref().to_vec().into(),
                }),
            })
            .collect(),
    };

    let response = client
        .execute_transaction(ExecuteTransactionRequest {
            transaction: Some(transaction),
            signatures: Some(signatures),
            read_mask: Some(FieldMask {
                paths: vec!["*".to_string()],
            }),
        })
        .await
        .unwrap()
        .into_inner();

    let executed_tx = response.transaction.unwrap();

    // Verify we got effects
    assert!(executed_tx.effects.is_some());

    // Verify digest is present
    assert!(executed_tx.digest.is_some());
}

#[sim_test]
async fn execute_transaction_readmask_scenarios() {
    let test_cluster = TestClusterBuilder::new()
        .with_fullnode_enable_grpc_api(true)
        .build()
        .await;

    let mut client = TransactionExecutionServiceClient::connect(test_cluster.grpc_url())
        .await
        .unwrap();

    let recipient = iota_types::base_types::IotaAddress::random_for_testing_only();
    let amount = 9;

    let txn =
        make_transfer_iota_transaction(&test_cluster.wallet, Some(recipient), Some(amount)).await;

    let create_transaction = || ProtoTransaction {
        bcs: Some(BcsData {
            data: bcs::to_bytes(txn.transaction_data()).unwrap().into(),
        }),
        ..Default::default()
    };

    let create_signatures = || UserSignatures {
        signatures: txn
            .tx_signatures()
            .iter()
            .map(|s| UserSignature {
                bcs: Some(BcsData {
                    data: s.as_ref().to_vec().into(),
                }),
            })
            .collect(),
    };

    // Test 1: Default readmask (None) - uses default mask "transaction.effects"
    assert_execute_transaction_request(
        &mut client,
        create_transaction(),
        create_signatures(),
        None,
        &["transaction"],
        "default readmask",
    )
    .await;

    // Test 2: Empty readmask - no fields are returned
    assert_execute_transaction_request(
        &mut client,
        create_transaction(),
        create_signatures(),
        Some(FieldMask::from_paths(&[] as &[&str])),
        &[],
        "empty readmask",
    )
    .await;

    // Test 3: Full readmask should return transaction field
    assert_execute_transaction_request(
        &mut client,
        create_transaction(),
        create_signatures(),
        Some(FieldMask::from_paths(["transaction"])),
        &["transaction"],
        "full readmask",
    )
    .await;

    // Test 4: Wildcard should return transaction field
    assert_execute_transaction_request(
        &mut client,
        create_transaction(),
        create_signatures(),
        Some(FieldMask::from_paths(["*"])),
        &["transaction"],
        "wildcard readmask",
    )
    .await;
}
