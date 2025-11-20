// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use iota_grpc_types::v0::{
    bcs::BcsData,
    signatures::{UserSignature, UserSignatures},
    transaction::Transaction as ProtoTransaction,
    transaction_execution_service::{
        ExecuteTransactionRequest,
        transaction_execution_service_client::TransactionExecutionServiceClient,
    },
};
use iota_macros::sim_test;
use iota_test_transaction_builder::make_transfer_iota_transaction;
use prost_types::FieldMask;
use test_cluster::TestClusterBuilder;

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

    let response = client
        .execute_transaction(ExecuteTransactionRequest {
            transaction: Some(ProtoTransaction {
                bcs: Some(BcsData {
                    data: bcs::to_bytes(txn.transaction_data()).unwrap().into(),
                }),
                ..Default::default()
            }),
            signatures: Some(UserSignatures {
                signatures: txn
                    .tx_signatures()
                    .iter()
                    .map(|s| UserSignature {
                        bcs: Some(BcsData {
                            data: s.as_ref().to_vec().into(),
                        }),
                    })
                    .collect(),
            }),
            read_mask: Some(FieldMask {
                paths: vec!["*".to_string()],
            }),
        })
        .await
        .unwrap()
        .into_inner();

    let transaction = response.transaction.unwrap();

    // Verify we got effects
    assert!(transaction.effects.is_some());

    // Verify digest is present
    assert!(transaction.digest.is_some());
}
