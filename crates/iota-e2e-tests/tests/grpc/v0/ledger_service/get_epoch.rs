// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use iota_config::local_ip_utils;
use iota_grpc_types::{
    field::{FieldMask, FieldMaskUtil},
    v0::ledger_service::{GetEpochRequest, ledger_service_client::LedgerServiceClient},
};
use iota_macros::sim_test;
use test_cluster::TestClusterBuilder;

#[sim_test]
async fn get_epoch() {
    let localhost = local_ip_utils::localhost_for_testing();
    let grpc_port = local_ip_utils::get_available_port(&localhost);
    let grpc_addr = format!("{localhost}:{grpc_port}");

    let test_cluster = TestClusterBuilder::new()
        .with_fullnode_grpc_api_address(grpc_addr.parse().expect("Invalid gRPC address"))
        .build()
        .await;

    // Wait for at least one checkpoint to be created
    test_cluster.wait_for_checkpoint(1, None).await;

    let mut client = LedgerServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .unwrap();

    let latest_epoch = client
        .get_epoch(GetEpochRequest::latest().with_read_mask(FieldMask::from_str("*")))
        .await
        .unwrap()
        .into_inner()
        .epoch
        .unwrap();

    let epoch_0 = client
        .get_epoch(GetEpochRequest::new(0).with_read_mask(FieldMask::from_str("*")))
        .await
        .unwrap()
        .into_inner()
        .epoch
        .unwrap();

    assert_eq!(latest_epoch.committee, epoch_0.committee);

    assert_eq!(epoch_0.epoch, Some(0));
    assert_eq!(epoch_0.first_checkpoint, Some(0));

    // Ensure that fetching the system state for the epoch works
    let epoch = client
        .get_epoch(
            GetEpochRequest::latest().with_read_mask(FieldMask::from_paths(["bcs_system_state"])),
        )
        .await
        .unwrap()
        .into_inner()
        .epoch
        .unwrap();
    assert!(epoch.bcs_system_state.is_some());
}
