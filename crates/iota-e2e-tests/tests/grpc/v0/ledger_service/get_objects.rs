// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use futures::StreamExt;
use iota_grpc_types::{
    field::{FieldMask, FieldMaskUtil},
    v0::{
        ledger_service::{
            GetObjectsRequest, GetObjectsResponse, ObjectRequest,
            ledger_service_client::LedgerServiceClient,
        },
        object::Object,
    },
};
use iota_macros::sim_test;
use iota_types::base_types::ObjectID;
use test_cluster::TestClusterBuilder;

#[sim_test]
async fn get_object() {
    let test_cluster = TestClusterBuilder::new()
        .with_fullnode_enable_grpc_api(true)
        .build()
        .await;

    let object_id = ObjectID::from_hex_literal("0x5").unwrap().to_string();

    let mut client = LedgerServiceClient::connect(test_cluster.grpc_url())
        .await
        .unwrap();

    // Request with no provided read_mask
    let Object { bcs, reference, .. } = {
        let mut stream = client
            .get_objects(GetObjectsRequest::new(vec![ObjectRequest::new(&object_id)]))
            .await
            .unwrap()
            .into_inner();

        stream.next().await.unwrap().unwrap().objects[0]
            .object()
            .clone()
    };

    // These fields default to being read
    assert_eq!(
        reference.as_ref().unwrap().object_id,
        Some(object_id.to_string())
    );
    assert!(reference.as_ref().unwrap().version.is_some());
    assert!(reference.as_ref().unwrap().digest.is_some());

    // while these fields default to not being read
    assert!(bcs.is_none());

    // Request with provided read_mask
    let Object { bcs, reference, .. } = {
        let mut stream = client
            .get_objects(
                GetObjectsRequest::new(vec![ObjectRequest::new(&object_id).with_version(1)])
                    .with_read_mask(FieldMask::from_str("reference.object_id,reference.version")),
            )
            .await
            .unwrap()
            .into_inner();

        stream.next().await.unwrap().unwrap().objects[0]
            .object()
            .clone()
    };

    assert_eq!(
        reference.as_ref().unwrap().object_id,
        Some(object_id.to_string())
    );
    assert_eq!(reference.as_ref().unwrap().version, Some(1));

    // These fields were not requested
    assert!(reference.as_ref().unwrap().digest.is_none());
    assert!(bcs.is_none());

    let response = {
        let mut stream = client
            .get_objects(
                GetObjectsRequest::new(vec![ObjectRequest::new(&object_id)]).with_read_mask(
                    FieldMask::from_paths([
                        "reference.object_id",
                        "reference.version",
                        "reference.digest",
                        "bcs",
                    ]),
                ),
            )
            .await
            .unwrap()
            .into_inner();

        stream.next().await.unwrap().unwrap().objects[0]
            .object()
            .clone()
    };

    let Object { bcs, reference, .. } = &response;

    assert!(reference.as_ref().unwrap().object_id.is_some());
    assert!(reference.as_ref().unwrap().version.is_some());
    assert!(reference.as_ref().unwrap().digest.is_some());
    assert!(bcs.is_some());
}

#[sim_test]
async fn batch_get_objects() {
    let test_cluster = TestClusterBuilder::new()
        .with_fullnode_enable_grpc_api(true)
        .build()
        .await;

    let mut client = LedgerServiceClient::connect(test_cluster.grpc_url())
        .await
        .unwrap();

    let GetObjectsResponse { objects, .. } = {
        let mut stream = client
            .get_objects(GetObjectsRequest::new(vec![
                ObjectRequest::new("0x1"),
                ObjectRequest::new("0x2"),
                ObjectRequest::new("0x3"),
            ]))
            .await
            .unwrap()
            .into_inner();

        stream.next().await.unwrap().unwrap()
    };

    assert_eq!(
        objects[0].object().reference.as_ref().unwrap().object_id,
        Some(ObjectID::from_hex_literal("0x1").unwrap().to_string())
    );
    assert_eq!(
        objects[1].object().reference.as_ref().unwrap().object_id,
        Some(ObjectID::from_hex_literal("0x2").unwrap().to_string())
    );
    assert_eq!(
        objects[2].object().reference.as_ref().unwrap().object_id,
        Some(ObjectID::from_hex_literal("0x3").unwrap().to_string())
    );
}
