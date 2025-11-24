// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use futures::StreamExt;
use iota_grpc_types::{
    field::FieldMaskUtil,
    v0::{
        ledger_service::{
            GetObjectsRequest, GetObjectsResponse, ObjectRequest, ObjectRequests,
            ledger_service_client::LedgerServiceClient,
        },
        object::Object,
        types::ObjectReference,
    },
};
use iota_macros::sim_test;
use iota_types::base_types::ObjectID;
use prost_types::FieldMask;
use test_cluster::TestClusterBuilder;

use crate::{impl_field_presence_checker, utils::assert_field_presence};

// Generate the FieldPresenceChecker implementation for Object
impl_field_presence_checker!(Object, {
    "reference" => reference,
    "bcs" => bcs,
});

// Generate the FieldPresenceChecker implementation for ObjectReference
impl_field_presence_checker!(ObjectReference, {
    "object_id" => object_id,
    "version" => version,
    "digest" => digest,
});

/// Helper function to assert object fields based on expected fields
fn assert_object_fields(
    object: &Object,
    expected_top_level: &[&str],
    expected_reference: &[&str],
    scenario: &str,
) {
    // Check top-level fields
    assert_field_presence(object, expected_top_level, scenario);

    // If reference is expected, check nested fields
    if expected_top_level.contains(&"reference") {
        if let Some(ref reference) = object.reference {
            assert_field_presence(reference, expected_reference, scenario);
        } else {
            panic!("Expected reference field in {scenario}, but it was None");
        }
    }
}

async fn assert_get_objects_request(
    client: &mut LedgerServiceClient<tonic::transport::Channel>,
    requests: Vec<ObjectRequest>,
    read_mask: Option<FieldMask>,
    max_message_size_bytes: Option<u32>,
    expected_top_level: &[&str],
    expected_reference: &[&str],
    expected_has_next: bool,
    scenario: &str,
) -> GetObjectsResponse {
    let request = GetObjectsRequest {
        requests: Some(ObjectRequests { requests }),
        read_mask,
        max_message_size_bytes,
    };

    let mut stream = client.get_objects(request).await.unwrap().into_inner();

    let response = stream.next().await.unwrap().unwrap();

    // Assert all returned objects have the expected fields
    for (idx, obj_result) in response.objects.iter().enumerate() {
        let object = obj_result.object();
        assert_object_fields(
            object,
            expected_top_level,
            expected_reference,
            &format!("{scenario} (object {idx})"),
        );
    }

    // Verify has_next value
    assert_eq!(
        response.has_next, expected_has_next,
        "{scenario}: has_next should be {expected_has_next}"
    );

    // If has_next is false, verify stream is exhausted
    if !expected_has_next {
        assert!(
            stream.next().await.is_none(),
            "{scenario}: stream should be exhausted when has_next is false"
        );
    }

    response
}

#[sim_test]
async fn get_objects_readmask_scenarios() {
    let test_cluster = TestClusterBuilder::new()
        .with_fullnode_enable_grpc_api(true)
        .build()
        .await;

    let mut grpc_client = LedgerServiceClient::connect(test_cluster.grpc_url())
        .await
        .unwrap();

    let object_id = ObjectID::from_hex_literal("0x5").unwrap().to_string();

    // Table-driven tests for single-object readmask scenarios
    type TestCase<'a> = (&'a str, Option<FieldMask>, &'a [&'a str], &'a [&'a str]);
    let test_cases: Vec<TestCase> = vec![
        (
            "default readmask",
            None,
            &["reference"],
            &["object_id", "version", "digest"],
        ),
        (
            "empty readmask",
            Some(FieldMask::from_paths(&[] as &[&str])),
            &[],
            &[],
        ),
        (
            "full readmask",
            Some(FieldMask::from_paths([
                "reference.object_id",
                "reference.version",
                "reference.digest",
                "bcs",
            ])),
            &["reference", "bcs"],
            &["object_id", "version", "digest"],
        ),
        (
            "partial readmask (reference fields only)",
            Some(FieldMask::from_paths([
                "reference.object_id",
                "reference.version",
            ])),
            &["reference"],
            &["object_id", "version"],
        ),
        (
            "partial readmask (bcs only)",
            Some(FieldMask::from_paths(["bcs"])),
            &["bcs"],
            &[],
        ),
    ];

    for (scenario, mask, expected_top, expected_ref) in test_cases {
        let response = assert_get_objects_request(
            &mut grpc_client,
            vec![ObjectRequest {
                object_ref: Some(ObjectReference {
                    object_id: Some(object_id.clone()),
                    version: None,
                    digest: None,
                }),
            }],
            mask,
            None,
            expected_top,
            expected_ref,
            false,
            scenario,
        )
        .await;

        assert_eq!(response.objects.len(), 1, "{scenario}: expected 1 object");
    }
}

#[sim_test]
async fn get_objects_batch() {
    let test_cluster = TestClusterBuilder::new()
        .with_fullnode_enable_grpc_api(true)
        .build()
        .await;

    let mut grpc_client = LedgerServiceClient::connect(test_cluster.grpc_url())
        .await
        .unwrap();

    // Test batch request with multiple objects and partial readmask
    let response = assert_get_objects_request(
        &mut grpc_client,
        vec![
            ObjectRequest {
                object_ref: Some(ObjectReference {
                    object_id: Some("0x1".to_string()),
                    version: None,
                    digest: None,
                }),
            },
            ObjectRequest {
                object_ref: Some(ObjectReference {
                    object_id: Some("0x2".to_string()),
                    version: None,
                    digest: None,
                }),
            },
            ObjectRequest {
                object_ref: Some(ObjectReference {
                    object_id: Some("0x3".to_string()),
                    version: None,
                    digest: None,
                }),
            },
            ObjectRequest {
                object_ref: Some(ObjectReference {
                    object_id: Some("0x5".to_string()),
                    version: None,
                    digest: None,
                }),
            },
        ],
        Some(FieldMask::from_paths(["reference.object_id", "bcs"])),
        None,
        &["reference", "bcs"],
        &["object_id"],
        false,
        "batch with 4 objects",
    )
    .await;

    assert_eq!(response.objects.len(), 4);
}

#[sim_test]
async fn get_objects_with_version() {
    let test_cluster = TestClusterBuilder::new()
        .with_fullnode_enable_grpc_api(true)
        .build()
        .await;

    let mut grpc_client = LedgerServiceClient::connect(test_cluster.grpc_url())
        .await
        .unwrap();

    let object_id = ObjectID::from_hex_literal("0x5").unwrap().to_string();

    // Request specific version
    assert_get_objects_request(
        &mut grpc_client,
        vec![ObjectRequest {
            object_ref: Some(ObjectReference {
                object_id: Some(object_id),
                version: Some(1),
                digest: None,
            }),
        }],
        Some(FieldMask::from_paths([
            "reference.object_id",
            "reference.version",
        ])),
        None,
        &["reference"],
        &["object_id", "version"],
        false,
        "specific version query",
    )
    .await;
}

#[sim_test]
async fn get_objects_streaming() {
    let test_cluster = TestClusterBuilder::new()
        .with_fullnode_enable_grpc_api(true)
        .build()
        .await;

    let mut grpc_client = LedgerServiceClient::connect(test_cluster.grpc_url())
        .await
        .unwrap();

    // Test streaming by requesting many objects with full readmask
    // Use only known-to-exist objects (0x1-0x6 commonly exist in test cluster)
    // but repeat them multiple times to ensure we have enough data for potential
    // multi-message streaming
    let mut requests = Vec::new();
    let known_objects = ["0x1", "0x2", "0x3", "0x5", "0x6"];

    // Request each object 20 times to create a larger payload (100 total objects,
    // around 2-3 MB)
    for _ in 0..20 {
        for obj_id in &known_objects {
            requests.push(ObjectRequest {
                object_ref: Some(ObjectReference {
                    object_id: Some(obj_id.to_string()),
                    version: None,
                    digest: None,
                }),
            });
        }
    }

    let request = GetObjectsRequest {
        requests: Some(ObjectRequests { requests }),
        read_mask: Some(FieldMask::from_paths([
            "reference.object_id",
            "reference.version",
            "reference.digest",
            "bcs",
        ])),
        // Use minimum allowed message size to maximize chance of streaming
        max_message_size_bytes: Some(1024 * 1024_u32), // 1MB (minimum allowed)
    };

    let mut stream = grpc_client.get_objects(request).await.unwrap().into_inner();

    let mut total_results = 0;
    let mut successful_objects = 0;
    let mut response_count = 0;
    let mut last_has_next = false;
    let mut has_next_values = Vec::new();

    while let Some(response) = stream.next().await {
        let response = response.unwrap();
        response_count += 1;
        total_results += response.objects.len();
        last_has_next = response.has_next;
        has_next_values.push(response.has_next);

        // Each response should have at least one result
        assert!(
            !response.objects.is_empty(),
            "Each stream message should contain at least one result"
        );

        // Validate that each successful object has the expected fields
        for obj_result in &response.objects {
            // Only validate successful objects (skip errors for non-existent objects)
            if let Some(iota_grpc_types::v0::ledger_service::object_result::Result::Object(
                object,
            )) = obj_result.result.as_ref()
            {
                successful_objects += 1;
                assert!(object.reference.is_some(), "object should have reference");
                assert!(object.bcs.is_some(), "object should have bcs");
                let reference = object.reference.as_ref().unwrap();
                assert!(
                    reference.object_id.is_some(),
                    "object should have object_id"
                );
                assert!(reference.version.is_some(), "object should have version");
                assert!(reference.digest.is_some(), "object should have digest");
            }
        }
    }

    // Validate has_next values: all intermediate messages should have has_next=true
    if response_count > 1 {
        for (idx, &has_next) in has_next_values[..has_next_values.len() - 1]
            .iter()
            .enumerate()
        {
            assert!(
                has_next,
                "Intermediate stream message #{} should have has_next=true, but got false",
                idx + 1
            );
        }
    }

    // Verify we got all 100 results (even if some are errors)
    assert_eq!(
        total_results, 100,
        "Should have received 100 results (objects or errors)"
    );

    // Verify we got at least some successful objects
    assert!(
        successful_objects > 0,
        "Should have at least some successful objects"
    );

    // Verify multi-message streaming occurred (more than 1 stream message)
    assert!(
        response_count > 1,
        "Expected multi-message streaming (>1 message), but got only {} message(s)",
        response_count
    );

    // Verify the last response had has_next=false
    assert!(
        !last_has_next,
        "Last stream message should have has_next=false"
    );
}

#[sim_test]
async fn get_objects_empty_request() {
    let test_cluster = TestClusterBuilder::new()
        .with_fullnode_enable_grpc_api(true)
        .build()
        .await;

    let mut grpc_client = LedgerServiceClient::connect(test_cluster.grpc_url())
        .await
        .unwrap();

    // Test empty request list
    let request = GetObjectsRequest {
        requests: Some(ObjectRequests { requests: vec![] }),
        read_mask: None,
        max_message_size_bytes: None,
    };

    let mut stream = grpc_client.get_objects(request).await.unwrap().into_inner();

    let response = stream.next().await.unwrap().unwrap();

    // Should return empty response with has_next=false
    assert_eq!(response.objects.len(), 0, "Should have 0 objects");
    assert!(
        !response.has_next,
        "has_next should be false for empty request"
    );

    // Stream should be exhausted
    assert!(
        stream.next().await.is_none(),
        "Stream should be exhausted after empty response"
    );
}
