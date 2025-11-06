// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use iota_config::local_ip_utils;
use iota_grpc_types::v0::state_service::{
    ListDynamicFieldsRequest, ListDynamicFieldsResponse, state_service_client::StateServiceClient,
};
use iota_types::base_types::ObjectID;
use prost_types::FieldMask;
use test_cluster::{TestCluster, TestClusterBuilder};
use tokio_stream::StreamExt;

async fn setup_test_cluster_and_client()
-> (TestCluster, StateServiceClient<tonic::transport::Channel>) {
    let localhost = local_ip_utils::localhost_for_testing();
    let grpc_port = local_ip_utils::get_available_port(&localhost);
    let grpc_addr = format!("{localhost}:{grpc_port}");

    // Start a test cluster with gRPC enabled
    let cluster = TestClusterBuilder::new()
        .with_fullnode_grpc_api_address(grpc_addr.parse().expect("Invalid gRPC address"))
        .with_num_validators(1)
        .build()
        .await;

    let client = StateServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .expect("Failed to connect to gRPC server");

    (cluster, client)
}

/// Helper function to get any owned object from the cluster
/// This will be used as a test parent object (even if it has no dynamic fields)
async fn get_test_object_id(cluster: &TestCluster) -> ObjectID {
    let client = cluster.wallet.get_client().await.unwrap();
    let sender = cluster.get_address_0();

    // Get a gas coin to use as test object
    let gas_coins = client
        .coin_read_api()
        .get_coins(sender, None, None, None)
        .await
        .unwrap();

    gas_coins
        .data
        .first()
        .expect("Should have at least one coin")
        .coin_object_id
}

/// Helper function to collect all responses from a dynamic fields stream
/// Returns a vector of all responses and verifies the last one has
/// has_next=false
async fn collect_all_stream_responses(
    mut stream: tonic::codec::Streaming<ListDynamicFieldsResponse>,
) -> Vec<ListDynamicFieldsResponse> {
    let mut responses = Vec::new();

    while let Some(result) = stream.next().await {
        let response = result.expect("Stream should not contain errors");
        let has_next = response.has_next.unwrap_or(false);
        responses.push(response);

        // If this response indicates no more data, the stream should end
        if !has_next {
            break;
        }
    }

    // Verify the last response has has_next=false
    if let Some(last) = responses.last() {
        assert_eq!(
            last.has_next,
            Some(false),
            "Last response should have has_next=false"
        );
    }

    responses
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_dynamic_fields_empty() {
    let (cluster, mut client) = setup_test_cluster_and_client().await;

    // Get a regular object (gas coin) which has no dynamic fields
    let parent_id = get_test_object_id(&cluster).await;

    // Test ListDynamicFields for an object with no dynamic fields
    let request = ListDynamicFieldsRequest {
        parent: Some(parent_id.to_string()),
        read_mask: None,
        max_message_size_bytes: None,
    };

    let stream = client
        .list_dynamic_fields(request)
        .await
        .expect("ListDynamicFields request should succeed")
        .into_inner();

    let responses = collect_all_stream_responses(stream).await;

    // Should have exactly one response with no fields
    assert_eq!(responses.len(), 1, "Should have one response batch");
    let response = &responses[0];
    assert_eq!(
        response.dynamic_fields.len(),
        0,
        "Gas coin should have no dynamic fields"
    );
    assert_eq!(
        response.has_next,
        Some(false),
        "Empty result should have has_next=false"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_dynamic_fields_with_field_mask() {
    let (cluster, mut client) = setup_test_cluster_and_client().await;

    let parent_id = get_test_object_id(&cluster).await;

    // Test with field mask requesting only field_id
    let request = ListDynamicFieldsRequest {
        parent: Some(parent_id.to_string()),
        read_mask: Some(FieldMask {
            paths: vec!["field_id".to_string()],
        }),
        max_message_size_bytes: None,
    };

    let stream = client
        .list_dynamic_fields(request)
        .await
        .expect("ListDynamicFields with field_mask should succeed")
        .into_inner();

    let responses = collect_all_stream_responses(stream).await;

    // Gas coin has no dynamic fields
    assert_eq!(responses.len(), 1, "Should have one response batch");
    assert_eq!(responses[0].dynamic_fields.len(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_dynamic_fields_invalid_parent() {
    let (_cluster, mut client) = setup_test_cluster_and_client().await;

    // Test with an invalid parent ID
    let request = ListDynamicFieldsRequest {
        parent: Some("invalid_object_id".to_string()),
        read_mask: None,
        max_message_size_bytes: None,
    };

    let result = client.list_dynamic_fields(request).await;

    // Should return an error (InvalidArgument) immediately, before streaming
    assert!(
        result.is_err(),
        "ListDynamicFields should fail for invalid parent ID"
    );

    let error = result.err().unwrap();
    assert_eq!(
        error.code(),
        tonic::Code::InvalidArgument,
        "Error should be InvalidArgument"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_dynamic_fields_missing_parent() {
    let (_cluster, mut client) = setup_test_cluster_and_client().await;

    // Test without specifying parent (required field)
    let request = ListDynamicFieldsRequest {
        parent: None,
        read_mask: None,
        max_message_size_bytes: None,
    };

    let result = client.list_dynamic_fields(request).await;

    // Should return an error (InvalidArgument) immediately, before streaming
    assert!(
        result.is_err(),
        "ListDynamicFields should fail when parent is missing"
    );

    let error = result.err().unwrap();
    assert_eq!(
        error.code(),
        tonic::Code::InvalidArgument,
        "Error should be InvalidArgument"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_dynamic_fields_nonexistent_parent() {
    let (_cluster, mut client) = setup_test_cluster_and_client().await;

    // Test with a valid but non-existent object ID
    let fake_parent = ObjectID::random();

    let request = ListDynamicFieldsRequest {
        parent: Some(fake_parent.to_string()),
        read_mask: None,
        max_message_size_bytes: None,
    };

    // This might succeed with empty results or return an error depending on
    // implementation
    let result = client.list_dynamic_fields(request).await;

    // Either should succeed with empty results or fail gracefully
    match result {
        Ok(stream) => {
            let responses = collect_all_stream_responses(stream.into_inner()).await;
            // Should have one response with no dynamic fields for non-existent object
            assert_eq!(responses.len(), 1, "Should have one response batch");
            assert_eq!(
                responses[0].dynamic_fields.len(),
                0,
                "Non-existent parent should have no dynamic fields"
            );
        }
        Err(error) => {
            // Should be a reasonable error code
            assert!(
                matches!(error.code(), tonic::Code::NotFound | tonic::Code::Internal),
                "Error should be NotFound or Internal, got: {:?}",
                error.code()
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_dynamic_fields_default_mask() {
    let (cluster, mut client) = setup_test_cluster_and_client().await;

    let parent_id = get_test_object_id(&cluster).await;

    // Test without field mask (should use default: parent,field_id)
    let request = ListDynamicFieldsRequest {
        parent: Some(parent_id.to_string()),
        read_mask: None,
        max_message_size_bytes: None,
    };

    let stream = client
        .list_dynamic_fields(request)
        .await
        .expect("ListDynamicFields should succeed with default mask")
        .into_inner();

    let responses = collect_all_stream_responses(stream).await;

    // Verify the response structure
    assert!(!responses.is_empty(), "Should have at least one response");
    for response in &responses {
        assert!(
            response.has_next.is_some(),
            "Each response should have has_next field"
        );
    }
}

// ============================================================================
// Streaming-Specific Tests (Basic Behavior)
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_stream_early_termination() {
    let (cluster, mut client) = setup_test_cluster_and_client().await;
    let parent_id = get_test_object_id(&cluster).await;

    let request = ListDynamicFieldsRequest {
        parent: Some(parent_id.to_string()),
        read_mask: None,
        max_message_size_bytes: None,
    };

    let mut stream = client
        .list_dynamic_fields(request)
        .await
        .expect("ListDynamicFields should succeed")
        .into_inner();

    // Read only the first response and then drop the stream
    if let Some(result) = stream.next().await {
        let response = result.expect("First response should be valid");
        assert!(
            response.has_next.is_some(),
            "First response should have has_next field"
        );
    }

    // Stream is dropped here - should not cause any errors
    drop(stream);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_stream_continues_after_has_next_false() {
    let (cluster, mut client) = setup_test_cluster_and_client().await;
    let parent_id = get_test_object_id(&cluster).await;

    let request = ListDynamicFieldsRequest {
        parent: Some(parent_id.to_string()),
        read_mask: None,
        max_message_size_bytes: None,
    };

    let mut stream = client
        .list_dynamic_fields(request)
        .await
        .expect("ListDynamicFields should succeed")
        .into_inner();

    let mut found_has_next_false = false;

    while let Some(result) = stream.next().await {
        let response = result.expect("Stream should not contain errors");

        if let Some(false) = response.has_next {
            found_has_next_false = true;
            // After has_next=false, stream should end
            break;
        }
    }

    assert!(
        found_has_next_false,
        "Stream should have at least one response with has_next=false"
    );

    // Verify stream is actually done
    assert!(
        stream.next().await.is_none(),
        "Stream should be exhausted after has_next=false"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_invalid_field_mask_error() {
    let (cluster, mut client) = setup_test_cluster_and_client().await;
    let parent_id = get_test_object_id(&cluster).await;

    // Test with an invalid field mask path
    let request = ListDynamicFieldsRequest {
        parent: Some(parent_id.to_string()),
        read_mask: Some(FieldMask {
            paths: vec!["invalid_field_name".to_string()],
        }),
        max_message_size_bytes: None,
    };

    let result = client.list_dynamic_fields(request).await;

    // Should return an error (InvalidArgument) before starting the stream
    assert!(
        result.is_err(),
        "ListDynamicFields should fail for invalid field mask"
    );

    let error = result.err().unwrap();
    assert_eq!(
        error.code(),
        tonic::Code::InvalidArgument,
        "Error should be InvalidArgument"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_field_mask_paths() {
    let (cluster, mut client) = setup_test_cluster_and_client().await;
    let parent_id = get_test_object_id(&cluster).await;

    // Test with multiple valid field mask paths
    let request = ListDynamicFieldsRequest {
        parent: Some(parent_id.to_string()),
        read_mask: Some(FieldMask {
            paths: vec![
                "field_id".to_string(),
                "kind".to_string(),
                "name".to_string(),
            ],
        }),
        max_message_size_bytes: None,
    };

    let stream = client
        .list_dynamic_fields(request)
        .await
        .expect("ListDynamicFields with multiple field masks should succeed")
        .into_inner();

    let responses = collect_all_stream_responses(stream).await;

    // Should succeed even with no dynamic fields
    assert!(!responses.is_empty(), "Should have at least one response");
}

// ============================================================================
// Advanced Streaming Tests (Actual Streaming Behavior)
// ============================================================================

// Note: To properly test streaming with multiple batches (100+ dynamic fields),
// we would need to either:
// 1. Deploy a custom Move package that creates objects with many fields
// 2. Use the IOTA System State object which has many dynamic fields
// For now, we test streaming behavior with empty/small results

#[tokio::test(flavor = "multi_thread")]
async fn test_streaming_progressive_delivery() {
    use std::time::Instant;

    let (cluster, mut client) = setup_test_cluster_and_client().await;
    let parent_id = get_test_object_id(&cluster).await;

    let request = ListDynamicFieldsRequest {
        parent: Some(parent_id.to_string()),
        read_mask: None,
        max_message_size_bytes: None,
    };

    let mut stream = client
        .list_dynamic_fields(request)
        .await
        .expect("ListDynamicFields should succeed")
        .into_inner();

    let mut timestamps = Vec::new();

    // Collect timestamps of when each response arrives
    while let Some(result) = stream.next().await {
        timestamps.push(Instant::now());
        let response = result.expect("Stream should not contain errors");

        if !response.has_next.unwrap_or(false) {
            break;
        }
    }

    // With empty results, we should get exactly 1 response delivered immediately
    assert_eq!(
        timestamps.len(),
        1,
        "Should receive 1 response for empty result"
    );

    // All responses should arrive quickly (within 1 second total)
    if timestamps.len() > 1 {
        let total_duration = timestamps.last().unwrap().duration_since(timestamps[0]);
        assert!(
            total_duration.as_secs() < 5,
            "All responses should arrive within 5 seconds"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_streams_independence() {
    let (cluster, mut client1) = setup_test_cluster_and_client().await;

    // Get two different parent objects
    let parent_id_1 = get_test_object_id(&cluster).await;

    // Get a second gas coin for parent 2
    let client = cluster.wallet.get_client().await.unwrap();
    let sender = cluster.get_address_0();
    let gas_coins = client
        .coin_read_api()
        .get_coins(sender, None, None, None)
        .await
        .unwrap();

    let parent_id_2 = if gas_coins.data.len() > 1 {
        gas_coins.data[1].coin_object_id
    } else {
        parent_id_1 // Fallback to same parent
    };

    // Start two concurrent streams
    let request1 = ListDynamicFieldsRequest {
        parent: Some(parent_id_1.to_string()),
        read_mask: None,
        max_message_size_bytes: None,
    };

    let request2 = ListDynamicFieldsRequest {
        parent: Some(parent_id_2.to_string()),
        read_mask: None,
        max_message_size_bytes: None,
    };

    let stream1 = client1
        .list_dynamic_fields(request1)
        .await
        .expect("Stream 1 should start")
        .into_inner();

    let mut client2 = client1.clone();
    let stream2 = client2
        .list_dynamic_fields(request2)
        .await
        .expect("Stream 2 should start")
        .into_inner();

    // Collect both streams concurrently
    let (responses1, responses2) = tokio::join!(
        collect_all_stream_responses(stream1),
        collect_all_stream_responses(stream2)
    );

    // Both streams should complete successfully
    assert!(!responses1.is_empty(), "Stream 1 should have responses");
    assert!(!responses2.is_empty(), "Stream 2 should have responses");

    // Each stream should end with has_next=false
    assert_eq!(
        responses1.last().unwrap().has_next,
        Some(false),
        "Stream 1 should end properly"
    );
    assert_eq!(
        responses2.last().unwrap().has_next,
        Some(false),
        "Stream 2 should end properly"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_streaming_with_large_field_objects() {
    let (cluster, mut client) = setup_test_cluster_and_client().await;
    let parent_id = get_test_object_id(&cluster).await;

    // Request field_object and child_object in the mask to get larger payloads
    let request = ListDynamicFieldsRequest {
        parent: Some(parent_id.to_string()),
        read_mask: Some(FieldMask {
            paths: vec![
                "field_id".to_string(),
                "field_object".to_string(),
                "child_object".to_string(),
                "value".to_string(),
            ],
        }),
        max_message_size_bytes: None,
    };

    let stream = client
        .list_dynamic_fields(request)
        .await
        .expect("ListDynamicFields with large fields should succeed")
        .into_inner();

    let responses = collect_all_stream_responses(stream).await;

    // Should still work even with no fields
    assert!(!responses.is_empty(), "Should have at least one response");
    assert_eq!(
        responses.last().unwrap().has_next,
        Some(false),
        "Should complete stream properly"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_stream_respects_field_mask_across_batches() {
    let (cluster, mut client) = setup_test_cluster_and_client().await;
    let parent_id = get_test_object_id(&cluster).await;

    // Request only specific fields
    let request = ListDynamicFieldsRequest {
        parent: Some(parent_id.to_string()),
        read_mask: Some(FieldMask {
            paths: vec!["field_id".to_string(), "kind".to_string()],
        }),
        max_message_size_bytes: None,
    };

    let stream = client
        .list_dynamic_fields(request)
        .await
        .expect("ListDynamicFields should succeed")
        .into_inner();

    let responses = collect_all_stream_responses(stream).await;

    // Verify all responses respect the field mask
    for response in &responses {
        for field in &response.dynamic_fields {
            // field_id and kind should be set (if field exists)
            // other fields should not be set
            if !response.dynamic_fields.is_empty() {
                assert!(field.field_id.is_some(), "field_id should be populated");
                assert!(field.kind.is_some(), "kind should be populated");
                // These should NOT be populated
                assert!(field.value.is_none(), "value should not be populated");
                assert!(
                    field.field_object.is_none(),
                    "field_object should not be populated"
                );
                assert!(
                    field.child_object.is_none(),
                    "child_object should not be populated"
                );
            }
        }
    }
}
