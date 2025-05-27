use std::{
    collections::BTreeMap,
    // collections::BTreeMap, // Unused
    net::{SocketAddr, TcpListener},
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
};

use futures::StreamExt;
use iota_gprc_api::{
    proto::iota::gprc::v1::{
        Direction,
        GetCheckpointRequest,
        ListCheckpointsRequest,
        StreamCheckpointsInRangeRequest,
        // StreamedCheckpoint, // Removed this unused import
        SubscribeNewCheckpointsRequest,
        checkpoint_gprc_service_client::CheckpointGprcServiceClient,
        streamed_checkpoint::CheckpointType,
    },
    server::{GrpcServer, StateReader},
};
use iota_types::{
    base_types::{
        AuthorityName, IotaAddress, MoveObjectType, ObjectID, SequenceNumber, TransactionDigest,
    },
    committee::{Committee, EpochId, StakeUnit, TOTAL_VOTING_POWER},
    crypto::{AuthorityKeyPair, AuthoritySignInfo, AuthorityStrongQuorumSignInfo, KeypairTraits},
    digests::{
        ChainIdentifier, CheckpointContentsDigest, CheckpointDigest, TransactionEventsDigest,
    },
    effects::{TransactionEffects, TransactionEvents},
    full_checkpoint_content::CheckpointData,
    gas::GasCostSummary,
    message_envelope::{Envelope, Message, VerifiedEnvelope},
    messages_checkpoint::{
        CheckpointContents, CheckpointSequenceNumber, CheckpointSummary,
        FullCheckpointContents as ActualFullCheckpointContents, VerifiedCheckpoint,
    },
    object::{Data, MoveObject, Object, ObjectInner, Owner},
    storage::{
        AccountOwnedObjectInfo, CoinInfo, DynamicFieldIndexInfo, DynamicFieldKey,
        ObjectStore as ActualObjectStore, ReadStore as ActualReadStore,
        RestStateReader as ActualRestStateReader, error::Result as StorageResult,
    },
    transaction::VerifiedTransaction,
};
// use move_core_types::language_storage::{StructTag, TypeTag}; // TypeTag unused
use move_core_types::language_storage::StructTag; // Kept StructTag
use rand::thread_rng;
use shared_crypto::intent::Intent; // Removed IntentMessage, IntentScope
use tokio::{
    sync::broadcast,
    time::{Duration, sleep, timeout},
}; // Added for to_intent_message // Import for key generation

fn get_available_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

// Mock implementation for RestStateReader for testing
struct MockRestStateReader {
    latest_sequence_number: Arc<AtomicU64>,
}

impl Default for MockRestStateReader {
    fn default() -> Self {
        Self {
            // Initialize with a low sequence number so tests can advance it.
            latest_sequence_number: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl MockRestStateReader {
    // Helper for tests to advance the mock's latest sequence number
    fn set_latest_sequence_number_for_test(&self, val: u64) {
        self.latest_sequence_number.store(val, Ordering::SeqCst);
    }
}

async fn spawn_test_server() -> (
    CheckpointGprcServiceClient<tonic::transport::Channel>,
    SocketAddr,
    broadcast::Sender<()>,
    Arc<MockRestStateReader>,
) {
    let port = get_available_port();
    let addr: SocketAddr = format!("[::1]:{}", port).parse().unwrap();

    let mock_state_reader = Arc::new(MockRestStateReader::default());
    let state_reader_for_server: StateReader = mock_state_reader.clone(); // Type erase for GrpcServer

    let (shutdown_tx, _shutdown_rx) = broadcast::channel(1);
    let server_shutdown_rx = shutdown_tx.subscribe();

    let grpc_server = GrpcServer::new(addr, state_reader_for_server);

    tokio::spawn(async move {
        if let Err(e) = grpc_server.start(server_shutdown_rx).await {
            eprintln!("Test gRPC server failed: {}", e);
        }
    });

    // Brief pause to allow the server to start
    sleep(Duration::from_millis(200)).await;

    let channel = tonic::transport::Channel::from_shared(format!("http://[::1]:{}", port))
        .unwrap()
        .connect_timeout(Duration::from_secs(1))
        .connect()
        .await
        .expect("Failed to connect to test gRPC server");

    let client = CheckpointGprcServiceClient::new(channel);
    (client, addr, shutdown_tx, mock_state_reader)
}

#[tokio::test]
async fn test_get_checkpoint_full_dummy() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) = spawn_test_server().await;

    let request_id_str = "999";
    let request_seq_num = request_id_str.parse::<u64>().unwrap();

    let request = tonic::Request::new(GetCheckpointRequest {
        checkpoint_id: request_id_str.to_string(),
    });

    let response = client.get_checkpoint_full(request).await;
    assert!(
        response.is_ok(),
        "GetCheckpointFull RPC call failed: {:?}",
        response.err()
    );
    let response_data_gprc = response.unwrap().into_inner();

    assert!(
        response_data_gprc.summary.is_some(),
        "Response summary should not be None"
    );
    let summary_gprc = response_data_gprc.summary.unwrap();

    // Create mock core data to compare against
    // The service internally calls get_checkpoint_by_sequence_number, which uses
    // create_mock_verified_checkpoint
    let mock_core_verified_checkpoint = create_mock_verified_checkpoint(request_seq_num, 1); // Assuming epoch 1 for test consistency
    let mock_core_summary_data = mock_core_verified_checkpoint.data(); // This is CheckpointSummary

    assert_eq!(
        summary_gprc.sequence_number,
        mock_core_summary_data.sequence_number
    );
    assert_eq!(summary_gprc.epoch, mock_core_summary_data.epoch);
    assert_eq!(
        summary_gprc.network_total_transactions,
        mock_core_summary_data.network_total_transactions
    );
    assert_eq!(
        summary_gprc.content_digest.as_ref().unwrap().digest, // gRPC digest
        mock_core_summary_data.content_digest.into_inner().to_vec()  // Core digest
    );

    if mock_core_summary_data.previous_digest.is_some() {
        assert!(
            summary_gprc.previous_digest.is_some(),
            "gRPC previous_digest should be Some"
        );
        assert_eq!(
            summary_gprc.previous_digest.as_ref().unwrap().digest,
            mock_core_summary_data
                .previous_digest
                .unwrap()
                .into_inner()
                .to_vec()
        );
    } else {
        assert!(
            summary_gprc.previous_digest.is_none(),
            "gRPC previous_digest should be None"
        );
    }

    // Check transactions (expecting empty from mock implementation of
    // get_checkpoint_data)
    assert!(
        response_data_gprc.transactions.is_empty(),
        "Expected empty transactions list from mock GetCheckpointFull, got: {:?}",
        response_data_gprc.transactions
    );

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_get_checkpoint_dummy() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) = spawn_test_server().await;

    let request_seq_num = 789u64;
    let request = tonic::Request::new(GetCheckpointRequest {
        checkpoint_id: request_seq_num.to_string(),
    });

    let response = client.get_checkpoint(request).await;
    assert!(
        response.is_ok(),
        "GetCheckpoint RPC call failed: {:?}",
        response.err()
    );
    let summary = response.unwrap().into_inner();

    // Compare with mock data generated correctly
    let mock_checkpoint = create_mock_verified_checkpoint(request_seq_num, 1); // Assuming epoch 1 for test consistency
    let mock_summary_data = mock_checkpoint.data(); // Get the inner CheckpointSummary

    assert_eq!(summary.sequence_number, mock_summary_data.sequence_number);
    assert_eq!(summary.epoch, mock_summary_data.epoch);
    assert_eq!(
        summary.network_total_transactions,
        mock_summary_data.network_total_transactions
    );
    assert_eq!(
        summary.content_digest.unwrap().digest,
        mock_summary_data.content_digest.into_inner().to_vec()
    );
    if mock_summary_data.previous_digest.is_some() {
        assert!(
            summary.previous_digest.is_some(),
            "gRPC previous_digest should be Some"
        );
        assert_eq!(
            summary.previous_digest.unwrap().digest,
            mock_summary_data
                .previous_digest
                .unwrap()
                .into_inner()
                .to_vec()
        );
    } else {
        assert!(
            summary.previous_digest.is_none(),
            "gRPC previous_digest should be None"
        );
    }

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_list_checkpoints_no_end_seq() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) = spawn_test_server().await;
    let request_limit = 3u32;
    let start_seq_num_str = "200".to_string();
    let request = tonic::Request::new(ListCheckpointsRequest {
        limit: Some(request_limit),
        start_sequence_number: Some(start_seq_num_str.clone()),
        end_sequence_number: None, // Explicitly None
        direction: Some(Direction::Ascending as i32),
    });

    let response = client.list_checkpoints(request).await;
    assert!(
        response.is_ok(),
        "ListCheckpoints RPC call failed: {:?}",
        response.err()
    );
    let page = response.unwrap().into_inner();

    assert_eq!(
        page.checkpoints.len(),
        request_limit as usize,
        "Incorrect number of checkpoints returned"
    );
    let start_seq_num = start_seq_num_str.parse::<u64>().unwrap();
    for (i, checkpoint) in page.checkpoints.iter().enumerate() {
        assert_eq!(checkpoint.sequence_number, start_seq_num + i as u64);
    }
    assert!(page.next_cursor.is_some());
    assert_eq!(
        page.next_cursor.unwrap(),
        (start_seq_num + request_limit as u64).to_string()
    );

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_list_checkpoints_with_end_seq_within_limit() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) = spawn_test_server().await;
    let request_limit = 10u32; // Limit is larger than the range
    let start_seq_num_str = "100".to_string();
    let end_seq_num_str = "102".to_string(); // Range of 3 items (100, 101, 102)
    let request = tonic::Request::new(ListCheckpointsRequest {
        limit: Some(request_limit),
        start_sequence_number: Some(start_seq_num_str.clone()),
        end_sequence_number: Some(end_seq_num_str.clone()),
        direction: Some(Direction::Ascending as i32),
    });

    let response = client.list_checkpoints(request).await;
    assert!(
        response.is_ok(),
        "ListCheckpoints RPC call failed: {:?}",
        response.err()
    );
    let page = response.unwrap().into_inner();

    assert_eq!(
        page.checkpoints.len(),
        3,
        "Should fetch 3 checkpoints (100, 101, 102)"
    );
    let start_seq_num = start_seq_num_str.parse::<u64>().unwrap();
    for (i, checkpoint) in page.checkpoints.iter().enumerate() {
        assert_eq!(checkpoint.sequence_number, start_seq_num + i as u64);
    }
    assert!(page.next_cursor.is_none());
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_list_checkpoints_with_end_seq_beyond_limit() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) = spawn_test_server().await;
    let request_limit = 2u32; // Limit is smaller than the range
    let start_seq_num_str = "50".to_string();
    let end_seq_num_str = "55".to_string(); // Range 50-55
    let request = tonic::Request::new(ListCheckpointsRequest {
        limit: Some(request_limit),
        start_sequence_number: Some(start_seq_num_str.clone()),
        end_sequence_number: Some(end_seq_num_str.clone()),
        direction: Some(Direction::Ascending as i32),
    });

    let response = client.list_checkpoints(request).await;
    assert!(
        response.is_ok(),
        "ListCheckpoints RPC call failed: {:?}",
        response.err()
    );
    let page = response.unwrap().into_inner();

    assert_eq!(
        page.checkpoints.len(),
        request_limit as usize,
        "Should fetch up to the limit"
    );
    let start_seq_num = start_seq_num_str.parse::<u64>().unwrap();
    assert_eq!(page.checkpoints[0].sequence_number, start_seq_num);
    assert_eq!(page.checkpoints[1].sequence_number, start_seq_num + 1);
    assert!(page.next_cursor.is_some());
    assert_eq!(
        page.next_cursor.unwrap(),
        (start_seq_num + request_limit as u64).to_string()
    );
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_list_checkpoints_end_seq_equals_start_seq() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) = spawn_test_server().await;
    let start_seq_num_str = "300".to_string();
    let request = tonic::Request::new(ListCheckpointsRequest {
        limit: Some(5), // Limit greater than 1
        start_sequence_number: Some(start_seq_num_str.clone()),
        end_sequence_number: Some(start_seq_num_str.clone()), // end == start
        direction: Some(Direction::Ascending as i32),
    });
    let response = client.list_checkpoints(request).await.unwrap().into_inner();
    assert_eq!(
        response.checkpoints.len(),
        1,
        "Should fetch exactly one checkpoint"
    );
    assert_eq!(
        response.checkpoints[0].sequence_number,
        start_seq_num_str.parse::<u64>().unwrap()
    );
    assert!(response.next_cursor.is_none());
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_list_checkpoints_invalid_end_less_than_start() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) = spawn_test_server().await;
    let request = tonic::Request::new(ListCheckpointsRequest {
        limit: Some(5),
        start_sequence_number: Some("100".to_string()),
        end_sequence_number: Some("99".to_string()), // end < start
        direction: Some(Direction::Ascending as i32),
    });
    let response = client.list_checkpoints(request).await;
    assert!(
        response.is_err(),
        "Expected an error for end_sequence_number < start_sequence_number"
    );
    let status = response.err().unwrap();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(
        status
            .message()
            .contains("end_sequence_number cannot be less than start_sequence_number")
    );
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_list_checkpoints_missing_start_seq() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) = spawn_test_server().await;
    let request = tonic::Request::new(ListCheckpointsRequest {
        limit: Some(5),
        start_sequence_number: None, // Missing start
        end_sequence_number: None,
        direction: Some(Direction::Ascending as i32),
    });
    let response = client.list_checkpoints(request).await;
    assert!(
        response.is_err(),
        "Expected an error for missing start_sequence_number"
    );
    let status = response.err().unwrap();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(
        status
            .message()
            .contains("start_sequence_number must be a valid u64 string")
    );
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_get_checkpoint_invalid_id_parse() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) = spawn_test_server().await;

    let request_id = "not_a_number".to_string();

    // Test GetCheckpointFull
    let request_full = tonic::Request::new(GetCheckpointRequest {
        checkpoint_id: request_id.clone(),
    });
    let response_full = client.get_checkpoint_full(request_full).await;
    assert!(
        response_full.is_err(),
        "GetCheckpointFull should fail for invalid ID"
    );
    let status_full = response_full.err().unwrap();
    assert_eq!(status_full.code(), tonic::Code::InvalidArgument);
    assert!(
        status_full
            .message()
            .contains("Could not parse checkpoint_id")
    );
    assert!(status_full.message().contains(&format!("{}", request_id)));

    // Test GetCheckpoint
    let request_summary = tonic::Request::new(GetCheckpointRequest {
        checkpoint_id: request_id.clone(),
    });
    let response_summary = client.get_checkpoint(request_summary).await;
    assert!(
        response_summary.is_err(),
        "GetCheckpoint should fail for invalid ID"
    );
    let status_summary = response_summary.err().unwrap();
    assert_eq!(status_summary.code(), tonic::Code::InvalidArgument);
    assert!(
        status_summary
            .message()
            .contains("Could not parse checkpoint_id")
    );
    assert!(
        status_summary
            .message()
            .contains(&format!("{}", request_id))
    );

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_stream_checkpoints_in_range_defined_end() {
    let (mut client, _addr, shutdown_tx, mock_state_reader) = spawn_test_server().await;

    let start_seq = 1u64;
    let end_seq = 3u64;
    let _count = (end_seq - start_seq + 1) as usize;
    mock_state_reader.set_latest_sequence_number_for_test(end_seq + 5);

    let request = tonic::Request::new(StreamCheckpointsInRangeRequest {
        start_sequence_number: start_seq.to_string(),
        end_sequence_number: Some(end_seq.to_string()),
        include_full_data: false,
    });

    let mut stream = client
        .stream_checkpoints_in_range(request)
        .await
        .expect("RPC call failed")
        .into_inner();

    let mut count = 0;
    let mut current_expected_seq = start_seq;
    loop {
        match timeout(Duration::from_secs(1), stream.next()).await {
            Ok(Some(Ok(item))) => {
                // Timeout didn't occur, stream yielded Ok(item)
                match item.checkpoint_type {
                    Some(CheckpointType::Summary(summary)) => {
                        assert_eq!(summary.sequence_number, current_expected_seq);
                        count += 1;
                        current_expected_seq += 1;
                    }
                    _ => panic!("Unexpected checkpoint type in stream"),
                }
            }
            Ok(Some(Err(status))) => {
                // Timeout didn't occur, stream yielded Err(status)
                panic!("Stream item contained an error status: {:?}", status);
            }
            Ok(None) => {
                // Timeout didn't occur, stream ended
                break; // Exit loop
            }
            Err(_) => {
                // Timeout occurred
                panic!(
                    "Timeout waiting for stream item. Items collected: {}",
                    count
                );
            }
        }
        // Protective break: if we've collected all expected items and the *next*
        // expected sequence number is beyond the end_seq, we can break. The
        // Ok(None) case should ideally handle this.
        if count == (end_seq - start_seq + 1) as usize && current_expected_seq > end_seq {
            break;
        }
    }
    assert_eq!(
        count,
        (end_seq - start_seq + 1) as usize,
        "Incorrect number of items streamed for defined range."
    );

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_stream_checkpoints_in_range_open_ended() {
    let (mut client, _addr, shutdown_tx, mock_state_reader) = spawn_test_server().await;

    let start_seq = 5u64;
    let _stream_limit = 3;
    mock_state_reader.set_latest_sequence_number_for_test(start_seq + 10);

    let request = tonic::Request::new(StreamCheckpointsInRangeRequest {
        start_sequence_number: start_seq.to_string(),
        end_sequence_number: None,
        include_full_data: false,
    });

    let mut stream = client
        .stream_checkpoints_in_range(request)
        .await
        .expect("RPC call failed")
        .into_inner();

    let mut count = 0;
    let items_to_collect = 5;
    let mut current_expected_seq = start_seq;
    while count < items_to_collect {
        match timeout(Duration::from_secs(1), stream.next()).await {
            Ok(Some(Ok(item))) => match item.checkpoint_type {
                Some(CheckpointType::Summary(summary)) => {
                    assert_eq!(summary.sequence_number, current_expected_seq);
                    count += 1;
                    current_expected_seq += 1;
                }
                _ => panic!("Unexpected checkpoint type in stream"),
            },
            Ok(Some(Err(status))) => panic!("Stream item contained an error status: {:?}", status),
            Ok(None) => panic!(
                "Stream ended prematurely after {} items, expected at least {}",
                count, items_to_collect
            ),
            Err(_) => panic!(
                "Timeout waiting for stream item after {} items, expected at least {}",
                count, items_to_collect
            ),
        }
    }
    assert_eq!(
        count, items_to_collect,
        "Did not collect the expected number of items from open-ended stream."
    );

    drop(stream);
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_subscribe_new_checkpoints_receives_items() {
    let (mut client, _addr, shutdown_tx, mock_state_reader) = spawn_test_server().await;

    let initial_latest_seq = 5u64;
    mock_state_reader.set_latest_sequence_number_for_test(initial_latest_seq);

    let request = tonic::Request::new(SubscribeNewCheckpointsRequest {
        start_from_sequence_number: Some((initial_latest_seq + 1).to_string()),
        include_full_data: false,
    });

    let stream_response = client.subscribe_new_checkpoints(request).await;
    assert!(
        stream_response.is_ok(),
        "RPC call failed: {:?}",
        stream_response.err()
    );
    let mut stream = stream_response.unwrap().into_inner();

    let mut received_checkpoints_seq_nums = Vec::new();
    let expected_seq_nums = vec![initial_latest_seq + 1, initial_latest_seq + 2];

    mock_state_reader.set_latest_sequence_number_for_test(initial_latest_seq + 1);
    if let Ok(Some(Ok(item))) = timeout(Duration::from_secs(2), stream.next()).await {
        if let Some(CheckpointType::Summary(ref s)) = item.checkpoint_type {
            println!("[Test] Received subscribed checkpoint: {:?}", item);
            received_checkpoints_seq_nums.push(s.sequence_number);
        } else {
            panic!("Expected summary, got full data or error");
        }
    } else {
        panic!(
            "Failed to receive first item or timeout for seq {}",
            initial_latest_seq + 1
        );
    }

    mock_state_reader.set_latest_sequence_number_for_test(initial_latest_seq + 2);
    if let Ok(Some(Ok(item))) = timeout(Duration::from_secs(2), stream.next()).await {
        if let Some(CheckpointType::Summary(ref s)) = item.checkpoint_type {
            println!("[Test] Received subscribed checkpoint: {:?}", item);
            received_checkpoints_seq_nums.push(s.sequence_number);
        } else {
            panic!("Expected summary, got full data or error");
        }
    } else {
        panic!(
            "Failed to receive second item or timeout for seq {}",
            initial_latest_seq + 2
        );
    }

    assert_eq!(
        received_checkpoints_seq_nums, expected_seq_nums,
        "Received incorrect sequence numbers."
    );

    let _ = shutdown_tx.send(());
}

impl ActualObjectStore for MockRestStateReader {
    fn get_object(&self, object_id: &ObjectID) -> StorageResult<Option<Object>> {
        // Provide a mock object for a specific ID used in tests
        let known_object_id_str =
            "0x1234500000000000000000000000000000000000000000000000000000000000";
        let known_object_id = ObjectID::from_hex_literal(known_object_id_str).unwrap();

        if *object_id == known_object_id {
            println!(
                "[MockRestStateReader] get_object called for known ID: {}",
                object_id
            );
            Ok(Some(create_mock_object(object_id, 1))) // version 1
        } else {
            println!(
                "[MockRestStateReader] get_object called for unknown ID: {}",
                object_id
            );
            Ok(None)
        }
    }
    fn get_object_by_key(
        &self,
        _object_id: &ObjectID,
        _version: SequenceNumber,
    ) -> StorageResult<Option<Object>> {
        unimplemented!("MockRestStateReader: get_object_by_key")
    }
    fn multi_get_objects_by_key(
        &self,
        _object_keys: &[iota_types::storage::ObjectKey],
    ) -> StorageResult<Vec<Option<Object>>> {
        unimplemented!("MockRestStateReader: multi_get_objects_by_key")
    }
}

impl ActualReadStore for MockRestStateReader {
    fn get_committee(&self, _epoch: EpochId) -> StorageResult<Option<Arc<Committee>>> {
        unimplemented!("MockRestStateReader: get_committee")
    }
    fn get_latest_checkpoint(&self) -> StorageResult<VerifiedCheckpoint> {
        unimplemented!("MockRestStateReader: get_latest_checkpoint")
    }
    fn get_latest_checkpoint_sequence_number(&self) -> StorageResult<CheckpointSequenceNumber> {
        Ok(self.latest_sequence_number.load(Ordering::SeqCst))
    }
    fn get_latest_epoch_id(&self) -> StorageResult<EpochId> {
        unimplemented!("MockRestStateReader: get_latest_epoch_id")
    }
    fn get_highest_verified_checkpoint(&self) -> StorageResult<VerifiedCheckpoint> {
        unimplemented!("MockRestStateReader: get_highest_verified_checkpoint")
    }
    fn get_highest_synced_checkpoint(&self) -> StorageResult<VerifiedCheckpoint> {
        unimplemented!("MockRestStateReader: get_highest_synced_checkpoint")
    }
    fn get_lowest_available_checkpoint(&self) -> StorageResult<CheckpointSequenceNumber> {
        unimplemented!("MockRestStateReader: get_lowest_available_checkpoint")
    }
    fn get_checkpoint_by_digest(
        &self,
        _digest: &CheckpointDigest,
    ) -> StorageResult<Option<VerifiedCheckpoint>> {
        unimplemented!("MockRestStateReader: get_checkpoint_by_digest")
    }
    fn get_checkpoint_by_sequence_number(
        &self,
        sequence_number: CheckpointSequenceNumber,
    ) -> StorageResult<Option<VerifiedCheckpoint>> {
        // Return mock data for a range of sequence numbers to support list tests
        if sequence_number <= 1000 {
            // Arbitrary upper limit for mock
            Ok(Some(create_mock_verified_checkpoint(sequence_number, 1))) // Assuming epoch 1 for consistency
        } else {
            Ok(None)
        }
    }
    fn get_checkpoint_contents_by_digest(
        &self,
        _digest: &CheckpointContentsDigest,
    ) -> StorageResult<Option<CheckpointContents>> {
        // For this mock, let's assume if someone asks for the specific digest we
        // create, we return the corresponding contents. Otherwise, None.
        if *_digest == CheckpointContentsDigest::new([0u8; 32]) {
            Ok(Some(create_mock_checkpoint_contents(0))) // Using 0 as a placeholder seq_num, might need adjustment if tests are sensitive
        } else {
            Ok(None)
        }
    }
    fn get_checkpoint_contents_by_sequence_number(
        &self,
        sequence_number: CheckpointSequenceNumber,
    ) -> StorageResult<Option<CheckpointContents>> {
        // Align with get_checkpoint_by_sequence_number: if summary exists, contents
        // should too for mock.
        if self
            .get_checkpoint_by_sequence_number(sequence_number)
            .unwrap()
            .is_some()
        {
            Ok(Some(create_mock_checkpoint_contents(sequence_number)))
        } else {
            Ok(None)
        }
    }
    fn get_transaction(
        &self,
        _tx_digest: &TransactionDigest,
    ) -> StorageResult<Option<Arc<VerifiedTransaction>>> {
        unimplemented!("MockRestStateReader: get_transaction")
    }
    fn get_transaction_effects(
        &self,
        _tx_digest: &TransactionDigest,
    ) -> StorageResult<Option<TransactionEffects>> {
        unimplemented!("MockRestStateReader: get_transaction_effects")
    }
    fn get_events(
        &self,
        _event_digest: &TransactionEventsDigest,
    ) -> StorageResult<Option<TransactionEvents>> {
        unimplemented!("MockRestStateReader: get_events")
    }
    fn get_full_checkpoint_contents_by_sequence_number(
        &self,
        _sequence_number: CheckpointSequenceNumber,
    ) -> StorageResult<Option<ActualFullCheckpointContents>> {
        Ok(None)
    }
    fn get_full_checkpoint_contents(
        &self,
        _digest: &CheckpointContentsDigest,
    ) -> StorageResult<Option<ActualFullCheckpointContents>> {
        unimplemented!("MockRestStateReader: get_full_checkpoint_contents")
    }
    fn get_checkpoint_data(
        &self,
        checkpoint: VerifiedCheckpoint,
        checkpoint_contents: CheckpointContents,
    ) -> anyhow::Result<CheckpointData> {
        // For the mock, we'll just package these together.
        // The transactions field in CheckpointData is
        // Vec<full_checkpoint_content::CheckpointTransaction>
        // For simplicity, let's return an empty list of transactions.
        // A more elaborate mock could convert checkpoint_contents.execution_digests
        // into CheckpointTransaction::DigestOnly.
        Ok(CheckpointData {
            checkpoint_summary: checkpoint.into(), /* Use .into() to convert VerifiedEnvelope to
                                                    * Envelope */
            checkpoint_contents,
            transactions: vec![], // Simple mock: no transactions
        })
    }
}

impl ActualRestStateReader for MockRestStateReader {
    fn get_transaction_checkpoint(
        &self,
        _digest: &TransactionDigest,
    ) -> StorageResult<Option<CheckpointSequenceNumber>> {
        unimplemented!("MockRestStateReader: get_transaction_checkpoint")
    }
    fn get_lowest_available_checkpoint_objects(&self) -> StorageResult<CheckpointSequenceNumber> {
        unimplemented!("MockRestStateReader: get_lowest_available_checkpoint_objects")
    }
    fn get_chain_identifier(&self) -> StorageResult<ChainIdentifier> {
        unimplemented!("MockRestStateReader: get_chain_identifier")
    }
    fn account_owned_objects_info_iter(
        &self,
        _owner: IotaAddress,
        _cursor: Option<ObjectID>,
    ) -> StorageResult<Box<dyn Iterator<Item = AccountOwnedObjectInfo> + '_>> {
        unimplemented!("MockRestStateReader: account_owned_objects_info_iter")
    }
    fn dynamic_field_iter(
        &self,
        _parent: ObjectID,
        _cursor: Option<ObjectID>,
    ) -> StorageResult<Box<dyn Iterator<Item = (DynamicFieldKey, DynamicFieldIndexInfo)> + '_>>
    {
        unimplemented!("MockRestStateReader: dynamic_field_iter")
    }
    fn get_coin_info(&self, _coin_type: &StructTag) -> StorageResult<Option<CoinInfo>> {
        unimplemented!("MockRestStateReader: get_coin_info")
    }
    fn get_epoch_last_checkpoint(
        &self,
        _epoch_id: EpochId,
    ) -> StorageResult<Option<VerifiedCheckpoint>> {
        unimplemented!("MockRestStateReader: get_epoch_last_checkpoint")
    }
}

// Helper function to create a mock VerifiedCheckpoint
fn create_mock_verified_checkpoint(
    seq_num: CheckpointSequenceNumber,
    epoch_id: EpochId,
) -> VerifiedCheckpoint {
    let summary = CheckpointSummary {
        epoch: epoch_id,
        sequence_number: seq_num,
        network_total_transactions: 1000 + seq_num,
        content_digest: CheckpointContentsDigest::new([0u8; 32]),
        previous_digest: if seq_num > 0 {
            Some(CheckpointDigest::new([1u8; 32]))
        } else {
            None
        },
        epoch_rolling_gas_cost_summary: GasCostSummary::default(),
        timestamp_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64,
        checkpoint_commitments: vec![],
        end_of_epoch_data: None,
        version_specific_data: vec![],
    };

    let keypair: AuthorityKeyPair = AuthorityKeyPair::generate(&mut thread_rng());
    // The address is not strictly needed for this mock, but if it were:
    // let address = IotaAddress::from(keypair.public());

    let authority_name = AuthorityName::from(keypair.public());
    let stake: StakeUnit = TOTAL_VOTING_POWER;

    let committee =
        iota_types::committee::Committee::new(epoch_id, BTreeMap::from([(authority_name, stake)]));

    let sign_info = AuthoritySignInfo::new(
        epoch_id,
        &summary,
        Intent::iota_app(CheckpointSummary::SCOPE),
        authority_name,
        &keypair,
    );

    let auth_strong_quorum_sig =
        AuthorityStrongQuorumSignInfo::new_from_auth_sign_infos(vec![sign_info], &committee)
            .expect("Auth strong quorum sig creation failed in mock");

    let certified_summary_envelope: Envelope<CheckpointSummary, AuthorityStrongQuorumSignInfo> =
        Envelope::new_from_data_and_sig(summary.clone(), auth_strong_quorum_sig);

    VerifiedEnvelope::new_unchecked(certified_summary_envelope)
}

// Helper function to create mock CheckpointContents
fn create_mock_checkpoint_contents(_seq_num: CheckpointSequenceNumber) -> CheckpointContents {
    // new_with_digests_only_for_tests takes an iterator of ExecutionDigests.
    // For a simple mock, we provide an empty list of digests.
    // The sequence number is not directly part of CheckpointContents constructed
    // this way; it's primarily associated with CheckpointSummary.
    CheckpointContents::new_with_digests_only_for_tests(vec![])
}

#[tokio::test]
async fn test_get_checkpoint_retrieves_and_converts() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) = spawn_test_server().await;

    let request_seq_num = 789u64;
    let request = tonic::Request::new(GetCheckpointRequest {
        checkpoint_id: request_seq_num.to_string(),
    });

    let response = client.get_checkpoint(request).await;
    assert!(
        response.is_ok(),
        "GetCheckpoint RPC call failed: {:?}",
        response.err()
    );
    let summary_gprc = response.unwrap().into_inner();

    // Expected data based on create_mock_verified_checkpoint for seq_num = 789,
    // epoch_id = 1
    assert_eq!(summary_gprc.sequence_number, request_seq_num);
    assert_eq!(summary_gprc.epoch, 1);
    assert_eq!(
        summary_gprc.network_total_transactions,
        1000 + request_seq_num
    );

    assert!(
        summary_gprc.content_digest.is_some(),
        "Content digest should be present"
    );
    assert_eq!(
        summary_gprc.content_digest.unwrap().digest,
        vec![0u8; 32],
        "Unexpected content digest"
    );

    // For seq_num = 789 (which is > 0), previous_digest should be Some([1u8; 32])
    assert!(
        summary_gprc.previous_digest.is_some(),
        "Previous digest should be present for seq > 0"
    );
    assert_eq!(
        summary_gprc.previous_digest.unwrap().digest,
        vec![1u8; 32],
        "Unexpected previous digest"
    );

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_get_checkpoint_not_found() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) = spawn_test_server().await;

    let request_seq_num = 12345u64; // A sequence number not returned by mock
    let request = tonic::Request::new(GetCheckpointRequest {
        checkpoint_id: request_seq_num.to_string(),
    });

    let response = client.get_checkpoint(request).await;
    assert!(
        response.is_err(),
        "Expected GetCheckpoint to fail for non-existent seq"
    );
    let status = response.err().unwrap();
    assert_eq!(status.code(), tonic::Code::NotFound);
    assert!(status.message().contains(&format!(
        "Checkpoint with sequence number {} not found",
        request_seq_num
    )));

    let _ = shutdown_tx.send(());
}

// Helper function to create a mock iota_types::object::Object
fn create_mock_object(_object_id: &ObjectID, version_val: u64) -> Object {
    let mock_move_object_type_str = "0x42::example_module::ExampleObject";
    let struct_tag = mock_move_object_type_str
        .parse::<StructTag>()
        .expect("Parsing mock StructTag failed");
    let move_object_type = MoveObjectType::from(struct_tag);

    let move_obj = MoveObject::new_from_execution(
        move_object_type,
        SequenceNumber::from_u64(version_val),
        b"mock_contents".to_vec(),
        &iota_protocol_config::ProtocolConfig::get_for_min_version(),
    )
    .unwrap();

    ObjectInner {
        data: Data::Move(move_obj),
        owner: Owner::AddressOwner(IotaAddress::from(
            ObjectID::from_hex_literal(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            )
            .unwrap(),
        )),
        previous_transaction: TransactionDigest::genesis_marker(),
        storage_rebate: 100,
    }
    .into()
}

#[tokio::test]
async fn test_stream_checkpoints_in_range_include_full_data() {
    let (mut client, _addr, shutdown_tx, mock_state_reader) = spawn_test_server().await;

    let start_seq = 10u64;
    let end_seq = 12u64;
    let count = (end_seq - start_seq + 1) as usize;
    mock_state_reader.set_latest_sequence_number_for_test(end_seq + 5); // Ensure data is available

    let request = tonic::Request::new(StreamCheckpointsInRangeRequest {
        start_sequence_number: start_seq.to_string(),
        end_sequence_number: Some(end_seq.to_string()),
        include_full_data: true,
    });

    let stream_response = client.stream_checkpoints_in_range(request).await;
    assert!(
        stream_response.is_ok(),
        "RPC call failed: {:?}",
        stream_response.err()
    );
    let mut stream = stream_response.unwrap().into_inner();

    let mut received_checkpoints = Vec::new();
    for i in 0..count {
        match timeout(Duration::from_secs(1), stream.next()).await {
            Ok(Some(Ok(streamed_checkpoint))) => {
                if let Some(CheckpointType::FullData(full_data)) =
                    streamed_checkpoint.checkpoint_type
                {
                    assert_eq!(
                        full_data.summary.as_ref().unwrap().sequence_number,
                        start_seq + i as u64
                    );
                    // Check that transactions list is present (it's empty in mock)
                    assert!(
                        full_data.transactions.is_empty(),
                        "Expected empty transactions for mock full data"
                    );
                    received_checkpoints.push(full_data);
                } else {
                    panic!(
                        "Expected FullData variant, got {:?}",
                        streamed_checkpoint.checkpoint_type
                    );
                }
            }
            Ok(Some(Err(status))) => panic!("Stream item was an error: {}", status),
            Ok(None) => panic!(
                "Stream ended prematurely after {} items, expected {}",
                i, count
            ),
            Err(_) => panic!("Timeout waiting for stream item {}/{}", i + 1, count),
        }
    }

    // Check that the stream ends after the expected count
    match timeout(Duration::from_secs(1), stream.next()).await {
        Ok(None) => { /* Expected: stream ended */ }
        Ok(Some(Ok(extra_item))) => panic!("Stream sent an extra item: {:?}", extra_item),
        Ok(Some(Err(status))) => panic!("Stream sent an extra error item: {}", status),
        Err(_) => panic!("Timeout waiting for stream to end (or it sent extra items)."),
    }
    assert_eq!(received_checkpoints.len(), count);

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_subscribe_new_checkpoints_include_full_data() {
    let (mut client, _addr, shutdown_tx, mock_state_reader) = spawn_test_server().await;

    let initial_latest_seq = 20u64;
    mock_state_reader.set_latest_sequence_number_for_test(initial_latest_seq);

    let request = tonic::Request::new(SubscribeNewCheckpointsRequest {
        start_from_sequence_number: Some((initial_latest_seq + 1).to_string()),
        include_full_data: true,
    });

    let stream_response = client.subscribe_new_checkpoints(request).await;
    assert!(
        stream_response.is_ok(),
        "RPC call failed: {:?}",
        stream_response.err()
    );
    let mut stream = stream_response.unwrap().into_inner();

    // Simulate new checkpoints appearing
    let new_seq_1 = initial_latest_seq + 1;
    let new_seq_2 = initial_latest_seq + 2;

    // Wait a moment for subscription to establish
    sleep(Duration::from_millis(100)).await;
    mock_state_reader.set_latest_sequence_number_for_test(new_seq_1);

    match timeout(Duration::from_secs(2), stream.next()).await {
        Ok(Some(Ok(streamed_checkpoint))) => {
            if let Some(CheckpointType::FullData(full_data)) = streamed_checkpoint.checkpoint_type {
                assert_eq!(
                    full_data.summary.as_ref().unwrap().sequence_number,
                    new_seq_1
                );
                assert!(
                    full_data.transactions.is_empty(),
                    "Expected empty transactions for mock full data"
                );
            } else {
                panic!(
                    "Expected FullData variant for seq {}, got {:?}",
                    new_seq_1, streamed_checkpoint.checkpoint_type
                );
            }
        }
        Ok(Some(Err(status))) => panic!("Stream item 1 was an error: {}", status),
        Ok(None) => panic!("Stream ended prematurely before item 1"),
        Err(_) => panic!("Timeout waiting for stream item 1 (seq {})", new_seq_1),
    }

    mock_state_reader.set_latest_sequence_number_for_test(new_seq_2);
    match timeout(Duration::from_secs(2), stream.next()).await {
        Ok(Some(Ok(streamed_checkpoint))) => {
            if let Some(CheckpointType::FullData(full_data)) = streamed_checkpoint.checkpoint_type {
                assert_eq!(
                    full_data.summary.as_ref().unwrap().sequence_number,
                    new_seq_2
                );
                assert!(
                    full_data.transactions.is_empty(),
                    "Expected empty transactions for mock full data"
                );
            } else {
                panic!(
                    "Expected FullData variant for seq {}, got {:?}",
                    new_seq_2, streamed_checkpoint.checkpoint_type
                );
            }
        }
        Ok(Some(Err(status))) => panic!("Stream item 2 was an error: {}", status),
        Ok(None) => panic!("Stream ended prematurely before item 2"),
        Err(_) => panic!("Timeout waiting for stream item 2 (seq {})", new_seq_2),
    }

    let _ = shutdown_tx.send(());
}
