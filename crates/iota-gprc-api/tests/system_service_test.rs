use std::{
    collections::BTreeMap,
    net::{SocketAddr, TcpListener},
    sync::Arc,
    time::Duration,
};

use anyhow;
use iota_gprc_api::{
    proto::iota::gprc::v1::{
        GetSystemInfoRequest, SubscribeSystemEventsRequest,
        system_gprc_service_client::SystemGprcServiceClient,
    },
    server::{GrpcServer, StateReader},
};
use iota_types::{
    base_types::{AuthorityName, ObjectID, SequenceNumber},
    committee::{Committee, EpochId, StakeUnit, TOTAL_VOTING_POWER},
    crypto::{AuthorityKeyPair, AuthoritySignInfo, AuthorityStrongQuorumSignInfo, KeypairTraits},
    digests::{
        ChainIdentifier, CheckpointContentsDigest, CheckpointDigest, TransactionDigest,
        TransactionEventsDigest,
    },
    effects::{TransactionEffects, TransactionEvents},
    full_checkpoint_content::CheckpointData,
    gas::GasCostSummary,
    message_envelope::{Envelope, Message, VerifiedEnvelope},
    messages_checkpoint::{
        CheckpointContents, CheckpointSequenceNumber, CheckpointSummary, FullCheckpointContents,
        VerifiedCheckpoint,
    },
    object::Object,
    storage::{
        AccountOwnedObjectInfo, CoinInfo, DynamicFieldIndexInfo, DynamicFieldKey, ListDirection,
        ObjectKey, ObjectStore, ReadStore, RestStateReader, error::Result as StorageResult,
    },
    transaction::VerifiedTransaction,
};
use move_core_types::language_storage::StructTag;
use rand::thread_rng;
use shared_crypto::intent::Intent;
use tokio::{sync::broadcast, time::sleep};
use tokio_stream::StreamExt;

#[derive(Default)]
struct MockRestStateReader {}

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
    let authority_name = AuthorityName::from(keypair.public());
    let stake: StakeUnit = TOTAL_VOTING_POWER;
    let committee = Committee::new(epoch_id, BTreeMap::from([(authority_name, stake)]));

    let sign_info = AuthoritySignInfo::new(
        epoch_id,
        &summary,
        Intent::iota_app(CheckpointSummary::SCOPE),
        authority_name,
        &keypair,
    );

    let auth_strong_quorum_sig =
        AuthorityStrongQuorumSignInfo::new_from_auth_sign_infos(vec![sign_info], &committee)
            .expect("Mock AuthStrongQuorumSignInfo creation failed");

    let envelope = Envelope::new_from_data_and_sig(summary, auth_strong_quorum_sig);
    VerifiedEnvelope::new_unchecked(envelope)
}

impl ReadStore for MockRestStateReader {
    fn get_committee(&self, _epoch: EpochId) -> StorageResult<Option<Arc<Committee>>> {
        Ok(None)
    }
    fn get_latest_checkpoint(&self) -> StorageResult<VerifiedCheckpoint> {
        Ok(create_mock_verified_checkpoint(0, 0))
    }
    fn get_latest_checkpoint_sequence_number(&self) -> StorageResult<CheckpointSequenceNumber> {
        Ok(0)
    }
    fn get_latest_epoch_id(&self) -> StorageResult<EpochId> {
        Ok(0)
    }
    fn get_highest_verified_checkpoint(&self) -> StorageResult<VerifiedCheckpoint> {
        Ok(create_mock_verified_checkpoint(0, 0))
    }
    fn get_highest_synced_checkpoint(&self) -> StorageResult<VerifiedCheckpoint> {
        Ok(create_mock_verified_checkpoint(0, 0))
    }
    fn get_lowest_available_checkpoint(&self) -> StorageResult<CheckpointSequenceNumber> {
        Ok(0)
    }
    fn get_checkpoint_by_digest(
        &self,
        _digest: &CheckpointDigest,
    ) -> StorageResult<Option<VerifiedCheckpoint>> {
        Ok(None)
    }
    fn get_checkpoint_by_sequence_number(
        &self,
        _sequence_number: CheckpointSequenceNumber,
    ) -> StorageResult<Option<VerifiedCheckpoint>> {
        Ok(None)
    }
    fn get_checkpoint_contents_by_digest(
        &self,
        _digest: &CheckpointContentsDigest,
    ) -> StorageResult<Option<CheckpointContents>> {
        Ok(None)
    }
    fn get_checkpoint_contents_by_sequence_number(
        &self,
        _sequence_number: CheckpointSequenceNumber,
    ) -> StorageResult<Option<CheckpointContents>> {
        Ok(None)
    }
    fn get_transaction(
        &self,
        _tx_digest: &TransactionDigest,
    ) -> StorageResult<Option<Arc<VerifiedTransaction>>> {
        Ok(None)
    }
    fn get_transaction_effects(
        &self,
        _tx_digest: &TransactionDigest,
    ) -> StorageResult<Option<TransactionEffects>> {
        Ok(None)
    }
    fn get_events(
        &self,
        _event_digest: &TransactionEventsDigest,
    ) -> StorageResult<Option<TransactionEvents>> {
        Ok(None)
    }
    fn get_full_checkpoint_contents_by_sequence_number(
        &self,
        _sequence_number: CheckpointSequenceNumber,
    ) -> StorageResult<Option<FullCheckpointContents>> {
        Ok(None)
    }
    fn get_full_checkpoint_contents(
        &self,
        _digest: &CheckpointContentsDigest,
    ) -> StorageResult<Option<FullCheckpointContents>> {
        Ok(None)
    }
    fn get_checkpoint_data(
        &self,
        _checkpoint: VerifiedCheckpoint,
        _checkpoint_contents: CheckpointContents,
    ) -> anyhow::Result<CheckpointData> {
        Err(anyhow::anyhow!("Mock: get_checkpoint_data not implemented"))
    }
}

impl ObjectStore for MockRestStateReader {
    fn get_object(&self, _object_id: &ObjectID) -> StorageResult<Option<Object>> {
        Ok(None)
    }
    fn get_object_by_key(
        &self,
        _object_id: &ObjectID,
        _version: SequenceNumber,
    ) -> StorageResult<Option<Object>> {
        Ok(None)
    }
    fn multi_get_objects_by_key(
        &self,
        _object_keys: &[ObjectKey],
    ) -> StorageResult<Vec<Option<Object>>> {
        Ok(Vec::new())
    }
}

impl RestStateReader for MockRestStateReader {
    fn get_transaction_checkpoint(
        &self,
        _digest: &TransactionDigest,
    ) -> StorageResult<Option<CheckpointSequenceNumber>> {
        Ok(None)
    }
    fn get_lowest_available_checkpoint_objects(&self) -> StorageResult<CheckpointSequenceNumber> {
        Ok(0)
    }
    fn get_chain_identifier(&self) -> StorageResult<ChainIdentifier> {
        Ok(ChainIdentifier::from(CheckpointDigest::new([0u8; 32])))
    }
    fn account_owned_objects_info_iter(
        &self,
        _owner: iota_types::base_types::IotaAddress,
        _cursor: Option<ObjectID>,
    ) -> StorageResult<Box<dyn Iterator<Item = AccountOwnedObjectInfo> + '_>> {
        Ok(Box::new(std::iter::empty()))
    }
    fn dynamic_field_iter(
        &self,
        _parent: ObjectID,
        _cursor: Option<ObjectID>,
    ) -> StorageResult<Box<dyn Iterator<Item = (DynamicFieldKey, DynamicFieldIndexInfo)> + '_>>
    {
        Ok(Box::new(std::iter::empty()))
    }
    fn get_coin_info(&self, _coin_type: &StructTag) -> StorageResult<Option<CoinInfo>> {
        Ok(None)
    }
    fn get_epoch_last_checkpoint(
        &self,
        _epoch_id: EpochId,
    ) -> StorageResult<Option<VerifiedCheckpoint>> {
        Ok(None)
    }
    fn list_transactions(
        &self,
        _cursor: Option<TransactionDigest>,
        _limit: u64,
        _direction: ListDirection,
    ) -> StorageResult<Vec<(TransactionDigest, Arc<VerifiedTransaction>)>> {
        Ok(Vec::new())
    }
}

fn get_available_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn spawn_test_server_with_system_client() -> (
    SystemGprcServiceClient<tonic::transport::Channel>,
    SocketAddr,
    broadcast::Sender<()>,
    Arc<MockRestStateReader>,
) {
    let port = get_available_port();
    let addr: SocketAddr = format!("[::1]:{}", port).parse().unwrap();

    let mock_state_reader_arc = Arc::new(MockRestStateReader::default());
    let server_state_reader: StateReader = mock_state_reader_arc.clone();

    let (shutdown_tx, server_shutdown_rx) = broadcast::channel(1);

    let grpc_server = GrpcServer::new(addr, server_state_reader);

    tokio::spawn(async move {
        if let Err(e) = grpc_server.start(server_shutdown_rx).await {
            eprintln!("[Test SystemService] gRPC server failed: {}", e);
        }
    });

    sleep(Duration::from_millis(200)).await;

    let client_endpoint = format!("http://[::1]:{}", port);
    let client_channel = tonic::transport::Channel::from_shared(client_endpoint.clone())
        .unwrap()
        .connect_timeout(Duration::from_secs(2))
        .connect()
        .await
        .unwrap_or_else(|e| {
            panic!(
                "[Test SystemService] Failed to connect to gRPC server at {}: {:?}",
                client_endpoint, e
            )
        });

    let client = SystemGprcServiceClient::new(client_channel);
    (client, addr, shutdown_tx, mock_state_reader_arc)
}

#[tokio::test]
async fn test_get_system_info_success() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) =
        spawn_test_server_with_system_client().await;

    // Brief pause to ensure uptime is non-zero.
    sleep(Duration::from_millis(20)).await; // Increased sleep slightly

    let request = tonic::Request::new(GetSystemInfoRequest {});
    let response_result = client.get_system_info(request).await;

    assert!(
        response_result.is_ok(),
        "GetSystemInfo RPC call failed: {:?}",
        response_result.err()
    );
    let system_info = response_result.unwrap().into_inner();

    // Check node_version (using the placeholder from system_service.rs)
    assert_eq!(system_info.node_version, "iota-gprc-api-dev");

    // Check uptime_ms
    assert!(
        system_info.uptime_ms.is_some(),
        "uptime_ms should be present"
    );
    let uptime_str = system_info.uptime_ms.as_ref().unwrap().value.clone();
    let uptime_val = uptime_str
        .parse::<u128>()
        .expect("uptime_ms should be a valid u128 string"); // Use u128 for millis
    assert!(uptime_val > 0, "uptime_ms should be positive");

    // Test idempotency: call again and check uptime increases or stays same
    sleep(Duration::from_millis(10)).await; // Ensure a small duration passes for uptime to potentially change
    let response2_result = client.get_system_info(GetSystemInfoRequest {}).await;
    assert!(
        response2_result.is_ok(),
        "GetSystemInfo (2nd call) RPC call failed: {:?}",
        response2_result.err()
    );
    let system_info2 = response2_result.unwrap().into_inner();

    assert!(
        system_info2.uptime_ms.is_some(),
        "uptime_ms (2nd call) should be present"
    );
    let uptime_str2 = system_info2.uptime_ms.as_ref().unwrap().value.clone();
    let uptime_val2 = uptime_str2
        .parse::<u128>()
        .expect("uptime_ms (2nd call) should be a valid u128 string");
    assert!(
        uptime_val2 >= uptime_val,
        "Uptime ({}) should increase or stay the same ({}) on subsequent calls",
        uptime_val2,
        uptime_val
    );

    // Cleanly shut down the server
    let _ = shutdown_tx.send(());
    sleep(Duration::from_millis(100)).await; // Give server time to shutdown
}

#[tokio::test]
async fn test_subscribe_system_events_receives_mock_events() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) =
        spawn_test_server_with_system_client().await;

    let request = tonic::Request::new(SubscribeSystemEventsRequest {});
    let response_result = client.subscribe_system_events(request).await;

    assert!(
        response_result.is_ok(),
        "SubscribeSystemEvents RPC call failed: {:?}",
        response_result.err()
    );

    let mut stream = response_result.unwrap().into_inner();
    let mut received_events = Vec::new();

    // The mock event generator in SystemServiceImpl sends an event every 10
    // seconds. Try to receive 2 events, with a timeout longer than 20 seconds.
    let timeout_duration = Duration::from_secs(25);

    for i in 0..2 {
        match tokio::time::timeout(timeout_duration, stream.next()).await {
            Ok(Some(Ok(event))) => {
                println!("[Test] Received system event: {:?}", event);
                assert_eq!(
                    event.event_type,
                    iota_gprc_api::proto::iota::gprc::v1::system_event_gprc::EventType::NodeStatusChanged as i32
                );
                assert!(event.timestamp_ms.is_some());
                let timestamp_value_str = event
                    .timestamp_ms
                    .as_ref()
                    .expect("Timestamp should be present")
                    .value
                    .clone();
                let _ = timestamp_value_str
                    .parse::<u128>()
                    .expect("Timestamp should be valid u128");

                // Check details_json structure (simple check for now)
                assert!(event.details_json.contains("\"status\": \"OK\""));
                assert!(event.details_json.contains(&format!("\"tick\": {}", i + 1))); // Assuming tick starts at 1 for received events

                received_events.push(event);
            }
            Ok(Some(Err(status))) => {
                panic!("[Test] Error receiving system event: {:?}", status);
            }
            Ok(None) => {
                panic!(
                    "[Test] Stream closed prematurely after {} events.",
                    received_events.len()
                );
            }
            Err(_) => {
                panic!(
                    "[Test] Timeout waiting for system event #{}. Received {} events so far.",
                    i + 1,
                    received_events.len()
                );
            }
        }
    }

    assert_eq!(
        received_events.len(),
        2,
        "Should have received exactly 2 events."
    );

    // Cleanly shut down the server
    let _ = shutdown_tx.send(());
    sleep(Duration::from_millis(100)).await; // Give server time to shutdown
}
