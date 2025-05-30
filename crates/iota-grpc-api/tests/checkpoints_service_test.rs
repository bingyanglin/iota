use std::{
    collections::BTreeMap,
    net::{SocketAddr, TcpListener},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use futures::StreamExt;
use iota_grpc_api::{
    proto::iota::gprc::v1::{
        GetCheckpointRequest, SubscribeNewCheckpointsRequest,
        checkpoint_gprc_service_client::CheckpointGprcServiceClient,
        streamed_checkpoint::CheckpointType,
    },
    server::{GrpcServer, StateReader},
};
use iota_types::{
    base_types::{AuthorityName, IotaAddress, ObjectID, TransactionDigest, VersionNumber},
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
    object::Object,
    storage::{
        AccountOwnedObjectInfo, CoinInfo, DynamicFieldIndexInfo, DynamicFieldKey,
        ObjectStore as ActualObjectStore, ReadStore as ActualReadStore,
        RestStateReader as ActualRestStateReader,
        error::{Error as StorageError, Result as StorageResult},
    },
    transaction::VerifiedTransaction,
};
use move_core_types::language_storage::StructTag;
use parking_lot::RwLock;
use shared_crypto::intent::Intent;
use tokio::{
    sync::broadcast,
    time::{Duration, sleep, timeout},
};

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
    checkpoint_event_sender: Option<broadcast::Sender<Arc<VerifiedCheckpoint>>>,
    mock_checkpoints_cache:
        std::sync::Mutex<BTreeMap<CheckpointSequenceNumber, Arc<VerifiedCheckpoint>>>,
    mock_checkpoint_contents_cache:
        std::sync::Mutex<BTreeMap<CheckpointSequenceNumber, Arc<CheckpointContents>>>,
}

impl Default for MockRestStateReader {
    fn default() -> Self {
        Self {
            latest_sequence_number: Arc::new(AtomicU64::new(0)),
            checkpoint_event_sender: None,
            mock_checkpoints_cache: std::sync::Mutex::new(BTreeMap::new()),
            mock_checkpoint_contents_cache: std::sync::Mutex::new(BTreeMap::new()),
        }
    }
}

impl MockRestStateReader {
    // New method to associate the sender
    fn set_checkpoint_event_sender(&mut self, sender: broadcast::Sender<Arc<VerifiedCheckpoint>>) {
        self.checkpoint_event_sender = Some(sender);
    }

    // Helper for tests to advance the mock's latest sequence number and publish
    // events
    fn set_latest_sequence_number_for_test(&self, val: u64) {
        let last_known_seq = self.latest_sequence_number.swap(val, Ordering::SeqCst);

        if val > last_known_seq {
            if let Some(sender) = &self.checkpoint_event_sender {
                let mut cache = self.mock_checkpoints_cache.lock().unwrap();
                for seq_to_publish in (last_known_seq + 1)..=val {
                    // Create and cache mock checkpoint if it doesn't exist
                    let checkpoint_to_send = cache
                        .entry(seq_to_publish)
                        .or_insert_with(|| {
                            // Assuming epoch 1 for all test checkpoints for simplicity here.
                            // Adjust if specific tests need different epochs.
                            Arc::new(create_mock_verified_checkpoint(seq_to_publish, 1))
                        })
                        .clone();

                    if sender.send(checkpoint_to_send).is_err() {
                        println!(
                            "[MockRestStateReader] Failed to send checkpoint {} event, no active subscribers.",
                            seq_to_publish
                        );
                    } else {
                        println!(
                            "[MockRestStateReader] Successfully sent checkpoint {} event.",
                            seq_to_publish
                        );
                    }
                }
            } else {
                println!(
                    "[MockRestStateReader] No event sender configured, cannot publish checkpoint events."
                );
            }
        }
    }

    // Helper to pre-populate mock checkpoints for get_checkpoint_by_sequence_number
    fn add_mock_checkpoint_to_cache(&self, checkpoint: Arc<VerifiedCheckpoint>) {
        let mut cache = self.mock_checkpoints_cache.lock().unwrap();
        let seq = checkpoint.inner().sequence_number();
        cache.insert(*seq, checkpoint.clone());

        // DO NOT add default content here. Content addition should be explicit.
        // let mut contents_cache =
        // self.mock_checkpoint_contents_cache.lock().unwrap();
        // contents_cache
        //     .entry(*seq)
        //     .or_insert_with(||
        // Arc::new(create_mock_checkpoint_contents(*seq)));
    }

    // Helper to specifically add mock contents
    fn add_mock_checkpoint_contents_to_cache(
        &self,
        seq: CheckpointSequenceNumber,
        contents: Arc<CheckpointContents>,
    ) {
        let mut contents_cache = self.mock_checkpoint_contents_cache.lock().unwrap();
        contents_cache.insert(seq, contents);
    }

    // Methods from ActualRestStateReader trait (some were already here, some added)
    // Note: The user's previous diff showed many of these as already existing, but
    // the linter errors indicated they were either missing from the trait impl
    // or had wrong signatures. We will define them here and then ensure they are
    // correctly listed in the `impl ActualRestStateReader for MockRestStateReader`
    // block.

    fn get_checkpoint_by_sequence_number_impl(
        &self,
        sequence_number: CheckpointSequenceNumber,
    ) -> StorageResult<Option<VerifiedCheckpoint>> {
        let cache = self.mock_checkpoints_cache.lock().unwrap();
        Ok(cache
            .get(&sequence_number)
            .cloned()
            .map(|arc_vc| (*arc_vc).clone()))
    }

    fn get_checkpoint_by_digest_impl(
        &self,
        digest: &CheckpointDigest,
    ) -> StorageResult<Option<VerifiedCheckpoint>> {
        let cache = self.mock_checkpoints_cache.lock().unwrap();
        Ok(cache
            .values()
            .find(|vc| vc.inner().digest() == digest)
            .cloned()
            .map(|arc_vc| (*arc_vc).clone()))
    }

    fn get_checkpoint_contents_by_sequence_number_impl(
        &self,
        sequence_number: CheckpointSequenceNumber,
    ) -> StorageResult<Option<CheckpointContents>> {
        let cache = self.mock_checkpoint_contents_cache.lock().unwrap();
        Ok(cache
            .get(&sequence_number)
            .cloned()
            .map(|arc_cc| (*arc_cc).clone()))
    }

    fn get_checkpoint_contents_by_digest_impl(
        &self,
        contents_digest: &CheckpointContentsDigest,
    ) -> StorageResult<Option<CheckpointContents>> {
        let cache = self.mock_checkpoint_contents_cache.lock().unwrap();
        Ok(cache
            .values()
            .find(|cc| cc.digest() == contents_digest)
            .cloned()
            .map(|arc_cc| (*arc_cc).clone()))
    }

    fn get_highest_known_checkpoint_impl(&self) -> StorageResult<Option<VerifiedCheckpoint>> {
        let seq = self.latest_sequence_number.load(Ordering::SeqCst);
        if seq == 0
            && self
                .mock_checkpoints_cache
                .lock()
                .unwrap()
                .get(&0)
                .is_none()
        {
            return Ok(None);
        }
        self.get_checkpoint_by_sequence_number_impl(seq)
    }

    fn get_lowest_available_checkpoint_impl(&self) -> StorageResult<CheckpointSequenceNumber> {
        let cache = self.mock_checkpoints_cache.lock().unwrap();
        Ok(cache.keys().min().cloned().unwrap_or(0))
    }

    fn get_checkpoint_data_impl(
        &self,
        verified_checkpoint: VerifiedCheckpoint,
        contents: CheckpointContents,
    ) -> StorageResult<CheckpointData> {
        Ok(CheckpointData {
            checkpoint_summary: verified_checkpoint.clone().into(), // Changed to .clone().into()
            checkpoint_contents: contents,
            transactions: vec![],
        })
    }
}

// Implement ActualObjectStore for MockRestStateReader
impl ActualObjectStore for MockRestStateReader {
    fn get_object(&self, object_id: &ObjectID) -> StorageResult<Option<Object>> {
        let _ = object_id;
        Err(StorageError::custom(
            "MockRestStateReader::get_object unimplemented",
        ))
    }

    fn get_object_by_key(
        &self,
        object_id: &ObjectID,
        version: VersionNumber,
    ) -> StorageResult<Option<Object>> {
        let _ = (object_id, version);
        Err(StorageError::custom(
            "MockRestStateReader::get_object_by_key unimplemented",
        ))
    }
}

// Implement ActualReadStore for MockRestStateReader
#[async_trait::async_trait]
impl ActualReadStore for MockRestStateReader {
    // Methods from ObjectStore (ancestor trait) are covered by the
    // ActualObjectStore impl above.

    // Committee Getters
    fn get_committee(&self, _epoch: EpochId) -> StorageResult<Option<Arc<Committee>>> {
        Err(StorageError::custom(
            "MockRestStateReader::get_committee unimplemented",
        ))
    }

    // Checkpoint Getters
    fn get_latest_checkpoint(&self) -> StorageResult<VerifiedCheckpoint> {
        self.get_highest_known_checkpoint_impl()?
            .ok_or_else(|| StorageError::custom("No latest checkpoint in mock"))
    }

    // get_latest_checkpoint_sequence_number() has a default implementation in
    // ReadStore get_latest_epoch_id() has a default implementation in ReadStore

    fn get_highest_verified_checkpoint(&self) -> StorageResult<VerifiedCheckpoint> {
        self.get_highest_known_checkpoint_impl()?
            .ok_or_else(|| StorageError::custom("No highest verified checkpoint in mock"))
    }

    fn get_highest_synced_checkpoint(&self) -> StorageResult<VerifiedCheckpoint> {
        self.get_highest_known_checkpoint_impl()?
            .ok_or_else(|| StorageError::custom("No highest synced checkpoint in mock"))
    }

    fn get_lowest_available_checkpoint(&self) -> StorageResult<CheckpointSequenceNumber> {
        self.get_lowest_available_checkpoint_impl()
    }

    fn get_checkpoint_by_digest(
        &self,
        digest: &CheckpointDigest,
    ) -> StorageResult<Option<VerifiedCheckpoint>> {
        self.get_checkpoint_by_digest_impl(digest)
    }

    fn get_checkpoint_by_sequence_number(
        &self,
        sequence_number: CheckpointSequenceNumber,
    ) -> StorageResult<Option<VerifiedCheckpoint>> {
        self.get_checkpoint_by_sequence_number_impl(sequence_number)
    }

    fn get_checkpoint_contents_by_digest(
        &self,
        digest: &CheckpointContentsDigest,
    ) -> StorageResult<Option<CheckpointContents>> {
        self.get_checkpoint_contents_by_digest_impl(digest)
    }

    fn get_checkpoint_contents_by_sequence_number(
        &self,
        sequence_number: CheckpointSequenceNumber,
    ) -> StorageResult<Option<CheckpointContents>> {
        self.get_checkpoint_contents_by_sequence_number_impl(sequence_number)
    }

    // Transaction Getters
    fn get_transaction(
        &self,
        tx_digest: &TransactionDigest,
    ) -> StorageResult<Option<Arc<VerifiedTransaction>>> {
        let _ = tx_digest;
        Err(StorageError::custom(
            "MockRestStateReader::get_transaction unimplemented",
        ))
    }
    // multi_get_transactions has a default implementation in ReadStore

    fn get_transaction_effects(
        &self,
        tx_digest: &TransactionDigest,
    ) -> StorageResult<Option<TransactionEffects>> {
        let _ = tx_digest;
        Err(StorageError::custom(
            "MockRestStateReader::get_transaction_effects unimplemented",
        ))
    }
    // multi_get_transaction_effects has a default implementation in ReadStore

    fn get_events(
        &self,
        event_digest: &TransactionEventsDigest,
    ) -> StorageResult<Option<TransactionEvents>> {
        let _ = event_digest;
        Err(StorageError::custom(
            "MockRestStateReader::get_events unimplemented",
        ))
    }
    // multi_get_events has a default implementation in ReadStore

    // Extra Checkpoint fetching apis
    fn get_full_checkpoint_contents_by_sequence_number(
        &self,
        _sequence_number: CheckpointSequenceNumber,
    ) -> StorageResult<Option<ActualFullCheckpointContents>> {
        Err(StorageError::custom(
            "MockRestStateReader::get_full_checkpoint_contents_by_sequence_number unimplemented",
        ))
    }

    fn get_full_checkpoint_contents(
        &self,
        _digest: &CheckpointContentsDigest,
    ) -> StorageResult<Option<ActualFullCheckpointContents>> {
        Err(StorageError::custom(
            "MockRestStateReader::get_full_checkpoint_contents unimplemented",
        ))
    }

    fn get_checkpoint_data(
        &self,
        verified_checkpoint: VerifiedCheckpoint,
        contents: CheckpointContents,
    ) -> anyhow::Result<CheckpointData> {
        self.get_checkpoint_data_impl(verified_checkpoint, contents)
            .map_err(anyhow::Error::new)
    }
}

// Implement ActualRestStateReader for MockRestStateReader
#[async_trait::async_trait]
impl ActualRestStateReader for MockRestStateReader {
    // Methods specific to RestStateReader
    fn get_transaction_checkpoint(
        &self,
        _digest: &TransactionDigest,
    ) -> StorageResult<Option<CheckpointSequenceNumber>> {
        Err(StorageError::custom(
            "MockRestStateReader::get_transaction_checkpoint unimplemented",
        ))
    }

    fn get_lowest_available_checkpoint_objects(&self) -> StorageResult<CheckpointSequenceNumber> {
        self.get_lowest_available_checkpoint_impl()
    }

    fn get_chain_identifier(&self) -> StorageResult<ChainIdentifier> {
        match self.get_checkpoint_by_sequence_number_impl(0) {
            Ok(Some(cp0)) => Ok(ChainIdentifier::from(*cp0.digest())),
            _ => Ok(ChainIdentifier::from(CheckpointDigest::new([0u8; 32]))),
        }
    }

    fn account_owned_objects_info_iter(
        &self,
        _owner: IotaAddress,
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
        Err(StorageError::custom(
            "MockRestStateReader::get_coin_info unimplemented",
        ))
    }

    fn get_epoch_last_checkpoint(
        &self,
        epoch_id: EpochId,
    ) -> StorageResult<Option<VerifiedCheckpoint>> {
        let cache = self.mock_checkpoints_cache.lock().unwrap();
        let last_cp_in_epoch = cache
            .values()
            .filter(|cp| cp.epoch() == epoch_id)
            .max_by_key(|cp| cp.sequence_number());
        Ok(last_cp_in_epoch.cloned().map(|arc_vc| (*arc_vc).clone()))
    }
}

// Newtype wrapper to implement traits for Arc<RwLock<MockRestStateReader>>
#[derive(Clone)]
struct TestStateReader(Arc<RwLock<MockRestStateReader>>);

// Implement traits for TestStateReader

impl ActualObjectStore for TestStateReader {
    fn get_object(&self, object_id: &ObjectID) -> StorageResult<Option<Object>> {
        self.0.read().get_object(object_id)
    }

    fn get_object_by_key(
        &self,
        object_id: &ObjectID,
        version: VersionNumber,
    ) -> StorageResult<Option<Object>> {
        self.0.read().get_object_by_key(object_id, version)
    }
}

#[async_trait::async_trait]
impl ActualReadStore for TestStateReader {
    fn get_committee(&self, epoch: EpochId) -> StorageResult<Option<Arc<Committee>>> {
        self.0.read().get_committee(epoch)
    }

    fn get_latest_checkpoint(&self) -> StorageResult<VerifiedCheckpoint> {
        self.0.read().get_latest_checkpoint()
    }

    fn get_highest_verified_checkpoint(&self) -> StorageResult<VerifiedCheckpoint> {
        self.0.read().get_highest_verified_checkpoint()
    }

    fn get_highest_synced_checkpoint(&self) -> StorageResult<VerifiedCheckpoint> {
        self.0.read().get_highest_synced_checkpoint()
    }

    fn get_lowest_available_checkpoint(&self) -> StorageResult<CheckpointSequenceNumber> {
        self.0.read().get_lowest_available_checkpoint()
    }

    fn get_checkpoint_by_digest(
        &self,
        digest: &CheckpointDigest,
    ) -> StorageResult<Option<VerifiedCheckpoint>> {
        self.0.read().get_checkpoint_by_digest(digest)
    }

    fn get_checkpoint_by_sequence_number(
        &self,
        sequence_number: CheckpointSequenceNumber,
    ) -> StorageResult<Option<VerifiedCheckpoint>> {
        self.0
            .read()
            .get_checkpoint_by_sequence_number(sequence_number)
    }

    fn get_checkpoint_contents_by_digest(
        &self,
        digest: &CheckpointContentsDigest,
    ) -> StorageResult<Option<CheckpointContents>> {
        self.0.read().get_checkpoint_contents_by_digest(digest)
    }

    fn get_checkpoint_contents_by_sequence_number(
        &self,
        sequence_number: CheckpointSequenceNumber,
    ) -> StorageResult<Option<CheckpointContents>> {
        self.0
            .read()
            .get_checkpoint_contents_by_sequence_number(sequence_number)
    }

    fn get_transaction(
        &self,
        tx_digest: &TransactionDigest,
    ) -> StorageResult<Option<Arc<VerifiedTransaction>>> {
        self.0.read().get_transaction(tx_digest)
    }

    fn get_transaction_effects(
        &self,
        tx_digest: &TransactionDigest,
    ) -> StorageResult<Option<TransactionEffects>> {
        self.0.read().get_transaction_effects(tx_digest)
    }

    fn get_events(
        &self,
        event_digest: &TransactionEventsDigest,
    ) -> StorageResult<Option<TransactionEvents>> {
        self.0.read().get_events(event_digest)
    }

    fn get_full_checkpoint_contents_by_sequence_number(
        &self,
        sequence_number: CheckpointSequenceNumber,
    ) -> StorageResult<Option<ActualFullCheckpointContents>> {
        self.0
            .read()
            .get_full_checkpoint_contents_by_sequence_number(sequence_number)
    }

    fn get_full_checkpoint_contents(
        &self,
        digest: &CheckpointContentsDigest,
    ) -> StorageResult<Option<ActualFullCheckpointContents>> {
        self.0.read().get_full_checkpoint_contents(digest)
    }

    fn get_checkpoint_data(
        &self,
        verified_checkpoint: VerifiedCheckpoint,
        contents: CheckpointContents,
    ) -> anyhow::Result<CheckpointData> {
        self.0
            .read()
            .get_checkpoint_data(verified_checkpoint, contents)
    }
}

#[async_trait::async_trait]
impl ActualRestStateReader for TestStateReader {
    fn get_transaction_checkpoint(
        &self,
        digest: &TransactionDigest,
    ) -> StorageResult<Option<CheckpointSequenceNumber>> {
        self.0.read().get_transaction_checkpoint(digest)
    }

    fn get_lowest_available_checkpoint_objects(&self) -> StorageResult<CheckpointSequenceNumber> {
        self.0.read().get_lowest_available_checkpoint_objects()
    }

    fn get_chain_identifier(&self) -> StorageResult<ChainIdentifier> {
        self.0.read().get_chain_identifier()
    }

    fn account_owned_objects_info_iter(
        &self,
        owner: IotaAddress,
        cursor: Option<ObjectID>,
    ) -> StorageResult<Box<dyn Iterator<Item = AccountOwnedObjectInfo> + '_>> {
        Ok(Box::new(
            self.0
                .read()
                .account_owned_objects_info_iter(owner, cursor)?
                .collect::<Vec<_>>()
                .into_iter(),
        ))
    }

    fn dynamic_field_iter(
        &self,
        parent: ObjectID,
        cursor: Option<ObjectID>,
    ) -> StorageResult<Box<dyn Iterator<Item = (DynamicFieldKey, DynamicFieldIndexInfo)> + '_>>
    {
        Ok(Box::new(
            self.0
                .read()
                .dynamic_field_iter(parent, cursor)?
                .collect::<Vec<_>>()
                .into_iter(),
        ))
    }

    fn get_coin_info(&self, coin_type: &StructTag) -> StorageResult<Option<CoinInfo>> {
        self.0.read().get_coin_info(coin_type)
    }

    fn get_epoch_last_checkpoint(
        &self,
        epoch_id: EpochId,
    ) -> StorageResult<Option<VerifiedCheckpoint>> {
        self.0.read().get_epoch_last_checkpoint(epoch_id)
    }
}

async fn spawn_test_server() -> (
    CheckpointGprcServiceClient<tonic::transport::Channel>,
    SocketAddr,
    broadcast::Sender<()>,                         // Shutdown sender
    Arc<parking_lot::RwLock<MockRestStateReader>>, // Keep this for direct mock access in tests
    broadcast::Sender<Arc<VerifiedCheckpoint>>,
) {
    let port = get_available_port();
    let addr: SocketAddr = format!("[::1]:{}", port).parse().unwrap();

    let (shutdown_tx, _shutdown_rx) = broadcast::channel(1); // Added back
    let server_shutdown_rx = shutdown_tx.subscribe(); // Added back

    let mock_reader_manager_inner =
        Arc::new(parking_lot::RwLock::new(MockRestStateReader::default()));
    // Wrap the Arc<RwLock<MockRestStateReader>> with TestStateReader for trait
    // implementation
    let test_state_reader_wrapper = TestStateReader(mock_reader_manager_inner.clone());
    // StateReader for GrpcServer now uses the wrapped type, which needs to be
    // Arced.
    let state_reader_for_server: StateReader = Arc::new(test_state_reader_wrapper);

    let grpc_server = GrpcServer::new(addr, state_reader_for_server); // GrpcServer creates its sender
    let actual_event_sender = grpc_server.checkpoint_event_sender(); // Get it

    mock_reader_manager_inner
        .write()
        .set_checkpoint_event_sender(actual_event_sender.clone());

    tokio::spawn(async move {
        if let Err(e) = grpc_server.start(server_shutdown_rx).await {
            eprintln!("Test gRPC server failed: {}", e);
        }
    });

    sleep(Duration::from_millis(200)).await;

    let channel = tonic::transport::Channel::from_shared(format!("http://[::1]:{}", port))
        .unwrap()
        .connect_timeout(Duration::from_secs(1))
        .connect()
        .await
        .expect("Failed to connect to test gRPC server");

    let client = CheckpointGprcServiceClient::new(channel);
    (
        client,
        addr,
        shutdown_tx,
        mock_reader_manager_inner, // Return the original Arc<RwLock<...>> for test manipulation
        actual_event_sender,
    )
}

// Helper function to create a mock VerifiedCheckpoint
fn create_mock_verified_checkpoint_detail(
    seq_num: CheckpointSequenceNumber,
    epoch_id: EpochId,
    content_digest: CheckpointContentsDigest, // Added parameter
) -> VerifiedCheckpoint {
    let summary = CheckpointSummary {
        epoch: epoch_id,
        sequence_number: seq_num,
        network_total_transactions: 1000 + seq_num,
        content_digest, // Use parameter here
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

    let keypair: AuthorityKeyPair = AuthorityKeyPair::generate(&mut rand::thread_rng());
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

fn create_mock_verified_checkpoint(
    seq_num: CheckpointSequenceNumber,
    epoch_id: EpochId,
) -> VerifiedCheckpoint {
    create_mock_verified_checkpoint_detail(
        seq_num,
        epoch_id,
        CheckpointContentsDigest::new([0u8; 32]), // Default content digest
    )
}

// Helper function to create mock CheckpointContents
fn create_mock_checkpoint_contents(_seq_num: CheckpointSequenceNumber) -> CheckpointContents {
    // For a simple mock, return empty contents
    CheckpointContents::new_with_digests_only_for_tests(Vec::new())
}

#[tokio::test]
async fn test_get_checkpoint_full_dummy() {
    let (mut client, _addr, shutdown_tx, mock_reader_manager, _event_sender) =
        spawn_test_server().await;

    let request_id_str = "999";
    let request_seq_num = request_id_str.parse::<u64>().unwrap();
    mock_reader_manager
        .write()
        .set_latest_sequence_number_for_test(request_seq_num + 10);
    let mock_checkpoint = Arc::new(create_mock_verified_checkpoint(request_seq_num, 1));
    let mock_checkpoint_contents = Arc::new(create_mock_checkpoint_contents(request_seq_num)); // Create contents

    // Add both summary and contents to their respective caches
    mock_reader_manager
        .write()
        .add_mock_checkpoint_to_cache(mock_checkpoint.clone());
    mock_reader_manager
        .write()
        .add_mock_checkpoint_contents_to_cache(request_seq_num, mock_checkpoint_contents.clone());

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

    // Compare with the mock data used to populate the cache
    let mock_core_summary_data = mock_checkpoint.data(); // This is CheckpointSummary from our mock_checkpoint

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
    // ... existing code ...
}

#[tokio::test]
async fn test_subscribe_new_checkpoints_receives_items() {
    let (mut client, _addr, shutdown_tx, mock_reader_manager, _event_sender) =
        spawn_test_server().await;

    let start_streaming_from_seq = 3u64;
    let num_checkpoints_to_target_for_publication = 5u64; // Number of checkpoints we specifically want to see after start_streaming_from_seq
    let _highest_seq_relevant_for_test =
        start_streaming_from_seq + num_checkpoints_to_target_for_publication - 1;

    // The mock reader will be triggered to publish events up to (and including)
    // this sequence number. It starts publishing from its `last_known_seq + 1`.
    // Assuming `last_known_seq` is 0. If start_streaming_from_seq is 3, and
    // highest_seq_relevant_for_test is 7,
    // we call set_latest_sequence_number_for_test(7). It will publish
    // 1,2,3,4,5,6,7. Client should get 3,4,5,6,7. That's 5 items.
    // Let's adjust this to be clearer.
    // We will publish up to sequence `X`. The client subscribes from `S`.
    // Client receives items S, S+1, ..., X. Number of items = X - S + 1.

    let first_seq_to_publish = 1u64; // Mock will start publishing from this (assuming its internal counter is 0)
    let last_seq_to_publish = 8u64; // Mock will publish up to this sequence.

    // Pre-populate all checkpoints that will be published into the cache FIRST.
    for i in first_seq_to_publish..=last_seq_to_publish {
        let mock_checkpoint = Arc::new(create_mock_verified_checkpoint(i, 1)); // Epoch 1 for simplicity
        mock_reader_manager
            .write()
            .add_mock_checkpoint_to_cache(mock_checkpoint);
    }

    let subscribe_request = tonic::Request::new(SubscribeNewCheckpointsRequest {
        start_from_checkpoint_sequence_number: Some(start_streaming_from_seq.to_string()),
        include_full_data: false,
    });

    let mut stream = client
        .subscribe_new_checkpoints(subscribe_request)
        .await
        .expect("Failed to subscribe")
        .into_inner();

    // Give a brief moment for the subscription to be processed.
    tokio::time::sleep(Duration::from_millis(200)).await; // Increased slightly

    // Simulate new checkpoints being "finalized" and published by the node.
    // This will trigger MockRestStateReader to send events.
    // The MockRestStateReader's `latest_sequence_number` is initially 0.
    // `set_latest_sequence_number_for_test(val)` publishes events from
    // `current_latest + 1` up to `val`.
    mock_reader_manager
        .write()
        .set_latest_sequence_number_for_test(last_seq_to_publish);

    let mut received_checkpoints = Vec::new();
    let expected_number_of_items = last_seq_to_publish - start_streaming_from_seq + 1;

    for i in 0..expected_number_of_items {
        match timeout(Duration::from_secs(2), stream.next()).await {
            Ok(Some(Ok(streamed_checkpoint))) => {
                println!(
                    "Test received checkpoint: {:?}",
                    streamed_checkpoint.checkpoint_type
                );
                received_checkpoints.push(streamed_checkpoint);
            }
            Ok(Some(Err(status))) => {
                panic!(
                    "Stream error after {} items: {:?}. Expected {} items.",
                    i, status, expected_number_of_items
                );
            }
            Ok(None) => {
                panic!(
                    "Stream closed prematurely after {} items. Expected {} items.",
                    i, expected_number_of_items
                );
            }
            Err(_) => {
                panic!(
                    "Timeout waiting for stream item #{} after {} items received. Expected {} items.",
                    i + 1,
                    i,
                    expected_number_of_items
                );
            }
        }
    }

    assert_eq!(
        received_checkpoints.len() as u64,
        expected_number_of_items,
        "Did not receive the expected number of checkpoints."
    );

    for (idx, streamed_cp) in received_checkpoints.iter().enumerate() {
        let expected_seq_num = start_streaming_from_seq + idx as u64;
        match &streamed_cp.checkpoint_type {
            Some(CheckpointType::Summary(summary)) => {
                assert_eq!(
                    summary.sequence_number, expected_seq_num,
                    "Received checkpoint summary with unexpected sequence number."
                );
            }
            Some(CheckpointType::FullData(_)) => {
                panic!("Received FullData checkpoint when Summary was expected.");
            }
            None => {
                panic!("Received StreamedCheckpoint with no CheckpointType.");
            }
        }
    }

    // Try to receive one more item, expecting a timeout (or Ok(None) if server
    // truly stops) to ensure no extra items are sent.
    // This depends on whether the mock continues to "publish" or if the stream
    // naturally ends. Given the broadcast setup, the stream won't end unless
    // the server task itself stops or the client is dropped.
    // The `set_latest_sequence_number_for_test` only publishes up to
    // `last_seq_to_publish`. So, no more events should be sent by the mock
    // after that. The client-side task in the service will keep running,
    // waiting for more broadcast events.
    match timeout(Duration::from_millis(500), stream.next()).await {
        Ok(Some(Ok(extra_checkpoint))) => {
            panic!(
                "Received an unexpected extra checkpoint: {:?}",
                extra_checkpoint
            );
        }
        Ok(Some(Err(status))) => {
            panic!(
                "Received an unexpected stream error when expecting no more items: {:?}",
                status
            );
        }
        Ok(None) => {
            // This means the stream was closed by the server, which is fine if it's
            // intentional after all expected items. For a broadcast
            // subscription, it usually stays open.
            println!("Stream closed as expected after all items.");
        }
        Err(_) => {
            // Timeout, which is the expected behavior if no more items are sent and stream
            // stays open.
            println!("Timeout as expected, no more items received.");
        }
    }

    let _ = shutdown_tx.send(());
}
