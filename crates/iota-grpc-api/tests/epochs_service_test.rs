use std::{
    collections::BTreeMap,
    net::{SocketAddr, TcpListener},
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow;
use iota_grpc_api::{
    proto::iota::gprc::v1::{
        Direction as GrpcDirection, EpochIdGprc, GetEpochInfoRequest, ListEpochsRequest,
        epochs_gprc_service_client::EpochsGprcServiceClient,
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
    error::{IotaError, UserInputError},
    full_checkpoint_content::CheckpointData,
    gas::GasCostSummary,
    message_envelope::{Envelope, Message, VerifiedEnvelope},
    messages_checkpoint::{
        CheckpointContents, CheckpointSequenceNumber, CheckpointSummary, FullCheckpointContents,
        VerifiedCheckpoint,
    },
    object::Object,
    quorum_driver_types::{QuorumDriverError, QuorumDriverResponse},
    storage::{
        AccountOwnedObjectInfo, CoinInfo, DynamicFieldIndexInfo, DynamicFieldKey, ListDirection,
        ObjectKey, ObjectStore, ReadStore, RestStateReader, error::Result as StorageResult,
    },
    transaction::{SignedTransaction, VerifiedTransaction},
};
use move_core_types::language_storage::StructTag;
use rand::thread_rng;
use shared_crypto::intent::Intent;
use tokio::{sync::broadcast, time::sleep};

#[derive(Default)]
struct MockEpochState {
    committees: BTreeMap<EpochId, Arc<Committee>>,
    latest_epoch_id: EpochId,
}

struct MockRestStateReader {
    mock_state: Arc<Mutex<MockEpochState>>,
}

impl Default for MockRestStateReader {
    fn default() -> Self {
        Self {
            mock_state: Arc::new(Mutex::new(MockEpochState::default())),
        }
    }
}

impl MockRestStateReader {
    fn add_committee(&self, epoch_id: EpochId, committee: Committee) {
        let mut state = self.mock_state.lock().unwrap();
        state.committees.insert(epoch_id, Arc::new(committee));
    }

    fn set_latest_epoch_id(&self, epoch_id: EpochId) {
        let mut state = self.mock_state.lock().unwrap();
        state.latest_epoch_id = epoch_id;
    }
}

fn create_mock_committee(epoch_id: EpochId) -> Committee {
    let keypair: AuthorityKeyPair = AuthorityKeyPair::generate(&mut thread_rng());
    let authority_name = AuthorityName::from(keypair.public());
    Committee::new(
        epoch_id,
        BTreeMap::from([(authority_name, TOTAL_VOTING_POWER)]),
    )
}

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
    let temp_committee = Committee::new(epoch_id, BTreeMap::from([(authority_name, stake)]));

    let sign_info = AuthoritySignInfo::new(
        epoch_id,
        &summary,
        Intent::iota_app(CheckpointSummary::SCOPE),
        authority_name,
        &keypair,
    );

    let auth_strong_quorum_sig =
        AuthorityStrongQuorumSignInfo::new_from_auth_sign_infos(vec![sign_info], &temp_committee)
            .expect("Mock AuthStrongQuorumSignInfo creation failed");

    let envelope = Envelope::new_from_data_and_sig(summary, auth_strong_quorum_sig);
    VerifiedEnvelope::new_unchecked(envelope)
}

impl ReadStore for MockRestStateReader {
    fn get_committee(&self, epoch_id: EpochId) -> StorageResult<Option<Arc<Committee>>> {
        let state = self.mock_state.lock().unwrap();
        Ok(state.committees.get(&epoch_id).cloned())
    }
    fn get_latest_checkpoint(&self) -> StorageResult<VerifiedCheckpoint> {
        Ok(create_mock_verified_checkpoint(
            self.mock_state.lock().unwrap().latest_epoch_id,
            0,
        ))
    }
    fn get_latest_checkpoint_sequence_number(&self) -> StorageResult<CheckpointSequenceNumber> {
        Ok(0)
    }
    fn get_latest_epoch_id(&self) -> StorageResult<EpochId> {
        let state = self.mock_state.lock().unwrap();
        Ok(state.latest_epoch_id)
    }
    fn get_highest_verified_checkpoint(&self) -> StorageResult<VerifiedCheckpoint> {
        Ok(create_mock_verified_checkpoint(
            self.mock_state.lock().unwrap().latest_epoch_id,
            0,
        ))
    }
    fn get_highest_synced_checkpoint(&self) -> StorageResult<VerifiedCheckpoint> {
        Ok(create_mock_verified_checkpoint(
            self.mock_state.lock().unwrap().latest_epoch_id,
            0,
        ))
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

#[async_trait::async_trait]
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
        unimplemented!()
    }
    fn account_owned_objects_info_iter(
        &self,
        _owner: iota_types::base_types::IotaAddress,
        _cursor: Option<ObjectID>,
    ) -> StorageResult<Box<dyn Iterator<Item = AccountOwnedObjectInfo> + '_>> {
        unimplemented!()
    }
    fn dynamic_field_iter(
        &self,
        _parent: ObjectID,
        _cursor: Option<ObjectID>,
    ) -> StorageResult<Box<dyn Iterator<Item = (DynamicFieldKey, DynamicFieldIndexInfo)> + '_>>
    {
        unimplemented!()
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

    async fn execute_transaction_for_gprc(
        &self,
        _transaction: SignedTransaction,
    ) -> std::result::Result<QuorumDriverResponse, QuorumDriverError> {
        Err(QuorumDriverError::QuorumDriverInternal(
            IotaError::UserInput {
                error: UserInputError::Unsupported(
                    "execute_transaction_for_gprc is not supported in this mock.".to_string(),
                ),
            },
        ))
    }
}

fn get_available_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn spawn_test_server_with_epochs_client() -> (
    EpochsGprcServiceClient<tonic::transport::Channel>,
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
            eprintln!("[Test EpochsService] gRPC server failed: {}", e);
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
                "[Test EpochsService] Failed to connect to gRPC server at {}: {:?}",
                client_endpoint, e
            )
        });

    let client = EpochsGprcServiceClient::new(client_channel);
    (client, shutdown_tx, mock_state_reader_arc)
}

#[tokio::test]
async fn test_get_epoch_info_success() {
    let (mut client, shutdown_tx, mock_state_reader_arc) =
        spawn_test_server_with_epochs_client().await;

    let test_epoch_id: EpochId = 5;
    let mock_committee = create_mock_committee(test_epoch_id);
    mock_state_reader_arc.add_committee(test_epoch_id, mock_committee);

    let request = GetEpochInfoRequest {
        epoch_id: Some(EpochIdGprc {
            epoch: test_epoch_id,
        }),
    };

    let response = client.get_epoch_info(request).await.unwrap().into_inner();

    assert_eq!(response.epoch_id.as_ref().unwrap().epoch, test_epoch_id);
    assert_eq!(
        response.total_stake.as_ref().unwrap().value,
        TOTAL_VOTING_POWER.to_string()
    );
    assert!(response.start_time_ms.is_none());
    assert!(response.end_time_ms.is_none());
    assert!(response.rewards_pool_balance.is_none());

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_get_epoch_info_latest_success() {
    let (mut client, shutdown_tx, mock_state_reader_arc) =
        spawn_test_server_with_epochs_client().await;

    let latest_epoch_id: EpochId = 10;
    let mock_committee = create_mock_committee(latest_epoch_id);
    mock_state_reader_arc.add_committee(latest_epoch_id, mock_committee);
    mock_state_reader_arc.set_latest_epoch_id(latest_epoch_id);

    let request = GetEpochInfoRequest { epoch_id: None };

    let response = client.get_epoch_info(request).await.unwrap().into_inner();

    assert_eq!(response.epoch_id.as_ref().unwrap().epoch, latest_epoch_id);
    assert_eq!(
        response.total_stake.as_ref().unwrap().value,
        TOTAL_VOTING_POWER.to_string()
    );

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_get_epoch_info_not_found() {
    let (mut client, shutdown_tx, _mock_state_reader_arc) =
        spawn_test_server_with_epochs_client().await;

    let request = GetEpochInfoRequest {
        epoch_id: Some(EpochIdGprc { epoch: 99 }),
    };

    let result = client.get_epoch_info(request).await;
    assert!(result.is_err());
    if let Err(status) = result {
        assert_eq!(status.code(), tonic::Code::NotFound);
    }
    drop(shutdown_tx);
}

#[tokio::test]
async fn test_list_epochs_returns_empty() {
    let (mut client, shutdown_tx, _mock_state_reader) =
        spawn_test_server_with_epochs_client().await;

    let request = ListEpochsRequest {
        start_epoch: None,
        end_epoch: None,
        page_size: None,
        cursor: None,
        direction: GrpcDirection::Ascending as i32,
    };

    let result = client.list_epochs(request).await;
    assert!(
        result.is_ok(),
        "ListEpochs should return Ok an empty list as a placeholder"
    );
    let response = result.unwrap().into_inner();
    assert!(response.epochs.is_empty());
    assert!(response.next_cursor.is_none());

    drop(shutdown_tx);
}
