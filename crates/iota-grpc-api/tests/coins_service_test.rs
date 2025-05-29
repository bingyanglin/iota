use std::{
    collections::BTreeMap,
    net::{SocketAddr, TcpListener},
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow;
use iota_grpc_api::{
    proto::iota::gprc::v1::{
        GetCoinInfoRequest, ListCoinsRequest, coins_gprc_service_client::CoinsGprcServiceClient,
    },
    server::{GrpcServer, StateReader},
};
use iota_protocol_config::ProtocolConfig;
use iota_types::{
    base_types::{
        // AuthorityName,
        // IotaAddress,
        MoveObjectType,
        ObjectID,
        SequenceNumber, // , StructTag
    },
    coin::{CoinMetadata, TreasuryCap},
    committee::{Committee, EpochId /* , StakeUnit, TOTAL_VOTING_POWER */},
    digests::{
        ChainIdentifier, CheckpointContentsDigest, CheckpointDigest, TransactionDigest,
        TransactionEventsDigest,
    },
    effects::{TransactionEffects, TransactionEvents},
    error::{IotaError, UserInputError},
    full_checkpoint_content::CheckpointData,
    id::UID,
    messages_checkpoint::{
        CheckpointContents, CheckpointSequenceNumber, FullCheckpointContents, VerifiedCheckpoint,
    },
    object::{Data, MoveObject, Object, /* ObjectInner, */ Owner},
    quorum_driver_types::{QuorumDriverError, QuorumDriverResponse},
    storage::{
        AccountOwnedObjectInfo, CoinInfo as CoreStorageCoinInfo, DynamicFieldIndexInfo,
        DynamicFieldKey, ListDirection, ObjectKey, ObjectStore as ActualObjectStore,
        ReadStore as ActualReadStore, RestStateReader as ActualRestStateReader,
        error::Result as StorageResult,
    },
    transaction::SignedTransaction,
    transaction::VerifiedTransaction,
};
use move_core_types::language_storage::StructTag;
use tokio::{sync::broadcast, time::sleep};

// Define a mock ObjectID for IOTA coin metadata
const MOCK_IOTA_METADATA_ID_STR: &str =
    "0xdeadbeef00000000000000000000000000000000000000000000000000000001";
const MOCK_IOTA_TREASURY_ID_STR: &str =
    "0xdeadbeef00000000000000000000000000000000000000000000000000000002";

fn get_mock_iota_metadata_id() -> ObjectID {
    ObjectID::from_hex_literal(MOCK_IOTA_METADATA_ID_STR).unwrap()
}

fn get_mock_iota_treasury_id() -> ObjectID {
    ObjectID::from_hex_literal(MOCK_IOTA_TREASURY_ID_STR).unwrap()
}

#[derive(Default)]
struct MockCoinState {
    coin_infos: BTreeMap<StructTag, CoreStorageCoinInfo>,
    objects: BTreeMap<ObjectID, Object>,
}

#[derive(Default)]
struct MockRestStateReader {
    mock_state: Arc<Mutex<MockCoinState>>,
}

impl MockRestStateReader {
    fn add_coin_info(&self, tag: StructTag, info: CoreStorageCoinInfo) {
        let mut state = self.mock_state.lock().unwrap();
        state.coin_infos.insert(tag, info);
    }

    fn add_object(&self, object_id: ObjectID, object: Object) {
        let mut state = self.mock_state.lock().unwrap();
        state.objects.insert(object_id, object);
    }
}

impl ActualReadStore for MockRestStateReader {
    fn get_committee(&self, _epoch: EpochId) -> StorageResult<Option<Arc<Committee>>> {
        Ok(None)
    }
    fn get_latest_checkpoint(&self) -> StorageResult<VerifiedCheckpoint> {
        unimplemented!()
    }
    fn get_latest_checkpoint_sequence_number(&self) -> StorageResult<CheckpointSequenceNumber> {
        Ok(0)
    }
    fn get_latest_epoch_id(&self) -> StorageResult<EpochId> {
        Ok(0)
    }
    fn get_highest_verified_checkpoint(&self) -> StorageResult<VerifiedCheckpoint> {
        unimplemented!()
    }
    fn get_highest_synced_checkpoint(&self) -> StorageResult<VerifiedCheckpoint> {
        unimplemented!()
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
        Err(anyhow::anyhow!("Unimplemented"))
    }
}

impl ActualObjectStore for MockRestStateReader {
    fn get_object(&self, object_id: &ObjectID) -> StorageResult<Option<Object>> {
        let state = self.mock_state.lock().unwrap();
        Ok(state.objects.get(object_id).cloned())
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
impl ActualRestStateReader for MockRestStateReader {
    fn get_coin_info(&self, coin_type: &StructTag) -> StorageResult<Option<CoreStorageCoinInfo>> {
        let state = self.mock_state.lock().unwrap();
        Ok(state.coin_infos.get(coin_type).cloned())
    }
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

async fn spawn_test_server_with_coins_client() -> (
    CoinsGprcServiceClient<tonic::transport::Channel>,
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
            eprintln!("[Test CoinsService] gRPC server failed: {}", e);
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
                "[Test CoinsService] Failed to connect to gRPC server at {}: {:?}",
                client_endpoint, e
            )
        });

    let client = CoinsGprcServiceClient::new(client_channel);
    (client, shutdown_tx, mock_state_reader_arc)
}

// #[tokio::test]
// async fn test_get_coin_info_unimplemented() {
// let (mut client, shutdown_tx, _mock_state_reader) =
// spawn_test_server_with_coins_client().await;
//
// let request = GetCoinInfoRequest {
// coin_type_tag: "0x1::sui::SUI".to_string(), // Example tag
// };
//
// let result = client.get_coin_info(request).await;
// assert!(result.is_err());
// if let Err(status) = result {
// assert_eq!(status.code(), tonic::Code::Unimplemented); // This was the old
// check assert!(status.message().contains("GetCoinInfo not implemented"));
// }
// drop(shutdown_tx);
// }

#[tokio::test]
async fn test_list_coins_returns_empty_as_placeholder() {
    let (mut client, shutdown_tx, _mock_state_reader) = spawn_test_server_with_coins_client().await;

    let request = ListCoinsRequest {
        page_size: None,
        cursor: None,
    };
    let result = client.list_coins(request).await;

    assert!(
        result.is_ok(),
        "ListCoins should now return Ok an empty list as a placeholder"
    );
    let response = result.unwrap().into_inner();
    assert!(response.coins.is_empty(), "Expected empty coins list");
    assert!(response.next_cursor.is_none(), "Expected no next_cursor");

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_get_coin_info_success() {
    let (mut client, shutdown_tx, mock_state_reader) = spawn_test_server_with_coins_client().await;

    let test_coin_type_tag_str = "0x2::iota::IOTA";
    let test_coin_type_tag = StructTag::from_str(test_coin_type_tag_str).unwrap();

    let mock_metadata_id = get_mock_iota_metadata_id();
    let mock_treasury_id = get_mock_iota_treasury_id();

    // 1. Setup Mock CoreStorageCoinInfo
    let core_coin_info = CoreStorageCoinInfo {
        coin_metadata_object_id: Some(mock_metadata_id),
        treasury_object_id: Some(mock_treasury_id),
    };
    mock_state_reader.add_coin_info(test_coin_type_tag.clone(), core_coin_info);

    // 2. Setup Mock CoinMetadata Object
    let iota_metadata = iota_types::coin::CoinMetadata {
        id: UID::new(mock_metadata_id), // The ID of this metadata object itself
        decimals: 6,
        name: "Iota".to_string(),
        symbol: "IOTA".to_string(),
        description: "The native IOTA token".to_string(),
        icon_url: None,
    };
    let metadata_bcs = bcs::to_bytes(&iota_metadata).unwrap();
    let metadata_move_object = MoveObject::new_from_execution(
        MoveObjectType::from(CoinMetadata::type_(test_coin_type_tag.clone())),
        SequenceNumber::from_u64(0), // version for genesis object
        metadata_bcs,
        &ProtocolConfig::get_for_min_version(), // protocol_config
    )
    .unwrap();
    let metadata_object = Object::new_from_genesis(
        Data::Move(metadata_move_object),
        Owner::Immutable,
        TransactionDigest::ZERO, // previous_transaction for genesis object
    );
    mock_state_reader.add_object(mock_metadata_id, metadata_object);

    // 3. Setup Mock TreasuryCap Object (needed for total_supply)
    let treasury_cap = TreasuryCap {
        id: UID::new(mock_treasury_id),
        total_supply: iota_types::balance::Supply { value: 1_000_000 },
    };
    let treasury_bcs = bcs::to_bytes(&treasury_cap).unwrap();
    let treasury_move_object = MoveObject::new_from_execution(
        MoveObjectType::from(TreasuryCap::type_(test_coin_type_tag.clone())),
        SequenceNumber::from_u64(0), // version for genesis object
        treasury_bcs,
        &ProtocolConfig::get_for_min_version(), // protocol_config
    )
    .unwrap();
    let treasury_object = Object::new_from_genesis(
        Data::Move(treasury_move_object),
        Owner::Shared {
            initial_shared_version: SequenceNumber::from_u64(0),
        }, // Example owner
        TransactionDigest::ZERO, // previous_transaction for genesis object
    );
    mock_state_reader.add_object(mock_treasury_id, treasury_object);

    let request = GetCoinInfoRequest {
        coin_type_tag: test_coin_type_tag_str.to_string(),
    };
    let response = client.get_coin_info(request).await.unwrap().into_inner();

    assert_eq!(response.coin_type_tag, test_coin_type_tag_str);
    assert_eq!(response.total_supply.unwrap().value, "1000000");
    assert!(response.balance.is_none());

    // Metadata assertions
    assert_eq!(response.name.as_deref(), Some("Iota"));
    assert_eq!(response.symbol.as_deref(), Some("IOTA"));
    assert_eq!(response.decimals, Some(6));
    assert_eq!(
        response.description.as_deref(),
        Some("The native IOTA token")
    );
    assert!(response.icon_url.is_none());
    assert_eq!(
        response.metadata_object_id.as_deref(),
        Some(MOCK_IOTA_METADATA_ID_STR)
    );

    let _ = shutdown_tx.send(()); // Shutdown server
    sleep(Duration::from_millis(50)).await;
}

#[tokio::test]
async fn test_get_coin_info_not_found() {
    let (mut client, shutdown_tx, _mock_state_reader) = spawn_test_server_with_coins_client().await;
    let coin_tag_str = "0x3::nonexistent::Coin";

    let request = tonic::Request::new(GetCoinInfoRequest {
        coin_type_tag: coin_tag_str.to_string(),
    });
    let response = client.get_coin_info(request).await;

    assert!(response.is_err());
    if let Err(status) = response {
        assert_eq!(status.code(), tonic::Code::NotFound);
    }
    let _ = shutdown_tx.send(());
    sleep(Duration::from_millis(50)).await;
}

#[tokio::test]
async fn test_get_coin_info_invalid_tag() {
    let (mut client, shutdown_tx, _mock_state_reader) = spawn_test_server_with_coins_client().await;
    let invalid_tag_str = "not-a-valid-tag";

    let request = tonic::Request::new(GetCoinInfoRequest {
        coin_type_tag: invalid_tag_str.to_string(),
    });
    let response = client.get_coin_info(request).await;

    assert!(response.is_err());
    if let Err(status) = response {
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }
    let _ = shutdown_tx.send(());
    sleep(Duration::from_millis(50)).await;
}
