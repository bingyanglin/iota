use std::{
    collections::{BTreeMap, VecDeque},
    net::{SocketAddr, TcpListener},
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow;
use iota_grpc_api::{
    proto::iota::gprc::v1::{
        GetAccountInfoRequest, ListAccountObjectsRequest, StringU64,
        accounts_gprc_service_client::AccountsGprcServiceClient,
    },
    server::{GrpcServer, StateReader},
};
use iota_types::{
    base_types::{AuthorityName, IotaAddress, MoveObjectType, ObjectID, SequenceNumber},
    coin::Coin,
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
    object::{Data, MoveObject, Object, Owner},
    storage::{
        AccountOwnedObjectInfo, CoinInfo, DynamicFieldIndexInfo, DynamicFieldKey, ListDirection,
        ObjectKey, ObjectStore, ReadStore, RestStateReader, error::Result as StorageResult,
    },
    transaction::VerifiedTransaction,
};
use move_core_types::language_storage::{StructTag, TypeTag};
use rand::thread_rng;
use shared_crypto::intent::Intent;
use tokio::{sync::broadcast, time::sleep};

fn mock_object_id(id: u8) -> ObjectID {
    ObjectID::new([id; ObjectID::LENGTH])
}

fn mock_coin_object(id_byte: u8, version_u64: u64, owner_address: IotaAddress) -> Object {
    let object_id = mock_object_id(id_byte);
    let aptos_coin_struct_tag = StructTag::from_str("0x1::aptos_coin::AptosCoin").unwrap();
    let coin_type_tag = TypeTag::Struct(Box::new(aptos_coin_struct_tag.clone()));
    let move_object_type = MoveObjectType::from(aptos_coin_struct_tag);

    let move_object = MoveObject::new_coin(
        move_object_type,
        SequenceNumber::from(version_u64),
        object_id,
        100u64,
    );
    Object::new_move(
        move_object,
        Owner::AddressOwner(owner_address),
        TransactionDigest::ZERO,
    )
}

#[derive(Default)]
struct MockAccountData {
    objects: BTreeMap<ObjectID, Object>,
    owned_by_account: BTreeMap<IotaAddress, VecDeque<AccountOwnedObjectInfo>>,
}

struct MockRestStateReader {
    data: Arc<Mutex<MockAccountData>>,
}

impl Default for MockRestStateReader {
    fn default() -> Self {
        Self {
            data: Arc::new(Mutex::new(MockAccountData::default())),
        }
    }
}

impl MockRestStateReader {
    fn add_object(&self, object: Object) {
        let mut data = self.data.lock().unwrap();
        let object_id = object.id();
        let owner_address = match object.owner {
            Owner::AddressOwner(addr) => addr,
            _ => IotaAddress::ZERO,
        };

        let type_ = match &object.data {
            Data::Move(mv) => mv.type_().clone(),
            Data::Package(_) => MoveObjectType::from(
                StructTag::from_str("0x1::module::PACKAGE_PLACEHOLDER").unwrap(),
            ),
        };

        data.objects.insert(object_id, object.clone());

        let info = AccountOwnedObjectInfo {
            object_id,
            version: object.version(),
            owner: owner_address,
            type_,
        };
        data.owned_by_account
            .entry(owner_address)
            .or_default()
            .push_back(info);
    }
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
    fn get_object(&self, object_id: &ObjectID) -> StorageResult<Option<Object>> {
        let data = self.data.lock().unwrap();
        Ok(data.objects.get(object_id).cloned())
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
        owner: iota_types::base_types::IotaAddress,
        cursor: Option<ObjectID>,
    ) -> StorageResult<Box<dyn Iterator<Item = AccountOwnedObjectInfo> + '_>> {
        let data_guard = self.data.lock().unwrap();

        let owned_infos_for_account: VecDeque<AccountOwnedObjectInfo> = data_guard
            .owned_by_account
            .get(&owner)
            .map(|original_deque| {
                original_deque
                    .iter()
                    .map(|info| {
                        // Manually "clone" AccountOwnedObjectInfo as it does not derive Clone
                        // directly and VecDeque<T>.cloned() requires T:
                        // Clone.
                        AccountOwnedObjectInfo {
                            owner: info.owner,         // IotaAddress is Copy
                            object_id: info.object_id, // ObjectID is Copy
                            version: info.version,     // SequenceNumber is Copy
                            type_: info.type_.clone(), // MoveObjectType is Clone
                        }
                    })
                    .collect::<VecDeque<AccountOwnedObjectInfo>>()
            })
            .unwrap_or_default();

        let start_index = if let Some(cursor_id) = cursor {
            owned_infos_for_account
                .iter()
                .position(|info| info.object_id == cursor_id)
                .map_or(0, |idx| idx + 1)
        } else {
            0
        };

        let iter = owned_infos_for_account.into_iter().skip(start_index);
        Ok(Box::new(iter))
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

async fn spawn_test_server_with_accounts_client() -> (
    AccountsGprcServiceClient<tonic::transport::Channel>,
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
            eprintln!("[Test AccountsService] gRPC server failed: {}", e);
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
                "[Test AccountsService] Failed to connect to gRPC server at {}: {:?}",
                client_endpoint, e
            )
        });

    let client = AccountsGprcServiceClient::new(client_channel);
    (client, addr, shutdown_tx, mock_state_reader_arc)
}

#[tokio::test]
async fn test_get_account_info_placeholder() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) =
        spawn_test_server_with_accounts_client().await;

    let test_address_str = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
    let request = GetAccountInfoRequest {
        address: test_address_str.to_string(),
    };

    let response = client.get_account_info(request).await.unwrap().into_inner();

    assert_eq!(response.address, test_address_str);
    assert_eq!(
        response.balance,
        Some(StringU64 {
            value: "0".to_string()
        })
    );
    assert_eq!(
        response.sequence_number,
        Some(StringU64 {
            value: "0".to_string()
        })
    );

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_list_account_objects_empty() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) =
        spawn_test_server_with_accounts_client().await;

    let test_address = IotaAddress::new([0u8; 32]);

    let request = ListAccountObjectsRequest {
        owner_address: test_address.to_string(),
        page_size: None,
        cursor: None,
    };

    let response = client
        .list_account_objects(request)
        .await
        .unwrap()
        .into_inner();
    assert!(response.objects.is_empty());
    assert!(response.next_cursor.is_none());

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_list_account_objects_success_simple() {
    let (mut client, _addr, shutdown_tx, mock_state_reader) =
        spawn_test_server_with_accounts_client().await;

    let owner_addr = IotaAddress::new([1u8; 32]);
    let obj1 = mock_coin_object(1, 0, owner_addr);
    let obj2 = mock_coin_object(2, 0, owner_addr);
    mock_state_reader.add_object(obj1.clone());
    mock_state_reader.add_object(obj2.clone());

    let request = ListAccountObjectsRequest {
        owner_address: owner_addr.to_string(),
        page_size: None,
        cursor: None,
    };

    let response = client
        .list_account_objects(request)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.objects.len(), 2);
    assert!(response.next_cursor.is_none());

    assert_eq!(response.objects[0].object_id, obj1.id().to_hex_literal());
    assert_eq!(response.objects[1].object_id, obj2.id().to_hex_literal());

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_list_account_objects_pagination() {
    let (mut client, _addr, shutdown_tx, mock_state_reader) =
        spawn_test_server_with_accounts_client().await;

    let owner_addr = IotaAddress::new([2u8; 32]);
    let obj_ids: Vec<ObjectID> = (1..=7)
        .map(|i| mock_coin_object(i as u8, 0, owner_addr).id())
        .collect();

    for i in 0..7 {
        mock_state_reader.add_object(mock_coin_object(i as u8 + 1, 0, owner_addr));
    }

    let request1 = ListAccountObjectsRequest {
        owner_address: owner_addr.to_string(),
        page_size: Some(StringU64 {
            value: "3".to_string(),
        }),
        cursor: None,
    };
    let response1 = client
        .list_account_objects(request1)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response1.objects.len(), 3);
    assert!(response1.next_cursor.is_some());
    assert_eq!(
        response1.next_cursor.as_ref().unwrap(),
        &response1.objects[2].object_id
    );

    assert_eq!(response1.objects[0].object_id, obj_ids[0].to_hex_literal());
    assert_eq!(response1.objects[1].object_id, obj_ids[1].to_hex_literal());
    assert_eq!(response1.objects[2].object_id, obj_ids[2].to_hex_literal());

    let request2 = ListAccountObjectsRequest {
        owner_address: owner_addr.to_string(),
        page_size: Some(StringU64 {
            value: "3".to_string(),
        }),
        cursor: response1.next_cursor.clone(),
    };
    let response2 = client
        .list_account_objects(request2)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response2.objects.len(), 3);
    assert!(response2.next_cursor.is_some());
    assert_eq!(
        response2.next_cursor.as_ref().unwrap(),
        &response2.objects[2].object_id
    );

    assert_eq!(response2.objects[0].object_id, obj_ids[3].to_hex_literal());
    assert_eq!(response2.objects[1].object_id, obj_ids[4].to_hex_literal());
    assert_eq!(response2.objects[2].object_id, obj_ids[5].to_hex_literal());

    let request3 = ListAccountObjectsRequest {
        owner_address: owner_addr.to_string(),
        page_size: Some(StringU64 {
            value: "3".to_string(),
        }),
        cursor: response2.next_cursor.clone(),
    };
    let response3 = client
        .list_account_objects(request3)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response3.objects.len(), 1);
    assert!(response3.next_cursor.is_none());

    assert_eq!(response3.objects[0].object_id, obj_ids[6].to_hex_literal());

    let _ = shutdown_tx.send(());
}
