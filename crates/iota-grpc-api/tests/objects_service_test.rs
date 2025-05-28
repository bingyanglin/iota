use std::{
    collections::{BTreeMap, HashSet}, // Added BTreeMap
    net::{SocketAddr, TcpListener},
    str::FromStr, // Added FromStr trait for .parse() / ::from_str()
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
};

use anyhow; // Added anyhow
use futures::StreamExt;
use iota_grpc_api::{
    proto::iota::gprc::v1::{
        GetObjectRequest,
        ListObjectsRequest, // Added for new tests
        StreamObjectsRequest,
        object_gprc_service_client::ObjectGprcServiceClient,
    },
    server::{GrpcServer, StateReader},
};
use iota_protocol_config;
use iota_types::{
    base_types::{AuthorityName, IotaAddress, MoveObjectType, ObjectID, SequenceNumber}, /* Removed TransactionDigest from here as it's also in digests */
    committee::{Committee, EpochId, StakeUnit, TOTAL_VOTING_POWER}, /* Added Committee, EpochId,
                                                                     * StakeUnit,
                                                                     * TOTAL_VOTING_POWER */
    crypto::{AuthorityKeyPair, AuthoritySignInfo, AuthorityStrongQuorumSignInfo, KeypairTraits}, /* Added crypto types */
    digests::{
        ChainIdentifier,
        CheckpointContentsDigest,
        CheckpointDigest,
        TransactionDigest, // Ensured TransactionDigest is here
        TransactionEventsDigest,
    },
    effects::{TransactionEffects, TransactionEvents},
    full_checkpoint_content::CheckpointData,
    gas::GasCostSummary, // Added GasCostSummary
    message_envelope::{Envelope, Message, VerifiedEnvelope}, /* Added Message, Envelope,
                          * VerifiedEnvelope */
    messages_checkpoint::{
        CheckpointContents,
        CheckpointSequenceNumber,
        CheckpointSummary, // Added CheckpointSummary
        FullCheckpointContents as ActualFullCheckpointContents,
        VerifiedCheckpoint,
    },
    object::{Data, MoveObject, Object, ObjectInner, Owner},
    storage::{
        AccountOwnedObjectInfo,
        CoinInfo,
        DynamicFieldIndexInfo,
        DynamicFieldKey,
        ListDirection,
        ObjectKey, // Added ObjectKey
        ObjectStore as ActualObjectStore,
        ReadStore as ActualReadStore,
        RestStateReader as ActualRestStateReader,
        error::Result as StorageResult,
    },
    transaction::VerifiedTransaction,
};
use move_core_types::language_storage::StructTag;
use rand::thread_rng; // Added rand::thread_rng
use shared_crypto::intent::Intent; // Added Intent
use tokio::{
    sync::broadcast,
    time::{Duration, sleep, timeout},
}; // For stream.next()

struct MockRestStateReader {
    latest_sequence_number: Arc<AtomicU64>,
}

impl Default for MockRestStateReader {
    fn default() -> Self {
        Self {
            latest_sequence_number: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl MockRestStateReader {
    #[allow(dead_code)]
    fn set_latest_sequence_number_for_test(&self, val: u64) {
        self.latest_sequence_number.store(val, Ordering::SeqCst);
    }
}

// Copied and adapted create_mock_verified_checkpoint
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
        Intent::iota_app(CheckpointSummary::SCOPE), /* CheckpointSummary needs Message trait in
                                                     * scope for SCOPE */
        authority_name,
        &keypair,
    );

    let auth_strong_quorum_sig =
        AuthorityStrongQuorumSignInfo::new_from_auth_sign_infos(vec![sign_info], &committee)
            .expect("Mock AuthStrongQuorumSignInfo creation failed");

    let envelope = Envelope::new_from_data_and_sig(summary, auth_strong_quorum_sig);
    VerifiedEnvelope::new_unchecked(envelope)
}

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

const TEST_OWNER_ADDRESS_HEX: &str =
    "0xaabbccddeeff00112233445566778899aabbccddeeff00112233445566778899";
const NUM_MOCK_OWNED_OBJECTS: usize = 15;

fn get_mock_owned_object_ids(owner_param: &IotaAddress) -> Vec<ObjectID> {
    let owner_addr_parsed = IotaAddress::from_str(TEST_OWNER_ADDRESS_HEX).unwrap();
    if *owner_param != owner_addr_parsed {
        return vec![];
    }
    (0..NUM_MOCK_OWNED_OBJECTS)
        .map(|i| {
            let mut bytes = [0u8; 32];
            bytes[0] = 0x70; // Prefix to distinguish from other mock IDs
            bytes[31] = i as u8;
            ObjectID::new(bytes)
        })
        .collect()
}

impl ActualObjectStore for MockRestStateReader {
    fn get_object(&self, object_id: &ObjectID) -> StorageResult<Option<Object>> {
        let known_main_test_id_str =
            "0x1234500000000000000000000000000000000000000000000000000000000000";
        let known_main_test_id = ObjectID::from_hex_literal(known_main_test_id_str).unwrap();

        if *object_id == known_main_test_id {
            return Ok(Some(create_mock_object(object_id, 2)));
        }

        let owner_addr_for_list_check = IotaAddress::from_str(TEST_OWNER_ADDRESS_HEX).unwrap();
        let owned_ids = get_mock_owned_object_ids(&owner_addr_for_list_check);
        if owned_ids.contains(object_id) {
            return Ok(Some(create_mock_object(object_id, 1)));
        }
        Ok(None)
    }

    fn get_object_by_key(
        &self,
        object_id: &ObjectID,
        version: SequenceNumber,
    ) -> StorageResult<Option<Object>> {
        let known_main_test_id_str =
            "0x1234500000000000000000000000000000000000000000000000000000000000";
        let known_main_test_id = ObjectID::from_hex_literal(known_main_test_id_str).unwrap();

        if *object_id == known_main_test_id {
            if version.value() == 1 {
                return Ok(Some(create_mock_object(object_id, 1)));
            } else if version.value() == 2 {
                return Ok(Some(create_mock_object(object_id, 2)));
            } else {
                return Ok(None);
            }
        }

        let owner_addr_for_list_check = IotaAddress::from_str(TEST_OWNER_ADDRESS_HEX).unwrap();
        let owned_ids = get_mock_owned_object_ids(&owner_addr_for_list_check);
        if owned_ids.contains(object_id) {
            if version.value() == 1 {
                return Ok(Some(create_mock_object(object_id, 1)));
            } else {
                return Ok(None);
            }
        }
        Ok(None)
    }
    fn multi_get_objects_by_key(
        &self,
        _object_keys: &[ObjectKey],
    ) -> StorageResult<Vec<Option<Object>>> {
        Ok(Vec::new())
    }
}

impl ActualReadStore for MockRestStateReader {
    fn get_committee(&self, _epoch: EpochId) -> StorageResult<Option<Arc<Committee>>> {
        Ok(None)
    }
    fn get_latest_checkpoint(&self) -> StorageResult<VerifiedCheckpoint> {
        Ok(create_mock_verified_checkpoint(0, 0))
    }
    fn get_latest_checkpoint_sequence_number(&self) -> StorageResult<CheckpointSequenceNumber> {
        Ok(self.latest_sequence_number.load(Ordering::SeqCst))
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
    ) -> StorageResult<Option<ActualFullCheckpointContents>> {
        Ok(None)
    }
    fn get_full_checkpoint_contents(
        &self,
        _digest: &CheckpointContentsDigest,
    ) -> StorageResult<Option<ActualFullCheckpointContents>> {
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

impl ActualRestStateReader for MockRestStateReader {
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
        owner: IotaAddress,
        cursor: Option<ObjectID>,
    ) -> StorageResult<Box<dyn Iterator<Item = AccountOwnedObjectInfo> + '_>> {
        let expected_mock_owner = IotaAddress::from_str(TEST_OWNER_ADDRESS_HEX).unwrap();
        if owner != expected_mock_owner {
            return Ok(Box::new(std::iter::empty()));
        }

        let all_mock_object_ids: Vec<ObjectID> = get_mock_owned_object_ids(&owner);

        let all_mock_infos: Vec<AccountOwnedObjectInfo> = all_mock_object_ids
            .into_iter()
            .map(|id| {
                // Create a dummy MoveObjectType for the mock
                let dummy_struct_tag = "0x1::dummy::Dummy".parse::<StructTag>().unwrap();
                AccountOwnedObjectInfo {
                    owner,
                    object_id: id,
                    version: SequenceNumber::from_u64(1),
                    type_: MoveObjectType::from(dummy_struct_tag),
                }
            })
            .collect();

        let mut start_index = 0;
        if let Some(cursor_id) = cursor {
            if let Some(pos) = all_mock_infos
                .iter()
                .position(|info| info.object_id == cursor_id)
            {
                start_index = pos + 1;
            } else {
                return Ok(Box::new(std::iter::empty()));
            }
        }
        let iter = all_mock_infos.into_iter().skip(start_index);
        Ok(Box::new(iter))
    }
    fn dynamic_field_iter(
        &self,
        _parent: ObjectID,
        _cursor: Option<ObjectID>,
    ) -> StorageResult<Box<dyn Iterator<Item = (DynamicFieldKey, DynamicFieldIndexInfo)> + '_>>
    {
        unimplemented!("Mock: dynamic_field_iter")
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

async fn spawn_test_server_with_object_client() -> (
    ObjectGprcServiceClient<tonic::transport::Channel>,
    SocketAddr,
    broadcast::Sender<()>,
    Arc<MockRestStateReader>,
) {
    let port = get_available_port();
    let addr: SocketAddr = format!("[::1]:{}", port).parse().unwrap();

    let state_reader_arc = Arc::new(MockRestStateReader::default());

    let (shutdown_tx, server_shutdown_rx) = broadcast::channel(1);

    let grpc_server = GrpcServer::new(addr, state_reader_arc.clone() as StateReader);

    tokio::spawn(async move {
        if let Err(e) = grpc_server.start(server_shutdown_rx).await {
            eprintln!("[Test ObjectService] gRPC server failed: {}", e);
        }
    });

    sleep(Duration::from_millis(200)).await;

    let endpoint_uri = format!("http://[::1]:{}", port);
    let channel = tonic::transport::Channel::from_shared(endpoint_uri.clone())
        .unwrap()
        .connect_timeout(Duration::from_secs(5))
        .connect()
        .await
        .unwrap_or_else(|e| {
            panic!(
                "Failed to connect to test gRPC server at {}: {:?}",
                endpoint_uri, e
            )
        });

    let client = ObjectGprcServiceClient::new(channel);
    (client, addr, shutdown_tx, state_reader_arc)
}

#[tokio::test]
async fn test_get_object_retrieves_and_converts() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) =
        spawn_test_server_with_object_client().await;
    let request_object_id_hex =
        "0x1234500000000000000000000000000000000000000000000000000000000000";
    let request = tonic::Request::new(GetObjectRequest {
        object_id: request_object_id_hex.to_string(),
        version: None,
    });

    let response_result = timeout(Duration::from_secs(5), client.get_object(request))
        .await
        .expect("GetObject RPC call timed out");

    assert!(
        response_result.is_ok(),
        "GetObject RPC call failed: {:?}",
        response_result.err()
    );
    let response = response_result.unwrap();
    let object_gprc = response.into_inner();

    assert_eq!(object_gprc.object_id, request_object_id_hex);
    assert_eq!(object_gprc.version, "0x2");
    assert_eq!(object_gprc.owner_type, "AddressOwner");
    assert_eq!(
        object_gprc.data_type,
        "0x0000000000000000000000000000000000000000000000000000000000000042::example_module::ExampleObject"
    );
    assert_eq!(object_gprc.raw_object, b"mock_contents".to_vec());

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_get_object_with_specific_version_found() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) =
        spawn_test_server_with_object_client().await;
    let request_object_id_hex =
        "0x1234500000000000000000000000000000000000000000000000000000000000";
    let request_version_str = "1";

    let request = tonic::Request::new(GetObjectRequest {
        object_id: request_object_id_hex.to_string(),
        version: Some(request_version_str.to_string()),
    });

    let response = client.get_object(request).await.unwrap().into_inner();
    assert_eq!(response.object_id, request_object_id_hex);
    assert_eq!(response.version, "0x1");
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_get_object_with_specific_version_not_found() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) =
        spawn_test_server_with_object_client().await;
    let request_object_id_hex =
        "0x1234500000000000000000000000000000000000000000000000000000000000";
    let request_version_str = "3";

    let request = tonic::Request::new(GetObjectRequest {
        object_id: request_object_id_hex.to_string(),
        version: Some(request_version_str.to_string()),
    });

    let response = client.get_object(request).await;
    assert!(response.is_err());
    let status = response.err().unwrap();
    assert_eq!(status.code(), tonic::Code::NotFound);
    assert!(status.message().contains(&format!(
        "Object with ID {} at version {} not found",
        request_object_id_hex, request_version_str
    )));
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_get_object_with_empty_version_string() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) =
        spawn_test_server_with_object_client().await;
    let request_object_id_hex =
        "0x1234500000000000000000000000000000000000000000000000000000000000";

    let request = tonic::Request::new(GetObjectRequest {
        object_id: request_object_id_hex.to_string(),
        version: Some("".to_string()),
    });
    let response = client.get_object(request).await.unwrap().into_inner();
    assert_eq!(response.object_id, request_object_id_hex);
    assert_eq!(response.version, "0x2");
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_get_object_with_invalid_version_string() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) =
        spawn_test_server_with_object_client().await;
    let request_object_id_hex =
        "0x1234500000000000000000000000000000000000000000000000000000000000";
    let invalid_version_str = "not_a_number";

    let request = tonic::Request::new(GetObjectRequest {
        object_id: request_object_id_hex.to_string(),
        version: Some(invalid_version_str.to_string()),
    });

    let response = client.get_object(request).await;
    assert!(response.is_err());
    let status = response.err().unwrap();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(status.message().contains(&format!(
        "Could not parse version '{}' as u64",
        invalid_version_str
    )));
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_get_object_not_found() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) =
        spawn_test_server_with_object_client().await;

    let request_object_id_hex =
        "0xffffff0000000000000000000000000000000000000000000000000000000000";

    let request = tonic::Request::new(GetObjectRequest {
        object_id: request_object_id_hex.to_string(),
        version: None,
    });

    let response_result = timeout(Duration::from_secs(5), client.get_object(request)).await;
    assert!(
        response_result.is_ok(),
        "GetObject RPC call should not time out for not found"
    );
    let response = response_result.unwrap();

    assert!(
        response.is_err(),
        "Expected GetObject to fail for non-existent ID"
    );
    let status = response.err().unwrap();
    assert_eq!(status.code(), tonic::Code::NotFound);
    assert!(status.message().contains(&format!(
        "Object with ID {}  not found",
        request_object_id_hex
    )));

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_get_object_invalid_id_parse() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) =
        spawn_test_server_with_object_client().await;

    let request_object_id_str = "not_an_object_id";
    let request = tonic::Request::new(GetObjectRequest {
        object_id: request_object_id_str.to_string(),
        version: None,
    });

    let response_result = timeout(Duration::from_secs(5), client.get_object(request)).await;
    assert!(
        response_result.is_ok(),
        "GetObject RPC call should not time out for invalid ID"
    );
    let response = response_result.unwrap();

    assert!(
        response.is_err(),
        "Expected GetObject to fail for invalid ID format"
    );
    let status = response.err().unwrap();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(status.message().contains(&format!(
        "Could not parse object_id '{}'",
        request_object_id_str
    )));

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_get_object_empty_id() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) =
        spawn_test_server_with_object_client().await;

    let request = tonic::Request::new(GetObjectRequest {
        object_id: "".to_string(),
        version: None,
    });

    let response_result = timeout(Duration::from_secs(5), client.get_object(request)).await;
    assert!(
        response_result.is_ok(),
        "GetObject RPC call should not time out for empty ID"
    );
    let response = response_result.unwrap();

    assert!(response.is_err(), "Expected GetObject to fail for empty ID");
    let status = response.err().unwrap();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(status.message().contains("Could not parse object_id"));

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_list_objects_first_page() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) =
        spawn_test_server_with_object_client().await;

    let request = tonic::Request::new(ListObjectsRequest {
        owner_address: Some(TEST_OWNER_ADDRESS_HEX.to_string()),
        cursor: None,
        limit: Some(5),
    });

    let response = client.list_objects(request).await.unwrap().into_inner();

    assert_eq!(response.objects.len(), 5);
    assert!(response.next_cursor.is_some());
    // Further checks: object IDs are distinct and as expected from mock
    let mut seen_ids = HashSet::new();
    for (i, obj) in response.objects.iter().enumerate() {
        let expected_id_byte = i as u8; // Based on get_mock_owned_object_ids
        assert!(
            obj.object_id
                .ends_with(&format!("{:02x}", expected_id_byte))
        );
        assert!(
            obj.object_id
                .starts_with("0x70000000000000000000000000000000000000000000000000000000000000")
        );
        assert_eq!(obj.version, "0x1"); // Mock lists version 1 objects
        assert!(seen_ids.insert(obj.object_id.clone()));
    }

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_list_objects_pagination() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) =
        spawn_test_server_with_object_client().await;

    let limit_per_page = (NUM_MOCK_OWNED_OBJECTS / 3) as u32; // e.g., 15 / 3 = 5

    // Page 1
    let request1 = tonic::Request::new(ListObjectsRequest {
        owner_address: Some(TEST_OWNER_ADDRESS_HEX.to_string()),
        cursor: None,
        limit: Some(limit_per_page),
    });
    let response1 = client.list_objects(request1).await.unwrap().into_inner();
    assert_eq!(response1.objects.len(), limit_per_page as usize);
    assert!(response1.next_cursor.is_some());
    let cursor1 = response1.next_cursor.clone();
    let mut seen_ids_page1 = HashSet::new();
    for obj in &response1.objects {
        seen_ids_page1.insert(obj.object_id.clone());
    }

    // Page 2
    let request2 = tonic::Request::new(ListObjectsRequest {
        owner_address: Some(TEST_OWNER_ADDRESS_HEX.to_string()),
        cursor: cursor1,
        limit: Some(limit_per_page),
    });
    let response2 = client.list_objects(request2).await.unwrap().into_inner();
    assert_eq!(response2.objects.len(), limit_per_page as usize);
    assert!(response2.next_cursor.is_some());
    let cursor2 = response2.next_cursor.clone();
    let mut seen_ids_page2 = HashSet::new();
    for obj in &response2.objects {
        assert!(
            !seen_ids_page1.contains(&obj.object_id),
            "Object ID from page 1 seen in page 2"
        );
        seen_ids_page2.insert(obj.object_id.clone());
    }

    // Page 3 (and potentially last part)
    let request3 = tonic::Request::new(ListObjectsRequest {
        owner_address: Some(TEST_OWNER_ADDRESS_HEX.to_string()),
        cursor: cursor2,
        limit: Some(limit_per_page + 5),
    });
    let response3 = client.list_objects(request3).await.unwrap().into_inner();

    let expected_items_page3 = 3;
    assert_eq!(
        response3.objects.len(),
        expected_items_page3,
        "Incorrect number of items on page 3"
    );

    assert!(
        response3.next_cursor.is_none(),
        "Expected no next cursor on the last page"
    );

    for obj in &response3.objects {
        assert!(
            !seen_ids_page1.contains(&obj.object_id),
            "Object ID from page 1 seen in page 3"
        );
        assert!(
            !seen_ids_page2.contains(&obj.object_id),
            "Object ID from page 2 seen in page 3"
        );
    }

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_list_objects_limit_respected() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) =
        spawn_test_server_with_object_client().await;
    let limit = 3u32;
    let request = tonic::Request::new(ListObjectsRequest {
        owner_address: Some(TEST_OWNER_ADDRESS_HEX.to_string()),
        cursor: None,
        limit: Some(limit),
    });
    let response = client.list_objects(request).await.unwrap().into_inner();
    assert_eq!(response.objects.len(), limit as usize);
    assert!(response.next_cursor.is_some()); // Assuming more than `limit` objects exist
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_list_objects_owner_not_found() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) =
        spawn_test_server_with_object_client().await;
    let unknown_owner = "0xdeadbeef000000000000000000000000000000000000000000000000deadbeef";
    let request = tonic::Request::new(ListObjectsRequest {
        owner_address: Some(unknown_owner.to_string()),
        cursor: None,
        limit: Some(5),
    });
    let response = client.list_objects(request).await.unwrap().into_inner();
    assert!(response.objects.is_empty());
    assert!(response.next_cursor.is_none());
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_list_objects_missing_owner_address() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) =
        spawn_test_server_with_object_client().await;
    let request = tonic::Request::new(ListObjectsRequest {
        owner_address: None,
        cursor: None,
        limit: Some(5),
    });
    let response = client.list_objects(request).await;
    assert!(response.is_err());
    let status = response.err().unwrap();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(status.message().contains("owner_address is required"));
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_list_objects_invalid_owner_address() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) =
        spawn_test_server_with_object_client().await;
    let request = tonic::Request::new(ListObjectsRequest {
        owner_address: Some("not_an_address".to_string()),
        cursor: None,
        limit: Some(5),
    });
    let response = client.list_objects(request).await;
    assert!(response.is_err());
    let status = response.err().unwrap();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(status.message().contains("Could not parse owner_address"));
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_list_objects_invalid_cursor() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) =
        spawn_test_server_with_object_client().await;
    let request = tonic::Request::new(ListObjectsRequest {
        owner_address: Some(TEST_OWNER_ADDRESS_HEX.to_string()),
        cursor: Some("not_a_cursor_object_id".to_string()),
        limit: Some(5),
    });
    let response = client.list_objects(request).await;
    assert!(response.is_err());
    let status = response.err().unwrap();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(
        status
            .message()
            .contains("Could not parse cursor object_id")
    );
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_list_objects_limit_zero() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) =
        spawn_test_server_with_object_client().await;
    let request = tonic::Request::new(ListObjectsRequest {
        owner_address: Some(TEST_OWNER_ADDRESS_HEX.to_string()),
        cursor: None,
        limit: Some(0), // Zero limit
    });
    let response = client.list_objects(request).await;
    assert!(response.is_err());
    let status = response.err().unwrap();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(status.message().contains("limit cannot be zero"));
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_stream_objects_for_owner() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) =
        spawn_test_server_with_object_client().await;

    let request = tonic::Request::new(StreamObjectsRequest {
        owner_address: TEST_OWNER_ADDRESS_HEX.to_string(),
        start_after_object_id: None,
    });

    let mut stream = client.stream_objects(request).await.unwrap().into_inner();
    let mut count = 0;
    let mut seen_ids = HashSet::new();

    loop {
        match timeout(Duration::from_secs(2), stream.next()).await {
            Ok(Some(Ok(obj_gprc))) => {
                count += 1;
                assert!(obj_gprc.object_id.starts_with("0x70"));
                assert_eq!(obj_gprc.owner_type, "AddressOwner");
                assert!(seen_ids.insert(obj_gprc.object_id.clone()));
            }
            Ok(Some(Err(status))) => {
                panic!("Stream item contained an error status: {:?}", status);
            }
            Ok(None) => {
                break; // Stream ended
            }
            Err(_) => {
                panic!(
                    "Timeout waiting for stream item after {} items. Expected {} items.",
                    count, NUM_MOCK_OWNED_OBJECTS
                );
            }
        }
    }
    assert_eq!(
        count, NUM_MOCK_OWNED_OBJECTS,
        "Incorrect number of objects streamed. Expected {}, got {}",
        NUM_MOCK_OWNED_OBJECTS, count
    );
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_stream_objects_with_cursor() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) =
        spawn_test_server_with_object_client().await;

    let mut cursor_bytes = [0u8; 32];
    cursor_bytes[0] = 0x70;
    cursor_bytes[31] = 2u8; // Object with index 2
    let cursor_object_id = ObjectID::new(cursor_bytes).to_hex_literal();

    let request = tonic::Request::new(StreamObjectsRequest {
        owner_address: TEST_OWNER_ADDRESS_HEX.to_string(),
        start_after_object_id: Some(cursor_object_id.clone()),
    });

    let mut stream = client.stream_objects(request).await.unwrap().into_inner();
    let mut count = 0;
    let mut first_received_id_suffix: Option<u8> = None;
    let expected_count_after_cursor = NUM_MOCK_OWNED_OBJECTS - 3;

    loop {
        match timeout(Duration::from_secs(2), stream.next()).await {
            Ok(Some(Ok(obj_gprc))) => {
                count += 1;
                if first_received_id_suffix.is_none() {
                    let id_suffix_hex = &obj_gprc.object_id[obj_gprc.object_id.len() - 2..];
                    first_received_id_suffix = Some(u8::from_str_radix(id_suffix_hex, 16).unwrap());
                }
            }
            Ok(Some(Err(status))) => {
                panic!("Stream item contained an error status: {:?}", status);
            }
            Ok(None) => {
                break; // Stream ended
            }
            Err(_) => {
                panic!(
                    "Timeout waiting for stream item after {} items. Expected {} items.",
                    count, expected_count_after_cursor
                );
            }
        }
    }
    assert_eq!(
        count, expected_count_after_cursor,
        "Incorrect number of objects streamed after cursor. Expected {}, got {}",
        expected_count_after_cursor, count
    );
    assert_eq!(
        first_received_id_suffix,
        Some(3u8),
        "Stream did not start from the correct object after cursor"
    );

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_stream_objects_owner_not_found() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) =
        spawn_test_server_with_object_client().await;
    let unknown_owner = "0xdeadbeef000000000000000000000000000000000000000000000000deadbeef";
    let request = tonic::Request::new(StreamObjectsRequest {
        owner_address: unknown_owner.to_string(),
        start_after_object_id: None,
    });
    let mut stream = client.stream_objects(request).await.unwrap().into_inner();

    match timeout(Duration::from_secs(1), stream.next()).await {
        Ok(Some(Ok(obj))) => panic!("Should not receive any object for unknown owner: {:?}", obj),
        Ok(Some(Err(status))) => {
            // The service implementation sends an error if account_owned_objects_info_iter
            // fails. If it succeeds but yields an empty iterator, the spawned
            // task finishes, tx is dropped, and stream.next() will yield None.
            // The mock for an unknown owner returns Ok(Box::new(std::iter::empty())).
            // So, object_infos will be an empty Vec. The spawned task iterates 0 times.
            // tx is dropped, rx yields None.
            panic!(
                "Stream sent an error status for unknown owner, expected empty stream (Ok(None)): {:?}",
                status
            );
        }
        Ok(None) => { /* Correct: stream ended immediately and is empty */ }
        Err(_elapsed) => {
            panic!("Timeout waiting for stream, expected it to end quickly for unknown owner")
        }
    }
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_stream_objects_invalid_owner_address() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) =
        spawn_test_server_with_object_client().await;
    let request = tonic::Request::new(StreamObjectsRequest {
        owner_address: "not_an_address".to_string(),
        start_after_object_id: None,
    });
    let response = client.stream_objects(request).await;
    assert!(response.is_err());
    let status = response.err().unwrap();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(status.message().contains("Could not parse owner_address"));
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_stream_objects_invalid_cursor_format() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) =
        spawn_test_server_with_object_client().await;
    let request = tonic::Request::new(StreamObjectsRequest {
        owner_address: TEST_OWNER_ADDRESS_HEX.to_string(),
        start_after_object_id: Some("not_a_valid_object_id".to_string()),
    });
    let response = client.stream_objects(request).await;
    assert!(response.is_err());
    let status = response.err().unwrap();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(
        status
            .message()
            .contains("Could not parse start_after_object_id")
    );
    let _ = shutdown_tx.send(());
}
