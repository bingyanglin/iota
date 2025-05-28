use std::{
    net::{SocketAddr, TcpListener},
    sync::{Arc, Mutex},
    time::Duration,
};

use iota_gprc_api::{
    proto::iota::gprc::v1::{
        EpochIdGprc, GetCommitteeRequest, StreamCommitteeRequest,
        committee_gprc_service_client::CommitteeGprcServiceClient,
    },
    server::{GrpcServer, StateReader},
};
use iota_types::crypto::KeypairTraits; // For kp.public()
use iota_types::crypto::{AuthorityKeyPair, AuthorityPublicKeyBytes};
use rand::{SeedableRng, rngs::StdRng};
use tokio::{sync::broadcast, time::sleep};

struct MockRestStateReader {
    current_epoch: Arc<Mutex<u64>>,
}

impl Default for MockRestStateReader {
    fn default() -> Self {
        Self {
            current_epoch: Arc::new(Mutex::new(0)),
        }
    }
}

impl iota_types::storage::ReadStore for MockRestStateReader {
    fn get_committee(
        &self,
        epoch_id_req: iota_types::committee::EpochId,
    ) -> iota_types::storage::error::Result<Option<Arc<iota_types::committee::Committee>>> {
        use std::collections::BTreeMap;

        use iota_types::{
            base_types::AuthorityName,
            committee::{Committee as CoreCommittee, StakeUnit},
            crypto::{AuthorityKeyPair, AuthorityPublicKeyBytes, KeypairTraits},
        };

        if epoch_id_req == 1 {
            let mut voting_weights = BTreeMap::<AuthorityName, StakeUnit>::new();
            let kp1 = AuthorityKeyPair::generate(&mut StdRng::from_seed([1u8; 32]));
            let name1 = AuthorityName::from(AuthorityPublicKeyBytes::from(kp1.public()));
            voting_weights.insert(name1, 100);
            let kp2 = AuthorityKeyPair::generate(&mut StdRng::from_seed([2u8; 32]));
            let name2 = AuthorityName::from(AuthorityPublicKeyBytes::from(kp2.public()));
            voting_weights.insert(name2, 200);
            let committee = CoreCommittee::new_for_testing_with_normalized_voting_power(
                epoch_id_req,
                voting_weights,
            );
            return Ok(Some(Arc::new(committee)));
        } else if epoch_id_req == 2 {
            // Added back mock for epoch 2
            let mut voting_weights = BTreeMap::<AuthorityName, StakeUnit>::new();
            let kp3 = AuthorityKeyPair::generate(&mut StdRng::from_seed([3u8; 32]));
            let name3 = AuthorityName::from(AuthorityPublicKeyBytes::from(kp3.public()));
            voting_weights.insert(name3, 300);
            let kp4 = AuthorityKeyPair::generate(&mut StdRng::from_seed([4u8; 32]));
            let name4 = AuthorityName::from(AuthorityPublicKeyBytes::from(kp4.public()));
            voting_weights.insert(name4, 400);
            let committee = CoreCommittee::new_for_testing_with_normalized_voting_power(
                epoch_id_req,
                voting_weights,
            );
            return Ok(Some(Arc::new(committee)));
        }

        Ok(None) // Default to None if not epoch 1 or 2
    }
    fn get_latest_checkpoint(
        &self,
    ) -> iota_types::storage::error::Result<iota_types::messages_checkpoint::VerifiedCheckpoint>
    {
        unimplemented!()
    }
    fn get_latest_checkpoint_sequence_number(
        &self,
    ) -> iota_types::storage::error::Result<iota_types::messages_checkpoint::CheckpointSequenceNumber>
    {
        Ok(0)
    }
    fn get_latest_epoch_id(
        &self,
    ) -> iota_types::storage::error::Result<iota_types::committee::EpochId> {
        let current_epoch = self.current_epoch.lock().unwrap();
        Ok(*current_epoch)
    }
    fn get_highest_verified_checkpoint(
        &self,
    ) -> iota_types::storage::error::Result<iota_types::messages_checkpoint::VerifiedCheckpoint>
    {
        unimplemented!()
    }
    fn get_highest_synced_checkpoint(
        &self,
    ) -> iota_types::storage::error::Result<iota_types::messages_checkpoint::VerifiedCheckpoint>
    {
        unimplemented!()
    }
    fn get_lowest_available_checkpoint(
        &self,
    ) -> iota_types::storage::error::Result<iota_types::messages_checkpoint::CheckpointSequenceNumber>
    {
        unimplemented!()
    }
    fn get_checkpoint_by_digest(
        &self,
        _digest: &iota_types::digests::CheckpointDigest,
    ) -> iota_types::storage::error::Result<
        Option<iota_types::messages_checkpoint::VerifiedCheckpoint>,
    > {
        unimplemented!()
    }
    fn get_checkpoint_by_sequence_number(
        &self,
        _sequence_number: iota_types::messages_checkpoint::CheckpointSequenceNumber,
    ) -> iota_types::storage::error::Result<
        Option<iota_types::messages_checkpoint::VerifiedCheckpoint>,
    > {
        unimplemented!()
    }
    fn get_checkpoint_contents_by_digest(
        &self,
        _digest: &iota_types::digests::CheckpointContentsDigest,
    ) -> iota_types::storage::error::Result<
        Option<iota_types::messages_checkpoint::CheckpointContents>,
    > {
        unimplemented!()
    }
    fn get_checkpoint_contents_by_sequence_number(
        &self,
        _sequence_number: iota_types::messages_checkpoint::CheckpointSequenceNumber,
    ) -> iota_types::storage::error::Result<
        Option<iota_types::messages_checkpoint::CheckpointContents>,
    > {
        unimplemented!()
    }
    fn get_transaction(
        &self,
        _tx_digest: &iota_types::digests::TransactionDigest,
    ) -> iota_types::storage::error::Result<
        Option<std::sync::Arc<iota_types::transaction::VerifiedTransaction>>,
    > {
        unimplemented!()
    }
    fn get_transaction_effects(
        &self,
        _tx_digest: &iota_types::digests::TransactionDigest,
    ) -> iota_types::storage::error::Result<Option<iota_types::effects::TransactionEffects>> {
        unimplemented!()
    }
    fn get_events(
        &self,
        _event_digest: &iota_types::digests::TransactionEventsDigest,
    ) -> iota_types::storage::error::Result<Option<iota_types::effects::TransactionEvents>> {
        unimplemented!()
    }
    fn get_full_checkpoint_contents_by_sequence_number(
        &self,
        _sequence_number: iota_types::messages_checkpoint::CheckpointSequenceNumber,
    ) -> iota_types::storage::error::Result<
        Option<iota_types::messages_checkpoint::FullCheckpointContents>,
    > {
        unimplemented!()
    }
    fn get_full_checkpoint_contents(
        &self,
        _digest: &iota_types::digests::CheckpointContentsDigest,
    ) -> iota_types::storage::error::Result<
        Option<iota_types::messages_checkpoint::FullCheckpointContents>,
    > {
        unimplemented!()
    }
    fn get_checkpoint_data(
        &self,
        _checkpoint: iota_types::messages_checkpoint::VerifiedCheckpoint,
        _checkpoint_contents: iota_types::messages_checkpoint::CheckpointContents,
    ) -> anyhow::Result<iota_types::full_checkpoint_content::CheckpointData> {
        unimplemented!()
    }
}
impl iota_types::storage::ObjectStore for MockRestStateReader {
    fn get_object(
        &self,
        _object_id: &iota_types::base_types::ObjectID,
    ) -> iota_types::storage::error::Result<Option<iota_types::object::Object>> {
        unimplemented!()
    }
    fn get_object_by_key(
        &self,
        _object_id: &iota_types::base_types::ObjectID,
        _version: iota_types::base_types::SequenceNumber,
    ) -> iota_types::storage::error::Result<Option<iota_types::object::Object>> {
        unimplemented!()
    }
    fn multi_get_objects_by_key(
        &self,
        _object_keys: &[iota_types::storage::ObjectKey],
    ) -> iota_types::storage::error::Result<Vec<Option<iota_types::object::Object>>> {
        unimplemented!()
    }
}
impl iota_types::storage::RestStateReader for MockRestStateReader {
    fn get_transaction_checkpoint(
        &self,
        _digest: &iota_types::digests::TransactionDigest,
    ) -> iota_types::storage::error::Result<
        Option<iota_types::messages_checkpoint::CheckpointSequenceNumber>,
    > {
        unimplemented!()
    }
    fn get_lowest_available_checkpoint_objects(
        &self,
    ) -> iota_types::storage::error::Result<iota_types::messages_checkpoint::CheckpointSequenceNumber>
    {
        unimplemented!()
    }
    fn get_chain_identifier(
        &self,
    ) -> iota_types::storage::error::Result<iota_types::digests::ChainIdentifier> {
        unimplemented!()
    }
    fn account_owned_objects_info_iter(
        &self,
        _owner: iota_types::base_types::IotaAddress,
        _cursor: Option<iota_types::base_types::ObjectID>,
    ) -> iota_types::storage::error::Result<
        Box<dyn Iterator<Item = iota_types::storage::AccountOwnedObjectInfo> + '_>,
    > {
        unimplemented!()
    }
    fn dynamic_field_iter(
        &self,
        _parent: iota_types::base_types::ObjectID,
        _cursor: Option<iota_types::base_types::ObjectID>,
    ) -> iota_types::storage::error::Result<
        Box<
            dyn Iterator<
                    Item = (
                        iota_types::storage::DynamicFieldKey,
                        iota_types::storage::DynamicFieldIndexInfo,
                    ),
                > + '_,
        >,
    > {
        unimplemented!()
    }
    fn get_coin_info(
        &self,
        _coin_type: &move_core_types::language_storage::StructTag,
    ) -> iota_types::storage::error::Result<Option<iota_types::storage::CoinInfo>> {
        unimplemented!()
    }
    fn get_epoch_last_checkpoint(
        &self,
        _epoch_id: iota_types::committee::EpochId,
    ) -> iota_types::storage::error::Result<
        Option<iota_types::messages_checkpoint::VerifiedCheckpoint>,
    > {
        unimplemented!()
    }
    fn list_transactions(
        &self,
        _cursor: Option<iota_types::digests::TransactionDigest>,
        _limit: u64,
        _direction: iota_types::storage::ListDirection,
    ) -> iota_types::storage::error::Result<
        Vec<(
            iota_types::digests::TransactionDigest,
            Arc<iota_types::transaction::VerifiedTransaction>,
        )>,
    > {
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

async fn spawn_test_server_with_committee_client() -> (
    CommitteeGprcServiceClient<tonic::transport::Channel>,
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
            eprintln!("[Test CommitteeService] gRPC server failed: {}", e);
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
                "[Test CommitteeService] Failed to connect to gRPC server at {}: {:?}",
                client_endpoint, e
            )
        });

    let client = CommitteeGprcServiceClient::new(client_channel);
    (client, addr, shutdown_tx, mock_state_reader_arc)
}

#[tokio::test]
async fn test_get_committee_found_and_not_found() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader_arc) =
        spawn_test_server_with_committee_client().await;

    let kp1_test = AuthorityKeyPair::generate(&mut StdRng::from_seed([1u8; 32]));
    let pk1_bytes_test = AuthorityPublicKeyBytes::from(kp1_test.public());
    let pk1_hex = hex::encode(pk1_bytes_test.as_ref());

    let kp2_test = AuthorityKeyPair::generate(&mut StdRng::from_seed([2u8; 32]));
    let pk2_bytes_test = AuthorityPublicKeyBytes::from(kp2_test.public());
    let pk2_hex = hex::encode(pk2_bytes_test.as_ref());

    let request_found = GetCommitteeRequest {
        epoch_id: Some(EpochIdGprc { epoch: 1 }),
    };
    let response_found_result = client.get_committee(request_found).await;
    assert!(
        response_found_result.is_ok(),
        "Expected OK for epoch 1, got {:?}",
        response_found_result.err()
    );
    let committee_gprc = response_found_result.unwrap().into_inner();

    assert_eq!(committee_gprc.epoch_id, Some(EpochIdGprc { epoch: 1 }));
    assert_eq!(
        committee_gprc.members.len(),
        2,
        "Expected 2 members for epoch 1"
    );

    let member1 = committee_gprc
        .members
        .iter()
        .find(|m| m.authority_name == pk1_hex)
        .expect("Member 1 not found with authority_name");
    assert_eq!(member1.authority_name, pk1_hex);
    assert_eq!(
        member1
            .stake_units
            .as_ref()
            .expect("Stake units missing for member 1")
            .value
            .parse::<u64>()
            .unwrap(),
        3333
    );

    let member2 = committee_gprc
        .members
        .iter()
        .find(|m| m.authority_name == pk2_hex)
        .expect("Member 2 not found with authority_name");
    assert_eq!(member2.authority_name, pk2_hex);
    assert_eq!(
        member2
            .stake_units
            .as_ref()
            .expect("Stake units missing for member 2")
            .value
            .parse::<u64>()
            .unwrap(),
        6667
    );

    let request_not_found = GetCommitteeRequest {
        epoch_id: Some(EpochIdGprc { epoch: 3 }),
    };
    let result_not_found = client.get_committee(request_not_found).await;
    assert!(result_not_found.is_err());
    if let Err(status) = result_not_found {
        assert_eq!(status.code(), tonic::Code::NotFound);
        assert!(status.message().contains("Committee not found for epoch 3"));
    }

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_stream_committee_receives_items() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader_arc) =
        spawn_test_server_with_committee_client().await;

    // mock_state_reader_arc.set_epoch(1); // Ensure we start from a known epoch if
    // needed Initially, mock state reader current_epoch is 0 (from default())
    // Stream should start from current_latest_epoch (0) + 1 = 1.

    let request = StreamCommitteeRequest {
        start_epoch: None, // Let service determine start from latest + 1
    };

    let mut stream = client.stream_committee(request).await.unwrap().into_inner();

    // 1. Expect Committee for Epoch 1
    println!("[StreamCommitteeTest] Expecting committee for epoch 1...");
    match tokio::time::timeout(Duration::from_secs(5), stream.message()).await {
        Ok(Ok(Some(committee_gprc))) => {
            assert_eq!(committee_gprc.epoch_id.unwrap().epoch, 1);
            assert_eq!(committee_gprc.members.len(), 2); // From mock for epoch 1
            println!("[StreamCommitteeTest] Received committee for epoch 1.");
        }
        Ok(Ok(None)) => panic!("[StreamCommitteeTest] Stream ended prematurely expecting epoch 1."),
        Ok(Err(e)) => panic!("[StreamCommitteeTest] Error receiving epoch 1: {:?}", e),
        Err(_) => panic!("[StreamCommitteeTest] Timeout waiting for epoch 1."),
    }

    // Advance mock state so that epoch 2 committee becomes available.
    // The stream is polling for current_epoch_to_check = 1 + 1 = 2.
    // mock_state_reader_arc.set_epoch(1); // This sets
    // ReadStore.get_latest_epoch_id() to 1. The service logic uses this for
    // initial_epoch if start_epoch is None. For polling, it increments its
    // internal current_epoch_to_check. So, advancing the shared current_epoch
    // isn't strictly necessary for the polling loop to find the next one, as
    // get_committee(2) will provide it.

    // 2. Expect Committee for Epoch 2
    println!("[StreamCommitteeTest] Expecting committee for epoch 2...");
    match tokio::time::timeout(Duration::from_secs(5), stream.message()).await {
        Ok(Ok(Some(committee_gprc))) => {
            assert_eq!(committee_gprc.epoch_id.unwrap().epoch, 2);
            assert_eq!(committee_gprc.members.len(), 2); // From mock for epoch 2
            println!("[StreamCommitteeTest] Received committee for epoch 2.");
        }
        Ok(Ok(None)) => panic!("[StreamCommitteeTest] Stream ended prematurely expecting epoch 2."),
        Ok(Err(e)) => panic!("[StreamCommitteeTest] Error receiving epoch 2: {:?}", e),
        Err(_) => panic!("[StreamCommitteeTest] Timeout waiting for epoch 2."),
    }

    // 3. Expect no more committees (epoch 3 is not defined in mock)
    // The stream should keep polling but not find anything for epoch 3.
    println!("[StreamCommitteeTest] Expecting no committee for epoch 3 (should timeout)...");
    match tokio::time::timeout(Duration::from_secs(3), stream.message()).await {
        Ok(Ok(Some(committee_gprc))) => {
            panic!(
                "[StreamCommitteeTest] Unexpected committee received for epoch: {:?}",
                committee_gprc.epoch_id
            );
        }
        Ok(Ok(None)) => {
            // Stream ended, which is ok if the server decided to close after no new data.
            println!("[StreamCommitteeTest] Stream ended as expected after epoch 2.");
        }
        Ok(Err(e)) => {
            panic!(
                "[StreamCommitteeTest] Error when expecting no new committee: {:?}",
                e
            );
        }
        Err(_) => {
            // Timeout, this is the expected behavior if stream is still open and polling
            println!("[StreamCommitteeTest] Timed out waiting for epoch 3, as expected.");
        }
    }

    drop(shutdown_tx);
}
