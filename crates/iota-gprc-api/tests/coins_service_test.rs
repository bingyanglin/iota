use std::{
    net::{SocketAddr, TcpListener},
    sync::Arc,
    time::Duration,
};

use iota_gprc_api::{
    proto::iota::gprc::v1::{
        GetCoinInfoRequest, ListCoinsRequest, SubscribeCoinEventsRequest,
        coins_gprc_service_client::CoinsGprcServiceClient,
    },
    server::{GrpcServer, StateReader},
};
use iota_types::storage::RestStateReader;
use tokio::{sync::broadcast, time::sleep};

#[derive(Default)]
struct MockRestStateReader {}

impl iota_types::storage::ReadStore for MockRestStateReader {
    fn get_committee(
        &self,
        _epoch: iota_types::committee::EpochId,
    ) -> iota_types::storage::error::Result<Option<std::sync::Arc<iota_types::committee::Committee>>>
    {
        unimplemented!()
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
        Ok(0)
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
impl RestStateReader for MockRestStateReader {
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
    (client, addr, shutdown_tx, mock_state_reader_arc)
}

#[tokio::test]
async fn test_get_coin_info_unimplemented() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) =
        spawn_test_server_with_coins_client().await;

    let request = GetCoinInfoRequest {
        coin_type_tag: "0x1::sui::SUI".to_string(), // Example tag
    };

    let result = client.get_coin_info(request).await;
    assert!(result.is_err());
    if let Err(status) = result {
        assert_eq!(status.code(), tonic::Code::Unimplemented);
        assert!(status.message().contains("GetCoinInfo not implemented"));
    }
    drop(shutdown_tx);
}

#[tokio::test]
async fn test_list_coins_unimplemented() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) =
        spawn_test_server_with_coins_client().await;

    let request = ListCoinsRequest {
        page_size: None,
        cursor: None,
    };

    let result = client.list_coins(request).await;
    assert!(result.is_err());
    if let Err(status) = result {
        assert_eq!(status.code(), tonic::Code::Unimplemented);
        assert!(status.message().contains("ListCoins not implemented"));
    }
    drop(shutdown_tx);
}

#[tokio::test]
async fn test_subscribe_coin_events_unimplemented() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader) =
        spawn_test_server_with_coins_client().await;

    let request = SubscribeCoinEventsRequest {
        coin_type_tag_filter: None,
    };

    let result = client.subscribe_coin_events(request).await;
    assert!(result.is_err());
    if let Err(status) = result {
        assert_eq!(status.code(), tonic::Code::Unimplemented);
        assert!(
            status
                .message()
                .contains("SubscribeCoinEvents not implemented")
        );
    }
    drop(shutdown_tx);
}
