use std::{
    net::{SocketAddr, TcpListener},
    sync::Arc,
    time::Duration,
};

use iota_gprc_api::{
    proto::iota::gprc::v1::{
        GetTransactionRequest, transaction_gprc_service_client::TransactionGprcServiceClient,
    },
    server::{GrpcServer, StateReader},
};
use iota_types::{
    base_types::{IotaAddress, /* MoveObjectType, */ ObjectID, ObjectRef, SequenceNumber},
    committee::{Committee, EpochId},
    crypto::{Ed25519IotaSignature, EmptySignInfo, IotaSignatureInner, Signature, ToFromBytes},
    digests::{
        ChainIdentifier, CheckpointContentsDigest, CheckpointDigest, ObjectDigest,
        TransactionDigest as ActualTransactionDigest, TransactionEventsDigest,
    },
    effects::{TransactionEffects, TransactionEvents},
    message_envelope::{Envelope, VerifiedEnvelope},
    messages_checkpoint::{
        CheckpointContents, CheckpointSequenceNumber,
        FullCheckpointContents as ActualFullCheckpointContents, VerifiedCheckpoint,
    },
    object::Object as IotaObject,
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    storage::{
        AccountOwnedObjectInfo, CoinInfo, DynamicFieldIndexInfo, DynamicFieldKey, ObjectKey,
    },
    transaction::{
        GasData, SenderSignedData, TransactionData, TransactionDataV1, TransactionExpiration,
        TransactionKind, VerifiedTransaction,
    },
};
use move_core_types::language_storage::StructTag;
use tokio::{
    sync::broadcast,
    time::{sleep, timeout},
};

#[derive(Default)]
struct MockRestStateReader {
    // We'll add specific transaction mocks later
}

impl iota_types::storage::ReadStore for MockRestStateReader {
    fn get_transaction(
        &self,
        tx_digest: &ActualTransactionDigest,
    ) -> iota_types::storage::error::Result<Option<Arc<VerifiedTransaction>>> {
        // Use a fixed byte array for the test transaction ID
        let mut tx_id_arr = [0u8; 32];
        tx_id_arr[31] = 0x01; // e.g., ...0001

        let known_tx_digest = ActualTransactionDigest::new(tx_id_arr);

        if *tx_digest == known_tx_digest {
            // Use fixed byte array for addresses
            let dummy_sender_address_bytes = [0u8; 32]; // All zeros
            let dummy_sender_address = IotaAddress::new(dummy_sender_address_bytes);

            let mut recipient_address_bytes = [0u8; 32];
            recipient_address_bytes[31] = 0x01; // e.g. ...0001 for recipient
            let recipient_address = IotaAddress::new(recipient_address_bytes);

            // Use a different array for object IDs to avoid confusion, or the same if
            // intended
            let mut obj_id_arr = [0u8; 32];
            obj_id_arr[31] = 0xaa; // e.g., ...00aa for the object
            let dummy_object_id = ObjectID::new(obj_id_arr);
            let dummy_object_digest = ObjectDigest::new(obj_id_arr); // Assuming same bytes for digest for simplicity

            let dummy_sequence_number = SequenceNumber::new();
            let dummy_object_ref: ObjectRef =
                (dummy_object_id, dummy_sequence_number, dummy_object_digest);

            let pt = {
                let mut builder = ProgrammableTransactionBuilder::new();
                builder
                    .transfer_object(recipient_address, dummy_object_ref)
                    .unwrap();
                builder.finish()
            };
            let tx_kind = TransactionKind::ProgrammableTransaction(pt);

            // Use fixed byte arrays for gas payment details
            let mut gas_obj_bytes = [0u8; 32];
            gas_obj_bytes[31] = 0xbb;

            let gas_payment_object_id = ObjectID::new(gas_obj_bytes);
            let gas_payment_digest = ObjectDigest::new(gas_obj_bytes); // Using same bytes for simplicity

            let gas_payment_seq_num = SequenceNumber::new();

            let gas_data = GasData {
                payment: vec![(
                    gas_payment_object_id,
                    gas_payment_seq_num,
                    gas_payment_digest,
                )],
                owner: dummy_sender_address,
                price: 100,
                budget: 1_000_000,
            };

            let tx_data = TransactionData::V1(TransactionDataV1 {
                kind: tx_kind,
                sender: dummy_sender_address,
                gas_data,
                expiration: TransactionExpiration::None,
            });

            let dummy_sig_bytes = [1u8; Ed25519IotaSignature::LENGTH];
            let signature = Signature::Ed25519IotaSignature(
                Ed25519IotaSignature::from_bytes(&dummy_sig_bytes).unwrap(),
            );
            let sender_signed_data =
                SenderSignedData::new_from_sender_signature(tx_data, signature);
            let envelope = Envelope::new_from_data_and_sig(sender_signed_data, EmptySignInfo {});
            let mock_transaction = VerifiedEnvelope::new_from_verified(envelope);

            Ok(Some(Arc::new(mock_transaction)))
        } else {
            Ok(None)
        }
    }
    fn get_committee(
        &self,
        _epoch: EpochId,
    ) -> iota_types::storage::error::Result<Option<Arc<Committee>>> {
        unimplemented!()
    }
    fn get_latest_checkpoint(&self) -> iota_types::storage::error::Result<VerifiedCheckpoint> {
        unimplemented!()
    }
    fn get_latest_checkpoint_sequence_number(
        &self,
    ) -> iota_types::storage::error::Result<CheckpointSequenceNumber> {
        unimplemented!()
    }
    fn get_latest_epoch_id(&self) -> iota_types::storage::error::Result<EpochId> {
        unimplemented!()
    }
    fn get_highest_verified_checkpoint(
        &self,
    ) -> iota_types::storage::error::Result<VerifiedCheckpoint> {
        unimplemented!()
    }
    fn get_highest_synced_checkpoint(
        &self,
    ) -> iota_types::storage::error::Result<VerifiedCheckpoint> {
        unimplemented!()
    }
    fn get_lowest_available_checkpoint(
        &self,
    ) -> iota_types::storage::error::Result<CheckpointSequenceNumber> {
        unimplemented!()
    }
    fn get_checkpoint_by_digest(
        &self,
        _digest: &CheckpointDigest,
    ) -> iota_types::storage::error::Result<Option<VerifiedCheckpoint>> {
        unimplemented!()
    }
    fn get_checkpoint_by_sequence_number(
        &self,
        _sequence_number: CheckpointSequenceNumber,
    ) -> iota_types::storage::error::Result<Option<VerifiedCheckpoint>> {
        unimplemented!()
    }
    fn get_checkpoint_contents_by_digest(
        &self,
        _digest: &CheckpointContentsDigest,
    ) -> iota_types::storage::error::Result<Option<CheckpointContents>> {
        unimplemented!()
    }
    fn get_checkpoint_contents_by_sequence_number(
        &self,
        _sequence_number: CheckpointSequenceNumber,
    ) -> iota_types::storage::error::Result<Option<CheckpointContents>> {
        unimplemented!()
    }
    fn get_transaction_effects(
        &self,
        _tx_digest: &ActualTransactionDigest,
    ) -> iota_types::storage::error::Result<Option<TransactionEffects>> {
        unimplemented!()
    }
    fn get_events(
        &self,
        _event_digest: &TransactionEventsDigest,
    ) -> iota_types::storage::error::Result<Option<TransactionEvents>> {
        unimplemented!()
    }
    fn get_full_checkpoint_contents_by_sequence_number(
        &self,
        _sequence_number: CheckpointSequenceNumber,
    ) -> iota_types::storage::error::Result<Option<ActualFullCheckpointContents>> {
        unimplemented!()
    }
    fn get_full_checkpoint_contents(
        &self,
        _digest: &CheckpointContentsDigest,
    ) -> iota_types::storage::error::Result<Option<ActualFullCheckpointContents>> {
        unimplemented!()
    }
    fn get_checkpoint_data(
        &self,
        _checkpoint: VerifiedCheckpoint,
        _checkpoint_contents: CheckpointContents,
    ) -> anyhow::Result<iota_types::full_checkpoint_content::CheckpointData> {
        unimplemented!()
    }
}

impl iota_types::storage::ObjectStore for MockRestStateReader {
    fn get_object(
        &self,
        _object_id: &ObjectID,
    ) -> iota_types::storage::error::Result<Option<IotaObject>> {
        unimplemented!()
    }
    fn get_object_by_key(
        &self,
        _object_id: &ObjectID,
        _version: SequenceNumber,
    ) -> iota_types::storage::error::Result<Option<IotaObject>> {
        unimplemented!()
    }
    fn multi_get_objects_by_key(
        &self,
        _object_keys: &[ObjectKey],
    ) -> iota_types::storage::error::Result<Vec<Option<IotaObject>>> {
        unimplemented!()
    }
}

impl iota_types::storage::RestStateReader for MockRestStateReader {
    fn get_transaction_checkpoint(
        &self,
        _digest: &ActualTransactionDigest,
    ) -> iota_types::storage::error::Result<Option<CheckpointSequenceNumber>> {
        unimplemented!()
    }
    fn get_lowest_available_checkpoint_objects(
        &self,
    ) -> iota_types::storage::error::Result<CheckpointSequenceNumber> {
        unimplemented!()
    }
    fn get_chain_identifier(&self) -> iota_types::storage::error::Result<ChainIdentifier> {
        unimplemented!()
    }
    fn account_owned_objects_info_iter(
        &self,
        _owner: IotaAddress,
        _cursor: Option<ObjectID>,
    ) -> iota_types::storage::error::Result<Box<dyn Iterator<Item = AccountOwnedObjectInfo> + '_>>
    {
        unimplemented!()
    }
    fn dynamic_field_iter(
        &self,
        _parent: ObjectID,
        _cursor: Option<ObjectID>,
    ) -> iota_types::storage::error::Result<
        Box<dyn Iterator<Item = (DynamicFieldKey, DynamicFieldIndexInfo)> + '_>,
    > {
        unimplemented!()
    }
    fn get_coin_info(
        &self,
        _coin_type: &StructTag,
    ) -> iota_types::storage::error::Result<Option<CoinInfo>> {
        unimplemented!()
    }
    fn get_epoch_last_checkpoint(
        &self,
        _epoch_id: EpochId,
    ) -> iota_types::storage::error::Result<Option<VerifiedCheckpoint>> {
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

async fn spawn_test_server_with_transaction_client() -> (
    TransactionGprcServiceClient<tonic::transport::Channel>,
    SocketAddr,
    broadcast::Sender<()>, // For shutdown
    Arc<MockRestStateReader>,
) {
    let port = get_available_port();
    let addr: SocketAddr = format!("[::1]:{}", port).parse().unwrap();

    let mock_state_reader_arc = Arc::new(MockRestStateReader::default());
    let server_state_reader: StateReader = mock_state_reader_arc.clone();

    let (shutdown_tx, _shutdown_rx_unused_by_caller) = broadcast::channel(1);
    let server_shutdown_rx = shutdown_tx.subscribe();

    let grpc_server = GrpcServer::new(addr, server_state_reader);

    tokio::spawn(async move {
        if let Err(e) = grpc_server.start(server_shutdown_rx).await {
            eprintln!("[Test TransactionService] gRPC server failed: {}", e);
        }
    });

    sleep(Duration::from_millis(200)).await; // Allow server to start

    let client_endpoint = format!("http://[::1]:{}", port);
    let client_channel = tonic::transport::Channel::from_shared(client_endpoint)
        .unwrap()
        .connect_timeout(Duration::from_secs(2))
        .connect()
        .await
        .expect("[Test TransactionService] Failed to connect to gRPC server");

    let client = TransactionGprcServiceClient::new(client_channel);
    (client, addr, shutdown_tx, mock_state_reader_arc)
}

#[tokio::test]
async fn test_get_transaction_found() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader_arc) =
        spawn_test_server_with_transaction_client().await;

    // Update expected tx_id_hex_str to match the new tx_id_arr
    let tx_id_hex_str = "0x0000000000000000000000000000000000000000000000000000000000000001";
    let mut tx_digest_bytes_arr = [0u8; 32];
    tx_digest_bytes_arr[31] = 0x01;

    let request = tonic::Request::new(GetTransactionRequest {
        transaction_digest_bytes: tx_digest_bytes_arr.to_vec(), // Send as Vec<u8>
    });

    let response = timeout(Duration::from_secs(2), client.get_transaction(request))
        .await
        .expect("RPC call timed out")
        .unwrap()
        .into_inner();

    assert_eq!(response.transaction_id_hex, tx_id_hex_str);
    assert_eq!(response.payload_type, "VerifiedTransaction");
    assert!(!response.raw_transaction.is_empty());

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_get_transaction_invalid_bytes_length() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader_arc) =
        spawn_test_server_with_transaction_client().await;
    let tx_digest_bytes = vec![0u8; 31]; // Invalid length
    let request = tonic::Request::new(GetTransactionRequest {
        transaction_digest_bytes: tx_digest_bytes.clone(),
    });
    let result = client.get_transaction(request).await;
    assert!(result.is_err());
    if let Err(status) = result {
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(
            status
                .message()
                .contains("Transaction digest bytes must be 32 bytes long")
        );
    }
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_get_transaction_empty_bytes() {
    let (mut client, _addr, shutdown_tx, _mock_state_reader_arc) =
        spawn_test_server_with_transaction_client().await;
    let tx_digest_bytes = Vec::new(); // Empty bytes
    let request = tonic::Request::new(GetTransactionRequest {
        transaction_digest_bytes: tx_digest_bytes,
    });
    let result = client.get_transaction(request).await;
    assert!(result.is_err());
    if let Err(status) = result {
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(
            status
                .message()
                .contains("Transaction digest bytes must be 32 bytes long")
        );
    }
    let _ = shutdown_tx.send(());
}
