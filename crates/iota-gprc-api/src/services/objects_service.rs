use std::{sync::Arc, time::Duration};

use anyhow;
use hex;
use iota_types::{
    base_types::{IotaAddress, ObjectID, ObjectIDParseError, SequenceNumber},
    object::Object as IotaObject,
    parse_iota_address,
    storage::AccountOwnedObjectInfo,
};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::{
    conversions::objects::convert_object_to_gprc,
    error::GrpcApiError,
    proto::iota::gprc::v1::{
        GetObjectRequest, ListObjectsRequest, ListObjectsResponse, ObjectGprc,
        StreamObjectsRequest, SubscribeObjectsByOwnerRequest,
        object_gprc_service_server::ObjectGprcService,
    },
    server::StateReader,
};

const DEFAULT_LIST_OBJECTS_LIMIT: u32 = 50;
const MAX_LIST_OBJECTS_LIMIT: u32 = 1000;

#[derive(Clone)]
pub struct ObjectServiceImpl {
    state_reader: StateReader,
    object_event_sender: broadcast::Sender<(IotaAddress, Arc<IotaObject>)>,
}

impl ObjectServiceImpl {
    pub fn new(state_reader: StateReader) -> Self {
        let (tx, _rx) = broadcast::channel::<(IotaAddress, Arc<IotaObject>)>(32);
        let poller_tx = tx.clone();

        // Mock poller task for object events
        tokio::spawn(async move {
            let mut version_counter: u64 = 1;
            // Use a fixed owner address for mock events, e.g., the one from tests
            let mock_owner_address_str =
                "0xaabbccddeeff00112233445566778899aabbccddeeff00112233445566778899";
            let mock_owner_address = match mock_owner_address_str.parse::<IotaAddress>() {
                Ok(addr) => addr,
                Err(_) => {
                    eprintln!(
                        "[ObjectPoller] Fatal: Could not parse mock owner address. Poller will not run."
                    );
                    return;
                }
            };
            // Use a fixed object ID for mock events for simplicity, just change its
            // version/content
            let mock_object_id_str =
                "0xdeadbeef00000000000000000000000000000000000000000000000000000000";
            let _mock_object_id = match mock_object_id_str.parse::<ObjectID>() {
                Ok(id) => id,
                Err(_) => {
                    eprintln!(
                        "[ObjectPoller] Fatal: Could not parse mock object ID. Poller will not run."
                    );
                    return;
                }
            };

            println!(
                "[ObjectPoller] Started. Will generate mock objects for owner: 0x{}",
                hex::encode(mock_owner_address.as_ref())
            );

            loop {
                tokio::time::sleep(Duration::from_secs(2)).await; // Poll/generate every 2 seconds

                // Create a mock object (simplified version of test's create_mock_object)
                let mock_move_object_type_str = "0x42::example_module::ExamplePolledObject";
                let struct_tag = mock_move_object_type_str
                    .parse::<move_core_types::language_storage::StructTag>()
                    .unwrap();
                let move_object_type = iota_types::base_types::MoveObjectType::from(struct_tag);
                let move_obj = iota_types::object::MoveObject::new_from_execution(
                    move_object_type,
                    iota_types::base_types::SequenceNumber::from_u64(version_counter),
                    format!("polled_mock_contents_{}", version_counter).into_bytes(),
                    &iota_protocol_config::ProtocolConfig::get_for_min_version(),
                )
                .unwrap();

                let iota_object = iota_types::object::ObjectInner {
                    data: iota_types::object::Data::Move(move_obj),
                    owner: iota_types::object::Owner::AddressOwner(mock_owner_address),
                    previous_transaction: iota_types::digests::TransactionDigest::genesis_marker(),
                    storage_rebate: 100u64 + version_counter,
                }
                .into();

                let object_arc = Arc::new(iota_object);

                // println!(
                //     "[ObjectPoller] Generated mock object version {} for owner {}",
                //     version_counter, mock_owner_address.to_hex_literal()
                // );

                if poller_tx.send((mock_owner_address, object_arc)).is_err() {
                    // All receivers dropped, which is fine if no one is
                    // subscribed. The poller continues, new
                    // subscribers will get new events.
                    // println!("[ObjectPoller] No active subscribers for object
                    // event. Polling continues.");
                }
                version_counter += 1;
            }
        });

        Self {
            state_reader,
            object_event_sender: tx,
        }
    }
}

#[tonic::async_trait]
impl ObjectGprcService for ObjectServiceImpl {
    async fn get_object(
        &self,
        request: Request<GetObjectRequest>,
    ) -> Result<Response<ObjectGprc>, Status> {
        let original_request_object_id = request.get_ref().object_id.clone();
        let original_request_version_str = request.get_ref().version.clone();

        println!(
            "[gRPC ObjectService] Received GetObject request: {:?}",
            request.get_ref()
        );

        let _req_inner = request.into_inner();
        let object_id_str = original_request_object_id;

        let object_id_parsed: ObjectID =
            object_id_str.parse().map_err(|e: ObjectIDParseError| {
                Status::invalid_argument(format!(
                    "Could not parse object_id '{}': {}",
                    object_id_str, e
                ))
            })?;

        let core_object_result =
            if let Some(version_str_val) = original_request_version_str.as_deref() {
                if version_str_val.is_empty() {
                    self.state_reader.get_object(&object_id_parsed)
                } else {
                    let version_u64 = version_str_val.parse::<u64>().map_err(|e| {
                        Status::invalid_argument(format!(
                            "Could not parse version '{}' as u64: {}",
                            version_str_val, e
                        ))
                    })?;
                    let version_seq_num = SequenceNumber::from_u64(version_u64);
                    self.state_reader
                        .get_object_by_key(&object_id_parsed, version_seq_num)
                }
            } else {
                self.state_reader.get_object(&object_id_parsed)
            };

        match core_object_result {
            Ok(Some(core_object)) => {
                let gprc_object = convert_object_to_gprc(&object_id_parsed, &core_object)
                    .map_err(GrpcApiError::from)?;
                Ok(Response::new(gprc_object))
            }
            Ok(None) => Err(Status::not_found(format!(
                "Object with ID {} {} not found",
                object_id_str,
                original_request_version_str
                    .as_deref()
                    .filter(|s| !s.is_empty())
                    .map_or_else(String::new, |v_str| format!("at version {}", v_str))
            ))),
            Err(storage_err) => {
                eprintln!(
                    "[gRPC ObjectService] Error fetching object {} from storage: {:?}",
                    object_id_str, storage_err
                );
                Err(Status::internal(format!(
                    "Failed to retrieve object {}: {}",
                    object_id_str, storage_err
                )))
            }
        }
    }

    async fn list_objects(
        &self,
        request: Request<ListObjectsRequest>,
    ) -> Result<Response<ListObjectsResponse>, Status> {
        println!(
            "[gRPC ObjectService] Received ListObjects request: {:?}",
            request.get_ref()
        );

        let req_inner = request.into_inner();

        let owner_address_str = req_inner
            .owner_address
            .ok_or_else(|| Status::invalid_argument("owner_address is required for ListObjects"))?;

        let owner_address: IotaAddress =
            owner_address_str.parse().map_err(|e: anyhow::Error| {
                Status::invalid_argument(format!(
                    "Could not parse owner_address '{}': {}",
                    owner_address_str, e
                ))
            })?;

        let cursor_object_id: Option<ObjectID> = req_inner.cursor.map_or(Ok(None), |cur_str| {
            cur_str.parse().map(Some).map_err(|e: ObjectIDParseError| {
                Status::invalid_argument(format!(
                    "Could not parse cursor object_id '{}': {}",
                    cur_str, e
                ))
            })
        })?;

        let limit = req_inner
            .limit
            .unwrap_or(DEFAULT_LIST_OBJECTS_LIMIT)
            .min(MAX_LIST_OBJECTS_LIMIT);
        if limit == 0 {
            return Err(Status::invalid_argument("limit cannot be zero"));
        }

        let mut objects_gprc = Vec::with_capacity(limit as usize);
        let mut next_cursor_object_id: Option<ObjectID> = None;

        match self
            .state_reader
            .account_owned_objects_info_iter(owner_address, cursor_object_id)
        {
            Ok(iter) => {
                for object_info in iter.take(limit as usize + 1) {
                    if objects_gprc.len() < limit as usize {
                        match self.state_reader.get_object(&object_info.object_id) {
                            Ok(Some(core_object)) => {
                                match convert_object_to_gprc(&object_info.object_id, &core_object) {
                                    Ok(gprc_obj) => objects_gprc.push(gprc_obj),
                                    Err(conv_err) => {
                                        eprintln!(
                                            "[gRPC ObjectService] Error converting object {}: {:?}",
                                            object_info.object_id, conv_err
                                        );
                                        // Skip and log
                                    }
                                }
                            }
                            Ok(None) => {
                                eprintln!(
                                    "[gRPC ObjectService] Object {} (from owned list) not found during fetch.",
                                    object_info.object_id
                                );
                                // Skip
                            }
                            Err(storage_err) => {
                                eprintln!(
                                    "[gRPC ObjectService] Error fetching object {}: {:?}",
                                    object_info.object_id, storage_err
                                );
                                return Err(Status::internal(format!(
                                    "Error fetching object data: {}",
                                    storage_err
                                )));
                            }
                        }
                    } else {
                        next_cursor_object_id = Some(object_info.object_id);
                        break; // Limit reached
                    }
                }
            }
            Err(storage_err) => {
                eprintln!(
                    "[gRPC ObjectService] Error calling account_owned_objects_info_iter for {}: {:?}",
                    owner_address, storage_err
                );
                return Err(Status::internal(format!(
                    "Failed to retrieve owned objects list: {}",
                    storage_err
                )));
            }
        }

        Ok(Response::new(ListObjectsResponse {
            objects: objects_gprc,
            next_cursor: next_cursor_object_id.map(|id| id.to_hex_literal()),
        }))
    }

    type StreamObjectsStream = ReceiverStream<Result<ObjectGprc, Status>>;

    async fn stream_objects(
        &self,
        request: Request<StreamObjectsRequest>,
    ) -> Result<Response<Self::StreamObjectsStream>, Status> {
        println!(
            "[gRPC ObjectService] Received StreamObjects request: {:?}",
            request.get_ref()
        );

        let req_inner = request.into_inner();

        let owner_address_str = req_inner.owner_address;
        let owner_address: IotaAddress =
            owner_address_str.parse().map_err(|e: anyhow::Error| {
                Status::invalid_argument(format!(
                    "Could not parse owner_address '{}': {}",
                    owner_address_str, e
                ))
            })?;

        let start_after_object_id: Option<ObjectID> =
            req_inner
                .start_after_object_id
                .map_or(Ok(None), |cur_str| {
                    cur_str.parse().map(Some).map_err(|e: ObjectIDParseError| {
                        Status::invalid_argument(format!(
                            "Could not parse start_after_object_id '{}': {}",
                            cur_str, e
                        ))
                    })
                })?;

        // Collect object infos first to make the subsequent processing Send-able
        let object_infos_result: Result<Vec<AccountOwnedObjectInfo>, Status> = {
            match self
                .state_reader
                .account_owned_objects_info_iter(owner_address, start_after_object_id)
            {
                Ok(iter) => Ok(iter.collect()), // Collect into a Vec
                Err(storage_err) => {
                    eprintln!(
                        "[gRPC StreamObjects] Error calling account_owned_objects_info_iter for {}: {:?}",
                        owner_address, storage_err
                    );
                    Err(Status::internal(format!(
                        "Failed to retrieve owned objects list: {}",
                        storage_err
                    )))
                }
            }
        };

        let object_infos = match object_infos_result {
            Ok(infos) => infos,
            Err(status) => {
                // If initial fetch failed, send one error and close.
                let (tx, rx) = mpsc::channel(1);
                let _ = tx.send(Err(status)).await; // Attempt to send, ignore if client already gone
                return Ok(Response::new(ReceiverStream::new(rx)));
            }
        };

        let (tx, rx) = mpsc::channel(4);
        let state_reader = self.state_reader.clone();

        tokio::spawn(async move {
            for object_info in object_infos {
                // Iterate over the collected Vec (which is Send)
                match state_reader.get_object(&object_info.object_id) {
                    Ok(Some(core_object)) => {
                        match convert_object_to_gprc(&object_info.object_id, &core_object) {
                            Ok(gprc_obj) => {
                                if tx.send(Ok(gprc_obj)).await.is_err() {
                                    eprintln!(
                                        "[gRPC StreamObjects] Client disconnected for {}",
                                        owner_address
                                    );
                                    break;
                                }
                            }
                            Err(conv_err) => {
                                eprintln!(
                                    "[gRPC StreamObjects] Convert error for {}: {:?}",
                                    object_info.object_id, conv_err
                                );
                                // Log and skip
                            }
                        }
                    }
                    Ok(None) => {
                        eprintln!(
                            "[gRPC StreamObjects] Object {} not found for {}",
                            object_info.object_id, owner_address
                        );
                    }
                    Err(storage_err) => {
                        eprintln!(
                            "[gRPC StreamObjects] Storage error for {}: {:?}",
                            object_info.object_id, storage_err
                        );
                        if tx
                            .send(Err(Status::internal(format!(
                                "Error fetching {}: {}",
                                object_info.object_id, storage_err
                            ))))
                            .await
                            .is_err()
                        {
                            eprintln!(
                                "[gRPC StreamObjects] Client disconnected sending storage error for {}",
                                owner_address
                            );
                        }
                        break; // Abort on error
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type SubscribeObjectsByOwnerStream = ReceiverStream<Result<ObjectGprc, Status>>;

    async fn subscribe_objects_by_owner(
        &self,
        request: Request<SubscribeObjectsByOwnerRequest>,
    ) -> Result<Response<Self::SubscribeObjectsByOwnerStream>, Status> {
        let req_inner = request.into_inner();
        println!(
            "[gRPC ObjectService] Received SubscribeObjectsByOwner request: {:?}",
            req_inner
        );

        let owner_address_str = req_inner.owner_address;
        let requested_owner_address: IotaAddress = match parse_iota_address(&owner_address_str) {
            Ok(addr) => addr,
            Err(e) => {
                return Err(Status::invalid_argument(format!(
                    "Invalid owner_address '{}': {}",
                    owner_address_str, e
                )));
            }
        };

        let mut broadcast_rx = self.object_event_sender.subscribe();
        let (tx_to_client, rx_from_task) = mpsc::channel(16); // Channel to client stream

        tokio::spawn(async move {
            println!(
                "[gRPC SubscribeObjectsTask] Started for client subscribing to owner: 0x{}",
                hex::encode(requested_owner_address.as_ref())
            );
            loop {
                match broadcast_rx.recv().await {
                    Ok((event_owner_address, object_arc)) => {
                        if event_owner_address == requested_owner_address {
                            // The object_arc is Arc<IotaObject>. We need its ID for conversion.
                            // However, IotaObject doesn't directly store its ID.
                            // The ID is context-dependent (how it was fetched/named).
                            // For a streamed *new* object, its ID would be derived from its
                            // content/creation tx. For this mock
                            // poller, we used a fixed mock_object_id.
                            // This highlights a slight conceptual mismatch: the poller sends
                            // (Owner, Object),
                            // but convert_object_to_gprc needs (ObjectID, &Object).
                            // Let's use the mock_object_id that the poller is using, for
                            // demonstration. A real system would have a
                            // proper way to get/assign an ID to a new object event.

                            let mock_polled_object_id_str = "0xdeadbeef00000000000000000000000000000000000000000000000000000000";
                            let mock_polled_object_id =
                                ObjectID::from_hex_literal(mock_polled_object_id_str).unwrap();

                            match convert_object_to_gprc(&mock_polled_object_id, &object_arc) {
                                Ok(gprc_object) => {
                                    if tx_to_client.send(Ok(gprc_object)).await.is_err() {
                                        println!(
                                            "[gRPC SubscribeObjectsTask] Client for 0x{} disconnected. Halting.",
                                            hex::encode(requested_owner_address.as_ref())
                                        );
                                        return; // Client disconnected
                                    }
                                }
                                Err(e) => {
                                    eprintln!(
                                        "[gRPC SubscribeObjectsTask] Error converting object for 0x{}: {:?}. Skipping for client.",
                                        hex::encode(requested_owner_address.as_ref()),
                                        e
                                    );
                                    // Don't send error to client, just skip
                                    // this problematic object
                                }
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        eprintln!(
                            "[gRPC SubscribeObjectsTask] Client for 0x{} lagged by {} messages. Some objects were missed.",
                            hex::encode(requested_owner_address.as_ref()),
                            n
                        );
                        // Consider sending an error to the client if this is
                        // critical
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        println!(
                            "[gRPC SubscribeObjectsTask] Broadcast channel closed. Terminating client subscription for 0x{}.",
                            hex::encode(requested_owner_address.as_ref())
                        );
                        break; // Broadcaster (poller) stopped.
                    }
                }
            }
            println!(
                "[gRPC SubscribeObjectsTask] Terminated for client 0x{}.",
                hex::encode(requested_owner_address.as_ref())
            );
        });

        Ok(Response::new(ReceiverStream::new(rx_from_task)))
    }
}
