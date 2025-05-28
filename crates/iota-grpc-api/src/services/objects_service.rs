use iota_types::{
    base_types::{IotaAddress, ObjectID, ObjectIDParseError, SequenceNumber},
    storage::AccountOwnedObjectInfo,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::{
    conversions::objects::convert_object_to_gprc,
    error::GrpcApiError,
    proto::iota::gprc::v1::{
        GetObjectRequest, ListObjectsRequest, ListObjectsResponse, ObjectGprc,
        StreamObjectsRequest, object_gprc_service_server::ObjectGprcService,
    },
    server::StateReader,
};

const DEFAULT_LIST_OBJECTS_LIMIT: u32 = 50;
const MAX_LIST_OBJECTS_LIMIT: u32 = 1000;

#[derive(Clone)]
pub struct ObjectServiceImpl {
    state_reader: StateReader,
}

impl ObjectServiceImpl {
    pub fn new(state_reader: StateReader) -> Self {
        Self { state_reader }
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

        let effective_limit_u32: u32 = req_inner
            .limit
            .unwrap_or(DEFAULT_LIST_OBJECTS_LIMIT)
            .min(MAX_LIST_OBJECTS_LIMIT);
        if effective_limit_u32 == 0 {
            return Err(Status::invalid_argument("limit cannot be zero"));
        }

        // Collect object infos into a Vec first to make it Sendable before any .await
        let object_infos: Vec<AccountOwnedObjectInfo> = match self
            .state_reader
            .account_owned_objects_info_iter(owner_address, cursor_object_id)
        {
            Ok(iter) => iter.take(effective_limit_u32 as usize + 1).collect(), /* Collect one
                                                                                 * more than limit
                                                                                 * to determine
                                                                                 * next_cursor */
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
        };

        let mut collected_objects_gprc = Vec::new();
        let mut next_cursor_gprc: Option<String> = None;

        for (idx, object_info) in object_infos.iter().enumerate() {
            if idx >= effective_limit_u32 as usize {
                // This item is the +1 item, use its ID for next_cursor if we took
                // effective_limit items
                if collected_objects_gprc.len() == effective_limit_u32 as usize {
                    next_cursor_gprc = Some(object_info.object_id.to_hex_literal());
                }
                break;
            }

            match self.state_reader.get_object(&object_info.object_id) {
                Ok(Some(core_object)) => {
                    match convert_object_to_gprc(&object_info.object_id, &core_object) {
                        Ok(gprc_object) => {
                            collected_objects_gprc.push(gprc_object);
                        }
                        Err(e) => {
                            eprintln!(
                                "[gRPC ObjectService] Error converting object {}: {:?}. Skipping.",
                                object_info.object_id, e
                            );
                        }
                    }
                }
                Ok(None) => {
                    eprintln!(
                        "[gRPC ObjectService] Object {} from AccountOwnedObjectInfo not found in store. Skipping.",
                        object_info.object_id
                    );
                }
                Err(e) => {
                    eprintln!(
                        "[gRPC ObjectService] Storage error fetching object {}: {:?}. Skipping.",
                        object_info.object_id, e
                    );
                }
            }
            // Removed yield_now().await from here as we collected infos first
        }

        Ok(Response::new(ListObjectsResponse {
            objects: collected_objects_gprc,
            next_cursor: next_cursor_gprc,
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
}
