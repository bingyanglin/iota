use std::str::FromStr; // Added for CheckpointDigest::from_str
use std::sync::Arc; // For Arc<VerifiedCheckpoint>

use anyhow; // Ensure anyhow is in scope
use iota_types::storage::error::Error as StorageError; // Direct import with alias
use iota_types::storage::{ReadStore, RestStateReader}; // Added RestStateReader
use iota_types::{
    digests::CheckpointDigest,                                     // Added
    full_checkpoint_content::CheckpointData as CoreCheckpointData, // Alias to avoid conflict
    messages_checkpoint::{CheckpointContents, VerifiedCheckpoint},
};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::error; // Added for logging macros, removed info and warn

use crate::conversions::checkpoints::{
    convert_full_checkpoint_data_to_gprc, convert_verified_checkpoint_to_gprc_summary,
}; // Import conversion functions
// This will eventually come from a shared context or be passed in.
// For now, let's assume a simplified StateReader is available.
// You'll need to define this trait and a concrete implementation for your actual state access.
pub use crate::server::StateReader; /* This now refers to Arc<dyn
                                      * iota_types::storage::RestStateReader> */
use crate::{
    error::GrpcApiError,
    proto::iota::gprc::v1::{
        CheckpointDataGprc, CheckpointPageGprc, Direction, GetCheckpointRequest,
        ListCheckpointsRequest, SignedCheckpointSummaryGprc, StreamedCheckpoint,
        SubscribeNewCheckpointsRequest, checkpoint_gprc_service_server::CheckpointGprcService,
    },
}; // Import GrpcApiError for mapping // Ensure VerifiedCheckpoint is imported // Alias for clarity

// Helper enum for identifying checkpoint request type
enum CheckpointIdentifier {
    SequenceNumber(u64),
    Digest(CheckpointDigest),
}

// Helper function to parse checkpoint_id string
fn parse_checkpoint_id_str(id_str: &str) -> Result<CheckpointIdentifier, Status> {
    if let Ok(seq_num) = id_str.parse::<u64>() {
        Ok(CheckpointIdentifier::SequenceNumber(seq_num))
    } else if let Ok(digest) = CheckpointDigest::from_str(id_str) {
        Ok(CheckpointIdentifier::Digest(digest))
    } else {
        Err(Status::invalid_argument(format!(
            "Invalid checkpoint_id format: '{id_str}'. Expected u64 sequence number or hex digest."
        )))
    }
}

#[derive(Clone)]
pub struct CheckpointServiceImpl {
    state_reader: StateReader,
    checkpoint_event_sender: broadcast::Sender<Arc<VerifiedCheckpoint>>,
}

impl CheckpointServiceImpl {
    pub fn new(
        state_reader: StateReader,
        checkpoint_event_sender: broadcast::Sender<Arc<VerifiedCheckpoint>>,
    ) -> Self {
        // The polling loop is removed.
        // The checkpoint_event_sender is now passed in directly.
        Self {
            state_reader,
            checkpoint_event_sender,
        }
    }
}

#[tonic::async_trait]
impl CheckpointGprcService for CheckpointServiceImpl {
    async fn get_checkpoint_full(
        &self,
        request: Request<GetCheckpointRequest>,
    ) -> Result<Response<CheckpointDataGprc>, Status> {
        println!(
            "[gRPC CheckpointService] Received GetCheckpointFull request: {:?}",
            request.get_ref()
        );

        let checkpoint_id_str = &request.get_ref().checkpoint_id;
        let identifier = parse_checkpoint_id_str(checkpoint_id_str)?;

        let verified_checkpoint: VerifiedCheckpoint;
        let checkpoint_contents: CheckpointContents;

        match identifier {
            CheckpointIdentifier::SequenceNumber(seq_num) => {
                verified_checkpoint = self
                    .state_reader
                    .get_checkpoint_by_sequence_number(seq_num)
                    .map_err(|e: StorageError| GrpcApiError::SystemError(anyhow::Error::new(e)))?
                    .ok_or_else(|| {
                        Status::not_found(format!(
                            "Checkpoint with sequence number {seq_num} not found"
                        ))
                    })?;

                checkpoint_contents = self.state_reader.get_checkpoint_contents_by_sequence_number(seq_num)
                    .map_err(|e: StorageError| GrpcApiError::SystemError(anyhow::Error::new(e)))?
                    .ok_or_else(|| {
                        Status::not_found(format!(
                            "Checkpoint contents for sequence number {seq_num} not found. This may indicate data inconsistency."
                        ))
                    })?;
            }
            CheckpointIdentifier::Digest(digest) => {
                verified_checkpoint = self
                    .state_reader
                    .get_checkpoint_by_digest(&digest)
                    .map_err(|e: StorageError| GrpcApiError::SystemError(anyhow::Error::new(e)))?
                    .ok_or_else(|| {
                        Status::not_found(format!("Checkpoint with digest {digest} not found"))
                    })?;

                checkpoint_contents = self.state_reader.get_checkpoint_contents_by_digest(&verified_checkpoint.inner().content_digest)
                    .map_err(|e: StorageError| GrpcApiError::SystemError(anyhow::Error::new(e)))?
                    .ok_or_else(|| {
                        Status::not_found(format!(
                            "Checkpoint contents for digest {} (from checkpoint {}) not found. This may indicate data inconsistency.",
                            verified_checkpoint.inner().content_digest,
                            digest
                        ))
                    })?;
            }
        }

        let core_checkpoint_data: CoreCheckpointData = self
            .state_reader
            .get_checkpoint_data(verified_checkpoint.clone(), checkpoint_contents)
            .map_err(|e| {
                eprintln!(
                    "[gRPC CheckpointService] Error constructing CheckpointData for {checkpoint_id_str}: {e:?}"
                );
                Status::internal(format!("Failed to construct full checkpoint data: {e}"))
            })?;

        let gprc_checkpoint_data = convert_full_checkpoint_data_to_gprc(&core_checkpoint_data)
            .map_err(|e| GrpcApiError::ConversionError(e.to_string()))?;

        Ok(Response::new(gprc_checkpoint_data))
    }

    async fn get_checkpoint(
        &self,
        request: Request<GetCheckpointRequest>,
    ) -> Result<Response<SignedCheckpointSummaryGprc>, Status> {
        println!(
            "[gRPC CheckpointService] Received GetCheckpoint request: {:?}",
            request.get_ref()
        );

        let checkpoint_id_str = &request.get_ref().checkpoint_id;
        let identifier = parse_checkpoint_id_str(checkpoint_id_str)?;

        let verified_checkpoint: VerifiedCheckpoint;

        match identifier {
            CheckpointIdentifier::SequenceNumber(seq_num) => {
                verified_checkpoint = self
                    .state_reader
                    .get_checkpoint_by_sequence_number(seq_num)
                    .map_err(|e: StorageError| GrpcApiError::SystemError(anyhow::Error::new(e)))?
                    .ok_or_else(|| {
                        Status::not_found(format!(
                            "Checkpoint summary for sequence number {seq_num} not found"
                        ))
                    })?;
            }
            CheckpointIdentifier::Digest(digest) => {
                verified_checkpoint = self
                    .state_reader
                    .get_checkpoint_by_digest(&digest)
                    .map_err(|e: StorageError| GrpcApiError::SystemError(anyhow::Error::new(e)))?
                    .ok_or_else(|| {
                        Status::not_found(format!(
                            "Checkpoint summary for digest {digest} not found"
                        ))
                    })?;
            }
        }

        let gprc_summary = convert_verified_checkpoint_to_gprc_summary(&verified_checkpoint)
            .map_err(|e| GrpcApiError::ConversionError(e.to_string()))?;

        Ok(Response::new(gprc_summary))
    }

    async fn list_checkpoints(
        &self,
        request: Request<ListCheckpointsRequest>,
    ) -> Result<Response<CheckpointPageGprc>, Status> {
        let req = request.into_inner();
        println!(
            "[gRPC CheckpointService] Received ListCheckpoints request: {:?}",
            req
        );

        let limit = req.limit.map_or(10_u32, |l: u32| l.min(100_u32).max(1_u32)) as usize;

        let start_seq_num = match req.start_sequence_number.as_deref() {
            Some(s) => s.parse::<u64>().map_err(|_| {
                Status::invalid_argument(
                    "start_sequence_number must be a valid u64 string".to_string(),
                )
            })?,
            None => {
                // Default to 0 if not provided, or handle as error if strictly required by API
                // spec For now, let's assume it's required as per previous
                // logic for None.
                return Err(Status::invalid_argument(
                    "start_sequence_number is required".to_string(),
                ));
            }
        };

        let end_seq_num_opt = match req.end_sequence_number.as_deref() {
            Some(s) if !s.is_empty() => match s.parse::<u64>() {
                Ok(val) => Some(val),
                Err(_) => {
                    return Err(Status::invalid_argument(
                        "If provided, end_sequence_number must be a valid u64 string".to_string(),
                    ));
                }
            },
            Some(_) => None, // Handles empty string as None
            None => None,
        };

        let direction =
            match Direction::try_from(req.direction.unwrap_or(Direction::Ascending as i32)) {
                Ok(dir) => dir,
                Err(_) => return Err(Status::invalid_argument("Invalid direction value")),
            };

        if let Some(end_val) = end_seq_num_opt {
            match direction {
                Direction::Ascending if end_val < start_seq_num => {
                    return Err(Status::invalid_argument(
                        "end_sequence_number cannot be less than start_sequence_number for Ascending direction",
                    ));
                }
                Direction::Descending if end_val > start_seq_num => {
                    return Err(Status::invalid_argument(
                        "end_sequence_number cannot be greater than start_sequence_number for Descending direction",
                    ));
                }
                _ => {}
            }
        }

        let mut checkpoints_gprc = Vec::with_capacity(limit);
        let mut current_seq = start_seq_num;
        let mut can_fetch_more = true; // Flag to control loop if end_seq_num_opt is None

        for _ in 0..limit {
            if !can_fetch_more {
                break;
            }

            // Boundary checks based on direction and end_sequence_number
            if let Some(end_val) = end_seq_num_opt {
                match direction {
                    Direction::Ascending if current_seq > end_val => break,
                    Direction::Descending if current_seq < end_val => break,
                    _ => {}
                }
            }

            match self
                .state_reader
                .get_checkpoint_by_sequence_number(current_seq)
            {
                Ok(Some(verified_checkpoint)) => {
                    match convert_verified_checkpoint_to_gprc_summary(&verified_checkpoint) {
                        Ok(summary_gprc) => {
                            checkpoints_gprc.push(summary_gprc);
                        }
                        Err(e) => {
                            // Log error and potentially continue or return internal error
                            eprintln!(
                                "[gRPC CheckpointService] Failed to convert checkpoint {current_seq}: {e:?}"
                            );
                            return Err(Status::internal(format!(
                                "Conversion error for checkpoint {current_seq}: {e}"
                            )));
                        }
                    }
                }
                Ok(None) => {
                    // No more checkpoints in this direction or sequence number doesn't exist
                    can_fetch_more = false; // Stop trying if we hit a None, assuming sequence is contiguous for the query
                    break;
                }
                Err(storage_err) => {
                    eprintln!(
                        "[gRPC CheckpointService] Error fetching checkpoint {current_seq} from storage: {storage_err:?}"
                    );
                    return Err(Status::internal(format!(
                        "Failed to retrieve checkpoint {current_seq}: {storage_err}"
                    )));
                }
            }

            if direction == Direction::Ascending {
                if current_seq == u64::MAX {
                    can_fetch_more = false;
                    break;
                } // Prevent overflow
                current_seq += 1;
            } else {
                // Descending
                if current_seq == 0 {
                    can_fetch_more = false;
                    break;
                } // Prevent underflow
                current_seq -= 1;
            }
        }

        let next_cursor = if checkpoints_gprc.len() == limit && can_fetch_more {
            // If we fetched a full page and didn't hit a boundary or a None from storage
            // The `current_seq` is now the one *after* the last one successfully processed
            // or attempted
            Some(current_seq.to_string())
        } else {
            None
        };

        Ok(Response::new(CheckpointPageGprc {
            checkpoints: checkpoints_gprc,
            next_cursor,
        }))
    }

    type SubscribeNewCheckpointsStream = ReceiverStream<Result<StreamedCheckpoint, Status>>;

    async fn subscribe_new_checkpoints(
        &self,
        request: Request<SubscribeNewCheckpointsRequest>,
    ) -> Result<Response<Self::SubscribeNewCheckpointsStream>, Status> {
        let req_inner = request.into_inner();
        let request_start_seq_str = req_inner.start_from_checkpoint_sequence_number;
        let request_include_full_data = req_inner.include_full_data;

        let start_from_checkpoint_sequence_number = request_start_seq_str
            .as_deref()
            .unwrap_or("0") // Default to 0 if not provided
            .parse::<u64>()
            .unwrap_or(0); // Default to 0 on parse error

        println!(
            "[gRPC CheckpointService] Client subscribed for new checkpoints starting from: {}, include_full_data: {}",
            start_from_checkpoint_sequence_number, request_include_full_data
        );

        let mut rx = self.checkpoint_event_sender.subscribe();
        let (tx, client_rx) = mpsc::channel(32); // Buffer for the client stream

        let state_reader_clone = self.state_reader.clone(); // Clone for the spawned task

        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(verified_checkpoint_arc) => {
                        let checkpoint_seq_num = *verified_checkpoint_arc.sequence_number();
                        if checkpoint_seq_num >= start_from_checkpoint_sequence_number {
                            println!(
                                "[gRPC CheckpointService] Processing checkpoint {} for client (requested start: {}, full_data: {}).",
                                checkpoint_seq_num,
                                start_from_checkpoint_sequence_number,
                                request_include_full_data
                            );

                            let checkpoint_to_send_result: Result<StreamedCheckpoint, Status> =
                                if request_include_full_data {
                                    match get_full_checkpoint_data_for_stream(
                                    verified_checkpoint_arc.clone(),
                                    &*state_reader_clone // Pass the cloned StateReader
                                ).await {
                                    Ok(full_data_gprc) => Ok(StreamedCheckpoint {
                                        checkpoint_type: Some(crate::proto::iota::gprc::v1::streamed_checkpoint::CheckpointType::FullData(full_data_gprc)),
                                    }),
                                    Err(e_status) => {
                                        // get_full_checkpoint_data_for_stream already returns a Status
                                        error!(
                                            "[gRPC CheckpointService] Error fetching full checkpoint data for {}: {}. Forwarding status to client.",
                                            checkpoint_seq_num, e_status
                                        );
                                        Err(e_status)
                                    }
                                }
                                } else {
                                    match convert_verified_checkpoint_to_gprc_summary(&*verified_checkpoint_arc) {
                                    Ok(summary_gprc) => Ok(StreamedCheckpoint {
                                        checkpoint_type: Some(crate::proto::iota::gprc::v1::streamed_checkpoint::CheckpointType::Summary(summary_gprc)),
                                    }),
                                    Err(e) => {
                                        error!(
                                            "[gRPC CheckpointService] Error converting checkpoint summary for {}: {}. Sending internal error.", 
                                            checkpoint_seq_num, e
                                        );
                                        Err(Status::internal(format!(
                                            "Error converting checkpoint summary for {}: {}",
                                            checkpoint_seq_num, e
                                        )))
                                    }
                                }
                                };

                            if tx.send(checkpoint_to_send_result).await.is_err() {
                                println!(
                                    "[gRPC CheckpointService] Client for checkpoint stream (seq start: {}) disconnected.",
                                    start_from_checkpoint_sequence_number
                                );
                                break; // Break from the loop if client is gone
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        eprintln!(
                            "[gRPC CheckpointService] Checkpoint broadcast receiver lagged by {n} messages. Sending DataLoss to client."
                        );
                        if tx.send(Err(Status::data_loss(format!(
                            "Checkpoint stream lagged by {} messages. Some checkpoints were missed.",
                            n
                        )))).await.is_err() {
                            println!(
                                "[gRPC CheckpointService] Client for checkpoint stream (seq start: {}) disconnected while sending Lagged error.",
                                start_from_checkpoint_sequence_number
                            );
                        }
                        break; // Lagged, terminate this specific client stream
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        println!(
                            "[gRPC CheckpointService] Checkpoint broadcast channel closed. Terminating client stream (seq start: {}).",
                            start_from_checkpoint_sequence_number
                        );
                        break; // Channel closed, terminate client stream
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(client_rx)))
    }
}

// Helper to get full data, converting errors to Status
// This helper function is now defined outside the CheckpointGprcService impl
// block and does not have `&self` as it's called from a static context in the
// tokio::spawn task.
async fn get_full_checkpoint_data_for_stream(
    verified_checkpoint: Arc<VerifiedCheckpoint>,
    state_reader: &dyn RestStateReader, // Changed from ReadStore to RestStateReader
) -> Result<crate::proto::iota::gprc::v1::CheckpointDataGprc, Status> {
    let seq_num = *verified_checkpoint.sequence_number();
    let contents = state_reader
        .get_checkpoint_contents_by_sequence_number(seq_num)
        .map_err(|db_err| {
            Status::internal(format!(
                "Database error fetching contents for checkpoint {}: {}",
                seq_num, db_err
            ))
        })?
        .ok_or_else(|| {
            Status::not_found(format!(
                "Checkpoint contents for sequence number {} not found",
                seq_num
            ))
        })?;

    let core_checkpoint_data = state_reader
        .get_checkpoint_data((*verified_checkpoint).clone(), contents)
        .map_err(|anyhow_err| {
            Status::internal(format!(
                "Error constructing CoreCheckpointData for checkpoint {}: {}",
                seq_num, anyhow_err
            ))
        })?;

    convert_full_checkpoint_data_to_gprc(&core_checkpoint_data).map_err(|conv_err| {
        Status::internal(format!(
            "Error converting CoreCheckpointData to gRPC for checkpoint {}: {}",
            seq_num, conv_err
        ))
    })
}
