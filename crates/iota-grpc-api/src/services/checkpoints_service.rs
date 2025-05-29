use std::str::FromStr; // Added for CheckpointDigest::from_str
use std::sync::Arc; // For Arc<VerifiedCheckpoint>
use std::time::Duration; // For tokio::time::interval

use anyhow; // Ensure anyhow is in scope
use iota_types::storage::error::Error as StorageError; // Direct import with alias
use iota_types::{
    digests::CheckpointDigest,                                     // Added
    full_checkpoint_content::CheckpointData as CoreCheckpointData, // Alias to avoid conflict
    messages_checkpoint::{CheckpointContents, VerifiedCheckpoint},
};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

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
            "Invalid checkpoint_id format: '{}'. Expected u64 sequence number or hex digest.",
            id_str
        )))
    }
}

#[derive(Clone)]
pub struct CheckpointServiceImpl {
    state_reader: StateReader,
    checkpoint_event_sender: broadcast::Sender<Arc<VerifiedCheckpoint>>,
    // Keep a receiver to prevent the channel from closing immediately if no one subscribes right
    // away. Or ensure the poller task keeps a receiver. For simplicity, the poller will keep
    // one.
}

impl CheckpointServiceImpl {
    pub fn new(state_reader: StateReader) -> Self {
        let (tx, _rx) = broadcast::channel::<Arc<VerifiedCheckpoint>>(32_usize); // Buffer size 32, can be tuned

        let poller_state_reader = state_reader.clone();
        let poller_tx = tx.clone();

        tokio::spawn(async move {
            let mut last_known_seq = match poller_state_reader
                .get_latest_checkpoint_sequence_number()
            {
                Ok(seq) => seq,
                Err(e) => {
                    eprintln!(
                        "[CheckpointPoller] Failed to get initial latest sequence number: {:?}. Starting from 0.",
                        e
                    );
                    0
                }
            };
            println!(
                "[CheckpointPoller] Initialized. Last known sequence number: {}",
                last_known_seq
            );

            let mut interval = tokio::time::interval(Duration::from_secs(1)); // Polling interval

            // Keep a receiver to ensure the channel stays open as long as the poller is
            // running.
            let _poller_self_receiver = poller_tx.subscribe();

            loop {
                interval.tick().await;
                match poller_state_reader.get_latest_checkpoint_sequence_number() {
                    Ok(current_latest_seq) => {
                        if current_latest_seq > last_known_seq {
                            println!(
                                "[CheckpointPoller] New checkpoints detected. From {} up to {}",
                                last_known_seq + 1,
                                current_latest_seq
                            );
                            for seq_to_fetch in (last_known_seq + 1)..=current_latest_seq {
                                match poller_state_reader
                                    .get_checkpoint_by_sequence_number(seq_to_fetch)
                                {
                                    Ok(Some(verified_checkpoint)) => {
                                        // println!("[CheckpointPoller] Publishing checkpoint {}",
                                        // seq_to_fetch);
                                        if poller_tx.send(Arc::new(verified_checkpoint)).is_err() {
                                            // This happens if all receivers are dropped.
                                            // The poller_self_receiver should prevent this unless
                                            // it's also dropped somehow
                                            // or the channel is explicitly closed.
                                            println!(
                                                "[CheckpointPoller] No active subscribers to send checkpoint {}. Polling continues.",
                                                seq_to_fetch
                                            );
                                        }
                                    }
                                    Ok(None) => {
                                        eprintln!(
                                            "[CheckpointPoller] Checkpoint {} reported as latest but not found. Possible inconsistency.",
                                            seq_to_fetch
                                        );
                                    }
                                    Err(e) => {
                                        eprintln!(
                                            "[CheckpointPoller] Error fetching checkpoint {} for broadcast: {:?}",
                                            seq_to_fetch, e
                                        );
                                    }
                                }
                            }
                            last_known_seq = current_latest_seq;
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "[CheckpointPoller] Error polling latest checkpoint sequence number: {:?}",
                            e
                        );
                    }
                }
            }
        });

        Self {
            state_reader,
            checkpoint_event_sender: tx,
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
                            "Checkpoint with sequence number {} not found",
                            seq_num
                        ))
                    })?;

                checkpoint_contents = self.state_reader.get_checkpoint_contents_by_sequence_number(seq_num)
                    .map_err(|e: StorageError| GrpcApiError::SystemError(anyhow::Error::new(e)))?
                    .ok_or_else(|| {
                        Status::not_found(format!(
                            "Checkpoint contents for sequence number {} not found. This may indicate data inconsistency.",
                            seq_num
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
                            "Checkpoint with digest {} not found",
                            digest.to_string()
                        ))
                    })?;

                checkpoint_contents = self.state_reader.get_checkpoint_contents_by_digest(&verified_checkpoint.inner().content_digest)
                    .map_err(|e: StorageError| GrpcApiError::SystemError(anyhow::Error::new(e)))?
                    .ok_or_else(|| {
                        Status::not_found(format!(
                            "Checkpoint contents for digest {} (from checkpoint {}) not found. This may indicate data inconsistency.",
                            verified_checkpoint.inner().content_digest.to_string(),
                            digest.to_string()
                        ))
                    })?;
            }
        }

        let core_checkpoint_data: CoreCheckpointData = self
            .state_reader
            .get_checkpoint_data(verified_checkpoint.clone(), checkpoint_contents)
            .map_err(|e| {
                eprintln!(
                    "[gRPC CheckpointService] Error constructing CheckpointData for {}: {:?}",
                    checkpoint_id_str, e
                );
                Status::internal(format!("Failed to construct full checkpoint data: {}", e))
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
                            "Checkpoint summary for sequence number {} not found",
                            seq_num
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
                            "Checkpoint summary for digest {} not found",
                            digest.to_string()
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

        let limit = req.limit.map_or(10_u32, |l: u32| l.min(100_u32).max(1_u32)) as usize; // Added explicit type for l

        let start_seq_num = req
            .start_sequence_number
            .as_deref()
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| {
                Status::invalid_argument("start_sequence_number must be a valid u64 string")
            })?;

        let end_seq_num_opt = req
            .end_sequence_number
            .as_deref()
            .and_then(|s| s.parse::<u64>().ok());

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
                                "[gRPC CheckpointService] Failed to convert checkpoint {}: {:?}",
                                current_seq, e
                            );
                            return Err(Status::internal(format!(
                                "Conversion error for checkpoint {}: {}",
                                current_seq, e
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
                        "[gRPC CheckpointService] Error fetching checkpoint {} from storage: {:?}",
                        current_seq, storage_err
                    );
                    return Err(Status::internal(format!(
                        "Failed to retrieve checkpoint {}: {}",
                        current_seq, storage_err
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
        let req = request.into_inner();
        println!(
            "[gRPC CheckpointService] Received SubscribeNewCheckpoints request: {:?}",
            req
        );

        let include_full_data = req.include_full_data;
        let requested_start_seq_opt = req
            .start_from_checkpoint_sequence_number
            .as_deref()
            .and_then(|s| s.parse::<u64>().ok());

        let mut broadcast_rx = self.checkpoint_event_sender.subscribe();
        let (tx_to_client, rx_from_task) = mpsc::channel(16); // Channel to client stream

        let state_reader_clone_for_full_data = self.state_reader.clone();

        tokio::spawn(async move {
            loop {
                match broadcast_rx.recv().await {
                    Ok(verified_checkpoint_arc) => {
                        let checkpoint_seq_num_ref = verified_checkpoint_arc.sequence_number();

                        // Filter based on start_from_sequence_number
                        if let Some(start_from) = requested_start_seq_opt {
                            if *checkpoint_seq_num_ref < start_from {
                                // println!("[gRPC Subscribe Task] Skipping checkpoint {} as it is <
                                // start_from_sequence_number {}.", *checkpoint_seq_num_ref,
                                // start_from);
                                continue;
                            }
                        }
                        // println!("[gRPC Subscribe Task] Processing checkpoint {} for client.",
                        // *checkpoint_seq_num_ref);

                        let checkpoint_result: Result<StreamedCheckpoint, Status> =
                            if include_full_data {
                                match state_reader_clone_for_full_data
                                    .get_checkpoint_contents_by_sequence_number(
                                        *checkpoint_seq_num_ref,
                                    ) {
                                    Ok(Some(checkpoint_contents)) => {
                                        let core_verified_checkpoint =
                                            (*verified_checkpoint_arc).clone();
                                        match state_reader_clone_for_full_data.get_checkpoint_data(core_verified_checkpoint, checkpoint_contents) {
                                        Ok(core_data) => convert_full_checkpoint_data_to_gprc(&core_data)
                                            .map(|gprc_data| StreamedCheckpoint {
                                                checkpoint_type: Some(crate::proto::iota::gprc::v1::streamed_checkpoint::CheckpointType::FullData(gprc_data)),
                                            })
                                            .map_err(|e| {
                                                eprintln!("[gRPC Subscribe Task] Convert full data error {}: {:?}. Skipping for client.", *checkpoint_seq_num_ref, e);
                                                Status::internal(format!("Skipping {} due to conversion error", *checkpoint_seq_num_ref))
                                            }),
                                        Err(e) => {
                                            eprintln!("[gRPC Subscribe Task] Get CheckpointData error {}: {:?}. Skipping for client.", *checkpoint_seq_num_ref, e);
                                            Err(Status::internal(format!("Skipping {} due to data construction error", *checkpoint_seq_num_ref)))
                                        }
                                    }
                                    }
                                    Ok(None) => {
                                        eprintln!(
                                            "[gRPC Subscribe Task] Contents for {} not found (needed for full_data). Skipping for client.",
                                            *checkpoint_seq_num_ref
                                        );
                                        Err(Status::internal(format!(
                                            "Skipping {} due to missing contents for full data",
                                            *checkpoint_seq_num_ref
                                        )))
                                    }
                                    Err(e) => {
                                        eprintln!(
                                            "[gRPC Subscribe Task] Storage error for contents {}: {:?}. Skipping for client.",
                                            *checkpoint_seq_num_ref, e
                                        );
                                        Err(Status::internal(format!(
                                            "Skipping {} due to storage error for contents",
                                            *checkpoint_seq_num_ref
                                        )))
                                    }
                                }
                            } else {
                                convert_verified_checkpoint_to_gprc_summary(&verified_checkpoint_arc)
                                .map(|summary_gprc| StreamedCheckpoint {
                                    checkpoint_type: Some(crate::proto::iota::gprc::v1::streamed_checkpoint::CheckpointType::Summary(summary_gprc)),
                                })
                                .map_err(|e| {
                                     eprintln!("[gRPC Subscribe Task] Convert summary error {}: {:?}. Skipping for client.", *checkpoint_seq_num_ref, e);
                                     Status::internal(format!("Skipping {} due to conversion error", *checkpoint_seq_num_ref))
                                })
                            };

                        match checkpoint_result {
                            Ok(streamed_item) => {
                                if tx_to_client.send(Ok(streamed_item)).await.is_err() {
                                    println!("[gRPC Subscribe Task] Client disconnected. Halting.");
                                    return;
                                }
                            }
                            Err(_status) => {
                                // Error already logged by the conversion/fetch
                                // logic if it was a skip.
                                // If client disconnected, loop will break on
                                // next send attempt or here.
                                // If it was a genuine error that should be
                                // propagated, it can be sent:
                                // if tx_to_client.send(Err(status)).await.
                                // is_err() { /*...*/ }
                                // Current design: log and skip problematic
                                // items, client sees no error for these.
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        eprintln!(
                            "[gRPC Subscribe Task] Lagged by {} messages. Some checkpoints were missed.",
                            n
                        );
                        // Potentially send an error to the client or try to
                        // resync? For now, just log.
                        // This client might need to re-subscribe if strict
                        // ordering and no-loss is critical.
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        println!(
                            "[gRPC Subscribe Task] Broadcast channel closed. Terminating client subscription."
                        );
                        break; // Broadcaster (poller) stopped or channel closed.
                    }
                }
            }
            println!("[gRPC Subscribe Task] Terminated for client.");
        });

        Ok(Response::new(ReceiverStream::new(rx_from_task)))
    }
}
