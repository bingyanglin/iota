use std::sync::Arc; // For Arc<VerifiedCheckpoint>
use std::time::Duration; // For tokio::time::interval

use iota_types::messages_checkpoint::VerifiedCheckpoint;
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
        ListCheckpointsRequest, SignedCheckpointSummaryGprc, StreamCheckpointsInRangeRequest,
        StreamedCheckpoint, SubscribeNewCheckpointsRequest,
        checkpoint_gprc_service_server::CheckpointGprcService,
    },
}; // Import GrpcApiError for mapping // Ensure VerifiedCheckpoint is imported

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
        let (tx, _rx) = broadcast::channel::<Arc<VerifiedCheckpoint>>(32); // Buffer size 32, can be tuned

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

        let req_inner = request.get_ref(); // Keep as ref if only reading checkpoint_id
        let checkpoint_id_str = &req_inner.checkpoint_id;

        let seq_num = checkpoint_id_str.parse::<u64>().map_err(|e| {
            Status::invalid_argument(format!(
                "Could not parse checkpoint_id '{}' as u64: {}",
                checkpoint_id_str, e
            ))
        })?;

        // In a real implementation:
        // 1. Use self.state_reader to fetch the actual checkpoint data from
        //    storage/core. The RestStateReader trait has
        //    `get_checkpoint_by_sequence_number` and
        //    `get_full_checkpoint_contents_by_sequence_number`. However,
        //    iota_types::full_checkpoint_content::CheckpointData expects
        //    CertifiedCheckpointSummary and CheckpointContents. The method
        //    `get_checkpoint_data(&self, checkpoint: VerifiedCheckpoint,
        //    checkpoint_contents: CheckpointContents) ->
        //    anyhow::Result<CheckpointData>` on RestStateReader seems most appropriate
        //    if we fetch VerifiedCheckpoint and CheckpointContents separately.

        let verified_checkpoint = self
            .state_reader
            .get_checkpoint_by_sequence_number(seq_num)
            .map_err(|e| {
                eprintln!(
                    "[gRPC CheckpointService] Storage error fetching checkpoint {}: {:?}",
                    seq_num, e
                );
                Status::internal(format!("Storage error fetching checkpoint: {}", e))
            })?;

        let verified_checkpoint = match verified_checkpoint {
            Some(vc) => vc,
            None => {
                return Err(Status::not_found(format!(
                    "Checkpoint with sequence number {} not found",
                    seq_num
                )));
            }
        };

        let checkpoint_contents = self
            .state_reader
            .get_checkpoint_contents_by_sequence_number(seq_num)
            .map_err(|e| {
                eprintln!(
                    "[gRPC CheckpointService] Storage error fetching checkpoint contents {}: {:?}",
                    seq_num, e
                );
                Status::internal(format!("Storage error fetching checkpoint contents: {}", e))
            })?;

        let checkpoint_contents = match checkpoint_contents {
            Some(cc) => cc,
            None => {
                // This case should ideally not happen if the summary was found, implies data
                // inconsistency
                eprintln!(
                    "[gRPC CheckpointService] Checkpoint contents for sequence number {} not found, though summary was present.",
                    seq_num
                );
                return Err(Status::internal(format!(
                    "Checkpoint contents for sequence number {} not found, data inconsistency?",
                    seq_num
                )));
            }
        };

        // Now, use the state_reader's get_checkpoint_data method
        match self
            .state_reader
            .get_checkpoint_data(verified_checkpoint, checkpoint_contents)
        {
            Ok(core_checkpoint_data) => {
                let gprc_checkpoint_data =
                    convert_full_checkpoint_data_to_gprc(&core_checkpoint_data)
                        .map_err(GrpcApiError::from)?; // Convert custom error to tonic::Status
                Ok(Response::new(gprc_checkpoint_data))
            }
            Err(e) => {
                eprintln!(
                    "[gRPC CheckpointService] Error constructing CheckpointData for {}: {:?}",
                    seq_num, e
                );
                Err(Status::internal(format!(
                    "Failed to construct full checkpoint data: {}",
                    e
                )))
            }
        }
    }

    async fn get_checkpoint(
        &self,
        request: Request<GetCheckpointRequest>,
    ) -> Result<Response<SignedCheckpointSummaryGprc>, Status> {
        println!(
            "[gRPC CheckpointService] Received GetCheckpoint request: {:?}",
            request.get_ref()
        );

        let req_inner = request.into_inner();
        let checkpoint_id_str = req_inner.checkpoint_id;

        let seq_num = checkpoint_id_str.parse::<u64>().map_err(|e| {
            Status::invalid_argument(format!(
                "Could not parse checkpoint_id '{}' as u64: {}",
                checkpoint_id_str, e
            ))
        })?;

        // Use state_reader to fetch the actual checkpoint data
        match self.state_reader.get_checkpoint_by_sequence_number(seq_num) {
            Ok(Some(verified_checkpoint)) => {
                // Convert the core Rust type to the gRPC type
                let gprc_summary =
                    convert_verified_checkpoint_to_gprc_summary(&verified_checkpoint)
                        .map_err(GrpcApiError::from)?; // Convert your GrpcApiError to tonic::Status
                Ok(Response::new(gprc_summary))
            }
            Ok(None) => Err(Status::not_found(format!(
                "Checkpoint with sequence number {} not found",
                seq_num
            ))),
            Err(storage_err) => {
                eprintln!(
                    "[gRPC CheckpointService] Error fetching checkpoint {} from storage: {:?}",
                    seq_num, storage_err
                );
                Err(Status::internal(format!(
                    "Failed to retrieve checkpoint {}: {}",
                    seq_num, storage_err
                )))
            }
        }
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

        let limit = req.limit.map_or(10u32, |l| l.min(100).max(1)) as usize; // Default 10, min 1, max 100

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

    type StreamCheckpointsInRangeStream = ReceiverStream<Result<StreamedCheckpoint, Status>>;

    async fn stream_checkpoints_in_range(
        &self,
        request: Request<StreamCheckpointsInRangeRequest>,
    ) -> Result<Response<Self::StreamCheckpointsInRangeStream>, Status> {
        let req = request.into_inner();
        println!(
            "[gRPC CheckpointService] Received StreamCheckpointsInRange request: {:?}",
            req
        );

        let start_seq_num = req
            .start_sequence_number
            .parse::<u64>()
            .map_err(|_e| Status::invalid_argument("Invalid start_sequence_number format"))?;

        let end_seq_num_opt = req
            .end_sequence_number
            .as_deref()
            .and_then(|s| s.parse::<u64>().ok());

        let include_full_data = req.include_full_data; // Added

        if let Some(end_val) = end_seq_num_opt {
            if end_val < start_seq_num {
                return Err(Status::invalid_argument(
                    "end_sequence_number cannot be less than start_sequence_number",
                ));
            }
        }

        let (tx, rx) = mpsc::channel(16);
        let state_reader_clone = self.state_reader.clone();

        tokio::spawn(async move {
            let mut current_seq = start_seq_num;

            loop {
                if let Some(end_val) = end_seq_num_opt {
                    if current_seq > end_val {
                        println!(
                            "[gRPC Stream] Reached end_sequence_number {} for range starting at {}. Stream finished.",
                            end_val, start_seq_num
                        );
                        break;
                    }
                }

                let checkpoint_result: Result<StreamedCheckpoint, Status> = if include_full_data {
                    match state_reader_clone.get_checkpoint_by_sequence_number(current_seq) {
                        Ok(Some(verified_checkpoint)) => {
                            match state_reader_clone
                                .get_checkpoint_contents_by_sequence_number(current_seq)
                            {
                                Ok(Some(checkpoint_contents)) => {
                                    match state_reader_clone.get_checkpoint_data(verified_checkpoint, checkpoint_contents) {
                                        Ok(core_checkpoint_data) => {
                                            match convert_full_checkpoint_data_to_gprc(&core_checkpoint_data) {
                                                Ok(gprc_data) => Ok(StreamedCheckpoint {
                                                    checkpoint_type: Some(crate::proto::iota::gprc::v1::streamed_checkpoint::CheckpointType::FullData(gprc_data)),
                                                }),
                                                Err(e) => {
                                                    eprintln!("[gRPC Stream] Failed to convert full checkpoint data {}: {:?}. Terminating.", current_seq, e);
                                                    Err(Status::internal(format!("Conversion error for full checkpoint {}: {}", current_seq, e)))
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            eprintln!("[gRPC Stream] Failed to construct CheckpointData for {}: {:?}. Terminating.", current_seq, e);
                                            Err(Status::internal(format!("Data construction error for checkpoint {}: {}", current_seq, e)))
                                        }
                                    }
                                }
                                Ok(None) => {
                                    eprintln!(
                                        "[gRPC Stream] Checkpoint contents for seq {} not found. Terminating.",
                                        current_seq
                                    );
                                    Err(Status::not_found(format!(
                                        "Checkpoint contents for {} not found",
                                        current_seq
                                    )))
                                }
                                Err(e) => {
                                    eprintln!(
                                        "[gRPC Stream] Storage error fetching contents for {}: {:?}. Terminating.",
                                        current_seq, e
                                    );
                                    Err(Status::internal(format!(
                                        "Storage error for checkpoint contents {}: {}",
                                        current_seq, e
                                    )))
                                }
                            }
                        }
                        Ok(None) => {
                            println!(
                                "[gRPC Stream] No checkpoint found for sequence number {} (or end of available data). Stream finished for range starting at {}.",
                                current_seq, start_seq_num
                            );
                            break; // Break from loop to finish stream
                        }
                        Err(storage_err) => {
                            eprintln!(
                                "[gRPC Stream] Storage error fetching checkpoint summary {}: {:?}. Terminating.",
                                current_seq, storage_err
                            );
                            Err(Status::internal(format!(
                                "Storage error for checkpoint {}: {}",
                                current_seq, storage_err
                            )))
                        }
                    }
                } else {
                    // Only summary
                    match state_reader_clone.get_checkpoint_by_sequence_number(current_seq) {
                        Ok(Some(verified_checkpoint)) => {
                            match convert_verified_checkpoint_to_gprc_summary(&verified_checkpoint) {
                                Ok(summary_gprc) => Ok(StreamedCheckpoint {
                                    checkpoint_type: Some(
                                        crate::proto::iota::gprc::v1::streamed_checkpoint::CheckpointType::Summary(
                                            summary_gprc,
                                        ),
                                    ),
                                }),
                                Err(e) => {
                                    eprintln!("[gRPC Stream] Failed to convert checkpoint summary {}: {:?}. Terminating.", current_seq, e);
                                    Err(Status::internal(format!("Conversion error for checkpoint summary {}: {}", current_seq, e)))
                                }
                            }
                        }
                        Ok(None) => {
                            println!(
                                "[gRPC Stream] No checkpoint found for sequence number {} (or end of available data). Stream finished for range starting at {}.",
                                current_seq, start_seq_num
                            );
                            break; // Break from loop to finish stream
                        }
                        Err(storage_err) => {
                            eprintln!("[gRPC Stream] Storage error fetching checkpoint summary {}: {:?}. Terminating.", current_seq, storage_err);
                            Err(Status::internal(format!("Storage error for checkpoint {}: {}", current_seq, storage_err)))
                        }
                    }
                };

                match checkpoint_result {
                    Ok(streamed_item) => {
                        if tx.send(Ok(streamed_item)).await.is_err() {
                            println!(
                                "[gRPC Stream] Client disconnected. Terminating stream for range starting at {}.",
                                start_seq_num
                            );
                            break;
                        }
                    }
                    Err(status) => {
                        // If Ok(None) was handled by 'break', this path is for actual errors.
                        if tx.send(Err(status)).await.is_err() {
                            println!(
                                "[gRPC Stream] Client disconnected while sending error. Terminating stream for range starting at {}.",
                                start_seq_num
                            );
                        }
                        break; // Terminate stream on error
                    }
                }

                if current_seq == u64::MAX {
                    println!(
                        "[gRPC Stream] Reached u64::MAX. Stream finished for range starting at {}.",
                        start_seq_num
                    );
                    break;
                }
                current_seq += 1;
            }
            drop(tx);
            println!(
                "[gRPC Stream] Producer task finished for range starting at {}, processed up to seq {}.",
                start_seq_num,
                current_seq.saturating_sub(1)
            );
        });

        Ok(Response::new(ReceiverStream::new(rx)))
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
            .start_from_sequence_number
            .as_deref()
            .and_then(|s| s.parse::<u64>().ok());

        let mut broadcast_rx = self.checkpoint_event_sender.subscribe();
        let (tx_to_client, rx_from_task) = mpsc::channel(16); // Channel to client stream

        let state_reader_clone_for_full_data = self.state_reader.clone();

        tokio::spawn(async move {
            // Determine effective start sequence number. If client requested a start,
            // only send checkpoints >= that. If not, send all new ones.
            let mut next_expected_seq_num = requested_start_seq_opt.unwrap_or(0);
            let mut initial_catch_up_done = requested_start_seq_opt.is_none();

            println!(
                "[gRPC Subscribe Task] Started for client. Requested start_seq: {:?}, include_full_data: {}. Effective next_expected: {}",
                requested_start_seq_opt, include_full_data, next_expected_seq_num
            );

            loop {
                match broadcast_rx.recv().await {
                    Ok(verified_checkpoint_arc) => {
                        let checkpoint_seq_num_ref = verified_checkpoint_arc.sequence_number();

                        if !initial_catch_up_done {
                            if *checkpoint_seq_num_ref < next_expected_seq_num {
                                continue;
                            }
                            initial_catch_up_done = true;
                            next_expected_seq_num = *checkpoint_seq_num_ref;
                        }

                        if *checkpoint_seq_num_ref < next_expected_seq_num {
                            continue;
                        }

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
                            Err(_status) => {}
                        }
                        next_expected_seq_num = *checkpoint_seq_num_ref + 1;
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
