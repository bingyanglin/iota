use std::{pin::Pin, time::Duration};

// use std::sync::mpsc; // Unused, replaced by tokio_mpsc
use tokio::sync::mpsc as tokio_mpsc;
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::{Request, Response, Status};

use crate::{
    conversions::committee::convert_core_committee_to_gprc,
    proto::iota::gprc::v1::{
        CommitteeGprc, GetCommitteeRequest, StreamCommitteeRequest,
        committee_gprc_service_server::CommitteeGprcService,
    },
    server::StateReader, // Assuming StateReader will be used
};

#[derive(Clone)]
pub struct CommitteeServiceImpl {
    #[allow(dead_code)] // Remove when state_reader is used
    state_reader: StateReader,
}

impl CommitteeServiceImpl {
    pub fn new(state_reader: StateReader) -> Self {
        Self { state_reader }
    }
}

#[tonic::async_trait]
impl CommitteeGprcService for CommitteeServiceImpl {
    async fn get_committee(
        &self,
        request: Request<GetCommitteeRequest>,
    ) -> Result<Response<CommitteeGprc>, Status> {
        let epoch_id_gprc_opt = request.into_inner().epoch_id;

        if epoch_id_gprc_opt.is_none() {
            return Err(Status::invalid_argument(
                "EpochIdGprc must be provided in GetCommitteeRequest.",
            ));
        }
        let epoch_id_val = epoch_id_gprc_opt.unwrap().epoch;

        println!(
            "[gRPC CommitteeService] Received GetCommittee request for epoch: {}",
            epoch_id_val
        );

        match self.state_reader.get_committee(epoch_id_val) {
            Ok(Some(core_committee_arc)) => {
                match convert_core_committee_to_gprc(&core_committee_arc) {
                    Ok(gprc_committee) => Ok(Response::new(gprc_committee)),
                    Err(conv_err) => {
                        eprintln!("Conversion error: {}", conv_err);
                        Err(Status::internal(format!(
                            "Failed to convert committee data: {}",
                            conv_err
                        )))
                    }
                }
            }
            Ok(None) => Err(Status::not_found(format!(
                "Committee not found for epoch {}",
                epoch_id_val
            ))),
            Err(storage_err) => {
                eprintln!("Storage error: {}", storage_err);
                Err(Status::internal(format!(
                    "Failed to retrieve committee: {}",
                    storage_err
                )))
            }
        }
    }

    type StreamCommitteeStream =
        Pin<Box<dyn Stream<Item = Result<CommitteeGprc, Status>> + Send + Sync + 'static>>;

    async fn stream_committee(
        &self,
        request: Request<StreamCommitteeRequest>,
    ) -> Result<Response<Self::StreamCommitteeStream>, Status> {
        let req_inner = request.into_inner();
        let start_epoch_gprc_opt = req_inner.start_epoch;

        println!(
            "[gRPC CommitteeService] Received StreamCommittee request: start_epoch_gprc_opt={:?}",
            start_epoch_gprc_opt
        );

        let initial_epoch = match start_epoch_gprc_opt {
            Some(epoch_gprc) => epoch_gprc.epoch,
            None => {
                // If no start_epoch, try to get the current latest known epoch from
                // state_reader and start streaming from the *next* one. This
                // requires a method like get_latest_epoch_id. For now, let's
                // assume if None, we start from epoch 0 or 1 for simplicity, or
                // require it. Or, let's try to get latest and add 1.
                // Assuming state_reader has get_latest_epoch_id() -> Result<EpochId>
                // This method exists on ReadStore trait.
                match self.state_reader.get_latest_epoch_id() {
                    Ok(latest_epoch) => latest_epoch + 1, // Start from next epoch
                    Err(e) => {
                        eprintln!(
                            "[StreamCommittee] Error getting latest epoch: {:?}. Defaulting to epoch 0.",
                            e
                        );
                        0 // Default to epoch 0 or handle error appropriately
                    }
                }
            }
        };

        let state_reader_clone = self.state_reader.clone();
        let (tx, rx) = tokio_mpsc::channel(4); // Buffer size for the stream

        tokio::spawn(async move {
            let mut current_epoch_to_check = initial_epoch;
            let mut last_sent_committee_epoch: Option<u64> = None;

            loop {
                match state_reader_clone.get_committee(current_epoch_to_check) {
                    Ok(Some(core_committee_arc)) => {
                        if last_sent_committee_epoch != Some(core_committee_arc.epoch()) {
                            match convert_core_committee_to_gprc(&core_committee_arc) {
                                Ok(gprc_committee) => {
                                    println!(
                                        "[StreamCommittee] Sending committee for epoch: {}",
                                        core_committee_arc.epoch()
                                    );
                                    if tx.send(Ok(gprc_committee)).await.is_err() {
                                        eprintln!(
                                            "[StreamCommittee] Client disconnected while sending epoch {}.",
                                            core_committee_arc.epoch()
                                        );
                                        break; // Client disconnected
                                    }
                                    last_sent_committee_epoch = Some(core_committee_arc.epoch());
                                    current_epoch_to_check = core_committee_arc.epoch() + 1; // Move to next epoch
                                }
                                Err(conv_err) => {
                                    eprintln!(
                                        "[StreamCommittee] Failed to convert committee for epoch {}: {}. Skipping.",
                                        current_epoch_to_check, conv_err
                                    );
                                    // Optionally send an error to client:
                                    // tx.send(Err(Status::internal("Conversion error"))).await;
                                    current_epoch_to_check += 1; // Try next epoch
                                }
                            }
                        } else {
                            // Committee for this epoch already sent or not yet changed, wait and
                            // poll for next
                            current_epoch_to_check += 1;
                        }
                    }
                    Ok(None) => {
                        // No committee found for current_epoch_to_check, wait
                        // and retry this epoch
                        // This means we are waiting for this epoch to be
                        // formed.
                        // println!("[StreamCommittee] No committee yet for
                        // epoch {}. Waiting...", current_epoch_to_check);
                    }
                    Err(storage_err) => {
                        eprintln!(
                            "[StreamCommittee] Error fetching committee for epoch {}: {}. Stopping stream.",
                            current_epoch_to_check, storage_err
                        );
                        if tx
                            .send(Err(Status::internal(format!(
                                "Storage error: {}",
                                storage_err
                            ))))
                            .await
                            .is_err()
                        {
                            eprintln!(
                                "[StreamCommittee] Client disconnected while sending storage error."
                            );
                        }
                        break; // Terminate on storage error
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await; // Polling interval
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}
