use tonic::{Request, Response, Status};

use crate::{
    conversions::epochs::convert_core_committee_to_epoch_info_gprc,
    proto::iota::gprc::v1::{
        EpochInfoGprc, GetEpochInfoRequest, ListEpochsRequest, ListEpochsResponse,
        epochs_gprc_service_server::EpochsGprcService,
    },
    server::StateReader,
};

#[derive(Clone)]
pub struct EpochsServiceImpl {
    #[allow(dead_code)]
    state_reader: StateReader,
}

impl EpochsServiceImpl {
    pub fn new(state_reader: StateReader) -> Self {
        Self { state_reader }
    }
}

#[tonic::async_trait]
impl EpochsGprcService for EpochsServiceImpl {
    async fn get_epoch_info(
        &self,
        request: Request<GetEpochInfoRequest>,
    ) -> Result<Response<EpochInfoGprc>, Status> {
        println!(
            "[gRPC EpochsService] Received GetEpochInfo request: {:?}",
            request.get_ref()
        );

        let req_inner = request.into_inner();
        let epoch_id_to_fetch = match req_inner.epoch_id {
            Some(eid_gprc) => eid_gprc.epoch,
            None => {
                // Fetch latest epoch ID if not specified
                match self.state_reader.get_latest_epoch_id() {
                    Ok(latest_epoch_id) => latest_epoch_id,
                    Err(e) => {
                        eprintln!(
                            "[gRPC EpochsService] Error fetching latest epoch_id: {:?}",
                            e
                        );
                        return Err(Status::internal(format!(
                            "Failed to fetch latest epoch ID: {}",
                            e
                        )));
                    }
                }
            }
        };

        match self.state_reader.get_committee(epoch_id_to_fetch) {
            Ok(Some(committee_arc)) => {
                let committee = &*committee_arc; // Deref Arc<Committee> to &Committee
                match convert_core_committee_to_epoch_info_gprc(committee) {
                    Ok(epoch_info_gprc) => Ok(Response::new(epoch_info_gprc)),
                    Err(conv_err) => {
                        eprintln!(
                            "[gRPC EpochsService] Conversion error for epoch {}: {:?}",
                            epoch_id_to_fetch, conv_err
                        );
                        Err(Status::internal(format!(
                            "Failed to convert epoch data: {}",
                            conv_err
                        )))
                    }
                }
            }
            Ok(None) => Err(Status::not_found(format!(
                "Committee not found for epoch: {}",
                epoch_id_to_fetch
            ))),
            Err(storage_err) => {
                eprintln!(
                    "[gRPC EpochsService] Storage error fetching committee for epoch {}: {:?}",
                    epoch_id_to_fetch, storage_err
                );
                Err(Status::internal(format!(
                    "Storage error fetching committee data: {}",
                    storage_err
                )))
            }
        }
    }

    async fn list_epochs(
        &self,
        request: Request<ListEpochsRequest>,
    ) -> Result<Response<ListEpochsResponse>, Status> {
        println!(
            "[gRPC EpochsService] Received ListEpochs request: {:?}",
            request.get_ref()
        );
        // TODO: Implementing a full ListEpochs RPC requires a method on StateReader
        // or an underlying storage mechanism to iterate through epochs.
        // For this PoC, we return an empty list.
        Ok(Response::new(ListEpochsResponse {
            epochs: Vec::new(),
            next_cursor: None,
        }))
    }
}
