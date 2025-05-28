use tonic::{Request, Response, Status};

use crate::{
    conversions::committee::convert_core_committee_to_gprc,
    proto::iota::gprc::v1::{
        CommitteeGprc, GetCommitteeRequest, committee_gprc_service_server::CommitteeGprcService,
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
}
