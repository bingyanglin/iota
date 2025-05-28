use tonic::{Request, Response, Status};

use crate::{
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
        println!(
            "[gRPC CommitteeService] Received GetCommittee request: {:?}",
            request.get_ref()
        );
        Err(Status::unimplemented("GetCommittee not implemented"))
    }

    type StreamCommitteeStream =
        tokio_stream::wrappers::ReceiverStream<Result<CommitteeGprc, Status>>;

    async fn stream_committee(
        &self,
        request: Request<StreamCommitteeRequest>,
    ) -> Result<Response<Self::StreamCommitteeStream>, Status> {
        println!(
            "[gRPC CommitteeService] Received StreamCommittee request: {:?}",
            request.get_ref()
        );
        Err(Status::unimplemented("StreamCommittee not implemented"))
    }
}
