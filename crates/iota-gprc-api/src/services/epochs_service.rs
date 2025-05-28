use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::{
    proto::iota::gprc::v1::{
        EpochInfoGprc, GetEpochInfoRequest, ListEpochsRequest, ListEpochsResponse,
        SubscribeNewEpochsRequest, epochs_gprc_service_server::EpochsGprcService,
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
        Err(Status::unimplemented("GetEpochInfo not implemented"))
    }

    async fn list_epochs(
        &self,
        request: Request<ListEpochsRequest>,
    ) -> Result<Response<ListEpochsResponse>, Status> {
        println!(
            "[gRPC EpochsService] Received ListEpochs request: {:?}",
            request.get_ref()
        );
        Err(Status::unimplemented("ListEpochs not implemented"))
    }

    type SubscribeNewEpochsStream = ReceiverStream<Result<EpochInfoGprc, Status>>;

    async fn subscribe_new_epochs(
        &self,
        request: Request<SubscribeNewEpochsRequest>,
    ) -> Result<Response<Self::SubscribeNewEpochsStream>, Status> {
        println!(
            "[gRPC EpochsService] Received SubscribeNewEpochs request: {:?}",
            request.get_ref()
        );
        Err(Status::unimplemented("SubscribeNewEpochs not implemented"))
    }
}
