use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::{
    proto::iota::gprc::v1::{
        GetSystemInfoRequest, SubscribeSystemEventsRequest, SystemEventGprc, SystemInfoGprc,
        system_gprc_service_server::SystemGprcService,
    },
    server::StateReader,
};

#[derive(Clone)]
pub struct SystemServiceImpl {
    #[allow(dead_code)]
    state_reader: StateReader,
}

impl SystemServiceImpl {
    pub fn new(state_reader: StateReader) -> Self {
        Self { state_reader }
    }
}

#[tonic::async_trait]
impl SystemGprcService for SystemServiceImpl {
    async fn get_system_info(
        &self,
        request: Request<GetSystemInfoRequest>,
    ) -> Result<Response<SystemInfoGprc>, Status> {
        println!(
            "[gRPC SystemService] Received GetSystemInfo request: {:?}",
            request.get_ref()
        );
        Err(Status::unimplemented("GetSystemInfo not implemented"))
    }

    type SubscribeSystemEventsStream = ReceiverStream<Result<SystemEventGprc, Status>>;

    async fn subscribe_system_events(
        &self,
        request: Request<SubscribeSystemEventsRequest>,
    ) -> Result<Response<Self::SubscribeSystemEventsStream>, Status> {
        println!(
            "[gRPC SystemService] Received SubscribeSystemEvents request: {:?}",
            request.get_ref()
        );
        Err(Status::unimplemented(
            "SubscribeSystemEvents not implemented",
        ))
    }
}
