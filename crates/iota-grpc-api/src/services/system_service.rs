use std::{sync::Arc, time::Instant};

use tonic::{Request, Response, Status};

use crate::{
    proto::iota::gprc::v1::{
        GetSystemInfoRequest, StringU64, SystemInfoGprc,
        system_gprc_service_server::SystemGprcService,
    },
    server::StateReader,
};

// const NODE_VERSION: &str = "iota-grpc-api-dev"; // Replaced by
// CARGO_PKG_VERSION

#[derive(Clone)]
pub struct SystemServiceImpl {
    #[allow(dead_code)]
    state_reader: StateReader,
    start_time: Arc<Instant>,
}

impl SystemServiceImpl {
    pub fn new(state_reader: StateReader, app_start_time: Arc<Instant>) -> Self {
        Self {
            state_reader,
            start_time: app_start_time,
        }
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

        let uptime_duration = self.start_time.elapsed();
        let uptime_ms = uptime_duration.as_millis();

        let system_info = SystemInfoGprc {
            node_version: env!("CARGO_PKG_VERSION").to_string(),
            uptime_ms: Some(StringU64 {
                value: uptime_ms.to_string(),
            }),
        };

        Ok(Response::new(system_info))
    }
}
