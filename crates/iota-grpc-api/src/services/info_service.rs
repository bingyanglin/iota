use std::time::Instant;

use tonic::{Request, Response, Status};

use crate::{
    proto::iota::gprc::v1::{
        GetInfoRequest, NodeInfoGprc, info_gprc_service_server::InfoGprcService,
    },
    server::StateReader, /* To access node state
                         * error::GrpcApiError, // Might be needed for error conversion */
};

const NODE_VERSION: &str = "0.1.0-dev"; // Placeholder
const MOCK_NODE_ID_HEX: &str = "0xdeadbeef000000000000000000000000000000000000000000000000deadbeef"; // Placeholder

#[derive(Clone)]
pub struct InfoServiceImpl {
    state_reader: StateReader,
    start_time: Instant,
}

impl InfoServiceImpl {
    pub fn new(state_reader: StateReader) -> Self {
        Self {
            state_reader,
            start_time: Instant::now(),
        }
    }
}

#[tonic::async_trait]
impl InfoGprcService for InfoServiceImpl {
    async fn get_info(
        &self,
        _request: Request<GetInfoRequest>,
    ) -> Result<Response<NodeInfoGprc>, Status> {
        println!("[gRPC InfoService] Received GetInfo request");

        let uptime_ms = self.start_time.elapsed().as_millis() as u64;

        let chain_id_core = self.state_reader.get_chain_identifier().map_err(|e| {
            eprintln!("[gRPC InfoService] Error getting chain_identifier: {:?}", e);
            Status::internal(format!("Failed to get chain identifier: {}", e))
        })?;
        let chain_id_hex = chain_id_core.to_string();

        let current_epoch_number = self.state_reader.get_latest_epoch_id().map_err(|e| {
            eprintln!("[gRPC InfoService] Error getting latest_epoch_id: {:?}", e);
            Status::internal(format!("Failed to get latest epoch ID: {}", e))
        })?;

        let latest_checkpoint_sequence_number = self
            .state_reader
            .get_latest_checkpoint_sequence_number()
            .map_err(|e| {
                eprintln!(
                    "[gRPC InfoService] Error getting latest_checkpoint_sequence_number: {:?}",
                    e
                );
                Status::internal(format!(
                    "Failed to get latest checkpoint sequence number: {}",
                    e
                ))
            })?;

        let lowest_available_checkpoint_sequence_number = self
            .state_reader
            .get_lowest_available_checkpoint()
            .map_err(|e| {
                eprintln!(
                    "[gRPC InfoService] Error getting lowest_available_checkpoint: {:?}",
                    e
                );
                Status::internal(format!("Failed to get lowest available checkpoint: {}", e))
            })?;

        let highest_verified_checkpoint_core = self
            .state_reader
            .get_highest_verified_checkpoint()
            .map_err(|e| {
                eprintln!(
                    "[gRPC InfoService] Error getting highest_verified_checkpoint: {:?}",
                    e
                );
                Status::internal(format!("Failed to get highest verified checkpoint: {}", e))
            })?;
        let highest_verified_checkpoint_sequence_number =
            *highest_verified_checkpoint_core.sequence_number();

        let node_info = NodeInfoGprc {
            version: NODE_VERSION.to_string(),
            uptime_ms,
            node_id_hex: MOCK_NODE_ID_HEX.to_string(),
            chain_id_hex,
            current_epoch_number,
            latest_checkpoint_sequence_number,
            lowest_available_checkpoint_sequence_number,
            highest_verified_checkpoint_sequence_number,
        };

        Ok(Response::new(node_info))
    }
}
