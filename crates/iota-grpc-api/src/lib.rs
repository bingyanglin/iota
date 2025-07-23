// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::{pin::Pin, sync::Arc};

use serde::{Deserialize, Serialize};
use tonic::{Request, Response, Status};

/// Configuration for the gRPC API service
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    /// The address to bind the gRPC server to
    #[serde(default = "default_grpc_api_address")]
    pub address: std::net::SocketAddr,

    /// Buffer size for broadcast channels used for checkpoint streaming
    #[serde(default = "default_checkpoint_broadcast_buffer_size")]
    pub checkpoint_broadcast_buffer_size: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            address: default_grpc_api_address(),
            checkpoint_broadcast_buffer_size: default_checkpoint_broadcast_buffer_size(),
        }
    }
}

fn default_grpc_api_address() -> std::net::SocketAddr {
    use std::net::{IpAddr, Ipv4Addr};
    std::net::SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 50051)
}

fn default_checkpoint_broadcast_buffer_size() -> usize {
    100
}

pub mod node {
    tonic::include_proto!("iota.grpc");
}

use iota_types::storage::RestStateReader;
use node::{BcsData, node_service_server::NodeService};
pub mod client;
use iota_grpc_types::{
    CertifiedCheckpointSummary as GrpcCertifiedCheckpointSummary,
    CheckpointData as GrpcCheckpointData,
};
use iota_types::{
    full_checkpoint_content::CheckpointData, messages_checkpoint::CertifiedCheckpointSummary,
};
use tracing::{debug, info};

type Receiver<T> = tokio::sync::broadcast::Receiver<T>;

impl BcsData {
    fn serialize_from<T>(data: &T) -> Result<Self, bcs::Error>
    where
        T: Serialize,
    {
        let serialized = bcs::to_bytes(data)?;
        Ok(BcsData { data: serialized })
    }

    fn deserialize_into<T>(&self) -> Result<T, bcs::Error>
    where
        T: for<'de> Deserialize<'de>,
    {
        bcs::from_bytes(&self.data)
    }
}

pub struct NodeGrpcService {
    pub state_reader: Arc<dyn RestStateReader>,
    pub grpc_checkpoint_summary_tx:
        tokio::sync::broadcast::Sender<Arc<GrpcCertifiedCheckpointSummary>>,
    pub grpc_checkpoint_data_tx: tokio::sync::broadcast::Sender<Arc<GrpcCheckpointData>>,
}

impl NodeGrpcService {
    pub fn new(
        state_reader: Arc<dyn RestStateReader>,
        grpc_checkpoint_summary_tx: tokio::sync::broadcast::Sender<
            Arc<GrpcCertifiedCheckpointSummary>,
        >,
        grpc_checkpoint_data_tx: tokio::sync::broadcast::Sender<Arc<GrpcCheckpointData>>,
    ) -> Self {
        Self {
            state_reader,
            grpc_checkpoint_summary_tx,
            grpc_checkpoint_data_tx,
        }
    }
}

// Checkpoint stream item.
// Note, node::Checkpoint may contain either checkpoint data or summary.
type CheckpointStreamResult = Result<node::Checkpoint, Status>;

// Helper trait for getting checkpoint data and summaries,
// intended as an abstraction for Arc<dyn RestStateReader>.
trait CheckpointReader<T>
where
    T: Send + Sync + 'static + Serialize,
    Self: Send + Sync + 'static,
{
    fn get_sequence_number(&self, item: &Arc<T>) -> u64;
    fn get_item(&self, ix: u64) -> Option<Arc<T>>;
    fn get_latest(&self) -> Option<u64>;

    fn create_checkpoint_response(&self, item: &Arc<T>, is_full: bool) -> CheckpointStreamResult {
        BcsData::serialize_from(item)
            .map(|data| node::Checkpoint {
                sequence_number: self.get_sequence_number(item),
                bcs_data: Some(data),
                is_full,
            })
            .map_err(|e| Status::internal(format!("BCS serialization error: {e}")))
    }
}

#[derive(Clone)]
struct Oracle {
    state_reader: Arc<dyn RestStateReader>,
}

fn get_full_checkpoint_data(
    state_reader: &std::sync::Arc<dyn RestStateReader>,
    seq: u64,
) -> Option<CheckpointData> {
    let summary = state_reader.get_checkpoint_by_sequence_number(seq)?;
    let contents = state_reader.get_checkpoint_contents_by_sequence_number(seq)?;
    Some(state_reader.get_checkpoint_data(summary, contents))
}

impl CheckpointReader<GrpcCheckpointData> for Oracle {
    fn get_sequence_number(&self, item: &Arc<GrpcCheckpointData>) -> u64 {
        item.sequence_number()
    }
    fn get_item(&self, ix: u64) -> Option<Arc<GrpcCheckpointData>> {
        get_full_checkpoint_data(&self.state_reader, ix)
            .map(GrpcCheckpointData::from)
            .map(Arc::new)
    }
    fn get_latest(&self) -> Option<u64> {
        Some(*self.state_reader.get_latest_checkpoint().sequence_number())
    }
}

impl CheckpointReader<GrpcCertifiedCheckpointSummary> for Oracle {
    fn get_sequence_number(&self, item: &Arc<GrpcCertifiedCheckpointSummary>) -> u64 {
        item.sequence_number()
    }
    fn get_item(&self, ix: u64) -> Option<Arc<GrpcCertifiedCheckpointSummary>> {
        self.state_reader
            .get_checkpoint_by_sequence_number(ix)
            .map(|v| GrpcCertifiedCheckpointSummary::from(CertifiedCheckpointSummary::from(v)))
            .map(Arc::new)
    }
    fn get_latest(&self) -> Option<u64> {
        Some(*self.state_reader.get_latest_checkpoint().sequence_number())
    }
}

fn create_checkpoint_stream<T, F>(
    oracle: F,
    mut rx: Receiver<Arc<T>>,
    start_sequence_number: Option<u64>,
    end_sequence_number: Option<u64>,
    is_full: bool,
) -> impl futures::Stream<Item = CheckpointStreamResult> + Send
where
    T: Send + Sync + 'static + Serialize,
    F: CheckpointReader<T> + Clone + Send + Sync + 'static,
{
    async_stream::try_stream! {
        // Link to issue (https://github.com/iotaledger/iota/issues/7943)
        // TODO: Modify the latest checkpoint to start from 1.
        // Note that we do not stream the Genesis checkpoint because its size
        // can be very big. The genesis checkpoint should be imported directly.
        let mut latest = oracle.get_latest().unwrap_or(0);
        debug!("[profile][grpc] Latest checkpoint index: {latest}.");
        let (mut start, end) = match (start_sequence_number, end_sequence_number) {
            (None, None) => (latest, u64::MAX),
            (None, Some(end)) => (end, end),
            (Some(start), None) => (start, u64::MAX),
            (Some(start), Some(end)) => (start, end),
        };
        while start <= end {
            // try fetching historical data from the DB first
            if start <= latest {
                if let Some(item) = oracle.get_item(start) {
                    // TODO: add backfill tracing messages
                    debug!("[profile][grpc] Fetched checkpoint data for index {start} from DB.");
                    yield oracle.create_checkpoint_response(&item, is_full)?;
                    if start == end {
                        break;
                    }
                    start += 1;
                    continue;
                } else {
                    Err(Status::internal(format!("Historical checkpoint data missing/pruned: index={start} latest={latest}.")))?;
                }
            }
            // latest < start, live phase
            // wait for broadcast
            match rx.recv().await {
                Ok(item) => {
                    debug!("[profile][grpc] Get checkpoint data for index {} from broadcast channel", oracle.get_sequence_number(&item));
                    let seq_number = oracle.get_sequence_number(&item);
                    if start == seq_number {
                        yield oracle.create_checkpoint_response(&item, is_full)?;
                        if start == end {
                            break;
                        }
                        start += 1;
                        continue;
                    }
                    // else item sequence doesn't match, drop it and continue
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    // continue, lagged item should be picked up from history DB
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    // report internal error to the stream and break
                    Err(Status::internal("Checkpoint data channel closed."))?;
                    break;
                },
            }
            latest = oracle.get_latest().unwrap_or(start);
            debug!("[profile][grpc] Updating latest checkpoint index to {latest}.");
        }
    }
}

impl NodeGrpcService {
    fn stream_checkpoint_data(
        &self,
        start_sequence_number: Option<u64>,
        end_sequence_number: Option<u64>,
    ) -> impl futures::Stream<Item = CheckpointStreamResult> + Send {
        let state_reader = self.state_reader.clone();
        let oracle = Oracle { state_reader };
        create_checkpoint_stream(
            oracle,
            self.grpc_checkpoint_data_tx.subscribe(),
            start_sequence_number,
            end_sequence_number,
            true,
        )
    }

    fn stream_checkpoint_summary(
        &self,
        start_sequence_number: Option<u64>,
        end_sequence_number: Option<u64>,
    ) -> impl futures::Stream<Item = CheckpointStreamResult> + Send {
        let state_reader = self.state_reader.clone();
        let oracle = Oracle { state_reader };
        create_checkpoint_stream(
            oracle,
            self.grpc_checkpoint_summary_tx.subscribe(),
            start_sequence_number,
            end_sequence_number,
            false,
        )
    }
}

#[tonic::async_trait]
impl NodeService for NodeGrpcService {
    type StreamCheckpointsStream =
        Pin<Box<dyn futures::Stream<Item = Result<node::Checkpoint, Status>> + Send>>;

    async fn stream_checkpoints(
        &self,
        request: Request<node::CheckpointStreamRequest>,
    ) -> Result<Response<Self::StreamCheckpointsStream>, Status> {
        let req = request.into_inner();
        let start_sequence_number = req.start_sequence_number;
        let end_sequence_number = req.end_sequence_number;
        let full = req.full;

        let stream: Self::StreamCheckpointsStream = if full.unwrap_or(false) {
            Box::pin(self.stream_checkpoint_data(start_sequence_number, end_sequence_number))
        } else {
            Box::pin(self.stream_checkpoint_summary(start_sequence_number, end_sequence_number))
        };
        Ok(Response::new(stream))
    }

    async fn get_epoch_first_checkpoint_sequence_number(
        &self,
        request: Request<node::EpochRequest>,
    ) -> Result<Response<node::CheckpointSequenceNumberResponse>, Status> {
        let epoch = request.into_inner().epoch;
        debug!(
            "get_epoch_first_checkpoint_sequence_number called for epoch {}",
            epoch
        );

        let sequence_number = if epoch == 0 {
            // Genesis epoch starts at checkpoint 0
            0
        } else {
            // Get the last checkpoint of the previous epoch
            match self.state_reader.get_epoch_last_checkpoint(epoch - 1) {
                Ok(Some(last_checkpoint)) => {
                    // First checkpoint of current epoch is the next one
                    *last_checkpoint.sequence_number() + 1
                }
                Ok(None) => {
                    return Err(Status::not_found(format!(
                        "No checkpoints found for previous epoch {}",
                        epoch - 1
                    )));
                }
                Err(e) => {
                    return Err(Status::internal(format!(
                        "Failed to get last checkpoint for epoch {}: {}",
                        epoch - 1,
                        e
                    )));
                }
            }
        };

        info!(
            "First checkpoint for epoch {}: seq={}",
            epoch, sequence_number
        );

        Ok(Response::new(node::CheckpointSequenceNumberResponse {
            sequence_number,
        }))
    }
}
