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

pub mod checkpoint {
    tonic::include_proto!("iota.grpc");
}

use checkpoint::{BcsData, checkpoint_service_server::CheckpointService};
use iota_types::storage::RestStateReader;
pub mod client;
use iota_types::{
    full_checkpoint_content::CheckpointData,
    grpc::{
        CertifiedCheckpointSummary as GrpcCertifiedCheckpointSummary,
        CheckpointData as GrpcCheckpointData,
    },
    messages_checkpoint::CertifiedCheckpointSummary,
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
pub struct CheckpointGrpcService {
    pub state_reader: Arc<dyn RestStateReader>,
    pub grpc_checkpoint_summary_tx:
        tokio::sync::broadcast::Sender<Arc<GrpcCertifiedCheckpointSummary>>,
    pub grpc_checkpoint_data_tx: tokio::sync::broadcast::Sender<Arc<GrpcCheckpointData>>,
}

impl CheckpointGrpcService {
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
// Note, checkpoint::Checkpoint may contain either checkpoint data or summary.
type CheckpointStreamResult = Result<checkpoint::Checkpoint, Status>;

// Helper trait for getting checkpoint data and summaries,
// intended as an abstractoin for Arc<dyn RestStateReader>.
trait CheckpointOracle<T>
where
    T: Send + Sync + 'static + Serialize,
    Self: Send + Sync + 'static,
{
    fn get_index(&self, item: &Arc<T>) -> u64;
    fn get_item(&self, ix: u64) -> Option<Arc<T>>;
    fn get_latest(&self) -> Option<u64>;

    fn create_checkpoint_response(&self, item: &Arc<T>, is_full: bool) -> CheckpointStreamResult {
        BcsData::serialize_from(item)
            .map(|data| checkpoint::Checkpoint {
                index: self.get_index(item),
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
    let summary = state_reader.get_checkpoint_by_sequence_number(seq).ok()??;
    let contents = state_reader
        .get_checkpoint_contents_by_sequence_number(seq)
        .ok()??;
    state_reader.get_checkpoint_data(summary, contents).ok()
}

impl CheckpointOracle<GrpcCheckpointData> for Oracle {
    fn get_index(&self, item: &Arc<GrpcCheckpointData>) -> u64 {
        item.sequence_number()
    }
    fn get_item(&self, ix: u64) -> Option<Arc<GrpcCheckpointData>> {
        get_full_checkpoint_data(&self.state_reader, ix)
            .map(GrpcCheckpointData::from)
            .map(Arc::new)
    }
    fn get_latest(&self) -> Option<u64> {
        self.state_reader
            .get_latest_checkpoint()
            .ok()
            .map(|cp| *cp.sequence_number())
    }
}

impl CheckpointOracle<GrpcCertifiedCheckpointSummary> for Oracle {
    fn get_index(&self, item: &Arc<GrpcCertifiedCheckpointSummary>) -> u64 {
        item.sequence_number()
    }
    fn get_item(&self, ix: u64) -> Option<Arc<GrpcCertifiedCheckpointSummary>> {
        self.state_reader
            .get_checkpoint_by_sequence_number(ix)
            .ok()
            .flatten()
            .map(|v| GrpcCertifiedCheckpointSummary::from(CertifiedCheckpointSummary::from(v)))
            .map(Arc::new)
    }
    fn get_latest(&self) -> Option<u64> {
        self.state_reader
            .get_latest_checkpoint()
            .ok()
            .map(|cp| *cp.sequence_number())
    }
}

fn create_checkpoint_stream<T, F>(
    oracle: F,
    mut rx: Receiver<Arc<T>>,
    start: Option<u64>,
    end: Option<u64>,
    is_full: bool,
) -> impl futures::Stream<Item = CheckpointStreamResult> + Send
where
    T: Send + Sync + 'static + Serialize,
    F: CheckpointOracle<T> + Clone + Send + Sync + 'static,
{
    async_stream::try_stream! {
        // TODO: Modify the latest checkpoint to start from 1.
        // Note that we do not stream the Genesis checkpoint because its size
        // can be very big. The genesis checkpoint should be imported directly.
        let mut latest = oracle.get_latest().unwrap_or(0);
        debug!("[profile][grpc] Latest checkpoint index: {latest}.");
        let (mut start, end) = match (start, end) {
            (None, None) => (latest, u64::MAX),
            (None, Some(end)) => (end, end),
            (Some(start), None) => (start, u64::MAX),
            (Some(start), Some(end)) => (start, end),
        };
        let mut cached = None;

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
            if let Some(item) = cached.take() {
                // already have something in cache
                debug!("[profile][grpc] Using cached checkpoint data for index {start}.");
                let idx = oracle.get_index(&item);
                if start == idx {
                    yield oracle.create_checkpoint_response(&item, is_full)?;
                    if start == end {
                        break;
                    }
                    start += 1;
                } else if start < idx {
                    cached = Some(item);
                }
            }
            // wait for broadcast
            match rx.recv().await {
                Ok(item) => {
                    debug!("[profile][grpc] Get checkpoint data for index {} from broadcast channel", oracle.get_index(&item));
                    let idx = oracle.get_index(&item);
                    if start == idx {
                        yield oracle.create_checkpoint_response(&item, is_full)?;
                        if start == end {
                            break;
                        }
                        start += 1;
                        continue;
                    } else if start < idx {
                        // the item is too fresh, need to fill the gap from history DB
                        debug!("[profile][grpc] Gap detected, waiting for historical data for index {start} (latest: {latest}).");
                        cached = Some(item);
                    } // else item is too old, just drop it and continue
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

impl CheckpointGrpcService {
    fn stream_checkpoint_data(
        &self,
        start: Option<u64>,
        end: Option<u64>,
    ) -> impl futures::Stream<Item = CheckpointStreamResult> + Send {
        let state_reader = self.state_reader.clone();
        let oracle = Oracle { state_reader };
        create_checkpoint_stream(
            oracle,
            self.grpc_checkpoint_data_tx.subscribe(),
            start,
            end,
            true,
        )
    }

    fn stream_checkpoint_summary(
        &self,
        start: Option<u64>,
        end: Option<u64>,
    ) -> impl futures::Stream<Item = CheckpointStreamResult> + Send {
        let state_reader = self.state_reader.clone();
        let oracle = Oracle { state_reader };
        create_checkpoint_stream(
            oracle,
            self.grpc_checkpoint_summary_tx.subscribe(),
            start,
            end,
            false,
        )
    }
}

#[tonic::async_trait]
impl CheckpointService for CheckpointGrpcService {
    type StreamCheckpointsStream =
        Pin<Box<dyn futures::Stream<Item = Result<checkpoint::Checkpoint, Status>> + Send>>;

    async fn stream_checkpoints(
        &self,
        request: Request<checkpoint::CheckpointStreamRequest>,
    ) -> Result<Response<Self::StreamCheckpointsStream>, Status> {
        let req = request.into_inner();
        let start = req.start_index;
        let end = req.end_index;
        let full = req.full;

        let stream: Self::StreamCheckpointsStream = if full.unwrap_or(false) {
            Box::pin(self.stream_checkpoint_data(start, end))
        } else {
            Box::pin(self.stream_checkpoint_summary(start, end))
        };
        Ok(Response::new(stream))
    }

    async fn get_epoch_first_checkpoint_sequence_number(
        &self,
        request: Request<checkpoint::EpochRequest>,
    ) -> Result<Response<checkpoint::CheckpointSequenceNumberResponse>, Status> {
        let epoch = request.into_inner().epoch;
        info!(
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

        Ok(Response::new(
            checkpoint::CheckpointSequenceNumberResponse { sequence_number },
        ))
    }
}
