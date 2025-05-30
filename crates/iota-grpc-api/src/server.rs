// Placeholder for gRPC server setup and run logic

// ... import other service implementations ...
use std::{net::SocketAddr, sync::Arc, time::Instant};

use iota_types::messages_checkpoint::VerifiedCheckpoint;
// IOTA-specific imports
use iota_types::storage::RestStateReader; // Import the actual RestStateReader
use tokio::sync::broadcast;
use tonic::transport::Server;

use crate::{
    proto::iota::gprc::v1::checkpoint_gprc_service_server::CheckpointGprcServiceServer,
    services::checkpoints_service::CheckpointServiceImpl,
};

// Type alias for convenience, using the concrete dummy type for now.
// In a real scenario, this would be Arc<dyn RestStateReaderExt> or a generic
// type. pub type StateReader = Arc<DummyStateReader>;
pub type StateReader = Arc<dyn RestStateReader + Send + Sync + 'static>; // Use the actual RestStateReader trait

// Placeholder for TransactionExecutor, if needed by services
// pub type TransactionExecutor = Arc<dyn some_crate::TransactionExecutorTrait +
// Send + Sync + 'static>;

pub struct GrpcServer {
    addr: SocketAddr,
    checkpoint_service: CheckpointServiceImpl,
    checkpoint_event_sender: broadcast::Sender<Arc<VerifiedCheckpoint>>,
    #[allow(dead_code)] // app_start_time might not be used directly by GrpcServer itself yet
    app_start_time: Arc<Instant>,
}

impl GrpcServer {
    pub fn new(addr: SocketAddr, state_reader: StateReader) -> Self {
        let app_start_time = Arc::new(Instant::now());

        // Create the broadcast channel for checkpoint events
        let (checkpoint_event_tx, _rx) = broadcast::channel::<Arc<VerifiedCheckpoint>>(32_usize);

        let checkpoint_service =
            CheckpointServiceImpl::new(state_reader.clone(), checkpoint_event_tx.clone());
        Self {
            addr,
            checkpoint_service,
            checkpoint_event_sender: checkpoint_event_tx,
            app_start_time,
        }
    }

    pub fn checkpoint_event_sender(&self) -> broadcast::Sender<Arc<VerifiedCheckpoint>> {
        self.checkpoint_event_sender.clone()
    }

    pub async fn start(
        &self,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> Result<(), anyhow::Error> {
        println!("[gRPC] Server starting on {}", self.addr);
        Server::builder()
            .add_service(CheckpointGprcServiceServer::new(
                self.checkpoint_service.clone(),
            ))
            .serve_with_shutdown(self.addr, async move {
                shutdown_rx.recv().await.ok();
                println!("[gRPC] Server shutting down gracefully.");
            })
            .await?;
        Ok(())
    }
}
