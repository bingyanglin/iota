// Placeholder for gRPC server setup and run logic

// ... import other service implementations ...
use std::{net::SocketAddr, sync::Arc, time::Instant};

// IOTA-specific imports
use iota_types::storage::RestStateReader; // Import the actual RestStateReader
use tokio::sync::broadcast;
use tonic::transport::Server;

// ... import other service servers as they are created ...
use crate::services::checkpoints_service::CheckpointServiceImpl;
use crate::{
    proto::iota::gprc::v1::{
        accounts_gprc_service_server::AccountsGprcServiceServer,
        checkpoint_gprc_service_server::CheckpointGprcServiceServer,
        coins_gprc_service_server::CoinsGprcServiceServer,
        committee_gprc_service_server::CommitteeGprcServiceServer,
        epochs_gprc_service_server::EpochsGprcServiceServer,
        info_gprc_service_server::InfoGprcServiceServer,
        object_gprc_service_server::ObjectGprcServiceServer,
        system_gprc_service_server::SystemGprcServiceServer,
        transaction_gprc_service_server::TransactionGprcServiceServer,
    },
    services::{
        accounts_service::AccountsServiceImpl, coins_service::CoinsServiceImpl,
        committee_service::CommitteeServiceImpl, epochs_service::EpochsServiceImpl,
        info_service::InfoServiceImpl, objects_service::ObjectServiceImpl,
        system_service::SystemServiceImpl, transactions_service::TransactionServiceImpl,
    },
};

// Define a placeholder trait and a concrete type for StateReader within this
// module In a real application, this would likely be imported from another
// crate (e.g., iota_storage or a core types crate) and the GrpcServer would be
// generic or take a trait object.
// #[async_trait::async_trait]
// pub trait RestStateReaderExt: Send + Sync + 'static {
// async fn get_some_data(&self) -> String; // Example method
// Add other methods that your gRPC services will need to read state
// }

// Dummy implementation of the StateReader trait for example purposes
// pub struct DummyStateReader;
// #[async_trait::async_trait]
// impl RestStateReaderExt for DummyStateReader {
// async fn get_some_data(&self) -> String {
// "data_from_dummy_reader".to_string()
// }
// }

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
    object_service: ObjectServiceImpl,
    transaction_service: TransactionServiceImpl,
    committee_service: CommitteeServiceImpl,
    system_service: SystemServiceImpl,
    coins_service: CoinsServiceImpl,
    epochs_service: EpochsServiceImpl,
    accounts_service: AccountsServiceImpl,
    info_service: InfoServiceImpl,
    #[allow(dead_code)] // app_start_time might not be used directly by GrpcServer itself yet
    app_start_time: Arc<Instant>,
}

impl GrpcServer {
    pub fn new(addr: SocketAddr, state_reader: StateReader) -> Self {
        let app_start_time = Arc::new(Instant::now());

        let checkpoint_service = CheckpointServiceImpl::new(state_reader.clone());
        let object_service = ObjectServiceImpl::new(state_reader.clone());
        let transaction_service = TransactionServiceImpl::new(state_reader.clone());
        let committee_service = CommitteeServiceImpl::new(state_reader.clone());
        let system_service = SystemServiceImpl::new(state_reader.clone(), app_start_time.clone());
        let coins_service = CoinsServiceImpl::new(state_reader.clone());
        let epochs_service = EpochsServiceImpl::new(state_reader.clone());
        let accounts_service = AccountsServiceImpl::new(state_reader.clone());
        let info_service = InfoServiceImpl::new(state_reader.clone());
        Self {
            addr,
            checkpoint_service,
            object_service,
            transaction_service,
            committee_service,
            system_service,
            coins_service,
            epochs_service,
            accounts_service,
            info_service,
            app_start_time,
        }
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
            .add_service(ObjectGprcServiceServer::new(self.object_service.clone()))
            .add_service(TransactionGprcServiceServer::new(
                self.transaction_service.clone(),
            ))
            .add_service(CommitteeGprcServiceServer::new(
                self.committee_service.clone(),
            ))
            .add_service(SystemGprcServiceServer::new(self.system_service.clone()))
            .add_service(CoinsGprcServiceServer::new(self.coins_service.clone()))
            .add_service(EpochsGprcServiceServer::new(self.epochs_service.clone()))
            .add_service(AccountsGprcServiceServer::new(
                self.accounts_service.clone(),
            ))
            .add_service(InfoGprcServiceServer::new(self.info_service.clone()))
            .serve_with_shutdown(self.addr, async move {
                shutdown_rx.recv().await.ok();
                println!("[gRPC] Server shutting down gracefully.");
            })
            .await?;
        Ok(())
    }
}
