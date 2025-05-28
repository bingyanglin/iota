use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::{
    proto::iota::gprc::v1::{
        AccountChangeEventGprc, AccountInfoGprc, GetAccountInfoRequest, ListAccountObjectsRequest,
        ListAccountObjectsResponse, SubscribeAccountChangesRequest,
        accounts_gprc_service_server::AccountsGprcService,
    },
    server::StateReader,
};

#[derive(Clone)]
pub struct AccountsServiceImpl {
    #[allow(dead_code)]
    state_reader: StateReader,
}

impl AccountsServiceImpl {
    pub fn new(state_reader: StateReader) -> Self {
        Self { state_reader }
    }
}

#[tonic::async_trait]
impl AccountsGprcService for AccountsServiceImpl {
    async fn get_account_info(
        &self,
        request: Request<GetAccountInfoRequest>,
    ) -> Result<Response<AccountInfoGprc>, Status> {
        println!(
            "[gRPC AccountsService] Received GetAccountInfo request: {:?}",
            request.get_ref()
        );
        Err(Status::unimplemented("GetAccountInfo not implemented"))
    }

    async fn list_account_objects(
        &self,
        request: Request<ListAccountObjectsRequest>,
    ) -> Result<Response<ListAccountObjectsResponse>, Status> {
        println!(
            "[gRPC AccountsService] Received ListAccountObjects request: {:?}",
            request.get_ref()
        );
        Err(Status::unimplemented("ListAccountObjects not implemented"))
    }

    type SubscribeAccountChangesStream = ReceiverStream<Result<AccountChangeEventGprc, Status>>;

    async fn subscribe_account_changes(
        &self,
        request: Request<SubscribeAccountChangesRequest>,
    ) -> Result<Response<Self::SubscribeAccountChangesStream>, Status> {
        println!(
            "[gRPC AccountsService] Received SubscribeAccountChanges request: {:?}",
            request.get_ref()
        );
        Err(Status::unimplemented(
            "SubscribeAccountChanges not implemented",
        ))
    }
}
