use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::{
    proto::iota::gprc::v1::{
        CoinEventGprc, CoinInfoGprc, GetCoinInfoRequest, ListCoinsRequest, ListCoinsResponse,
        SubscribeCoinEventsRequest, coins_gprc_service_server::CoinsGprcService,
    },
    server::StateReader,
};

#[derive(Clone)]
pub struct CoinsServiceImpl {
    #[allow(dead_code)]
    state_reader: StateReader,
}

impl CoinsServiceImpl {
    pub fn new(state_reader: StateReader) -> Self {
        Self { state_reader }
    }
}

#[tonic::async_trait]
impl CoinsGprcService for CoinsServiceImpl {
    async fn get_coin_info(
        &self,
        request: Request<GetCoinInfoRequest>,
    ) -> Result<Response<CoinInfoGprc>, Status> {
        println!(
            "[gRPC CoinsService] Received GetCoinInfo request: {:?}",
            request.get_ref()
        );
        Err(Status::unimplemented("GetCoinInfo not implemented"))
    }

    async fn list_coins(
        &self,
        request: Request<ListCoinsRequest>,
    ) -> Result<Response<ListCoinsResponse>, Status> {
        println!(
            "[gRPC CoinsService] Received ListCoins request: {:?}",
            request.get_ref()
        );
        Err(Status::unimplemented("ListCoins not implemented"))
    }

    type SubscribeCoinEventsStream = ReceiverStream<Result<CoinEventGprc, Status>>;

    async fn subscribe_coin_events(
        &self,
        request: Request<SubscribeCoinEventsRequest>,
    ) -> Result<Response<Self::SubscribeCoinEventsStream>, Status> {
        println!(
            "[gRPC CoinsService] Received SubscribeCoinEvents request: {:?}",
            request.get_ref()
        );
        Err(Status::unimplemented("SubscribeCoinEvents not implemented"))
    }
}
