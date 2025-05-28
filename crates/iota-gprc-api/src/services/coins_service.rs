use tonic::{Request, Response, Status};

use crate::{
    conversions::coins::convert_storage_coin_info_to_gprc,
    proto::iota::gprc::v1::{
        CoinInfoGprc, GetCoinInfoRequest, ListCoinsRequest, ListCoinsResponse,
        coins_gprc_service_server::CoinsGprcService,
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

        let req_inner = request.into_inner();
        let coin_type_tag_str = req_inner.coin_type_tag;

        if coin_type_tag_str.is_empty() {
            return Err(Status::invalid_argument("coin_type_tag cannot be empty"));
        }

        let coin_type_struct_tag: move_core_types::language_storage::StructTag =
            match coin_type_tag_str.parse() {
                Ok(tag) => tag,
                Err(e) => {
                    return Err(Status::invalid_argument(format!(
                        "Invalid coin_type_tag '{}': {}",
                        coin_type_tag_str, e
                    )));
                }
            };

        match self.state_reader.get_coin_info(&coin_type_struct_tag) {
            Ok(Some(core_storage_info)) => {
                match convert_storage_coin_info_to_gprc(
                    &coin_type_struct_tag,
                    &core_storage_info,
                    &self.state_reader,
                ) {
                    Ok(gprc_coin_info) => Ok(Response::new(gprc_coin_info)),
                    Err(conv_err) => {
                        eprintln!(
                            "[gRPC CoinsService] Conversion error for coin info {}: {:?}",
                            coin_type_tag_str, conv_err
                        );
                        Err(Status::internal(format!(
                            "Failed to convert coin info data: {}",
                            conv_err
                        )))
                    }
                }
            }
            Ok(None) => Err(Status::not_found(format!(
                "Coin info not found for type_tag: {}",
                coin_type_tag_str
            ))),
            Err(storage_err) => {
                eprintln!(
                    "[gRPC CoinsService] Storage error fetching coin info for {}: {:?}",
                    coin_type_tag_str, storage_err
                );
                Err(Status::internal(format!(
                    "Storage error fetching coin info: {}",
                    storage_err
                )))
            }
        }
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
}
