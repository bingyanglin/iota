use std::str::FromStr;

use iota_types::base_types::{IotaAddress, ObjectID};
use tonic::{Request, Response, Status};

use crate::{
    conversions::objects::convert_object_to_gprc,
    proto::iota::gprc::v1::{
        AccountInfoGprc, GetAccountInfoRequest, ListAccountObjectsRequest,
        ListAccountObjectsResponse, ObjectGprc, StringU64,
        accounts_gprc_service_server::AccountsGprcService,
    },
    server::StateReader,
    utils::parse_optional_string_u64_to_u64,
};

const DEFAULT_LIST_ACCOUNT_OBJECTS_PAGE_SIZE: u64 = 50;
const MAX_LIST_ACCOUNT_OBJECTS_PAGE_SIZE: u64 = 1000;

#[derive(Clone)]
pub struct AccountsServiceImpl {
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
        let req_address = request.into_inner().address;
        println!(
            "[gRPC AccountsService] Received GetAccountInfo request for address: {}",
            req_address
        );

        // TODO: Implementing GetAccountInfo requires a way to resolve an IotaAddress
        // to its primary account object (e.g., main coin or account resource) to fetch
        // its balance and sequence number. This is not directly available via
        // RestStateReader without further conventions or a dedicated lookup
        // method. For this PoC, returning placeholder data.

        Ok(Response::new(AccountInfoGprc {
            address: req_address,
            balance: Some(StringU64 {
                value: "0".to_string(),
            }), // Placeholder
            sequence_number: Some(StringU64 {
                value: "0".to_string(),
            }), // Placeholder
        }))
    }

    async fn list_account_objects(
        &self,
        request: Request<ListAccountObjectsRequest>,
    ) -> Result<Response<ListAccountObjectsResponse>, Status> {
        let req_inner = request.into_inner();
        println!(
            "[gRPC AccountsService] Received ListAccountObjects request: {:?}",
            req_inner
        );

        let owner_address = match IotaAddress::from_str(&req_inner.owner_address) {
            Ok(addr) => addr,
            Err(e) => {
                return Err(Status::invalid_argument(format!(
                    "Invalid owner_address: {}. Error: {}",
                    req_inner.owner_address, e
                )));
            }
        };

        let page_size_opt_ref: Option<&StringU64> = req_inner.page_size.as_ref();
        let page_size = parse_optional_string_u64_to_u64(
            page_size_opt_ref,
            DEFAULT_LIST_ACCOUNT_OBJECTS_PAGE_SIZE,
        );
        let effective_limit = std::cmp::min(page_size, MAX_LIST_ACCOUNT_OBJECTS_PAGE_SIZE);
        if effective_limit == 0 {
            return Err(Status::invalid_argument("page_size cannot be zero"));
        }
        let query_limit = effective_limit + 1;

        let cursor_object_id: Option<ObjectID> = match req_inner.cursor {
            Some(cursor_str) if !cursor_str.is_empty() => match ObjectID::from_str(&cursor_str) {
                Ok(obj_id) => Some(obj_id),
                Err(e) => {
                    return Err(Status::invalid_argument(format!(
                        "Invalid cursor format: {}. Error: {}",
                        cursor_str, e
                    )));
                }
            },
            _ => None,
        };

        let object_infos_iter = match self
            .state_reader
            .account_owned_objects_info_iter(owner_address, cursor_object_id)
        {
            Ok(iter) => iter,
            Err(e) => {
                eprintln!(
                    "[gRPC AccountsService] Error creating account_owned_objects_info_iter: {:?}",
                    e
                );
                return Err(Status::internal(format!(
                    "Failed to initiate listing account objects: {}",
                    e
                )));
            }
        };

        let mut fetched_objects_gprc: Vec<ObjectGprc> = Vec::new();

        for info in object_infos_iter.take(query_limit as usize) {
            match self.state_reader.get_object(&info.object_id) {
                Ok(Some(core_object)) => {
                    match convert_object_to_gprc(&info.object_id, &core_object) {
                        Ok(gprc_obj) => fetched_objects_gprc.push(gprc_obj),
                        Err(conv_err) => {
                            eprintln!(
                                "[gRPC AccountsService] Failed to convert object {}: {:?}",
                                info.object_id, conv_err
                            );
                            continue;
                        }
                    }
                }
                Ok(None) => {
                    eprintln!(
                        "[gRPC AccountsService] Object {} not found though listed as owned by account {}.",
                        info.object_id, owner_address
                    );
                    continue;
                }
                Err(storage_err) => {
                    eprintln!(
                        "[gRPC AccountsService] Failed to fetch object {}: {:?}",
                        info.object_id, storage_err
                    );
                    return Err(Status::internal(format!(
                        "Error fetching object {}: {}",
                        info.object_id, storage_err
                    )));
                }
            }
        }

        let mut next_cursor: Option<String> = None;
        if fetched_objects_gprc.len() > effective_limit as usize {
            if let Some(last_returned_obj) =
                fetched_objects_gprc.get((effective_limit - 1) as usize)
            {
                next_cursor = Some(last_returned_obj.object_id.clone());
            }
            fetched_objects_gprc.truncate(effective_limit as usize);
        } else if fetched_objects_gprc.len() == query_limit as usize
            && query_limit > effective_limit
        {
            if let Some(last_item) = fetched_objects_gprc.get(effective_limit as usize) {
                next_cursor = Some(last_item.object_id.clone());
            }
            fetched_objects_gprc.truncate(effective_limit as usize);
        }

        Ok(Response::new(ListAccountObjectsResponse {
            objects: fetched_objects_gprc,
            next_cursor,
        }))
    }
}
