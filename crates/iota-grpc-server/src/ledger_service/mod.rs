// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

mod get_epoch;
mod get_object;
mod get_service_info;
mod get_transactions;

use std::sync::Arc;

pub use get_epoch::{get_epoch as get_epoch_impl, protocol_config_to_proto};
pub use get_transactions::get_transactions as get_transactions_impl;
use iota_grpc_types::v0::ledger_service::{
    self as grpc_ledger, ledger_service_server::LedgerService,
};
use iota_protocol_config::Chain;
use iota_types::digests::ChainIdentifier;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

use crate::types::GrpcReader;

pub struct LedgerGrpcService {
    pub reader: Arc<GrpcReader>,
    pub cancellation_token: CancellationToken,
    pub chain: Chain,
    pub chain_id: ChainIdentifier,
    pub server_version: Option<String>,
}

impl LedgerGrpcService {
    pub fn new(
        reader: Arc<GrpcReader>,
        cancellation_token: CancellationToken,
        chain: Chain,
        chain_id: ChainIdentifier,
        server_version: Option<String>,
    ) -> Self {
        Self {
            reader,
            cancellation_token,
            chain,
            chain_id,
            server_version,
        }
    }
}

#[tonic::async_trait]
impl LedgerService for LedgerGrpcService {
    async fn get_epoch(
        &self,
        request: Request<grpc_ledger::GetEpochRequest>,
    ) -> Result<Response<grpc_ledger::GetEpochResponse>, Status> {
        get_epoch_impl(self, request.into_inner())
            .map(Response::new)
            .map_err(Into::into)
    }

    async fn get_service_info(
        &self,
        request: Request<grpc_ledger::GetServiceInfoRequest>,
    ) -> Result<Response<grpc_ledger::GetServiceInfoResponse>, Status> {
        get_service_info::get_service_info(
            (*self.reader).clone(),
            self.chain_id,
            self.chain,
            self.server_version.clone(),
            request.into_inner(),
        )
        .await
        .map(Response::new)
    }

    type GetObjectsStream = futures::stream::Once<
        futures::future::Ready<Result<grpc_ledger::GetObjectsResponse, Status>>,
    >;

    async fn get_objects(
        &self,
        request: Request<grpc_ledger::GetObjectsRequest>,
    ) -> Result<Response<Self::GetObjectsStream>, Status> {
        // For now, return a single response with all objects
        // TODO: Implement proper streaming with chunking based on
        // max_message_size_bytes
        let response =
            get_object::get_objects((*self.reader).clone(), request.into_inner()).await?;

        // Convert single response to stream
        let stream = futures::stream::once(futures::future::ready(Ok(response)));
        Ok(Response::new(stream))
    }

    type GetTransactionsStream = futures::stream::Once<
        futures::future::Ready<Result<grpc_ledger::GetTransactionsResponse, Status>>,
    >;

    async fn get_transactions(
        &self,
        request: Request<grpc_ledger::GetTransactionsRequest>,
    ) -> Result<Response<Self::GetTransactionsStream>, Status> {
        // For now, return a single response with all transactions
        // TODO: Implement proper streaming with chunking based on
        // max_message_size_bytes
        let response = get_transactions_impl(&self.reader, request.into_inner())
            .map_err(|e| Status::from(e))?;

        // Convert single response to stream
        let stream = futures::stream::once(futures::future::ready(Ok(response)));
        Ok(Response::new(stream))
    }

    type GetCheckpointDataStream =
        futures::stream::Empty<Result<grpc_ledger::CheckpointData, Status>>;

    async fn get_checkpoint_data(
        &self,
        _request: Request<grpc_ledger::GetCheckpointDataRequest>,
    ) -> Result<Response<Self::GetCheckpointDataStream>, Status> {
        Err(Status::unimplemented(
            "get_checkpoint_data not yet implemented",
        ))
    }

    type StreamCheckpointDataStream =
        futures::stream::Empty<Result<grpc_ledger::CheckpointData, Status>>;

    async fn stream_checkpoint_data(
        &self,
        _request: Request<grpc_ledger::CheckpointDataStreamRequest>,
    ) -> Result<Response<Self::StreamCheckpointDataStream>, Status> {
        Err(Status::unimplemented(
            "stream_checkpoint_data not yet implemented",
        ))
    }
}
