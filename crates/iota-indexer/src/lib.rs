// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2024 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

#![recursion_limit = "256"]

use std::time::Duration;

use anyhow::Result;
use errors::IndexerError;
use futures::TryFutureExt;
use iota_grpc_client::Client as GrpcClient;
use iota_json_rpc::{JsonRpcServerBuilder, ServerHandle, ServerType};
use iota_json_rpc_api::CLIENT_SDK_TYPE_HEADER;
use iota_metrics::spawn_monitored_task;
use jsonrpsee::http_client::{HeaderMap, HeaderValue, HttpClient, HttpClientBuilder};
use metrics::IndexerMetrics;
use prometheus::Registry;
use system_package_task::SystemPackageTask;
use tap::Pipe;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::{
    apis::{
        CoinReadApi, ExtendedApi, GovernanceReadApi, IndexerApi, MoveUtilsApi, OptimisticWriteApi,
        ReadApi, TransactionBuilderApi, WriteApi,
    },
    config::JsonRpcConfig,
    errors::IndexerResult,
    optimistic_indexing::OptimisticTransactionExecutor,
    read::IndexerReader,
    store::PgIndexerStore,
};

pub mod apis;
pub mod backfill;
pub mod config;
pub mod db;
pub mod errors;
pub mod historical_fallback;
pub mod indexer;
pub mod ingestion;
pub mod metrics;
pub mod models;
pub mod optimistic_indexing;
pub mod processors;
pub mod pruning;
pub mod read;
pub mod schema;
pub mod store;
pub mod system_package_task;
pub mod test_utils;
pub mod types;

pub async fn build_json_rpc_server(
    store: PgIndexerStore,
    prometheus_registry: &Registry,
    reader: IndexerReader,
    config: &JsonRpcConfig,
    metrics: IndexerMetrics,
) -> Result<ServerHandle, IndexerError> {
    let mut builder =
        JsonRpcServerBuilder::new(env!("CARGO_PKG_VERSION"), prometheus_registry, None, None);

    let fullnode_client = FullnodeRpc::new(&config.rpc_client_url).await?;
    // Register common modules
    builder.register_module(IndexerApi::new(
        reader.clone(),
        config.iota_names_options.clone().into(),
    ))?;
    builder.register_module(TransactionBuilderApi::from(reader.clone()))?;
    builder.register_module(MoveUtilsApi::new(reader.clone()))?;
    builder.register_module(GovernanceReadApi::new(reader.clone()))?;
    builder.register_module(ReadApi::new(reader.clone(), fullnode_client.clone()))?;
    builder.register_module(CoinReadApi::new(reader.clone())?)?;
    builder.register_module(ExtendedApi::new(reader.clone()))?;
    builder.register_module(OptimisticWriteApi::new(
        WriteApi::new_with_fullnode_rpc(fullnode_client, reader.clone()),
        OptimisticTransactionExecutor::new(&config.rpc_client_url, reader.clone(), store, metrics)
            .await?,
    ))?;

    let cancel = CancellationToken::new();
    let system_package_task =
        SystemPackageTask::new(reader, cancel.clone(), Duration::from_secs(10));

    tracing::info!("Starting system package task");
    spawn_monitored_task!(async move { system_package_task.run().await });

    Ok(builder
        .start(config.rpc_address, None, ServerType::Http, Some(cancel))
        .await?)
}

fn get_http_client(rpc_client_url: &str) -> Result<HttpClient, IndexerError> {
    let mut headers = HeaderMap::new();
    headers.insert(CLIENT_SDK_TYPE_HEADER, HeaderValue::from_static("indexer"));

    HttpClientBuilder::default()
        .max_request_size(2 << 30)
        .set_headers(headers.clone())
        .build(rpc_client_url)
        .map_err(|e| {
            warn!("failed to get new Http client with error: {:?}", e);
            IndexerError::HttpClientInit(format!(
                "failed to initialize fullnode RPC client with error: {e:?}"
            ))
        })
}

/// Abstracts the fullnode RPC API.
///
/// The gRPC API is still in alpha but will be the default in the future.
/// On the otherhand, the JSON RPC API is scheduled to be deprecated in the
/// future.
/// We need to maintain backward compatibility with the JSON RPC API for now
/// until the gRPC API is fully compatible.
#[derive(Clone)]
pub enum FullnodeRpc {
    Grpc(Box<iota_grpc_client::Client>),
    JsonRpc(Box<HttpClient>),
}

impl FullnodeRpc {
    /// Creates a new instance of `FullnodeRpc`.
    ///
    /// Under the hood it first tries to establish a gRPC connection to the
    /// fullnode. If successful, it returns a [`FullnodeRpc::Grpc`] variant.
    /// If not, it falls back to JSON RPC and returns a [`FullnodeRpc::JsonRpc`]
    /// variant.
    pub async fn new(fullnode_rpc_url: &str) -> IndexerResult<Self> {
        match GrpcClient::connect(fullnode_rpc_url)
            .and_then(|grpc_client| async {
                // check if we can make gRPC request to client
                grpc_client.get_service_info(None).await?;
                Ok(grpc_client)
            })
            .await
        {
            Ok(grpc_client) => {
                info!("using fullnode gRPC API");
                Self::Grpc(Box::new(grpc_client)).pipe(Ok)
            }
            // fallback to JSON RPC if gRPC fails
            Err(e) => {
                debug!("unable to establish a gRPC connection to fullnode: {e}");
                info!("using fullnode JSON RPC API");
                get_http_client(fullnode_rpc_url).map(|client| Self::JsonRpc(Box::new(client)))
            }
        }
    }
}
