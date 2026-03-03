// Copyright (c) 2026 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! gRPC server utilities for Simulacrum.
//!
//! Provides [`start_simulacrum_grpc_server`] which wires up a complete gRPC
//! server backed by a [`Simulacrum`] instance, including proper chain ID
//! resolution and an optional [`TransactionExecutor`].
//!
//! This is used by `simulacrum-server` (the standalone binary) and can be
//! reused by any test harness that needs a simulacrum-backed gRPC endpoint.

use std::sync::Arc;

use anyhow::Result;
use iota_grpc_server::{GrpcReader, GrpcServerHandle, start_grpc_server};
use iota_node_storage::NodeStateReader;
use iota_types::transaction_executor::TransactionExecutor as TransactionExecutorTrait;
use tokio_util::sync::CancellationToken;

use crate::{Simulacrum, transaction_executor::TransactionExecutor};

/// Start a gRPC server for the given simulacrum instance.
///
/// This creates the full server stack with:
/// - The [`Simulacrum`] as the state reader (implements [`NodeStateReader`])
/// - A [`TransactionExecutor`] for transaction execution/simulation
/// - The real chain identifier from the simulacrum genesis
pub async fn start_simulacrum_grpc_server(
    simulacrum: Arc<Simulacrum>,
    config: iota_config::node::GrpcApiConfig,
    shutdown_token: CancellationToken,
) -> Result<GrpcServerHandle> {
    let chain_id = simulacrum
        .get_chain_identifier()
        .expect("chain identifier should be set");

    let executor =
        Some(Arc::new(TransactionExecutor::new(simulacrum.clone()))
            as Arc<dyn TransactionExecutorTrait>);

    let grpc_reader = Arc::new(GrpcReader::new(simulacrum, None));

    start_grpc_server(grpc_reader, executor, config, shutdown_token, chain_id).await
}
