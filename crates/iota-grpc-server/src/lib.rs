// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

// Modules
pub mod constants;
pub mod error;
pub mod ledger_service;
pub mod server;
// TODO: Re-enable after addressing iota_sdk2 dependency issues
// pub mod transaction_execution_service;
pub mod types;

// Re-export commonly used types and traits
pub use error::RpcError;
pub use ledger_service::LedgerGrpcService;
pub use server::{GrpcServerHandle, start_grpc_server};
pub use types::{
    CheckpointDataBroadcaster, CheckpointSummaryBroadcaster, EventSubscriber,
    GrpcCheckpointDataBroadcaster, GrpcCheckpointSummaryBroadcaster, GrpcReader, GrpcStateReader,
    RestStateReaderAdapter,
};
