// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

// Modules
// pub mod checkpoint_service;
// pub mod event_service;
pub mod error;
pub mod ledger_service;
pub mod server;
// TODO: Re-enable once transaction_execution_service is fixed
// pub mod transaction_execution_service;
pub mod types;

// Re-export commonly used types and traits
// pub use checkpoint_service::CheckpointGrpcService;
// pub use event_service::EventGrpcService;
pub use error::{ErrorReason, FieldViolation, RpcError};
pub use ledger_service::LedgerGrpcService;
pub use server::{GrpcServerHandle, start_grpc_server};
// TODO: Re-enable once transaction_execution_service is fixed
// pub use transaction_execution_service::TransactionExecutionGrpcService;
pub use types::{
    CheckpointDataBroadcaster, CheckpointSummaryBroadcaster, EventSubscriber,
    GrpcCheckpointDataBroadcaster, GrpcCheckpointSummaryBroadcaster, GrpcReader, GrpcStateReader,
    RestStateReaderAdapter,
};
