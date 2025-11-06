// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

// Modules
pub mod checkpoint_service;
pub mod event_service;
pub mod ledger_service;
pub mod server;
pub mod state_service;
pub mod types;

// Re-export commonly used types and traits
pub use checkpoint_service::CheckpointGrpcService;
pub use event_service::EventGrpcService;
pub use ledger_service::LedgerGrpcService;
pub use server::{GrpcServerHandle, start_grpc_server};
pub use state_service::StateGrpcService;
pub use types::{
    CheckpointDataBroadcaster, CheckpointSummaryBroadcaster, EventSubscriber,
    GrpcCheckpointDataBroadcaster, GrpcCheckpointSummaryBroadcaster, GrpcReader, GrpcStateReader,
    RestStateReaderAdapter,
};
