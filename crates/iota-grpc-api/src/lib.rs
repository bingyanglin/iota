// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

// Generated protobuf code
pub mod common {
    tonic::include_proto!("iota.grpc.common");
}

pub mod read {
    tonic::include_proto!("iota.grpc.read");
}

pub mod write {
    tonic::include_proto!("iota.grpc.write");
}

pub mod checkpoints {
    tonic::include_proto!("iota.grpc.checkpoints");
}

pub mod events {
    tonic::include_proto!("iota.grpc.events");
}

pub mod transactions {
    tonic::include_proto!("iota.grpc.transactions");
}

// Modules
pub mod checkpoint_service;
pub mod client;
pub mod conversions;
pub mod event_service;
pub mod read_service;
pub mod server;
pub mod transaction_service;
pub mod types;
pub mod write_service;

// Re-export commonly used types and traits
pub use checkpoint_service::CheckpointGrpcService;
pub use client::{
    CheckpointClient, CheckpointContent, EventClient, NodeClient, ReadClient, TransactionClient,
    WriteClient,
};
pub use event_service::EventGrpcService;
pub use read_service::ReadGrpcService;
pub use server::{GrpcServerHandle, start_grpc_server};
pub use transaction_service::TransactionGrpcService;
pub use types::{
    CheckpointDataBroadcaster, CheckpointSummaryBroadcaster, GrpcCheckpointDataBroadcaster,
    GrpcCheckpointSummaryBroadcaster, GrpcReader, GrpcStateReader, RestStateReaderAdapter,
};
pub use write_service::WriteGrpcService;
