// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

/// Configuration for the gRPC API service
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    /// The address to bind the gRPC server to
    #[serde(default = "default_grpc_api_address")]
    pub address: std::net::SocketAddr,

    /// Buffer size for broadcast channels used for checkpoint streaming
    #[serde(default = "default_checkpoint_broadcast_buffer_size")]
    pub checkpoint_broadcast_buffer_size: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            address: default_grpc_api_address(),
            checkpoint_broadcast_buffer_size: default_checkpoint_broadcast_buffer_size(),
        }
    }
}

fn default_grpc_api_address() -> std::net::SocketAddr {
    use std::net::{IpAddr, Ipv4Addr};
    std::net::SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 50051)
}

fn default_checkpoint_broadcast_buffer_size() -> usize {
    100
}

/// Buffer size for broadcast channels used for checkpoint streaming.
pub const CHECKPOINT_BROADCAST_BUFFER_SIZE: usize = 100;

/// Buffer size for mpsc channels used for event streaming.
pub const EVENT_STREAM_BUFFER_SIZE: usize = 100;

/// Buffer size for broadcast channels used for event integration.
pub const EVENT_INTEGRATION_BROADCAST_BUFFER_SIZE: usize = 1000;

pub mod checkpoint {
    tonic::include_proto!("iota.grpc.checkpoint");
}

pub mod events {
    tonic::include_proto!("iota.grpc.events");
}

pub mod event_integration;
pub mod event_service;

pub use event_integration::{EventIntegration, EventIntegrationTrait};
pub mod client;

pub mod checkpoint_service;
pub mod types;

// Re-export commonly used types and traits
pub use checkpoint_service::CheckpointGrpcService;
pub use client::{CheckpointClient, CheckpointContent, NodeClient};
pub use types::{
    CheckpointDataBroadcaster, CheckpointSummaryBroadcaster, GrpcCheckpointDataBroadcaster,
    GrpcCheckpointSummaryBroadcaster, GrpcReader, GrpcStateReader, RestStateReaderAdapter,
};
