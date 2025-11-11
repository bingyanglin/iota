// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! gRPC client for IOTA node operations.

// TODO: Re-enable when proto implementations are ready
// mod checkpoint;
// mod event;
mod node_client;

// pub use checkpoint::{CheckpointClient, CheckpointContent};
// pub use event::EventClient;
pub use node_client::NodeClient;
