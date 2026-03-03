// Copyright (c) 2026 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! gRPC streaming server for Simulacrum

pub use simulacrum::grpc::start_simulacrum_grpc_server;

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use iota_config::local_ip_utils;
    use simulacrum::Simulacrum;

    use super::*;

    #[tokio::test]
    async fn test_grpc_server_startup_with_mutex() {
        let sim = Simulacrum::new();

        // Create some checkpoints
        sim.advance_clock(Duration::from_secs(1));
        sim.create_checkpoint();

        let simulacrum = Arc::new(sim);

        // Start gRPC server with test configuration using test utilities
        let address = local_ip_utils::new_local_tcp_socket_for_testing();
        let config = iota_config::node::GrpcApiConfig {
            address,
            ..Default::default()
        };
        let shutdown_token = tokio_util::sync::CancellationToken::new();

        let server_handle =
            start_simulacrum_grpc_server(simulacrum, config, shutdown_token.clone())
                .await
                .unwrap();

        // Verify server handle was created with proper address resolution
        assert!(server_handle.address().ip().is_loopback());
        assert!(server_handle.address().port() > 0);

        // Shutdown
        shutdown_token.cancel();
        server_handle.shutdown().await.unwrap();
    }
}
