// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use tonic::transport::Channel;

use crate::write::{
    ExecuteTransactionRequest, ExecuteTransactionResponse, write_service_client::WriteServiceClient,
};

/// Dedicated client for write-related gRPC operations.
///
/// This client handles all write service interactions including transaction
/// execution with various response options.
#[derive(Clone)]
pub struct WriteClient {
    client: WriteServiceClient<Channel>,
}

impl WriteClient {
    /// Create a new WriteClient from a shared gRPC channel.
    pub(super) fn new(channel: Channel) -> Self {
        Self {
            client: WriteServiceClient::new(channel),
        }
    }

    /// Execute a transaction with specified options.
    ///
    /// # Arguments
    /// * `request` - ExecuteTransaction request containing transaction bytes,
    ///   signatures, and options
    ///
    /// # Returns
    /// ExecuteTransactionResponse containing transaction results, effects,
    /// events, etc.
    pub async fn execute_transaction(
        &mut self,
        request: ExecuteTransactionRequest,
    ) -> Result<ExecuteTransactionResponse, tonic::Status> {
        let response = self.client.execute_transaction(request).await?;
        Ok(response.into_inner())
    }
}
