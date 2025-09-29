// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use iota_json_rpc_types::IotaTransactionBlockResponse;
use tonic::{Status, transport::Channel};

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
    pub async fn execute_transaction(
        &mut self,
        request: ExecuteTransactionRequest,
    ) -> Result<IotaTransactionBlockResponse, tonic::Status> {
        // Make gRPC call
        let response = self.client.execute_transaction(request).await?;

        let grpc_response = response.into_inner();

        // Deserialize JSON response
        Self::deserialize_response(&grpc_response)
    }

    /// Deserialize JSON response into IotaTransactionBlockResponse
    fn deserialize_response(
        response: &ExecuteTransactionResponse,
    ) -> Result<IotaTransactionBlockResponse, Status> {
        // Extract data from JsonData wrapper
        let json_data = response
            .json_data
            .as_ref()
            .ok_or_else(|| Status::internal("Missing json_data in response"))?;

        // Deserialize directly from JSON
        serde_json::from_slice(&json_data.data).map_err(|e| {
            Status::internal(format!(
                "Failed to deserialize transaction response from JSON: {e}"
            ))
        })
    }
}
