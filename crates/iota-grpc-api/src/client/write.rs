// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use tonic::transport::Channel;

use crate::{
    common::{BcsData, TransactionResponse},
    write::{
        DevInspectTransactionRequest, DevInspectTransactionResponse, DryRunTransactionRequest,
        DryRunTransactionResponse, ExecuteTransactionRequest,
        write_service_client::WriteServiceClient,
    },
};

/// Dedicated client for write-related gRPC operations.
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

    /// Execute a transaction and return the gRPC response.
    pub async fn execute_transaction(
        &mut self,
        request: ExecuteTransactionRequest,
    ) -> Result<TransactionResponse, tonic::Status> {
        let response = self.client.execute_transaction(request).await?;
        Ok(response.into_inner())
    }

    /// Dev inspect a transaction and return the response.
    pub async fn dev_inspect_transaction(
        &mut self,
        request: DevInspectTransactionRequest,
    ) -> Result<DevInspectTransactionResponse, tonic::Status> {
        let response = self.client.dev_inspect_transaction(request).await?;
        Ok(response.into_inner())
    }

    /// Dry run a transaction and return the response.
    pub async fn dry_run_transaction(
        &mut self,
        tx_bytes: Vec<u8>,
    ) -> Result<DryRunTransactionResponse, tonic::Status> {
        let request = DryRunTransactionRequest {
            tx_bytes: Some(BcsData { data: tx_bytes }),
        };
        let response = self.client.dry_run_transaction(request).await?;
        Ok(response.into_inner())
    }
}
