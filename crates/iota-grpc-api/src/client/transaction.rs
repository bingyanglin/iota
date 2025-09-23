// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use futures::Stream;
use tonic::transport::Channel;

use crate::transactions::{
    Transaction, TransactionStreamRequest, transaction_service_client::TransactionServiceClient,
};

/// Dedicated client for transaction-related gRPC operations.
///
/// This client handles all transaction service interactions including streaming
/// transactions with filtering capabilities.
#[derive(Clone)]
pub struct TransactionClient {
    client: TransactionServiceClient<Channel>,
}

impl TransactionClient {
    /// Create a new TransactionClient from a shared gRPC channel.
    pub(super) fn new(channel: Channel) -> Self {
        Self {
            client: TransactionServiceClient::new(channel),
        }
    }

    /// Stream transactions with filtering.
    ///
    /// # Arguments
    /// * `filter` - Transaction filter to apply to the stream
    ///
    /// # Returns
    /// A stream of protobuf Transaction messages that match the specified
    /// filter
    pub async fn stream_transactions(
        &mut self,
        filter: crate::transactions::TransactionFilter,
    ) -> Result<impl Stream<Item = Result<Transaction, tonic::Status>>, tonic::Status> {
        let request = TransactionStreamRequest {
            filter: Some(filter),
        };
        let stream = self.client.stream_transactions(request).await?.into_inner();

        Ok(stream)
    }
}
