// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use anyhow::anyhow;
use futures::{Stream, StreamExt};
use iota_json_rpc_types::IotaTransactionBlockEffects;
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

    /// Stream transaction effects with automatic BCS deserialization and
    /// filtering.
    ///
    /// # Arguments
    /// * `filter` - Transaction filter to apply to the stream
    ///
    /// # Returns
    /// A stream of IOTA transaction effects that match the specified filter
    pub async fn stream_transactions(
        &mut self,
        filter: crate::transactions::TransactionFilter,
    ) -> Result<impl Stream<Item = Result<IotaTransactionBlockEffects, tonic::Status>>, tonic::Status>
    {
        let request = TransactionStreamRequest {
            filter: Some(filter),
        };
        let response = self.client.stream_transactions(request).await?;
        let stream = response.into_inner();

        // Transform the stream to deserialize Transaction protobuf messages
        // into native IotaTransactionBlockEffects
        Ok(stream.map(|transaction_result| {
            transaction_result.and_then(|transaction| {
                Self::deserialize_transaction(&transaction).map_err(|e| {
                    tonic::Status::internal(format!("Failed to deserialize transaction: {e}"))
                })
            })
        }))
    }

    /// Deserialize transaction effects from JSON-serialized data.
    fn deserialize_transaction(tx: &Transaction) -> anyhow::Result<IotaTransactionBlockEffects> {
        // Extract data from JsonData wrapper
        let json_data = tx
            .json_data
            .as_ref()
            .ok_or_else(|| anyhow!("Missing json_data in transaction"))?;

        // Deserialize directly from JSON
        serde_json::from_slice(&json_data.data)
            .map_err(|e| anyhow!("Failed to deserialize transaction effects from JSON: {e}"))
    }
}
