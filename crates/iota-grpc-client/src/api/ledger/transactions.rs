// Copyright (c) 2026 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! High-level API for transaction queries.

use std::collections::HashMap;

use iota_grpc_types::{
    proto::proto_to_timestamp_ms,
    v0::ledger_service::{GetTransactionsRequest, TransactionRequest, TransactionRequests},
};
use iota_sdk_types::Digest;

use crate::{
    Client,
    api::{
        Error, FieldMask, FieldMaskUtil, ProtoResult, Result, TRANSACTIONS_READ_MASK,
        TransactionResponse, TryFromProtoError, extract_execution_data,
    },
};

impl Client {
    /// Get transactions by their digests.
    ///
    /// Returns `TransactionResponse` for each transaction, containing the
    /// transaction data, signatures, effects, and optional events.
    ///
    /// Results are returned in the same order as the input digests.
    /// If a transaction is not found, an error is returned.
    ///
    /// # Field Mask
    ///
    /// The optional `read_mask` parameter controls which fields the server
    /// returns. If `None`, uses [`TRANSACTIONS_READ_MASK`] which includes all
    /// fields needed for `TransactionResponse`.
    ///
    /// **Required fields** (must be included in custom masks):
    /// - `transaction.bcs` - Transaction data
    /// - `effects.bcs` - Transaction effects
    ///
    /// **Optional fields:**
    /// - `events` - Transaction events
    /// - `checkpoint` - Checkpoint sequence number
    /// - `timestamp` - Execution timestamp
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use iota_grpc_client::Client;
    /// # use iota_sdk_types::Digest;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::connect("http://localhost:9000").await?;
    /// let digest: Digest = todo!();
    ///
    /// // Default: get all fields
    /// let txs = client.get_transactions(&[digest], None).await?;
    ///
    /// // Custom: only required fields (smaller response)
    /// let txs = client
    ///     .get_transactions(&[digest], Some("transaction.bcs,effects.bcs"))
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`TRANSACTIONS_READ_MASK`]: crate::TRANSACTIONS_READ_MASK
    pub async fn get_transactions(
        &self,
        digests: &[Digest],
        read_mask: Option<&str>,
    ) -> Result<Vec<TransactionResponse>> {
        if digests.is_empty() {
            return Ok(vec![]);
        }

        let requests = TransactionRequests {
            requests: digests
                .iter()
                .map(|d| TransactionRequest {
                    digest: Some((*d).into()),
                })
                .collect(),
        };

        let mask = read_mask.unwrap_or(TRANSACTIONS_READ_MASK);
        let request = GetTransactionsRequest {
            requests: Some(requests),
            read_mask: Some(FieldMask::from_str(mask)),
            max_message_size_bytes: None,
        };

        let mut client = self.ledger_service_client();

        let mut stream = client.get_transactions(request).await?.into_inner();

        // Build a map from digest to index for O(1) lookups
        let digest_to_index: HashMap<Digest, usize> =
            digests.iter().enumerate().map(|(i, d)| (*d, i)).collect();

        // Pre-allocate results vector with None values
        let mut results: Vec<Option<TransactionResponse>> = vec![None; digests.len()];

        while let Some(response) = stream.message().await? {
            for result in response.transactions {
                let proto_tx = result.into_result()?;
                let tx_response = convert_to_response(*proto_tx)?;
                if let Some(&index) = digest_to_index.get(&tx_response.digest) {
                    results[index] = Some(tx_response);
                }
            }
        }

        // Convert to Vec<TransactionResponse>, erroring if any are missing
        results
            .into_iter()
            .enumerate()
            .map(|(i, opt)| {
                opt.ok_or_else(|| Error::NotFound {
                    resource: "transaction",
                    id: digests[i].to_string(),
                })
            })
            .collect()
    }
}

/// Convert a proto ExecutedTransaction to TransactionResponse.
fn convert_to_response(
    proto: iota_grpc_types::v0::transaction::ExecutedTransaction,
) -> Result<TransactionResponse> {
    // Extract and deserialize transaction BCS (required)
    let tx_bcs = proto
        .transaction
        .as_ref()
        .and_then(|t| t.bcs.as_ref())
        .ok_or(TryFromProtoError::missing("transaction.bcs"))?;

    let signed_tx: iota_sdk_types::SignedTransaction = tx_bcs
        .deserialize()
        .map_err(|e| TryFromProtoError::invalid("transaction.bcs", e))?;

    // Extract checkpoint and timestamp from proto (these are per-transaction
    // historical data, not available in response headers)
    let checkpoint = proto.checkpoint;
    let timestamp_ms = proto.timestamp.map(proto_to_timestamp_ms).transpose()?;

    let data = extract_execution_data(&proto)?;

    Ok(TransactionResponse {
        digest: signed_tx.transaction.digest(),
        transaction: signed_tx.transaction,
        signatures: signed_tx.signatures,
        effects: data.effects,
        events: data.events,
        checkpoint,
        timestamp_ms,
    })
}
