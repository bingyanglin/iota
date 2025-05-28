use std::convert::TryInto;

// use anyhow; // For parsing error type -- No longer needed directly here
use iota_types::digests::TransactionDigest; // For parsing transaction_id
use tonic::{Request, Response, Status};

use crate::{
    conversions::transactions::convert_verified_transaction_to_gprc, /* Use the new conversion
                                                                      * function */
    error::GrpcApiError, // Will be used for error conversion
    proto::iota::gprc::v1::{
        Direction, GetTransactionRequest, ListTransactionsRequest, ListTransactionsResponse,
        StreamTransactionsRequest, TransactionGprc,
        transaction_gprc_service_server::TransactionGprcService,
    },
    server::StateReader,
};

// This function is no longer used, as list_transactions now uses state_reader.
// fn create_mock_verified_transaction(
//     id_byte: u8,
// ) -> (
//     TransactionDigest,
//     std::sync::Arc<iota_types::transaction::VerifiedTransaction>,
// ) { ... }

#[derive(Clone)]
pub struct TransactionServiceImpl {
    state_reader: StateReader,
}

impl TransactionServiceImpl {
    pub fn new(state_reader: StateReader) -> Self {
        Self { state_reader }
    }
}

#[tonic::async_trait]
impl TransactionGprcService for TransactionServiceImpl {
    async fn get_transaction(
        &self,
        request: Request<GetTransactionRequest>,
    ) -> Result<Response<TransactionGprc>, Status> {
        let request_ref = request.get_ref();
        // For logging, convert bytes to hex if needed, or log a slice
        println!(
            "[gRPC TransactionService] Received GetTransaction request: DigestBytes (len={})",
            request_ref.transaction_digest_bytes.len()
        );

        let transaction_digest_bytes = request.into_inner().transaction_digest_bytes;
        if transaction_digest_bytes.len() != 32 {
            return Err(Status::invalid_argument(format!(
                "Transaction digest bytes must be 32 bytes long, got {}",
                transaction_digest_bytes.len()
            )));
        }

        // Directly convert bytes to TransactionDigest
        let transaction_digest_array: [u8; 32] = transaction_digest_bytes
            .try_into()
            .map_err(|_e| Status::internal("Failed to convert digest bytes to array"))?;
        let transaction_digest = TransactionDigest::new(transaction_digest_array);

        // No more string parsing here

        match self.state_reader.get_transaction(&transaction_digest) {
            Ok(Some(core_transaction_arc)) => {
                // Pass the &TransactionDigest to the conversion function
                let gprc_transaction = convert_verified_transaction_to_gprc(
                    &transaction_digest,
                    &core_transaction_arc,
                )
                .map_err(GrpcApiError::from)?;

                Ok(Response::new(gprc_transaction))
            }
            Ok(None) => Err(Status::not_found(format!(
                "Transaction not found for digest {:?}", // Use digest's Debug for now
                transaction_digest
            ))),
            Err(storage_err) => {
                eprintln!("Storage error: {}", storage_err);
                Err(Status::internal(format!(
                    "Failed to retrieve transaction: {}",
                    storage_err
                )))
            }
        }
    }

    async fn list_transactions(
        &self,
        request: Request<ListTransactionsRequest>,
    ) -> Result<Response<ListTransactionsResponse>, Status> {
        let req_inner = request.into_inner();
        println!(
            "[gRPC TransactionService] Received ListTransactions request: {:?}",
            req_inner
        );

        let limit = req_inner.limit.map_or(50, |l| l.min(100).max(1)) as u64; // Default 50, max 100, min 1
        let direction_gprc = req_inner.direction.map_or(Direction::Ascending, |d| {
            Direction::try_from(d).unwrap_or(Direction::Ascending)
        });

        let storage_direction = match direction_gprc {
            Direction::Ascending => iota_types::storage::ListDirection::Ascending,
            Direction::Descending => iota_types::storage::ListDirection::Descending,
            // Should not happen if Direction enum is kept in sync
            _ => iota_types::storage::ListDirection::Ascending,
        };

        let cursor_digest_opt: Option<TransactionDigest> = match req_inner.cursor {
            Some(cursor_hex) => {
                if !cursor_hex.starts_with("0x") || cursor_hex.len() != 66 {
                    return Err(Status::invalid_argument(
                        "Cursor must be a 0x-prefixed 64-char hex string for transaction digest.",
                    ));
                }
                match hex::decode(&cursor_hex[2..]) {
                    Ok(bytes) => {
                        if bytes.len() == 32 {
                            let arr: [u8; 32] = bytes.try_into().map_err(|_e| {
                                Status::internal("Failed to convert cursor hex to digest array")
                            })?;
                            Some(TransactionDigest::new(arr))
                        } else {
                            return Err(Status::invalid_argument(
                                "Cursor hex string must represent 32 bytes for transaction digest.",
                            ));
                        }
                    }
                    Err(_) => {
                        return Err(Status::invalid_argument(
                            "Invalid hex string for cursor transaction digest.",
                        ));
                    }
                }
            }
            None => None,
        };

        match self
            .state_reader
            .list_transactions(cursor_digest_opt, limit, storage_direction)
        {
            Ok(core_transactions) => {
                let mut gprc_transactions = Vec::new();
                for (digest, verified_tx) in core_transactions {
                    match convert_verified_transaction_to_gprc(&digest, &verified_tx) {
                        Ok(gprc_tx) => gprc_transactions.push(gprc_tx),
                        Err(e) => {
                            eprintln!("Error converting core transaction to gRPC: {:?}", e);
                            // Optionally, return an internal error or skip the transaction
                            return Err(Status::internal("Failed to convert transaction data."));
                        }
                    }
                }

                let next_cursor_hex: Option<String> = if gprc_transactions.len() == limit as usize {
                    gprc_transactions
                        .last()
                        .map(|tx| tx.transaction_id_hex.clone())
                } else {
                    None
                };

                Ok(Response::new(ListTransactionsResponse {
                    transactions: gprc_transactions,
                    next_cursor: next_cursor_hex,
                }))
            }
            Err(e) => {
                eprintln!(
                    "[gRPC TransactionService] Error from state_reader.list_transactions: {}",
                    e
                );
                Err(Status::internal(format!(
                    "Failed to list transactions: {}",
                    e
                )))
            }
        }
    }

    async fn stream_transactions(
        &self,
        request: Request<StreamTransactionsRequest>,
    ) -> Result<Response<Self::StreamTransactionsStream>, Status> {
        let req_inner = request.into_inner();
        println!(
            "[gRPC TransactionService] Received StreamTransactions request: {:?}",
            req_inner
        );

        let initial_cursor_digest_opt: Option<TransactionDigest> = match req_inner
            .start_from_transaction_id
        {
            Some(cursor_hex) => {
                if !cursor_hex.starts_with("0x") || cursor_hex.len() != 66 {
                    return Err(Status::invalid_argument(
                        "start_from_transaction_id must be a 0x-prefixed 64-char hex string.",
                    ));
                }
                match hex::decode(&cursor_hex[2..]) {
                    Ok(bytes) => {
                        if bytes.len() == 32 {
                            let arr: [u8; 32] = bytes.try_into().map_err(|_e| {
                                Status::internal(
                                    "Failed to convert start_from_transaction_id to digest array",
                                )
                            })?;
                            Some(TransactionDigest::new(arr))
                        } else {
                            return Err(Status::invalid_argument(
                                "start_from_transaction_id hex string must represent 32 bytes.",
                            ));
                        }
                    }
                    Err(_) => {
                        return Err(Status::invalid_argument(
                            "Invalid hex string for start_from_transaction_id.",
                        ));
                    }
                }
            }
            None => None, // Start from the beginning if no specific ID is provided
        };

        let state_reader = self.state_reader.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(128); // Increased buffer size

        tokio::spawn(async move {
            let mut current_cursor = initial_cursor_digest_opt;
            let polling_interval = std::time::Duration::from_secs(2);
            const STREAM_POLL_LIMIT: u64 = 10;

            loop {
                tokio::time::sleep(polling_interval).await;

                match state_reader.list_transactions(
                    current_cursor,
                    STREAM_POLL_LIMIT,
                    iota_types::storage::ListDirection::Ascending,
                ) {
                    Ok(core_transactions) => {
                        if core_transactions.is_empty() && current_cursor.is_some() {
                            // If we had a cursor and got nothing, we are likely at the tip.
                            // Continue polling from the same cursor.
                            continue;
                        } else if core_transactions.is_empty() && current_cursor.is_none() {
                            // Started from beginning and got nothing, DB might be empty.
                            // Continue polling from None cursor.
                            continue;
                        }

                        for (digest, verified_tx) in core_transactions {
                            match convert_verified_transaction_to_gprc(&digest, &verified_tx) {
                                Ok(gprc_tx) => {
                                    if tx.send(Ok(gprc_tx)).await.is_err() {
                                        println!(
                                            "[gRPC StreamTransactions] Client disconnected. Stopping stream."
                                        );
                                        return; // Client disconnected
                                    }
                                    current_cursor = Some(digest); // Update cursor to the last sent transaction
                                }
                                Err(e) => {
                                    eprintln!(
                                        "[gRPC StreamTransactions] Error converting core transaction: {:?}",
                                        e
                                    );
                                    if tx
                                        .send(Err(Status::internal(
                                            "Error converting transaction data.",
                                        )))
                                        .await
                                        .is_err()
                                    {
                                        println!(
                                            "[gRPC StreamTransactions] Client disconnected while sending error."
                                        );
                                    }
                                    return; // Stop on conversion error
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "[gRPC StreamTransactions] Error from state_reader.list_transactions: {}",
                            e
                        );
                        if tx
                            .send(Err(Status::internal(
                                "Error fetching transactions from storage.",
                            )))
                            .await
                            .is_err()
                        {
                            println!(
                                "[gRPC StreamTransactions] Client disconnected while sending storage error."
                            );
                        }
                        // Depending on the error, might want to retry or stop.
                        // For now, stop the stream on storage error.
                        return;
                    }
                }
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    // Define the stream type for StreamTransactions
    type StreamTransactionsStream =
        tokio_stream::wrappers::ReceiverStream<Result<TransactionGprc, Status>>;
}
