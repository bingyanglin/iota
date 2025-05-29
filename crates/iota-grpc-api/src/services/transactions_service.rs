use iota_types::{
    digests::TransactionDigest,
    // All other iota_types imports were related to mock generation and are now removed.
};
use tonic::{Request, Response, Status};

use crate::{
    conversions::{
        transactions::convert_verified_transaction_to_gprc,
        transactions_execution::{
            convert_raw_bytes_to_signed_transaction, convert_successful_execution_to_gprc,
        },
    },
    error::GrpcApiError,
    proto::iota::gprc::v1::{
        Direction, ExecuteTransactionRequestGprc, GetTransactionRequest, ListTransactionsRequest,
        ListTransactionsResponse, TransactionExecutionResponseGprc, TransactionGprc,
        transaction_gprc_service_server::TransactionGprcService,
    },
    server::StateReader,
};

// Error status codes for TransactionExecutionResponseGprc, can be moved to a
// common place if needed
// const EXEC_STATUS_SUCCESS: u32 = 0; // Removed unused constant
const EXEC_STATUS_ERROR_INVALID_TX_BYTES: u32 = 1;
const EXEC_STATUS_ERROR_QUORUM_DRIVER: u32 = 2;
// ... add more specific error codes as needed ...

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

        let limit_u64: u64 = req_inner.limit.map_or(50, |l_val| l_val.min(100).max(1)); // Use u64, consistent with proto type
        let limit: u64 = limit_u64;

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

    async fn execute_transaction(
        &self,
        request: Request<ExecuteTransactionRequestGprc>,
    ) -> Result<Response<TransactionExecutionResponseGprc>, Status> {
        println!("[gRPC TransactionService] Received ExecuteTransaction request");
        let req_inner = request.into_inner();

        let signed_transaction =
            match convert_raw_bytes_to_signed_transaction(&req_inner.raw_transaction_bytes) {
                Ok(tx) => tx,
                Err(e) => {
                    eprintln!("[gRPC TransactionService] Invalid transaction bytes: {}", e);
                    let err_resp = TransactionExecutionResponseGprc {
                        transaction_id_hex: String::new(), // No valid tx_id if parsing failed
                        status_code: EXEC_STATUS_ERROR_INVALID_TX_BYTES,
                        status_message: format!("Failed to parse raw_transaction_bytes: {}", e),
                        gas_used: None,
                        effects_digest_hex: None,
                        created_object_ids: vec![],
                        deleted_object_ids: vec![],
                        mutated_object_ids: vec![],
                    };
                    // Return Ok with an error payload for parsing errors
                    return Ok(Response::new(err_resp));
                }
            };

        let transaction_digest = *signed_transaction.digest();
        let transaction_id_hex = transaction_digest.to_string(); // Get this early for error responses

        match self
            .state_reader
            .execute_transaction_for_gprc(signed_transaction)
            .await
        {
            Ok(core_response) => {
                // core_response is QuorumDriverResponse
                let gprc_response =
                    convert_successful_execution_to_gprc(&core_response, &transaction_digest);
                Ok(Response::new(gprc_response))
            }
            Err(quorum_driver_error) => {
                eprintln!(
                    "[gRPC TransactionService] QuorumDriver error for tx {}: {}",
                    transaction_id_hex, quorum_driver_error
                );
                let error_message =
                    format!("Transaction execution failed: {}", quorum_driver_error);
                // Construct a gRPC response indicating failure
                let err_resp = TransactionExecutionResponseGprc {
                    transaction_id_hex, // We have the tx_id
                    status_code: EXEC_STATUS_ERROR_QUORUM_DRIVER,
                    status_message: error_message,
                    gas_used: None,
                    effects_digest_hex: None,
                    created_object_ids: vec![],
                    deleted_object_ids: vec![],
                    mutated_object_ids: vec![],
                };
                // For execution errors that are not transport/gRPC level,
                // returning Ok with an error payload is often preferred.
                Ok(Response::new(err_resp))
            }
        }
    }
}
