use std::convert::TryInto;

// use anyhow; // For parsing error type -- No longer needed directly here
use iota_types::digests::TransactionDigest; // For parsing transaction_id
use tonic::{Request, Response, Status};

use crate::{
    conversions::transactions::convert_verified_transaction_to_gprc, /* Use the new conversion
                                                                      * function */
    error::GrpcApiError, // Will be used for error conversion
    proto::iota::gprc::v1::{
        GetTransactionRequest, TransactionGprc,
        transaction_gprc_service_server::TransactionGprcService,
    },
    server::StateReader,
};

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
}
