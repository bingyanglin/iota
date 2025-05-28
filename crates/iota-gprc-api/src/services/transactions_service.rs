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

// Helper function to create a mock VerifiedTransaction (simplified)
// In a real scenario, these would come from the state_reader
fn create_mock_verified_transaction(
    id_byte: u8,
) -> (
    TransactionDigest,
    std::sync::Arc<iota_types::transaction::VerifiedTransaction>,
) {
    use std::sync::Arc;

    use iota_types::{
        base_types::{IotaAddress, ObjectID, ObjectRef, SequenceNumber},
        crypto::{Ed25519IotaSignature, EmptySignInfo, IotaSignatureInner, Signature, ToFromBytes},
        digests::ObjectDigest,
        message_envelope::{Envelope, VerifiedEnvelope},
        programmable_transaction_builder::ProgrammableTransactionBuilder,
        transaction::{
            GasData, SenderSignedData, TransactionData, TransactionDataV1, TransactionExpiration,
            TransactionKind,
        },
    };

    let mut tx_id_arr = [0u8; 32];
    tx_id_arr[0] = id_byte; // Make digest unique based on id_byte
    let tx_digest = TransactionDigest::new(tx_id_arr);

    let dummy_sender_address_bytes = [1u8; 32];
    let dummy_sender_address = IotaAddress::new(dummy_sender_address_bytes);
    let mut recipient_address_bytes = [2u8; 32];
    recipient_address_bytes[0] = id_byte;
    let recipient_address = IotaAddress::new(recipient_address_bytes);
    let mut obj_id_arr = [3u8; 32];
    obj_id_arr[0] = id_byte;
    let dummy_object_id = ObjectID::new(obj_id_arr);
    let dummy_object_digest = ObjectDigest::new(obj_id_arr);
    let dummy_object_ref: ObjectRef = (dummy_object_id, SequenceNumber::new(), dummy_object_digest);
    let pt = {
        let mut builder = ProgrammableTransactionBuilder::new();
        builder
            .transfer_object(recipient_address, dummy_object_ref)
            .unwrap();
        builder.finish()
    };
    let tx_kind = TransactionKind::ProgrammableTransaction(pt);
    let mut gas_obj_bytes = [4u8; 32];
    gas_obj_bytes[0] = id_byte;
    let gas_payment_object_id = ObjectID::new(gas_obj_bytes);
    let gas_payment_digest = ObjectDigest::new(gas_obj_bytes);
    let gas_data = GasData {
        payment: vec![(
            gas_payment_object_id,
            SequenceNumber::new(),
            gas_payment_digest,
        )],
        owner: dummy_sender_address,
        price: 100,
        budget: 1_000_000,
    };
    let tx_data = TransactionData::V1(TransactionDataV1 {
        kind: tx_kind,
        sender: dummy_sender_address,
        gas_data,
        expiration: TransactionExpiration::None,
    });
    let mut dummy_sig_bytes = [5u8; Ed25519IotaSignature::LENGTH];
    dummy_sig_bytes[0] = id_byte;
    let signature = Signature::Ed25519IotaSignature(
        Ed25519IotaSignature::from_bytes(&dummy_sig_bytes).unwrap(),
    );
    let sender_signed_data = SenderSignedData::new_from_sender_signature(tx_data, signature);
    let envelope = Envelope::new_from_data_and_sig(sender_signed_data, EmptySignInfo {});
    let mock_transaction = VerifiedEnvelope::new_from_verified(envelope);
    (tx_digest, Arc::new(mock_transaction))
}

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

        // TODO: This is a mock implementation.
        // Full pagination, cursor handling, and direction sorting depend on StateReader
        // capabilities. The actual StateReader might offer methods like
        // `list_transactions(cursor, limit, direction)`.

        let mut mock_transactions = Vec::new();
        for i in 1..=5 {
            // Create 5 mock transactions
            let (digest, verified_tx) = create_mock_verified_transaction(i);
            match convert_verified_transaction_to_gprc(&digest, &verified_tx) {
                Ok(gprc_tx) => mock_transactions.push(gprc_tx),
                Err(e) => {
                    eprintln!("Error converting mock transaction: {:?}", e);
                    // Skip this transaction or return an error
                }
            }
        }

        let direction = req_inner.direction.map_or(Direction::Ascending, |d| {
            Direction::try_from(d).unwrap_or(Direction::Ascending)
        });

        if direction == Direction::Descending {
            mock_transactions.reverse();
        }

        let mut cursor_index = 0;
        if let Some(cursor_hex) = req_inner.cursor {
            // Naive cursor implementation: find index of cursor
            // A real implementation would use the cursor to fetch the correct page from the
            // DB
            if let Some(pos) = mock_transactions
                .iter()
                .position(|tx| tx.transaction_id_hex == cursor_hex)
            {
                cursor_index = pos + 1; // Start from the item AFTER the cursor
            } else {
                // Cursor not found, could return error or empty
                // For mock, let's return empty if cursor is specified but not found
                return Ok(Response::new(ListTransactionsResponse {
                    transactions: vec![],
                    next_cursor: None,
                }));
            }
        }

        let limit = req_inner
            .limit
            .map_or(mock_transactions.len(), |l| l as usize); // Default to all if no limit

        let paginated_transactions: Vec<TransactionGprc> = mock_transactions
            .into_iter()
            .skip(cursor_index)
            .take(limit)
            .collect();

        let next_cursor = if cursor_index + limit < 5 && !paginated_transactions.is_empty() {
            // Crude next_cursor: ID of the last item if there are more items
            // (assuming 5 total mock items for this logic)
            paginated_transactions
                .last()
                .map(|tx| tx.transaction_id_hex.clone())
        } else {
            None
        };

        // A more robust next_cursor logic would be:
        // if paginated_transactions.len() == limit && (cursor_index + limit) <
        // total_mock_items {    paginated_transactions.last().map(|tx|
        // tx.transaction_id_hex.clone()) } else {
        //    None
        // }
        // Where total_mock_items is the actual count of items before pagination.
        // For this mock, we used 5 items.

        let response = ListTransactionsResponse {
            transactions: paginated_transactions,
            next_cursor,
        };

        Ok(Response::new(response))
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

        // TODO: Implement actual logic for streaming transactions.
        // This will involve a real stream producer, possibly from an event bus or by
        // polling. For now, this is a mock implementation that periodically
        // sends new mock transactions. The `start_from_transaction_id` is
        // currently ignored in this mock.

        let (tx, rx) = tokio::sync::mpsc::channel(16); // Channel buffer size 16

        tokio::spawn(async move {
            let mut transaction_counter: u8 = 100; // Start from a different ID range than ListTransactions
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await; // Send a new tx every second

                let (digest, verified_tx) = create_mock_verified_transaction(transaction_counter);
                match convert_verified_transaction_to_gprc(&digest, &verified_tx) {
                    Ok(gprc_tx) => {
                        if tx.send(Ok(gprc_tx)).await.is_err() {
                            println!(
                                "[gRPC StreamTransactions] Client disconnected. Stopping stream."
                            );
                            break; // Client disconnected
                        }
                        // println!(
                        //     "[gRPC StreamTransactions] Sent mock transaction
                        // with id_byte: {}",
                        //     transaction_counter
                        // );
                    }
                    Err(e) => {
                        eprintln!(
                            "[gRPC StreamTransactions] Error converting mock transaction: {:?}",
                            e
                        );
                        // Optionally send an error to the client or just log and continue/stop
                        if tx
                            .send(Err(Status::internal(
                                "Error generating streamed transaction".to_string(),
                            )))
                            .await
                            .is_err()
                        {
                            println!(
                                "[gRPC StreamTransactions] Client disconnected while sending error. Stopping stream."
                            );
                        }
                        break; // Stop on error for simplicity in mock
                    }
                }

                transaction_counter = transaction_counter.wrapping_add(1);
                if transaction_counter == 0 {
                    // Avoid reusing IDs from ListTransactions extensively if it wraps quickly
                    transaction_counter = 100;
                }
            }
            println!("[gRPC StreamTransactions] Mock stream task finished.");
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    // Define the stream type for StreamTransactions
    type StreamTransactionsStream =
        tokio_stream::wrappers::ReceiverStream<Result<TransactionGprc, Status>>;
}
