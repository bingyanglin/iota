// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use iota_grpc_types::{
    field::{FieldMask, FieldMaskTree, FieldMaskUtil},
    v0::{
        bcs::BcsData,
        ledger_service::{
            GetTransactionsRequest, GetTransactionsResponse, TransactionRequest, TransactionResult,
        },
        signatures::{UserSignature, UserSignatures},
        transaction::{
            ExecutedTransaction, Transaction as ProtoTransaction,
            TransactionEffects as ProtoTransactionEffects,
            TransactionEvents as ProtoTransactionEvents,
        },
        types::Digest,
    },
};
use iota_types::{digests::TransactionDigest, message_envelope::Message};
use prost_types::Timestamp;

use crate::{
    error::{ErrorReason, FieldViolation, RpcError},
    types::{GrpcReader, TransactionRead},
};

pub const READ_MASK_DEFAULT: &str = "digest";

/// Get transactions by their digests
#[tracing::instrument(skip(reader))]
pub fn get_transactions(
    reader: &GrpcReader,
    request: GetTransactionsRequest,
) -> Result<GetTransactionsResponse, RpcError> {
    // Extract requests
    let requests = request
        .requests
        .ok_or_else(|| {
            FieldViolation::new("requests")
                .with_description("missing requests")
                .with_reason(ErrorReason::FieldMissing)
        })?
        .requests;

    // Validate and parse the read mask
    let read_mask = {
        let read_mask = request
            .read_mask
            .unwrap_or_else(|| FieldMask::from_str(READ_MASK_DEFAULT));
        // TODO: Enable validation once MessageFields is implemented for
        // ExecutedTransaction read_mask
        //     .validate::<ExecutedTransaction>()
        //     .map_err(|path| {
        //         FieldViolation::new("read_mask")
        //             .with_description(format!("invalid read_mask path: {path}"))
        //             .with_reason(ErrorReason::FieldInvalid)
        //     })?;
        FieldMaskTree::from(read_mask)
    };

    // Process each transaction request
    let transactions = requests
        .into_iter()
        .enumerate()
        .map(|(idx, tx_request)| process_transaction_request(reader, tx_request, idx, &read_mask))
        .collect();

    Ok(GetTransactionsResponse { transactions })
}

/// Process a single transaction request
fn process_transaction_request(
    reader: &GrpcReader,
    request: TransactionRequest,
    index: usize,
    mask: &FieldMaskTree,
) -> TransactionResult {
    // Parse the digest
    let digest_bytes = match request.digest {
        Some(d) => d.digest,
        None => {
            let error = FieldViolation::new_at("requests", index)
                .with_description("missing digest")
                .with_reason(ErrorReason::FieldMissing);
            return TransactionResult {
                result: Some(
                    iota_grpc_types::v0::ledger_service::transaction_result::Result::Error(
                        error.into_status_proto(),
                    ),
                ),
            };
        }
    };

    let digest = match TransactionDigest::try_from(digest_bytes.as_slice()) {
        Ok(d) => d,
        Err(e) => {
            let error = FieldViolation::new_at("requests", index)
                .with_description(format!("invalid digest: {e}"))
                .with_reason(ErrorReason::FieldInvalid);
            return TransactionResult {
                result: Some(
                    iota_grpc_types::v0::ledger_service::transaction_result::Result::Error(
                        error.into_status_proto(),
                    ),
                ),
            };
        }
    };

    // Fetch transaction data
    match reader.get_transaction_read(digest) {
        Ok(transaction_read) => {
            let transaction = transaction_to_response(transaction_read, mask);
            TransactionResult {
                result: Some(
                    iota_grpc_types::v0::ledger_service::transaction_result::Result::Transaction(
                        transaction,
                    ),
                ),
            }
        }
        Err(error) => TransactionResult {
            result: Some(
                iota_grpc_types::v0::ledger_service::transaction_result::Result::Error(
                    FieldViolation::new_at("requests", index)
                        .with_description(error.to_string())
                        .with_reason(ErrorReason::FieldInvalid)
                        .into_status_proto(),
                ),
            ),
        },
    }
}

/// Convert internal TransactionRead to ExecutedTransaction proto with field
/// masking
fn transaction_to_response(source: TransactionRead, mask: &FieldMaskTree) -> ExecutedTransaction {
    let mut message = ExecutedTransaction::default();

    // Populate digest if requested
    if mask.contains("digest") {
        message.digest = Some(Digest {
            digest: source.digest.into_inner().to_vec(),
        });
    }

    // Populate transaction if requested
    if let Some(submask) = mask.subtree("transaction") {
        let mut transaction = ProtoTransaction::default();

        // Transaction digest
        if submask.contains("digest") {
            transaction.digest = Some(Digest {
                digest: source.digest.into_inner().to_vec(),
            });
        }

        // Transaction BCS data
        if submask.contains("bcs") {
            if let Ok(bcs_bytes) = bcs::to_bytes(source.transaction.data()) {
                transaction.bcs = Some(BcsData { data: bcs_bytes });
            }
        }

        message.transaction = Some(transaction);
    }

    // Populate signatures if requested
    if let Some(_submask) = mask.subtree("signatures") {
        let signatures: Vec<UserSignature> = source
            .transaction
            .tx_signatures()
            .iter()
            .map(|sig| UserSignature {
                bcs: Some(BcsData {
                    data: sig.as_ref().to_vec(),
                }),
            })
            .collect();

        message.signatures = Some(UserSignatures { signatures });
    }

    // Populate effects if requested
    if let Some(submask) = mask.subtree("effects") {
        let mut effects = ProtoTransactionEffects::default();

        // Effects digest
        if submask.contains("digest") {
            use iota_types::digests::TransactionEffectsDigest;
            let digest: TransactionEffectsDigest = source.effects.digest();
            effects.digest = Some(Digest {
                digest: digest.into_inner().to_vec(),
            });
        }

        // Effects BCS data
        if submask.contains("bcs") {
            if let Ok(bcs_bytes) = bcs::to_bytes(&source.effects) {
                effects.bcs = Some(BcsData { data: bcs_bytes });
            }
        }

        message.effects = Some(effects);
    }

    // Populate events if requested
    if let Some(submask) = mask.subtree("events") {
        if let Some(core_events) = source.events {
            let mut events = ProtoTransactionEvents::default();

            // Events digest
            if submask.contains("digest") {
                use iota_types::digests::TransactionEventsDigest;
                let digest: TransactionEventsDigest = core_events.digest();
                events.digest = Some(Digest {
                    digest: digest.into_inner().to_vec(),
                });
            }

            // Events data - for now, skip the detailed events field
            // TODO: Implement Events conversion if needed
            // if submask.contains("events") {
            //     events.events = Some(...);
            // }

            message.events = Some(events);
        }
    }

    // Populate checkpoint if requested
    if mask.contains("checkpoint") {
        message.checkpoint = source.checkpoint;
    }

    // Populate timestamp if requested
    if mask.contains("timestamp") {
        if let Some(timestamp_ms) = source.timestamp_ms {
            message.timestamp = Some(Timestamp {
                seconds: (timestamp_ms / 1000) as i64,
                nanos: ((timestamp_ms % 1000) * 1_000_000) as i32,
            });
        }
    }

    // Note: input_objects and output_objects are not stored with transactions
    // These would need to be fetched separately if requested
    // TODO: Implement input/output objects if needed

    message
}
