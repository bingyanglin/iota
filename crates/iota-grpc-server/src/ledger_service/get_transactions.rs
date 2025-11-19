// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use iota_grpc_types::{
    field::{FieldMaskTree, FieldMaskUtil},
    merge::Merge,
    transaction::TransactionReadSource,
    v0::{
        event::Event as ProtoEvent,
        ledger_service::{
            GetTransactionsRequest, GetTransactionsResponse, TransactionRequest, TransactionResult,
        },
        transaction::{ExecutedTransaction, TransactionEvents as ProtoTransactionEvents},
    },
};
use iota_types::digests::TransactionDigest;
use prost::Message as ProstMessage;
use prost_types::FieldMask;

use crate::{
    constants::{DEFAULT_MAX_MESSAGE_SIZE, MAX_MESSAGE_SIZE, MIN_MESSAGE_SIZE},
    error::{ErrorReason, FieldViolation, Result},
    types::{GrpcReader, TransactionRead},
};

pub const READ_MASK_DEFAULT: &str = "digest";

/// Get transactions by their digests
#[tracing::instrument(skip(reader))]
pub fn get_transactions(
    reader: &GrpcReader,
    GetTransactionsRequest {
        requests,
        read_mask,
        max_message_size_bytes,
    }: GetTransactionsRequest,
) -> Result<Vec<GetTransactionsResponse>> {
    let requests = requests
        .ok_or_else(|| {
            FieldViolation::new("requests")
                .with_description("missing requests")
                .with_reason(ErrorReason::FieldMissing)
        })?
        .requests;

    let read_mask = {
        let read_mask = read_mask.unwrap_or_else(|| FieldMask::from_str(READ_MASK_DEFAULT));
        read_mask
            .validate::<ExecutedTransaction>()
            .map_err(|path| {
                FieldViolation::new("read_mask")
                    .with_description(format!("invalid read_mask path: {path}"))
                    .with_reason(ErrorReason::FieldInvalid)
            })?;
        FieldMaskTree::from(read_mask)
    };

    // Validate and set max_message_size
    let max_message_size = match max_message_size_bytes {
        Some(size) => {
            let size = usize::try_from(size).map_err(|_| {
                FieldViolation::new("max_message_size_bytes")
                    .with_description("must be a valid positive integer")
                    .with_reason(ErrorReason::FieldInvalid)
            })?;

            match size {
                s if s < MIN_MESSAGE_SIZE => {
                    return Err(FieldViolation::new("max_message_size_bytes")
                        .with_description(format!("must be at least {MIN_MESSAGE_SIZE} bytes"))
                        .with_reason(ErrorReason::FieldInvalid)
                        .into());
                }
                s if s > MAX_MESSAGE_SIZE => {
                    return Err(FieldViolation::new("max_message_size_bytes")
                        .with_description(format!("must be at most {MAX_MESSAGE_SIZE} bytes"))
                        .with_reason(ErrorReason::FieldInvalid)
                        .into());
                }
                s => s,
            }
        }
        None => DEFAULT_MAX_MESSAGE_SIZE,
    };

    // Create an iterator that lazily fetches transactions
    let mut transactions_iter = requests
        .into_iter()
        .enumerate()
        .map(|(idx, request)| {
            let digest = parse_digest(request, idx)?;

            reader.get_transaction_read(digest).map(|transaction_read| {
                transaction_to_response(reader, transaction_read, &read_mask)
            })
        })
        .map(|result| match result {
            Ok(transaction) => TransactionResult::new_transaction(transaction),
            Err(error) => TransactionResult::new_error(error.into_status_proto()),
        })
        .peekable();

    // Track if we've returned at least one response
    let mut returned_response = false;

    // Stream responses on-demand using the iterator
    Ok(std::iter::from_fn(move || {
        let mut current_batch = Vec::new();
        let mut current_size = 0;

        // Fill the current batch up to max_message_size
        loop {
            // Peek at the next transaction to check if it fits
            let next_size = transactions_iter.peek().map(|tx| tx.encoded_len());

            match next_size {
                Some(tx_size) => {
                    // Check if adding this transaction would exceed the limit
                    if current_size + tx_size > max_message_size && !current_batch.is_empty() {
                        // Current batch is full, return it
                        // has_next is true because we peeked and found another transaction
                        returned_response = true;
                        return Some(GetTransactionsResponse {
                            transactions: current_batch,
                            has_next: true,
                        });
                    }

                    // Transaction fits, consume it and add to batch
                    let transaction = transactions_iter.next().unwrap();
                    current_batch.push(transaction);
                    current_size += tx_size;
                }
                None => {
                    // No more transactions
                    if !current_batch.is_empty() {
                        returned_response = true;
                        return Some(GetTransactionsResponse {
                            transactions: current_batch,
                            has_next: false,
                        });
                    } else if !returned_response {
                        // Return empty response if we haven't returned anything yet
                        returned_response = true;
                        return Some(GetTransactionsResponse {
                            transactions: vec![],
                            has_next: false,
                        });
                    } else {
                        // We've already returned responses, stop iteration
                        return None;
                    }
                }
            }
        }
    })
    .collect())
}

fn parse_digest(request: TransactionRequest, index: usize) -> Result<TransactionDigest> {
    let digest_bytes = request.digest.ok_or_else(|| {
        FieldViolation::new_at("requests", index)
            .with_description("missing digest")
            .with_reason(ErrorReason::FieldMissing)
    })?;

    TransactionDigest::try_from(digest_bytes.digest.as_ref()).map_err(|e| {
        FieldViolation::new_at("requests", index)
            .with_description(format!("invalid digest: {e}"))
            .with_reason(ErrorReason::FieldInvalid)
            .into()
    })
}

fn transaction_to_response(
    reader: &GrpcReader,
    source: TransactionRead,
    mask: &FieldMaskTree,
) -> ExecutedTransaction {
    // Create the source for Merge trait
    let tx_source = TransactionReadSource {
        digest: source.digest,
        transaction: &source.transaction,
        effects: &source.effects,
        events: source.events.as_ref(),
        checkpoint: source.checkpoint,
        timestamp_ms: source.timestamp_ms,
    };

    // Use Merge trait for all fields (digest, transaction, signatures, effects,
    // checkpoint, timestamp, input_objects, output_objects)
    let mut message = ExecutedTransaction::merge_from(&tx_source, mask);

    // Handle events separately since json_contents needs GrpcReader for struct
    // layout
    if let Some(submask) = mask.subtree(ExecutedTransaction::EVENTS_FIELD.name) {
        message.events = source
            .events
            .as_ref()
            .map(|events| build_events(events, &submask, reader));
    }

    message
}

fn build_events(
    events: &iota_types::effects::TransactionEvents,
    mask: &FieldMaskTree,
    reader: &GrpcReader,
) -> ProtoTransactionEvents {
    // Use Merge trait for basic fields
    let mut message = ProtoTransactionEvents::merge_from(events, mask);

    // Handle json_contents separately since it needs GrpcReader for struct layout
    if let Some(events_mask) = mask.subtree(ProtoTransactionEvents::EVENTS_FIELD.name) {
        if events_mask.contains(ProtoEvent::JSON_CONTENTS_FIELD.name) {
            // Re-render events with json_contents
            if let Some(proto_events) = &mut message.events {
                for (proto_event, event) in proto_events.events.iter_mut().zip(&events.data) {
                    proto_event.json_contents =
                        render_json(reader, &event.type_, &event.contents).map(Box::new);
                }
            }
        }
    }

    message
}

/// Render Move value as JSON using proto_value visitor
fn render_json(
    reader: &GrpcReader,
    struct_tag: &move_core_types::language_storage::StructTag,
    contents: &[u8],
) -> Option<prost_types::Value> {
    // Get the struct layout from storage
    let layout = reader.get_struct_layout(struct_tag).ok().flatten()?;

    // Use ProtoVisitorBuilder to deserialize the Move value to JSON
    const MAX_JSON_MOVE_VALUE_SIZE: usize = 1024 * 1024;
    iota_types::proto_value::ProtoVisitorBuilder::new(MAX_JSON_MOVE_VALUE_SIZE)
        .deserialize_value(contents, &layout)
        .map_err(|e| tracing::debug!("unable to convert move value to JSON: {e}"))
        .ok()
}
