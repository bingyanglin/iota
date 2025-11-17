// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use iota_grpc_types::{
    field::{FieldMaskTree, FieldMaskUtil},
    v0::{
        bcs::BcsData,
        event::Event as ProtoEvent,
        ledger_service::{
            GetTransactionsRequest, GetTransactionsResponse, TransactionRequest, TransactionResult,
        },
        object::{Object as ProtoObject, Objects as ProtoObjects},
        signatures::{UserSignature, UserSignatures},
        transaction::{
            ExecutedTransaction, Transaction as ProtoTransaction,
            TransactionEffects as ProtoTransactionEffects,
            TransactionEvents as ProtoTransactionEvents,
        },
        types::{Address, Digest, ObjectReference},
    },
};
use iota_types::{
    digests::TransactionDigest, effects::TransactionEffectsAPI, message_envelope::Message,
};
use prost_types::{FieldMask, Timestamp};

use crate::{
    constants::{DEFAULT_MAX_MESSAGE_SIZE, MAX_MESSAGE_SIZE, MIN_MESSAGE_SIZE},
    error::{ErrorReason, FieldViolation, Result},
    types::{GrpcReader, TransactionRead},
};

pub const READ_MASK_DEFAULT: &str = "digest";

/// Get transactions by their digests
///
/// This implementation is based on SUI's batch_get_transactions pattern,
/// with extensions for streaming support based on max_message_size_bytes.
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

    let transactions: Vec<TransactionResult> = requests
        .into_iter()
        .enumerate()
        .map(|(idx, request)| {
            let digest = parse_digest(request, idx)?;

            reader
                .get_transaction_read(digest)
                .map(|transaction_read| transaction_to_response(reader, transaction_read, &read_mask))
        })
        .map(|result| match result {
            Ok(transaction) => TransactionResult::new_transaction(transaction),
            Err(error) => TransactionResult::new_error(error.into_status_proto()),
        })
        .collect();

    // Chunk transactions into separate responses based on max_message_size
    Ok(chunk_responses(transactions, max_message_size))
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

fn chunk_responses(
    transactions: Vec<TransactionResult>,
    max_message_size: usize,
) -> Vec<GetTransactionsResponse> {
    let mut responses = Vec::new();
    let mut current_batch = Vec::new();
    let mut current_size = 0;

    for transaction in transactions {
        // Estimate the size of this transaction result
        // Note: This is a conservative estimate based on protobuf encoding
        let estimated_size = estimate_result_size(&transaction);

        // If adding this transaction would exceed max size and we have items in batch,
        // finalize the current batch
        if current_size + estimated_size > max_message_size && !current_batch.is_empty() {
            responses.push(GetTransactionsResponse {
                transactions: std::mem::take(&mut current_batch),
                has_next: true, // More batches may follow
            });
            current_size = 0;
        }

        current_batch.push(transaction);
        current_size += estimated_size;
    }

    // Add any remaining transactions
    if !current_batch.is_empty() {
        responses.push(GetTransactionsResponse {
            transactions: current_batch,
            has_next: false, // This is the last batch
        });
    }

    // If no responses were created, return at least one empty response
    if responses.is_empty() {
        responses.push(GetTransactionsResponse {
            transactions: Vec::new(),
            has_next: false,
        });
    }

    responses
}

fn estimate_result_size(result: &TransactionResult) -> usize {
    // Conservative estimate: 1KB base + additional for actual content
    // In a production system, you might want to use prost's encoded_len()
    match &result.result {
        Some(iota_grpc_types::v0::ledger_service::transaction_result::Result::Transaction(tx)) => {
            let mut size = 1024; // Base overhead

            if let Some(bcs) = &tx.transaction.as_ref().and_then(|t| t.bcs.as_ref()) {
                size += bcs.data.len();
            }
            if let Some(effects_bcs) = &tx.effects.as_ref().and_then(|e| e.bcs.as_ref()) {
                size += effects_bcs.data.len();
            }
            if let Some(events) = &tx.events {
                if let Some(events_list) = &events.events {
                    size += events_list.events.len() * 512; // Estimate per event
                }
            }

            size
        }
        Some(iota_grpc_types::v0::ledger_service::transaction_result::Result::Error(_)) => 512, /* Errors are typically small */
        None => 256,
    }
}

fn transaction_to_response(reader: &GrpcReader, source: TransactionRead, mask: &FieldMaskTree) -> ExecutedTransaction {
    let mut message = ExecutedTransaction::default();

    if mask.contains(ExecutedTransaction::DIGEST_FIELD.name) {
        message.digest = Some(Digest {
            digest: source.digest.into_inner().to_vec().into(),
        });
    }

    if let Some(submask) = mask.subtree(ExecutedTransaction::TRANSACTION_FIELD.name) {
        message.transaction = Some(build_transaction(&source, &submask));
    }

    if let Some(submask) = mask.subtree(ExecutedTransaction::SIGNATURES_FIELD.name) {
        message.signatures = Some(build_signatures(&source, &submask));
    }

    if let Some(submask) = mask.subtree(ExecutedTransaction::EFFECTS_FIELD.name) {
        message.effects = Some(build_effects(&source, &submask));
    }

    if let Some(submask) = mask.subtree(ExecutedTransaction::EVENTS_FIELD.name) {
        message.events = source
            .events
            .as_ref()
            .map(|events| build_events(events, &submask, reader));
    }

    if mask.contains(ExecutedTransaction::CHECKPOINT_FIELD.name) {
        message.checkpoint = source.checkpoint;
    }

    if mask.contains(ExecutedTransaction::TIMESTAMP_FIELD.name) {
        message.timestamp = source.timestamp_ms.map(|timestamp_ms| Timestamp {
            seconds: (timestamp_ms / 1000) as i64,
            nanos: ((timestamp_ms % 1000) * 1_000_000) as i32,
        });
    }

    // IOTA-specific: input_objects and output_objects
    // Note: SUI has a single 'objects' field instead
    if mask.contains(ExecutedTransaction::INPUT_OBJECTS_FIELD.name) {
        message.input_objects = Some(build_input_objects(&source));
    }

    if mask.contains(ExecutedTransaction::OUTPUT_OBJECTS_FIELD.name) {
        message.output_objects = Some(build_output_objects(&source));
    }

    message
}

fn build_transaction(source: &TransactionRead, mask: &FieldMaskTree) -> ProtoTransaction {
    let mut transaction = ProtoTransaction::default();

    if mask.contains(ProtoTransaction::DIGEST_FIELD.name) {
        transaction.digest = Some(Digest {
            digest: source.digest.into_inner().to_vec().into(),
        });
    }

    if mask.contains(ProtoTransaction::BCS_FIELD.name) {
        if let Ok(bcs_bytes) = bcs::to_bytes(source.transaction.data()) {
            transaction.bcs = Some(BcsData {
                data: bcs_bytes.into(),
            });
        }
    }

    transaction
}

fn build_signatures(source: &TransactionRead, _mask: &FieldMaskTree) -> UserSignatures {
    UserSignatures {
        signatures: source
            .transaction
            .tx_signatures()
            .iter()
            .map(|sig| UserSignature {
                bcs: Some(BcsData {
                    data: sig.as_ref().to_vec().into(),
                }),
            })
            .collect(),
    }
}

fn build_effects(source: &TransactionRead, mask: &FieldMaskTree) -> ProtoTransactionEffects {
    let mut effects = ProtoTransactionEffects::default();

    if mask.contains(ProtoTransactionEffects::DIGEST_FIELD.name) {
        effects.digest = Some(Digest {
            digest: source.effects.digest().into_inner().to_vec().into(),
        });
    }

    if mask.contains(ProtoTransactionEffects::BCS_FIELD.name) {
        if let Ok(bcs_bytes) = bcs::to_bytes(&source.effects) {
            effects.bcs = Some(BcsData {
                data: bcs_bytes.into(),
            });
        }
    }

    effects
}

fn build_events(
    events: &iota_types::effects::TransactionEvents,
    mask: &FieldMaskTree,
    reader: &GrpcReader,
) -> ProtoTransactionEvents {
    use iota_grpc_types::v0::event::{Event as ProtoEvent, Events as ProtoEvents};

    let mut message = ProtoTransactionEvents::default();

    if mask.contains(ProtoTransactionEvents::DIGEST_FIELD.name) {
        message.digest = Some(Digest {
            digest: events.digest().into_inner().to_vec().into(),
        });
    }

    if let Some(events_mask) = mask.subtree(ProtoTransactionEvents::EVENTS_FIELD.name) {
        let proto_events: Vec<ProtoEvent> = events
            .data
            .iter()
            .map(|event| build_event(event, &events_mask, reader))
            .collect();

        message.events = Some(ProtoEvents {
            events: proto_events,
        });
    }

    message
}

fn build_event(
    event: &iota_types::event::Event,
    mask: &FieldMaskTree,
    reader: &GrpcReader,
) -> iota_grpc_types::v0::event::Event {
    let mut proto_event = ProtoEvent::default();

    if mask.contains(ProtoEvent::BCS_FIELD.name) {
        if let Ok(bcs_bytes) = bcs::to_bytes(event) {
            proto_event.bcs = Some(BcsData {
                data: bcs_bytes.into(),
            });
        }
    }

    if mask.contains(ProtoEvent::PACKAGE_ID_FIELD.name) {
        proto_event.package_id = Some(Address {
            address: event.package_id.to_vec().into(),
        });
    }

    if mask.contains(ProtoEvent::MODULE_FIELD.name) {
        proto_event.module = Some(event.transaction_module.to_string());
    }

    if mask.contains(ProtoEvent::SENDER_FIELD.name) {
        proto_event.sender = Some(Address {
            address: event.sender.to_vec().into(),
        });
    }

    if mask.contains(ProtoEvent::EVENT_TYPE_FIELD.name) {
        proto_event.event_type = Some(event.type_.to_canonical_string(true));
    }

    if mask.contains(ProtoEvent::BCS_CONTENTS_FIELD.name) {
        proto_event.bcs_contents = Some(BcsData {
            data: event.contents.clone().into(),
        });
    }

    // Render json_contents if requested
    if mask.contains(ProtoEvent::JSON_CONTENTS_FIELD.name) {
        proto_event.json_contents = render_json(reader, &event.type_, &event.contents).map(Box::new);
    }

    proto_event
}

/// Render Move value as JSON using proto_value visitor
///
/// This converts BCS-encoded Move values into prost_types::Value for JSON representation.
/// Similar to SUI's render_json implementation.
fn render_json(
    reader: &GrpcReader,
    struct_tag: &move_core_types::language_storage::StructTag,
    contents: &[u8],
) -> Option<prost_types::Value> {
    // Get the struct layout from storage
    let layout = reader
        .get_struct_layout(struct_tag)
        .ok()
        .flatten()?;

    // Use ProtoVisitorBuilder to deserialize the Move value to JSON
    // Using a reasonable max size of 1MB for JSON values
    const MAX_JSON_MOVE_VALUE_SIZE: usize = 1024 * 1024;
    iota_types::proto_value::ProtoVisitorBuilder::new(MAX_JSON_MOVE_VALUE_SIZE)
        .deserialize_value(contents, &layout)
        .map_err(|e| tracing::debug!("unable to convert move value to JSON: {e}"))
        .ok()
}

fn build_input_objects(source: &TransactionRead) -> iota_grpc_types::v0::object::Objects {
    let mut input_refs = Vec::new();

    // Add gas object (it's always an input)
    let (gas_ref, _owner) = source.effects.gas_object();
    input_refs.push(gas_ref);

    // Add shared objects from effects
    for shared_obj in source.effects.input_shared_objects() {
        input_refs.push(shared_obj.object_ref());
    }

    // Add modified objects (they were inputs)
    for (obj_ref, _owner) in source.effects.old_object_metadata() {
        input_refs.push(obj_ref);
    }

    let objects: Vec<ProtoObject> = input_refs
        .into_iter()
        .map(|(object_id, version, digest)| ProtoObject {
            reference: Some(ObjectReference {
                object_id: Some(object_id.to_string()),
                version: Some(version.value()),
                digest: Some(Digest {
                    digest: digest.into_inner().to_vec().into(),
                }),
            }),
            ..Default::default()
        })
        .collect();

    ProtoObjects { objects }
}

fn build_output_objects(source: &TransactionRead) -> iota_grpc_types::v0::object::Objects {
    let mut output_refs = Vec::new();

    // Add created objects
    output_refs.extend(source.effects.created().into_iter().map(|(r, _)| r));

    // Add mutated objects (they are outputs with new versions)
    output_refs.extend(source.effects.mutated().into_iter().map(|(r, _)| r));

    // Add unwrapped objects
    output_refs.extend(source.effects.unwrapped().into_iter().map(|(r, _)| r));

    let objects: Vec<ProtoObject> = output_refs
        .into_iter()
        .map(|(object_id, version, digest)| ProtoObject {
            reference: Some(ObjectReference {
                object_id: Some(object_id.to_string()),
                version: Some(version.value()),
                digest: Some(Digest {
                    digest: digest.into_inner().to_vec().into(),
                }),
            }),
            ..Default::default()
        })
        .collect();

    ProtoObjects { objects }
}
