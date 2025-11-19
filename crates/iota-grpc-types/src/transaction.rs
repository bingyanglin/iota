// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! Merge implementations for transaction-related proto types.

use std::sync::Arc;

use iota_types::{
    effects::{TransactionEffects, TransactionEffectsAPI, TransactionEvents},
    message_envelope::Message as EnvelopeMessage,
    transaction::VerifiedTransaction,
};

use crate::{
    field::FieldMaskTree,
    merge::Merge,
    proto_helpers::timestamp_ms_to_proto,
    v0::{
        bcs::BcsData,
        event::{Event as ProtoEvent, Events as ProtoEvents},
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

/// Source data for building an ExecutedTransaction
pub struct TransactionReadSource<'a> {
    pub digest: iota_types::digests::TransactionDigest,
    pub transaction: &'a Arc<VerifiedTransaction>,
    pub effects: &'a TransactionEffects,
    pub events: Option<&'a TransactionEvents>,
    pub checkpoint: Option<u64>,
    pub timestamp_ms: Option<u64>,
}

// Merge implementation for ExecutedTransaction
impl Merge<&TransactionReadSource<'_>> for ExecutedTransaction {
    fn merge(&mut self, source: &TransactionReadSource<'_>, mask: &FieldMaskTree) {
        if mask.contains(Self::DIGEST_FIELD.name) {
            self.digest = Some(Digest {
                digest: source.digest.into_inner().to_vec().into(),
            });
        }

        if let Some(submask) = mask.subtree(Self::TRANSACTION_FIELD.name) {
            self.transaction = Some(ProtoTransaction::merge_from(
                source.transaction.as_ref(),
                &submask,
            ));
        }

        if let Some(submask) = mask.subtree(Self::SIGNATURES_FIELD.name) {
            self.signatures = Some(UserSignatures::merge_from(
                source.transaction.as_ref(),
                &submask,
            ));
        }

        if let Some(submask) = mask.subtree(Self::EFFECTS_FIELD.name) {
            self.effects = Some(ProtoTransactionEffects::merge_from(
                source.effects,
                &submask,
            ));
        }

        // Note: Events are handled separately by the caller since they need
        // JSON rendering context (access to GrpcReader for struct layouts)

        if mask.contains(Self::CHECKPOINT_FIELD.name) {
            self.checkpoint = source.checkpoint;
        }

        if mask.contains(Self::TIMESTAMP_FIELD.name) {
            self.timestamp = source.timestamp_ms.map(timestamp_ms_to_proto);
        }

        // IOTA-specific: input_objects and output_objects
        if mask.contains(Self::INPUT_OBJECTS_FIELD.name) {
            self.input_objects = Some(build_input_objects(source.effects));
        }

        if mask.contains(Self::OUTPUT_OBJECTS_FIELD.name) {
            self.output_objects = Some(build_output_objects(source.effects));
        }
    }
}

fn build_input_objects(effects: &TransactionEffects) -> ProtoObjects {
    let mut input_refs = Vec::new();

    // Add gas object (it's always an input)
    let (gas_ref, _owner) = effects.gas_object();
    input_refs.push(gas_ref);

    // Add shared objects from effects
    for shared_obj in effects.input_shared_objects() {
        input_refs.push(shared_obj.object_ref());
    }

    // Add modified objects (they were inputs)
    for (obj_ref, _owner) in effects.old_object_metadata() {
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

fn build_output_objects(effects: &TransactionEffects) -> ProtoObjects {
    let mut output_refs = Vec::new();

    // Add created objects
    output_refs.extend(effects.created().into_iter().map(|(r, _)| r));

    // Add mutated objects (they are outputs with new versions)
    output_refs.extend(effects.mutated().into_iter().map(|(r, _)| r));

    // Add unwrapped objects
    output_refs.extend(effects.unwrapped().into_iter().map(|(r, _)| r));

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

// Merge implementation for Transaction
impl Merge<&VerifiedTransaction> for ProtoTransaction {
    fn merge(&mut self, source: &VerifiedTransaction, mask: &FieldMaskTree) {
        if mask.contains(Self::DIGEST_FIELD.name) {
            self.digest = Some(Digest {
                digest: source.digest().into_inner().to_vec().into(),
            });
        }

        if mask.contains(Self::BCS_FIELD.name) {
            if let Ok(bcs_bytes) = bcs::to_bytes(source.data()) {
                self.bcs = Some(BcsData {
                    data: bcs_bytes.into(),
                });
            }
        }
    }
}

// Merge implementation for UserSignatures (extracts signatures from
// VerifiedTransaction)
impl Merge<&VerifiedTransaction> for UserSignatures {
    fn merge(&mut self, source: &VerifiedTransaction, _mask: &FieldMaskTree) {
        self.signatures = source
            .tx_signatures()
            .iter()
            .map(|sig| UserSignature {
                bcs: Some(BcsData {
                    data: sig.as_ref().to_vec().into(),
                }),
            })
            .collect();
    }
}

// Merge implementation for TransactionEffects
impl Merge<&TransactionEffects> for ProtoTransactionEffects {
    fn merge(&mut self, source: &TransactionEffects, mask: &FieldMaskTree) {
        if mask.contains(Self::DIGEST_FIELD.name) {
            self.digest = Some(Digest {
                digest: source.digest().into_inner().to_vec().into(),
            });
        }

        if mask.contains(Self::BCS_FIELD.name) {
            if let Ok(bcs_bytes) = bcs::to_bytes(source) {
                self.bcs = Some(BcsData {
                    data: bcs_bytes.into(),
                });
            }
        }
    }
}

// Merge implementation for TransactionEvents
impl Merge<&TransactionEvents> for ProtoTransactionEvents {
    fn merge(&mut self, source: &TransactionEvents, mask: &FieldMaskTree) {
        if mask.contains(Self::DIGEST_FIELD.name) {
            self.digest = Some(Digest {
                digest: source.digest().into_inner().to_vec().into(),
            });
        }

        if let Some(events_mask) = mask.subtree(Self::EVENTS_FIELD.name) {
            let proto_events: Vec<ProtoEvent> = source
                .data
                .iter()
                .map(|event| ProtoEvent::merge_from(event, &events_mask))
                .collect();

            self.events = Some(ProtoEvents {
                events: proto_events,
            });
        }
    }
}

// Merge implementation for Event
// Note: json_contents field is NOT handled here because it requires
// access to GrpcReader for struct layout resolution. The caller should
// handle json_contents separately after calling merge_from.
impl Merge<&iota_types::event::Event> for ProtoEvent {
    fn merge(&mut self, source: &iota_types::event::Event, mask: &FieldMaskTree) {
        if mask.contains(Self::BCS_FIELD.name) {
            if let Ok(bcs_bytes) = bcs::to_bytes(source) {
                self.bcs = Some(BcsData {
                    data: bcs_bytes.into(),
                });
            }
        }

        if mask.contains(Self::PACKAGE_ID_FIELD.name) {
            self.package_id = Some(Address {
                address: source.package_id.to_vec().into(),
            });
        }

        if mask.contains(Self::MODULE_FIELD.name) {
            self.module = Some(source.transaction_module.to_string());
        }

        if mask.contains(Self::SENDER_FIELD.name) {
            self.sender = Some(Address {
                address: source.sender.to_vec().into(),
            });
        }

        if mask.contains(Self::EVENT_TYPE_FIELD.name) {
            self.event_type = Some(source.type_.to_canonical_string(true));
        }

        if mask.contains(Self::BCS_CONTENTS_FIELD.name) {
            self.bcs_contents = Some(BcsData {
                data: source.contents.clone().into(),
            });
        }

        // json_contents is NOT handled here - requires GrpcReader for struct
        // layout
    }
}
