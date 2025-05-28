use bcs;
use iota_types::{
    base_types::ExecutionDigests,
    crypto::{self, AuthorityStrongQuorumSignInfo},
    digests,
    effects::TransactionEffects,
    full_checkpoint_content::{CheckpointData, CheckpointTransaction},
    message_envelope::{Envelope, Message},
    messages_checkpoint::{CheckpointContents, CheckpointSummary, VerifiedCheckpoint},
    signature::GenericSignature,
    transaction::{self, SenderSignedData, TransactionData},
};
use roaring::RoaringBitmap;

use crate::{
    error::GrpcApiError,
    proto::iota::gprc::v1::{
        CheckpointDataGprc, CheckpointDigestGprc, CheckpointTransactionGprc,
        SignedCheckpointSummaryGprc, VerifiedTransactionGprc,
    },
};

// Helper to convert Option<&iota_types::digests::CheckpointDigest> to
// Option<CheckpointDigestGprc>
pub fn convert_option_core_digest_to_gprc(
    core_digest_opt: Option<&digests::CheckpointDigest>,
) -> Option<CheckpointDigestGprc> {
    core_digest_opt.map(|core_digest| CheckpointDigestGprc {
        digest: core_digest.into_inner().to_vec(),
    })
}

pub fn convert_verified_checkpoint_to_gprc_summary(
    verified_checkpoint: &VerifiedCheckpoint,
) -> Result<SignedCheckpointSummaryGprc, GrpcApiError> {
    let summary_data = verified_checkpoint.data();

    let content_digest_gprc = Some(CheckpointDigestGprc {
        digest: summary_data.content_digest.into_inner().to_vec(),
    });

    let previous_digest_gprc =
        convert_option_core_digest_to_gprc(summary_data.previous_digest.as_ref());

    Ok(SignedCheckpointSummaryGprc {
        epoch: summary_data.epoch,
        sequence_number: summary_data.sequence_number,
        network_total_transactions: summary_data.network_total_transactions,
        content_digest: content_digest_gprc,
        previous_digest: previous_digest_gprc,
    })
}

pub fn convert_full_checkpoint_data_to_gprc(
    core_checkpoint_data: &CheckpointData,
) -> Result<CheckpointDataGprc, GrpcApiError> {
    // 1. Convert the summary
    let summary_data = core_checkpoint_data.checkpoint_summary.data();

    let gprc_summary = SignedCheckpointSummaryGprc {
        epoch: summary_data.epoch,
        sequence_number: summary_data.sequence_number,
        network_total_transactions: summary_data.network_total_transactions,
        content_digest: Some(CheckpointDigestGprc {
            digest: summary_data.content_digest.into_inner().to_vec(),
        }),
        previous_digest: convert_option_core_digest_to_gprc(summary_data.previous_digest.as_ref()),
    };

    // 2. Convert transactions
    let mut gprc_transactions = Vec::new();
    for core_tx in &core_checkpoint_data.transactions {
        let raw_tx_bytes = bcs::to_bytes(core_tx.transaction.transaction_data()).map_err(|e| {
            eprintln!("BCS serialization failed for transaction data: {:?}", e);
            GrpcApiError::SerializationError(format!("Failed to serialize transaction data: {}", e))
        })?;

        let full_tx_gprc = VerifiedTransactionGprc {
            raw_tx: raw_tx_bytes,
        };

        gprc_transactions.push(CheckpointTransactionGprc {
            content: Some(
                crate::proto::iota::gprc::v1::checkpoint_transaction_gprc::Content::FullTransaction(
                    full_tx_gprc,
                ),
            ),
        });
    }

    Ok(CheckpointDataGprc {
        summary: Some(gprc_summary),
        transactions: gprc_transactions,
    })
}

// TODO: Add conversion for StreamedCheckpoint if it differs or needs more logic

pub fn convert_checkpoint_data_gprc_to_core(
    gprc_checkpoint_data: CheckpointDataGprc,
) -> Result<CheckpointData, GrpcApiError> {
    let gprc_summary = gprc_checkpoint_data.summary.ok_or_else(|| {
        GrpcApiError::ConversionError("Missing summary in CheckpointDataGprc".to_string())
    })?;

    let content_digest_vec = gprc_summary
        .content_digest
        .ok_or_else(|| {
            GrpcApiError::ConversionError("Missing content_digest in gRPC summary".to_string())
        })?
        .digest;
    let core_content_digest_array: [u8; 32] =
        content_digest_vec.try_into().map_err(|v: Vec<u8>| {
            GrpcApiError::ConversionError(format!(
                "Invalid content_digest length: expected 32, got {}",
                v.len()
            ))
        })?;
    let core_content_digest = digests::CheckpointContentsDigest::new(core_content_digest_array);

    let core_previous_digest_opt_result = gprc_summary.previous_digest.map(|pd_gprc| {
        let prev_digest_vec = pd_gprc.digest;
        prev_digest_vec
            .try_into()
            .map_err(|v: Vec<u8>| {
                GrpcApiError::ConversionError(format!(
                    "Invalid previous_digest length: expected 32, got {}",
                    v.len()
                ))
            })
            .map(digests::CheckpointDigest::new)
    });

    let core_previous_digest = match core_previous_digest_opt_result {
        Some(Ok(digest)) => Some(digest),
        Some(Err(e)) => return Err(e),
        None => None,
    };

    let core_summary_data = CheckpointSummary {
        epoch: gprc_summary.epoch,
        sequence_number: gprc_summary.sequence_number,
        network_total_transactions: gprc_summary.network_total_transactions,
        content_digest: core_content_digest,
        previous_digest: core_previous_digest,
        epoch_rolling_gas_cost_summary: Default::default(),
        timestamp_ms: 0,
        checkpoint_commitments: Default::default(),
        end_of_epoch_data: None,
        version_specific_data: Default::default(),
    };

    let dummy_checkpoint_sig = AuthorityStrongQuorumSignInfo {
        epoch: core_summary_data.epoch,
        signature: crypto::AggregateAuthoritySignature::default(),
        signers_map: RoaringBitmap::new(),
    };
    let core_checkpoint_envelope =
        Envelope::new_from_data_and_sig(core_summary_data.clone(), dummy_checkpoint_sig.clone());

    let mut core_transactions: Vec<CheckpointTransaction> = Vec::new();
    for gprc_tx_wrapper in gprc_checkpoint_data.transactions {
        if let Some(gprc_tx_content) = gprc_tx_wrapper.content {
            match gprc_tx_content {
                crate::proto::iota::gprc::v1::checkpoint_transaction_gprc::Content::FullTransaction(verified_tx_gprc) => {
                    let core_tx_data: TransactionData =
                        bcs::from_bytes(&verified_tx_gprc.raw_tx).map_err(|e| {
                            GrpcApiError::DeserializationError(format!("Failed to deserialize transaction data from gRPC: {}", e))
                        })?;

                    let sender_signed_data = SenderSignedData::new(core_tx_data, Vec::<GenericSignature>::new());
                    let core_tx = transaction::Transaction::new(sender_signed_data);

                    let dummy_effects = TransactionEffects::default();

                    core_transactions.push(CheckpointTransaction {
                        transaction: core_tx,
                        effects: dummy_effects,
                        events: None,
                        input_objects: Vec::new(),
                        output_objects: Vec::new(),
                    });
                }
                crate::proto::iota::gprc::v1::checkpoint_transaction_gprc::Content::DigestOnly(_) => {
                    return Err(GrpcApiError::ConversionError(
                        "Received transaction digest from gRPC, but full transaction data is required".to_string(),
                    ));
                }
            }
        }
    }
    Ok(CheckpointData {
        checkpoint_summary: core_checkpoint_envelope,
        checkpoint_contents: CheckpointContents::new_with_digests_only_for_tests(
            core_transactions.iter().map(|tx| ExecutionDigests {
                transaction: *tx.transaction.digest(),
                effects: tx.effects.digest(),
            }),
        ),
        transactions: core_transactions,
    })
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use iota_types::{
        base_types::{ExecutionDigests as CoreExecutionDigests, IotaAddress, ObjectID, ObjectRef},
        crypto::{
            AuthorityStrongQuorumSignInfo as CoreAuthorityStrongQuorumSignInfo,
            Signature as CoreSignature,
        },
        digests::{
            CheckpointContentsDigest as CoreCheckpointContentsDigest,
            CheckpointDigest as CoreCheckpointDigest, ObjectDigest as CoreObjectDigest,
        },
        effects::TransactionEffects as CoreTransactionEffects,
        full_checkpoint_content::{
            CheckpointData as CoreCheckpointData,
            CheckpointTransaction as CoreCheckpointTransaction,
        },
        gas::GasCostSummary as CoreGasCostSummary,
        message_envelope::{Envelope as CoreEnvelope, Message as CoreMessage},
        messages_checkpoint::{
            CheckpointContents as CoreCheckpointContents,
            CheckpointSummary as CoreCheckpointSummary,
        },
        signature::GenericSignature as CoreGenericSignature,
        transaction::{
            SenderSignedData as CoreSenderSignedData, Transaction as CoreTransaction,
            TransactionData as CoreTransactionData,
        },
    };
    use roaring::RoaringBitmap;

    use super::*;

    // Helper to create a mock ObjectID
    fn mock_object_id() -> ObjectID {
        ObjectID::from_str("0x0000000000000000000000000000000000000000000000000000000000000001")
            .unwrap()
    }

    // Helper to create a mock IotaAddress
    fn mock_iota_address() -> IotaAddress {
        IotaAddress::from(mock_object_id())
    }

    // Helper to create a mock ObjectRef
    fn mock_object_ref() -> ObjectRef {
        (mock_object_id(), 0.into(), CoreObjectDigest::random())
    }

    fn create_mock_core_checkpoint_data(seq_num: u64) -> CoreCheckpointData {
        let summary_data = CoreCheckpointSummary {
            epoch: 0,
            sequence_number: seq_num,
            network_total_transactions: 100 + seq_num,
            content_digest: CoreCheckpointContentsDigest::random(),
            previous_digest: if seq_num > 0 {
                Some(CoreCheckpointDigest::random())
            } else {
                None
            },
            epoch_rolling_gas_cost_summary: CoreGasCostSummary::default(),
            timestamp_ms: 123456789000 + seq_num * 1000,
            checkpoint_commitments: Default::default(),
            end_of_epoch_data: None,
            version_specific_data: Default::default(),
        };

        let dummy_sig = CoreAuthorityStrongQuorumSignInfo {
            epoch: summary_data.epoch,
            signature: crypto::AggregateAuthoritySignature::default(),
            signers_map: RoaringBitmap::new(),
        };
        let verified_summary_envelope =
            CoreEnvelope::new_from_data_and_sig(summary_data.clone(), dummy_sig.clone());

        let mut transactions = Vec::new();
        if seq_num % 2 == 0 {
            let tx_data = CoreTransactionData::new_transfer(
                mock_iota_address(),
                mock_object_ref(),
                mock_iota_address(),
                mock_object_ref(),
                100,
                10,
            );
            let sender_signed_data =
                CoreSenderSignedData::new(tx_data, Vec::<CoreGenericSignature>::new());
            let core_tx = CoreTransaction::new(sender_signed_data);
            let core_effects = CoreTransactionEffects::default();

            transactions.push(CoreCheckpointTransaction {
                transaction: core_tx,
                effects: core_effects,
                events: None,
                input_objects: Vec::new(),
                output_objects: Vec::new(),
            });
        }

        let checkpoint_contents = CoreCheckpointContents::new_with_digests_only_for_tests(
            transactions.iter().map(|tx| CoreExecutionDigests {
                transaction: *tx.transaction.digest(),
                effects: tx.effects.digest(),
            }),
        );

        CoreCheckpointData {
            checkpoint_summary: verified_summary_envelope,
            checkpoint_contents,
            transactions,
        }
    }

    #[test]
    fn test_checkpoint_data_round_trip() {
        let original_core_data = create_mock_core_checkpoint_data(1);

        let gprc_data_result = convert_full_checkpoint_data_to_gprc(&original_core_data);
        assert!(
            gprc_data_result.is_ok(),
            "Core to gRPC conversion failed: {:?}",
            gprc_data_result.err()
        );
        let gprc_data = gprc_data_result.unwrap();

        let converted_core_data_result = convert_checkpoint_data_gprc_to_core(gprc_data);
        assert!(
            converted_core_data_result.is_ok(),
            "gRPC to Core conversion failed: {:?}",
            converted_core_data_result.err()
        );
        let converted_core_data = converted_core_data_result.unwrap();

        assert_eq!(
            original_core_data.checkpoint_summary.data().epoch,
            converted_core_data.checkpoint_summary.data().epoch
        );
        assert_eq!(
            original_core_data.checkpoint_summary.data().sequence_number,
            converted_core_data
                .checkpoint_summary
                .data()
                .sequence_number
        );
        assert_eq!(
            original_core_data
                .checkpoint_summary
                .data()
                .network_total_transactions,
            converted_core_data
                .checkpoint_summary
                .data()
                .network_total_transactions
        );
        assert_eq!(
            original_core_data.checkpoint_summary.data().content_digest,
            converted_core_data.checkpoint_summary.data().content_digest
        );
        assert_eq!(
            original_core_data.checkpoint_summary.data().previous_digest,
            converted_core_data
                .checkpoint_summary
                .data()
                .previous_digest
        );

        assert_eq!(
            original_core_data.transactions.len(),
            converted_core_data.transactions.len()
        );

        for (original_tx, converted_tx) in original_core_data
            .transactions
            .iter()
            .zip(converted_core_data.transactions.iter())
        {
            assert_eq!(
                original_tx.transaction.digest(),
                converted_tx.transaction.digest()
            );
            assert_eq!(original_tx.effects.digest(), converted_tx.effects.digest());
        }

        assert_eq!(
            original_core_data.checkpoint_contents.digest(),
            converted_core_data.checkpoint_contents.digest()
        );
    }
}
