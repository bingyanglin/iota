use bcs;
use iota_types::{
    full_checkpoint_content::CheckpointData, messages_checkpoint::VerifiedCheckpoint,
};

use crate::{
    error::GrpcApiError,
    proto::iota::gprc::v1::{
        CheckpointDataGprc, CheckpointDigestGprc, CheckpointTransactionGprc,
        SignedCheckpointSummaryGprc, VerifiedTransactionGprc,
    },
};

// Helper to convert Option<&iota_types::digests::CheckpointDigest> to
// Option<CheckpointDigestGprc>
fn convert_option_core_digest_to_gprc(
    core_digest_opt: Option<&iota_types::digests::CheckpointDigest>,
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
