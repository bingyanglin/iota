use bcs;
use iota_types::{
    digests::TransactionDigest, // Added TransactionDigest import
    transaction::{TransactionDataAPI, TransactionKind, VerifiedTransaction},
};

use crate::proto::iota::gprc::v1::TransactionGprc; // If direct BCS serialization is needed for raw_transaction

// Converts a VerifiedTransaction (from iota-types) to a TransactionGprc
// (protobuf type)
pub fn convert_verified_transaction_to_gprc(
    transaction_id: &TransactionDigest, // Parameter is &TransactionDigest
    core_transaction: &VerifiedTransaction, // Renamed to indicate it's not fully used yet
) -> Result<TransactionGprc, anyhow::Error> {
    let raw_tx_data = bcs::to_bytes(core_transaction.data().transaction_data()).map_err(|e| {
        anyhow::anyhow!(
            "BCS serialization failed for core_transaction.data().transaction_data(): {}",
            e
        )
    })?;

    let payload_type = format!("{:?}", core_transaction.data().transaction_data().kind());

    let timestamp_ms = match core_transaction.data().transaction_data().kind() {
        TransactionKind::ConsensusCommitPrologueV1(prologue) => {
            // Assuming CheckpointTimestamp is u64 representing milliseconds
            prologue.commit_timestamp_ms
        }
        // Add other kinds if they have timestamps, e.g. some system transactions
        // For UserTransaction and others without an intrinsic timestamp in TransactionData:
        _ => 0, // Default to 0 if no specific timestamp is available
    };

    Ok(TransactionGprc {
        transaction_id_hex: format!("{:#x}", transaction_id), /* Use the new field name, format
                                                               * digest to hex */
        payload_type,                 // Use the determined payload type
        raw_transaction: raw_tx_data, // Use actual BCS serialized data
        timestamp_ms,
    })
}
