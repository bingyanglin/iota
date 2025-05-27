use iota_types::{
    digests::TransactionDigest, // Added TransactionDigest import
    // base_types::TransactionDigest, // Will be needed if converting from digest + transaction
    transaction::VerifiedTransaction,
};

use crate::proto::iota::gprc::v1::TransactionGprc;
// use bcs; // If direct BCS serialization is needed for raw_transaction

// Converts a VerifiedTransaction (from iota-types) to a TransactionGprc
// (protobuf type)
pub fn convert_verified_transaction_to_gprc(
    transaction_id: &TransactionDigest, // Parameter is &TransactionDigest
    _core_transaction: &VerifiedTransaction, // Renamed to indicate it's not fully used yet
) -> Result<TransactionGprc, anyhow::Error> {
    // Placeholder for raw_transaction logic:
    // let raw_tx_data = bcs::to_bytes(core_transaction).unwrap_or_default(); // BCS
    // requires Serialize
    let raw_tx_data = format!("mock raw data for digest {:?}", transaction_id).into_bytes();

    Ok(TransactionGprc {
        transaction_id_hex: format!("{:#x}", transaction_id), /* Use the new field name, format
                                                               * digest to hex */
        payload_type: "VerifiedTransaction".to_string(), // Placeholder
        raw_transaction: raw_tx_data,
    })
}
