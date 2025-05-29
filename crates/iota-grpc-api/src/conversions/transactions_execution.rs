// Placeholder for transactions_execution conversions

use iota_types::{
    digests::TransactionDigest,
    effects::TransactionEffectsAPI,
    message_envelope::Message,
    quorum_driver_types::QuorumDriverResponse,
    transaction::SignedTransaction, // For parsing the request
};

use crate::{
    error::GrpcApiError,
    proto::iota::gprc::v1::{StringU64, TransactionExecutionResponseGprc},
};

// Status codes for TransactionExecutionResponseGprc
const EXEC_STATUS_SUCCESS: u32 = 0;
// Error status codes might be set by the service layer based on the Result from
// state_reader const EXEC_STATUS_GAS_ERROR: u32 = 1;
// const EXEC_STATUS_OTHER_ERROR: u32 = 2;

pub fn convert_raw_bytes_to_signed_transaction(
    raw_bytes: &[u8],
) -> Result<SignedTransaction, GrpcApiError> {
    bcs::from_bytes::<SignedTransaction>(raw_bytes).map_err(|e| {
        GrpcApiError::InvalidArgument(format!(
            "Failed to parse raw_transaction_bytes into SignedTransaction: {}",
            e
        ))
    })
}

// This function now assumes it's called upon successful execution resulting in
// a QuorumDriverResponse. Error handling (mapping QuorumDriverError or other
// errors to gRPC status) will be done in the service.
pub fn convert_successful_execution_to_gprc(
    core_response: &QuorumDriverResponse,
    transaction_id: &TransactionDigest,
) -> TransactionExecutionResponseGprc {
    let transaction_id_hex = transaction_id.to_string();
    let effects = &core_response.effects_cert.data(); // MODIFIED: Access effects via .data()
    let gas_summary = effects.gas_cost_summary();

    let created_object_ids: Vec<String> = effects
        .created()
        .iter()
        .map(|(obj_ref, _owner)| obj_ref.0.to_string())
        .collect();
    let mutated_object_ids: Vec<String> = effects
        .mutated()
        .iter()
        .map(|(obj_ref, _owner)| obj_ref.0.to_string())
        .collect();
    let deleted_object_ids: Vec<String> = effects
        .deleted()
        .iter()
        .map(|obj_ref| obj_ref.0.to_string())
        .collect();

    TransactionExecutionResponseGprc {
        transaction_id_hex,
        status_code: EXEC_STATUS_SUCCESS,
        status_message: "Transaction executed successfully".to_string(), // Default success message
        gas_used: Some(StringU64 {
            value: gas_summary.computation_cost.to_string(),
        }),
        effects_digest_hex: Some(effects.digest().to_string()),
        created_object_ids,
        deleted_object_ids,
        mutated_object_ids,
    }
}
