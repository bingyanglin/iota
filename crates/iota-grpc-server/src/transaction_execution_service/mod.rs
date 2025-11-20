// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

mod simulate;

use std::sync::Arc;

use iota_grpc_types::{
    field::FieldMaskTree,
    merge::Merge,
    transaction::TransactionReadSource,
    v0::{
        transaction::{ExecutedTransaction, TransactionEvents as ProtoTransactionEvents},
        transaction_execution_service::{
            self as grpc_tx_service, ExecuteTransactionRequest, ExecuteTransactionResponse,
            SimulateTransactionRequest, SimulateTransactionResponse,
        },
    },
};
use iota_types::{
    crypto::ToFromBytes, quorum_driver_types::ExecuteTransactionRequestV1,
    transaction_executor::TransactionExecutor,
};
use tonic::{Request, Response};

use crate::{
    error::{ErrorReason, FieldViolation, Result, RpcError},
    types::GrpcReader,
};

pub struct TransactionExecutionGrpcService {
    pub reader: Arc<GrpcReader>,
    pub executor: Arc<dyn TransactionExecutor>,
}

impl TransactionExecutionGrpcService {
    pub fn new(reader: Arc<GrpcReader>, executor: Arc<dyn TransactionExecutor>) -> Self {
        Self { reader, executor }
    }
}

#[tonic::async_trait]
impl grpc_tx_service::transaction_execution_service_server::TransactionExecutionService
    for TransactionExecutionGrpcService
{
    async fn execute_transaction(
        &self,
        request: Request<ExecuteTransactionRequest>,
    ) -> std::result::Result<Response<ExecuteTransactionResponse>, tonic::Status> {
        execute_transaction(&self.executor, request.into_inner())
            .await
            .map(Response::new)
            .map_err(Into::into)
    }

    async fn simulate_transaction(
        &self,
        request: Request<SimulateTransactionRequest>,
    ) -> std::result::Result<Response<SimulateTransactionResponse>, tonic::Status> {
        simulate::simulate_transaction(&self.reader, &self.executor, request.into_inner())
            .map(Response::new)
            .map_err(Into::into)
    }
}

pub const EXECUTE_TRANSACTION_READ_MASK_DEFAULT: &str = "effects";

#[tracing::instrument(skip(executor))]
pub async fn execute_transaction(
    executor: &Arc<dyn TransactionExecutor>,
    request: ExecuteTransactionRequest,
) -> Result<ExecuteTransactionResponse> {
    // Extract and validate transaction
    let transaction_proto = request
        .transaction
        .as_ref()
        .ok_or_else(|| FieldViolation::new("transaction").with_reason(ErrorReason::FieldMissing))?;

    let transaction_bcs = transaction_proto.bcs.as_ref().ok_or_else(|| {
        FieldViolation::new("transaction.bcs")
            .with_description("transaction BCS is required")
            .with_reason(ErrorReason::FieldMissing)
    })?;

    let transaction_data: iota_types::transaction::TransactionData =
        bcs::from_bytes(&transaction_bcs.data).map_err(|e| {
            FieldViolation::new("transaction.bcs")
                .with_description(format!("invalid transaction BCS: {e}"))
                .with_reason(ErrorReason::FieldInvalid)
        })?;

    // Extract and validate signatures
    let signatures_proto = request
        .signatures
        .as_ref()
        .ok_or_else(|| FieldViolation::new("signatures").with_reason(ErrorReason::FieldMissing))?;

    let signatures = signatures_proto
        .signatures
        .iter()
        .enumerate()
        .map(|(i, sig)| {
            let bcs_data = sig.bcs.as_ref().ok_or_else(|| {
                FieldViolation::new_at("signatures", i)
                    .with_description("signature BCS is required")
                    .with_reason(ErrorReason::FieldMissing)
            })?;

            iota_types::crypto::Signature::from_bytes(&bcs_data.data).map_err(|e| {
                FieldViolation::new_at("signatures", i)
                    .with_description(format!("invalid signature: {e}"))
                    .with_reason(ErrorReason::FieldInvalid)
            })
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;

    // Create signed transaction
    let signed_transaction =
        iota_types::transaction::Transaction::from_data(transaction_data, signatures);

    // Parse read mask
    let read_mask = request
        .read_mask
        .map(|mask| FieldMaskTree::from_field_mask(&mask))
        .unwrap_or_else(|| {
            EXECUTE_TRANSACTION_READ_MASK_DEFAULT
                .parse::<FieldMaskTree>()
                .unwrap()
        });

    // Determine what to include in the request based on read mask
    let include_events = read_mask.contains(ExecutedTransaction::EVENTS_FIELD.name);
    let include_input_objects = read_mask.contains(ExecutedTransaction::INPUT_OBJECTS_FIELD.name)
        || read_mask.contains(ExecutedTransaction::EFFECTS_FIELD.name);
    let include_output_objects = read_mask.contains(ExecutedTransaction::OUTPUT_OBJECTS_FIELD.name)
        || read_mask.contains(ExecutedTransaction::EFFECTS_FIELD.name);

    // Create execution request
    let exec_request = ExecuteTransactionRequestV1 {
        transaction: signed_transaction.clone(),
        include_events,
        include_input_objects,
        include_output_objects,
        include_auxiliary_data: false,
    };

    // Execute the transaction
    let exec_response = executor
        .execute_transaction(exec_request, None)
        .await
        .map_err(|e| {
            RpcError::new(
                tonic::Code::Internal,
                format!("transaction execution failed: {e}"),
            )
        })?;

    let effects = exec_response.effects.effects;
    let events = exec_response.events;

    // Get transaction digest
    let digest = *signed_transaction.digest();

    // For execute_transaction, checkpoint and timestamp are not available
    // immediately as the transaction is just being executed and not yet
    // included in a checkpoint
    let checkpoint = None;
    let timestamp_ms = None;

    // Build the response using merge
    let mut executed_transaction = ExecutedTransaction::default();

    let source = TransactionReadSource {
        digest,
        transaction: &Arc::new(iota_types::transaction::VerifiedTransaction::new_unchecked(
            signed_transaction,
        )),
        effects: &effects,
        events: events.as_ref(),
        checkpoint,
        timestamp_ms,
    };

    executed_transaction.merge(&source, &read_mask);

    // Handle events separately since they need special rendering
    if let Some(events_mask) = read_mask.subtree(ExecutedTransaction::EVENTS_FIELD.name) {
        if let Some(events) = &events {
            let mut proto_events = ProtoTransactionEvents::default();
            proto_events.merge(events, &events_mask);
            executed_transaction.events = Some(proto_events);
        }
    }

    Ok(ExecuteTransactionResponse {
        transaction: Some(executed_transaction),
    })
}
