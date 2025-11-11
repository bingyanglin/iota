// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use crate::error::{ErrorReason, FieldViolation, Result};
use crate::types::GrpcReader;
use crate::RpcError;
use iota_grpc_types::field::{FieldMaskTree, FieldMaskUtil};
use iota_grpc_types::merge::Merge;
use iota_grpc_types::v0::transaction::ExecutedTransaction;
use iota_grpc_types::v0::transaction_execution_service::{
    transaction_execution_service_server::TransactionExecutionService, ExecuteTransactionRequest,
    ExecuteTransactionResponse, SimulateTransactionRequest, SimulateTransactionResponse,
};
use iota_types::transaction_executor::TransactionExecutor;

// Helper function to derive balance changes from effects and objects
fn derive_balance_changes(
    effects: &iota_types::effects::TransactionEffects,
    input_objects: &[iota_types::object::Object],
    output_objects: &[iota_types::object::Object],
) -> Vec<iota_types::balance_change::BalanceChange> {
    let mut balance_changes = Vec::new();

    // Process gas used
    let gas_summary = effects.gas_cost_summary();
    if let Some(gas_owner) = effects.gas_object().0.owner.get_owner_address() {
        balance_changes.push(iota_types::balance_change::BalanceChange {
            owner: gas_owner,
            coin_type: iota_types::gas_coin::GAS::type_().into(),
            amount: -(gas_summary.net_gas_usage() as i128),
        });
    }

    balance_changes
}
use prost_types::FieldMask;
use tap::Pipe;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

mod simulate;

pub struct TransactionExecutionGrpcService<E: TransactionExecutor> {
    pub reader: Arc<GrpcReader>,
    pub executor: Arc<E>,
    pub cancellation_token: CancellationToken,
}

impl<E: TransactionExecutor> TransactionExecutionGrpcService<E> {
    pub fn new(
        reader: Arc<GrpcReader>,
        executor: Arc<E>,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            reader,
            executor,
            cancellation_token,
        }
    }
}

#[tonic::async_trait]
impl<E: TransactionExecutor + 'static> TransactionExecutionService
    for TransactionExecutionGrpcService<E>
{
    async fn execute_transaction(
        &self,
        request: Request<ExecuteTransactionRequest>,
    ) -> std::result::Result<Response<ExecuteTransactionResponse>, Status> {
        execute_transaction(&self.reader, &*self.executor, request.into_inner())
            .await
            .map(Response::new)
            .map_err(Into::into)
    }

    async fn simulate_transaction(
        &self,
        request: Request<SimulateTransactionRequest>,
    ) -> std::result::Result<Response<SimulateTransactionResponse>, Status> {
        simulate::simulate_transaction(&self.reader, &*self.executor, request.into_inner())
            .map(Response::new)
            .map_err(Into::into)
    }
}

pub const EXECUTE_TRANSACTION_READ_MASK_DEFAULT: &str = "effects.status,checkpoint";

#[tracing::instrument(skip(reader, executor))]
pub async fn execute_transaction<E: TransactionExecutor>(
    reader: &GrpcReader,
    executor: &E,
    request: ExecuteTransactionRequest,
) -> Result<ExecuteTransactionResponse> {
    let transaction = request
        .transaction
        .as_ref()
        .ok_or_else(|| FieldViolation::new("transaction").with_reason(ErrorReason::FieldMissing))?
        .pipe(iota_sdk2::types::Transaction::try_from)
        .map_err(|e| {
            FieldViolation::new("transaction")
                .with_description(format!("invalid transaction: {e}"))
                .with_reason(ErrorReason::FieldInvalid)
        })?;

    let signatures = request
        .signatures
        .as_ref()
        .ok_or_else(|| FieldViolation::new("signatures").with_reason(ErrorReason::FieldMissing))?
        .signatures
        .iter()
        .enumerate()
        .map(|(i, signature)| {
            iota_sdk2::types::UserSignature::try_from(signature).map_err(|e| {
                FieldViolation::new_at("signatures", i)
                    .with_description(format!("invalid signature: {e}"))
                    .with_reason(ErrorReason::FieldInvalid)
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let signed_transaction = iota_sdk2::types::SignedTransaction {
        transaction: transaction.clone(),
        signatures: signatures.clone(),
    };

    let read_mask = {
        let read_mask = request
            .read_mask
            .unwrap_or_else(|| FieldMask::from_str(EXECUTE_TRANSACTION_READ_MASK_DEFAULT));
        read_mask
            .validate::<ExecutedTransaction>()
            .map_err(|path| {
                FieldViolation::new("read_mask")
                    .with_description(format!("invalid read_mask path: {path}"))
                    .with_reason(ErrorReason::FieldInvalid)
            })?;
        FieldMaskTree::from(read_mask)
    };

    let request = iota_types::quorum_driver_types::ExecuteTransactionRequestV1 {
        transaction: signed_transaction.try_into()?,
        include_events: read_mask.contains(ExecutedTransaction::EVENTS_FIELD.name),
        include_input_objects: read_mask.contains(ExecutedTransaction::BALANCE_CHANGES_FIELD.name)
            || read_mask.contains(ExecutedTransaction::OBJECTS_FIELD.name)
            || read_mask.contains(ExecutedTransaction::EFFECTS_FIELD.name),
        include_output_objects: read_mask.contains(ExecutedTransaction::BALANCE_CHANGES_FIELD.name)
            || read_mask.contains(ExecutedTransaction::OBJECTS_FIELD.name)
            || read_mask.contains(ExecutedTransaction::EFFECTS_FIELD.name),
        include_auxiliary_data: false,
    };

    let iota_types::quorum_driver_types::ExecuteTransactionResponseV1 {
        effects:
            iota_types::quorum_driver_types::FinalizedEffects {
                effects,
                finality_info: _,
            },
        events,
        input_objects,
        output_objects,
        auxiliary_data: _,
    } = executor
        .execute_transaction(request, None)
        .await
        .map_err(|e| RpcError::new(tonic::Code::Internal, format!("execution failed: {e}")))?;

    let executed_transaction = {
        let events = read_mask
            .subtree(ExecutedTransaction::EVENTS_FIELD)
            .and_then(|mask| {
                events.map(|e| {
                    iota_grpc_types::v0::transaction::TransactionEvents::merge_from(&e, &mask)
                })
            });

        let input_objects = input_objects.unwrap_or_default();
        let output_objects = output_objects.unwrap_or_default();

        let balance_changes = read_mask
            .contains(ExecutedTransaction::BALANCE_CHANGES_FIELD.name)
            .then(|| {
                derive_balance_changes(&effects, &input_objects, &output_objects)
                    .into_iter()
                    .map(Into::into)
                    .collect()
            })
            .unwrap_or_default();

        let input_objects = input_objects
            .into_iter()
            .map(iota_sdk2::types::Object::try_from)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let output_objects = output_objects
            .into_iter()
            .map(iota_sdk2::types::Object::try_from)
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let effects_sdk = iota_sdk2::types::TransactionEffects::try_from(effects)?;
        let effects = read_mask
            .subtree(ExecutedTransaction::EFFECTS_FIELD.name)
            .map(|mask| {
                let mut effects =
                    iota_grpc_types::v0::transaction::TransactionEffects::merge_from(
                        &effects_sdk,
                        &mask,
                    );

                if mask.contains(
                    iota_grpc_types::v0::transaction::TransactionEffects::CHANGED_OBJECTS_FIELD
                        .name,
                ) {
                    for changed_object in effects.changed_objects.iter_mut() {
                        let Ok(object_id) =
                            changed_object.object_id().parse::<iota_sdk2::types::Address>()
                        else {
                            continue;
                        };

                        if let Some(object) = input_objects
                            .iter()
                            .chain(&output_objects)
                            .find(|o| o.object_id() == object_id)
                        {
                            changed_object.object_type = Some(match object.object_type() {
                                iota_sdk2::types::ObjectType::Package => "package".to_owned(),
                                iota_sdk2::types::ObjectType::Struct(struct_tag) => {
                                    struct_tag.to_string()
                                }
                            });
                        }
                    }
                }

                if mask.contains(
                    iota_grpc_types::v0::transaction::TransactionEffects::UNCHANGED_CONSENSUS_OBJECTS_FIELD.name,
                ) {
                    for unchanged_consensus_object in effects.unchanged_consensus_objects.iter_mut()
                    {
                        let Ok(object_id) =
                            unchanged_consensus_object
                                .object_id()
                                .parse::<iota_sdk2::types::Address>()
                        else {
                            continue;
                        };

                        if let Some(object) =
                            input_objects.iter().find(|o| o.object_id() == object_id)
                        {
                            unchanged_consensus_object.object_type =
                                Some(match object.object_type() {
                                    iota_sdk2::types::ObjectType::Package => "package".to_owned(),
                                    iota_sdk2::types::ObjectType::Struct(struct_tag) => {
                                        struct_tag.to_string()
                                    }
                                });
                        }
                    }
                }

                effects
            });

        let mut message = ExecutedTransaction::default();
        message.digest = read_mask
            .contains(ExecutedTransaction::DIGEST_FIELD.name)
            .then(|| transaction.digest().to_string());
        message.transaction = read_mask
            .subtree(ExecutedTransaction::TRANSACTION_FIELD.name)
            .map(|mask| iota_grpc_types::v0::transaction::Transaction::merge_from(transaction, &mask));
        message.signatures = read_mask
            .subtree(ExecutedTransaction::SIGNATURES_FIELD.name)
            .map(|mask| {
                Some(iota_grpc_types::v0::signatures::UserSignatures {
                    signatures: signatures
                        .into_iter()
                        .map(|s| iota_grpc_types::v0::signatures::UserSignature::merge_from(s, &mask))
                        .collect(),
                })
            })
            .flatten();
        message.effects = effects;
        message.events = events;
        message.balance_changes = balance_changes;
        message.objects = read_mask
            .subtree(
                ExecutedTransaction::path_builder()
                    .objects()
                    .objects()
                    .finish(),
            )
            .map(|mask| {
                let set: std::collections::BTreeMap<_, _> = input_objects
                    .into_iter()
                    .chain(output_objects.into_iter())
                    .map(|object| ((object.object_id(), object.version()), object))
                    .collect();
                iota_grpc_types::v0::transaction::ObjectSet::default().with_objects(
                    set.into_values()
                        .map(|o| iota_grpc_types::v0::object::Object::merge_from(o, &mask))
                        .collect(),
                )
            });
        message
    };

    Ok(ExecuteTransactionResponse::default().with_transaction(executed_transaction))
}
