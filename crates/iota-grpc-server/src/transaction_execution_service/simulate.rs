// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use iota_grpc_types::{
    field::FieldMaskTree,
    merge::Merge,
    v0::{
        bcs::BcsData,
        command::{
            Argument as ProtoArgument, CommandOutput, CommandOutputs, CommandResult,
            CommandResults, argument,
        },
        transaction::{
            ExecutedTransaction, Transaction as ProtoTransaction,
            TransactionEffects as ProtoTransactionEffects,
            TransactionEvents as ProtoTransactionEvents,
        },
        transaction_execution_service::{
            SimulateTransactionRequest, SimulateTransactionResponse,
            simulate_transaction_request::TransactionCheckModes,
        },
        types::{TypeTag as ProtoTypeTag, TypeTagStruct, type_tag},
    },
};
use iota_types::{
    effects::TransactionEffectsAPI,
    execution::ExecutionResult,
    gas::GasCostSummary,
    transaction::TransactionDataAPI,
    transaction_executor::{TransactionExecutor, VmChecks},
};

use crate::{
    error::{ErrorReason, FieldViolation, Result, RpcError},
    types::GrpcReader,
};

pub fn simulate_transaction(
    reader: &Arc<GrpcReader>,
    executor: &Arc<dyn TransactionExecutor>,
    request: SimulateTransactionRequest,
) -> Result<SimulateTransactionResponse> {
    // Parse read mask
    let read_mask = request
        .read_mask
        .as_ref()
        .map(FieldMaskTree::from_field_mask)
        .unwrap_or_else(FieldMaskTree::new_wildcard);

    // Extract and validate transaction
    let transaction_proto = request
        .transaction
        .as_ref()
        .ok_or_else(|| FieldViolation::new("transaction").with_reason(ErrorReason::FieldMissing))?;

    let transaction_bcs = transaction_proto.bcs.as_ref().ok_or_else(|| {
        FieldViolation::new("transaction.bcs")
            .with_description("transaction BCS is required for simulation")
            .with_reason(ErrorReason::FieldMissing)
    })?;

    let mut transaction_data: iota_types::transaction::TransactionData =
        bcs::from_bytes(&transaction_bcs.data).map_err(|e| {
            FieldViolation::new("transaction.bcs")
                .with_description(format!("invalid transaction BCS: {e}"))
                .with_reason(ErrorReason::FieldInvalid)
        })?;

    // Determine VM checks from request
    let vm_checks = if request
        .tx_checks
        .contains(&(TransactionCheckModes::DisableVmChecks as i32))
    {
        VmChecks::Disabled
    } else {
        VmChecks::Enabled
    };

    // Perform gas budget estimation if requested
    if request.estimate_gas_budget.unwrap_or(false) {
        // Run simulation to get gas cost
        let estimation_result = executor
            .simulate_transaction(transaction_data.clone(), VmChecks::Enabled)
            .map_err(|e| {
                RpcError::new(
                    tonic::Code::Internal,
                    format!("transaction simulation for gas estimation failed: {e}"),
                )
            })?;

        let reference_gas_price = transaction_data.gas_data().price;
        let estimate = estimate_gas_budget_from_gas_cost(
            estimation_result.effects.gas_cost_summary(),
            reference_gas_price,
        );

        // Update transaction with estimated budget
        transaction_data.gas_data_mut().budget = estimate;
    }

    // Simulate the transaction
    let simulation_result = executor
        .simulate_transaction(transaction_data.clone(), vm_checks)
        .map_err(|e| {
            RpcError::new(
                tonic::Code::Internal,
                format!("transaction simulation failed: {e}"),
            )
        })?;

    let effects = simulation_result.effects;
    let events = simulation_result.events;
    let execution_result = simulation_result.execution_result;

    // Build the response
    let mut response = SimulateTransactionResponse::default();

    // Build executed transaction if requested
    if let Some(tx_mask) = read_mask.subtree("transaction") {
        let mut executed_transaction = ExecutedTransaction::default();

        // Set digest
        if tx_mask.contains(ExecutedTransaction::DIGEST_FIELD.name) {
            // Calculate transaction digest using the transaction data's digest method
            let digest = transaction_data.digest();
            executed_transaction.digest = Some(iota_grpc_types::v0::types::Digest {
                digest: digest.into_inner().to_vec().into(),
            });
        }

        // Set transaction BCS (includes updated gas budget if estimation was requested)
        if tx_mask
            .subtree(ExecutedTransaction::TRANSACTION_FIELD.name)
            .is_some()
        {
            executed_transaction.transaction = Some(ProtoTransaction {
                digest: None,
                bcs: Some(BcsData {
                    data: bcs::to_bytes(&transaction_data)
                        .map_err(|e| {
                            RpcError::new(
                                tonic::Code::Internal,
                                format!("failed to serialize transaction: {e}"),
                            )
                        })?
                        .into(),
                }),
            });
        }

        // Set effects
        if let Some(effects_mask) = tx_mask.subtree(ExecutedTransaction::EFFECTS_FIELD.name) {
            let mut proto_effects = ProtoTransactionEffects::default();
            proto_effects.merge(&effects, &effects_mask);
            executed_transaction.effects = Some(proto_effects);
        }

        // Set events
        if let Some(events_mask) = tx_mask.subtree(ExecutedTransaction::EVENTS_FIELD.name) {
            if let Some(events) = &events {
                let mut proto_events = ProtoTransactionEvents::default();
                proto_events.merge(events, &events_mask);
                executed_transaction.events = Some(proto_events);
            }
        }

        response.transaction = Some(executed_transaction);
    }

    // Build command results if requested
    if read_mask.contains("command_results") {
        let command_results = build_command_results(reader, execution_result)?;
        response.command_results = Some(command_results);
    }

    Ok(response)
}

fn build_command_results(
    _reader: &Arc<GrpcReader>,
    execution_result: std::result::Result<Vec<ExecutionResult>, iota_types::error::ExecutionError>,
) -> Result<CommandResults> {
    let mut results = CommandResults::default();

    match execution_result {
        Ok(execution_results) => {
            results.results = execution_results
                .into_iter()
                .map(|(mutable_reference_outputs, return_values)| {
                    let mut command_result = CommandResult::default();

                    // Process return values
                    let return_outputs: Vec<CommandOutput> = return_values
                        .into_iter()
                        .map(|(bcs_bytes, tt)| CommandOutput {
                            argument: None,
                            type_tag: Some(ProtoTypeTag {
                                type_tag: Some(type_tag::TypeTag::StructTag(TypeTagStruct {
                                    struct_tag: tt.to_canonical_string(true),
                                })),
                            }),
                            bcs: Some(BcsData {
                                data: bcs_bytes.into(),
                            }),
                        })
                        .collect();
                    command_result.return_values = Some(CommandOutputs {
                        outputs: return_outputs,
                    });

                    // Process mutable reference outputs
                    let mutated_outputs: Vec<CommandOutput> = mutable_reference_outputs
                        .into_iter()
                        .map(|(arg, bcs_bytes, tt)| CommandOutput {
                            argument: Some(convert_argument(arg)),
                            type_tag: Some(ProtoTypeTag {
                                type_tag: Some(type_tag::TypeTag::StructTag(TypeTagStruct {
                                    struct_tag: tt.to_canonical_string(true),
                                })),
                            }),
                            bcs: Some(BcsData {
                                data: bcs_bytes.into(),
                            }),
                        })
                        .collect();
                    command_result.mutated_by_ref = Some(CommandOutputs {
                        outputs: mutated_outputs,
                    });

                    command_result
                })
                .collect();
        }
        Err(e) => {
            // If execution failed, return empty results with error info
            // The error is captured in the effects status
            tracing::debug!("Simulation execution failed: {e}");
        }
    }

    Ok(results)
}

fn convert_argument(arg: iota_types::transaction::Argument) -> ProtoArgument {
    match arg {
        iota_types::transaction::Argument::GasCoin => ProtoArgument {
            kind: Some(argument::Kind::GasCoin(argument::GasCoin {})),
        },
        iota_types::transaction::Argument::Input(idx) => ProtoArgument {
            kind: Some(argument::Kind::Input(argument::Input {
                index: Some(idx as u32),
            })),
        },
        iota_types::transaction::Argument::Result(idx) => ProtoArgument {
            kind: Some(argument::Kind::Result(argument::Result {
                index: Some(idx as u32),
                nested_result_index: None,
            })),
        },
        iota_types::transaction::Argument::NestedResult(idx, nested_idx) => ProtoArgument {
            kind: Some(argument::Kind::Result(argument::Result {
                index: Some(idx as u32),
                nested_result_index: Some(nested_idx as u32),
            })),
        },
    }
}

/// Estimate the gas budget using the gas_cost_summary from a previous
/// simulation
///
/// The estimated gas budget is computed as following:
/// * the maximum between A and B, where: A = computation cost +
///   GAS_SAFE_OVERHEAD * reference gas price B = computation cost + storage
///   cost - storage rebate + GAS_SAFE_OVERHEAD * reference gas price
///
/// This gas estimate is computed similarly as in the TypeScript SDK and SUI's
/// implementation
fn estimate_gas_budget_from_gas_cost(
    gas_cost_summary: &GasCostSummary,
    reference_gas_price: u64,
) -> u64 {
    const GAS_SAFE_OVERHEAD: u64 = 1000;

    let safe_overhead = GAS_SAFE_OVERHEAD * reference_gas_price;
    let computation_cost_with_overhead = gas_cost_summary.computation_cost + safe_overhead;

    let gas_usage = gas_cost_summary.net_gas_usage() + safe_overhead as i64;
    computation_cost_with_overhead.max(if gas_usage < 0 { 0 } else { gas_usage as u64 })
}
