// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::error::{ErrorReason, FieldViolation, Result};
use crate::types::GrpcReader;
use crate::RpcError;
use iota_grpc_types::field::FieldMaskTree;
use iota_grpc_types::merge::Merge;
use iota_grpc_types::v0::transaction::ExecutedTransaction;
use iota_grpc_types::v0::transaction_execution_service::{
    SimulateTransactionRequest, SimulateTransactionResponse,
};
use iota_protocol_config::ProtocolConfig;
use iota_types::base_types::{ObjectID, ObjectRef, IotaAddress};
use iota_types::effects::TransactionEffectsAPI;
use iota_types::transaction::TransactionDataAPI;
use iota_types::transaction_executor::TransactionExecutor;
use itertools::Itertools;

mod resolve;

pub fn simulate_transaction<E: TransactionExecutor>(
    reader: &GrpcReader,
    executor: &E,
    request: SimulateTransactionRequest,
) -> Result<SimulateTransactionResponse> {
    let read_mask = request
        .read_mask
        .as_ref()
        .map(FieldMaskTree::from_field_mask)
        .unwrap_or_else(FieldMaskTree::new_wildcard);

    let transaction_proto = request
        .transaction
        .as_ref()
        .ok_or_else(|| FieldViolation::new("transaction").with_reason(ErrorReason::FieldMissing))?;

    // TODO: Implement transaction checks from request.tx_checks
    // For now, we'll use default checks

    // TODO: Make this more efficient
    let (reference_gas_price, protocol_config) = {
        let system_state = reader.get_system_state_summary()?;
        let protocol_config = ProtocolConfig::get_for_version_if_supported(
            system_state.protocol_version.into(),
            reader.get_chain_identifier()?.chain(),
        )
        .ok_or_else(|| {
            RpcError::new(
                tonic::Code::Internal,
                "unable to get current protocol config",
            )
        })?;

        (system_state.reference_gas_price, protocol_config)
    };

    // Try to parse out a fully-formed transaction. If one wasn't provided then we will attempt to
    // perform transaction resolution.
    let mut transaction = match iota_sdk2::types::Transaction::try_from(transaction_proto) {
        Ok(transaction) => iota_types::transaction::TransactionData::try_from(transaction)?,

        // If we weren't able to parse out a fully-formed transaction and the client provided BCS
        // TransactionData, then we'll error out early since we're unable to perform resolution
        // given a BCS payload
        Err(e) if transaction_proto.bcs.is_some() => {
            return Err(FieldViolation::new("transaction")
                .with_description(format!("invalid transaction: {e}"))
                .with_reason(ErrorReason::FieldInvalid)
                .into())
        }

        // We weren't able to parse out a fully-formed transaction so we'll attempt to perform
        // transaction resolution
        _ => resolve::resolve_transaction(
            reader,
            transaction_proto,
            reference_gas_price,
            &protocol_config,
        )?,
    };

    // Perform budget estimation and gas selection if requested
    if request.estimate_gas_budget() {
        // At this point, the budget on the transaction can be set to one of the following:
        // - The budget from the request, if specified.
        // - The total balance of all of the gas payment coins (clamped to the protocol
        //   MAX_GAS_BUDGET) in the request if the budget was not
        //   specified but the gas payment coins were specified.
        // - Protocol MAX_GAS_BUDGET if the request did not specified neither gas payment or budget.
        //
        // If the request did not specify a budget, then simulate the transaction to get a budget estimate and
        // overwrite the resolved budget with the more accurate estimate.
        if request.transaction().gas_payment().budget.is_none()
            && request.transaction().bcs_opt().is_none()
        {
            let simulation_result = executor
                .simulate_transaction(transaction.clone())
                .map_err(|e| RpcError::new(tonic::Code::Internal, format!("simulation failed: {e}")))?;

            let estimate = estimate_gas_budget_from_gas_cost(
                simulation_result.effects.gas_cost_summary(),
                reference_gas_price,
            );

            // If the request specified gas payment, then transaction.gas_data().budget should have been
            // resolved to the cumulative balance of those coins. We don't want to return a resolved transaction
            // where the gas payment can't satisfy the budget, so validate that balance can actually cover the
            // estimated budget.
            let gas_balance = transaction.gas_data().budget;
            if gas_balance < estimate {
                return Err(RpcError::new(
                    tonic::Code::InvalidArgument,
                    format!("Insufficient gas balance to cover estimated transaction cost. \
                        Available gas balance: {gas_balance} NANOS. Estimated gas budget required: {estimate} NANOS"),
                ));
            }
            transaction.gas_data_mut().budget = estimate;
        }

        if transaction.gas_data().payment.is_empty() {
            let input_objects = transaction
                .input_objects()
                .map_err(|e| RpcError::new(tonic::Code::Internal, format!("failed to get input objects: {e}")))?
                .iter()
                .flat_map(|obj| match obj {
                    iota_types::transaction::InputObjectKind::ImmOrOwnedMoveObject((id, _, _)) => {
                        Some(*id)
                    }
                    _ => None,
                })
                .collect_vec();
            let gas_coins = select_gas(
                reader,
                transaction.gas_data().owner,
                transaction.gas_data().budget,
                protocol_config.max_gas_payment_objects(),
                &input_objects,
            )?;
            transaction.gas_data_mut().payment = gas_coins;
        }
    }

    let simulation_result = executor
        .simulate_transaction(transaction.clone())
        .map_err(|e| RpcError::new(tonic::Code::Internal, format!("simulation failed: {e}")))?;

    let iota_types::transaction_executor::SimulateTransactionResult {
        effects,
        events,
        input_objects,
        output_objects,
        mock_gas_id: _,
    } = simulation_result;

    // Merge input and output objects for balance changes and object lookups
    let all_objects: Vec<_> = input_objects
        .into_values()
        .chain(output_objects.into_values())
        .collect();

    let transaction = if let Some(submask) = read_mask.subtree("transaction") {
        let mut message = ExecutedTransaction::default();
        let transaction_sdk = iota_sdk2::types::Transaction::try_from(transaction)?;

        message.balance_changes = submask
            .contains(ExecutedTransaction::BALANCE_CHANGES_FIELD.name)
            .then(|| {
                // Derive balance changes from gas cost
                let mut balance_changes = Vec::new();
                let gas_summary = effects.gas_cost_summary();
                if let Some(gas_owner) = effects.gas_object().0.owner.get_owner_address() {
                    balance_changes.push(iota_types::balance_change::BalanceChange {
                        owner: gas_owner,
                        coin_type: iota_types::gas_coin::GAS::type_().into(),
                        amount: -(gas_summary.net_gas_usage() as i128),
                    });
                }
                balance_changes
                    .into_iter()
                    .map(Into::into)
                    .collect()
            })
            .unwrap_or_default();

        message.effects = {
            let effects_sdk = iota_sdk2::types::TransactionEffects::try_from(effects)?;
            submask
                .subtree(ExecutedTransaction::EFFECTS_FIELD)
                .map(|mask| {
                    let mut effects_proto = iota_grpc_types::v0::transaction::TransactionEffects::merge_from(&effects_sdk, &mask);

                    if mask.contains(iota_grpc_types::v0::transaction::TransactionEffects::CHANGED_OBJECTS_FIELD.name) {
                        for changed_object in effects_proto.changed_objects.iter_mut() {
                            let Ok(object_id) = changed_object.object_id().parse::<ObjectID>()
                            else {
                                continue;
                            };

                            if let Some(object) = all_objects.iter().find(|o| o.id() == object_id) {
                                changed_object.object_type = Some(match object.struct_tag() {
                                    Some(struct_tag) => struct_tag.to_canonical_string(true),
                                    None => "package".to_owned(),
                                });
                            }
                        }
                    }

                    effects_proto
                })
        };

        message.events = submask
            .subtree(ExecutedTransaction::EVENTS_FIELD.name)
            .and_then(|mask| {
                events.map(|events| {
                    iota_sdk2::types::TransactionEvents::try_from(events)
                        .map(|events| iota_grpc_types::v0::transaction::TransactionEvents::merge_from(events, &mask))
                })
            })
            .transpose()?;

        message.transaction = submask
            .subtree(ExecutedTransaction::TRANSACTION_FIELD.name)
            .map(|mask| iota_grpc_types::v0::transaction::Transaction::merge_from(transaction_sdk, &mask));

        message.objects = submask
            .subtree(
                ExecutedTransaction::path_builder()
                    .objects()
                    .objects()
                    .finish(),
            )
            .map(|mask| {
                iota_grpc_types::v0::transaction::ObjectSet::default().with_objects(
                    all_objects
                        .iter()
                        .map(|o| iota_grpc_types::v0::object::Object::merge_from(o, &mask))
                        .collect(),
                )
            });

        Some(message)
    } else {
        None
    };

    // TODO: Implement command_results extraction if execution_result is available
    let command_results = if read_mask.contains(SimulateTransactionResponse::COMMAND_RESULTS_FIELD) {
        // For now, return empty. In SUI this requires execution_result from the simulator
        // which IOTA's SimulateTransactionResult doesn't currently expose
        Vec::new()
    } else {
        Vec::new()
    };

    let mut response = SimulateTransactionResponse::default();
    response.transaction = transaction;
    response.command_results = Some(iota_grpc_types::v0::command::CommandResults {
        results: command_results,
    });
    Ok(response)
}

/// Estimate the gas budget using the gas_cost_summary from a previous DryRun
///
/// The estimated gas budget is computed as following:
/// * the maximum between A and B, where:
///     A = computation cost + GAS_SAFE_OVERHEAD * reference gas price
///     B = computation cost + storage cost - storage rebate + GAS_SAFE_OVERHEAD * reference gas price
///     overhead
///
/// This gas estimate is computed similarly as in the TypeScript SDK
fn estimate_gas_budget_from_gas_cost(
    gas_cost_summary: &iota_types::gas::GasCostSummary,
    reference_gas_price: u64,
) -> u64 {
    const GAS_SAFE_OVERHEAD: u64 = 1000;

    let safe_overhead = GAS_SAFE_OVERHEAD * reference_gas_price;
    let computation_cost_with_overhead = gas_cost_summary.computation_cost + safe_overhead;

    let gas_usage = gas_cost_summary.net_gas_usage() + safe_overhead as i64;
    computation_cost_with_overhead.max(if gas_usage < 0 { 0 } else { gas_usage as u64 })
}

fn select_gas(
    reader: &GrpcReader,
    owner: IotaAddress,
    budget: u64,
    max_gas_payment_objects: u32,
    input_objects: &[ObjectID],
) -> Result<Vec<ObjectRef>> {
    use iota_types::gas_coin::GasCoin;

    let gas_coins = reader
        .indexes()
        .ok_or_else(|| RpcError::new(tonic::Code::NotFound, "indexes not available"))?
        .owned_objects_iter(owner, Some(GasCoin::type_()), None)
        .map_err(|e| RpcError::new(tonic::Code::Internal, format!("failed to iterate owned objects: {e}")))?
        .filter_ok(|info| !input_objects.contains(&info.object_id))
        .filter_map_ok(|info| reader.get_object(&info.object_id))
        // filter for objects which are not ConsensusAddress owned,
        // since only Address owned can be used for gas payments today
        .filter_ok(|object| !object.is_consensus())
        .filter_map_ok(|object| {
            GasCoin::try_from(&object)
                .ok()
                .map(|coin| (object.compute_object_reference(), coin.value()))
        })
        .take(max_gas_payment_objects as usize);

    let mut selected_gas = vec![];
    let mut selected_gas_value = 0;

    for maybe_coin in gas_coins {
        let (object_ref, value) =
            maybe_coin.map_err(|e| RpcError::new(tonic::Code::Internal, e.to_string()))?;
        selected_gas.push(object_ref);
        selected_gas_value += value;
    }

    if selected_gas_value >= budget {
        Ok(selected_gas)
    } else {
        Err(RpcError::new(
            tonic::Code::InvalidArgument,
            format!(
                "unable to select sufficient gas coins from account {owner} \
                    to satisfy required budget {budget}"
            ),
        ))
    }
}
