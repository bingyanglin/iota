// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use fastcrypto::traits::ToFromBytes;
use iota_core::{
    authority_client::NetworkAuthorityClient, transaction_orchestrator::TransactionOrchestrator,
};
use iota_json_rpc::{ObjectProviderCache, get_balance_changes_from_effect, get_object_changes};
use iota_json_rpc_types::{
    IotaTransactionBlock, IotaTransactionBlockEvents, IotaTransactionBlockResponse,
};
use iota_metrics::spawn_monitored_task;
use iota_types::{
    base_types::IotaAddress,
    effects::TransactionEffectsAPI,
    quorum_driver_types::{
        ExecuteTransactionRequestType, ExecuteTransactionRequestV1, ExecuteTransactionResponseV1,
        IsTransactionExecutedLocally,
    },
    signature::GenericSignature,
    storage::PostExecutionPackageResolver,
    transaction::{InputObjectKind, Transaction, TransactionData, TransactionDataAPI},
};
use tonic::{Request, Response, Status};
use tracing::{Instrument, debug, instrument};

use crate::{
    GrpcReader,
    write::{
        ExecuteTransactionRequest, ExecuteTransactionResponse, TransactionResponseOptions,
        write_service_server::WriteService,
    },
};

pub struct WriteGrpcService {
    /// Transaction orchestrator
    pub transaction_orchestrator: Option<Arc<TransactionOrchestrator<NetworkAuthorityClient>>>,
    /// GrpcReader for data access including epoch store when available
    pub grpc_reader: Arc<GrpcReader>,
}

impl WriteGrpcService {
    pub fn new(
        transaction_orchestrator: Option<Arc<TransactionOrchestrator<NetworkAuthorityClient>>>,
        grpc_reader: Arc<GrpcReader>,
    ) -> Self {
        Self {
            transaction_orchestrator,
            grpc_reader,
        }
    }

    /// Prepare transaction request
    #[expect(clippy::type_complexity)]
    fn prepare_execute_transaction_request(
        &self,
        tx_bytes: Vec<u8>,
        signatures: Vec<Vec<u8>>,
        opts: Option<TransactionResponseOptions>,
    ) -> Result<
        (
            ExecuteTransactionRequestV1,
            TransactionResponseOptions,
            IotaAddress,
            Vec<InputObjectKind>,
            Transaction,
            Option<IotaTransactionBlock>,
            Vec<u8>,
        ),
        Status,
    > {
        let opts = opts.unwrap_or_default();
        let tx_data: TransactionData = bcs::from_bytes(&tx_bytes)
            .map_err(|e| Status::invalid_argument(format!("Failed to deserialize: {e}")))?;
        let sender = tx_data.sender();
        let input_objs = tx_data.input_objects().unwrap_or_default();

        let mut sigs = Vec::new();
        for sig in signatures {
            let signature = GenericSignature::from_bytes(&sig)
                .map_err(|e| Status::invalid_argument(format!("Invalid signature: {e}")))?;
            sigs.push(signature);
        }
        let txn = Transaction::from_generic_sig_data(tx_data, sigs);
        let raw_transaction = if opts.show_raw_input {
            bcs::to_bytes(txn.data()).map_err(|e| {
                Status::internal(format!("Raw transaction serialization failed: {e}"))
            })?
        } else {
            vec![]
        };

        let transaction_block = if opts.show_input {
            if let Some(epoch_store) = self.grpc_reader.load_epoch_store_one_call_per_task() {
                Some(
                    IotaTransactionBlock::try_from(
                        txn.data().clone(),
                        epoch_store.module_cache(),
                        *txn.digest(),
                    )
                    .map_err(|e| {
                        Status::internal(format!("Failed to create IotaTransactionBlock: {e}"))
                    })?,
                )
            } else {
                return Err(Status::internal("Epoch store not available"));
            }
        } else {
            None
        };

        let request = ExecuteTransactionRequestV1 {
            transaction: txn.clone(),
            include_events: opts.show_events,
            include_input_objects: opts.show_balance_changes || opts.show_object_changes,
            include_output_objects: opts.show_balance_changes
                || opts.show_object_changes
                || opts.show_events,
            include_auxiliary_data: false,
        };

        Ok((
            request,
            opts,
            sender,
            input_objs,
            txn,
            transaction_block,
            raw_transaction,
        ))
    }

    /// Serialize IotaTransactionBlockResponse to JSON
    fn serialize_response_to_json(
        response: &IotaTransactionBlockResponse,
    ) -> Result<ExecuteTransactionResponse, Status> {
        let json_data = serde_json::to_string(response)
            .map_err(|e| Status::internal(format!("Failed to serialize response to JSON: {e}")))?;

        Ok(ExecuteTransactionResponse { json_data })
    }

    async fn handle_post_orchestration(
        &self,
        response: ExecuteTransactionResponseV1,
        is_executed_locally: IsTransactionExecutedLocally,
        opts: TransactionResponseOptions,
        digest: iota_types::base_types::TransactionDigest,
        input_objs: Vec<InputObjectKind>,
        transaction: Option<IotaTransactionBlock>,
        raw_transaction: Vec<u8>,
        sender: IotaAddress,
    ) -> Result<Response<ExecuteTransactionResponse>, Status> {
        let events = if opts.show_events {
            tracing::trace!("Resolving events");
            if let (Some(epoch_store), Some(authority_state)) = (
                self.grpc_reader.load_epoch_store_one_call_per_task(),
                self.grpc_reader.authority_state().as_ref(),
            ) {
                let backing_package_store = PostExecutionPackageResolver::new(
                    authority_state.get_backing_package_store().clone(),
                    &response.output_objects,
                );
                let mut layout_resolver = epoch_store
                    .executor()
                    .type_layout_resolver(Box::new(backing_package_store));
                Some(
                    IotaTransactionBlockEvents::try_from(
                        response.events.unwrap_or_default(),
                        digest,
                        None,
                        layout_resolver.as_mut(),
                    )
                    .map_err(|e| Status::internal(format!("Failed to convert events: {e}")))?,
                )
            } else {
                return Err(Status::internal(
                    "Cannot convert events: missing epoch store or authority state",
                ));
            }
        } else {
            None
        };

        let object_cache = {
            response.output_objects.and_then(|output_objects| {
                self.grpc_reader.authority_state().map(|authority_state| {
                    ObjectProviderCache::new_with_output_objects(
                        authority_state.clone(),
                        output_objects,
                    )
                })
            })
        };

        let balance_changes = match &object_cache {
            Some(object_cache) if opts.show_balance_changes => Some(
                get_balance_changes_from_effect(
                    object_cache,
                    &response.effects.effects,
                    input_objs,
                    None,
                )
                .instrument(tracing::trace_span!("resolving balance changes"))
                .await
                .map_err(|e| Status::internal(format!("Failed to get balance changes: {e}")))?,
            ),
            _ => None,
        };

        let object_changes = match &object_cache {
            Some(object_cache) if opts.show_object_changes => Some(
                get_object_changes(
                    object_cache,
                    sender,
                    response.effects.effects.modified_at_versions(),
                    response.effects.effects.all_changed_objects(),
                    response.effects.effects.all_removed_objects(),
                )
                .instrument(tracing::trace_span!("resolving object changes"))
                .await
                .map_err(|e| Status::internal(format!("Failed to get object changes: {e}")))?,
            ),
            _ => None,
        };

        let raw_effects = if opts.show_raw_effects {
            bcs::to_bytes(&response.effects.effects)
                .map_err(|e| Status::internal(format!("Raw effects serialization failed: {e}")))?
        } else {
            vec![]
        };

        let iota_response =
            IotaTransactionBlockResponse {
                digest,
                transaction,
                raw_transaction,
                effects: opts
                    .show_effects
                    .then(|| {
                        response.effects.effects.try_into().map_err(|e| {
                            Status::internal(format!("Failed to convert effects: {e}"))
                        })
                    })
                    .transpose()?,
                events,
                object_changes,
                balance_changes,
                timestamp_ms: None,
                confirmed_local_execution: Some(is_executed_locally),
                checkpoint: None,
                errors: vec![],
                raw_effects,
            };

        // Serialize to JSON
        let grpc_response = Self::serialize_response_to_json(&iota_response)?;

        debug!("Transaction executed successfully");
        Ok(Response::new(grpc_response))
    }
}

// The `WriteService` is the auto-generated trait from the protobuf definition.
// It's generated by tonic/protobuf and defines the interface that any gRPC
// write service must implement.
#[tonic::async_trait]
impl WriteService for WriteGrpcService {
    #[instrument("grpc_api_execute_transaction", level = "trace", skip_all)]
    async fn execute_transaction(
        &self,
        request: Request<ExecuteTransactionRequest>,
    ) -> Result<Response<ExecuteTransactionResponse>, Status> {
        let req = request.into_inner();

        let request_type = req
            .request_type
            .map(|rt| match rt {
                0 => ExecuteTransactionRequestType::WaitForEffectsCert,
                1 => ExecuteTransactionRequestType::WaitForLocalExecution,
                _ => ExecuteTransactionRequestType::WaitForEffectsCert, // fallback to default
            })
            .unwrap_or(ExecuteTransactionRequestType::WaitForEffectsCert);

        let (execute_request, opts, sender, input_objs, txn, transaction_block, raw_transaction) =
            self.prepare_execute_transaction_request(req.tx_bytes, req.signatures, req.options)?;

        let digest = *txn.digest();

        let orchestrator = self
            .transaction_orchestrator
            .clone()
            .ok_or_else(|| Status::unimplemented("Transaction execution not available"))?;

        tracing::trace!("Spawning transaction orchestrator task for transaction: {digest}",);
        let (response, is_executed_locally) = spawn_monitored_task!(
            orchestrator.execute_transaction_block(execute_request, request_type, None)
        )
        .await
        .map_err(|e| Status::internal(format!("Task execution failed: {e}")))?
        .map_err(|e| Status::internal(format!("Transaction execution failed: {e}")))?;

        self.handle_post_orchestration(
            response,
            is_executed_locally,
            opts,
            digest,
            input_objs,
            transaction_block,
            raw_transaction,
            sender,
        )
        .await
    }
}
