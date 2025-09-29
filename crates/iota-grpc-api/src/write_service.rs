// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use fastcrypto::traits::ToFromBytes;
use iota_core::{
    authority_client::NetworkAuthorityClient, transaction_orchestrator::TransactionOrchestrator,
};
use iota_json_rpc_types::{
    BalanceChange, IotaTransactionBlock, IotaTransactionBlockEvents, IotaTransactionBlockResponse,
    ObjectChange,
};
use iota_types::{
    base_types::{IotaAddress, ObjectID, ObjectRef, SequenceNumber},
    effects::{ObjectRemoveKind, TransactionEffectsAPI},
    object::{Object, Owner},
    quorum_driver_types::{
        ExecuteTransactionRequestType, ExecuteTransactionRequestV1, ExecuteTransactionResponseV1,
        IsTransactionExecutedLocally,
    },
    signature::GenericSignature,
    storage::{PostExecutionPackageResolver, WriteKind},
    transaction::{InputObjectKind, Transaction, TransactionData, TransactionDataAPI},
};
use move_core_types::language_storage::TypeTag;
use tonic::{Request, Response, Status};
use tracing::debug;

use crate::{
    GrpcReader,
    utils::convert_bytes,
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

    /// Convert bytes to any deserializable type
    fn convert_bytes<T: serde::de::DeserializeOwned>(&self, tx_bytes: &[u8]) -> Result<T, Status> {
        convert_bytes(tx_bytes)
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
            Option<Vec<u8>>,
            Vec<u8>,
        ),
        Status,
    > {
        let opts = opts.unwrap_or_default();
        let tx_data: TransactionData = self.convert_bytes(&tx_bytes)?;
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
                debug!("Creating IotaTransactionBlock with epoch store and module cache");
                match IotaTransactionBlock::try_from(
                    txn.data().clone(),
                    epoch_store.module_cache(),
                    *txn.digest(),
                ) {
                    Ok(iota_tx_block) => {
                        match bcs::to_bytes(&iota_tx_block) {
                            Ok(serialized) => Some(serialized),
                            Err(e) => {
                                debug!(
                                    "Failed to serialize IotaTransactionBlock, falling back to basic serialization: {e}"
                                );
                                // Fallback to basic transaction data serialization
                                Some(bcs::to_bytes(txn.data()).map_err(|e| {
                                    Status::internal(format!(
                                        "Transaction serialization failed: {e}"
                                    ))
                                })?)
                            }
                        }
                    }
                    Err(e) => {
                        debug!(
                            "Failed to create IotaTransactionBlock, falling back to basic serialization: {e}"
                        );
                        // Fallback to basic transaction data serialization
                        Some(bcs::to_bytes(txn.data()).map_err(|e| {
                            Status::internal(format!("Transaction serialization failed: {e}"))
                        })?)
                    }
                }
            } else {
                // Graceful fallback: No epoch store available, use basic transaction data
                debug!("No epoch store available, using basic transaction data serialization");
                Some(bcs::to_bytes(txn.data()).map_err(|e| {
                    Status::internal(format!("Transaction serialization failed: {e}"))
                })?)
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
                || opts.show_events, // Include for events too!
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

    /// Create IotaTransactionBlockResponse from execution results
    async fn create_transaction_block_response(
        &self,
        response: ExecuteTransactionResponseV1,
        is_executed_locally: IsTransactionExecutedLocally,
        opts: TransactionResponseOptions,
        digest: iota_types::base_types::TransactionDigest,
        transaction_block: Option<Vec<u8>>,
        raw_transaction: Vec<u8>,
        sender: IotaAddress,
    ) -> Result<IotaTransactionBlockResponse, Status> {
        // Build transaction block from serialized data if requested
        let transaction = if opts.show_input {
            transaction_block.and_then(|data| {
                // Try to deserialize back to IotaTransactionBlock
                bcs::from_bytes::<IotaTransactionBlock>(&data).ok()
            })
        } else {
            None
        };

        let raw_transaction = if opts.show_raw_input {
            raw_transaction
        } else {
            vec![]
        };

        let effects = if opts.show_effects {
            // Convert TransactionEffects to IotaTransactionBlockEffects
            Some(
                response
                    .effects
                    .effects
                    .clone()
                    .try_into()
                    .map_err(|e| Status::internal(format!("Failed to convert effects: {e}")))?,
            )
        } else {
            None
        };

        let events = if opts.show_events {
            // Convert TransactionEvents to IotaTransactionBlockEvents
            if let Some(transaction_events) = response.events {
                // We need epoch store and authority state for event conversion (like JSON-RPC)
                if let (Some(epoch_store), Some(authority_state)) = (
                    self.grpc_reader.load_epoch_store_one_call_per_task(),
                    self.grpc_reader.authority_state().as_ref(),
                ) {
                    // Create PostExecutionPackageResolver exactly like JSON-RPC API
                    let package_resolver = PostExecutionPackageResolver::new(
                        authority_state.get_backing_package_store().clone(),
                        &response.output_objects,
                    );
                    let mut layout_resolver = epoch_store
                        .executor()
                        .type_layout_resolver(Box::new(package_resolver));

                    match IotaTransactionBlockEvents::try_from(
                        transaction_events,
                        digest,
                        None,
                        layout_resolver.as_mut(),
                    ) {
                        Ok(iota_events) => Some(iota_events),
                        Err(e) => {
                            debug!("Failed to convert events: {e}");
                            None
                        }
                    }
                } else {
                    debug!("Cannot convert events: missing epoch store or authority state");
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        let (object_changes, balance_changes) =
            if opts.show_object_changes || opts.show_balance_changes {
                match (&response.input_objects, &response.output_objects) {
                    (Some(input_objects), Some(output_objects)) => {
                        let object_changes = if opts.show_object_changes {
                            Some(compute_object_changes(
                                sender,
                                response.effects.effects.modified_at_versions(),
                                response.effects.effects.all_changed_objects(),
                                response.effects.effects.all_removed_objects(),
                                input_objects,
                                output_objects,
                            ))
                        } else {
                            None
                        };

                        let balance_changes = if opts.show_balance_changes {
                            Some(derive_balance_changes(input_objects, output_objects))
                        } else {
                            None
                        };

                        (object_changes, balance_changes)
                    }
                    _ => {
                        debug!(
                            "Cannot compute object/balance changes: missing input or output objects"
                        );
                        (None, None)
                    }
                }
            } else {
                (None, None)
            };

        let timestamp_ms =
            if opts.show_effects || opts.show_object_changes || opts.show_balance_changes {
                Some(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64,
                )
            } else {
                None
            };

        let confirmed_local_execution = Some(is_executed_locally);

        let raw_effects = if opts.show_raw_effects {
            bcs::to_bytes(&response.effects.effects)
                .map_err(|e| Status::internal(format!("Raw effects serialization failed: {e}")))?
        } else {
            vec![]
        };

        Ok(IotaTransactionBlockResponse {
            digest,
            transaction,
            raw_transaction,
            effects,
            events,
            object_changes,
            balance_changes,
            timestamp_ms,
            confirmed_local_execution,
            checkpoint: None, // Same as JSON RPC execution API
            errors: vec![],
            raw_effects,
        })
    }

    /// Serialize IotaTransactionBlockResponse to JSON
    fn serialize_response_to_json(
        response: &IotaTransactionBlockResponse,
    ) -> Result<ExecuteTransactionResponse, Status> {
        let data = serde_json::to_vec(response)
            .map_err(|e| Status::internal(format!("Failed to serialize response to JSON: {e}")))?;

        Ok(ExecuteTransactionResponse {
            json_data: Some(crate::common::JsonData { data }),
        })
    }

    async fn handle_post_orchestration(
        &self,
        response: ExecuteTransactionResponseV1,
        is_executed_locally: IsTransactionExecutedLocally,
        opts: TransactionResponseOptions,
        digest: iota_types::base_types::TransactionDigest,
        _input_objs: Vec<InputObjectKind>,
        transaction_block: Option<Vec<u8>>,
        raw_transaction: Vec<u8>,
        sender: IotaAddress,
    ) -> Result<Response<ExecuteTransactionResponse>, Status> {
        // Create IotaTransactionBlockResponse using same logic as JSON RPC
        let iota_response = self
            .create_transaction_block_response(
                response,
                is_executed_locally,
                opts,
                digest,
                transaction_block,
                raw_transaction,
                sender,
            )
            .await?;

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
    async fn execute_transaction(
        &self,
        request: Request<ExecuteTransactionRequest>,
    ) -> Result<Response<ExecuteTransactionResponse>, Status> {
        let req = request.into_inner();

        // Phase 1: Request Preparation
        let (execute_request, opts, sender, input_objs, txn, transaction_block, raw_transaction) =
            self.prepare_execute_transaction_request(req.tx_bytes, req.signatures, req.options)?;

        let digest = *txn.digest();

        let orchestrator = self
            .transaction_orchestrator
            .as_ref()
            .ok_or_else(|| Status::unimplemented("Transaction execution not available"))?;

        debug!("Executing transaction: {digest}");
        let request_type = req
            .request_type
            .map(|rt| match rt {
                0 => ExecuteTransactionRequestType::WaitForEffectsCert,
                1 => ExecuteTransactionRequestType::WaitForLocalExecution,
                _ => ExecuteTransactionRequestType::WaitForEffectsCert, // fallback to default
            })
            .unwrap_or(ExecuteTransactionRequestType::WaitForEffectsCert);
        let (response, is_executed_locally) = orchestrator
            .execute_transaction_block(execute_request, request_type, None)
            .await
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

/// Extract coins from objects
fn coins(objects: &[Object]) -> impl Iterator<Item = (&IotaAddress, (TypeTag, u64))> + '_ {
    objects.iter().filter_map(|object| {
        let address = match object.owner() {
            Owner::AddressOwner(address) => address,
            Owner::ObjectOwner(address) => address,
            Owner::Shared { .. } | Owner::Immutable => return None,
        };

        if let Some(coin) = object.as_coin_maybe() {
            if let Some(coin_type) = object.coin_type_maybe() {
                return Some((address, (coin_type, coin.value())));
            }
        }
        None
    })
}

/// Derive balance changes
fn derive_balance_changes(
    input_objects: &[Object],
    output_objects: &[Object],
) -> Vec<BalanceChange> {
    let balances = coins(input_objects).fold(
        std::collections::BTreeMap::<_, i128>::new(),
        |mut acc, (address, (coin_type, amount))| {
            *acc.entry((address, coin_type.clone())).or_default() -= amount as i128;
            acc
        },
    );

    let balances =
        coins(output_objects).fold(balances, |mut acc, (address, (coin_type, amount))| {
            *acc.entry((address, coin_type.clone())).or_default() += amount as i128;
            acc
        });

    balances
        .into_iter()
        .filter_map(|((address, coin_type), amount)| {
            if amount == 0 {
                return None;
            }
            Some(BalanceChange {
                owner: Owner::AddressOwner(*address),
                coin_type,
                amount,
            })
        })
        .collect()
}

/// Compute object changes
fn compute_object_changes(
    sender: IotaAddress,
    modified_at_versions: Vec<(ObjectID, SequenceNumber)>,
    all_changed_objects: Vec<(ObjectRef, Owner, WriteKind)>,
    all_removed_objects: Vec<(ObjectRef, ObjectRemoveKind)>,
    input_objects: &[Object],
    output_objects: &[Object],
) -> Vec<ObjectChange> {
    let mut object_changes = vec![];
    let modify_at_version = modified_at_versions
        .into_iter()
        .collect::<std::collections::HashMap<_, _>>();

    let mut all_objects = std::collections::HashMap::new();
    for obj in input_objects.iter().chain(output_objects.iter()) {
        all_objects.insert((obj.id(), obj.version()), obj);
    }

    for ((object_id, version, digest), owner, kind) in all_changed_objects {
        if let Some(obj) = all_objects.get(&(object_id, version)) {
            if let Some(type_) = obj.type_() {
                let object_type = type_.clone().into();

                match kind {
                    WriteKind::Mutate => object_changes.push(ObjectChange::Mutated {
                        sender,
                        owner,
                        object_type,
                        object_id,
                        version,
                        previous_version: modify_at_version
                            .get(&object_id)
                            .cloned()
                            .unwrap_or_default(),
                        digest,
                    }),
                    WriteKind::Create => object_changes.push(ObjectChange::Created {
                        sender,
                        owner,
                        object_type,
                        object_id,
                        version,
                        digest,
                    }),
                    _ => {}
                }
            } else if let Some(p) = obj.data.try_as_package() {
                if kind == WriteKind::Create {
                    object_changes.push(ObjectChange::Published {
                        package_id: p.id(),
                        version: p.version(),
                        digest,
                        modules: p.serialized_module_map().keys().cloned().collect(),
                    });
                }
            }
        }
    }

    for ((id, version, _), kind) in all_removed_objects {
        if let Some(obj) = all_objects.get(&(id, version)) {
            if let Some(type_) = obj.type_() {
                let object_type = type_.clone().into();
                match kind {
                    ObjectRemoveKind::Delete => object_changes.push(ObjectChange::Deleted {
                        sender,
                        object_type,
                        object_id: id,
                        version,
                    }),
                    ObjectRemoveKind::Wrap => object_changes.push(ObjectChange::Wrapped {
                        sender,
                        object_type,
                        object_id: id,
                        version,
                    }),
                }
            }
        }
    }

    object_changes
}
