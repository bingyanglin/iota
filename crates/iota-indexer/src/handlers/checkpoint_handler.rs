// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2024 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, slice, sync::Arc};

use async_trait::async_trait;
use iota_data_ingestion_core::Worker;
use iota_json_rpc_types::IotaTransactionKind;
use iota_metrics::{get_metrics, spawn_monitored_task};
use iota_rest_api::{CheckpointData, CheckpointTransaction};
use iota_types::{
    base_types::ObjectID,
    dynamic_field::{DynamicFieldInfo, DynamicFieldType},
    effects::TransactionEffectsAPI,
    event::SystemEpochInfoEvent,
    iota_system_state::{IotaSystemStateTrait, get_iota_system_state},
    messages_checkpoint::{
        CertifiedCheckpointSummary, CheckpointContents, CheckpointSequenceNumber,
    },
    object::{Object, Owner},
    transaction::TransactionDataAPI,
};
use itertools::Itertools;
use move_core_types::{
    self,
    language_storage::{StructTag, TypeTag},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::{
    db::ConnectionPool,
    errors::IndexerError,
    handlers::{
        CheckpointDataToCommit, EpochToCommit, TransactionObjectChangesToCommit,
        committer::start_tx_checkpoint_commit_task,
        tx_processor::{EpochEndIndexingObjectStore, TxChangesProcessor},
    },
    metrics::IndexerMetrics,
    models::{display::StoredDisplay, epoch::StoredEpochInfo, obj_indices::StoredObjectVersion},
    store::{IndexerStore, PgIndexerStore},
    types::{
        EventIndex, IndexedCheckpoint, IndexedDeletedObject, IndexedEpochInfo, IndexedEvent,
        IndexedObject, IndexedPackage, IndexedTransaction, IndexerResult, TxIndex,
    },
};

const CHECKPOINT_QUEUE_SIZE: usize = 100;

pub async fn new_handlers(
    state: PgIndexerStore,
    metrics: IndexerMetrics,
    next_checkpoint_sequence_number: CheckpointSequenceNumber,
    cancel: CancellationToken,
) -> Result<CheckpointHandler, IndexerError> {
    let checkpoint_queue_size = std::env::var("CHECKPOINT_QUEUE_SIZE")
        .unwrap_or(CHECKPOINT_QUEUE_SIZE.to_string())
        .parse::<usize>()
        .unwrap();
    let global_metrics = get_metrics().unwrap();
    let (indexed_checkpoint_sender, indexed_checkpoint_receiver) =
        iota_metrics::metered_channel::channel(
            checkpoint_queue_size,
            &global_metrics
                .channel_inflight
                .with_label_values(&["checkpoint_indexing"]),
        );

    let state_clone = state.clone();
    let metrics_clone = metrics.clone();
    spawn_monitored_task!(start_tx_checkpoint_commit_task(
        state_clone,
        metrics_clone,
        indexed_checkpoint_receiver,
        next_checkpoint_sequence_number,
        cancel.clone()
    ));
    Ok(CheckpointHandler::new(
        state,
        metrics,
        indexed_checkpoint_sender,
    ))
}

pub struct CheckpointHandler {
    state: PgIndexerStore,
    metrics: IndexerMetrics,
    indexed_checkpoint_sender: iota_metrics::metered_channel::Sender<CheckpointDataToCommit>,
}

#[async_trait]
impl Worker for CheckpointHandler {
    type Message = ();
    type Error = IndexerError;

    async fn process_checkpoint(
        &self,
        checkpoint: Arc<CheckpointData>,
    ) -> Result<Self::Message, Self::Error> {
        let cp_seq = checkpoint.checkpoint_summary.sequence_number;
        info!(
            "CheckpointHandler: process_checkpoint called for checkpoint sequence number: {}",
            cp_seq
        );
        self.metrics
            .latest_fullnode_checkpoint_sequence_number
            .set(checkpoint.checkpoint_summary.sequence_number as i64);
        let time_now_ms = chrono::Utc::now().timestamp_millis();
        let cp_download_lag = time_now_ms - checkpoint.checkpoint_summary.timestamp_ms as i64;
        info!(
            "checkpoint download lag for cp {}: {} ms",
            checkpoint.checkpoint_summary.sequence_number, cp_download_lag
        );
        self.metrics.download_lag_ms.set(cp_download_lag);
        self.metrics
            .max_downloaded_checkpoint_sequence_number
            .set(checkpoint.checkpoint_summary.sequence_number as i64);
        self.metrics
            .downloaded_checkpoint_timestamp_ms
            .set(checkpoint.checkpoint_summary.timestamp_ms as i64);
        info!(
            "Indexer lag: downloaded checkpoint {} with time now {} and checkpoint time {}",
            checkpoint.checkpoint_summary.sequence_number,
            time_now_ms,
            checkpoint.checkpoint_summary.timestamp_ms
        );

        info!(
            "CheckpointHandler: About to call Self::index_checkpoint for cp_seq: {}",
            cp_seq
        );
        let checkpoint_data_to_commit = Self::index_checkpoint(
            self.state.clone().into(),
            &checkpoint,
            Arc::new(self.metrics.clone()),
            Self::index_packages(slice::from_ref(&checkpoint), &self.metrics),
        )
        .await?;
        info!(
            "CheckpointHandler: Self::index_checkpoint SUCCEEDED for cp_seq: {}. Attempting to send to committer.",
            cp_seq
        );

        self.indexed_checkpoint_sender
            .send(checkpoint_data_to_commit)
            .await
            .map_err(|e| {
                error!(
                    "CheckpointHandler: FAILED to send checkpoint data for cp_seq: {} to committer. Error: {:?}",
                    cp_seq, e
                );
                IndexerError::MpscChannel(
                    format!("Failed to send checkpoint data for cp_seq: {} to committer", cp_seq)
                )
            })?;
        info!(
            "CheckpointHandler: SUCCESSFULLY sent checkpoint data for cp_seq: {} to committer.",
            cp_seq
        );
        Ok(())
    }
}

impl CheckpointHandler {
    fn new(
        state: PgIndexerStore,
        metrics: IndexerMetrics,
        indexed_checkpoint_sender: iota_metrics::metered_channel::Sender<CheckpointDataToCommit>,
    ) -> Self {
        Self {
            state,
            metrics,
            indexed_checkpoint_sender,
        }
    }

    async fn index_epoch(
        state: Arc<PgIndexerStore>,
        data: &CheckpointData,
    ) -> Result<Option<EpochToCommit>, IndexerError> {
        let checkpoint_object_store = EpochEndIndexingObjectStore::new(data);

        let CheckpointData {
            transactions,
            checkpoint_summary,
            checkpoint_contents: _,
        } = data;

        // Genesis epoch
        if *checkpoint_summary.sequence_number() == 0 {
            info!("Processing genesis epoch");
            let system_state =
                get_iota_system_state(&checkpoint_object_store)?.into_iota_system_state_summary();
            return Ok(Some(EpochToCommit {
                last_epoch: None,
                new_epoch: IndexedEpochInfo::from_new_system_state_summary(
                    &system_state,
                    0,    // first_checkpoint_id
                    None, // event is None for genesis
                ),
                network_total_transactions: 0, // Corrected: For genesis
            }));
        }

        // This line will only be reached if not genesis (sequence_number != 0)
        let previous_epoch_id = checkpoint_summary.epoch() - 1;
        let prev_epoch_db_info_opt: Option<StoredEpochInfo> =
            state.get_epoch_info_by_id(previous_epoch_id).await?;

        if prev_epoch_db_info_opt.is_none() {
            error!(
                previous_epoch_id,
                "Previous epoch info not found in DB, skip epoch indexing."
            );
            return Ok(None);
        }
        let prev_epoch_stored_info = prev_epoch_db_info_opt.unwrap();

        let network_total_tx_num_at_prev_epoch_end = state
            .get_network_total_transactions_by_end_of_epoch(previous_epoch_id)
            .await?;

        if let Some(_end_of_epoch_data) = &checkpoint_summary.end_of_epoch_data {
            // _end_of_epoch_data is used by from_end_of_epoch_data indirectly via
            // checkpoint_summary
            info!(
                epoch_id = checkpoint_summary.epoch(),
                "Processing end of epoch data"
            );
            let system_state =
                get_iota_system_state(&checkpoint_object_store)?.into_iota_system_state_summary();

            let system_epoch_info_event = transactions // Use the `transactions` from checkpoint data
                .iter()
                .find_map(|tx| {
                    tx.events.as_ref().and_then(|events_wrapper| {
                        events_wrapper
                            .data
                            .iter()
                            .find_map(|event| SystemEpochInfoEvent::try_from(event.clone()).ok())
                    })
                });

            if system_epoch_info_event.is_none() {
                error!(
                    "SystemEpochInfoEvent not found for epoch {} at checkpoint {}",
                    checkpoint_summary.epoch(),
                    checkpoint_summary.sequence_number()
                );
                return Ok(None);
            }
            let system_epoch_info_event_ref = system_epoch_info_event.as_ref().unwrap();

            let new_epoch = IndexedEpochInfo::from_end_of_epoch_data(
                &system_state,
                checkpoint_summary,          // Correct: pass the full summary
                system_epoch_info_event_ref, // Correct: pass the event
                network_total_tx_num_at_prev_epoch_end, // Correct: pass the calculated total
            );

            // Construct IndexedEpochInfo for the *previous* epoch to pass to
            // EpochToCommit.last_epoch This requires converting StoredEpochInfo
            // to IndexedEpochInfo. For simplicity, if a direct conversion isn't
            // trivial, we might need to re-fetch or adapt.
            // Assuming StoredEpochInfo can be (or is already) suitable for what
            // persist_epoch needs for the last epoch. For now, let's try to
            // create a minimal IndexedEpochInfo for the last_epoch if possible,
            // or check if persist_epoch can work with just the epoch number for
            // `last_epoch`. Given the definition of EpochToCommit, it needs
            // Option<IndexedEpochInfo>. We will create one from
            // `prev_epoch_stored_info`
            let last_indexed_epoch_info = IndexedEpochInfo {
                // This is a simplified representation
                epoch: prev_epoch_stored_info.epoch as u64,
                first_checkpoint_id: prev_epoch_stored_info.first_checkpoint_id as u64,
                epoch_start_timestamp: prev_epoch_stored_info.epoch_start_timestamp as u64,
                reference_gas_price: prev_epoch_stored_info.reference_gas_price as u64,
                protocol_version: prev_epoch_stored_info.protocol_version as u64,
                total_stake: prev_epoch_stored_info.total_stake as u64,
                storage_fund_balance: prev_epoch_stored_info.storage_fund_balance as u64,
                system_state: prev_epoch_stored_info.system_state.clone(), // This is Vec<u8>
                // Fields below are Option<u64> and are for the *end* of that epoch
                epoch_total_transactions: prev_epoch_stored_info
                    .epoch_total_transactions
                    .map(|v| v as u64),
                last_checkpoint_id: prev_epoch_stored_info.last_checkpoint_id.map(|v| v as u64),
                epoch_end_timestamp: prev_epoch_stored_info.epoch_end_timestamp.map(|v| v as u64),
                storage_charge: prev_epoch_stored_info.storage_charge.map(|v| v as u64),
                storage_rebate: prev_epoch_stored_info.storage_rebate.map(|v| v as u64),
                total_gas_fees: prev_epoch_stored_info.total_gas_fees.map(|v| v as u64),
                total_stake_rewards_distributed: prev_epoch_stored_info
                    .total_stake_rewards_distributed
                    .map(|v| v as u64),
                epoch_commitments: prev_epoch_stored_info
                    .epoch_commitments
                    .as_ref()
                    .and_then(|bytes| bcs::from_bytes(bytes).ok()),
                burnt_tokens_amount: prev_epoch_stored_info.burnt_tokens_amount.map(|v| v as u64),
                minted_tokens_amount: prev_epoch_stored_info
                    .minted_tokens_amount
                    .map(|v| v as u64),
                tips_amount: None, /* StoredEpochInfo doesn't have tips_amount directly, assume
                                    * None or calculate if needed */
            };

            Ok(Some(EpochToCommit {
                last_epoch: Some(last_indexed_epoch_info), // Corrected: pass IndexedEpochInfo
                new_epoch,
                network_total_transactions: checkpoint_summary.network_total_transactions,
            }))
        } else {
            Ok(None)
        }
    }

    fn derive_object_versions(
        object_history_changes: &TransactionObjectChangesToCommit,
    ) -> Vec<StoredObjectVersion> {
        object_history_changes
            .changed_objects
            .iter()
            .map(|obj| StoredObjectVersion {
                object_id: obj.object.id().to_vec(),
                object_version: obj.object.version().value() as i64,
                cp_sequence_number: obj.checkpoint_sequence_number as i64,
            })
            .collect()
    }

    async fn index_checkpoint(
        state: Arc<PgIndexerStore>,
        data: &CheckpointData,
        metrics: Arc<IndexerMetrics>,
        packages: Vec<IndexedPackage>,
    ) -> Result<CheckpointDataToCommit, IndexerError> {
        let checkpoint_seq = data.checkpoint_summary.sequence_number;
        info!(checkpoint_seq, "Indexing checkpoint data blob");

        // Index epoch
        let epoch = Self::index_epoch(state, data).await?;

        // Index Objects
        let object_changes: TransactionObjectChangesToCommit =
            Self::index_objects(data, &metrics).await?;
        let object_history_changes: TransactionObjectChangesToCommit =
            Self::index_objects_history(data).await?;
        let object_versions = Self::derive_object_versions(&object_history_changes);

        let (checkpoint, db_transactions, db_events, db_tx_indices, db_event_indices, db_displays) = {
            let CheckpointData {
                transactions,
                checkpoint_summary,
                checkpoint_contents,
            } = data;

            let (db_transactions, db_events, db_tx_indices, db_event_indices, db_displays) =
                Self::index_transactions(
                    transactions,
                    checkpoint_summary,
                    checkpoint_contents,
                    &metrics,
                )
                .await?;

            let successful_tx_num: u64 = db_transactions.iter().map(|t| t.successful_tx_num).sum();
            (
                IndexedCheckpoint::from_iota_checkpoint(
                    checkpoint_summary,
                    checkpoint_contents,
                    successful_tx_num as usize,
                ),
                db_transactions,
                db_events,
                db_tx_indices,
                db_event_indices,
                db_displays,
            )
        };
        let time_now_ms = chrono::Utc::now().timestamp_millis();
        metrics
            .index_lag_ms
            .set(time_now_ms - checkpoint.timestamp_ms as i64);
        metrics
            .max_indexed_checkpoint_sequence_number
            .set(checkpoint.sequence_number as i64);
        metrics
            .indexed_checkpoint_timestamp_ms
            .set(checkpoint.timestamp_ms as i64);
        info!(
            "Indexer lag: indexed checkpoint {} with time now {} and checkpoint time {}",
            checkpoint.sequence_number, time_now_ms, checkpoint.timestamp_ms
        );

        Ok(CheckpointDataToCommit {
            checkpoint,
            transactions: db_transactions,
            events: db_events,
            tx_indices: db_tx_indices,
            event_indices: db_event_indices,
            display_updates: db_displays,
            object_changes,
            object_history_changes,
            object_versions,
            packages,
            epoch,
        })
    }

    async fn index_transactions(
        transactions: &[CheckpointTransaction],
        checkpoint_summary: &CertifiedCheckpointSummary,
        checkpoint_contents: &CheckpointContents,
        metrics: &IndexerMetrics,
    ) -> IndexerResult<(
        Vec<IndexedTransaction>,
        Vec<IndexedEvent>,
        Vec<TxIndex>,
        Vec<EventIndex>,
        BTreeMap<String, StoredDisplay>,
    )> {
        let checkpoint_seq = checkpoint_summary.sequence_number();

        let mut tx_seq_num_iter = checkpoint_contents
            .enumerate_transactions(checkpoint_summary)
            .map(|(seq, execution_digest)| (execution_digest.transaction, seq));

        if checkpoint_contents.size() != transactions.len() {
            return Err(IndexerError::FullNodeReading(format!(
                "CheckpointContents has different size {} compared to Transactions {} for checkpoint {}",
                checkpoint_contents.size(),
                transactions.len(),
                checkpoint_seq
            )));
        }

        let mut db_transactions = Vec::new();
        let mut db_events = Vec::new();
        let mut db_displays = BTreeMap::new();
        let mut db_tx_indices = Vec::new();
        let mut db_event_indices = Vec::new();

        for tx in transactions {
            // Unwrap safe - we checked they have equal length above
            let (tx_digest, tx_sequence_number) = tx_seq_num_iter.next().unwrap();
            let actual_tx_digest = tx.transaction.digest();
            if tx_digest != *actual_tx_digest {
                return Err(IndexerError::FullNodeReading(format!(
                    "Transactions has different ordering from CheckpointContents, for checkpoint {}, Mismatch found at {} v.s. {}",
                    checkpoint_seq, tx_digest, actual_tx_digest,
                )));
            }

            let (indexed_tx, tx_indices, indexed_events, events_indices, stored_displays) =
                Self::index_transaction(
                    tx,
                    tx_sequence_number,
                    *checkpoint_seq,
                    checkpoint_summary.timestamp_ms,
                    metrics,
                )
                .await?;
            db_transactions.push(indexed_tx);
            db_tx_indices.push(tx_indices);
            db_events.extend(indexed_events);
            db_event_indices.extend(events_indices);
            db_displays.extend(stored_displays);
        }
        Ok((
            db_transactions,
            db_events,
            db_tx_indices,
            db_event_indices,
            db_displays,
        ))
    }

    pub async fn index_transaction(
        tx: &CheckpointTransaction,
        tx_sequence_number: u64,
        checkpoint_seq: CheckpointSequenceNumber,
        checkpoint_timestamp_ms: u64,
        metrics: &IndexerMetrics,
    ) -> IndexerResult<(
        IndexedTransaction,
        TxIndex,
        Vec<IndexedEvent>,
        Vec<EventIndex>,
        BTreeMap<String, StoredDisplay>,
    )> {
        let CheckpointTransaction {
            transaction: sender_signed_data,
            effects: fx,
            events,
            input_objects,
            output_objects,
        } = tx;

        let tx_digest = sender_signed_data.digest();
        let tx = sender_signed_data.transaction_data();
        let events = events
            .as_ref()
            .map(|events| events.data.clone())
            .unwrap_or_default();

        let transaction_kind = IotaTransactionKind::from(tx.kind());

        let db_events = events
            .iter()
            .enumerate()
            .map(|(idx, event)| {
                IndexedEvent::from_event(
                    tx_sequence_number,
                    idx as u64,
                    checkpoint_seq,
                    *tx_digest,
                    event,
                    checkpoint_timestamp_ms,
                )
            })
            .collect();

        let db_event_indices = events
            .iter()
            .enumerate()
            .map(|(idx, event)| EventIndex::from_event(tx_sequence_number, idx as u64, event))
            .collect();

        let db_displays = events
            .iter()
            .flat_map(StoredDisplay::try_from_event)
            .map(|display| (display.object_type.clone(), display))
            .collect();

        let objects = input_objects
            .iter()
            .chain(output_objects.iter())
            .collect::<Vec<_>>();

        let (balance_change, object_changes) = TxChangesProcessor::new(&objects, metrics.clone())
            .get_changes(tx, fx, tx_digest)
            .await?;

        let db_txn = IndexedTransaction {
            tx_sequence_number,
            tx_digest: *tx_digest,
            checkpoint_sequence_number: checkpoint_seq,
            timestamp_ms: checkpoint_timestamp_ms,
            sender_signed_data: sender_signed_data.data().clone(),
            effects: fx.clone(),
            object_changes,
            balance_change,
            events,
            transaction_kind,
            successful_tx_num: if fx.status().is_ok() {
                tx.kind().tx_count() as u64
            } else {
                0
            },
        };

        // Input Objects
        let input_objects = tx
            .input_objects()
            .expect("committed txns have been validated")
            .into_iter()
            .map(|obj_kind| obj_kind.object_id())
            .collect::<Vec<_>>();

        // Changed Objects
        let changed_objects = fx
            .all_changed_objects()
            .into_iter()
            .map(|(object_ref, _owner, _write_kind)| object_ref.0)
            .collect::<Vec<_>>();

        // Payers
        let payers = vec![tx.gas_owner()];

        // Sender
        let sender = tx.sender();

        // Recipients
        let recipients = fx
            .all_changed_objects()
            .into_iter()
            .filter_map(|(_object_ref, owner, _write_kind)| match owner {
                Owner::AddressOwner(address) => Some(address),
                _ => None,
            })
            .unique()
            .collect::<Vec<_>>();

        // Move Calls
        let move_calls = tx
            .move_calls()
            .iter()
            .map(|(p, m, f)| (*<&ObjectID>::clone(p), m.to_string(), f.to_string()))
            .collect();

        let db_tx_indices = TxIndex {
            tx_sequence_number,
            transaction_digest: *tx_digest,
            checkpoint_sequence_number: checkpoint_seq,
            input_objects,
            changed_objects,
            sender,
            payers,
            recipients,
            move_calls,
            tx_kind: transaction_kind,
        };

        Ok((
            db_txn,
            db_tx_indices,
            db_events,
            db_event_indices,
            db_displays,
        ))
    }

    pub(crate) async fn index_objects(
        data: &CheckpointData,
        metrics: &IndexerMetrics,
    ) -> Result<TransactionObjectChangesToCommit, IndexerError> {
        let _timer = metrics.indexing_objects_latency.start_timer();
        let checkpoint_seq = data.checkpoint_summary.sequence_number;

        let eventually_removed_object_refs_post_version =
            data.eventually_removed_object_refs_post_version();
        let indexed_eventually_removed_objects = eventually_removed_object_refs_post_version
            .into_iter()
            .map(|obj_ref| IndexedDeletedObject {
                object_id: obj_ref.0,
                object_version: obj_ref.1.into(),
                checkpoint_sequence_number: checkpoint_seq,
            })
            .collect();

        let latest_live_output_objects = data.latest_live_output_objects();
        let changed_objects = latest_live_output_objects
            .into_iter()
            .map(|o| {
                try_extract_df_kind(o)
                    .map(|df_kind| IndexedObject::from_object(checkpoint_seq, o.clone(), df_kind))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(TransactionObjectChangesToCommit {
            changed_objects,
            deleted_objects: indexed_eventually_removed_objects,
        })
    }

    // similar to index_objects, but objects_history keeps all versions of objects
    async fn index_objects_history(
        data: &CheckpointData,
    ) -> Result<TransactionObjectChangesToCommit, IndexerError> {
        let checkpoint_seq = data.checkpoint_summary.sequence_number;
        let deleted_objects = data
            .transactions
            .iter()
            .flat_map(|tx| tx.removed_object_refs_post_version())
            .collect::<Vec<_>>();
        let indexed_deleted_objects: Vec<IndexedDeletedObject> = deleted_objects
            .into_iter()
            .map(|obj_ref| IndexedDeletedObject {
                object_id: obj_ref.0,
                object_version: obj_ref.1.into(),
                checkpoint_sequence_number: checkpoint_seq,
            })
            .collect();

        let output_objects: Vec<_> = data
            .transactions
            .iter()
            .flat_map(|tx| &tx.output_objects)
            .collect();
        // TODO(gegaowp): the current df_info implementation is not correct,
        // but we have decided remove all df_* except df_kind.
        let changed_objects = output_objects
            .into_iter()
            .map(|o| {
                try_extract_df_kind(o)
                    .map(|df_kind| IndexedObject::from_object(checkpoint_seq, o.clone(), df_kind))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(TransactionObjectChangesToCommit {
            changed_objects,
            deleted_objects: indexed_deleted_objects,
        })
    }

    fn index_packages(
        checkpoint_data: &[CheckpointData],
        metrics: &IndexerMetrics,
    ) -> Vec<IndexedPackage> {
        let _timer = metrics.indexing_packages_latency.start_timer();
        checkpoint_data
            .iter()
            .flat_map(|data| {
                let checkpoint_sequence_number = data.checkpoint_summary.sequence_number;
                data.transactions
                    .iter()
                    .flat_map(|tx| &tx.output_objects)
                    .filter_map(|o| {
                        if let iota_types::object::Data::Package(p) = &o.data {
                            Some(IndexedPackage {
                                package_id: o.id(),
                                move_package: p.clone(),
                                checkpoint_sequence_number,
                            })
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    pub(crate) fn pg_blocking_cp(state: PgIndexerStore) -> Result<ConnectionPool, IndexerError> {
        let state_as_any = state.as_any();
        if let Some(pg_state) = state_as_any.downcast_ref::<PgIndexerStore>() {
            return Ok(pg_state.blocking_cp());
        }
        Err(IndexerError::Uncategorized(anyhow::anyhow!(
            "Failed to downcast state to PgIndexerStore"
        )))
    }
}

/// If `o` is a dynamic `Field<K, V>`, determine whether it represents a Dynamic
/// Field or a Dynamic Object Field based on its type.
fn try_extract_df_kind(o: &Object) -> IndexerResult<Option<DynamicFieldType>> {
    // Skip if not a move object
    let Some(move_object) = o.data.try_as_move() else {
        return Ok(None);
    };

    if !move_object.type_().is_dynamic_field() {
        return Ok(None);
    }

    let type_: StructTag = move_object.type_().clone().into();
    let [name, _] = type_.type_params.as_slice() else {
        return Ok(None);
    };

    Ok(Some(
        if matches!(name, TypeTag::Struct(s) if DynamicFieldInfo::is_dynamic_object_field_wrapper(s))
        {
            DynamicFieldType::DynamicObject
        } else {
            DynamicFieldType::DynamicField
        },
    ))
}
