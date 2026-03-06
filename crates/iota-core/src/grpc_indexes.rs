// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2024 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Mutex,
    time::{Duration, Instant},
};

use iota_types::{
    committee::EpochId,
    digests::TransactionDigest,
    error::IotaResult,
    full_checkpoint_content::CheckpointData,
    iota_system_state::IotaSystemStateTrait,
    messages_checkpoint::{CheckpointContents, CheckpointSequenceNumber},
    storage::{EpochInfo, TransactionInfo, error::Error as StorageError},
};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use tracing::{debug, info};
use typed_store::{
    DBMapUtils, TypedStoreError,
    rocks::{DBMap, MetricConf},
    traits::{Map, TableSummary, TypedStoreDebug},
};

use crate::{authority::AuthorityStore, checkpoints::CheckpointStore};

/// Only increment this version when an active table's schema changes, as that
/// triggers a full re-index of all checkpoints.
const CURRENT_DB_VERSION: u64 = 1;

/// On-disk directory name for the gRPC indexes store.
pub const GRPC_INDEXES_DIR: &str = "grpc_indexes";

/// Legacy directory name from before the REST API removal.
/// Used by `migrate_legacy_dirs` to find and rename the old directory.
const LEGACY_INDEX_DIR: &str = "rest_index";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct MetadataInfo {
    /// Version of the Database
    version: u64,
}

/// Checkpoint watermark type
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub enum Watermark {
    Indexed,
    Pruned,
}

/// RocksDB tables for the GrpcIndexesStore
///
/// Anytime a new table is added, or an existing one has its schema changed,
/// make sure to also update the value of `CURRENT_DB_VERSION`.
///
/// NOTE: Authors and Reviewers before adding any new tables ensure that they
/// are either:
/// - bounded in size by the live object set
/// - are prune-able and have corresponding logic in the `prune` function
#[derive(DBMapUtils)]
struct IndexStoreTables {
    /// A singleton that store metadata information on the DB.
    ///
    /// A few uses for this singleton:
    /// - determining if the DB has been initialized (as some tables will still
    ///   be empty post initialization)
    /// - version of the DB. Everytime a new table or schema is changed the
    ///   version number needs to be incremented.
    meta: DBMap<(), MetadataInfo>,

    /// Table used to track watermark for the highest indexed checkpoint
    ///
    /// This is useful to help know the highest checkpoint that was indexed in
    /// the event that the node was running with indexes enabled, then run
    /// for a period of time with indexes disabled, and then run with them
    /// enabled again so that the tables can be reinitialized.
    watermark: DBMap<Watermark, CheckpointSequenceNumber>,

    /// An index of extra metadata for Epochs.
    ///
    /// Only contains entries for epochs which have yet to be pruned from the
    /// main database.
    epochs: DBMap<EpochId, EpochInfo>,

    /// Maps transaction digests to the checkpoint that contains them.
    ///
    /// Only contains entries for transactions which have yet to be pruned from
    /// the main database.
    transaction_checkpoints: DBMap<TransactionDigest, CheckpointSequenceNumber>,

    /// Deprecated: migrated to `transaction_checkpoints` (checkpoint-only).
    #[allow(dead_code)]
    #[deprecated_db_map(migration = "migrate_transactions_to_checkpoints")]
    transactions: Option<DBMap<TransactionDigest, TransactionInfo>>,

    /// Deprecated: was used by the removed REST API for object ownership
    /// queries.
    #[allow(dead_code)]
    #[deprecated_db_map]
    owner: Option<DBMap<(), ()>>,

    /// Deprecated: was used by the removed REST API for dynamic field queries.
    #[allow(dead_code)]
    #[deprecated_db_map]
    dynamic_field: Option<DBMap<(), ()>>,

    /// Deprecated: was used by the removed REST API for coin info queries.
    #[allow(dead_code)]
    #[deprecated_db_map]
    coin: Option<DBMap<(), ()>>,
    // NOTE: Authors and Reviewers before adding any new tables ensure that they
    // are either:
    // - bounded in size by the live object set
    // - are prune-able and have corresponding logic in the `prune` function
}

/// Migration: copy checkpoint numbers from old `transactions` table into
/// `transaction_checkpoints`, discarding the now-unused `object_types` field.
fn migrate_transactions_to_checkpoints(
    db: &std::sync::Arc<typed_store::rocks::RocksDB>,
) -> Result<(), TypedStoreError> {
    use typed_store::traits::Map;

    let old = typed_store::rocks::DBMap::<TransactionDigest, TransactionInfo>::reopen(
        db,
        Some("transactions"),
        &typed_store::rocks::ReadWriteOptions::default(),
        true,
    )?;
    let new = typed_store::rocks::DBMap::<TransactionDigest, CheckpointSequenceNumber>::reopen(
        db,
        Some("transaction_checkpoints"),
        &typed_store::rocks::ReadWriteOptions::default(),
        false,
    )?;

    const BATCH_SIZE: usize = 10_000;
    let mut batch = new.batch();
    let mut count = 0usize;
    for item in old.safe_iter() {
        let (digest, info) = item?;
        batch.insert_batch(&new, std::iter::once((digest, info.checkpoint)))?;
        count += 1;
        if count.is_multiple_of(BATCH_SIZE) {
            batch.write()?;
            batch = new.batch();
        }
    }
    if !count.is_multiple_of(BATCH_SIZE) {
        batch.write()?;
    }

    info!("migrated transactions -> transaction_checkpoints");
    Ok(())
}

impl IndexStoreTables {
    fn open<P: Into<PathBuf>>(path: P) -> Self {
        IndexStoreTables::open_tables_read_write(
            path.into(),
            MetricConf::new("grpc-index"),
            None,
            None,
        )
    }

    fn needs_to_do_initialization(&self, checkpoint_store: &CheckpointStore) -> bool {
        (match self.meta.get(&()) {
            Ok(Some(metadata)) => metadata.version != CURRENT_DB_VERSION,
            Ok(None) => true,
            Err(_) => true,
        }) || self.is_indexed_watermark_out_of_date(checkpoint_store)
    }

    // Check if the index watermark is behind the highest_executed_checkpoint.
    fn is_indexed_watermark_out_of_date(&self, checkpoint_store: &CheckpointStore) -> bool {
        let highest_executed_checkpoint = checkpoint_store
            .get_highest_executed_checkpoint_seq_number()
            .ok()
            .flatten();
        let watermark = self.watermark.get(&Watermark::Indexed).ok().flatten();
        watermark < highest_executed_checkpoint
    }

    #[tracing::instrument(skip_all)]
    fn init(
        &mut self,
        authority_store: &AuthorityStore,
        checkpoint_store: &CheckpointStore,
    ) -> Result<(), StorageError> {
        info!("Initializing gRPC indexes");

        let highest_executed_checkpoint =
            checkpoint_store.get_highest_executed_checkpoint_seq_number()?;
        let lowest_available_checkpoint = checkpoint_store
            .get_highest_pruned_checkpoint_seq_number()?
            .map(|c| c.saturating_add(1))
            .unwrap_or(0);

        let checkpoint_range = highest_executed_checkpoint.map(|highest_executed_checkpoint| {
            lowest_available_checkpoint..=highest_executed_checkpoint
        });

        if let Some(checkpoint_range) = checkpoint_range {
            self.index_existing_transactions(authority_store, checkpoint_store, checkpoint_range)?;
        }

        self.initialize_current_epoch(authority_store, checkpoint_store)?;

        self.watermark.insert(
            &Watermark::Indexed,
            &highest_executed_checkpoint.unwrap_or(0),
        )?;

        self.meta.insert(
            &(),
            &MetadataInfo {
                version: CURRENT_DB_VERSION,
            },
        )?;

        info!("Finished initializing gRPC indexes");

        Ok(())
    }

    #[tracing::instrument(skip(self, authority_store, checkpoint_store))]
    fn index_existing_transactions(
        &mut self,
        authority_store: &AuthorityStore,
        checkpoint_store: &CheckpointStore,
        checkpoint_range: std::ops::RangeInclusive<u64>,
    ) -> Result<(), StorageError> {
        info!(
            "Indexing {} checkpoints in range {checkpoint_range:?}",
            checkpoint_range.size_hint().0
        );
        let start_time = Instant::now();

        checkpoint_range.into_par_iter().try_for_each(|seq| {
            let checkpoint_data =
                sparse_checkpoint_data_for_backfill(authority_store, checkpoint_store, seq)?;

            let mut batch = self.transaction_checkpoints.batch();

            self.index_epoch(&checkpoint_data, &mut batch)?;
            self.index_transactions(&checkpoint_data, &mut batch)?;

            batch.write().map_err(StorageError::from)
        })?;

        info!(
            "Indexing checkpoints took {} seconds",
            start_time.elapsed().as_secs()
        );
        Ok(())
    }

    /// Prune data from this Index
    fn prune(
        &self,
        pruned_checkpoint_watermark: u64,
        checkpoint_contents_to_prune: &[CheckpointContents],
    ) -> Result<(), TypedStoreError> {
        let mut batch = self.transaction_checkpoints.batch();

        let transactions_to_prune = checkpoint_contents_to_prune
            .iter()
            .flat_map(|contents| contents.iter().map(|digests| digests.transaction));

        batch.delete_batch(&self.transaction_checkpoints, transactions_to_prune)?;
        batch.insert_batch(
            &self.watermark,
            [(Watermark::Pruned, pruned_checkpoint_watermark)],
        )?;

        batch.write()
    }

    /// Index a Checkpoint
    fn index_checkpoint(
        &self,
        checkpoint: &CheckpointData,
    ) -> Result<typed_store::rocks::DBBatch, StorageError> {
        debug!(
            checkpoint = checkpoint.checkpoint_summary.sequence_number,
            "indexing checkpoint"
        );

        let mut batch = self.transaction_checkpoints.batch();

        self.index_epoch(checkpoint, &mut batch)?;
        self.index_transactions(checkpoint, &mut batch)?;

        batch.insert_batch(
            &self.watermark,
            [(
                Watermark::Indexed,
                checkpoint.checkpoint_summary.sequence_number,
            )],
        )?;

        debug!(
            checkpoint = checkpoint.checkpoint_summary.sequence_number,
            "finished indexing checkpoint"
        );

        Ok(batch)
    }

    fn index_epoch(
        &self,
        checkpoint: &CheckpointData,
        batch: &mut typed_store::rocks::DBBatch,
    ) -> Result<(), StorageError> {
        let Some(epoch_info) = checkpoint.epoch_info()? else {
            return Ok(());
        };

        // We need to handle closing the previous epoch by updating the entry for it, if
        // it exists.
        if epoch_info.epoch > 0 {
            let prev_epoch = epoch_info.epoch - 1;

            if let Some(mut previous_epoch) = self.epochs.get(&prev_epoch)? {
                previous_epoch.end_timestamp_ms = Some(epoch_info.start_timestamp_ms);
                previous_epoch.end_checkpoint = Some(epoch_info.start_checkpoint - 1);
                batch.insert_batch(&self.epochs, [(prev_epoch, previous_epoch)])?;
            }
        }

        // Insert the current epoch info
        batch.insert_batch(&self.epochs, [(epoch_info.epoch, epoch_info)])?;

        Ok(())
    }

    // After attempting to reindex past epochs, ensure that the current epoch is at
    // least partially initialized
    fn initialize_current_epoch(
        &mut self,
        authority_store: &AuthorityStore,
        checkpoint_store: &CheckpointStore,
    ) -> Result<(), StorageError> {
        let Some(checkpoint) = checkpoint_store.get_highest_executed_checkpoint()? else {
            return Ok(());
        };

        if self.epochs.get(&checkpoint.epoch)?.is_some() {
            // no need to initialize if it already exists
            return Ok(());
        }

        let system_state = iota_types::iota_system_state::get_iota_system_state(authority_store)
            .map_err(|e| StorageError::custom(format!("Failed to find system state: {e}")))?;

        // Determine the start checkpoint of the current epoch
        let start_checkpoint = if checkpoint.epoch != 0 {
            let previous_epoch = checkpoint.epoch - 1;

            // Find the last checkpoint of the previous epoch
            if let Some(previous_epoch_info) = self.epochs.get(&previous_epoch)? {
                if let Some(end_checkpoint) = previous_epoch_info.end_checkpoint {
                    end_checkpoint + 1
                } else {
                    // Fall back to scanning checkpoints if the end_checkpoint is None
                    self.scan_for_epoch_start_checkpoint(
                        checkpoint_store,
                        checkpoint.sequence_number,
                        previous_epoch,
                    )?
                }
            } else {
                // Fall back to scanning checkpoints if the previous epoch info is missing
                self.scan_for_epoch_start_checkpoint(
                    checkpoint_store,
                    checkpoint.sequence_number,
                    previous_epoch,
                )?
            }
        } else {
            // First epoch starts at checkpoint 0
            0
        };

        let epoch_info = EpochInfo {
            epoch: checkpoint.epoch,
            protocol_version: system_state.protocol_version(),
            start_timestamp_ms: system_state.epoch_start_timestamp_ms(),
            end_timestamp_ms: None,
            start_checkpoint,
            end_checkpoint: None,
            reference_gas_price: system_state.reference_gas_price(),
            system_state,
        };

        self.epochs.insert(&epoch_info.epoch, &epoch_info)?;

        Ok(())
    }

    fn scan_for_epoch_start_checkpoint(
        &self,
        checkpoint_store: &CheckpointStore,
        current_checkpoint_seq_number: u64,
        previous_epoch: EpochId,
    ) -> Result<u64, StorageError> {
        // Scan from current checkpoint backwards to 0 to find the start of this epoch.
        let mut last_checkpoint_seq_number_of_prev_epoch = None;
        for seq in (0..=current_checkpoint_seq_number).rev() {
            let Some(chkpt) = checkpoint_store
                .get_checkpoint_by_sequence_number(seq)
                .ok()
                .flatten()
            else {
                // continue if there is a gap in the checkpoints
                continue;
            };

            if chkpt.epoch < previous_epoch {
                // we must stop searching if we are past the previous epoch
                break;
            }

            if chkpt.epoch == previous_epoch && chkpt.end_of_epoch_data.is_some() {
                // We found the checkpoint with end of epoch data for the previous epoch
                last_checkpoint_seq_number_of_prev_epoch = Some(chkpt.sequence_number);
                break;
            }
        }

        let last_checkpoint_seq_number_of_prev_epoch = last_checkpoint_seq_number_of_prev_epoch
            .ok_or(StorageError::custom(format!(
                "Failed to get the last checkpoint of the previous epoch {previous_epoch}",
            )))?;

        Ok(last_checkpoint_seq_number_of_prev_epoch + 1)
    }

    fn index_transactions(
        &self,
        checkpoint: &CheckpointData,
        batch: &mut typed_store::rocks::DBBatch,
    ) -> Result<(), StorageError> {
        let seq = checkpoint.checkpoint_summary.sequence_number;
        for tx in &checkpoint.transactions {
            let digest = tx.transaction.digest();
            batch.insert_batch(&self.transaction_checkpoints, [(digest, seq)])?;
        }

        Ok(())
    }

    fn get_epoch_info(&self, epoch: EpochId) -> Result<Option<EpochInfo>, TypedStoreError> {
        self.epochs.get(&epoch)
    }

    fn get_transaction_info(
        &self,
        digest: &TransactionDigest,
    ) -> Result<Option<TransactionInfo>, TypedStoreError> {
        Ok(self
            .transaction_checkpoints
            .get(digest)?
            .map(|checkpoint| TransactionInfo {
                checkpoint,
                object_types: Default::default(),
            }))
    }
}

pub struct GrpcIndexesStore {
    tables: IndexStoreTables,
    pending_updates: Mutex<BTreeMap<u64, typed_store::rocks::DBBatch>>,
}

impl GrpcIndexesStore {
    /// One-time migration: rename the legacy `rest_index` directory to
    /// [`GRPC_INDEXES_DIR`].
    ///
    /// Must be called before [`GrpcIndexesStore::new`] so that the DB is not
    /// yet open. Safe to call multiple times — it is a no-op when the target
    /// directory already exists.
    ///
    /// TODO(cleanup): Remove after one release cycle once all production nodes
    /// have upgraded past this version.
    pub fn migrate_legacy_dirs(db_path: &std::path::Path) {
        let target = db_path.join(GRPC_INDEXES_DIR);
        if target.exists() {
            return;
        }
        let legacy = db_path.join(LEGACY_INDEX_DIR);
        if legacy.exists() {
            info!(
                "migrating index directory: renaming {:?} -> {:?}",
                legacy, target
            );
            if let Err(e) = std::fs::rename(&legacy, &target) {
                // Non-fatal: GrpcIndexesStore::new will re-create and re-index.
                tracing::warn!(
                    "failed to rename {:?} to {:?}: {e}. \
                     The index will be rebuilt from scratch on next startup.",
                    legacy,
                    target
                );
            }
        }
    }

    pub async fn new(
        path: PathBuf,
        authority_store: &AuthorityStore,
        checkpoint_store: &CheckpointStore,
    ) -> Self {
        let tables = {
            let tables = IndexStoreTables::open(&path);

            // If the index tables are uninitialized or on an older version then we need to
            // populate them
            if tables.needs_to_do_initialization(checkpoint_store) {
                let mut tables = {
                    drop(tables);
                    typed_store::rocks::safe_drop_db(path.clone(), Duration::from_secs(30))
                        .await
                        .expect("unable to destroy old gRPC index db");
                    IndexStoreTables::open(path)
                };

                tables
                    .init(authority_store, checkpoint_store)
                    .expect("unable to initialize gRPC index");
                tables
            } else {
                tables
            }
        };

        Self {
            tables,
            pending_updates: Default::default(),
        }
    }

    pub fn new_without_init(path: PathBuf) -> Self {
        let tables = IndexStoreTables::open(path);

        Self {
            tables,
            pending_updates: Default::default(),
        }
    }

    pub fn checkpoint_db(&self, path: &Path) -> IotaResult {
        // We are checkpointing the whole db
        self.tables.meta.checkpoint_db(path).map_err(Into::into)
    }

    pub fn prune(
        &self,
        pruned_checkpoint_watermark: u64,
        checkpoint_contents_to_prune: &[CheckpointContents],
    ) -> Result<(), TypedStoreError> {
        self.tables
            .prune(pruned_checkpoint_watermark, checkpoint_contents_to_prune)
    }

    /// Index a checkpoint and stage the index updated in `pending_updates`.
    ///
    /// Updates will not be committed to the database until
    /// `commit_update_for_checkpoint` is called.
    #[tracing::instrument(
        skip_all,
        fields(checkpoint = checkpoint.checkpoint_summary.sequence_number)
    )]
    pub fn index_checkpoint(&self, checkpoint: &CheckpointData) {
        let sequence_number = checkpoint.checkpoint_summary.sequence_number;
        let batch = self.tables.index_checkpoint(checkpoint).expect("db error");

        self.pending_updates
            .lock()
            .unwrap()
            .insert(sequence_number, batch);
    }

    /// Commits the pending updates for the provided checkpoint number.
    ///
    /// Invariants:
    /// - `index_checkpoint` must have been called for the provided checkpoint
    /// - Callers of this function must ensure that it is called for each
    ///   checkpoint in sequential order. This will panic if the provided
    ///   checkpoint does not match the expected next checkpoint to commit.
    #[tracing::instrument(skip(self))]
    pub fn commit_update_for_checkpoint(&self, checkpoint: u64) -> Result<(), StorageError> {
        let next_batch = self.pending_updates.lock().unwrap().pop_first();

        // Its expected that the next batch exists
        let (next_sequence_number, batch) = next_batch.unwrap();
        assert_eq!(
            checkpoint, next_sequence_number,
            "commit_update_for_checkpoint must be called in order"
        );

        Ok(batch.write()?)
    }

    pub fn get_epoch_info(&self, epoch: EpochId) -> Result<Option<EpochInfo>, TypedStoreError> {
        self.tables.get_epoch_info(epoch)
    }

    pub fn get_transaction_info(
        &self,
        digest: &TransactionDigest,
    ) -> Result<Option<TransactionInfo>, TypedStoreError> {
        self.tables.get_transaction_info(digest)
    }
}

/// Build a lightweight `CheckpointData` for backfill indexing.
///
/// Only checkpoint summary, contents, and transaction blocks/effects are
/// fetched. Input/output objects are left empty because we only need the
/// transaction digests and checkpoint sequence number for the index.
fn sparse_checkpoint_data_for_backfill(
    authority_store: &AuthorityStore,
    checkpoint_store: &CheckpointStore,
    checkpoint: u64,
) -> Result<CheckpointData, StorageError> {
    use iota_types::full_checkpoint_content::CheckpointTransaction;

    let summary = checkpoint_store
        .get_checkpoint_by_sequence_number(checkpoint)?
        .ok_or_else(|| StorageError::missing(format!("missing checkpoint {checkpoint}")))?;
    let contents = checkpoint_store
        .get_checkpoint_contents(&summary.content_digest)?
        .ok_or_else(|| StorageError::missing(format!("missing checkpoint {checkpoint}")))?;

    let transaction_digests = contents
        .iter()
        .map(|execution_digests| execution_digests.transaction)
        .collect::<Vec<_>>();
    let transactions = authority_store
        .multi_get_transaction_blocks(&transaction_digests)?
        .into_iter()
        .map(|maybe_transaction| {
            maybe_transaction.ok_or_else(|| StorageError::custom("missing transaction"))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let effects = authority_store
        .multi_get_executed_effects(&transaction_digests)?
        .into_iter()
        .map(|maybe_effects| maybe_effects.ok_or_else(|| StorageError::custom("missing effects")))
        .collect::<Result<Vec<_>, _>>()?;

    let full_transactions = transactions
        .into_iter()
        .zip(effects)
        .map(|(tx, fx)| CheckpointTransaction {
            transaction: tx.into(),
            effects: fx,
            events: None,
            input_objects: vec![],
            output_objects: vec![],
        })
        .collect();

    Ok(CheckpointData {
        checkpoint_summary: summary.into(),
        checkpoint_contents: contents,
        transactions: full_transactions,
    })
}
