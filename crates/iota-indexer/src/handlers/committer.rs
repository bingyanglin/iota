// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2024 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, HashMap};

use iota_types::messages_checkpoint::CheckpointSequenceNumber;
use tap::tap::TapFallible;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument};

use super::{CheckpointDataToCommit, EpochToCommit};
use crate::{metrics::IndexerMetrics, store::IndexerStore, types::IndexerResult};

pub(crate) const CHECKPOINT_COMMIT_BATCH_SIZE: usize = 100;

pub async fn start_tx_checkpoint_commit_task<S>(
    state: S,
    metrics: IndexerMetrics,
    tx_indexing_receiver: iota_metrics::metered_channel::Receiver<CheckpointDataToCommit>,
    mut next_checkpoint_sequence_number: CheckpointSequenceNumber,
    cancel: CancellationToken,
) -> IndexerResult<()>
where
    S: IndexerStore + Clone + Sync + Send + 'static,
{
    use futures::StreamExt;

    info!(
        "Indexer checkpoint commit task started. Initial next_checkpoint_sequence_number: {}",
        next_checkpoint_sequence_number
    );
    let checkpoint_commit_batch_size = std::env::var("CHECKPOINT_COMMIT_BATCH_SIZE")
        .unwrap_or(CHECKPOINT_COMMIT_BATCH_SIZE.to_string())
        .parse::<usize>()
        .unwrap();
    info!("Using checkpoint commit batch size {checkpoint_commit_batch_size}");

    let mut stream = iota_metrics::metered_channel::ReceiverStream::new(tx_indexing_receiver)
        .ready_chunks(checkpoint_commit_batch_size);

    let mut unprocessed = HashMap::new();
    let mut batch = vec![];

    while let Some(indexed_checkpoint_batch_from_stream) = stream.next().await {
        info!(
            "Commit task main loop: received a chunk of {} checkpoints from ready_chunks.",
            indexed_checkpoint_batch_from_stream.len()
        );
        if cancel.is_cancelled() {
            info!("Commit task main loop: cancel signalled, breaking.");
            break;
        }

        for checkpoint_data_to_commit in indexed_checkpoint_batch_from_stream {
            info!(
                "Commit task main loop: received checkpoint {} from stream, adding to unprocessed.",
                checkpoint_data_to_commit.checkpoint.sequence_number
            );
            unprocessed.insert(
                checkpoint_data_to_commit.checkpoint.sequence_number,
                checkpoint_data_to_commit,
            );
        }
        info!(
            "Commit task main loop: unprocessed size after adding from stream: {}. Current next_checkpoint_sequence_number: {}",
            unprocessed.len(),
            next_checkpoint_sequence_number
        );

        while let Some(checkpoint) = unprocessed.remove(&next_checkpoint_sequence_number) {
            let current_cp_seq = checkpoint.checkpoint.sequence_number;
            info!(
                "Commit task inner loop: processing unprocessed checkpoint {} (matches next_checkpoint_sequence_number {}), adding to batch.",
                current_cp_seq, next_checkpoint_sequence_number
            );
            let epoch = checkpoint.epoch.clone();
            batch.push(checkpoint);
            next_checkpoint_sequence_number += 1;
            info!(
                "Commit task inner loop: batch_len: {}, checkpoint_commit_batch_size: {}, epoch_is_some: {}. New next_checkpoint_sequence_number: {}",
                batch.len(),
                checkpoint_commit_batch_size,
                epoch.is_some(),
                next_checkpoint_sequence_number
            );
            if batch.len() == checkpoint_commit_batch_size || epoch.is_some() {
                info!(
                    "Commit task inner loop: condition met (batch full or epoch end). Calling commit_checkpoints for {} items. First_seq in batch: {}. Last_seq in batch: {}.",
                    batch.len(),
                    batch.first().map_or("N/A".to_string(), |cp| cp
                        .checkpoint
                        .sequence_number
                        .to_string()),
                    batch.last().map_or("N/A".to_string(), |cp| cp
                        .checkpoint
                        .sequence_number
                        .to_string())
                );
                commit_checkpoints(&state, batch, epoch, &metrics).await;
                batch = vec![];
                info!("Commit task inner loop: batch cleared after commit.");
            }
        }
        info!(
            "Commit task main loop: finished inner while. Batch len: {}, Unprocessed len: {}.",
            batch.len(),
            unprocessed.len()
        );
        if !batch.is_empty() && unprocessed.is_empty() {
            info!(
                "Commit task main loop: condition met (drain remaining). Calling commit_checkpoints for {} items. First_seq in batch: {}. Last_seq in batch: {}.",
                batch.len(),
                batch.first().map_or("N/A".to_string(), |cp| cp
                    .checkpoint
                    .sequence_number
                    .to_string()),
                batch.last().map_or("N/A".to_string(), |cp| cp
                    .checkpoint
                    .sequence_number
                    .to_string())
            );
            commit_checkpoints(&state, batch, None, &metrics).await;
            batch = vec![];
            info!("Commit task main loop: batch cleared after draining commit.");
        }
    }
    info!("Indexer checkpoint commit task ended.");
    Ok(())
}

// Unwrap: Caller needs to make sure indexed_checkpoint_batch is not empty
#[instrument(skip_all, fields(
    first = indexed_checkpoint_batch.first().as_ref().unwrap().checkpoint.sequence_number,
    last = indexed_checkpoint_batch.last().as_ref().unwrap().checkpoint.sequence_number
))]
async fn commit_checkpoints<S>(
    state: &S,
    indexed_checkpoint_batch: Vec<CheckpointDataToCommit>,
    epoch: Option<EpochToCommit>,
    metrics: &IndexerMetrics,
) where
    S: IndexerStore + Clone + Sync + Send + 'static,
{
    if indexed_checkpoint_batch.is_empty() {
        // Should not happen due to caller logic, but good to guard.
        info!("commit_checkpoints called with empty batch, skipping.");
        return;
    }

    let first_seq = indexed_checkpoint_batch
        .first()
        .as_ref()
        .unwrap()
        .checkpoint
        .sequence_number;
    let last_seq = indexed_checkpoint_batch
        .last()
        .as_ref()
        .unwrap()
        .checkpoint
        .sequence_number;
    info!(
        "Commit task (commit_checkpoints fn): processing batch of {} checkpoints from {} to {}.",
        indexed_checkpoint_batch.len(),
        first_seq,
        last_seq
    );

    let mut checkpoint_batch = vec![];
    let mut tx_batch = vec![];
    let mut events_batch = vec![];
    let mut tx_indices_batch = vec![];
    let mut event_indices_batch = vec![];
    let mut display_updates_batch = BTreeMap::new();
    let mut object_changes_batch = vec![];
    let mut object_history_changes_batch = vec![];
    let mut object_versions_batch = vec![];
    let mut packages_batch = vec![];

    for indexed_checkpoint in indexed_checkpoint_batch {
        let CheckpointDataToCommit {
            checkpoint,
            transactions,
            events,
            event_indices,
            tx_indices,
            display_updates,
            object_changes,
            object_history_changes,
            object_versions,
            packages,
            epoch: _,
        } = indexed_checkpoint;
        checkpoint_batch.push(checkpoint);
        tx_batch.push(transactions);
        events_batch.push(events);
        tx_indices_batch.push(tx_indices);
        event_indices_batch.push(event_indices);
        display_updates_batch.extend(display_updates.into_iter());
        object_changes_batch.push(object_changes);
        object_history_changes_batch.push(object_history_changes);
        object_versions_batch.push(object_versions);
        packages_batch.push(packages);
    }

    let first_checkpoint_seq = checkpoint_batch.first().as_ref().unwrap().sequence_number;
    let last_checkpoint_seq = checkpoint_batch.last().as_ref().unwrap().sequence_number;

    let guard = metrics.checkpoint_db_commit_latency.start_timer();
    let tx_batch = tx_batch.into_iter().flatten().collect::<Vec<_>>();
    let tx_indices_batch = tx_indices_batch.into_iter().flatten().collect::<Vec<_>>();
    let events_batch = events_batch.into_iter().flatten().collect::<Vec<_>>();
    let event_indices_batch = event_indices_batch
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let object_versions_batch = object_versions_batch
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let packages_batch = packages_batch.into_iter().flatten().collect::<Vec<_>>();
    let checkpoint_num = checkpoint_batch.len();
    let tx_count = tx_batch.len();

    {
        let _step_1_guard = metrics.checkpoint_db_commit_latency_step_1.start_timer();
        let mut persist_tasks = vec![
            state.persist_transactions(tx_batch),
            state.persist_tx_indices(tx_indices_batch),
            state.persist_events(events_batch),
            state.persist_event_indices(event_indices_batch),
            state.persist_displays(display_updates_batch),
            state.persist_packages(packages_batch),
            state.persist_objects(object_changes_batch.clone()),
            state.persist_object_history(object_history_changes_batch.clone()),
            state.persist_object_versions(object_versions_batch.clone()),
        ];
        if let Some(epoch_data) = epoch.clone() {
            persist_tasks.push(state.persist_epoch(epoch_data));
        }
        futures::future::join_all(persist_tasks)
            .await
            .into_iter()
            .map(|res| {
                if res.is_err() {
                    error!("Failed to persist partial data in batch {}-{}: {:?}", first_seq, last_seq, res);
                }
                res
            })
            .collect::<IndexerResult<Vec<_>>>()
            .tap_err(|e| {
                error!("Critical error during batched data persistence for checkpoints {}-{}: {:?}. Data might be partially committed.", first_seq, last_seq, e);
            })
            .unwrap_or_else(|e| {
                // Log and potentially trigger a more graceful shutdown if possible,
                // instead of outright panic, especially if some data might be committed.
                error!("FATAL: Persisting partial data into DB FAILED for batch {}-{}: {:?}. Cancelling commit task to prevent inconsistent state.", first_seq, last_seq, e);
                // Ideally, we'd have a way to signal cancellation to the main task if we can't recover.
                // For now, this will likely lead to the outer task also failing if it tries to proceed.
                vec![] // Return an empty vec or handle error appropriately
            });
    }

    let is_epoch_end = epoch.is_some();

    // handle partitioning on epoch boundary
    if let Some(epoch_data) = epoch {
        state
            .advance_epoch(epoch_data)
            .await
            .tap_err(|e| {
                error!("Failed to advance epoch with error: {}", e.to_string());
            })
            .expect("Advancing epochs in DB should not fail.");
        metrics.total_epoch_committed.inc();

        // Refresh participation metrics after advancing epoch
        state
            .refresh_participation_metrics()
            .await
            .tap_err(|e| {
                error!("Failed to update participation metrics: {e}");
            })
            .expect("Updating participation metrics should not fail.");
    }

    state
        .persist_checkpoints(checkpoint_batch)
        .await
        .tap_err(|e| {
            error!(
                "Failed to persist main checkpoint entries for {}-{}: {:?}",
                first_seq, last_seq, e
            );
        })
        .unwrap_or_else(|e| {
            error!(
                "FATAL: Persisting main checkpoint entries FAILED for {}-{}: {:?}. Cancelling commit task.",
                first_seq, last_seq, e
            );
            // Signal cancellation or handle error
        });

    if is_epoch_end {
        // The epoch has advanced so we update the configs for the new protocol version,
        // if it has changed.
        let chain_id = state
            .get_chain_identifier()
            .await
            .expect("Failed to get chain identifier")
            .expect("Chain identifier should have been indexed at this point");
        let _ = state.persist_protocol_configs_and_feature_flags(chain_id);
    }

    let elapsed = guard.stop_and_record();

    info!(
        elapsed,
        "Checkpoint {}-{} committed with {} transactions.",
        first_checkpoint_seq,
        last_checkpoint_seq,
        tx_count,
    );
    metrics
        .max_committed_checkpoint_sequence_number
        .set(last_checkpoint_seq as i64);
    metrics
        .total_tx_checkpoint_committed
        .inc_by(checkpoint_num as u64);
    metrics.total_transaction_committed.inc_by(tx_count as u64);
    metrics
        .transaction_per_checkpoint
        .observe(tx_count as f64 / (last_checkpoint_seq - first_checkpoint_seq + 1) as f64);
    // 1000.0 is not necessarily the batch size, it's to roughly map average tx
    // commit latency to [0.1, 1] seconds, which is well covered by
    // DB_COMMIT_LATENCY_SEC_BUCKETS.
    metrics
        .thousand_transaction_avg_db_commit_latency
        .observe(elapsed * 1000.0 / tx_count as f64);
}
