// Copyright (c) 2026 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! One-time backfill of [`StoreObjectValueV2::previous_transaction_checkpoint`]
//! for live objects lifted from a pre-V2 on-disk format (which carry `None`).
//!
//! The snapshot V2 writer refuses to publish if any live object's
//! `previous_transaction_checkpoint` is `None`. A node that still holds pre-V2
//! (`StoreObjectV1`) rows in its live set therefore cannot publish V2 snapshots
//! without this backfill, which fills each `None` from the local
//! `executed_transactions_to_checkpoint` map. Rows whose mapping has already
//! been pruned stay `None` (unrecoverable locally — such a node must re-restore
//! from a V2 snapshot to publish).
//!
//! [`StoreObjectValueV2::previous_transaction_checkpoint`]: super::authority_store_types::StoreObjectValueV2::previous_transaction_checkpoint

use std::time::Duration;

use iota_types::{error::IotaResult, storage::ObjectKey};
use tracing::info;
use typed_store::{Map, rocks::DBBatch};

use super::{
    authority_store_tables::AuthorityPerpetualTables,
    authority_store_types::{StoreObject, StoreObjectWrapper},
};

/// Rows rewritten per RocksDB batch before flushing and pausing, to bound write
/// amplification on a large live set.
const PTC_BACKFILL_BATCH_ROWS: usize = 1000;
/// Pause between batches so the backfill does not starve normal execution.
const PTC_BACKFILL_BATCH_PAUSE: Duration = Duration::from_millis(50);

/// Outcome of a backfill run (for logging).
#[derive(Default, Debug, Clone, Copy)]
pub struct PtcBackfillStats {
    /// Highest-version rows scanned (one per object id, including tombstones).
    pub rows_scanned: u64,
    /// Live objects whose `previous_transaction_checkpoint` was `None`.
    pub candidates: u64,
    /// Candidates filled from the local checkpoint map.
    pub filled: u64,
    /// Candidates left `None` because the tx→checkpoint mapping was pruned.
    pub skipped_pruned: u64,
}

/// Fill every live object's missing `previous_transaction_checkpoint` from the
/// local checkpoint map. Synchronous (walks a `!Send` RocksDB iterator), so the
/// caller runs it on a blocking thread. A no-op once the completion marker is
/// set.
pub fn run_previous_tx_checkpoint_backfill(
    perpetual: &AuthorityPerpetualTables,
) -> IotaResult<PtcBackfillStats> {
    if perpetual.is_previous_tx_checkpoint_backfill_complete()? {
        return Ok(PtcBackfillStats::default());
    }
    info!("starting one-time previous_transaction_checkpoint backfill");

    let mut stats = PtcBackfillStats::default();
    let mut batch = perpetual.objects.batch();
    let mut pending = 0usize;

    // Mirror `LiveSetIter`'s dedup: the live row for an object id is its
    // highest-version row — the last one before the id changes (and the final
    // row overall). Only live rows feed the snapshot writer, so only they need
    // filling.
    let mut prev: Option<(ObjectKey, StoreObjectWrapper)> = None;
    for entry in perpetual.objects.safe_iter() {
        let (key, wrapper) = entry?;
        if let Some((prev_key, prev_wrapper)) = prev.take() {
            if prev_key.0 != key.0 {
                backfill_live_row(
                    perpetual,
                    prev_key,
                    prev_wrapper,
                    &mut batch,
                    &mut pending,
                    &mut stats,
                )?;
            }
        }
        prev = Some((key, wrapper));
    }
    if let Some((key, wrapper)) = prev.take() {
        backfill_live_row(
            perpetual,
            key,
            wrapper,
            &mut batch,
            &mut pending,
            &mut stats,
        )?;
    }
    if pending > 0 {
        batch.write()?;
    }

    perpetual.mark_previous_tx_checkpoint_backfill_complete()?;
    info!(?stats, "previous_transaction_checkpoint backfill complete");
    Ok(stats)
}

/// Fill `key`'s `previous_transaction_checkpoint` if it is a live `Value` row
/// still carrying `None`. Staged into `batch`, flushed every
/// [`PTC_BACKFILL_BATCH_ROWS`].
fn backfill_live_row(
    perpetual: &AuthorityPerpetualTables,
    key: ObjectKey,
    wrapper: StoreObjectWrapper,
    batch: &mut DBBatch,
    pending: &mut usize,
    stats: &mut PtcBackfillStats,
) -> IotaResult<()> {
    stats.rows_scanned += 1;
    // `migrate` lifts a pre-V2 row to V2 (with `None`); Deleted/Wrapped
    // tombstones have no checkpoint to fill.
    let StoreObject::Value(mut value) = wrapper.migrate().into_inner() else {
        return Ok(());
    };
    if value.previous_transaction_checkpoint.is_some() {
        return Ok(());
    }
    stats.candidates += 1;
    match perpetual.get_checkpoint_sequence_number(&value.previous_transaction)? {
        Some((_epoch, checkpoint)) => {
            value.previous_transaction_checkpoint = Some(checkpoint);
            batch.insert_batch(
                &perpetual.objects,
                std::iter::once((key, StoreObjectWrapper::from(StoreObject::Value(value)))),
            )?;
            stats.filled += 1;
            *pending += 1;
            if *pending >= PTC_BACKFILL_BATCH_ROWS {
                std::mem::replace(batch, perpetual.objects.batch()).write()?;
                *pending = 0;
                std::thread::sleep(PTC_BACKFILL_BATCH_PAUSE);
            }
        }
        // tx→checkpoint mapping pruned; cannot recover the checkpoint locally.
        None => stats.skipped_pruned += 1,
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use iota_types::{
        base_types::{ObjectID, TransactionDigest},
        object::Object,
        storage::ObjectKey,
    };
    use typed_store::Map;

    use super::*;

    /// Backfill fills a live `None` row whose `previous_transaction` is in the
    /// checkpoint map, leaves a row whose mapping is pruned as `None`, and is
    /// idempotent via its completion marker.
    #[tokio::test]
    async fn backfill_fills_from_map_skips_pruned_and_is_idempotent() {
        let tmp = iota_common::tempdir();
        let perpetual = AuthorityPerpetualTables::open(tmp.path(), None);

        // A: previous_transaction recorded in the map → will be filled.
        let tx_a = TransactionDigest::random();
        let obj_a = object_with_prev_tx(ObjectID::random(), tx_a);
        let key_a = ObjectKey::from(obj_a.compute_object_reference());
        perpetual
            .insert_store_object_v2_test_only(obj_a, None)
            .unwrap();

        // B: previous_transaction NOT in the map (pruned) → stays `None`.
        let tx_b = TransactionDigest::random();
        let obj_b = object_with_prev_tx(ObjectID::random(), tx_b);
        let key_b = ObjectKey::from(obj_b.compute_object_reference());
        perpetual
            .insert_store_object_v2_test_only(obj_b, None)
            .unwrap();

        let checkpoint: u64 = 5000;
        let mut wb = perpetual.executed_transactions_to_checkpoint.batch();
        wb.insert_batch(
            &perpetual.executed_transactions_to_checkpoint,
            [(tx_a, (0u64, checkpoint))],
        )
        .unwrap();
        wb.write().unwrap();

        let stats = run_previous_tx_checkpoint_backfill(&perpetual).unwrap();
        assert_eq!(stats.candidates, 2);
        assert_eq!(stats.filled, 1);
        assert_eq!(stats.skipped_pruned, 1);

        assert_eq!(ptc_of(&perpetual, &key_a), Some(checkpoint));
        assert_eq!(ptc_of(&perpetual, &key_b), None);
        assert!(
            perpetual
                .is_previous_tx_checkpoint_backfill_complete()
                .unwrap()
        );

        // Second run is a no-op once the marker is set.
        let again = run_previous_tx_checkpoint_backfill(&perpetual).unwrap();
        assert_eq!(again.rows_scanned, 0);
    }

    fn object_with_prev_tx(id: ObjectID, tx: TransactionDigest) -> Object {
        let mut inner = Object::immutable_with_id_for_testing(id).into_inner();
        inner.previous_transaction = tx;
        inner.into()
    }

    fn ptc_of(perpetual: &AuthorityPerpetualTables, key: &ObjectKey) -> Option<u64> {
        match perpetual
            .objects
            .get(key)
            .unwrap()
            .unwrap()
            .migrate()
            .into_inner()
        {
            StoreObject::Value(value) => value.previous_transaction_checkpoint,
            other => panic!("expected StoreObject::Value, got {other:?}"),
        }
    }
}
