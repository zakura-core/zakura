//! Startup audit and self-repair for the zakura header store.
//!
//! The zakura header store is five height-indexed column families acting as a
//! replicated view of the canonical header chain above the finalized tip. Its
//! invariants (hash↔height bijection, parent linkage anchored at the finalized
//! tip, no gaps or stranded rows below the header tip) are enforced by the
//! writers, but a store corrupted by an earlier binary stays corrupted on disk
//! and wedges header sync: consensus writes can no longer anchor cleanly in the
//! poisoned window, so the node can no longer make progress past it.
//!
//! This module runs the store audit once at [`ZakuraDb`] startup and repairs
//! any violation by truncating the zakura column families to the last
//! coherent height in bounded batches. Headers are re-fetchable — header sync
//! re-downloads the truncated suffix — so correctness beats preserved rows:
//! any residual write-path bug in this class becomes a self-healing,
//! observable transient instead of a permanent on-disk wedge.
//!
//! The audit cost is `O(header frontier)`: the zakura column families only
//! hold rows above the finalized tip (plus any stale rows this audit exists
//! to remove), and the verified commitment-roots history below the tip is
//! never scanned.

use std::{fmt::Debug, ops::Bound::*, sync::Arc};

use zakura_chain::block::{self, Height};

use super::{
    AdvertisedBodySize, ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT, ZAKURA_HEADER_BY_HEIGHT,
    ZAKURA_HEADER_HASH_BY_HEIGHT, ZAKURA_HEADER_HEIGHT_BY_HASH,
};
use crate::service::finalized_state::{
    disk_db::{DiskWriteBatch, ReadDisk, WriteDisk},
    disk_format::{shielded::CommitmentRootsByHeight, FromDisk, IntoDisk},
    zakura_db::ZakuraDb,
    COMMITMENT_ROOTS_BY_HEIGHT,
};

/// How many violations are included in the repair log line. The full list can
/// be as long as the stranded suffix; the first few identify the fault shape.
const LOGGED_VIOLATIONS: usize = 8;

/// Maximum number of rows the startup audit materializes from one column
/// family at a time. Roughly 1.5MB with 1.5 KB headers.
const AUDIT_BATCH_ROWS: usize = 100_000;

/// A single header-store invariant violation found by the startup audit.
///
/// Every violation is repaired by deleting rows; the variants exist for
/// logging and for test assertions on the fault shape. The fields are
/// diagnostic payload rendered through `Debug` in the repair warning.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) enum ZakuraStoreViolation {
    /// A zakura row at a height with a committed block (`contains_height`).
    ///
    /// Committed heights have authoritative full-block rows, and the zakura
    /// row at a height is trimmed when its body commits, so a surviving row
    /// here is stale (the pre-guard re-delivery bug shape). The predicate is
    /// the consensus `hash_by_height` row — which pruning retains — not body
    /// presence, so stale rows at pruned heights are found too.
    StaleRowAtCommittedHeight {
        /// The column family holding the stale row.
        cf: &'static str,
        /// The committed height of the stale row.
        height: Height,
    },

    /// A `hash_by_height` row at `height` has no `header_by_height` row.
    MissingHeaderRow {
        /// The height with a hash row but no header row.
        height: Height,
    },

    /// The header stored at `height` is not the block its hash row names.
    HeaderHashMismatch {
        /// The height of the divergent rows.
        height: Height,
        /// The hash the height→hash index names.
        indexed: block::Hash,
        /// The stored header's actual hash.
        computed: block::Hash,
    },

    /// The header at `height` does not link to the stored row below it.
    BrokenLinkage {
        /// The height of the header whose parent link failed to resolve.
        height: Height,
        /// The parent hash the header claims (`previous_block_hash`).
        expected_parent: block::Hash,
        /// The hash actually stored at `height - 1`.
        actual_below: block::Hash,
    },

    /// The `height_by_hash` index is missing or wrong for the hash stored at
    /// `height` (a forward bijection failure).
    WrongHeightByHash {
        /// The height whose hash failed the round-trip.
        height: Height,
        /// The hash stored at `height`.
        hash: block::Hash,
        /// The height the hash→height index reports, if any.
        indexed: Option<Height>,
    },

    /// A row above the last coherent height (a stranded suffix: rows above a
    /// gap, above a broken link, or above the linked chain's tip).
    RowAboveLastCoherent {
        /// The column family holding the stranded row.
        cf: &'static str,
        /// The stranded height.
        height: Height,
    },

    /// A `height_by_hash` entry whose target height stores a different hash
    /// (or no row at all): a stranded reverse-index row.
    OrphanHeightByHash {
        /// The hash of the stranded entry.
        hash: block::Hash,
        /// The height the entry points at.
        points_at: Height,
    },
}

/// The outcome of a startup repair: what was found and what was deleted.
///
/// The fields are diagnostic payload: read by test assertions and rendered
/// through `Debug`.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct ZakuraStoreRepair {
    /// The last height that passed every audit check, and its hash.
    ///
    /// Rows above it were truncated. `None` means the store had zakura rows
    /// but no finalized tip to anchor them at, so all rows were removed.
    pub last_coherent: Option<(Height, block::Hash)>,

    /// The number of rows deleted across all five column families.
    pub deleted_rows: usize,

    /// Every violation found, in audit order.
    pub violations: Vec<ZakuraStoreViolation>,
}

impl ZakuraDb {
    /// Audits the zakura header store invariants and repairs any violation by
    /// deleting the offending rows in bounded batches.
    ///
    /// Checks, over the whole zakura store (which only holds the header
    /// frontier above the finalized tip, so this is cheap):
    ///
    /// - **bijection**: `hash_by_height` ↔ `height_by_hash` are mutually
    ///   inverse, and every stored header is the block its hash row names;
    /// - **linkage**: rows chain by `previous_block_hash` from the finalized
    ///   tip hash upward;
    /// - **tip integrity**: no rows in any zakura column family above the
    ///   last linked height, no gaps below it, and no zakura rows at
    ///   committed heights.
    ///
    /// On violation, emits the `state.zakura.header_store.incoherent` metric
    /// and a warning, then truncates the zakura column families to the last
    /// coherent height (and removes stale rows below the finalized tip and
    /// orphaned reverse-index entries). Header sync re-downloads the
    /// truncated suffix. A store whose corrupted indexes prevent anchored
    /// header writes is repaired by this audit, because the audit checks are a
    /// superset of the writer's anchor checks over the same rows.
    ///
    /// Returns `Ok(None)` if the store is coherent (nothing is written), or
    /// the repair summary after a successful repair write.
    ///
    /// Verified commitment roots at committed heights are never touched: the
    /// roots column family is only scanned above the finalized tip. Each scan
    /// is batched so a large header frontier does not have to fit in memory at
    /// startup.
    pub(crate) fn audit_and_repair_zakura_header_store(
        &self,
    ) -> Result<Option<ZakuraStoreRepair>, rocksdb::Error> {
        // Databases opened through `ZakuraDb::new` without the zakura column
        // families have no header store to audit.
        let (
            Some(header_cf),
            Some(hash_cf),
            Some(height_by_hash_cf),
            Some(body_size_cf),
            Some(roots_cf),
        ) = (
            self.db.cf_handle(ZAKURA_HEADER_BY_HEIGHT),
            self.db.cf_handle(ZAKURA_HEADER_HASH_BY_HEIGHT),
            self.db.cf_handle(ZAKURA_HEADER_HEIGHT_BY_HASH),
            self.db.cf_handle(ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT),
            self.db.cf_handle(COMMITMENT_ROOTS_BY_HEIGHT),
        )
        else {
            return Ok(None);
        };

        let finalized_tip = self.tip();

        // The roots column family also holds verified rows at committed
        // heights (written by body commits, kept through pruning); those are
        // not zakura frontier rows and are never audited or repaired. Only
        // rows above the finalized tip are provisional header-sync data.
        let provisional_roots_start =
            finalized_tip.and_then(|(tip_height, _)| tip_height.next().ok());
        let provisional_roots_empty = match (finalized_tip, provisional_roots_start) {
            // The finalized tip is at the maximum height: no frontier can
            // exist above it.
            (Some(_), None) => true,
            (Some(_), Some(start)) => self
                .db
                .zs_forward_range_iter::<_, Height, CommitmentRootsByHeight, _>(&roots_cf, start..)
                .next()
                .is_none(),
            // No finalized tip: roots rows can be committed history whose
            // tip index is missing, and header sync cannot restore them.
            (None, _) => true,
        };

        if self.db.zs_is_empty(&header_cf)
            && self.db.zs_is_empty(&hash_cf)
            && self.db.zs_is_empty(&height_by_hash_cf)
            && self.db.zs_is_empty(&body_size_cf)
            && provisional_roots_empty
        {
            // Log the pass so operators can verify the audit ran on this boot.
            tracing::info!(
                ?finalized_tip,
                frontier_rows = 0,
                "zakura header store passed its startup coherence audit (empty frontier)"
            );
            return Ok(None);
        }

        let mut violations = Vec::new();

        // Walk the linked chain upward from the finalized tip, verifying at
        // each height that the hash and header rows agree, the header links
        // to the row below, and the reverse index round-trips. The walk stops
        // at the first missing hash row (the candidate chain tip) or the
        // first violation; everything it passed is the coherent prefix.
        let last_coherent = finalized_tip.map(|(anchor_height, anchor_hash)| {
            let (mut last_height, mut last_hash) = (anchor_height, anchor_hash);

            while let Ok(height) = last_height.next() {
                let Some(hash) = self.db.zs_get(&hash_cf, &height) else {
                    break;
                };
                let Some(header): Option<Arc<block::Header>> = self.db.zs_get(&header_cf, &height)
                else {
                    violations.push(ZakuraStoreViolation::MissingHeaderRow { height });
                    break;
                };

                let computed = block::Hash::from(&*header);
                if computed != hash {
                    violations.push(ZakuraStoreViolation::HeaderHashMismatch {
                        height,
                        indexed: hash,
                        computed,
                    });
                    break;
                }

                if header.previous_block_hash != last_hash {
                    violations.push(ZakuraStoreViolation::BrokenLinkage {
                        height,
                        expected_parent: header.previous_block_hash,
                        actual_below: last_hash,
                    });
                    break;
                }

                let indexed = self.db.zs_get(&height_by_hash_cf, &hash);
                if indexed != Some(height) {
                    violations.push(ZakuraStoreViolation::WrongHeightByHash {
                        height,
                        hash,
                        indexed,
                    });
                    break;
                }

                (last_height, last_hash) = (height, hash);
            }

            (last_height, last_hash)
        });

        // Committed heights have authoritative full-block rows: the consensus
        // `hash_by_height` rows are contiguous from genesis to the finalized
        // tip and retained by pruning, so `height <= finalized tip` is
        // exactly `contains_height` — the same predicate as the writers'
        // committed-height insert gate. (A body-presence predicate would miss
        // stale rows at pruned heights.)
        let committed =
            |height: Height| finalized_tip.is_some_and(|(tip_height, _)| height <= tip_height);
        // The coherent frontier window: strictly above the finalized tip, at
        // or below the last height the linkage walk verified.
        let in_window = |height: Height| {
            !committed(height)
                && last_coherent.is_some_and(|(last_height, _)| height <= last_height)
        };

        let mut batch = DiskWriteBatch::new();
        let mut deleted_rows = 0;
        let mut pending_deletes = 0;

        // Reverse-index entries survive only when their target height is
        // inside the window and stores exactly their hash. This removes the
        // reverse rows of every deleted hash row, plus entries orphaned by
        // earlier overwrites that never cleaned up the displaced hash.
        let mut start_after_hash = None;
        loop {
            let heights_by_hash =
                hash_keyed_batch::<Height>(&self.db, &height_by_hash_cf, start_after_hash);
            let Some(&(last_hash, _)) = heights_by_hash.last() else {
                break;
            };
            start_after_hash = Some(last_hash);

            for &(hash, points_at) in &heights_by_hash {
                let target_matches =
                    self.db.zs_get::<_, _, block::Hash>(&hash_cf, &points_at) == Some(hash);
                if in_window(points_at) && target_matches {
                    continue;
                }

                // Entries whose forward row is deleted above are repair fallout,
                // not separate faults; only a live-but-disagreeing target is a
                // distinct violation shape worth reporting.
                if target_matches {
                    violations.push(if committed(points_at) {
                        ZakuraStoreViolation::StaleRowAtCommittedHeight {
                            cf: ZAKURA_HEADER_HEIGHT_BY_HASH,
                            height: points_at,
                        }
                    } else {
                        ZakuraStoreViolation::RowAboveLastCoherent {
                            cf: ZAKURA_HEADER_HEIGHT_BY_HASH,
                            height: points_at,
                        }
                    });
                } else {
                    violations.push(ZakuraStoreViolation::OrphanHeightByHash { hash, points_at });
                }

                queue_repair_delete(
                    &self.db,
                    &mut batch,
                    &mut pending_deletes,
                    &height_by_hash_cf,
                    hash,
                )?;
                deleted_rows += 1;
            }
        }

        // Height-keyed zakura rows survive only inside the coherent window.
        audit_height_keyed_rows::<Arc<block::Header>>(
            &self.db,
            &header_cf,
            ZAKURA_HEADER_BY_HEIGHT,
            &committed,
            &in_window,
            &mut violations,
            &mut batch,
            &mut pending_deletes,
            &mut deleted_rows,
            None,
        )?;
        let frontier_rows = audit_height_keyed_rows::<block::Hash>(
            &self.db,
            &hash_cf,
            ZAKURA_HEADER_HASH_BY_HEIGHT,
            &committed,
            &in_window,
            &mut violations,
            &mut batch,
            &mut pending_deletes,
            &mut deleted_rows,
            None,
        )?;
        audit_height_keyed_rows::<AdvertisedBodySize>(
            &self.db,
            &body_size_cf,
            ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT,
            &committed,
            &in_window,
            &mut violations,
            &mut batch,
            &mut pending_deletes,
            &mut deleted_rows,
            None,
        )?;

        // Provisional roots above the window are part of the stranded suffix.
        // If the finalized tip is missing, roots rows are not auditable: they
        // might be committed history whose tip index was damaged.
        if let Some(start) = match (finalized_tip, provisional_roots_start) {
            (Some(_), None) => None,
            (Some(_), Some(start)) => Some(start),
            (None, _) => None,
        } {
            audit_height_keyed_rows::<CommitmentRootsByHeight>(
                &self.db,
                &roots_cf,
                COMMITMENT_ROOTS_BY_HEIGHT,
                &committed,
                &in_window,
                &mut violations,
                &mut batch,
                &mut pending_deletes,
                &mut deleted_rows,
                Some(start),
            )?;
        }

        if violations.is_empty() {
            // Log the pass so operators can verify the audit ran on this boot
            // (the fleet soak requires observing it finding nothing).
            tracing::info!(
                ?finalized_tip,
                ?last_coherent,
                frontier_rows,
                "zakura header store passed its startup coherence audit"
            );
            return Ok(None);
        }

        metrics::counter!("state.zakura.header_store.incoherent").increment(1);
        tracing::warn!(
            ?finalized_tip,
            ?last_coherent,
            deleted_rows,
            violation_count = violations.len(),
            first_violations = ?&violations[..violations.len().min(LOGGED_VIOLATIONS)],
            "zakura header store failed its startup coherence audit; \
             truncating to the last coherent height so header sync re-downloads the rest"
        );

        flush_repair_batch(&self.db, &mut batch, &mut pending_deletes)?;

        Ok(Some(ZakuraStoreRepair {
            last_coherent,
            deleted_rows,
            violations,
        }))
    }
}

fn height_keyed_batch<V>(
    db: &crate::service::finalized_state::disk_db::DiskDb,
    cf: &rocksdb::ColumnFamilyRef<'_>,
    start: Height,
) -> Vec<Height>
where
    V: FromDisk,
{
    db.zs_forward_range_iter::<_, Height, V, _>(cf, start..)
        .map(|(height, _)| height)
        .take(AUDIT_BATCH_ROWS)
        .collect()
}

fn hash_keyed_batch<V>(
    db: &crate::service::finalized_state::disk_db::DiskDb,
    cf: &rocksdb::ColumnFamilyRef<'_>,
    start_after: Option<block::Hash>,
) -> Vec<(block::Hash, V)>
where
    V: FromDisk,
{
    match start_after {
        Some(start_after) => db
            .zs_forward_range_iter::<_, block::Hash, V, _>(cf, (Excluded(start_after), Unbounded))
            .take(AUDIT_BATCH_ROWS)
            .collect(),
        None => db
            .zs_forward_range_iter::<_, block::Hash, V, _>(cf, ..)
            .take(AUDIT_BATCH_ROWS)
            .collect(),
    }
}

#[allow(clippy::too_many_arguments)]
fn audit_height_keyed_rows<V>(
    db: &crate::service::finalized_state::disk_db::DiskDb,
    cf: &rocksdb::ColumnFamilyRef<'_>,
    cf_name: &'static str,
    committed: &impl Fn(Height) -> bool,
    in_window: &impl Fn(Height) -> bool,
    violations: &mut Vec<ZakuraStoreViolation>,
    batch: &mut DiskWriteBatch,
    pending_deletes: &mut usize,
    deleted_rows: &mut usize,
    start: Option<Height>,
) -> Result<usize, rocksdb::Error>
where
    V: FromDisk,
{
    let mut next_start = start.unwrap_or(Height(0));
    let mut scanned_rows = 0;

    loop {
        let heights = height_keyed_batch::<V>(db, cf, next_start);
        let Some(&last_height) = heights.last() else {
            break;
        };
        scanned_rows += heights.len();
        next_start = match last_height.next() {
            Ok(next_height) => next_height,
            Err(_) => break,
        };

        for height in heights {
            if in_window(height) {
                continue;
            }

            violations.push(if committed(height) {
                ZakuraStoreViolation::StaleRowAtCommittedHeight {
                    cf: cf_name,
                    height,
                }
            } else {
                ZakuraStoreViolation::RowAboveLastCoherent {
                    cf: cf_name,
                    height,
                }
            });

            queue_repair_delete(db, batch, pending_deletes, cf, height)?;
            *deleted_rows += 1;
        }
    }

    Ok(scanned_rows)
}

fn queue_repair_delete<K>(
    db: &crate::service::finalized_state::disk_db::DiskDb,
    batch: &mut DiskWriteBatch,
    pending_deletes: &mut usize,
    cf: &rocksdb::ColumnFamilyRef<'_>,
    key: K,
) -> Result<(), rocksdb::Error>
where
    K: IntoDisk + Debug,
{
    batch.zs_delete(cf, key);
    *pending_deletes += 1;

    if *pending_deletes >= AUDIT_BATCH_ROWS {
        flush_repair_batch(db, batch, pending_deletes)?;
    }

    Ok(())
}

fn flush_repair_batch(
    db: &crate::service::finalized_state::disk_db::DiskDb,
    batch: &mut DiskWriteBatch,
    pending_deletes: &mut usize,
) -> Result<(), rocksdb::Error> {
    if *pending_deletes == 0 {
        return Ok(());
    }

    db.write(std::mem::take(batch))?;
    *pending_deletes = 0;
    Ok(())
}
