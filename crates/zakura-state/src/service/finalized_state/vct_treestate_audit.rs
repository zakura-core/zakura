//! Read-only audit of a database's historical-treestate serving inputs.
//!
//! A verified-commitment-trees fast-synced node cannot read per-height note commitment trees across
//! its absent band `[U, H)`, and instead rebuilds them by replaying block bodies forward and
//! checking the result against the authenticated roots in `commitment_roots_by_height` (see
//! [`crate::service::read::historical_tree`]). That rests on two inputs actually being present:
//! a gap-free root index across the band, and retained block bodies. This module reports whether
//! they are, checks the pre-band anchor frontiers, and measures what the replay costs.
//!
//! Everything here is read-only, runs off the consensus path, and is safe against a quiesced
//! database snapshot.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

use zakura_chain::{block::Height, subtree::NoteCommitmentSubtreeIndex};

use crate::service::{
    finalized_state::{CommitmentRootIndexIssue, ZakuraDb},
    read::{
        historical_tree::{
            derive_historical_frontiers_measured, replay_with_subtrees, verify_against_index,
            CompletedSubtree, HistoricalTreeDerivationError, ShieldedPool,
        },
        DerivedFrontiers, HistoricalTreeCache,
    },
};

/// What a database offers for serving historical treestates.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VctTreestateInventory {
    /// The finalized tip height.
    pub finalized_tip: Option<Height>,

    /// The verified-commitment-trees upgrade height `U`: the lowest height this binary committed.
    ///
    /// `None` on a database written before this marker existed.
    pub upgrade_height: Option<Height>,

    /// The last checkpoint height `H`, the exclusive upper bound of the absent band.
    ///
    /// `None` on a normally-synced database, which has per-height trees everywhere and needs
    /// nothing from this design.
    pub last_checkpoint: Option<Height>,

    /// The lowest height whose raw transactions are retained, if this database is pruned.
    ///
    /// Replay reads block bodies, so a pruned database cannot derive below this height.
    pub lowest_retained_height: Option<Height>,

    /// Whether the authenticated-root index scan ran.
    pub root_index_scanned: bool,

    /// Whether the retained block-body scan ran.
    pub block_bodies_scanned: bool,

    /// The first height in the absent band with no `commitment_roots_by_height` row.
    ///
    /// `None` means the index is gap-free across the band, which is what derivation needs: every
    /// derived frontier is checked against the row at its own height. Only meaningful when
    /// [`Self::root_index_scanned`].
    pub root_index_gap: Option<Height>,

    /// The first height in the absent band with a malformed `commitment_roots_by_height` value.
    pub malformed_root_row: Option<Height>,

    /// The first height in the absent band whose block body is not retained.
    ///
    /// `None` means the whole band is replayable. Only meaningful when
    /// [`Self::block_bodies_scanned`].
    pub missing_block_body: Option<Height>,

    /// The required pre-band frontier height, if any pool has no stored tree at or below it.
    pub missing_anchor: Option<Height>,

    /// How long the two scans took.
    pub scan_duration: Duration,
}

impl VctTreestateInventory {
    /// Returns the committed part of the absent band `[U, min(H, T + 1))`.
    pub fn absent_band(&self) -> Option<(Height, Height)> {
        let last_checkpoint = self.last_checkpoint?;
        let finalized_tip = self.finalized_tip?;
        let upgrade = self.upgrade_height.unwrap_or(Height(0));
        let committed_end = Height(last_checkpoint.0.min(finalized_tip.0.saturating_add(1)));

        (upgrade < committed_end).then_some((upgrade, committed_end))
    }

    /// Returns whether this database has everything derivation needs across its absent band.
    ///
    /// Returns `None` if no problem is known but either full-band scan was skipped.
    pub fn can_derive(&self) -> Option<bool> {
        let known_problem = self.absent_band().is_none()
            || self.root_index_gap.is_some()
            || self.malformed_root_row.is_some()
            || self.missing_block_body.is_some()
            || self.missing_anchor.is_some();
        if known_problem {
            Some(false)
        } else if self.root_index_scanned && self.block_bodies_scanned {
            Some(true)
        } else {
            None
        }
    }
}

/// Inspects `db` for the inputs historical-treestate derivation depends on.
///
/// With `scan_band`, visits every height in the absent band to check the root index and block-body
/// retention, which is proportional to the band's height range rather than constant time. Without
/// it, only the cheap markers are read.
pub fn inventory(db: &ZakuraDb, scan_band: bool) -> VctTreestateInventory {
    inventory_with_scans(db, scan_band, scan_band)
}

/// Inspects `db`, allowing callers that will replay the whole band to defer the redundant
/// block-body scan while retaining the compact authenticated-root preflight.
pub fn inventory_with_scans(
    db: &ZakuraDb,
    scan_root_index: bool,
    scan_block_bodies: bool,
) -> VctTreestateInventory {
    let start = Instant::now();

    let upgrade_height = db.vct_upgrade_height();
    let last_checkpoint = db.vct_synced_below();

    let mut inventory = VctTreestateInventory {
        finalized_tip: db.finalized_tip_height(),
        upgrade_height,
        last_checkpoint,
        lowest_retained_height: db.lowest_retained_height(),
        root_index_scanned: scan_root_index,
        block_bodies_scanned: scan_block_bodies,
        root_index_gap: None,
        malformed_root_row: None,
        missing_block_body: None,
        missing_anchor: None,
        scan_duration: Duration::ZERO,
    };

    if let Some((band_start, band_end)) = inventory.absent_band() {
        // The band is half-open, so the last height it covers is `H - 1`.
        let last = Height(band_end.0 - 1);
        if scan_root_index {
            match db.first_commitment_root_issue(band_start..=last) {
                Some(CommitmentRootIndexIssue::Missing(height)) => {
                    inventory.root_index_gap = Some(height);
                }
                Some(CommitmentRootIndexIssue::Malformed(height)) => {
                    inventory.malformed_root_row = Some(height);
                }
                None => {}
            }
        }
        if scan_block_bodies {
            inventory.missing_block_body = db.first_missing_block_body(band_start, last);
        }
    }

    if let Some((band_start, _)) = inventory.absent_band() {
        if band_start.0 > 0 {
            let anchor = Height(band_start.0 - 1);
            if db.latest_stored_sapling_tree(&anchor).is_none()
                || db.latest_stored_orchard_tree(&anchor).is_none()
                || db.latest_stored_ironwood_tree(&anchor).is_none()
            {
                inventory.missing_anchor = Some(anchor);
            }
        }
    }

    inventory.scan_duration = start.elapsed();
    inventory
}

/// The cost of deriving one historical frontier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DerivationSample {
    /// The height derived.
    pub height: Height,

    /// How many blocks the derivation replayed.
    ///
    /// Zero means the height was already memoized, so nothing was replayed.
    pub replayed_blocks: u64,

    /// How long the derivation took, including the root check.
    pub elapsed: Duration,
}

impl DerivationSample {
    /// Returns the average time per replayed block, or `None` if nothing was replayed.
    pub fn per_block(&self) -> Option<Duration> {
        (self.replayed_blocks > 0)
            .then(|| self.elapsed / u32::try_from(self.replayed_blocks).unwrap_or(u32::MAX))
    }
}

/// Derives the frontiers at each of `heights`, in order, timing each derivation.
///
/// Every derivation is root-checked against `commitment_roots_by_height`, so a returned `Ok` for a
/// height *is* a root match at that height, and the first mismatch stops the walk. That makes this
/// both the invariant check and the cost measurement.
///
/// `cache` carries memoized frontiers between heights. Pass a fresh cache to measure cold cost from
/// the bottom of the band; reuse one across ascending heights to measure the sequential cost a
/// wallet actually pays.
pub fn measure_derivations(
    db: &ZakuraDb,
    cache: &std::sync::Mutex<HistoricalTreeCache>,
    heights: impl IntoIterator<Item = Height>,
    max_replay_blocks: u64,
    mut on_sample: impl FnMut(&DerivationSample),
) -> Result<(), (Height, HistoricalTreeDerivationError)> {
    for height in heights {
        let start = Instant::now();
        // The derivation reports its own replay length: it may have anchored on the memo, on a
        // published grid entry, or on genesis, and only it knows which.
        let derivation = derive_historical_frontiers_measured(db, cache, height, max_replay_blocks)
            .map_err(|error| (height, error))?;
        let elapsed = start.elapsed();
        let replayed_blocks = derivation.replayed_blocks;

        let sample = DerivationSample {
            height,
            replayed_blocks,
            elapsed,
        };
        on_sample(&sample);
    }

    Ok(())
}

/// The outcome of checking replay-derived subtrees against the ones the database stored.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SubtreeVerification {
    /// Subtrees that completed during the replay and matched the stored root and completion height.
    pub matched: usize,

    /// Subtrees that completed during the replay but whose stored root or completion height differs.
    ///
    /// Any entry here falsifies the claim that replay reproduces subtree roots, which is what the
    /// generated subtree artifact rests on.
    pub mismatched: Vec<(NoteCommitmentSubtreeIndex, &'static str)>,

    /// Subtrees that completed during the replay with no stored row to compare against.
    ///
    /// Any entry makes verification incomplete: replay only covers the band above the last checkpoint,
    /// where every completed subtree is expected to have a stored row.
    pub unstored: usize,

    /// Stored subtrees whose completion height is in the replay range, but which replay did not
    /// produce.
    ///
    /// Any entry is an extra or stale database row and makes the comparison incomplete in the
    /// storage-to-replay direction.
    pub stored_only: Vec<(NoteCommitmentSubtreeIndex, &'static str)>,
}

/// Replays `(from, to]`, authenticates the final frontiers at `to`, and compares replayed and
/// stored subtrees in both directions over that range.
///
/// This exists because subtree roots produced by replay are otherwise unvalidated: they are
/// interior nodes, so the per-height root check does not test them directly. Above a fast-synced
/// node's last checkpoint the database *does* store subtree rows, which makes that band the one
/// place the two can be compared. `from` must be a height whose per-height trees are present, so
/// the replay starts from a known-good frontier rather than reconstructing one. `to` must be
/// strictly greater than `from`; an empty or reversed range cannot verify any replay.
pub fn verify_subtrees_against_stored(
    db: &ZakuraDb,
    from: Height,
    to: Height,
) -> Result<SubtreeVerification, HistoricalTreeDerivationError> {
    if to <= from {
        return Err(HistoricalTreeDerivationError::InvalidReplayRange { from, to });
    }

    let (Some(sapling), Some(orchard), Some(ironwood)) = (
        db.latest_stored_sapling_tree(&from),
        db.latest_stored_orchard_tree(&from),
        db.latest_stored_ironwood_tree(&from),
    ) else {
        return Err(HistoricalTreeDerivationError::MissingAnchor {
            height: to,
            anchor: from,
        });
    };

    let anchor = DerivedFrontiers {
        sapling,
        orchard,
        ironwood,
    };

    let mut completions = Vec::new();
    let frontiers = replay_with_subtrees(db, to, from.0 + 1, anchor, |pool, completed| {
        completions.push((pool, completed))
    })?;
    verify_against_index(db, to, &frontiers)?;

    let stored = db
        .sapling_subtree_list_by_index_range(..)
        .into_iter()
        .map(|(index, data)| (("sapling", index), (data.root.to_bytes(), data.end_height)))
        .chain(
            db.orchard_subtree_list_by_index_range(..)
                .into_iter()
                .map(|(index, data)| (("orchard", index), (data.root.to_repr(), data.end_height))),
        )
        .chain(
            db.ironwood_subtree_list_by_index_range(..)
                .into_iter()
                .map(|(index, data)| (("ironwood", index), (data.root.to_repr(), data.end_height))),
        )
        .collect();

    Ok(compare_subtrees(completions, stored, from, to))
}

fn compare_subtrees(
    completions: impl IntoIterator<Item = (ShieldedPool, CompletedSubtree)>,
    stored: BTreeMap<(&'static str, NoteCommitmentSubtreeIndex), ([u8; 32], Height)>,
    from: Height,
    to: Height,
) -> SubtreeVerification {
    let mut outcome = SubtreeVerification::default();
    let mut replayed = BTreeSet::new();

    for (pool, completed) in completions {
        let pool_name = pool_name(pool);
        replayed.insert((pool_name, completed.index));

        match stored.get(&(pool_name, completed.index)) {
            Some((root, end_height)) if stored_subtree_matches(*root, *end_height, &completed) => {
                outcome.matched += 1
            }
            Some(_) => outcome.mismatched.push((completed.index, pool_name)),
            None => outcome.unstored += 1,
        }
    }

    outcome.stored_only = stored
        .into_iter()
        .filter_map(|((pool, index), (_, end_height))| {
            (end_height > from && end_height <= to && !replayed.contains(&(pool, index)))
                .then_some((index, pool))
        })
        .collect();

    outcome
}

fn pool_name(pool: ShieldedPool) -> &'static str {
    match pool {
        ShieldedPool::Sapling => "sapling",
        ShieldedPool::Orchard => "orchard",
        ShieldedPool::Ironwood => "ironwood",
    }
}

fn stored_subtree_matches(
    stored_root: [u8; 32],
    stored_end_height: Height,
    completed: &CompletedSubtree,
) -> bool {
    stored_root == completed.root && stored_end_height == completed.end_height
}

/// Returns the per-pool note commitment roots derived at each of `heights`, hex-encoded in the
/// display order `z_gettreestate` uses.
///
/// Exists so a derived treestate can be compared against another node's `z_gettreestate` output.
/// That is the strongest check available for this design: the other node built its trees the
/// legacy way, block by block, so agreement is independent evidence that replay reconstructs the
/// same history rather than merely being self-consistent.
pub fn derived_roots_in_display_order(
    db: &ZakuraDb,
    cache: &std::sync::Mutex<HistoricalTreeCache>,
    heights: impl IntoIterator<Item = Height>,
    max_replay_blocks: u64,
) -> Result<Vec<(Height, String, String, String)>, (Height, HistoricalTreeDerivationError)> {
    let mut roots = Vec::new();

    for height in heights {
        let derivation = derive_historical_frontiers_measured(db, cache, height, max_replay_blocks)
            .map_err(|error| (height, error))?;

        roots.push((
            height,
            hex::encode(derivation.frontiers.sapling.root().bytes_in_display_order()),
            hex::encode(derivation.frontiers.orchard.root().bytes_in_display_order()),
            hex::encode(
                derivation
                    .frontiers
                    .ironwood
                    .root()
                    .bytes_in_display_order(),
            ),
        ));
    }

    Ok(roots)
}

/// What a height range contains, for fitting the grid's replay cost model.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReplayInputs {
    /// Blocks in the range.
    pub blocks: u64,

    /// Total serialized size of those blocks, in bytes.
    ///
    /// The cost model prices a block at a flat constant plus its note commitments, which misses
    /// the cost of reading and deserialising a large body that carries few or no commitments.
    /// Reporting bytes separately is what lets that be tested rather than assumed.
    pub bytes: u64,

    /// Total Sapling, Orchard and Ironwood note commitments in the range.
    pub commitments: u64,
}

/// Returns what the blocks in `[from, to]` contain, for cost-model fitting.
pub fn replay_inputs(db: &ZakuraDb, from: Height, to: Height) -> ReplayInputs {
    let mut inputs = ReplayInputs::default();

    for height in from.0..=to.0 {
        let height = Height(height);
        inputs.blocks += 1;
        inputs.bytes += db
            .block_info(height.into())
            .map_or(0, |info| u64::from(info.size()));

        if let Some(block) = db.block(height.into()) {
            // Cast is safe: one block holds far fewer commitments than fit in a u64.
            inputs.commitments += (block.sapling_note_commitments().count()
                + block.orchard_note_commitments().count()
                + block.ironwood_note_commitments().count())
                as u64;
        }
    }

    inputs
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inventory_with_tip(finalized_tip: Height) -> VctTreestateInventory {
        VctTreestateInventory {
            finalized_tip: Some(finalized_tip),
            upgrade_height: Some(Height(100)),
            last_checkpoint: Some(Height(200)),
            lowest_retained_height: None,
            root_index_scanned: true,
            block_bodies_scanned: true,
            root_index_gap: None,
            malformed_root_row: None,
            missing_block_body: None,
            missing_anchor: None,
            scan_duration: Duration::ZERO,
        }
    }

    #[test]
    fn absent_band_is_capped_at_finalized_tip() {
        assert_eq!(
            inventory_with_tip(Height(149)).absent_band(),
            Some((Height(100), Height(150)))
        );
        assert_eq!(
            inventory_with_tip(Height(250)).absent_band(),
            Some((Height(100), Height(200)))
        );
        assert_eq!(inventory_with_tip(Height(99)).absent_band(), None);
    }

    #[test]
    fn missing_anchor_prevents_derivation() {
        let mut inventory = inventory_with_tip(Height(149));
        assert_eq!(inventory.can_derive(), Some(true));

        inventory.missing_anchor = Some(Height(99));
        assert_eq!(inventory.can_derive(), Some(false));
    }

    #[test]
    fn malformed_root_row_prevents_derivation() {
        let mut inventory = inventory_with_tip(Height(149));
        inventory.malformed_root_row = Some(Height(125));

        assert_eq!(inventory.can_derive(), Some(false));
    }

    #[test]
    fn deferred_body_scan_keeps_derivation_pending_unless_an_issue_is_known() {
        let mut inventory = inventory_with_tip(Height(149));
        inventory.block_bodies_scanned = false;
        assert_eq!(inventory.can_derive(), None);

        inventory.root_index_gap = Some(Height(125));
        assert_eq!(inventory.can_derive(), Some(false));
    }

    #[test]
    fn stored_subtree_match_requires_root_and_end_height() {
        let completed = CompletedSubtree {
            index: NoteCommitmentSubtreeIndex(3),
            end_height: Height(100),
            root: [7; 32],
        };

        assert!(stored_subtree_matches(
            completed.root,
            completed.end_height,
            &completed
        ));
        assert!(!stored_subtree_matches(
            completed.root,
            Height(101),
            &completed
        ));
        assert!(!stored_subtree_matches(
            [8; 32],
            completed.end_height,
            &completed
        ));
    }

    #[test]
    fn subtree_comparison_detects_stored_only_rows_in_range() {
        let replayed = CompletedSubtree {
            index: NoteCommitmentSubtreeIndex(3),
            end_height: Height(100),
            root: [7; 32],
        };
        let stored = [
            (
                ("sapling", replayed.index),
                (replayed.root, replayed.end_height),
            ),
            (
                ("sapling", NoteCommitmentSubtreeIndex(4)),
                ([8; 32], Height(101)),
            ),
            (
                ("orchard", NoteCommitmentSubtreeIndex(2)),
                ([9; 32], Height(99)),
            ),
        ]
        .into_iter()
        .collect();

        let outcome = compare_subtrees(
            [(ShieldedPool::Sapling, replayed)],
            stored,
            Height(99),
            Height(101),
        );

        assert_eq!(outcome.matched, 1);
        assert_eq!(
            outcome.stored_only,
            [(NoteCommitmentSubtreeIndex(4), "sapling")]
        );
    }
}
