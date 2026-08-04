//! Read-only audit of a database's historical-treestate serving inputs.
//!
//! A verified-commitment-trees fast-synced node cannot read per-height note commitment trees across
//! its absent band `[U, H)`, and instead rebuilds them by replaying block bodies forward and
//! checking the result against the authenticated roots in `commitment_roots_by_height` (see
//! [`crate::service::read::historical_tree`]). That rests on two inputs actually being present:
//! a gap-free root index across the band, and retained block bodies. This module reports whether
//! they are, and measures what the replay costs.
//!
//! Everything here is read-only, runs off the consensus path, and is safe against a quiesced
//! database snapshot.

use std::time::{Duration, Instant};

use zakura_chain::{block::Height, subtree::NoteCommitmentSubtreeIndex};

use crate::service::{
    finalized_state::ZakuraDb,
    read::{
        historical_tree::{
            derive_historical_frontiers_measured, replay_with_subtrees,
            HistoricalTreeDerivationError, ShieldedPool,
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

    /// The checkpoint handoff height `H`, the exclusive upper bound of the absent band.
    ///
    /// `None` on a normally-synced database, which has per-height trees everywhere and needs
    /// nothing from this design.
    pub handoff_height: Option<Height>,

    /// The lowest height whose raw transactions are retained, if this database is pruned.
    ///
    /// Replay reads block bodies, so a pruned database cannot derive below this height.
    pub lowest_retained_height: Option<Height>,

    /// Whether the full-band scans ran.
    ///
    /// When they did not, [`Self::root_index_gap`] and [`Self::missing_block_body`] are `None`
    /// because nothing looked, not because nothing is missing.
    pub scanned: bool,

    /// The first height in the absent band with no `commitment_roots_by_height` row.
    ///
    /// `None` means the index is gap-free across the band, which is what derivation needs: every
    /// derived frontier is checked against the row at its own height. Only meaningful when
    /// [`Self::scanned`].
    pub root_index_gap: Option<Height>,

    /// The first height in the absent band whose block body is not retained.
    ///
    /// `None` means the whole band is replayable. Only meaningful when [`Self::scanned`].
    pub missing_block_body: Option<Height>,

    /// How long the two scans took.
    pub scan_duration: Duration,
}

impl VctTreestateInventory {
    /// Returns the absent band `[U, H)`, or `None` if this database has no band.
    pub fn absent_band(&self) -> Option<(Height, Height)> {
        let handoff = self.handoff_height?;
        let upgrade = self.upgrade_height.unwrap_or(Height(0));

        (upgrade < handoff).then_some((upgrade, handoff))
    }

    /// Returns whether this database has everything derivation needs across its absent band, or
    /// `None` if the scans that would answer that were skipped.
    pub fn can_derive(&self) -> Option<bool> {
        self.scanned.then(|| {
            self.absent_band().is_some()
                && self.root_index_gap.is_none()
                && self.missing_block_body.is_none()
        })
    }
}

/// Inspects `db` for the inputs historical-treestate derivation depends on.
///
/// With `scan_band`, visits every height in the absent band to check the root index and block-body
/// retention, which is proportional to the band's height range rather than constant time. Without
/// it, only the cheap markers are read.
pub fn inventory(db: &ZakuraDb, scan_band: bool) -> VctTreestateInventory {
    let start = Instant::now();

    let upgrade_height = db.vct_upgrade_height();
    let handoff_height = db.vct_synced_below();

    let mut inventory = VctTreestateInventory {
        finalized_tip: db.finalized_tip_height(),
        upgrade_height,
        handoff_height,
        lowest_retained_height: db.lowest_retained_height(),
        scanned: scan_band,
        root_index_gap: None,
        missing_block_body: None,
        scan_duration: Duration::ZERO,
    };

    if let (true, Some((band_start, band_end))) = (scan_band, inventory.absent_band()) {
        // The band is half-open, so the last height it covers is `H - 1`.
        let last = Height(band_end.0 - 1);
        inventory.root_index_gap = db.first_commitment_root_gap(band_start..=last);
        inventory.missing_block_body = db.first_missing_block_body(band_start, last);
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
) -> Result<Vec<DerivationSample>, (Height, HistoricalTreeDerivationError)> {
    let mut samples = Vec::new();

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
        samples.push(sample);
    }

    Ok(samples)
}

/// The outcome of checking replay-derived subtree roots against the ones the database stored.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SubtreeVerification {
    /// Subtrees that completed during the replay and matched the stored root.
    pub matched: usize,

    /// Subtrees that completed during the replay but whose stored root differs.
    ///
    /// Any entry here falsifies the claim that replay reproduces subtree roots, which is what the
    /// generated subtree artifact rests on.
    pub mismatched: Vec<(NoteCommitmentSubtreeIndex, &'static str)>,

    /// Subtrees that completed during the replay with no stored row to compare against.
    pub unstored: usize,
}

/// Replays `(from, to]` and checks every subtree that completes against the stored subtree rows.
///
/// This exists because subtree roots produced by replay are otherwise unvalidated: they are
/// interior nodes, so the per-height root check does not test them directly. Above a fast-synced
/// node's handoff the database *does* store subtree rows, which makes that band the one place the
/// two can be compared. `from` must be a height whose per-height trees are present, so the replay
/// starts from a known-good frontier rather than reconstructing one.
pub fn verify_subtrees_against_stored(
    db: &ZakuraDb,
    from: Height,
    to: Height,
) -> Result<SubtreeVerification, HistoricalTreeDerivationError> {
    let (Some(sapling), Some(orchard), Some(ironwood)) = (
        db.sapling_tree_by_height(&from),
        db.orchard_tree_by_height(&from),
        db.ironwood_tree_by_height(&from),
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
    replay_with_subtrees(db, to, from.0 + 1, anchor, |pool, completed| {
        completions.push((pool, completed))
    })?;

    let mut outcome = SubtreeVerification::default();

    for (pool, completed) in completions {
        let stored = match pool {
            ShieldedPool::Sapling => db
                .sapling_subtree_list_by_index_range(completed.index..=completed.index)
                .get(&completed.index)
                .map(|data| data.root.to_bytes()),
            ShieldedPool::Orchard => db
                .orchard_subtree_list_by_index_range(completed.index..=completed.index)
                .get(&completed.index)
                .map(|data| data.root.to_repr()),
            ShieldedPool::Ironwood => db
                .ironwood_subtree_list_by_index_range(completed.index..=completed.index)
                .get(&completed.index)
                .map(|data| data.root.to_repr()),
        };

        let pool_name = match pool {
            ShieldedPool::Sapling => "sapling",
            ShieldedPool::Orchard => "orchard",
            ShieldedPool::Ironwood => "ironwood",
        };

        match stored {
            Some(root) if root == completed.root => outcome.matched += 1,
            Some(_) => outcome.mismatched.push((completed.index, pool_name)),
            None => outcome.unstored += 1,
        }
    }

    Ok(outcome)
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
            inputs.commitments += (block.sapling_note_commitments().count()
                + block.orchard_note_commitments().count()
                + block.ironwood_note_commitments().count())
                as u64;
        }
    }

    inputs
}
