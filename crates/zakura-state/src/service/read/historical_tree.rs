//! Deriving historical note commitment frontiers by replaying retained block bodies.
//!
//! A verified-commitment-trees fast-synced node never writes per-height note commitment trees
//! across the absent band `[U, H)`, so `z_gettreestate` and the `trees` sizes in
//! `getblock`/`getblockheader` have nothing to read there. This module rebuilds a frontier for
//! any height in that band on demand: it starts from the nearest frontier it already has, appends
//! the note commitments of every block in between, and accepts the result only if its root
//! matches the authenticated root already in `commitment_roots_by_height`.
//!
//! # Correctness
//!
//! A note commitment root is a binding commitment to its frontier, so the root check is what
//! makes derivation trustworthy: a frontier that reproduces the authenticated root is the
//! frontier. Nothing here is accepted without that check, which is also why derived frontiers are
//! safe to reuse as anchors for later derivations.
//!
//! Replay needs retained block bodies, so this cannot work on a pruned node.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use thiserror::Error;

use zakura_chain::{
    block::Height, ironwood, orchard, parallel::commitment_aux::BlockCommitmentRoots, sapling,
    transaction::Transaction,
};

use zakura_chain::subtree::NoteCommitmentSubtreeIndex;

use crate::service::finalized_state::{
    serve_block_roots, FrontierArtifact, TransactionLocation, ZakuraDb,
};

/// The most derived frontiers to keep in the per-node cache.
///
/// Wallet access is sequential, so a single entry already collapses a scan's steady-state cost to
/// one batch of replay. The rest of the budget covers concurrent clients scanning different parts
/// of the band. Each entry is a few kilobytes, so this is a negligible amount of memory.
pub const MAX_CACHED_FRONTIERS: usize = 64;

/// Which shielded pool a replay event belongs to.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShieldedPool {
    /// The Sapling pool.
    Sapling,
    /// The Orchard pool.
    Orchard,
    /// The Ironwood pool.
    Ironwood,
}

/// A subtree that finished filling during a replay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompletedSubtree {
    /// The subtree index.
    pub index: NoteCommitmentSubtreeIndex,

    /// The height whose block added the subtree's last leaf.
    pub end_height: Height,

    /// The subtree root.
    pub root: [u8; 32],
}

/// Per-pool note commitment frontiers as of the end of one block.
#[derive(Clone, Debug)]
pub struct DerivedFrontiers {
    /// The Sapling frontier.
    pub sapling: Arc<sapling::tree::NoteCommitmentTree>,

    /// The Orchard frontier.
    pub orchard: Arc<orchard::tree::NoteCommitmentTree>,

    /// The Ironwood frontier.
    pub ironwood: Arc<ironwood::tree::NoteCommitmentTree>,
}

impl DerivedFrontiers {
    /// Returns empty frontiers, the state before any block has been applied.
    pub fn empty() -> Self {
        Self {
            sapling: Arc::<sapling::tree::NoteCommitmentTree>::default(),
            orchard: Arc::<orchard::tree::NoteCommitmentTree>::default(),
            ironwood: Arc::<ironwood::tree::NoteCommitmentTree>::default(),
        }
    }

    /// Returns `true` if every pool's root matches `roots`.
    fn matches(&self, roots: &BlockCommitmentRoots) -> bool {
        self.sapling.root() == roots.sapling_root
            && self.orchard.root() == roots.orchard_root
            && self.ironwood.root() == roots.ironwood_root
    }
}

/// Why a historical frontier could not be derived.
///
/// Every variant is a serving failure, never a consensus one: derivation runs entirely on the read
/// path, and a node that cannot derive simply cannot answer the request.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum HistoricalTreeDerivationError {
    /// A subtree audit endpoint is not above its anchor, so there is no replay range.
    #[error(
        "cannot verify note commitment subtrees in ({from:?}, {to:?}]: the endpoint must be above \
         the anchor"
    )]
    InvalidReplayRange {
        /// The frontier height used as the replay anchor.
        from: Height,
        /// The requested replay endpoint.
        to: Height,
    },

    /// The replay would have to cover more blocks than the configured limit.
    #[error(
        "deriving the note commitment tree at {height:?} needs {blocks} blocks of replay, more \
         than the {limit} block limit"
    )]
    ReplayTooLong {
        /// The requested height.
        height: Height,
        /// How many blocks the replay would cover.
        blocks: u64,
        /// The configured limit.
        limit: u64,
    },

    /// A block body in the replay range is unavailable, so its commitments cannot be read.
    ///
    /// On a pruned node this is expected for every height below the retention window.
    #[error("cannot derive the note commitment tree at {height:?}: block body {missing:?} is not retained")]
    MissingBlockBody {
        /// The requested height.
        height: Height,
        /// The height whose body is missing.
        missing: Height,
    },

    /// A usable frontier to start the replay from is missing, so there is nothing to anchor on.
    #[error(
        "cannot derive the note commitment tree at {height:?}: no usable anchor frontier at {anchor:?}"
    )]
    MissingAnchor {
        /// The requested height.
        height: Height,
        /// The height the anchor was expected at.
        anchor: Height,
    },

    /// There is no authenticated root to check the derived frontier against.
    ///
    /// Without it the result would be unverified, which this module never serves.
    #[error("cannot derive the note commitment tree at {height:?}: no authenticated root is stored for that height")]
    MissingAuthenticatedRoot {
        /// The requested height.
        height: Height,
    },

    /// The derived frontier does not reproduce the authenticated root.
    ///
    /// The replay inputs disagree with the roots the node authenticated against its own header
    /// chain, so the frontier is wrong and must not be served.
    #[error(
        "the note commitment tree derived at {height:?} does not match the authenticated root"
    )]
    RootMismatch {
        /// The requested height.
        height: Height,
    },

    /// Appending a block's note commitments failed.
    #[error("cannot derive the note commitment tree at {height:?}: appending block {block:?} failed: {error}")]
    Append {
        /// The requested height.
        height: Height,
        /// The block being applied.
        block: Height,
        /// The underlying tree error, rendered because it is not `PartialEq`.
        error: String,
    },
}

/// A bounded in-memory cache of frontiers this node has already derived and root-checked.
///
/// Entries double as anchors. A derivation for height `h` starts from the highest verified
/// frontier at or below `h` across this cache and the published grid, so a wallet scanning forward
/// replays only from the end of its previous batch, and a cold request still takes the nearest
/// grid entry rather than a distant cache hit.
#[derive(Debug, Default)]
pub struct HistoricalTreeCache {
    /// Verified frontiers, keyed by the height they are the state at the end of.
    frontiers: BTreeMap<Height, Arc<DerivedFrontiers>>,

    /// A published frontier grid to fall back on when the cache has nothing nearby.
    ///
    /// Entries here are *not* trusted: one is root-checked before it anchors anything, exactly
    /// like a locally derived frontier, which is what lets the grid be coarse and distributed
    /// outside the binary.
    artifact: Option<Arc<FrontierArtifact>>,
}

impl HistoricalTreeCache {
    /// Returns a cache that can also anchor on `artifact`'s published grid.
    pub fn with_artifact(artifact: Arc<FrontierArtifact>) -> Self {
        Self {
            frontiers: BTreeMap::new(),
            artifact: Some(artifact),
        }
    }

    /// The last checkpoint encoded in the published grid, if one is loaded.
    pub(crate) fn last_checkpoint(&self) -> Option<Height> {
        self.artifact
            .as_ref()
            .map(|artifact| artifact.last_checkpoint)
    }

    /// Returns the highest published grid entry at or below `height`, if any.
    fn artifact_anchor_at_or_below(&self, height: Height) -> Option<(Height, DerivedFrontiers)> {
        let entry = self.artifact.as_ref()?.anchor_at_or_below(height)?;

        Some((
            entry.height,
            DerivedFrontiers {
                sapling: entry.sapling.clone(),
                orchard: entry.orchard.clone(),
                ironwood: entry.ironwood.clone(),
            },
        ))
    }

    /// Returns the highest cached frontier at or below `height`, if any.
    fn cached_anchor_at_or_below(&self, height: Height) -> Option<(Height, Arc<DerivedFrontiers>)> {
        self.frontiers
            .range(..=height)
            .next_back()
            .map(|(anchor, frontiers)| (*anchor, frontiers.clone()))
    }

    /// Caches `frontiers` as the verified state at the end of `height`.
    ///
    /// Evicts the lowest height when full. Clients sweep forward, so the lowest entry is the one
    /// least likely to anchor the next request.
    fn insert(&mut self, height: Height, frontiers: Arc<DerivedFrontiers>) {
        self.frontiers.insert(height, frontiers);

        while self.frontiers.len() > MAX_CACHED_FRONTIERS {
            self.frontiers.pop_first();
        }
    }
}

/// Locks the cache, recovering from poisoning.
///
/// The cache holds only root-checked frontiers keyed by height, so a panic elsewhere cannot leave
/// it in a state that would make a later derivation wrong. Refusing to serve because an unrelated
/// request panicked would be strictly worse than reusing the map.
fn lock(cache: &Mutex<HistoricalTreeCache>) -> std::sync::MutexGuard<'_, HistoricalTreeCache> {
    cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Returns whether a published grid entry is strictly closer to the target than a cached one.
///
/// Equal heights prefer the cache: it is already root-checked and already resident. A cache hit at
/// any lower height is not closer; that is the case that used to skip the grid entirely.
fn published_is_nearer(cached: Option<Height>, published: Option<Height>) -> bool {
    published.is_some_and(|published_height| {
        cached.is_none_or(|cached_height| published_height > cached_height)
    })
}

/// Derives the note commitment frontiers as of the end of block `height`, verified against the
/// authenticated root stored for that height.
///
/// Replays block bodies forward from the nearest anchor: the higher of the highest cached
/// frontier and the highest published grid entry at or below `height`, else the last frontier
/// stored below the absent band, else empty frontiers at genesis. The result is stored in
/// `cache` only after it reproduces the authenticated root, so a derivation can never be anchored
/// on an unverified frontier.
///
/// `max_replay_blocks` bounds the work one request can cost. It is a serving limit, not a
/// correctness one.
pub fn derive_historical_frontiers(
    db: &ZakuraDb,
    cache: &Mutex<HistoricalTreeCache>,
    height: Height,
    max_replay_blocks: u64,
) -> Result<Arc<DerivedFrontiers>, HistoricalTreeDerivationError> {
    derive_historical_frontiers_measured(db, cache, height, max_replay_blocks)
        .map(|derivation| derivation.frontiers)
}

/// A completed derivation, and what it cost.
#[derive(Clone, Debug)]
pub struct Derivation {
    /// The frontiers as of the end of the requested height.
    pub frontiers: Arc<DerivedFrontiers>,

    /// How many blocks were replayed to produce them.
    ///
    /// Zero when the requested height was already available as an anchor. Callers measuring cost
    /// must read this rather than infer it from the height, because the anchor may have come from
    /// the cache or from a published grid rather than from genesis.
    pub replayed_blocks: u64,
}

/// Derives the frontiers at `height`, reporting how many blocks the replay covered.
///
/// See [`derive_historical_frontiers`].
pub fn derive_historical_frontiers_measured(
    db: &ZakuraDb,
    cache: &Mutex<HistoricalTreeCache>,
    height: Height,
    max_replay_blocks: u64,
) -> Result<Derivation, HistoricalTreeDerivationError> {
    let anchor = anchor_for(db, cache, height)?;

    if let Some((anchor_height, frontiers)) = &anchor {
        match anchor_height.cmp(&height) {
            std::cmp::Ordering::Equal => {
                // Cached entries have already passed this check, but a database fallback has
                // not. Keep the check here so a zero-replay result follows the same acceptance
                // rule as every replayed result.
                verify_against_index(db, height, frontiers)?;

                lock(cache).insert(height, frontiers.clone());
                return Ok(Derivation {
                    frontiers: frontiers.clone(),
                    replayed_blocks: 0,
                });
            }
            std::cmp::Ordering::Greater => {
                return Err(HistoricalTreeDerivationError::MissingAnchor {
                    height,
                    anchor: *anchor_height,
                });
            }
            std::cmp::Ordering::Less => {}
        }
    }

    // The anchor is the state at the *end* of its height, so replay starts at the next block.
    // With no anchor the replay starts at genesis.
    let replay_from = anchor.as_ref().map_or(0, |(anchor, _)| anchor.0 + 1);
    let blocks = u64::from(
        height
            .0
            .checked_sub(replay_from)
            .expect("anchors above the requested height are rejected"),
    ) + 1;
    if blocks > max_replay_blocks {
        return Err(HistoricalTreeDerivationError::ReplayTooLong {
            height,
            blocks,
            limit: max_replay_blocks,
        });
    }

    let frontiers = anchor.map_or_else(DerivedFrontiers::empty, |(_, frontiers)| {
        (*frontiers).clone()
    });
    let frontiers = replay_with_subtrees(db, height, replay_from, frontiers, |_, _| {})?;

    verify_against_index(db, height, &frontiers)?;

    let frontiers = Arc::new(frontiers);
    lock(cache).insert(height, frontiers.clone());

    Ok(Derivation {
        frontiers,
        replayed_blocks: blocks,
    })
}

/// Returns the frontiers to replay forward from, and the height they are the state at the end of.
///
/// `None` means start from empty frontiers at genesis, which is correct when this binary committed
/// every block from genesis (`U == 0`) and so stored no per-height trees to anchor on.
///
/// Published grid entries at or above the VCT upgrade height are tried nearest-first. Entries
/// below the upgrade cannot be checked against the VCT roots index and are also lower than the
/// stored `U - 1` frontier, so they are never useful anchors. A failed root check skips that cell
/// and tries the next-lower eligible one while it stays nearer than the cache. The grid is the
/// bound on cold replay; ignoring it and restarting at genesis is the expensive path it exists to
/// avoid.
fn anchor_for(
    db: &ZakuraDb,
    cache: &Mutex<HistoricalTreeCache>,
    height: Height,
) -> Result<Option<(Height, Arc<DerivedFrontiers>)>, HistoricalTreeDerivationError> {
    let cached = lock(cache).cached_anchor_at_or_below(height);
    let cached_height = cached.as_ref().map(|(anchor_height, _)| *anchor_height);
    let published_floor = db.vct_upgrade_height().unwrap_or(Height(0));

    let mut skip_at_or_above: Option<Height> = None;
    loop {
        let search_height = match skip_at_or_above {
            None => Some(height),
            Some(Height(0)) => None,
            Some(skipped) => Some(Height(skipped.0 - 1)),
        };
        let Some(search_height) = search_height else {
            break;
        };

        let published = lock(cache).artifact_anchor_at_or_below(search_height);
        let Some((anchor_height, frontiers)) = published else {
            break;
        };

        // The roots index starts at U, and the durable U - 1 frontier is nearer than every
        // published entry below U. Since artifact entries are sorted, all remaining entries are
        // also below the floor.
        if anchor_height < published_floor {
            break;
        }

        if !published_is_nearer(cached_height, Some(anchor_height)) {
            break;
        }

        // The entry is checked against this node's own authenticated root before it anchors
        // anything, so a wrong or hostile artifact cannot steer a derivation.
        //
        // A failed check is deliberately *not* fatal: the artifact is an optimization with no
        // trust weight, so a bad entry is ignored and the derivation tries the next-lower cell.
        // Making it fatal would hand anyone who can supply a corrupt artifact a denial of service
        // over a node that is perfectly capable of answering without one.
        match verify_against_index(db, anchor_height, &frontiers) {
            Ok(()) => {
                let frontiers = Arc::new(frontiers);
                lock(cache).insert(anchor_height, frontiers.clone());

                return Ok(Some((anchor_height, frontiers)));
            }
            Err(error) => {
                tracing::warn!(
                    ?anchor_height,
                    %error,
                    "ignoring a published frontier entry that does not match the authenticated root",
                );
                metrics::counter!("state.historical_tree.artifact_entry_rejected").increment(1);
                skip_at_or_above = Some(anchor_height);
            }
        }
    }

    if cached.is_some() {
        return Ok(cached);
    }

    stored_frontier_before_absent_band(db, height)
        .map(|anchor| anchor.map(|(height, frontiers)| (height, Arc::new(frontiers))))
}

/// Returns the stored frontier immediately before the absent band, if the band starts above
/// genesis.
///
/// `height` identifies the derivation or export that needs the anchor in any returned error.
/// `None` means the absent band starts at genesis, so replay must start from empty frontiers.
pub(crate) fn stored_frontier_before_absent_band(
    db: &ZakuraDb,
    height: Height,
) -> Result<Option<(Height, DerivedFrontiers)>, HistoricalTreeDerivationError> {
    // Below the upgrade height `U` this binary did not run, so per-height trees are present. The
    // tree at `U - 1` is therefore the last stored frontier before the absent band starts.
    let Some(upgrade) = db.vct_upgrade_height().filter(|upgrade| upgrade.0 > 0) else {
        return Ok(None);
    };

    let anchor = Height(upgrade.0 - 1);
    // Rollback can move the tip below the write-once upgrade marker. In that case a backwards tree
    // lookup would return a retained row below the rollback target and mislabel it as `anchor`.
    if db
        .finalized_tip_height()
        .is_none_or(|tip_height| anchor > tip_height)
    {
        return Err(HistoricalTreeDerivationError::MissingAnchor { height, anchor });
    }

    // The tip check above establishes that the chain still reaches `anchor`, so the newest stored
    // row at or below it is its state rather than a pre-rollback leftover.
    let frontiers = stored_frontiers_at(db, anchor)
        .map_err(|_| HistoricalTreeDerivationError::MissingAnchor { height, anchor })?;

    Ok(Some((anchor, frontiers)))
}

/// Returns the per-height trees the database stores at `height`.
///
/// Unchanged trees are deduplicated, so the newest stored row at or below `height` is the state
/// at `height`. That holds only where the database actually stores trees: callers must not use
/// this inside a fast-synced database's absent band, where the newest row below the requested
/// height belongs to a different stretch of the chain entirely. Genesis writes a row for every
/// pool, so a pool with no activity yet still resolves to its empty tree.
pub(crate) fn stored_frontiers_at(
    db: &ZakuraDb,
    height: Height,
) -> Result<DerivedFrontiers, HistoricalTreeDerivationError> {
    let (Some(sapling), Some(orchard), Some(ironwood)) = (
        db.latest_stored_sapling_tree(&height),
        db.latest_stored_orchard_tree(&height),
        db.latest_stored_ironwood_tree(&height),
    ) else {
        return Err(HistoricalTreeDerivationError::MissingAnchor {
            height,
            anchor: height,
        });
    };

    Ok(DerivedFrontiers {
        sapling,
        orchard,
        ironwood,
    })
}

/// Appends the note commitments of blocks `replay_from..=height` to `frontiers`, reporting every
/// subtree that completes along the way.
///
/// `on_subtree` sees each completion in order. A subtree root reported here is pinned by the same
/// root check that validates the replay's endpoints: the replay is deterministic, so an error
/// anywhere in it would have to cancel out exactly to still reproduce the verified end root.
pub fn replay_with_subtrees(
    db: &ZakuraDb,
    height: Height,
    replay_from: u32,
    frontiers: DerivedFrontiers,
    mut on_subtree: impl FnMut(ShieldedPool, CompletedSubtree),
) -> Result<DerivedFrontiers, HistoricalTreeDerivationError> {
    let DerivedFrontiers {
        sapling,
        orchard,
        ironwood,
    } = frontiers;

    let mut sapling = (*sapling).clone();
    let mut orchard = (*orchard).clone();
    let mut ironwood = (*ironwood).clone();

    let first_height = Height(replay_from);
    let mut transactions = db
        .transactions_by_location_range(
            TransactionLocation::min_for_height(first_height)
                ..=TransactionLocation::max_for_height(height),
        )
        .peekable();

    for block_height in replay_from..=height.0 {
        let block_height = Height(block_height);
        let append_error = |error: &dyn std::fmt::Display| HistoricalTreeDerivationError::Append {
            height,
            block: block_height,
            error: error.to_string(),
        };

        let Some((first_location, first_transaction)) = transactions.next() else {
            return Err(HistoricalTreeDerivationError::MissingBlockBody {
                height,
                missing: block_height,
            });
        };
        if first_location != TransactionLocation::min_for_height(block_height) {
            return Err(HistoricalTreeDerivationError::MissingBlockBody {
                height,
                missing: block_height,
            });
        }

        // A block never spans two subtree boundaries: the block size limit caps it far below the
        // 65,536 commitments a subtree holds, so a single batch append is always valid.
        let mut sapling_commitments = Vec::new();
        let mut orchard_commitments = Vec::new();
        let mut ironwood_commitments = Vec::new();
        extend_commitments(
            &first_transaction,
            &mut sapling_commitments,
            &mut orchard_commitments,
            &mut ironwood_commitments,
        );
        while transactions
            .peek()
            .is_some_and(|(location, _)| location.height == block_height)
        {
            let (_, transaction) = transactions
                .next()
                .expect("a transaction observed through peek is available");
            extend_commitments(
                &transaction,
                &mut sapling_commitments,
                &mut orchard_commitments,
                &mut ironwood_commitments,
            );
        }

        if !sapling_commitments.is_empty() {
            let completed = sapling
                .append_batch(&sapling_commitments)
                .map_err(|error| append_error(&error))?;
            if let Some((index, root)) = completed {
                on_subtree(
                    ShieldedPool::Sapling,
                    CompletedSubtree {
                        index,
                        end_height: block_height,
                        root: root.to_bytes(),
                    },
                );
            }
        }

        if !orchard_commitments.is_empty() {
            let completed = orchard
                .append_batch(&orchard_commitments)
                .map_err(|error| append_error(&error))?;
            if let Some((index, root)) = completed {
                on_subtree(
                    ShieldedPool::Orchard,
                    CompletedSubtree {
                        index,
                        end_height: block_height,
                        root: root.to_repr(),
                    },
                );
            }
        }

        if !ironwood_commitments.is_empty() {
            let completed = ironwood
                .append_batch(&ironwood_commitments)
                .map_err(|error| append_error(&error))?;
            if let Some((index, root)) = completed {
                on_subtree(
                    ShieldedPool::Ironwood,
                    CompletedSubtree {
                        index,
                        end_height: block_height,
                        root: root.to_repr(),
                    },
                );
            }
        }
    }

    Ok(DerivedFrontiers {
        sapling: Arc::new(sapling),
        orchard: Arc::new(orchard),
        ironwood: Arc::new(ironwood),
    })
}

fn extend_commitments(
    transaction: &Transaction,
    sapling: &mut Vec<sapling::tree::NoteCommitmentUpdate>,
    orchard: &mut Vec<orchard::tree::NoteCommitmentUpdate>,
    ironwood: &mut Vec<ironwood::tree::NoteCommitmentUpdate>,
) {
    sapling.extend(transaction.sapling_note_commitments().cloned());
    orchard.extend(transaction.orchard_note_commitments().copied());
    ironwood.extend(transaction.ironwood_note_commitments().copied());
}

/// Checks `frontiers` against the authenticated roots stored for `height`.
///
/// This is the check the whole design rests on: a note commitment root is a binding commitment to
/// its frontier, so a frontier that reproduces the authenticated root *is* the frontier, whatever
/// source it came from. Both the serving path and the artifact generator route through here so the
/// two can never drift apart.
pub fn verify_against_index(
    db: &ZakuraDb,
    height: Height,
    frontiers: &DerivedFrontiers,
) -> Result<(), HistoricalTreeDerivationError> {
    let roots = authenticated_roots(db, height)?;

    if frontiers.matches(&roots) {
        Ok(())
    } else {
        Err(HistoricalTreeDerivationError::RootMismatch { height })
    }
}

/// Checks `frontiers` against the best roots the database can produce for `height`.
///
/// Generation-only. It differs from [`verify_against_index`] in one place: below the upgrade
/// marker, where the serving index has no rows because an older binary committed those blocks,
/// it falls back to roots derived from the per-height trees. There the check is a consistency
/// check rather than an independent one, since the roots come from the same trees the entry does.
/// That is acceptable precisely because the artifact carries no trust of its own — the consumer
/// re-checks every entry against roots authenticated by its own header chain before anchoring on
/// it, and a consumer's absent band never overlaps a range where it has trees of its own.
pub(crate) fn verify_against_available_roots(
    db: &ZakuraDb,
    height: Height,
    frontiers: &DerivedFrontiers,
) -> Result<(), HistoricalTreeDerivationError> {
    let roots = serve_block_roots(db, height..=height)
        .into_iter()
        .next()
        .filter(|roots| roots.height == height)
        .ok_or(HistoricalTreeDerivationError::MissingAuthenticatedRoot { height })?;

    if frontiers.matches(&roots) {
        Ok(())
    } else {
        Err(HistoricalTreeDerivationError::RootMismatch { height })
    }
}

/// Returns the authenticated per-pool roots stored for `height`.
fn authenticated_roots(
    db: &ZakuraDb,
    height: Height,
) -> Result<BlockCommitmentRoots, HistoricalTreeDerivationError> {
    db.commitment_roots_by_height_range(height..=height)
        .into_iter()
        .next()
        .filter(|roots| roots.height == height)
        .ok_or(HistoricalTreeDerivationError::MissingAuthenticatedRoot { height })
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use zakura_chain::{
        block::{merkle::AuthDataRoot, Height},
        orchard,
        parameters::Network,
    };

    use crate::{
        config::Config,
        constants::{
            state_database_format_version_in_code, MAX_HISTORICAL_TREE_REPLAY_BLOCKS,
            STATE_DATABASE_KIND,
        },
        service::finalized_state::{
            DiskWriteBatch, FrontierArtifact, FrontierEntry, STATE_COLUMN_FAMILIES_IN_CODE,
        },
    };

    use super::*;

    /// A low cache fill, matching the "request U+1000 first" case.
    const LOW: Height = Height(1_000);
    /// A grid cell between LOW and HIGH.
    const MID: Height = Height(2_000_000);
    /// A later high request, matching the "then ask for 3,000,000" case.
    const HIGH: Height = Height(3_000_000);

    fn ephemeral_db() -> ZakuraDb {
        ZakuraDb::new(
            &Config::ephemeral(),
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &Network::Mainnet,
            true,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            false,
        )
        .expect("opening an ephemeral database should succeed")
    }

    fn mismatched_frontiers() -> DerivedFrontiers {
        let mut orchard = orchard::tree::NoteCommitmentTree::default();
        orchard
            .append(halo2::pasta::pallas::Base::from(1u64))
            .expect("test tree is not full");
        DerivedFrontiers {
            sapling: Arc::new(Default::default()),
            orchard: Arc::new(orchard),
            ironwood: Arc::new(Default::default()),
        }
    }

    fn seed_roots(db: &ZakuraDb, height: Height, frontiers: &DerivedFrontiers) {
        let mut batch = DiskWriteBatch::new();
        batch.insert_commitment_roots_by_height(
            db,
            height,
            &frontiers.sapling.root(),
            &frontiers.orchard.root(),
            &frontiers.ironwood.root(),
            0,
            0,
            0,
            &AuthDataRoot::from([0; 32]),
        );
        db.write_batch(batch)
            .expect("seeding authenticated roots succeeds");
    }

    fn artifact_at(height: Height, frontiers: &DerivedFrontiers) -> Arc<FrontierArtifact> {
        artifact_entries(Height(height.0 + 1), &[(height, frontiers)])
    }

    fn artifact_entries(
        last_checkpoint: Height,
        entries: &[(Height, &DerivedFrontiers)],
    ) -> Arc<FrontierArtifact> {
        Arc::new(FrontierArtifact {
            spacing: 1,
            last_checkpoint,
            entries: entries
                .iter()
                .map(|(height, frontiers)| FrontierEntry {
                    height: *height,
                    sapling: frontiers.sapling.clone(),
                    orchard: frontiers.orchard.clone(),
                    ironwood: frontiers.ironwood.clone(),
                })
                .collect(),
        })
    }

    fn cache_with(
        artifact: Arc<FrontierArtifact>,
        cached_height: Height,
        cached: DerivedFrontiers,
    ) -> Mutex<HistoricalTreeCache> {
        let mut cache = HistoricalTreeCache::with_artifact(artifact);
        cache.insert(cached_height, Arc::new(cached));
        Mutex::new(cache)
    }

    #[test]
    fn published_is_nearer_takes_the_higher_height() {
        assert!(published_is_nearer(None, Some(HIGH)));
        assert!(published_is_nearer(Some(LOW), Some(HIGH)));
        assert!(!published_is_nearer(Some(HIGH), Some(LOW)));
        assert!(!published_is_nearer(Some(HIGH), Some(HIGH)));
        assert!(!published_is_nearer(Some(LOW), None));
        assert!(!published_is_nearer(None, None));
    }

    #[test]
    fn genesis_absent_band_has_no_stored_predecessor() {
        let _init_guard = zakura_test::init();
        let db = ephemeral_db();

        assert!(stored_frontier_before_absent_band(&db, Height(0))
            .expect("an unset upgrade marker starts replay at genesis")
            .is_none());

        let mut batch = DiskWriteBatch::new();
        batch.update_vct_upgrade_marker(&db, Height(0));
        db.write_batch(batch)
            .expect("seeding a genesis upgrade marker succeeds");

        assert!(stored_frontier_before_absent_band(&db, Height(0))
            .expect("a genesis upgrade starts replay at genesis")
            .is_none());
    }

    /// The bug: a cache hit at or below the target used to win unconditionally, so a later high
    /// request replayed from the low fill instead of the nearby grid entry.
    #[test]
    fn nearer_grid_entry_wins_over_a_lower_cache_entry() {
        let _init_guard = zakura_test::init();
        let db = ephemeral_db();
        let frontiers = DerivedFrontiers::empty();
        seed_roots(&db, HIGH, &frontiers);

        let cache = cache_with(artifact_at(HIGH, &frontiers), LOW, frontiers);
        let derivation = derive_historical_frontiers_measured(&db, &cache, HIGH, 0)
            .expect("a grid entry at the target is a zero-replay hit");

        assert_eq!(derivation.replayed_blocks, 0);
    }

    /// Published entries below U have no VCT roots-index rows and are farther away than the
    /// durable U - 1 frontier. They must not be considered even if an unexpected stale index row
    /// would make one pass verification.
    #[test]
    fn grid_entries_below_the_vct_upgrade_height_are_not_anchors() {
        let _init_guard = zakura_test::init();
        let db = ephemeral_db();
        let cached_height = Height(500);
        let published_height = Height(1_500);
        let upgrade = Height(2_000);
        let frontiers = DerivedFrontiers::empty();

        let mut batch = DiskWriteBatch::new();
        batch.update_vct_upgrade_marker(&db, upgrade);
        db.write_batch(batch)
            .expect("seeding the VCT upgrade marker succeeds");

        // This row is deliberately inconsistent with the production layout. It makes accepting
        // the below-U grid entry observable instead of merely producing the same final fallback.
        seed_roots(&db, published_height, &frontiers);

        let cache = cache_with(
            artifact_at(published_height, &frontiers),
            cached_height,
            frontiers,
        );
        let (anchor_height, _) = anchor_for(&db, &cache, upgrade)
            .expect("the cached anchor is available")
            .unwrap();

        assert_eq!(
            anchor_height, cached_height,
            "a published entry below U must not displace an eligible fallback"
        );
    }

    /// Sequential scans still prefer a higher cache entry over a coarser grid point behind it.
    #[test]
    fn nearer_cache_entry_wins_over_a_lower_grid_entry() {
        let _init_guard = zakura_test::init();
        let db = ephemeral_db();
        let frontiers = DerivedFrontiers::empty();
        seed_roots(&db, LOW, &frontiers);
        seed_roots(&db, HIGH, &frontiers);

        let cache = cache_with(artifact_at(LOW, &frontiers), HIGH, frontiers);
        let derivation = derive_historical_frontiers_measured(&db, &cache, HIGH, 0)
            .expect("a cached target is a zero-replay hit");

        assert_eq!(derivation.replayed_blocks, 0);
    }

    /// A rejected nearer grid entry must not discard a usable cache entry. Genesis replay from
    /// height 0 would be one block longer than replay from the low cache entry.
    #[test]
    fn rejected_nearer_grid_entry_falls_back_to_the_cache() {
        let _init_guard = zakura_test::init();
        let db = ephemeral_db();
        let frontiers = DerivedFrontiers::empty();
        seed_roots(&db, HIGH, &frontiers);

        let cache = cache_with(artifact_at(HIGH, &mismatched_frontiers()), LOW, frontiers);
        let error = derive_historical_frontiers_measured(&db, &cache, HIGH, 0)
            .expect_err("replay from the low cache entry exceeds a zero-block bound");

        assert_eq!(
            error,
            HistoricalTreeDerivationError::ReplayTooLong {
                height: HIGH,
                blocks: u64::from(HIGH.0 - LOW.0),
                limit: 0,
            }
        );
    }

    /// A rejected nearer grid entry must try the next-lower cell rather than restart at genesis.
    #[test]
    fn rejected_nearer_grid_entry_falls_back_to_the_previous_grid_entry() {
        let _init_guard = zakura_test::init();
        let db = ephemeral_db();
        let frontiers = DerivedFrontiers::empty();
        let mismatched = mismatched_frontiers();
        seed_roots(&db, MID, &frontiers);
        seed_roots(&db, HIGH, &frontiers);

        let cache = Mutex::new(HistoricalTreeCache::with_artifact(artifact_entries(
            Height(HIGH.0 + 1),
            &[(MID, &frontiers), (HIGH, &mismatched)],
        )));
        let error = derive_historical_frontiers_measured(&db, &cache, HIGH, 0)
            .expect_err("replay from the previous grid entry exceeds a zero-block bound");

        assert_eq!(
            error,
            HistoricalTreeDerivationError::ReplayTooLong {
                height: HIGH,
                blocks: u64::from(HIGH.0 - MID.0),
                limit: 0,
            }
        );
    }

    /// A grid whose every entry fails its root check falls through to a genesis replay, and the
    /// serving bound must refuse that rather than replaying the whole absent band.
    ///
    /// This is what ties the load-time grid-gap check to the request path: a stale or wrong-chain
    /// grid can pass the gap check at startup and still verify against nothing here, so the same
    /// constant has to hold on both sides.
    #[test]
    fn a_wholly_unverifiable_grid_cannot_replay_past_the_serving_bound() {
        let _init_guard = zakura_test::init();
        let db = ephemeral_db();
        let mismatched = mismatched_frontiers();
        seed_roots(&db, HIGH, &DerivedFrontiers::empty());

        // Gaps of one block, so this grid passes the load-time check, yet no entry anchors.
        let cache = Mutex::new(HistoricalTreeCache::with_artifact(artifact_entries(
            Height(HIGH.0 + 1),
            &[(MID, &mismatched), (HIGH, &mismatched)],
        )));
        let error = derive_historical_frontiers_measured(
            &db,
            &cache,
            HIGH,
            MAX_HISTORICAL_TREE_REPLAY_BLOCKS,
        )
        .expect_err("a genesis fall-through must not be served");

        assert_eq!(
            error,
            HistoricalTreeDerivationError::ReplayTooLong {
                height: HIGH,
                blocks: u64::from(HIGH.0) + 1,
                limit: MAX_HISTORICAL_TREE_REPLAY_BLOCKS,
            },
            "the serving bound is what stops a fall-through to genesis"
        );
    }
}
