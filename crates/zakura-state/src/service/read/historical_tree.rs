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

use crate::service::finalized_state::{TransactionLocation, ZakuraDb};

/// The most derived frontiers to keep memoized per node.
///
/// Wallet access is sequential, so a single entry already collapses a scan's steady-state cost to
/// one batch of replay. The rest of the budget covers concurrent clients scanning different parts
/// of the band. Each entry is a few kilobytes, so this is a negligible amount of memory.
pub const MAX_MEMOIZED_FRONTIERS: usize = 64;

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

/// A bounded memo of frontiers this node has already derived and root-checked.
///
/// Entries double as anchors: a derivation for height `h` starts from the highest memoized height
/// at or below `h`, so a wallet scanning forward replays only from the end of its previous batch.
#[derive(Debug, Default)]
pub struct HistoricalTreeCache {
    /// Verified frontiers, keyed by the height they are the state at the end of.
    frontiers: BTreeMap<Height, Arc<DerivedFrontiers>>,
}

impl HistoricalTreeCache {
    /// Returns the highest memoized frontier at or below `height`, if any.
    fn anchor_at_or_below(&self, height: Height) -> Option<(Height, Arc<DerivedFrontiers>)> {
        self.frontiers
            .range(..=height)
            .next_back()
            .map(|(anchor, frontiers)| (*anchor, frontiers.clone()))
    }

    /// Memoizes `frontiers` as the verified state at the end of `height`.
    ///
    /// Evicts the lowest height when full. Clients sweep forward, so the lowest entry is the one
    /// least likely to anchor the next request.
    fn insert(&mut self, height: Height, frontiers: Arc<DerivedFrontiers>) {
        self.frontiers.insert(height, frontiers);

        while self.frontiers.len() > MAX_MEMOIZED_FRONTIERS {
            self.frontiers.pop_first();
        }
    }
}

/// Locks the memo, recovering from poisoning.
///
/// The cache holds only root-checked frontiers keyed by height, so a panic elsewhere cannot leave
/// it in a state that would make a later derivation wrong. Refusing to serve because an unrelated
/// request panicked would be strictly worse than reusing the map.
fn lock(cache: &Mutex<HistoricalTreeCache>) -> std::sync::MutexGuard<'_, HistoricalTreeCache> {
    cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Derives the note commitment frontiers as of the end of block `height`, verified against the
/// authenticated root stored for that height.
///
/// Replays block bodies forward from the nearest anchor: the highest memoized frontier at or below
/// `height`, else the last frontier stored below the absent band, else empty frontiers at genesis.
/// The result is memoized in `cache` only after it reproduces the authenticated root, so a
/// derivation can never be anchored on an unverified frontier.
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
    /// the memo or from a published grid rather than from genesis.
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
                // Memoized entries have already passed this check, but a database fallback has
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
    let frontiers = replay(db, height, replay_from, frontiers)?;

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
fn anchor_for(
    db: &ZakuraDb,
    cache: &Mutex<HistoricalTreeCache>,
    height: Height,
) -> Result<Option<(Height, Arc<DerivedFrontiers>)>, HistoricalTreeDerivationError> {
    let memoized = lock(cache).anchor_at_or_below(height);

    if memoized.is_some() {
        return Ok(memoized);
    }

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

    // These reads intentionally search backwards because unchanged trees are deduplicated. The tip
    // check above establishes that the chain still reaches `anchor`, so such a row is its state.
    let (Some(sapling), Some(orchard), Some(ironwood)) = (
        db.latest_stored_sapling_tree(&anchor),
        db.latest_stored_orchard_tree(&anchor),
        db.latest_stored_ironwood_tree(&anchor),
    ) else {
        return Err(HistoricalTreeDerivationError::MissingAnchor { height, anchor });
    };

    Ok(Some((
        anchor,
        Arc::new(DerivedFrontiers {
            sapling,
            orchard,
            ironwood,
        }),
    )))
}

/// Appends the note commitments of blocks `replay_from..=height` to `frontiers`.
fn replay(
    db: &ZakuraDb,
    height: Height,
    replay_from: u32,
    frontiers: DerivedFrontiers,
) -> Result<DerivedFrontiers, HistoricalTreeDerivationError> {
    replay_with_subtrees(db, height, replay_from, frontiers, |_, _| {})
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
