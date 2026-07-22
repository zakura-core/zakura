//! Payload types and the producer/serving half of the verified-commitment-trees
//! (`docs/design/verified-commitment-trees.md`).

#[cfg(test)]
use std::collections::HashMap;
use std::{
    fmt,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use thiserror::Error;
use zakura_chain::{block, ironwood, orchard, sapling, sprout};

#[cfg(test)]
use zakura_chain::block::merkle::AuthDataRoot;

use super::{FromDisk, IntoDisk, ZakuraDb};

/// Per-block verified commitment roots
pub(super) use zakura_chain::parallel::commitment_aux::BlockCommitmentRoots;

/// The verified final note-commitment frontiers at the last checkpoint height.
///
/// Verified-commitment-tree (VCT) mode skips the per-block frontier recompute below the checkpoint, so the
/// running Sapling/Orchard frontiers are never advanced. To let post-checkpoint
/// semantic verification resume, the real frontiers at the checkpoint are supplied
/// here, verified (`frontier.root() == the verified root at the checkpoint`), and
/// written as the tip treestate at the last checkpoint. Subtree tips are not carried: the
/// resuming chain recomputes them from the frontier position.
#[derive(Clone, Debug)]
pub(super) struct FinalFrontiers {
    pub(super) height: block::Height,
    pub(super) sapling: Arc<sapling::tree::NoteCommitmentTree>,
    pub(super) orchard: Arc<orchard::tree::NoteCommitmentTree>,
    pub(super) sprout: Arc<sprout::tree::NoteCommitmentTree>,
    /// The Ironwood frontier at the handoff height. Absent from the on-disk byte format
    /// written before Ironwood existed; [`Self::from_bytes`] defaults it to the empty
    /// tree when parsing such older bytes.
    pub(super) ironwood: Arc<ironwood::tree::NoteCommitmentTree>,
}

/// Errors producing [`FinalFrontiers`] from a finalized database.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum FinalFrontiersGenerationError {
    /// The database has no finalized tip identity to bind the generated frontier.
    #[error("cannot produce final frontiers at height {height:?}: finalized database is empty")]
    MissingFinalizedTip {
        /// The requested final frontier height.
        height: block::Height,
    },

    /// The finalized tip has no persisted Sprout tree.
    #[error("missing Sprout final frontier tree at height {height:?}")]
    MissingSproutTree {
        /// The finalized tip height.
        height: block::Height,
    },

    /// The database has no Sapling tree at the requested height.
    #[error("missing Sapling final frontier tree at height {height:?}")]
    MissingSaplingTree {
        /// The requested final frontier height.
        height: block::Height,
    },

    /// The database has no Orchard tree at the requested height.
    #[error("missing Orchard final frontier tree at height {height:?}")]
    MissingOrchardTree {
        /// The requested final frontier height.
        height: block::Height,
    },

    /// The database has no Ironwood tree at the requested height.
    #[error("missing Ironwood final frontier tree at height {height:?}")]
    MissingIronwoodTree {
        /// The requested final frontier height.
        height: block::Height,
    },

    /// The requested height is not the finalized tip, so its Sprout frontier is unavailable.
    #[error(
        "cannot produce final frontiers at height {height:?}: Sprout is only stored at finalized tip {tip:?}"
    )]
    RequestedHeightIsNotTip {
        /// The requested final frontier height.
        height: block::Height,
        /// The finalized database tip height.
        tip: block::Height,
    },

    /// The finalized tip changed while the four frontiers were being read.
    #[error(
        "finalized tip changed while producing final frontiers: before {before:?}, after {after:?}; retry generation"
    )]
    FinalizedTipChanged {
        /// The full tip identity captured before reading any frontier.
        before: Option<(block::Height, block::Hash)>,
        /// The full tip identity captured after reading all frontiers.
        after: Option<(block::Height, block::Hash)>,
    },

    /// A block above the requested height appended Sprout note commitments, so the stored tip
    /// Sprout frontier is not the Sprout frontier at the requested height.
    #[error(
        "cannot produce final frontiers at height {height:?}: Sprout note commitments were \
         appended at {last_change:?}, below finalized tip {tip:?}; retry once the requested \
         height passes that block"
    )]
    SproutChangedAboveRequestedHeight {
        /// The highest block above `height` that appended Sprout note commitments.
        last_change: block::Height,
        /// The requested final frontier height.
        height: block::Height,
        /// The finalized database tip height.
        tip: block::Height,
    },

    /// A block body needed to prove the Sprout frontier is settled was not retained.
    #[error(
        "cannot produce final frontiers at height {height:?}: block {missing:?} below finalized \
         tip {tip:?} is not retained, so the settled-Sprout scan cannot run"
    )]
    MissingBlockInSproutWindow {
        /// The unretained block height.
        missing: block::Height,
        /// The requested final frontier height.
        height: block::Height,
        /// The finalized database tip height.
        tip: block::Height,
    },
}

/// Errors parsing [`FinalFrontiers`] from the embedded/frontier-file byte format.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum FinalFrontiersParseError {
    /// The input ended before the 4-byte height field.
    MissingHeight {
        /// The total number of bytes in the input.
        actual_len: usize,
    },
    /// The input ended before a tree blob's 4-byte length prefix.
    MissingLength {
        /// The tree whose length prefix was being read.
        tree: &'static str,
        /// Byte offset where the length prefix starts.
        offset: usize,
        /// Bytes remaining from `offset`.
        remaining: usize,
    },
    /// A tree blob's length prefix points past the end of the input.
    TruncatedBlob {
        /// The tree whose blob was being read.
        tree: &'static str,
        /// Byte offset where the blob starts.
        offset: usize,
        /// Blob length from the prefix.
        expected_len: usize,
        /// Bytes remaining from `offset`.
        remaining: usize,
    },
    /// A tree blob's length prefix overflows `usize` arithmetic.
    LengthOverflow {
        /// The tree whose blob was being read.
        tree: &'static str,
        /// Byte offset where the blob starts.
        offset: usize,
        /// Blob length from the prefix.
        len: usize,
    },
    /// The parser consumed all expected fields, but extra bytes remained.
    TrailingBytes {
        /// Byte offset where the trailing data starts.
        offset: usize,
        /// Number of trailing bytes.
        trailing_len: usize,
    },
}

impl fmt::Display for FinalFrontiersParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FinalFrontiersParseError::MissingHeight { actual_len } => write!(
                f,
                "missing final frontier height: expected 4 bytes, got {actual_len}"
            ),
            FinalFrontiersParseError::MissingLength {
                tree,
                offset,
                remaining,
            } => write!(
                f,
                "missing {tree} frontier length prefix at byte {offset}: expected 4 bytes, got {remaining}"
            ),
            FinalFrontiersParseError::TruncatedBlob {
                tree,
                offset,
                expected_len,
                remaining,
            } => write!(
                f,
                "truncated {tree} frontier blob at byte {offset}: length prefix says {expected_len} bytes, but only {remaining} remain"
            ),
            FinalFrontiersParseError::LengthOverflow { tree, offset, len } => write!(
                f,
                "{tree} frontier blob length overflows at byte {offset}: {len} bytes"
            ),
            FinalFrontiersParseError::TrailingBytes {
                offset,
                trailing_len,
            } => write!(
                f,
                "unexpected trailing final frontier bytes at byte {offset}: {trailing_len} bytes"
            ),
        }
    }
}

impl std::error::Error for FinalFrontiersParseError {}

impl FinalFrontiers {
    /// Serialize to the embedded byte format: height (u32 LE), then sapling, orchard,
    /// sprout, and ironwood trees, each as `u32`-LE-length-prefixed `IntoDisk` bytes.
    /// Used to create embedded or test final-frontier fixtures.
    pub(super) fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&self.height.0.to_le_bytes());
        let blobs: [Vec<u8>; 4] = [
            IntoDisk::as_bytes(&*self.sapling),
            IntoDisk::as_bytes(&*self.orchard),
            IntoDisk::as_bytes(&*self.sprout),
            IntoDisk::as_bytes(&*self.ironwood),
        ];
        for blob in blobs {
            let len = u32::try_from(blob.len()).expect("note commitment tree fits in u32 bytes");
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(&blob);
        }
        out
    }

    /// Parse the embedded byte format written by [`Self::to_bytes`].
    pub(super) fn from_bytes(bytes: &[u8]) -> Result<Self, FinalFrontiersParseError> {
        let height_bytes = bytes
            .get(0..4)
            .ok_or(FinalFrontiersParseError::MissingHeight {
                actual_len: bytes.len(),
            })?;
        let height_bytes: [u8; 4] =
            height_bytes
                .try_into()
                .map_err(|_| FinalFrontiersParseError::MissingHeight {
                    actual_len: bytes.len(),
                })?;
        let height = block::Height(u32::from_le_bytes(height_bytes));

        // Read a `u32`-length-prefixed blob starting at `cursor`, returning the blob and
        // the cursor position just past it. A plain fn (not a closure) so the cursor
        // threads through by value, and callers can freely inspect it between calls.
        fn read_blob(
            bytes: &[u8],
            cursor: usize,
            tree: &'static str,
        ) -> Result<(Vec<u8>, usize), FinalFrontiersParseError> {
            let len_end =
                cursor
                    .checked_add(4)
                    .ok_or(FinalFrontiersParseError::LengthOverflow {
                        tree,
                        offset: cursor,
                        len: 4,
                    })?;
            let len_bytes =
                bytes
                    .get(cursor..len_end)
                    .ok_or(FinalFrontiersParseError::MissingLength {
                        tree,
                        offset: cursor,
                        remaining: bytes.len().saturating_sub(cursor),
                    })?;
            let len_bytes: [u8; 4] =
                len_bytes
                    .try_into()
                    .map_err(|_| FinalFrontiersParseError::MissingLength {
                        tree,
                        offset: cursor,
                        remaining: bytes.len().saturating_sub(cursor),
                    })?;
            // Zebra's supported platforms have at least 32-bit `usize`, so every
            // u32 length prefix fits in memory indexes.
            let len = u32::from_le_bytes(len_bytes) as usize;
            let cursor = len_end;
            let blob_end =
                cursor
                    .checked_add(len)
                    .ok_or(FinalFrontiersParseError::LengthOverflow {
                        tree,
                        offset: cursor,
                        len,
                    })?;
            let blob =
                bytes
                    .get(cursor..blob_end)
                    .ok_or(FinalFrontiersParseError::TruncatedBlob {
                        tree,
                        offset: cursor,
                        expected_len: len,
                        remaining: bytes.len().saturating_sub(cursor),
                    })?;
            Ok((blob.to_vec(), blob_end))
        }

        // Read three `u32`-length-prefixed blobs starting after the height.
        let (sapling, cursor) = read_blob(bytes, 4, "sapling")?;
        let (orchard, cursor) = read_blob(bytes, cursor, "orchard")?;
        let (sprout, cursor) = read_blob(bytes, cursor, "sprout")?;

        // The Ironwood blob was added after the original 3-blob format shipped (and after
        // the embedded `vct/mainnet-frontier.bin` was generated). Older bytes end right
        // after sprout; treat that as "no Ironwood frontier yet" rather than an error, so
        // the existing embedded file keeps parsing unmodified. Newer bytes carry a 4th
        // blob, parsed and trailing-byte-checked exactly like the other three.
        let (ironwood, cursor) = if cursor == bytes.len() {
            (None, cursor)
        } else {
            let (blob, cursor) = read_blob(bytes, cursor, "ironwood")?;
            (Some(blob), cursor)
        };

        if cursor != bytes.len() {
            return Err(FinalFrontiersParseError::TrailingBytes {
                offset: cursor,
                trailing_len: bytes.len() - cursor,
            });
        }

        Ok(FinalFrontiers {
            height,
            sapling: Arc::new(<sapling::tree::NoteCommitmentTree as FromDisk>::from_bytes(
                sapling,
            )),
            orchard: Arc::new(<orchard::tree::NoteCommitmentTree as FromDisk>::from_bytes(
                orchard,
            )),
            sprout: Arc::new(<sprout::tree::NoteCommitmentTree as FromDisk>::from_bytes(
                sprout,
            )),
            ironwood: Arc::new(match ironwood {
                Some(ironwood) => {
                    <ironwood::tree::NoteCommitmentTree as FromDisk>::from_bytes(ironwood)
                }
                None => ironwood::tree::NoteCommitmentTree::default(),
            }),
        })
    }
}

/// Source for the VCT fast-sync's verified per-block roots and final frontier.
pub(super) trait CommitmentRootSource: std::fmt::Debug + Send + Sync {
    /// The supplied roots for `height`, if this source has them.
    fn vct_root(
        &self,
        height: block::Height,
    ) -> Option<(
        sapling::tree::Root,
        orchard::tree::Root,
        ironwood::tree::Root,
    )>;

    /// The checkpoint handoff height: the boundary below which the vct path skips
    /// per-height trees, from the source's final frontier.
    fn vct_last_checkpoint_height(&self) -> block::Height {
        self.final_frontiers().height
    }

    /// The verified final frontiers at the handoff height.
    ///
    /// Every source carries one: the fast path only runs on networks with an embedded
    /// handoff frontier, and test fixtures construct one explicitly.
    fn final_frontiers(&self) -> &FinalFrontiers;

    /// Discard the supplied root for `height` so a later [`vct_root`](Self::vct_root)
    /// returns `None` for it.
    ///
    /// Called by the committer when a supplied root fails verification: dropping the bad
    /// root un-poisons the store so a re-fetch from a different peer can replace it, rather
    /// than the committer re-reading the same rejected root forever. The default is a no-op
    /// for test-only local sources; the peer source overrides it.
    fn invalidate(&self, _height: block::Height) {}
}

/// Test-only local source over a height-keyed roots map.
#[cfg(test)]
#[derive(Debug)]
pub(super) struct FixtureSource {
    roots: HashMap<
        u32,
        (
            sapling::tree::Root,
            orchard::tree::Root,
            ironwood::tree::Root,
        ),
    >,
    frontiers: FinalFrontiers,
}

#[cfg(test)]
impl FixtureSource {
    pub(super) fn new(
        roots: HashMap<
            u32,
            (
                sapling::tree::Root,
                orchard::tree::Root,
                ironwood::tree::Root,
            ),
        >,
        frontiers: FinalFrontiers,
    ) -> Self {
        FixtureSource { roots, frontiers }
    }
}

#[cfg(test)]
impl CommitmentRootSource for FixtureSource {
    fn vct_root(
        &self,
        height: block::Height,
    ) -> Option<(
        sapling::tree::Root,
        orchard::tree::Root,
        ironwood::tree::Root,
    )> {
        self.roots.get(&height.0).copied()
    }
    fn final_frontiers(&self) -> &FinalFrontiers {
        &self.frontiers
    }
}

/// A [`CommitmentRootSource`] backed by provisional header-ahead roots in `db`.
///
/// Header sync persists peer-supplied roots into `db` ahead of body commit
/// ([`ZakuraDb::insert_supplied_commitment_roots`]); the committer reads them per
/// height through the [`CommitmentRootSource`] seam, and tests fill roots through the
/// same database write path. The handoff frontier is embedded in the binary, held
/// immutably here and never fetched over the network — a peer source always has one,
/// because peer mode is only selected on networks with an embedded frontier. Committed
/// rows are cleaned up by the database's own retention, not through this seam.
#[derive(Debug)]
pub(super) struct PeerSource {
    db: ZakuraDb,
    frontiers: FinalFrontiers,
}

impl PeerSource {
    /// Create a source backed by provisional header-ahead roots in `db`. `frontiers`
    /// is the embedded handoff frontier for the network.
    pub(super) fn new(db: ZakuraDb, frontiers: FinalFrontiers) -> Self {
        PeerSource { db, frontiers }
    }
}

impl CommitmentRootSource for PeerSource {
    fn vct_root(
        &self,
        height: block::Height,
    ) -> Option<(
        sapling::tree::Root,
        orchard::tree::Root,
        ironwood::tree::Root,
    )> {
        self.db
            .supplied_commitment_roots_by_height_range(height..=height)
            .into_iter()
            .next()
            .map(|roots| (roots.sapling_root, roots.orchard_root, roots.ironwood_root))
    }
    fn final_frontiers(&self) -> &FinalFrontiers {
        &self.frontiers
    }
    fn invalidate(&self, height: block::Height) {
        // Drop the rejected root so the next read misses; header sync can then deliver a
        // verifiable replacement for this height from another peer.
        if let Err(error) = self.db.delete_supplied_commitment_roots([height]) {
            tracing::debug!(?error, ?height, "failed to delete rejected VCT root");
        }
    }
}

/// `tree`'s root, or the empty-tree root when `tree` is `None`.
///
/// `db.ironwood_tree_by_height(height)` only returns `None` for a height above the
/// finalized tip, or within a verified-commitment-trees fast-synced database's `[U, H)`
/// absent band (see [`ZakuraDb::vct_tree_absent`](super::ZakuraDb::vct_tree_absent)); it is
/// not how a pre-Ironwood-upgrade empty root arises — that comes from the tree's own
/// content, since every database has an Ironwood row from genesis onward (written at
/// genesis commit, or backfilled by the `add_ironwood_tree` upgrade), and that row is
/// genuinely the empty tree below the upgrade height. In [`produce_block_roots`] below,
/// the `None` branch is in practice unreachable: the Sapling/Orchard lookups immediately
/// above already `break` on the same absent-height conditions before this is called.
fn ironwood_root_or_empty(
    tree: Option<Arc<zakura_chain::ironwood::tree::NoteCommitmentTree>>,
) -> ironwood::tree::Root {
    tree.map(|tree| tree.root())
        .unwrap_or_else(|| ironwood::tree::NoteCommitmentTree::default().root())
}

/// Produce the per-block roots payload for `range` from `db`'s per-height trees.
///
/// Derives each root from the per-height note commitment tree.
pub(crate) fn produce_block_roots(
    db: &ZakuraDb,
    range: std::ops::RangeInclusive<block::Height>,
) -> Vec<BlockCommitmentRoots> {
    let (start, end) = (range.start().0, range.end().0);
    let mut roots = Vec::new();
    for h in start..=end {
        let height = block::Height(h);
        let (Some(sapling), Some(orchard)) = (
            db.sapling_tree_by_height(&height),
            db.orchard_tree_by_height(&height),
        ) else {
            break;
        };
        // Never `None` here in practice: the Sapling/Orchard lookups above already broke
        // out of the loop for any height this would return `None` for (see
        // `ironwood_root_or_empty`'s doc comment).
        let ironwood_root = ironwood_root_or_empty(db.ironwood_tree_by_height(&height));
        // Below the upgrade height the serving index does not exist, so derive the
        // auth-data root and shielded transaction counts from the stored block body.
        // Pruned nodes can retain these trees after deleting the body. Stop at the first
        // missing body so callers never mistake fabricated zero metadata for a complete
        // payload.
        let Some(block) = db.block(height.into()) else {
            metrics::counter!("state.block_roots.missing_body").increment(1);
            static MISSING_BODIES: AtomicU64 = AtomicU64::new(0);
            let occurrences = MISSING_BODIES.fetch_add(1, Ordering::Relaxed) + 1;
            if occurrences.is_power_of_two() {
                tracing::error!(
                    ?height,
                    occurrences,
                    "stopping tree-aux root serving because the finalized block body is unavailable"
                );
            }
            break;
        };
        let (sapling_tx, orchard_tx, ironwood_tx, auth_data_root) = (
            block.sapling_transactions_count(),
            block.orchard_transactions_count(),
            block.ironwood_transactions_count(),
            block.auth_data_root(),
        );
        roots.push(BlockCommitmentRoots {
            height,
            sapling_root: sapling.root(),
            orchard_root: orchard.root(),
            ironwood_root,
            sapling_tx,
            orchard_tx,
            ironwood_tx,
            auth_data_root,
        });
    }
    roots
}

/// Serve the per-block roots for `range`, joining tree-derived roots below the VCT upgrade height
/// with indexed roots at and above it.
///
/// Each source stops at the first missing height, so the result is always a contiguous prefix from
/// `range.start()`. Databases without a recorded upgrade height derive the whole range from trees.
pub(crate) fn serve_block_roots(
    db: &ZakuraDb,
    range: std::ops::RangeInclusive<block::Height>,
) -> Vec<BlockCommitmentRoots> {
    // Below the VCT upgrade height, we use the per-height trees to derive the roots.
    let Some(upgrade) = db.vct_upgrade_height() else {
        return produce_block_roots(db, range);
    };

    let (start, end) = (*range.start(), *range.end());

    // Wholly at/above `U`: the VCT-specific index covers it. (`U == 0` for a node that fast-synced from
    // genesis takes this path for every request, never touching the absent per-height trees.)
    if start >= upgrade {
        return db.commitment_roots_by_height_range(range);
    }

    // Below `U`: derive the per-height-tree run up to `U - 1` (`start < upgrade` so `upgrade >= 1`).
    let trees_end = block::Height(end.0.min(upgrade.0 - 1));
    let mut roots = produce_block_roots(db, start..=trees_end);

    // Continue into the index only if the tree run is contiguous up to `U - 1`; a short run means a
    // gap below `U`, so serve it alone and let the client retry the remainder.
    if roots.last().map(|root| root.height) == Some(trees_end) && end >= upgrade {
        roots.extend(db.commitment_roots_by_height_range(upgrade..=end));
    }

    roots
}

/// Produce the final frontiers at the finalized database tip `height`.
///
/// Sprout stores only its current tip frontier, so producing a frontier at any other height would
/// combine the requested Sapling/Orchard/Ironwood trees with a later Sprout tree.
pub(super) fn produce_final_frontiers(
    db: &ZakuraDb,
    height: block::Height,
) -> Result<FinalFrontiers, FinalFrontiersGenerationError> {
    produce_final_frontiers_with_post_read(db, height, || {})
}

fn produce_final_frontiers_with_post_read(
    db: &ZakuraDb,
    height: block::Height,
    post_read: impl FnOnce(),
) -> Result<FinalFrontiers, FinalFrontiersGenerationError> {
    let before = db
        .tip()
        .ok_or(FinalFrontiersGenerationError::MissingFinalizedTip { height })?;
    if before.0 != height {
        let tip = before.0;
        return Err(FinalFrontiersGenerationError::RequestedHeightIsNotTip { height, tip });
    }

    read_final_frontiers_at(db, height, before, post_read)
}

/// Produce the final frontiers at `height`, which may be below the finalized tip.
///
/// Sprout stores only its current tip frontier, so this proves the tip frontier is also the
/// frontier at `height` before pairing them: it scans every retained block body in
/// `(height, tip]` and fails closed if any block appended Sprout note commitments
/// ([`FinalFrontiersGenerationError::SproutChangedAboveRequestedHeight`]) or is not retained
/// ([`FinalFrontiersGenerationError::MissingBlockInSproutWindow`]). See
/// [`produce_final_frontiers`] for the tip-only variant that needs no scan.
pub(super) fn produce_settled_final_frontiers(
    db: &ZakuraDb,
    height: block::Height,
) -> Result<FinalFrontiers, FinalFrontiersGenerationError> {
    let before = db
        .tip()
        .ok_or(FinalFrontiersGenerationError::MissingFinalizedTip { height })?;
    let tip = before.0;

    // Scan the whole window rather than stopping at the first change, so the error
    // names the height the next export's grid has to pass.
    let mut last_change = None;
    for raw_height in height.0.saturating_add(1)..=tip.0 {
        let scan_height = block::Height(raw_height);
        let block = db.block(scan_height.into()).ok_or(
            FinalFrontiersGenerationError::MissingBlockInSproutWindow {
                missing: scan_height,
                height,
                tip,
            },
        )?;
        if block.sprout_note_commitments().next().is_some() {
            last_change = Some(scan_height);
        }
    }
    if let Some(last_change) = last_change {
        return Err(
            FinalFrontiersGenerationError::SproutChangedAboveRequestedHeight {
                last_change,
                height,
                tip,
            },
        );
    }

    read_final_frontiers_at(db, height, before, || {})
}

/// Read the four frontiers for `height` and re-check that the tip identity `before` did not
/// change during the reads. The caller has already established that pairing `height`'s trees
/// with the tip Sprout frontier is sound.
fn read_final_frontiers_at(
    db: &ZakuraDb,
    height: block::Height,
    before: (block::Height, block::Hash),
    post_read: impl FnOnce(),
) -> Result<FinalFrontiers, FinalFrontiersGenerationError> {
    let sapling = db
        .sapling_tree_by_height(&height)
        .ok_or(FinalFrontiersGenerationError::MissingSaplingTree { height })?;
    let orchard = db
        .orchard_tree_by_height(&height)
        .ok_or(FinalFrontiersGenerationError::MissingOrchardTree { height })?;
    let ironwood = db
        .ironwood_tree_by_height(&height)
        .ok_or(FinalFrontiersGenerationError::MissingIronwoodTree { height })?;
    let sprout = db
        .sprout_tree_for_tip()
        .map_err(|error| FinalFrontiersGenerationError::MissingSproutTree { height: error.tip })?;

    post_read();
    let after = db.tip();
    if after != Some(before) {
        return Err(FinalFrontiersGenerationError::FinalizedTipChanged {
            before: Some(before),
            after,
        });
    }

    Ok(FinalFrontiers {
        height,
        sapling,
        orchard,
        sprout,
        ironwood,
    })
}

/// Produce serialized final-frontier bytes for the checkpoint handoff at `height`.
///
/// These bytes use the same format as the embedded `mainnet-frontier.bin` file consumed by
/// the VCT frontier loader (landing with the committer fast path in a follow-up increment).
pub fn produce_final_frontiers_bytes(
    db: &ZakuraDb,
    height: block::Height,
) -> Result<Vec<u8>, FinalFrontiersGenerationError> {
    Ok(produce_final_frontiers(db, height)?.to_bytes())
}

/// Produce serialized final-frontier bytes for the checkpoint handoff at `height`, which may
/// be below the finalized tip.
///
/// Same byte format as [`produce_final_frontiers_bytes`]; the settled-Sprout scan contract is
/// documented on [`produce_settled_final_frontiers`].
pub fn produce_settled_final_frontiers_bytes(
    db: &ZakuraDb,
    height: block::Height,
) -> Result<Vec<u8>, FinalFrontiersGenerationError> {
    Ok(produce_settled_final_frontiers(db, height)?.to_bytes())
}

#[cfg(test)]
mod tests {
    use zakura_chain::{ironwood, parameters::Network, serialization::ZcashDeserializeInto};

    use crate::{
        constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
        request::{CheckpointVerifiedBlock, FinalizedBlock, Treestate},
        service::finalized_state::{
            disk_db::WriteDisk, DiskWriteBatch, STATE_COLUMN_FAMILIES_IN_CODE,
        },
        Config,
    };

    use super::*;

    fn ephemeral_mainnet_db() -> ZakuraDb {
        let network = Network::Mainnet;
        ZakuraDb::new(
            &Config::ephemeral(),
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &network,
            true,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            false,
        )
        .expect("opening the finalized state database should succeed")
    }

    fn sapling_note_commitment(value: u64) -> sapling::tree::NoteCommitmentUpdate {
        let mut bytes = [0; 32];
        bytes[..8].copy_from_slice(&value.to_le_bytes());

        Option::<sapling::tree::NoteCommitmentUpdate>::from(
            sapling::tree::NoteCommitmentUpdate::from_bytes(&bytes),
        )
        .expect("small little-endian integers are canonical Jubjub field elements")
    }

    fn sapling_tree(value: u64) -> sapling::tree::NoteCommitmentTree {
        let mut tree = sapling::tree::NoteCommitmentTree::default();
        tree.append(sapling_note_commitment(value))
            .expect("single-note Sapling tree is not full");
        tree
    }

    fn orchard_tree(value: u64) -> orchard::tree::NoteCommitmentTree {
        let mut tree = orchard::tree::NoteCommitmentTree::default();
        tree.append(halo2::pasta::pallas::Base::from(value))
            .expect("single-note Orchard tree is not full");
        tree
    }

    fn seed_trees(db: &ZakuraDb, heights: impl IntoIterator<Item = u32>) {
        let mut batch = DiskWriteBatch::new();
        for height in heights {
            let height = block::Height(height);
            batch.create_sapling_tree(db, &height, &sapling_tree(u64::from(height.0)));
            batch.create_orchard_tree(db, &height, &orchard_tree(u64::from(height.0)));
            // A real database always has an Ironwood tree row from genesis onward (written
            // at genesis commit, or backfilled by the `add_ironwood_tree` upgrade), so mirror
            // that invariant here with the empty pre-Nu6_3 tree.
            batch.create_ironwood_tree(db, &height, &ironwood::tree::NoteCommitmentTree::default());
        }
        db.write_batch(batch).expect("seeding trees succeeds");
    }

    fn seed_block_bodies(db: &ZakuraDb, heights: impl IntoIterator<Item = u32>) {
        let blocks = Network::Mainnet.blockchain_map();
        for height in heights {
            let block: Arc<block::Block> = blocks
                .get(&height)
                .expect("block height has test data")
                .zcash_deserialize_into()
                .expect("test data deserializes");
            let finalized = FinalizedBlock::from_checkpoint_verified(
                CheckpointVerifiedBlock::from(block),
                Treestate::default(),
            );
            let mut batch = DiskWriteBatch::new();
            batch
                .prepare_block_header_and_transaction_data_batch(db, &finalized, true, None)
                .expect("test block header and transactions are valid");
            db.write_batch(batch).expect("seeding block body succeeds");
        }
    }

    fn seed_sprout_tree(db: &ZakuraDb, tree: &sprout::tree::NoteCommitmentTree) {
        let mut batch = DiskWriteBatch::new();
        batch.update_sprout_tree(db, tree);
        db.write_batch(batch).expect("seeding Sprout tree succeeds");
    }

    fn sprout_tree_with_note(value: u8) -> sprout::tree::NoteCommitmentTree {
        let mut tree = sprout::tree::NoteCommitmentTree::default();
        tree.append(sprout::commitment::NoteCommitment::from([value; 32]))
            .expect("single-note Sprout tree is not full");
        tree
    }

    fn seed_finalized_tip(db: &ZakuraDb, height: block::Height) {
        let hash_by_height = db.db().cf_handle("hash_by_height").unwrap();
        let height_byte =
            u8::try_from(height.0).expect("test heights fit in a byte for hash fixtures");
        let mut batch = DiskWriteBatch::new();
        batch.zs_insert(&hash_by_height, height, block::Hash([height_byte; 32]));
        db.write_batch(batch)
            .expect("seeding finalized tip succeeds");
    }

    fn seed_index_roots(db: &ZakuraDb, heights: impl IntoIterator<Item = u32>) {
        let mut batch = DiskWriteBatch::new();
        for height in heights {
            let height_byte =
                u8::try_from(height).expect("test heights fit in a byte for auth root fixtures");
            batch.insert_commitment_roots_by_height(
                db,
                block::Height(height),
                &sapling_tree(u64::from(height) + 100).root(),
                &orchard_tree(u64::from(height) + 100).root(),
                &ironwood::tree::NoteCommitmentTree::default().root(),
                u64::from(height),
                u64::from(height) + 1,
                u64::from(height) + 2,
                &AuthDataRoot::from([height_byte; 32]),
            );
        }
        db.write_batch(batch)
            .expect("seeding commitment root index succeeds");
    }

    #[test]
    fn commitment_root_range_stops_at_first_gap() {
        let _init_guard = zakura_test::init();
        let db = ephemeral_mainnet_db();

        seed_index_roots(&db, [10, 12]);

        assert_eq!(
            db.commitment_roots_by_height_range(block::Height(10)..=block::Height(12))
                .into_iter()
                .map(|roots| roots.height)
                .collect::<Vec<_>>(),
            vec![block::Height(10)],
        );
        assert!(db
            .commitment_roots_by_height_range(block::Height(11)..=block::Height(12))
            .is_empty());
    }

    fn set_upgrade_height(db: &ZakuraDb, height: block::Height) {
        let mut batch = DiskWriteBatch::new();
        batch.update_vct_upgrade_marker(db, height);
        db.write_batch(batch)
            .expect("seeding VCT upgrade marker succeeds");
    }

    fn expected_tree_roots(height: u32) -> BlockCommitmentRoots {
        let block: Arc<block::Block> = Network::Mainnet
            .blockchain_map()
            .get(&height)
            .expect("block height has test data")
            .zcash_deserialize_into()
            .expect("test data deserializes");
        BlockCommitmentRoots {
            height: block::Height(height),
            sapling_root: sapling_tree(u64::from(height)).root(),
            orchard_root: orchard_tree(u64::from(height)).root(),
            ironwood_root: ironwood::tree::NoteCommitmentTree::default().root(),
            sapling_tx: block.sapling_transactions_count(),
            orchard_tx: block.orchard_transactions_count(),
            ironwood_tx: block.ironwood_transactions_count(),
            auth_data_root: block.auth_data_root(),
        }
    }

    fn expected_index_roots(height: u32) -> BlockCommitmentRoots {
        let height_byte =
            u8::try_from(height).expect("test heights fit in a byte for auth root fixtures");
        BlockCommitmentRoots {
            height: block::Height(height),
            sapling_root: sapling_tree(u64::from(height) + 100).root(),
            orchard_root: orchard_tree(u64::from(height) + 100).root(),
            ironwood_root: ironwood::tree::NoteCommitmentTree::default().root(),
            sapling_tx: u64::from(height),
            orchard_tx: u64::from(height) + 1,
            ironwood_tx: u64::from(height) + 2,
            auth_data_root: AuthDataRoot::from([height_byte; 32]),
        }
    }

    #[test]
    fn produce_block_roots_derives_contiguous_tree_roots() {
        let _init_guard = zakura_test::init();
        let db = ephemeral_mainnet_db();
        seed_block_bodies(&db, [1, 2]);
        seed_trees(&db, [1, 2]);
        seed_finalized_tip(&db, block::Height(2));

        let roots = produce_block_roots(&db, block::Height(1)..=block::Height(4));

        assert_eq!(
            roots,
            vec![expected_tree_roots(1), expected_tree_roots(2)],
            "tree-derived roots stop at the first missing height"
        );
    }

    fn non_empty_ironwood_tree(value: u64) -> ironwood::tree::NoteCommitmentTree {
        let mut tree = ironwood::tree::NoteCommitmentTree::default();
        tree.append(halo2::pasta::pallas::Base::from(value))
            .expect("single-note Ironwood tree is not full");
        tree
    }

    /// [`ironwood_root_or_empty`] returns the tree's own root when given `Some` tree
    /// (the caller's `db.ironwood_tree_by_height(height)` lookup), and the empty-tree
    /// root when given `None` (a height above the finalized tip, or within a fast-synced
    /// database's absent band — not "no row for this height", which does not occur in
    /// practice; see the function's doc comment).
    #[test]
    fn ironwood_root_or_empty_reads_tree_or_falls_back_to_empty() {
        let empty_root = ironwood::tree::NoteCommitmentTree::default().root();

        assert_eq!(
            ironwood_root_or_empty(None),
            empty_root,
            "a missing per-height Ironwood tree falls back to the empty-tree root"
        );

        let non_empty_tree = Arc::new(non_empty_ironwood_tree(1));
        let non_empty_root = non_empty_tree.root();
        assert_ne!(
            non_empty_root, empty_root,
            "test needs a root distinct from the empty-tree root"
        );
        assert_eq!(
            ironwood_root_or_empty(Some(non_empty_tree)),
            non_empty_root,
            "a present per-height Ironwood tree contributes its own root, not the empty one"
        );
    }

    /// `produce_block_roots` reads the real per-height Ironwood tree from the database
    /// when one is present, rather than always defaulting to the empty-tree root.
    #[test]
    fn produce_block_roots_reads_real_ironwood_tree_when_present() {
        let _init_guard = zakura_test::init();
        let db = ephemeral_mainnet_db();
        seed_block_bodies(&db, [1]);
        seed_trees(&db, [1]);
        let non_empty_ironwood = non_empty_ironwood_tree(7);
        let non_empty_root = non_empty_ironwood.root();
        let mut batch = DiskWriteBatch::new();
        batch.create_ironwood_tree(&db, &block::Height(1), &non_empty_ironwood);
        db.write_batch(batch)
            .expect("overwriting the seeded Ironwood tree succeeds");
        seed_finalized_tip(&db, block::Height(1));

        let roots = produce_block_roots(&db, block::Height(1)..=block::Height(1));

        assert_eq!(
            roots,
            vec![BlockCommitmentRoots {
                ironwood_root: non_empty_root,
                ..expected_tree_roots(1)
            }],
            "produce_block_roots surfaces the real per-height Ironwood root, not the empty one"
        );
    }

    #[test]
    fn serve_block_roots_without_upgrade_marker_uses_tree_fallback() {
        let _init_guard = zakura_test::init();
        let db = ephemeral_mainnet_db();
        seed_block_bodies(&db, [1, 2]);
        seed_trees(&db, [1, 2]);
        seed_index_roots(&db, [1, 2]);
        seed_finalized_tip(&db, block::Height(2));

        let roots = serve_block_roots(&db, block::Height(1)..=block::Height(2));

        assert_eq!(
            roots,
            vec![expected_tree_roots(1), expected_tree_roots(2)],
            "pre-index archive databases derive roots from per-height trees"
        );
    }

    #[test]
    fn serve_block_roots_stitches_trees_to_index_at_upgrade() {
        let _init_guard = zakura_test::init();
        let db = ephemeral_mainnet_db();
        seed_block_bodies(&db, [1, 2]);
        seed_trees(&db, [1, 2]);
        seed_index_roots(&db, [3, 4]);
        seed_finalized_tip(&db, block::Height(4));
        set_upgrade_height(&db, block::Height(3));

        let roots = serve_block_roots(&db, block::Height(1)..=block::Height(4));

        assert_eq!(
            roots,
            vec![
                expected_tree_roots(1),
                expected_tree_roots(2),
                expected_index_roots(3),
                expected_index_roots(4),
            ],
            "ranges crossing U are served as one contiguous tree/index run"
        );
    }

    #[test]
    fn serve_block_roots_does_not_cross_short_tree_prefix_below_upgrade() {
        let _init_guard = zakura_test::init();
        let db = ephemeral_mainnet_db();
        seed_block_bodies(&db, [1]);
        seed_trees(&db, [1]);
        seed_index_roots(&db, [3, 4]);
        seed_finalized_tip(&db, block::Height(1));
        set_upgrade_height(&db, block::Height(3));

        let roots = serve_block_roots(&db, block::Height(1)..=block::Height(4));

        assert_eq!(
            roots,
            vec![expected_tree_roots(1)],
            "a short tree-derived prefix below U is not extended with index rows"
        );
    }

    #[test]
    fn serve_block_roots_at_or_above_upgrade_uses_index() {
        let _init_guard = zakura_test::init();
        let db = ephemeral_mainnet_db();
        seed_trees(&db, [3, 4]);
        seed_index_roots(&db, [3, 4]);
        seed_finalized_tip(&db, block::Height(4));
        set_upgrade_height(&db, block::Height(3));

        let roots = serve_block_roots(&db, block::Height(3)..=block::Height(4));

        assert_eq!(
            roots,
            vec![expected_index_roots(3), expected_index_roots(4)],
            "requests at or above U are served from the compact index"
        );
    }

    #[test]
    fn produce_final_frontiers_reads_tip_trees_and_sprout() {
        let _init_guard = zakura_test::init();
        let db = ephemeral_mainnet_db();
        let height = block::Height(2);
        let sprout = sprout::tree::NoteCommitmentTree::default();
        seed_trees(&db, [2]);
        seed_sprout_tree(&db, &sprout);
        seed_finalized_tip(&db, height);

        let frontiers =
            produce_final_frontiers(&db, height).expect("seeded frontiers should be produced");

        assert_eq!(frontiers.height, height);
        assert_eq!(frontiers.sapling.root(), sapling_tree(2).root());
        assert_eq!(frontiers.orchard.root(), orchard_tree(2).root());
        assert_eq!(frontiers.sprout.root(), sprout.root());
        assert_eq!(
            frontiers.ironwood.root(),
            ironwood::tree::NoteCommitmentTree::default().root(),
            "the seeded fixture's pre-Nu6_3 Ironwood tree is empty"
        );
    }

    #[test]
    fn produce_final_frontiers_rejects_height_below_tip_after_sprout_change() {
        let _init_guard = zakura_test::init();
        let db = ephemeral_mainnet_db();
        let height = block::Height(2);
        let tip = block::Height(3);
        let later_sprout = sprout_tree_with_note(1);

        seed_trees(&db, [2, 3]);
        seed_sprout_tree(&db, &later_sprout);
        seed_finalized_tip(&db, tip);

        assert_ne!(
            later_sprout.root(),
            sprout::tree::NoteCommitmentTree::default().root(),
            "the later block must change the Sprout frontier"
        );
        assert_eq!(
            produce_final_frontiers(&db, height).expect_err(
                "a historical request must not mix height 2 trees with the tip Sprout tree"
            ),
            FinalFrontiersGenerationError::RequestedHeightIsNotTip { height, tip },
            "a historical request must not combine height 2 trees with the changed tip Sprout tree"
        );
    }

    #[test]
    fn produce_final_frontiers_retries_when_tip_identity_changes_during_read() {
        let _init_guard = zakura_test::init();
        let db = ephemeral_mainnet_db();
        let height = block::Height(2);
        let advanced_height = block::Height(3);

        seed_trees(&db, [2, 3]);
        seed_sprout_tree(&db, &sprout::tree::NoteCommitmentTree::default());
        seed_finalized_tip(&db, height);

        assert_eq!(
            produce_final_frontiers_with_post_read(&db, height, || {
                seed_finalized_tip(&db, advanced_height);
            })
            .expect_err("tip advancement during frontier reads must request a retry"),
            FinalFrontiersGenerationError::FinalizedTipChanged {
                before: Some((height, block::Hash([2; 32]))),
                after: Some((advanced_height, block::Hash([3; 32]))),
            }
        );
    }

    #[test]
    fn produce_final_frontiers_reports_missing_trees() {
        let _init_guard = zakura_test::init();
        let db = ephemeral_mainnet_db();
        let height = block::Height(2);

        assert_eq!(
            produce_final_frontiers(&db, height).expect_err("empty database is reported first"),
            FinalFrontiersGenerationError::MissingFinalizedTip { height },
        );
    }

    // `MissingIronwoodTree` (unlike `MissingSaplingTree`/`MissingOrchardTree`, which are
    // soft `Option`s) is defense-in-depth: `ZakuraDb::ironwood_tree_by_height` panics
    // rather than returning `None` for a height at or below the finalized tip with no
    // Ironwood row, because every migrated database has one from genesis onward (written
    // at commit or backfilled by the `add_ironwood_tree` upgrade). So this error variant
    // is not reachable through the normal commit path in this test suite; it exists so
    // `produce_final_frontiers`'s Ironwood read is symmetric with Sapling/Orchard's, not
    // to be independently exercised here.

    #[test]
    fn produce_final_frontiers_bytes_serializes_generated_frontiers() {
        let _init_guard = zakura_test::init();
        let db = ephemeral_mainnet_db();
        let height = block::Height(2);
        seed_trees(&db, [2]);
        seed_sprout_tree(&db, &sprout::tree::NoteCommitmentTree::default());
        seed_finalized_tip(&db, height);

        let frontiers =
            produce_final_frontiers(&db, height).expect("seeded frontiers should be produced");
        let bytes = produce_final_frontiers_bytes(&db, height)
            .expect("seeded frontiers should serialize to bytes");

        assert_eq!(
            bytes,
            frontiers.to_bytes(),
            "public byte producer serializes the generated final frontiers"
        );
    }

    #[test]
    fn produce_settled_final_frontiers_at_tip_matches_tip_variant() {
        let _init_guard = zakura_test::init();
        let db = ephemeral_mainnet_db();
        let height = block::Height(2);
        seed_trees(&db, [2]);
        seed_sprout_tree(&db, &sprout::tree::NoteCommitmentTree::default());
        seed_finalized_tip(&db, height);

        let tip_variant =
            produce_final_frontiers(&db, height).expect("seeded frontiers should be produced");
        let settled = produce_settled_final_frontiers(&db, height)
            .expect("an empty settled-Sprout window needs no block bodies");

        assert_eq!(settled.height, tip_variant.height);
        assert_eq!(settled.sapling.root(), tip_variant.sapling.root());
        assert_eq!(settled.orchard.root(), tip_variant.orchard.root());
        assert_eq!(settled.sprout.root(), tip_variant.sprout.root());
        assert_eq!(settled.ironwood.root(), tip_variant.ironwood.root());
    }

    #[test]
    fn produce_settled_final_frontiers_requires_retained_window_bodies() {
        let _init_guard = zakura_test::init();
        let db = ephemeral_mainnet_db();
        let height = block::Height(2);
        let tip = block::Height(3);
        seed_trees(&db, [2, 3]);
        seed_sprout_tree(&db, &sprout::tree::NoteCommitmentTree::default());
        seed_finalized_tip(&db, tip);

        assert_eq!(
            produce_settled_final_frontiers(&db, height)
                .expect_err("an unretained window body must fail the settled-Sprout scan closed"),
            FinalFrontiersGenerationError::MissingBlockInSproutWindow {
                missing: tip,
                height,
                tip,
            },
        );
    }

    /// Pin the settled-Sprout scan's change detector against real Mainnet blocks:
    /// block 396 contains the first Mainnet JoinSplit, whose dummy output notes
    /// still append Sprout commitments; block 395 precedes it.
    #[test]
    fn sprout_change_detector_matches_known_mainnet_blocks() {
        let clean: zakura_chain::block::Block = zakura_test::vectors::BLOCK_MAINNET_395_BYTES
            .zcash_deserialize_into()
            .expect("test vector block 395 deserializes");
        let changed: zakura_chain::block::Block = zakura_test::vectors::BLOCK_MAINNET_396_BYTES
            .zcash_deserialize_into()
            .expect("test vector block 396 deserializes");

        assert!(
            clean.sprout_note_commitments().next().is_none(),
            "block 395 appends no Sprout note commitments"
        );
        assert!(
            changed.sprout_note_commitments().next().is_some(),
            "block 396 appends the first Mainnet Sprout note commitments"
        );
    }

    /// The final-frontier serialization round-trips: parsed frontiers carry the same
    /// height and tree roots as the originals.
    #[test]
    fn final_frontiers_bytes_round_trips() {
        let mut ironwood = ironwood::tree::NoteCommitmentTree::default();
        ironwood
            .append(halo2::pasta::pallas::Base::from(9u64))
            .expect("single-note Ironwood tree is not full");
        let frontiers = FinalFrontiers {
            height: block::Height(1_687_200),
            sapling: Arc::new(Default::default()),
            orchard: Arc::new(Default::default()),
            sprout: Arc::new(Default::default()),
            ironwood: Arc::new(ironwood),
        };

        let parsed =
            FinalFrontiers::from_bytes(&frontiers.to_bytes()).expect("frontiers should parse");

        assert_eq!(parsed.height, frontiers.height, "height round-trips");
        assert_eq!(
            parsed.sapling.root(),
            frontiers.sapling.root(),
            "sapling frontier round-trips"
        );
        assert_eq!(
            parsed.orchard.root(),
            frontiers.orchard.root(),
            "orchard frontier round-trips"
        );
        assert_eq!(
            parsed.sprout.root(),
            frontiers.sprout.root(),
            "sprout frontier round-trips"
        );
        assert_eq!(
            parsed.ironwood.root(),
            frontiers.ironwood.root(),
            "ironwood frontier round-trips"
        );
        assert_ne!(
            parsed.ironwood.root(),
            ironwood::tree::NoteCommitmentTree::default().root(),
            "test needs a non-empty ironwood root to prove it round-trips, not just defaults"
        );
    }

    /// Bytes written before the Ironwood blob existed (3 blobs: sapling, orchard, sprout)
    /// still parse, with `ironwood` defaulting to the empty tree — this is what lets the
    /// existing embedded `vct/mainnet-frontier.bin` keep loading unmodified.
    #[test]
    fn final_frontiers_bytes_parses_pre_ironwood_format_with_empty_default() {
        let frontiers = FinalFrontiers {
            height: block::Height(1_687_200),
            sapling: Arc::new(Default::default()),
            orchard: Arc::new(Default::default()),
            sprout: Arc::new(Default::default()),
            ironwood: Arc::new(Default::default()),
        };

        // Build the pre-Ironwood 3-blob format by hand: the 4-blob writer always emits an
        // (here empty) Ironwood blob, so strip it back off to simulate an older file.
        let four_blob_bytes = frontiers.to_bytes();
        let ironwood_blob_len = 4 + IntoDisk::as_bytes(&*frontiers.ironwood).len() as u32 as usize;
        let three_blob_bytes =
            four_blob_bytes[..four_blob_bytes.len() - ironwood_blob_len].to_vec();

        let parsed = FinalFrontiers::from_bytes(&three_blob_bytes)
            .expect("pre-Ironwood 3-blob bytes should still parse");

        assert_eq!(parsed.height, frontiers.height, "height round-trips");
        assert_eq!(
            parsed.ironwood.root(),
            ironwood::tree::NoteCommitmentTree::default().root(),
            "a pre-Ironwood frontier defaults to the empty Ironwood tree"
        );
    }

    #[test]
    fn final_frontiers_bytes_reject_malformed_payloads() {
        assert_eq!(
            FinalFrontiers::from_bytes(&[0, 0, 0]).expect_err("short height is rejected"),
            FinalFrontiersParseError::MissingHeight { actual_len: 3 }
        );

        assert_eq!(
            FinalFrontiers::from_bytes(&block::Height(1).0.to_le_bytes())
                .expect_err("missing first length prefix is rejected"),
            FinalFrontiersParseError::MissingLength {
                tree: "sapling",
                offset: 4,
                remaining: 0,
            }
        );

        let mut truncated = block::Height(1).0.to_le_bytes().to_vec();
        truncated.extend_from_slice(&1u32.to_le_bytes());
        assert_eq!(
            FinalFrontiers::from_bytes(&truncated).expect_err("truncated first blob is rejected"),
            FinalFrontiersParseError::TruncatedBlob {
                tree: "sapling",
                offset: 8,
                expected_len: 1,
                remaining: 0,
            }
        );

        let frontiers = FinalFrontiers {
            height: block::Height(1_687_200),
            sapling: Arc::new(Default::default()),
            orchard: Arc::new(Default::default()),
            sprout: Arc::new(Default::default()),
            ironwood: Arc::new(Default::default()),
        };
        let mut trailing = frontiers.to_bytes();
        trailing.push(0);

        assert_eq!(
            FinalFrontiers::from_bytes(&trailing).expect_err("trailing bytes are rejected"),
            FinalFrontiersParseError::TrailingBytes {
                offset: frontiers.to_bytes().len(),
                trailing_len: 1,
            }
        );
    }

    /// The test fixture source looks up produced roots by height and exposes
    /// the handoff frontier — the consumer view of producer output.
    #[test]
    fn fixture_source_round_trips_payload() {
        let roots = vec![
            BlockCommitmentRoots {
                height: block::Height(10),
                sapling_root: sapling::tree::NoteCommitmentTree::default().root(),
                orchard_root: orchard::tree::NoteCommitmentTree::default().root(),
                ironwood_root: zakura_chain::ironwood::tree::NoteCommitmentTree::default().root(),
                sapling_tx: 0,
                orchard_tx: 0,
                ironwood_tx: 0,
                auth_data_root: AuthDataRoot::from([0u8; 32]),
            },
            BlockCommitmentRoots {
                height: block::Height(11),
                sapling_root: sapling::tree::NoteCommitmentTree::default().root(),
                orchard_root: orchard::tree::NoteCommitmentTree::default().root(),
                ironwood_root: zakura_chain::ironwood::tree::NoteCommitmentTree::default().root(),
                sapling_tx: 0,
                orchard_tx: 0,
                ironwood_tx: 0,
                auth_data_root: AuthDataRoot::from([0u8; 32]),
            },
        ];
        let roots = roots
            .into_iter()
            .map(|root| {
                (
                    root.height.0,
                    (root.sapling_root, root.orchard_root, root.ironwood_root),
                )
            })
            .collect();
        let frontiers = FinalFrontiers {
            height: block::Height(11),
            sapling: Arc::new(Default::default()),
            orchard: Arc::new(Default::default()),
            sprout: Arc::new(Default::default()),
            ironwood: Arc::new(Default::default()),
        };

        let source = FixtureSource::new(roots, frontiers);

        assert!(
            source.vct_root(block::Height(10)).is_some(),
            "produced root is looked up by height"
        );
        assert!(
            source.vct_root(block::Height(99)).is_none(),
            "absent height has no root"
        );
        assert_eq!(
            source.vct_last_checkpoint_height(),
            block::Height(11),
            "handoff height comes from the supplied frontiers"
        );
    }

    /// The peer source reads roots persisted by the header-sync write path, and
    /// `invalidate` deletes a root so a later read misses it, letting the driver re-fetch
    /// a verifiable replacement from another peer. This un-poisons the store after a bad
    /// root is rejected by the committer, so one malicious peer cannot wedge the same
    /// rejected root in place forever. Exercises the same database rows production uses.
    #[test]
    fn peer_source_reads_and_invalidates_header_sync_roots() {
        let db = ephemeral_mainnet_db();
        db.insert_supplied_commitment_roots([BlockCommitmentRoots {
            height: block::Height(42),
            sapling_root: sapling::tree::NoteCommitmentTree::default().root(),
            orchard_root: orchard::tree::NoteCommitmentTree::default().root(),
            ironwood_root: zakura_chain::ironwood::tree::NoteCommitmentTree::default().root(),
            sapling_tx: 0,
            orchard_tx: 0,
            ironwood_tx: 0,
            auth_data_root: AuthDataRoot::from([0u8; 32]),
        }])
        .expect("writing header-sync roots to an ephemeral database succeeds");

        // The handoff frontier is mandatory for a peer source; its height is above the
        // roots under test so it does not interact with the lookups.
        let frontiers = FinalFrontiers {
            height: block::Height(50),
            sapling: Arc::new(Default::default()),
            orchard: Arc::new(Default::default()),
            sprout: Arc::new(Default::default()),
            ironwood: Arc::new(Default::default()),
        };
        let source = PeerSource::new(db, frontiers);

        assert!(
            source.vct_root(block::Height(42)).is_some(),
            "a header-sync-persisted root is read back by height"
        );
        assert!(
            source.vct_root(block::Height(43)).is_none(),
            "an absent height has no root"
        );

        source.invalidate(block::Height(42));

        assert!(
            source.vct_root(block::Height(42)).is_none(),
            "an invalidated root is gone, so the next read misses and a re-fetch can replace it"
        );
    }
}
