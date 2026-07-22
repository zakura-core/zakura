//! State-owned access to the commitment-root index.
//!
//! This module is the lifecycle boundary for commitment-root rows. It keeps
//! disk-row conversion, contiguous reads, and the distinct body, legacy
//! header, reorganization, rollback, and repair write policies in one place.

use std::ops::{Bound, RangeBounds, RangeInclusive};

use thiserror::Error;
use zakura_chain::{
    block::{self, Height},
    history_tree::HistoryTree,
    parallel::{
        commitment_aux::BlockCommitmentRoots, commitment_aux_verify::VerifiedHeaderCommitmentRoots,
    },
    parameters::NetworkUpgrade,
};

use crate::service::finalized_state::{
    disk_db::{DiskWriteBatch, ReadDisk},
    disk_format::{
        chain::{HistoryTreeDecodeError, HistoryTreeParts},
        shielded::CommitmentRootsByHeight,
        RawBytes,
    },
    IntoDisk, TypedColumnFamily,
};

use super::ZakuraDb;

/// The name of the per-height commitment-root column family.
pub const COMMITMENT_ROOTS_BY_HEIGHT: &str = "commitment_roots_by_height";

/// The name of the single-row authenticated header-root frontier column family.
pub const HEADER_ROOT_AUTH_FRONTIER: &str = "header_root_auth_frontier";

type CommitmentRootsCf<'cf> = TypedColumnFamily<'cf, Height, CommitmentRootsByHeight>;
type HeaderRootAuthFrontierCf<'cf> = TypedColumnFamily<'cf, RawBytes, RawBytes>;

const FRONTIER_FORMAT_VERSION: u8 = 1;
const FRONTIER_FIXED_BYTES: usize = 1 + 4 + 32 + 1;

/// The durable boundary through which peer-supplied roots have been authenticated.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderRootAuthFrontier {
    confirmed_height: Height,
    confirmed_hash: block::Hash,
    history_tree: HistoryTree,
}

impl HeaderRootAuthFrontier {
    /// Returns the last authenticated height.
    pub fn confirmed_height(&self) -> Height {
        self.confirmed_height
    }

    /// Returns the canonical hash binding the frontier to its header branch.
    pub fn confirmed_hash(&self) -> block::Hash {
        self.confirmed_hash
    }

    /// Returns the history tree containing roots through the confirmed height.
    pub fn history_tree(&self) -> &HistoryTree {
        &self.history_tree
    }
}

/// Errors restoring an authenticated header-root frontier.
#[derive(Debug, Error)]
pub enum HeaderRootAuthFrontierError {
    /// The frontier row uses an unsupported encoding.
    #[error("invalid header-root authentication frontier encoding")]
    InvalidEncoding,
    /// The persisted history tree could not be decoded.
    #[error("invalid header-root authentication history tree: {0}")]
    HistoryTree(#[from] HistoryTreeDecodeError),
    /// The history tree is not positioned at the recorded confirmed height.
    #[error(
        "header-root authentication history tree is at {tree_height:?}, not {confirmed_height:?}"
    )]
    TreeHeightMismatch {
        /// Height recorded by the frontier.
        confirmed_height: Height,
        /// Height reconstructed from the history tree.
        tree_height: Option<Height>,
    },
    /// The history tree is empty at or after Heartwood activation.
    #[error(
        "header-root authentication history tree is empty at {confirmed_height:?}, \
         at or after Heartwood activation {heartwood_height:?}"
    )]
    EmptyHistoryTree {
        /// Height recorded by the frontier.
        confirmed_height: Height,
        /// Heartwood activation height.
        heartwood_height: Height,
    },
    /// The frontier hash does not match the canonical stored header.
    #[error("header-root authentication frontier hash is not canonical at {height:?}")]
    CanonicalHashMismatch {
        /// Height whose hash did not match.
        height: Height,
    },
    /// A non-empty database has no durable authentication frontier.
    #[error("missing header-root authentication frontier for non-empty finalized state")]
    MissingFrontier,
    /// Header or root state exists without a finalized tip.
    #[error(
        "header-root authentication state exists without a finalized tip \
         (roots: {has_roots}, headers: {has_headers}, frontier: {has_frontier})"
    )]
    StateWithoutFinalizedTip {
        /// Whether commitment-root rows exist.
        has_roots: bool,
        /// Whether header-store rows exist.
        has_headers: bool,
        /// Whether a frontier row exists.
        has_frontier: bool,
    },
    /// The frontier is behind the finalized body tip.
    #[error(
        "header-root authentication frontier {frontier_height:?} is below body tip {body_tip:?}"
    )]
    FrontierBehindBodyTip {
        /// Frontier height.
        frontier_height: Height,
        /// Finalized body tip.
        body_tip: Height,
    },
    /// A commitment-root row is missing from the authenticated prefix.
    #[error("missing authenticated commitment-root row at {height:?}")]
    MissingRoot { height: Height },
    /// A commitment-root row exists above the authenticated frontier.
    #[error("commitment-root row at {height:?} is above the authenticated frontier")]
    RootAboveFrontier { height: Height },
    /// Verified promotion did not append exactly after the current frontier.
    #[error("verified root promotion starts at {actual:?}, expected exact append at {expected:?}")]
    NonContiguousAppend {
        /// Required first height.
        expected: Height,
        /// Supplied first height.
        actual: Height,
    },
    /// Heights inside a verified promotion are not contiguous.
    #[error("verified root promotion contains height {actual:?}, expected {expected:?}")]
    NonContiguousVerifiedPrefix {
        /// Required height.
        expected: Height,
        /// Supplied height.
        actual: Height,
    },
    /// A verified promotion would overwrite a non-body row outside the current frontier.
    #[error("commitment-root row already exists outside the frontier at {height:?}")]
    ExistingRootOutsideFrontier { height: Height },
    /// A height operation overflowed.
    #[error("header-root authentication height overflow")]
    HeightOverflow,
    /// The frontier could not be written.
    #[error("could not write header-root authentication frontier: {0}")]
    Storage(#[from] rocksdb::Error),
}

fn disk_row(roots: &BlockCommitmentRoots) -> CommitmentRootsByHeight {
    CommitmentRootsByHeight {
        sapling: roots.sapling_root,
        orchard: roots.orchard_root,
        auth_data_root: roots.auth_data_root,
        ironwood: roots.ironwood_root,
        sapling_tx: roots.sapling_tx,
        orchard_tx: roots.orchard_tx,
        ironwood_tx: roots.ironwood_tx,
    }
}

fn domain_roots(height: Height, row: CommitmentRootsByHeight) -> BlockCommitmentRoots {
    BlockCommitmentRoots {
        height,
        sapling_root: row.sapling,
        orchard_root: row.orchard,
        auth_data_root: row.auth_data_root,
        ironwood_root: row.ironwood,
        sapling_tx: row.sapling_tx,
        orchard_tx: row.orchard_tx,
        ironwood_tx: row.ironwood_tx,
    }
}

fn frontier_bytes(frontier: &HeaderRootAuthFrontier) -> RawBytes {
    let mut bytes = Vec::new();
    bytes.push(FRONTIER_FORMAT_VERSION);
    bytes.extend_from_slice(&frontier.confirmed_height.0.to_le_bytes());
    bytes.extend_from_slice(&frontier.confirmed_hash.0);
    match frontier.history_tree.as_ref() {
        Some(tree) => {
            bytes.push(1);
            bytes.extend(HistoryTreeParts::from(tree).as_bytes());
        }
        None => bytes.push(0),
    }
    RawBytes::new_raw_bytes(bytes)
}

fn validate_history_tree_height(
    db: &ZakuraDb,
    confirmed_height: Height,
    history_tree: &HistoryTree,
) -> Result<(), HeaderRootAuthFrontierError> {
    let tree_height = history_tree.as_ref().map(|tree| tree.current_height());
    if tree_height.is_some_and(|tree_height| tree_height != confirmed_height) {
        return Err(HeaderRootAuthFrontierError::TreeHeightMismatch {
            confirmed_height,
            tree_height,
        });
    }

    if let Some(heartwood_height) = NetworkUpgrade::Heartwood.activation_height(&db.network()) {
        if confirmed_height >= heartwood_height && tree_height.is_none() {
            return Err(HeaderRootAuthFrontierError::EmptyHistoryTree {
                confirmed_height,
                heartwood_height,
            });
        }
    }

    Ok(())
}

fn decode_frontier(
    db: &ZakuraDb,
    bytes: &RawBytes,
) -> Result<HeaderRootAuthFrontier, HeaderRootAuthFrontierError> {
    let bytes = bytes.raw_bytes();
    if bytes.len() < FRONTIER_FIXED_BYTES || bytes[0] != FRONTIER_FORMAT_VERSION {
        return Err(HeaderRootAuthFrontierError::InvalidEncoding);
    }

    let confirmed_height = Height(u32::from_le_bytes(
        bytes[1..5]
            .try_into()
            .map_err(|_| HeaderRootAuthFrontierError::InvalidEncoding)?,
    ));
    let confirmed_hash = block::Hash(
        bytes[5..37]
            .try_into()
            .map_err(|_| HeaderRootAuthFrontierError::InvalidEncoding)?,
    );
    let history_tree = match bytes[37] {
        0 if bytes.len() == FRONTIER_FIXED_BYTES => HistoryTree::default(),
        1 if bytes.len() > FRONTIER_FIXED_BYTES => HistoryTree::from(
            HistoryTreeParts::try_from_bytes(&bytes[FRONTIER_FIXED_BYTES..])?
                .with_network(&db.network())?,
        ),
        _ => return Err(HeaderRootAuthFrontierError::InvalidEncoding),
    };

    validate_history_tree_height(db, confirmed_height, &history_tree)?;

    if db.header_hash(confirmed_height) != Some(confirmed_hash) {
        return Err(HeaderRootAuthFrontierError::CanonicalHashMismatch {
            height: confirmed_height,
        });
    }

    Ok(HeaderRootAuthFrontier {
        confirmed_height,
        confirmed_hash,
        history_tree,
    })
}

fn inclusive_bounds(range: impl RangeBounds<Height>) -> Option<(Height, Height)> {
    let start = match range.start_bound() {
        Bound::Included(height) => *height,
        Bound::Excluded(height) => height.next().ok()?,
        Bound::Unbounded => Height::MIN,
    };
    let end = match range.end_bound() {
        Bound::Included(height) => *height,
        Bound::Excluded(height) => height.previous().ok()?,
        Bound::Unbounded => Height::MAX,
    };

    (start <= end).then_some((start, end))
}

impl ZakuraDb {
    fn commitment_roots_cf(&self) -> CommitmentRootsCf<'_> {
        CommitmentRootsCf::new(&self.db, COMMITMENT_ROOTS_BY_HEIGHT)
            .expect("column family was created when database was created")
    }

    fn header_root_auth_frontier_cf(&self) -> HeaderRootAuthFrontierCf<'_> {
        HeaderRootAuthFrontierCf::new(&self.db, HEADER_ROOT_AUTH_FRONTIER)
            .expect("column family was created when database was created")
    }

    pub(super) fn has_commitment_roots_index(&self) -> bool {
        CommitmentRootsCf::new(&self.db, COMMITMENT_ROOTS_BY_HEIGHT).is_some()
    }

    /// Returns the commitment roots stored at `height`.
    pub fn commitment_roots(&self, height: Height) -> Option<BlockCommitmentRoots> {
        self.commitment_roots_cf()
            .zs_get(&height)
            .map(|row| domain_roots(height, row))
    }

    /// Restores the authenticated header-root frontier using fallible tree decoding.
    pub fn try_header_root_auth_frontier(
        &self,
    ) -> Result<Option<HeaderRootAuthFrontier>, HeaderRootAuthFrontierError> {
        self.header_root_auth_frontier_cf()
            .zs_get(&RawBytes::new_raw_bytes(Vec::new()))
            .as_ref()
            .map(|bytes| decode_frontier(self, bytes))
            .transpose()
    }

    pub(crate) fn has_commitment_root_rows(&self) -> bool {
        !self.commitment_roots_cf().zs_is_empty()
    }

    pub(crate) fn has_header_root_auth_frontier_row(&self) -> bool {
        !self.header_root_auth_frontier_cf().zs_is_empty()
    }

    fn has_zakura_header_rows(&self) -> bool {
        [
            "zakura_header_hash_by_height",
            "zakura_header_height_by_hash",
            "zakura_header_by_height",
            "zakura_header_body_size_by_height",
        ]
        .into_iter()
        .any(|name| {
            self.db
                .cf_handle(name)
                .is_some_and(|cf| !self.db.zs_is_empty(&cf))
        })
    }

    /// Validates and restores the complete durable authenticated-root state.
    pub(crate) fn validate_header_root_auth_state(
        &self,
    ) -> Result<Option<HeaderRootAuthFrontier>, HeaderRootAuthFrontierError> {
        let has_roots = self.has_commitment_root_rows();
        let has_headers = self.has_zakura_header_rows();
        let has_frontier = self.has_header_root_auth_frontier_row();
        let Some((body_tip, _body_hash)) = self.tip() else {
            if has_roots || has_headers || has_frontier {
                return Err(HeaderRootAuthFrontierError::StateWithoutFinalizedTip {
                    has_roots,
                    has_headers,
                    has_frontier,
                });
            }
            return Ok(None);
        };

        let frontier = self
            .try_header_root_auth_frontier()?
            .ok_or(HeaderRootAuthFrontierError::MissingFrontier)?;
        if frontier.confirmed_height < body_tip {
            return Err(HeaderRootAuthFrontierError::FrontierBehindBodyTip {
                frontier_height: frontier.confirmed_height,
                body_tip,
            });
        }

        if let Ok(mut expected) = body_tip.next() {
            for (height, _row) in self
                .commitment_roots_cf()
                .zs_forward_range_iter(expected..=frontier.confirmed_height)
            {
                if height != expected {
                    return Err(HeaderRootAuthFrontierError::MissingRoot { height: expected });
                }
                expected = match expected.next() {
                    Ok(next) => next,
                    Err(_) => break,
                };
            }
            if expected <= frontier.confirmed_height {
                return Err(HeaderRootAuthFrontierError::MissingRoot { height: expected });
            }
        }

        if let Ok(first_above) = frontier.confirmed_height.next() {
            if let Some((height, _row)) = self
                .commitment_roots_cf()
                .zs_next_key_value_from(&first_above)
            {
                return Err(HeaderRootAuthFrontierError::RootAboveFrontier { height });
            }
        }

        Ok(Some(frontier))
    }

    #[cfg(test)]
    pub(crate) fn delete_header_root_auth_frontier_for_test(&self) {
        let mut batch = DiskWriteBatch::new();
        let _ = self
            .header_root_auth_frontier_cf()
            .with_batch_for_writing(&mut batch)
            .zs_delete(&RawBytes::new_raw_bytes(Vec::new()));
        self.write_batch(batch)
            .expect("test frontier deletion must write successfully");
    }

    /// Atomically promotes successfully verified roots and advances their durable frontier.
    ///
    /// Body-derived rows retain precedence if full blocks already own any promoted height.
    #[allow(dead_code)] // Called by the production authentication lane added in Phase 3.
    pub(crate) fn write_verified_header_commitment_roots(
        &self,
        verified: VerifiedHeaderCommitmentRoots,
    ) -> Result<(), HeaderRootAuthFrontierError> {
        let confirmed_roots = verified.confirmed_roots();
        let confirmed_hashes = verified.confirmed_hashes();
        let Some(first_roots) = confirmed_roots.first() else {
            return Ok(());
        };
        let Some((&confirmed_hash, last_roots)) =
            confirmed_hashes.last().zip(confirmed_roots.last())
        else {
            return Err(HeaderRootAuthFrontierError::InvalidEncoding);
        };
        if confirmed_roots.len() != confirmed_hashes.len() {
            return Err(HeaderRootAuthFrontierError::InvalidEncoding);
        }

        let frontier = self
            .validate_header_root_auth_state()?
            .ok_or(HeaderRootAuthFrontierError::MissingFrontier)?;
        let expected_start = frontier
            .confirmed_height
            .next()
            .map_err(|_| HeaderRootAuthFrontierError::HeightOverflow)?;
        if first_roots.height != expected_start {
            return Err(HeaderRootAuthFrontierError::NonContiguousAppend {
                expected: expected_start,
                actual: first_roots.height,
            });
        }

        let mut expected_height = expected_start;
        for (roots, hash) in confirmed_roots.iter().zip(confirmed_hashes) {
            if roots.height != expected_height {
                return Err(HeaderRootAuthFrontierError::NonContiguousVerifiedPrefix {
                    expected: expected_height,
                    actual: roots.height,
                });
            }
            if self.header_hash(roots.height) != Some(*hash) {
                return Err(HeaderRootAuthFrontierError::CanonicalHashMismatch {
                    height: roots.height,
                });
            }
            if !self.contains_height(roots.height) && self.commitment_roots(roots.height).is_some()
            {
                return Err(HeaderRootAuthFrontierError::ExistingRootOutsideFrontier {
                    height: roots.height,
                });
            }
            expected_height = match expected_height.next() {
                Ok(next) => next,
                Err(_) if roots.height == last_roots.height => expected_height,
                Err(_) => return Err(HeaderRootAuthFrontierError::HeightOverflow),
            };
        }

        let confirmed_height = last_roots.height;
        validate_history_tree_height(self, confirmed_height, verified.history_tree())?;
        let frontier = HeaderRootAuthFrontier {
            confirmed_height,
            confirmed_hash,
            history_tree: verified.history_tree().clone(),
        };

        let mut batch = DiskWriteBatch::new();
        for roots in confirmed_roots {
            batch.insert_verified_header_commitment_roots(self, roots);
        }
        batch.set_header_root_auth_frontier(self, &frontier);
        self.write_batch(batch)?;
        Ok(())
    }

    #[allow(dead_code)] // Used by the unregistered header-root auth frontier cutover.
    pub(crate) fn prepare_header_root_auth_frontier_from_body_tip(
        &self,
        batch: &mut DiskWriteBatch,
    ) -> Result<(), HeaderRootAuthFrontierError> {
        let Some((confirmed_height, confirmed_hash)) = self.tip() else {
            return Ok(());
        };
        let history_tree = (*self.try_history_tree()?).clone();
        validate_history_tree_height(self, confirmed_height, &history_tree)?;
        let frontier = HeaderRootAuthFrontier {
            confirmed_height,
            confirmed_hash,
            history_tree,
        };
        batch.set_header_root_auth_frontier(self, &frontier);
        Ok(())
    }

    /// Returns the contiguous stored prefix of `range`.
    ///
    /// The read stops at the first missing height.
    pub fn commitment_roots_by_height_range(
        &self,
        range: RangeInclusive<Height>,
    ) -> Vec<BlockCommitmentRoots> {
        self.contiguous_commitment_roots(range)
    }

    /// Returns legacy header-sync roots for the contiguous stored prefix of `range`.
    ///
    /// The index currently contains both body-derived and legacy header-supplied
    /// rows, so this compatibility name does not imply per-row provenance.
    pub fn zakura_header_commitment_roots_by_height_range(
        &self,
        range: impl RangeBounds<Height>,
    ) -> Vec<BlockCommitmentRoots> {
        self.contiguous_commitment_roots(range)
    }

    fn contiguous_commitment_roots(
        &self,
        range: impl RangeBounds<Height>,
    ) -> Vec<BlockCommitmentRoots> {
        let Some((start, end)) = inclusive_bounds(range) else {
            return Vec::new();
        };
        let cf = self.commitment_roots_cf();
        let mut roots = Vec::new();

        for height in (start.0..=end.0).map(Height) {
            let Some(row) = cf.zs_get(&height) else {
                break;
            };
            roots.push(domain_roots(height, row));
        }

        roots
    }

    /// Persists raw header roots outside a larger header transaction.
    ///
    /// This remains the provisional header-sync write path until the
    /// authenticated-root cutover is enabled. Prefer
    /// [`Self::write_verified_header_commitment_roots`] once that lane is live.
    pub fn insert_zakura_header_commitment_roots(
        &self,
        roots: impl IntoIterator<Item = BlockCommitmentRoots>,
    ) -> Result<(), rocksdb::Error> {
        let mut batch = DiskWriteBatch::new();
        for roots in roots {
            batch.insert_legacy_header_commitment_roots(self, &roots);
        }
        self.write_batch(batch)
    }

    /// Deletes provisional header roots outside a larger header transaction.
    pub fn delete_zakura_header_commitment_roots(
        &self,
        heights: impl IntoIterator<Item = Height>,
    ) -> Result<(), rocksdb::Error> {
        let mut batch = DiskWriteBatch::new();
        for height in heights {
            batch.delete_legacy_header_commitment_root(self, height);
        }
        self.write_batch(batch)
    }

    /// Returns at most `limit` root heights for startup repair.
    pub(crate) fn commitment_root_heights_for_repair(
        &self,
        start: Height,
        limit: usize,
    ) -> Vec<Height> {
        self.commitment_roots_cf()
            .zs_forward_range_iter(start..)
            .map(|(height, _row)| height)
            .take(limit)
            .collect()
    }

    /// Visits root rows in `range` for rollback and migration bookkeeping.
    pub(super) fn visit_commitment_roots_for_migration(
        &self,
        range: impl RangeBounds<Height>,
        mut visit: impl FnMut(Height, BlockCommitmentRoots),
    ) {
        for (height, row) in self.commitment_roots_cf().zs_forward_range_iter(range) {
            visit(height, domain_roots(height, row));
        }
    }
}

impl DiskWriteBatch {
    /// Inserts or replaces an authoritative body-derived commitment-root row.
    pub fn insert_body_derived_commitment_roots(
        &mut self,
        db: &ZakuraDb,
        roots: &BlockCommitmentRoots,
    ) {
        let _ = db
            .commitment_roots_cf()
            .with_batch_for_writing(self)
            .zs_insert(&roots.height, &disk_row(roots));
    }

    #[allow(dead_code)] // Called through the Phase 1 promotion boundary above.
    fn insert_verified_header_commitment_roots(
        &mut self,
        db: &ZakuraDb,
        roots: &BlockCommitmentRoots,
    ) {
        if db.contains_height(roots.height) {
            return;
        }

        let _ = db
            .commitment_roots_cf()
            .with_batch_for_writing(self)
            .zs_insert(&roots.height, &disk_row(roots));
    }

    pub(crate) fn set_header_root_auth_frontier(
        &mut self,
        db: &ZakuraDb,
        frontier: &HeaderRootAuthFrontier,
    ) {
        let _ = db
            .header_root_auth_frontier_cf()
            .with_batch_for_writing(self)
            .zs_insert(
                &RawBytes::new_raw_bytes(Vec::new()),
                &frontier_bytes(frontier),
            );
    }

    #[allow(dead_code)] // Wired when body tip owns the authenticated frontier.
    pub(crate) fn advance_header_root_auth_frontier_from_body(
        &mut self,
        db: &ZakuraDb,
        confirmed_height: Height,
        confirmed_hash: block::Hash,
        history_tree: &HistoryTree,
    ) -> Result<(), HeaderRootAuthFrontierError> {
        let stored_frontier = db.try_header_root_auth_frontier()?;
        if stored_frontier
            .as_ref()
            .is_some_and(|frontier| frontier.confirmed_height > confirmed_height)
            && db.header_hash(confirmed_height) == Some(confirmed_hash)
        {
            return Ok(());
        }

        self.rebase_header_root_auth_frontier(db, confirmed_height, confirmed_hash, history_tree)
    }

    #[allow(dead_code)] // Wired when body tip owns the authenticated frontier.
    pub(crate) fn rebase_header_root_auth_frontier(
        &mut self,
        db: &ZakuraDb,
        confirmed_height: Height,
        confirmed_hash: block::Hash,
        history_tree: &HistoryTree,
    ) -> Result<(), HeaderRootAuthFrontierError> {
        validate_history_tree_height(db, confirmed_height, history_tree)?;
        self.set_header_root_auth_frontier(
            db,
            &HeaderRootAuthFrontier {
                confirmed_height,
                confirmed_hash,
                history_tree: history_tree.clone(),
            },
        );
        Ok(())
    }

    #[allow(dead_code)] // Wired when the authenticated-root cutover is enabled.
    pub(crate) fn delete_header_root_auth_frontier(&mut self, db: &ZakuraDb) {
        let _ = db
            .header_root_auth_frontier_cf()
            .with_batch_for_writing(self)
            .zs_delete(&RawBytes::new_raw_bytes(Vec::new()));
    }

    #[allow(dead_code)] // Wired when the authenticated-root cutover is enabled.
    pub(crate) fn truncate_all_commitment_roots(&mut self, db: &ZakuraDb) {
        let writer = db
            .commitment_roots_cf()
            .with_batch_for_writing(self)
            .zs_delete_range(&Height::MIN, &Height::MAX)
            .zs_delete(&Height::MAX);
        let _ = writer;
    }

    /// Inserts a raw legacy header-supplied row unless a committed body owns the height.
    ///
    /// "Legacy" identifies the temporary pre-authentication behavior where header sync persists
    /// peer-supplied roots directly. The verified-root persistence boundary will replace this path.
    pub(super) fn insert_legacy_header_commitment_roots(
        &mut self,
        db: &ZakuraDb,
        roots: &BlockCommitmentRoots,
    ) {
        if db.contains_height(roots.height) {
            return;
        }

        let _ = db
            .commitment_roots_cf()
            .with_batch_for_writing(self)
            .zs_insert(&roots.height, &disk_row(roots));
    }

    /// Deletes one legacy header-supplied row.
    pub(super) fn delete_legacy_header_commitment_root(&mut self, db: &ZakuraDb, height: Height) {
        let _ = db
            .commitment_roots_cf()
            .with_batch_for_writing(self)
            .zs_delete(&height);
    }

    /// Deletes the inclusive legacy-header suffix displaced by a header reorganization.
    pub(super) fn delete_header_reorg_commitment_roots(
        &mut self,
        db: &ZakuraDb,
        start: Height,
        end: Height,
    ) {
        if start > end {
            return;
        }

        let mut writer = db.commitment_roots_cf().with_batch_for_writing(self);
        for height in (start.0..=end.0).map(Height) {
            writer = writer.zs_delete(&height);
        }
        let _ = writer;
    }

    /// Truncates authoritative rows strictly above a finalized rollback target.
    pub(crate) fn truncate_commitment_roots_after(&mut self, db: &ZakuraDb, target: Height) {
        let Ok(start) = target.next() else {
            return;
        };
        let writer = db
            .commitment_roots_cf()
            .with_batch_for_writing(self)
            .zs_delete_range(&start, &Height::MAX)
            .zs_delete(&Height::MAX);
        let _ = writer;
    }

    /// Deletes one row selected by startup repair or a database migration.
    pub(super) fn delete_commitment_root_for_repair(&mut self, db: &ZakuraDb, height: Height) {
        let _ = db
            .commitment_roots_cf()
            .with_batch_for_writing(self)
            .zs_delete(&height);
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    /// Inserts a body-derived row assembled from separate test fixture fields.
    pub fn insert_commitment_roots_by_height(
        &mut self,
        db: &ZakuraDb,
        height: Height,
        sapling_root: &zakura_chain::sapling::tree::Root,
        orchard_root: &zakura_chain::orchard::tree::Root,
        ironwood_root: &zakura_chain::ironwood::tree::Root,
        sapling_tx: u64,
        orchard_tx: u64,
        ironwood_tx: u64,
        auth_data_root: &zakura_chain::block::merkle::AuthDataRoot,
    ) {
        self.insert_body_derived_commitment_roots(
            db,
            &BlockCommitmentRoots {
                height,
                sapling_root: *sapling_root,
                orchard_root: *orchard_root,
                auth_data_root: *auth_data_root,
                ironwood_root: *ironwood_root,
                sapling_tx,
                orchard_tx,
                ironwood_tx,
            },
        );
    }

    #[cfg(test)]
    /// Deletes the half-open row range used by legacy serving tests.
    pub fn delete_range_commitment_roots_by_height(
        &mut self,
        db: &ZakuraDb,
        from: &Height,
        until_strictly_before: &Height,
    ) {
        let _ = db
            .commitment_roots_cf()
            .with_batch_for_writing(self)
            .zs_delete_range(from, until_strictly_before);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{
        constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
        service::finalized_state::{WriteDisk, STATE_COLUMN_FAMILIES_IN_CODE},
        Config,
    };
    use zakura_chain::{
        block::Block,
        parallel::commitment_aux_verify::verify_supplied_roots_from_parts,
        parameters::{Network, NetworkUpgrade},
        serialization::ZcashDeserializeInto,
    };

    fn ephemeral_mainnet_db() -> ZakuraDb {
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
        .expect("ephemeral database opens")
    }

    fn mainnet_block_at(height: u32) -> Arc<Block> {
        let (blocks, _) = Network::Mainnet.block_sapling_roots_map();
        Arc::new(
            blocks
                .get(&height)
                .expect("test vector block exists")
                .zcash_deserialize_into()
                .expect("test vector block deserializes"),
        )
    }

    fn mainnet_sapling_root_at(height: u32) -> zakura_chain::sapling::tree::Root {
        let (_, roots) = Network::Mainnet.block_sapling_roots_map();
        zakura_chain::sapling::tree::Root::try_from(**roots.get(&height).expect("test root exists"))
            .expect("test root is valid")
    }

    fn roots_from_block(block: &Block) -> BlockCommitmentRoots {
        let height = block.coinbase_height().expect("test block has a height");
        BlockCommitmentRoots {
            height,
            sapling_root: mainnet_sapling_root_at(height.0),
            orchard_root: zakura_chain::orchard::tree::NoteCommitmentTree::default().root(),
            ironwood_root: zakura_chain::ironwood::tree::NoteCommitmentTree::default().root(),
            sapling_tx: block.sapling_transactions_count(),
            orchard_tx: block.orchard_transactions_count(),
            ironwood_tx: block.ironwood_transactions_count(),
            auth_data_root: block.auth_data_root(),
        }
    }

    fn seed_frontier_and_headers(
        db: &ZakuraDb,
        frontier_height: Height,
        confirmed: &[(Height, block::Hash)],
    ) {
        let base_hash = block::Hash([0x55; 32]);
        let hash_by_height = db.db.cf_handle("hash_by_height").unwrap();
        let height_by_hash = db.db.cf_handle("height_by_hash").unwrap();
        let header_hash_by_height = db.db.cf_handle("zakura_header_hash_by_height").unwrap();
        let mut batch = DiskWriteBatch::new();
        batch.zs_insert(&hash_by_height, frontier_height, base_hash);
        batch.zs_insert(&height_by_hash, base_hash, frontier_height);
        for (height, hash) in confirmed {
            batch.zs_insert(&header_hash_by_height, *height, *hash);
        }
        batch
            .rebase_header_root_auth_frontier(
                db,
                frontier_height,
                base_hash,
                &HistoryTree::default(),
            )
            .expect("pre-Heartwood frontier is coherent");
        db.write_batch(batch).expect("frontier fixture writes");
    }

    fn verified_activation_root() -> (
        VerifiedHeaderCommitmentRoots,
        BlockCommitmentRoots,
        block::Hash,
    ) {
        let activation = NetworkUpgrade::Heartwood
            .activation_height(&Network::Mainnet)
            .expect("Mainnet has Heartwood");
        let block = mainnet_block_at(activation.0);
        let successor = mainnet_block_at(activation.0 + 1);
        let roots = roots_from_block(&block);
        let successor_roots = roots_from_block(&successor);
        let verified = verify_supplied_roots_from_parts(
            &Network::Mainnet,
            HistoryTree::default(),
            [
                (block.header.as_ref(), &roots),
                (successor.header.as_ref(), &successor_roots),
            ],
        )
        .expect("real activation roots verify");
        let hash = block::Hash::from(block.header.as_ref());
        (verified, roots, hash)
    }

    #[test]
    fn production_root_column_access_is_centralized() {
        let production_sources = [
            ("block.rs", include_str!("block.rs")),
            ("shielded.rs", include_str!("shielded.rs")),
            ("rollback.rs", include_str!("rollback.rs")),
            (
                "block/startup_audit.rs",
                include_str!("block/startup_audit.rs"),
            ),
        ];

        for (path, source) in production_sources {
            let compact = source.split_whitespace().collect::<String>();
            assert!(
                !compact.contains("cf_handle(COMMITMENT_ROOTS_BY_HEIGHT)"),
                "{path} accesses the commitment-root column family directly",
            );
        }
    }

    #[test]
    fn malformed_frontier_history_tree_returns_decode_error() {
        let db = ephemeral_mainnet_db();
        let mut malformed = vec![0; FRONTIER_FIXED_BYTES + 1];
        malformed[0] = FRONTIER_FORMAT_VERSION;
        malformed[37] = 1;
        malformed[FRONTIER_FIXED_BYTES] = 0xff;
        let mut batch = DiskWriteBatch::new();
        let _ = db
            .header_root_auth_frontier_cf()
            .with_batch_for_writing(&mut batch)
            .zs_insert(
                &RawBytes::new_raw_bytes(Vec::new()),
                &RawBytes::new_raw_bytes(malformed),
            );
        db.write_batch(batch).expect("malformed test row writes");

        assert!(matches!(
            db.try_header_root_auth_frontier(),
            Err(HeaderRootAuthFrontierError::HistoryTree(_))
        ));
    }

    #[test]
    fn empty_history_tree_is_rejected_at_heartwood() {
        let db = ephemeral_mainnet_db();
        let heartwood = NetworkUpgrade::Heartwood
            .activation_height(&Network::Mainnet)
            .expect("Mainnet has Heartwood");
        let mut batch = DiskWriteBatch::new();

        assert!(matches!(
            batch.rebase_header_root_auth_frontier(
                &db,
                heartwood,
                block::Hash([0x44; 32]),
                &HistoryTree::default(),
            ),
            Err(HeaderRootAuthFrontierError::EmptyHistoryTree {
                confirmed_height,
                ..
            }) if confirmed_height == heartwood
        ));
    }

    #[test]
    fn verified_promotion_appends_exact_canonical_prefix() {
        let db = ephemeral_mainnet_db();
        let (verified, roots, hash) = verified_activation_root();
        let base = roots
            .height
            .previous()
            .expect("activation has a predecessor");
        seed_frontier_and_headers(&db, base, &[(roots.height, hash)]);

        db.write_verified_header_commitment_roots(verified)
            .expect("canonical verified prefix promotes");

        assert_eq!(db.commitment_roots(roots.height), Some(roots.clone()));
        let frontier = db
            .validate_header_root_auth_state()
            .expect("promoted frontier is coherent")
            .expect("non-empty state has a frontier");
        assert_eq!(frontier.confirmed_height(), roots.height);
        assert_eq!(frontier.confirmed_hash(), hash);

        let (_cancel_sender, cancel_receiver) = crossbeam_channel::bounded(1);
        let validation =
            crate::service::finalized_state::disk_format::upgrade::DiskFormatUpgrade::validate(
                &crate::service::finalized_state::disk_format::upgrade::header_root_auth_frontier::Upgrade,
                &db,
                &cancel_receiver,
            )
            .expect("validation is not cancelled");
        assert_eq!(validation, Ok(()));
    }

    #[test]
    fn verified_promotion_rejects_stale_prefix() {
        let db = ephemeral_mainnet_db();
        let (verified, roots, hash) = verified_activation_root();
        let base = roots
            .height
            .previous()
            .expect("activation has a predecessor");
        seed_frontier_and_headers(&db, base, &[(roots.height, hash)]);
        db.write_verified_header_commitment_roots(verified.clone())
            .expect("first promotion succeeds");

        assert!(matches!(
            db.write_verified_header_commitment_roots(verified),
            Err(HeaderRootAuthFrontierError::NonContiguousAppend { .. })
        ));
    }

    #[test]
    fn verified_promotion_rejects_gap() {
        let db = ephemeral_mainnet_db();
        let (verified, roots, hash) = verified_activation_root();
        let base = Height(roots.height.0 - 2);
        seed_frontier_and_headers(&db, base, &[(roots.height, hash)]);

        assert!(matches!(
            db.write_verified_header_commitment_roots(verified),
            Err(HeaderRootAuthFrontierError::NonContiguousAppend { .. })
        ));
    }

    #[test]
    fn verified_promotion_rejects_noncanonical_hash() {
        let db = ephemeral_mainnet_db();
        let (verified, roots, _hash) = verified_activation_root();
        let base = roots
            .height
            .previous()
            .expect("activation has a predecessor");
        seed_frontier_and_headers(&db, base, &[(roots.height, block::Hash([0x99; 32]))]);

        assert!(matches!(
            db.write_verified_header_commitment_roots(verified),
            Err(HeaderRootAuthFrontierError::CanonicalHashMismatch { height })
                if height == roots.height
        ));
    }

    #[test]
    fn verified_insert_preserves_body_derived_row() {
        let db = ephemeral_mainnet_db();
        let (_verified, mut peer_roots, hash) = verified_activation_root();
        let body_roots = peer_roots.clone();
        peer_roots.sapling_tx = peer_roots.sapling_tx.saturating_add(1);
        let hash_by_height = db.db.cf_handle("hash_by_height").unwrap();
        let mut batch = DiskWriteBatch::new();
        batch.zs_insert(&hash_by_height, body_roots.height, hash);
        batch.insert_body_derived_commitment_roots(&db, &body_roots);
        db.write_batch(batch).expect("body fixture writes");

        let mut batch = DiskWriteBatch::new();
        batch.insert_verified_header_commitment_roots(&db, &peer_roots);
        db.write_batch(batch).expect("verified insert batch writes");

        assert_eq!(db.commitment_roots(body_roots.height), Some(body_roots));
    }

    #[test]
    fn conflicting_body_tip_atomically_rebases_authenticated_frontier() {
        let db = ephemeral_mainnet_db();
        let (verified, roots, hash) = verified_activation_root();
        let base = roots
            .height
            .previous()
            .expect("activation has a predecessor");
        seed_frontier_and_headers(&db, base, &[(roots.height, hash)]);
        db.write_verified_header_commitment_roots(verified)
            .expect("authenticated lead promotes");

        let replacement_body_hash = block::Hash([0x77; 32]);
        let hash_by_height = db.db.cf_handle("hash_by_height").unwrap();
        let mut batch = DiskWriteBatch::new();
        batch.zs_insert(&hash_by_height, base, replacement_body_hash);
        batch.truncate_commitment_roots_after(&db, base);
        batch
            .advance_header_root_auth_frontier_from_body(
                &db,
                base,
                replacement_body_hash,
                &HistoryTree::default(),
            )
            .expect("conflicting body rebases the frontier");
        db.write_batch(batch).expect("body rebase batch writes");

        let frontier = db
            .validate_header_root_auth_state()
            .expect("rebased frontier is coherent")
            .expect("body state has a frontier");
        assert_eq!(frontier.confirmed_height(), base);
        assert_eq!(frontier.confirmed_hash(), replacement_body_hash);
        assert_eq!(db.commitment_roots(roots.height), None);
    }
}
