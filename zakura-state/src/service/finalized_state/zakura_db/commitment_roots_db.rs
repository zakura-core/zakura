//! State-owned access to the commitment-root index.
//!
//! This module is the lifecycle boundary for commitment-root rows. It keeps
//! disk-row conversion, contiguous reads, and the distinct body, authenticated
//! header, reorganization, rollback, and repair write policies in one place.

use std::{
    ops::{Bound, RangeBounds, RangeInclusive},
    sync::Arc,
};

use thiserror::Error;
use zakura_chain::{
    block::{self, merkle::AuthDataRoot, Height},
    history_tree::HistoryTree,
    parallel::{
        commitment_aux::BlockCommitmentRoots,
        commitment_aux_verify::{
            verify_supplied_roots_from_parts, HeaderWitness, SuppliedRootsError,
            VerifiedHeaderCommitmentRoots,
        },
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

use super::{highest_completed_checkpoint::HighestCompletedCheckpoint, ZakuraDb};

/// The name of the per-height commitment-root column family.
pub const COMMITMENT_ROOTS_BY_HEIGHT: &str = "commitment_roots_by_height";

/// The name of the single-row authenticated header-root frontier column family.
pub const HEADER_ROOT_AUTH_FRONTIER: &str = "header_root_auth_frontier";

type CommitmentRootsCf<'cf> = TypedColumnFamily<'cf, Height, CommitmentRootsByHeight>;
type HeaderRootAuthFrontierCf<'cf> = TypedColumnFamily<'cf, RawBytes, RawBytes>;

const LEGACY_FRONTIER_FORMAT_VERSION: u8 = 1;
const FRONTIER_FORMAT_VERSION: u8 = 2;
const LEGACY_FRONTIER_FIXED_BYTES: usize = 1 + 4 + 32 + 1;
const FRONTIER_PREFIX_BYTES: usize = 1 + 4 + 32;
const WITNESS_FIXED_BYTES: usize = 4 + 32 + 32;
const AUTH_FRONTIER_KEY: &[u8] = &[];

/// Compact identity for the retained terminal header witness.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct HeaderWitnessState {
    /// The witness header's height.
    pub height: Height,
    /// The canonical witness header hash.
    pub hash: block::Hash,
}

/// Compact header-root authentication progress published to header sync.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct HeaderRootAuthState {
    /// The last height whose supplied roots have been authenticated.
    pub authenticated_height: Height,
    /// The canonical header hash at `authenticated_height`.
    pub authenticated_hash: block::Hash,
    /// The highest completely stored configured checkpoint height.
    pub completed_checkpoint_height: Height,
    /// The configured checkpoint hash at `completed_checkpoint_height`.
    pub completed_checkpoint_hash: block::Hash,
    /// The retained terminal header witness, when it is canonical and valid.
    pub header_witness: Option<HeaderWitnessState>,
}

/// The durable change made by successful supplied-root authentication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HeaderRootAuthUpdate {
    /// The authenticated root frontier advanced over this inclusive range.
    Advanced {
        /// Newly authenticated root heights.
        authenticated: RangeInclusive<Height>,
    },
    /// A missing terminal witness was recovered without advancing the frontier.
    WitnessRecovered {
        /// The recovered canonical witness.
        witness: HeaderWitnessState,
    },
}

/// A successful supplied-root authentication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthenticatedHeaderRoots {
    /// Authentication progress after the durable write.
    pub state: HeaderRootAuthState,
    /// The durable authentication change.
    pub update: HeaderRootAuthUpdate,
}

/// Stable classification for supplied-root authentication outcomes.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum AuthenticateHeaderRootsOutcome {
    /// The caller raced a newer canonical frontier and should refresh.
    Stale,
    /// The supplied headers or roots are invalid.
    Invalid,
    /// Durable state is incoherent or a local read/write failed.
    Local,
}

/// Errors authenticating and durably promoting peer-supplied roots.
#[derive(Debug, Error)]
pub enum AuthenticateHeaderRootsError {
    /// The caller's state snapshot is no longer current.
    #[error("stale header-root authentication state: expected {expected:?}, current {current:?}")]
    StaleState {
        /// State supplied by the caller.
        expected: HeaderRootAuthState,
        /// Current durable state.
        current: HeaderRootAuthState,
    },
    /// The supplied anchor does not match the current authenticated frontier.
    #[error("header-root anchor {actual} does not match current frontier hash {expected}")]
    AnchorMismatch {
        /// Current authenticated hash.
        expected: block::Hash,
        /// Supplied anchor.
        actual: block::Hash,
    },
    /// The supplied range does not start immediately after the authenticated frontier.
    #[error("header-root range starts at {actual:?}, expected {expected:?}")]
    StartMismatch {
        /// Required start height.
        expected: Height,
        /// Supplied start height.
        actual: Height,
    },
    /// Header and root counts differ.
    #[error("header-root item count mismatch: {headers} headers, {roots} roots")]
    CountMismatch {
        /// Number of headers.
        headers: usize,
        /// Number of root records.
        roots: usize,
    },
    /// At least a final header witness record is required.
    #[error("header-root authentication requires at least one aligned item, got {items}")]
    MissingHeaderWitness {
        /// Number of supplied items.
        items: usize,
    },
    /// The one-item recovery is unnecessary because a valid witness is already retained.
    #[error("header-root witness recovery is unnecessary: retained witness {witness:?}")]
    WitnessAlreadyPresent {
        /// The currently retained canonical witness.
        witness: HeaderWitnessState,
    },
    /// The finalized body tip has caught the authenticated frontier.
    #[error(
        "header-root witness recovery is unnecessary at frontier {authenticated_height:?}: finalized body tip is {finalized_body_tip:?}"
    )]
    WitnessNotNeeded {
        /// Current authenticated frontier height.
        authenticated_height: Height,
        /// Current finalized body tip.
        finalized_body_tip: Height,
    },
    /// A root record is not at its required contiguous height.
    #[error("header-root item is at {actual:?}, expected {expected:?}")]
    NonContiguous {
        /// Required item height.
        expected: Height,
        /// Supplied item height.
        actual: Height,
    },
    /// A supplied header is not the canonical stored header at its height.
    #[error("supplied header is not canonical at {height:?}")]
    NonCanonicalHeader {
        /// Non-canonical height.
        height: Height,
    },
    /// The successor witness is not itself covered by the completed checkpoint.
    #[error(
        "header-root successor witness {witness_height:?} is above completed checkpoint {completed_checkpoint_height:?}"
    )]
    WitnessAboveCompletedCheckpoint {
        /// Successor witness height.
        witness_height: Height,
        /// Current completed checkpoint height.
        completed_checkpoint_height: Height,
    },
    /// Cryptographic commitment verification failed.
    #[error("supplied roots failed authentication at {height:?}: {source}")]
    Verification {
        /// First failing height.
        height: Height,
        /// Commitment or history-tree failure.
        #[source]
        source: SuppliedRootsError,
    },
    /// A height operation overflowed.
    #[error("header-root authentication height overflow")]
    HeightOverflow,
    /// Durable authenticated-root state was invalid or could not be updated.
    #[error("header-root authentication state failure: {0}")]
    Frontier(#[from] HeaderRootAuthFrontierError),
}

impl AuthenticateHeaderRootsError {
    /// Returns the stable outcome class for this error.
    pub fn outcome(&self) -> AuthenticateHeaderRootsOutcome {
        match self {
            Self::StaleState { .. }
            | Self::AnchorMismatch { .. }
            | Self::StartMismatch { .. }
            | Self::NonCanonicalHeader { .. }
            | Self::WitnessAboveCompletedCheckpoint { .. }
            | Self::WitnessAlreadyPresent { .. }
            | Self::WitnessNotNeeded { .. } => AuthenticateHeaderRootsOutcome::Stale,
            Self::CountMismatch { .. }
            | Self::MissingHeaderWitness { .. }
            | Self::NonContiguous { .. }
            | Self::Verification { .. }
            | Self::HeightOverflow => AuthenticateHeaderRootsOutcome::Invalid,
            Self::Frontier(_) => AuthenticateHeaderRootsOutcome::Local,
        }
    }
}

/// The durable boundary through which peer-supplied roots have been authenticated.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderRootAuthFrontier {
    confirmed_height: Height,
    confirmed_hash: block::Hash,
    header_witness: Option<HeaderWitness>,
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

    /// Returns compact authentication progress suitable for a watch channel.
    pub fn state(&self, completed_checkpoint: HighestCompletedCheckpoint) -> HeaderRootAuthState {
        HeaderRootAuthState {
            authenticated_height: self.confirmed_height,
            authenticated_hash: self.confirmed_hash,
            completed_checkpoint_height: completed_checkpoint.height,
            completed_checkpoint_hash: completed_checkpoint.hash,
            header_witness: self.header_witness.map(|witness| HeaderWitnessState {
                height: witness.height(),
                hash: witness.hash(),
            }),
        }
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
    /// The retained header witness is not exactly one height above the frontier.
    #[error("header witness is at {actual:?}, expected {expected:?}")]
    HeaderWitnessHeightMismatch {
        /// Required successor height.
        expected: Height,
        /// Retained witness height.
        actual: Height,
    },
    /// The retained header witness does not match the canonical stored header.
    #[error("header witness hash is not canonical at {height:?}")]
    HeaderWitnessHashMismatch {
        /// Witness height whose hash did not match.
        height: Height,
    },
    /// A non-empty database has no durable authentication frontier.
    #[error("missing header-root authentication frontier for non-empty finalized state")]
    MissingFrontier,
    /// A durable authentication frontier exists without a completed checkpoint.
    ///
    /// Reachable after `HighestCompletedCheckpointTracker::rebind_from_db` clears
    /// published progress on reconstruction failure while the frontier row remains.
    #[error("durable header-root authentication frontier exists without a completed checkpoint")]
    MissingCompletedCheckpoint,
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
    /// The serialized state write task is unavailable.
    #[error("header-root authentication write task is unavailable")]
    WriteTaskUnavailable,
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
    match frontier.header_witness {
        Some(witness) => {
            bytes.push(1);
            bytes.extend_from_slice(&witness.height().0.to_le_bytes());
            bytes.extend_from_slice(&witness.hash().0);
            bytes.extend_from_slice(&<[u8; 32]>::from(witness.auth_data_root()));
        }
        None => bytes.push(0),
    }
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

fn validate_header_witness(
    db: &ZakuraDb,
    confirmed_height: Height,
    witness: HeaderWitness,
) -> Result<(), HeaderRootAuthFrontierError> {
    let expected = confirmed_height
        .next()
        .map_err(|_| HeaderRootAuthFrontierError::HeightOverflow)?;
    if witness.height() != expected {
        return Err(HeaderRootAuthFrontierError::HeaderWitnessHeightMismatch {
            expected,
            actual: witness.height(),
        });
    }
    if db.header_hash(expected) != Some(witness.hash()) {
        return Err(HeaderRootAuthFrontierError::HeaderWitnessHashMismatch { height: expected });
    }

    Ok(())
}

fn decode_frontier(
    db: &ZakuraDb,
    bytes: &RawBytes,
) -> Result<HeaderRootAuthFrontier, HeaderRootAuthFrontierError> {
    let bytes = bytes.raw_bytes();
    if bytes.len() < LEGACY_FRONTIER_FIXED_BYTES {
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
    let (header_witness, history_tree_tag_offset) = match bytes[0] {
        LEGACY_FRONTIER_FORMAT_VERSION => (None, FRONTIER_PREFIX_BYTES),
        FRONTIER_FORMAT_VERSION => match bytes.get(FRONTIER_PREFIX_BYTES) {
            Some(0) => (None, FRONTIER_PREFIX_BYTES + 1),
            Some(1) if bytes.len() > FRONTIER_PREFIX_BYTES + 1 + WITNESS_FIXED_BYTES => {
                let witness_offset = FRONTIER_PREFIX_BYTES + 1;
                let height = Height(u32::from_le_bytes(
                    bytes[witness_offset..witness_offset + 4]
                        .try_into()
                        .map_err(|_| HeaderRootAuthFrontierError::InvalidEncoding)?,
                ));
                let hash_offset = witness_offset + 4;
                let hash = block::Hash(
                    bytes[hash_offset..hash_offset + 32]
                        .try_into()
                        .map_err(|_| HeaderRootAuthFrontierError::InvalidEncoding)?,
                );
                let auth_data_root_offset = hash_offset + 32;
                let auth_data_root = AuthDataRoot::from(
                    <[u8; 32]>::try_from(&bytes[auth_data_root_offset..auth_data_root_offset + 32])
                        .map_err(|_| HeaderRootAuthFrontierError::InvalidEncoding)?,
                );
                (
                    Some(HeaderWitness::from_parts(height, hash, auth_data_root)),
                    auth_data_root_offset + 32,
                )
            }
            _ => return Err(HeaderRootAuthFrontierError::InvalidEncoding),
        },
        _ => return Err(HeaderRootAuthFrontierError::InvalidEncoding),
    };
    let history_tree_bytes = history_tree_tag_offset + 1;
    let history_tree = match bytes.get(history_tree_tag_offset) {
        Some(0) if bytes.len() == history_tree_bytes => HistoryTree::default(),
        Some(1) if bytes.len() > history_tree_bytes => HistoryTree::from(
            HistoryTreeParts::try_from_bytes(&bytes[history_tree_bytes..])?
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
    // A header-only reorg can stale the optional successor witness without changing
    // the confirmed frontier. Treat it as missing so header sync can authenticate it again.
    let header_witness = header_witness
        .filter(|witness| validate_header_witness(db, confirmed_height, *witness).is_ok());

    Ok(HeaderRootAuthFrontier {
        confirmed_height,
        confirmed_hash,
        header_witness,
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

    /// Returns the retained authorizing-data root when its header witness matches
    /// the canonical header requested by the caller.
    pub(crate) fn header_witness_auth_data_root(
        &self,
        height: Height,
        hash: block::Hash,
    ) -> Option<AuthDataRoot> {
        let frontier = self.try_header_root_auth_frontier().ok()??;
        let witness = frontier.header_witness?;
        (witness.height() == height && witness.hash() == hash).then_some(witness.auth_data_root())
    }

    /// Restores the authenticated header-root frontier using fallible tree decoding.
    pub fn try_header_root_auth_frontier(
        &self,
    ) -> Result<Option<HeaderRootAuthFrontier>, HeaderRootAuthFrontierError> {
        let cf = self.header_root_auth_frontier_cf();
        cf.zs_get(&RawBytes::new_raw_bytes(AUTH_FRONTIER_KEY.to_vec()))
            .as_ref()
            .map(|bytes| decode_frontier(self, bytes))
            .transpose()
    }

    pub(crate) fn has_commitment_root_rows(&self) -> bool {
        !self.commitment_roots_cf().zs_is_empty()
    }

    pub(crate) fn has_header_root_auth_frontier_row(&self) -> bool {
        self.header_root_auth_frontier_cf()
            .zs_get(&RawBytes::new_raw_bytes(AUTH_FRONTIER_KEY.to_vec()))
            .is_some()
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

    /// Loads and checks the durable frontier without auditing its historical prefix.
    ///
    /// Full prefix reconstruction is performed by [`Self::validate_header_root_auth_state`]
    /// during startup and explicit disk-format validation.
    pub(crate) fn load_header_root_auth_frontier(
        &self,
    ) -> Result<Option<HeaderRootAuthFrontier>, HeaderRootAuthFrontierError> {
        let Some((body_tip, _body_hash)) = self.tip() else {
            let has_roots = self.has_commitment_root_rows();
            let has_headers = self.has_zakura_header_rows();
            let has_frontier = self.has_header_root_auth_frontier_row();
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

    /// Validates and restores the complete durable authenticated-root state.
    pub(crate) fn validate_header_root_auth_state(
        &self,
    ) -> Result<Option<HeaderRootAuthFrontier>, HeaderRootAuthFrontierError> {
        let Some(frontier) = self.load_header_root_auth_frontier()? else {
            return Ok(None);
        };

        let (body_tip, _body_hash) = self
            .tip()
            .expect("the lightweight frontier check requires a finalized tip");

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

        Ok(Some(frontier))
    }

    #[cfg(test)]
    pub(crate) fn delete_header_root_auth_frontier_for_test(&self) {
        let mut batch = DiskWriteBatch::new();
        let _ = self
            .header_root_auth_frontier_cf()
            .with_batch_for_writing(&mut batch)
            .zs_delete(&RawBytes::new_raw_bytes(AUTH_FRONTIER_KEY.to_vec()));
        self.write_batch(batch)
            .expect("test frontier deletion must write successfully");
    }

    /// Atomically promotes successfully verified roots and advances their durable frontier.
    ///
    /// Body-derived rows retain precedence if full blocks already own any promoted height.
    pub(crate) fn write_verified_header_commitment_roots(
        &self,
        verified: VerifiedHeaderCommitmentRoots,
    ) -> Result<HeaderRootAuthFrontier, HeaderRootAuthFrontierError> {
        let confirmed_roots = verified.confirmed_roots();
        let confirmed_hashes = verified.confirmed_hashes();
        let Some(first_roots) = confirmed_roots.first() else {
            return self
                .load_header_root_auth_frontier()?
                .ok_or(HeaderRootAuthFrontierError::MissingFrontier);
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
            .load_header_root_auth_frontier()?
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
        let header_witness = verified.header_witness();
        if let Some(witness) = header_witness {
            validate_header_witness(self, confirmed_height, witness)?;
        }
        let frontier = HeaderRootAuthFrontier {
            confirmed_height,
            confirmed_hash,
            header_witness,
            history_tree: verified.history_tree().clone(),
        };

        let mut batch = DiskWriteBatch::new();
        for roots in confirmed_roots {
            batch.insert_verified_header_commitment_roots(self, roots);
        }
        batch.set_header_root_auth_frontier(self, &frontier);
        self.write_batch(batch)?;
        Ok(frontier)
    }

    /// Atomically restores a missing terminal witness without changing the root frontier.
    fn write_recovered_header_witness(
        &self,
        frontier: HeaderRootAuthFrontier,
        verified: VerifiedHeaderCommitmentRoots,
    ) -> Result<HeaderRootAuthFrontier, HeaderRootAuthFrontierError> {
        if !verified.confirmed_roots().is_empty()
            || !verified.confirmed_hashes().is_empty()
            || verified.history_tree() != &frontier.history_tree
        {
            return Err(HeaderRootAuthFrontierError::InvalidEncoding);
        }
        let witness = verified
            .header_witness()
            .ok_or(HeaderRootAuthFrontierError::InvalidEncoding)?;
        validate_header_witness(self, frontier.confirmed_height, witness)?;

        let recovered = HeaderRootAuthFrontier {
            header_witness: Some(witness),
            ..frontier
        };
        let mut batch = DiskWriteBatch::new();
        batch.set_header_root_auth_frontier(self, &recovered);
        self.write_batch(batch)?;
        Ok(recovered)
    }

    /// Validates supplied roots against the exact durable frontier and atomically promotes them.
    pub(crate) fn authenticate_header_roots(
        &self,
        completed_checkpoint: HighestCompletedCheckpoint,
        expected_state: HeaderRootAuthState,
        anchor: block::Hash,
        start: Height,
        headers: &[Arc<block::Header>],
        roots: &[BlockCommitmentRoots],
    ) -> Result<AuthenticatedHeaderRoots, AuthenticateHeaderRootsError> {
        let frontier = self
            .load_header_root_auth_frontier()?
            .ok_or(HeaderRootAuthFrontierError::MissingFrontier)?;
        let current = frontier.state(completed_checkpoint);
        // Checkpoint coverage can advance independently while this request is in flight. That
        // does not stale the authentication base: the current checkpoint is still used below
        // to bound the witness, while only the durable root frontier is compare-and-swapped.
        if expected_state.authenticated_height != current.authenticated_height
            || expected_state.authenticated_hash != current.authenticated_hash
            || expected_state.header_witness != current.header_witness
        {
            return Err(AuthenticateHeaderRootsError::StaleState {
                expected: expected_state,
                current,
            });
        }
        if anchor != current.authenticated_hash {
            return Err(AuthenticateHeaderRootsError::AnchorMismatch {
                expected: current.authenticated_hash,
                actual: anchor,
            });
        }
        let expected_start = current
            .authenticated_height
            .next()
            .map_err(|_| AuthenticateHeaderRootsError::HeightOverflow)?;
        if start != expected_start {
            return Err(AuthenticateHeaderRootsError::StartMismatch {
                expected: expected_start,
                actual: start,
            });
        }
        if headers.len() != roots.len() {
            return Err(AuthenticateHeaderRootsError::CountMismatch {
                headers: headers.len(),
                roots: roots.len(),
            });
        }
        if headers.is_empty() {
            return Err(AuthenticateHeaderRootsError::MissingHeaderWitness {
                items: headers.len(),
            });
        }

        let mut expected_height = start;
        for (header, roots) in headers.iter().zip(roots) {
            if roots.height != expected_height {
                return Err(AuthenticateHeaderRootsError::NonContiguous {
                    expected: expected_height,
                    actual: roots.height,
                });
            }
            if self.header_hash(expected_height) != Some(block::Hash::from(header.as_ref())) {
                return Err(AuthenticateHeaderRootsError::NonCanonicalHeader {
                    height: expected_height,
                });
            }
            expected_height = expected_height
                .next()
                .map_err(|_| AuthenticateHeaderRootsError::HeightOverflow)?;
        }

        let witness_height = roots
            .last()
            .expect("non-empty root delivery has a terminal witness")
            .height;
        if witness_height > current.completed_checkpoint_height {
            return Err(
                AuthenticateHeaderRootsError::WitnessAboveCompletedCheckpoint {
                    witness_height,
                    completed_checkpoint_height: current.completed_checkpoint_height,
                },
            );
        }
        if headers.len() == 1 {
            if let Some(witness) = current.header_witness {
                return Err(AuthenticateHeaderRootsError::WitnessAlreadyPresent { witness });
            }
            let finalized_body_tip = self.tip().map_or(Height::MIN, |(height, _)| height);
            if current.authenticated_height <= finalized_body_tip {
                return Err(AuthenticateHeaderRootsError::WitnessNotNeeded {
                    authenticated_height: current.authenticated_height,
                    finalized_body_tip,
                });
            }
        }

        let verified = verify_supplied_roots_from_parts(
            &self.network(),
            frontier.history_tree.clone(),
            headers
                .iter()
                .zip(roots)
                .map(|(header, roots)| (header.as_ref(), roots)),
        )
        .map_err(
            |(height, source)| AuthenticateHeaderRootsError::Verification { height, source },
        )?;

        if headers.len() == 1 {
            let state = self
                .write_recovered_header_witness(frontier, verified)?
                .state(completed_checkpoint);
            let witness = state
                .header_witness
                .expect("successful witness recovery writes the verified witness");
            return Ok(AuthenticatedHeaderRoots {
                state,
                update: HeaderRootAuthUpdate::WitnessRecovered { witness },
            });
        }

        let confirmed_height = roots[roots.len() - 2].height;
        let authenticated = start..=confirmed_height;
        let state = self
            .write_verified_header_commitment_roots(verified)?
            .state(completed_checkpoint);
        Ok(AuthenticatedHeaderRoots {
            state,
            update: HeaderRootAuthUpdate::Advanced { authenticated },
        })
    }

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
            header_witness: None,
            history_tree,
        };
        batch.set_header_root_auth_frontier(self, &frontier);
        Ok(())
    }

    /// Returns the contiguous stored prefix of `range`.
    ///
    /// The read stops at the first missing height. Every returned row is
    /// authoritative: authenticated by the header-root lane or derived from a
    /// committed body (the index stores no per-row provenance).
    pub fn commitment_roots_by_height_range(
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

    /// Persists raw roots for test fixtures outside a larger transaction.
    ///
    /// Production callers must use [`Self::write_verified_header_commitment_roots`].
    #[cfg(any(test, feature = "proptest-impl"))]
    pub fn insert_zakura_header_commitment_roots(
        &self,
        roots: impl IntoIterator<Item = BlockCommitmentRoots>,
    ) -> Result<(), rocksdb::Error> {
        let mut batch = DiskWriteBatch::new();
        for roots in roots {
            batch.insert_unauthenticated_commitment_roots_for_test(self, &roots);
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

    pub(crate) fn advance_header_root_auth_frontier_from_body(
        &mut self,
        db: &ZakuraDb,
        confirmed_height: Height,
        confirmed_hash: block::Hash,
        history_tree: &HistoryTree,
    ) -> Result<(), HeaderRootAuthFrontierError> {
        let stored_frontier = db.try_header_root_auth_frontier()?;
        if stored_frontier.is_some_and(|frontier| {
            frontier.confirmed_height > confirmed_height
                && db.header_hash(confirmed_height) == Some(confirmed_hash)
        }) {
            return Ok(());
        }

        validate_history_tree_height(db, confirmed_height, history_tree)?;
        self.set_header_root_auth_frontier(
            db,
            &HeaderRootAuthFrontier {
                confirmed_height,
                confirmed_hash,
                header_witness: None,
                history_tree: history_tree.clone(),
            },
        );
        Ok(())
    }

    #[cfg(test)]
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
                header_witness: None,
                history_tree: history_tree.clone(),
            },
        );
        Ok(())
    }

    pub(crate) fn rebase_header_root_auth_frontier_for_rollback(
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
                header_witness: None,
                history_tree: history_tree.clone(),
            },
        );
        Ok(())
    }

    pub(crate) fn delete_header_root_auth_frontier(&mut self, db: &ZakuraDb) {
        let writer = db
            .header_root_auth_frontier_cf()
            .with_batch_for_writing(self)
            .zs_delete(&RawBytes::new_raw_bytes(AUTH_FRONTIER_KEY.to_vec()));
        let _ = writer;
    }

    pub(crate) fn truncate_all_commitment_roots(&mut self, db: &ZakuraDb) {
        let writer = db
            .commitment_roots_cf()
            .with_batch_for_writing(self)
            .zs_delete_range(&Height::MIN, &Height::MAX)
            .zs_delete(&Height::MAX);
        let _ = writer;
    }

    /// Inserts a raw fixture row unless a committed body owns the height.
    ///
    /// Test-only: production writes go through the sealed verified-root boundary
    /// ([`ZakuraDb::write_verified_header_commitment_roots`]) or the body-commit path.
    #[cfg(any(test, feature = "proptest-impl"))]
    pub(super) fn insert_unauthenticated_commitment_roots_for_test(
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

    /// Deletes the header-supplied row superseded by a committed body.
    ///
    /// Only body commit calls this, and the same batch writes the authoritative
    /// body-derived replacement row, so the index never loses contiguity.
    pub(super) fn delete_superseded_header_commitment_root(
        &mut self,
        db: &ZakuraDb,
        height: Height,
    ) {
        let _ = db
            .commitment_roots_cf()
            .with_batch_for_writing(self)
            .zs_delete(&height);
    }

    /// Deletes the inclusive header-supplied suffix displaced by a header reorganization.
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
        service::finalized_state::{
            HighestCompletedCheckpointTracker, WriteDisk, STATE_COLUMN_FAMILIES_IN_CODE,
        },
        Config,
    };
    use zakura_chain::{
        block::Block,
        parallel::commitment_aux_verify::{
            normalize_unauthenticated_commitment_fields, verify_supplied_roots_from_parts,
        },
        parameters::{testnet, Network, NetworkUpgrade},
        serialization::ZcashDeserializeInto,
        work::difficulty::ParameterDifficulty,
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
        roots.get(&height).map_or_else(
            || zakura_chain::sapling::tree::NoteCommitmentTree::default().root(),
            |root| zakura_chain::sapling::tree::Root::try_from(**root).expect("test root is valid"),
        )
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
        let block_header_by_height = db.db.cf_handle("block_header_by_height").unwrap();
        let header_hash_by_height = db.db.cf_handle("zakura_header_hash_by_height").unwrap();
        let mut batch = DiskWriteBatch::new();
        let genesis_hash = db.network().genesis_hash();
        let genesis = mainnet_block_at(0);
        batch.zs_insert(&hash_by_height, Height::MIN, genesis_hash);
        batch.zs_insert(&height_by_hash, genesis_hash, Height::MIN);
        batch.zs_insert(&block_header_by_height, Height::MIN, &genesis.header);
        batch.zs_insert(&hash_by_height, frontier_height, base_hash);
        batch.zs_insert(&height_by_hash, base_hash, frontier_height);
        for (height, hash) in confirmed {
            batch.zs_insert(&header_hash_by_height, *height, *hash);
        }
        batch.set_header_root_auth_frontier(
            db,
            &HeaderRootAuthFrontier {
                confirmed_height: frontier_height,
                confirmed_hash: base_hash,
                header_witness: None,
                history_tree: HistoryTree::default(),
            },
        );
        db.write_batch(batch).expect("frontier fixture writes");
    }

    fn verified_activation_root() -> (
        VerifiedHeaderCommitmentRoots,
        BlockCommitmentRoots,
        block::Hash,
        (Height, block::Hash),
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
        let successor_header = (
            successor_roots.height,
            block::Hash::from(successor.header.as_ref()),
        );
        (verified, roots, hash, successor_header)
    }

    fn two_block_checkpoint_fixture_with_config(
        config: &Config,
    ) -> (ZakuraDb, Arc<Block>, Arc<Block>, HeaderRootAuthState) {
        let genesis = mainnet_block_at(0);
        let block1 = mainnet_block_at(1);
        let block2 = mainnet_block_at(2);
        let network = testnet::Parameters::build()
            .with_network_name("RootAuthTest")
            .expect("test network name is valid")
            .with_genesis_hash(genesis.hash())
            .expect("genesis hash is valid")
            .with_target_difficulty_limit(Network::Mainnet.target_difficulty_limit())
            .expect("difficulty limit is valid")
            .with_activation_heights(testnet::ConfiguredActivationHeights {
                heartwood: Some(2),
                canopy: Some(2),
                ..Default::default()
            })
            .expect("activation heights are valid")
            .clear_funding_streams()
            .with_checkpoints(testnet::ConfiguredCheckpoints::HeightsAndHashes(vec![
                (Height::MIN, genesis.hash()),
                (Height(2), block2.hash()),
            ]))
            .expect("linked checkpoints are valid")
            .to_network()
            .expect("test network is valid");
        let db = ZakuraDb::new(
            config,
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &network,
            true,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            false,
        )
        .expect("ephemeral database opens");
        let hash_by_height = db.db.cf_handle("hash_by_height").unwrap();
        let height_by_hash = db.db.cf_handle("height_by_hash").unwrap();
        let block_header_by_height = db.db.cf_handle("block_header_by_height").unwrap();
        let mut batch = DiskWriteBatch::new();
        batch.zs_insert(&hash_by_height, Height::MIN, genesis.hash());
        batch.zs_insert(&height_by_hash, genesis.hash(), Height::MIN);
        batch.zs_insert(&block_header_by_height, Height::MIN, &genesis.header);
        db.write_batch(batch).expect("genesis rows write");
        let mut batch = DiskWriteBatch::new();
        batch
            .rebase_header_root_auth_frontier(
                &db,
                Height::MIN,
                genesis.hash(),
                &HistoryTree::default(),
            )
            .expect("genesis frontier is coherent");
        db.write_batch(batch).expect("genesis fixture writes");

        let header_hash_by_height = db.db.cf_handle("zakura_header_hash_by_height").unwrap();
        let header_height_by_hash = db.db.cf_handle("zakura_header_height_by_hash").unwrap();
        let header_by_height = db.db.cf_handle("zakura_header_by_height").unwrap();
        let mut batch = DiskWriteBatch::new();
        let linked = [(Height(1), block1.clone()), (Height(2), block2.clone())];
        for (height, block) in &linked {
            let hash = block.hash();
            batch.zs_insert(&header_hash_by_height, height, hash);
            batch.zs_insert(&header_height_by_hash, hash, height);
            batch.zs_insert(&header_by_height, height, &block.header);
        }
        db.write_batch(batch).expect("linked headers write");
        let (tracker, _receiver) = HighestCompletedCheckpointTracker::open(&db);
        let state = db
            .validate_header_root_auth_state()
            .expect("linked fixture validates")
            .expect("frontier exists")
            .state(
                tracker
                    .current()
                    .expect("linked fixture completes the genesis checkpoint"),
            );
        (db, block1, block2, state)
    }

    fn two_block_checkpoint_fixture() -> (ZakuraDb, Arc<Block>, Arc<Block>, HeaderRootAuthState) {
        two_block_checkpoint_fixture_with_config(&Config::ephemeral())
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
        let mut malformed = vec![0; LEGACY_FRONTIER_FIXED_BYTES + 1];
        malformed[0] = LEGACY_FRONTIER_FORMAT_VERSION;
        malformed[37] = 1;
        malformed[LEGACY_FRONTIER_FIXED_BYTES] = 0xff;
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
    fn legacy_frontier_decodes_without_a_header_witness() {
        let db = ephemeral_mainnet_db();
        let confirmed_height = Height(1);
        seed_frontier_and_headers(&db, confirmed_height, &[]);
        let confirmed_hash = db
            .header_hash(confirmed_height)
            .expect("fixture stores the confirmed hash");
        let mut legacy = vec![LEGACY_FRONTIER_FORMAT_VERSION];
        legacy.extend_from_slice(&confirmed_height.0.to_le_bytes());
        legacy.extend_from_slice(&confirmed_hash.0);
        legacy.push(0);
        let mut batch = DiskWriteBatch::new();
        let _ = db
            .header_root_auth_frontier_cf()
            .with_batch_for_writing(&mut batch)
            .zs_insert(
                &RawBytes::new_raw_bytes(Vec::new()),
                &RawBytes::new_raw_bytes(legacy),
            );
        db.write_batch(batch).expect("legacy frontier row writes");

        let frontier = db
            .try_header_root_auth_frontier()
            .expect("legacy frontier decodes")
            .expect("legacy frontier exists");
        assert_eq!(frontier.confirmed_height(), confirmed_height);
        assert_eq!(frontier.header_witness, None);
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
        let (verified, roots, hash, successor_header) = verified_activation_root();
        let base = roots
            .height
            .previous()
            .expect("activation has a predecessor");
        seed_frontier_and_headers(&db, base, &[(roots.height, hash), successor_header]);

        db.write_verified_header_commitment_roots(verified)
            .expect("canonical verified prefix promotes");

        assert_eq!(
            db.commitment_roots(roots.height),
            Some(normalize_unauthenticated_commitment_fields(
                &db.network(),
                roots.clone()
            )),
            "pre-NU5 auth_data_root is cleared on promotion"
        );
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
        let (verified, roots, hash, successor_header) = verified_activation_root();
        let base = roots
            .height
            .previous()
            .expect("activation has a predecessor");
        seed_frontier_and_headers(&db, base, &[(roots.height, hash), successor_header]);
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
        let (verified, roots, hash, successor_header) = verified_activation_root();
        let base = Height(roots.height.0 - 2);
        seed_frontier_and_headers(&db, base, &[(roots.height, hash), successor_header]);

        assert!(matches!(
            db.write_verified_header_commitment_roots(verified),
            Err(HeaderRootAuthFrontierError::NonContiguousAppend { .. })
        ));
    }

    #[test]
    fn verified_promotion_rejects_noncanonical_hash() {
        let db = ephemeral_mainnet_db();
        let (verified, roots, _hash, successor_header) = verified_activation_root();
        let base = roots
            .height
            .previous()
            .expect("activation has a predecessor");
        seed_frontier_and_headers(
            &db,
            base,
            &[(roots.height, block::Hash([0x99; 32])), successor_header],
        );

        assert!(matches!(
            db.write_verified_header_commitment_roots(verified),
            Err(HeaderRootAuthFrontierError::CanonicalHashMismatch { height })
                if height == roots.height
        ));
    }

    #[test]
    fn verified_promotion_rejects_noncanonical_header_witness() {
        let db = ephemeral_mainnet_db();
        let (verified, roots, hash, (successor_height, _successor_hash)) =
            verified_activation_root();
        let base = roots
            .height
            .previous()
            .expect("activation has a predecessor");
        seed_frontier_and_headers(
            &db,
            base,
            &[
                (roots.height, hash),
                (successor_height, block::Hash([0x99; 32])),
            ],
        );

        assert!(matches!(
            db.write_verified_header_commitment_roots(verified),
            Err(HeaderRootAuthFrontierError::HeaderWitnessHashMismatch { height })
                if height == successor_height
        ));
    }

    #[test]
    fn stale_header_witness_degrades_to_missing_on_frontier_load() {
        let db = ephemeral_mainnet_db();
        let (verified, roots, hash, (successor_height, successor_hash)) =
            verified_activation_root();
        let base = roots
            .height
            .previous()
            .expect("activation has a predecessor");
        seed_frontier_and_headers(
            &db,
            base,
            &[(roots.height, hash), (successor_height, successor_hash)],
        );
        db.write_verified_header_commitment_roots(verified)
            .expect("canonical verified prefix promotes");

        let successor = mainnet_block_at(successor_height.0);
        let mut replacement_header = *successor.header;
        replacement_header.nonce.0[0] ^= 1;
        let replacement_header = Arc::new(replacement_header);
        let replacement_hash = block::Hash::from(replacement_header.as_ref());
        let header_hash_by_height = db.db.cf_handle("zakura_header_hash_by_height").unwrap();
        let header_height_by_hash = db.db.cf_handle("zakura_header_height_by_hash").unwrap();
        let header_by_height = db.db.cf_handle("zakura_header_by_height").unwrap();
        let mut batch = DiskWriteBatch::new();
        batch.zs_delete(&header_height_by_hash, successor_hash);
        batch.zs_insert(&header_hash_by_height, successor_height, replacement_hash);
        batch.zs_insert(&header_height_by_hash, replacement_hash, successor_height);
        batch.zs_insert(&header_by_height, successor_height, &replacement_header);
        db.write_batch(batch)
            .expect("canonical successor replacement writes");

        let frontier = db
            .validate_header_root_auth_state()
            .expect("a stale optional witness does not invalidate the confirmed frontier")
            .expect("the confirmed frontier remains available");
        assert_eq!(frontier.confirmed_height(), roots.height);
        assert_eq!(frontier.confirmed_hash(), hash);
        assert_eq!(frontier.header_witness, None);
        assert_eq!(
            db.commitment_roots(roots.height),
            Some(normalize_unauthenticated_commitment_fields(
                &db.network(),
                roots
            ))
        );
        assert_eq!(
            db.header_witness_auth_data_root(successor_height, replacement_hash),
            None,
            "a stale witness is never used for the replacement header"
        );
    }

    #[test]
    fn verified_insert_preserves_body_derived_row() {
        let db = ephemeral_mainnet_db();
        let (_verified, mut peer_roots, hash, _successor_header) = verified_activation_root();
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
    fn startup_repair_reconstructs_checkpoint_after_deleting_covered_headers() {
        let cache = tempfile::tempdir().expect("temporary cache directory is created");
        let config = Config {
            cache_dir: cache.path().to_owned(),
            ephemeral: false,
            repair_zakura_header_store_on_startup: true,
            ..Config::default()
        };
        let (mut db, _block1, _block2, state) = two_block_checkpoint_fixture_with_config(&config);
        assert_eq!(state.completed_checkpoint_height, Height(2));

        let header_hash_by_height = db.db.cf_handle("zakura_header_hash_by_height").unwrap();
        let header_by_height = db.db.cf_handle("zakura_header_by_height").unwrap();
        let mut batch = DiskWriteBatch::new();
        batch.zs_delete(&header_hash_by_height, Height(1));
        batch.zs_delete(&header_by_height, Height(1));
        db.write_batch(batch)
            .expect("interior checkpoint corruption writes");
        db.update_format_version_on_disk(&state_database_format_version_in_code())
            .expect("fixture format version writes");

        let network = db.network();
        db.shutdown(true);
        drop(db);
        let db = ZakuraDb::new(
            &config,
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &network,
            true,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            false,
        )
        .expect("database reopens after repairing the checkpoint bracket");

        let (tracker, _receiver) = HighestCompletedCheckpointTracker::open(&db);
        let repaired = db
            .validate_header_root_auth_state()
            .expect("repaired authenticated state validates")
            .expect("repaired frontier exists")
            .state(
                tracker
                    .current()
                    .expect("repaired database retains the genesis checkpoint"),
            );
        assert_eq!(repaired.authenticated_height, Height::MIN);
        assert_eq!(
            repaired.completed_checkpoint_height,
            Height::MIN,
            "repair must not preserve a checkpoint whose header bracket was deleted"
        );
        assert_eq!(
            repaired.completed_checkpoint_hash,
            db.network().genesis_hash()
        );
    }

    #[test]
    fn steady_state_frontier_load_does_not_audit_authenticated_root_prefix() {
        let db = ephemeral_mainnet_db();
        let (verified, roots, hash, successor_header) = verified_activation_root();
        let base = roots
            .height
            .previous()
            .expect("activation has a predecessor");
        seed_frontier_and_headers(&db, base, &[(roots.height, hash), successor_header]);
        let expected_frontier = db
            .write_verified_header_commitment_roots(verified)
            .expect("canonical verified prefix promotes");

        let mut batch = DiskWriteBatch::new();
        let _ = db
            .commitment_roots_cf()
            .with_batch_for_writing(&mut batch)
            .zs_delete(&roots.height);
        db.write_batch(batch).expect("root gap fixture writes");

        assert_eq!(
            db.load_header_root_auth_frontier()
                .expect("steady-state load trusts the atomically written root prefix")
                .expect("frontier exists")
                .confirmed_height(),
            roots.height
        );
        let frontier = db
            .load_header_root_auth_frontier()
            .expect("steady-state frontier loads")
            .expect("frontier exists");
        let empty_verified = verify_supplied_roots_from_parts(
            &db.network(),
            frontier.history_tree,
            std::iter::empty(),
        )
        .expect("empty delivery preserves the verified frontier");
        assert_eq!(
            db.write_verified_header_commitment_roots(empty_verified)
                .expect("steady-state write does not audit the historical root prefix"),
            expected_frontier
        );
        assert!(matches!(
            db.validate_header_root_auth_state(),
            Err(HeaderRootAuthFrontierError::MissingRoot { height }) if height == roots.height
        ));
    }

    #[test]
    fn authenticates_one_lag_and_rejects_invalid_stale_and_noncanonical_without_writes() {
        let (db, block, successor, current) = two_block_checkpoint_fixture();
        let start = Height(1);
        let roots = roots_from_block(&block);
        let successor_roots = roots_from_block(&successor);
        let headers = vec![block.header.clone(), successor.header.clone()];
        let supplied = vec![roots.clone(), successor_roots.clone()];
        let completed = HighestCompletedCheckpoint {
            height: current.completed_checkpoint_height,
            hash: current.completed_checkpoint_hash,
        };

        let mut invalid = supplied.clone();
        invalid[0].sapling_root = mainnet_sapling_root_at(
            NetworkUpgrade::Heartwood
                .activation_height(&Network::Mainnet)
                .expect("Mainnet Heartwood height exists")
                .0,
        );
        let invalid_error = db
            .authenticate_header_roots(
                completed,
                current,
                current.authenticated_hash,
                start,
                &headers,
                &invalid,
            )
            .expect_err("invalid supplied roots are rejected");
        assert!(matches!(
            &invalid_error,
            AuthenticateHeaderRootsError::Verification { .. }
        ));
        assert_eq!(
            invalid_error.outcome(),
            AuthenticateHeaderRootsOutcome::Invalid
        );
        assert_eq!(db.commitment_roots(start), None);
        assert_eq!(
            db.validate_header_root_auth_state()
                .expect("failed authentication leaves coherent state")
                .expect("frontier exists")
                .state(completed),
            current
        );

        let mut stale = current;
        stale.authenticated_hash = block::Hash([0x99; 32]);
        let stale_error = db
            .authenticate_header_roots(
                completed,
                stale,
                current.authenticated_hash,
                start,
                &headers,
                &supplied,
            )
            .expect_err("stale state is rejected");
        assert!(matches!(
            &stale_error,
            AuthenticateHeaderRootsError::StaleState { .. }
        ));
        assert_eq!(stale_error.outcome(), AuthenticateHeaderRootsOutcome::Stale);

        let successor_height = Height(2);
        let mut wrong_successor = *successor.header;
        *wrong_successor.nonce = [0x77; 32];
        let wrong_headers = vec![block.header.clone(), Arc::new(wrong_successor)];
        let noncanonical_error = db
            .authenticate_header_roots(
                completed,
                current,
                current.authenticated_hash,
                start,
                &wrong_headers,
                &supplied,
            )
            .expect_err("noncanonical request is rejected");
        assert!(matches!(
            &noncanonical_error,
            AuthenticateHeaderRootsError::NonCanonicalHeader { height }
                if *height == successor_height
        ));
        assert_eq!(
            noncanonical_error.outcome(),
            AuthenticateHeaderRootsOutcome::Stale
        );
        assert_eq!(db.commitment_roots(start), None);

        let result = db
            .authenticate_header_roots(
                completed,
                current,
                current.authenticated_hash,
                start,
                &headers,
                &supplied,
            )
            .expect("valid one-lag delivery authenticates");
        assert_eq!(
            result.update,
            HeaderRootAuthUpdate::Advanced {
                authenticated: start..=start
            }
        );
        assert_eq!(result.state.authenticated_height, start);
        assert_eq!(
            db.commitment_roots(start),
            Some(normalize_unauthenticated_commitment_fields(
                &db.network(),
                roots
            )),
            "pre-NU5 auth_data_root is cleared on promotion"
        );
        assert_eq!(
            db.commitment_roots(successor_height),
            None,
            "the successor's unconfirmed note roots stay out of the authoritative index"
        );
        assert_eq!(
            db.header_witness_auth_data_root(successor_height, successor.hash()),
            Some(successor_roots.auth_data_root),
            "only the canonical header witness's auth-data root is retained separately"
        );
        let frontier = db
            .try_header_root_auth_frontier()
            .expect("header-witness frontier decodes")
            .expect("header-witness frontier exists");
        assert_eq!(
            frontier_bytes(&frontier).raw_bytes().len(),
            FRONTIER_PREFIX_BYTES + 1 + WITNESS_FIXED_BYTES + 1,
            "an empty history tree frontier stores only the compact header witness"
        );

        let encoded = frontier_bytes(&frontier).raw_bytes().to_vec();
        let mut truncated = encoded.clone();
        truncated.pop();
        assert!(matches!(
            decode_frontier(&db, &RawBytes::new_raw_bytes(truncated)),
            Err(HeaderRootAuthFrontierError::InvalidEncoding)
        ));

        let witness_offset = FRONTIER_PREFIX_BYTES + 1;
        let mut wrong_height = encoded.clone();
        wrong_height[witness_offset..witness_offset + 4]
            .copy_from_slice(&Height(3).0.to_le_bytes());
        assert_eq!(
            decode_frontier(&db, &RawBytes::new_raw_bytes(wrong_height))
                .expect("an invalid optional witness does not poison the frontier")
                .header_witness,
            None
        );

        let mut wrong_hash = encoded;
        wrong_hash[witness_offset + 4] ^= 1;
        assert_eq!(
            decode_frontier(&db, &RawBytes::new_raw_bytes(wrong_hash))
                .expect("a stale optional witness does not poison the frontier")
                .header_witness,
            None
        );
    }

    #[test]
    fn marker_upgrade_preserves_affected_frontier_then_runtime_recovers_witness() {
        use crate::constants::{state_database_format_version_in_code, STATE_DATABASE_KIND};
        use crate::service::finalized_state::disk_format::upgrade::{
            no_migration::NoMigration, DiskFormatUpgrade,
        };
        use semver::Version;

        let cache = tempfile::tempdir().expect("temporary cache directory is created");
        let config = Config {
            cache_dir: cache.path().to_owned(),
            ephemeral: false,
            ..Config::default()
        };
        let (mut db, block, successor, current) = two_block_checkpoint_fixture_with_config(&config);
        let completed = HighestCompletedCheckpoint {
            height: current.completed_checkpoint_height,
            hash: current.completed_checkpoint_hash,
        };
        let roots = roots_from_block(&block);
        let successor_roots = roots_from_block(&successor);
        db.authenticate_header_roots(
            completed,
            current,
            current.authenticated_hash,
            Height(1),
            &[block.header.clone(), successor.header.clone()],
            &[roots.clone(), successor_roots.clone()],
        )
        .expect("fixture roots authenticate");

        let mut affected_batch = DiskWriteBatch::new();
        affected_batch
            .rebase_header_root_auth_frontier(&db, Height(1), block.hash(), &HistoryTree::default())
            .expect("affected frontier is coherent");
        db.write_batch(affected_batch)
            .expect("affected frontier without its witness writes");
        assert!(db.commitment_roots(Height(1)).is_some());
        assert_eq!(
            db.header_witness_auth_data_root(Height(2), successor.hash()),
            None,
            "the 28.0.2-style frontier has discarded its terminal witness"
        );

        db.update_format_version_on_disk(&Version::new(28, 0, 2))
            .expect("the affected database version marker writes");
        let (_cancel_sender, cancel_receiver) = crossbeam_channel::bounded(1);
        let upgrade = NoMigration::new(
            "retain terminal header witnesses in the authenticated frontier",
            Version::new(28, 0, 3),
        );
        DiskFormatUpgrade::run(&upgrade, Height::MIN, &db, &cancel_receiver)
            .expect("the marker-only upgrade completes");
        db.update_format_version_on_disk(&state_database_format_version_in_code())
            .expect("startup marks the marker-only upgrade complete");
        assert_eq!(
            db.format_version_on_disk()
                .expect("upgraded database version is readable"),
            Some(state_database_format_version_in_code())
        );

        let affected = db
            .validate_header_root_auth_state()
            .expect("affected state validates")
            .expect("body tip has a frontier");
        assert_eq!(affected.confirmed_height(), Height(1));
        assert_eq!(
            db.commitment_roots(Height(1)),
            Some(normalize_unauthenticated_commitment_fields(
                &db.network(),
                roots.clone()
            ))
        );
        assert_eq!(db.header_hash(Height(1)), Some(block.hash()));
        assert_eq!(db.header_hash(Height(2)), Some(successor.hash()));

        let affected_state = affected.state(completed);
        let frontier_before = db
            .header_root_auth_frontier_cf()
            .zs_get(&RawBytes::new_raw_bytes(AUTH_FRONTIER_KEY.to_vec()))
            .expect("affected frontier row exists");
        let mut stale_state = affected_state;
        stale_state.authenticated_hash.0[0] ^= 1;
        assert!(matches!(
            db.authenticate_header_roots(
                completed,
                stale_state,
                affected_state.authenticated_hash,
                Height(2),
                std::slice::from_ref(&successor.header),
                std::slice::from_ref(&successor_roots),
            ),
            Err(AuthenticateHeaderRootsError::StaleState { .. })
        ));
        let mut noncanonical_header = *successor.header;
        noncanonical_header.nonce[0] ^= 1;
        assert!(matches!(
            db.authenticate_header_roots(
                completed,
                affected_state,
                affected_state.authenticated_hash,
                Height(2),
                &[Arc::new(noncanonical_header)],
                std::slice::from_ref(&successor_roots),
            ),
            Err(AuthenticateHeaderRootsError::NonCanonicalHeader { height: Height(2) })
        ));
        assert_eq!(
            db.header_root_auth_frontier_cf()
                .zs_get(&RawBytes::new_raw_bytes(AUTH_FRONTIER_KEY.to_vec())),
            Some(frontier_before),
            "stale and noncanonical recovery attempts do not rewrite the frontier"
        );

        let recovered = db
            .authenticate_header_roots(
                completed,
                affected_state,
                affected_state.authenticated_hash,
                Height(2),
                std::slice::from_ref(&successor.header),
                std::slice::from_ref(&successor_roots),
            )
            .expect("one-record authentication recovers the missing witness");

        assert_eq!(
            db.commitment_roots(Height(1)),
            Some(normalize_unauthenticated_commitment_fields(
                &db.network(),
                roots
            ))
        );
        assert_eq!(db.commitment_roots(Height(2)), None);
        assert_eq!(
            recovered.update,
            HeaderRootAuthUpdate::WitnessRecovered {
                witness: HeaderWitnessState {
                    height: Height(2),
                    hash: successor.hash(),
                }
            }
        );
        assert_eq!(
            db.header_witness_auth_data_root(Height(2), successor.hash()),
            Some(successor_roots.auth_data_root)
        );
        assert!(matches!(
            db.authenticate_header_roots(
                completed,
                recovered.state,
                recovered.state.authenticated_hash,
                Height(2),
                std::slice::from_ref(&successor.header),
                &[successor_roots],
            ),
            Err(AuthenticateHeaderRootsError::WitnessAlreadyPresent { .. })
        ));

        let network = db.network();
        db.shutdown(true);
        drop(db);
        let reopened = ZakuraDb::new(
            &config,
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &network,
            true,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            false,
        )
        .expect("the recovered database reopens");
        assert!(reopened.commitment_roots(Height(1)).is_some());
        assert_eq!(reopened.commitment_roots(Height(2)), None);
        assert_eq!(
            reopened.header_witness_auth_data_root(Height(2), successor.hash()),
            Some(roots_from_block(&successor).auth_data_root)
        );
    }

    #[test]
    fn terminal_witness_recovery_is_suppressed_after_body_catches_frontier() {
        let (db, block, successor, current) = two_block_checkpoint_fixture();
        let completed = HighestCompletedCheckpoint {
            height: current.completed_checkpoint_height,
            hash: current.completed_checkpoint_hash,
        };
        let roots = roots_from_block(&block);
        let successor_roots = roots_from_block(&successor);
        db.authenticate_header_roots(
            completed,
            current,
            current.authenticated_hash,
            Height(1),
            &[block.header.clone(), successor.header.clone()],
            &[roots, successor_roots.clone()],
        )
        .expect("fixture roots authenticate");
        let mut affected_batch = DiskWriteBatch::new();
        affected_batch
            .rebase_header_root_auth_frontier(&db, Height(1), block.hash(), &HistoryTree::default())
            .expect("affected frontier is coherent");
        let hash_by_height = db.db.cf_handle("hash_by_height").unwrap();
        let height_by_hash = db.db.cf_handle("height_by_hash").unwrap();
        let block_header_by_height = db.db.cf_handle("block_header_by_height").unwrap();
        affected_batch.zs_insert(&hash_by_height, Height(1), block.hash());
        affected_batch.zs_insert(&height_by_hash, block.hash(), Height(1));
        affected_batch.zs_insert(&block_header_by_height, Height(1), &block.header);
        db.write_batch(affected_batch)
            .expect("caught-up body fixture writes");

        let affected = db
            .load_header_root_auth_frontier()
            .expect("affected frontier loads")
            .expect("affected frontier exists")
            .state(completed);
        assert!(matches!(
            db.authenticate_header_roots(
                completed,
                affected,
                affected.authenticated_hash,
                Height(2),
                std::slice::from_ref(&successor.header),
                &[successor_roots],
            ),
            Err(AuthenticateHeaderRootsError::WitnessNotNeeded {
                authenticated_height: Height(1),
                finalized_body_tip: Height(1),
            })
        ));
        assert_eq!(
            db.header_witness_auth_data_root(Height(2), successor.hash()),
            None
        );
        assert!(db.commitment_roots(Height(1)).is_some());
        assert_eq!(db.commitment_roots(Height(2)), None);
    }

    #[test]
    fn compact_header_witness_survives_database_reopen() {
        let cache = tempfile::tempdir().expect("temporary cache directory is created");
        let config = Config {
            cache_dir: cache.path().to_owned(),
            ephemeral: false,
            ..Config::default()
        };
        let (mut db, block, successor, current) = two_block_checkpoint_fixture_with_config(&config);
        let completed = HighestCompletedCheckpoint {
            height: current.completed_checkpoint_height,
            hash: current.completed_checkpoint_hash,
        };
        let successor_roots = roots_from_block(&successor);
        db.authenticate_header_roots(
            completed,
            current,
            current.authenticated_hash,
            Height(1),
            &[block.header.clone(), successor.header.clone()],
            &[roots_from_block(&block), successor_roots.clone()],
        )
        .expect("header witness writes before restart");

        let network = db.network();
        db.shutdown(true);
        drop(db);
        let db = ZakuraDb::new(
            &config,
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &network,
            true,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            false,
        )
        .expect("database with a compact header witness reopens");

        assert_eq!(db.commitment_roots(Height(2)), None);
        assert_eq!(
            db.header_witness_auth_data_root(Height(2), successor.hash()),
            Some(successor_roots.auth_data_root)
        );
    }

    #[test]
    fn simulated_authentication_storage_failure_advances_no_state() {
        let cache = tempfile::tempdir().expect("temporary cache directory is created");
        let config = Config {
            cache_dir: cache.path().to_owned(),
            ephemeral: false,
            ..Config::default()
        };
        let (mut writable_db, block, successor, current) =
            two_block_checkpoint_fixture_with_config(&config);
        let network = writable_db.network();
        writable_db.shutdown(true);
        drop(writable_db);
        let db = ZakuraDb::new(
            &config,
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &network,
            true,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            true,
        )
        .expect("fixture reopens read-only");
        let completed = HighestCompletedCheckpoint {
            height: current.completed_checkpoint_height,
            hash: current.completed_checkpoint_hash,
        };
        let (sender, mut receiver) = tokio::sync::watch::channel(Some(current));
        let _ = receiver.borrow_and_update();

        let result = db.authenticate_header_roots(
            completed,
            current,
            current.authenticated_hash,
            Height(1),
            &[block.header.clone(), successor.header.clone()],
            &[roots_from_block(&block), roots_from_block(&successor)],
        );
        if let Ok(success) = &result {
            let _ = sender.send(Some(success.state));
        }

        let error = result.expect_err("simulated storage failure rejects the write");
        assert!(matches!(
            error,
            AuthenticateHeaderRootsError::Frontier(HeaderRootAuthFrontierError::Storage(_))
        ));
        assert_eq!(error.outcome(), AuthenticateHeaderRootsOutcome::Local);
        assert_eq!(db.commitment_roots(Height(1)), None);
        assert_eq!(
            db.validate_header_root_auth_state()
                .expect("failed write leaves coherent durable state")
                .expect("frontier remains present")
                .state(completed),
            current
        );
        assert!(!receiver.has_changed().expect("watch sender remains open"));
    }

    #[test]
    fn authenticated_roots_survive_restart_before_watch_publication() {
        let cache = tempfile::tempdir().expect("temporary cache directory is created");
        let config = Config {
            cache_dir: cache.path().to_owned(),
            ephemeral: false,
            ..Config::default()
        };
        let (mut db, block, successor, current) = two_block_checkpoint_fixture_with_config(&config);
        let network = db.network();
        let completed = HighestCompletedCheckpoint {
            height: current.completed_checkpoint_height,
            hash: current.completed_checkpoint_hash,
        };
        let roots = roots_from_block(&block);

        let authenticated = db
            .authenticate_header_roots(
                completed,
                current,
                current.authenticated_hash,
                Height(1),
                &[block.header.clone(), successor.header.clone()],
                &[roots.clone(), roots_from_block(&successor)],
            )
            .expect("valid roots authenticate durably");

        // Model the process losing its in-memory state after the atomic
        // roots/frontier batch commits but before the write worker publishes
        // the returned state.
        db.shutdown(true);
        drop(db);

        let reopened = ZakuraDb::new(
            &config,
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &network,
            true,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            false,
        )
        .expect("database reopens after root authentication");
        let (tracker, _receiver) = HighestCompletedCheckpointTracker::open(&reopened);
        let restored = reopened
            .validate_header_root_auth_state()
            .expect("authenticated state remains coherent")
            .expect("authenticated frontier remains present")
            .state(
                tracker
                    .current()
                    .expect("checkpoint progress reconstructs from durable headers"),
            );

        assert_eq!(restored, authenticated.state);
        assert_eq!(
            reopened.commitment_roots(Height(1)),
            Some(normalize_unauthenticated_commitment_fields(
                &reopened.network(),
                roots
            ))
        );
    }

    #[test]
    fn body_frontier_advance_moves_forward_and_preserves_newer_authentication() {
        let (db, block, _successor, current) = two_block_checkpoint_fixture();
        let mut batch = DiskWriteBatch::new();
        batch
            .advance_header_root_auth_frontier_from_body(
                &db,
                Height(1),
                block.hash(),
                &HistoryTree::default(),
            )
            .expect("pre-Heartwood body frontier is coherent");
        db.write_batch(batch).expect("body frontier batch writes");

        let advanced = db
            .load_header_root_auth_frontier()
            .expect("advanced frontier decodes")
            .expect("advanced frontier exists");
        assert_eq!(advanced.confirmed_height, Height(1));
        assert_eq!(advanced.confirmed_hash, block.hash());

        let mut lagging_body_batch = DiskWriteBatch::new();
        lagging_body_batch
            .advance_header_root_auth_frontier_from_body(
                &db,
                current.authenticated_height,
                current.authenticated_hash,
                &HistoryTree::default(),
            )
            .expect("lagging body commit preserves the newer frontier");
        db.write_batch(lagging_body_batch)
            .expect("no-op body frontier batch writes");

        assert_eq!(
            db.load_header_root_auth_frontier()
                .expect("preserved frontier decodes")
                .expect("preserved frontier exists"),
            advanced
        );
    }

    #[test]
    fn body_frontier_advance_rejects_inconsistent_history_tree_without_writing() {
        let (db, _block, successor, _current) = two_block_checkpoint_fixture();
        let before = db
            .load_header_root_auth_frontier()
            .expect("initial frontier decodes")
            .expect("initial frontier exists");
        let mut batch = DiskWriteBatch::new();

        assert!(matches!(
            batch.advance_header_root_auth_frontier_from_body(
                &db,
                Height(2),
                successor.hash(),
                &HistoryTree::default(),
            ),
            Err(HeaderRootAuthFrontierError::EmptyHistoryTree {
                confirmed_height: Height(2),
                ..
            })
        ));
        db.write_batch(batch)
            .expect("rejected frontier leaves an empty batch");
        assert_eq!(
            db.load_header_root_auth_frontier()
                .expect("unchanged frontier decodes")
                .expect("unchanged frontier exists"),
            before
        );
    }

    #[test]
    fn checkpoint_progress_does_not_stale_an_unchanged_authentication_frontier() {
        let (db, block, successor, current) = two_block_checkpoint_fixture();
        let completed = HighestCompletedCheckpoint {
            height: current.completed_checkpoint_height,
            hash: current.completed_checkpoint_hash,
        };
        let mut expected = current;
        expected.completed_checkpoint_height = Height::MIN;
        expected.completed_checkpoint_hash = db.network().genesis_hash();
        let roots = roots_from_block(&block);
        let result = db
            .authenticate_header_roots(
                completed,
                expected,
                current.authenticated_hash,
                Height(1),
                &[block.header.clone(), successor.header.clone()],
                &[roots.clone(), roots_from_block(&successor)],
            )
            .expect("new checkpoint coverage preserves the same authentication base");

        assert_eq!(
            result.update,
            HeaderRootAuthUpdate::Advanced {
                authenticated: Height(1)..=Height(1)
            }
        );
        assert_eq!(result.state.authenticated_height, Height(1));
        assert_eq!(
            db.commitment_roots(Height(1)),
            Some(normalize_unauthenticated_commitment_fields(
                &db.network(),
                roots
            )),
            "pre-NU5 auth_data_root is cleared on promotion"
        );
    }

    #[test]
    fn out_of_order_prefetched_range_changes_no_durable_state() {
        let (db, block, successor, current) = two_block_checkpoint_fixture();
        let completed = HighestCompletedCheckpoint {
            height: current.completed_checkpoint_height,
            hash: current.completed_checkpoint_hash,
        };
        let mut first = roots_from_block(&block);
        first.height = Height(2);
        let mut second = roots_from_block(&successor);
        second.height = Height(3);

        let error = db
            .authenticate_header_roots(
                completed,
                current,
                current.authenticated_hash,
                Height(2),
                &[block.header.clone(), successor.header.clone()],
                &[first, second],
            )
            .expect_err("a prefetched range cannot leapfrog the durable frontier");

        assert!(matches!(
            error,
            AuthenticateHeaderRootsError::StartMismatch {
                expected: Height(1),
                actual: Height(2),
            }
        ));
        assert_eq!(db.commitment_roots(Height(1)), None);
        assert_eq!(db.commitment_roots(Height(2)), None);
        assert_eq!(
            db.validate_header_root_auth_state()
                .expect("rejected prefetch leaves coherent state")
                .expect("frontier exists")
                .state(completed),
            current
        );
    }

    #[test]
    fn header_witness_must_be_at_or_below_completed_checkpoint() {
        let (db, block, successor, current) = two_block_checkpoint_fixture();
        let header_hash_by_height = db.db.cf_handle("zakura_header_hash_by_height").unwrap();
        let header_height_by_hash = db.db.cf_handle("zakura_header_height_by_hash").unwrap();
        let header_by_height = db.db.cf_handle("zakura_header_by_height").unwrap();
        let mut witness = *successor.header;
        witness.previous_block_hash = successor.hash();
        let witness = Arc::new(witness);
        let witness_hash = block::Hash::from(witness.as_ref());
        let mut batch = DiskWriteBatch::new();
        batch.zs_insert(&header_hash_by_height, Height(3), witness_hash);
        batch.zs_insert(&header_height_by_hash, witness_hash, Height(3));
        batch.zs_insert(&header_by_height, Height(3), &witness);
        db.write_batch(batch)
            .expect("uncheckpointed witness writes");

        let first = roots_from_block(&block);
        let second = roots_from_block(&successor);
        let mut third = second.clone();
        third.height = Height(3);
        let completed = HighestCompletedCheckpoint {
            height: current.completed_checkpoint_height,
            hash: current.completed_checkpoint_hash,
        };
        let result = db.authenticate_header_roots(
            completed,
            current,
            current.authenticated_hash,
            Height(1),
            &[block.header.clone(), successor.header.clone(), witness],
            &[first, second, third],
        );
        assert!(matches!(
            result,
            Err(
                AuthenticateHeaderRootsError::WitnessAboveCompletedCheckpoint {
                    witness_height: Height(3),
                    completed_checkpoint_height: Height(2),
                }
            )
        ));
        assert_eq!(db.commitment_roots(Height(1)), None);
    }

    #[test]
    fn local_frontier_failures_are_not_peer_invalid() {
        let error =
            AuthenticateHeaderRootsError::Frontier(HeaderRootAuthFrontierError::MissingRoot {
                height: Height(1),
            });
        assert_eq!(error.outcome(), AuthenticateHeaderRootsOutcome::Local);
    }
}
