//! Verified-commitment-trees fast-sync experiment state.
//!
//! This module holds the embedded-final-frontier plumbing and run counters for the
//! verified-commitment-trees fast-sync. On networks with an embedded final frontier,
//! the default source is exact hash-scoped `tree_aux` data. `checkpoint_sync = false` or
//! `consensus.vct_fast_sync = false` selects legacy recompute.

use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex,
};

use thiserror::Error;
#[cfg(test)]
use zakura_chain::parallel::tree::NoteCommitmentTrees;
use zakura_chain::{
    block::{self, merkle::AuthDataRoot, Header},
    ironwood, orchard,
    parameters::{Network, NetworkUpgrade},
    sapling, sprout,
};
use zakura_header_chain::{AuxAuthentication, AuxDelivery};

/// Positive result proving which exact successor boundary authenticated supplied roots.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum VctAuthenticationProof {
    /// The successful commit did not authenticate an auxiliary delivery.
    NotAuthenticated,
    /// One exact successor history-root commitment accepted the delivery roots.
    Successor {
        /// Delivery whose roots verification folded into the verified history tree.
        delivery_id: zakura_header_chain::EvidenceId,
        /// Header that owns the delivery roots.
        delivery_header_hash: block::Hash,
        /// Exact successor header whose commitment verification checked.
        boundary_hash: block::Hash,
        /// Exact successor authorizing-data root checked with that header.
        boundary_auth_data_root: AuthDataRoot,
    },
}

use super::commitment_aux::{CommitmentRootSource, EmbeddedFrontierSource, FinalFrontiers};
use crate::error::VctCommitFailure;

/// A selected successor header and auxiliary delivery that authenticate VCT roots.
#[derive(Clone, Debug)]
pub struct VctSuccessorWitness {
    /// The successor header that commits to the current block's VCT roots.
    pub(crate) header: Arc<Header>,
    /// The successor header's height.
    pub(crate) height: block::Height,
    /// The successor header's hash, used for prevalidation deduplication.
    pub(crate) hash: block::Hash,
    /// The successor block's precomputed ZIP-244 auth-data root, if available.
    pub(crate) auth_data_root: Option<AuthDataRoot>,
    /// Exact auxiliary delivery that supplied the successor auth-data root.
    pub(crate) delivery: Option<AuxDelivery>,
}

impl VctSuccessorWitness {
    /// Builds a successor witness from a header and its precomputed authorizing-data root.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn from_header(
        header: Arc<Header>,
        height: block::Height,
        auth_data_root: AuthDataRoot,
    ) -> Self {
        let hash = block::Hash::from(&header);

        Self {
            header,
            height,
            hash,
            auth_data_root: Some(auth_data_root),
            delivery: None,
        }
    }

    /// Builds a successor witness from an exact auxiliary delivery.
    ///
    /// The method returns `None` when the delivery identifies another header or height.
    pub(crate) fn from_delivery(
        header: Arc<Header>,
        height: block::Height,
        delivery: AuxDelivery,
    ) -> Option<Self> {
        let aux = delivery.tree_aux?;
        if delivery.header_hash != header.hash() || aux.height != height {
            return None;
        }
        let hash = block::Hash::from(&header);

        Some(Self {
            header,
            height,
            hash,
            auth_data_root: Some(aux.auth_data_root),
            delivery: Some(delivery),
        })
    }
}

/// One selected VCT auxiliary delivery and its optional successor authentication boundary.
#[derive(Clone, Debug)]
pub(crate) struct VctAuxiliaryWindow {
    /// Committed engine snapshot under which state selected both deliveries.
    pub(crate) engine_snapshot: zakura_header_chain::EngineSnapshot,
    /// Retained header that owns the delivery under verification.
    ///
    /// The committer already holds this header in the block it will commit. The sweep
    /// verifies ahead of block bodies, so the window carries the header for the sweep.
    pub(crate) delivery_header: Arc<Header>,
    /// Auxiliary delivery whose roots verification folds for the current block.
    pub(crate) delivery: AuxDelivery,
    /// Height of the retained direct successor, even when state lacks its auxiliary delivery.
    pub(crate) successor_height: Option<block::Height>,
    /// Exact direct-successor witness used for one-header-later authentication.
    pub(crate) successor: Option<VctSuccessorWitness>,
}

/// Attribution result for a failed VCT auxiliary verification.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum VctAuxiliaryFailureAttribution {
    /// Verification proved only the current roots invalid.
    CurrentDelivery,
    /// Verification proved only the successor auth-data root invalid.
    SuccessorDelivery,
    /// Either of two unauthenticated deliveries may have caused the boundary mismatch.
    AmbiguousDeliveries,
    /// Verification cannot safely attribute failure to a mutable metadata delivery.
    NoDelivery,
}

impl VctAuxiliaryFailureAttribution {
    /// Returns a stable metrics label for this attribution result.
    pub(crate) fn attribution_label(self) -> &'static str {
        match self {
            Self::CurrentDelivery => "current",
            Self::SuccessorDelivery => "successor",
            Self::AmbiguousDeliveries => "ambiguous",
            Self::NoDelivery => "none",
        }
    }

    /// Returns whether the writer must dispute both deliveries.
    pub(crate) fn requires_dispute(self) -> bool {
        self == Self::AmbiguousDeliveries
    }

    /// Returns the lowest delivery height that repair must replace.
    pub(crate) fn repair_height(
        self,
        current_delivery_height: block::Height,
        successor_delivery_height: Option<block::Height>,
    ) -> Option<block::Height> {
        match self {
            Self::CurrentDelivery | Self::AmbiguousDeliveries => Some(current_delivery_height),
            Self::SuccessorDelivery => successor_delivery_height,
            Self::NoDelivery => None,
        }
    }
}

impl VctAuxiliaryWindow {
    /// Returns the delivery roots when the delivery identifies the requested block.
    pub(crate) fn delivery_roots(
        &self,
        height: block::Height,
        hash: block::Hash,
    ) -> Option<(
        sapling::tree::Root,
        orchard::tree::Root,
        ironwood::tree::Root,
    )> {
        let auxiliary_data = self.delivery.tree_aux?;
        (self.delivery.header_hash == hash && auxiliary_data.height == height).then_some((
            auxiliary_data.sapling_root,
            auxiliary_data.orchard_root,
            auxiliary_data.ironwood_root,
        ))
    }

    /// Attributes a verification failure without weakening authenticated evidence.
    pub(crate) fn attribute_failure(
        &self,
        failure: VctCommitFailure,
    ) -> VctAuxiliaryFailureAttribution {
        attribute_vct_auxiliary_failure(
            self.delivery,
            self.successor
                .as_ref()
                .and_then(|successor| successor.delivery),
            failure,
        )
    }
}

fn attribute_vct_auxiliary_failure(
    current_delivery: AuxDelivery,
    successor_delivery: Option<AuxDelivery>,
    failure: VctCommitFailure,
) -> VctAuxiliaryFailureAttribution {
    let current_untrusted = matches!(
        current_delivery.authentication,
        AuxAuthentication::Unauthenticated | AuxAuthentication::Disputed { .. }
    );
    if failure == VctCommitFailure::CurrentRoots {
        return if current_untrusted {
            VctAuxiliaryFailureAttribution::CurrentDelivery
        } else {
            VctAuxiliaryFailureAttribution::NoDelivery
        };
    }

    let Some(successor_delivery) = successor_delivery else {
        return if current_untrusted {
            VctAuxiliaryFailureAttribution::CurrentDelivery
        } else {
            VctAuxiliaryFailureAttribution::NoDelivery
        };
    };
    let successor_untrusted = matches!(
        successor_delivery.authentication,
        AuxAuthentication::Unauthenticated | AuxAuthentication::Disputed { .. }
    );

    match (current_untrusted, successor_untrusted) {
        (true, true) => VctAuxiliaryFailureAttribution::AmbiguousDeliveries,
        (true, false) => VctAuxiliaryFailureAttribution::CurrentDelivery,
        (false, true) => VctAuxiliaryFailureAttribution::SuccessorDelivery,
        (false, false) => VctAuxiliaryFailureAttribution::NoDelivery,
    }
}

/// Embedded verified final note-commitment frontiers for Mainnet.
const MAINNET_FINAL_FRONTIERS: &[u8] = include_bytes!("vct/mainnet-frontier.bin");

/// Errors validating serialized VCT final-frontier bytes.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum FinalFrontiersValidationError {
    /// The bytes could not be parsed as [`FinalFrontiers`].
    #[error("invalid VCT final frontier bytes: {error}")]
    InvalidBytes {
        /// The parser error message.
        error: String,
    },

    /// The serialized frontier height does not match the expected checkpoint handoff height.
    #[error("embedded VCT final frontier height must match the network's max checkpoint height")]
    HeightMismatch {
        /// Height encoded in the serialized frontier.
        actual: block::Height,
        /// Expected checkpoint handoff height.
        expected: block::Height,
    },
}

/// State for the verified-commitment-trees fast-sync.
/// (`docs/design/verified-commitment-trees.md`).
///
/// A checkpoint-trusting sync (`checkpoint_sync = true`) uses exact header `tree_aux` data by
/// default on networks with embedded final frontiers; `checkpoint_sync = false` or
/// `vct_fast_sync = false` opts out to the legacy per-block recompute (no VCT state).
#[derive(Debug)]
pub(crate) struct VctState {
    /// `true` when the VCT fast-sync is enabled.
    enabled: bool,
    /// Embedded final-frontier authority, plus test-only root fixtures.
    source: Box<dyn CommitmentRootSource>,
    /// Whether roots from this VCT state must be confirmed against a stored successor header
    /// before they are committed.
    requires_verified_successor: bool,
    /// Count of blocks that took the VCT fast-sync, for the run summary.
    vct_count: AtomicU64,
    /// Count of VCT fast-sync blocks whose own commitment check was skipped because the
    /// previous block's look-ahead already validated it (the dedup). Lets tests
    /// assert the dedup actually engages, so it can't be silently regressed.
    prevalidated_count: AtomicU64,
}

/// Commitment-root source that the committer uses, resolved from the
/// configuration signals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceMode {
    /// Legacy committer that recomputes every tree without VCT state.
    Legacy,
    /// Consume exact hash-scoped header auxiliary data.
    HeaderAuxiliary,
}

/// Resolve the source mode without reading embedded-frontier files.
/// The selector chooses the exact header-auxiliary path for checkpoint-trusting sync on a network
/// with an embedded handoff frontier. Disabling checkpoint sync selects legacy recomputation.
/// Disabling VCT fast sync also selects legacy recomputation.
/// A network without an embedded handoff frontier uses legacy recomputation.
/// Storage mode does not affect this selection.
fn select_source_mode(
    checkpoint_sync: bool,
    vct_fast_sync: bool,
    has_embedded_frontiers: bool,
) -> SourceMode {
    if !checkpoint_sync || !vct_fast_sync || !has_embedded_frontiers {
        SourceMode::Legacy
    } else {
        SourceMode::HeaderAuxiliary
    }
}

impl VctState {
    /// Builds committer state from `checkpoint_sync` and `vct_fast_sync`.
    /// `checkpoint_sync` mirrors `consensus.checkpoint_sync`.
    /// Mainnet checkpoint sync defaults to the peer `tree_aux` source.
    /// Disabled checkpoint sync returns `None` for legacy per-block recomputation.
    /// Disabled VCT fast sync also returns `None`.
    /// Networks without an embedded handoff frontier also return `None`.
    pub(super) fn from_config(
        checkpoint_sync: bool,
        vct_fast_sync: bool,
        network: &Network,
    ) -> Option<Arc<Self>> {
        // Parse the embedded handoff frontier once.
        // Networks such as Testnet return `None`.
        // Source selection uses only the frontier's presence.
        // The peer arm reuses the parsed value.
        let embedded = embedded_final_frontiers(network);

        match select_source_mode(checkpoint_sync, vct_fast_sync, embedded.is_some()) {
            // Default: hash-scoped `tree_aux` deliveries from the header-chain store,
            // authenticated against this embedded handoff frontier.
            SourceMode::HeaderAuxiliary => {
                let parsed = embedded?;
                tracing::info!(
                    handoff_height = parsed.height.0,
                    "VCT: exact header auxiliary source enabled by default"
                );
                let source = EmbeddedFrontierSource::new(parsed);
                Some(Arc::new(VctState {
                    enabled: true,
                    source: Box::new(source),
                    requires_verified_successor: true,
                    vct_count: AtomicU64::new(0),
                    prevalidated_count: AtomicU64::new(0),
                }))
            }

            // The legacy committer performs full per-block recomputation.
            // This mode allocates no VCT state.
            SourceMode::Legacy => None,
        }
    }

    /// The supplied roots for `height`, when vct mode has a source entry for it
    /// (the signal that this block takes the VCT fast-sync).
    #[cfg(test)]
    pub(super) fn vct_roots_at_height(
        &self,
        height: block::Height,
    ) -> Option<(
        sapling::tree::Root,
        orchard::tree::Root,
        ironwood::tree::Root,
    )> {
        if !self.enabled {
            return None;
        }

        if height > self.source.vct_last_checkpoint_height() {
            return None;
        }

        self.source.vct_root(height)
    }

    /// Return `true` when the VCT path needs a stored successor header before it can safely persist
    /// this block's supplied roots.
    ///
    /// Only untrusted peer-supplied roots at or above Heartwood require a successor header.
    /// The checkpoint handoff verifies embedded final frontiers against this block's roots before
    /// writing the real tip tree state. Trusted local fixtures can commit their tip root during the
    /// in-arrears check.
    pub(super) fn vct_root_needs_successor(
        &self,
        height: block::Height,
        network: &Network,
        has_exact_roots: bool,
    ) -> bool {
        self.enabled
            && has_exact_roots
            && height <= self.source.vct_last_checkpoint_height()
            && self.requires_verified_successor
            && self.source.final_frontiers().height != height
            && Some(height) >= NetworkUpgrade::Heartwood.activation_height(network)
    }

    /// `true` when exact header-owned roots are required for `height`.
    pub(super) fn accepts_exact_roots_at(&self, height: block::Height) -> bool {
        self.enabled && height <= self.source.vct_last_checkpoint_height()
    }

    /// The checkpoint handoff height: the boundary below which the fast path skips
    /// per-height note-commitment trees.
    pub(super) fn vct_sync_last_checkpoint_height(&self) -> block::Height {
        self.source.vct_last_checkpoint_height()
    }

    /// The verified `(sapling, orchard, sprout, ironwood)` frontiers to write as the tip
    /// treestate, when `height` is the checkpoint handoff height.
    pub(super) fn final_frontiers_for_last_checkpoint(
        &self,
        height: block::Height,
    ) -> Option<(
        Arc<sapling::tree::NoteCommitmentTree>,
        Arc<orchard::tree::NoteCommitmentTree>,
        Arc<sprout::tree::NoteCommitmentTree>,
        Arc<ironwood::tree::NoteCommitmentTree>,
    )> {
        let frontiers = self.source.final_frontiers();
        (frontiers.height == height).then(|| {
            (
                frontiers.sapling.clone(),
                frontiers.orchard.clone(),
                frontiers.sprout.clone(),
                frontiers.ironwood.clone(),
            )
        })
    }

    /// Record that a block took the fast (skip-recompute) path.
    pub(super) fn record_fast_block(&self) {
        self.vct_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a fast block whose own commitment check was skipped by the dedup.
    pub(super) fn record_prevalidated(&self) {
        self.prevalidated_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Number of blocks that took the fast path so far.
    pub(super) fn vct_count(&self) -> u64 {
        self.vct_count.load(Ordering::Relaxed)
    }

    /// Number of VCT fast-sync blocks whose own commitment check the dedup skipped.
    #[cfg(test)]
    pub(super) fn prevalidated_count(&self) -> u64 {
        self.prevalidated_count.load(Ordering::Relaxed)
    }

    /// Test-only: build fast-mode state from an arbitrary commitment-root source
    /// (e.g. a payload produced from a database), so the producer→consumer round-trip
    /// can be exercised without networking.
    #[cfg(test)]
    pub(super) fn test_with_source(
        source: Box<dyn CommitmentRootSource>,
        requires_verified_successor: bool,
    ) -> Arc<Self> {
        Arc::new(VctState {
            enabled: true,
            source,
            requires_verified_successor,
            vct_count: AtomicU64::new(0),
            prevalidated_count: AtomicU64::new(0),
        })
    }
}

/// Commit-time vct state carried by [`super::FinalizedState`]: the configured
/// root source plus the commit-loop dedup and below-last-checkpoint state its
/// fast path depends on, grouped so their invariants live next to the data they guard.
#[derive(Clone, Debug)]
pub(crate) struct VctCommitState {
    /// The root source (peer/fixture/capture mode), or `None` for any of:
    /// - checkpoint sync is disabled
    /// - vct fast sync is disabled
    /// - legacy Zebra checkpoint sync
    source: Option<Arc<VctState>>,

    /// `(height, hash, auth_data_root)` of the next block already validated by
    /// the previous fast commit's look-ahead, so its own commitment check can
    /// be skipped.
    ///
    /// The auth-data root is `None` below NU5, where it is not an input to the
    /// block commitment. At NU5 and later it stays paired with the header hash,
    /// so a same-header body with different authorizing data cannot reuse the
    /// earlier prevalidation.
    ///
    /// This cache is shared across [`super::FinalizedState`] clones. The
    /// production node has one finalized writer, but the public state type is
    /// cloneable and its clone contract requires mutable commit safety state to
    /// remain coherent across clones.
    prevalidated_next: Arc<Mutex<Option<(block::Height, block::Hash, Option<AuthDataRoot>)>>>,

    /// `true` while a vct sync is in-progress below the last checkpoint height.
    /// During this time, we do not reconstruct per-height note-commitment trees.
    /// As a result, the frontier is unknown.
    ///
    /// This flag is shared across [`super::FinalizedState`] clones so a clone
    /// cannot miss that another clone has frozen the frontier and then
    /// incorrectly fall back to legacy recomputation.
    is_vct_sync_below_last_checkpoint: Arc<AtomicBool>,
}

impl VctCommitState {
    /// Builds the commit state from a resolved `source` and an
    /// `is_vct_sync_below_last_checkpoint` flag re-derived from durable state on open.
    pub(super) fn new(
        source: Option<Arc<VctState>>,
        is_vct_sync_below_last_checkpoint: bool,
    ) -> Self {
        VctCommitState {
            source,
            prevalidated_next: Arc::new(Mutex::new(None)),
            is_vct_sync_below_last_checkpoint: Arc::new(AtomicBool::new(
                is_vct_sync_below_last_checkpoint,
            )),
        }
    }

    /// The configured root source, or `None` for legacy recompute.
    pub(super) fn source(&self) -> Option<&Arc<VctState>> {
        self.source.as_ref()
    }

    /// `true` while the note-commitment frontier is below the last checkpoint height.
    pub(super) fn is_below_last_checkpoint(&self) -> bool {
        self.is_vct_sync_below_last_checkpoint
            .load(Ordering::Acquire)
    }

    /// The cached successor prevalidation, if any.
    pub(super) fn prevalidated_next(
        &self,
    ) -> Option<(block::Height, block::Hash, Option<AuthDataRoot>)> {
        *self
            .prevalidated_next
            .lock()
            .expect("VCT prevalidation lock is not poisoned because commit panics are fatal")
    }

    /// Caches the next header as already validated by this fast commit's look-ahead.
    pub(super) fn mark_prevalidated(
        &self,
        height: block::Height,
        hash: block::Hash,
        auth_data_root: Option<AuthDataRoot>,
    ) {
        *self
            .prevalidated_next
            .lock()
            .expect("VCT prevalidation lock is not poisoned because commit panics are fatal") =
            Some((height, hash, auth_data_root));
    }

    /// Clears any cached successor prevalidation.
    pub(super) fn clear_prevalidated_next(&self) {
        *self
            .prevalidated_next
            .lock()
            .expect("VCT prevalidation lock is not poisoned because commit panics are fatal") =
            None;
    }

    /// Test-only: overwrites the cached successor prevalidation, so tests can
    /// install a stale or forged entry to exercise the dedup's guard checks.
    #[cfg(test)]
    pub(super) fn set_prevalidated_next(
        &self,
        next: Option<(block::Height, block::Hash, Option<AuthDataRoot>)>,
    ) {
        *self
            .prevalidated_next
            .lock()
            .expect("VCT prevalidation lock is not poisoned because commit panics are fatal") =
            next;
    }

    /// Starts a VCT sync below the last checkpoint height: below the last checkpoint height,
    /// the frontier is unknown as we are not reconstructing the trees every height.
    pub(super) fn start_vct_sync_below_last_checkpoint(&self) {
        self.is_vct_sync_below_last_checkpoint
            .store(true, Ordering::Release);
    }

    /// Stops a VCT sync at the last checkpoint height: the last checkpoint wrote the
    /// real final frontier as the tip treestate.
    pub(super) fn stop_vct_sync_at_last_checkpoint(&self) {
        self.is_vct_sync_below_last_checkpoint
            .store(false, Ordering::Release);
    }

    /// Test-only: installs an arbitrary [`CommitmentRootSource`] as fast-mode
    /// state, so the producer→consumer round-trip can be exercised in-process.
    /// `requires_verified_successor` marks an untrusted source that must defer
    /// tip roots until their successor is buffered.
    #[cfg(test)]
    pub(super) fn install_test_source(
        &mut self,
        source: Box<dyn CommitmentRootSource>,
        requires_verified_successor: bool,
    ) {
        self.source = Some(VctState::test_with_source(
            source,
            requires_verified_successor,
        ));
    }
}

/// Fast-path (vct) outputs for the block being committed, passed as one
/// parameter from the committer through
/// `super::ZakuraDb::write_block` to `super::ZakuraDb::prepare_trees_batch`.
///
/// The fields are independent: a checkpoint-handoff block sets `sync_below`
/// but leaves `anchor_roots` `None` (it writes the real frontier via the
/// legacy path instead), while a non-handoff fast block sets both.
#[derive(Clone, Copy, Debug, Default)]
pub struct VctWriteData {
    /// When `Some`, skip per-height tree writes and fold these roots into the anchor set.
    pub anchor_roots: Option<(
        sapling::tree::Root,
        orchard::tree::Root,
        ironwood::tree::Root,
    )>,
    /// When `Some(height)`, mark the database as vct-synced below `height`.
    pub sync_below: Option<block::Height>,
}

/// The verified final frontiers embedded for `network`, if supported.
///
/// Mainnet uses the constant embedded in the binary. Regtest has no fixed checkpoint —
/// its checkpoint list is derived at runtime from the mined chain — so there is no
/// committed frontier to embed; for deterministic e2e/integration testing of the fast
/// path on Regtest, the frontier is instead loaded from the file named by the
/// `VCT_REGTEST_FRONTIER` env var. This is scoped to **Regtest only** and validated
/// against the configured Regtest checkpoint height, so Mainnet always uses the
/// embedded constant and never reads the env. Other testnets have no frontier.
pub(super) fn embedded_final_frontiers(network: &Network) -> Option<FinalFrontiers> {
    match network {
        Network::Mainnet => {
            Some(embedded_mainnet_final_frontiers().unwrap_or_else(|error| panic!("{error}")))
        }
        Network::Testnet(params) if params.is_regtest() => {
            let path = std::env::var_os("VCT_REGTEST_FRONTIER")?;
            Some(load_frontier_file(
                path.as_ref(),
                network.checkpoint_list().max_height(),
            ))
        }
        Network::Testnet(_) => None,
    }
}

/// Returns the verified Sapling, Orchard, and Ironwood leaf counts at `last_checkpoint`, when the
/// configured network has a matching embedded final frontier.
pub(crate) fn embedded_last_checkpoint_leaf_counts(
    network: &Network,
    last_checkpoint: block::Height,
) -> Option<(u64, u64, u64)> {
    let frontiers = embedded_final_frontiers(network)?;
    (frontiers.height == last_checkpoint).then(|| {
        (
            frontiers.sapling.count(),
            frontiers.orchard.count(),
            frontiers.ironwood.count(),
        )
    })
}

/// Parse the Mainnet frontier without panicking, for fallible startup validation.
pub(super) fn embedded_mainnet_final_frontiers(
) -> Result<FinalFrontiers, FinalFrontiersValidationError> {
    parse_final_frontiers_bytes(
        MAINNET_FINAL_FRONTIERS,
        Network::Mainnet.checkpoint_list().max_height(),
    )
}

/// Load and validate a final-frontier fixture file (the Regtest path; see
/// [`embedded_final_frontiers`]). Separated from the env read so it is unit-testable
/// without mutating process environment variables.
fn load_frontier_file(path: &std::ffi::OsStr, expected_height: block::Height) -> FinalFrontiers {
    let bytes =
        std::fs::read(path).expect("VCT_REGTEST_FRONTIER must name a readable final-frontier file");
    parse_embedded_final_frontiers(&bytes, expected_height)
}

/// Parse embedded final frontiers and verify they match the checkpoint list.
fn parse_embedded_final_frontiers(bytes: &[u8], expected_height: block::Height) -> FinalFrontiers {
    parse_final_frontiers_bytes(bytes, expected_height).unwrap_or_else(|error| panic!("{error}"))
}

fn parse_final_frontiers_bytes(
    bytes: &[u8],
    expected_height: block::Height,
) -> Result<FinalFrontiers, FinalFrontiersValidationError> {
    let parsed = FinalFrontiers::from_bytes(bytes).map_err(|error| {
        FinalFrontiersValidationError::InvalidBytes {
            error: error.to_string(),
        }
    })?;

    if parsed.height != expected_height {
        return Err(FinalFrontiersValidationError::HeightMismatch {
            actual: parsed.height,
            expected: expected_height,
        });
    }

    Ok(parsed)
}

/// Validate serialized VCT final-frontier bytes against an expected final frontier height.
pub fn validate_final_frontiers_bytes(
    bytes: &[u8],
    expected_height: block::Height,
) -> Result<(), FinalFrontiersValidationError> {
    parse_final_frontiers_bytes(bytes, expected_height).map(|_| ())
}

/// Test/developer helper for producing embedded final-frontier bytes from a
/// legacy-computed final frontier.
#[cfg(test)]
fn final_frontiers_bytes(height: block::Height, trees: &NoteCommitmentTrees) -> Vec<u8> {
    FinalFrontiers {
        height,
        sapling: trees.sapling.clone(),
        orchard: trees.orchard.clone(),
        sprout: trees.sprout.clone(),
        ironwood: trees.ironwood.clone(),
    }
    .to_bytes()
}

#[cfg(test)]
mod tests {
    use std::{io::Write, num::NonZeroU64};

    use serde::Deserialize;
    use sha2::{Digest, Sha256};

    use super::*;

    fn aux_delivery(byte: u8, authentication: AuxAuthentication) -> AuxDelivery {
        AuxDelivery {
            delivery_id: zakura_header_chain::EvidenceId::from_digest([byte; 32]),
            header_hash: block::Hash([byte; 32]),
            source: zakura_header_chain::SourceId::from_digest([byte; 32]),
            owner: zakura_header_chain::BodyWorkOwner {
                authority: zakura_header_chain::BodyWorkAuthority {
                    header: zakura_header_chain::HeaderWorkAuthority {
                        header_generation: zakura_header_chain::HeaderGeneration::new(2),
                        branch: zakura_header_chain::BranchId::new(
                            block::Hash([4; 32]),
                            block::Hash([5; 32]),
                        ),
                    },
                    verified_generation: zakura_header_chain::VerifiedGeneration::new(3),
                },
                session_id: 6,
                request_id: NonZeroU64::new(7).expect("seven is nonzero"),
            }
            .into(),
            body_size: zakura_header_chain::BodySizeHint::Unknown,
            tree_aux: None,
            authentication,
        }
    }

    #[test]
    fn vct_boundary_failure_attribution_never_weakens_authenticated_evidence() {
        let unauthenticated = aux_delivery(1, AuxAuthentication::Unauthenticated);
        let authenticated = aux_delivery(
            2,
            AuxAuthentication::Authenticated {
                evidence: zakura_header_chain::EvidenceId::from_digest([3; 32]),
                boundary_hash: block::Hash([4; 32]),
            },
        );

        assert_eq!(
            attribute_vct_auxiliary_failure(
                unauthenticated,
                Some(authenticated),
                VctCommitFailure::SuccessorBoundary,
            ),
            VctAuxiliaryFailureAttribution::CurrentDelivery,
        );
        assert_eq!(
            attribute_vct_auxiliary_failure(
                authenticated,
                Some(unauthenticated),
                VctCommitFailure::SuccessorBoundary,
            ),
            VctAuxiliaryFailureAttribution::SuccessorDelivery,
        );
        assert_eq!(
            attribute_vct_auxiliary_failure(
                unauthenticated,
                Some(unauthenticated),
                VctCommitFailure::SuccessorBoundary,
            ),
            VctAuxiliaryFailureAttribution::AmbiguousDeliveries,
        );
        assert_eq!(
            attribute_vct_auxiliary_failure(
                authenticated,
                Some(authenticated),
                VctCommitFailure::SuccessorBoundary,
            ),
            VctAuxiliaryFailureAttribution::NoDelivery,
        );
        assert_eq!(
            attribute_vct_auxiliary_failure(
                unauthenticated,
                Some(authenticated),
                VctCommitFailure::CurrentRoots,
            ),
            VctAuxiliaryFailureAttribution::CurrentDelivery,
        );
    }

    /// The tracked provenance record for the embedded Mainnet VCT state.
    const MAINNET_VCT_MANIFEST: &[u8] = include_bytes!("vct/mainnet-vct-manifest.json");

    /// Embedded Mainnet completed-subtree roots authenticated by the same manifest.
    const MAINNET_SUBTREES: &[u8] = include_bytes!("vct/mainnet-subtrees.bin");

    /// The provenance schema written by the release-state refresh workflow and
    /// checked by `scripts/check-release-state.sh`; this test keeps the record
    /// bound to the embedded checkpoint list, frontier, and subtree bytes on every PR.
    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct MainnetVctManifest {
        schema_version: u32,
        network: String,
        source: String,
        generated_at: String,
        finalized_height: u32,
        finalized_hash: String,
        checkpoints_sha256: String,
        frontier_sha256: String,
        frontier_size: u64,
        subtrees_sha256: String,
        subtrees_size: u64,
        #[serde(default)]
        meta_sha256: Option<String>,
    }

    #[test]
    fn source_mode_precedence() {
        use SourceMode::*;
        // Args are (checkpoint_sync, vct_fast_sync, has_embedded_frontiers).

        // The default: a checkpoint-trusting sync with VCT fast sync on uses header auxiliary data
        // wherever embedded frontiers exist (Mainnet). Storage mode (Archive/Pruned) is not an
        // input, so this covers both Archive and Pruned.
        assert_eq!(select_source_mode(true, true, true), HeaderAuxiliary);
        // `vct_fast_sync = false` keeps checkpoint sync on but forces the legacy recompute,
        // regardless of embedded frontiers.
        assert_eq!(select_source_mode(true, false, true), Legacy);
        assert_eq!(select_source_mode(true, false, false), Legacy);
        // `checkpoint_sync = false` also fully recomputes the trees: legacy, never auxiliary,
        // regardless of the fast-sync knob or embedded frontiers.
        assert_eq!(select_source_mode(false, true, true), Legacy);
        assert_eq!(select_source_mode(false, true, false), Legacy);
        assert_eq!(select_source_mode(false, false, true), Legacy);
        assert_eq!(select_source_mode(false, false, false), Legacy);
        // Networks without embedded frontiers use legacy recomputation during checkpoint sync.
        assert_eq!(select_source_mode(true, true, false), Legacy);
    }

    #[test]
    fn successor_policy_is_vct_state_data() {
        let network = Network::Mainnet;
        let height = NetworkUpgrade::Heartwood
            .activation_height(&network)
            .expect("mainnet has a Heartwood activation height");
        let root_map = || {
            std::iter::once((
                height.0,
                (Default::default(), Default::default(), Default::default()),
            ))
            .collect()
        };
        // The handoff is above the height under test, so the handoff exemption
        // does not mask the successor policy.
        let frontiers = || FinalFrontiers {
            height: (height + 1_000).expect("test height is valid"),
            sapling: Arc::new(Default::default()),
            orchard: Arc::new(Default::default()),
            sprout: Arc::new(Default::default()),
            ironwood: Arc::new(Default::default()),
        };

        let trusted = VctState::test_with_source(
            Box::new(super::super::commitment_aux::FixtureSource::new(
                root_map(),
                frontiers(),
            )),
            false,
        );
        assert!(
            !trusted.vct_root_needs_successor(height, &network, true),
            "trusted fixture roots can commit without a stored successor header"
        );

        let untrusted = VctState::test_with_source(
            Box::new(super::super::commitment_aux::FixtureSource::new(
                root_map(),
                frontiers(),
            )),
            true,
        );
        assert!(
            untrusted.vct_root_needs_successor(height, &network, true),
            "untrusted roots defer until a stored successor header verifies them"
        );
    }

    #[test]
    fn vct_root_is_bounded_by_handoff_height() {
        let handoff = block::Height(10);
        let after_handoff = (handoff + 1).expect("test height is valid");
        let roots = std::collections::HashMap::from([
            (
                handoff.0,
                (Default::default(), Default::default(), Default::default()),
            ),
            (
                after_handoff.0,
                (Default::default(), Default::default(), Default::default()),
            ),
        ]);
        let frontiers = FinalFrontiers {
            height: handoff,
            sapling: Arc::new(sapling::tree::NoteCommitmentTree::default()),
            orchard: Arc::new(orchard::tree::NoteCommitmentTree::default()),
            sprout: Arc::new(sprout::tree::NoteCommitmentTree::default()),
            ironwood: Arc::new(ironwood::tree::NoteCommitmentTree::default()),
        };

        let bounded = VctState::test_with_source(
            Box::new(super::super::commitment_aux::FixtureSource::new(
                roots, frontiers,
            )),
            false,
        );
        assert!(
            bounded.vct_roots_at_height(handoff).is_some(),
            "the handoff root remains fast-path eligible"
        );
        assert!(
            bounded.vct_roots_at_height(after_handoff).is_none(),
            "roots above the handoff are ignored even when the source has them"
        );
    }

    #[test]
    fn cloned_commit_state_shares_frozen_frontier_and_prevalidation() {
        let state = VctCommitState::new(None, false);
        let clone = state.clone();
        let prevalidated = (
            block::Height(7),
            block::Hash([7; 32]),
            Some(AuthDataRoot::from([7; 32])),
        );

        state.start_vct_sync_below_last_checkpoint();
        state.mark_prevalidated(prevalidated.0, prevalidated.1, prevalidated.2);

        assert!(
            clone.is_below_last_checkpoint(),
            "a clone must observe that another clone froze the frontier"
        );
        assert_eq!(
            clone.prevalidated_next(),
            Some(prevalidated),
            "a clone must observe the shared successor prevalidation cache"
        );

        clone.stop_vct_sync_at_last_checkpoint();
        clone.clear_prevalidated_next();

        assert!(
            !state.is_below_last_checkpoint(),
            "unfreezing through one clone must update every clone"
        );
        assert_eq!(
            state.prevalidated_next(),
            None,
            "clearing prevalidation through one clone must update every clone"
        );
    }

    #[test]
    fn embedded_mainnet_final_frontiers_parse() {
        let frontiers = embedded_final_frontiers(&Network::Mainnet)
            .expect("mainnet has embedded final frontiers");
        let provenance: MainnetVctManifest = serde_json::from_slice(MAINNET_VCT_MANIFEST)
            .expect("embedded Mainnet VCT manifest must be strict JSON");
        let finalized_hash: block::Hash = provenance
            .finalized_hash
            .parse()
            .expect("manifest must contain a canonical finalized block hash");

        assert_eq!(
            frontiers.height,
            Network::Mainnet.checkpoint_list().max_height(),
            "embedded frontier is tied to the last mainnet checkpoint"
        );
        assert_eq!(provenance.schema_version, 1);
        assert_eq!(provenance.network, "Mainnet");
        assert!(
            matches!(
                provenance.source.as_str(),
                "legacy-bootstrap" | "release-state-bundle"
            ),
            "manifest must identify a supported source"
        );
        assert!(
            chrono::DateTime::parse_from_rfc3339(&provenance.generated_at).is_ok(),
            "manifest must contain an RFC 3339 generation time"
        );
        assert_eq!(provenance.finalized_height, frontiers.height.0);
        assert_eq!(
            Network::Mainnet.checkpoint_list().hash(frontiers.height),
            Some(finalized_hash),
            "manifest must identify the terminal Mainnet checkpoint"
        );
        assert_eq!(
            provenance.checkpoints_sha256,
            hex::encode(Sha256::digest(
                Network::Mainnet.checkpoint_list().iter_cloned().fold(
                    Vec::new(),
                    |mut bytes, (height, hash)| {
                        writeln!(&mut bytes, "{} {hash}", height.0)
                            .expect("writing to a Vec is infallible");
                        bytes
                    }
                )
            )),
            "manifest must authenticate the complete Mainnet checkpoint file"
        );
        assert_eq!(
            provenance.frontier_size,
            u64::try_from(MAINNET_FINAL_FRONTIERS.len()).expect("frontier length fits in u64")
        );
        assert_eq!(
            provenance.frontier_sha256,
            hex::encode(Sha256::digest(MAINNET_FINAL_FRONTIERS)),
            "manifest must authenticate the embedded Mainnet frontier bytes"
        );
        assert_eq!(
            provenance.subtrees_size,
            u64::try_from(MAINNET_SUBTREES.len()).expect("subtree artifact length fits in u64")
        );
        assert_eq!(
            provenance.subtrees_sha256,
            hex::encode(Sha256::digest(MAINNET_SUBTREES)),
            "manifest must authenticate the embedded Mainnet subtree bytes"
        );
        match provenance.source.as_str() {
            "legacy-bootstrap" => assert!(
                provenance.meta_sha256.is_none(),
                "bootstrap provenance predates release-state bundles"
            ),
            _ => assert_eq!(
                provenance.meta_sha256.as_deref().map(str::len),
                Some(64),
                "bundle provenance must bind its bundle meta digest"
            ),
        }
        let ironwood_active = NetworkUpgrade::Nu6_3
            .activation_height(&Network::Mainnet)
            .is_some_and(|activation| frontiers.height >= activation);
        if !ironwood_active {
            assert_eq!(
                frontiers.ironwood.root(),
                ironwood::tree::NoteCommitmentTree::default().root(),
                "frontiers below the Ironwood activation height carry the empty Ironwood tree"
            );
        }
    }

    #[test]
    fn final_frontiers_capture_helper_serializes_tip_trees() {
        let height = block::Height(3_358_006);
        let trees = NoteCommitmentTrees::default();

        let parsed = FinalFrontiers::from_bytes(&final_frontiers_bytes(height, &trees))
            .expect("captured final frontiers should parse");

        assert_eq!(parsed.height, height, "captured height round-trips");
        assert_eq!(
            parsed.sapling.root(),
            trees.sapling.root(),
            "captured sapling frontier round-trips"
        );
        assert_eq!(
            parsed.orchard.root(),
            trees.orchard.root(),
            "captured orchard frontier round-trips"
        );
        assert_eq!(
            parsed.sprout.root(),
            trees.sprout.root(),
            "captured sprout frontier round-trips"
        );
        assert_eq!(
            parsed.ironwood.root(),
            trees.ironwood.root(),
            "captured ironwood frontier round-trips"
        );
    }

    #[test]
    #[should_panic(expected = "embedded VCT final frontier height must match")]
    fn embedded_final_frontiers_reject_checkpoint_height_mismatch() {
        let frontiers = FinalFrontiers {
            height: block::Height(1),
            sapling: Arc::new(Default::default()),
            orchard: Arc::new(Default::default()),
            sprout: Arc::new(Default::default()),
            ironwood: Arc::new(Default::default()),
        };

        let _ = parse_embedded_final_frontiers(&frontiers.to_bytes(), block::Height(2));
    }

    #[test]
    fn final_frontiers_parser_rejects_short_height() {
        let error =
            FinalFrontiers::from_bytes(&[0, 1, 2]).expect_err("short height should be rejected");

        assert_eq!(
            error.to_string(),
            "missing final frontier height: expected 4 bytes, got 3"
        );
    }

    #[test]
    fn final_frontiers_parser_rejects_missing_tree_length() {
        let bytes = block::Height(1).0.to_le_bytes();

        let error =
            FinalFrontiers::from_bytes(&bytes).expect_err("missing length should be rejected");

        assert_eq!(
            error.to_string(),
            "missing sapling frontier length prefix at byte 4: expected 4 bytes, got 0"
        );
    }

    #[test]
    fn final_frontiers_parser_rejects_truncated_tree_blob() {
        let mut bytes = block::Height(1).0.to_le_bytes().to_vec();
        bytes.extend_from_slice(&3u32.to_le_bytes());
        bytes.extend_from_slice(&[0, 1]);

        let error =
            FinalFrontiers::from_bytes(&bytes).expect_err("truncated blob should be rejected");

        assert_eq!(
            error.to_string(),
            "truncated sapling frontier blob at byte 8: length prefix says 3 bytes, but only 2 remain"
        );
    }

    #[test]
    fn final_frontiers_parser_rejects_trailing_bytes() {
        let bytes = FinalFrontiers {
            height: block::Height(1),
            sapling: Arc::new(Default::default()),
            orchard: Arc::new(Default::default()),
            sprout: Arc::new(Default::default()),
            ironwood: Arc::new(Default::default()),
        }
        .to_bytes()
        .into_iter()
        .chain([0])
        .collect::<Vec<_>>();

        let error =
            FinalFrontiers::from_bytes(&bytes).expect_err("trailing bytes should be rejected");

        assert_eq!(
            error.to_string(),
            format!(
                "unexpected trailing final frontier bytes at byte {}: 1 bytes",
                bytes.len() - 1
            )
        );
    }

    #[test]
    #[should_panic(expected = "invalid VCT final frontier bytes: truncated sapling frontier blob")]
    fn embedded_final_frontiers_reject_malformed_bytes_with_context() {
        let mut bytes = block::Height(1).0.to_le_bytes().to_vec();
        bytes.extend_from_slice(&3u32.to_le_bytes());
        bytes.extend_from_slice(&[0, 1]);

        let _ = parse_embedded_final_frontiers(&bytes, block::Height(1));
    }

    #[test]
    fn embedded_final_frontiers_are_network_specific() {
        assert!(
            embedded_final_frontiers(&Network::new_default_testnet()).is_none(),
            "testnet has no embedded final frontier until VCT fast sync supports it"
        );
    }

    /// The Regtest frontier-file loader (the `VCT_REGTEST_FRONTIER` path) round-trips a
    /// captured frontier and ties it to the expected checkpoint height — exercising the
    /// producer (`to_bytes`) → loader (`load_frontier_file`) seam without env vars.
    #[test]
    fn load_frontier_file_round_trips_a_captured_frontier() {
        let height = block::Height(123);
        let bytes = FinalFrontiers {
            height,
            sapling: Arc::new(Default::default()),
            orchard: Arc::new(Default::default()),
            sprout: Arc::new(Default::default()),
            ironwood: Arc::new(Default::default()),
        }
        .to_bytes();

        let path =
            std::env::temp_dir().join(format!("vct-frontier-load-test-{}.bin", std::process::id()));
        std::fs::write(&path, &bytes).expect("write temp frontier file");

        let loaded = load_frontier_file(path.as_os_str(), height);
        assert_eq!(loaded.height, height, "loaded frontier height matches");
        assert_eq!(
            loaded.sapling.root(),
            sapling::tree::NoteCommitmentTree::default().root(),
            "loaded sapling frontier round-trips"
        );

        let _ = std::fs::remove_file(&path);
    }

    /// A frontier whose height does not match the checkpoint height is rejected, so a
    /// stale/wrong Regtest fixture cannot silently mis-seed the handoff.
    #[test]
    #[should_panic(expected = "embedded VCT final frontier height must match")]
    fn load_frontier_file_rejects_height_mismatch() {
        let bytes = FinalFrontiers {
            height: block::Height(5),
            sapling: Arc::new(Default::default()),
            orchard: Arc::new(Default::default()),
            sprout: Arc::new(Default::default()),
            ironwood: Arc::new(Default::default()),
        }
        .to_bytes();
        let path = std::env::temp_dir().join(format!(
            "vct-frontier-mismatch-test-{}.bin",
            std::process::id()
        ));
        std::fs::write(&path, &bytes).expect("write temp frontier file");

        let _ = load_frontier_file(path.as_os_str(), block::Height(6));
    }
}
