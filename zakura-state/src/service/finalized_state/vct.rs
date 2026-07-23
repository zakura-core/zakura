//! Verified-commitment-trees fast-sync experiment state.
//!
//! This module holds the embedded-final-frontier plumbing and run counters for the
//! verified-commitment-trees fast-sync. On networks with an embedded final frontier,
//! the default source is the peer `tree_aux` source. `checkpoint_sync = false` or
//! `consensus.vct_fast_sync = false` selects legacy recompute.

pub(super) mod artifact;
pub use artifact::{generate_mainnet_from_archive, GeneratorError};

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
use zakura_header_chain::AuxDelivery;

use super::{
    commitment_aux::{CommitmentRootSource, FinalFrontiers, PeerSource},
    ZakuraDb,
};

/// A VCT successor header used to authenticate the current block's supplied
/// note-commitment roots.
#[derive(Clone, Debug)]
pub struct NextVctBlock {
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

impl NextVctBlock {
    /// Build a successor witness from a header and its precomputed auth-data root.
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

    /// Build a successor witness while retaining its exact auxiliary delivery.
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

/// One atomically selected current delivery and its optional direct-successor witness.
#[derive(Clone, Debug)]
pub(crate) struct VctAuxWindow {
    /// Exact auxiliary delivery whose roots are folded for the current block.
    pub(crate) current: AuxDelivery,
    /// Exact direct-successor witness used for one-header-later authentication.
    pub(crate) successor: Option<NextVctBlock>,
}

impl VctAuxWindow {
    /// Return the exact current roots when the delivery still agrees with the block.
    pub(crate) fn current_roots(
        &self,
        height: block::Height,
        hash: block::Hash,
    ) -> Option<(
        sapling::tree::Root,
        orchard::tree::Root,
        ironwood::tree::Root,
    )> {
        let aux = self.current.tree_aux?;
        (self.current.header_hash == hash && aux.height == height).then_some((
            aux.sapling_root,
            aux.orchard_root,
            aux.ironwood_root,
        ))
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
/// A checkpoint-trusting sync (`checkpoint_sync = true`) uses the peer `tree_aux` source by
/// default on networks with embedded final frontiers; `checkpoint_sync = false` or
/// `vct_fast_sync = false` opts out to the legacy per-block recompute (no VCT state).
#[derive(Debug)]
pub(crate) struct VctState {
    /// `true` when the VCT fast-sync is enabled.
    enabled: bool,
    /// Where the verified per-block roots and final frontier come from. The
    /// committer reads roots/final frontier through this seam only.
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

/// Which commitment-root source the committer uses, resolved from the (already read)
/// configuration signals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceMode {
    /// Legacy recompute committer (no VCT state).
    Legacy,
    /// Fetch per-block roots from peers — the default where embedded frontiers exist.
    Peer,
}

/// Resolve the source mode as a pure function, so the peer-source default is
/// unit-testable without touching embedded-frontier files. The fast verified path
/// (peer source) is the default whenever the node syncs under checkpoint trust and
/// the network has an embedded handoff frontier. `checkpoint_sync = false` or
/// `vct_fast_sync = false` selects the legacy recompute; a network with no embedded
/// frontier also falls back to legacy. Storage mode (Archive vs. Pruned) is orthogonal and not
/// an input here.
fn select_source_mode(
    checkpoint_sync: bool,
    vct_fast_sync: bool,
    has_embedded_frontiers: bool,
) -> SourceMode {
    if !checkpoint_sync || !vct_fast_sync || !has_embedded_frontiers {
        SourceMode::Legacy
    } else {
        SourceMode::Peer
    }
}

impl VctState {
    /// Build the committer state from `checkpoint_sync` (the mirror of
    /// `consensus.checkpoint_sync`) and the `vct_fast_sync` knob.
    /// On networks with an embedded handoff frontier (Mainnet) a checkpoint-trusting sync
    /// defaults to the peer (`tree_aux`) fast source; disabling checkpoint sync, setting
    /// `vct_fast_sync = false`, or using a network without an embedded frontier returns `None` for
    /// a zero-overhead legacy committer that recomputes the trees per block.
    pub(super) fn from_config(
        checkpoint_sync: bool,
        vct_fast_sync: bool,
        network: &Network,
        db: ZakuraDb,
    ) -> Option<Arc<Self>> {
        // Parse the embedded handoff frontier once (None on networks without one, e.g.
        // Testnet). The decision below only needs its presence; the peer arm reuses the
        // parsed value.
        let embedded = embedded_final_frontiers(network);

        match select_source_mode(checkpoint_sync, vct_fast_sync, embedded.is_some()) {
            // Default: the peer (`tree_aux`) source on any network with embedded final
            // frontiers (Mainnet). Per-block roots arrive from peers into a shared cache
            // filled by the driver; the committer reads them per height and folds them in,
            // skipping the recompute. A height the peer cannot supply — or any node with no
            // serving peers — stays in legacy mode, bit-identical to a legacy committer by
            // construction.
            SourceMode::Peer => {
                let parsed = embedded?;
                tracing::info!(
                    handoff_height = parsed.height.0,
                    "VCT: peer (tree_aux) source enabled by default — roots fetched from peers"
                );
                let source = PeerSource::new(db, parsed);
                Some(Arc::new(VctState {
                    enabled: true,
                    source: Box::new(source),
                    requires_verified_successor: true,
                    vct_count: AtomicU64::new(0),
                    prevalidated_count: AtomicU64::new(0),
                }))
            }

            // Legacy committer: full per-block recompute when checkpoint sync is disabled, the
            // force-disable knob is set, or the network has no embedded frontiers. No VCT state,
            // zero overhead.
            SourceMode::Legacy => None,
        }
    }

    /// `true` when the VCT fast-sync is enabled.
    pub(super) fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// The supplied roots for `height`, when vct mode has a source entry for it
    /// (the signal that this block takes the VCT fast-sync).
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

    /// `true` when committing `height` on the vct path needs a stored successor header before
    /// it can safely persist this block's supplied roots.
    ///
    /// Only untrusted peer-supplied roots at or above Heartwood require this. The
    /// checkpoint handoff is exempt because its embedded final frontiers are verified
    /// against this block's roots before the real tip treestate is written; trusted
    /// local fixtures can commit their tip root on the in-arrears check.
    pub(super) fn vct_root_needs_successor(
        &self,
        height: block::Height,
        network: &Network,
    ) -> bool {
        self.enabled
            && self.vct_roots_at_height(height).is_some()
            && self.requires_verified_successor
            && self.source.final_frontiers().height != height
            && Some(height) >= NetworkUpgrade::Heartwood.activation_height(network)
    }

    /// Discard the supplied root for `height` after it failed verification, so a re-fetch
    /// can replace it. See [`CommitmentRootSource::invalidate`].
    pub(super) fn invalidate_fast_root(&self, height: block::Height) {
        self.source.invalidate(height);
    }

    /// The checkpoint handoff height: the boundary below which the fast path skips
    /// per-height note-commitment trees.
    pub(super) fn vct_sync_last_checkpoint_height(&self) -> block::Height {
        self.source.vct_last_checkpoint_height()
    }

    /// The verified `(sapling, orchard, sprout, ironwood)` frontiers to write as the tip
    /// treestate, when `height` is the checkpoint handoff height.
    #[allow(clippy::type_complexity)]
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
/// parameter from the committer down through
/// `ZakuraDb::write_block` to `ZakuraDb::prepare_trees_batch`.
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
    use std::io::Write;

    use serde::Deserialize;
    use sha2::{Digest, Sha256};

    use super::*;

    /// The tracked provenance record for the embedded Mainnet frontier.
    const MAINNET_FRONTIER_PROVENANCE: &[u8] = include_bytes!("vct/mainnet-frontier.json");

    /// The provenance schema written by the release-state refresh workflow and
    /// checked by `scripts/check-release-state.sh`; this test keeps the record
    /// bound to the embedded checkpoint list and frontier bytes on every PR.
    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct MainnetFrontierProvenance {
        schema_version: u32,
        network: String,
        source: String,
        generated_at: String,
        finalized_height: u32,
        finalized_hash: String,
        checkpoints_sha256: String,
        frontier_sha256: String,
        frontier_size: u64,
        #[serde(default)]
        meta_sha256: Option<String>,
    }

    #[test]
    fn source_mode_precedence() {
        use SourceMode::*;
        // Args are (checkpoint_sync, vct_fast_sync, has_embedded_frontiers).

        // The default: a checkpoint-trusting sync with VCT fast sync on uses the peer source
        // wherever embedded frontiers exist (Mainnet). Storage mode (Archive/Pruned) is not an
        // input, so this covers both Archive and Pruned.
        assert_eq!(select_source_mode(true, true, true), Peer);
        // `vct_fast_sync = false` keeps checkpoint sync on but forces the legacy recompute,
        // regardless of embedded frontiers.
        assert_eq!(select_source_mode(true, false, true), Legacy);
        assert_eq!(select_source_mode(true, false, false), Legacy);
        // `checkpoint_sync = false` also fully recomputes the trees: legacy, never peer,
        // regardless of the fast-sync knob or embedded frontiers.
        assert_eq!(select_source_mode(false, true, true), Legacy);
        assert_eq!(select_source_mode(false, true, false), Legacy);
        assert_eq!(select_source_mode(false, false, true), Legacy);
        assert_eq!(select_source_mode(false, false, false), Legacy);
        // No embedded frontiers (e.g. Testnet): legacy, never peer, even under checkpoint sync.
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
            !trusted.vct_root_needs_successor(height, &network),
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
            untrusted.vct_root_needs_successor(height, &network),
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
        let provenance: MainnetFrontierProvenance =
            serde_json::from_slice(MAINNET_FRONTIER_PROVENANCE)
                .expect("embedded Mainnet frontier provenance must be strict JSON");
        let finalized_hash: block::Hash = provenance
            .finalized_hash
            .parse()
            .expect("provenance must contain a canonical finalized block hash");

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
            "provenance must identify a supported source"
        );
        assert!(
            chrono::DateTime::parse_from_rfc3339(&provenance.generated_at).is_ok(),
            "provenance must contain an RFC 3339 generation time"
        );
        assert_eq!(provenance.finalized_height, frontiers.height.0);
        assert_eq!(
            Network::Mainnet.checkpoint_list().hash(frontiers.height),
            Some(finalized_hash),
            "provenance must identify the terminal Mainnet checkpoint"
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
            "provenance must authenticate the complete Mainnet checkpoint file"
        );
        assert_eq!(
            provenance.frontier_size,
            u64::try_from(MAINNET_FINAL_FRONTIERS.len()).expect("frontier length fits in u64")
        );
        assert_eq!(
            provenance.frontier_sha256,
            hex::encode(Sha256::digest(MAINNET_FINAL_FRONTIERS)),
            "provenance must authenticate the embedded Mainnet frontier bytes"
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
