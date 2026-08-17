//! Durable metadata root and published engine snapshot.

use zakura_chain::{block, parameters::NetworkKind};

use crate::{
    BodyUnavailableSummary, BodyWorkAuthority, BodyWorkEpoch, BranchId, ChainScore, EngineMode,
    FinalityEpoch, Frontier, FrontierSet, HeaderGeneration, HeaderWorkAuthority, StateVersion,
    VerifiedGeneration,
};

use super::event::TransitionFingerprint;

/// Opaque version of the durable header-chain disk schema.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct HeaderChainDiskVersion(pub u32);

impl HeaderChainDiskVersion {
    /// Current durable header-chain schema version.
    pub const CURRENT: Self = Self(2);
}

/// Persistent externally visible engine alarms.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AlarmSet {
    /// Protected paths prevented resource-bound enforcement.
    pub resource_stalled: bool,
    /// The selected branch has exhausted its current body suppliers/retry episode.
    pub header_best_body_unavailable: Option<BodyUnavailableSummary>,
    /// Deterministic body validation refuted an imported headers-only trust pin.
    pub migrated_pin_refuted: Option<Frontier>,
}

/// Atomic read snapshot published only after durable commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EngineSnapshot {
    /// Finality authority mode.
    pub mode: EngineMode,
    /// Complete durable state version.
    pub state_version: StateVersion,
    /// Selected-header work generation.
    pub header_generation: HeaderGeneration,
    /// Full-state verified-path generation.
    pub verified_generation: VerifiedGeneration,
    /// Exact finalized, selected-header, and verified frontiers.
    pub frontiers: FrontierSet,
    /// Exact score of `frontiers.header_best` after the work anchor.
    pub header_best_score: ChainScore,
    /// Lowest retained height available for serving/context.
    pub oldest_retained_height: block::Height,
    /// Durable operator-visible alarms.
    pub alarms: AlarmSet,
}

/// One atomic committed snapshot and its body-work compatibility epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommittedHeaderChainView {
    /// Exact durable header-chain snapshot.
    pub snapshot: EngineSnapshot,
    /// Process-local selected-lineage epoch.
    pub body_work_epoch: BodyWorkEpoch,
}

impl CommittedHeaderChainView {
    /// Construct a committed view at one body-work epoch.
    pub const fn new(snapshot: EngineSnapshot, body_work_epoch: BodyWorkEpoch) -> Self {
        Self {
            snapshot,
            body_work_epoch,
        }
    }
}

impl std::ops::Deref for CommittedHeaderChainView {
    type Target = EngineSnapshot;

    fn deref(&self) -> &Self::Target {
        &self.snapshot
    }
}

impl HeaderWorkAuthority {
    /// Capture authority for one exact advertised header target.
    pub fn for_target(snapshot: &EngineSnapshot, target_tip_hash: block::Hash) -> Self {
        Self {
            header_generation: snapshot.header_generation,
            branch: BranchId::new(snapshot.frontiers.finalized.hash, target_tip_hash),
        }
    }
}

impl BodyWorkAuthority {
    /// Capture body-affecting authority from one atomic committed snapshot.
    pub fn for_snapshot(snapshot: &EngineSnapshot) -> Self {
        Self {
            header: HeaderWorkAuthority {
                header_generation: snapshot.header_generation,
                branch: BranchId::new(
                    snapshot.frontiers.finalized.hash,
                    snapshot.frontiers.header_best.hash,
                ),
            },
            verified_generation: snapshot.verified_generation,
            body_work_epoch: BodyWorkEpoch::default(),
        }
    }

    /// Capture body authority from one atomic committed view.
    pub fn for_view(view: &CommittedHeaderChainView) -> Self {
        let mut authority = Self::for_snapshot(&view.snapshot);
        authority.body_work_epoch = view.body_work_epoch;
        authority
    }
}

/// Singleton durable metadata row that serves as the logical root of one committed state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EngineMetadata {
    /// Durable schema version.
    pub disk_format: HeaderChainDiskVersion,
    /// Persisted finality mode.
    pub mode: EngineMode,
    /// Persisted authenticated network identity.
    pub network_id: NetworkKind,
    /// Digest of the release-authenticated settled manifest.
    pub anchor_manifest_digest: [u8; 32],
    /// Immutable work-coordinate origin.
    pub work_origin: Frontier,
    /// Complete durable state version.
    pub state_version: StateVersion,
    /// Selected-header work generation.
    pub header_generation: HeaderGeneration,
    /// Full-state verified-path generation.
    pub verified_generation: VerifiedGeneration,
    /// Finality advancement epoch.
    pub finality_epoch: FinalityEpoch,
    /// Last finality epoch imported by a headers-only to integrated mode migration.
    pub headers_only_migration_epoch: Option<FinalityEpoch>,
    /// Exact durable frontiers.
    pub frontiers: FrontierSet,
    /// Exact selected-header score.
    pub header_best_score: ChainScore,
    /// Lowest retained height.
    pub oldest_retained_height: block::Height,
    /// Durable alarms.
    pub alarms: AlarmSet,
    /// Domain- and payload-bound identity of the most recent committed transition.
    pub last_transition: Option<TransitionFingerprint>,
}

impl EngineMetadata {
    /// Project the authoritative metadata row into its externally visible snapshot.
    pub fn snapshot(&self) -> EngineSnapshot {
        EngineSnapshot {
            mode: self.mode,
            state_version: self.state_version,
            header_generation: self.header_generation,
            verified_generation: self.verified_generation,
            frontiers: self.frontiers,
            header_best_score: self.header_best_score,
            oldest_retained_height: self.oldest_retained_height,
            alarms: self.alarms.clone(),
        }
    }
}
