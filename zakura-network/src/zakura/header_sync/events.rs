use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use zakura_chain::{block, parameters::Network};

use super::{
    GetHeaders, HeaderEntry, HeaderSyncCodec, HeaderSyncMessage, HeaderSyncPeerSession,
    HeadersOutcomeCode, ZakuraHeaderSyncConfig,
};
use crate::zakura::{
    FrontierUpdate, HeaderSyncServiceSummary, ServicePeerSnapshot, ZakuraHeaderSyncCandidateState,
    ZakuraPeerId, ZakuraTrace,
};

/// Cached full-state frontiers used by header sync and block sync.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct FullStateFrontiers {
    /// Shared finalized height.
    pub finalized_height: block::Height,
    /// Highest verified block-body height.
    pub verified_block_tip: block::Height,
    /// Hash at the verified block-body tip.
    pub verified_block_hash: block::Hash,
}

/// Startup inputs for the header-sync reactor.
#[derive(Clone, Debug)]
pub struct HeaderSyncStartup {
    /// Active network.
    pub network: Network,
    /// Trusted anchor height and hash.
    pub anchor: (block::Height, block::Hash),
    /// Cached full-state frontiers at startup.
    pub frontiers: FullStateFrontiers,
    /// Durable best header tip loaded at startup.
    pub best_header_tip: Option<(block::Height, block::Hash)>,
    /// Shared sync frontier updates.
    pub frontier_updates: Option<watch::Receiver<FrontierUpdate>>,
    /// Atomic snapshots from the durable header engine.
    pub committed_snapshots: Option<watch::Receiver<Option<zakura_header_chain::EngineSnapshot>>>,
    /// VCT metadata repair needs published by the finalized writer.
    pub vct_root_repairs: Option<watch::Receiver<zakura_header_chain::VctRootRepairStatus>>,
    /// Local header-sync configuration.
    pub config: ZakuraHeaderSyncConfig,
    /// Application frame cap for header-sync messages.
    pub max_frame_bytes: u32,
    /// Per-request timeout.
    pub request_timeout: Duration,
    /// Status refresh interval.
    pub status_refresh_interval: Duration,
    /// Optional JSONL trace emitter.
    pub trace: ZakuraTrace,
    /// Shared shutdown signal.
    pub shutdown: CancellationToken,
}

impl HeaderSyncStartup {
    /// Build startup configuration from durable state facts.
    pub fn new(
        network: Network,
        anchor: (block::Height, block::Hash),
        frontiers: FullStateFrontiers,
        best_header_tip: Option<(block::Height, block::Hash)>,
        config: ZakuraHeaderSyncConfig,
        max_frame_bytes: u32,
    ) -> Self {
        let status_refresh_interval = config.status_refresh_interval;
        Self {
            network,
            anchor,
            frontiers,
            best_header_tip,
            frontier_updates: None,
            committed_snapshots: None,
            vct_root_repairs: None,
            config,
            max_frame_bytes,
            request_timeout: Duration::from_secs(30),
            status_refresh_interval,
            trace: ZakuraTrace::noop(),
            shutdown: CancellationToken::new(),
        }
    }
}

/// Cheap cloneable handle used by transport and discovery services.
#[derive(Clone, Debug)]
pub struct HeaderSyncHandle {
    pub(super) events: mpsc::Sender<HeaderSyncEvent>,
    pub(super) lifecycle: mpsc::UnboundedSender<HeaderSyncEvent>,
    pub(super) tip: watch::Receiver<(block::Height, block::Hash)>,
    pub(super) peers: watch::Receiver<ServicePeerSnapshot>,
    pub(super) candidates: watch::Receiver<ZakuraHeaderSyncCandidateState>,
    pub(super) codec: HeaderSyncCodec,
}

impl HeaderSyncHandle {
    /// Send an event to the reactor.
    pub async fn send(
        &self,
        event: HeaderSyncEvent,
    ) -> Result<(), mpsc::error::SendError<HeaderSyncEvent>> {
        self.events.send(event).await
    }

    /// Try to send an event without awaiting.
    pub fn try_send(
        &self,
        event: HeaderSyncEvent,
    ) -> Result<(), mpsc::error::TrySendError<HeaderSyncEvent>> {
        self.events.try_send(event)
    }

    /// Send a lifecycle event over the unbounded control channel.
    pub fn send_lifecycle(
        &self,
        event: HeaderSyncEvent,
    ) -> Result<(), mpsc::error::SendError<HeaderSyncEvent>> {
        self.lifecycle
            .send(event)
            .map_err(|error| mpsc::error::SendError(error.0))
    }

    /// Subscribe to selected header-tip updates.
    pub fn subscribe_tip(&self) -> watch::Receiver<(block::Height, block::Hash)> {
        self.tip.clone()
    }

    /// Return the cached selected header tip.
    pub fn best_header_tip(&self) -> (block::Height, block::Hash) {
        *self.tip.borrow()
    }

    /// Subscribe to header-sync peer accounting.
    pub fn subscribe_peer_snapshot(&self) -> watch::Receiver<ServicePeerSnapshot> {
        self.peers.clone()
    }

    /// Return the cached peer accounting snapshot.
    pub fn peer_snapshot(&self) -> ServicePeerSnapshot {
        *self.peers.borrow()
    }

    /// Subscribe to discovery candidate hints.
    pub fn subscribe_candidate_state(&self) -> watch::Receiver<ZakuraHeaderSyncCandidateState> {
        self.candidates.clone()
    }

    /// Return the cached discovery candidate hints.
    pub fn candidate_state(&self) -> ZakuraHeaderSyncCandidateState {
        self.candidates.borrow().clone()
    }

    /// Return the one codec used by every admitted header-sync stream.
    pub(crate) fn codec(&self) -> HeaderSyncCodec {
        self.codec.clone()
    }
}

/// Facts accepted by the header-sync reactor.
#[derive(Clone, Debug)]
pub enum HeaderSyncEvent {
    /// A canonical header-sync stream opened.
    PeerConnected(HeaderSyncPeerSession),
    /// A canonical header-sync stream closed.
    PeerDisconnected(ZakuraPeerId),
    /// First-party discovery summary used only for dial preference.
    AdvisoryHeaderSummary {
        /// Peer that supplied the summary.
        peer: ZakuraPeerId,
        /// Advisory summary.
        summary: HeaderSyncServiceSummary,
    },
    /// State committed a full block.
    FullBlockCommitted {
        /// Committed height.
        height: block::Height,
        /// Committed hash.
        hash: block::Hash,
    },
    /// A message decoded on a canonical stream.
    SessionWireMessage {
        /// Sending peer.
        peer: ZakuraPeerId,
        /// Ordered-stream generation.
        session_id: u64,
        /// Decoded message.
        msg: HeaderSyncMessage,
    },
    /// Full-state frontiers changed.
    StateFrontiersChanged(FullStateFrontiers),
    /// State returned the selected-path locator for an advertised target.
    HeaderLocatorReady {
        /// Peer whose target requested the locator.
        peer: ZakuraPeerId,
        /// Ordered-stream generation.
        session_id: u64,
        /// Exact advertised target hash.
        target_tip_hash: block::Hash,
        /// Coherent locator, absent when state is unavailable.
        locator: Option<zakura_header_chain::HeaderLocator>,
    },
    /// State resolved one branch-owned VCT repair against the exact selected projection.
    VctRepairContextReady {
        /// Owner echoed from the exact state query.
        owner: zakura_header_chain::WorkOwner,
        /// Exact state-read outcome.
        result: VctRepairContextResult,
    },
    /// State finished acquiring an immutable path for one inbound request.
    HeaderPathLeaseReady {
        /// Peer that sent the request.
        peer: ZakuraPeerId,
        /// Ordered-stream generation that owns the request.
        session_id: u64,
        /// Exact request whose state read completed.
        request: GetHeaders,
        /// Acquired lease or explicit wire outcome.
        result: HeaderPathLeaseResult,
    },
    /// State finished reading one hash-keyed page from an immutable path.
    HeaderPathPageReady {
        /// Peer that owns the lease.
        peer: ZakuraPeerId,
        /// Ordered-stream generation that owns the lease.
        session_id: u64,
        /// Exact request correlation identifier.
        request_id: HeaderSyncRequestId,
        /// Exact snapshot-bound target.
        target_tip_hash: block::Hash,
        /// Page data or an unavailable-lease outcome.
        result: HeaderPathPageResult,
    },
    /// Requester validation completed outside the reactor.
    HeaderTargetPrepared {
        /// Peer whose exact active work produced the completion.
        peer: ZakuraPeerId,
        /// Stable source identity used by the pending-owner gate.
        source: zakura_header_chain::SourceId,
        /// Ownership token echoed by the driver.
        owner: zakura_header_chain::WorkOwner,
        /// Sealed evidence or a typed preparation failure.
        result: HeaderTargetPreparationResult,
    },
    /// One exact selected VCT metadata redelivery was prepared outside the reactor.
    VctRepairPrepared {
        /// Supplying peer.
        peer: ZakuraPeerId,
        /// Stable supplier identity.
        source: zakura_header_chain::SourceId,
        /// Exact current repair owner.
        owner: zakura_header_chain::WorkOwner,
        /// Sealed metadata-only insertion or typed preparation failure.
        result: HeaderTargetPreparationResult,
    },
    /// Atomic state admission completed.
    HeaderTargetAdmissionReady {
        /// Peer whose exact active work produced the completion.
        peer: ZakuraPeerId,
        /// Stable source identity used by the pending-owner gate.
        source: zakura_header_chain::SourceId,
        /// Ownership token echoed by the driver.
        owner: zakura_header_chain::WorkOwner,
        /// Commit, stale-work, peer-invalid, or local-failure result.
        result: HeaderTargetAdmissionResult,
    },
    /// Atomic selected VCT metadata admission completed.
    VctRepairAdmissionReady {
        /// Supplying peer.
        peer: ZakuraPeerId,
        /// Stable supplier identity.
        source: zakura_header_chain::SourceId,
        /// Exact current repair owner.
        owner: zakura_header_chain::WorkOwner,
        /// Commit, stale-work, peer-invalid, or local-failure result.
        result: HeaderTargetAdmissionResult,
    },
}

/// Result of resolving one branch-owned VCT repair against durable state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VctRepairContextResult {
    /// The owner is current and the exact selected request was resolved.
    Resolved(zakura_header_chain::VctRepairContext),
    /// The owner or requested selected height is no longer current.
    Stale,
    /// Local state or driver capacity prevented the read.
    Unavailable,
}

impl HeaderSyncEvent {
    pub(super) fn metrics_label(&self) -> &'static str {
        match self {
            Self::PeerConnected(_) => "peer_connected",
            Self::PeerDisconnected(_) => "peer_disconnected",
            Self::AdvisoryHeaderSummary { .. } => "advisory_header_summary",
            Self::FullBlockCommitted { .. } => "full_block_committed",
            Self::SessionWireMessage { .. } => "session_wire_message",
            Self::StateFrontiersChanged(_) => "state_frontiers_changed",
            Self::HeaderLocatorReady { .. } => "header_locator_ready",
            Self::VctRepairContextReady { .. } => "vct_repair_context_ready",
            Self::HeaderPathLeaseReady { .. } => "header_path_lease_ready",
            Self::HeaderPathPageReady { .. } => "header_path_page_ready",
            Self::HeaderTargetPrepared { .. } => "header_target_prepared",
            Self::VctRepairPrepared { .. } => "vct_repair_prepared",
            Self::HeaderTargetAdmissionReady { .. } => "header_target_admission_ready",
            Self::VctRepairAdmissionReady { .. } => "vct_repair_admission_ready",
        }
    }
}

/// Network-facing state result for one exact retained target lease.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HeaderPathLeaseResult {
    /// The immutable target path was acquired.
    Acquired(HeaderPathLease),
    /// State mapped the request to one explicit non-data protocol outcome.
    Outcome(HeadersOutcomeCode),
}

/// Minimum immutable lease facts needed by the serving reactor.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct HeaderPathLease {
    /// State-issued lease identity.
    pub lease_id: u64,
    /// First requester-order locator intersection.
    pub common_ancestor: zakura_header_chain::Frontier,
    /// Exact retained target fixed by the lease.
    pub target: zakura_header_chain::Frontier,
}

/// Network-facing state result for one retained target page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HeaderPathPageResult {
    /// One fully assembled hash-keyed page.
    Page(HeaderPathPage),
    /// The lease expired or became unavailable before the read.
    Unavailable,
}

/// One count-bounded page read from an immutable retained target path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderPathPage {
    /// State-issued lease identity.
    pub lease_id: u64,
    /// Exact page ancestor: the initial intersection or previous page tip.
    pub common_ancestor: zakura_header_chain::Frontier,
    /// Exact retained target fixed by the lease.
    pub target: zakura_header_chain::Frontier,
    /// Canonical headers and parallel advisory metadata.
    pub entries: Vec<HeaderEntry>,
    /// Whether this page reaches the immutable target.
    pub complete: bool,
}

/// Result of preparing and atomically applying one complete requester target.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum HeaderTargetAdmissionResult {
    /// State committed the insertion or recognized its idempotent replay.
    Applied,
    /// The durable generation or validation lease changed before admission.
    Stale,
    /// A deterministic peer-supplied header rule failed.
    InvalidHeader,
    /// Local state, configuration, or service availability prevented admission.
    LocalFailure,
}

/// Sealed complete-target insertion returned by off-reactor validation.
#[derive(Clone, Debug)]
pub enum HeaderTargetPreparationResult {
    /// All deterministic rules passed and the target is ready for the completion gate.
    Prepared(Box<zakura_header_chain::InsertHeaders>),
    /// Durable context changed before preparation completed.
    Stale,
    /// A deterministic peer-supplied header rule failed.
    InvalidHeader,
    /// Local state, configuration, clock, or task availability failed.
    LocalFailure,
}

/// Actions emitted by the header-sync reactor for node wiring.
#[derive(Clone, Debug)]
pub enum HeaderSyncAction {
    /// Ask state for one exact coherent selected-path locator.
    QueryHeaderLocator {
        /// Peer whose advertisement owns the query.
        peer: ZakuraPeerId,
        /// Ordered-stream generation.
        session_id: u64,
        /// Exact advertised target hash.
        target_tip_hash: block::Hash,
    },
    /// Ask state to resolve one current branch-owned VCT repair.
    QueryVctRepairContext {
        /// Complete owner captured with the committed repair signal.
        owner: zakura_header_chain::WorkOwner,
        /// Exact unavailable selected-header height.
        height: block::Height,
    },
    /// Acquire an immutable retained path for one inbound request.
    AcquireHeaderPath {
        /// Peer that sent the request.
        peer: ZakuraPeerId,
        /// Ordered-stream generation that owns the request.
        session_id: u64,
        /// Exact request to snapshot in state.
        request: GetHeaders,
    },
    /// Read one bounded page from an already acquired retained path.
    ReadHeaderPath {
        /// Peer that owns the lease.
        peer: ZakuraPeerId,
        /// Ordered-stream generation that owns the lease.
        session_id: u64,
        /// State-issued lease identity.
        lease_id: u64,
        /// Exact wire request identifier.
        request_id: HeaderSyncRequestId,
        /// Exact snapshot-bound target.
        target_tip_hash: block::Hash,
        /// Common ancestor or previous page tip.
        after_hash: block::Hash,
        /// Count cap after local, remote, and byte limits.
        max_header_count: u32,
    },
    /// Release one retained path owned by an exact peer session.
    ReleaseHeaderPath {
        /// Peer that owns the lease.
        peer: ZakuraPeerId,
        /// Ordered-stream generation that owns the lease.
        session_id: u64,
        /// State-issued lease identity.
        lease_id: u64,
    },
    /// Validate all staged pages and submit exactly one complete-target insertion.
    PrepareHeaderTarget {
        /// Supplying peer.
        peer: ZakuraPeerId,
        /// Stable source identity used by the pending-owner gate.
        source: zakura_header_chain::SourceId,
        /// Authenticated network parameters.
        network: Network,
        /// Exact asynchronous ownership fixed by the initial request.
        owner: zakura_header_chain::WorkOwner,
        /// Exact initial locator intersection.
        common_ancestor: zakura_header_chain::Frontier,
        /// Exact advertised target.
        target: zakura_header_chain::Frontier,
        /// All response entries in parent-first order.
        entries: Vec<HeaderEntry>,
    },
    /// Validate one exact selected-header redelivery and seal its auxiliary provenance.
    PrepareVctRepair {
        /// Supplying peer.
        peer: ZakuraPeerId,
        /// Stable supplier identity.
        source: zakura_header_chain::SourceId,
        /// Authenticated network parameters.
        network: Network,
        /// Exact current repair owner.
        owner: zakura_header_chain::WorkOwner,
        /// State-resolved selected request context.
        context: zakura_header_chain::VctRepairContext,
        /// Exact complete one-header response.
        entry: HeaderEntry,
    },
    /// Submit sealed evidence only after the centralized completion gate accepts its owner.
    ApplyHeaderTarget {
        /// Supplying peer, retained for result attribution.
        peer: ZakuraPeerId,
        /// Stable source identity used by the pending-owner gate.
        source: zakura_header_chain::SourceId,
        /// Exact current owner.
        owner: zakura_header_chain::WorkOwner,
        /// Sealed insertion produced by `prepare_headers`.
        insert: Box<zakura_header_chain::InsertHeaders>,
    },
    /// Submit one sealed selected VCT metadata redelivery after the completion gate.
    ApplyVctRepair {
        /// Supplying peer.
        peer: ZakuraPeerId,
        /// Stable supplier identity.
        source: zakura_header_chain::SourceId,
        /// Exact current repair owner.
        owner: zakura_header_chain::WorkOwner,
        /// Sealed selected-auxiliary insertion.
        insert: Box<zakura_header_chain::InsertHeaders>,
    },
    /// Ask state for missing block-body gaps.
    QueryMissingBlockBodies {
        /// First height to consider.
        from: block::Height,
        /// Maximum number of heights.
        limit: u32,
    },
    /// Record peer protocol misbehavior.
    Misbehavior {
        /// Misbehaving peer.
        peer: ZakuraPeerId,
        /// Classification.
        reason: HeaderSyncMisbehavior,
    },
    /// Notify body download wiring about a missing interval.
    BodyGaps {
        /// First missing height.
        from: block::Height,
        /// Last missing height.
        to: block::Height,
    },
    /// Notify wiring that the selected header target advanced.
    HeaderAdvanced {
        /// New height.
        height: block::Height,
        /// New hash.
        hash: block::Hash,
    },
    /// Notify wiring that the selected header target re-anchored.
    HeaderReanchored {
        /// Previous target.
        old: (block::Height, block::Hash),
        /// New target.
        new: (block::Height, block::Hash),
    },
}

/// Header-sync peer-accounting violations.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum HeaderSyncMisbehavior {
    /// A wire payload was malformed.
    MalformedMessage,
    /// A header failed protocol or consensus validation.
    InvalidHeader,
}

/// Nonzero request identifier, strictly increasing per stream session.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct HeaderSyncRequestId(u64);

impl HeaderSyncRequestId {
    /// Create a nonzero request identifier.
    pub fn new(id: u64) -> Option<Self> {
        (id != 0).then_some(Self(id))
    }

    /// Return the wire value.
    pub fn get(self) -> u64 {
        self.0
    }
}
