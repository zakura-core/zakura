use std::{fmt, sync::Arc, time::Duration};

use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use zakura_chain::{block, parameters::Network};

use super::{
    v9::{Admitted, Ended, PageLimits},
    HeaderEntry, Headers, PeerSession, Status, ZakuraHeaderSyncConfig,
};
use crate::zakura::{
    regulation::ServeCapacity, HeaderSyncServiceSummary, ServicePeerSnapshot, Stream,
    ZakuraHeaderSyncCandidateState, ZakuraPeerId, ZakuraTrace,
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
    /// Atomic snapshots from the durable header engine.
    pub committed_snapshots: Option<watch::Receiver<Option<zakura_header_chain::EngineSnapshot>>>,
    /// VCT metadata repair needs published by the finalized writer.
    pub vct_root_repairs: Option<watch::Receiver<zakura_header_chain::VctRootRepairStatus>>,
    /// Typed durable header-chain operations.
    pub header_chain_port: Arc<dyn zakura_node_services::header_chain::Port>,
    /// Selects whether reactor operations use the typed port or the test observer.
    pub(crate) port_dispatch: PortDispatch,
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
    /// Private fatal notifications consumed by the node root.
    pub(crate) fatal_events: Option<mpsc::UnboundedSender<HeaderSyncFatalEvent>>,
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
        let status_refresh_interval = config.effective_status_refresh_interval();
        Self {
            network,
            anchor,
            frontiers,
            best_header_tip,
            committed_snapshots: None,
            vct_root_repairs: None,
            #[cfg(not(test))]
            header_chain_port: Arc::new(zakura_node_services::header_chain::UnavailablePort),
            #[cfg(test)]
            header_chain_port: Arc::new(zakura_node_services::header_chain::InertHeaderChainPort),
            #[cfg(not(any(test, feature = "zakura-testkit")))]
            port_dispatch: PortDispatch::Direct,
            #[cfg(any(test, feature = "zakura-testkit"))]
            port_dispatch: PortDispatch::External,
            config,
            max_frame_bytes,
            request_timeout: Duration::from_secs(30),
            status_refresh_interval,
            trace: ZakuraTrace::noop(),
            shutdown: CancellationToken::new(),
            fatal_events: None,
        }
    }

    /// Use the installed typed header-chain port directly.
    pub(crate) fn use_direct_port(&mut self) {
        self.port_dispatch = PortDispatch::Direct;
    }
}

/// A VCT repair state operation exceeded its local hard deadline.
///
/// This event stays inside the process. It does not change the header-sync wire protocol.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct HeaderSyncFatalEvent {
    /// Local state operation phase.
    pub phase: &'static str,
    /// Exact operation owner.
    pub owner: zakura_header_chain::HeaderSyncWorkOwner,
    /// Repair generation that started the operation.
    pub repair_generation: u64,
    /// Selected repair target.
    pub target: zakura_header_chain::Frontier,
    /// Time the local operation remained pending.
    pub elapsed: Duration,
}

impl fmt::Display for HeaderSyncFatalEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "VCT local {} operation exceeded its hard deadline: owner={:?}, generation={}, branch={:?}, target={:?}, elapsed={:?}",
            self.phase,
            self.owner,
            self.repair_generation,
            self.owner.header_authority().branch,
            self.target,
            self.elapsed,
        )
    }
}

impl std::error::Error for HeaderSyncFatalEvent {}

/// Explicit reactor port-dispatch policy.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum PortDispatch {
    /// Execute operations through the installed typed port.
    Direct,
    /// Publish operations to the test driver channel.
    #[cfg(any(test, feature = "zakura-testkit"))]
    External,
}

/// Cheap cloneable handle used by transport and discovery services.
#[derive(Clone, Debug)]
pub struct HeaderSyncHandle {
    pub(super) events: mpsc::Sender<Event>,
    pub(super) lifecycle: mpsc::UnboundedSender<Event>,
    pub(super) tip: watch::Receiver<(block::Height, block::Hash)>,
    pub(super) peers: watch::Receiver<ServicePeerSnapshot>,
    pub(super) candidates: watch::Receiver<ZakuraHeaderSyncCandidateState>,
    pub(super) trace: ZakuraTrace,
    /// The active network, which fixes the header size and stream layout.
    pub(super) network: Network,
    /// The stream layout for the network's header size.
    pub(super) layout: &'static [Stream],
    /// The header-chain port that publishers read pages from.
    pub(super) port: Arc<dyn zakura_node_services::header_chain::Port>,
    /// Node-wide capacity that every session's published pages share.
    pub(super) serving: ServeCapacity,
    /// The serving limits this node advertises in its `Status`.
    pub(super) page_limits: PageLimits,
}

impl HeaderSyncHandle {
    /// Send an event to the reactor.
    pub async fn send(&self, event: Event) -> Result<(), mpsc::error::SendError<Event>> {
        self.events.send(event).await
    }

    /// Try to send an event without awaiting.
    pub fn try_send(&self, event: Event) -> Result<(), mpsc::error::TrySendError<Event>> {
        self.events.try_send(event)
    }

    /// Send a lifecycle event over the unbounded control channel.
    pub fn send_lifecycle(&self, event: Event) -> Result<(), mpsc::error::SendError<Event>> {
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
}

/// Facts accepted by the header-sync reactor.
#[derive(Clone, Debug)]
pub enum Event {
    /// A canonical header-sync stream opened.
    PeerConnected(PeerSession),
    /// A canonical header-sync stream closed.
    PeerDisconnected {
        /// Authenticated peer identity.
        peer: ZakuraPeerId,
        /// Exact ordered-stream generation that closed.
        session_id: u64,
        /// Bounded first-cause teardown reason.
        reason: &'static str,
    },
    /// First-party discovery summary used only for dial preference.
    AdvisorySummary {
        /// Peer that supplied the summary.
        peer: ZakuraPeerId,
        /// Advisory summary.
        summary: HeaderSyncServiceSummary,
    },
    /// A `Status` decoded on a canonical stream.
    Status {
        /// Sending peer.
        peer: ZakuraPeerId,
        /// Ordered-stream generation.
        session_id: u64,
        /// The peer's advertisement.
        status: Status,
    },
    /// A page that the session's subscriber admitted.
    Page {
        /// Sending peer.
        peer: ZakuraPeerId,
        /// Ordered-stream generation.
        session_id: u64,
        /// The decoded page.
        page: Headers,
        /// Where the page leaves its subscription.
        admitted: Admitted,
    },
    /// A terminal outcome that the session's subscriber admitted.
    Outcome {
        /// Sending peer.
        peer: ZakuraPeerId,
        /// Ordered-stream generation.
        session_id: u64,
        /// The subscription the outcome ended.
        ended: Ended,
    },
    /// State returned the selected-path locator for an advertised target.
    #[cfg(any(test, feature = "zakura-testkit"))]
    HeaderLocatorReady {
        /// Peer whose target requested the locator.
        peer: ZakuraPeerId,
        /// Ordered-stream generation.
        session_id: u64,
        /// Exact advertised target hash.
        target_tip_hash: block::Hash,
        /// Durable generation and exact branch that scheduled the locator read.
        scope: zakura_header_chain::HeaderWorkAuthority,
        /// Coherent locator.
        /// State returns `None` when it is unavailable.
        locator: Option<zakura_header_chain::HeaderLocator>,
    },
    /// State resolved one branch-owned VCT repair against the exact selected projection.
    #[cfg(any(test, feature = "zakura-testkit"))]
    VctRepairContextReady {
        /// Owner echoed from the exact state query.
        owner: zakura_header_chain::BodyWorkOwner,
        /// Exact state-read outcome.
        result: VctRepairContextResult,
    },
    /// Requester validation completed outside the reactor.
    #[cfg(any(test, feature = "zakura-testkit"))]
    HeaderTargetPrepared {
        /// Peer whose exact active work produced the completion.
        peer: ZakuraPeerId,
        /// Stable source identity used by the pending-owner gate.
        source: zakura_header_chain::SourceId,
        /// Ownership token echoed by the driver.
        owner: zakura_header_chain::HeaderSyncWorkOwner,
        /// Sealed evidence or a typed preparation failure.
        result: HeaderTargetPreparationResult,
    },
    /// Atomic state admission completed.
    #[cfg(any(test, feature = "zakura-testkit"))]
    HeaderTargetAdmissionReady {
        /// Peer whose exact active work produced the completion.
        peer: ZakuraPeerId,
        /// Stable source identity used by the pending-owner gate.
        source: zakura_header_chain::SourceId,
        /// Ownership token echoed by the driver.
        owner: zakura_header_chain::HeaderSyncWorkOwner,
        /// Commit, stale-work, peer-invalid, or local-failure result.
        result: HeaderTargetAdmissionResult,
    },
}

/// Result of resolving one branch-owned VCT repair against durable state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VctRepairContextResult {
    /// State resolved the exact selected request for the current owner.
    Resolved(zakura_header_chain::VctRepairContext),
    /// State found a stale owner or selected height.
    Stale,
    /// Local state or driver capacity prevented the read.
    Unavailable,
}

impl Event {
    pub(super) fn metrics_label(&self) -> &'static str {
        match self {
            Self::PeerConnected(_) => "peer_connected",
            Self::PeerDisconnected { .. } => "peer_disconnected",
            Self::AdvisorySummary { .. } => "advisory_header_summary",
            Self::Status { .. } => "status",
            Self::Page { .. } => "page",
            Self::Outcome { .. } => "outcome",
            #[cfg(any(test, feature = "zakura-testkit"))]
            Self::HeaderLocatorReady { .. } => "header_locator_ready",
            #[cfg(any(test, feature = "zakura-testkit"))]
            Self::VctRepairContextReady { .. } => "vct_repair_context_ready",
            #[cfg(any(test, feature = "zakura-testkit"))]
            Self::HeaderTargetPrepared { .. } => "header_target_prepared",
            #[cfg(any(test, feature = "zakura-testkit"))]
            Self::HeaderTargetAdmissionReady { .. } => "header_target_admission_ready",
        }
    }
}

/// Result of preparing and atomically applying one complete requester target.
#[derive(Clone, Debug)]
pub enum HeaderTargetAdmissionResult {
    /// State committed the insertion or recognized its idempotent replay.
    Applied,
    /// State committed a local resource-stall outcome without admitting the target.
    ResourceStalled(zakura_header_chain::CommittedStallReceipt),
    /// Exact typed failure preserved from the state/driver boundary.
    Failed(Arc<zakura_header_chain::HeaderChainError>),
}

/// Sealed complete-target insertion returned by off-reactor validation.
#[derive(Clone, Debug)]
pub enum HeaderTargetPreparationResult {
    /// Validation passed all deterministic rules.
    /// The completion gate can now process the target.
    Prepared(zakura_node_services::header_chain::PreparedHeaderTarget),
    /// Exact typed failure preserved from the validation/driver boundary.
    Failed(Arc<zakura_header_chain::HeaderChainError>),
}

/// Reactor-local descriptions used to start typed port operations.
#[derive(Clone, Debug)]
pub enum HeaderPortOperation {
    /// Ask state for one exact coherent selected-path locator.
    QueryHeaderLocator {
        /// Peer whose advertisement owns the query.
        peer: ZakuraPeerId,
        /// Ordered-stream generation.
        session_id: u64,
        /// Exact advertised target hash.
        target_tip_hash: block::Hash,
        /// Durable generation and exact branch that owns the query.
        scope: zakura_header_chain::HeaderWorkAuthority,
    },
    /// Ask state to resolve one current branch-owned VCT repair.
    QueryVctRepairContext {
        /// Complete owner captured with the committed repair signal.
        owner: zakura_header_chain::BodyWorkOwner,
        /// Exact unavailable selected-header height.
        height: block::Height,
    },
    /// Validate all staged pages and submit exactly one complete-target insertion.
    PrepareHeaderTarget {
        /// Admission policy carried through the shared target lifecycle.
        purpose: super::HeaderTargetPurpose,
        /// Supplying peer.
        peer: ZakuraPeerId,
        /// Stable source identity used by the pending-owner gate.
        source: zakura_header_chain::SourceId,
        /// Exact asynchronous ownership fixed by the initial request.
        owner: zakura_header_chain::HeaderSyncWorkOwner,
        /// Exact initial locator intersection.
        common_ancestor: zakura_header_chain::Frontier,
        /// Exact admitted target.
        /// The admitted target can be a bounded prefix of the advertised target.
        target: zakura_header_chain::Frontier,
        /// Proof that this batch completes either the advertised target or a bounded prefix.
        completion: zakura_header_chain::TargetCompletion,
        /// All response entries in parent-first order.
        entries: Vec<HeaderEntry>,
    },
    /// Submit sealed evidence only after the centralized completion gate accepts its owner.
    ApplyHeaderTarget {
        /// Admission policy carried through the shared target lifecycle.
        purpose: super::HeaderTargetPurpose,
        /// Supplying peer, retained for result attribution.
        peer: ZakuraPeerId,
        /// Stable source identity used by the pending-owner gate.
        source: zakura_header_chain::SourceId,
        /// Exact current owner.
        owner: zakura_header_chain::HeaderSyncWorkOwner,
        /// Port-sealed target produced by deterministic preparation.
        target: zakura_node_services::header_chain::PreparedHeaderTarget,
    },
    /// Record peer protocol misbehavior.
    Misbehavior {
        /// Misbehaving peer.
        peer: ZakuraPeerId,
        /// Classification.
        reason: HeaderSyncMisbehavior,
    },
    /// Close one exact header-sync session that stopped making progress.
    ///
    /// An unresponsive peer does not violate the protocol.
    /// The drop action therefore remains distinct from [`Misbehavior`](Self::Misbehavior).
    /// Conflating the actions would feed a future ban score.
    DropPeer {
        /// Peer whose session the reactor closes.
        peer: ZakuraPeerId,
        /// Ordered-stream generation that the reactor closes.
        /// A strike against a superseded session cannot close its replacement.
        session_id: u64,
        /// Stable metrics and trace label for the session-close reason.
        reason: &'static str,
    },
}

/// Testkit observer for reactor-local port scheduling.
#[cfg(any(test, feature = "zakura-testkit"))]
pub type HeaderSyncAction = HeaderPortOperation;

/// Header-sync peer-accounting violations.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum HeaderSyncMisbehavior {
    /// The codec rejected a malformed wire payload.
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
