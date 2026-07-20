use super::{config::*, error::*, validation::*, wire::*, *};
use crate::zakura::{
    FrontierUpdate, HeaderSyncPeerSession, HeaderSyncServiceSummary, ServicePeerSnapshot,
    ZakuraHeaderSyncCandidateState,
};

/// Cached state frontiers used by the header-sync reactor.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct HeaderSyncFrontiers {
    /// Shared finalized height `F`, supplied by state.
    pub finalized_height: block::Height,
    /// Highest verified block body height, supplied by state.
    pub verified_block_tip: block::Height,
    /// Hash at the highest verified block body height, supplied by state.
    pub verified_block_hash: block::Hash,
}

/// Startup inputs for the dependency-neutral header-sync reactor.
#[derive(Clone, Debug)]
pub struct HeaderSyncStartup {
    /// Active network.
    pub network: Network,
    /// Trusted anchor height and hash.
    pub anchor: (block::Height, block::Hash),
    /// Cached state frontiers at startup.
    pub frontiers: HeaderSyncFrontiers,
    /// Durable best header tip loaded from storage at startup.
    pub best_header_tip: Option<(block::Height, block::Hash)>,
    /// Shared sync exchange frontier stream.
    pub frontier_updates: Option<watch::Receiver<FrontierUpdate>>,
    /// Local header-sync advertisement.
    pub config: ZakuraHeaderSyncConfig,
    /// Negotiated or local application frame cap for header-sync responses.
    pub max_frame_bytes: u32,
    /// Per-request timeout.
    pub request_timeout: Duration,
    /// Minimum interval between unsolicited status refreshes to a peer.
    pub status_refresh_interval: Duration,
    /// Optional JSONL trace emitter for header-sync runtime events.
    pub trace: ZakuraTrace,
    /// Shared shutdown signal owned by the embedding endpoint or test harness.
    pub shutdown: CancellationToken,
    /// Enables outbound range scheduling and state-backed header actions.
    pub range_state_actions_enabled: bool,
    /// Enables relaying inbound `NewBlock` messages after local block acceptance is wired.
    pub inbound_new_block_acceptance_enabled: bool,
}

impl HeaderSyncStartup {
    /// Build a startup config from the active network and durable/frontier facts.
    pub fn new(
        network: Network,
        anchor: (block::Height, block::Hash),
        frontiers: HeaderSyncFrontiers,
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
            config,
            max_frame_bytes,
            request_timeout: DEFAULT_HS_REQUEST_TIMEOUT,
            status_refresh_interval,
            trace: ZakuraTrace::noop(),
            shutdown: CancellationToken::new(),
            range_state_actions_enabled: false,
            inbound_new_block_acceptance_enabled: false,
        }
    }
}

/// Cheap cloneable handle used by other services to inform header sync.
#[derive(Clone, Debug)]
pub struct HeaderSyncHandle {
    pub(super) events: mpsc::Sender<HeaderSyncEvent>,
    pub(super) lifecycle: mpsc::UnboundedSender<HeaderSyncEvent>,
    pub(super) tip: watch::Receiver<(block::Height, block::Hash)>,
    pub(super) peers: watch::Receiver<ServicePeerSnapshot>,
    pub(super) candidates: watch::Receiver<ZakuraHeaderSyncCandidateState>,
}

impl HeaderSyncHandle {
    /// Send a fact/event to the header-sync reactor.
    pub async fn send(
        &self,
        event: HeaderSyncEvent,
    ) -> Result<(), mpsc::error::SendError<HeaderSyncEvent>> {
        self.events.send(event).await
    }

    /// Try to send a fact/event without awaiting.
    pub fn try_send(
        &self,
        event: HeaderSyncEvent,
    ) -> Result<(), mpsc::error::TrySendError<HeaderSyncEvent>> {
        self.events.try_send(event)
    }

    /// Send a peer lifecycle event without sharing the bounded wire-event queue.
    pub fn send_lifecycle(
        &self,
        event: HeaderSyncEvent,
    ) -> Result<(), mpsc::error::SendError<HeaderSyncEvent>> {
        self.lifecycle
            .send(event)
            .map_err(|error| mpsc::error::SendError(error.0))
    }

    /// Subscribe to best-header frontier updates.
    pub fn subscribe_tip(&self) -> watch::Receiver<(block::Height, block::Hash)> {
        self.tip.clone()
    }

    /// Return the currently cached best-header frontier.
    pub fn best_header_tip(&self) -> (block::Height, block::Hash) {
        *self.tip.borrow()
    }

    /// Subscribe to header-sync peer slot snapshots.
    pub fn subscribe_peer_snapshot(&self) -> watch::Receiver<ServicePeerSnapshot> {
        self.peers.clone()
    }

    /// Return the currently cached peer slot snapshot.
    pub fn peer_snapshot(&self) -> ServicePeerSnapshot {
        *self.peers.borrow()
    }

    /// Subscribe to header-sync candidate hints for discovery selection.
    pub fn subscribe_candidate_state(&self) -> watch::Receiver<ZakuraHeaderSyncCandidateState> {
        self.candidates.clone()
    }

    /// Return the currently cached header-sync candidate hints.
    pub fn candidate_state(&self) -> ZakuraHeaderSyncCandidateState {
        self.candidates.borrow().clone()
    }
}

/// Facts accepted by the header-sync reactor.
#[derive(Clone, Debug)]
pub enum HeaderSyncEvent {
    /// A peer became available for negotiated header sync.
    PeerConnected(HeaderSyncPeerSession),
    /// A peer disconnected; all of its outstanding work is dropped.
    PeerDisconnected(ZakuraPeerId),
    /// First-party header-sync summary observed over the authenticated discovery stream.
    AdvisoryHeaderSummary {
        /// Peer that supplied its own summary.
        peer: ZakuraPeerId,
        /// Advisory header-sync summary for dial/admission preference only.
        summary: HeaderSyncServiceSummary,
    },
    /// State committed a full block.
    FullBlockCommitted {
        /// Committed block height.
        height: block::Height,
        /// Committed block hash.
        hash: block::Hash,
    },
    /// The node's block pipeline accepted an inbound `NewBlock` body.
    NewBlockAccepted {
        /// Source peer.
        peer: ZakuraPeerId,
        /// Accepted block height.
        height: block::Height,
        /// Accepted block hash.
        hash: block::Hash,
        /// Accepted full block.
        block: Arc<block::Block>,
    },
    /// The node's block pipeline reported an inbound `NewBlock` was already known.
    NewBlockDuplicate {
        /// Source peer.
        peer: ZakuraPeerId,
        /// Duplicate block height.
        height: block::Height,
        /// Duplicate block hash.
        hash: block::Hash,
    },
    /// The node's block pipeline accepted an inbound `NewBlock` body, but it
    /// did not land on the best chain. The block is remembered for dedup only:
    /// a non-best-chain block must not advance the header or verified
    /// frontiers and must not be forwarded to peers, or the whole Zakura layer
    /// gossips a losing branch while the node's own chain stays on the best
    /// one.
    NewBlockAcceptedNonBestChain {
        /// Source peer.
        peer: ZakuraPeerId,
        /// Accepted non-best-chain block height.
        height: block::Height,
        /// Accepted non-best-chain block hash.
        hash: block::Hash,
    },
    /// The node's block pipeline rejected an inbound `NewBlock` body.
    NewBlockRejected {
        /// Source peer.
        peer: ZakuraPeerId,
        /// Rejected block hash.
        hash: block::Hash,
    },
    /// Test-only inbound header-sync message without a session generation.
    #[cfg(test)]
    WireMessage {
        /// Serving peer.
        peer: ZakuraPeerId,
        /// Decoded header-sync message.
        msg: HeaderSyncMessage,
    },
    /// Inbound control message from a specific transport session.
    SessionWireMessage {
        /// Serving peer.
        peer: ZakuraPeerId,
        /// Ordered-stream generation that delivered the message.
        session_id: u64,
        /// Decoded header-sync message.
        msg: HeaderSyncMessage,
    },
    /// Inbound `Headers` response with its mandatory request ID.
    WireHeaders {
        /// Exact request identity for this response.
        wire_request: HeaderSyncWireRequestIdentity,
        /// Structurally aligned per-height records.
        entries: Vec<HeaderRangeEntry>,
    },
    /// Inbound `GetHeaders` request with its mandatory request ID.
    WireGetHeaders {
        /// Requesting peer.
        peer: ZakuraPeerId,
        /// Ordered-stream generation that delivered the request.
        session_id: u64,
        /// Request ID supplied by the peer.
        request_id: HeaderSyncRequestId,
        /// First requested height.
        start_height: block::Height,
        /// Requested header count.
        count: u32,
        /// Whether the requester wants all-or-nothing tree-aux roots.
        want_tree_aux_roots: bool,
    },
    /// Header-sync frame decoding failed after handler admission.
    WireDecodeFailed {
        /// Peer that sent the malformed frame.
        peer: ZakuraPeerId,
        /// Decode/validation error.
        error: Arc<HeaderSyncWireError>,
    },
    /// Header-sync protocol failure decoded by the peer-owned session.
    WireProtocolFailure {
        /// Peer that sent the invalid message.
        peer: ZakuraPeerId,
        /// Misbehavior classification for the protocol failure.
        reason: HeaderSyncMisbehavior,
        /// Decode/validation error.
        error: Arc<HeaderSyncWireError>,
    },
    /// State finalized or verified-body frontiers changed.
    StateFrontiersChanged(HeaderSyncFrontiers),
    /// State needs a bounded re-delivery of VCT supplied roots for a covered height.
    VctRootRepairRequested {
        /// Height whose supplied root is missing or was evicted after rejection.
        height: block::Height,
        /// State repair generation.
        generation: u64,
        /// Parent hash for `height - 1`, used as the first header anchor.
        anchor_hash: block::Hash,
        /// Canonical hashes expected in the repair response, starting at `height`.
        expected_hashes: Vec<(block::Height, block::Hash)>,
    },
    /// State successfully committed the formerly parked VCT height.
    VctRootRepairResolved {
        /// State repair generation that was resolved.
        generation: u64,
    },
    /// State returned the durable best header tip during startup or refresh.
    BestHeaderTipLoaded {
        /// Durable best header tip height.
        tip_height: block::Height,
        /// Durable best header tip hash.
        tip_hash: block::Hash,
    },
    /// State successfully committed a header range.
    HeaderRangeOperationCompleted {
        /// Exact header-commit operation that completed.
        operation: HeaderSyncOperationIdentity,
        /// New best header tip hash.
        tip_hash: block::Hash,
    },
    /// State rejected a previously requested range.
    HeaderRangeOperationFailed {
        /// Exact header-commit operation that failed.
        operation: HeaderSyncOperationIdentity,
        /// Whether state rejected peer data or hit a local resource/channel failure.
        kind: HeaderSyncCommitFailureKind,
    },
    /// Node wiring finished or abandoned a `Headers` response to an inbound `GetHeaders`.
    HeaderRangeResponseFinished {
        /// Peer whose served-response slot can be released.
        peer: ZakuraPeerId,
        /// Ordered-stream generation that requested the range.
        session_id: u64,
        /// Request ID supplied by the peer.
        request_id: HeaderSyncRequestId,
        /// First requested height.
        start_height: block::Height,
        /// Requested header count.
        requested_count: u32,
        /// Number of headers read from state and sent in the response.
        returned_count: u32,
    },
    /// State returned headers requested by a peer and the reactor should send them.
    HeaderRangeResponseReady {
        /// Peer whose inbound request is being served.
        peer: ZakuraPeerId,
        /// Ordered-stream generation that requested the range.
        session_id: u64,
        /// Request ID supplied by the peer.
        request_id: HeaderSyncRequestId,
        /// First requested height.
        start_height: block::Height,
        /// Requested header count.
        requested_count: u32,
        /// Whether the original request wanted all-or-nothing tree-aux roots.
        want_tree_aux_roots: bool,
        /// Bounded headers returned by state.
        headers: Vec<Arc<block::Header>>,
        /// Advisory serialized body sizes, parallel to `headers`.
        body_sizes: Vec<u32>,
        /// Per-height commitment roots, parallel to `headers`.
        tree_aux_roots: Vec<BlockCommitmentRoots>,
    },
}

impl HeaderSyncEvent {
    /// Low-cardinality variant label for reactor liveness metrics.
    pub(super) fn metrics_label(&self) -> &'static str {
        match self {
            Self::PeerConnected(_) => "peer_connected",
            Self::PeerDisconnected(_) => "peer_disconnected",
            Self::AdvisoryHeaderSummary { .. } => "advisory_header_summary",
            Self::FullBlockCommitted { .. } => "full_block_committed",
            Self::NewBlockAccepted { .. } => "new_block_accepted",
            Self::NewBlockDuplicate { .. } => "new_block_duplicate",
            Self::NewBlockAcceptedNonBestChain { .. } => "new_block_accepted_non_best_chain",
            Self::NewBlockRejected { .. } => "new_block_rejected",
            #[cfg(test)]
            Self::WireMessage { .. } => "wire_message",
            Self::SessionWireMessage { .. } => "session_wire_message",
            Self::WireHeaders { .. } => "wire_headers",
            Self::WireGetHeaders { .. } => "wire_get_headers",
            Self::WireDecodeFailed { .. } => "wire_decode_failed",
            Self::WireProtocolFailure { .. } => "wire_protocol_failure",
            Self::StateFrontiersChanged(_) => "state_frontiers_changed",
            Self::VctRootRepairRequested { .. } => "vct_root_repair_requested",
            Self::VctRootRepairResolved { .. } => "vct_root_repair_resolved",
            Self::BestHeaderTipLoaded { .. } => "best_header_tip_loaded",
            Self::HeaderRangeOperationCompleted { .. } => "header_range_operation_completed",
            Self::HeaderRangeOperationFailed { .. } => "header_range_operation_failed",
            Self::HeaderRangeResponseFinished { .. } => "header_range_response_finished",
            Self::HeaderRangeResponseReady { .. } => "header_range_response_ready",
        }
    }
}

/// Actions emitted by the header-sync reactor for the eventual node wiring.
#[derive(Clone, Debug)]
pub enum HeaderSyncAction {
    /// Test-only observation of a header-sync message sent through a typed session.
    #[cfg(test)]
    SendMessage {
        /// Destination peer.
        peer: ZakuraPeerId,
        /// Message that was queued.
        msg: HeaderSyncMessage,
    },
    /// Ask state to commit a contiguous header range.
    CommitHeaderRange {
        /// Exact header-commit operation represented by this action.
        operation: HeaderSyncOperationIdentity,
        /// Parent anchor hash for the first header.
        anchor: block::Hash,
        /// Checked headers and aligned per-height data to commit.
        payload: HeaderRangePayload,
        /// Whether the range is expected to be finalized by checkpoint policy.
        finalized: bool,
    },
    /// Ask state for the durable best header tip.
    QueryBestHeaderTip,
    /// Ask state for a bounded contiguous range of headers.
    QueryHeadersByHeightRange {
        /// Peer that requested the range.
        peer: ZakuraPeerId,
        /// Ordered-stream generation that requested the range.
        session_id: u64,
        /// Request ID supplied by the peer.
        request_id: HeaderSyncRequestId,
        /// First height.
        start: block::Height,
        /// Maximum count.
        count: u32,
        /// Whether the requester wants all-or-nothing tree-aux roots.
        want_tree_aux_roots: bool,
    },
    /// Ask state for missing block-body gaps.
    QueryMissingBlockBodies {
        /// First height to consider.
        from: block::Height,
        /// Maximum number of heights.
        limit: u32,
    },
    /// Report peer misbehavior to the supervisor.
    Misbehavior {
        /// Misbehaving peer.
        peer: ZakuraPeerId,
        /// Reason for reporting.
        reason: HeaderSyncMisbehavior,
    },
    /// Notify body download wiring that header-known body gaps exist.
    BodyGaps {
        /// First missing height.
        from: block::Height,
        /// Last missing height.
        to: block::Height,
    },
    /// Notify production wiring that header sync advanced its best header target.
    HeaderAdvanced {
        /// New best-header target height.
        height: block::Height,
        /// New best-header target hash.
        hash: block::Hash,
    },
    /// Notify production wiring that header sync re-anchored its best header target.
    HeaderReanchored {
        /// Previous best-header target.
        old: (block::Height, block::Hash),
        /// New best-header target.
        new: (block::Height, block::Hash),
    },
    /// Inform later block-pipeline wiring that a validated tip block arrived.
    NewBlockReceived {
        /// Source peer.
        peer: ZakuraPeerId,
        /// Block height from the coinbase transaction.
        height: block::Height,
        /// Block hash used for deduplication.
        hash: block::Hash,
        /// Full block received from the peer.
        block: Arc<block::Block>,
    },
    /// Test-only observation of an unseen valid full tip block forwarded through a typed session.
    #[cfg(test)]
    ForwardNewBlock {
        /// Source peer, if the block was received from the network.
        source: Option<ZakuraPeerId>,
        /// Destination peer.
        peer: ZakuraPeerId,
        /// Block height from the coinbase transaction.
        height: block::Height,
        /// Block hash used for deduplication.
        hash: block::Hash,
        /// Full block that was queued.
        block: Arc<block::Block>,
    },
}

/// Header-sync peer-accounting violations.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum HeaderSyncMisbehavior {
    /// Peer status is internally impossible.
    InvalidStatus,
    /// `Headers` arrived without an outstanding request.
    UnsolicitedHeaders,
    /// `Headers` was empty and made no progress.
    EmptyHeaders,
    /// `Headers` exceeded the outstanding request contract.
    ResponseTooLong,
    /// Peer supplied a range that failed state/contextual commit.
    InvalidRange,
    /// A header-sync payload was malformed before semantic handling.
    MalformedMessage,
    /// A peer sent semantic `Status` messages faster than the v1 budget.
    StatusSpam,
    /// A peer sent semantic `NewBlock` messages faster than the v1 budget.
    NewBlockSpam,
    /// A peer exceeded this node's inbound `GetHeaders` serving budget.
    GetHeadersSpam,
    /// A peer requested more headers than this node advertised it can serve.
    GetHeadersTooLong,
    /// A header-sync message came from a peer with no active header-sync state.
    UnknownPeer,
    /// A full-block tip flood failed stateless validation.
    InvalidNewBlock,
}

/// State commit failure classification returned to the reactor by node wiring.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum HeaderSyncCommitFailureKind {
    /// The supplied headers failed contextual validation or checkpoint consistency.
    InvalidPeerRange,
    /// Local storage/channel/resource failure; do not score the peer.
    Local,
}

/// Header-sync request identifier, non-zero and strictly increasing per stream session.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct HeaderSyncRequestId(u64);

impl HeaderSyncRequestId {
    /// Create a non-zero request identifier.
    pub fn new(id: u64) -> Option<Self> {
        (id != 0).then_some(Self(id))
    }

    /// Return the wire representation of this request identifier.
    pub fn get(self) -> u64 {
        self.0
    }
}

/// Exact identity of one outbound header-sync request.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct HeaderSyncWireRequestIdentity {
    /// Peer that served the request.
    pub peer: ZakuraPeerId,
    /// Ordered-stream generation that issued the request.
    pub session_id: u64,
    /// Request ID allocated within that stream session.
    pub request_id: HeaderSyncRequestId,
}

/// Kind of state operation started from a header-sync response.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub enum HeaderSyncOperationKind {
    /// Commit canonical headers to state.
    CommitHeaders,
    /// Authenticate supplied commitment roots against canonical headers.
    AuthenticateRoots,
}

/// Exact identity of one state operation started from a header-sync request.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct HeaderSyncOperationIdentity {
    /// Wire request that supplied the operation payload.
    pub wire_request: HeaderSyncWireRequestIdentity,
    /// Operation performed on that payload.
    pub op_kind: HeaderSyncOperationKind,
}

/// A single outbound `GetHeaders` range expected by a peer session.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ExpectedHeadersResponse {
    /// Request ID correlating the response with the request that solicited it.
    pub request_id: HeaderSyncRequestId,
    /// First requested height.
    pub start_height: block::Height,
    /// Requested header count.
    pub count: u32,
    /// Whether this request asked the peer to include all-or-nothing roots.
    pub want_tree_aux_roots: bool,
}

impl ExpectedHeadersResponse {
    /// Create a bounded expected response.
    pub fn new(
        request_id: HeaderSyncRequestId,
        start_height: block::Height,
        count: u32,
        want_tree_aux_roots: bool,
    ) -> Result<Self, HeaderSyncWireError> {
        validate_get_headers_count(count)?;
        Ok(Self {
            request_id,
            start_height,
            count,
            want_tree_aux_roots,
        })
    }
}
