use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use zakura_chain::{block, parameters::Network};

use super::{HeaderSyncCodec, HeaderSyncMessage, HeaderSyncPeerSession, ZakuraHeaderSyncConfig};
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
        }
    }
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
