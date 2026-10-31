//! Runtime handles of one block-sync peer routine.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use zakura_rt::{
    sync::{
        atomic::{AtomicBool, AtomicU64},
        mpsc, watch, CancellationToken, Mutex,
    },
    time::Instant,
};

use super::super::{
    events::RoutineToReactor,
    peer_registry::PeerRegistry,
    regulated::{live_requester::LiveRequester, session::ServingSession},
    sequencer_task::{SequencedBody, SequencerView},
    state::{RequestKey, ThroughputMeter, WriteOutcome},
    work_queue::{BudgetReader, RequestWriteStatus, WorkQueue},
    BlockSyncPeerSession, ZakuraPeerId, ZakuraTrace,
};
use crate::zakura::{FramedRecv, ZakuraConnId};

/// The streams, shared owners, channels, and trace handles that one peer
/// routine uses to execute its decisions. `PeerIO` makes no download, status,
/// or liveness decision of its own.
pub(super) struct PeerIO {
    /// Authorizes every response read from `recv`. The transport precheck
    /// shares this owner, so authorization happens before body decoding.
    pub(super) requester: LiveRequester,
    /// Admits this peer's `GetBlocks` requests.
    pub(super) serving: ServingSession,
    pub(super) peer: ZakuraPeerId,
    pub(super) conn_id: ZakuraConnId,
    pub(super) source: zakura_header_chain::SourceId,
    pub(super) session: BlockSyncPeerSession,
    /// This peer's ordered stream-6 frame reader. Decoded in the routine's own
    /// task; inbound never flows through the reactor (per-peer routines inverted data flow).
    pub(super) recv: FramedRecv,
    /// Live write handles of the requests in the session's window.
    pub(super) writes: RequestWrites,
    /// Last sampled fill-stop time for each bounded reason label.
    pub(super) fill_stop_trace_at: BTreeMap<&'static str, Instant>,
    /// Generation this routine was spawned with; gates its registry writes (and
    /// its `Drop`) so a superseded routine (e.g. a session replacement before the
    /// old task's async Drop runs) cannot corrupt the live entry.
    pub(super) generation: u64,
    /// Reads the budget that `work` owns. Only queue transitions change it.
    pub(super) budget: BudgetReader,
    pub(super) work: Arc<WorkQueue>,
    pub(super) registry: Arc<PeerRegistry>,
    pub(super) received_throughput: Arc<Mutex<ThroughputMeter>>,
    /// The per-peer sequencer path. A full input stalls only this routine.
    pub(super) sequencer_input: mpsc::Sender<SequencedBody>,
    pub(super) sequencer_input_bytes: Arc<AtomicU64>,
    pub(super) sequencer_input_decoded_attributed_memory_bytes: Arc<AtomicU64>,
    /// This generation's registry flag, set while the routine is blocked on
    /// its sequencer handoff (see [`HandoffBlocked`](super::HandoffBlocked)).
    pub(super) handoff_blocked: Arc<AtomicBool>,
    /// Shared routine-to-reactor channel for serving, status, re-query, and misbehavior events.
    /// Bounded `try_send` prevents a busy reactor from stalling the transport decode loop.
    pub(super) routine_to_reactor: mpsc::Sender<RoutineToReactor>,
    pub(super) sequencer_view: watch::Receiver<SequencerView>,
    /// Cancellation token for the peer's service session.
    /// Disconnect, park, or shutdown triggers the token.
    /// The routine then exits and its `Drop` guard returns work.
    pub(super) cancel: CancellationToken,
    pub(super) trace: ZakuraTrace,
    /// Receives the routine's events and snapshots, in test and simulator
    /// builds.
    #[cfg(any(test, zakura_sim))]
    pub(super) observer: super::super::block_sync_observe::ObserverSlot,
    /// Frames taken from `recv` so far.
    #[cfg(any(test, zakura_sim))]
    pub(super) frames_judged: u64,
}

/// Live write handles of this session's outstanding requests, under the keys
/// of the outstanding records in [`PeerState`](super::PeerState).
///
/// Transport changes each handle concurrently. Reading a handle observes the
/// write; settling it arbitrates a queued frame against writer startup.
#[derive(Debug, Default)]
pub(super) struct RequestWrites {
    statuses: BTreeMap<RequestKey, RequestWriteStatus>,
    /// Requests whose frame write had not started when last checked. Transport
    /// can skip only these requests.
    unstarted: BTreeSet<RequestKey>,
}

impl RequestWrites {
    /// Track the write of the request just recorded under `key`.
    pub(super) fn insert(&mut self, key: RequestKey, status: RequestWriteStatus) {
        self.unstarted.insert(key);
        self.statuses.insert(key, status);
    }

    /// Where the write of `key` stands now.
    #[cfg(any(test, zakura_sim))]
    pub(super) fn phase(
        &self,
        key: RequestKey,
    ) -> Option<super::super::work_queue::RequestWritePhase> {
        self.statuses.get(&key).map(RequestWriteStatus::phase)
    }

    /// Requests whose frame transport skipped, in issue order. Forget requests
    /// whose write has started, because transport can no longer skip them.
    pub(super) fn skipped(&mut self) -> Vec<RequestKey> {
        let statuses = &self.statuses;
        let mut skipped = Vec::new();
        self.unstarted.retain(|key| {
            let status = &statuses[key];
            if status.was_skipped() {
                skipped.push(*key);
                true
            } else {
                !status.has_started()
            }
        });
        skipped
    }

    /// Settle the write of `key` and stop tracking it. A queued frame expires
    /// here unless transport has already started writing it.
    pub(super) fn settle(&mut self, key: RequestKey) -> WriteOutcome {
        self.unstarted.remove(&key);
        let status = self
            .statuses
            .remove(&key)
            .expect("every outstanding request keeps its write handle until it settles");
        if status.expire_unwritten() || status.was_skipped() {
            WriteOutcome::Skipped
        } else {
            WriteOutcome::Started
        }
    }
}
