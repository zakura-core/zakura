//! Per-peer pipe-routine for Zakura block sync.
//!
//! A per-peer routine inverts the inbound data flow. One task owns each connected
//! peer's `FramedRecv`. The task decodes each stream-6 frame and runs the download
//! logic directly. The reactor does not demultiplex inbound frames or create a
//! per-peer `PeerInput` channel. The routine sends only shared concerns to the
//! reactor through [`RoutineToReactor`]. These concerns include `GetBlocks`
//! serving, status advertisements, producer re-query pings, and serving-side
//! misbehavior. The routine owns its `BlockSyncPeerSession`, outstanding requests,
//! adaptive outbound window, timeout-recovery slots, servable caps, and fill loop.
//!
//! The per-peer task runs the throughput-critical matched-body
//! `sequencer_input.send(..).await`. Sequencer backpressure therefore stalls only
//! one routine. The download decision uses the byte budget and per-peer slots.
//! `take_in_range(servable_low, servable_high, n)` uses `servable_high` as its
//! upper bound.
//!
//! The routine or shared [`PeerRegistry`] owns all per-peer download state. The
//! routine receives inbound traffic from its own `FramedRecv`. Its fill loop,
//! matched-body path, and unmatched-body paths run in the same task.

use std::collections::BTreeMap;

use zakura_rt::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        futures::Notified,
        mpsc, watch, CancellationToken, Mutex,
    },
    time::{self, Instant},
};

use super::events::RoutineToReactor;
use super::{
    admission::{admit_received_body, AdmissionSnapshot},
    peer_registry::{hard_outbound_capacity, PeerRegistry},
    pipe::block_sync_guard,
    reactor::tolerated_bytes,
    regulated::{wire::Outcome, CompletedRange},
    reorder::BufferedBlockBody,
    sequencer_task::{charge_decoded_handoff, SequencedBody, SequencerView},
    state::{DownloadWindow, OutstandingBlockRange, RequestKey, ThroughputMeter, WriteOutcome},
    work_queue::{WorkQueue, WorkReturnOutcome},
    BlockSyncMisbehavior, BlockSyncPeerSession, BlockSyncStatus, ZakuraBlockSyncConfig,
    ZakuraPeerId, ZakuraTrace,
};
use crate::zakura::transport::OrderedStreamFailure;
use crate::zakura::{trace::BlockBodySource, Admit, FramedRecv, SinkReject, ZakuraConnId};
use std::{sync::Arc, time::Duration};
use zakura_chain::{block, serialization::ZcashSerialize};

mod fill;
#[cfg(any(test, feature = "p2p-fuzz"))]
pub use fill::{replay_block_fill_bytes, BlockFillReplaySummary};
mod io;
#[cfg(test)]
mod requester_tests;
#[cfg(test)]
mod scaling_tests;
mod state;
mod trace;

use fill::FillStop;
use io::PeerIO;
#[cfg(any(test, zakura_sim, feature = "zakura-testkit"))]
use state::check_requested_hash;
use state::{
    unreceived_heights, BodyMatch, Disposition, LivenessDecision, MatchedBody, NoProgressResponse,
    PeerState, SharedDeadlines, StatusUpdate,
};

/// Heights checked for lost ownership per work-queue lock hold.
const OWNERSHIP_CHECK_CHUNK: usize = 1024;

/// How long a routine avoids a height after returning it because of a failure.
/// The delay lets another routine take the height first on the single-threaded
/// test runtime. The queue keeps the height pending for every other peer.
const RETRY_AVOID_BACKOFF: Duration = Duration::from_millis(50);
/// Poll interval while this peer's outbound stream queue is full.
const OUTBOUND_FULL_POLL_INTERVAL: Duration = Duration::from_millis(10);
/// Cadence of the per-peer BBR heartbeat trace (`block_peer_bbr`).
/// The trace records controller state while a peer is idle between deliveries.
const BBR_TRACE_INTERVAL: Duration = Duration::from_secs(10);
/// Minimum interval between repeated fill-stop trace rows for the same peer and reason.
///
/// The counter remains exact. The JSONL trace samples steady-state refusal details.
/// Without this bound, idle peers can emit a row on every wake and consume hundreds
/// of megabytes per minute during initial sync.
const FILL_STOP_TRACE_INTERVAL: Duration = Duration::from_secs(10);

fn fill_stop_trace_due(last: Option<Instant>, now: Instant) -> bool {
    last.is_none_or(|last| now.saturating_duration_since(last) >= FILL_STOP_TRACE_INTERVAL)
}

const PARK_BLOCK_SYNC_NO_BLOCK_PROGRESS: &str = "block_sync_no_block_progress";

/// Records decoded-memory metrics for an accepted block and returns its attributed size.
/// The decoded-to-serialized ratio is omitted when the wire size is missing or zero.
fn record_decoded_memory_size(block: &block::Block, body_wire_bytes: Option<u64>) -> u64 {
    let decoded_attributed_memory_size_bytes = block.attributed_memory_size_bytes();
    // Metrics accepts f64 samples; these lossy conversions are observability-only.
    metrics::histogram!(
        "sync.block.body.decoded.attributed_memory_size_bytes",
        "stage" => "peer"
    )
    .record(decoded_attributed_memory_size_bytes as f64);
    if let Some(serialized_bytes) = body_wire_bytes.filter(|bytes| *bytes > 0) {
        metrics::histogram!(
            "sync.block.body.decoded.to_serialized_ratio",
            "stage" => "peer"
        )
        .record(decoded_attributed_memory_size_bytes as f64 / serialized_bytes as f64);
    }
    decoded_attributed_memory_size_bytes
}

/// Whether a body's transactions are the ones its header commits to: their
/// Merkle root matches, and no transaction repeats (CVE-2012-2459). A conformant
/// peer serves the block its header names, so a mismatch is unambiguous. It is
/// checked before the body enters the pipeline, because a body of junk
/// transactions can decode to many times the memory of the real block.
fn transactions_match_header(block: &block::Block) -> bool {
    let hashes: Vec<_> = block
        .transactions
        .iter()
        .map(|transaction| transaction.hash())
        .collect();
    let root: block::merkle::Root = hashes.iter().copied().collect();
    root == block.header.merkle_root && hashes.iter().collect::<HashSet<_>>().len() == hashes.len()
}

/// Why a routine rejects a decoded body when its frame arrives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FrameBodyReject {
    /// Its coinbase height isn't the height its frame was authorized for.
    CoinbaseHeight,
    /// Its transactions aren't the ones its header commits to.
    Transactions,
}

/// The checks a routine runs, in order, on a body decoded from a frame
/// authorized for `height`.
#[inline]
fn check_frame_body(block: &block::Block, height: block::Height) -> Result<(), FrameBodyReject> {
    if block.coinbase_height() != Some(height) {
        return Err(FrameBodyReject::CoinbaseHeight);
    }
    if !transactions_match_header(block) {
        return Err(FrameBodyReject::Transactions);
    }
    Ok(())
}

/// Whether a routine accepts a decoded body from a frame authorized for
/// `height`, when its outstanding request expects `expected` there: the
/// checks on frame receipt, then the requested-hash check in
/// `PeerState::match_body`. Controlled storage runs fixture bodies through
/// these checks.
#[cfg(any(test, zakura_sim, feature = "zakura-testkit"))]
pub fn routine_accepts_body(
    block: &block::Block,
    height: block::Height,
    expected: Option<block::Hash>,
) -> bool {
    check_frame_body(block, height).is_ok() && check_requested_hash(expected, block.hash()).is_ok()
}

fn header_hash_payload_mismatch(
    owner: zakura_header_chain::BodyWorkOwner,
    source: zakura_header_chain::SourceId,
    requested: block::Hash,
    delivered: block::Hash,
) -> zakura_header_chain::BodyPayloadMismatch {
    let mut hasher = blake2b_simd::Params::new()
        .hash_length(32)
        .personal(b"ZkBodyMismatch1_")
        .to_state();
    hasher.update(&owner.header_generation.get().to_le_bytes());
    hasher.update(&owner.verified_generation.get().to_le_bytes());
    hasher.update(&owner.branch.anchor_hash.0);
    hasher.update(&owner.branch.target_tip_hash.0);
    hasher.update(&owner.session_id.to_le_bytes());
    hasher.update(&owner.request_id.get().to_le_bytes());
    hasher.update(&source.digest());
    hasher.update(&requested.0);
    hasher.update(&delivered.0);
    zakura_header_chain::BodyPayloadMismatch {
        evidence: zakura_header_chain::EvidenceId::from_digest(
            hasher
                .finalize()
                .as_bytes()
                .try_into()
                .expect("the configured payload-mismatch digest is exactly 32 bytes"),
        ),
        requested,
        delivered,
        kind: zakura_header_chain::BodyCommitmentKind::HeaderHash,
        source,
    }
}

/// Keeps a receipt charge live until the synchronous send finishes or unwinds.
struct ReceiptSettlement {
    funding: Option<crate::zakura::transport::ByteReservation>,
    #[cfg(any(test, zakura_sim))]
    observer: super::block_sync_observe::ObserverSlot,
    #[cfg(any(test, zakura_sim))]
    peer: ZakuraPeerId,
    #[cfg(any(test, zakura_sim))]
    owner: zakura_header_chain::BodyWorkOwner,
    #[cfg(any(test, zakura_sim))]
    height: block::Height,
}

impl ReceiptSettlement {
    fn new(
        routine: &PeerRoutine,
        owner: zakura_header_chain::BodyWorkOwner,
        height: block::Height,
        funding: crate::zakura::transport::ByteReservation,
    ) -> Self {
        #[cfg(any(test, zakura_sim))]
        routine.observe_handoff_started(owner, height, funding.bytes());
        #[cfg(not(any(test, zakura_sim)))]
        let _ = (routine, owner, height);
        Self {
            funding: Some(funding),
            #[cfg(any(test, zakura_sim))]
            observer: routine.io.observer.clone(),
            #[cfg(any(test, zakura_sim))]
            peer: routine.io.peer.clone(),
            #[cfg(any(test, zakura_sim))]
            owner,
            #[cfg(any(test, zakura_sim))]
            height,
        }
    }
}

impl Drop for ReceiptSettlement {
    fn drop(&mut self) {
        let funding = self
            .funding
            .take()
            .expect("receipt owns its funding until drop");
        #[cfg(not(any(test, zakura_sim)))]
        drop(funding);
        #[cfg(any(test, zakura_sim))]
        {
            let charge = funding.bytes();
            funding.release_then(|| {
                observe!(
                    self.observer,
                    super::block_sync_observe::BlockEvent::HandoffEnded {
                        peer: self.peer.clone(),
                        owner: self.owner,
                        height: self.height,
                        charge,
                    }
                );
            });
        }
    }
}

/// Marks a routine blocked on its sequencer handoff, from creation until
/// drop. Other routines skip a blocked routine as a floor carrier
/// ([`PeerRegistry::floor_has_preferred_unsaturated_server`]), because it
/// can't request the floor until its handoff completes. Dropping the guard,
/// including by cancelling the wait, clears the flag.
struct HandoffBlocked<'a>(&'a AtomicBool);

impl<'a> HandoffBlocked<'a> {
    /// Sets `routine`'s flag. A routine that published a free slot may be
    /// some other routine's preferred floor carrier, so this wakes them to
    /// rank the floor again.
    fn start(routine: &'a PeerRoutine) -> Self {
        routine.io.handoff_blocked.store(true, Ordering::Relaxed);
        if routine.state.published_free_slots {
            routine.io.registry.notify_floor_ranking_changed();
        }
        HandoffBlocked(&routine.io.handoff_blocked)
    }
}

impl Drop for HandoffBlocked<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Relaxed);
    }
}

/// The per-peer pipe-routine: one task per connected peer, spawned at the pipe
/// spawn point (`service::add_peer`) so a protocol reject cancels the whole
/// connection. [`PeerState`] decides from the session's facts, and [`PeerIO`]
/// executes those decisions with the runtime handles.
pub(super) struct PeerRoutine {
    config: ZakuraBlockSyncConfig,
    state: PeerState,
    io: PeerIO,
}

impl PeerRoutine {
    /// Build a pipe-routine for `peer`. The caller (`service::add_peer`) drives
    /// `run()` inside `spawn_supervised_pipe` so a protocol reject cancels the
    /// whole connection. `generation` is the value obtained from
    /// [`PeerRegistry::admit_session`](super::peer_registry::PeerRegistry::admit_session).
    /// `requester` authorizes every response read from `recv`, and `serving`
    /// admits this peer's `GetBlocks`.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        peer: ZakuraPeerId,
        conn_id: ZakuraConnId,
        session: BlockSyncPeerSession,
        recv: FramedRecv,
        requester: super::regulated::live_requester::LiveRequester,
        serving: super::regulated::session::ServingSession,
        config: ZakuraBlockSyncConfig,
        allow_no_progress_park: bool,
        generation: u64,
        work: Arc<WorkQueue>,
        registry: Arc<PeerRegistry>,
        received_throughput: Arc<Mutex<ThroughputMeter>>,
        sequencer_input: mpsc::Sender<SequencedBody>,
        sequencer_input_bytes: Arc<AtomicU64>,
        sequencer_input_decoded_attributed_memory_bytes: Arc<AtomicU64>,
        routine_to_reactor: mpsc::Sender<RoutineToReactor>,
        sequencer_view: watch::Receiver<SequencerView>,
        cancel: CancellationToken,
        trace: ZakuraTrace,
    ) -> Self {
        let source_digest: [u8; 32] = peer.as_bytes().try_into().expect(
            "block-sync peers have 32-byte identities because they are authenticated Iroh nodes",
        );
        let source = zakura_header_chain::SourceId::from_digest(source_digest);
        let state = PeerState::new(
            &config,
            allow_no_progress_park,
            sequencer_view.borrow().reset_epoch,
            work.revocations(),
        );
        let handoff_blocked = registry.handoff_blocked_flag(&peer, generation);
        PeerRoutine {
            io: PeerIO {
                requester,
                serving,
                peer,
                conn_id,
                source,
                session,
                recv,
                writes: io::RequestWrites::default(),
                fill_stop_trace_at: BTreeMap::new(),
                generation,
                budget: work.budget(),
                work,
                registry,
                received_throughput,
                sequencer_input,
                sequencer_input_bytes,
                sequencer_input_decoded_attributed_memory_bytes,
                handoff_blocked,
                routine_to_reactor,
                sequencer_view,
                cancel,
                trace,
                #[cfg(any(test, zakura_sim))]
                observer: Default::default(),
                #[cfg(any(test, zakura_sim))]
                frames_judged: 0,
            },
            config,
            state,
        }
    }

    /// This routine, reporting to `observer`.
    #[cfg(any(test, zakura_sim))]
    pub(super) fn with_observer(
        mut self,
        observer: super::block_sync_observe::ObserverSlot,
    ) -> Self {
        self.io.observer = observer;
        self
    }

    /// Reports the routine's owner state and the work queue's items, if an
    /// observer is installed. `ended` marks the routine's last snapshot.
    ///
    /// Each request lists its received heights and the unreceived heights
    /// the work queue still assigns to it. Another actor's revocation
    /// releases a height's charge at once, but the routine drops the height
    /// only at its next fill (`gc_obsolete_outstanding`), which a full
    /// outbound queue skips. The snapshot applies the same check now, so it
    /// lists only heights the budget still counts.
    #[cfg(any(test, zakura_sim))]
    fn observe_owners(&self, ended: bool) {
        use super::block_sync_observe::{BlockEvent, RegistryClaim, RoutineSnapshot};
        let mut requests = self.request_ownership();
        let unreceived: Vec<_> = requests
            .iter()
            .flat_map(|request| {
                request
                    .heights
                    .iter()
                    .filter(|height| !height.received)
                    .map(|height| (height.height, request.owner))
            })
            .collect();
        if !unreceived.is_empty() {
            let disowned = self.io.work.disowned(&unreceived, |owner| *owner);
            for request in &mut requests {
                request
                    .heights
                    .retain(|height| !disowned.contains(&(height.height, request.owner)));
            }
        }
        observe!(
            self.io.observer,
            BlockEvent::Routine(RoutineSnapshot {
                peer: self.io.peer.clone(),
                conn_id: self.io.conn_id,
                generation: self.io.generation,
                requests,
                claims: self
                    .io
                    .registry
                    .outstanding_snapshot(&self.io.peer, self.io.generation)
                    .into_iter()
                    .map(|(height, meta)| RegistryClaim {
                        height,
                        owner: meta.owner,
                        estimated_bytes: meta.estimated_bytes,
                    })
                    .collect(),
                serving: self.io.serving.admitted_ranges(),
                ended,
            })
        );
        self.io.work.observe_items();
    }

    /// Each outstanding request with the write phase of its live handle.
    #[cfg(any(test, zakura_sim))]
    fn request_ownership(&self) -> Vec<super::state::RequestOwnership> {
        self.state
            .window
            .outstanding
            .ownership(|key| self.io.writes.phase(key))
    }

    /// Run the pipe-routine until stream close, cancellation, or a protocol
    /// reject. A reject returns `Err(SinkReject::protocol(..))` so the supervised
    /// pipe tears the whole connection down.
    pub(super) async fn run(mut self) -> Result<(), SinkReject> {
        let mut guard = block_sync_guard();
        // A stream replacement keeps the same connection advertisement. Waiting
        // for a fresh one would stall requests until the next cadence interval.
        if let Some(status) = self.io.session.received_status() {
            self.handle_status(status);
        }
        let result = self.run_inner(&mut guard).await;
        // Traced here rather than in the liveness check, which runs on every
        // loop iteration.
        if matches!(result, Err(SinkReject::Local(_)))
            && self
                .io
                .requester
                .retirement_deadline(self.config.effective_liveness_timeout())
                .is_some_and(|deadline| Instant::now() >= deadline)
        {
            self.trace_ending_timeout();
        }
        // A transport failure can cancel the session before its queued responses
        // reach us. Validate them under the existing decode-capacity bound before
        // scoring unanswered work. Connection shutdown can still stop this drain.
        if result.is_ok() {
            if let Some(failure) = self.io.recv.failure() {
                self.io.recv.close();
                while let Ok(frame) = self.io.recv.try_recv() {
                    self.handle_frame(&mut guard, frame).await?;
                }
                return self.handle_stream_failure(Instant::now(), failure);
            }
        }
        result
    }

    async fn run_inner(
        &mut self,
        guard: &mut crate::zakura::SessionGuard,
    ) -> Result<(), SinkReject> {
        // Local clones so the `Notified` futures below borrow these handles, not
        // `self` — `self.try_fill()` needs `&mut self` while the notifications are
        // pinned. The budget reader observes releases from this queue, including
        // receipt reservations that outlive a queue transition.
        let budget = self.io.budget.clone();
        let work = self.io.work.clone();
        let registry = Arc::clone(&self.io.registry);
        let request_pool = self.io.requester.pool.clone();
        // Per-peer BBR heartbeat cadence. `Skip` so a routine busy past a tick emits one
        // fresh sample rather than a catch-up burst. Observability only.
        let mut bbr_trace_ticks = time::interval(BBR_TRACE_INTERVAL);
        bbr_trace_ticks.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        loop {
            if self.io.cancel.is_cancelled() {
                return Ok(());
            }
            // missed-wake safety: register all `Notify`s via
            // `Notified::enable()` BEFORE the fill attempt. These
            // `Notify`s use `notify_waiters` (no stored permit), so a
            // release/extend that lands between the fill-check and the await
            // would be lost if we registered after — the routine would stall.
            let capacity = budget.subscribe_capacity().notified();
            let available = work.subscribe_available().notified();
            let floor_ranking = registry.subscribe_floor_ranking().notified();
            zakura_rt::pin!(capacity);
            zakura_rt::pin!(available);
            zakura_rt::pin!(floor_ranking);
            Notified::enable(capacity.as_mut());
            Notified::enable(available.as_mut());
            Notified::enable(floor_ranking.as_mut());

            // Check before the biased receive arm so incoming traffic cannot
            // keep abandoned authorization alive or postpone request timeouts.
            let now = Instant::now();
            self.check_request_liveness(now)?;
            if self.state.deadlines_due(now) {
                self.handle_deadlines(now)?;
            }
            self.flush_pending_status();
            let retry_filter_deadline = if self.io.session.outbound_capacity() > 0 {
                self.try_fill().retry_at
            } else {
                self.gc_skipped_outstanding();
                None
            };
            let outbound_queue_has_capacity = self.io.session.outbound_capacity() > 0;
            // Track the start of the current continuous outbound-full stretch so the
            // liveness check can bound the write-congestion grace: a peer that stopped
            // reading holds this full until `outbound_full_since` ages past
            // `request_timeout`, at which point it is disconnected rather than excused.
            self.state
                .note_outbound_capacity(outbound_queue_has_capacity, Instant::now());

            // Sleep until the earliest outstanding deadline (own-timeout arm).
            let timeout = self.earliest_deadline_sleep(retry_filter_deadline);
            zakura_rt::pin!(timeout);
            let outbound_queue_poll = time::sleep(OUTBOUND_FULL_POLL_INTERVAL);
            zakura_rt::pin!(outbound_queue_poll);

            // The routine's snapshot, once per iteration, after the deadlines
            // and the fill above have reserved or released request bytes. The
            // routine can park below for a whole timeout, so a snapshot here
            // lets an observer reconcile the budget with every routine's
            // outstanding estimates while the run rests.
            #[cfg(any(test, zakura_sim))]
            self.observe_owners(false);
            zakura_rt::select! {
                biased;
                _ = self.io.cancel.cancelled() => return Ok(()),
                frame = self.io.recv.recv() => {
                    match frame {
                        // Decode the frame and run the download/serving dispatch
                        // in this same task. A protocol reject propagates out so
                        // the supervised pipe cancels the connection; the `Drop`
                        // guard returns unreceived work on the way out.
                        Some(frame) => self.handle_frame(guard, frame).await?,
                        // Stream closed by the peer. With no outstanding work this
                        // is a clean exit; with unanswered requests it is a
                        // no-progress stall (park or disconnect). `Drop` returns
                        // unreceived outstanding heights and releases their budget.
                        None => return self.handle_stream_failure(
                            Instant::now(),
                            self.io.recv.failure().unwrap_or(OrderedStreamFailure::RemoteClose),
                        ),
                    }
                }
                changed = self.io.sequencer_view.changed() => {
                    match changed {
                        Ok(()) => self.on_view_changed(),
                        // The Sequencer task ended (shutdown); the routine follows.
                        Err(_) => return Ok(()),
                    }
                }
                _ = &mut timeout => self.handle_deadlines(Instant::now())?,
                _ = &mut capacity => {
                    self.trace_wake("budget_capacity");
                }
                _ = &mut available => {
                    self.trace_wake("work_added");
                }
                _ = &mut floor_ranking => {
                    self.trace_wake("floor_ranking_changed");
                }
                entry = request_pool.entry(), if self.state.request_pool_waiting => { drop(entry); }
                _ = bbr_trace_ticks.tick() => self.trace_bbr_sample(),
                _ = &mut outbound_queue_poll, if !outbound_queue_has_capacity => {}
            }
        }
    }

    /// Admit, decode, and dispatch one inbound frame in this task. Authorization
    /// decodes each control frame once and consumes an ending's reservation.
    /// `Block` and both endings (download) are handled locally; `Status`
    /// updates own servable/caps locally and pings the reactor to advertise;
    /// `GetBlocks` goes to serving admission; a malformed body reports
    /// `MalformedMessage` and rejects the peer.
    async fn handle_frame(
        &mut self,
        guard: &mut crate::zakura::SessionGuard,
        frame: crate::zakura::Frame,
    ) -> Result<(), SinkReject> {
        #[cfg(any(test, zakura_sim))]
        {
            self.io.frames_judged += 1;
            observe!(
                self.io.observer,
                super::block_sync_observe::BlockEvent::FrameJudged {
                    peer: self.io.peer.clone(),
                    conn_id: self.io.conn_id,
                    generation: self.io.generation,
                    seq: self.io.frames_judged,
                    message_type: frame.message_type,
                    flags: frame.flags,
                    payload_len: frame.payload.len(),
                }
            );
        }
        self.gc_skipped_outstanding();
        match guard.admit(&frame) {
            Admit::Pass => {}
            Admit::Throttle => {
                return Err(SinkReject::local(
                    "block-sync guard unexpectedly throttled an inbound frame",
                ));
            }
            Admit::Reject(reason) => {
                return Err(SinkReject::protocol(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    reason,
                )));
            }
        }

        use super::regulated::live_requester::Authorization;
        let was_stopped = self.requester_stop().is_some();
        let authorization = self.io.requester.authorize(&frame)?;
        // A last body or an ending can close or open the requester. Other
        // routines must re-rank the floor now, not at this window's next change.
        if self.requester_stop().is_some() != was_stopped {
            self.publish_slot_diagnostics();
        }
        let height = match authorization {
            Authorization::Block(height) => height,
            Authorization::Status(status) => {
                self.trace_message_received("status");
                self.handle_status(status);
                return Ok(());
            }
            Authorization::GetBlocks(range) => {
                self.trace_message_received("get_blocks");
                return self.io.serving.admit(range);
            }
            Authorization::Ended(completed) => {
                self.trace_message_received(match completed.outcome {
                    Outcome::Done => "blocks_done",
                    Outcome::Unavailable => "range_unavailable",
                });
                self.handle_ending(completed);
                return Ok(());
            }
            Authorization::IncompatibleHeader => {
                // Height-only requests cannot identify which live range the
                // unexpected header belongs to. Defer each still-wanted
                // body for this supplier without delaying other suppliers.
                let bodies = self
                    .io
                    .requester
                    .unreceived_expected(super::peer_registry::MAX_SUPPLIER_RETRIES)
                    .into_iter()
                    .filter_map(|(height, hash)| {
                        self.io
                            .work
                            .item_for_height(height)
                            .filter(|item| item.hash == hash)
                            .map(|item| (item.scope, hash))
                    });
                self.io.registry.defer_incompatible_bodies(
                    self.io.source,
                    bodies,
                    Instant::now() + self.config.effective_liveness_timeout(),
                );
                return Err(SinkReject::local(
                    "GetBlocks header does not match the next expected header",
                ));
            }
        };
        let body_permit = {
            let permit = self.reserve_body_decode_permit();
            zakura_rt::pin!(permit);
            zakura_rt::select! {
                biased;
                () = self.io.cancel.cancelled() => {
                    if self.io.recv.failure().is_none() {
                        return Ok(());
                    }
                    // A remote close cannot excuse an unvalidated body. Keep
                    // this bounded raw frame until its decode slot is available.
                    permit.await?
                }
                permit = &mut permit => permit?,
            }
        };
        // Authorization checked the envelope, so the body is the payload after
        // its type byte. Measured here, on the per-peer task, so the body size
        // never has to be recomputed by re-serializing the block on another thread.
        let body_wire_bytes = u64::try_from(
            frame
                .payload
                .len()
                .saturating_sub(super::wire::BLOCK_SYNC_MESSAGE_TYPE_BYTES),
        )
        .unwrap_or(u64::MAX);
        let (block, raw_block_payload) = match super::wire::decode_block_payload(frame.payload) {
            Ok(decoded) => decoded,
            Err(error) => {
                // A malformed body is `MalformedMessage` misbehavior AND a fatal
                // protocol reject for the whole connection. Report via the shared
                // channel, then reject; the report is best-effort and never blocks.
                let protocol_error =
                    std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string());
                tracing::debug!(peer = ?self.io.peer, ?error, "malformed Zakura block-sync frame");
                let _ = self
                    .io
                    .routine_to_reactor
                    .try_send(RoutineToReactor::Misbehavior {
                        peer: self.io.peer.clone(),
                        reason: BlockSyncMisbehavior::MalformedMessage,
                    });
                return Err(SinkReject::protocol(protocol_error));
            }
        };
        self.trace_message_received("block");
        match check_frame_body(&block, height) {
            Ok(()) => {}
            Err(FrameBodyReject::CoinbaseHeight) => {
                return Err(SinkReject::protocol(
                    "authorized block has a different coinbase height",
                ));
            }
            Err(FrameBodyReject::Transactions) => {
                let _ = self
                    .io
                    .routine_to_reactor
                    .try_send(RoutineToReactor::Misbehavior {
                        peer: self.io.peer.clone(),
                        reason: BlockSyncMisbehavior::InvalidBlock,
                    });
                return Err(SinkReject::protocol(
                    "authorized block's transactions do not match its header",
                ));
            }
        }
        self.trace_wake("own_body");
        self.handle_body(block, Some(body_wire_bytes), body_permit, raw_block_payload)
            .await;
        Ok(())
    }

    /// Return only unreceived work still owned by this routine. A body already
    /// handed to the sequencer keeps its ownership and cannot be requeued here.
    fn return_unreceived_requests(&mut self, reason: &'static str) {
        let outstanding_ranges = self.state.window.outstanding.drain();
        for (key, outstanding) in outstanding_ranges {
            let write = self.io.writes.settle(key);
            self.retire_authorization(&outstanding, write);
            let unreceived: Vec<_> = unreceived_heights(&outstanding).collect();
            let outcome = self
                .io
                .work
                .release_reserved_and_return_items_detailed_for_owner(
                    outstanding.request.owner,
                    unreceived.iter().copied(),
                );
            self.trace_work_returned(reason, &outstanding, unreceived.len(), outcome);
        }
        self.io
            .registry
            .clear_outstanding(&self.io.peer, self.io.generation);
    }

    async fn reserve_body_decode_permit(
        &self,
    ) -> Result<mpsc::OwnedPermit<SequencedBody>, SinkReject> {
        let _read_pause = self.io.requester.pause_reading();
        let capacity_before = self.io.sequencer_input.capacity();
        let started = time::Instant::now();
        let closed = || SinkReject::local("block-sync sequencer body input closed");
        let permit = match self.io.sequencer_input.clone().try_reserve_owned() {
            Ok(permit) => permit,
            Err(mpsc::error::TrySendError::Full(sequencer_input)) => {
                // The input queue is full, so this routine parks until the
                // sequencer drains a body.
                let _blocked = HandoffBlocked::start(self);
                sequencer_input
                    .reserve_owned()
                    .await
                    .map_err(|_| closed())?
            }
            Err(mpsc::error::TrySendError::Closed(_)) => return Err(closed()),
        };
        self.trace_body_decode_permit(started.elapsed(), capacity_before);
        Ok(permit)
    }

    /// Apply this peer's `Status` through [`PeerState::on_status`]. When the
    /// meter allows publication, write it to the registry and ping the reactor
    /// to advertise our reply and republish the candidate.
    fn handle_status(&mut self, status: BlockSyncStatus) {
        self.io.session.record_received_status(status);
        let update = self.state.on_status(status, Instant::now());
        // Publish a stop even when the meter defers the status, so no routine
        // keeps deferring the floor to this peer. A reopen waits for a
        // rate-limited publication, so a Status flood cannot toggle the shared
        // ranking on every frame.
        if self.requester_stop().is_some() && !self.state.published_requester_stopped {
            self.publish_slot_diagnostics();
        }
        let StatusUpdate::Publish { send_reply } = update else {
            return;
        };
        if self.requester_stop().is_some() != self.state.published_requester_stopped {
            self.publish_slot_diagnostics();
        }
        // Publish the servable range / clamped caps / received_status to the
        // registry so the reactor's candidate reads see them; generation-gated.
        self.io
            .registry
            .upsert_status(&self.io.peer, self.io.generation, status);
        self.trace_status_received(status);
        // Ask the reactor to advertise our Status reply (if due) and republish the
        // candidate. Best-effort; a full channel just defers the candidate refresh
        // to the next reactor tick.
        let _ = self
            .io
            .routine_to_reactor
            .try_send(RoutineToReactor::StatusReceived {
                peer: self.io.peer.clone(),
                send_reply,
            });
    }

    fn flush_pending_status(&mut self) {
        if let Some(status) = self.state.take_due_status(Instant::now()) {
            self.handle_status(status);
        }
    }

    /// React to a committed-view change: refresh the floor/tip the routine reads,
    /// and on a destructive `reset_epoch` bump clear this routine's outstanding
    /// **in place** (return unreceived heights to `work.pending`, release their
    /// budget, clear the registry outstanding, drop retry-avoid) and re-fan from
    /// the post-`reset_above` `WorkQueue`. The transport is never torn down:
    /// reset clears outstanding work in place instead of respawning the routine.
    fn on_view_changed(&mut self) {
        let reset_epoch = self.io.sequencer_view.borrow().reset_epoch;
        if !self.state.observe_reset_epoch(reset_epoch) {
            // A non-destructive advance: the floor/tip the routine reads come
            // straight from the live `view` each time they are needed, so nothing
            // to do but let the want-work loop re-run at the top (a committed
            // floor advance may GC our fully-committed outstanding).
            return;
        }
        self.trace_wake("view_reset");
        // The Sequencer already pinned its floor/tip and `work.reset_above`'d the
        // dropped successor heights. Return our unreceived outstanding to
        // `work.pending` (a no-op for heights already dropped from `in_flight` by
        // `reset_above`) and release their reservations exactly once.
        self.return_unreceived_requests("view_reset");
        // `return_unreceived_requests` cleared our registry claims.
        self.publish_slot_diagnostics();
        // A destructive reset pulled this peer's outstanding on our initiative, so
        // neither its retry bias nor its no-progress probe streak may stay charged.
        self.state.note_locally_returned_requests();
        // Ping the producer immediately: `reset_above` emptied `pending`, and the
        // reactor's post-reset query may have run while our (now cleared) outstanding
        // still inflated the low-water gate. Without this ping a routine that then
        // sleeps on an empty deadline set would leave the pipeline dry.
        let _ = self
            .io
            .routine_to_reactor
            .try_send(RoutineToReactor::RequeryNeeded);
        // The want-work loop re-fans from the queue at the top of the next
        // iteration (the `reset_above` + producer re-query repopulate `pending`).
    }

    /// Sleep future resolving at the earliest wake the routine schedules for
    /// itself: the soonest outstanding request deadline (own-timeout), block
    /// liveness deadline, pending status refresh, or the soonest retry-avoid expiry
    /// (local failure bias or registry-owned floor-watchdog hard exclude), so a routine that quiet-returned
    /// its only work re-runs want-work once the bias lifts even if no external event
    /// arrives. Defaults to a long idle sleep when none exists.
    fn earliest_deadline_sleep(&self, retry_filter_deadline: Option<Instant>) -> time::Sleep {
        let now = Instant::now();
        let floor_watchdog_avoid = self
            .io
            .registry
            .next_floor_avoid_deadline(&self.io.peer, now);
        let body_retry_avoid = self
            .io
            .registry
            .next_body_retry_deadline(&self.io.peer, now);
        let earliest = self.state.next_wake(SharedDeadlines {
            requester_retirement: self
                .io
                .requester
                .retirement_deadline(self.config.effective_liveness_timeout()),
            floor_watchdog_avoid,
            body_retry_avoid,
            retry_filter: retry_filter_deadline,
        });
        match earliest {
            // Floor the wait at the deadline so a far-future request still wakes
            // promptly; an already-due deadline wakes immediately.
            Some(deadline) => time::sleep(deadline.saturating_duration_since(now)),
            None => time::sleep(Duration::from_secs(3600)),
        }
    }

    /// Read the admission inputs and publish the above-floor reservation that
    /// admission compares with the look-ahead limit.
    fn admission_snapshot(&self, view: &SequencerView) -> AdmissionSnapshot {
        let (reserved_above_floor_bytes, reserved_above_floor_blocks) =
            self.io.work.reserved_above(view.download_floor);
        let snapshot = AdmissionSnapshot {
            download_floor: view.download_floor,
            verified_block_tip: view.verified_tip,
            reorder_buffered_bytes: view.reorder_buffered_bytes,
            reorder_buffered_blocks: view.reorder_len,
            applying_buffered_bytes: view.applying_buffered_bytes,
            applying_buffered_blocks: view.applying_len,
            sequencer_input_queued_bytes: self.io.sequencer_input_bytes.load(Ordering::Relaxed),
            in_flight_submission_bytes: view.in_flight_submission_bytes,
            decoded_resident_bytes: self
                .io
                .sequencer_input_decoded_attributed_memory_bytes
                .load(Ordering::Relaxed)
                .saturating_add(view.reorder_decoded_attributed_memory_bytes)
                .saturating_add(view.applying_decoded_attributed_memory_bytes),
            reserved_above_floor_bytes,
            reserved_above_floor_blocks,
            budget_available: self.io.budget.available(),
        };
        // This gauge records an admission input, so it changes only here. This
        // lossy cast is metrics-only; admission keeps the integer value.
        metrics::gauge!("sync.block.work.reserved_above_floor_bytes")
            .set(snapshot.reserved_above_floor_bytes as f64);
        snapshot
    }

    /// Record heights this routine just returned on a failure so it will not
    /// immediately re-grab them (the peer-local retry bias). The heights stay
    /// `pending` and contestable by every other peer; only this routine defers.
    fn note_retry_avoid(&mut self, heights: impl IntoIterator<Item = block::Height>) {
        let until = Instant::now() + RETRY_AVOID_BACKOFF;
        self.state.avoid_retry(heights, until);
    }

    // ===================== own-timeout arm (ports `expire_due_timeouts`) =====

    fn check_request_liveness(&self, now: Instant) -> Result<(), SinkReject> {
        self.io
            .requester
            .check_liveness(now, self.config.effective_liveness_timeout())
    }

    fn handle_deadlines(&mut self, now: Instant) -> Result<(), SinkReject> {
        self.check_request_liveness(now)?;
        let rescued_timed_out = self.expire_due_timeouts(now);
        if rescued_timed_out && self.io.session.outbound_capacity() > 0 {
            self.try_fill();
        }
        self.check_block_liveness(now)
    }

    fn expire_due_timeouts(&mut self, now: Instant) -> bool {
        self.gc_skipped_outstanding();
        // Arbitrate due queued frames against writer startup before charging
        // failures.
        let due: Vec<_> = self
            .state
            .due_requests(now)
            .into_iter()
            .map(|key| (key, self.io.writes.settle(key)))
            .collect();
        let expired = self.state.expire_requests(due);
        if expired.is_empty() {
            return false;
        }
        let skipped = expired
            .skipped
            .iter()
            .map(|outstanding| (outstanding, WriteOutcome::Skipped));
        let timed_out = expired
            .timed_out
            .iter()
            .map(|outstanding| (outstanding, WriteOutcome::Started));
        for (outstanding, write) in skipped.chain(timed_out) {
            self.retire_authorization(outstanding, write);
            self.withdraw_claims(outstanding);
            // Return only the unreceived heights — received ones are buffered (in
            // `in_flight` until committed); re-queuing them would re-fetch a body
            // we already hold (the WorkQueue single-owner invariant forbids it).
            let unreceived: Vec<_> = unreceived_heights(outstanding).collect();
            let outcome = self
                .io
                .work
                .release_reserved_and_return_items_detailed_for_owner(
                    outstanding.request.owner,
                    unreceived.iter().copied(),
                );
            self.trace_work_returned("request_timeout", outstanding, unreceived.len(), outcome);
        }
        let timed_out = expired.timed_out;
        // Bias away from immediately re-grabbing the heights this peer just timed
        // out, so another peer can contest them (the peer-local timeout bias).
        let timed_out_heights: Vec<_> = timed_out.iter().flat_map(unreceived_heights).collect();
        self.note_retry_avoid(timed_out_heights);
        if timed_out.is_empty() {
            self.publish_slot_diagnostics();
        } else {
            // Trace the window and reliability this timeout cut; the sample also publishes.
            self.trace_bbr_sample();
        }
        true
    }

    fn check_block_liveness(&mut self, now: Instant) -> Result<(), SinkReject> {
        match self.state.check_block_liveness(
            now,
            self.io.session.outbound_capacity() == 0,
            self.config.request_timeout,
        ) {
            LivenessDecision::Continue => Ok(()),
            LivenessDecision::Stall => self.no_progress_stall(
                now,
                "block-sync peer made no accepted block progress before liveness deadline",
            ),
        }
    }

    /// Apply the no-progress stall protocol: disconnect on a repeated stall,
    /// otherwise park the session for the cooldown and exit locally. An unended
    /// request still closes the connection (see `WriterFence`).
    fn no_progress_stall(&mut self, now: Instant, error: &'static str) -> Result<(), SinkReject> {
        if self.state.no_progress_response() == NoProgressResponse::Disconnect {
            tracing::debug!(
                peer = ?self.io.peer,
                outstanding = self.state.window.outstanding.len(),
                "disconnecting Zakura block-sync peer after repeated no-progress stall"
            );
            return Err(SinkReject::local(error));
        }
        self.io.registry.park_session(
            &self.io.peer,
            self.io.conn_id,
            self.io.generation,
            now + self.config.effective_no_progress_peer_cooldown(),
        );
        self.trace_liveness_park(error);
        tracing::debug!(
            peer = ?self.io.peer,
            outstanding = self.state.window.outstanding.len(),
            "parking Zakura block-sync session after no accepted block progress"
        );
        Err(SinkReject::local(error))
    }

    /// Retiring a failed session must not erase unanswered download work.
    /// Request expiry can empty `outstanding` without disarming liveness, so
    /// that pending no-progress deadline must also survive stream replacement.
    fn handle_stream_failure(
        &mut self,
        now: Instant,
        failure: OrderedStreamFailure,
    ) -> Result<(), SinkReject> {
        // Local reset may have withdrawn the requests while decoding waited
        // for capacity. It must remain neutral when the failed stream retires.
        self.on_view_changed();
        self.gc_skipped_outstanding();
        let Some(error) = self.state.stream_failure_stall(failure) else {
            return Ok(());
        };
        self.no_progress_stall(now, error)
    }

    /// Drop this routine's outstanding requests whose whole range is at or below
    /// the download floor: their bodies have entered the commit pipeline or have
    /// already been verified, so
    /// release the size-estimate reservation still held for any unreceived heights
    /// and free the slot. No heights return to the queue (they are committed,
    /// below the floor, GC'd from the WorkQueue). A partially-committed request
    /// (suffix still above the floor) is left so its remaining bodies keep their
    /// reservation and arrive on the same request.
    fn gc_committed_outstanding(&mut self) {
        let floor = self.download_floor();
        let mut removed = false;
        for key in self.state.window.outstanding.ending_at_or_below(floor) {
            let Some((outstanding, write)) = self.retire_request_locally(key) else {
                continue;
            };
            self.retire_authorization(&outstanding, write);
            self.withdraw_claims(&outstanding);
            // Release only estimates whose per-height ledger is still
            // `Reserved`. A competing delivery changes that ledger to
            // `Released` at receipt, so floor GC must not release it again.
            self.io.work.release_reserved_heights_for_owner(
                outstanding.request.owner,
                unreceived_heights(&outstanding),
            );
            removed = true;
        }
        if removed {
            self.publish_slot_diagnostics();
            self.state.window.disarm_liveness_after_progress_if_idle();
        }
    }

    /// A skipped frame ends the peer obligation independently of any received
    /// body whose owner must remain available to the sequencer.
    fn gc_skipped_outstanding(&mut self) {
        let skipped: Vec<_> = self
            .io
            .writes
            .skipped()
            .into_iter()
            .filter_map(|key| self.retire_request_locally(key))
            .collect();
        if skipped.is_empty() {
            return;
        }
        for (outstanding, write) in &skipped {
            self.retire_authorization(outstanding, *write);
            self.withdraw_claims(outstanding);
        }
        self.publish_slot_diagnostics();
    }

    /// Free request slots after the central queue retires their exact owners.
    /// Queue retirement already released their reservations.
    /// The cleanup path drops only routine-local and registry bookkeeping.
    ///
    /// Only a revocation by another actor can take unreceived work from a live
    /// request, so the scan runs once after each revocation.
    fn gc_obsolete_outstanding(&mut self) {
        if !self.state.observe_revocations(self.io.work.revocations()) {
            return;
        }

        let unreceived: Vec<_> = self
            .state
            .window
            .outstanding
            .entries()
            .flat_map(|(key, outstanding)| {
                unreceived_heights(outstanding)
                    .map(move |height| (height, (key, outstanding.request.owner)))
            })
            .collect();
        let mut lost: BTreeMap<RequestKey, Vec<block::Height>> = BTreeMap::new();
        // Bound each queue lock hold so a large window cannot stall other peers.
        for chunk in unreceived.chunks(OWNERSHIP_CHECK_CHUNK) {
            for (height, (key, _)) in self.io.work.disowned(chunk, |(_, owner)| *owner) {
                lost.entry(key).or_default().push(height);
            }
        }

        let mut removed = false;
        let obsolete: Vec<_> = self
            .state
            .window
            .outstanding
            .entries()
            .filter(|(key, outstanding)| {
                let lost = lost.get(key).map_or(0, Vec::len);
                lost == unreceived_heights(outstanding).count()
            })
            .map(|(key, _)| key)
            .collect();
        for key in obsolete {
            lost.remove(&key);
            let Some((retired, write)) = self.retire_request_locally(key) else {
                continue;
            };
            self.retire_authorization(&retired, write);
            self.withdraw_claims(&retired);
            removed = true;
        }
        // A request that still owns some heights keeps its slot but stops
        // advertising the heights it lost.
        for (key, heights) in lost {
            if let Some(outstanding) = self.state.window.outstanding.get(key) {
                self.io.registry.remove_outstanding_for_owner(
                    &self.io.peer,
                    self.io.generation,
                    outstanding.request.owner,
                    heights,
                );
            }
        }
        if removed {
            self.publish_slot_diagnostics();
            self.state.window.disarm_liveness_after_progress_if_idle();
        }
    }

    // ===================== inbound matched body (ports `handle_block`) ======

    /// Handle a decoded body that a live reservation already authorized.
    /// `raw_block_payload` is the frame payload `block` was decoded from.
    async fn handle_body(
        &mut self,
        block: Arc<block::Block>,
        body_wire_bytes: Option<u64>,
        body_permit: mpsc::OwnedPermit<SequencedBody>,
        raw_block_payload: Arc<[u8]>,
    ) {
        let hash = block.hash();
        let Some(height) = block.coinbase_height() else {
            self.report_misbehavior(BlockSyncMisbehavior::InvalidBlock)
                .await;
            return;
        };

        let matched = match self.state.match_body(height, hash) {
            BodyMatch::Matched(matched) => matched,
            BodyMatch::Unmatched => {
                // No outstanding match — run the unmatched fallthroughs locally.
                if self
                    .accept_unmatched_queued_body(
                        height,
                        hash,
                        block.clone(),
                        body_wire_bytes,
                        body_permit,
                        raw_block_payload.clone(),
                    )
                    .await
                {
                    return;
                }
                // Local retirement, reorgs and GC change interest, not authority.
                metrics::counter!("sync.block.response.unmatched_authorized_ignored").increment(1);
                tracing::debug!(
                    peer = ?self.io.peer,
                    ?height,
                    "ignoring an authorized block-sync body for retired local work"
                );
                return;
            }
            BodyMatch::Duplicate => {
                tracing::debug!(peer = ?self.io.peer, ?height, "ignoring duplicate block-sync body frame");
                return;
            }
            BodyMatch::PayloadMismatch { owner, requested } => {
                let mismatch = header_hash_payload_mismatch(owner, self.io.source, requested, hash);
                self.report_misbehavior(BlockSyncMisbehavior::BodyPayloadMismatch(mismatch))
                    .await;
                return;
            }
            BodyMatch::NotRequested => {
                self.report_misbehavior(BlockSyncMisbehavior::InvalidBlock)
                    .await;
                return;
            }
        };
        let key = matched.key;
        self.io
            .trace
            .record_block_body_received(hash, BlockBodySource::Zakura);
        let outstanding_owner = matched.owner;
        if self.io.work.owner_for_height(height) != Some(outstanding_owner) {
            metrics::counter!("sync.block.stale_completion.total", "kind" => "body_range")
                .increment(1);
            self.drop_obsolete_outstanding(key);
            return;
        }
        if !self
            .io
            .registry
            .peer_has_outstanding_height(&self.io.peer, height)
        {
            tracing::debug!(
                peer = ?self.io.peer,
                ?height,
                "ignoring late block-sync body for a claim cancelled by the floor watchdog"
            );
            self.finish_outstanding_at(key, Disposition::RetryMissing);
            return;
        }
        let estimated_bytes = matched.estimated_bytes;

        // The body's transactions are not validated against the header here;
        // consensus does it on apply (`handle_block_apply_finished` attributes a
        // rejection back to the delivering peer for misbehavior scoring).

        // Prefer the wire-measured body size; only re-serialize when absent (test
        // event).
        let serialized_bytes = match body_wire_bytes {
            Some(bytes) => bytes,
            None => match block.zcash_serialize_to_vec() {
                Ok(bytes) => bytes.len() as u64,
                Err(error) => {
                    tracing::debug!(?error, "failed to serialize decoded block-sync body");
                    self.finish_outstanding_at(key, Disposition::RetryOriginal);
                    self.report_misbehavior(BlockSyncMisbehavior::InvalidBlock)
                        .await;
                    return;
                }
            },
        };
        if serialized_bytes > tolerated_bytes(estimated_bytes, self.config.size_deviation_tolerance)
        {
            // The header matched the requested hash, so the size hint was wrong.
            // Consensus still validates the body. Discarding it would let one bad
            // advertised size stall this height on every honest delivery. A
            // regulated reservation already bounds the body's bytes, so the
            // estimate says nothing about that supplier.
            metrics::counter!("sync.block.body.size_hint_mismatch").increment(1);
        }

        self.finish_matched_receipt(
            matched,
            height,
            hash,
            block,
            raw_block_payload,
            body_wire_bytes,
            serialized_bytes,
            body_permit,
        );
    }

    /// Transfer request funding through the body send without a suspension point.
    #[allow(clippy::too_many_arguments)]
    fn finish_matched_receipt(
        &mut self,
        matched: MatchedBody,
        height: block::Height,
        hash: block::Hash,
        block: Arc<block::Block>,
        raw_block_payload: Arc<[u8]>,
        body_wire_bytes: Option<u64>,
        serialized_bytes: u64,
        body_permit: mpsc::OwnedPermit<SequencedBody>,
    ) {
        let MatchedBody {
            key,
            owner: outstanding_owner,
            estimated_bytes,
            start_height: request_start_height,
            count: request_range_count,
            queued_at,
            delivery_snapshot,
        } = matched;
        let request_elapsed = queued_at.elapsed();
        let request_elapsed_ms = elapsed_ms_u64(request_elapsed);

        metrics::counter!("sync.block.body.received").increment(1);
        self.record_received(serialized_bytes);
        // End the request reservation at receipt, but release its bytes only
        // after the body is visible to the resident-memory accounting.
        let receipt_settlement = match self
            .io
            .work
            .begin_receipt_for_owner(outstanding_owner, height)
        {
            Ok(settlement) => settlement,
            Err(_) => {
                self.io.cancel.cancel();
                return;
            }
        };
        let Some(receipt_settlement) = receipt_settlement else {
            tracing::debug!(
                peer = ?self.io.peer,
                ?height,
                serialized_bytes,
                "block-sync body already settled by another peer; marking received"
            );
            self.accept_already_settled_height(key, outstanding_owner, height);
            return;
        };
        let decoded_attributed_memory_size_bytes =
            record_decoded_memory_size(&block, body_wire_bytes);
        self.trace_body_received(
            height,
            serialized_bytes,
            decoded_attributed_memory_size_bytes,
            Some(request_start_height),
            Some(request_range_count),
            Some(request_elapsed_ms),
        );

        let complete = self.state.record_body(
            key,
            height,
            serialized_bytes,
            Instant::now(),
            self.config.effective_liveness_timeout(),
        );
        self.withdraw_claim(outstanding_owner, height);
        let completed = if complete {
            self.remove_request(key)
        } else {
            None
        };
        if let Some((outstanding, _)) = &completed {
            // Feed the BBR estimators on request completion: the round-trip (RTprop)
            // and the per-ack delivery rate (BtlBw) for this request's block count and
            // delivered bytes.
            self.state.window.record_delivery(
                Instant::now(),
                request_elapsed,
                request_range_count,
                outstanding.delivered_bytes,
                delivery_snapshot,
            );
        }
        if let Some((outstanding, write)) = completed {
            self.finish_detached(outstanding, write, Disposition::Satisfied);
        } else {
            self.publish_slot_diagnostics();
        }

        // Admission reserved the sequencer slot before decode. Transfer the body
        // synchronously, then settle its request funding.
        let receipt_settlement =
            ReceiptSettlement::new(self, outstanding_owner, height, receipt_settlement.funding);
        self.forward_body_to_sequencer(
            outstanding_owner,
            height,
            hash,
            block,
            raw_block_payload,
            decoded_attributed_memory_size_bytes,
            serialized_bytes,
            Some(estimated_bytes),
            body_permit,
        );
        drop(receipt_settlement);
        // This body opened only this peer's slots; the want-work loop runs at the
        // top of the next iteration.
    }

    /// Reports that the routine is about to send the body at `height` to the
    /// sequencer while its request still charges `charge` to the budget. The
    /// report comes before the send, because the sequencer can dequeue the
    /// body as soon as the send succeeds. Reports nothing when `charge` is 0,
    /// because then the budget never counted the body.
    /// The receipt owner reports the end after the send or during unwind.
    #[cfg(any(test, zakura_sim))]
    fn observe_handoff_started(
        &self,
        owner: zakura_header_chain::BodyWorkOwner,
        height: block::Height,
        charge: u64,
    ) {
        if charge == 0 {
            return;
        }
        observe!(
            self.io.observer,
            super::block_sync_observe::BlockEvent::HandoffStarted {
                peer: self.io.peer.clone(),
                owner,
                height,
                charge,
            }
        );
    }

    // ===================== unmatched fallthroughs (ported) ==================

    #[allow(clippy::too_many_arguments)]
    fn forward_body_to_sequencer(
        &self,
        owner: zakura_header_chain::BodyWorkOwner,
        height: block::Height,
        hash: block::Hash,
        block: Arc<block::Block>,
        raw_block_payload: Arc<[u8]>,
        decoded_attributed_memory_size_bytes: u64,
        serialized_bytes: u64,
        admitted_estimate_bytes: Option<u64>,
        body_permit: mpsc::OwnedPermit<SequencedBody>,
    ) {
        let received_at = Instant::now();
        let sequencer_send_started = time::Instant::now();
        let previous_block_hash = block.header.previous_block_hash;
        // The body owns the decoded charge from here, with no await between.
        let charged = charge_decoded_handoff(
            &self.io.sequencer_input_decoded_attributed_memory_bytes,
            &self.io.sequencer_view,
            decoded_attributed_memory_size_bytes,
        );
        let body = BufferedBlockBody::for_handoff(
            block,
            raw_block_payload,
            decoded_attributed_memory_size_bytes,
            charged,
        );
        let body = SequencedBody::new_handed_off(
            owner,
            self.io.source,
            height,
            hash,
            previous_block_hash,
            body,
            serialized_bytes,
            admitted_estimate_bytes,
            self.io.peer.clone(),
            received_at,
            self.io.sequencer_input_bytes.clone(),
            self.io
                .sequencer_input_decoded_attributed_memory_bytes
                .clone(),
        );

        // Report retention before sending because the sequencer can dequeue
        // the body immediately on another thread.
        #[cfg(any(test, zakura_sim))]
        {
            use super::block_sync_observe::{BlockEvent, HeldBody, Holder, HolderChange};
            let queued = (
                HeldBody {
                    height,
                    owner,
                    bytes: serialized_bytes,
                },
                Holder::Queued {
                    allocation: body.body.allocation(),
                    decoded_attributed_memory_bytes: body
                        .body
                        .measured_decoded_attributed_memory_bytes(),
                },
            );
            let (body, holder) = queued;
            observe!(
                self.io.observer,
                BlockEvent::Holder {
                    body,
                    holder,
                    change: HolderChange::Retained,
                }
            );
        }
        body_permit.send(body);
        self.trace_body_sequencer_sent(height, sequencer_send_started.elapsed(), true);
    }

    /// Accept a wanted unmatched body whose original requester is gone or whose
    /// height is currently reserved by another peer. The resident `admit()` check
    /// is the sole gate for queued heights — a received body consumes no request
    /// budget; for reserved in-flight heights the arrival ends that request's
    /// reservation (first-completion-wins).
    #[allow(clippy::too_many_arguments)]
    async fn accept_unmatched_queued_body(
        &mut self,
        height: block::Height,
        hash: block::Hash,
        block: Arc<block::Block>,
        body_wire_bytes: Option<u64>,
        body_permit: mpsc::OwnedPermit<SequencedBody>,
        raw_block_payload: Arc<[u8]>,
    ) -> bool {
        if self.io.work.hash_for_height(height) != Some(hash) {
            return false;
        }
        if !self.state.serves(height) {
            return false;
        }
        let serialized_bytes = match body_wire_bytes {
            Some(bytes) => bytes,
            None => match block.zcash_serialize_to_vec() {
                Ok(bytes) => u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                Err(error) => {
                    tracing::debug!(
                        peer = ?self.io.peer,
                        ?height,
                        ?error,
                        "failed to serialize unmatched queued block-sync body"
                    );
                    self.report_misbehavior(BlockSyncMisbehavior::InvalidBlock)
                        .await;
                    return true;
                }
            },
        };

        self.finish_unmatched_receipt(
            height,
            hash,
            block,
            body_wire_bytes,
            serialized_bytes,
            body_permit,
            raw_block_payload,
        )
    }

    /// Claim and send a wanted late body without suspending its funding transfer.
    #[allow(clippy::too_many_arguments)]
    fn finish_unmatched_receipt(
        &mut self,
        height: block::Height,
        hash: block::Hash,
        block: Arc<block::Block>,
        body_wire_bytes: Option<u64>,
        serialized_bytes: u64,
        body_permit: mpsc::OwnedPermit<SequencedBody>,
        raw_block_payload: Arc<[u8]>,
    ) -> bool {
        let reserved_in_flight = self.io.work.reserved_in_flight_charge(height);
        let is_pending = self.io.work.pending_contains(height);
        if reserved_in_flight.is_none() && !is_pending {
            return false;
        }
        self.io
            .trace
            .record_block_body_received(hash, BlockBodySource::Zakura);
        let Some(owner) = self.io.work.owner_for_height(height) else {
            // Pending work has no active request owner.
            // Accepting a body here would create an unowned completion.
            // Leave it to the unsolicited or stale classification path.
            return false;
        };

        // The reservation this arrival ended (an active competing request, or a
        // stale charge on the claimed height); released after the forward below.
        let (receipt_settlement, admitted_estimate_bytes) = if is_pending {
            let sequencer_view = *self.io.sequencer_view.borrow();
            let snapshot = self.admission_snapshot(&sequencer_view);
            if !admit_received_body(&self.config, &snapshot, height, serialized_bytes) {
                tracing::debug!(
                    peer = ?self.io.peer,
                    ?height,
                    serialized_bytes,
                    "not buffering unmatched queued block-sync body at look-ahead cap"
                );
                return true;
            }

            // Claim this height into `in_flight` so it leaves `pending`; if it is
            // already `in_flight` the take is a no-op and the Sequencer drops the
            // later duplicate. The received body charges no request budget, but any
            // stale request reservation the height still owned is released below.
            let _ = self.io.work.take_in_range(height, height, 1);
            metrics::counter!("sync.block.response.unmatched_queued_accepted").increment(1);
            let funding = match self.io.work.claim_receipt_for_owner(owner, height) {
                Ok(funding) => funding,
                Err(_) => {
                    self.io.cancel.cancel();
                    return true;
                }
            };
            (funding, None)
        } else {
            // First-completion-wins for a timed-out height already re-issued to
            // another peer: this arrival ends that request's reservation instead of
            // discarding a valid body because another peer currently owns the
            // request slot.
            let settlement = match self.io.work.begin_competing_receipt(owner, height) {
                Ok(Some(settlement)) => settlement,
                Ok(None) => return false,
                Err(_) => {
                    self.io.cancel.cancel();
                    return true;
                }
            };
            metrics::counter!("sync.block.response.unmatched_active_accepted").increment(1);
            (settlement.funding, Some(settlement.charge))
        };

        let decoded_attributed_memory_size_bytes =
            record_decoded_memory_size(&block, body_wire_bytes);
        self.record_received(serialized_bytes);
        self.trace_body_received(
            height,
            serialized_bytes,
            decoded_attributed_memory_size_bytes,
            None,
            None,
            None,
        );

        // A real, wanted body that no longer matches an outstanding request (typically
        // arrived just after its request timed out). Count it as block progress: resets
        // the no-progress streak and proves the peer, so a slow-but-useful peer is not
        // parked as "silent". Deliberately do NOT feed the BBR RTprop/BtlBw estimators —
        // the originating request is gone, so there's no trustworthy send timestamp and a
        // stale late-delivery interval would corrupt the rate/latency samples.
        // Also credit the reliability EWMA: this late body offsets the failure its own
        // timeout charged, so a peer that merely slowed down (backlog draining past the
        // per-request deadline but every body still arriving) keeps a reduced-but-nonzero
        // window instead of being sealed to zero like a genuine dropper — which sends no
        // late body to credit. This is the slow-vs-wedged distinction the seal relies on.
        self.state
            .record_late_body(Instant::now(), self.config.effective_liveness_timeout());

        let receipt_settlement = ReceiptSettlement::new(self, owner, height, receipt_settlement);
        self.forward_body_to_sequencer(
            owner,
            height,
            hash,
            block,
            raw_block_payload,
            decoded_attributed_memory_size_bytes,
            serialized_bytes,
            admitted_estimate_bytes,
            body_permit,
        );
        // Release the ended reservation only now that the body is counted in
        // `sequencer_input_bytes`, mirroring the matched receipt path above, so
        // the bytes are never invisible to both the limiter and the resident
        // snapshot.
        drop(receipt_settlement);
        true
    }

    // ===================== terminators =====================================

    /// Both endings close the whole original request. A short `BlocksDone`
    /// retries its missing suffix; `RangeUnavailable` retries the original
    /// range even if the peer reported a smaller count.
    fn handle_ending(&mut self, completed: CompletedRange) {
        // Local timeouts, reorgs, and finality can retire the scheduler's work
        // while the peer's authority, already consumed above, remained live.
        let Some(ending) = self.state.on_ending(
            completed.range.start(),
            completed.outcome,
            self.download_floor(),
        ) else {
            return;
        };
        if completed.outcome == Outcome::Unavailable {
            self.trace_range_unavailable(
                completed.range.start(),
                ending.count,
                elapsed_ms_u64(ending.queued_at.elapsed()),
            );
        }
        if !ending.stale_heights.is_empty() {
            self.io
                .work
                .release_reserved_heights_for_owner(ending.owner, ending.stale_heights);
        }
        self.finish_outstanding_at(ending.key, ending.disposition);
    }

    // ===================== outstanding lifecycle (ported) ===================

    fn finish_outstanding_at(&mut self, key: RequestKey, disposition: Disposition) {
        let Some((outstanding, write)) = self.remove_request(key) else {
            return;
        };
        self.finish_detached(outstanding, write, disposition);
    }

    /// Remove request `key` from the window and settle its write.
    fn remove_request(&mut self, key: RequestKey) -> Option<(OutstandingBlockRange, WriteOutcome)> {
        let outstanding = self.state.window.outstanding.remove(key)?;
        Some((outstanding, self.io.writes.settle(key)))
    }

    /// Retire request `key` on this node's initiative and settle its write.
    /// See [`DownloadWindow::retire_locally`].
    fn retire_request_locally(
        &mut self,
        key: RequestKey,
    ) -> Option<(OutstandingBlockRange, WriteOutcome)> {
        self.state.window.outstanding.get(key)?;
        let write = self.io.writes.settle(key);
        let outstanding = self.state.window.retire_locally(key, write)?;
        Some((outstanding, write))
    }

    /// Drop a local request after the central queue retires its work scope.
    /// The central queue already released the reservation.
    /// The cleanup path preserves any replacement item at the same height.
    fn drop_obsolete_outstanding(&mut self, key: RequestKey) {
        let Some((retired, write)) = self.retire_request_locally(key) else {
            return;
        };
        self.retire_authorization(&retired, write);
        self.withdraw_claims(&retired);
        self.publish_slot_diagnostics();
        self.state.window.disarm_liveness_after_progress_if_idle();
    }

    /// End the live exchange of a request whose write settled as `write`.
    fn retire_authorization(&self, outstanding: &OutstandingBlockRange, write: WriteOutcome) {
        self.io.requester.retire(
            outstanding.request.start_height,
            write == WriteOutcome::Skipped,
        );
    }

    fn finish_detached(
        &mut self,
        outstanding: OutstandingBlockRange,
        write: WriteOutcome,
        disposition: Disposition,
    ) {
        self.retire_authorization(&outstanding, write);
        self.withdraw_claims(&outstanding);
        match disposition {
            Disposition::Satisfied => {
                // Every requested height was received and buffered; nothing
                // returns to the queue (buffered heights stay in `in_flight`
                // until the floor commits past them). Release any residual
                // reserved estimate (normally none once complete).
                self.io.work.release_reserved_heights_for_owner(
                    outstanding.request.owner,
                    unreceived_heights(&outstanding),
                );
            }
            // With fanout = 1 a received height is already buffered and must never
            // be re-fetched, so both retry dispositions return only the still-reserved
            // unreceived heights to `pending`. `return_items` is idempotent.
            Disposition::RetryOriginal | Disposition::RetryMissing => {
                let unreceived: Vec<_> = unreceived_heights(&outstanding).collect();
                let outcome = self
                    .io
                    .work
                    .release_reserved_and_return_items_detailed_for_owner(
                        outstanding.request.owner,
                        unreceived.iter().copied(),
                    );
                self.trace_work_returned(
                    disposition.trace_label(),
                    &outstanding,
                    unreceived.len(),
                    outcome,
                );
                // This peer just failed these heights (RangeUnavailable / short
                // BlocksDone): bias away from re-grabbing them so another peer
                // contests the range first (and so the routine cannot self-wake
                // into a re-take spin off its own `return_items`).
                self.note_retry_avoid(unreceived);
            }
        }
        self.publish_slot_diagnostics();
        if disposition == Disposition::Satisfied {
            self.state.window.disarm_liveness_after_progress_if_idle();
        }
    }

    /// A body arrived for a request this peer owns, but its per-height ledger was
    /// already `Released` by a competing receipt, watchdog, or floor GC, so
    /// `release_active_reserved_height` returned `None`. Record the height as
    /// received so the request can complete without re-queueing it or touching
    /// the budget again. Count it as block progress since a real wanted body did
    /// arrive on this peer's stream.
    fn accept_already_settled_height(
        &mut self,
        key: RequestKey,
        owner: zakura_header_chain::BodyWorkOwner,
        height: block::Height,
    ) {
        let Some(complete) = self.state.record_settled_body(
            key,
            height,
            Instant::now(),
            self.config.effective_liveness_timeout(),
        ) else {
            return;
        };
        self.withdraw_claim(owner, height);
        if complete {
            self.finish_outstanding_at(key, Disposition::Satisfied);
        } else {
            self.publish_slot_diagnostics();
        }
    }

    /// Publish the unreceived heights of a newly issued request to the registry,
    /// so the producer's `!has_outstanding_request` filter and the low-water
    /// `total_unreceived` gate count them. Received-but-uncommitted heights stay
    /// out of the registry because `work.in_flight` holds them.
    ///
    /// Another actor can revoke a height after publication checked its owner.
    /// Rechecking after the insert withdraws such a claim. A later revocation
    /// triggers [`gc_obsolete_outstanding`](Self::gc_obsolete_outstanding).
    fn publish_request_claims(&self, key: RequestKey) {
        let Some(outstanding) = self.state.window.outstanding.get(key) else {
            return;
        };
        let owner = outstanding.request.owner;
        let claims: Vec<_> = outstanding
            .request
            .expected_blocks
            .iter()
            .filter(|expected| !outstanding.has_received(expected.height))
            .map(|expected| {
                (
                    expected.height,
                    super::peer_registry::OutstandingMeta {
                        owner,
                        hash: expected.hash,
                        estimated_bytes: expected.estimated_bytes,
                        queued_at: outstanding.queued_at,
                        deadline: outstanding.deadline,
                    },
                )
            })
            .collect();
        self.io.registry.insert_outstanding(
            &self.io.peer,
            self.io.generation,
            claims.iter().copied(),
        );
        let disowned = self.io.work.disowned(&claims, |meta| meta.owner);
        if !disowned.is_empty() {
            self.io.registry.remove_outstanding_for_owner(
                &self.io.peer,
                self.io.generation,
                owner,
                disowned.into_iter().map(|(height, _)| height),
            );
        }
    }

    /// Withdraw every registry claim that a request leaving the window still
    /// holds. A replacement request's claim at the same height stays published.
    fn withdraw_claims(&self, outstanding: &OutstandingBlockRange) {
        self.io.registry.remove_outstanding_for_owner(
            &self.io.peer,
            self.io.generation,
            outstanding.request.owner,
            outstanding
                .request
                .expected_blocks
                .iter()
                .map(|expected| expected.height),
        );
    }

    /// Withdraw the registry claim for one received height.
    fn withdraw_claim(&self, owner: zakura_header_chain::BodyWorkOwner, height: block::Height) {
        self.io.registry.remove_outstanding_for_owner(
            &self.io.peer,
            self.io.generation,
            owner,
            [height],
        );
    }

    /// Why the live requester refuses another request, whatever the window allows.
    fn requester_stop(&self) -> Option<FillStop> {
        self.state.requester_stop(self.io.requester.occupancy())
    }

    /// Publish the window diagnostics for the reactor's periodic trace row and
    /// for other routines' cross-peer floor-bias decisions. A routine whose
    /// requester refuses new requests publishes no free slots, so no other
    /// routine defers the floor to it.
    fn publish_slot_diagnostics(&mut self) {
        let hard_capacity = hard_outbound_capacity(self.state.window.max_inflight_requests);
        self.state.published_requester_stopped = self.requester_stop().is_some();
        let available_slots = if self.state.published_requester_stopped {
            0
        } else {
            self.state.window.available_slots_at(Instant::now())
        };
        self.state.published_free_slots = available_slots > 0;
        self.io.registry.publish_slots(
            &self.io.peer,
            self.io.generation,
            super::peer_registry::SlotDiagnostics {
                hard_capacity,
                effective_window: self.state.window.bbr_effective_cwnd().min(hard_capacity),
                available_slots,
                outstanding_requests: self.state.window.outstanding.len(),
                // Filter the published RTprop by now so a peer that stopped completing
                // requests stops advertising a stale-low RTprop to the cross-peer
                // floor-preference comparison.
                bbr_rtprop_ms: self.state.window.bbr_rtprop_ms(Instant::now()),
            },
        );
    }

    // ===================== misbehavior (shared count via registry) ==========

    async fn report_misbehavior(&self, reason: BlockSyncMisbehavior) {
        // This path only records misbehavior: observe and forward it, but never
        // cancel the session. Only a proven tampered body, reported by the
        // sequencer, disconnects (see `BlockSyncMisbehavior::proves_tampered_body`).
        metrics::counter!("sync.block.peer.violation").increment(1);
        // `Misbehavior` is best-effort: never block the routine. The reactor owns
        // action dispatch so attacker-triggered reports cannot bypass its reserved
        // control capacity.
        let _ = self
            .io
            .routine_to_reactor
            .try_send(RoutineToReactor::Misbehavior {
                peer: self.io.peer.clone(),
                reason,
            });
    }

    // ===================== view reads ======================================

    fn download_floor(&self) -> block::Height {
        self.io.sequencer_view.borrow().download_floor
    }

    fn record_received(&self, bytes: u64) {
        // Count accepted serialized block payloads, excluding transport framing.
        metrics::counter!("sync.block.payload.received.bytes").increment(bytes);
        if let Ok(mut meter) = self.io.received_throughput.lock() {
            meter.record(bytes);
        }
    }

    // ===================== tracing =========================================
}

fn elapsed_ms_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

impl Drop for PeerRoutine {
    /// disconnect-mid-fetch correctness: on every exit path
    /// (cancel/panic/normal) return this routine's unreceived outstanding heights
    /// to `work.pending`, release their byte reservation, and clear this peer's
    /// outstanding set in the registry. All operations are sync (lock/atomic), so
    /// the guard is cancel-safe and panic-safe. Each return names its request's
    /// exact owner, which carries this session's generation, and the registry
    /// ignores a stale generation, so an old routine cannot return or clear its
    /// replacement's work.
    ///
    /// The guard clears the peer's *outstanding* rather than removing the whole
    /// registry entry: a reset respawns the routine (the reactor cancels + spawns
    /// a fresh one) while the peer stays connected, so its servable/caps must
    /// survive. If the guard removed the entry, an old routine's async Drop could
    /// race *after* the respawned routine re-inserted and nuke the live entry.
    /// The reactor owns entry insert (on connect) and remove (on disconnect/
    /// admission-reject); see `handle_peer_disconnected`.
    fn drop(&mut self) {
        // A routine dropped inside its handoff no longer blocks on it.
        self.io.handoff_blocked.store(false, Ordering::Relaxed);
        self.return_unreceived_requests("peer_routine_drop");
        // The routine's last snapshot, after it returned its work.
        #[cfg(any(test, zakura_sim))]
        self.observe_owners(true);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::AtomicU64;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use tokio::sync::{mpsc, watch};
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;
    use zakura_chain::block;
    use zakura_rt::time::Instant;

    use super::super::config::MAX_BS_INFLIGHT_REQUESTS;
    use super::super::peer_registry::PeerRegistry;
    use super::super::regulated::{
        live_requester::LiveRequester,
        session::{Serving, ServingSession},
    };
    use super::super::request::BlockSizeEstimate;
    use super::super::sequencer_task::{initial_view, SequencedBody};
    use super::super::source::EmptyBlockRangeSource;
    use super::super::state::{ByteBudget, ThroughputMeter};
    use super::super::tests::genesis_frontiers;
    use super::super::work_queue::WorkQueue;
    use super::super::work_queue::{RequestWritePhase, WorkItemPlace};
    use super::super::{
        BlockSyncMessage, BlockSyncPeerSession, BlockSyncStatus, CwndUnit, ZakuraBlockSyncConfig,
    };
    use super::PeerRoutine;
    use super::RoutineToReactor;
    use crate::zakura::framed_channel;
    use crate::zakura::regulation::{ReservationPool, WriterFence};
    use crate::zakura::trace::ZakuraTrace;
    use crate::zakura::CloseCause;
    use crate::zakura::ZakuraPeerId;

    /// A private pool with production's node-wide request capacity.
    pub(super) fn test_request_pool() -> ReservationPool {
        ReservationPool::new(usize::try_from(MAX_BS_INFLIGHT_REQUESTS).unwrap()).unwrap()
    }

    /// Production response authorization and serving for a test routine. Both
    /// write through `session`, and the requester's fence closes `connection`.
    pub(super) fn test_authorization(
        session: &BlockSyncPeerSession,
        recv: &crate::zakura::FramedRecv,
        config: &ZakuraBlockSyncConfig,
        pool: ReservationPool,
        connection: &CancellationToken,
    ) -> (LiveRequester, ServingSession) {
        let close_cause = CloseCause::default();
        let requester = LiveRequester::new(
            pool,
            WriterFence::new(connection.clone(), close_cause.clone()),
            recv,
            session.request_sender(),
        );
        let serving = Serving::new(Arc::new(EmptyBlockRangeSource), config).session(
            session.peer_id(),
            super::super::block_sync_observe::SessionKey::default(),
            session.request_sender(),
            session.cancel_token(),
            connection.clone(),
            close_cause,
        );
        (requester, serving)
    }

    /// [`status_test_routine_with`] using a private pool and connection.
    pub(super) fn status_test_routine() -> (
        PeerRoutine,
        crate::zakura::FramedRecv,
        mpsc::Receiver<RoutineToReactor>,
    ) {
        status_test_routine_with(test_request_pool(), &CancellationToken::new())
    }

    /// An admitted routine with default config that authorizes from `pool` and
    /// closes `connection`. Returns its outbound frames and reactor reports.
    pub(super) fn status_test_routine_with(
        pool: ReservationPool,
        connection: &CancellationToken,
    ) -> (
        PeerRoutine,
        crate::zakura::FramedRecv,
        mpsc::Receiver<RoutineToReactor>,
    ) {
        let config = ZakuraBlockSyncConfig::default();
        let peer = ZakuraPeerId::new(vec![7u8; 32]).expect("test peer id is within bounds");
        let cancel = CancellationToken::new();
        let (out_send, out_recv) = framed_channel(16);
        let (_in_send, in_recv) = framed_channel(16);
        let session = BlockSyncPeerSession::for_test(peer.clone(), out_send, cancel.clone());
        let registry = Arc::new(PeerRegistry::new());
        let generation = registry
            .admit_session(&peer, session.direction(), &config, 0, Instant::now())
            .generation();
        let budget = ByteBudget::new(config.max_inflight_block_bytes);
        let work = Arc::new(WorkQueue::new(block::Height(0), budget.clone()));
        let (sequencer_input_tx, _sequencer_input_rx) = mpsc::channel(16);
        let (routine_to_reactor_tx, routine_to_reactor_rx) = mpsc::channel(16);
        let (_view_tx, view_rx) = watch::channel(initial_view(genesis_frontiers()));
        let (requester, serving) =
            test_authorization(&session, &in_recv, &config, pool, connection);
        let routine = PeerRoutine::new(
            peer.clone(),
            0,
            session,
            in_recv,
            requester,
            serving,
            config.clone(),
            true,
            generation,
            Arc::clone(&work),
            Arc::clone(&registry),
            Arc::new(Mutex::new(ThroughputMeter::new(Instant::now()))),
            sequencer_input_tx,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            routine_to_reactor_tx,
            view_rx,
            cancel,
            ZakuraTrace::noop(),
        );
        (routine, out_recv, routine_to_reactor_rx)
    }

    #[tokio::test]
    async fn floor_provider_does_not_defer_to_a_peer_behind_the_missing_block() {
        use super::super::peer_registry::SlotDiagnostics;

        let (mut routine, mut outbound, _reactor_events) = status_test_routine();
        routine.state.received_status = true;
        routine.state.servable_low = block::Height(0);
        routine.state.servable_high = block::Height(1);
        let fast_peer = ZakuraPeerId::new(vec![8; 32]).unwrap();
        let fast_generation = routine
            .io
            .registry
            .admit_session(
                &fast_peer,
                routine.io.session.direction(),
                &routine.config,
                0,
                Instant::now(),
            )
            .generation();
        for (peer, generation, high, latency) in [
            (&routine.io.peer, routine.io.generation, 1, 145),
            (&fast_peer, fast_generation, 0, 5),
        ] {
            routine.io.registry.upsert_status(
                peer,
                generation,
                BlockSyncStatus {
                    servable_low: block::Height(0),
                    servable_high: block::Height(high),
                    ..BlockSyncStatus::default()
                },
            );
            routine.io.registry.publish_slots(
                peer,
                generation,
                SlotDiagnostics {
                    available_slots: 3,
                    bbr_rtprop_ms: Some(latency),
                    ..SlotDiagnostics::default()
                },
            );
        }
        routine.io.work.extend(
            super::super::test_work_scope(),
            [(
                block::Height(1),
                block::Hash([1; 32]),
                BlockSizeEstimate::Advertised(1_000),
            )],
        );

        routine.try_fill();

        let frame = outbound
            .try_recv()
            .expect("the only provider must request the missing block immediately");
        assert!(matches!(
            BlockSyncMessage::decode_frame(frame).unwrap(),
            BlockSyncMessage::GetBlocks {
                start_height: block::Height(1),
                count: 1
            }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn floor_ranking_change_wakes_a_deferred_routine() {
        use super::super::peer_registry::SlotDiagnostics;

        let (mut slow, mut outbound, _reactor_events) = status_test_routine();
        // Keep the input streams open while driving the real wait loop.
        let (_in_send, in_recv) = framed_channel(16);
        slow.io.recv = in_recv;
        let (_view_tx, view_rx) = watch::channel(*slow.io.sequencer_view.borrow());
        slow.io.sequencer_view = view_rx;
        slow.state.received_status = true;
        slow.state.servable_low = block::Height(1);
        slow.state.servable_high = block::Height(10);

        let registry = Arc::clone(&slow.io.registry);
        let work = Arc::clone(&slow.io.work);
        let slow_peer = slow.io.peer.clone();
        let slow_generation = slow.io.generation;
        let cancel = slow.io.cancel.clone();
        let fast_peer = ZakuraPeerId::new(vec![8; 32]).unwrap();
        let fast_generation = registry
            .admit_session(
                &fast_peer,
                slow.io.session.direction(),
                &slow.config,
                0,
                Instant::now(),
            )
            .generation();
        for (peer, generation) in [(&slow_peer, slow_generation), (&fast_peer, fast_generation)] {
            registry.upsert_status(
                peer,
                generation,
                BlockSyncStatus {
                    servable_low: block::Height(0),
                    servable_high: block::Height(10),
                    ..BlockSyncStatus::default()
                },
            );
        }

        let mut running = Box::pin(slow.run());
        assert!(futures::poll!(running.as_mut()).is_pending());
        // Consume the initial heartbeat before publishing the two finite scores.
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(futures::poll!(running.as_mut()).is_pending());
        let before_refill = Instant::now();
        let slots = SlotDiagnostics {
            available_slots: 3,
            bbr_rtprop_ms: Some(75),
            ..SlotDiagnostics::default()
        };
        registry.publish_slots(&fast_peer, fast_generation, slots);
        registry.publish_slots(
            &slow_peer,
            slow_generation,
            SlotDiagnostics {
                bbr_rtprop_ms: Some(145),
                ..slots
            },
        );
        work.extend(
            super::super::test_work_scope(),
            [(
                block::Height(1),
                block::Hash([1; 32]),
                BlockSizeEstimate::Advertised(1_000),
            )],
        );

        // The slow worker sees the faster carrier and goes back to sleep.
        assert!(futures::poll!(running.as_mut()).is_pending());
        assert!(outbound.try_recv().is_err());
        assert!(work.pending_contains(block::Height(1)));

        registry.publish_slots(
            &fast_peer,
            fast_generation,
            SlotDiagnostics {
                bbr_rtprop_ms: None,
                ..slots
            },
        );
        assert!(registry.floor_has_preferred_unsaturated_server(
            block::Height(0),
            &fast_peer,
            None,
            false
        ));

        // The fast worker now defers to the sleeping slow worker. Only the
        // ranking update can wake it: no work, capacity, view, or timer changed.
        assert!(futures::poll!(running.as_mut()).is_pending());
        let frame = outbound
            .try_recv()
            .expect("a ranking change must wake the deferred worker");
        assert!(matches!(
            BlockSyncMessage::decode_frame(frame).unwrap(),
            BlockSyncMessage::GetBlocks {
                start_height: block::Height(1),
                count: 1
            }
        ));
        assert!(work.in_flight_contains(block::Height(1)));
        assert!(!work.pending_contains(block::Height(1)));
        assert_eq!(Instant::now(), before_refill);

        cancel.cancel();
        assert!(timeout(Duration::from_secs(1), running)
            .await
            .unwrap()
            .is_ok());
    }

    #[tokio::test]
    async fn status_flood_cannot_republish_ranges_or_caps_without_a_bound() {
        let (mut routine, _outbound, mut reactor_events) = status_test_routine();
        let mut status = BlockSyncStatus {
            servable_low: block::Height(1),
            servable_high: block::Height(3),
            tip_hash: block::Hash([3; 32]),
            max_blocks_per_response: routine.config.advertised_max_blocks_per_response(),
            max_inflight_requests: routine.config.advertised_max_inflight_requests(),
            max_response_bytes: routine.config.advertised_max_response_bytes(),
        };
        routine.handle_status(status);
        let _ = reactor_events
            .try_recv()
            .expect("initial status is published");
        let next_allowed = Instant::now() + Duration::from_secs(60);
        routine.state.inbound_status_meter.next_allowed = next_allowed;
        routine.state.status_reply_meter.next_allowed = next_allowed;

        status.servable_low = block::Height(2);
        routine.handle_status(status);
        assert!(matches!(
            reactor_events.try_recv(),
            Ok(RoutineToReactor::StatusReceived {
                send_reply: false,
                ..
            })
        ));
        let immediate_range = routine.io.registry.candidate_snapshot();
        let immediate_caps = routine.state.published_limits;
        status.max_response_bytes /= 2;
        for (low, high) in [(1, 3), (2, 4), (0, 0), (3, 3)]
            .into_iter()
            .cycle()
            .take(256)
        {
            status.servable_low = block::Height(low);
            status.servable_high = block::Height(high);
            routine.handle_status(status);
            assert!(
                reactor_events.try_recv().is_err(),
                "range oscillation must not bypass the meter indefinitely"
            );
            assert_eq!(routine.io.registry.candidate_snapshot(), immediate_range);
            assert_eq!(routine.state.published_limits, immediate_caps);
            assert_eq!(routine.state.max_response_bytes, status.max_response_bytes);
        }
        routine.flush_pending_status();
        assert!(reactor_events.try_recv().is_err());

        routine.state.inbound_status_meter.next_allowed = Instant::now();
        routine.flush_pending_status();
        assert_eq!(
            (routine.state.servable_low, routine.state.servable_high),
            (block::Height(3), block::Height(3)),
            "the final range must apply after the window opens"
        );
        assert_eq!(routine.state.max_response_bytes, status.max_response_bytes);
        assert!(matches!(
            reactor_events.try_recv(),
            Ok(RoutineToReactor::StatusReceived {
                send_reply: false,
                ..
            })
        ));

        // A newer return to the applied range must cancel an older deferred change.
        routine.state.inbound_status_meter.next_allowed = next_allowed;
        status.servable_low = block::Height(1);
        routine.handle_status(status);
        let _ = reactor_events
            .try_recv()
            .expect("one new bypass is available");
        status.servable_low = block::Height(2);
        routine.handle_status(status);
        status.servable_low = block::Height(1);
        routine.handle_status(status);
        routine.state.inbound_status_meter.next_allowed = Instant::now();
        routine.flush_pending_status();
        assert!(reactor_events.try_recv().is_err());
        assert_eq!(routine.state.servable_low, block::Height(1));
    }

    #[tokio::test]
    async fn a_throttled_limits_only_status_applies_when_the_window_opens() {
        let (mut routine, mut outbound, mut reactor_events) = status_test_routine();
        let mut status = BlockSyncStatus {
            servable_low: block::Height(1),
            servable_high: block::Height(3),
            tip_hash: block::Hash([3; 32]),
            max_blocks_per_response: 1,
            max_inflight_requests: 8,
            max_response_bytes: routine.config.advertised_max_response_bytes(),
        };
        routine.handle_status(status);
        let _ = reactor_events
            .try_recv()
            .expect("initial status is published");
        assert_eq!(routine.state.window.max_inflight_requests, 8);

        // A second Status, buffered behind a read pause, only lowers the limit.
        routine.state.inbound_status_meter.next_allowed = Instant::now() + Duration::from_secs(60);
        status.max_inflight_requests = 1;
        routine.handle_status(status);
        assert_eq!(
            routine.state.window.max_inflight_requests, 1,
            "new requests obey the lowered limit before the meter opens"
        );
        assert!(
            reactor_events.try_recv().is_err(),
            "publication still waits for the meter"
        );
        // One-block requests for three heights: only one may be in flight.
        routine.io.work.extend(
            super::super::test_work_scope(),
            (1..=3).map(|height| {
                (
                    block::Height(height),
                    block::Hash([u8::try_from(height).unwrap(); 32]),
                    BlockSizeEstimate::Advertised(1_000),
                )
            }),
        );
        routine.try_fill();
        let issued = std::iter::from_fn(|| outbound.try_recv().ok())
            .filter(|frame| {
                matches!(
                    BlockSyncMessage::decode_frame(frame.clone()),
                    Ok(BlockSyncMessage::GetBlocks { .. })
                )
            })
            .count();
        assert_eq!(
            issued, 1,
            "no request is issued against the superseded limit"
        );
        // A repeat must not cancel the unpublished change.
        routine.handle_status(status);

        routine.state.inbound_status_meter.next_allowed = Instant::now();
        routine.flush_pending_status();
        assert_eq!(
            routine.state.published_limits.2, 1,
            "the lowered limit is published"
        );
        assert!(matches!(
            reactor_events.try_recv(),
            Ok(RoutineToReactor::StatusReceived { .. })
        ));
    }

    #[tokio::test]
    async fn status_range_changes_bypass_throttle_without_extra_replies() {
        let (mut routine, mut out_recv, mut routine_to_reactor_rx) = status_test_routine();
        let config = routine.config.clone();
        let work = Arc::clone(&routine.io.work);
        let registry = Arc::clone(&routine.io.registry);
        let peer = routine.io.peer.clone();
        let mut status = BlockSyncStatus {
            servable_low: block::Height(1),
            servable_high: block::Height(3),
            tip_hash: block::Hash([3; 32]),
            max_blocks_per_response: config.advertised_max_blocks_per_response(),
            max_inflight_requests: config.advertised_max_inflight_requests(),
            max_response_bytes: config.advertised_max_response_bytes(),
        };
        routine.handle_status(status);
        assert!(matches!(
            routine_to_reactor_rx.try_recv(),
            Ok(RoutineToReactor::StatusReceived {
                send_reply: true,
                ..
            })
        ));

        // Keep both windows closed without depending on the test's runtime speed.
        let next_allowed = Instant::now() + Duration::from_secs(60);
        routine.state.inbound_status_meter.next_allowed = next_allowed;
        routine.state.status_reply_meter.next_allowed = next_allowed;
        for (index, (low, high)) in [(2, 3), (1, 3), (1, 2), (1, 3), (0, 3), (0, 0), (2, 3)]
            .into_iter()
            .enumerate()
        {
            if index > 0 {
                routine.state.inbound_status_meter.next_allowed = Instant::now();
                routine.handle_status(status);
                let _ = routine_to_reactor_rx
                    .try_recv()
                    .expect("new status window opens");
                routine.state.inbound_status_meter.next_allowed = next_allowed;
            }
            status.servable_low = block::Height(low);
            status.servable_high = block::Height(high);
            routine.handle_status(status);
            assert_eq!(
                (routine.state.servable_low, routine.state.servable_high),
                (status.servable_low, status.servable_high),
                "range changes must apply while the inbound meter is closed"
            );
            assert_eq!(
                registry.candidate_snapshot(),
                vec![(
                    peer.clone(),
                    true,
                    status.servable_low,
                    status.servable_high
                )]
            );
            assert!(matches!(
                routine_to_reactor_rx.try_recv(),
                Ok(RoutineToReactor::StatusReceived {
                    send_reply: false,
                    ..
                })
            ));
        }

        let old_limits = routine.state.published_limits;
        status.max_response_bytes /= 2;
        routine.handle_status(status);
        assert_eq!(routine.state.published_limits, old_limits);
        assert_eq!(routine.state.max_response_bytes, status.max_response_bytes);
        assert!(
            routine_to_reactor_rx.try_recv().is_err(),
            "unchanged ranges must still be rate limited"
        );

        work.extend(
            super::super::test_work_scope(),
            [
                (
                    block::Height(1),
                    block::Hash([1; 32]),
                    BlockSizeEstimate::Advertised(1_000),
                ),
                (
                    block::Height(2),
                    block::Hash([2; 32]),
                    BlockSizeEstimate::Advertised(1_000),
                ),
            ],
        );
        routine.try_fill();
        let frame = timeout(Duration::from_secs(5), out_recv.recv())
            .await
            .expect("retained block request is sent")
            .expect("outbound stream remains open");
        assert!(matches!(
            BlockSyncMessage::decode_frame(frame).expect("request decodes"),
            BlockSyncMessage::GetBlocks {
                start_height: block::Height(2),
                count: 1,
            }
        ));
        assert!(
            work.pending_contains(block::Height(1)),
            "the pruned block must remain available for another peer"
        );
    }

    #[tokio::test]
    async fn retry_filter_carries_the_checked_deadline_to_the_waiter() {
        let config = ZakuraBlockSyncConfig::default();
        let budget = ByteBudget::new(1_000_000);
        let work = Arc::new(WorkQueue::new(block::Height(0), budget.clone()));
        let scope = super::super::test_work_scope();
        let hash = block::Hash([1; 32]);
        work.extend(
            scope,
            [(block::Height(1), hash, BlockSizeEstimate::Advertised(1_000))],
        );

        let cancel = CancellationToken::new();
        let (out_send, mut out_recv) = framed_channel(16);
        let (_in_send, in_recv) = framed_channel(16);
        let peer = ZakuraPeerId::new(vec![7u8; 32]).expect("test peer id is within bounds");
        let session = BlockSyncPeerSession::for_test(peer.clone(), out_send, cancel.clone());
        let registry = Arc::new(PeerRegistry::new());
        let until = Instant::now() + Duration::from_secs(60);
        registry.defer_body_retry(
            [zakura_header_chain::SourceId::from_digest(peer.digest())],
            scope,
            hash,
            until,
        );

        let (sequencer_input_tx, _sequencer_input_rx) = mpsc::channel(16);
        let (routine_to_reactor_tx, _routine_to_reactor_rx) = mpsc::channel(16);
        let (_view_tx, view_rx) = watch::channel(initial_view(genesis_frontiers()));
        let (requester, serving) = test_authorization(
            &session,
            &in_recv,
            &config,
            test_request_pool(),
            &CancellationToken::new(),
        );
        let mut routine = PeerRoutine::new(
            peer,
            0,
            session,
            in_recv,
            requester,
            serving,
            config,
            true,
            0,
            Arc::clone(&work),
            registry,
            Arc::new(Mutex::new(ThroughputMeter::new(Instant::now()))),
            sequencer_input_tx,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            routine_to_reactor_tx,
            view_rx,
            cancel,
            ZakuraTrace::noop(),
        );
        routine.state.received_status = true;
        routine.state.servable_low = block::Height(1);
        routine.state.servable_high = block::Height(10);

        assert_eq!(routine.try_fill().retry_at, Some(until));
        assert!(work.pending_contains(block::Height(1)));
        assert!(!work.in_flight_contains(block::Height(1)));
        assert!(
            timeout(Duration::from_millis(1), out_recv.recv())
                .await
                .is_err(),
            "filtered work must not be sent"
        );
    }

    #[test]
    fn repeated_fill_stop_traces_are_sampled() {
        let now = Instant::now();

        assert!(super::fill_stop_trace_due(None, now));
        assert!(!super::fill_stop_trace_due(
            Some(now),
            now + Duration::from_secs(9)
        ));
        assert!(super::fill_stop_trace_due(
            Some(now),
            now + Duration::from_secs(10)
        ));
    }

    /// A floor request overdrafts a full in-flight budget by at most one request
    /// and is sent without a sequencer round trip.
    #[tokio::test]
    async fn floor_overdraft_is_bounded_and_immediate() {
        for unit in [CwndUnit::Bytes, CwndUnit::Blocks] {
            for measurement_age in [None, Some(Duration::ZERO), Some(Duration::from_secs(11))] {
                check_floor_overdraft_and_deadline(unit, measurement_age).await;
            }
        }
    }

    async fn check_floor_overdraft_and_deadline(unit: CwndUnit, measurement_age: Option<Duration>) {
        let config = ZakuraBlockSyncConfig {
            bbr_cwnd_unit: unit,
            bbr_delivery_rate_window: Duration::from_secs(10),
            ..ZakuraBlockSyncConfig::default()
        };

        // A byte budget reserved down to exactly zero free: the case that used to wedge.
        let budget = ByteBudget::new(8_192);
        let _unrelated = budget.reserve_owned(8_192).unwrap();
        assert_eq!(budget.available(), 0, "the budget is exactly full");

        // The floor height (1) is pending and servable by this peer; the download floor
        // is 0 so height 1 is the floor.
        let work = Arc::new(WorkQueue::new(block::Height(0), budget.clone()));
        assert_eq!(
            work.extend(
                super::super::test_work_scope(),
                [(
                    block::Height(1),
                    block::Hash([1; 32]),
                    BlockSizeEstimate::Advertised(1_000),
                )]
            ),
            1,
        );

        let cancel = CancellationToken::new();
        let (out_send, mut out_recv) = framed_channel(16);
        let (_in_send, in_recv) = framed_channel(16);
        let peer = ZakuraPeerId::new(vec![7u8; 32]).expect("test peer id is within bounds");
        let session = BlockSyncPeerSession::for_test(peer.clone(), out_send, cancel.clone());

        let (sequencer_input_tx, _sequencer_input_rx) = mpsc::channel(16);
        let (routine_to_reactor_tx, _routine_to_reactor_rx) = mpsc::channel(16);
        let (_view_tx, view_rx) = watch::channel(initial_view(genesis_frontiers()));

        let (requester, serving) = test_authorization(
            &session,
            &in_recv,
            &config,
            test_request_pool(),
            &CancellationToken::new(),
        );
        let mut routine = PeerRoutine::new(
            peer,
            0,
            session,
            in_recv,
            requester,
            serving,
            config,
            true,
            0,
            work.clone(),
            Arc::new(PeerRegistry::new()),
            Arc::new(Mutex::new(ThroughputMeter::new(Instant::now()))),
            sequencer_input_tx,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            routine_to_reactor_tx,
            view_rx,
            cancel,
            ZakuraTrace::noop(),
        );
        // The routine learns these from a `Status` frame in production; set them directly
        // so a single `try_fill` pass exercises the floor arm.
        routine.state.received_status = true;
        routine.state.servable_low = block::Height(1);
        routine.state.servable_high = block::Height(10);

        if let Some(age) = measurement_age {
            let delivered_at = Instant::now() - age;
            let elapsed = Duration::from_secs(1);
            let snapshot = routine
                .state
                .window
                .delivery_snapshot(delivered_at - elapsed);
            routine
                .state
                .window
                .record_delivery(delivered_at, elapsed, 1, 256 * 1024, snapshot);
        }
        routine.try_fill();

        let outstanding = routine.state.window.outstanding.first().unwrap();
        let base = if measurement_age == Some(Duration::ZERO) {
            routine.config.effective_floor_rescue_timeout()
        } else {
            routine.config.request_timeout
        };
        let transfer = Duration::from_secs_f64(
            f64::from(u32::try_from(outstanding.request.estimated_bytes).unwrap())
                / (256.0 * 1024.0),
        );
        assert_eq!(
            outstanding.deadline,
            outstanding.queued_at + base + transfer,
            "unit={unit:?}, measurement_age={measurement_age:?}"
        );

        // The floor request went out synchronously (no funding round trip)…
        let frame = timeout(Duration::from_secs(5), out_recv.recv())
            .await
            .expect("the floor GetBlocks is sent within the timeout");
        assert!(
            frame.is_some(),
            "an exhausted budget must not block the floor request",
        );
        // …and the budget recorded a bounded overdraft: exactly the floor request's
        // marked size-estimate reservation past the configured maximum.
        let marked_estimate = work.reserved_bytes();
        assert!(
            marked_estimate > 0,
            "the floor request marked a reservation"
        );
        assert_eq!(
            budget.reserved(),
            8_192 + marked_estimate,
            "the floor reservation overdrafts by one request's estimate",
        );
        assert!(
            !work.pending_contains(block::Height(1)),
            "the floor height was taken, not returned",
        );
    }

    /// A routine for peer `[id; 32]` that shares `work` and `registry`
    /// and reads the download floor from `view`. It has received a `Status`
    /// that makes heights 1 to 10 servable.
    fn shared_floor_routine(
        id: u8,
        work: &Arc<WorkQueue>,
        registry: &Arc<PeerRegistry>,
        view: watch::Receiver<super::super::sequencer_task::SequencerView>,
    ) -> (PeerRoutine, crate::zakura::FramedRecv) {
        let config = ZakuraBlockSyncConfig::default();
        let cancel = CancellationToken::new();
        let (out_send, out_recv) = framed_channel(16);
        let (_in_send, in_recv) = framed_channel(16);
        let peer = ZakuraPeerId::new(vec![id; 32]).expect("test peer id is within bounds");
        let session = BlockSyncPeerSession::for_test(peer.clone(), out_send, cancel.clone());
        let generation = registry
            .admit_session(
                &peer,
                session.direction(),
                &config,
                u64::from(id),
                Instant::now(),
            )
            .generation();
        let (sequencer_input_tx, _sequencer_input_rx) = mpsc::channel(16);
        let (routine_to_reactor_tx, _routine_to_reactor_rx) = mpsc::channel(16);
        let (requester, serving) = test_authorization(
            &session,
            &in_recv,
            &config,
            test_request_pool(),
            &CancellationToken::new(),
        );
        let mut routine = PeerRoutine::new(
            peer,
            u64::from(id),
            session,
            in_recv,
            requester,
            serving,
            config,
            true,
            generation,
            Arc::clone(work),
            Arc::clone(registry),
            Arc::new(Mutex::new(ThroughputMeter::new(Instant::now()))),
            sequencer_input_tx,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            routine_to_reactor_tx,
            view,
            cancel,
            ZakuraTrace::noop(),
        );
        routine.state.received_status = true;
        routine.state.servable_low = block::Height(1);
        routine.state.servable_high = block::Height(10);
        (routine, out_recv)
    }

    /// Each outstanding height, read from the routine's request handles, from
    /// the registry's published claims, and from the work queue's items. All
    /// three name the same owner and the estimate reserved at admission, and
    /// those estimates are the budget's only charges. Dropping the routine
    /// empties all three owners and returns the budget to zero.
    #[tokio::test]
    async fn request_owners_agree_across_routine_registry_and_queue() {
        let budget = ByteBudget::new(1_000_000);
        let work = Arc::new(WorkQueue::new(block::Height(0), budget.clone()));
        work.extend(
            super::super::test_work_scope(),
            (1u8..=4).map(|height| {
                (
                    block::Height(u32::from(height)),
                    block::Hash([height; 32]),
                    BlockSizeEstimate::Advertised(1_000 * u32::from(height)),
                )
            }),
        );
        let registry = Arc::new(PeerRegistry::new());
        let (_view_tx, view_rx) = watch::channel(initial_view(genesis_frontiers()));
        let (mut routine, mut out_recv) = shared_floor_routine(7, &work, &registry, view_rx);
        let _ = routine.try_fill();

        let requests = routine.request_ownership();
        assert!(!requests.is_empty(), "the routine issued a request");
        assert!(
            requests
                .iter()
                .all(|request| request.write == RequestWritePhase::Queued),
            "transport hasn't taken the frames: {requests:?}"
        );
        let from_requests: BTreeMap<_, _> = requests
            .iter()
            .flat_map(|request| {
                request.heights.iter().map(move |requested| {
                    assert!(!requested.received);
                    (requested.height, (request.owner, requested.estimated_bytes))
                })
            })
            .collect();
        let from_registry: BTreeMap<_, _> = registry
            .outstanding_snapshot(&routine.io.peer, routine.io.generation)
            .into_iter()
            .map(|(height, meta)| (height, (meta.owner, meta.estimated_bytes)))
            .collect();
        assert!(
            registry
                .outstanding_snapshot(&routine.io.peer, routine.io.generation + 1)
                .is_empty(),
            "another generation's routine sees none of these claims"
        );
        let from_queue: BTreeMap<_, _> = work
            .ownership_snapshot()
            .into_iter()
            .filter(|item| item.place == WorkItemPlace::InFlight)
            .map(|item| {
                assert_eq!(item.reserved_bytes, item.estimated_bytes);
                (
                    item.height,
                    (
                        item.owner.expect("an in-flight item has an owner"),
                        item.estimated_bytes,
                    ),
                )
            })
            .collect();
        assert_eq!(from_requests, from_registry);
        assert_eq!(from_requests, from_queue);
        assert_eq!(
            budget.reserved(),
            from_requests
                .values()
                .map(|(_, estimate)| estimate)
                .sum::<u64>()
        );

        // The in-memory transport writes every frame it takes, so each
        // handle records a completed write.
        while let Ok(Some(_)) = timeout(Duration::from_millis(1), out_recv.recv()).await {}
        assert!(routine
            .request_ownership()
            .iter()
            .all(|request| request.write == RequestWritePhase::Written));

        let (peer, generation) = (routine.io.peer.clone(), routine.io.generation);
        drop(routine);
        assert!(registry.outstanding_snapshot(&peer, generation).is_empty());
        assert!(work
            .ownership_snapshot()
            .iter()
            .all(|item| item.place == WorkItemPlace::Pending && item.reserved_bytes == 0));
        assert_eq!(budget.reserved(), 0);
    }

    /// A handoff that ends no reservation reports nothing, so each reported
    /// handoff pairs with one budget release.
    #[tokio::test]
    async fn a_handoff_without_a_charge_reports_nothing() {
        use super::super::block_sync_observe::{BlockEvent, BlockSyncObserver, ObserverSlot};

        #[derive(Debug, Default)]
        struct Log(Mutex<Vec<BlockEvent>>);
        impl BlockSyncObserver for Log {
            fn observe(&self, event: BlockEvent) {
                self.0.lock().unwrap().push(event);
            }
        }

        let budget = ByteBudget::new(1_000_000);
        let work = Arc::new(WorkQueue::new(block::Height(0), budget.clone()));
        work.extend(
            super::super::test_work_scope(),
            [(
                block::Height(1),
                block::Hash([1; 32]),
                BlockSizeEstimate::Advertised(1_000),
            )],
        );
        let registry = Arc::new(PeerRegistry::new());
        let (_view_tx, view_rx) = watch::channel(initial_view(genesis_frontiers()));
        let (routine, _out_recv) = shared_floor_routine(7, &work, &registry, view_rx);
        let log = Arc::new(Log::default());
        let mut routine = routine.with_observer(ObserverSlot(Some(log.clone())));
        let _ = routine.try_fill();
        let owner = routine.request_ownership()[0].owner;
        log.0.lock().unwrap().clear();

        drop(super::ReceiptSettlement::new(
            &routine,
            owner,
            block::Height(1),
            budget.empty_reservation(),
        ));
        let events = std::mem::take(&mut *log.0.lock().unwrap());
        assert_eq!(events, []);
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum DeadlineWriteState {
        Queued,
        Started,
        Written,
    }

    #[tokio::test]
    async fn deadline_expiry_only_penalizes_requests_that_started() {
        use DeadlineWriteState::*;

        for writes in [
            vec![Queued],
            vec![Started],
            vec![Written],
            vec![Started, Queued],
            vec![Written, Queued],
            vec![Written, Started, Queued],
        ] {
            for progress_after_first in [false, true] {
                check_deadline_write_states(&writes, progress_after_first).await;
            }
        }
    }

    async fn check_deadline_write_states(
        writes: &[DeadlineWriteState],
        progress_after_first: bool,
    ) {
        let config = ZakuraBlockSyncConfig {
            initial_block_probe_requests: u32::try_from(writes.len()).unwrap(),
            ..ZakuraBlockSyncConfig::default()
        };
        let liveness_timeout = config.effective_liveness_timeout();
        let mut expected_window = super::DownloadWindow::new(&config);
        let budget = ByteBudget::new(1_000_000);
        let work = Arc::new(WorkQueue::new(block::Height(0), budget.clone()));
        work.set_estimate_floor_for_tests(1);
        let cancel = CancellationToken::new();
        let (out_send, mut out_recv) = crate::zakura::transport::worker_framed_channel(16);
        let (_in_send, in_recv) = framed_channel(16);
        let peer = ZakuraPeerId::new(vec![9u8; 32]).unwrap();
        let session = BlockSyncPeerSession::for_test(peer.clone(), out_send, cancel.clone());
        let registry = Arc::new(PeerRegistry::new());
        let generation = registry
            .admit_session(
                &peer,
                crate::zakura::ServicePeerDirection::Outbound,
                &config,
                0,
                Instant::now(),
            )
            .generation();
        let (sequencer_input_tx, _sequencer_input_rx) = mpsc::channel(16);
        let (routine_to_reactor_tx, _routine_to_reactor_rx) = mpsc::channel(16);
        let (_view_tx, view_rx) = watch::channel(initial_view(genesis_frontiers()));
        let (requester, serving) = test_authorization(
            &session,
            &in_recv,
            &config,
            test_request_pool(),
            &CancellationToken::new(),
        );
        let mut routine = PeerRoutine::new(
            peer.clone(),
            0,
            session,
            in_recv,
            requester,
            serving,
            config,
            true,
            generation,
            work.clone(),
            registry.clone(),
            Arc::new(Mutex::new(ThroughputMeter::new(Instant::now()))),
            sequencer_input_tx,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            routine_to_reactor_tx,
            view_rx,
            cancel.clone(),
            ZakuraTrace::noop(),
        );
        routine.handle_status(super::BlockSyncStatus {
            servable_low: block::Height(1),
            servable_high: block::Height(10),
            max_blocks_per_response: 1,
            ..super::BlockSyncStatus::default()
        });

        let mut started_writes = Vec::new();
        let mut peer_timeouts = Vec::new();
        for (index, state) in writes.iter().enumerate() {
            let height = u8::try_from(index + 2).unwrap();
            work.extend(
                super::super::test_work_scope(),
                [(
                    block::Height(u32::from(height)),
                    block::Hash([height; 32]),
                    BlockSizeEstimate::Confirmed(1_000),
                )],
            );
            routine.try_fill();
            assert_eq!(routine.state.window.outstanding.len(), index + 1);
            match state {
                DeadlineWriteState::Queued => {}
                DeadlineWriteState::Started => {
                    let frame = out_recv.recv().await.unwrap();
                    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
                    let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
                    let writer = tokio::spawn(frame.write_with(move |_| async move {
                        started_tx.send(()).unwrap();
                        finish_rx.await.unwrap();
                        Ok::<_, ()>(())
                    }));
                    timeout(Duration::from_secs(1), started_rx)
                        .await
                        .unwrap()
                        .unwrap();
                    started_writes.push((finish_tx, writer));
                    peer_timeouts.push(block::Height(u32::from(height)));
                }
                DeadlineWriteState::Written => {
                    out_recv
                        .recv()
                        .await
                        .unwrap()
                        .write_with(|_| async { Ok::<_, ()>(()) })
                        .await
                        .unwrap();
                    peer_timeouts.push(block::Height(u32::from(height)));
                }
            }
            if progress_after_first && index == 0 {
                routine
                    .state
                    .window
                    .note_block_progress(Instant::now(), liveness_timeout);
            }
        }
        let first_owner = routine
            .state
            .window
            .outstanding
            .first()
            .unwrap()
            .request
            .owner;
        let liveness_deadline = routine.state.window.block_liveness_deadline.unwrap();
        let deadline = routine
            .state
            .window
            .outstanding
            .iter()
            .map(|request| request.deadline)
            .max()
            .unwrap();
        assert!(routine.expire_due_timeouts(deadline));
        if !peer_timeouts.is_empty() {
            expected_window.record_timeout(peer_timeouts.len());
        }
        assert_eq!(
            routine.state.window.bbr_reliability_permille(),
            expected_window.bbr_reliability_permille(),
            "only started writes count as peer failures: {writes:?}, progress={progress_after_first}"
        );
        assert_eq!(
            routine.state.window.bbr_effective_cwnd(),
            expected_window.bbr_effective_cwnd(),
            "queued-only expiry must not dip the congestion window"
        );
        assert_eq!(
            routine.state.window.requests_without_block_progress,
            u32::try_from(peer_timeouts.len())
                .unwrap()
                .saturating_sub(u32::from(
                    progress_after_first && writes[0] != DeadlineWriteState::Queued
                )),
        );
        assert_eq!(
            routine
                .state
                .retry_avoid
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            peer_timeouts,
        );
        assert!(routine.state.window.outstanding.is_empty());
        assert_eq!(budget.reserved(), 0);
        assert_eq!(work.reserved_bytes(), 0);
        assert_eq!(work.pending_len(), writes.len());
        for index in 0..writes.len() {
            let height = block::Height(u32::try_from(index + 2).unwrap());
            assert!(work.pending_contains(height));
            assert!(!registry.peer_has_outstanding_height(&peer, height));
        }
        assert_eq!(
            routine.state.window.check_liveness(liveness_deadline),
            if peer_timeouts.is_empty() {
                super::super::state::LivenessOutcome::Ok
            } else {
                super::super::state::LivenessOutcome::Park
            },
        );
        assert!(!cancel.is_cancelled());

        if peer_timeouts.is_empty() {
            // Reissue before the old queued frame is drained. Its later drop
            // must preserve the replacement's ownership and byte charge.
            routine.try_fill();
            assert_eq!(routine.state.window.outstanding.len(), 1);
            assert_ne!(
                routine
                    .state
                    .window
                    .outstanding
                    .first()
                    .unwrap()
                    .request
                    .owner,
                first_owner
            );
        }
        for (finish, writer) in started_writes {
            finish.send(()).unwrap();
            timeout(Duration::from_secs(1), writer)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
        for state in writes {
            if *state == DeadlineWriteState::Queued {
                out_recv
                    .recv()
                    .await
                    .unwrap()
                    .write_with(|_| async {
                        panic!("a request expired before writer startup must not reach the wire");
                        #[allow(unreachable_code)]
                        Ok::<_, ()>(())
                    })
                    .await
                    .unwrap();
            }
        }
        assert_eq!(
            budget.reserved(),
            if peer_timeouts.is_empty() { 1_000 } else { 0 }
        );
        assert_eq!(budget.reserved(), work.reserved_bytes());
        assert!(!cancel.is_cancelled());
    }

    #[derive(Clone, Copy)]
    enum ReceiptCleanup {
        BeforeWriter,
        Writer,
        CommittedGc,
        ObsoleteGc,
    }

    #[tokio::test]
    async fn skipped_write_retires_only_its_own_peer_obligation() {
        for order in [
            ReceiptCleanup::Writer,
            ReceiptCleanup::CommittedGc,
            ReceiptCleanup::ObsoleteGc,
        ] {
            for (other_request, progress_before_other) in
                [(false, false), (true, false), (true, true)]
            {
                check_skipped_write_obligation(order, other_request, progress_before_other).await;
            }
        }
    }

    #[tokio::test]
    async fn competing_receipt_retires_unwritable_request_before_writer_reaches_it() {
        check_skipped_write_obligation(ReceiptCleanup::BeforeWriter, false, false).await;
    }

    async fn check_skipped_write_obligation(
        order: ReceiptCleanup,
        other_request: bool,
        progress_before_other: bool,
    ) {
        let config = ZakuraBlockSyncConfig {
            request_timeout: if matches!(order, ReceiptCleanup::BeforeWriter) {
                Duration::from_secs(1)
            } else {
                ZakuraBlockSyncConfig::default().request_timeout
            },
            initial_block_probe_requests: if other_request { 2 } else { 1 },
            ..ZakuraBlockSyncConfig::default()
        };
        let liveness_timeout = config.effective_liveness_timeout();
        let budget = ByteBudget::new(1_000_000);
        let work = Arc::new(WorkQueue::new(block::Height(0), budget.clone()));
        work.set_estimate_floor_for_tests(1);
        let add_work = |height: u8| {
            work.extend(
                super::super::test_work_scope(),
                [(
                    block::Height(u32::from(height)),
                    block::Hash([height; 32]),
                    BlockSizeEstimate::Confirmed(
                        if matches!(order, ReceiptCleanup::BeforeWriter) {
                            1_000_000
                        } else {
                            1_000
                        },
                    ),
                )],
            );
        };
        // Height 1 stays missing, so the competing body at height 2 cannot move
        // the committed floor and conceal a stale outstanding request.
        add_work(2);
        let cancel = CancellationToken::new();
        let (out_send, mut out_recv) = crate::zakura::transport::worker_framed_channel(16);
        let (_in_send, in_recv) = framed_channel(16);
        let peer = ZakuraPeerId::new(vec![9u8; 32]).unwrap();
        let session = BlockSyncPeerSession::for_test(peer.clone(), out_send, cancel.clone());
        let registry = Arc::new(PeerRegistry::new());
        let generation = registry
            .admit_session(
                &peer,
                crate::zakura::ServicePeerDirection::Outbound,
                &config,
                0,
                Instant::now(),
            )
            .generation();
        let (sequencer_input_tx, _sequencer_input_rx) = mpsc::channel(16);
        let (routine_to_reactor_tx, _routine_to_reactor_rx) = mpsc::channel(16);
        let (view_tx, view_rx) = watch::channel(initial_view(genesis_frontiers()));
        let (requester, serving) = test_authorization(
            &session,
            &in_recv,
            &config,
            test_request_pool(),
            &CancellationToken::new(),
        );
        let mut routine = PeerRoutine::new(
            peer.clone(),
            0,
            session,
            in_recv,
            requester,
            serving,
            config,
            true,
            generation,
            work.clone(),
            registry.clone(),
            Arc::new(Mutex::new(ThroughputMeter::new(Instant::now()))),
            sequencer_input_tx,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            routine_to_reactor_tx,
            view_rx,
            cancel.clone(),
            ZakuraTrace::noop(),
        );
        routine.handle_status(super::BlockSyncStatus {
            servable_low: block::Height(1),
            servable_high: block::Height(10),
            max_blocks_per_response: 1,
            ..super::BlockSyncStatus::default()
        });
        routine.try_fill();
        assert_eq!(routine.state.window.outstanding.len(), 1);
        let owner = routine
            .state
            .window
            .outstanding
            .first()
            .unwrap()
            .request
            .owner;
        let deadline = routine.state.window.outstanding.first().unwrap().deadline;
        let queued = out_recv.recv().await.unwrap();
        assert!(registry.peer_has_outstanding_height(&peer, block::Height(2)));

        if progress_before_other {
            routine
                .state
                .window
                .note_block_progress(Instant::now(), liveness_timeout);
        }
        if other_request {
            add_work(3);
            routine.try_fill();
            assert_eq!(routine.state.window.outstanding.len(), 2);
        }
        let skipped = work.subscribe_available().notified();
        tokio::pin!(skipped);
        skipped.as_mut().enable();
        // Another peer wins height 2 before this writer starts. Its body retains
        // this exact owner until the sequencer consumes it.
        drop(
            work.begin_receipt_for_owner(owner, block::Height(2))
                .unwrap()
                .unwrap(),
        );
        match order {
            ReceiptCleanup::BeforeWriter => {
                let liveness_deadline = routine.state.window.block_liveness_deadline.unwrap();
                assert!(
                    liveness_deadline < deadline,
                    "transfer allowance outlasts liveness"
                );
                assert_eq!(work.owner_for_height(block::Height(2)), Some(owner));
                assert_eq!(budget.reserved(), 0);
                routine.gc_obsolete_outstanding();
                let result = routine.handle_deadlines(liveness_deadline);
                assert!(
                    result.is_ok(),
                    "a request made unwritable by receipt must not park its peer: {result:?}"
                );
                assert!(routine.state.window.outstanding.is_empty());
                return;
            }
            ReceiptCleanup::Writer => {}
            ReceiptCleanup::CommittedGc => {
                work.advance_floor(block::Height(2));
                view_tx.send_modify(|view| view.download_floor = block::Height(2));
                routine.gc_committed_outstanding();
            }
            ReceiptCleanup::ObsoleteGc => {
                work.advance_floor(block::Height(2));
                routine.gc_obsolete_outstanding();
            }
        }
        queued
            .write_with(|_| async {
                panic!("a superseded request must not reach the wire");
                #[allow(unreachable_code)]
                Ok::<_, ()>(())
            })
            .await
            .unwrap();
        if matches!(order, ReceiptCleanup::Writer) {
            timeout(Duration::from_secs(1), skipped)
                .await
                .expect("receipt retirement must wake its routine");
        }
        if other_request {
            out_recv
                .recv()
                .await
                .unwrap()
                .write_with(|_| async { Ok::<_, ()>(()) })
                .await
                .unwrap();
        }

        // Deadline handling must reconcile the skipped write before scoring a
        // timeout, even if it was the event that woke the routine.
        routine.handle_deadlines(deadline).unwrap();
        assert!(!registry.peer_has_outstanding_height(&peer, block::Height(2)));
        assert_eq!(
            work.owner_for_height(block::Height(2)),
            matches!(order, ReceiptCleanup::Writer).then_some(owner),
        );
        assert!(!work.pending_contains(block::Height(2)));
        assert!(routine.state.retry_avoid.is_empty());
        assert!(!cancel.is_cancelled());
        if other_request {
            assert_eq!(routine.state.window.outstanding.len(), 1);
            assert_eq!(routine.state.window.requests_without_block_progress, 1);
            assert!(registry.peer_has_outstanding_height(&peer, block::Height(3)));
            let liveness_deadline = routine.state.window.block_liveness_deadline.unwrap();
            assert_eq!(
                routine.state.window.check_liveness(liveness_deadline),
                super::super::state::LivenessOutcome::Park
            );
            assert_eq!(budget.reserved(), 1_000);
        } else {
            assert!(routine.state.window.outstanding.is_empty());
            assert_eq!(routine.state.window.requests_without_block_progress, 0);
            assert!(routine.state.window.block_liveness_deadline.is_none());
            routine
                .handle_deadlines(deadline + liveness_timeout)
                .unwrap();
            assert_eq!(budget.reserved(), 0);
            add_work(3);
            routine.try_fill();
            assert_eq!(
                routine.state.window.outstanding.len(),
                1,
                "the cold peer can probe again"
            );
        }
    }

    #[tokio::test]
    async fn routine_drop_leaves_a_body_won_by_another_peer_to_the_sequencer() {
        let config = ZakuraBlockSyncConfig::default();

        // Ample budget so the floor take reserves directly (no funding round-trip)
        // and sends a real request, creating the outstanding claim.
        let budget = ByteBudget::new(1_000_000);
        let budget_probe = budget.clone();

        // Height 1 is the floor (download floor is 0) and this peer's only work item.
        let work = Arc::new(WorkQueue::new(block::Height(0), budget.clone()));
        work.set_estimate_floor_for_tests(1);
        assert_eq!(
            work.extend(
                super::super::test_work_scope(),
                [(
                    block::Height(1),
                    block::Hash([1; 32]),
                    BlockSizeEstimate::Advertised(1_000),
                )]
            ),
            1,
        );

        let cancel = CancellationToken::new();
        let (out_send, _out_recv) = framed_channel(16);
        let (_in_send, in_recv) = framed_channel(16);
        let peer = ZakuraPeerId::new(vec![9u8; 32]).expect("test peer id is within bounds");
        let session = BlockSyncPeerSession::for_test(peer.clone(), out_send, cancel.clone());

        let (sequencer_input_tx, _sequencer_input_rx) = mpsc::channel(16);
        let (routine_to_reactor_tx, _routine_to_reactor_rx) = mpsc::channel(16);
        let (_view_tx, view_rx) = watch::channel(initial_view(genesis_frontiers()));

        let (requester, serving) = test_authorization(
            &session,
            &in_recv,
            &config,
            test_request_pool(),
            &CancellationToken::new(),
        );
        let mut routine = PeerRoutine::new(
            peer,
            0,
            session,
            in_recv,
            requester,
            serving,
            config,
            true,
            0,
            Arc::clone(&work),
            Arc::new(PeerRegistry::new()),
            Arc::new(Mutex::new(ThroughputMeter::new(Instant::now()))),
            sequencer_input_tx,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            routine_to_reactor_tx,
            view_rx,
            cancel,
            ZakuraTrace::noop(),
        );
        routine.state.received_status = true;
        routine.state.servable_low = block::Height(1);
        routine.state.servable_high = block::Height(10);

        // One fill pass: the routine reserves height 1's estimate and sends its
        // request, creating an outstanding claim for a still-reserved height.
        routine.try_fill();
        assert!(
            work.in_flight_contains(block::Height(1)),
            "height 1 is reserved and outstanding after the fill"
        );
        assert!(!work.pending_contains(block::Height(1)));
        assert_eq!(budget_probe.reserved(), 1_000);
        assert_eq!(routine.state.window.outstanding.len(), 1);

        // A competing peer delivers height 1 first: its receipt ends the shared
        // request reservation. The winner releases the estimate to the ByteBudget
        // only after its forward, so it is still charged here.
        let owner = work.owner_for_height(block::Height(1)).unwrap();
        let settlement = work
            .begin_receipt_for_owner(owner, block::Height(1))
            .unwrap()
            .expect("height 1 still owns its active reservation");
        assert_eq!(settlement.funding.bytes(), 1_000);
        assert_eq!(budget_probe.reserved(), 1_000);

        // Tear the routine down while it still lists height 1 as unreceived. `Drop`
        // is synchronous, so its cleanup is observable immediately.
        drop(routine);

        assert_eq!(
            budget_probe.reserved(),
            1_000,
            "Drop double-released the received height's ended reservation"
        );
        assert!(
            !work.pending_contains(block::Height(1)),
            "Drop phantom-re-queued a body already held in the commit pipeline"
        );
        assert!(
            work.in_flight_contains(block::Height(1)),
            "the received body stays in_flight for the Sequencer to commit"
        );
        drop(settlement);
        assert_eq!(budget_probe.reserved(), 0);
    }

    /// The liveness grace is granted only for genuinely-transient local write congestion:
    /// outbound full but full for *less* than `request_timeout`.
    #[test]
    fn liveness_grace_only_for_fresh_outbound_backpressure() {
        let now = Instant::now();
        let request_timeout = Duration::from_secs(8);

        // Grant a delay when the outbound queue filled one second ago.
        let fresh = now - Duration::from_secs(1);
        assert!(super::state::liveness_grace_allowed(
            true,
            Some(fresh),
            now,
            request_timeout
        ));

        // Disconnect when the outbound queue stays full for `request_timeout`.
        let sustained = now - request_timeout;
        assert!(!super::state::liveness_grace_allowed(
            true,
            Some(sustained),
            now,
            request_timeout
        ));
        let long = now - Duration::from_secs(30);
        assert!(!super::state::liveness_grace_allowed(
            true,
            Some(long),
            now,
            request_timeout
        ));

        // Disconnect normally when the outbound queue has capacity.
        assert!(!super::state::liveness_grace_allowed(
            false,
            Some(fresh),
            now,
            request_timeout
        ));
        // Refuse a delay when a full queue has no recorded start time.
        assert!(!super::state::liveness_grace_allowed(
            true,
            None,
            now,
            request_timeout
        ));
    }

    #[tokio::test]
    async fn cancelled_session_settles_peer_failures_before_reopening() {
        for failure in [
            super::OrderedStreamFailure::RemoteClose,
            super::OrderedStreamFailure::WriteTimeout,
        ] {
            check_cancelled_session_policy(Some(failure), true, false, false).await;
            check_cancelled_session_policy(Some(failure), true, false, true).await;
            check_cancelled_session_policy(Some(failure), true, true, false).await;
        }
    }

    #[tokio::test]
    async fn local_cancel_and_idle_write_timeout_do_not_park_peers() {
        check_cancelled_session_policy(None, true, false, false).await;
        for buffered in [
            BufferedResponse::Backpressured,
            BufferedResponse::AlreadyProcessing,
        ] {
            check_cancelled_session_with_buffered_response(None, true, false, false, buffered)
                .await;
        }
        check_cancelled_session_policy(
            Some(super::OrderedStreamFailure::WriteTimeout),
            false,
            false,
            false,
        )
        .await;
    }

    async fn check_cancelled_session_policy(
        failure: Option<super::OrderedStreamFailure>,
        unanswered: bool,
        readmitted: bool,
        expired: bool,
    ) {
        check_cancelled_session_with_buffered_response(
            failure,
            unanswered,
            readmitted,
            expired,
            BufferedResponse::None,
        )
        .await;
    }

    #[derive(Clone, Copy, PartialEq)]
    enum BufferedResponse {
        None,
        Complete,
        CompleteWhileOutboundFull,
        StatusOnly,
        Malformed,
        MalformedBlock,
        MalformedBlockBackpressured,
        MalformedBlockAlreadyProcessing,
        Backpressured,
        AlreadyProcessing,
        ResetDuringBackpressure,
    }

    #[tokio::test]
    async fn malformed_blocks_are_validated_before_stream_failure_settlement() {
        for failure in [
            super::OrderedStreamFailure::RemoteClose,
            super::OrderedStreamFailure::WriteTimeout,
        ] {
            for readmitted in [false, true] {
                for buffered in [
                    BufferedResponse::MalformedBlock,
                    BufferedResponse::MalformedBlockBackpressured,
                    BufferedResponse::MalformedBlockAlreadyProcessing,
                ] {
                    check_cancelled_session_with_buffered_response(
                        Some(failure),
                        true,
                        readmitted,
                        false,
                        buffered,
                    )
                    .await;
                }
            }
        }
    }

    #[tokio::test]
    async fn buffered_responses_are_considered_before_stream_failure() {
        for failure in [
            super::OrderedStreamFailure::RemoteClose,
            super::OrderedStreamFailure::WriteTimeout,
        ] {
            for readmitted in [false, true] {
                for buffered in [
                    BufferedResponse::Complete,
                    BufferedResponse::CompleteWhileOutboundFull,
                    BufferedResponse::StatusOnly,
                    BufferedResponse::Malformed,
                    BufferedResponse::Backpressured,
                    BufferedResponse::ResetDuringBackpressure,
                ] {
                    check_cancelled_session_with_buffered_response(
                        Some(failure),
                        true,
                        readmitted,
                        false,
                        buffered,
                    )
                    .await;
                }
            }
        }
    }

    #[tokio::test]
    async fn cancellation_during_local_body_backpressure_does_not_charge_a_stall() {
        for readmitted in [false, true] {
            check_cancelled_session_with_buffered_response(
                Some(super::OrderedStreamFailure::RemoteClose),
                true,
                readmitted,
                false,
                BufferedResponse::AlreadyProcessing,
            )
            .await;
        }
    }

    async fn check_cancelled_session_with_buffered_response(
        failure: Option<super::OrderedStreamFailure>,
        unanswered: bool,
        readmitted: bool,
        expired: bool,
        buffered: BufferedResponse,
    ) {
        use super::super::peer_registry::SessionAdmission;
        use crate::zakura::transport::OrderedStreamFailureCause;
        use crate::zakura::{ServicePeerDirection, SinkReject};

        use zakura_chain::serialization::{ZcashDeserializeInto, ZcashSerialize};
        let body: Arc<block::Block> = Arc::new(
            zakura_test::vectors::BLOCK_MAINNET_1_BYTES
                .zcash_deserialize_into()
                .unwrap(),
        );
        let config = ZakuraBlockSyncConfig::default();
        let budget = ByteBudget::new(1_000_000);
        let work = Arc::new(WorkQueue::new(block::Height(0), budget.clone()));
        work.set_estimate_floor_for_tests(1);
        if unanswered {
            work.extend(
                super::super::test_work_scope(),
                [(
                    block::Height(1),
                    body.hash(),
                    BlockSizeEstimate::Confirmed(
                        u32::try_from(zakura_test::vectors::BLOCK_MAINNET_1_BYTES.len()).unwrap(),
                    ),
                )],
            );
        }
        let peer = ZakuraPeerId::new(vec![19; 32]).unwrap();
        let registry = Arc::new(PeerRegistry::new());
        let now = Instant::now();
        let mut generation = registry
            .admit_session(&peer, ServicePeerDirection::Outbound, &config, 0, now)
            .generation();
        if readmitted {
            assert!(registry.park_session(&peer, 0, generation, now));
            let admission =
                registry.admit_session(&peer, ServicePeerDirection::Outbound, &config, 0, now);
            assert!(matches!(admission, SessionAdmission::Readmitted { .. }));
            generation = admission.generation();
        }
        let cancel = CancellationToken::new();
        let (out_send, mut out_recv) = crate::zakura::transport::worker_framed_channel(4);
        let fill_outbound = out_send.clone();
        let (in_send, in_recv) = framed_channel(4);
        let cause = OrderedStreamFailureCause::default();
        let session = BlockSyncPeerSession::for_test(peer.clone(), out_send, cancel.clone());
        let (sequencer_input, mut sequencer_recv) = mpsc::channel(4);
        let mut held_capacity = Vec::new();
        if matches!(
            buffered,
            BufferedResponse::Backpressured
                | BufferedResponse::AlreadyProcessing
                | BufferedResponse::ResetDuringBackpressure
                | BufferedResponse::MalformedBlockBackpressured
                | BufferedResponse::MalformedBlockAlreadyProcessing
        ) {
            for _ in 0..4 {
                held_capacity.push(sequencer_input.clone().try_reserve_owned().unwrap());
            }
        }
        let (reactor_input, _reactor_recv) = mpsc::channel(4);
        let (view_tx, view_rx) = watch::channel(initial_view(genesis_frontiers()));
        let in_recv = in_recv.with_failure_cause(cause.clone());
        let (requester, serving) = test_authorization(
            &session,
            &in_recv,
            &config,
            test_request_pool(),
            &CancellationToken::new(),
        );
        let mut routine = PeerRoutine::new(
            peer.clone(),
            0,
            session,
            in_recv,
            requester,
            serving,
            config.clone(),
            !readmitted,
            generation,
            work.clone(),
            registry.clone(),
            Arc::new(Mutex::new(ThroughputMeter::new(now))),
            sequencer_input,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            reactor_input,
            view_rx,
            cancel.clone(),
            ZakuraTrace::noop(),
        );
        if unanswered {
            routine.handle_status(super::BlockSyncStatus {
                servable_low: block::Height(1),
                servable_high: block::Height(1),
                max_blocks_per_response: 1,
                ..super::BlockSyncStatus::default()
            });
            routine.try_fill();
            assert_eq!(routine.state.window.outstanding.len(), 1);
            timeout(Duration::from_secs(1), out_recv.recv())
                .await
                .unwrap()
                .unwrap()
                .write_with(|_| async { Ok::<_, std::convert::Infallible>(()) })
                .await
                .unwrap();
            assert!(work.in_flight_contains(block::Height(1)));
            if expired {
                let after_deadline = routine.state.window.outstanding.first().unwrap().deadline
                    + Duration::from_millis(1);
                routine.expire_due_timeouts(after_deadline);
                assert!(routine.state.window.outstanding.is_empty());
                assert!(routine.state.window.block_liveness_deadline.is_some());
            }
        }
        match buffered {
            BufferedResponse::Complete
            | BufferedResponse::CompleteWhileOutboundFull
            | BufferedResponse::Backpressured
            | BufferedResponse::AlreadyProcessing
            | BufferedResponse::ResetDuringBackpressure => {
                in_send
                    .send(
                        super::super::BlockSyncMessage::Block(body.clone())
                            .encode_frame()
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                in_send
                    .send(
                        super::super::BlockSyncMessage::BlocksDone {
                            start_height: block::Height(1),
                            returned: 1,
                        }
                        .encode_frame()
                        .unwrap(),
                    )
                    .await
                    .unwrap();
            }
            BufferedResponse::StatusOnly => {
                in_send
                    .send(
                        super::super::BlockSyncMessage::Status(super::BlockSyncStatus::default())
                            .encode_frame()
                            .unwrap(),
                    )
                    .await
                    .unwrap();
            }
            BufferedResponse::Malformed => {
                in_send
                    .send(crate::zakura::Frame {
                        message_type: u16::MAX,
                        flags: 0,
                        payload: vec![],
                    })
                    .await
                    .unwrap();
            }
            BufferedResponse::MalformedBlock
            | BufferedResponse::MalformedBlockBackpressured
            | BufferedResponse::MalformedBlockAlreadyProcessing => {
                // The requested header passes authorization, so the missing
                // transactions fail only in body decoding.
                let mut payload = vec![super::super::MSG_BS_BLOCK];
                body.header.zcash_serialize(&mut payload).unwrap();
                let frame = crate::zakura::Frame {
                    message_type: u16::from(super::super::MSG_BS_BLOCK),
                    flags: 0,
                    payload,
                };
                assert!(super::super::BlockSyncMessage::decode_frame(frame.clone()).is_err());
                in_send.send(frame).await.unwrap();
            }
            BufferedResponse::None => {}
        }
        if buffered == BufferedResponse::CompleteWhileOutboundFull {
            for _ in 0..4 {
                fill_outbound
                    .try_send(
                        super::super::BlockSyncMessage::Status(super::BlockSyncStatus::default())
                            .encode_frame()
                            .unwrap(),
                    )
                    .unwrap();
            }
            assert_eq!(routine.io.session.outbound_capacity(), 0);
        }
        let mut running = Box::pin(routine.run());
        if matches!(
            buffered,
            BufferedResponse::AlreadyProcessing | BufferedResponse::MalformedBlockAlreadyProcessing
        ) {
            // Poll until the body leaves the frame queue and waits for local
            // capacity. Cancellation must not turn that wait into a peer fault.
            assert!(futures::poll!(running.as_mut()).is_pending());
            assert_eq!(
                in_send.capacity(),
                if buffered == BufferedResponse::MalformedBlockAlreadyProcessing {
                    4
                } else {
                    3
                }
            );
        }
        if let Some(failure) = failure {
            cause.record(failure);
        }
        cancel.cancel();
        if failure.is_some() && !held_capacity.is_empty() {
            assert!(
                futures::poll!(running.as_mut()).is_pending(),
                "a failed stream must retain its unvalidated response until decode capacity returns"
            );
            assert!(work.in_flight_contains(block::Height(1)));
            assert!(budget.reserved() > 0);
            assert!(sequencer_recv.try_recv().is_err());
            assert!(!registry.is_peer_parked(&peer, Instant::now()));
            if buffered == BufferedResponse::ResetDuringBackpressure {
                work.reset_above(block::Height(0));
                view_tx.send_modify(|view| view.reset_epoch += 1);
            }
            drop(held_capacity);
        }
        let result = timeout(Duration::from_secs(1), running).await.unwrap();
        if matches!(
            buffered,
            BufferedResponse::Malformed
                | BufferedResponse::MalformedBlock
                | BufferedResponse::MalformedBlockBackpressured
                | BufferedResponse::MalformedBlockAlreadyProcessing
        ) {
            assert!(matches!(result, Err(SinkReject::Protocol(_))), "{result:?}");
            assert!(!registry.is_peer_parked(&peer, Instant::now()));
        } else if failure.is_some()
            && unanswered
            && matches!(
                buffered,
                BufferedResponse::None | BufferedResponse::StatusOnly
            )
        {
            assert!(matches!(result, Err(SinkReject::Local(_))), "{result:?}");
            if !readmitted {
                assert!(matches!(
                    registry.admit_session(
                        &peer,
                        ServicePeerDirection::Outbound,
                        &config,
                        0,
                        Instant::now()
                    ),
                    SessionAdmission::Parked,
                ));
                assert!(matches!(
                    registry.admit_session(
                        &peer,
                        ServicePeerDirection::Outbound,
                        &config,
                        0,
                        Instant::now() + config.effective_no_progress_peer_cooldown()
                    ),
                    SessionAdmission::Readmitted { .. },
                ));
            }
        } else {
            assert!(result.is_ok(), "{result:?}");
            assert!(!registry.is_peer_parked(&peer, Instant::now()));
        }
        assert_eq!(budget.reserved(), 0);
        let received = failure.is_some()
            && matches!(
                buffered,
                BufferedResponse::Complete
                    | BufferedResponse::CompleteWhileOutboundFull
                    | BufferedResponse::Backpressured
                    | BufferedResponse::AlreadyProcessing
            );
        assert_eq!(
            work.pending_contains(block::Height(1)),
            unanswered && !received && buffered != BufferedResponse::ResetDuringBackpressure
        );
        assert_eq!(work.in_flight_contains(block::Height(1)), received);
        assert_eq!(sequencer_recv.try_recv().is_ok(), received);
        assert!(sequencer_recv.try_recv().is_err());
    }

    #[tokio::test]
    async fn oversized_body_against_an_advertised_hint_is_still_sequenced() {
        use zakura_chain::serialization::ZcashDeserializeInto;

        use crate::zakura::transport::OrderedStreamFailureCause;
        use crate::zakura::ServicePeerDirection;

        let body: Arc<block::Block> = Arc::new(
            zakura_test::vectors::BLOCK_MAINNET_1_BYTES
                .zcash_deserialize_into()
                .unwrap(),
        );
        let config = ZakuraBlockSyncConfig::default();
        let budget = ByteBudget::new(1_000_000);
        let work = Arc::new(WorkQueue::new(block::Height(0), budget.clone()));
        work.set_estimate_floor_for_tests(1);
        // 500 B advertised at the default 200 % tolerance means anything over 1 000 B
        // mismatches; the block header alone is 1 487 B.
        work.extend(
            super::super::test_work_scope(),
            [(
                block::Height(1),
                body.hash(),
                BlockSizeEstimate::Advertised(500),
            )],
        );
        let peer = ZakuraPeerId::new(vec![23; 32]).unwrap();
        let registry = Arc::new(PeerRegistry::new());
        let now = Instant::now();
        let generation = registry
            .admit_session(&peer, ServicePeerDirection::Outbound, &config, 0, now)
            .generation();
        let cancel = CancellationToken::new();
        let (out_send, mut out_recv) = crate::zakura::transport::worker_framed_channel(4);
        let (in_send, in_recv) = framed_channel(4);
        let cause = OrderedStreamFailureCause::default();
        let session = BlockSyncPeerSession::for_test(peer.clone(), out_send, cancel.clone());
        let (sequencer_input, mut sequencer_recv) = mpsc::channel(4);
        let (reactor_input, mut reactor_recv) = mpsc::channel(4);
        let (_view_tx, view_rx) = watch::channel(initial_view(genesis_frontiers()));
        let in_recv = in_recv.with_failure_cause(cause);
        let (requester, serving) = test_authorization(
            &session,
            &in_recv,
            &config,
            test_request_pool(),
            &CancellationToken::new(),
        );
        let mut routine = PeerRoutine::new(
            peer.clone(),
            0,
            session,
            in_recv,
            requester,
            serving,
            config.clone(),
            true,
            generation,
            work.clone(),
            registry.clone(),
            Arc::new(Mutex::new(ThroughputMeter::new(now))),
            sequencer_input,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            reactor_input,
            view_rx,
            cancel.clone(),
            ZakuraTrace::noop(),
        );
        routine.handle_status(BlockSyncStatus {
            servable_low: block::Height(1),
            servable_high: block::Height(1),
            max_blocks_per_response: 1,
            ..BlockSyncStatus::default()
        });
        routine.try_fill();
        assert_eq!(routine.state.window.outstanding.len(), 1);
        timeout(Duration::from_secs(1), out_recv.recv())
            .await
            .unwrap()
            .unwrap()
            .write_with(|_| async { Ok::<_, std::convert::Infallible>(()) })
            .await
            .unwrap();
        in_send
            .send(
                BlockSyncMessage::Block(body.clone())
                    .encode_frame()
                    .unwrap(),
            )
            .await
            .unwrap();
        in_send
            .send(
                BlockSyncMessage::BlocksDone {
                    start_height: block::Height(1),
                    returned: 1,
                }
                .encode_frame()
                .unwrap(),
            )
            .await
            .unwrap();

        let mut running = Box::pin(routine.run());
        let sequenced = tokio::select! {
            sequenced = sequencer_recv.recv() => sequenced,
            _ = &mut running => panic!("the routine must stay alive until the body is sequenced"),
        };
        assert!(
            sequenced.is_some(),
            "a hash-matched body is sequenced even though it exceeds its size hint"
        );
        let reported = std::iter::from_fn(|| reactor_recv.try_recv().ok())
            .any(|message| matches!(message, RoutineToReactor::Misbehavior { .. }));
        assert!(
            !reported,
            "a wrong size hint does not implicate the supplier"
        );

        cancel.cancel();
        let result = timeout(Duration::from_secs(1), running).await.unwrap();
        assert!(result.is_ok(), "{result:?}");
    }

    /// Runs Mainnet block 1 through a routine's real frame path, while the
    /// sequencer view shows `applying_decoded_bytes` in `applying` and the
    /// input queue holds `queued_decoded_bytes`. Returns the routine's
    /// decoded block size, the body it sequenced, and the input queue's
    /// decoded gauge with that body queued.
    async fn hand_off_mainnet_block_1(
        applying_decoded_bytes: u64,
        queued_decoded_bytes: u64,
    ) -> (u64, SequencedBody, u64) {
        use zakura_chain::serialization::ZcashDeserializeInto;

        use crate::zakura::ServicePeerDirection;

        let body: Arc<block::Block> = Arc::new(
            zakura_test::vectors::BLOCK_MAINNET_1_BYTES
                .zcash_deserialize_into()
                .unwrap(),
        );
        let config = ZakuraBlockSyncConfig::default();
        let budget = ByteBudget::new(1_000_000);
        let work = Arc::new(WorkQueue::new(block::Height(0), budget.clone()));
        work.extend(
            super::super::test_work_scope(),
            [(
                block::Height(1),
                body.hash(),
                BlockSizeEstimate::Advertised(
                    u32::try_from(zakura_test::vectors::BLOCK_MAINNET_1_BYTES.len()).unwrap(),
                ),
            )],
        );
        let peer = ZakuraPeerId::new(vec![24; 32]).unwrap();
        let registry = Arc::new(PeerRegistry::new());
        let now = Instant::now();
        let generation = registry
            .admit_session(&peer, ServicePeerDirection::Outbound, &config, 0, now)
            .generation();
        let cancel = CancellationToken::new();
        let (out_send, mut out_recv) = crate::zakura::transport::worker_framed_channel(4);
        let (in_send, in_recv) = framed_channel(4);
        let session = BlockSyncPeerSession::for_test(peer.clone(), out_send, cancel.clone());
        let (sequencer_input, mut sequencer_recv) = mpsc::channel(4);
        let (reactor_input, _reactor_recv) = mpsc::channel(4);
        let mut view = initial_view(genesis_frontiers());
        view.applying_decoded_attributed_memory_bytes = applying_decoded_bytes;
        let (_view_tx, view_rx) = watch::channel(view);
        let input_decoded_bytes = Arc::new(AtomicU64::new(queued_decoded_bytes));
        let (requester, serving) = test_authorization(
            &session,
            &in_recv,
            &config,
            test_request_pool(),
            &CancellationToken::new(),
        );
        let mut routine = PeerRoutine::new(
            peer,
            0,
            session,
            in_recv,
            requester,
            serving,
            config.clone(),
            true,
            generation,
            work,
            registry,
            Arc::new(Mutex::new(ThroughputMeter::new(now))),
            sequencer_input,
            Arc::new(AtomicU64::new(0)),
            input_decoded_bytes.clone(),
            reactor_input,
            view_rx,
            cancel.clone(),
            ZakuraTrace::noop(),
        );
        routine.handle_status(BlockSyncStatus {
            servable_low: block::Height(1),
            servable_high: block::Height(1),
            max_blocks_per_response: 1,
            ..BlockSyncStatus::default()
        });
        routine.try_fill();
        timeout(Duration::from_secs(1), out_recv.recv())
            .await
            .unwrap()
            .unwrap()
            .write_with(|_| async { Ok::<_, std::convert::Infallible>(()) })
            .await
            .unwrap();
        in_send
            .send(
                BlockSyncMessage::Block(body.clone())
                    .encode_frame()
                    .unwrap(),
            )
            .await
            .unwrap();

        let mut running = Box::pin(routine.run());
        let sequenced = tokio::select! {
            sequenced = sequencer_recv.recv() => sequenced,
            _ = &mut running => panic!("the routine must stay alive until the body is sequenced"),
        }
        .expect("the routine sequences the requested body");
        let queued = input_decoded_bytes.load(std::sync::atomic::Ordering::Relaxed);
        cancel.cancel();
        let result = timeout(Duration::from_secs(1), running).await.unwrap();
        assert!(result.is_ok(), "{result:?}");
        (body.attributed_memory_size_bytes(), sequenced, queued)
    }

    /// With room in the decoded budget, the routine hands its decoded copy
    /// to the sequencer and charges it to the input queue.
    #[tokio::test]
    async fn a_handoff_with_room_keeps_the_decoded_copy() {
        let (size, sequenced, queued) = hand_off_mainnet_block_1(0, 0).await;
        assert!(sequenced.body.is_decoded());
        assert_eq!(sequenced.body.decoded_attributed_memory_size_bytes(), size);
        assert_eq!(queued, size);
    }

    /// The witness for #1522 under a slow verifier: once `applying`'s and the
    /// input queue's decoded bytes leave no room for the body under the
    /// decoded cap, the routine hands over raw bytes only. A body that fills
    /// the cap exactly still fits.
    #[tokio::test]
    async fn a_handoff_without_room_hands_over_raw_bytes() {
        let (size, _, _) = hand_off_mainnet_block_1(0, 0).await;
        let cap = super::super::config::BS_MAX_DECODED_SUBMISSION_WINDOW_BYTES;
        for (applying, queued, fits) in [
            (cap - size, 0, true),
            (cap - size, 1, false),
            (cap, 0, false),
            (0, cap, false),
        ] {
            let (_, sequenced, after) = hand_off_mainnet_block_1(applying, queued).await;
            assert_eq!(
                sequenced.body.is_decoded(),
                fits,
                "applying {applying}, queued {queued}"
            );
            assert_eq!(after, queued + if fits { size } else { 0 });
        }
    }

    /// At the tip the decoded budget has room, so the block the routine
    /// decoded reaches the verifier: the sequencer submits that same
    /// allocation and never decodes the body again.
    #[tokio::test]
    async fn a_tip_body_is_decoded_once() {
        use super::super::sequencer::Sequencer;
        use super::super::sequencer_task::SequencerTask;
        use super::super::BlockSyncAction;

        let (_, mut sequenced, _) = hand_off_mainnet_block_1(0, 0).await;
        assert!(sequenced.body.is_decoded());
        // The body holds the routine's decode, so this reads it back.
        let routine_decode = sequenced.body.decoded_block();

        let frontiers = genesis_frontiers();
        let (body_tx, body_rx) = mpsc::channel(1);
        let (_control_tx, control_rx) = mpsc::unbounded_channel();
        let (actions, mut actions_rx) = mpsc::channel(1);
        let (view_tx, _view_rx) = watch::channel(initial_view(frontiers));
        let budget = ByteBudget::new(1_000_000);
        let task = SequencerTask::new(
            Sequencer::new(block::Height(0), 1),
            Arc::new(WorkQueue::new(block::Height(0), budget)),
            Arc::new(PeerRegistry::new()),
            actions,
            ThroughputMeter::new(Instant::now()),
            frontiers,
            Some(super::super::test_work_scope()),
            crate::zakura::header_sync::SeededRetryJitter::new([0; 32]),
            Arc::new(crate::zakura::block_sync::BlockSyncClock),
            body_rx,
            control_rx,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            view_tx,
            Duration::from_secs(60),
            ZakuraTrace::noop(),
        );
        body_tx
            .try_send(sequenced)
            .unwrap_or_else(|_| panic!("the input has room"));

        let running = task.run();
        tokio::pin!(running);
        let action = tokio::select! {
            action = actions_rx.recv() => action.expect("the task submits the tip body"),
            () = &mut running => panic!("the task runs until the test drops it"),
        };
        let BlockSyncAction::SubmitBlock { block, .. } = action else {
            panic!("expected a body submission, got {action:?}");
        };
        assert!(Arc::ptr_eq(&block, &routine_decode));
    }

    #[test]
    fn repeated_no_progress_stall_disconnects_instead_of_parking_again() {
        assert_eq!(
            super::state::no_progress_response(true),
            super::NoProgressResponse::Park
        );
        assert_eq!(
            super::state::no_progress_response(false),
            super::NoProgressResponse::Disconnect
        );
    }

    #[tokio::test]
    async fn repeated_no_progress_is_a_local_failure() {
        let (mut routine, _outbound, _events) = status_test_routine();
        routine.state.allow_no_progress_park = false;
        assert!(matches!(
            routine.no_progress_stall(Instant::now(), "stalled"),
            Err(crate::zakura::SinkReject::Local(_))
        ));
    }
}
