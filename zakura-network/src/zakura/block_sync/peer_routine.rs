//! Per-peer pipe-routine for Zakura block sync.
//!
//! per-peer routines inverts the inbound data flow. One task per connected peer owns its
//! `FramedRecv` (the transport read), decodes each stream-6 frame, AND runs the
//! download logic as a direct continuation in the **same task** — there is no
//! reactor inbound demux and no per-peer `PeerInput` channel. Data flows
//! pipe-routine → reactor (over [`RoutineToReactor`]) for shared concerns only:
//! serving (`GetBlocks`), status advertisement, the producer re-query ping, and
//! serving-side misbehavior. The routine owns its `BlockSyncPeerSession` clone,
//! `outstanding`, the adaptive outbound window + timeout-recovery slots,
//! `received_status`/servable caps, and the want-work fill loop.
//!
//! The one throughput-critical effect: the matched-body `sequencer_input.send(..).await`
//! runs in this per-peer task, so a slow verifier (Sequencer backpressure)
//! stalls only one routine, not the whole fleet. The download decision gates only
//! on the byte budget + per-peer slots: `take_in_range(servable_low,
//! servable_high, n)` uses `servable_high` as the upper bound.
//!
//! All per-peer download state lives routine-local or in the shared
//! [`PeerRegistry`], and inbound traffic arrives as decoded frames from this
//! task's own `FramedRecv`: a want-work fill loop, the matched-body tail, and the
//! unmatched-body fallthroughs all run in this one task.

use std::collections::BTreeMap;

use tokio::sync::{futures::Notified, mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

use super::super::trace::{
    ordered_send_error_label, queue_send_trace as qs_trace, QUEUE_SEND_TABLE,
};
use super::events::RoutineToReactor;
use super::{
    admission::{
        admit, floor_rescue_high, request_deadline, request_priority as classify_priority,
        AdmissionOutcome, AdmissionSnapshot, RequestPriority,
    },
    outstanding::{
        OutstandingBlockRange, OutstandingRequestState, ReceivedBlockTracker, RetirementReason,
    },
    peer_registry::{hard_outbound_capacity, PeerRegistry},
    pipe::block_sync_guard,
    reactor::{
        block_sync_message_label, bs_insert_height, bs_insert_peer, bs_insert_str, bs_insert_u64,
        tolerated_bytes,
    },
    reorder::BufferedBlockBody,
    request::{BlockRangeRequest, ExpectedBlock},
    sequencer_task::{SequencedBody, SequencerControlInput, SequencerView},
    state::{DownloadWindow, LivenessOutcome, ThroughputMeter},
    work_queue::{LateBodyClaim, ReservationOwner, WorkItem, WorkQueue, WorkReturnOutcome},
    BlockSyncAction, BlockSyncMessage, BlockSyncMisbehavior, BlockSyncPeerSession, BlockSyncStatus,
    ZakuraBlockSyncConfig, ZakuraPeerId, ZakuraTrace, MSG_BS_BLOCK,
};
use crate::zakura::{
    trace::{block_sync_trace as bs_trace, BLOCK_SYNC_TABLE},
    Admit, FramedRecv, OrderedSendError, SinkReject,
};
use std::{sync::Arc, time::Duration, time::Instant};
use tokio::time;
use zakura_chain::{block, serialization::ZcashSerialize};

/// How long a routine avoids re-taking a height it just returned on a failure
/// (RangeUnavailable / timeout / send-failure / disconnect-retry) before it will
/// contest that height again. The window only has to be long enough that, on the
/// single-threaded test runtime, the other routines woken by the same failure
/// `return_items` get a chance to take the contested work first — a peer-local
/// bias away from work this routine just failed. It is negligible against real
/// sync timescales, and the height stays `pending` and fully contestable by every
/// other peer throughout.
const RETRY_AVOID_BACKOFF: Duration = Duration::from_millis(50);
/// Poll interval while this peer's outbound stream queue is full.
const OUTBOUND_FULL_POLL_INTERVAL: Duration = Duration::from_millis(10);
/// Cadence of the per-peer BBR heartbeat trace (`block_peer_bbr`). Observability only:
/// emits controller state on a fixed interval so a trace can spot oscillation even while
/// the peer is idle between deliveries.
const BBR_TRACE_INTERVAL: Duration = Duration::from_secs(10);

/// Why a fill pass stopped issuing requests. Typed so every admission refusal is
/// attributed exhaustively; the `as_str` labels feed the `sync.block.fill_stop`
/// metric and the fill-stop trace.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum FillStop {
    NoStatus,
    CwndSaturated,
    NoWork,
    /// The resident look-ahead gate refused an above-window take (either lane: the floor lane or the speculative lane / above floor lane).
    LookaheadCap,
    /// The gate has headroom but the in-flight byte budget funds zero bytes.
    /// This can happen when the in-flight byte budget is exhausted
    /// but the resident look-ahead gate is not full.
    /// This status is for the above floor speculative lane.
    InflightBudget,
    RetryAvoid,
    Budget,
    Internal,
    OutboundFull,
    SendError,
    /// The proven-peer no-progress request cap: this peer has served at least one
    /// body but reached `max_requests_without_block_progress` with no further
    /// accepted body, so the no-progress liveness deadline governs from here.
    NoBlockProgressRequestCap,
    /// The probe-first cap: an unproven peer's single cold-start probe is in flight,
    /// so no further request is issued until it serves (or fails) a body.
    InitialBlockProbeRequestCap,
}

impl FillStop {
    fn as_str(self) -> &'static str {
        match self {
            FillStop::NoStatus => "no_status",
            FillStop::CwndSaturated => "cwnd_saturated",
            FillStop::NoWork => "no_work",
            FillStop::LookaheadCap => "lookahead_cap",
            FillStop::InflightBudget => "inflight_budget",
            FillStop::RetryAvoid => "retry_avoid",
            FillStop::Budget => "budget",
            FillStop::Internal => "internal",
            FillStop::OutboundFull => "outbound_full",
            FillStop::SendError => "send_error",
            FillStop::NoBlockProgressRequestCap => "no_block_progress_request_cap",
            FillStop::InitialBlockProbeRequestCap => "initial_block_probe_request_cap",
        }
    }
}
const CLOSE_BLOCK_SYNC_NO_BLOCK_PROGRESS: &str = "block_sync_no_block_progress";

/// Whether a due block-liveness deadline gets one bounded grace instead of disconnecting.
/// Granted only for *our own* transient outbound write congestion: outbound full **and**
/// continuously full for less than `request_timeout`. A peer that stopped reading holds
/// our outbound full indefinitely, so once the full stretch reaches `request_timeout` the
/// grace is denied and the peer is disconnected — it can no longer dodge the timer by
/// refusing to read (the previous unbounded `outbound_capacity() == 0 → extend` escape let
/// a wedged peer survive to the ~180 s transport idle timeout).
fn liveness_grace_allowed(
    outbound_full: bool,
    outbound_full_since: Option<Instant>,
    now: Instant,
    request_timeout: Duration,
) -> bool {
    outbound_full
        && outbound_full_since
            .is_some_and(|since| now.saturating_duration_since(since) < request_timeout)
}

fn is_block_frame(frame: &crate::zakura::Frame) -> bool {
    frame.payload.first().copied() == Some(MSG_BS_BLOCK)
}

fn release_counter_bytes(counter: &std::sync::atomic::AtomicU64, bytes: u64) {
    let mut current = counter.load(std::sync::atomic::Ordering::Relaxed);
    loop {
        let next = current.saturating_sub(bytes);
        match counter.compare_exchange_weak(
            current,
            next,
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}

/// Outcome classification for finishing an outstanding request.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Disposition {
    Satisfied,
    RetryOriginal,
    RetryMissing,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum UnmatchedBodyOutcome {
    NotHandled,
    Handled,
    Accepted,
}

impl UnmatchedBodyOutcome {
    fn is_handled(self) -> bool {
        self != Self::NotHandled
    }
}

impl Disposition {
    fn trace_label(self) -> &'static str {
        match self {
            Self::Satisfied => "satisfied",
            Self::RetryOriginal => "retry_original",
            Self::RetryMissing => "retry_missing",
        }
    }
}

/// The per-peer pipe-routine. Owns its `FramedRecv` (transport read), the session
/// clone, the download window, the `outstanding` requests, the servable caps /
/// `received_status` it learns from `Status` frames, and holds clones of the
/// shared primitives. One task per connected peer; spawned at the pipe spawn point
/// (`service::add_peer`) so a protocol reject cancels the whole connection.
pub(super) struct PeerRoutine {
    peer: ZakuraPeerId,
    session: BlockSyncPeerSession,
    config: ZakuraBlockSyncConfig,

    // ---- transport inbound (the pipe half) ----
    /// This peer's ordered stream-6 frame reader. Decoded in the routine's own
    /// task; inbound never flows through the reactor (per-peer routines inverted data flow).
    recv: FramedRecv,

    // ---- per-peer download state (moved out of `PeerBlockState`) ----
    window: DownloadWindow,
    /// Whether this peer has sent a `Status` yet (gates want-work; mirrored into
    /// the registry for the reactor's serving/candidate reads).
    received_status: bool,
    /// This peer's advertised servable range, learned from its `Status`. The
    /// want-work upper bound; never the floor.
    servable_low: block::Height,
    servable_high: block::Height,
    /// This peer's clamped advertised serving caps, learned from its `Status`.
    /// Authoritative for the routine's own want-work decision (mirrored into the
    /// registry for the reactor's serving-side reads).
    max_blocks_per_response: u32,
    max_response_bytes: u32,
    /// Rate meter for sending our `Status` reply to this peer's inbound `Status`.
    /// The reply decision is routine-local; the actual send stays reactor-side via
    /// `RoutineToReactor::StatusReceived`.
    status_reply_meter: super::state::RateMeter,
    /// Rate meter gating how often this peer's `Status` frames are applied at all,
    /// so a status flood cannot spin the routine. A status that grows the servable
    /// range bypasses the meter.
    inbound_status_meter: super::state::RateMeter,
    /// Heights this routine recently returned on a failure, mapped to the instant
    /// after which it may re-take them. While avoided, the routine leaves the
    /// height `pending` (contestable by any other peer) but does not re-grab it
    /// itself — the peer-local retry bias (see [`RETRY_AVOID_BACKOFF`]). Pruned on
    /// expiry each fill pass.
    retry_avoid: BTreeMap<block::Height, Instant>,

    // ---- shared primitives (clones) ----
    /// Generation this routine was spawned with; gates its registry writes (and
    /// its `Drop`) so a superseded routine (e.g. a session replacement before the
    /// old task's async Drop runs) cannot corrupt the live entry.
    generation: u64,
    budget: super::state::ByteBudget,
    work: Arc<WorkQueue>,
    registry: Arc<PeerRegistry>,
    received_throughput: Arc<std::sync::Mutex<ThroughputMeter>>,
    sequencer_input: mpsc::Sender<SequencedBody>,
    sequencer_input_bytes: Arc<std::sync::atomic::AtomicU64>,
    sequencer_control: mpsc::UnboundedSender<SequencerControlInput>,
    actions: mpsc::Sender<BlockSyncAction>,
    /// Shared routine→reactor channel for serving / status-advertise / re-query /
    /// serving-misbehavior. `try_send` (bounded, never-wedging) so a busy reactor
    /// cannot backpressure this decode loop into stalling the transport.
    routine_to_reactor: mpsc::Sender<RoutineToReactor>,
    sequencer_view: watch::Receiver<SequencerView>,
    /// Last `reset_epoch` this routine reacted to, so a `view.changed()` can tell
    /// a destructive reset (in-place clear of outstanding) from a plain advance.
    last_reset_epoch: u64,
    /// When our outbound queue to this peer *first* filled in the current continuous full
    /// stretch (`None` while it has capacity). Lets the liveness check tell transient local
    /// write congestion (just filled) from a peer that stopped reading for `request_timeout`
    /// — the latter is disconnected at the liveness deadline rather than excused indefinitely.
    outbound_full_since: Option<Instant>,

    /// Cancellation: the peer's service session token. Fires on disconnect, park,
    /// or local shutdown; the routine exits and its `Drop` guard returns work.
    cancel: CancellationToken,
    trace: ZakuraTrace,
}

impl PeerRoutine {
    /// Build a pipe-routine for `peer`. The caller (`service::add_peer`) drives
    /// `run()` inside `spawn_supervised_pipe` so a protocol reject cancels the
    /// whole connection. `generation` is the value obtained from
    /// [`PeerRegistry::admit`](super::peer_registry::PeerRegistry::admit).
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        peer: ZakuraPeerId,
        session: BlockSyncPeerSession,
        recv: FramedRecv,
        config: ZakuraBlockSyncConfig,
        generation: u64,
        budget: super::state::ByteBudget,
        work: Arc<WorkQueue>,
        registry: Arc<PeerRegistry>,
        received_throughput: Arc<std::sync::Mutex<ThroughputMeter>>,
        sequencer_input: mpsc::Sender<SequencedBody>,
        sequencer_input_bytes: Arc<std::sync::atomic::AtomicU64>,
        sequencer_control: mpsc::UnboundedSender<SequencerControlInput>,
        actions: mpsc::Sender<BlockSyncAction>,
        routine_to_reactor: mpsc::Sender<RoutineToReactor>,
        sequencer_view: watch::Receiver<SequencerView>,
        cancel: CancellationToken,
        trace: ZakuraTrace,
    ) -> Self {
        let window = DownloadWindow::new(&config);
        let last_reset_epoch = sequencer_view.borrow().reset_epoch;
        let status_reply_meter = super::state::RateMeter::new(config.status_refresh_interval);
        let inbound_status_meter = super::state::RateMeter::new(
            config.status_refresh_interval.min(Duration::from_secs(1)),
        );
        let max_blocks_per_response = config.advertised_max_blocks_per_response();
        let max_response_bytes = config.advertised_max_response_bytes();
        PeerRoutine {
            peer,
            session,
            config,
            recv,
            window,
            received_status: false,
            servable_low: block::Height::MIN,
            servable_high: block::Height::MIN,
            max_blocks_per_response,
            max_response_bytes,
            status_reply_meter,
            inbound_status_meter,
            retry_avoid: BTreeMap::new(),
            generation,
            budget,
            work,
            registry,
            received_throughput,
            sequencer_input,
            sequencer_input_bytes,
            sequencer_control,
            actions,
            routine_to_reactor,
            sequencer_view,
            last_reset_epoch,
            outbound_full_since: None,
            cancel,
            trace,
        }
    }

    /// Run the pipe-routine until stream close, cancellation, or a protocol
    /// reject. A reject returns `Err(SinkReject::protocol(..))` so the supervised
    /// pipe tears the whole connection down.
    pub(super) async fn run(mut self) -> Result<(), SinkReject> {
        // Local clones so the `Notified` futures below borrow these handles, not
        // `self` — `self.try_fill()` needs `&mut self` while the notifications are
        // pinned. The clones share the same underlying `Arc`, so the wakes still
        // fire for releases/extends done through the routine's own `self.budget` /
        // `self.work`.
        let budget = self.budget.clone();
        let work = self.work.clone();
        // The per-connection oversize guard applied to inbound frames at ingress.
        let mut guard = block_sync_guard();
        // Per-peer BBR heartbeat cadence. `Skip` so a routine busy past a tick emits one
        // fresh sample rather than a catch-up burst. Observability only.
        let mut bbr_trace_ticks = time::interval(BBR_TRACE_INTERVAL);
        bbr_trace_ticks.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        loop {
            self.reconcile_registry_retirements(Instant::now());
            // missed-wake safety: register both `Notify`s via
            // `Notified::enable()` BEFORE the fill attempt. The budget/work
            // `Notify`s use `notify_waiters` (no stored permit), so a
            // release/extend that lands between the fill-check and the await
            // would be lost if we registered after — the routine would stall.
            let capacity = budget.subscribe_capacity().notified();
            let available = work.subscribe_available().notified();
            tokio::pin!(capacity);
            tokio::pin!(available);
            Notified::enable(capacity.as_mut());
            Notified::enable(available.as_mut());

            if self.session.outbound_capacity() > 0 {
                self.try_fill().await;
            }
            let outbound_queue_has_capacity = self.session.outbound_capacity() > 0;
            // Track the start of the current continuous outbound-full stretch so the
            // liveness check can bound the write-congestion grace: a peer that stopped
            // reading holds this full until `outbound_full_since` ages past
            // `request_timeout`, at which point it is disconnected rather than excused.
            if outbound_queue_has_capacity {
                self.outbound_full_since = None;
            } else if self.outbound_full_since.is_none() {
                self.outbound_full_since = Some(Instant::now());
            }

            // Sleep until the earliest outstanding deadline (own-timeout arm).
            let timeout = self.earliest_deadline_sleep();
            tokio::pin!(timeout);
            let outbound_queue_poll = time::sleep(OUTBOUND_FULL_POLL_INTERVAL);
            tokio::pin!(outbound_queue_poll);

            tokio::select! {
                biased;
                _ = self.cancel.cancelled() => return Ok(()),
                frame = self.recv.recv(), if outbound_queue_has_capacity => {
                    match frame {
                        // Decode the frame and run the download/serving dispatch
                        // in this same task. A protocol reject propagates out so
                        // the supervised pipe cancels the connection; the `Drop`
                        // guard returns unreceived work on the way out.
                        Some(frame) => self.handle_frame(&mut guard, frame).await?,
                        // Stream closed (peer gone): exit cleanly. `Drop` returns
                        // unreceived outstanding heights and releases their budget.
                        None => return Ok(()),
                    }
                }
                changed = self.sequencer_view.changed() => {
                    match changed {
                        Ok(()) => self.on_view_changed(),
                        // The Sequencer task ended (shutdown); the routine follows.
                        Err(_) => return Ok(()),
                    }
                }
                _ = &mut timeout => self.handle_deadlines(Instant::now()).await?,
                _ = &mut capacity => {
                    self.trace_wake("budget_capacity");
                }
                _ = &mut available => {
                    self.trace_wake("work_added");
                }
                _ = bbr_trace_ticks.tick() => self.trace_bbr_sample(),
                _ = &mut outbound_queue_poll, if !outbound_queue_has_capacity => {}
            }
        }
    }

    /// Admit, decode, and dispatch one inbound frame in this task. `Block` /
    /// `BlocksDone` / `RangeUnavailable` (download) are handled locally; `Status`
    /// updates own servable/caps locally and pings the reactor to advertise;
    /// `GetBlocks` (serving) forwards to the reactor; a decode error reports
    /// `MalformedMessage` and rejects the peer.
    async fn handle_frame(
        &mut self,
        guard: &mut crate::zakura::SessionGuard,
        frame: crate::zakura::Frame,
    ) -> Result<(), SinkReject> {
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

        let frame_payload_bytes = frame.payload.len();
        let body_permit = if is_block_frame(&frame) {
            Some(self.reserve_body_decode_permit().await?)
        } else {
            None
        };
        // Measured here, on the per-peer task, so the body size never has to be
        // recomputed by re-serializing the block on another thread (A1).
        let (msg, raw_block_payload) =
            match BlockSyncMessage::decode_frame_with_raw_block_payload(frame) {
                Ok(decoded) => decoded,
                Err(error) => {
                    // A malformed frame is `MalformedMessage` misbehavior AND a fatal
                    // protocol reject for the whole connection. Report via the shared
                    // channel, then reject; the report is best-effort and never blocks.
                    let protocol_error =
                        std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string());
                    tracing::debug!(peer = ?self.peer, ?error, "malformed Zakura block-sync frame");
                    let _ = self
                        .routine_to_reactor
                        .try_send(RoutineToReactor::Misbehavior {
                            peer: self.peer.clone(),
                            reason: BlockSyncMisbehavior::MalformedMessage,
                        });
                    return Err(SinkReject::protocol(protocol_error));
                }
            };
        let body_wire_bytes = msg.block_body_wire_bytes(frame_payload_bytes);
        self.trace_message_received(&msg);

        match msg {
            BlockSyncMessage::Status(status) => self.handle_status(status),
            BlockSyncMessage::GetBlocks {
                start_height,
                count,
            } => {
                // Serving is reactor-owned (state query + driver). Forward the
                // request; the reactor serves via the session clone it holds.
                let _ = self
                    .routine_to_reactor
                    .try_send(RoutineToReactor::ServeGetBlocks {
                        peer: self.peer.clone(),
                        start_height,
                        count,
                    });
            }
            BlockSyncMessage::Block(block) => {
                self.trace_wake("own_body");
                self.handle_body(block, body_wire_bytes, body_permit, raw_block_payload)
                    .await;
            }
            BlockSyncMessage::BlocksDone {
                start_height,
                returned: _,
            } => self.handle_blocks_done(start_height).await,
            BlockSyncMessage::RangeUnavailable {
                start_height,
                count: _,
            } => self.handle_range_unavailable(start_height).await,
        }
        Ok(())
    }

    async fn reserve_body_decode_permit(
        &self,
    ) -> Result<mpsc::OwnedPermit<SequencedBody>, SinkReject> {
        let capacity_before = self.sequencer_input.capacity();
        let started = Instant::now();
        let permit = self
            .sequencer_input
            .clone()
            .reserve_owned()
            .await
            .map_err(|_| SinkReject::local("block-sync sequencer body input closed"))?;
        self.trace_body_decode_permit(started.elapsed(), capacity_before);
        Ok(permit)
    }

    /// Apply this peer's `Status` locally (servable range, caps, `received_status`)
    /// and into the registry, then ping the reactor to advertise our reply and
    /// republish the candidate. Runs the validate / rate-meter / upsert; the
    /// servable read for want-work is this routine's own fields.
    fn handle_status(&mut self, status: BlockSyncStatus) {
        if status.servable_low > status.servable_high {
            let _ = self
                .routine_to_reactor
                .try_send(RoutineToReactor::Misbehavior {
                    peer: self.peer.clone(),
                    reason: BlockSyncMisbehavior::InvalidStatus,
                });
            return;
        }
        let now = Instant::now();
        // A status is applied if the rate meter allows it OR it grows our servable
        // range (so a peer that just extended its range is never throttled out).
        let grows =
            status.servable_high > self.servable_high || status.servable_low < self.servable_low;
        if !self.inbound_status_meter.try_take(now) && !grows {
            return;
        }
        // The reply is best-effort: if both the connect-time Status and this
        // first reply are dropped by a full outbound queue, recovery depends on
        // the remote's later Status retry arriving after this meter reopens.
        let send_reply = self.status_reply_meter.try_take(now);
        self.received_status = true;
        self.servable_low = status.servable_low;
        self.servable_high = status.servable_high;
        self.max_blocks_per_response =
            super::config::clamp_advertised_blocks(status.max_blocks_per_response);
        self.max_response_bytes =
            super::config::clamp_advertised_response_bytes(status.max_response_bytes);
        self.window.max_inflight_requests =
            super::config::clamp_advertised_inflight(status.max_inflight_requests);
        // Publish the servable range / clamped caps / received_status to the
        // registry so the reactor's serving/candidate reads and `GetBlocks`
        // admission see them; generation-gated.
        self.registry
            .upsert_status(&self.peer, self.generation, status);
        self.trace_status_received(status);
        // Ask the reactor to advertise our Status reply (if due) and republish the
        // candidate. Best-effort; a full channel just defers the candidate refresh
        // to the next reactor tick.
        let _ = self
            .routine_to_reactor
            .try_send(RoutineToReactor::StatusReceived {
                peer: self.peer.clone(),
                send_reply,
            });
    }

    /// React to a committed-view change: refresh the floor/tip the routine reads,
    /// and on a destructive `reset_epoch` bump retire this routine's active
    /// outstanding **in place** (return unreceived heights to `work.pending`,
    /// release their budget, clear the registry outstanding, drop retry-avoid)
    /// while preserving bounded response-correlation tombstones, then re-fan from
    /// the post-`reset_above` `WorkQueue`. The transport is never torn down.
    fn on_view_changed(&mut self) {
        let reset_epoch = self.sequencer_view.borrow().reset_epoch;
        if reset_epoch == self.last_reset_epoch {
            // A non-destructive advance: the floor/tip the routine reads come
            // straight from the live `view` each time they are needed, so nothing
            // to do but let the want-work loop re-run at the top (a committed
            // floor advance may GC our fully-committed outstanding).
            return;
        }
        self.last_reset_epoch = reset_epoch;
        self.trace_wake("view_reset");
        // The Sequencer already pinned its floor/tip and `work.reset_above`'d the
        // dropped successor heights. Return our unreceived outstanding to
        // `work.pending` (a no-op for heights already dropped from `in_flight` by
        // `reset_above`) and release their reservations exactly once. Preserve
        // existing retired records unchanged: a late terminator must still close
        // its old correlation window before this peer can safely receive a reissue.
        let now = Instant::now();
        let correlation_deadline = self.retirement_deadline(now);
        let active_indices: Vec<_> = self
            .window
            .outstanding
            .iter()
            .enumerate()
            .filter_map(|(index, outstanding)| outstanding.is_active().then_some(index))
            .collect();
        for index in active_indices {
            let outstanding = self
                .window
                .outstanding
                .get(index)
                .expect("active index exists because reset does not remove records")
                .clone();
            let unreceived: Vec<_> = unreceived_heights(&outstanding).collect();
            let outcome = self.work.release_reserved_and_return_items_detailed(
                unreceived.iter().copied(),
                reservation_owner(self.generation, &outstanding),
            );
            self.budget.release(outcome.released_bytes);
            self.trace_work_returned("view_reset", &outstanding, unreceived.len(), outcome);
            let retired = self.window.outstanding.retire(
                index,
                RetirementReason::ViewReset,
                now,
                correlation_deadline,
            );
            debug_assert!(retired, "collected active request must retire exactly once");
        }
        self.retry_avoid.clear();
        // Publish zero active ownership while keeping local retired tombstones.
        self.publish_outstanding();
        // A destructive reset pulled this peer's outstanding on our initiative, so its
        // no-progress probe streak must not stay charged: reset it and explicitly
        // disarm liveness (retained tombstones keep the collection non-empty) so an
        // unproven peer whose only probe was in flight at the
        // reset can probe again instead of wedging at its cap.
        self.window.note_view_reset();
        // Ping the producer immediately: `reset_above` emptied `pending`, and the
        // reactor's post-reset query may have run while our (now cleared) outstanding
        // still inflated the low-water gate. Without this ping a routine that then
        // sleeps on an empty deadline set would leave the pipeline dry.
        let _ = self
            .routine_to_reactor
            .try_send(RoutineToReactor::RequeryNeeded);
        // The want-work loop re-fans from the queue at the top of the next
        // iteration (the `reset_above` + producer re-query repopulate `pending`).
    }

    /// Sleep future resolving at the earliest wake the routine schedules for
    /// itself: the soonest outstanding request deadline (own-timeout), block
    /// liveness deadline, **or** the soonest retry-avoid expiry (local failure bias
    /// or registry-owned floor-watchdog hard exclude), so a routine that quiet-returned
    /// its only work re-runs want-work once the bias lifts even if no external event
    /// arrives. Defaults to a long idle sleep when none exists.
    fn earliest_deadline_sleep(&self) -> time::Sleep {
        let now = Instant::now();
        let earliest_deadline = self
            .window
            .outstanding
            .iter()
            .map(|outstanding| match outstanding.state {
                OutstandingRequestState::Active => outstanding.deadline,
                OutstandingRequestState::Retired {
                    correlation_deadline,
                    ..
                } => correlation_deadline,
            })
            .min();
        let liveness_deadline = self.window.block_liveness_deadline;
        let local_retry_avoid = self.retry_avoid.values().min().copied();
        let floor_watchdog_avoid = self.registry.next_floor_avoid_deadline(&self.peer, now);
        let earliest = [
            earliest_deadline,
            liveness_deadline,
            local_retry_avoid,
            floor_watchdog_avoid,
        ]
        .into_iter()
        .flatten()
        .min();
        match earliest {
            // Floor the wait at the deadline so a far-future request still wakes
            // promptly; an already-due deadline wakes immediately so `handle_deadlines`
            // can process and clear it (a due deadline it does not clear would
            // busy-spin the loop — see `handle_deadlines`).
            Some(deadline) => time::sleep(deadline.saturating_duration_since(now)),
            None => time::sleep(Duration::from_secs(3600)),
        }
    }

    // ===================== want-work fill loop (ports `fill_peer`) ===========

    /// Fill this peer's available slots in a single pass, letting the byte budget
    /// (re-checked each iteration via `try_reserve`) be the congestion window. The
    /// per-peer state is routine-local / in the registry.
    ///
    /// There is no floor gate: downloads are governed by the byte budget and
    /// per-peer slots, never floor-distance / near-tip lag.
    async fn try_fill(&mut self) {
        // The BBR cwnd is clamped to the peer's advertised hard cap inside
        // `available_slots`, so there is no separate window to reconcile on a
        // `Status` change.
        // GC this routine's own fully-committed outstanding requests: when the
        // committed floor passes the end of a request, its bodies are no longer
        // needed, so release its reservation and free its slot promptly rather
        // than waiting for the request's own timeout. This GCs *our own* covered
        // requests; it is never a fetch throttle and never churns other peers (a
        // partially-received request whose suffix is still above the floor is left
        // in place).
        self.gc_committed_outstanding();
        if self.window.prune_expired_retired(Instant::now()) {
            self.window.disarm_liveness_after_progress_if_idle();
        }
        // Drop expired retry-avoid entries: those heights are contestable by this
        // routine again.
        let now = Instant::now();
        self.retry_avoid.retain(|_, until| *until > now);
        // Count requests issued this pass and capture *why* the fill loop stops, so a
        // trace can attribute carrier idle ("bubble") time to a cause. The loop yields a
        // `&'static str` reason via `break`; a pass that issues nothing (`fill_sent == 0`)
        // is a candidate bubble.
        let mut fill_sent = 0u32;
        let fill_stop: FillStop = loop {
            // Floor bypass scaled by reliability: a healthy saturated carrier keeps the
            // full bypass so the floor keeps moving; a failing/sealed peer earns *no*
            // above-window slots even for a near-floor block.
            let base_floor_bonus = usize::try_from(self.config.floor_bypass_slots).unwrap_or(0);
            let floor_bonus = self.window.scaled_floor_bonus(base_floor_bonus);
            let normal_slots = self.window.available_slots_at(now);
            let floor_slots = self.window.available_slots_with_bonus_at(floor_bonus, now);
            // Break only when even a bypassed floor request has no slot. A cwnd that is
            // saturated for above-floor work (`normal_slots == 0`) still leaves up to
            // `floor_bonus` slots so the lowest missing height keeps moving — unless the
            // peer is sealed (`floor_bonus` is 0), which gets no work.
            if !self.received_status {
                break FillStop::NoStatus;
            }
            if self.window.requests_without_block_progress >= self.window.no_progress_request_cap()
            {
                break if self.window.has_block_progress() {
                    FillStop::NoBlockProgressRequestCap
                } else {
                    FillStop::InitialBlockProbeRequestCap
                };
            }
            if floor_slots == 0 {
                break FillStop::CwndSaturated;
            }
            let in_bypass = normal_slots == 0;
            let (servable_low, servable_high) = (self.servable_low, self.servable_high);

            // Compute this chunk's count and byte ceiling before taking any work.
            // The count cap is the peer/request cap; the byte cap is enforced by
            // the budgeted work-queue take and then by the reservation below.
            let max_count = self.request_count_cap();
            let response_byte_cap = u64::from(self.max_response_bytes.max(1));

            let view = *self.sequencer_view.borrow();
            let floor_high = floor_rescue_high(view.download_floor);
            // One snapshot per iteration: the floor and speculative lanes decide
            // against the same memory picture, and `admit` is the single authority
            // for the commit-window exemption, the resident gate, and take sizing
            // (geometry included — an exempt grant is clamped at the window top, so
            // no above-window height can ride an exempt request past the gate).
            let snapshot = self.admission_snapshot(&view);
            // This asks the shared peer registry:
            // "Is there another pper that should take the floor instead of this peer?"
            // This is helpful for rescuing the floor with a peer who has better latency score and
            // is not saturated.
            let floor_arm_allowed = !self.registry.floor_has_preferred_unsaturated_server(
                view.download_floor,
                &self.peer,
                self.window.bbr_rtprop_ms(now),
                in_bypass,
            );
            let mut items = Vec::new();
            let mut reservation_owner = None;
            if floor_arm_allowed && servable_low <= floor_high {
                if let Some(floor_start) = self
                    .work
                    .first_pending_in_range(servable_low, servable_high.min(floor_high))
                {
                    // Prioritize the lowest missing block so commit can keep moving, even if
                    // that means freeing look-ahead budget. `admit` is the single authority
                    // for the commit-window exemption, the resident-memory gate, and take
                    // geometry/sizing; layer the per-peer BBR byte window
                    // (`cwnd_byte_headroom`) on top so a saturated congestion window cannot
                    // fund a large speculative tail. The floor bypass adds `floor_bonus`
                    // bodies of headroom. `.max(1)` preserves the always-take-first-item
                    // floor-progress guarantee even at zero headroom (that single body is the
                    // only permitted overshoot; `reserve_request_budget`'s floor path sheds an
                    // above-floor reorder body to pay for it).
                    match admit(
                        &self.config,
                        snapshot,
                        floor_start,
                        servable_high,
                        response_byte_cap,
                    ) {
                        AdmissionOutcome::Admit(grant) => {
                            let floor_cwnd_cap = self
                                .window
                                .cwnd_byte_headroom_at(floor_bonus, now)
                                .unwrap_or(u64::MAX);
                            let owner = self.next_reservation_owner();
                            items = self.work.take_in_range_budgeted_owned(
                                servable_low,
                                grant.take_high,
                                max_count,
                                grant.max_request_bytes.min(floor_cwnd_cap).max(1),
                                owner,
                            );
                            reservation_owner = (!items.is_empty()).then_some(owner);
                        }
                        AdmissionOutcome::LookaheadAtCap => break FillStop::LookaheadCap,
                        // Unreachable for floor-priority starts (their cap is floored
                        // at one byte); attribute honestly if it ever fires.
                        AdmissionOutcome::InflightBudgetEmpty => break FillStop::InflightBudget,
                    }
                }
            }

            if items.is_empty() {
                if in_bypass {
                    // Saturated cwnd: the floor bypass funds the floor only, never a
                    // speculative above-floor fetch. Nothing more to take this pass.
                    break FillStop::CwndSaturated;
                }
                let Some(start_height) = self
                    .work
                    .first_pending_in_range(servable_low, servable_high)
                else {
                    break FillStop::NoWork;
                };
                match admit(
                    &self.config,
                    snapshot,
                    start_height,
                    servable_high,
                    response_byte_cap,
                ) {
                    AdmissionOutcome::Admit(grant)
                        if grant.priority == RequestPriority::AboveFloor =>
                    {
                        metrics::gauge!("sync.block.backlog.at_cap").set(0.0);
                        // Bound the take by remaining cwnd byte headroom (byte mode, no floor
                        // bonus) so an above-floor request never overshoots the byte window
                        // beyond the one always-taken item.
                        let above_cwnd_cap = self
                            .window
                            .cwnd_byte_headroom_at(0, now)
                            .unwrap_or(u64::MAX);
                        let owner = self.next_reservation_owner();
                        items = self.work.take_in_range_budgeted_owned(
                            servable_low,
                            grant.take_high,
                            max_count,
                            grant.max_request_bytes.min(above_cwnd_cap),
                            owner,
                        );
                        reservation_owner = (!items.is_empty()).then_some(owner);
                    }
                    // A floor-priority start while the floor arm deferred to a
                    // preferred carrier: leave the take to that peer (falls through
                    // to `no_work`, exactly as before).
                    AdmissionOutcome::Admit(_) => {}
                    AdmissionOutcome::LookaheadAtCap => {
                        metrics::gauge!("sync.block.backlog.at_cap").set(1.0);
                        break FillStop::LookaheadCap;
                    }
                    AdmissionOutcome::InflightBudgetEmpty => break FillStop::InflightBudget,
                }
            }
            if items.is_empty() {
                break FillStop::NoWork;
            }
            let reservation_owner =
                reservation_owner.expect("a non-empty production take has an assigned owner");
            // Peer-local retry bias: if the contiguous chunk we just took leads
            // with heights this routine recently *failed* (RangeUnavailable /
            // timeout / send-failure), quietly put those back so another peer can
            // contest them first, and only keep the suffix this routine is allowed
            // to re-take. `return_items_quiet` does NOT notify (the other peers were
            // already woken by the original failure return), so this cannot
            // self-wake into a take/return spin. If the whole chunk is still
            // avoided, break — the routine wakes to retry when the avoid window
            // expires (see `earliest_deadline_sleep`).
            {
                let keep_from = items.iter().position(|(height, _)| {
                    !self.retry_avoid.contains_key(height)
                        && !self
                            .registry
                            .is_floor_height_avoided(&self.peer, *height, now)
                });
                match keep_from {
                    Some(0) => {}
                    Some(index) => {
                        let avoided: Vec<_> = items.drain(..index).map(|(h, _)| h).collect();
                        self.work.return_items_quiet(avoided, reservation_owner);
                    }
                    None => {
                        let avoided: Vec<_> = items.iter().map(|(h, _)| *h).collect();
                        self.work.return_items_quiet(avoided, reservation_owner);
                        break FillStop::RetryAvoid;
                    }
                }
            }
            if items
                .iter()
                .any(|(height, _)| self.window.has_outstanding_height(*height))
            {
                self.return_taken_items(&items, reservation_owner);
                break FillStop::RetryAvoid;
            }
            self.trace_work_taken(servable_low, servable_high, items.len());

            // Reserve the summed per-block size estimate for this request (not
            // worst case), so the budget admits far more typically-small bodies.
            // `take_in_range_budgeted` already bounded the summed estimate to the
            // response-byte cap.
            let kept_count = items.len();

            // Mislabel guard: another routine may have taken the intended (floor) start
            // between our `first_pending_in_range` probe and the take, so the contiguous
            // chunk we actually kept can begin above the floor-rescue window. Label the
            // request by its *actual* lowest height, so a purely speculative take is never
            // funded as a floor reservation or given the short floor-rescue leash.
            let request_priority = classify_priority(view.download_floor, items[0].0);

            let reserved_bytes = items.iter().fold(0u64, |acc, (_, item)| {
                acc.saturating_add(item.estimated_bytes)
            });
            if !self
                .reserve_request_budget(request_priority, reserved_bytes)
                .await
            {
                self.return_taken_items(&items, reservation_owner);
                break FillStop::Budget;
            }
            let marked = self
                .work
                .mark_reserved(items.iter().map(|(height, _)| *height), reservation_owner);
            if marked != reserved_bytes {
                // Release both parts of the failed two-phase reservation: bytes
                // admitted globally but never attached to this owner, plus this
                // owner's still-reserved queue entries. Held/replacement entries
                // remain untouched by the owner-aware queue cleanup.
                release_failed_request_reservation(
                    &self.work,
                    &mut self.budget,
                    items.iter().map(|(height, _)| *height),
                    reservation_owner,
                    reserved_bytes,
                    marked,
                );
                break FillStop::Internal;
            }

            let count = match u32::try_from(kept_count) {
                Ok(count) => count,
                Err(_) => {
                    let released = self.work.release_reserved_and_return_items(
                        items.iter().map(|(height, _)| *height),
                        reservation_owner,
                    );
                    self.budget.release(released);
                    break FillStop::Internal;
                }
            };
            let request = BlockRangeRequest {
                start_height: items[0].0,
                count,
                anchor_hash: items[0].1.hash,
                // The summed size-estimate reservation for this request (released
                // on a send failure below); equals the sum of the per-height
                // `expected_blocks` estimates.
                estimated_bytes: reserved_bytes,
                expected_blocks: items
                    .iter()
                    .map(|(height, item)| ExpectedBlock {
                        height: *height,
                        hash: item.hash,
                        estimated_bytes: item.estimated_bytes,
                    })
                    .collect(),
            };

            let queued_at = Instant::now();
            let request_token = reservation_owner.request_token;
            let msg = BlockSyncMessage::GetBlocks {
                start_height: request.start_height,
                count: request.count,
            };
            if let Err(error) = self
                .session
                .try_send_get_blocks(request.start_height, request.count)
            {
                tracing::debug!(
                    peer = ?self.peer,
                    start_height = ?request.start_height,
                    count = request.count,
                    ?error,
                    "failed to queue Zakura block-sync GetBlocks"
                );
                self.trace_queue_send_failed(&msg, &error);
                // Nothing was received, so return every taken height to the queue.
                // Held-aware: a competing peer's late body may have converted a taken
                // height during the reserve await; that body is owned by the Sequencer,
                // so skip it here rather than re-queue and double-release it.
                let released = self.work.release_reserved_and_return_items(
                    items.iter().map(|(height, _)| *height),
                    reservation_owner,
                );
                self.budget.release(released);
                if matches!(error, OrderedSendError::Full) {
                    break FillStop::OutboundFull;
                }
                self.session.cancel_token().cancel();
                break FillStop::SendError;
            }

            let deadline = request_deadline(
                request_priority,
                queued_at,
                self.config.request_timeout,
                self.config.effective_floor_rescue_timeout(),
                reserved_bytes,
                // Filter BtlBw by the request's send time so a stale-high rate from a
                // now-slow peer cannot tighten the deadline below what it can meet.
                self.window.bbr_btlbw_bytes_per_sec(queued_at),
            );
            metrics::counter!("sync.block.request.sent").increment(1);
            if in_bypass {
                // A floor request borrowed a bypass slot while the cwnd was saturated.
                metrics::counter!("sync.block.request.floor_bypass").increment(1);
            }
            let request_start_height = request.start_height;
            let request_count = request.count;
            let request_estimated_bytes = request.estimated_bytes;
            self.window.outstanding.insert(OutstandingBlockRange {
                token: request_token,
                state: OutstandingRequestState::Active,
                request,
                queued_at,
                deadline,
                delivery_snapshot: self.window.delivery_snapshot(queued_at),
                delivered_bytes: 0,
                received: ReceivedBlockTracker::default(),
                late_reliability_credited: false,
            });
            self.window
                .arm_liveness(queued_at, self.config.effective_liveness_timeout());
            self.publish_outstanding();
            self.trace_get_blocks_sent(
                request_start_height,
                request_count,
                request_estimated_bytes,
                in_bypass,
            );
            fill_sent = fill_sent.saturating_add(1);
        };
        // Attribute this pass's stop. A pass that issued nothing is a candidate bubble;
        // the reason + the live slot/budget/work snapshot let a trace tell a legitimate
        // stop (no_work with empty queue, cwnd_saturated) from a recoverable one (slots +
        // budget + work all free, stopped anyway). The at-cap gauge is latched here so
        // every gate refusal — floor arm, speculative arm, in bypass or not — sets it.
        if fill_stop == FillStop::LookaheadCap {
            metrics::gauge!("sync.block.backlog.at_cap").set(1.0);
        }
        metrics::counter!("sync.block.fill_stop", "reason" => fill_stop.as_str()).increment(1);
        if fill_sent == 0 {
            self.trace_fill_stop(fill_stop.as_str());
        }

        // If pending work is running low, ping the reactor to re-query (the
        // producer self-gates on low-water, so this is idempotent/cheap).
        if self.work.pending_len() < self.refill_low_water_blocks() {
            let _ = self
                .routine_to_reactor
                .try_send(RoutineToReactor::RequeryNeeded);
        }
    }

    fn admission_snapshot(&self, view: &SequencerView) -> AdmissionSnapshot {
        let (reserved_above_floor_bytes, reserved_above_floor_blocks) =
            self.work.reserved_above(view.download_floor);
        AdmissionSnapshot {
            download_floor: view.download_floor,
            verified_block_tip: view.verified_tip,
            reorder_buffered_bytes: view.reorder_buffered_bytes,
            reorder_buffered_blocks: view.reorder_len,
            applying_buffered_bytes: view.applying_buffered_bytes,
            applying_buffered_blocks: view.applying_len,
            sequencer_input_queued_bytes: self
                .sequencer_input_bytes
                .load(std::sync::atomic::Ordering::Relaxed),
            reserved_above_floor_bytes,
            reserved_above_floor_blocks,
            budget_available: self.budget.available(),
        }
    }

    fn request_count_cap(&self) -> usize {
        usize::try_from(
            self.max_blocks_per_response
                .min(self.config.advertised_max_blocks_per_response())
                .max(1),
        )
        .unwrap_or(usize::MAX)
    }

    async fn reserve_request_budget(
        &mut self,
        priority: RequestPriority,
        reserved_bytes: u64,
    ) -> bool {
        if priority == RequestPriority::AboveFloor {
            return self.budget.try_reserve(reserved_bytes);
        }

        loop {
            if self.budget.try_reserve(reserved_bytes) {
                return true;
            }

            let (reply, funded) = oneshot::channel();
            if self
                .sequencer_control
                .send(SequencerControlInput::FundFloorReservation {
                    needed_bytes: reserved_bytes,
                    reply,
                })
                .is_err()
            {
                return false;
            }

            match funded.await {
                Ok(true) => continue,
                Ok(false) | Err(_) => return false,
            }
        }
    }

    /// Refill low-water mark in blocks, computed from a single peer's caps.
    fn refill_low_water_blocks(&self) -> usize {
        let max_blocks_per_response =
            usize::try_from(self.config.advertised_max_blocks_per_response()).unwrap_or(usize::MAX);
        let max_inflight_per_peer = hard_outbound_capacity(self.window.max_inflight_requests);
        max_inflight_per_peer
            .saturating_mul(max_blocks_per_response)
            .max(max_blocks_per_response)
    }

    /// Put back a chunk this routine took but is not issuing this fill pass
    /// (budget race / send failure). Quiet (no notify): the returning routine must
    /// not re-wake its own want-work arm into a take/return spin, and any other
    /// peer waiting on budget capacity is woken by the matching `budget.release`.
    fn return_taken_items(&self, items: &[(block::Height, WorkItem)], owner: ReservationOwner) {
        self.work
            .return_items_quiet(items.iter().map(|(height, _)| *height), owner);
    }

    fn next_reservation_owner(&mut self) -> ReservationOwner {
        ReservationOwner {
            generation: self.generation,
            request_token: self.window.next_request_token(),
        }
    }

    /// Record heights this routine just returned on a failure so it will not
    /// immediately re-grab them (the peer-local retry bias). The heights stay
    /// `pending` and contestable by every other peer; only this routine defers.
    fn note_retry_avoid(&mut self, heights: impl IntoIterator<Item = block::Height>) {
        let until = Instant::now() + RETRY_AVOID_BACKOFF;
        for height in heights {
            self.retry_avoid.insert(height, until);
        }
    }

    fn retirement_deadline(&self, now: Instant) -> Instant {
        // Quarantine a retired wire-v2 range for one additional request timeout.
        // This bounds ambiguous late terminators without pinning a useful proven
        // peer until its longer no-progress liveness deadline.
        self.window
            .block_liveness_deadline
            .unwrap_or(now + self.config.request_timeout)
            .min(now + self.config.request_timeout)
    }

    /// Reconcile floor-watchdog retirements from the shared registry with this
    /// routine's active requests, deferring their unreceived heights from immediate
    /// local retry while retaining retired requests for late-response correlation.
    fn reconcile_registry_retirements(&mut self, now: Instant) {
        let tokens = self
            .registry
            .take_retired_request_tokens(&self.peer, self.generation);
        if tokens.is_empty() {
            return;
        }

        let correlation_deadline = self.retirement_deadline(now);
        let mut avoided = Vec::new();
        let mut newly_retired = 0usize;
        for token in tokens {
            let Some(index) = self.window.outstanding.active_index_for_token(token) else {
                continue;
            };
            if let Some(outstanding) = self.window.outstanding.get(index) {
                avoided.extend(unreceived_heights(outstanding));
            }
            if self.window.outstanding.retire(
                index,
                RetirementReason::FloorWatchdog,
                now,
                correlation_deadline,
            ) {
                newly_retired = newly_retired.saturating_add(1);
            }
        }
        if newly_retired > 0 {
            self.window.record_timeout(newly_retired);
        }
        self.note_retry_avoid(avoided);
        self.publish_outstanding();
    }

    // ===================== own-timeout arm (ports `expire_due_timeouts`) =====

    async fn handle_deadlines(&mut self, now: Instant) -> Result<(), SinkReject> {
        // A watchdog retirement may have landed since the last reconcile; fold it in
        // before expiring timeouts so a request the reactor already retired (and
        // returned to the queue) is not re-processed here as a local timeout, which
        // would release/return a reservation another peer may now own.
        self.reconcile_registry_retirements(now);
        // Prune expired deadlines here because a full outbound queue skips
        // `try_fill` (the other prune site). `earliest_deadline_sleep` includes
        // both retired-tombstone correlation deadlines and retry-avoid deadlines
        // unfiltered, so a due one left in place resolves the timeout arm at zero
        // delay every iteration and busy-spins the loop until outbound drains.
        if self.window.prune_expired_retired(now) {
            self.window.disarm_liveness_after_progress_if_idle();
        }
        self.retry_avoid.retain(|_, until| *until > now);
        let rescued_timed_out = self.expire_due_timeouts(now);
        if rescued_timed_out && self.session.outbound_capacity() > 0 {
            self.try_fill().await;
        }
        self.check_block_liveness(now)
    }

    fn expire_due_timeouts(&mut self, now: Instant) -> bool {
        let timed_out_indices: Vec<_> = self
            .window
            .outstanding
            .iter()
            .enumerate()
            .filter_map(|(index, outstanding)| {
                (outstanding.is_active() && outstanding.deadline <= now).then_some(index)
            })
            .collect();
        if timed_out_indices.is_empty() {
            return false;
        }
        self.window.record_timeout(timed_out_indices.len());
        let correlation_deadline = self.retirement_deadline(now);
        let mut timed_out_heights = Vec::new();
        for index in timed_out_indices {
            let outstanding = self.window.outstanding[index].clone();
            // Return only the unreceived heights — received ones are buffered (in
            // `in_flight` until committed); re-queuing them would re-fetch a body
            // we already hold (the WorkQueue single-owner invariant forbids it).
            let unreceived: Vec<_> = unreceived_heights(&outstanding).collect();
            let outcome = self.work.release_reserved_and_return_items_detailed(
                unreceived.iter().copied(),
                reservation_owner(self.generation, &outstanding),
            );
            self.budget.release(outcome.released_bytes);
            self.trace_work_returned("request_timeout", &outstanding, unreceived.len(), outcome);
            timed_out_heights.extend(unreceived);
            self.window.outstanding.retire(
                index,
                RetirementReason::RequestTimeout,
                now,
                correlation_deadline,
            );
        }
        // Bias away from immediately re-grabbing the heights this peer just timed
        // out, so another peer can contest them (the peer-local timeout bias).
        self.note_retry_avoid(timed_out_heights);
        self.publish_outstanding();
        true
    }

    fn check_block_liveness(&mut self, now: Instant) -> Result<(), SinkReject> {
        match self.window.check_liveness(now) {
            LivenessOutcome::Ok => Ok(()),
            LivenessOutcome::Disarm => {
                self.window.clear_liveness_if_idle();
                Ok(())
            }
            LivenessOutcome::Disconnect
                if liveness_grace_allowed(
                    self.session.outbound_capacity() == 0,
                    self.outbound_full_since,
                    now,
                    self.config.request_timeout,
                ) =>
            {
                // Outbound full but *only just* filled (< one `request_timeout` of
                // continuous backpressure): plausibly transient local write congestion, not
                // a dead peer. While outbound is full the select loop does not drain inbound
                // frames (`if outbound_queue_has_capacity`), so a block the peer already sent
                // may be waiting behind our write side. Grant one bounded grace period.
                // A peer that stops reading remains full and is then disconnected.
                self.window
                    .extend_liveness_deadline(now, self.config.request_timeout);
                Ok(())
            }
            LivenessOutcome::Disconnect => {
                let error =
                    "block-sync peer made no accepted block progress before liveness deadline";
                self.registry.park_peer_until(
                    &self.peer,
                    now + self.config.effective_no_progress_peer_cooldown(),
                );
                self.trace_protocol_reject_liveness(error);
                tracing::info!(
                    peer = ?self.peer,
                    outstanding = self.window.active_len(),
                    retired = self.window.retired_len(),
                    "disconnecting Zakura block-sync peer after no accepted block progress"
                );
                Err(SinkReject::protocol(error))
            }
        }
    }

    /// Retire active requests whose whole range is at or below the download
    /// floor. They no longer own scheduling resources, but their correlation
    /// record and peer-specific liveness deadline remain until a response or
    /// bounded retirement expiry.
    fn gc_committed_outstanding(&mut self) {
        let floor = self.download_floor();
        let mut released = 0u64;
        let now = Instant::now();
        let correlation_deadline = self.retirement_deadline(now);
        let work = Arc::clone(&self.work);
        let generation = self.generation;
        let retired = self.window.outstanding.retire_covered(
            floor,
            now,
            correlation_deadline,
            |outstanding| {
                // Release only the size-estimate still reserved for unreceived
                // heights. A height a competing peer delivered late is `Held`: its
                // body is in the commit pipeline and the Sequencer releases those
                // bytes on commit, so it must not be released a second time here.
                released = released.saturating_add(work.release_reserved_heights(
                    unreceived_heights(outstanding),
                    reservation_owner(generation, outstanding),
                ));
            },
        );
        if released > 0 {
            self.budget.release(released);
        }
        if retired > 0 {
            self.publish_outstanding();
        }
    }

    // ===================== inbound matched body (ports `handle_block`) ======

    async fn handle_body(
        &mut self,
        block: Arc<block::Block>,
        body_wire_bytes: Option<u64>,
        body_permit: Option<mpsc::OwnedPermit<SequencedBody>>,
        raw_block_payload: Option<Arc<[u8]>>,
    ) {
        self.reconcile_registry_retirements(Instant::now());
        let hash = block.hash();
        let Some(height) = block.coinbase_height() else {
            self.report_misbehavior(BlockSyncMisbehavior::InvalidBlock)
                .await;
            return;
        };

        let Some(index) = self.window.outstanding_index_for_height(height) else {
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
                .is_handled()
            {
                return;
            }
            if self.ignore_stale_response(height, "body").await {
                return;
            }
            if self.ignore_unmatched_needed_response(height, "body") {
                return;
            }
            if self.ignore_unmatched_active_body_response(height, hash) {
                return;
            }
            if self.ignore_servable_range_response(height, "body") {
                return;
            }
            self.report_misbehavior(BlockSyncMisbehavior::UnsolicitedBlock)
                .await;
            return;
        };
        if self.window.outstanding[index].is_retired() {
            self.handle_retired_body(
                index,
                height,
                hash,
                block,
                body_wire_bytes,
                body_permit,
                raw_block_payload,
            )
            .await;
            return;
        }
        let outstanding = &self.window.outstanding[index];
        let delivery_snapshot = outstanding.delivery_snapshot;
        if outstanding.has_received(height) {
            tracing::debug!(peer = ?self.peer, ?height, "ignoring duplicate block-sync body frame");
            return;
        }
        if outstanding.request.expected_hash(height) != Some(hash) {
            self.report_misbehavior(BlockSyncMisbehavior::InvalidBlock)
                .await;
            return;
        }
        let estimated_bytes = outstanding.estimated_bytes_for_height(height).unwrap_or(0);
        let request_start_height = outstanding.request.start_height;
        let request_range_count = outstanding.request.count;
        let request_elapsed = outstanding.queued_at.elapsed();
        let request_elapsed_ms = elapsed_ms_u64(request_elapsed);

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
                    self.finish_outstanding_at(index, Disposition::RetryOriginal);
                    self.report_misbehavior(BlockSyncMisbehavior::InvalidBlock)
                        .await;
                    return;
                }
            },
        };
        if serialized_bytes > tolerated_bytes(estimated_bytes, self.config.size_deviation_tolerance)
        {
            self.report_misbehavior(BlockSyncMisbehavior::SizeMismatch)
                .await;
            self.finish_outstanding_at(index, Disposition::RetryOriginal);
            return;
        }

        metrics::counter!("sync.block.body.received").increment(1);
        self.record_received(serialized_bytes);
        // The block reserved its size estimate at send time; settle to the actual
        // size. When the body is no larger than its estimate this frees the
        // slack; when it is larger (a stale/under-advertised hint) this charges
        // the overshoot so held bodies are never under-counted.
        // `mark_received` then stops `reserved_bytes()` counting this height; the
        // only bytes still held are the `serialized_bytes` carried into the reorder
        // buffer.
        let previous_block_hash = block.header.previous_block_hash;
        let Some(claim) =
            self.claim_received_body(height, hash, previous_block_hash, serialized_bytes)
        else {
            self.reconcile_registry_retirements(Instant::now());
            return;
        };
        let reset_epoch = match claim {
            LateBodyClaim::SettledReserved { delta, reset_epoch } => {
                self.apply_budget_delta(delta);
                reset_epoch
            }
            LateBodyClaim::ClaimedPending(reset_epoch) => {
                metrics::counter!("sync.block.response.watchdog_late_accepted").increment(1);
                reset_epoch
            }
            LateBodyClaim::AlreadyHeld => {
                self.reconcile_registry_retirements(Instant::now());
                tracing::debug!(
                    peer = ?self.peer,
                    ?height,
                    serialized_bytes,
                    "block-sync body already settled by another peer; marking received"
                );
                self.accept_already_settled_height(index, height);
                return;
            }
            LateBodyClaim::BudgetFull => {
                self.reconcile_registry_retirements(Instant::now());
                tracing::debug!(
                    peer = ?self.peer,
                    ?height,
                    serialized_bytes,
                    "not buffering late block-sync body; height stays queued for retry"
                );
                return;
            }
            LateBodyClaim::Missing | LateBodyClaim::HashMismatch => {
                self.reconcile_registry_retirements(Instant::now());
                tracing::debug!(
                    peer = ?self.peer,
                    ?height,
                    "not buffering block-sync body no longer owned by the work queue"
                );
                return;
            }
            LateBodyClaim::PendingAdmissionRequired => {
                unreachable!("claim_received_body resolves pending admission")
            }
        };

        // The watchdog publishes its retirement before returning WorkQueue
        // ownership. Reconcile again after the authoritative claim so a watchdog
        // interleaving during body processing still records the local request's
        // correlation-tombstone state and timeout accountability.
        self.reconcile_registry_retirements(Instant::now());
        self.trace_body_received(
            height,
            serialized_bytes,
            Some(request_start_height),
            Some(request_range_count),
            Some(request_elapsed_ms),
        );

        self.window
            .note_block_progress(Instant::now(), self.config.effective_liveness_timeout());
        self.window
            .outstanding
            .record_body_bytes(index, serialized_bytes);
        self.window.outstanding.mark_received(index, height);
        let retired = self.window.outstanding[index].is_retired();
        if retired && self.window.outstanding.take_late_reliability_credit(index) {
            self.window.credit_late_delivery();
        }
        let completed = if self.window.outstanding.is_complete(index) {
            Some(self.window.outstanding.remove(index))
        } else {
            None
        };
        if !retired {
            if let Some(outstanding) = &completed {
                // Feed the BBR estimators on request completion: the round-trip (RTprop)
                // and the per-ack delivery rate (BtlBw) for this request's block count and
                // delivered bytes.
                self.window.record_delivery(
                    Instant::now(),
                    request_elapsed,
                    request_range_count,
                    outstanding.delivered_bytes,
                    delivery_snapshot,
                );
            }
        }
        if let Some(outstanding) = completed {
            self.finish_detached(outstanding, Disposition::Satisfied);
        } else {
            self.publish_outstanding();
        }

        // Forward the body to the commit-pipeline task. THE ONLY blocking send in
        // the routine: a slow verifier blocks the task draining input, the bounded
        // input channel fills, and this routine blocks here — backpressure
        // isolated to this peer (the per-peer routines throughput win).
        let body = BufferedBlockBody::from_decoded_block(block, raw_block_payload);
        self.forward_body_to_sequencer(
            height,
            hash,
            body,
            serialized_bytes,
            reset_epoch,
            body_permit,
        )
        .await;
        // This body opened only this peer's slots; the want-work loop runs at the
        // top of the next iteration.
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_retired_body(
        &mut self,
        index: usize,
        height: block::Height,
        hash: block::Hash,
        block: Arc<block::Block>,
        body_wire_bytes: Option<u64>,
        body_permit: Option<mpsc::OwnedPermit<SequencedBody>>,
        raw_block_payload: Option<Arc<[u8]>>,
    ) {
        let Some(outstanding) = self.window.outstanding.get(index) else {
            return;
        };
        if outstanding.has_received(height) {
            return;
        }
        if outstanding.request.expected_hash(height) != Some(hash) {
            self.report_misbehavior(BlockSyncMisbehavior::InvalidBlock)
                .await;
            return;
        }
        let outcome = self
            .accept_unmatched_queued_body(
                height,
                hash,
                block,
                body_wire_bytes,
                body_permit,
                raw_block_payload,
            )
            .await;
        if outcome == UnmatchedBodyOutcome::Accepted {
            if self.window.outstanding.take_late_reliability_credit(index) {
                self.window.credit_late_delivery();
            }
            self.window.outstanding.mark_received(index, height);
            if self.window.outstanding.is_complete(index) {
                self.window.outstanding.remove(index);
                self.window.disarm_liveness_after_progress_if_idle();
            }
        }
        self.publish_outstanding();
    }

    // ===================== unmatched fallthroughs (ported) ==================

    /// Whether a response for `height` is stale (already downloaded or held). The
    /// held-height portion is recovered through the WorkQueue's `in_flight`
    /// (every buffered/applying height stays claimed until the download floor
    /// passes it). Reads `download_floor` from the view.
    fn is_stale_response_height(&self, height: block::Height) -> bool {
        height <= self.download_floor() || self.work.in_flight_contains(height)
    }

    async fn ignore_stale_response(&mut self, height: block::Height, response_kind: &str) -> bool {
        if !self.is_stale_response_height(height) {
            return false;
        }
        tracing::debug!(peer = ?self.peer, ?height, response_kind, "ignoring stale block-sync response");
        true
    }

    async fn forward_body_to_sequencer(
        &self,
        height: block::Height,
        hash: block::Hash,
        body: BufferedBlockBody,
        serialized_bytes: u64,
        reset_epoch: u64,
        body_permit: Option<mpsc::OwnedPermit<SequencedBody>>,
    ) {
        let received_at = Instant::now();
        let sequencer_send_started = Instant::now();
        let body = SequencedBody {
            height,
            hash,
            body,
            bytes: serialized_bytes,
            peer: self.peer.clone(),
            received_at,
            reset_epoch,
        };

        let ok = if let Some(permit) = body_permit {
            self.sequencer_input_bytes
                .fetch_add(serialized_bytes, std::sync::atomic::Ordering::Relaxed);
            permit.send(body);
            true
        } else {
            self.sequencer_input_bytes
                .fetch_add(serialized_bytes, std::sync::atomic::Ordering::Relaxed);
            let send_result = self.sequencer_input.send(body).await;
            if send_result.is_err() {
                release_counter_bytes(&self.sequencer_input_bytes, serialized_bytes);
            }
            send_result.is_ok()
        };

        self.trace_body_sequencer_sent(height, sequencer_send_started.elapsed(), ok);
    }

    fn admit_late_pending_body(&self, height: block::Height, serialized_bytes: u64) -> bool {
        let sequencer_view = *self.sequencer_view.borrow();
        let snapshot = self.admission_snapshot(&sequencer_view);
        let admitted_bytes = match admit(&self.config, snapshot, height, height, serialized_bytes) {
            AdmissionOutcome::Admit(grant) => grant.max_request_bytes,
            AdmissionOutcome::LookaheadAtCap | AdmissionOutcome::InflightBudgetEmpty => {
                tracing::debug!(
                    peer = ?self.peer,
                    ?height,
                    serialized_bytes,
                    "not buffering unmatched queued block-sync body at look-ahead cap"
                );
                return false;
            }
        };
        if admitted_bytes < serialized_bytes {
            tracing::debug!(
                peer = ?self.peer,
                ?height,
                serialized_bytes,
                admitted_bytes,
                "not buffering unmatched queued block-sync body; insufficient admitted budget"
            );
            return false;
        }
        true
    }

    /// Atomically settle the current reservation or claim a height that the
    /// watchdog returned to `pending` while this body was being processed.
    fn claim_received_body(
        &mut self,
        height: block::Height,
        hash: block::Hash,
        previous_block_hash: block::Hash,
        serialized_bytes: u64,
    ) -> Option<LateBodyClaim> {
        let mut pending_admitted = false;
        loop {
            match self.work.claim_late_body(
                height,
                hash,
                previous_block_hash,
                serialized_bytes,
                &mut self.budget,
                pending_admitted,
            ) {
                LateBodyClaim::PendingAdmissionRequired => {
                    if !self.admit_late_pending_body(height, serialized_bytes) {
                        return None;
                    }
                    pending_admitted = true;
                }
                outcome => return Some(outcome),
            }
        }
    }

    /// Accept a wanted unmatched body whose original requester is gone or whose height
    /// is currently reserved by another peer. Queued heights reserve their actual size
    /// before buffering; reserved in-flight heights settle the existing reservation to
    /// the actual held bytes.
    async fn accept_unmatched_queued_body(
        &mut self,
        height: block::Height,
        hash: block::Hash,
        block: Arc<block::Block>,
        body_wire_bytes: Option<u64>,
        body_permit: Option<mpsc::OwnedPermit<SequencedBody>>,
        raw_block_payload: Option<Arc<[u8]>>,
    ) -> UnmatchedBodyOutcome {
        if self.work.hash_for_height(height) != Some(hash) {
            return UnmatchedBodyOutcome::NotHandled;
        }
        if !self.received_status || height < self.servable_low || height > self.servable_high {
            return UnmatchedBodyOutcome::NotHandled;
        }

        let serialized_bytes = match body_wire_bytes {
            Some(bytes) => bytes,
            None => match block.zcash_serialize_to_vec() {
                Ok(bytes) => u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                Err(error) => {
                    tracing::debug!(
                        peer = ?self.peer,
                        ?height,
                        ?error,
                        "failed to serialize unmatched queued block-sync body"
                    );
                    self.report_misbehavior(BlockSyncMisbehavior::InvalidBlock)
                        .await;
                    return UnmatchedBodyOutcome::Handled;
                }
            },
        };

        let previous_block_hash = block.header.previous_block_hash;
        let reset_epoch = match self.claim_received_body(
            height,
            hash,
            previous_block_hash,
            serialized_bytes,
        ) {
            Some(LateBodyClaim::ClaimedPending(reset_epoch)) => {
                metrics::counter!("sync.block.response.unmatched_queued_accepted").increment(1);
                reset_epoch
            }
            Some(LateBodyClaim::SettledReserved { delta, reset_epoch }) => {
                self.apply_budget_delta(delta);
                metrics::counter!("sync.block.response.unmatched_active_accepted").increment(1);
                reset_epoch
            }
            Some(LateBodyClaim::BudgetFull) => {
                tracing::debug!(
                    peer = ?self.peer,
                    ?height,
                    serialized_bytes,
                    "not buffering unmatched queued block-sync body; height stays queued for retry"
                );
                return UnmatchedBodyOutcome::Handled;
            }
            Some(LateBodyClaim::AlreadyHeld) => return UnmatchedBodyOutcome::Handled,
            Some(LateBodyClaim::Missing | LateBodyClaim::HashMismatch) => {
                return UnmatchedBodyOutcome::NotHandled;
            }
            Some(LateBodyClaim::PendingAdmissionRequired) => {
                unreachable!("claim_received_body resolves pending admission")
            }
            None => return UnmatchedBodyOutcome::Handled,
        };

        self.record_received(serialized_bytes);
        self.trace_body_received(height, serialized_bytes, None, None, None);

        // A real, wanted body that no longer matches an outstanding request (typically
        // arrived just after its request timed out). Count it as block progress: resets
        // the no-progress streak and proves the peer, so a slow-but-useful peer is not
        // parked as "silent". Deliberately do NOT feed the BBR RTprop/BtlBw estimators —
        // the originating request is gone, so there's no trustworthy send timestamp and a
        // stale late-delivery interval would corrupt the rate/latency samples.
        self.window
            .note_block_progress(Instant::now(), self.config.effective_liveness_timeout());
        let body = BufferedBlockBody::from_decoded_block(block, raw_block_payload);
        self.forward_body_to_sequencer(
            height,
            hash,
            body,
            serialized_bytes,
            reset_epoch,
            body_permit,
        )
        .await;
        UnmatchedBodyOutcome::Accepted
    }

    fn ignore_unmatched_needed_response(&self, height: block::Height, response_kind: &str) -> bool {
        // The reactor-local `needed_heights` is gone from the routine; the
        // structural equivalent is "the height is still wanted" = pending or
        // in-flight in the WorkQueue.
        if !(self.work.pending_contains(height) || self.work.in_flight_contains(height)) {
            return false;
        }
        metrics::counter!("sync.block.response.unmatched_needed_ignored").increment(1);
        tracing::debug!(
            peer = ?self.peer,
            ?height,
            response_kind,
            "ignoring unmatched block-sync response for currently needed height"
        );
        true
    }

    fn ignore_unmatched_active_body_response(
        &self,
        height: block::Height,
        hash: block::Hash,
    ) -> bool {
        if !self.registry.has_outstanding_request(height, hash) {
            return false;
        }
        metrics::counter!("sync.block.response.unmatched_active_ignored").increment(1);
        tracing::debug!(
            peer = ?self.peer,
            ?height,
            "ignoring unmatched block-sync body for height active on another request"
        );
        true
    }

    fn ignore_unmatched_active_terminator_response(&self, start_height: block::Height) -> bool {
        // We reach this only when *this* peer has no outstanding request starting
        // at `start_height`; the registry answers whether another peer is actively
        // requesting a range covering it (cross-peer fanout/retry race), in which
        // case the terminator is dropped quietly rather than scored.
        if !self.registry.has_outstanding_height(start_height) {
            return false;
        }
        metrics::counter!("sync.block.response.unmatched_active_done_ignored").increment(1);
        tracing::debug!(
            peer = ?self.peer,
            ?start_height,
            "ignoring unmatched block-sync terminator for range active on another request"
        );
        true
    }

    /// An unmatched response for a height the peer *claims to serve*
    /// (`download_floor < height <= servable_high`) that no other fallthrough
    /// claimed. The common cause is an honest, in-flight body/terminator for a
    /// height we requested before a destructive reset (reorg) then dropped from
    /// our `outstanding` and from `work` (`reset_above`), or one that simply
    /// raced ahead of the producer's asynchronous `work.extend`. The peer asked
    /// for and served this range honestly, so scoring it the *hard*
    /// `UnsolicitedBlock`/`UnsolicitedDone` (immediate, thresholdless disconnect)
    /// would churn honest peers on every reorg. The reset that drops outstanding
    /// runs on the Sequencer task asynchronously, so an honest in-flight response
    /// can arrive after its `outstanding` entry is gone — drop it quietly to keep
    /// the no-churn property. A response *outside* the peer's advertised range is
    /// still scored.
    fn ignore_servable_range_response(&self, height: block::Height, response_kind: &str) -> bool {
        if !self.received_status || height <= self.download_floor() || height > self.servable_high {
            return false;
        }
        metrics::counter!("sync.block.response.unmatched_servable_ignored").increment(1);
        tracing::debug!(
            peer = ?self.peer,
            ?height,
            response_kind,
            "ignoring unmatched block-sync response within the peer's servable range"
        );
        true
    }

    // ===================== terminators (ports `handle_blocks_done` etc.) =====

    async fn handle_blocks_done(&mut self, start_height: block::Height) {
        self.reconcile_registry_retirements(Instant::now());
        if let Some(index) = self.window.retired_index_for_start(start_height) {
            self.window.outstanding.remove(index);
            self.publish_outstanding();
            self.window.disarm_liveness_after_progress_if_idle();
            return;
        }
        let Some(index) = self.window.outstanding_index_for_start(start_height) else {
            if self.ignore_stale_response(start_height, "terminator").await {
                return;
            }
            if self.ignore_unmatched_needed_response(start_height, "terminator") {
                return;
            }
            if self.ignore_unmatched_active_terminator_response(start_height) {
                return;
            }
            if self.ignore_servable_range_response(start_height, "terminator") {
                return;
            }
            // A known, active peer sent a terminator correlating to no outstanding
            // range, outside the range it claims to serve. Fail closed:
            // `UnsolicitedDone` (a hard misbehavior).
            self.report_misbehavior(BlockSyncMisbehavior::UnsolicitedDone)
                .await;
            return;
        };
        let disposition = self.stale_adjusted_disposition(index, Disposition::RetryMissing);
        self.charge_short_response_reliability(index, disposition);
        self.finish_outstanding_at(index, disposition);
    }

    async fn handle_range_unavailable(&mut self, start_height: block::Height) {
        self.reconcile_registry_retirements(Instant::now());
        if let Some(index) = self.window.retired_index_for_start(start_height) {
            self.window.outstanding.remove(index);
            self.publish_outstanding();
            self.window.disarm_liveness_after_progress_if_idle();
            return;
        }
        let Some(index) = self.window.outstanding_index_for_start(start_height) else {
            if self
                .ignore_stale_response(start_height, "unavailable range")
                .await
            {
                return;
            }
            self.trace_range_unavailable(start_height, None, None);
            return;
        };
        let outstanding = &self.window.outstanding[index];
        self.trace_range_unavailable(
            start_height,
            Some(outstanding.request.count),
            Some(elapsed_ms_u64(outstanding.queued_at.elapsed())),
        );
        let disposition = self.stale_adjusted_disposition(index, Disposition::RetryOriginal);
        self.charge_short_response_reliability(index, disposition);
        self.finish_outstanding_at(index, disposition);
    }

    /// Fold a short response into the reliability EWMA: a `BlocksDone`/`RangeUnavailable`
    /// that leaves the outstanding request at `index` with any unreceived height is one
    /// goodput failure for the request, like a timeout — per request, not per missing height
    /// (see `penalize_short_response`). A `Satisfied` disposition means the shortfall was
    /// covered by the floor advancing (not the peer's fault), so it is not charged. Reads the
    /// outstanding *before* `finish_outstanding_at` removes it.
    fn charge_short_response_reliability(&mut self, index: usize, disposition: Disposition) {
        if disposition == Disposition::Satisfied {
            return;
        }
        let missing = self
            .window
            .outstanding
            .get(index)
            .map(|outstanding| unreceived_heights(outstanding).count())
            .unwrap_or(0);
        self.window.penalize_short_response(missing);
    }

    /// A late response can still match after the floor moved through its prefix;
    /// mark the stale prefix satisfied and retry only the remaining suffix.
    fn stale_adjusted_disposition(&mut self, index: usize, current: Disposition) -> Disposition {
        let tip = self.download_floor();
        let Some(outstanding) = self.window.outstanding.get(index) else {
            return current;
        };
        if outstanding.request.start_height > tip {
            return current;
        }
        let owner = reservation_owner(self.generation, outstanding);
        let released_heights: Vec<_> = outstanding_unreceived_through(outstanding, tip).collect();
        self.window.outstanding.mark_received_through(index, tip);
        // Held-aware: release only the still-reserved estimate for the committed
        // prefix; a height a competing peer delivered late is owned by the
        // Sequencer, so it is left in place instead of double-released.
        let released_bytes = self.work.release_reserved_heights(released_heights, owner);
        self.budget.release(released_bytes);
        if self.window.outstanding.is_complete(index) {
            Disposition::Satisfied
        } else {
            Disposition::RetryMissing
        }
    }

    // ===================== outstanding lifecycle (ported) ===================

    fn finish_outstanding_at(&mut self, index: usize, disposition: Disposition) {
        if index >= self.window.outstanding.len() {
            return;
        }
        let outstanding = self.window.outstanding.remove(index);
        self.finish_detached(outstanding, disposition);
    }

    fn finish_detached(&mut self, outstanding: OutstandingBlockRange, disposition: Disposition) {
        if outstanding.is_retired() {
            self.publish_outstanding();
            self.window.disarm_liveness_after_progress_if_idle();
            return;
        }
        // Every release path below is Held-aware: a height a competing peer
        // delivered late settled to `Held(actual)` in the shared work queue and is
        // owned by the Sequencer (which releases those bytes on commit), so it must
        // never be released or re-queued from this stale claim. Only still-reserved
        // (unreceived, never-delivered) heights are released here.
        match disposition {
            Disposition::Satisfied => {
                // Every requested height was received and buffered; nothing
                // returns to the queue (buffered heights stay in `in_flight`
                // until the floor commits past them). Release any residual
                // reserved estimate (normally none once complete).
                let released = self.work.release_reserved_heights(
                    unreceived_heights(&outstanding),
                    reservation_owner(self.generation, &outstanding),
                );
                self.budget.release(released);
            }
            // With fanout = 1 a received height is already buffered and must never
            // be re-fetched, so both retry dispositions return only the still-reserved
            // unreceived heights to `pending`. `return_items` is idempotent.
            Disposition::RetryOriginal | Disposition::RetryMissing => {
                let unreceived: Vec<_> = unreceived_heights(&outstanding).collect();
                let outcome = self.work.release_reserved_and_return_items_detailed(
                    unreceived.iter().copied(),
                    reservation_owner(self.generation, &outstanding),
                );
                self.budget.release(outcome.released_bytes);
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
        self.publish_outstanding();
        self.window.disarm_liveness_after_progress_if_idle();
    }

    /// A body arrived for a request this peer owns, but another body already won
    /// the WorkQueue claim and entered the commit pipeline. Record the height as
    /// received without touching the winner's budget charge. Count it as block
    /// progress since this peer also delivered the expected body.
    fn accept_already_settled_height(&mut self, index: usize, height: block::Height) {
        self.window
            .note_block_progress(Instant::now(), self.config.effective_liveness_timeout());
        self.window.outstanding.mark_received(index, height);
        let completed = self.window.outstanding.is_complete(index);
        if completed {
            self.finish_outstanding_at(index, Disposition::Satisfied);
        } else {
            self.publish_outstanding();
        }
    }

    fn apply_budget_delta(&mut self, delta: i128) {
        if delta > 0 {
            self.budget
                .charge(u64::try_from(delta).expect("positive budget delta fits in u64"));
        } else if delta < 0 {
            self.budget
                .release(u64::try_from(-delta).expect("negative budget delta fits in u64"));
        }
    }

    /// Publish this peer's current *unreceived* in-flight height metadata to the
    /// registry, so the producer's `!has_outstanding_request` filter and the
    /// low-water `total_unreceived` gate read the same per-request-granularity
    /// count (`expected_blocks.len() − received.len()`).
    /// Received-but-uncommitted heights are excluded here because they are held in
    /// `work.in_flight` instead — the producer's `!in_flight_contains` clause
    /// already keeps them out of `pending`.
    fn publish_outstanding(&self) {
        let mut map: BTreeMap<block::Height, super::peer_registry::OutstandingMeta> =
            BTreeMap::new();
        for outstanding in self.window.active_iter() {
            for expected in &outstanding.request.expected_blocks {
                if !outstanding.has_received(expected.height) {
                    map.insert(
                        expected.height,
                        super::peer_registry::OutstandingMeta {
                            request_token: outstanding.token,
                            hash: expected.hash,
                            estimated_bytes: expected.estimated_bytes,
                            queued_at: outstanding.queued_at,
                            deadline: outstanding.deadline,
                        },
                    );
                }
            }
        }
        if map.is_empty() {
            self.registry.clear_outstanding(&self.peer, self.generation);
        } else {
            self.registry
                .set_outstanding(&self.peer, self.generation, map);
        }
        // Publish the window diagnostics for the reactor's periodic trace row and
        // for other routines' cross-peer floor-bias decisions.
        let hard_capacity = hard_outbound_capacity(self.window.max_inflight_requests);
        self.registry.publish_slots(
            &self.peer,
            self.generation,
            super::peer_registry::SlotDiagnostics {
                hard_capacity,
                effective_window: self.window.bbr_effective_cwnd().min(hard_capacity),
                available_slots: self.window.available_slots(),
                outstanding_requests: self.window.active_len(),
                // Filter the published RTprop by now so a peer that stopped completing
                // requests stops advertising a stale-low RTprop to the cross-peer
                // floor-preference comparison.
                bbr_rtprop_ms: self.window.bbr_rtprop_ms(Instant::now()),
            },
        );
    }

    // ===================== misbehavior (shared count via registry) ==========

    async fn report_misbehavior(&self, reason: BlockSyncMisbehavior) {
        // Misbehavior is record-only: observe and forward it, but never cancel the
        // session. Peer scoring no longer drives disconnects.
        metrics::counter!("sync.block.peer.violation").increment(1);
        // `Misbehavior` is best-effort: never block the routine.
        let _ = self.actions.try_send(BlockSyncAction::Misbehavior {
            peer: self.peer.clone(),
            reason,
        });
    }

    // ===================== view reads ======================================

    fn download_floor(&self) -> block::Height {
        self.sequencer_view.borrow().download_floor
    }

    fn record_received(&self, bytes: u64) {
        if let Ok(mut meter) = self.received_throughput.lock() {
            meter.record(bytes);
        }
    }

    // ===================== tracing =========================================

    fn emit(
        &self,
        event: &'static str,
        build: impl FnOnce(&mut serde_json::Map<String, serde_json::Value>),
    ) {
        if !self.trace.is_enabled() {
            return;
        }
        self.trace.emit_with(BLOCK_SYNC_TABLE, |row| {
            row.insert(
                bs_trace::EVENT.to_string(),
                serde_json::Value::String(event.to_string()),
            );
            build(row);
        });
    }

    fn trace_wake(&self, reason: &'static str) {
        self.emit("block_peer_wake", |row| {
            bs_insert_u64(
                row,
                "outstanding",
                u64::try_from(self.window.active_len()).unwrap_or(u64::MAX),
            );
            bs_insert_u64(
                row,
                "retired",
                u64::try_from(self.window.retired_len()).unwrap_or(u64::MAX),
            );
            row.insert(
                "reason".to_string(),
                serde_json::Value::String(reason.to_string()),
            );
        });
    }

    fn trace_protocol_reject_liveness(&self, error: &str) {
        self.emit(bs_trace::BLOCK_PEER_PROTOCOL_REJECT, |row| {
            bs_insert_peer(row, bs_trace::PEER, &self.peer);
            row.insert(
                bs_trace::REASON.to_string(),
                serde_json::Value::String(CLOSE_BLOCK_SYNC_NO_BLOCK_PROGRESS.to_string()),
            );
            row.insert(
                bs_trace::ERROR.to_string(),
                serde_json::Value::String(error.to_string()),
            );
            bs_insert_u64(
                row,
                bs_trace::OUTSTANDING,
                u64::try_from(self.window.active_len()).unwrap_or(u64::MAX),
            );
            bs_insert_u64(
                row,
                "bbr_cwnd",
                u64::try_from(self.window.bbr_effective_cwnd()).unwrap_or(u64::MAX),
            );
            bs_insert_u64(
                row,
                "available_slots",
                u64::try_from(self.window.available_slots()).unwrap_or(u64::MAX),
            );
            if let Some(last_block_at) = self.window.last_block_at {
                bs_insert_u64(
                    row,
                    "last_block_age_ms",
                    elapsed_ms_u64(last_block_at.elapsed()),
                );
            }
        });
    }

    /// Trace a decoded inbound message (the previous reactor's `trace_message_received`,
    /// now emitted in the routine that decoded it). Records the message kind only;
    /// the per-variant field detail lives on the reactor's heavier trace path.
    fn trace_message_received(&self, msg: &BlockSyncMessage) {
        self.emit(bs_trace::BLOCK_MESSAGE_RECEIVED, |row| {
            row.insert(
                bs_trace::KIND.to_string(),
                serde_json::Value::String(block_sync_message_label(msg).to_string()),
            );
        });
    }

    fn trace_status_received(&self, status: BlockSyncStatus) {
        self.emit(bs_trace::BLOCK_STATUS_RECEIVED, |row| {
            bs_insert_peer(row, bs_trace::PEER, &self.peer);
            bs_insert_height(row, "servable_low", status.servable_low);
            bs_insert_height(row, "servable_high", status.servable_high);
        });
    }

    fn trace_work_taken(&self, low: block::Height, high: block::Height, count: usize) {
        self.emit(bs_trace::BLOCK_WORK_TAKEN, |row| {
            bs_insert_height(row, "servable_low", low);
            bs_insert_height(row, "servable_high", high);
            bs_insert_u64(row, bs_trace::RANGE_COUNT, count as u64);
        });
    }

    fn trace_work_returned(
        &self,
        reason: &'static str,
        outstanding: &OutstandingBlockRange,
        unreceived_count: usize,
        outcome: WorkReturnOutcome,
    ) {
        let unreceived_count = u64::try_from(unreceived_count).unwrap_or(u64::MAX);
        if outcome.missing_count == 0
            && outcome.held_count == 0
            && outcome.released_count == 0
            && outcome.owner_mismatch_count == 0
            && outcome.returned_count == unreceived_count
        {
            return;
        }

        self.emit(bs_trace::BLOCK_WORK_RETURNED, |row| {
            bs_insert_peer(row, bs_trace::PEER, &self.peer);
            bs_insert_str(row, bs_trace::REASON, reason);
            bs_insert_height(row, bs_trace::RANGE_START, outstanding.request.start_height);
            bs_insert_u64(
                row,
                bs_trace::RANGE_COUNT,
                u64::from(outstanding.request.count),
            );
            bs_insert_u64(row, "unreceived_count", unreceived_count);
            insert_work_return_outcome(row, outcome);
        });
    }

    /// Emitted when a `try_fill` pass issued no request (a candidate carrier "bubble").
    /// The reason plus the live slot/budget/work snapshot let a trace tell a legitimate
    /// idle (`no_work` with an empty queue, `cwnd_saturated`) from a recoverable one
    /// (slots + budget + work all free yet stopped — a wakeup gap to fix).
    fn trace_fill_stop(&self, reason: &'static str) {
        // Mirror the effective (reliability-scaled) bypass the fill loop used, so the
        // snapshot reflects a sealed peer's collapsed floor bonus.
        let base_floor_bonus = usize::try_from(self.config.floor_bypass_slots).unwrap_or(0);
        let floor_bonus = self.window.scaled_floor_bonus(base_floor_bonus);
        let now = Instant::now();
        self.emit(bs_trace::BLOCK_FILL_STOP, |row| {
            bs_insert_peer(row, bs_trace::PEER, &self.peer);
            bs_insert_str(row, bs_trace::FILL_STOP_REASON, reason);
            bs_insert_u64(row, bs_trace::FILL_SENT, 0);
            bs_insert_u64(
                row,
                "normal_slots",
                self.window.available_slots_at(now) as u64,
            );
            bs_insert_u64(
                row,
                "floor_slots",
                self.window.available_slots_with_bonus_at(floor_bonus, now) as u64,
            );
            bs_insert_u64(row, "budget_available", self.budget.available());
            bs_insert_u64(row, "pending_work", self.work.pending_len() as u64);
            bs_insert_u64(row, "received_status", u64::from(self.received_status));
        });
    }

    fn trace_queue_send_failed(&self, msg: &BlockSyncMessage, error: &OrderedSendError) {
        self.trace.emit_with(QUEUE_SEND_TABLE, |row| {
            bs_insert_str(row, qs_trace::EVENT, qs_trace::QUEUE_SEND_FAILED);
            bs_insert_str(row, qs_trace::SERVICE, "block_sync");
            bs_insert_str(row, qs_trace::MESSAGE, block_sync_message_label(msg));
            bs_insert_peer(row, qs_trace::PEER, &self.peer);
            bs_insert_str(row, qs_trace::ERROR, ordered_send_error_label(error));
            bs_insert_u64(
                row,
                qs_trace::QUEUE_CAPACITY,
                u64::try_from(self.session.outbound_capacity()).unwrap_or(u64::MAX),
            );
            bs_insert_u64(
                row,
                qs_trace::QUEUE_MAX_CAPACITY,
                u64::try_from(self.session.outbound_max_capacity()).unwrap_or(u64::MAX),
            );
            if let BlockSyncMessage::GetBlocks {
                start_height,
                count,
            } = msg
            {
                bs_insert_height(row, qs_trace::RANGE_START, *start_height);
                bs_insert_u64(row, qs_trace::RANGE_COUNT, u64::from(*count));
            }
        });
    }

    fn trace_get_blocks_sent(
        &self,
        start_height: block::Height,
        count: u32,
        estimated_bytes: u64,
        floor_bypass: bool,
    ) {
        self.emit(bs_trace::BLOCK_GET_BLOCKS_SENT, |row| {
            bs_insert_peer(row, bs_trace::PEER, &self.peer);
            bs_insert_height(row, bs_trace::RANGE_START, start_height);
            bs_insert_u64(row, bs_trace::RANGE_COUNT, u64::from(count));
            bs_insert_u64(row, bs_trace::ESTIMATED_BYTES, estimated_bytes);
            bs_insert_u64(row, "available_slots", self.window.available_slots() as u64);
            bs_insert_u64(
                row,
                "peer_outstanding",
                u64::try_from(self.window.active_len()).unwrap_or(u64::MAX),
            );
            bs_insert_u64(
                row,
                "peer_retired",
                u64::try_from(self.window.retired_len()).unwrap_or(u64::MAX),
            );
            self.insert_no_progress_fields(row);
            // The reliability estimate discounts the admission cwnd, so trace it at
            // request time too (not only on delivery): a dropping peer keeps requesting at
            // a shrinking cwnd, and these rows capture the fall.
            bs_insert_u64(
                row,
                "bbr_reliability_permille",
                self.window.bbr_reliability_permille(),
            );
            // A floor request issued while the peer was saturated at its cwnd — borrowed
            // a floor-bypass slot. Lets the analysis confirm the bypass actually fired.
            bs_insert_u64(row, "floor_bypass", u64::from(floor_bypass));
        });
    }

    fn trace_body_received(
        &self,
        height: block::Height,
        serialized_bytes: u64,
        request_start_height: Option<block::Height>,
        request_range_count: Option<u32>,
        request_elapsed_ms: Option<u64>,
    ) {
        self.emit(bs_trace::BLOCK_BODY_RECEIVED, |row| {
            bs_insert_peer(row, bs_trace::PEER, &self.peer);
            bs_insert_height(row, bs_trace::HEIGHT, height);
            bs_insert_u64(row, bs_trace::SERIALIZED_BYTES, serialized_bytes);
            bs_insert_u64(row, bs_trace::BUDGET_RESERVED_AFTER, self.budget.reserved());
            bs_insert_u64(
                row,
                "sequencer_input_capacity",
                u64::try_from(self.sequencer_input.capacity()).unwrap_or(u64::MAX),
            );
            bs_insert_u64(
                row,
                "sequencer_input_max_capacity",
                u64::try_from(self.sequencer_input.max_capacity()).unwrap_or(u64::MAX),
            );
            if let Some(request_start_height) = request_start_height {
                bs_insert_height(row, "request_start", request_start_height);
            }
            if let Some(request_range_count) = request_range_count {
                bs_insert_u64(row, "request_range_count", u64::from(request_range_count));
            }
            if let Some(request_elapsed_ms) = request_elapsed_ms {
                bs_insert_u64(row, "request_elapsed_ms", request_elapsed_ms);
            }
            self.insert_bbr_fields(row);
        });
    }

    /// Insert the per-peer BBR controller fields (effective cwnd, RTprop, BtlBw, phase,
    /// delay-gradient ceiling, reliability) into a trace row. Shared by the per-delivery
    /// `block_body_received` row and the `block_peer_bbr` heartbeat so both report an
    /// identical field set.
    /// Insert the per-peer no-progress accounting fields shared by the GetBlocks-sent row
    /// and the BBR heartbeat, so the two row types stay in lockstep — one definition of the
    /// field names and their `u64` encoding, rather than a copy that can drift stylistically.
    fn insert_no_progress_fields(&self, row: &mut serde_json::Map<String, serde_json::Value>) {
        bs_insert_u64(
            row,
            "requests_without_block_progress",
            u64::from(self.window.requests_without_block_progress),
        );
        bs_insert_u64(
            row,
            "no_progress_request_cap",
            u64::from(self.window.no_progress_request_cap()),
        );
        bs_insert_u64(
            row,
            "block_progress_proven",
            u64::from(self.window.has_block_progress()),
        );
    }

    fn insert_bbr_fields(&self, row: &mut serde_json::Map<String, serde_json::Value>) {
        // Read the windowed estimators as of now, so a trace taken during a quiet bad
        // period reports freshly-filtered (possibly `None`) values, not stale ones.
        let now = Instant::now();
        bs_insert_u64(
            row,
            "bbr_cwnd",
            u64::try_from(self.window.bbr_effective_cwnd()).unwrap_or(u64::MAX),
        );
        if let Some(rtprop_ms) = self.window.bbr_rtprop_ms(now) {
            bs_insert_u64(row, "bbr_rtprop_ms", rtprop_ms);
        }
        if let Some(btlbw) = self.window.bbr_btlbw_milliblocks(now) {
            bs_insert_u64(row, "bbr_btlbw_milliblocks_per_sec", btlbw);
        }
        // Byte-denomination fields (emitted only under `CwndUnit::Bytes`): byte cwnd,
        // bytes/sec BtlBw, in-flight reserved bytes. `bbr_cwnd` above stays the derived
        // in-flight *request* count so existing analysis scripts work in either unit.
        if let Some(cwnd_bytes) = self.window.bbr_effective_cwnd_bytes() {
            bs_insert_u64(row, "bbr_cwnd_bytes", cwnd_bytes);
            bs_insert_u64(row, "bbr_inflight_bytes", self.window.bbr_inflight_bytes());
        }
        if let Some(btlbw_bytes) = self.window.bbr_btlbw_bytes_per_sec(now) {
            bs_insert_u64(row, "bbr_btlbw_bytes_per_sec", btlbw_bytes);
        }
        bs_insert_u64(row, "bbr_delivered", self.window.bbr_delivered());
        bs_insert_u64(row, "bbr_phase", self.window.bbr_phase_code());
        if let Some(smoothed_ms) = self.window.bbr_smoothed_elapsed_ms() {
            bs_insert_u64(row, "bbr_smoothed_elapsed_ms", smoothed_ms);
        }
        if let Some(delay_cap) = self.window.bbr_delay_cap() {
            bs_insert_u64(row, "bbr_delay_cap", delay_cap);
        }
        bs_insert_u64(
            row,
            "bbr_reliability_permille",
            self.window.bbr_reliability_permille(),
        );
    }

    /// Emit the periodic per-peer BBR heartbeat (`block_peer_bbr`). Fires even while the
    /// peer is idle, so the controller's balance is observable between deliveries — e.g.
    /// a cwnd that keeps ramping up only to be pulled back by the reliability discount
    /// instead of settling near `r = 1.0`.
    fn trace_bbr_sample(&self) {
        self.emit(bs_trace::BLOCK_PEER_BBR, |row| {
            bs_insert_peer(row, bs_trace::PEER, &self.peer);
            bs_insert_u64(
                row,
                "peer_outstanding",
                u64::try_from(self.window.active_len()).unwrap_or(u64::MAX),
            );
            bs_insert_u64(
                row,
                "peer_retired",
                u64::try_from(self.window.retired_len()).unwrap_or(u64::MAX),
            );
            bs_insert_u64(row, "budget_reserved", self.budget.reserved());
            self.insert_no_progress_fields(row);
            self.insert_bbr_fields(row);
        });
        // Refresh the published slot diagnostics on the same cadence so the cross-peer
        // floor-preference view cannot hold a stale-low RTprop for a quiet peer:
        // `publish_outstanding` re-reads `bbr_rtprop_ms(now)`, filtering out samples aged
        // past the horizon (→ `None` = worst floor server).
        self.publish_outstanding();
    }

    fn trace_body_sequencer_sent(&self, height: block::Height, elapsed: Duration, ok: bool) {
        self.emit(bs_trace::BLOCK_BODY_SEQUENCER_SENT, |row| {
            bs_insert_peer(row, bs_trace::PEER, &self.peer);
            bs_insert_height(row, bs_trace::HEIGHT, height);
            bs_insert_u64(
                row,
                "sequencer_send_elapsed_us",
                u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX),
            );
            row.insert("ok".to_string(), serde_json::Value::Bool(ok));
        });
    }

    fn trace_body_decode_permit(&self, elapsed: Duration, capacity_before: usize) {
        self.emit(bs_trace::BLOCK_BODY_DECODE_PERMIT, |row| {
            bs_insert_peer(row, bs_trace::PEER, &self.peer);
            bs_insert_u64(
                row,
                "decode_permit_wait_us",
                u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX),
            );
            bs_insert_u64(
                row,
                "sequencer_input_capacity_before",
                u64::try_from(capacity_before).unwrap_or(u64::MAX),
            );
            bs_insert_u64(
                row,
                "sequencer_input_max_capacity",
                u64::try_from(self.sequencer_input.max_capacity()).unwrap_or(u64::MAX),
            );
        });
    }

    fn trace_range_unavailable(
        &self,
        start_height: block::Height,
        range_count: Option<u32>,
        request_elapsed_ms: Option<u64>,
    ) {
        self.emit(bs_trace::BLOCK_RANGE_UNAVAILABLE, |row| {
            bs_insert_peer(row, bs_trace::PEER, &self.peer);
            bs_insert_height(row, bs_trace::RANGE_START, start_height);
            if let Some(range_count) = range_count {
                bs_insert_u64(row, bs_trace::RANGE_COUNT, u64::from(range_count));
            }
            if let Some(request_elapsed_ms) = request_elapsed_ms {
                bs_insert_u64(row, "request_elapsed_ms", request_elapsed_ms);
            }
        });
    }
}

fn elapsed_ms_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn insert_work_return_outcome(
    row: &mut serde_json::Map<String, serde_json::Value>,
    outcome: WorkReturnOutcome,
) {
    bs_insert_u64(row, "released_bytes", outcome.released_bytes);
    bs_insert_u64(row, "returned_count", outcome.returned_count);
    bs_insert_u64(row, "already_pending_count", outcome.already_pending_count);
    bs_insert_u64(row, "held_count", outcome.held_count);
    bs_insert_u64(row, "released_count", outcome.released_count);
    bs_insert_u64(row, "owner_mismatch_count", outcome.owner_mismatch_count);
    bs_insert_u64(row, "missing_count", outcome.missing_count);
    if let Some(height) = outcome.min_height {
        bs_insert_height(row, "return_min_height", height);
    }
    if let Some(height) = outcome.max_height {
        bs_insert_height(row, "return_max_height", height);
    }
}

fn reservation_owner(generation: u64, outstanding: &OutstandingBlockRange) -> ReservationOwner {
    ReservationOwner {
        generation,
        request_token: outstanding.token,
    }
}

/// Unwind a global request admission that could not be fully attached to its
/// owner in the work queue.
///
/// `admitted_bytes - marked_bytes` was charged to the shared budget but never
/// entered a queue ledger. The owner-aware queue release returns the remainder
/// only when it still belongs to this request; replacements and held bodies are
/// left untouched.
fn release_failed_request_reservation(
    work: &WorkQueue,
    budget: &mut super::state::ByteBudget,
    heights: impl IntoIterator<Item = block::Height>,
    owner: ReservationOwner,
    admitted_bytes: u64,
    marked_bytes: u64,
) {
    let unattached_bytes = admitted_bytes.saturating_sub(marked_bytes);
    let released_bytes = work.release_reserved_and_return_items(heights, owner);
    budget.release(unattached_bytes.saturating_add(released_bytes));
}

/// The still-unreceived heights of an outstanding request (the ones that return
/// to `pending` on retry/timeout — never the received-and-buffered ones, which
/// stay claimed in `work.in_flight`).
fn unreceived_heights(
    outstanding: &OutstandingBlockRange,
) -> impl Iterator<Item = block::Height> + '_ {
    outstanding
        .request
        .expected_blocks
        .iter()
        .filter(move |expected| !outstanding.has_received(expected.height))
        .map(|expected| expected.height)
}

fn outstanding_unreceived_through(
    outstanding: &OutstandingBlockRange,
    tip: block::Height,
) -> impl Iterator<Item = block::Height> + '_ {
    outstanding
        .request
        .expected_blocks
        .iter()
        .filter(move |expected| {
            expected.height <= tip && !outstanding.has_received(expected.height)
        })
        .map(|expected| expected.height)
}

impl Drop for PeerRoutine {
    /// disconnect-mid-fetch correctness: on every exit path
    /// (cancel/panic/normal) return this routine's unreceived outstanding heights
    /// to `work.pending`, release their byte reservation, and clear this peer's
    /// outstanding set in the registry. All operations are sync (lock/atomic), so
    /// the guard is cancel-safe and panic-safe.
    ///
    /// The guard clears the peer's *outstanding* rather than removing the whole
    /// registry entry: a reset respawns the routine (the reactor cancels + spawns
    /// a fresh one) while the peer stays connected, so its servable/caps must
    /// survive. If the guard removed the entry, an old routine's async Drop could
    /// race *after* the respawned routine re-inserted and nuke the live entry.
    /// The reactor owns entry insert (on connect) and remove (on disconnect/
    /// admission-reject); see `handle_peer_disconnected`.
    fn drop(&mut self) {
        let outstanding_ranges = self.window.outstanding.drain_all();
        for outstanding in outstanding_ranges {
            if outstanding.is_retired() {
                continue;
            }
            // Held-aware: a height a competing peer delivered late is owned by the
            // Sequencer, so return + release only still-reserved unreceived heights.
            let unreceived: Vec<_> = outstanding
                .request
                .expected_blocks
                .iter()
                .filter(|expected| !outstanding.has_received(expected.height))
                .map(|expected| expected.height)
                .collect();
            let outcome = self.work.release_reserved_and_return_items_detailed(
                unreceived.iter().copied(),
                reservation_owner(self.generation, &outstanding),
            );
            self.budget.release(outcome.released_bytes);
            self.trace_work_returned("peer_routine_drop", &outstanding, unreceived.len(), outcome);
        }
        self.registry.clear_outstanding(&self.peer, self.generation);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use tokio::sync::{mpsc, watch};
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;
    use zakura_chain::{block, serialization::ZcashDeserializeInto};
    use zakura_test::vectors::BLOCK_MAINNET_1_BYTES;

    use super::super::outstanding::{
        OutstandingBlockRange, OutstandingRequestState, ReceivedBlockTracker, RetirementReason,
    };
    use super::super::peer_registry::PeerRegistry;
    use super::super::request::BlockSizeEstimate;
    use super::super::request::{BlockRangeRequest, ExpectedBlock};
    use super::super::sequencer_task::{initial_view, SequencerControlInput};
    use super::super::state::{ByteBudget, LivenessOutcome, ThroughputMeter};
    use super::super::work_queue::{ReservationOwner, WorkQueue};
    use super::super::{BlockSyncFrontiers, BlockSyncPeerSession, ZakuraBlockSyncConfig};
    use super::{release_failed_request_reservation, Disposition, PeerRoutine};
    use crate::zakura::framed_channel;
    use crate::zakura::trace::ZakuraTrace;
    use crate::zakura::{ServicePeerDirection, ZakuraPeerId};

    #[test]
    fn pre_reservation_aba_cleanup_preserves_budget_and_replacement_owner() {
        let work = WorkQueue::new(block::Height(0));
        let mut budget = ByteBudget::new(8_192);
        let height = block::Height(1);
        let old_estimate = 1_024;
        let replacement_estimate = 2_048;
        let old_owner = ReservationOwner {
            generation: 1,
            request_token: 1,
        };
        let replacement_owner = ReservationOwner {
            generation: 2,
            request_token: 1,
        };

        assert_eq!(
            work.extend([(
                height,
                block::Hash([1; 32]),
                BlockSizeEstimate::Advertised(old_estimate),
            )]),
            1
        );
        assert_eq!(
            work.take_in_range_budgeted_owned(height, height, 1, u64::MAX, old_owner,)
                .len(),
            1
        );

        // While the old routine is waiting for global admission, a destructive
        // reset removes its unfunded take and repopulates the same height.
        assert_eq!(work.reset_above(block::Height(0)), 0);
        assert_eq!(
            work.extend([(
                height,
                block::Hash([2; 32]),
                BlockSizeEstimate::Advertised(replacement_estimate),
            )]),
            1
        );
        assert_eq!(
            work.take_in_range_budgeted_owned(height, height, 1, u64::MAX, replacement_owner,)
                .len(),
            1
        );
        assert!(budget.try_reserve(u64::from(replacement_estimate)));
        assert_eq!(
            work.mark_reserved([height], replacement_owner),
            u64::from(replacement_estimate)
        );

        // The old admission then completes. Its owner-bound mark must reject the
        // replacement, and its unwind must release exactly the unattached old
        // admission without returning or uncharging replacement work.
        assert!(budget.try_reserve(u64::from(old_estimate)));
        let marked = work.mark_reserved([height], old_owner);
        assert_eq!(marked, 0);
        release_failed_request_reservation(
            &work,
            &mut budget,
            [height],
            old_owner,
            u64::from(old_estimate),
            marked,
        );

        assert!(work.in_flight_contains(height));
        assert!(!work.pending_contains(height));
        assert_eq!(
            work.reserved_bytes(),
            u64::from(replacement_estimate),
            "the queue must retain exactly the replacement charge"
        );
        assert_eq!(
            budget.reserved(),
            u64::from(replacement_estimate),
            "the global budget must retain exactly the replacement charge"
        );

        assert_eq!(
            work.release_reserved_and_return_items([height], replacement_owner),
            u64::from(replacement_estimate)
        );
        budget.release(u64::from(replacement_estimate));
        assert_eq!(work.reserved_bytes(), 0);
        assert_eq!(budget.reserved(), 0);
    }

    /// A floor request whose byte reservation cannot be met must still reach the
    /// sequencer's floor-funding path so the rescue shed can free room — even when the
    /// byte budget is *exactly* full.
    ///
    /// Regression guard for the wedge where `try_fill`'s floor arm sized its take by
    /// `budget.available()`: at `available() == 0` the take came back empty, the fill
    /// loop broke, and `reserve_request_budget` (the only caller that emits
    /// `FundFloorReservation`) was never reached — so the shed that would rescue the
    /// floor never fired and the floor wedged permanently. The fix sizes the floor take
    /// by one response and lets the reservation shed; here we assert the funding request
    /// is emitted with a non-zero need.
    #[tokio::test]
    async fn exhausted_budget_floor_request_still_reaches_the_funding_path() {
        let config = ZakuraBlockSyncConfig::default();

        // A byte budget reserved down to exactly zero free: the case that used to wedge.
        let mut budget = ByteBudget::new(8_192);
        assert!(budget.try_reserve(8_192));
        assert_eq!(budget.available(), 0, "the budget is exactly full");

        // The floor height (1) is pending and servable by this peer; the download floor
        // is 0 so height 1 is the floor.
        let work = Arc::new(WorkQueue::new(block::Height(0)));
        assert_eq!(
            work.extend([(
                block::Height(1),
                block::Hash([1; 32]),
                BlockSizeEstimate::Advertised(1_000),
            )]),
            1,
        );

        let cancel = CancellationToken::new();
        let (out_send, _out_recv) = framed_channel(16);
        let (_in_send, in_recv) = framed_channel(16);
        let peer = ZakuraPeerId::new(vec![7u8; 32]).expect("test peer id is within bounds");
        let session = BlockSyncPeerSession::for_test(peer.clone(), out_send, cancel.clone());

        let (sequencer_input_tx, _sequencer_input_rx) = mpsc::channel(16);
        let (control_tx, mut control_rx) = mpsc::unbounded_channel();
        let (actions_tx, _actions_rx) = mpsc::channel(16);
        let (routine_to_reactor_tx, _routine_to_reactor_rx) = mpsc::channel(16);
        let (_view_tx, view_rx) = watch::channel(initial_view(BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        }));

        let mut routine = PeerRoutine::new(
            peer,
            session,
            in_recv,
            config,
            0,
            budget,
            work,
            Arc::new(PeerRegistry::new()),
            Arc::new(Mutex::new(ThroughputMeter::new(Instant::now()))),
            sequencer_input_tx,
            Arc::new(AtomicU64::new(0)),
            control_tx,
            actions_tx,
            routine_to_reactor_tx,
            view_rx,
            cancel,
            ZakuraTrace::noop(),
        );
        // The routine learns these from a `Status` frame in production; set them directly
        // so a single `try_fill` pass exercises the floor arm.
        routine.received_status = true;
        routine.servable_low = block::Height(1);
        routine.servable_high = block::Height(10);

        let fill = tokio::spawn(async move {
            routine.try_fill().await;
        });

        let message = timeout(Duration::from_secs(5), control_rx.recv())
            .await
            .expect("an exhausted floor must request funding within the timeout")
            .expect("the sequencer-control channel stays open");
        match message {
            SequencerControlInput::FundFloorReservation {
                needed_bytes,
                reply,
            } => {
                assert!(
                    needed_bytes > 0,
                    "the floor reservation funds a non-zero request",
                );
                // No reorder body to shed in this unit-level test: deny the funding. The
                // routine returns the taken floor height and exits the pass cleanly.
                let _ = reply.send(false);
            }
            other => panic!("expected FundFloorReservation, got {other:?}"),
        }

        fill.await
            .expect("try_fill completes after the funding decision");
    }

    /// First-completion-wins can settle a height a routine still owns to `Held` when
    /// a competing peer delivers it first. This routine's teardown (`Drop`) must be
    /// Held-aware: the held body is owned by the Sequencer, so `Drop` must neither
    /// release its bytes a second time (the Sequencer releases them on commit) nor
    /// re-queue a body already in the commit pipeline. The pre-fix `Drop` used
    /// `release_and_return_items`, which for a `Held(actual)` height returned
    /// `actual` — double-releasing the `ByteBudget` and re-queuing the height into
    /// `pending`.
    #[tokio::test]
    async fn routine_drop_leaves_a_body_won_by_another_peer_to_the_sequencer() {
        let config = ZakuraBlockSyncConfig::default();

        // Ample budget so the floor take reserves directly (no funding round-trip)
        // and sends a real request, creating the outstanding claim.
        let budget = ByteBudget::new(1_000_000);
        let budget_probe = budget.clone();

        // Height 1 is the floor (download floor is 0) and this peer's only work item.
        let work = Arc::new(WorkQueue::new(block::Height(0)));
        work.set_estimate_floor_for_tests(1);
        assert_eq!(
            work.extend([(
                block::Height(1),
                block::Hash([1; 32]),
                BlockSizeEstimate::Advertised(1_000),
            )]),
            1,
        );

        let cancel = CancellationToken::new();
        let (out_send, _out_recv) = framed_channel(16);
        let (_in_send, in_recv) = framed_channel(16);
        let peer = ZakuraPeerId::new(vec![9u8; 32]).expect("test peer id is within bounds");
        let session = BlockSyncPeerSession::for_test(peer.clone(), out_send, cancel.clone());

        let (sequencer_input_tx, _sequencer_input_rx) = mpsc::channel(16);
        let (control_tx, _control_rx) = mpsc::unbounded_channel();
        let (actions_tx, _actions_rx) = mpsc::channel(16);
        let (routine_to_reactor_tx, _routine_to_reactor_rx) = mpsc::channel(16);
        let (_view_tx, view_rx) = watch::channel(initial_view(BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        }));

        let mut routine = PeerRoutine::new(
            peer,
            session,
            in_recv,
            config,
            0,
            budget,
            Arc::clone(&work),
            Arc::new(PeerRegistry::new()),
            Arc::new(Mutex::new(ThroughputMeter::new(Instant::now()))),
            sequencer_input_tx,
            Arc::new(AtomicU64::new(0)),
            control_tx,
            actions_tx,
            routine_to_reactor_tx,
            view_rx,
            cancel,
            ZakuraTrace::noop(),
        );
        routine.received_status = true;
        routine.servable_low = block::Height(1);
        routine.servable_high = block::Height(10);

        // One fill pass: the routine reserves height 1's estimate and sends its
        // request, creating an outstanding claim for a still-reserved height.
        timeout(Duration::from_secs(5), routine.try_fill())
            .await
            .expect("try_fill completes");
        assert!(
            work.in_flight_contains(block::Height(1)),
            "height 1 is reserved and outstanding after the fill"
        );
        assert!(!work.pending_contains(block::Height(1)));
        assert_eq!(budget_probe.reserved(), 1_000);
        assert_eq!(routine.window.outstanding.len(), 1);

        // A competing peer delivers height 1 first: settle the shared reservation to
        // `Held(actual)`. The estimate matches the actual, so the budget is unchanged
        // and now holds the body's actual bytes.
        let delta = work
            .settle_active_reserved_height(block::Height(1), 1_000)
            .expect("height 1 still owns its active reservation");
        assert_eq!(delta, 0);
        assert_eq!(budget_probe.reserved(), 1_000);

        // Tear the routine down while it still lists height 1 as unreceived. `Drop`
        // is synchronous, so its cleanup is observable immediately.
        drop(routine);

        assert_eq!(
            budget_probe.reserved(),
            1_000,
            "Drop double-released the held body's bytes (ByteBudget drift)"
        );
        assert!(
            !work.pending_contains(block::Height(1)),
            "Drop phantom-re-queued a body already held in the commit pipeline"
        );
        assert!(
            work.in_flight_contains(block::Height(1)),
            "the held body stays in_flight for the Sequencer to release on commit"
        );
    }

    /// The liveness grace is granted only for genuinely-transient local write congestion:
    /// outbound full but full for *less* than `request_timeout`.
    #[test]
    fn liveness_grace_only_for_fresh_outbound_backpressure() {
        let now = Instant::now();
        let request_timeout = Duration::from_secs(8);

        // Outbound just filled (1 s ago): plausibly our own write congestion — grace.
        let fresh = now - Duration::from_secs(1);
        assert!(super::liveness_grace_allowed(
            true,
            Some(fresh),
            now,
            request_timeout
        ));

        // Outbound has been full for a full `request_timeout` (the peer has stopped
        // reading): NO grace — the peer is disconnected at the liveness deadline.
        let sustained = now - request_timeout;
        assert!(!super::liveness_grace_allowed(
            true,
            Some(sustained),
            now,
            request_timeout
        ));
        let long = now - Duration::from_secs(30);
        assert!(!super::liveness_grace_allowed(
            true,
            Some(long),
            now,
            request_timeout
        ));

        // Outbound has capacity (not full): the escape does not apply — disconnects normally.
        assert!(!super::liveness_grace_allowed(
            false,
            Some(fresh),
            now,
            request_timeout
        ));
        // Full but no recorded start (defensive, shouldn't happen while full): no grace.
        assert!(!super::liveness_grace_allowed(
            true,
            None,
            now,
            request_timeout
        ));
    }

    #[tokio::test]
    async fn fill_does_not_reissue_height_with_active_or_retired_local_request() {
        let config = ZakuraBlockSyncConfig::default();
        let budget = ByteBudget::new(1_000_000);
        let work = Arc::new(WorkQueue::new(block::Height(0)));
        work.set_estimate_floor_for_tests(1);
        assert_eq!(
            work.extend([(
                block::Height(1),
                block::Hash([1; 32]),
                BlockSizeEstimate::Advertised(1_000),
            )]),
            1,
        );

        let cancel = CancellationToken::new();
        let (out_send, mut out_recv) = framed_channel(16);
        let (_in_send, in_recv) = framed_channel(16);
        let peer = ZakuraPeerId::new(vec![8u8; 32]).expect("test peer id is within bounds");
        let session = BlockSyncPeerSession::for_test(peer.clone(), out_send, cancel.clone());
        let (sequencer_input_tx, _sequencer_input_rx) = mpsc::channel(16);
        let (control_tx, _control_rx) = mpsc::unbounded_channel();
        let (actions_tx, _actions_rx) = mpsc::channel(16);
        let (routine_to_reactor_tx, _routine_to_reactor_rx) = mpsc::channel(16);
        let (_view_tx, view_rx) = watch::channel(initial_view(BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        }));

        let mut routine = PeerRoutine::new(
            peer,
            session,
            in_recv,
            config,
            0,
            budget,
            Arc::clone(&work),
            Arc::new(PeerRegistry::new()),
            Arc::new(Mutex::new(ThroughputMeter::new(Instant::now()))),
            sequencer_input_tx,
            Arc::new(AtomicU64::new(0)),
            control_tx,
            actions_tx,
            routine_to_reactor_tx,
            view_rx,
            cancel,
            ZakuraTrace::noop(),
        );
        routine.received_status = true;
        routine.servable_low = block::Height(1);
        routine.servable_high = block::Height(1);

        let now = Instant::now();
        routine.window.outstanding.push(OutstandingBlockRange {
            token: 1,
            state: OutstandingRequestState::Active,
            request: BlockRangeRequest {
                start_height: block::Height(1),
                count: 1,
                anchor_hash: block::Hash([1; 32]),
                estimated_bytes: 1_000,
                expected_blocks: vec![ExpectedBlock {
                    height: block::Height(1),
                    hash: block::Hash([1; 32]),
                    estimated_bytes: 1_000,
                }],
            },
            queued_at: now,
            deadline: now + Duration::from_secs(1),
            delivery_snapshot: routine.window.delivery_snapshot(now),
            delivered_bytes: 0,
            received: ReceivedBlockTracker::default(),
            late_reliability_credited: false,
        });

        routine.try_fill().await;
        assert!(work.pending_contains(block::Height(1)));
        assert!(
            timeout(Duration::from_millis(20), out_recv.recv())
                .await
                .is_err(),
            "an active local request must prevent ambiguous same-peer reissue"
        );

        assert!(routine.window.outstanding.retire(
            0,
            RetirementReason::FloorWatchdog,
            now,
            now + Duration::from_secs(1),
        ));
        routine.try_fill().await;
        assert!(work.pending_contains(block::Height(1)));
        assert!(
            timeout(Duration::from_millis(20), out_recv.recv())
                .await
                .is_err(),
            "a retired tombstone must prevent ambiguous same-peer reissue"
        );
    }

    #[tokio::test]
    async fn view_reset_quarantines_old_terminator_until_reissue_is_safe() {
        let config = ZakuraBlockSyncConfig::default();
        let budget = ByteBudget::new(1_000_000);
        let budget_probe = budget.clone();
        let height = block::Height(1);
        let hash = block::Hash([1; 32]);
        let work = Arc::new(WorkQueue::new(block::Height(0)));
        work.set_estimate_floor_for_tests(1);
        assert_eq!(
            work.extend([(height, hash, BlockSizeEstimate::Advertised(1_000))]),
            1,
        );

        let cancel = CancellationToken::new();
        let (out_send, mut out_recv) = framed_channel(16);
        let (_in_send, in_recv) = framed_channel(16);
        let peer = ZakuraPeerId::new(vec![11u8; 32]).expect("test peer id is within bounds");
        let session = BlockSyncPeerSession::for_test(peer.clone(), out_send, cancel.clone());
        let registry = Arc::new(PeerRegistry::new());
        let generation = registry.admit(&peer, ServicePeerDirection::Outbound, &config);
        let (sequencer_input_tx, _sequencer_input_rx) = mpsc::channel(16);
        let (control_tx, _control_rx) = mpsc::unbounded_channel();
        let (actions_tx, _actions_rx) = mpsc::channel(16);
        let (routine_to_reactor_tx, _routine_to_reactor_rx) = mpsc::channel(16);
        let (view_tx, view_rx) = watch::channel(initial_view(BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        }));

        let mut routine = PeerRoutine::new(
            peer.clone(),
            session,
            in_recv,
            config,
            generation,
            budget,
            Arc::clone(&work),
            Arc::clone(&registry),
            Arc::new(Mutex::new(ThroughputMeter::new(Instant::now()))),
            sequencer_input_tx,
            Arc::new(AtomicU64::new(0)),
            control_tx,
            actions_tx,
            routine_to_reactor_tx,
            view_rx,
            cancel,
            ZakuraTrace::noop(),
        );
        routine.received_status = true;
        routine.servable_low = height;
        routine.servable_high = height;

        timeout(Duration::from_secs(5), routine.try_fill())
            .await
            .expect("initial fill completes");
        assert!(timeout(Duration::from_millis(100), out_recv.recv())
            .await
            .expect("initial request is sent")
            .is_some());
        assert_eq!(routine.window.active_len(), 1);
        assert!(registry.peer_has_outstanding_height(&peer, height));
        assert_eq!(budget_probe.reserved(), 1_000);

        let preexisting_retired_state = OutstandingRequestState::Retired {
            reason: RetirementReason::FloorWatchdog,
            retired_at: Instant::now() - Duration::from_secs(1),
            correlation_deadline: Instant::now() + Duration::from_secs(300),
        };
        routine.window.outstanding.push(OutstandingBlockRange {
            token: u64::MAX,
            state: preexisting_retired_state,
            request: BlockRangeRequest {
                start_height: block::Height(2),
                count: 1,
                anchor_hash: block::Hash([2; 32]),
                estimated_bytes: 0,
                expected_blocks: vec![ExpectedBlock {
                    height: block::Height(2),
                    hash: block::Hash([2; 32]),
                    estimated_bytes: 0,
                }],
            },
            queued_at: Instant::now(),
            deadline: Instant::now(),
            delivery_snapshot: routine.window.delivery_snapshot(Instant::now()),
            delivered_bytes: 0,
            received: ReceivedBlockTracker::default(),
            late_reliability_credited: false,
        });

        view_tx.send_modify(|view| {
            view.reset_epoch = view.reset_epoch.saturating_add(1);
        });
        routine.on_view_changed();

        assert_eq!(routine.window.active_len(), 0);
        assert_eq!(routine.window.retired_len(), 2);
        assert_eq!(routine.window.outstanding.active_reserved_bytes(), 0);
        assert_eq!(budget_probe.reserved(), 0);
        assert!(work.pending_contains(height));
        assert!(!registry.peer_has_outstanding_height(&peer, height));
        assert!(
            routine.window.block_liveness_deadline.is_none(),
            "reset must disarm liveness even though retained tombstones keep the collection non-empty"
        );
        assert_eq!(
            routine
                .window
                .outstanding
                .iter()
                .find(|outstanding| outstanding.token == u64::MAX)
                .expect("pre-existing tombstone survives reset")
                .state,
            preexisting_retired_state,
            "reset must preserve a pre-existing tombstone's state and deadline unchanged"
        );
        assert_eq!(
            routine
                .window
                .outstanding
                .iter()
                .find(|outstanding| outstanding.request.start_height == height)
                .expect("reset request remains correlated")
                .retirement_reason(),
            Some(RetirementReason::ViewReset)
        );

        routine.try_fill().await;
        assert!(work.pending_contains(height));
        assert!(
            timeout(Duration::from_millis(20), out_recv.recv())
                .await
                .is_err(),
            "the reset tombstone must quarantine the old terminator before reissue"
        );

        routine.handle_blocks_done(height).await;
        assert_eq!(routine.window.retired_len(), 1);
        routine.try_fill().await;
        assert!(timeout(Duration::from_millis(100), out_recv.recv())
            .await
            .expect("reissue is sent after the old terminator closes its tombstone")
            .is_some());
        assert_eq!(routine.window.active_len(), 1);
        assert_eq!(routine.window.retired_len(), 1);
        assert!(registry.peer_has_outstanding_height(&peer, height));
    }

    /// The deadline arm must prune expired retired tombstones itself: while the
    /// outbound queue is full the loop skips `try_fill` (the only other prune
    /// site), so an expired correlation deadline left in place would hold
    /// `earliest_deadline_sleep` at zero and busy-spin the select loop.
    #[tokio::test]
    async fn deadline_arm_prunes_expired_retired_tombstones() {
        let config = ZakuraBlockSyncConfig::default();
        let budget = ByteBudget::new(1_000_000);
        let work = Arc::new(WorkQueue::new(block::Height(0)));

        let cancel = CancellationToken::new();
        let (out_send, _out_recv) = framed_channel(16);
        let (_in_send, in_recv) = framed_channel(16);
        let peer = ZakuraPeerId::new(vec![7u8; 32]).expect("test peer id is within bounds");
        let session = BlockSyncPeerSession::for_test(peer.clone(), out_send, cancel.clone());
        let (sequencer_input_tx, _sequencer_input_rx) = mpsc::channel(16);
        let (control_tx, _control_rx) = mpsc::unbounded_channel();
        let (actions_tx, _actions_rx) = mpsc::channel(16);
        let (routine_to_reactor_tx, _routine_to_reactor_rx) = mpsc::channel(16);
        let (_view_tx, view_rx) = watch::channel(initial_view(BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        }));

        let mut routine = PeerRoutine::new(
            peer,
            session,
            in_recv,
            config,
            0,
            budget,
            Arc::clone(&work),
            Arc::new(PeerRegistry::new()),
            Arc::new(Mutex::new(ThroughputMeter::new(Instant::now()))),
            sequencer_input_tx,
            Arc::new(AtomicU64::new(0)),
            control_tx,
            actions_tx,
            routine_to_reactor_tx,
            view_rx,
            cancel,
            ZakuraTrace::noop(),
        );

        let now = Instant::now();
        // A proven peer: it delivered a body after its last request was armed...
        routine
            .window
            .arm_liveness(now - Duration::from_secs(60), Duration::from_secs(300));
        routine.window.outstanding.push(OutstandingBlockRange {
            token: 1,
            state: OutstandingRequestState::Active,
            request: BlockRangeRequest {
                start_height: block::Height(1),
                count: 1,
                anchor_hash: block::Hash([1; 32]),
                estimated_bytes: 0,
                expected_blocks: vec![ExpectedBlock {
                    height: block::Height(1),
                    hash: block::Hash([1; 32]),
                    estimated_bytes: 0,
                }],
            },
            queued_at: now - Duration::from_secs(60),
            deadline: now + Duration::from_secs(60),
            delivery_snapshot: routine
                .window
                .delivery_snapshot(now - Duration::from_secs(60)),
            delivered_bytes: 0,
            received: ReceivedBlockTracker::default(),
            late_reliability_credited: false,
        });
        routine
            .window
            .note_block_progress(now - Duration::from_secs(30), Duration::from_secs(300));
        // ...and its covered request's correlation window has already expired.
        assert!(routine.window.outstanding.retire(
            0,
            RetirementReason::Covered,
            now - Duration::from_secs(10),
            now - Duration::from_secs(1),
        ));

        assert!(routine.handle_deadlines(now).await.is_ok());
        assert!(
            routine.window.outstanding.is_empty(),
            "the deadline arm must prune the expired tombstone so \
             `earliest_deadline_sleep` stops returning an already-due deadline"
        );
        assert!(
            routine.window.block_liveness_deadline.is_none(),
            "an idle proven peer disarms once its expired tombstone is pruned"
        );
    }

    /// The deadline arm must also prune expired retry-avoid entries. They feed
    /// `earliest_deadline_sleep` unfiltered but are otherwise pruned only in
    /// `try_fill`, which the loop skips while the outbound queue is full — so a due
    /// entry left in place would resolve the timeout arm at zero delay and busy-spin.
    #[tokio::test]
    async fn deadline_arm_prunes_expired_retry_avoid_entries() {
        let config = ZakuraBlockSyncConfig::default();
        let budget = ByteBudget::new(1_000_000);
        let work = Arc::new(WorkQueue::new(block::Height(0)));

        let cancel = CancellationToken::new();
        let (out_send, _out_recv) = framed_channel(16);
        let (_in_send, in_recv) = framed_channel(16);
        let peer = ZakuraPeerId::new(vec![6u8; 32]).expect("test peer id is within bounds");
        let session = BlockSyncPeerSession::for_test(peer.clone(), out_send, cancel.clone());
        let (sequencer_input_tx, _sequencer_input_rx) = mpsc::channel(16);
        let (control_tx, _control_rx) = mpsc::unbounded_channel();
        let (actions_tx, _actions_rx) = mpsc::channel(16);
        let (routine_to_reactor_tx, _routine_to_reactor_rx) = mpsc::channel(16);
        let (_view_tx, view_rx) = watch::channel(initial_view(BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        }));

        let mut routine = PeerRoutine::new(
            peer,
            session,
            in_recv,
            config,
            0,
            budget,
            work,
            Arc::new(PeerRegistry::new()),
            Arc::new(Mutex::new(ThroughputMeter::new(Instant::now()))),
            sequencer_input_tx,
            Arc::new(AtomicU64::new(0)),
            control_tx,
            actions_tx,
            routine_to_reactor_tx,
            view_rx,
            cancel,
            ZakuraTrace::noop(),
        );

        let now = Instant::now();
        // An already-expired retry-avoid entry, as a full outbound queue would leave
        // it (no `try_fill` to prune it).
        routine
            .retry_avoid
            .insert(block::Height(5), now - Duration::from_secs(1));

        assert!(routine.handle_deadlines(now).await.is_ok());
        assert!(
            routine.retry_avoid.is_empty(),
            "the deadline arm must drop expired retry-avoid entries so \
             `earliest_deadline_sleep` stops returning an already-due deadline"
        );
    }

    /// Floor-GC retires a request from active scheduling while preserving its
    /// peer-accountability deadline.
    #[tokio::test]
    async fn floor_gc_retired_terminator_uses_accepted_progress_for_liveness() {
        let config = ZakuraBlockSyncConfig::default();
        let budget = ByteBudget::new(1_000_000);
        let work = Arc::new(WorkQueue::new(block::Height(0)));

        let cancel = CancellationToken::new();
        let (out_send, _out_recv) = framed_channel(16);
        let (_in_send, in_recv) = framed_channel(16);
        let peer = ZakuraPeerId::new(vec![8u8; 32]).expect("test peer id is within bounds");
        let session = BlockSyncPeerSession::for_test(peer.clone(), out_send, cancel.clone());

        let (sequencer_input_tx, _sequencer_input_rx) = mpsc::channel(16);
        let (control_tx, _control_rx) = mpsc::unbounded_channel();
        let (actions_tx, _actions_rx) = mpsc::channel(16);
        let (routine_to_reactor_tx, _routine_to_reactor_rx) = mpsc::channel(16);
        // The download floor sits above the request below, as if other peers
        // delivered those heights and the sequencer committed them.
        let (_view_tx, view_rx) = watch::channel(initial_view(BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(100),
            verified_block_hash: block::Hash([0; 32]),
        }));

        let mut routine = PeerRoutine::new(
            peer,
            session,
            in_recv,
            config,
            0,
            budget,
            work,
            Arc::new(PeerRegistry::new()),
            Arc::new(Mutex::new(ThroughputMeter::new(Instant::now()))),
            sequencer_input_tx,
            Arc::new(AtomicU64::new(0)),
            control_tx,
            actions_tx,
            routine_to_reactor_tx,
            view_rx,
            cancel,
            ZakuraTrace::noop(),
        );

        let now = Instant::now();
        let liveness = Duration::from_secs(30);

        // The peer delivered a block for an earlier request...
        routine
            .window
            .arm_liveness(now - Duration::from_secs(120), liveness);
        routine
            .window
            .note_block_progress(now - Duration::from_secs(90), liveness);

        // ...then received a newer request (deadline re-armed) whose heights
        // the floor later passed.
        routine
            .window
            .arm_liveness(now - Duration::from_secs(60), liveness);
        routine.window.outstanding.push(OutstandingBlockRange {
            token: 1,
            state: OutstandingRequestState::Active,
            request: BlockRangeRequest {
                start_height: block::Height(99),
                count: 2,
                anchor_hash: block::Hash([9; 32]),
                estimated_bytes: 0,
                expected_blocks: vec![
                    ExpectedBlock {
                        height: block::Height(99),
                        hash: block::Hash([99; 32]),
                        estimated_bytes: 0,
                    },
                    ExpectedBlock {
                        height: block::Height(100),
                        hash: block::Hash([100; 32]),
                        estimated_bytes: 0,
                    },
                ],
            },
            queued_at: now - Duration::from_secs(60),
            deadline: now + Duration::from_secs(60),
            delivery_snapshot: routine
                .window
                .delivery_snapshot(now - Duration::from_secs(60)),
            delivered_bytes: 0,
            received: ReceivedBlockTracker::default(),
            late_reliability_credited: false,
        });

        routine.gc_committed_outstanding();
        let retired_request = routine.window.outstanding[0].clone();

        assert_eq!(
            routine.window.active_len(),
            0,
            "the floor passed the whole request, so it must stop consuming an active slot"
        );
        assert!(
            routine.window.outstanding[0].is_retired(),
            "the request remains as a response-correlation tombstone"
        );
        assert!(
            routine.window.block_liveness_deadline.is_some(),
            "retirement must preserve peer accountability"
        );
        assert!(
            matches!(
                routine.window.check_liveness(now),
                LivenessOutcome::Disconnect
            ),
            "a silent retired peer must still reach its finite deadline"
        );

        let no_progress_requests = routine.window.requests_without_block_progress;
        let liveness_deadline = routine.window.block_liveness_deadline;
        routine.handle_blocks_done(block::Height(99)).await;
        assert!(
            routine.window.outstanding.is_empty(),
            "a matching retired terminator closes only its correlation tombstone"
        );
        assert_eq!(
            routine.window.requests_without_block_progress, no_progress_requests,
            "a terminator is not accepted block progress"
        );
        assert_eq!(
            routine.window.block_liveness_deadline, liveness_deadline,
            "a retired terminator must not erase peer accountability"
        );

        // If accepted progress happened after the latest request, removing the
        // final correlation tombstone must not leave that proven peer subject to
        // a stale deadline.
        routine.window.outstanding.push(retired_request.clone());
        routine.window.arm_liveness(now, liveness);
        routine
            .window
            .note_block_progress(now + Duration::from_millis(1), liveness);
        routine.handle_blocks_done(block::Height(99)).await;
        assert!(routine.window.outstanding.is_empty());
        assert_eq!(
            routine.window.block_liveness_deadline, None,
            "closing the final retired tombstone must disarm liveness after accepted progress"
        );
        assert_eq!(
            routine.window.check_liveness(now + liveness),
            LivenessOutcome::Ok
        );

        // Retry dispositions, including short responses, remove active requests
        // through `finish_detached` and need the same causal disarm.
        let retry_at = now + Duration::from_secs(1);
        let mut retrying_request = retired_request;
        retrying_request.token = 2;
        retrying_request.state = OutstandingRequestState::Active;
        routine.window.outstanding.push(retrying_request);
        routine.window.arm_liveness(retry_at, liveness);
        routine
            .window
            .note_block_progress(retry_at + Duration::from_millis(1), liveness);
        routine.finish_outstanding_at(0, Disposition::RetryMissing);
        assert!(routine.window.outstanding.is_empty());
        assert_eq!(
            routine.window.block_liveness_deadline, None,
            "retry cleanup must disarm liveness after accepted progress"
        );
    }

    /// A discarded body retains the retired request as a correlation tombstone.
    /// That tombstone blocks ambiguous same-peer reissue until the old response's
    /// terminator consumes it, after which the normal fill path can reissue.
    #[tokio::test]
    async fn discarded_retired_body_tombstone_blocks_reissue_until_terminator() {
        let block: Arc<block::Block> = Arc::new(
            BLOCK_MAINNET_1_BYTES
                .zcash_deserialize_into()
                .expect("block vector parses"),
        );
        let height = block
            .coinbase_height()
            .expect("mainnet block one has a coinbase height");
        let hash = block.hash();
        let config = ZakuraBlockSyncConfig::default();
        let budget = ByteBudget::new(1_000_000);
        // Deliberately leave the WorkQueue empty so the matching retired body
        // cannot be claimed and `accept_unmatched_queued_body` returns NotHandled.
        let work = Arc::new(WorkQueue::new(block::Height(0)));

        let cancel = CancellationToken::new();
        let (out_send, mut out_recv) = framed_channel(16);
        let (_in_send, in_recv) = framed_channel(16);
        let peer = ZakuraPeerId::new(vec![10u8; 32]).expect("test peer id is within bounds");
        let session = BlockSyncPeerSession::for_test(peer.clone(), out_send, cancel.clone());
        let (sequencer_input_tx, _sequencer_input_rx) = mpsc::channel(16);
        let (control_tx, _control_rx) = mpsc::unbounded_channel();
        let (actions_tx, _actions_rx) = mpsc::channel(16);
        let (routine_to_reactor_tx, _routine_to_reactor_rx) = mpsc::channel(16);
        let (_view_tx, view_rx) = watch::channel(initial_view(BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        }));

        let mut routine = PeerRoutine::new(
            peer,
            session,
            in_recv,
            config,
            0,
            budget,
            Arc::clone(&work),
            Arc::new(PeerRegistry::new()),
            Arc::new(Mutex::new(ThroughputMeter::new(Instant::now()))),
            sequencer_input_tx,
            Arc::new(AtomicU64::new(0)),
            control_tx,
            actions_tx,
            routine_to_reactor_tx,
            view_rx,
            cancel,
            ZakuraTrace::noop(),
        );
        routine.received_status = true;
        routine.servable_low = height;
        routine.servable_high = height;

        let now = Instant::now();
        let liveness_timeout = Duration::from_secs(60);
        routine.window.outstanding.push(OutstandingBlockRange {
            token: 1,
            state: OutstandingRequestState::Retired {
                reason: RetirementReason::FloorWatchdog,
                retired_at: now,
                correlation_deadline: now + liveness_timeout,
            },
            request: BlockRangeRequest {
                start_height: height,
                count: 1,
                anchor_hash: hash,
                estimated_bytes: 0,
                expected_blocks: vec![ExpectedBlock {
                    height,
                    hash,
                    estimated_bytes: 0,
                }],
            },
            queued_at: now,
            deadline: now,
            delivery_snapshot: routine.window.delivery_snapshot(now),
            delivered_bytes: 0,
            received: ReceivedBlockTracker::default(),
            late_reliability_credited: false,
        });

        let no_progress_requests = routine.window.requests_without_block_progress;
        let liveness_deadline = routine.window.block_liveness_deadline;
        routine
            .handle_retired_body(0, height, hash, block, None, None, None)
            .await;

        assert_eq!(
            routine.window.requests_without_block_progress, no_progress_requests,
            "a discarded retired body must not reset no-progress request accounting"
        );
        assert_eq!(
            routine.window.block_liveness_deadline, liveness_deadline,
            "a discarded retired body must not extend or disarm peer liveness"
        );
        assert!(
            !routine.window.has_block_progress(),
            "a discarded retired body must not prove the peer"
        );
        assert_eq!(
            routine.window.retired_len(),
            1,
            "discarding the body must retain its correlation tombstone"
        );

        assert_eq!(
            work.extend([(height, hash, BlockSizeEstimate::Advertised(1_000))]),
            1
        );
        routine.try_fill().await;
        assert!(work.pending_contains(height));
        assert!(
            timeout(Duration::from_millis(20), out_recv.recv())
                .await
                .is_err(),
            "the retained tombstone must block same-peer reissue"
        );

        // The old terminator closes the tombstone. Only then can the normal fill
        // path claim and reissue the same range.
        routine.handle_blocks_done(height).await;
        assert_eq!(routine.window.retired_len(), 0);
        assert!(routine.window.outstanding.is_empty());
        assert!(work.pending_contains(height));
        routine.try_fill().await;
        assert!(
            work.in_flight_contains(height),
            "the normal fill path must claim the range after tombstone removal"
        );
        assert!(
            timeout(Duration::from_millis(100), out_recv.recv())
                .await
                .expect("reissue is sent after the tombstone is removed")
                .is_some(),
            "the same range must become issuable after its old terminator"
        );
        assert_eq!(routine.window.active_len(), 1);
    }

    /// A global apply backlog is not peer-specific evidence, so it cannot excuse
    /// an otherwise silent peer past its liveness deadline.
    #[tokio::test]
    async fn global_apply_backlog_does_not_defer_silent_peer_liveness() {
        let config = ZakuraBlockSyncConfig::default();
        let budget = ByteBudget::new(1_000_000);
        let work = Arc::new(WorkQueue::new(block::Height(0)));

        let cancel = CancellationToken::new();
        let (out_send, _out_recv) = framed_channel(16);
        let (_in_send, in_recv) = framed_channel(16);
        let peer = ZakuraPeerId::new(vec![9u8; 32]).expect("test peer id is within bounds");
        let session = BlockSyncPeerSession::for_test(peer.clone(), out_send, cancel.clone());

        let (sequencer_input_tx, _sequencer_input_rx) = mpsc::channel(16);
        let (control_tx, _control_rx) = mpsc::unbounded_channel();
        let (actions_tx, _actions_rx) = mpsc::channel(16);
        let (routine_to_reactor_tx, _routine_to_reactor_rx) = mpsc::channel(16);
        let mut view = initial_view(BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(100),
            verified_block_hash: block::Hash([0; 32]),
        });
        view.applying_len = 60;
        let (_view_tx, view_rx) = watch::channel(view);

        let mut routine = PeerRoutine::new(
            peer,
            session,
            in_recv,
            config,
            0,
            budget,
            work,
            Arc::new(PeerRegistry::new()),
            Arc::new(Mutex::new(ThroughputMeter::new(Instant::now()))),
            sequencer_input_tx,
            Arc::new(AtomicU64::new(0)),
            control_tx,
            actions_tx,
            routine_to_reactor_tx,
            view_rx,
            cancel,
            ZakuraTrace::noop(),
        );

        let now = Instant::now();
        // An armed deadline that expired 10s ago.
        routine
            .window
            .arm_liveness(now - Duration::from_secs(60), Duration::from_secs(50));

        assert!(
            routine.check_block_liveness(now).is_err(),
            "an expired peer-specific deadline must disconnect despite unrelated applies"
        );
    }
}
