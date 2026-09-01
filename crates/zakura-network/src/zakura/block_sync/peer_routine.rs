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

use std::{collections::BTreeMap, num::NonZeroU64, ops::Range};

use tokio::sync::{futures::Notified, mpsc, watch};
use tokio_util::sync::CancellationToken;

use super::events::RoutineToReactor;
use super::{
    admission::{
        admit, admit_received_body, floor_rescue_high, request_deadline,
        request_priority as classify_priority, AdmissionOutcome, AdmissionSnapshot,
        RequestPriority,
    },
    peer_registry::{hard_outbound_capacity, PeerRegistry},
    pipe::block_sync_guard,
    reactor::tolerated_bytes,
    reorder::BufferedBlockBody,
    request::{BlockRangeRequest, ExpectedBlock},
    sequencer_task::{SequencedBody, SequencerView},
    state::{
        DownloadWindow, LivenessOutcome, OutstandingBlockRange, ReceivedBlockTracker,
        ThroughputMeter,
    },
    work_queue::{WorkItem, WorkQueue, WorkReturnOutcome},
    BlockSyncMessage, BlockSyncMisbehavior, BlockSyncPeerSession, BlockSyncStatus,
    ZakuraBlockSyncConfig, ZakuraPeerId, ZakuraTrace, MSG_BS_BLOCK,
};
use crate::zakura::{
    trace::BlockBodySource, Admit, FramedRecv, OrderedSendError, SinkReject, ZakuraConnId,
};
use std::{sync::Arc, time::Duration, time::Instant};
use tokio::time;
use zakura_chain::{block, serialization::ZcashSerialize};

mod trace;

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

/// Return the first contiguous run that the predicate accepts.
///
/// The function evaluates each visited item once. This property matters when
/// the predicate reads shared retry state that the reactor or sequencer can update.
fn first_allowed_run<T>(
    items: &[T],
    mut is_allowed: impl FnMut(&T) -> bool,
) -> Option<Range<usize>> {
    let mut start = None;

    for (index, item) in items.iter().enumerate() {
        if is_allowed(item) {
            start.get_or_insert(index);
        } else if let Some(start) = start {
            return Some(start..index);
        }
    }

    start.map(|start| start..items.len())
}

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
const PARK_BLOCK_SYNC_NO_BLOCK_PROGRESS: &str = "block_sync_no_block_progress";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NoProgressResponse {
    Park,
    Disconnect,
}

fn no_progress_response(allow_no_progress_park: bool) -> NoProgressResponse {
    if allow_no_progress_park {
        NoProgressResponse::Park
    } else {
        NoProgressResponse::Disconnect
    }
}

/// Whether the routine grants one bounded delay at a block-liveness deadline.
/// The routine grants the delay only for transient outbound write congestion that
/// lasts less than `request_timeout`. A peer that stops reading keeps the outbound
/// queue full. The routine disconnects that peer when the interval reaches
/// `request_timeout`.
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

/// Outcome classification for finishing an outstanding request.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Disposition {
    Satisfied,
    RetryOriginal,
    RetryMissing,
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
    conn_id: ZakuraConnId,
    source: zakura_header_chain::SourceId,
    session: BlockSyncPeerSession,
    config: ZakuraBlockSyncConfig,
    /// A connection gets one local no-progress park/re-admission cycle. A
    /// repeated stall is connection-fatal so it cannot reclaim download slots
    /// indefinitely without paying the redial cost.
    allow_no_progress_park: bool,

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
    /// Last sampled fill-stop time for each bounded reason label.
    fill_stop_trace_at: BTreeMap<&'static str, Instant>,

    // ---- shared primitives (clones) ----
    /// Generation this routine was spawned with; gates its registry writes (and
    /// its `Drop`) so a superseded routine (e.g. a session replacement before the
    /// old task's async Drop runs) cannot corrupt the live entry.
    generation: u64,
    /// Next request identity in this peer-session generation. Exhaustion fails
    /// closed instead of reusing an owner.
    next_request_id: Option<NonZeroU64>,
    budget: super::state::ByteBudget,
    work: Arc<WorkQueue>,
    registry: Arc<PeerRegistry>,
    received_throughput: Arc<std::sync::Mutex<ThroughputMeter>>,
    sequencer_input: mpsc::Sender<SequencedBody>,
    sequencer_input_bytes: Arc<std::sync::atomic::AtomicU64>,
    sequencer_input_decoded_attributed_memory_bytes: Arc<std::sync::atomic::AtomicU64>,
    /// Shared routine-to-reactor channel for serving, status, re-query, and misbehavior events.
    /// Bounded `try_send` prevents a busy reactor from stalling the transport decode loop.
    routine_to_reactor: mpsc::Sender<RoutineToReactor>,
    sequencer_view: watch::Receiver<SequencerView>,
    /// Last `reset_epoch` that this routine processed.
    /// A `view.changed()` event uses the epoch to distinguish a reset from an advance.
    last_reset_epoch: u64,
    /// Start of the current interval in which this peer's outbound queue stayed full.
    /// The liveness check uses the interval to distinguish congestion from a peer that stopped reading.
    outbound_full_since: Option<Instant>,

    /// Cancellation token for the peer's service session.
    /// Disconnect, park, or shutdown triggers the token.
    /// The routine then exits and its `Drop` guard returns work.
    cancel: CancellationToken,
    trace: ZakuraTrace,
}

impl PeerRoutine {
    /// Build a pipe-routine for `peer`. The caller (`service::add_peer`) drives
    /// `run()` inside `spawn_supervised_pipe` so a protocol reject cancels the
    /// whole connection. `generation` is the value obtained from
    /// [`PeerRegistry::admit_session`](super::peer_registry::PeerRegistry::admit_session).
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        peer: ZakuraPeerId,
        conn_id: ZakuraConnId,
        session: BlockSyncPeerSession,
        recv: FramedRecv,
        config: ZakuraBlockSyncConfig,
        allow_no_progress_park: bool,
        generation: u64,
        budget: super::state::ByteBudget,
        work: Arc<WorkQueue>,
        registry: Arc<PeerRegistry>,
        received_throughput: Arc<std::sync::Mutex<ThroughputMeter>>,
        sequencer_input: mpsc::Sender<SequencedBody>,
        sequencer_input_bytes: Arc<std::sync::atomic::AtomicU64>,
        sequencer_input_decoded_attributed_memory_bytes: Arc<std::sync::atomic::AtomicU64>,
        routine_to_reactor: mpsc::Sender<RoutineToReactor>,
        sequencer_view: watch::Receiver<SequencerView>,
        cancel: CancellationToken,
        trace: ZakuraTrace,
    ) -> Self {
        let source_digest: [u8; 32] = peer.as_bytes().try_into().expect(
            "block-sync peers have 32-byte identities because they are authenticated Iroh nodes",
        );
        let source = zakura_header_chain::SourceId::from_digest(source_digest);
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
            conn_id,
            source,
            session,
            config,
            allow_no_progress_park,
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
            fill_stop_trace_at: BTreeMap::new(),
            generation,
            next_request_id: NonZeroU64::new(1),
            budget,
            work,
            registry,
            received_throughput,
            sequencer_input,
            sequencer_input_bytes,
            sequencer_input_decoded_attributed_memory_bytes,
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

            let retry_filter_deadline = if self.session.outbound_capacity() > 0 {
                self.try_fill().await
            } else {
                None
            };
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
            let timeout = self.earliest_deadline_sleep(retry_filter_deadline);
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
                        // Stream closed by the peer. With no outstanding work this
                        // is a clean exit; with unanswered requests it is a
                        // no-progress stall (park or disconnect). `Drop` returns
                        // unreceived outstanding heights and releases their budget.
                        None => return self.handle_remote_stream_closed(Instant::now()),
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
    /// and on a destructive `reset_epoch` bump clear this routine's outstanding
    /// **in place** (return unreceived heights to `work.pending`, release their
    /// budget, clear the registry outstanding, drop retry-avoid) and re-fan from
    /// the post-`reset_above` `WorkQueue`. The transport is never torn down:
    /// reset clears outstanding work in place instead of respawning the routine.
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
        // `reset_above`) and release their reservations exactly once.
        let outstanding = std::mem::take(&mut self.window.outstanding);
        for outstanding in outstanding {
            let unreceived: Vec<_> = unreceived_heights(&outstanding).collect();
            let outcome = self
                .work
                .release_reserved_and_return_items_detailed_for_owner(
                    outstanding.request.owner,
                    unreceived.iter().copied(),
                );
            self.budget.release(outcome.released_bytes);
            self.trace_work_returned("view_reset", &outstanding, unreceived.len(), outcome);
        }
        self.retry_avoid.clear();
        // Clear our (now-empty) registry outstanding and refresh slot diagnostics.
        self.publish_outstanding();
        // A destructive reset pulled this peer's outstanding on our initiative, so its
        // no-progress probe streak must not stay charged: reset it (and clear the idle
        // liveness deadline) so an unproven peer whose only probe was in flight at the
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
    fn earliest_deadline_sleep(&self, retry_filter_deadline: Option<Instant>) -> time::Sleep {
        let now = Instant::now();
        let earliest_deadline = self
            .window
            .outstanding
            .iter()
            .map(|outstanding| outstanding.deadline)
            .min();
        let liveness_deadline = self.window.block_liveness_deadline;
        let local_retry_avoid = self.retry_avoid.values().min().copied();
        let floor_watchdog_avoid = self.registry.next_floor_avoid_deadline(&self.peer, now);
        let body_retry_avoid = self.registry.next_body_retry_deadline(&self.peer, now);
        let earliest = [
            earliest_deadline,
            liveness_deadline,
            local_retry_avoid,
            floor_watchdog_avoid,
            body_retry_avoid,
            retry_filter_deadline,
        ]
        .into_iter()
        .flatten()
        .min();
        match earliest {
            // Floor the wait at the deadline so a far-future request still wakes
            // promptly; an already-due deadline wakes immediately.
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
    async fn try_fill(&mut self) -> Option<Instant> {
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
        self.gc_obsolete_outstanding();
        self.gc_committed_outstanding();
        // Drop expired retry-avoid entries: those heights are contestable by this
        // routine again.
        let now = Instant::now();
        self.retry_avoid.retain(|_, until| *until > now);
        let mut retry_filter_deadline = None;
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
                            items = self.work.take_in_range_budgeted(
                                servable_low,
                                grant.take_high,
                                max_count,
                                grant.max_request_bytes.min(floor_cwnd_cap).max(1),
                            );
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
                        items = self.work.take_in_range_budgeted(
                            servable_low,
                            grant.take_high,
                            max_count,
                            grant.max_request_bytes.min(above_cwnd_cap),
                        );
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
                let is_allowed = |height: &block::Height, item: &WorkItem| {
                    !self.retry_avoid.contains_key(height)
                        && !self
                            .registry
                            .is_floor_height_avoided(&self.peer, *height, now)
                        && !self
                            .registry
                            .is_body_retry_avoided(&self.peer, item.scope, item.hash, now)
                };
                let Some(keep) =
                    first_allowed_run(&items, |(height, item)| is_allowed(height, item))
                else {
                    let avoided: Vec<_> = items.iter().map(|(h, _)| *h).collect();
                    self.work.return_items_quiet(avoided);
                    retry_filter_deadline = Some(self.retry_filter_wake_deadline(now));
                    break FillStop::RetryAvoid;
                };
                let keep_len = keep.len();
                let mut returned_avoided = false;
                if keep.start > 0 {
                    let avoided: Vec<_> = items.drain(..keep.start).map(|(h, _)| h).collect();
                    self.work.return_items_quiet(avoided);
                    returned_avoided = true;
                }
                if keep_len < items.len() {
                    let avoided = items.split_off(keep_len);
                    self.work
                        .return_items_quiet(avoided.into_iter().map(|(height, _)| height));
                    returned_avoided = true;
                }
                if returned_avoided {
                    let deadline = self.retry_filter_wake_deadline(now);
                    retry_filter_deadline = Some(
                        retry_filter_deadline
                            .map_or(deadline, |current: Instant| current.min(deadline)),
                    );
                }
            }
            self.trace_work_taken(servable_low, servable_high, items.len());
            debug_assert!(
                !items.is_empty(),
                "retry filtering must retain a nonempty allowed run"
            );
            let Some((first_height, first_item)) = items.first().copied() else {
                break FillStop::Internal;
            };
            let scope = first_item.scope;
            debug_assert!(items.iter().all(|(_, item)| item.scope == scope));

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
            let request_priority = classify_priority(view.download_floor, first_height);

            let reserved_bytes = items.iter().fold(0u64, |acc, (_, item)| {
                acc.saturating_add(item.estimated_bytes)
            });
            if !self.reserve_request_budget(request_priority, reserved_bytes) {
                self.return_taken_items(&items);
                break FillStop::Budget;
            }
            let Some(request_id) = self.next_request_id else {
                self.budget.release(reserved_bytes);
                self.return_taken_items(&items);
                break FillStop::Internal;
            };
            self.next_request_id = request_id.get().checked_add(1).and_then(NonZeroU64::new);
            let owner = scope.bind(self.generation, request_id);
            let marked = self
                .work
                .mark_reserved_for_owner(owner, items.iter().map(|(height, _)| *height));
            if marked != reserved_bytes {
                self.budget.release(reserved_bytes);
                let _ = self
                    .work
                    .release_reserved_and_return_items_detailed_for_owner(
                        owner,
                        items.iter().map(|(height, _)| *height),
                    );
                break FillStop::Internal;
            }

            let count = match u32::try_from(kept_count) {
                Ok(count) => count,
                Err(_) => {
                    let released = self
                        .work
                        .release_reserved_and_return_items_detailed_for_owner(
                            owner,
                            items.iter().map(|(height, _)| *height),
                        );
                    self.budget.release(released.released_bytes);
                    break FillStop::Internal;
                }
            };
            let request = BlockRangeRequest {
                owner,
                start_height: first_height,
                count,
                anchor_hash: first_item.hash,
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
                // Return every still-reserved height to the queue. A competing
                // peer's late body may have claimed a taken height and released its
                // request reservation during the reserve await; leave that height
                // in flight rather than re-queueing or releasing it twice.
                let released = self
                    .work
                    .release_reserved_and_return_items_detailed_for_owner(
                        request.owner,
                        items.iter().map(|(height, _)| *height),
                    );
                self.budget.release(released.released_bytes);
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
            self.window.outstanding.push(OutstandingBlockRange {
                request,
                queued_at,
                deadline,
                delivery_snapshot: self.window.delivery_snapshot(queued_at),
                delivered_bytes: 0,
                received: ReceivedBlockTracker::default(),
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
        retry_filter_deadline
    }

    /// Capture the retry deadline against the same time snapshot that rejected
    /// the work. If shared state changed after filtering, retry immediately.
    fn retry_filter_wake_deadline(&self, now: Instant) -> Instant {
        let local = self.retry_avoid.values().min().copied();
        let floor = self.registry.next_floor_avoid_deadline(&self.peer, now);
        let body = self.registry.next_body_retry_deadline(&self.peer, now);
        [local, floor, body]
            .into_iter()
            .flatten()
            .min()
            .unwrap_or(now)
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
            in_flight_submission_bytes: view.in_flight_submission_bytes,
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

    fn reserve_request_budget(&mut self, priority: RequestPriority, reserved_bytes: u64) -> bool {
        if self.budget.try_reserve(reserved_bytes) {
            return true;
        }
        if priority == RequestPriority::Floor {
            // The WorkQueue owns each height once, so there can only be one
            // floor-priority overdraft globally. Its charge is released by the
            // normal reservation paths: receipt, timeout, watchdog, or reset.
            self.budget.charge(reserved_bytes);
            metrics::counter!("sync.block.budget.floor_overdraft").increment(1);
            return true;
        }
        false
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
    fn return_taken_items(&self, items: &[(block::Height, WorkItem)]) {
        self.work
            .return_items_quiet(items.iter().map(|(height, _)| *height));
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

    // ===================== own-timeout arm (ports `expire_due_timeouts`) =====

    async fn handle_deadlines(&mut self, now: Instant) -> Result<(), SinkReject> {
        let rescued_timed_out = self.expire_due_timeouts(now);
        if rescued_timed_out && self.session.outbound_capacity() > 0 {
            let _ = self.try_fill().await;
        }
        self.check_block_liveness(now)
    }

    fn expire_due_timeouts(&mut self, now: Instant) -> bool {
        let mut timed_out = Vec::new();
        let mut index = 0;
        while index < self.window.outstanding.len() {
            if self.window.outstanding[index].deadline <= now {
                timed_out.push(self.window.outstanding.remove(index));
            } else {
                index += 1;
            }
        }
        if timed_out.is_empty() {
            return false;
        }
        self.window.record_timeout(timed_out.len());
        for outstanding in &timed_out {
            // Return only the unreceived heights — received ones are buffered (in
            // `in_flight` until committed); re-queuing them would re-fetch a body
            // we already hold (the WorkQueue single-owner invariant forbids it).
            let unreceived: Vec<_> = unreceived_heights(outstanding).collect();
            let outcome = self
                .work
                .release_reserved_and_return_items_detailed_for_owner(
                    outstanding.request.owner,
                    unreceived.iter().copied(),
                );
            self.budget.release(outcome.released_bytes);
            self.trace_work_returned("request_timeout", outstanding, unreceived.len(), outcome);
        }
        // Bias away from immediately re-grabbing the heights this peer just timed
        // out, so another peer can contest them (the peer-local timeout bias).
        let timed_out_heights: Vec<_> = timed_out.iter().flat_map(unreceived_heights).collect();
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
            LivenessOutcome::Park
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
                // may be waiting behind our write side. Grant one short, BOUNDED grace. This
                // is the *only* liveness extension: a peer that stopped reading holds outbound
                // full past `request_timeout`, falls through to the park arm, and is
                // parked at the liveness deadline — it cannot dodge the timer.
                self.window
                    .extend_liveness_deadline(now, self.config.request_timeout);
                Ok(())
            }
            LivenessOutcome::Park => self.no_progress_stall(
                now,
                "block-sync peer made no accepted block progress before liveness deadline",
            ),
        }
    }

    /// Apply the no-progress stall protocol: disconnect on a repeated stall,
    /// otherwise park the session for the cooldown and exit locally (the
    /// connection survives).
    fn no_progress_stall(&mut self, now: Instant, error: &'static str) -> Result<(), SinkReject> {
        if no_progress_response(self.allow_no_progress_park) == NoProgressResponse::Disconnect {
            tracing::debug!(
                peer = ?self.peer,
                outstanding = self.window.outstanding.len(),
                "disconnecting Zakura block-sync peer after repeated no-progress stall"
            );
            return Err(SinkReject::protocol(error));
        }
        self.registry.park_session(
            &self.peer,
            self.conn_id,
            self.generation,
            now + self.config.effective_no_progress_peer_cooldown(),
        );
        self.trace_liveness_park(error);
        tracing::debug!(
            peer = ?self.peer,
            outstanding = self.window.outstanding.len(),
            "parking Zakura block-sync session after no accepted block progress"
        );
        Err(SinkReject::local(error))
    }

    /// Handle the peer closing its send side of the stream. Honest peers close
    /// *connections*, not lone streams; a stream-only EOF while block-progress
    /// liveness is still armed is the same signal as the liveness stall and must
    /// not reset the park/second-stall state machine — otherwise a peer could
    /// take work, deliver nothing, let its requests time out (which drains
    /// `outstanding` without disarming liveness), EOF before the liveness
    /// deadline, and be readmitted fresh forever. An armed deadline with empty
    /// `outstanding` can only mean charged requests were consumed without
    /// accepted progress — every answered-everything path disarms it — and
    /// `DownloadWindow::check_liveness` parks at that deadline
    /// regardless of `outstanding`, so parking here only moves the already
    /// scheduled outcome earlier. Frames are processed in-order in this task, so
    /// at EOF everything the peer sent has already been counted. The liveness
    /// grace does not apply: it waits for in-flight frames stuck behind our full
    /// outbound queue, and a closed stream has none.
    fn handle_remote_stream_closed(&mut self, now: Instant) -> Result<(), SinkReject> {
        if self.window.outstanding.is_empty() && self.window.block_liveness_deadline.is_none() {
            return Ok(());
        }
        self.no_progress_stall(
            now,
            "block-sync peer closed the stream with its requests unanswered",
        )
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
        let mut released = 0u64;
        let mut removed = false;
        let mut index = 0;
        while index < self.window.outstanding.len() {
            if self.window.outstanding[index].request.end_height() <= floor {
                let outstanding = self.window.outstanding.remove(index);
                // Release only estimates whose per-height ledger is still
                // `Reserved`. A competing delivery changes that ledger to
                // `Released` at receipt, so floor GC must not release it again.
                released = released.saturating_add(self.work.release_reserved_heights_for_owner(
                    outstanding.request.owner,
                    unreceived_heights(&outstanding),
                ));
                removed = true;
            } else {
                index += 1;
            }
        }
        if released > 0 {
            self.budget.release(released);
        }
        if removed {
            self.publish_outstanding();
            self.window.disarm_liveness_after_progress_if_idle();
        }
    }

    /// Free request slots after the central queue retires their exact owners.
    /// Queue retirement already released their reservations.
    /// The cleanup path drops only routine-local and registry bookkeeping.
    fn gc_obsolete_outstanding(&mut self) {
        let mut removed = false;
        let mut index = 0;
        while index < self.window.outstanding.len() {
            let outstanding = &self.window.outstanding[index];
            let owner = outstanding.request.owner;
            let still_owned = outstanding
                .request
                .expected_blocks
                .iter()
                .filter(|expected| !outstanding.has_received(expected.height))
                .any(|expected| self.work.owner_for_height(expected.height) == Some(owner));
            if still_owned {
                index += 1;
            } else {
                self.window.outstanding.remove(index);
                removed = true;
            }
        }
        if removed {
            self.publish_outstanding();
            self.window.disarm_liveness_after_progress_if_idle();
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
        let outstanding = &self.window.outstanding[index];
        let delivery_snapshot = outstanding.delivery_snapshot;
        if outstanding.has_received(height) {
            tracing::debug!(peer = ?self.peer, ?height, "ignoring duplicate block-sync body frame");
            return;
        }
        match outstanding.request.expected_hash(height) {
            Some(requested) if requested != hash => {
                let mismatch = header_hash_payload_mismatch(
                    outstanding.request.owner,
                    self.source,
                    requested,
                    hash,
                );
                self.report_misbehavior(BlockSyncMisbehavior::BodyPayloadMismatch(mismatch))
                    .await;
                return;
            }
            Some(_) => {}
            None => {
                self.report_misbehavior(BlockSyncMisbehavior::InvalidBlock)
                    .await;
                return;
            }
        }
        self.trace
            .record_block_body_received(hash, BlockBodySource::Zakura);
        let outstanding_owner = outstanding.request.owner;
        if self.work.owner_for_height(height) != Some(outstanding_owner) {
            metrics::counter!("sync.block.stale_completion.total", "kind" => "body_range")
                .increment(1);
            self.drop_obsolete_outstanding(index);
            return;
        }
        if !self
            .registry
            .peer_has_outstanding_height(&self.peer, height)
        {
            tracing::debug!(
                peer = ?self.peer,
                ?height,
                "ignoring late block-sync body for a claim cancelled by the floor watchdog"
            );
            self.finish_outstanding_at(index, Disposition::RetryMissing);
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
        // End the request reservation at receipt, but release its bytes only
        // after the body is visible to the resident-memory accounting.
        let Some(reserved_estimate) = self
            .work
            .release_active_reserved_height_for_owner(outstanding_owner, height)
        else {
            tracing::debug!(
                peer = ?self.peer,
                ?height,
                serialized_bytes,
                "block-sync body already settled by another peer; marking received"
            );
            self.accept_already_settled_height(index, height);
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

        self.window
            .note_block_progress(Instant::now(), self.config.effective_liveness_timeout());
        let mut completed = None;
        if let Some(outstanding) = self.window.outstanding.get_mut(index) {
            outstanding.record_body_bytes(serialized_bytes);
            outstanding.mark_received(height);
            if outstanding.is_complete() {
                completed = Some(self.window.outstanding.remove(index));
            }
        }
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
        if let Some(outstanding) = completed {
            self.finish_detached(outstanding, Disposition::Satisfied);
        } else {
            self.publish_outstanding();
        }

        // Forward the body to the commit-pipeline task. THE ONLY blocking send in
        // the routine: a slow verifier blocks the task draining input, the bounded
        // input channel fills, and this routine blocks here — backpressure
        // isolated to this peer (the per-peer routines throughput win).
        let previous_block_hash = block.header.previous_block_hash;
        let body = BufferedBlockBody::from_measured_decoded_block(
            block,
            raw_block_payload,
            decoded_attributed_memory_size_bytes,
        );
        self.forward_body_to_sequencer(
            outstanding_owner,
            height,
            hash,
            previous_block_hash,
            body,
            serialized_bytes,
            body_permit,
        )
        .await;
        self.budget.release(reserved_estimate);
        // This body opened only this peer's slots; the want-work loop runs at the
        // top of the next iteration.
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

    #[allow(clippy::too_many_arguments)]
    async fn forward_body_to_sequencer(
        &self,
        owner: zakura_header_chain::BodyWorkOwner,
        height: block::Height,
        hash: block::Hash,
        previous_block_hash: block::Hash,
        body: BufferedBlockBody,
        serialized_bytes: u64,
        body_permit: Option<mpsc::OwnedPermit<SequencedBody>>,
    ) {
        let received_at = Instant::now();
        let sequencer_send_started = Instant::now();
        let body = SequencedBody::new_queued(
            owner,
            self.source,
            height,
            hash,
            previous_block_hash,
            body,
            serialized_bytes,
            self.peer.clone(),
            received_at,
            self.sequencer_input_bytes.clone(),
            self.sequencer_input_decoded_attributed_memory_bytes.clone(),
        );

        let ok = if let Some(permit) = body_permit {
            permit.send(body);
            true
        } else {
            let send_result = self.sequencer_input.send(body).await;
            send_result.is_ok()
        };

        self.trace_body_sequencer_sent(height, sequencer_send_started.elapsed(), ok);
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
        body_permit: Option<mpsc::OwnedPermit<SequencedBody>>,
        raw_block_payload: Option<Arc<[u8]>>,
    ) -> bool {
        if self.work.hash_for_height(height) != Some(hash) {
            return false;
        }
        if !self.received_status || height < self.servable_low || height > self.servable_high {
            return false;
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
                    return true;
                }
            },
        };

        let reserved_in_flight = self.work.reserved_in_flight_charge(height);
        let is_pending = self.work.pending_contains(height);
        if reserved_in_flight.is_none() && !is_pending {
            return false;
        }
        self.trace
            .record_block_body_received(hash, BlockBodySource::Zakura);
        let Some(owner) = self.work.owner_for_height(height) else {
            // Pending work has no active request owner.
            // Accepting a body here would create an unowned completion.
            // Leave it to the unsolicited or stale classification path.
            return false;
        };

        // The reservation this arrival ended (an active competing request, or a
        // stale charge on the claimed height); released after the forward below.
        let ended_reservation = if is_pending {
            let sequencer_view = *self.sequencer_view.borrow();
            let snapshot = self.admission_snapshot(&sequencer_view);
            if !admit_received_body(&self.config, &snapshot, height, serialized_bytes) {
                tracing::debug!(
                    peer = ?self.peer,
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
            let _ = self.work.take_in_range(height, height, 1);
            metrics::counter!("sync.block.response.unmatched_queued_accepted").increment(1);
            self.work.claim_received_for_owner(owner, height)
        } else {
            // First-completion-wins for a timed-out height already re-issued to
            // another peer: this arrival ends that request's reservation instead of
            // discarding a valid body because another peer currently owns the
            // request slot.
            let Some(estimate) = self
                .work
                .release_active_reserved_height_for_owner(owner, height)
            else {
                return false;
            };
            metrics::counter!("sync.block.response.unmatched_active_accepted").increment(1);
            estimate
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
        self.window
            .note_block_progress(Instant::now(), self.config.effective_liveness_timeout());
        // Also credit the reliability EWMA: this late body offsets the failure its own
        // timeout charged, so a peer that merely slowed down (backlog draining past the
        // per-request deadline but every body still arriving) keeps a reduced-but-nonzero
        // window instead of being sealed to zero like a genuine dropper — which sends no
        // late body to credit. This is the slow-vs-wedged distinction the seal relies on.
        self.window.credit_late_delivery();

        let previous_block_hash = block.header.previous_block_hash;
        let body = BufferedBlockBody::from_measured_decoded_block(
            block,
            raw_block_payload,
            decoded_attributed_memory_size_bytes,
        );
        self.forward_body_to_sequencer(
            owner,
            height,
            hash,
            previous_block_hash,
            body,
            serialized_bytes,
            body_permit,
        )
        .await;
        // Release the ended reservation only now that the body is counted in
        // `sequencer_input_bytes`, mirroring the matched receipt path above, so
        // the bytes are never invisible to both the limiter and the resident
        // snapshot.
        self.budget.release(ended_reservation);
        true
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
        let Some(outstanding) = self.window.outstanding.get_mut(index) else {
            return current;
        };
        if outstanding.request.start_height > tip {
            return current;
        }
        let released_heights: Vec<_> = outstanding_unreceived_through(outstanding, tip).collect();
        let _ = outstanding.mark_received_through(tip);
        let released_bytes = self
            .work
            .release_reserved_heights_for_owner(outstanding.request.owner, released_heights);
        self.budget.release(released_bytes);
        if outstanding.is_complete() {
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

    /// Drop a local request after the central queue retires its work scope.
    /// The central queue already released the reservation.
    /// The cleanup path preserves any replacement item at the same height.
    fn drop_obsolete_outstanding(&mut self, index: usize) {
        if index >= self.window.outstanding.len() {
            return;
        }
        self.window.outstanding.remove(index);
        self.publish_outstanding();
        self.window.disarm_liveness_after_progress_if_idle();
    }

    fn finish_detached(&mut self, outstanding: OutstandingBlockRange, disposition: Disposition) {
        match disposition {
            Disposition::Satisfied => {
                // Every requested height was received and buffered; nothing
                // returns to the queue (buffered heights stay in `in_flight`
                // until the floor commits past them). Release any residual
                // reserved estimate (normally none once complete).
                let released = self.work.release_reserved_heights_for_owner(
                    outstanding.request.owner,
                    unreceived_heights(&outstanding),
                );
                self.budget.release(released);
            }
            // With fanout = 1 a received height is already buffered and must never
            // be re-fetched, so both retry dispositions return only the still-reserved
            // unreceived heights to `pending`. `return_items` is idempotent.
            Disposition::RetryOriginal | Disposition::RetryMissing => {
                let unreceived: Vec<_> = unreceived_heights(&outstanding).collect();
                let outcome = self
                    .work
                    .release_reserved_and_return_items_detailed_for_owner(
                        outstanding.request.owner,
                        unreceived.iter().copied(),
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
        if disposition == Disposition::Satisfied {
            self.window.disarm_liveness_after_progress_if_idle();
        }
    }

    /// A body arrived for a request this peer owns, but its per-height ledger was
    /// already `Released` by a competing receipt, watchdog, or floor GC, so
    /// `release_active_reserved_height` returned `None`. Record the height as
    /// received so the request can complete without re-queueing it or touching
    /// the budget again. Count it as block progress since a real wanted body did
    /// arrive on this peer's stream.
    fn accept_already_settled_height(&mut self, index: usize, height: block::Height) {
        self.window
            .note_block_progress(Instant::now(), self.config.effective_liveness_timeout());
        let completed = self
            .window
            .outstanding
            .get_mut(index)
            .map(|outstanding| {
                outstanding.mark_received(height);
                outstanding.is_complete()
            })
            .unwrap_or(false);
        if completed {
            self.finish_outstanding_at(index, Disposition::Satisfied);
        } else {
            self.publish_outstanding();
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
        for outstanding in &self.window.outstanding {
            for expected in &outstanding.request.expected_blocks {
                if !outstanding.has_received(expected.height) {
                    map.insert(
                        expected.height,
                        super::peer_registry::OutstandingMeta {
                            owner: outstanding.request.owner,
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
                outstanding_requests: self.window.outstanding.len(),
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
        // `Misbehavior` is best-effort: never block the routine. The reactor owns
        // action dispatch so attacker-triggered reports cannot bypass its reserved
        // control capacity.
        let _ = self
            .routine_to_reactor
            .try_send(RoutineToReactor::Misbehavior {
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
}

fn elapsed_ms_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
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
        let outstanding_ranges = std::mem::take(&mut self.window.outstanding);
        for outstanding in outstanding_ranges {
            let unreceived: Vec<_> = outstanding
                .request
                .expected_blocks
                .iter()
                .filter(|expected| !outstanding.has_received(expected.height))
                .map(|expected| expected.height)
                .collect();
            let outcome = self
                .work
                .release_reserved_and_return_items_detailed_for_owner(
                    outstanding.request.owner,
                    unreceived.iter().copied(),
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
    use zakura_chain::block;

    use super::super::peer_registry::PeerRegistry;
    use super::super::request::BlockSizeEstimate;
    use super::super::sequencer_task::initial_view;
    use super::super::state::{ByteBudget, ThroughputMeter};
    use super::super::work_queue::WorkQueue;
    use super::super::{BlockSyncFrontiers, BlockSyncPeerSession, ZakuraBlockSyncConfig};
    use super::PeerRoutine;
    use crate::zakura::framed_channel;
    use crate::zakura::trace::ZakuraTrace;
    use crate::zakura::ZakuraPeerId;

    fn reference_first_allowed_run(allowed: &[bool]) -> Option<std::ops::Range<usize>> {
        let start = allowed.iter().position(|allowed| *allowed)?;
        let len = allowed[start..]
            .iter()
            .take_while(|allowed| **allowed)
            .count();
        Some(start..start + len)
    }

    #[test]
    fn retry_filter_retains_each_small_allowed_run() {
        for len in 0..=6 {
            for mask in 0..(1usize << len) {
                let allowed: Vec<_> = (0..len)
                    .map(|index| mask & (1usize << index) != 0)
                    .collect();
                let expected = reference_first_allowed_run(&allowed);
                let items: Vec<_> = (0..len).collect();
                let mut calls = vec![0usize; len];

                let keep = super::first_allowed_run(&items, |item| {
                    calls[*item] += 1;
                    allowed[*item]
                });

                assert_eq!(keep, expected, "len={len}, mask={mask:#08b}");

                let visited_len = match &expected {
                    Some(range) if range.end < len => range.end + 1,
                    Some(range) => range.end,
                    None => len,
                };
                for (index, calls) in calls.into_iter().enumerate() {
                    assert_eq!(
                        calls,
                        usize::from(index < visited_len),
                        "len={len}, mask={mask:#08b}, index={index}"
                    );
                }

                let mut retained = items;
                let mut returned = Vec::new();
                match keep {
                    Some(keep) => {
                        let keep_len = keep.len();
                        if keep.start > 0 {
                            returned.extend(retained.drain(..keep.start));
                        }
                        if keep_len < retained.len() {
                            returned.extend(retained.split_off(keep_len));
                        }
                    }
                    None => {
                        returned.extend(retained.iter().copied());
                        retained.clear();
                    }
                }

                let expected_retained: Vec<_> = expected.clone().into_iter().flatten().collect();
                let expected_returned: Vec<_> = (0..len)
                    .filter(|index| !expected.as_ref().is_some_and(|range| range.contains(index)))
                    .collect();
                assert_eq!(retained, expected_retained, "len={len}, mask={mask:#08b}");
                assert_eq!(returned, expected_returned, "len={len}, mask={mask:#08b}");
                assert_eq!(
                    retained.is_empty(),
                    expected.is_none(),
                    "len={len}, mask={mask:#08b}"
                );
            }
        }
    }

    #[tokio::test]
    async fn retry_filter_carries_the_checked_deadline_to_the_waiter() {
        let config = ZakuraBlockSyncConfig::default();
        let budget = ByteBudget::new(1_000_000);
        let work = Arc::new(WorkQueue::new(block::Height(0)));
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
        let (_view_tx, view_rx) = watch::channel(initial_view(BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        }));
        let mut routine = PeerRoutine::new(
            peer,
            0,
            session,
            in_recv,
            config,
            true,
            0,
            budget,
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
        routine.received_status = true;
        routine.servable_low = block::Height(1);
        routine.servable_high = block::Height(10);

        assert_eq!(routine.try_fill().await, Some(until));
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
        let config = ZakuraBlockSyncConfig::default();

        // A byte budget reserved down to exactly zero free: the case that used to wedge.
        let mut budget = ByteBudget::new(8_192);
        assert!(budget.try_reserve(8_192));
        assert_eq!(budget.available(), 0, "the budget is exactly full");

        // The floor height (1) is pending and servable by this peer; the download floor
        // is 0 so height 1 is the floor.
        let work = Arc::new(WorkQueue::new(block::Height(0)));
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
        let (_view_tx, view_rx) = watch::channel(initial_view(BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        }));

        let mut routine = PeerRoutine::new(
            peer,
            0,
            session,
            in_recv,
            config,
            true,
            0,
            budget.clone(),
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
        routine.received_status = true;
        routine.servable_low = block::Height(1);
        routine.servable_high = block::Height(10);

        let _ = routine.try_fill().await;

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

    /// Routine teardown must not release or requeue a height already received
    /// through first-completion-wins.
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
        let (_view_tx, view_rx) = watch::channel(initial_view(BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        }));

        let mut routine = PeerRoutine::new(
            peer,
            0,
            session,
            in_recv,
            config,
            true,
            0,
            budget,
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
        routine.received_status = true;
        routine.servable_low = block::Height(1);
        routine.servable_high = block::Height(10);

        // One fill pass: the routine reserves height 1's estimate and sends its
        // request, creating an outstanding claim for a still-reserved height.
        let _ = timeout(Duration::from_secs(5), routine.try_fill())
            .await
            .expect("try_fill completes");
        assert!(
            work.in_flight_contains(block::Height(1)),
            "height 1 is reserved and outstanding after the fill"
        );
        assert!(!work.pending_contains(block::Height(1)));
        assert_eq!(budget_probe.reserved(), 1_000);
        assert_eq!(routine.window.outstanding.len(), 1);

        // A competing peer delivers height 1 first: its receipt ends the shared
        // request reservation. The winner releases the estimate to the ByteBudget
        // only after its forward, so it is still charged here.
        let estimate = work
            .release_active_reserved_height(block::Height(1))
            .expect("height 1 still owns its active reservation");
        assert_eq!(estimate, 1_000);
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
    }

    /// The liveness grace is granted only for genuinely-transient local write congestion:
    /// outbound full but full for *less* than `request_timeout`.
    #[test]
    fn liveness_grace_only_for_fresh_outbound_backpressure() {
        let now = Instant::now();
        let request_timeout = Duration::from_secs(8);

        // Grant a delay when the outbound queue filled one second ago.
        let fresh = now - Duration::from_secs(1);
        assert!(super::liveness_grace_allowed(
            true,
            Some(fresh),
            now,
            request_timeout
        ));

        // Disconnect when the outbound queue stays full for `request_timeout`.
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

        // Disconnect normally when the outbound queue has capacity.
        assert!(!super::liveness_grace_allowed(
            false,
            Some(fresh),
            now,
            request_timeout
        ));
        // Refuse a delay when a full queue has no recorded start time.
        assert!(!super::liveness_grace_allowed(
            true,
            None,
            now,
            request_timeout
        ));
    }

    #[test]
    fn repeated_no_progress_stall_disconnects_instead_of_parking_again() {
        assert_eq!(
            super::no_progress_response(true),
            super::NoProgressResponse::Park
        );
        assert_eq!(
            super::no_progress_response(false),
            super::NoProgressResponse::Disconnect
        );
    }
}
