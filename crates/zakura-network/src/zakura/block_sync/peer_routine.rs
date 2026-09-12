//! Per-peer pipe-routine for Zakura block sync.
//!
//! A per-peer routine inverts the inbound data flow. One task owns each connected
//! peer's `FramedRecv`. The task decodes each stream-6 frame and runs the download
//! logic directly. The reactor does not demultiplex inbound frames or create a
//! per-peer `PeerInput` channel. The routine sends only shared concerns to the
//! reactor through [`RoutineToReactor`]. These concerns include
//! status advertisements, producer re-query pings, and
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
    peer_registry::{hard_outbound_capacity, OutstandingMeta, PeerRegistry},
    pipe::block_sync_guard,
    reorder::BufferedBlockBody,
    request::{BlockRangeRequest, ExpectedBlock},
    sequencer_task::{SequencedBody, SequencerView},
    state::{
        DownloadWindow, LivenessOutcome, OutstandingBlockRange, ReceivedBlockTracker,
        ThroughputMeter,
    },
    work_queue::{RequestWrite, WorkItem, WorkQueue, WorkReturnOutcome},
    BlockSyncMessage, BlockSyncMisbehavior, BlockSyncPeerSession, BlockSyncStatus,
    ZakuraBlockSyncConfig, ZakuraPeerId, ZakuraTrace, MSG_BS_BLOCK,
};
use crate::zakura::regulation::{
    collection_allocation_bytes, ResponseAdmissionError, ResponseAuthorization, ResponseCredit,
    ResponseVec,
};
use crate::zakura::transport::OrderedStreamFailure;
use crate::zakura::{trace::BlockBodySource, Admit, FramedRecv, SinkReject, ZakuraConnId};
use std::{sync::Arc, time::Duration, time::Instant};
use tokio::time;
use zakura_chain::{
    block,
    serialization::{ZcashDeserialize, ZcashSerialize},
};

mod trace;

#[cfg(test)]
mod compliance;

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
    ResponseMemory,
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
            FillStop::ResponseMemory => "response_metadata",
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
    #[cfg(test)]
    decode_probe: Option<Arc<zakura_test::execution::ExecutionProbe>>,

    // ---- per-peer download state (moved out of `PeerBlockState`) ----
    window: DownloadWindow,
    /// Funded scratch storage swapped with the registry's published height index.
    outstanding_snapshot: ResponseVec<(block::Height, OutstandingMeta)>,
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
    response_memory_waiting: bool,
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
    /// Shared status, re-query, and misbehavior notifications use `try_send`.
    routine_to_reactor: mpsc::Sender<RoutineToReactor>,
    /// Current download frontiers and reset authority.
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
            #[cfg(test)]
            decode_probe: None,
            window,
            outstanding_snapshot: ResponseVec::new(),
            received_status: false,
            servable_low: block::Height::MIN,
            servable_high: block::Height::MIN,
            max_blocks_per_response,
            response_memory_waiting: false,
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
        let result = self.run_download().await;
        match &result {
            Err(SinkReject::Protocol(_)) => {
                self.session.close_connection("service_protocol_reject")
            }
            Err(SinkReject::Connection(_)) => self
                .session
                .close_connection("service_local_connection_close"),
            _ => {}
        }
        result
    }

    async fn run_download(&mut self) -> Result<(), SinkReject> {
        let mut guard = block_sync_guard();
        let result = self.run_inner(&mut guard).await;
        // A transport failure can cancel the session before its queued responses
        // reach us. Validate them under the existing decode-capacity bound before
        // scoring unanswered work. Connection shutdown can still stop this drain.
        if result.is_ok() {
            if let Some(failure) = self.recv.failure() {
                self.recv.close();
                while let Ok(frame) = self.recv.try_recv() {
                    self.handle_frame(&mut guard, frame).await?;
                }
                return self.handle_stream_failure(Instant::now(), failure);
            }
        }
        if !matches!(result, Err(SinkReject::Protocol(_))) && self.has_started_responses() {
            return Err(SinkReject::local_connection(
                "unfinished responses cannot outlive their receiver",
            ));
        }
        result
    }

    fn has_started_responses(&self) -> bool {
        self.window.outstanding.iter().any(|range| {
            range.write_status.expire_unwritten();
            !range.write_status.was_skipped()
        })
    }

    async fn run_inner(
        &mut self,
        guard: &mut crate::zakura::SessionGuard,
    ) -> Result<(), SinkReject> {
        // Local clones so the `Notified` futures below borrow these handles, not
        // `self` — `self.try_fill()` needs `&mut self` while the notifications are
        // pinned. The clones share the same underlying `Arc`, so the wakes still
        // fire for releases/extends done through the routine's own `self.budget` /
        // `self.work`.
        let budget = self.budget.clone();
        let work = self.work.clone();
        let response_memory = self.session.response_memory();
        // Per-peer BBR heartbeat cadence. `Skip` so a routine busy past a tick emits one
        // fresh sample rather than a catch-up burst. Observability only.
        let mut bbr_trace_ticks = time::interval(BBR_TRACE_INTERVAL);
        bbr_trace_ticks.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        loop {
            if self.cancel.is_cancelled() {
                return Ok(());
            }
            // missed-wake safety: register both `Notify`s via
            // `Notified::enable()` BEFORE the fill attempt. The budget/work
            // `Notify`s use `notify_waiters` (no stored permit), so a
            // release/extend that lands between the fill-check and the await
            // would be lost if we registered after — the routine would stall.
            let capacity = budget.subscribe_capacity().notified();
            let available = work.subscribe_available().notified();
            let response_capacity = response_memory.subscribe_capacity().notified();
            tokio::pin!(capacity);
            tokio::pin!(available);
            tokio::pin!(response_capacity);
            Notified::enable(capacity.as_mut());
            Notified::enable(available.as_mut());
            Notified::enable(response_capacity.as_mut());

            let retry_filter_deadline = if self.session.outbound_capacity() > 0 {
                self.try_fill().await
            } else {
                self.gc_skipped_outstanding();
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
                frame = self.recv.recv() => {
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
                            self.recv.failure().unwrap_or(OrderedStreamFailure::RemoteClose),
                        ),
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
                _ = &mut response_capacity, if self.response_memory_waiting => {
                    self.trace_wake("response_metadata_capacity");
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

        let frame_payload_bytes = frame.payload.len();
        let frame_type = match BlockSyncMessage::checked_frame_type(&frame) {
            Ok(kind) => kind,
            Err(error) => {
                self.report_misbehavior(BlockSyncMisbehavior::MalformedMessage)
                    .await;
                return Err(SinkReject::protocol(error));
            }
        };
        let body_permit = if frame_type == MSG_BS_BLOCK {
            if self.window.outstanding.is_empty() {
                self.report_misbehavior(BlockSyncMisbehavior::UnsolicitedBlock)
                    .await;
                return Err(SinkReject::protocol("Block has no response authorization"));
            }
            let header = match block::Header::zcash_deserialize_from_slice(&mut &frame.payload[1..])
            {
                Ok(header) => header,
                Err(error) => {
                    self.report_misbehavior(BlockSyncMisbehavior::MalformedMessage)
                        .await;
                    return Err(SinkReject::protocol(error));
                }
            };
            let index = match self.response_index(header.hash()) {
                Ok(index) => index,
                Err(error) => {
                    self.report_misbehavior(BlockSyncMisbehavior::UnsolicitedBlock)
                        .await;
                    return Err(error);
                }
            };
            let bytes = u64::try_from(frame.payload.len() - 1).expect("frame length fits u64");
            if let Err(error) = self.window.outstanding[index].response.check(1, bytes) {
                self.report_misbehavior(BlockSyncMisbehavior::MalformedMessage)
                    .await;
                return Err(SinkReject::protocol(error));
            }
            let permit = self.reserve_body_decode_permit();
            tokio::pin!(permit);
            Some(tokio::select! {
                biased;
                () = self.cancel.cancelled() => {
                    if self.recv.failure().is_none() {
                        return Ok(());
                    }
                    // A remote close cannot excuse an unvalidated body. Keep
                    // this bounded raw frame until its decode slot is available.
                    permit.await?
                }
                permit = &mut permit => permit?,
            })
        } else {
            None
        };
        // Measured here, on the per-peer task, so the body size never has to be
        // recomputed by re-serializing the block on another thread (A1).
        #[cfg(test)]
        let decoded = if let Some(probe) = &self.decode_probe {
            let (decoded, allocations) = zakura_test::allocations::measure(|| {
                BlockSyncMessage::decode_frame_with_raw_block_payload(frame)
            });
            probe.allocations(allocations);
            decoded
        } else {
            BlockSyncMessage::decode_frame_with_raw_block_payload(frame)
        };
        #[cfg(not(test))]
        let decoded = BlockSyncMessage::decode_frame_with_raw_block_payload(frame);
        let (msg, raw_block_payload) = match decoded {
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
            BlockSyncMessage::GetBlocks { .. } => {
                return Err(SinkReject::protocol(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "GetBlocks belongs on the request stream",
                )));
            }
            BlockSyncMessage::Block(block) => {
                self.trace_wake("own_body");
                self.handle_body(block, body_wire_bytes, body_permit, raw_block_payload)
                    .await?;
            }
            BlockSyncMessage::BlocksDone {
                start_height,
                returned,
            } => self.handle_blocks_done(start_height, returned).await?,
            BlockSyncMessage::RangeUnavailable {
                start_height,
                count,
            } => self.handle_range_unavailable(start_height, count).await?,
        }
        Ok(())
    }

    /// Return only unreceived work still owned by this routine. A body already
    /// handed to the sequencer keeps its ownership and cannot be requeued here.
    fn return_unreceived_requests(&mut self, reason: &'static str) {
        for index in (0..self.window.outstanding.len()).rev() {
            self.detach_local_work(index, reason);
        }
        self.registry.clear_outstanding(&self.peer, self.generation);
    }

    /// End local ownership once. Only a proven skipped write can erase authorization.
    fn detach_local_work(&mut self, index: usize, reason: &'static str) {
        let outstanding = &mut self.window.outstanding[index];
        if !outstanding.local_work_active {
            return;
        }
        outstanding.local_work_active = false;
        outstanding.write_status.expire_unwritten();
        let outstanding = &self.window.outstanding[index];
        let (unreceived, outcome) = return_range_work(
            &self.work,
            &mut self.budget,
            outstanding,
            self.sequencer_view.borrow().download_floor,
        );
        self.trace_work_returned(reason, outstanding, unreceived.len(), outcome);
        if outstanding.write_status.was_skipped() {
            self.window.retire_locally(index);
        }
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
        if self.received_status
            && (status.max_blocks_per_response != self.max_blocks_per_response
                || status.max_response_bytes != self.max_response_bytes
                || status.max_inflight_requests != self.window.max_inflight_requests)
        {
            self.session
                .close_connection("block_sync_numeric_limits_changed");
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
        self.session.mark_status_received();
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

    /// A chain reset returns local work while retaining the original wire authorization.
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
        self.return_unreceived_requests("view_reset");
        self.retry_avoid.clear();
        // Clear our (now-empty) registry outstanding and refresh slot diagnostics.
        self.publish_outstanding();
        // A destructive reset pulled this peer's outstanding on our initiative, so its
        // no-progress probe streak must not stay charged: reset it (and clear the idle
        // liveness deadline) so an unproven peer whose only probe was in flight at the
        // reset can probe again instead of wedging at its cap.
        self.window.note_locally_returned_requests();
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
            .filter(|outstanding| outstanding.local_work_active)
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

    /// Admit the request and any retained window growth together. Trying exact
    /// growth and smaller batches avoids stranding capacity in an unused buffer.
    fn authorize_request_metadata(
        &mut self,
    ) -> Result<(usize, ResponseAuthorization), ResponseAdmissionError> {
        let mut count = self.request_count_cap();
        loop {
            let bytes = RequestWrite::metadata_bytes(count)
                .and_then(|bytes| {
                    bytes.checked_add(collection_allocation_bytes::<ExpectedBlock>(count)?)
                })
                .ok_or(ResponseAdmissionError::MemoryFull)?;
            let required_heights = self
                .window
                .outstanding
                .iter()
                .try_fold(count, |total, range| {
                    total.checked_add(range.request.expected_blocks.len())
                })
                .ok_or(ResponseAdmissionError::MemoryFull)?;
            let required_ranges = self
                .window
                .outstanding
                .len()
                .checked_add(1)
                .ok_or(ResponseAdmissionError::MemoryFull)?;
            for geometric in [true, false] {
                let prepared = self.registry.prepare_response_storage(
                    &self.peer,
                    self.generation,
                    |published, ranges| {
                        let window_plan = self.window.outstanding.plan_capacity(1, geometric)?;
                        let scratch_plan = self.outstanding_snapshot.plan_capacity(
                            required_heights.saturating_sub(self.outstanding_snapshot.len()),
                            geometric,
                        )?;
                        let published_plan = published.plan_capacity(
                            required_heights.saturating_sub(published.len()),
                            geometric,
                        )?;
                        let ranges_plan = ranges.plan_capacity(
                            required_ranges.saturating_sub(ranges.len()),
                            geometric,
                        )?;
                        let retained_bytes = [
                            window_plan.as_ref().map_or(0, |plan| plan.bytes()),
                            scratch_plan.as_ref().map_or(0, |plan| plan.bytes()),
                            published_plan.as_ref().map_or(0, |plan| plan.bytes()),
                            ranges_plan.as_ref().map_or(0, |plan| plan.bytes()),
                        ]
                        .into_iter()
                        .try_fold(0u64, u64::checked_add)
                        .ok_or(ResponseAdmissionError::MemoryFull)?;
                        let (authorization, mut funding) = self
                            .session
                            .authorize_response_with_retained_memory(bytes, retained_bytes)?;
                        self.window
                            .outstanding
                            .apply_capacity_from(window_plan, &mut funding)?;
                        self.outstanding_snapshot
                            .apply_capacity_from(scratch_plan, &mut funding)?;
                        published.apply_capacity_from(published_plan, &mut funding)?;
                        ranges.apply_capacity_from(ranges_plan, &mut funding)?;
                        assert!(
                            funding.is_none_or(|funding| funding.bytes() == 0),
                            "every admitted allocation is transferred to its retained buffer"
                        );
                        Ok(authorization)
                    },
                );
                match prepared {
                    Ok(authorization) => return Ok((count, authorization)),
                    Err(ResponseAdmissionError::MemoryFull) => {}
                    Err(error) => return Err(error),
                }
            }
            if count == 1 {
                return Err(ResponseAdmissionError::MemoryFull);
            }
            count = count.div_ceil(2);
        }
    }

    /// Fill this peer's available slots in a single pass, letting the byte budget
    /// (re-checked each iteration via `try_reserve`) be the congestion window. The
    /// per-peer state is routine-local / in the registry.
    ///
    /// There is no floor gate: downloads are governed by the byte budget and
    /// per-peer slots, never floor-distance / near-tip lag.
    async fn try_fill(&mut self) -> Option<Instant> {
        self.response_memory_waiting = false;
        self.gc_skipped_outstanding();
        // The BBR cwnd is clamped to the peer's advertised hard cap inside
        // `available_slots`, so there is no separate window to reconcile on a
        // `Status` change.
        // Chain progress releases local work, while original wire slots stay charged.
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
        let request_sender = self.session.request_sender();
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
            // Reserve transport capacity before taking work or charging bytes.
            let slot = match request_sender.try_reserve_guarded() {
                Ok(slot) => slot,
                Err(crate::zakura::transport::GuardedReserveError::Full) => {
                    break FillStop::OutboundFull
                }
                Err(error) => {
                    tracing::debug!(
                        peer = ?self.peer,
                        generation = self.generation,
                        ?error,
                        "could not reserve guarded block request transport capacity"
                    );
                    self.session.cancel_token().cancel();
                    break FillStop::SendError;
                }
            };
            let (max_count, authorization) = match self.authorize_request_metadata() {
                Ok(prepared) => prepared,
                Err(ResponseAdmissionError::MemoryFull) => {
                    self.response_memory_waiting = true;
                    break FillStop::ResponseMemory;
                }
                Err(ResponseAdmissionError::Retired) => {
                    self.session.cancel_token().cancel();
                    break FillStop::SendError;
                }
            };
            let Some(request_id) = self.next_request_id else {
                break FillStop::Internal;
            };
            self.next_request_id = request_id.get().checked_add(1).and_then(NonZeroU64::new);
            let in_bypass = normal_slots == 0;
            let (servable_low, servable_high) = (self.servable_low, self.servable_high);

            // Compute this chunk's count and byte ceiling before taking any work.
            // The count cap is the peer/request cap; the byte cap is enforced by
            // the budgeted work-queue take and then by the reservation below.
            let response_byte_cap = u64::from(self.max_response_bytes.max(1));

            let view = *self.sequencer_view.borrow();
            let floor_high = floor_rescue_high(view.download_floor);
            // One snapshot per iteration: the floor and speculative lanes decide
            // against the same memory picture, and `admit` is the single authority
            // for the commit-window exemption, the resident gate, and take sizing
            // (geometry included — an exempt grant is clamped at the window top, so
            // no above-window height can ride an exempt request past the gate).
            let snapshot = self.admission_snapshot(&view);
            let mut items = Vec::new();
            if servable_low <= floor_high {
                if let Some(floor_start) = self
                    .work
                    .first_pending_in_range(servable_low, servable_high.min(floor_high))
                    .filter(|height| {
                        // Prefer only a peer that can serve the actual missing height.
                        !self.registry.floor_has_preferred_unsaturated_server(
                            *height,
                            &self.peer,
                            self.window.bbr_rtprop_ms(now),
                            in_bypass,
                        )
                    })
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
                            items = self.work.take_for_request(
                                servable_low,
                                grant.take_high,
                                max_count,
                                grant.max_request_bytes.min(floor_cwnd_cap).max(1),
                                self.generation,
                                request_id,
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
                        items = self.work.take_for_request(
                            servable_low,
                            grant.take_high,
                            max_count,
                            grant.max_request_bytes.min(above_cwnd_cap),
                            self.generation,
                            request_id,
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
            // to re-take. `return_unpublished` does NOT notify (the other peers were
            // already woken by the original failure return), so this cannot
            // self-wake into a take/return spin. If the whole chunk is still
            // avoided, break — the routine wakes to retry when the avoid window
            // expires (see `earliest_deadline_sleep`).
            {
                let is_allowed = |height: &block::Height, item: &WorkItem| {
                    !self
                        .window
                        .outstanding
                        .iter()
                        .any(|range| range.request.contains(*height))
                        && !self.retry_avoid.contains_key(height)
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
                    self.work.return_unpublished(&items);
                    retry_filter_deadline = Some(self.retry_filter_wake_deadline(now));
                    break FillStop::RetryAvoid;
                };
                let keep_len = keep.len();
                let mut returned_avoided = false;
                if keep.start > 0 {
                    self.work.return_unpublished(&items[..keep.start]);
                    items.drain(..keep.start);
                    returned_avoided = true;
                }
                if keep_len < items.len() {
                    self.work.return_unpublished(&items[keep_len..]);
                    items.truncate(keep_len);
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
            let owner = scope.bind(self.generation, request_id);
            let expected_blocks = items
                .iter()
                .map(|(height, item)| ExpectedBlock {
                    height: *height,
                    hash: item.hash,
                    estimated_bytes: item.estimated_bytes,
                })
                .collect();
            let claim = RequestWrite::new(
                owner,
                items,
                self.work.clone(),
                self.budget.clone(),
                self.session.cancel_token(),
                authorization.write_permission(),
            );
            let count = match u32::try_from(kept_count) {
                Ok(count) => count,
                Err(_) => break FillStop::Internal,
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
                expected_blocks,
            };

            let queued_at = Instant::now();
            let msg = BlockSyncMessage::GetBlocks {
                start_height: request.start_height,
                count: request.count,
            };
            let frame = match msg.encode_frame() {
                Ok(frame) => frame,
                Err(_) => {
                    self.session.cancel_token().cancel();
                    break FillStop::SendError;
                }
            };

            // A block-count delivery sample proves progress even though it cannot
            // supply a byte rate for estimating transfer time.
            let byte_rate = self.window.bbr_btlbw_bytes_per_sec(queued_at);
            let has_delivery_measurement =
                byte_rate.is_some() || self.window.bbr_btlbw_milliblocks(queued_at).is_some();
            let deadline = request_deadline(
                request_priority,
                queued_at,
                self.config.request_timeout,
                self.config.effective_floor_rescue_timeout(),
                // Responses share an ordered stream. Include earlier unreceived
                // work so this request cannot expire while those bodies arrive.
                self.window
                    .outstanding_reserved_bytes()
                    .saturating_add(reserved_bytes),
                // Filter BtlBw by the request's send time so a stale-high rate from a
                // now-slow peer cannot tighten the deadline below what it can meet.
                byte_rate,
                has_delivery_measurement,
            );
            let request_start_height = request.start_height;
            let request_count = request.count;
            let request_estimated_bytes = request.estimated_bytes;
            let mut delivered = false;
            if !claim.publish(|| {
                self.window.outstanding.push(OutstandingBlockRange {
                    authorization,
                    response: ResponseCredit::new(
                        u64::from(request.count),
                        u64::from(self.max_response_bytes),
                    ),
                    local_work_active: true,
                    request,
                    write_status: claim.status(),
                    charged_for_liveness: false,
                    queued_at,
                    deadline,
                    delivery_snapshot: self.window.delivery_snapshot(queued_at),
                    delivered_bytes: 0,
                    received: ReceivedBlockTracker::default(),
                });
                delivered = slot.send_request(frame, claim.clone());
            }) {
                break FillStop::Internal;
            }
            if !delivered {
                tracing::debug!(
                    peer = ?self.peer,
                    generation = self.generation,
                    start_height = ?request_start_height,
                    count = request_count,
                    "block request transport closed during publication"
                );
                claim.delivery_failed();
                break FillStop::SendError;
            }
            metrics::counter!("sync.block.request.sent").increment(1);
            #[cfg(test)]
            self.registry
                .observe_exchange_for_test(&self.peer, self.generation, true);
            if in_bypass {
                // A floor request borrowed a bypass slot while the cwnd was saturated.
                metrics::counter!("sync.block.request.floor_bypass").increment(1);
            }
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
        self.work.return_unpublished(items);
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
        self.gc_skipped_outstanding();
        let mut expired = 0;
        let mut timed_out = 0;
        let mut avoided = Vec::new();
        for index in (0..self.window.outstanding.len()).rev() {
            let outstanding = &self.window.outstanding[index];
            if !outstanding.local_work_active || outstanding.deadline > now {
                continue;
            }
            outstanding.write_status.expire_unwritten();
            if !outstanding.write_status.was_skipped() {
                timed_out += 1;
                avoided.extend(unreceived_heights(outstanding));
            }
            self.detach_local_work(index, "request_timeout");
            expired += 1;
        }
        if timed_out > 0 {
            self.window.record_timeout(timed_out);
        }
        self.note_retry_avoid(avoided);
        if expired > 0 {
            self.publish_outstanding();
        }
        expired > 0
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
                // A briefly full outbound queue may mean our request has not
                // reached the peer yet. Keep the existing bounded write grace;
                // responses are read independently of that queue.
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

    /// An undrainable response requires local connection closure. Idle sessions may park.
    fn no_progress_stall(&mut self, now: Instant, error: &'static str) -> Result<(), SinkReject> {
        if !self.window.outstanding.is_empty()
            || no_progress_response(self.allow_no_progress_park) == NoProgressResponse::Disconnect
        {
            tracing::debug!(
                peer = ?self.peer,
                outstanding = self.window.outstanding.len(),
                "closing Zakura connection after an undrainable block-sync stall"
            );
            return Err(SinkReject::local_connection(error));
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
        if self.window.outstanding.is_empty() && self.window.block_liveness_deadline.is_none() {
            return Ok(());
        }
        let error = match failure {
            OrderedStreamFailure::RemoteClose => {
                "block-sync peer closed the stream with its requests unanswered"
            }
            OrderedStreamFailure::WriteTimeout => {
                "block-sync stream write stalled with download requests unanswered"
            }
        };
        self.no_progress_stall(now, error)
    }

    /// Release local work below the floor without consuming wire parts or freeing protocol slots.
    fn gc_committed_outstanding(&mut self) {
        let floor = self.download_floor();
        for index in (0..self.window.outstanding.len()).rev() {
            if self.window.outstanding[index].request.end_height() <= floor {
                self.detach_local_work(index, "committed_work");
            }
        }
        self.publish_outstanding();
    }

    /// A skipped frame ends the peer obligation independently of any received
    /// body whose owner must remain available to the sequencer.
    fn gc_skipped_outstanding(&mut self) {
        if self.window.discard_skipped_requests() {
            self.publish_outstanding();
        }
    }

    /// Detach work whose exact owner was retired. Started wire ranges remain live.
    fn gc_obsolete_outstanding(&mut self) {
        for index in (0..self.window.outstanding.len()).rev() {
            let outstanding = &self.window.outstanding[index];
            let owner = outstanding.request.owner;
            let still_owned = unreceived_heights(outstanding)
                .any(|height| self.work.owner_for_height(height) == Some(owner));
            if !still_owned {
                self.detach_local_work(index, "obsolete_work");
            }
        }
        self.publish_outstanding();
    }

    // ===================== inbound matched body (ports `handle_block`) ======

    /// Match the next unconsumed header of exactly one original range.
    fn response_index(&self, hash: block::Hash) -> Result<usize, SinkReject> {
        let mut matches =
            self.window
                .outstanding
                .iter()
                .enumerate()
                .filter_map(|(index, range)| {
                    let consumed = usize::try_from(range.response.consumed_objects()).ok()?;
                    range
                        .request
                        .expected_blocks
                        .get(consumed)
                        .filter(|expected| expected.hash == hash)
                        .map(|_| index)
                });
        let index = matches
            .next()
            .ok_or_else(|| SinkReject::protocol("block has no next expected hash"))?;
        if matches.next().is_some() {
            return Err(SinkReject::protocol(
                "block matches ambiguous response authorization",
            ));
        }
        Ok(index)
    }

    async fn handle_body(
        &mut self,
        block: Arc<block::Block>,
        body_wire_bytes: Option<u64>,
        body_permit: Option<mpsc::OwnedPermit<SequencedBody>>,
        raw_block_payload: Option<Arc<[u8]>>,
    ) -> Result<(), SinkReject> {
        let hash = block.hash();
        let index = self.response_index(hash)?;
        let height = block
            .coinbase_height()
            .ok_or_else(|| SinkReject::protocol("block has no coinbase height"))?;
        let outstanding = &self.window.outstanding[index];
        let consumed = usize::try_from(outstanding.response.consumed_objects())
            .expect("consumption cannot exceed the bounded request count");
        if outstanding.request.expected_blocks[consumed].height != height {
            return Err(SinkReject::protocol(
                "block height disagrees with its authorized header",
            ));
        }
        let serialized_bytes = match body_wire_bytes {
            Some(bytes) => bytes,
            None => u64::try_from(
                block
                    .zcash_serialize_to_vec()
                    .map_err(SinkReject::local)?
                    .len(),
            )
            .expect("serialized block length fits u64"),
        };
        let original_owner = outstanding.request.owner;
        let request_start = outstanding.request.start_height;
        let request_count = outstanding.request.count;
        let elapsed = outstanding.queued_at.elapsed();
        let delivery_snapshot = outstanding.delivery_snapshot;
        let was_detached = !outstanding.local_work_active;
        let outstanding = &mut self.window.outstanding[index];
        outstanding
            .response
            .consume(1, serialized_bytes)
            .map_err(SinkReject::protocol)?;
        outstanding.mark_received(height);
        outstanding.record_body_bytes(serialized_bytes);
        let complete = outstanding.is_complete();
        let delivered_bytes = outstanding.response.consumed_bytes();
        if complete {
            outstanding.local_work_active = false;
        }
        self.window
            .note_block_progress(Instant::now(), self.config.effective_liveness_timeout());
        if complete {
            self.window.record_delivery(
                Instant::now(),
                elapsed,
                request_count,
                delivered_bytes,
                delivery_snapshot,
            );
        }
        if was_detached {
            self.window.credit_late_delivery();
        }
        self.publish_outstanding();

        // Application policy runs only after consumption. Local obsolescence
        // cannot restore credit, authorize duplicates, or erase the ending.
        if height <= self.download_floor() || self.work.hash_for_height(height) != Some(hash) {
            let released = self
                .work
                .release_reserved_heights_for_owner(original_owner, [height]);
            self.budget.release(released);
            return Ok(());
        }
        if self.work.pending_contains(height) {
            let view = *self.sequencer_view.borrow();
            let snapshot = self.admission_snapshot(&view);
            if !admit_received_body(&self.config, &snapshot, height, serialized_bytes) {
                return Ok(());
            }
        }
        let Some(request_id) = self.next_request_id else {
            let released = self
                .work
                .release_reserved_heights_for_owner(original_owner, [height]);
            self.budget.release(released);
            return Err(SinkReject::local_connection(
                "body work identities exhausted",
            ));
        };
        self.next_request_id = request_id.get().checked_add(1).and_then(NonZeroU64::new);
        let Some((owner, released)) =
            self.work
                .claim_authorized_body(height, hash, self.generation, request_id)
        else {
            return Ok(());
        };
        self.trace
            .record_block_body_received(hash, BlockBodySource::Zakura);
        metrics::counter!("sync.block.body.received").increment(1);
        self.record_received(serialized_bytes);
        let decoded_bytes = record_decoded_memory_size(&block, body_wire_bytes);
        self.trace_body_received(
            height,
            serialized_bytes,
            decoded_bytes,
            Some(request_start),
            Some(request_count),
            Some(elapsed_ms_u64(elapsed)),
        );
        let previous_block_hash = block.header.previous_block_hash;
        let body =
            BufferedBlockBody::from_measured_decoded_block(block, raw_block_payload, decoded_bytes);
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
        self.budget.release(released);
        self.publish_outstanding();
        Ok(())
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

    async fn handle_blocks_done(
        &mut self,
        start_height: block::Height,
        returned: u32,
    ) -> Result<(), SinkReject> {
        let Some(index) = self.window.outstanding_index_for_start(start_height) else {
            self.report_misbehavior(BlockSyncMisbehavior::UnsolicitedDone)
                .await;
            return Err(SinkReject::protocol("BlocksDone has no live range"));
        };
        let range = &self.window.outstanding[index];
        if returned == 0
            || returned > range.request.count
            || u64::from(returned) != range.response.consumed_objects()
        {
            self.report_misbehavior(BlockSyncMisbehavior::MalformedMessage)
                .await;
            return Err(SinkReject::protocol(
                "BlocksDone count differs from consumed prefix",
            ));
        }
        let disposition = if range.is_complete() {
            Disposition::Satisfied
        } else {
            Disposition::RetryMissing
        };
        self.charge_short_response_reliability(index, disposition);
        self.finish_outstanding_at(index, disposition);
        #[cfg(test)]
        self.registry
            .observe_exchange_for_test(&self.peer, self.generation, false);
        Ok(())
    }

    async fn handle_range_unavailable(
        &mut self,
        start_height: block::Height,
        count: u32,
    ) -> Result<(), SinkReject> {
        let Some(index) = self.window.outstanding_index_for_start(start_height) else {
            self.report_misbehavior(BlockSyncMisbehavior::UnsolicitedDone)
                .await;
            return Err(SinkReject::protocol("RangeUnavailable has no live range"));
        };
        let range = &self.window.outstanding[index];
        if count != range.request.count || range.response.consumed_objects() != 0 {
            self.report_misbehavior(BlockSyncMisbehavior::MalformedMessage)
                .await;
            return Err(SinkReject::protocol(
                "RangeUnavailable does not match an unconsumed original range",
            ));
        }
        self.trace_range_unavailable(
            start_height,
            Some(count),
            Some(elapsed_ms_u64(range.queued_at.elapsed())),
        );
        self.charge_short_response_reliability(index, Disposition::RetryOriginal);
        self.finish_outstanding_at(index, Disposition::RetryOriginal);
        #[cfg(test)]
        self.registry
            .observe_exchange_for_test(&self.peer, self.generation, false);
        Ok(())
    }

    /// Charge a short response only while its original local work remains active.
    fn charge_short_response_reliability(&mut self, index: usize, disposition: Disposition) {
        if disposition == Disposition::Satisfied
            || !self.window.outstanding[index].local_work_active
        {
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

    // ===================== outstanding lifecycle ===================

    fn finish_outstanding_at(&mut self, index: usize, disposition: Disposition) {
        if index >= self.window.outstanding.len() {
            return;
        }
        let mut outstanding = self.window.outstanding.remove(index);
        outstanding.authorization.finish();
        self.finish_detached(outstanding, disposition);
    }

    fn finish_detached(&mut self, outstanding: OutstandingBlockRange, disposition: Disposition) {
        if !outstanding.local_work_active {
            self.publish_outstanding();
            self.window.disarm_liveness_after_progress_if_idle();
            return;
        }
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
                let (unreceived, outcome) = return_range_work(
                    &self.work,
                    &mut self.budget,
                    &outstanding,
                    self.sequencer_view.borrow().download_floor,
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
        self.publish_outstanding();
        if disposition == Disposition::Satisfied {
            self.window.disarm_liveness_after_progress_if_idle();
        }
    }

    /// Publish this peer's current *unreceived* in-flight height metadata to the
    /// registry, so the producer's `!has_outstanding_request` filter and the
    /// low-water `total_unreceived` gate read the same per-request-granularity
    /// count (`expected_blocks.len() − received.len()`).
    /// Received-but-uncommitted heights are excluded here because they are held in
    /// `work.in_flight` instead — the producer's `!in_flight_contains` clause
    /// already keeps them out of `pending`.
    fn publish_outstanding(&mut self) {
        self.outstanding_snapshot.clear();
        for outstanding in &self.window.outstanding {
            if !outstanding.local_work_active {
                continue;
            }
            for expected in &outstanding.request.expected_blocks {
                if !outstanding.has_received(expected.height) {
                    self.outstanding_snapshot.push((
                        expected.height,
                        OutstandingMeta {
                            owner: outstanding.request.owner,
                            hash: expected.hash,
                            estimated_bytes: expected.estimated_bytes,
                            queued_at: outstanding.queued_at,
                            deadline: outstanding.deadline,
                        },
                    ));
                }
            }
        }
        // Filter before combining overlapping heights so a stale request cannot
        // hide the current owner's metadata. Release the queue lock before the registry lock.
        let retained = self
            .work
            .retain_owned(&mut self.outstanding_snapshot, |meta| meta.owner);
        self.outstanding_snapshot.truncate(retained);
        self.outstanding_snapshot
            .sort_unstable_by_key(|(height, _)| *height);
        assert!(
            self.outstanding_snapshot
                .windows(2)
                .all(|pair| pair[0].0 != pair[1].0),
            "each height belongs to one current request owner"
        );
        // Publish the window diagnostics for the reactor's periodic trace row and
        // for other routines' cross-peer floor-bias decisions.
        let hard_capacity = hard_outbound_capacity(self.window.max_inflight_requests);
        self.registry.publish_response_snapshot(
            &self.peer,
            self.generation,
            &mut self.outstanding_snapshot,
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
            self.window
                .outstanding
                .iter()
                .map(|range| (range.request.start_height, range.request.end_height())),
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

/// Return only still-needed unreceived work. Finality releases estimates, never wire credit.
fn return_range_work(
    work: &WorkQueue,
    budget: &mut super::state::ByteBudget,
    outstanding: &OutstandingBlockRange,
    floor: block::Height,
) -> (Vec<block::Height>, WorkReturnOutcome) {
    let released = work.release_reserved_heights_for_owner(
        outstanding.request.owner,
        unreceived_heights(outstanding).filter(|height| *height <= floor),
    );
    let needed: Vec<_> = unreceived_heights(outstanding)
        .filter(|height| *height > floor)
        .collect();
    let outcome = work.release_reserved_and_return_items_detailed_for_owner(
        outstanding.request.owner,
        needed.iter().copied(),
    );
    budget.release(released.saturating_add(outcome.released_bytes));
    (needed, outcome)
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

impl Drop for PeerRoutine {
    /// Destruction cannot erase started authorization on a live connection.
    /// Queued requests arbitrate against writer startup before local cleanup.
    fn drop(&mut self) {
        if self.has_started_responses() {
            self.session
                .close_connection("unfinished_response_state_retired");
        }
        self.return_unreceived_requests("peer_routine_drop");
    }
}

#[cfg(test)]
mod tests {
    mod memory;
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
    use super::super::{BlockSyncFrontiers, BlockSyncPeerSession, CwndUnit, ZakuraBlockSyncConfig};
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
        let generation = registry
            .admit_session(
                &peer,
                crate::zakura::ServicePeerDirection::Outbound,
                &config,
                0,
                Instant::now(),
            )
            .generation();
        let mut routine = PeerRoutine::new(
            peer,
            0,
            session,
            in_recv,
            config,
            true,
            generation,
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
        let mut routine = PeerRoutine::new(
            peer,
            0,
            session,
            in_recv,
            config,
            true,
            generation,
            budget.clone(),
            work.clone(),
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
        // The routine learns these from a `Status` frame in production; set them directly
        // so a single `try_fill` pass exercises the floor arm.
        routine.received_status = true;
        routine.servable_low = block::Height(1);
        routine.servable_high = block::Height(10);

        if let Some(age) = measurement_age {
            let delivered_at = Instant::now() - age;
            let elapsed = Duration::from_secs(1);
            let snapshot = routine.window.delivery_snapshot(delivered_at - elapsed);
            routine
                .window
                .record_delivery(delivered_at, elapsed, 1, 256 * 1024, snapshot);
        }
        let _ = routine.try_fill().await;

        let outstanding = &routine.window.outstanding[0];
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
        let work = Arc::new(WorkQueue::new(block::Height(0)));
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
        let (_view_tx, view_rx) = watch::channel(initial_view(BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        }));
        let mut routine = PeerRoutine::new(
            peer.clone(),
            0,
            session,
            in_recv,
            config,
            true,
            generation,
            budget.clone(),
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
            routine.try_fill().await;
            assert_eq!(routine.window.outstanding.len(), index + 1);
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
                    .window
                    .note_block_progress(Instant::now(), liveness_timeout);
            }
        }
        let first_owner = routine.window.outstanding[0].request.owner;
        let liveness_deadline = routine.window.block_liveness_deadline.unwrap();
        let deadline = routine
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
            routine.window.bbr_reliability_permille(),
            expected_window.bbr_reliability_permille(),
            "only started writes count as peer failures: {writes:?}, progress={progress_after_first}"
        );
        assert_eq!(
            routine.window.bbr_effective_cwnd(),
            expected_window.bbr_effective_cwnd(),
            "queued-only expiry must not dip the congestion window"
        );
        assert_eq!(
            routine.window.requests_without_block_progress,
            u32::try_from(peer_timeouts.len())
                .unwrap()
                .saturating_sub(u32::from(
                    progress_after_first && writes[0] != DeadlineWriteState::Queued
                )),
        );
        assert_eq!(
            routine.retry_avoid.keys().copied().collect::<Vec<_>>(),
            peer_timeouts,
        );
        assert_eq!(routine.window.outstanding.len(), peer_timeouts.len());
        for range in &routine.window.outstanding {
            assert!(!range.local_work_active);
            assert_eq!(range.response.consumed_objects(), 0);
        }
        assert_eq!(budget.reserved(), 0);
        assert_eq!(work.reserved_bytes(), 0);
        assert_eq!(work.pending_len(), writes.len());
        for index in 0..writes.len() {
            let height = block::Height(u32::try_from(index + 2).unwrap());
            assert!(work.pending_contains(height));
            assert!(!registry.peer_has_outstanding_height(&peer, height));
        }
        assert_eq!(
            routine.window.check_liveness(liveness_deadline),
            if peer_timeouts.is_empty() {
                super::LivenessOutcome::Ok
            } else {
                super::LivenessOutcome::Park
            },
        );
        assert!(!cancel.is_cancelled());

        if peer_timeouts.is_empty() {
            // Reissue before the old queued frame is drained. Its later drop
            // must preserve the replacement's ownership and byte charge.
            routine.try_fill().await;
            assert_eq!(routine.window.outstanding.len(), 1);
            assert_ne!(routine.window.outstanding[0].request.owner, first_owner);
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
        let mut budget = ByteBudget::new(1_000_000);
        let work = Arc::new(WorkQueue::new(block::Height(0)));
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
        let (view_tx, view_rx) = watch::channel(initial_view(BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        }));
        let mut routine = PeerRoutine::new(
            peer.clone(),
            0,
            session,
            in_recv,
            config,
            true,
            generation,
            budget.clone(),
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
        routine.try_fill().await;
        assert_eq!(routine.window.outstanding.len(), 1);
        let owner = routine.window.outstanding[0].request.owner;
        let deadline = routine.window.outstanding[0].deadline;
        let queued = out_recv.recv().await.unwrap();
        assert!(registry.peer_has_outstanding_height(&peer, block::Height(2)));

        if progress_before_other {
            routine
                .window
                .note_block_progress(Instant::now(), liveness_timeout);
        }
        if other_request {
            add_work(3);
            routine.try_fill().await;
            assert_eq!(routine.window.outstanding.len(), 2);
        }
        let skipped = work.subscribe_available().notified();
        tokio::pin!(skipped);
        skipped.as_mut().enable();
        // Another peer wins height 2 before this writer starts. Its body retains
        // this exact owner until the sequencer consumes it.
        budget.release(
            work.release_active_reserved_height_for_owner(owner, block::Height(2))
                .unwrap(),
        );
        match order {
            ReceiptCleanup::BeforeWriter => {
                let liveness_deadline = routine.window.block_liveness_deadline.unwrap();
                assert!(
                    liveness_deadline < deadline,
                    "transfer allowance outlasts liveness"
                );
                assert_eq!(work.owner_for_height(block::Height(2)), Some(owner));
                assert_eq!(budget.reserved(), 0);
                routine.gc_obsolete_outstanding();
                let result = routine.handle_deadlines(liveness_deadline).await;
                assert!(
                    result.is_ok(),
                    "a request made unwritable by receipt must not park its peer: {result:?}"
                );
                assert!(routine.window.outstanding.is_empty());
                return;
            }
            ReceiptCleanup::Writer => {}
            ReceiptCleanup::CommittedGc => {
                budget.release(work.advance_floor(block::Height(2)));
                view_tx.send_modify(|view| view.download_floor = block::Height(2));
                routine.gc_committed_outstanding();
            }
            ReceiptCleanup::ObsoleteGc => {
                budget.release(work.advance_floor(block::Height(2)));
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
        routine.handle_deadlines(deadline).await.unwrap();
        assert!(!registry.peer_has_outstanding_height(&peer, block::Height(2)));
        assert_eq!(
            work.owner_for_height(block::Height(2)),
            matches!(order, ReceiptCleanup::Writer).then_some(owner),
        );
        assert!(!work.pending_contains(block::Height(2)));
        assert!(routine.retry_avoid.is_empty());
        assert!(!cancel.is_cancelled());
        if other_request {
            assert_eq!(routine.window.outstanding.len(), 1);
            assert_eq!(routine.window.requests_without_block_progress, 1);
            assert!(registry.peer_has_outstanding_height(&peer, block::Height(3)));
            let liveness_deadline = routine.window.block_liveness_deadline.unwrap();
            assert_eq!(
                routine.window.check_liveness(liveness_deadline),
                super::LivenessOutcome::Park
            );
            assert_eq!(budget.reserved(), 1_000);
        } else {
            assert!(routine.window.outstanding.is_empty());
            assert_eq!(routine.window.requests_without_block_progress, 0);
            assert!(routine.window.block_liveness_deadline.is_none());
            routine
                .handle_deadlines(deadline + liveness_timeout)
                .await
                .unwrap();
            assert_eq!(budget.reserved(), 0);
            add_work(3);
            routine.try_fill().await;
            assert_eq!(
                routine.window.outstanding.len(),
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
        let mut routine = PeerRoutine::new(
            peer,
            0,
            session,
            in_recv,
            config,
            true,
            generation,
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

        use zakura_chain::serialization::ZcashDeserializeInto;
        let body: Arc<block::Block> = Arc::new(
            zakura_test::vectors::BLOCK_MAINNET_1_BYTES
                .zcash_deserialize_into()
                .unwrap(),
        );
        let config = ZakuraBlockSyncConfig::default();
        let budget = ByteBudget::new(1_000_000);
        let work = Arc::new(WorkQueue::new(block::Height(0)));
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
        let session_observer = session.clone();
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
        let (view_tx, view_rx) = watch::channel(initial_view(BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        }));
        let mut routine = PeerRoutine::new(
            peer.clone(),
            0,
            session,
            in_recv.with_failure_cause(cause.clone()),
            config.clone(),
            !readmitted,
            generation,
            budget.clone(),
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
            routine.try_fill().await;
            assert_eq!(routine.window.outstanding.len(), 1);
            timeout(Duration::from_secs(1), out_recv.recv())
                .await
                .unwrap()
                .unwrap()
                .write_with(|_| async { Ok::<_, std::convert::Infallible>(()) })
                .await
                .unwrap();
            assert!(work.in_flight_contains(block::Height(1)));
            if expired {
                let after_deadline =
                    routine.window.outstanding[0].deadline + Duration::from_millis(1);
                routine.expire_due_timeouts(after_deadline);
                assert_eq!(routine.window.outstanding.len(), 1);
                assert!(!routine.window.outstanding[0].local_work_active);
                assert_eq!(routine.window.outstanding[0].response.consumed_objects(), 0);
                assert!(routine.window.block_liveness_deadline.is_some());
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
                        super::BlockSyncMessage::Block(body.clone())
                            .encode_frame()
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                in_send
                    .send(
                        super::BlockSyncMessage::BlocksDone {
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
                        super::BlockSyncMessage::Status(super::BlockSyncStatus::default())
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
                let frame = crate::zakura::Frame {
                    message_type: u16::from(super::MSG_BS_BLOCK),
                    flags: 0,
                    payload: vec![super::MSG_BS_BLOCK],
                };
                assert!(super::BlockSyncMessage::decode_frame(frame.clone()).is_err());
                in_send.send(frame).await.unwrap();
            }
            BufferedResponse::None => {}
        }
        if buffered == BufferedResponse::CompleteWhileOutboundFull {
            for _ in 0..4 {
                fill_outbound
                    .try_send(
                        super::BlockSyncMessage::Status(super::BlockSyncStatus::default())
                            .encode_frame()
                            .unwrap(),
                    )
                    .unwrap();
            }
            assert_eq!(routine.session.outbound_capacity(), 0);
        }
        let mut running = Box::pin(routine.run());
        if buffered == BufferedResponse::AlreadyProcessing {
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
        if failure.is_some()
            && !held_capacity.is_empty()
            && !matches!(
                buffered,
                BufferedResponse::MalformedBlockBackpressured
                    | BufferedResponse::MalformedBlockAlreadyProcessing
            )
        {
            assert!(
                futures::poll!(running.as_mut()).is_pending(),
                "a failed stream must retain its unvalidated response until decode capacity returns"
            );
            assert!(work.in_flight_contains(block::Height(1)));
            assert!(budget.reserved() > 0);
            assert!(sequencer_recv.try_recv().is_err());
            assert!(!registry.is_peer_parked(&peer, Instant::now()));
            if buffered == BufferedResponse::ResetDuringBackpressure {
                budget.clone().release(work.reset_above(block::Height(0)));
                view_tx.send_modify(|view| view.reset_epoch += 1);
            }
            drop(held_capacity);
        }
        let result = timeout(Duration::from_secs(1), running).await.unwrap();
        assert_eq!(
            session_observer.connection_is_closed_for_test(),
            result
                .as_ref()
                .is_err_and(|error| error.closes_connection())
        );
        if matches!(
            buffered,
            BufferedResponse::Malformed
                | BufferedResponse::MalformedBlock
                | BufferedResponse::MalformedBlockBackpressured
                | BufferedResponse::MalformedBlockAlreadyProcessing
        ) {
            assert!(matches!(result, Err(SinkReject::Protocol(_))), "{result:?}");
            assert!(!registry.is_peer_parked(&peer, Instant::now()));
        } else if unanswered
            && (failure.is_none()
                || matches!(
                    buffered,
                    BufferedResponse::None | BufferedResponse::StatusOnly
                ))
        {
            assert!(
                matches!(result, Err(SinkReject::Connection(_))),
                "{result:?}"
            );
            assert!(!registry.is_peer_parked(&peer, Instant::now()));
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
