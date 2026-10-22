//! The want-work fill pass (ports `fill_peer`).
//!
//! [`PeerRoutine::try_fill`] runs one synchronous pass of request attempts, and
//! [`PeerRoutine::try_issue_one`] shows one attempt's phases in order.
//! [`PeerState`](super::state::PeerState) and the pure planners in [`plan`] make
//! the decisions. The methods below read shared state, acquire resources, and
//! publish at the existing synchronization points.

use std::{num::NonZeroU64, sync::Arc, time::Instant};

use zakura_chain::block;
use zakura_rt::time;

use super::PeerRoutine;
use crate::zakura::{
    block_sync::{
        admission::{floor_rescue_high, request_deadline},
        events::RoutineToReactor,
        peer_registry::hard_outbound_capacity,
        regulated::live_requester::FencedRequest,
        request::{BlockRangeRequest, ExpectedBlock},
        state::{OutstandingBlockRange, ReceivedBlockTracker, RequestKey},
        work_queue::{RequestWrite, WorkItem},
        BlockSyncMessage,
    },
    regulation::{Exchange, ExchangeWriter, PoolEntry},
    transport::{FrameWriteClaim, GuardedFrameSlot, GuardedReserveError},
    Frame, FramedSend,
};

mod plan;

use plan::{Funding, RetryDeadlines, RetryFact, TakePlan};

/// Why a fill pass stopped issuing requests. Typed so every admission refusal is
/// attributed exhaustively; the `as_str` labels feed the `sync.block.fill_stop`
/// metric and the fill-stop trace.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum FillStop {
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
    /// Too many fully received requests still owe their ending
    /// (see `MAX_PENDING_ENDINGS`).
    PendingEndings,
}

impl FillStop {
    pub(super) fn as_str(self) -> &'static str {
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
            FillStop::PendingEndings => "pending_endings",
        }
    }
}

/// Peer-local limits for one fill attempt, decided by
/// [`PeerState::fill_limits`](super::state::PeerState::fill_limits) before any
/// capacity is acquired.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct FillLimits {
    /// The inclusive range this peer advertised it can serve.
    pub(super) servable_low: block::Height,
    pub(super) servable_high: block::Height,
    /// Whether the congestion window is full for above-floor work, so only the
    /// floor bypass has a slot.
    pub(super) floor_bypass_only: bool,
    /// The request's block-count cap.
    pub(super) max_count: usize,
    /// The peer's response byte cap, at least one.
    pub(super) response_byte_cap: u64,
    /// Congestion-window byte headroom for a floor take, including the floor
    /// bypass. `None` when the window counts requests rather than bytes.
    pub(super) floor_cwnd_bytes: Option<u64>,
    /// Congestion-window byte headroom for an above-floor take, without the
    /// floor bypass. `None` when the window counts requests rather than bytes.
    pub(super) above_floor_cwnd_bytes: Option<u64>,
}

impl FillLimits {
    /// Refuse above-floor work while only the floor bypass has a slot: the
    /// bypass funds the floor, never a speculative fetch.
    fn above_floor_slot(&self) -> Result<(), FillStop> {
        if self.floor_bypass_only {
            Err(FillStop::CwndSaturated)
        } else {
            Ok(())
        }
    }
}

/// What one fill pass did.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct FillReport {
    /// Why the pass stopped.
    pub(super) stop: FillStop,
    /// Requests the pass published.
    pub(super) sent: u32,
    /// When to retry work that the retry filter returned, if it returned any.
    pub(super) retry_at: Option<Instant>,
}

/// One fill pass's time and its retry wake.
struct FillPass {
    /// Sampled once at the start of the pass. Eligibility, floor ranking, and
    /// retry checks all use it.
    now: Instant,
    /// The wake that the retry filter scheduled for work it returned.
    retry_at: Option<Instant>,
}

/// Requester and transport capacity for one request, acquired before any work
/// is taken or any byte is charged. Dropping it releases what it still holds,
/// in field order.
struct RequestCapacity<'a> {
    /// A reserved slot in this peer's guarded outbound queue.
    slot: GuardedFrameSlot<'a>,
    /// The writer-fenced exchange that the request's response will end.
    exchange: Exchange,
    /// This request's entry in the node-wide requester pool.
    entry: PoolEntry,
}

/// A contiguous run that the work queue holds provisionally for one attempt's
/// exact owner. Every stop before funding returns it with `return_unpublished`;
/// funding transfers it to a [`RequestWrite`].
struct TakenWork {
    /// The download floor that both lanes planned against.
    download_floor: block::Height,
    items: Vec<(block::Height, WorkItem)>,
}

/// How publication of one prepared request ended.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum PublishOutcome {
    /// The request entered the guarded transport queue. This means queue
    /// admission, not wire delivery.
    Published(RequestKey),
    /// The writer fence retired, or the connection closed, before publication.
    FenceRefused,
    /// A reset or cancellation revoked the attempt's provisional work.
    ClaimRefused,
    /// The transport queue closed during publication. The outstanding record
    /// stays until the routine collects its skipped write.
    TransportClosed,
}

/// The facts that a published request's diagnostics report.
#[derive(Copy, Clone, Debug)]
struct IssuedRequest {
    start_height: block::Height,
    count: u32,
    estimated_bytes: u64,
    /// Whether the request used a floor-bypass slot.
    floor_bypass: bool,
}

impl PeerRoutine {
    /// Fill this peer's available slots in a single pass, letting the byte budget
    /// (re-checked each attempt via `try_reserve`) be the congestion window.
    ///
    /// There is no floor gate: downloads are governed by the byte budget and
    /// per-peer slots, never floor-distance / near-tip lag. The pass never
    /// awaits. Other tasks change shared state between its synchronous operations.
    pub(super) fn try_fill(&mut self) -> FillReport {
        self.request_pool_waiting = false;
        self.gc_skipped_outstanding();
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
        let now = time::Instant::now().into_std();
        self.retry_avoid.retain(|_, until| *until > now);
        let mut pass = FillPass {
            now,
            retry_at: None,
        };
        let request_sender = self.session.request_sender();
        let mut sent = 0u32;
        let stop = loop {
            match self.try_issue_one(&request_sender, &mut pass) {
                Ok(()) => sent = sent.saturating_add(1),
                Err(stop) => break stop,
            }
        };
        let report = FillReport {
            stop,
            sent,
            retry_at: pass.retry_at,
        };
        self.finish_fill_pass(report);
        report
    }

    /// Try to publish one request. Each phase hands its result to the next or
    /// stops the pass. A guard that an earlier phase acquired settles when it drops.
    fn try_issue_one(
        &mut self,
        request_sender: &FramedSend,
        pass: &mut FillPass,
    ) -> Result<(), FillStop> {
        // Local eligibility: status, the no-progress cap, and the congestion window.
        let limits = self
            .state
            .fill_limits(&self.config, &self.window, pass.now)?;
        // Capacity: a requester pool entry, an exchange, and a transport slot.
        let capacity = self.acquire_request_capacity(request_sender)?;
        // A later failure of this attempt still consumes its request ID.
        let request_id = self.state.allocate_request_id().ok_or(FillStop::Internal)?;
        // Selection: the floor lane, then the above-floor lane.
        let taken = self.select_work(&limits, request_id, pass.now)?;
        // Retry filter: keep the first allowed contiguous run.
        let items = self.filter_retry(taken.items, pass)?;
        self.trace_work_taken(limits.servable_low, limits.servable_high, items.len());
        debug_assert!(
            !items.is_empty(),
            "retry filtering must retain a nonempty allowed run"
        );
        let Some((first_height, first_item)) = items.first().copied() else {
            return Err(FillStop::Internal);
        };
        debug_assert!(items.iter().all(|(_, item)| item.scope == first_item.scope));

        // Funding: classify the retained run, never the probe's priority.
        let funding = plan::funding_for(
            taken.download_floor,
            first_height,
            items.iter().map(|(_, item)| item.estimated_bytes),
        );
        let owner = first_item.scope.bind(self.generation, request_id);
        let claim = self.fund(owner, &items, funding)?;

        // Preparation: the request, its response authorization, and its frame.
        let request = block_range_request(owner, &items, funding)?;
        let writer = self.authorize_response(&request, capacity.entry, capacity.exchange)?;
        let queued_at = time::Instant::now().into_std();
        let frame = self.encode_request(&request)?;
        let deadline = self.request_deadline(funding, queued_at);

        // Publication: writer fence, work claim, outstanding record, then send.
        let issued = IssuedRequest {
            start_height: request.start_height,
            count: request.count,
            estimated_bytes: request.estimated_bytes,
            floor_bypass: limits.floor_bypass_only,
        };
        match self.publish_request(
            request,
            capacity.slot,
            frame,
            &writer,
            &claim,
            queued_at,
            deadline,
        ) {
            PublishOutcome::Published(key) => {
                self.complete_request(key, issued, queued_at);
                Ok(())
            }
            PublishOutcome::FenceRefused | PublishOutcome::ClaimRefused => {
                self.requester.retire(first_height, true);
                Err(FillStop::Internal)
            }
            PublishOutcome::TransportClosed => {
                tracing::debug!(
                    peer = ?self.peer,
                    generation = self.generation,
                    start_height = ?issued.start_height,
                    count = issued.count,
                    "block request transport closed during publication"
                );
                claim.delivery_failed();
                self.requester.retire(first_height, true);
                Err(FillStop::SendError)
            }
        }
    }

    /// Attribute the pass's stop and ask for more work if pending work runs low.
    fn finish_fill_pass(&mut self, report: FillReport) {
        // A pass that issued nothing is a candidate bubble; the reason + the live
        // slot/budget/work snapshot let a trace tell a legitimate stop (no_work with
        // empty queue, cwnd_saturated) from a recoverable one (slots + budget + work
        // all free, stopped anyway). The at-cap gauge is latched here so every gate
        // refusal — floor arm, speculative arm, in bypass or not — sets it.
        if report.stop == FillStop::LookaheadCap {
            metrics::gauge!("sync.block.backlog.at_cap").set(1.0);
        }
        metrics::counter!("sync.block.fill_stop", "reason" => report.stop.as_str()).increment(1);
        if report.sent == 0 {
            self.trace_fill_stop(report.stop.as_str());
        }

        // If pending work is running low, ping the reactor to re-query (the
        // producer self-gates on low-water, so this is idempotent/cheap).
        if self.work.pending_len() < self.refill_low_water_blocks() {
            let _ = self
                .routine_to_reactor
                .try_send(RoutineToReactor::RequeryNeeded);
        }
    }

    /// Acquire a requester pool entry, an exchange, and a guarded transport slot,
    /// in that order. A refusal releases whatever this call already acquired.
    fn acquire_request_capacity<'a>(
        &mut self,
        request_sender: &'a FramedSend,
    ) -> Result<RequestCapacity<'a>, FillStop> {
        self.request_pool_waiting = false;
        if let Some(stop) = self.requester_stop() {
            return Err(stop);
        }
        let Some(entry) = self.requester.pool.try_entry() else {
            self.request_pool_waiting = true;
            return Err(FillStop::Budget);
        };
        let Some(exchange) = self.requester.open() else {
            return Err(FillStop::SendError);
        };
        let slot = match request_sender.try_reserve_guarded() {
            Ok(slot) => slot,
            Err(GuardedReserveError::Full) => return Err(FillStop::OutboundFull),
            Err(error) => {
                tracing::debug!(
                    peer = ?self.peer,
                    generation = self.generation,
                    ?error,
                    "could not reserve guarded block request transport capacity"
                );
                self.session.cancel_token().cancel();
                return Err(FillStop::SendError);
            }
        };
        Ok(RequestCapacity {
            slot,
            exchange,
            entry,
        })
    }

    /// Take one contiguous run for `request_id`: the floor lane first, then the
    /// above-floor lane.
    ///
    /// Both lanes plan against one admission snapshot, so they decide against
    /// the same memory picture. Each probe and its take run at separate
    /// synchronization points, so another routine can take a probed height first.
    fn select_work(
        &self,
        limits: &FillLimits,
        request_id: NonZeroU64,
        now: Instant,
    ) -> Result<TakenWork, FillStop> {
        let view = *self.sequencer_view.borrow();
        let snapshot = self.admission_snapshot(&view);
        let floor_start = self.probe_floor(limits, view.download_floor, now);
        if let Some(take) = plan::plan_floor(&self.config, limits, snapshot, floor_start)? {
            let items = self.take_work(take, request_id);
            if !items.is_empty() {
                return Ok(TakenWork {
                    download_floor: view.download_floor,
                    items,
                });
            }
        }

        limits.above_floor_slot()?;
        let start = self
            .work
            .first_pending_in_range(limits.servable_low, limits.servable_high);
        let take = plan::plan_above_floor(&self.config, limits, snapshot, start);
        match take {
            Ok(_) => metrics::gauge!("sync.block.backlog.at_cap").set(0.0),
            Err(FillStop::LookaheadCap) => metrics::gauge!("sync.block.backlog.at_cap").set(1.0),
            Err(_) => {}
        }
        let items = self.take_work(take?, request_id);
        if items.is_empty() {
            return Err(FillStop::NoWork);
        }
        Ok(TakenWork {
            download_floor: view.download_floor,
            items,
        })
    }

    /// The first pending height this peer can serve in the floor-rescue window,
    /// unless a preferred carrier should serve it. A preferred peer must serve
    /// the missing height; a peer that only serves the committed floor cannot
    /// take this request.
    fn probe_floor(
        &self,
        limits: &FillLimits,
        download_floor: block::Height,
        now: Instant,
    ) -> Option<block::Height> {
        let floor_high = floor_rescue_high(download_floor);
        let start = self
            .work
            .first_pending_in_range(limits.servable_low, limits.servable_high.min(floor_high))?;
        let deferred = self.registry.floor_has_preferred_unsaturated_server(
            start,
            &self.peer,
            self.window.bbr_rtprop_ms(now),
            limits.floor_bypass_only,
        );
        (!deferred).then_some(start)
    }

    /// Take `take` from the work queue for this attempt's exact owner.
    fn take_work(&self, take: TakePlan, request_id: NonZeroU64) -> Vec<(block::Height, WorkItem)> {
        self.work.take_for_request(
            take.low,
            take.high,
            take.max_count,
            take.max_bytes,
            self.generation,
            request_id,
        )
    }

    /// Peer-local retry bias: if the contiguous chunk we just took leads with
    /// heights this routine recently *failed* (RangeUnavailable / timeout /
    /// send-failure), quietly put those back so another peer can contest them
    /// first, and only keep the run this routine is allowed to re-take.
    ///
    /// `return_unpublished` does NOT notify (the other peers were already woken
    /// by the original failure return), so this cannot self-wake into a
    /// take/return spin. If the whole chunk is still avoided, the pass stops; the
    /// routine wakes to retry when the avoid window expires (see
    /// `earliest_deadline_sleep`).
    fn filter_retry(
        &self,
        mut items: Vec<(block::Height, WorkItem)>,
        pass: &mut FillPass,
    ) -> Result<Vec<(block::Height, WorkItem)>, FillStop> {
        let now = pass.now;
        let run =
            plan::select_retry_run(&items, |(height, item)| self.retry_fact(*height, item, now));
        let Some(keep) = run.keep else {
            self.work.return_unpublished(&items);
            // The pass stops here, and this wake replaces any earlier attempt's.
            pass.retry_at = self.retry_filter_wake_deadline(now, run.timed_rejection);
            return Err(FillStop::RetryAvoid);
        };
        let keep_len = keep.len();
        let mut returned_avoided = false;
        if keep.start > 0 {
            let avoided: Vec<_> = items.drain(..keep.start).collect();
            self.work.return_unpublished(&avoided);
            returned_avoided = true;
        }
        if keep_len < items.len() {
            let avoided = items.split_off(keep_len);
            self.work.return_unpublished(&avoided);
            returned_avoided = true;
        }
        if returned_avoided {
            pass.retry_at = [
                pass.retry_at,
                self.retry_filter_wake_deadline(now, run.timed_rejection),
            ]
            .into_iter()
            .flatten()
            .min();
        }
        Ok(items)
    }

    /// Read one taken item's retry facts from their owners.
    fn retry_fact(&self, height: block::Height, item: &WorkItem, now: Instant) -> RetryFact {
        if self.requester.blocks_retry(height) {
            return RetryFact::ProtocolBlocked;
        }
        let avoided = self.retry_avoid.contains_key(&height)
            || self
                .registry
                .is_floor_height_avoided(&self.peer, height, now)
            || self
                .registry
                .is_body_retry_avoided(&self.peer, item.scope, item.hash, now);
        if avoided {
            RetryFact::Avoided
        } else {
            RetryFact::Allowed
        }
    }

    /// When to retry after the filter returned work. See [`plan::retry_wake_deadline`].
    pub(super) fn retry_filter_wake_deadline(
        &self,
        now: Instant,
        timed_rejection: bool,
    ) -> Option<Instant> {
        plan::retry_wake_deadline(
            now,
            timed_rejection,
            RetryDeadlines {
                local: self.retry_avoid.values().min().copied(),
                floor_watchdog: self.registry.next_floor_avoid_deadline(&self.peer, now),
                body_retry: self.registry.next_body_retry_deadline(&self.peer, now),
                retirement: self
                    .requester
                    .retirement_deadline(self.config.effective_liveness_timeout()),
            },
        )
    }

    /// Charge the byte budget for the retained run, then transfer the run and
    /// its charge to one exact write claim.
    fn fund(
        &mut self,
        owner: zakura_header_chain::BodyWorkOwner,
        items: &[(block::Height, WorkItem)],
        funding: Funding,
    ) -> Result<Arc<RequestWrite>, FillStop> {
        if !self.reserve_request_budget(funding) {
            // Quiet (no notify): the returning routine must not re-wake its own
            // want-work arm into a take/return spin, and any other peer waiting
            // on budget capacity is woken by the matching `budget.release`.
            self.work.return_unpublished(items);
            return Err(FillStop::Budget);
        }
        Ok(RequestWrite::new(
            owner,
            items.to_vec(),
            self.work.clone(),
            self.budget.clone(),
            self.session.cancel_token(),
        ))
    }

    fn reserve_request_budget(&mut self, funding: Funding) -> bool {
        if self.budget.try_reserve(funding.reserved_bytes) {
            return true;
        }
        if funding.allows_overdraft() {
            self.budget.charge(funding.reserved_bytes);
            metrics::counter!("sync.block.budget.floor_overdraft").increment(1);
            return true;
        }
        false
    }

    /// Reserve response authorization for `request`. A valid scheduler request
    /// cannot fail here, so a failure cancels the routine.
    fn authorize_response(
        &self,
        request: &BlockRangeRequest,
        entry: PoolEntry,
        exchange: Exchange,
    ) -> Result<ExchangeWriter, FillStop> {
        let expected: Vec<_> = request
            .expected_blocks
            .iter()
            .map(|block| block.hash)
            .collect();
        self.requester
            .reserve(
                request.start_height,
                &expected,
                self.state.max_response_bytes,
                entry,
                exchange,
            )
            .map_err(|error| {
                tracing::error!(
                    ?error,
                    "valid scheduler request could not reserve its response"
                );
                self.cancel.cancel();
                FillStop::Internal
            })
    }

    /// Encode the request's `GetBlocks` frame. A failure cancels the session.
    fn encode_request(&self, request: &BlockRangeRequest) -> Result<Frame, FillStop> {
        BlockSyncMessage::GetBlocks {
            start_height: request.start_height,
            count: request.count,
        }
        .encode_frame()
        .map_err(|_| {
            self.session.cancel_token().cancel();
            FillStop::SendError
        })
    }

    /// The request's deadline, from its queue time and this peer's delivery rate.
    fn request_deadline(&self, funding: Funding, queued_at: Instant) -> Instant {
        // A block-count delivery sample proves progress even though it cannot
        // supply a byte rate for estimating transfer time.
        let byte_rate = self.window.bbr_btlbw_bytes_per_sec(queued_at);
        let has_delivery_measurement =
            byte_rate.is_some() || self.window.bbr_btlbw_milliblocks(queued_at).is_some();
        request_deadline(
            funding.priority,
            queued_at,
            self.config.request_timeout,
            self.config.effective_floor_rescue_timeout(),
            // Responses share an ordered stream. Include earlier unreceived
            // work so this request cannot expire while those bodies arrive.
            self.window
                .outstanding_reserved_bytes()
                .saturating_add(funding.reserved_bytes),
            // Filter BtlBw by the request's send time so a stale-high rate from a
            // now-slow peer cannot tighten the deadline below what it can meet.
            byte_rate,
            has_delivery_measurement,
        )
    }

    /// Publish a prepared request. The writer fence admits publication, then the
    /// work claim admits it under the work-queue lock. Inside both, the routine
    /// records the outstanding request and then sends the frame, so a reset or
    /// writer start cannot observe a published claim without its record.
    #[allow(clippy::too_many_arguments)]
    fn publish_request(
        &mut self,
        request: BlockRangeRequest,
        slot: GuardedFrameSlot<'_>,
        frame: Frame,
        writer: &ExchangeWriter,
        claim: &Arc<RequestWrite>,
        queued_at: Instant,
        deadline: Instant,
    ) -> PublishOutcome {
        let write_claim: Arc<dyn FrameWriteClaim> = Arc::new(FencedRequest {
            exchange: writer.clone(),
            work: claim.clone(),
        });
        let mut issued = None;
        let mut delivered = false;
        let fenced = writer.publish(|| {
            claim.publish(|| {
                issued = Some(self.window.outstanding.push(OutstandingBlockRange {
                    request,
                    write_status: claim.status(),
                    queued_at,
                    deadline,
                    delivery_snapshot: self.window.delivery_snapshot(queued_at),
                    delivered_bytes: 0,
                    received: ReceivedBlockTracker::default(),
                }));
                delivered = slot.send_request(frame, write_claim);
            });
        });
        // The claim records the request only when it admits publication.
        match (fenced, issued) {
            (false, _) => PublishOutcome::FenceRefused,
            (true, None) => PublishOutcome::ClaimRefused,
            (true, Some(_)) if !delivered => PublishOutcome::TransportClosed,
            (true, Some(key)) => PublishOutcome::Published(key),
        }
    }

    /// Record a published request: metrics, liveness, shared claims, and diagnostics.
    fn complete_request(&mut self, key: RequestKey, issued: IssuedRequest, queued_at: Instant) {
        metrics::counter!("sync.block.request.sent").increment(1);
        let estimate_kind = if issued.estimated_bytes
            >= u64::from(issued.count).saturating_mul(block::MAX_BLOCK_BYTES)
        {
            "worst_case"
        } else {
            "hinted"
        };
        metrics::counter!("sync.block.request.size_estimate", "kind" => estimate_kind).increment(1);
        if issued.floor_bypass {
            // A floor request borrowed a bypass slot while the cwnd was saturated.
            metrics::counter!("sync.block.request.floor_bypass").increment(1);
        }
        self.window
            .arm_liveness(queued_at, self.config.effective_liveness_timeout());
        self.publish_request_claims(key);
        self.publish_slot_diagnostics();
        self.trace_get_blocks_sent(
            issued.start_height,
            issued.count,
            issued.estimated_bytes,
            issued.floor_bypass,
        );
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
}

/// Build the request for a funded run. The run's size estimates sum to the
/// funded reservation.
fn block_range_request(
    owner: zakura_header_chain::BodyWorkOwner,
    items: &[(block::Height, WorkItem)],
    funding: Funding,
) -> Result<BlockRangeRequest, FillStop> {
    let (Some((start_height, first_item)), Ok(count)) =
        (items.first().copied(), u32::try_from(items.len()))
    else {
        return Err(FillStop::Internal);
    };
    Ok(BlockRangeRequest {
        owner,
        start_height,
        count,
        anchor_hash: first_item.hash,
        // The summed size-estimate reservation for this request (released on a
        // send failure); equals the sum of the per-height `expected_blocks`
        // estimates.
        estimated_bytes: funding.reserved_bytes,
        expected_blocks: items
            .iter()
            .map(|(height, item)| ExpectedBlock {
                height: *height,
                hash: item.hash,
                estimated_bytes: item.estimated_bytes,
            })
            .collect(),
    })
}
