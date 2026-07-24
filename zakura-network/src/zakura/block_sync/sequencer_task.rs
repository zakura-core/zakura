//! The Sequencer's own serial task (Sequencer task boundary split).
//!
//! Sequencer task moves the consensus-critical commit pipeline (`Sequencer`: reorder →
//! applying → `SubmitBlock` → apply-finished) off the reactor's single thread
//! and into this spawned serial task. The reactor keeps issuance, peer matching,
//! serving, and the producer; peer routines forward block bodies over a bounded
//! body input channel, while the reactor forwards progress-critical control
//! events over a non-blocking control channel. The reactor learns committed
//! progress back over a non-blocking `watch` ([`SequencerView`]).
//!
//! Each input handler owns one stage of the commit pipeline: the body-acceptance
//! tail (`handle_accept_body`), the verified-tip frontier advance
//! (`handle_frontier_advance`), the chain-tip reset (`handle_frontier_reset`), and
//! the apply completion (`handle_apply_finished`). They mutate the `Sequencer`,
//! byte budget, and work queue directly and emit `SubmitBlock`/`Misbehavior`
//! actions on the same channel the reactor uses.

use super::super::trace::queue_send_trace as qs_trace;
use super::{
    events::*,
    reactor::{bs_insert_height, bs_insert_str, bs_insert_u64},
    reorder::BufferedBlockBody,
    sequencer::*,
    state::*,
    work_queue::WorkQueue,
    *,
};

/// Delay before retrying a verifier submission that could not enter the shared
/// action channel.
const SUBMISSION_RETRY_DELAY: Duration = Duration::from_millis(100);

/// A received body a peer routine matched (or accepted unmatched) and forwards
/// to the commit pipeline. This is the only bounded Sequencer input: a slow
/// verifier can backpressure body intake, but must not block apply/frontier
/// control events that release budget and drive the next scheduling reaction.
#[derive(Debug)]
pub(super) struct SequencedBody {
    pub(super) height: block::Height,
    pub(super) hash: block::Hash,
    pub(super) previous_block_hash: block::Hash,
    pub(super) body: BufferedBlockBody,
    pub(super) bytes: u64,
    pub(super) peer: ZakuraPeerId,
    pub(super) received_at: Instant,
    queue_accounting: SequencerInputAccounting,
}

impl SequencedBody {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new_queued(
        height: block::Height,
        hash: block::Hash,
        previous_block_hash: block::Hash,
        body: BufferedBlockBody,
        bytes: u64,
        peer: ZakuraPeerId,
        received_at: Instant,
        input_bytes: Arc<std::sync::atomic::AtomicU64>,
        input_decoded_attributed_memory_bytes: Arc<std::sync::atomic::AtomicU64>,
    ) -> Self {
        let decoded_attributed_memory_size_bytes = body.decoded_attributed_memory_size_bytes();
        Self {
            height,
            hash,
            previous_block_hash,
            body,
            bytes,
            peer,
            received_at,
            queue_accounting: SequencerInputAccounting::new(
                input_bytes,
                bytes,
                input_decoded_attributed_memory_bytes,
                decoded_attributed_memory_size_bytes,
            ),
        }
    }

    /// Transfer this body out of queue ownership before processing it.
    pub(super) fn leave_queue(&mut self) {
        self.queue_accounting.release();
    }
}

#[derive(Debug)]
struct SequencerInputAccounting {
    input_bytes: Arc<std::sync::atomic::AtomicU64>,
    bytes: u64,
    input_decoded_attributed_memory_bytes: Arc<std::sync::atomic::AtomicU64>,
    decoded_attributed_memory_size_bytes: u64,
    active: bool,
}

impl SequencerInputAccounting {
    fn new(
        input_bytes: Arc<std::sync::atomic::AtomicU64>,
        bytes: u64,
        input_decoded_attributed_memory_bytes: Arc<std::sync::atomic::AtomicU64>,
        decoded_attributed_memory_size_bytes: u64,
    ) -> Self {
        add_atomic_bytes(&input_bytes, bytes);
        add_atomic_bytes(
            &input_decoded_attributed_memory_bytes,
            decoded_attributed_memory_size_bytes,
        );
        Self {
            input_bytes,
            bytes,
            input_decoded_attributed_memory_bytes,
            decoded_attributed_memory_size_bytes,
            active: true,
        }
    }

    fn release(&mut self) {
        if !std::mem::take(&mut self.active) {
            return;
        }
        release_atomic_bytes(&self.input_bytes, self.bytes);
        release_atomic_bytes(
            &self.input_decoded_attributed_memory_bytes,
            self.decoded_attributed_memory_size_bytes,
        );
    }
}

impl Drop for SequencerInputAccounting {
    fn drop(&mut self) {
        self.release();
    }
}

/// Progress-critical Sequencer events forwarded by the reactor.
///
/// These locally-generated events must not sit behind downloaded bodies, so
/// they use a separate prioritized channel.
#[derive(Debug)]
pub(super) enum SequencerControlInput {
    /// A verified-tip advance (frontier growth/commit).
    FrontierAdvance {
        frontiers: BlockSyncFrontiers,
        release_applied: bool,
    },
    /// A chain-tip reset (reorg/checkpoint/coalesced update). The two `peer_*`
    /// bools are the peer-outstanding-derived halves of the reset decision,
    /// precomputed by the reactor (which owns peer state); the task ORs them with
    /// its own Sequencer-internal predicates.
    FrontierReset {
        frontiers: BlockSyncFrontiers,
        preserve_active_successors: bool,
        /// `peers.any(outstanding.end_height() >= tip+1)` — half of
        /// `has_active_successor_after`.
        peer_has_successor_after: bool,
        /// `peers.any(outstanding.expected_hash(tip) is Some(h) && h != hash)` —
        /// the peer-outstanding clause of `reset_tip_conflicts_with_local_work`.
        peer_outstanding_conflicts_at_tip: bool,
    },
    /// A verifier apply completion.
    ApplyFinished {
        token: BlockApplyToken,
        height: block::Height,
        hash: block::Hash,
        result: BlockApplyResult,
        local_frontier: Option<BlockSyncFrontiers>,
    },
}

/// The progress view the reactor reacts to. A `watch` (latest-wins) send never
/// blocks, so the task never blocks on the reactor and the bounded input channel
/// cannot deadlock against it.
#[derive(Copy, Clone, Debug, PartialEq)]
pub(super) struct SequencerView {
    pub(super) verified_tip: block::Height,
    pub(super) verified_hash: block::Hash,
    pub(super) download_floor: block::Height,
    pub(super) finalized: block::Height,
    /// Increments only when the task performs a destructive `reset_to`, so the
    /// reactor distinguishes an advance (drop outstanding *through* tip) from a
    /// reset (drop *all* outstanding).
    pub(super) reset_epoch: u64,
    /// Increments once per processed frontier/reset/apply input (NOT per accepted
    /// body). The reactor runs its heavy serving/producer/schedule reaction only
    /// when this advances: a pure body buffer/submit needs nothing but the
    /// forwarding peer's own reschedule, while a frontier advance, reset, or
    /// apply-finished must re-query and reschedule.
    pub(super) reaction_epoch: u64,
    pub(super) reorder_len: u64,
    pub(super) applying_len: u64,
    pub(super) reorder_buffered_bytes: u64,
    pub(super) applying_buffered_bytes: u64,
    pub(super) sequencer_input_decoded_attributed_memory_bytes: u64,
    pub(super) reorder_decoded_attributed_memory_bytes: u64,
    pub(super) applying_decoded_attributed_memory_bytes: u64,
    pub(super) active_pipeline_decoded_attributed_memory_bytes: u64,
    pub(super) unsubmitted_applying_count: u64,
    /// Submitted decoded bodies awaiting matching completion, including entries
    /// detached from `applying` but still retained by the driver.
    pub(super) in_flight_submission_count: u64,
    pub(super) in_flight_submission_bytes: u64,
    pub(super) committed_bytes_per_sec: u64,
    pub(super) committed_blocks_per_sec: u64,
}

/// Build the initial view from the startup frontiers, before the task runs.
pub(super) fn initial_view(frontiers: BlockSyncFrontiers) -> SequencerView {
    SequencerView {
        verified_tip: frontiers.verified_block_tip,
        verified_hash: frontiers.verified_block_hash,
        download_floor: frontiers.verified_block_tip,
        finalized: frontiers.finalized_height,
        reset_epoch: 0,
        reaction_epoch: 0,
        reorder_len: 0,
        applying_len: 0,
        reorder_buffered_bytes: 0,
        applying_buffered_bytes: 0,
        sequencer_input_decoded_attributed_memory_bytes: 0,
        reorder_decoded_attributed_memory_bytes: 0,
        applying_decoded_attributed_memory_bytes: 0,
        active_pipeline_decoded_attributed_memory_bytes: 0,
        unsubmitted_applying_count: 0,
        in_flight_submission_count: 0,
        in_flight_submission_bytes: 0,
        committed_bytes_per_sec: 0,
        committed_blocks_per_sec: 0,
    }
}

/// The serial commit-pipeline task. Owns the `Sequencer` (moved out of state), a
/// `ByteBudget` clone, an `Arc<WorkQueue>` clone, an action sender clone, and the
/// committed throughput meter. Releases bytes directly and emits `SubmitBlock` /
/// `Misbehavior` on the same action channel the reactor uses.
pub(super) struct SequencerTask {
    sequencer: Sequencer,
    budget: ByteBudget,
    work: Arc<WorkQueue>,
    actions: mpsc::Sender<BlockSyncAction>,
    committed_throughput: ThroughputMeter,
    /// Tracks the finalized height so the published view carries it forward; the
    /// reactor folds it into its `finalized_height` mirror with a `max`.
    finalized_height: block::Height,
    verified_block_hash: block::Hash,
    reset_epoch: u64,
    reaction_epoch: u64,
    body_input_rx: mpsc::Receiver<SequencedBody>,
    control_input_rx: mpsc::UnboundedReceiver<SequencerControlInput>,
    _body_input_bytes: Arc<std::sync::atomic::AtomicU64>,
    body_input_decoded_attributed_memory_bytes: Arc<std::sync::atomic::AtomicU64>,
    view_tx: watch::Sender<SequencerView>,
    action_send_timeout: Duration,
    submission_retry_at: Option<time::Instant>,
    submission_retry_started_at: Option<time::Instant>,
    submission_retry_attempt: u64,
    trace: ZakuraTrace,
}

impl Drop for SequencerTask {
    fn drop(&mut self) {
        self.body_input_rx.close();
        while self.body_input_rx.try_recv().is_ok() {}

        self.view_tx.send_modify(|view| {
            view.reorder_decoded_attributed_memory_bytes = 0;
            view.applying_decoded_attributed_memory_bytes = 0;
            view.sequencer_input_decoded_attributed_memory_bytes = 0;
            view.active_pipeline_decoded_attributed_memory_bytes = 0;
        });
    }
}

impl SequencerTask {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        sequencer: Sequencer,
        budget: ByteBudget,
        work: Arc<WorkQueue>,
        actions: mpsc::Sender<BlockSyncAction>,
        committed_throughput: ThroughputMeter,
        frontiers: BlockSyncFrontiers,
        body_input_rx: mpsc::Receiver<SequencedBody>,
        control_input_rx: mpsc::UnboundedReceiver<SequencerControlInput>,
        body_input_bytes: Arc<std::sync::atomic::AtomicU64>,
        body_input_decoded_attributed_memory_bytes: Arc<std::sync::atomic::AtomicU64>,
        view_tx: watch::Sender<SequencerView>,
        action_send_timeout: Duration,
        trace: ZakuraTrace,
    ) -> Self {
        Self {
            sequencer,
            budget,
            work,
            actions,
            committed_throughput,
            finalized_height: frontiers.finalized_height,
            verified_block_hash: frontiers.verified_block_hash,
            reset_epoch: 0,
            reaction_epoch: 0,
            body_input_rx,
            control_input_rx,
            _body_input_bytes: body_input_bytes,
            body_input_decoded_attributed_memory_bytes,
            view_tx,
            action_send_timeout,
            submission_retry_at: None,
            submission_retry_started_at: None,
            submission_retry_attempt: 0,
            trace,
        }
    }

    pub(super) async fn run(mut self) {
        // Track input closure explicitly so the loop exits once both inputs close:
        // a `select!` whose arms are all disabled with no `else` panics, so the
        // top-of-loop guard breaks out before the last open channel is gated off.
        let mut control_open = true;
        let mut body_open = true;
        loop {
            if !control_open && !body_open && self.submission_retry_at.is_none() {
                break;
            }
            let submission_retry_at = self.submission_retry_at;
            tokio::select! {
                biased;

                input = self.control_input_rx.recv(), if control_open => {
                    match input {
                        Some(input) => {
                            let needs_reaction = self.handle_control_input(input).await;
                            if needs_reaction {
                                self.reaction_epoch = self.reaction_epoch.saturating_add(1);
                            }
                            self.publish_view();
                        }
                        None => control_open = false,
                    }
                }

                _ = time::sleep_until(submission_retry_at.unwrap_or_else(time::Instant::now)),
                    if submission_retry_at.is_some() =>
                {
                    self.submission_retry_at = None;
                    self.submit_pending_blocks().await;
                    self.publish_view();
                }

                body = self.body_input_rx.recv(), if body_open => {
                    match body {
                        Some(mut body) => {
                            body.leave_queue();
                            self.handle_accept_body(body);
                            // Publish the synchronous queue → reorder/applying ownership
                            // transfer before an action-channel send can await or time out.
                            // This updates observability without waking scheduling watchers.
                            self.publish_decoded_ownership_view();
                            self.submit_pending_blocks().await;
                            self.publish_view();
                        }
                        None => body_open = false,
                    }
                }
            }
        }
    }

    async fn handle_control_input(&mut self, input: SequencerControlInput) -> bool {
        // Each handler reports whether it did work that needs the reactor's heavy
        // serving/producer/schedule tail. Bumping `reaction_epoch` only then keeps
        // the reactor from re-querying/-scheduling on a pure body buffer/submit or
        // a no-op (stale/duplicate) apply completion.
        match input {
            SequencerControlInput::FrontierAdvance {
                frontiers,
                release_applied,
            } => {
                self.handle_frontier_advance(frontiers, release_applied)
                    .await;
                true
            }
            SequencerControlInput::FrontierReset {
                frontiers,
                preserve_active_successors,
                peer_has_successor_after,
                peer_outstanding_conflicts_at_tip,
            } => {
                self.handle_frontier_reset(
                    frontiers,
                    preserve_active_successors,
                    peer_has_successor_after,
                    peer_outstanding_conflicts_at_tip,
                )
                .await;
                true
            }
            SequencerControlInput::ApplyFinished {
                token,
                height,
                hash,
                result,
                local_frontier,
            } => {
                let needs_reaction = self
                    .handle_apply_finished(token, height, hash, result, local_frontier)
                    .await;
                self.submit_pending_blocks().await;
                needs_reaction
            }
        }
    }

    /// Body-acceptance tail: offer the body to the reorder buffer, then drain the
    /// ready contiguous prefix into applying.
    fn handle_accept_body(&mut self, body: SequencedBody) {
        let queued_elapsed = body.received_at.elapsed();
        let outcome = match self.sequencer.accept_buffered_body(
            body.height,
            body.hash,
            body.previous_block_hash,
            body.body,
            body.bytes,
            body.peer,
        ) {
            AcceptOutcome::Buffered { .. } => "buffered",
            AcceptOutcome::Redundant { .. } => "redundant",
        };
        self.trace_body_accepted(body.height, queued_elapsed, outcome);
        let _ = self.sequencer.drain_ready_into_applying();
    }

    /// Apply a verified-tip frontier advance: fold finalized height forward, drop
    /// stale updates, then advance the verified tip and floor and drain the newly
    /// contiguous prefix.
    async fn handle_frontier_advance(
        &mut self,
        frontiers: BlockSyncFrontiers,
        release_applied: bool,
    ) {
        // Fold the finalized height forward unconditionally, then drop a stale
        // update. The verified tip is monotonic: an advance whose target is below
        // our verified tip must be a no-op, never a regression. Without this guard
        // the second growth-reset path (`< floor`, which permits `< verified_tip`)
        // would call `advance_verified_tip` with a lower tip and regress it.
        self.finalized_height = self.finalized_height.max(frontiers.finalized_height);
        if frontiers.verified_block_tip < self.sequencer.verified_tip() {
            return;
        }
        self.verified_block_hash = frontiers.verified_block_hash;
        let advance = self
            .sequencer
            .advance_verified_tip(frontiers.verified_block_tip, release_applied);
        if advance.changed {
            let released = self.work.advance_floor(frontiers.verified_block_tip);
            self.budget.release(released);
            self.release_contiguous_blocks().await;
        }
    }

    /// Handle a chain-tip reset: classify it as growth (treat as an advance) or a
    /// destructive reorg (pin tip/floor to the target, clear successor buffers, and
    /// bump the reset epoch). The peer-outstanding clauses of the decision arrive as
    /// the precomputed `peer_*` bools, since the reactor owns peer state.
    async fn handle_frontier_reset(
        &mut self,
        frontiers: BlockSyncFrontiers,
        preserve_active_successors: bool,
        peer_has_successor_after: bool,
        peer_outstanding_conflicts_at_tip: bool,
    ) {
        let reset_tip_matches_local_work = !self.reset_tip_conflicts_with_local_work(
            &frontiers,
            frontiers.verified_block_tip <= self.sequencer.floor(),
            peer_outstanding_conflicts_at_tip,
        );

        // State can report a forward `Reset` while checkpoint commits advance
        // under active successor bodies. Preserve them only when the caller knows
        // their anchor is unchanged; header reanchors can carry coalesced body
        // growth while those successors belong to the old fork.
        if preserve_active_successors
            && frontiers.verified_block_tip > self.sequencer.verified_tip()
            && (frontiers.verified_block_tip <= self.sequencer.floor()
                || self.has_active_successor_after(
                    frontiers.verified_block_tip,
                    peer_has_successor_after,
                ))
            && reset_tip_matches_local_work
        {
            self.trace_frontier_reset_classified(
                "growth",
                &frontiers,
                preserve_active_successors,
                peer_has_successor_after,
                peer_outstanding_conflicts_at_tip,
                reset_tip_matches_local_work,
            );
            // Growth-classified reset: treat it as a frontier advance, releasing
            // applied bodies.
            self.handle_frontier_advance(frontiers, true).await;
            return;
        }

        metrics::counter!("sync.block.reorg.reset").increment(1);

        // A `Reset` can also be a stale or coalesced state update for a tip
        // already inside our contiguous submitted/downloaded body floor. Do not
        // destructively clear successor bodies in that case: a stale reset
        // snapshot can otherwise erase `applying`/covered state and re-request
        // the same bodies while their first apply is still in flight.
        if preserve_active_successors
            && frontiers.verified_block_tip < self.sequencer.floor()
            && reset_tip_matches_local_work
            && self
                .has_active_successor_after(frontiers.verified_block_tip, peer_has_successor_after)
            && self.active_successor_links_to_anchor(
                frontiers.verified_block_tip,
                frontiers.verified_block_hash,
            )
        {
            self.trace_frontier_reset_classified(
                "preserved_stale",
                &frontiers,
                preserve_active_successors,
                peer_has_successor_after,
                peer_outstanding_conflicts_at_tip,
                reset_tip_matches_local_work,
            );
            self.handle_frontier_advance(frontiers, true).await;
            return;
        }

        self.trace_frontier_reset_classified(
            "destructive",
            &frontiers,
            preserve_active_successors,
            peer_has_successor_after,
            peer_outstanding_conflicts_at_tip,
            reset_tip_matches_local_work,
        );
        let remember_released_applies = frontiers.verified_block_tip > frontiers.finalized_height
            && frontiers.verified_block_tip <= self.sequencer.floor();

        self.finalized_height = frontiers.finalized_height;
        self.verified_block_hash = frontiers.verified_block_hash;

        // Retained bodies do not charge the request budget.
        let _ = self
            .sequencer
            .reset_to(frontiers.verified_block_tip, remember_released_applies);
        // Return unreceived request reservations above the reset target.
        let released = self.work.reset_above(self.sequencer.floor());
        self.budget.release(released);
        // A destructive reset: bump the epoch so the reactor drops *all*
        // outstanding requests (not just those through the tip).
        self.reset_epoch = self.reset_epoch.saturating_add(1);
    }

    /// Handle a verifier apply completion: release its verifier slot, fold in
    /// any embedded `local_frontier` as a frontier advance with
    /// `release_applied: false`, and on a rejection roll the floor back below the
    /// bad block so its range is re-requestable. Returns whether the reactor needs
    /// its serving/query/schedule reaction (the view reaction runs that tail).
    async fn handle_apply_finished(
        &mut self,
        token: BlockApplyToken,
        height: block::Height,
        hash: block::Hash,
        result: BlockApplyResult,
        local_frontier: Option<BlockSyncFrontiers>,
    ) -> bool {
        // A stale completion (no live applying entry, or token/hash mismatch)
        // releases only its exact token-aware in-flight-submission charge and
        // returns; there is no query/schedule tail here, so it needs no reaction.
        let Some((applying_token, applying_hash)) = self.sequencer.applying_token_hash(height)
        else {
            self.sequencer.finish_submission(token, height, hash);
            return false;
        };
        if applying_hash != hash || applying_token != token {
            self.sequencer.finish_submission(token, height, hash);
            return false;
        }

        let accepted_local_frontier = if let Some(frontiers) = local_frontier {
            // Fold the `local_frontier` advance in as a frontier advance without
            // releasing committed applying bodies (`release_applied: false`). It is
            // accepted only when it is not a stale (older-tip) update.
            if frontiers.verified_block_tip < self.sequencer.verified_tip() {
                None
            } else {
                self.handle_frontier_advance(frontiers, false).await;
                Some(frontiers)
            }
        } else {
            None
        };

        if matches!(result, BlockApplyResult::Duplicate) && self.sequencer.verified_tip() < height {
            // Stale duplicate for a height we have not verified to: the reactor
            // needs the serving/query tail only when the accepted local frontier
            // actually advanced serving. The body stays attached until a later
            // frontier update removes it, but the driver has released its decoded
            // copy, so release the token-aware decode-window charge now.
            self.sequencer
                .finish_attached_submission(token, height, hash);
            return accepted_local_frontier.is_some();
        }
        let applying = self
            .sequencer
            .remove_applying(height)
            .expect("applying entry exists because it was just checked");

        // A `Committed` result is a body that newly extended the chain; count it
        // toward commit throughput (the apply rate the download path is racing).
        if matches!(result, BlockApplyResult::Committed) {
            self.committed_throughput.record(applying.bytes);
        }
        self.sequencer.finish_submission(token, height, hash);
        match result {
            BlockApplyResult::Committed | BlockApplyResult::Duplicate => {}
            BlockApplyResult::Rejected | BlockApplyResult::TimedOut
                if height > self.sequencer.verified_tip() =>
            {
                // Drop the rejected body and every successor (in applying and
                // reorder), roll the floor back below it, and drop the WorkQueue
                // entries above the rolled-back floor so the heights are
                // re-requestable (the reactor's `query_needed_blocks` re-fills).
                let _ = self.sequencer.release_applying_blocks_from(height);
                self.sequencer.reset_floor_below(height);
                let released = self.work.reset_above(self.sequencer.floor());
                self.budget.release(released);
                let _ = self.sequencer.drop_reorder_from(height);
                // A `Rejected` result means consensus found the body invalid.
                // Attribute it to the delivering peer so repeat offenders are
                // scored and eventually disconnected. `TimedOut` is a local apply
                // timeout, not a peer fault, so it is not scored.
                if matches!(result, BlockApplyResult::Rejected) {
                    self.send_action(BlockSyncAction::Misbehavior {
                        peer: applying.source_peer.clone(),
                        reason: BlockSyncMisbehavior::InvalidBlock,
                    })
                    .await;
                }
            }
            BlockApplyResult::Rejected | BlockApplyResult::TimedOut => {}
        }
        if let Some(frontiers) = accepted_local_frontier {
            let _ = self
                .sequencer
                .release_applied_through(frontiers.verified_block_tip);
        }

        self.release_contiguous_blocks().await;
        true
    }

    /// Drain the contiguous reorder prefix into applying, then submit it.
    async fn release_contiguous_blocks(&mut self) {
        let _ = self.sequencer.drain_ready_into_applying();
        self.submit_pending_blocks().await;
    }

    async fn submit_pending_blocks(&mut self) {
        let submittable_heights = self.sequencer.submittable_heights();
        if submittable_heights.is_empty() {
            if self.sequencer.unsubmitted_applying_count() == 0 {
                self.submission_retry_at = None;
                self.submission_retry_started_at = None;
                self.submission_retry_attempt = 0;
            } else if self.submission_retry_started_at.is_some()
                && self.submission_retry_at.is_none()
                && !self.actions.is_closed()
            {
                self.submission_retry_at = Some(time::Instant::now() + SUBMISSION_RETRY_DELAY);
            }
            return;
        }

        self.submission_retry_at = None;
        for height in submittable_heights {
            let Some(item) = self.sequencer.prepare_submit(height) else {
                continue;
            };

            metrics::counter!("sync.block.submit.sent").increment(1);
            let queue_depth = self
                .actions
                .max_capacity()
                .saturating_sub(self.actions.capacity());
            // Metrics accepts f64 samples; this lossy conversion is observability-only.
            metrics::histogram!(
                "sync.block.action.queue.depth",
                "action" => "submit_block"
            )
            .record(queue_depth as f64);
            let send_started = time::Instant::now();
            let sent = self
                .send_action(BlockSyncAction::SubmitBlock {
                    token: item.token,
                    block: item.block,
                })
                .await;
            metrics::histogram!("sync.block.submit.queue_wait_seconds")
                .record(send_started.elapsed().as_secs_f64());
            if !sent {
                self.sequencer.unsubmit(item.height, item.token);
                if !self.actions.is_closed() {
                    let now = time::Instant::now();
                    self.submission_retry_started_at.get_or_insert(now);
                    self.submission_retry_attempt = self.submission_retry_attempt.saturating_add(1);
                    self.submission_retry_at = Some(now + SUBMISSION_RETRY_DELAY);
                    metrics::counter!("sync.block.submit.retry.scheduled").increment(1);
                    self.trace_submission_retry_scheduled(item.height);
                }
                return;
            }
            if let Some(started_at) = self.submission_retry_started_at.take() {
                metrics::counter!("sync.block.submit.retry.succeeded").increment(1);
                metrics::histogram!("sync.block.submit.retry.delay_seconds")
                    .record(started_at.elapsed().as_secs_f64());
                self.submission_retry_attempt = 0;
            }
            self.sequencer
                .record_submitted_apply(item.height, item.hash);
            self.trace_body_submitted(item.height, item.token);
        }
    }

    fn trace_body_submitted(&self, height: block::Height, token: BlockApplyToken) {
        self.trace.emit_with(BLOCK_SYNC_TABLE, |row| {
            row.insert(
                bs_trace::EVENT.to_string(),
                serde_json::Value::String(bs_trace::BLOCK_BODY_SUBMITTED.to_string()),
            );
            bs_insert_height(row, bs_trace::HEIGHT, height);
            bs_insert_u64(row, bs_trace::APPLY_TOKEN, token);
        });
    }

    fn trace_submission_retry_scheduled(&self, height: block::Height) {
        self.trace.emit_with(BLOCK_SYNC_TABLE, |row| {
            bs_insert_str(
                row,
                bs_trace::EVENT,
                bs_trace::BLOCK_BODY_SUBMISSION_RETRY_SCHEDULED,
            );
            bs_insert_height(row, bs_trace::HEIGHT, height);
            bs_insert_u64(
                row,
                qs_trace::QUEUE_CAPACITY,
                u64::try_from(self.actions.capacity()).unwrap_or(u64::MAX),
            );
            bs_insert_u64(
                row,
                qs_trace::QUEUE_MAX_CAPACITY,
                u64::try_from(self.actions.max_capacity()).unwrap_or(u64::MAX),
            );
            bs_insert_u64(
                row,
                "in_flight_submission_count",
                u64::try_from(self.sequencer.in_flight_submission_count()).unwrap_or(u64::MAX),
            );
            bs_insert_u64(
                row,
                "unsubmitted_applying_count",
                u64::try_from(self.sequencer.unsubmitted_applying_count()).unwrap_or(u64::MAX),
            );
            bs_insert_u64(row, "retry_attempt", self.submission_retry_attempt);
        });
    }

    fn trace_body_accepted(&self, height: block::Height, queued_elapsed: Duration, outcome: &str) {
        self.trace.emit_with(BLOCK_SYNC_TABLE, |row| {
            row.insert(
                bs_trace::EVENT.to_string(),
                serde_json::Value::String(bs_trace::BLOCK_BODY_ACCEPTED.to_string()),
            );
            bs_insert_height(row, bs_trace::HEIGHT, height);
            bs_insert_u64(
                row,
                "sequencer_queue_elapsed_us",
                u64::try_from(queued_elapsed.as_micros()).unwrap_or(u64::MAX),
            );
            row.insert(
                bs_trace::RESULT.to_string(),
                serde_json::Value::String(outcome.to_string()),
            );
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn trace_frontier_reset_classified(
        &self,
        classification: &'static str,
        frontiers: &BlockSyncFrontiers,
        preserve_active_successors: bool,
        peer_has_successor_after: bool,
        peer_outstanding_conflicts_at_tip: bool,
        reset_tip_matches_local_work: bool,
    ) {
        self.trace.emit_with(BLOCK_SYNC_TABLE, |row| {
            bs_insert_str(
                row,
                bs_trace::EVENT,
                bs_trace::BLOCK_FRONTIER_RESET_CLASSIFIED,
            );
            bs_insert_str(row, bs_trace::RESULT, classification);
            bs_insert_height(
                row,
                bs_trace::VERIFIED_BLOCK_TIP,
                frontiers.verified_block_tip,
            );
            bs_insert_height(row, "previous_verified_tip", self.sequencer.verified_tip());
            bs_insert_height(row, "previous_download_floor", self.sequencer.floor());
            bs_insert_u64(
                row,
                "preserve_active_successors",
                u64::from(preserve_active_successors),
            );
            bs_insert_u64(
                row,
                "peer_has_successor_after",
                u64::from(peer_has_successor_after),
            );
            bs_insert_u64(
                row,
                "peer_outstanding_conflicts_at_tip",
                u64::from(peer_outstanding_conflicts_at_tip),
            );
            bs_insert_u64(
                row,
                "reset_tip_matches_local_work",
                u64::from(reset_tip_matches_local_work),
            );
            bs_insert_u64(
                row,
                "has_local_successor_after",
                u64::from(
                    next_height(frontiers.verified_block_tip)
                        .is_some_and(|next| self.sequencer.has_buffered_at_or_above(next)),
                ),
            );
        });
    }

    /// `reset_tip_conflicts_with_local_work`'s Sequencer-internal predicates,
    /// with the peer-outstanding clause supplied by the reactor.
    fn reset_tip_conflicts_with_local_work(
        &self,
        frontiers: &BlockSyncFrontiers,
        ignore_non_material_conflicts: bool,
        peer_outstanding_conflicts_at_tip: bool,
    ) -> bool {
        let height = frontiers.verified_block_tip;
        let hash = frontiers.verified_block_hash;

        if self
            .sequencer
            .reorder_hash(height)
            .is_some_and(|buffered_hash| buffered_hash != hash)
        {
            return true;
        }
        if self
            .sequencer
            .applying_hash(height)
            .is_some_and(|applying_hash| applying_hash != hash)
        {
            return true;
        }
        if !ignore_non_material_conflicts
            && self.sequencer.submitted_has_only_other_hashes(height, hash)
        {
            return true;
        }
        if !ignore_non_material_conflicts && peer_outstanding_conflicts_at_tip {
            return true;
        }
        false
    }

    fn has_active_successor_after(
        &self,
        height: block::Height,
        peer_has_successor_after: bool,
    ) -> bool {
        let Some(next) = next_height(height) else {
            return false;
        };

        self.sequencer.has_buffered_at_or_above(next) || peer_has_successor_after
    }

    fn active_successor_links_to_anchor(
        &self,
        height: block::Height,
        anchor_hash: block::Hash,
    ) -> bool {
        let Some(next) = next_height(height) else {
            return true;
        };

        direct_successor_links_to_anchor(
            self.sequencer.applying_previous_block_hash(next),
            anchor_hash,
        )
    }

    async fn send_action(&self, action: BlockSyncAction) -> bool {
        // `SubmitBlock` is the intended verifier-backpressure point: a slow
        // verifier blocks the task here, stopping it from draining `input`. The
        // timeout matches the reactor's `dispatch_action` so a permanently
        // stalled driver does not wedge the pipeline forever.
        let action_label = action.metric_label();
        match time::timeout(self.action_send_timeout, self.actions.send(action)).await {
            Ok(Ok(())) => true,
            Ok(Err(_)) => false,
            Err(_) => {
                metrics::counter!(
                    "sync.block.action.send_timeout",
                    "action" => action_label
                )
                .increment(1);
                false
            }
        }
    }

    fn publish_view(&mut self) {
        self.committed_throughput.sample(Instant::now());
        let reorder_buffered_bytes = self.sequencer.reorder_buffered_bytes();
        let applying_buffered_bytes = self.sequencer.applying_buffered_bytes();
        let sequencer_input_decoded_attributed_memory_bytes = self
            .body_input_decoded_attributed_memory_bytes
            .load(std::sync::atomic::Ordering::Relaxed);
        let reorder_decoded_attributed_memory_bytes =
            self.sequencer.reorder_decoded_attributed_memory_bytes();
        let applying_decoded_attributed_memory_bytes =
            self.sequencer.applying_decoded_attributed_memory_bytes();
        let active_pipeline_decoded_attributed_memory_bytes =
            sequencer_input_decoded_attributed_memory_bytes
                .saturating_add(reorder_decoded_attributed_memory_bytes)
                .saturating_add(applying_decoded_attributed_memory_bytes);
        // Retained bodies do not charge the request budget.
        self.budget
            .audit(self.work.reserved_bytes(), "block-sync sequencer view");
        let next = SequencerView {
            verified_tip: self.sequencer.verified_tip(),
            verified_hash: self.verified_block_hash,
            download_floor: self.sequencer.floor(),
            finalized: self.finalized_height,
            reset_epoch: self.reset_epoch,
            reaction_epoch: self.reaction_epoch,
            reorder_len: self.sequencer.reorder_len() as u64,
            applying_len: self.sequencer.applying_len() as u64,
            reorder_buffered_bytes,
            applying_buffered_bytes,
            sequencer_input_decoded_attributed_memory_bytes,
            reorder_decoded_attributed_memory_bytes,
            applying_decoded_attributed_memory_bytes,
            active_pipeline_decoded_attributed_memory_bytes,
            unsubmitted_applying_count: self.sequencer.unsubmitted_applying_count() as u64,
            in_flight_submission_count: self.sequencer.in_flight_submission_count() as u64,
            in_flight_submission_bytes: self.sequencer.in_flight_submission_bytes(),
            committed_bytes_per_sec: self.committed_throughput.bytes_per_sec(),
            committed_blocks_per_sec: self.committed_throughput.blocks_per_sec(),
        };
        // Only wake watchers (the reactor + every per-peer routine) when a field
        // they schedule against actually changed. The two committed_*_per_sec rates
        // are observability-only; without this guard a stale or duplicate
        // `ApplyFinished` input can publish an otherwise-identical view and re-wake
        // every routine's `sequencer_view.changed()` arm into an immediate refill
        // retry.
        // That is a timer-free reactor<->sequencer<->routine busy-spin: it wastes a
        // core (and starves progress under CI load) on a real clock and fully wedges
        // a `start_paused` test clock, which auto-advances only once every task
        // parks. Keep the stored rates fresh, but notify only on a schedulable change.
        publish_sequencer_view(&self.view_tx, next);
    }

    fn publish_decoded_ownership_view(&self) {
        let sequencer_input_decoded_attributed_memory_bytes = self
            .body_input_decoded_attributed_memory_bytes
            .load(std::sync::atomic::Ordering::Relaxed);
        let reorder_decoded_attributed_memory_bytes =
            self.sequencer.reorder_decoded_attributed_memory_bytes();
        let applying_decoded_attributed_memory_bytes =
            self.sequencer.applying_decoded_attributed_memory_bytes();
        let active_pipeline_decoded_attributed_memory_bytes =
            sequencer_input_decoded_attributed_memory_bytes
                .saturating_add(reorder_decoded_attributed_memory_bytes)
                .saturating_add(applying_decoded_attributed_memory_bytes);
        self.view_tx.send_if_modified(|view| {
            view.sequencer_input_decoded_attributed_memory_bytes =
                sequencer_input_decoded_attributed_memory_bytes;
            view.reorder_decoded_attributed_memory_bytes = reorder_decoded_attributed_memory_bytes;
            view.applying_decoded_attributed_memory_bytes =
                applying_decoded_attributed_memory_bytes;
            view.active_pipeline_decoded_attributed_memory_bytes =
                active_pipeline_decoded_attributed_memory_bytes;
            false
        });
    }
}

fn add_atomic_bytes(counter: &std::sync::atomic::AtomicU64, bytes: u64) {
    let mut current = counter.load(std::sync::atomic::Ordering::Relaxed);
    loop {
        let next = current.saturating_add(bytes);
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

fn release_atomic_bytes(counter: &std::sync::atomic::AtomicU64, bytes: u64) {
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

fn direct_successor_links_to_anchor(
    previous_block_hash: Option<block::Hash>,
    anchor_hash: block::Hash,
) -> bool {
    previous_block_hash.is_some_and(|hash| hash == anchor_hash)
}

fn publish_sequencer_view(view_tx: &watch::Sender<SequencerView>, next: SequencerView) {
    view_tx.send_if_modified(|current| {
        let schedulable_changed = view_schedulable_ne(current, &next);
        *current = next;
        schedulable_changed
    });
}

/// True when two views differ in any field the reactor or per-peer routines
/// schedule against. Ignores the observability-only committed throughput rates,
/// which move on nearly every sample and must not, on their own, wake — or under a
/// paused test clock, spin — the whole fleet of watchers.
fn view_schedulable_ne(a: &SequencerView, b: &SequencerView) -> bool {
    let strip_rates = |v: &SequencerView| {
        let mut v = *v;
        v.committed_bytes_per_sec = 0;
        v.committed_blocks_per_sec = 0;
        v.sequencer_input_decoded_attributed_memory_bytes = 0;
        v.reorder_decoded_attributed_memory_bytes = 0;
        v.applying_decoded_attributed_memory_bytes = 0;
        v.active_pipeline_decoded_attributed_memory_bytes = 0;
        v
    };
    strip_rates(a) != strip_rates(b)
}

#[cfg(test)]
mod tests {
    use zakura_chain::serialization::ZcashDeserializeInto;
    use zakura_test::vectors::BLOCK_MAINNET_1_BYTES;

    use super::*;

    #[test]
    fn missing_direct_successor_cannot_prove_reset_anchor() {
        let anchor_hash = block::Hash([1; 32]);

        assert!(direct_successor_links_to_anchor(
            Some(anchor_hash),
            anchor_hash
        ));
        assert!(!direct_successor_links_to_anchor(
            Some(block::Hash([2; 32])),
            anchor_hash
        ));
        assert!(!direct_successor_links_to_anchor(None, anchor_hash));
    }

    fn test_view() -> SequencerView {
        SequencerView {
            verified_tip: block::Height(1),
            verified_hash: block::Hash([1; 32]),
            download_floor: block::Height(1),
            finalized: block::Height(1),
            reset_epoch: 0,
            reaction_epoch: 0,
            reorder_len: 0,
            applying_len: 0,
            reorder_buffered_bytes: 0,
            applying_buffered_bytes: 0,
            sequencer_input_decoded_attributed_memory_bytes: 0,
            reorder_decoded_attributed_memory_bytes: 0,
            applying_decoded_attributed_memory_bytes: 0,
            active_pipeline_decoded_attributed_memory_bytes: 0,
            unsubmitted_applying_count: 0,
            in_flight_submission_count: 0,
            in_flight_submission_bytes: 0,
            committed_bytes_per_sec: 0,
            committed_blocks_per_sec: 0,
        }
    }

    fn test_block() -> Arc<block::Block> {
        Arc::new(
            BLOCK_MAINNET_1_BYTES
                .zcash_deserialize_into()
                .expect("block test vector parses"),
        )
    }

    fn queued_test_body(
        input_bytes: Arc<std::sync::atomic::AtomicU64>,
        input_decoded_attributed_memory_bytes: Arc<std::sync::atomic::AtomicU64>,
    ) -> SequencedBody {
        let block = test_block();
        let previous_block_hash = block.header.previous_block_hash;
        SequencedBody::new_queued(
            block::Height(1),
            block.hash(),
            previous_block_hash,
            BufferedBlockBody::from_decoded_block(block, None),
            123,
            ZakuraPeerId::new(vec![1; 32]).expect("test peer id is valid"),
            Instant::now(),
            input_bytes,
            input_decoded_attributed_memory_bytes,
        )
    }

    #[test]
    fn sequenced_body_leave_and_drop_release_queue_counters_once() {
        let input_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let input_decoded_attributed_memory_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut body = queued_test_body(
            input_bytes.clone(),
            input_decoded_attributed_memory_bytes.clone(),
        );

        assert_eq!(input_bytes.load(std::sync::atomic::Ordering::Relaxed), 123);
        assert!(
            input_decoded_attributed_memory_bytes.load(std::sync::atomic::Ordering::Relaxed) > 0
        );

        body.leave_queue();
        assert_eq!(input_bytes.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert_eq!(
            input_decoded_attributed_memory_bytes.load(std::sync::atomic::Ordering::Relaxed),
            0
        );

        drop(body);
        assert_eq!(input_bytes.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn cancelled_send_drops_its_queue_accounting() {
        let input_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let input_decoded_attributed_memory_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (body_tx, mut body_rx) = mpsc::channel(1);
        body_tx
            .try_send(queued_test_body(
                input_bytes.clone(),
                input_decoded_attributed_memory_bytes.clone(),
            ))
            .expect("body channel has capacity");
        let blocked_body = queued_test_body(
            input_bytes.clone(),
            input_decoded_attributed_memory_bytes.clone(),
        );
        let blocked_send = tokio::spawn(async move { body_tx.send(blocked_body).await });
        tokio::task::yield_now().await;
        assert!(!blocked_send.is_finished());

        blocked_send.abort();
        let _ = blocked_send.await;
        assert_eq!(
            input_bytes.load(std::sync::atomic::Ordering::Relaxed),
            123,
            "only the body already queued remains charged"
        );

        let mut queued = body_rx.recv().await.expect("first body remains queued");
        queued.leave_queue();
        assert_eq!(input_bytes.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert_eq!(
            input_decoded_attributed_memory_bytes.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn closed_receiver_with_live_permit_drops_queue_accounting() {
        let input_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let input_decoded_attributed_memory_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (body_tx, body_rx) = mpsc::channel(1);
        let permit = body_tx
            .reserve_owned()
            .await
            .expect("receiver is initially open");
        drop(body_rx);

        permit.send(queued_test_body(
            input_bytes.clone(),
            input_decoded_attributed_memory_bytes.clone(),
        ));

        assert_eq!(input_bytes.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert_eq!(
            input_decoded_attributed_memory_bytes.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[tokio::test(start_paused = true)]
    async fn submission_retries_after_action_channel_capacity_returns() {
        let frontiers = BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        };
        let body_input_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let body_input_decoded_attributed_memory_bytes =
            Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (body_tx, body_rx) = mpsc::channel(1);
        let (_control_tx, control_rx) = mpsc::unbounded_channel();
        let (actions, mut actions_rx) = mpsc::channel(1);
        actions
            .try_send(BlockSyncAction::QueryNeededBlocks {
                from: block::Height(1),
                limit: 1,
                best_header_tip: block::Height(1),
            })
            .expect("test fills the action channel");
        let (view_tx, mut view_rx) = watch::channel(initial_view(frontiers));
        let task = SequencerTask::new(
            Sequencer::new(block::Height(0), 1),
            ByteBudget::new(123),
            Arc::new(WorkQueue::new(block::Height(0))),
            actions,
            ThroughputMeter::new(Instant::now()),
            frontiers,
            body_rx,
            control_rx,
            body_input_bytes.clone(),
            body_input_decoded_attributed_memory_bytes.clone(),
            view_tx,
            Duration::from_secs(1),
            ZakuraTrace::noop(),
        );
        let task = tokio::spawn(task.run());

        body_tx
            .send(queued_test_body(
                body_input_bytes,
                body_input_decoded_attributed_memory_bytes,
            ))
            .await
            .expect("body queues");

        time::timeout(Duration::from_secs(2), async {
            while view_rx.borrow_and_update().unsubmitted_applying_count == 0 {
                view_rx
                    .changed()
                    .await
                    .expect("sequencer view remains live");
            }
        })
        .await
        .expect("initial submission times out");

        assert!(matches!(
            actions_rx.recv().await,
            Some(BlockSyncAction::QueryNeededBlocks { .. })
        ));

        let retried = time::timeout(Duration::from_secs(1), actions_rx.recv())
            .await
            .expect("submission is retried after capacity returns")
            .expect("action channel remains live");
        assert!(matches!(retried, BlockSyncAction::SubmitBlock { .. }));

        task.abort();
    }

    #[test]
    fn handoff_publishes_applying_decoded_bytes_before_submission() {
        let frontiers = BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        };
        let body_input_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let body_input_decoded_attributed_memory_bytes =
            Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut body = queued_test_body(
            body_input_bytes.clone(),
            body_input_decoded_attributed_memory_bytes.clone(),
        );
        let (_body_tx, body_rx) = mpsc::channel(1);
        let (_control_tx, control_rx) = mpsc::unbounded_channel();
        let (actions, _actions_rx) = mpsc::channel(1);
        let (view_tx, view_rx) = watch::channel(initial_view(frontiers));
        let mut task = SequencerTask::new(
            Sequencer::new(block::Height(0), 1),
            ByteBudget::new(123),
            Arc::new(WorkQueue::new(block::Height(0))),
            actions,
            ThroughputMeter::new(Instant::now()),
            frontiers,
            body_rx,
            control_rx,
            body_input_bytes,
            body_input_decoded_attributed_memory_bytes,
            view_tx,
            Duration::from_secs(60),
            ZakuraTrace::noop(),
        );

        body.leave_queue();
        task.handle_accept_body(body);
        task.publish_decoded_ownership_view();

        let handoff = *view_rx.borrow();
        assert_eq!(handoff.sequencer_input_decoded_attributed_memory_bytes, 0);
        assert!(handoff.applying_decoded_attributed_memory_bytes > 0);
        assert_eq!(handoff.reorder_decoded_attributed_memory_bytes, 0);
        assert_eq!(
            handoff.active_pipeline_decoded_attributed_memory_bytes,
            handoff.applying_decoded_attributed_memory_bytes
        );
    }

    #[test]
    fn dropping_task_releases_queue_and_publishes_terminal_decoded_view() {
        let frontiers = BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        };
        let (body_tx, body_rx) = mpsc::channel(1);
        let body_input_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let body_input_decoded_attributed_memory_bytes =
            Arc::new(std::sync::atomic::AtomicU64::new(0));
        body_tx
            .try_send(queued_test_body(
                body_input_bytes.clone(),
                body_input_decoded_attributed_memory_bytes.clone(),
            ))
            .expect("body channel has capacity");
        let (_control_tx, control_rx) = mpsc::unbounded_channel();
        let (actions, _actions_rx) = mpsc::channel(1);
        let mut initial = initial_view(frontiers);
        initial.reorder_decoded_attributed_memory_bytes = 10;
        initial.applying_decoded_attributed_memory_bytes = 20;
        initial.active_pipeline_decoded_attributed_memory_bytes =
            body_input_decoded_attributed_memory_bytes
                .load(std::sync::atomic::Ordering::Relaxed)
                .saturating_add(30);
        let (view_tx, view_rx) = watch::channel(initial);
        let task = SequencerTask::new(
            Sequencer::new(block::Height(0), 1),
            ByteBudget::new(1),
            Arc::new(WorkQueue::new(block::Height(0))),
            actions,
            ThroughputMeter::new(Instant::now()),
            frontiers,
            body_rx,
            control_rx,
            body_input_bytes.clone(),
            body_input_decoded_attributed_memory_bytes.clone(),
            view_tx,
            Duration::from_secs(1),
            ZakuraTrace::noop(),
        );

        drop(task);

        assert_eq!(
            body_input_bytes.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert_eq!(
            body_input_decoded_attributed_memory_bytes.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        let terminal = *view_rx.borrow();
        assert_eq!(terminal.sequencer_input_decoded_attributed_memory_bytes, 0);
        assert_eq!(terminal.reorder_decoded_attributed_memory_bytes, 0);
        assert_eq!(terminal.applying_decoded_attributed_memory_bytes, 0);
        assert_eq!(terminal.active_pipeline_decoded_attributed_memory_bytes, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn sequencer_view_rate_refresh_does_not_wake_watchers() {
        let initial = test_view();
        let (view_tx, mut view_rx) = watch::channel(initial);

        let rate_only = SequencerView {
            committed_bytes_per_sec: 1024,
            committed_blocks_per_sec: 3,
            ..initial
        };
        publish_sequencer_view(&view_tx, rate_only);

        assert_eq!(*view_rx.borrow(), rate_only);
        assert!(
            time::timeout(Duration::from_millis(1), view_rx.changed())
                .await
                .is_err(),
            "throughput-only view refresh must not wake watchers"
        );

        let schedulable = SequencerView {
            reaction_epoch: 1,
            ..rate_only
        };
        publish_sequencer_view(&view_tx, schedulable);

        view_rx
            .changed()
            .await
            .expect("sequencer view sender is still live");
        assert_eq!(*view_rx.borrow(), schedulable);
    }
}
