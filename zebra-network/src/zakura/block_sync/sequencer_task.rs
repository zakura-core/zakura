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

use super::{
    events::*,
    reactor::{bs_insert_height, bs_insert_u64},
    reorder::BufferedBlockBody,
    sequencer::*,
    state::*,
    work_queue::WorkQueue,
    *,
};

/// Favor the lowest needed height over the speculative high tail.
///
/// While the byte budget cannot fund even one worst-case request yet the lowest
/// needed height (pending or outstanding) sits *below* the highest buffered body,
/// drop that top body: release its bytes to the budget and return its height to
/// `pending` (it was held, hence in `work.in_flight` per the `held ⟺ in_flight`
/// invariant) for later re-fetch. Because another top can always be shed, a low
/// retry never blocks on budget; the floor can never wedge behind a full buffer,
/// and under a stall the speculative tail is shed and the chain fills bottom-up,
/// which also bounds the reorder backlog. The rescue path is purely demand-driven:
/// it runs inline after each accepted body and synchronously when a floor requester
/// needs budget through [`SequencerControlInput::FundFloorReservation`]. Returns
/// whether it shed anything.
pub(super) fn shed_top_until_available(
    budget: &mut ByteBudget,
    work: &WorkQueue,
    sequencer: &mut Sequencer,
    target_available: u64,
) -> bool {
    let mut shed_any = false;
    while budget.available() < target_available {
        let lowest_needed = match (work.min_pending(), work.min_in_flight()) {
            (Some(pending), Some(in_flight)) => pending.min(in_flight),
            (Some(pending), None) => pending,
            (None, Some(in_flight)) => in_flight,
            (None, None) => break,
        };
        let Some(top) = sequencer.reorder_max_height() else {
            break;
        };
        // Only shed a body that sits above a starved lower height: we trade a
        // far-from-floor body for the ability to fetch a nearer, higher-value one.
        if lowest_needed >= top {
            break;
        }
        let freed = sequencer.drop_reorder_from(top);
        if freed == 0 {
            break;
        }
        let released = work.release_and_return_items([top]);
        debug_assert!(
            released == 0 || released == freed,
            "shed reorder release must match the per-height budget ledger when present"
        );
        budget.release(if released == 0 { freed } else { released });
        shed_any = true;
    }
    shed_any
}

pub(super) fn shed_top_for_floor_starvation(
    budget: &mut ByteBudget,
    work: &WorkQueue,
    sequencer: &mut Sequencer,
) -> bool {
    shed_top_until_available(
        budget,
        work,
        sequencer,
        super::config::BS_PER_BLOCK_WORST_CASE_BYTES,
    )
}

/// A received body a peer routine matched (or accepted unmatched) and forwards
/// to the commit pipeline. This is the only bounded Sequencer input: a slow
/// verifier can backpressure body intake, but must not block apply/frontier
/// control events that release budget and drive the next scheduling reaction.
#[derive(Clone, Debug)]
pub(super) struct SequencedBody {
    pub(super) height: block::Height,
    pub(super) hash: block::Hash,
    pub(super) body: BufferedBlockBody,
    pub(super) bytes: u64,
    pub(super) peer: ZakuraPeerId,
    pub(super) received_at: Instant,
}

/// Progress-critical Sequencer events forwarded by the reactor.
///
/// These events must not sit behind downloaded bodies. `ApplyFinished` releases
/// budget and verifier slots, and frontier/reset events can release or discard
/// stale body work. They are locally generated and tiny, so they use a separate
/// unbounded channel and are prioritized by the Sequencer task.
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
    /// Synchronously pop the speculative high tail until a floor request can
    /// reserve `needed_bytes`, then wake the requester to retry the reservation.
    FundFloorReservation {
        needed_bytes: u64,
        reply: oneshot::Sender<bool>,
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
    pub(super) unsubmitted_applying_count: u64,
    pub(super) submitted_applying_count: u64,
    pub(super) submitted_applying_bytes: u64,
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
        unsubmitted_applying_count: 0,
        submitted_applying_count: 0,
        submitted_applying_bytes: 0,
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
    body_input_bytes: Arc<std::sync::atomic::AtomicU64>,
    view_tx: watch::Sender<SequencerView>,
    action_send_timeout: Duration,
    trace: ZakuraTrace,
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
            body_input_bytes,
            view_tx,
            action_send_timeout,
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
            if !control_open && !body_open {
                break;
            }
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

                body = self.body_input_rx.recv(), if body_open => {
                    match body {
                        Some(body) => {
                            self.release_body_input_bytes(body.bytes);
                            self.handle_accept_body(body).await;
                            shed_top_for_floor_starvation(
                                &mut self.budget,
                                &self.work,
                                &mut self.sequencer,
                            );
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
                self.handle_apply_finished(token, height, hash, result, local_frontier)
                    .await
            }
            SequencerControlInput::FundFloorReservation {
                needed_bytes,
                reply,
            } => {
                let shed = shed_top_until_available(
                    &mut self.budget,
                    &self.work,
                    &mut self.sequencer,
                    needed_bytes,
                );
                let _ = reply.send(self.budget.available() >= needed_bytes);
                shed
            }
        }
    }

    fn release_body_input_bytes(&self, bytes: u64) {
        let mut current = self
            .body_input_bytes
            .load(std::sync::atomic::Ordering::Relaxed);
        loop {
            let next = current.saturating_sub(bytes);
            match self.body_input_bytes.compare_exchange_weak(
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

    /// Body-acceptance tail: offer the body to the reorder buffer, release its
    /// bytes on a `Redundant` outcome, then drain the ready contiguous prefix into
    /// applying and submit it.
    async fn handle_accept_body(&mut self, body: SequencedBody) {
        let queued_elapsed = body.received_at.elapsed();
        let outcome = match self.sequencer.accept_buffered_body(
            body.height,
            body.hash,
            body.body,
            body.bytes,
            body.peer,
        ) {
            AcceptOutcome::Buffered { .. } => "buffered",
            AcceptOutcome::Redundant { release_bytes } => {
                self.budget.release(release_bytes);
                "redundant"
            }
        };
        self.trace_body_accepted(body.height, queued_elapsed, outcome);
        self.release_contiguous_blocks().await;
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
        self.budget.release(advance.release_bytes);
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
        // under already-submitted or still-downloading successor bodies. Treat
        // that as verified growth once it is inside our submitted/downloaded
        // floor, or when we already have successor work in flight. Keep fork
        // resets destructive when they are not anchored by active successor
        // work.
        if frontiers.verified_block_tip > self.sequencer.verified_tip()
            && (frontiers.verified_block_tip <= self.sequencer.floor()
                || self.has_active_successor_after(
                    frontiers.verified_block_tip,
                    peer_has_successor_after,
                ))
            && reset_tip_matches_local_work
        {
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
            self.handle_frontier_advance(frontiers, true).await;
            return;
        }

        let remember_released_applies = frontiers.verified_block_tip > frontiers.finalized_height
            && frontiers.verified_block_tip <= self.sequencer.floor();

        self.finalized_height = frontiers.finalized_height;
        self.verified_block_hash = frontiers.verified_block_hash;

        // The Sequencer pins its verified tip and floor to the reset target and
        // clears the reorder/applying buffers, returning the freed bytes for
        // release.
        let released = self
            .sequencer
            .reset_to(frontiers.verified_block_tip, remember_released_applies);
        self.budget.release(released);
        // Drop every download work item above the reset target (their buffers
        // were cleared by `reset_to`); the reactor's `query_needed_blocks`
        // re-fills.
        let released = self.work.reset_above(self.sequencer.floor());
        self.budget.release(released);
        // A destructive reset: bump the epoch so the reactor drops *all*
        // outstanding requests (not just those through the tip).
        self.reset_epoch = self.reset_epoch.saturating_add(1);
    }

    /// Handle a verifier apply completion: release the body's bytes and verifier
    /// slot, fold in any embedded `local_frontier` as a frontier advance with
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
        // only decrements the submitted-apply record and returns; there is no
        // query/schedule tail here, so it needs no reaction.
        let Some((applying_token, applying_hash)) = self.sequencer.applying_token_hash(height)
        else {
            self.sequencer.decrement_submitted_apply(height, hash);
            return false;
        };
        if applying_hash != hash || applying_token != token {
            self.sequencer.decrement_submitted_apply(height, hash);
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
            // actually advanced serving.
            return accepted_local_frontier.is_some();
        }
        let applying = self
            .sequencer
            .remove_applying(height)
            .expect("applying entry exists because it was just checked");

        self.budget.release(applying.bytes);
        // A `Committed` result is a body that newly extended the chain; count it
        // toward commit throughput (the apply rate the download path is racing).
        if matches!(result, BlockApplyResult::Committed) {
            self.committed_throughput.record(applying.bytes);
        }
        self.sequencer.decrement_submitted_apply(height, hash);
        match result {
            BlockApplyResult::Committed | BlockApplyResult::Duplicate => {}
            BlockApplyResult::Rejected | BlockApplyResult::TimedOut
                if height > self.sequencer.verified_tip() =>
            {
                // Drop the rejected body and every successor (in applying and
                // reorder), roll the floor back below it, and drop the WorkQueue
                // entries above the rolled-back floor so the heights are
                // re-requestable (the reactor's `query_needed_blocks` re-fills).
                let released = self.sequencer.release_applying_blocks_from(height);
                self.budget.release(released);
                self.sequencer.reset_floor_below(height);
                let released = self.work.reset_above(self.sequencer.floor());
                self.budget.release(released);
                let dropped = self.sequencer.drop_reorder_from(height);
                self.budget.release(dropped);
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
            let released = self
                .sequencer
                .release_applied_through(frontiers.verified_block_tip);
            self.budget.release(released);
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
        for height in self.sequencer.submittable_heights() {
            let Some(item) = self.sequencer.prepare_submit(height) else {
                continue;
            };

            metrics::counter!("sync.block.submit.sent").increment(1);
            if !self
                .send_action(BlockSyncAction::SubmitBlock {
                    token: item.token,
                    block: item.block,
                })
                .await
            {
                self.sequencer.unsubmit(item.height, item.token);
                return;
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

        self.sequencer
            .applying_previous_block_hash(next)
            .map(|previous_block_hash| previous_block_hash == anchor_hash)
            .unwrap_or(true)
    }

    async fn send_action(&self, action: BlockSyncAction) -> bool {
        // `SubmitBlock` is the intended verifier-backpressure point: a slow
        // verifier blocks the task here, stopping it from draining `input`. The
        // timeout matches the reactor's `dispatch_action` so a permanently
        // stalled driver does not wedge the pipeline forever.
        match time::timeout(self.action_send_timeout, self.actions.send(action)).await {
            Ok(Ok(())) => true,
            Ok(Err(_)) => false,
            Err(_) => {
                metrics::counter!("sync.block.action.send_timeout").increment(1);
                false
            }
        }
    }

    fn publish_view(&mut self) {
        self.committed_throughput.sample(Instant::now());
        let reorder_buffered_bytes = self.sequencer.reorder_buffered_bytes();
        let applying_buffered_bytes = self.sequencer.applying_buffered_bytes();
        let body_input_bytes = self
            .body_input_bytes
            .load(std::sync::atomic::Ordering::Relaxed);
        // Cross-layer drift check: the independently-maintained `ByteBudget` total
        // must equal the sum of the component counters. `work.reserved_bytes()` is
        // now an O(1) counter, so this runs on every event without a work-queue scan.
        let expected_budget = self
            .work
            .reserved_bytes()
            .saturating_add(reorder_buffered_bytes)
            .saturating_add(applying_buffered_bytes)
            .saturating_add(body_input_bytes);
        self.budget
            .audit(expected_budget, "block-sync sequencer view");
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
            unsubmitted_applying_count: self.sequencer.unsubmitted_applying_count() as u64,
            submitted_applying_count: self.sequencer.submitted_applying_count() as u64,
            submitted_applying_bytes: self.sequencer.submitted_applying_bytes(),
            committed_bytes_per_sec: self.committed_throughput.bytes_per_sec(),
            committed_blocks_per_sec: self.committed_throughput.blocks_per_sec(),
        };
        // Only wake watchers (the reactor + every per-peer routine) when a field
        // they schedule against actually changed. The two committed_*_per_sec rates
        // are observability-only; without this guard a no-op control input — e.g. a
        // `FundFloorReservation` that shed nothing while the byte budget is pinned —
        // still publishes an otherwise-identical view and re-wakes the requesting
        // routine's `sequencer_view.changed()` arm into an immediate refill retry.
        // That is a timer-free reactor<->sequencer<->routine busy-spin: it wastes a
        // core (and starves progress under CI load) on a real clock and fully wedges
        // a `start_paused` test clock, which auto-advances only once every task
        // parks. Keep the stored rates fresh, but notify only on a schedulable change.
        publish_sequencer_view(&self.view_tx, next);
    }
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
        v
    };
    strip_rates(a) != strip_rates(b)
}

#[cfg(test)]
mod tests {
    use super::*;

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
            unsubmitted_applying_count: 0,
            submitted_applying_count: 0,
            submitted_applying_bytes: 0,
            committed_bytes_per_sec: 0,
            committed_blocks_per_sec: 0,
        }
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
