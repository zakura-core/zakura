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
    reactor::{bs_insert_height, bs_insert_str, bs_insert_u64},
    reorder::BufferedBlockBody,
    sequencer::*,
    state::*,
    work_queue::WorkQueue,
    *,
};

#[cfg(test)]
use super::work_queue::{LateBodyClaim, ReservationOwner};
#[cfg(test)]
use zakura_chain::serialization::ZcashDeserializeInto;
#[cfg(test)]
use zakura_test::vectors::{BLOCK_MAINNET_1_BYTES, BLOCK_MAINNET_2_BYTES};

/// A received body a peer routine matched (or accepted unmatched) and forwards
/// to the commit pipeline. This is the only bounded Sequencer input: a slow
/// verifier can backpressure body intake, but must not block apply/frontier
/// control events that release budget and drive the next scheduling reaction.
#[derive(Clone, Debug)]
pub(super) struct SequencedBody {
    pub(super) height: block::Height,
    pub(super) hash: block::Hash,
    pub(super) previous_block_hash: block::Hash,
    pub(super) body: BufferedBlockBody,
    pub(super) bytes: u64,
    pub(super) peer: ZakuraPeerId,
    pub(super) received_at: Instant,
    /// Destructive-reset epoch under which the peer routine claimed this body's
    /// WorkQueue ownership. A reset can overtake the bounded body channel, so the
    /// Sequencer must reject bodies from an older ownership epoch.
    pub(super) reset_epoch: u64,
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
    /// A chain-tip reset (reorg/checkpoint/coalesced update). The peer conflict
    /// predicate is precomputed by the reactor, which owns peer state.
    FrontierReset {
        frontiers: BlockSyncFrontiers,
        preserve_active_successors: bool,
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
                peer_outstanding_conflicts_at_tip,
            } => {
                self.handle_frontier_reset(
                    frontiers,
                    preserve_active_successors,
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

    /// Buffer the body, then submit the ready contiguous prefix.
    async fn handle_accept_body(&mut self, body: SequencedBody) {
        let queued_elapsed = body.received_at.elapsed();
        if body.reset_epoch != self.reset_epoch {
            // A destructive reset cleared the WorkQueue claim and all successor
            // buffers before this body reached the front of the bounded channel.
            // Its held byte charge is intentionally not released by
            // `WorkQueue::reset_above`: ownership travelled with this queued body,
            // so release it here without touching replacement work at the same
            // height.
            self.budget.release(body.bytes);
            metrics::counter!("sync.block.body.stale_reset_epoch").increment(1);
            self.trace_body_accepted(body.height, queued_elapsed, "stale_reset_epoch");
            return;
        }
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
        if advance.changed {
            let released = self.work.advance_floor(frontiers.verified_block_tip);
            self.budget.release(released);
            self.release_contiguous_blocks().await;
        }
    }

    /// Handle a chain-tip reset: classify it as growth (treat as an advance) or a
    /// destructive reorg (pin tip/floor to the target, clear successor buffers, and
    /// bump the reset epoch). The peer-outstanding conflict predicate arrives
    /// precomputed because the reactor owns peer state.
    async fn handle_frontier_reset(
        &mut self,
        frontiers: BlockSyncFrontiers,
        preserve_active_successors: bool,
        peer_outstanding_conflicts_at_tip: bool,
    ) {
        let reset_tip_matches_local_work = !self.reset_tip_conflicts_with_local_work(
            &frontiers,
            frontiers.verified_block_tip <= self.sequencer.floor(),
            peer_outstanding_conflicts_at_tip,
        );

        // State can report a forward `Reset` while checkpoint commits advance
        // under an already-downloaded successor body. Preserve only when the
        // exact H+1 body proves it extends the reset hash; a floor position,
        // higher buffered body, or outstanding peer request does not prove
        // ancestry.
        if frontiers.verified_block_tip > self.sequencer.verified_tip()
            && self.exact_successor_links_to_anchor(
                frontiers.verified_block_tip,
                frontiers.verified_block_hash,
            )
            && reset_tip_matches_local_work
        {
            self.trace_frontier_reset_classified(
                "growth",
                &frontiers,
                preserve_active_successors,
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
            && self.exact_successor_links_to_anchor(
                frontiers.verified_block_tip,
                frontiers.verified_block_hash,
            )
        {
            self.trace_frontier_reset_classified(
                "preserved_stale",
                &frontiers,
                preserve_active_successors,
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
        // Remove obsolete ownership through the reset target, then drop
        // successors while advancing the claim epoch atomically.
        let released = self.work.advance_floor(self.sequencer.floor());
        self.budget.release(released);
        let (released, reset_epoch) = self.work.destructive_reset_above(self.sequencer.floor());
        self.budget.release(released);
        // Publish the epoch advanced atomically with clearing WorkQueue ownership,
        // so claims cannot observe a mixed old-owner/new-epoch state.
        self.reset_epoch = reset_epoch;
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
                let (released, reset_epoch) =
                    self.work.destructive_reset_above(self.sequencer.floor());
                self.budget.release(released);
                self.reset_epoch = reset_epoch;
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

    #[allow(clippy::too_many_arguments)]
    fn trace_frontier_reset_classified(
        &self,
        classification: &'static str,
        frontiers: &BlockSyncFrontiers,
        preserve_active_successors: bool,
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
                "exact_successor_links_to_anchor",
                u64::from(self.exact_successor_links_to_anchor(
                    frontiers.verified_block_tip,
                    frontiers.verified_block_hash,
                )),
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

    fn exact_successor_links_to_anchor(
        &self,
        height: block::Height,
        anchor_hash: block::Hash,
    ) -> bool {
        let Some(next) = next_height(height) else {
            return false;
        };

        self.sequencer.body_links_to_parent(next, anchor_hash)
            || self.work.held_successor_links_to(next, anchor_hash)
    }

    async fn send_action(&mut self, action: BlockSyncAction) -> bool {
        // `SubmitBlock` is the intended verifier-backpressure point: a slow
        // verifier blocks the task here, stopping it from draining `input`. The
        // timeout matches the reactor's `dispatch_action` so a permanently
        // stalled driver does not wedge the pipeline forever.
        //
        // Publish after any mutation that led to this action and before awaiting
        // channel capacity. In particular, a rejection clears applying/submission
        // ownership before reporting peer misbehavior; the reactor's admission
        // snapshot must not retain that stale ownership while this send blocks.
        self.publish_view();
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
}

#[cfg(test)]
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
        v
    };
    strip_rates(a) != strip_rates(b)
}

#[cfg(test)]
mod tests {
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
            unsubmitted_applying_count: 0,
            in_flight_submission_count: 0,
            in_flight_submission_bytes: 0,
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

    #[tokio::test]
    async fn destructive_reset_rejects_queued_body_from_old_epoch() {
        let startup_frontiers = BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        };
        let reset_frontiers = BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(1),
            verified_block_hash: block::Hash([9; 32]),
        };
        let height = block::Height(3);
        let hash = block::Hash([1; 32]);
        let mut budget = ByteBudget::new(1_024);
        let work = Arc::new(WorkQueue::new(block::Height(0)));
        work.set_estimate_floor_for_tests(1);
        assert_eq!(
            work.extend([(height, hash, BlockSizeEstimate::Advertised(64))]),
            1
        );
        let owner = ReservationOwner {
            generation: 1,
            request_token: 1,
        };
        assert_eq!(
            work.take_in_range_budgeted_owned(height, height, 1, u64::MAX, owner)
                .len(),
            1
        );
        assert_eq!(work.mark_reserved([height], owner), 64);
        assert!(budget.try_reserve(64));
        let claim = work.claim_late_body(height, hash, block::Hash([0; 32]), true);
        let LateBodyClaim::ReleasedReserved {
            released_bytes,
            reset_epoch,
        } = claim
        else {
            panic!("reserved body claim must release its request reservation");
        };
        budget.release(released_bytes);
        let budget_view = budget.clone();
        let work_view = Arc::clone(&work);
        let (actions_tx, _actions_rx) = mpsc::channel(1);
        let (_body_tx, body_rx) = mpsc::channel(1);
        let (_control_tx, control_rx) = mpsc::unbounded_channel();
        let body_input_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (view_tx, _view_rx) = watch::channel(initial_view(startup_frontiers));
        let mut task = SequencerTask::new(
            Sequencer::new(block::Height(0), 1),
            budget,
            work,
            actions_tx,
            ThroughputMeter::new(Instant::now()),
            startup_frontiers,
            body_rx,
            control_rx,
            body_input_bytes,
            view_tx,
            Duration::from_secs(1),
            ZakuraTrace::noop(),
        );

        task.handle_frontier_reset(reset_frontiers, false, false)
            .await;
        assert_eq!(task.reset_epoch, 1);
        assert!(
            !work_view.in_flight_contains(height),
            "destructive reset must remove the old held WorkQueue claim"
        );

        task.handle_accept_body(SequencedBody {
            height,
            hash,
            previous_block_hash: block::Hash([0; 32]),
            body: BufferedBlockBody::RawFramePayload(Arc::from([])),
            bytes: 64,
            peer: ZakuraPeerId::new(vec![1; 32]).expect("32-byte test peer id is valid"),
            received_at: Instant::now(),
            reset_epoch,
        })
        .await;

        assert_eq!(
            budget_view.reserved(),
            0,
            "stale queued body must release its held byte charge"
        );
        assert!(
            !task.sequencer.reorder_contains(height),
            "body claimed before the reset must not enter the new-fork reorder buffer"
        );

        let replacement_hash = block::Hash([2; 32]);
        assert_eq!(
            work_view.extend([(height, replacement_hash, BlockSizeEstimate::Advertised(64),)]),
            1
        );
        let replacement_claim = work_view.claim_late_body(
            height,
            replacement_hash,
            reset_frontiers.verified_block_hash,
            true,
        );
        let LateBodyClaim::ClaimedPending {
            reset_epoch: replacement_epoch,
        } = replacement_claim
        else {
            panic!("replacement work must be claimable after the reset");
        };
        assert_eq!(replacement_epoch, task.reset_epoch);

        task.handle_accept_body(SequencedBody {
            height,
            hash: replacement_hash,
            previous_block_hash: reset_frontiers.verified_block_hash,
            body: BufferedBlockBody::RawFramePayload(Arc::from([])),
            bytes: 64,
            peer: ZakuraPeerId::new(vec![2; 32]).expect("32-byte test peer id is valid"),
            received_at: Instant::now(),
            reset_epoch: replacement_epoch,
        })
        .await;

        assert_eq!(
            task.sequencer.reorder_hash(height),
            Some(replacement_hash),
            "a replacement claimed under the new epoch must enter the reorder pipeline"
        );
    }

    #[tokio::test]
    async fn apply_rejection_discards_queued_successor_from_old_epoch() {
        let predecessor: Arc<block::Block> = Arc::new(
            BLOCK_MAINNET_1_BYTES
                .zcash_deserialize_into()
                .expect("block 1 vector parses"),
        );
        let successor: Arc<block::Block> = Arc::new(
            BLOCK_MAINNET_2_BYTES
                .zcash_deserialize_into()
                .expect("block 2 vector parses"),
        );
        let predecessor_height = block::Height(1);
        let successor_height = block::Height(2);
        let peer = ZakuraPeerId::new(vec![3; 32]).expect("32-byte test peer id is valid");
        let frontiers = BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        };

        let budget = ByteBudget::new(1_024);
        let budget_view = budget.clone();
        let mut sequencer = Sequencer::new(block::Height(0), 2);
        assert_eq!(
            sequencer.accept_body(
                predecessor_height,
                predecessor.hash(),
                predecessor,
                64,
                peer.clone(),
            ),
            AcceptOutcome::Buffered {
                covered: predecessor_height
            }
        );
        assert_eq!(
            sequencer.drain_ready_into_applying(),
            vec![predecessor_height]
        );
        let predecessor_submit = sequencer
            .prepare_submit(predecessor_height)
            .expect("contiguous predecessor is submittable");
        sequencer.record_submitted_apply(predecessor_height, predecessor_submit.hash);

        let work = Arc::new(WorkQueue::new(block::Height(0)));
        work.set_estimate_floor_for_tests(1);
        assert_eq!(
            work.extend([(
                successor_height,
                successor.hash(),
                BlockSizeEstimate::Advertised(64),
            )]),
            1
        );
        assert_eq!(
            work.claim_late_body(
                successor_height,
                successor.hash(),
                predecessor_submit.hash,
                true,
            ),
            LateBodyClaim::ClaimedPending { reset_epoch: 0 }
        );

        let body_input_bytes = Arc::new(std::sync::atomic::AtomicU64::new(64));
        let (body_tx, body_rx) = mpsc::channel(1);
        body_tx
            .send(SequencedBody {
                height: successor_height,
                hash: successor.hash(),
                previous_block_hash: predecessor_submit.hash,
                body: BufferedBlockBody::Decoded(successor),
                bytes: 64,
                peer: peer.clone(),
                received_at: Instant::now(),
                reset_epoch: 0,
            })
            .await
            .expect("old-epoch successor queues before rejection");
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        control_tx
            .send(SequencerControlInput::ApplyFinished {
                token: predecessor_submit.token,
                height: predecessor_height,
                hash: predecessor_submit.hash,
                result: BlockApplyResult::Rejected,
                local_frontier: None,
            })
            .expect("predecessor rejection queues");
        drop(body_tx);
        drop(control_tx);

        let (actions_tx, mut actions_rx) = mpsc::channel(4);
        let (view_tx, view_rx) = watch::channel(initial_view(frontiers));
        let task = SequencerTask::new(
            sequencer,
            budget,
            work,
            actions_tx,
            ThroughputMeter::new(Instant::now()),
            frontiers,
            body_rx,
            control_rx,
            body_input_bytes,
            view_tx,
            Duration::from_secs(1),
            ZakuraTrace::noop(),
        );

        task.run().await;

        assert_eq!(budget_view.reserved(), 0);
        let final_view = *view_rx.borrow();
        assert_eq!(final_view.reset_epoch, 1);
        assert_eq!(final_view.reorder_len, 0);
        assert_eq!(final_view.applying_len, 0);
        assert_eq!(final_view.in_flight_submission_count, 0);
        assert!(matches!(
            actions_rx.recv().await,
            Some(BlockSyncAction::Misbehavior {
                peer: source,
                reason: BlockSyncMisbehavior::InvalidBlock,
            }) if source == peer
        ));
        assert!(
            actions_rx.recv().await.is_none(),
            "the stale successor must not be submitted after predecessor rollback"
        );
    }

    #[tokio::test]
    async fn blocked_rejection_action_publishes_released_ownership() {
        let block: Arc<block::Block> = Arc::new(
            BLOCK_MAINNET_1_BYTES
                .zcash_deserialize_into()
                .expect("block 1 vector parses"),
        );
        let height = block::Height(1);
        let hash = block.hash();
        let peer = ZakuraPeerId::new(vec![4; 32]).expect("32-byte test peer id is valid");
        let frontiers = BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        };

        let mut sequencer = Sequencer::new(block::Height(0), 1);
        assert_eq!(
            sequencer.accept_body(height, hash, block, 64, peer.clone()),
            AcceptOutcome::Buffered { covered: height }
        );
        assert_eq!(sequencer.drain_ready_into_applying(), vec![height]);
        let submission = sequencer
            .prepare_submit(height)
            .expect("contiguous block is submittable");
        sequencer.record_submitted_apply(height, submission.hash);

        let work = Arc::new(WorkQueue::new(block::Height(0)));
        let (actions_tx, mut actions_rx) = mpsc::channel(1);
        actions_tx
            .send(BlockSyncAction::Misbehavior {
                peer: peer.clone(),
                reason: BlockSyncMisbehavior::MalformedMessage,
            })
            .await
            .expect("test fills the action channel");
        let (body_tx, body_rx) = mpsc::channel(1);
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let body_input_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (view_tx, mut view_rx) = watch::channel(initial_view(frontiers));
        let mut task = SequencerTask::new(
            sequencer,
            ByteBudget::new(1_024),
            work,
            actions_tx,
            ThroughputMeter::new(Instant::now()),
            frontiers,
            body_rx,
            control_rx,
            body_input_bytes,
            view_tx,
            Duration::from_secs(60),
            ZakuraTrace::noop(),
        );
        task.publish_view();
        let initial_ownership = *view_rx.borrow_and_update();
        assert_eq!(initial_ownership.applying_len, 1);
        assert_eq!(initial_ownership.in_flight_submission_count, 1);

        control_tx
            .send(SequencerControlInput::ApplyFinished {
                token: submission.token,
                height,
                hash: submission.hash,
                result: BlockApplyResult::Rejected,
                local_frontier: None,
            })
            .expect("rejection control input queues");
        drop(control_tx);
        drop(body_tx);
        let task = tokio::spawn(task.run());

        time::timeout(Duration::from_secs(1), view_rx.changed())
            .await
            .expect("ownership view publishes while action send is blocked")
            .expect("sequencer view sender stays live");
        let blocked_view = *view_rx.borrow();
        assert_eq!(blocked_view.reset_epoch, 1);
        assert_eq!(blocked_view.applying_len, 0);
        assert_eq!(blocked_view.applying_buffered_bytes, 0);
        assert_eq!(blocked_view.in_flight_submission_count, 0);
        assert_eq!(blocked_view.in_flight_submission_bytes, 0);

        assert!(matches!(
            actions_rx.recv().await,
            Some(BlockSyncAction::Misbehavior {
                reason: BlockSyncMisbehavior::MalformedMessage,
                ..
            })
        ));
        assert!(matches!(
            actions_rx.recv().await,
            Some(BlockSyncAction::Misbehavior {
                peer: source,
                reason: BlockSyncMisbehavior::InvalidBlock,
            }) if source == peer
        ));
        task.await.expect("sequencer task exits cleanly");
    }

    #[tokio::test]
    async fn forward_reset_with_held_successor_classifies_growth_and_admits_body() {
        // A forward advance onto our own chain must not be treated as a reorg when
        // its successor body is downloaded (held in the work queue) but still
        // travelling the bounded body channel. Treating it destructively would bump
        // the reset epoch and drop the valid successor.
        let startup_frontiers = BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(1),
            verified_block_hash: block::Hash([1; 32]),
        };
        // Advance the verified tip forward to height 2 on the same chain.
        let reset_frontiers = BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(2),
            verified_block_hash: block::Hash([2; 32]),
        };
        // The successor at height 3 was delivered and settled to Held; its body is
        // still in the body channel (fed below), so no peer request references it.
        let successor = block::Height(3);
        let successor_hash = block::Hash([3; 32]);
        let mut budget = ByteBudget::new(1_024);
        let work = Arc::new(WorkQueue::new(block::Height(0)));
        work.set_estimate_floor_for_tests(1);
        assert_eq!(
            work.extend([(successor, successor_hash, BlockSizeEstimate::Advertised(64))]),
            1
        );
        let owner = ReservationOwner {
            generation: 1,
            request_token: 1,
        };
        assert_eq!(
            work.take_in_range_budgeted_owned(successor, successor, 1, u64::MAX, owner)
                .len(),
            1
        );
        assert_eq!(work.mark_reserved([successor], owner), 64);
        assert!(budget.try_reserve(64));
        assert_eq!(
            work.claim_late_body(
                successor,
                successor_hash,
                reset_frontiers.verified_block_hash,
                true,
            ),
            LateBodyClaim::ReleasedReserved {
                released_bytes: 64,
                reset_epoch: 0,
            }
        );
        budget.release(64);
        let work_view = Arc::clone(&work);
        let (actions_tx, _actions_rx) = mpsc::channel(1);
        let (_body_tx, body_rx) = mpsc::channel(1);
        let (_control_tx, control_rx) = mpsc::unbounded_channel();
        let body_input_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (view_tx, _view_rx) = watch::channel(initial_view(startup_frontiers));
        let mut task = SequencerTask::new(
            Sequencer::new(block::Height(0), 1),
            budget,
            work,
            actions_tx,
            ThroughputMeter::new(Instant::now()),
            startup_frontiers,
            body_rx,
            control_rx,
            body_input_bytes,
            view_tx,
            Duration::from_secs(1),
            ZakuraTrace::noop(),
        );

        task.handle_frontier_reset(reset_frontiers, false, false)
            .await;

        // The classification is the fix: no epoch bump means a body still carrying
        // the pre-reset epoch matches and is admitted rather than dropped. The
        // end-to-end admission of the in-channel body is exercised with a real block
        // by `reactor_forward_reset_preserves_buffered_successor_body`.
        assert_eq!(
            task.reset_epoch, 0,
            "a forward advance with a held successor is growth, not a reorg, so the \
             reset epoch must not bump"
        );
        assert!(
            work_view.in_flight_contains(successor),
            "growth must preserve the held successor claim rather than reset_above it"
        );
    }

    #[tokio::test]
    async fn forward_reset_with_conflicting_held_successor_is_destructive() {
        let startup_frontiers = BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(1),
            verified_block_hash: block::Hash([1; 32]),
        };
        let reset_frontiers = BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(2),
            verified_block_hash: block::Hash([2; 32]),
        };
        let successor = block::Height(3);
        let successor_hash = block::Hash([3; 32]);
        let mut budget = ByteBudget::new(1_024);
        let work = Arc::new(WorkQueue::new(block::Height(0)));
        work.set_estimate_floor_for_tests(1);
        assert_eq!(
            work.extend([(successor, successor_hash, BlockSizeEstimate::Advertised(64))]),
            1
        );
        let owner = ReservationOwner {
            generation: 1,
            request_token: 1,
        };
        assert_eq!(
            work.take_in_range_budgeted_owned(successor, successor, 1, u64::MAX, owner)
                .len(),
            1
        );
        assert_eq!(work.mark_reserved([successor], owner), 64);
        assert!(budget.try_reserve(64));
        assert_eq!(
            work.claim_late_body(successor, successor_hash, block::Hash([8; 32]), true,),
            LateBodyClaim::ReleasedReserved {
                released_bytes: 64,
                reset_epoch: 0,
            }
        );
        budget.release(64);

        let work_view = Arc::clone(&work);
        let (actions_tx, _actions_rx) = mpsc::channel(1);
        let (_body_tx, body_rx) = mpsc::channel(1);
        let (_control_tx, control_rx) = mpsc::unbounded_channel();
        let body_input_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (view_tx, _view_rx) = watch::channel(initial_view(startup_frontiers));
        let mut task = SequencerTask::new(
            Sequencer::new(block::Height(0), 1),
            budget,
            work,
            actions_tx,
            ThroughputMeter::new(Instant::now()),
            startup_frontiers,
            body_rx,
            control_rx,
            body_input_bytes,
            view_tx,
            Duration::from_secs(1),
            ZakuraTrace::noop(),
        );

        task.handle_frontier_reset(reset_frontiers, false, false)
            .await;

        assert_eq!(task.reset_epoch, 1);
        assert!(
            !work_view.in_flight_contains(successor),
            "a held successor with the wrong parent hash must be cleared"
        );
    }
}
