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
//! The task has two halves. [`SequencerState`] decides body acceptance, frontier
//! advance and reset, apply-completion handling, body retry, submission retry, and
//! required evidence, and returns each decision's shared-resource effects as
//! [`SequencerOperation`]s. [`SequencerIO`] owns the input channels, the input-queue
//! accounting, the byte budget, the work queue, the peer registry, the action
//! channel, the committed throughput meter, the view `watch`, timers, and traces.
//! It feeds each input to the state, runs the returned operations in order, and
//! publishes the view.
//!
//! A body's ownership moves in three steps. A peer routine charges the input-queue
//! counters when it queues a [`SequencedBody`]. IO releases that charge when it
//! receives the body, then hands the immutable payload to the state. The state's
//! `Sequencer` retains the payload until a frontier, reset, or apply completion
//! drops it. A submitted body also lives in the driver's `SubmitBlock` action, and
//! the `Sequencer` keeps its in-flight charge until the exact completion arrives.

use zakura_rt::sync::atomic::{AtomicU64, Ordering};

use super::{
    events::*,
    peer_registry::{retry_deadline_instant, PeerRegistry},
    reorder::BufferedBlockBody,
    sequencer::*,
    state::*,
    work_queue::WorkQueue,
    *,
};
use std::collections::BTreeSet;

mod state;
#[cfg(any(test, feature = "p2p-fuzz"))]
pub use state::{
    replay_sequencer_evidence_bytes, replay_sequencer_order_bytes, replay_sequencer_reset_bytes,
    SequencerReplaySummary,
};

mod trace;

use state::{BodyAcceptance, ReceivedBody, SequencerOperation, SequencerState};

/// A received body a peer routine matched (or accepted unmatched) and forwards
/// to the commit pipeline. This is the only bounded Sequencer input: a slow
/// verifier can backpressure body intake, but must not block apply/frontier
/// control events that release budget and drive the next scheduling reaction.
#[derive(Debug)]
pub(super) struct SequencedBody {
    pub(super) owner: zakura_header_chain::BodyWorkOwner,
    pub(super) source: zakura_header_chain::SourceId,
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
        owner: zakura_header_chain::BodyWorkOwner,
        source: zakura_header_chain::SourceId,
        height: block::Height,
        hash: block::Hash,
        previous_block_hash: block::Hash,
        body: BufferedBlockBody,
        bytes: u64,
        peer: ZakuraPeerId,
        received_at: Instant,
        input_bytes: Arc<AtomicU64>,
        input_decoded_attributed_memory_bytes: Arc<AtomicU64>,
    ) -> Self {
        let decoded_attributed_memory_size_bytes = body.decoded_attributed_memory_size_bytes();
        Self {
            owner,
            source,
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
    input_bytes: Arc<AtomicU64>,
    bytes: u64,
    input_decoded_attributed_memory_bytes: Arc<AtomicU64>,
    decoded_attributed_memory_size_bytes: u64,
    active: bool,
}

impl SequencerInputAccounting {
    fn new(
        input_bytes: Arc<AtomicU64>,
        bytes: u64,
        input_decoded_attributed_memory_bytes: Arc<AtomicU64>,
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
    /// Reauthorize compatible body work under the latest exact authority.
    WorkAuthorityRefreshed {
        authority: zakura_header_chain::BodyWorkAuthority,
    },
    /// Retire body work after the header-chain engine invalidates its lineage.
    BodyWorkEpochChanged {
        authority: zakura_header_chain::BodyWorkAuthority,
        frontiers: BlockSyncFrontiers,
    },
    /// Refresh the global CAS coordinate used only by synchronous state writes.
    StateVersionChanged(zakura_header_chain::StateVersion),
    /// A committed transition cleared the persistent alarm for this exact work.
    BodyAlarmCleared {
        scope: zakura_header_chain::BodyWorkAuthority,
        hash: block::Hash,
    },
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
        owner: Box<zakura_header_chain::BodyWorkOwner>,
        source: zakura_header_chain::SourceId,
        token: BlockApplyToken,
        height: block::Height,
        hash: block::Hash,
        outcome: BlockApplyOutcome,
        eligible_sources: BTreeSet<zakura_header_chain::SourceId>,
        persisted_availability: Option<zakura_header_chain::BodyUnavailableSummary>,
        semantic_completion: Option<(
            zakura_header_chain::BodyWorkOwner,
            zakura_header_chain::StateVersion,
        )>,
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

/// The serial commit-pipeline task's IO half. Owns the pure [`SequencerState`], a
/// `ByteBudget` clone, an `Arc<WorkQueue>` clone, the peer registry, an action
/// sender clone, and the committed throughput meter. Runs the state's operations
/// and emits `SubmitBlock` / `Misbehavior` on the same action channel the reactor
/// uses.
pub(super) struct SequencerIO {
    state: SequencerState,
    budget: ByteBudget,
    work: Arc<WorkQueue>,
    registry: Arc<PeerRegistry>,
    actions: mpsc::Sender<BlockSyncAction>,
    committed_throughput: ThroughputMeter,
    body_input_rx: mpsc::Receiver<SequencedBody>,
    control_input_rx: mpsc::UnboundedReceiver<SequencerControlInput>,
    _body_input_bytes: Arc<AtomicU64>,
    body_input_decoded_attributed_memory_bytes: Arc<AtomicU64>,
    view_tx: watch::Sender<SequencerView>,
    action_send_timeout: Duration,
    trace: ZakuraTrace,
}

impl Drop for SequencerIO {
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

impl SequencerIO {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        sequencer: Sequencer,
        budget: ByteBudget,
        work: Arc<WorkQueue>,
        registry: Arc<PeerRegistry>,
        actions: mpsc::Sender<BlockSyncAction>,
        committed_throughput: ThroughputMeter,
        frontiers: BlockSyncFrontiers,
        current_scope: Option<zakura_header_chain::BodyWorkAuthority>,
        retry_jitter: crate::zakura::header_sync::SeededRetryJitter,
        body_input_rx: mpsc::Receiver<SequencedBody>,
        control_input_rx: mpsc::UnboundedReceiver<SequencerControlInput>,
        body_input_bytes: Arc<AtomicU64>,
        body_input_decoded_attributed_memory_bytes: Arc<AtomicU64>,
        view_tx: watch::Sender<SequencerView>,
        action_send_timeout: Duration,
        trace: ZakuraTrace,
    ) -> Self {
        Self {
            state: SequencerState::new(sequencer, frontiers, current_scope, retry_jitter),
            budget,
            work,
            registry,
            actions,
            committed_throughput,
            body_input_rx,
            control_input_rx,
            _body_input_bytes: body_input_bytes,
            body_input_decoded_attributed_memory_bytes,
            view_tx,
            action_send_timeout,
            trace,
        }
    }

    pub(super) fn with_retained_body_limit(mut self, limit: u64) -> Self {
        self.state.set_retained_body_limit(limit);
        self
    }

    /// Seed the synchronous state-write coordinate from the startup snapshot.
    pub(super) fn with_initial_state_version(
        mut self,
        state_version: Option<zakura_header_chain::StateVersion>,
    ) -> Self {
        self.state.seed_state_version(state_version);
        self
    }

    pub(super) async fn run(mut self) {
        // Track input closure explicitly so the loop exits once both inputs close:
        // a `select!` whose arms are all disabled with no `else` panics, so the
        // top-of-loop guard breaks out before the last open channel is gated off.
        let mut control_open = true;
        let mut body_open = true;
        loop {
            let submission_retry_at = self.state.submission_retry_at();
            if !control_open && !body_open && submission_retry_at.is_none() {
                break;
            }
            zakura_rt::select! {
                biased;

                input = self.control_input_rx.recv(), if control_open => {
                    match input {
                        Some(input) => {
                            self.handle_control_input(input).await;
                            self.publish_view();
                        }
                        None => control_open = false,
                    }
                }

                _ = time::sleep_until(submission_retry_at.unwrap_or_else(time::Instant::now)),
                    if submission_retry_at.is_some() =>
                {
                    self.state.submission_retry_due();
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

    /// Feed one control input to the state and run its operations. Returns
    /// whether the input needs the reactor's query and scheduling reaction.
    async fn handle_control_input(&mut self, input: SequencerControlInput) -> bool {
        let wall_now = zakura_header_chain::Clock::now(&zakura_header_chain::SystemClock);
        let outputs = self.state.on_control_input(input, wall_now);
        self.apply_control_outputs(outputs).await
    }

    /// Record one control transition's diagnostics and run its operations.
    async fn apply_control_outputs(&mut self, outputs: state::ControlOutputs) -> bool {
        if let Some(classified) = outputs.frontier_reset {
            if classified.kind != state::FrontierResetKind::Growth {
                metrics::counter!("sync.block.reorg.reset").increment(1);
            }
            self.trace_frontier_reset_classified(&classified);
        }
        if let Some(bytes) = outputs.committed_bytes {
            self.committed_throughput.record(bytes);
            #[cfg(feature = "sync-metrics")]
            metrics::counter!("sync.block.payload.committed.bytes").increment(bytes);
        }
        self.execute(outputs.operations).await;
        outputs.needs_reaction
    }

    /// Hand a received body, already out of queue ownership, to the state.
    fn handle_accept_body(&mut self, body: SequencedBody) {
        let queued_elapsed = body.received_at.elapsed();
        let height = body.height;
        let acceptance = self.state.accept_body(ReceivedBody {
            owner: body.owner,
            source: body.source,
            height,
            hash: body.hash,
            previous_block_hash: body.previous_block_hash,
            body: body.body,
            bytes: body.bytes,
            peer: body.peer,
        });
        if let BodyAcceptance::RetentionDeferred {
            owner,
            height,
            hash,
            bytes,
            verified_tip,
        } = acceptance
        {
            self.work
                .defer_received_for_owner(owner, height, hash, bytes, verified_tip);
            metrics::counter!("sync.block.retention.deferred").increment(1);
        }
        self.trace_body_accepted(height, queued_elapsed, acceptance.label());
    }

    /// Run one transition's operations in order.
    async fn execute(&mut self, operations: Vec<SequencerOperation>) {
        for operation in operations {
            match operation {
                SequencerOperation::RefreshWorkAuthority(authority) => {
                    self.work.refresh_authority(authority);
                }
                SequencerOperation::ResetWorkAbove(height) => {
                    let released = self.work.reset_above(height);
                    self.budget.release(released);
                }
                SequencerOperation::AdvanceWorkFloor(height) => {
                    let released = self.work.advance_floor(height);
                    self.budget.release(released);
                }
                SequencerOperation::RefreshBodyRetryScope(authority) => {
                    self.registry.refresh_body_retry_scope(authority);
                }
                SequencerOperation::RetainBodyRetryScope(authority) => {
                    self.registry.retain_body_retry_scope(Some(authority));
                }
                SequencerOperation::ClearBodyRetry { scope, hash } => {
                    self.registry.clear_body_retry(scope, hash);
                }
                SequencerOperation::DeferBodyRetry {
                    sources,
                    scope,
                    hash,
                    retry_at,
                } => {
                    self.registry.defer_body_retry(
                        sources,
                        scope,
                        hash,
                        retry_deadline_instant(retry_at),
                    );
                }
                SequencerOperation::DeliverEvidence => self.deliver_required_evidence().await,
                SequencerOperation::SendAction(action) => {
                    let _ = self.send_action(action).await;
                }
                SequencerOperation::SubmitPending => self.submit_pending_blocks().await,
            }
        }
    }

    async fn submit_pending_blocks(&mut self) {
        let Some(mut pass) = self
            .state
            .begin_submission_pass(time::Instant::now(), self.actions.is_closed())
        else {
            return;
        };

        while let Some(item) = self.state.next_submission(&mut pass) {
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
            let (height, hash, token) = (item.height, item.hash, item.token);
            let send_started = time::Instant::now();
            let sent = self
                .send_action(BlockSyncAction::SubmitBlock {
                    owner: item.owner,
                    source: item.source,
                    token,
                    block: item.block,
                })
                .await;
            metrics::histogram!("sync.block.submit.queue_wait_seconds")
                .record(send_started.elapsed().as_secs_f64());
            if !sent {
                if self.state.submission_refused(
                    height,
                    token,
                    time::Instant::now(),
                    self.actions.is_closed(),
                ) {
                    metrics::counter!("sync.block.submit.retry.scheduled").increment(1);
                    self.trace_submission_retry_scheduled(height);
                }
                return;
            }
            if let Some(started_at) = self.state.submission_accepted(height, hash) {
                metrics::counter!("sync.block.submit.retry.succeeded").increment(1);
                metrics::histogram!("sync.block.submit.retry.delay_seconds")
                    .record(started_at.elapsed().as_secs_f64());
            }
            self.trace_body_submitted(height, token);
        }
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

    /// Deliver the oldest pending required action once the driver has capacity.
    ///
    /// A bounded timeout is correct for replaceable submissions and telemetry.
    /// It is not correct for deterministic invalidity or the latest retry state.
    /// Waiting on the bounded channel gives this sender FIFO capacity ahead of
    /// later peer-serving `try_send` calls. The state keeps the action pending
    /// until a reserved permit guarantees delivery. A closed receiver leaves this
    /// future fail-closed with the exact action still pending in the state until
    /// subsystem supervision cancels the task.
    async fn deliver_required_evidence(&mut self) {
        let Some(action_label) = self
            .state
            .pending_evidence()
            .map(BlockSyncAction::metric_label)
        else {
            return;
        };
        match self.actions.reserve().await {
            Ok(permit) => {
                let action = self
                    .state
                    .take_pending_evidence()
                    .expect("pending evidence exists because it was just observed");
                permit.send(action);
            }
            Err(_) => {
                metrics::counter!(
                    "sync.block.action.required_receiver_closed",
                    "action" => action_label
                )
                .increment(1);
                tracing::error!(
                    action = action_label,
                    "required block-sync action receiver closed"
                );
                std::future::pending::<()>().await;
            }
        }
    }

    fn publish_view(&mut self) {
        #[cfg(feature = "sync-metrics")]
        self.committed_throughput
            .sample_at_interval(Instant::now(), Duration::from_secs(1));
        #[cfg(not(feature = "sync-metrics"))]
        self.committed_throughput.sample(Instant::now());
        let sequencer_input_decoded_attributed_memory_bytes = self
            .body_input_decoded_attributed_memory_bytes
            .load(Ordering::Relaxed);
        // Retained bodies do not charge the request budget.
        self.budget
            .audit(self.work.reserved_bytes(), "block-sync sequencer view");
        let next = self.state.view(
            sequencer_input_decoded_attributed_memory_bytes,
            self.committed_throughput.bytes_per_sec(),
            self.committed_throughput.blocks_per_sec(),
        );
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
            .load(Ordering::Relaxed);
        let next = self
            .state
            .view(sequencer_input_decoded_attributed_memory_bytes, 0, 0);
        self.view_tx.send_if_modified(|view| {
            view.sequencer_input_decoded_attributed_memory_bytes =
                next.sequencer_input_decoded_attributed_memory_bytes;
            view.reorder_decoded_attributed_memory_bytes =
                next.reorder_decoded_attributed_memory_bytes;
            view.applying_decoded_attributed_memory_bytes =
                next.applying_decoded_attributed_memory_bytes;
            view.active_pipeline_decoded_attributed_memory_bytes =
                next.active_pipeline_decoded_attributed_memory_bytes;
            false
        });
    }
}

fn add_atomic_bytes(counter: &AtomicU64, bytes: u64) {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        let next = current.saturating_add(bytes);
        match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}

fn release_atomic_bytes(counter: &AtomicU64, bytes: u64) {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        let next = current.saturating_sub(bytes);
        match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
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
/// paused test clock, spin — the whole fleet of watchers. Admission reads the
/// reorder and applying decoded totals, so a release there wakes gated routines;
/// the input total and pipeline sum are sampled from an atomic and stay ignored.
fn view_schedulable_ne(a: &SequencerView, b: &SequencerView) -> bool {
    let strip_rates = |v: &SequencerView| {
        let mut v = *v;
        v.committed_bytes_per_sec = 0;
        v.committed_blocks_per_sec = 0;
        v.sequencer_input_decoded_attributed_memory_bytes = 0;
        v.active_pipeline_decoded_attributed_memory_bytes = 0;
        v
    };
    strip_rates(a) != strip_rates(b)
}

#[cfg(test)]
mod tests {
    mod commit_metrics;

    use zakura_chain::serialization::ZcashDeserializeInto;
    use zakura_test::vectors::BLOCK_MAINNET_1_BYTES;

    use super::super::tests::genesis_frontiers;
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
            super::super::test_work_owner(),
            zakura_header_chain::SourceId::from_digest([1; 32]),
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

    /// Feed one apply completion through the state and run its operations.
    ///
    /// Returns whether it needs a reactor reaction and whether it may refill
    /// submission slots.
    #[allow(clippy::too_many_arguments)]
    async fn finish_apply(
        task: &mut SequencerIO,
        owner: zakura_header_chain::BodyWorkOwner,
        source: zakura_header_chain::SourceId,
        token: BlockApplyToken,
        height: block::Height,
        hash: block::Hash,
        outcome: BlockApplyOutcome,
        eligible_sources: BTreeSet<zakura_header_chain::SourceId>,
        persisted_availability: Option<zakura_header_chain::BodyUnavailableSummary>,
        semantic_completion: Option<(
            zakura_header_chain::BodyWorkOwner,
            zakura_header_chain::StateVersion,
        )>,
    ) -> (bool, bool) {
        let outputs = task.state.on_control_input(
            SequencerControlInput::ApplyFinished {
                owner: Box::new(owner),
                source,
                token,
                height,
                hash,
                outcome,
                eligible_sources,
                persisted_availability,
                semantic_completion,
            },
            chrono::Utc::now(),
        );
        let allows_submit = outputs
            .operations
            .iter()
            .any(|operation| matches!(operation, SequencerOperation::SubmitPending));
        let needs_reaction = task.apply_control_outputs(outputs).await;
        (needs_reaction, allows_submit)
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

    fn retention_test_task(limit: u64) -> SequencerIO {
        let frontiers = genesis_frontiers();
        let (_body_tx, body_rx) = mpsc::channel(1);
        let (_control_tx, control_rx) = mpsc::unbounded_channel();
        let (actions, _actions_rx) = mpsc::channel(1);
        let (view_tx, _view_rx) = watch::channel(initial_view(frontiers));
        SequencerIO::new(
            Sequencer::new(block::Height(0), 1),
            ByteBudget::new(1_000_000),
            Arc::new(WorkQueue::new(block::Height(0))),
            Arc::new(PeerRegistry::new()),
            actions,
            ThroughputMeter::new(Instant::now()),
            frontiers,
            Some(super::test_work_scope()),
            crate::zakura::header_sync::SeededRetryJitter::new([0; 32]),
            body_rx,
            control_rx,
            Arc::new(std::sync::atomic::AtomicU64::new(0)),
            Arc::new(std::sync::atomic::AtomicU64::new(0)),
            view_tx,
            Duration::from_secs(1),
            ZakuraTrace::noop(),
        )
        .with_retained_body_limit(limit)
    }

    fn received_test_block() -> (Arc<block::Block>, Arc<[u8]>) {
        use zakura_chain::serialization::ZcashSerialize;
        let block: Arc<block::Block> = Arc::new(
            zakura_test::vectors::BLOCK_MAINNET_982681_BYTES
                .zcash_deserialize_into()
                .unwrap(),
        );
        let raw: Arc<[u8]> = block.zcash_serialize_to_vec().unwrap().into();
        (block, raw)
    }

    /// Wire size of every body [`received_test_body`] builds.
    fn received_test_body_bytes() -> u64 {
        u64::try_from(received_test_block().1.len()).unwrap()
    }

    // Use a real serialized block. Only the scheduling height is synthetic.
    fn received_test_body(task: &mut SequencerIO, height: u32, estimate: u64) -> SequencedBody {
        let (block, raw) = received_test_block();
        let bytes = u64::try_from(raw.len()).unwrap();
        let height = block::Height(height);
        task.work.extend(
            super::test_work_scope(),
            [(
                height,
                block.hash(),
                super::super::request::BlockSizeEstimate::Advertised(
                    u32::try_from(estimate).unwrap(),
                ),
            )],
        );
        let taken = task.work.take_for_request(
            height,
            height,
            1,
            estimate,
            u64::from(height.0),
            std::num::NonZeroU64::new(1).unwrap(),
        );
        let owner = taken[0].1.owner.unwrap();
        let reserved = task.work.mark_reserved_for_owner(owner, [height]);
        task.budget.charge(reserved);
        assert_eq!(
            task.work
                .release_active_reserved_height_for_owner(owner, height),
            Some(reserved)
        );
        task.budget.release(reserved);
        let mut body = SequencedBody::new_queued(
            owner,
            zakura_header_chain::SourceId::from_digest([1; 32]),
            height,
            block.hash(),
            block.header.previous_block_hash,
            BufferedBlockBody::from_decoded_block(block, Some(raw)),
            bytes,
            ZakuraPeerId::new(vec![1; 32]).unwrap(),
            Instant::now(),
            task._body_input_bytes.clone(),
            task.body_input_decoded_attributed_memory_bytes.clone(),
        );
        body.leave_queue();
        body
    }

    #[test]
    fn actual_retention_refuses_understated_body_and_defers_exact_attempt() {
        let mut task = retention_test_task(2_048);
        let body = received_test_body(&mut task, 500, 1_024);
        let bytes = body.bytes;
        assert!(bytes > 2_048);
        let owner = body.owner;
        let hash = body.hash;
        task.handle_accept_body(body);
        assert_eq!(task.state.sequencer().reorder_buffered_bytes(), 0);
        assert_eq!(task.work.reserved_bytes(), 0);
        assert_eq!(task.budget.reserved(), 0);
        assert!(task.work.pending_contains(block::Height(500)));
        assert!(task
            .work
            .take_in_range(block::Height(500), block::Height(500), 1)
            .is_empty());
        assert!(!task.work.defer_received_for_owner(
            owner,
            block::Height(500),
            hash,
            bytes,
            block::Height(0)
        ));
        task.work.advance_floor(block::Height(1));
        let retry = task.work.take_for_request(
            block::Height(500),
            block::Height(500),
            1,
            bytes,
            99,
            std::num::NonZeroU64::new(2).unwrap(),
        );
        assert_eq!(retry[0].1.estimated_bytes, bytes);
        assert!(!task.work.defer_received_for_owner(
            owner,
            block::Height(500),
            hash,
            bytes,
            block::Height(1)
        ));
    }

    #[tokio::test]
    async fn actual_retention_serializes_arrivals_and_preserves_checkpoint_progress() {
        let bytes = received_test_body_bytes();
        let mut task = retention_test_task(bytes);
        let first = received_test_body(&mut task, 500, 1_024);
        assert_eq!(first.bytes, bytes);
        let second = received_test_body(&mut task, 501, 1_024);
        // Both replies were issued before the first was retained.
        task.handle_accept_body(first);
        task.handle_accept_body(second);
        assert_eq!(task.state.sequencer().reorder_buffered_bytes(), bytes);
        assert_eq!(task.state.sequencer().reorder_len(), 1);
        assert!(task.work.pending_contains(block::Height(501)));
        // A full speculative backlog cannot prevent assembling the checkpoint.
        let checkpoint = received_test_body(&mut task, 401, 1_024);
        task.handle_accept_body(checkpoint);
        assert_eq!(task.state.sequencer().reorder_buffered_bytes(), 2 * bytes);
        // A reset to the verified tip, with no successor work anchored, is destructive.
        task.handle_control_input(SequencerControlInput::FrontierReset {
            frontiers: genesis_frontiers(),
            preserve_active_successors: false,
            peer_has_successor_after: false,
            peer_outstanding_conflicts_at_tip: false,
        })
        .await;
        assert_eq!(task.state.reset_epoch(), 1);
        assert_eq!(task.state.sequencer().reorder_buffered_bytes(), 0);
        let after_reset = received_test_body(&mut task, 502, 1_024);
        task.handle_accept_body(after_reset);
        assert_eq!(task.state.sequencer().reorder_buffered_bytes(), bytes);
    }

    #[tokio::test]
    async fn state_version_refresh_does_not_retire_body_authority() {
        let frontiers = genesis_frontiers();
        let scope = super::test_work_scope();
        let (_body_tx, body_rx) = mpsc::channel(1);
        let (_control_tx, control_rx) = mpsc::unbounded_channel();
        let (actions, _actions_rx) = mpsc::channel(1);
        let (view_tx, _view_rx) = watch::channel(initial_view(frontiers));
        let mut task = SequencerIO::new(
            Sequencer::new(block::Height(0), 1),
            ByteBudget::new(123),
            Arc::new(WorkQueue::new(block::Height(0))),
            Arc::new(PeerRegistry::new()),
            actions,
            ThroughputMeter::new(Instant::now()),
            frontiers,
            Some(scope),
            crate::zakura::header_sync::SeededRetryJitter::new([0; 32]),
            body_rx,
            control_rx,
            Arc::new(std::sync::atomic::AtomicU64::new(0)),
            Arc::new(std::sync::atomic::AtomicU64::new(0)),
            view_tx,
            Duration::from_secs(1),
            ZakuraTrace::noop(),
        );

        assert!(
            !task
                .handle_control_input(SequencerControlInput::StateVersionChanged(
                    zakura_header_chain::StateVersion::new(9),
                ))
                .await,
            "a CAS-coordinate refresh does not require a scheduling reaction"
        );
        assert_eq!(task.state.current_scope(), Some(scope));
        assert_eq!(
            task.state.current_state_version(),
            Some(zakura_header_chain::StateVersion::new(9))
        );
    }

    #[tokio::test]
    async fn same_target_scope_advance_preserves_downloaded_bodies() {
        let frontiers = genesis_frontiers();
        let old_scope = super::test_work_scope();
        let input_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let input_decoded_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (_body_tx, body_rx) = mpsc::channel(1);
        let (_control_tx, control_rx) = mpsc::unbounded_channel();
        let (actions, _actions_rx) = mpsc::channel(1);
        let (view_tx, _view_rx) = watch::channel(initial_view(frontiers));
        let mut task = SequencerIO::new(
            Sequencer::new(block::Height(0), 1),
            ByteBudget::new(123),
            Arc::new(WorkQueue::new(block::Height(0))),
            Arc::new(PeerRegistry::new()),
            actions,
            ThroughputMeter::new(Instant::now()),
            frontiers,
            Some(old_scope),
            crate::zakura::header_sync::SeededRetryJitter::new([0; 32]),
            body_rx,
            control_rx,
            input_bytes.clone(),
            input_decoded_bytes.clone(),
            view_tx,
            Duration::from_secs(1),
            ZakuraTrace::noop(),
        );
        let mut advanced_scope = old_scope;
        advanced_scope.header.header_generation = zakura_header_chain::HeaderGeneration::new(8);
        advanced_scope.verified_generation = zakura_header_chain::VerifiedGeneration::new(9);
        advanced_scope.header.branch.anchor_hash = block::Hash([8; 32]);

        assert!(
            !task
                .handle_control_input(SequencerControlInput::WorkAuthorityRefreshed {
                    authority: advanced_scope,
                })
                .await,
            "a same-target authority advance does not require a destructive reaction"
        );
        assert_eq!(task.state.current_scope(), Some(advanced_scope));
        assert_eq!(task.state.reset_epoch(), 0);

        let mut body = queued_test_body(input_bytes, input_decoded_bytes);
        body.leave_queue();
        task.handle_accept_body(body);

        assert_eq!(task.state.sequencer().applying_len(), 1);
        assert_eq!(task.state.sequencer().reorder_len(), 0);
    }

    #[tokio::test]
    async fn body_invalid_reselection_resets_block_pipeline() {
        let frontiers = genesis_frontiers();
        let old_authority = super::test_work_scope();
        let work = Arc::new(WorkQueue::new(block::Height(0)));
        work.extend(
            old_authority,
            [(
                block::Height(2),
                block::Hash([2; 32]),
                BlockSizeEstimate::Advertised(123),
            )],
        );
        let input_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let input_decoded_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (_body_tx, body_rx) = mpsc::channel(1);
        let (_control_tx, control_rx) = mpsc::unbounded_channel();
        let (actions, _actions_rx) = mpsc::channel(1);
        let (view_tx, _view_rx) = watch::channel(initial_view(frontiers));
        let mut task = SequencerIO::new(
            Sequencer::new(block::Height(0), 1),
            ByteBudget::new(123),
            work.clone(),
            Arc::new(PeerRegistry::new()),
            actions,
            ThroughputMeter::new(Instant::now()),
            frontiers,
            Some(old_authority),
            crate::zakura::header_sync::SeededRetryJitter::new([0; 32]),
            body_rx,
            control_rx,
            input_bytes.clone(),
            input_decoded_bytes.clone(),
            view_tx,
            Duration::from_secs(1),
            ZakuraTrace::noop(),
        );

        let mut body = queued_test_body(input_bytes, input_decoded_bytes);
        body.leave_queue();
        task.handle_accept_body(body);
        assert_eq!(task.state.sequencer().applying_len(), 1);
        assert_eq!(work.pending_len(), 1);

        let mut new_authority = old_authority;
        new_authority.body_work_epoch = zakura_header_chain::BodyWorkEpoch::new(1);
        assert!(
            task.handle_control_input(SequencerControlInput::BodyWorkEpochChanged {
                authority: new_authority,
                frontiers,
            })
            .await
        );

        assert_eq!(task.state.sequencer().floor(), frontiers.verified_block_tip);
        assert_eq!(task.state.sequencer().applying_len(), 0);
        assert_eq!(task.state.sequencer().reorder_len(), 0);
        assert_eq!(work.pending_len(), 0);
        assert_eq!(work.in_flight_len(), 0);
        assert_eq!(task.state.reset_epoch(), 1);
    }

    #[tokio::test]
    async fn stale_retry_completion_releases_only_exact_infrastructure() {
        let frontiers = genesis_frontiers();
        let input_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let input_decoded_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (_body_tx, body_rx) = mpsc::channel(1);
        let (_control_tx, control_rx) = mpsc::unbounded_channel();
        let (actions, mut actions_rx) = mpsc::channel(4);
        let (view_tx, _view_rx) = watch::channel(initial_view(frontiers));
        let mut task = SequencerIO::new(
            Sequencer::new(block::Height(0), 1),
            ByteBudget::new(123),
            Arc::new(WorkQueue::new(block::Height(0))),
            Arc::new(PeerRegistry::new()),
            actions,
            ThroughputMeter::new(Instant::now()),
            frontiers,
            Some(super::test_work_scope()),
            crate::zakura::header_sync::SeededRetryJitter::new([0; 32]),
            body_rx,
            control_rx,
            input_bytes.clone(),
            input_decoded_bytes.clone(),
            view_tx,
            Duration::from_secs(1),
            ZakuraTrace::noop(),
        );

        let mut body = queued_test_body(input_bytes.clone(), input_decoded_bytes.clone());
        body.leave_queue();
        task.handle_accept_body(body);
        task.submit_pending_blocks().await;
        let BlockSyncAction::SubmitBlock {
            owner,
            source,
            token,
            block,
        } = actions_rx.recv().await.expect("body is submitted")
        else {
            panic!("expected a body submission");
        };
        let height = block.coinbase_height().expect("test block has height");
        let hash = block.hash();
        let outcome = super::test_block_apply_outcome(BlockApplyResult::Unavailable);

        assert_eq!(
            finish_apply(
                &mut task,
                owner,
                source,
                token,
                height,
                hash,
                outcome,
                BTreeSet::new(),
                None,
                None,
            )
            .await,
            (false, false),
            "a stale transient completion cannot trigger scheduling or reaction"
        );
        assert!(!task.state.sequencer().applying_contains(height));
        assert_eq!(task.state.sequencer().in_flight_submission_count(), 0);
        assert!(
            task.state.body_retries().is_empty(),
            "stale transient completion cannot create a retry episode"
        );
        assert!(
            actions_rx.try_recv().is_err(),
            "stale completion emits no action"
        );

        let mut current_scope = super::test_work_scope();
        current_scope.verified_generation = zakura_header_chain::VerifiedGeneration::new(9);
        current_scope.body_work_epoch = zakura_header_chain::BodyWorkEpoch::new(1);
        assert!(
            task.handle_control_input(SequencerControlInput::BodyWorkEpochChanged {
                authority: current_scope,
                frontiers,
            })
            .await
        );
        assert_eq!(task.state.sequencer().floor(), block::Height(0));

        let mut old_body = queued_test_body(input_bytes, input_decoded_bytes);
        old_body.leave_queue();
        task.handle_accept_body(old_body);
        assert_eq!(task.state.sequencer().reorder_len(), 0);
        assert_eq!(task.state.sequencer().applying_len(), 0);
    }

    #[tokio::test]
    async fn consensus_invalid_evidence_waits_for_action_capacity() {
        let frontiers = genesis_frontiers();
        let input_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let input_decoded_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (_body_tx, body_rx) = mpsc::channel(1);
        let (_control_tx, control_rx) = mpsc::unbounded_channel();
        let (actions, mut actions_rx) = mpsc::channel(1);
        let action_sender = actions.clone();
        let (view_tx, _view_rx) = watch::channel(initial_view(frontiers));
        let mut task = SequencerIO::new(
            Sequencer::new(block::Height(0), 1),
            ByteBudget::new(123),
            Arc::new(WorkQueue::new(block::Height(0))),
            Arc::new(PeerRegistry::new()),
            actions,
            ThroughputMeter::new(Instant::now()),
            frontiers,
            Some(super::test_work_scope()),
            crate::zakura::header_sync::SeededRetryJitter::new([0; 32]),
            body_rx,
            control_rx,
            input_bytes.clone(),
            input_decoded_bytes.clone(),
            view_tx,
            Duration::from_millis(20),
            ZakuraTrace::noop(),
        );

        let mut body = queued_test_body(input_bytes, input_decoded_bytes);
        body.leave_queue();
        task.handle_accept_body(body);
        task.submit_pending_blocks().await;
        let BlockSyncAction::SubmitBlock {
            owner,
            source,
            token,
            block,
        } = actions_rx.recv().await.expect("body is submitted")
        else {
            panic!("expected a body submission");
        };
        let height = block.coinbase_height().expect("test block has height");
        let hash = block.hash();
        let invalid = zakura_header_chain::ConsensusBodyInvalid {
            hash,
            evidence: zakura_header_chain::EvidenceId::from_digest([0xa1; 32]),
            rule: zakura_header_chain::BodyRuleId::new("test.consensus_invalid"),
            source,
        };
        let outcome = BlockApplyOutcome::consensus_invalid(invalid.clone());

        action_sender
            .send(BlockSyncAction::Misbehavior {
                peer: ZakuraPeerId::new(vec![0xee; 32])
                    .expect("the filler peer ID has the required length"),
                reason: BlockSyncMisbehavior::InvalidBlock,
            })
            .await
            .expect("the filler occupies the action channel");
        let completion = tokio::spawn(async move {
            let result = finish_apply(
                &mut task,
                owner,
                source,
                token,
                height,
                hash,
                outcome,
                BTreeSet::new(),
                None,
                Some((owner, zakura_header_chain::StateVersion::new(7))),
            )
            .await;
            (task, result)
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !completion.is_finished(),
            "invalidity must remain pending after the ordinary action timeout",
        );
        assert!(matches!(
            actions_rx.recv().await,
            Some(BlockSyncAction::Misbehavior {
                reason: BlockSyncMisbehavior::InvalidBlock,
                ..
            })
        ));
        assert!(matches!(
            actions_rx.recv().await,
            Some(BlockSyncAction::RecordBodyInvalid {
                expected_version,
                invalid: actual,
            }) if expected_version == zakura_header_chain::StateVersion::new(7)
                && actual == invalid
        ));

        let (task, result) = completion.await.expect("the sequencer task does not panic");
        assert_eq!(result, (true, true));
        assert!(
            !task.state.sequencer().applying_contains(height),
            "the sequencer retires the body only after it hands off invalidity",
        );
        assert!(matches!(
            actions_rx.recv().await,
            Some(BlockSyncAction::Misbehavior {
                reason: BlockSyncMisbehavior::ConsensusBodyInvalid(actual),
                ..
            }) if actual == invalid
        ));
    }

    #[tokio::test]
    async fn retryable_evidence_waits_for_action_capacity() {
        let frontiers = genesis_frontiers();
        let input_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let input_decoded_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (_body_tx, body_rx) = mpsc::channel(1);
        let (_control_tx, control_rx) = mpsc::unbounded_channel();
        let (actions, mut actions_rx) = mpsc::channel(1);
        let action_sender = actions.clone();
        let (view_tx, _view_rx) = watch::channel(initial_view(frontiers));
        let mut task = SequencerIO::new(
            Sequencer::new(block::Height(0), 1),
            ByteBudget::new(123),
            Arc::new(WorkQueue::new(block::Height(0))),
            Arc::new(PeerRegistry::new()),
            actions,
            ThroughputMeter::new(Instant::now()),
            frontiers,
            Some(super::test_work_scope()),
            crate::zakura::header_sync::SeededRetryJitter::new([0; 32]),
            body_rx,
            control_rx,
            input_bytes.clone(),
            input_decoded_bytes.clone(),
            view_tx,
            Duration::from_millis(20),
            ZakuraTrace::noop(),
        );

        let mut body = queued_test_body(input_bytes, input_decoded_bytes);
        body.leave_queue();
        task.handle_accept_body(body);
        task.submit_pending_blocks().await;
        let BlockSyncAction::SubmitBlock {
            owner,
            source,
            token,
            block,
        } = actions_rx.recv().await.expect("body is submitted")
        else {
            panic!("expected a body submission");
        };
        let height = block.coinbase_height().expect("test block has height");
        let hash = block.hash();
        let outcome = BlockApplyOutcome::retryable(zakura_header_chain::TransientBodyFailure {
            hash,
            evidence: zakura_header_chain::EvidenceId::from_digest([0xa2; 32]),
            kind: zakura_header_chain::TransientBodyFailureKind::VerifierUnavailable,
            availability: zakura_header_chain::BodyUnavailableSummary::default(),
        });

        action_sender
            .send(BlockSyncAction::Misbehavior {
                peer: ZakuraPeerId::new(vec![0xee; 32])
                    .expect("the filler peer ID has the required length"),
                reason: BlockSyncMisbehavior::InvalidBlock,
            })
            .await
            .expect("the filler occupies the action channel");
        let completion = tokio::spawn(async move {
            let result = finish_apply(
                &mut task,
                owner,
                source,
                token,
                height,
                hash,
                outcome,
                BTreeSet::new(),
                None,
                Some((owner, zakura_header_chain::StateVersion::new(8))),
            )
            .await;
            (task, result)
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !completion.is_finished(),
            "retry state must remain pending after the ordinary action timeout",
        );
        assert!(matches!(
            actions_rx.recv().await,
            Some(BlockSyncAction::Misbehavior {
                reason: BlockSyncMisbehavior::InvalidBlock,
                ..
            })
        ));
        assert!(matches!(
            actions_rx.recv().await,
            Some(BlockSyncAction::RecordBodyUnavailable {
                expected_version,
                failure,
            }) if expected_version == zakura_header_chain::StateVersion::new(8)
                && failure.hash == hash
                && failure.evidence
                    == zakura_header_chain::EvidenceId::from_digest([0xa2; 32])
                && failure.kind
                    == zakura_header_chain::TransientBodyFailureKind::VerifierUnavailable
        ));

        let (task, result) = completion.await.expect("the sequencer task does not panic");
        assert_eq!(result, (true, true));
        assert!(
            !task.state.sequencer().applying_contains(height),
            "the sequencer retires the body only after it hands off retry state",
        );
    }

    #[tokio::test]
    async fn closed_action_receiver_retains_exact_required_evidence() {
        let frontiers = genesis_frontiers();
        let input_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let input_decoded_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (_body_tx, body_rx) = mpsc::channel(1);
        let (_control_tx, control_rx) = mpsc::unbounded_channel();
        let (actions, mut actions_rx) = mpsc::channel(1);
        let (view_tx, _view_rx) = watch::channel(initial_view(frontiers));
        let mut task = SequencerIO::new(
            Sequencer::new(block::Height(0), 1),
            ByteBudget::new(123),
            Arc::new(WorkQueue::new(block::Height(0))),
            Arc::new(PeerRegistry::new()),
            actions,
            ThroughputMeter::new(Instant::now()),
            frontiers,
            Some(super::test_work_scope()),
            crate::zakura::header_sync::SeededRetryJitter::new([0; 32]),
            body_rx,
            control_rx,
            input_bytes.clone(),
            input_decoded_bytes.clone(),
            view_tx,
            Duration::from_millis(20),
            ZakuraTrace::noop(),
        );

        let mut body = queued_test_body(input_bytes, input_decoded_bytes);
        body.leave_queue();
        task.handle_accept_body(body);
        task.submit_pending_blocks().await;
        let BlockSyncAction::SubmitBlock {
            owner,
            source,
            token,
            block,
        } = actions_rx.recv().await.expect("body is submitted")
        else {
            panic!("expected a body submission");
        };
        drop(actions_rx);
        let height = block.coinbase_height().expect("test block has height");
        let hash = block.hash();

        let rule: Arc<str> = Arc::from("test.retained_consensus_invalid");
        let retained_rule = Arc::downgrade(&rule);
        let outcome =
            BlockApplyOutcome::consensus_invalid(zakura_header_chain::ConsensusBodyInvalid {
                hash,
                evidence: zakura_header_chain::EvidenceId::from_digest([0xa4; 32]),
                rule: zakura_header_chain::BodyRuleId::new(rule),
                source,
            });
        let mut handoff = tokio::spawn(async move {
            finish_apply(
                &mut task,
                owner,
                source,
                token,
                height,
                hash,
                outcome,
                BTreeSet::new(),
                None,
                Some((owner, zakura_header_chain::StateVersion::new(9))),
            )
            .await;
        });

        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut handoff)
                .await
                .is_err(),
            "a closed receiver must leave the required handoff fail-closed",
        );
        assert!(
            retained_rule.upgrade().is_some(),
            "the fail-closed handoff must retain the exact invalidity evidence",
        );

        handoff.abort();
        let _ = handoff.await;
        assert!(
            retained_rule.upgrade().is_none(),
            "supervision cancellation releases the retained evidence",
        );
    }

    #[tokio::test]
    // IN-02: enumerate every commitment mismatch so each proves body-only
    // attribution while preserving the independently valid header.
    async fn each_commitment_mismatch_scores_only_body_delivery() {
        let kinds = [
            zakura_header_chain::BodyCommitmentKind::HeaderHash,
            zakura_header_chain::BodyCommitmentKind::TransactionMerkleRoot,
            zakura_header_chain::BodyCommitmentKind::AuthDataRoot,
            zakura_header_chain::BodyCommitmentKind::Other("test.other_commitment"),
        ];
        for (index, kind) in kinds.into_iter().enumerate() {
            for attribution_matches in [true, false] {
                let frontiers = genesis_frontiers();
                let input_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
                let input_decoded_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
                let (_body_tx, body_rx) = mpsc::channel(1);
                let (_control_tx, control_rx) = mpsc::unbounded_channel();
                let (actions, mut actions_rx) = mpsc::channel(4);
                let (view_tx, _view_rx) = watch::channel(initial_view(frontiers));
                let mut task = SequencerIO::new(
                    Sequencer::new(block::Height(0), 1),
                    ByteBudget::new(123),
                    Arc::new(WorkQueue::new(block::Height(0))),
                    Arc::new(PeerRegistry::new()),
                    actions,
                    ThroughputMeter::new(Instant::now()),
                    frontiers,
                    Some(super::test_work_scope()),
                    crate::zakura::header_sync::SeededRetryJitter::new([0; 32]),
                    body_rx,
                    control_rx,
                    input_bytes.clone(),
                    input_decoded_bytes.clone(),
                    view_tx,
                    Duration::from_secs(1),
                    ZakuraTrace::noop(),
                );

                let mut body = queued_test_body(input_bytes, input_decoded_bytes);
                body.leave_queue();
                task.handle_accept_body(body);
                task.submit_pending_blocks().await;
                let BlockSyncAction::SubmitBlock {
                    owner,
                    source,
                    token,
                    block,
                } = actions_rx.recv().await.expect("body is submitted")
                else {
                    panic!("expected a body submission");
                };
                let height = block.coinbase_height().expect("test block has height");
                let hash = block.hash();
                let attributed_source = if attribution_matches {
                    source
                } else {
                    zakura_header_chain::SourceId::from_digest([9; 32])
                };
                let marker = u8::try_from(index).expect("the mismatch matrix fits in u8");
                let mismatch = zakura_header_chain::BodyPayloadMismatch {
                    evidence: zakura_header_chain::EvidenceId::from_digest(
                        [marker.wrapping_add(0x80); 32],
                    ),
                    requested: hash,
                    delivered: block::Hash([marker.wrapping_add(0x40); 32]),
                    kind,
                    source: attributed_source,
                };
                let outcome = BlockApplyOutcome::payload_mismatch(mismatch);

                assert_eq!(
                    finish_apply(
                        &mut task,
                        owner,
                        source,
                        token,
                        height,
                        hash,
                        outcome,
                        BTreeSet::new(),
                        None,
                        Some((owner, zakura_header_chain::StateVersion::default())),
                    )
                    .await,
                    (true, true),
                    "a current mismatch retires the bad body and requests more work"
                );
                assert!(!task.state.sequencer().applying_contains(height));
                assert_eq!(task.state.sequencer().in_flight_submission_count(), 0);

                if attribution_matches {
                    assert!(matches!(
                        actions_rx.recv().await,
                        Some(BlockSyncAction::Misbehavior {
                            peer,
                            reason: BlockSyncMisbehavior::BodyPayloadMismatch(actual),
                        }) if peer == ZakuraPeerId::new(vec![1; 32])
                            .expect("test peer ID is valid")
                            && actual == mismatch
                    ));
                }
                assert!(
                    actions_rx.try_recv().is_err(),
                    "payload mismatch can emit only exact-supplier scoring, never eligibility state"
                );
            }
        }
    }

    /// A tampered body's record waits for a busy driver instead of timing out.
    /// The height is still reset so another supplier can serve it.
    #[tokio::test]
    async fn tampered_body_misbehavior_waits_for_a_busy_driver() {
        let frontiers = genesis_frontiers();
        let input_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let input_decoded_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (_body_tx, body_rx) = mpsc::channel(1);
        let (_control_tx, control_rx) = mpsc::unbounded_channel();
        let (actions, mut actions_rx) = mpsc::channel(1);
        let (view_tx, _view_rx) = watch::channel(initial_view(frontiers));
        let mut task = SequencerIO::new(
            Sequencer::new(block::Height(0), 1),
            ByteBudget::new(123),
            Arc::new(WorkQueue::new(block::Height(0))),
            Arc::new(PeerRegistry::new()),
            actions.clone(),
            ThroughputMeter::new(Instant::now()),
            frontiers,
            Some(super::test_work_scope()),
            crate::zakura::header_sync::SeededRetryJitter::new([0; 32]),
            body_rx,
            control_rx,
            input_bytes.clone(),
            input_decoded_bytes.clone(),
            view_tx,
            Duration::from_millis(20),
            ZakuraTrace::noop(),
        );

        let mut body = queued_test_body(input_bytes, input_decoded_bytes);
        body.leave_queue();
        task.handle_accept_body(body);
        task.submit_pending_blocks().await;
        let BlockSyncAction::SubmitBlock {
            owner,
            source,
            token,
            block,
        } = actions_rx.recv().await.expect("body is submitted")
        else {
            panic!("expected a body submission");
        };
        let height = block.coinbase_height().expect("test block has height");
        let hash = block.hash();
        let busy_peer = ZakuraPeerId::new(vec![7; 32]).expect("test peer ID is valid");
        actions
            .send(BlockSyncAction::Misbehavior {
                peer: busy_peer,
                reason: BlockSyncMisbehavior::StatusSpam,
            })
            .await
            .expect("the filler action fills the channel");
        let mismatch = zakura_header_chain::BodyPayloadMismatch {
            evidence: zakura_header_chain::EvidenceId::from_digest([0x81; 32]),
            requested: hash,
            delivered: hash,
            kind: zakura_header_chain::BodyCommitmentKind::AuthDataRoot,
            source,
        };
        let outcome = BlockApplyOutcome::payload_mismatch(mismatch);

        let finished = {
            let finish = finish_apply(
                &mut task,
                owner,
                source,
                token,
                height,
                hash,
                outcome,
                BTreeSet::new(),
                None,
                Some((owner, zakura_header_chain::StateVersion::default())),
            );
            tokio::pin!(finish);
            assert!(
                tokio::time::timeout(Duration::from_millis(100), &mut finish)
                    .await
                    .is_err(),
                "the record must outlast the 20ms bounded send timeout"
            );
            assert!(matches!(
                actions_rx.recv().await,
                Some(BlockSyncAction::Misbehavior {
                    reason: BlockSyncMisbehavior::StatusSpam,
                    ..
                })
            ));
            finish.await
        };
        assert_eq!(finished, (true, true));
        assert!(matches!(
            actions_rx.recv().await,
            Some(BlockSyncAction::Misbehavior {
                peer,
                reason: BlockSyncMisbehavior::BodyPayloadMismatch(actual),
            }) if peer == ZakuraPeerId::new(vec![1; 32]).expect("test peer ID is valid")
                && actual == mismatch
        ));
        assert!(!task.state.sequencer().applying_contains(height));
        assert!(
            task.state.sequencer().floor() < height,
            "the height is requestable again"
        );
    }

    #[tokio::test]
    // IN-02: persist a consensus-invalid result.
    // Attribute it only to the authenticated supplier.
    async fn body_invalid_after_header_extension_reaches_header_dag() {
        for attribution_matches in [true, false] {
            let frontiers = genesis_frontiers();
            let input_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let input_decoded_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let (_body_tx, body_rx) = mpsc::channel(1);
            let (_control_tx, control_rx) = mpsc::unbounded_channel();
            let (actions, mut actions_rx) = mpsc::channel(4);
            let (view_tx, _view_rx) = watch::channel(initial_view(frontiers));
            let mut task = SequencerIO::new(
                Sequencer::new(block::Height(0), 1),
                ByteBudget::new(123),
                Arc::new(WorkQueue::new(block::Height(0))),
                Arc::new(PeerRegistry::new()),
                actions,
                ThroughputMeter::new(Instant::now()),
                frontiers,
                Some(super::test_work_scope()),
                crate::zakura::header_sync::SeededRetryJitter::new([0; 32]),
                body_rx,
                control_rx,
                input_bytes.clone(),
                input_decoded_bytes.clone(),
                view_tx,
                Duration::from_secs(1),
                ZakuraTrace::noop(),
            );

            let mut body = queued_test_body(input_bytes, input_decoded_bytes);
            body.leave_queue();
            task.handle_accept_body(body);
            task.submit_pending_blocks().await;
            let BlockSyncAction::SubmitBlock {
                owner,
                source,
                token,
                block,
            } = actions_rx.recv().await.expect("body is submitted")
            else {
                panic!("expected a body submission");
            };
            let height = block.coinbase_height().expect("test block has height");
            let hash = block.hash();
            let attributed_source = if attribution_matches {
                source
            } else {
                zakura_header_chain::SourceId::from_digest([9; 32])
            };
            let invalid = zakura_header_chain::ConsensusBodyInvalid {
                hash,
                evidence: zakura_header_chain::EvidenceId::from_digest([8; 32]),
                rule: zakura_header_chain::BodyRuleId::new("test.consensus_invalid"),
                source: attributed_source,
            };
            let outcome = BlockApplyOutcome::consensus_invalid(invalid.clone());
            let mut refreshed_authority = owner.authority();
            refreshed_authority.header.header_generation =
                zakura_header_chain::HeaderGeneration::new(8);
            refreshed_authority.header.branch.target_tip_hash = block::Hash([8; 32]);
            assert!(
                !task
                    .handle_control_input(SequencerControlInput::WorkAuthorityRefreshed {
                        authority: refreshed_authority,
                    })
                    .await
            );
            let semantic_owner = refreshed_authority.bind(owner.session_id(), owner.request_id());

            assert_eq!(
                finish_apply(
                    &mut task,
                    owner,
                    source,
                    token,
                    height,
                    hash,
                    outcome,
                    BTreeSet::new(),
                    None,
                    Some((semantic_owner, zakura_header_chain::StateVersion::default(),)),
                )
                .await,
                (true, true)
            );
            assert!(!task.state.sequencer().applying_contains(height));
            assert_eq!(task.state.sequencer().in_flight_submission_count(), 0);

            if attribution_matches {
                assert!(matches!(
                    actions_rx.recv().await,
                    Some(BlockSyncAction::RecordBodyInvalid {
                        expected_version,
                        invalid: actual,
                    }) if expected_version == zakura_header_chain::StateVersion::default()
                        && actual == invalid
                ));
                assert!(matches!(
                    actions_rx.recv().await,
                    Some(BlockSyncAction::Misbehavior {
                        peer,
                        reason: BlockSyncMisbehavior::ConsensusBodyInvalid(actual),
                    }) if peer == ZakuraPeerId::new(vec![1; 32]).expect("test peer ID is valid")
                        && actual == invalid
                ));
            } else {
                assert!(
                    actions_rx.try_recv().is_err(),
                    "mismatched body attribution can neither mutate state nor score a peer"
                );
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn submission_retries_after_action_channel_capacity_returns() {
        let frontiers = genesis_frontiers();
        let body_input_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let body_input_decoded_attributed_memory_bytes =
            Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (body_tx, body_rx) = mpsc::channel(1);
        let (_control_tx, control_rx) = mpsc::unbounded_channel();
        let (actions, mut actions_rx) = mpsc::channel(1);
        actions
            .try_send(BlockSyncAction::QueryNeededBlocks {
                query_id: std::num::NonZeroU64::new(1).expect("one is nonzero"),
                from: block::Height(1),
                limit: 1,
                best_header_tip: block::Height(1),
                scope: super::test_work_scope(),
            })
            .expect("test fills the action channel");
        let (view_tx, mut view_rx) = watch::channel(initial_view(frontiers));
        let task = SequencerIO::new(
            Sequencer::new(block::Height(0), 1),
            ByteBudget::new(123),
            Arc::new(WorkQueue::new(block::Height(0))),
            Arc::new(PeerRegistry::new()),
            actions,
            ThroughputMeter::new(Instant::now()),
            frontiers,
            Some(super::test_work_scope()),
            crate::zakura::header_sync::SeededRetryJitter::new([0; 32]),
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
        assert!(matches!(
            retried,
            BlockSyncAction::SubmitBlock { owner, source, .. }
                if owner == super::super::test_work_owner()
                    && source == zakura_header_chain::SourceId::from_digest([1; 32])
        ));

        task.abort();
    }

    #[test]
    fn handoff_publishes_applying_decoded_bytes_before_submission() {
        let frontiers = genesis_frontiers();
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
        let mut task = SequencerIO::new(
            Sequencer::new(block::Height(0), 1),
            ByteBudget::new(123),
            Arc::new(WorkQueue::new(block::Height(0))),
            Arc::new(PeerRegistry::new()),
            actions,
            ThroughputMeter::new(Instant::now()),
            frontiers,
            Some(super::test_work_scope()),
            crate::zakura::header_sync::SeededRetryJitter::new([0; 32]),
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
        let frontiers = genesis_frontiers();
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
        let task = SequencerIO::new(
            Sequencer::new(block::Height(0), 1),
            ByteBudget::new(1),
            Arc::new(WorkQueue::new(block::Height(0))),
            Arc::new(PeerRegistry::new()),
            actions,
            ThroughputMeter::new(Instant::now()),
            frontiers,
            Some(super::test_work_scope()),
            crate::zakura::header_sync::SeededRetryJitter::new([0; 32]),
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
