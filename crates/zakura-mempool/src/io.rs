//! The pool core's task: [`MempoolIO`] owns the machine and serves the port.
//! It holds the channels, the timer and the running verifications, and
//! decides nothing: every rule is the machine's.
//!
//! The task takes one input at a time: a caller's command, a chain event, a
//! verification's outcome, a change to whether the pool is on, or a deadline.
//! It hands the input to [`MempoolState`], then takes every other input that
//! is already waiting, up to [`MAX_INPUTS_PER_PUBLISH`], the same way. Then it
//! carries out their steps, in this order:
//!
//! 1. It publishes each step's change batch, as a batch of its own, and one
//!    snapshot of the pool after the last, so a verification that starts in
//!    any of these steps reads a pool at least as new as the one that
//!    started it.
//! 2. It sends the answers to the commands it took, so no caller hears of a
//!    change before readers can see it.
//! 3. It does each step's effects in order: starts and cancels
//!    verifications, and sends verdicts.
//!
//! A verifier the core owns reads the pool through a `PoolReader`, so it
//! does not keep the core running once every handle is gone.
//!
//! A verification runs in a task of its own, outside the core's task, and
//! comes back as an input. See [`MempoolState::outcome`] for when the machine
//! counts it.
//!
//! The machine emits no metrics. After each flush the task reads what storage
//! holds and emits today's pool metrics under today's names.
//!
//! Every name the task uses for time, tasks and channels comes from
//! `zakura_rt`, so it builds and runs under the simulator.

use std::{future::Future, pin::Pin};

use chrono::{DateTime, Utc};
use zakura_chain::transaction::UnminedTx;
use zakura_node_services::mempool::port::{
    ChainEvent, ChainTip, ChangeSeq, LocalFault, PoolCommand, PoolInbox, PoolSnapshot,
    ReservationId, Verdict,
};
use zakura_rt::{
    collections::HashMap,
    sync::{mpsc, oneshot, watch, CancellationToken},
    task::{self, AbortOnDropHandle},
    time,
};

use crate::{
    config::Config,
    machine::{Effect, JobId, Limits, MempoolState, Outcome, Step, VerifyError, WaiterId},
    storage::{SameEffectsTipRejectionError, StorageStats},
};

#[cfg(test)]
mod tests;

/// The most inputs the core takes before it publishes. Every input that is
/// already waiting, up to this many, shares one snapshot, so a burst of
/// outcomes while a large pool is restored does not copy the pool once per
/// outcome.
const MAX_INPUTS_PER_PUBLISH: usize = 64;

/// The node's best-chain changes, in order. zakurad implements this over
/// the state's tip watch.
pub trait ChainFeed: Send + 'static {
    /// Waits for the next change, or answers `None` once the state is gone.
    ///
    /// The core drops this future whenever another input is ready first, so
    /// dropping it must lose no change.
    fn next(&mut self) -> Pin<Box<dyn Future<Output = Option<ChainEvent>> + Send + '_>>;
}

/// What the core asks of the node to verify one transaction: the
/// already-mined lookup, then the verifier, without the fetch.
pub trait Verifier<E>: Clone + Send + Sync + 'static {
    /// Verifies `tx` for the block after `tip`.
    ///
    /// The core calls this in the step that starts the job, after that
    /// step's snapshot is published. A panic here, or in the future, is a
    /// local fault.
    ///
    /// `after_reset` says the chain was reset since the transaction's body
    /// arrived. See `Effect::Start`.
    fn verify(
        &self,
        tx: UnminedTx,
        tip: ChainTip,
        after_reset: bool,
    ) -> Pin<Box<dyn Future<Output = Outcome<E>> + Send>>;
}

/// A verification's report to the core.
struct Report<E> {
    job: JobId,
    outcome: Outcome<E>,
}

/// What the snapshot last published carried, beside the transactions.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct Published {
    tip: ChainTip,
    active: bool,
}

/// The pool core: the machine, the port's inbox, and the work in flight.
pub struct MempoolIO<E, V> {
    machine: MempoolState<E>,
    inbox: PoolInbox,
    chain: Box<dyn ChainFeed>,
    verifier: V,
    /// Whether the node is near enough to the tip for the pool to be on.
    near_tip: watch::Receiver<bool>,
    /// Every verification reports here. At most one report per running job
    /// is in flight, and the machine bounds the jobs, so this is bounded.
    reports_tx: mpsc::UnboundedSender<Report<E>>,
    reports: mpsc::UnboundedReceiver<Report<E>>,
    /// The running verifications. Dropping a handle cancels its task.
    jobs: HashMap<JobId, AbortOnDropHandle<()>>,
    /// The cancellation of each reservation the core granted for a fetch,
    /// until its caller submits, fails or drops it. One per slot at most.
    fetches: HashMap<ReservationId, CancellationToken>,
    /// Callers waiting for a verdict.
    verdicts: HashMap<WaiterId, oneshot::Sender<Verdict>>,
    published: Option<Published>,
    /// Steps taken and not yet carried out.
    steps: Vec<Step>,
    /// Answers to commands, sent once the steps before them are published,
    /// so no caller hears of a change before readers can see it.
    replies: Vec<Box<dyn FnOnce() + Send>>,
    /// What storage held when its metrics were last emitted.
    emitted: StorageStats,
    /// Told about each chain event the core applies. See
    /// [`Self::on_tip_applied`].
    tips: Option<TipObserver>,
}

/// What [`MempoolIO::on_tip_applied`] registers.
type TipObserver = Box<dyn FnMut(&TipApplied) + Send>;

/// One chain event the core applied, for the node's trace.
#[derive(Clone, Debug)]
pub struct TipApplied {
    /// The tip after it.
    pub tip: ChainTip,
    /// Whether the pool was emptied to be verified again: a reorg, skipped
    /// blocks or a rules change. Otherwise the tip grew by one block, and
    /// only its transactions and what they conflict with left the pool.
    pub reset: bool,
    /// Pooled transactions after it.
    pub pooled: usize,
    /// What the pool held or was bringing in before it: pooled
    /// transactions, undecided ones, and those an earlier reset left
    /// waiting. A reset verifies all of these again, and today's trace
    /// counts them as requeued.
    pub held_before: usize,
    /// How long the machine took to apply it.
    pub took: std::time::Duration,
}

impl<E: VerifyError, V: Verifier<E>> MempoolIO<E, V> {
    /// A core that reads chain events from `chain` and serves `inbox`. It is
    /// on while `near_tip` holds `true` and `chain` has named a tip.
    pub fn new(
        config: &Config,
        chain: Box<dyn ChainFeed>,
        verifier: V,
        inbox: PoolInbox,
        near_tip: watch::Receiver<bool>,
    ) -> Self {
        Self::with_limits(config, chain, verifier, inbox, near_tip, Limits::default())
    }

    /// [`Self::new`] with other limits.
    pub fn with_limits(
        config: &Config,
        chain: Box<dyn ChainFeed>,
        verifier: V,
        inbox: PoolInbox,
        near_tip: watch::Receiver<bool>,
        limits: Limits,
    ) -> Self {
        let machine = MempoolState::new(config, time::Instant::now()).with_limits(limits);
        let (reports_tx, reports) = mpsc::unbounded_channel();
        Self {
            machine,
            inbox,
            chain,
            verifier,
            near_tip,
            reports_tx,
            reports,
            jobs: HashMap::default(),
            fetches: HashMap::default(),
            verdicts: HashMap::default(),
            published: None,
            steps: Vec::new(),
            replies: Vec::new(),
            emitted: StorageStats::default(),
            tips: None,
        }
    }

    /// Runs the core until every port handle is gone or the chain feed
    /// ends.
    pub async fn run(mut self) {
        // The pool is off until the chain names a tip and the node is near
        // it, and the first snapshot says so.
        self.near_tip();
        self.flush();
        loop {
            let deadline = self.machine.next_deadline();
            let sleep = async move {
                match deadline {
                    Some(deadline) => time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            };
            zakura_rt::select! {
                biased;
                event = self.chain.next() => match event {
                    Some(event) => self.chain_event(event),
                    None => break,
                },
                Some(report) = self.reports.recv() => self.report(report),
                changed = self.near_tip.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    self.near_tip();
                }
                command = self.inbox.next() => match command {
                    Some(command) => self.command(command),
                    None => break,
                },
                () = sleep => {
                    let step = self.machine.tick(time::Instant::now());
                    self.finish(step);
                }
            }
            // Take what else is already waiting, then carry it all out.
            for _ in 1..MAX_INPUTS_PER_PUBLISH {
                if let Ok(report) = self.reports.try_recv() {
                    self.report(report);
                } else if let Some(command) = self.inbox.try_next() {
                    self.command(command);
                } else {
                    break;
                }
            }
            self.flush();
        }
    }

    /// One caller's command.
    fn command(&mut self, command: PoolCommand) {
        let now = time::Instant::now();
        match command {
            PoolCommand::Reserve { tx_id, reply } => {
                let (answer, step) = self.machine.reserve(tx_id, now);
                let answer = answer.map(|id| {
                    let cancel = CancellationToken::new();
                    self.fetches.insert(id, cancel.clone());
                    self.inbox.grant(id, tx_id, cancel)
                });
                // A caller that left drops the reservation with the reply,
                // which frees the slot.
                self.reply(move || {
                    let _ = reply.send(answer);
                });
                self.finish(step);
            }
            PoolCommand::Submit {
                reservation,
                transaction,
                reply,
            } => {
                // The caller gave up after the command was queued. Dropping
                // the reservation frees its slot, as the port promises.
                if reply.is_closed() {
                    drop(reservation);
                    return;
                }
                let give_up = reservation.give_up();
                let id = reservation.accept();
                self.fetches.remove(&id);
                let (waiter, step) = self.machine.submit(id, transaction, give_up, now);
                self.verdicts.insert(waiter, reply);
                self.finish(step);
            }
            PoolCommand::FetchFailed(reservation) => {
                let id = reservation.accept();
                self.fetches.remove(&id);
                let step = self.machine.fetch_failed(id, now);
                self.finish(step);
            }
            PoolCommand::Released(id) => {
                self.fetches.remove(&id);
                let step = self.machine.release(id, now);
                self.finish(step);
            }
            PoolCommand::KnownIds { ids, reply } => {
                // A caller that gave up on the lookup costs the core no scan.
                if reply.is_closed() {
                    return;
                }
                let answer = self.machine.known_ids(ids, now);
                self.reply(move || {
                    let _ = reply.send(answer);
                });
            }
            PoolCommand::RejectedIds { ids, reply } => {
                // A caller that gave up on the lookup costs the core no scan.
                if reply.is_closed() {
                    return;
                }
                let answer = self.machine.rejected_ids(ids, now);
                self.reply(move || {
                    let _ = reply.send(answer);
                });
            }
        }
    }

    /// Has `tips` told about each chain event the core applies, as the
    /// node's trace records each tip today's pool applies.
    pub fn on_tip_applied(mut self, tips: impl FnMut(&TipApplied) + Send + 'static) -> Self {
        self.tips = Some(Box::new(tips));
        self
    }

    /// One chain event.
    fn chain_event(&mut self, event: ChainEvent) {
        let now = time::Instant::now();
        let was_active = self.machine.is_active();
        let held_before = self
            .machine
            .pooled_count()
            .saturating_add(self.machine.undecided_count())
            .saturating_add(self.machine.queued_count());
        let (step, reset) = match event {
            ChainEvent::Grow(block) => (self.machine.grow(block, now), false),
            ChainEvent::Reset(tip) => (self.machine.reset(tip, now), true),
        };
        // Today's pool writes its trace row only for a tip it applies, and it
        // applies none while it is off.
        if let Some(tips) = self.tips.as_mut().filter(|_| was_active) {
            tips(&TipApplied {
                tip: self.machine.tip(),
                reset,
                pooled: self.machine.pooled_count(),
                held_before,
                took: time::Instant::now().saturating_duration_since(now),
            });
        }
        self.finish(step);
    }

    /// One verification's report.
    fn report(&mut self, report: Report<E>) {
        self.jobs.remove(&report.job);
        let step =
            self.machine
                .outcome(report.job, report.outcome, time::Instant::now(), wall_now());
        self.finish(step);
    }

    /// The node moved toward or away from the tip.
    fn near_tip(&mut self) {
        let near = *self.near_tip.borrow_and_update();
        let step = self.machine.near_tip(near, time::Instant::now());
        self.finish(step);
    }

    /// Keeps an answer to send at the next [`Self::flush`], after the steps
    /// taken so far are published.
    fn reply(&mut self, send: impl FnOnce() + Send + 'static) {
        self.replies.push(Box::new(send));
    }

    /// Keeps a step to carry out at the next [`Self::flush`].
    fn finish(&mut self, step: Step) {
        self.steps.push(step);
    }

    /// Carries out every step taken since the last flush. See the module
    /// docs for the order.
    fn flush(&mut self) {
        let steps = std::mem::take(&mut self.steps);
        let (batches, effects): (Vec<_>, Vec<_>) = steps
            .into_iter()
            .map(|step| (step.changes, step.effects))
            .unzip();
        self.publish(batches);
        for reply in std::mem::take(&mut self.replies) {
            reply();
        }
        for effect in effects.into_iter().flatten() {
            match effect {
                Effect::Start {
                    job,
                    tx,
                    tip,
                    after_reset,
                } => self.start(job, tx, tip, after_reset),
                Effect::Cancel(job) => {
                    self.jobs.remove(&job);
                }
                Effect::CancelFetch(reservation) => {
                    if let Some(cancel) = self.fetches.remove(&reservation) {
                        cancel.cancel();
                    }
                }
                Effect::Verdict { waiter, verdict } => {
                    if let Some(reply) = self.verdicts.remove(&waiter) {
                        let _ = reply.send(verdict);
                    }
                }
            }
        }
        // Today's gauge of the transactions being brought in. A count this
        // small converts to `f64` exactly.
        metrics::gauge!("mempool.currently.queued.transactions")
            .set(self.machine.undecided_count() as f64);
        self.emit_storage_metrics();
    }

    /// Emits what storage holds under today's pool metric names, if it
    /// changed since the last emission. Counters grow by what storage counted
    /// since then.
    fn emit_storage_metrics(&mut self) {
        let stats = self.machine.storage_stats();
        if stats == self.emitted {
            return;
        }
        let last = std::mem::replace(&mut self.emitted, stats);
        let stats = &self.emitted;

        // Metrics take `f64`. Counts and sizes this small convert exactly, and
        // a metric tolerates rounding above 2^53.
        let weights = &stats.weights;
        for (bucket, unpaid) in [
            ("< 0.2", weights.unpaid_actions_with_weight_lt20pct),
            ("< 0.4", weights.unpaid_actions_with_weight_lt40pct),
            ("< 0.6", weights.unpaid_actions_with_weight_lt60pct),
            ("< 0.8", weights.unpaid_actions_with_weight_lt80pct),
            ("< 1", weights.unpaid_actions_with_weight_lt1),
        ] {
            metrics::gauge!("zcash.mempool.actions.unpaid", "bk" => bucket).set(f64::from(unpaid));
        }
        metrics::gauge!("zcash.mempool.actions.paid").set(f64::from(weights.paid_actions));
        metrics::gauge!("zcash.mempool.size.transactions").set(stats.transactions as f64);
        for (bucket, size) in [
            ("< 1", weights.size_with_weight_lt1),
            ("1", weights.size_with_weight_eq1),
            ("> 1", weights.size_with_weight_gt1),
            ("> 2", weights.size_with_weight_gt2),
            ("> 3", weights.size_with_weight_gt3),
        ] {
            metrics::gauge!("zcash.mempool.size.weighted", "bk" => bucket).set(size as f64);
        }
        metrics::gauge!("zcash.mempool.size.bytes").set(stats.serialized_size as f64);
        metrics::gauge!("zcash.mempool.cost.bytes").set(stats.cost as f64);

        metrics::gauge!("mempool.rejected.transaction.ids").set(stats.rejected as f64);
        // This is just an approximation, as today.
        let item_size = std::mem::size_of::<(
            zakura_chain::transaction::Hash,
            SameEffectsTipRejectionError,
        )>();
        metrics::gauge!("mempool.rejected.transaction.ids.bytes")
            .set(stats.rejected.saturating_mul(item_size) as f64);

        metrics::gauge!("zcash.mempool.eviction_cost.floor").set(stats.eviction_cost_floor);
        let evicted = stats.evicted.saturating_sub(last.evicted);
        if evicted > 0 {
            metrics::counter!("zcash.mempool.evicted").increment(evicted);
        }
        let below = stats
            .below_eviction_cost
            .saturating_sub(last.below_eviction_cost);
        if below > 0 {
            metrics::counter!("zcash.mempool.rejected.below_eviction_cost").increment(below);
        }
    }

    /// Publishes the batches, and one snapshot after them, when a batch
    /// changed anything or the snapshot's tip or state would differ. A
    /// snapshot copies the pool's indexes, so steps that changed
    /// nothing a reader sees publish nothing.
    ///
    /// Batches with no changes are published too when anything is, so the
    /// log's batch numbers stay one per input.
    fn publish(&mut self, batches: Vec<Vec<zakura_node_services::mempool::port::Change>>) {
        let now = Published {
            tip: self.machine.tip(),
            active: self.machine.is_active(),
        };
        if batches.iter().all(Vec::is_empty) && self.published == Some(now) {
            return;
        }
        self.published = Some(now);
        let machine = &self.machine;
        // Every change to what the pool holds is logged, so the last snapshot
        // plus these changes is the pool now. The first snapshot starts from
        // the empty pool the core starts with.
        let previous = self.inbox.published();
        let empty = PoolSnapshot::inactive(now.tip, ChangeSeq(0));
        let previous = previous.as_deref().unwrap_or(&empty);
        let changes: Vec<_> = batches.iter().flatten().copied().collect();
        self.inbox.publish_batches(batches, |last_change| {
            machine.next_snapshot(previous, &changes, last_change)
        });
    }

    /// Starts a verification task for `job`.
    fn start(&mut self, job: JobId, tx: UnminedTx, tip: ChainTip, after_reset: bool) {
        // A panic while the verifier builds its future is a local fault too,
        // not the end of the core.
        let verification = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.verifier.verify(tx, tip, after_reset)
        }))
        .unwrap_or_else(|_| {
            Box::pin(std::future::ready(Outcome::LocalFault(
                LocalFault::VerifyPanicked,
            )))
        });
        let reports = self.reports_tx.clone();
        let handle = task::spawn_named("mempool-verify", async move {
            // The verifier runs in a task of its own, so a panic in it is
            // a local fault and not the end of this one.
            let inner =
                AbortOnDropHandle::new(task::spawn_named("mempool-verify-call", verification));
            let outcome = match inner.await {
                Ok(outcome) => outcome,
                Err(error) if error.is_panic() => Outcome::LocalFault(LocalFault::VerifyPanicked),
                // Cancelled with this task.
                Err(_) => return,
            };
            // The core is gone if this fails.
            let _ = reports.send(Report { job, outcome });
        });
        self.jobs.insert(job, AbortOnDropHandle::new(handle));
    }
}

/// The wall-clock time, from `zakura_rt` so the simulator can set it.
fn wall_now() -> DateTime<Utc> {
    zakura_rt::time::utc_now().into()
}
