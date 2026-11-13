//! The pool core's task: [`PoolCore`] owns the machine and serves the port.
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
//! 2. It publishes the cooldowns, if one started.
//! 3. It sends the answers to the commands it took, so no caller hears of a
//!    change, such as a cooldown, before readers can see it.
//! 4. It does each step's effects in order: starts and cancels
//!    verifications, sends verdicts, and answers output waiters.
//!
//! The pool stays off until the chain names a real tip, whatever the
//! activity signal says. A verifier the core owns reads the pool through a
//! `PoolReader`, so it does not keep the core running once every handle is
//! gone.
//!
//! A verification runs in a task of its own, outside the core's task. It
//! reads the generation before its first state lookup and after its last,
//! and comes back as an input with both readings. See
//! [`MempoolState::outcome`] for how the machine uses them.
//!
//! Every name the task uses for time, tasks and channels comes from
//! `zakura_rt`, so it builds and runs under the simulator.

use std::{future::Future, pin::Pin};

use chrono::{DateTime, Utc};
use zakura_chain::{
    block::{self, Height},
    transaction::UnminedTx,
    transparent,
};
use zakura_node_services::mempool::port::{
    AwaitOutputError, ChainEvent, ChainEvents, ChainStart, ChainTip, Generation, GenerationCell,
    GenerationRead, LocalFault, PoolCommand, PoolInbox, ReservationId, Verdict,
};
use zakura_rt::{
    collections::HashMap,
    sync::{mpsc, oneshot, watch, CancellationToken},
    task::{self, AbortOnDropHandle},
    time,
};

use crate::{
    config::Config,
    machine::{
        Effect, JobId, Limits, MempoolState, Outcome, OutputAnswer, Step, VerifyError, WaiterId,
    },
};

#[cfg(test)]
mod tests;

/// The most inputs the core takes before it publishes. Every input that is
/// already waiting, up to this many, shares one snapshot, so a burst of
/// outcomes while a large pool is restored does not copy the pool once per
/// outcome.
const MAX_INPUTS_PER_PUBLISH: usize = 64;

/// The most times a verification yields while it waits for the state writer
/// to finish a change before its first lookup. A change is short. A worker
/// that waits this long goes ahead, and its outcome is then stale.
const STABLE_WAIT_YIELDS: usize = 10_000;

/// What the core asks of the node to verify one transaction: the
/// already-mined lookup, the tip lookup, then the verifier, without the
/// fetch. The core reads the generation around the whole call.
pub trait Verifier<E>: Clone + Send + Sync + 'static {
    /// Verifies `tx` for the block after `tip`. `revalidation` says it is a
    /// pooled transaction being checked again.
    ///
    /// The core calls this in the step that starts the job, after that
    /// step's snapshot is published. The returned future must not read the
    /// state until it is polled: the core reads the generation before it
    /// polls it. A panic here, or in the future, is a local fault.
    ///
    /// `after_reset` says the chain was reset since the transaction's body
    /// arrived. See `Effect::Start`.
    fn verify(
        &self,
        tx: UnminedTx,
        tip: ChainTip,
        revalidation: bool,
        after_reset: bool,
    ) -> Pin<Box<dyn Future<Output = Outcome<E>> + Send>>;
}

/// A verification's report to the core.
struct Report<E> {
    job: JobId,
    outcome: Outcome<E>,
    before: GenerationRead,
    after: GenerationRead,
}

/// What the snapshot last published carried, beside the transactions.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct Published {
    tip: ChainTip,
    generation: Generation,
    active: bool,
}

/// The pool core: the machine, the port's inbox, and the work in flight.
pub struct PoolCore<E, V> {
    machine: MempoolState<E>,
    inbox: PoolInbox,
    chain: ChainEvents,
    generation: GenerationCell,
    verifier: V,
    /// Whether the node is near enough to the tip for the pool to be on.
    active: watch::Receiver<bool>,
    /// The latest tip and generation from the chain, which the machine
    /// skips while it is off and takes when it turns on.
    chain_tip: ChainTip,
    chain_generation: Generation,
    /// Whether the chain has named a real tip. Before it has, the pool stays
    /// off, whatever the activity signal says.
    has_tip: bool,
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
    /// Callers waiting for an output.
    outputs: HashMap<WaiterId, oneshot::Sender<Result<transparent::Output, AwaitOutputError>>>,
    published: Option<Published>,
    /// Steps taken and not yet carried out.
    steps: Vec<Step>,
    /// Answers to commands, sent once the steps before them are published,
    /// so no caller hears of a change before readers can see it.
    replies: Vec<Box<dyn FnOnce() + Send>>,
}

impl<E: VerifyError, V: Verifier<E>> PoolCore<E, V> {
    /// A core that starts at `start`, reads chain events from `chain`, and
    /// serves `inbox`. It is on while `active` holds `true`.
    pub fn new(
        config: &Config,
        start: ChainStart,
        chain: ChainEvents,
        generation: GenerationCell,
        verifier: V,
        inbox: PoolInbox,
        active: watch::Receiver<bool>,
    ) -> Self {
        Self::with_limits(
            config,
            start,
            chain,
            generation,
            verifier,
            inbox,
            active,
            Limits::default(),
        )
    }

    /// [`Self::new`] with other limits.
    #[allow(clippy::too_many_arguments)]
    pub fn with_limits(
        config: &Config,
        start: ChainStart,
        chain: ChainEvents,
        generation: GenerationCell,
        verifier: V,
        inbox: PoolInbox,
        active: watch::Receiver<bool>,
        limits: Limits,
    ) -> Self {
        // Before the genesis block there is no tip, and the pool stays off
        // until the first transition names one.
        let chain_tip = start.tip.unwrap_or(ChainTip {
            hash: block::Hash([0; 32]),
            height: Height(0),
        });
        let machine = MempoolState::new(config, chain_tip, start.generation, time::Instant::now())
            .with_limits(limits);
        let (reports_tx, reports) = mpsc::unbounded_channel();
        Self {
            machine,
            inbox,
            chain,
            generation,
            verifier,
            active,
            chain_tip,
            chain_generation: start.generation,
            has_tip: start.tip.is_some(),
            reports_tx,
            reports,
            jobs: HashMap::default(),
            fetches: HashMap::default(),
            verdicts: HashMap::default(),
            outputs: HashMap::default(),
            published: None,
            steps: Vec::new(),
            replies: Vec::new(),
        }
    }

    /// Runs the core until every port handle is gone or the chain's sender
    /// is dropped.
    pub async fn run(mut self) {
        // The machine starts on. Until the node is near a tip the chain has
        // named, it is off, and the first snapshot says so.
        if !*self.active.borrow_and_update() || !self.has_tip {
            let step = self.machine.deactivate(time::Instant::now());
            self.finish(step);
        }
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
                changed = self.active.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    self.activity();
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
                if let Some(event) = self.chain.try_next() {
                    self.chain_event(event);
                } else if let Ok(report) = self.reports.try_recv() {
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
            PoolCommand::Reserve {
                tx_id,
                source,
                intent,
                reply,
            } => {
                let answer = self
                    .machine
                    .reserve(tx_id, source, intent, now)
                    .map(|grant| {
                        let cancel = CancellationToken::new();
                        if !grant.joined {
                            self.fetches.insert(grant.id, cancel.clone());
                        }
                        self.inbox
                            .grant(grant.id, tx_id, grant.joined, grant.wants_body, cancel)
                    });
                // A caller that left drops the reservation with the reply,
                // which frees the slot.
                self.reply(move || {
                    let _ = reply.send(answer);
                });
                let step = self.machine.effects();
                self.finish(step);
            }
            PoolCommand::Submit {
                reservation,
                transaction,
                supplier,
                reply,
            } => {
                // The caller gave up after the command was queued. Dropping
                // the reservation frees its slot, as the port promises.
                if reply.is_closed() {
                    drop(reservation);
                    return;
                }
                let id = reservation.accept();
                self.fetches.remove(&id);
                let (waiter, step) = self.machine.submit(id, transaction, supplier, now);
                self.verdicts.insert(waiter, reply);
                self.finish(step);
            }
            PoolCommand::Join {
                reservation,
                supplier,
                reply,
            } => {
                if reply.is_closed() {
                    drop(reservation);
                    return;
                }
                let tx_id = reservation.tx_id();
                let id = reservation.accept();
                let (waiter, step) = self.machine.wait(id, tx_id, supplier, now);
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
            PoolCommand::ReportViolation(violation) => {
                self.machine.violation(&violation.peer, now);
                let step = self.machine.effects();
                self.finish(step);
            }
            PoolCommand::AwaitOutput { outpoint, reply } => {
                let outputs = &self.outputs;
                let mut closed = Vec::new();
                let answer = self.machine.await_output(outpoint, now, |waiter| {
                    let gone = outputs.get(&waiter).is_none_or(oneshot::Sender::is_closed);
                    if gone {
                        closed.push(waiter);
                    }
                    gone
                });
                for waiter in closed {
                    self.outputs.remove(&waiter);
                }
                let answer = match answer {
                    OutputAnswer::Ready(output) => Ok(output),
                    OutputAnswer::Waiting(waiter) => {
                        self.outputs.insert(waiter, reply);
                        let step = self.machine.effects();
                        return self.finish(step);
                    }
                    OutputAnswer::TooManyWaiters => Err(AwaitOutputError::TooManyWaiters),
                    OutputAnswer::Inactive => Err(AwaitOutputError::Inactive),
                };
                self.reply(move || {
                    let _ = reply.send(answer);
                });
                let step = self.machine.effects();
                self.finish(step);
            }
            PoolCommand::RejectedIds { ids, reply } => {
                let answer = self.machine.rejected_ids(ids, now);
                self.reply(move || {
                    let _ = reply.send(answer);
                });
                let step = self.machine.effects();
                self.finish(step);
            }
        }
    }

    /// One chain event. The core remembers the tip even while it is off.
    fn chain_event(&mut self, event: ChainEvent) {
        let now = time::Instant::now();
        let step = match event {
            ChainEvent::Transition(transition) => {
                self.chain_tip = transition.after;
                self.chain_generation = transition.generation;
                self.machine.transition(transition, now)
            }
            ChainEvent::Resync { tip, generation } => {
                self.chain_tip = tip;
                self.chain_generation = generation;
                self.machine.resync(tip, generation, now)
            }
        };
        self.has_tip = true;
        self.finish(step);
        // The pool may have been waiting only for a real tip.
        self.activity();
    }

    /// One verification's report.
    fn report(&mut self, report: Report<E>) {
        self.jobs.remove(&report.job);
        let step = self.machine.outcome(
            report.job,
            report.outcome,
            report.before,
            report.after,
            time::Instant::now(),
            wall_now(),
        );
        self.finish(step);
    }

    /// The node moved toward or away from the tip.
    fn activity(&mut self) {
        let active = *self.active.borrow_and_update() && self.has_tip;
        if active == self.machine.is_active() {
            return;
        }
        let now = time::Instant::now();
        if active {
            self.machine
                .activate(self.chain_tip, self.chain_generation, now);
            let step = self.machine.effects();
            self.finish(step);
        } else {
            let step = self.machine.deactivate(now);
            self.finish(step);
        }
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
        if let Some(cooldowns) = self.machine.cooldowns_if_changed() {
            self.inbox.publish_cooldowns(cooldowns);
        }
        for reply in std::mem::take(&mut self.replies) {
            reply();
        }
        for effect in effects.into_iter().flatten() {
            match effect {
                Effect::Start {
                    job,
                    tx,
                    revalidation,
                    after_reset,
                } => self.start(job, tx, revalidation, after_reset),
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
                Effect::OutputReady { waiter, output, .. } => {
                    if let Some(reply) = self.outputs.remove(&waiter) {
                        let _ = reply.send(Ok(output));
                    }
                }
                Effect::OutputWaitEnded(waiter) => {
                    if let Some(reply) = self.outputs.remove(&waiter) {
                        let _ = reply.send(Err(AwaitOutputError::Inactive));
                    }
                }
            }
        }
    }

    /// Publishes the batches, and one snapshot after them, when a batch
    /// changed anything or the snapshot's tip, generation or state would
    /// differ. A snapshot copies the visible pool, so steps that changed
    /// nothing a reader sees publish nothing.
    ///
    /// Batches with no changes are published too when anything is, so the
    /// log's batch numbers stay one per input.
    fn publish(&mut self, batches: Vec<Vec<zakura_node_services::mempool::port::Change>>) {
        let now = Published {
            tip: self.machine.tip(),
            generation: self.machine.generation(),
            active: self.machine.is_active(),
        };
        if batches.iter().all(Vec::is_empty) && self.published == Some(now) {
            return;
        }
        self.published = Some(now);
        let machine = &self.machine;
        self.inbox
            .publish_batches(batches, |last_change| machine.snapshot(last_change));
    }

    /// Starts a verification task for `job`.
    fn start(&mut self, job: JobId, tx: UnminedTx, revalidation: bool, after_reset: bool) {
        // The flag in the effect is as of the step that made it. A reset the
        // core took later in the same flush counts too.
        let after_reset = after_reset || self.machine.job_after_reset(job);
        // The job verifies for the machine's tip, which belongs to the
        // generation the machine has applied. A worker that first reads any
        // other generation would report a result for one tip as fresh at
        // another, so its first reading is reported as unknown instead.
        let bound = GenerationRead::Stable(self.machine.generation());
        // A panic while the verifier builds its future is a local fault too,
        // not the end of the core.
        let verification = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.verifier
                .verify(tx, self.machine.tip(), revalidation, after_reset)
        }))
        .unwrap_or_else(|_| {
            Box::pin(std::future::ready(Outcome::LocalFault(
                LocalFault::VerifyPanicked,
            )))
        });
        let generation = self.generation.clone();
        let reports = self.reports_tx.clone();
        let handle = task::spawn_named("mempool-verify", async move {
            let read = stable_reading(&generation).await;
            let before = if read == bound {
                read
            } else {
                GenerationRead::Changing
            };
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
            let after = generation.read();
            // The core is gone if this fails.
            let _ = reports.send(Report {
                job,
                outcome,
                before,
                after,
            });
        });
        self.jobs.insert(job, AbortOnDropHandle::new(handle));
    }
}

/// Reads the generation once the state writer is not in the middle of a
/// change, or after [`STABLE_WAIT_YIELDS`] tries.
async fn stable_reading(generation: &GenerationCell) -> GenerationRead {
    for _ in 0..STABLE_WAIT_YIELDS {
        let read = generation.read();
        if matches!(read, GenerationRead::Stable(_)) {
            return read;
        }
        task::yield_now().await;
    }
    generation.read()
}

/// The wall-clock time, from `zakura_rt` so the simulator can set it.
fn wall_now() -> DateTime<Utc> {
    zakura_rt::time::utc_now().into()
}
