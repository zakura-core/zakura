//! The core through its port, compared with the machine alone.
//!
//! The machine's own tests show the machine is right. This shows the task
//! around it adds nothing and loses nothing. A random run drives the core
//! through its port and gives a second machine the same inputs directly, one
//! at a time. After each one, everything a caller can see must be what the
//! second machine says: every reply, verdict, output and lookup, every
//! verification the core starts or cancels and the tip and flags it starts
//! it with, the snapshot, and the change log with its batches.
//!
//! The verifier here answers only when the test tells it to, with an outcome
//! the test also hands the second machine, so the two see every outcome in
//! one order. Time is paused and moves only when the test moves it.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use chrono::Utc;
use proptest::test_runner::TestRunner;
use tokio::task::JoinHandle;
use zakura_chain::{
    block::{self, Height},
    transaction::{self, UnminedTx, UnminedTxId},
    transparent::OutPoint,
};
use zakura_node_services::{
    block_memory::TransactionHandle,
    mempool::port::{
        pool_port, ChainEvent, ChainTip, Change, ChangeSeq, ChangesSince, Charge, ConnectedBlock,
        PoolHandle, PoolSnapshot, Reservation, ReservationId, Verdict,
    },
};
use zakura_rt::{
    collections::{HashMap, HashSet},
    sync::{mpsc, oneshot, watch},
    time::{self, Instant},
};

use super::Feed;
use crate::{
    config::Config,
    io::{PoolCore, Verifier},
    machine::{
        tests::{build, tip, TestVerifyError, Tx},
        Effect, JobId, Limits, MempoolState, Outcome, Step, WaiterId,
    },
    storage::{ExactTipRejectionError, RejectionError},
};

/// The height both sides start at.
const START: u32 = 100;

/// One verification the core asked for, held until the test answers it.
struct Call {
    tx: UnminedTxId,
    tip: ChainTip,
    revalidation: bool,
    after_reset: bool,
    /// The pool the core had published when it asked.
    snapshot: Arc<PoolSnapshot>,
    answer: oneshot::Sender<Outcome<TestVerifyError>>,
}

/// A verifier that records each call and answers when the test says.
#[derive(Clone)]
struct HeldVerifier {
    pool: zakura_node_services::mempool::port::PoolReader,
    calls: Arc<Mutex<Vec<Call>>>,
}

impl Verifier<TestVerifyError> for HeldVerifier {
    fn verify(
        &self,
        tx: UnminedTx,
        tip: ChainTip,
        revalidation: bool,
        after_reset: bool,
    ) -> Pin<Box<dyn Future<Output = Outcome<TestVerifyError>> + Send>> {
        let (answer, held) = oneshot::channel();
        self.calls
            .lock()
            .expect("no test panics holding it")
            .push(Call {
                tx: tx.id(),
                tip,
                revalidation,
                after_reset,
                snapshot: self
                    .pool
                    .snapshot()
                    .expect("the core publishes before it starts a job"),
                answer,
            });
        Box::pin(async move {
            match held.await {
                Ok(outcome) => outcome,
                // The run ended with this job still held.
                Err(_dropped) => std::future::pending().await,
            }
        })
    }
}

/// A small generator, so a failing run is named by its seed alone.
struct Dice(u64);

impl Dice {
    /// The next number below `bound`.
    fn below(&mut self, bound: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        // The remainder is below `bound`, which fits `usize`.
        (self.0 % bound as u64) as usize
    }
}

/// The core and the machine it is compared with, and what the machine says
/// the core's callers should see next.
struct Mirror {
    handle: PoolHandle,
    chain: mpsc::UnboundedSender<ChainEvent>,
    active: watch::Sender<bool>,
    core: JoinHandle<()>,
    calls: Arc<Mutex<Vec<Call>>>,
    reference: MempoolState<TestVerifyError>,
    txs: Vec<Tx>,
    /// The transactions the verifier rejects.
    invalid: HashSet<UnminedTxId>,
    /// What each block above the start mined.
    blocks: Vec<Vec<transaction::Hash>>,
    fork: u8,
    chain_tip: ChainTip,
    wanted_active: bool,
    /// The machine's steps that carried changes: the log's batches.
    log: Vec<Vec<Change>>,
    /// The batches a reader has read through the port, and its cursor.
    read: Vec<(u64, Vec<Change>)>,
    cursor: ChangeSeq,
    /// The verifications the machine started in this step.
    started: Vec<(JobId, UnminedTxId, bool, bool)>,
    /// The verifications running.
    running: Vec<(JobId, Call)>,
    cancelled: Vec<JobId>,
    cancelled_fetches: HashSet<ReservationId>,
    held: Vec<(usize, Reservation)>,
    /// Which run and input this is, for a failed comparison to name.
    at: String,
    verdicts: HashMap<WaiterId, JoinHandle<Verdict>>,
    due_verdicts: Vec<(WaiterId, Verdict)>,
}

impl Mirror {
    /// Starts a core and a machine, names one tip to both, and gives them a
    /// fixed set of transactions: four that stand alone, a parent and its two children,
    /// two that spend one chain output, two the verifier rejects, and one
    /// that expires.
    fn start() -> Self {
        let mut runner = TestRunner::deterministic();
        let mut make =
            |spends: &[OutPoint], outputs, expiry| build(&mut runner, spends, outputs, expiry);
        let mut txs: Vec<Tx> = (0..4).map(|_| make(&[], 1, None)).collect();
        let parent = make(&[], 2, None);
        let spend = |index| OutPoint {
            hash: parent.id().mined_id(),
            index,
        };
        let (first, second) = (make(&[spend(0)], 1, None), make(&[spend(1)], 1, None));
        txs.extend([parent, first, second]);
        let shared = OutPoint {
            hash: transaction::Hash([0xc0; 32]),
            index: 0,
        };
        txs.extend([make(&[shared], 1, None), make(&[shared], 1, None)]);
        let invalid: Vec<Tx> = (0..2).map(|_| make(&[], 1, None)).collect();
        let invalid_ids = invalid.iter().map(Tx::id).collect();
        txs.extend(invalid);
        txs.push(make(&[], 1, Some(Height(START + 3))));

        let (chain, events) = mpsc::unbounded_channel();
        let (handle, inbox) = pool_port();
        let (active, active_rx) = watch::channel(true);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let verifier = HeldVerifier {
            pool: handle.reader(),
            calls: calls.clone(),
        };
        let config = Config {
            tx_cost_limit: u64::MAX / 2,
            ..Config::default()
        };
        let limits = Limits {
            pending: 4,
            revalidating: 2,
        };
        // The core is off until its chain names a tip, and so is the
        // machine it is compared with.
        let no_tip = ChainTip {
            hash: block::Hash([0; 32]),
            height: Height(0),
        };
        let mut reference = MempoolState::new(&config, no_tip, Instant::now()).with_limits(limits);
        assert!(reference.deactivate(Instant::now()).is_empty());
        let core = PoolCore::with_limits(
            &config,
            Box::new(Feed(events)),
            verifier,
            inbox,
            active_rx,
            limits,
        );
        let mut mirror = Self {
            handle,
            chain,
            active,
            core: tokio::spawn(core.run()),
            calls,
            reference,
            txs,
            invalid: invalid_ids,
            blocks: Vec::new(),
            fork: 0,
            chain_tip: no_tip,
            wanted_active: true,
            log: Vec::new(),
            read: Vec::new(),
            cursor: ChangeSeq(0),
            started: Vec::new(),
            running: Vec::new(),
            cancelled: Vec::new(),
            cancelled_fetches: HashSet::default(),
            held: Vec::new(),
            at: String::new(),
            verdicts: HashMap::default(),
            due_verdicts: Vec::new(),
        };
        mirror.reset_to(tip(START, 0));
        mirror
    }

    /// Takes in one step of the machine: what the log, the verifier and the
    /// callers should see because of it.
    fn absorb(&mut self, step: Step) {
        if !step.changes.is_empty() {
            self.log.push(step.changes);
        }
        for effect in step.effects {
            match effect {
                Effect::Start {
                    job,
                    tx,
                    revalidation,
                    after_reset,
                } => self.started.push((job, tx.id(), revalidation, after_reset)),
                Effect::Cancel(job) => self.cancelled.push(job),
                Effect::CancelFetch(reservation) => {
                    self.cancelled_fetches.insert(reservation);
                }
                Effect::Verdict { waiter, verdict } => self.due_verdicts.push((waiter, verdict)),
            }
        }
    }

    /// The machine's answer to the node moving toward or away from the tip,
    /// as the core asks for it after an activity change and after each chain
    /// event.
    fn mirror_activity(&mut self) {
        if self.wanted_active == self.reference.is_active() {
            return;
        }
        let now = Instant::now();
        if self.wanted_active {
            self.reference.activate(self.chain_tip, now);
            let step = self.reference.effects();
            self.absorb(step);
        } else {
            let step = self.reference.deactivate(now);
            self.absorb(step);
        }
    }

    /// Lets the core finish everything the last input caused, without moving
    /// time. It yields until everything the machine says should have
    /// happened has: the verifications started, the cancelled ones stopped,
    /// the answers delivered and the snapshot published. Then it yields a
    /// little more, so anything that should not happen has had its chance.
    async fn settle(&self) {
        for _ in 0..1_000 {
            tokio::task::yield_now().await;
            if self.caught_up() {
                break;
            }
        }
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
    }

    /// Whether the core shows everything the machine's steps so far call
    /// for. [`Self::compare`] checks each of these, and the rest, in full.
    fn caught_up(&self) -> bool {
        let started = self.calls.lock().expect("not poisoned").len() >= self.started.len();
        let cancelled = self.cancelled.iter().all(|job| {
            self.running
                .iter()
                .find(|(running, _)| running == job)
                .is_none_or(|(_, call)| call.answer.is_closed())
        });
        let verdicts = self.due_verdicts.iter().all(|(waiter, _)| {
            self.verdicts
                .get(waiter)
                .is_some_and(JoinHandle::is_finished)
        });
        let logged: usize = self.log.iter().map(Vec::len).sum();
        let published = self.handle.snapshot().is_some_and(|snapshot| {
            (
                snapshot.active,
                snapshot.tip,
                usize::try_from(snapshot.last_change.0).ok(),
            ) == (
                self.reference.is_active(),
                self.reference.tip(),
                Some(logged),
            )
        });
        started && cancelled && verdicts && published
    }

    /// Ends one input: gives the machine any deadline already due, as the
    /// core's timer would, lets the core settle, and compares the two.
    async fn compare(&mut self, what: &str) {
        let what = &format!("{}: {what}", self.at);
        for _ in 0..8 {
            let now = Instant::now();
            if self.reference.next_deadline().is_none_or(|due| due > now) {
                break;
            }
            let step = self.reference.tick(now);
            self.absorb(step);
        }
        self.settle().await;
        self.reference.assert_invariants();

        // The verifications the core started are the machine's, in order,
        // at its tip, with the flags the core passes on.
        let calls: Vec<Call> = std::mem::take(&mut *self.calls.lock().expect("not poisoned"));
        let started = std::mem::take(&mut self.started);
        assert_eq!(calls.len(), started.len(), "{what}: verifications started");
        for (call, (job, tx, revalidation, after_reset)) in calls.into_iter().zip(started) {
            let after_reset = after_reset || self.reference.job_after_reset(job);
            assert_eq!(
                (call.tx, call.tip, call.revalidation, call.after_reset),
                (tx, self.reference.tip(), revalidation, after_reset),
                "{what}: a verification's transaction, tip and flags"
            );
            self.running.push((job, call));
        }
        for job in std::mem::take(&mut self.cancelled) {
            if let Some(at) = self.running.iter().position(|(running, _)| *running == job) {
                let (_, call) = self.running.remove(at);
                assert!(call.answer.is_closed(), "{what}: a cancelled job stops");
            }
        }
        for (_, call) in &self.running {
            assert!(
                !call.answer.is_closed(),
                "{what}: a running job keeps running"
            );
        }

        // Each caller the machine answered has that answer, and no other
        // caller has one.
        for (waiter, verdict) in std::mem::take(&mut self.due_verdicts) {
            let task = self
                .verdicts
                .remove(&waiter)
                .expect("the machine answers a submission the test made");
            assert!(task.is_finished(), "{what}: a verdict reaches its caller");
            let got = task.await.expect("the submission does not panic");
            assert_eq!(
                format!("{got:?}"),
                format!("{verdict:?}"),
                "{what}: a verdict"
            );
        }
        for task in self.verdicts.values() {
            assert!(!task.is_finished(), "{what}: an undecided submission waits");
        }
        for (_, reservation) in &self.held {
            assert_eq!(
                reservation.cancellation().is_cancelled(),
                self.cancelled_fetches.contains(&reservation.id()),
                "{what}: a fetch is cancelled when the machine cancels it"
            );
        }

        // The snapshot is the machine's, but for the insertion times, which
        // each side reads from the wall clock.
        let logged: usize = self.log.iter().map(Vec::len).sum();
        let logged = ChangeSeq(u64::try_from(logged).expect("a short log"));
        let expected = self.reference.snapshot(logged);
        let view = self.handle.view(self.cursor).expect("a published snapshot");
        let latest = self
            .handle
            .snapshot()
            .expect("the core publishes when it starts");
        let untimed = |snapshot: &PoolSnapshot| {
            snapshot
                .transactions
                .iter()
                .cloned()
                .map(|mut tx| {
                    tx.time = None;
                    tx
                })
                .collect::<Vec<_>>()
        };
        // Both the latest snapshot and the one a reader gets with the log.
        for published in [&latest, &view.snapshot] {
            assert_eq!(
                (published.active, published.tip, published.last_change),
                (expected.active, expected.tip, expected.last_change),
                "{what}: the snapshot's state, tip and position"
            );
            assert!(
                untimed(published) == untimed(&expected),
                "{what}: the snapshot's transactions, in mining order"
            );
            assert_eq!(
                format!("{:?}", sorted(published)),
                format!("{:?}", sorted(&expected)),
                "{what}: the snapshot's dependencies"
            );
        }

        // A reader gets the machine's changes, in its batches.
        let ChangesSince::Entries(entries) = view.changes else {
            panic!("{what}: the reader keeps up with the log");
        };
        for entry in entries {
            assert_eq!(entry.seq.0, self.cursor.0 + 1, "{what}: entries in order");
            self.cursor = entry.seq;
            match self.read.last_mut() {
                Some((batch, changes)) if *batch == entry.batch => changes.push(entry.change),
                _ => self.read.push((entry.batch, vec![entry.change])),
            }
        }
        assert_eq!(
            self.cursor, view.snapshot.last_change,
            "{what}: the reader's position"
        );
        let read: Vec<&Vec<Change>> = self.read.iter().map(|(_, changes)| changes).collect();
        assert_eq!(read, self.log.iter().collect::<Vec<_>>(), "{what}: the log");
    }

    /// Reserves `tx`, and holds the reservation if one is granted.
    async fn reserve(&mut self, tx: usize) {
        let id = self.txs[tx].id();
        let expected = self.reference.reserve(id, Instant::now());
        let step = self.reference.effects();
        self.absorb(step);
        match (expected, answered(self.handle.reserve(id)).await) {
            (Ok(granted), Ok(reservation)) => {
                assert_eq!(
                    (reservation.id(), reservation.tx_id()),
                    (granted, id),
                    "{}: a grant reaches its caller as the machine made it",
                    self.at
                );
                self.held.push((tx, reservation));
            }
            (Err(expected), Err(got)) => {
                assert_eq!(
                    format!("{got:?}"),
                    format!("{expected:?}"),
                    "{}: a refusal",
                    self.at
                )
            }
            (expected, got) => panic!(
                "{}: the machine answered {expected:?} and the core {got:?}",
                self.at
            ),
        }
    }

    /// Submits the body under a held reservation.
    fn submit(&mut self, held: usize) {
        let (tx, reservation) = self.held.remove(held);
        let body = self.txs[tx].real.transaction.clone();
        let (waiter, step) = self
            .reference
            .submit(reservation.id(), body.clone(), Instant::now());
        let handle = self.handle.clone();
        let task = tokio::spawn(async move { handle.submit(reservation, body).await });
        self.verdicts.insert(waiter, task);
        self.absorb(step);
    }

    /// Answers a running verification, on both sides, with what a verifier
    /// reading the chain now and the pool the job started with would find.
    fn answer(&mut self, running: usize) {
        let (job, call) = self.running.remove(running);
        let mined: HashSet<transaction::Hash> = self.blocks.iter().flatten().copied().collect();
        let known: HashSet<transaction::Hash> =
            self.txs.iter().map(|tx| tx.id().mined_id()).collect();
        let tx = self
            .txs
            .iter()
            .find(|tx| tx.id() == call.tx)
            .expect("the core verifies only the test's transactions");
        let outcome = if self.invalid.contains(&call.tx) {
            Outcome::Fail {
                error: RejectionError::ExactTip(ExactTipRejectionError::FailedVerification(
                    TestVerifyError,
                )),
                charge: Charge::Cooldown,
            }
        } else if mined.contains(&call.tx.mined_id()) {
            Outcome::AlreadyMined(Arc::new(TestVerifyError))
        } else {
            let (pool_inputs, missing): (Vec<OutPoint>, Vec<OutPoint>) = tx
                .real
                .transaction
                .transaction()
                .spent_outpoints()
                .filter(|outpoint| {
                    known.contains(&outpoint.hash) && !mined.contains(&outpoint.hash)
                })
                .partition(|outpoint| call.snapshot.created_output(outpoint).is_some());
            if missing.is_empty() {
                Outcome::Pass {
                    tx: tx.real.clone(),
                    pool_inputs,
                }
            } else {
                Outcome::MissingInputs {
                    missing,
                    error: TestVerifyError,
                }
            }
        };
        let step = self
            .reference
            .outcome(job, outcome.clone(), Instant::now(), Utc::now());
        self.absorb(step);
        let _ = call.answer.send(outcome);
    }

    /// The transactions of `mines` the chain has not mined yet.
    fn unmined(&self, mines: Vec<usize>) -> Vec<Tx> {
        let mined: HashSet<transaction::Hash> = self.blocks.iter().flatten().copied().collect();
        mines
            .into_iter()
            .map(|tx| self.txs[tx].clone())
            .filter(|tx| !mined.contains(&tx.id().mined_id()))
            .collect()
    }

    /// The tip of the chain the test keeps.
    fn tip(&self) -> ChainTip {
        let height = START + u32::try_from(self.blocks.len()).expect("a short chain");
        tip(height, self.fork)
    }

    /// Connects one block that mines `mines`.
    fn grow(&mut self, mines: Vec<usize>) {
        let mines = self.unmined(mines);
        self.blocks
            .push(mines.iter().map(|tx| tx.id().mined_id()).collect());
        let after = self.tip();
        let block = ConnectedBlock {
            hash: after.hash,
            height: after.height,
            transactions: mines
                .iter()
                .map(|tx| TransactionHandle::Shared(tx.real.transaction.transaction().clone()))
                .collect(),
            transaction_hashes: mines.iter().map(|tx| tx.id().mined_id()).collect(),
        };
        self.chain
            .send(ChainEvent::Grow(block.clone()))
            .expect("the core reads its feed");
        let step = self.reference.grow(block, Instant::now());
        self.absorb(step);
        self.chain_tip = after;
        self.mirror_activity();
    }

    /// Takes the tip block off and connects one on another fork that mines
    /// `mines`. Both sides are told only the new tip.
    fn reorg(&mut self, mines: Vec<usize>) {
        self.blocks.pop();
        self.fork = self.fork.wrapping_add(1);
        self.skip(mines, 1);
    }

    /// Connects `blocks` blocks at once, the first of which mines `mines`.
    /// Both sides are told only the new tip.
    fn skip(&mut self, mines: Vec<usize>, blocks: usize) {
        let mines = self.unmined(mines);
        self.blocks
            .push(mines.iter().map(|tx| tx.id().mined_id()).collect());
        for _ in 1..blocks {
            self.blocks.push(Vec::new());
        }
        self.reset_to(self.tip());
    }

    /// Tells both sides the tip is `tip`, and nothing else.
    fn reset_to(&mut self, tip: ChainTip) {
        self.chain
            .send(ChainEvent::Reset(tip))
            .expect("the core reads its feed");
        let step = self.reference.reset(tip, Instant::now());
        self.absorb(step);
        self.chain_tip = tip;
        self.mirror_activity();
    }

    /// Moves time on by `by`, stopping at each of the machine's deadlines on
    /// the way so both sides apply it at the same instant.
    async fn advance(&mut self, by: Duration) {
        let target = Instant::now() + by;
        while let Some(due) = self.reference.next_deadline().filter(|due| *due <= target) {
            time::sleep_until(due).await;
            self.compare("a deadline").await;
            assert!(
                self.reference.next_deadline().is_none_or(|next| next > due),
                "{}: a deadline is applied once it is reached",
                self.at
            );
        }
        time::sleep_until(target).await;
        self.compare("time passing").await;
    }

    /// Does one random thing to both sides and compares them.
    async fn step(&mut self, dice: &mut Dice) {
        let any_tx = dice.below(self.txs.len());
        match dice.below(100) {
            0..=24 => {
                self.reserve(any_tx).await;
                self.compare("a reservation").await;
            }
            25..=44 if !self.held.is_empty() => {
                self.submit(dice.below(self.held.len()));
                self.compare("a submission").await;
            }
            45..=49 if !self.held.is_empty() => {
                let (_, reservation) = self.held.remove(dice.below(self.held.len()));
                let step = self
                    .reference
                    .fetch_failed(reservation.id(), Instant::now());
                self.absorb(step);
                answered(self.handle.fetch_failed(reservation)).await;
                self.compare("a failed fetch").await;
            }
            50..=53 if !self.held.is_empty() => {
                let (_, reservation) = self.held.remove(dice.below(self.held.len()));
                let step = self.reference.release(reservation.id(), Instant::now());
                self.absorb(step);
                drop(reservation);
                self.compare("a dropped reservation").await;
            }
            54..=73 if !self.running.is_empty() => {
                // Mostly the oldest, sometimes another.
                let which = if dice.below(4) == 0 {
                    dice.below(self.running.len())
                } else {
                    0
                };
                self.answer(which);
                self.compare("a verification's outcome").await;
            }
            74..=75 if self.held.len() >= 2 => {
                // Two failed fetches the core takes in one pass, as two
                // batches. One task sends both, so they reach the core's
                // queue in this order whatever the scheduler does.
                let (_, first) = self.held.remove(0);
                let (_, second) = self.held.remove(0);
                for reservation in [&first, &second] {
                    let step = self
                        .reference
                        .fetch_failed(reservation.id(), Instant::now());
                    self.absorb(step);
                }
                answered(async {
                    futures::join!(
                        self.handle.fetch_failed(first),
                        self.handle.fetch_failed(second)
                    )
                })
                .await;
                self.compare("two failed fetches at once").await;
            }
            74..=79 => {
                let mines = (0..dice.below(3))
                    .map(|_| dice.below(self.txs.len()))
                    .collect();
                self.grow(mines);
                self.compare("a block").await;
            }
            80..=81 => {
                self.reorg(vec![any_tx]);
                self.compare("a reorg").await;
            }
            82..=83 => {
                self.skip(vec![any_tx], 2);
                self.compare("two blocks at once").await;
            }
            84..=91 => {
                let ids: HashSet<UnminedTxId> = (0..3)
                    .map(|_| self.txs[dice.below(self.txs.len())].id())
                    .collect();
                let expected = self.reference.rejected_ids(ids.clone(), Instant::now());
                let step = self.reference.effects();
                self.absorb(step);
                assert_eq!(
                    answered(self.handle.rejected_ids(ids)).await,
                    expected,
                    "{}: a lookup",
                    self.at
                );
                self.compare("a rejection lookup").await;
            }
            92 => {
                self.wanted_active = !self.wanted_active;
                self.active.send_replace(self.wanted_active);
                self.mirror_activity();
                self.compare("the pool turning on or off").await;
            }
            _ => {
                let by = [1, 1_000, 30_000, 120_000, 1_200_000][dice.below(5)];
                self.advance(Duration::from_millis(by)).await;
            }
        }
    }
}

/// `call`'s answer, which the core gives without time passing. A call it
/// never answers fails here once nothing else can run.
async fn answered<T>(call: impl Future<Output = T>) -> T {
    time::timeout(Duration::from_secs(1), call)
        .await
        .expect("the core answers the call")
}

/// `snapshot`'s dependency edges, both ways, in a fixed order.
#[allow(clippy::type_complexity)]
fn sorted(snapshot: &PoolSnapshot) -> Vec<Vec<(transaction::Hash, Vec<transaction::Hash>)>> {
    [
        snapshot.dependencies.dependencies(),
        snapshot.dependencies.dependents(),
    ]
    .into_iter()
    .map(|edges| {
        let mut edges: Vec<(transaction::Hash, Vec<transaction::Hash>)> = edges
            .iter()
            .map(|(tx, others)| {
                let mut others: Vec<_> = others.iter().copied().collect();
                others.sort();
                (*tx, others)
            })
            .collect();
        edges.sort();
        edges
    })
    .collect()
}

/// The core, driven through its port by random inputs, shows its callers
/// exactly what the machine alone says they should see, after every input.
#[tokio::test(start_paused = true)]
async fn the_core_shows_its_callers_what_the_machine_decides() {
    let runs: u64 = std::env::var("POOL_MIRROR_RUNS")
        .ok()
        .and_then(|runs| runs.parse().ok())
        .unwrap_or(200);
    for seed in 1..=runs {
        let mut dice = Dice(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
        let mut mirror = Mirror::start();
        mirror.at = format!("seed {seed}, the start");
        mirror.compare("the first snapshot").await;
        for at in 0..150 {
            mirror.at = format!("seed {seed}, input {at}");
            mirror.step(&mut dice).await;
        }
        mirror.core.abort();
    }
}
