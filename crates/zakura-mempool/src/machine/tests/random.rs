//! Random runs of the machine.
//!
//! Each run gives the machine a few hundred random inputs over a small
//! universe of transactions. After every input the [`Bench`] checks the
//! machine's invariants and the tracker's record. The run adds what it knows
//! from the inputs it chose: whether the pool is on, which transactions its
//! own blocks mined, and what a verifier would have found. At the end every
//! caller lets go, every job reports, and the run must drain.
//!
//! `POOL_RUNS` sets how many runs, `POOL_FIRST` the first seed, and
//! `POOL_TRACE` prints each run's transactions and steps to stderr.

// The trace is for a developer chasing one failing seed.
#![allow(clippy::print_stderr)]

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use proptest::{
    prelude::*,
    test_runner::{RngAlgorithm, TestRng, TestRunner},
};
use zakura_chain::{
    block::Height,
    transaction::{self, UnminedTxId},
    transparent::OutPoint,
};
use zakura_node_services::mempool::port::{Charge, LocalFault, ReservationId, Unreserved};
use zakura_rt::collections::HashSet;

use super::{build, twin, Bench, TestVerifyError, Tx, START};
use crate::{
    config::Config,
    machine::{Effect, JobId, Limits, Outcome, Step},
    storage::{
        ExactTipRejectionError, RejectionError, SameEffectsChainRejectionError,
        SameEffectsTipRejectionError,
    },
};

/// How long a run remembers a chain-scoped rejection: short, so runs reach
/// the end of it.
const REJECTION_MEMORY: Duration = Duration::from_secs(150);

/// How far one run's clock moves before it drains. Deadlines past it wait
/// for the drain.
const RUN_TIME: Duration = Duration::from_secs(9 * 60);

/// The limits of a run: small, so full states are reached.
const LIMITS: Limits = Limits {
    pending: 6,
    readmitting: 3,
};

/// A number below `n`.
fn pick(rng: &mut TestRng, n: usize) -> usize {
    let n = u64::try_from(n.max(1)).expect("a small bound");
    usize::try_from(rng.next_u64() % n).expect("a small number")
}

/// True one time in `n`.
fn chance(rng: &mut TestRng, n: usize) -> bool {
    pick(rng, n) == 0
}

/// A run's transactions: some spend chain outputs, some spend each other, two
/// spenders of one chain output conflict, and some expire soon.
fn universe(rng: &mut TestRng, runner: &mut TestRunner, size: usize) -> Vec<Tx> {
    let mut txs: Vec<Tx> = Vec::new();
    for _ in 0..size {
        let mut spends = Vec::new();
        let inputs = pick(rng, 3);
        for _ in 0..inputs {
            let spend = if !txs.is_empty() && chance(rng, 2) {
                let parent = &txs[pick(rng, txs.len())];
                parent.outpoint(u32::try_from(pick(rng, 2)).expect("a small index"))
            } else {
                // A chain output. Few of them, so spenders conflict.
                OutPoint {
                    hash: transaction::Hash(
                        [0xc0 ^ u8::try_from(pick(rng, 6)).expect("small"); 32],
                    ),
                    index: 0,
                }
            };
            if !spends.contains(&spend) {
                spends.push(spend);
            }
        }
        let expiry = chance(rng, 4)
            .then(|| Height(START + 1 + u32::try_from(pick(rng, 6)).expect("a small height")));
        let outputs = 1 + u32::try_from(pick(rng, 2)).expect("a small count");
        txs.push(build(runner, &spends, outputs, expiry));
    }
    // Some transactions get a second authorization: the same effects and
    // nullifier, other authorizing data.
    for index in 0..txs.len() {
        if chance(rng, 4) {
            let twin = twin(&txs[index]);
            txs.push(twin);
        }
    }
    txs
}

/// A granted reservation, as the run's callers hold it.
#[derive(Clone, Debug)]
struct Held {
    id: ReservationId,
    tx: usize,
}

/// One run: the machine, the callers' side of it, and the chain.
struct Run {
    rng: TestRng,
    txs: Vec<Tx>,
    bench: Bench,
    /// Whether the run has the pool turned on.
    active: bool,
    held: Vec<Held>,
    /// The effects of the pool when each job started, which is what
    /// its verifier read.
    views: BTreeMap<u64, HashSet<transaction::Hash>>,
    steps: usize,
    /// Whether the run is draining, when every job is answered for good.
    draining: bool,
    /// The transactions of the universe the current chain mined.
    mined: HashSet<transaction::Hash>,
    /// Reservations whose fetch the machine cancelled. Their callers drop
    /// them next.
    to_drop: Vec<ReservationId>,
}

impl Run {
    /// A run for `seed`.
    fn new(seed: u64) -> Self {
        let mut seed_bytes = [0u8; 32];
        seed_bytes[..8].copy_from_slice(&seed.to_le_bytes());
        let mut rng = TestRng::from_seed(RngAlgorithm::ChaCha, &seed_bytes);
        let mut runner = TestRunner::deterministic();
        let size = 8 + pick(&mut rng, 12);
        let txs = universe(&mut rng, &mut runner, size);
        let bench = Bench::with(
            Config {
                // Far above what a run pools, so nothing is evicted.
                tx_cost_limit: u64::MAX / 2,
                eviction_memory_time: REJECTION_MEMORY,
                ..Config::default()
            },
            LIMITS,
        );
        Self {
            rng,
            txs,
            bench,
            active: true,
            held: Vec::new(),
            views: BTreeMap::new(),
            steps: 0,
            draining: false,
            mined: HashSet::default(),
            to_drop: Vec::new(),
        }
    }

    /// The name a trace gives `id`.
    fn name(&self, id: &UnminedTxId) -> String {
        self.txs
            .iter()
            .position(|tx| tx.id() == *id)
            .map_or_else(|| "?".to_owned(), |index| format!("t{index}"))
    }

    /// Follows up one step the bench has already recorded and checked: the
    /// run's own callers react to it.
    fn took(&mut self, what: &str, step: &Step) {
        if std::env::var_os("POOL_TRACE").is_some() {
            eprintln!(
                "{} {what}: {:?} {:?} pooled {:?}",
                self.steps,
                step.effects,
                step.changes,
                self.bench
                    .machine
                    .pooled_ids()
                    .iter()
                    .map(|id| self.name(id))
                    .collect::<Vec<_>>()
            );
        }
        if self.bench.machine.is_parked_any() {
            self.bench.tracker.saw("parked");
        }
        let pooled_now = self.bench.tracker.pooled_effects();
        for effect in &step.effects {
            match effect {
                Effect::Start { job, .. } => {
                    self.views.insert(job.0, pooled_now.clone());
                }
                // The caller stops its fetch and drops the reservation, as
                // the adapter does, on a later input.
                Effect::CancelFetch(reservation) => {
                    if let Some(index) = self.held.iter().position(|held| held.id == *reservation) {
                        let held = self.held.swap_remove(index);
                        self.to_drop.push(held.id);
                    }
                }
                _ => {}
            }
        }
    }

    /// One random input.
    fn input(&mut self) {
        self.steps += 1;
        if let Some(reservation) = self.to_drop.pop() {
            let step = self.bench.release(reservation);
            return self.took("drop a cancelled fetch", &step);
        }
        match pick(&mut self.rng, 20) {
            0..=4 => self.reserve(),
            5..=8 => self.submit(),
            9 => self.drop_held(),
            10..=13 => self.outcome(),
            14 => self.chain(),
            15..=16 => self.tick(),
            17..=18 => self.rejected_ids(),
            _ => self.toggle(),
        }
    }

    fn reserve(&mut self) {
        // Often a transaction someone already holds a reservation for, so
        // several callers meet on one id.
        let tx = if !self.held.is_empty() && chance(&mut self.rng, 2) {
            self.held[pick(&mut self.rng, self.held.len())].tx
        } else {
            pick(&mut self.rng, self.txs.len())
        };
        let body = self.txs[tx].clone();
        let (answer, step) = self.bench.reserve(&body);
        self.took("reserve", &step);
        // The answer was made after the step's deadlines were applied, so
        // the log as it stands now is the pool the answer saw.
        let pooled = self.bench.tracker.pooled(&body.id());
        match &answer {
            Ok(reservation) => {
                assert!(self.active, "a reservation from a pool that is off");
                assert!(!pooled, "a reservation for a pooled transaction");
                self.bench.tracker.saw("reserved");
                self.held.push(Held {
                    id: *reservation,
                    tx,
                });
            }
            Err(refused) => {
                let kind = match refused {
                    Unreserved::Inactive => {
                        assert!(!self.active, "the pool is on and answered that it is off");
                        "Inactive"
                    }
                    Unreserved::Pooled => {
                        assert!(
                            pooled,
                            "the log does not hold the id the pool said it holds"
                        );
                        "Pooled"
                    }
                    Unreserved::NoRoom => {
                        assert_eq!(
                            self.bench.machine.slots_in_use(),
                            LIMITS.pending,
                            "no room, with slots free"
                        );
                        "NoRoom"
                    }
                    Unreserved::Rejected(_) => {
                        assert!(
                            self.bench.rejected(&[&body]).contains(&body.id()),
                            "a reservation refused for a rejection the query does not report"
                        );
                        "Rejected"
                    }
                    Unreserved::Pending => "Pending",
                    Unreserved::Stopped => {
                        unreachable!("the machine never answers that the core stopped")
                    }
                };
                self.bench.tracker.saw(format!("unreserved {kind}"));
            }
        }
        if !self.active {
            assert!(
                matches!(answer, Err(Unreserved::Inactive)),
                "a pool that is off answered {answer:?}"
            );
        } else if pooled {
            assert!(
                matches!(answer, Err(Unreserved::Pooled)),
                "a pooled transaction's reservation was answered {answer:?}"
            );
        }
    }

    fn submit(&mut self) {
        if self.held.is_empty() {
            return self.tick();
        }
        let held = self.held.swap_remove(pick(&mut self.rng, self.held.len()));
        // Rarely, a body that is not the transaction reserved.
        let tx = if chance(&mut self.rng, 20) {
            pick(&mut self.rng, self.txs.len())
        } else {
            held.tx
        };
        let body = self.txs[tx].clone();
        let (_, step) = self.bench.submit(held.id, &body);
        self.took("submit", &step);
    }

    fn drop_held(&mut self) {
        if self.held.is_empty() {
            return self.tick();
        }
        let held = self.held.swap_remove(pick(&mut self.rng, self.held.len()));
        let step = if chance(&mut self.rng, 2) {
            self.bench.release(held.id)
        } else {
            self.bench.fetch_failed(held.id)
        };
        self.took("drop", &step);
    }

    fn outcome(&mut self) {
        // Sometimes a cancelled job answers late. The machine ignores it.
        let cancelled = self.bench.tracker.cancelled().to_vec();
        let late = !cancelled.is_empty() && !self.draining && chance(&mut self.rng, 10);
        let jobs = self.bench.tracker.jobs();
        if jobs.is_empty() && !late {
            return self.tick();
        }
        let (job, tx_id) = if late {
            let (job, tx) = cancelled[pick(&mut self.rng, cancelled.len())];
            (JobId(job), tx)
        } else {
            jobs[pick(&mut self.rng, jobs.len())]
        };
        let view = self.views.remove(&job.0);
        let tx = self
            .txs
            .iter()
            .find(|tx| tx.id() == tx_id)
            .expect("a job names a transaction of the universe")
            .clone();
        // Usually the pool now, sometimes the pool the job started with,
        // which may have changed since.
        let pooled: HashSet<transaction::Hash> = match view {
            Some(view) if chance(&mut self.rng, 3) => view,
            _ => self.bench.tracker.pooled_effects(),
        };
        // Transactions of the universe that the chain has not mined. A spend
        // of one is found only in the pool.
        let unmined: HashSet<transaction::Hash> = self
            .txs
            .iter()
            .map(|tx| tx.id().mined_id())
            .filter(|hash| !self.mined.contains(hash))
            .collect();
        // The verifier finds a spend in the pool, or in the chain if it
        // spends a chain output or a mined transaction. A spend of an
        // unmined transaction of the universe that is not pooled is missing.
        let (from_pool, missing): (Vec<OutPoint>, Vec<OutPoint>) = tx
            .spends()
            .into_iter()
            .filter(|spend| unmined.contains(&spend.hash))
            .partition(|spend| pooled.contains(&spend.hash));
        // The verifier's lookup finds a transaction the chain mined. Its
        // children may already be pooled, spending its outputs from the
        // chain, so passing it would join them in a group no real input
        // builds.
        let kind = if self.mined.contains(&tx_id.mined_id()) {
            7
        } else if self.draining {
            // While draining, every answer is final: a pass or a failure.
            [0, 6][pick(&mut self.rng, 2)]
        } else {
            pick(&mut self.rng, 10)
        };
        let outcome = match kind {
            0..=5 if missing.is_empty() => Outcome::Pass {
                tx: tx.real.clone(),
                pool_inputs: from_pool,
            },
            0..=5 => Outcome::MissingInputs {
                missing,
                error: TestVerifyError,
            },
            6 => {
                let charge = [Charge::None, Charge::None, Charge::Cooldown, Charge::Ban]
                    [pick(&mut self.rng, 4)];
                let error = match pick(&mut self.rng, 6) {
                    0 => {
                        RejectionError::SameEffectsTip(SameEffectsTipRejectionError::SpendConflict)
                    }
                    1 => RejectionError::SameEffectsChain(SameEffectsChainRejectionError::Expired),
                    2 => RejectionError::SameEffectsChain(
                        SameEffectsChainRejectionError::DuplicateSpend,
                    ),
                    _ => RejectionError::ExactTip(ExactTipRejectionError::FailedVerification(
                        TestVerifyError,
                    )),
                };
                Outcome::Fail { error, charge }
            }
            7 => Outcome::AlreadyMined(Arc::new(TestVerifyError)),
            _ => Outcome::LocalFault(
                [
                    LocalFault::StateUnavailable,
                    LocalFault::VerifyPanicked,
                    LocalFault::VerifyTimeout,
                ][pick(&mut self.rng, 3)],
            ),
        };
        let step = self.bench.outcome(job, outcome);
        self.took("outcome", &step);
    }

    /// The chain grows by one block, or the tip moves some other way and
    /// the machine is told only where it is.
    fn chain(&mut self) {
        if !self.active {
            return self.tick();
        }
        if chance(&mut self.rng, 5) {
            let skipped = pick(&mut self.rng, 3);
            if skipped == 0 {
                // A reorg. The run does not track which block mined what,
                // so it forgets every transaction it mined. Later blocks may
                // mine them again.
                self.mined.clear();
            }
            // Blocks the machine never sees mine transactions too.
            for _ in 0..skipped {
                let tx = pick(&mut self.rng, self.txs.len());
                self.mined.insert(self.txs[tx].id().mined_id());
            }
            let step = self
                .bench
                .reset_by(u32::try_from(skipped).expect("a small count"));
            return self.took("reset", &step);
        }
        // A chain mines a transaction once.
        let mut block = Vec::new();
        for _ in 0..pick(&mut self.rng, 3) {
            let tx = self.txs[pick(&mut self.rng, self.txs.len())].clone();
            if self.mined.insert(tx.id().mined_id()) {
                block.push(tx);
            }
        }
        let mined: Vec<&Tx> = block.iter().collect();
        let step = self.bench.grow(&mined);
        self.took("grow", &step);
    }

    fn tick(&mut self) {
        // The run's clock stops at RUN_TIME.
        let by = Duration::from_millis(u64::try_from(pick(&mut self.rng, 30_000)).expect("small"));
        let by = by.min(RUN_TIME.saturating_sub(self.bench.now));
        let step = self.bench.tick(by);
        self.took("tick", &step);
    }

    fn rejected_ids(&mut self) {
        let txs: Vec<Tx> = self.txs.clone();
        let all: Vec<&Tx> = txs.iter().collect();
        self.bench.rejected(&all);
    }

    fn toggle(&mut self) {
        if self.active {
            if !chance(&mut self.rng, 3) {
                return self.tick();
            }
            self.active = false;
            let step = self.bench.deactivate();
            self.took("deactivate", &step);
        } else {
            self.active = true;
            let step = self.bench.activate();
            self.took("activate", &step);
        }
    }

    /// Ends the run: every caller lets go, every job reports, and time passes
    /// until nothing is undecided.
    fn drain(&mut self) {
        self.draining = true;
        for reservation in std::mem::take(&mut self.to_drop) {
            let step = self.bench.release(reservation);
            self.took("drain a cancelled fetch", &step);
        }
        while let Some(held) = self.held.pop() {
            let step = self.bench.release(held.id);
            self.took("drain release", &step);
        }
        for _ in 0..1_000 {
            if self.bench.tracker.jobs().is_empty() {
                // Nothing runs, so the next thing to happen is a deadline.
                let Some(deadline) = self.bench.machine.next_deadline() else {
                    break;
                };
                let to = deadline.saturating_duration_since(self.bench.base);
                let step = self.bench.tick(to.saturating_sub(self.bench.now));
                self.took("drain tick", &step);
                continue;
            }
            self.outcome();
        }
        let machine = &self.bench.machine;
        assert!(
            self.bench.tracker.jobs().is_empty(),
            "the drain ends every job"
        );
        assert_eq!(
            machine.slots_in_use(),
            0,
            "the drain frees every slot: held {}",
            machine.caller_held()
        );
        assert_eq!(machine.caller_held(), 0, "the drain ends every reservation");
        assert!(
            machine.queued_ids().is_empty(),
            "the drain verifies again everything a reset took out"
        );
        self.bench.tracker.every_submission_is_answered();
    }
}

/// `pool/keeps-its-invariants`: over many random runs, the machine keeps its
/// invariants after every input, its outputs agree with the run's record,
/// and each run drains.
#[test]
fn random_runs_keep_the_invariants_and_drain() {
    let env = |name: &str| {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
    };
    let runs: u64 = env("POOL_RUNS").unwrap_or(400);
    let first: u64 = env("POOL_FIRST").unwrap_or(0);
    let trace = std::env::var_os("POOL_TRACE").is_some();
    let mut seen: BTreeMap<String, usize> = BTreeMap::new();
    for seed in first..first + runs {
        let mut run = Run::new(seed);
        if trace {
            eprintln!("seed {seed}");
            for (index, tx) in run.txs.iter().enumerate() {
                let spends: Vec<String> = tx
                    .spends()
                    .iter()
                    .map(|spend| {
                        run.txs
                            .iter()
                            .position(|parent| parent.id().mined_id() == spend.hash)
                            .map_or_else(
                                || format!("chain{:x}:{}", spend.hash.0[0], spend.index),
                                |parent| format!("t{parent}:{}", spend.index),
                            )
                    })
                    .collect();
                eprintln!(
                    "t{index} spends {spends:?} expiry {:?}",
                    tx.real.transaction.transaction().expiry_height()
                );
            }
        }
        let steps = 100 + pick(&mut run.rng, 300);
        for _ in 0..steps {
            run.input();
        }
        run.drain();
        for (kind, count) in run.bench.tracker.seen() {
            *seen.entry(kind.clone()).or_default() += count;
        }
    }
    if trace {
        eprintln!("{seen:#?}");
    }
    // The runs reach every rule they check.
    if runs >= 400 {
        for kind in [
            "added",
            "refused",
            "parked",
            "reserved",
            "start",
            "start again after a reset",
            "added again after a reset",
            "removed Reset",
            "cancel",
            "cancel fetch",
            "answer for another tip",
            "removed Mined",
            "removed Conflicted",
            "removed Expired",
            "removed Invalidated",
            "removed Deactivated",
            "verdict Admitted",
            "verdict Rejected",
            "verdict LocalFault(VerifyTimeout)",
            "verdict LocalFault(WrongTransaction)",
            "verdict LocalFault(Deactivated)",
            "unreserved Pending",
            "unreserved Pooled",
            "unreserved Rejected",
            "unreserved Inactive",
            "unreserved NoRoom",
        ] {
            assert!(
                seen.get(kind).is_some_and(|count| *count > 0),
                "the runs never reached {kind}: {seen:#?}"
            );
        }
    }
}
