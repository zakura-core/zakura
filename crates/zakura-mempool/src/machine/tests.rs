//! The machine tests: the machine against the model.
//!
//! `pool/state-matches-model` drives [`MempoolState`] and the port's
//! independent model with the same random inputs and compares, after every
//! input, what each caused and what each holds. The transactions are real:
//! V6 transactions with transparent spends between them, so dependencies,
//! conflicts and expiry all happen.
//!
//! The model leaves out three storage rules, so the runs stay where those
//! rules agree with it. The cost limit is far above what a run pools, so
//! nothing is evicted. A run lasts less than one cooldown, so no peer earns a
//! second, longer one. And the model's visible pool is oldest first, which is
//! compared as a set, not in mining order.
//!
//! `POOL_MODEL_RUNS` sets how many runs, `POOL_MODEL_FIRST` the first seed,
//! and `POOL_MODEL_TRACE` prints each run's transactions and steps to stderr.

// The trace is for a developer chasing one failing seed.
#![allow(clippy::print_stderr)]

use std::{sync::Arc, time::Duration};

use proptest::{
    prelude::*,
    strategy::ValueTree,
    test_runner::{RngAlgorithm, TestRng, TestRunner},
};
use zakura_chain::{
    amount::Amount,
    at_least_one,
    block::{self, Height},
    ironwood,
    orchard::{self, tree},
    parameters::NetworkUpgrade,
    primitives::Halo2Proof,
    transaction::{self, LockTime, Transaction, UnminedTxId, VerifiedUnminedTx},
    transparent::{self, OutPoint},
};
use zakura_node_services::{
    block_memory::TransactionHandle,
    mempool::port::{
        model::{
            Effect as ModelEffect, ModelBlock, ModelTransition, ModelTx, ModelUnreserved,
            ModelVerdict, Outcome as ModelOutcome, OutputAnswer as ModelOutputAnswer, PoolModel,
            Step as ModelStep,
        },
        ChainReason, ChainTip, ChainTransition, ChangeSeq, Charge, ConnectedBlock, Generation,
        GenerationRead, Intent, LocalFault, RejectionScope, ReservationId, Unreserved, Verdict,
    },
};
use zakura_rt::collections::HashSet;

use super::{Effect, Grant, Limits, MempoolState, Outcome, OutputAnswer, Step};
use crate::{
    config::Config,
    storage::{
        ExactTipRejectionError, RejectionError, SameEffectsChainRejectionError,
        SameEffectsTipRejectionError,
    },
    Instant,
};

/// The verifier's error in these tests.
#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
#[error("the test verifier rejected the transaction")]
pub(crate) struct TestVerifyError;

/// The machine under test.
type Machine = MempoolState<TestVerifyError>;

/// The tip a run starts at.
const START: u32 = 100;

/// How long a run remembers a chain-scoped rejection: short, so runs reach
/// the end of it.
const REJECTION_MEMORY: Duration = Duration::from_secs(150);

/// How far one run's clock moves before it drains. Less than one cooldown, so
/// no peer earns a second, longer one, which the model leaves out.
const RUN_TIME: Duration = Duration::from_secs(9 * 60);

/// A P2PKH lock script to a zero key hash, which is standard.
fn p2pkh() -> transparent::Script {
    let mut script = vec![0x76, 0xa9, 0x14];
    script.extend_from_slice(&[0; 20]);
    script.extend_from_slice(&[0x88, 0xac]);
    transparent::Script::new(&script)
}

/// One transaction of a run's universe, as each side sees it.
#[derive(Clone)]
pub(crate) struct Tx {
    pub(crate) real: VerifiedUnminedTx,
    pub(crate) model: ModelTx,
}

impl Tx {
    /// Its exact id.
    pub(crate) fn id(&self) -> UnminedTxId {
        self.model.id
    }
}

/// Builds a V6 transaction that spends `spends`, creates `outputs` P2PKH
/// outputs and expires at `expiry`. Each one reveals a new Ironwood
/// nullifier, so two never conflict through a nullifier.
pub(crate) fn build(
    runner: &mut TestRunner,
    spends: &[OutPoint],
    outputs: u32,
    expiry: Option<Height>,
) -> Tx {
    let action = any::<ironwood::Action>()
        .new_tree(runner)
        .expect("the action strategy makes a value")
        .current();
    let transaction = Transaction::V6 {
        network_upgrade: NetworkUpgrade::Nu6_3,
        lock_time: LockTime::unlocked(),
        // Zero is no expiry.
        expiry_height: expiry.unwrap_or(Height(0)),
        inputs: spends
            .iter()
            .map(|outpoint| transparent::Input::PrevOut {
                outpoint: *outpoint,
                unlock_script: transparent::Script::new(&[]),
                sequence: 0,
            })
            .collect(),
        outputs: (0..outputs)
            .map(|_| transparent::Output {
                value: Amount::try_from(1_000_000).expect("a valid test amount"),
                lock_script: p2pkh(),
            })
            .collect(),
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: Some(ironwood::ShieldedData {
            flags: orchard::Flags::ENABLE_SPENDS,
            value_balance: Amount::zero(),
            shared_anchor: tree::Root::default(),
            proof: Halo2Proof(vec![0; 4992]),
            actions: at_least_one![ironwood::AuthorizedAction {
                action,
                spend_auth_sig: [0u8; 64].into(),
            }],
            binding_sig: [0u8; 64].into(),
        }),
    };
    let real = VerifiedUnminedTx::new(
        Arc::new(transaction).into(),
        Amount::try_from(1_000_000).expect("a valid test fee"),
        0,
        0,
        Arc::new(Vec::new()),
    )
    .expect("the test fee passes the ZIP-317 checks");
    let model = ModelTx {
        id: real.transaction.id(),
        spends: spends.to_vec(),
        nullifiers: Vec::new(),
        outputs,
        expiry,
    };
    Tx { real, model }
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
                OutPoint {
                    hash: parent.id().mined_id(),
                    index: u32::try_from(pick(rng, 2)).expect("a small index"),
                }
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
        let mut tx = build(runner, &spends, outputs, expiry);
        // Each transaction reveals its own nullifier, which the model names
        // by its index.
        tx.model.nullifiers = vec![u64::try_from(txs.len()).expect("a small index")];
        txs.push(tx);
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

/// `tx` with other authorizing data: the same mined id and effects, and
/// another exact id.
fn twin(tx: &Tx) -> Tx {
    let mut transaction = (**tx.real.transaction.transaction()).clone();
    if let Transaction::V6 {
        ironwood_shielded_data: Some(data),
        ..
    } = &mut transaction
    {
        data.binding_sig = [1u8; 64].into();
    }
    let real = VerifiedUnminedTx::new(
        Arc::new(transaction).into(),
        Amount::try_from(1_000_000).expect("a valid test fee"),
        0,
        0,
        Arc::new(Vec::new()),
    )
    .expect("the test fee passes the ZIP-317 checks");
    let model = ModelTx {
        id: real.transaction.id(),
        ..tx.model.clone()
    };
    assert_eq!(
        model.id.mined_id(),
        tx.id().mined_id(),
        "a twin has the same effects"
    );
    assert_ne!(model.id, tx.id(), "a twin has other authorizing data");
    Tx { real, model }
}

/// A number below `n`.
fn pick(rng: &mut TestRng, n: usize) -> usize {
    let n = u64::try_from(n.max(1)).expect("a small bound");
    usize::try_from(rng.next_u64() % n).expect("a small number")
}

/// True one time in `n`.
fn chance(rng: &mut TestRng, n: usize) -> bool {
    pick(rng, n) == 0
}

/// A tip at `height` on the fork numbered `fork`.
pub(crate) fn tip(height: u32, fork: u8) -> ChainTip {
    let mut hash = [fork; 32];
    hash[..4].copy_from_slice(&height.to_be_bytes());
    ChainTip {
        hash: block::Hash(hash),
        height: Height(height),
    }
}

/// A sort key for an id: its mined id, then its authorizing data.
fn key(id: &UnminedTxId) -> ([u8; 32], Option<[u8; 32]>) {
    (id.mined_id().0, id.auth_digest().map(|digest| digest.0))
}

/// The model's verdict for a real one.
fn model_verdict(verdict: &Verdict) -> ModelVerdict {
    match verdict {
        Verdict::Admitted => ModelVerdict::Admitted,
        Verdict::Rejected(rejection) => ModelVerdict::Rejected {
            scope: rejection.scope,
            charge: rejection.charge,
        },
        Verdict::NoRoom => ModelVerdict::NoRoom,
        Verdict::LocalFault(fault) => ModelVerdict::LocalFault(*fault),
    }
}

/// The model's answer for a real refused reservation.
fn model_unreserved(unreserved: &Unreserved) -> ModelUnreserved {
    match unreserved {
        Unreserved::Inactive => ModelUnreserved::Inactive,
        Unreserved::Pooled => ModelUnreserved::Pooled,
        Unreserved::Pending => ModelUnreserved::Pending,
        Unreserved::Rejected(rejection) => ModelUnreserved::Rejected(rejection.scope),
        Unreserved::NoRoom => ModelUnreserved::NoRoom,
        Unreserved::Stopped => unreachable!("the machine never answers that the core stopped"),
    }
}

/// One effect in a form both sides share. The model's `Outputs` effect has
/// no counterpart: the machine answers each output waiter directly.
#[derive(Clone, Debug, Eq, PartialEq)]
enum Seen {
    Start {
        job: u64,
        tx: UnminedTxId,
        revalidation: bool,
    },
    Cancel(u64),
    CancelFetch(u64),
    Verdict {
        waiter: u64,
        verdict: ModelVerdict,
    },
    OutputReady {
        waiter: u64,
        outpoint: OutPoint,
    },
    OutputWaitEnded(u64),
}

/// The machine's effects, as [`Seen`].
fn seen_real(step: &Step) -> Vec<Seen> {
    step.effects
        .iter()
        .map(|effect| match effect {
            Effect::Start {
                job,
                tx,
                revalidation,
                ..
            } => Seen::Start {
                job: job.0,
                tx: tx.id(),
                revalidation: *revalidation,
            },
            Effect::Cancel(job) => Seen::Cancel(job.0),
            Effect::CancelFetch(reservation) => Seen::CancelFetch(reservation.0),
            Effect::Verdict { waiter, verdict } => Seen::Verdict {
                waiter: waiter.0,
                verdict: model_verdict(verdict),
            },
            Effect::OutputReady {
                waiter, outpoint, ..
            } => Seen::OutputReady {
                waiter: waiter.0,
                outpoint: *outpoint,
            },
            Effect::OutputWaitEnded(waiter) => Seen::OutputWaitEnded(waiter.0),
        })
        .collect()
}

/// The model's effects, as [`Seen`].
fn seen_model(step: &ModelStep) -> Vec<Seen> {
    step.effects
        .iter()
        .filter_map(|effect| match effect {
            ModelEffect::Start {
                job,
                tx,
                revalidation,
            } => Some(Seen::Start {
                job: job.0,
                tx: *tx,
                revalidation: *revalidation,
            }),
            ModelEffect::Cancel(job) => Some(Seen::Cancel(job.0)),
            ModelEffect::CancelFetch(reservation) => Some(Seen::CancelFetch(reservation.0)),
            ModelEffect::Verdict { waiter, verdict } => Some(Seen::Verdict {
                waiter: waiter.0,
                verdict: *verdict,
            }),
            ModelEffect::Outputs { .. } => None,
            ModelEffect::OutputReady { waiter, outpoint } => Some(Seen::OutputReady {
                waiter: waiter.0,
                outpoint: *outpoint,
            }),
            ModelEffect::OutputWaitEnded(waiter) => Some(Seen::OutputWaitEnded(waiter.0)),
        })
        .collect()
}

/// A granted reservation, as the run's callers hold it.
#[derive(Clone, Debug)]
struct Held {
    id: ReservationId,
    tx: usize,
    wants_body: bool,
}

/// How often each kind of outcome happened over every run, so the test shows
/// it reaches each rule it compares.
#[derive(Debug, Default)]
struct Coverage(std::collections::BTreeMap<String, usize>);

impl Coverage {
    /// Counts one occurrence of `kind`.
    fn saw(&mut self, kind: impl Into<String>) {
        *self.0.entry(kind.into()).or_default() += 1;
    }

    /// Counts what one step caused.
    fn step(&mut self, seen: &[Seen], changes: &[zakura_node_services::mempool::port::Change]) {
        use zakura_node_services::mempool::port::Change;
        for effect in seen {
            match effect {
                Seen::Start { revalidation, .. } => self.saw(if *revalidation {
                    "start revalidation"
                } else {
                    "start"
                }),
                Seen::Cancel(_) => self.saw("cancel"),
                Seen::CancelFetch(_) => self.saw("cancel fetch"),
                Seen::Verdict { verdict, .. } => self.saw(format!("verdict {verdict:?}")),
                Seen::OutputReady { .. } => self.saw("output ready"),
                Seen::OutputWaitEnded(_) => self.saw("output wait ended"),
            }
        }
        for change in changes {
            match change {
                Change::Added(_) => self.saw("added"),
                Change::Removed { reason, .. } => self.saw(format!("removed {reason:?}")),
                Change::Hidden(_) => self.saw("hidden"),
                Change::Revealed { .. } => self.saw("revealed"),
                Change::Refused(_) => self.saw("refused"),
            }
        }
    }
}

/// One run: both sides, the inputs' bookkeeping and the clock.
struct Run {
    rng: TestRng,
    txs: Vec<Tx>,
    machine: Machine,
    model: PoolModel,
    base: Instant,
    now: Duration,
    tip: u32,
    fork: u8,
    applied: u64,
    active: bool,
    held: Vec<Held>,
    /// Jobs started and not yet answered or cancelled, with their target.
    jobs: Vec<(u64, UnminedTxId)>,
    /// The visible pool when each job started, which is what its verifier
    /// read.
    views: std::collections::HashMap<u64, HashSet<transaction::Hash>>,
    /// Jobs cancelled before they answered. Their late outcomes are ignored.
    cancelled: Vec<(u64, UnminedTxId)>,
    /// Output waiters that may still be answered.
    output_waiters: Vec<u64>,
    steps: usize,
    /// Whether the run is draining, when every job is answered for good.
    draining: bool,
    /// The transactions of the universe the current chain mined.
    mined: HashSet<transaction::Hash>,
    /// Reservations whose fetch the core cancelled. Their callers drop them
    /// next.
    to_drop: Vec<ReservationId>,
    coverage: Coverage,
}

impl Run {
    /// A run for `seed`, with small limits so full states are reached.
    fn new(seed: u64) -> Self {
        let mut seed_bytes = [0u8; 32];
        seed_bytes[..8].copy_from_slice(&seed.to_le_bytes());
        let mut rng = TestRng::from_seed(RngAlgorithm::ChaCha, &seed_bytes);
        let mut runner = TestRunner::deterministic();
        let size = 8 + pick(&mut rng, 12);
        let txs = universe(&mut rng, &mut runner, size);
        let limits = Limits {
            pending: 6,
            revalidating: 3,
        };
        let base = Instant::now();
        let machine = Machine::new(
            &Config {
                tx_cost_limit: u64::MAX / 2,
                eviction_memory_time: REJECTION_MEMORY,
                ..Config::default()
            },
            tip(START, 0),
            Generation(0),
            base,
        )
        .with_limits(limits);
        let model = PoolModel::new(Height(START), Generation(0))
            .with_limits(limits.pending, limits.revalidating)
            .with_rejection_memory(REJECTION_MEMORY);
        Self {
            rng,
            txs,
            machine,
            model,
            base,
            now: Duration::ZERO,
            tip: START,
            fork: 0,
            applied: 0,
            active: true,
            held: Vec::new(),
            jobs: Vec::new(),
            views: std::collections::HashMap::new(),
            cancelled: Vec::new(),
            output_waiters: Vec::new(),
            steps: 0,
            draining: false,
            mined: HashSet::default(),
            to_drop: Vec::new(),
            coverage: Coverage::default(),
        }
    }

    /// The machine's instant for the model's time.
    fn at(&self) -> Instant {
        self.base + self.now
    }

    /// Compares the two steps one input caused, and records the jobs and
    /// output waiters they name.
    fn compare(&mut self, what: &str, real: Step, model: ModelStep) {
        let (seen, expected) = (seen_real(&real), seen_model(&model));
        assert_eq!(
            seen, expected,
            "step {} ({what}): the machine's effects differ from the model's",
            self.steps
        );
        assert_eq!(
            real.changes, model.changes,
            "step {} ({what}): the machine's changes differ from the model's",
            self.steps
        );
        if std::env::var_os("POOL_MODEL_TRACE").is_some() {
            let name = |id: &UnminedTxId| {
                self.txs
                    .iter()
                    .position(|tx| tx.id() == *id)
                    .map_or_else(|| "?".to_owned(), |index| format!("t{index}"))
            };
            eprintln!(
                "{} {what}: {:?} {:?} visible {:?}",
                self.steps,
                seen,
                real.changes,
                self.model.visible().iter().map(name).collect::<Vec<_>>()
            );
        }
        self.coverage.step(&seen, &real.changes);
        if self.machine.is_parked_any() {
            self.coverage.saw("parked");
        }
        for effect in &real.effects {
            if let Effect::OutputReady {
                outpoint, output, ..
            } = effect
            {
                let creator = self
                    .txs
                    .iter()
                    .find(|tx| tx.id().mined_id() == outpoint.hash)
                    .expect("an output comes from a transaction of the universe");
                assert_eq!(
                    Some(output),
                    creator
                        .real
                        .transaction
                        .transaction()
                        .outputs()
                        .get(usize::try_from(outpoint.index).expect("a small index")),
                    "step {}: the output a waiter gets",
                    self.steps
                );
            }
        }
        let visible_now: HashSet<transaction::Hash> = self
            .model
            .visible()
            .iter()
            .map(UnminedTxId::mined_id)
            .collect();
        for effect in seen {
            match effect {
                Seen::Start { job, tx, .. } => {
                    self.jobs.push((job, tx));
                    self.views.insert(job, visible_now.clone());
                }
                Seen::Cancel(job) => {
                    if let Some(index) = self.jobs.iter().position(|(started, _)| *started == job) {
                        let cancelled = self.jobs.swap_remove(index);
                        self.cancelled.push(cancelled);
                    }
                }
                Seen::OutputReady { waiter, .. } | Seen::OutputWaitEnded(waiter) => {
                    self.output_waiters.retain(|w| *w != waiter)
                }
                // The caller stops its fetch and drops the reservation, as
                // the adapter does, on a later input.
                Seen::CancelFetch(reservation) => {
                    if let Some(index) = self.held.iter().position(|held| held.id.0 == reservation)
                    {
                        let held = self.held.swap_remove(index);
                        self.to_drop.push(held.id);
                    }
                }
                Seen::Verdict { .. } => {}
            }
        }
        self.compare_state(what);
    }

    /// Compares what both sides hold.
    fn compare_state(&mut self, what: &str) {
        let step = self.steps;
        self.machine.assert_invariants();
        self.model.assert_invariants();
        let mut visible = self.machine.visible();
        let mut expected = self.model.visible();
        visible.sort_by_key(key);
        expected.sort_by_key(key);
        assert_eq!(
            visible.iter().map(key).collect::<Vec<_>>(),
            expected.iter().map(key).collect::<Vec<_>>(),
            "step {step} ({what}): visible pool"
        );
        assert_eq!(
            self.machine.hidden(),
            self.model.hidden(),
            "step {step} ({what}): hidden pool"
        );
        // The snapshot holds the visible pool, and each transaction depends
        // on exactly its visible parents.
        let snapshot = self.machine.snapshot(ChangeSeq(0));
        let mut in_snapshot: Vec<UnminedTxId> = snapshot
            .transactions
            .iter()
            .map(|tx| tx.transaction.id())
            .collect();
        in_snapshot.sort_by_key(key);
        assert_eq!(
            in_snapshot.iter().map(key).collect::<Vec<_>>(),
            expected.iter().map(key).collect::<Vec<_>>(),
            "step {step} ({what}): snapshot"
        );
        let visible_hashes: HashSet<transaction::Hash> =
            expected.iter().map(UnminedTxId::mined_id).collect();
        for id in &expected {
            let tx = self
                .txs
                .iter()
                .find(|tx| tx.id() == *id)
                .expect("a pooled transaction is from the universe");
            let mut parents: Vec<[u8; 32]> = tx
                .model
                .spends
                .iter()
                .map(|spend| spend.hash)
                .filter(|parent| visible_hashes.contains(parent))
                .map(|parent| parent.0)
                .collect();
            parents.sort_unstable();
            parents.dedup();
            let mut dependencies: Vec<[u8; 32]> = snapshot
                .dependencies
                .dependencies()
                .get(&id.mined_id())
                .map(|deps| deps.iter().map(|dep| dep.0).collect())
                .unwrap_or_default();
            dependencies.sort_unstable();
            assert_eq!(
                dependencies, parents,
                "step {step} ({what}): the dependencies of a visible transaction"
            );
        }
        assert_eq!(
            self.machine.slots_in_use(),
            self.model.slots_in_use(),
            "step {step} ({what}): slots in use"
        );
        assert_eq!(
            self.machine.caller_held(),
            self.model.caller_held(),
            "step {step} ({what}): reservations callers hold"
        );
        assert_eq!(
            self.machine.bodies_held(),
            self.model.bodies_held(),
            "step {step} ({what}): bodies held"
        );
        assert_eq!(
            self.machine.output_waits(),
            self.model.output_waits(),
            "step {step} ({what}): output waits"
        );
        for tx in &self.txs {
            assert_eq!(
                self.machine.is_parked(tx.id()),
                self.model.is_parked(tx.id()),
                "step {step} ({what}): whether {:?} is parked",
                tx.id()
            );
        }
    }

    /// One random input to both sides.
    fn input(&mut self) {
        self.steps += 1;
        if let Some(reservation) = self.to_drop.pop() {
            let at = self.at();
            let (real, model) = (
                self.machine.release(reservation, at),
                self.model.release(reservation, self.now),
            );
            return self.compare("drop a cancelled fetch", real, model);
        }
        match pick(&mut self.rng, 20) {
            0..=4 => self.reserve(),
            5..=8 => self.submit(),
            9 => self.drop_held(),
            10..=13 => self.outcome(),
            14 => self.transition(),
            15 => self.tick(),
            16 => self.await_output(),
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
        let intent = if chance(&mut self.rng, 2) {
            Intent::Fetch
        } else {
            Intent::Deliver
        };
        let id = self.txs[tx].id();
        let at = self.at();
        let real = self.machine.reserve(id, intent, at);
        let model = self.model.reserve(id, intent, self.now);
        match (&real, &model) {
            (Ok(grant), Ok(expected)) => {
                assert_eq!(
                    *grant,
                    Grant {
                        id: expected.id,
                        joined: expected.joined,
                        wants_body: expected.wants_body,
                    },
                    "step {}: reserve granted differently",
                    self.steps
                );
                self.coverage
                    .saw(if grant.joined { "joined" } else { "reserved" });
                self.held.push(Held {
                    id: grant.id,
                    tx,
                    wants_body: grant.wants_body,
                });
            }
            (Err(refused), Err(expected)) => {
                assert_eq!(
                    model_unreserved(refused),
                    *expected,
                    "step {}: reserve refused differently",
                    self.steps
                );
                self.coverage.saw(format!("unreserved {expected:?}"));
            }
            _ => panic!(
                "step {}: reserve answered {real:?} and the model {model:?}",
                self.steps
            ),
        }
        let (real, model) = (self.machine.effects(), self.model.effects());
        self.compare("reserve", real, model);
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
        let at = self.at();
        let body = self.txs[tx].clone();
        let ((real_waiter, real), (model_waiter, model)) = if held.wants_body {
            (
                self.machine
                    .submit(held.id, body.real.transaction.clone(), at),
                self.model.submit(held.id, body.model.clone(), self.now),
            )
        } else {
            (
                self.machine.wait(held.id, body.id(), at),
                self.model.wait(held.id, body.id(), self.now),
            )
        };
        assert_eq!(real_waiter.0, model_waiter.0, "step {}: waiter", self.steps);
        self.compare("submit", real, model);
    }

    fn drop_held(&mut self) {
        if self.held.is_empty() {
            return self.tick();
        }
        let held = self.held.swap_remove(pick(&mut self.rng, self.held.len()));
        let at = self.at();
        let (real, model) = if chance(&mut self.rng, 2) {
            (
                self.machine.release(held.id, at),
                self.model.release(held.id, self.now),
            )
        } else {
            (
                self.machine.fetch_failed(held.id, at),
                self.model.fetch_failed(held.id, self.now),
            )
        };
        self.compare("drop", real, model);
    }

    /// The generation readings a job reports: mostly fresh, sometimes stale.
    fn readings(&mut self) -> (GenerationRead, GenerationRead) {
        let applied = self.applied;
        if self.draining {
            return (
                GenerationRead::Stable(Generation(applied)),
                GenerationRead::Stable(Generation(applied)),
            );
        }
        match pick(&mut self.rng, 8) {
            0 => (
                GenerationRead::Stable(Generation(applied)),
                GenerationRead::Changing,
            ),
            1 => (
                GenerationRead::Stable(Generation(applied.saturating_sub(1))),
                GenerationRead::Stable(Generation(applied)),
            ),
            2 => (
                GenerationRead::Stable(Generation(applied + 1)),
                GenerationRead::Stable(Generation(applied + 1)),
            ),
            _ => (
                GenerationRead::Stable(Generation(applied)),
                GenerationRead::Stable(Generation(applied)),
            ),
        }
    }

    fn outcome(&mut self) {
        // Sometimes a cancelled job answers late. Both sides ignore it.
        let late = !self.cancelled.is_empty() && !self.draining && chance(&mut self.rng, 10);
        if self.jobs.is_empty() && !late {
            return self.tick();
        }
        let (job, tx_id) = if late {
            self.cancelled
                .swap_remove(pick(&mut self.rng, self.cancelled.len()))
        } else {
            self.jobs.swap_remove(pick(&mut self.rng, self.jobs.len()))
        };
        let view = self.views.remove(&job);
        let tx = self
            .txs
            .iter()
            .find(|tx| tx.id() == tx_id)
            .expect("a job names a transaction of the universe")
            .clone();
        // Usually the pool now, sometimes the pool the job started with,
        // which may have changed since.
        let visible: HashSet<transaction::Hash> = match view {
            Some(view) if chance(&mut self.rng, 3) => view,
            _ => self
                .model
                .visible()
                .iter()
                .map(UnminedTxId::mined_id)
                .collect(),
        };
        // Transactions of the universe that the chain has not mined. A spend
        // of one is found only in the visible pool.
        let pooled: HashSet<transaction::Hash> = self
            .txs
            .iter()
            .map(|tx| tx.id().mined_id())
            .filter(|hash| !self.mined.contains(hash))
            .filter(|hash| tx.model.spends.iter().any(|spend| spend.hash == *hash))
            .collect();
        // The verifier finds a spend in the visible pool, or in the chain if
        // it spends a chain output or a mined transaction. A spend of an
        // unmined transaction of the universe that is not visible is
        // missing.
        let (from_pool, missing): (Vec<OutPoint>, Vec<OutPoint>) = tx
            .model
            .spends
            .iter()
            .filter(|spend| pooled.contains(&spend.hash))
            .partition(|spend| visible.contains(&spend.hash));
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
        let (real, model) = match kind {
            0..=5 if missing.is_empty() => (
                Outcome::Pass {
                    tx: tx.real.clone(),
                    pool_inputs: from_pool.clone(),
                },
                ModelOutcome::PassUsing(from_pool),
            ),
            0..=5 => (
                Outcome::MissingInputs {
                    missing: missing.clone(),
                    error: TestVerifyError,
                },
                ModelOutcome::MissingInputs(missing),
            ),
            6 => {
                let charge = [Charge::None, Charge::None, Charge::Cooldown, Charge::Ban]
                    [pick(&mut self.rng, 4)];
                let (error, scope) = match pick(&mut self.rng, 6) {
                    0 => (
                        RejectionError::SameEffectsTip(SameEffectsTipRejectionError::SpendConflict),
                        RejectionScope::EffectsTip,
                    ),
                    1 => (
                        RejectionError::SameEffectsChain(SameEffectsChainRejectionError::Expired),
                        RejectionScope::EffectsChain(ChainReason::Expired),
                    ),
                    2 => (
                        RejectionError::SameEffectsChain(
                            SameEffectsChainRejectionError::DuplicateSpend,
                        ),
                        RejectionScope::EffectsChain(ChainReason::DuplicateSpend),
                    ),
                    _ => (
                        RejectionError::ExactTip(ExactTipRejectionError::FailedVerification(
                            TestVerifyError,
                        )),
                        RejectionScope::ExactTip,
                    ),
                };
                (
                    Outcome::Fail { error, charge },
                    ModelOutcome::Fail { scope, charge },
                )
            }
            7 => (
                Outcome::AlreadyMined(Arc::new(TestVerifyError)),
                ModelOutcome::AlreadyMined,
            ),
            _ => {
                let fault = [
                    LocalFault::StateUnavailable,
                    LocalFault::VerifyPanicked,
                    LocalFault::VerifyTimeout,
                ][pick(&mut self.rng, 3)];
                (Outcome::LocalFault(fault), ModelOutcome::LocalFault(fault))
            }
        };
        let (before, after) = self.readings();
        let at = self.at();
        let wall = chrono::Utc::now();
        let real = self
            .machine
            .outcome(super::JobId(job), real, before, after, at, wall);
        let model = self.model.outcome(
            zakura_node_services::mempool::port::model::JobId(job),
            model,
            before,
            after,
            self.now,
        );
        self.compare("outcome", real, model);
    }

    fn transition(&mut self) {
        if !self.active {
            return self.tick();
        }
        self.applied += 1;
        let generation = Generation(self.applied);
        if chance(&mut self.rng, 12) {
            let at = self.at();
            let (real, model) = (
                self.machine
                    .resync(tip(self.tip, self.fork), generation, at),
                self.model.resync(Height(self.tip), generation, self.now),
            );
            return self.compare("resync", real, model);
        }
        let before = tip(self.tip, self.fork);
        let disconnected = if chance(&mut self.rng, 6) {
            1 + pick(&mut self.rng, 2)
        } else {
            0
        };
        if disconnected > 0 {
            self.fork = self.fork.wrapping_add(1);
            // The run does not track which block mined what, so a reorg
            // forgets every block it mined. The new blocks may mine them again.
            self.mined.clear();
        }
        let blocks = if disconnected > 0 && chance(&mut self.rng, 4) {
            // The tip goes back and nothing connects.
            0
        } else if disconnected > 0 {
            disconnected + pick(&mut self.rng, 2)
        } else {
            1 + pick(&mut self.rng, 2)
        };
        let base = self.tip - u32::try_from(disconnected).expect("a small reorg");
        let mut real_blocks = Vec::new();
        let mut model_blocks = Vec::new();
        for index in 0..blocks {
            let height = base + 1 + u32::try_from(index).expect("a small block count");
            // A chain mines a transaction once.
            let candidates: Vec<Tx> = (0..pick(&mut self.rng, 3))
                .map(|_| self.txs[pick(&mut self.rng, self.txs.len())].clone())
                .collect();
            let mut mined = Vec::new();
            for tx in candidates {
                if self.mined.insert(tx.id().mined_id()) {
                    mined.push(tx);
                }
            }
            let tip_here = tip(height, self.fork);
            real_blocks.push(ConnectedBlock {
                hash: tip_here.hash,
                height: tip_here.height,
                transactions: mined
                    .iter()
                    .map(|tx| TransactionHandle::Shared(tx.real.transaction.transaction().clone()))
                    .collect(),
                transaction_hashes: mined.iter().map(|tx| tx.id().mined_id()).collect(),
            });
            model_blocks.push(ModelBlock {
                transactions: mined.iter().map(|tx| tx.model.clone()).collect(),
            });
        }
        self.tip = base + u32::try_from(blocks).expect("a small block count");
        let rules_change = chance(&mut self.rng, 15);
        let real = ChainTransition {
            before: Some(before),
            disconnected: (0..disconnected)
                .map(|back| {
                    tip(
                        before.height.0 - u32::try_from(back).expect("small"),
                        self.fork.wrapping_sub(1),
                    )
                })
                .collect(),
            connected: real_blocks,
            after: tip(self.tip, self.fork),
            generation,
            rules_change,
        };
        let model = ModelTransition {
            disconnected,
            connected: model_blocks,
            tip: Height(self.tip),
            generation,
            rules_change,
        };
        let at = self.at();
        let (real, model) = (
            self.machine.transition(real, at),
            self.model.transition(model, self.now),
        );
        self.compare("transition", real, model);
    }

    fn tick(&mut self) {
        // The run's clock stops at RUN_TIME. Deadlines past it wait for the
        // drain.
        let by = Duration::from_millis(u64::try_from(pick(&mut self.rng, 30_000)).expect("small"));
        self.now = (self.now + by).min(RUN_TIME);
        let at = self.at();
        let (real, model) = (self.machine.tick(at), self.model.tick(self.now));
        self.compare("tick", real, model);
    }

    fn await_output(&mut self) {
        // Sometimes a caller stops waiting.
        if !self.output_waiters.is_empty() && chance(&mut self.rng, 3) {
            let waiter = self
                .output_waiters
                .swap_remove(pick(&mut self.rng, self.output_waiters.len()));
            self.machine.cancel_output_wait(super::WaiterId(waiter));
            self.model
                .cancel_output_wait(zakura_node_services::mempool::port::model::WaiterId(waiter));
        }
        let tx = &self.txs[pick(&mut self.rng, self.txs.len())];
        let outpoint = OutPoint {
            hash: tx.id().mined_id(),
            index: u32::try_from(pick(&mut self.rng, 2)).expect("a small index"),
        };
        let at = self.at();
        let real = self.machine.await_output(outpoint, at, |_| false);
        let model = self.model.await_output(outpoint, self.now);
        match (&real, model) {
            (OutputAnswer::Ready(output), ModelOutputAnswer::Ready) => assert_eq!(
                Some(output),
                tx.real
                    .transaction
                    .transaction()
                    .outputs()
                    .get(usize::try_from(outpoint.index).expect("a small index")),
                "step {}: the output a waiter gets",
                self.steps
            ),
            (OutputAnswer::Waiting(waiter), ModelOutputAnswer::Waiting(expected)) => {
                assert_eq!(waiter.0, expected.0, "step {}: output waiter", self.steps);
                self.output_waiters.push(waiter.0);
            }
            (OutputAnswer::TooManyWaiters, ModelOutputAnswer::TooManyWaiters)
            | (OutputAnswer::Inactive, ModelOutputAnswer::Inactive) => {}
            _ => panic!(
                "step {}: await_output answered {real:?} and the model {model:?}",
                self.steps
            ),
        }
        let (real, model) = (self.machine.effects(), self.model.effects());
        self.compare("await_output", real, model);
    }

    fn rejected_ids(&mut self) {
        let ids: HashSet<UnminedTxId> = self.txs.iter().map(Tx::id).collect();
        let at = self.at();
        let mut real: Vec<UnminedTxId> = self
            .machine
            .rejected_ids(ids.clone(), at)
            .into_iter()
            .collect();
        let mut model: Vec<UnminedTxId> = self
            .model
            .rejected_ids(&ids.into_iter().collect(), self.now)
            .into_iter()
            .collect();
        real.sort_by_key(key);
        model.sort_by_key(key);
        assert_eq!(
            real.iter().map(key).collect::<Vec<_>>(),
            model.iter().map(key).collect::<Vec<_>>(),
            "step {}: rejected ids",
            self.steps
        );
        let (real, model) = (self.machine.effects(), self.model.effects());
        self.compare("rejected_ids", real, model);
    }

    fn toggle(&mut self) {
        let at = self.at();
        if self.active {
            if !chance(&mut self.rng, 3) {
                return self.tick();
            }
            self.active = false;
            let (real, model) = (self.machine.deactivate(at), self.model.deactivate(self.now));
            self.output_waiters.clear();
            self.compare("deactivate", real, model);
        } else {
            self.active = true;
            self.machine
                .activate(tip(self.tip, self.fork), Generation(self.applied), at);
            self.model
                .activate(Height(self.tip), Generation(self.applied), self.now);
            let (real, model) = (self.machine.effects(), self.model.effects());
            self.compare("activate", real, model);
        }
    }

    /// Ends the run: every caller lets go, every job reports, and time passes
    /// until nothing is undecided.
    fn drain(&mut self) {
        // A stale reading may have named a generation past the last
        // transition. Its transition comes now, as the state's would.
        if self.active {
            self.applied += 1;
            let at = self.at();
            let (real, model) = (
                self.machine
                    .resync(tip(self.tip, self.fork), Generation(self.applied), at),
                self.model
                    .resync(Height(self.tip), Generation(self.applied), self.now),
            );
            self.compare("drain resync", real, model);
        }
        self.draining = true;
        let cancelled: Vec<ReservationId> = std::mem::take(&mut self.to_drop);
        for reservation in cancelled {
            let at = self.at();
            let (real, model) = (
                self.machine.release(reservation, at),
                self.model.release(reservation, self.now),
            );
            self.compare("drain a cancelled fetch", real, model);
        }
        while let Some(held) = self.held.pop() {
            let at = self.at();
            let (real, model) = (
                self.machine.release(held.id, at),
                self.model.release(held.id, self.now),
            );
            self.compare("drain release", real, model);
        }
        for _ in 0..1_000 {
            if self.jobs.is_empty() {
                // Nothing runs, so the next thing to happen is a deadline.
                let Some(deadline) = self.machine.next_deadline() else {
                    break;
                };
                self.now = self.now.max(deadline.saturating_duration_since(self.base));
                let at = self.at();
                let (real, model) = (self.machine.tick(at), self.model.tick(self.now));
                self.compare("drain tick", real, model);
                continue;
            }
            self.outcome();
        }
        assert_eq!(
            self.machine.slots_in_use(),
            0,
            "the drain frees every slot: jobs {:?}, hidden {}, held {}",
            self.jobs,
            self.machine.hidden().len(),
            self.machine.caller_held()
        );
        assert_eq!(
            self.machine.caller_held(),
            0,
            "the drain ends every reservation"
        );
        assert!(
            self.machine.hidden().is_empty(),
            "the drain decides every hidden transaction"
        );
    }
}

/// `pool/state-matches-model`: the machine and the model agree on every
/// input of many random runs.
#[test]
fn pool_state_matches_model() {
    let runs: u64 = std::env::var("POOL_MODEL_RUNS")
        .ok()
        .and_then(|runs| runs.parse().ok())
        .unwrap_or(400);
    let mut coverage = Coverage::default();
    let first: u64 = std::env::var("POOL_MODEL_FIRST")
        .ok()
        .and_then(|first| first.parse().ok())
        .unwrap_or(0);
    for seed in first..first + runs {
        if std::env::var_os("POOL_MODEL_TRACE").is_some() {
            eprintln!("seed {seed}");
        }
        let mut run = Run::new(seed);
        if std::env::var_os("POOL_MODEL_TRACE").is_some() {
            for (index, tx) in run.txs.iter().enumerate() {
                let spends: Vec<String> = tx
                    .model
                    .spends
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
                    "t{index} spends {spends:?} outputs {} expiry {:?}",
                    tx.model.outputs, tx.model.expiry
                );
            }
        }
        let steps = 100 + pick(&mut run.rng, 300);
        for _ in 0..steps {
            run.input();
        }
        run.drain();
        for (kind, count) in run.coverage.0 {
            *coverage.0.entry(kind).or_default() += count;
        }
    }
    if std::env::var_os("POOL_MODEL_TRACE").is_some() {
        eprintln!("{coverage:#?}");
    }
    if runs >= 400 {
        for kind in [
            "added",
            "hidden",
            "revealed",
            "refused",
            "parked",
            "joined",
            "start revalidation",
            "cancel",
            "cancel fetch",
            "output ready",
            "output wait ended",
            "removed Mined",
            "removed Conflicted",
            "removed Expired",
            "removed Invalidated",
            "removed Deactivated",
            "verdict Admitted",
            "verdict LocalFault(NoBody)",
            "verdict LocalFault(PoolUnsettled)",
            "verdict LocalFault(VerifyTimeout)",
            "verdict LocalFault(WrongTransaction)",
            "verdict LocalFault(Deactivated)",
            "unreserved Pending",
            "unreserved Pooled",
            "unreserved Inactive",
            "unreserved NoRoom",
        ] {
            assert!(
                coverage.0.get(kind).is_some_and(|count| *count > 0),
                "the runs never reached {kind}: {coverage:#?}"
            );
        }
    }
}

/// The machine and the model side by side, driven by hand, with every step
/// compared.
struct Pair {
    machine: Machine,
    model: PoolModel,
    base: Instant,
    now: Duration,
    applied: u64,
    tip: u32,
}

impl Pair {
    /// Both sides at the start tip, with `memory` for chain-scoped
    /// rejections.
    fn new(memory: Duration) -> Self {
        let base = Instant::now();
        Self {
            machine: Machine::new(
                &Config {
                    tx_cost_limit: u64::MAX / 2,
                    eviction_memory_time: memory,
                    ..Config::default()
                },
                tip(START, 0),
                Generation(0),
                base,
            ),
            model: PoolModel::new(Height(START), Generation(0)).with_rejection_memory(memory),
            base,
            now: Duration::ZERO,
            applied: 0,
            tip: START,
        }
    }

    /// Compares one input's steps, and returns the jobs it started.
    fn same(&self, real: Step, model: ModelStep) -> Vec<u64> {
        assert_eq!(seen_real(&real), seen_model(&model), "effects");
        assert_eq!(real.changes, model.changes, "changes");
        self.machine.assert_invariants();
        self.model.assert_invariants();
        seen_real(&real)
            .into_iter()
            .filter_map(|seen| match seen {
                Seen::Start { job, .. } => Some(job),
                _ => None,
            })
            .collect()
    }

    /// Reserves `tx` for this node on both sides. Returns the reservation if
    /// both granted it, after checking both answered the same.
    fn reserve(&mut self, tx: &Tx) -> Option<ReservationId> {
        let at = self.base + self.now;
        let real = self.machine.reserve(tx.id(), Intent::Deliver, at);
        let model = self.model.reserve(tx.id(), Intent::Deliver, self.now);
        let granted = match (&real, &model) {
            (Ok(grant), Ok(expected)) => {
                assert_eq!(grant.id, expected.id);
                Some(grant.id)
            }
            (Err(refused), Err(expected)) => {
                assert_eq!(model_unreserved(refused), *expected);
                None
            }
            _ => panic!("reserve answered {real:?} and the model {model:?}"),
        };
        let (real, model) = (self.machine.effects(), self.model.effects());
        self.same(real, model);
        granted
    }

    /// Submits `tx` under `reservation`, and returns the job that started.
    fn submit(&mut self, reservation: ReservationId, tx: &Tx) -> u64 {
        let at = self.base + self.now;
        let (_, real) = self
            .machine
            .submit(reservation, tx.real.transaction.clone(), at);
        let (_, model) = self.model.submit(reservation, tx.model.clone(), self.now);
        let started = self.same(real, model);
        assert_eq!(started.len(), 1, "a body starts one job");
        started[0]
    }

    /// Reports `job`'s fresh outcome on both sides, and returns the jobs the
    /// step started.
    fn outcome(
        &mut self,
        job: u64,
        real: Outcome<TestVerifyError>,
        model: ModelOutcome,
    ) -> Vec<u64> {
        let read = GenerationRead::Stable(Generation(self.applied));
        let at = self.base + self.now;
        let real =
            self.machine
                .outcome(super::JobId(job), real, read, read, at, chrono::Utc::now());
        let model = self.model.outcome(
            zakura_node_services::mempool::port::model::JobId(job),
            model,
            read,
            read,
            self.now,
        );
        self.same(real, model)
    }

    /// A pass for `tx`, with no pool inputs.
    fn pass(&mut self, job: u64, tx: &Tx) -> Vec<u64> {
        self.outcome(
            job,
            Outcome::Pass {
                tx: tx.real.clone(),
                pool_inputs: Vec::new(),
            },
            ModelOutcome::Pass,
        )
    }

    /// Connects one block that mines `mined`, after disconnecting
    /// `disconnect` blocks. Returns the jobs the step started.
    fn transition(&mut self, disconnect: u32, mined: &[&Tx]) -> Vec<u64> {
        self.applied += 1;
        let generation = Generation(self.applied);
        let before = tip(self.tip, 0);
        self.tip = self.tip - disconnect + 1;
        let after = tip(self.tip, u8::from(disconnect > 0));
        let real = ChainTransition {
            before: Some(before),
            disconnected: (0..disconnect)
                .map(|back| tip(before.height.0 - back, 0))
                .collect(),
            connected: vec![ConnectedBlock {
                hash: after.hash,
                height: after.height,
                transactions: mined
                    .iter()
                    .map(|tx| TransactionHandle::Shared(tx.real.transaction.transaction().clone()))
                    .collect(),
                transaction_hashes: mined.iter().map(|tx| tx.id().mined_id()).collect(),
            }],
            after,
            generation,
            rules_change: false,
        };
        let model = ModelTransition {
            disconnected: usize::try_from(disconnect).expect("a small reorg"),
            connected: vec![ModelBlock {
                transactions: mined.iter().map(|tx| tx.model.clone()).collect(),
            }],
            tip: Height(self.tip),
            generation,
            rules_change: false,
        };
        let at = self.base + self.now;
        let (real, model) = (
            self.machine.transition(real, at),
            self.model.transition(model, self.now),
        );
        self.same(real, model)
    }
}

/// Terra's case from M1 review round 1: while a pooled transaction is hidden,
/// another authorization of the same effects is refused for its effects. The
/// pooled one's revalidation then passes, and the model reveals it. Storage
/// must not refuse the reveal for the other authorization's rejection.
#[test]
fn a_passing_revalidation_ignores_a_rejection_of_another_authorization() {
    let mut runner = TestRunner::deterministic();
    let pooled = build(&mut runner, &[], 1, None);
    let other = twin(&pooled);
    let mut pair = Pair::new(Duration::from_secs(3600));

    let first = pair.reserve(&pooled).expect("room for the first");
    let second = pair.reserve(&other).expect("an exact id of its own");
    let first_job = pair.submit(first, &pooled);
    let second_job = pair.submit(second, &other);
    pair.pass(first_job, &pooled);

    // A reorg hides the pooled one and starts its revalidation.
    let revalidation = pair.transition(1, &[]);
    assert_eq!(
        revalidation.len(),
        1,
        "the hidden transaction is checked again"
    );

    // The other authorization's job reports on the new chain and is refused
    // for its effects, which caches a rejection of its mined id.
    pair.outcome(
        second_job,
        Outcome::Fail {
            error: RejectionError::SameEffectsTip(SameEffectsTipRejectionError::SpendConflict),
            charge: Charge::None,
        },
        ModelOutcome::Fail {
            scope: RejectionScope::EffectsTip,
            charge: Charge::None,
        },
    );

    pair.pass(revalidation[0], &pooled);
    assert_eq!(pair.machine.visible(), vec![pooled.id()], "it is revealed");
}

/// Both sides still remember a chain-scoped rejection at exactly its memory,
/// as today's list does, and forget it a moment later.
#[test]
fn a_rejection_holds_at_exactly_its_memory_and_ends_after() {
    let memory = Duration::from_secs(100);
    let mut runner = TestRunner::deterministic();
    let mined = build(&mut runner, &[], 1, None);
    let mut pair = Pair::new(memory);

    pair.transition(0, &[&mined]);
    pair.now = memory;
    assert_eq!(pair.reserve(&mined), None, "still remembered at its memory");
    pair.now = memory + Duration::from_millis(1);
    assert!(pair.reserve(&mined).is_some(), "forgotten after it");
}

/// Sol's case from M1 review round 1: hidden transactions keep counting
/// against the cost limit, so a reorg followed by new admissions cannot hold
/// more than the limit.
#[test]
fn hidden_transactions_count_against_the_cost_limit() {
    let mut runner = TestRunner::deterministic();
    let txs: Vec<Tx> = (0..3).map(|_| build(&mut runner, &[], 1, None)).collect();
    let limit = txs[0].real.cost() + txs[1].real.cost();
    let base = Instant::now();
    let mut machine = Machine::new(
        &Config {
            tx_cost_limit: limit,
            ..Config::default()
        },
        tip(START, 0),
        Generation(0),
        base,
    );
    let read = GenerationRead::Stable(Generation(0));
    let admit = |machine: &mut Machine, tx: &Tx, read: GenerationRead| -> Verdict {
        let grant = machine
            .reserve(tx.id(), Intent::Deliver, base)
            .expect("room for a reservation");
        assert!(machine.effects().is_empty());
        let (waiter, step) = machine.submit(grant.id, tx.real.transaction.clone(), base);
        let Some(Effect::Start { job, .. }) = step.effects.first().cloned() else {
            panic!("a body starts a job: {step:?}");
        };
        let step = machine.outcome(
            job,
            Outcome::Pass {
                tx: tx.real.clone(),
                pool_inputs: Vec::new(),
            },
            read,
            read,
            base,
            chrono::Utc::now(),
        );
        machine.assert_invariants();
        step.verdict(waiter).cloned().expect("the pass decides it")
    };
    assert!(matches!(
        admit(&mut machine, &txs[0], read),
        Verdict::Admitted
    ));
    assert!(matches!(
        admit(&mut machine, &txs[1], read),
        Verdict::Admitted
    ));

    // A reorg hides both. Their bodies are still held.
    let before = tip(START, 0);
    let after = tip(START, 1);
    let step = machine.transition(
        ChainTransition {
            before: Some(before),
            disconnected: vec![before],
            connected: vec![ConnectedBlock {
                hash: after.hash,
                height: after.height,
                transactions: Vec::new(),
                transaction_hashes: Arc::new([]),
            }],
            after,
            generation: Generation(1),
            rules_change: false,
        },
        base,
    );
    assert_eq!(machine.hidden().len(), 2);
    machine.assert_invariants();
    let revalidations: Vec<super::JobId> = step
        .effects
        .iter()
        .filter_map(|effect| match effect {
            Effect::Start { job, .. } => Some(*job),
            _ => None,
        })
        .collect();

    // A new transaction finds no room: the hidden ones can't be evicted.
    let read = GenerationRead::Stable(Generation(1));
    assert!(
        matches!(admit(&mut machine, &txs[2], read), Verdict::Rejected(_)),
        "the pool is full of hidden transactions"
    );

    // Both pass again and are revealed.
    for (job, tx) in revalidations.into_iter().zip(&txs) {
        machine.outcome(
            job,
            Outcome::Pass {
                tx: tx.real.clone(),
                pool_inputs: Vec::new(),
            },
            read,
            read,
            base,
            chrono::Utc::now(),
        );
        machine.assert_invariants();
    }
    assert_eq!(machine.visible().len(), 2);
}

/// Sol's case from M1 review round 2: a reveal that storage refuses takes its
/// body's cost off the hidden cost once, while another hidden body still
/// counts. Three children of a mined parent come back after a reorg, the
/// parent is pooled again, and the third child's reveal breaks the package
/// limit.
#[test]
fn a_refused_reveal_takes_its_cost_off_once() {
    let mut runner = TestRunner::deterministic();
    let parent = build(&mut runner, &[], 3, None);
    let children: Vec<Tx> = (0..3)
        .map(|index| {
            build(
                &mut runner,
                &[OutPoint {
                    hash: parent.id().mined_id(),
                    index,
                }],
                1,
                None,
            )
        })
        .collect();
    let other = build(&mut runner, &[], 1, None);
    let base = Instant::now();
    let mut machine = Machine::new(
        &Config {
            tx_cost_limit: u64::MAX / 2,
            ..Config::default()
        },
        tip(START, 0),
        Generation(0),
        base,
    );
    let pass = |tx: &Tx, inputs: Vec<OutPoint>| Outcome::Pass {
        tx: tx.real.clone(),
        pool_inputs: inputs,
    };
    let started = |step: &Step| -> Vec<(super::JobId, UnminedTxId)> {
        step.effects
            .iter()
            .filter_map(|effect| match effect {
                Effect::Start { job, tx, .. } => Some((*job, tx.id())),
                _ => None,
            })
            .collect()
    };
    let read0 = GenerationRead::Stable(Generation(0));
    let read1 = GenerationRead::Stable(Generation(1));
    let admit = |machine: &mut Machine, tx: &Tx, inputs: Vec<OutPoint>, read| {
        let grant = machine
            .reserve(tx.id(), Intent::Deliver, base)
            .expect("room for a reservation");
        assert!(machine.effects().is_empty());
        let (waiter, step) = machine.submit(grant.id, tx.real.transaction.clone(), base);
        let job = started(&step)[0].0;
        let step = machine.outcome(job, pass(tx, inputs), read, read, base, chrono::Utc::now());
        machine.assert_invariants();
        assert!(matches!(step.verdict(waiter), Some(Verdict::Admitted)));
    };

    // The parent is on the chain, so its children spend it from there.
    for child in children.iter().chain([&other]) {
        admit(&mut machine, child, Vec::new(), read0);
    }

    // A reorg takes the parent off the chain and hides the pool.
    let before = tip(START, 0);
    let after = tip(START, 1);
    let step = machine.transition(
        ChainTransition {
            before: Some(before),
            disconnected: vec![before],
            connected: vec![ConnectedBlock {
                hash: after.hash,
                height: after.height,
                transactions: Vec::new(),
                transaction_hashes: Arc::new([]),
            }],
            after,
            generation: Generation(1),
            rules_change: false,
        },
        base,
    );
    machine.assert_invariants();
    let revalidations = started(&step);
    assert_eq!(
        revalidations.len(),
        4,
        "every hidden transaction is checked again"
    );

    // The parent comes back into the pool.
    admit(&mut machine, &parent, Vec::new(), read1);

    // Each child now spends the pooled parent. The third breaks the package
    // limit, and the other transaction stays hidden throughout.
    for (index, child) in children.iter().enumerate() {
        let job = revalidations
            .iter()
            .find(|(_, id)| *id == child.id())
            .expect("each child has a revalidation")
            .0;
        let inputs = vec![OutPoint {
            hash: parent.id().mined_id(),
            index: u32::try_from(index).expect("a small index"),
        }];
        machine.outcome(
            job,
            pass(child, inputs),
            read1,
            read1,
            base,
            chrono::Utc::now(),
        );
        machine.assert_invariants();
    }
    assert_eq!(machine.hidden(), vec![other.id()]);
    assert_eq!(machine.visible().len(), 3, "the parent and two children");
}

/// Sol's case from M1 review round 4: a job that runs again after a reset is
/// marked, so the verifier charges no cooldown for it, as today's retry after
/// a tip reset is not charged. A job for a body that arrives after the reset
/// is not marked.
#[test]
fn a_job_run_again_after_a_reset_is_marked() {
    let mut runner = TestRunner::deterministic();
    let before_reset = build(&mut runner, &[], 1, None);
    let after_reset = build(&mut runner, &[], 1, None);
    let base = Instant::now();
    let mut machine = Machine::new(&Config::default(), tip(START, 0), Generation(0), base);
    let start = |machine: &mut Machine, tx: &Tx| -> Step {
        let grant = machine
            .reserve(tx.id(), Intent::Deliver, base)
            .expect("room for a reservation");
        assert!(machine.effects().is_empty());
        machine
            .submit(grant.id, tx.real.transaction.clone(), base)
            .1
    };
    let marked = |step: &Step| -> Vec<bool> {
        step.effects
            .iter()
            .filter_map(|effect| match effect {
                Effect::Start { after_reset, .. } => Some(*after_reset),
                _ => None,
            })
            .collect()
    };
    let first = start(&mut machine, &before_reset);
    assert_eq!(marked(&first), vec![false]);
    let Some(Effect::Start { job, .. }) = first.effects.first().cloned() else {
        panic!("a body starts a job");
    };
    assert!(!machine.job_after_reset(job));

    // A reorg lands while the job runs, so its outcome is stale and it runs
    // again, marked.
    let old = tip(START, 0);
    let new = tip(START, 1);
    let step = machine.transition(
        ChainTransition {
            before: Some(old),
            disconnected: vec![old],
            connected: vec![ConnectedBlock {
                hash: new.hash,
                height: new.height,
                transactions: Vec::new(),
                transaction_hashes: Arc::new([]),
            }],
            after: new,
            generation: Generation(1),
            rules_change: false,
        },
        base,
    );
    assert!(marked(&step).is_empty());
    // A core that has not started the job yet reads the reset now.
    assert!(
        machine.job_after_reset(job),
        "the job's admission was reset"
    );
    let stale = GenerationRead::Stable(Generation(0));
    let step = machine.outcome(
        job,
        Outcome::LocalFault(LocalFault::StateUnavailable),
        stale,
        stale,
        base,
        chrono::Utc::now(),
    );
    assert_eq!(marked(&step), vec![true], "the rerun is marked");

    assert_eq!(marked(&start(&mut machine, &after_reset)), vec![false]);
    machine.assert_invariants();
}

/// Terra's case from M1 review round 7: a body the pool refuses for its own
/// policy, not for its verification, earns its supplier nothing. `same`
/// checks the verdict against the model's.
#[test]
fn a_policy_refusal_charges_nothing() {
    let mut runner = TestRunner::deterministic();
    let pooled = build(&mut runner, &[], 1, None);
    let other = twin(&pooled);
    let mut pair = Pair::new(Duration::from_secs(3600));

    let first = pair.reserve(&pooled).expect("room for the first");
    let second = pair.reserve(&other).expect("an exact id of its own");
    let first_job = pair.submit(first, &pooled);
    let second_job = pair.submit(second, &other);
    pair.pass(first_job, &pooled);

    // The twin passes its verification, and the pool refuses it because the
    // same effects are already pooled.
    pair.pass(second_job, &other);
}

/// Terra's case from M1 review round 7: an admission that evicts another
/// transaction logs the addition and the eviction, so the log still leads to
/// the pool. The model leaves eviction to storage, so the random runs never
/// evict.
#[test]
fn an_admission_that_evicts_logs_both_changes() {
    use zakura_node_services::mempool::port::{Change, RemovalReason};

    let mut runner = TestRunner::deterministic();
    let mut txs: Vec<Tx> = (0..3).map(|_| build(&mut runner, &[], 1, None)).collect();
    let limit = txs[0].real.cost() + txs[1].real.cost();
    // Storage evicts only for a body that pays enough more than its victim.
    txs[2].real = VerifiedUnminedTx::new(
        txs[2].real.transaction.clone(),
        Amount::try_from(100_000_000).expect("a valid test fee"),
        0,
        0,
        Arc::new(Vec::new()),
    )
    .expect("a high fee passes the ZIP-317 checks");
    let base = Instant::now();
    let mut machine = Machine::new(
        &Config {
            tx_cost_limit: limit,
            ..Config::default()
        },
        tip(START, 0),
        Generation(0),
        base,
    );
    let read = GenerationRead::Stable(Generation(0));
    let mut logged = std::collections::HashSet::new();
    let mut evictions = 0;
    for tx in &txs {
        let grant = machine
            .reserve(tx.id(), Intent::Deliver, base)
            .expect("room for a reservation");
        assert!(machine.effects().is_empty());
        let (waiter, step) = machine.submit(grant.id, tx.real.transaction.clone(), base);
        let Some(Effect::Start { job, .. }) = step.effects.first().cloned() else {
            panic!("a body starts a job: {step:?}");
        };
        let step = machine.outcome(
            job,
            Outcome::Pass {
                tx: tx.real.clone(),
                pool_inputs: Vec::new(),
            },
            read,
            read,
            base,
            chrono::Utc::now(),
        );
        machine.assert_invariants();
        let verdict = step.verdict(waiter).cloned().expect("the pass decides it");
        for change in &step.changes {
            match change {
                Change::Added(id) => assert!(logged.insert(*id), "added once"),
                Change::Removed {
                    id,
                    reason: RemovalReason::Evicted,
                } => {
                    evictions += 1;
                    assert!(logged.remove(id), "an eviction removes a logged addition");
                }
                Change::Refused(id) => assert_eq!(*id, tx.id(), "only the new one is refused"),
                other => panic!("an admission logs only these changes: {other:?}"),
            }
        }
        if matches!(verdict, Verdict::Admitted) {
            assert!(
                logged.contains(&tx.id()),
                "an admitted transaction is logged"
            );
        }
    }
    assert!(evictions > 0, "the third admission evicts");
    let visible: std::collections::HashSet<UnminedTxId> = machine.visible().into_iter().collect();
    assert_eq!(logged, visible, "the log leads to the pool");
    let snapshot: std::collections::HashSet<UnminedTxId> = machine
        .snapshot(ChangeSeq(0))
        .transactions
        .iter()
        .map(|tx| tx.transaction.id())
        .collect();
    assert_eq!(snapshot, visible, "the snapshot holds the pool");
}

/// Sol's case from M1 review round 8: a verified body that both conflicts
/// with a pooled one and has a dust output is refused as non-standard, for
/// its exact bytes, as today's insertion checks standardness first.
#[test]
fn a_dust_body_that_also_conflicts_is_refused_as_non_standard() {
    use crate::{storage::NonStandardTransactionError, MempoolError};

    let mut runner = TestRunner::deterministic();
    let coin = OutPoint {
        hash: transaction::Hash([0xd5; 32]),
        index: 0,
    };
    let pooled = build(&mut runner, &[coin], 1, None);
    let mut dust = build(&mut runner, &[coin], 1, None);
    let mut transaction = Transaction::clone(dust.real.transaction.transaction());
    if let Transaction::V6 { outputs, .. } = &mut transaction {
        outputs[0].value = Amount::try_from(1).expect("a valid test amount");
    }
    dust.real = VerifiedUnminedTx::new(
        Arc::new(transaction).into(),
        Amount::try_from(1_000_000).expect("a valid test fee"),
        0,
        0,
        Arc::new(Vec::new()),
    )
    .expect("the test fee passes the ZIP-317 checks");
    dust.model.id = dust.real.transaction.id();

    let base = Instant::now();
    let mut machine = Machine::new(
        &Config {
            tx_cost_limit: u64::MAX / 2,
            ..Config::default()
        },
        tip(START, 0),
        Generation(0),
        base,
    );
    let read = GenerationRead::Stable(Generation(0));
    let decide = |machine: &mut Machine, tx: &Tx| -> Verdict {
        let grant = machine
            .reserve(tx.id(), Intent::Deliver, base)
            .expect("room for a reservation");
        assert!(machine.effects().is_empty());
        let (waiter, step) = machine.submit(grant.id, tx.real.transaction.clone(), base);
        let Some(Effect::Start { job, .. }) = step.effects.first().cloned() else {
            panic!("a body starts a job: {step:?}");
        };
        let step = machine.outcome(
            job,
            Outcome::Pass {
                tx: tx.real.clone(),
                pool_inputs: Vec::new(),
            },
            read,
            read,
            base,
            chrono::Utc::now(),
        );
        machine.assert_invariants();
        step.verdict(waiter).cloned().expect("the pass decides it")
    };
    assert!(matches!(decide(&mut machine, &pooled), Verdict::Admitted));
    let Verdict::Rejected(rejection) = decide(&mut machine, &dust) else {
        panic!("a dust body is refused");
    };
    assert_eq!(rejection.scope, RejectionScope::ExactTip);
    assert!(
        matches!(
            rejection
                .detail
                .downcast_ref::<MempoolError<TestVerifyError>>(),
            Some(MempoolError::NonStandardTransaction(
                NonStandardTransactionError::IsDust
            ))
        ),
        "{:?}",
        rejection.detail
    );
}
