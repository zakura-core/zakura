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

use std::{net::SocketAddr, sync::Arc, time::Duration};

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
    mempool::{
        port::{
            model::{
                Effect as ModelEffect, ModelBlock, ModelTransition, ModelTx, ModelUnreserved,
                ModelVerdict, Outcome as ModelOutcome, OutputAnswer as ModelOutputAnswer,
                PoolModel, Step as ModelStep,
            },
            ChainTip, ChainTransition, Charge, ConnectedBlock, Generation, GenerationRead, Intent,
            LocalFault, RejectionScope, ReservationId, Source, Supplier, Unreserved, Verdict,
        },
        QueueSource,
    },
};
use zakura_rt::collections::HashSet;

use super::{Effect, Grant, Limits, MempoolState, Outcome, OutputAnswer, Step};
use crate::{
    config::Config,
    storage::{ExactTipRejectionError, RejectionError},
    Instant,
};

/// The verifier's error in these tests.
#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
#[error("the test verifier rejected the transaction")]
struct TestVerifyError;

/// The machine under test.
type Machine = MempoolState<TestVerifyError>;

/// The tip a run starts at.
const START: u32 = 100;

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
struct Tx {
    real: VerifiedUnminedTx,
    model: ModelTx,
}

impl Tx {
    /// Its exact id.
    fn id(&self) -> UnminedTxId {
        self.model.id
    }
}

/// Builds a V6 transaction that spends `spends`, creates `outputs` P2PKH
/// outputs and expires at `expiry`. Each one reveals a new Ironwood
/// nullifier, so two never conflict through a nullifier.
fn build(runner: &mut TestRunner, spends: &[OutPoint], outputs: u32, expiry: Option<Height>) -> Tx {
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
        txs.push(build(runner, &spends, outputs, expiry));
    }
    txs
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

/// A legacy peer with its own IP address.
fn legacy(n: u8) -> QueueSource {
    QueueSource::LegacySocket(SocketAddr::from(([10, 0, 0, n], 8233)))
}

/// A tip at `height` on the fork numbered `fork`.
fn tip(height: u32, fork: u8) -> ChainTip {
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
        Verdict::NoRoom(limit) => ModelVerdict::NoRoom(*limit),
        Verdict::CoolingDown => ModelVerdict::CoolingDown,
        Verdict::LocalFault(fault) => ModelVerdict::LocalFault(*fault),
    }
}

/// The model's answer for a real refused reservation.
fn model_unreserved(unreserved: &Unreserved) -> ModelUnreserved {
    match unreserved {
        Unreserved::Inactive => ModelUnreserved::Inactive,
        Unreserved::CoolingDown => ModelUnreserved::CoolingDown,
        Unreserved::Pooled => ModelUnreserved::Pooled,
        Unreserved::Pending => ModelUnreserved::Pending,
        Unreserved::Rejected(rejection) => ModelUnreserved::Rejected(rejection.scope),
        Unreserved::NoRoom(limit) => ModelUnreserved::NoRoom(*limit),
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
            } => Seen::Start {
                job: job.0,
                tx: tx.id(),
                revalidation: *revalidation,
            },
            Effect::Cancel(job) => Seen::Cancel(job.0),
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
    source: Source,
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
    /// Output waiters that may still be answered.
    output_waiters: Vec<u64>,
    peers: Vec<QueueSource>,
    steps: usize,
    /// Whether the run is draining, when every job is answered for good.
    draining: bool,
    /// The transactions of the universe the current chain mined.
    mined: HashSet<transaction::Hash>,
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
            per_source: 2,
            revalidating: 3,
        };
        let base = Instant::now();
        let machine = Machine::new(
            &Config {
                tx_cost_limit: u64::MAX / 2,
                ..Config::default()
            },
            tip(START, 0),
            Generation(0),
            base,
        )
        .with_limits(limits);
        let model = PoolModel::new(Height(START), Generation(0)).with_limits(
            limits.pending,
            limits.per_source,
            limits.revalidating,
        );
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
            output_waiters: Vec::new(),
            peers: (1..=6).map(legacy).collect(),
            steps: 0,
            draining: false,
            mined: HashSet::default(),
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
        for effect in seen {
            match effect {
                Seen::Start { job, tx, .. } => self.jobs.push((job, tx)),
                Seen::Cancel(job) => self.jobs.retain(|(started, _)| *started != job),
                Seen::OutputReady { waiter, .. } | Seen::OutputWaitEnded(waiter) => {
                    self.output_waiters.retain(|w| *w != waiter)
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
        for peer in &self.peers {
            let source = Source::Peer(peer.clone());
            assert_eq!(
                self.machine.counted_against(&source),
                self.model.counted_against(&source),
                "step {step} ({what}): slots counted against {peer:?}"
            );
            assert_eq!(
                self.machine.is_cooling(peer, self.base + self.now),
                self.model.cooling(peer, self.now),
                "step {step} ({what}): cooldown of {peer:?}"
            );
        }
        for tx in &self.txs {
            assert_eq!(
                self.machine.is_parked(tx.id()),
                self.model.is_parked(tx.id()),
                "step {step} ({what}): whether {:?} is parked",
                tx.id()
            );
        }
    }

    /// A random source: a peer or this node.
    fn source(&mut self) -> Source {
        if chance(&mut self.rng, 4) {
            Source::Node
        } else {
            let peer = self.peers[pick(&mut self.rng, self.peers.len())].clone();
            Source::Peer(peer)
        }
    }

    /// One random input to both sides.
    fn input(&mut self) {
        self.steps += 1;
        match pick(&mut self.rng, 20) {
            0..=4 => self.reserve(),
            5..=8 => self.submit(),
            9 => self.drop_held(),
            10..=13 => self.outcome(),
            14 => self.transition(),
            15 => self.tick(),
            16 => self.await_output(),
            17 => self.violation(),
            18 => self.rejected_ids(),
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
        let source = self.source();
        let intent = if chance(&mut self.rng, 2) {
            Intent::Fetch
        } else {
            Intent::Deliver
        };
        let id = self.txs[tx].id();
        let at = self.at();
        let real = self.machine.reserve(id, source.clone(), intent, at);
        let model = self.model.reserve(id, source.clone(), intent, self.now);
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
                    source,
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
        // Sometimes another peer answers, or nobody can be charged.
        let supplier = match (&held.source, pick(&mut self.rng, 4)) {
            (_, 0) => Supplier::Nobody,
            (_, 1) => Supplier::Peer(self.peers[pick(&mut self.rng, self.peers.len())].clone()),
            (Source::Peer(peer), _) => Supplier::Peer(peer.clone()),
            (Source::Node, _) => Supplier::Nobody,
        };
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
                    .submit(held.id, body.real.transaction.clone(), supplier.clone(), at),
                self.model
                    .submit(held.id, body.model.clone(), supplier, self.now),
            )
        } else {
            (
                self.machine.wait(held.id, body.id(), supplier.clone(), at),
                self.model.wait(held.id, body.id(), supplier, self.now),
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
        if self.jobs.is_empty() {
            return self.tick();
        }
        let (job, tx_id) = self.jobs.swap_remove(pick(&mut self.rng, self.jobs.len()));
        let tx = self
            .txs
            .iter()
            .find(|tx| tx.id() == tx_id)
            .expect("a job names a transaction of the universe")
            .clone();
        let visible: HashSet<transaction::Hash> = self
            .model
            .visible()
            .iter()
            .map(UnminedTxId::mined_id)
            .collect();
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
                (
                    Outcome::Fail {
                        error: RejectionError::ExactTip(
                            ExactTipRejectionError::FailedVerification(TestVerifyError),
                        ),
                        charge,
                    },
                    ModelOutcome::Fail {
                        scope: RejectionScope::ExactTip,
                        charge,
                    },
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
        let blocks = if disconnected > 0 {
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

    fn violation(&mut self) {
        // Rare, so peers are not all cooling down for most of a run.
        if !chance(&mut self.rng, 4) {
            return self.tick();
        }
        let peer = self.peers[pick(&mut self.rng, self.peers.len())].clone();
        let at = self.at();
        self.machine.violation(&peer, at);
        self.model.violation(peer, self.now);
        let (real, model) = (self.machine.effects(), self.model.effects());
        self.compare("violation", real, model);
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
            "output ready",
            "output wait ended",
            "removed Mined",
            "removed Conflicted",
            "removed Expired",
            "removed Invalidated",
            "removed Deactivated",
            "verdict Admitted",
            "verdict CoolingDown",
            "verdict LocalFault(NoBody)",
            "verdict LocalFault(PoolUnsettled)",
            "verdict LocalFault(StaleReservation)",
            "verdict LocalFault(VerifyTimeout)",
            "verdict LocalFault(WrongTransaction)",
            "verdict LocalFault(Deactivated)",
            "unreserved Pending",
            "unreserved Pooled",
            "unreserved CoolingDown",
            "unreserved Inactive",
            "unreserved NoRoom(Node)",
            "unreserved NoRoom(Source)",
        ] {
            assert!(
                coverage.0.get(kind).is_some_and(|count| *count > 0),
                "the runs never reached {kind}: {coverage:#?}"
            );
        }
    }
}
