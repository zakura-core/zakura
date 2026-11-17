//! The machine tests.
//!
//! Every test here drives [`MempoolState`], the one encoding of the pool's
//! rules. None restates a rule to compare against.
//!
//! - [`random`] gives the machine random inputs. After each one it checks
//!   the machine's own invariants, and what a [`tracker::Tracker`] recorded
//!   of the run's inputs and the machine's outputs. At the end every caller
//!   lets go, and the run must drain.
//! - [`scenarios`] pins one rule each, by name.
//!
//! The transactions are real: V6 transactions with transparent spends
//! between them, so dependencies, conflicts and expiry all happen.

use std::{sync::Arc, time::Duration};

use proptest::{prelude::*, strategy::ValueTree, test_runner::TestRunner};
use zakura_chain::{
    amount::Amount,
    at_least_one,
    block::{self, Height},
    ironwood,
    orchard::{self, tree},
    parameters::NetworkUpgrade,
    primitives::Halo2Proof,
    transaction::{LockTime, Transaction, UnminedTxId, VerifiedUnminedTx},
    transparent::{self, OutPoint},
};
use zakura_node_services::{
    block_memory::TransactionHandle,
    mempool::port::{
        ChainTip, ChainTransition, Charge, ConnectedBlock, Generation, GenerationRead, Intent,
        ReservationId, Unreserved, Verdict,
    },
};
use zakura_rt::collections::HashSet;

use self::tracker::Tracker;
use super::{Effect, Grant, JobId, Limits, MempoolState, Outcome, OutputAnswer, Step, WaiterId};
use crate::{config::Config, storage::RejectionError, Instant};

mod random;
mod scenarios;
mod tracker;

/// The verifier's error in these tests.
#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
#[error("the test verifier rejected the transaction")]
pub(crate) struct TestVerifyError;

/// The machine under test.
pub(crate) type Machine = MempoolState<TestVerifyError>;

/// The tip a test starts at.
const START: u32 = 100;

/// A P2PKH lock script to a zero key hash, which is standard.
fn p2pkh() -> transparent::Script {
    let mut script = vec![0x76, 0xa9, 0x14];
    script.extend_from_slice(&[0; 20]);
    script.extend_from_slice(&[0x88, 0xac]);
    transparent::Script::new(&script)
}

/// One transaction a test built.
#[derive(Clone)]
pub(crate) struct Tx {
    /// The transaction, as a verifier passes it.
    pub(crate) real: VerifiedUnminedTx,
}

impl Tx {
    /// Its exact id.
    pub(crate) fn id(&self) -> UnminedTxId {
        self.real.transaction.id()
    }

    /// The outputs it spends.
    pub(crate) fn spends(&self) -> Vec<OutPoint> {
        self.real
            .transaction
            .transaction()
            .spent_outpoints()
            .collect()
    }

    /// Its output at `index`.
    pub(crate) fn output(&self, index: u32) -> Option<&transparent::Output> {
        self.real
            .transaction
            .transaction()
            .outputs()
            .get(usize::try_from(index).expect("a small index"))
    }

    /// The outpoint of its output at `index`.
    pub(crate) fn outpoint(&self, index: u32) -> OutPoint {
        OutPoint {
            hash: self.id().mined_id(),
            index,
        }
    }
}

/// Wraps `transaction` as a verifier passes it, paying `fee` zatoshis.
fn verified(transaction: Transaction, fee: i64) -> VerifiedUnminedTx {
    VerifiedUnminedTx::new(
        Arc::new(transaction).into(),
        Amount::try_from(fee).expect("a valid test fee"),
        0,
        0,
        Arc::new(Vec::new()),
    )
    .expect("the test fee passes the ZIP-317 checks")
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
    Tx {
        real: verified(transaction, 1_000_000),
    }
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
    let twin = Tx {
        real: verified(transaction, 1_000_000),
    };
    assert_eq!(
        twin.id().mined_id(),
        tx.id().mined_id(),
        "a twin has the same effects"
    );
    assert_ne!(twin.id(), tx.id(), "a twin has other authorizing data");
    twin
}

/// `tx` paying `fee` zatoshis.
fn paying(tx: &Tx, fee: i64) -> Tx {
    Tx {
        real: verified((**tx.real.transaction.transaction()).clone(), fee),
    }
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

/// The jobs `step` started, in order.
fn started(step: &Step) -> Vec<JobId> {
    step.effects
        .iter()
        .filter_map(|effect| match effect {
            Effect::Start { job, .. } => Some(*job),
            _ => None,
        })
        .collect()
}

/// One machine, driven by hand. Every step it takes is recorded by a
/// [`Tracker`] and followed by the machine's invariants.
struct Bench {
    machine: Machine,
    tracker: Tracker,
    base: Instant,
    /// How far the test's clock is past `base`.
    now: Duration,
    /// The generation of the last chain event the machine was handed.
    generation: u64,
    height: u32,
    fork: u8,
}

impl Bench {
    /// A machine at the start tip, with a cost limit no test reaches and
    /// production's other limits.
    fn new() -> Self {
        Self::with(
            Config {
                tx_cost_limit: u64::MAX / 2,
                ..Config::default()
            },
            Limits::default(),
        )
    }

    /// A machine at the start tip, with `config` and `limits`.
    fn with(config: Config, limits: Limits) -> Self {
        let base = Instant::now();
        Self {
            machine: Machine::new(&config, tip(START, 0), Generation(0), base).with_limits(limits),
            tracker: Tracker::default(),
            base,
            now: Duration::ZERO,
            generation: 0,
            height: START,
            fork: 0,
        }
    }

    /// The machine's instant for the test's clock.
    fn at(&self) -> Instant {
        self.base + self.now
    }

    /// The tip the machine was last handed.
    fn tip(&self) -> ChainTip {
        tip(self.height, self.fork)
    }

    /// Records the step an input caused, and checks the machine after it.
    fn took(&mut self, step: Step) -> Step {
        self.tracker.step(&step);
        self.machine.assert_invariants();
        self.tracker.check(&self.machine);
        step
    }

    /// Reserves `tx`. Returns the answer, and what else the input caused.
    fn reserve(&mut self, tx: &Tx, intent: Intent) -> (Result<Grant, Unreserved>, Step) {
        let answer = self.machine.reserve(tx.id(), intent, self.at());
        if let Ok(grant) = &answer {
            self.tracker.reserved(grant.id);
        }
        let step = self.machine.effects();
        (answer, self.took(step))
    }

    /// Reserves `tx` as a caller that holds its body, and expects a grant.
    fn reserved(&mut self, tx: &Tx) -> ReservationId {
        match self.reserve(tx, Intent::Deliver).0 {
            Ok(grant) => grant.id,
            Err(refused) => panic!("the reservation was refused: {refused:?}"),
        }
    }

    /// Submits the body of `tx` under `reservation`.
    fn submit(&mut self, reservation: ReservationId, tx: &Tx) -> (WaiterId, Step) {
        let (waiter, step) =
            self.machine
                .submit(reservation, tx.real.transaction.clone(), self.at());
        self.tracker.waits(waiter, tx.id());
        (waiter, self.took(step))
    }

    /// Waits for the verdict of `tx` under a joined `reservation` that was
    /// not asked for the body.
    fn wait(&mut self, reservation: ReservationId, tx: &Tx) -> (WaiterId, Step) {
        let (waiter, step) = self.machine.wait(reservation, tx.id(), self.at());
        self.tracker.waits(waiter, tx.id());
        (waiter, self.took(step))
    }

    /// Reserves `tx` and submits its body. Returns its waiter and the job
    /// that started.
    fn start(&mut self, tx: &Tx) -> (WaiterId, JobId) {
        let reservation = self.reserved(tx);
        let (waiter, step) = self.submit(reservation, tx);
        let jobs = started(&step);
        assert_eq!(jobs.len(), 1, "a body starts one job: {step:?}");
        (waiter, jobs[0])
    }

    /// Drops `reservation`.
    fn release(&mut self, reservation: ReservationId) -> Step {
        let step = self.machine.release(reservation, self.at());
        self.took(step)
    }

    /// The fetch under `reservation` ended without a body.
    fn fetch_failed(&mut self, reservation: ReservationId) -> Step {
        let step = self.machine.fetch_failed(reservation, self.at());
        self.took(step)
    }

    /// Reports `outcome` for `job`, with the generation readings `before`
    /// and `after` its lookups.
    fn outcome_read(
        &mut self,
        job: JobId,
        outcome: Outcome<TestVerifyError>,
        before: GenerationRead,
        after: GenerationRead,
    ) -> Step {
        // The test knows the answer is not stale when both readings are the
        // generation it last handed the machine.
        let current = GenerationRead::Stable(Generation(self.generation));
        let charge = match &outcome {
            Outcome::Fail { charge, .. } if before == current && after == current => Some(*charge),
            _ => None,
        };
        self.tracker.answers(job, charge);
        let step = self
            .machine
            .outcome(job, outcome, before, after, self.at(), chrono::Utc::now());
        self.took(step)
    }

    /// Reports `outcome` for `job`, read at the generation the machine has.
    fn outcome(&mut self, job: JobId, outcome: Outcome<TestVerifyError>) -> Step {
        let read = GenerationRead::Stable(Generation(self.generation));
        self.outcome_read(job, outcome, read, read)
    }

    /// `job` passes `tx`, which spends `pool_inputs` from the pool.
    fn pass_using(&mut self, job: JobId, tx: &Tx, pool_inputs: Vec<OutPoint>) -> Step {
        self.outcome(
            job,
            Outcome::Pass {
                tx: tx.real.clone(),
                pool_inputs,
            },
        )
    }

    /// `job` passes `tx`, which spends nothing from the pool.
    fn pass(&mut self, job: JobId, tx: &Tx) -> Step {
        self.pass_using(job, tx, Vec::new())
    }

    /// `job` fails with `error`, which earns its supplier `charge`.
    fn fail(
        &mut self,
        job: JobId,
        error: impl Into<RejectionError<TestVerifyError>>,
        charge: Charge,
    ) -> Step {
        self.outcome(
            job,
            Outcome::Fail {
                error: error.into(),
                charge,
            },
        )
    }

    /// `job` finds `missing` in neither the chain nor the pool.
    fn missing(&mut self, job: JobId, missing: Vec<OutPoint>) -> Step {
        self.outcome(
            job,
            Outcome::MissingInputs {
                missing,
                error: TestVerifyError,
            },
        )
    }

    /// Admits `tx`, which spends `pool_inputs` from the pool.
    fn admit_using(&mut self, tx: &Tx, pool_inputs: Vec<OutPoint>) {
        let (waiter, job) = self.start(tx);
        let step = self.pass_using(job, tx, pool_inputs);
        assert!(
            matches!(step.verdict(waiter), Some(Verdict::Admitted)),
            "the pass admits it: {step:?}"
        );
    }

    /// Admits `tx`, which spends nothing from the pool.
    fn admit(&mut self, tx: &Tx) {
        self.admit_using(tx, Vec::new());
    }

    /// `disconnect` blocks leave the chain, then one block joins it for
    /// each entry of `blocks`, mining that entry's transactions.
    fn transition(&mut self, disconnect: u32, blocks: &[Vec<&Tx>], rules_change: bool) -> Step {
        self.generation += 1;
        let before = self.tip();
        let old_fork = self.fork;
        if disconnect > 0 {
            self.fork = self.fork.wrapping_add(1);
        }
        let base = self.height - disconnect;
        let connected: Vec<ConnectedBlock> = blocks
            .iter()
            .enumerate()
            .map(|(index, mined)| {
                let height = base + 1 + u32::try_from(index).expect("a small block count");
                let at = tip(height, self.fork);
                ConnectedBlock {
                    hash: at.hash,
                    height: at.height,
                    transactions: mined
                        .iter()
                        .map(|tx| {
                            TransactionHandle::Shared(tx.real.transaction.transaction().clone())
                        })
                        .collect(),
                    transaction_hashes: mined.iter().map(|tx| tx.id().mined_id()).collect(),
                }
            })
            .collect();
        self.height = base + u32::try_from(blocks.len()).expect("a small block count");
        let transition = ChainTransition {
            before: Some(before),
            disconnected: (0..disconnect)
                .map(|back| tip(before.height.0 - back, old_fork))
                .collect(),
            connected,
            after: self.tip(),
            generation: Generation(self.generation),
            rules_change,
        };
        let step = self.machine.transition(transition, self.at());
        let step = self.took(step);
        let mined: Vec<&Tx> = blocks.iter().flatten().copied().collect();
        self.tracker.connected(&mined);
        step
    }

    /// The chain grows by one block that mines `mined`.
    fn grow(&mut self, mined: &[&Tx]) -> Step {
        self.transition(0, &[mined.to_vec()], false)
    }

    /// The tip block leaves the chain, and a block that mines `mined` takes
    /// its place.
    fn reorg(&mut self, mined: &[&Tx]) -> Step {
        self.transition(1, &[mined.to_vec()], false)
    }

    /// The machine missed chain events, and is told only the tip.
    fn resync(&mut self) -> Step {
        self.generation += 1;
        let step = self
            .machine
            .resync(self.tip(), Generation(self.generation), self.at());
        self.took(step)
    }

    /// `by` passes, and the machine is told the time.
    fn tick(&mut self, by: Duration) -> Step {
        self.now += by;
        let step = self.machine.tick(self.at());
        self.took(step)
    }

    /// The pool turns off.
    fn deactivate(&mut self) -> Step {
        let step = self.machine.deactivate(self.at());
        let step = self.took(step);
        self.tracker.every_submission_is_answered();
        self.tracker.pool_is_empty();
        step
    }

    /// The pool turns on again, at the tip the test is at.
    fn activate(&mut self) -> Step {
        self.machine
            .activate(self.tip(), Generation(self.generation), self.at());
        let step = self.machine.effects();
        self.took(step)
    }

    /// Waits for `outpoint`. Returns the answer, and what else the input
    /// caused.
    fn await_output(&mut self, outpoint: OutPoint) -> (OutputAnswer, Step) {
        let answer = self.machine.await_output(outpoint, self.at(), |_| false);
        if let OutputAnswer::Waiting(waiter) = &answer {
            self.tracker.waits_for_output(*waiter);
        }
        let step = self.machine.effects();
        (answer, self.took(step))
    }

    /// The members of `txs` a cached rejection covers.
    fn rejected(&mut self, txs: &[&Tx]) -> HashSet<UnminedTxId> {
        let ids: HashSet<UnminedTxId> = txs.iter().map(|tx| tx.id()).collect();
        let rejected = self.machine.rejected_ids(ids.clone(), self.at());
        assert!(
            rejected.is_subset(&ids),
            "a rejection of an id nobody asked about"
        );
        let step = self.machine.effects();
        self.took(step);
        rejected
    }
}
