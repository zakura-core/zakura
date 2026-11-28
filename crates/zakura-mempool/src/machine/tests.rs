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
        ChainTip, Charge, ConnectedBlock, LocalFault, RelayedFetch, ReservationId, Unreserved,
        Verdict,
    },
};
use zakura_rt::collections::HashSet;

use self::tracker::Tracker;
use super::{Effect, JobId, Limits, MempoolState, Outcome, Step, WaiterId};
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

    /// A machine at the start tip, with `config` and `limits`, turned on.
    fn with(config: Config, limits: Limits) -> Self {
        let base = Instant::now();
        let mut machine = Machine::new(&config, base).with_limits(limits);
        assert!(machine.reset(tip(START, 0), base).is_empty());
        assert!(machine.near_tip(true, base).is_empty());
        Self {
            machine,
            tracker: Tracker::default(),
            base,
            now: Duration::ZERO,
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
    fn reserve(&mut self, tx: &Tx) -> (Result<ReservationId, Unreserved>, Step) {
        let (answer, step) = self.machine.reserve(tx.id(), self.at());
        self.tracker.reserve_answered(tx.id(), &answer);
        (answer, self.took(step))
    }

    /// Reserves `tx` for a `tx-relay` fetch whose peer announced
    /// `announced_size` and whose caller leased `leased_bytes` for it.
    fn reserve_relayed(
        &mut self,
        tx: &Tx,
        announced_size: u64,
        leased_bytes: u64,
    ) -> (Result<ReservationId, Unreserved>, Step) {
        let fetch = RelayedFetch {
            announced_size,
            leased_bytes,
        };
        let (answer, step) = self.machine.reserve_relayed(tx.id(), fetch, self.at());
        self.tracker.reserve_answered(tx.id(), &answer);
        if let Ok(reservation) = answer {
            self.tracker.tx_leased(reservation);
        }
        (answer, self.took(step))
    }

    /// Reserves `tx`, and expects a grant.
    fn reserved(&mut self, tx: &Tx) -> ReservationId {
        match self.reserve(tx).0 {
            Ok(reservation) => reservation,
            Err(refused) => panic!("the reservation was refused: {refused:?}"),
        }
    }

    /// Submits the body of `tx` under `reservation`.
    fn submit(&mut self, reservation: ReservationId, tx: &Tx) -> (WaiterId, Step) {
        self.submit_until(reservation, tx, None)
    }

    /// Submits the body of `tx` under `reservation`, whose submitter stops
    /// waiting at `give_up` on the test's clock, if set.
    fn submit_until(
        &mut self,
        reservation: ReservationId,
        tx: &Tx,
        give_up: Option<Duration>,
    ) -> (WaiterId, Step) {
        let give_up = give_up.map(|at| self.base + at);
        let (waiter, step) = self.machine.submit(
            reservation,
            tx.real.transaction.clone(),
            give_up,
            None,
            self.at(),
        );
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

    /// Reports `outcome` for `job`.
    fn outcome(&mut self, job: JobId, outcome: Outcome<TestVerifyError>) -> Step {
        let charge = match &outcome {
            Outcome::Fail { charge, .. } => Some(*charge),
            _ => None,
        };
        let passed = matches!(outcome, Outcome::Pass { .. });
        self.tracker.answers(job, passed, charge);
        let step = self
            .machine
            .outcome(job, outcome, self.at(), chrono::Utc::now());
        self.took(step)
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

    /// The chain grows by one block that mines `mined`.
    fn grow(&mut self, mined: &[&Tx]) -> Step {
        self.height += 1;
        let at = self.tip();
        let block = ConnectedBlock {
            hash: at.hash,
            height: at.height,
            transactions: mined
                .iter()
                .map(|tx| TransactionHandle::Shared(tx.real.transaction.transaction().clone()))
                .collect(),
            transaction_hashes: mined.iter().map(|tx| tx.id().mined_id()).collect(),
        };
        self.tracker.tip_changed();
        let step = self.machine.grow(block, self.at());
        let step = self.took(step);
        self.tracker.connected(mined);
        step
    }

    /// The tip moves to another fork at the same height, and the machine is
    /// told only the new tip.
    fn reset(&mut self) -> Step {
        self.reset_by(0)
    }

    /// The tip moves `blocks` up without the machine seeing them, or to
    /// another fork at the same height if that is zero. The machine is told
    /// only the new tip.
    fn reset_by(&mut self, blocks: u32) -> Step {
        if blocks == 0 {
            self.fork = self.fork.wrapping_add(1);
        }
        self.height += blocks;
        self.tracker.tip_changed();
        let step = self.machine.reset(self.tip(), self.at());
        self.took(step)
    }

    /// `by` passes, and the machine is told the time.
    fn tick(&mut self, by: Duration) -> Step {
        self.now += by;
        let step = self.machine.tick(self.at());
        self.took(step)
    }

    /// The node falls behind the tip, and the pool turns off.
    fn deactivate(&mut self) -> Step {
        let step = self.machine.near_tip(false, self.at());
        self.tracker.forget_rejections();
        let step = self.took(step);
        self.tracker.every_submission_is_answered();
        self.tracker.pool_is_empty();
        step
    }

    /// The node is near the tip again, and the pool turns on.
    fn activate(&mut self) -> Step {
        let step = self.machine.near_tip(true, self.at());
        self.took(step)
    }

    /// The members of `txs` a cached rejection covers.
    fn rejected(&self, txs: &[&Tx]) -> HashSet<UnminedTxId> {
        let ids: HashSet<UnminedTxId> = txs.iter().map(|tx| tx.id()).collect();
        let rejected = self.machine.rejected_ids(ids.clone(), self.at());
        assert!(
            rejected.is_subset(&ids),
            "a rejection of an id nobody asked about"
        );
        rejected
    }
}

/// The size of `tx`'s body.
fn size(tx: &Tx) -> u64 {
    u64::try_from(tx.real.transaction.size()).expect("a test body's size fits in u64")
}

/// Whether `step` ends the memory lease of the relayed fetch under
/// `reservation`.
fn ends_tx_lease(step: &Step, reservation: ReservationId) -> bool {
    step.effects
        .iter()
        .any(|effect| matches!(effect, Effect::EndTxLease(ended) if *ended == reservation))
}

/// mem-7 (`mempool-pending-bytes`), `02 §7` item 7:
/// - A relayed fetch without a covering memory lease is reserved only into a
///   pool that holds nothing. A second one, or one into a pool that holds a
///   transaction, gets `NoRoom`.
/// - A leased fetch's lease stays counted through verification, and ends
///   when the transaction is decided or the reservation is dropped.
/// - A body larger than the memory lease its fetch announced is not the body
///   announced. It never reaches a verifier, and its lease ends.
#[test]
fn mem7_relayed_lease_lifecycle_and_oversized_body() {
    let mut bench = Bench::new();
    let mut runner = TestRunner::deterministic();
    let txs: Vec<Tx> = (0..4).map(|_| build(&mut runner, &[], 0, None)).collect();

    let (unleased, step) = bench.reserve_relayed(&txs[0], size(&txs[0]), 0);
    let unleased = unleased.expect("a pool that holds nothing takes one unleased fetch");
    assert!(!ends_tx_lease(&step, unleased));
    assert!(
        matches!(
            bench.reserve_relayed(&txs[1], size(&txs[1]), 0).0,
            Err(Unreserved::NoRoom)
        ),
        "a second unleased fetch waits for the first"
    );

    // A leased fetch comes in beside it, and its lease stays counted until
    // the verdict.
    let leased = bench
        .reserve_relayed(&txs[1], size(&txs[1]), size(&txs[1]))
        .0
        .expect("a leased fetch is reserved");
    let (_, step) = bench.submit(leased, &txs[1]);
    assert!(
        !ends_tx_lease(&step, leased),
        "verification holds the lease"
    );
    let job = started(&step)[0];
    assert!(ends_tx_lease(&bench.pass(job, &txs[1]), leased));

    // Dropping the unleased fetch ends it. The pool holds a transaction now,
    // so the next unleased fetch waits.
    assert!(ends_tx_lease(&bench.release(unleased), unleased));
    assert!(matches!(
        bench.reserve_relayed(&txs[2], size(&txs[2]), 0).0,
        Err(Unreserved::NoRoom)
    ));
    let leased = bench
        .reserve_relayed(&txs[2], size(&txs[2]), size(&txs[2]))
        .0
        .expect("a leased fetch is reserved");
    assert!(ends_tx_lease(&bench.release(leased), leased));

    let announced = size(&txs[3]) - 1;
    let reservation = bench
        .reserve_relayed(&txs[3], announced, announced)
        .0
        .expect("a leased fetch is reserved");
    let (waiter, step) = bench.submit(reservation, &txs[3]);
    assert!(started(&step).is_empty());
    assert!(matches!(
        step.verdict(waiter),
        Some(Verdict::LocalFault(LocalFault::WrongTransaction))
    ));
    assert!(ends_tx_lease(&step, reservation));
}
