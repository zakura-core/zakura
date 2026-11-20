//! Named scenarios: each pins one rule of the machine.

use std::{sync::Arc, time::Duration};

use proptest::test_runner::TestRunner;
use zakura_chain::{
    amount::Amount,
    block::Height,
    transaction::{self, Transaction, UnminedTxId},
    transparent::OutPoint,
};
use zakura_node_services::mempool::port::{
    ChainReason, Change, Charge, LocalFault, Rejection, RejectionScope, RemovalReason, Unreserved,
    Verdict, T_PARK, T_VERIFY,
};

use super::{build, paying, started, twin, verified, Bench, TestVerifyError, Tx, START};
use crate::{
    config::Config,
    machine::{Effect, JobId, Limits, Outcome, Step, WaiterId, RETRY_GAP},
    storage::{
        ExactTipRejectionError, NonStandardTransactionError, RejectionError,
        SameEffectsChainRejectionError, SameEffectsTipRejectionError,
    },
    MempoolError,
};

/// An output on the chain that no test transaction creates.
fn coin(tag: u8) -> OutPoint {
    OutPoint {
        hash: transaction::Hash([tag; 32]),
        index: 0,
    }
}

/// The rejection `step` gave `waiter`'s submission.
fn rejection(step: &Step, waiter: WaiterId) -> Rejection {
    match step.verdict(waiter) {
        Some(Verdict::Rejected(rejection)) => rejection.clone(),
        other => panic!("the submission is rejected: {other:?}"),
    }
}

// Charges: who a verdict costs.

/// A failed verification earns its supplier the charge the verifier gave
/// it, a cooldown or a ban.
#[test]
fn a_failed_verification_charges_its_supplier() {
    let mut runner = TestRunner::deterministic();
    for charge in [Charge::Cooldown, Charge::Ban] {
        let tx = build(&mut runner, &[], 1, None);
        let mut bench = Bench::new();
        let (waiter, job) = bench.start(&tx);
        let step = bench.fail(
            job,
            ExactTipRejectionError::FailedVerification(TestVerifyError),
            charge,
        );
        assert_eq!(rejection(&step, waiter).charge, charge);
    }
}

/// A body that passes verification and is refused because the pool already
/// spends one of its inputs earns its supplier nothing: the body was valid.
#[test]
fn a_body_refused_for_a_spend_conflict_charges_nothing() {
    let mut runner = TestRunner::deterministic();
    let pooled = build(&mut runner, &[coin(0xd1)], 1, None);
    let rival = build(&mut runner, &[coin(0xd1)], 1, None);
    let mut bench = Bench::new();
    bench.admit(&pooled);

    let (waiter, job) = bench.start(&rival);
    let step = bench.pass(job, &rival);
    let rejection = rejection(&step, waiter);
    assert_eq!(rejection.charge, Charge::None);
    assert_eq!(rejection.scope, RejectionScope::EffectsTip);
    assert_eq!(bench.machine.pooled_ids(), vec![pooled.id()]);
}

/// A body the pool refuses for its own policy, not for its verification,
/// earns its supplier nothing. Here another authorization of the same
/// effects is already pooled, which is a duplicate and caches nothing.
#[test]
fn a_policy_refusal_charges_nothing() {
    let mut runner = TestRunner::deterministic();
    let pooled = build(&mut runner, &[], 1, None);
    let other = twin(&pooled);
    let mut bench = Bench::new();
    let (_, first_job) = bench.start(&pooled);
    let (waiter, second_job) = bench.start(&other);
    bench.pass(first_job, &pooled);

    // The twin passes its verification, and the pool refuses it because the
    // same effects are already pooled.
    let step = bench.pass(second_job, &other);
    let rejection = rejection(&step, waiter);
    assert_eq!(rejection.charge, Charge::None);
    assert_eq!(rejection.scope, RejectionScope::Uncached);
    assert!(bench.rejected(&[&other]).is_empty(), "nothing is cached");
}

// Rejections.

/// A chain-scoped rejection is still remembered at exactly its memory, as
/// today's list remembers it, and is forgotten a moment later.
#[test]
fn a_rejection_holds_at_exactly_its_memory_and_ends_after() {
    let memory = Duration::from_secs(100);
    let mut runner = TestRunner::deterministic();
    let mined = build(&mut runner, &[], 1, None);
    let mut bench = Bench::with(
        Config {
            eviction_memory_time: memory,
            ..Config::default()
        },
        Limits::default(),
    );

    bench.grow(&[&mined]);
    bench.now = memory;
    assert!(
        matches!(bench.reserve(&mined).0, Err(Unreserved::Rejected(_))),
        "still remembered at its memory"
    );
    bench.now = memory + Duration::from_millis(1);
    assert!(bench.reserve(&mined).0.is_ok(), "forgotten after it");
}

/// A verified body that both conflicts with a pooled one and has a dust
/// output is refused as non-standard, for its exact bytes, as today's
/// insertion checks standardness first.
#[test]
fn a_dust_body_that_also_conflicts_is_refused_as_non_standard() {
    let mut runner = TestRunner::deterministic();
    let pooled = build(&mut runner, &[coin(0xd5)], 1, None);
    let mut transaction = Transaction::clone(
        build(&mut runner, &[coin(0xd5)], 1, None)
            .real
            .transaction
            .transaction(),
    );
    if let Transaction::V6 { outputs, .. } = &mut transaction {
        outputs[0].value = Amount::try_from(1).expect("a valid test amount");
    }
    let dust = Tx {
        real: verified(transaction, 1_000_000),
    };

    let mut bench = Bench::new();
    bench.admit(&pooled);
    let (waiter, job) = bench.start(&dust);
    let step = bench.pass(job, &dust);
    let rejection = rejection(&step, waiter);
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

// The change log.

/// An admission that evicts another transaction logs the addition and the
/// eviction, so the log still leads to the pool. The random runs never
/// evict: their cost limit is out of reach.
#[test]
fn an_admission_that_evicts_logs_both_changes() {
    let mut runner = TestRunner::deterministic();
    let mut txs: Vec<Tx> = (0..3).map(|_| build(&mut runner, &[], 1, None)).collect();
    let limit = txs[0].real.cost() + txs[1].real.cost();
    // Storage evicts only for a body that pays enough more than its victim.
    txs[2] = paying(&txs[2], 100_000_000);
    let mut bench = Bench::with(
        Config {
            tx_cost_limit: limit,
            ..Config::default()
        },
        Limits::default(),
    );

    bench.admit(&txs[0]);
    bench.admit(&txs[1]);
    let (waiter, job) = bench.start(&txs[2]);
    let step = bench.pass(job, &txs[2]);
    assert!(matches!(step.verdict(waiter), Some(Verdict::Admitted)));
    let evicted: Vec<UnminedTxId> = step
        .changes
        .iter()
        .filter_map(|change| match change {
            Change::Removed {
                id,
                reason: RemovalReason::Evicted,
            } => Some(*id),
            _ => None,
        })
        .collect();
    assert_eq!(evicted.len(), 1, "the third admission evicts: {step:?}");
    assert!(step.changes.contains(&Change::Added(txs[2].id())));
    // The bench has already checked that the log leads to the pool and to
    // the snapshot.
    assert_eq!(bench.machine.pooled_ids().len(), 2);
}

// Resets: the pool leaves and comes back through admission.

/// The jobs `step` started, with their transactions.
fn started_for(step: &Step) -> Vec<(JobId, UnminedTxId)> {
    step.effects
        .iter()
        .filter_map(|effect| match effect {
            Effect::Start { job, tx, .. } => Some((*job, tx.id())),
            _ => None,
        })
        .collect()
}

/// A reset takes every pooled transaction out of the pool, oldest first,
/// and verifies each again like a new one. The node holds their bodies, so
/// nobody fetches them. One that passes is added again, with nobody waiting
/// for a verdict.
#[test]
fn a_reset_sends_the_pool_back_through_admission() {
    let mut runner = TestRunner::deterministic();
    let txs = unrelated(&mut runner, 2);
    let mut bench = Bench::new();
    bench.admit(&txs[0]);
    bench.admit(&txs[1]);

    let step = bench.reset();
    let removed = |tx: &Tx| Change::Removed {
        id: tx.id(),
        reason: RemovalReason::Reset,
    };
    assert_eq!(step.changes, vec![removed(&txs[0]), removed(&txs[1])]);
    assert!(bench.machine.pooled_ids().is_empty());
    let jobs = started_for(&step);
    assert_eq!(
        jobs.iter().map(|(_, id)| *id).collect::<Vec<_>>(),
        vec![txs[0].id(), txs[1].id()]
    );
    assert!(matches!(bench.reserve(&txs[0]).0, Err(Unreserved::Pending)));

    for ((job, _), tx) in jobs.into_iter().zip(&txs) {
        let step = bench.pass(job, tx);
        assert_eq!(step.changes, vec![Change::Added(tx.id())]);
        assert!(step.effects.is_empty(), "nobody waits for it: {step:?}");
    }
    assert_eq!(bench.machine.pooled_ids(), vec![txs[0].id(), txs[1].id()]);
}

/// A pooled transaction that fails when it is verified again after a reset
/// stays out, and its rejection is remembered. One that hits a local fault
/// is dropped, and nothing is remembered.
#[test]
fn a_transaction_that_does_not_pass_after_a_reset_stays_out() {
    let mut runner = TestRunner::deterministic();
    let txs = unrelated(&mut runner, 2);
    let mut bench = Bench::new();
    bench.admit(&txs[0]);
    bench.admit(&txs[1]);
    let jobs = started_for(&bench.reset());

    let step = bench.fail(
        jobs[0].0,
        ExactTipRejectionError::FailedVerification(TestVerifyError),
        Charge::Ban,
    );
    assert_eq!(step.changes, vec![Change::Refused(txs[0].id())]);
    assert!(step.effects.is_empty(), "nobody is charged: {step:?}");
    let step = bench.outcome(jobs[1].0, Outcome::LocalFault(LocalFault::StateUnavailable));
    assert_eq!(step.changes, vec![Change::Refused(txs[1].id())]);

    assert!(bench.machine.pooled_ids().is_empty());
    assert_eq!(bench.rejected(&[&txs[0], &txs[1]]).len(), 1);
    assert!(matches!(
        bench.reserve(&txs[0]).0,
        Err(Unreserved::Rejected(_))
    ));
    assert!(bench.reserve(&txs[1]).0.is_ok());
}

/// What a reset took out and no slot has verified again yet counts against
/// the cost limit with the pool. So a reset followed by new admissions
/// cannot hold more than the limit: the newest waiting bodies are dropped.
#[test]
fn transactions_waiting_after_a_reset_count_against_the_cost_limit() {
    let mut runner = TestRunner::deterministic();
    let txs = unrelated(&mut runner, 3);
    let mut bench = Bench::with(
        Config {
            tx_cost_limit: txs[0].real.cost() + txs[1].real.cost(),
            ..Config::default()
        },
        Limits {
            pending: 5,
            readmitting: 1,
        },
    );
    bench.admit(&txs[0]);
    bench.admit(&txs[1]);

    // The first is verified again, and the second waits for a slot.
    let jobs = started_for(&bench.reset());
    assert_eq!(jobs.len(), 1);
    assert_eq!(bench.machine.queued_ids(), vec![txs[1].id()]);

    // A new transaction gets in. With the waiting one it fills the limit.
    bench.admit(&txs[2]);
    assert_eq!(bench.machine.queued_ids(), vec![txs[1].id()]);

    // The first passes again. The pool is full now, so the one still
    // waiting is dropped.
    let step = bench.pass(jobs[0].0, &txs[0]);
    assert_eq!(step.changes, vec![Change::Added(txs[0].id())]);
    assert!(bench.machine.queued_ids().is_empty());
    assert!(started(&step).is_empty());
    assert_eq!(bench.machine.pooled_ids(), vec![txs[2].id(), txs[0].id()]);
}

/// A block that mines a transaction still waiting to be verified again
/// takes it out of the queue.
#[test]
fn a_block_that_mines_a_waiting_transaction_takes_it_out_of_the_queue() {
    let mut runner = TestRunner::deterministic();
    let txs = unrelated(&mut runner, 2);
    let mut bench = Bench::with(
        Config::default(),
        Limits {
            pending: 5,
            readmitting: 1,
        },
    );
    bench.admit(&txs[0]);
    bench.admit(&txs[1]);
    bench.reset();
    assert_eq!(bench.machine.queued_ids(), vec![txs[1].id()]);

    bench.grow(&[&txs[1]]);
    assert!(bench.machine.queued_ids().is_empty());
    assert!(matches!(
        bench.reserve(&txs[1]).0,
        Err(Unreserved::Rejected(_))
    ));
}

/// Another authorization of a waiting transaction's effects can get in
/// first, if its body was already being verified at the reset. The waiting
/// one then has nothing to come back to, and leaves the queue.
#[test]
fn an_authorization_admitted_first_takes_the_place_of_the_one_waiting() {
    let mut runner = TestRunner::deterministic();
    let txs = unrelated(&mut runner, 2);
    let other = twin(&txs[1]);
    let mut bench = Bench::with(
        Config::default(),
        Limits {
            pending: 5,
            readmitting: 1,
        },
    );
    let (waiter, job) = bench.start(&other);
    bench.admit(&txs[0]);
    bench.admit(&txs[1]);

    // The first is verified again, and the second waits for a slot.
    bench.reset();
    assert_eq!(bench.machine.queued_ids(), vec![txs[1].id()]);

    // The other authorization's job was for the old tip, so it runs again,
    // and passes at the new one.
    let rerun = started(&bench.pass(job, &other));
    assert_eq!(rerun.len(), 1);
    let step = bench.pass(rerun[0], &other);
    assert!(matches!(step.verdict(waiter), Some(Verdict::Admitted)));
    assert!(bench.machine.queued_ids().is_empty());
}

/// A job that runs again after a reset is marked, so the verifier charges no
/// cooldown for it, as today's retry after a tip reset is not charged. A job
/// for a body that arrives after the reset is not marked.
#[test]
fn a_job_run_again_after_a_reset_is_marked() {
    let marked = |step: &Step| -> Vec<bool> {
        step.effects
            .iter()
            .filter_map(|effect| match effect {
                Effect::Start { after_reset, .. } => Some(*after_reset),
                _ => None,
            })
            .collect()
    };
    let mut runner = TestRunner::deterministic();
    let before_reset = build(&mut runner, &[], 1, None);
    let after_reset = build(&mut runner, &[], 1, None);
    let mut bench = Bench::new();

    let reservation = bench.reserved(&before_reset);
    let (_, first) = bench.submit(reservation, &before_reset);
    assert_eq!(marked(&first), vec![false]);
    let job = started(&first)[0];

    // A reset lands while the job runs, so its outcome is for another tip
    // and it runs again, marked.
    let step = bench.reset();
    assert!(marked(&step).is_empty());
    let step = bench.outcome(job, Outcome::LocalFault(LocalFault::StateUnavailable));
    assert_eq!(marked(&step), vec![true], "the rerun is marked");

    let reservation = bench.reserved(&after_reset);
    let (_, step) = bench.submit(reservation, &after_reset);
    assert_eq!(marked(&step), vec![false]);
}

/// A job verifies for the block after the tip it started at. An outcome
/// that arrives after the tip moved says nothing about the new tip, pass or
/// fail, so the job runs again and nobody is charged for the old answer.
#[test]
fn an_outcome_for_another_tip_runs_the_job_again() {
    let mut runner = TestRunner::deterministic();
    let txs = unrelated(&mut runner, 2);
    let mut bench = Bench::new();

    let (waiter, job) = bench.start(&txs[0]);
    bench.grow(&[]);
    let step = bench.pass(job, &txs[0]);
    assert!(step.verdict(waiter).is_none());
    assert!(bench.machine.pooled_ids().is_empty());
    let rerun = started(&step);
    assert_eq!(rerun.len(), 1, "the job runs again at the new tip");
    let step = bench.pass(rerun[0], &txs[0]);
    assert!(matches!(step.verdict(waiter), Some(Verdict::Admitted)));

    let (waiter, job) = bench.start(&txs[1]);
    bench.grow(&[]);
    let step = bench.fail(
        job,
        ExactTipRejectionError::FailedVerification(TestVerifyError),
        Charge::Ban,
    );
    assert!(step.verdict(waiter).is_none());
    let rerun = started(&step);
    assert_eq!(rerun.len(), 1, "the job runs again at the new tip");
    let step = bench.pass(rerun[0], &txs[1]);
    assert!(matches!(step.verdict(waiter), Some(Verdict::Admitted)));
}

// Slots: the node's limit, from the reservation to the verdict.

/// A bench whose node limit is five slots.
fn five_slots() -> Bench {
    Bench::with(
        Config::default(),
        Limits {
            pending: 5,
            readmitting: 4,
        },
    )
}

/// `count` transactions that spend nothing from each other.
fn unrelated(runner: &mut TestRunner, count: usize) -> Vec<Tx> {
    (0..count).map(|_| build(runner, &[], 1, None)).collect()
}

/// A slot is taken by the reservation, before any body exists, so the bodies
/// the node holds stay within its limit.
#[test]
fn a_slot_is_held_from_the_reservation() {
    let mut runner = TestRunner::deterministic();
    let txs = unrelated(&mut runner, 6);
    let mut bench = five_slots();
    let reservations: Vec<_> = txs[..5].iter().map(|tx| bench.reserved(tx)).collect();

    // The node is full before one body has been fetched.
    assert!(matches!(bench.reserve(&txs[5]).0, Err(Unreserved::NoRoom)));

    // Every fetch completes. No body exists outside a reservation.
    for (reservation, tx) in reservations.into_iter().zip(&txs) {
        let (_, step) = bench.submit(reservation, tx);
        assert_eq!(started(&step).len(), 1);
    }
    assert_eq!(bench.machine.slots_in_use(), 5);
}

/// A reservation its caller drops frees its slot, and logs nothing.
#[test]
fn a_released_reservation_frees_its_slot() {
    let mut runner = TestRunner::deterministic();
    let txs = unrelated(&mut runner, 6);
    let mut bench = five_slots();
    let held: Vec<_> = txs[..5].iter().map(|tx| bench.reserved(tx)).collect();
    assert!(matches!(bench.reserve(&txs[5]).0, Err(Unreserved::NoRoom)));

    assert!(bench.release(held[0]).is_empty());
    assert!(bench.reserve(&txs[5]).0.is_ok());
    assert_eq!(bench.machine.slots_in_use(), 5);
}

/// An id that is already undecided is not fetched a second time.
#[test]
fn an_undecided_id_is_not_fetched_twice() {
    let mut runner = TestRunner::deterministic();
    let tx = build(&mut runner, &[], 1, None);
    let mut bench = Bench::new();
    assert!(bench.reserve(&tx).0.is_ok());
    assert!(matches!(bench.reserve(&tx).0, Err(Unreserved::Pending)));
}

/// The ids `reserve` would refuse without a slot are known to a caller that
/// asks first: pooled, undecided, waiting after a reset, or rejected.
#[test]
fn the_ids_a_reservation_would_refuse_are_known() {
    let mut runner = TestRunner::deterministic();
    let txs = unrelated(&mut runner, 5);
    let mut bench = Bench::new();
    bench.admit(&txs[0]);
    bench.reserved(&txs[1]);
    let (_, job) = bench.start(&txs[2]);
    bench.fail(
        job,
        ExactTipRejectionError::FailedVerification(TestVerifyError),
        Charge::None,
    );
    let ids = |txs: &[Tx]| {
        txs.iter()
            .map(Tx::id)
            .collect::<zakura_rt::collections::HashSet<_>>()
    };
    assert_eq!(
        bench.machine.known_ids(ids(&txs), bench.at()),
        ids(&txs[..3])
    );

    // A reset puts the pooled one back in line to be verified again.
    let readmitting = Bench::with(
        Config::default(),
        Limits {
            pending: 1,
            readmitting: 1,
        },
    );
    let mut bench = readmitting;
    bench.admit(&txs[3]);
    bench.admit(&txs[4]);
    bench.reset();
    assert_eq!(bench.machine.queued_count(), 1);
    assert_eq!(
        bench.machine.known_ids(ids(&txs[3..]), bench.at()),
        ids(&txs[3..])
    );
}

/// A fetch that is still out when its admission ends keeps its slot: a
/// decoded body is still on its way. Its caller gets the admission's verdict
/// when it comes back.
#[test]
fn a_fetch_that_is_still_out_keeps_its_slot_after_its_admission_ends() {
    let mut runner = TestRunner::deterministic();
    let txs = unrelated(&mut runner, 6);
    let mut bench = five_slots();
    let fetching: Vec<_> = txs[..5].iter().map(|tx| bench.reserved(tx)).collect();

    // The pool turns off, which ends every admission and tells every fetch
    // to stop, and on again before the fetches come back.
    let step = bench.deactivate();
    for reservation in &fetching {
        assert!(
            step.effects
                .iter()
                .any(|effect| matches!(effect, Effect::CancelFetch(id) if id == reservation)),
            "every fetch still out is told to stop"
        );
    }
    assert!(bench.activate().is_empty());
    assert_eq!(bench.machine.slots_in_use(), 5);
    assert!(matches!(bench.reserve(&txs[5]).0, Err(Unreserved::NoRoom)));

    // One fetch comes back and gets its admission's verdict. One brings a
    // body for another id, which is still a wrong body. One fails, which
    // adds nothing to the log: its admission's end is already there.
    let (late, step) = bench.submit(fetching[0], &txs[0]);
    assert!(matches!(
        step.verdict(late),
        Some(Verdict::LocalFault(LocalFault::Deactivated))
    ));
    let (wrong, step) = bench.submit(fetching[1], &txs[5]);
    assert!(matches!(
        step.verdict(wrong),
        Some(Verdict::LocalFault(LocalFault::WrongTransaction))
    ));
    assert!(bench.fetch_failed(fetching[2]).is_empty());
    assert_eq!(bench.machine.slots_in_use(), 2);
    assert!(bench.reserve(&txs[5]).0.is_ok());
}

/// A block that mines an id while it is being fetched tells the fetch to
/// stop. The slot stays counted until the fetch's caller drops it.
#[test]
fn a_block_that_mines_an_id_being_fetched_cancels_the_fetch() {
    let mut runner = TestRunner::deterministic();
    let tx = build(&mut runner, &[], 1, None);
    let mut bench = Bench::new();
    let fetch = bench.reserved(&tx);

    let step = bench.grow(&[&tx]);
    assert!(
        step.effects
            .iter()
            .any(|effect| matches!(effect, Effect::CancelFetch(id) if *id == fetch)),
        "the fetch is told to stop: {step:?}"
    );
    assert_eq!(bench.machine.slots_in_use(), 1);
    bench.release(fetch);
    assert_eq!(bench.machine.slots_in_use(), 0);
}

/// A body that is not the transaction reserved is refused, and frees the
/// slot.
#[test]
fn a_wrong_body_is_refused_and_frees_the_slot() {
    let mut runner = TestRunner::deterministic();
    let txs = unrelated(&mut runner, 2);
    let mut bench = Bench::new();
    let reservation = bench.reserved(&txs[0]);
    let (waiter, step) = bench.submit(reservation, &txs[1]);
    assert!(matches!(
        step.verdict(waiter),
        Some(Verdict::LocalFault(LocalFault::WrongTransaction))
    ));
    assert!(started(&step).is_empty());
    assert_eq!(bench.machine.slots_in_use(), 0);
}

/// A fetch that fails is logged, as today's change stream reports a download
/// that failed.
#[test]
fn a_failed_fetch_is_logged() {
    let mut runner = TestRunner::deterministic();
    let tx = build(&mut runner, &[], 1, None);
    let mut bench = Bench::new();
    let fetching = bench.reserved(&tx);
    let step = bench.fetch_failed(fetching);
    assert_eq!(step.changes, vec![Change::Refused(tx.id())]);
    assert_eq!(bench.machine.slots_in_use(), 0);
}

// Verification.

/// A verification that runs past its limit ends as a local fault, and its
/// late outcome is ignored.
#[test]
fn a_verification_that_runs_too_long_is_a_local_fault() {
    let mut runner = TestRunner::deterministic();
    let tx = build(&mut runner, &[], 1, None);
    let mut bench = Bench::new();
    let (waiter, job) = bench.start(&tx);

    let step = bench.tick(T_VERIFY);
    assert!(matches!(
        step.verdict(waiter),
        Some(Verdict::LocalFault(LocalFault::VerifyTimeout))
    ));
    assert!(step
        .effects
        .iter()
        .any(|effect| matches!(effect, Effect::Cancel(cancelled) if *cancelled == job)));
    assert_eq!(bench.machine.slots_in_use(), 0);

    bench.now += Duration::from_secs(1);
    assert!(bench.pass(job, &tx).is_empty(), "the late pass is ignored");
    assert!(bench.machine.pooled_ids().is_empty());
}

/// A failure on this node is logged, as today's change stream reports a
/// verification that failed, but nothing is cached: the same bytes can be
/// submitted again.
#[test]
fn a_local_fault_is_logged_and_not_cached() {
    let mut runner = TestRunner::deterministic();
    let txs = unrelated(&mut runner, 2);
    let mut bench = Bench::new();
    let (_, job) = bench.start(&txs[0]);
    let step = bench.outcome(job, Outcome::LocalFault(LocalFault::VerifyPanicked));
    assert_eq!(step.changes, vec![Change::Refused(txs[0].id())]);
    assert!(bench.reserve(&txs[0]).0.is_ok());

    bench.start(&txs[1]);
    assert_eq!(
        bench.tick(T_VERIFY).changes,
        vec![Change::Refused(txs[1].id())]
    );
}

/// A transaction the verifier finds already mined is answered after the
/// lookup every time: nothing is cached that would answer the second
/// submission at its reservation.
#[test]
fn a_transaction_found_already_mined_is_answered_the_same_way_each_time() {
    let mut runner = TestRunner::deterministic();
    let tx = build(&mut runner, &[], 1, None);
    let mut bench = Bench::new();
    for _ in 0..2 {
        let (waiter, job) = bench.start(&tx);
        let step = bench.outcome(job, Outcome::AlreadyMined(Arc::new(TestVerifyError)));
        let rejection = rejection(&step, waiter);
        assert_eq!(rejection.scope, RejectionScope::Uncached);
        assert_eq!(rejection.charge, Charge::None);
    }
}

/// The pool does not rely on the verifier for expiry: a transaction at its
/// expiry height does not get in even if a verification passes it.
#[test]
fn nothing_expired_gets_in_even_if_a_verification_passes_it() {
    let mut runner = TestRunner::deterministic();
    let expired = build(&mut runner, &[], 1, Some(Height(START)));
    let mut bench = Bench::new();
    let (waiter, job) = bench.start(&expired);
    let step = bench.pass(job, &expired);
    assert_eq!(
        rejection(&step, waiter).scope,
        RejectionScope::EffectsChain(ChainReason::Expired)
    );
    assert!(bench.machine.pooled_ids().is_empty());
}

/// A child verified against a parent the pool has since lost is not
/// admitted. It is verified again.
#[test]
fn a_child_verified_against_a_parent_the_pool_has_since_lost_is_not_admitted() {
    let mut runner = TestRunner::deterministic();
    let parent = build(&mut runner, &[coin(0xe1)], 1, None);
    let child = build(&mut runner, &[parent.outpoint(0)], 1, None);
    let rival = build(&mut runner, &[coin(0xe1)], 1, None);
    let mut bench = Bench::new();
    bench.admit(&parent);
    let (waiter, job) = bench.start(&child);

    // A block mines a transaction that conflicts with the parent. The
    // child's verifier read the pool that still offered the parent.
    bench.grow(&[&rival]);
    assert!(bench.machine.pooled_ids().is_empty());

    let step = bench.pass_using(job, &child, vec![parent.outpoint(0)]);
    assert!(step.verdict(waiter).is_none());
    assert!(bench.machine.pooled_ids().is_empty());
    assert_eq!(started(&step).len(), 1, "it is verified again");
}

// Parents: a transaction whose input is in neither the chain nor the pool
// waits for it.

/// A parent and a child that spends its first output.
fn family(runner: &mut TestRunner) -> (Tx, Tx) {
    let parent = build(runner, &[], 1, None);
    let child = build(runner, &[parent.outpoint(0)], 1, None);
    (parent, child)
}

/// A parked child runs again when its parent is admitted. Parked, it still
/// holds its slot.
#[test]
fn a_parked_child_runs_again_when_its_parent_is_admitted() {
    let mut runner = TestRunner::deterministic();
    let (parent, child) = family(&mut runner);
    let mut bench = Bench::new();
    let (waiter, job) = bench.start(&child);
    assert!(bench.missing(job, vec![parent.outpoint(0)]).is_empty());
    assert_eq!(bench.machine.slots_in_use(), 1);

    let (_, parent_job) = bench.start(&parent);
    let step = bench.pass(parent_job, &parent);
    let rerun = started(&step);
    assert_eq!(rerun.len(), 1, "the child runs again: {step:?}");
    let step = bench.pass_using(rerun[0], &child, vec![parent.outpoint(0)]);
    assert!(matches!(step.verdict(waiter), Some(Verdict::Admitted)));
    assert_eq!(bench.machine.pooled_ids(), vec![parent.id(), child.id()]);
}

/// A parent admitted after the child's verifier looked, and before the
/// machine hears that it found nothing, does not strand the child: parking
/// then would wait for an admission that has already happened.
#[test]
fn a_parent_admitted_between_the_lookup_and_the_parking_does_not_strand_the_child() {
    let mut runner = TestRunner::deterministic();
    let (parent, child) = family(&mut runner);
    let mut bench = Bench::new();
    let (_, child_job) = bench.start(&child);
    let (_, parent_job) = bench.start(&parent);

    bench.pass(parent_job, &parent);
    let step = bench.missing(child_job, vec![parent.outpoint(0)]);
    assert_eq!(started(&step).len(), 1, "the child runs again at once");
}

/// A parked child runs again when a block mines its parent.
#[test]
fn a_parked_child_runs_again_when_a_block_mines_its_parent() {
    let mut runner = TestRunner::deterministic();
    let (parent, child) = family(&mut runner);
    let mut bench = Bench::new();
    let (_, job) = bench.start(&child);
    bench.missing(job, vec![parent.outpoint(0)]);

    assert_eq!(started(&bench.grow(&[&parent])).len(), 1);
}

/// The wait for a parent is one flat span from when the input was first
/// found missing. It is not paused while the parent is verified, and waking
/// and parking again does not restart it. That keeps the child's lifetime
/// bounded.
#[test]
fn the_wait_for_a_parent_is_flat() {
    let mut runner = TestRunner::deterministic();
    let (parent, child) = family(&mut runner);
    let unrelated = build(&mut runner, &[], 1, None);
    let mut bench = Bench::new();
    let (waiter, job) = bench.start(&child);
    bench.missing(job, vec![parent.outpoint(0)]);

    // Forty seconds in, a reset runs the child again, and the output is
    // still missing.
    bench.now = Duration::from_secs(40);
    let rerun = started(&bench.reset());
    assert_eq!(rerun.len(), 1);
    assert!(bench.missing(rerun[0], vec![parent.outpoint(0)]).is_empty());

    // The parent's body arrives, and its verification is slow.
    bench.start(&parent);
    bench.admit(&unrelated);

    assert!(bench
        .tick(Duration::from_secs(19))
        .verdict(waiter)
        .is_none());
    let step = bench.tick(Duration::from_secs(1));
    assert_eq!(rejection(&step, waiter).scope, RejectionScope::ExactTip);
}

/// A parent that never arrives is a missing-input failure, remembered for
/// those exact bytes until the tip changes, as today.
#[test]
fn a_parent_that_never_arrives_is_remembered_until_the_tip_changes() {
    let mut runner = TestRunner::deterministic();
    let (parent, child) = family(&mut runner);
    let mut bench = Bench::new();
    let (waiter, job) = bench.start(&child);
    bench.missing(job, vec![parent.outpoint(0)]);
    let step = bench.tick(T_PARK);
    let rejection = rejection(&step, waiter);
    assert_eq!(rejection.scope, RejectionScope::ExactTip);
    assert_eq!(rejection.charge, Charge::None);

    assert!(matches!(
        bench.reserve(&child).0,
        Err(Unreserved::Rejected(_))
    ));
    assert!(bench.rejected(&[&child]).contains(&child.id()));
    bench.grow(&[]);
    assert!(bench.reserve(&child).0.is_ok());
}

/// A child that spends an output its pooled parent does not have is not
/// verified in a loop: nothing changed since its verifier looked, so it
/// waits, and the wait ends.
#[test]
fn a_child_that_spends_an_output_its_pooled_parent_lacks_waits_once() {
    let mut runner = TestRunner::deterministic();
    let parent = build(&mut runner, &[], 1, None);
    let child = build(&mut runner, &[parent.outpoint(5)], 1, None);
    let mut bench = Bench::new();
    bench.admit(&parent);
    let (waiter, job) = bench.start(&child);

    bench.now = Duration::from_secs(1);
    assert!(
        bench.missing(job, vec![parent.outpoint(5)]).is_empty(),
        "no immediate second run"
    );
    let step = bench.tick(T_PARK);
    assert_eq!(rejection(&step, waiter).scope, RejectionScope::ExactTip);
    assert_eq!(bench.machine.slots_in_use(), 0);
}

/// Inputs that arrive one at a time earn one check after the wait has ended,
/// and no more. The answer of that check is out of date if another input
/// arrived during it, so it is not remembered as a rejection.
#[test]
fn inputs_that_arrive_one_at_a_time_earn_one_check_after_the_wait() {
    let mut runner = TestRunner::deterministic();
    let parents = unrelated(&mut runner, 3);
    let spends: Vec<OutPoint> = parents.iter().map(|parent| parent.outpoint(0)).collect();
    let child = build(&mut runner, &spends, 1, None);
    let mut bench = Bench::new();
    let (waiter, job) = bench.start(&child);
    bench.missing(job, spends.clone());

    // Just before the wait ends a reset runs the child again. The check is
    // still running when the wait ends, and while it runs the second parent
    // arrives. That earns one more check.
    bench.now = Duration::from_secs(59);
    let rerun = started(&bench.reset());
    assert_eq!(rerun.len(), 1);
    bench.admit(&parents[1]);
    bench.now = Duration::from_secs(62);
    let step = bench.missing(rerun[0], spends.clone());
    assert!(step.verdict(waiter).is_none());
    let last = started(&step);
    assert_eq!(last.len(), 1, "one more check");

    // During that check the third parent arrives too. The first is still
    // missing, and the one extra check is used up.
    bench.admit(&parents[2]);
    bench.now = Duration::from_secs(63);
    let step = bench.missing(last[0], vec![spends[0], spends[2]]);
    assert!(matches!(
        step.verdict(waiter),
        Some(Verdict::LocalFault(LocalFault::VerifyTimeout))
    ));
    assert!(started(&step).is_empty());
    assert!(bench.reserve(&child).0.is_ok());
}

/// A child that spends one output that never exists and an output of each
/// of `parents`, which are not in the pool yet. The verifier reports every
/// spend of such a child as missing.
fn child_of_many(runner: &mut TestRunner, parents: usize) -> (Vec<Tx>, Tx, Vec<OutPoint>) {
    let parents = unrelated(runner, parents);
    let mut spends = vec![coin(0xb0)];
    spends.extend(parents.iter().map(|parent| parent.outpoint(0)));
    let child = build(runner, &spends, 1, None);
    (parents, child, spends)
}

/// Admits `parent` and returns the jobs its admission started.
fn admit_parent(bench: &mut Bench, parent: &Tx) -> Vec<JobId> {
    let (_, job) = bench.start(parent);
    started(&bench.pass(job, parent))
}

/// Arriving parents run a parked transaction again at once at most
/// `MAX_MEMPOOL_ANCESTORS` times per tip, however many it names, and later
/// arrivals at most once per `RETRY_GAP`. A block gives it its retries back.
#[test]
fn arriving_parents_run_a_parked_transaction_again_at_most_twice() {
    let mut runner = TestRunner::deterministic();
    let (parents, child, spends) = child_of_many(&mut runner, 5);
    let mut bench = Bench::new();
    let (waiter, job) = bench.start(&child);
    assert!(bench.missing(job, spends.clone()).is_empty());

    for parent in &parents[..2] {
        let rerun = admit_parent(&mut bench, parent);
        assert_eq!(rerun.len(), 1, "an arriving parent runs the child again");
        assert!(bench.missing(rerun[0], spends.clone()).is_empty());
    }
    for parent in &parents[2..4] {
        assert!(
            admit_parent(&mut bench, parent).is_empty(),
            "the child's retries are used up"
        );
    }
    // Arrivals past the cap wait, and run it once together after the gap.
    let rerun = started(&bench.tick(RETRY_GAP - Duration::from_millis(1)));
    assert!(rerun.is_empty(), "not before the gap");
    let rerun = started(&bench.tick(Duration::from_millis(1)));
    assert_eq!(rerun.len(), 1, "both waiting arrivals run it once");
    assert!(bench.missing(rerun[0], spends.clone()).is_empty());

    // A block mines a parent, which runs the child again and gives it its
    // retries back.
    let rerun = started(&bench.grow(&[&parents[0]]));
    assert_eq!(rerun.len(), 1);
    assert!(bench.missing(rerun[0], spends.clone()).is_empty());
    let rerun = admit_parent(&mut bench, &parents[4]);
    assert_eq!(rerun.len(), 1, "the block gave the child its retries back");
    assert!(bench.missing(rerun[0], spends.clone()).is_empty());

    let step = bench.tick(T_PARK);
    assert_eq!(rejection(&step, waiter).scope, RejectionScope::ExactTip);
    assert_eq!(bench.machine.slots_in_use(), 0);
}

/// A parent that arrives while the child is checked counts as a retry too.
/// One the cap holds back is owed: when the wait ends the child gets one
/// last check instead of the missing inputs it last saw being cached.
#[test]
fn a_parent_the_cap_holds_back_earns_one_last_check() {
    let mut runner = TestRunner::deterministic();
    let (parents, child, spends) = child_of_many(&mut runner, 3);
    let mut bench = Bench::new();
    let (waiter, mut job) = bench.start(&child);
    for parent in &parents[..2] {
        bench.admit(parent);
        let rerun = started(&bench.missing(job, spends.clone()));
        assert_eq!(rerun.len(), 1, "a parent that arrived during the check");
        job = rerun[0];
    }
    bench.admit(&parents[2]);
    assert!(
        bench.missing(job, spends.clone()).is_empty(),
        "the cap holds the third arrival back"
    );

    let step = bench.tick(T_PARK);
    assert!(step.verdict(waiter).is_none());
    let last = started(&step);
    assert_eq!(last.len(), 1, "one last check");
    let step = bench.missing(last[0], vec![coin(0xb0)]);
    assert_eq!(rejection(&step, waiter).scope, RejectionScope::ExactTip);
}

/// A parent that is evicted and enters again can spend the child's retries,
/// but cannot keep its other parent from running it: that arrival runs it
/// within `RETRY_GAP`, well before a peer's deadline.
#[test]
fn a_parent_that_enters_again_cannot_hold_the_child_back() {
    let mut runner = TestRunner::deterministic();
    let parents = unrelated(&mut runner, 2);
    let spends: Vec<OutPoint> = parents.iter().map(|parent| parent.outpoint(0)).collect();
    let child = build(&mut runner, &spends, 1, None);
    let fillers = unrelated(&mut runner, 2);
    // The pool holds two transactions, and evicts the one that pays least.
    let mut bench = Bench::with(
        Config {
            tx_cost_limit: parents[0].real.cost() + fillers[0].real.cost(),
            ..Config::default()
        },
        Limits::default(),
    );
    let (_, job) = bench.start(&child);
    assert!(bench.missing(job, spends.clone()).is_empty());

    let first = paying(&parents[0], 500_000);
    let rerun = admit_parent(&mut bench, &first);
    assert_eq!(rerun.len(), 1, "the first parent runs the child again");
    assert!(bench.missing(rerun[0], spends.clone()).is_empty());

    // Two richer transactions push the first parent out, and it comes back
    // under other authorizing data.
    bench.admit(&paying(&fillers[0], 100_000_000));
    bench.admit(&paying(&fillers[1], 200_000_000));
    assert!(!bench.machine.pooled_ids().contains(&first.id()));
    let again = paying(&twin(&parents[0]), 300_000_000);
    let rerun = admit_parent(&mut bench, &again);
    assert_eq!(rerun.len(), 1, "its return spends the second retry");
    assert!(bench.missing(rerun[0], spends.clone()).is_empty());

    assert!(
        admit_parent(&mut bench, &paying(&parents[1], 400_000_000)).is_empty(),
        "past the cap, the other parent waits"
    );
    let rerun = started(&bench.tick(RETRY_GAP));
    assert_eq!(
        rerun.len(),
        1,
        "the other parent runs the child after the gap"
    );
}

/// A parent that leaves before the check it started ends, and then enters
/// again, runs the child again. The verifier also reports the child's
/// confirmed spend as missing, which is not a parent still to come.
#[test]
fn a_parent_that_left_before_its_check_runs_the_child_when_it_returns() {
    let mut runner = TestRunner::deterministic();
    let parent = build(&mut runner, &[], 1, None);
    let spends = vec![coin(0xc1), parent.outpoint(0)];
    let child = build(&mut runner, &spends, 1, None);
    let fillers = unrelated(&mut runner, 2);
    let mut bench = Bench::with(
        Config {
            tx_cost_limit: parent.real.cost() + fillers[0].real.cost(),
            ..Config::default()
        },
        Limits::default(),
    );
    let (_, job) = bench.start(&child);
    assert!(bench.missing(job, spends.clone()).is_empty());

    let rerun = admit_parent(&mut bench, &paying(&parent, 500_000));
    assert_eq!(rerun.len(), 1);
    // The parent is pushed out before that check looks for it.
    bench.admit(&paying(&fillers[0], 100_000_000));
    bench.admit(&paying(&fillers[1], 200_000_000));
    assert!(bench.missing(rerun[0], spends.clone()).is_empty());

    let again = paying(&twin(&parent), 300_000_000);
    assert_eq!(
        admit_parent(&mut bench, &again).len(),
        1,
        "no check saw the parent, so it runs the child again"
    );
}

/// A parent that leaves before the check it started looks for it, and comes
/// back before that check ends, runs the child again at once.
#[test]
fn a_parent_that_returns_during_its_check_runs_the_child_again() {
    let mut runner = TestRunner::deterministic();
    let parent = build(&mut runner, &[], 1, None);
    let child = build(&mut runner, &[parent.outpoint(0)], 1, None);
    let fillers = unrelated(&mut runner, 2);
    let mut bench = Bench::with(
        Config {
            tx_cost_limit: parent.real.cost() + fillers[0].real.cost(),
            ..Config::default()
        },
        Limits::default(),
    );
    let (_, job) = bench.start(&child);
    assert!(bench.missing(job, vec![parent.outpoint(0)]).is_empty());

    let rerun = admit_parent(&mut bench, &paying(&parent, 500_000));
    assert_eq!(rerun.len(), 1);
    bench.admit(&paying(&fillers[0], 100_000_000));
    bench.admit(&paying(&fillers[1], 200_000_000));
    bench.admit(&paying(&twin(&parent), 300_000_000));
    let step = bench.missing(rerun[0], vec![parent.outpoint(0)]);
    assert_eq!(
        started(&step).len(),
        1,
        "the parent came back during the check"
    );
}

/// Past the cap, the gap runs from when the last check started, not from
/// when its answer came back: a parent that arrived during a slow check
/// still runs the child `RETRY_GAP` after that check began.
#[test]
fn the_retry_gap_runs_from_the_start_of_the_last_check() {
    let mut runner = TestRunner::deterministic();
    let (parents, child, spends) = child_of_many(&mut runner, 3);
    let mut bench = Bench::new();
    let (_, job) = bench.start(&child);
    assert!(bench.missing(job, spends.clone()).is_empty());
    let rerun = admit_parent(&mut bench, &parents[0]);
    assert!(bench.missing(rerun[0], spends.clone()).is_empty());
    let rerun = admit_parent(&mut bench, &parents[1]);

    // The last parent arrives a second into the second retry, whose answer
    // comes back four seconds in.
    bench.now += Duration::from_secs(1);
    bench.admit(&parents[2]);
    bench.now += Duration::from_secs(3);
    assert!(bench.missing(rerun[0], spends.clone()).is_empty());
    let step = bench.tick(RETRY_GAP - Duration::from_secs(4));
    assert_eq!(started(&step).len(), 1, "a gap after the check started");
}

/// A parent held back past the cap whose gap would end after the
/// submitter's deadline spends the child's last check at once. Here the
/// child parks at 15 s with a 73 s deadline, a returning parent spends its
/// second retry at 65 s, and its other parent arrives at 66 s, when the gap
/// would end at 75 s.
#[test]
fn a_held_parent_spends_the_last_check_before_the_deadline() {
    let mut runner = TestRunner::deterministic();
    let parents = unrelated(&mut runner, 2);
    let spends: Vec<OutPoint> = parents.iter().map(|parent| parent.outpoint(0)).collect();
    let child = build(&mut runner, &spends, 1, None);
    let fillers = unrelated(&mut runner, 2);
    let mut bench = Bench::with(
        Config {
            tx_cost_limit: parents[0].real.cost() + fillers[0].real.cost(),
            ..Config::default()
        },
        Limits::default(),
    );
    bench.now = Duration::from_secs(15);
    let reservation = bench.reserved(&child);
    let (_, step) = bench.submit_until(reservation, &child, Some(Duration::from_secs(73)));
    assert!(bench.missing(started(&step)[0], spends.clone()).is_empty());

    bench.now = Duration::from_secs(20);
    let rerun = admit_parent(&mut bench, &paying(&parents[0], 500_000));
    assert!(bench.missing(rerun[0], spends.clone()).is_empty());
    bench.admit(&paying(&fillers[0], 100_000_000));
    bench.admit(&paying(&fillers[1], 200_000_000));
    bench.now = Duration::from_secs(65);
    let rerun = admit_parent(&mut bench, &paying(&twin(&parents[0]), 300_000_000));
    assert!(bench.missing(rerun[0], spends.clone()).is_empty());

    bench.now = Duration::from_secs(66);
    let rerun = admit_parent(&mut bench, &paying(&parents[1], 400_000_000));
    assert_eq!(rerun.len(), 1, "the last check runs before 73 s");
}

// Deadlines.

/// A deadline that has passed is applied before the input that shows it,
/// whether or not a tick came first.
#[test]
fn a_deadline_that_has_passed_is_applied_before_the_input_that_shows_it() {
    let mut runner = TestRunner::deterministic();
    let (parent, child) = family(&mut runner);
    let slow = build(&mut runner, &[], 1, None);
    let mut bench = Bench::new();

    // A pass that arrives after its verification's limit is too late.
    let (waiter, job) = bench.start(&slow);
    bench.now = T_VERIFY;
    let step = bench.pass(job, &slow);
    assert!(matches!(
        step.verdict(waiter),
        Some(Verdict::LocalFault(LocalFault::VerifyTimeout))
    ));
    assert!(bench.machine.pooled_ids().is_empty());

    // A block that mines a parent after the child's wait for it has ended
    // finds the wait already decided.
    let (waiter, job) = bench.start(&child);
    bench.missing(job, vec![parent.outpoint(0)]);
    bench.now += T_PARK;
    let step = bench.grow(&[&parent]);
    assert_eq!(rejection(&step, waiter).scope, RejectionScope::ExactTip);
}

/// The machine's clock never goes back for a caller whose time is stale.
#[test]
fn an_older_time_from_a_caller_does_not_turn_the_clock_back() {
    let mut runner = TestRunner::deterministic();
    let txs = unrelated(&mut runner, 3);
    let mut bench = Bench::new();
    let (waiter, job) = bench.start(&txs[0]);

    // Another input moves time far past this job's deadline, and is the one
    // that finds it due.
    bench.now = T_VERIFY + Duration::from_secs(1);
    let (answer, step) = bench.reserve(&txs[1]);
    assert!(answer.is_ok());
    assert!(matches!(
        step.verdict(waiter),
        Some(Verdict::LocalFault(LocalFault::VerifyTimeout))
    ));

    // The job's outcome comes from a caller whose time is stale. The pass
    // changes nothing.
    bench.now = Duration::from_secs(1);
    assert!(bench.pass(job, &txs[0]).is_empty());
    assert!(bench.machine.pooled_ids().is_empty());

    // A second submission is judged on the machine's clock too: its
    // deadline runs from then, not from the stale time its caller passes.
    let (waiter, job) = bench.start(&txs[2]);
    bench.now = Duration::from_secs(2);
    let step = bench.pass(job, &txs[2]);
    assert!(matches!(step.verdict(waiter), Some(Verdict::Admitted)));
}

/// A parked transaction ends at its submitter's deadline, as a timeout,
/// even though its own wait for a parent runs longer, and caches nothing.
/// Its wait for a parent still wins a tie, with today's cached
/// missing-input answer.
#[test]
fn a_submitters_deadline_ends_a_parked_admission() {
    let mut runner = TestRunner::deterministic();
    let (parent, child) = family(&mut runner);
    let mut bench = Bench::new();

    // The fetch took 15 s, and the adapter stops waiting at 73 s.
    bench.now = Duration::from_secs(15);
    let reservation = bench.reserved(&child);
    let (waiter, step) = bench.submit_until(reservation, &child, Some(Duration::from_secs(73)));
    let job = started(&step)[0];
    assert!(bench.missing(job, vec![parent.outpoint(0)]).is_empty());

    let step = bench.tick(Duration::from_secs(58));
    assert!(matches!(
        step.verdict(waiter),
        Some(Verdict::LocalFault(LocalFault::VerifyTimeout))
    ));
    assert_eq!(step.changes, vec![Change::Refused(child.id())]);
    assert_eq!(bench.machine.slots_in_use(), 0);
    assert!(bench.reserve(&child).0.is_ok(), "a timeout caches nothing");

    // A deadline at the same time as the end of the wait for a parent.
    let mut bench = Bench::new();
    let reservation = bench.reserved(&child);
    let (waiter, step) = bench.submit_until(reservation, &child, Some(T_PARK));
    let job = started(&step)[0];
    bench.missing(job, vec![parent.outpoint(0)]);
    let step = bench.tick(T_PARK);
    assert_eq!(rejection(&step, waiter).scope, RejectionScope::ExactTip);
}

/// A verification ends at its submitter's deadline when that comes before
/// its own limit, and a body that arrives after the deadline is not
/// verified at all.
#[test]
fn a_submitters_deadline_ends_a_verification_and_a_late_body() {
    let mut runner = TestRunner::deterministic();
    let txs = unrelated(&mut runner, 2);
    let mut bench = Bench::new();
    let reservation = bench.reserved(&txs[0]);
    let (waiter, step) = bench.submit_until(reservation, &txs[0], Some(Duration::from_secs(10)));
    let job = started(&step)[0];
    let step = bench.tick(Duration::from_secs(10));
    assert!(matches!(
        step.verdict(waiter),
        Some(Verdict::LocalFault(LocalFault::VerifyTimeout))
    ));
    assert!(step
        .effects
        .iter()
        .any(|effect| matches!(effect, Effect::Cancel(cancelled) if *cancelled == job)));
    assert!(
        bench.pass(job, &txs[0]).is_empty(),
        "the late pass is ignored"
    );

    let reservation = bench.reserved(&txs[1]);
    bench.now = Duration::from_secs(80);
    let (waiter, step) = bench.submit_until(reservation, &txs[1], Some(Duration::from_secs(73)));
    assert!(matches!(
        step.verdict(waiter),
        Some(Verdict::LocalFault(LocalFault::VerifyTimeout))
    ));
    assert!(started(&step).is_empty());
    assert_eq!(bench.machine.slots_in_use(), 0);
}

// Chain events.

/// A block removes what it mined, then what it conflicts with, as today. A
/// conflict is found by the outputs the block spends: a transaction the pool
/// never saw can spend what a pooled one spends.
#[test]
fn a_block_removes_a_pooled_transaction_that_conflicts_with_one_never_pooled() {
    let mut runner = TestRunner::deterministic();
    let spender = build(&mut runner, &[coin(0xe2)], 1, None);
    let child = build(&mut runner, &[spender.outpoint(0)], 1, None);
    let mined = build(&mut runner, &[], 1, None);
    let rival = build(&mut runner, &[coin(0xe2)], 1, None);
    let mut bench = Bench::new();
    bench.admit(&spender);
    bench.admit_using(&child, vec![spender.outpoint(0)]);
    bench.admit(&mined);

    let step = bench.grow(&[&rival, &mined]);
    assert_eq!(
        step.changes,
        vec![
            Change::Removed {
                id: mined.id(),
                reason: RemovalReason::Mined
            },
            Change::Removed {
                id: spender.id(),
                reason: RemovalReason::Conflicted
            },
            Change::Removed {
                id: child.id(),
                reason: RemovalReason::Invalidated
            },
        ]
    );
    assert!(bench.machine.pooled_ids().is_empty());
}

/// A mined parent leaves its child in the pool: the child's input is on the
/// chain now.
#[test]
fn a_mined_parent_leaves_its_child_in_the_pool() {
    let mut runner = TestRunner::deterministic();
    let (parent, child) = family(&mut runner);
    let mut bench = Bench::new();
    bench.admit(&parent);
    bench.admit_using(&child, vec![parent.outpoint(0)]);
    bench.grow(&[&parent]);
    assert_eq!(bench.machine.pooled_ids(), vec![child.id()]);
}

/// A block expires transactions by height. One that expires unmined takes
/// what spends its outputs with it: those outputs will never exist.
#[test]
fn a_block_expires_transactions_by_height() {
    let mut runner = TestRunner::deterministic();
    let parent = build(&mut runner, &[], 1, Some(Height(START + 2)));
    let child = build(&mut runner, &[parent.outpoint(0)], 1, None);
    let mut bench = Bench::new();
    bench.admit(&parent);
    bench.admit_using(&child, vec![parent.outpoint(0)]);

    assert!(bench.grow(&[]).changes.is_empty());
    let step = bench.grow(&[]);
    assert_eq!(
        step.changes,
        vec![
            Change::Removed {
                id: parent.id(),
                reason: RemovalReason::Expired
            },
            Change::Removed {
                id: child.id(),
                reason: RemovalReason::Invalidated
            },
        ]
    );
}

/// The scope a rejection of `tx` is cached under, as its next reservation
/// reports it.
fn cached_scope(bench: &mut Bench, tx: &Tx) -> Option<RejectionScope> {
    match bench.reserve(tx).0 {
        Err(Unreserved::Rejected(rejection)) => Some(rejection.scope),
        Ok(reservation) => {
            bench.release(reservation);
            None
        }
        Err(refused) => {
            panic!("the reservation is granted or refused for a rejection: {refused:?}")
        }
    }
}

/// A tip change forgets the rejections that only held for the old tip, and
/// keeps the ones that hold while the chain grows.
#[test]
fn a_tip_change_forgets_tip_scoped_rejections_only() {
    let mut runner = TestRunner::deterministic();
    let txs = unrelated(&mut runner, 3);
    let mut bench = Bench::new();
    let errors: [RejectionError<TestVerifyError>; 3] = [
        ExactTipRejectionError::FailedVerification(TestVerifyError).into(),
        SameEffectsTipRejectionError::SpendConflict.into(),
        SameEffectsChainRejectionError::DuplicateSpend.into(),
    ];
    let scopes = [
        RejectionScope::ExactTip,
        RejectionScope::EffectsTip,
        RejectionScope::EffectsChain(ChainReason::DuplicateSpend),
    ];
    for ((tx, error), scope) in txs.iter().zip(errors).zip(scopes) {
        let (_, job) = bench.start(tx);
        bench.fail(job, error, Charge::None);
        assert_eq!(cached_scope(&mut bench, tx), Some(scope));
    }

    bench.grow(&[]);
    assert_eq!(cached_scope(&mut bench, &txs[0]), None);
    assert_eq!(cached_scope(&mut bench, &txs[1]), None);
    assert_eq!(cached_scope(&mut bench, &txs[2]), Some(scopes[2]));
}

/// A block caches what it mined, what conflicted with it and what expired,
/// each under the reason today's lists keep, so none is fetched again. A
/// conflict with a later transaction of the block is found though an earlier
/// one already took the parent away.
#[test]
fn a_block_caches_what_it_mined_what_conflicted_and_what_expired() {
    let mut runner = TestRunner::deterministic();
    let spender = build(&mut runner, &[coin(0xe3)], 1, None);
    let child = build(&mut runner, &[spender.outpoint(0), coin(0xe4)], 1, None);
    let expiring = build(&mut runner, &[], 1, Some(Height(START + 1)));
    let mined = build(&mut runner, &[], 1, None);
    let rivals = [
        build(&mut runner, &[coin(0xe3)], 1, None),
        build(&mut runner, &[coin(0xe4)], 1, None),
    ];
    let never_seen = build(&mut runner, &[], 1, None);
    let mut bench = Bench::new();
    bench.admit(&spender);
    bench.admit_using(&child, vec![spender.outpoint(0)]);
    bench.admit(&expiring);
    bench.admit(&mined);

    bench.grow(&[&rivals[0], &rivals[1], &mined]);
    assert!(bench.machine.pooled_ids().is_empty());
    for (tx, reason) in [
        (&spender, ChainReason::DuplicateSpend),
        (&child, ChainReason::DuplicateSpend),
        (&expiring, ChainReason::Expired),
        (&mined, ChainReason::Mined),
        (&rivals[0], ChainReason::Mined),
    ] {
        assert_eq!(
            cached_scope(&mut bench, tx),
            Some(RejectionScope::EffectsChain(reason))
        );
    }
    // The query takes no slot and does not depend on room.
    let rejected = bench.rejected(&[&spender, &mined, &never_seen]);
    assert_eq!(rejected.len(), 2);
    assert!(!rejected.contains(&never_seen.id()));
    assert_eq!(bench.machine.slots_in_use(), 0);
}

/// A reset forgets every cached rejection, the chain-scoped ones too: a
/// rejection made on the old chain or under the old rules says nothing.
#[test]
fn a_reset_forgets_chain_scoped_rejections_too() {
    let mut runner = TestRunner::deterministic();
    let mined = build(&mut runner, &[], 1, None);
    let mut bench = Bench::new();
    bench.grow(&[&mined]);
    assert!(cached_scope(&mut bench, &mined).is_some());
    bench.reset();
    assert_eq!(cached_scope(&mut bench, &mined), None);
}

/// A rejection and a later admission of one id keep their order in the log,
/// so a reader that fell behind still ends on the addition.
#[test]
fn a_rejection_and_a_later_admission_of_one_id_keep_their_order_in_the_log() {
    let mut runner = TestRunner::deterministic();
    let tx = build(&mut runner, &[], 1, None);
    let mut bench = Bench::new();
    let mut log = Vec::new();
    let (_, job) = bench.start(&tx);
    log.extend(
        bench
            .fail(
                job,
                ExactTipRejectionError::FailedVerification(TestVerifyError),
                Charge::None,
            )
            .changes,
    );
    log.extend(bench.grow(&[]).changes);
    let (_, job) = bench.start(&tx);
    log.extend(bench.pass(job, &tx).changes);
    assert_eq!(log, vec![Change::Refused(tx.id()), Change::Added(tx.id())]);
}

// The off switch.

/// Turning the pool off answers every submission, cancels what runs, and
/// empties the pool. Turned on again, it starts empty.
#[test]
fn turning_the_pool_off_answers_every_waiter() {
    let mut runner = TestRunner::deterministic();
    let (parent, child) = family(&mut runner);
    let txs = unrelated(&mut runner, 3);
    let mut bench = Bench::new();
    bench.admit(&txs[0]);
    let (verifying, job) = bench.start(&txs[1]);
    let (parked, parked_job) = bench.start(&child);
    bench.missing(parked_job, vec![parent.outpoint(0)]);

    let step = bench.deactivate();
    for waiter in [verifying, parked] {
        assert!(matches!(
            step.verdict(waiter),
            Some(Verdict::LocalFault(LocalFault::Deactivated))
        ));
    }
    assert!(step
        .effects
        .iter()
        .any(|effect| matches!(effect, Effect::Cancel(cancelled) if *cancelled == job)));
    assert_eq!(
        step.changes,
        vec![Change::Removed {
            id: txs[0].id(),
            reason: RemovalReason::Deactivated
        }]
    );
    assert!(matches!(
        bench.reserve(&txs[2]).0,
        Err(Unreserved::Inactive)
    ));

    bench.activate();
    assert!(bench.machine.pooled_ids().is_empty());
    assert!(bench.reserve(&txs[2]).0.is_ok());
}

// Reorgs and missed events: more of the pool being verified again.

/// A reset checks the pool and every parked transaction again: a block the
/// machine never saw may have mined a parked child's parent, so the child
/// cannot wait for that block.
#[test]
fn a_reset_checks_the_pool_and_every_parked_transaction_again() {
    let mut runner = TestRunner::deterministic();
    let (parent, child) = family(&mut runner);
    let pooled = build(&mut runner, &[], 1, None);
    let mut bench = Bench::new();
    bench.admit(&pooled);
    let (_, job) = bench.start(&child);
    bench.missing(job, vec![parent.outpoint(0)]);

    let step = bench.reset();
    assert_eq!(
        step.changes,
        vec![Change::Removed {
            id: pooled.id(),
            reason: RemovalReason::Reset,
        }]
    );
    let jobs: Vec<UnminedTxId> = started_for(&step).into_iter().map(|(_, id)| id).collect();
    assert_eq!(jobs, vec![child.id(), pooled.id()]);
}

/// After a reset, the pool's transactions take at most their share of the
/// slots, so new transactions still get in while the pool is verified
/// again. One that finishes hands its slot to the next one waiting.
#[test]
fn the_pool_verified_again_leaves_room_for_new_transactions() {
    let mut runner = TestRunner::deterministic();
    let pooled = unrelated(&mut runner, 6);
    let fresh = unrelated(&mut runner, 2);
    let mut bench = five_slots();
    for tx in &pooled {
        bench.admit(tx);
    }

    let jobs = started(&bench.reset());
    assert_eq!(jobs.len(), 4, "the pool's share of the slots");
    assert_eq!(bench.machine.slots_in_use(), 4);
    assert!(bench.reserve(&fresh[0]).0.is_ok());
    assert!(matches!(
        bench.reserve(&fresh[1]).0,
        Err(Unreserved::NoRoom)
    ));

    let step = bench.pass(jobs[0], &pooled[0]);
    assert_eq!(started(&step).len(), 1, "the next one waiting starts");
}
