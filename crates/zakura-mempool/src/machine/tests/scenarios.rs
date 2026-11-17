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
    ChainReason, Change, Charge, Generation, GenerationRead, Intent, LocalFault, Rejection,
    RejectionScope, RemovalReason, Unreserved, Verdict, MAX_UNDECIDED, T_PARK, T_VERIFY,
};

use super::{build, paying, started, twin, verified, Bench, TestVerifyError, Tx, START};
use crate::{
    config::Config,
    machine::{Effect, JobId, Limits, Outcome, Step, WaiterId},
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
    assert_eq!(bench.machine.visible(), vec![pooled.id()]);
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
        matches!(
            bench.reserve(&mined, Intent::Deliver).0,
            Err(Unreserved::Rejected(_))
        ),
        "still remembered at its memory"
    );
    bench.now = memory + Duration::from_millis(1);
    assert!(
        bench.reserve(&mined, Intent::Deliver).0.is_ok(),
        "forgotten after it"
    );
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
    assert_eq!(bench.machine.visible().len(), 2);
}

// Reorgs: the pool is hidden and verified again.

/// While a pooled transaction is hidden, another authorization of the same
/// effects is refused for its effects. The pooled one's revalidation then
/// passes, and it is revealed: the other authorization's rejection does not
/// stand in its way.
#[test]
fn a_passing_revalidation_ignores_a_rejection_of_another_authorization() {
    let mut runner = TestRunner::deterministic();
    let pooled = build(&mut runner, &[], 1, None);
    let other = twin(&pooled);
    let mut bench = Bench::new();
    let (_, first_job) = bench.start(&pooled);
    let (_, second_job) = bench.start(&other);
    bench.pass(first_job, &pooled);

    // A reorg hides the pooled one and starts its revalidation.
    let revalidation = started(&bench.reorg(&[]));
    assert_eq!(
        revalidation.len(),
        1,
        "the hidden transaction is checked again"
    );

    // The other authorization's job reports on the new chain and is refused
    // for its effects, which caches a rejection of its mined id.
    bench.fail(
        second_job,
        SameEffectsTipRejectionError::SpendConflict,
        Charge::None,
    );

    bench.pass(revalidation[0], &pooled);
    assert_eq!(bench.machine.visible(), vec![pooled.id()], "it is revealed");
}

/// Hidden transactions keep counting against the cost limit, so a reorg
/// followed by new admissions cannot hold more than the limit.
#[test]
fn hidden_transactions_count_against_the_cost_limit() {
    let mut runner = TestRunner::deterministic();
    let txs: Vec<Tx> = (0..3).map(|_| build(&mut runner, &[], 1, None)).collect();
    let limit = txs[0].real.cost() + txs[1].real.cost();
    let mut bench = Bench::with(
        Config {
            tx_cost_limit: limit,
            ..Config::default()
        },
        Limits::default(),
    );
    bench.admit(&txs[0]);
    bench.admit(&txs[1]);

    // A reorg hides both. Their bodies are still held.
    let revalidations = started(&bench.reorg(&[]));
    assert_eq!(bench.machine.hidden().len(), 2);

    // A new transaction finds no room: the hidden ones can't be evicted.
    // That is not remembered, since the hidden ones were never weighed
    // against it.
    let (waiter, job) = bench.start(&txs[2]);
    let step = bench.pass(job, &txs[2]);
    assert!(
        matches!(
            step.verdict(waiter),
            Some(Verdict::LocalFault(LocalFault::PoolUnsettled))
        ),
        "the pool is full of hidden transactions, so nothing is decided: {step:?}"
    );
    assert!(
        bench.rejected(&[&txs[2]]).is_empty(),
        "a refusal while hidden transactions take room is not cached"
    );

    // Both pass again and are revealed.
    for (job, tx) in revalidations.into_iter().zip(&txs) {
        bench.pass(job, tx);
    }
    assert_eq!(bench.machine.visible().len(), 2);

    // The same bytes are checked again, against the pool as it is now.
    assert!(
        bench.reserve(&txs[2], Intent::Deliver).0.is_ok(),
        "the earlier refusal does not stand in the way"
    );
}

/// A reveal that storage refuses takes its body's cost off the hidden cost
/// once, while another hidden body still counts. Three children of a mined
/// parent come back after a reorg, the parent is pooled again, and the third
/// child's reveal breaks the package limit.
#[test]
fn a_refused_reveal_takes_its_cost_off_once() {
    let mut runner = TestRunner::deterministic();
    let parent = build(&mut runner, &[], 3, None);
    let children: Vec<Tx> = (0..3)
        .map(|index| build(&mut runner, &[parent.outpoint(index)], 1, None))
        .collect();
    let other = build(&mut runner, &[], 1, None);
    let mut bench = Bench::new();

    // The parent is on the chain, so its children spend it from there.
    for child in children.iter().chain([&other]) {
        bench.admit(child);
    }

    // A reorg takes the parent off the chain and hides the pool.
    let step = bench.reorg(&[]);
    let revalidations: Vec<(JobId, UnminedTxId)> = step
        .effects
        .iter()
        .filter_map(|effect| match effect {
            Effect::Start { job, tx, .. } => Some((*job, tx.id())),
            _ => None,
        })
        .collect();
    assert_eq!(
        revalidations.len(),
        4,
        "every hidden transaction is checked again"
    );

    // The parent comes back into the pool.
    bench.admit(&parent);

    // Each child now spends the pooled parent. The third breaks the package
    // limit, and the other transaction stays hidden throughout. The bench
    // checks the hidden cost after each one.
    for (index, child) in children.iter().enumerate() {
        let job = revalidations
            .iter()
            .find(|(_, id)| *id == child.id())
            .expect("each child has a revalidation")
            .0;
        let index = u32::try_from(index).expect("a small index");
        bench.pass_using(job, child, vec![parent.outpoint(index)]);
    }
    assert_eq!(bench.machine.hidden(), vec![other.id()]);
    assert_eq!(
        bench.machine.visible().len(),
        3,
        "the parent and two children"
    );
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
    assert!(!bench.machine.job_after_reset(job));

    // A reorg lands while the job runs, so its outcome is stale and it runs
    // again, marked.
    let step = bench.reorg(&[]);
    assert!(marked(&step).is_empty());
    // A core that has not started the job yet reads the reset now.
    assert!(
        bench.machine.job_after_reset(job),
        "the job's admission was reset"
    );
    let stale = GenerationRead::Stable(Generation(0));
    let step = bench.outcome_read(
        job,
        Outcome::LocalFault(LocalFault::StateUnavailable),
        stale,
        stale,
    );
    assert_eq!(marked(&step), vec![true], "the rerun is marked");

    let reservation = bench.reserved(&after_reset);
    let (_, step) = bench.submit(reservation, &after_reset);
    assert_eq!(marked(&step), vec![false]);
}

// Slots: the node's limit, from the reservation to the verdict.

/// A bench whose node limit is five slots.
fn five_slots() -> Bench {
    Bench::with(
        Config::default(),
        Limits {
            pending: 5,
            revalidating: 4,
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
    let reservations: Vec<_> = txs[..5]
        .iter()
        .map(|tx| match bench.reserve(tx, Intent::Fetch).0 {
            Ok(grant) => grant.id,
            Err(refused) => panic!("five fit: {refused:?}"),
        })
        .collect();

    // The node is full before one body has been fetched.
    for intent in [Intent::Fetch, Intent::Deliver] {
        assert!(matches!(
            bench.reserve(&txs[5], intent).0,
            Err(Unreserved::NoRoom)
        ));
    }

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
    assert!(matches!(
        bench.reserve(&txs[5], Intent::Fetch).0,
        Err(Unreserved::NoRoom)
    ));

    assert!(bench.release(held[0]).is_empty());
    assert!(bench.reserve(&txs[5], Intent::Fetch).0.is_ok());
    assert_eq!(bench.machine.slots_in_use(), 5);
}

/// An id that is already undecided is not fetched a second time.
#[test]
fn an_undecided_id_is_not_fetched_twice() {
    let mut runner = TestRunner::deterministic();
    let tx = build(&mut runner, &[], 1, None);
    let mut bench = Bench::new();
    assert!(bench.reserve(&tx, Intent::Fetch).0.is_ok());
    assert!(matches!(
        bench.reserve(&tx, Intent::Fetch).0,
        Err(Unreserved::Pending)
    ));
}

/// A fetch that is still out when its admission ends keeps its slot: a
/// decoded body is still on its way. Its caller gets the admission's verdict
/// when it comes back.
#[test]
fn a_fetch_that_is_still_out_keeps_its_slot_after_its_admission_ends() {
    let mut runner = TestRunner::deterministic();
    let txs = unrelated(&mut runner, 6);
    let mut bench = five_slots();
    let fetching: Vec<_> = txs[..5]
        .iter()
        .map(|tx| match bench.reserve(tx, Intent::Fetch).0 {
            Ok(grant) => grant.id,
            Err(refused) => panic!("five fit: {refused:?}"),
        })
        .collect();

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
    assert!(matches!(
        bench.reserve(&txs[5], Intent::Fetch).0,
        Err(Unreserved::NoRoom)
    ));

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
    assert!(bench.reserve(&txs[5], Intent::Fetch).0.is_ok());
}

/// A block that mines an id while it is being fetched tells the fetch to
/// stop. The slot stays counted until the fetch's caller drops it.
#[test]
fn a_block_that_mines_an_id_being_fetched_cancels_the_fetch() {
    let mut runner = TestRunner::deterministic();
    let tx = build(&mut runner, &[], 1, None);
    let mut bench = Bench::new();
    let fetch = match bench.reserve(&tx, Intent::Fetch).0 {
        Ok(grant) => grant.id,
        Err(refused) => panic!("room: {refused:?}"),
    };

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
    assert!(bench.machine.visible().is_empty());
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
    assert!(bench.reserve(&txs[0], Intent::Fetch).0.is_ok());

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
    assert!(bench.machine.visible().is_empty());
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
    assert!(bench.machine.visible().is_empty());

    let step = bench.pass_using(job, &child, vec![parent.outpoint(0)]);
    assert!(step.verdict(waiter).is_none());
    assert!(bench.machine.visible().is_empty());
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
    assert_eq!(bench.machine.visible(), vec![parent.id(), child.id()]);
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

    // Forty seconds in, a missed chain event runs the child again, and the
    // output is still missing.
    bench.now = Duration::from_secs(40);
    let rerun = started(&bench.resync());
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
        bench.reserve(&child, Intent::Fetch).0,
        Err(Unreserved::Rejected(_))
    ));
    assert!(bench.rejected(&[&child]).contains(&child.id()));
    bench.grow(&[]);
    assert!(bench.reserve(&child, Intent::Fetch).0.is_ok());
}

/// A child that spends an output its visible parent does not have is not
/// verified in a loop: nothing changed since its verifier looked, so it
/// waits, and the wait ends.
#[test]
fn a_child_that_spends_an_output_its_visible_parent_lacks_waits_once() {
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

    // Just before the wait ends a missed event runs the child again. The
    // check is still running when the wait ends, and while it runs the
    // second parent arrives. That earns one more check.
    bench.now = Duration::from_secs(59);
    let rerun = started(&bench.resync());
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
    assert!(bench.reserve(&child, Intent::Fetch).0.is_ok());
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
    assert!(bench.machine.visible().is_empty());

    // A block that mines a parent after the child's wait for it has ended
    // finds the wait already decided.
    let (waiter, job) = bench.start(&child);
    bench.missing(job, vec![parent.outpoint(0)]);
    bench.now += T_PARK;
    let step = bench.grow(&[&parent]);
    assert_eq!(rejection(&step, waiter).scope, RejectionScope::ExactTip);
}

/// The deadline that was due first decides, however late the machine gets
/// to it: a parked transaction whose wait ended before its slot's limit is
/// answered for its missing parent either way.
#[test]
fn the_deadline_that_was_due_first_decides() {
    let mut runner = TestRunner::deterministic();
    let (parent, child) = family(&mut runner);
    for late in [T_PARK, MAX_UNDECIDED + Duration::from_secs(1)] {
        let mut bench = Bench::new();
        let (waiter, job) = bench.start(&child);
        bench.missing(job, vec![parent.outpoint(0)]);
        let step = bench.tick(late);
        assert_eq!(rejection(&step, waiter).scope, RejectionScope::ExactTip);
        assert!(matches!(
            bench.reserve(&child, Intent::Fetch).0,
            Err(Unreserved::Rejected(_))
        ));
    }
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
    bench.now = MAX_UNDECIDED + Duration::from_secs(1);
    let (answer, step) = bench.reserve(&txs[1], Intent::Fetch);
    assert!(answer.is_ok());
    assert!(matches!(
        step.verdict(waiter),
        Some(Verdict::LocalFault(LocalFault::VerifyTimeout))
    ));

    // The job's outcome comes from a caller whose time is stale. The pass
    // changes nothing.
    bench.now = Duration::from_secs(1);
    assert!(bench.pass(job, &txs[0]).is_empty());
    assert!(bench.machine.visible().is_empty());

    // A second submission is judged on the machine's clock too: its
    // deadline runs from then, not from the stale time its caller passes.
    let (waiter, job) = bench.start(&txs[2]);
    bench.now = Duration::from_secs(2);
    let step = bench.pass(job, &txs[2]);
    assert!(matches!(step.verdict(waiter), Some(Verdict::Admitted)));
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
    assert!(bench.machine.visible().is_empty());
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
    assert_eq!(bench.machine.visible(), vec![child.id()]);
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
    match bench.reserve(tx, Intent::Fetch).0 {
        Err(Unreserved::Rejected(rejection)) => Some(rejection.scope),
        Ok(grant) => {
            bench.release(grant.id);
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
    assert!(bench.machine.visible().is_empty());
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

/// A reorg and a rules change both forget every cached rejection, the
/// chain-scoped ones too: a rejection made on the old chain or under the old
/// rules says nothing.
#[test]
fn a_reorg_forgets_chain_scoped_rejections_too() {
    let mut runner = TestRunner::deterministic();
    let mined = build(&mut runner, &[], 1, None);
    let mut bench = Bench::new();
    bench.grow(&[&mined]);
    assert!(cached_scope(&mut bench, &mined).is_some());
    bench.reorg(&[]);
    assert_eq!(cached_scope(&mut bench, &mined), None);

    bench.grow(&[&mined]);
    assert!(cached_scope(&mut bench, &mined).is_some());
    bench.transition(0, &[Vec::new()], true);
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
        bench.reserve(&txs[2], Intent::Fetch).0,
        Err(Unreserved::Inactive)
    ));

    bench.activate();
    assert!(bench.machine.visible().is_empty());
    assert!(bench.reserve(&txs[2], Intent::Fetch).0.is_ok());
}

// Reorgs and missed events: more of the pool being verified again.

/// A missed chain event checks the pool and every parked transaction again:
/// a block the machine never saw may have mined a parked child's parent, so
/// the child cannot wait for that event.
#[test]
fn a_missed_chain_event_checks_the_pool_and_every_parked_transaction_again() {
    let mut runner = TestRunner::deterministic();
    let (parent, child) = family(&mut runner);
    let pooled = build(&mut runner, &[], 1, None);
    let mut bench = Bench::new();
    bench.admit(&pooled);
    let (_, job) = bench.start(&child);
    bench.missing(job, vec![parent.outpoint(0)]);

    let step = bench.resync();
    assert_eq!(step.changes, vec![Change::Hidden(pooled.id())]);
    let jobs: Vec<(UnminedTxId, bool)> = step
        .effects
        .iter()
        .filter_map(|effect| match effect {
            Effect::Start {
                tx, revalidation, ..
            } => Some((tx.id(), *revalidation)),
            _ => None,
        })
        .collect();
    assert_eq!(jobs, vec![(child.id(), false), (pooled.id(), true)]);
}

/// Revalidation after a reorg takes at most its share of the slots, so new
/// transactions still get in while the pool is checked again. A finished
/// revalidation hands its place to the next one queued.
#[test]
fn revalidation_leaves_room_for_new_transactions() {
    let mut runner = TestRunner::deterministic();
    let pooled = unrelated(&mut runner, 6);
    let fresh = unrelated(&mut runner, 2);
    let mut bench = five_slots();
    for tx in &pooled {
        bench.admit(tx);
    }

    let revalidations = started(&bench.reorg(&[]));
    assert_eq!(revalidations.len(), 4, "the revalidation share");
    assert_eq!(bench.machine.slots_in_use(), 4);
    assert!(bench.reserve(&fresh[0], Intent::Fetch).0.is_ok());
    assert!(matches!(
        bench.reserve(&fresh[1], Intent::Fetch).0,
        Err(Unreserved::NoRoom)
    ));

    let step = bench.pass(revalidations[0], &pooled[0]);
    assert_eq!(started(&step).len(), 1, "the next queued one starts");
}
