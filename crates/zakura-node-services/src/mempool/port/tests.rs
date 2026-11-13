//! Tests of the port and its model.
//!
//! Each model test is one case the pool core has to cover. Most come from
//! review findings on the M1 and M2 plans, and the test name says which
//! rule it pins.

use std::{net::SocketAddr, time::Duration};

use zakura_chain::{
    block::Height,
    transaction::{self, UnminedTxId},
    transparent::OutPoint,
};

use super::{
    model::{
        Effect, JobId, ModelBlock, ModelTransition, ModelTx, ModelUnreserved, ModelVerdict,
        Outcome, PoolModel, Step, WaiterId, COOLDOWN, REVALIDATE_RETRY, T_PARK, T_VERIFY,
    },
    pool_port, Change, ChangeLog, ChangeSeq, ChangesSince, Charge, Generation, GenerationCell,
    GenerationRead, Intent, LocalFault, NoRoom, PoolCommand, RejectionScope, RemovalReason,
    ReservationId, Source, Supplier, Unreserved, CHANGE_LOG_CAPACITY, MAX_PENDING,
    MAX_PENDING_PER_SOURCE, MAX_REVALIDATING, MAX_WAITERS,
};
use crate::mempool::QueueSource;

fn hash(n: u16) -> transaction::Hash {
    let mut bytes = [0; 32];
    bytes[..2].copy_from_slice(&n.to_le_bytes());
    transaction::Hash(bytes)
}

fn id(n: u16) -> UnminedTxId {
    UnminedTxId::Legacy(hash(n))
}

fn tx(n: u16) -> ModelTx {
    ModelTx::new(id(n))
}

fn legacy(n: u16) -> QueueSource {
    QueueSource::LegacySocket(SocketAddr::from(([10, 0, 0, 1], n)))
}

fn v2(n: u8) -> QueueSource {
    QueueSource::Zakura(vec![n])
}

fn peer(n: u16) -> Source {
    Source::Peer(legacy(n))
}

fn stable(generation: u64) -> GenerationRead {
    GenerationRead::Stable(Generation(generation))
}

fn secs(seconds: u64) -> Duration {
    Duration::from_secs(seconds)
}

fn new_model() -> PoolModel {
    PoolModel::new(Height(100), Generation(0))
}

fn rejected(scope: RejectionScope, charge: Charge) -> ModelVerdict {
    ModelVerdict::Rejected { scope, charge }
}

fn the_job(step: &Step) -> JobId {
    let started = step.started();
    assert_eq!(started.len(), 1, "exactly one job starts: {step:?}");
    started[0]
}

/// Reserves, then submits `tx` from one peer that is both source and
/// supplier. Returns the waiter and the job that started.
fn bring_in(model: &mut PoolModel, tx: ModelTx, from: QueueSource) -> (WaiterId, JobId) {
    let reservation = model
        .reserve(tx.id, Source::Peer(from.clone()), Intent::Fetch, secs(0))
        .expect("the test has room");
    let (waiter, step) = model.submit(reservation.id, tx, Supplier::Peer(from), secs(0));
    (waiter, the_job(&step))
}

/// Admits `tx` at generation `generation`.
fn admit(model: &mut PoolModel, tx: ModelTx, from: u16, generation: u64) {
    let (waiter, job) = bring_in(model, tx, legacy(from));
    let step = model.outcome(
        job,
        Outcome::Pass,
        stable(generation),
        stable(generation),
        secs(0),
    );
    assert_eq!(step.verdict(waiter), Some(ModelVerdict::Admitted));
}

fn transition(generation: u64, tip: u32, blocks: Vec<Vec<ModelTx>>) -> ModelTransition {
    ModelTransition {
        disconnected: 0,
        connected: blocks
            .into_iter()
            .map(|transactions| ModelBlock { transactions })
            .collect(),
        tip: Height(tip),
        generation: Generation(generation),
    }
}

fn reorg(generation: u64, tip: u32) -> ModelTransition {
    ModelTransition {
        disconnected: 1,
        connected: vec![ModelBlock::default()],
        tip: Height(tip),
        generation: Generation(generation),
    }
}

// The port's plain data.

#[test]
fn generation_reads_changing_from_begin_until_finish() {
    let cell = GenerationCell::new();
    assert_eq!(cell.read(), stable(0));

    cell.begin_change();
    assert_eq!(cell.read(), GenerationRead::Changing);

    assert_eq!(cell.finish_change(), Generation(1));
    assert_eq!(cell.read(), stable(1));
}

#[test]
fn only_two_equal_stable_reads_of_the_applied_generation_bracket_work() {
    let applied = Generation(3);
    assert!(stable(3).brackets(stable(3), applied));
    // A change became visible during the work.
    assert!(!stable(3).brackets(stable(4), applied));
    assert!(!stable(3).brackets(GenerationRead::Changing, applied));
    assert!(!GenerationRead::Changing.brackets(GenerationRead::Changing, applied));
    // The readers were already ahead of what the core has applied.
    assert!(!stable(4).brackets(stable(4), applied));
}

#[test]
fn change_log_numbers_entries_in_order_across_batches() {
    let mut log = ChangeLog::default();
    assert_eq!(log.since(ChangeSeq(0)), ChangesSince::Entries(Vec::new()));

    assert_eq!(log.append([Change::Refused(id(1))]), ChangeSeq(1));
    // An empty batch moves no cursor.
    assert_eq!(log.append([]), ChangeSeq(1));
    assert_eq!(
        log.append([Change::Added(id(1)), Change::Added(id(2))]),
        ChangeSeq(3)
    );

    let ChangesSince::Entries(entries) = log.since(ChangeSeq(1)) else {
        panic!("a reader one batch behind is still on the log");
    };
    assert_eq!(
        entries
            .iter()
            .map(|entry| (entry.seq, entry.batch, entry.change))
            .collect::<Vec<_>>(),
        vec![
            (ChangeSeq(2), 3, Change::Added(id(1))),
            (ChangeSeq(3), 3, Change::Added(id(2))),
        ]
    );
}

#[test]
fn change_log_resyncs_a_reader_that_fell_off() {
    let mut log = ChangeLog::default();
    let total = u16::try_from(CHANGE_LOG_CAPACITY + 2).expect("the capacity fits the test's ids");
    for n in 0..total {
        log.append([Change::Added(id(n))]);
    }
    // Entries 1 and 2 are gone. A reader at 2 needs entry 3, which is kept.
    assert_eq!(log.since(ChangeSeq(1)), ChangesSince::Resync);
    assert!(
        matches!(log.since(ChangeSeq(2)), ChangesSince::Entries(entries) if entries.len() == CHANGE_LOG_CAPACITY)
    );
}

#[tokio::test]
async fn a_dropped_reservation_reaches_the_core() {
    let (handle, mut inbox) = pool_port();
    let core = tokio::spawn(async move {
        let Some(PoolCommand::Reserve { tx_id, reply, .. }) = inbox.command().await else {
            panic!("the first command is the reservation");
        };
        let _ = reply.send(Ok(inbox.grant(ReservationId(7), tx_id, false)));
        inbox
    });

    let reservation = handle
        .reserve(id(1), Source::Node, Intent::Fetch)
        .await
        .expect("the test core grants it");
    let mut inbox = core.await.expect("the test core does not panic");
    assert_eq!(inbox.released(), None);

    drop(reservation);
    assert_eq!(inbox.released(), Some(ReservationId(7)));
}

#[tokio::test]
async fn a_stopped_core_answers_every_call() {
    let (handle, inbox) = pool_port();
    drop(inbox);
    assert!(matches!(
        handle.reserve(id(1), Source::Node, Intent::Fetch).await,
        Err(Unreserved::Stopped)
    ));
    assert!(handle.snapshot().is_none());
}

// The model: reservations and limits.

#[test]
fn a_slot_is_held_from_the_reservation_so_bodies_stay_within_the_node_limit() {
    let mut model = new_model();
    let peers = u16::try_from(MAX_PENDING / MAX_PENDING_PER_SOURCE).expect("100 peers");
    let mut reservations = Vec::new();
    let mut n = 0;
    for p in 0..peers {
        for _ in 0..MAX_PENDING_PER_SOURCE {
            let reservation = model
                .reserve(id(n), peer(p), Intent::Fetch, secs(0))
                .expect("each peer has five slots");
            reservations.push((reservation.id, n, p));
            n += 1;
        }
    }
    // The node is full before one body has been fetched.
    assert_eq!(model.bodies_held(), 0);
    assert_eq!(
        model.reserve(id(n), peer(peers), Intent::Fetch, secs(0)),
        Err(ModelUnreserved::NoRoom(NoRoom::Node))
    );
    assert_eq!(
        model.reserve(id(n), Source::Node, Intent::Deliver, secs(0)),
        Err(ModelUnreserved::NoRoom(NoRoom::Node))
    );

    // Every fetch completes. No body exists outside a reservation.
    for (reservation, n, p) in reservations {
        model.submit(reservation, tx(n), Supplier::Peer(legacy(p)), secs(0));
    }
    assert_eq!(model.bodies_held(), MAX_PENDING);
    model.assert_invariants();
}

#[test]
fn a_released_reservation_frees_its_slot() {
    let mut model = new_model();
    let mut held = Vec::new();
    for n in 0..5 {
        held.push(
            model
                .reserve(id(n), peer(1), Intent::Fetch, secs(0))
                .expect("five fit"),
        );
    }
    assert_eq!(
        model.reserve(id(9), peer(1), Intent::Fetch, secs(0)),
        Err(ModelUnreserved::NoRoom(NoRoom::Source))
    );

    // The fetch of id 0 failed, so its caller dropped the reservation.
    model.release(held[0].id, secs(0));
    assert!(model
        .reserve(id(9), peer(1), Intent::Fetch, secs(0))
        .is_ok());
    assert_eq!(model.counted_against(&peer(1)), 5);
    assert_eq!(model.slots_in_use(), 5);
}

#[test]
fn a_v2_peer_on_the_adapter_is_counted_and_never_charged() {
    let mut model = new_model();
    let source = Source::Peer(v2(1));
    let mut first = None;
    for n in 0..5 {
        let reservation = model
            .reserve(id(n), source.clone(), Intent::Deliver, secs(0))
            .expect("five pushed bodies fit");
        // The adapter does not record who served a body.
        let (waiter, step) = model.submit(reservation.id, tx(n), Supplier::Nobody, secs(0));
        first.get_or_insert((waiter, the_job(&step)));
    }
    // The sixth body from the same peer has no room, as today.
    assert_eq!(
        model.reserve(id(5), source, Intent::Deliver, secs(0)),
        Err(ModelUnreserved::NoRoom(NoRoom::Source))
    );

    let (waiter, job) = first.expect("five were submitted");
    let step = model.outcome(
        job,
        Outcome::Fail {
            scope: RejectionScope::ExactTip,
            charge: Charge::Ban,
        },
        stable(0),
        stable(0),
        secs(1),
    );
    assert_eq!(
        step.verdict(waiter),
        Some(rejected(RejectionScope::ExactTip, Charge::None))
    );
}

// The model: one reservation, several suppliers.

#[test]
fn a_second_peer_can_answer_without_moving_the_reservation() {
    let mut model = new_model();
    let (alice, bob) = (legacy(1), legacy(2));
    // Bob already has five undecided transactions of his own.
    for n in 10..15 {
        model
            .reserve(id(n), Source::Peer(bob.clone()), Intent::Fetch, secs(0))
            .expect("five fit");
    }

    // Alice is asked first and goes silent. Bob is asked next and answers.
    let reservation = model
        .reserve(id(1), Source::Peer(alice.clone()), Intent::Fetch, secs(0))
        .expect("alice has room");
    let (waiter, step) = model.submit(reservation.id, tx(1), Supplier::Peer(bob.clone()), secs(9));
    let job = the_job(&step);

    // The slot is still Alice's. Bob stays at his five.
    assert_eq!(model.counted_against(&Source::Peer(alice)), 1);
    assert_eq!(model.counted_against(&Source::Peer(bob)), 5);
    model.assert_invariants();

    // The bytes were Bob's, so the charge is Bob's.
    let step = model.outcome(
        job,
        Outcome::Fail {
            scope: RejectionScope::ExactTip,
            charge: Charge::Ban,
        },
        stable(0),
        stable(0),
        secs(10),
    );
    assert_eq!(
        step.verdict(waiter),
        Some(rejected(RejectionScope::ExactTip, Charge::Ban))
    );
}

#[test]
fn a_body_that_arrives_after_the_verdict_is_answered_and_not_charged() {
    let mut model = new_model();
    // tx-relay reserved id 1 and is still waiting for its body.
    let relay = model
        .reserve(id(1), Source::Peer(v2(1)), Intent::Fetch, secs(0))
        .expect("room");
    // A legacy peer pushes the same transaction.
    let pushed = model
        .reserve(id(1), peer(2), Intent::Deliver, secs(1))
        .expect("a delivery joins an undecided id");
    assert!(pushed.joined);
    let (pusher, step) = model.submit(pushed.id, tx(1), Supplier::Peer(legacy(2)), secs(1));
    let job = the_job(&step);
    // The pushed body verifies under the relay's slot. No second slot.
    assert_eq!(model.slots_in_use(), 1);

    let step = model.outcome(job, Outcome::Pass, stable(0), stable(0), secs(2));
    assert_eq!(step.verdict(pusher), Some(ModelVerdict::Admitted));
    assert_eq!(model.slots_in_use(), 0);

    // The relay's own answer arrives late. It gets a verdict and no charge.
    let (late, step) = model.submit(relay.id, tx(1), Supplier::Peer(v2(1)), secs(3));
    assert_eq!(
        step.verdict(late),
        Some(rejected(RejectionScope::Uncached, Charge::None))
    );
    assert!(step.started().is_empty());
}

#[test]
fn a_stale_reservation_for_an_unknown_id_is_a_local_fault() {
    let mut model = new_model();
    let (waiter, job) = bring_in(&mut model, tx(1), legacy(1));
    let late = model
        .reserve(id(1), peer(2), Intent::Deliver, secs(0))
        .expect("joins");
    let step = model.outcome(
        job,
        Outcome::LocalFault(LocalFault::StateUnavailable),
        stable(0),
        stable(0),
        secs(1),
    );
    assert_eq!(
        step.verdict(waiter),
        Some(ModelVerdict::LocalFault(LocalFault::StateUnavailable))
    );

    // Nothing is cached after a local fault, so the late body cannot be
    // answered from a cache. The caller reserves again.
    let (waiter, step) = model.submit(late.id, tx(1), Supplier::Peer(legacy(2)), secs(2));
    assert_eq!(
        step.verdict(waiter),
        Some(ModelVerdict::LocalFault(LocalFault::StaleReservation))
    );
    assert!(model
        .reserve(id(1), peer(2), Intent::Deliver, secs(2))
        .is_ok());
}

#[test]
fn an_undecided_id_is_not_fetched_twice() {
    let mut model = new_model();
    model
        .reserve(id(1), peer(1), Intent::Fetch, secs(0))
        .expect("room");
    assert_eq!(
        model.reserve(id(1), peer(2), Intent::Fetch, secs(0)),
        Err(ModelUnreserved::Pending)
    );
}

#[test]
fn joined_submissions_share_one_verification_and_each_supplier_is_charged() {
    let mut model = new_model();
    let (first, job) = bring_in(&mut model, tx(1), legacy(1));
    let joined = model
        .reserve(id(1), peer(2), Intent::Deliver, secs(0))
        .expect("joins");
    let (second, step) = model.submit(joined.id, tx(1), Supplier::Peer(legacy(2)), secs(0));
    assert!(step.started().is_empty(), "no second verification");
    let local = model
        .reserve(id(1), Source::Node, Intent::Deliver, secs(0))
        .expect("joins");
    let (rpc, _) = model.submit(local.id, tx(1), Supplier::Nobody, secs(0));
    assert_eq!(model.slots_in_use(), 1, "a joined submission takes no slot");

    let step = model.outcome(
        job,
        Outcome::Fail {
            scope: RejectionScope::EffectsChain,
            charge: Charge::Cooldown,
        },
        stable(0),
        stable(0),
        secs(1),
    );
    let charged = rejected(RejectionScope::EffectsChain, Charge::Cooldown);
    assert_eq!(step.verdict(first), Some(charged));
    assert_eq!(step.verdict(second), Some(charged));
    assert_eq!(
        step.verdict(rpc),
        Some(rejected(RejectionScope::EffectsChain, Charge::None))
    );
    assert_eq!(step.changes, vec![Change::Refused(id(1))]);

    // Both peer suppliers are cooling down. The cached rejection answers
    // everyone else.
    assert_eq!(
        model.reserve(id(2), peer(1), Intent::Fetch, secs(1)),
        Err(ModelUnreserved::CoolingDown)
    );
    assert_eq!(
        model.reserve(id(2), peer(2), Intent::Fetch, secs(1)),
        Err(ModelUnreserved::CoolingDown)
    );
    assert_eq!(
        model.reserve(id(1), peer(3), Intent::Fetch, secs(1)),
        Err(ModelUnreserved::Rejected(RejectionScope::EffectsChain))
    );
}

#[test]
fn one_id_takes_a_bounded_number_of_waiters_and_one_per_peer() {
    let mut model = new_model();
    bring_in(&mut model, tx(1), legacy(0));
    // The same peer cannot wait on the id twice.
    assert_eq!(
        model.reserve(id(1), peer(0), Intent::Deliver, secs(0)),
        Err(ModelUnreserved::Pending)
    );
    for p in 1..u16::try_from(MAX_WAITERS).expect("small") {
        let joined = model
            .reserve(id(1), peer(p), Intent::Deliver, secs(0))
            .expect("the waiter limit is not reached");
        model.submit(joined.id, tx(1), Supplier::Peer(legacy(p)), secs(0));
    }
    assert_eq!(
        model.reserve(id(1), peer(99), Intent::Deliver, secs(0)),
        Err(ModelUnreserved::Pending)
    );
    model.assert_invariants();
}

// The model: verification and the chain.

#[test]
fn an_outcome_that_saw_a_chain_change_gives_no_verdict_and_runs_again() {
    let mut model = new_model();
    let (waiter, job) = bring_in(&mut model, tx(1), legacy(1));

    // State advanced during the verification. The outcome is a failure, but
    // it may be a failure against a mix of two chains.
    let step = model.outcome(
        job,
        Outcome::Fail {
            scope: RejectionScope::EffectsChain,
            charge: Charge::Ban,
        },
        stable(0),
        stable(1),
        secs(1),
    );
    assert_eq!(step, Step::default(), "no verdict, no cache, no charge");

    // The job runs again once the core has applied that change.
    let step = model.transition(transition(1, 101, vec![vec![]]), secs(2));
    let job = the_job(&step);
    let step = model.outcome(job, Outcome::Pass, stable(1), stable(1), secs(3));
    assert_eq!(step.verdict(waiter), Some(ModelVerdict::Admitted));
}

#[test]
fn a_chain_that_goes_away_and_comes_back_is_still_a_change() {
    let mut model = new_model();
    let (waiter, job) = bring_in(&mut model, tx(1), legacy(1));

    // Tip A, then B, then A again. The hash is the same, the generation is
    // not.
    let step = model.outcome(job, Outcome::Pass, stable(0), stable(2), secs(1));
    assert_eq!(step, Step::default());
    assert!(model.visible().is_empty());

    let step = model.transition(reorg(1, 100), secs(2));
    let job = the_job(&step);
    // That run also finishes before the core has caught up.
    let step = model.outcome(job, Outcome::Pass, stable(2), stable(2), secs(3));
    assert_eq!(step, Step::default());

    let step = model.transition(reorg(2, 100), secs(4));
    let job = the_job(&step);
    let step = model.outcome(job, Outcome::Pass, stable(2), stable(2), secs(5));
    assert_eq!(step.verdict(waiter), Some(ModelVerdict::Admitted));
}

#[test]
fn a_read_taken_while_state_is_changing_is_never_used() {
    let mut model = new_model();
    let (_, job) = bring_in(&mut model, tx(1), legacy(1));
    let step = model.outcome(
        job,
        Outcome::Pass,
        GenerationRead::Changing,
        GenerationRead::Changing,
        secs(1),
    );
    assert_eq!(step, Step::default());
}

#[test]
fn an_old_outcome_that_the_core_is_already_past_runs_again_at_once() {
    let mut model = new_model();
    let (_, job) = bring_in(&mut model, tx(1), legacy(1));
    model.transition(transition(1, 101, vec![vec![]]), secs(1));
    // The worker read generation 0 both times, before the change.
    let step = model.outcome(job, Outcome::Pass, stable(0), stable(0), secs(2));
    assert_eq!(step.started().len(), 1);
    assert!(step.changes.is_empty());
}

#[test]
fn a_verification_that_runs_too_long_is_a_local_fault() {
    let mut model = new_model();
    let (waiter, job) = bring_in(&mut model, tx(1), legacy(1));
    let step = model.tick(T_VERIFY);
    assert_eq!(
        step.verdict(waiter),
        Some(ModelVerdict::LocalFault(LocalFault::VerifyTimeout))
    );
    assert!(step.effects.contains(&Effect::Cancel(job)));
    assert_eq!(model.slots_in_use(), 0);

    // Its late outcome is ignored.
    let step = model.outcome(job, Outcome::Pass, stable(0), stable(0), T_VERIFY + secs(1));
    assert_eq!(step, Step::default());
    assert!(model.visible().is_empty());
}

// The model: parents.

fn child_of(n: u16, parent: u16) -> ModelTx {
    tx(n).spending(hash(parent), 0)
}

fn missing(parent: u16) -> Outcome {
    Outcome::MissingInputs(vec![OutPoint {
        hash: hash(parent),
        index: 0,
    }])
}

#[test]
fn a_parked_child_runs_again_when_its_parent_is_admitted() {
    let mut model = new_model();
    let (child, job) = bring_in(&mut model, child_of(2, 1), legacy(1));
    let step = model.outcome(job, missing(1), stable(0), stable(0), secs(1));
    assert_eq!(step, Step::default());
    assert!(model.is_parked(id(2)));
    // A parked transaction still holds its slot.
    assert_eq!(model.slots_in_use(), 1);

    let (_, parent_job) = bring_in(&mut model, tx(1), legacy(2));
    let step = model.outcome(parent_job, Outcome::Pass, stable(0), stable(0), secs(2));
    let job = the_job(&step);
    let step = model.outcome(job, Outcome::Pass, stable(0), stable(0), secs(3));
    assert_eq!(step.verdict(child), Some(ModelVerdict::Admitted));
    assert_eq!(model.visible(), vec![id(1), id(2)]);
}

#[test]
fn a_parent_admitted_between_the_lookup_and_the_parking_does_not_strand_the_child() {
    let mut model = new_model();
    let (_, child_job) = bring_in(&mut model, child_of(2, 1), legacy(1));
    let (_, parent_job) = bring_in(&mut model, tx(1), legacy(2));

    // The child's verifier looked first and found nothing. The parent is
    // admitted before the core handles the child's outcome.
    model.outcome(parent_job, Outcome::Pass, stable(0), stable(0), secs(1));
    let step = model.outcome(child_job, missing(1), stable(0), stable(0), secs(1));

    // Parking now would wait for an admission that has already happened.
    assert!(!model.is_parked(id(2)));
    assert_eq!(step.started().len(), 1);
}

#[test]
fn a_parked_child_runs_again_when_a_block_mines_its_parent() {
    let mut model = new_model();
    let (_, job) = bring_in(&mut model, child_of(2, 1), legacy(1));
    model.outcome(job, missing(1), stable(0), stable(0), secs(1));

    let step = model.transition(transition(1, 101, vec![vec![tx(1)]]), secs(2));
    assert_eq!(step.started().len(), 1);
    assert!(!model.is_parked(id(2)));
}

#[test]
fn a_child_parked_on_a_hidden_parent_runs_again_when_the_parent_passes() {
    let mut model = new_model();
    admit(&mut model, tx(1), 1, 0);
    let step = model.transition(reorg(1, 100), secs(1));
    assert_eq!(step.changes, vec![Change::Hidden(id(1))]);
    let parent_job = the_job(&step);

    // The parent is in the pool but hidden, so the verifier cannot see its
    // output.
    let (child, job) = bring_in(&mut model, child_of(2, 1), legacy(2));
    model.outcome(job, missing(1), stable(1), stable(1), secs(2));
    assert!(model.is_parked(id(2)));

    // The parent was admitted long ago. Passing again is what wakes the
    // child.
    let step = model.outcome(parent_job, Outcome::Pass, stable(1), stable(1), secs(3));
    assert_eq!(step.changes, vec![Change::Revealed(id(1))]);
    let job = the_job(&step);
    let step = model.outcome(job, Outcome::Pass, stable(1), stable(1), secs(4));
    assert_eq!(step.verdict(child), Some(ModelVerdict::Admitted));
}

#[test]
fn a_child_of_a_hidden_parent_that_fails_is_rejected_for_an_unresolved_parent() {
    let mut model = new_model();
    admit(&mut model, tx(1), 1, 0);
    let step = model.transition(reorg(1, 100), secs(0));
    let parent_job = the_job(&step);
    let (child, job) = bring_in(&mut model, child_of(2, 1), legacy(2));
    model.outcome(job, missing(1), stable(1), stable(1), secs(0));

    // While the parent is being verified again, the child's clock is
    // stopped.
    assert_eq!(model.tick(secs(50)), Step::default());
    let step = model.outcome(
        parent_job,
        Outcome::Fail {
            scope: RejectionScope::EffectsChain,
            charge: Charge::Ban,
        },
        stable(1),
        stable(1),
        secs(50),
    );
    // A pooled transaction that fails again charges nobody.
    assert_eq!(
        step.changes,
        vec![Change::Removed {
            id: id(1),
            reason: RemovalReason::Invalidated
        }]
    );
    assert!(step.effects.is_empty());

    // Now nobody is bringing the parent in, so the clock runs.
    assert_eq!(model.tick(secs(50) + T_PARK - secs(1)), Step::default());
    let step = model.tick(secs(50) + T_PARK);
    assert_eq!(
        step.verdict(child),
        Some(rejected(RejectionScope::Uncached, Charge::None))
    );
    assert_eq!(step.changes, vec![Change::Refused(id(2))]);
}

#[test]
fn the_park_clock_stops_while_the_parent_is_being_verified() {
    let mut model = new_model();
    let (child, job) = bring_in(&mut model, child_of(2, 1), legacy(1));
    model.outcome(job, missing(1), stable(0), stable(0), secs(0));

    // Thirty seconds with nobody bringing the parent in.
    let reservation = model
        .reserve(id(1), peer(2), Intent::Fetch, secs(30))
        .expect("room");
    // A reservation is not a body. The clock still runs until the body is
    // here.
    model.submit(reservation.id, tx(1), Supplier::Peer(legacy(2)), secs(40));

    // The parent's verification never finishes.
    let step = model.tick(secs(40) + T_VERIFY);
    assert_eq!(step.verdict(child), None);
    assert!(model.is_parked(id(2)));

    // Forty seconds had passed before the body arrived. Twenty more remain.
    let step = model.tick(secs(40) + T_VERIFY + secs(19));
    assert_eq!(step.verdict(child), None);
    let step = model.tick(secs(40) + T_VERIFY + secs(20));
    assert_eq!(
        step.verdict(child),
        Some(rejected(RejectionScope::Uncached, Charge::None))
    );
}

// The model: chain events and the pool.

#[test]
fn a_block_removes_a_pooled_transaction_that_conflicts_with_one_never_pooled() {
    let mut model = new_model();
    let coin = hash(900);
    admit(&mut model, tx(1).spending(coin, 0), 1, 0);
    admit(&mut model, child_of(2, 1), 2, 0);
    admit(&mut model, tx(3), 3, 0);

    // The block mines transaction 7, which the pool never saw. It spends the
    // same output as transaction 1. Its id alone would not show that.
    let block = vec![tx(7).spending(coin, 0), tx(3)];
    let step = model.transition(transition(1, 101, vec![block]), secs(1));
    assert_eq!(
        step.changes,
        vec![
            Change::Removed {
                id: id(1),
                reason: RemovalReason::Conflicted
            },
            Change::Removed {
                id: id(2),
                reason: RemovalReason::Invalidated
            },
            Change::Removed {
                id: id(3),
                reason: RemovalReason::Mined
            },
        ]
    );
    assert!(model.visible().is_empty());
    model.assert_invariants();
}

#[test]
fn a_mined_parent_leaves_its_child_in_the_pool() {
    let mut model = new_model();
    admit(&mut model, tx(1), 1, 0);
    admit(&mut model, child_of(2, 1), 2, 0);
    model.transition(transition(1, 101, vec![vec![tx(1)]]), secs(1));
    assert_eq!(model.visible(), vec![id(2)]);
}

#[test]
fn a_block_expires_transactions_by_height() {
    let mut model = new_model();
    let mut expiring = tx(1);
    expiring.expiry = Some(Height(102));
    admit(&mut model, expiring, 1, 0);

    let step = model.transition(transition(1, 101, vec![vec![]]), secs(1));
    assert!(step.changes.is_empty());
    let step = model.transition(transition(2, 102, vec![vec![]]), secs(2));
    assert_eq!(
        step.changes,
        vec![Change::Removed {
            id: id(1),
            reason: RemovalReason::Expired
        }]
    );
}

#[test]
fn a_tip_change_forgets_tip_scoped_rejections_only() {
    let mut model = new_model();
    for (n, scope) in [
        (1, RejectionScope::ExactTip),
        (2, RejectionScope::EffectsTip),
        (3, RejectionScope::EffectsChain),
    ] {
        let (_, job) = bring_in(&mut model, tx(n), legacy(n));
        model.outcome(
            job,
            Outcome::Fail {
                scope,
                charge: Charge::None,
            },
            stable(0),
            stable(0),
            secs(0),
        );
        assert_eq!(
            model.reserve(id(n), peer(50), Intent::Fetch, secs(0)),
            Err(ModelUnreserved::Rejected(scope))
        );
    }

    model.transition(transition(1, 101, vec![vec![]]), secs(1));
    assert!(model
        .reserve(id(1), peer(50), Intent::Fetch, secs(1))
        .is_ok());
    assert!(model
        .reserve(id(2), peer(50), Intent::Fetch, secs(1))
        .is_ok());
    assert_eq!(
        model.reserve(id(3), peer(50), Intent::Fetch, secs(1)),
        Err(ModelUnreserved::Rejected(RejectionScope::EffectsChain))
    );
}

#[test]
fn a_reorg_keeps_the_pool_and_verifies_it_again_parents_first() {
    let mut model = new_model();
    admit(&mut model, tx(1), 1, 0);
    admit(&mut model, child_of(2, 1), 2, 0);

    let step = model.transition(reorg(1, 100), secs(1));
    assert_eq!(
        step.changes,
        vec![Change::Hidden(id(1)), Change::Hidden(id(2))]
    );
    assert!(model.visible().is_empty());
    // Only the parent starts. The child waits for it.
    assert_eq!(
        step.effects,
        vec![Effect::Start {
            job: the_job(&step),
            tx: id(1),
            revalidation: true
        }]
    );

    let step = model.outcome(the_job(&step), Outcome::Pass, stable(1), stable(1), secs(2));
    assert_eq!(step.changes, vec![Change::Revealed(id(1))]);
    let step = model.outcome(the_job(&step), Outcome::Pass, stable(1), stable(1), secs(3));
    assert_eq!(step.changes, vec![Change::Revealed(id(2))]);
    // pool/survives-reorg: both are still valid, and both are back.
    assert_eq!(model.visible(), vec![id(1), id(2)]);
}

#[test]
fn a_revalidation_that_hits_a_local_fault_keeps_the_transaction_and_tries_again() {
    let mut model = new_model();
    admit(&mut model, tx(1), 1, 0);
    let step = model.transition(reorg(1, 100), secs(0));
    let fault = Outcome::LocalFault(LocalFault::StateUnavailable);

    let step = model.outcome(the_job(&step), fault.clone(), stable(1), stable(1), secs(0));
    assert_eq!(step, Step::default(), "not removed, nobody charged");
    assert_eq!(model.hidden(), vec![id(1)]);

    // The wait doubles after each fault.
    assert_eq!(model.tick(REVALIDATE_RETRY - secs(1)), Step::default());
    let step = model.tick(REVALIDATE_RETRY);
    let retried_at = REVALIDATE_RETRY;
    let step = model.outcome(the_job(&step), fault, stable(1), stable(1), retried_at);
    assert_eq!(step, Step::default());
    assert_eq!(
        model.tick(retried_at + REVALIDATE_RETRY * 2 - secs(1)),
        Step::default()
    );
    let step = model.tick(retried_at + REVALIDATE_RETRY * 2);

    let step = model.outcome(
        the_job(&step),
        Outcome::Pass,
        stable(1),
        stable(1),
        secs(10),
    );
    assert_eq!(step.changes, vec![Change::Revealed(id(1))]);
}

#[test]
fn a_missed_chain_event_checks_the_pool_and_every_parked_transaction_again() {
    let mut model = new_model();
    admit(&mut model, tx(1), 1, 0);
    let (_, job) = bring_in(&mut model, child_of(3, 2), legacy(2));
    model.outcome(job, missing(2), stable(0), stable(0), secs(0));
    assert!(model.is_parked(id(3)));

    // The queue overflowed. One of the blocks the core never saw may have
    // mined transaction 2, so the parked child cannot wait for that event.
    let step = model.resync(Height(240), Generation(5), secs(1));
    assert_eq!(step.changes, vec![Change::Hidden(id(1))]);
    assert_eq!(
        step.effects,
        vec![
            Effect::Start {
                job: step.started()[0],
                tx: id(3),
                revalidation: false
            },
            Effect::Start {
                job: step.started()[1],
                tx: id(1),
                revalidation: true
            },
        ]
    );
}

#[test]
fn revalidation_leaves_room_for_new_transactions() {
    let mut model = new_model();
    let pooled = u16::try_from(MAX_REVALIDATING + 50).expect("small");
    for n in 0..pooled {
        admit(&mut model, tx(n), n, 0);
    }

    let step = model.transition(reorg(1, 100), secs(1));
    assert_eq!(step.started().len(), MAX_REVALIDATING);
    assert_eq!(model.slots_in_use(), MAX_REVALIDATING);

    // New transactions still get in while the pool is checked again.
    for n in 0..u16::try_from(MAX_PENDING - MAX_REVALIDATING).expect("small") {
        assert!(model
            .reserve(id(1_000 + n), peer(1_000 + n), Intent::Fetch, secs(1))
            .is_ok());
    }
    assert_eq!(
        model.reserve(id(2_000), peer(2_000), Intent::Fetch, secs(1)),
        Err(ModelUnreserved::NoRoom(NoRoom::Node))
    );

    // A finished revalidation hands its place to the next queued one.
    let step = model.outcome(
        step.started()[0],
        Outcome::Pass,
        stable(1),
        stable(1),
        secs(2),
    );
    assert_eq!(step.started().len(), 1);
    model.assert_invariants();
}

#[test]
fn a_rejection_and_a_later_admission_of_one_id_keep_their_order_in_the_log() {
    let mut model = new_model();
    let mut log = ChangeLog::default();

    let (_, job) = bring_in(&mut model, tx(1), legacy(1));
    let step = model.outcome(
        job,
        Outcome::Fail {
            scope: RejectionScope::ExactTip,
            charge: Charge::None,
        },
        stable(0),
        stable(0),
        secs(0),
    );
    log.append(step.changes);
    log.append(
        model
            .transition(transition(1, 101, vec![vec![]]), secs(1))
            .changes,
    );
    let (_, job) = bring_in(&mut model, tx(1), legacy(2));
    let step = model.outcome(job, Outcome::Pass, stable(1), stable(1), secs(2));
    log.append(step.changes);

    // One numbered stream carries both, so a reader that fell behind still
    // ends on "added".
    let ChangesSince::Entries(entries) = log.since(ChangeSeq(0)) else {
        panic!("the reader is on the log");
    };
    assert_eq!(
        entries.iter().map(|entry| entry.change).collect::<Vec<_>>(),
        vec![Change::Refused(id(1)), Change::Added(id(1))]
    );
}

// The model: cooldowns and the off switch.

#[test]
fn a_reported_violation_starts_a_cooldown_without_a_submission() {
    let mut model = new_model();
    model.violation(v2(1), secs(0));
    assert_eq!(
        model.reserve(id(1), Source::Peer(v2(1)), Intent::Fetch, secs(1)),
        Err(ModelUnreserved::CoolingDown)
    );
    model.tick(COOLDOWN);
    assert!(model
        .reserve(id(1), Source::Peer(v2(1)), Intent::Fetch, COOLDOWN)
        .is_ok());
}

#[test]
fn turning_the_pool_off_answers_every_waiter() {
    let mut model = new_model();
    admit(&mut model, tx(1), 1, 0);
    let (verifying, job) = bring_in(&mut model, tx(2), legacy(2));
    let (parked, parked_job) = bring_in(&mut model, child_of(4, 3), legacy(3));
    model.outcome(parked_job, missing(3), stable(0), stable(0), secs(0));

    let step = model.deactivate(secs(1));
    let fault = Some(ModelVerdict::LocalFault(LocalFault::Deactivated));
    assert_eq!(step.verdict(verifying), fault);
    assert_eq!(step.verdict(parked), fault);
    assert!(step.effects.contains(&Effect::Cancel(job)));
    assert_eq!(
        step.changes,
        vec![Change::Removed {
            id: id(1),
            reason: RemovalReason::Deactivated
        }]
    );
    assert_eq!(
        model.reserve(id(5), peer(5), Intent::Fetch, secs(1)),
        Err(ModelUnreserved::Inactive)
    );

    model.activate(Height(500), Generation(9), secs(2));
    assert!(model.visible().is_empty());
    assert!(model
        .reserve(id(5), peer(5), Intent::Fetch, secs(2))
        .is_ok());
}

// A scripted random run.

/// A small deterministic generator, so the run needs no extra dependency
/// and replays exactly.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self, bound: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % bound
    }

    /// An index into a non-empty list of `len` items.
    fn pick(&mut self, len: usize) -> usize {
        let bound = u64::try_from(len).expect("a test list fits in u64");
        usize::try_from(self.next(bound)).expect("an index below a usize fits in usize")
    }
}

#[test]
fn every_submission_gets_exactly_one_verdict_and_the_limits_hold() {
    for seed in 0..40 {
        let mut rng = Lcg(seed);
        let mut model = new_model();
        let mut generation = 0;
        let mut tip = 100;
        let mut now = secs(0);
        let mut reservations: Vec<(ReservationId, u16, u16)> = Vec::new();
        let mut jobs: Vec<JobId> = Vec::new();
        let mut waiters: Vec<WaiterId> = Vec::new();
        let mut verdicts: Vec<WaiterId> = Vec::new();

        let record = |step: Step, jobs: &mut Vec<JobId>, verdicts: &mut Vec<WaiterId>| {
            for effect in step.effects {
                match effect {
                    Effect::Start { job, .. } => jobs.push(job),
                    Effect::Cancel(job) => jobs.retain(|running| *running != job),
                    Effect::Verdict { waiter, .. } => verdicts.push(waiter),
                    Effect::Outputs { .. } => {}
                }
            }
        };

        for _ in 0..600 {
            now += Duration::from_millis(rng.next(4_000));
            let n = u16::try_from(rng.next(40)).expect("small");
            let p = u16::try_from(rng.next(6)).expect("small");
            match rng.next(10) {
                0..=2 => {
                    let intent = if rng.next(2) == 0 {
                        Intent::Fetch
                    } else {
                        Intent::Deliver
                    };
                    if let Ok(reservation) = model.reserve(id(n), peer(p), intent, now) {
                        reservations.push((reservation.id, n, p));
                    }
                }
                3..=4 if !reservations.is_empty() => {
                    let pick = rng.pick(reservations.len());
                    let (reservation, n, p) = reservations.swap_remove(pick);
                    if rng.next(5) == 0 {
                        let step = model.release(reservation, now);
                        record(step, &mut jobs, &mut verdicts);
                    } else {
                        // Some transactions spend the output of a lower id.
                        let body = if n % 3 == 0 && n > 0 {
                            child_of(n, n - 1)
                        } else {
                            tx(n)
                        };
                        let (waiter, step) =
                            model.submit(reservation, body, Supplier::Peer(legacy(p)), now);
                        waiters.push(waiter);
                        record(step, &mut jobs, &mut verdicts);
                    }
                }
                5..=7 if !jobs.is_empty() => {
                    let pick = rng.pick(jobs.len());
                    let job = jobs.swap_remove(pick);
                    let outcome = match rng.next(6) {
                        0 => Outcome::Fail {
                            scope: RejectionScope::ExactTip,
                            charge: Charge::None,
                        },
                        1 if n > 0 => missing(n - 1),
                        2 => Outcome::LocalFault(LocalFault::StateUnavailable),
                        _ => Outcome::Pass,
                    };
                    let read = stable(generation - rng.next(2).min(generation));
                    let step = model.outcome(job, outcome, read, stable(generation), now);
                    record(step, &mut jobs, &mut verdicts);
                }
                8 => {
                    generation += 1;
                    tip += 1;
                    let mut change = transition(generation, tip, vec![vec![tx(n)]]);
                    change.disconnected = usize::try_from(rng.next(4) / 3).expect("small");
                    let step = model.transition(change, now);
                    record(step, &mut jobs, &mut verdicts);
                }
                _ => {
                    let step = model.tick(now);
                    record(step, &mut jobs, &mut verdicts);
                }
            }
            model.assert_invariants();
        }

        // pool/verdict-for-every-submission: turning the pool off ends what
        // is still undecided, and then every waiter has exactly one verdict.
        let step = model.deactivate(now);
        record(step, &mut jobs, &mut verdicts);
        waiters.sort();
        verdicts.sort();
        assert_eq!(waiters, verdicts, "seed {seed}");
    }
}
