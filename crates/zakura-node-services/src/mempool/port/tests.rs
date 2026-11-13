//! Tests of the port and its model.
//!
//! Each model test is one case the pool core has to cover. Most come from
//! review findings on the M1 and M2 plans, and the test name says which
//! rule it pins.

use std::{
    collections::HashSet,
    net::{Ipv6Addr, SocketAddr},
    time::Duration,
};

use zakura_chain::{
    block::Height,
    transaction::{self, UnminedTxId},
    transparent::OutPoint,
};

use super::{
    model::{
        Effect, JobId, ModelBlock, ModelReader, ModelTransition, ModelTx, ModelUnreserved,
        ModelVerdict, Outcome, OutputAnswer, PoolModel, Step, WaiterId, COOLDOWN, MAX_UNDECIDED,
        REJECTION_MEMORY, REVALIDATE_RETRY, T_PARK, T_VERIFY,
    },
    pool_port, ChainReason, ChainTip, Change, ChangeLog, ChangeSeq, ChangesSince, Charge,
    CooldownKey, Generation, GenerationCell, GenerationRead, Intent, LocalFault, NoRoom,
    PoolCommand, PoolSnapshot, RejectionScope, RemovalReason, ReservationId, Source, Supplier,
    Unreserved, Verdict, Violation, CHANGE_LOG_CAPACITY, COMMAND_QUEUE, MAX_COOLDOWNS, MAX_PENDING,
    MAX_PENDING_PER_SOURCE, MAX_REJECTIONS, MAX_REVALIDATING, MAX_WAITERS,
};
use crate::mempool::QueueSource;
use crate::mempool::TransactionDependencies;

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

/// A legacy peer with its own IP address, since a cooldown is per address.
fn legacy(n: u16) -> QueueSource {
    let [high, low] = n.to_be_bytes();
    QueueSource::LegacySocket(SocketAddr::from(([10, 0, high, low], 8233)))
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

/// The entry for transaction `n` becoming visible again, after it was
/// hidden by the log entry at position `hidden_at`.
fn revealed(n: u16, hidden_at: u64) -> Change {
    Change::Revealed {
        id: id(n),
        hidden_at: ChangeSeq(hidden_at),
    }
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
        rules_change: false,
    }
}

fn reorg(generation: u64, tip: u32) -> ModelTransition {
    ModelTransition {
        disconnected: 1,
        connected: vec![ModelBlock::default()],
        tip: Height(tip),
        generation: Generation(generation),
        rules_change: false,
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
        let _ = reply.send(Ok(inbox.grant(ReservationId(7), tx_id, false, true)));
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
    // The relay's fetch is still out, so its slot is still taken.
    assert_eq!(model.slots_in_use(), 1);

    // The relay's own answer arrives late. It gets a verdict and no charge.
    let (late, step) = model.submit(relay.id, tx(1), Supplier::Peer(v2(1)), secs(3));
    assert_eq!(
        step.verdict(late),
        Some(rejected(RejectionScope::Uncached, Charge::None))
    );
    assert!(step.started().is_empty());
    assert_eq!(model.slots_in_use(), 0);
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
            scope: RejectionScope::EffectsChain(ChainReason::DuplicateSpend),
            charge: Charge::Cooldown,
        },
        stable(0),
        stable(0),
        secs(1),
    );
    let charged = rejected(
        RejectionScope::EffectsChain(ChainReason::DuplicateSpend),
        Charge::Cooldown,
    );
    assert_eq!(step.verdict(first), Some(charged));
    assert_eq!(step.verdict(second), Some(charged));
    assert_eq!(
        step.verdict(rpc),
        Some(rejected(
            RejectionScope::EffectsChain(ChainReason::DuplicateSpend),
            Charge::None
        ))
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
        Err(ModelUnreserved::Rejected(RejectionScope::EffectsChain(
            ChainReason::DuplicateSpend
        )))
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
            scope: RejectionScope::EffectsChain(ChainReason::DuplicateSpend),
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
    let (waiter, job) = bring_in(&mut model, tx(1), legacy(1));
    let step = model.outcome(
        job,
        Outcome::Pass,
        GenerationRead::Changing,
        GenerationRead::Changing,
        secs(1),
    );
    // No verdict and nothing admitted. The core cannot tell whether that
    // change is one it has applied, so it does not wait for an event. The
    // job runs again.
    assert_eq!(step.verdict(waiter), None);
    assert!(step.changes.is_empty());
    assert_eq!(step.started().len(), 1);
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
    assert_eq!(step.changes, vec![revealed(1, 2)]);
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

    assert_eq!(model.tick(secs(50)), Step::default());
    let step = model.outcome(
        parent_job,
        Outcome::Fail {
            scope: RejectionScope::EffectsChain(ChainReason::DuplicateSpend),
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

    // The child's wait began when its input was first missing, and ends a
    // flat sixty seconds later.
    assert_eq!(model.tick(T_PARK - secs(1)), Step::default());
    let step = model.tick(T_PARK);
    assert_eq!(
        step.verdict(child),
        Some(rejected(RejectionScope::ExactTip, Charge::None))
    );
    assert_eq!(step.changes, vec![Change::Refused(id(2))]);
}

#[test]
fn the_wait_for_a_parent_is_flat_and_does_not_stop_while_the_parent_is_verified() {
    let mut model = new_model();
    let (child, job) = bring_in(&mut model, child_of(2, 1), legacy(1));
    model.outcome(job, missing(1), stable(0), stable(0), secs(0));

    // The parent's body arrives at forty seconds and its verification is
    // slow.
    let reservation = model
        .reserve(id(1), peer(2), Intent::Fetch, secs(30))
        .expect("room");
    model.submit(reservation.id, tx(1), Supplier::Peer(legacy(2)), secs(40));

    // The child's sixty seconds are not paused for that. This is today's
    // wait, and it is what keeps the child's lifetime bounded.
    assert_eq!(model.tick(T_PARK - secs(1)).verdict(child), None);
    assert_eq!(
        model.tick(T_PARK).verdict(child),
        Some(rejected(RejectionScope::ExactTip, Charge::None))
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
    // What the block mined leaves first, then what it conflicts with, as
    // today.
    assert_eq!(
        step.changes,
        vec![
            Change::Removed {
                id: id(3),
                reason: RemovalReason::Mined
            },
            Change::Removed {
                id: id(1),
                reason: RemovalReason::Conflicted
            },
            Change::Removed {
                id: id(2),
                reason: RemovalReason::Invalidated
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
        (3, RejectionScope::EffectsChain(ChainReason::DuplicateSpend)),
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
        Err(ModelUnreserved::Rejected(RejectionScope::EffectsChain(
            ChainReason::DuplicateSpend
        )))
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
    assert_eq!(step.changes, vec![revealed(1, 3)]);
    let step = model.outcome(the_job(&step), Outcome::Pass, stable(1), stable(1), secs(3));
    assert_eq!(step.changes, vec![revealed(2, 4)]);
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
    assert_eq!(step.changes, vec![revealed(1, 2)]);
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
                    Effect::Outputs { .. }
                    | Effect::OutputReady { .. }
                    | Effect::OutputWaitEnded(_) => {}
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

// Cases from review of the contract's first version.

/// A unit transaction that the test only needs for its id.
fn body(n: u8) -> zakura_chain::transaction::UnminedTx {
    use zakura_chain::{
        block::Height as BlockHeight,
        transaction::{LockTime, Transaction},
    };
    let mut expiry = BlockHeight(1_000_000);
    expiry.0 += u32::from(n);
    Transaction::V4 {
        inputs: Vec::new(),
        outputs: Vec::new(),
        lock_time: LockTime::unlocked(),
        expiry_height: expiry,
        joinsplit_data: None,
        sapling_shielded_data: None,
    }
    .into()
}

#[tokio::test]
async fn cancelling_a_blocked_submission_frees_its_slot() {
    let (handle, mut inbox) = pool_port();
    let transaction = body(1);
    let tx_id = transaction.id();
    let reservation = inbox.grant(ReservationId(7), tx_id, false, true);

    // The command queue is full, so the submission waits to be sent.
    for _ in 0..COMMAND_QUEUE {
        handle
            .report_violation(Violation {
                peer: v2(1),
                cause: "test",
            })
            .await;
    }
    let caller = {
        let handle = handle.clone();
        tokio::spawn(async move {
            handle
                .submit(reservation, transaction, Supplier::Nobody)
                .await
        })
    };
    tokio::task::yield_now().await;
    assert_eq!(inbox.released(), None);

    // The caller goes away while it waits. The command never reaches the
    // core, so the reservation inside it is dropped still armed.
    caller.abort();
    assert!(caller
        .await
        .expect_err("the caller was aborted")
        .is_cancelled());
    assert_eq!(inbox.released(), Some(ReservationId(7)));
}

#[tokio::test]
async fn an_accepted_reservation_is_the_cores_and_a_dropped_command_is_not() {
    let (handle, mut inbox) = pool_port();
    let transaction = body(1);
    let reservation = inbox.grant(ReservationId(7), transaction.id(), false, true);
    let caller = {
        let handle = handle.clone();
        tokio::spawn(async move {
            handle
                .submit(reservation, transaction, Supplier::Nobody)
                .await
        })
    };

    let Some(PoolCommand::Submit {
        reservation, reply, ..
    }) = inbox.command().await
    else {
        panic!("a reservation that wants the body sends it");
    };
    assert_eq!(reservation.accept(), ReservationId(7));
    let _ = reply.send(Verdict::Admitted);
    assert!(matches!(
        caller.await.expect("the caller is not cancelled"),
        Verdict::Admitted
    ));
    // The core owns the outcome now. Nothing is released.
    assert_eq!(inbox.released(), None);
}

#[tokio::test]
async fn a_joined_reservation_that_needs_no_body_sends_none() {
    let (handle, mut inbox) = pool_port();
    let transaction = body(1);
    let reservation = inbox.grant(ReservationId(8), transaction.id(), true, false);
    let caller = {
        let handle = handle.clone();
        tokio::spawn(async move {
            handle
                .submit(reservation, transaction, Supplier::Peer(v2(1)))
                .await
        })
    };

    let Some(PoolCommand::Join { reply, .. }) = inbox.command().await else {
        panic!("the body stays with the caller");
    };
    let _ = reply.send(Verdict::Admitted);
    assert!(matches!(
        caller.await.expect("the caller is not cancelled"),
        Verdict::Admitted
    ));
}

#[tokio::test]
async fn a_body_for_another_id_is_refused_and_frees_the_slot() {
    let (handle, mut inbox) = pool_port();
    let reservation = inbox.grant(ReservationId(9), body(1).id(), false, true);
    assert!(matches!(
        handle.submit(reservation, body(2), Supplier::Nobody).await,
        Verdict::LocalFault(LocalFault::WrongTransaction)
    ));
    assert_eq!(inbox.released(), Some(ReservationId(9)));
}

fn snapshot_at(last_change: ChangeSeq) -> PoolSnapshot {
    PoolSnapshot::new(
        ChainTip {
            hash: zakura_chain::block::Hash([0; 32]),
            height: Height(100),
        },
        Generation(0),
        last_change,
        Vec::new(),
        TransactionDependencies::default(),
    )
}

#[tokio::test]
async fn a_view_pairs_each_snapshot_with_exactly_its_changes() {
    let (handle, inbox) = pool_port();
    assert!(handle.view(ChangeSeq(0)).is_none());

    inbox.publish([Change::Added(id(1))], snapshot_at);
    inbox.publish([Change::Added(id(2)), Change::Refused(id(3))], snapshot_at);

    let view = handle.view(ChangeSeq(1)).expect("published");
    assert_eq!(view.snapshot.last_change, ChangeSeq(3));
    let ChangesSince::Entries(entries) = view.changes else {
        panic!("the reader is on the log");
    };
    assert_eq!(
        entries.iter().map(|entry| entry.change).collect::<Vec<_>>(),
        vec![Change::Added(id(2)), Change::Refused(id(3))]
    );
}

#[test]
fn a_reader_is_never_handed_changes_newer_than_its_snapshot() {
    let mut log = ChangeLog::default();
    log.append([Change::Added(id(1))]);
    log.append([Change::Added(id(2))]);
    // A snapshot taken after the first batch.
    let ChangesSince::Entries(entries) = log.between(ChangeSeq(0), ChangeSeq(1)) else {
        panic!("the reader is on the log");
    };
    assert_eq!(
        entries.iter().map(|entry| entry.change).collect::<Vec<_>>(),
        vec![Change::Added(id(1))]
    );
}

#[test]
fn a_legacy_cooldown_key_is_the_canonical_ip() {
    let plain = QueueSource::LegacySocket(SocketAddr::from(([10, 0, 0, 1], 8233)));
    let other_port = QueueSource::LegacySocket(SocketAddr::from(([10, 0, 0, 1], 9999)));
    let mapped = QueueSource::LegacySocket(SocketAddr::from((
        Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0x0a00, 0x0001),
        8233,
    )));
    assert_eq!(CooldownKey::from(&plain), CooldownKey::from(&other_port));
    assert_eq!(CooldownKey::from(&plain), CooldownKey::from(&mapped));
    assert_ne!(CooldownKey::from(&v2(1)), CooldownKey::from(&v2(2)));
}

#[test]
fn a_cooldown_follows_a_legacy_peer_to_a_new_port() {
    let mut model = new_model();
    let first = QueueSource::LegacySocket(SocketAddr::from(([10, 9, 9, 9], 8233)));
    let reconnected = QueueSource::LegacySocket(SocketAddr::from(([10, 9, 9, 9], 40_000)));
    model.violation(first, secs(0));
    assert_eq!(
        model.reserve(id(1), Source::Peer(reconnected), Intent::Fetch, secs(1)),
        Err(ModelUnreserved::CoolingDown)
    );
}

#[test]
fn only_one_joined_caller_is_asked_for_the_body() {
    let mut model = new_model();
    // tx-relay reserved the id and is fetching it.
    model
        .reserve(id(1), Source::Peer(v2(1)), Intent::Fetch, secs(0))
        .expect("room");
    let first = model
        .reserve(id(1), peer(2), Intent::Deliver, secs(0))
        .expect("joins");
    let second = model
        .reserve(id(1), peer(3), Intent::Deliver, secs(0))
        .expect("joins");
    // Both hold the body. The core takes it from one of them.
    assert!(first.wants_body);
    assert!(!second.wants_body);

    let (second_waiter, step) = model.wait(second.id, id(1), Supplier::Peer(legacy(3)), secs(0));
    assert!(step.started().is_empty());
    let (first_waiter, step) = model.submit(first.id, tx(1), Supplier::Peer(legacy(2)), secs(0));
    let job = the_job(&step);

    // A caller that joins once the core holds the body is not asked either.
    let third = model
        .reserve(id(1), peer(4), Intent::Deliver, secs(0))
        .expect("joins");
    assert!(!third.wants_body);

    let step = model.outcome(job, Outcome::Pass, stable(0), stable(0), secs(1));
    assert_eq!(step.verdict(first_waiter), Some(ModelVerdict::Admitted));
    assert_eq!(step.verdict(second_waiter), Some(ModelVerdict::Admitted));
    assert_eq!(model.bodies_held(), 0);
}

#[test]
fn waiters_get_a_local_fault_when_nobody_brings_the_body() {
    let mut model = new_model();
    let owner = model
        .reserve(id(1), Source::Peer(v2(1)), Intent::Fetch, secs(0))
        .expect("room");
    let bringer = model
        .reserve(id(1), peer(2), Intent::Deliver, secs(0))
        .expect("joins");
    let waiting = model
        .reserve(id(1), peer(3), Intent::Deliver, secs(0))
        .expect("joins");
    let (waiter, _) = model.wait(waiting.id, id(1), Supplier::Peer(legacy(3)), secs(0));

    // The fetch fails, and the caller that was asked for the body is
    // cancelled.
    assert_eq!(model.release(owner.id, secs(1)), Step::default());
    let step = model.release(bringer.id, secs(1));
    assert_eq!(
        step.verdict(waiter),
        Some(ModelVerdict::LocalFault(LocalFault::NoBody))
    );
    assert_eq!(model.slots_in_use(), 0);
    model.assert_invariants();
}

#[test]
fn a_joined_reservation_does_not_reach_a_later_admission_of_the_same_id() {
    let mut model = new_model();
    let (_, job) = bring_in(&mut model, tx(1), legacy(1));
    let alice = model
        .reserve(id(1), peer(2), Intent::Deliver, secs(0))
        .expect("joins");
    model.outcome(
        job,
        Outcome::LocalFault(LocalFault::StateUnavailable),
        stable(0),
        stable(0),
        secs(1),
    );

    // Bob starts a new admission of the same id.
    let (bob, _) = bring_in(&mut model, tx(1), legacy(3));
    // Alice's old reservation died with the first admission. It neither
    // joins Bob's nor counts among its waiters.
    let (late, step) = model.submit(alice.id, tx(1), Supplier::Peer(legacy(2)), secs(2));
    assert_eq!(
        step.verdict(late),
        Some(ModelVerdict::LocalFault(LocalFault::StaleReservation))
    );
    assert_eq!(step.verdict(bob), None);
    model.assert_invariants();
}

#[test]
fn a_child_that_spends_an_output_its_visible_parent_lacks_is_not_verified_in_a_loop() {
    let mut model = new_model();
    admit(&mut model, tx(1), 1, 0);
    // The parent has one output. The child spends output 5 of it.
    let child = tx(2).spending(hash(1), 5);
    let (waiter, job) = bring_in(&mut model, child, legacy(2));
    let no_such_output = Outcome::MissingInputs(vec![OutPoint {
        hash: hash(1),
        index: 5,
    }]);

    // The parent is visible, but nothing changed since the verifier looked.
    let step = model.outcome(job, no_such_output, stable(0), stable(0), secs(1));
    assert_eq!(step, Step::default(), "no immediate second run");
    assert!(model.is_parked(id(2)));

    // A visible parent does not stop the wait, so the child is answered.
    let step = model.tick(secs(1) + T_PARK);
    assert_eq!(
        step.verdict(waiter),
        Some(rejected(RejectionScope::ExactTip, Charge::None))
    );
    assert_eq!(model.slots_in_use(), 0);
}

#[test]
fn waking_and_parking_again_does_not_restart_the_wait() {
    let mut model = new_model();
    let (waiter, job) = bring_in(&mut model, child_of(2, 1), legacy(1));
    model.outcome(job, missing(1), stable(0), stable(0), secs(0));

    // Forty seconds parked. Then a block mines something with the parent's
    // id, the child runs again, and the output is still missing.
    let step = model.transition(transition(1, 101, vec![vec![tx(1)]]), secs(40));
    let job = the_job(&step);
    model.outcome(job, missing(1), stable(1), stable(1), secs(40));
    assert!(model.is_parked(id(2)));

    assert_eq!(model.tick(secs(59)).verdict(waiter), None);
    assert_eq!(
        model.tick(secs(60)).verdict(waiter),
        Some(rejected(RejectionScope::ExactTip, Charge::None))
    );
}

#[test]
fn a_missed_block_that_mined_a_parent_keeps_its_child() {
    let mut model = new_model();
    admit(&mut model, tx(1), 1, 0);
    admit(&mut model, child_of(2, 1), 2, 0);

    let step = model.resync(Height(240), Generation(5), secs(1));
    let parent_job = the_job(&step);

    // The state finds the parent in the best chain.
    let step = model.outcome(
        parent_job,
        Outcome::AlreadyMined,
        stable(5),
        stable(5),
        secs(2),
    );
    assert_eq!(
        step.changes,
        vec![Change::Removed {
            id: id(1),
            reason: RemovalReason::Mined
        }]
    );
    // The child is not removed with it. It is checked against the chain.
    let child_job = the_job(&step);
    let step = model.outcome(child_job, Outcome::Pass, stable(5), stable(5), secs(3));
    assert_eq!(step.changes, vec![revealed(2, 4)]);
    assert_eq!(model.visible(), vec![id(2)]);
}

#[test]
fn a_reorg_forgets_chain_scoped_rejections_too() {
    let mut model = new_model();
    let (_, job) = bring_in(&mut model, tx(1), legacy(1));
    // Rejected as expired at this height.
    model.outcome(
        job,
        Outcome::Fail {
            scope: RejectionScope::EffectsChain(ChainReason::DuplicateSpend),
            charge: Charge::None,
        },
        stable(0),
        stable(0),
        secs(0),
    );

    // The chain goes back below the expiry height. The old answer says
    // nothing about the new chain.
    model.transition(reorg(1, 90), secs(1));
    assert!(model
        .reserve(id(1), peer(2), Intent::Fetch, secs(1))
        .is_ok());

    let (_, job) = bring_in(&mut model, tx(3), legacy(3));
    model.outcome(
        job,
        Outcome::Fail {
            scope: RejectionScope::EffectsChain(ChainReason::DuplicateSpend),
            charge: Charge::None,
        },
        stable(1),
        stable(1),
        secs(1),
    );
    model.resync(Height(200), Generation(4), secs(2));
    assert!(model
        .reserve(id(3), peer(4), Intent::Fetch, secs(2))
        .is_ok());
}

#[test]
fn an_upgrade_activation_verifies_the_pool_again() {
    let mut model = new_model();
    admit(&mut model, tx(1), 1, 0);

    // No block left the chain, but the rules changed.
    let mut activation = transition(1, 101, vec![vec![]]);
    activation.rules_change = true;
    let step = model.transition(activation, secs(1));
    assert_eq!(step.changes, vec![Change::Hidden(id(1))]);
    assert!(model.visible().is_empty());
    assert_eq!(step.started().len(), 1);
}

#[test]
fn a_block_caches_what_it_mined_what_conflicted_and_what_expired() {
    let mut model = new_model();
    let coin = hash(900);
    admit(&mut model, tx(1).spending(coin, 0), 1, 0);
    let mut expiring = tx(2);
    expiring.expiry = Some(Height(101));
    admit(&mut model, expiring, 2, 0);
    admit(&mut model, tx(3), 3, 0);

    let block = vec![tx(7).spending(coin, 0), tx(3)];
    model.transition(transition(1, 101, vec![block]), secs(1));

    // None of them is fetched again, and each answers at the first stage
    // with the reason today's lists keep.
    for (n, reason) in [
        (1, ChainReason::DuplicateSpend),
        (2, ChainReason::Expired),
        (3, ChainReason::Mined),
        (7, ChainReason::Mined),
    ] {
        assert_eq!(
            model.reserve(id(n), peer(50), Intent::Fetch, secs(1)),
            Err(ModelUnreserved::Rejected(RejectionScope::EffectsChain(
                reason
            ))),
            "transaction {n}"
        );
    }
    // The query takes no slot and does not depend on room.
    let asked: HashSet<_> = [id(1), id(3), id(40)].into_iter().collect();
    let cached: HashSet<_> = [id(1), id(3)].into_iter().collect();
    assert_eq!(model.rejected_ids(&asked, secs(1)), cached);
    assert_eq!(model.slots_in_use(), 0);
}

#[test]
fn a_chain_scoped_rejection_is_forgotten_after_its_memory_time() {
    let mut model = new_model();
    let (_, job) = bring_in(&mut model, tx(1), legacy(1));
    model.outcome(
        job,
        Outcome::Fail {
            scope: RejectionScope::EffectsChain(ChainReason::DuplicateSpend),
            charge: Charge::None,
        },
        stable(0),
        stable(0),
        secs(0),
    );
    model.tick(REJECTION_MEMORY - secs(1));
    assert!(model
        .reserve(id(1), peer(2), Intent::Fetch, REJECTION_MEMORY - secs(1))
        .is_err());
    model.tick(REJECTION_MEMORY);
    assert!(model
        .reserve(id(1), peer(2), Intent::Fetch, REJECTION_MEMORY)
        .is_ok());
}

#[test]
fn the_rejection_lists_and_the_cooldowns_stay_bounded() {
    let mut model = new_model();
    let over = u16::try_from(MAX_REJECTIONS + 1).expect("the limit fits the test's ids");
    for n in 0..over {
        let (_, job) = bring_in(&mut model, tx(n), legacy(n));
        model.outcome(
            job,
            Outcome::Fail {
                scope: RejectionScope::ExactTip,
                charge: Charge::None,
            },
            stable(0),
            stable(0),
            secs(0),
        );
    }
    // A tip-scoped list that passes its limit is cleared, as today.
    assert!(model
        .reserve(id(0), peer(1), Intent::Fetch, secs(0))
        .is_ok());
    model.assert_invariants();

    let over = u16::try_from(MAX_COOLDOWNS + 1).expect("the limit fits the test's ids");
    let address = |n: u16| {
        let [high, low] = n.to_be_bytes();
        QueueSource::LegacySocket(SocketAddr::from(([10, 1, high, low], 8233)))
    };
    // All at one time, so none of them ends before the last one starts.
    for n in 0..over {
        model.violation(address(n), secs(0));
    }
    model.assert_invariants();
    // One made room for the last, and the last is in.
    let cooling = (0..over)
        .filter(|n| model.cooling(&address(*n), secs(0)))
        .count();
    assert_eq!(cooling, MAX_COOLDOWNS);
    assert!(model.cooling(&address(over - 1), secs(0)));
}

// Cases from Sol's review of the contract's first version.

fn output(parent: u16, index: u32) -> OutPoint {
    OutPoint {
        hash: hash(parent),
        index,
    }
}

#[test]
fn a_child_verified_against_a_parent_the_pool_has_since_lost_is_not_admitted() {
    let mut model = new_model();
    let coin = hash(900);
    admit(&mut model, tx(1).spending(coin, 0), 1, 0);
    let (child, job) = bring_in(&mut model, child_of(2, 1), legacy(2));

    // A block mines a transaction that conflicts with the parent. State is
    // at generation 1 and stable. The verifier reads that state, but the
    // pool it reads is the core's older one, which still offers the parent.
    let block = vec![tx(7).spending(coin, 0)];
    model.transition(transition(1, 101, vec![block]), secs(1));
    assert!(model.visible().is_empty());

    // Both readings are generation 1, which the core has applied. The
    // readings alone would let this through.
    let step = model.outcome(
        job,
        Outcome::PassUsing(vec![output(1, 0)]),
        stable(1),
        stable(1),
        secs(2),
    );
    assert_eq!(step.verdict(child), None);
    assert!(model.visible().is_empty());
    assert_eq!(step.started().len(), 1, "it is verified again");
    model.assert_invariants();
}

#[test]
fn a_changing_read_that_the_core_has_already_applied_does_not_strand_the_job() {
    let mut model = new_model();
    let (waiter, job) = bring_in(&mut model, tx(1), legacy(1));
    model.transition(transition(1, 101, vec![vec![]]), secs(1));

    // The worker read while generation 1 was becoming visible. By the time
    // its outcome arrives, the core has applied generation 1, and no other
    // chain event is coming.
    let step = model.outcome(
        job,
        Outcome::Pass,
        GenerationRead::Changing,
        stable(1),
        secs(2),
    );
    let job = the_job(&step);
    let step = model.outcome(job, Outcome::Pass, stable(1), stable(1), secs(3));
    assert_eq!(step.verdict(waiter), Some(ModelVerdict::Admitted));
}

#[test]
fn a_change_in_flight_on_top_of_the_applied_generation_is_waited_for() {
    let mut model = new_model();
    let (_, job) = bring_in(&mut model, tx(1), legacy(1));
    let step = model.outcome(
        job,
        Outcome::Pass,
        stable(0),
        GenerationRead::Changing,
        secs(1),
    );
    // Running again now would only read the same change in flight.
    assert_eq!(step, Step::default());
    let step = model.transition(transition(1, 101, vec![vec![]]), secs(2));
    assert_eq!(step.started().len(), 1);
}

#[test]
fn a_fourth_transaction_in_a_chain_of_dependencies_is_rejected() {
    let mut model = new_model();
    admit(&mut model, tx(1), 1, 0);
    for n in 2..=3 {
        let (waiter, job) = bring_in(&mut model, child_of(n, n - 1), legacy(n));
        let step = model.outcome(
            job,
            Outcome::PassUsing(vec![output(n - 1, 0)]),
            stable(0),
            stable(0),
            secs(0),
        );
        assert_eq!(step.verdict(waiter), Some(ModelVerdict::Admitted));
    }

    // Each of the four passes the verifier. The pool takes three.
    let (waiter, job) = bring_in(&mut model, child_of(4, 3), legacy(4));
    let step = model.outcome(
        job,
        Outcome::PassUsing(vec![output(3, 0)]),
        stable(0),
        stable(0),
        secs(0),
    );
    assert_eq!(
        step.verdict(waiter),
        Some(rejected(RejectionScope::EffectsTip, Charge::None))
    );
    assert_eq!(model.visible(), vec![id(1), id(2), id(3)]);
}

#[test]
fn a_fourth_transaction_beside_its_siblings_is_rejected_too() {
    let mut model = new_model();
    let mut parent = tx(1);
    parent.outputs = 3;
    admit(&mut model, parent, 1, 0);
    for (n, index) in [(2, 0), (3, 1)] {
        let (waiter, job) = bring_in(&mut model, tx(n).spending(hash(1), index), legacy(n));
        let step = model.outcome(
            job,
            Outcome::PassUsing(vec![output(1, index)]),
            stable(0),
            stable(0),
            secs(0),
        );
        assert_eq!(step.verdict(waiter), Some(ModelVerdict::Admitted));
    }

    // One ancestor only, but the group it would join already has three.
    let (waiter, job) = bring_in(&mut model, tx(4).spending(hash(1), 2), legacy(4));
    let step = model.outcome(
        job,
        Outcome::PassUsing(vec![output(1, 2)]),
        stable(0),
        stable(0),
        secs(0),
    );
    assert_eq!(
        step.verdict(waiter),
        Some(rejected(RejectionScope::EffectsTip, Charge::None))
    );
}

#[test]
fn a_cooled_supplier_cannot_bring_a_body_in_under_another_source() {
    let mut model = new_model();
    // The crawler reserved the id. Nothing is counted against a peer.
    let crawled = model
        .reserve(id(1), Source::Node, Intent::Fetch, secs(0))
        .expect("room");
    // The peer the body comes from enters a cooldown while it is fetched.
    model.violation(legacy(5), secs(1));

    let (waiter, step) = model.submit(crawled.id, tx(1), Supplier::Peer(legacy(5)), secs(2));
    assert_eq!(step.verdict(waiter), Some(ModelVerdict::CoolingDown));
    assert!(step.started().is_empty(), "its body is not verified");
    // Nothing is cached, and the slot is free.
    assert_eq!(model.slots_in_use(), 0);
    assert!(model
        .reserve(id(1), peer(6), Intent::Fetch, secs(2))
        .is_ok());
}

#[test]
fn a_revealed_entry_says_when_the_transaction_was_hidden() {
    let mut model = new_model();
    admit(&mut model, tx(1), 1, 0);
    let step = model.transition(reorg(1, 100), secs(1));
    // Entry 1 added it. Entry 2 hides it.
    assert_eq!(step.changes, vec![Change::Hidden(id(1))]);

    // A session that opens now has cursor 2 and a snapshot without the
    // transaction. One that opened before the reorg has cursor 1 and has
    // announced it.
    let step = model.outcome(the_job(&step), Outcome::Pass, stable(1), stable(1), secs(2));
    let Change::Revealed { hidden_at, .. } = step.changes[0] else {
        panic!("the transaction is visible again");
    };
    assert_eq!(hidden_at, ChangeSeq(2));
    let announces = |opened_at: ChangeSeq| opened_at >= hidden_at;
    assert!(announces(ChangeSeq(2)));
    assert!(!announces(ChangeSeq(1)));
}

// Cases from the second review.

#[test]
fn a_hidden_child_whose_parent_arrives_while_it_is_checked_is_checked_again() {
    let mut model = new_model();
    // The child is in the pool. Its parent was mined, so it is not.
    admit(&mut model, child_of(2, 1), 2, 0);
    // A reorg undoes the block that mined the parent.
    let step = model.transition(reorg(1, 99), secs(1));
    let child_job = the_job(&step);

    // The child's verifier looks first and misses the parent. The parent is
    // submitted again and admitted before the core handles that outcome.
    let (_, parent_job) = bring_in(&mut model, tx(1), legacy(1));
    model.outcome(parent_job, Outcome::Pass, stable(1), stable(1), secs(2));
    let step = model.outcome(child_job, missing(1), stable(1), stable(1), secs(2));

    // The child is valid again. It is not thrown out for an old answer.
    assert!(step.changes.is_empty());
    assert_eq!(model.hidden(), vec![id(2)]);
    let step = model.outcome(
        the_job(&step),
        Outcome::PassUsing(vec![output(1, 0)]),
        stable(1),
        stable(1),
        secs(3),
    );
    assert_eq!(step.changes, vec![revealed(2, 2)]);
}

#[test]
fn a_cooldown_and_a_remembered_rejection_end_on_time_without_a_tick() {
    let mut model = new_model();
    model.violation(v2(1), secs(0));
    let (_, job) = bring_in(&mut model, tx(1), legacy(1));
    model.outcome(
        job,
        Outcome::Fail {
            scope: RejectionScope::EffectsChain(ChainReason::Expired),
            charge: Charge::None,
        },
        stable(0),
        stable(0),
        secs(0),
    );

    // No tick in between. Each lookup uses the time of its own call.
    assert_eq!(
        model.reserve(
            id(2),
            Source::Peer(v2(1)),
            Intent::Fetch,
            COOLDOWN - secs(1)
        ),
        Err(ModelUnreserved::CoolingDown)
    );
    assert!(model
        .reserve(id(2), Source::Peer(v2(1)), Intent::Fetch, COOLDOWN)
        .is_ok());
    assert!(model
        .reserve(id(1), peer(2), Intent::Fetch, REJECTION_MEMORY - secs(1))
        .is_err());
    assert!(model
        .reserve(id(1), peer(2), Intent::Fetch, REJECTION_MEMORY)
        .is_ok());
}

#[test]
fn each_chain_rejection_reason_keeps_its_own_list_and_the_configured_memory() {
    let memory = secs(120);
    let mut model = new_model().with_rejection_memory(memory);
    let coin = hash(900);
    admit(&mut model, tx(1).spending(coin, 0), 1, 0);
    // One duplicate spend, then more mined transactions than one list holds.
    let block = vec![tx(7).spending(coin, 0)];
    model.transition(transition(1, 101, vec![block]), secs(0));
    let many = u16::try_from(MAX_REJECTIONS + 1).expect("the limit fits the test's ids");
    let mined: Vec<ModelTx> = (1_000..1_000 + many).map(tx).collect();
    model.transition(transition(2, 102, vec![mined]), secs(0));

    // The mined list dropped its oldest entry. The duplicate spend is in
    // another list and is still remembered.
    assert_eq!(
        model.reserve(id(1), peer(2), Intent::Fetch, secs(0)),
        Err(ModelUnreserved::Rejected(RejectionScope::EffectsChain(
            ChainReason::DuplicateSpend
        )))
    );
    assert!(model
        .reserve(id(7), peer(2), Intent::Fetch, secs(0))
        .is_ok());
    model.assert_invariants();

    // The memory time is the configured one, not the default.
    assert!(model.reserve(id(1), peer(3), Intent::Fetch, memory).is_ok());
}

#[test]
fn a_reader_that_resynchronized_while_a_transaction_was_hidden_announces_its_reveal() {
    let mut model = new_model();
    let mut log = ChangeLog::default();
    // The session opens on an empty pool.
    let opened_at = log.last();
    let (_, job) = bring_in(&mut model, tx(1), legacy(1));
    log.append(
        model
            .outcome(job, Outcome::Pass, stable(0), stable(0), secs(0))
            .changes,
    );
    let step = model.transition(reorg(1, 100), secs(1));
    let job = the_job(&step);
    log.append(step.changes);

    // The session falls off the log before it reads the addition, and
    // resynchronizes from a snapshot taken now, while the transaction is
    // hidden. That snapshot does not hold it.
    assert!(model.visible().is_empty());
    let resynced_at = log.last();

    log.append(
        model
            .outcome(job, Outcome::Pass, stable(1), stable(1), secs(2))
            .changes,
    );
    let ChangesSince::Entries(entries) = log.since(resynced_at) else {
        panic!("the reader is on the log again");
    };
    let Change::Revealed { hidden_at, .. } = entries[0].change else {
        panic!("the transaction is visible again");
    };
    // Its opening cursor would say "already announced". Its baseline moved
    // when it resynchronized, and says it has not.
    assert!(!opened_at.announces_reveal(hidden_at));
    assert!(resynced_at.announces_reveal(hidden_at));
}

#[tokio::test]
async fn a_dropped_reservation_wakes_an_idle_core() {
    let (handle, mut inbox) = pool_port();
    let reservation = inbox.grant(ReservationId(7), id(1), false, true);
    // The core has nothing to do and waits.
    let core = tokio::spawn(async move { inbox.next().await });
    tokio::task::yield_now().await;

    // No command follows. The drop alone has to wake it.
    drop(reservation);
    let event = tokio::time::timeout(secs(5), core)
        .await
        .expect("the core wakes")
        .expect("the core does not panic");
    assert!(matches!(
        event,
        Some(PoolCommand::Released(ReservationId(7)))
    ));
    drop(handle);
}

// Cases from Sol's second review.

#[test]
fn a_fetch_that_is_still_out_keeps_its_slot_after_someone_else_decides_the_id() {
    let mut model = new_model();
    let alice = peer(1);
    // Alice's five fetches are out.
    let fetching: Vec<_> = (0..5)
        .map(|n| {
            model
                .reserve(id(n), alice.clone(), Intent::Fetch, secs(0))
                .expect("five fit")
        })
        .collect();
    // Bob delivers all five, and they are verified before Alice's fetches
    // come back.
    for n in 0..5 {
        let joined = model
            .reserve(id(n), peer(2), Intent::Deliver, secs(0))
            .expect("joins");
        let (_, step) = model.submit(joined.id, tx(n), Supplier::Peer(legacy(2)), secs(0));
        model.outcome(the_job(&step), Outcome::Pass, stable(0), stable(0), secs(0));
    }
    assert_eq!(model.visible().len(), 5);

    // Five decoded bodies are still on their way from Alice's peers. She
    // cannot start a sixth fetch.
    assert_eq!(model.counted_against(&alice), 5);
    assert_eq!(
        model.reserve(id(9), alice.clone(), Intent::Fetch, secs(1)),
        Err(ModelUnreserved::NoRoom(NoRoom::Source))
    );

    // One fetch comes back, and one fails. Each frees its slot.
    let (late, step) = model.submit(fetching[0].id, tx(0), Supplier::Peer(legacy(1)), secs(2));
    assert_eq!(
        step.verdict(late),
        Some(rejected(RejectionScope::Uncached, Charge::None))
    );
    model.release(fetching[1].id, secs(2));
    assert_eq!(model.counted_against(&alice), 3);
    assert!(model.reserve(id(9), alice, Intent::Fetch, secs(2)).is_ok());
    model.assert_invariants();
}

#[test]
fn a_rollback_runs_every_parked_transaction_again() {
    let mut model = new_model();
    // The tip spent the output the child needs, so the verifier does not
    // find it and the child parks.
    let (_, job) = bring_in(&mut model, child_of(2, 1), legacy(1));
    model.outcome(job, missing(1), stable(0), stable(0), secs(0));
    assert!(model.is_parked(id(2)));

    // A reorg disconnects the spender. No connected block names the parent,
    // so nothing else would wake the child.
    let step = model.transition(reorg(1, 99), secs(1));
    assert_eq!(step.started().len(), 1);
    assert!(!model.is_parked(id(2)));
}

#[test]
fn a_hidden_transaction_waits_for_a_parent_that_is_on_its_way_back() {
    let mut model = new_model();
    // The child is pooled. Its parent was mined.
    admit(&mut model, child_of(2, 1), 2, 0);
    let step = model.transition(reorg(1, 99), secs(0));
    // The reorg undid the parent's block. The child's check does not find
    // the parent anywhere yet.
    let step = model.outcome(the_job(&step), missing(1), stable(1), stable(1), secs(1));
    assert!(step.changes.is_empty(), "it is not thrown out");
    assert_eq!(model.hidden(), vec![id(2)]);

    // The parent is delivered a second later.
    let (_, parent_job) = bring_in(&mut model, tx(1), legacy(1));
    let step = model.outcome(parent_job, Outcome::Pass, stable(1), stable(1), secs(2));
    let child_job = step.started()[0];
    let step = model.outcome(
        child_job,
        Outcome::PassUsing(vec![output(1, 0)]),
        stable(1),
        stable(1),
        secs(3),
    );
    assert_eq!(step.changes, vec![revealed(2, 2)]);
    assert_eq!(model.visible(), vec![id(2), id(1)]);
}

#[test]
fn a_hidden_transaction_whose_parent_never_returns_is_removed_after_its_wait() {
    let mut model = new_model();
    admit(&mut model, child_of(2, 1), 2, 0);
    let step = model.transition(reorg(1, 99), secs(0));
    model.outcome(the_job(&step), missing(1), stable(1), stable(1), secs(0));

    assert!(model.tick(T_PARK - secs(1)).changes.is_empty());
    assert_eq!(
        model.tick(T_PARK).changes,
        vec![Change::Removed {
            id: id(2),
            reason: RemovalReason::Invalidated
        }]
    );
}

#[test]
fn a_reveal_cannot_grow_a_dependency_group_past_its_limit() {
    let mut model = new_model();
    // Three pooled children of one mined parent. Each stands alone.
    for (n, index) in [(2, 0), (3, 1), (4, 2)] {
        admit(&mut model, tx(n).spending(hash(1), index), n, 0);
    }
    // A reorg undoes the parent's block, and the parent is admitted again.
    let step = model.transition(reorg(1, 99), secs(0));
    let jobs = step.started();
    let mut parent = tx(1);
    parent.outputs = 3;
    let (_, parent_job) = bring_in(&mut model, parent, legacy(1));
    model.outcome(parent_job, Outcome::Pass, stable(1), stable(1), secs(1));

    // Each child passes against the parent in the pool. The third would
    // make a group of four.
    for (job, index) in jobs.iter().zip(0..) {
        model.outcome(
            *job,
            Outcome::PassUsing(vec![output(1, index)]),
            stable(1),
            stable(1),
            secs(2),
        );
        model.assert_invariants();
    }
    assert_eq!(model.visible(), vec![id(2), id(3), id(1)]);
    assert!(model.hidden().is_empty());

    // The child that did not fit is remembered until the tip changes, so
    // it is not fetched again at once.
    assert_eq!(
        model.reserve(id(4), peer(9), Intent::Fetch, secs(2)),
        Err(ModelUnreserved::Rejected(RejectionScope::EffectsTip))
    );
    model.transition(transition(2, 100, vec![vec![]]), secs(3));
    assert!(model
        .reserve(id(4), peer(9), Intent::Fetch, secs(3))
        .is_ok());
}

#[test]
fn a_revealed_transaction_offers_its_outputs_to_waiters_again() {
    let mut model = new_model();
    admit(&mut model, tx(1), 1, 0);
    let step = model.transition(reorg(1, 100), secs(0));
    let step = model.outcome(the_job(&step), Outcome::Pass, stable(1), stable(1), secs(1));
    assert!(step.effects.contains(&Effect::Outputs {
        tx: hash(1),
        count: 1
    }));
}

#[test]
fn a_local_fault_is_in_the_change_log_but_not_in_any_cache() {
    let mut model = new_model();
    let (_, job) = bring_in(&mut model, tx(1), legacy(1));
    let step = model.outcome(
        job,
        Outcome::LocalFault(LocalFault::VerifyPanicked),
        stable(0),
        stable(0),
        secs(0),
    );
    // Today's change stream reports a verification that failed.
    assert_eq!(step.changes, vec![Change::Refused(id(1))]);
    assert!(model
        .reserve(id(1), peer(2), Intent::Fetch, secs(0))
        .is_ok());

    let (_, _) = bring_in(&mut model, tx(2), legacy(3));
    assert_eq!(model.tick(T_VERIFY).changes, vec![Change::Refused(id(2))]);
}

#[test]
fn a_conflict_with_a_later_block_transaction_is_cached_though_its_parent_went_first() {
    let mut model = new_model();
    let (coin_u, coin_v) = (hash(900), hash(901));
    admit(&mut model, tx(1).spending(coin_u, 0), 1, 0);
    let child = child_of(2, 1).spending(coin_v, 0);
    let (waiter, job) = bring_in(&mut model, child, legacy(2));
    let step = model.outcome(
        job,
        Outcome::PassUsing(vec![output(1, 0)]),
        stable(0),
        stable(0),
        secs(0),
    );
    assert_eq!(step.verdict(waiter), Some(ModelVerdict::Admitted));

    // The block's first transaction conflicts with the parent, which takes
    // the child with it. Its second conflicts with the child directly.
    let block = vec![tx(7).spending(coin_u, 0), tx(8).spending(coin_v, 0)];
    model.transition(transition(1, 101, vec![block]), secs(1));
    assert!(model.visible().is_empty());
    for n in [1, 2] {
        assert_eq!(
            model.reserve(id(n), peer(9), Intent::Fetch, secs(1)),
            Err(ModelUnreserved::Rejected(RejectionScope::EffectsChain(
                ChainReason::DuplicateSpend
            ))),
            "transaction {n}"
        );
    }
}

/// Appends a step's batch to the log and returns the entries a reader at
/// `cursor` then sees.
fn logged(log: &mut ChangeLog, step: Step, cursor: ChangeSeq) -> Vec<super::ChangeEntry> {
    log.append(step.changes);
    match log.since(cursor) {
        ChangesSince::Entries(entries) => entries,
        ChangesSince::Resync => panic!("the test's reader stays on the log"),
    }
}

#[test]
fn a_reader_that_reads_an_addition_after_it_was_hidden_announces_it_on_the_reveal() {
    let mut model = new_model();
    let mut log = ChangeLog::default();
    // The session opens on an empty pool, and announces nothing.
    let (mut reader, announced) = ModelReader::open(&model.visible(), log.last());
    assert!(announced.is_empty());

    // A transaction is added and then hidden before the session reads.
    let (_, job) = bring_in(&mut model, tx(1), legacy(1));
    log.append(
        model
            .outcome(job, Outcome::Pass, stable(0), stable(0), secs(0))
            .changes,
    );
    let step = model.transition(reorg(1, 100), secs(1));
    let job = the_job(&step);
    let entries = logged(&mut log, step, reader.cursor());
    assert_eq!(entries.len(), 2);
    // The snapshot does not hold it, so there is nothing to announce yet.
    assert!(reader.read(&entries, &model.visible()).is_empty());

    // It opened before the hide, and it still announces the reveal, because
    // it never announced the addition.
    let step = model.outcome(job, Outcome::Pass, stable(1), stable(1), secs(2));
    let entries = logged(&mut log, step, reader.cursor());
    assert_eq!(reader.read(&entries, &model.visible()), vec![id(1)]);
}

#[test]
fn a_reader_that_announced_a_transaction_skips_its_reveal() {
    let mut model = new_model();
    let mut log = ChangeLog::default();
    let (mut reader, _) = ModelReader::open(&model.visible(), log.last());

    let (_, job) = bring_in(&mut model, tx(1), legacy(1));
    let step = model.outcome(job, Outcome::Pass, stable(0), stable(0), secs(0));
    let entries = logged(&mut log, step, reader.cursor());
    assert_eq!(reader.read(&entries, &model.visible()), vec![id(1)]);

    let step = model.transition(reorg(1, 100), secs(1));
    let job = the_job(&step);
    let entries = logged(&mut log, step, reader.cursor());
    assert!(reader.read(&entries, &model.visible()).is_empty());
    let step = model.outcome(job, Outcome::Pass, stable(1), stable(1), secs(2));
    let entries = logged(&mut log, step, reader.cursor());
    assert!(reader.read(&entries, &model.visible()).is_empty());
}

#[test]
fn a_reader_that_opens_or_resynchronizes_while_a_transaction_is_hidden_announces_its_reveal() {
    let mut model = new_model();
    let mut log = ChangeLog::default();
    let (_, job) = bring_in(&mut model, tx(1), legacy(1));
    log.append(
        model
            .outcome(job, Outcome::Pass, stable(0), stable(0), secs(0))
            .changes,
    );
    // This reader announced it from its opening snapshot.
    let (mut early, announced) = ModelReader::open(&model.visible(), log.last());
    assert_eq!(announced, vec![id(1)]);

    let step = model.transition(reorg(1, 100), secs(1));
    let job = the_job(&step);
    log.append(step.changes);
    // One reader opens now, and the early one falls off the log and
    // resynchronizes now. Neither snapshot holds the transaction.
    let (mut late, announced) = ModelReader::open(&model.visible(), log.last());
    assert!(announced.is_empty());
    assert!(early.resync(&model.visible(), log.last()).is_empty());

    let step = model.outcome(job, Outcome::Pass, stable(1), stable(1), secs(2));
    let entries = logged(&mut log, step, late.cursor());
    assert_eq!(late.read(&entries, &model.visible()), vec![id(1)]);
    assert_eq!(early.read(&entries, &model.visible()), vec![id(1)]);
}

#[tokio::test]
async fn a_caller_can_see_which_peers_are_cooling_down() {
    use zakura_rt::time::Instant;
    let (handle, inbox) = pool_port();
    let now = Instant::now();
    assert!(!handle.is_cooling(&v2(1), now));

    let until = now + secs(600);
    inbox.publish_cooldowns([(CooldownKey::from(&v2(1)), until)].into_iter().collect());
    assert!(handle.is_cooling(&v2(1), now));
    assert!(!handle.is_cooling(&v2(2), now));
    assert!(!handle.is_cooling(&v2(1), until));
}

// Cases from the third review.

fn witnessed(n: u16, authorizing: u8) -> UnminedTxId {
    use zakura_chain::transaction::{AuthDigest, WtxId};
    UnminedTxId::Witnessed(WtxId {
        id: hash(n),
        auth_digest: AuthDigest([authorizing; 32]),
    })
}

#[test]
fn a_wrong_body_is_refused_and_frees_the_slot_in_the_model_too() {
    let mut model = new_model();
    let reservation = model
        .reserve(id(1), peer(1), Intent::Fetch, secs(0))
        .expect("room");
    let (waiter, step) = model.submit(reservation.id, tx(2), Supplier::Peer(legacy(1)), secs(0));
    assert_eq!(
        step.verdict(waiter),
        Some(ModelVerdict::LocalFault(LocalFault::WrongTransaction))
    );
    assert!(step.started().is_empty());
    assert_eq!(model.slots_in_use(), 0);
    model.assert_invariants();
}

#[test]
fn a_failed_fetch_is_logged_and_answers_anyone_waiting_on_it() {
    let mut model = new_model();
    let fetching = model
        .reserve(id(1), Source::Node, Intent::Fetch, secs(0))
        .expect("room");
    let bringer = model
        .reserve(id(1), peer(2), Intent::Deliver, secs(0))
        .expect("joins");
    let waiting = model
        .reserve(id(1), peer(3), Intent::Deliver, secs(0))
        .expect("joins");
    let (waiter, _) = model.wait(waiting.id, id(1), Supplier::Peer(legacy(3)), secs(0));
    model.release(bringer.id, secs(0));

    // The crawler's download fails. Today's change stream reports that.
    let step = model.fetch_failed(fetching.id, secs(1));
    assert_eq!(step.changes, vec![Change::Refused(id(1))]);
    assert_eq!(
        step.verdict(waiter),
        Some(ModelVerdict::LocalFault(LocalFault::NoBody))
    );
    assert_eq!(model.slots_in_use(), 0);

    // A caller that only stops waiting logs nothing.
    let fetching = model
        .reserve(id(2), Source::Node, Intent::Fetch, secs(1))
        .expect("room");
    assert_eq!(model.release(fetching.id, secs(2)), Step::default());
}

#[test]
fn a_transaction_found_already_mined_is_answered_the_same_way_each_time() {
    let mut model = new_model();
    // The pool started after this transaction was mined, so no block the
    // core saw named it.
    for attempt in 0..2 {
        let (waiter, job) = bring_in(&mut model, tx(1), legacy(attempt));
        let step = model.outcome(job, Outcome::AlreadyMined, stable(0), stable(0), secs(0));
        // The verdict comes after the lookup both times. Nothing is cached
        // that would answer the second one at the reservation instead.
        assert_eq!(
            step.verdict(waiter),
            Some(rejected(RejectionScope::Uncached, Charge::None))
        );
    }
}

#[test]
fn a_second_set_of_authorizing_data_is_a_duplicate_not_a_conflict() {
    let mut model = new_model();
    let coin = hash(900);
    let variant = |authorizing| {
        let mut tx = ModelTx::new(witnessed(1, authorizing));
        tx.spends.push(output(900, 0));
        tx
    };
    assert_eq!(coin, output(900, 0).hash);
    // Both are verified at once. They spend the same input, because they
    // are the same transaction.
    let (first, first_job) = bring_in(&mut model, variant(1), legacy(1));
    let (second, second_job) = bring_in(&mut model, variant(2), legacy(2));
    let step = model.outcome(first_job, Outcome::Pass, stable(0), stable(0), secs(0));
    assert_eq!(step.verdict(first), Some(ModelVerdict::Admitted));

    let step = model.outcome(second_job, Outcome::Pass, stable(0), stable(0), secs(0));
    assert_eq!(
        step.verdict(second),
        Some(rejected(RejectionScope::Uncached, Charge::None))
    );
    // Today's change stream reports the insertion the pool refused.
    assert_eq!(step.changes, vec![Change::Refused(witnessed(1, 2))]);
    // Nothing is cached against the transaction the pool holds.
    let pooled: HashSet<_> = [witnessed(1, 1), witnessed(1, 2)].into_iter().collect();
    assert!(model.rejected_ids(&pooled, secs(0)).is_empty());
    assert_eq!(model.visible(), vec![witnessed(1, 1)]);
}

#[test]
fn a_hidden_transaction_that_is_woken_and_parks_again_keeps_its_wait() {
    let mut model = new_model();
    // The child spends two mined parents.
    let child = tx(3).spending(hash(1), 0).spending(hash(2), 0);
    admit(&mut model, child, 3, 0);
    let step = model.transition(reorg(1, 98), secs(0));
    let both = Outcome::MissingInputs(vec![output(1, 0), output(2, 0)]);
    model.outcome(the_job(&step), both, stable(1), stable(1), secs(0));

    // Forty seconds later the first parent arrives and wakes it. The second
    // is still missing.
    // The first parent arrives at forty seconds.
    let reservation = model
        .reserve(id(1), peer(1), Intent::Fetch, secs(40))
        .expect("room");
    let (_, step) = model.submit(reservation.id, tx(1), Supplier::Peer(legacy(1)), secs(40));
    let step = model.outcome(
        the_job(&step),
        Outcome::Pass,
        stable(1),
        stable(1),
        secs(40),
    );
    let child_job = step.started()[0];
    model.outcome(child_job, missing(2), stable(1), stable(1), secs(40));

    // It has twenty seconds left, not another sixty.
    assert!(model.tick(secs(59)).changes.is_empty());
    assert_eq!(
        model.tick(secs(60)).changes,
        vec![Change::Removed {
            id: id(3),
            reason: RemovalReason::Invalidated
        }]
    );
}

#[test]
fn the_rejection_query_uses_the_time_it_is_asked_at() {
    let mut model = new_model();
    let (_, job) = bring_in(&mut model, tx(1), legacy(1));
    model.outcome(
        job,
        Outcome::Fail {
            scope: RejectionScope::EffectsChain(ChainReason::Expired),
            charge: Charge::None,
        },
        stable(0),
        stable(0),
        secs(0),
    );
    // No input moves the model's clock between these.
    let asked: HashSet<_> = [id(1)].into_iter().collect();
    assert_eq!(
        model.rejected_ids(&asked, REJECTION_MEMORY - secs(1)),
        asked
    );
    assert!(model.rejected_ids(&asked, REJECTION_MEMORY).is_empty());
    assert!(!model.cooling(&v2(1), secs(0)));
}

#[test]
fn a_snapshot_says_whether_the_pool_is_on() {
    let tip = ChainTip {
        hash: zakura_chain::block::Hash([0; 32]),
        height: Height(100),
    };
    assert!(snapshot_at(ChangeSeq(0)).active);
    let off = PoolSnapshot::inactive(tip, Generation(0), ChangeSeq(0));
    assert!(!off.active);
    assert!(off.transactions.is_empty());
}

// Cases from the fourth review.

#[test]
fn an_output_wait_is_answered_whichever_side_of_the_admission_it_arrives() {
    let mut model = new_model();
    // The wait comes first. The admission answers it in the same step.
    let OutputAnswer::Waiting(waiter) = model.await_output(output(1, 0), secs(0)) else {
        panic!("nothing creates the output yet");
    };
    let (_, job) = bring_in(&mut model, tx(1), legacy(1));
    let step = model.outcome(job, Outcome::Pass, stable(0), stable(0), secs(1));
    assert!(step.effects.contains(&Effect::OutputReady {
        waiter,
        outpoint: output(1, 0)
    }));
    assert_eq!(model.output_waits(), 0);

    // The wait comes second. The lookup answers it.
    assert_eq!(
        model.await_output(output(1, 0), secs(2)),
        OutputAnswer::Ready
    );
    // An output the transaction does not create is still waited for.
    assert!(matches!(
        model.await_output(output(1, 5), secs(2)),
        OutputAnswer::Waiting(_)
    ));
}

#[test]
fn an_output_of_a_hidden_transaction_is_waited_for_until_the_reveal() {
    let mut model = new_model();
    admit(&mut model, tx(1), 1, 0);
    let step = model.transition(reorg(1, 100), secs(0));
    let OutputAnswer::Waiting(waiter) = model.await_output(output(1, 0), secs(1)) else {
        panic!("a hidden transaction offers nothing");
    };
    let step = model.outcome(the_job(&step), Outcome::Pass, stable(1), stable(1), secs(2));
    assert!(step.effects.contains(&Effect::OutputReady {
        waiter,
        outpoint: output(1, 0)
    }));
}

#[test]
fn output_waits_are_limited_by_distinct_outpoints_and_end_when_the_pool_turns_off() {
    use super::model::MAX_OUTPUT_WAITS;
    let mut model = new_model();
    let limit = u32::try_from(MAX_OUTPUT_WAITS).expect("fits");
    let mut first = None;
    for index in 0..limit {
        let OutputAnswer::Waiting(waiter) = model.await_output(output(1, index), secs(0)) else {
            panic!("below the limit");
        };
        first.get_or_insert(waiter);
    }
    assert_eq!(
        model.await_output(output(2, 0), secs(0)),
        OutputAnswer::TooManyWaiters
    );
    // Another caller for an outpoint already waited on is not refused.
    let OutputAnswer::Waiting(second) = model.await_output(output(1, 0), secs(0)) else {
        panic!("the outpoint is already counted");
    };

    // A caller that stops waiting frees its outpoint once nobody else
    // waits on it.
    let first = first.expect("the limit is not zero");
    model.cancel_output_wait(first);
    assert_eq!(model.output_waits(), MAX_OUTPUT_WAITS);
    model.cancel_output_wait(second);
    assert_eq!(model.output_waits(), MAX_OUTPUT_WAITS - 1);
    assert!(matches!(
        model.await_output(output(2, 0), secs(0)),
        OutputAnswer::Waiting(_)
    ));

    // Turning the pool off answers everyone still waiting.
    let step = model.deactivate(secs(1));
    let ended = step
        .effects
        .iter()
        .filter(|effect| matches!(effect, Effect::OutputWaitEnded(_)))
        .count();
    assert_eq!(ended, MAX_OUTPUT_WAITS);
    assert_eq!(model.output_waits(), 0);
    assert_eq!(
        model.await_output(output(1, 0), secs(2)),
        OutputAnswer::Inactive
    );
}

#[test]
fn a_fetch_that_outlived_its_admission_still_knows_its_id() {
    let mut model = new_model();
    let fetching: Vec<_> = (0..2)
        .map(|n| {
            model
                .reserve(id(n), peer(1), Intent::Fetch, secs(0))
                .expect("room")
        })
        .collect();
    // Another caller's bodies decide both ids while the fetches are out.
    for n in 0..2 {
        let joined = model
            .reserve(id(n), peer(2), Intent::Deliver, secs(0))
            .expect("joins");
        let (_, step) = model.submit(joined.id, tx(n), Supplier::Peer(legacy(2)), secs(0));
        model.outcome(the_job(&step), Outcome::Pass, stable(0), stable(0), secs(0));
    }
    assert_eq!(model.slots_in_use(), 2);

    // A wrong body is still a wrong body, as the handle would say.
    let (waiter, step) = model.submit(fetching[0].id, tx(9), Supplier::Peer(legacy(1)), secs(1));
    assert_eq!(
        step.verdict(waiter),
        Some(ModelVerdict::LocalFault(LocalFault::WrongTransaction))
    );
    // A fetch that fails now adds nothing to the log. The id's outcome is
    // already there.
    assert_eq!(model.fetch_failed(fetching[1].id, secs(1)), Step::default());
    assert_eq!(model.slots_in_use(), 0);
}

#[test]
fn a_pooled_transaction_found_mined_by_lookup_is_not_cached_either() {
    let mut model = new_model();
    admit(&mut model, tx(1), 1, 0);
    let step = model.resync(Height(240), Generation(5), secs(1));
    model.outcome(
        the_job(&step),
        Outcome::AlreadyMined,
        stable(5),
        stable(5),
        secs(2),
    );
    assert!(model.visible().is_empty());

    // The core never saw its block connect. A later submission is looked
    // up again, and is answered at that stage.
    let (waiter, job) = bring_in(&mut model, tx(1), legacy(2));
    let step = model.outcome(job, Outcome::AlreadyMined, stable(5), stable(5), secs(3));
    assert_eq!(
        step.verdict(waiter),
        Some(rejected(RejectionScope::Uncached, Charge::None))
    );
}

#[test]
fn parked_children_cannot_hold_the_slots_their_hidden_parent_needs() {
    // Four slots, so the test can fill the core.
    let mut model = new_model().with_limits(4, 5, 3);
    admit(&mut model, tx(1), 1, 0);
    // Four local submissions spend the parent's output and are verifying.
    let mut children = Vec::new();
    for n in 2..6 {
        let reservation = model
            .reserve(id(n), Source::Node, Intent::Deliver, secs(0))
            .expect("four fit");
        let (waiter, step) =
            model.submit(reservation.id, child_of(n, 1), Supplier::Nobody, secs(0));
        children.push((waiter, the_job(&step)));
    }

    // A reorg hides the parent. Its check cannot start: the core is full.
    let step = model.transition(reorg(1, 99), secs(0));
    assert!(step.started().is_empty());
    for (_, job) in &children {
        model.outcome(*job, missing(1), stable(1), stable(1), secs(0));
    }
    assert_eq!(model.slots_in_use(), 4);

    // The parent is hidden but nothing is verifying it, so the children's
    // wait runs. When it ends they free the slots, and the parent's check
    // starts.
    assert!(model.tick(T_PARK - secs(1)).effects.is_empty());
    let step = model.tick(T_PARK);
    for (waiter, _) in &children {
        assert_eq!(
            step.verdict(*waiter),
            Some(rejected(RejectionScope::ExactTip, Charge::None))
        );
    }
    assert_eq!(step.started().len(), 1, "the parent is checked now");
    model.assert_invariants();
}

/// One random run's bookkeeping: what the test still owes the model.
#[derive(Default)]
struct Run {
    jobs: Vec<JobId>,
    waiters: Vec<WaiterId>,
    verdicts: Vec<WaiterId>,
    /// Output waits that have had no answer and were not cancelled, with
    /// the outpoint each one asked for.
    output_waits: Vec<(WaiterId, OutPoint)>,
}

impl Run {
    fn record(&mut self, step: Step) {
        for effect in step.effects {
            match effect {
                Effect::Start { job, .. } => self.jobs.push(job),
                Effect::Cancel(job) => self.jobs.retain(|running| *running != job),
                Effect::Verdict { waiter, .. } => self.verdicts.push(waiter),
                // An output wait ends once: with its output, or because the
                // pool turned off. An answer for a wait that is not out is
                // a duplicate, or one for a caller that already left.
                Effect::OutputReady { waiter, outpoint } => {
                    let index = self
                        .output_waits
                        .iter()
                        .position(|(waiting, _)| *waiting == waiter)
                        .expect("an output answer names a wait that is still out");
                    let (_, asked) = self.output_waits.swap_remove(index);
                    assert_eq!(asked, outpoint, "the answer is the output asked for");
                }
                Effect::OutputWaitEnded(waiter) => {
                    let index = self
                        .output_waits
                        .iter()
                        .position(|(waiting, _)| *waiting == waiter)
                        .expect("an ended output wait was still out");
                    self.output_waits.swap_remove(index);
                }
                Effect::Outputs { .. } => {}
            }
        }
    }
}

/// The body of transaction `n` in the random runs. Every third one spends
/// the one before it.
fn random_body(n: u16) -> ModelTx {
    let mut body = if n.is_multiple_of(3) && n > 0 {
        child_of(n, n - 1)
    } else {
        tx(n)
    };
    // Every fourth one expires a few blocks into the run.
    if n.is_multiple_of(4) {
        body.expiry = Some(Height(104 + u32::from(n)));
    }
    body
}

#[test]
fn a_random_run_with_small_limits_always_drains() {
    // Limits small enough that a few hundred steps reach every full state:
    // six slots, two for one source, three for revalidation.
    for seed in 0..300 {
        let mut rng = Lcg(seed);
        let mut model = new_model().with_limits(6, 2, 3);
        let mut run = Run::default();
        let mut generation = 0;
        let mut tip = 100;
        let mut now = secs(0);
        // Granted reservations: id, transaction, peer, whether the core
        // wants the body.
        let mut granted: Vec<(ReservationId, u16, u16, bool)> = Vec::new();
        // Whether a reading has told the core that a chain event is coming.
        let mut chain_event_owed = false;

        for _ in 0..400 {
            now += Duration::from_millis(rng.next(9_000));
            let n = u16::try_from(rng.next(12)).expect("small");
            let p = u16::try_from(rng.next(4)).expect("small");
            match rng.next(14) {
                0..=3 => {
                    let source = if p == 0 { Source::Node } else { peer(p) };
                    let intent = if rng.next(2) == 0 {
                        Intent::Fetch
                    } else {
                        Intent::Deliver
                    };
                    if let Ok(reservation) = model.reserve(id(n), source, intent, now) {
                        granted.push((reservation.id, n, p, reservation.wants_body));
                    }
                }
                4..=6 if !granted.is_empty() => {
                    let (reservation, n, p, wants_body) =
                        granted.swap_remove(rng.pick(granted.len()));
                    let supplier = if p == 0 {
                        Supplier::Nobody
                    } else {
                        Supplier::Peer(legacy(p))
                    };
                    match rng.next(8) {
                        0 => run.record(model.release(reservation, now)),
                        1 => run.record(model.fetch_failed(reservation, now)),
                        _ if wants_body => {
                            let (waiter, step) =
                                model.submit(reservation, random_body(n), supplier, now);
                            run.waiters.push(waiter);
                            run.record(step);
                        }
                        _ => {
                            let (waiter, step) = model.wait(reservation, id(n), supplier, now);
                            run.waiters.push(waiter);
                            run.record(step);
                        }
                    }
                }
                7..=9 if !run.jobs.is_empty() => {
                    let job = run.jobs.swap_remove(rng.pick(run.jobs.len()));
                    let outcome = match rng.next(9) {
                        0 => Outcome::Fail {
                            scope: RejectionScope::ExactTip,
                            charge: Charge::Cooldown,
                        },
                        1 => Outcome::Fail {
                            scope: RejectionScope::EffectsChain(ChainReason::DuplicateSpend),
                            charge: Charge::None,
                        },
                        2 if n > 0 => missing(n - 1),
                        3 => Outcome::LocalFault(LocalFault::StateUnavailable),
                        4 => Outcome::AlreadyMined,
                        5 if n > 0 => Outcome::PassUsing(vec![output(n - 1, 0)]),
                        _ => Outcome::Pass,
                    };
                    // The readings are usually current. Sometimes the first
                    // is one generation old. Sometimes the state was ahead of
                    // the core, or changing, and then a chain event is owed:
                    // the core waits for it and it has to come.
                    let (before, after) = match rng.next(8) {
                        0 => (stable(generation - 1.min(generation)), stable(generation)),
                        1 => {
                            chain_event_owed = true;
                            (stable(generation + 1), stable(generation + 1))
                        }
                        2 => {
                            chain_event_owed = true;
                            (stable(generation), GenerationRead::Changing)
                        }
                        3 => (GenerationRead::Changing, GenerationRead::Changing),
                        _ => (stable(generation), stable(generation)),
                    };
                    run.record(model.outcome(job, outcome, before, after, now));
                }
                10 => {
                    generation += 1;
                    tip += 1;
                    let mut change = transition(generation, tip, vec![vec![random_body(n)]]);
                    change.disconnected = usize::try_from(rng.next(3) / 2).expect("small");
                    change.rules_change = rng.next(8) == 0;
                    chain_event_owed = false;
                    run.record(model.transition(change, now));
                }
                11 if rng.next(4) == 0 => {
                    generation += 1;
                    tip += 1;
                    chain_event_owed = false;
                    run.record(model.resync(Height(tip), Generation(generation), now));
                }
                12 => {
                    if let OutputAnswer::Waiting(waiter) = model.await_output(output(n, 0), now) {
                        run.output_waits.push((waiter, output(n, 0)));
                    }
                }
                13 if !run.output_waits.is_empty() => {
                    let pick = rng.pick(run.output_waits.len());
                    let (waiter, _) = run.output_waits.swap_remove(pick);
                    model.cancel_output_wait(waiter);
                }
                _ => run.record(model.tick(now)),
            }
            model.assert_invariants();
        }

        // Drain. The chain event the readings promised arrives. Then callers
        // drop what they still hold and time goes by. Nothing else happens.
        if chain_event_owed {
            generation += 1;
            tip += 1;
            run.record(model.transition(transition(generation, tip, vec![vec![]]), now));
        }
        for (reservation, ..) in granted.drain(..) {
            run.record(model.release(reservation, now));
        }
        // What is visible when the drain starts. On a run where every
        // verification passes and the chain stays put, none of it has a
        // reason to leave. A hidden transaction can: one parked on a parent
        // that never comes is removed when its wait ends.
        let held_at_drain: Vec<UnminedTxId> = model.visible();
        let nothing_should_leave = seed % 3 == 2 && seed % 4 != 3;
        for round in 0..400 {
            // On one run in four the chain keeps changing for a while, with
            // a missed event each round. Everything is checked again each
            // time, and must still be answered in the end.
            let chain_still_moving = seed % 4 == 3 && round < 30;
            if chain_still_moving {
                generation += 1;
                tip += 1;
                run.record(model.resync(Height(tip), Generation(generation), now));
            } else if model.slots_in_use() == 0 && model.hidden().is_empty() && run.jobs.is_empty()
            {
                break;
            }
            for job in std::mem::take(&mut run.jobs) {
                let read = stable(generation);
                // On some runs the verifier fails on this node for a while,
                // or keeps missing a parent, before it passes. A hidden
                // transaction is kept through local faults on purpose, so
                // the faults have to stop for the pool to settle.
                let outcome = match seed % 3 {
                    0 if round < 20 => Outcome::LocalFault(LocalFault::StateUnavailable),
                    1 => missing(11),
                    _ => Outcome::Pass,
                };
                run.record(model.outcome(job, outcome, read, read, now));
            }
            now += secs(61);
            run.record(model.tick(now));
            model.assert_invariants();
        }

        // Only now do callers still waiting for an output give up. Their
        // waits stayed out through the drain, so an admission or reveal
        // that should have answered one did, and was checked. An output
        // that is visible now cannot still have a caller waiting for it.
        for (waiter, outpoint) in std::mem::take(&mut run.output_waits) {
            // Every random body creates output 0, so a visible transaction
            // with this hash offers the output.
            assert!(
                !model
                    .visible()
                    .iter()
                    .any(|visible| visible.mined_id() == outpoint.hash),
                "seed {seed}: a wait was left out though its output is visible"
            );
            model.cancel_output_wait(waiter);
        }
        assert_eq!(
            model.output_waits(),
            0,
            "seed {seed}: an output wait is stuck"
        );
        // Every caller has submitted or dropped what it was granted, so the
        // model remembers no reservation.
        assert_eq!(
            model.caller_held(),
            0,
            "seed {seed}: a reservation is remembered"
        );
        assert!(run.jobs.is_empty(), "seed {seed}: a job never finished");
        // Settling is not allowed to mean throwing things away.
        if nothing_should_leave {
            let visible = model.visible();
            for held in &held_at_drain {
                assert!(
                    visible.contains(held),
                    "seed {seed}: a valid transaction was lost while the pool settled"
                );
            }
        }
        // Nothing is stuck: no slot is held, nothing is hidden, and every
        // submission was answered exactly once, with the pool still on.
        assert_eq!(model.slots_in_use(), 0, "seed {seed}: a slot is stuck");
        assert!(
            model.hidden().is_empty(),
            "seed {seed}: a transaction is stuck hidden"
        );
        run.waiters.sort();
        run.verdicts.sort();
        assert_eq!(run.waiters, run.verdicts, "seed {seed}: verdicts");
    }
}

// Cases from the fifth review.

#[test]
fn a_joined_reservation_keeps_its_id_after_its_admission_ends() {
    let mut model = new_model();
    let (_, job) = bring_in(&mut model, tx(1), legacy(1));
    // Two callers join. Neither is asked for the body: the core has it.
    let waits = model
        .reserve(id(1), peer(2), Intent::Deliver, secs(0))
        .expect("joins");
    assert!(!waits.wants_body);
    model.outcome(job, Outcome::Pass, stable(0), stable(0), secs(1));

    // The admission is over. A wrong body under the old reservation is
    // still a wrong body, as the handle would say, not a stale one.
    let (waiter, step) = model.submit(waits.id, tx(9), Supplier::Peer(legacy(2)), secs(2));
    assert_eq!(
        step.verdict(waiter),
        Some(ModelVerdict::LocalFault(LocalFault::WrongTransaction))
    );

    // The same for a joined caller that was asked for the body.
    let owner = model
        .reserve(id(3), peer(1), Intent::Fetch, secs(2))
        .expect("room");
    let brings = model
        .reserve(id(3), peer(2), Intent::Deliver, secs(2))
        .expect("joins");
    assert!(brings.wants_body);
    let (_, step) = model.submit(owner.id, tx(3), Supplier::Peer(legacy(1)), secs(2));
    model.outcome(the_job(&step), Outcome::Pass, stable(0), stable(0), secs(3));
    let (waiter, step) = model.submit(brings.id, tx(9), Supplier::Peer(legacy(2)), secs(4));
    assert_eq!(
        step.verdict(waiter),
        Some(ModelVerdict::LocalFault(LocalFault::WrongTransaction))
    );
    // With the right body it is a duplicate of what the pool holds.
    let brings = model.reserve(id(3), peer(2), Intent::Deliver, secs(4));
    assert_eq!(brings, Err(ModelUnreserved::Pooled));
}

#[test]
fn a_pooled_transaction_that_fails_its_check_is_remembered_and_charges_nobody() {
    let mut model = new_model();
    admit(&mut model, tx(1), 1, 0);
    let step = model.resync(Height(240), Generation(5), secs(1));
    let step = model.outcome(
        the_job(&step),
        Outcome::Fail {
            scope: RejectionScope::ExactTip,
            charge: Charge::Ban,
        },
        stable(5),
        stable(5),
        secs(2),
    );
    // It passed once, so nobody is charged, and no verdict goes out.
    assert!(step.effects.is_empty());
    assert!(!model.cooling(&legacy(1), secs(2)));
    // The rejection is cached as its scope says, so the next announcement
    // does not fetch and verify it again.
    assert_eq!(
        model.reserve(id(1), peer(2), Intent::Fetch, secs(2)),
        Err(ModelUnreserved::Rejected(RejectionScope::ExactTip))
    );
}

// Cases from the sixth review.

#[test]
fn a_parent_that_is_verified_again_and_again_cannot_hold_its_child_for_ever() {
    let mut model = new_model();
    let (child, job) = bring_in(&mut model, child_of(2, 1), legacy(1));
    model.outcome(job, missing(1), stable(0), stable(0), secs(0));

    // The parent is submitted, its verification never finishes, it times
    // out, and it is submitted again at once.
    let mut now = secs(0);
    let mut answered = None;
    for attempt in 0..4 {
        let reservation = model
            .reserve(id(1), peer(10 + attempt), Intent::Fetch, now)
            .expect("room");
        model.submit(
            reservation.id,
            tx(1),
            Supplier::Peer(legacy(10 + attempt)),
            now,
        );
        now += T_VERIFY;
        let step = model.tick(now);
        if let Some(verdict) = step.verdict(child) {
            answered = Some((now, verdict));
            break;
        }
    }
    // The child is answered when its own wait ends, and its slot comes
    // back.
    assert_eq!(
        answered,
        Some((T_PARK, rejected(RejectionScope::ExactTip, Charge::None)))
    );
    model.assert_invariants();
}

#[test]
fn a_missed_chain_event_past_an_expiry_height_expires_the_transaction() {
    let mut model = new_model();
    let mut expiring = tx(1);
    expiring.expiry = Some(Height(101));
    admit(&mut model, expiring, 1, 0);
    admit(&mut model, tx(2), 2, 0);

    // The core missed the blocks, so no transition told it the height.
    let step = model.resync(Height(101), Generation(3), secs(1));
    assert_eq!(
        step.changes,
        vec![
            Change::Removed {
                id: id(1),
                reason: RemovalReason::Expired
            },
            Change::Hidden(id(2)),
        ]
    );
    model.assert_invariants();
    assert_eq!(
        model.reserve(id(1), peer(3), Intent::Fetch, secs(1)),
        Err(ModelUnreserved::Rejected(RejectionScope::EffectsChain(
            ChainReason::Expired
        )))
    );
}

#[tokio::test]
async fn a_caller_that_stops_waiting_for_an_output_closes_its_reply() {
    let (handle, mut inbox) = pool_port();
    let caller = tokio::spawn(async move { handle.await_output(output(1, 0)).await });
    let Some(PoolCommand::AwaitOutput { reply, .. }) = inbox.next().await else {
        panic!("the wait reaches the core");
    };
    assert!(!reply.is_closed());

    // The caller is cancelled. The core can see that on the reply it
    // holds, which is how it drops the waiter before the limit refuses
    // someone who is still there.
    caller.abort();
    assert!(caller.await.expect_err("aborted").is_cancelled());
    assert!(reply.is_closed());
}

#[test]
fn nothing_expired_gets_in_even_if_a_verification_passes_it() {
    let mut model = new_model();
    let mut expired = tx(1);
    expired.expiry = Some(Height(100));
    let (waiter, job) = bring_in(&mut model, expired, legacy(1));
    // The tip is at the expiry height. A verifier that passed it is wrong,
    // and the pool does not depend on it being right.
    let step = model.outcome(job, Outcome::Pass, stable(0), stable(0), secs(0));
    assert_eq!(
        step.verdict(waiter),
        Some(rejected(
            RejectionScope::EffectsChain(ChainReason::Expired),
            Charge::None
        ))
    );
    assert!(model.visible().is_empty());
    model.assert_invariants();
}

// Cases from the seventh review.

#[test]
fn checking_a_transaction_again_and_again_does_not_extend_its_wait_for_a_parent() {
    let mut model = new_model();
    let (child, job) = bring_in(&mut model, child_of(2, 1), legacy(1));
    model.outcome(job, missing(1), stable(0), stable(0), secs(0));

    // The chain keeps changing. Every twenty seconds a missed event runs
    // the child again, and its check finds the parent missing just before
    // the next one. It is being verified almost all the time.
    let mut now = secs(0);
    let mut answered = None;
    for generation in 1..10u32 {
        let step = model.resync(
            Height(100 + generation),
            Generation(u64::from(generation)),
            now,
        );
        let job = step.started()[0];
        now += secs(19);
        let read = stable(u64::from(generation));
        let step = model.outcome(job, missing(1), read, read, now);
        if let Some(verdict) = step.verdict(child) {
            answered = Some((now, verdict));
            break;
        }
        now += secs(1);
    }
    // The first check after its sixty seconds is its answer.
    assert_eq!(
        answered,
        Some((secs(79), rejected(RejectionScope::ExactTip, Charge::None)))
    );
    assert_eq!(model.slots_in_use(), 0);
}

#[test]
fn a_hidden_transaction_checked_again_and_again_does_not_outlive_its_wait() {
    let mut model = new_model();
    admit(&mut model, child_of(2, 1), 2, 0);
    let mut now = secs(0);
    let mut removed_at = None;
    for generation in 1..10u32 {
        let step = model.resync(Height(100), Generation(u64::from(generation)), now);
        let job = step.started()[0];
        now += secs(19);
        let read = stable(u64::from(generation));
        let step = model.outcome(job, missing(1), read, read, now);
        if !step.changes.is_empty() {
            removed_at = Some(now);
            break;
        }
        now += secs(1);
    }
    assert_eq!(removed_at, Some(secs(79)));
    assert!(model.hidden().is_empty());
}

#[test]
fn a_chain_rejection_recorded_twice_keeps_its_first_place_and_time() {
    let mut model = new_model().with_rejection_memory(secs(100));
    // Transaction 1 is mined at time zero, then transaction 2 at fifty.
    model.transition(transition(1, 101, vec![vec![tx(1)]]), secs(0));
    model.transition(transition(2, 102, vec![vec![tx(2)]]), secs(50));
    // A block names transaction 1 again later. Its entry is not refreshed.
    model.transition(transition(3, 103, vec![vec![tx(1)]]), secs(90));

    let both: HashSet<_> = [id(1), id(2)].into_iter().collect();
    let second: HashSet<_> = [id(2)].into_iter().collect();
    assert_eq!(model.rejected_ids(&both, secs(99)), both);
    // The first entry ends a hundred seconds after it was first made. The
    // one behind it in the order is still remembered.
    assert_eq!(model.rejected_ids(&both, secs(100)), second);
    model.assert_invariants();
}

#[test]
fn a_missed_block_that_mined_an_expired_parent_keeps_its_child() {
    let mut model = new_model();
    let mut parent = tx(1);
    parent.expiry = Some(Height(102));
    admit(&mut model, parent, 1, 0);
    let mut child = child_of(2, 1);
    child.expiry = Some(Height(200));
    let (waiter, job) = bring_in(&mut model, child, legacy(2));
    let step = model.outcome(
        job,
        Outcome::PassUsing(vec![output(1, 0)]),
        stable(0),
        stable(0),
        secs(0),
    );
    assert_eq!(step.verdict(waiter), Some(ModelVerdict::Admitted));

    // The core missed block 101, which mined the parent, and the tip is
    // now past the parent's expiry height. The parent goes as expired. Its
    // output is on the chain, so the child is still valid, and is checked.
    let step = model.resync(Height(103), Generation(4), secs(1));
    assert_eq!(
        step.changes,
        vec![
            Change::Removed {
                id: id(1),
                reason: RemovalReason::Expired
            },
            Change::Hidden(id(2)),
        ]
    );
    let step = model.outcome(the_job(&step), Outcome::Pass, stable(4), stable(4), secs(2));
    assert!(matches!(step.changes[0], Change::Revealed { .. }));
    assert_eq!(model.visible(), vec![id(2)]);
}

#[test]
fn a_parent_that_never_arrives_is_remembered_until_the_tip_changes() {
    let mut model = new_model();
    let (_, job) = bring_in(&mut model, child_of(2, 1), legacy(1));
    model.outcome(job, missing(1), stable(0), stable(0), secs(0));
    model.tick(T_PARK);

    // As today: the same bytes are not fetched and verified again at this
    // tip, and the adapter's rejection query sees it.
    assert_eq!(
        model.reserve(id(2), peer(2), Intent::Fetch, T_PARK),
        Err(ModelUnreserved::Rejected(RejectionScope::ExactTip))
    );
    let asked: HashSet<_> = [id(2)].into_iter().collect();
    assert_eq!(model.rejected_ids(&asked, T_PARK), asked);
    model.transition(transition(1, 101, vec![vec![]]), T_PARK);
    assert!(model.reserve(id(2), peer(2), Intent::Fetch, T_PARK).is_ok());
}

#[test]
fn waiting_for_one_output_and_leaving_many_times_keeps_nothing() {
    let mut model = new_model();
    for _ in 0..1_000 {
        let OutputAnswer::Waiting(waiter) = model.await_output(output(1, 0), secs(0)) else {
            panic!("nothing creates the output");
        };
        model.cancel_output_wait(waiter);
    }
    // One outpoint, far below the limit on distinct outpoints. Nothing is
    // kept for a caller that left.
    assert_eq!(model.output_waiters(), 0);
    assert_eq!(model.output_waits(), 0);
}

// Cases from the eighth review.

#[test]
fn an_expired_hidden_parent_does_not_take_its_child_with_it() {
    let mut model = new_model();
    let mut parent = tx(1);
    parent.expiry = Some(Height(102));
    admit(&mut model, parent, 1, 0);
    let (waiter, job) = bring_in(&mut model, child_of(2, 1), legacy(2));
    let step = model.outcome(
        job,
        Outcome::PassUsing(vec![output(1, 0)]),
        stable(0),
        stable(0),
        secs(0),
    );
    assert_eq!(step.verdict(waiter), Some(ModelVerdict::Admitted));

    // A missed block at 101 mined the parent. The resync is before its
    // expiry height, so both are hidden and waiting to be checked.
    model.resync(Height(101), Generation(1), secs(1));
    assert_eq!(model.hidden(), vec![id(1), id(2)]);

    // The next block reaches the parent's expiry height before its check
    // has answered. The core does not know yet that the parent was mined.
    let step = model.transition(transition(2, 102, vec![vec![]]), secs(2));
    assert_eq!(
        step.changes,
        vec![Change::Removed {
            id: id(1),
            reason: RemovalReason::Expired
        }]
    );
    // The child stays, hidden, and is checked against the chain.
    assert_eq!(model.hidden(), vec![id(2)]);
    model.assert_invariants();
}

#[test]
fn a_visible_expired_parent_still_takes_its_child_with_it() {
    let mut model = new_model();
    let mut parent = tx(1);
    parent.expiry = Some(Height(101));
    admit(&mut model, parent, 1, 0);
    let (_, job) = bring_in(&mut model, child_of(2, 1), legacy(2));
    model.outcome(
        job,
        Outcome::PassUsing(vec![output(1, 0)]),
        stable(0),
        stable(0),
        secs(0),
    );
    // Every block was seen, and none mined the parent. It expired unmined,
    // so its output will never exist.
    let step = model.transition(transition(1, 101, vec![vec![]]), secs(1));
    assert_eq!(step.changes.len(), 2);
    assert!(model.visible().is_empty() && model.hidden().is_empty());
}

#[test]
fn inputs_that_arrive_one_at_a_time_earn_one_check_after_the_wait_and_no_more() {
    let mut model = new_model();
    // The child spends three parents. The first never arrives.
    let child = tx(9)
        .spending(hash(1), 0)
        .spending(hash(2), 0)
        .spending(hash(3), 0);
    let (waiter, job) = bring_in(&mut model, child, legacy(9));
    model.outcome(
        job,
        Outcome::MissingInputs(vec![output(1, 0), output(2, 0), output(3, 0)]),
        stable(0),
        stable(0),
        secs(0),
    );

    // After the wait has ended a block wakes the child, and while it is
    // checked the second parent arrives. That earns one more check.
    let step = model.transition(transition(1, 101, vec![vec![tx(50)]]), secs(59));
    assert!(step.started().is_empty());
    let step = model.resync(Height(102), Generation(2), secs(61));
    let job = step.started()[0];
    admit(&mut model, tx(2), 2, 2);
    let step = model.outcome(
        job,
        Outcome::MissingInputs(vec![output(1, 0), output(2, 0), output(3, 0)]),
        stable(2),
        stable(2),
        secs(62),
    );
    let job = the_job(&step);
    assert_eq!(step.verdict(waiter), None);

    // During that check the third parent arrives too. The first is still
    // missing, and the one extra check is used up.
    admit(&mut model, tx(3), 3, 2);
    let step = model.outcome(
        job,
        Outcome::MissingInputs(vec![output(1, 0), output(3, 0)]),
        stable(2),
        stable(2),
        secs(63),
    );
    assert_eq!(
        step.verdict(waiter),
        Some(rejected(RejectionScope::ExactTip, Charge::None))
    );
    assert!(step.started().is_empty());
}

#[test]
fn a_hidden_transaction_whose_parent_never_returns_is_remembered_too() {
    let mut model = new_model();
    admit(&mut model, child_of(2, 1), 2, 0);
    let step = model.transition(reorg(1, 99), secs(0));
    model.outcome(the_job(&step), missing(1), stable(1), stable(1), secs(0));
    model.tick(T_PARK);
    assert!(model.hidden().is_empty());

    // The same bytes are not fetched again at this tip.
    assert_eq!(
        model.reserve(id(2), peer(3), Intent::Fetch, T_PARK),
        Err(ModelUnreserved::Rejected(RejectionScope::ExactTip))
    );
}

#[test]
fn a_chain_that_changes_during_every_check_cannot_hold_a_slot_for_ever() {
    let mut model = new_model();
    let (waiter, mut job) = bring_in(&mut model, tx(1), legacy(1));

    // Every twenty seconds the chain changes, and each verification spans
    // the change, so no outcome can be trusted and each runs again.
    let mut now = secs(0);
    let mut answered = None;
    for generation in 1..20u64 {
        now += secs(20);
        let step = model.outcome(
            job,
            Outcome::Pass,
            stable(generation - 1),
            stable(generation),
            now,
        );
        if let Some(verdict) = step.verdict(waiter) {
            answered = Some((now, verdict));
            break;
        }
        let step = model.transition(transition(generation, 100, vec![vec![]]), now);
        if let Some(verdict) = step.verdict(waiter) {
            answered = Some((now, verdict));
            break;
        }
        job = step.started()[0];
    }
    // The slot comes back when its own time is up. The core never got an
    // answer it could trust, so this is a local fault and nothing is
    // remembered against the transaction.
    assert_eq!(
        answered,
        Some((
            MAX_UNDECIDED,
            ModelVerdict::LocalFault(LocalFault::VerifyTimeout)
        ))
    );
    assert_eq!(model.slots_in_use(), 0);
    assert!(model.reserve(id(1), peer(2), Intent::Fetch, now).is_ok());
}

#[test]
fn a_slot_is_held_at_most_its_limit_however_the_transaction_is_moved_about() {
    let mut model = new_model();
    let (waiter, job) = bring_in(&mut model, tx(1), legacy(1));
    // The outcome waits for a chain event that the reading promised.
    model.outcome(
        job,
        Outcome::Pass,
        stable(0),
        GenerationRead::Changing,
        secs(1),
    );
    // No event comes. A tick at the limit ends it.
    assert_eq!(model.tick(MAX_UNDECIDED - secs(1)).verdict(waiter), None);
    assert_eq!(
        model.tick(MAX_UNDECIDED).verdict(waiter),
        Some(ModelVerdict::LocalFault(LocalFault::VerifyTimeout))
    );
    assert_eq!(model.slots_in_use(), 0);
}
