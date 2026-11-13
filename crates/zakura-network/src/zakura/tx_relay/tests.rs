//! Cases the reactor has to cover, run on its model with the pool's model
//! behind it. Most come from review findings on the M2 plan.

use std::time::Duration;

use zakura_chain::{
    block::Height,
    transaction::{self, UnminedTxId},
};
use zakura_node_services::mempool::{
    port::{
        model::{ModelTx, ModelUnreserved, ModelVerdict, Outcome, PoolModel, Step, T_VERIFY},
        ChainReason, Change, Charge, Generation, GenerationRead, Intent, LocalFault,
        RejectionScope, Source, Supplier, MAX_PENDING, MAX_PENDING_PER_SOURCE,
    },
    QueueSource,
};

use super::reactor_model::{
    ReactorModel, RelayEffect, SessionId, MAX_ATTEMPTS, MAX_REFETCHES, MAX_REQUESTS_PER_PEER,
    PENDING_PER_PEER, REQUEST_TIMEOUT, RETRY_FIRST, RETRY_MAX,
};

const MAX_TX: u32 = 250_000;

fn id(n: u16) -> UnminedTxId {
    let mut bytes = [0; 32];
    bytes[..2].copy_from_slice(&n.to_le_bytes());
    UnminedTxId::Legacy(transaction::Hash(bytes))
}

fn tx(n: u16) -> ModelTx {
    ModelTx::new(id(n))
}

fn v2(n: u8) -> QueueSource {
    QueueSource::Zakura(vec![n])
}

fn secs(seconds: u64) -> Duration {
    Duration::from_secs(seconds)
}

fn stable() -> GenerationRead {
    GenerationRead::Stable(Generation(0))
}

fn ask(session: SessionId, n: u16) -> RelayEffect {
    RelayEffect::Ask { session, id: id(n) }
}

/// A pool and a reactor with `peers` live sessions, none of them dialed.
/// Session `n` of the returned list belongs to peer `v2(n + 1)`.
fn setup(peers: u8) -> (PoolModel, ReactorModel, Vec<SessionId>) {
    let pool = PoolModel::new(Height(100), Generation(0));
    let mut reactor = ReactorModel::new(MAX_TX);
    let sessions = (1..=peers)
        .map(|peer| reactor.connect(v2(peer), false, MAX_REQUESTS_PER_PEER))
        .collect();
    (pool, reactor, sessions)
}

/// Feeds the outcome of the one job the pool started, and routes what the
/// pool then did back to the reactor.
fn finish(
    pool: &mut PoolModel,
    reactor: &mut ReactorModel,
    outcome: Outcome,
    now: Duration,
) -> (Step, Vec<RelayEffect>) {
    let started = reactor.take_started();
    assert_eq!(started.len(), 1, "exactly one job is running");
    let step = pool.outcome(started[0], outcome, stable(), stable(), now);
    // The verdict's own effects, with no later input to flush them.
    let effects = reactor.pool_step(&step, now, pool);
    (step, effects)
}

/// Fills the core with reservations from legacy peers.
fn fill_core(pool: &mut PoolModel) -> Vec<zakura_node_services::mempool::port::ReservationId> {
    let mut held = Vec::new();
    let peers = u16::try_from(MAX_PENDING / MAX_PENDING_PER_SOURCE).expect("small");
    for p in 0..peers {
        let [high, low] = p.to_be_bytes();
        let source = Source::Peer(QueueSource::LegacySocket(([10, 0, high, low], 8233).into()));
        for n in 0..u16::try_from(MAX_PENDING_PER_SOURCE).expect("small") {
            let reserved = pool
                .reserve(
                    id(10_000 + p * 5 + n),
                    source.clone(),
                    Intent::Fetch,
                    secs(0),
                )
                .expect("the core has room");
            held.push(reserved.id);
        }
    }
    held
}

#[test]
fn an_announced_id_is_reserved_then_asked_for_then_submitted() {
    let (mut pool, mut reactor, s) = setup(1);
    let effects = reactor.announce(s[0], id(1), 300, secs(0), &mut pool);
    assert_eq!(effects, vec![ask(s[0], 1)]);
    // The slot is taken before the request goes out.
    assert_eq!(pool.counted_against(&Source::Peer(v2(1))), 1);
    assert_eq!(pool.bodies_held(), 0);

    reactor.tx(s[0], tx(1), secs(1), &mut pool);
    assert_eq!(pool.bodies_held(), 1);
    assert_eq!(reactor.outstanding(s[0]), 0);
    assert_eq!(reactor.verifying(&v2(1)), 1);

    finish(&mut pool, &mut reactor, Outcome::Pass, secs(2));
    assert!(!reactor.wants(id(1)));
    assert_eq!(reactor.verifying(&v2(1)), 0);
    assert_eq!(pool.visible(), vec![id(1)]);
    reactor.assert_invariants();
}

#[test]
fn nothing_is_requested_while_the_core_is_full() {
    let (mut pool, mut reactor, s) = setup(1);
    let held = fill_core(&mut pool);

    // No slot, so no request: a body is never downloaded to be turned away.
    assert!(reactor
        .announce(s[0], id(1), 300, secs(0), &mut pool)
        .is_empty());
    assert!(reactor.wants(id(1)));
    assert_eq!(reactor.outstanding(s[0]), 0);

    // The node tries again after a delay, not on every tick.
    pool.release(held[0], secs(0));
    assert!(reactor.tick(RETRY_FIRST - secs(1), &mut pool).is_empty());
    assert_eq!(reactor.tick(RETRY_FIRST, &mut pool), vec![ask(s[0], 1)]);
}

#[test]
fn the_retry_delay_doubles_up_to_its_limit() {
    let (mut pool, mut reactor, s) = setup(1);
    fill_core(&mut pool);
    reactor.announce(s[0], id(1), 300, secs(0), &mut pool);

    let mut now = secs(0);
    let mut delay = RETRY_FIRST;
    for _ in 0..8 {
        now += delay;
        assert!(reactor.tick(now - secs(1), &mut pool).is_empty());
        // Still full, so it backs off again, for longer.
        assert!(reactor.tick(now, &mut pool).is_empty());
        delay = (delay * 2).min(RETRY_MAX);
    }
    assert_eq!(delay, RETRY_MAX);
    assert_eq!(reactor.outstanding(s[0]), 0);
}

#[test]
fn a_timeout_asks_the_next_announcer_and_keeps_the_first_request_outstanding() {
    let (mut pool, mut reactor, s) = setup(2);
    reactor.announce(s[0], id(1), 300, secs(0), &mut pool);
    reactor.announce(s[1], id(1), 300, secs(0), &mut pool);

    assert!(reactor
        .tick(REQUEST_TIMEOUT - secs(1), &mut pool)
        .is_empty());
    assert_eq!(reactor.tick(REQUEST_TIMEOUT, &mut pool), vec![ask(s[1], 1)]);
    // The first peer may still answer, so its request slot stays taken.
    assert_eq!(reactor.outstanding(s[0]), 1);
    assert_eq!(reactor.outstanding(s[1]), 1);
    // One slot at the core, still counted against the first peer asked.
    assert_eq!(pool.slots_in_use(), 1);
    assert_eq!(pool.counted_against(&Source::Peer(v2(1))), 1);
    assert_eq!(pool.counted_against(&Source::Peer(v2(2))), 0);
    reactor.assert_invariants();
}

#[test]
fn the_next_announcer_can_answer_while_it_holds_its_own_five_at_the_core() {
    let (mut pool, mut reactor, s) = setup(2);
    // Bob's five slots at the core are taken by his adapter traffic.
    for n in 0..5 {
        pool.reserve(id(100 + n), Source::Peer(v2(2)), Intent::Deliver, secs(0))
            .expect("five fit");
    }
    reactor.announce(s[0], id(1), 300, secs(0), &mut pool);
    reactor.announce(s[1], id(1), 300, secs(0), &mut pool);

    // Alice times out. Bob is asked, though nothing more can be counted
    // against him: the reservation stays Alice's.
    assert_eq!(reactor.tick(REQUEST_TIMEOUT, &mut pool), vec![ask(s[1], 1)]);
    reactor.tx(s[1], tx(1), REQUEST_TIMEOUT + secs(1), &mut pool);
    assert_eq!(pool.counted_against(&Source::Peer(v2(2))), 5);
    pool.assert_invariants();

    // The bytes were Bob's. A ban closes Bob, not Alice.
    let (_, effects) = finish(
        &mut pool,
        &mut reactor,
        Outcome::Fail {
            scope: RejectionScope::ExactTip,
            charge: Charge::Ban,
        },
        REQUEST_TIMEOUT + secs(2),
    );
    assert_eq!(effects, vec![RelayEffect::Close(s[1])]);
}

#[test]
fn the_first_body_wins_and_a_later_one_is_dropped_without_penalty() {
    let (mut pool, mut reactor, s) = setup(2);
    reactor.announce(s[0], id(1), 300, secs(0), &mut pool);
    reactor.announce(s[1], id(1), 300, secs(0), &mut pool);
    reactor.tick(REQUEST_TIMEOUT, &mut pool);

    // The first peer answers late, before the second.
    let late = REQUEST_TIMEOUT + secs(1);
    assert!(reactor.tx(s[0], tx(1), late, &mut pool).is_empty());
    assert_eq!(pool.bodies_held(), 1);
    assert_eq!(reactor.outstanding(s[0]), 0);

    // The second answer is a valid answer to a request this node sent.
    let effects = reactor.tx(s[1], tx(1), late + secs(1), &mut pool);
    assert!(effects.is_empty(), "no violation: {effects:?}");
    assert_eq!(reactor.outstanding(s[1]), 0);
    assert!(!pool.cooling(&v2(2), secs(0)));
    assert_eq!(pool.bodies_held(), 1);
    // Only the body that went to the core counts against its supplier.
    assert_eq!(reactor.verifying(&v2(1)), 1);
    assert_eq!(reactor.verifying(&v2(2)), 0);
}

#[test]
fn a_body_that_arrives_after_the_verdict_is_dropped_without_penalty() {
    let (mut pool, mut reactor, s) = setup(2);
    reactor.announce(s[0], id(1), 300, secs(0), &mut pool);
    reactor.announce(s[1], id(1), 300, secs(0), &mut pool);
    reactor.tick(REQUEST_TIMEOUT, &mut pool);
    reactor.tx(s[1], tx(1), REQUEST_TIMEOUT, &mut pool);
    finish(&mut pool, &mut reactor, Outcome::Pass, REQUEST_TIMEOUT);

    let effects = reactor.tx(s[0], tx(1), REQUEST_TIMEOUT + secs(5), &mut pool);
    assert!(effects.is_empty());
    assert_eq!(pool.bodies_held(), 0);
    assert_eq!(reactor.outstanding(s[0]), 0);
}

#[test]
fn a_silent_peer_holds_only_its_own_five_requests() {
    let (mut pool, mut reactor, s) = setup(2);
    for n in 0..7 {
        reactor.announce(s[0], id(n), 300, secs(0), &mut pool);
    }
    // Five requests are out. The other two ids wait for a slot.
    assert_eq!(reactor.outstanding(s[0]), MAX_REQUESTS_PER_PEER);

    // The peer never answers. Nothing more is asked of it, ever, and the
    // core slots of the five it was asked for are free again.
    let effects = reactor.tick(REQUEST_TIMEOUT, &mut pool);
    assert!(effects.is_empty(), "nobody else announced these");
    assert_eq!(reactor.outstanding(s[0]), MAX_REQUESTS_PER_PEER);
    assert_eq!(pool.slots_in_use(), 0);

    // An honest peer announces one it never got to. It is fetched from that
    // peer.
    let effects = reactor.announce(s[1], id(5), 300, REQUEST_TIMEOUT, &mut pool);
    assert_eq!(effects, vec![ask(s[1], 5)]);
    reactor.assert_invariants();
    pool.assert_invariants();
}

#[test]
fn an_unrequested_body_is_a_violation_and_starts_a_cooldown() {
    let (mut pool, mut reactor, s) = setup(1);
    let effects = reactor.tx(s[0], tx(1), secs(0), &mut pool);
    assert_eq!(effects, vec![RelayEffect::Close(s[0])]);
    // The core heard about it, though no body was submitted.
    assert!(pool.cooling(&v2(1), secs(0)));
    assert_eq!(
        pool.reserve(id(2), Source::Peer(v2(1)), Intent::Fetch, secs(1)),
        Err(ModelUnreserved::CoolingDown)
    );
}

#[test]
fn a_not_found_moves_the_id_on_and_is_never_penalised() {
    let (mut pool, mut reactor, s) = setup(2);
    reactor.announce(s[0], id(1), 300, secs(0), &mut pool);
    reactor.announce(s[1], id(1), 300, secs(0), &mut pool);

    let effects = reactor.not_found(s[0], id(1), secs(1), &mut pool);
    assert_eq!(effects, vec![ask(s[1], 1)]);
    assert_eq!(reactor.outstanding(s[0]), 0);
    assert!(!pool.cooling(&v2(1), secs(0)));

    // One that answers no request is a violation.
    let effects = reactor.not_found(s[0], id(9), secs(2), &mut pool);
    assert_eq!(effects, vec![RelayEffect::Close(s[0])]);
}

#[test]
fn after_the_last_attempt_the_node_gives_up_and_frees_the_core_slot() {
    let (mut pool, mut reactor, s) = setup(4);
    for session in &s {
        reactor.announce(*session, id(1), 300, secs(0), &mut pool);
    }
    let mut now = secs(0);
    for next in s.iter().take(MAX_ATTEMPTS).skip(1) {
        now += REQUEST_TIMEOUT;
        assert_eq!(reactor.tick(now, &mut pool), vec![ask(*next, 1)]);
    }
    assert_eq!(pool.slots_in_use(), 1);

    // The third attempt times out too. The fourth announcer is not asked.
    now += REQUEST_TIMEOUT;
    assert!(reactor.tick(now, &mut pool).is_empty());
    assert!(!reactor.wants(id(1)));
    assert_eq!(pool.slots_in_use(), 0);
}

#[test]
fn a_verdict_ends_the_fetch_for_every_announcer() {
    let (mut pool, mut reactor, s) = setup(3);
    for session in &s {
        reactor.announce(*session, id(1), 300, secs(0), &mut pool);
    }
    reactor.tx(s[0], tx(1), secs(1), &mut pool);
    let (_, effects) = finish(
        &mut pool,
        &mut reactor,
        Outcome::Fail {
            scope: RejectionScope::EffectsChain(ChainReason::DuplicateSpend),
            charge: Charge::None,
        },
        secs(2),
    );
    assert!(effects.is_empty(), "nobody else is asked for a decided id");
    assert!(!reactor.wants(id(1)));

    // A peer that announces it again is not asked: the core remembers.
    assert!(reactor
        .announce(s[1], id(1), 300, secs(3), &mut pool)
        .is_empty());
    assert!(!reactor.wants(id(1)));
}

#[test]
fn a_local_fault_fetches_the_id_again_after_a_delay() {
    let (mut pool, mut reactor, s) = setup(2);
    reactor.announce(s[0], id(1), 300, secs(0), &mut pool);
    reactor.announce(s[1], id(1), 300, secs(0), &mut pool);
    reactor.tx(s[0], tx(1), secs(1), &mut pool);

    let (_, effects) = finish(
        &mut pool,
        &mut reactor,
        Outcome::LocalFault(LocalFault::StateUnavailable),
        secs(2),
    );
    // The transaction may be fine. It is not asked for again at once.
    assert!(effects.is_empty());
    assert!(reactor.wants(id(1)));
    assert_eq!(pool.slots_in_use(), 0);

    let effects = reactor.tick(secs(2) + RETRY_FIRST, &mut pool);
    assert_eq!(effects.len(), 1, "one new request: {effects:?}");
    assert_eq!(pool.slots_in_use(), 1);
    reactor.assert_invariants();
}

#[test]
fn a_verification_that_times_out_is_a_local_fault_for_the_reactor_too() {
    let (mut pool, mut reactor, s) = setup(1);
    reactor.announce(s[0], id(1), 300, secs(0), &mut pool);
    reactor.tx(s[0], tx(1), secs(0), &mut pool);
    reactor.take_started();

    let step = pool.tick(T_VERIFY);
    reactor.pool_step(&step, T_VERIFY, &mut pool);
    assert!(reactor.wants(id(1)));
    assert!(!pool.cooling(&v2(1), secs(0)));
}

#[test]
fn a_session_that_ends_moves_its_ids_on_and_keeps_the_core_slot() {
    let (mut pool, mut reactor, s) = setup(2);
    reactor.announce(s[0], id(1), 300, secs(0), &mut pool);
    reactor.announce(s[1], id(1), 300, secs(0), &mut pool);
    reactor.announce(s[0], id(2), 300, secs(0), &mut pool);

    let effects = reactor.end(s[0], secs(1), &mut pool);
    // Id 1 has another announcer. Id 2 has nobody left.
    assert_eq!(effects, vec![ask(s[1], 1)]);
    assert!(!reactor.wants(id(2)));
    // Id 1's slot is the one taken when the first peer was asked.
    assert_eq!(pool.slots_in_use(), 1);
    assert_eq!(pool.counted_against(&Source::Peer(v2(1))), 1);
    reactor.assert_invariants();
}

#[test]
fn a_peer_this_node_dialed_is_asked_first() {
    let mut pool = PoolModel::new(Height(100), Generation(0));
    let mut reactor = ReactorModel::new(MAX_TX);
    let accepted = reactor.connect(v2(1), false, MAX_REQUESTS_PER_PEER);
    let dialed = reactor.connect(v2(2), true, MAX_REQUESTS_PER_PEER);
    // The core is full while both announce, so the choice is made later
    // with both announcers known.
    let held = fill_core(&mut pool);
    reactor.announce(accepted, id(1), 300, secs(0), &mut pool);
    reactor.announce(dialed, id(1), 300, secs(0), &mut pool);

    pool.release(held[0], secs(0));
    assert_eq!(reactor.tick(RETRY_FIRST, &mut pool), vec![ask(dialed, 1)]);
}

#[test]
fn a_peer_is_never_asked_for_more_than_its_hello_allows() {
    let mut pool = PoolModel::new(Height(100), Generation(0));
    let mut reactor = ReactorModel::new(MAX_TX);
    let session = reactor.connect(v2(1), false, 2);
    for n in 0..4 {
        reactor.announce(session, id(n), 300, secs(0), &mut pool);
    }
    assert_eq!(reactor.outstanding(session), 2);
    reactor.assert_invariants();
}

#[test]
fn an_id_too_large_for_this_node_is_skipped() {
    let (mut pool, mut reactor, s) = setup(1);
    assert!(reactor
        .announce(s[0], id(1), MAX_TX + 1, secs(0), &mut pool)
        .is_empty());
    assert!(!reactor.wants(id(1)));
    assert_eq!(pool.slots_in_use(), 0);
}

#[test]
fn a_full_pending_set_drops_and_counts() {
    let (mut pool, mut reactor, s) = setup(1);
    let total = u16::try_from(PENDING_PER_PEER).expect("fits the test's ids");
    for n in 0..total {
        reactor.announce(s[0], id(n), 300, secs(0), &mut pool);
    }
    assert_eq!(reactor.overflowed, 0);
    reactor.announce(s[0], id(total), 300, secs(0), &mut pool);
    assert_eq!(reactor.overflowed, 1);
    assert!(!reactor.wants(id(total)));
    reactor.assert_invariants();
}

#[test]
fn an_id_another_source_is_bringing_in_is_not_fetched_twice() {
    let (mut pool, mut reactor, s) = setup(1);
    // A legacy peer is already bringing id 1 in.
    let legacy = Source::Peer(QueueSource::LegacySocket(([10, 0, 0, 9], 8233).into()));
    let reserved = pool
        .reserve(id(1), legacy, Intent::Fetch, secs(0))
        .expect("room");

    assert!(reactor
        .announce(s[0], id(1), 300, secs(0), &mut pool)
        .is_empty());
    assert!(reactor.wants(id(1)), "it may still be needed");

    // The legacy fetch fails. The relay fetches it on its next try.
    pool.release(reserved.id, secs(0));
    assert_eq!(reactor.tick(RETRY_FIRST, &mut pool), vec![ask(s[0], 1)]);
    // While the relay is fetching it, nobody else fetches it either.
    assert_eq!(
        pool.reserve(id(1), Source::Node, Intent::Fetch, RETRY_FIRST),
        Err(ModelUnreserved::Pending)
    );
}

// Cases from the second review.

#[test]
fn an_old_session_that_ends_does_not_remove_its_replacement() {
    let (mut pool, mut reactor, s) = setup(1);
    reactor.announce(s[0], id(1), 300, secs(0), &mut pool);
    // The same peer opens a new session before the old one's end arrives.
    let replacement = reactor.connect(v2(1), false, MAX_REQUESTS_PER_PEER);

    reactor.end(s[0], secs(1), &mut pool);
    assert!(!reactor.is_live(s[0]));
    assert!(reactor.is_live(replacement));
    // The old session's request went with it. The new one starts clean.
    assert_eq!(reactor.outstanding(replacement), 0);
    assert!(!reactor.wants(id(1)));

    // An answer on the new session to the old session's request answers
    // nothing this session was asked.
    let effects = reactor.tx(replacement, tx(1), secs(2), &mut pool);
    assert_eq!(effects, vec![RelayEffect::Close(replacement)]);
}

#[test]
fn an_id_announced_again_is_not_asked_twice_on_one_session() {
    let (mut pool, mut reactor, s) = setup(1);
    reactor.announce(s[0], id(1), 300, secs(0), &mut pool);
    // The only announcer times out. The node gives up on the id, and the
    // request stays outstanding.
    reactor.tick(REQUEST_TIMEOUT, &mut pool);
    assert!(!reactor.wants(id(1)));
    assert_eq!(reactor.outstanding(s[0]), 1);

    // The peer announces it again. A second request for the same id would
    // break the wire rule and make the two answers indistinguishable.
    let effects = reactor.announce(s[0], id(1), 300, REQUEST_TIMEOUT, &mut pool);
    assert!(effects.is_empty());
    assert!(reactor.wants(id(1)));
    reactor.assert_invariants();

    // The answer to the first request is still good, and is delivered.
    let effects = reactor.tx(s[0], tx(1), REQUEST_TIMEOUT + secs(1), &mut pool);
    assert!(effects.is_empty());
    assert_eq!(pool.bodies_held(), 1);
}

#[test]
fn a_refetch_after_a_local_fault_skips_a_session_that_still_owes_an_answer() {
    let (mut pool, mut reactor, s) = setup(2);
    reactor.announce(s[0], id(1), 300, secs(0), &mut pool);
    reactor.announce(s[1], id(1), 300, secs(0), &mut pool);
    // Alice times out. Bob supplies the body, and it hits a local fault.
    reactor.tick(REQUEST_TIMEOUT, &mut pool);
    reactor.tx(s[1], tx(1), REQUEST_TIMEOUT, &mut pool);
    finish(
        &mut pool,
        &mut reactor,
        Outcome::LocalFault(LocalFault::StateUnavailable),
        REQUEST_TIMEOUT,
    );

    // Both are eligible again in the new fetch, but Alice still has the
    // first request outstanding. Bob is asked.
    let effects = reactor.tick(REQUEST_TIMEOUT + RETRY_FIRST, &mut pool);
    assert_eq!(effects, vec![ask(s[1], 1)]);
    assert_eq!(reactor.outstanding(s[0]), 1);
    reactor.assert_invariants();
}

#[test]
fn local_faults_do_not_fetch_one_id_for_ever() {
    let (mut pool, mut reactor, s) = setup(1);
    reactor.announce(s[0], id(1), 300, secs(0), &mut pool);
    let mut now = secs(0);
    for _ in 0..=MAX_REFETCHES {
        assert!(reactor.wants(id(1)));
        reactor.tx(s[0], tx(1), now, &mut pool);
        finish(
            &mut pool,
            &mut reactor,
            Outcome::LocalFault(LocalFault::StateUnavailable),
            now,
        );
        reactor.assert_invariants();
        now += RETRY_MAX;
        reactor.tick(now, &mut pool);
    }
    assert!(!reactor.wants(id(1)));
    assert_eq!(pool.slots_in_use(), 0);
}

#[test]
fn a_peer_has_at_most_five_ids_requested_or_awaiting_a_verdict() {
    let (mut pool, mut reactor, s) = setup(6);
    let bob = s[5];
    // Five peers each announce one id first, and Bob announces all five.
    for n in 0..5 {
        reactor.announce(s[usize::from(n)], id(n), 300, secs(0), &mut pool);
        reactor.announce(bob, id(n), 300, secs(0), &mut pool);
    }
    // They all time out, so Bob is asked for the five. Their core slots
    // stay counted against the first peers asked, not against Bob.
    let effects = reactor.tick(REQUEST_TIMEOUT, &mut pool);
    assert_eq!(effects.len(), 5);
    assert_eq!(pool.counted_against(&Source::Peer(v2(6))), 0);

    // Bob answers all five. None of his requests is outstanding now.
    for n in 0..5 {
        reactor.tx(bob, tx(n), REQUEST_TIMEOUT, &mut pool);
    }
    assert_eq!(reactor.outstanding(bob), 0);
    assert_eq!(reactor.verifying(&v2(6)), 5);

    // The core would count a sixth against Bob. The reactor does not ask,
    // because five of his bodies are still awaiting a verdict.
    let effects = reactor.announce(bob, id(9), 300, REQUEST_TIMEOUT, &mut pool);
    assert!(effects.is_empty());
    reactor.assert_invariants();
}

#[test]
fn a_supplier_that_is_cooling_down_does_not_strand_the_id() {
    let (mut pool, mut reactor, s) = setup(2);
    reactor.announce(s[0], id(1), 300, secs(0), &mut pool);
    reactor.announce(s[1], id(1), 300, secs(0), &mut pool);
    // Alice does not have it. Bob is asked, then enters a cooldown.
    reactor.not_found(s[0], id(1), secs(1), &mut pool);
    pool.violation(v2(2), secs(1));

    // Bob's body is not verified. Nobody is left to ask, so the node gives
    // up on the id instead of keeping it with nothing to wait for.
    let effects = reactor.tx(s[1], tx(1), secs(2), &mut pool);
    assert!(effects.is_empty());
    assert!(!reactor.wants(id(1)));
    assert_eq!(reactor.verifying(&v2(2)), 0);
    assert_eq!(pool.slots_in_use(), 0);
    reactor.assert_invariants();
}

#[test]
fn giving_up_passes_on_the_verdict_it_causes_for_another_caller() {
    let (mut pool, mut reactor, s) = setup(1);
    reactor.announce(s[0], id(1), 300, secs(0), &mut pool);
    reactor.take_pool_steps();

    // Two legacy callers hold the same transaction. The first is asked for
    // the body and is cancelled. The second waits for its verdict.
    let legacy = |n: u8| QueueSource::LegacySocket(([10, 0, 0, n], 8233).into());
    let bringer = pool
        .reserve(id(1), Source::Peer(legacy(1)), Intent::Deliver, secs(0))
        .expect("joins the relay's reservation");
    let waiting = pool
        .reserve(id(1), Source::Peer(legacy(2)), Intent::Deliver, secs(0))
        .expect("joins");
    let (waiter, _) = pool.wait(waiting.id, id(1), Supplier::Peer(legacy(2)), secs(0));
    pool.release(bringer.id, secs(0));

    // The relay's only announcer times out, and the relay gives up. Nobody
    // can bring the body now. The waiting caller's answer is not lost.
    reactor.tick(REQUEST_TIMEOUT, &mut pool);
    let verdict = reactor
        .take_pool_steps()
        .iter()
        .find_map(|step| step.verdict(waiter));
    assert_eq!(verdict, Some(ModelVerdict::LocalFault(LocalFault::NoBody)));
}

// Cases from Sol's second review.

#[test]
fn a_handoff_skips_an_announcer_that_is_cooling_down() {
    let (mut pool, mut reactor, s) = setup(3);
    for session in &s {
        reactor.announce(*session, id(1), 300, secs(0), &mut pool);
    }
    // Bob enters a cooldown for something else while Alice is being asked.
    pool.violation(v2(2), secs(1));

    // Alice times out. The slot is hers, so the core is not asked again,
    // and it would refuse Bob's body. Bob is not asked. Carol is.
    let effects = reactor.tick(REQUEST_TIMEOUT, &mut pool);
    assert_eq!(effects, vec![ask(s[2], 1)]);
    assert_eq!(reactor.outstanding(s[1]), 0);
}

#[test]
fn a_local_fault_after_the_last_session_ended_does_not_leave_the_id_wanted() {
    let (mut pool, mut reactor, s) = setup(1);
    reactor.announce(s[0], id(1), 300, secs(0), &mut pool);
    reactor.tx(s[0], tx(1), secs(0), &mut pool);
    // The session ends while its body is verified. The body stays.
    reactor.end(s[0], secs(1), &mut pool);
    assert!(reactor.wants(id(1)));

    finish(
        &mut pool,
        &mut reactor,
        Outcome::LocalFault(LocalFault::StateUnavailable),
        secs(2),
    );
    // Nobody is left to ask, now or after the retry delay.
    reactor.tick(secs(2) + RETRY_MAX, &mut pool);
    assert!(!reactor.wants(id(1)));
    assert_eq!(pool.slots_in_use(), 0);
}

// Cases from the third review.

#[test]
fn a_peer_keeps_its_five_across_a_session_that_ends_and_one_that_replaces_it() {
    let (mut pool, mut reactor, s) = setup(6);
    let bob = s[5];
    for n in 0..5 {
        reactor.announce(s[usize::from(n)], id(n), 300, secs(0), &mut pool);
        reactor.announce(bob, id(n), 300, secs(0), &mut pool);
    }
    // Bob supplies five bodies under other peers' slots, then ends his
    // session while they are verified and opens another.
    reactor.tick(REQUEST_TIMEOUT, &mut pool);
    for n in 0..5 {
        reactor.tx(bob, tx(n), REQUEST_TIMEOUT, &mut pool);
    }
    reactor.end(bob, REQUEST_TIMEOUT, &mut pool);
    let again = reactor.connect(v2(6), false, MAX_REQUESTS_PER_PEER);
    assert_eq!(reactor.verifying(&v2(6)), 5);

    // The five are still his. The new session is asked for nothing.
    let effects = reactor.announce(again, id(9), 300, REQUEST_TIMEOUT, &mut pool);
    assert!(effects.is_empty());
    assert_eq!(reactor.in_use(&v2(6)), 5);
    reactor.assert_invariants();
}

#[test]
fn two_live_sessions_of_one_peer_share_its_five() {
    let (mut pool, mut reactor, s) = setup(1);
    let second = reactor.connect(v2(1), false, MAX_REQUESTS_PER_PEER);
    for n in 0..5 {
        reactor.announce(s[0], id(n), 300, secs(0), &mut pool);
    }
    assert_eq!(reactor.outstanding(s[0]), 5);

    // The replacement is live while the old session's requests are out.
    let effects = reactor.announce(second, id(9), 300, secs(0), &mut pool);
    assert!(effects.is_empty());
    assert_eq!(reactor.in_use(&v2(1)), 5);
    reactor.assert_invariants();
}

#[test]
fn a_ban_reaches_the_peer_after_its_session_was_replaced() {
    let (mut pool, mut reactor, s) = setup(1);
    reactor.announce(s[0], id(1), 300, secs(0), &mut pool);
    reactor.tx(s[0], tx(1), secs(0), &mut pool);
    // The peer ends the session that sent the body and opens another
    // before the verdict.
    reactor.end(s[0], secs(1), &mut pool);
    let replacement = reactor.connect(v2(1), false, MAX_REQUESTS_PER_PEER);

    let (_, effects) = finish(
        &mut pool,
        &mut reactor,
        Outcome::Fail {
            scope: RejectionScope::ExactTip,
            charge: Charge::Ban,
        },
        secs(2),
    );
    assert_eq!(effects, vec![RelayEffect::Close(replacement)]);
}

#[test]
fn a_peer_is_asked_again_once_its_cooldown_has_ended() {
    let (mut pool, mut reactor, s) = setup(1);
    pool.violation(v2(1), secs(0));
    assert!(reactor
        .announce(s[0], id(1), 300, secs(1), &mut pool)
        .is_empty());

    // Only the reactor's clock moves. Nothing else touches the pool.
    let mut now = secs(1);
    let mut asked = Vec::new();
    while asked.is_empty() && now < secs(1_000) {
        now += RETRY_MAX;
        asked = reactor.tick(now, &mut pool);
    }
    assert_eq!(asked, vec![ask(s[0], 1)]);
    assert!(now >= secs(600), "not before the cooldown ended");
}

#[test]
fn abandoned_requests_free_their_core_slots_and_stay_bounded_on_each_session() {
    let (mut pool, mut reactor, s) = setup(3);
    // Every peer is asked for five ids only it announced, and none answers.
    for (peer, session) in s.iter().enumerate() {
        for n in 0..5 {
            let n = u16::try_from(peer * 10 + n).expect("small");
            reactor.announce(*session, id(n), 300, secs(0), &mut pool);
        }
    }
    assert_eq!(pool.slots_in_use(), 15);
    reactor.tick(REQUEST_TIMEOUT, &mut pool);

    // The node stopped waiting, so the core is free for others. Peers that
    // never answer cannot fill it.
    assert_eq!(pool.slots_in_use(), 0);
    // Each session still has its five requests out, and is asked nothing
    // more. That is the bound on late bodies.
    for session in &s {
        assert_eq!(reactor.outstanding(*session), MAX_REQUESTS_PER_PEER);
        assert!(reactor
            .announce(*session, id(99), 300, REQUEST_TIMEOUT, &mut pool)
            .is_empty());
    }
    // A late body is dropped before it reaches the core.
    reactor.tx(s[0], tx(0), REQUEST_TIMEOUT + secs(1), &mut pool);
    assert_eq!(pool.bodies_held(), 0);
    reactor.assert_invariants();
}

// Cases from the fourth review.

#[test]
fn a_verdict_frees_a_request_slot_and_the_next_id_is_asked_at_once() {
    let (mut pool, mut reactor, s) = setup(1);
    for n in 0..6 {
        reactor.announce(s[0], id(n), 300, secs(0), &mut pool);
    }
    // Five answered and awaiting verdicts. The sixth id waits for room.
    for n in 0..5 {
        reactor.tx(s[0], tx(n), secs(0), &mut pool);
    }
    let started = reactor.take_started();
    assert_eq!(reactor.in_use(&v2(1)), 5);

    // The verdict itself asks for the sixth. No tick is needed.
    let step = pool.outcome(started[0], Outcome::Pass, stable(), stable(), secs(1));
    let effects = reactor.pool_step(&step, secs(1), &mut pool);
    assert_eq!(effects, vec![ask(s[0], 5)]);
}

#[test]
fn a_late_body_for_an_id_that_was_announced_again_is_delivered() {
    let (mut pool, mut reactor, s) = setup(2);
    reactor.announce(s[0], id(1), 300, secs(0), &mut pool);
    reactor.tick(REQUEST_TIMEOUT, &mut pool);
    assert!(!reactor.wants(id(1)));

    // Another peer announces it, and is asked. The node wants it again.
    let effects = reactor.announce(s[1], id(1), 300, REQUEST_TIMEOUT, &mut pool);
    assert_eq!(effects, vec![ask(s[1], 1)]);

    // The first peer's old answer arrives. It answers a request this node
    // sent, for an id it wants, and goes in under the new slot.
    let effects = reactor.tx(s[0], tx(1), REQUEST_TIMEOUT + secs(1), &mut pool);
    assert!(effects.is_empty());
    assert_eq!(pool.bodies_held(), 1);
    assert_eq!(pool.slots_in_use(), 1);
    assert_eq!(reactor.verifying(&v2(1)), 1);
    reactor.assert_invariants();
    pool.assert_invariants();
}

#[test]
fn two_live_sessions_of_one_peer_share_its_pending_bound() {
    let (mut pool, mut reactor, s) = setup(1);
    let second = reactor.connect(v2(1), false, MAX_REQUESTS_PER_PEER);
    let total = u16::try_from(PENDING_PER_PEER).expect("fits the test's ids");
    for n in 0..total {
        reactor.announce(s[0], id(n), 300, secs(0), &mut pool);
    }
    // The peer's bound is used up. Its other session adds nothing.
    reactor.announce(second, id(total), 300, secs(0), &mut pool);
    assert_eq!(reactor.overflowed, 1);
    assert_eq!(reactor.pending(&v2(1)), PENDING_PER_PEER);
    reactor.assert_invariants();
}

#[test]
fn a_fetch_every_peer_answered_without_a_body_is_a_failed_fetch() {
    let (mut pool, mut reactor, s) = setup(2);
    reactor.announce(s[0], id(1), 300, secs(0), &mut pool);
    reactor.announce(s[1], id(1), 300, secs(0), &mut pool);
    reactor.take_pool_steps();

    // Both peers answer that they do not have it. Nothing is still out, so
    // the fetch failed, and the core logs that as a failed download.
    reactor.not_found(s[0], id(1), secs(1), &mut pool);
    reactor.not_found(s[1], id(1), secs(2), &mut pool);
    let logged: Vec<Change> = reactor
        .take_pool_steps()
        .into_iter()
        .flat_map(|step| step.changes)
        .collect();
    assert_eq!(logged, vec![Change::Refused(id(1))]);

    // A peer that only went quiet may still answer. That is not a failed
    // fetch, and nothing is logged.
    reactor.announce(s[0], id(2), 300, secs(3), &mut pool);
    reactor.take_pool_steps();
    reactor.tick(secs(3) + REQUEST_TIMEOUT, &mut pool);
    assert!(!reactor.wants(id(2)));
    assert!(reactor
        .take_pool_steps()
        .iter()
        .all(|step| step.changes.is_empty()));
}

/// A small deterministic generator, so the run replays exactly.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self, bound: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % bound
    }

    fn pick(&mut self, len: usize) -> usize {
        let bound = u64::try_from(len).expect("a test list fits in u64");
        usize::try_from(self.next(bound)).expect("an index below a usize fits in usize")
    }
}

#[test]
fn a_random_run_always_drains_once_the_sessions_end() {
    for seed in 0..300 {
        let mut rng = Lcg(seed);
        // A small core, so the run reaches a full one.
        let mut pool = PoolModel::new(Height(100), Generation(0)).with_limits(6, 2, 3);
        let mut reactor = ReactorModel::new(MAX_TX);
        let mut now = secs(0);
        let mut live: Vec<SessionId> = Vec::new();
        // Requests on the wire: the session and the id.
        let mut asked: Vec<(SessionId, u16)> = Vec::new();
        let mut jobs = Vec::new();

        let apply = |effects: Vec<RelayEffect>,
                     reactor: &mut ReactorModel,
                     pool: &mut PoolModel,
                     live: &mut Vec<SessionId>,
                     asked: &mut Vec<(SessionId, u16)>,
                     now: Duration| {
            let mut pending = effects;
            while let Some(effect) = pending.pop() {
                match effect {
                    RelayEffect::Ask {
                        session,
                        id: asked_id,
                    } => {
                        let n = (0..16u16).find(|n| id(*n) == asked_id).expect("a test id");
                        asked.push((session, n));
                    }
                    // A closed connection ends its session.
                    RelayEffect::Close(session) => {
                        live.retain(|s| *s != session);
                        asked.retain(|(s, _)| *s != session);
                        pending.extend(reactor.end(session, now, pool));
                    }
                }
            }
        };

        for _ in 0..400 {
            now += Duration::from_millis(rng.next(4_000));
            let n = u16::try_from(rng.next(16)).expect("small");
            let peer = u8::try_from(rng.next(3) + 1).expect("small");
            let effects = match rng.next(12) {
                0 => {
                    let served = usize::try_from(rng.next(6) + 1).expect("small");
                    live.push(reactor.connect(v2(peer), rng.next(2) == 0, served));
                    Vec::new()
                }
                1 if !live.is_empty() => {
                    let session = live.swap_remove(rng.pick(live.len()));
                    asked.retain(|(s, _)| *s != session);
                    reactor.end(session, now, &mut pool)
                }
                2..=5 if !live.is_empty() => {
                    let session = live[rng.pick(live.len())];
                    reactor.announce(session, id(n), 300, now, &mut pool)
                }
                6..=7 if !asked.is_empty() => {
                    let (session, n) = asked.swap_remove(rng.pick(asked.len()));
                    if rng.next(4) == 0 {
                        reactor.not_found(session, id(n), now, &mut pool)
                    } else {
                        reactor.tx(session, tx(n), now, &mut pool)
                    }
                }
                // A body nobody asked for.
                8 if !live.is_empty() && rng.next(6) == 0 => {
                    let session = live[rng.pick(live.len())];
                    if asked.contains(&(session, n)) {
                        Vec::new()
                    } else {
                        reactor.tx(session, tx(n), now, &mut pool)
                    }
                }
                9..=10 => {
                    jobs.extend(reactor.take_started());
                    if jobs.is_empty() {
                        Vec::new()
                    } else {
                        let job = jobs.swap_remove(rng.pick(jobs.len()));
                        let outcome = match rng.next(5) {
                            0 => Outcome::Fail {
                                scope: RejectionScope::ExactTip,
                                charge: Charge::Ban,
                            },
                            1 => Outcome::Fail {
                                scope: RejectionScope::ExactTip,
                                charge: Charge::Cooldown,
                            },
                            2 => Outcome::LocalFault(LocalFault::StateUnavailable),
                            _ => Outcome::Pass,
                        };
                        let step = pool.outcome(job, outcome, stable(), stable(), now);
                        reactor.pool_step(&step, now, &mut pool)
                    }
                }
                _ => {
                    let step = pool.tick(now);
                    let mut effects = reactor.pool_step(&step, now, &mut pool);
                    effects.extend(reactor.tick(now, &mut pool));
                    effects
                }
            };
            apply(effects, &mut reactor, &mut pool, &mut live, &mut asked, now);
            reactor.assert_invariants();
            pool.assert_invariants();
        }

        // Drain. Every session ends and time goes by. The verifications
        // still running end in a local fault on some runs and pass on the
        // others, because a fault after the last session ended is its own
        // way to get stuck.
        for session in live.drain(..) {
            reactor.end(session, now, &mut pool);
        }
        for _ in 0..200 {
            jobs.extend(reactor.take_started());
            if reactor.wanted_count() == 0 && pool.slots_in_use() == 0 && jobs.is_empty() {
                break;
            }
            for job in std::mem::take(&mut jobs) {
                let outcome = if seed % 2 == 0 {
                    Outcome::LocalFault(LocalFault::StateUnavailable)
                } else {
                    Outcome::Pass
                };
                let step = pool.outcome(job, outcome, stable(), stable(), now);
                reactor.pool_step(&step, now, &mut pool);
            }
            now += RETRY_MAX + T_VERIFY;
            let step = pool.tick(now);
            reactor.pool_step(&step, now, &mut pool);
            reactor.tick(now, &mut pool);
            reactor.assert_invariants();
        }

        // Nothing is left wanted with nobody to ask, and the reactor holds
        // no slot at the core.
        assert_eq!(reactor.wanted_count(), 0, "seed {seed}: an id is stuck");
        assert_eq!(pool.slots_in_use(), 0, "seed {seed}: a core slot is stuck");
    }
}
