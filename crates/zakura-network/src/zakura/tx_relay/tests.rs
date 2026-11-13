//! Cases the reactor has to cover, run on its model with the pool's model
//! behind it. Most come from review findings on the M2 plan.

use std::time::Duration;

use zakura_chain::{
    block::Height,
    transaction::{self, UnminedTxId},
};
use zakura_node_services::mempool::{
    port::{
        model::{ModelTx, ModelUnreserved, Outcome, PoolModel, Step, T_VERIFY},
        Charge, Generation, GenerationRead, Intent, LocalFault, RejectionScope, Source,
        MAX_PENDING, MAX_PENDING_PER_SOURCE,
    },
    QueueSource,
};

use super::reactor_model::{
    ReactorModel, RelayEffect, MAX_ATTEMPTS, MAX_REQUESTS_PER_PEER, PENDING_PER_PEER,
    REQUEST_TIMEOUT, RETRY_FIRST, RETRY_MAX,
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

fn ask(peer: u8, n: u16) -> RelayEffect {
    RelayEffect::Ask {
        peer: v2(peer),
        id: id(n),
    }
}

/// A pool and a reactor with `peers` live sessions, none of them dialed.
fn setup(peers: u8) -> (PoolModel, ReactorModel) {
    let pool = PoolModel::new(Height(100), Generation(0));
    let mut reactor = ReactorModel::new(MAX_TX);
    for peer in 1..=peers {
        reactor.connect(v2(peer), false, MAX_REQUESTS_PER_PEER);
    }
    (pool, reactor)
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
    reactor.pool_step(&step, now, pool);
    let effects = reactor.tick(now, pool);
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
    let (mut pool, mut reactor) = setup(1);
    let effects = reactor.announce(&v2(1), id(1), 300, secs(0), &mut pool);
    assert_eq!(effects, vec![ask(1, 1)]);
    // The slot is taken before the request goes out.
    assert_eq!(pool.counted_against(&Source::Peer(v2(1))), 1);
    assert_eq!(pool.bodies_held(), 0);

    reactor.tx(&v2(1), tx(1), secs(1), &mut pool);
    assert_eq!(pool.bodies_held(), 1);
    assert_eq!(reactor.outstanding(&v2(1)), 0);

    finish(&mut pool, &mut reactor, Outcome::Pass, secs(2));
    assert!(!reactor.wants(id(1)));
    assert_eq!(pool.visible(), vec![id(1)]);
    reactor.assert_invariants();
}

#[test]
fn nothing_is_requested_while_the_core_is_full() {
    let (mut pool, mut reactor) = setup(1);
    let held = fill_core(&mut pool);

    // No slot, so no request: a body is never downloaded to be turned away.
    assert!(reactor
        .announce(&v2(1), id(1), 300, secs(0), &mut pool)
        .is_empty());
    assert!(reactor.wants(id(1)));
    assert_eq!(reactor.outstanding(&v2(1)), 0);

    // The node tries again after a delay, not on every tick.
    pool.release(held[0], secs(0));
    assert!(reactor.tick(RETRY_FIRST - secs(1), &mut pool).is_empty());
    assert_eq!(reactor.tick(RETRY_FIRST, &mut pool), vec![ask(1, 1)]);
}

#[test]
fn the_retry_delay_doubles_up_to_its_limit() {
    let (mut pool, mut reactor) = setup(1);
    fill_core(&mut pool);
    reactor.announce(&v2(1), id(1), 300, secs(0), &mut pool);

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
    assert_eq!(reactor.outstanding(&v2(1)), 0);
}

#[test]
fn a_timeout_asks_the_next_announcer_and_keeps_the_first_request_outstanding() {
    let (mut pool, mut reactor) = setup(2);
    reactor.announce(&v2(1), id(1), 300, secs(0), &mut pool);
    reactor.announce(&v2(2), id(1), 300, secs(0), &mut pool);

    assert!(reactor
        .tick(REQUEST_TIMEOUT - secs(1), &mut pool)
        .is_empty());
    assert_eq!(reactor.tick(REQUEST_TIMEOUT, &mut pool), vec![ask(2, 1)]);
    // The first peer may still answer, so its request slot stays taken.
    assert_eq!(reactor.outstanding(&v2(1)), 1);
    assert_eq!(reactor.outstanding(&v2(2)), 1);
    // One slot at the core, still counted against the first peer asked.
    assert_eq!(pool.slots_in_use(), 1);
    assert_eq!(pool.counted_against(&Source::Peer(v2(1))), 1);
    assert_eq!(pool.counted_against(&Source::Peer(v2(2))), 0);
    reactor.assert_invariants();
}

#[test]
fn the_next_announcer_can_answer_while_it_holds_its_own_five_at_the_core() {
    let (mut pool, mut reactor) = setup(2);
    // Bob's five slots at the core are taken by his adapter traffic.
    for n in 0..5 {
        pool.reserve(id(100 + n), Source::Peer(v2(2)), Intent::Deliver, secs(0))
            .expect("five fit");
    }
    reactor.announce(&v2(1), id(1), 300, secs(0), &mut pool);
    reactor.announce(&v2(2), id(1), 300, secs(0), &mut pool);

    // Alice times out. Bob is asked, though nothing more can be counted
    // against him: the reservation stays Alice's.
    assert_eq!(reactor.tick(REQUEST_TIMEOUT, &mut pool), vec![ask(2, 1)]);
    reactor.tx(&v2(2), tx(1), REQUEST_TIMEOUT + secs(1), &mut pool);
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
    assert_eq!(effects, vec![RelayEffect::Close(v2(2))]);
}

#[test]
fn the_first_body_wins_and_a_later_one_is_dropped_without_penalty() {
    let (mut pool, mut reactor) = setup(2);
    reactor.announce(&v2(1), id(1), 300, secs(0), &mut pool);
    reactor.announce(&v2(2), id(1), 300, secs(0), &mut pool);
    reactor.tick(REQUEST_TIMEOUT, &mut pool);

    // The first peer answers late, before the second.
    let late = REQUEST_TIMEOUT + secs(1);
    assert!(reactor.tx(&v2(1), tx(1), late, &mut pool).is_empty());
    assert_eq!(pool.bodies_held(), 1);
    assert_eq!(reactor.outstanding(&v2(1)), 0);

    // The second answer is a valid answer to a request this node sent.
    let effects = reactor.tx(&v2(2), tx(1), late + secs(1), &mut pool);
    assert!(effects.is_empty(), "no violation: {effects:?}");
    assert_eq!(reactor.outstanding(&v2(2)), 0);
    assert!(!pool.cooling(&v2(2)));
    assert_eq!(pool.bodies_held(), 1);
}

#[test]
fn a_body_that_arrives_after_the_verdict_is_dropped_without_penalty() {
    let (mut pool, mut reactor) = setup(2);
    reactor.announce(&v2(1), id(1), 300, secs(0), &mut pool);
    reactor.announce(&v2(2), id(1), 300, secs(0), &mut pool);
    reactor.tick(REQUEST_TIMEOUT, &mut pool);
    reactor.tx(&v2(2), tx(1), REQUEST_TIMEOUT, &mut pool);
    finish(&mut pool, &mut reactor, Outcome::Pass, REQUEST_TIMEOUT);

    let effects = reactor.tx(&v2(1), tx(1), REQUEST_TIMEOUT + secs(5), &mut pool);
    assert!(effects.is_empty());
    assert_eq!(pool.bodies_held(), 0);
    assert_eq!(reactor.outstanding(&v2(1)), 0);
}

#[test]
fn a_silent_peer_holds_only_its_own_five_requests() {
    let (mut pool, mut reactor) = setup(2);
    for n in 0..7 {
        reactor.announce(&v2(1), id(n), 300, secs(0), &mut pool);
    }
    // Five requests are out. The other two ids wait for a slot.
    assert_eq!(reactor.outstanding(&v2(1)), MAX_REQUESTS_PER_PEER);

    // The peer never answers. Nothing more is asked of it, ever.
    let effects = reactor.tick(REQUEST_TIMEOUT, &mut pool);
    assert!(effects.is_empty(), "nobody else announced these");
    assert_eq!(reactor.outstanding(&v2(1)), MAX_REQUESTS_PER_PEER);

    // An honest peer announces the two it never got to. They are fetched
    // from that peer.
    let effects = reactor.announce(&v2(2), id(5), 300, REQUEST_TIMEOUT, &mut pool);
    assert_eq!(effects, vec![ask(2, 5)]);
    reactor.assert_invariants();
    pool.assert_invariants();
}

#[test]
fn an_unrequested_body_is_a_violation_and_starts_a_cooldown() {
    let (mut pool, mut reactor) = setup(1);
    let effects = reactor.tx(&v2(1), tx(1), secs(0), &mut pool);
    assert_eq!(effects, vec![RelayEffect::Close(v2(1))]);
    // The core heard about it, though no body was submitted.
    assert!(pool.cooling(&v2(1)));
    assert_eq!(
        pool.reserve(id(2), Source::Peer(v2(1)), Intent::Fetch, secs(1)),
        Err(ModelUnreserved::CoolingDown)
    );
}

#[test]
fn a_not_found_moves_the_id_on_and_is_never_penalised() {
    let (mut pool, mut reactor) = setup(2);
    reactor.announce(&v2(1), id(1), 300, secs(0), &mut pool);
    reactor.announce(&v2(2), id(1), 300, secs(0), &mut pool);

    let effects = reactor.not_found(&v2(1), id(1), secs(1), &mut pool);
    assert_eq!(effects, vec![ask(2, 1)]);
    assert_eq!(reactor.outstanding(&v2(1)), 0);
    assert!(!pool.cooling(&v2(1)));

    // One that answers no request is a violation.
    let effects = reactor.not_found(&v2(1), id(9), secs(2), &mut pool);
    assert_eq!(effects, vec![RelayEffect::Close(v2(1))]);
}

#[test]
fn after_the_last_announcer_the_node_gives_up_and_frees_the_core_slot() {
    let (mut pool, mut reactor) = setup(4);
    for peer in 1..=4 {
        reactor.announce(&v2(peer), id(1), 300, secs(0), &mut pool);
    }
    let mut now = secs(0);
    for attempt in 1..MAX_ATTEMPTS {
        now += REQUEST_TIMEOUT;
        let peer = u8::try_from(attempt + 1).expect("small");
        assert_eq!(reactor.tick(now, &mut pool), vec![ask(peer, 1)]);
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
    let (mut pool, mut reactor) = setup(3);
    for peer in 1..=3 {
        reactor.announce(&v2(peer), id(1), 300, secs(0), &mut pool);
    }
    reactor.tx(&v2(1), tx(1), secs(1), &mut pool);
    let (_, effects) = finish(
        &mut pool,
        &mut reactor,
        Outcome::Fail {
            scope: RejectionScope::EffectsChain,
            charge: Charge::None,
        },
        secs(2),
    );
    assert!(effects.is_empty(), "nobody else is asked for a decided id");
    assert!(!reactor.wants(id(1)));

    // A peer that announces it again is not asked: the core remembers.
    assert!(reactor
        .announce(&v2(2), id(1), 300, secs(3), &mut pool)
        .is_empty());
    assert!(!reactor.wants(id(1)));
}

#[test]
fn a_local_fault_fetches_the_id_again_after_a_delay() {
    let (mut pool, mut reactor) = setup(2);
    reactor.announce(&v2(1), id(1), 300, secs(0), &mut pool);
    reactor.announce(&v2(2), id(1), 300, secs(0), &mut pool);
    reactor.tx(&v2(1), tx(1), secs(1), &mut pool);

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
    let (mut pool, mut reactor) = setup(1);
    reactor.announce(&v2(1), id(1), 300, secs(0), &mut pool);
    reactor.tx(&v2(1), tx(1), secs(0), &mut pool);
    reactor.take_started();

    let step = pool.tick(T_VERIFY);
    reactor.pool_step(&step, T_VERIFY, &mut pool);
    assert!(reactor.wants(id(1)));
    assert!(!pool.cooling(&v2(1)));
}

#[test]
fn a_session_that_ends_moves_its_ids_on_and_keeps_the_core_slot() {
    let (mut pool, mut reactor) = setup(2);
    reactor.announce(&v2(1), id(1), 300, secs(0), &mut pool);
    reactor.announce(&v2(2), id(1), 300, secs(0), &mut pool);
    reactor.announce(&v2(1), id(2), 300, secs(0), &mut pool);

    let effects = reactor.end(&v2(1), secs(1), &mut pool);
    // Id 1 has another announcer. Id 2 has nobody left.
    assert_eq!(effects, vec![ask(2, 1)]);
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
    reactor.connect(v2(1), false, MAX_REQUESTS_PER_PEER);
    reactor.connect(v2(2), true, MAX_REQUESTS_PER_PEER);
    // The core is full while both announce, so the choice is made later
    // with both announcers known.
    let held = fill_core(&mut pool);
    reactor.announce(&v2(1), id(1), 300, secs(0), &mut pool);
    reactor.announce(&v2(2), id(1), 300, secs(0), &mut pool);

    pool.release(held[0], secs(0));
    assert_eq!(reactor.tick(RETRY_FIRST, &mut pool), vec![ask(2, 1)]);
}

#[test]
fn a_peer_is_never_asked_for_more_than_its_hello_allows() {
    let mut pool = PoolModel::new(Height(100), Generation(0));
    let mut reactor = ReactorModel::new(MAX_TX);
    reactor.connect(v2(1), false, 2);
    for n in 0..4 {
        reactor.announce(&v2(1), id(n), 300, secs(0), &mut pool);
    }
    assert_eq!(reactor.outstanding(&v2(1)), 2);
    reactor.assert_invariants();
}

#[test]
fn an_id_too_large_for_this_node_is_skipped() {
    let (mut pool, mut reactor) = setup(1);
    assert!(reactor
        .announce(&v2(1), id(1), MAX_TX + 1, secs(0), &mut pool)
        .is_empty());
    assert!(!reactor.wants(id(1)));
    assert_eq!(pool.slots_in_use(), 0);
}

#[test]
fn a_full_pending_set_drops_and_counts() {
    let (mut pool, mut reactor) = setup(1);
    let total = u16::try_from(PENDING_PER_PEER).expect("fits the test's ids");
    for n in 0..total {
        reactor.announce(&v2(1), id(n), 300, secs(0), &mut pool);
    }
    assert_eq!(reactor.overflowed, 0);
    reactor.announce(&v2(1), id(total), 300, secs(0), &mut pool);
    assert_eq!(reactor.overflowed, 1);
    assert!(!reactor.wants(id(total)));
    reactor.assert_invariants();
}

#[test]
fn an_id_the_pool_has_or_that_another_source_is_bringing_in_is_not_fetched() {
    let (mut pool, mut reactor) = setup(1);
    // A legacy peer is already bringing id 1 in.
    let legacy = Source::Peer(QueueSource::LegacySocket(([10, 0, 0, 9], 8233).into()));
    let reserved = pool
        .reserve(id(1), legacy, Intent::Fetch, secs(0))
        .expect("room");

    assert!(reactor
        .announce(&v2(1), id(1), 300, secs(0), &mut pool)
        .is_empty());
    assert!(reactor.wants(id(1)), "it may still be needed");

    // The legacy fetch fails. The relay fetches it on its next try.
    pool.release(reserved.id, secs(0));
    assert_eq!(reactor.tick(RETRY_FIRST, &mut pool), vec![ask(1, 1)]);
    // While the relay is fetching it, nobody else fetches it either.
    assert_eq!(
        pool.reserve(id(1), Source::Node, Intent::Fetch, RETRY_FIRST),
        Err(ModelUnreserved::Pending)
    );
}
