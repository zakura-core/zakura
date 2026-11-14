//! The cases the M2 plan names, run on the model with the pool's model
//! behind it, and a random run that must drain.

use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use zakura_chain::{
    block::Height,
    transaction::{self, AuthDigest, UnminedTxId, WtxId},
};
use zakura_node_services::mempool::port::{
    model::{ModelBlock, ModelTransition, ModelTx, Outcome, PoolModel},
    Charge, Generation, Intent, RejectionScope, MAX_PENDING,
};

use super::model::{
    peer::Hello, window, CloseReason, PeerKey, RelayEffect, RelayModel, SessionId, Violation,
    ADVERT_DELAY, BASE_COOLDOWN, BASE_DEADLINE, INBOUND_DELAY, MAX_REQUESTS_PER_PEER, MAX_TX_BYTES,
    MIN_RATE, NO_ROOM_FIRST, PENDING_PER_PEER, RETRY_FIRST, SENDER_ALLOWANCE, WINDOW_FULL,
    WINDOW_SMALL,
};

fn id(n: u32) -> UnminedTxId {
    let mut bytes = [0; 32];
    bytes[..4].copy_from_slice(&n.to_le_bytes());
    UnminedTxId::Legacy(transaction::Hash(bytes))
}

fn witnessed(n: u32, auth: u8) -> UnminedTxId {
    let mut bytes = [0; 32];
    bytes[..4].copy_from_slice(&n.to_le_bytes());
    UnminedTxId::Witnessed(WtxId {
        id: transaction::Hash(bytes),
        auth_digest: AuthDigest([auth; 32]),
    })
}

fn tx(n: u32) -> ModelTx {
    ModelTx::new(id(n))
}

fn secs(seconds: u64) -> Duration {
    Duration::from_secs(seconds)
}

const SIZE: u32 = 300;

fn hello() -> Hello {
    Hello {
        serves: MAX_REQUESTS_PER_PEER,
        max_tx_bytes: MAX_TX_BYTES,
    }
}

fn relay() -> RelayModel {
    RelayModel::new(PoolModel::new(Height(100), Generation(0)))
}

/// A session of `peer` that has sent its `Hello`.
fn open(relay: &mut RelayModel, peer: u64, dialed: bool, now: Duration) -> SessionId {
    let session = relay.connect(PeerKey(peer), dialed, now);
    relay.hello(session, hello(), now);
    session
}

fn asks(effects: &[RelayEffect]) -> Vec<(SessionId, UnminedTxId)> {
    effects
        .iter()
        .filter_map(|effect| match effect {
            RelayEffect::Ask { session, id } => Some((*session, *id)),
            _ => None,
        })
        .collect()
}

fn adverts(effects: &[RelayEffect]) -> Vec<(SessionId, UnminedTxId)> {
    effects
        .iter()
        .filter_map(|effect| match effect {
            RelayEffect::Advert { session, id } => Some((*session, *id)),
            _ => None,
        })
        .collect()
}

fn closes(effects: &[RelayEffect]) -> Vec<(SessionId, CloseReason)> {
    effects
        .iter()
        .filter_map(|effect| match effect {
            RelayEffect::Close { session, reason } => Some((*session, *reason)),
            _ => None,
        })
        .collect()
}

/// Feeds `outcome` to every running job.
fn finish_all(relay: &mut RelayModel, outcome: &Outcome, now: Duration) {
    for job in relay.jobs().to_vec() {
        relay.outcome(job, outcome.clone(), now);
    }
}

/// Announces `n` from `session`, has it answer, and admits it.
fn fetch(relay: &mut RelayModel, session: SessionId, n: u32, now: Duration) {
    relay.inv(session, &[(id(n), SIZE)], now);
    assert_eq!(asks(&relay.take_effects()), vec![(session, id(n))]);
    relay.tx(session, id(n), tx(n), SIZE, now);
    finish_all(relay, &Outcome::Pass, now);
}

/// The deadline of the only request a session has, sent at `sent`.
fn deadline(size: u32, sent: Duration) -> Duration {
    let bytes = u64::from(size) + SENDER_ALLOWANCE;
    sent + BASE_DEADLINE + Duration::from_millis(bytes * 1_000 / MIN_RATE)
}

#[test]
fn an_id_is_claimed_reserved_asked_for_and_admitted() {
    let mut relay = relay();
    let a = open(&mut relay, 1, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    assert_eq!(asks(&relay.take_effects()), vec![(a, id(1))]);
    // The pool's slot is taken before the request goes out.
    assert_eq!(relay.pool().slots_in_use(), 1);
    assert_eq!(relay.wanted().entry(id(1)).unwrap().claim, Some(a));
    relay.assert_invariants();

    relay.tx(a, id(1), tx(1), SIZE, secs(1));
    assert_eq!(relay.in_flight(PeerKey(1)), 1);
    finish_all(&mut relay, &Outcome::Pass, secs(2));
    assert_eq!(relay.pool().visible(), vec![id(1)]);
    assert!(relay.wanted().entry(id(1)).is_none());
    assert_eq!(relay.in_flight(PeerKey(1)), 0);
    assert!(relay.session(a).unwrap().pending.is_empty());
    relay.assert_invariants();
}

#[test]
fn an_inbound_announcer_waits_and_a_dialed_one_goes_first() {
    let mut relay = relay();
    let inbound = open(&mut relay, 1, false, secs(0));
    relay.inv(inbound, &[(id(1), SIZE)], secs(0));
    assert!(asks(&relay.take_effects()).is_empty());

    // A dialed peer that announces later still claims first.
    let dialed = open(&mut relay, 2, true, secs(1));
    relay.inv(dialed, &[(id(1), SIZE)], secs(1));
    assert_eq!(asks(&relay.take_effects()), vec![(dialed, id(1))]);

    // An inbound announcer alone is asked once the delay passes, with no
    // other traffic.
    relay.inv(inbound, &[(id(2), SIZE)], secs(1));
    assert!(asks(&relay.take_effects()).is_empty());
    assert_eq!(relay.next_deadline(), Some(secs(2)));
    relay.tick(secs(1) + INBOUND_DELAY);
    assert_eq!(asks(&relay.take_effects()), vec![(inbound, id(2))]);
    relay.assert_invariants();
}

#[test]
fn not_found_moves_the_id_to_an_announcer_that_has_not_tried_it() {
    let mut relay = relay();
    let a = open(&mut relay, 1, true, secs(0));
    let b = open(&mut relay, 2, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    relay.inv(b, &[(id(1), SIZE)], secs(0));
    assert_eq!(asks(&relay.take_effects()), vec![(a, id(1))]);

    // A answers NotFound. A is the older session, so it would win every
    // race, but it never claims the id again.
    relay.not_found(a, id(1), secs(1));
    assert_eq!(asks(&relay.take_effects()), vec![(b, id(1))]);
    let generation = relay.wanted().entry(id(1)).unwrap().generation;
    assert!(relay.session(a).unwrap().has_tried(id(1), generation));
    // A announcing it again changes nothing.
    relay.inv(a, &[(id(1), SIZE)], secs(1));
    assert!(relay.session(a).unwrap().pending.is_empty());
    relay.assert_invariants();
}

#[test]
fn a_deadline_lets_another_announcer_try_and_the_late_body_is_dropped() {
    let mut relay = relay();
    let a = open(&mut relay, 1, true, secs(0));
    let b = open(&mut relay, 2, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    relay.inv(b, &[(id(1), SIZE)], secs(0));
    assert_eq!(asks(&relay.take_effects()), vec![(a, id(1))]);

    let due = deadline(SIZE, secs(0));
    relay.tick(due - Duration::from_millis(1));
    assert!(asks(&relay.take_effects()).is_empty());
    relay.tick(due);
    // B is asked. A's request stays outstanding and keeps A's slot.
    assert_eq!(asks(&relay.take_effects()), vec![(b, id(1))]);
    assert_eq!(relay.in_flight(PeerKey(1)), 1);
    assert_eq!(relay.reservations_held(), 1);

    // A's late body never reaches the pool, and frees A's slot.
    relay.tx(a, id(1), tx(1), SIZE, due + secs(1));
    assert_eq!(relay.pool().bodies_held(), 0);
    assert_eq!(relay.in_flight(PeerKey(1)), 0);
    assert!(relay.take_effects().is_empty());

    relay.tx(b, id(1), tx(1), SIZE, due + secs(2));
    finish_all(&mut relay, &Outcome::Pass, due + secs(2));
    assert_eq!(relay.pool().visible(), vec![id(1)]);
    relay.assert_invariants();
}

#[test]
fn a_timed_out_peer_waits_for_announcers_not_yet_asked() {
    let mut relay = relay();
    let a = open(&mut relay, 1, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    relay.take_effects();
    let due = deadline(SIZE, secs(0));
    relay.tick(due);
    // A's late answer arrives before anyone else claims. A is not asked
    // again yet.
    relay.tx(a, id(1), tx(1), SIZE, due);
    assert!(asks(&relay.take_effects()).is_empty());
    // B announces within the delay and is asked first.
    let b = open(&mut relay, 2, true, due);
    relay.inv(b, &[(id(1), SIZE)], due);
    assert_eq!(asks(&relay.take_effects()), vec![(b, id(1))]);
    relay.assert_invariants();
}

#[test]
fn a_sole_slow_announcer_is_asked_again_with_a_doubled_deadline() {
    let mut relay = relay();
    let a = open(&mut relay, 1, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    relay.take_effects();
    let first = deadline(SIZE, secs(0));
    relay.tick(first);
    relay.tx(a, id(1), tx(1), SIZE, first + secs(1));
    relay.tick(first + secs(1) + INBOUND_DELAY);
    let again = first + secs(1) + INBOUND_DELAY;
    assert_eq!(asks(&relay.take_effects()), vec![(a, id(1))]);
    let request = relay.session(a).unwrap().requests[&id(1)];
    assert_eq!(request.deadline, again + (first - secs(0)) * 2);
    // This time it answers in time.
    relay.tx(a, id(1), tx(1), SIZE, again + secs(20));
    finish_all(&mut relay, &Outcome::Pass, again + secs(20));
    assert_eq!(relay.pool().visible(), vec![id(1)]);
    relay.assert_invariants();
}

#[test]
fn deadlines_cover_every_body_outstanding_to_the_peer() {
    let mut relay = relay();
    let a = open(&mut relay, 1, true, secs(0));
    let big = 2_000_000;
    relay.inv(a, &[(id(1), big)], secs(0));
    relay.inv(a, &[(id(2), big)], secs(1));
    let state = relay.session(a).unwrap();
    // The first request's deadline grew when the second was sent, to cover
    // both bodies from then, in either order.
    let total = 2 * u64::from(big) + SENDER_ALLOWANCE;
    let span = BASE_DEADLINE + Duration::from_millis(total * 1_000 / MIN_RATE);
    assert_eq!(state.requests[&id(1)].deadline, secs(1) + span);
    assert_eq!(state.requests[&id(2)].deadline, secs(1) + span);
}

#[test]
fn only_an_announcers_own_size_binds_its_body() {
    // A hostile peer gives the wrong size first. The honest peer's body of
    // its own announced size is accepted.
    let mut relay = relay();
    let hostile = open(&mut relay, 1, true, secs(0));
    let honest = open(&mut relay, 2, true, secs(0));
    relay.inv(hostile, &[(id(1), 100)], secs(0));
    relay.inv(honest, &[(id(1), SIZE)], secs(0));
    assert_eq!(asks(&relay.take_effects()), vec![(hostile, id(1))]);
    relay.not_found(hostile, id(1), secs(1));
    assert_eq!(asks(&relay.take_effects()), vec![(honest, id(1))]);
    relay.tx(honest, id(1), tx(1), SIZE, secs(2));
    assert!(closes(&relay.take_effects()).is_empty());
    finish_all(&mut relay, &Outcome::Pass, secs(2));
    assert_eq!(relay.pool().visible(), vec![id(1)]);

    // The other order: the honest size first, and a body of another size
    // from the peer that announced it is that peer's violation.
    let mut relay = self::relay();
    let honest = open(&mut relay, 1, true, secs(0));
    let hostile = open(&mut relay, 2, true, secs(0));
    relay.inv(hostile, &[(id(1), 100)], secs(0));
    relay.inv(honest, &[(id(1), SIZE)], secs(0));
    relay.take_effects();
    relay.tx(hostile, id(1), tx(1), SIZE, secs(1));
    assert_eq!(
        closes(&relay.take_effects()),
        vec![(hostile, CloseReason::Violation(Violation::WrongSize))]
    );
    relay.assert_invariants();
}

#[test]
fn a_full_pool_costs_one_probe_per_backoff() {
    let mut relay = relay();
    let held: Vec<_> = (0..u32::try_from(MAX_PENDING).unwrap())
        .map(|n| {
            relay.outside(secs(0), |pool, now| {
                let answer = pool.reserve(id(100_000 + n), Intent::Fetch, now);
                (answer.expect("room").id, pool.effects())
            })
        })
        .collect();
    let a = open(&mut relay, 1, true, secs(0));
    relay.inv(a, &[(id(1), SIZE), (id(2), SIZE)], secs(0));
    assert!(asks(&relay.take_effects()).is_empty());
    assert!(relay.wanted().in_backoff());
    // The probe after the backoff finds no room again, and the backoff
    // doubles.
    relay.tick(NO_ROOM_FIRST);
    assert!(asks(&relay.take_effects()).is_empty());
    assert_eq!(
        relay.wanted().backoff_until(NO_ROOM_FIRST),
        Some(NO_ROOM_FIRST * 3)
    );
    // Room comes back, and the next probe gets it.
    relay.outside(secs(2), |pool, now| ((), pool.release(held[0], now)));
    relay.tick(NO_ROOM_FIRST * 3);
    assert_eq!(asks(&relay.take_effects()), vec![(a, id(1))]);
    // That took the last slot, so the next claim finds the pool full again.
    assert!(relay.wanted().in_backoff());
    assert_eq!(
        relay.wanted().backoff_until(NO_ROOM_FIRST * 3),
        Some(NO_ROOM_FIRST * 4)
    );
    relay.assert_invariants();
}

#[test]
fn an_id_another_caller_fetches_is_retried_later() {
    let mut relay = relay();
    let other = relay.outside(secs(0), |pool, now| {
        let answer = pool.reserve(id(1), Intent::Fetch, now);
        (answer.expect("room").id, pool.effects())
    });
    let a = open(&mut relay, 1, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    assert!(asks(&relay.take_effects()).is_empty());
    assert_eq!(relay.wanted().entry(id(1)).unwrap().retry_at, RETRY_FIRST);
    relay.outside(secs(0), |pool, now| ((), pool.release(other, now)));
    relay.tick(RETRY_FIRST);
    assert_eq!(asks(&relay.take_effects()), vec![(a, id(1))]);
    // A block mines it. The cancellation is tx-relay's, and nothing is
    // addressed to the other caller, which already let go.
    let block = ModelTransition {
        disconnected: 0,
        connected: vec![ModelBlock {
            transactions: vec![tx(1)],
        }],
        tip: Height(101),
        generation: Generation(1),
        rules_change: false,
    };
    relay.transition(block, RETRY_FIRST);
    assert!(relay.take_outside().is_empty());
    assert_eq!(relay.reservations_held(), 0);
    relay.assert_invariants();
}

#[test]
fn a_cooldown_goes_to_the_supplier_and_a_second_one_closes_it() {
    let mut relay = relay();
    let announcer = open(&mut relay, 1, false, secs(0));
    let supplier = open(&mut relay, 2, true, secs(0));
    relay.inv(announcer, &[(id(1), SIZE)], secs(0));
    relay.inv(supplier, &[(id(1), SIZE)], secs(0));
    assert_eq!(asks(&relay.take_effects()), vec![(supplier, id(1))]);
    relay.tx(supplier, id(1), tx(1), SIZE, secs(1));
    let bad = Outcome::Fail {
        scope: RejectionScope::ExactTip,
        charge: Charge::Cooldown,
    };
    finish_all(&mut relay, &bad, secs(1));
    assert!(relay.reactor().is_cooling(PeerKey(2), secs(1)));
    assert!(!relay.reactor().is_cooling(PeerKey(1), secs(1)));

    // While it cools, its announcements are ignored.
    relay.inv(supplier, &[(id(2), SIZE)], secs(2));
    assert!(relay.session(supplier).unwrap().pending.is_empty());

    // A second cooldown after the first ends closes it.
    let later = secs(1) + BASE_COOLDOWN;
    relay.inv(supplier, &[(id(3), SIZE)], later);
    assert_eq!(asks(&relay.take_effects()), vec![(supplier, id(3))]);
    relay.tx(supplier, id(3), tx(3), SIZE, later);
    finish_all(&mut relay, &bad, later);
    assert_eq!(
        closes(&relay.take_effects()),
        vec![(supplier, CloseReason::RepeatedCooldown)]
    );
    assert!(relay.session(supplier).is_none());
    relay.assert_invariants();
}

#[test]
fn a_cooldown_waits_for_the_node_to_be_current() {
    let mut relay = relay();
    relay.set_current(false);
    let a = open(&mut relay, 1, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    relay.tx(a, id(1), tx(1), SIZE, secs(0));
    let bad = Outcome::Fail {
        scope: RejectionScope::ExactTip,
        charge: Charge::Cooldown,
    };
    finish_all(&mut relay, &bad, secs(0));
    assert!(!relay.reactor().is_cooling(PeerKey(1), secs(0)));
}

#[test]
fn a_body_that_arrives_after_its_peer_started_cooling_is_not_submitted() {
    let mut relay = relay();
    let a = open(&mut relay, 1, true, secs(0));
    relay.inv(a, &[(id(1), SIZE), (id(2), SIZE)], secs(0));
    assert_eq!(asks(&relay.take_effects()).len(), 2);
    relay.tx(a, id(1), tx(1), SIZE, secs(0));
    let bad = Outcome::Fail {
        scope: RejectionScope::ExactTip,
        charge: Charge::Cooldown,
    };
    finish_all(&mut relay, &bad, secs(0));
    relay.tx(a, id(2), tx(2), SIZE, secs(1));
    assert_eq!(relay.pool().bodies_held(), 0);
    assert_eq!(relay.reservations_held(), 0);
    relay.assert_invariants();
}

#[test]
fn a_ban_closes_every_session_of_the_peer() {
    let mut relay = relay();
    let a = open(&mut relay, 1, true, secs(0));
    let replacement = relay.connect(PeerKey(1), true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    relay.tx(a, id(1), tx(1), SIZE, secs(0));
    let banned = Outcome::Fail {
        scope: RejectionScope::ExactTip,
        charge: Charge::Ban,
    };
    finish_all(&mut relay, &banned, secs(1));
    let closed = closes(&relay.take_effects());
    assert!(closed.contains(&(a, CloseReason::Ban)));
    assert!(closed.contains(&(replacement, CloseReason::Ban)));
    relay.assert_invariants();
}

#[test]
fn a_replacement_session_inherits_the_limit_and_the_charges() {
    let mut relay = relay();
    let old = open(&mut relay, 1, true, secs(0));
    let ids: Vec<_> = (1..=5).map(|n| (id(n), SIZE)).collect();
    relay.inv(old, &ids, secs(0));
    for n in 1..=5 {
        relay.tx(old, id(n), tx(n), SIZE, secs(0));
    }
    relay.take_effects();
    assert_eq!(relay.in_flight(PeerKey(1)), 5);

    // The replacement opens before the old session's cleanup. It neither
    // announces nor fetches.
    let new = open(&mut relay, 1, true, secs(1));
    relay.inv(new, &[(id(6), SIZE)], secs(1));
    assert!(relay.take_effects().is_empty());
    assert!(!relay.reactor().has_go(PeerKey(1), new));

    // The old session ends with all five unsettled. The new one gets the go
    // with no free slot.
    // The peer stays live for the adapter across the handover.
    assert!(relay.reactor().is_live(PeerKey(1)));
    relay.end(old, secs(2));
    assert!(relay.reactor().is_live(PeerKey(1)));
    assert!(relay.reactor().has_go(PeerKey(1), new));
    assert_eq!(relay.in_flight(PeerKey(1)), 5);
    assert!(asks(&relay.take_effects()).is_empty());

    // One verdict frees one slot, and an admission is not announced back to
    // the peer that supplied it.
    let jobs = relay.jobs().to_vec();
    relay.outcome(jobs[0], Outcome::Pass, secs(3));
    let effects = relay.take_effects();
    assert_eq!(asks(&effects), vec![(new, id(6))]);
    assert!(adverts(&effects).is_empty());

    // A cooldown earned by a body from the old session cools the peer.
    let bad = Outcome::Fail {
        scope: RejectionScope::ExactTip,
        charge: Charge::Cooldown,
    };
    relay.outcome(jobs[1], bad, secs(3));
    assert!(relay.reactor().is_cooling(PeerKey(1), secs(3)));
    relay.assert_invariants();
}

#[test]
fn a_session_waiting_for_the_go_keeps_its_announcements_to_itself() {
    let mut relay = relay();
    let old = open(&mut relay, 1, true, secs(0));
    let new = open(&mut relay, 1, true, secs(0));
    relay.inv(new, &[(id(1), SIZE), (id(2), SIZE)], secs(0));
    // Nothing reaches the table, so the peer holds one share at most.
    assert_eq!(relay.wanted().len(), 0);
    assert_eq!(relay.session(new).unwrap().waiting.len(), 2);
    relay.take_effects();
    // With the go, the new session takes them in and asks.
    relay.end(old, secs(1));
    assert_eq!(
        asks(&relay.take_effects()),
        vec![(new, id(1)), (new, id(2))]
    );
    relay.assert_invariants();
}

#[test]
fn a_mined_id_cancels_its_fetch() {
    let mut relay = relay();
    let a = open(&mut relay, 1, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    relay.take_effects();
    let block = ModelTransition {
        disconnected: 0,
        connected: vec![ModelBlock {
            transactions: vec![tx(1)],
        }],
        tip: Height(101),
        generation: Generation(1),
        rules_change: false,
    };
    relay.transition(block, secs(1));
    assert!(relay.wanted().entry(id(1)).is_none());
    assert_eq!(relay.reservations_held(), 0);
    // The request stays outstanding, and its answer is dropped.
    assert_eq!(relay.in_flight(PeerKey(1)), 1);
    relay.tx(a, id(1), tx(1), SIZE, secs(2));
    assert_eq!(relay.in_flight(PeerKey(1)), 0);
    assert_eq!(relay.pool().bodies_held(), 0);
    relay.assert_invariants();
}

#[test]
fn turning_the_pool_off_releases_fetches_until_it_is_live() {
    let mut relay = relay();
    let a = open(&mut relay, 1, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    relay.take_effects();
    relay.deactivate(secs(1));
    assert_eq!(relay.reservations_held(), 0);
    assert!(relay.wanted().entry(id(1)).unwrap().claim.is_none());
    relay.tx(a, id(1), tx(1), SIZE, secs(2));
    relay.inv(a, &[(id(2), SIZE)], secs(2));
    assert!(asks(&relay.take_effects()).is_empty());
    relay.activate(secs(3));
    let asked = asks(&relay.take_effects());
    assert_eq!(asked, vec![(a, id(1)), (a, id(2))]);
    relay.assert_invariants();
}

#[test]
fn a_decided_id_announced_again_is_a_new_generation() {
    let mut relay = relay();
    let a = open(&mut relay, 1, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    relay.take_effects();
    let first = relay.wanted().entry(id(1)).unwrap().generation;
    relay.tx(a, id(1), tx(1), SIZE, secs(0));
    let rejected = Outcome::Fail {
        scope: RejectionScope::ExactTip,
        charge: Charge::None,
    };
    finish_all(&mut relay, &rejected, secs(0));
    assert!(relay.wanted().entry(id(1)).is_none());
    // A new block forgets the tip-scoped rejection, and a later
    // announcement makes a new entry.
    let block = ModelTransition {
        disconnected: 0,
        connected: vec![ModelBlock::default()],
        tip: Height(101),
        generation: Generation(1),
        rules_change: false,
    };
    relay.transition(block, secs(1));
    let b = open(&mut relay, 2, true, secs(1));
    relay.inv(b, &[(id(1), SIZE)], secs(1));
    let second = relay.wanted().entry(id(1)).unwrap().generation;
    assert_ne!(first, second);
    assert_eq!(asks(&relay.take_effects()), vec![(b, id(1))]);
    relay.assert_invariants();
}

#[test]
fn overflowing_ids_leave_no_holder_and_a_disconnect_leaves_nothing() {
    let mut relay = relay();
    let a = relay.connect(PeerKey(1), true, secs(0));
    for round in 0..3u32 {
        let ids: Vec<_> = (0..u32::try_from(PENDING_PER_PEER).unwrap() + 10)
            .map(|n| (id(round * 100_000 + n), SIZE))
            .collect();
        relay.inv(a, &ids, secs(0));
    }
    assert_eq!(relay.session(a).unwrap().pending.len(), PENDING_PER_PEER);
    assert_eq!(relay.wanted().len(), PENDING_PER_PEER);
    assert!(relay.session(a).unwrap().overflow > 0);
    relay.assert_invariants();
    relay.end(a, secs(1));
    assert_eq!(relay.wanted().len(), 0);
}

#[test]
fn peers_that_never_answer_cannot_crowd_out_an_honest_announcement() {
    let mut relay = relay();
    let gone: Vec<_> = (1..=2)
        .map(|p| relay.connect(PeerKey(p), true, secs(0)))
        .collect();
    let silent: Vec<_> = (3..=4)
        .map(|p| open(&mut relay, p, true, secs(0)))
        .collect();
    for (k, session) in gone.iter().chain(&silent).enumerate() {
        let base = u32::try_from(k).unwrap() * 100_000;
        let ids: Vec<_> = (0..u32::try_from(PENDING_PER_PEER).unwrap())
            .map(|n| (id(base + n), SIZE))
            .collect();
        relay.inv(*session, &ids, secs(0));
    }
    for session in gone {
        relay.end(session, secs(1));
    }
    relay.take_effects();
    let honest = open(&mut relay, 9, true, secs(1));
    relay.inv(honest, &[(id(999_999), SIZE)], secs(1));
    assert_eq!(asks(&relay.take_effects()), vec![(honest, id(999_999))]);
    relay.assert_invariants();
}

#[test]
fn ids_an_honest_peer_tried_never_take_its_room() {
    let mut relay = relay();
    let hostile = open(&mut relay, 1, false, secs(0));
    let honest = open(&mut relay, 2, true, secs(0));
    let ids: Vec<_> = (0..20).map(|n| (id(n), SIZE)).collect();
    relay.inv(hostile, &ids, secs(0));
    relay.inv(honest, &ids, secs(0));
    for _ in 0..4 {
        for (session, n) in asks(&relay.take_effects()) {
            assert_eq!(session, honest);
            relay.not_found(honest, n, secs(0));
        }
    }
    let state = relay.session(honest).unwrap();
    assert!(state.pending.is_empty());
    assert_eq!(state.tried.len(), 20);
    // The hostile peer still holds every entry. The honest peer's new id is
    // taken and asked for.
    assert_eq!(relay.wanted().len(), 20);
    relay.inv(honest, &[(id(100), SIZE)], secs(0));
    assert_eq!(asks(&relay.take_effects()), vec![(honest, id(100))]);
    relay.assert_invariants();
}

#[test]
fn a_whole_pool_announcement_is_all_fetched() {
    let mut relay = relay();
    let a = open(&mut relay, 1, true, secs(0));
    let count = 8_000u32;
    let ids: Vec<_> = (0..count).map(|n| (id(n), SIZE)).collect();
    relay.inv(a, &ids, secs(0));
    let mut now = secs(0);
    let mut fetched = 0;
    while fetched < count {
        let asked = asks(&relay.take_effects());
        assert!(!asked.is_empty(), "every id is asked for in turn");
        for (session, asked_id) in asked {
            let n = u32::from_le_bytes(match asked_id {
                UnminedTxId::Legacy(hash) => hash.0[..4].try_into().unwrap(),
                UnminedTxId::Witnessed(_) => unreachable!(),
            });
            relay.tx(session, asked_id, tx(n), SIZE, now);
            fetched += 1;
        }
        finish_all(&mut relay, &Outcome::Pass, now);
        now += Duration::from_millis(10);
        relay.tick(now);
    }
    assert_eq!(relay.pool().visible().len(), 8_000);
    assert_eq!(relay.wanted().len(), 0);
    relay.assert_invariants();
}

#[test]
fn a_session_announces_the_pool_once_and_never_back_to_its_source() {
    let mut relay = relay();
    let a = open(&mut relay, 1, true, secs(0));
    let b = open(&mut relay, 2, true, secs(0));
    relay.set_ranking(vec![a, b]);
    fetch(&mut relay, a, 1, secs(0));
    // B hears it at once, inside the window. A supplied it and does not.
    assert_eq!(adverts(&relay.take_effects()), vec![(b, id(1))]);

    // A new peer's opening announcement carries the pool.
    let c = open(&mut relay, 3, true, secs(1));
    assert_eq!(adverts(&relay.take_effects()), vec![(c, id(1))]);
    relay.assert_invariants();
}

#[test]
fn sessions_outside_the_window_hear_after_the_delay() {
    let mut relay = relay();
    let supplier = open(&mut relay, 100, true, secs(0));
    let peers: Vec<_> = (1..=20)
        .map(|p| open(&mut relay, p, true, secs(0)))
        .collect();
    relay.set_ranking(peers.clone());
    assert_eq!(window(Some(SIZE)), WINDOW_SMALL);
    fetch(&mut relay, supplier, 1, secs(0));
    let now: HashSet<_> = adverts(&relay.take_effects())
        .into_iter()
        .map(|(s, _)| s)
        .collect();
    assert_eq!(now, peers[..WINDOW_SMALL].iter().copied().collect());

    // One outside the window announced it meanwhile, so it is skipped.
    relay.inv(peers[19], &[(id(1), SIZE)], secs(1));
    relay.tick(ADVERT_DELAY);
    let later: HashSet<_> = adverts(&relay.take_effects())
        .into_iter()
        .map(|(s, _)| s)
        .collect();
    assert_eq!(later, peers[WINDOW_SMALL..19].iter().copied().collect());
    assert_eq!(window(Some(MAX_TX_BYTES)), WINDOW_FULL);
    relay.assert_invariants();
}

#[test]
fn with_no_advert_delay_only_the_window_hears() {
    let mut relay = relay().with_advert_delay(None);
    let supplier = open(&mut relay, 100, true, secs(0));
    let peers: Vec<_> = (1..=20)
        .map(|p| open(&mut relay, p, true, secs(0)))
        .collect();
    relay.set_ranking(peers.clone());
    fetch(&mut relay, supplier, 1, secs(0));
    assert_eq!(adverts(&relay.take_effects()).len(), WINDOW_SMALL);
    relay.tick(secs(60));
    assert!(adverts(&relay.take_effects()).is_empty());
}

#[test]
fn only_an_announced_exact_id_is_served() {
    let mut relay = relay();
    let supplier = open(&mut relay, 1, true, secs(0));
    let a = open(&mut relay, 2, true, secs(0));
    let b = open(&mut relay, 3, true, secs(0));
    relay.set_ranking(vec![a]);
    let tx_id = witnessed(1, 7);
    relay.inv(supplier, &[(tx_id, SIZE)], secs(0));
    relay.tx(supplier, tx_id, ModelTx::new(tx_id), SIZE, secs(0));
    finish_all(&mut relay, &Outcome::Pass, secs(0));
    relay.take_effects();

    relay.get_txs(a, tx_id, secs(1));
    // The same mined id with other authorizing data was never announced.
    relay.get_txs(a, witnessed(1, 8), secs(1));
    // B is outside the window and has not been told yet.
    relay.get_txs(b, tx_id, secs(1));
    assert_eq!(
        relay.take_effects(),
        vec![
            RelayEffect::Serve {
                session: a,
                id: tx_id
            },
            RelayEffect::NotFound {
                session: a,
                id: witnessed(1, 8)
            },
            RelayEffect::NotFound {
                session: b,
                id: tx_id
            },
        ]
    );
}

#[test]
fn a_hidden_transaction_is_announced_when_it_is_revealed() {
    let mut relay = relay();
    let supplier = open(&mut relay, 1, true, secs(0));
    fetch(&mut relay, supplier, 1, secs(0));
    relay.take_effects();
    // A reorg hides it while it is checked again.
    let reorg = ModelTransition {
        disconnected: 1,
        connected: vec![ModelBlock::default(), ModelBlock::default()],
        tip: Height(101),
        generation: Generation(1),
        rules_change: false,
    };
    relay.transition(reorg, secs(1));
    assert_eq!(relay.pool().hidden(), vec![id(1)]);
    // A session that opens now does not announce it.
    let a = open(&mut relay, 2, true, secs(1));
    relay.set_ranking(vec![a]);
    assert!(adverts(&relay.take_effects()).is_empty());
    finish_all(&mut relay, &Outcome::Pass, secs(2));
    assert_eq!(adverts(&relay.take_effects()), vec![(a, id(1))]);
    relay.assert_invariants();
}

#[test]
fn a_session_that_falls_off_the_log_rebuilds_from_the_snapshot() {
    let mut relay =
        RelayModel::new(PoolModel::new(Height(100), Generation(0))).with_manual_reads(3);
    let supplier = open(&mut relay, 1, true, secs(0));
    let ranked = open(&mut relay, 2, true, secs(0));
    let outside = open(&mut relay, 3, true, secs(0));
    relay.set_ranking(vec![ranked]);
    fetch(&mut relay, supplier, 1, secs(0));
    relay.read(ranked, secs(0));
    relay.read(outside, secs(0));
    assert_eq!(adverts(&relay.take_effects()), vec![(ranked, id(1))]);
    // The outside session holds its advert for the delay.
    assert_eq!(relay.session(outside).unwrap().held.len(), 1);

    // Enough admissions to overrun a three-entry log.
    for n in 2..=6 {
        fetch(&mut relay, supplier, n, secs(1));
    }
    relay.take_effects();
    relay.read(outside, secs(1));
    // It announces the whole snapshot it was not told, at once, and its
    // held advert goes with it, so nothing is sent twice.
    let sent: HashSet<_> = adverts(&relay.take_effects())
        .into_iter()
        .map(|(_, id)| id)
        .collect();
    assert_eq!(sent, (1..=6).map(id).collect());
    assert!(relay.session(outside).unwrap().held.is_empty());
    relay.assert_invariants();
    relay.tick(ADVERT_DELAY + secs(1));
    assert!(adverts(&relay.take_effects()).is_empty());

    // A session already told the first one sends only the rest. The
    // supplier is told none, because it supplied them all.
    relay.read(ranked, secs(3));
    let sent: HashSet<_> = adverts(&relay.take_effects())
        .into_iter()
        .map(|(_, id)| id)
        .collect();
    assert_eq!(sent, (2..=6).map(id).collect());
    relay.read(supplier, secs(3));
    assert!(adverts(&relay.take_effects()).is_empty());
    relay.assert_invariants();
}

#[test]
fn an_addition_hidden_before_it_is_read_waits_for_its_reveal() {
    let mut relay =
        RelayModel::new(PoolModel::new(Height(100), Generation(0))).with_manual_reads(100);
    let supplier = open(&mut relay, 1, true, secs(0));
    let a = open(&mut relay, 2, true, secs(0));
    relay.set_ranking(vec![a]);
    fetch(&mut relay, supplier, 1, secs(0));
    let reorg = ModelTransition {
        disconnected: 1,
        connected: vec![ModelBlock::default(), ModelBlock::default()],
        tip: Height(101),
        generation: Generation(1),
        rules_change: false,
    };
    relay.transition(reorg, secs(1));
    relay.take_effects();
    relay.read(a, secs(1));
    assert!(adverts(&relay.take_effects()).is_empty());
    finish_all(&mut relay, &Outcome::Pass, secs(2));
    relay.read(a, secs(2));
    assert_eq!(adverts(&relay.take_effects()), vec![(a, id(1))]);
    relay.assert_invariants();
}

#[test]
fn wire_violations_close_the_session() {
    let mut relay = relay();
    let a = relay.connect(PeerKey(1), true, secs(0));
    relay.get_txs(a, id(1), secs(0));
    assert_eq!(
        closes(&relay.take_effects()),
        vec![(a, CloseReason::Violation(Violation::NoHelloFirst))]
    );
    let b = open(&mut relay, 2, true, secs(0));
    relay.hello(b, hello(), secs(0));
    assert_eq!(
        closes(&relay.take_effects()),
        vec![(b, CloseReason::Violation(Violation::HelloTwice))]
    );
    let c = open(&mut relay, 3, true, secs(0));
    relay.tx(c, id(1), tx(1), SIZE, secs(0));
    assert_eq!(
        closes(&relay.take_effects()),
        vec![(c, CloseReason::Violation(Violation::Unrequested))]
    );
    let d = open(&mut relay, 4, true, secs(0));
    relay.inv(d, &[(id(5), SIZE)], secs(0));
    relay.take_effects();
    relay.tx(d, id(5), tx(6), SIZE, secs(0));
    assert_eq!(
        closes(&relay.take_effects()),
        vec![(d, CloseReason::Violation(Violation::WrongId))]
    );
    let e = relay.connect(PeerKey(5), true, secs(0));
    relay.hello(
        e,
        Hello {
            serves: 0,
            max_tx_bytes: MAX_TX_BYTES,
        },
        secs(0),
    );
    assert_eq!(
        closes(&relay.take_effects()),
        vec![(e, CloseReason::Violation(Violation::BadHello))]
    );
    relay.assert_invariants();
}

#[test]
fn an_inv_before_hello_is_kept_and_asked_for_after_it() {
    let mut relay = relay();
    let a = relay.connect(PeerKey(1), true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    assert!(asks(&relay.take_effects()).is_empty());
    relay.hello(a, hello(), secs(1));
    assert_eq!(asks(&relay.take_effects()), vec![(a, id(1))]);
}

#[test]
fn a_hello_lowers_how_many_requests_go_out() {
    let mut relay = relay();
    let a = relay.connect(PeerKey(1), true, secs(0));
    relay.hello(
        a,
        Hello {
            serves: 2,
            max_tx_bytes: MAX_TX_BYTES,
        },
        secs(0),
    );
    let ids: Vec<_> = (1..=5).map(|n| (id(n), SIZE)).collect();
    relay.inv(a, &ids, secs(0));
    assert_eq!(asks(&relay.take_effects()).len(), 2);
}

#[test]
fn reserves_in_flight_and_the_one_probe() {
    let mut relay =
        RelayModel::new(PoolModel::new(Height(100), Generation(0))).with_manual_reserves();
    fill_pool(&mut relay);
    let a = open(&mut relay, 1, true, secs(0));
    let b = open(&mut relay, 2, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    relay.inv(b, &[(id(2), SIZE)], secs(0));
    // Both calls are in flight before either is answered.
    assert_eq!(relay.calls_in_flight(), 2);
    relay.assert_invariants();
    assert!(relay.answer_reserve(secs(0)));
    assert_eq!(relay.wanted().backoff_until(secs(0)), Some(NO_ROOM_FIRST));
    // The second was already in flight, so it does not extend the backoff.
    assert!(relay.answer_reserve(secs(0)));
    assert_eq!(relay.wanted().backoff_until(secs(0)), Some(NO_ROOM_FIRST));

    // After the backoff one probe goes out, though both sessions could ask.
    relay.tick(NO_ROOM_FIRST);
    assert_eq!(relay.calls_in_flight(), 1);
    let (holder, _) = relay.wanted().probe().expect("a probe is out");
    assert_eq!(holder, a);
    // The probe's session ends, so the permit passes to the other session.
    relay.end(a, NO_ROOM_FIRST);
    assert_eq!(relay.wanted().probe().map(|(holder, _)| holder), Some(b));
    relay.assert_invariants();
    // The ended session's call is dropped with it. The new probe finds no
    // room, and the backoff doubles.
    assert!(relay.answer_reserve(NO_ROOM_FIRST));
    assert!(relay.answer_reserve(NO_ROOM_FIRST));
    assert_eq!(
        relay.wanted().backoff_until(NO_ROOM_FIRST),
        Some(NO_ROOM_FIRST * 3)
    );
    relay.assert_invariants();
}

#[test]
fn a_probe_with_another_answer_hands_the_permit_on() {
    let mut relay = relay();
    // Another caller fetches id 1, and the pool is otherwise full.
    relay.outside(secs(0), |pool, now| {
        let answer = pool.reserve(id(1), Intent::Fetch, now);
        (answer.expect("room").id, pool.effects())
    });
    for n in 1..u32::try_from(MAX_PENDING).unwrap() {
        relay.outside(secs(0), |pool, now| {
            let answer = pool.reserve(id(100_000 + n), Intent::Fetch, now);
            (answer.expect("room").id, pool.effects())
        });
    }
    // Id 1 is refused as another caller's fetch, then id 2 finds no room.
    let a = open(&mut relay, 1, true, secs(0));
    relay.inv(a, &[(id(1), SIZE), (id(2), SIZE)], secs(0));
    assert_eq!(relay.wanted().entry(id(1)).unwrap().retry_at, RETRY_FIRST);
    assert_eq!(relay.wanted().backoff_until(secs(0)), Some(NO_ROOM_FIRST));
    // At the end of the backoff the probe asks for id 1 first. That answer
    // says nothing about room, so the permit passes on at once, and the
    // next probe, for id 2, finds the pool full.
    relay.tick(NO_ROOM_FIRST);
    assert_eq!(
        relay.wanted().entry(id(1)).unwrap().retry_at,
        NO_ROOM_FIRST + RETRY_FIRST * 2
    );
    assert_eq!(
        relay.wanted().backoff_until(NO_ROOM_FIRST),
        Some(NO_ROOM_FIRST * 3)
    );
    relay.assert_invariants();
}

#[test]
fn an_ended_backoff_with_nobody_to_probe_is_not_a_deadline() {
    let mut relay = relay();
    fill_pool(&mut relay);
    let a = open(&mut relay, 1, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    relay.end(a, secs(0));
    relay.tick(NO_ROOM_FIRST);
    assert!(relay
        .next_deadline()
        .is_none_or(|next| next > NO_ROOM_FIRST));
    // A new session probes at once.
    let b = open(&mut relay, 2, true, secs(2));
    relay.inv(b, &[(id(1), SIZE)], secs(2));
    assert_eq!(
        relay.wanted().backoff_until(secs(2)),
        Some(secs(2) + NO_ROOM_FIRST * 2)
    );
    relay.assert_invariants();
}

#[test]
fn a_late_or_cooling_body_is_dropped_unread() {
    // After a deadline.
    let mut relay = relay();
    let a = open(&mut relay, 1, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    relay.tick(deadline(SIZE, secs(0)));
    relay.tx(a, id(1), tx(9), SIZE + 1, deadline(SIZE, secs(0)));
    assert!(closes(&relay.take_effects()).is_empty());
    assert_eq!(relay.in_flight(PeerKey(1)), 0);

    // After a cancellation.
    let mut relay = self::relay();
    let a = open(&mut relay, 1, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    let block = ModelTransition {
        disconnected: 0,
        connected: vec![ModelBlock {
            transactions: vec![tx(1)],
        }],
        tip: Height(101),
        generation: Generation(1),
        rules_change: false,
    };
    relay.transition(block, secs(1));
    relay.tx(a, id(1), tx(9), SIZE + 1, secs(2));
    assert!(closes(&relay.take_effects()).is_empty());

    // While its peer cools.
    let mut relay = self::relay();
    let a = open(&mut relay, 1, true, secs(0));
    relay.inv(a, &[(id(1), SIZE), (id(2), SIZE)], secs(0));
    relay.tx(a, id(1), tx(1), SIZE, secs(0));
    let bad = Outcome::Fail {
        scope: RejectionScope::ExactTip,
        charge: Charge::Cooldown,
    };
    finish_all(&mut relay, &bad, secs(0));
    relay.take_effects();
    relay.tx(a, id(2), tx(9), SIZE + 1, secs(1));
    assert!(closes(&relay.take_effects()).is_empty());
    relay.assert_invariants();
}

#[test]
fn a_waiting_session_keeps_distinct_ids_only() {
    let mut relay = relay();
    let old = open(&mut relay, 1, true, secs(0));
    let new = open(&mut relay, 1, true, secs(0));
    let copies: Vec<_> = (0..PENDING_PER_PEER).map(|_| (id(1), SIZE)).collect();
    relay.inv(new, &copies, secs(0));
    relay.inv(new, &[(id(2), SIZE), (id(3), MAX_TX_BYTES + 1)], secs(0));
    assert_eq!(relay.session(new).unwrap().waiting.len(), 2);
    relay.take_effects();
    relay.end(old, secs(1));
    assert_eq!(
        asks(&relay.take_effects()),
        vec![(new, id(1)), (new, id(2))]
    );
}

#[test]
fn new_work_never_eats_into_an_older_deadline() {
    let mut relay = relay();
    let a = open(&mut relay, 1, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    let first = relay.session(a).unwrap().requests[&id(1)].deadline;
    let later = first - secs(1);
    relay.inv(a, &[(id(2), MAX_TX_BYTES)], later);
    let bytes = u64::from(SIZE) + u64::from(MAX_TX_BYTES) + SENDER_ALLOWANCE;
    let span = BASE_DEADLINE + Duration::from_millis(bytes * 1_000 / MIN_RATE);
    assert_eq!(
        relay.session(a).unwrap().requests[&id(1)].deadline,
        later + span
    );
}

#[test]
fn fan_out_uses_the_size_of_the_body_admitted() {
    let mut relay = relay();
    let liar = open(&mut relay, 1, true, secs(0));
    let supplier = open(&mut relay, 2, true, secs(0));
    let peers: Vec<_> = (10..30)
        .map(|p| open(&mut relay, p, true, secs(0)))
        .collect();
    relay.set_ranking(peers.clone());
    relay.inv(liar, &[(id(1), 100)], secs(0));
    relay.inv(supplier, &[(id(1), MAX_TX_BYTES)], secs(0));
    relay.not_found(liar, id(1), secs(1));
    relay.tx(supplier, id(1), tx(1), MAX_TX_BYTES, secs(1));
    relay.take_effects();
    finish_all(&mut relay, &Outcome::Pass, secs(1));
    let now: HashSet<_> = adverts(&relay.take_effects())
        .into_iter()
        .map(|(s, _)| s)
        .collect();
    assert_eq!(now, peers[..WINDOW_FULL].iter().copied().collect());
    relay.assert_invariants();
}

#[test]
fn a_late_verdict_from_an_ended_session_leaves_a_newer_entry_alone() {
    let mut relay = relay();
    let a = open(&mut relay, 1, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    relay.tx(a, id(1), tx(1), SIZE, secs(0));
    relay.end(a, secs(1));
    assert_eq!(relay.reactor().unsettled_count(PeerKey(1)), 1);
    let b = open(&mut relay, 2, true, secs(1));
    relay.inv(b, &[(id(1), SIZE)], secs(1));
    let newer = relay.wanted().entry(id(1)).unwrap().generation;
    let rejected = Outcome::Fail {
        scope: RejectionScope::ExactTip,
        charge: Charge::None,
    };
    finish_all(&mut relay, &rejected, secs(1));
    // The old submission's verdict decides only its own generation.
    assert_eq!(relay.wanted().entry(id(1)).unwrap().generation, newer);
    assert_eq!(relay.reactor().unsettled_peers(), 0);
    // B's own reserve then learns of the rejection.
    relay.tick(secs(1) + RETRY_FIRST);
    assert!(relay.wanted().entry(id(1)).is_none());
    relay.assert_invariants();
}

#[test]
fn a_timeout_hands_the_id_to_a_peer_that_holds_four() {
    let mut relay = relay();
    let a = open(&mut relay, 1, true, secs(0));
    let b = open(&mut relay, 2, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    relay.inv(
        b,
        &[
            (id(10), SIZE),
            (id(11), SIZE),
            (id(12), SIZE),
            (id(13), SIZE),
        ],
        secs(0),
    );
    relay.inv(b, &[(id(1), SIZE), (id(14), SIZE)], secs(0));
    relay.take_effects();
    assert_eq!(relay.in_flight(PeerKey(2)), 5);
    // B is full, so it cannot take id 1 even after A's deadline.
    relay.tick(deadline(SIZE, secs(0)));
    assert!(asks(&relay.take_effects()).is_empty());
    let mut relay = self::relay();
    let a = open(&mut relay, 1, true, secs(0));
    let b = open(&mut relay, 2, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    relay.inv(
        b,
        &[
            (id(10), SIZE),
            (id(11), SIZE),
            (id(12), SIZE),
            (id(13), SIZE),
        ],
        secs(0),
    );
    relay.inv(b, &[(id(1), SIZE)], secs(0));
    relay.take_effects();
    assert_eq!(relay.in_flight(PeerKey(2)), 4);
    relay.tick(deadline(SIZE, secs(0)));
    assert_eq!(asks(&relay.take_effects()), vec![(b, id(1))]);
    assert_eq!(relay.in_flight(PeerKey(2)), 5);
    relay.assert_invariants();
}

#[test]
fn sessions_without_work_leave_no_reactor_record() {
    let mut relay = relay();
    for p in 0..10 {
        let session = open(&mut relay, p, true, secs(0));
        relay.end(session, secs(0));
    }
    assert_eq!(relay.reactor().unsettled_peers(), 0);
}

#[test]
fn a_cooldown_needs_the_tip_it_was_earned_at() {
    let mut relay = relay();
    let a = open(&mut relay, 1, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    relay.tx(a, id(1), tx(1), SIZE, secs(0));
    // The node's best tip moved on before the pool applied it.
    relay.set_best_tip(Height(101));
    let bad = Outcome::Fail {
        scope: RejectionScope::ExactTip,
        charge: Charge::Cooldown,
    };
    finish_all(&mut relay, &bad, secs(0));
    assert!(!relay.reactor().is_cooling(PeerKey(1), secs(0)));
}

#[test]
fn timeout_history_lasts_only_while_the_id_is_pending() {
    let mut relay = relay();
    let a = open(&mut relay, 1, true, secs(0));
    let mut now = secs(0);
    for n in 0..50 {
        relay.inv(a, &[(id(n), SIZE)], now);
        now = relay.session(a).unwrap().requests[&id(n)].deadline;
        relay.tick(now);
        relay.not_found(a, id(n), now);
        assert!(relay.session(a).unwrap().timed_out.is_empty());
    }
    relay.assert_invariants();
}

/// Fills the pool with another caller's reservations.
fn fill_pool(relay: &mut RelayModel) {
    for n in 0..u32::try_from(MAX_PENDING).unwrap() {
        relay.outside(secs(0), |pool, now| {
            let answer = pool.reserve(id(100_000 + n), Intent::Fetch, now);
            (answer.expect("room").id, pool.effects())
        });
    }
}

/// A small deterministic generator, so a failing seed can be replayed.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self, bound: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % bound.max(1)
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum Behaviour {
    /// Answers every request within a second, and only announces what it
    /// has.
    Honest,
    /// Never answers.
    Silent,
    /// Answers every request with `NotFound`.
    Liar,
}

/// Random inputs, then time with nothing else happening. Every id an honest
/// peer announced must be decided, and nothing may be left held.
#[test]
fn random_runs_drain() {
    for seed in 0..200 {
        run(seed);
    }
}

fn run(seed: u64) {
    let mut rng = Lcg(seed);
    let manual = seed % 2 == 1;
    let mut relay = RelayModel::new(PoolModel::new(Height(100), Generation(0)).with_limits(8, 2));
    if manual {
        relay = relay.with_manual_reserves().with_manual_reads(6);
    }
    let behaviours = [
        Behaviour::Honest,
        Behaviour::Honest,
        Behaviour::Honest,
        Behaviour::Silent,
        Behaviour::Liar,
    ];
    let mut live: Vec<(SessionId, Behaviour)> = Vec::new();
    let mut replies: Vec<(Duration, SessionId, UnminedTxId)> = Vec::new();
    // What each honest session announced: the oracle, kept apart from the
    // model's own table.
    let mut honest: HashMap<SessionId, HashSet<UnminedTxId>> = HashMap::new();
    let mut failed: HashSet<UnminedTxId> = HashSet::new();
    let mut now = Duration::ZERO;

    let answer = |relay: &mut RelayModel,
                  replies: &mut Vec<(Duration, SessionId, UnminedTxId)>,
                  live: &[(SessionId, Behaviour)],
                  now: Duration,
                  rng: &mut Lcg| {
        for effect in relay.take_effects() {
            if let RelayEffect::Ask { session, id } = effect {
                let behaviour = live.iter().find(|(s, _)| *s == session).map(|(_, b)| *b);
                if matches!(behaviour, Some(Behaviour::Honest | Behaviour::Liar)) {
                    let delay = Duration::from_millis(rng.next(1_000));
                    replies.push((now + delay, session, id));
                }
            }
        }
    };
    let verify = |relay: &mut RelayModel,
                  failed: &mut HashSet<UnminedTxId>,
                  rng: &mut Lcg,
                  now: Duration,
                  fail_some: bool| {
        for (job, tx_id) in relay.job_txs().to_vec() {
            let outcome = if fail_some && rng.next(5) == 0 {
                failed.insert(tx_id);
                Outcome::Fail {
                    scope: RejectionScope::ExactTip,
                    charge: Charge::None,
                }
            } else {
                Outcome::Pass
            };
            relay.outcome(job, outcome, now);
        }
    };

    for _ in 0..150 {
        now += Duration::from_millis(rng.next(1_500));
        match rng.next(12) {
            0 | 1 if live.len() < 6 => {
                let peer = rng.next(5);
                let behaviour = behaviours[usize::try_from(peer).unwrap()];
                let session = relay.connect(PeerKey(peer), rng.next(2) == 0, now);
                relay.hello(session, hello(), now);
                live.push((session, behaviour));
            }
            2 if !live.is_empty() => {
                let index = usize::try_from(rng.next(u64::try_from(live.len()).unwrap())).unwrap();
                let (session, _) = live.remove(index);
                honest.remove(&session);
                relay.end(session, now);
            }
            3..=6 if !live.is_empty() => {
                let index = usize::try_from(rng.next(u64::try_from(live.len()).unwrap())).unwrap();
                let (session, behaviour) = live[index];
                let n = u32::try_from(rng.next(30)).unwrap();
                relay.inv(session, &[(id(n), SIZE)], now);
                if behaviour == Behaviour::Honest {
                    honest.entry(session).or_default().insert(id(n));
                }
            }
            7 => verify(&mut relay, &mut failed, &mut rng, now, true),
            8 if manual => {
                relay.answer_reserve(now);
            }
            9 if manual && !live.is_empty() => {
                let index = usize::try_from(rng.next(u64::try_from(live.len()).unwrap())).unwrap();
                relay.read(live[index].0, now);
            }
            _ => relay.tick(now),
        }
        answer(&mut relay, &mut replies, &live, now, &mut rng);
        deliver(&mut relay, &mut replies, &live, now);
        relay.assert_invariants();
    }

    // Quiet time: only answers, reserve replies, reads, verdicts and the
    // clock.
    let mut drained = false;
    for _ in 0..5_000 {
        while relay.answer_reserve(now) {}
        for (session, _) in &live {
            relay.read(*session, now);
        }
        answer(&mut relay, &mut replies, &live, now, &mut rng);
        deliver(&mut relay, &mut replies, &live, now);
        verify(&mut relay, &mut failed, &mut rng, now, false);
        answer(&mut relay, &mut replies, &live, now, &mut rng);
        relay.assert_invariants();
        if !relay.jobs().is_empty() || relay.calls_in_flight() > 0 {
            continue;
        }
        let next_reply = replies.iter().map(|(at, _, _)| *at).min();
        let Some(next) = [relay.next_deadline(), next_reply]
            .into_iter()
            .flatten()
            .min()
        else {
            drained = true;
            break;
        };
        assert!(
            next > now,
            "seed {seed}: a deadline that is due must not be returned"
        );
        now = next;
        relay.tick(now);
    }
    assert!(drained, "seed {seed}: quiet time reached a resting state");

    assert_eq!(
        relay.reservations_held(),
        0,
        "seed {seed}: no reservation is left"
    );
    assert_eq!(
        relay.reactor().unsettled_peers(),
        0,
        "seed {seed}: every submission settled"
    );
    let visible: HashSet<UnminedTxId> = relay.pool().visible().into_iter().collect();
    for (session, behaviour) in &live {
        let state = relay.session(*session).expect("live");
        if *behaviour != Behaviour::Silent {
            assert!(
                state.requests.is_empty(),
                "seed {seed}: only silent peers keep requests"
            );
        }
        if *behaviour == Behaviour::Honest && relay.reactor().has_go(state.peer, *session) {
            for announced in honest.get(session).into_iter().flatten() {
                assert!(
                    visible.contains(announced) || failed.contains(announced),
                    "seed {seed}: {announced:?} from an honest announcer was not decided"
                );
            }
        }
    }
    for (id, entry) in relay.wanted().entries() {
        assert!(
            entry.claim.is_none(),
            "seed {seed}: {id:?} is still claimed"
        );
    }
    // Sizes for fan-out are kept only for what the pool holds.
    assert!(relay.sizes_kept() <= visible.len() + relay.pool().hidden().len());
}

/// Delivers the scripted answers that are due.
fn deliver(
    relay: &mut RelayModel,
    replies: &mut Vec<(Duration, SessionId, UnminedTxId)>,
    live: &[(SessionId, Behaviour)],
    now: Duration,
) {
    let (due, later): (Vec<_>, Vec<_>) = replies.drain(..).partition(|(at, _, _)| *at <= now);
    *replies = later;
    for (_, session, asked) in due {
        let Some((_, behaviour)) = live.iter().find(|(s, _)| *s == session) else {
            continue;
        };
        match behaviour {
            Behaviour::Honest => relay.tx(session, asked, ModelTx::new(asked), SIZE, now),
            Behaviour::Liar => relay.not_found(session, asked, now),
            Behaviour::Silent => {}
        }
    }
}
