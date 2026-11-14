//! The cases the M2 plan names, run on the model with the pool's model
//! behind it, and a random run that must drain.

use std::{collections::HashSet, time::Duration};

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
    // The first request's deadline grew when the second was sent, so it
    // covers either order of answers.
    let total = 2 * u64::from(big) + SENDER_ALLOWANCE;
    let span = BASE_DEADLINE + Duration::from_millis(total * 1_000 / MIN_RATE);
    assert_eq!(state.requests[&id(1)].deadline, span);
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
    assert!(relay.wanted().in_backoff(secs(0)));
    // The probe after the backoff finds no room again, and the backoff
    // doubles.
    relay.tick(NO_ROOM_FIRST);
    assert!(asks(&relay.take_effects()).is_empty());
    assert_eq!(relay.wanted().backoff_until(), Some(NO_ROOM_FIRST * 3));
    // Room comes back, and the next probe gets it.
    relay.outside(secs(2), |pool, now| ((), pool.release(held[0], now)));
    relay.tick(NO_ROOM_FIRST * 3);
    assert_eq!(asks(&relay.take_effects()), vec![(a, id(1))]);
    // That took the last slot, so the next claim finds the pool full again.
    assert!(relay.wanted().in_backoff(NO_ROOM_FIRST * 3));
    assert_eq!(relay.wanted().backoff_until(), Some(NO_ROOM_FIRST * 4));
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
    relay.set_may_cool(false);
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
fn a_resync_rebuilds_the_announced_set_and_keeps_the_sources() {
    let mut relay = relay();
    let supplier = open(&mut relay, 1, true, secs(0));
    let a = open(&mut relay, 2, true, secs(0));
    relay.set_ranking(vec![a, supplier]);
    fetch(&mut relay, supplier, 1, secs(0));
    fetch(&mut relay, supplier, 2, secs(0));
    relay.take_effects();
    relay.resync(a, secs(1));
    // A was told both already, so nothing is sent again.
    assert!(adverts(&relay.take_effects()).is_empty());
    // The supplier is told neither, because it supplied both.
    relay.resync(supplier, secs(1));
    assert!(adverts(&relay.take_effects()).is_empty());
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
    let mut relay = RelayModel::new(PoolModel::new(Height(100), Generation(0)).with_limits(8, 2));
    let behaviours = [
        Behaviour::Honest,
        Behaviour::Honest,
        Behaviour::Honest,
        Behaviour::Silent,
        Behaviour::Liar,
    ];
    let mut live: Vec<(SessionId, Behaviour)> = Vec::new();
    let mut replies: Vec<(Duration, SessionId, UnminedTxId)> = Vec::new();
    let mut honest_announced: HashSet<UnminedTxId> = HashSet::new();
    let mut now = Duration::ZERO;

    let answer = |relay: &mut RelayModel,
                  replies: &mut Vec<(Duration, SessionId, UnminedTxId)>,
                  live: &[(SessionId, Behaviour)],
                  now: Duration,
                  rng: &mut Lcg| {
        for effect in relay.take_effects() {
            if let RelayEffect::Ask { session, id } = effect {
                let behaviour = live.iter().find(|(s, _)| *s == session).map(|(_, b)| *b);
                match behaviour {
                    Some(Behaviour::Honest | Behaviour::Liar) => {
                        let delay = Duration::from_millis(rng.next(1_000));
                        replies.push((now + delay, session, id));
                    }
                    Some(Behaviour::Silent) | None => {}
                }
            }
        }
    };

    for _ in 0..150 {
        now += Duration::from_millis(rng.next(1_500));
        match rng.next(10) {
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
                relay.end(session, now);
            }
            3..=6 if !live.is_empty() => {
                let index = usize::try_from(rng.next(u64::try_from(live.len()).unwrap())).unwrap();
                let (session, behaviour) = live[index];
                let n = u32::try_from(rng.next(30)).unwrap();
                relay.inv(session, &[(id(n), SIZE)], now);
                if behaviour == Behaviour::Honest {
                    honest_announced.insert(id(n));
                }
            }
            7 => {
                for job in relay.jobs().to_vec() {
                    let outcome = if rng.next(5) == 0 {
                        Outcome::Fail {
                            scope: RejectionScope::ExactTip,
                            charge: Charge::None,
                        }
                    } else {
                        Outcome::Pass
                    };
                    relay.outcome(job, outcome, now);
                }
            }
            _ => relay.tick(now),
        }
        answer(&mut relay, &mut replies, &live, now, &mut rng);
        deliver(&mut relay, &mut replies, &live, now);
        relay.assert_invariants();
    }

    // Quiet time: only answers, verdicts and the clock.
    for _ in 0..5_000 {
        answer(&mut relay, &mut replies, &live, now, &mut rng);
        deliver(&mut relay, &mut replies, &live, now);
        for job in relay.jobs().to_vec() {
            relay.outcome(job, Outcome::Pass, now);
        }
        answer(&mut relay, &mut replies, &live, now, &mut rng);
        relay.assert_invariants();
        if !relay.jobs().is_empty() {
            continue;
        }
        let next_reply = replies.iter().map(|(at, _, _)| *at).min();
        let Some(next) = [relay.next_deadline(), next_reply]
            .into_iter()
            .flatten()
            .min()
        else {
            break;
        };
        now = now.max(next);
        relay.tick(now);
    }

    assert!(
        relay.jobs().is_empty(),
        "seed {seed}: verifications finished"
    );
    assert_eq!(
        relay.reservations_held(),
        0,
        "seed {seed}: no reservation is left"
    );
    for (id, entry) in relay.wanted().entries() {
        assert!(
            entry.claim.is_none(),
            "seed {seed}: {id:?} is still claimed"
        );
        for (session, behaviour) in &live {
            if *behaviour == Behaviour::Honest {
                let held = relay
                    .session(*session)
                    .is_some_and(|state| state.holds(*id, entry.generation));
                if held {
                    let state = relay.session(*session).unwrap();
                    panic!(
                        "seed {seed}: an honest announcer still holds {id:?}: entry {entry:?}, now {now:?}, next {:?}, go {}, cooling {}, requests {:?}, subs {}, unsettled {}, timed_out {:?}, hello {:?}, backoff {:?}",
                        relay.next_deadline(),
                        relay.reactor().has_go(state.peer, *session),
                        relay.reactor().is_cooling(state.peer, now),
                        state.requests,
                        state.submissions.len(),
                        relay.reactor().unsettled_count(state.peer),
                        state.timed_out,
                        state.hello,
                        relay.wanted().backoff_until(),
                    );
                }
            }
        }
    }
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
