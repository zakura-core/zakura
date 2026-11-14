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
    MIN_RATE, NO_ROOM_FIRST, PENDING_PER_PEER, RETRY_FIRST, SENDER_ALLOWANCE, TRIED_PER_PEER,
    WINDOW_FULL, WINDOW_SMALL,
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
    relay.tx(session, id(n), tx(n), now);
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

    relay.tx(a, id(1), tx(1), secs(1));
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
    relay.tx(a, id(1), tx(1), due + secs(1));
    assert_eq!(relay.pool().bodies_held(), 0);
    assert_eq!(relay.in_flight(PeerKey(1)), 0);
    assert!(relay.take_effects().is_empty());

    relay.tx(b, id(1), tx(1), due + secs(2));
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
    relay.tx(a, id(1), tx(1), due);
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
    relay.tx(a, id(1), tx(1), first + secs(1));
    relay.tick(first + secs(1) + INBOUND_DELAY);
    let again = first + secs(1) + INBOUND_DELAY;
    assert_eq!(asks(&relay.take_effects()), vec![(a, id(1))]);
    let request = relay.session(a).unwrap().requests[&id(1)];
    assert_eq!(request.deadline, again + (first - secs(0)) * 2);
    // This time it answers in time.
    relay.tx(a, id(1), tx(1), again + secs(20));
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
    relay.tx(honest, id(1), tx(1), secs(2));
    assert!(closes(&relay.take_effects()).is_empty());
    finish_all(&mut relay, &Outcome::Pass, secs(2));
    assert_eq!(relay.pool().visible(), vec![id(1)]);

    // The other order: the honest size first, and a body of another size
    // from the peer that announced it is that peer's violation. The honest
    // peer, asked first, has its body accepted.
    let mut relay = self::relay();
    let honest = open(&mut relay, 1, true, secs(0));
    let hostile = open(&mut relay, 2, true, secs(0));
    relay.inv(hostile, &[(id(1), 100)], secs(0));
    relay.inv(honest, &[(id(1), SIZE)], secs(0));
    relay.take_effects();
    relay.tx(hostile, id(1), tx(1), secs(1));
    assert_eq!(
        closes(&relay.take_effects()),
        vec![(hostile, CloseReason::Violation(Violation::WrongSize))]
    );
    relay.assert_invariants();

    let mut relay = self::relay();
    let honest = open(&mut relay, 1, true, secs(0));
    let hostile = open(&mut relay, 2, true, secs(0));
    relay.inv(honest, &[(id(1), SIZE)], secs(0));
    relay.inv(hostile, &[(id(1), 100)], secs(0));
    assert_eq!(asks(&relay.take_effects()), vec![(honest, id(1))]);
    relay.tx(honest, id(1), tx(1), secs(1));
    finish_all(&mut relay, &Outcome::Pass, secs(1));
    assert!(closes(&relay.take_effects()).is_empty());
    assert_eq!(relay.pool().visible(), vec![id(1)]);
    let _ = hostile;
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
    relay.tx(supplier, id(1), tx(1), secs(1));
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
    relay.tx(supplier, id(3), tx(3), later);
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
    relay.tx(a, id(1), tx(1), secs(0));
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
    relay.tx(a, id(1), tx(1), secs(0));
    let bad = Outcome::Fail {
        scope: RejectionScope::ExactTip,
        charge: Charge::Cooldown,
    };
    finish_all(&mut relay, &bad, secs(0));
    relay.tx(a, id(2), tx(2), secs(1));
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
    relay.tx(a, id(1), tx(1), secs(0));
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
        relay.tx(old, id(n), tx(n), secs(0));
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
    // The token does not say why, so the fetch ends and the id stays wanted
    // until the pool is asked again.
    assert!(relay.wanted().entry(id(1)).unwrap().claim.is_none());
    assert_eq!(relay.reservations_held(), 0);
    // The request stays outstanding, and its answer is dropped.
    assert_eq!(relay.in_flight(PeerKey(1)), 1);
    relay.tx(a, id(1), tx(1), secs(2));
    assert_eq!(relay.in_flight(PeerKey(1)), 0);
    // Asked again, the pool says a block mined it, which decides the id.
    assert!(relay.wanted().entry(id(1)).is_none());
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
    relay.tx(a, id(1), tx(1), secs(2));
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
    relay.tx(a, id(1), tx(1), secs(0));
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
            relay.tx(session, asked_id, tx(n), now);
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
    relay.tx(supplier, tx_id, ModelTx::new(tx_id), secs(0));
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
    relay.tx(c, id(1), tx(1), secs(0));
    assert_eq!(
        closes(&relay.take_effects()),
        vec![(c, CloseReason::Violation(Violation::Unrequested))]
    );
    let d = open(&mut relay, 4, true, secs(0));
    relay.inv(d, &[(id(5), SIZE)], secs(0));
    relay.take_effects();
    relay.tx(d, id(5), tx(6), secs(0));
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
    relay.tx(a, id(1), tx(9), deadline(SIZE, secs(0)));
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
    relay.tx(a, id(1), tx(9), secs(2));
    assert!(closes(&relay.take_effects()).is_empty());

    // While its peer cools.
    let mut relay = self::relay();
    let a = open(&mut relay, 1, true, secs(0));
    relay.inv(a, &[(id(1), SIZE), (id(2), SIZE)], secs(0));
    relay.tx(a, id(1), tx(1), secs(0));
    let bad = Outcome::Fail {
        scope: RejectionScope::ExactTip,
        charge: Charge::Cooldown,
    };
    finish_all(&mut relay, &bad, secs(0));
    relay.take_effects();
    relay.tx(a, id(2), tx(9), secs(1));
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
    let mut relay = relay().with_tx_size(one_is_big);
    let liar = open(&mut relay, 1, true, secs(0));
    let supplier = open(&mut relay, 2, true, secs(0));
    let peers: Vec<_> = (10..30)
        .map(|p| open(&mut relay, p, true, secs(0)))
        .collect();
    relay.set_ranking(peers.clone());
    relay.inv(liar, &[(id(1), 100)], secs(0));
    relay.inv(supplier, &[(id(1), MAX_TX_BYTES)], secs(0));
    relay.not_found(liar, id(1), secs(1));
    relay.tx(supplier, id(1), tx(1), secs(1));
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
    relay.tx(a, id(1), tx(1), secs(0));
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
    relay.tx(a, id(1), tx(1), secs(0));
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

#[test]
fn an_old_call_never_acts_on_a_newer_one() {
    // Cancelled before the pool processed it.
    let mut relay =
        RelayModel::new(PoolModel::new(Height(100), Generation(0))).with_manual_reserves();
    let a = open(&mut relay, 1, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    relay.cancel_reserve(a, id(1), secs(0));
    // A claims the id again at once, with a new call.
    assert_eq!(relay.calls_in_flight(), 2);
    while relay.answer_reserve(secs(0)) {}
    assert_eq!(asks(&relay.take_effects()), vec![(a, id(1))]);
    assert_eq!(relay.reservations_held(), 1);
    assert_eq!(
        relay.pool().slots_in_use(),
        1,
        "the old call never reached the pool"
    );
    relay.assert_invariants();

    // Cancelled after the pool granted it.
    let mut relay =
        RelayModel::new(PoolModel::new(Height(100), Generation(0))).with_manual_reserves();
    let a = open(&mut relay, 1, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    assert!(relay.process_reserve(secs(0)));
    relay.cancel_reserve(a, id(1), secs(0));
    // The new call is refused, because the old grant still holds the id.
    assert!(relay.process_reserve(secs(0)));
    // The old grant is given back, not used for the new call.
    assert!(relay.deliver_reserve(secs(0)));
    assert!(asks(&relay.take_effects()).is_empty());
    assert!(relay.deliver_reserve(secs(0)));
    relay.tick(RETRY_FIRST);
    while relay.answer_reserve(RETRY_FIRST) {}
    assert_eq!(asks(&relay.take_effects()), vec![(a, id(1))]);
    assert_eq!(relay.pool().slots_in_use(), 1);
    relay.assert_invariants();
}

#[test]
fn a_grant_cancelled_by_a_restart_decides_nothing() {
    let mut relay =
        RelayModel::new(PoolModel::new(Height(100), Generation(0))).with_manual_reserves();
    let a = open(&mut relay, 1, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    assert!(relay.process_reserve(secs(0)));
    relay.deactivate(secs(1));
    relay.activate(secs(2));
    assert!(relay.deliver_reserve(secs(2)));
    assert!(relay.wanted().entry(id(1)).is_some());
    while relay.answer_reserve(secs(2)) {}
    assert_eq!(asks(&relay.take_effects()), vec![(a, id(1))]);
    relay.assert_invariants();
}

#[test]
fn a_rejection_the_tip_moved_past_keeps_the_id_wanted() {
    // A late verdict.
    let mut relay = relay().with_manual_verdicts();
    let a = open(&mut relay, 1, true, secs(0));
    let b = open(&mut relay, 2, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    relay.inv(b, &[(id(1), SIZE)], secs(0));
    relay.tx(a, id(1), tx(1), secs(0));
    let rejected = Outcome::Fail {
        scope: RejectionScope::ExactTip,
        charge: Charge::None,
    };
    finish_all(&mut relay, &rejected, secs(0));
    let block = ModelTransition {
        disconnected: 0,
        connected: vec![ModelBlock::default()],
        tip: Height(101),
        generation: Generation(1),
        rules_change: false,
    };
    relay.transition(block, secs(1));
    relay.take_effects();
    assert!(relay.deliver_verdict(secs(1)));
    assert!(relay
        .session(b)
        .unwrap()
        .holds(id(1), relay.wanted().entry(id(1)).unwrap().generation));
    relay.tick(secs(1) + RETRY_FIRST);
    assert_eq!(asks(&relay.take_effects()).len(), 1);
    relay.assert_invariants();

    // A late cached-rejection answer.
    let mut relay =
        RelayModel::new(PoolModel::new(Height(100), Generation(0))).with_manual_reserves();
    let supplier = open(&mut relay, 1, true, secs(0));
    relay.inv(supplier, &[(id(1), SIZE)], secs(0));
    while relay.answer_reserve(secs(0)) {}
    relay.tx(supplier, id(1), tx(1), secs(0));
    finish_all(&mut relay, &rejected, secs(0));
    let b = open(&mut relay, 2, true, secs(0));
    relay.inv(b, &[(id(1), SIZE)], secs(0));
    assert!(relay.process_reserve(secs(0)));
    let block = ModelTransition {
        disconnected: 0,
        connected: vec![ModelBlock::default()],
        tip: Height(101),
        generation: Generation(1),
        rules_change: false,
    };
    relay.transition(block, secs(1));
    assert!(relay.deliver_reserve(secs(1)));
    assert!(
        relay.wanted().entry(id(1)).is_some(),
        "the old rejection does not decide it"
    );
    relay.assert_invariants();
}

#[test]
fn a_late_grant_asks_nothing_of_a_cooling_peer() {
    let mut relay =
        RelayModel::new(PoolModel::new(Height(100), Generation(0))).with_manual_reserves();
    let a = open(&mut relay, 1, true, secs(0));
    relay.inv(a, &[(id(1), SIZE), (id(2), SIZE)], secs(0));
    assert!(relay.process_reserve(secs(0)));
    assert!(relay.process_reserve(secs(0)));
    assert!(relay.deliver_reserve(secs(0)));
    assert_eq!(asks(&relay.take_effects()), vec![(a, id(1))]);
    relay.tx(a, id(1), tx(1), secs(0));
    let bad = Outcome::Fail {
        scope: RejectionScope::ExactTip,
        charge: Charge::Cooldown,
    };
    finish_all(&mut relay, &bad, secs(0));
    assert!(relay.reactor().is_cooling(PeerKey(1), secs(0)));
    assert!(relay.deliver_reserve(secs(0)));
    assert!(asks(&relay.take_effects()).is_empty());
    assert_eq!(relay.reservations_held(), 0);
    relay.assert_invariants();
}

#[test]
fn a_late_body_arrives_while_another_fetches_and_after_its_verdict() {
    for after_verdict in [false, true] {
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
        relay.inv(b, &[(id(1), SIZE)], secs(0));
        relay.take_effects();
        let due = deadline(SIZE, secs(0));
        relay.tick(due);
        assert_eq!(asks(&relay.take_effects()), vec![(b, id(1))]);
        if after_verdict {
            relay.tx(b, id(1), tx(1), due);
            finish_all(&mut relay, &Outcome::Pass, due);
        }
        relay.tx(a, id(1), tx(1), due + secs(1));
        assert_eq!(
            relay.pool().bodies_held(),
            0,
            "the late body never reaches the pool"
        );
        assert_eq!(relay.in_flight(PeerKey(1)), 0);
        if !after_verdict {
            relay.tx(b, id(1), tx(1), due + secs(1));
            finish_all(&mut relay, &Outcome::Pass, due + secs(1));
        }
        assert_eq!(relay.pool().visible(), vec![id(1)]);
        relay.assert_invariants();
    }
}

#[test]
fn repeated_log_overruns_across_hides_reveals_and_removals() {
    let mut relay =
        RelayModel::new(PoolModel::new(Height(100), Generation(0))).with_manual_reads(2);
    let supplier = open(&mut relay, 1, true, secs(0));
    let a = open(&mut relay, 2, true, secs(0));
    relay.set_ranking(vec![a]);
    let mut now = secs(0);
    let mut height = 100;
    for round in 0..4u32 {
        for n in 0..3 {
            fetch(&mut relay, supplier, round * 10 + n, now);
        }
        height += 1;
        let reorg = ModelTransition {
            disconnected: 1,
            connected: vec![ModelBlock::default(), ModelBlock::default()],
            tip: Height(height),
            generation: Generation(u64::from(round) * 2 + 1),
            rules_change: false,
        };
        relay.transition(reorg, now);
        let mined = round * 10;
        height += 1;
        let block = ModelTransition {
            disconnected: 0,
            connected: vec![ModelBlock {
                transactions: vec![tx(mined)],
            }],
            tip: Height(height),
            generation: Generation(u64::from(round) * 2 + 2),
            rules_change: false,
        };
        relay.transition(block, now);
        relay.read(a, now);
        relay.assert_invariants();
        finish_all(&mut relay, &Outcome::Pass, now);
        relay.read(a, now);
        relay.assert_invariants();
        now += secs(1);
    }
    // After the last read, A was told exactly what the pool holds.
    let announced: HashSet<_> = relay
        .session(a)
        .unwrap()
        .announced
        .keys()
        .copied()
        .collect();
    let visible: HashSet<_> = relay.pool().visible().into_iter().collect();
    assert_eq!(announced, visible);
}

#[test]
fn more_than_a_pool_of_ids_through_one_session() {
    let mut relay = relay();
    let a = open(&mut relay, 1, true, secs(0));
    let b = open(&mut relay, 2, true, secs(0));
    let count = u32::try_from(TRIED_PER_PEER).unwrap() + 500;
    for n in 0..count {
        relay.inv(a, &[(id(n), SIZE)], secs(0));
        relay.not_found(a, id(n), secs(0));
    }
    assert_eq!(relay.session(a).unwrap().tried.len(), TRIED_PER_PEER);
    relay.take_effects();
    assert_eq!(relay.wanted().len(), 0);
    // The oldest tried mark was forgotten, so A may be asked about it once
    // more, alongside B.
    relay.inv(b, &[(id(0), SIZE)], secs(0));
    relay.inv(a, &[(id(0), SIZE)], secs(0));
    assert_eq!(asks(&relay.take_effects()).len(), 1);
    relay.assert_invariants();
}

#[test]
fn an_admission_published_before_the_handoff_is_in_the_opening_announcement() {
    let mut relay = relay();
    let old = open(&mut relay, 1, true, secs(0));
    let other = open(&mut relay, 2, true, secs(0));
    let new = open(&mut relay, 1, true, secs(0));
    // Another peer's transaction is admitted while the new session waits.
    fetch(&mut relay, other, 7, secs(0));
    assert!(adverts(&relay.take_effects())
        .iter()
        .all(|(session, _)| *session != new));
    relay.end(old, secs(1));
    assert_eq!(adverts(&relay.take_effects()), vec![(new, id(7))]);
    relay.assert_invariants();
}

#[test]
fn a_short_id_collision_only_skips_one_advert_to_one_peer() {
    fn collide(tx_id: UnminedTxId) -> u64 {
        // Ids 1 and 2 share a short id.
        let n = u64::from(u32::from_le_bytes(
            tx_id.mined_id().0[..4].try_into().unwrap(),
        ));
        if n == 2 {
            1
        } else {
            n
        }
    }
    let mut relay = relay().with_short_id(collide);
    let supplier = open(&mut relay, 1, true, secs(0));
    let a = open(&mut relay, 2, true, secs(0));
    let b = open(&mut relay, 3, true, secs(0));
    relay.set_ranking(vec![a, b]);
    // A announced id 1, so its skip set holds the short id 1.
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    relay.take_effects();
    fetch(&mut relay, supplier, 2, secs(0));
    // Id 2 collides, so A is not told. B is. Serving still uses full ids.
    assert_eq!(adverts(&relay.take_effects()), vec![(b, id(2))]);
    relay.get_txs(a, id(2), secs(1));
    assert_eq!(
        relay.take_effects(),
        vec![RelayEffect::NotFound {
            session: a,
            id: id(2)
        }]
    );
    relay.assert_invariants();
}

#[test]
fn deadlines_keep_doubling_until_time_runs_out() {
    let mut relay = relay();
    let a = open(&mut relay, 1, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    let one = relay.session(a).unwrap().requests[&id(1)].deadline;
    let mut state = relay.session(a).unwrap().clone();
    let pending = state.pending[0];
    state.requests.clear();
    state.timed_out.insert((pending.id, pending.generation), 33);
    state.send(
        pending,
        zakura_node_services::mempool::port::ReservationId(1),
        secs(0),
    );
    let thirty_three = state.requests[&id(1)].deadline;
    state.requests.clear();
    state.timed_out.insert((pending.id, pending.generation), 32);
    state.send(
        pending,
        zakura_node_services::mempool::port::ReservationId(1),
        secs(0),
    );
    let thirty_two = state.requests[&id(1)].deadline;
    assert!(thirty_three > thirty_two && thirty_two > one);
    state.requests.clear();
    state
        .timed_out
        .insert((pending.id, pending.generation), 200);
    state.send(
        pending,
        zakura_node_services::mempool::port::ReservationId(1),
        secs(0),
    );
    assert_eq!(state.requests[&id(1)].deadline, Duration::MAX);
}

/// A one-block reorg that ends at the same height.
fn same_height_reorg(tip: u32, generation: u64) -> ModelTransition {
    ModelTransition {
        disconnected: 1,
        connected: vec![ModelBlock::default()],
        tip: Height(tip),
        generation: Generation(generation),
        rules_change: false,
    }
}

#[test]
fn a_same_height_reorg_still_counts_as_a_chain_change() {
    let rejected = Outcome::Fail {
        scope: RejectionScope::ExactTip,
        charge: Charge::None,
    };
    // A late verdict.
    let mut relay = relay().with_manual_verdicts();
    let a = open(&mut relay, 1, true, secs(0));
    let b = open(&mut relay, 2, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    relay.inv(b, &[(id(1), SIZE)], secs(0));
    relay.tx(a, id(1), tx(1), secs(0));
    finish_all(&mut relay, &rejected, secs(0));
    relay.transition(same_height_reorg(100, 1), secs(1));
    assert!(relay.deliver_verdict(secs(1)));
    let entry = relay.wanted().entry(id(1)).expect("still wanted");
    assert!(relay.session(b).unwrap().holds(id(1), entry.generation));
    relay.assert_invariants();

    // A late cached-rejection answer.
    let mut relay =
        RelayModel::new(PoolModel::new(Height(100), Generation(0))).with_manual_reserves();
    let a = open(&mut relay, 1, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    while relay.answer_reserve(secs(0)) {}
    relay.tx(a, id(1), tx(1), secs(0));
    finish_all(&mut relay, &rejected, secs(0));
    let b = open(&mut relay, 2, true, secs(0));
    relay.inv(b, &[(id(1), SIZE)], secs(0));
    assert!(relay.process_reserve(secs(0)));
    relay.transition(same_height_reorg(100, 1), secs(1));
    assert!(relay.deliver_reserve(secs(1)));
    assert!(relay.wanted().entry(id(1)).is_some());
    relay.assert_invariants();

    // A grant cancelled by a block that a same-height reorg then replaced.
    let mut relay =
        RelayModel::new(PoolModel::new(Height(100), Generation(0))).with_manual_reserves();
    let a = open(&mut relay, 1, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    assert!(relay.process_reserve(secs(0)));
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
    relay.transition(same_height_reorg(101, 2), secs(1));
    assert!(relay.deliver_reserve(secs(1)));
    while relay.answer_reserve(secs(1)) {}
    assert_eq!(asks(&relay.take_effects()), vec![(a, id(1))]);
    relay.assert_invariants();
}

#[test]
fn an_admission_from_before_a_restart_does_not_settle_a_fresh_announcement() {
    let mut relay = relay().with_manual_verdicts();
    let a = open(&mut relay, 1, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    relay.take_effects();
    relay.tx(a, id(1), tx(1), secs(0));
    finish_all(&mut relay, &Outcome::Pass, secs(0));
    relay.deactivate(secs(1));
    relay.activate(secs(2));
    let b = open(&mut relay, 2, true, secs(2));
    relay.inv(b, &[(id(1), SIZE)], secs(2));
    while relay.deliver_verdict(secs(2)) {}
    // The restarted pool is empty, so the old admission settles nothing:
    // the id stays wanted, B still holds it, and it is fetched again.
    let entry = relay.wanted().entry(id(1)).cloned().expect("still wanted");
    assert!(relay.session(b).unwrap().holds(id(1), entry.generation));
    assert_eq!(asks(&relay.take_effects()), vec![(a, id(1))]);
    relay.assert_invariants();
}

#[test]
fn a_call_given_up_after_it_was_sent_still_runs() {
    let mut relay = RelayModel::new(PoolModel::new(Height(100), Generation(0)).with_limits(1, 1))
        .with_manual_reserves();
    let a = open(&mut relay, 1, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    relay.end(a, secs(0));
    let b = open(&mut relay, 2, true, secs(0));
    relay.inv(b, &[(id(2), SIZE)], secs(0));
    // A's call was already sent, so the pool runs it and takes the one slot.
    assert!(relay.process_reserve(secs(0)));
    assert!(relay.process_reserve(secs(0)));
    assert_eq!(relay.pool().slots_in_use(), 1);
    // Its answer finds nobody, and the slot goes back.
    while relay.deliver_reserve(secs(0)) {}
    assert_eq!(relay.pool().slots_in_use(), 0);
    assert!(relay.wanted().in_backoff());
    relay.tick(NO_ROOM_FIRST);
    while relay.answer_reserve(NO_ROOM_FIRST) {}
    assert_eq!(asks(&relay.take_effects()), vec![(b, id(2))]);
    relay.assert_invariants();
}

#[test]
fn an_old_answer_leaves_a_recreated_entry_alone() {
    let mut relay =
        RelayModel::new(PoolModel::new(Height(100), Generation(0))).with_manual_reserves();
    let a = open(&mut relay, 1, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    let first = relay.wanted().entry(id(1)).unwrap().generation;
    assert!(relay.process_reserve(secs(0)));
    // A gives the call up, the entry goes, and B makes a new one.
    relay.cancel_reserve(a, id(1), secs(0));
    relay.end(a, secs(0));
    let b = open(&mut relay, 2, true, secs(0));
    relay.inv(b, &[(id(1), SIZE)], secs(0));
    let second = relay.wanted().entry(id(1)).unwrap().generation;
    assert_ne!(first, second);
    // A's old grant arrives and changes nothing of B's.
    assert!(relay.deliver_reserve(secs(0)));
    assert_eq!(relay.wanted().entry(id(1)).unwrap().generation, second);
    while relay.answer_reserve(secs(0)) {}
    assert_eq!(asks(&relay.take_effects()), vec![(b, id(1))]);
    relay.assert_invariants();
}

#[test]
fn a_collision_after_a_removal_costs_one_redundant_advert() {
    fn collide(tx_id: UnminedTxId) -> u64 {
        let n = u64::from(u32::from_le_bytes(
            tx_id.mined_id().0[..4].try_into().unwrap(),
        ));
        if n == 2 {
            1
        } else {
            n
        }
    }
    let mut relay = relay().with_short_id(collide);
    let a = open(&mut relay, 1, true, secs(0));
    relay.set_ranking(vec![a]);
    fetch(&mut relay, a, 1, secs(0));
    fetch(&mut relay, a, 2, secs(0));
    assert!(adverts(&relay.take_effects()).is_empty(), "A supplied both");
    // A block mines id 1, which erases the shared short id, and a reorg then
    // hides and reveals id 2.
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
    let reorg = ModelTransition {
        disconnected: 1,
        connected: vec![ModelBlock::default(), ModelBlock::default()],
        tip: Height(102),
        generation: Generation(2),
        rules_change: false,
    };
    relay.transition(reorg, secs(1));
    // A had been told nothing, so id 2's reveal is the one redundant advert.
    finish_all(&mut relay, &Outcome::Pass, secs(1));
    let sent: Vec<_> = adverts(&relay.take_effects());
    assert!(sent.len() <= 1, "{sent:?}");
    relay.assert_invariants();
}

/// The default short id of `tx_id`, as the model keeps it.
fn short_of(tx_id: UnminedTxId) -> u64 {
    let mut short = [0u8; 8];
    short.copy_from_slice(&tx_id.mined_id().0[..8]);
    u64::from_le_bytes(short)
}

/// Id 1 is a 2,000,000 byte transaction. Every other id is small.
fn one_is_big(tx_id: UnminedTxId) -> u32 {
    if tx_id == id(1) {
        MAX_TX_BYTES
    } else {
        SIZE
    }
}

#[test]
fn work_that_falls_due_is_done_on_any_input() {
    let mut relay = relay();
    let inbound = open(&mut relay, 1, false, secs(0));
    relay.inv(inbound, &[(id(1), SIZE)], secs(0));
    relay.take_effects();
    // An input that does nothing else still claims what became due.
    assert!(!relay.answer_reserve(INBOUND_DELAY));
    assert_eq!(asks(&relay.take_effects()), vec![(inbound, id(1))]);
}

#[test]
fn a_body_above_the_requesters_limit_is_never_served() {
    let mut relay = relay().with_tx_size(one_is_big);
    let supplier = open(&mut relay, 1, true, secs(0));
    relay.inv(supplier, &[(id(1), MAX_TX_BYTES)], secs(0));
    relay.tx(supplier, id(1), tx(1), secs(0));
    finish_all(&mut relay, &Outcome::Pass, secs(0));
    assert_eq!(relay.pool().visible(), vec![id(1)]);
    let small = relay.connect(PeerKey(2), true, secs(1));
    relay.hello(
        small,
        Hello {
            serves: MAX_REQUESTS_PER_PEER,
            max_tx_bytes: 1_000,
        },
        secs(1),
    );
    relay.take_effects();
    relay.get_txs(small, id(1), secs(1));
    assert_eq!(
        relay.take_effects(),
        vec![RelayEffect::NotFound {
            session: small,
            id: id(1)
        }]
    );
}

#[test]
fn an_old_removal_does_not_erase_a_newer_announcement() {
    let mut relay =
        RelayModel::new(PoolModel::new(Height(100), Generation(0))).with_manual_reads(100);
    let supplier = open(&mut relay, 1, true, secs(0));
    let a = open(&mut relay, 2, true, secs(0));
    fetch(&mut relay, supplier, 1, secs(0));
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
    // A announces it after the removal, before reading the removal.
    relay.inv(a, &[(id(1), SIZE)], secs(2));
    relay.read(a, secs(2));
    assert!(relay.session(a).unwrap().knows(short_of(id(1))));
}

#[test]
fn a_restarted_pool_reconciles_unread_removals() {
    let mut relay =
        RelayModel::new(PoolModel::new(Height(100), Generation(0))).with_manual_reads(100);
    let supplier = open(&mut relay, 1, true, secs(0));
    let a = open(&mut relay, 2, true, secs(0));
    relay.set_ranking(vec![a]);
    fetch(&mut relay, supplier, 1, secs(0));
    relay.read(a, secs(0));
    assert!(relay.session(a).unwrap().announced.contains_key(&id(1)));
    relay.deactivate(secs(1));
    relay.activate(secs(2));
    assert!(relay.session(a).unwrap().announced.is_empty());
    relay.assert_invariants();
}

#[test]
fn a_fresh_announcement_waits_behind_an_old_request() {
    let mut relay = relay();
    let a = open(&mut relay, 1, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    relay.take_effects();
    // A block mines it and a reorg takes that block away, so it is wanted
    // again. A's first request is left outstanding.
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
    let reorg = ModelTransition {
        disconnected: 1,
        connected: vec![ModelBlock::default(), ModelBlock::default()],
        tip: Height(102),
        generation: Generation(2),
        rules_change: false,
    };
    relay.transition(reorg, secs(1));
    // A announces it once more. It is kept, but not asked for twice at once.
    relay.inv(a, &[(id(1), SIZE)], secs(2));
    let second = relay.wanted().entry(id(1)).unwrap().generation;
    assert!(relay.session(a).unwrap().holds(id(1), second));
    assert!(asks(&relay.take_effects()).is_empty());
    // The old answer leaves the new entry pending, and A is asked again.
    relay.not_found(a, id(1), secs(3));
    assert_eq!(asks(&relay.take_effects()), vec![(a, id(1))]);
    relay.tx(a, id(1), tx(1), secs(4));
    finish_all(&mut relay, &Outcome::Pass, secs(4));
    assert_eq!(relay.pool().visible(), vec![id(1)]);
    relay.assert_invariants();
}

#[test]
fn a_sole_slow_announcer_keeps_doubling() {
    let mut relay = relay();
    let a = open(&mut relay, 1, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    relay.take_effects();
    let mut now = secs(0);
    let mut last_span = Duration::ZERO;
    for round in 0..12 {
        let request = relay.session(a).unwrap().requests[&id(1)];
        let span = request.deadline - now;
        if round > 0 {
            assert_eq!(span, last_span * 2, "round {round}");
        }
        last_span = span;
        now = request.deadline;
        relay.tick(now);
        relay.tx(a, id(1), tx(1), now);
        now += INBOUND_DELAY;
        relay.tick(now);
        assert_eq!(
            asks(&relay.take_effects()),
            vec![(a, id(1))],
            "round {round}"
        );
    }
}

#[test]
fn a_cancelled_probe_passes_its_permit_on() {
    let mut relay =
        RelayModel::new(PoolModel::new(Height(100), Generation(0))).with_manual_reserves();
    fill_pool(&mut relay);
    let a = open(&mut relay, 1, true, secs(0));
    let b = open(&mut relay, 2, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    relay.inv(b, &[(id(2), SIZE)], secs(0));
    while relay.answer_reserve(secs(0)) {}
    relay.tick(NO_ROOM_FIRST);
    assert_eq!(relay.wanted().probe().map(|(holder, _)| holder), Some(a));
    relay.cancel_reserve(a, id(1), NO_ROOM_FIRST);
    // The permit came back and was taken again at once by the oldest
    // eligible session, with a new call. Only one probe is ever out.
    assert!(relay.wanted().probe().is_some());
    assert_eq!(
        relay.calls_in_flight(),
        2,
        "the cancelled call and the new probe"
    );
    relay.assert_invariants();
    // A's call never reaches the pool. B's finds no room, and the backoff
    // doubles.
    while relay.answer_reserve(NO_ROOM_FIRST) {}
    assert_eq!(
        relay.wanted().backoff_until(NO_ROOM_FIRST),
        Some(NO_ROOM_FIRST * 3)
    );
    relay.assert_invariants();
}

#[test]
fn a_stale_answer_costs_at_most_one_more_probe() {
    let mut relay = RelayModel::new(PoolModel::new(Height(100), Generation(0)).with_limits(1, 1))
        .with_manual_reserves();
    let a = open(&mut relay, 1, true, secs(0));
    let b = open(&mut relay, 2, true, secs(0));
    relay.inv(a, &[(id(1), SIZE)], secs(0));
    relay.inv(b, &[(id(2), SIZE)], secs(0));
    // The pool grants A, then refuses B, and B hears first. Callers cannot
    // order answers across each other, so A's grant clears the backoff B
    // started.
    assert!(relay.process_reserve(secs(0)));
    assert!(relay.process_reserve(secs(0)));
    assert!(relay.deliver_reserve_for(b, secs(0)));
    assert!(relay.wanted().in_backoff());
    assert!(relay.deliver_reserve_for(a, secs(0)));
    assert_eq!(asks(&relay.take_effects()), vec![(a, id(1))]);
    assert!(!relay.wanted().in_backoff());
    // The cost is one more refused reserve, which starts the backoff again.
    while relay.answer_reserve(secs(0)) {}
    assert_eq!(relay.wanted().backoff_until(secs(0)), Some(NO_ROOM_FIRST));
    relay.assert_invariants();
}

#[test]
fn a_cooldown_delivered_after_the_tip_moved_does_not_start() {
    for move_tip in [false, true] {
        let mut relay = relay().with_manual_verdicts();
        let a = open(&mut relay, 1, true, secs(0));
        relay.inv(a, &[(id(1), SIZE)], secs(0));
        relay.tx(a, id(1), tx(1), secs(0));
        let bad = Outcome::Fail {
            scope: RejectionScope::ExactTip,
            charge: Charge::Cooldown,
        };
        finish_all(&mut relay, &bad, secs(0));
        assert_eq!(relay.verdicts_in_flight(), 1);
        if move_tip {
            let block = ModelTransition {
                disconnected: 0,
                connected: vec![ModelBlock::default()],
                tip: Height(101),
                generation: Generation(1),
                rules_change: false,
            };
            relay.transition(block, secs(1));
        }
        assert!(relay.deliver_verdict(secs(1)));
        assert_eq!(relay.reactor().is_cooling(PeerKey(1), secs(1)), !move_tip);
        relay.assert_invariants();
    }
}

#[test]
fn an_outside_admission_uses_its_own_size_for_fan_out() {
    let mut relay = relay().with_advert_delay(None);
    let peers: Vec<_> = (1..=40)
        .map(|p| open(&mut relay, p, true, secs(0)))
        .collect();
    relay.set_ranking(peers);
    relay.outside(secs(0), |pool, now| {
        let reservation = pool.reserve(id(1), Intent::Deliver, now).expect("room");
        let _ = pool.effects();
        let (_, step) = pool.submit(reservation.id, tx(1), now);
        ((), step)
    });
    finish_all(&mut relay, &Outcome::Pass, secs(0));
    assert_eq!(adverts(&relay.take_effects()).len(), WINDOW_SMALL);
}

#[test]
fn a_ban_after_the_session_ended_closes_the_current_one() {
    let mut relay = relay();
    let old = open(&mut relay, 1, true, secs(0));
    relay.inv(old, &[(id(1), SIZE)], secs(0));
    relay.tx(old, id(1), tx(1), secs(0));
    relay.end(old, secs(1));
    let new = open(&mut relay, 1, true, secs(1));
    relay.take_effects();
    let banned = Outcome::Fail {
        scope: RejectionScope::ExactTip,
        charge: Charge::Ban,
    };
    finish_all(&mut relay, &banned, secs(2));
    assert_eq!(closes(&relay.take_effects()), vec![(new, CloseReason::Ban)]);
    relay.assert_invariants();
}

#[test]
fn a_late_verdict_leaves_another_announcers_fetch_alone() {
    let mut relay = relay().with_manual_verdicts();
    let first = open(&mut relay, 1, true, secs(0));
    relay.inv(first, &[(id(1), SIZE)], secs(0));
    relay.take_effects();
    relay.tx(first, id(1), tx(1), secs(0));
    let rejected = Outcome::Fail {
        scope: RejectionScope::ExactTip,
        charge: Charge::None,
    };
    finish_all(&mut relay, &rejected, secs(0));
    relay.end(first, secs(1));
    // A new block forgets the tip-scoped rejection, and another announcer
    // fetches the id before the old verdict arrives.
    let block = ModelTransition {
        disconnected: 0,
        connected: vec![ModelBlock::default()],
        tip: Height(101),
        generation: Generation(1),
        rules_change: false,
    };
    relay.transition(block, secs(1));
    let second = open(&mut relay, 2, true, secs(1));
    relay.inv(second, &[(id(1), SIZE)], secs(1));
    assert_eq!(asks(&relay.take_effects()), vec![(second, id(1))]);
    assert!(relay.deliver_verdict(secs(2)));
    assert_eq!(relay.wanted().entry(id(1)).unwrap().claim, Some(second));
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
    for seed in 0..400 {
        run(seed);
    }
}

fn run(seed: u64) {
    let mut rng = Lcg(seed);
    let manual = seed % 2 == 1;
    let mut relay = RelayModel::new(PoolModel::new(Height(100), Generation(0)).with_limits(8, 2));
    if manual {
        relay = relay
            .with_manual_reserves()
            .with_manual_reads(6)
            .with_manual_verdicts();
    }
    let mut height = 100u32;
    let mut generation = 0u64;
    // What each block still on the chain mined, lowest first.
    let mut chain: Vec<Vec<UnminedTxId>> = Vec::new();
    let behaviours = [
        Behaviour::Honest,
        Behaviour::Honest,
        Behaviour::Honest,
        Behaviour::Silent,
        Behaviour::Liar,
    ];
    let mut live: Vec<(SessionId, Behaviour)> = Vec::new();
    let mut replies: Vec<(Duration, SessionId, UnminedTxId)> = Vec::new();
    // What each honest session announced since the last chain transition,
    // and what failed since then: the oracle, kept apart from the
    // model's own table.
    // An obligation is met once the id is seen in the pool, fails, or is
    // mined after it was announced. Rejections and minings still in force
    // when it is announced meet it at once.
    let mut honest: HashMap<SessionId, HashMap<UnminedTxId, bool>> = HashMap::new();
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
                FAILED_NOW.with(|now| now.borrow_mut().push(tx_id));
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
        match rng.next(13) {
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
                    let settled = failed.contains(&id(n))
                        || chain.iter().flatten().any(|mined| *mined == id(n))
                        || relay.pool().visible().contains(&id(n));
                    let met = honest
                        .entry(session)
                        .or_default()
                        .entry(id(n))
                        .or_insert(false);
                    *met |= settled;
                }
            }
            7 => verify(&mut relay, &mut failed, &mut rng, now, true),
            8 if manual => match rng.next(3) {
                0 => {
                    relay.process_reserve(now);
                }
                1 => {
                    relay.deliver_reserve(now);
                }
                _ => {
                    relay.deliver_verdict(now);
                }
            },
            10 => {
                // A block, mining one pooled transaction if there is one.
                let visible = relay.pool().visible();
                let transactions: Vec<ModelTx> = if visible.is_empty() {
                    Vec::new()
                } else {
                    let pick = visible
                        [usize::try_from(rng.next(u64::try_from(visible.len()).unwrap())).unwrap()];
                    vec![ModelTx::new(pick)]
                };
                height += 1;
                generation += 1;
                let block = ModelTransition {
                    disconnected: 0,
                    connected: vec![ModelBlock {
                        transactions: transactions.clone(),
                    }],
                    tip: Height(height),
                    generation: Generation(generation),
                    rules_change: false,
                };
                chain.push(transactions.iter().map(|tx| tx.id).collect());
                for mined in transactions.iter().map(|tx| tx.id) {
                    for obligations in honest.values_mut() {
                        if let Some(met) = obligations.get_mut(&mined) {
                            *met = true;
                        }
                    }
                }
                relay.transition(block, now);
                // A transition can forget rejections and take mined blocks
                // away, so older decisions no longer settle anything.
                failed.clear();
            }
            11 => {
                // A one-block reorg, which hides the pool while it is checked
                // again.
                height += 1;
                generation += 1;
                let reorg = ModelTransition {
                    disconnected: 1,
                    connected: vec![ModelBlock::default(), ModelBlock::default()],
                    tip: Height(height),
                    generation: Generation(generation),
                    rules_change: false,
                };
                chain.pop();
                chain.push(Vec::new());
                chain.push(Vec::new());
                relay.transition(reorg, now);
                failed.clear();
            }
            9 if manual && !live.is_empty() => {
                let index = usize::try_from(rng.next(u64::try_from(live.len()).unwrap())).unwrap();
                relay.read(live[index].0, now);
            }
            _ => relay.tick(now),
        }
        answer(&mut relay, &mut replies, &live, now, &mut rng);
        deliver(&mut relay, &mut replies, &live, now);
        settle_obligations(&relay, &mut honest);
        relay.assert_invariants();
    }

    // Quiet time: only answers, reserve replies, reads, verdicts and the
    // clock.
    let mut drained = false;
    for _ in 0..5_000 {
        while relay.answer_reserve(now) {}
        while relay.deliver_verdict(now) {}
        for (session, _) in &live {
            relay.read(*session, now);
        }
        answer(&mut relay, &mut replies, &live, now, &mut rng);
        deliver(&mut relay, &mut replies, &live, now);
        verify(&mut relay, &mut failed, &mut rng, now, false);
        answer(&mut relay, &mut replies, &live, now, &mut rng);
        settle_obligations(&relay, &mut honest);
        relay.assert_invariants();
        if !relay.jobs().is_empty() || relay.calls_in_flight() > 0 || relay.verdicts_in_flight() > 0
        {
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
            for (announced, met) in honest.get(session).into_iter().flatten() {
                assert!(
                    *met || visible.contains(announced),
                    "seed {seed}: {announced:?} from an honest announcer was not decided"
                );
            }
        }
    }
    for (id, entry) in relay.wanted().entries() {
        for (session, behaviour) in &live {
            let held = relay
                .session(*session)
                .is_some_and(|state| state.holds(*id, entry.generation));
            assert!(
                !(held && *behaviour == Behaviour::Honest),
                "seed {seed}: an honest announcer still holds {id:?}"
            );
        }
        assert!(
            entry.claim.is_none(),
            "seed {seed}: {id:?} is still claimed"
        );
    }
}

thread_local! {
    /// Ids whose verification the random run failed since the last look.
    static FAILED_NOW: std::cell::RefCell<Vec<UnminedTxId>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// Meets every obligation whose id is in the pool now, or failed since the
/// last look.
fn settle_obligations(
    relay: &RelayModel,
    honest: &mut HashMap<SessionId, HashMap<UnminedTxId, bool>>,
) {
    let visible: HashSet<UnminedTxId> = relay.pool().visible().into_iter().collect();
    let failed: Vec<UnminedTxId> = FAILED_NOW.with(|now| std::mem::take(&mut *now.borrow_mut()));
    for obligations in honest.values_mut() {
        for (id, met) in obligations.iter_mut() {
            if visible.contains(id) || failed.contains(id) {
                *met = true;
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
            Behaviour::Honest => relay.tx(session, asked, ModelTx::new(asked), now),
            Behaviour::Liar => relay.not_found(session, asked, now),
            Behaviour::Silent => {}
        }
    }
}
