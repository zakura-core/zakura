//! Named scenarios: each pins one rule of the machine, on the real machine,
//! through the [`Bench`].

use std::time::Duration;

use zakura_chain::transaction::UnminedTxId;
use zakura_node_services::mempool::port::{
    Change, Charge, LocalFault, RejectionScope, RemovalReason,
};

use super::{body, hello, ids, script_len, Bench, SIZE};
use crate::zakura::tx_relay::machine::{
    peer::Hello,
    reactor::Recorded,
    session::{Timer, Timers},
    CloseReason, FanOut, Paid, PeerKey, RelayEffect, SessionId, Unreserved, Verdict, Violation,
    ADVERT_DELAY, BASE_COOLDOWN, INV_RETRY, MAX_COOLDOWN, MAX_COOLDOWN_PEERS,
    MAX_REQUESTS_PER_PEER, MAX_SERVES, MAX_TX_BYTES, NO_ROOM_FIRST, PARK_COOLDOWN,
    PENDING_PER_PEER, RETRY_FIRST, TRIED_PER_ENTRY,
};

/// A millisecond.
const MS: Duration = Duration::from_millis(1);

/// The ids of the unanswered reserve calls, oldest first.
fn called(bench: &Bench) -> Vec<UnminedTxId> {
    bench.calls().into_iter().map(|(_, id)| id).collect()
}

/// The sessions the last input queued an advert of `id` for, in order.
fn advertised(bench: &Bench, id: UnminedTxId) -> Vec<SessionId> {
    let adverts = bench.adverts().into_iter().filter(|(_, of)| *of == id);
    let mut sessions: Vec<SessionId> = adverts.map(|(session, _)| session).collect();
    sessions.sort();
    sessions
}

/// Whether `session` holds `id` pending.
fn holds(bench: &Bench, session: SessionId, id: UnminedTxId) -> bool {
    let state = bench.machine.session(session);
    state.is_some_and(|state| state.pending.contains(id))
}

/// The deadline of `session`'s request for `id`.
fn deadline(bench: &Bench, session: SessionId, id: UnminedTxId) -> Duration {
    let state = bench.machine.session(session).expect("a live session");
    state.requests[&id].deadline
}

/// A fan-out that announces at once to the first session only, and to the
/// rest after `advert_delay`.
fn one_at_once(advert_delay: Duration) -> FanOut {
    FanOut {
        advert_delay,
        window_small: 1,
        window_full: 1,
    }
}

/// A `Hello` that serves `serves` requests and takes `max_tx_bytes`.
fn hello_of(serves: usize, max_tx_bytes: u32) -> Hello {
    Hello {
        serves,
        max_tx_bytes,
    }
}

/// The answer to `session`'s `GetTxs` for `id`: the body, or `NotFound`.
fn answer(session: SessionId, id: UnminedTxId, served: bool) -> RelayEffect {
    if served {
        RelayEffect::Serve { session, id }
    } else {
        RelayEffect::NotFound { session, id }
    }
}

/// The close of `session` for `violation`.
fn broke(session: SessionId, violation: Violation) -> (SessionId, CloseReason) {
    (session, CloseReason::Violation(violation))
}

// Claiming, reserving and asking.

/// Claim, reserve, then ask: an announced id goes to the pool as a reserve
/// call first, and its `GetTxs` goes out only in the step that grants the
/// reservation. A refusal asks nothing.
#[test]
fn a_request_goes_out_only_once_the_pool_grants_its_reservation() {
    let mut bench = Bench::new();
    let (granted, refused) = (bench.id(0), bench.id(1));
    let session = bench.join(1);
    bench.inv(session, &[granted, refused]);
    assert_eq!(called(&bench), vec![granted, refused]);
    assert!(bench.asked().is_empty(), "nothing is asked before a grant");
    let scope = RejectionScope::ExactTip;
    bench.answer(bench.call(refused), Err(Unreserved::Rejected { scope }));
    assert!(bench.asked().is_empty(), "a refusal asks nothing");
    bench.fetch(session, granted);
}

/// Each session takes its own id ready longest first: after a full pool's
/// backoff the one probe is for the oldest id it holds, and an attempt that
/// ends undecided sends its id behind the work already waiting there.
#[test]
fn each_session_takes_its_id_ready_longest_first() {
    let mut bench = Bench::new();
    let (old, young) = (bench.id(0), bench.id(1));
    let session = bench.join(1);
    bench.inv(session, &[old]);
    bench.tick(Duration::from_secs(1));
    bench.inv(session, &[young]);
    for (call, _) in bench.calls() {
        bench.answer(call, Err(Unreserved::NoRoom));
    }
    assert!(bench.next(), "the backoff ends");
    assert_eq!(called(&bench), vec![old], "one probe, for the oldest id");
    // The probe's grant comes back cancelled: room, and an undecided end.
    let cancelled = bench.granted(true);
    bench.answer(bench.call(old), cancelled);
    assert_eq!(called(&bench), vec![young, old], "requeued goes behind");
}

/// One attempt at a time: while one session's attempt at an id is out, at
/// the pool and then on the wire, no other session asks for it. When it
/// ends undecided, an announcer that has not tried goes next.
#[test]
fn one_attempt_at_a_time_then_the_next_announcer() {
    let mut bench = Bench::new();
    let id = bench.id(0);
    let (first, second) = (bench.join(1), bench.join(2));
    bench.inv(first, &[id]);
    bench.inv(second, &[id]);
    assert_eq!(called(&bench), vec![id], "one reserve call for the id");
    bench.fetch(first, id);
    bench.tick(Duration::from_secs(1));
    assert!(bench.calls().is_empty(), "nobody else asks meanwhile");
    bench.not_found(first, id);
    bench.fetch(second, id);
}

/// One try per peer. A peer that answered `NotFound`, missed the deadline,
/// ended its session with the claim out, or supplied a body the pool
/// rejected without settling the id has tried it: it drops the id, keeps it
/// dropped across a reconnection, and the next ask goes to another holder.
/// What this node caused is no try: after a full pool or a local fault the
/// same peer is asked again, and after a cancelled fetch it keeps the id,
/// though its request on the wire holds it back.
#[test]
fn a_peer_that_tried_an_id_is_not_asked_again_while_another_has_not() {
    /// How the first peer's attempt ends.
    #[derive(Debug, PartialEq)]
    enum End {
        NotFound,
        Deadline,
        Ended,
        Rejected,
        NoRoom,
        Fault,
        Cancelled,
    }
    use End::*;
    for end in [
        NotFound, Deadline, Ended, Rejected, NoRoom, Fault, Cancelled,
    ] {
        let mut bench = Bench::new();
        let id = bench.id(0);
        let (mut first, second) = (bench.join(1), bench.join(2));
        bench.inv(first, &[id]);
        bench.inv(second, &[id]);
        if end == NoRoom {
            bench.answer(bench.call(id), Err(Unreserved::NoRoom));
        } else {
            bench.fetch(first, id);
        }
        match end {
            NotFound => bench.not_found(first, id),
            Deadline => assert!(bench.next()),
            Ended => {
                bench.end(first);
                first = bench.join(1);
                bench.inv(first, &[id]);
            }
            Rejected | Fault => {
                assert!(bench.deliver(first, id, bench.tx(0)));
                let waiter = bench.submitted().expect("submitted");
                let rejected = bench.rejection(RejectionScope::Uncached, Charge::None);
                let fault = Verdict::LocalFault(LocalFault::VerifyTimeout);
                bench.verdict(waiter, if end == Rejected { rejected } else { fault });
            }
            Cancelled => bench.cancel(bench.tracker.requests[0]),
            NoRoom => {}
        }
        let tried = !matches!(end, NoRoom | Fault | Cancelled);
        assert_eq!(holds(&bench, first, id), !tried, "{end:?}: a try drops it");
        while bench.calls().is_empty() {
            assert!(bench.next(), "{end:?}: the id is asked about again");
        }
        bench.grant(bench.calls()[0].0);
        let next = if matches!(end, NoRoom | Fault) {
            first
        } else {
            second
        };
        assert_eq!(bench.asked(), vec![(next, id)], "{end:?}");
    }
}

/// The limit of 5 is counted once: a session's reserve calls, requests and
/// bodies awaiting their verdicts stay within 5 together, and the bodies an
/// ended session left at the pool count against its peer's next session
/// until their verdicts come.
#[test]
fn the_limit_of_five_counts_the_bodies_an_ended_session_left() {
    let mut bench = Bench::new();
    let ids: Vec<UnminedTxId> = (0..12).map(|n| bench.id(n)).collect();
    let first = bench.join(1);
    bench.inv(first, &ids[..7]);
    assert_eq!(bench.calls().len(), MAX_REQUESTS_PER_PEER, "5 calls of 7");
    for (call, _) in bench.calls() {
        bench.grant(call);
    }
    let mut waiters = Vec::new();
    for (n, id) in (0..).zip(&ids[..2]) {
        assert!(bench.deliver(first, *id, bench.tx(n)));
        waiters.push(bench.submitted().expect("submitted"));
    }
    assert!(bench.calls().is_empty(), "two bodies, three requests");
    bench.end(first);
    let second = bench.join(1);
    bench.inv(second, &ids[7..]);
    let calls = bench.calls().len();
    assert_eq!(calls, MAX_REQUESTS_PER_PEER - 2, "the bodies count");
    bench.verdict(waiters[0], Verdict::Admitted);
    let calls = bench.calls().len();
    assert_eq!(calls, MAX_REQUESTS_PER_PEER - 1, "until their verdicts");
}

/// A session that ends with its reserve calls unanswered leaves them
/// counted against its peer's 5: the peer's next session claims nothing
/// until an old call is answered, so ending and reopening while the pool is
/// slow cannot pile up calls.
#[test]
fn an_ended_sessions_reserve_calls_count_against_its_peer() {
    let mut bench = Bench::new();
    let first = bench.join(1);
    let (old, fresh) = (ids(0..5), ids(5..10));
    bench.inv(first, &old);
    let calls = bench.calls();
    assert_eq!(calls.len(), 5, "the first session uses its 5");
    bench.end(first);

    let second = bench.join(1);
    bench.inv(second, &fresh);
    assert_eq!(bench.calls(), calls, "the old calls still count");
    // One old call is answered, refused, so one place comes back.
    bench.answer(calls[0].0, Err(Unreserved::NoRoom));
    assert_eq!(fresh.iter().filter(|id| bench.reserved(**id)).count(), 1);
}

/// A grant whose caller gave up goes straight back: the reserve call of a
/// session that ended is answered by a release in the same step.
#[test]
fn a_grant_for_an_ended_session_goes_straight_back() {
    let mut bench = Bench::new();
    let id = bench.id(0);
    let session = bench.join(1);
    bench.inv(session, &[id]);
    let call = bench.call(id);
    bench.end(session);
    let reservation = bench.grant(call);
    assert_eq!(bench.effects, vec![RelayEffect::Release { reservation }]);
}

/// A new session gets the go only after every older session of its peer
/// ended: until then its announcements wait, and nothing is asked of it.
#[test]
fn a_second_session_waits_for_the_go_until_the_first_ends() {
    let mut bench = Bench::new();
    let id = bench.id(0);
    let (first, second) = (bench.join(1), bench.join(1));
    bench.inv(second, &[id]);
    assert!(bench.calls().is_empty(), "the younger session waits");
    bench.end(first);
    bench.fetch(second, id);
}

// Deadlines.

/// A deadline frees an unanswered request's reservation and claim, the peer
/// has had its try, and another announcer is asked. One more deadline
/// without an answer ends the session as unresponsive. A body handled after
/// the deadline is late, even with no input in between, since each input
/// applies what is due first: it is dropped unread and costs nothing.
#[test]
fn a_deadline_moves_the_id_on_and_one_more_ends_the_session() {
    for answers_late in [false, true] {
        let mut bench = Bench::new();
        let id = bench.id(0);
        let (slow, other) = (bench.join(1), bench.join(2));
        bench.inv(slow, &[id]);
        bench.inv(other, &[id]);
        let reservation = bench.fetch(slow, id);
        let due = deadline(&bench, slow, id);
        if answers_late {
            bench.now = due;
            assert!(!bench.deliver(slow, id, bench.tx(0)), "dropped unread");
        } else {
            assert!(bench.next());
            assert_eq!(bench.now, due, "the next deadline is the request's");
        }
        let released = RelayEffect::Release { reservation };
        assert!(bench.effects.contains(&released), "the slot goes back");
        let closed = !bench.closes().is_empty();
        assert!(bench.reserved(id) && !closed, "another is asked");
        bench.fetch(other, id);
        assert!(bench.deliver(other, id, bench.tx(0)));
        let waiter = bench.submitted().expect("submitted");
        bench.add(&[id]);
        bench.verdict(waiter, Verdict::Admitted);
        if answers_late {
            bench.tick(Duration::from_secs(600));
            let next = bench.id(1);
            bench.inv(slow, &[next]);
            assert!(bench.reserved(next), "an answered peer stays, uncharged");
        } else {
            assert!(bench.next());
            assert_eq!(bench.parks(), vec![slow], "a first stall parks");
        }
    }
}

/// A deadline covers what the peer has outstanding: each new request moves
/// every unmissed deadline of the session out to cover all its bodies from
/// then.
#[test]
fn a_new_request_moves_the_outstanding_deadlines_out() {
    let mut bench = Bench::new();
    let (first, second) = (bench.id(0), bench.id(1));
    let session = bench.join(1);
    bench.inv(session, &[first]);
    bench.fetch(session, first);
    let before = deadline(&bench, session, first);
    bench.tick(Duration::from_secs(1));
    bench.inv(session, &[second]);
    bench.fetch(session, second);
    let moved = deadline(&bench, session, first);
    assert!(moved > before, "the earlier deadline moves out");
    assert_eq!(moved, deadline(&bench, session, second), "they cover all");
}

/// A body is checked against the table before it is submitted: once the
/// pool cancelled its fetch, the slot is back, and a body that still
/// arrives is dropped unread and the id asked about again. A cancelled
/// fetch is no try, but its deadline still runs: left unanswered, the
/// request ends the session.
#[test]
fn a_cancelled_fetch_drops_its_body_but_its_deadline_still_runs() {
    for answered in [true, false] {
        let mut bench = Bench::new();
        let id = bench.id(0);
        let session = bench.join(1);
        bench.inv(session, &[id]);
        let reservation = bench.fetch(session, id);
        bench.cancel(reservation);
        assert_eq!(bench.effects, vec![RelayEffect::Release { reservation }]);
        if answered {
            assert!(!bench.deliver(session, id, bench.tx(0)), "dropped unread");
            assert!(bench.reserved(id), "the id is asked about again");
            continue;
        }
        while bench.parks().is_empty() {
            assert!(bench.next(), "the deadlines run");
        }
        assert_eq!(bench.parks(), vec![session]);
    }
}

/// A stall parks the session, not its connection. The first stall, a
/// request left unanswered past one more deadline or a writer its peer
/// stopped reading, ends the session, and the peer's next sessions are
/// refused until the park ends. The session the park lets in then closes
/// its connection on its own stall. Neither is a charge.
#[test]
fn a_stall_parks_the_session_and_a_repeat_closes_the_connection() {
    let mut bench = Bench::new();
    let stalled = bench.join(1);
    bench.stopped_reading(stalled);
    assert_eq!(bench.parks(), vec![stalled], "the first stall parks");
    let refused = bench.connect(1, false);
    assert_eq!(bench.parks(), vec![refused], "refused while the park runs");
    bench.tick(PARK_COOLDOWN);
    let readmitted = bench.connect(1, false);
    assert!(bench.parks().is_empty(), "the ended park lets it in");
    bench.hello(readmitted, hello());
    let id = bench.id(0);
    bench.inv(readmitted, &[id]);
    assert!(bench.reserved(id), "uncharged, it is asked");
    bench.stopped_reading(readmitted);
    let closed = (readmitted, CloseReason::StoppedReading);
    assert_eq!(bench.closes(), vec![closed], "its stall closes");
}

/// Honest ids get fetched despite hostile announcers: with `K` announcers
/// ahead of an honest one that never answer, the honest one is asked within
/// `K + 1` request deadlines, and with `K` that answer `NotFound`, at once.
#[test]
fn hostile_announcers_delay_an_honest_one_by_at_most_a_deadline_each() {
    const K: u64 = 3;
    for silent in [true, false] {
        let mut bench = Bench::new();
        let id = bench.id(0);
        let hostile: Vec<SessionId> = (0..K).map(|peer| bench.join(peer)).collect();
        let honest = bench.join(K);
        for session in hostile.iter().chain([&honest]) {
            bench.inv(*session, &[id]);
        }
        let (start, mut tries, mut span) = (bench.now, 0, Duration::ZERO);
        loop {
            let Some((call, _)) = bench.calls().first().copied() else {
                assert!(bench.next(), "time moves on");
                continue;
            };
            bench.grant(call);
            let (asked, _) = bench.asked()[0];
            let request = bench.machine.session(asked).expect("asked").requests[&id];
            span = span.max(request.deadline - request.sent);
            if asked == honest {
                break;
            }
            tries += 1;
            if !silent {
                bench.not_found(asked, id);
            }
        }
        assert_eq!(tries, K, "each hostile announcer had one try");
        let waited = bench.now - start;
        let bound = span * u32::try_from(K + 1).expect("small");
        assert!(waited <= bound, "{waited:?}");
        assert!(silent || waited.is_zero(), "a NotFound costs no time");
    }
}

/// A pending set at its bound drops new announcements as overflow, never as
/// a violation, and leaves no holder behind for them. An id it already holds
/// is checked first, and is no overflow. The flood costs only its own peer:
/// another peer's id is asked for at once.
#[test]
fn a_full_pending_set_counts_new_announcements_as_overflow() {
    let mut bench = Bench::new();
    let script = script_len();
    let flood = 0..u32::try_from(PENDING_PER_PEER).expect("fits");
    let full: Vec<UnminedTxId> = flood.map(|n| body(n + 1_000, script).id()).collect();
    let flooder = bench.join(1);
    bench.inv(flooder, &full);
    let fresh = [bench.id(0), bench.id(1)];
    bench.inv(flooder, &[full[0], fresh[0], fresh[1]]);
    assert!(bench.closes().is_empty(), "overflow is no violation");
    let state = bench.machine.session(flooder).expect("open");
    assert_eq!(state.pending.len(), PENDING_PER_PEER);
    assert_eq!(state.overflow, 2, "a held id is no overflow");
    let wanted = bench.machine.wanted();
    let gone = fresh.iter().all(|id| wanted.entry(*id).is_none());
    assert!(gone, "no holder is left");

    let honest = bench.join(2);
    let id = bench.id(2);
    bench.inv(honest, &[id]);
    assert!(bench.reserved(id), "another peer's id is not delayed");
}

/// More colluding sessions than the tried set's cap, all older than honest
/// H, take turns at announcing X and answering `NotFound`. Each stays
/// connected, so none is forgotten and none tries again, and H is asked
/// once each has had its one try.
#[test]
fn colluding_sessions_past_the_tried_cap_cannot_starve_a_later_holder() {
    let mut bench = Bench::new();
    let id = bench.id(0);
    let hostile_count = u64::try_from(TRIED_PER_ENTRY).expect("small") + 2;
    let hostile: Vec<SessionId> = (0..hostile_count).map(|peer| bench.join(peer)).collect();
    for colluder in &hostile {
        bench.inv(*colluder, &[id]);
    }
    let honest = bench.join(hostile_count);
    bench.inv(honest, &[id]);

    let mut hostile_tries = 0;
    for _ in 0..10 * hostile_count {
        let mut asked = Vec::new();
        for (call, _) in bench.calls() {
            bench.grant(call);
            asked.extend(bench.asked().into_iter().map(|(session, _)| session));
        }
        if asked.contains(&honest) {
            assert_eq!(hostile_tries, hostile_count, "each colluder tried once");
            return;
        }
        for session in asked {
            hostile_tries += 1;
            bench.not_found(session, id);
            // Every colluder announces X again, which a forgotten one could
            // use for another try.
            for colluder in &hostile {
                bench.inv(*colluder, &[id]);
            }
        }
    }
    panic!("H was never asked after {hostile_tries} colluding tries");
}

/// A peer we asked for X answers `NotFound` and announces X again, and the
/// two arrive in either order, since they travel on different streams.
/// Both orders end the same way: the peer's try is spent, and the fresh
/// announcement starts a new entry that asks it again.
#[test]
fn a_not_found_and_a_new_inv_end_the_same_in_either_order() {
    /// Grants every call, and counts the requests the grants send.
    fn grant_all(bench: &mut Bench) -> usize {
        let mut asks = 0;
        for (call, _) in bench.calls() {
            bench.grant(call);
            asks += bench.asked().len();
        }
        asks
    }
    let run = |inv_first: bool| {
        let mut bench = Bench::new();
        let id = bench.id(0);
        let session = bench.join(1);
        bench.inv(session, &[id]);
        let mut asks = grant_all(&mut bench);
        assert_eq!(asks, 1, "the first announcement is asked");
        if inv_first {
            bench.inv(session, &[id]);
            asks += grant_all(&mut bench);
            bench.not_found(session, id);
        } else {
            bench.not_found(session, id);
            asks += grant_all(&mut bench);
            bench.inv(session, &[id]);
        }
        asks += grant_all(&mut bench);
        let state = bench.machine.session(session).expect("live");
        let entry = bench.machine.wanted().entry(id);
        let entry = entry.map(|entry| (entry.generation.0, entry.claim, entry.holders));
        let pending: Vec<u64> = state.pending.iter().map(|held| held.generation.0).collect();
        let (requests, reannounced) = (state.requests.len(), &state.reannounced);
        let shape = format!("{entry:?} {pending:?} {requests} {reannounced:?}");
        (asks, shape)
    };
    let inv_first = run(true);
    assert_eq!(inv_first, run(false));
    assert_eq!(inv_first.0, 2, "the fresh announcement is asked");
}

// The pool's answers.

/// A rejection of a scope the pool caches settles the id for every holder
/// when the tip is unchanged since the call or the submission, for a
/// verdict and a reserve answer alike: every pending set drops it at once,
/// and nobody asks again. An uncached rejection, or one across a tip change,
/// has the id asked about again after the retry delay, and after a rejected
/// body by another announcer.
#[test]
fn a_cached_rejection_settles_the_id_and_others_retry_the_rest() {
    let cases = [
        (RejectionScope::ExactTip, false, true),
        (RejectionScope::Uncached, false, false),
        (RejectionScope::ExactTip, true, false),
    ];
    for at_reserve in [false, true] {
        for (scope, tip_moves, settles) in cases {
            let case = format!("{scope:?}, tip moves {tip_moves}, at reserve {at_reserve}");
            let mut bench = Bench::new();
            let id = bench.id(0);
            let (first, second) = (bench.join(1), bench.join(2));
            bench.inv(first, &[id]);
            bench.inv(second, &[id]);
            if at_reserve {
                let call = bench.call(id);
                if tip_moves {
                    bench.new_tip(false);
                }
                bench.answer(call, Err(Unreserved::Rejected { scope }));
            } else {
                bench.fetch(first, id);
                assert!(bench.deliver(first, id, bench.tx(0)));
                let waiter = bench.submitted().expect("submitted");
                if tip_moves {
                    bench.new_tip(false);
                }
                bench.verdict(waiter, bench.rejection(scope, Charge::None));
            }
            assert_eq!(holds(&bench, second, id), !settles, "{case}: held");
            bench.tick(RETRY_FIRST - MS);
            assert!(bench.calls().is_empty(), "{case}: waits the delay");
            bench.tick(MS);
            assert_eq!(bench.reserved(id), !settles, "{case}: asked about again");
            if !settles && !at_reserve {
                bench.fetch(second, id);
            }
        }
    }
}

/// A full pool costs one probe per backoff: after a `NoRoom` nobody claims
/// until the backoff ends, and then one session probes. A slot given back
/// is room, and ends the backoff for everyone at once.
#[test]
fn a_full_pool_costs_one_probe_per_backoff_until_a_slot_comes_back() {
    let mut bench = Bench::new();
    let ids: Vec<UnminedTxId> = (0..3).map(|n| bench.id(n)).collect();
    let fetching = bench.join(1);
    bench.inv(fetching, &ids[..1]);
    bench.fetch(fetching, ids[0]);
    let waiting = bench.join(2);
    bench.inv(waiting, &ids[1..]);
    for (call, _) in bench.calls() {
        bench.answer(call, Err(Unreserved::NoRoom));
    }
    bench.tick(NO_ROOM_FIRST - MS);
    assert!(bench.calls().is_empty(), "nobody claims during the backoff");
    bench.tick(MS);
    assert_eq!(bench.calls().len(), 1, "one probe when it ends");
    bench.answer(bench.calls()[0].0, Err(Unreserved::NoRoom));
    bench.tick(NO_ROOM_FIRST);
    assert!(bench.calls().is_empty(), "the refusal doubles it");
    // The fetching peer answers: its slot goes back, which is room.
    bench.not_found(fetching, ids[0]);
    assert_eq!(called(&bench), ids[1..].to_vec(), "room ends the backoff");
}

/// A grant to the probe ends the backoff for every session at once, though
/// no id changed: a session that waited behind the probe claims as soon as
/// the probe is granted, not at its next event.
#[test]
fn a_granted_probe_lets_every_session_claim_at_once() {
    let mut bench = Bench::new();
    let (probed, waiting) = (bench.id(0), bench.id(1));
    let prober = bench.join(1);
    bench.inv(prober, &[probed]);
    let waiter = bench.join(2);
    bench.inv(waiter, &[waiting]);
    for (call, _) in bench.calls() {
        bench.answer(call, Err(Unreserved::NoRoom));
    }
    bench.tick(NO_ROOM_FIRST);
    assert_eq!(called(&bench), vec![probed], "one probe when it ends");
    bench.grant(bench.call(probed));
    assert!(bench.reserved(waiting), "the grant is room for everyone");
}

/// A stopped core ends claiming for good, whichever `Stopped` answer tells
/// the machine: one to the reserve call of a session that ended is still
/// the core's answer.
#[test]
fn a_stopped_answer_to_an_ended_sessions_call_ends_claiming() {
    let mut bench = Bench::new();
    let (first, second) = (bench.id(0), bench.id(1));
    let ended = bench.join(1);
    bench.inv(ended, &[first]);
    let call = bench.call(first);
    bench.end(ended);
    bench.answer(call, Err(Unreserved::Stopped));
    let session = bench.join(2);
    bench.inv(session, &[second]);
    assert!(
        bench.calls().is_empty(),
        "nothing is reserved after the core stopped"
    );
}

/// A stopped core ends claiming for good: after a `Stopped` answer nothing
/// is reserved again, however much is announced or however long passes, and
/// a body already asked for goes back unsubmitted. An `Inactive` answer
/// while the snapshot shows the pool on waits one retry delay.
#[test]
fn a_stopped_core_ends_claiming_and_an_inactive_one_waits_a_retry_delay() {
    let mut bench = Bench::new();
    let (asked, stopped, later) = (bench.id(0), bench.id(1), bench.id(2));
    let session = bench.join(1);
    bench.inv(session, &[asked, stopped]);
    let reservation = bench.fetch(session, asked);
    bench.answer(bench.call(stopped), Err(Unreserved::Stopped));
    assert!(bench.deliver(session, asked, bench.tx(0)));
    let released = vec![RelayEffect::Release { reservation }];
    assert_eq!(bench.effects, released, "nothing goes in");
    bench.inv(session, &[later]);
    bench.tick(Duration::from_secs(600));
    assert!(bench.calls().is_empty(), "nothing is reserved after");

    let mut bench = Bench::new();
    let id = bench.id(0);
    let session = bench.join(1);
    bench.inv(session, &[id]);
    bench.answer(bench.call(id), Err(Unreserved::Inactive));
    bench.tick(RETRY_FIRST - MS);
    assert!(bench.calls().is_empty(), "an Inactive answer waits");
    bench.tick(MS);
    assert!(bench.reserved(id), "for one retry delay");
}

/// The pool says another caller, such as the legacy adapter, is fetching X,
/// so the claim waits out a retry delay. When that caller's attempt ends,
/// the pool logs X as refused, and X is claimed again at once rather than
/// at the end of the delay.
#[test]
fn a_refusal_ends_a_retry_that_waited_for_another_caller() {
    let mut bench = Bench::new();
    let id = bench.id(0);
    let session = bench.join(1);
    bench.inv(session, &[id]);
    bench.answer(bench.call(id), Err(Unreserved::Pending));
    assert!(bench.effects.is_empty(), "the retry waits");
    let retry_at = bench
        .machine
        .wanted()
        .entry(id)
        .expect("still wanted")
        .retry_at;
    assert!(retry_at > bench.now);

    // The other caller's fetch fails a tenth of a second later.
    bench.now += Duration::from_millis(100);
    bench.publish(vec![Change::Refused(id)]);
    assert!(bench.reserved(id), "the id is claimed again at once");
}

/// A refusal ends only a retry that waited for another caller. A retry for
/// another reason, here a pool that was off, keeps its delay.
#[test]
fn a_refusal_leaves_other_retries_alone() {
    let mut bench = Bench::new();
    let id = bench.id(0);
    let session = bench.join(1);
    bench.inv(session, &[id]);
    bench.answer(bench.call(id), Err(Unreserved::Inactive));
    bench.now += Duration::from_millis(100);
    bench.publish(vec![Change::Refused(id)]);
    assert!(bench.calls().is_empty(), "the delay stands");
}

/// A session that announces an id and ends gives up its hold on the entry,
/// even while another session keeps the id pending: a peer that keeps
/// reconnecting and announcing it during a `NoRoom` backoff can't grow the
/// entry's holder count.
#[test]
fn ended_sessions_give_up_their_hold_while_another_holds_the_id() {
    let mut bench = Bench::new();
    let id = bench.id(1);
    let keeper = bench.join(1);
    bench.inv(keeper, &[id]);
    // The pool is full, so the id stays pending behind the backoff.
    for (call, _) in bench.calls() {
        bench.answer(call, Err(Unreserved::NoRoom));
    }
    let holders = |bench: &Bench| {
        bench
            .machine
            .wanted()
            .entry(id)
            .map_or(0, |entry| entry.holders)
    };
    assert_eq!(holders(&bench), 1);
    for round in 0..50 {
        let churn = bench.join(2);
        bench.inv(churn, &[id]);
        let named = holders(&bench);
        assert!(named <= 2, "round {round}: {named} holders");
        bench.end(churn);
    }
    assert_eq!(holders(&bench), 1, "only the holder is left");
}

// Charges.

/// Never back to its source: an id a peer announced while it cooled is not
/// announced back to it when the pool admits the id from elsewhere.
#[test]
fn an_id_a_cooling_peer_announced_is_not_announced_back_to_it() {
    let mut bench = Bench::new();
    let cooling = bench.join(1);
    let waiter = bench.supply(cooling, 0);
    bench.verdict(
        waiter,
        bench.rejection(RejectionScope::ExactTip, Charge::Cooldown),
    );
    let id = bench.id(1);
    bench.inv(cooling, &[id]);
    bench.add(&[id]);
    assert!(advertised(&bench, id).is_empty(), "its peer announced it");
}

/// A verdict's charge reaches only the peer that supplied the body: a
/// cooldown stops new asks of that peer and no other, its announcements are
/// dropped until the cooldown ends, and a second cooldown after the first
/// closes it.
#[test]
fn a_cooldown_stops_its_supplier_only_and_a_second_one_closes_it() {
    let mut bench = Bench::new();
    let (supplier, other) = (bench.join(1), bench.join(2));
    let cooldown = bench.rejection(RejectionScope::ExactTip, Charge::Cooldown);
    let waiter = bench.supply(supplier, 0);
    bench.verdict(waiter, cooldown);
    assert!(bench.closes().is_empty(), "a first cooldown closes nothing");
    let (ignored, heard) = (bench.id(1), bench.id(2));
    bench.inv(supplier, &[ignored]);
    bench.inv(other, &[heard]);
    assert_eq!(called(&bench), vec![heard], "the cooling peer is skipped");

    bench.tick(BASE_COOLDOWN);
    let waiter = bench.supply(supplier, 3);
    bench.verdict(waiter, cooldown);
    let repeated = CloseReason::RepeatedCooldown;
    assert_eq!(bench.closes(), vec![(supplier, repeated)]);
}

/// A peer whose bodies earn cooldowns gets no more of them verified per
/// cooldown than it had out, at most five: the first cooldown stops new asks
/// of the peer id, the bodies still on the wire are dropped unread, a body
/// already at the pool adds no strike, and a reconnect is asked nothing until
/// the cooldown ends.
#[test]
fn a_cooling_peer_gets_no_more_bodies_verified_than_it_had_out() {
    let mut bench = Bench::new();
    let cooldown = bench.rejection(RejectionScope::ExactTip, Charge::Cooldown);
    let session = bench.join(1);
    let ids: Vec<UnminedTxId> = (0..10).map(|n| bench.id(n)).collect();
    bench.inv(session, &ids);
    let out = called(&bench);
    assert_eq!(out.len(), MAX_REQUESTS_PER_PEER, "its limit is out");
    for id in &out {
        bench.fetch(session, *id);
    }
    let body = |bench: &Bench, id: UnminedTxId| {
        let n = ids
            .iter()
            .position(|of| *of == id)
            .expect("an announced id");
        bench.tx(u32::try_from(n).expect("a small index"))
    };
    let mut waiters = Vec::new();
    for id in &out[..2] {
        assert!(bench.deliver(session, *id, body(&bench, *id)));
        waiters.push(bench.submitted().expect("a read body goes to the pool"));
    }
    bench.verdict(waiters[0], cooldown);
    for id in &out[2..] {
        let read = bench.deliver(session, *id, body(&bench, *id));
        assert!(!read, "a body still on the wire is dropped unread");
    }
    bench.verdict(waiters[1], cooldown);
    assert!(
        bench.closes().is_empty(),
        "a body at the pool adds no strike"
    );

    bench.end(session);
    let session = bench.join(1);
    bench.tick(BASE_COOLDOWN - MS);
    bench.inv(session, &ids);
    assert!(bench.calls().is_empty(), "the peer id still cools");
    bench.tick(MS);
    bench.inv(session, &ids);
    assert_eq!(called(&bench).len(), MAX_REQUESTS_PER_PEER, "its next five");
}

/// A body from an unconditional peer's session is never charged: a `Ban`
/// closes nothing and a `Cooldown` leaves the peer heard. That holds when
/// the verdict comes after the supplying session ended, and the peer's next
/// session is the one left open. A peer that is not exempt is charged both
/// ways.
#[test]
fn an_unconditional_peer_is_never_charged() {
    for exempt in [false, true] {
        for deferred in [false, true] {
            for charge in [Charge::Ban, Charge::Cooldown] {
                let case = format!("exempt {exempt}, deferred {deferred}, {charge:?}");
                let mut bench = Bench::new();
                let mut session = bench.connect(1, exempt);
                bench.hello(session, hello());
                let waiter = bench.supply(session, 0);
                if deferred {
                    bench.end(session);
                    session = bench.connect(1, exempt);
                    bench.hello(session, hello());
                }
                bench.verdict(waiter, bench.rejection(RejectionScope::Uncached, charge));
                let closed = bench.closes().iter().any(|(closed, _)| *closed == session);
                if charge == Charge::Ban {
                    assert_eq!(closed, !exempt, "{case}");
                    continue;
                }
                assert!(!closed, "{case}");
                // A cooling peer's announcements are ignored.
                let next = bench.id(1);
                bench.inv(session, &[next]);
                bench.tick(Duration::from_secs(1));
                assert_eq!(called(&bench).contains(&next), exempt, "{case}");
            }
        }
    }
}

/// Whether the node starts to fetch body `n` when a new session of `peer`
/// announces it at `at`. The session ends before this returns.
fn heard(bench: &mut Bench, peer: u64, n: u32, at: Duration) -> bool {
    bench.now = at;
    let id = bench.id(n);
    let session = bench.join(peer);
    bench.inv(session, &[id]);
    let heard = bench.reserved(id);
    bench.end(session);
    heard
}

/// A `Ban` whose supplier's session ended before the verdict closes the
/// peer's live session, the one it reopened, and not the ended one, and
/// leaves the peer id cooling for [`MAX_COOLDOWN`]: a session that connects
/// after the close is not heard until then, and is heard once the cooldown
/// ends.
#[test]
fn a_ban_after_the_session_ended_cools_the_peer_id_and_closes_its_live_sessions() {
    let mut bench = Bench::new();
    let peer = 1;
    let ended = bench.join(peer);
    let waiter = bench.supply(ended, 0);
    bench.end(ended);
    let live = bench.join(peer);
    bench.verdict(
        waiter,
        bench.rejection(RejectionScope::Uncached, Charge::Ban),
    );
    assert_eq!(bench.closes(), vec![(live, CloseReason::Ban)]);
    assert!(bench.machine.session(live).is_none(), "the live one ended");
    let banned_at = bench.now;
    let cooling = bench
        .machine
        .reactor()
        .cooling_until(PeerKey(peer), banned_at);
    assert_eq!(cooling, Some(banned_at + MAX_COOLDOWN));

    let early = banned_at + MAX_COOLDOWN - Duration::from_secs(1);
    assert!(!heard(&mut bench, peer, 1, early));
    assert!(heard(&mut bench, peer, 2, banned_at + MAX_COOLDOWN));
}

/// With the cooldown table full, a fresh peer whose body earns a cooldown
/// is closed and not cooled, and every cooldown the table held stays. A
/// peer already in the table is handled as before. Once the held histories
/// are forgotten, they make room, and a fresh offender is cooled again.
#[test]
fn a_full_cooldown_table_closes_a_fresh_offender_and_keeps_every_cooldown() {
    let mut bench = Bench::new();
    let count = u64::try_from(MAX_COOLDOWN_PEERS).expect("20,000 fits a u64");
    let held: Vec<PeerKey> = (0..count).map(|n| PeerKey(1_000 + n)).collect();
    for peer in &held {
        let recorded = bench.machine.reactor_mut().record_invalid(*peer, bench.now);
        assert_eq!(recorded, Recorded::Started(BASE_COOLDOWN));
    }

    let fresh = 1;
    let session = bench.join(fresh);
    let waiter = bench.supply(session, 0);
    bench.verdict(
        waiter,
        bench.rejection(RejectionScope::Uncached, Charge::Cooldown),
    );
    assert_eq!(bench.closes(), vec![(session, CloseReason::CooldownsFull)]);
    let now = bench.now;
    let reactor = bench.machine.reactor_mut();
    assert!(!reactor.is_cooling(PeerKey(fresh), now), "not cooled");
    let kept = held.iter().all(|peer| reactor.is_cooling(*peer, now));
    assert!(kept, "no cooldown is lost");
    let again = reactor.record_invalid(held[0], now);
    assert_eq!(again, Recorded::AlreadyCooling, "a held peer as before");

    // A history is forgotten `MAX_COOLDOWN` after its cooldown ends.
    let forgotten = BASE_COOLDOWN + MAX_COOLDOWN;
    let recorded = reactor.record_invalid(PeerKey(fresh), forgotten);
    assert_eq!(recorded, Recorded::Started(BASE_COOLDOWN));
}

// Announcing and serving.

/// Announce what the pool admits, once, and never back to its source:
/// sessions inside the id's window announce at once, the rest after the
/// advert delay, and the session whose peer supplied the body not at all.
#[test]
fn an_admission_is_announced_once_inside_the_window_at_once_and_the_rest_later() {
    let mut bench = Bench::with(one_at_once(ADVERT_DELAY), true, &[]);
    let (first, supplier, last) = (bench.join(1), bench.join(2), bench.join(3));
    let waiter = bench.supply(supplier, 0);
    let id = bench.id(0);
    bench.add(&[id]);
    assert_eq!(bench.adverts(), vec![(first, id)], "inside the window");
    bench.verdict(waiter, Verdict::Admitted);
    bench.tick(ADVERT_DELAY - MS);
    assert!(bench.adverts().is_empty(), "the rest wait");
    bench.tick(MS);
    assert_eq!(bench.adverts(), vec![(last, id)], "for the advert delay");
    bench.tick(ADVERT_DELAY);
    bench.resync();
    assert!(bench.adverts().is_empty(), "each session hears once");
}

/// An `Inv` goes only while the data writer keeps room for every body the
/// peer may have requested, once the last `Inv` is written, and once the
/// byte rate pays for it. A session that has to wait tries again after the
/// retry interval, or when the rate pays.
#[test]
fn an_inv_waits_for_room_its_last_write_and_the_byte_rate() {
    let mut bench = Bench::new();
    let id = bench.id(0);
    bench.add(&[id]);
    let session = bench.join(1);
    let room = MAX_SERVES + 1;
    assert!(bench.send_inv(session, MAX_SERVES, Paid::Now).is_empty());
    assert!(
        bench.send_inv(session, room, Paid::Now).is_empty(),
        "it waits"
    );
    bench.tick(INV_RETRY);
    assert!(bench.send_inv(session, room, Paid::Unwritten).is_empty());
    bench.tick(INV_RETRY);
    let paid = bench.now + Duration::from_secs(1);
    assert!(bench.send_inv(session, room, Paid::At(paid)).is_empty());
    bench.tick(Duration::from_secs(1) - MS);
    assert!(
        bench.send_inv(session, room, Paid::Now).is_empty(),
        "until paid"
    );
    bench.tick(MS);
    assert_eq!(bench.send_inv(session, room, Paid::Now), vec![id]);
}

/// A small child inside a session's window waits for its big parent
/// outside it: the child's advert is queued at once, but no `Inv` carries
/// it until the parent's has gone out to that session. So the peer never
/// fetches the child first and holds it waiting for the parent.
#[test]
fn a_child_goes_out_only_after_its_parent() {
    let fan_out = FanOut {
        advert_delay: ADVERT_DELAY,
        window_small: 8,
        window_full: 1,
    };
    let mut bench = Bench::with(fan_out, true, &[]);
    let sessions = [bench.join(1), bench.join(2)];
    let (parent, child) = (bench.id(0), bench.id(1));
    bench.pool.insert(parent, MAX_TX_BYTES);
    bench.pool.insert(child, SIZE);
    bench.depends(child, &[parent]);
    bench.resync();
    let late = sessions
        .into_iter()
        .find(|session| !advertised(&bench, parent).contains(session))
        .expect("one session is outside the parent's window");
    assert!(
        advertised(&bench, child).contains(&late),
        "the child is queued"
    );
    assert!(
        bench.take_inv(late, 16).is_empty(),
        "behind its held parent"
    );
    bench.tick(ADVERT_DELAY);
    assert_eq!(bench.take_inv(late, 16), vec![parent], "the parent first");
    assert_eq!(bench.take_inv(late, 16), vec![child], "then the child");
}

/// With the advert delay set to zero, a session outside the window
/// announces in the same step as one inside it, on a pool change and on a
/// resync, with no later input to wake the machine.
#[test]
fn a_zero_advert_delay_announces_to_everyone_at_once() {
    let mut bench = Bench::with(one_at_once(Duration::ZERO), true, &[]);
    let sessions = vec![bench.join(1), bench.join(2)];
    let (added, resynced) = (bench.id(0), bench.id(1));
    bench.now = Duration::from_secs(1);
    bench.add(&[added]);
    assert_eq!(advertised(&bench, added), sessions);

    bench.pool.insert(resynced, SIZE);
    bench.now = Duration::from_secs(2);
    bench.resync();
    assert_eq!(advertised(&bench, resynced), sessions);
}

/// Sessions that open before the pool attaches announce the whole pool once
/// the first snapshot, which the IO reads as a resync, turns it on: every
/// session, whatever its rank, in that step, even with a window of one, so
/// none waits the advert delay.
#[test]
fn the_first_snapshot_opens_sessions_waiting_for_the_pool() {
    let mut bench = Bench::with(one_at_once(ADVERT_DELAY), false, &[]);
    let sessions = vec![bench.join(1), bench.join(2)];
    let quiet = !bench.tracker.seen.contains_key("advert");
    assert!(quiet, "nothing goes out before the pool");

    let pooled = bench.id(0);
    bench.pool.insert(pooled, SIZE);
    bench.active = true;
    bench.now = Duration::from_secs(1);
    bench.resync();
    assert_eq!(advertised(&bench, pooled), sessions);
}

/// A resync keeps the adverts that still stand, forgets the rest, and
/// announces what it finds.
#[test]
fn a_resync_rebuilds_every_session_from_the_snapshot() {
    let [kept, gone, found] = ids(0..3)[..] else {
        unreachable!("three ids")
    };
    let mut bench = Bench::with(FanOut::default(), true, &[kept, gone]);
    let session = bench.connect(1, false);
    assert_eq!(bench.adverts(), vec![(session, kept), (session, gone)]);
    bench.hello(session, hello());

    bench.pool.shift_remove(&gone);
    bench.pool.insert(found, SIZE);
    bench.now = Duration::from_secs(1);
    bench.resync();
    assert_eq!(bench.adverts(), vec![(session, found)]);
    let state = bench.machine.session(session).expect("live");
    assert!(state.announced.contains_key(&kept));
    assert!(!state.announced.contains_key(&gone));
    assert_eq!(bench.machine.visible().len(), 2);
}

/// Serve only what was announced: a `GetTxs` gets the body only for an id
/// this session was sent in an `Inv` and the pool still holds, within the
/// size its `Hello` takes. An id never announced, one sent only to another
/// session, one the pool let go of, and one too large for the asker get
/// `NotFound`.
#[test]
fn only_an_id_sent_on_the_session_and_still_pooled_is_served() {
    let [kept, dropped, unannounced] = ids(0..3)[..] else {
        unreachable!("three ids")
    };
    let mut bench = Bench::with(FanOut::default(), true, &[kept, dropped]);
    let (asker, other) = (bench.join(1), bench.join(2));
    let small = bench.connect(3, false);
    bench.hello(small, hello_of(MAX_REQUESTS_PER_PEER, SIZE - 1));
    bench.take_inv(asker, usize::MAX);
    bench.take_inv(small, usize::MAX);
    bench.remove(dropped, RemovalReason::Evicted);
    let cases = [
        (asker, kept, true),
        (asker, dropped, false),
        (asker, unannounced, false),
        (other, kept, false),
        (small, kept, false),
    ];
    for (session, id, served) in cases {
        bench.get_txs(session, id);
        assert_eq!(bench.effects, vec![answer(session, id, served)]);
    }
}

/// A queued advert is served only once an `Inv` took it: a `GetTxs` for it
/// before then gets `NotFound`, which leaves the advert queued.
#[test]
fn a_queued_announcement_is_served_only_once_it_is_sent() {
    let [queued, sent] = ids(0..2)[..] else {
        unreachable!("two ids")
    };
    let mut bench = Bench::with(FanOut::default(), true, &[sent, queued]);
    let session = bench.join(1);
    assert_eq!(bench.take_inv(session, 1), vec![sent]);

    bench.now = Duration::from_secs(1);
    bench.get_txs(session, sent);
    assert_eq!(bench.effects, vec![answer(session, sent, true)]);
    bench.get_txs(session, queued);
    assert_eq!(bench.effects, vec![answer(session, queued, false)]);
    // A `NotFound` withdraws only a sent advert. The queued one has not
    // reached the peer, so it stays queued.
    assert_eq!(bench.machine.queued_inv(session), 1);
}

/// An admission the pool removes before an `Inv` takes it leaves nothing
/// queued.
#[test]
fn an_addition_removed_before_its_inv_is_never_sent() {
    let mut bench = Bench::new();
    let session = bench.join(1);
    let id = bench.id(0);
    bench.now = Duration::from_secs(1);
    bench.add(&[id]);
    bench.remove(id, RemovalReason::Evicted);
    assert_eq!(bench.machine.queued_inv(session), 0);
    assert!(bench.take_inv(session, 1_024).is_empty());
}

/// An advert stands until the session reads the id's removal: a removal for
/// any reason, a tip reset among them, withdraws it whether it was queued or
/// sent, and a later admission is announced afresh to the peer that was
/// told before.
#[test]
fn a_removal_withdraws_an_advert_and_a_readmission_is_announced_again() {
    for sent in [false, true] {
        for reason in [RemovalReason::Reset, RemovalReason::Mined] {
            let case = format!("sent {sent}, {reason:?}");
            let id = ids(0..1)[0];
            let mut bench = Bench::with(FanOut::default(), true, &[id]);
            let session = bench.join(1);
            if sent {
                assert_eq!(bench.take_inv(session, usize::MAX), vec![id], "{case}");
            }
            match reason {
                RemovalReason::Reset => bench.new_tip(true),
                _ => bench.remove(id, reason),
            }
            assert_eq!(bench.machine.queued_inv(session), 0, "{case}: withdrawn");
            bench.get_txs(session, id);
            assert_eq!(bench.effects, vec![answer(session, id, false)], "{case}");

            bench.add(&[id]);
            assert_eq!(bench.adverts(), vec![(session, id)], "{case}: again");
            assert_eq!(bench.take_inv(session, usize::MAX), vec![id], "{case}");
            bench.get_txs(session, id);
            assert_eq!(bench.effects, vec![answer(session, id, true)], "{case}");
        }
    }
}

/// A peer's own `NotFound` withdraws what it told this node: an id it
/// announced and then answered `NotFound` for is announced to it once the
/// pool admits it from elsewhere, while a peer that still holds the id is
/// skipped.
#[test]
fn a_peers_not_found_lets_a_later_admission_reach_it() {
    let mut bench = Bench::new();
    let id = bench.id(0);
    let (denier, holder) = (bench.join(1), bench.join(2));
    bench.inv(denier, &[id]);
    bench.inv(holder, &[id]);
    bench.fetch(denier, id);
    bench.not_found(denier, id);
    bench.add(&[id]);
    assert_eq!(bench.adverts(), vec![(denier, id)]);
}

// The wire.

/// The `Hello` rules: a state message before the `Hello`, a second `Hello`
/// and a `Hello` outside the protocol's limits each close the session with
/// their own reason, and no request goes to a session before its `Hello`.
#[test]
fn hello_rules_close_with_their_own_reasons() {
    let mut bench = Bench::new();
    let early = bench.connect(1, false);
    bench.get_txs(early, bench.id(0));
    assert_eq!(bench.closes(), vec![broke(early, Violation::NoHelloFirst)]);
    let twice = bench.join(2);
    bench.hello(twice, hello());
    assert_eq!(bench.closes(), vec![broke(twice, Violation::HelloTwice)]);
    let (bytes, too_many) = (MAX_TX_BYTES, MAX_SERVES + 1);
    let bad = [
        hello_of(0, bytes),
        hello_of(too_many, bytes),
        hello_of(1, bytes + 1),
    ];
    for (peer, bad) in (3..).zip(bad) {
        let session = bench.connect(peer, false);
        bench.hello(session, bad);
        let closed = broke(session, Violation::BadHello);
        assert_eq!(bench.closes(), vec![closed], "{bad:?}");
    }

    let waiting = bench.connect(9, false);
    let id = bench.id(1);
    bench.inv(waiting, &[id]);
    assert!(bench.calls().is_empty(), "nothing before the Hello");
    bench.hello(waiting, hello());
    bench.fetch(waiting, id);
}

/// A body is judged before it is decoded: one of another size than its
/// peer announced closes the session as `WrongSize`, one that does not hash
/// to the requested id as `WrongId`, one that does not decode as
/// `Undecodable`, and an answer to nothing, a body or a `NotFound`, as
/// `Unrequested`.
#[test]
fn each_bad_answer_closes_with_its_own_reason() {
    for case in 0..5 {
        let mut bench = Bench::new();
        let (id, other) = (bench.id(0), bench.id(1));
        let session = bench.join(1);
        bench.inv(session, &[id]);
        bench.fetch(session, id);
        // Whether the machine let the IO decode the body, and why it closes
        // the session.
        let (read, violation) = match case {
            0 => (bench.arrived(session, id, SIZE + 1), Violation::WrongSize),
            1 => (bench.deliver(session, id, bench.tx(1)), Violation::WrongId),
            2 => (bench.arrived(session, other, SIZE), Violation::Unrequested),
            3 => {
                bench.undecodable(session, id);
                (true, Violation::Undecodable)
            }
            _ => {
                bench.not_found(session, other);
                (false, Violation::Unrequested)
            }
        };
        // Only a body of the announced size, for a request, is decoded.
        let decoded = matches!(violation, Violation::WrongId | Violation::Undecodable);
        assert_eq!(read, decoded, "{violation:?}");
        assert_eq!(bench.closes(), vec![broke(session, violation)]);
    }
}

/// The peer may have as many requests with answers coming as our `Hello`
/// serves. One answered `NotFound` frees its place in its own step, and one
/// served frees it once the IO starts writing the answer. A request past
/// the limit, or a second request for an id whose answer is coming, closes
/// the session.
#[test]
fn a_request_past_our_hello_or_twice_while_answered_closes_the_peer() {
    let mut bench = Bench::new();
    let ids: Vec<UnminedTxId> = (0..=16).map(|n| bench.id(n)).collect();
    bench.add(&ids);
    let unknown = bench.id(17);
    let twice = bench.join(1);
    bench.take_inv(twice, ids.len());
    bench.get_txs(twice, ids[0]);
    bench.get_txs(twice, ids[0]);
    assert_eq!(
        bench.closes(),
        vec![broke(twice, Violation::RequestedTwice)]
    );

    let pushy = bench.join(2);
    bench.take_inv(pushy, ids.len());
    for _ in 0..2 * MAX_SERVES {
        bench.get_txs(pushy, unknown);
    }
    for id in &ids[..MAX_SERVES] {
        bench.get_txs(pushy, *id);
    }
    bench.served(pushy, ids[0]);
    bench.get_txs(pushy, ids[MAX_SERVES]);
    assert!(
        bench.closes().is_empty(),
        "a started answer frees its place"
    );
    bench.get_txs(pushy, unknown);
    assert_eq!(
        bench.closes(),
        vec![broke(pushy, Violation::TooManyRequests)]
    );
}

/// The IO asks the machine about a body before it decodes it: an answer to
/// nothing is a violation, and a late answer is dropped unread, so a
/// malformed late body never costs the peer its connection.
#[test]
fn a_body_is_judged_before_it_is_decoded() {
    let mut bench = Bench::new();
    let (wanted, other) = (bench.id(0), bench.id(1));
    let session = bench.join(1);
    bench.inv(session, &[wanted]);
    bench.fetch(session, wanted);

    // A body of the announced size for the outstanding request is read.
    assert!(bench.arrived(session, wanted, SIZE));
    assert!(bench.effects.is_empty());

    // Past the request's deadline the node stops waiting, and the answer is
    // dropped unread, without a penalty.
    assert!(bench.next());
    assert!(!bench.arrived(session, wanted, SIZE));
    assert!(bench.closes().is_empty());

    // An answer to nothing closes the connection.
    assert!(!bench.arrived(session, other, SIZE));
    assert_eq!(bench.closes(), vec![broke(session, Violation::Unrequested)]);
}

// The machine's own bookkeeping.

/// A timer kind with one time at once keeps one entry however often its
/// source moves it, as a peer whose deadlines move on every send would push
/// it. Held adverts keep one entry per due time, and popping the front
/// forgets it.
#[test]
fn a_moved_timer_replaces_its_older_entry() {
    let id = ids(0..1)[0];
    let mut timers = Timers::default();
    for n in 1..=1_000u64 {
        let at = Duration::from_secs(n);
        timers.push(at, Timer::Deadline(id));
        timers.push(at, Timer::GiveUp(id));
    }
    assert_eq!(timers.set.len(), 2);
    for n in 1..=3u64 {
        timers.push(Duration::from_secs(n), Timer::Held);
    }
    assert_eq!(timers.set.len(), 5);
    while timers.first().is_some() {
        timers.pop_first();
    }
    assert!(timers.latest.is_empty());
}
