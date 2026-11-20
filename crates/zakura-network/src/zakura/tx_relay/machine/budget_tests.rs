//! The machine's bounds with 100 sessions: a full pool costs a few refused
//! reserves per `NoRoom` backoff, and nothing the machine keeps grows past
//! what its sessions hold.

use std::time::Duration;

use zakura_chain::transaction::{self, UnminedTxId};

use super::{
    tests::{body, hello, on, script_len, SIZE},
    CallId, Granted, Mail, Mirror, PeerKey, PeerMachine, RelayConfig, RelayEffect, RelayMachine,
    Shared, Unreserved, MAX_SERVES, NO_ROOM_FIRST, PENDING_CAP, PENDING_PER_PEER, PENDING_SHARE,
};
use crate::zakura::tx_relay::wire::MAX_INV_ENTRIES;

/// The number of sessions the bounds are for.
const SESSIONS: u64 = 100;

/// The reserve calls among `effects`.
fn reserves(effects: &[RelayEffect]) -> Vec<CallId> {
    effects
        .iter()
        .filter_map(|effect| match effect {
            RelayEffect::Reserve { call, .. } => Some(*call),
            _ => None,
        })
        .collect()
}

/// A full pool costs a few refused reserves per `NoRoom` backoff, not one per
/// session: those in flight when the first `NoRoom` lands, then one probe
/// per backoff.
#[test]
fn refused_reserves_per_backoff_stay_within_those_in_flight_plus_the_probe() {
    let script = script_len();
    let mut machine = RelayMachine::new(
        RelayConfig {
            fan_out: super::FanOut::default(),
            short_id_key: 7,
        },
        on(Vec::new()),
        Vec::new(),
    );
    let mut now = Duration::ZERO;
    // Every session announces five ids of its own.
    for peer in 0..SESSIONS {
        let session = machine.connect(PeerKey(peer), false, now);
        machine.hello(session, hello(), now);
        let first = u32::try_from(peer).expect("small") * 5;
        let announced: Vec<(UnminedTxId, u32)> = (first..first + 5)
            .map(|n| (body(n, script).id(), SIZE))
            .collect();
        machine.inv(session, &announced, now);
    }
    let in_flight = reserves(&machine.take_effects());
    assert!(!in_flight.is_empty(), "the sessions reserve");
    let mut refused = 0;
    for call in &in_flight {
        machine.reserve_answer(*call, Err(Unreserved::NoRoom), now);
        refused += 1;
    }
    // Every call in flight is refused, and none starts in its place.
    assert!(reserves(&machine.take_effects()).is_empty());

    // Each backoff ends with exactly one probe, which is refused again.
    let mut probes = 0;
    let mut backoff = NO_ROOM_FIRST;
    for _ in 0..3 {
        now += backoff;
        machine.tick(now);
        let calls = reserves(&machine.take_effects());
        assert_eq!(calls.len(), 1, "one probe per backoff, at {now:?}");
        for call in calls {
            machine.reserve_answer(call, Err(Unreserved::NoRoom), now);
            probes += 1;
        }
        assert!(reserves(&machine.take_effects()).is_empty());
        backoff = backoff.saturating_mul(2);
    }
    assert_eq!(refused + probes, in_flight.len() + 3);

    // Room again: the probe's grant clears the backoff and every session
    // reserves again.
    now += backoff;
    machine.tick(now);
    let probe = reserves(&machine.take_effects());
    assert_eq!(probe.len(), 1);
    machine.reserve_answer(
        probe[0],
        Ok(Granted {
            reservation: zakura_node_services::mempool::port::ReservationId(1),
            cancelled: false,
        }),
        now,
    );
    assert!(reserves(&machine.take_effects()).len() > 1);
    machine.assert_invariants();
}

/// The many-sessions load the plan names, in one run: 100 sessions, each
/// announcing the same pool-sized set of ids, which fills the node-wide
/// pending cap and leaves the later sessions their share, beside a full
/// pool of other transactions the node holds and announces, which stays
/// full through repeated `NoRoom` backoffs, then rounds in which every
/// grant's request misses its deadline and releases its slot before the
/// peer answers `NotFound`, with the actor falling off the pool's change log
/// after each round. The refused reserves per backoff stay within the
/// budget, and nothing the machine keeps grows past what its sessions hold.
#[test]
fn a_full_load_keeps_its_budgets_and_its_state_bounded() {
    let script = script_len();
    let pool_size = u32::try_from(PENDING_PER_PEER).expect("small");
    // A full pool the node holds, apart from the ids the peers announce.
    let pooled: Vec<(UnminedTxId, u32)> = (0..pool_size)
        .map(|n| (body(1_000_000 + n, script).id(), SIZE))
        .collect();
    let mut machine = RelayMachine::new(
        RelayConfig {
            fan_out: super::FanOut::default(),
            short_id_key: 7,
        },
        on(pooled.clone()),
        pooled.clone(),
    );
    let mut now = Duration::ZERO;
    let ids: Vec<(UnminedTxId, u32)> = (0..pool_size)
        .map(|n| (body(n, script).id(), SIZE))
        .collect();
    let mut sessions = Vec::new();
    for peer in 0..SESSIONS {
        let session = machine.connect(PeerKey(peer), false, now);
        machine.hello(session, hello(), now);
        machine.inv(session, &ids, now);
        sessions.push(session);
    }
    // The first sessions fill the node-wide cap, and the rest hold their
    // share, the ids past it counted as overflow, which closes nothing.
    let full = PENDING_CAP / PENDING_PER_PEER;
    let dropped = u64::try_from(PENDING_PER_PEER - PENDING_SHARE).expect("fits");
    for (n, session) in sessions.iter().enumerate() {
        let state = machine.session(*session).expect("live");
        let (held, overflow) = if n < full {
            (PENDING_PER_PEER, 0)
        } else {
            (PENDING_SHARE, dropped)
        };
        assert_eq!(state.pending.len(), held, "session {n}'s pending set");
        assert_eq!(state.overflow, overflow, "session {n}'s overflow");
    }
    // Sessions that have not sent their `Hello` keep what they were
    // announced while they wait, within their own pending bound, and that
    // leaves what fetchable sessions may take alone.
    let records = machine.shared.pending;
    let silent: Vec<_> = (0..=full)
        .map(|n| {
            let peer = SESSIONS + u64::try_from(n).expect("small");
            let session = machine.connect(PeerKey(peer), false, now);
            machine.inv(session, &ids, now);
            session
        })
        .collect();
    for (n, session) in silent.iter().enumerate() {
        let state = machine.session(*session).expect("live");
        assert_eq!(state.waiting.len(), PENDING_PER_PEER, "silent session {n}");
    }
    assert_eq!(machine.shared.pending, records, "waiting holds no record");
    // Once one may fetch, what it kept joins its pending set by the same
    // rule as any announcement. Every id already has a source, so past the
    // full cap it keeps its share and no id loses its source. Sessions that
    // announce before their `Hello`, one after another, stay within the
    // cap's bound however they order it.
    machine.hello(silent[0], hello(), now);
    let state = machine.session(silent[0]).expect("live");
    assert_eq!(state.pending.len(), PENDING_SHARE, "its share of held ids");
    let mut late = Vec::new();
    for n in 0..SESSIONS {
        let session = machine.connect(PeerKey(2 * SESSIONS + n), false, now);
        machine.inv(session, &ids, now);
        machine.hello(session, hello(), now);
        late.push(session);
    }
    let live = sessions.len() + silent.len() + late.len();
    let bound = 2 * PENDING_CAP + live * PENDING_SHARE;
    assert!(machine.shared.pending <= bound, "records stay bounded");
    for session in silent.into_iter().chain(late) {
        machine.end(session, now);
    }
    assert_eq!(machine.wanted().len(), PENDING_PER_PEER);
    let sessions_count = usize::try_from(SESSIONS).expect("small");

    // A full pool: those in flight are refused, then one probe a backoff.
    let in_flight = reserves(&machine.take_effects());
    assert!(in_flight.len() <= sessions_count * MAX_SERVES);
    for call in &in_flight {
        machine.reserve_answer(*call, Err(Unreserved::NoRoom), now);
    }
    assert!(reserves(&machine.take_effects()).is_empty());
    let mut backoff = NO_ROOM_FIRST;
    for _ in 0..3 {
        now += backoff;
        machine.tick(now);
        let calls = reserves(&machine.take_effects());
        assert_eq!(calls.len(), 1, "one probe per backoff under a full load");
        machine.reserve_answer(calls[0], Err(Unreserved::NoRoom), now);
        backoff = backoff.saturating_mul(2);
    }

    let mut reservation = 0;
    // Reserve calls the last round's answers started, which this one grants.
    let mut carried: Vec<CallId> = Vec::new();
    for round in 0..5 {
        now += backoff;
        machine.tick(now);
        // Every reserve is granted, and each grant sends a request.
        let mut granted = 0;
        loop {
            let mut calls = std::mem::take(&mut carried);
            calls.extend(reserves(&machine.take_effects()));
            if calls.is_empty() {
                break;
            }
            for call in calls {
                reservation += 1;
                granted += 1;
                machine.reserve_answer(
                    call,
                    Ok(Granted {
                        reservation: zakura_node_services::mempool::port::ReservationId(
                            reservation,
                        ),
                        cancelled: false,
                    }),
                    now,
                );
            }
        }
        assert!(granted > 0, "round {round}: the sessions fetch");
        assert!(machine.reservations_held() <= sessions_count * MAX_SERVES);
        // Each request misses its deadline and gives its slot back, which
        // a session that holds the id and has a free slot claims.
        while machine.reservations_held() > 0 {
            now = machine.next_deadline().expect("a deadline is pending");
            machine.tick(now);
            carried.extend(reserves(&machine.take_effects()));
        }
        // The peers answer late, before their sessions give up on them.
        for session in &sessions {
            let asked: Vec<UnminedTxId> = machine
                .session(*session)
                .map(|state| state.requests.keys().copied().collect())
                .unwrap_or_default();
            for id in asked {
                machine.not_found(*session, id, now);
            }
        }
        carried.extend(reserves(&machine.take_effects()));
        // The actor falls off the pool's change log.
        machine.pool_resynced(pooled.clone(), on(pooled.clone()), now);
        carried.extend(reserves(&machine.take_effects()));

        assert_eq!(
            machine.session_ids().count(),
            sessions_count,
            "round {round}: none closed"
        );
        assert_eq!(machine.calls_in_flight(), carried.len());
        assert!(carried.len() <= sessions_count * MAX_SERVES);
        assert_eq!(
            machine.reservations_held(),
            0,
            "round {round}: every slot came back"
        );
        assert!(machine.wanted().len() <= PENDING_PER_PEER);
        assert_eq!(
            machine.visible().len(),
            PENDING_PER_PEER,
            "the pool stays full"
        );
        let adverts: usize = sessions
            .iter()
            .filter_map(|session| machine.session(*session))
            .map(|state| state.held.iter().count() + state.announced.iter().count())
            .sum();
        assert!(adverts > 0, "round {round}: the full pool is announced");
        for (id, _) in &ids {
            let holders = machine.wanted().entry(*id).map_or(0, |entry| entry.holders);
            assert!(holders <= sessions_count);
        }
        for session in &sessions {
            let state = machine.session(*session).expect("live");
            assert!(state.pending.len() <= PENDING_PER_PEER);
            assert!(
                state.held.iter().count() + state.announced.iter().count() <= 2 * PENDING_PER_PEER,
                "round {round}: a session's adverts stay within the pool"
            );
            assert!(
                state.requests.is_empty(),
                "round {round}: every request answered"
            );
        }
        assert!(
            machine.timers_len() <= 4 * sessions_count * MAX_SERVES,
            "round {round}: {} timers kept",
            machine.timers_len()
        );
    }
    machine.assert_invariants();
}

/// Honest peers that all announce the same full pool in wire-sized frames,
/// interleaved, every peer's first two before any `Hello`, leave every id a
/// session that can fetch it, however full the pending cap gets: an id no
/// session holds yet is its first source, which copies of other ids never
/// crowd out.
#[test]
fn interleaved_full_pools_leave_every_id_a_source() {
    let script = script_len();
    let pool_size = u32::try_from(PENDING_PER_PEER).expect("small");
    let ids: Vec<(UnminedTxId, u32)> = (0..pool_size)
        .map(|n| (body(n, script).id(), SIZE))
        .collect();
    let config = RelayConfig {
        fan_out: super::FanOut::default(),
        short_id_key: 7,
    };
    let mut machine = RelayMachine::new(config, on(Vec::new()), Vec::new());
    let now = Duration::ZERO;
    let sessions: Vec<_> = (0..92)
        .map(|peer| machine.connect(PeerKey(peer), false, now))
        .collect();
    for (frame, chunk) in ids.chunks(MAX_INV_ENTRIES).enumerate() {
        if frame == 2 {
            for session in &sessions {
                machine.hello(*session, hello(), now);
            }
        }
        for session in &sessions {
            machine.inv(*session, chunk, now);
        }
    }
    assert!(machine.shared.pending >= PENDING_CAP, "the cap filled");
    assert_eq!(
        machine.wanted().len(),
        PENDING_PER_PEER,
        "every id a source"
    );
    machine.assert_invariants();
}

/// A decided id stays counted against the pending cap until each session
/// that held it drops its reference, not when the entry ends, so sessions
/// that read the decision late never let others past the cap.
#[test]
fn a_decided_reference_counts_until_its_session_drops_it() {
    let script = script_len();
    let ids: Vec<(UnminedTxId, u32)> = (0..10).map(|n| (body(n, script).id(), SIZE)).collect();
    let config = RelayConfig {
        fan_out: super::FanOut::default(),
        short_id_key: 7,
    };
    let mut shared = Shared::new(config);
    let pool = Mirror::new(&on(Vec::new()), Vec::new());
    let now = Duration::ZERO;
    let mut holder = PeerMachine::connect(&mut shared, &pool, PeerKey(1), now);
    holder.hello(&mut shared, &pool, hello(), now);
    holder.inv(&mut shared, &pool, &ids, now);
    assert_eq!(shared.pending, ids.len());

    for (id, _) in &ids {
        let generation = shared.wanted.entry(*id).expect("wanted").generation;
        shared.wanted.decide(*id, generation);
    }
    assert_eq!(
        shared.pending,
        ids.len(),
        "still held until it reads the ring"
    );
    holder.tick(&mut shared, &pool, now);
    assert_eq!(shared.pending, 0, "dropped once it read the decisions");
}

/// A cheap distinct id: the machine only keys on ids.
fn plain_id(n: u64) -> UnminedTxId {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&n.to_le_bytes());
    UnminedTxId::from_legacy_id(transaction::Hash(bytes))
}

/// Sessions that read decisions late still hold their references, so
/// first sources stop at twice the cap's references however fast the table
/// empties: holders of decided ids they have not read yet, and each next
/// session taking fresh ids, stay within that plus their shares.
#[test]
fn late_readers_keep_first_sources_within_twice_the_cap() {
    let config = RelayConfig {
        fan_out: super::FanOut::default(),
        short_id_key: 7,
    };
    let mut shared = Shared::new(config);
    let pool = Mirror::new(&on(Vec::new()), Vec::new());
    let now = Duration::ZERO;
    let per = u64::try_from(PENDING_PER_PEER).expect("small");
    let holders = 20;
    let mut machines = Vec::new();
    for peer in 0..holders {
        let ids: Vec<(UnminedTxId, u32)> =
            (0..per).map(|n| (plain_id(peer * per + n), SIZE)).collect();
        let mut holder = PeerMachine::connect(&mut shared, &pool, PeerKey(peer), now);
        holder.hello(&mut shared, &pool, hello(), now);
        holder.inv(&mut shared, &pool, &ids, now);
        // Its ids are decided before it reads the ring.
        for (id, _) in &ids {
            if let Some(entry) = shared.wanted.entry(*id) {
                let generation = entry.generation;
                shared.wanted.decide(*id, generation);
            }
        }
        machines.push(holder);
    }
    let shares = usize::try_from(holders).expect("small") * PENDING_SHARE;
    assert!(
        shared.pending <= 2 * PENDING_CAP + shares,
        "{}",
        shared.pending
    );
}

/// A session that gets the go takes its waiting ids in and counts them
/// before any other session reads its mail, so two replacements never
/// spend the same room.
#[test]
fn the_go_counts_the_ids_it_takes_in() {
    let script = script_len();
    let ids: Vec<(UnminedTxId, u32)> = (0..3).map(|n| (body(n, script).id(), SIZE)).collect();
    let config = RelayConfig {
        fan_out: super::FanOut::default(),
        short_id_key: 7,
    };
    let mut shared = Shared::new(config);
    let pool = Mirror::new(&on(Vec::new()), Vec::new());
    let now = Duration::ZERO;
    let mut first = PeerMachine::connect(&mut shared, &pool, PeerKey(1), now);
    first.hello(&mut shared, &pool, hello(), now);
    let mut second = PeerMachine::connect(&mut shared, &pool, PeerKey(1), now);
    second.hello(&mut shared, &pool, hello(), now);
    second.inv(&mut shared, &pool, &ids, now);
    assert_eq!(second.state.waiting.len(), ids.len(), "it waits for the go");

    first.end_session(&mut shared);
    second.read_mail(&mut shared, &pool, Mail::Go);
    assert_eq!(second.state.pending.len(), ids.len());
    assert_eq!(
        shared.pending,
        ids.len(),
        "counted before anyone else reads"
    );
}
