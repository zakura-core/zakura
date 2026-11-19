//! The machine's bounds with 100 sessions: a full pool costs a few refused
//! reserves per `NoRoom` backoff, and nothing the machine keeps grows past
//! what its sessions hold.
//!
//! The memory measurement is a by-hand release run that prints what it
//! measures:
//!
//! ```text
//! cargo test --release -p zakura-network --lib -- --ignored tx_relay::machine::budget_tests::bookkeeping_memory --nocapture
//! ```

// The memory measurement prints what it measured, which is its result.
#![allow(clippy::print_stdout)]

use std::time::Duration;

use zakura_chain::transaction::{self, UnminedTxId};
use zakura_test::allocations::{measure, AllocationStats};

use super::{
    tests::{body, hello, on, script_len, SIZE},
    CallId, Granted, PeerKey, RelayConfig, RelayEffect, RelayMachine, SessionId, SnapshotInfo,
    Unreserved, MAX_SERVES, NO_ROOM_FIRST, PENDING_CAP, PENDING_PER_PEER, PENDING_SHARE,
    TRIED_PER_ENTRY,
};

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
    // share.
    let full = PENDING_CAP / PENDING_PER_PEER;
    for (n, session) in sessions.iter().enumerate() {
        let state = machine.session(*session).expect("live");
        let held = if n < full {
            PENDING_PER_PEER
        } else {
            PENDING_SHARE
        };
        assert_eq!(state.pending.len(), held, "session {n}'s pending set");
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

/// The sessions of the memory measurement: the default connection count.
const MEASURED_SESSIONS: u64 = 92;

/// A cheap distinct id for the memory measurement: the machine only keys on
/// ids, and an `UnminedTxId` takes the same space whatever its kind.
fn plain_id(n: u64) -> UnminedTxId {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&n.to_le_bytes());
    bytes[31] = 0x7e;
    UnminedTxId::from_legacy_id(transaction::Hash(bytes))
}

/// `stats` in MiB, retained and at its peak.
fn mib(stats: &AllocationStats) -> (f64, f64) {
    // Byte counts far below 2^53 convert to `f64` exactly enough to print.
    let to_mib = |bytes: usize| bytes as f64 / (1024.0 * 1024.0);
    (to_mib(stats.retained_bytes), to_mib(stats.peak_live_bytes))
}

/// The machine's bookkeeping memory at the worst case, measured on this
/// thread's heap: 92 sessions, a full pool announced to each, and two
/// shapes that can't coexist. In the first, every session announces as many
/// ids of its own as it may hold, which fills the pending sets and the
/// wanted table. In the second, every session announces the same ids, which
/// fills the pending sets with shared references, and each entry is then
/// tried by `TRIED_PER_ENTRY` sessions, which fills the tried sets. It also
/// measures the peak of a resync that turns the pool on.
#[test]
#[ignore = "a by-hand memory measurement in a release build; run it with --release --ignored --nocapture"]
fn bookkeeping_memory_at_92_full_sessions() {
    let pool_len = u64::try_from(PENDING_PER_PEER).expect("small");
    let pool: Vec<(UnminedTxId, u32)> = (0..pool_len)
        .map(|n| (plain_id(1 << 40 | n), SIZE))
        .collect();
    let config = RelayConfig {
        fan_out: super::FanOut::default(),
        short_id_key: 7,
    };
    let now = Duration::ZERO;
    let off = SnapshotInfo {
        active: false,
        ..on(Vec::new())
    };

    // The pool's mirror, and the sessions with the pool announced to each.
    let (mut machine, mirror) =
        measure(|| RelayMachine::new(config, on(pool.clone()), pool.clone()));
    let (sessions, opened) = measure(|| {
        let sessions: Vec<SessionId> = (0..MEASURED_SESSIONS)
            .map(|peer| {
                let session = machine.connect(PeerKey(peer), false, now);
                machine.hello(session, hello(), now);
                session
            })
            .collect();
        drop(machine.take_effects());
        sessions
    });

    // Shape one: as many distinct ids as each session may hold.
    let announced: Vec<Vec<(UnminedTxId, u32)>> = (0..MEASURED_SESSIONS)
        .map(|peer| {
            (0..pool_len)
                .map(|n| (plain_id(peer * pool_len + n), SIZE))
                .collect()
        })
        .collect();
    let ((), pending) = measure(|| {
        for (session, ids) in sessions.iter().zip(&announced) {
            machine.inv(*session, ids, now);
            drop(machine.take_effects());
        }
    });
    let entries = machine.wanted().len();
    drop(machine);

    // The same sessions with the pool off, then a resync that turns it on
    // and opens every session with the whole pool.
    let mut machine = RelayMachine::new(config, off, Vec::new());
    for peer in 0..MEASURED_SESSIONS {
        let session = machine.connect(PeerKey(peer), false, now);
        machine.hello(session, hello(), now);
    }
    drop(machine.take_effects());
    let ((), activation) = measure(|| {
        machine.pool_resynced(pool.clone(), on(pool.clone()), now);
        drop(machine.take_effects());
    });
    drop(machine);

    // Shape two: every session announces the same ids, and each entry is
    // tried by as many sessions as it remembers.
    let shared: Vec<(UnminedTxId, u32)> = (0..pool_len)
        .map(|n| (plain_id(1 << 41 | n), SIZE))
        .collect();
    let mut machine = RelayMachine::new(config, on(pool.clone()), pool.clone());
    let sessions: Vec<SessionId> = (0..MEASURED_SESSIONS)
        .map(|peer| {
            let session = machine.connect(PeerKey(peer), false, now);
            machine.hello(session, hello(), now);
            session
        })
        .collect();
    let ((), shared_pending) = measure(|| {
        for session in &sessions {
            machine.inv(*session, &shared, now);
        }
    });
    let shared_refs = machine.wanted().held();
    let ((), tried) = measure(|| {
        let mut reservation = 0;
        loop {
            let effects = machine.take_effects();
            let calls = reserves(&effects);
            let asks: Vec<(SessionId, UnminedTxId)> = effects
                .iter()
                .filter_map(|effect| match effect {
                    RelayEffect::Ask { session, id, .. } => Some((*session, *id)),
                    _ => None,
                })
                .collect();
            if calls.is_empty() && asks.is_empty() {
                break;
            }
            for call in calls {
                reservation += 1;
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
            for (session, id) in asks {
                machine.not_found(session, id, now);
            }
        }
    });
    let pending_left: usize = sessions
        .iter()
        .filter_map(|session| machine.session(*session))
        .map(|state| state.pending.len())
        .sum();

    let (mirror_mib, _) = mib(&mirror);
    let (opened_mib, _) = mib(&opened);
    let (pending_mib, pending_peak) = mib(&pending);
    let (_, activation_peak) = mib(&activation);
    let (tried_mib, tried_peak) = mib(&tried);
    let (shared_mib, _) = mib(&shared_pending);
    // 92 converts to `f64` exactly.
    let sessions_f = MEASURED_SESSIONS as f64;
    println!(
        "bookkeeping at {MEASURED_SESSIONS} sessions, a pool of {pool_len}: \
         pool mirror {mirror_mib:.1} MiB; sessions opened with the pool announced \
         {opened_mib:.1} MiB ({:.2} MiB a session); full distinct pending sets \
         {pending_mib:.1} MiB retained, {pending_peak:.1} MiB at peak ({:.2} MiB a session, \
         {entries} entries); a resync that turns the pool on peaks at {activation_peak:.1} MiB; \
         the same ids in every session {shared_mib:.1} MiB retained ({shared_refs} references); \
         shared ids tried by up to {TRIED_PER_ENTRY} sessions each {tried_mib:.1} MiB \
         retained, {tried_peak:.1} MiB at peak, {pending_left} pending left",
        opened_mib / sessions_f,
        pending_mib / sessions_f,
    );
}
