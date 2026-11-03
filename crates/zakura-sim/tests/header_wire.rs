//! M5-C: `header-sync/wire`, a header-sync node behind its real transport on
//! simulated QUIC, with scripted peers that run the Zakura handshake from
//! the public wire types. These pin what the node negotiates and which
//! streams it admits or refuses in both dial directions (M5.2), read back
//! from its transport events and the identity map's `Session` rows.
#![cfg(zakura_sim)]

use std::{collections::BTreeMap, sync::Arc};

use zakura_chain::parameters::Network;
use zakura_network::{
    zakura::{
        TransportEvent, TransportObserver, ZakuraLocalLimits, ZAKURA_CAP_BLOCK_SYNC_REGULATED,
        ZAKURA_CAP_DISCOVERY, ZAKURA_CAP_HEADER_SYNC, ZAKURA_CAP_LEGACY_GOSSIP,
        ZAKURA_DISCOVERY_STREAM_VERSION, ZAKURA_HEADER_SYNC_STREAM_VERSION,
        ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION, ZAKURA_STREAM_BLOCK_SYNC,
        ZAKURA_STREAM_DISCOVERY, ZAKURA_STREAM_HEADER_SYNC,
    },
    Config,
};
use zakura_scheduler::SimInstant;
use zakura_sim::{
    harness::{
        header_sync::{wire_config, HeaderWire, WireScenario},
        run_harness,
        wire_node::TransportTap,
        NetScenario,
    },
    observe::{DatagramOutcome, TransportFact},
    peer::{peer_addr, PeerAction, PeerSpec},
    ActionSource, Boundary, HarnessFailure, Identity, MemorySink, NoChecks, Observation,
    ObservationLog, Outcome, Plan, PlanInput, RunReport, Sim,
};

/// Every capability bit the node's code defines.
const EVERY_CAPABILITY: u64 = ZAKURA_CAP_LEGACY_GOSSIP
    | ZAKURA_CAP_BLOCK_SYNC_REGULATED
    | ZAKURA_CAP_DISCOVERY
    | ZAKURA_CAP_HEADER_SYNC;

/// Header sync plus the other two native services.
const BROAD: u64 = ZAKURA_CAP_HEADER_SYNC | ZAKURA_CAP_DISCOVERY | ZAKURA_CAP_BLOCK_SYNC_REGULATED;

/// `len` bytes from a linear congruential generator seeded with `seed`.
fn bytes(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed;
    (0..len)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            state.to_be_bytes()[0]
        })
        .collect()
}

/// The plans each test runs: all defaults, then two whose picks and draws
/// come from bytes all through the handshakes and the scripted actions.
fn plans() -> Vec<PlanInput> {
    vec![
        Plan::from_fuzz(&[]),
        Plan::from_fuzz(&bytes(1, 16_384)),
        Plan::from_fuzz(&bytes(2, 16_384)),
    ]
}

/// `peers` around a node at genesis, with the default network.
fn scenario(peers: Vec<PeerSpec>) -> WireScenario {
    WireScenario {
        peers,
        ..WireScenario::default()
    }
}

/// A peer of `spec` with capabilities `capabilities` and `script`.
fn peer(spec: PeerSpec, capabilities: u64, script: Vec<PeerAction>) -> PeerSpec {
    PeerSpec {
        capabilities,
        script,
        ..spec
    }
}

/// Runs `scenario` under `plan`, and returns the report of a run that
/// ended without a harness failure.
fn run(scenario: &WireScenario, plan: PlanInput) -> RunReport {
    run_harness::<HeaderWire>(scenario, plan, &mut MemorySink::default(), None)
        .unwrap_or_else(|error| panic!("{error}"))
}

/// One transport event from the log.
#[derive(Clone, Debug)]
struct Event {
    /// The step that logged it.
    step: u64,
    connection: Option<u64>,
    remote: String,
    fact: TransportFact,
}

/// One `Session` row from the log.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Session {
    /// The step that logged it.
    step: u64,
    connection: u64,
    peer: String,
    quic_stream: u64,
    kind: u16,
    version: u16,
    session_id: u64,
}

/// A session's identity as (connection, quic_stream, kind, version,
/// session_id). The connection is optional so an admission that names none
/// matches no row.
type SessionKey = (Option<u64>, u64, u16, u16, u64);

/// The identity `obs` admits, if it's a transport `Admitted` event.
fn admitted_key(obs: &Observation) -> Option<SessionKey> {
    match obs {
        Observation::Transport {
            connection,
            event:
                TransportFact::Admitted {
                    quic_stream,
                    kind,
                    version,
                    session_id,
                    ..
                },
            ..
        } => Some((*connection, *quic_stream, *kind, *version, *session_id)),
        _ => None,
    }
}

/// The identity `obs` names, if it's a `Session` row.
fn row_key(obs: &Observation) -> Option<SessionKey> {
    match obs {
        Observation::Session {
            connection,
            quic_stream,
            kind,
            version,
            session_id,
            ..
        } => Some((
            Some(*connection),
            *quic_stream,
            *kind,
            *version,
            *session_id,
        )),
        _ => None,
    }
}

/// The transport events and session rows in `report`'s log, in order.
/// Every `Admitted` event must be followed directly by its `Session` row,
/// equal on all five identity fields, and every row must follow one, since
/// peer evidence maps sessions to connections through these rows.
fn read(report: &RunReport) -> (Vec<Event>, Vec<Session>) {
    let (mut events, mut sessions) = (Vec::new(), Vec::new());
    let entries = report.log.entries();
    for (index, entry) in entries.iter().enumerate() {
        let before = index
            .checked_sub(1)
            .and_then(|before| admitted_key(&entries[before].obs));
        if let Some(row) = row_key(&entry.obs) {
            assert_eq!(
                before,
                Some(row),
                "the session row at entry {index} matches the admission it follows"
            );
        }
        if admitted_key(&entry.obs).is_some() {
            assert!(
                entries
                    .get(index + 1)
                    .is_some_and(|next| row_key(&next.obs).is_some()),
                "the admission at entry {index} is followed by its session row"
            );
        }
        match &entry.obs {
            Observation::Transport {
                connection,
                remote,
                event,
                ..
            } => events.push(Event {
                step: entry.step,
                connection: *connection,
                remote: remote.clone(),
                fact: event.clone(),
            }),
            Observation::Session {
                connection,
                peer,
                quic_stream,
                kind,
                version,
                session_id,
            } => sessions.push(Session {
                step: entry.step,
                connection: *connection,
                peer: peer.clone(),
                quic_stream: *quic_stream,
                kind: *kind,
                version: *version,
                session_id: *session_id,
            }),
            _ => {}
        }
    }
    (events, sessions)
}

/// Each registered connection by its remote address.
fn connections(events: &[Event]) -> BTreeMap<String, u64> {
    events
        .iter()
        .filter(|event| event.fact == TransportFact::Registered)
        .map(|event| {
            (
                event.remote.clone(),
                event
                    .connection
                    .expect("a registration names its connection"),
            )
        })
        .collect()
}

/// The capability masks negotiated with `remote`, as (local, negotiated).
fn negotiated(events: &[Event], remote: &str) -> Vec<(u64, u64)> {
    events
        .iter()
        .filter(|event| event.remote == remote)
        .filter_map(|event| match event.fact {
            TransportFact::Negotiated {
                local, negotiated, ..
            } => Some((local, negotiated)),
            _ => None,
        })
        .collect()
}

/// The streams the node refused on `connection`, as (kind, version, reason).
fn refusals(events: &[Event], connection: u64) -> Vec<(u16, u16, String)> {
    events
        .iter()
        .filter(|event| event.connection == Some(connection))
        .filter_map(|event| match &event.fact {
            TransportFact::StreamRefused {
                kind,
                version,
                reason,
                ..
            } => Some((*kind, *version, reason.clone())),
            _ => None,
        })
        .collect()
}

/// The close decisions on `connection`, with the event.
fn closes(events: &[Event], connection: u64) -> Vec<(Event, Option<String>)> {
    events
        .iter()
        .filter(|event| event.connection == Some(connection))
        .filter_map(|event| match &event.fact {
            TransportFact::CloseDecided { cause } => Some((event.clone(), cause.clone())),
            _ => None,
        })
        .collect()
}

/// The frames the node wrote on `quic_stream` of `connection`, as each
/// `FrameWritten` with its sequence number.
fn writes(events: &[Event], connection: u64, quic_stream: u64) -> Vec<(Event, u64)> {
    events
        .iter()
        .filter(|event| event.connection == Some(connection))
        .filter_map(|event| match event.fact {
            TransportFact::FrameWritten {
                quic_stream: stream,
                seq,
                ..
            } if stream == quic_stream => Some((event.clone(), seq)),
            _ => None,
        })
        .collect()
}

/// `peer-<index>`'s address for its `dial`-th connection, as the log
/// writes it.
fn remote(index: usize, dial: usize) -> String {
    peer_addr(index, dial).to_string()
}

/// A scripted scenario with every peer action, a refused stream, a reopen,
/// a redial and a refused handshake replays the same picks and observations
/// when one plan runs it twice in one process, so nothing process-wide,
/// such as a QUIC connection's stable ID, leaks into the log. The test first
/// checks that the whole history happened, so two equally short runs can't
/// pass.
#[test]
fn a_scripted_wire_run_replays_identically() {
    let open = |kind, version| PeerAction::Open { kind, version };
    let scenario = scenario(vec![
        peer(
            PeerSpec::dialing(),
            BROAD,
            vec![
                open(ZAKURA_STREAM_DISCOVERY, ZAKURA_DISCOVERY_STREAM_VERSION),
                PeerAction::Close,
                PeerAction::Redial,
            ],
        ),
        peer(
            PeerSpec::listening(),
            ZAKURA_CAP_HEADER_SYNC,
            vec![PeerAction::FinishHeader],
        ),
        PeerSpec {
            bad_nonce_echo: true,
            ..PeerSpec::listening()
        },
    ]);
    for plan in plans() {
        let runs: Vec<_> = (0..2).map(|_| run(&scenario, plan.clone())).collect();
        // The history happened: every action, and each one's outcome.
        let picked: Vec<_> = runs[0]
            .picks
            .iter()
            .map(|pick| pick.identity.as_str())
            .filter(|identity| identity.starts_with("peer:"))
            .collect();
        for action in [
            "peer:peer-0:open",
            "peer:peer-0:close",
            "peer:peer-0:redial",
            "peer:peer-1:finish-header",
        ] {
            assert_eq!(
                picked.iter().filter(|pick| **pick == action).count(),
                1,
                "{action}: {picked:?}"
            );
        }
        let (events, sessions) = read(&runs[0]);
        let connections = connections(&events);
        assert_eq!(
            refusals(&events, connections[&remote(0, 0)]),
            [(
                ZAKURA_STREAM_DISCOVERY,
                ZAKURA_DISCOVERY_STREAM_VERSION,
                "unknown_kind".to_owned()
            )]
        );
        assert!(connections.contains_key(&remote(0, 1)), "the redial");
        let reopened = sessions
            .iter()
            .filter(|session| session.connection == connections[&remote(1, 0)])
            .count();
        assert_eq!(reopened, 2, "the reopen");
        assert!(events.iter().any(|event| event.remote == remote(2, 0)
            && matches!(event.fact, TransportFact::HandshakeRefused { .. })));
        assert_eq!(runs[0].picks, runs[1].picks);
        assert_eq!(runs[0].log.entries(), runs[1].log.entries());
    }
}

/// The node advertises header sync and nothing else, whichever end dials:
/// its mask, read from `Negotiated.local`, equals `ZAKURA_CAP_HEADER_SYNC`
/// exactly, and so does what each connection negotiates, though both peers
/// offer every defined bit. Each connection, `peer-0`'s redial from a fresh
/// address included, gets exactly one header session, kind 5 at version 9.
///
/// This fails if the registry or the advertised mask widens.
#[test]
fn negotiated_masks_are_header_sync_only() {
    let scenario = scenario(vec![
        peer(
            PeerSpec::dialing(),
            EVERY_CAPABILITY,
            vec![PeerAction::Close, PeerAction::Redial],
        ),
        peer(PeerSpec::listening(), EVERY_CAPABILITY, Vec::new()),
    ]);
    for plan in plans() {
        let (events, sessions) = read(&run(&scenario, plan));
        let remotes = [remote(0, 0), remote(0, 1), remote(1, 0)];
        let connections = connections(&events);
        assert_eq!(
            connections.keys().cloned().collect::<Vec<_>>(),
            remotes.to_vec(),
            "one connection per dial, each from its own address"
        );
        for remote in &remotes {
            assert_eq!(
                negotiated(&events, remote),
                [(ZAKURA_CAP_HEADER_SYNC, ZAKURA_CAP_HEADER_SYNC)],
                "{remote}"
            );
            let on: Vec<_> = sessions
                .iter()
                .filter(|session| session.connection == connections[remote])
                .map(|session| (session.kind, session.version))
                .collect();
            assert_eq!(
                on,
                [(ZAKURA_STREAM_HEADER_SYNC, ZAKURA_HEADER_SYNC_STREAM_VERSION)],
                "{remote}"
            );
        }
    }
}

/// A peer that offers header sync, discovery and block sync negotiates
/// header sync alone, whichever end dials. The node's registry has header
/// sync only, so the peer's discovery and block-sync opens each fail its
/// lookup: each shows as a `StreamRefused` with `unknown_kind`, not just as
/// a missing session, and the connection stays up, with the node's next
/// `Status` written on its header stream after both.
///
/// This fails if the node negotiates a bit it doesn't serve, or stops
/// refusing a kind it doesn't register.
#[test]
fn a_broader_peer_negotiates_down() {
    let script = vec![
        PeerAction::Open {
            kind: ZAKURA_STREAM_DISCOVERY,
            version: ZAKURA_DISCOVERY_STREAM_VERSION,
        },
        PeerAction::Open {
            kind: ZAKURA_STREAM_BLOCK_SYNC,
            version: ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION,
        },
    ];
    let scenario = scenario(vec![
        peer(PeerSpec::dialing(), BROAD, script.clone()),
        peer(PeerSpec::listening(), BROAD, script),
    ]);
    for plan in plans() {
        let (events, sessions) = read(&run(&scenario, plan));
        let connections = connections(&events);
        for remote in [remote(0, 0), remote(1, 0)] {
            let connection = connections[&remote];
            assert_eq!(
                negotiated(&events, &remote),
                [(ZAKURA_CAP_HEADER_SYNC, ZAKURA_CAP_HEADER_SYNC)],
                "{remote}"
            );
            assert_eq!(
                refusals(&events, connection),
                [
                    (
                        ZAKURA_STREAM_DISCOVERY,
                        ZAKURA_DISCOVERY_STREAM_VERSION,
                        "unknown_kind".to_owned()
                    ),
                    (
                        ZAKURA_STREAM_BLOCK_SYNC,
                        ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION,
                        "unknown_kind".to_owned()
                    ),
                ],
                "{remote}"
            );
            let header: Vec<_> = sessions
                .iter()
                .filter(|session| session.connection == connection)
                .collect();
            assert_eq!(header.len(), 1, "{remote}: {header:?}");
            assert_eq!(header[0].kind, ZAKURA_STREAM_HEADER_SYNC, "{remote}");
            let refused_last = events
                .iter()
                .filter(|event| {
                    event.connection == Some(connection)
                        && matches!(event.fact, TransportFact::StreamRefused { .. })
                })
                .map(|event| event.step)
                .max()
                .expect("both opens were refused");
            let (later, _) = writes(&events, connection, header[0].quic_stream)
                .into_iter()
                .find(|(write, _)| write.step > refused_last)
                .unwrap_or_else(|| panic!("{remote}: the node wrote again after the refusals"));
            assert!(
                closes(&events, connection)
                    .iter()
                    .all(|(close, _)| close.step > later.step),
                "{remote}: the connection stayed up past the refusals"
            );
        }
    }
}

/// A peer that offers discovery alone negotiates nothing the node serves,
/// whichever end dials. It gets no header session, and its own header-sync
/// open is refused as an unnegotiated capability: header sync is
/// registered, but not negotiated on this connection.
///
/// This fails if an unnegotiated stream opens.
#[test]
fn a_peer_without_header_sync_gets_no_header_session() {
    let script = vec![PeerAction::Open {
        kind: ZAKURA_STREAM_HEADER_SYNC,
        version: ZAKURA_HEADER_SYNC_STREAM_VERSION,
    }];
    let scenario = scenario(vec![
        peer(PeerSpec::dialing(), ZAKURA_CAP_DISCOVERY, script.clone()),
        peer(PeerSpec::listening(), ZAKURA_CAP_DISCOVERY, script),
    ]);
    for plan in plans() {
        let (events, sessions) = read(&run(&scenario, plan));
        let connections = connections(&events);
        for remote in [remote(0, 0), remote(1, 0)] {
            let connection = connections[&remote];
            assert_eq!(
                negotiated(&events, &remote),
                [(ZAKURA_CAP_HEADER_SYNC, 0)],
                "{remote}"
            );
            assert!(
                !sessions
                    .iter()
                    .any(|session| session.connection == connection),
                "{remote}: {sessions:?}"
            );
            assert_eq!(
                refusals(&events, connection),
                [(
                    ZAKURA_STREAM_HEADER_SYNC,
                    ZAKURA_HEADER_SYNC_STREAM_VERSION,
                    "unnegotiated_capability".to_owned()
                )],
                "{remote}"
            );
        }
    }
}

/// When the node dials, it opens the header stream, and a responder that
/// opens one too is refused. That path emits no stream refusal: the node
/// closes the connection, with the cause pinned here. The node's own
/// session is the only one, on a stream the dialer opened: client-opened
/// bidirectional QUIC streams have IDs divisible by 4.
///
/// This fails if a non-dialer's header stream is accepted.
#[test]
fn only_the_dialer_opens_the_header_stream() {
    let scenario = scenario(vec![peer(
        PeerSpec::listening(),
        ZAKURA_CAP_HEADER_SYNC,
        vec![PeerAction::Open {
            kind: ZAKURA_STREAM_HEADER_SYNC,
            version: ZAKURA_HEADER_SYNC_STREAM_VERSION,
        }],
    )]);
    for plan in plans() {
        let (events, sessions) = read(&run(&scenario, plan));
        let connection = connections(&events)[&remote(0, 0)];
        assert_eq!(sessions.len(), 1, "{sessions:?}");
        assert_eq!(sessions[0].connection, connection);
        assert_eq!(sessions[0].quic_stream % 4, 0, "the node opened it");
        assert_eq!(refusals(&events, connection), []);
        let causes: Vec<_> = closes(&events, connection)
            .into_iter()
            .map(|(_, cause)| cause)
            .collect();
        assert_eq!(causes, [Some("unexpected_stream".to_owned())]);
    }
}

/// When the node dials and the responder's ack echoes a wrong nonce, the
/// node refuses the connection in its handshake, before it negotiates or
/// registers anything, so no session starts, and its dial fails.
#[test]
fn a_bad_nonce_echo_is_refused() {
    let scenario = scenario(vec![PeerSpec {
        bad_nonce_echo: true,
        ..PeerSpec::listening()
    }]);
    for plan in plans() {
        let (events, sessions) = read(&run(&scenario, plan));
        let seen: Vec<_> = events
            .iter()
            .map(|event| (event.connection, event.remote.clone(), event.fact.clone()))
            .collect();
        // The node's dial then fails with the same error.
        assert_eq!(
            seen,
            [
                (
                    None,
                    remote(0, 0),
                    TransportFact::HandshakeRefused {
                        reason: "Zakura control nonce mismatch".to_owned()
                    }
                ),
                (
                    None,
                    remote(0, 0),
                    TransportFact::DialFailed {
                        error: "Zakura control nonce mismatch".to_owned()
                    }
                )
            ]
        );
        assert_eq!(sessions, []);
    }
}

/// Retiring a header stream without closing the connection needs no
/// exchange started without an ending and no following frame being written
/// (`regulation/writer_fence.rs`), and only the dialer reopens. The state is
/// reachable for header sync, and this builds it on purpose: the node dials
/// `peer-0`, neither side subscribes, and the peer has read the node's one
/// frame, its `Status`, when it finishes its side of the header stream.
///
/// The node then reopens on the live connection. The new stream is a new
/// identity: a second `Session` row on the same connection, with a new QUIC
/// stream ID and a new session ID, whose frames count from 0 again, and no
/// close decided before the node's first frame on it. A missing observation
/// is a harness failure, never evidence that the state is unreachable.
///
/// This fails if a retirement with nothing outstanding closes the
/// connection, or the dialer stops reopening.
#[test]
fn a_reopened_header_stream_gets_a_new_identity() {
    let scenario = scenario(vec![peer(
        PeerSpec::listening(),
        ZAKURA_CAP_HEADER_SYNC,
        vec![PeerAction::FinishHeader],
    )]);
    for plan in plans() {
        let report = run(&scenario, plan);
        let (events, sessions) = read(&report);
        let finished = report
            .picks
            .iter()
            .find(|pick| pick.identity == "peer:peer-0:finish-header")
            .expect("the peer finished its header stream")
            .step;
        let connection = connections(&events)[&remote(0, 0)];
        let first = &sessions[0];
        assert!(first.step < finished);
        // The state the reopen needs, from the node's side: no regulation
        // transition, so no exchange, and one frame written, which the peer
        // read before it was ready to finish.
        let before: Vec<_> = events
            .iter()
            .filter(|event| event.step < finished)
            .collect();
        assert!(!before
            .iter()
            .any(|event| matches!(event.fact, TransportFact::Regulated { .. })));
        let written: Vec<_> = writes(&events, connection, first.quic_stream)
            .into_iter()
            .filter(|(write, _)| write.step < finished)
            .map(|(_, seq)| seq)
            .collect();
        assert_eq!(written, [0]);

        let reopened = sessions
            .iter()
            .find(|session| session.step > finished)
            .expect("the node reopened the header stream");
        assert_eq!(
            (
                reopened.connection,
                &reopened.peer,
                reopened.kind,
                reopened.version
            ),
            (
                connection,
                &first.peer,
                ZAKURA_STREAM_HEADER_SYNC,
                ZAKURA_HEADER_SYNC_STREAM_VERSION
            ),
            "on the same connection"
        );
        assert_ne!(reopened.quic_stream, first.quic_stream);
        assert_ne!(reopened.session_id, first.session_id);
        let (status, seq) = writes(&events, connection, reopened.quic_stream)
            .into_iter()
            .next()
            .expect("the node wrote on the reopened stream");
        assert_eq!(seq, 0, "the reopened stream's frames count from 0");
        assert!(
            closes(&events, connection)
                .iter()
                .all(|(close, _)| close.step > status.step),
            "the connection stayed up through the reopen"
        );
    }
}

/// With `loss_after_admission`, no datagram is dropped until the identity
/// map admits a session on its link. The tap opens the link when it logs
/// the session's row, and only then may the `Plan` drop on it, within the
/// link's budget.
///
/// This fails if the tap doesn't open the link, or opens it early.
#[test]
fn loss_opens_once_a_session_is_admitted() {
    let mut scenario = scenario(vec![PeerSpec::dialing()]);
    scenario.net.loss_after_admission = true;
    scenario.net.default_link.loss_budget = 2;
    // Byte-chosen picks all through the handshake and past it, so the plan
    // takes drops once they're offered.
    let report = run(&scenario, Plan::from_fuzz(&bytes(1, 16_384)));
    let (_, sessions) = read(&report);
    let admitted = sessions[0].step;
    let lost: Vec<_> = report
        .log
        .entries()
        .iter()
        .filter(|entry| {
            matches!(
                entry.obs,
                Observation::Datagram {
                    outcome: DatagramOutcome::Lost,
                    ..
                }
            )
        })
        .map(|entry| entry.step)
        .collect();
    assert!(
        !lost.is_empty(),
        "the plan dropped a datagram once it could"
    );
    assert!(
        lost.iter().all(|step| *step > admitted),
        "every drop came after the session's row at step {admitted}: {lost:?}"
    );
}

/// The node's endpoint binds at its production capacities, so 17 peers that
/// dial it at once each get a connection and a header session. A bind with
/// room for 16 connections leaves one peer out.
///
/// This fails if the node binds at test capacities instead of
/// `quic_bind_config`'s.
#[test]
fn seventeen_dialing_peers_are_all_admitted() {
    let scenario = scenario(vec![PeerSpec::dialing(); 17]);
    for plan in plans() {
        let (events, sessions) = read(&run(&scenario, plan));
        let connections = connections(&events);
        for index in 0..17 {
            let remote = remote(index, 0);
            let connection = connections
                .get(&remote)
                .unwrap_or_else(|| panic!("{remote} registered: {connections:?}"));
            assert_eq!(
                sessions
                    .iter()
                    .filter(|session| session.connection == *connection)
                    .count(),
                1,
                "{remote}"
            );
        }
    }
}

/// A transport tap that forwards everything, but observes into a copy of
/// the log and logs what it delivered there after `edit` changes it.
struct Rewrite {
    inner: TransportTap,
    edit: fn(Vec<Observation>) -> Vec<Observation>,
}

impl ActionSource for Rewrite {
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        self.inner.enabled(now, out);
    }

    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        self.inner.boundaries(now, out);
    }

    fn execute(&mut self, local: u64, log: &mut ObservationLog) {
        self.inner.execute(local, log);
    }

    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        let mut copy = log.clone();
        let cursor = copy.entries().len();
        self.inner.observe(&mut copy)?;
        let delivered = copy.entries()[cursor..]
            .iter()
            .map(|entry| entry.obs.clone())
            .collect();
        for obs in (self.edit)(delivered) {
            log.push(obs);
        }
        Ok(())
    }
}

/// Runs a transport tap, wrapped to apply `edit` to what it delivers, on
/// one connection's registration and header-session admission, reported
/// by a task. Returns the report.
fn tap_run(edit: fn(Vec<Observation>) -> Vec<Observation>) -> RunReport {
    let mut sim = Sim::new(wire_config(), Plan::from_fuzz(&[]));
    let (observer, tap) = TransportTap::new(
        &mut sim,
        "node",
        NetScenario::default().net(),
        vec!["peer-0".to_owned()],
    );
    sim.add_source(Box::new(Rewrite { inner: tap, edit }));
    let reporter: Arc<dyn TransportObserver> = observer.clone();
    sim.scheduler().spawn_root("node", async move {
        let config = Config {
            network: Network::new_regtest(Default::default()),
            ..Config::default()
        };
        let limits = ZakuraLocalLimits::from_config(&config);
        reporter.observe(TransportEvent::Registered {
            conn_id: 1,
            remote: peer_addr(0, 0),
        });
        reporter.observe(TransportEvent::Admitted {
            conn_id: 1,
            quic_stream: 4,
            kind: ZAKURA_STREAM_HEADER_SYNC,
            version: ZAKURA_HEADER_SYNC_STREAM_VERSION,
            session_id: 7,
            negotiated_limits: limits.clamp(&limits.initial_limits()),
            inbound_depth: 4,
            outbound_depth: 4_096,
        });
    });
    let report = sim.run(&mut NoChecks, &mut MemorySink::default());
    // The observer outlives the run, as the node's handler does.
    drop(observer);
    report
}

/// The tap's checkpoint covers the `Session` rows it derives, not just the
/// transport's own events: a wrapper that drops a row, or changes one,
/// fails the run as lost evidence, and one that relogs everything
/// unchanged passes.
///
/// This fails if the checkpoint records only the transport's events.
#[test]
fn a_lost_or_altered_session_row_fails_the_run() {
    let faithful = tap_run(|delivered| delivered);
    assert_ne!(faithful.end.outcome, Outcome::Harness, "{:?}", faithful.end);
    assert!(faithful
        .log
        .entries()
        .iter()
        .any(|entry| matches!(entry.obs, Observation::Session { .. })));
    let dropped: fn(Vec<Observation>) -> Vec<Observation> = |delivered| {
        delivered
            .into_iter()
            .filter(|obs| !matches!(obs, Observation::Session { .. }))
            .collect()
    };
    let altered: fn(Vec<Observation>) -> Vec<Observation> = |delivered| {
        delivered
            .into_iter()
            .map(|obs| match obs {
                Observation::Session {
                    connection,
                    quic_stream,
                    kind,
                    version,
                    session_id,
                    ..
                } => Observation::Session {
                    connection,
                    peer: "peer-1".to_owned(),
                    quic_stream,
                    kind,
                    version,
                    session_id,
                },
                obs => obs,
            })
            .collect()
    };
    for (case, edit) in [("dropped", dropped), ("altered", altered)] {
        let report = tap_run(edit);
        let Some(HarnessFailure::SourceEvidence { source, detail }) = &report.end.harness else {
            panic!("{case}: expected an evidence failure, got {:?}", report.end);
        };
        assert_eq!(source, "transport:node#0", "{case}");
        assert!(detail.starts_with("Undelivered"), "{case}: {detail}");
    }
}
