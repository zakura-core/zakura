//! disc-2.1: the discovery node runs production's discovery behind its real
//! transport on simulated QUIC, and every discovery task is a scheduler
//! task. One scripted peer dials the node offering discovery, opens a
//! discovery stream and drains what the node sends on it. The node's static
//! candidate is an address no peer listens on, so the node keeps dialing
//! it.
//!
//! disc-2.2: sim-5's peers run a discovery script. A peer that answers the
//! node's exchange gets its `Hello` and relayed record verified on
//! `discovery_verify` tasks, and the node dials a listening peer it holds
//! as a static candidate, again and again, each connection with its own
//! identity. A peer holds an answer until a virtual time, and sends its own
//! requests.
#![cfg(zakura_sim)]

use zakura_network::zakura::{
    MSG_DISCOVERY_PEERS, MSG_DISCOVERY_SERVICES, ZAKURA_CAP_DISCOVERY,
    ZAKURA_DISCOVERY_STREAM_VERSION, ZAKURA_STREAM_DISCOVERY,
};
use zakura_sim::{
    harness::{
        discovery_service::{spawn, NodeSpec},
        header_sync::wire_config,
        NetScenario,
    },
    observe::TransportFact,
    peer::{
        discovery::{Answer, DiscoveryScript, Hold, Request},
        peer_addr, PeerAction, PeerSpec,
    },
    MemorySink, NoChecks, Observation, Plan, RunReport, Sim,
};

/// The tasks production's discovery startup and sessions spawn, by label:
/// the candidate dialer and its dial, the static candidate's maintained
/// dial, and the session's pipe and source.
const DISCOVERY_TASKS: [&str; 5] = [
    "discovery_dialer",
    "discovery_dial",
    "bootstrap_dial",
    "discovery_pipe",
    "discovery_source",
];

/// Runs the node with one verification slot and `peer-1`'s address, where
/// no peer listens, as its static candidate, and `peer-0` dialing and
/// opening a discovery stream, under the all-defaults Plan.
fn run() -> RunReport {
    let mut sim = Sim::new(wire_config(), Plan::from_fuzz(&[]));
    spawn(
        &mut sim,
        &NetScenario::default(),
        &NodeSpec {
            verification_slots: 1,
            static_candidates: vec![1],
            peer_sample_limit: None,
        },
        &[PeerSpec {
            capabilities: ZAKURA_CAP_DISCOVERY,
            script: vec![PeerAction::Open {
                kind: ZAKURA_STREAM_DISCOVERY,
                version: ZAKURA_DISCOVERY_STREAM_VERSION,
            }],
            ..PeerSpec::dialing()
        }],
    );
    sim.run(&mut NoChecks, &mut MemorySink::default())
}

/// The name of every task the run's observations name as spawned, such as
/// `wire-node.1/discovery_dialer.1`.
fn spawned(report: &RunReport) -> Vec<String> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::TaskSpawned { task } => Some(task.clone()),
            _ => None,
        })
        .collect()
}

/// The label a task was spawned with: `discovery_dialer` for
/// `wire-node.1/discovery_dialer.1`.
fn label(task: &str) -> &str {
    let last = task.rsplit('/').next().unwrap_or(task);
    last.split('.').next().unwrap_or(last)
}

/// Each discovery task shows up in the run's task observations, so the
/// scheduler offered it and ran it, and the node admits the peer's
/// discovery session. The run ends with no harness failure, so the
/// transport tap saw no event from outside the run. A second run under the
/// same Plan logs the same observations, so the Plan alone decides the
/// schedule.
#[test]
fn discovery_tasks_run_on_the_scheduler() {
    let report = run();
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    let spawned = spawned(&report);
    for label in DISCOVERY_TASKS {
        assert!(
            spawned.iter().any(|task| self::label(task) == label),
            "no `{label}` task among {spawned:#?}"
        );
    }
    assert!(
        report.log.entries().iter().any(|entry| matches!(
            &entry.obs,
            Observation::Session { peer, kind, .. }
                if peer == "peer-0" && *kind == ZAKURA_STREAM_DISCOVERY
        )),
        "the node admitted no discovery session from peer-0"
    );
    let again = run();
    assert_eq!(report.log.entries(), again.log.entries());
    assert_eq!(report.end, again.end);
}

/// Runs the node with one verification slot, `node`'s static candidates
/// and `peers`, under the all-defaults Plan.
fn run_peers(static_candidates: Vec<usize>, peers: &[PeerSpec]) -> RunReport {
    let mut sim = Sim::new(wire_config(), Plan::from_fuzz(&[]));
    spawn(
        &mut sim,
        &NetScenario::default(),
        &NodeSpec {
            verification_slots: 1,
            static_candidates,
            peer_sample_limit: None,
        },
        peers,
    );
    sim.run(&mut NoChecks, &mut MemorySink::default())
}

/// A peer that speaks discovery alone and runs `script`.
fn discovering(spec: PeerSpec, script: DiscoveryScript) -> PeerSpec {
    PeerSpec {
        capabilities: ZAKURA_CAP_DISCOVERY,
        discovery: Some(script),
        ..spec
    }
}

/// The rows of the peers' `PeerFrame`s on discovery streams, with their
/// labels' names, in log order.
fn peer_frames(report: &RunReport) -> Vec<(u64, String, &'static str)> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::PeerFrame {
                connection,
                stream,
                row,
                label,
                ..
            } if stream.kind == ZAKURA_STREAM_DISCOVERY => {
                Some((*connection, row.clone().unwrap_or_default(), label.name()))
            }
            _ => None,
        })
        .collect()
}

/// The causes the node closed connections with.
fn closes(report: &RunReport) -> Vec<String> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::ConnectionClosed { cause, .. } => Some(cause.clone()),
            _ => None,
        })
        .collect()
}

/// A dialing peer that answers the node's Hello, GetPeers and GetServices,
/// relaying one record, makes the node verify on `discovery_verify`
/// scheduler tasks, and completes the exchange. Its three frames are
/// logged as conformant `PeerFrame`s on discovery's rows, each with its
/// `PeerWritten`, and the node's frames as `PeerReceived`s. A second run
/// under the same Plan logs the same observations.
#[test]
fn an_answering_peer_runs_discovery_verify() {
    let peers = [discovering(
        PeerSpec::dialing(),
        DiscoveryScript {
            relays: 1,
            ..DiscoveryScript::default()
        },
    )];
    let report = run_peers(Vec::new(), &peers);
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    let spawned = spawned(&report);
    assert!(
        spawned.iter().any(|task| label(task) == "discovery_verify"),
        "no `discovery_verify` task among {spawned:#?}"
    );
    let frames = peer_frames(&report);
    let rows: Vec<_> = frames.iter().map(|(_, row, _)| row.as_str()).collect();
    assert_eq!(rows, ["hello", "peers", "services"], "{frames:?}");
    assert!(
        frames.iter().all(|(_, _, label)| *label == "conformant"),
        "{frames:?}"
    );
    let count = |matches: fn(&Observation) -> bool| {
        report
            .log
            .entries()
            .iter()
            .filter(|entry| matches(&entry.obs))
            .count()
    };
    assert_eq!(
        count(|obs| matches!(obs, Observation::PeerWritten { .. })),
        3
    );
    assert!(
        count(|obs| matches!(obs, Observation::PeerReceived { .. })) >= 3,
        "the node's Hello, GetPeers and GetServices"
    );
    assert_eq!(
        count(|obs| matches!(obs, Observation::NodeFrame { .. })),
        0,
        "disc-3.1 labels the node's discovery frames"
    );
    assert_eq!(closes(&report), ["discovery_exchange_complete"]);
    let again = run_peers(Vec::new(), &peers);
    assert_eq!(report.log.entries(), again.log.entries());
    assert_eq!(report.end, again.end);
}

/// The node dials `peer-0`, which listens, because it holds it as a static
/// candidate. Each dial registers a connection from the peer's one address,
/// the exchange completes on it, and the bootstrap dial dials again. The
/// peer's frames on each connection are placed on that connection, so the
/// run ends with no harness failure.
#[test]
fn the_node_dials_a_listening_static_candidate() {
    let peers = [discovering(
        PeerSpec::listening(),
        DiscoveryScript::default(),
    )];
    let report = run_peers(vec![0], &peers);
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    let remote = peer_addr(0, 0).to_string();
    let registered: Vec<u64> = report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Transport {
                connection: Some(connection),
                remote: from,
                event: TransportFact::Registered,
                ..
            } if *from == remote => Some(*connection),
            _ => None,
        })
        .collect();
    assert!(
        registered.len() >= 2,
        "the node registered {registered:?} from {remote}"
    );
    let frames = peer_frames(&report);
    for connection in &registered[..2] {
        let rows: Vec<_> = frames
            .iter()
            .filter(|(on, ..)| on == connection)
            .map(|(_, row, label)| (row.as_str(), *label))
            .collect();
        assert_eq!(
            rows,
            [
                ("hello", "conformant"),
                ("peers", "conformant"),
                ("services", "conformant")
            ],
            "connection {connection}"
        );
    }
    let closes = closes(&report);
    assert!(
        closes.len() >= 2
            && closes
                .iter()
                .all(|cause| cause == "discovery_exchange_complete"),
        "{closes:?}"
    );
}

/// The message types the peers read from the node on discovery streams, in
/// log order.
fn received(report: &RunReport) -> Vec<u16> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::PeerReceived { message_type, .. } => Some(*message_type),
            _ => None,
        })
        .collect()
}

/// A peer that holds its `Services` for 1.5 s keeps the exchange open, and
/// sends its own `GetPeers` and `GetServices` meanwhile, which the node
/// answers. Both requests are labelled conformant, the held answer goes
/// last, and it still completes the exchange inside the node's 2 s wait.
#[test]
fn a_peer_holds_an_answer_and_asks_on_its_own() {
    let peers = [PeerSpec {
        script: vec![
            PeerAction::Ask {
                request: Request::GetPeers,
            },
            PeerAction::Ask {
                request: Request::GetServices,
            },
        ],
        ..discovering(
            PeerSpec::dialing(),
            DiscoveryScript {
                holds: vec![Hold {
                    answer: Answer::Services,
                    until_ms: 1_500,
                }],
                ..DiscoveryScript::default()
            },
        )
    }];
    let report = run_peers(Vec::new(), &peers);
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    let frames = peer_frames(&report);
    let mut rows: Vec<_> = frames.iter().map(|(_, row, _)| row.as_str()).collect();
    assert_eq!(rows.last(), Some(&"services"), "{frames:?}");
    rows.sort_unstable();
    assert_eq!(
        rows,
        ["get-peers", "get-services", "hello", "peers", "services"],
        "{frames:?}"
    );
    assert!(
        frames.iter().all(|(_, _, label)| *label == "conformant"),
        "{frames:?}"
    );
    let received = received(&report);
    for answer in [MSG_DISCOVERY_PEERS, MSG_DISCOVERY_SERVICES] {
        assert!(
            received.contains(&u16::from(answer)),
            "the node answered no type {answer}: {received:?}"
        );
    }
    assert_eq!(closes(&report), ["discovery_exchange_complete"]);
}

/// A peer that holds an answer past the node's 2 s initial wait times the
/// exchange out. Without the peer's `Hello`, the node closes with
/// `discovery_exchange_timed_out`. Without its `Peers`, the node's
/// `GetPeers` exchange is still open when the session retires its queries,
/// so the writer fence closes first, with `unfinished_exchange`.
#[test]
fn a_held_answer_times_the_exchange_out() {
    for (answer, cause) in [
        (Answer::Hello, "discovery_exchange_timed_out"),
        (Answer::Peers, "unfinished_exchange"),
    ] {
        let peers = [discovering(
            PeerSpec::dialing(),
            DiscoveryScript {
                holds: vec![Hold {
                    answer,
                    until_ms: 3_000,
                }],
                ..DiscoveryScript::default()
            },
        )];
        let report = run_peers(Vec::new(), &peers);
        assert_eq!(report.end.harness, None, "{answer:?}: {:?}", report.end);
        assert_eq!(closes(&report), [cause], "{answer:?}");
    }
}
