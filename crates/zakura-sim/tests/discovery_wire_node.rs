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

mod support;

use support::steer::{before, steer, Steer};
use zakura_chain::parameters::Network;
use zakura_network::{
    zakura::{
        ZakuraLocalLimits, MSG_DISCOVERY_PEERS, MSG_DISCOVERY_SERVICES, ZAKURA_CAP_DISCOVERY,
        ZAKURA_DISCOVERY_STREAM_VERSION, ZAKURA_STREAM_DISCOVERY,
    },
    Config,
};
use zakura_sim::{
    harness::{
        discovery_service::{spawn, NodeSpec},
        header_sync::wire_config,
        LinkEntry, LinkSpec, NetScenario,
    },
    model::Label,
    observe::{DatagramOutcome, TransportFact},
    peer::{
        discovery::{Answer, DiscoveryScript, Hold, Request},
        peer_addr, PeerAction, PeerSpec, NODE_ADDR,
    },
    Checker, MemorySink, NoChecks, Observation, Plan, PlanInput, RunReport, Sim, StepCtx,
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
    // disc-3.1's sender adapter labels each node frame a peer read.
    assert_eq!(
        count(|obs| matches!(obs, Observation::NodeFrame { .. })),
        count(|obs| matches!(obs, Observation::PeerReceived { .. })),
    );
    assert_eq!(
        count(|obs| matches!(
            obs,
            Observation::NodeFrame {
                label: Label::Conformant,
                exhausted: false,
                ..
            }
        )),
        count(|obs| matches!(obs, Observation::NodeFrame { .. })),
        "the node's frames follow the sender's rules"
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

/// The node's datagrams to `peer` that [`a_redial_the_peer_never_handled`]
/// drops, by ID on their link, from the log entries `entries`. Each redial
/// after the node tore its first connection from `peer` down starts with a
/// client Initial, padded to at least 1,200 bytes, and the redial's later
/// datagrams are dropped until the node ends it with a registration or a
/// failed dial. The first redial the node ends with a refused handshake is
/// the last one dropped.
fn dropped_redial(entries: &[zakura_sim::observe::Entry], peer: std::net::SocketAddr) -> Vec<u64> {
    let remote = peer.to_string();
    let to_peer = format!("{NODE_ADDR}->{peer}");
    let mut torn_down = false;
    let mut redialing = false;
    let mut dropped = Vec::new();
    for entry in entries {
        match &entry.obs {
            Observation::Transport {
                remote: from,
                event,
                ..
            } if *from == remote => match event {
                TransportFact::TornDown => torn_down = true,
                TransportFact::HandshakeRefused { .. } if redialing => return dropped,
                TransportFact::Registered | TransportFact::DialFailed { .. } => redialing = false,
                _ => {}
            },
            Observation::Datagram {
                id,
                link,
                len,
                outcome: DatagramOutcome::Sent,
                ..
            } if *link == to_peer && torn_down => {
                if redialing {
                    dropped.push(*id);
                } else if *len >= 1_200 {
                    redialing = true;
                }
            }
            _ => {}
        }
    }
    dropped
}

/// The production QUIC handshake timeout a listening peer accepts with,
/// from the config the wire node builds.
fn handshake_timeout_ns() -> u64 {
    let config = Config {
        network: Network::new_regtest(Default::default()),
        ..Config::default()
    };
    let timeout = ZakuraLocalLimits::from_config(&config)
        .quic
        .handshake_timeout()
        .expect("production sets a handshake timeout");
    u64::try_from(timeout.as_nanos()).expect("seconds fit u64 nanoseconds")
}

/// The node redials `peer-0`, which listens, and a focused Plan drops every
/// datagram the node sends on the redial after its first Initial, until the
/// node refuses the redial's handshake, at least the peer's QUIC
/// handshake timeout after the redial's Initial. The peer's flight reaches the node, so the node's QUIC
/// handshake completes, but the node's last flight never reaches the peer,
/// whose handshake ends before `Acceptor::handle`. So the node refuses
/// a handshake the peer never counted as a connection. The node's next
/// connection to the peer registers, the exchange completes on it, and the
/// peer's frames are placed on it.
///
/// This fails with `UnplacedRecord` if the node and the peer count the
/// peer's connections apart (#2160), and if the drops, the refusal or the
/// next connection never happened.
#[test]
fn a_redial_the_peer_never_handled() {
    let peer = peer_addr(0, 0);
    let net = NetScenario {
        links: vec![LinkEntry {
            src: NODE_ADDR,
            dst: peer,
            model: LinkSpec {
                loss_budget: 64,
                ..LinkSpec::default()
            },
        }],
        loss_after_admission: true,
        ..NetScenario::default()
    };
    let peers = [discovering(
        PeerSpec::listening(),
        DiscoveryScript::default(),
    )];
    let to_peer = format!("{NODE_ADDR}->{peer}");
    let rule = |cx: &StepCtx<'_>| {
        let drop = dropped_redial(before(cx), peer)
            .into_iter()
            .map(|id| format!("drop:{to_peer}:{id}"))
            .find(|drop| cx.enabled.iter().any(|e| e.identity.to_string() == *drop));
        Ok(drop.map_or(Steer::Fine, Steer::Pick))
    };
    let (report, ..) = steer(
        Plan::from_fuzz(&[]),
        &rule,
        &mut |plan: PlanInput, checker: &mut dyn Checker| {
            let mut sim = Sim::new(wire_config(), plan);
            spawn(
                &mut sim,
                &net,
                &NodeSpec {
                    verification_slots: 1,
                    static_candidates: vec![0],
                    peer_sample_limit: None,
                },
                &peers,
            );
            let mut sink = MemorySink::default();
            let report = sim.run(checker, &mut sink);
            (report, sink.lines())
        },
    );
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    let entries = report.log.entries();
    let dropped = dropped_redial(entries, peer);
    assert!(!dropped.is_empty(), "no redial datagram was dropped");
    for id in &dropped {
        assert!(
            entries
                .iter()
                .any(|entry| matches!(&entry.obs, Observation::Datagram {
                id: lost,
                link,
                outcome: DatagramOutcome::Lost,
                ..
            } if lost == id && *link == to_peer)),
            "the node's datagram {id} to the peer wasn't dropped"
        );
    }
    let remote = peer.to_string();
    let refused = entries
        .iter()
        .position(|entry| {
            matches!(&entry.obs, Observation::Transport {
                remote: from,
                event: TransportFact::HandshakeRefused { .. },
                ..
            } if *from == remote)
        })
        .expect("the node refused the redial's handshake");
    // The redial's Initial is the node's datagram to the peer just before
    // the first one dropped.
    let initial = dropped[0] - 1;
    let initial_at = entries
        .iter()
        .find_map(|entry| match &entry.obs {
            Observation::Datagram {
                id,
                link,
                outcome: DatagramOutcome::Sent,
                sent_at,
                ..
            } if *id == initial && *link == to_peer => Some(*sent_at),
            _ => None,
        })
        .expect("the node sent the redial's Initial");
    let refused_at = entries[..refused]
        .iter()
        .rev()
        .find_map(|entry| match entry.obs {
            Observation::TimeAdvanced { to_ns, .. } => Some(to_ns),
            _ => None,
        })
        .unwrap_or_default();
    assert!(
        refused_at - initial_at >= handshake_timeout_ns(),
        "the node refused the redial {} ns after its Initial",
        refused_at - initial_at
    );
    let next = entries[refused..]
        .iter()
        .find_map(|entry| match &entry.obs {
            Observation::Transport {
                connection: Some(connection),
                remote: from,
                event: TransportFact::Registered,
                ..
            } if *from == remote => Some(*connection),
            _ => None,
        })
        .expect("the node registered a connection after the refusal");
    let rows: Vec<_> = peer_frames(&report)
        .into_iter()
        .filter(|(on, ..)| *on == next)
        .map(|(_, row, label)| (row, label))
        .collect();
    assert_eq!(
        rows,
        [
            ("hello".to_owned(), "conformant"),
            ("peers".to_owned(), "conformant"),
            ("services".to_owned(), "conformant")
        ],
        "connection {next}"
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
