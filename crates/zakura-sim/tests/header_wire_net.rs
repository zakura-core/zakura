//! M5-F: `header-sync/wire` over a network that loses and reorders
//! datagrams (M5.5). Conformant peers survive loss and reordering, a lost
//! QUIC Initial is retransmitted, a handshake that times out is neutral, and
//! crossed `Status` and credit updates stay conformant. Every required
//! history is read back from the run's records, and a test whose history
//! never happened fails: a missing observation is a harness failure, never
//! evidence.
//!
//! The focused runs are steered ([`support::steer`]): a rule names the drop
//! or the delivery hold a history needs, and the plan is changed one pick at
//! a time until it follows the rule.
#![cfg(zakura_sim)]

mod support;

use std::{collections::BTreeMap, net::SocketAddr, time::Duration};

use support::{
    inputs,
    steer::{before, steer, Steer},
};
use zakura_chain::parameters::Network;
use zakura_network::{
    zakura::{ZakuraHandlerError, ZakuraLocalLimits},
    Config,
};
use zakura_sim::{
    harness::{
        header_node::{Chain, Policy},
        header_sync::{HeaderWire, WirePorts, WireScenario},
        run_prepared, HarnessError, LinkEntry, LinkSpec, NetScenario, Subsystem,
    },
    model::{is_fault_cause, Label},
    observe::{DatagramOutcome, Entry, TransportFact},
    peer::{
        honest::{CreditSpec, GrantPolicy, HonestSpec, SubscribeSpec},
        peer_addr, PeerSpec, NODE_ADDR,
    },
    Checker, MemorySink, Observation, Plan, PlanInput, PropertyOutcome, RunReport, StepCtx,
};

/// The node's chain at the start of most runs.
const PRELOAD: u32 = 50;

/// The `Status` message type.
const STATUS: u16 = 1;

/// The `Headers` message type.
const HEADERS: u16 = 3;

/// The ID of header sync's generated `conformant-never-closes`.
const NEVER_CLOSES: &str = "header-sync/s5v9/conformant-never-closes";

/// Runs `scenario` under `plan` with `extra`, and returns the report, even
/// of a run that ended in a harness failure, with its records and the
/// ports.
fn run_with(
    scenario: &WireScenario,
    plan: PlanInput,
    extra: &mut dyn Checker,
) -> (RunReport, Vec<String>, WirePorts) {
    let ports = HeaderWire::prepare(scenario);
    let mut sink = MemorySink::default();
    let ran = run_prepared::<HeaderWire>(scenario, ports.clone(), plan, &mut sink, Some(extra));
    let report = match ran {
        Ok(report) => report,
        Err(HarnessError::Run { report, .. }) => *report,
        Err(error) => panic!("{error}"),
    };
    (report, sink.lines(), ports)
}

/// Runs `scenario` from `plan`, steered by `rule`, and returns the steered
/// run's report and ports.
fn steered(
    scenario: &WireScenario,
    plan: PlanInput,
    rule: &support::steer::Rule<'_>,
) -> (RunReport, WirePorts) {
    let mut ports = None;
    let (report, ..) = steer(plan, rule, &mut |plan, checker| {
        let (report, lines, run_ports) = run_with(scenario, plan, checker);
        ports = Some(run_ports);
        (report, lines)
    });
    (report, ports.expect("a steered run ran"))
}

/// An honest peer holding `chain` headers, as `spec`.
fn honest(spec: PeerSpec, chain: u32, subscribe: Option<SubscribeSpec>) -> PeerSpec {
    PeerSpec {
        honest: Some(HonestSpec { chain, subscribe }),
        ..spec
    }
}

/// The directed link from `src` to `dst`, as identities and datagram
/// records name it.
fn link(src: SocketAddr, dst: SocketAddr) -> String {
    format!("{src}->{dst}")
}

/// A link entry for `src` to `dst`: the default model, with `loss_budget`.
fn lossy(src: SocketAddr, dst: SocketAddr, loss_budget: u64) -> LinkEntry {
    LinkEntry {
        src,
        dst,
        model: LinkSpec {
            loss_budget,
            ..LinkSpec::default()
        },
    }
}

/// The production QUIC handshake timeout the node dials and accepts with,
/// from the config the wire node builds.
fn handshake_timeout() -> Duration {
    let config = Config {
        network: Network::new_regtest(Default::default()),
        ..Config::default()
    };
    ZakuraLocalLimits::from_config(&config)
        .quic
        .handshake_timeout()
        .expect("production sets a handshake timeout")
}

/// One datagram record with its place in the log and the virtual time.
#[derive(Clone, Debug)]
struct Datagram {
    at: usize,
    id: u64,
    link: String,
    len: u64,
    outcome: DatagramOutcome,
    sent_at: u64,
}

/// Every datagram record in `report`, in log order.
fn datagrams(report: &RunReport) -> Vec<Datagram> {
    report
        .log
        .entries()
        .iter()
        .enumerate()
        .filter_map(|(at, entry)| match &entry.obs {
            Observation::Datagram {
                id,
                link,
                len,
                outcome,
                sent_at,
            } => Some(Datagram {
                at,
                id: *id,
                link: link.clone(),
                len: *len,
                outcome: *outcome,
                sent_at: *sent_at,
            }),
            _ => None,
        })
        .collect()
}

/// The virtual time of each log entry of `report`, in nanoseconds.
fn times(report: &RunReport) -> Vec<u64> {
    let mut ns = 0;
    report
        .log
        .entries()
        .iter()
        .map(|entry| {
            if let Observation::TimeAdvanced { to_ns, .. } = entry.obs {
                ns = to_ns;
            }
            ns
        })
        .collect()
}

/// The position of the first entry of `entries` that `pred` accepts.
fn find(entries: &[Entry], pred: impl Fn(&Observation) -> bool) -> Option<usize> {
    entries.iter().position(|entry| pred(&entry.obs))
}

/// The connection the transport registered for `remote`, if any.
fn connection_of(entries: &[Entry], remote: SocketAddr) -> Option<u64> {
    let remote = remote.to_string();
    entries.iter().find_map(|entry| match &entry.obs {
        Observation::Transport {
            connection: Some(connection),
            remote: r,
            event: TransportFact::Registered,
            ..
        } if *r == remote => Some(*connection),
        _ => None,
    })
}

/// The checks every conformant run passes: no harness failure and no
/// violation, every frame each side sent labelled `Conformant`,
/// `conformant-never-closes` held where it applied, no close with a fault
/// cause and no misbehavior, every peer record placed, and the node at
/// `tip` headers.
fn assert_conformant(report: &RunReport, ports: &WirePorts, tip: u32) {
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    for entry in report.log.entries() {
        match &entry.obs {
            Observation::PeerFrame { label, .. } | Observation::NodeFrame { label, .. } => {
                assert_eq!(*label, Label::Conformant, "{:?}", entry.obs);
            }
            Observation::Transport {
                event: TransportFact::CloseDecided { cause: Some(cause) },
                ..
            } => assert!(!is_fault_cause(cause), "a fault close: {cause}"),
            Observation::Misbehavior { .. } => panic!("misbehavior: {:?}", entry.obs),
            _ => {}
        }
    }
    let never_closes = report
        .properties
        .iter()
        .find(|property| property.property == NEVER_CLOSES)
        .map(|property| &property.outcome);
    assert!(
        matches!(never_closes, Some(PropertyOutcome::Held { applicable }) if *applicable > 0),
        "{never_closes:?}"
    );
    assert_eq!(ports.unplaced_records(), 0, "every peer record was logged");
    assert_eq!(
        ports.selected_tip(),
        Chain::main(tip).tip(&ports.fixture),
        "the node reaches the fixture's tip"
    );
}

/// Whether some link delivered a datagram before one sent ahead of it.
fn reordered(datagrams: &[Datagram]) -> bool {
    let mut last: BTreeMap<&str, u64> = BTreeMap::new();
    datagrams
        .iter()
        .filter(|d| d.outcome == DatagramOutcome::Delivered)
        .any(|d| {
            let before = last.insert(&d.link, d.id);
            before.is_some_and(|before| d.id < before)
        })
}

/// M5.5's lossy run: an honest peer dials the node and the node dials
/// another, both holding the 120-header tree, under 64 plans. Every link
/// may lose 8 datagrams and deliver out of order, once the node has
/// admitted a session on it, so loss stays away from the handshakes.
///
/// In every plan every frame either side sends is `Conformant`,
/// `conformant-never-closes` holds, nothing closes with a fault cause, and
/// the node reaches the tip. Across the plans, at least one dropped a
/// datagram and at least one delivered one out of order, read from the
/// datagram records, and every drop came after its link's admission.
///
/// This fails if network delay is read as misconduct, or if the plans
/// never lost or reordered a datagram.
#[test]
fn conformant_peers_survive_loss_and_reordering() {
    let scenario = WireScenario {
        net: NetScenario {
            default_link: LinkSpec {
                loss_budget: 8,
                reorder: true,
                ..LinkSpec::default()
            },
            loss_after_admission: true,
            ..NetScenario::default()
        },
        node: Chain::main(PRELOAD),
        peers: vec![
            honest(PeerSpec::dialing(), 120, None),
            honest(PeerSpec::listening(), 120, None),
        ],
        policy: Policy::converging(),
    };
    let (mut lost, mut out_of_order) = (0, 0);
    for plan in inputs(64, 16_384) {
        let (report, _, ports) = run_with(&scenario, plan, &mut zakura_sim::NoChecks);
        assert_conformant(&report, &ports, 120);
        let datagrams = datagrams(&report);
        let entries = report.log.entries();
        for drop in datagrams
            .iter()
            .filter(|d| d.outcome == DatagramOutcome::Lost)
        {
            let admitted = find(entries, |obs| {
                matches!(obs, Observation::Session { peer, .. }
                if [0, 1].iter().any(|index| {
                    let addr = peer_addr(*index, 0);
                    format!("peer-{index}") == *peer
                        && (drop.link == link(addr, NODE_ADDR) || drop.link == link(NODE_ADDR, addr))
                }))
            });
            assert!(
                admitted.is_some_and(|admitted| admitted < drop.at),
                "a drop before its link's admission: {drop:?}"
            );
        }
        if datagrams.iter().any(|d| d.outcome == DatagramOutcome::Lost) {
            lost += 1;
        }
        if reordered(&datagrams) {
            out_of_order += 1;
        }
    }
    assert!(lost > 0, "no plan dropped a datagram");
    assert!(out_of_order > 0, "no plan delivered out of order");
}

/// A focused Plan drops `peer-0`'s first QUIC Initial: datagram 0 on the
/// fresh link from its first address to the node, sent before the node
/// sent anything to it, so it can only be the client's first Initial. That
/// link may lose one datagram and no other may lose any. The client then
/// retransmits: a later datagram on that link, sent before the node sent
/// anything, so an Initial too, reaches the node. The node admits the
/// session within the production 10 s handshake timeout of the first send,
/// every frame after admission is `Conformant`, no close has a fault cause,
/// and the node syncs.
///
/// This fails if the drop or the retransmission never happened, or if the
/// admission waited past the handshake timeout.
#[test]
fn a_lost_initial_is_retransmitted() {
    let peer = peer_addr(0, 0);
    let scenario = WireScenario {
        net: NetScenario {
            links: vec![lossy(peer, NODE_ADDR, 1)],
            ..NetScenario::default()
        },
        node: Chain::main(PRELOAD),
        peers: vec![honest(PeerSpec::dialing(), 120, None)],
        policy: Policy::converging(),
    };
    let to_node = link(peer, NODE_ADDR);
    let first = format!("drop:{to_node}:0");
    let rule = |cx: &StepCtx<'_>| {
        let offered = cx.enabled.iter().any(|e| e.identity.to_string() == first);
        Ok(if offered {
            Steer::Pick(first.clone())
        } else {
            Steer::Fine
        })
    };
    let (report, ports) = steered(&scenario, Plan::from_fuzz(&[]), &rule);
    let datagrams = datagrams(&report);
    let from_node = link(NODE_ADDR, peer);
    let node_first_sent = datagrams
        .iter()
        .find(|d| d.link == from_node && d.outcome == DatagramOutcome::Sent)
        .map_or(usize::MAX, |d| d.at);
    let sent: Vec<&Datagram> = datagrams
        .iter()
        .filter(|d| d.link == to_node && d.outcome == DatagramOutcome::Sent)
        .collect();
    let initial = sent.first().expect("the peer sent an Initial");
    assert_eq!(initial.id, 0);
    assert!(initial.at < node_first_sent, "sent before the node sent");
    let lost: Vec<&Datagram> = datagrams
        .iter()
        .filter(|d| d.outcome == DatagramOutcome::Lost)
        .collect();
    assert_eq!(lost.len(), 1, "one drop: {lost:?}");
    assert_eq!((lost[0].link.as_str(), lost[0].id), (to_node.as_str(), 0));
    // A client Initial is padded to at least 1,200 bytes.
    assert!(initial.len >= 1_200, "{initial:?}");
    let retransmitted = sent
        .iter()
        .find(|d| d.at > lost[0].at && d.at < node_first_sent)
        .expect("the client retransmitted before the node sent anything");
    assert!(retransmitted.len >= 1_200, "{retransmitted:?}");
    assert!(
        datagrams.iter().any(|d| d.link == to_node
            && d.id == retransmitted.id
            && d.outcome == DatagramOutcome::Delivered),
        "the retransmitted Initial reached the node"
    );
    let entries = report.log.entries();
    let admitted = find(
        entries,
        |obs| matches!(obs, Observation::Session { peer, .. } if peer == "peer-0"),
    )
    .expect("the node admitted the session");
    let ns = times(&report);
    let timeout = u64::try_from(handshake_timeout().as_nanos()).expect("seconds");
    assert!(
        ns[admitted] - initial.sent_at < timeout,
        "admitted {} ns after the first Initial",
        ns[admitted] - initial.sent_at
    );
    assert!(
        find(entries, |obs| matches!(obs, Observation::PeerFrame { .. }))
            .is_some_and(|frame| frame > admitted),
        "the peer's frames came after admission"
    );
    assert_conformant(&report, &ports, 120);
}

/// The node dials `peer-1`, and a focused Plan drops every datagram on that
/// link, which may lose 64, while `peer-0` dials the node and serves it.
/// `zakura-quic` returns `HandshakeTimeout` before any connection is
/// authenticated, so the wire node logs `dial-failed` keyed by `peer-1`'s
/// fresh address, with the error `ZakuraEndpoint::dial` returned, exactly
/// the production 10 s handshake timeout after the dial's first datagram.
/// Nothing reached `peer-1` and nothing came back, no connection to it
/// registered, so no session or frame of its exists, and no close anywhere
/// has a fault cause. The node still syncs from `peer-0`.
///
/// This fails if the drops or the timeout never happened, if the dial
/// failed some other way, or if a timeout is read as a fault.
#[test]
fn a_handshake_timeout_is_neutral() {
    let listener = peer_addr(1, 0);
    let scenario = WireScenario {
        net: NetScenario {
            links: vec![lossy(NODE_ADDR, listener, 64)],
            ..NetScenario::default()
        },
        node: Chain::main(PRELOAD),
        peers: vec![
            honest(PeerSpec::dialing(), 120, None),
            honest(PeerSpec::listening(), 120, None),
        ],
        policy: Policy::converging(),
    };
    let to_peer = link(NODE_ADDR, listener);
    let prefix = format!("drop:{to_peer}:");
    let rule = |cx: &StepCtx<'_>| {
        let drop = cx
            .enabled
            .iter()
            .map(|e| e.identity.to_string())
            .find(|identity| identity.starts_with(&prefix));
        Ok(drop.map_or(Steer::Fine, Steer::Pick))
    };
    let (report, ports) = steered(&scenario, Plan::from_fuzz(&[]), &rule);
    assert_conformant(&report, &ports, 120);
    let datagrams = datagrams(&report);
    let on_link: Vec<&Datagram> = datagrams.iter().filter(|d| d.link == to_peer).collect();
    let first = on_link
        .iter()
        .find(|d| d.outcome == DatagramOutcome::Sent)
        .expect("the node dialed peer-1");
    // Each send has its drop, and nothing else happened to any.
    let outcomes = |outcome| {
        on_link
            .iter()
            .filter(|d| d.outcome == outcome)
            .map(|d| d.id)
            .collect::<std::collections::BTreeSet<u64>>()
    };
    let sends = outcomes(DatagramOutcome::Sent);
    assert_eq!(
        outcomes(DatagramOutcome::Lost),
        sends,
        "every datagram to peer-1 was dropped: {on_link:?}"
    );
    assert_eq!(
        on_link.len(),
        2 * sends.len(),
        "nothing else reached peer-1: {on_link:?}"
    );
    assert!(sends.len() > 1, "the node retransmitted before it gave up");
    assert!(
        !datagrams
            .iter()
            .any(|d| d.link == link(listener, NODE_ADDR)),
        "peer-1 never sent"
    );
    let entries = report.log.entries();
    let remote = listener.to_string();
    let events: Vec<(usize, &TransportFact)> = entries
        .iter()
        .enumerate()
        .filter_map(|(at, entry)| match &entry.obs {
            Observation::Transport {
                remote: r, event, ..
            } if *r == remote => Some((at, event)),
            _ => None,
        })
        .collect();
    let expected = ZakuraHandlerError::QuicConnect(zakura_quic::ConnectError::HandshakeTimeout);
    assert_eq!(
        events.iter().map(|(_, event)| *event).collect::<Vec<_>>(),
        [&TransportFact::DialFailed {
            error: expected.to_string(),
        }],
        "the dial failed with the handshake timeout, and nothing else reached the transport"
    );
    let ns = times(&report);
    let timeout = u64::try_from(handshake_timeout().as_nanos()).expect("seconds");
    assert_eq!(
        ns[events[0].0] - first.sent_at,
        timeout,
        "the dial failed at the handshake deadline"
    );
    assert!(
        on_link.iter().all(|d| d.sent_at <= ns[events[0].0]),
        "nothing more was sent after the deadline"
    );
    assert_eq!(connection_of(entries, listener), None);
    let peer0 = connection_of(entries, peer_addr(0, 0)).expect("peer-0 connected");
    for entry in entries {
        match &entry.obs {
            Observation::Session { peer, .. } => assert_eq!(peer, "peer-0"),
            Observation::PeerFrame { connection, .. } => assert_eq!(*connection, peer0),
            _ => {}
        }
    }
}

/// A frame by its stream and `seq`, with where its milestones sit in the
/// log.
#[derive(Clone, Copy, Debug)]
struct Named {
    quic_stream: u64,
    seq: u64,
}

/// Where in `entries` the peer's `PeerWritten` of `frame` on `connection`
/// sits.
fn peer_written(entries: &[Entry], connection: u64, frame: Named) -> Option<usize> {
    find(entries, |obs| {
        matches!(obs, Observation::PeerWritten { connection: c, quic_stream, seq }
            if (*c, *quic_stream, *seq) == (connection, frame.quic_stream, frame.seq))
    })
}

/// Where the node's `frame-written` of `frame` on `connection` sits.
fn node_written(entries: &[Entry], connection: u64, frame: Named) -> Option<usize> {
    find(entries, |obs| {
        matches!(obs, Observation::Transport {
            connection: Some(c),
            event: TransportFact::FrameWritten { quic_stream, seq, .. },
            ..
        } if (*c, *quic_stream, *seq) == (connection, frame.quic_stream, frame.seq))
    })
}

/// Where the node's reader got the 8 header bytes of the peer's `frame`.
fn node_read(entries: &[Entry], connection: u64, frame: Named) -> Option<usize> {
    find(entries, |obs| {
        matches!(obs, Observation::Transport {
            connection: Some(c),
            event: TransportFact::HeaderRead { quic_stream, seq, .. },
            ..
        } if (*c, *quic_stream, *seq) == (connection, frame.quic_stream, frame.seq))
    })
}

/// Where the peer's `NodeFrame` of the node's `frame` sits: the peer read
/// and labelled it.
fn peer_read(entries: &[Entry], connection: u64, frame: Named) -> Option<usize> {
    find(entries, |obs| {
        matches!(obs, Observation::NodeFrame { connection: c, quic_stream: Some(q), seq, .. }
            if (*c, *q, *seq) == (connection, frame.quic_stream, frame.seq))
    })
}

/// The named frames of the crossing run, as far as `entries` reaches: the
/// node's first `Status` and second page, and the peer's first `Status`
/// and first `Grant`, its second `SubscribeHeaders` after its `Open`.
#[derive(Debug, Default)]
struct Crossing {
    /// The connection, from its header session's `Session` row.
    connection: Option<u64>,
    node_status: Option<Named>,
    node_page: Option<Named>,
    peer_status: Option<Named>,
    peer_grant: Option<Named>,
    /// Where the peer's `PeerFrame` of its first `Grant` sits.
    grant_offered: Option<usize>,
}

impl Crossing {
    /// The named frames among `entries`.
    fn of(entries: &[Entry]) -> Self {
        let mut crossing = Crossing::default();
        let (mut pages, mut updates) = (0, 0);
        for (at, entry) in entries.iter().enumerate() {
            match &entry.obs {
                Observation::Session {
                    connection, peer, ..
                } if peer == "peer-0" && crossing.connection.is_none() => {
                    crossing.connection = Some(*connection);
                }
                Observation::Transport {
                    connection: Some(c),
                    event:
                        TransportFact::FrameWriteStarted {
                            quic_stream,
                            seq,
                            message_type,
                        },
                    ..
                } if Some(*c) == crossing.connection => {
                    let named = Named {
                        quic_stream: *quic_stream,
                        seq: *seq,
                    };
                    if *message_type == STATUS && crossing.node_status.is_none() {
                        crossing.node_status = Some(named);
                    }
                    if *message_type == HEADERS {
                        pages += 1;
                        if pages == 2 {
                            crossing.node_page = Some(named);
                        }
                    }
                }
                Observation::PeerFrame {
                    connection,
                    quic_stream: Some(quic_stream),
                    seq,
                    row: Some(row),
                    ..
                } if Some(*connection) == crossing.connection => {
                    let named = Named {
                        quic_stream: *quic_stream,
                        seq: *seq,
                    };
                    if row == "status" && crossing.peer_status.is_none() {
                        crossing.peer_status = Some(named);
                    }
                    if row == "subscribe-headers" {
                        updates += 1;
                        if updates == 2 {
                            crossing.peer_grant = Some(named);
                            crossing.grant_offered = Some(at);
                        }
                    }
                }
                _ => {}
            }
        }
        crossing
    }

    /// Whether `entries` already hold both writes of a pair.
    fn written(
        entries: &[Entry],
        connection: u64,
        node: Option<Named>,
        peer: Option<Named>,
    ) -> bool {
        node.is_some_and(|frame| node_written(entries, connection, frame).is_some())
            && peer.is_some_and(|frame| peer_written(entries, connection, frame).is_some())
    }
}

/// Two crossings on the connection the node dials to `peer-0`. The peer
/// holds 50 headers and subscribes to the node, which holds 1,200, from
/// height 50 with credit for 2,000 headers and two full pages' bytes, and
/// grants back each page at once. So the node's first page carries the
/// 1,000 headers the peer advertises and its second the last 150, both
/// within credit, and the peer's `Grant` for the first page is its second
/// update.
///
/// Holds pin the crossings. From the header session's admission, the peer's
/// datagrams to the node wait until both `Status` frames have been handed to
/// their streams: the node's by its `frame-written`, the peer's by its
/// `PeerWritten`. From the peer's first `Grant` on, they wait until it and
/// the node's second page have been handed over. Each pair was written
/// before the opposite frame was read: the node's reader got the peer's
/// frame's header after its own write, and the peer labelled the node's
/// frame as a `NodeFrame` after its own. Frames are matched by connection,
/// QUIC stream and `seq`, never by datagram order, and the datagram records
/// only show that the network carried both ways.
///
/// Every frame is labelled against its sender's models at its send, and
/// all are `Conformant`, within the 2× margins and the sender's own limits.
///
/// This fails if a crossing never happened, or if a crossed frame is read
/// as misconduct.
#[test]
fn crossed_status_and_credit_updates_stay_conformant() {
    let peer = peer_addr(0, 0);
    let subscribe = SubscribeSpec {
        start: PRELOAD,
        open: CreditSpec {
            objects: 2_000,
            // Two pages of 1,000 plain headers: 82 fixed bytes and 181 per
            // header each.
            bytes: 2 * (82 + 1_000 * 181),
        },
        grants: GrantPolicy::Immediate,
    };
    let scenario = WireScenario {
        node: Chain::main(1_200),
        peers: vec![honest(PeerSpec::listening(), PRELOAD, Some(subscribe))],
        policy: Policy::converging(),
        ..WireScenario::default()
    };
    let to_node = format!("deliver:{}:", link(peer, NODE_ADDR));
    let rule = |cx: &StepCtx<'_>| {
        let entries = before(cx);
        let crossing = Crossing::of(entries);
        let Some(connection) = crossing.connection else {
            return Ok(Steer::Fine);
        };
        let status_held = !Crossing::written(
            entries,
            connection,
            crossing.node_status,
            crossing.peer_status,
        );
        let grant_held = crossing.grant_offered.is_some()
            && !Crossing::written(entries, connection, crossing.node_page, crossing.peer_grant);
        if !(status_held || grant_held) || !cx.pick.to_string().starts_with(&to_node) {
            return Ok(Steer::Fine);
        }
        cx.enabled
            .iter()
            .map(|e| e.identity.to_string())
            .find(|identity| !identity.starts_with(&to_node))
            .map(Steer::Pick)
            .ok_or_else(|| "only the held deliveries are enabled".to_owned())
    };
    let mut plans = vec![Plan::from_fuzz(&[])];
    plans.extend(inputs(2, 16_384));
    for plan in plans {
        let (report, ports) = steered(&scenario, plan, &rule);
        assert_conformant(&report, &ports, 1_200);
        let entries = report.log.entries();
        let crossing = Crossing::of(entries);
        let connection = crossing.connection.expect("the node connected to peer-0");
        let pairs = [
            ("status", crossing.node_status, crossing.peer_status),
            ("grant", crossing.node_page, crossing.peer_grant),
        ];
        for (name, node, peer) in pairs {
            let node = node.unwrap_or_else(|| panic!("{name}: the node's frame was never written"));
            let peer = peer.unwrap_or_else(|| panic!("{name}: the peer's frame was never sent"));
            let node_wrote = node_written(entries, connection, node)
                .unwrap_or_else(|| panic!("{name}: the node's write never finished"));
            let peer_wrote = peer_written(entries, connection, peer)
                .unwrap_or_else(|| panic!("{name}: the peer's write never finished"));
            let node_got = node_read(entries, connection, peer)
                .unwrap_or_else(|| panic!("{name}: the node never read the peer's frame"));
            let peer_got = peer_read(entries, connection, node)
                .unwrap_or_else(|| panic!("{name}: the peer never read the node's frame"));
            assert!(
                node_wrote < node_got,
                "{name}: the node read the peer's frame before its own was written"
            );
            assert!(
                peer_wrote < peer_got,
                "{name}: the peer read the node's frame before its own was written"
            );
        }
        let datagrams = datagrams(&report);
        for direction in [link(peer, NODE_ADDR), link(NODE_ADDR, peer)] {
            assert!(
                datagrams
                    .iter()
                    .any(|d| d.link == direction && d.outcome == DatagramOutcome::Delivered),
                "{direction} carried traffic"
            );
        }
    }
}
