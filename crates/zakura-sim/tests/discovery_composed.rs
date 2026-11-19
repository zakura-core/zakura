//! `discovery/composed` (fuzz-11 for stream 4): a prefix composes the
//! discovery peer's script. Two requests in either order both reach the
//! node on the peer's connection and get their answers, and an act that
//! never becomes ready is skipped rather than stalling or failing the run.
#![cfg(zakura_sim)]

use zakura_network::zakura::{MSG_DISCOVERY_PEERS, MSG_DISCOVERY_SERVICES};
use zakura_sim::{
    explore::{classify, run_bytes, ExploreOutcome},
    harness::{
        self,
        discovery_service::{composed_prefix, DiscoveryComposed, WireScenario},
        Subsystem,
    },
    peer::{
        discovery::{DiscoveryAct, DiscoveryRow, Request},
        PeerAction,
    },
    NullSink, Observation, RunReport,
};

/// Runs `prefix` on `discovery/composed` under the all-defaults Plan, and
/// returns the report once the run classifies clean.
fn run_clean(prefix: &[u8]) -> RunReport {
    let entry = harness::find("discovery/composed").unwrap();
    let run = run_bytes::<DiscoveryComposed>(prefix, &mut NullSink, None);
    let classified = classify(entry, &run);
    assert_eq!(classified.outcome, ExploreOutcome::Clean, "{classified:?}");
    run.result.unwrap()
}

/// The message types the peer read from the node, in log order.
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

/// The rows the peer sent, each with its label, in log order.
fn sent(report: &RunReport) -> Vec<(String, String)> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::PeerFrame { row, label, .. } => {
                Some((row.clone().unwrap_or_default(), label.name().to_owned()))
            }
            _ => None,
        })
        .collect()
}

/// A prefix decodes to the steps it encodes, the encoder refuses a
/// rule-breaking act unless its script byte allows one, and a prefix with
/// another version byte draws the default scenario.
#[test]
fn a_prefix_round_trips_its_steps() {
    let steps = [
        PeerAction::Ask {
            request: Request::GetOnePeer,
        },
        PeerAction::Discover {
            act: DiscoveryAct::HelloBurst,
        },
        PeerAction::Ask {
            request: Request::GetHeaderSyncService,
        },
    ];
    let mut prefix = composed_prefix(0, 1, 0, &steps).unwrap();
    let scenario = DiscoveryComposed::draw(&prefix);
    assert_eq!(scenario.peers[0].script, steps);
    assert!(scenario.peers[0].skip_stale_acts);
    assert!(DiscoveryComposed::accepts(&prefix));
    prefix[0] ^= 0xff;
    assert!(!DiscoveryComposed::accepts(&prefix));
    assert_eq!(DiscoveryComposed::draw(&prefix), WireScenario::default());
    let undeclared = PeerAction::Discover {
        act: DiscoveryAct::UndeclaredType { message_type: 0 },
    };
    assert!(composed_prefix(0, 1, 0, &[undeclared]).is_err());
    let hostile = composed_prefix(0, 0x81, 0, &[undeclared]).unwrap();
    assert_eq!(
        DiscoveryComposed::draw(&hostile).peers[0].script,
        [undeclared]
    );
}

/// Holds `Services` (bit 2) until 1,500 ms (bits 3 and 4: 1), which keeps
/// the node's exchange, and so its session, open meanwhile.
const HOLD_SERVICES: u8 = 0b0000_1100;

/// The peer asks for one record and for the header-sync service, in both
/// orders, on top of the node's own exchange, which its held `Services`
/// keeps open. Every frame it sends is conformant, the run violates
/// nothing, and the node answers both.
#[test]
fn two_requests_in_either_order_are_both_answered() {
    let one_peer = PeerAction::Ask {
        request: Request::GetOnePeer,
    };
    let header_sync = PeerAction::Ask {
        request: Request::GetHeaderSyncService,
    };
    for steps in [[one_peer, header_sync], [header_sync, one_peer]] {
        let report = run_clean(&composed_prefix(0, 1, HOLD_SERVICES, &steps).unwrap());
        let sent = sent(&report);
        assert!(
            sent.iter().all(|(_, label)| label == "conformant"),
            "{sent:?}"
        );
        for row in ["get-peers", "get-services"] {
            assert!(
                sent.iter().any(|(sent, _)| sent == row),
                "{steps:?}: no {row} sent: {sent:?}"
            );
        }
        let received = received(&report);
        for answer in [MSG_DISCOVERY_PEERS, MSG_DISCOVERY_SERVICES] {
            assert!(
                received.contains(&u16::from(answer)),
                "{steps:?}: the node answered no type {answer}: {received:?}"
            );
        }
    }
}

/// An expired relay needs a held `Peers` that carries records, and this
/// peer holds only its `Services`, until 1.5 s, so the act never becomes
/// ready while the held answer keeps the node's exchange open. The peer
/// skips the act once its wait runs out, and still sends the request after
/// it, which the node answers.
#[test]
fn an_act_that_never_becomes_ready_is_skipped() {
    let steps = [
        PeerAction::Discover {
            act: DiscoveryAct::ExpiredRelay,
        },
        PeerAction::Ask {
            request: Request::GetServices,
        },
    ];
    let report = run_clean(&composed_prefix(0, 1, HOLD_SERVICES, &steps).unwrap());
    let skipped: Vec<&str> = report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::ActSkipped { act, .. } => Some(act.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(skipped, ["expired-relay"]);
    let sent = sent(&report);
    assert!(
        sent.iter().any(|(row, _)| row == "get-services"),
        "{sent:?}"
    );
    assert!(received(&report).contains(&u16::from(MSG_DISCOVERY_SERVICES)));
}

/// A peer's request, as a script step.
fn ask(request: Request) -> PeerAction {
    PeerAction::Ask { request }
}

/// A discovery act, as a script step.
fn act(act: DiscoveryAct) -> PeerAction {
    PeerAction::Discover { act }
}

/// A run of requests on `row`, as a script step.
fn request_run(row: DiscoveryRow) -> PeerAction {
    act(DiscoveryAct::RequestRun { row })
}

/// A peer that holds its `Peers` tries an over-owed `Peers` against the
/// node's default `GetPeers` limit, which already owes the row's largest
/// payload, so the act fails and the peer skips it. The failed act puts
/// back the held `Peers` it took, and the peer still sends it when the hold
/// passes.
#[test]
fn a_skipped_act_keeps_the_held_answer() {
    let over_owed = act(DiscoveryAct::OverOwed {
        row: DiscoveryRow::Peers,
    });
    // Rule-breaking acts allowed; `Peers` held until 500 ms.
    let report = run_clean(&composed_prefix(0, 0x80, 0b0000_0010, &[over_owed]).unwrap());
    let skipped = report.log.entries().iter().any(
        |entry| matches!(&entry.obs, Observation::ActSkipped { act, .. } if act == "over-owed"),
    );
    assert!(skipped, "the over-owed act is skipped");
    let sent = sent(&report);
    assert!(
        sent.iter()
            .any(|(row, label)| row == "peers" && label == "conformant"),
        "the held Peers still goes out: {sent:?}"
    );
}

/// The input of a run the generator found: the prefix [`composed_prefix`]
/// encodes from bytes `node`, `script` and `holds` and the peer's `steps`,
/// then the Plan bytes the generator drew, which pick the order the run's
/// tasks take.
fn found(node: u8, script: u8, holds: u8, steps: &[PeerAction], plan: &[u8]) -> Vec<u8> {
    let mut input = composed_prefix(node, script, holds, steps)
        .unwrap()
        .to_vec();
    input.extend_from_slice(plan);
    input
}

/// A conformant script that interleaves request runs and `GetServices`
/// asks, found by the generator (seed 3203, run 337). Its eleventh
/// `GetServices` leaves 76 and 77 unanswered by the peer's count, so the
/// peer's model labels it `OverMargin`, and the evidence settles it
/// `Conformant` by the node's count, which had already answered both. The
/// model must still count it open: before it did, the model labelled the
/// third request after it `Conformant` with three open on the node, which
/// the node rightly refused, and `conformant-never-closes` failed.
#[test]
fn a_crossed_request_counts_toward_the_margin() {
    // One verification slot, production's `GetPeers` limit and 50 to 400 ms
    // links. The peer dials, relays no records and holds its `Peers` until
    // 2.5 s into the run.
    let steps = [
        request_run(DiscoveryRow::GetServices),
        ask(Request::GetServices),
        act(DiscoveryAct::HelloBurst),
        request_run(DiscoveryRow::GetPeers),
        ask(Request::GetServices),
        ask(Request::GetHeaderSyncService),
        act(DiscoveryAct::HelloBurst),
        act(DiscoveryAct::HelloBurst),
        ask(Request::GetOnePeer),
        request_run(DiscoveryRow::GetServices),
        ask(Request::GetServices),
        request_run(DiscoveryRow::GetServices),
        ask(Request::GetHeaderSyncService),
    ];
    const PLAN: [u8; 66] = [
        0x54, 0x41, 0x0a, 0x38, 0xd3, 0x08, 0x09, 0x4a, 0x02, 0xef, 0x19, 0x30, 0xb0, 0xf2, 0x49,
        0x4d, 0x72, 0xdc, 0xcf, 0x31, 0xd5, 0xd6, 0xfe, 0x00, 0x83, 0xa1, 0x78, 0x02, 0xa6, 0xd9,
        0xf7, 0x16, 0x2e, 0xfe, 0x19, 0x17, 0x82, 0x63, 0x13, 0x00, 0x65, 0xf2, 0xa9, 0x07, 0x82,
        0x95, 0x3e, 0xae, 0xf6, 0xff, 0x07, 0x21, 0xd8, 0xe2, 0x65, 0xef, 0xdc, 0x38, 0x65, 0x4b,
        0x40, 0x88, 0x10, 0x1b, 0x63, 0xd7,
    ];
    let report = run_clean(&found(0x40, 0x20, 0xb2, &steps, &PLAN));
    let sent = sent(&report);
    assert!(
        sent.iter()
            .any(|(row, label)| row == "get-services" && label == "over-margin"),
        "the refused request is judged over the margin: {sent:?}"
    );
}

/// A script of request runs and asks, found by the generator (seed 4203,
/// run 416), whose last `GetPeers` reaches the evidence only after the node
/// tore its connection down. The evidence logs it unjudged, as the teardown
/// left the others. Before it did, it held the request back for a count
/// that never came, and the run failed `WithheldFrames`.
#[test]
fn a_request_placed_after_its_teardown_is_logged() {
    // One verification slot, a `GetPeers` limit of 8 and 1 to 1,000 ms
    // links. The node dials the peer, which relays one record and holds its
    // `Hello` and `Peers` until 4 s into the run.
    let steps = [
        act(DiscoveryAct::ExpiredHello),
        request_run(DiscoveryRow::GetPeers),
        ask(Request::GetPeers),
        request_run(DiscoveryRow::GetPeers),
        ask(Request::GetOnePeer),
        act(DiscoveryAct::ExpiredRelay),
        ask(Request::GetServices),
        request_run(DiscoveryRow::GetPeers),
        ask(Request::GetOnePeer),
        ask(Request::GetHeaderSyncService),
        ask(Request::GetPeers),
        ask(Request::GetPeers),
        ask(Request::GetOnePeer),
    ];
    const PLAN: [u8; 180] = [
        0x28, 0x1c, 0x56, 0xb0, 0x96, 0x3c, 0xb1, 0x68, 0xa0, 0x44, 0x19, 0x73, 0xbd, 0x13, 0xa5,
        0x71, 0x8c, 0x05, 0x1d, 0xe3, 0x4e, 0xd2, 0x90, 0x87, 0xf6, 0xc1, 0x4e, 0x0b, 0xfd, 0x5c,
        0x31, 0xbe, 0x86, 0x79, 0xbd, 0x32, 0x01, 0x92, 0x79, 0x31, 0x95, 0xb3, 0xf0, 0xf1, 0x4c,
        0xab, 0x61, 0xc6, 0xb8, 0x7c, 0x8b, 0xfd, 0x7a, 0x44, 0xc8, 0xbe, 0xe8, 0x12, 0x1e, 0x4a,
        0xe1, 0x7e, 0x3c, 0xad, 0x97, 0x66, 0x1d, 0xc7, 0x2d, 0xbb, 0xa0, 0xcd, 0x43, 0x04, 0x52,
        0x99, 0x6b, 0xdf, 0x5c, 0xf6, 0x79, 0x90, 0x33, 0x03, 0x41, 0x60, 0x19, 0x56, 0xe5, 0x3c,
        0x22, 0xd3, 0x78, 0xa8, 0xc3, 0xb0, 0x05, 0x08, 0x9f, 0x4e, 0xc2, 0xd1, 0x70, 0x97, 0x6f,
        0x9b, 0x8f, 0x8b, 0x79, 0x1a, 0x04, 0x81, 0x55, 0x0f, 0xf2, 0xba, 0x08, 0xbf, 0x2a, 0xf7,
        0x6f, 0xa2, 0xa9, 0xc5, 0x96, 0x0f, 0xd3, 0x6d, 0xb0, 0x7f, 0x49, 0xcb, 0x3f, 0x1f, 0x3d,
        0xee, 0xa2, 0x89, 0xb3, 0x2d, 0x74, 0xa0, 0xa2, 0x32, 0x7d, 0x62, 0xc5, 0x03, 0x99, 0xb6,
        0xf3, 0xc0, 0xc2, 0x31, 0xc2, 0x44, 0x71, 0xf6, 0x8f, 0x07, 0x98, 0x74, 0x3a, 0x02, 0x67,
        0x5c, 0xd9, 0x1f, 0x89, 0xe7, 0x18, 0x91, 0x69, 0x0a, 0x03, 0xb9, 0x82, 0x9c, 0x61, 0x2e,
    ];
    run_clean(&found(0x7c, 0x3d, 0x3b, &steps, &PLAN));
}
