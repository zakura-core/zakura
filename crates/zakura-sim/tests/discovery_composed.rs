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
        discovery::{DiscoveryAct, Request},
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
    const CROSSED: [u8; 83] = [
        0x01, 0x40, 0x20, 0xb2, 0xfa, 0xad, 0xb0, 0xf9, 0x92, 0x28, 0xb0, 0xc2, 0x30, 0xcd, 0x80,
        0x46, 0xb8, 0x54, 0x41, 0x0a, 0x38, 0xd3, 0x08, 0x09, 0x4a, 0x02, 0xef, 0x19, 0x30, 0xb0,
        0xf2, 0x49, 0x4d, 0x72, 0xdc, 0xcf, 0x31, 0xd5, 0xd6, 0xfe, 0x00, 0x83, 0xa1, 0x78, 0x02,
        0xa6, 0xd9, 0xf7, 0x16, 0x2e, 0xfe, 0x19, 0x17, 0x82, 0x63, 0x13, 0x00, 0x65, 0xf2, 0xa9,
        0x07, 0x82, 0x95, 0x3e, 0xae, 0xf6, 0xff, 0x07, 0x21, 0xd8, 0xe2, 0x65, 0xef, 0xdc, 0x38,
        0x65, 0x4b, 0x40, 0x88, 0x10, 0x1b, 0x63, 0xd7,
    ];
    let report = run_clean(&CROSSED);
    let sent = sent(&report);
    assert!(
        sent.iter()
            .any(|(row, label)| row == "get-services" && label == "over-margin"),
        "the refused request is judged over the margin: {sent:?}"
    );
}

/// A request run, found by the generator (seed 4203, run 416), whose last
/// `GetPeers` reaches the evidence only after the node tore its connection
/// down. The evidence logs it unjudged, as the teardown left the others.
/// Before it did, it held the request back for a count that never came,
/// and the run failed `WithheldFrames`.
#[test]
fn a_request_placed_after_its_teardown_is_logged() {
    run_clean(include_bytes!(
        "fixtures/discovery-composed-late-request.bin"
    ));
}
