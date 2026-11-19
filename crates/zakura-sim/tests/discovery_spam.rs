//! `discovery/spam`: spammers that act without a rate limit and dial again
//! after every close, beside an honest peer. The node refuses each hostile
//! connection while the honest peer's requests are still answered, and the
//! run judges no property that assumes a bounded adversary.
#![cfg(zakura_sim)]

use std::collections::BTreeSet;

use zakura_network::zakura::MSG_DISCOVERY_PEERS;
use zakura_sim::{
    explore::{classify, run_bytes, ExploreOutcome},
    harness::{
        self,
        discovery_service::{spam_prefix, DiscoverySpam, WireScenario},
        Subsystem,
    },
    peer::{
        discovery::{DiscoveryAct, Request},
        PeerAction,
    },
    property::Assume,
    seed, NullSink, Observation, RunReport,
};

/// Runs `prefix` on `discovery/spam` under the all-defaults Plan, and
/// returns the report once the run classifies clean.
fn run_clean(prefix: &[u8]) -> RunReport {
    let entry = harness::find("discovery/spam").unwrap();
    let run = run_bytes::<DiscoverySpam>(prefix, &mut NullSink, None);
    let classified = classify(entry, &run);
    assert_eq!(classified.outcome, ExploreOutcome::Clean, "{classified:?}");
    run.result.unwrap()
}

/// A prefix decodes to its spammers and honest peer, a prefix with another
/// version byte draws the default scenario, and a spammer's scenario
/// declares no `BoundedAdversary`.
#[test]
fn a_prefix_round_trips_its_peers() {
    let steps = [
        PeerAction::Discover {
            act: DiscoveryAct::UndeclaredType { message_type: 0 },
        },
        PeerAction::Disconnect,
    ];
    let mut prefix = spam_prefix(0, 2, &[Request::GetPeers], &steps).unwrap();
    let scenario = DiscoverySpam::draw(&prefix);
    assert_eq!(scenario.peers.len(), 3);
    for spammer in &scenario.peers[..2] {
        assert!(spammer.spams);
        assert_eq!(spammer.script, steps);
    }
    let honest = &scenario.peers[2];
    assert!(!honest.spams);
    assert_eq!(
        honest.script,
        [PeerAction::Ask {
            request: Request::GetPeers
        }]
    );
    assert!(!DiscoverySpam::assumptions(&scenario).declares(Assume::BoundedAdversary));
    prefix[0] ^= 0xff;
    assert!(!DiscoverySpam::accepts(&prefix));
    assert_eq!(DiscoverySpam::draw(&prefix), WireScenario::default());
    assert!(spam_prefix(0, 4, &[], &steps).is_err());
}

/// The connections the spammers sent a rule-breaking frame on, the
/// node's closes for a frame it refused, and the message types the peers
/// read from the node.
fn summary(report: &RunReport) -> (BTreeSet<u64>, usize, Vec<u16>) {
    let entries = report.log.entries();
    let hostile = entries
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::PeerFrame {
                connection, label, ..
            } if label.name() != "conformant" => Some(*connection),
            _ => None,
        })
        .collect();
    let refusals = entries
        .iter()
        .filter(|entry| {
            matches!(&entry.obs, Observation::ConnectionClosed { cause, .. }
                if cause == "ordered_read_rejected")
        })
        .count();
    let answers = entries
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::PeerReceived { message_type, .. } => Some(*message_type),
            _ => None,
        })
        .collect();
    (hostile, refusals, answers)
}

/// A spammer sends two undeclared message types back to back, disconnects,
/// and sends two more on its next connection. With no rate limit, each
/// pair goes out as soon as its connection is up, so rule-breaking frames
/// reach the node on two connections, and the node refuses the connection
/// for one at least. The honest peer's `GetPeers`, the only one asked, is
/// still answered, and nothing it sent closed it.
#[test]
fn a_spammer_keeps_going_after_its_connection_ends() {
    let undeclared = PeerAction::Discover {
        act: DiscoveryAct::UndeclaredType { message_type: 0 },
    };
    let steps = [
        undeclared,
        undeclared,
        PeerAction::Disconnect,
        undeclared,
        undeclared,
    ];
    let prefix = spam_prefix(0, 1, &[Request::GetPeers], &steps).unwrap();
    let (hostile, refusals, answers) = summary(&run_clean(&prefix));
    assert!(hostile.len() >= 2, "rule-breaking frames on {hostile:?}");
    assert!(refusals >= 1, "{refusals} refusals");
    assert!(
        answers.contains(&u16::from(MSG_DISCOVERY_PEERS)),
        "{answers:?}"
    );
}

/// A spammer's `GetServices` with bad wanted services, sent past the margin,
/// found by the generator (seed 4106, run 302). The node decodes a request
/// before its serving counts it, so it refuses this one at decode, with no
/// count. The peer's model labels it `BadContent`, which justifies the
/// close. Before, it labelled it `OverMargin`, which the evidence settles
/// only by the node's count, so the justified close failed
/// `conformant-never-closes`.
#[test]
fn a_request_with_bad_content_is_refused_before_its_margin() {
    let seed =
        seed::find("discovery-spam-content-before-margin").expect("the found input is a seed");
    run_clean(seed.input());
}
