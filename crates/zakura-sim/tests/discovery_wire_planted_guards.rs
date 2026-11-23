//! disc-3.2: the planted cases of the discovery IDs whose guard is
//! discovery's own. Each case is a typed case under `qa/planted/<id>/` in the
//! QA store, outside the repository: a
//! patch that bypasses one of discovery's guards for one row, or for a
//! `cadence-never-closes` ID closes a peer that empties the row's cadence
//! bucket, and the scenario that reaches that code, under the default Plan.
//! A receiver ID's
//! scenario is its act's witness ([`act_scenario`]). A sender ID's scenario
//! is the honest exchange, with the peer asking once for an answer the node
//! writes, or holding the answer to a request the node repeats past the
//! node's exchange wait.
#![cfg(zakura_sim)]

use std::{fs, path::PathBuf};

use zakura_sim::{
    harness::{
        discovery_service::{
            act_scenario, asking_scenario, has_planted_case, DiscoveryWire, WireScenario, FAMILY,
        },
        Subsystem,
    },
    peer::discovery::{Answer, DiscoveryAct, DiscoveryRow, DiscoveryRule, Hold, Request},
};

/// When a sender case's held answer goes, in milliseconds past the run's
/// start: past the node's 2 s initial exchange wait, so the node closes the
/// exchange as timed out before the peer answers a request it repeated.
const PAST_EXCHANGE_MS: u64 = 3_000;

/// How a case's scenario comes about.
#[derive(Clone, Copy, Debug)]
enum Scenario {
    /// The act's witness.
    Act(DiscoveryAct),
    /// The honest exchange.
    Honest,
    /// The honest exchange, with the peer holding `answer` past the node's
    /// exchange wait.
    Held(Answer),
    /// The honest exchange, with the peer sending `request` once
    /// ([`asking_scenario`]).
    Asks(Request),
}

impl Scenario {
    /// The scenario.
    fn build(self) -> WireScenario {
        match self {
            Scenario::Act(act) => act_scenario(act),
            Scenario::Honest => WireScenario::default(),
            Scenario::Held(answer) => {
                let mut scenario = WireScenario::default();
                scenario.peers[0]
                    .discovery
                    .as_mut()
                    .expect("the honest peer runs a discovery script")
                    .holds
                    .push(Hold {
                        answer,
                        until_ms: PAST_EXCHANGE_MS,
                    });
                scenario
            }
            Scenario::Asks(request) => asking_scenario(request),
        }
    }
}

/// Each planted case: its ID's suffix, its name, and its scenario.
fn cases() -> Vec<(&'static str, &'static str, Scenario)> {
    use DiscoveryRow::{Peers, Services};
    let act = Scenario::Act;
    let bad = |rule| Scenario::Act(DiscoveryAct::BadContent { rule });
    vec![
        (
            "conformant-never-closes",
            "close-on-expired-hello",
            act(DiscoveryAct::ExpiredHello),
        ),
        (
            "hello/bad-content-closes",
            "drop-forged-hello",
            bad(DiscoveryRule::HelloSignature),
        ),
        (
            "peers/unsolicited-closes",
            "admit-unreserved-peers",
            act(DiscoveryAct::Unsolicited { row: Peers }),
        ),
        (
            "peers/over-owed-closes",
            "reserve-largest-peers",
            act(DiscoveryAct::OverOwed { row: Peers }),
        ),
        (
            "peers/bad-content-closes",
            "drop-forged-relay",
            bad(DiscoveryRule::PeersSignature),
        ),
        (
            "services/unsolicited-closes",
            "admit-unreserved-services",
            act(DiscoveryAct::Unsolicited { row: Services }),
        ),
        ("hello/sender-cadence", "repeat-hello", Scenario::Honest),
        (
            "get-peers/sender-within-limit",
            "ask-peers-twice",
            Scenario::Held(Answer::Peers),
        ),
        ("get-peers/sender-cadence", "re-ask-peers", Scenario::Honest),
        (
            "get-services/sender-within-limit",
            "ask-services-twice",
            Scenario::Held(Answer::Services),
        ),
        (
            "get-services/sender-cadence",
            "re-ask-services",
            Scenario::Honest,
        ),
        (
            "services/sender-within-owed",
            "ignore-wanted-services",
            Scenario::Asks(Request::GetHeaderSyncService),
        ),
    ]
}

/// The generated ID `discovery/s4v2/<suffix>`.
fn id(suffix: &str) -> String {
    format!("{FAMILY}/s4v2/{suffix}")
}

/// The local QA store, `$ZAKURA_QA_STORE`, which holds the planted cases
/// outside the repository. `scripts/qa_store.py pull` fills it from the
/// Space.
fn store() -> PathBuf {
    std::env::var_os("ZAKURA_QA_STORE")
        .filter(|root| !root.is_empty())
        .map(PathBuf::from)
        .expect("set ZAKURA_QA_STORE to the local QA store; `scripts/qa_store.py pull` fills it")
}

/// A case's files without their extension, relative to the QA store:
/// `qa/planted/<id>/<name>`.
fn case(suffix: &str, name: &str) -> String {
    format!("qa/planted/{}/{name}", id(suffix))
}

/// Each ID whose guard is discovery's own has a planted case, in
/// registration order.
#[test]
fn each_planted_guard_case_has_its_scenario() {
    let planted: Vec<String> = DiscoveryWire::properties()
        .into_iter()
        .map(|property| property.id.to_string())
        .filter(|id| has_planted_case(id))
        .collect();
    let ids: Vec<String> = cases().iter().map(|(suffix, ..)| id(suffix)).collect();
    let planted: Vec<&String> = planted.iter().filter(|id| ids.contains(id)).collect();
    assert_eq!(ids.iter().collect::<Vec<_>>(), planted);
}

/// Rewrites each case's scenario in the QA store. Run it with
/// `cargo test -p zakura-sim --test discovery_wire_planted_guards --
/// --ignored regenerate_planted_guard_scenarios`.
#[test]
#[ignore = "a tool, not a test: it rewrites the planted cases' scenarios"]
fn regenerate_planted_guard_scenarios() {
    let root = store();
    for (suffix, name, scenario) in cases() {
        let case = case(suffix, name);
        let json = serde_json::to_string(&scenario.build()).unwrap();
        fs::write(root.join(format!("{case}.scenario.json")), json + "\n").unwrap();
    }
}
