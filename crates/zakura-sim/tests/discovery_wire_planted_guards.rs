//! disc-3.2: the planted cases of the discovery IDs whose guard is
//! discovery's own. Each case is a typed case under `qa/planted/<id>/`: a
//! patch that bypasses one of discovery's guards for one row, and the
//! scenario that reaches that guard, under the default Plan. A receiver ID's
//! scenario is its act's witness ([`act_scenario`]). A sender ID's scenario
//! is the honest exchange, with the peer asking once for an answer the node
//! writes, or holding the answer to a request the node repeats past the
//! node's exchange wait.
#![cfg(zakura_sim)]

use std::{
    fs,
    path::{Path, PathBuf},
};

use zakura_sim::{
    harness::{
        discovery_service::{act_scenario, has_planted_case, DiscoveryWire, WireScenario, FAMILY},
        Subsystem,
    },
    peer::{
        discovery::{Answer, DiscoveryAct, DiscoveryRow, DiscoveryRule, Hold, Request},
        PeerAction,
    },
};

/// The harness every case runs on.
const HARNESS: &str = "discovery/wire";

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
    /// The honest exchange, with the peer sending `request` once.
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
            Scenario::Asks(request) => {
                let mut scenario = WireScenario::default();
                scenario.peers[0].script.push(PeerAction::Ask { request });
                scenario
            }
        }
    }
}

/// Each planted case: its ID's suffix, its name, and its scenario.
fn cases() -> Vec<(&'static str, &'static str, Scenario)> {
    use DiscoveryRow::{GetPeers, GetServices, Peers, Services};
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
            "get-peers/over-margin-closes",
            "admit-past-margin",
            act(DiscoveryAct::OverMargin { row: GetPeers }),
        ),
        (
            "get-peers/bad-content-closes",
            "drop-refused-get-peers",
            bad(DiscoveryRule::GetPeersLimit),
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
            "get-services/over-margin-closes",
            "admit-past-margin",
            act(DiscoveryAct::OverMargin { row: GetServices }),
        ),
        (
            "get-services/bad-content-closes",
            "drop-refused-get-services",
            bad(DiscoveryRule::GetServicesWanted),
        ),
        (
            "services/unsolicited-closes",
            "admit-unreserved-services",
            act(DiscoveryAct::Unsolicited { row: Services }),
        ),
        (
            "services/bad-content-closes",
            "skip-services-node-id",
            bad(DiscoveryRule::ServicesNodeId),
        ),
        ("hello/sender-cadence", "repeat-hello", Scenario::Honest),
        (
            "get-peers/sender-within-limit",
            "ask-peers-twice",
            Scenario::Held(Answer::Peers),
        ),
        ("get-peers/sender-cadence", "re-ask-peers", Scenario::Honest),
        (
            "peers/sender-ends-once",
            "answer-peers-twice",
            Scenario::Asks(Request::GetPeers),
        ),
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
            "services/sender-ends-once",
            "answer-services-twice",
            Scenario::Asks(Request::GetServices),
        ),
    ]
}

/// The generated ID `discovery/s4v2/<suffix>`.
fn id(suffix: &str) -> String {
    format!("{FAMILY}/s4v2/{suffix}")
}

/// The repository root.
fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// A case's files without their extension, relative to the repository
/// root: `qa/planted/<id>/<name>`.
fn case(suffix: &str, name: &str) -> String {
    format!("qa/planted/{}/{name}", id(suffix))
}

/// Every ID with a planted case has one case here, in registration order,
/// and each case runs its scenario under the default Plan, the empty Plan
/// bytes. Its case file names the harness, the scenario and the Plan bytes.
///
/// This fails if a case's scenario changes or stops parsing; rerun
/// `regenerate_planted_guard_scenarios` then.
#[test]
fn each_planted_guard_case_runs_its_scenario() {
    let planted: Vec<String> = DiscoveryWire::properties()
        .into_iter()
        .map(|property| property.id.to_string())
        .filter(|id| has_planted_case(id))
        .collect();
    let ids: Vec<String> = cases().iter().map(|(suffix, ..)| id(suffix)).collect();
    assert_eq!(ids, planted);
    let root = root();
    for (suffix, name, scenario) in cases() {
        let case = case(suffix, name);
        assert!(
            root.join(format!("{case}.patch")).is_file(),
            "{case}: no patch"
        );
        let file: toml::Table =
            toml::from_str(&fs::read_to_string(root.join(format!("{case}.toml"))).unwrap())
                .unwrap_or_else(|error| panic!("{case}: {error}"));
        let typed = file["typed"].as_table().expect("a typed case");
        let scenario_file = format!("{case}.scenario.json");
        let plan = format!("{case}.plan.bin");
        assert_eq!(typed["harness"].as_str(), Some(HARNESS), "{case}");
        assert_eq!(typed["scenario"].as_str(), Some(scenario_file.as_str()));
        assert_eq!(typed["plan"].as_str(), Some(plan.as_str()));
        let committed: WireScenario =
            serde_json::from_str(&fs::read_to_string(root.join(&scenario_file)).unwrap())
                .unwrap_or_else(|error| panic!("{scenario_file}: {error}"));
        assert_eq!(committed, scenario.build(), "{scenario_file}");
        assert_eq!(fs::read(root.join(&plan)).unwrap(), Vec::<u8>::new());
    }
}

/// Rewrites each case's scenario and its empty Plan bytes. Run it with
/// `cargo test -p zakura-sim --test discovery_wire_planted_guards --
/// --ignored regenerate_planted_guard_scenarios`.
#[test]
#[ignore = "a tool, not a test: it rewrites the planted cases' scenarios"]
fn regenerate_planted_guard_scenarios() {
    let root = root();
    for (suffix, name, scenario) in cases() {
        let case = case(suffix, name);
        let json = serde_json::to_string(&scenario.build()).unwrap();
        fs::write(root.join(format!("{case}.scenario.json")), json + "\n").unwrap();
        fs::write(root.join(format!("{case}.plan.bin")), []).unwrap();
    }
}
