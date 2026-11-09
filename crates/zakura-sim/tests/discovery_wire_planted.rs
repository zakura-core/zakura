//! disc-3.3: the planted cases of discovery's stateless `<label>-closes`
//! IDs, whose guard is the shared frame reader. Each case is a typed case
//! under `qa/planted/<id>/`: a patch that makes the reader skip one check
//! for one row of discovery's table and discovery drop that frame, and the
//! scenario of the act that breaks that check, under the default Plan.
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
    model::Label,
    peer::discovery::{DiscoveryAct, DiscoveryRow},
};

/// The rows of discovery's table.
const ROWS: [DiscoveryRow; 5] = [
    DiscoveryRow::Hello,
    DiscoveryRow::GetPeers,
    DiscoveryRow::Peers,
    DiscoveryRow::GetServices,
    DiscoveryRow::Services,
];

/// The harness every case runs on.
const HARNESS: &str = "discovery/wire";

/// The act of each planted case: an undeclared type, then each row's
/// reserved flag, short payload and long payload, as the witnesses in
/// `discovery_wire_witness.rs` send them.
fn acts() -> Vec<DiscoveryAct> {
    let mut acts = vec![DiscoveryAct::UndeclaredType { message_type: 0 }];
    for row in ROWS {
        acts.extend([
            DiscoveryAct::ReservedFlags { row, bit: 0 },
            DiscoveryAct::ShortPayload { row },
            DiscoveryAct::LongPayload { row },
        ]);
    }
    acts
}

/// The ID `act` violates once its check is bypassed.
fn id(act: DiscoveryAct) -> String {
    match act.label() {
        Label::UndeclaredType => format!("{FAMILY}/s4v2/undeclared-type-closes"),
        label => format!("{FAMILY}/s4v2/{}/{}-closes", act.row().name(), label.name()),
    }
}

/// The repository root.
fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// `act`'s case files without their extension, relative to the
/// repository root: `qa/planted/<id>/drop-<label>`.
fn case(act: DiscoveryAct) -> String {
    format!("qa/planted/{}/drop-{}", id(act), act.label().name())
}

/// Every ID with a planted case has one case here, and each case runs its
/// act's scenario, [`act_scenario`], under the default Plan, the empty Plan
/// bytes. Its case file names the harness, the scenario and the Plan bytes.
///
/// This fails if an act's scenario changes or stops parsing; rerun
/// `regenerate_planted_reader_scenarios` then.
#[test]
fn each_planted_reader_case_runs_its_act() {
    let planted: Vec<String> = DiscoveryWire::properties()
        .into_iter()
        .map(|property| property.id.to_string())
        .filter(|id| has_planted_case(id))
        .collect();
    let ids: Vec<String> = acts().into_iter().map(id).collect();
    assert_eq!(ids, planted);
    let root = root();
    for act in acts() {
        let case = case(act);
        assert!(
            root.join(format!("{case}.patch")).is_file(),
            "{case}: no patch"
        );
        let file: toml::Table =
            toml::from_str(&fs::read_to_string(root.join(format!("{case}.toml"))).unwrap())
                .unwrap_or_else(|error| panic!("{case}: {error}"));
        let typed = file["typed"].as_table().expect("a typed case");
        let scenario = format!("{case}.scenario.json");
        let plan = format!("{case}.plan.bin");
        assert_eq!(typed["harness"].as_str(), Some(HARNESS), "{case}");
        assert_eq!(typed["scenario"].as_str(), Some(scenario.as_str()));
        assert_eq!(typed["plan"].as_str(), Some(plan.as_str()));
        let committed: WireScenario =
            serde_json::from_str(&fs::read_to_string(root.join(&scenario)).unwrap())
                .unwrap_or_else(|error| panic!("{scenario}: {error}"));
        assert_eq!(committed, act_scenario(act), "{scenario}");
        assert_eq!(fs::read(root.join(&plan)).unwrap(), Vec::<u8>::new());
    }
}

/// Rewrites each case's scenario from its act, and its empty Plan bytes.
/// Run it with `cargo test -p zakura-sim --test discovery_wire_planted --
/// --ignored regenerate_planted_reader_scenarios`.
#[test]
#[ignore = "a tool, not a test: it rewrites the planted cases' scenarios"]
fn regenerate_planted_reader_scenarios() {
    let root = root();
    for act in acts() {
        let case = case(act);
        let scenario = serde_json::to_string(&act_scenario(act)).unwrap();
        fs::write(root.join(format!("{case}.scenario.json")), scenario + "\n").unwrap();
        fs::write(root.join(format!("{case}.plan.bin")), []).unwrap();
    }
}
