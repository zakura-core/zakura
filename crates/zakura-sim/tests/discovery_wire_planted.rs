//! disc-3.3: the planted cases of discovery's stateless `<label>-closes`
//! IDs, whose guard is the shared frame reader. Each case is a typed case
//! under `qa/planted/<id>/` in the QA store, outside the repository: a patch
//! that makes the reader skip one check for one row of discovery's table and
//! discovery drop that frame, and the scenario of the act that breaks that
//! check, under the default Plan.
#![cfg(zakura_sim)]

use std::{fs, path::PathBuf};

use zakura_sim::{
    harness::{
        discovery_service::{act_scenario, has_planted_case, DiscoveryWire, FAMILY},
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

/// The local QA store, `$ZAKURA_QA_STORE`, which holds the planted cases
/// outside the repository. `scripts/qa_store.py pull` fills it from the
/// Space.
fn store() -> PathBuf {
    std::env::var_os("ZAKURA_QA_STORE")
        .filter(|root| !root.is_empty())
        .map(PathBuf::from)
        .expect("set ZAKURA_QA_STORE to the local QA store; `scripts/qa_store.py pull` fills it")
}

/// `act`'s case files without their extension, relative to the QA store:
/// `qa/planted/<id>/drop-<label>`.
fn case(act: DiscoveryAct) -> String {
    format!("qa/planted/{}/drop-{}", id(act), act.label().name())
}

/// Each stateless ID has a planted case, in registration order, and each
/// act breaks its ID's check.
#[test]
fn each_planted_reader_case_has_its_act() {
    let planted: Vec<String> = DiscoveryWire::properties()
        .into_iter()
        .map(|property| property.id.to_string())
        .filter(|id| has_planted_case(id))
        .collect();
    let ids: Vec<String> = acts().into_iter().map(id).collect();
    let planted: Vec<&String> = planted.iter().filter(|id| ids.contains(id)).collect();
    assert_eq!(ids.iter().collect::<Vec<_>>(), planted);
}

/// Rewrites each case's scenario from its act, in the QA store.
/// Run it with `cargo test -p zakura-sim --test discovery_wire_planted --
/// --ignored regenerate_planted_reader_scenarios`.
#[test]
#[ignore = "a tool, not a test: it rewrites the planted cases' scenarios"]
fn regenerate_planted_reader_scenarios() {
    let root = store();
    for act in acts() {
        let case = case(act);
        let scenario = serde_json::to_string(&act_scenario(act)).unwrap();
        fs::write(root.join(format!("{case}.scenario.json")), scenario + "\n").unwrap();
    }
}
