//! Handshake harness wiring, retained histories, and focused wire witnesses.
#![cfg(zakura_sim)]

use zakura_sim::{
    harness::{
        self, handshake_counts,
        handshake_wire::{Scenario, Wire},
        RecordInput, Subsystem,
    },
    peer::handshake::options::{ControlFault, HandshakeSpec},
    trace::{replay_with, ReplayOptions},
    NullSink, PlanInput, PropertyOutcome,
};

#[test]
fn counts_trace_replays_with_each_named_rule() {
    let input = [0, 0, 0, 2, 0, 0, 3, 0, 0, 5, 0, 0];
    let dir = tempfile::TempDir::new().unwrap();
    let trace = harness::record(
        &handshake_counts::ENTRY,
        RecordInput::Input(&input),
        dir.path(),
    )
    .unwrap();
    assert_eq!(trace.meta.properties.len(), 4);
    replay_with(&trace, &handshake_counts::ENTRY, ReplayOptions::default()).unwrap();
}

fn run(scenario: Scenario) -> zakura_sim::RunReport {
    let entry = harness::find(Wire::NAME).unwrap();
    let report = (entry.run)(
        &serde_json::to_value(scenario).unwrap(),
        PlanInput::Bytes(Vec::new()),
        &mut NullSink,
    )
    .unwrap();
    assert!(report.end.harness.is_none(), "{report:?}");
    assert!(report.end.violation.is_none(), "{report:?}");
    report
}

#[test]
fn honest_control_handshake_holds() {
    let report = run(Scenario::default());
    assert_held(&report, "handshake/hello/conformant-never-refused");
}

#[test]
fn wrong_network_is_refused_and_releases_its_slot() {
    let mut scenario = Scenario::default();
    scenario.peers[0].handshake = Some(HandshakeSpec {
        control: ControlFault::Network,
        ..HandshakeSpec::default()
    });
    let report = run(scenario);
    assert_held(&report, "handshake/hello/wrong-network-refused");
    assert_held(&report, "handshake/hello/refusal-releases-slot");
}

fn assert_held(report: &zakura_sim::RunReport, id: &str) {
    let property = report
        .properties
        .iter()
        .find(|property| property.property == id)
        .unwrap();
    assert!(
        matches!(property.outcome, PropertyOutcome::Held { applicable } if applicable > 0),
        "{property:?}"
    );
}
