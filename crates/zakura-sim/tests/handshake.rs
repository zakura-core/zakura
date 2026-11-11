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
    assert!(report.end.harness.is_none(), "{:?}", report.end);
    assert!(report.end.violation.is_none(), "{:?}", report.end);
    report
}

#[test]
fn honest_control_handshake_holds() {
    let report = run(Scenario::default());
    assert_held(&report, "handshake/hello/conformant-never-refused");
    assert_held(&report, "handshake/finishes-in-time");
    for id in [
        "handshake/pending-bounded",
        "handshake/connections-per-address-bounded",
        "handshake/counts-match-permits",
    ] {
        assert_held(&report, id);
    }
}

#[test]
fn every_family_draws_a_conformant_control_exchange() {
    for family in 0..7 {
        let mut input = [0; Wire::SCENARIO_BYTES];
        input[0] = family;
        let report = run(Wire::draw(&input));
        assert_held(
            &report,
            if family == 1 {
                "handshake/ack/conformant-never-refused"
            } else {
                "handshake/hello/conformant-never-refused"
            },
        );
        assert!(
            report
                .properties
                .iter()
                .all(|property| !matches!(property.outcome, PropertyOutcome::Violated { .. })),
            "family {family} must have no violations"
        );
    }
}

#[test]
fn shared_addresses_hold_pending_and_permit_bounds() {
    for family in 0..3 {
        let mut input = [0; Wire::SCENARIO_BYTES];
        input[0] = 4;
        input[1] = family;
        input[2] = 225;
        input[3] = 15;
        let report = run(Wire::draw(&input));
        for id in [
            "handshake/pending-bounded",
            "handshake/connections-per-address-bounded",
            "handshake/counts-match-permits",
        ] {
            assert_held(&report, id);
        }
        assert!(report.log.entries().iter().any(|entry| matches!(&entry.obs,
            zakura_sim::Observation::Transport { event: zakura_sim::observe::TransportFact::HandshakeCounts { hosts, quic_hosts, .. }, .. }
                if hosts.iter().any(|(_, _, pending)| *pending >= 3) || quic_hosts.iter().any(|(_, pending)| *pending >= 3)
        )), "family {family}: shared-address load must reach the address bound; counts: {:?}", report.log.entries().iter().filter_map(|entry| match &entry.obs { zakura_sim::Observation::Transport { event: event @ zakura_sim::observe::TransportFact::HandshakeCounts { .. }, .. } => Some(event), _ => None }).collect::<Vec<_>>());
    }
}

#[test]
fn slow_control_steps_exceed_the_overall_deadline() {
    let mut scenario = Scenario::default();
    scenario.peers[0].handshake = Some(HandshakeSpec {
        length_delay_ms: 9_000,
        payload_delay_ms: 9_000,
        ..HandshakeSpec::default()
    });
    let entry = harness::find(Wire::NAME).unwrap();
    let report = (entry.run)(
        &serde_json::to_value(scenario).unwrap(),
        PlanInput::Bytes(Vec::new()),
        &mut NullSink,
    )
    .unwrap();
    assert!(report.end.harness.is_none(), "{:?}", report.end);
    zakura_sim::witness::judge_report::<Wire>(
        &report,
        "handshake/finishes-in-time",
        zakura_sim::witness::Expect::Violated,
    )
    .unwrap();
    let deadline = report
        .properties
        .iter()
        .find(|property| property.property == "handshake/finishes-in-time")
        .unwrap();
    assert!(
        matches!(deadline.outcome, PropertyOutcome::Violated { .. }),
        "{deadline:?}"
    );
    assert!(
        deadline
            .violations
            .iter()
            .any(|violation| violation.actual.ends_with("Some(18000) ms")),
        "{deadline:?}"
    );
}

#[test]
fn capability_flip_covers_inflight_handshake() {
    use zakura_sim::harness::handshake_wire::CapabilityFlip;
    for order in [
        CapabilityFlip::BeforeRegistration,
        CapabilityFlip::AfterRegistration,
        CapabilityFlip::TwiceBeforeRegistration,
    ] {
        let mut scenario = Scenario::default();
        scenario.capability_flip = Some(order);
        scenario.peers[0].handshake = Some(HandshakeSpec {
            dial_delay_ms: 1,
            ..HandshakeSpec::default()
        });
        let report = run(scenario);
        use zakura_sim::{observe::TransportFact, Observation};
        let expected = if order == CapabilityFlip::AfterRegistration {
            zakura_sim::witness::Expect::Holds
        } else {
            zakura_sim::witness::Expect::Violated
        };
        zakura_sim::witness::judge_report::<Wire>(
            &report,
            "session/renegotiation-covers-inflight-handshakes",
            expected,
        )
        .unwrap();
        let entries = report.log.entries();
        let registered = entries.iter().position(|entry| matches!(&entry.obs,
            Observation::Transport { event: TransportFact::Handshake { stage, .. }, .. } if stage == "registered"
        )).unwrap();
        let changes: Vec<_> = entries
            .iter()
            .enumerate()
            .filter_map(|(index, entry)| match &entry.obs {
                Observation::Transport {
                    event: TransportFact::CapabilitiesChanged { supported },
                    ..
                } if supported & zakura_network::zakura::ZAKURA_CAP_HEADER_SYNC != 0 => Some(index),
                _ => None,
            })
            .collect();
        assert_eq!(
            changes.len(),
            if order == CapabilityFlip::TwiceBeforeRegistration {
                2
            } else {
                1
            }
        );
        assert_eq!(
            registered < changes[0],
            order == CapabilityFlip::AfterRegistration
        );
    }
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

#[test]
fn hello_guards_refuse_each_corrupt_field() {
    for &fault in &zakura_sim::harness::handshake_wire::HELLO_FAULTS[1..] {
        let mut scenario = Scenario::default();
        scenario.peers[0].handshake = Some(HandshakeSpec {
            control: fault,
            ..HandshakeSpec::default()
        });
        let report = run(scenario);
        assert_held(
            &report,
            &format!("handshake/hello/{}-refused", fault.label()),
        );
    }
}

#[test]
fn ack_guards_refuse_each_corrupt_field() {
    for &fault in &zakura_sim::harness::handshake_wire::ACK_FAULTS[1..] {
        let mut scenario = Scenario::default();
        scenario.peers[0] = zakura_sim::peer::PeerSpec::listening();
        scenario.peers[0].handshake = Some(HandshakeSpec {
            control: fault,
            ..HandshakeSpec::default()
        });
        let report = run(scenario);
        assert_held(&report, &format!("handshake/ack/{}-refused", fault.label()));
    }
}

#[test]
fn malformed_prelude_resets_the_stream() {
    let mut scenario = Scenario::default();
    scenario.peers[0].handshake = Some(HandshakeSpec {
        prelude: zakura_sim::peer::handshake::options::PreludeFault::Magic,
        ..HandshakeSpec::default()
    });
    let report = run(scenario);
    assert_held(&report, "handshake/prelude/bad-magic-resets");
}

#[test]
fn prelude_guards_apply_each_declared_consequence() {
    use zakura_sim::harness::handshake_wire::PRELUDE_FAULTS;
    for index in 1..PRELUDE_FAULTS.len() {
        let mut input = [0; Wire::SCENARIO_BYTES];
        input[0] = 2;
        input[1] = u8::try_from(index).unwrap();
        let report = run(Wire::draw(&input));
        let label = PRELUDE_FAULTS[index].label();
        let effect = zakura_sim::table::handshake::PRELUDE
            .iter()
            .find(|(name, _)| *name == label)
            .unwrap()
            .1;
        assert_held(&report, &format!("handshake/prelude/{label}-{effect}"));
    }
}
