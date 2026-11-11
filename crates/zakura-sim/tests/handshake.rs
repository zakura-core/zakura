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
    (handshake_counts::ENTRY.witness)(
        &(handshake_counts::ENTRY.draw)(&input),
        PlanInput::Bytes(Vec::new()),
        "handshake/counts-match-permits",
        zakura_sim::witness::Expect::Holds,
        &[],
        &mut NullSink,
    )
    .unwrap();
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
    assert_held(&report, "handshake/prelude/conformant-never-refused");
    assert_held(&report, "handshake/finishes-in-time");
    assert_held(&report, "session/installed-services-authorized");
    for id in [
        "handshake/pending-bounded",
        "handshake/connections-per-address-bounded",
        "handshake/counts-match-permits",
    ] {
        assert_held(&report, id);
    }
}

#[test]
fn conformant_control_and_prelude_are_accepted() {
    for family in [0, 1] {
        let mut input = [0; Wire::SCENARIO_BYTES];
        input[0] = family;
        let report = run(Wire::draw(&input));
        assert_witness_holds(
            &report,
            if family == 0 {
                "handshake/hello/conformant-never-refused"
            } else {
                "handshake/ack/conformant-never-refused"
            },
        );
        if family == 0 {
            assert_witness_holds(&report, "handshake/prelude/conformant-never-refused");
        }
    }
}

#[test]
fn every_family_draws_a_conformant_control_exchange() {
    for family in 0..10 {
        let mut input = [0; Wire::SCENARIO_BYTES];
        input[0] = family;
        let report = run(Wire::draw(&input));
        assert_held(
            &report,
            if family == 1 || family == 7 {
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
fn shared_address_registrations_stop_at_the_cap() {
    use zakura_sim::{observe::TransportFact, Observation};
    let mut input = [0; Wire::SCENARIO_BYTES];
    input[..5].copy_from_slice(&[4, 0, 0, 16, 0x80 | 10]);
    let report = run(Wire::draw(&input));
    assert_witness_holds(&report, "handshake/connections-per-address-bounded");
    assert_held(&report, "handshake/counts-match-permits");
    let accepted = report.log.entries().iter().filter(|entry| matches!(&entry.obs,
        Observation::Transport { event: TransportFact::Handshake { stage, error: None, .. }, .. }
            if stage == "hello"
    )).count();
    let registered = report
        .log
        .entries()
        .iter()
        .filter(|entry| {
            matches!(
                &entry.obs,
                Observation::Transport {
                    event: TransportFact::Registered,
                    ..
                }
            )
        })
        .count();
    assert_eq!(
        accepted, 17,
        "all control exchanges must reach registration admission"
    );
    assert_eq!(
        registered, 16,
        "the address cap must refuse the seventeenth registration"
    );
    assert!(
        report.log.entries().iter().any(|entry| matches!(&entry.obs,
            Observation::Transport { event: TransportFact::HandshakeCounts { hosts, .. }, .. }
                if hosts.iter().any(|(_, registered, _)| *registered == 16)
        )),
        "one address must hold sixteen registrations at once"
    );
}

fn assert_dial_under_load(report: &zakura_sim::RunReport, pending: usize, connections: usize) {
    use zakura_sim::{observe::TransportFact, Observation};
    assert_witness_holds(report, "handshake/dial-reserve-kept");
    let (dial_seq, dial) = report
        .log
        .entries()
        .iter()
        .find_map(|entry| match &entry.obs {
            Observation::Transport {
                seq,
                remote,
                event: TransportFact::DialStarted,
                ..
            } => Some((*seq, remote)),
            _ => None,
        })
        .expect("the node attempts its dial");
    let held = report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Transport {
                seq,
                event:
                    TransportFact::HandshakeCounts {
                        inbound,
                        inbound_connections,
                        ..
                    },
                ..
            } if *seq < dial_seq => Some((*inbound, *inbound_connections)),
            _ => None,
        })
        .last();
    assert_eq!(
        held,
        Some((pending, connections)),
        "inbound pressure must hold when the node dials; {:?}",
        report.end
    );
    assert!(report.log.entries().iter().any(|entry| matches!(&entry.obs,
        Observation::Transport { seq, remote, event: TransportFact::Registered, .. } if remote == dial && *seq > dial_seq
    )), "the reserved dial must register");
}

#[test]
fn inbound_control_pressure_preserves_a_wire_dial() {
    let mut input = [0; Wire::SCENARIO_BYTES];
    input[0] = 7;
    input[1] = 25;
    input[3] = 225;
    input[4] = 100;
    let report = run(Wire::draw(&input));
    assert_witness_holds(&report, "handshake/pending-bounded");
    assert_dial_under_load(&report, 24, 24);
}

#[test]
fn inbound_connection_pressure_preserves_a_wire_dial() {
    let mut input = [0; Wire::SCENARIO_BYTES];
    input[0] = 7;
    input[1] = 70;
    input[4] = 100;
    input[5] = 10;
    let report = run(Wire::draw(&input));
    assert_dial_under_load(&report, 0, 69);
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
    assert_witness_holds(&report, "handshake/hello/refusal-releases-slot");
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

fn assert_witness_holds(report: &zakura_sim::RunReport, id: &str) {
    zakura_sim::witness::judge_report::<Wire>(report, id, zakura_sim::witness::Expect::Holds)
        .unwrap_or_else(|error| panic!("{id}: {error}"));
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
        assert_witness_holds(
            &report,
            &format!("handshake/hello/{}-refused", fault.label()),
        );
        assert_witness_holds(&report, "handshake/hello/refusal-releases-slot");
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
        assert_witness_holds(&report, &format!("handshake/ack/{}-refused", fault.label()));
        assert_witness_holds(&report, "handshake/ack/refusal-releases-slot");
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
        assert_witness_holds(&report, &format!("handshake/prelude/{label}-{effect}"));
    }
}

#[test]
fn installed_services_follow_each_peers_negotiation() {
    use zakura_network::zakura::{ZAKURA_CAP_DISCOVERY, ZAKURA_CAP_HEADER_SYNC};
    use zakura_sim::peer::PeerSpec;
    for node_dials in [false, true] {
        let mut scenario = Scenario::default();
        scenario.peers = [
            ZAKURA_CAP_HEADER_SYNC,
            ZAKURA_CAP_HEADER_SYNC | ZAKURA_CAP_DISCOVERY,
            ZAKURA_CAP_HEADER_SYNC | (1 << 60) | (1 << 61),
        ]
        .into_iter()
        .map(|capabilities| PeerSpec {
            capabilities,
            ..if node_dials {
                PeerSpec::listening()
            } else {
                PeerSpec::dialing()
            }
        })
        .collect();
        let report = run(scenario);
        assert_held(&report, "session/installed-services-authorized");
        if node_dials {
            assert!(
                report.log.entries().iter().any(|entry| matches!(
                    entry.obs,
                    zakura_sim::Observation::Transport {
                        event: zakura_sim::observe::TransportFact::Admitted {
                            kind: 900,
                            version: 2,
                            ..
                        },
                        ..
                    }
                )),
                "the node must install the selected alternative layout"
            );
        }
        zakura_sim::witness::judge_report::<Wire>(
            &report,
            "session/installed-services-authorized",
            zakura_sim::witness::Expect::Holds,
        )
        .unwrap();
    }
}

#[test]
fn invalid_stream_churn_obeys_the_open_rate() {
    use zakura_sim::{observe::TransportFact, Observation};
    let report = run(Wire::draw(include_bytes!(
        "fixtures/handshake-stream-churn.bin"
    )));
    assert_held(&report, "session/stream-opens-bounded");
    zakura_sim::witness::judge_report::<Wire>(
        &report,
        "session/stream-opens-bounded",
        zakura_sim::witness::Expect::Holds,
    )
    .unwrap();
    assert!(
        report.log.entries().iter().any(|entry| matches!(
            entry.obs,
            Observation::Transport {
                event: TransportFact::TornDown,
                ..
            }
        )),
        "the churn witness must tear down and return its permits"
    );
    assert!(
        report
            .log
            .entries()
            .iter()
            .any(|entry| matches!(entry.obs, Observation::PeerStreamReset { code: 3, .. })),
        "the peer must receive a stream-open rate refusal"
    );
    let starts = report
        .log
        .entries()
        .iter()
        .filter(|entry| {
            matches!(
                entry.obs,
                Observation::Transport {
                    event: TransportFact::StreamSetupStarted { .. },
                    ..
                }
            )
        })
        .count();
    assert!(
        starts >= 32,
        "the hostile peer must consume the initial stream-open burst; got {starts}"
    );
}

#[test]
fn ownership_probe_counts_last_owners_and_setup_transfers() {
    use zakura_network::zakura::{
        testkit::regulation::SessionCapacityHandle, FreeSlots, ServicePeerDirection,
        ServicePeerLimits,
    };
    const INBOUND: ServicePeerDirection = ServicePeerDirection::Inbound;
    const OUTBOUND: ServicePeerDirection = ServicePeerDirection::Outbound;
    let capacity = SessionCapacityHandle::new(&ServicePeerLimits {
        max_inbound_peers: 2,
        max_outbound_peers: 1,
        max_pending_escalations: 2,
        ..ServicePeerLimits::default()
    });
    let probe = capacity.probe();
    let read = || zakura_rt::sim::boundary_read(|| probe.ownership().unwrap());
    let baseline = read().free;
    let inbound = capacity.reserve(INBOUND).unwrap();
    let retained = inbound.clone();
    let outbound = capacity.reserve(OUTBOUND).unwrap();
    assert!(capacity.reserve(INBOUND).is_err());
    assert_eq!(
        read().held,
        FreeSlots {
            inbound: 1,
            outbound: 1,
            setup: 2,
            inbound_setup: 1
        }
    );
    inbound.admitted();
    inbound.admitted();
    assert_eq!(
        read().held,
        FreeSlots {
            inbound: 1,
            outbound: 1,
            setup: 1,
            inbound_setup: 0
        }
    );
    drop(inbound);
    drop(capacity);
    assert_eq!(
        read().held.inbound,
        1,
        "the last owner retains the direction slot"
    );
    drop(retained);
    assert_eq!(read().held.inbound, 0);
    drop(outbound);
    assert_eq!(read().free, baseline);
    assert_eq!(
        read().held,
        FreeSlots {
            inbound: 0,
            outbound: 0,
            setup: 0,
            inbound_setup: 0
        }
    );
}

#[test]
fn partial_session_offers_release_setup_capacity() {
    use zakura_sim::{
        observe::{Observation, SessionStepFact, TransportFact},
        peer::{PeerAction, PeerSpec},
    };
    let mut peer = PeerSpec::dialing();
    peer.capabilities |= 1 << 59;
    peer.handshake = Some(HandshakeSpec::default());
    peer.script = vec![
        PeerAction::Offer {
            kind: 902,
            version: 1,
            wire_id: 1,
        },
        PeerAction::Close,
    ];
    let report = run(Scenario {
        peers: vec![peer; 3],
        ..Scenario::default()
    });
    for id in ["session/capacity-conserved", "session/setup-slot-bounded"] {
        assert_held(&report, id);
    }
    assert!(
        report.log.entries().iter().any(|entry| matches!(
            entry.obs,
            Observation::Transport {
                event: TransportFact::SessionLifecycle {
                    kind: 902,
                    step: SessionStepFact::OfferExpired { .. }
                },
                ..
            }
        )),
        "the production pending session must expire"
    );
    let last = report
        .log
        .entries()
        .iter()
        .rev()
        .find_map(|entry| match &entry.obs {
            Observation::Transport {
                event:
                    TransportFact::SessionCapacity {
                        free,
                        held,
                        capacity,
                        ..
                    },
                ..
            } => Some((free, held, capacity)),
            _ => None,
        })
        .unwrap();
    assert_eq!(last.0, last.2);
    assert_eq!(*last.1, [0; 4]);
    assert!(report.log.entries().iter().any(|entry| matches!(entry.obs,
        Observation::Transport { event: TransportFact::SessionCapacity { held, .. }, .. } if held[2] == 1
    )), "one inbound partial offer must reserve setup capacity");
}

#[test]
fn complete_session_transfers_setup_to_installed_owners() {
    use zakura_sim::{
        observe::{Observation, TransportFact},
        peer::PeerSpec,
    };
    let mut peer = PeerSpec::listening();
    peer.capabilities |= 1 << 59;
    peer.handshake = Some(HandshakeSpec::default());
    let scenario = Scenario {
        peers: vec![peer],
        ..Scenario::default()
    };
    let mut input = [0; Wire::SCENARIO_BYTES];
    input[..3].copy_from_slice(&[9, 1, 1]);
    assert_eq!(scenario, Wire::draw(&input));
    (harness::find(Wire::NAME).unwrap().witness)(
        &serde_json::to_value(&scenario).unwrap(),
        PlanInput::Bytes(Vec::new()),
        "session/capacity-conserved",
        zakura_sim::witness::Expect::Holds,
        &[],
        &mut NullSink,
    )
    .unwrap();
    let report = run(scenario);
    assert_held(&report, "session/capacity-conserved");
    assert!(
        report.log.entries().iter().any(|entry| matches!(
            entry.obs,
            Observation::Transport {
                event: TransportFact::SessionCapacity {
                    held: [0, 1, 0, 0],
                    ..
                },
                ..
            }
        )),
        "the installed outbound session must release its setup permit"
    );
}

#[test]
fn pending_transport_load_exposes_outbound_control_overcommit() {
    use zakura_sim::{
        harness::{LinkEntry, LinkSpec},
        peer::{PeerSpec, NODE_ADDR},
    };
    let mut scenario = Scenario::default();
    scenario.net.default_link = LinkSpec {
        min_delay_ms: 0,
        max_delay_ms: 0,
        reorder: false,
        loss_budget: 0,
    };
    scenario.peers = (0..8).map(|_| PeerSpec::dialing()).collect();
    for (index, peer) in scenario.peers.iter().enumerate() {
        scenario.net.links.push(LinkEntry {
            src: NODE_ADDR,
            dst: peer.address(index, 0),
            model: LinkSpec {
                min_delay_ms: 9_000,
                max_delay_ms: 9_000,
                ..scenario.net.default_link
            },
        });
    }
    scenario.peers.extend((0..32).map(|_| {
        let mut peer = PeerSpec::listening();
        peer.handshake = Some(HandshakeSpec {
            dial_delay_ms: 20,
            length_delay_ms: 8_000,
            ..HandshakeSpec::default()
        });
        peer
    }));
    zakura_sim::witness::witness_into::<Wire>(
        &scenario,
        PlanInput::Bytes(Vec::new()),
        "handshake/pending-bounded",
        zakura_sim::witness::Expect::Violated,
        &[],
        &mut NullSink,
    )
    .unwrap();
}
