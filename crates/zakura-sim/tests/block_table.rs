//! BS6: stream-6 table wiring on the block-sync harness (gate BS.9).
//!
//! Every link to the node carries a labelling relay, and the node's tap
//! writes its identities and effects, so the `block-sync/storage` harness
//! checks stream 6's generated receiver, downstream and sender properties
//! on the real routine, serving session and sequencer. These tests show
//! that the harness registers exactly the generated stream-6 IDs, that an
//! honest run holds every one it reaches with each judged frame labelled
//! once, that each probe fault reaches its exact ID, and that a scripted
//! bypass violates each ID.
#![cfg(zakura_sim)]

mod support;

use support::{
    inputs,
    script::{run_under, Node, OBSERVED},
};
use zakura_network::zakura::block_sync_streams;
use zakura_sim::{
    harness::{
        block_node::{
            control::{ControlAction, ControlStep},
            policy::{BlockAllow, BlockKind, Hold, Select, Trigger},
            probe::ProbeFrame,
            Policy, PROBE,
        },
        block_sync::{table_wiring_properties, BlockScenario, Storage, FAMILY},
        run_prepared, Subsystem,
    },
    model::Label,
    observe::{RowAt, StreamId},
    table::{GeneratedId, Template},
    MemorySink, Observation, PropertyOutcome, RunReport,
};

/// Stream 6 at the regulated version.
const STREAM: StreamId = StreamId {
    kind: 6,
    version: 3,
};

/// The generated ID `block-sync/s6v3/<suffix>`.
fn id(suffix: &str) -> String {
    format!("{FAMILY}/{STREAM}/{suffix}")
}

/// Row `row` of stream 6.
fn row(row: &str) -> RowAt {
    RowAt::new(STREAM, row)
}

/// Runs `scenario` under each of `count` plan inputs, and returns each
/// report. No run may end at a harness failure: a judged frame without
/// exactly one label is one.
fn runs(scenario: &BlockScenario, count: usize) -> Vec<RunReport> {
    inputs(count, 256)
        .into_iter()
        .map(|input| {
            let ports = Storage::prepare(scenario);
            let report =
                run_prepared::<Storage>(scenario, ports, input, &mut MemorySink::default(), None)
                    .unwrap_or_else(|error| panic!("{error}"));
            assert_eq!(report.end.harness, None, "{:?}", report.end);
            report
        })
        .collect()
}

/// The outcome `report` gives `id`.
fn outcome<'a>(report: &'a RunReport, id: &str) -> &'a PropertyOutcome {
    &report
        .properties
        .iter()
        .find(|r| r.property == id)
        .unwrap_or_else(|| panic!("the run reports {id}: {:?}", report.properties))
        .outcome
}

/// True if `id` applied in `report` and held.
fn held(report: &RunReport, id: &str) -> bool {
    matches!(outcome(report, id), PropertyOutcome::Held { applicable } if *applicable > 0)
}

/// A node holding every fixture body, with no mirrors, and a probe that
/// sends `frames` once the node's status has reached it.
fn probe(frames: Vec<ProbeFrame>) -> BlockScenario {
    BlockScenario {
        node_bodies: 8,
        mirrors: Vec::new(),
        probe: true,
        control: vec![ControlStep {
            action: ControlAction::ProbeSend { frames },
            when: Some(Trigger::VirtualTime(1_000)),
        }],
        ..BlockScenario::default()
    }
}

/// A node holding no body, with no mirrors, and a probe that advertises the
/// whole chain with `max_response_bytes`, so the node asks it for bodies,
/// then sends `frames` while that request is open.
fn requested(max_response_bytes: u32, frames: Vec<ProbeFrame>) -> BlockScenario {
    BlockScenario {
        node_bodies: 0,
        mirrors: Vec::new(),
        probe: true,
        control: vec![
            ControlStep {
                action: ControlAction::ProbeSend {
                    frames: vec![ProbeFrame::LimitedStatus {
                        servable_low: 0,
                        servable_high: 8,
                        max_inflight_requests: 1,
                        max_response_bytes,
                    }],
                },
                when: None,
            },
            ControlStep {
                action: ControlAction::ProbeSend { frames },
                when: Some(Trigger::VirtualTime(2_000)),
            },
        ],
        ..BlockScenario::default()
    }
}

/// A raw frame of `message_type` with `flags` and a `len`-byte payload.
fn raw(message_type: u16, flags: u16, len: u32) -> ProbeFrame {
    ProbeFrame::Raw {
        message_type,
        flags,
        len,
    }
}

/// The node's honest run: one mirror serves it the chain.
fn honest_sync() -> BlockScenario {
    BlockScenario::default()
}

/// The node's honest serving: the probe asks for two held bodies and one
/// past the node's tip.
fn honest_serving() -> BlockScenario {
    probe(vec![
        ProbeFrame::GetBlocks {
            start_height: 1,
            count: 1,
        },
        ProbeFrame::GetBlocks {
            start_height: 2,
            count: 1,
        },
        ProbeFrame::GetBlocks {
            start_height: 20,
            count: 1,
        },
    ])
}

/// The harness registers exactly stream 6's generated IDs beside its own
/// properties, each parsing back to the block-sync family on `s6v3`.
#[test]
fn the_harness_registers_exactly_the_generated_stream_six_ids() {
    let generated: Vec<String> = table_wiring_properties::<Node>()
        .into_iter()
        .map(|property| property.id.to_string())
        .collect();
    // Two per stream, five rows' close templates, one cadence row, seven
    // downstream rows and five sender rows.
    assert_eq!(generated.len(), 42, "{generated:?}");
    for generated_id in &generated {
        let parsed = GeneratedId::parse(generated_id).expect("a generated ID parses back");
        assert_eq!((parsed.family, parsed.stream), (FAMILY, STREAM));
    }
    let registered: Vec<String> = Storage::properties()
        .into_iter()
        .map(|property| property.id.to_string())
        .filter(|registered| GeneratedId::parse(registered).is_some())
        .collect();
    assert_eq!(registered, generated);
    let streams = block_sync_streams();
    assert_eq!(
        (streams[0].kind, streams[0].version),
        (STREAM.kind, STREAM.version)
    );
}

/// An honest sync and honest serving end with no violation, label every
/// judged frame once, and hold every generated ID they reach: the
/// receiver's never-closes check, its work, serving and output checks, and
/// the node's sender checks.
#[test]
fn honest_runs_hold_every_generated_id_they_reach() {
    for (scenario, reached) in [
        (
            honest_sync(),
            &[
                "conformant-never-closes",
                "block/receiver-work-authorized",
                "get-blocks/sender-within-limit",
                "status/sender-cadence",
            ][..],
        ),
        (
            honest_serving(),
            &[
                "conformant-never-closes",
                "get-blocks/receiver-serving-bounded",
                "block/sender-within-response-quantity",
                "blocks-done/sender-within-response-quantity",
                "range-unavailable/sender-within-response-quantity",
                "block/sender-answers-open",
                "blocks-done/sender-ends-once",
                "range-unavailable/sender-ends-once",
            ][..],
        ),
    ] {
        for report in runs(&scenario, 4) {
            assert_eq!(report.end.violation, None, "{:?}", report.end);
            for suffix in reached {
                assert!(
                    held(&report, &id(suffix)),
                    "{suffix}: {:?}",
                    report.properties
                );
            }
            let labelled = report
                .log
                .entries()
                .iter()
                .filter(|entry| matches!(entry.obs, Observation::PeerFrame { .. }))
                .count();
            assert!(labelled > 0, "the relays label the peers' frames");
        }
    }
}

/// Five generated IDs no block-sync scenario reaches, which stay
/// `NotApplicable`:
/// - `get-blocks/over-margin-closes`: the node's default config advertises
///   a request limit no probe script can exceed twice over, and the
///   scenario can't lower it;
/// - each ending's `over-owed-closes`: every exchange owes room for its
///   ending, and conformant bodies leave it;
/// - each ending's `receiver-work-authorized`: an ending carries no work.
#[test]
fn ids_no_scenario_reaches_stay_not_applicable() {
    for scenario in [honest_sync(), honest_serving()] {
        for report in runs(&scenario, 1) {
            for suffix in [
                "get-blocks/over-margin-closes",
                "blocks-done/over-owed-closes",
                "range-unavailable/over-owed-closes",
                "blocks-done/receiver-work-authorized",
                "range-unavailable/receiver-work-authorized",
            ] {
                assert_eq!(
                    *outcome(&report, &id(suffix)),
                    PropertyOutcome::NotApplicable,
                    "{suffix}"
                );
            }
        }
    }
}

/// Each probe fault the node judges outside any exchange of its own reaches
/// its exact `<label>-closes` ID, and the node's fault close discharges it:
/// undeclared types, flags, short and long payloads on every row, bad
/// content on the rows the codec checks, unsolicited responses, and an
/// overlapping request while the first one's read is held. A burst of
/// statuses empties the cadence bucket, and the node punishes nothing.
#[test]
fn each_probe_fault_reaches_its_exact_id() {
    let mut cases = vec![
        ("undeclared-type-closes", probe(vec![raw(9, 0, 1)])),
        ("status/bad-content-closes", probe(vec![raw(1, 0, 53)])),
        ("get-blocks/bad-content-closes", probe(vec![raw(2, 0, 9)])),
        ("block/unsolicited-closes", probe(vec![raw(3, 0, 2)])),
        (
            "blocks-done/unsolicited-closes",
            probe(vec![ProbeFrame::BlocksDone {
                start_height: 1,
                returned: 1,
            }]),
        ),
        (
            "range-unavailable/unsolicited-closes",
            probe(vec![ProbeFrame::RangeUnavailable {
                start_height: 1,
                count: 1,
            }]),
        ),
        (
            "status/cadence-never-closes",
            probe(
                (0..30)
                    .map(|_| ProbeFrame::Status {
                        servable_low: 0,
                        servable_high: 0,
                    })
                    .collect(),
            ),
        ),
    ];
    for (name, message_type, len) in [
        ("status", 1, 53),
        ("get-blocks", 2, 9),
        ("block", 3, 2),
        ("blocks-done", 4, 9),
        ("range-unavailable", 5, 9),
    ] {
        cases.push((
            match name {
                "status" => "status/reserved-flags-closes",
                "get-blocks" => "get-blocks/reserved-flags-closes",
                "block" => "block/reserved-flags-closes",
                "blocks-done" => "blocks-done/reserved-flags-closes",
                _ => "range-unavailable/reserved-flags-closes",
            },
            probe(vec![raw(message_type, 1, len)]),
        ));
        cases.push((
            match name {
                "status" => "status/short-payload-closes",
                "get-blocks" => "get-blocks/short-payload-closes",
                "block" => "block/short-payload-closes",
                "blocks-done" => "blocks-done/short-payload-closes",
                _ => "range-unavailable/short-payload-closes",
            },
            probe(vec![raw(message_type, 0, len - 1)]),
        ));
        let long = if message_type == 3 {
            2_000_002
        } else {
            len + 1
        };
        cases.push((
            match name {
                "status" => "status/long-payload-closes",
                "get-blocks" => "get-blocks/long-payload-closes",
                "block" => "block/long-payload-closes",
                "blocks-done" => "blocks-done/long-payload-closes",
                _ => "range-unavailable/long-payload-closes",
            },
            probe(vec![raw(message_type, 0, long)]),
        ));
    }
    // The node's read for the first request stays held until the probe's
    // connection closes, so the overlap is certain.
    let mut overlap = probe(vec![
        ProbeFrame::GetBlocks {
            start_height: 1,
            count: 1,
        },
        ProbeFrame::GetBlocks {
            start_height: 1,
            count: 1,
        },
    ]);
    overlap.policy = Policy {
        holds: vec![Hold {
            select: Select::nth(BlockKind::Read, 1),
            until: Trigger::Closed(PROBE.to_owned()),
            then: vec![BlockAllow::Serve, BlockAllow::Error],
            deadline_ms: None,
        }],
        ..Policy::converging()
    };
    cases.push(("get-blocks/bad-content-closes", overlap));
    for (suffix, scenario) in cases {
        for report in runs(&scenario, 2) {
            assert_eq!(report.end.violation, None, "{suffix}: {:?}", report.end);
            assert!(
                held(&report, &id(suffix)),
                "{suffix}: {:?}",
                report.properties
            );
        }
    }
}

/// Found by the wiring: a fault in a response to the node's own open
/// request closes the probe's connection, but the close records
/// `unfinished_exchange`, the writer fence's no-fault cause, before the
/// routine's `service_protocol_reject`. So an over-owed body, an
/// undecodable body, and an ending that disagrees with the bodies before it
/// each violate their exact ID, and the probe goes unpunished.
#[test]
fn faults_inside_the_nodes_exchange_close_without_a_fault_cause() {
    let body = ProbeFrame::Block { height: 1 };
    for (suffix, scenario) in [
        ("block/over-owed-closes", requested(1_000, vec![body])),
        (
            "block/bad-content-closes",
            requested(2_000_000, vec![raw(3, 0, 2)]),
        ),
        (
            "blocks-done/bad-content-closes",
            requested(
                2_000_000,
                vec![ProbeFrame::BlocksDone {
                    start_height: 1,
                    returned: 2,
                }],
            ),
        ),
        (
            "range-unavailable/bad-content-closes",
            requested(
                2_000_000,
                vec![
                    body,
                    ProbeFrame::RangeUnavailable {
                        start_height: 1,
                        count: 1,
                    },
                ],
            ),
        ),
    ] {
        for report in runs(&scenario, 2) {
            let violation = report
                .end
                .violation
                .as_ref()
                .expect("the close has no fault cause");
            assert_eq!(violation.property, id(suffix), "{:?}", report.end);
            let causes: Vec<_> = report
                .log
                .entries()
                .iter()
                .filter_map(|entry| match &entry.obs {
                    Observation::ConnectionClosed { cause, .. } => Some(cause.as_str()),
                    _ => None,
                })
                .collect();
            assert_eq!(causes, ["unfinished_exchange"], "{suffix}");
        }
    }
}

/// The scripted history that violates the generated ID `id` on connection
/// 1, one batch per step: a fault the node never punishes, a punishment of
/// a conformant peer, or an effect the role models never allowed, written
/// through no frame.
fn bypass(id: &GeneratedId<'_>) -> Vec<Vec<Observation>> {
    let row_name = id.row.map(str::to_owned);
    let peer = |label: Label, exhausted: bool| Observation::PeerFrame {
        connection: 1,
        stream: STREAM,
        seq: 0,
        row: row_name.clone().or(Some("status".to_owned())),
        rule_stream: STREAM,
        label,
        exhausted,
        exchange: None,
        payload: 9,
    };
    let node = |label: Label, exhausted: bool| {
        vec![vec![Observation::NodeFrame {
            connection: 1,
            stream: STREAM,
            seq: 0,
            row: row_name.clone(),
            rule_stream: STREAM,
            label,
            exhausted,
        }]]
    };
    let judged = Observation::FrameJudged {
        connection: 1,
        stream: STREAM,
        seq: 0,
    };
    let fault_close = Observation::ConnectionClosed {
        connection: 1,
        cause: "service_protocol_reject".to_owned(),
    };
    let frame = || row(id.row.expect("a per-row template"));
    match id.template {
        Template::Closes(label) => {
            let mut sent = peer(label, false);
            if label == Label::UndeclaredType {
                if let Observation::PeerFrame { row, .. } = &mut sent {
                    *row = None;
                }
            }
            vec![vec![sent], vec![judged]]
        }
        Template::ConformantNeverCloses => {
            vec![
                vec![peer(Label::Conformant, false)],
                vec![judged, fault_close],
            ]
        }
        Template::CadenceNeverCloses => {
            vec![
                vec![peer(Label::Conformant, true)],
                vec![judged, fault_close],
            ]
        }
        Template::WorkAuthorized => vec![vec![Observation::WorkAdmitted {
            connection: 1,
            frame: frame(),
            seq: 0,
            request: row("get-blocks"),
            exchange: 1,
            objects: 1,
            bytes: 1,
            ends: false,
        }]],
        Template::SenderWithinResponseQuantity => vec![vec![Observation::NodeOutput {
            connection: 1,
            frame: frame(),
            request: row("get-blocks"),
            exchange: 1,
            payload: 9,
            ends: id.row != Some("block"),
        }]],
        Template::ServingBounded => vec![vec![Observation::Serving {
            connection: 1,
            request: frame(),
            exchange: 1,
        }]],
        Template::SenderWithinLimit => node(Label::OverMargin, false),
        Template::SenderAnswersOpen | Template::SenderEndsOnce => node(Label::Unsolicited, false),
        Template::SenderCadence => node(Label::Conformant, true),
        template => panic!("stream 6 generates no {template:?}"),
    }
}

/// Every generated stream-6 ID fails on a scripted history that bypasses
/// its rule: a fault the node never punishes, a punishment of a conformant
/// peer, work, serving or output no conformant frame allowed, and node
/// frames the peer's models refuse.
#[test]
fn scripted_bypasses_violate_every_generated_id() {
    for property in table_wiring_properties::<Node>() {
        let name = property.id.to_string();
        let parsed = GeneratedId::parse(&name).expect("a generated ID parses back");
        let report = run_under(table_wiring_properties(), OBSERVED, bypass(&parsed));
        assert!(
            matches!(outcome(&report, &name), PropertyOutcome::Violated { .. }),
            "{name}: {:?}",
            report.properties
        );
    }
}
