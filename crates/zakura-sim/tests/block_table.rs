//! BS6: stream-6 table wiring on the block-sync harness (gate BS.9).
//!
//! Every link to the node carries a labelling relay, and the node's tap
//! writes its identities and effects, so the `block-sync/storage` harness
//! checks stream 6's generated receiver, downstream and sender properties
//! on the real routine, serving session and sequencer. These tests show
//! that the harness registers exactly the generated stream-6 IDs, that an
//! honest run holds every one it reaches with each judged frame labelled
//! once, that each probe fault reaches its exact ID, that each relay
//! label and adapter effect a check reads is the one the node acted on, and
//! that a scripted bypass violates each ID.
#![cfg(zakura_sim)]

mod support;

use support::{
    inputs,
    script::{run_under, Node, OBSERVED},
};
use zakura_network::zakura::{block_sync_streams, BlockSyncMessage};
use zakura_sim::{
    harness::{
        block_node::{
            control::{ControlAction, ControlStep},
            policy::{BlockAllow, BlockKind, Hold, Select, Trigger},
            probe::ProbeFrame,
            Policy, PROBE,
        },
        block_sync::{table_wiring_properties, BlockScenario, Ports, Storage, FAMILY},
        run_prepared, Subsystem,
    },
    model::Label,
    observe::{RowAt, StorageFact, StorageRequest, StreamId},
    table::{GeneratedId, Template},
    MemorySink, Observation, PlanInput, PropertyOutcome, RunReport,
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

/// Runs `scenario` under `input`, and returns its report and ports. No run
/// may end at a harness failure: a judged frame without exactly one label
/// is one. Every block storage operation the node dispatches pairs with the
/// owner that sent it.
fn run(scenario: &BlockScenario, input: PlanInput) -> (RunReport, Ports) {
    let ports = Storage::prepare(scenario);
    let report = run_prepared::<Storage>(
        scenario,
        ports.clone(),
        input,
        &mut MemorySink::default(),
        None,
    )
    .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    {
        let view = ports.view.borrow();
        assert!(view.pairing.is_empty(), "{:?}", view.pairing);
    }
    (report, ports)
}

/// Runs `scenario` under each of `count` plan inputs, as [`run`] does, and
/// returns each report.
fn runs(scenario: &BlockScenario, count: usize) -> Vec<RunReport> {
    inputs(count, 256)
        .into_iter()
        .map(|input| run(scenario, input).0)
        .collect()
}

/// The close causes in `report`, in order.
fn causes(report: &RunReport) -> Vec<&str> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::ConnectionClosed { cause, .. } => Some(cause.as_str()),
            _ => None,
        })
        .collect()
}

/// A hold on the node's `nth` range read until the probe's connection
/// closes.
fn held_read(nth: u32) -> Hold {
    Hold {
        select: Select::nth(BlockKind::Read, nth),
        until: Trigger::Closed(PROBE.to_owned()),
        then: vec![BlockAllow::Serve, BlockAllow::Error],
        deadline_ms: None,
    }
}

/// A request for `count` bodies from `start_height`.
fn get_blocks(start_height: u32, count: u32) -> ProbeFrame {
    ProbeFrame::GetBlocks {
        start_height,
        count,
    }
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

/// Each probe fault the node judges outside any exchange of its own, by the
/// ID it reaches: undeclared types, flags, short and long payloads on every
/// row, bad content on the rows the codec checks, unsolicited responses,
/// and an overlapping request while the first one's read is held. A burst
/// of statuses empties the cadence bucket.
fn probe_faults() -> Vec<(&'static str, BlockScenario)> {
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
    let mut overlap = probe(vec![get_blocks(1, 1), get_blocks(1, 1)]);
    overlap.policy = Policy {
        holds: vec![held_read(1)],
        ..Policy::converging()
    };
    cases.push(("get-blocks/bad-content-closes", overlap));
    cases
}

/// Each probe fault inside the node's own exchange, by the ID it reaches:
/// a body over the bytes the node's request allows, an undecodable body, a
/// body whose header doesn't commit to its transactions, and endings that
/// disagree with the bodies before them.
fn exchange_faults() -> Vec<(&'static str, BlockScenario)> {
    vec![
        (
            "block/over-owed-closes",
            requested(1_000, vec![ProbeFrame::Block { height: 1 }]),
        ),
        (
            "block/bad-content-closes",
            requested(2_000_000, vec![raw(3, 0, 2)]),
        ),
        (
            "block/bad-content-closes",
            requested(2_000_000, vec![ProbeFrame::TamperedBlock { height: 1 }]),
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
                    ProbeFrame::Block { height: 1 },
                    ProbeFrame::RangeUnavailable {
                        start_height: 1,
                        count: 1,
                    },
                ],
            ),
        ),
    ]
}

/// The node holds every fixture body and serves one request at a time.
/// The probe sends three requests while the node's first two reads are
/// held, which is past twice its limit.
fn over_margin() -> BlockScenario {
    BlockScenario {
        serving_limit: Some(1),
        policy: Policy {
            holds: vec![held_read(1), held_read(2)],
            ..Policy::converging()
        },
        ..probe(vec![get_blocks(1, 1), get_blocks(2, 1), get_blocks(3, 1)])
    }
}

/// Four generated IDs no block-sync scenario reaches stay `NotApplicable`
/// in every scenario these tests run:
/// - each ending's `over-owed-closes`: every exchange owes room for its
///   ending, and conformant bodies leave it;
/// - each ending's `receiver-work-authorized`: an ending carries no body,
///   so the node admits no work from it.
#[test]
fn ids_no_scenario_reaches_stay_not_applicable() {
    let scenarios = [honest_sync(), honest_serving(), over_margin()]
        .into_iter()
        .chain(probe_faults().into_iter().map(|(_, scenario)| scenario))
        .chain(exchange_faults().into_iter().map(|(_, scenario)| scenario));
    for scenario in scenarios {
        for report in runs(&scenario, 1) {
            for suffix in [
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
/// its exact `<label>-closes` ID, and the node's fault close discharges it.
/// The cadence burst reaches `cadence-never-closes`, and the node punishes
/// nothing.
#[test]
fn each_probe_fault_reaches_its_exact_id() {
    for (suffix, scenario) in probe_faults() {
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

/// Each probe fault inside the node's own exchange reaches its exact ID,
/// and the node's fault close discharges it: the node's stream reader
/// refuses an over-owed body's header, and the routine rejects the rest.
/// The node also reports the tampered body's sender as misbehaving.
#[test]
fn faults_inside_the_nodes_exchange_reach_their_exact_ids() {
    for (suffix, scenario) in exchange_faults() {
        let tampered = matches!(
            scenario.control[1].action,
            ControlAction::ProbeSend { ref frames } if matches!(frames[..], [ProbeFrame::TamperedBlock { .. }])
        );
        for report in runs(&scenario, 2) {
            assert_eq!(report.end.violation, None, "{suffix}: {:?}", report.end);
            assert!(
                held(&report, &id(suffix)),
                "{suffix}: {:?}",
                report.properties
            );
            let causes = causes(&report);
            assert!(
                matches!(causes[..], [cause] if cause != "unfinished_exchange"),
                "{suffix}: {causes:?}"
            );
            if tampered {
                assert!(
                    report
                        .log
                        .entries()
                        .iter()
                        .any(|entry| matches!(entry.obs, Observation::Misbehavior { .. })),
                    "the node reports the tampered body's sender"
                );
            }
        }
    }
}

/// Requests past twice the node's serving limit, while none is served,
/// reach `get-blocks/over-margin-closes`, and the node's close discharges
/// it.
#[test]
fn requests_past_twice_the_serving_limit_reach_over_margin() {
    for report in runs(&over_margin(), 4) {
        assert_eq!(report.end.violation, None, "{:?}", report.end);
        assert!(
            held(&report, &id("get-blocks/over-margin-closes")),
            "{:?}",
            report.properties
        );
    }
}

/// The relay judges a request's overlap against the ranges the node still
/// serves when it judges the request. A repeat of a request the node may
/// already have answered holds every ID, whichever way the run orders the
/// node's ending and its judgment. A request inside an earlier range's full
/// count, while that range's read is held, reaches
/// `get-blocks/bad-content-closes`.
#[test]
fn an_overlap_is_judged_against_the_nodes_live_ranges() {
    for report in runs(&probe(vec![get_blocks(1, 1), get_blocks(1, 1)]), 8) {
        assert_eq!(report.end.violation, None, "{:?}", report.end);
    }
    let mut inside = probe(vec![get_blocks(1, 4), get_blocks(2, 1)]);
    inside.policy = Policy {
        holds: vec![held_read(1)],
        ..Policy::converging()
    };
    for report in runs(&inside, 2) {
        assert_eq!(report.end.violation, None, "{:?}", report.end);
        assert!(
            held(&report, &id("get-blocks/bad-content-closes")),
            "{:?}",
            report.properties
        );
    }
}

/// A repeated body inside the node's open range is a local incompatibility,
/// not a fault: the run holds every ID, and the node admits the body's work
/// once, from the frame that delivered it first.
#[test]
fn a_repeated_body_admits_its_work_once() {
    let body = ProbeFrame::Block { height: 1 };
    for report in runs(&requested(2_000_000, vec![body, body]), 4) {
        assert_eq!(report.end.violation, None, "{:?}", report.end);
        assert!(
            held(&report, &id("block/receiver-work-authorized")),
            "{:?}",
            report.properties
        );
    }
}

/// A well-formed body while the node has no range open is unsolicited: the
/// relay labels it `Unsolicited`, the node's stream reader refuses its
/// header and closes the probe's connection, and the close discharges
/// `block/unsolicited-closes`.
#[test]
fn a_well_formed_unsolicited_body_reaches_unsolicited_closes() {
    for report in runs(&probe(vec![ProbeFrame::Block { height: 1 }]), 4) {
        assert_eq!(report.end.violation, None, "{:?}", report.end);
        assert!(
            held(&report, &id("block/unsolicited-closes")),
            "{:?}",
            report.properties
        );
        let labels: Vec<Label> = report
            .log
            .entries()
            .iter()
            .filter_map(|entry| match &entry.obs {
                Observation::PeerFrame { row, label, .. } if row.as_deref() == Some("block") => {
                    Some(*label)
                }
                _ => None,
            })
            .collect();
        assert_eq!(labels, [Label::Unsolicited]);
        assert_eq!(causes(&report), ["ordered_read_rejected"]);
    }
}

/// A verifier verdict against a mirror's body makes the node report the
/// mirror, which no relay label justifies. The adapter writes no
/// punishment for that mirror, so the run holds every ID.
#[test]
fn a_verifier_verdict_punishes_without_a_violation() {
    let scenario = BlockScenario {
        policy: Policy {
            holds: vec![Hold {
                select: Select::nth(BlockKind::Apply, 1),
                until: Trigger::VirtualTime(1),
                then: vec![BlockAllow::ConsensusInvalid, BlockAllow::PayloadMismatch],
                deadline_ms: None,
            }],
            ..Policy::converging()
        },
        ..honest_sync()
    };
    for report in runs(&scenario, 4) {
        assert_eq!(report.end.violation, None, "{:?}", report.end);
        assert!(
            report.log.entries().iter().any(|entry| matches!(
                &entry.obs,
                Observation::Storage {
                    fact: StorageFact::BlockSubmitted {
                        request: StorageRequest::Misbehavior { .. },
                        ..
                    },
                    ..
                }
            )),
            "the node reports the mirror"
        );
    }
}

/// Each `NodeOutput` carries the payload length of the response frame the
/// node sent, in the order the probe received them.
#[test]
fn node_outputs_carry_the_frames_the_node_sent() {
    for input in inputs(4, 256) {
        let (report, ports) = run(&honest_serving(), input);
        let sent: Vec<u64> = ports
            .probe
            .received()
            .into_iter()
            .map(|message| message.expect("the node sends frames the codec reads"))
            .filter(|message| {
                matches!(
                    message,
                    BlockSyncMessage::Block(_)
                        | BlockSyncMessage::BlocksDone { .. }
                        | BlockSyncMessage::RangeUnavailable { .. }
                )
            })
            .map(|message| {
                let frame = message.encode_frame().expect("a read message encodes");
                u64::try_from(frame.payload.len()).expect("a payload length fits in u64")
            })
            .collect();
        let outputs: Vec<u64> = report
            .log
            .entries()
            .iter()
            .filter_map(|entry| match entry.obs {
                Observation::NodeOutput { payload, .. } => Some(payload),
                _ => None,
            })
            .collect();
        assert_eq!(sent.len(), 5, "{sent:?}");
        assert_eq!(outputs, sent);
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
