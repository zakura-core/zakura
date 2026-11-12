//! P3d-1: witness scenarios for the properties `header-sync/wire`
//! registers (P3.3). Each generated property applies in its witness, a run
//! of the harness itself, with its receiver partner, the stream's
//! `conformant-never-closes`, applied and held. Each invalid-label witness
//! has a conformant control, and no witness punishes a peer the scenario
//! marks `HonestPeer` (`session/honest-never-punished`). Scripted histories
//! violate each ID, so no check passes vacuously.
#![cfg(zakura_sim)]

mod support;

use std::{collections::BTreeSet, rc::Rc};

use support::{
    inputs,
    patched::{run_ok, Worktree},
    script::{run_scripted, run_under, Node, OBSERVED},
};
use zakura_network::zakura::v9::RegtestMessage;
use zakura_sim::harness::header_acceptance::history::run_declared;
use zakura_sim::NaReason;
use zakura_sim::{
    domain::{
        header_sync::OUTSTANDING_WORK_BOUNDED,
        header_sync_progress::HONEST_PROGRESS,
        session::{honest_never_punished, HonestPeers, HONEST_NEVER_PUNISHED},
    },
    harness::{
        header_node::{Chain, Policy},
        header_sync::{
            storage_properties, HeaderWire, WireScenario, FAMILY, HEADER_FAULT_PENALIZED,
        },
        run_prepared,
        wire_plans::{act_scenario, SavedPlan},
        HarnessError, LinkSpec, NetScenario, Subsystem,
    },
    model::{is_fault_cause, Label},
    observe::{StreamId, TransportFact},
    peer::{
        adversary::{Act, ContentRule, CursorField, Row},
        honest::{CreditSpec, GrantPolicy, HonestSpec, SubscribeSpec},
        peer_addr, peer_of, PeerAction, PeerSpec,
    },
    property::{Assume, AssumeRecord, Assumptions, HasView, Property, Source},
    table::{sender_properties, table_properties, GeneratedId, Template},
    witness::{judge_report, Expect},
    Observation, Plan, PlanInput, PropertyOutcome, RunReport, Violation,
};

/// Header sync's stream at the version it tables.
const STREAM: StreamId = StreamId {
    kind: 5,
    version: 9,
};

/// The node's chain at the start of most witnesses.
const PRELOAD: u32 = 50;

/// The generated ID `header-sync/s5v9/<suffix>`.
fn id(suffix: &str) -> String {
    format!("{FAMILY}/{STREAM}/{suffix}")
}

/// One fault act per `<label>-closes` ID header sync can reach, and the
/// cadence burst. [`reached`] names each one's ID.
const ACTS: [Act; 24] = [
    Act::UndeclaredType { message_type: 0 },
    Act::ReservedFlags {
        row: Row::Status,
        bit: 0,
    },
    Act::ShortPayload { row: Row::Status },
    Act::LongPayload { row: Row::Status },
    Act::BadContent {
        rule: ContentRule::StatusAnchor,
    },
    Act::StatusBurst,
    Act::ReservedFlags {
        row: Row::SubscribeHeaders,
        bit: 0,
    },
    Act::ShortPayload {
        row: Row::SubscribeHeaders,
    },
    Act::LongPayload {
        row: Row::SubscribeHeaders,
    },
    Act::OverLive,
    Act::OverCreditObjects,
    Act::AfterClose,
    Act::BadCursor {
        field: CursorField::Hash,
    },
    Act::BadContent {
        rule: ContentRule::OpenLocators,
    },
    Act::ReservedFlags {
        row: Row::Headers,
        bit: 0,
    },
    Act::ShortPayload { row: Row::Headers },
    Act::LongPayload { row: Row::Headers },
    Act::Unsolicited { row: Row::Headers },
    Act::BadContent {
        rule: ContentRule::Linkage,
    },
    Act::ReservedFlags {
        row: Row::HeadersOutcome,
        bit: 0,
    },
    Act::ShortPayload {
        row: Row::HeadersOutcome,
    },
    Act::LongPayload {
        row: Row::HeadersOutcome,
    },
    Act::Unsolicited {
        row: Row::HeadersOutcome,
    },
    Act::BadContent {
        rule: ContentRule::OutcomeWindow,
    },
];

/// The IDs the honest witness, [`both_ways`], reaches: the receiver
/// partner and every sender ID.
const HONEST: [&str; 6] = [
    "conformant-never-closes",
    "status/sender-cadence",
    "subscribe-headers/sender-within-live",
    "subscribe-headers/sender-within-credit",
    "headers/sender-answers-open",
    "headers-outcome/sender-ends-once",
];

/// The generated IDs no header-sync scenario reaches. Header sync's
/// responses answer a subscription, whose pages spend credit, so the
/// models label an overrun `over-credit` and never `over-owed`.
const UNREACHED: [&str; 2] = [
    "headers/over-owed-closes",
    "headers-outcome/over-owed-closes",
];

/// The ID `act`'s witness reaches: its label's `<label>-closes` on the row
/// whose rule the label breaks, or the burst's `cadence-never-closes`.
fn reached(act: Act) -> String {
    match act.label() {
        Label::Conformant => id("status/cadence-never-closes"),
        Label::UndeclaredType => id("undeclared-type-closes"),
        // A page's credit is its subscription's.
        Label::OverCredit => id("subscribe-headers/over-credit-closes"),
        label => id(&format!("{}/{}-closes", act.row().name(), label.name())),
    }
}

/// An honest peer holding `chain` headers, as `spec`.
fn honest(spec: PeerSpec, chain: u32, subscribe: Option<SubscribeSpec>) -> PeerSpec {
    PeerSpec {
        honest: Some(HonestSpec { chain, subscribe }),
        ..spec
    }
}

/// M5-E2's `both_ways` run: the node at 50 dials an honest peer that serves
/// it 120 headers and subscribes to it from genesis, so the node grants,
/// publishes and ends the subscription when the peer closes it.
fn both_ways() -> WireScenario {
    WireScenario {
        node: Chain::main(PRELOAD),
        peers: vec![honest(
            PeerSpec::listening(),
            120,
            Some(SubscribeSpec {
                start: 0,
                open: CreditSpec {
                    objects: 250,
                    bytes: 65_750,
                },
                grants: GrantPolicy::Immediate,
            }),
        )],
        policy: Policy::converging(),
        ..WireScenario::default()
    }
}

/// `act`'s witness: M5-E1's setup history for it, with an honest peer
/// beside the adversary that dials the node and holds its chain, so
/// nobody subscribes to it.
fn act_witness(act: Act) -> WireScenario {
    let mut scenario = act_scenario(act);
    let chain = scenario.node.main;
    scenario
        .peers
        .push(honest(PeerSpec::dialing(), chain, None));
    scenario
}

/// Runs `scenario` under the default plan as the manifest history `case`,
/// which the acceptance tool records from this declaration (C18), and
/// returns the report even of a run that ended in a harness failure.
fn run_case(case: &str, scenario: &WireScenario) -> RunReport {
    run_declared(
        HeaderWire::NAME,
        case,
        scenario,
        Plan::from_fuzz(&[]),
        run_plan_into,
    )
}

/// `act`'s history's case: the generated ID it reaches, less the family's
/// prefix.
fn act_case(act: Act) -> String {
    let reached = reached(act);
    reached
        .strip_prefix(&id(""))
        .unwrap_or_else(|| panic!("{reached} is a generated ID"))
        .to_owned()
}

/// Runs `scenario` under `plan`, recording into `sink`, and returns the
/// report even of a run that ended in a harness failure.
fn run_plan_into(
    scenario: &WireScenario,
    plan: PlanInput,
    sink: &mut dyn zakura_sim::RecordSink,
) -> RunReport {
    let ports = HeaderWire::prepare(scenario);
    let ran = run_prepared::<HeaderWire>(scenario, ports, plan, sink, None);
    match ran {
        Ok(report) => report,
        Err(HarnessError::Run { report, .. }) => *report,
        Err(error) => panic!("{error}"),
    }
}

/// The outcome `report` gives `id`.
fn outcome<'r>(report: &'r RunReport, id: &str) -> &'r PropertyOutcome {
    &report
        .properties
        .iter()
        .find(|property| property.property == id)
        .unwrap_or_else(|| panic!("{id} is registered"))
        .outcome
}

/// Requires `id` to apply and hold in `report`, as an `applicable` witness
/// test judges it.
fn applies(report: &RunReport, id: &str, what: impl std::fmt::Debug) {
    if let Err(error) = judge_report::<HeaderWire>(report, id, Expect::Applicable) {
        panic!("{what:?}: {id}: {error}: {:?}", report.end);
    }
}

/// Requires `session/honest-never-punished` to apply and hold in `report`.
/// Its trigger history reads the harness's view, which a finished report
/// doesn't carry, so this reads the property's outcome. The trigger itself
/// is judged where the view is, as its planted case records it.
fn honest_holds(report: &RunReport, what: impl std::fmt::Debug) {
    let outcome = outcome(report, HONEST_NEVER_PUNISHED);
    assert!(
        matches!(outcome, PropertyOutcome::Held { applicable } if *applicable > 0),
        "{what:?}: {HONEST_NEVER_PUNISHED}: {outcome:?}"
    );
}

/// Every generated ID `header-sync/wire` registers, in order.
fn generated() -> Vec<String> {
    HeaderWire::properties()
        .into_iter()
        .map(|property| property.id.to_string())
        .filter(|registered| GeneratedId::parse(registered).is_some())
        .collect()
}

/// The harness registers header sync's generated receiver and sender IDs,
/// then `session/honest-never-punished` and `header-sync/honest-progress`,
/// after the storage properties, less `header-sync/outstanding-work-bounded`,
/// whose tap only the storage harness watches, and
/// `header-sync/header-fault-penalized`.
/// No downstream template is registered, since the adapter observes none
/// of their effects.
#[test]
fn the_harness_registers_the_generated_header_ids() {
    let mut expected: Vec<String> = table_properties::<Node>(FAMILY, &RegtestMessage::LAYOUT)
        .into_iter()
        .chain(sender_properties::<Node>(FAMILY, &RegtestMessage::LAYOUT))
        .map(|property| property.id.to_string())
        .collect();
    // Two per stream, 24 per-row close IDs, one cadence ID and five sender
    // IDs.
    assert_eq!(expected.len(), 32, "{expected:?}");
    assert_eq!(generated(), expected);
    let registered: Vec<String> = HeaderWire::properties()
        .into_iter()
        .map(|property| property.id.to_string())
        .collect();
    expected.push(HONEST_NEVER_PUNISHED.to_owned());
    expected.push(HONEST_PROGRESS.to_owned());
    // The storage harness's properties but one come first, then the fault
    // property, then the generated ones.
    let storage: Vec<String> = storage_properties::<HeaderWire>()
        .into_iter()
        .map(|property| property.id.to_string())
        .filter(|id| id != OUTSTANDING_WORK_BOUNDED)
        .collect();
    let fault = registered
        .iter()
        .position(|id| id == HEADER_FAULT_PENALIZED)
        .expect("the wire harness registers the fault property");
    assert_eq!(registered[..fault], storage[..], "{registered:?}");
    assert_eq!(registered[fault + 1..], expected[..]);
}

/// Requires `ids` to apply and hold in `report`, the run of `witness`,
/// judged as `applicable` witness tests, and so do the receiver partner,
/// `conformant-never-closes`, and `session/honest-never-punished`. The two
/// [`UNREACHED`] IDs stay `NotApplicable`.
fn witnessed(report: &RunReport, witness: impl std::fmt::Debug, ids: &[String]) {
    assert_eq!(report.end.violation, None, "{witness:?}: {:?}", report.end);
    for covered in ids {
        applies(report, covered, &witness);
    }
    applies(report, &id("conformant-never-closes"), &witness);
    honest_holds(report, &witness);
    for suffix in UNREACHED {
        assert_eq!(
            *outcome(report, &id(suffix)),
            PropertyOutcome::not_applicable(NaReason::NeverApplied),
            "{witness:?}: {suffix}"
        );
    }
}

/// Runs the witness of each act in [`ACTS`] whose ID names `row`, and
/// requires its ID to apply and hold there ([`witnessed`]).
fn apply_on_row(row: &str) {
    let on_row = |act: &&Act| reached(**act).split('/').nth(2) == Some(row);
    let acts: Vec<&Act> = ACTS.iter().filter(on_row).collect();
    assert!(!acts.is_empty());
    for act in acts {
        witnessed(
            &run_case(&act_case(*act), &act_witness(*act)),
            act,
            &[reached(*act)],
        );
    }
}

/// The witnesses reach every generated ID but the [`UNREACHED`] ones, each
/// fault act its own, and the honest witness applies and holds
/// `conformant-never-closes` and every sender ID. The fault witnesses run
/// in one test per row, below, so each test takes seconds.
///
/// This fails if a witness stops reaching its ID, or the node punishes the
/// honest peer.
#[test]
fn every_generated_property_applies_in_its_witness() {
    let listed: Vec<String> = ACTS
        .into_iter()
        .map(reached)
        .chain(HONEST.iter().chain(&UNREACHED).map(|suffix| id(suffix)))
        .collect();
    let distinct: BTreeSet<String> = listed.iter().cloned().collect();
    assert_eq!(distinct.len(), listed.len(), "{listed:?}");
    assert_eq!(distinct, generated().into_iter().collect());
    let honest: Vec<String> = HONEST.iter().map(|suffix| id(suffix)).collect();
    witnessed(&run_case("both-ways", &both_ways()), "honest", &honest);
}

/// `undeclared-type-closes` and each `status` ID apply and hold in their
/// witnesses, with the cadence burst's `cadence-never-closes`.
#[test]
fn status_faults_apply_in_their_witnesses() {
    witnessed(
        &run_case(&act_case(ACTS[0]), &act_witness(ACTS[0])),
        ACTS[0],
        &[id("undeclared-type-closes")],
    );
    apply_on_row("status");
}

/// Each `subscribe-headers` fault ID applies and holds in its witness.
#[test]
fn subscribe_headers_faults_apply_in_their_witnesses() {
    apply_on_row("subscribe-headers");
}

/// Each `headers` fault ID applies and holds in its witness, and
/// `headers/over-owed-closes` stays `NotApplicable` there.
#[test]
fn headers_faults_apply_in_their_witnesses() {
    apply_on_row("headers");
}

/// Each `headers-outcome` fault ID applies and holds in its witness, and
/// `headers-outcome/over-owed-closes` stays `NotApplicable` there.
#[test]
fn headers_outcome_faults_apply_in_their_witnesses() {
    apply_on_row("headers-outcome");
}

/// The conformant frame at the boundary `fault` crosses, on the same row:
/// the row's smallest frame, or for a `headers` page over its size or
/// credit, the largest the credit allows. A second `Open` stands for the
/// third, and a renewal for an `Open` after the close or with a bad
/// cursor.
fn control(fault: Act) -> Act {
    match fault {
        Act::LongPayload { row: Row::Headers } | Act::OverCreditObjects => {
            Act::MaxPayload { row: Row::Headers }
        }
        Act::OverLive => Act::SecondOpen,
        Act::AfterClose | Act::BadCursor { .. } => Act::MinPayload {
            row: Row::SubscribeHeaders,
        },
        fault => Act::MinPayload { row: fault.row() },
    }
}

/// The first frame peer 0 sent on `act`'s row after the plan picked `act`,
/// with its connection, QUIC stream, sequence number and label.
fn act_frame(report: &RunReport, act: Act) -> (u64, u64, u64, Label) {
    let identity = format!("peer:peer-0:{}", act.name());
    let picked = report
        .picks
        .iter()
        .find(|pick| pick.identity == identity)
        .unwrap_or_else(|| panic!("the plan never picked {identity}: {:?}", report.end))
        .step;
    let peer0 = peer_connections(report, &BTreeSet::from([0]));
    report
        .log
        .entries()
        .iter()
        .filter(|entry| entry.step >= picked)
        .find_map(|entry| match &entry.obs {
            Observation::PeerFrame {
                connection,
                quic_stream: Some(quic_stream),
                seq,
                row: Some(row),
                label,
                ..
            } if peer0.contains(connection) && row == act.row().name() => {
                Some((*connection, *quic_stream, *seq, *label))
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("{act:?} sent no frame on its row"))
}

/// Each invalid-label witness has a conformant control: the same row's
/// boundary, sent conformant from its own setup history. The control's
/// frame is `Conformant`, the node judges it, and the node records no
/// fault close cause on the adversary's connection. A neutral close may
/// come later: an adversary that serves no more pages gets
/// `unfinished_exchange`. `conformant-never-closes` applies and holds.
///
/// This fails if the node punishes a conformant frame at a boundary its
/// fault witness crosses.
#[test]
fn each_invalid_label_witness_has_a_conformant_control() {
    let mut controls: Vec<Act> = Vec::new();
    for fault in ACTS.into_iter().filter(|act| *act != Act::StatusBurst) {
        let control = control(fault);
        assert_ne!(fault.label(), Label::Conformant, "{fault:?}");
        assert_eq!(control.label(), Label::Conformant, "{fault:?}");
        if !controls.contains(&control) {
            controls.push(control);
        }
    }
    assert_eq!(controls.len(), 6, "{controls:?}");
    for control in controls {
        let case = format!("{}/{}", control.row().name(), control.name());
        let report = run_case(&case, &act_witness(control));
        assert_eq!(report.end.violation, None, "{control:?}: {:?}", report.end);
        assert_eq!(report.end.harness, None, "{control:?}: {:?}", report.end);
        let (connection, quic_stream, seq, label) = act_frame(&report, control);
        assert_eq!(label, Label::Conformant, "{control:?}");
        let judged = report.log.entries().iter().any(|entry| {
            matches!(entry.obs, Observation::FrameJudged { connection: c, quic_stream: q, seq: s, .. }
                if (c, q, s) == (connection, Some(quic_stream), seq))
        });
        assert!(judged, "{control:?}: the node never judged the frame");
        let faulted = report
            .log
            .entries()
            .iter()
            .find_map(|entry| match &entry.obs {
                Observation::ConnectionClosed {
                    connection: c,
                    cause,
                } if *c == connection && is_fault_cause(cause) => Some(cause.clone()),
                _ => None,
            });
        assert_eq!(faulted, None, "{control:?}: the node punished the frame");
        applies(&report, &id("conformant-never-closes"), control);
    }
}

/// The node's connections to peers the scenario marks `HonestPeer`, in
/// `report`.
fn honest_connections(report: &RunReport, scenario: &WireScenario) -> BTreeSet<u64> {
    peer_connections(report, &scenario.honest_peers())
}

/// The node's connections to the scenario peers `peers`, in `report`, from
/// the remote addresses their `registered` rows name.
fn peer_connections(report: &RunReport, peers: &BTreeSet<usize>) -> BTreeSet<u64> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Transport {
                connection: Some(connection),
                remote,
                event: TransportFact::Registered,
                ..
            } => {
                let peer = peer_of(remote.parse().ok()?)?;
                peers.contains(&peer).then_some(*connection)
            }
            _ => None,
        })
        .collect()
}

/// Across honest runs both ways, a lossy reordering network, an honest
/// close and redial, and a fault witness's bystander, the node records no
/// fault close cause and asks for no `Misbehavior` on any connection of a
/// peer the scenario marks `HonestPeer`, and the property applies and
/// holds. The lossy run's read errors get neutral causes (P3e), so they
/// pass.
///
/// This fails if the node punishes an honest peer, or blames the
/// bystander for the adversary's fault.
#[test]
fn honest_peer_never_punished() {
    let lossy = WireScenario {
        net: NetScenario {
            default_link: LinkSpec {
                loss_budget: 8,
                reorder: true,
                ..LinkSpec::default()
            },
            loss_after_admission: true,
            ..NetScenario::default()
        },
        node: Chain::main(PRELOAD),
        peers: vec![
            honest(PeerSpec::dialing(), 120, None),
            honest(PeerSpec::listening(), 120, None),
        ],
        policy: Policy::converging(),
        budget: None,
    };
    let redial = WireScenario {
        peers: vec![PeerSpec {
            script: vec![PeerAction::Close, PeerAction::Redial],
            ..honest(PeerSpec::dialing(), 120, None)
        }],
        ..SavedPlan::HonestSync.scenario()
    };
    let bystander = act_witness(Act::ShortPayload { row: Row::Status });
    let cases = [
        ("both ways", both_ways(), vec![Plan::from_fuzz(&[])]),
        ("lossy", lossy, inputs(2, 16_384)),
        ("redial", redial, vec![Plan::from_fuzz(&[])]),
        ("bystander", bystander, vec![Plan::from_fuzz(&[])]),
    ];
    for (name, scenario, plans) in cases {
        for (index, plan) in plans.into_iter().enumerate() {
            let case = match name {
                "lossy" => format!("lossy plan {index} of inputs(2, 16384)"),
                name => name.to_owned(),
            };
            // Each run is a manifest history, which the acceptance tool
            // records from this declaration (C18).
            let report = run_declared(HeaderWire::NAME, &case, &scenario, plan, run_plan_into);
            assert_eq!(report.end.harness, None, "{name}: {:?}", report.end);
            assert_eq!(report.end.violation, None, "{name}: {:?}", report.end);
            honest_holds(&report, name);
            let honest = honest_connections(&report, &scenario);
            assert!(!honest.is_empty(), "{name}: no honest peer connected");
            for entry in report.log.entries() {
                match &entry.obs {
                    Observation::ConnectionClosed { connection, cause }
                        if honest.contains(connection) =>
                    {
                        assert!(!is_fault_cause(cause), "{name}: closed with {cause}");
                    }
                    Observation::Misbehavior { connection } => {
                        assert!(!honest.contains(connection), "{name}: Misbehavior");
                    }
                    _ => {}
                }
            }
            if name == "redial" {
                assert_eq!(honest.len(), 2, "the redial is a second connection");
            }
            if name == "bystander" {
                let faulted = report.log.entries().iter().any(|entry| {
                    matches!(&entry.obs, Observation::ConnectionClosed { connection, cause }
                        if !honest.contains(connection) && is_fault_cause(cause))
                });
                assert!(faulted, "the node closed the adversary's connection");
            }
        }
    }
}

/// `HonestPeer` marks each peer that runs the honest script and nothing
/// else, and the scenario declares it with their names. A scenario with no
/// honest peer declares no `HonestPeer`, so the property is
/// `NotApplicable` there.
#[test]
fn honest_marks_follow_the_peer_specs() {
    let honest_peer = honest(PeerSpec::dialing(), PRELOAD, None);
    let mut adversary = act_scenario(Act::ShortPayload { row: Row::Status });
    assert!(adversary.honest_peers().is_empty());
    let declared = HeaderWire::assumptions(&adversary);
    assert!(declared
        .records()
        .iter()
        .all(|record| record.assume != Assume::HonestPeer));
    adversary.peers.extend([
        honest_peer.clone(),
        PeerSpec {
            bad_nonce_echo: true,
            ..honest_peer.clone()
        },
        PeerSpec {
            script: vec![PeerAction::Open {
                kind: 9,
                version: 1,
            }],
            ..honest_peer.clone()
        },
        PeerSpec::dialing(),
        PeerSpec {
            script: vec![PeerAction::Close, PeerAction::Redial],
            ..honest_peer
        },
    ]);
    assert_eq!(adversary.honest_peers(), BTreeSet::from([1, 5]));
    let declared = HeaderWire::assumptions(&adversary);
    assert_eq!(
        declared.records().last(),
        Some(&AssumeRecord {
            assume: Assume::HonestPeer,
            params: serde_json::json!({ "peers": ["peer-1", "peer-5"] }),
        })
    );
}

/// The scripted history that violates the generated ID `id` on connection
/// 1, one batch per step: a fault the node never punishes, a punishment of
/// a conformant peer, or a node frame the peer's models refuse.
fn bypass(id: &GeneratedId<'_>) -> Vec<Vec<Observation>> {
    let row = id.row.map(str::to_owned);
    let peer = |label: Label, exhausted: bool| Observation::PeerFrame {
        connection: 1,
        stream: STREAM,
        quic_stream: Some(4),
        seq: 0,
        row: if label == Label::UndeclaredType {
            None
        } else {
            row.clone().or(Some("status".to_owned()))
        },
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
            quic_stream: Some(4),
            seq: 0,
            row: row.clone(),
            rule_stream: STREAM,
            label,
            exhausted,
            exchange: None,
        }]]
    };
    let judged = Observation::FrameJudged {
        connection: 1,
        stream: STREAM,
        quic_stream: Some(4),
        seq: 0,
    };
    let fault_close = Observation::ConnectionClosed {
        connection: 1,
        cause: "service_protocol_reject".to_owned(),
    };
    match id.template {
        Template::Closes(label) => vec![vec![peer(label, false)], vec![judged]],
        Template::ConformantNeverCloses => vec![
            vec![peer(Label::Conformant, false)],
            vec![judged, fault_close],
        ],
        Template::CadenceNeverCloses => vec![
            vec![peer(Label::Conformant, true)],
            vec![judged, fault_close],
        ],
        Template::SenderWithinLive => node(Label::OverLive, false),
        Template::SenderWithinCredit => node(Label::OverCredit, false),
        Template::SenderAnswersOpen | Template::SenderEndsOnce => node(Label::Unsolicited, false),
        Template::SenderCadence => node(Label::Conformant, true),
        template => panic!("header sync generates no {template:?}"),
    }
}

/// Every generated header-sync ID fails on a scripted history that
/// bypasses its rule: a fault the node never punishes, a punishment of a
/// conformant peer, and node frames the peer's models refuse.
#[test]
fn scripted_bypasses_violate_every_generated_header_id() {
    let properties = || -> Vec<Property<Node>> {
        let mut properties = table_properties(FAMILY, &RegtestMessage::LAYOUT);
        properties.extend(sender_properties(FAMILY, &RegtestMessage::LAYOUT));
        properties
    };
    for property in properties() {
        let name = property.id.to_string();
        let parsed = GeneratedId::parse(&name).expect("a generated ID parses back");
        let report = run_under(properties(), OBSERVED, bypass(&parsed));
        assert!(
            matches!(outcome(&report, &name), PropertyOutcome::Violated { .. }),
            "{name}: {:?}",
            report.properties
        );
    }
}

/// A scripted harness whose scenario marks peer 0 `HonestPeer`, or no peer.
struct Marked(BTreeSet<usize>);

/// [`Marked`]'s view: its honest peers.
struct MarkedView(BTreeSet<usize>);

impl HonestPeers for MarkedView {
    fn is_honest(&self, index: usize) -> bool {
        self.0.contains(&index)
    }
}

impl HasView for Marked {
    type View = MarkedView;

    /// The honest peers.
    fn view(&self) -> MarkedView {
        MarkedView(self.0.clone())
    }
}

/// Runs `script` with peer 0 marked `HonestPeer` if `marked`, under
/// `session/honest-never-punished`, after header sync's generated receiver
/// properties if `table`.
fn run_marked(marked: bool, table: bool, script: Vec<Vec<Observation>>) -> RunReport {
    let honest: BTreeSet<usize> = if marked { [0].into() } else { [].into() };
    let mut properties: Vec<Property<Marked>> = if table {
        table_properties(FAMILY, &RegtestMessage::LAYOUT)
    } else {
        Vec::new()
    };
    properties.push(honest_never_punished());
    let assumptions = Assumptions::declare(vec![AssumeRecord {
        assume: Assume::HonestPeer,
        params: serde_json::json!({ "peers": ["peer-0"] }),
    }]);
    let mut observed = OBSERVED.to_vec();
    observed.push(Source::View);
    run_scripted(
        Rc::new(Marked(honest)),
        properties,
        assumptions,
        &observed,
        script,
    )
}

/// The node's transport registered connection 1 from peer 0's address.
fn registered() -> Observation {
    Observation::Transport {
        instance: 0,
        seq: 1,
        node: "node".to_owned(),
        connection: Some(1),
        remote: peer_addr(0, 0).to_string(),
        event: TransportFact::Registered,
    }
}

/// Peer 0's frame on connection 1 with `label`, then its judgement.
fn labelled(label: Label) -> [Observation; 2] {
    [
        Observation::PeerFrame {
            connection: 1,
            stream: STREAM,
            quic_stream: Some(4),
            seq: 0,
            row: Some("status".to_owned()),
            rule_stream: STREAM,
            label,
            exhausted: false,
            exchange: None,
            payload: 9,
        },
        Observation::FrameJudged {
            connection: 1,
            stream: STREAM,
            quic_stream: Some(4),
            seq: 0,
        },
    ]
}

/// A close of connection 1 with `cause`.
fn closed(cause: &str) -> Observation {
    Observation::ConnectionClosed {
        connection: 1,
        cause: cause.to_owned(),
    }
}

/// Two punishments of an honest peer at one step, a fault close of its
/// connection 1 and a `Misbehavior` against its connection 2, are two
/// violations, and the report keeps both.
#[test]
fn every_punishment_of_an_honest_peer_at_a_step_is_reported() {
    let second = Observation::Transport {
        instance: 0,
        seq: 2,
        node: "node".to_owned(),
        connection: Some(2),
        remote: peer_addr(0, 1).to_string(),
        event: TransportFact::Registered,
    };
    let report = run_marked(
        true,
        false,
        vec![
            vec![registered(), second],
            vec![
                closed("ordered_read_rejected"),
                Observation::Misbehavior { connection: 2 },
            ],
        ],
    );
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    let honest = report
        .properties
        .iter()
        .find(|property| property.property == HONEST_NEVER_PUNISHED)
        .expect("registered");
    let found = |expected: &str, actual: &str| Violation {
        property: "session/honest-never-punished".to_owned(),
        step: 2,
        witness: vec![2],
        expected: expected.to_owned(),
        actual: actual.to_owned(),
    };
    assert_eq!(
        honest.violations,
        [
            found(
                "honest peer-0's connection 1 is never punished",
                "a close with cause ordered_read_rejected"
            ),
            found(
                "honest peer-0's connection 2 is never punished",
                "a Misbehavior"
            ),
        ]
    );
}

/// Scripted punishments of an honest peer violate
/// `session/honest-never-punished`, even where a label excuses them: a
/// fault close after a frame its models label `short-payload`, which
/// `conformant-never-closes` accepts, and a `Misbehavior`. The same
/// histories for an unmarked peer, and a neutral close of an honest one,
/// hold. A punished connection no transport row names is missing evidence.
#[test]
fn scripted_punishments_of_an_honest_peer_violate_honest_never_punished() {
    let [frame, judged] = labelled(Label::ShortPayload);
    // `status/short-payload-closes` holds: the fault close discharges it.
    // `conformant-never-closes` holds: the label excuses the close.
    let excused = vec![
        vec![registered()],
        vec![frame],
        vec![judged, closed("ordered_read_rejected")],
    ];
    let misbehavior = vec![
        vec![registered()],
        vec![Observation::Misbehavior { connection: 1 }],
    ];
    for (name, script) in [("excused close", excused), ("misbehavior", misbehavior)] {
        let report = run_marked(true, true, script.clone());
        assert!(
            matches!(
                outcome(&report, HONEST_NEVER_PUNISHED),
                PropertyOutcome::Violated { .. }
            ),
            "{name}: {:?}",
            report.properties
        );
        assert_eq!(report.end.harness, None, "{name}: {:?}", report.end);
        let unmarked = run_marked(false, true, script);
        assert_eq!(
            *outcome(&unmarked, HONEST_NEVER_PUNISHED),
            PropertyOutcome::not_applicable(NaReason::NeverApplied),
            "{name}"
        );
    }
    let neutral = vec![vec![registered()], vec![closed("ordered_read_timeout")]];
    let report = run_marked(true, true, neutral);
    assert!(
        matches!(
            outcome(&report, HONEST_NEVER_PUNISHED),
            PropertyOutcome::Held { applicable } if *applicable > 0
        ),
        "{:?}",
        report.properties
    );
    assert_eq!(
        *outcome(&report, &id("conformant-never-closes")),
        PropertyOutcome::not_applicable(NaReason::NeverApplied)
    );
    let report = run_marked(true, false, vec![vec![closed("ordered_read_rejected")]]);
    assert!(
        report.end.harness.is_some(),
        "a punishment with no transport row is missing evidence: {:?}",
        report.end
    );
}

/// A scripted history as a harness, with peer 0 marked `HonestPeer`, so a
/// witness test judges `session/honest-never-punished`, whose trigger reads
/// the view, on it.
struct HonestScript;

/// [`HonestScript`]'s histories.
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
enum Punished {
    /// The node reports a `Misbehavior` against honest peer 0's connection
    /// before it judges any frame there.
    BeforeJudging,
    /// The node fault-closes honest peer 0's connection before it judges any
    /// frame there.
    ClosedBeforeJudging,
    /// The node judges peer 0's conformant frame, and punishes no one.
    Judged,
}

impl Punished {
    /// The history, one batch per step.
    fn script(self) -> Vec<Vec<Observation>> {
        match self {
            Punished::BeforeJudging => vec![
                vec![registered()],
                vec![Observation::Misbehavior { connection: 1 }],
            ],
            Punished::ClosedBeforeJudging => {
                vec![vec![registered()], vec![closed("ordered_read_rejected")]]
            }
            Punished::Judged => {
                let [frame, judged] = labelled(Label::Conformant);
                vec![vec![registered()], vec![frame], vec![judged]]
            }
        }
    }
}

impl HasView for HonestScript {
    type View = MarkedView;

    /// Peer 0, the one honest peer.
    fn view(&self) -> MarkedView {
        MarkedView([0].into())
    }
}

impl Subsystem for HonestScript {
    const NAME: &'static str = "test/honest-script";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = Punished;
    type Prepared = ();

    fn config(_: &Punished) -> zakura_sim::RunConfig {
        support::config(1, 1_000)
    }

    /// Nothing to set up.
    fn prepare(_: &Punished) {}

    fn build(sim: &mut zakura_sim::Sim, history: &Punished, (): ()) -> Self {
        support::script::feed(sim, history.script());
        HonestScript
    }

    fn sources() -> zakura_sim::property::Sources {
        let mut observed = OBSERVED.to_vec();
        observed.push(Source::View);
        zakura_sim::property::Sources::observed(&observed)
    }

    fn assumptions(_: &Punished) -> Assumptions<Self> {
        Assumptions::declare(vec![AssumeRecord {
            assume: Assume::HonestPeer,
            params: serde_json::json!({ "peers": ["peer-0"] }),
        }])
    }

    fn properties() -> Vec<Property<Self>> {
        vec![honest_never_punished()]
    }

    fn draw(_: &[u8]) -> Punished {
        Punished::Judged
    }
}

/// The witness judge counts a punishment of an honest peer before the node
/// judged any frame on its connection as `session/honest-never-punished`'s
/// trigger history, so that catch isn't lost: a `Misbehavior` and a fault
/// close, each before any `FrameJudged`. A judged frame with no punishment
/// still reaches it on a holding history.
#[test]
fn a_punishment_before_any_judgement_reaches_the_honest_trigger() {
    let judged = |history, expect| {
        zakura_sim::witness::witness::<HonestScript>(
            zakura_sim::witness::WitnessScenario {
                scenario: history,
                input: Plan::from_fuzz(&[1; 64]),
            },
            HONEST_NEVER_PUNISHED,
            expect,
        )
    };
    for history in [Punished::BeforeJudging, Punished::ClosedBeforeJudging] {
        assert_eq!(judged(history, Expect::Violated), Ok(()), "{history:?}");
    }
    assert_eq!(judged(Punished::Judged, Expect::Holds), Ok(()));
}

/// The committed Plan bytes every typed planted input below runs under:
/// none, so each step takes its first action by identity, as the default
/// Plan does.
const DEFAULT_PLAN: &str = "header-wire-default.plan";

/// The committed scenarios of the generated IDs' typed planted cases
/// (hdr-14.5), by fixture name `header-wire-planted-<name>.json`: an act's
/// focused scenario ([`act_scenario`]) for each act no saved Plan takes, an
/// adversary serving the node its chain and the held subscription, both
/// with no act, the node serving an honest subscriber, and two reserved
/// flags `Status` frames over fixed 0 ms links, the two-finding case's.
/// The saved Plans' inputs serve the other cases.
fn planted_scenarios() -> Vec<(&'static str, WireScenario)> {
    let act = |name, act| (name, act_scenario(act));
    let mut held = act_scenario(Act::MinPayload {
        row: Row::SubscribeHeaders,
    });
    held.peers[0].script.clear();
    let mut serving = act_scenario(Act::ExtraEnding);
    serving.peers[0].script.clear();
    let reserved = Act::ReservedFlags {
        row: Row::Status,
        bit: 0,
    };
    let mut twice = act_scenario(reserved);
    twice.peers[0].script = vec![PeerAction::Act { act: reserved }; 2];
    // Fixed and in order, so the script alone decides when each frame
    // arrives.
    twice.net.default_link = LinkSpec {
        min_delay_ms: 0,
        max_delay_ms: 0,
        reorder: false,
        ..twice.net.default_link
    };
    vec![
        act(
            "status-bad-content",
            Act::BadContent {
                rule: ContentRule::StatusAnchor,
            },
        ),
        act("status-burst", Act::StatusBurst),
        act(
            "subscribe-headers-reserved-flags",
            Act::ReservedFlags {
                row: Row::SubscribeHeaders,
                bit: 0,
            },
        ),
        act(
            "subscribe-headers-short-payload",
            Act::ShortPayload {
                row: Row::SubscribeHeaders,
            },
        ),
        act(
            "subscribe-headers-long-payload",
            Act::LongPayload {
                row: Row::SubscribeHeaders,
            },
        ),
        act(
            "subscribe-headers-bad-content",
            Act::BadContent {
                rule: ContentRule::OpenLocators,
            },
        ),
        act(
            "headers-reserved-flags",
            Act::ReservedFlags {
                row: Row::Headers,
                bit: 0,
            },
        ),
        act(
            "headers-short-payload",
            Act::ShortPayload { row: Row::Headers },
        ),
        act(
            "headers-outcome-reserved-flags",
            Act::ReservedFlags {
                row: Row::HeadersOutcome,
                bit: 0,
            },
        ),
        act(
            "headers-outcome-short-payload",
            Act::ShortPayload {
                row: Row::HeadersOutcome,
            },
        ),
        act(
            "headers-outcome-long-payload",
            Act::LongPayload {
                row: Row::HeadersOutcome,
            },
        ),
        act(
            "headers-outcome-unsolicited",
            Act::Unsolicited {
                row: Row::HeadersOutcome,
            },
        ),
        act(
            "headers-outcome-bad-content",
            Act::BadContent {
                rule: ContentRule::OutcomeWindow,
            },
        ),
        ("serving-adversary", serving),
        ("held-subscription", held),
        ("served-subscription", served_subscription()),
        ("status-reserved-flags-twice", twice),
    ]
}

/// The node at 120 serving an honest peer at 50, which dials it and
/// subscribes from genesis with its grants sent at once. The node never
/// subscribes, so every frame of its subscription rows is its publisher's.
fn served_subscription() -> WireScenario {
    WireScenario {
        node: Chain::main(120),
        peers: vec![honest(
            PeerSpec::dialing(),
            PRELOAD,
            Some(SubscribeSpec {
                start: 0,
                open: CreditSpec {
                    objects: 250,
                    bytes: 65_750,
                },
                grants: GrantPolicy::Immediate,
            }),
        )],
        policy: Policy::converging(),
        ..WireScenario::default()
    }
}

/// The committed fixture file `name`.
fn fixture_file(name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

/// `scenario` as its committed JSON.
fn scenario_json(scenario: &WireScenario) -> String {
    serde_json::to_string_pretty(scenario).expect("scenarios serialize") + "\n"
}

/// The committed typed planted inputs are what their builders give, and
/// their Plan bytes are empty.
///
/// This fails if a builder or the scenario format changes; after a
/// deliberate change, run `regenerate_planted_scenarios`.
#[test]
fn planted_scenarios_match_their_builders() {
    let read = |name: &str| {
        std::fs::read_to_string(fixture_file(name))
            .unwrap_or_else(|error| panic!("{name}: {error}"))
    };
    for (name, scenario) in planted_scenarios() {
        let file = format!("header-wire-planted-{name}.json");
        assert_eq!(
            read(&file),
            scenario_json(&scenario),
            "run `regenerate_planted_scenarios`"
        );
    }
    assert_eq!(read(DEFAULT_PLAN), "");
}

/// Rewrites the typed planted inputs and their empty Plan bytes. Run it
/// with `RUSTFLAGS="--cfg zakura_sim" cargo test -p zakura-sim --test
/// header_wire_witness -- --ignored regenerate_planted_scenarios`.
#[test]
#[ignore = "rewrites the committed planted scenarios"]
fn regenerate_planted_scenarios() {
    for (name, scenario) in planted_scenarios() {
        let file = fixture_file(&format!("header-wire-planted-{name}.json"));
        std::fs::write(file, scenario_json(&scenario)).expect("the fixture directory exists");
    }
    std::fs::write(fixture_file(DEFAULT_PLAN), "").expect("the fixture directory exists");
}

/// C21 for the generated receiver checks: reserved flags' two-frame planted
/// case's patched `Draft` run keeps two distinct findings of the ID at one
/// boundary, through the real reader and registry. The case's patch is
/// applied in a scratch worktree of `HEAD` and built as the planted runner
/// builds it ([`Worktree::build_patched`]), and that build records the
/// case's scripted input as a `Draft` run of the ID, so the run goes on
/// past the first finding.
///
/// The reader skips both reserved-flags `Status` frames, each at its own
/// step, so the ID is the only one violated, and the run's full report
/// beside its trace holds two of its findings at one step, the quiescence
/// that concludes both open closes, each naming its own frame's step. A
/// fresh replay by the same build matches exactly.
#[test]
fn two_skipped_reserved_flags_frames_keep_two_findings_at_one_boundary() {
    use serde_json::Value;
    const ID: &str = "header-sync/s5v9/status/reserved-flags-closes";
    let case =
        "qa/planted/header-sync/s5v9/status/reserved-flags-closes/two-reserved-flags-skipped";
    let work = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("reserved-flags-twice");
    let worktree = Worktree::of_head(&work);
    let patched = worktree.build_patched(&format!("{case}.patch"));
    let file: toml::Table = toml::from_str(
        &std::fs::read_to_string(worktree.path.join(format!("{case}.toml")))
            .expect("the case file reads"),
    )
    .expect("a case file");
    let typed = file["typed"].as_table().expect("a typed case");
    let input = |key: &str| worktree.path.join(typed[key].as_str().expect("a path"));
    let out = work.join("draft");
    if out.exists() {
        std::fs::remove_dir_all(&out).expect("the old recording is removable");
    }
    run_ok(
        std::process::Command::new(&patched)
            .args(["record", "--sim", HeaderWire::NAME, "--scenario"])
            .arg(std::fs::read_to_string(input("scenario")).expect("the scenario reads"))
            .arg("--script")
            .arg(input("script"))
            .args(["--draft", ID, "--out"])
            .arg(&out),
    );
    let report: Value = serde_json::from_str(
        &std::fs::read_to_string(out.join(zakura_sim::record::FULL_REPORT_FILE))
            .expect("the run wrote its full report"),
    )
    .expect("the full report is JSON");
    let properties = report["properties"]
        .as_array()
        .expect("the report lists each property");
    let violated: Vec<&str> = properties
        .iter()
        .filter(|property| property["outcome"].get("violated").is_some())
        .filter_map(|property| property["property"].as_str())
        .collect();
    assert_eq!(violated, [ID], "only the skipped frames' ID is violated");
    let findings = properties
        .iter()
        .find(|property| property["property"] == ID)
        .and_then(|property| property["violations"].as_array())
        .expect("the ID lists its findings");
    let [first, second] = &findings[..] else {
        panic!("two findings: {findings:?}");
    };
    assert_eq!(first["step"], second["step"], "both at one boundary");
    let opened = |finding: &Value| finding["witness"][0].as_u64().expect("an opening step");
    assert!(
        opened(first) < opened(second) && opened(second) < first["step"].as_u64().unwrap(),
        "each opened at its own frame's step, before the boundary: {findings:?}"
    );
    assert_ne!(first["actual"], second["actual"], "two distinct findings");
    run_ok(std::process::Command::new(&patched).arg("replay").arg(&out));
}
