//! disc-3.1: witness scenarios for discovery's generated receiver and
//! sender properties, which `discovery/wire` registers. Each ID applies and holds
//! in its witness, a run of the harness with one peer taking one
//! [`DiscoveryAct`], and no other ID is violated there. Conformant controls
//! show that `conformant-never-closes` and `cadence-never-closes` apply and
//! hold on frames the node must not punish. `discovery/late-response-admitted`
//! applies and holds in the `late-response` saved Plan (disc-3.4).
#![cfg(zakura_sim)]

mod support;

use std::{
    collections::BTreeSet,
    future::Future,
    pin::pin,
    task::{Context, Poll, Waker},
};

use support::script::{run_under, OBSERVED};
use zakura_network::zakura::{
    wire_codec::WireMessage, DiscoveryMessage, MAX_NODE_RECORD_BYTES, MSG_DISCOVERY_PEERS,
};
use zakura_sim::{
    domain::discovery::LATE_RESPONSE_ADMITTED,
    harness::{
        discovery_service::{
            act_scenario, asking_scenario, wire_properties, DiscoveryWire, NodeHandle, SavedPlan,
            WireScenario, FAMILY, SCRIPTED,
        },
        run_prepared, HarnessError, Subsystem,
    },
    model::{is_fault_cause, Label},
    observe::{StreamId, TransportFact},
    peer::discovery::{relayed_node, DiscoveryAct, DiscoveryRow, DiscoveryRule, Request},
    table::{GeneratedId, Template},
    witness::{judge_report, Expect},
    MemorySink, Observation, Plan, PropertyOutcome, RunReport,
};

/// Discovery's stream at the version it tables.
const STREAM: StreamId = StreamId {
    kind: 4,
    version: 2,
};

/// The generated ID `discovery/s4v2/<suffix>`.
fn id(suffix: &str) -> String {
    format!("{FAMILY}/{STREAM}/{suffix}")
}

/// One act per `<label>-closes` ID discovery can reach, and the two
/// cadence runs. [`reached`] names each one's ID.
const ACTS: [DiscoveryAct; 29] = [
    DiscoveryAct::UndeclaredType { message_type: 0 },
    DiscoveryAct::ReservedFlags {
        row: DiscoveryRow::Hello,
        bit: 0,
    },
    DiscoveryAct::ShortPayload {
        row: DiscoveryRow::Hello,
    },
    DiscoveryAct::LongPayload {
        row: DiscoveryRow::Hello,
    },
    DiscoveryAct::BadContent {
        rule: DiscoveryRule::HelloSignature,
    },
    DiscoveryAct::HelloBurst,
    DiscoveryAct::ReservedFlags {
        row: DiscoveryRow::GetPeers,
        bit: 0,
    },
    DiscoveryAct::ShortPayload {
        row: DiscoveryRow::GetPeers,
    },
    DiscoveryAct::LongPayload {
        row: DiscoveryRow::GetPeers,
    },
    DiscoveryAct::OverMargin {
        row: DiscoveryRow::GetPeers,
    },
    DiscoveryAct::BadContent {
        rule: DiscoveryRule::GetPeersLimit,
    },
    DiscoveryAct::RequestRun {
        row: DiscoveryRow::GetPeers,
    },
    DiscoveryAct::ReservedFlags {
        row: DiscoveryRow::Peers,
        bit: 0,
    },
    DiscoveryAct::ShortPayload {
        row: DiscoveryRow::Peers,
    },
    DiscoveryAct::LongPayload {
        row: DiscoveryRow::Peers,
    },
    DiscoveryAct::Unsolicited {
        row: DiscoveryRow::Peers,
    },
    DiscoveryAct::OverOwed {
        row: DiscoveryRow::Peers,
    },
    DiscoveryAct::BadContent {
        rule: DiscoveryRule::PeersSignature,
    },
    DiscoveryAct::ReservedFlags {
        row: DiscoveryRow::GetServices,
        bit: 0,
    },
    DiscoveryAct::ShortPayload {
        row: DiscoveryRow::GetServices,
    },
    DiscoveryAct::LongPayload {
        row: DiscoveryRow::GetServices,
    },
    DiscoveryAct::OverMargin {
        row: DiscoveryRow::GetServices,
    },
    DiscoveryAct::BadContent {
        rule: DiscoveryRule::GetServicesWanted,
    },
    DiscoveryAct::RequestRun {
        row: DiscoveryRow::GetServices,
    },
    DiscoveryAct::ReservedFlags {
        row: DiscoveryRow::Services,
        bit: 0,
    },
    DiscoveryAct::ShortPayload {
        row: DiscoveryRow::Services,
    },
    DiscoveryAct::LongPayload {
        row: DiscoveryRow::Services,
    },
    DiscoveryAct::Unsolicited {
        row: DiscoveryRow::Services,
    },
    DiscoveryAct::BadContent {
        rule: DiscoveryRule::ServicesNodeId,
    },
];

/// The IDs the honest exchange reaches: the receiver partner and the
/// sender IDs of the node's own `Hello` and requests. The honest peer asks
/// nothing, so the node's answers wait for the request runs ([`also`]).
const HONEST: [&str; 6] = [
    "conformant-never-closes",
    "hello/sender-cadence",
    "get-peers/sender-within-limit",
    "get-peers/sender-cadence",
    "get-services/sender-within-limit",
    "get-services/sender-cadence",
];

/// The `sender-within-owed` IDs, which the node's answers to requests that
/// ask for less than everything reach
/// ([`answers_within_their_requests_hold_sender_within_owed`]).
const OWED: [(&str, Request); 2] = [
    ("peers/sender-within-owed", Request::GetOnePeer),
    ("services/sender-within-owed", Request::GetHeaderSyncService),
];

/// The generated ID no discovery scenario reaches. The node asks for
/// every service, so its `GetServices` owes the row's largest `Services`,
/// and a larger one is `long-payload`.
const UNREACHED: [&str; 1] = ["services/over-owed-closes"];

/// The ID `act`'s witness reaches: its label's `<label>-closes` on its
/// row, or a run's `cadence-never-closes`.
fn reached(act: DiscoveryAct) -> String {
    match act.label() {
        Label::Conformant => id(&format!("{}/cadence-never-closes", act.row().name())),
        Label::UndeclaredType => id("undeclared-type-closes"),
        label => id(&format!("{}/{}-closes", act.row().name(), label.name())),
    }
}

/// The IDs `act`'s witness also reaches: a request run's answers from the
/// node, which reach their row's `sender-ends-once`.
fn also(act: DiscoveryAct) -> Vec<String> {
    match act {
        DiscoveryAct::RequestRun { row } => {
            let answer = match row {
                DiscoveryRow::GetPeers => DiscoveryRow::Peers,
                _ => DiscoveryRow::Services,
            };
            vec![id(&format!("{}/sender-ends-once", answer.name()))]
        }
        _ => Vec::new(),
    }
}

/// Runs `scenario` under the default Plan, and returns the report even of
/// a run that ended in a harness failure.
fn run(scenario: &WireScenario) -> RunReport {
    run_node(scenario).0
}

/// Runs `scenario` as [`run`] does, and also returns the node's handle.
fn run_node(scenario: &WireScenario) -> (RunReport, NodeHandle) {
    run_plan(scenario, &[])
}

/// Runs `scenario` under the Plan `plan`, as [`run_node`] does.
fn run_plan(scenario: &WireScenario, plan: &[u8]) -> (RunReport, NodeHandle) {
    let node = DiscoveryWire::prepare(scenario);
    let ran = run_prepared::<DiscoveryWire>(
        scenario,
        node.clone(),
        Plan::from_fuzz(plan),
        &mut MemorySink::default(),
        None,
    );
    let report = match ran {
        Ok(report) => report,
        Err(HarnessError::Run { report, .. }) => *report,
        Err(error) => panic!("{error}"),
    };
    (report, node)
}

/// Whether the node's book holds a record for the author of the peer's
/// relayed record, read once the run has ended. Every task has stopped by
/// then, so the book's lock is free and the read finishes in one poll.
fn book_holds_relayed(node: &NodeHandle) -> bool {
    let handle = node.get().expect("the run built the node");
    let read = pin!(handle.record_for(relayed_node(0)));
    match read.poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(record) => record.is_some(),
        Poll::Pending => panic!("the book's lock is free after the run"),
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

/// Every generated ID `discovery/wire` registers, in order.
fn generated() -> Vec<String> {
    DiscoveryWire::properties()
        .into_iter()
        .map(|property| property.id.to_string())
        .filter(|registered| GeneratedId::parse(registered).is_some())
        .collect()
}

/// Requires `ids` to apply and hold in `report`, the run of `witness`, as
/// `applicable` witness tests judge them, and so does the stream's
/// `conformant-never-closes`. A `Draft` property's violation doesn't stop
/// the run, so this also requires that no property is violated. The
/// [`UNREACHED`] ID stays `NotApplicable`.
fn witnessed(report: &RunReport, witness: impl std::fmt::Debug, ids: &[String]) {
    assert_eq!(report.end.harness, None, "{witness:?}: {:?}", report.end);
    assert_eq!(report.end.violation, None, "{witness:?}: {:?}", report.end);
    for property in &report.properties {
        assert!(
            !matches!(property.outcome, PropertyOutcome::Violated { .. }),
            "{witness:?}: {}: {:?}",
            property.property,
            property.outcome
        );
    }
    for covered in ids.iter().chain([&id("conformant-never-closes")]) {
        if let Err(error) = judge_report::<DiscoveryWire>(report, covered, Expect::Applicable) {
            panic!("{witness:?}: {covered}: {error}: {:?}", report.properties);
        }
    }
    for suffix in UNREACHED {
        assert_eq!(
            *outcome(report, &id(suffix)),
            PropertyOutcome::NotApplicable,
            "{witness:?}: {suffix}"
        );
    }
}

/// The `over-owed` act on `peers`, the one whose witness also checks its
/// cap ([`over_owed_at_its_cap`]).
const OVER_OWED: DiscoveryAct = DiscoveryAct::OverOwed {
    row: DiscoveryRow::Peers,
};

/// The node's `GetPeers` limit in the over-owed witness, under production's
/// 32.
const LIMIT: usize = 1;

/// Runs the witness of each act in [`ACTS`] whose ID names `row`, and
/// requires its ID to apply and hold there ([`witnessed`]).
fn apply_on_row(row: DiscoveryRow) {
    let acts: Vec<DiscoveryAct> = ACTS
        .into_iter()
        .filter(|act| reached(*act).split('/').nth(2) == Some(row.name()))
        .collect();
    assert!(!acts.is_empty());
    for act in acts {
        let scenario = act_scenario(act);
        let report = run(&scenario);
        let mut ids = vec![reached(act)];
        ids.extend(also(act));
        witnessed(&report, act, &ids);
        if act == OVER_OWED {
            over_owed_at_its_cap(&scenario, &report);
        }
    }
}

/// #2114's audit: the node checks a `Peers` against its own request's cap,
/// `peers_cap(limit)`, which is under the row's maximum when its limit is
/// under 32. In the over-owed witness, a node asking for [`LIMIT`] record
/// gets a `Peers` one byte over that cap and under the row's maximum, two
/// validly signed records that decode. The
/// peer's models label it `over-owed`, not `Conformant` and not
/// `long-payload`. The node reads it, its claim of the answer refuses it,
/// and the node closes the connection with a fault cause.
///
/// This fails if the models owe a `Peers` the row's largest ending again.
fn over_owed_at_its_cap(scenario: &WireScenario, report: &RunReport) {
    assert_eq!(scenario.node.peer_sample_limit, Some(LIMIT));
    let peers = <DiscoveryMessage as WireMessage>::RULES
        .iter()
        .find(|rule| rule.message_type == u16::from(MSG_DISCOVERY_PEERS))
        .expect("discovery tables Peers");
    let cap = peers.payload.min() + LIMIT * MAX_NODE_RECORD_BYTES;
    assert!(cap < peers.payload.max(), "{cap}");
    let entries = report.log.entries();
    let (connection, seq) = entries
        .iter()
        .find_map(|entry| match &entry.obs {
            Observation::PeerFrame {
                connection,
                quic_stream: Some(_),
                seq,
                row: Some(row),
                label: Label::OverOwed,
                ..
            } if row == DiscoveryRow::Peers.name() => Some((*connection, *seq)),
            _ => None,
        })
        .expect("the act's Peers is over-owed");
    let read = entries.iter().find_map(|entry| match &entry.obs {
        Observation::Transport {
            connection: Some(on),
            event:
                TransportFact::HeaderRead {
                    seq: read,
                    message_type,
                    payload_len,
                    ..
                },
            ..
        } if (*on, *read, *message_type) == (connection, seq, u16::from(MSG_DISCOVERY_PEERS)) => {
            Some(*payload_len)
        }
        _ => None,
    });
    // A u32 fits usize on every supported target.
    assert_eq!(read.map(|len| len as usize), Some(cap + 1));
    let fault = entries.iter().any(|entry| {
        matches!(&entry.obs, Observation::ConnectionClosed { connection: on, cause }
            if *on == connection && is_fault_cause(cause))
    });
    assert!(fault, "the node admitted a Peers over its cap");
}

/// The node's fault closes in `report`, by cause.
fn fault_closes(report: &RunReport) -> Vec<String> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::ConnectionClosed { cause, .. } if is_fault_cause(cause) => {
                Some(cause.clone())
            }
            _ => None,
        })
        .collect()
}

/// The witnesses reach every generated ID but the [`UNREACHED`] one, each
/// act its own, and the honest exchange applies and holds
/// `conformant-never-closes` and every sender ID. The act witnesses run in one test per row,
/// below, so each test takes seconds.
///
/// This fails if a witness stops reaching its ID, or the node punishes the
/// honest peer.
#[test]
fn every_discovery_property_applies_in_its_witness() {
    let listed: Vec<String> = ACTS
        .into_iter()
        .map(reached)
        .chain(ACTS.into_iter().flat_map(also))
        .chain(HONEST.iter().chain(&UNREACHED).map(|suffix| id(suffix)))
        .chain(OWED.iter().map(|(suffix, ..)| id(suffix)))
        .collect();
    let distinct: BTreeSet<String> = listed.iter().cloned().collect();
    assert_eq!(distinct.len(), listed.len(), "{listed:?}");
    assert_eq!(distinct, generated().into_iter().collect());
    let report = run(&WireScenario::default());
    let honest: Vec<String> = HONEST.iter().map(|suffix| id(suffix)).collect();
    witnessed(&report, "honest", &honest);
    assert_eq!(fault_closes(&report), Vec::<String>::new());
}

/// The node's answers carry only what their requests asked for:
/// `sender-within-owed` applies and holds on a `Peers` that answers a
/// `GetPeers` for one record, and on a `Services` that answers a
/// `GetServices` for header sync alone, which the node doesn't run.
/// `services/sender-within-owed`'s planted case makes the node ignore the
/// wanted list, and the same scenario then violates the ID.
/// `peers/sender-within-owed` stays a `Draft`: every simulated peer binds
/// an RFC 1918 address, which the book's sampler refuses, so the `Peers`
/// answer carries no records.
///
/// This fails if the node's answer passes its request, or a witness stops
/// reaching its ID.
#[test]
fn answers_within_their_requests_hold_sender_within_owed() {
    for (suffix, request) in OWED {
        let report = run(&asking_scenario(request));
        witnessed(&report, request, &[id(suffix)]);
        assert_eq!(fault_closes(&report), Vec::<String>::new(), "{request:?}");
    }
}

/// `undeclared-type-closes` and each `hello` ID apply and hold in their
/// witnesses, with the `Hello` burst's `cadence-never-closes`.
#[test]
fn hello_acts_apply_in_their_witnesses() {
    witnessed(
        &run(&act_scenario(ACTS[0])),
        ACTS[0],
        &[id("undeclared-type-closes")],
    );
    apply_on_row(DiscoveryRow::Hello);
}

/// Each `get-peers` ID applies and holds in its witness, with the request
/// run's `cadence-never-closes` and the node's answers' `peers/sender-ends-once`.
#[test]
fn get_peers_acts_apply_in_their_witnesses() {
    apply_on_row(DiscoveryRow::GetPeers);
}

/// Each `peers` ID applies and holds in its witness. The `over-owed`
/// witness's node asks for one record, so its cap is under the row's
/// maximum, and its `Peers` is one byte over that cap
/// ([`over_owed_at_its_cap`]).
#[test]
fn peers_acts_apply_in_their_witnesses() {
    apply_on_row(DiscoveryRow::Peers);
}

/// Each `get-services` ID applies and holds in its witness, with the
/// request run's `cadence-never-closes` and the node's answers'
/// `services/sender-ends-once`.
#[test]
fn get_services_acts_apply_in_their_witnesses() {
    apply_on_row(DiscoveryRow::GetServices);
}

/// Each `services` ID applies and holds in its witness, and
/// `services/over-owed-closes` stays `NotApplicable` there.
#[test]
fn services_acts_apply_in_their_witnesses() {
    apply_on_row(DiscoveryRow::Services);
}

/// The conformant controls: an expired `Hello`, a `Peers` that relays an
/// expired record, and a `GetPeers` run inside the cadence capacity, its
/// last request finding the bucket empty. Each act's frames are
/// `Conformant`, the node judges them, and it records no fault close.
/// `conformant-never-closes` applies and holds, and so does the run's
/// `cadence-never-closes`. Each expired record expired past the node's
/// clock skew tolerance, so the node's expiry check refuses it. The node
/// drops the expired relayed record: its book doesn't hold it.
///
/// This fails if the node punishes a frame that local policy, not the
/// specification, refuses, or a request that finds the bucket empty, or if
/// it stores an expired relayed record.
#[test]
fn conformant_controls_hold_conformant_never_closes() {
    for control in [
        DiscoveryAct::ExpiredHello,
        DiscoveryAct::ExpiredRelay,
        DiscoveryAct::RequestRun {
            row: DiscoveryRow::GetPeers,
        },
    ] {
        assert_eq!(control.label(), Label::Conformant, "{control:?}");
        let (report, node) = run_node(&act_scenario(control));
        if control == DiscoveryAct::ExpiredRelay {
            assert!(
                !book_holds_relayed(&node),
                "the node stored an expired record"
            );
        }
        let ids = match control {
            DiscoveryAct::RequestRun { .. } => vec![reached(control)],
            _ => Vec::new(),
        };
        witnessed(&report, control, &ids);
        assert_eq!(fault_closes(&report), Vec::<String>::new(), "{control:?}");
        let labels: BTreeSet<&str> = report
            .log
            .entries()
            .iter()
            .filter_map(|entry| match &entry.obs {
                Observation::PeerFrame { stream, label, .. } if *stream == STREAM => {
                    Some(label.name())
                }
                _ => None,
            })
            .collect();
        assert_eq!(labels, BTreeSet::from(["conformant"]), "{control:?}");
    }
}

/// The scripted history that violates `id` on connection 1, one batch per
/// step: a fault the node never punishes.
fn bypass(id: &GeneratedId<'_>) -> Vec<Vec<Observation>> {
    let peer = |label: Label| Observation::PeerFrame {
        connection: 1,
        stream: STREAM,
        quic_stream: Some(4),
        seq: 0,
        row: id.row.map(str::to_owned),
        rule_stream: STREAM,
        label,
        exhausted: false,
        exchange: None,
        payload: 9,
    };
    let judged = Observation::FrameJudged {
        connection: 1,
        stream: STREAM,
        quic_stream: Some(4),
        seq: 0,
    };
    match id.template {
        Template::Closes(label) => vec![vec![peer(label)], vec![judged]],
        template => panic!("no scripted bypass covers {template:?}"),
    }
}

/// Each [`SCRIPTED`] ID fails on a scripted history that bypasses its
/// rule.
#[test]
fn scripted_bypasses_violate_the_unplanted_discovery_ids() {
    for name in SCRIPTED {
        let parsed = GeneratedId::parse(name).expect("a generated ID parses back");
        let report = run_under(wire_properties(), OBSERVED, bypass(&parsed));
        assert!(
            matches!(outcome(&report, name), PropertyOutcome::Violated { .. }),
            "{name}: {:?}",
            report.properties
        );
    }
}

/// The `late-response` saved Plan's committed input: its tag, then its
/// Plan.
const LATE_RESPONSE: &[u8] = include_bytes!("fixtures/discovery-wire-late-response.bin");

/// `discovery/late-response-admitted` applies and holds in the
/// `late-response` saved Plan, where the node reads the held `Peers` after
/// its initial wait timed out (`discovery_wire_lifecycle.rs` checks that
/// history). No property is violated, and the honest exchange, whose
/// answers come before the timeout, leaves it `NotApplicable`.
///
/// This fails if the node punishes a late answer, or if the Plan stops
/// delivering the `Peers` after the timeout.
#[test]
fn late_response_admitted_applies_in_its_saved_plan() {
    let (prefix, plan) = LATE_RESPONSE.split_at(DiscoveryWire::SCENARIO_BYTES);
    assert_eq!(SavedPlan::from_tag(prefix), Some(SavedPlan::LateResponse));
    let (report, _) = run_plan(&DiscoveryWire::draw(prefix), plan);
    witnessed(
        &report,
        "late-response",
        &[LATE_RESPONSE_ADMITTED.to_owned()],
    );
    assert_eq!(fault_closes(&report), Vec::<String>::new());
    assert_eq!(
        *outcome(&run(&WireScenario::default()), LATE_RESPONSE_ADMITTED),
        PropertyOutcome::NotApplicable
    );
}
