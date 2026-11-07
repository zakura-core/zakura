//! disc-3.1: witness scenarios for discovery's generated receiver
//! properties, which `discovery/wire` registers. Each ID applies and holds
//! in its witness, a run of the harness with one peer taking one
//! [`DiscoveryAct`], and no other ID is violated there. Conformant controls
//! show that `conformant-never-closes` and `cadence-never-closes` apply and
//! hold on frames the node must not punish.
#![cfg(zakura_sim)]

use std::collections::BTreeSet;

use zakura_network::zakura::{
    wire_codec::WireMessage, DiscoveryMessage, MAX_NODE_RECORD_BYTES, MSG_DISCOVERY_PEERS,
};
use zakura_sim::{
    harness::{
        discovery_service::{act_scenario, DiscoveryWire, WireScenario, FAMILY},
        run_prepared, HarnessError, Subsystem,
    },
    model::{is_fault_cause, Label},
    observe::{StreamId, TransportFact},
    peer::discovery::{DiscoveryAct, DiscoveryRow, DiscoveryRule},
    table::GeneratedId,
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

/// Runs `scenario` under the default Plan, and returns the report even of
/// a run that ended in a harness failure.
fn run(scenario: &WireScenario) -> RunReport {
    let node = DiscoveryWire::prepare(scenario);
    let ran = run_prepared::<DiscoveryWire>(
        scenario,
        node,
        Plan::from_fuzz(&[]),
        &mut MemorySink::default(),
        None,
    );
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
/// `conformant-never-closes`. Every registered property is a `Draft`,
/// whose violation doesn't stop the run, so this also requires that no
/// property is violated. The [`UNREACHED`] ID stays `NotApplicable`.
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
        witnessed(&report, act, &[reached(act)]);
        if act == OVER_OWED {
            over_owed_at_its_cap(&scenario, &report);
        }
    }
}

/// #2114's audit: the node checks a `Peers` against its own request's cap,
/// `peers_cap(limit)`, which is under the row's maximum when its limit is
/// under 32. In the over-owed witness, a node asking for [`LIMIT`] record
/// gets a `Peers` one byte over that cap and under the row's maximum. The
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
/// `conformant-never-closes`. The act witnesses run in one test per row,
/// below, so each test takes seconds.
///
/// This fails if a witness stops reaching its ID, or the node punishes the
/// honest peer.
#[test]
fn every_discovery_property_applies_in_its_witness() {
    let listed: Vec<String> = ACTS
        .into_iter()
        .map(reached)
        .chain(
            ["conformant-never-closes"]
                .iter()
                .chain(&UNREACHED)
                .map(|suffix| id(suffix)),
        )
        .collect();
    let distinct: BTreeSet<String> = listed.iter().cloned().collect();
    assert_eq!(distinct.len(), listed.len(), "{listed:?}");
    assert_eq!(distinct, generated().into_iter().collect());
    let report = run(&WireScenario::default());
    witnessed(&report, "honest", &[]);
    assert_eq!(fault_closes(&report), Vec::<String>::new());
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
/// run's `cadence-never-closes`.
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
/// request run's `cadence-never-closes`.
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
/// `cadence-never-closes`.
///
/// This fails if the node punishes a frame that local policy, not the
/// specification, refuses, or a request that finds the bucket empty.
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
        let report = run(&act_scenario(control));
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
