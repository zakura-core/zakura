//! disc-3.1: `discovery/wire` registers discovery's generated receiver
//! properties over discovery's own stream, and its peer's models label a
//! `Peers` over its request's own cap `over-owed`.
#![cfg(zakura_sim)]

use zakura_network::zakura::{
    wire_codec::WireMessage, DiscoveryMessage, MAX_NODE_RECORD_BYTES, MSG_DISCOVERY_PEERS,
};
use zakura_sim::{
    harness::{
        discovery_service::{act_scenario, DiscoveryWire, WireScenario, FAMILY, STREAMS},
        run_prepared, HarnessError, Subsystem,
    },
    model::{is_fault_cause, Label},
    observe::TransportFact,
    peer::discovery::{discovery_stream, DiscoveryAct, DiscoveryRow},
    property::{Source, Status},
    table::table_properties,
    witness::{judge_report, Expect},
    MemorySink, Observation, Plan, RunReport,
};

/// The node's `GetPeers` limit in the over-owed run, under production's 32.
const LIMIT: usize = 1;

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

/// The properties name discovery's stream as the service declares it: its
/// kind, version and table.
#[test]
fn the_registered_stream_is_the_services() {
    let [registered] = STREAMS;
    let declared = discovery_stream();
    assert_eq!(
        (registered.kind, registered.version),
        (declared.kind, declared.version)
    );
    assert_eq!(registered.messages, declared.messages);
    assert_eq!(
        registered.messages,
        Some(<DiscoveryMessage as WireMessage>::RULES)
    );
}

/// The harness registers discovery's 31 generated receiver IDs and no
/// other property, each a `Draft` that reads only the peers' frames and
/// the node's closes. No sender or downstream template is registered.
#[test]
fn the_harness_registers_the_generated_discovery_ids() {
    let expected: Vec<String> = table_properties::<DiscoveryWire>(FAMILY, &STREAMS)
        .into_iter()
        .map(|property| property.id.to_string())
        .collect();
    // Two per stream, then each row's: hello's five, get-peers' and
    // get-services' six, peers' and services' six.
    assert_eq!(expected.len(), 31, "{expected:?}");
    let registered = DiscoveryWire::properties();
    let ids: Vec<String> = registered
        .iter()
        .map(|property| property.id.to_string())
        .collect();
    assert_eq!(ids, expected);
    for property in &registered {
        assert_eq!(property.status, Status::Draft, "{}", property.id);
        assert_eq!(
            property.needs,
            &[Source::Frames, Source::Closes],
            "{}",
            property.id
        );
        assert!(property.id.to_string().starts_with("discovery/s4v2/"));
    }
}

/// #2114's audit: the node checks a `Peers` against its own request's cap,
/// `peers_cap(limit)`, which is under the row's maximum when its limit is
/// under 32. A node asking for [`LIMIT`] record gets a `Peers` one byte
/// over that cap and under the row's maximum. The peer's models label it
/// `over-owed`, not `Conformant` and not `long-payload`, the node closes
/// the connection with a fault cause at the frame's header, and
/// `conformant-never-closes` applies and holds, as `peers/over-owed-closes`
/// does.
///
/// This fails if the models owe a `Peers` the row's largest ending again.
#[test]
fn a_peers_over_its_requests_cap_is_over_owed() {
    let act = DiscoveryAct::OverOwed {
        row: DiscoveryRow::Peers,
    };
    let scenario = act_scenario(act);
    assert_eq!(scenario.node.peer_sample_limit, Some(LIMIT));
    let report = run(&scenario);
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    assert_eq!(report.end.violation, None, "{:?}", report.end);
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
    for id in [
        "discovery/s4v2/peers/over-owed-closes",
        "discovery/s4v2/conformant-never-closes",
    ] {
        if let Err(error) = judge_report::<DiscoveryWire>(&report, id, Expect::Applicable) {
            panic!("{id}: {error}: {:?}", report.properties);
        }
    }
}
