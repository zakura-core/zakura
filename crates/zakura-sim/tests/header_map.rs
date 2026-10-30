//! The supervisor map's boundary read on `header-sync/wire` (M6a-7b): the
//! handler's registered connections, read at every step boundary with no
//! scheduling point, each by its own `conn_id`. These only build with
//! `RUSTFLAGS="--cfg zakura_sim"`, in a separate `CARGO_TARGET_DIR`.
#![cfg(zakura_sim)]

mod support;

use std::collections::BTreeMap;

use support::inputs;
use zakura_sim::{
    harness::{
        header_node::{reads::without_reads, Chain, Policy},
        header_sync::{HeaderWire, WireScenario},
        run_prepared, HarnessError, Subsystem,
    },
    observe::{ComponentReading, ComponentValue, MapRow, TransportFact},
    peer::{honest::HonestSpec, peer_addr, PeerAction, PeerSpec},
    MemorySink, Observation, Plan, PlanInput, RunReport,
};

/// `peer-0`, an honest peer with 120 headers, dials the node at 50,
/// closes, and dials again from its next address: M5's replacement
/// history, with no holds.
fn replacement() -> WireScenario {
    WireScenario {
        node: Chain::main(50),
        peers: vec![PeerSpec {
            honest: Some(HonestSpec {
                chain: 120,
                subscribe: None,
            }),
            script: vec![PeerAction::Close, PeerAction::Redial],
            ..PeerSpec::dialing()
        }],
        policy: Policy::converging(),
        ..WireScenario::default()
    }
}

/// The plans these tests run: all defaults, then two byte-chosen.
fn plans() -> Vec<PlanInput> {
    let mut plans = vec![Plan::from_fuzz(&[])];
    plans.extend(inputs(2, 16_384));
    plans
}

/// Runs `scenario` under `plan`, and returns the report, even of a run that
/// ended in a harness failure.
fn run(scenario: &WireScenario, plan: PlanInput) -> RunReport {
    let ports = HeaderWire::prepare(scenario);
    let mut sink = MemorySink::default();
    match run_prepared::<HeaderWire>(scenario, ports, plan, &mut sink, None) {
        Ok(report) => report,
        Err(HarnessError::Run { report, .. }) => *report,
        Err(error) => panic!("{error}"),
    }
}

/// The map's logged reads, with their steps.
fn map_reads(report: &RunReport) -> Vec<(u64, &ComponentReading)> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::ComponentRead {
                component, read, ..
            } if component == "SupervisorMap" => Some((entry.step, read)),
            _ => None,
        })
        .collect()
}

/// A map read's entries, if it's current.
fn entries(read: &ComponentReading) -> Option<&[MapRow]> {
    match read {
        ComponentReading::Current(ComponentValue::SupervisorMap { entries }) => Some(entries),
        _ => None,
    }
}

/// Each registered connection's remote address, from its `registered`
/// row, with the row's place in the log.
fn registered(report: &RunReport) -> BTreeMap<u64, (String, usize)> {
    report
        .log
        .entries()
        .iter()
        .enumerate()
        .filter_map(|(at, entry)| match &entry.obs {
            Observation::Transport {
                connection: Some(connection),
                remote,
                event: TransportFact::Registered,
                ..
            } => Some((*connection, (remote.clone(), at))),
            _ => None,
        })
        .collect()
}

/// Every task stop the run logged, with its step, op, object and site.
fn stops(report: &RunReport) -> Vec<(u64, &Observation)> {
    report
        .log
        .entries()
        .iter()
        .filter(|entry| entry.obs.ran_task().is_some())
        .map(|entry| (entry.step, &entry.obs))
        .collect()
}

/// M5's replacement history: `peer-0`'s two connections each show in the
/// map under their own `conn_id`, with the IP that connection registered
/// from, and never together. The first's `registry-removed` comes before
/// the second registers. The run first shows both connections registered,
/// from `peer-0`'s first and second addresses, and the map reading each.
///
/// This fails if a map entry is folded by peer, which would give the second
/// connection the first one's `conn_id`.
#[test]
fn one_peer_two_connections_stay_apart() {
    let scenario = replacement();
    for plan in plans() {
        let report = run(&scenario, plan);
        assert_eq!(report.end.harness, None, "{:?}", report.end);
        let registered = registered(&report);
        let remotes: Vec<&str> = registered
            .values()
            .map(|(remote, _)| remote.as_str())
            .collect();
        assert_eq!(
            remotes,
            [peer_addr(0, 0).to_string(), peer_addr(0, 1).to_string()],
            "both connections register"
        );
        let (first, second) = {
            let mut connections = registered.keys().copied();
            (
                connections.next().expect("two connections"),
                connections.next().expect("two connections"),
            )
        };

        let reads = map_reads(&report);
        let mut shown: Vec<u64> = Vec::new();
        let mut peers = Vec::new();
        for (step, read) in &reads {
            let Some(entries) = entries(read) else {
                continue;
            };
            assert!(entries.len() <= 1, "step {step}: one connection at a time");
            for entry in entries {
                let (remote, _) = registered
                    .get(&entry.connection)
                    .unwrap_or_else(|| panic!("step {step}: {entry:?} names a registration"));
                let ip = remote.rsplit_once(':').map(|(ip, _)| ip);
                assert_eq!(
                    entry.remote_ip.as_deref(),
                    ip,
                    "step {step}: the entry keeps its own connection's IP"
                );
                if shown.last() != Some(&entry.connection) {
                    shown.push(entry.connection);
                }
                peers.push(entry.peer.clone());
            }
        }
        assert_eq!(shown, [first, second], "the map reads each connection");
        peers.dedup();
        assert_eq!(peers.len(), 1, "one peer: {peers:?}");

        let removed = report.log.entries().iter().position(|entry| {
            matches!(
                &entry.obs,
                Observation::Transport {
                    connection: Some(connection),
                    event: TransportFact::RegistryRemoved { .. },
                    ..
                } if *connection == first
            )
        });
        let (_, second_registered) = registered[&second];
        assert!(
            removed.is_some_and(|removed| removed < second_registered),
            "the first connection's services are removed before the second registers"
        );
    }
}

/// The map's reads add no scheduling point: under each plan, the
/// replacement history takes the same picks and task stops, with each
/// stop's op, object and site, and ends the same, with the boundary reads
/// as without them. The runs with reads first show the map read current
/// with a connection in it, and held while a task holds the supervisor's
/// lock.
///
/// This fails if the read keeps the supervisor's lock past the read, or
/// reaches any scheduling point the scheduler records.
#[test]
fn map_reads_add_no_scheduling_point() {
    let scenario = replacement();
    for plan in plans() {
        let read = run(&scenario, plan.clone());
        let unread = without_reads(|| run(&scenario, plan));
        let reads = map_reads(&read);
        assert!(
            reads
                .iter()
                .any(|(_, read)| entries(read).is_some_and(|entries| !entries.is_empty())),
            "the map reads a connection"
        );
        assert!(
            reads
                .iter()
                .any(|(_, read)| matches!(read, ComponentReading::Held { .. })),
            "the map reads held"
        );
        assert!(map_reads(&unread).is_empty());
        assert_eq!(read.end, unread.end);
        assert_eq!(support::picks(&read), support::picks(&unread));
        assert_eq!(stops(&read), stops(&unread));
    }
}
