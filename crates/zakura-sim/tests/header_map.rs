//! The supervisor map's boundary read on `header-sync/wire` (M6a-7b): the
//! handler's registered connections, read at every step boundary with no
//! scheduling point, each by its own `conn_id`. These only build with
//! `RUSTFLAGS="--cfg zakura_sim"`, in a separate `CARGO_TARGET_DIR`.
#![cfg(zakura_sim)]

mod support;

use std::{collections::BTreeMap, sync::Arc};

use support::inputs;
use zakura_network::zakura::{
    TransportEvent, TransportObserver, ZAKURA_CAP_HEADER_SYNC, ZAKURA_STREAM_HEADER_SYNC,
};
use zakura_sim::{
    harness::{
        header_node::{
            policy::{Allow, Hold, OpKind, Select, Trigger},
            reads::without_reads,
            Chain, Policy,
        },
        header_sync::{wire_config, HeaderWire, WireScenario},
        run_prepared,
        wire_node::TransportTap,
        HarnessError, NetScenario, Subsystem,
    },
    observe::{ComponentReading, ComponentValue, MapRow, TransportFact},
    peer::{honest::HonestSpec, peer_addr, PeerAction, PeerSpec},
    HarnessFailure, MemorySink, NoChecks, Observation, Plan, PlanInput, RunReport, Sim,
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

/// `peer-0`, an honest peer with 120 headers, dials the node at 50.
fn honest() -> WireScenario {
    WireScenario {
        node: Chain::main(50),
        peers: vec![PeerSpec {
            honest: Some(HonestSpec {
                chain: 120,
                subscribe: None,
            }),
            ..PeerSpec::dialing()
        }],
        policy: Policy::converging(),
        ..WireScenario::default()
    }
}

/// The node at 50 dials `peer-0`, which holds the 1,200-header tree and
/// finishes its side of the header stream once every subscription has
/// ended, so the stream's session tears down and a new one reopens on the
/// same connection (`header_wire_view.rs`'s reopen).
fn reopen() -> WireScenario {
    let mut peer = PeerSpec {
        honest: Some(HonestSpec {
            chain: 1_200,
            subscribe: None,
        }),
        ..PeerSpec::listening()
    };
    peer.script = vec![PeerAction::FinishHeader];
    WireScenario {
        node: Chain::main(50),
        peers: vec![peer],
        policy: Policy {
            holds: vec![Hold {
                select: Select::nth(OpKind::Locator, 2),
                until: Trigger::VirtualTime(5_000),
                then: vec![Allow::Engine],
                deadline_ms: None,
            }],
            ..Policy::converging()
        },
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

/// Each registry record carries the services the registry reached, one
/// record per fanout call: every fanout's mask is within the mask its peer
/// carried, a connection's fanout reaches header sync exactly when its
/// header stream was admitted since its last fanout, every header
/// admission is followed by such a fanout, so each connection has as many
/// header fanouts as header admissions and none is left over, and each
/// `registry-removed` names the union of its connection's fanout masks,
/// the services its cleanup guard removes. The runs first show a fanout
/// that reaches header sync on the honest sync, a removal on the
/// replacement history's first connection, and, on the reopen history,
/// one connection fanning out header sync's mask twice.
///
/// This fails if a record names services the call didn't reach, or a call
/// makes no record, even one whose mask an earlier record already named.
#[test]
fn registry_records_carry_their_masks() {
    for (name, scenario) in [
        ("honest", honest()),
        ("replacement", replacement()),
        ("reopen", reopen()),
    ] {
        let report = run(&scenario, Plan::from_fuzz(&[]));
        assert_eq!(report.end.harness, None, "{name}: {:?}", report.end);
        let mut admitted: BTreeMap<u64, bool> = BTreeMap::new();
        let mut reached: BTreeMap<u64, u64> = BTreeMap::new();
        // Each connection's header admissions and header fanouts.
        let mut counts: BTreeMap<u64, (usize, usize)> = BTreeMap::new();
        let (mut header_fanouts, mut removals) = (0, 0);
        for entry in report.log.entries() {
            let Observation::Transport {
                connection: Some(connection),
                event,
                ..
            } = &entry.obs
            else {
                continue;
            };
            match event {
                TransportFact::Admitted { kind, .. } if *kind == ZAKURA_STREAM_HEADER_SYNC => {
                    assert!(
                        admitted.insert(*connection, true).is_none(),
                        "{name}: {connection}: an admission before the last one's fanout"
                    );
                    counts.entry(*connection).or_default().0 += 1;
                }
                TransportFact::RegistryFanout {
                    negotiated,
                    admitted_mask,
                } => {
                    assert_eq!(
                        admitted_mask & !negotiated,
                        0,
                        "{connection}: within the peer"
                    );
                    let header = admitted_mask & ZAKURA_CAP_HEADER_SYNC != 0;
                    assert_eq!(
                        header,
                        admitted.remove(connection).unwrap_or(false),
                        "{connection}: the fanout reaches header sync after its stream"
                    );
                    header_fanouts += usize::from(header);
                    counts.entry(*connection).or_default().1 += usize::from(header);
                    *reached.entry(*connection).or_default() |= admitted_mask;
                }
                TransportFact::RegistryRemoved { admitted_mask } => {
                    assert_eq!(
                        Some(*admitted_mask),
                        reached.remove(connection),
                        "{connection}: the removal names what its fanouts reached"
                    );
                    removals += 1;
                }
                _ => {}
            }
        }
        assert!(header_fanouts > 0, "{name}: a fanout reaches header sync");
        assert!(
            removals > 0 || name != "replacement",
            "{name}: the first connection's services are removed"
        );
        assert!(
            admitted.is_empty(),
            "{name}: admissions with no fanout after them: {admitted:?}"
        );
        for (connection, (admissions, fanouts)) in &counts {
            assert_eq!(
                admissions, fanouts,
                "{name}: {connection}: one header fanout per header admission"
            );
        }
        if name == "reopen" {
            assert!(
                counts.values().any(|(_, fanouts)| *fanouts >= 2),
                "{name}: a connection fans out header sync's mask twice: {counts:?}"
            );
        }
    }
}

/// Drops every `RegistryRemoved` it's handed.
#[derive(Debug)]
struct DropsRemovals(Arc<dyn TransportObserver>);

impl TransportObserver for DropsRemovals {
    fn observe(&self, event: TransportEvent) {
        if !matches!(event, TransportEvent::RegistryRemoved { .. }) {
            self.0.observe(event);
        }
    }
}

/// The transport counts each event before its observer sees it, so an
/// observer that drops a `RegistryRemoved` fails the run as unobserved
/// evidence at that boundary, and the same events delivered whole pass.
/// The task reports a registration, a fanout and a removal.
///
/// This fails if the tap's checkpoint ignores the transport's count.
#[test]
fn a_dropped_registry_record_fails_the_run() {
    for drop_removals in [false, true] {
        let mut sim = Sim::new(wire_config(), Plan::from_fuzz(&[]));
        let (slot, tap) = TransportTap::new(
            &mut sim,
            "node",
            NetScenario::default().net(),
            vec!["peer-0".to_owned()],
        );
        sim.add_source(Box::new(tap));
        let reporter = if drop_removals {
            slot.wrapped(|inner| Arc::new(DropsRemovals(inner)))
        } else {
            slot.clone()
        };
        sim.scheduler().spawn_root("node", async move {
            reporter.emit(TransportEvent::Registered {
                conn_id: 1,
                remote: peer_addr(0, 0),
            });
            reporter.emit(TransportEvent::RegistryFanout {
                conn_id: 1,
                negotiated: ZAKURA_CAP_HEADER_SYNC,
                admitted_mask: ZAKURA_CAP_HEADER_SYNC,
            });
            reporter.emit(TransportEvent::RegistryRemoved {
                conn_id: 1,
                admitted_mask: ZAKURA_CAP_HEADER_SYNC,
            });
        });
        let report = sim.run(&mut NoChecks, &mut MemorySink::default());
        // The observer outlives the run, as the node's handler does.
        drop(slot);
        let removed = report.log.entries().iter().any(|entry| {
            matches!(
                entry.obs,
                Observation::Transport {
                    event: TransportFact::RegistryRemoved { .. },
                    ..
                }
            )
        });
        if !drop_removals {
            assert_eq!(report.end.harness, None, "{:?}", report.end);
            assert!(removed, "the whole run logs the removal");
            continue;
        }
        assert!(!removed);
        let Some(HarnessFailure::SourceEvidence { source, detail }) = &report.end.harness else {
            panic!("expected an evidence failure, got {:?}", report.end);
        };
        assert_eq!(source, "transport:node#0");
        assert!(detail.starts_with("Unobserved"), "{detail}");
    }
}
