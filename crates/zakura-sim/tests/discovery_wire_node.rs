//! disc-2.1: the discovery node runs production's discovery behind its real
//! transport on simulated QUIC, and every discovery task is a scheduler
//! task. One scripted peer dials the node offering discovery, opens a
//! discovery stream and drains what the node sends on it. The node's static
//! candidate is an address no peer listens on, so the node keeps dialing
//! it. sim-5's peer evidence can't yet hold a node that redials a listening
//! peer, and its peers speak no discovery, which disc-2.2 adds.
#![cfg(zakura_sim)]

use zakura_network::zakura::{
    ZAKURA_CAP_DISCOVERY, ZAKURA_DISCOVERY_STREAM_VERSION, ZAKURA_STREAM_DISCOVERY,
};
use zakura_sim::{
    harness::{
        discovery_service::{spawn, NodeSpec},
        header_sync::wire_config,
        NetScenario,
    },
    peer::{PeerAction, PeerSpec},
    MemorySink, NoChecks, Observation, Plan, RunReport, Sim,
};

/// The tasks production's discovery startup and sessions spawn, by label:
/// the candidate dialer and its dial, the static candidate's maintained
/// dial, and the session's pipe and source.
const DISCOVERY_TASKS: [&str; 5] = [
    "discovery_dialer",
    "discovery_dial",
    "bootstrap_dial",
    "discovery_pipe",
    "discovery_source",
];

/// Runs the node with one verification slot and `peer-1`'s address, where
/// no peer listens, as its static candidate, and `peer-0` dialing and
/// opening a discovery stream, under the all-defaults Plan.
fn run() -> RunReport {
    let mut sim = Sim::new(wire_config(), Plan::from_fuzz(&[]));
    spawn(
        &mut sim,
        &NetScenario::default(),
        &NodeSpec {
            verification_slots: 1,
            static_candidates: vec![1],
        },
        &[PeerSpec {
            capabilities: ZAKURA_CAP_DISCOVERY,
            script: vec![PeerAction::Open {
                kind: ZAKURA_STREAM_DISCOVERY,
                version: ZAKURA_DISCOVERY_STREAM_VERSION,
            }],
            ..PeerSpec::dialing()
        }],
    );
    sim.run(&mut NoChecks, &mut MemorySink::default())
}

/// The name of every task the run's observations name as spawned, such as
/// `wire-node.1/discovery_dialer.1`.
fn spawned(report: &RunReport) -> Vec<String> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::TaskSpawned { task } => Some(task.clone()),
            _ => None,
        })
        .collect()
}

/// The label a task was spawned with: `discovery_dialer` for
/// `wire-node.1/discovery_dialer.1`.
fn label(task: &str) -> &str {
    let last = task.rsplit('/').next().unwrap_or(task);
    last.split('.').next().unwrap_or(last)
}

/// Each discovery task shows up in the run's task observations, so the
/// scheduler offered it and ran it, and the node admits the peer's
/// discovery session. The run ends with no harness failure, so the
/// transport tap saw no event from outside the run. A second run under the
/// same Plan logs the same observations, so the Plan alone decides the
/// schedule.
#[test]
fn discovery_tasks_run_on_the_scheduler() {
    let report = run();
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    let spawned = spawned(&report);
    for label in DISCOVERY_TASKS {
        assert!(
            spawned.iter().any(|task| self::label(task) == label),
            "no `{label}` task among {spawned:#?}"
        );
    }
    assert!(
        report.log.entries().iter().any(|entry| matches!(
            &entry.obs,
            Observation::Session { peer, kind, .. }
                if peer == "peer-0" && *kind == ZAKURA_STREAM_DISCOVERY
        )),
        "the node admitted no discovery session from peer-0"
    );
    let again = run();
    assert_eq!(report.log.entries(), again.log.entries());
    assert_eq!(report.end, again.end);
}
