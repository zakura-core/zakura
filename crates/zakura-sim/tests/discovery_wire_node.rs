//! disc-2.1: the discovery node runs production's discovery behind its real
//! transport on simulated QUIC, and every discovery task is a scheduler
//! task. One listening scripted peer, held as a static candidate, accepts
//! discovery and drains what the node sends.
#![cfg(zakura_sim)]

use zakura_network::zakura::ZAKURA_CAP_DISCOVERY;
use zakura_sim::{
    harness::{
        discovery_service::{spawn, NodeSpec},
        header_sync::wire_config,
        NetScenario,
    },
    peer::PeerSpec,
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

/// Runs the node with one verification slot and `peer-0` as its static
/// candidate, under the all-defaults Plan.
fn run() -> RunReport {
    let mut sim = Sim::new(wire_config(), Plan::from_fuzz(&[]));
    spawn(
        &mut sim,
        &NetScenario::default(),
        &NodeSpec {
            verification_slots: 1,
            static_candidates: vec![0],
        },
        &[PeerSpec {
            capabilities: ZAKURA_CAP_DISCOVERY,
            ..PeerSpec::listening()
        }],
    );
    sim.run(&mut NoChecks, &mut MemorySink::default())
}

/// The labels of every task the run's observations name as spawned, from
/// names such as `wire-node/discovery_dialer.1`.
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

/// Each discovery task shows up in the run's task observations, so the
/// scheduler offered it and ran it, and the run ends with no harness
/// failure, so the transport tap saw no event from outside the run. A
/// second run under the same Plan logs the same observations, which work
/// on a thread the scheduler doesn't own would make differ.
#[test]
fn discovery_tasks_run_on_the_scheduler() {
    let report = run();
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    let spawned = spawned(&report);
    for label in DISCOVERY_TASKS {
        assert!(
            spawned
                .iter()
                .any(|task| task.rsplit('/').next().is_some_and(|last| last
                    .split('.')
                    .next()
                    == Some(label))),
            "no `{label}` task among {spawned:#?}"
        );
    }
    let again = run();
    assert_eq!(report.log.entries(), again.log.entries());
    assert_eq!(report.end, again.end);
}
