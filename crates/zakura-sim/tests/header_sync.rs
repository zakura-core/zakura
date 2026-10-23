//! The header-sync reactor on the step loop, with inert storage and no peers.
//! It runs on a `Plan`, never panics, and replays.
#![cfg(zakura_sim)]

mod support;

use std::sync::Arc;

use support::{config, inputs};
use zakura_chain::parameters::Network;
use zakura_network::zakura::{
    spawn_header_sync_reactor, FullStateFrontiers, HeaderSyncStartup, ZakuraHeaderSyncConfig,
    LOCAL_MAX_MESSAGE_BYTES,
};
use zakura_node_services::header_chain::InertHeaderChainPort;
use zakura_sim::{
    observe::Exit, MemorySink, NoChecks, Observation, Outcome, PlanInput, RunReport, Sim,
};

/// Header-sync startup on Regtest at genesis, with inert storage.
fn startup() -> HeaderSyncStartup {
    let network = Network::new_regtest(Default::default());
    let anchor = (zakura_chain::block::Height(0), network.genesis_hash());
    let mut startup = HeaderSyncStartup::new(
        network,
        anchor,
        FullStateFrontiers {
            finalized_height: anchor.0,
            verified_block_tip: anchor.0,
            verified_block_hash: anchor.1,
        },
        Some(anchor),
        ZakuraHeaderSyncConfig::default(),
        LOCAL_MAX_MESSAGE_BYTES,
    );
    startup.header_chain_port = Arc::new(InertHeaderChainPort);
    startup
}

/// Runs the reactor for a short budget under `plan`. Returns the report and
/// the records.
fn run(plan: PlanInput) -> (RunReport, Vec<String>) {
    let mut sim = Sim::new(config(2, 2_000), plan);
    // The handle and the action channel stay alive until the run ends, so the
    // reactor doesn't see its inputs close.
    let (handle, actions, reactor) =
        spawn_header_sync_reactor(startup()).expect("the reactor starts");
    sim.scheduler().spawn_root("join", async move {
        let _ = reactor.await;
    });
    let mut sink = MemorySink::default();
    let report = sim.run(&mut NoChecks, &mut sink);
    drop((handle, actions));
    (report, sink.lines())
}

#[test]
fn header_sync_runs_and_replays() {
    for plan in inputs(8, 64) {
        let (report, first) = run(plan.clone());
        assert!(
            matches!(report.end.outcome, Outcome::Budget | Outcome::Idle { .. }),
            "{:?}",
            report.end
        );
        let panicked = report.log.entries().iter().any(|e| {
            matches!(
                e.obs,
                Observation::TaskExited {
                    exit: Exit::Panicked,
                    ..
                }
            )
        });
        assert!(!panicked);
        assert!(
            report
                .picks
                .iter()
                .any(|p| p.identity.starts_with("task:header_sync_reactor.1:")),
            "the reactor ran under its spawn name"
        );
        assert_eq!(run(plan).1, first, "the same plan replays the same records");
    }
}
