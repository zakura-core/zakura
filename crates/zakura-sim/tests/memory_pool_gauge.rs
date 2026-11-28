//! Passing witnesses for the generic memory-pool gauge and the block-sync
//! lease checks (`mem-2`, `mem-3`, `mem-11`, `mem-13`).
#![cfg(zakura_sim)]

mod support;

use support::memory::{
    assert_held, assert_witness_holds, mirror, run, smallest_accepted_budget_b, wire_scenario,
};
use zakura_chain::parameters::Network;
use zakura_network::zakura::memory::Consumer;
use zakura_sim::{
    domain::{
        block_sync::{BUDGET_LEDGERS_AGREE, RETAINED_BYTE_BOUND},
        memory_pool::{POOL_BOUNDED, SESSIONS_WITHIN_SHARE, TEARDOWN_RETURNS_ZERO},
    },
    harness::{
        block_sync::{BlockScenario, Storage, PEERS_LEAVE_SERVED_TAG},
        header_sync::Dialer,
        sync::MirrorServices,
        sync_wire::SyncWire,
        Subsystem,
    },
    observe::PoolFact,
};

/// Passing witness for `memory/pool-bounded` (`mem-3`, G1): an honest
/// `block-sync/storage` run emits `Observation::Pool` events and stays within
/// `S + floors` and each consumer's cap at every step.
#[test]
fn pool_gauge_bounds_and_teardown_hold_on_sync() {
    let scenario = BlockScenario::default();
    let report = run::<Storage>(&scenario, Storage::prepare(&scenario));
    assert_held(
        &report,
        &[POOL_BOUNDED, RETAINED_BYTE_BOUND, BUDGET_LEDGERS_AGREE],
    );
    assert_witness_holds::<Storage>(&report, POOL_BOUNDED);
}

/// Passing witness for `memory/teardown-returns-zero` (`mem-11`, G4): after
/// peers leave and serving reads finish, every live pool lease and consumer
/// total returns to zero.
#[test]
fn pool_teardown_returns_zero_after_peers_leave() {
    let scenario = Storage::draw(&PEERS_LEAVE_SERVED_TAG);
    let ports = Storage::prepare(&scenario);
    let report = run::<Storage>(&scenario, ports.clone());
    assert_held(&report, &[TEARDOWN_RETURNS_ZERO]);
    assert_eq!(ports.view.borrow().pool.borrow().total_leased(), 0);
    assert_eq!(ports.view.borrow().pool.borrow().live_lease_count(), 0);
    assert_witness_holds::<Storage>(&report, TEARDOWN_RETURNS_ZERO);
}

/// Passing witness for `memory/sessions-within-share` (`mem-13`): run
/// `sync/wire` at a small budget `B` (tight above the minimum accepted `B`)
/// with peers supporting different reactor sets (`DiscoveryOnly`,
/// `HeaderSync`, and `All`), verifying `Sessions` leases never exceed their
/// protected share, no partial reactor set is admitted on refusal, and floor
/// connections succeed without waiting on pool contention.
#[test]
fn sessions_stay_within_share_at_small_budget_with_mixed_reactors() {
    let min_b = smallest_accepted_budget_b(&Network::new_regtest(Default::default()));
    // Give `S` just a small margin above the minimum so `Sessions`'s protected
    // share is tight and exercises both admission of mixed reactor sets and
    // non-borrowing enforcement.
    let small_b = min_b.saturating_add(16 * 1024 * 1024);
    let scenario = wire_scenario(
        6,
        2_000,
        vec![
            mirror(Dialer::Mirror, MirrorServices::All),
            mirror(Dialer::Node, MirrorServices::HeaderSync),
            mirror(Dialer::Mirror, MirrorServices::DiscoveryOnly),
            mirror(Dialer::Node, MirrorServices::All),
        ],
        45_000,
        small_b,
    );
    let report = run::<SyncWire>(&scenario, SyncWire::prepare(&scenario));
    assert_held(&report, &[SESSIONS_WITHIN_SHARE, POOL_BOUNDED]);
    // Verify mixed reactor sets produced distinct `Sessions` lease sizes on the
    // node (a full mirror costs more than a discovery-only mirror).
    let session_grants: Vec<u64> = report
        .log
        .entries()
        .iter()
        .filter_map(|e| match &e.obs {
            zakura_sim::Observation::Pool {
                event:
                    PoolFact::Granted {
                        consumer: Consumer::Sessions,
                        bytes,
                        ..
                    },
                ..
            } => Some(*bytes),
            _ => None,
        })
        .collect();
    assert!(
        session_grants.len() >= 2,
        "expected multiple Sessions grants across mixed-reactor peers, got {session_grants:?}"
    );
    let min_grant = *session_grants.iter().min().unwrap();
    let max_grant = *session_grants.iter().max().unwrap();
    assert!(
        min_grant < max_grant,
        "peers with fewer reactors must lease smaller session bases: {session_grants:?}"
    );
}
