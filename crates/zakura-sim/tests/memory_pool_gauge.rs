//! Passing witnesses for the generic memory-pool gauge and the block-sync
//! lease checks (`mem-2`, `mem-3`, `mem-11`, `mem-13`).
#![cfg(zakura_sim)]

mod support;

use support::memory::{held, outcome, smallest_accepted_budget_b};
use zakura_chain::parameters::Network;
use zakura_sim::{
    domain::{
        block_sync::{BUDGET_LEDGERS_AGREE, RETAINED_BYTE_BOUND},
        memory_pool::{POOL_BOUNDED, SESSIONS_WITHIN_SHARE, TEARDOWN_RETURNS_ZERO},
    },
    harness::{
        block_sync::{BlockScenario, Storage, PEERS_LEAVE_SERVED_TAG},
        header_sync::Dialer,
        run_prepared,
        sync::{MirrorServices, SyncMirror, SyncScenario},
        sync_wire::{SyncWire, SyncWireScenario},
        HarnessError, NetScenario, Subsystem,
    },
    observe::PoolFact,
    witness::{check_witness, Expect, WitnessScenario},
    MemorySink, Plan,
};

/// Passing witness for `memory/pool-bounded` (`mem-3`, G1): an honest
/// `block-sync/storage` run emits `Observation::Pool` events and stays within
/// `S + floors` and each consumer's cap at every step.
#[test]
fn pool_gauge_bounds_and_teardown_hold_on_sync() {
    let scenario = BlockScenario::default();
    let ports = Storage::prepare(&scenario);
    let report = run_prepared::<Storage>(
        &scenario,
        ports.clone(),
        Plan::from_fuzz(&[]),
        &mut MemorySink::default(),
        None,
    )
    .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    assert!(
        held(&report, POOL_BOUNDED),
        "{:?}",
        outcome(&report, POOL_BOUNDED)
    );
    assert!(
        held(&report, RETAINED_BYTE_BOUND),
        "{:?}",
        outcome(&report, RETAINED_BYTE_BOUND)
    );
    assert!(
        held(&report, BUDGET_LEDGERS_AGREE),
        "{:?}",
        outcome(&report, BUDGET_LEDGERS_AGREE)
    );
    let witness = WitnessScenario::<Storage> {
        scenario,
        input: Plan::from_fuzz(&[]),
    };
    check_witness(witness, POOL_BOUNDED, Expect::Holds);
}

/// Passing witness for `memory/teardown-returns-zero` (`mem-11`, G4): after
/// peers leave and serving reads finish, every live pool lease and consumer
/// total returns to zero.
#[test]
fn pool_teardown_returns_zero_after_peers_leave() {
    let scenario = Storage::draw(&PEERS_LEAVE_SERVED_TAG);
    let witness = WitnessScenario::<Storage> {
        scenario: scenario.clone(),
        input: Plan::from_fuzz(&[]),
    };
    let ports = Storage::prepare(&scenario);
    let report = run_prepared::<Storage>(
        &scenario,
        ports.clone(),
        Plan::from_fuzz(&[]),
        &mut MemorySink::default(),
        None,
    )
    .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    assert!(
        held(&report, TEARDOWN_RETURNS_ZERO),
        "{:?}",
        outcome(&report, TEARDOWN_RETURNS_ZERO)
    );
    assert_eq!(ports.view.borrow().pool.borrow().total_leased(), 0);
    assert_eq!(ports.view.borrow().pool.borrow().live_lease_count(), 0);
    check_witness(witness, TEARDOWN_RETURNS_ZERO, Expect::Holds);
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
    let scenario = SyncWireScenario {
        net: NetScenario::default(),
        sync: SyncScenario {
            blocks: 6,
            body_bytes: 2_000,
            mirrors: vec![
                SyncMirror {
                    dialer: Dialer::Mirror,
                    services: Some(MirrorServices::All),
                },
                SyncMirror {
                    dialer: Dialer::Node,
                    services: Some(MirrorServices::HeaderSync),
                },
                SyncMirror {
                    dialer: Dialer::Mirror,
                    services: Some(MirrorServices::DiscoveryOnly),
                },
                SyncMirror {
                    dialer: Dialer::Node,
                    services: Some(MirrorServices::All),
                },
            ],
            steps: Some(45_000),
            ..SyncScenario::default()
        },
        windows: None,
        superseded_hold: None,
        memory_budget_bytes: Some(small_b),
    };
    let ports = SyncWire::prepare(&scenario);
    let report = match run_prepared::<SyncWire>(
        &scenario,
        ports.clone(),
        Plan::from_fuzz(&[]),
        &mut MemorySink::default(),
        None,
    ) {
        Ok(report) => report,
        Err(HarnessError::Run { failure, report }) => panic!("{failure}: {:?}", report.end),
        Err(error) => panic!("{error}"),
    };
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    assert!(
        held(&report, SESSIONS_WITHIN_SHARE),
        "{:?}",
        outcome(&report, SESSIONS_WITHIN_SHARE)
    );
    assert!(
        held(&report, POOL_BOUNDED),
        "{:?}",
        outcome(&report, POOL_BOUNDED)
    );
    // Verify mixed reactor sets produced distinct `Sessions` lease sizes on the
    // node (a full mirror costs more than a discovery-only mirror).
    let session_grants: Vec<u64> = report
        .log
        .entries()
        .iter()
        .filter_map(|e| match &e.obs {
            zakura_sim::Observation::Pool {
                event: PoolFact::Granted {
                    consumer, bytes, ..
                },
                ..
            } if consumer == "sessions" => Some(*bytes),
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
