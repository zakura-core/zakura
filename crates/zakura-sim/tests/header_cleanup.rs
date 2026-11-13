//! The cleanup comparison (C17) on the header harnesses' witnesses: after
//! `Sim::run` returns, past the scheduler's shutdown, every evidence store
//! the node keeps equals its snapshot at the log's freeze, and the run's
//! log equals the log at the freeze ([`run_compared`]). The negative
//! controls, a store whose freeze is skipped and an append to the log after
//! the freeze, are `zakura_sim::stores`' unit tests.
#![cfg(zakura_sim)]

mod support;

use support::{inputs, storage_witnesses};
use zakura_sim::{
    harness::{
        cleanup::{run_compared, Compared},
        header_node::{Chain, Fork, Policy},
        header_sync::{
            Dialer, HeaderWire, Ports, ReleaseWake, Storage, StorageScenario, WakeScenario,
            WirePorts, WireScenario,
        },
        Subsystem,
    },
    peer::{
        honest::{CreditSpec, GrantPolicy, HonestSpec, SubscribeSpec},
        PeerSpec,
    },
    stores::EvidenceStores,
    PlanInput,
};

/// Each plan a witness runs under: the default one, then a few drawn.
fn plans() -> Vec<PlanInput> {
    let mut plans = vec![PlanInput::Bytes(Vec::new())];
    plans.extend(inputs(4, 128));
    plans
}

/// Runs `scenario` under every plan in [`plans`] on freshly prepared
/// ports, whose node's stores `stores` names, and requires each run's
/// stores and log to come out of cleanup as they froze.
fn holds<S: Subsystem>(scenario: &S::Scenario, stores: fn(&S::Prepared) -> EvidenceStores)
where
    S::Scenario: std::fmt::Debug,
{
    for plan in plans() {
        let prepared = S::prepare(scenario);
        let handle = stores(&prepared);
        let Compared { report, drift } = run_compared::<S>(scenario, prepared, &handle, plan)
            .unwrap_or_else(|error| panic!("{} on {scenario:?}: {error}", S::NAME));
        assert_eq!(drift, None, "{} on {scenario:?}: {:?}", S::NAME, report.end);
        assert!(
            handle.frozen_snapshot().is_some(),
            "the run froze the node's stores"
        );
    }
}

/// A storage harness's node stores.
fn storage_stores(ports: &Ports) -> EvidenceStores {
    ports.stores.clone()
}

/// The wire harness's node stores.
fn wire_stores(ports: &WirePorts) -> EvidenceStores {
    ports.stores.clone()
}

/// The worked example: the node at A3, `mirror-0` dialing in from a heavier
/// fork, and the node dialing `mirror-1` at genesis, so it both requests and
/// serves.
fn worked_example() -> StorageScenario {
    StorageScenario {
        node: Chain::main(3),
        mirrors: vec![
            storage_witnesses::mirror(
                Chain {
                    main: 1,
                    fork: Some(Fork { branch: 1, len: 4 }),
                },
                Dialer::Mirror,
            ),
            storage_witnesses::mirror(Chain::default(), Dialer::Node),
        ],
        policy: Policy::converging(),
        controls: Vec::new(),
        weights: Default::default(),
        node_settings: Default::default(),
        page_delays: Vec::new(),
        budget: None,
    }
}

/// The storage witnesses, including ones that close links, stop the node
/// and reset it, so cleanup has suspended work to resume.
#[test]
fn storage_witnesses_keep_their_stores_through_cleanup() {
    for scenario in [
        worked_example(),
        storage_witnesses::competing_mirrors(),
        storage_witnesses::dropped_locator(),
        storage_witnesses::apply_at_close(),
        storage_witnesses::reset_held_locator(),
    ] {
        holds::<Storage>(&scenario, storage_stores);
    }
}

/// The wire witness: the node at 50 dials an honest peer that serves it
/// 120 headers and subscribes to it from genesis.
#[test]
fn the_wire_witness_keeps_its_stores_through_cleanup() {
    let scenario = WireScenario {
        node: Chain::main(50),
        peers: vec![PeerSpec {
            honest: Some(HonestSpec {
                chain: 120,
                subscribe: Some(SubscribeSpec {
                    start: 0,
                    open: CreditSpec {
                        objects: 250,
                        bytes: 65_750,
                    },
                    grants: GrantPolicy::Immediate,
                }),
            }),
            ..PeerSpec::listening()
        }],
        policy: Policy::converging(),
        ..WireScenario::default()
    };
    holds::<HeaderWire>(&scenario, wire_stores);
}

/// Both release-wake fixtures.
#[test]
fn the_release_wake_witnesses_keep_their_stores_through_cleanup() {
    for scenario in [WakeScenario::Release, WakeScenario::Deferred] {
        holds::<ReleaseWake>(&scenario, storage_stores);
    }
}

/// A completed storage run drops every handle on its node's stores with
/// the harness, so repeated runs don't keep each other's evidence. A store
/// holding another store's owning handle would keep the registry alive
/// through itself, which this catches.
#[test]
fn a_completed_storage_run_releases_its_stores() {
    for scenario in [worked_example(), storage_witnesses::competing_mirrors()] {
        let prepared = Storage::prepare(&scenario);
        let handle = storage_stores(&prepared);
        let released = handle.downgrade();
        let Compared { drift, .. } =
            run_compared::<Storage>(&scenario, prepared, &handle, PlanInput::Bytes(Vec::new()))
                .unwrap_or_else(|error| panic!("{scenario:?}: {error}"));
        assert_eq!(drift, None);
        drop(handle);
        assert!(
            released.upgrade().is_none(),
            "the run's harness kept its node's stores alive on {scenario:?}"
        );
    }
}
