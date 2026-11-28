//! Simulator witnesses for tip priority, pressure and benefit caps (`mem-5`,
//! `mem-9`, `mem-10`):
//!
//! - `mem-5` (`memory/tip-never-waits`): with bulk consumers saturated by
//!   stalled readers and withheld responses, consensus and control reach
//!   their bounds without waiting on the pool. No simulated run takes a
//!   `Propagation` lease, so only the direct pool run reaches it here.
//!   BPv1's `zakura-network` integration test witnesses `Propagation` with
//!   real BPv1 leases.
//! - `mem-9` (`pressure-never-faults`): at the smallest accepted budget `B`,
//!   no local memory limit causes a fault close or a penalty
//!   (`session/honest-never-punished`).
//! - `mem-10` (`use-tracks-demand`): at `B = 64 GiB`, each consumer's peak
//!   stays within the cap the pool reports, and raising `B` to `256 GiB`
//!   changes no consumer's peak or cap.
#![cfg(zakura_sim)]

mod support;

use std::{
    rc::Rc,
    sync::{Arc, Mutex},
};

use support::memory::{
    assert_held, assert_witness_holds, mirror, run, smallest_accepted_budget_b, wire_scenario,
};
use zakura_chain::parameters::Network;
use zakura_network::{
    zakura::memory::{Consumer, LeaseDenied, MemoryPlan, PoolEvent, PoolObserver, Priority},
    Config,
};
use zakura_scheduler::{SimInstant, Status as SchedulerStatus};
use zakura_sim::{
    check::{Checker, Phase, StepCtx},
    domain::{
        block_sync::{BUDGET_LEDGERS_AGREE, RETAINED_BYTE_BOUND},
        memory_pool::{PoolGaugeModel, POOL_BOUNDED, SESSIONS_WITHIN_SHARE, TIP_NEVER_WAITS},
        session::{honest_never_punished, HonestPeers, HONEST_NEVER_PUNISHED},
    },
    harness::{
        block_node::policy::{BlockAllow, BlockKind, Hold, Select, Trigger},
        block_sync::{BlockScenario, Storage},
        header_sync::Dialer,
        sync::MirrorServices,
        sync_wire::SyncWire,
        Subsystem,
    },
    observe::{PoolFact, TransportFact},
    property::{
        Assume, AssumeRecord, Assumptions, HasView, Registry, RegistryChecker, Source, Sources,
    },
    Identity, Observation, ObservationLog, Outcome, PropertyOutcome, RunEnd,
};

const GIB: u64 = 1024 * 1024 * 1024;
const MIB: u64 = 1024 * 1024;

#[derive(Debug, Default)]
struct RecordingPoolObserver {
    events: Mutex<Vec<PoolEvent>>,
}

impl PoolObserver for RecordingPoolObserver {
    fn observe(&self, event: PoolEvent) {
        self.events.lock().expect("observer lock").push(event);
    }
}

/// Passing witness for `memory/tip-never-waits` (`mem-5`):
///
/// 1. Runs a real `block-sync/storage` simulation at the smallest accepted
///    budget, where the shared space `S` is zero. A probe requests 1.5 MB
///    blocks whose reads are held, and a mirror withholds height 3, so the
///    node fills commit-window leases. The run asserts that serving waits on
///    pool contention, so bulk is saturated, and that
///    `memory/tip-never-waits` holds.
/// 2. Drives a real `MemoryPool` with `PoolObserver` to full bulk saturation
///    across every bulk consumer (`Serving` stalled readers, `BlockSync`
///    withheld responses, `Tx` pending fetches, and `Sessions` peer bases)
///    until `shared_used == S` and bulk consumers hit `PoolFull`, then proves
///    `Propagation`, `Consensus`, and `Control` all reach 100% of their
///    reserves without emitting a single `PoolFact::Waited`.
#[test]
fn tip_never_waits_under_full_bulk_saturation() {
    // Part 1: a real `block-sync/storage` run with stalled serving reads and
    // withheld responses at the smallest accepted budget, where `S` is zero.
    let min_b = smallest_accepted_budget_b(&Network::new_regtest(Default::default()));
    let mut scenario = BlockScenario::peers_leave_served();
    scenario.memory_budget_bytes = Some(min_b);
    scenario.body_bytes = 1_500_000;
    scenario.mirrors[0].withheld = vec![3];
    scenario.policy.holds.push(Hold {
        select: Select::nth(BlockKind::Read, 1),
        until: Trigger::VirtualTime(5_000),
        then: vec![BlockAllow::Serve],
        deadline_ms: None,
    });
    let ports = Storage::prepare(&scenario);
    let report = run::<Storage>(&scenario, ports.clone());
    assert_held(&report, &[TIP_NEVER_WAITS, POOL_BOUNDED]);
    assert!(
        ports
            .view
            .borrow()
            .pool
            .borrow()
            .waited_reserved()
            .is_empty(),
        "no reserved consumer may wait during stalled serving + withheld block sync"
    );
    let bulk_waits: Vec<Consumer> = report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match entry.obs {
            Observation::Pool {
                event: PoolFact::Waited { consumer, .. },
                ..
            } if consumer.priority() == Priority::Bulk => Some(consumer),
            _ => None,
        })
        .collect();
    assert!(
        !bulk_waits.is_empty(),
        "two 1.5 MB responses overrun serving's floor with S at zero, so serving waits on \
         contention"
    );
    assert_witness_holds::<Storage>(&report, TIP_NEVER_WAITS);

    // Part 2: Full saturation of EVERY bulk consumer (`Serving`, `BlockSync`,
    // `Tx`, `Sessions`) to 100% of `S + floors` while proving `Propagation`,
    // `Consensus`, and `Control` reach their full reserves with zero waits.
    let network = Network::new_regtest(Default::default());
    let config = Config {
        network: network.clone(),
        ..Config::default()
    };
    let budget_b = min_b.saturating_add(256 * MIB);
    let plan = MemoryPlan::size_for_node(budget_b, &config, &network, None, &[])
        .expect("plan fits with 256 MiB shared space");
    let observer = Arc::new(RecordingPoolObserver::default());
    let pool = plan.build_pool().with_observer(Some(observer.clone()));

    let mut held_bulk = Vec::new();
    // 1. Fill `Sessions` up to its protected share (non-borrowing).
    let sessions_share = pool.protected_share(Consumer::Sessions);
    if sessions_share > 0 {
        held_bulk.push(
            pool.try_acquire(Consumer::Sessions, Some(1), sessions_share)
                .expect("sessions fits up to share"),
        );
    }
    // 2. Fill `Serving` (stalled readers), `BlockSync` (withheld responses),
    //    and `Tx` (pending fetches) until every byte of `S` and every bulk
    //    floor is occupied.
    for consumer in [Consumer::Serving, Consumer::BlockSync, Consumer::Tx] {
        let floor = plan.pool_config.consumers[consumer.index()].floor;
        if floor > 0 {
            held_bulk.push(
                pool.try_acquire(consumer, Some(2), floor)
                    .expect("bulk consumer reaches its floor"),
            );
        }
    }
    // Drain the remaining shared space `S` across bulk consumers in 1 MiB chunks,
    // plus whatever remainder fits, until `Serving` and `BlockSync` actually
    // hit pool saturation (`PoolFull` / `CapExceeded`).
    for consumer in [Consumer::Serving, Consumer::BlockSync, Consumer::Tx] {
        loop {
            let avail = pool.available_for(consumer);
            if avail == 0 {
                break;
            }
            let chunk = avail.min(4 * MIB);
            match pool.try_acquire(consumer, Some(3), chunk) {
                Ok(lease) => held_bulk.push(lease),
                Err(_) => break,
            }
        }
    }
    let snap = pool.snapshot();
    assert_eq!(
        snap.shared_used, snap.shared_capacity,
        "bulk consumers must completely saturate shared space S: {snap:?}"
    );
    // Confirm bulk consumers are now actively blocked on saturation.
    let bulk_denied = pool
        .try_acquire(Consumer::Serving, Some(4), 4096)
        .expect_err("saturated pool refuses extra bulk Serving lease");
    assert!(matches!(
        bulk_denied,
        LeaseDenied::PoolFull | LeaseDenied::CapExceeded | LeaseDenied::BelowShareWaiting
    ));

    // Now drive every reserved consumer (`Propagation`, `Consensus`, `Control`)
    // all the way to its exact full reserve bound while bulk remains 100% full!
    let mut held_reserved = Vec::new();
    for consumer in [
        Consumer::Propagation,
        Consumer::Consensus,
        Consumer::Control,
    ] {
        let reserve = plan.pool_config.consumers[consumer.index()].floor;
        assert!(reserve > 0, "{consumer:?} reserve must be positive");
        let lease = pool
            .try_acquire(consumer, Some(10), reserve)
            .unwrap_or_else(|err| {
                panic!("reserved consumer {consumer:?} waited under bulk saturation: {err:?}")
            });
        assert_eq!(lease.bytes(), reserve);
        held_reserved.push(lease);
    }

    // Release everything cleanly and fold all observer events into PoolGaugeModel.
    drop(held_reserved);
    drop(held_bulk);

    let events = observer.events.lock().expect("observer lock");
    let mut gauge = PoolGaugeModel::new(
        plan.pool_config.consumers[Consumer::Sessions.index()].floor,
        true,
    );
    for event in events.iter() {
        gauge.observe(event);
    }
    assert_eq!(gauge.total_leased(), 0);
    assert_eq!(gauge.live_lease_count(), 0);
    assert!(
        gauge.waited_reserved().is_empty(),
        "expected zero reserved waits, got {:?}",
        gauge.waited_reserved()
    );
    assert!(
        gauge.reserved_grants_observed() >= 3,
        "expected Control, Propagation, and Consensus grants"
    );
    assert!(
        gauge.bound_violations().is_empty(),
        "{POOL_BOUNDED}: {:?}",
        gauge.bound_violations()
    );
}

/// View marking every mirror peer in `SyncWire` as `HonestPeer` for evaluating
/// `session/honest-never-punished` over a `SyncWire` run's observation log.
#[derive(Clone)]
struct AllMirrorsHonest;

impl HasView for AllMirrorsHonest {
    type View = AllMirrorsHonest;

    fn view(&self) -> Self::View {
        self.clone()
    }
}

impl HonestPeers for AllMirrorsHonest {
    fn is_honest(&self, _index: usize) -> bool {
        true
    }
}

/// Replay `session/honest-never-punished` over `log` with every mirror marked
/// `HonestPeer`, returning its final [`PropertyOutcome`].
fn check_honest_never_punished_on_log(
    log: &ObservationLog,
    mirror_count: usize,
) -> PropertyOutcome {
    let harness = Rc::new(AllMirrorsHonest);
    let registry =
        Registry::new(vec![honest_never_punished::<AllMirrorsHonest>()]).expect("registers");
    let assumptions = Assumptions::declare(vec![AssumeRecord {
        assume: Assume::HonestPeer,
        params: serde_json::json!({ "mirrors": mirror_count }),
    }]);
    let sources = Sources::observed(&[Source::View, Source::Closes, Source::Storage]);
    let mut checker = RegistryChecker::new(registry, harness, assumptions, &sources);

    let pick = Identity::advance_time(SimInstant::ZERO);
    let last_step = log.entries().last().map_or(0, |entry| entry.step);
    let step = StepCtx {
        step: 0,
        pick: &pick,
        enabled: &[],
        phase: Phase::Live,
        budget: u64::MAX,
        now: SimInstant::ZERO,
        status: SchedulerStatus::Quiescent,
        quiescent: true,
        log,
    };
    if let Some(violation) = checker.after_step(&step) {
        panic!("`{HONEST_NEVER_PUNISHED}` violated at smallest accepted B: {violation:?}");
    }
    let end = RunEnd {
        outcome: Outcome::Budget,
        steps: last_step,
        violation: None,
        drain: None,
        harness: None,
        rest: None,
    };
    let reports = checker.at_end(&end, log);
    reports
        .into_iter()
        .find(|r| r.property == HONEST_NEVER_PUNISHED)
        .expect("honest-never-punished reported")
        .outcome
}

/// Witness for `mem-9` (`pressure-never-faults`): at the exact smallest
/// fitting budget `B` (where `B - 1` falls short in `MemoryPlan::size_for_node`),
/// honest peers syncing headers, blocks, and discovery suffer zero fault
/// closes and zero `Misbehavior` penalties (`session/honest-never-punished`
/// holds). With `S` at zero, every lease above a floor waits or is refused,
/// but no `Sessions` limit fires: four mirrors fit the plan's connection
/// floor.
#[test]
fn smallest_accepted_budget_never_faults_honest_peers() {
    let network = Network::new_regtest(Default::default());
    let config = Config {
        network: network.clone(),
        ..Config::default()
    };
    let min_b = smallest_accepted_budget_b(&network);
    assert!(
        MemoryPlan::size_for_node(min_b - 1, &config, &network, None, &[])
            .expect("a default config sizes")
            .shortfall
            .is_some(),
        "min_b - 1 must fall short"
    );
    let min_plan = MemoryPlan::size_for_node(min_b, &config, &network, None, &[])
        .expect("a default config sizes");
    assert!(min_plan.shortfall.is_none(), "min_b must fit");
    assert_eq!(
        min_plan.build_pool().snapshot().shared_capacity,
        0,
        "at the exact smallest accepted B, shared space S should be 0 (maximum pressure)"
    );

    // Connect 4 honest mirrors at `min_b`. The plan's connection floor at
    // `min_b` is above 4, so every mirror is admitted and syncs with `S` at
    // zero.
    let mirrors = vec![
        mirror(Dialer::Mirror, MirrorServices::All),
        mirror(Dialer::Node, MirrorServices::All),
        mirror(Dialer::Mirror, MirrorServices::All),
        mirror(Dialer::Node, MirrorServices::All),
    ];
    let mirror_count = mirrors.len();
    let scenario = wire_scenario(6, 2_000, mirrors, 45_000, min_b);
    let ports = SyncWire::prepare(&scenario);
    let report = run::<SyncWire>(&scenario, ports.clone());

    // Verify `session/honest-never-punished` applies and holds across the run.
    let honest_outcome = check_honest_never_punished_on_log(&report.log, mirror_count);
    assert!(
        matches!(honest_outcome, PropertyOutcome::Held { applicable } if applicable > 0),
        "expected {HONEST_NEVER_PUNISHED} to hold at min_b ({min_b} B), got {honest_outcome:?}"
    );

    // Verify directly that zero fault closes, zero Misbehavior, and zero
    // handshake validation faults occurred even under maximum memory pressure.
    // The wire tier records no `ConnectionClosed`: the node's transport tap
    // reports each close it decides, so the scan reads that, and counts the
    // registrations to show the tap saw every mirror's connection.
    let mut registered = 0;
    for entry in report.log.entries() {
        match &entry.obs {
            Observation::ConnectionClosed { connection, cause } if cause.is_fault() => {
                panic!("fault close on honest peer connection {connection} at min_b: {cause}");
            }
            Observation::Transport {
                remote,
                event: TransportFact::CloseDecided { cause: Some(cause) },
                ..
            } if cause.is_fault() => {
                panic!("the node closed honest {remote} with a fault at min_b: {cause}");
            }
            Observation::Transport {
                event: TransportFact::Registered,
                ..
            } => registered += 1,
            Observation::Misbehavior { connection } => {
                panic!("Misbehavior recorded on honest peer connection {connection} at min_b");
            }
            Observation::Transport {
                remote,
                event:
                    TransportFact::Handshake {
                        validation: Some(reason),
                        ..
                    },
                ..
            } => {
                panic!("handshake validation fault against honest {remote} at min_b: {reason}");
            }
            _ => {}
        }
    }
    assert!(
        registered >= mirror_count,
        "the transport tap registered {registered} of {mirror_count} mirror connections"
    );

    assert_held(
        &report,
        &[
            POOL_BOUNDED,
            SESSIONS_WITHIN_SHARE,
            TIP_NEVER_WAITS,
            RETAINED_BYTE_BOUND,
            BUDGET_LEDGERS_AGREE,
        ],
    );

    // Confirm the node still synced to the mirrors' tip at `min_b`.
    let tip = ports.mirror_tip();
    assert_eq!(
        ports.tips(),
        (tip, tip),
        "node must still sync headers and blocks to tip at smallest accepted B"
    );
}

/// Each consumer's peak leased bytes at a grant or growing resize and the
/// cap the pool reported at that peak, by [`Consumer::index`], and the peak
/// retained wire bytes. Drains a move adds ignore the target's cap by design.
type Demand = ([u64; Consumer::COUNT], [Option<u64>; Consumer::COUNT], u64);

/// Read a run's [`Demand`] from its pool gauge and owner model.
fn demand(pool: &PoolGaugeModel, retained_peak: u64) -> Demand {
    (pool.granted_peaks(), pool.caps_at_peak(), retained_peak)
}

/// Each consumer's peak stays within the cap the pool reported at that peak,
/// and raising `B` from 64 GiB to 256 GiB changes no peak, cap or retained
/// byte count.
fn assert_use_tracks_demand(workload: &str, at_64: &Demand, at_256: &Demand) {
    let (peaks, caps, _) = at_64;
    assert!(
        peaks.iter().any(|peak| *peak > 0),
        "{workload}: the pool granted leases"
    );
    for consumer in Consumer::ALL {
        let peak = &peaks[consumer.index()];
        if *peak == 0 {
            continue;
        }
        let cap = caps[consumer.index()]
            .as_ref()
            .unwrap_or_else(|| panic!("{workload}: `{consumer:?}` peaked without a reported cap"));
        assert!(
            peak <= cap,
            "{workload}: `{consumer:?}` peaked at {peak} B above its cap {cap} B at B = 64 GiB"
        );
    }
    assert_eq!(
        at_64, at_256,
        "{workload}: raising B from 64 GiB to 256 GiB changes no consumer's peak, cap or \
         retained bytes"
    );
}

/// Witness for `mem-10` (`use-tracks-demand`): at `B = 64 GiB`, each
/// consumer's peak leased bytes stay within the cap the pool reports, and
/// raising `B` to `256 GiB` changes no consumer's peak, its cap, or the peak
/// retained bytes.
#[test]
fn use_tracks_demand_between_64_and_256_gib() {
    // A `sync/wire` workload: header sync, block sync and discovery.
    let run_wire = |budget_b: u64| {
        let mirrors = vec![
            mirror(Dialer::Mirror, MirrorServices::All),
            mirror(Dialer::Node, MirrorServices::All),
        ];
        let scenario = wire_scenario(8, 8_000, mirrors, 60_000, budget_b);
        let ports = SyncWire::prepare(&scenario);
        run::<SyncWire>(&scenario, ports.clone());
        let tip = ports.mirror_tip();
        assert_eq!(ports.tips(), (tip, tip), "wire sync reached tip");
        let view = ports.view.borrow();
        let pool = view.pool.borrow();
        demand(&pool, view.owners.peaks().retained)
    };
    assert_use_tracks_demand("sync/wire", &run_wire(64 * GIB), &run_wire(256 * GIB));

    // A `block-sync/storage` workload with a serving probe and peers that
    // leave.
    let run_storage = |budget_b: u64| {
        let mut scenario = BlockScenario::peers_leave_served();
        scenario.memory_budget_bytes = Some(budget_b);
        let ports = Storage::prepare(&scenario);
        run::<Storage>(&scenario, ports.clone());
        let view = ports.view.borrow();
        let pool = view.pool.borrow();
        demand(&pool, view.owners.peaks().retained)
    };
    assert_use_tracks_demand(
        "block-sync/storage",
        &run_storage(64 * GIB),
        &run_storage(256 * GIB),
    );
}
