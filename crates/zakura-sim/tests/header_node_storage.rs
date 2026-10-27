//! Lifecycle sweeps for a header-sync node on controlled storage (M4-C):
//! operation identity and order (M4.1), one committed state for requester
//! and publisher (M4.2), permit and lease reclamation (M4.4), and
//! preparation under the scenario clock (M4.6), with a smoke test of a
//! large preparation. Thread confinement isn't proven here.
//!
//! Fresh-process replay (M4.6) is `cli.rs`'s
//! `every_harness_replays_byte_for_byte_in_a_fresh_process`, which covers
//! `header-sync/storage` like every registered harness.
#![cfg(zakura_sim)]

mod support;

use std::sync::Arc;

use support::inputs;
use zakura_sim::{
    check::after_step,
    harness::{
        header_node::{Base, Chain, Fork, Policy},
        header_sync::{
            storage_properties, storage_sources, Dialer, MirrorSpec, Ports, Storage,
            StorageScenario, OPERATION_IDS,
        },
        run_prepared, Subsystem,
    },
    observe::{Exit, StorageFact, StorageRequest},
    Checker, MemorySink, Observation, PlanInput, PropertyOutcome, RunReport,
};
use zakura_storage::header::{
    controlled::{ControlledPort, OpId},
    Fault, Outcome,
};

/// Every outcome, in canonical order.
const OUTCOMES: [Outcome; 5] = [
    Outcome::Engine,
    Outcome::Fault(Fault::Timeout),
    Outcome::Fault(Fault::Unavailable),
    Outcome::Fault(Fault::LocalResource),
    Outcome::Rejected,
];

/// A sweep's plans.
fn sweep() -> Vec<PlanInput> {
    inputs(64, 256)
}

/// Runs `scenario` on `ports` under `input`, with `extra` checking each
/// step after the harness's own checks. The run must end without a
/// violation or harness failure.
fn run_on(
    scenario: &StorageScenario,
    ports: &Ports,
    input: PlanInput,
    extra: &mut dyn Checker,
) -> RunReport {
    let report = run_prepared::<Storage>(
        scenario,
        ports.clone(),
        input,
        &mut MemorySink::default(),
        Some(extra),
    )
    .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    report
}

/// [`run_on`] on freshly prepared ports, with no extra checks.
fn run(scenario: &StorageScenario, input: PlanInput) -> (RunReport, Ports) {
    let ports = Storage::prepare(scenario);
    let report = run_on(scenario, &ports, input, &mut zakura_sim::NoChecks);
    (report, ports)
}

/// A mirror holding `chain`.
fn mirror(chain: Chain, dialer: Dialer) -> MirrorSpec {
    MirrorSpec { chain, dialer }
}

/// The worked example under `policy`: the node at A3, `mirror-0` dialing in
/// from a heavier fork A1, B2 to B5, and the node dialing `mirror-1` at
/// genesis, so the node both requests and serves.
fn worked_example(policy: Policy) -> StorageScenario {
    StorageScenario {
        node: Chain::main(3),
        mirrors: vec![
            mirror(
                Chain {
                    main: 1,
                    fork: Some(Fork { branch: 1, len: 4 }),
                },
                Dialer::Mirror,
            ),
            mirror(Chain::default(), Dialer::Node),
        ],
        policy,
        controls: Vec::new(),
    }
}

/// Every outcome the contract allows, with no latency bound.
fn any() -> Policy {
    Policy {
        base: Base::Any,
        bound_ms: None,
        holds: Vec::new(),
    }
}

/// The node's storage facts, in log order, with their steps.
fn facts(report: &RunReport) -> Vec<(u64, &StorageFact)> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Storage {
                instance: 0, fact, ..
            } => Some((entry.step, fact)),
            _ => None,
        })
        .collect()
}

/// Under every outcome and no bound, the node's operation IDs rise by one per
/// submission, and each completes at most once, after its submission.
/// The harness checks that after every step, and some plan completes an
/// operation with a fault, so faults are exercised.
#[test]
fn operation_ids_are_unique_and_complete_once() {
    let scenario = worked_example(any());
    let mut faulted = false;
    for input in sweep() {
        let (report, _) = run(&scenario, input);
        let ids = report
            .properties
            .iter()
            .find(|report| report.property == OPERATION_IDS)
            .expect("the harness reports operation IDs");
        assert!(
            matches!(ids.outcome, PropertyOutcome::Held { .. }),
            "{ids:?}"
        );
        faulted |= facts(&report).iter().any(|(_, fact)| {
            matches!(fact, StorageFact::Completed { outcome, .. } if outcome != "engine")
        });
    }
    assert!(faulted, "no plan completed an operation with a fault");
}

/// The node's storage history: every storage observation, as text, with its
/// step.
fn history(report: &RunReport) -> Vec<String> {
    report
        .log
        .entries()
        .iter()
        .filter(|entry| {
            matches!(
                entry.obs,
                Observation::Storage { .. } | Observation::StorageState { .. }
            )
        })
        .map(|entry| format!("{} {:?}", entry.step, entry.obs))
        .collect()
}

/// After every step, `port` lists its pending operations in ID order, each
/// with its legal outcomes in canonical order.
fn ordered(port: Arc<ControlledPort>) -> impl Checker {
    after_step(move |_| {
        let pending = port.pending();
        assert!(
            pending.windows(2).all(|pair| pair[0].id < pair[1].id),
            "{pending:?}"
        );
        for op in &pending {
            let order: Vec<usize> = op
                .enabled
                .iter()
                .map(|outcome| OUTCOMES.iter().position(|o| o == outcome).unwrap())
                .collect();
            assert!(order.windows(2).all(|pair| pair[0] < pair[1]), "{op:?}");
        }
        None
    })
}

/// A run's storage history depends only on its scenario, plan and
/// preloaded ports: plan A twice, with plan B between, gives A's history
/// both times. B's differs, so the comparison can fail. Every step lists
/// pending operations in order.
#[test]
fn node_storage_history_repeats_a_a_and_a_b_a() {
    let scenario = worked_example(any());
    let plans = inputs(3, 256);
    let mut histories = Vec::new();
    for input in [&plans[1], &plans[2], &plans[1]] {
        let ports = Storage::prepare(&scenario);
        let report = run_on(
            &scenario,
            &ports,
            input.clone(),
            &mut ordered(ports.node.clone()),
        );
        histories.push(history(&report));
    }
    assert_eq!(histories[0], histories[2], "plan A, then B, then A");
    assert_ne!(histories[0], histories[1], "plan B differs");
}

/// After every step, every outcome the port doesn't allow for a pending
/// operation now, and any outcome for an unused ID, is refused, and the
/// refusal changes neither the pending list nor the engine's state.
#[test]
fn illegal_outcomes_are_refused_in_a_node_run() {
    let scenario = worked_example(any());
    let mut refused = 0;
    for input in sweep() {
        let ports = Storage::prepare(&scenario);
        let port = ports.node.clone();
        let mut tried = 0;
        let mut check = after_step(|_| {
            let pending = port.pending();
            let digest = port.engine().state_digest();
            let unused = OpId(pending.iter().map(|op| op.id.0).max().unwrap_or(0) + 1_000);
            for op in &pending {
                for outcome in OUTCOMES.iter().filter(|o| !op.enabled.contains(o)) {
                    assert!(
                        port.complete(op.id, *outcome).is_err(),
                        "{op:?} {outcome:?}"
                    );
                    tried += 1;
                }
            }
            assert!(port.complete(unused, Outcome::Engine).is_err());
            assert_eq!(port.pending(), pending, "a refusal leaves the operations");
            assert_eq!(port.engine().state_digest(), digest, "and the engine");
            None
        });
        run_on(&scenario, &ports, input, &mut check);
        drop(check);
        refused += tried;
    }
    assert!(refused > 0, "no step had an illegal outcome to refuse");
}

/// The worked example, converging, under 64 plans. After every step,
/// the node's selected-chain watch holds its engine's snapshot, and at the
/// end `mirror-1`, served by its publisher, holds its tip.
///
/// Left alone, serving finishes long before the node's prepare, so holds pin
/// the overlap: its first publisher read waits for its first prepare, and
/// its first apply waits for that read. Every plan must then complete a
/// read between the prepare and the apply.
#[test]
fn requester_and_publisher_share_committed_state() {
    use zakura_node_services::header_chain::Port;
    use zakura_sim::harness::header_node::policy::{Allow, Hold, OpKind, Select, Trigger};

    let hold = |select, until| Hold {
        select,
        until,
        then: vec![Allow::Engine],
        deadline_ms: None,
    };
    let scenario = worked_example(Policy {
        holds: vec![
            hold(
                Select::nth(OpKind::Read, 1),
                Trigger::Completed(Select::nth(OpKind::Prepare, 1)),
            ),
            hold(
                Select::nth(OpKind::Apply, 1),
                Trigger::Completed(Select::nth(OpKind::Read, 1)),
            ),
        ],
        ..Policy::converging()
    });
    for input in sweep() {
        let ports = Storage::prepare(&scenario);
        let port = ports.node.clone();
        let watch = port.selected_chain();
        let mut check = after_step(|_| {
            assert_eq!(
                watch.borrow().as_ref(),
                Some(&port.engine().snapshot()),
                "the watch is the engine's"
            );
            None
        });
        let report = run_on(&scenario, &ports, input, &mut check);
        drop(check);
        let tip = port.engine().snapshot().frontiers.header_best;
        assert_eq!(tip, scenario.mirrors[0].chain.tip(&ports.fixture));
        assert_eq!(
            ports.mirrors[1].snapshot().frontiers.header_best,
            tip,
            "mirror-1"
        );

        // Which kind each operation is, then where a serving completion
        // falls against the node's first prepare and apply completions.
        let mut kinds = std::collections::BTreeMap::new();
        let (mut prepared, mut served_between) = (false, false);
        for (_, fact) in facts(&report) {
            match fact {
                StorageFact::Submitted { id, request, .. } => {
                    kinds.insert(*id, request.clone());
                }
                StorageFact::Completed { id, .. } => match kinds.get(id) {
                    Some(StorageRequest::PrepareHeaderTarget { .. }) => prepared = true,
                    Some(StorageRequest::ApplyHeaderTarget { .. }) => break,
                    Some(
                        StorageRequest::AcquireHeaderPath { .. }
                        | StorageRequest::ReadHeaderPath { .. },
                    ) if prepared => served_between = true,
                    _ => {}
                },
                _ => {}
            }
        }
        assert!(
            served_between,
            "a read completed between the prepare and the apply"
        );
    }
}

/// A smoke test: the node prepares a 160-header batch from `mirror-0`, and no
/// task panics.
///
/// It doesn't prove that preparation stays on the run's thread. Waking a
/// completion's caller from another OS thread passes it, because the
/// reactor polls its port futures with `now_or_never`, which registers a
/// no-op waker, so the wake never reaches the scheduler's thread
/// assertion. Thread confinement rests on the simulator build's `Each` in
/// the preparation pipeline, which iterates on the caller's thread.
#[test]
fn a_160_header_preparation_panics_no_task() {
    let scenario = StorageScenario {
        mirrors: vec![mirror(Chain::main(160), Dialer::Mirror)],
        ..StorageScenario::default()
    };
    for input in inputs(4, 256) {
        let (report, ports) = run(&scenario, input);
        let panicked = report.log.entries().iter().any(|entry| {
            matches!(
                entry.obs,
                Observation::TaskExited {
                    exit: Exit::Panicked,
                    ..
                }
            )
        });
        assert!(!panicked, "{:?}", report.end);
        assert_eq!(report.cleanup.panicked, 0);
        let batch = facts(&report).into_iter().any(|(_, fact)| {
            matches!(
                fact,
                StorageFact::Submitted {
                    request: StorageRequest::PrepareHeaderTarget { headers: 160, .. },
                    ..
                }
            )
        });
        assert!(batch, "the node prepared all 160 headers in one batch");
        assert_eq!(
            ports
                .node
                .engine()
                .snapshot()
                .frontiers
                .header_best
                .height
                .0,
            160
        );
    }
}

/// What a probe task does on the node's port as peer `probe`, in a
/// [`Probing`] run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
enum Probe {
    /// Nothing.
    Idle,
    /// Acquire a path to the node's tip, then release it.
    AcquireRelease,
    /// Acquire, release, then read the released handle.
    ReadReleased,
    /// Acquire, then hold the lease until the run ends.
    AcquireHold,
    /// Submit an acquire, let it complete, then drop the call unread.
    AbandonAcquire,
    /// After 31 s, acquire and release, which sweeps idle leases.
    SweepLate,
    /// Prepare `len` fixture headers on genesis.
    Prepare {
        /// The headers.
        len: u32,
    },
}

/// A storage scenario with a probe, and optionally a scenario clock
/// origin, in seconds since the Unix epoch.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Probed {
    storage: StorageScenario,
    probe: Probe,
    utc_origin: Option<u64>,
    /// Live steps, if not the storage run's.
    budget: Option<u64>,
}

/// What a probe saw, for the test to read after the run.
#[derive(Default)]
struct ProbeOut {
    /// The read of a released handle.
    read: Option<String>,
    /// A prepared target and the scenario clock when it came back.
    prepared: Option<(
        zakura_node_services::header_chain::PreparedHeaderTarget,
        std::time::SystemTime,
    )>,
}

/// [`Probing`]'s prepared state: the storage ports and the probe's output.
#[derive(Clone)]
struct ProbeReady {
    ports: Ports,
    out: std::rc::Rc<std::cell::RefCell<ProbeOut>>,
}

/// `header-sync/storage` with a probe task, for causes an honest node
/// doesn't reach on its own. Test-only, so it isn't registered.
struct Probing(std::rc::Rc<Storage>);

impl zakura_sim::property::HasView for Probing {
    type View = zakura_sim::harness::header_sync::StorageView;

    /// The storage harness's view.
    fn view(&self) -> Self::View {
        self.0.view()
    }
}

/// The probe's peer identity.
fn probe_id() -> zakura_header_chain::SourceId {
    zakura_header_chain::SourceId::from_digest([9; 32])
}

/// Runs `probe` as peer `probe` against `port`, whose chains come from
/// `fixture`.
async fn probe(
    probe: Probe,
    port: Arc<ControlledPort>,
    fixture: zakura_storage::header::fixture::HeaderFixture,
    out: std::rc::Rc<std::cell::RefCell<ProbeOut>>,
) {
    use std::time::Duration;
    use zakura_node_services::header_chain::{AcquirePath, AcquirePathReply, Port, ReadPath};

    let snapshot = || {
        port.selected_chain()
            .borrow()
            .clone()
            .expect("the port publishes a snapshot")
    };
    let acquire = || {
        let snapshot = snapshot();
        let tip = snapshot.frontiers.header_best.hash;
        port.acquire_header_path(AcquirePath {
            source: probe_id(),
            session_id: 1,
            scope: zakura_header_chain::HeaderWorkAuthority::for_target(&snapshot, tip),
            target_tip_hash: tip,
            locator_hashes: vec![fixture.anchor().hash],
        })
    };
    let acquired = |reply| match reply {
        Ok(AcquirePathReply::Acquired(path)) => *path,
        other => panic!("the probe acquires: {other:?}"),
    };
    match probe {
        Probe::Idle => {}
        Probe::AcquireRelease => {
            let path = acquired(acquire().await);
            port.release_header_path(path)
                .await
                .expect("the probe releases");
        }
        Probe::AcquireHold => {
            let _path = acquired(acquire().await);
            std::future::pending::<()>().await;
        }
        Probe::ReadReleased => {
            let path = acquired(acquire().await);
            port.release_header_path(path.clone())
                .await
                .expect("the probe releases");
            let read = ReadPath {
                after_hash: fixture.anchor().hash,
                max_header_count: 160,
                want_tree_aux: false,
            };
            out.borrow_mut().read = Some(format!("{:?}", port.read_header_path(path, read).await));
        }
        Probe::AbandonAcquire => {
            let mut call = acquire();
            // Submits the call on its first poll, then leaves it unread
            // while the bound makes the run complete it.
            std::future::poll_fn(|cx| {
                let _ = call.as_mut().poll(cx);
                std::task::Poll::Ready(())
            })
            .await;
            zakura_rt::time::sleep(Duration::from_secs(6)).await;
            drop(call);
        }
        Probe::SweepLate => {
            zakura_rt::time::sleep(Duration::from_secs(31)).await;
            let path = acquired(acquire().await);
            port.release_header_path(path)
                .await
                .expect("the probe releases");
        }
        Probe::Prepare { len } => {
            let anchor = fixture.anchor();
            let headers = fixture.extend(anchor, len, 0);
            let request = fixture.target_request(&snapshot(), anchor, &headers, 1);
            let prepared = port
                .prepare_header_target(request)
                .await
                .expect("fixture headers prepare");
            out.borrow_mut().prepared = Some((prepared, zakura_rt::time::utc_now()));
        }
    }
}

impl Subsystem for Probing {
    const NAME: &'static str = "test/header-probe";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = Probed;
    type Prepared = ProbeReady;

    /// The storage run's config, with the scenario's clock origin.
    fn config(scenario: &Probed) -> zakura_sim::RunConfig {
        let mut config = zakura_sim::harness::header_sync::storage_config();
        config.scheduler.utc_origin = scenario
            .utc_origin
            .map(|secs| std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs));
        config.budget = scenario.budget.unwrap_or(config.budget);
        config
    }

    /// The storage ports, and an empty probe output.
    fn prepare(scenario: &Probed) -> ProbeReady {
        ProbeReady {
            ports: Storage::prepare(&scenario.storage),
            out: Default::default(),
        }
    }

    /// The storage harness, then the probe.
    fn build(sim: &mut zakura_sim::Sim, scenario: &Probed, ready: ProbeReady) -> Self {
        let storage = Storage::build(sim, &scenario.storage, ready.ports.clone());
        sim.scheduler().spawn_root(
            "probe",
            probe(
                scenario.probe,
                ready.ports.node.clone(),
                ready.ports.fixture.clone(),
                ready.out,
            ),
        );
        Probing(std::rc::Rc::new(storage))
    }

    /// The storage harness's properties.
    fn properties() -> Vec<zakura_sim::property::Property<Self>> {
        storage_properties()
    }

    /// The storage harness's sources.
    fn sources() -> zakura_sim::property::Sources {
        storage_sources()
    }

    /// Unused: probes are only built by hand.
    fn draw(_: &[u8]) -> Probed {
        unreachable!("probe scenarios aren't drawn")
    }
}

/// Runs `scenario` with its probe under `input`, with `extra` checking each
/// step. Returns the report, the ports and the probe's output.
fn run_probed(
    scenario: &Probed,
    input: PlanInput,
    extra: &mut dyn Checker,
) -> (RunReport, ProbeReady) {
    let ready = Probing::prepare(scenario);
    let report = run_prepared::<Probing>(
        scenario,
        ready.clone(),
        input,
        &mut MemorySink::default(),
        Some(extra),
    )
    .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    (report, ready)
}

/// The node alone at A3, or dialing `mirror-0` at genesis, converging,
/// with `holds`.
fn node_at_a3(
    with_mirror: bool,
    holds: Vec<zakura_sim::harness::header_node::policy::Hold>,
) -> StorageScenario {
    StorageScenario {
        node: Chain::main(3),
        mirrors: if with_mirror {
            vec![mirror(Chain::default(), Dialer::Node)]
        } else {
            Vec::new()
        },
        policy: Policy {
            holds,
            ..Policy::converging()
        },
        controls: Vec::new(),
    }
}

/// A hold on `select` that opens at once and offers `then`.
fn fault(
    select: zakura_sim::harness::header_node::policy::Select,
    then: zakura_sim::harness::header_node::policy::Allow,
) -> zakura_sim::harness::header_node::policy::Hold {
    use zakura_sim::harness::header_node::policy::{Hold, Select, Trigger};
    Hold {
        select,
        until: Trigger::Submitted(Select::held()),
        then: vec![then],
        deadline_ms: None,
    }
}

/// Each lease end, `(step, lease ID, release)`, in log order.
fn lease_ends(report: &RunReport) -> Vec<(u64, u64, String)> {
    facts(report)
        .into_iter()
        .filter_map(|(step, fact)| match fact {
            StorageFact::CapabilityEnded { lease, release } => {
                Some((step, lease.lease_id, release.clone()))
            }
            _ => None,
        })
        .collect()
}

/// Each of the five ways a lease ends happens, and reclaims the lease on
/// the step it happens: the live count drops by each lease that ended that
/// step, from the count the step before, net of leases made that step. No
/// lease ends twice, in the log or in the port's shutdown after it.
#[test]
fn every_lease_end_cause_reclaims_before_shutdown() {
    use zakura_sim::harness::header_node::policy::{Allow, OpKind, Select};

    let read = || Select::nth(OpKind::Read, 1);
    let release = || Select::nth(OpKind::Release, 1);
    let cases = [
        (
            "lease-consumed-by-read",
            node_at_a3(true, Vec::new()),
            Probe::Idle,
        ),
        (
            "lease-released",
            node_at_a3(false, Vec::new()),
            Probe::AcquireRelease,
        ),
        (
            "lease-released",
            node_at_a3(true, vec![fault(read(), Allow::Unavailable)]),
            Probe::Idle,
        ),
        (
            "lease-idle-expired",
            node_at_a3(
                true,
                vec![
                    fault(read(), Allow::Unavailable),
                    fault(release(), Allow::Unavailable),
                ],
            ),
            Probe::SweepLate,
        ),
        (
            "lease-abandoned-acquire",
            node_at_a3(false, Vec::new()),
            Probe::AbandonAcquire,
        ),
    ];
    for (cause, storage, probe) in cases {
        let scenario = Probed {
            storage,
            probe,
            utc_origin: None,
            budget: None,
        };
        for input in inputs(8, 256) {
            // The live lease count after each step, from step 0.
            let live = std::cell::RefCell::new(vec![0]);
            let ready = Probing::prepare(&scenario);
            let port = ready.ports.node.clone();
            let mut count = after_step(|_| {
                live.borrow_mut().push(port.engine().live_leases());
                None
            });
            let report = run_prepared::<Probing>(
                &scenario,
                ready.clone(),
                input,
                &mut MemorySink::default(),
                Some(&mut count),
            )
            .unwrap_or_else(|error| panic!("{cause}: {error}"));
            drop(count);
            assert_eq!(report.end.violation, None, "{cause}: {:?}", report.end);
            let live = live.into_inner();
            let ends = lease_ends(&report);
            let (step, _, _) = ends
                .iter()
                .find(|(_, _, release)| release == cause)
                .unwrap_or_else(|| panic!("{cause} happens: {ends:?}"));
            let step = usize::try_from(*step).unwrap();
            // A sweep's own acquire can lease in the same step, so count
            // that step's ends and new leases too.
            let (mut ended_here, mut leased_here) = (0, 0);
            for (at, fact) in facts(&report) {
                if usize::try_from(at).unwrap() == step {
                    ended_here += usize::from(matches!(fact, StorageFact::CapabilityEnded { .. }));
                    leased_here += usize::from(matches!(fact, StorageFact::Leased { .. }));
                }
            }
            assert_eq!(
                live[step] + ended_here,
                live[step - 1] + leased_here,
                "{cause} reclaims its lease on its step {step}"
            );
            // The run froze its log before shutting the port down, so the
            // shutdown's facts are still in the port.
            let mut ended: Vec<u64> = ends.into_iter().map(|(_, lease, _)| lease).collect();
            for fact in ready.ports.node.take_facts() {
                if let zakura_storage::header::controlled::FactKind::Lease(
                    zakura_storage::header::evidence::LeaseFact::CapabilityEnded { lease, .. },
                ) = fact.kind
                {
                    ended.push(lease.lease_id);
                }
            }
            let distinct: std::collections::BTreeSet<&u64> = ended.iter().collect();
            assert_eq!(distinct.len(), ended.len(), "{cause}: a lease ended twice");
        }
    }
}

/// Accepted page work survives its session's cancellation and still
/// releases its lease. The node serves `mirror-0`, whose link closes once
/// the node's first read is submitted, and that read is held until the
/// connection has closed, then completes with `engine`. The publisher's
/// page job outlives the session and takes the reply, and the read consumes
/// its lease in the log, so the lease ends before shutdown, not by it.
#[test]
fn page_work_outlives_a_closed_session_and_ends_its_lease() {
    use zakura_sim::harness::header_node::{
        policy::{Allow, Hold, OpKind, Select, Trigger},
        ControlAction, ControlStep,
    };

    let read = || Select::nth(OpKind::Read, 1);
    let scenario = StorageScenario {
        controls: vec![ControlStep {
            peer: "mirror-0".to_owned(),
            action: ControlAction::Close,
            when: Some(Trigger::Submitted(read())),
        }],
        ..node_at_a3(
            true,
            vec![Hold {
                select: read(),
                until: Trigger::Closed("mirror-0".to_owned()),
                then: vec![Allow::Engine],
                deadline_ms: None,
            }],
        )
    };
    for input in inputs(8, 256) {
        let ports = Storage::prepare(&scenario);
        let port = ports.node.clone();
        let live = std::cell::Cell::new(None);
        let mut last = after_step(|_| {
            live.set(Some(port.engine().live_leases()));
            None
        });
        let report = run_on(&scenario, &ports, input, &mut last);
        drop(last);
        let facts = facts(&report);
        let (id, lease) = facts
            .iter()
            .find_map(|(_, fact)| match fact {
                StorageFact::Submitted {
                    id,
                    request: StorageRequest::ReadHeaderPath { lease, .. },
                    ..
                } => Some((*id, lease.clone().expect("the node's port issued it"))),
                _ => None,
            })
            .expect("the node reads a page for mirror-0");
        let step_of = |wanted: &dyn Fn(&StorageFact) -> bool| {
            facts
                .iter()
                .find(|(_, fact)| wanted(fact))
                .map(|(step, _)| *step)
        };
        let closed = report
            .picks
            .iter()
            .find(|pick| pick.identity == "peer:mirror-0:close")
            .expect("the close is picked")
            .step;
        let completed = step_of(&|fact| {
            matches!(fact, StorageFact::Completed { id: c, outcome, .. } if *c == id && outcome == "engine")
        })
        .expect("the held read completes with engine");
        let taken = step_of(&|fact| matches!(fact, StorageFact::ReplyTaken { id: t } if *t == id))
            .expect("the page job outlives the session and takes the read's reply");
        let ended = step_of(&|fact| {
            matches!(
                fact,
                StorageFact::CapabilityEnded { lease: l, release }
                    if l.lease_id == lease.lease_id && release == "lease-consumed-by-read"
            )
        })
        .expect("the read consumes its lease during the run");
        assert!(
            closed < ended && ended <= completed && completed < taken,
            "closed {closed}, lease ended {ended}, completed {completed}, taken {taken}"
        );
        assert_eq!(live.get(), Some(0), "no lease is live at the last step");
    }
}

/// The probe acquires a path to the node's tip, releases it, then reads the
/// released handle. The read replies `Unavailable`, never a page.
#[test]
fn ended_handle_reads_unavailable() {
    let scenario = Probed {
        storage: node_at_a3(false, Vec::new()),
        probe: Probe::ReadReleased,
        utc_origin: None,
        budget: None,
    };
    for input in inputs(8, 256) {
        let (_, ready) = run_probed(&scenario, input, &mut zakura_sim::NoChecks);
        let read = ready.out.borrow_mut().read.take();
        assert_eq!(read.as_deref(), Some("Ok(Unavailable)"));
    }
}

/// A run that stops at its step budget with a locator pending and a lease
/// live, both witnessed at its last step, shuts down cleanly: nothing
/// leaks, the port then has nothing pending and no live lease, and the
/// frozen log has no shutdown entry. The node at A3 holds its locator toward
/// `mirror-0`'s A5 forever, and the probe holds a path to A3, which nothing
/// sweeps since nobody else acquires.
#[test]
fn run_end_returns_every_permit_and_lease() {
    use zakura_sim::harness::header_node::policy::{Hold, OpKind, Select, Trigger};

    let scenario = Probed {
        storage: StorageScenario {
            node: Chain::main(3),
            policy: Policy {
                holds: vec![Hold {
                    select: Select::nth(OpKind::Locator, 1),
                    until: Trigger::Closed("nobody".to_owned()),
                    then: Vec::new(),
                    deadline_ms: None,
                }],
                ..Policy::converging()
            },
            ..StorageScenario::default()
        },
        probe: Probe::AcquireHold,
        utc_origin: None,
        budget: None,
    };
    for input in inputs(8, 256) {
        let last = std::cell::Cell::new((false, 0));
        let ready = Probing::prepare(&scenario);
        let port = ready.ports.node.clone();
        let mut witness = after_step(|_| {
            let locator = port
                .pending()
                .iter()
                .any(|op| op.op == zakura_storage::header::HeaderOp::ContinuationLocator);
            last.set((locator, port.engine().live_leases()));
            None
        });
        let report = run_prepared::<Probing>(
            &scenario,
            ready.clone(),
            input,
            &mut MemorySink::default(),
            Some(&mut witness),
        )
        .unwrap_or_else(|error| panic!("{error}"));
        drop(witness);
        assert_eq!(
            report.end.outcome,
            zakura_sim::Outcome::Budget,
            "{:?}",
            report.end
        );
        let (locator, leases) = last.get();
        assert!(locator && leases > 0, "at the end: {:?}", last.get());
        assert_eq!(report.cleanup.leaked, 0);
        assert_eq!(port.pending(), [], "shutdown ends every operation");
        assert_eq!(port.engine().live_leases(), 0, "and every lease");
        assert!(
            !facts(&report)
                .iter()
                .any(|(_, fact)| matches!(fact, StorageFact::ShutDown { .. })),
            "shutdown isn't logged"
        );
    }
}

/// The peak number of the node's locators pending at once in each wave: the
/// first, before its first 16 locators completed, and the second, among
/// locators submitted after step `grown`, when every mirror's new tip was
/// committed. A locator submitted before then, such as a retry toward an
/// old tip, never counts toward the second wave.
fn locator_waves(report: &RunReport, grown: u64) -> [usize; 2] {
    let mut waves = std::collections::BTreeMap::new();
    let (mut pending, mut completed, mut peaks) = ([0; 2], 0, [0; 2]);
    for (step, fact) in facts(report) {
        match fact {
            StorageFact::Submitted {
                id,
                request: StorageRequest::ContinuationLocator,
                ..
            } => {
                let wave = if step > grown {
                    Some(1)
                } else {
                    (completed < 16).then_some(0)
                };
                if let Some(wave) = wave {
                    waves.insert(*id, wave);
                    pending[wave] += 1;
                }
            }
            StorageFact::Completed { id, .. } => {
                if let Some(wave) = waves.get(id) {
                    pending[*wave] -= 1;
                    completed += usize::from(*wave == 0);
                }
            }
            _ => {}
        }
        for wave in 0..2 {
            peaks[wave] = peaks[wave].max(pending[wave]);
        }
    }
    peaks
}

/// Locator capacity is 16, and it all comes back. Sixteen mirrors at
/// distinct height-1 tips draw 16 locators, each held until all 16 are
/// pending. Once they complete, each mirror grows a new tip. Locators
/// carry no target, so the second wave counts only locators submitted after
/// every mirror's own watch has published its new tip, when no mirror still
/// advertises an old one. Those are again held until all 16 are pending at
/// once. A permit
/// leaked by the first wave would cap the second below 16. The first wave
/// completes with `engine`, then, in a second variant, with `unavailable`.
#[test]
fn locator_capacity_recovers_after_success_and_failure() {
    use zakura_sim::harness::header_node::{
        policy::{Allow, Hold, OpKind, Select, Trigger},
        ControlAction, ControlStep,
    };

    let locators = || Select {
        kind: Some(OpKind::Locator),
        ..Select::default()
    };
    let submitted = |n| Trigger::Count(n, Box::new(Trigger::Submitted(locators())));
    let wave = |nths: std::ops::RangeInclusive<u32>, until: u32, then| {
        nths.map(move |nth| Hold {
            select: Select::nth(OpKind::Locator, nth),
            until: submitted(until),
            then: vec![then],
            deadline_ms: None,
        })
    };
    for first in [Allow::Engine, Allow::Unavailable] {
        let storage = StorageScenario {
            mirrors: (1..=16)
                .map(|branch| {
                    let fork = Some(Fork { branch, len: 1 });
                    mirror(Chain { main: 0, fork }, Dialer::Mirror)
                })
                .collect(),
            policy: Policy {
                holds: wave(1..=16, 16, first)
                    .chain(wave(17..=32, 32, Allow::Engine))
                    .collect(),
                ..Policy::converging()
            },
            controls: (1..=16)
                .map(|m| ControlStep {
                    peer: format!("mirror-{}", m - 1),
                    action: ControlAction::Grow { len: 1 },
                    when: (m == 1)
                        .then(|| Trigger::Count(16, Box::new(Trigger::Completed(locators())))),
                })
                .collect(),
            ..StorageScenario::default()
        };
        let scenario = Probed {
            storage,
            probe: Probe::Idle,
            utc_origin: None,
            budget: Some(30_000),
        };
        for input in inputs(8, 256) {
            // A pick only queues a growth, so the witness is each mirror's
            // own watch publishing its new height-2 tip.
            let ready = Probing::prepare(&scenario);
            let tips: Vec<_> = ready
                .ports
                .mirrors
                .iter()
                .map(|port| {
                    use zakura_node_services::header_chain::Port;
                    port.selected_chain()
                })
                .collect();
            let grown = std::cell::Cell::new(None);
            let mut witness = after_step(|cx| {
                let all = tips.iter().all(|tip| {
                    tip.borrow()
                        .as_ref()
                        .is_some_and(|snapshot| snapshot.frontiers.header_best.height.0 == 2)
                });
                if all && grown.get().is_none() {
                    grown.set(Some(cx.step));
                }
                None
            });
            let report = run_prepared::<Probing>(
                &scenario,
                ready.clone(),
                input,
                &mut MemorySink::default(),
                Some(&mut witness),
            )
            .unwrap_or_else(|error| panic!("{error}"));
            drop(witness);
            let grown = grown.get().expect("every mirror's growth committed");
            assert_eq!(locator_waves(&report, grown), [16, 16], "{first:?}");
        }
    }
}

/// Fixture headers on the main branch from genesis in these clock tests.
const CLOCK_HEADERS: u32 = 150;

/// A scenario clock origin, in Unix seconds, that puts the last
/// `deferred_margin` fixture headers more than 2 h past the start, so
/// preparation defers them.
fn origin_deferring(fixture: &zakura_storage::header::fixture::HeaderFixture, margin: u32) -> u64 {
    let genesis = u64::try_from(fixture.genesis_header().time.timestamp()).unwrap();
    genesis + u64::from(CLOCK_HEADERS - margin) - 7_200
}

/// Whether preparation at `now` defers a header whose time is `secs` Unix
/// seconds: past now plus 2 h.
fn deferred(secs: i64, now: std::time::SystemTime) -> bool {
    let now = now
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    i128::from(secs) * 1_000_000_000 > i128::try_from(now).unwrap() + 7_200 * 1_000_000_000
}

/// The probe prepares 150 fixture headers on the node's port with the
/// scenario clock set so the last ones are more than 2 h ahead. At the
/// completion, each prepared header is `DeferredUntil` 2 h before its time
/// exactly when it's more than 2 h past the scenario clock, and `Valid`
/// otherwise. Some are deferred, which host time never would, since every
/// fixture header is in the host's past.
#[test]
fn prepared_target_follows_the_scenario_clock() {
    use zakura_header_chain::HeaderValidationState;

    let fixture = zakura_storage::header::fixture::HeaderFixture::regtest();
    let scenario = Probed {
        storage: StorageScenario {
            mirrors: Vec::new(),
            ..StorageScenario::default()
        },
        probe: Probe::Prepare { len: CLOCK_HEADERS },
        utc_origin: Some(origin_deferring(&fixture, 100)),
        budget: None,
    };
    for input in inputs(8, 256) {
        let (_, ready) = run_probed(&scenario, input, &mut zakura_sim::NoChecks);
        let (prepared, now) = ready
            .out
            .borrow_mut()
            .prepared
            .take()
            .expect("the probe's prepare completed");
        let insert = prepared
            .into_insert(ready.ports.node.engine().adapter_key())
            .unwrap_or_else(|_| panic!("the node's port sealed the target"));
        let headers = insert.batch.headers();
        assert_eq!(headers.len(), usize::try_from(CLOCK_HEADERS).unwrap());
        let mut deferrals = 0;
        for header in headers {
            let time = header.header.time.timestamp();
            match header.validation {
                HeaderValidationState::DeferredUntil(until) => {
                    assert!(deferred(time, now), "{:?} at {now:?}", header.height);
                    assert_eq!(until.timestamp(), time - 7_200, "2 h before its time");
                    deferrals += 1;
                }
                HeaderValidationState::Valid => {
                    assert!(!deferred(time, now), "{:?} at {now:?}", header.height);
                }
            }
        }
        assert!(deferrals > 0, "no header was deferred at {now:?}");
    }
}

/// The node syncs 150 headers from `mirror-0` with the scenario clock set so
/// the last ones are more than 2 h ahead. Its first apply is held until 20 s
/// into the run, so admission sees a later clock than preparation did and
/// must promote the deferred headers that came due meanwhile. Right after
/// that apply completes, its tip is the last header no more than 2 h past
/// the scenario clock then, and some header is still deferred. With the
/// clock a day past every header, it reaches `mirror-0`'s tip.
#[test]
fn selected_tip_follows_the_scenario_clock() {
    use zakura_sim::harness::header_node::policy::{Allow, Hold, OpKind, Select, Trigger};

    let fixture = zakura_storage::header::fixture::HeaderFixture::regtest();
    let late = origin_deferring(&fixture, 100);
    let ahead = origin_deferring(&fixture, 0) + 7_200 + 86_400;
    let held_apply = Hold {
        select: Select::nth(OpKind::Apply, 1),
        until: Trigger::VirtualTime(20_000),
        then: vec![Allow::Engine],
        deadline_ms: Some(21_000),
    };
    for (origin, all) in [(late, false), (ahead, true)] {
        let scenario = Probed {
            storage: StorageScenario {
                mirrors: vec![mirror(Chain::main(CLOCK_HEADERS), Dialer::Mirror)],
                policy: Policy {
                    holds: vec![held_apply.clone()],
                    ..Policy::converging()
                },
                ..StorageScenario::default()
            },
            probe: Probe::Idle,
            utc_origin: Some(origin),
            budget: None,
        };
        for input in inputs(8, 256) {
            let ready = Probing::prepare(&scenario);
            let port = ready.ports.node.clone();
            let applies = std::cell::RefCell::new(std::collections::BTreeSet::new());
            let first = std::cell::Cell::new(None);
            let mut watch = after_step(|cx| {
                for obs in cx.observations() {
                    let Observation::Storage {
                        instance: 0, fact, ..
                    } = obs
                    else {
                        continue;
                    };
                    match fact {
                        StorageFact::Submitted {
                            id,
                            request: StorageRequest::ApplyHeaderTarget { .. },
                            ..
                        } => {
                            applies.borrow_mut().insert(*id);
                        }
                        StorageFact::Completed { id, .. }
                            if applies.borrow().contains(id) && first.get().is_none() =>
                        {
                            let now = std::time::UNIX_EPOCH
                                + std::time::Duration::from_secs(origin)
                                + cx.now.since_start();
                            let tip = port.engine().snapshot().frontiers.header_best.height.0;
                            first.set(Some((now, tip)));
                        }
                        _ => {}
                    }
                }
                None
            });
            run_prepared::<Probing>(
                &scenario,
                ready.clone(),
                input,
                &mut MemorySink::default(),
                Some(&mut watch),
            )
            .unwrap_or_else(|error| panic!("{error}"));
            drop(watch);
            let (now, tip) = first.get().expect("the node applied");
            let headers = fixture.extend(fixture.anchor(), CLOCK_HEADERS, 0);
            let eligible = headers
                .iter()
                .take_while(|header| !deferred(header.time.timestamp(), now))
                .count();
            if all {
                assert_eq!(eligible, usize::try_from(CLOCK_HEADERS).unwrap());
                assert_eq!(
                    port.engine().snapshot().frontiers.header_best.height.0,
                    CLOCK_HEADERS
                );
            } else {
                assert!(
                    eligible < usize::try_from(CLOCK_HEADERS).unwrap(),
                    "some header is deferred at {now:?}"
                );
                assert_eq!(
                    usize::try_from(tip).unwrap(),
                    eligible,
                    "the tip at {now:?}"
                );
            }
        }
    }
}
