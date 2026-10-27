//! The node's reactor calls paired with its storage receipts (M4-B3b): a
//! timed-out locator pairs with whichever drop its caller made, a port
//! future caught panicking fails the run as `ReactorPortPanicked`, and a
//! hold can wait for the reactor to handle a call.
#![cfg(zakura_sim)]

mod support;

use std::{
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use support::{inputs, run};
use zakura_chain::block;
use zakura_header_chain::{BodyWorkOwner, EngineSnapshot, HeaderLocator};
use zakura_network::zakura::observe::{CallEnd, HeaderSyncObserver, ReactorEvent};
use zakura_node_services::header_chain::{
    AcquirePath, AcquirePathReply, ApplyHeaderTargetReply, HeaderChainFuture, LocatorReadPermit,
    Port, PortError, PrepareHeaderTarget, PrepareHeaderTargetReply, PreparedHeaderTarget, ReadPath,
    ReadPathReply, RetainedHeaderPath, SelectedChainMembership, VctRepairContextReply,
};
use zakura_scheduler::SimInstant;
use zakura_sim::{
    harness::{
        header_node::{
            policy::{Allow, Hold, OpKind, Select, Trigger},
            preload,
            tap::ReactorTap,
            Chain, Control, ControlAction, ControlStep, DispatchFlag, Effects, HeaderNode, Policy,
            PolicySource, Topology,
        },
        header_sync::{storage_config, Storage, StorageScenario},
        run_prepared, Subsystem,
    },
    observe::{ReactorCall, ReactorFact, StorageFact, StorageRequest},
    ActionSource, Boundary, HarnessFailure, Identity, MemorySink, Observation, ObservationLog,
    PlanInput, RunReport,
};
use zakura_storage::header::{
    adapter::EnginePort, controlled::ControlledPort, fixture::HeaderFixture,
};

/// Runs `scenario` on the storage harness under `input`. The run must end
/// without a violation or harness failure.
fn storage_run(scenario: &StorageScenario, input: PlanInput) -> (RunReport, Arc<ControlledPort>) {
    let ports = Storage::prepare(scenario);
    let node = ports.node.clone();
    let report = run_prepared::<Storage>(scenario, ports, input, &mut MemorySink::default(), None)
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    (report, node)
}

/// The default scenario, the node at genesis and `mirror-0` at A5,
/// converging, with `holds`.
fn holding(holds: Vec<Hold>) -> StorageScenario {
    StorageScenario {
        policy: Policy {
            holds,
            ..Policy::converging()
        },
        ..StorageScenario::default()
    }
}

/// The node's reactor events, with their storage watermarks, in log order.
fn events(report: &RunReport) -> Vec<(u64, &ReactorFact)> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Reactor {
                storage_seq, event, ..
            } => Some((*storage_seq, event)),
            _ => None,
        })
        .collect()
}

/// The node's storage fact numbered `seq`.
fn fact_at(report: &RunReport, seq: u64) -> &StorageFact {
    report
        .log
        .entries()
        .iter()
        .find_map(|entry| match &entry.obs {
            Observation::Storage {
                instance: 0,
                seq: logged,
                fact,
                ..
            } if *logged == seq => Some(fact),
            _ => None,
        })
        .unwrap_or_else(|| panic!("the node's storage logged fact {seq}"))
}

/// With the node's first locator held until 30 s after its submission, the
/// reactor's timeout and the completion share an instant. The caller drops
/// its call either before the completion, leaving `CallerDropped`, or after
/// it, leaving `ReplyDropped`. Both histories happen, and each run's
/// `timed-out` end pairs with that operation's drop.
#[test]
fn timed_out_receipt_pairs_with_either_drop() {
    let scenario = holding(vec![Hold {
        select: Select::nth(OpKind::Locator, 1),
        until: Trigger::VirtualTime(30_000),
        then: vec![Allow::Engine],
        deadline_ms: None,
    }]);
    let (mut caller_dropped, mut reply_dropped) = (0, 0);
    for input in inputs(64, 256) {
        let (report, _) = storage_run(&scenario, input);
        for (storage_seq, event) in events(&report) {
            let ReactorFact::CallEnded { end, op, .. } = event else {
                continue;
            };
            if end != "timed-out" {
                continue;
            }
            let id = op.expect("a timed-out call is paired");
            match fact_at(&report, storage_seq) {
                StorageFact::CallerDropped { id: dropped } if *dropped == id => {
                    caller_dropped += 1;
                }
                StorageFact::ReplyDropped { id: dropped } if *dropped == id => {
                    reply_dropped += 1;
                }
                other => panic!("call ended timed-out at {other:?}"),
            }
        }
    }
    assert!(
        caller_dropped > 0,
        "no plan dropped the call before completing it"
    );
    assert!(
        reply_dropped > 0,
        "no plan dropped the call after completing it"
    );
}

/// Where [`PanickingPort`] panics in its apply.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PanicAt {
    /// Before it delegates the call.
    BeforeDelegating,
    /// After it takes the delegated call's reply.
    AfterReply,
}

/// The node's controlled port, except that every apply panics.
struct PanickingPort {
    inner: Arc<ControlledPort>,
    at: PanicAt,
    /// Set just before an apply panics.
    panicking: Arc<AtomicBool>,
}

impl Port for PanickingPort {
    fn continuation_locator(
        &self,
        permit: LocatorReadPermit,
    ) -> HeaderChainFuture<'_, Result<Option<HeaderLocator>, PortError>> {
        self.inner.continuation_locator(permit)
    }

    fn vct_repair_context(
        &self,
        owner: BodyWorkOwner,
        height: block::Height,
    ) -> HeaderChainFuture<'_, Result<VctRepairContextReply, PortError>> {
        self.inner.vct_repair_context(owner, height)
    }

    fn acquire_header_path(
        &self,
        request: AcquirePath,
    ) -> HeaderChainFuture<'_, Result<AcquirePathReply, PortError>> {
        self.inner.acquire_header_path(request)
    }

    fn read_header_path(
        &self,
        path: RetainedHeaderPath,
        request: ReadPath,
    ) -> HeaderChainFuture<'_, Result<ReadPathReply, PortError>> {
        self.inner.read_header_path(path, request)
    }

    fn release_header_path(
        &self,
        path: RetainedHeaderPath,
    ) -> HeaderChainFuture<'_, Result<(), PortError>> {
        self.inner.release_header_path(path)
    }

    fn prepare_header_target(
        &self,
        request: PrepareHeaderTarget,
    ) -> HeaderChainFuture<'_, PrepareHeaderTargetReply> {
        self.inner.prepare_header_target(request)
    }

    /// Panics where `at` says, delegating the call first for
    /// [`PanicAt::AfterReply`].
    fn apply_header_target(
        &self,
        target: PreparedHeaderTarget,
    ) -> HeaderChainFuture<'_, ApplyHeaderTargetReply> {
        Box::pin(async move {
            if self.at == PanicAt::BeforeDelegating {
                self.panicking.store(true, Ordering::Relaxed);
                panic!("the test port panics before delegating its apply");
            }
            let _reply = self.inner.apply_header_target(target).await;
            self.panicking.store(true, Ordering::Relaxed);
            panic!("the test port panics after taking its apply's reply");
        })
    }

    fn selected_chain(&self) -> zakura_rt::sync::watch::Receiver<Option<EngineSnapshot>> {
        self.inner.selected_chain()
    }

    fn selected_chain_contains(
        &self,
        hash: block::Hash,
    ) -> HeaderChainFuture<'_, Result<SelectedChainMembership, PortError>> {
        self.inner.selected_chain_contains(hash)
    }
}

/// A source, added before the node's tap, whose observation fails from the
/// step an apply panics on.
struct FailsOnPanic(Arc<AtomicBool>);

impl ActionSource for FailsOnPanic {
    fn enabled(&self, _: SimInstant, _: &mut Vec<(Identity, u64)>) {}

    fn boundaries(&self, _: SimInstant, _: &mut Vec<Boundary>) {}

    fn execute(&mut self, _: u64, _: &mut ObservationLog) {
        unreachable!("it offers no actions");
    }

    fn observe(&mut self, _: &mut ObservationLog) -> Result<(), String> {
        match self.0.load(Ordering::Relaxed) {
            true => Err("EarlierFailure: a source before the node's failed".to_owned()),
            false => Ok(()),
        }
    }
}

/// The node at genesis, tapped, syncing from `mirror-0` at A5 through a
/// [`PanickingPort`] that panics `at`, converging. With `close`, the node's
/// first apply is held until `mirror-0`'s link has closed and a second has
/// passed, and the `Plan` may close the link once that apply is submitted.
/// With `fail_first`, a [`FailsOnPanic`] source comes before the node's.
fn panic_run(at: PanicAt, close: bool, fail_first: bool, input: PlanInput) -> RunReport {
    let fixture = HeaderFixture::regtest();
    let node = Arc::new(ControlledPort::new(&fixture));
    let mirror = Arc::new(EnginePort::new(&fixture));
    preload(&mirror, &fixture, &Chain::main(5));
    let first_apply = Select::nth(OpKind::Apply, 1);
    let (holds, controls) = if close {
        (
            vec![Hold {
                select: first_apply.clone(),
                // Time moves only once no task is ready, so a close before
                // then is handled, and the apply's owner retired, first.
                until: Trigger::All(vec![
                    Trigger::Closed("mirror-0".to_owned()),
                    Trigger::VirtualTime(1_000),
                ]),
                then: vec![Allow::Engine],
                deadline_ms: None,
            }],
            vec![ControlStep {
                peer: "mirror-0".to_owned(),
                action: ControlAction::Close,
                when: Some(Trigger::Submitted(first_apply)),
            }],
        )
    } else {
        (Vec::new(), Vec::new())
    };
    let panicking = Arc::new(AtomicBool::new(false));
    let mut topology = None;
    let report = run(
        storage_config(),
        input,
        |sim| {
            if fail_first {
                sim.add_source(Box::new(FailsOnPanic(panicking.clone())));
            }
            let dispatch = DispatchFlag::default();
            let effects = Effects::default();
            let policy = Policy {
                holds,
                ..Policy::converging()
            };
            let storage = PolicySource::new(sim, node.clone(), policy, effects.clone());
            let (observer, tap) = ReactorTap::new(sim, &node, "node");
            let storage = storage.with_tap(tap);
            let predicates = storage.predicates();
            sim.add_source(Box::new(storage));
            let port: Arc<dyn Port> = Arc::new(PanickingPort {
                inner: node.clone(),
                at,
                panicking,
            });
            let nodes = vec![
                HeaderNode::spawn(
                    sim,
                    "node",
                    1,
                    port,
                    &fixture,
                    Chain::default().tip(&fixture),
                    dispatch.clone(),
                    Some(observer),
                ),
                HeaderNode::spawn(
                    sim,
                    "mirror-0",
                    2,
                    mirror.clone(),
                    &fixture,
                    Chain::main(5).tip(&fixture),
                    dispatch,
                    None,
                ),
            ];
            let linked = Rc::new(Topology::new(nodes, effects));
            linked.link(1, 0);
            let control = Control::new(sim, linked.clone(), fixture.clone(), controls, predicates);
            sim.add_source(Box::new(control));
            topology = Some(linked);
        },
        &mut zakura_sim::NoChecks,
    );
    drop(topology);
    report
}

/// What a port panic run showed.
#[derive(Debug, Default)]
struct Panicked {
    /// The panicking call.
    call: u64,
    /// Why its owner was retired before it ended, if it was. Then the call
    /// held the owner's last header capacity lease.
    owner_retired: Option<String>,
}

/// Checks that `report` ended with `ReactorPortPanicked` for the node's first
/// apply call, logged as `port-panicked` with no operation, and that no
/// call is paired with any apply operation. With `submitted`, the apply's
/// operation must have taken its reply.
fn panicked(report: &RunReport, submitted: bool) -> Panicked {
    let Some(HarnessFailure::ReactorPortPanicked { source, call }) = &report.end.harness else {
        panic!("expected a reactor port panic, got {:?}", report.end);
    };
    assert_eq!(source, "reactor:node#0");
    let events = events(report);
    let dispatched = events
        .iter()
        .find_map(|(_, event)| match event {
            ReactorFact::Dispatched {
                call,
                port: ReactorCall::Apply { owner, .. },
            } => Some((*call, owner.clone())),
            _ => None,
        })
        .expect("the node dispatched an apply");
    assert_eq!(*call, dispatched.0);
    let ended = events
        .iter()
        .position(|(_, event)| {
            matches!(event, ReactorFact::CallEnded { call: ended, .. } if ended == call)
        })
        .expect("the panicking call's end is logged");
    assert_eq!(
        events[ended].1,
        &ReactorFact::CallEnded {
            call: *call,
            end: "port-panicked".to_owned(),
            op: None,
        }
    );
    let applies: Vec<u64> = report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Storage {
                instance: 0,
                fact:
                    StorageFact::Submitted {
                        id,
                        request: StorageRequest::ApplyHeaderTarget { .. },
                        ..
                    },
                ..
            } => Some(*id),
            _ => None,
        })
        .collect();
    let taken = applies.iter().any(|apply| {
        report.log.entries().iter().any(|entry| {
            matches!(
                &entry.obs,
                Observation::Storage { instance: 0, fact: StorageFact::ReplyTaken { id }, .. }
                    if id == apply
            )
        })
    });
    assert_eq!(taken, submitted, "the apply's reply was taken: {taken}");
    let paired_apply = events.iter().any(|(_, event)| {
        matches!(event, ReactorFact::CallEnded { op: Some(op), .. } if applies.contains(op))
    });
    assert!(!paired_apply, "no call pairs with an apply operation");
    let owner_retired = events[..ended].iter().find_map(|(_, event)| match event {
        ReactorFact::Retired {
            request, reason, ..
        } if request.owner == dispatched.1 => Some(reason.clone()),
        _ => None,
    });
    Panicked {
        call: *call,
        owner_retired,
    }
}

/// A port future caught panicking ends the run with
/// `ReactorPortPanicked`, logged first with `op: null`, whether it panicked
/// before submitting its apply or after taking the apply's reply, which
/// stays unpaired. The third variant retires the apply's owner first, by
/// closing `mirror-0`'s link while the apply is held, so the call holds the
/// owner's last header capacity lease. Releasing that lease reaches a
/// scheduling point, which the panic's end comes before, so its receipt
/// and its end share a drain and the panic outranks the unpaired receipt.
/// The second variant's commit can retire the owner `snapshot_obsolete`
/// before the reactor takes the reply, which makes it a last lease case
/// too, but nothing forces that.
#[test]
fn port_panic_is_a_harness_failure() {
    for input in inputs(8, 256) {
        let report = panic_run(PanicAt::BeforeDelegating, false, false, input.clone());
        assert_eq!(panicked(&report, false).owner_retired, None);
        panicked(&panic_run(PanicAt::AfterReply, false, false, input), true);
    }
    for input in inputs(32, 256) {
        let report = panic_run(PanicAt::AfterReply, true, false, input);
        let seen = panicked(&report, true);
        assert!(seen.call > 0);
        assert_eq!(seen.owner_retired.as_deref(), Some("disconnected"));
    }
}

/// A port panic outranks another source's observation failure in the same
/// step, even one that observes before the node's tap.
#[test]
fn port_panic_outranks_an_earlier_sources_failure() {
    for input in inputs(8, 256) {
        let report = panic_run(PanicAt::AfterReply, false, true, input.clone());
        panicked(&report, true);
        let report = panic_run(PanicAt::BeforeDelegating, false, true, input);
        panicked(&report, false);
    }
}

/// An observer that forwards to the tap's, except that it panics on the
/// first replied call end instead of recording it, as an observer bug would.
#[derive(Debug)]
struct PanicsOnReply(Arc<dyn HeaderSyncObserver>);

impl HeaderSyncObserver for PanicsOnReply {
    /// Panics on a replied `CallEnded`, which unwinds the reactor, and
    /// forwards anything else.
    fn observe(&self, event: ReactorEvent) {
        if matches!(
            event,
            ReactorEvent::CallEnded {
                end: CallEnd::Replied,
                ..
            }
        ) {
            panic!("the test observer panics on a replied call end");
        }
        self.0.observe(event);
    }
}

/// The node at genesis, tapped through [`PanicsOnReply`], syncing from
/// `mirror-0` at A5, converging.
fn observer_panic_run(input: PlanInput) -> RunReport {
    let fixture = HeaderFixture::regtest();
    let node = Arc::new(ControlledPort::new(&fixture));
    let mirror = Arc::new(EnginePort::new(&fixture));
    preload(&mirror, &fixture, &Chain::main(5));
    let mut topology = None;
    let report = run(
        storage_config(),
        input,
        |sim| {
            let dispatch = DispatchFlag::default();
            let effects = Effects::default();
            let storage =
                PolicySource::new(sim, node.clone(), Policy::converging(), effects.clone());
            let (observer, tap) = ReactorTap::new(sim, &node, "node");
            sim.add_source(Box::new(storage.with_tap(tap)));
            let nodes = vec![
                HeaderNode::spawn(
                    sim,
                    "node",
                    1,
                    node.clone(),
                    &fixture,
                    Chain::default().tip(&fixture),
                    dispatch.clone(),
                    Some(Arc::new(PanicsOnReply(observer))),
                ),
                HeaderNode::spawn(
                    sim,
                    "mirror-0",
                    2,
                    mirror.clone(),
                    &fixture,
                    Chain::main(5).tip(&fixture),
                    dispatch,
                    None,
                ),
            ];
            let linked = Rc::new(Topology::new(nodes, effects));
            linked.link(1, 0);
            topology = Some(linked);
        },
        &mut zakura_sim::NoChecks,
    );
    drop(topology);
    report
}

/// A reactor that unwinds from an observer panic ends the run as
/// `ReactorGone` on the node's tap. The call whose end panicked took its
/// storage receipt, which no `CallEnded` pairs with, but the tap reports
/// the reactor crash rather than that unpaired receipt.
#[test]
fn an_observer_panic_ends_the_run_reactor_gone() {
    for input in inputs(8, 256) {
        let report = observer_panic_run(input);
        let Some(HarnessFailure::SourceEvidence { source, detail }) = &report.end.harness else {
            panic!("expected a source evidence failure, got {:?}", report.end);
        };
        assert!(detail.starts_with("ReactorGone"), "{source}: {detail}");
        assert_eq!(source, "reactor:node#0");
    }
}

/// A hold on the node's applies until `Handled` opens once its reactor has
/// handled its first prepare, so the tap's events reach the policy's
/// predicates. Held until a VCT context call it never makes, no apply
/// completes.
#[test]
fn a_hold_waits_for_the_reactor_to_handle_a_call() {
    let fixture = HeaderFixture::regtest();
    let a5 = Chain::main(5).tip(&fixture);
    let hold = |until: Trigger| Hold {
        select: Select {
            kind: Some(OpKind::Apply),
            ..Select::default()
        },
        until,
        then: vec![Allow::Engine],
        deadline_ms: None,
    };
    for input in inputs(8, 256) {
        let scenario = holding(vec![hold(Trigger::Handled(Select::nth(
            OpKind::Prepare,
            1,
        )))]);
        let (report, node) = storage_run(&scenario, input.clone());
        assert_eq!(node.engine().snapshot().frontiers.header_best, a5);
        let events = events(&report);
        let prepare = events
            .iter()
            .find_map(|(_, event)| match event {
                ReactorFact::Dispatched {
                    call,
                    port: ReactorCall::Prepare { .. },
                } => Some(*call),
                _ => None,
            })
            .expect("the node prepares");
        assert!(events.iter().any(|(_, event)| matches!(
            event,
            ReactorFact::Handled { call, .. } if *call == prepare
        )));

        let scenario = holding(vec![hold(Trigger::Handled(Select {
            kind: Some(OpKind::VctContext),
            ..Select::default()
        }))]);
        let (_, node) = storage_run(&scenario, input);
        assert_ne!(node.engine().snapshot().frontiers.header_best, a5);
    }
}
