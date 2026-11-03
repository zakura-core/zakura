//! A block-sync node's tap as run evidence (BS4, gate BS.5): the run checks
//! the tap's checkpoint like storage's, so every event reaches the run log
//! once and in order, and a dropped, extra, changed or reordered event fails
//! the run. On an honest node, every block storage submission and read
//! pairs with the event that owns it, and the view folds the owners back to
//! empty. An operation that doesn't pair is an anomaly in the view, and the
//! run goes on; a lost storage fact fails the run.
#![cfg(zakura_sim)]

mod support;

use std::{cell::RefCell, collections::BTreeSet, rc::Rc, sync::Arc};

use support::{config, inputs, run};
use zakura_chain::block::Height;
use zakura_network::zakura::{
    block_sync_observe::{BlockEvent, BlockSyncObserver},
    ZakuraPeerId,
};
use zakura_scheduler::{SchedulerConfig, SimInstant};
use zakura_sim::{
    harness::{
        block_node::Policy,
        block_node::{
            control::{ControlAction, ControlStep},
            policy::{BlockAllow, BlockKind, Hold, Select, Trigger},
            probe::ProbeFrame,
            tap::BlockTap,
            view::{BlockSyncView, PairingKind},
        },
        block_sync::{BlockScenario, Ports, Storage},
        run_prepared, Subsystem,
    },
    observe::{BlockSyncFact, StorageBranch, StorageFact, StorageRequest, WorkOwner, WorkScope},
    ActionSource, Boundary, HarnessFailure, Identity, IdleStatus, MemorySink, Observation,
    ObservationLog, Outcome, PlanInput, RunConfig, RunReport, Sim,
};
use zakura_storage::block::controlled::ControlledBlockService;

/// A small run's config: 2 workers, the default stack, and 100 steps.
fn tap_config() -> RunConfig {
    let mut config = config(2, 100);
    config.scheduler.stack_size = SchedulerConfig::default().stack_size;
    config
}

/// A serving frame from height `start`, an event that changes no owner
/// state, so it's simple to emit by hand.
fn frame(start: u32) -> BlockEvent {
    BlockEvent::ServingFrame {
        peer: ZakuraPeerId::new(vec![1; 32]).expect("32 bytes is a valid peer id"),
        conn_id: 1,
        generation: 1,
        start: Height(start),
        message_type: 4,
        body_bytes: 0,
    }
}

/// The default scenario's block service, which no node uses.
fn service() -> Arc<ControlledBlockService> {
    Storage::prepare(&BlockScenario::default()).service
}

/// What a [`tap_run`] does beyond emitting its events.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct Twist {
    /// Drop the observer once the emitting task is done.
    drop_observer: bool,
    /// Drop the block service at setup, so the tap finds it gone.
    drop_service: bool,
}

/// A run in which a task named `block-sync` emits two events at each of
/// three steps through a tap, after one emitted at setup. The tap's reader
/// is added as `wrap` makes it, and the observer lives until the run ends
/// unless `twist` drops it.
fn tap_run(twist: Twist, wrap: impl FnOnce(BlockTap) -> Box<dyn ActionSource>) -> RunReport {
    tap_run_into(twist, Rc::default(), wrap)
}

/// [`tap_run`] with the tap folding into `view`.
fn tap_run_into(
    twist: Twist,
    view: Rc<RefCell<BlockSyncView>>,
    wrap: impl FnOnce(BlockTap) -> Box<dyn ActionSource>,
) -> RunReport {
    let mut kept = None;
    let mut service = Some(service());
    let report = run(
        tap_config(),
        PlanInput::Bytes(Vec::new()),
        |sim| {
            let held = service.as_ref().expect("the service is made before setup");
            let (observer, tap) = BlockTap::new(sim, held, "node", view);
            if twist.drop_service {
                service = None;
            }
            observer.observe(frame(1));
            if !twist.drop_observer {
                kept = Some(observer.clone());
            }
            spawn_emitter(sim, observer);
            sim.add_source(wrap(tap));
        },
        &mut zakura_sim::NoChecks,
    );
    drop(kept);
    drop(service);
    report
}

/// Spawns `block-sync`, which emits two events at each of three steps and
/// then exits, dropping its `observer`.
fn spawn_emitter(sim: &mut Sim, observer: Arc<dyn BlockSyncObserver>) {
    sim.scheduler().spawn_root("block-sync", async move {
        for start in 2..=4 {
            observer.observe(frame(start));
            observer.observe(frame(start + 100));
            zakura_rt::task::yield_now().await;
        }
    });
}

/// [`tap_run`] with the reader added plainly.
fn plain_run() -> RunReport {
    tap_run(Twist::default(), |tap| Box::new(tap))
}

/// The steps at which `report`'s log holds block-sync events.
fn delivering(report: &RunReport) -> BTreeSet<u64> {
    report
        .log
        .entries()
        .iter()
        .filter(|entry| matches!(entry.obs, Observation::BlockSync { .. }))
        .map(|entry| entry.step)
        .collect()
}

/// The source and detail of `report`'s evidence failure, which it must
/// have.
fn evidence_failure(report: &RunReport) -> (&str, &str) {
    assert_eq!(report.end.outcome, Outcome::Harness, "{:?}", report.end);
    let Some(HarnessFailure::SourceEvidence { source, detail }) = &report.end.harness else {
        panic!("expected a source evidence failure, got {:?}", report.end);
    };
    (source, detail)
}

/// Forwards every call but `observe` to `inner`.
macro_rules! forward {
    () => {
        fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
            self.inner.enabled(now, out);
        }

        fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
            self.inner.boundaries(now, out);
        }

        fn execute(&mut self, local: u64, log: &mut ObservationLog) {
            self.inner.execute(local, log);
        }
    };
}

/// A reader that skips its observation at step `skip`.
struct SkipAt<S> {
    inner: S,
    skip: u64,
}

impl<S: ActionSource> ActionSource for SkipAt<S> {
    forward!();

    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        if log.step() == self.skip {
            return Ok(());
        }
        self.inner.observe(log)
    }
}

/// A reader that, at step `at`, observes into a copy of the log and logs
/// what `edit` makes of what it delivered there.
struct Rewrite<S> {
    inner: S,
    at: u64,
    edit: fn(Vec<Observation>) -> Vec<Observation>,
}

impl<S: ActionSource> ActionSource for Rewrite<S> {
    forward!();

    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        if log.step() != self.at {
            return self.inner.observe(log);
        }
        let mut copy = log.clone();
        let cursor = copy.entries().len();
        self.inner.observe(&mut copy)?;
        let delivered: Vec<Observation> = copy.entries()[cursor..]
            .iter()
            .map(|entry| entry.obs.clone())
            .collect();
        for obs in (self.edit)(delivered) {
            log.push(obs);
        }
        Ok(())
    }
}

/// A reader dropped right after its observation at step `at`.
struct DropAfter<S> {
    inner: Option<S>,
    at: u64,
}

impl<S: ActionSource> ActionSource for DropAfter<S> {
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        if let Some(inner) = &self.inner {
            inner.enabled(now, out);
        }
    }

    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        if let Some(inner) = &self.inner {
            inner.boundaries(now, out);
        }
    }

    fn execute(&mut self, local: u64, log: &mut ObservationLog) {
        self.inner
            .as_mut()
            .expect("a dropped reader offers no actions")
            .execute(local, log);
    }

    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        let Some(inner) = &mut self.inner else {
            return Ok(());
        };
        inner.observe(log)?;
        if log.step() == self.at {
            self.inner = None;
        }
        Ok(())
    }
}

/// A reader that, at step `at`, hands its tap `fact` as the block storage
/// fact numbered `seq`, as if the node's storage had logged it.
struct Plant {
    inner: BlockTap,
    at: u64,
    seq: u64,
    fact: StorageFact,
}

impl ActionSource for Plant {
    forward!();

    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        if log.step() == self.at {
            self.inner.storage_facts([(self.seq, &self.fact)]);
        }
        self.inner.observe(log)
    }
}

/// A body submission under token `token`, as block storage logs it.
fn submission(token: u64) -> StorageFact {
    StorageFact::BlockSubmitted {
        id: 1,
        request: StorageRequest::SubmitBlock {
            owner: WorkOwner {
                kind: "body".to_owned(),
                scope: WorkScope {
                    header_generation: 1,
                    branch: StorageBranch {
                        anchor: "00".to_owned(),
                        target: "11".to_owned(),
                    },
                },
                verified_generation: Some(1),
                body_work_epoch: Some(1),
                session_id: 1,
                request_id: 1,
            },
            source: "01".to_owned(),
            token,
            height: Some(3),
            hash: "aa".to_owned(),
            parent: "bb".to_owned(),
            class: "full".to_owned(),
            decoded_bytes: 2_000,
        },
        stage: Some("queued".to_owned()),
    }
}

/// A body submission that no sequencer holder retained is the node's
/// anomaly, not the harness's: the view keeps it for a property to judge,
/// and the run goes on to end as a plain run does.
#[test]
fn an_unpaired_submission_is_an_anomaly_and_the_run_goes_on() {
    let plain = plain_run();
    let view = Rc::default();
    let report = tap_run_into(Twist::default(), Rc::clone(&view), |inner| {
        Box::new(Plant {
            inner,
            at: 1,
            seq: 1,
            fact: submission(9),
        })
    });
    assert_eq!(report.end, plain.end);
    let view = view.borrow();
    let kinds: Vec<_> = view
        .pairing
        .iter()
        .map(|anomaly| (anomaly.kind, anomaly.fact))
        .collect();
    assert_eq!(kinds, [(PairingKind::Unpaired, 1)]);
    assert!(
        view.pairing[0].detail.contains("token 9"),
        "{:?}",
        view.pairing
    );
    assert!(view.submissions.is_empty());
    assert!(view.anomalies.is_empty(), "{:?}", view.anomalies);
}

/// A storage fact that skips a sequence number means the harness lost one,
/// so it fails the run at that step with `LostFact`.
#[test]
fn a_lost_storage_fact_ends_the_run_there() {
    let report = tap_run(Twist::default(), |inner| {
        Box::new(Plant {
            inner,
            at: 1,
            seq: 2,
            fact: submission(9),
        })
    });
    let (source, detail) = evidence_failure(&report);
    assert_eq!(
        source, "source 0",
        "the run names a failing source by its index"
    );
    assert_eq!(report.end.steps, 1);
    assert!(detail.starts_with("LostFact"), "{detail}");
}

/// A plain run logs every event once, numbered from 1 in the order the
/// tasks recorded them.
#[test]
fn every_event_reaches_the_log_once_in_order() {
    let report = plain_run();
    assert_eq!(
        report.end.outcome,
        Outcome::Idle {
            status: IdleStatus::Quiescent
        },
        "{:?}",
        report.end
    );
    let logged: Vec<(u64, u32)> = report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::BlockSync {
                seq,
                event: BlockSyncFact::ServingFrame { start, .. },
                ..
            } => Some((*seq, *start)),
            _ => None,
        })
        .collect();
    assert_eq!(
        logged,
        [(1, 1), (2, 2), (3, 102), (4, 3), (5, 103), (6, 4), (7, 104)]
    );
}

/// A reader the run never observes fails setup with `NotObserved`, and one
/// that skips an interior observation fails there with `BoundarySkipped`.
#[test]
fn a_skipped_tap_observation_ends_the_run_there() {
    let last = plain_run().end.steps;
    assert!(last > 2, "the run has an interior step");
    for (skip, failure) in [
        (0, "NotObserved"),
        (1, "BoundarySkipped"),
        (last, "BoundarySkipped"),
    ] {
        let report = tap_run(Twist::default(), |inner| Box::new(SkipAt { inner, skip }));
        let (source, detail) = evidence_failure(&report);
        assert_eq!(source, "block-sync:node#0");
        assert_eq!(report.end.steps, skip);
        assert!(detail.starts_with(failure), "skipping {skip}: {detail}");
    }
}

/// Leaves the delivery as it was.
fn unchanged(delivered: Vec<Observation>) -> Vec<Observation> {
    delivered
}

/// Changes the first event's payload, keeping its numbers.
fn alter(mut delivered: Vec<Observation>) -> Vec<Observation> {
    if let Some(event) = delivered.iter_mut().find_map(|obs| match obs {
        Observation::BlockSync { event, .. } => Some(event),
        _ => None,
    }) {
        *event = BlockSyncFact::SequencerClosed;
    }
    delivered
}

/// Swaps the first two events.
fn reorder(mut delivered: Vec<Observation>) -> Vec<Observation> {
    delivered.swap(0, 1);
    delivered
}

/// Leaves out the first event.
fn leave_out(mut delivered: Vec<Observation>) -> Vec<Observation> {
    delivered.remove(0);
    delivered
}

/// Logs the first event twice.
fn duplicate(mut delivered: Vec<Observation>) -> Vec<Observation> {
    delivered.insert(0, delivered[0].clone());
    delivered
}

/// Delivers nothing to the run log.
fn divert(_: Vec<Observation>) -> Vec<Observation> {
    Vec::new()
}

/// The run log must hold exactly what the tap delivered, so relogging a
/// step's events with one altered, two swapped, one left out, one extra or
/// none at all fails the run there with `Undelivered`. Relogging them
/// unchanged passes.
#[test]
fn tap_events_must_reach_the_log_unchanged() {
    let at = *delivering(&plain_run())
        .iter()
        .find(|step| **step > 0)
        .expect("the tap delivers after setup");
    let relogged = |edit| {
        tap_run(Twist::default(), move |inner| {
            Box::new(Rewrite { inner, at, edit })
        })
    };
    assert_eq!(
        relogged(unchanged).end.outcome,
        Outcome::Idle {
            status: IdleStatus::Quiescent
        }
    );
    for (name, edit) in [
        ("altered", alter as fn(Vec<Observation>) -> Vec<Observation>),
        ("reordered", reorder),
        ("left out", leave_out),
        ("duplicated", duplicate),
        ("diverted", divert),
    ] {
        let report = relogged(edit);
        let (_, detail) = evidence_failure(&report);
        assert_eq!(report.end.steps, at, "{name}");
        assert!(detail.starts_with("Undelivered"), "{name}: {detail}");
    }
}

/// A reader dropped while the run is live fails it with `Dropped`.
#[test]
fn a_tap_dropped_during_the_run_fails_it() {
    let report = tap_run(Twist::default(), |inner| {
        Box::new(DropAfter {
            inner: Some(inner),
            at: 1,
        })
    });
    let (_, detail) = evidence_failure(&report);
    assert_eq!(report.end.steps, 1);
    assert!(detail.starts_with("Dropped"), "{detail}");
}

/// A tap belongs to the run that made it, so adding it to another fails
/// that run with `ForeignRun`.
#[test]
fn a_tap_from_a_dropped_run_fails_a_later_one() {
    let service = service();
    let mut first = Sim::new(tap_config(), PlanInput::Bytes(Vec::new()));
    let (observer, tap) = BlockTap::new(&mut first, &service, "node", Rc::default());
    drop(first);
    let mut second = Sim::new(tap_config(), PlanInput::Bytes(Vec::new()));
    spawn_emitter(&mut second, observer);
    second.add_source(Box::new(tap));
    let report = second.run(&mut zakura_sim::NoChecks, &mut MemorySink::default());
    let (_, detail) = evidence_failure(&report);
    assert!(detail.starts_with("ForeignRun"), "{detail}");
}

/// Block sync dropping its observer while the run is live fails the run
/// with `ReactorGone`, at the step it does.
#[test]
fn a_dropped_observer_fails_reactor_gone() {
    let gone = plain_run()
        .log
        .entries()
        .iter()
        .find(|entry| {
            matches!(&entry.obs, Observation::TaskExited { task, .. } if task.starts_with("block-sync"))
        })
        .expect("the emitting task exits")
        .step;
    let report = tap_run(
        Twist {
            drop_observer: true,
            ..Twist::default()
        },
        |tap| Box::new(tap),
    );
    let (_, detail) = evidence_failure(&report);
    assert_eq!(report.end.steps, gone);
    assert!(detail.starts_with("ReactorGone"), "{detail}");
}

/// An event recorded after the node's block service is gone fails the run
/// with `PortGone`, at setup here.
#[test]
fn an_event_after_the_service_is_gone_fails_port_gone() {
    let report = tap_run(
        Twist {
            drop_service: true,
            ..Twist::default()
        },
        |tap| Box::new(tap),
    );
    let (_, detail) = evidence_failure(&report);
    assert_eq!(report.end.steps, 0);
    assert!(detail.starts_with("PortGone"), "{detail}");
}

/// Runs `scenario` under `input`, and returns the report with the ports.
/// The run must end without a violation.
fn node_run(scenario: &BlockScenario, input: PlanInput) -> (RunReport, Ports) {
    let ports = Storage::prepare(scenario);
    let report = run_prepared::<Storage>(
        scenario,
        ports.clone(),
        input,
        &mut MemorySink::default(),
        None,
    )
    .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    (report, ports)
}

/// Each block storage operation of `report` that submits a body or reads a
/// range, by ID.
fn operations(report: &RunReport) -> (BTreeSet<u64>, BTreeSet<u64>) {
    let mut submits = BTreeSet::new();
    let mut reads = BTreeSet::new();
    for entry in report.log.entries() {
        if let Observation::Storage {
            service,
            fact: StorageFact::BlockSubmitted { id, request, .. },
            ..
        } = &entry.obs
        {
            match request {
                StorageRequest::SubmitBlock { .. } if service == "block" => {
                    submits.insert(*id);
                }
                StorageRequest::ReadRange { .. } if service == "block" => {
                    reads.insert(*id);
                }
                _ => {}
            }
        }
    }
    (submits, reads)
}

/// The tap's events of `report`, by their tags.
fn event_kinds(report: &RunReport) -> BTreeSet<&'static str> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::BlockSync { event, .. } => Some(match event {
                BlockSyncFact::SessionAdmitted { .. } => "session-admitted",
                BlockSyncFact::FrameJudged { .. } => "frame-judged",
                BlockSyncFact::Request { .. } => "request",
                BlockSyncFact::Budget { .. } => "budget",
                BlockSyncFact::HandoffStarted { .. } => "handoff-started",
                BlockSyncFact::HandoffEnded { .. } => "handoff-ended",
                BlockSyncFact::Holder { .. } => "holder",
                BlockSyncFact::ServingLease { .. } => "serving-lease",
                BlockSyncFact::ServingAdmission { .. } => "serving-admission",
                BlockSyncFact::ServingFrame { .. } => "serving-frame",
                BlockSyncFact::Routine { .. } => "routine",
                BlockSyncFact::WorkQueue { .. } => "work-queue",
                BlockSyncFact::Sequencer { .. } => "sequencer",
                BlockSyncFact::SequencerClosed => "sequencer-closed",
            }),
            _ => None,
        })
        .collect()
}

/// The tap's event numbers in `report`, which must run from 1 with no gap.
fn assert_numbered(report: &RunReport) {
    let seqs: Vec<u64> = report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match entry.obs {
            Observation::BlockSync { seq, .. } => Some(seq),
            _ => None,
        })
        .collect();
    assert!(!seqs.is_empty());
    assert_eq!(seqs, (1..=seqs.len() as u64).collect::<Vec<_>>());
}

/// What an honest node's owners hold once it has converged: nothing in
/// flight, no event the fold couldn't apply, and no operation that didn't
/// pair.
fn assert_settled(view: &BlockSyncView) {
    assert!(view.anomalies.is_empty(), "{:?}", view.anomalies);
    assert!(view.pairing.is_empty(), "{:?}", view.pairing);
    assert_eq!(view.reserved, 0, "the byte budget drains");
    assert!(view.handoffs.is_empty(), "{:?}", view.handoffs);
    assert!(view.holders.is_empty(), "{:?}", view.holders);
    assert!(view.leases.is_empty(), "{:?}", view.leases);
}

/// An honest node syncing from a mirror logs its admissions, frames,
/// requests, budget changes, handoffs, holders and owner snapshots, in
/// sequence. Every body submission pairs with the sequencer's submitted
/// holder of its token, and the view ends with nothing held.
#[test]
fn an_honest_sync_pairs_every_submission() {
    for input in inputs(8, 256) {
        let (report, ports) = node_run(&BlockScenario::default(), input);
        assert_numbered(&report);
        let kinds = event_kinds(&report);
        for kind in [
            "session-admitted",
            "frame-judged",
            "request",
            "budget",
            "handoff-started",
            "handoff-ended",
            "holder",
            "routine",
            "work-queue",
            "sequencer",
        ] {
            assert!(kinds.contains(kind), "{kind} missing from {kinds:?}");
        }
        let (submits, reads) = operations(&report);
        assert_eq!(submits.len(), 8, "one submission per block");
        assert!(reads.is_empty());
        let view = ports.view.borrow();
        let paired: BTreeSet<u64> = view.submissions.values().copied().collect();
        assert_eq!(paired, submits, "every submission pairs once");
        assert_eq!(view.step, Some(report.end.steps));
        assert_settled(&view);
    }
}

/// A node holding every body serves the probe's two requests: each range
/// read pairs with the serving lease on its first height, and the leases
/// end released.
#[test]
fn an_honest_serve_pairs_every_read() {
    let scenario = BlockScenario {
        blocks: 4,
        node_bodies: 4,
        mirrors: Vec::new(),
        probe: true,
        control: vec![ControlStep {
            action: ControlAction::ProbeSend {
                frames: vec![
                    ProbeFrame::GetBlocks {
                        start_height: 1,
                        count: 1,
                    },
                    ProbeFrame::GetBlocks {
                        start_height: 2,
                        count: 1,
                    },
                ],
            },
            when: None,
        }],
        ..BlockScenario::default()
    };
    for input in inputs(8, 256) {
        let (report, ports) = node_run(&scenario, input);
        assert_numbered(&report);
        let kinds = event_kinds(&report);
        assert!(kinds.contains("serving-lease"), "{kinds:?}");
        assert!(kinds.contains("serving-admission"), "{kinds:?}");
        assert!(kinds.contains("serving-frame"), "{kinds:?}");
        let (submits, reads) = operations(&report);
        assert!(submits.is_empty());
        assert_eq!(reads.len(), 2, "one read per request");
        let view = ports.view.borrow();
        assert_eq!(
            view.reads.keys().copied().collect::<BTreeSet<_>>(),
            reads,
            "every read pairs once"
        );
        let starts: BTreeSet<u32> = view.reads.values().map(|height| height.0).collect();
        assert_eq!(starts, BTreeSet::from([1, 2]));
        assert_settled(&view);
    }
}

/// Whether `report`'s block storage deferred a body submission behind a
/// query.
fn deferred_a_submission(report: &RunReport) -> bool {
    report.log.entries().iter().any(|entry| {
        matches!(
            &entry.obs,
            Observation::Storage { service, fact: StorageFact::Deferred { queue }, .. }
                if service == "block"
                    && queue
                        .iter()
                        .any(|request| matches!(request, StorageRequest::SubmitBlock { .. }))
        )
    })
}

/// While the driver awaits a held query, it takes no action, so the
/// sequencer's submissions back up in the action channel, and storage later
/// takes them off it behind a query and defers them. Every submission still
/// pairs, each deferral comes after its holder, and the view ends settled.
#[test]
fn backed_up_submissions_pair_after_their_holders() {
    let scenario = BlockScenario {
        blocks: 16,
        policy: Policy {
            holds: vec![Hold {
                select: Select::nth(BlockKind::Query, 2),
                until: Trigger::VirtualTime(1_000),
                then: vec![BlockAllow::Answer],
                deadline_ms: None,
            }],
            ..Policy::converging()
        },
        ..BlockScenario::default()
    };
    let mut deferred = 0;
    for input in inputs(4, 256) {
        let (report, ports) = node_run(&scenario, input);
        assert_numbered(&report);
        deferred += usize::from(deferred_a_submission(&report));
        let (submits, _) = operations(&report);
        assert_eq!(submits.len(), 16, "one submission per block");
        let view = ports.view.borrow();
        let paired: BTreeSet<u64> = view.submissions.values().copied().collect();
        assert_eq!(paired, submits, "every submission pairs once");
        assert_settled(&view);
    }
    assert!(deferred > 0, "some plan defers a submission");
}

/// The tapped run replays: the same input makes the same picks and the same
/// log.
#[test]
fn a_tapped_run_replays() {
    for input in inputs(4, 256) {
        let (first, _) = node_run(&BlockScenario::default(), input.clone());
        let (second, _) = node_run(&BlockScenario::default(), input);
        assert_eq!(support::picks(&first), support::picks(&second));
        assert_eq!(first.log.entries(), second.log.entries());
    }
}
