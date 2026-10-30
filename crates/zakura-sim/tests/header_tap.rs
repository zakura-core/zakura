//! A header-sync reactor's tap as run evidence (M4-B3a): the run checks
//! the tap's checkpoint like storage's, so its events can't be skipped,
//! diverted, rewritten, dropped or left undelivered, and the tap changes
//! nothing about the run it observes.
#![cfg(zakura_sim)]

mod support;

use std::{cell::RefCell, collections::BTreeSet, rc::Rc, sync::Arc};

use support::{config, inputs, run};
use zakura_network::zakura::observe::{HeaderSyncObserver, ObserverSlot, ReactorEvent};
use zakura_node_services::header_chain::Port;
use zakura_scheduler::{SchedulerConfig, SimInstant};
use zakura_sim::{
    harness::{
        header_node::{
            preload, tap::ReactorTap, Chain, DispatchFlag, Effects, HeaderNode, Policy,
            PolicySource, Topology,
        },
        header_sync::storage_config,
    },
    observe::ReactorFact,
    storage::HeaderStorage,
    ActionSource, Boundary, HarnessFailure, Identity, IdleStatus, MemorySink, Observation,
    ObservationLog, Outcome, PlanInput, RunConfig, RunReport, Sim, Violation,
};
use zakura_storage::header::{
    adapter::EnginePort, controlled::ControlledPort, fixture::HeaderFixture,
};

/// A small run's config: 2 workers, the default stack, and 100 steps.
fn tap_config() -> RunConfig {
    let mut config = config(2, 100);
    config.scheduler.stack_size = SchedulerConfig::default().stack_size;
    config
}

/// A `Handled` event for `call`, the simplest event to emit by hand.
fn handled(call: u64) -> ReactorEvent {
    ReactorEvent::Handled {
        call,
        panicked: false,
    }
}

/// What a [`tap_run`] does beyond emitting its events.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct Twist {
    /// Drop the observer once the emitting task is done, instead of keeping
    /// it for the whole run.
    drop_observer: bool,
    /// Drop the port's last strong handle at setup, so the tap finds it
    /// gone.
    drop_port: bool,
    /// Have the observer drop this call's `Handled`, though its slot counts
    /// it, as a buggy observer would.
    lose_call: Option<u64>,
    /// Have the observer record the first call's `Handled` twice and drop
    /// the second call's, so its total matches the slot's.
    repeat_then_lose: Option<(u64, u64)>,
}

/// A run in which a task reads membership through a controlled port, as
/// storage evidence, and a task named `reactor` emits two events at each of
/// three steps through a tap on that port. One more event is emitted at
/// setup. The tap's reader is added as `wrap` makes it, and the observer
/// lives until the run ends unless `twist` drops it.
fn tap_run(twist: Twist, wrap: impl FnOnce(ReactorTap) -> Box<dyn ActionSource>) -> RunReport {
    let mut kept = None;
    let report = run(
        tap_config(),
        PlanInput::Bytes(Vec::new()),
        |sim| {
            let port = Arc::new(ControlledPort::new(&HeaderFixture::regtest()));
            let (observer, tap) = ReactorTap::new(sim, &port, "node");
            if !twist.drop_port {
                let caller = port.clone();
                sim.scheduler().spawn_root("caller", async move {
                    let anchor = HeaderFixture::regtest().anchor();
                    let _ = caller.selected_chain_contains(anchor.hash).await;
                });
                let storage = HeaderStorage::new(sim, port);
                sim.add_source(Box::new(storage));
            } else {
                drop(port);
            }
            let observer = match (twist.lose_call, twist.repeat_then_lose) {
                (Some(call), _) => observer.wrapped(|inner| Arc::new(Loses { inner, call })),
                (None, Some((repeat, lose))) => observer.wrapped(|inner| {
                    Arc::new(Substitutes {
                        inner,
                        repeat,
                        lose,
                    })
                }),
                (None, None) => observer,
            };
            observer.emit(handled(1));
            if !twist.drop_observer {
                kept = Some(observer.clone());
            }
            spawn_emitter(sim, observer);
            sim.add_source(wrap(tap));
        },
        &mut zakura_sim::NoChecks,
    );
    drop(kept);
    report
}

/// Spawns `reactor`, which emits two events at each of three steps and then
/// exits, dropping its `observer`.
fn spawn_emitter(sim: &mut Sim, observer: ObserverSlot) {
    sim.scheduler().spawn_root("reactor", async move {
        for call in 2..=4 {
            observer.emit(handled(call));
            observer.emit(handled(call + 100));
            zakura_rt::task::yield_now().await;
        }
    });
}

/// [`tap_run`] with the reader added plainly.
fn plain_run() -> RunReport {
    tap_run(Twist::default(), |tap| Box::new(tap))
}

/// The steps at which `report`'s log holds reactor events.
fn delivering(report: &RunReport) -> BTreeSet<u64> {
    report
        .log
        .entries()
        .iter()
        .filter(|entry| matches!(entry.obs, Observation::Reactor { .. }))
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

/// A reader that forwards everything to `inner` but skips the observation
/// at step `skip`, as a buggy wrapper might.
struct SkipAt<S> {
    inner: S,
    skip: u64,
}

impl<S: ActionSource> ActionSource for SkipAt<S> {
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        self.inner.enabled(now, out);
    }

    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        self.inner.boundaries(now, out);
    }

    fn execute(&mut self, local: u64, log: &mut ObservationLog) {
        self.inner.execute(local, log);
    }

    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        if log.step() == self.skip {
            return Ok(());
        }
        self.inner.observe(log)
    }
}

/// A reader that forwards everything to `inner`, but at step `at` observes
/// into a copy of the log, or observes and then logs the last entry again.
struct Divert<S> {
    inner: S,
    at: u64,
    duplicate: bool,
}

impl<S: ActionSource> ActionSource for Divert<S> {
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        self.inner.enabled(now, out);
    }

    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        self.inner.boundaries(now, out);
    }

    fn execute(&mut self, local: u64, log: &mut ObservationLog) {
        self.inner.execute(local, log);
    }

    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        if log.step() != self.at {
            return self.inner.observe(log);
        }
        if !self.duplicate {
            return self.inner.observe(&mut log.clone());
        }
        let cursor = log.entries().len();
        self.inner.observe(log)?;
        if let Some(last) = log.entries()[cursor..].last() {
            log.push(last.obs.clone());
        }
        Ok(())
    }
}

/// A reader that forwards everything to `inner`, but at step `at` observes
/// into a copy of the log and logs what it delivered there after `edit`
/// changes it.
struct Rewrite<S> {
    inner: S,
    at: u64,
    edit: fn(Vec<Observation>) -> Vec<Observation>,
}

impl<S: ActionSource> ActionSource for Rewrite<S> {
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        self.inner.enabled(now, out);
    }

    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        self.inner.boundaries(now, out);
    }

    fn execute(&mut self, local: u64, log: &mut ObservationLog) {
        self.inner.execute(local, log);
    }

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

/// A reader that forwards everything to `inner`, then drops it right after
/// its observation at step `at`.
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

/// Never draining the tap fails setup with `NotObserved`, since it
/// registered its checkpoint when it was made.
#[test]
fn a_tap_the_run_never_observes_fails_at_setup() {
    let mut kept = None;
    let report = tap_run(Twist::default(), |tap| {
        kept = Some(tap);
        Box::new(SharedSource(Rc::default()))
    });
    drop(kept);
    let (source, detail) = evidence_failure(&report);
    assert_eq!(report.end.steps, 0);
    assert_eq!(source, "reactor:node#0");
    assert!(detail.starts_with("NotObserved"), "{detail}");
}

/// A reader that skips its observation at setup, at an interior step or at
/// the last step ends the run there.
#[test]
fn a_skipped_tap_observation_ends_the_run_there() {
    let whole = plain_run();
    assert_eq!(
        whole.end.outcome,
        Outcome::Idle {
            status: IdleStatus::Quiescent
        },
        "{:?}",
        whole.end
    );
    let last = whole.end.steps;
    assert!(last > 2, "the run has an interior step");
    for (skip, failure) in [
        (0, "NotObserved"),
        (1, "BoundarySkipped"),
        (last, "BoundarySkipped"),
    ] {
        let report = tap_run(Twist::default(), |inner| Box::new(SkipAt { inner, skip }));
        let (_, detail) = evidence_failure(&report);
        assert_eq!(report.end.steps, skip);
        assert!(detail.starts_with(failure), "skipping {skip}: {detail}");
    }
}

/// Events delivered into a copy of the log, at setup, at an interior step
/// or at the last delivering step, or logged twice, end the run there.
#[test]
fn tap_events_delivered_anywhere_but_the_run_log_end_the_run() {
    let whole = plain_run();
    let steps = delivering(&whole);
    assert!(steps.contains(&0), "setup delivers an event");
    let last = *steps.last().expect("the tap delivers");
    let interior = *steps
        .iter()
        .find(|step| **step != 0 && **step != last)
        .expect("the tap delivers at an interior step");
    for (at, duplicate) in [
        (0, false),
        (interior, false),
        (last, false),
        (interior, true),
    ] {
        let report = tap_run(Twist::default(), |inner| {
            Box::new(Divert {
                inner,
                at,
                duplicate,
            })
        });
        let (_, detail) = evidence_failure(&report);
        assert_eq!(report.end.steps, at);
        assert!(detail.starts_with("Undelivered"), "at {at}: {detail}");
    }
}

/// Leaves the delivery as it was.
fn unchanged(delivered: Vec<Observation>) -> Vec<Observation> {
    delivered
}

/// Changes the first event's payload, keeping its numbers.
fn alter(mut delivered: Vec<Observation>) -> Vec<Observation> {
    if let Some(event) = delivered.iter_mut().find_map(|obs| match obs {
        Observation::Reactor { event, .. } => Some(event),
        _ => None,
    }) {
        *event = ReactorFact::Handled {
            call: 999,
            panicked: true,
        };
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

/// The run log must hold exactly what the tap delivered, so a wrapper that
/// relogs a step's events with one altered, two swapped, or one left out
/// fails the run with `Undelivered`. Relogging them unchanged passes.
#[test]
fn tap_events_must_reach_the_log_unchanged() {
    let whole = plain_run();
    let at = *delivering(&whole)
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
    ] {
        let report = relogged(edit);
        let (_, detail) = evidence_failure(&report);
        assert_eq!(report.end.steps, at, "{name}");
        assert!(detail.starts_with("Undelivered"), "{name}: {detail}");
    }
}

/// A tap belongs to the run that made it. One made with a first run that's
/// then dropped, and added to a second run, whether through a wrapper that
/// skips its final observation or plainly, fails the second run.
#[test]
fn a_tap_from_a_dropped_run_fails_a_later_one() {
    for wrapped in [true, false] {
        let port = Arc::new(ControlledPort::new(&HeaderFixture::regtest()));
        let mut first = Sim::new(tap_config(), PlanInput::Bytes(Vec::new()));
        let (observer, tap) = ReactorTap::new(&mut first, &port, "node");
        drop(first);
        let mut second = Sim::new(tap_config(), PlanInput::Bytes(Vec::new()));
        spawn_emitter(&mut second, observer);
        if wrapped {
            second.add_source(Box::new(SkipAt {
                inner: tap,
                skip: 3,
            }));
        } else {
            second.add_source(Box::new(tap));
        }
        let report = second.run(&mut zakura_sim::NoChecks, &mut MemorySink::default());
        let (_, detail) = evidence_failure(&report);
        assert!(
            detail.starts_with("ForeignRun"),
            "wrapped {wrapped}: {detail}"
        );
    }
}

/// A run with taps on two nodes' ports, each emitting one event at setup,
/// with tap `divert`, if any, observing into a copy of the log at setup.
fn two_tap_run(divert: Option<u64>) -> RunReport {
    let mut kept = Vec::new();
    let report = run(
        tap_config(),
        PlanInput::Bytes(Vec::new()),
        |sim| {
            for (instance, node) in [(0, "node"), (1, "mirror-0")] {
                let port = Arc::new(ControlledPort::new(&HeaderFixture::regtest()));
                let (observer, tap) = ReactorTap::new(sim, &port, node);
                observer.emit(handled(1));
                kept.push((port, observer));
                if divert == Some(instance) {
                    sim.add_source(Box::new(Divert {
                        inner: tap,
                        at: 0,
                        duplicate: false,
                    }));
                } else {
                    sim.add_source(Box::new(tap));
                }
            }
            sim.scheduler().spawn_root("idle", async {
                zakura_rt::task::yield_now().await;
            });
        },
        &mut zakura_sim::NoChecks,
    );
    drop(kept);
    report
}

/// Taps are told apart by their registration index: two that both deliver
/// pass, and either one delivering into a copy of the log fails, though the
/// other's events are in the run log.
#[test]
fn each_tap_is_checked_on_its_own() {
    let both = two_tap_run(None);
    assert_eq!(
        both.end.outcome,
        Outcome::Idle {
            status: IdleStatus::Quiescent
        },
        "{:?}",
        both.end
    );
    let instances: BTreeSet<u64> = both
        .log
        .entries()
        .iter()
        .filter_map(|entry| match entry.obs {
            Observation::Reactor { instance, .. } => Some(instance),
            _ => None,
        })
        .collect();
    assert_eq!(instances, BTreeSet::from([0, 1]));
    for (divert, node) in [(0, "node"), (1, "mirror-0")] {
        let report = two_tap_run(Some(divert));
        let (source, detail) = evidence_failure(&report);
        assert_eq!(report.end.steps, 0);
        assert_eq!(source, format!("reactor:{node}#{divert}"));
        assert!(detail.starts_with("Undelivered"), "{detail}");
    }
}

/// A reader dropped while the run is live, right after its last observation
/// or at an interior step, fails the run, since later events go
/// undelivered.
#[test]
fn a_tap_dropped_during_the_run_fails_it() {
    let last = plain_run().end.steps;
    for at in [last, 1] {
        let report = tap_run(Twist::default(), |inner| {
            Box::new(DropAfter {
                inner: Some(inner),
                at,
            })
        });
        let (_, detail) = evidence_failure(&report);
        assert_eq!(report.end.steps, at);
        assert!(detail.starts_with("Dropped"), "dropping at {at}: {detail}");
    }
}

/// An observer that forwards every event to `inner` except `call`'s
/// `Handled`.
#[derive(Debug)]
struct Loses {
    inner: Arc<dyn HeaderSyncObserver>,
    call: u64,
}

impl HeaderSyncObserver for Loses {
    /// Forwards `event` unless it's the lost call's `Handled`.
    fn observe(&self, event: ReactorEvent) {
        if event != handled(self.call) {
            self.inner.observe(event);
        }
    }
}

/// An event the slot counted but the observer dropped fails the run with
/// `Unobserved` at the step that emitted it: the first, a middle one, and
/// the run's last. The emitter's events are counted and recorded alike
/// otherwise, so a faithful observer passes.
///
/// This fails if the tap checks only what its observer recorded.
#[test]
fn an_event_the_observer_drops_fails_unobserved() {
    let whole = plain_run();
    assert_eq!(whole.end.harness, None, "{:?}", whole.end);
    let step_of = |call: u64| {
        whole
            .log
            .entries()
            .iter()
            .find(|entry| {
                matches!(&entry.obs, Observation::Reactor {
                    event: ReactorFact::Handled { call: c, .. }, ..
                } if *c == call)
            })
            .expect("the plain run logs the call")
            .step
    };
    for call in [1, 3, 104] {
        let report = tap_run(
            Twist {
                lose_call: Some(call),
                ..Twist::default()
            },
            |tap| Box::new(tap),
        );
        let (source, detail) = evidence_failure(&report);
        assert_eq!(source, "reactor:node#0");
        assert!(detail.starts_with("Unobserved"), "losing {call}: {detail}");
        assert_eq!(report.end.steps, step_of(call), "losing {call}");
    }
}

/// An observer that forwards `repeat`'s `Handled` twice, drops `lose`'s, and
/// forwards everything else once.
#[derive(Debug)]
struct Substitutes {
    inner: Arc<dyn HeaderSyncObserver>,
    repeat: u64,
    lose: u64,
}

impl HeaderSyncObserver for Substitutes {
    /// Forwards `event` twice, not at all, or once.
    fn observe(&self, event: ReactorEvent) {
        if event == handled(self.repeat) {
            self.inner.observe(event.clone());
            self.inner.observe(event);
        } else if event != handled(self.lose) {
            self.inner.observe(event);
        }
    }
}

/// An observer that records one event twice and drops a later one keeps
/// its total level with the slot's, yet the run fails `Unobserved` at the
/// step of the repeat, since that event arrived out of step with the
/// slot's count.
///
/// This fails if the tap compares only its total with the slot's.
#[test]
fn a_repeated_event_never_hides_a_lost_one() {
    let whole = plain_run();
    let step_of = |call: u64| {
        whole
            .log
            .entries()
            .iter()
            .find(|entry| {
                matches!(&entry.obs, Observation::Reactor {
                    event: ReactorFact::Handled { call: c, .. }, ..
                } if *c == call)
            })
            .expect("the plain run logs the call")
            .step
    };
    assert!(step_of(2) < step_of(3), "the lost event comes a step later");
    let report = tap_run(
        Twist {
            repeat_then_lose: Some((2, 3)),
            ..Twist::default()
        },
        |tap| Box::new(tap),
    );
    let repeats = report
        .log
        .entries()
        .iter()
        .filter(|entry| {
            matches!(
                &entry.obs,
                Observation::Reactor {
                    event: ReactorFact::Handled { call: 2, .. },
                    ..
                }
            )
        })
        .count();
    assert_eq!(repeats, 2, "the observer recorded the event twice");
    let (source, detail) = evidence_failure(&report);
    assert_eq!(source, "reactor:node#0");
    assert!(detail.starts_with("Unobserved"), "{detail}");
    assert_eq!(report.end.steps, step_of(2));
}

/// A reader shared with a checker.
type Shared = Rc<RefCell<Option<ReactorTap>>>;

/// A source that forwards to a shared reader while it's there.
struct SharedSource(Shared);

impl ActionSource for SharedSource {
    fn enabled(&self, _: SimInstant, _: &mut Vec<(Identity, u64)>) {}

    fn boundaries(&self, _: SimInstant, _: &mut Vec<Boundary>) {}

    fn execute(&mut self, _: u64, _: &mut ObservationLog) {
        unreachable!("a tap offers no actions");
    }

    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        match &mut *self.0.borrow_mut() {
            Some(inner) => inner.observe(log),
            None => Ok(()),
        }
    }
}

/// What [`AtEnd`] does in `at_end`.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Late {
    /// Nothing.
    Nothing,
    /// Drops the shared reader.
    DropReader,
    /// Emits an event, inside the run.
    Emit,
    /// Emits an event from a thread outside the run.
    EmitOutside,
}

/// A checker that acts on the tap in `at_end`, after the last observation.
struct AtEnd {
    shared: Shared,
    observer: ObserverSlot,
    late: Late,
}

impl zakura_sim::Checker for AtEnd {
    fn after_step(&mut self, _: &zakura_sim::StepCtx<'_>) -> Option<Violation> {
        None
    }

    fn at_end(
        &mut self,
        _: &zakura_sim::RunEnd,
        _: &ObservationLog,
    ) -> Vec<zakura_sim::PropertyReport> {
        match self.late {
            Late::Nothing => {}
            Late::DropReader => drop(self.shared.borrow_mut().take()),
            Late::Emit => self.observer.emit(handled(50)),
            Late::EmitOutside => emit_outside_the_run(&self.observer),
        }
        Vec::new()
    }
}

/// Emits an event from a thread with no run, which the tap refuses.
fn emit_outside_the_run(observer: &ObserverSlot) {
    std::thread::scope(|scope| {
        scope
            .spawn(|| observer.emit(handled(60)))
            .join()
            .expect("a refused emission doesn't panic");
    });
}

/// [`tap_run`]'s setup, with the reader shared with a checker that acts as
/// `late` says in `at_end`.
fn late_run(late: Late) -> RunReport {
    let mut sim = Sim::new(tap_config(), PlanInput::Bytes(Vec::new()));
    let port = Arc::new(ControlledPort::new(&HeaderFixture::regtest()));
    let (observer, tap) = ReactorTap::new(&mut sim, &port, "node");
    observer.emit(handled(1));
    spawn_emitter(&mut sim, observer.clone());
    let shared: Shared = Rc::new(RefCell::new(Some(tap)));
    sim.add_source(Box::new(SharedSource(shared.clone())));
    let mut checker = AtEnd {
        shared,
        observer,
        late,
    };
    sim.run(&mut checker, &mut MemorySink::default())
}

/// Every evidence check reads the tap live, so after the last observation a
/// dropped reader fails with `Dropped`, an event recorded in the run with
/// `Unobserved`, and one refused outside the run with
/// `RefusedOutsideRun`, all at the last step. A quiet `at_end` passes.
#[test]
fn the_tap_is_checked_live_after_the_last_observation() {
    let quiet = late_run(Late::Nothing);
    assert_eq!(
        quiet.end.outcome,
        Outcome::Idle {
            status: IdleStatus::Quiescent
        },
        "{:?}",
        quiet.end
    );
    let last = quiet.end.steps;
    for (late, failure) in [
        (Late::DropReader, "Dropped"),
        (Late::Emit, "Unobserved"),
        (Late::EmitOutside, "RefusedOutsideRun"),
    ] {
        let report = late_run(late);
        let (_, detail) = evidence_failure(&report);
        assert_eq!(report.end.steps, last, "{late:?}");
        assert!(detail.starts_with(failure), "{late:?}: {detail}");
    }
}

/// A reader that, at step `at`, emits an event from outside the run just
/// before observing.
struct RefuseBefore<S> {
    inner: S,
    observer: ObserverSlot,
    at: u64,
}

impl<S: ActionSource> ActionSource for RefuseBefore<S> {
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        self.inner.enabled(now, out);
    }

    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        self.inner.boundaries(now, out);
    }

    fn execute(&mut self, local: u64, log: &mut ObservationLog) {
        self.inner.execute(local, log);
    }

    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        if log.step() == self.at {
            emit_outside_the_run(&self.observer);
        }
        self.inner.observe(log)
    }
}

/// An emission from outside the run is refused, never logged, and fails
/// the run at the next evidence check.
#[test]
fn an_emission_outside_the_run_fails_it() {
    let mut sim = Sim::new(tap_config(), PlanInput::Bytes(Vec::new()));
    let port = Arc::new(ControlledPort::new(&HeaderFixture::regtest()));
    let (observer, tap) = ReactorTap::new(&mut sim, &port, "node");
    spawn_emitter(&mut sim, observer.clone());
    sim.add_source(Box::new(RefuseBefore {
        inner: tap,
        observer: observer.clone(),
        at: 1,
    }));
    let report = sim.run(&mut zakura_sim::NoChecks, &mut MemorySink::default());
    let (_, detail) = evidence_failure(&report);
    assert_eq!(report.end.steps, 1);
    assert!(detail.starts_with("RefusedOutsideRun"), "{detail}");
    let logged: Vec<u64> = report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Reactor {
                event: ReactorFact::Handled { call, .. },
                ..
            } => Some(*call),
            _ => None,
        })
        .collect();
    assert!(
        !logged.contains(&60),
        "the refused event isn't logged: {logged:?}"
    );
}

/// A reactor that drops its observer while the run is live fails it with
/// `ReactorGone`, at the step it does.
#[test]
fn a_dropped_observer_fails_reactor_gone() {
    let whole = plain_run();
    let gone = whole
        .log
        .entries()
        .iter()
        .find(|entry| {
            matches!(&entry.obs, Observation::TaskExited { task, .. } if task.starts_with("reactor"))
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

/// An event recorded after the node's port is gone fails the run with
/// `PortGone`, at setup here.
#[test]
fn an_event_after_the_port_is_gone_fails_port_gone() {
    let report = tap_run(
        Twist {
            drop_port: true,
            ..Twist::default()
        },
        |tap| Box::new(tap),
    );
    let (_, detail) = evidence_failure(&report);
    assert_eq!(report.end.steps, 0);
    assert!(detail.starts_with("PortGone"), "{detail}");
}

/// The node at genesis syncing from `mirror-0` at A5 under the converging
/// policy, with the node's reactor tapped if `tapped`.
fn node_run(input: PlanInput, tapped: bool) -> RunReport {
    node_run_losing(input, tapped, None)
}

/// [`node_run`], with the tapped observer losing the `lose`-th event its
/// slot counts, if given.
fn node_run_losing(input: PlanInput, tapped: bool, lose: Option<u64>) -> RunReport {
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
            let (observer, storage) = if tapped {
                let (observer, tap) = ReactorTap::new(sim, &node, "node");
                let observer = match lose {
                    Some(nth) => observer.wrapped(|inner| {
                        Arc::new(LosesNth {
                            inner,
                            nth,
                            seen: std::sync::atomic::AtomicU64::new(0),
                        })
                    }),
                    None => observer,
                };
                (Some(observer), storage.with_tap(tap))
            } else {
                (None, storage)
            };
            sim.add_source(Box::new(storage));
            let nodes = vec![
                HeaderNode::spawn(
                    sim,
                    "node",
                    1,
                    node.clone(),
                    &fixture,
                    Chain::default().tip(&fixture),
                    dispatch.clone(),
                    observer,
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

/// An observer that forwards every event to `inner` but the `nth`, from 1.
#[derive(Debug)]
struct LosesNth {
    inner: Arc<dyn HeaderSyncObserver>,
    nth: u64,
    seen: std::sync::atomic::AtomicU64,
}

impl HeaderSyncObserver for LosesNth {
    /// Forwards `event` unless it's the `nth`.
    fn observe(&self, event: ReactorEvent) {
        let seen = self.seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        if seen != self.nth {
            self.inner.observe(event);
        }
    }
}

/// A real reactor whose observer drops one event fails the run with
/// `Unobserved` at the step that emitted it: its first `Settled`, at setup,
/// a `Resumed` mid-run, its last `CallEnded`, whose storage receipt the
/// tap then leaves to its own failure, and the last event before the run
/// ends. The faithful run passes, and dropping changes no pick.
///
/// This fails if the tap checks only what its observer recorded.
#[test]
fn suppression_is_caught_at_the_first_middle_and_last_event() {
    let input = PlanInput::Bytes(Vec::new());
    let whole = node_run(input.clone(), true);
    assert_eq!(whole.end.harness, None, "{:?}", whole.end);
    let events: Vec<(u64, u64, &ReactorFact)> = whole
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Reactor { seq, event, .. } => Some((*seq, entry.step, event)),
            _ => None,
        })
        .collect();
    let first = events[0];
    assert!(
        matches!(first.2, ReactorFact::Settled { turn: 0, .. }) && first.1 == 0,
        "the first event is the build's Settled, at setup: {first:?}"
    );
    let resumes: Vec<_> = events
        .iter()
        .filter(|(_, _, event)| matches!(event, ReactorFact::Resumed { .. }))
        .collect();
    let middle = *resumes[resumes.len() / 2];
    let call_end = *events
        .iter()
        .rev()
        .find(|(_, _, event)| matches!(event, ReactorFact::CallEnded { .. }))
        .expect("the run ends a call");
    let last = *events.last().expect("the run has events");
    assert!(
        middle.1 > 0 && middle.1 < last.1,
        "the middle Resumed is mid-run: {middle:?}"
    );
    for (seq, step, event) in [first, middle, call_end, last] {
        let lost = node_run_losing(input.clone(), true, Some(seq));
        let (source, detail) = evidence_failure(&lost);
        assert_eq!(source, "reactor:node#0", "losing {event:?}: {detail}");
        assert!(
            detail.starts_with("Unobserved"),
            "losing {event:?}: {detail}"
        );
        assert_eq!(lost.end.steps, step, "losing {event:?}");
        assert_eq!(
            support::picks(&lost),
            support::picks(&whole)[..support::picks(&lost).len()],
            "losing {event:?} changes no pick"
        );
    }
}

/// The storage entries of `report`'s log, in order, with their steps.
fn storage_entries(report: &RunReport) -> Vec<(u64, &Observation)> {
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
        .map(|entry| (entry.step, &entry.obs))
        .collect()
}

/// The tap's events of `report`'s log, by their tags.
fn event_kinds(report: &RunReport) -> BTreeSet<&'static str> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Reactor { event, .. } => Some(match event {
                ReactorFact::Dispatched { .. } => "dispatched",
                ReactorFact::CallEnded { .. } => "call-ended",
                ReactorFact::Handled { .. } => "handled",
                ReactorFact::Started { .. } => "started",
                ReactorFact::Retired { .. } => "retired",
                ReactorFact::OfferWaiting { .. } => "offer-waiting",
                ReactorFact::Misbehavior { .. } => "misbehavior",
                ReactorFact::Settled { .. } => "settled",
                ReactorFact::Resumed { .. } => "resumed",
                ReactorFact::Dropping { .. } => "dropping",
                ReactorFact::Placed { .. } => "placed",
                ReactorFact::Bound { .. } => "bound",
                ReactorFact::LeasesCaptured { .. } => "leases-captured",
            }),
            _ => None,
        })
        .collect()
}

/// Tapping the node's reactor adds no scheduling point: under every plan, the
/// run with the tap makes the same picks and logs the same storage entries
/// as the run without it, and the node's dispatches, call ends, completions,
/// starts, retirements, settled markers, placements, bindings and captured
/// leases all reach the log, in sequence.
#[test]
fn tap_adds_no_scheduling_point() {
    for input in inputs(16, 256) {
        let bare = node_run(input.clone(), false);
        let tapped = node_run(input, true);
        assert_eq!(tapped.end, bare.end);
        assert_eq!(support::picks(&tapped), support::picks(&bare));
        assert_eq!(storage_entries(&tapped), storage_entries(&bare));
        assert!(event_kinds(&bare).is_empty());
        assert_eq!(
            event_kinds(&tapped),
            BTreeSet::from([
                "dispatched",
                "call-ended",
                "handled",
                "started",
                "retired",
                "settled",
                "resumed",
                "placed",
                "bound",
                "leases-captured"
            ])
        );
        let seqs: Vec<u64> = tapped
            .log
            .entries()
            .iter()
            .filter_map(|entry| match entry.obs {
                Observation::Reactor { seq, .. } => Some(seq),
                _ => None,
            })
            .collect();
        assert_eq!(seqs, (1..=seqs.len() as u64).collect::<Vec<_>>());
    }
}

/// Each task stop in `report`'s log, with its step, as the log records it:
/// where the task reached a point, with the point's object and site, or
/// how it waited, parked or exited.
fn stops(report: &RunReport) -> Vec<(u64, String)> {
    report
        .log
        .entries()
        .iter()
        .filter(|entry| entry.obs.ran_task().is_some())
        .map(|entry| (entry.step, format!("{:?}", entry.obs)))
        .collect()
}

/// The work identities add no scheduling point (M6a-4): under every plan,
/// the tapped run makes the same picks and stops every task at the same
/// point, object and site as the bare run, and the tapped run reaches each
/// capture path: a placement for staged work, its binding and start, a
/// call's captured leases, and a settled snapshot holding a reservation and
/// a lease.
///
/// This fails if a capture path reads anything through the simulator's
/// primitives, such as a reservation's rest.
#[test]
fn work_identities_add_no_scheduling_point() {
    for input in inputs(8, 256) {
        let bare = node_run(input.clone(), false);
        let tapped = node_run(input, true);
        let facts: Vec<&ReactorFact> = tapped
            .log
            .entries()
            .iter()
            .filter_map(|entry| match &entry.obs {
                Observation::Reactor { event, .. } => Some(event),
                _ => None,
            })
            .collect();
        assert!(
            facts.iter().any(|fact| matches!(
                fact,
                ReactorFact::Placed {
                    placement: Some(_),
                    ..
                }
            )) && facts
                .iter()
                .any(|fact| matches!(fact, ReactorFact::Bound { .. }))
                && facts.iter().any(
                    |fact| matches!(fact, ReactorFact::Started { request, .. } if request.incarnation > 0)
                )
                && facts.iter().any(
                    |fact| matches!(fact, ReactorFact::LeasesCaptured { leases, .. } if !leases.is_empty())
                ),
            "the run places, binds, starts and captures leases"
        );
        let holdings = facts.iter().filter_map(|fact| match fact {
            ReactorFact::Settled {
                snapshot: Some(snapshot),
                ..
            } => Some(&snapshot.holdings),
            _ => None,
        });
        let (mut reserved, mut leased) = (false, false);
        for holding in holdings.flatten() {
            reserved |= holding.reservation.is_some();
            leased |= !holding.leases.is_empty();
        }
        assert!(
            reserved && leased,
            "a settled snapshot holds a reservation and a lease"
        );
        assert_eq!(tapped.end, bare.end);
        assert_eq!(support::picks(&tapped), support::picks(&bare));
        assert_eq!(stops(&tapped), stops(&bare));
    }
}
