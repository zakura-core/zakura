//! hdr-13: `header-sync/storage` declares `BoundedAdversary` from a
//! recovery's `recovered` pick, so a node that keeps the clock from moving
//! with `header-sync/bounded-adversary-progress` owed violates
//! `sim/no-livelock` rather than running out its step budget (Rule 13 of
//! the hdr-9 to hdr-13 plan). Honest recoveries stay well under the limit.
#![cfg(zakura_sim)]

mod support;

use std::time::Duration;

use support::{inputs, storage_witnesses};
use zakura_sim::{
    domain::header_sync_progress::BOUNDED_ADVERSARY_PROGRESS,
    harness::{
        header_node::recovery::{Disruption, RecoveryEvidence},
        header_sync::{
            recovered_bound, recovery_assumptions, recovery_source, storage_properties,
            storage_sources, Ports, Storage, StorageScenario, StorageView, RECOVERY_BUDGET,
        },
        run_prepared, Subsystem,
    },
    observe::Exit,
    property::{Assume, Assumptions, EndViolation, HasView, Property, Sources},
    sim::NO_LIVELOCK,
    MemorySink, Observation, ObservationLog, Outcome, RunConfig, RunEnd, Sim,
};

/// Every storage recovery witness hdr-13 runs: fuzz-12's five classes,
/// hdr-13's owed reply hold and departure, the declared stall, and the
/// stall under slow storage.
fn witnesses() -> Vec<(String, StorageScenario)> {
    let mut all: Vec<(String, StorageScenario)> = storage_witnesses::recovery_witnesses()
        .into_iter()
        .chain(storage_witnesses::owed_recovery_witnesses())
        .map(|(class, scenario)| (class.to_owned(), scenario))
        .collect();
    all.push(("stall".to_owned(), storage_witnesses::grown_source()));
    let mut slow = storage_witnesses::grown_source();
    slow.policy.disruptions = vec![Disruption::SlowStorage];
    all.push(("slow stall".to_owned(), slow));
    all
}

/// The storage harness's declaration for `scenario`, for a harness that
/// wraps [`Storage`]: `BoundedAdversary` from the `recovered` pick and the
/// five recovery assumptions, holding as [`Storage::recovery_holds`] says.
fn recovering_assumptions<S: Wraps>(scenario: &StorageScenario) -> Assumptions<S> {
    let source = recovery_source(scenario).expect("every scenario here recovers");
    let mut declared = vec![recovered_bound()];
    declared.extend(recovery_assumptions(&source));
    Assumptions::declare(declared)
        .holds_when(|wrapper: &S, assume| wrapper.storage().recovery_holds(assume))
}

/// A harness that wraps [`Storage`].
trait Wraps {
    /// The storage harness inside.
    fn storage(&self) -> &Storage;
}

/// `header-sync/storage` with a lower livelock limit, the scenario's, so a
/// test can hold honest recoveries under half of the real one.
struct Limited(Storage);

impl Wraps for Limited {
    fn storage(&self) -> &Storage {
        &self.0
    }
}

impl HasView for Limited {
    type View = StorageView;

    /// The storage harness's view.
    fn view(&self) -> StorageView {
        self.0.view()
    }

    /// The storage harness's end.
    fn ended(&self, end: &RunEnd) {
        self.0.ended(end);
    }

    /// The storage harness's end violations.
    fn end_violations(&self, log: &ObservationLog) -> Vec<EndViolation> {
        self.0.end_violations(log)
    }

    /// The storage harness's stamps.
    fn stamps_after(&self, after: Option<u64>) -> Option<Vec<u64>> {
        self.0.stamps_after(after)
    }
}

impl Subsystem for Limited {
    const NAME: &'static str = "test/recovery-limited";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    /// The scenario, and the steps at one instant past which the run
    /// violates `sim/no-livelock` while `BoundedAdversary` holds.
    type Scenario = (StorageScenario, u64);
    type Prepared = Ports;

    /// [`Storage`]'s, with the scenario's livelock limit.
    fn config((scenario, limit): &(StorageScenario, u64)) -> RunConfig {
        RunConfig {
            enforce_no_livelock: true,
            max_steps_per_instant: *limit,
            ..Storage::config(scenario)
        }
    }

    /// [`Storage`]'s.
    fn prepare((scenario, _): &(StorageScenario, u64)) -> Ports {
        Storage::prepare(scenario)
    }

    /// The storage harness, finish included.
    fn build(sim: &mut Sim, (scenario, _): &(StorageScenario, u64), ports: Ports) -> Self {
        Limited(Storage::build(sim, scenario, ports))
    }

    /// [`Storage`]'s.
    fn properties() -> Vec<Property<Self>> {
        let mut properties = storage_properties();
        properties.push(zakura_sim::domain::header_sync_progress::bounded_adversary_progress());
        properties
    }

    /// [`Storage`]'s.
    fn sources() -> Sources {
        storage_sources()
    }

    /// [`Storage`]'s.
    fn assumptions((scenario, _): &(StorageScenario, u64)) -> Assumptions<Self> {
        recovering_assumptions(scenario)
    }

    /// Never drawn: limited runs are typed.
    fn draw(_: &[u8]) -> (StorageScenario, u64) {
        unreachable!("limited runs are typed")
    }
}

/// Every storage recovery witness's honest runs take at most half the
/// livelock limit at one instant after its `recovered` pick, under 16
/// plans, so the limit has room for plans these don't sample. Each ends
/// idle at its finish.
#[test]
fn honest_recoveries_stay_under_half_the_livelock_limit() {
    let half = RECOVERY_BUDGET / 10 / 2;
    assert!(Storage::assumptions(&storage_witnesses::recovering(vec![
        storage_witnesses::recover(None)
    ]))
    .declares(Assume::BoundedAdversary));
    for (class, scenario) in witnesses() {
        for input in inputs(16, 256) {
            let scenario = (scenario.clone(), half);
            let report = run_prepared::<Limited>(
                &scenario,
                Limited::prepare(&scenario),
                input,
                &mut MemorySink::default(),
                None,
            )
            .unwrap_or_else(|error| panic!("{class}: {error}"));
            assert_eq!(report.end.violation, None, "{class}: {:?}", report.end);
            assert!(
                matches!(report.end.outcome, Outcome::Idle { .. }),
                "{class}: {:?}",
                report.end
            );
        }
    }
}

/// `header-sync/storage` on a scenario, with one more task: it waits, a
/// millisecond at a time, for the `recovered` pick, then spins at that
/// instant from then on. It stands in for a reactor that keeps the clock
/// still with an obligation open.
struct Spinning(Storage);

impl Wraps for Spinning {
    fn storage(&self) -> &Storage {
        &self.0
    }
}

impl HasView for Spinning {
    type View = StorageView;

    /// The storage harness's view.
    fn view(&self) -> StorageView {
        self.0.view()
    }

    /// The storage harness's end.
    fn ended(&self, end: &RunEnd) {
        self.0.ended(end);
    }

    /// The storage harness's end violations.
    fn end_violations(&self, log: &ObservationLog) -> Vec<EndViolation> {
        self.0.end_violations(log)
    }

    /// The storage harness's stamps.
    fn stamps_after(&self, after: Option<u64>) -> Option<Vec<u64>> {
        self.0.stamps_after(after)
    }
}

impl Subsystem for Spinning {
    const NAME: &'static str = "test/recovery-spinning";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = StorageScenario;
    type Prepared = Ports;

    /// [`Storage`]'s.
    fn config(scenario: &StorageScenario) -> RunConfig {
        Storage::config(scenario)
    }

    /// [`Storage`]'s.
    fn prepare(scenario: &StorageScenario) -> Ports {
        Storage::prepare(scenario)
    }

    /// The storage harness, then the spinning task.
    fn build(sim: &mut Sim, scenario: &StorageScenario, ports: Ports) -> Self {
        let recovery: RecoveryEvidence = ports.recovery.clone();
        let storage = Storage::build(sim, scenario, ports);
        sim.scheduler().spawn_root("spin", async move {
            while recovery.record().is_none() {
                zakura_rt::time::sleep(Duration::from_millis(1)).await;
            }
            loop {
                zakura_rt::task::yield_now().await;
            }
        });
        Spinning(storage)
    }

    /// [`Storage`]'s.
    fn properties() -> Vec<Property<Self>> {
        let mut properties = storage_properties();
        properties.push(zakura_sim::domain::header_sync_progress::bounded_adversary_progress());
        properties
    }

    /// [`Storage`]'s.
    fn sources() -> Sources {
        storage_sources()
    }

    /// [`Storage`]'s.
    fn assumptions(scenario: &StorageScenario) -> Assumptions<Self> {
        recovering_assumptions(scenario)
    }

    /// Never drawn: spinning runs are typed.
    fn draw(_: &[u8]) -> StorageScenario {
        unreachable!("spinning runs are typed")
    }
}

/// hdr-13's owed reply-hold witness grows its source just before the
/// recover, so its recovery owes the grown tip at the `recovered` pick,
/// which reaches the node only behind the source's status floor. The
/// spinning task keeps the clock a millisecond after the pick, so that never
/// comes. Under 8 plans the spin violates `sim/no-livelock` without a panic,
/// with the property's obligation still open, instead of the run ending at
/// its budget.
#[test]
fn a_spin_after_recovered_violates_no_livelock() {
    let (_, scenario) = storage_witnesses::owed_recovery_witnesses()
        .into_iter()
        .find(|(class, _)| *class == "reply holds")
        .expect("hdr-13 has an owed reply-hold witness");
    for input in inputs(8, 256) {
        let report = run_prepared::<Spinning>(
            &scenario,
            Spinning::prepare(&scenario),
            input,
            &mut MemorySink::default(),
            None,
        )
        .unwrap_or_else(|error| panic!("{error}"));
        let violation = report.end.violation.as_ref().expect("the spin violates");
        assert_eq!(violation.property, NO_LIVELOCK, "{:?}", report.end);
        assert_eq!(report.end.harness, None, "{:?}", report.end);
        assert!(
            report.log.entries().iter().all(|entry| !matches!(
                &entry.obs,
                Observation::TaskExited {
                    exit: Exit::Panicked,
                    ..
                }
            )),
            "nothing panics"
        );
        let progress = report
            .properties
            .iter()
            .find(|reported| reported.property == BOUNDED_ADVERSARY_PROGRESS)
            .expect("the property reports");
        assert!(
            matches!(
                progress.outcome,
                zakura_sim::check::PropertyOutcome::Inconclusive { .. }
            ),
            "the obligation was still open: {:?}",
            progress.outcome
        );
    }
}
