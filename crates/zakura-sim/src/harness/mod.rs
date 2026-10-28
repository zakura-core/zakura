//! The harness registry: every scenario a trace can name (traces
//! implementation plan, T1b).
//!
//! A [`Subsystem`] builds one scenario on a [`Sim`] and checks it. A trace's
//! `meta.sim` names the harness, and `meta.scenario` holds its serialized
//! scenario, so [`find`] and [`HarnessEntry::run`] can rerun any trace
//! without knowing its type.
//!
//! Every harness checks its properties with the registry, plus the step
//! loop's own: [`NO_TASK_PANIC`] always, and [`NO_LIVELOCK`] while the
//! scenario's [`Assume::BoundedAdversary`] holds ([`run_config`]).

pub mod block_node;
pub mod block_sync;
pub mod coverage;
pub mod discovery;
pub mod fixtures;
pub mod header_node;
pub mod header_sync;
pub mod policy;
mod record;

use std::{fmt, rc::Rc};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;

pub use record::{record, record_witness, RecordError, RecordInput, WITNESS_FILE};

use crate::{
    check::{Checker, Phase, PropertyReport, StepCtx, Violation},
    end::{HarnessFailure, Outcome, RunEnd},
    observe::Exit,
    property::{
        Assume, Assumptions, CheckCtx, CheckEvent, HasView, Kind, Property, PropertyId, Registry,
        RegistryChecker, Source, Sources, Status, Tee, Verdict,
    },
    record::{CONTRACT_VERSION, FORMAT_VERSION},
    sim::NO_LIVELOCK,
    trace::{
        meta::{BudgetRecord, PropertyRef, SchedulerRecord, RNG},
        BuildInfo, MetaRecord,
    },
    witness::{witness_into, Expect, WitnessError},
    Observation, ObservationLog, PlanInput, RecordSink, RunConfig, RunReport, Sim,
};

/// The property a run violates when a task panics. Every harness checks it.
pub const NO_TASK_PANIC: &str = "sim/no-task-panic";

/// One runnable scenario family, such as discovery's `wait_complete` run.
///
/// The built harness is the [`HasView`] handle. [`run_harness`] keeps it in
/// an `Rc`, so the checker can hold a clone and read the view after each
/// step. Production tasks never hold that `Rc`.
///
/// Its functions of the scenario must depend on the scenario alone. [`meta`]
/// and each replay call them again, and must get what the run used.
pub trait Subsystem: HasView + Sized + 'static {
    /// The harness's name, written to `meta.sim`, such as
    /// `discovery/wait-complete`.
    const NAME: &'static str;

    /// The generator version. Bump it when [`Subsystem::draw`] or its byte
    /// decoding changes.
    const GENERATOR: u32;

    /// The length of the scenario prefix in fuzz input. Bump `GENERATOR`
    /// when it changes. A harness with a fixed scenario sets 0.
    const SCENARIO_BYTES: usize;

    /// Everything that varies between runs of this harness, written in full
    /// to `meta.scenario`. It must hold no floats.
    type Scenario: Serialize + DeserializeOwned + Clone;

    /// What [`Subsystem::prepare`] sets up before the run, such as storage
    /// with its chains committed. `()` for a harness with nothing to set up.
    type Prepared;

    /// The run's limits for `scenario`. [`run_config`] adds the livelock
    /// limit.
    fn config(scenario: &Self::Scenario) -> RunConfig;

    /// Sets up what must exist before `Sim::new`, outside any run, such as
    /// committed fixture chains, which validate against the host clock only
    /// there. [`run_harness`] calls it, then hands the result to `build`.
    fn prepare(scenario: &Self::Scenario) -> Self::Prepared;

    /// Spawns the production tasks and adds the action sources, using only
    /// the `prepared` state it's handed.
    fn build(sim: &mut Sim, scenario: &Self::Scenario, prepared: Self::Prepared) -> Self;

    /// The harness's own properties, in report order. None by default.
    fn properties() -> Vec<Property<Self>> {
        Vec::new()
    }

    /// The sources the harness observes, and the ones its tier excludes.
    /// By default, the step loop's task observations only.
    fn sources() -> Sources {
        Sources::observed(&[Source::Schedule])
    }

    /// The assumptions `scenario` declares. None by default.
    fn assumptions(_scenario: &Self::Scenario) -> Assumptions<Self> {
        Assumptions::none()
    }

    /// Decodes a scenario from exactly [`Subsystem::SCENARIO_BYTES`] bytes of
    /// fuzz input.
    fn draw(prefix: &[u8]) -> Self::Scenario;
}

/// Why a harness couldn't run a scenario.
#[derive(Debug)]
pub enum HarnessError {
    /// `meta.sim` names no entry in [`HARNESSES`].
    UnknownHarness(String),
    /// `meta.scenario` doesn't deserialize as the harness's scenario.
    Scenario(serde_json::Error),
    /// The run ended in a harness failure.
    Run {
        /// The failure, from `RunEnd::harness`.
        failure: HarnessFailure,
        /// The run's report, for callers that still read its log.
        report: Box<RunReport>,
    },
}

impl fmt::Display for HarnessError {
    /// Names the error.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HarnessError::UnknownHarness(name) => write!(f, "no harness is named `{name}`"),
            HarnessError::Scenario(error) => {
                write!(f, "the scenario doesn't fit the harness: {error}")
            }
            HarnessError::Run { failure, .. } => {
                write!(f, "the run hit a harness failure: {failure}")
            }
        }
    }
}

impl std::error::Error for HarnessError {}

/// Builds, runs and checks one scenario, emitting its records to `sink`.
/// `extra` runs after the harness's own checker in a [`Tee`], for callers
/// that collect more than the harness checks.
///
/// Returns the report, or [`HarnessError::Run`] with it if the run ended in
/// a harness failure. The `end` record goes to `sink` either way.
pub fn run_harness<S: Subsystem>(
    scenario: &S::Scenario,
    input: PlanInput,
    sink: &mut dyn RecordSink,
    extra: Option<&mut dyn Checker>,
) -> Result<RunReport, HarnessError> {
    run_prepared::<S>(scenario, S::prepare(scenario), input, sink, extra)
}

/// [`run_harness`] with state `prepared` by the caller, who may keep a
/// clone of it, such as a port handle, to inspect after the run.
pub fn run_prepared<S: Subsystem>(
    scenario: &S::Scenario,
    prepared: S::Prepared,
    input: PlanInput,
    sink: &mut dyn RecordSink,
    extra: Option<&mut dyn Checker>,
) -> Result<RunReport, HarnessError> {
    // One read of the assumptions sets both the step loop's livelock limit
    // and the checker's, so the two can't disagree (V12 F-299).
    let assumptions = S::assumptions(scenario);
    let config = limit_livelock(S::config(scenario), &assumptions);
    let mut sim = Sim::new(config, input);
    let this = Rc::new(S::build(&mut sim, scenario, prepared));
    let mut checker = registry_checker(&this, assumptions);
    let report = match extra {
        Some(extra) => sim.run(&mut Tee(Dyn(&mut *checker), Dyn(extra)), sink),
        None => sim.run(&mut *checker, sink),
    };
    // The harness outlives the run, so production tasks never see their
    // handles close mid-run.
    drop(checker);
    drop(this);
    match (&report.end.outcome, &report.end.harness) {
        (Outcome::Harness, Some(failure)) => Err(HarnessError::Run {
            failure: failure.clone(),
            report: Box::new(report),
        }),
        _ => Ok(report),
    }
}

/// The `meta` record for a run of `scenario`, without `input`. Its
/// `scenario` field is the scenario reserialized, so equal scenarios write
/// equal bytes.
pub fn meta<S: Subsystem>(scenario: &S::Scenario) -> Result<MetaRecord, HarnessError> {
    let config = run_config::<S>(scenario);
    Ok(MetaRecord {
        format: FORMAT_VERSION,
        contract: CONTRACT_VERSION,
        generator: S::GENERATOR,
        build: BuildInfo::current(),
        sim: S::NAME.to_owned(),
        scenario: serde_json::to_value(scenario).map_err(HarnessError::Scenario)?,
        assumptions: S::assumptions(scenario).records().to_vec(),
        scheduler: SchedulerRecord::from(config.scheduler),
        budget: BudgetRecord::from(&config),
        rng: RNG.to_owned(),
        fixtures: Vec::new(),
        properties: registered::<S>(),
        input: None,
        derived_from: None,
    })
}

/// The run's limits for `scenario`: [`Subsystem::config`], with
/// [`NO_LIVELOCK`] enforced when the scenario declares
/// [`Assume::BoundedAdversary`]. The step loop counts only steps taken while
/// the assumption holds ([`Checker::bounds_livelock`]).
///
/// Hostile peers then act a bounded number of times per instant, so a node
/// that keeps the clock from moving is spinning on its own. The limit sits
/// at a tenth of the budget, so the livelock check fires well before the
/// budget ends the run.
pub fn run_config<S: Subsystem>(scenario: &S::Scenario) -> RunConfig {
    limit_livelock(S::config(scenario), &S::assumptions(scenario))
}

/// `config` with [`NO_LIVELOCK`] enforced if `assumptions` declares
/// [`Assume::BoundedAdversary`] ([`run_config`]).
fn limit_livelock<S>(mut config: RunConfig, assumptions: &Assumptions<S>) -> RunConfig {
    if assumptions.declares(Assume::BoundedAdversary) {
        config.enforce_no_livelock = true;
        config.max_steps_per_instant = config
            .max_steps_per_instant
            .min((config.budget / 10).max(1));
    }
    config
}

/// The run's checker: the registry of the step loop's properties and `S`'s,
/// over the assumptions [`run_harness`] read once for the run.
fn registry_checker<S: Subsystem>(this: &Rc<S>, assumptions: Assumptions<S>) -> Box<dyn Checker> {
    Box::new(RegistryChecker::new(
        registry::<S>(),
        Rc::clone(this),
        assumptions,
        &S::sources(),
    ))
}

/// The step loop's properties, then `S`'s.
///
/// # Panics
///
/// If two share an ID. `every_harness_names_its_statuses` builds every
/// harness's registry.
pub(crate) fn registry<S: Subsystem>() -> Registry<S> {
    let mut properties = vec![
        Property {
            id: PropertyId::new(NO_TASK_PANIC),
            claim: "no task panics",
            kind: Kind::Safety,
            assumes: &[],
            needs: &[Source::Schedule],
            status: Status::Active,
            check: no_task_panic::<S>,
            witness: None,
        },
        Property {
            id: PropertyId::new(NO_LIVELOCK),
            claim: "the clock moves within the step limit per instant",
            kind: Kind::Safety,
            assumes: &[Assume::BoundedAdversary],
            needs: &[Source::Schedule],
            status: Status::Active,
            check: live_step::<S>,
            witness: None,
        },
    ];
    properties.extend(S::properties());
    Registry::new(properties).unwrap_or_else(|error| panic!("{}: {error}", S::NAME))
}

/// Checks [`NO_TASK_PANIC`]: violated at a step where a task panicked.
fn no_task_panic<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    let panicked = cx.observations().find_map(|obs| match obs {
        Observation::TaskExited {
            task,
            exit: Exit::Panicked,
        } => Some(task.clone()),
        _ => None,
    });
    match panicked {
        Some(task) => cx.violated(
            "every task exits without panicking",
            format!("{task} panicked"),
        ),
        None => Verdict::Holds,
    }
}

/// Applies [`NO_LIVELOCK`] at each live step. The step loop finds the
/// violation, and only while [`Assume::BoundedAdversary`] holds; the
/// registry reports it as this property's.
fn live_step<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    match cx.phase {
        Phase::Live => Verdict::Holds,
        Phase::Drain => Verdict::NotApplicable,
    }
}

/// Every property `S` checks, in report order, for `meta.properties`: the
/// registry's.
fn registered<S: Subsystem>() -> Vec<PropertyRef> {
    registry::<S>()
        .properties()
        .iter()
        .map(|property| PropertyRef {
            id: property.id.to_string(),
            status: property.status,
        })
        .collect()
}

/// A harness by name, without its type, so a trace's `meta.sim` can pick
/// it.
pub struct HarnessEntry {
    /// [`Subsystem::NAME`].
    pub name: &'static str,
    /// [`Subsystem::GENERATOR`].
    pub generator: u32,
    /// [`Subsystem::SCENARIO_BYTES`].
    pub scenario_bytes: usize,
    /// [`run_harness`] on a serialized scenario, with no extra checker.
    pub run: fn(&Value, PlanInput, &mut dyn RecordSink) -> Result<RunReport, HarnessError>,
    /// [`meta`] for a serialized scenario.
    pub meta: fn(&Value) -> Result<MetaRecord, HarnessError>,
    /// [`run_config`] for a serialized scenario.
    pub config: fn(&Value) -> Result<RunConfig, HarnessError>,
    /// [`Subsystem::draw`], serialized. The prefix must be exactly
    /// `scenario_bytes` long.
    pub draw: fn(&[u8]) -> Value,
    /// Every property the harness checks in any scenario, with its status.
    pub properties: fn() -> Vec<PropertyRef>,
    /// [`witness_into`] on a serialized scenario.
    pub witness: WitnessFn,
}

/// [`HarnessEntry::witness`]: the serialized scenario, the input, the
/// property, the expectation and the sink.
pub type WitnessFn =
    fn(&Value, PlanInput, &str, Expect, &mut dyn RecordSink) -> Result<(), WitnessError>;

impl HarnessEntry {
    /// Property `id`'s status in this harness, as the explorer's classifier
    /// reads it. `None` if the harness never reports `id`.
    pub fn status(&self, id: &str) -> Option<Status> {
        (self.properties)()
            .into_iter()
            .find(|property| property.id == id)
            .map(|property| property.status)
    }
}

impl fmt::Debug for HarnessEntry {
    /// The harness's name and versions.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HarnessEntry")
            .field("name", &self.name)
            .field("generator", &self.generator)
            .field("scenario_bytes", &self.scenario_bytes)
            .finish_non_exhaustive()
    }
}

/// The entry for `S`.
pub const fn entry<S: Subsystem>() -> HarnessEntry {
    HarnessEntry {
        name: S::NAME,
        generator: S::GENERATOR,
        scenario_bytes: S::SCENARIO_BYTES,
        run: run_value::<S>,
        meta: meta_value::<S>,
        config: config_value::<S>,
        draw: draw_value::<S>,
        properties: registered::<S>,
        witness: witness_value::<S>,
    }
}

/// Every harness, by name.
pub static HARNESSES: &[HarnessEntry] = &[
    entry::<fixtures::Race<fixtures::LostUpdate>>(),
    entry::<fixtures::Race<fixtures::SnapshotReserve>>(),
    entry::<fixtures::Race<fixtures::MissedNotify>>(),
    entry::<fixtures::Race<fixtures::TimerCancel>>(),
    entry::<fixtures::Race<fixtures::AbBa>>(),
    entry::<fixtures::Race<fixtures::AbortWithoutJoin>>(),
    entry::<discovery::WaitComplete>(),
    entry::<header_sync::Smoke>(),
    entry::<header_sync::Storage>(),
    entry::<block_sync::Storage>(),
];

/// The harness named `name`.
pub fn find(name: &str) -> Result<&'static HarnessEntry, HarnessError> {
    HARNESSES
        .iter()
        .find(|entry| entry.name == name)
        .ok_or_else(|| HarnessError::UnknownHarness(name.to_owned()))
}

/// `scenario` as `S`'s scenario.
fn scenario<S: Subsystem>(scenario: &Value) -> Result<S::Scenario, HarnessError> {
    S::Scenario::deserialize(scenario).map_err(HarnessError::Scenario)
}

/// [`HarnessEntry::run`] for `S`.
fn run_value<S: Subsystem>(
    value: &Value,
    input: PlanInput,
    sink: &mut dyn RecordSink,
) -> Result<RunReport, HarnessError> {
    run_harness::<S>(&scenario::<S>(value)?, input, sink, None)
}

/// [`HarnessEntry::meta`] for `S`.
fn meta_value<S: Subsystem>(value: &Value) -> Result<MetaRecord, HarnessError> {
    meta::<S>(&scenario::<S>(value)?)
}

/// [`HarnessEntry::config`] for `S`.
fn config_value<S: Subsystem>(value: &Value) -> Result<RunConfig, HarnessError> {
    Ok(run_config::<S>(&scenario::<S>(value)?))
}

/// [`HarnessEntry::witness`] for `S`.
fn witness_value<S: Subsystem>(
    value: &Value,
    input: PlanInput,
    property: &str,
    expect: Expect,
    sink: &mut dyn RecordSink,
) -> Result<(), WitnessError> {
    let scenario = scenario::<S>(value)
        .map_err(|error| WitnessError::UnsupportedScenario(error.to_string()))?;
    witness_into::<S>(&scenario, input, property, expect, sink)
}

/// [`HarnessEntry::draw`] for `S`.
fn draw_value<S: Subsystem>(prefix: &[u8]) -> Value {
    serde_json::to_value(S::draw(prefix))
        .expect("scenarios are plain data, and serde_json serializes plain data")
}

/// A borrowed checker as a sized [`Checker`], so it can sit in a [`Tee`].
struct Dyn<'a>(&'a mut dyn Checker);

impl Checker for Dyn<'_> {
    /// Forwards to the checker.
    fn after_step(&mut self, cx: &StepCtx<'_>) -> Option<Violation> {
        self.0.after_step(cx)
    }

    /// Forwards to the checker.
    fn at_end(&mut self, end: &RunEnd, log: &ObservationLog) -> Vec<PropertyReport> {
        self.0.at_end(end, log)
    }

    /// Forwards to the checker.
    fn events(&mut self) -> Vec<CheckEvent> {
        self.0.events()
    }

    /// Forwards to the checker.
    fn observe_events(&mut self, events: &[CheckEvent]) {
        self.0.observe_events(events);
    }

    /// Forwards to the checker.
    fn harness_failure(&mut self) -> Option<HarnessFailure> {
        self.0.harness_failure()
    }

    /// Forwards to the checker.
    fn bounds_livelock(&mut self) -> bool {
        self.0.bounds_livelock()
    }

    /// Forwards to the checker.
    fn step_loop_violation(&mut self, violation: &Violation) {
        self.0.step_loop_violation(violation);
    }
}
