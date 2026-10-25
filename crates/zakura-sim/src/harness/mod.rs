//! The harness registry: every scenario a trace can name (traces
//! implementation plan, T1b).
//!
//! A [`Subsystem`] builds one scenario on a [`Sim`] and checks it. A trace's
//! `meta.sim` names the harness, and `meta.scenario` holds its serialized
//! scenario, so [`find`] and [`HarnessEntry::run`] can rerun any trace
//! without knowing its type.

pub mod discovery;
pub mod fixtures;
pub mod header_sync;
mod record;

use std::{fmt, rc::Rc};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;

pub use record::{record, RecordError, RecordInput};

use crate::{
    check::{Checker, PropertyReport, StepCtx, Violation},
    end::{HarnessFailure, Outcome, RunEnd},
    property::{AssumeRecord, CheckEvent, HasView, Tee},
    record::{CONTRACT_VERSION, FORMAT_VERSION},
    trace::{
        meta::{BudgetRecord, PropertyRef, SchedulerRecord, RNG},
        BuildInfo, MetaRecord,
    },
    ObservationLog, PlanInput, RecordSink, RunConfig, RunReport, Sim,
};

/// One runnable scenario family, such as discovery's `wait_complete` run.
///
/// The built harness is the [`HasView`] handle. [`run_harness`] keeps it in
/// an `Rc`, so the checker can hold a clone and read the view after each
/// step. Production tasks never hold that `Rc`.
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

    /// The run's limits for `scenario`.
    fn config(scenario: &Self::Scenario) -> RunConfig;

    /// Spawns the production tasks and adds the action sources.
    fn build(sim: &mut Sim, scenario: &Self::Scenario) -> Self;

    /// The checker for `scenario`. It may keep a clone of `this` and call
    /// `view()` inside `after_step`.
    fn checker(this: &Rc<Self>, scenario: &Self::Scenario) -> Box<dyn Checker>;

    /// Every property the checker reports, in report order, for
    /// `meta.properties`.
    fn properties(scenario: &Self::Scenario) -> Vec<PropertyRef>;

    /// The assumptions `scenario` declares, for `meta.assumptions`. None by
    /// default.
    fn assumptions(_scenario: &Self::Scenario) -> Vec<AssumeRecord> {
        Vec::new()
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
    /// The run ended in a harness failure, from `RunEnd::harness`.
    Run(HarnessFailure),
}

impl fmt::Display for HarnessError {
    /// Names the error.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HarnessError::UnknownHarness(name) => write!(f, "no harness is named `{name}`"),
            HarnessError::Scenario(error) => {
                write!(f, "the scenario doesn't fit the harness: {error}")
            }
            HarnessError::Run(failure) => write!(f, "the run hit a harness failure: {failure}"),
        }
    }
}

impl std::error::Error for HarnessError {}

/// Builds, runs and checks one scenario, emitting its records to `sink`.
/// `extra` runs after the harness's own checker in a [`Tee`], for callers
/// that collect more than the harness checks.
///
/// Returns the report, or [`HarnessError::Run`] if the run ended in a
/// harness failure. The `end` record goes to `sink` either way.
pub fn run_harness<S: Subsystem>(
    scenario: &S::Scenario,
    input: PlanInput,
    sink: &mut dyn RecordSink,
    extra: Option<&mut dyn Checker>,
) -> Result<RunReport, HarnessError> {
    let mut sim = Sim::new(S::config(scenario), input);
    let this = Rc::new(S::build(&mut sim, scenario));
    let mut checker = S::checker(&this, scenario);
    let report = match extra {
        Some(extra) => sim.run(&mut Tee(Dyn(&mut *checker), Dyn(extra)), sink),
        None => sim.run(&mut *checker, sink),
    };
    // The harness outlives the run, so production tasks never see their
    // handles close mid-run.
    drop(checker);
    drop(this);
    match (&report.end.outcome, &report.end.harness) {
        (Outcome::Harness, Some(failure)) => Err(HarnessError::Run(failure.clone())),
        _ => Ok(report),
    }
}

/// The `meta` record for a run of `scenario`, without `input`. Its
/// `scenario` field is the scenario reserialized, so equal scenarios write
/// equal bytes.
pub fn meta<S: Subsystem>(scenario: &S::Scenario) -> Result<MetaRecord, HarnessError> {
    let config = S::config(scenario);
    Ok(MetaRecord {
        format: FORMAT_VERSION,
        contract: CONTRACT_VERSION,
        generator: S::GENERATOR,
        build: BuildInfo::current(),
        sim: S::NAME.to_owned(),
        scenario: serde_json::to_value(scenario).map_err(HarnessError::Scenario)?,
        assumptions: S::assumptions(scenario),
        scheduler: SchedulerRecord::from(config.scheduler),
        budget: BudgetRecord::from(&config),
        rng: RNG.to_owned(),
        fixtures: Vec::new(),
        properties: S::properties(scenario),
        input: None,
        derived_from: None,
    })
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
    /// [`Subsystem::config`] for a serialized scenario.
    pub config: fn(&Value) -> Result<RunConfig, HarnessError>,
    /// [`Subsystem::draw`], serialized. The prefix must be exactly
    /// `scenario_bytes` long.
    pub draw: fn(&[u8]) -> Value,
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
    Ok(S::config(&scenario::<S>(value)?))
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
}
