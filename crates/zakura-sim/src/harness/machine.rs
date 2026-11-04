//! Transport-free execution of a pure machine's history.
//!
//! A [`Machine`] names a decoder that runs the real owner against its
//! independent model and returns one [`MachineStep`] per operation. This module
//! runs that history under the shared registry, recorder, replay verifier and
//! explorer. [`machine_entry!`](crate::machine_entry) makes a [`Machine`] a
//! [`HarnessEntry`](super::HarnessEntry).

use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

use serde_json::Value;
use zakura_scheduler::{CleanupReport, SimInstant, Status as SchedulerStatus};

use super::HarnessError;
use crate::{
    action::{Action, Enabled, SourceId},
    check::{Checker, Phase, PropertyReport, StepCtx, Violation},
    end::{HarnessFailure, IdleStatus, Outcome, RunEnd},
    explore::ExploreRun,
    plan::Decider,
    property::{
        Assumptions, Capture, CheckCtx, HasView, Kind, Property, PropertyId, Registry,
        RegistryChecker, Source, Sources, Status, Verdict,
    },
    record::{
        machine::{MachineScenario, MachineStep},
        InitRecord, StepRecord, CONTRACT_VERSION, FORMAT_VERSION,
    },
    sim::Pick,
    trace::{
        meta::{BudgetRecord, PropertyRef, SchedulerRecord, RNG},
        BuildInfo, MetaRecord,
    },
    witness::{Expect, WitnessError},
    Identity, ObservationLog, PlanInput, Record, RecordSink, RunConfig, RunReport,
};

/// The most causal bytes a machine reads. The bytes after them are Plan input.
pub const INPUT_BYTES: usize = 512;

/// One pure machine: the facts its harness entry and property need.
pub struct Machine {
    /// The harness name and the ID of its one property.
    pub name: &'static str,
    /// The `machine:<identity>:<step>` part of each pick's identity.
    pub identity: &'static str,
    /// The property's claim, in one sentence.
    pub claim: &'static str,
    /// The decoder's version.
    pub generator: u32,
    /// The most operations the decoder returns.
    pub budget: u64,
    /// The numbered operations that the causal input decodes to. A disagreement
    /// with the model must appear as the failure of the last step.
    pub history: fn(&[u8]) -> Vec<(usize, MachineStep)>,
    /// Whether a run's steps reached the resource-bearing transition that a
    /// witness expecting [`Expect`] needs. [`Expect::Applicable`] never asks.
    /// The property applies from the first step whose prefix reaches it under
    /// [`Expect::Holds`], so a run that never reaches it is not applicable.
    pub reached: fn(&[MachineStep], Expect) -> bool,
}

/// The [`HarnessEntry`](crate::harness::HarnessEntry) of the
/// [`Machine`] constant at `$machine`.
#[macro_export]
macro_rules! machine_entry {
    ($machine:path) => {
        $crate::harness::HarnessEntry {
            name: $machine.name,
            generator: $machine.generator,
            scenario_bytes: $crate::harness::machine::INPUT_BYTES,
            machine: true,
            composes: &[],
            planted: false,
            run: |value, input, sink| $crate::harness::machine::run(&$machine, value, input, sink),
            meta: |value| $crate::harness::machine::meta(&$machine, value),
            config: |value| $crate::harness::machine::config(&$machine, value),
            draw: $crate::harness::machine::draw,
            properties: || $crate::harness::machine::registered(&$machine),
            witness: |value, input, property, expect, sink| {
                $crate::harness::machine::witness(&$machine, value, input, property, expect, sink)
            },
            run_bytes: |bytes, sink, extra| {
                $crate::harness::machine::run_bytes(&$machine, bytes, sink, extra)
            },
        }
    };
}

/// The current step, and whether the run's steps so far reached the machine's
/// resource-bearing transition.
struct Handle {
    step: RefCell<Option<(u64, MachineStep)>>,
    reached: Cell<bool>,
}

/// A step and whether the run reached [`Machine::reached`] by it.
type StepView = (MachineStep, bool);

impl HasView for Handle {
    type View = Option<StepView>;

    fn view(&self) -> Self::View {
        self.step
            .borrow()
            .as_ref()
            .map(|(_, step)| (step.clone(), self.reached.get()))
    }

    fn captured(&self) -> Capture {
        self.step
            .borrow()
            .as_ref()
            .map_or(Capture::Missing, |(step, _)| Capture::AfterStep(*step))
    }
}

fn properties(machine: &Machine) -> Vec<Property<Handle>> {
    vec![Property {
        id: PropertyId::new(machine.name),
        claim: machine.claim,
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::View],
        status: Status::Active,
        check,
        witness: None,
    }]
}

/// A step that fails is a violation, even before the run reaches the
/// resource-bearing transition. A step that passes counts as an applicable
/// evaluation only once the run has reached it, so idle-only runs are not
/// coverage.
fn check(cx: &mut CheckCtx<'_, Handle>) -> Verdict {
    let Some((step, reached)) = cx.view() else {
        cx.missing_evidence("machine transition has no captured view");
        return Verdict::NotApplicable;
    };
    let Some(failure) = step.failure() else {
        return if reached {
            Verdict::Holds
        } else {
            Verdict::NotApplicable
        };
    };
    if !failure.property_failure {
        cx.missing_evidence(format!("machine driver panicked: {}", failure.message));
        return Verdict::NotApplicable;
    }
    Verdict::Violated(Violation {
        property: cx.id().to_string(),
        step: cx.step,
        witness: (1..=cx.step).collect(),
        expected: "independent model agrees with the transition and retained state".into(),
        actual: format!("{:?}: {}", failure.phase, failure.message),
    })
}

/// [`HarnessEntry::properties`](crate::harness::HarnessEntry::properties).
pub fn registered(machine: &Machine) -> Vec<PropertyRef> {
    properties(machine)
        .into_iter()
        .map(|property| PropertyRef {
            id: property.id.to_string(),
            status: property.status,
        })
        .collect()
}

fn scenario(value: &Value) -> Result<MachineScenario, HarnessError> {
    serde_json::from_value(value.clone()).map_err(HarnessError::Scenario)
}

/// [`HarnessEntry::config`](crate::harness::HarnessEntry::config).
pub fn config(machine: &Machine, value: &Value) -> Result<RunConfig, HarnessError> {
    let _ = scenario(value)?;
    Ok(RunConfig {
        budget: machine.budget,
        drain_bound: 0,
        ..RunConfig::default()
    })
}

/// [`HarnessEntry::meta`](crate::harness::HarnessEntry::meta).
pub fn meta(machine: &Machine, value: &Value) -> Result<MetaRecord, HarnessError> {
    let config = config(machine, value)?;
    Ok(MetaRecord {
        format: FORMAT_VERSION,
        contract: CONTRACT_VERSION,
        generator: machine.generator,
        build: BuildInfo::current(),
        sim: machine.name.into(),
        scenario: serde_json::to_value(scenario(value)?).map_err(HarnessError::Scenario)?,
        assumptions: Vec::new(),
        scheduler: SchedulerRecord::from(config.scheduler),
        budget: BudgetRecord::from(&config),
        rng: RNG.into(),
        fixtures: Vec::new(),
        properties: registered(machine),
        input: None,
        derived_from: None,
    })
}

/// [`HarnessEntry::draw`](crate::harness::HarnessEntry::draw).
pub fn draw(bytes: &[u8]) -> Value {
    serde_json::to_value(MachineScenario {
        input: bytes[..bytes.len().min(INPUT_BYTES)].to_vec(),
    })
    .expect("a byte vector serializes")
}

/// [`HarnessEntry::run`](crate::harness::HarnessEntry::run).
pub fn run(
    machine: &Machine,
    value: &Value,
    input: PlanInput,
    sink: &mut dyn RecordSink,
) -> Result<RunReport, HarnessError> {
    run_checked(machine, value, input, sink, None)
}

#[allow(
    clippy::unwrap_in_result,
    reason = "duplicate static property IDs are a programming error"
)]
fn run_checked(
    machine: &Machine,
    value: &Value,
    input: PlanInput,
    sink: &mut dyn RecordSink,
    mut extra: Option<&mut dyn Checker>,
) -> Result<RunReport, HarnessError> {
    let scenario = scenario(value)?;
    let history = (machine.history)(&scenario.input);
    let handle = Rc::new(Handle {
        step: RefCell::new(None),
        reached: Cell::new(false),
    });
    // The steps so far, kept only until the run reaches the transition.
    let mut prefix = Vec::new();
    let mut checker = RegistryChecker::new(
        Registry::new(properties(machine)).expect("unique static property"),
        Rc::clone(&handle),
        Assumptions::none(),
        &Sources::observed(&[Source::View]),
    );
    let mut decider = Decider::new(input);
    let log = ObservationLog::default();
    let mut picks = Vec::new();
    let mut end = RunEnd {
        outcome: Outcome::Idle {
            status: IdleStatus::Quiescent,
        },
        steps: 0,
        violation: None,
        drain: None,
        harness: checker.harness_failure(),
    };
    sink.record(Record::Init(InitRecord::default()));
    for (number, work) in history {
        if end.harness.is_some() {
            break;
        }
        // A history's length is bounded by INPUT_BYTES, so its numbers fit.
        let number = number as u64;
        let identity = Identity::machine(machine.identity, number);
        let decision = match decider.pick(std::slice::from_ref(&identity)) {
            Ok(decision) => decision,
            Err(error) => {
                end.harness = Some(HarnessFailure::Plan(error));
                break;
            }
        };
        if !handle.reached.get() {
            prefix.push(work.clone());
            handle
                .reached
                .set((machine.reached)(&prefix, Expect::Holds));
        }
        *handle.step.borrow_mut() = Some((number, work.clone()));
        end.steps = number;
        picks.push(Pick {
            step: number,
            identity: identity.to_string(),
            options: 1,
            phase: Phase::Live,
        });
        if sink.wants_steps() {
            sink.record(Record::Step(StepRecord {
                step: number,
                pick: identity.to_string(),
                key: None,
                options: 1,
                by: decision.by,
                offset: decision.offset,
                draws: Vec::new(),
                obs: None,
                machine: Some(Box::new(work)),
            }));
        }
        let enabled = [Enabled {
            identity: identity.clone(),
            action: Action::Source {
                source: SourceId(0),
                local: number,
            },
        }];
        let cx = StepCtx {
            step: number,
            pick: &identity,
            enabled: &enabled,
            phase: Phase::Live,
            now: SimInstant::ZERO,
            status: SchedulerStatus::Quiescent,
            log: &log,
        };
        end.violation = checker.after_step(&cx);
        end.harness = checker.harness_failure();
        if let Some(extra) = extra.as_deref_mut() {
            extra.observe_events(&checker.events());
            let other = extra.after_step(&cx);
            if end.violation.is_none() {
                end.violation = other;
            }
            if end.harness.is_none() {
                end.harness = extra.harness_failure();
            }
        }
        if end.violation.is_some() || end.harness.is_some() {
            break;
        }
    }
    if end.harness.is_none() {
        if let Err(error) = decider.finish() {
            end.harness = Some(HarnessFailure::Plan(error));
        }
    }
    end.outcome = if end.harness.is_some() {
        Outcome::Harness
    } else if end.violation.is_some() {
        Outcome::Violation
    } else {
        Outcome::Idle {
            status: IdleStatus::Quiescent,
        }
    };
    let mut properties = checker.at_end(&end, &log);
    if let Some(extra) = extra {
        properties.extend(extra.at_end(&end, &log));
    }
    // As in `Sim::run`: a harness failure leaves no property held, since the
    // run's evidence may be incomplete. The report and the end record both
    // take these.
    if end.outcome == Outcome::Harness {
        properties = properties
            .into_iter()
            .map(|report| PropertyReport {
                outcome: report.outcome.after_harness_failure(),
                ..report
            })
            .collect();
    }
    let report = RunReport {
        end,
        properties,
        cleanup: CleanupReport::default(),
        log,
        picks,
        resolved: decider.resolved(),
    };
    sink.record(Record::End(report.end_record()));
    match report.end.harness.clone() {
        Some(failure) => Err(HarnessError::Run {
            failure,
            report: Box::new(report),
        }),
        None => Ok(report),
    }
}

/// [`HarnessEntry::run_bytes`](crate::harness::HarnessEntry::run_bytes).
pub fn run_bytes(
    machine: &Machine,
    bytes: &[u8],
    sink: &mut dyn RecordSink,
    extra: Option<&mut dyn Checker>,
) -> ExploreRun {
    let scenario = draw(bytes);
    let result = run_checked(
        machine,
        &scenario,
        PlanInput::Bytes(bytes[bytes.len().min(INPUT_BYTES)..].to_vec()),
        sink,
        extra,
    );
    ExploreRun {
        scenario: Some(scenario),
        result,
    }
}

/// [`HarnessEntry::witness`](crate::harness::HarnessEntry::witness).
pub fn witness(
    machine: &Machine,
    value: &Value,
    input: PlanInput,
    property: &str,
    expect: Expect,
    sink: &mut dyn RecordSink,
) -> Result<(), WitnessError> {
    if property != machine.name {
        return Err(WitnessError::Unregistered);
    }
    let mut evidence = WitnessSink {
        sink,
        steps: Vec::new(),
    };
    let report = run(machine, value, input, &mut evidence)
        .map_err(|error| WitnessError::HarnessEnd(error.to_string()))?;
    crate::witness::judge(&report, property, Status::Active, expect)?;
    if expect != Expect::Applicable && !(machine.reached)(&evidence.steps, expect) {
        return Err(WitnessError::LostTriggerHistory);
    }
    Ok(())
}

/// Keeps the semantic records that exact replay verifies, for
/// [`Machine::reached`] to read.
struct WitnessSink<'a> {
    sink: &'a mut dyn RecordSink,
    steps: Vec<MachineStep>,
}

impl RecordSink for WitnessSink<'_> {
    fn record(&mut self, record: Record) {
        if let Record::Step(step) = &record {
            if let Some(machine) = step.machine.as_deref() {
                self.steps.push(machine.clone());
            }
        }
        self.sink.record(record);
    }
}
