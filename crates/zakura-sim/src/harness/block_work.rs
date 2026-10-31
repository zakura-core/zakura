//! Transport-free execution of the preserved block ownership model.

use std::{cell::RefCell, rc::Rc};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use zakura_scheduler::{CleanupReport, SimInstant, Status as SchedulerStatus};

use super::{HarnessEntry, HarnessError};
use crate::{
    action::{Action, Enabled, SourceId},
    check::{Checker, Phase, StepCtx, Violation},
    end::{HarnessFailure, IdleStatus, Outcome, RunEnd},
    explore::ExploreRun,
    plan::Decider,
    property::{
        Assumptions, Capture, HasView, Kind, Property, PropertyId, Registry, RegistryChecker,
        Source, Sources, Status, Verdict,
    },
    record::{
        machine::{MachineStep, WorkStep},
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

const NAME: &str = "block/work-ownership";
const PROPERTY: &str = "block/work-ownership";

/// Decoder-v1 input. The decoder consumes at most 512 bytes.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    /// Complete causal machine input, independent of scheduling choices.
    pub input: Vec<u8>,
}

struct Handle(RefCell<Option<(u64, WorkStep)>>);

impl HasView for Handle {
    type View = Option<WorkStep>;

    fn view(&self) -> Self::View {
        self.0.borrow().as_ref().map(|(_, step)| step.clone())
    }

    fn captured(&self) -> Capture {
        self.0
            .borrow()
            .as_ref()
            .map_or(Capture::Missing, |(step, _)| Capture::AfterStep(*step))
    }
}

fn properties() -> Vec<Property<Handle>> {
    vec![Property {
        id: PropertyId::new(PROPERTY),
        claim:
            "The block work owner agrees with the independent ownership model after every input.",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::View],
        status: Status::Active,
        check: |cx| {
            let Some(view) = cx.view() else {
                cx.missing_evidence("machine transition has no captured view");
                return Verdict::NotApplicable;
            };
            if let Some(failure) = view.failure {
                if !failure.property_failure {
                    cx.missing_evidence(format!("machine driver panicked: {}", failure.message));
                    return Verdict::NotApplicable;
                }
                return Verdict::Violated(Violation {
                    property: PROPERTY.into(),
                    step: cx.step,
                    witness: (1..=cx.step).collect(),
                    expected: "independent model agrees with the transition and retained state"
                        .into(),
                    actual: format!("{:?}: {}", failure.phase, failure.message),
                });
            }
            if view.actual != view.expected {
                return Verdict::Violated(Violation {
                    property: PROPERTY.into(),
                    step: cx.step,
                    witness: (1..=cx.step).collect(),
                    expected: format!("{:?}", view.expected),
                    actual: format!("{:?}", view.actual),
                });
            }
            Verdict::Holds
        },
        witness: None,
    }]
}

fn registered() -> Vec<PropertyRef> {
    properties()
        .into_iter()
        .map(|property| PropertyRef {
            id: property.id.to_string(),
            status: property.status,
        })
        .collect()
}

fn scenario(value: &Value) -> Result<Scenario, HarnessError> {
    serde_json::from_value(value.clone()).map_err(HarnessError::Scenario)
}

fn config(value: &Value) -> Result<RunConfig, HarnessError> {
    let _ = scenario(value)?;
    Ok(RunConfig {
        budget: 128,
        drain_bound: 0,
        ..RunConfig::default()
    })
}

fn meta(value: &Value) -> Result<MetaRecord, HarnessError> {
    let config = config(value)?;
    Ok(MetaRecord {
        format: FORMAT_VERSION,
        contract: CONTRACT_VERSION,
        generator: 1,
        build: BuildInfo::current(),
        sim: NAME.into(),
        scenario: serde_json::to_value(scenario(value)?).map_err(HarnessError::Scenario)?,
        assumptions: Vec::new(),
        scheduler: SchedulerRecord::from(config.scheduler),
        budget: BudgetRecord::from(&config),
        rng: RNG.into(),
        fixtures: Vec::new(),
        properties: registered(),
        input: None,
        derived_from: None,
    })
}

fn draw(bytes: &[u8]) -> Value {
    serde_json::to_value(Scenario {
        input: bytes[..bytes.len().min(512)].to_vec(),
    })
    .expect("a byte vector serializes")
}

fn run(
    value: &Value,
    input: PlanInput,
    sink: &mut dyn RecordSink,
) -> Result<RunReport, HarnessError> {
    run_checked(value, input, sink, None)
}

#[allow(
    clippy::unwrap_in_result,
    reason = "duplicate static property IDs are a programming error"
)]
fn run_checked(
    value: &Value,
    input: PlanInput,
    sink: &mut dyn RecordSink,
    mut extra: Option<&mut dyn Checker>,
) -> Result<RunReport, HarnessError> {
    let scenario = scenario(value)?;
    let history = zakura_network::zakura::trace_work_ownership_bytes(&scenario.input);
    let handle = Rc::new(Handle(RefCell::new(None)));
    let mut checker = RegistryChecker::new(
        Registry::new(properties()).expect("unique static property"),
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
    for step in history.steps {
        if end.harness.is_some() {
            break;
        }
        let number = step.step as u64;
        let identity = Identity::machine("block-work", number);
        let decision = match decider.pick(std::slice::from_ref(&identity)) {
            Ok(decision) => decision,
            Err(error) => {
                end.harness = Some(HarnessFailure::Plan(error));
                break;
            }
        };
        let work = WorkStep::from(step);
        *handle.0.borrow_mut() = Some((number, work.clone()));
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
                machine: Some(Box::new(MachineStep::Work(work))),
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

fn run_bytes(
    bytes: &[u8],
    sink: &mut dyn RecordSink,
    extra: Option<&mut dyn Checker>,
) -> ExploreRun {
    let scenario = ENTRY.scenario(bytes);
    let result = run_checked(
        &scenario,
        PlanInput::Bytes(bytes[bytes.len().min(512)..].to_vec()),
        sink,
        extra,
    );
    ExploreRun {
        scenario: Some(scenario),
        result,
    }
}

fn witness(
    value: &Value,
    input: PlanInput,
    property: &str,
    expect: Expect,
    sink: &mut dyn RecordSink,
) -> Result<(), WitnessError> {
    if property != PROPERTY {
        return Err(WitnessError::Unregistered);
    }
    let mut evidence = WitnessSink {
        sink,
        charged_work: false,
    };
    let report = run(value, input, &mut evidence)
        .map_err(|error| WitnessError::HarnessEnd(error.to_string()))?;
    crate::witness::judge(&report, property, Status::Active, expect)?;
    if expect != Expect::Applicable && !evidence.charged_work {
        return Err(WitnessError::LostTriggerHistory);
    }
    Ok(())
}

// Machine witnesses read the same semantic records that exact replay verifies.
// The model's nonzero charge proves the input reached resource-bearing work,
// including when a mutation prevents the actual owner from charging it.
struct WitnessSink<'a> {
    sink: &'a mut dyn RecordSink,
    charged_work: bool,
}

impl RecordSink for WitnessSink<'_> {
    fn record(&mut self, record: Record) {
        if let Record::Step(step) = &record {
            if let Some(MachineStep::Work(work)) = step.machine.as_deref() {
                self.charged_work |= work
                    .expected
                    .items
                    .iter()
                    .any(|item| item.in_flight && item.owner.is_some() && item.charge > 0);
            }
        }
        self.sink.record(record);
    }
}

/// The machine uses the shared registry, recorder, replay verifier and explorer.
pub const ENTRY: HarnessEntry = HarnessEntry {
    name: NAME,
    generator: 1,
    scenario_bytes: 512,
    pad_scenario: false,
    composes: &[],
    planted: false,
    run,
    meta,
    config,
    draw,
    properties: registered,
    witness,
    run_bytes,
};
