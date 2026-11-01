//! Transport-free execution of header reactor and work ownership models.

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
        Assumptions, Capture, CheckCtx, HasView, Kind, Property, PropertyId, Registry,
        RegistryChecker, Source, Sources, Status, Verdict,
    },
    record::{machine::MachineStep, InitRecord, StepRecord, CONTRACT_VERSION, FORMAT_VERSION},
    sim::Pick,
    trace::{
        meta::{BudgetRecord, PropertyRef, SchedulerRecord, RNG},
        BuildInfo, MetaRecord,
    },
    witness::{Expect, WitnessError},
    Identity, ObservationLog, PlanInput, Record, RecordSink, RunConfig, RunReport,
};

#[derive(Clone, Copy)]
enum Target {
    Reactor,
}

impl Target {
    const fn name(self) -> &'static str {
        match self {
            Self::Reactor => "header/reactor-policy",
        }
    }

    const fn identity(self) -> &'static str {
        match self {
            Self::Reactor => "header-reactor",
        }
    }

    const fn claim(self) -> &'static str {
        match self {
            Self::Reactor => {
                "The header reactor's scheduling and locator state agree with the \
                 independent model after every input, with exact storage call identities."
            }
        }
    }

    fn history(self, input: &[u8]) -> Vec<(usize, MachineStep)> {
        match self {
            Self::Reactor => zakura_network::zakura::trace_header_reactor_bytes(input)
                .steps
                .into_iter()
                .map(|step| (step.step, MachineStep::HeaderReactor(step.into())))
                .collect(),
        }
    }

    /// Whether `step`'s model saw this target's resource-bearing transition.
    fn reached(self, step: &MachineStep) -> bool {
        match (self, step) {
            (Self::Reactor, MachineStep::HeaderReactor(work)) => !work.expected.calls.is_empty(),
        }
    }

    /// The expected and actual views of `step`, when they disagree.
    fn difference(self, step: &MachineStep) -> Option<(String, String)> {
        match step {
            MachineStep::HeaderReactor(work) => (work.actual != work.expected)
                .then(|| (format!("{:?}", work.expected), format!("{:?}", work.actual))),
        }
    }
}

/// Whether `name` names a machine harness in this module.
pub fn is_machine(name: &str) -> bool {
    matches!(name, "header/reactor-policy")
}

/// Causal input for the selected harness's decoder-v1.
///
/// Each harness has its own decoder identity. Each consumes at most 512 bytes.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    /// Complete causal machine input, independent of scheduling choices.
    pub input: Vec<u8>,
}

struct Handle(RefCell<Option<(u64, MachineStep)>>);

impl HasView for Handle {
    type View = Option<MachineStep>;

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

fn properties(target: Target) -> Vec<Property<Handle>> {
    vec![Property {
        id: PropertyId::new(target.name()),
        claim: target.claim(),
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::View],
        status: Status::Active,
        check: match target {
            Target::Reactor => |cx| check(Target::Reactor, cx),
        },
        witness: None,
    }]
}

fn check(target: Target, cx: &mut CheckCtx<'_, Handle>) -> Verdict {
    let Some(view) = cx.view() else {
        cx.missing_evidence("machine transition has no captured view");
        return Verdict::NotApplicable;
    };
    if let Some(failure) = view.failure() {
        if !failure.property_failure {
            cx.missing_evidence(format!("machine driver panicked: {}", failure.message));
            return Verdict::NotApplicable;
        }
        return Verdict::Violated(Violation {
            property: target.name().into(),
            step: cx.step,
            witness: (1..=cx.step).collect(),
            expected: "independent model agrees with the transition and retained state".into(),
            actual: format!("{:?}: {}", failure.phase, failure.message),
        });
    }
    if let Some((expected, actual)) = target.difference(&view) {
        return Verdict::Violated(Violation {
            property: target.name().into(),
            step: cx.step,
            witness: (1..=cx.step).collect(),
            expected,
            actual,
        });
    }
    Verdict::Holds
}

fn registered(target: Target) -> Vec<PropertyRef> {
    properties(target)
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

fn config(target: Target, value: &Value) -> Result<RunConfig, HarnessError> {
    let _ = scenario(value)?;
    Ok(RunConfig {
        budget: match target {
            Target::Reactor => 128,
        },
        drain_bound: 0,
        ..RunConfig::default()
    })
}

fn meta(target: Target, value: &Value) -> Result<MetaRecord, HarnessError> {
    let config = config(target, value)?;
    Ok(MetaRecord {
        format: FORMAT_VERSION,
        contract: CONTRACT_VERSION,
        generator: 1,
        build: BuildInfo::current(),
        sim: target.name().into(),
        scenario: serde_json::to_value(scenario(value)?).map_err(HarnessError::Scenario)?,
        assumptions: Vec::new(),
        scheduler: SchedulerRecord::from(config.scheduler),
        budget: BudgetRecord::from(&config),
        rng: RNG.into(),
        fixtures: Vec::new(),
        properties: registered(target),
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
    target: Target,
    value: &Value,
    input: PlanInput,
    sink: &mut dyn RecordSink,
) -> Result<RunReport, HarnessError> {
    run_checked(target, value, input, sink, None)
}

#[allow(
    clippy::unwrap_in_result,
    reason = "duplicate static property IDs are a programming error"
)]
fn run_checked(
    target: Target,
    value: &Value,
    input: PlanInput,
    sink: &mut dyn RecordSink,
    mut extra: Option<&mut dyn Checker>,
) -> Result<RunReport, HarnessError> {
    let scenario = scenario(value)?;
    let history = target.history(&scenario.input);
    let handle = Rc::new(Handle(RefCell::new(None)));
    let mut checker = RegistryChecker::new(
        Registry::new(properties(target)).expect("unique static property"),
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
        let number = number as u64;
        let identity = Identity::machine(target.identity(), number);
        let decision = match decider.pick(std::slice::from_ref(&identity)) {
            Ok(decision) => decision,
            Err(error) => {
                end.harness = Some(HarnessFailure::Plan(error));
                break;
            }
        };
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
    target: Target,
    bytes: &[u8],
    sink: &mut dyn RecordSink,
    extra: Option<&mut dyn Checker>,
) -> ExploreRun {
    let scenario = draw(bytes);
    let result = run_checked(
        target,
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
    target: Target,
    value: &Value,
    input: PlanInput,
    property: &str,
    expect: Expect,
    sink: &mut dyn RecordSink,
) -> Result<(), WitnessError> {
    if property != target.name() {
        return Err(WitnessError::Unregistered);
    }
    let mut evidence = WitnessSink {
        target,
        sink,
        reached: false,
    };
    let report = run(target, value, input, &mut evidence)
        .map_err(|error| WitnessError::HarnessEnd(error.to_string()))?;
    crate::witness::judge(&report, property, Status::Active, expect)?;
    if expect != Expect::Applicable && !evidence.reached {
        return Err(WitnessError::LostTriggerHistory);
    }
    Ok(())
}

// Machine witnesses read the same semantic records that exact replay verifies.
// The model's view proves the input reached a resource-bearing transition,
// including when a mutation keeps the actual owner from reaching it.
struct WitnessSink<'a> {
    target: Target,
    sink: &'a mut dyn RecordSink,
    reached: bool,
}

impl RecordSink for WitnessSink<'_> {
    fn record(&mut self, record: Record) {
        if let Record::Step(step) = &record {
            if let Some(machine) = step.machine.as_deref() {
                self.reached |= self.target.reached(machine);
            }
        }
        self.sink.record(record);
    }
}

/// Header reactor policy uses the shared registry, recorder, replay verifier and
/// explorer without starting a runtime.
pub const REACTOR_ENTRY: HarnessEntry = HarnessEntry {
    name: Target::Reactor.name(),
    generator: 1,
    scenario_bytes: 512,
    pad_scenario: false,
    composes: &[],
    planted: false,
    run: |value, input, sink| run(Target::Reactor, value, input, sink),
    meta: |value| meta(Target::Reactor, value),
    config: |value| config(Target::Reactor, value),
    draw,
    properties: || registered(Target::Reactor),
    witness: |value, input, property, expect, sink| {
        witness(Target::Reactor, value, input, property, expect, sink)
    },
    run_bytes: |bytes, sink, extra| run_bytes(Target::Reactor, bytes, sink, extra),
};
