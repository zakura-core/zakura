//! Transport-free execution of discovery's record book and session owner.
//!
//! Each history runs the real owner against its independent model. A fixed
//! disclosure key, a seeded shuffle and a fixture clock make signed samples replay.

use std::{cell::RefCell, rc::Rc};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use zakura_network::zakura::BookFact;
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
    Book,
    State,
}

impl Target {
    const fn name(self) -> &'static str {
        match self {
            Self::Book => "discovery/record-book",
            Self::State => "discovery/session-installation",
        }
    }

    const fn identity(self) -> &'static str {
        match self {
            Self::Book => "discovery-book",
            Self::State => "discovery-state",
        }
    }

    fn history(self, input: &[u8]) -> Vec<(usize, MachineStep)> {
        match self {
            Self::Book => zakura_network::zakura::trace_discovery_book_bytes(input)
                .into_iter()
                .map(|step| (step.step, MachineStep::DiscoveryBook(step)))
                .collect(),
            Self::State => zakura_network::zakura::trace_discovery_state_bytes(input)
                .into_iter()
                .map(|step| (step.step, MachineStep::DiscoveryState(step)))
                .collect(),
        }
    }
}

/// Causal input for the selected harness's decoder-v2.
///
/// Each harness has its own decoder identity and consumes at most 512 bytes.
/// Decoder-v1 remains the `qa/fuzz/p2p` targets' corpus format.
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
        claim: match target {
            Target::Book => "After each operation, the book's entries, relay attribution and exact install, replacement and eviction facts agree with the independent model, and each signed sample stays inside its requester's disclosure window.",
            Target::State => "After each operation, admitted sessions, retained service hints and each verification result's installation agree with the independent model.",
        },
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::View],
        status: Status::Active,
        check: match target {
            Target::Book => |cx| check(Target::Book, cx),
            Target::State => |cx| check(Target::State, cx),
        },
        witness: None,
    }]
}

fn check(target: Target, cx: &mut CheckCtx<'_, Handle>) -> Verdict {
    let Some(view) = cx.view() else {
        cx.missing_evidence("machine transition has no captured view");
        return Verdict::NotApplicable;
    };
    let (failure, difference) = match (target, &view) {
        (Target::Book, MachineStep::DiscoveryBook(step)) => (
            &step.failure,
            (step.actual != step.expected)
                .then(|| (format!("{:?}", step.expected), format!("{:?}", step.actual))),
        ),
        (Target::State, MachineStep::DiscoveryState(step)) => (
            &step.failure,
            (step.actual != step.expected)
                .then(|| (format!("{:?}", step.expected), format!("{:?}", step.actual))),
        ),
        _ => {
            cx.missing_evidence("machine transition has the wrong evidence kind");
            return Verdict::NotApplicable;
        }
    };
    if let Some(failure) = failure {
        if !failure.property_failure {
            cx.missing_evidence(format!("machine driver panicked: {}", failure.message));
            return Verdict::NotApplicable;
        }
        return Verdict::Violated(Violation {
            property: target.name().into(),
            step: cx.step,
            witness: (1..=cx.step).collect(),
            expected: "independent model agrees with the transition and retained state".into(),
            actual: failure.message.clone(),
        });
    }
    if let Some((expected, actual)) = difference {
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

fn config(_: Target, value: &Value) -> Result<RunConfig, HarnessError> {
    let _ = scenario(value)?;
    Ok(RunConfig {
        budget: 128,
        drain_bound: 0,
        ..RunConfig::default()
    })
}

fn meta(target: Target, value: &Value) -> Result<MetaRecord, HarnessError> {
    let config = config(target, value)?;
    Ok(MetaRecord {
        format: FORMAT_VERSION,
        contract: CONTRACT_VERSION,
        generator: 2,
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
        sink,
        reached: Reached::default(),
    };
    let report = run(target, value, input, &mut evidence)
        .map_err(|error| WitnessError::HarnessEnd(error.to_string()))?;
    crate::witness::judge(&report, property, Status::Active, expect)?;
    if expect != Expect::Applicable && !evidence.reached.complete(target) {
        return Err(WitnessError::LostTriggerHistory);
    }
    Ok(())
}

/// The model's facts show that the input reached each resource-bearing rule,
/// including when a mutation stops the real owner from applying it.
#[derive(Default)]
struct Reached {
    installed: bool,
    replaced: bool,
    evicted: bool,
    sampled: bool,
    stale_result: bool,
    current_result: bool,
    retained_hint: bool,
}

impl Reached {
    fn complete(&self, target: Target) -> bool {
        match target {
            Target::Book => self.installed && self.replaced && self.evicted && self.sampled,
            Target::State => self.stale_result && self.current_result && self.retained_hint,
        }
    }

    fn note(&mut self, step: &MachineStep) {
        match step {
            MachineStep::DiscoveryBook(book) => {
                for change in &book.expected.changes {
                    match change {
                        BookFact::Installed { .. } => self.installed = true,
                        BookFact::Replaced { .. } => self.replaced = true,
                        BookFact::Evicted { .. } => self.evicted = true,
                    }
                }
                self.sampled |= !book.sample.is_empty();
            }
            MachineStep::DiscoveryState(state) => {
                let installation = state.expected.installation.as_deref();
                self.stale_result |= installation == Some("superseded");
                self.current_result |=
                    installation.is_some_and(|result| result.starts_with("installed:"));
                self.retained_hint |= !state.expected.hints.is_empty();
            }
        }
    }
}

// Machine witnesses read the same semantic records that exact replay verifies.
struct WitnessSink<'a> {
    sink: &'a mut dyn RecordSink,
    reached: Reached,
}

impl RecordSink for WitnessSink<'_> {
    fn record(&mut self, record: Record) {
        if let Record::Step(step) = &record {
            if let Some(machine) = step.machine.as_deref() {
                self.reached.note(machine);
            }
        }
        self.sink.record(record);
    }
}

/// The record book's history under the shared registry, recorder, replay verifier and explorer.
pub const BOOK_ENTRY: HarnessEntry = HarnessEntry {
    name: Target::Book.name(),
    generator: 2,
    scenario_bytes: 512,
    pad_scenario: false,
    composes: &[],
    planted: false,
    run: |value, input, sink| run(Target::Book, value, input, sink),
    meta: |value| meta(Target::Book, value),
    config: |value| config(Target::Book, value),
    draw,
    properties: || registered(Target::Book),
    witness: |value, input, property, expect, sink| {
        witness(Target::Book, value, input, property, expect, sink)
    },
    run_bytes: |bytes, sink, extra| run_bytes(Target::Book, bytes, sink, extra),
};

/// The session owner's history under the same pipeline. This target establishes
/// completed installation rules. `discovery/verification-capacity` runs the real
/// handle's asynchronous verification.
pub const STATE_ENTRY: HarnessEntry = HarnessEntry {
    name: Target::State.name(),
    generator: 2,
    scenario_bytes: 512,
    pad_scenario: false,
    composes: &[],
    planted: false,
    run: |value, input, sink| run(Target::State, value, input, sink),
    meta: |value| meta(Target::State, value),
    config: |value| config(Target::State, value),
    draw,
    properties: || registered(Target::State),
    witness: |value, input, property, expect, sink| {
        witness(Target::State, value, input, property, expect, sink)
    },
    run_bytes: |bytes, sink, extra| run_bytes(Target::State, bytes, sink, extra),
};
