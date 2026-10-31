//! Transport-free execution of block policy and runtime funding models.

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
    Ownership,
    Funding,
    DecodedRetention,
}

impl Target {
    const fn name(self) -> &'static str {
        match self {
            Self::Ownership => "block/work-ownership",
            Self::Funding => "block/work-funding",
            Self::DecodedRetention => "block/decoded-retention",
        }
    }

    const fn identity(self) -> &'static str {
        match self {
            Self::Ownership => "block-work",
            Self::Funding => "block-work-funding",
            Self::DecodedRetention => "block-decoded-retention",
        }
    }

    fn history(self, input: &[u8]) -> Vec<(usize, MachineStep)> {
        match self {
            Self::Ownership => zakura_network::zakura::trace_work_ownership_bytes(input)
                .steps
                .into_iter()
                .map(|step| (step.step, MachineStep::Work(step.into())))
                .collect(),
            Self::Funding => zakura_network::zakura::trace_work_funding_bytes(input)
                .steps
                .into_iter()
                .map(|step| (step.step, MachineStep::WorkFunding(step.into())))
                .collect(),
            Self::DecodedRetention => {
                zakura_node_services::block_memory::model::trace_retention_bytes(input)
                    .into_iter()
                    .map(|step| (step.step, MachineStep::DecodedRetention(step.into())))
                    .collect()
            }
        }
    }
}

/// Whether `name` names a machine harness in this module.
pub fn is_machine(name: &str) -> bool {
    matches!(
        name,
        "block/work-ownership" | "block/work-funding" | "block/decoded-retention"
    )
}

/// Causal input for the selected harness's decoder-v1.
///
/// Each harness has its own decoder identity and consumes at most 512 bytes.
/// The runtime-owner targets observe an explicit drain after the final input.
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
            Target::Ownership => "The block work owner agrees with the independent ownership model after every input.",
            Target::Funding => "At each completed driver operation and before queue destruction, actual queue funding and retained request/receipt reservations agree with the independent model.",
            Target::DecodedRetention => "At each completed owner operation and before pool destruction, independently read block/header/transaction allocations and exact pool charges agree with the model's handle graph. Transfers and closed generations preserve final-holder settlement. This target excludes asynchronous backend-worker coverage.",
        },
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::View],
        status: Status::Active,
        check: match target {
            Target::Ownership => |cx| check(Target::Ownership, cx),
            Target::Funding => |cx| check(Target::Funding, cx),
            Target::DecodedRetention => |cx| check(Target::DecodedRetention, cx),
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
        (Target::Ownership, MachineStep::Work(work)) => (
            &work.failure,
            (work.actual != work.expected)
                .then(|| (format!("{:?}", work.expected), format!("{:?}", work.actual))),
        ),
        (Target::Funding, MachineStep::WorkFunding(work)) => (
            &work.failure,
            (work.actual != work.expected)
                .then(|| (format!("{:?}", work.expected), format!("{:?}", work.actual))),
        ),
        (Target::DecodedRetention, MachineStep::DecodedRetention(work)) => {
            let Some(actual) = &work.actual else {
                cx.missing_evidence(work.failure.as_ref().map_or_else(
                    || "decoded owner allocation capture is missing".to_owned(),
                    |failure| failure.message.clone(),
                ));
                return Verdict::NotApplicable;
            };
            (
                &work.failure,
                (actual != &work.expected)
                    .then(|| (format!("{:?}", work.expected), format!("{actual:?}"))),
            )
        }
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
            actual: format!("{:?}: {}", failure.phase, failure.message),
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

fn config(target: Target, value: &Value) -> Result<RunConfig, HarnessError> {
    let _ = scenario(value)?;
    Ok(RunConfig {
        budget: match target {
            Target::Ownership => 128,
            Target::Funding | Target::DecodedRetention => 129,
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
        sink,
        charged_work: false,
        drained: false,
    };
    let report = run(target, value, input, &mut evidence)
        .map_err(|error| WitnessError::HarnessEnd(error.to_string()))?;
    crate::witness::judge(&report, property, Status::Active, expect)?;
    if expect != Expect::Applicable && !evidence.charged_work {
        return Err(WitnessError::LostTriggerHistory);
    }
    if matches!(target, Target::Funding | Target::DecodedRetention)
        && expect == Expect::Holds
        && !evidence.drained
    {
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
    drained: bool,
}

impl RecordSink for WitnessSink<'_> {
    fn record(&mut self, record: Record) {
        if let Record::Step(step) = &record {
            match step.machine.as_deref() {
                Some(MachineStep::Work(work)) => {
                    self.charged_work |= work
                        .expected
                        .items
                        .iter()
                        .any(|item| item.in_flight && item.owner.is_some() && item.charge > 0);
                }
                Some(MachineStep::WorkFunding(work)) => {
                    self.charged_work |= work.expected.reserved > 0;
                    self.drained |=
                        work.drain && work.actual.reserved == 0 && work.expected.reserved == 0;
                }
                Some(MachineStep::DecodedRetention(work)) => {
                    self.charged_work |= work.expected.pools.iter().any(|pool| pool.bytes > 0);
                    self.drained |= work.drain
                        && work.actual.as_ref().is_some_and(|actual| {
                            actual.pools.iter().all(|pool| pool.bytes == 0)
                                && actual.allocations.iter().all(|allocation| {
                                    allocation.holders == 0
                                        && allocation.header_holders == 0
                                        && allocation
                                            .transaction_holders
                                            .iter()
                                            .all(|&count| count == 0)
                                })
                        })
                        && work.expected.pools.iter().all(|pool| pool.bytes == 0);
                }
                _ => {}
            }
        }
        self.sink.record(record);
    }
}

/// Pure policy uses the shared registry, recorder, replay verifier and explorer.
pub const ENTRY: HarnessEntry = HarnessEntry {
    name: Target::Ownership.name(),
    generator: 1,
    scenario_bytes: 512,
    pad_scenario: false,
    composes: &[],
    planted: false,
    run: |value, input, sink| run(Target::Ownership, value, input, sink),
    meta: |value| meta(Target::Ownership, value),
    config: |value| config(Target::Ownership, value),
    draw,
    properties: || registered(Target::Ownership),
    witness: |value, input, property, expect, sink| {
        witness(Target::Ownership, value, input, property, expect, sink)
    },
    run_bytes: |bytes, sink, extra| run_bytes(Target::Ownership, bytes, sink, extra),
};

/// Real queue owners use funding decoder-v1 and the same execution/evidence loop.
/// This target establishes completed-operation retention, not asynchronous races
/// or actual decoded-body/backend-worker lifetime.
pub const FUNDING_ENTRY: HarnessEntry = HarnessEntry {
    name: Target::Funding.name(),
    generator: 1,
    scenario_bytes: 512,
    pad_scenario: false,
    composes: &[],
    planted: false,
    run: |value, input, sink| run(Target::Funding, value, input, sink),
    meta: |value| meta(Target::Funding, value),
    config: |value| config(Target::Funding, value),
    draw,
    properties: || registered(Target::Funding),
    witness: |value, input, property, expect, sink| {
        witness(Target::Funding, value, input, property, expect, sink)
    },
    run_bytes: |bytes, sink, extra| run_bytes(Target::Funding, bytes, sink, extra),
};

/// Decoded owners share the registry and exact replay path, without QUIC.
pub const DECODED_RETENTION_ENTRY: HarnessEntry = HarnessEntry {
    name: Target::DecodedRetention.name(),
    generator: 1,
    scenario_bytes: 512,
    pad_scenario: false,
    composes: &[],
    planted: false,
    run: |value, input, sink| run(Target::DecodedRetention, value, input, sink),
    meta: |value| meta(Target::DecodedRetention, value),
    config: |value| config(Target::DecodedRetention, value),
    draw,
    properties: || registered(Target::DecodedRetention),
    witness: |value, input, property, expect, sink| {
        witness(
            Target::DecodedRetention,
            value,
            input,
            property,
            expect,
            sink,
        )
    },
    run_bytes: |bytes, sink, extra| run_bytes(Target::DecodedRetention, bytes, sink, extra),
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_allocation_capture_is_not_a_resource_violation() {
        let step = zakura_node_services::block_memory::model::trace_retention_bytes(&[0, 0, 0, 0])
            .remove(0);
        let mut view: crate::record::machine::DecodedRetentionStep = step.into();
        view.actual = None;
        let handle = Handle(RefCell::new(Some((1, MachineStep::DecodedRetention(view)))));
        let registered = properties(Target::DecodedRetention);
        let identity = Identity::machine(Target::DecodedRetention.identity(), 1);
        let log = ObservationLog::default();
        let step = StepCtx {
            step: 1,
            pick: &identity,
            enabled: &[],
            phase: Phase::Live,
            now: SimInstant::ZERO,
            status: SchedulerStatus::Quiescent,
            log: &log,
        };
        let mut cx = CheckCtx::new(&step, &handle, &registered[0], &registered);
        assert!(matches!(
            check(Target::DecodedRetention, &mut cx),
            Verdict::NotApplicable
        ));
        assert_eq!(
            cx.into_parts().missing.as_deref(),
            Some("decoded owner allocation capture is missing")
        );
    }
}
