//! Witness tests (traces.md, Witness tests).
//!
//! A witness test runs one scenario through [`harness::run_harness`] and
//! checks one property's report against what the test expects. It passes
//! only on a complete run whose report fits, and whose trigger history,
//! the property's [`Property::witness`], shows the run reached the steps
//! the property's bug needs. A run that reaches the right verdict along a
//! harmless path proves nothing, so it fails. An `Applicable` witness
//! claims no bug, so it needs a trigger history only if the property has
//! one.

use std::{fmt, str::FromStr};

use crate::{
    harness::{self, run_reading, HarnessError, Subsystem},
    property::{HasView, PropertyId, Status, TriggerHistory},
    record::{FullReport, Record},
    trace::end_records::{self, TriggerState, WitnessRecord},
    MemorySink, NaReason, ObservationLog, Outcome, PlanInput, PropertyOutcome, PropertyReport,
    RecordSink, RunEnd, RunReport,
};

/// What a witness test expects of its property.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Expect {
    /// The property applies and holds, and no property is violated: the
    /// scenario reaches the bug's trigger history, and the fix stops it.
    Holds,
    /// The property's run shows the violation, with its trigger history. A
    /// `Known` property that holds instead fails the test with
    /// [`WitnessError::Fixed`]. No other property may be violated, except
    /// one the caller lists as an expected co-violation.
    Violated,
    /// The property applies and holds. Other `Known` or `Draft` properties
    /// may report violations, as long as none stopped the run. A property
    /// with no trigger-history check passes on its outcome alone.
    Applicable,
}

impl Expect {
    /// The expectation's name, as `zakura-sim record --expect` takes it.
    pub fn name(self) -> &'static str {
        match self {
            Expect::Holds => "holds",
            Expect::Violated => "violated",
            Expect::Applicable => "applicable",
        }
    }
}

impl FromStr for Expect {
    type Err = String;

    /// Reads [`Expect::name`].
    fn from_str(name: &str) -> Result<Expect, String> {
        [Expect::Holds, Expect::Violated, Expect::Applicable]
            .into_iter()
            .find(|expect| expect.name() == name)
            .ok_or_else(|| format!("`{name}` isn't holds, violated or applicable"))
    }
}

/// A witness test's scenario: the harness's scenario and the run's input.
pub struct WitnessScenario<S: Subsystem> {
    /// The harness's scenario.
    pub scenario: S::Scenario,
    /// The peer actions, storage outcomes and orderings, as fuzz bytes or a
    /// scripted [`crate::Plan`].
    pub input: PlanInput,
}

/// Why a witness test failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WitnessError {
    /// The harness doesn't register the property.
    Unregistered,
    /// The property has no trigger-history check, so no run can show it
    /// reached its trigger. Only `Applicable` does without one.
    NoTriggerHistory,
    /// The harness couldn't run the scenario.
    UnsupportedScenario(String),
    /// The run ended at a harness failure.
    HarnessEnd(String),
    /// The run wrote no `end` record.
    NoEnd,
    /// The run's report has no entry for the property.
    MissingReport,
    /// The run's report has no entry for another registered property, so
    /// the run's evidence is incomplete.
    Unreported {
        /// The registered property with no report.
        property: String,
    },
    /// Another property ended `NotApplicable` with a reason that isn't
    /// benign ([`NaReason::is_benign`]): the run's observations of it were
    /// incomplete, so the run can't show that nothing else broke.
    UnrelatedIncomplete {
        /// The property.
        property: String,
        /// Its reason, such as `assumptions-never-held`.
        reason: NaReason,
    },
    /// The property's outcome isn't the one the test expects.
    Outcome(PropertyOutcome),
    /// Another property was violated.
    Unrelated {
        /// The violated property.
        property: String,
    },
    /// The verdict fits, but the run didn't reach the property's trigger
    /// history.
    LostTriggerHistory,
    /// An adversary act the run scripted was skipped, so the witness's act
    /// never ran ([`crate::Observation::ActSkipped`]).
    ActSkipped {
        /// The act's name.
        act: String,
    },
    /// An adversary act the run scripted never ran
    /// ([`crate::Observation::ActRan`]): the run ended before the Plan
    /// picked it, or a violation stopped the run before any act ran.
    ActNotRun {
        /// The act's name.
        act: String,
    },
    /// The property's trigger history reads the harness's view, and the
    /// judge has no view showing the run's end, as when it has only the
    /// run's report.
    NoView,
    /// A reduction reached a different machine failure.
    ChangedFailure,
    /// A `Known` property that the test expects violated holds.
    Fixed {
        /// The open bug's issue.
        issue: u32,
    },
}

impl fmt::Display for WitnessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WitnessError::Unregistered => f.write_str("the harness doesn't register the property"),
            WitnessError::NoTriggerHistory => {
                f.write_str("the property has no trigger-history check")
            }
            WitnessError::UnsupportedScenario(error) => {
                write!(f, "the harness can't run the scenario: {error}")
            }
            WitnessError::HarnessEnd(failure) => {
                write!(f, "the run ended at a harness failure: {failure}")
            }
            WitnessError::ChangedFailure => f.write_str("the machine failure changed"),
            WitnessError::NoEnd => f.write_str("the run wrote no end record"),
            WitnessError::MissingReport => f.write_str("the run doesn't report the property"),
            WitnessError::Unreported { property } => {
                write!(f, "the run doesn't report {property}")
            }
            WitnessError::UnrelatedIncomplete { property, reason } => {
                write!(f, "{property}'s observations are incomplete: {reason}")
            }
            WitnessError::Outcome(outcome) => write!(f, "the property's outcome is {outcome:?}"),
            WitnessError::Unrelated { property } => write!(f, "{property} was violated"),
            WitnessError::LostTriggerHistory => {
                f.write_str("witness test lost its trigger history")
            }
            WitnessError::ActSkipped { act } => {
                write!(f, "the witness's act {act} was skipped and never ran")
            }
            WitnessError::ActNotRun { act } => {
                write!(f, "the witness's act {act} never ran")
            }
            WitnessError::NoView => {
                f.write_str("the trigger history reads the view, and no view shows the run's end")
            }
            WitnessError::Fixed { issue } => write!(
                f,
                "fixed? close the issue and set Active: the property holds, but #{issue} says it's violated"
            ),
        }
    }
}

impl std::error::Error for WitnessError {}

impl WitnessError {
    /// The error's name, as an end record writes it.
    pub fn kind(&self) -> &'static str {
        match self {
            WitnessError::Unregistered => "unregistered",
            WitnessError::NoTriggerHistory => "no-trigger-history",
            WitnessError::UnsupportedScenario(_) => "unsupported-scenario",
            WitnessError::HarnessEnd(_) => "harness-end",
            WitnessError::NoEnd => "no-end",
            WitnessError::MissingReport => "missing-report",
            WitnessError::Unreported { .. } => "unreported",
            WitnessError::UnrelatedIncomplete { .. } => "unrelated-incomplete",
            WitnessError::Outcome(PropertyOutcome::Held { .. }) => "held",
            WitnessError::Outcome(PropertyOutcome::NotApplicable { reason })
                if reason.is_benign() =>
            {
                "not-applicable"
            }
            WitnessError::Outcome(PropertyOutcome::NotApplicable { .. }) => "incomplete",
            WitnessError::Outcome(PropertyOutcome::Violated { .. }) => "violated",
            WitnessError::Unrelated { .. } => "unrelated",
            WitnessError::LostTriggerHistory => "lost-trigger-history",
            WitnessError::ActSkipped { .. } => "act-skipped",
            WitnessError::ActNotRun { .. } => "act-not-run",
            WitnessError::NoView => "no-view",
            WitnessError::ChangedFailure => "changed-failure",
            WitnessError::Fixed { .. } => "fixed",
        }
    }
}

/// Runs `scenario` and checks `property`'s report against `expect`.
///
/// # Panics
///
/// With [`WitnessError`]'s message, unless the report fits.
pub fn check_witness<S: Subsystem>(scenario: WitnessScenario<S>, property: &str, expect: Expect) {
    if let Err(error) = witness::<S>(scenario, property, expect) {
        panic!("{}: {property}: {error}", S::NAME);
    }
}

/// [`check_witness`], returning the reason instead of panicking.
pub fn witness<S: Subsystem>(
    scenario: WitnessScenario<S>,
    property: &str,
    expect: Expect,
) -> Result<(), WitnessError> {
    witness_into::<S>(
        &scenario.scenario,
        scenario.input,
        property,
        expect,
        &[],
        &mut MemorySink::default(),
    )
}

/// [`witness`], writing the run's records to `sink`, so a recorded trace
/// is the run the test judged. Nothing is written if the property isn't
/// registered or has no trigger-history check.
///
/// An [`Expect::Violated`] test also passes when properties in
/// `also_violates` are violated beside `property`: a bug that breaks
/// `property` can break them too.
pub fn witness_into<S: Subsystem>(
    scenario: &S::Scenario,
    input: PlanInput,
    property: &str,
    expect: Expect,
    also_violates: &[String],
    sink: &mut dyn RecordSink,
) -> Result<(), WitnessError> {
    witness_judged::<S>(
        scenario,
        S::prepare(scenario),
        input,
        property,
        expect,
        also_violates,
        sink,
    )
}

/// [`witness_into`] with state `prepared` by the caller, who may keep a
/// clone of it, such as a port handle, to inspect after the run.
pub fn witness_prepared<S: Subsystem>(
    scenario: &S::Scenario,
    prepared: S::Prepared,
    input: PlanInput,
    property: &str,
    expect: Expect,
    sink: &mut dyn RecordSink,
) -> Result<(), WitnessError> {
    witness_judged::<S>(scenario, prepared, input, property, expect, &[], sink)
}

/// [`witness_prepared`], with [`witness_into`]'s `also_violates`.
fn witness_judged<S: Subsystem>(
    scenario: &S::Scenario,
    prepared: S::Prepared,
    input: PlanInput,
    property: &str,
    expect: Expect,
    also_violates: &[String],
    sink: &mut dyn RecordSink,
) -> Result<(), WitnessError> {
    let (judged, trigger) = witness_run::<S>(
        scenario,
        prepared,
        input,
        property,
        expect,
        also_violates,
        sink,
    );
    write_witness(property, expect, &judged, trigger);
    judged
}

/// [`witness_judged`]'s run and judgement, with whether the run reached the
/// property's trigger history when the end-record hook asks for it.
fn witness_run<S: Subsystem>(
    scenario: &S::Scenario,
    prepared: S::Prepared,
    input: PlanInput,
    property: &str,
    expect: Expect,
    also_violates: &[String],
    sink: &mut dyn RecordSink,
) -> (Result<(), WitnessError>, TriggerState) {
    let registered = match Registered::<S>::of(property, expect) {
        Ok(registered) => registered,
        Err(error) => return (Err(error), TriggerState::None),
    };
    let mut sink = EndSeen { sink, ended: false };
    // Judged once the run's end is final and before shutdown, which can
    // still change the view.
    let id = property.to_owned();
    let also_violates = also_violates.to_vec();
    let hooked = end_records::dir().is_some();
    let judge =
        move |harness: &S, end: &RunEnd, reports: &[PropertyReport], log: &ObservationLog| {
            let judged = registered.judge(
                end,
                reports,
                log,
                Some(harness),
                &id,
                expect,
                &also_violates,
            );
            let trigger = if hooked {
                registered.trigger(end, log, Some(harness))
            } else {
                TriggerState::None
            };
            (judged, trigger)
        };
    let (_, (judged, trigger)) =
        match run_reading::<S, _>(scenario, prepared, input, &mut sink, None, judge) {
            Ok(run) => run,
            Err(HarnessError::Run { failure, .. }) => {
                return (
                    Err(WitnessError::HarnessEnd(failure.to_string())),
                    TriggerState::NoView,
                )
            }
            Err(error) => {
                return (
                    Err(WitnessError::UnsupportedScenario(error.to_string())),
                    TriggerState::None,
                )
            }
        };
    if !sink.ended {
        return (Err(WitnessError::NoEnd), trigger);
    }
    (judged, trigger)
}

/// Writes one witness judgement of `property` to the end-record hook, if
/// [`end_records::END_RECORDS_VAR`] names a directory.
fn write_witness(
    property: &str,
    expect: Expect,
    judged: &Result<(), WitnessError>,
    trigger_history: TriggerState,
) {
    end_records::write_witness(&WitnessRecord {
        property: property.to_owned(),
        expect: expect.name(),
        error: judged.as_ref().err().map(WitnessError::kind),
        trigger_history,
    });
}

/// Checks `report`, a complete run of `S`, against `expect` for
/// `property`, with the rule [`witness`] applies after its run. A test that
/// judges several properties on one run calls it once for each. A property
/// whose trigger history reads the view fails with [`WitnessError::NoView`].
pub fn judge_report<S: Subsystem>(
    report: &RunReport,
    property: &str,
    expect: Expect,
) -> Result<(), WitnessError> {
    let (judged, trigger) = match Registered::<S>::of(property, expect) {
        Err(error) => (Err(error), TriggerState::None),
        Ok(registered) => (
            registered.judge(
                &report.end,
                &report.properties,
                &report.log,
                None,
                property,
                expect,
                &[],
            ),
            registered.trigger(&report.end, &report.log, None),
        ),
    };
    write_witness(property, expect, &judged, trigger);
    judged
}

/// What a witness test reads from a property's registration.
struct Registered<S: HasView> {
    id: PropertyId,
    status: Status,
    trigger_history: Option<TriggerHistory<S>>,
    /// Every property the harness registers, which the run must report.
    every: Vec<String>,
}

impl<S: Subsystem> Registered<S> {
    /// `property`'s registration in `S`, if `expect` can judge it.
    fn of(property: &str, expect: Expect) -> Result<Self, WitnessError> {
        let registry = harness::registry::<S>();
        let registered = registry
            .properties()
            .iter()
            .find(|p| p.id.as_str() == property)
            .ok_or(WitnessError::Unregistered)?;
        if registered.witness.is_none() && expect != Expect::Applicable {
            return Err(WitnessError::NoTriggerHistory);
        }
        Ok(Registered {
            id: registered.id.clone(),
            status: registered.status,
            trigger_history: registered.witness,
            every: registry
                .properties()
                .iter()
                .map(|property| property.id.to_string())
                .collect(),
        })
    }

    /// Checks the run's `end` and `reports` against `expect`, allowing
    /// `also_violates` beside a violation, then the trigger history, if the
    /// property has one, over the final `log` and, for a view trigger, the
    /// `harness`'s view. The view is read only once the report fits, so
    /// only on a run a check could read it in, and only if it shows the
    /// run's last step.
    #[allow(clippy::too_many_arguments)]
    fn judge(
        &self,
        end: &RunEnd,
        reports: &[PropertyReport],
        log: &ObservationLog,
        harness: Option<&S>,
        property: &str,
        expect: Expect,
        also_violates: &[String],
    ) -> Result<(), WitnessError> {
        judge_end(end, reports, property, self.status, expect, also_violates)?;
        acts_ran(end, log)?;
        if let Some(missing) = self
            .every
            .iter()
            .find(|id| !reports.iter().any(|report| &report.property == *id))
        {
            return Err(WitnessError::Unreported {
                property: missing.clone(),
            });
        }
        match self.trigger(end, log, harness) {
            TriggerState::None | TriggerState::Reached => Ok(()),
            TriggerState::Lost => Err(WitnessError::LostTriggerHistory),
            TriggerState::NoView => Err(WitnessError::NoView),
        }
    }

    /// Whether the run that ended at `end` with `log` reached the
    /// property's trigger history. A view trigger reads `harness`'s view
    /// only if the run ended without a harness failure and the view shows
    /// its last step.
    fn trigger(&self, end: &RunEnd, log: &ObservationLog, harness: Option<&S>) -> TriggerState {
        let reached = |reached: bool| {
            if reached {
                TriggerState::Reached
            } else {
                TriggerState::Lost
            }
        };
        match self.trigger_history {
            None => TriggerState::None,
            Some(TriggerHistory::Log(history)) => reached(history(log)),
            Some(TriggerHistory::LogOf(history)) => reached(history(&self.id, log)),
            Some(TriggerHistory::View(history)) => match harness {
                Some(harness)
                    if end.outcome != Outcome::Harness
                        && harness.captured().fresh_at(end.steps) =>
                {
                    reached(history(&harness.view(), log))
                }
                _ => TriggerState::NoView,
            },
        }
    }
}

/// Requires the run's scripted adversary acts to have run, which means
/// each wrote its frames. None may be skipped. A run that came to its end
/// ran every one. A run a violation stopped ran at least one by the
/// violation's step, since its later acts may never have been offered.
fn acts_ran(end: &RunEnd, log: &ObservationLog) -> Result<(), WitnessError> {
    use crate::Observation;
    let mut scripted: Vec<&str> = Vec::new();
    let mut ran: Vec<(u64, &str)> = Vec::new();
    for entry in log.entries() {
        match &entry.obs {
            Observation::ActSkipped { act, .. } => {
                return Err(WitnessError::ActSkipped { act: act.clone() })
            }
            Observation::ActsScripted { acts } => scripted.extend(acts.iter().map(String::as_str)),
            Observation::ActRan { act, .. } => ran.push((entry.step, act)),
            _ => {}
        }
    }
    let Some(first) = scripted.first() else {
        return Ok(());
    };
    if let Some(violation) = &end.violation {
        return if ran.iter().any(|(step, _)| *step <= violation.step) {
            Ok(())
        } else {
            Err(WitnessError::ActNotRun {
                act: (*first).to_owned(),
            })
        };
    }
    for act in scripted {
        match ran.iter().position(|(_, ran)| *ran == act) {
            Some(at) => {
                ran.swap_remove(at);
            }
            None => {
                return Err(WitnessError::ActNotRun {
                    act: act.to_owned(),
                })
            }
        }
    }
    Ok(())
}

/// A sink that notes whether the last record it passed on was the `end`.
struct EndSeen<'a> {
    sink: &'a mut dyn RecordSink,
    ended: bool,
}

impl RecordSink for EndSeen<'_> {
    fn record(&mut self, record: Record) {
        self.ended = matches!(record, Record::End(_));
        self.sink.record(record);
    }

    /// Forwards to the sink.
    fn wants_steps(&self) -> bool {
        self.sink.wants_steps()
    }

    /// Forwards to the sink.
    fn report(&mut self, report: &FullReport) {
        self.sink.report(report);
    }
}

/// Checks `property`'s report in `report` against `expect`, before its
/// trigger history.
pub(crate) fn judge(
    report: &RunReport,
    property: &str,
    status: Status,
    expect: Expect,
    also_violates: &[String],
) -> Result<(), WitnessError> {
    judge_end(
        &report.end,
        &report.properties,
        property,
        status,
        expect,
        also_violates,
    )
}

/// [`judge`] on a run's `end` and property `reports`. Under
/// [`Expect::Violated`], a violation of a property in `also_violates`
/// isn't unrelated. Under [`Expect::Holds`] and a violated expectation, another
/// property the run left with incomplete observations, a `NotApplicable`
/// reason that isn't benign, fails the test too: the run can't show nothing
/// else broke. The one exception is `StoppedByViolation` in a run the
/// expected violation stopped.
fn judge_end(
    end: &RunEnd,
    reports: &[PropertyReport],
    property: &str,
    status: Status,
    expect: Expect,
    also_violates: &[String],
) -> Result<(), WitnessError> {
    if end.outcome == Outcome::Harness {
        let failure = end
            .harness
            .as_ref()
            .map_or_else(String::new, ToString::to_string);
        return Err(WitnessError::HarnessEnd(failure));
    }
    let outcome = &reports
        .iter()
        .find(|r| r.property == property)
        .ok_or(WitnessError::MissingReport)?
        .outcome;
    let violated = expect == Expect::Violated;
    // Another property's violation is unrelated to this one: one that
    // stopped the run fails every expectation, and one that didn't fails
    // all but `Applicable`. A `Violated` test expects `also_violates`.
    let expected =
        |other: &str| other == property || (violated && also_violates.iter().any(|a| a == other));
    let unrelated = reports
        .iter()
        .filter(|r| !expected(&r.property))
        .find(|r| matches!(r.outcome, PropertyOutcome::Violated { .. }));
    let stopped_by = end
        .violation
        .as_ref()
        .map(|v| v.property.as_str())
        .filter(|p| !expected(p));
    if let Some(stopped_by) = stopped_by {
        return Err(WitnessError::Unrelated {
            property: stopped_by.to_owned(),
        });
    }
    if let (true, Some(other)) = (expect != Expect::Applicable, unrelated) {
        return Err(WitnessError::Unrelated {
            property: other.property.clone(),
        });
    }
    // A run its expected violation stopped can leave another property's
    // obligation open through no fault of its own, which is what
    // `StoppedByViolation` says. Any unexpected stop failed above. A run
    // that went on to its end, as a `Draft` run does, leaves no such reason.
    let stopped = end.outcome == Outcome::Violation;
    if expect == Expect::Holds || violated {
        let incomplete = reports
            .iter()
            .filter(|r| !expected(&r.property))
            .find_map(|r| match r.outcome {
                PropertyOutcome::NotApplicable { reason }
                    if !reason.is_benign()
                        && !(stopped && reason == NaReason::StoppedByViolation) =>
                {
                    Some((&r.property, reason))
                }
                _ => None,
            });
        if let Some((other, reason)) = incomplete {
            return Err(WitnessError::UnrelatedIncomplete {
                property: other.clone(),
                reason,
            });
        }
    }
    match expect {
        Expect::Holds | Expect::Applicable => match outcome {
            PropertyOutcome::Held { applicable } if *applicable > 0 => Ok(()),
            outcome => Err(WitnessError::Outcome(outcome.clone())),
        },
        Expect::Violated => match (outcome, status) {
            (PropertyOutcome::Violated { .. }, _) => Ok(()),
            (PropertyOutcome::Held { .. }, Status::Known { issue }) => {
                Err(WitnessError::Fixed { issue })
            }
            (outcome, _) => Err(WitnessError::Outcome(outcome.clone())),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{check::PropertyReport, RunEnd, Violation};

    /// A report whose run `property` stopped with a violation.
    fn stopped_by(property: &str) -> RunReport {
        RunReport {
            end: RunEnd {
                outcome: Outcome::Violation,
                steps: 3,
                violation: Some(violation(property, 3, &[3])),
                drain: None,
                harness: None,
                rest: None,
            },
            properties: vec![violated_report(property, &[(3, &[3])])],
            cleanup: Default::default(),
            log: Default::default(),
            picks: Vec::new(),
            resolved: Default::default(),
        }
    }

    /// `property`'s violation at `step`, depending on `witness`.
    fn violation(property: &str, step: u64, witness: &[u64]) -> Violation {
        Violation {
            property: property.to_owned(),
            step,
            witness: witness.to_vec(),
            expected: String::new(),
            actual: format!("a break at step {step}"),
        }
    }

    /// A report of `property` violated once per `(step, witness)`.
    fn violated_report(property: &str, found: &[(u64, &[u64])]) -> PropertyReport {
        PropertyReport {
            violations: found
                .iter()
                .map(|(step, witness)| violation(property, *step, witness))
                .collect(),
            ..PropertyReport::new(
                property,
                PropertyOutcome::Violated {
                    witness: found[0].1.to_vec(),
                },
            )
        }
    }

    /// A run the property itself stopped fails `Holds` and `Applicable`
    /// with the property's own outcome, not as an unrelated violation, and
    /// passes `Violated`.
    #[test]
    fn own_violation_is_the_outcome() {
        let report = stopped_by("test/own");
        for expect in [Expect::Holds, Expect::Applicable] {
            assert_eq!(
                judge(&report, "test/own", Status::Active, expect, &[]),
                Err(WitnessError::Outcome(PropertyOutcome::Violated {
                    witness: vec![3]
                })),
                "{expect:?}"
            );
        }
        assert_eq!(
            judge(&report, "test/own", Status::Active, Expect::Violated, &[]),
            Ok(())
        );
    }

    /// A scripted act fails the witness unless it ran: skipped, or never
    /// picked by a run that came to its end. A run a violation stopped
    /// needs only an act that ran by then.
    #[test]
    fn a_witness_needs_its_acts_to_run() {
        use crate::Observation;
        let ran = |act: &str| Observation::ActRan {
            connection: 0,
            quic_stream: 0,
            act: act.to_owned(),
        };
        let scripted = Observation::ActsScripted {
            acts: vec!["after-close".to_owned(), "over-live".to_owned()],
        };
        let finished = stopped_by("test/own").end;
        let finished = RunEnd {
            outcome: Outcome::Budget,
            violation: None,
            ..finished
        };
        let stopped = stopped_by("test/own").end;

        let mut log = ObservationLog::default();
        assert_eq!(acts_ran(&finished, &log), Ok(()), "no acts scripted");
        log.push(scripted);
        let not_run = |act: &str| {
            Err(WitnessError::ActNotRun {
                act: act.to_owned(),
            })
        };
        assert_eq!(acts_ran(&finished, &log), not_run("after-close"));
        assert_eq!(acts_ran(&stopped, &log), not_run("after-close"));
        log.push(ran("after-close"));
        assert_eq!(acts_ran(&finished, &log), not_run("over-live"));
        assert_eq!(acts_ran(&stopped, &log), Ok(()));
        log.push(ran("over-live"));
        assert_eq!(acts_ran(&finished, &log), Ok(()));
        log.push(Observation::ActSkipped {
            connection: 0,
            quic_stream: 0,
            act: "over-live".to_owned(),
        });
        let skipped = Err(WitnessError::ActSkipped {
            act: "over-live".to_owned(),
        });
        assert_eq!(acts_ran(&finished, &log), skipped);
        assert_eq!(acts_ran(&stopped, &log), skipped);
    }

    /// A `Violated` test passes with a listed co-violation beside the
    /// property's own, fails on an unlisted one, and still needs the
    /// property's own violation. Other expectations ignore the list.
    #[test]
    fn listed_co_violations_pass_violated_only() {
        let mut report = stopped_by("test/own");
        report.properties.push(PropertyReport::new(
            "test/co",
            PropertyOutcome::Violated { witness: vec![3] },
        ));
        let also = ["test/co".to_owned()];
        let judged = |report: &RunReport, also: &[String], expect| {
            judge(report, "test/own", Status::Active, expect, also)
        };
        assert_eq!(judged(&report, &also, Expect::Violated), Ok(()));
        let unrelated = Err(WitnessError::Unrelated {
            property: "test/co".to_owned(),
        });
        assert_eq!(judged(&report, &[], Expect::Violated), unrelated);
        // The listed property stopped the run, and the own one held.
        let mut held = stopped_by("test/co");
        held.properties.push(PropertyReport::new(
            "test/own",
            PropertyOutcome::Held { applicable: 1 },
        ));
        assert_eq!(
            judged(&held, &also, Expect::Violated),
            Err(WitnessError::Outcome(PropertyOutcome::Held {
                applicable: 1
            }))
        );
        assert_eq!(judged(&held, &also, Expect::Holds), unrelated);
    }

    /// A violated expectation tolerates another property's
    /// `StoppedByViolation` only in a run its violation stopped. A run that
    /// went on to its end, as a `Draft` run does, must leave nothing else
    /// incomplete, and a stopped run any other incomplete reason.
    #[test]
    fn an_expected_stop_tolerates_only_stopped_by_violation() {
        let mut report = stopped_by("test/own");
        report.properties.push(PropertyReport::new(
            "test/open",
            PropertyOutcome::not_applicable(NaReason::StoppedByViolation),
        ));
        assert_eq!(
            judge(&report, "test/own", Status::Active, Expect::Violated, &[]),
            Ok(())
        );
        let mut never = stopped_by("test/own");
        never.properties.push(PropertyReport::new(
            "test/open",
            PropertyOutcome::not_applicable(NaReason::AssumptionsNeverHeld),
        ));
        assert_eq!(
            judge(&never, "test/own", Status::Active, Expect::Violated, &[]),
            Err(WitnessError::UnrelatedIncomplete {
                property: "test/open".to_owned(),
                reason: NaReason::AssumptionsNeverHeld,
            })
        );
        report.end.outcome = Outcome::Budget;
        report.end.violation = None;
        assert_eq!(
            judge(&report, "test/own", Status::Draft, Expect::Violated, &[]),
            Err(WitnessError::UnrelatedIncomplete {
                property: "test/open".to_owned(),
                reason: NaReason::StoppedByViolation,
            })
        );
    }

    /// E2: a held target fails beside another property whose observations
    /// were incomplete, such as one whose assumptions never held or whose
    /// lost assumption withdrew its only obligation, and passes beside a
    /// benign `NotApplicable`.
    #[test]
    fn a_held_target_refuses_an_unrelated_incomplete() {
        let held = |other: NaReason| {
            let mut report = stopped_by("test/own");
            report.end.outcome = Outcome::Budget;
            report.end.violation = None;
            report.properties = vec![
                PropertyReport::new("test/own", PropertyOutcome::Held { applicable: 1 }),
                PropertyReport::new("test/other", PropertyOutcome::not_applicable(other)),
            ];
            judge(&report, "test/own", Status::Active, Expect::Holds, &[])
        };
        for reason in [NaReason::AssumptionsNeverHeld, NaReason::AssumptionLost] {
            assert_eq!(
                held(reason),
                Err(WitnessError::UnrelatedIncomplete {
                    property: "test/other".to_owned(),
                    reason,
                }),
                "{reason:?}"
            );
        }
        for reason in [
            NaReason::NeverApplied,
            NaReason::Undeclared,
            NaReason::Excluded,
        ] {
            assert_eq!(held(reason), Ok(()), "{reason:?}");
        }
    }
}
