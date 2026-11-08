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
    property::{HasView, Status, TriggerHistory},
    record::Record,
    MemorySink, ObservationLog, Outcome, PlanInput, PropertyOutcome, PropertyReport, RecordSink,
    RunEnd, RunReport,
};

/// What a witness test expects of its property.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Expect {
    /// The property applies and holds, and no property is violated: the
    /// scenario reaches the bug's trigger history, and the fix stops it.
    Holds,
    /// The property's run shows the violation, with its trigger history. A
    /// `Known` property that holds instead fails the test with
    /// [`WitnessError::Fixed`].
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
        &mut MemorySink::default(),
    )
}

/// [`witness`], writing the run's records to `sink`, so a recorded trace
/// is the run the test judged. Nothing is written if the property isn't
/// registered or has no trigger-history check.
pub fn witness_into<S: Subsystem>(
    scenario: &S::Scenario,
    input: PlanInput,
    property: &str,
    expect: Expect,
    sink: &mut dyn RecordSink,
) -> Result<(), WitnessError> {
    witness_prepared::<S>(
        scenario,
        S::prepare(scenario),
        input,
        property,
        expect,
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
    let registered = Registered::<S>::of(property, expect)?;
    let mut sink = EndSeen { sink, ended: false };
    // Judged once the run's end is final and before shutdown, which can
    // still change the view.
    let id = property.to_owned();
    let judge =
        move |harness: &S, end: &RunEnd, reports: &[PropertyReport], log: &ObservationLog| {
            registered.judge(end, reports, log, Some(harness), &id, expect)
        };
    let (_, judged) = match run_reading::<S, _>(scenario, prepared, input, &mut sink, None, judge) {
        Ok(run) => run,
        Err(HarnessError::Run { failure, .. }) => {
            return Err(WitnessError::HarnessEnd(failure.to_string()))
        }
        Err(error) => return Err(WitnessError::UnsupportedScenario(error.to_string())),
    };
    if !sink.ended {
        return Err(WitnessError::NoEnd);
    }
    judged
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
    Registered::<S>::of(property, expect)?.judge(
        &report.end,
        &report.properties,
        &report.log,
        None,
        property,
        expect,
    )
}

/// What a witness test reads from a property's registration.
struct Registered<S: HasView> {
    status: Status,
    trigger_history: Option<TriggerHistory<S>>,
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
            status: registered.status,
            trigger_history: registered.witness,
        })
    }

    /// Checks the run's `end` and `reports` against `expect`, then the
    /// trigger history, if the property has one, over the final `log` and,
    /// for a view trigger, the `harness`'s view. The view is read only once
    /// the report fits, so only on a run a check could read it in, and only
    /// if it shows the run's last step.
    fn judge(
        &self,
        end: &RunEnd,
        reports: &[PropertyReport],
        log: &ObservationLog,
        harness: Option<&S>,
        property: &str,
        expect: Expect,
    ) -> Result<(), WitnessError> {
        judge_end(end, reports, property, self.status, expect)?;
        acts_ran(end, log)?;
        let reached = match self.trigger_history {
            None => true,
            Some(TriggerHistory::Log(history)) => history(log),
            Some(TriggerHistory::View(history)) => match harness {
                Some(harness) if harness.captured().fresh_at(end.steps) => {
                    history(&harness.view(), log)
                }
                _ => return Err(WitnessError::NoView),
            },
        };
        if reached {
            Ok(())
        } else {
            Err(WitnessError::LostTriggerHistory)
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
}

/// Checks `property`'s report in `report` against `expect`, before its
/// trigger history.
pub(crate) fn judge(
    report: &RunReport,
    property: &str,
    status: Status,
    expect: Expect,
) -> Result<(), WitnessError> {
    judge_end(&report.end, &report.properties, property, status, expect)
}

/// [`judge`] on a run's `end` and property `reports`.
fn judge_end(
    end: &RunEnd,
    reports: &[PropertyReport],
    property: &str,
    status: Status,
    expect: Expect,
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
    // Another property's violation is unrelated to this one: one that
    // stopped the run fails every expectation, and one that didn't fails
    // all but `Applicable`.
    let unrelated = reports
        .iter()
        .filter(|r| r.property != property)
        .find(|r| matches!(r.outcome, PropertyOutcome::Violated { .. }));
    let stopped_by = end
        .violation
        .as_ref()
        .map(|v| v.property.as_str())
        .filter(|p| *p != property);
    match expect {
        Expect::Holds | Expect::Applicable => {
            if let Some(stopped_by) = stopped_by {
                return Err(WitnessError::Unrelated {
                    property: stopped_by.to_owned(),
                });
            }
            if let (Expect::Holds, Some(other)) = (expect, unrelated) {
                return Err(WitnessError::Unrelated {
                    property: other.property.clone(),
                });
            }
            match outcome {
                PropertyOutcome::Held { applicable } if *applicable > 0 => Ok(()),
                outcome => Err(WitnessError::Outcome(outcome.clone())),
            }
        }
        Expect::Violated => {
            if let Some(stopped_by) = stopped_by {
                return Err(WitnessError::Unrelated {
                    property: stopped_by.to_owned(),
                });
            }
            if let Some(other) = unrelated {
                return Err(WitnessError::Unrelated {
                    property: other.property.clone(),
                });
            }
            match (outcome, status) {
                (PropertyOutcome::Violated { .. }, _) => Ok(()),
                (PropertyOutcome::Held { .. }, Status::Known { issue }) => {
                    Err(WitnessError::Fixed { issue })
                }
                (outcome, _) => Err(WitnessError::Outcome(outcome.clone())),
            }
        }
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
                violation: Some(Violation {
                    property: property.to_owned(),
                    step: 3,
                    witness: vec![3],
                    expected: String::new(),
                    actual: String::new(),
                }),
                drain: None,
                harness: None,
            },
            properties: vec![PropertyReport::new(
                property,
                PropertyOutcome::Violated { witness: vec![3] },
            )],
            cleanup: Default::default(),
            log: Default::default(),
            picks: Vec::new(),
            resolved: Default::default(),
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
                judge(&report, "test/own", Status::Active, expect),
                Err(WitnessError::Outcome(PropertyOutcome::Violated {
                    witness: vec![3]
                })),
                "{expect:?}"
            );
        }
        assert_eq!(
            judge(&report, "test/own", Status::Active, Expect::Violated),
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
}
