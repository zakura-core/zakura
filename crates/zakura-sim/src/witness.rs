//! Witness tests (traces.md, Witness tests).
//!
//! A witness test runs one scenario through [`harness::run_harness`] and
//! checks one property's report against what the test expects. It passes
//! only on a complete run whose report fits, and whose trigger history,
//! the property's [`Property::witness`], shows the run reached the steps
//! the property's bug needs. A run that reaches the right verdict along a
//! harmless path proves nothing, so it fails.

use std::{fmt, str::FromStr};

use crate::{
    harness::{self, run_prepared, HarnessError, Subsystem},
    property::Status,
    record::Record,
    MemorySink, Outcome, PlanInput, PropertyOutcome, RecordSink, RunReport,
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
    /// may report violations, as long as none stopped the run.
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
    /// reached its trigger.
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
            WitnessError::NoEnd => f.write_str("the run wrote no end record"),
            WitnessError::MissingReport => f.write_str("the run doesn't report the property"),
            WitnessError::Outcome(outcome) => write!(f, "the property's outcome is {outcome:?}"),
            WitnessError::Unrelated { property } => write!(f, "{property} was violated"),
            WitnessError::LostTriggerHistory => {
                f.write_str("witness test lost its trigger history")
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
    let registered = harness::registry::<S>();
    let registered = registered
        .properties()
        .iter()
        .find(|p| p.id.as_str() == property)
        .ok_or(WitnessError::Unregistered)?;
    let trigger_history = registered.witness.ok_or(WitnessError::NoTriggerHistory)?;
    let status = registered.status;

    let mut sink = EndSeen { sink, ended: false };
    let report = match run_prepared::<S>(scenario, prepared, input, &mut sink, None) {
        Ok(report) => report,
        Err(HarnessError::Run { failure, .. }) => {
            return Err(WitnessError::HarnessEnd(failure.to_string()))
        }
        Err(error) => return Err(WitnessError::UnsupportedScenario(error.to_string())),
    };
    if !sink.ended {
        return Err(WitnessError::NoEnd);
    }
    judge(&report, property, status, expect)?;
    if trigger_history(&report.log) {
        Ok(())
    } else {
        Err(WitnessError::LostTriggerHistory)
    }
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
}

/// Checks `property`'s report in `report` against `expect`, before its
/// trigger history.
fn judge(
    report: &RunReport,
    property: &str,
    status: Status,
    expect: Expect,
) -> Result<(), WitnessError> {
    if report.end.outcome == Outcome::Harness {
        let failure = report
            .end
            .harness
            .as_ref()
            .map_or_else(String::new, ToString::to_string);
        return Err(WitnessError::HarnessEnd(failure));
    }
    let outcome = &report
        .properties
        .iter()
        .find(|r| r.property == property)
        .ok_or(WitnessError::MissingReport)?
        .outcome;
    // Another property's violation is unrelated to this one: one that
    // stopped the run fails every expectation, and one that didn't fails
    // all but `Applicable`.
    let unrelated = report
        .properties
        .iter()
        .filter(|r| r.property != property)
        .find(|r| matches!(r.outcome, PropertyOutcome::Violated { .. }));
    let stopped_by = report
        .end
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
}
