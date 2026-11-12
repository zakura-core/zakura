//! The one judge of a simulation run that counts as sim-7 acceptance
//! evidence (the sim-7 plan's Rule 10), and the one table that says what
//! each cleanup count means.

use std::{collections::BTreeSet, fmt};

use crate::{
    check::{NaReason, PropertyOutcome},
    record::{CleanupRecord, EndRecord, OutcomeKind},
};

/// What a run that counts as evidence is expected to end as.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunExpect {
    /// It ends without a violation or a harness failure, at its budget or
    /// idle, as a seed with no witness or a passing history does.
    Passes,
    /// It ends at a violation of `property`. `also` lists the other
    /// properties it may violate too, as a nested case's exact set does.
    Violates {
        /// The property whose violation ends the run.
        property: String,
        /// Other properties allowed to report a violation.
        also: Vec<String>,
    },
    /// It ends at a harness failure carrying exactly `diagnostic`, as an
    /// integrity mutation's patched run does.
    HarnessFails {
        /// The failure's text, as the end record holds it.
        diagnostic: String,
    },
}

/// The properties a run's harness registers, as the judge needs them.
#[derive(Clone, Debug, Default)]
pub struct Registered {
    /// Every registered property's ID.
    pub ids: BTreeSet<String>,
    /// The ones that are `Known` or `Draft`, whose violations a run may
    /// report without failing, as long as none stopped it.
    pub tolerated: BTreeSet<String>,
}

/// Why [`owned_run_passes`] blocked a run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Blocked {
    /// The run didn't end as expected.
    Outcome {
        /// What was expected, in words.
        expected: String,
        /// What the end record says.
        actual: String,
    },
    /// A registered property has no report.
    Unreported(String),
    /// A property reported a violation the expectation doesn't allow.
    UnexpectedViolation(String),
    /// A property's `NotApplicable` reason says the run's evidence was
    /// incomplete, and the expectation doesn't allow it.
    Incomplete {
        /// The property.
        property: String,
        /// Its reason.
        reason: String,
    },
    /// A cleanup count [`CLEANUP`] classifies as a fault is nonzero.
    Cleanup {
        /// The field.
        field: &'static str,
        /// Its count.
        count: usize,
    },
}

impl fmt::Display for Blocked {
    /// The reason in words, as the record shows it.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Blocked::Outcome { expected, actual } => {
                write!(
                    f,
                    "expected the run to end {expected}, but it ended {actual}"
                )
            }
            Blocked::Unreported(property) => write!(f, "{property} has no report"),
            Blocked::UnexpectedViolation(property) => {
                write!(f, "{property} reported a violation the run doesn't expect")
            }
            Blocked::Incomplete { property, reason } => {
                write!(
                    f,
                    "{property} is not-applicable for {reason}: incomplete evidence"
                )
            }
            Blocked::Cleanup { field, count } => write!(f, "cleanup `{field}` is {count}"),
        }
    }
}

/// What a nonzero cleanup count means.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CleanupMeaning {
    /// Something went wrong in teardown: the run blocks.
    Fault,
    /// Ordinary teardown of a run that stopped with work in flight.
    Teardown,
    /// A count taken as shutdown began, not a fault.
    NotAFault,
}

/// Each [`CleanupRecord`] field, once, with what a nonzero count means.
/// [`cleanup_counts`] destructures the record, so a new field can't be
/// left out of this table.
pub const CLEANUP: [(&str, CleanupMeaning); 6] = [
    // Cancelling an unfinished task at a poll boundary is how shutdown ends one.
    ("cancelled", CleanupMeaning::Teardown),
    // A task that panics while shutdown drops it can leave `leaked == 0`.
    ("panicked", CleanupMeaning::Fault),
    // Force-unwinding a parked task, such as one in a lock cycle, is teardown.
    ("unwound", CleanupMeaning::Teardown),
    // Task code that catches a forced unwind keeps running past its cancel.
    ("unwind_failures", CleanupMeaning::Fault),
    // Tasks stuck in cleanup, leaked without running their destructors.
    ("leaked", CleanupMeaning::Fault),
    // Timers counted as shutdown began.
    ("timers_left", CleanupMeaning::NotAFault),
];

/// `cleanup`'s counts, by [`CLEANUP`]'s field names, in its order.
pub fn cleanup_counts(cleanup: &CleanupRecord) -> [(&'static str, usize); 6] {
    // Destructured, so a new field fails to compile until CLEANUP has it.
    let CleanupRecord {
        cancelled,
        panicked,
        unwound,
        unwind_failures,
        leaked,
        timers_left,
    } = *cleanup;
    [
        ("cancelled", cancelled),
        ("panicked", panicked),
        ("unwound", unwound),
        ("unwind_failures", unwind_failures),
        ("leaked", leaked),
        ("timers_left", timers_left),
    ]
}

/// Judges one run that counts as acceptance evidence from its end record:
/// it passes only if it ended as `expect` says, every property in
/// `registered` reported, no report is a violation the expectation
/// doesn't allow, no report is an incomplete-evidence `NotApplicable` the
/// expectation doesn't allow, and no cleanup count [`CLEANUP`] calls a
/// fault is nonzero.
///
/// Two incomplete reasons are allowed only where the run's own expected
/// end causes them: `StoppedByViolation` on a run its expected violation
/// stopped, and `HarnessFailed` on a run expected to end at that harness
/// failure. Neither credits the property it's on.
pub fn owned_run_passes(
    end: &EndRecord,
    registered: &Registered,
    expect: &RunExpect,
) -> Result<(), Blocked> {
    ended_as(end, expect)?;
    let reported: BTreeSet<&str> = end.properties.iter().map(|r| r.property.as_str()).collect();
    if let Some(missing) = registered
        .ids
        .iter()
        .find(|id| !reported.contains(id.as_str()))
    {
        return Err(Blocked::Unreported(missing.clone()));
    }
    let allowed: BTreeSet<&str> = match expect {
        RunExpect::Violates { property, also } => std::iter::once(property.as_str())
            .chain(also.iter().map(String::as_str))
            .collect(),
        RunExpect::Passes | RunExpect::HarnessFails { .. } => BTreeSet::new(),
    };
    for report in &end.properties {
        let property = report.property.as_str();
        match &report.outcome {
            PropertyOutcome::Held { .. } => {}
            PropertyOutcome::Violated { .. } => {
                if !allowed.contains(property) && !registered.tolerated.contains(property) {
                    return Err(Blocked::UnexpectedViolation(property.to_owned()));
                }
            }
            PropertyOutcome::NotApplicable { reason } => {
                let caused_by_expected_end = matches!(
                    (reason, expect),
                    (NaReason::StoppedByViolation, RunExpect::Violates { .. })
                        | (NaReason::HarnessFailed, RunExpect::HarnessFails { .. })
                );
                if !reason.is_benign() && !caused_by_expected_end {
                    return Err(Blocked::Incomplete {
                        property: property.to_owned(),
                        reason: reason.to_string(),
                    });
                }
            }
        }
    }
    for ((field, count), (_, meaning)) in cleanup_counts(&end.cleanup).into_iter().zip(CLEANUP) {
        if count > 0 && meaning == CleanupMeaning::Fault {
            return Err(Blocked::Cleanup { field, count });
        }
    }
    Ok(())
}

/// Whether `end` ended as `expect` says.
fn ended_as(end: &EndRecord, expect: &RunExpect) -> Result<(), Blocked> {
    let actual = || match end.outcome {
        OutcomeKind::Violation => format!(
            "at a violation of {}",
            end.violation
                .as_ref()
                .map_or("an unnamed property", |v| v.property.as_str())
        ),
        OutcomeKind::Harness => format!(
            "at the harness failure `{}`",
            end.harness.as_deref().unwrap_or("")
        ),
        OutcomeKind::Budget => "at its budget".to_owned(),
        OutcomeKind::Idle => "idle".to_owned(),
    };
    let (ok, expected) = match expect {
        RunExpect::Passes => (
            matches!(end.outcome, OutcomeKind::Budget | OutcomeKind::Idle),
            "without a violation or harness failure".to_owned(),
        ),
        RunExpect::Violates { property, .. } => (
            end.outcome == OutcomeKind::Violation
                && end
                    .violation
                    .as_ref()
                    .is_some_and(|v| &v.property == property),
            format!("at a violation of {property}"),
        ),
        RunExpect::HarnessFails { diagnostic } => (
            end.outcome == OutcomeKind::Harness && end.harness.as_deref() == Some(diagnostic),
            format!("at the harness failure `{diagnostic}`"),
        ),
    };
    if ok {
        Ok(())
    } else {
        Err(Blocked::Outcome {
            expected,
            actual: actual(),
        })
    }
}
