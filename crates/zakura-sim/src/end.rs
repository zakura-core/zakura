//! How a run ends.

use std::fmt;

use crate::{check::Violation, PlanError};
use serde::{Deserialize, Serialize};

/// A failure of the simulation itself, not of the code under test.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum HarnessFailure {
    /// A task name contains `:`, which identities use as a separator.
    TaskNameHasColon {
        /// The name.
        name: String,
    },
    /// Two enabled actions share an identity.
    DuplicateIdentity {
        /// The shared identity.
        identity: String,
    },
    /// A scripted plan couldn't be followed.
    Plan(PlanError),
    /// A task drew while the run's `Plan` was borrowed.
    PlanBorrowedDuringDraw,
    /// A draw came from a task other than the one the step runs.
    DrawFromOtherTask {
        /// The drawing task's index, or `None` for code outside any task.
        found: Option<usize>,
    },
    /// A draw of a kind this crate has no name for.
    UnknownDrawKind,
    /// A checker, or a source's `enabled` or `boundaries`, drew between
    /// steps. That code is harness code, so it must not consume the plan.
    DrawOutsideStep,
    /// Nothing is enabled, yet the scheduler says the run can go on, as when a
    /// source's deadline blocks time while it offers no action.
    StuckWithoutAction {
        /// The scheduler's status.
        status: &'static str,
    },
    /// A violation, or a property report at the run's end, names a step the
    /// run didn't take as its step or evidence.
    EvidenceOutsideRun {
        /// The violated property.
        property: String,
        /// The step it names.
        step: u64,
    },
    /// A property report's `holds_since` is past the run's last step.
    HoldsSinceOutsideRun {
        /// The property.
        property: String,
        /// The step it names.
        step: u64,
    },
    /// A property needs a source that the harness neither observes nor
    /// excludes.
    MissingSource {
        /// The property.
        property: String,
        /// The source, such as `Frames`.
        source: String,
    },
    /// A property's check found its declared source missing evidence it
    /// requires.
    MissingEvidence {
        /// The property.
        property: String,
        /// The step whose boundary the check ran at.
        step: u64,
        /// What was missing.
        detail: String,
    },
    /// A property that reads the view would have read state captured before
    /// the step's boundary.
    StaleView {
        /// The property.
        property: String,
        /// The step whose boundary the check needed.
        step: u64,
        /// The step whose boundary the view showed, or `None` if it had no
        /// snapshot.
        captured: Option<u64>,
    },
    /// A source's evidence was incomplete: its facts disagreed with its
    /// state, didn't reach the log, or it skipped a boundary.
    SourceEvidence {
        /// The source: `source <index>` for one whose `observe` failed, or
        /// the name of a checkpoint the run holds, such as `storage:header`.
        source: String,
        /// What was wrong, led by the failure's name, such as
        /// `BoundarySkipped`.
        detail: String,
    },
}

impl fmt::Display for HarnessFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HarnessFailure::TaskNameHasColon { name } => {
                write!(f, "task name `{name}` contains ':'")
            }
            HarnessFailure::DuplicateIdentity { identity } => {
                write!(f, "two enabled actions are both {identity}")
            }
            HarnessFailure::Plan(error) => error.fmt(f),
            HarnessFailure::PlanBorrowedDuringDraw => {
                f.write_str("a task drew while the plan was borrowed")
            }
            HarnessFailure::DrawFromOtherTask { found: Some(task) } => {
                write!(f, "task {task} drew during another task's step")
            }
            HarnessFailure::DrawFromOtherTask { found: None } => {
                f.write_str("code outside any task drew during a task's step")
            }
            HarnessFailure::UnknownDrawKind => f.write_str("a draw of an unknown kind"),
            HarnessFailure::DrawOutsideStep => {
                f.write_str("a checker or a source's enabled set drew between steps")
            }
            HarnessFailure::StuckWithoutAction { status } => {
                write!(f, "nothing is enabled, but the scheduler is {status}")
            }
            HarnessFailure::EvidenceOutsideRun { property, step } => {
                write!(f, "{property} names step {step}, which the run didn't take")
            }
            HarnessFailure::HoldsSinceOutsideRun { property, step } => {
                write!(
                    f,
                    "{property} holds since step {step}, which the run didn't take"
                )
            }
            HarnessFailure::MissingSource { property, source } => write!(
                f,
                "{property} needs {source}, which the harness neither observes nor excludes"
            ),
            HarnessFailure::MissingEvidence {
                property,
                step,
                detail,
            } => write!(f, "{property} lacks evidence after step {step}: {detail}"),
            HarnessFailure::StaleView {
                property,
                step,
                captured: Some(captured),
            } => write!(
                f,
                "{property} needs the view after step {step}, but it shows step {captured}"
            ),
            HarnessFailure::StaleView {
                property,
                step,
                captured: None,
            } => write!(
                f,
                "{property} needs the view after step {step}, but it has no snapshot"
            ),
            HarnessFailure::SourceEvidence { source, detail } => {
                write!(f, "{source}'s evidence failed: {detail}")
            }
        }
    }
}

/// Why nothing was enabled at the end of a live run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IdleStatus {
    /// Every task finished.
    Quiescent,
    /// No task can ever run again.
    Deadlock,
    /// A task is ready, but parked tasks hold every worker.
    WorkerStarved,
    /// Tasks wait on a source outside the scheduler.
    WaitingOnExternal,
}

/// How a run ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// A property was violated.
    Violation,
    /// The step budget ran out, and the drain followed.
    Budget,
    /// Nothing was enabled.
    Idle {
        /// What the scheduler reported.
        status: IdleStatus,
    },
    /// The simulation itself failed.
    Harness,
}

/// What the drain after a budget did.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Drain {
    /// Steps the drain took.
    pub steps: u64,
    /// True if it stopped at its bound with tasks still ready.
    pub exhausted: bool,
}

/// The end of a run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunEnd {
    /// How it ended.
    pub outcome: Outcome,
    /// Every step, drain included.
    pub steps: u64,
    /// The violation, for [`Outcome::Violation`].
    pub violation: Option<Violation>,
    /// The drain, if one started.
    pub drain: Option<Drain>,
    /// The failure, for [`Outcome::Harness`].
    pub harness: Option<HarnessFailure>,
}
