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
