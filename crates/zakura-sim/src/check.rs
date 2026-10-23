//! Property checks a run calls after each step and at its end.
//!
//! properties.md's registry (P1) builds on these hooks. M1 ships
//! [`NoChecks`] and [`FnChecker`] for tests.

use serde::{Deserialize, Serialize};
use zakura_scheduler::{SimInstant, Status};

use crate::{end::RunEnd, observe::Observation, Enabled, Identity, ObservationLog};

/// A property that failed, and the steps its failure depends on.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Violation {
    /// The property's ID, such as `sim/no-livelock`.
    pub property: String,
    /// The step the check failed at.
    pub step: u64,
    /// The steps the violation depends on, in order.
    pub witness: Vec<u64>,
    /// What the property expected.
    pub expected: String,
    /// What the run did.
    pub actual: String,
}

/// One property's outcome over a whole run (properties.md, Verdicts and
/// obligations).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum PropertyOutcome {
    /// `applicable` evaluations applied, none failed, and no obligation is
    /// open.
    Held {
        /// How many evaluations applied.
        applicable: u32,
    },
    /// No evaluation applied.
    NotApplicable,
    /// An obligation was still open at run end, within its bound, or the
    /// property's assumptions never held.
    Inconclusive {
        /// Which of the two.
        reason: String,
    },
    /// Some evaluation failed, or an obligation's bound expired.
    Violated {
        /// The steps the violation depends on.
        witness: Vec<u64>,
    },
}

/// A property's ID and its run outcome.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PropertyReport {
    /// The property's ID.
    pub property: String,
    /// Its outcome.
    pub outcome: PropertyOutcome,
}

/// Which part of the run a step belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Phase {
    /// Before the step budget runs out. Every action can be enabled.
    Live,
    /// After the budget, when only ready tasks run.
    Drain,
}

/// What a checker sees after one step.
#[derive(Clone, Copy, Debug)]
pub struct StepCtx<'a> {
    /// The step, from 1.
    pub step: u64,
    /// The step's pick.
    pub pick: &'a Identity,
    /// The enabled set the pick came from, sorted by identity.
    pub enabled: &'a [Enabled],
    /// The step's phase.
    pub phase: Phase,
    /// Virtual time after the step.
    pub now: SimInstant,
    /// The scheduler's status after the step.
    pub status: Status,
    /// Every observation so far.
    pub log: &'a ObservationLog,
}

impl StepCtx<'_> {
    /// The observations this step appended.
    pub fn observations(&self) -> impl Iterator<Item = &Observation> {
        self.log.of_step(self.step)
    }
}

/// Checks properties as a run goes.
pub trait Checker {
    /// Called after every step. A violation ends the run at once.
    fn after_step(&mut self, cx: &StepCtx<'_>) -> Option<Violation>;

    /// Called once the run has ended, before cleanup, with the complete live
    /// log. Returns each property's outcome.
    fn at_end(&mut self, end: &RunEnd, log: &ObservationLog) -> Vec<PropertyReport>;
}

/// A checker that checks nothing.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoChecks;

impl Checker for NoChecks {
    fn after_step(&mut self, _: &StepCtx<'_>) -> Option<Violation> {
        None
    }

    fn at_end(&mut self, _: &RunEnd, _: &ObservationLog) -> Vec<PropertyReport> {
        Vec::new()
    }
}

/// A checker made of two closures, for tests.
pub struct FnChecker<A, E> {
    after: A,
    end: E,
}

impl<A, E> FnChecker<A, E>
where
    A: FnMut(&StepCtx<'_>) -> Option<Violation>,
    E: FnMut(&RunEnd, &ObservationLog) -> Vec<PropertyReport>,
{
    /// A checker that calls `after` after each step and `end` at run end.
    pub fn new(after: A, end: E) -> Self {
        FnChecker { after, end }
    }
}

/// A checker that calls `after` after each step and reports no properties.
pub fn after_step<A>(
    after: A,
) -> FnChecker<A, impl FnMut(&RunEnd, &ObservationLog) -> Vec<PropertyReport>>
where
    A: FnMut(&StepCtx<'_>) -> Option<Violation>,
{
    FnChecker::new(after, |_: &RunEnd, _: &ObservationLog| Vec::new())
}

impl<A, E> Checker for FnChecker<A, E>
where
    A: FnMut(&StepCtx<'_>) -> Option<Violation>,
    E: FnMut(&RunEnd, &ObservationLog) -> Vec<PropertyReport>,
{
    fn after_step(&mut self, cx: &StepCtx<'_>) -> Option<Violation> {
        (self.after)(cx)
    }

    fn at_end(&mut self, end: &RunEnd, log: &ObservationLog) -> Vec<PropertyReport> {
        (self.end)(end, log)
    }
}
