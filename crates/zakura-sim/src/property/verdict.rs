//! Verdicts and obligations (properties.md, Verdicts and obligations).

use std::{fmt, time::Duration};

use zakura_scheduler::{Op, SimInstant};

use super::PropertyId;
use crate::{check::Violation, Observation};

/// What one evaluation of a check found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// The check applied and passed.
    Holds,
    /// The check didn't apply at this step.
    NotApplicable,
    /// The check applied and failed.
    Violated(Violation),
}

/// Something the node must still do, within a bound. A check opens it, and a
/// later observation discharges it, or the bound expires it.
#[derive(Clone, Debug)]
pub struct Obligation {
    /// What the obligation waits on.
    pub key: ObligationKey,
    /// The trigger step.
    pub opened_at: u64,
    /// The trigger time.
    pub opened_time: SimInstant,
    /// The trigger's position among its step's observations, or `None` for
    /// a trigger that isn't one, such as a change in the view. Only later
    /// observations discharge the obligation: in the trigger step, those
    /// after this position, and with `None`, none.
    pub trigger: Option<usize>,
    /// How long the node has.
    pub bound: LivenessBound,
    /// True if an observation discharges the obligation with `key`.
    pub discharged_by: fn(&ObligationKey, &Observation) -> bool,
    /// The steps a violation would depend on, in order.
    pub witness: Vec<u64>,
}

/// What an obligation waits on: its property and one subject. One
/// observation discharges at most the oldest open obligation that it
/// matches, so two subjects never share a discharge.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObligationKey {
    /// The property that opened it.
    pub property: PropertyId,
    /// Who or what it waits on.
    pub subject: Subject,
}

/// Who or what an obligation waits on.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum Subject {
    /// A peer session, by its label.
    Session(String),
    /// An owner of work or charges, such as a peer's routine.
    Owner(String),
    /// A storage operation, by its ID.
    Operation(u64),
}

/// How long an obligation may stay open (properties.md, Liveness bounds).
/// Scheduler steps are never a unit, since their count depends on unrelated
/// `Plan` choices.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LivenessBound {
    /// Discharged before the end of the next step after which no task can
    /// run.
    BeforeQuiescence,
    /// Discharged within this much virtual time after the later of the
    /// trigger and the property's `holds_since`.
    VirtualTime(Duration),
    /// Discharged before task `task` reaches more than `max` points of `op`
    /// after the obligation opens.
    Ops {
        /// The task's name, such as `reactor.1`.
        task: &'static str,
        /// The operation counted. Not `Op::Poll`: a poll boundary logs
        /// `TaskWaiting`, never a point, so a registry rejects it.
        op: Op,
        /// The runtime object's creation label. Matching on it waits for the
        /// scheduler to keep `Point.object` in its observations
        /// (zakura#1643), so a registry rejects `Some` today.
        object: Option<&'static str>,
        /// The most points allowed.
        max: u32,
    },
}

/// Why a property's run didn't finish its check. Its `Display` form fills
/// [`crate::PropertyOutcome::Inconclusive`]'s reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Inconclusive {
    /// The scenario declares the property's assumptions, but they never all
    /// held.
    AssumptionsNeverHeld,
    /// An assumption stopped holding while an obligation was open.
    AssumptionLost,
    /// An obligation was still open at run end, within its bound.
    ObligationOpen,
}

impl fmt::Display for Inconclusive {
    /// Writes the reason's fixed name, such as `obligation-open`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Inconclusive::AssumptionsNeverHeld => "assumptions-never-held",
            Inconclusive::AssumptionLost => "assumption-lost",
            Inconclusive::ObligationOpen => "obligation-open",
        })
    }
}
