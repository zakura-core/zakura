//! Verdicts and obligations (properties.md, Verdicts and obligations).

use std::{fmt, time::Duration};

use serde::{Deserialize, Serialize};
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
/// later observation or view-side fact discharges it, or the bound expires
/// it. `V` is the harness's view, which [`FactDischarge`] reads.
#[derive(Clone, Debug)]
pub struct Obligation<V> {
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
    /// How many open obligations one matching observation discharges.
    pub cardinality: Cardinality,
    /// The steps a violation would depend on, in order.
    pub witness: Vec<u64>,
    /// The view-side facts that answer it, or `None` if observations do.
    /// With `Some`, `discharged_by` and `trigger` are unused, since facts
    /// and observations share no order.
    pub facts: Option<FactDischarge<V>>,
}

/// How view-side facts answer an obligation. After each step's
/// observations, the registry offers it each fact the step added, in order
/// of their stamps ([`HasView::stamps_after`](super::HasView::stamps_after)),
/// if the fact follows its trigger.
#[derive(Clone, Debug)]
pub struct FactDischarge<V> {
    /// The trigger fact's stamp, which must be one the step added, or `None`
    /// for a trigger that isn't a fact. Only facts with later stamps answer
    /// the obligation: in the trigger step, those after this stamp, and with
    /// `None`, none.
    pub after: Option<u64>,
    /// What the fact with stamp `stamp` in the view means for the obligation
    /// with this key.
    pub answer: fn(&ObligationKey, &V, u64) -> FactAnswer,
}

/// Who ended something an obligation waits on, or an assumption rests on.
///
/// Only the environment may discharge an obligation by ending what it
/// waits on, or break an assumption. The environment's ends are the
/// harness closing or relinking a link, a scripted peer closing its own
/// connection (its `PeerClosed`), and the harness starting to stop the
/// node. Anything the node under test ends is the node's: a session its
/// reactor ends, refuses or retires, or a run loop that stops taking work.
/// A node-side end answers nothing, so its obligation runs to its deadline
/// or to the run's settled end and is violated there. Otherwise a node that
/// drops the peer it owes, or wedges, would read as not applicable, and
/// that's the denial of service these properties exist to catch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EndedBy {
    /// One of the environment's ends listed above.
    Environment,
    /// The node under test.
    Node,
}

impl EndedBy {
    /// What this end means for an obligation that waits on what ended:
    /// the environment's discharges it, and the node's is unrelated.
    pub fn answer(self) -> FactAnswer {
        match self {
            EndedBy::Environment => FactAnswer::Discharges,
            EndedBy::Node => FactAnswer::Unrelated,
        }
    }
}

/// What one view-side fact means for an obligation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FactAnswer {
    /// Nothing.
    Unrelated,
    /// It discharges the obligation, under the obligation's [`Cardinality`].
    /// An end discharges only as [`EndedBy`] allows.
    Discharges,
    /// It moves a `BeforeQuiescence` obligation's deadline to this instant.
    /// From then on, the obligation no longer expires at quiescence, but at
    /// the first step whose time is past the instant, before that step's
    /// observations or facts count. An instant already behind the clock
    /// expires it at once. If the run ends idle first, the instant can't
    /// come, so it expires at the run's end. A run cut off by its budget
    /// leaves it open. Every matching obligation defers, and a later
    /// deferral replaces an earlier one.
    DefersTo(SimInstant),
}

/// How many open obligations one observation discharges. Either way, it
/// discharges only obligations whose trigger it follows and whose key it
/// matches, so an old close never answers a later trigger.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cardinality {
    /// The oldest match only, as a completion belongs to one operation or
    /// owner.
    One,
    /// Every match, as one connection close answers every earlier
    /// consequence on that connection.
    Every,
}

/// What an obligation waits on: its property and one subject. Its
/// [`Cardinality`] says whether one observation may discharge several
/// obligations with matching keys.
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
        /// The runtime object's creation label. Observations name an object
        /// only by its creator and ordinal, and only a `Mutex` reports one,
        /// so a registry rejects `Some` today.
        object: Option<&'static str>,
        /// The most points allowed.
        max: u32,
    },
}

/// Where a check left a pending mark ([`CheckCtx::pending`](super::CheckCtx::pending)):
/// its trigger fired, but the run hasn't yet shown what the check needs to
/// judge it. A mark left at the run's end, which no end judgment resolved,
/// makes the property `NotApplicable { Unsettled { site } }`
/// ([`crate::NaReason::Unsettled`]). The variants' order is the order the
/// registry picks a property's reason in when it left several.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum Pending {
    /// `header-sync/header-fault-penalized`: a penalty whose call is still being
    /// handled, or a failed reply the reactor hasn't taken yet.
    FaultOpen,
    /// `header-sync/chunk-budget-bound`: the boundary's budget read found
    /// the lock held with the reactor stopped mid-poll.
    UnreadBoundary,
    /// `header-sync/teardown-returns-baseline`: a released round is
    /// unsettled, or the node's stop has begun and it hasn't settled.
    TeardownOpen,
    /// `block-sync/teardown-returns-baseline`: every peer left, but a
    /// routine, queued body or serving lease is still open, or the
    /// scheduler isn't at rest.
    BlockTeardownOpen,
    /// A watch property: a published state still waits for its read or its
    /// third event.
    WatchOpen,
    /// `header-sync/peer-retirement-progress`: a trigger came before any
    /// read of the fixture state the check needed to judge it, which no
    /// later read can supply.
    UnreadTrigger,
    /// `header-sync/peer-retirement-progress`: a departure the slot rule
    /// hasn't judged at a unit boundary yet.
    UnjudgedDeparture,
}

impl fmt::Display for Pending {
    /// Writes the site's kebab-case name, such as `watch-open`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Pending::FaultOpen => "fault-open",
            Pending::UnreadBoundary => "unread-boundary",
            Pending::TeardownOpen => "teardown-open",
            Pending::BlockTeardownOpen => "block-teardown-open",
            Pending::WatchOpen => "watch-open",
            Pending::UnreadTrigger => "unread-trigger",
            Pending::UnjudgedDeparture => "unjudged-departure",
        })
    }
}
