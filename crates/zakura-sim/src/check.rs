//! Property checks a run calls after each step and at its end.
//!
//! The property registry, [`crate::property::RegistryChecker`], builds on
//! these hooks. M1 ships [`NoChecks`] and [`FnChecker`] for tests.

use std::{borrow::Cow, collections::BTreeMap, fmt};

use serde::{Deserialize, Serialize};
use zakura_scheduler::{SimInstant, Status};

use crate::{
    end::{HarnessFailure, RunEnd},
    observe::Observation,
    property::{CheckEvent, Pending},
    Enabled, Identity, ObservationLog,
};

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
    /// `applicable` evaluations applied, none failed, and nothing is open.
    Held {
        /// How many evaluations applied.
        applicable: u32,
    },
    /// The run gave no answer about the property, for `reason`. A benign
    /// reason ([`NaReason::is_benign`]) means there was nothing to judge.
    /// Any other means the run's observations were incomplete.
    NotApplicable {
        /// Why.
        reason: NaReason,
    },
    /// Some evaluation failed, or an obligation's bound expired.
    Violated {
        /// The steps the violation depends on.
        witness: Vec<u64>,
    },
}

impl PropertyOutcome {
    /// `NotApplicable` for `reason`.
    pub fn not_applicable(reason: NaReason) -> Self {
        PropertyOutcome::NotApplicable { reason }
    }
}

/// Why a property ended [`PropertyOutcome::NotApplicable`]. The registry
/// picks the first that applies, in the order `registry::outcome` gives.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum NaReason {
    /// Benign: the check ran, but no evaluation applied.
    NeverApplied,
    /// Benign: the scenario doesn't declare every assumption the property
    /// needs, so it never ran.
    Undeclared,
    /// Benign: the property needs a source the harness's tier excludes, so
    /// it never ran. The report's `excluded` names the source.
    Excluded,
    /// Incomplete: the scenario declares the property's assumptions, but
    /// they never all held.
    AssumptionsNeverHeld,
    /// Incomplete: an assumption stopped holding while an obligation was
    /// open, which withdrew the obligation, and no evaluation applied in
    /// the whole run (#1840 rule 4).
    AssumptionLost,
    /// Incomplete: an `Active` property's violation stopped the run while
    /// this property had an obligation, a deferred obligation or a pending
    /// mark open.
    StoppedByViolation,
    /// Incomplete: the run ended at a harness failure, so its evidence may
    /// be incomplete.
    HarnessFailed,
    /// Incomplete: a watch read was still open when the run ended, and the
    /// harness had begun to stop the node, which may leave it unread
    /// ([`crate::domain::header_sync::watch_end_violations`]).
    NodeStopped,
    /// Incomplete: a pending mark at `site` was left at the run's end, and
    /// no end judgment resolved it.
    Unsettled {
        /// The mark's site.
        site: Pending,
    },
}

impl NaReason {
    /// Whether the reason means there was nothing to judge, rather than
    /// that the run's observations were incomplete: `NeverApplied`,
    /// `Undeclared` and `Excluded`.
    pub fn is_benign(&self) -> bool {
        matches!(
            self,
            NaReason::NeverApplied | NaReason::Undeclared | NaReason::Excluded
        )
    }

    /// The reason's kebab-case name, such as `never-applied`, with an
    /// `Unsettled` mark's site after a colon, such as `unsettled:watch-open`.
    /// Only `Unsettled` allocates.
    pub fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed(match self {
            NaReason::NeverApplied => "never-applied",
            NaReason::Undeclared => "undeclared",
            NaReason::Excluded => "excluded",
            NaReason::AssumptionsNeverHeld => "assumptions-never-held",
            NaReason::AssumptionLost => "assumption-lost",
            NaReason::StoppedByViolation => "stopped-by-violation",
            NaReason::HarnessFailed => "harness-failed",
            NaReason::NodeStopped => "node-stopped",
            NaReason::Unsettled { site } => return Cow::Owned(format!("unsettled:{site}")),
        })
    }
}

impl fmt::Display for NaReason {
    /// Writes [`NaReason::name`].
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name())
    }
}

/// A property's ID and its run outcome.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PropertyReport {
    /// The property's ID.
    pub property: String,
    /// Its outcome.
    pub outcome: PropertyOutcome,
    /// The step after which all the property's assumptions held. `None`
    /// until properties declare assumptions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub holds_since: Option<u64>,
    /// Why the harness doesn't run the property: the excluded source it
    /// needs and the exclusion's reason. Set only with `NotApplicable`
    /// `Excluded`, unless an end finding violated it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub excluded: Option<String>,
    /// Each quantity's largest value over the run, as the property's check
    /// measured it, such as the most bytes a resource bound's allowance
    /// held. Empty for a property that measures none.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub peaks: BTreeMap<String, u64>,
    /// Every violation, in the order the run found them. `Violated`'s
    /// witness is the first one's. A `Known` or `Draft` property can fail
    /// again after its first violation, and one step can expire several
    /// obligations, so the first alone can hide the rest.
    ///
    /// The trace's `end` record leaves them out: their number grows with the
    /// run, and a record has a size limit. A replay of the trace finds every
    /// one again, in `ReplayReport::properties`.
    #[serde(skip)]
    pub violations: Vec<Violation>,
    /// How many of `violations`, the last ones, the run's end found
    /// ([`crate::property::HasView::end_violations`], or an obligation still
    /// deferred when the run went idle) rather than a step's check. Each is
    /// reported at the run's last step, which isn't the step of the break it
    /// reports. 0 from a checker with no end findings.
    #[serde(skip)]
    pub end_found: usize,
    /// True if the scenario doesn't declare every assumption the property
    /// needs, so it never ran. Its outcome says so unless an end finding
    /// violated it, and the harness-end trim reads it so that dropping the
    /// finding gives `Undeclared` back ([`Self::after_harness_failure`]).
    /// False from a checker that doesn't track declarations.
    #[serde(skip)]
    pub undeclared: bool,
}

impl PropertyReport {
    /// A report of `outcome` for `property`, with no `holds_since`, peaks,
    /// exclusion, or retained violations.
    pub fn new(property: impl Into<String>, outcome: PropertyOutcome) -> Self {
        PropertyReport {
            property: property.into(),
            outcome,
            holds_since: None,
            excluded: None,
            peaks: BTreeMap::new(),
            violations: Vec::new(),
            end_found: 0,
            undeclared: false,
        }
    }

    /// This report once the run ended at a harness failure (the trim): its
    /// end findings, the last `end_found` violations, are dropped, since the
    /// run's end may have judged incomplete evidence, and its outcome is
    /// recomputed over what's left by `registry::outcome`'s rules for a
    /// harness end ([`crate::property::leading_outcome`]). A step violation
    /// stays `Violated`, `Undeclared` and `Excluded` stay, and every other
    /// outcome becomes `NotApplicable { HarnessFailed }`.
    ///
    /// Only a retained violation proves a step found it, so a `Violated`
    /// outcome with none behind it is an end finding and is dropped too.
    pub(crate) fn after_harness_failure(mut self) -> Self {
        let kept = self.violations.len().saturating_sub(self.end_found);
        self.violations.truncate(kept);
        self.end_found = 0;
        // A checker that builds its own reports may say so only through the
        // outcome.
        let undeclared = self.undeclared
            || self.outcome == PropertyOutcome::not_applicable(NaReason::Undeclared);
        let excluded = self.excluded.is_some()
            || self.outcome == PropertyOutcome::not_applicable(NaReason::Excluded);
        self.outcome =
            crate::property::leading_outcome(&self.violations, (undeclared, excluded), true)
                .expect("a harness end decides every outcome by its first three rules");
        self
    }
}

/// Which part of the run a step belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Phase {
    /// Before the step budget runs out. Every action can be enabled.
    Live,
    /// After the budget, or a live end with work left to resolve, when only
    /// ready tasks, recovery actions and the clock move.
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
    /// Whether the run is quiescent after the step: no task can run, and no
    /// source action is eligible in the step's phase. Live, that's an
    /// enabled or quiet action, so a completion a hold hides doesn't count.
    /// In the drain, it's a recovery action, quiet or not. A boundary ahead
    /// never counts. A `BeforeQuiescence` obligation expires here.
    pub quiescent: bool,
    /// Every observation so far.
    pub log: &'a ObservationLog,
}

impl StepCtx<'_> {
    /// The observations this step appended.
    pub fn observations(&self) -> impl Iterator<Item = &Observation> {
        self.log.of_step(self.step)
    }
}

/// The checks a checker still has open: each would need more of the run to
/// conclude.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Unfinished {
    /// Each open check, in the checker's order.
    pub open: Vec<OpenCheck>,
}

impl Unfinished {
    /// The earliest deadline among the open checks, or `None` if none has
    /// one.
    pub fn earliest_deadline(&self) -> Option<SimInstant> {
        self.open.iter().filter_map(|check| check.deadline).min()
    }

    /// Whether nothing is open.
    pub fn is_empty(&self) -> bool {
        self.open.is_empty()
    }
}

/// One open check.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenCheck {
    /// The property's ID.
    pub property: String,
    /// What the check waits on, in the form its violation would name it.
    pub subject: String,
    /// What kind of check it is.
    pub kind: OpenKind,
    /// The instant it expires after, if it has one.
    pub deadline: Option<SimInstant>,
}

/// What kind of check is open.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpenKind {
    /// An obligation with a virtual-time bound.
    Timed,
    /// An obligation bounded by a task's operations.
    Ops,
    /// An obligation that must be discharged before quiescence.
    BeforeQuiescence,
    /// An obligation a view-side fact deferred to a deadline.
    Deferred,
    /// An evaluation that couldn't yet judge its trigger.
    Pending,
}

/// Checks properties as a run goes.
pub trait Checker {
    /// Called after every step. A violation ends the run at once.
    fn after_step(&mut self, cx: &StepCtx<'_>) -> Option<Violation>;

    /// Called once the run has ended, before cleanup, with the complete live
    /// log. Returns each property's outcome.
    fn at_end(&mut self, end: &RunEnd, log: &ObservationLog) -> Vec<PropertyReport>;

    /// Drains the events this checker emitted since the last call, for a
    /// checker that runs after it in a [`crate::property::Tee`].
    fn events(&mut self) -> Vec<CheckEvent> {
        Vec::new()
    }

    /// Receives the events of a checker that runs before this one in a
    /// [`crate::property::Tee`].
    fn observe_events(&mut self, _events: &[CheckEvent]) {}

    /// A harness failure the checker found, such as a source a property
    /// needs that the harness lacks, or a stale view. The run calls it
    /// before the first step, after every step and after
    /// [`at_end`](Self::at_end), and ends at the first failure. One found
    /// at the end still fails a run that didn't already fail.
    fn harness_failure(&mut self) -> Option<HarnessFailure> {
        None
    }

    /// Whether the step loop's livelock limit applies after this step. A
    /// checker returns false while a scenario's `BoundedAdversary`
    /// assumption doesn't hold. The step loop then restarts its count, so
    /// the limit covers only steps taken while the assumption holds.
    fn bounds_livelock(&mut self) -> bool {
        true
    }

    /// The step loop also found `violation`, at the step where this
    /// checker's own violation ends the run. `end.violation` keeps the
    /// checker's, so a checker that reports the step loop's properties
    /// records this one itself.
    fn step_loop_violation(&mut self, _violation: &Violation) {}

    /// Whether a violation of `property` the step loop itself finds, such
    /// as `sim/no-livelock`'s, ends the run: true unless the checker
    /// registers it with a status that only records violations, such as a
    /// `Draft` override. The default is true.
    fn stops(&mut self, _property: &str) -> bool {
        true
    }

    /// The checks still open now, which the drain keeps running for.
    fn unfinished(&self) -> Unfinished;
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

    /// Nothing, since it checks nothing.
    fn unfinished(&self) -> Unfinished {
        Unfinished::default()
    }
}

/// A checker made of two closures, for tests.
pub struct FnChecker<A, E> {
    after: A,
    end: E,
    /// Every violation `after` returned, so the end's reports carry them as
    /// step findings.
    found: Vec<Violation>,
}

impl<A, E> FnChecker<A, E>
where
    A: FnMut(&StepCtx<'_>) -> Option<Violation>,
    E: FnMut(&RunEnd, &ObservationLog) -> Vec<PropertyReport>,
{
    /// A checker that calls `after` after each step and `end` at run end.
    pub fn new(after: A, end: E) -> Self {
        FnChecker {
            after,
            end,
            found: Vec::new(),
        }
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
    /// Calls `after`, keeping what it found.
    fn after_step(&mut self, cx: &StepCtx<'_>) -> Option<Violation> {
        let violation = (self.after)(cx);
        self.found.extend(violation.clone());
        violation
    }

    /// Calls `end`. A `Violated` report that retains no violations takes the ones
    /// `after` found for its property, as step findings, so a harness
    /// failure keeps them ([`PropertyReport::after_harness_failure`]).
    fn at_end(&mut self, end: &RunEnd, log: &ObservationLog) -> Vec<PropertyReport> {
        let mut reports = (self.end)(end, log);
        let bare = |r: &&mut PropertyReport| {
            r.violations.is_empty() && matches!(r.outcome, PropertyOutcome::Violated { .. })
        };
        for report in reports.iter_mut().filter(bare) {
            report.violations = (self.found.iter())
                .filter(|v| v.property == report.property)
                .cloned()
                .collect();
            report.end_found = 0;
        }
        reports
    }

    /// Nothing, since closures keep no obligations.
    fn unfinished(&self) -> Unfinished {
        Unfinished::default()
    }
}

impl<C: Checker + ?Sized> Checker for Box<C> {
    /// Forwards to the checker.
    fn after_step(&mut self, cx: &StepCtx<'_>) -> Option<Violation> {
        (**self).after_step(cx)
    }

    /// Forwards to the checker.
    fn at_end(&mut self, end: &RunEnd, log: &ObservationLog) -> Vec<PropertyReport> {
        (**self).at_end(end, log)
    }

    /// Forwards to the checker.
    fn events(&mut self) -> Vec<CheckEvent> {
        (**self).events()
    }

    /// Forwards to the checker.
    fn observe_events(&mut self, events: &[CheckEvent]) {
        (**self).observe_events(events);
    }

    /// Forwards to the checker.
    fn harness_failure(&mut self) -> Option<HarnessFailure> {
        (**self).harness_failure()
    }

    /// Forwards to the checker.
    fn bounds_livelock(&mut self) -> bool {
        (**self).bounds_livelock()
    }

    /// Forwards to the checker.
    fn step_loop_violation(&mut self, violation: &Violation) {
        (**self).step_loop_violation(violation);
    }

    /// Forwards to the checker.
    fn stops(&mut self, property: &str) -> bool {
        (**self).stops(property)
    }

    /// Forwards to the checker.
    fn unfinished(&self) -> Unfinished {
        (**self).unfinished()
    }
}
