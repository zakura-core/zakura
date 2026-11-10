//! The registry and its checker.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    rc::Rc,
};

use zakura_scheduler::{Op, SimInstant};

use super::{
    Assume, Assumptions, Capture, Cardinality, CheckCtx, EndViolation, FactAnswer, HasView,
    Inconclusive, Kind, LivenessBound, Obligation, Property, PropertyId, Source, Status,
    TriggerHistory, Verdict,
};
use crate::{
    check::{Checker, PropertyOutcome, PropertyReport, StepCtx, Violation},
    end::{HarnessFailure, Outcome, RunEnd},
    model::LabelledFrame,
    Observation, ObservationLog,
};

/// An event one checker passes to another through [`Tee`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CheckEvent {
    /// A property reached a point in its life.
    Lifecycle {
        /// The property.
        property: PropertyId,
        /// The point it reached.
        to: Lifecycle,
    },
    /// A property's check labelled a frame through [`CheckCtx::label`].
    Label {
        /// The property.
        property: PropertyId,
        /// The frame, its label, and what the receiver did with it.
        frame: LabelledFrame,
    },
}

/// A point in a property's life during a run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lifecycle {
    /// Its check applied for the first time.
    Applicable,
    /// It opened an obligation.
    Obliged,
    /// An observation discharged one of its obligations.
    Discharged,
    /// One of its obligations expired undischarged.
    Expired,
}

/// Two checkers run as one pipeline: `.0` sees each step first, and its
/// events go to `.1` before `.1` sees the step. Events from outside go to
/// `.0`, and `.1`'s events come out, so nested tees chain in order.
///
/// It returns the first violation and concatenates the two checkers'
/// reports.
pub struct Tee<A, B>(pub A, pub B);

impl<A: Checker, B: Checker> Checker for Tee<A, B> {
    /// Runs `.0`, passes its events to `.1`, then runs `.1`.
    fn after_step(&mut self, cx: &StepCtx<'_>) -> Option<Violation> {
        let first = self.0.after_step(cx);
        self.1.observe_events(&self.0.events());
        let second = self.1.after_step(cx);
        first.or(second)
    }

    /// Ends `.0`, passes its last events to `.1`, then ends `.1`.
    fn at_end(&mut self, end: &RunEnd, log: &ObservationLog) -> Vec<PropertyReport> {
        let mut reports = self.0.at_end(end, log);
        self.1.observe_events(&self.0.events());
        reports.extend(self.1.at_end(end, log));
        reports
    }

    /// `.1`'s events.
    fn events(&mut self) -> Vec<CheckEvent> {
        self.1.events()
    }

    /// Passes `events` to `.0`.
    fn observe_events(&mut self, events: &[CheckEvent]) {
        self.0.observe_events(events);
    }

    /// `.0`'s failure, or else `.1`'s.
    fn harness_failure(&mut self) -> Option<HarnessFailure> {
        self.0
            .harness_failure()
            .or_else(|| self.1.harness_failure())
    }

    /// True only if both bound it. Asks both.
    fn bounds_livelock(&mut self) -> bool {
        let first = self.0.bounds_livelock();
        self.1.bounds_livelock() && first
    }

    /// Passes `violation` to both.
    fn step_loop_violation(&mut self, violation: &Violation) {
        self.0.step_loop_violation(violation);
        self.1.step_loop_violation(violation);
    }

    /// True only if both stop at it. Asks both.
    fn stops(&mut self, property: &str) -> bool {
        let first = self.0.stops(property);
        self.1.stops(property) && first
    }
}

/// A harness's properties, with unique IDs.
pub struct Registry<S: HasView> {
    properties: Vec<Property<S>>,
}

impl<S: HasView> Registry<S> {
    /// A registry of `properties`, in order. Fails on a duplicate ID, on an
    /// `Ops` bound that names an object or counts polls, or on a trigger
    /// history that reads the view of a property that doesn't need it.
    pub fn new(properties: Vec<Property<S>>) -> Result<Self, RegistryError> {
        let mut ids = BTreeSet::new();
        for property in &properties {
            if !ids.insert(&property.id) {
                return Err(RegistryError::DuplicateId {
                    id: property.id.clone(),
                });
            }
            if let Kind::Liveness {
                bound: LivenessBound::Ops { op, object, .. },
            } = property.kind
            {
                if object.is_some() {
                    return Err(RegistryError::OpsObject {
                        id: property.id.clone(),
                    });
                }
                if op == Op::Poll {
                    return Err(RegistryError::OpsPoll {
                        id: property.id.clone(),
                    });
                }
            }
            // The registry checks the view's freshness only for a property
            // that needs it, and the witness judge trusts that check.
            if matches!(property.witness, Some(TriggerHistory::View(_)))
                && !property.needs.contains(&Source::View)
            {
                return Err(RegistryError::ViewTriggerWithoutView {
                    id: property.id.clone(),
                });
            }
        }
        Ok(Registry { properties })
    }

    /// The properties, in order.
    pub fn properties(&self) -> &[Property<S>] {
        &self.properties
    }
}

/// Why a registry couldn't be built.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RegistryError {
    /// Two properties share an ID.
    DuplicateId {
        /// The shared ID.
        id: PropertyId,
    },
    /// An `Ops` bound names an object, which needs object creation labels.
    OpsObject {
        /// The property.
        id: PropertyId,
    },
    /// An `Ops` bound counts `Op::Poll`, which no observation reports.
    OpsPoll {
        /// The property.
        id: PropertyId,
    },
    /// A trigger history reads the view of a property that doesn't need
    /// `Source::View`.
    ViewTriggerWithoutView {
        /// The property.
        id: PropertyId,
    },
}

impl fmt::Display for RegistryError {
    /// Names the error and the property.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RegistryError::DuplicateId { id } => write!(f, "two properties are both {id}"),
            RegistryError::OpsObject { id } => write!(
                f,
                "{id} has an Ops bound that names an object, which waits for object creation labels"
            ),
            RegistryError::OpsPoll { id } => {
                write!(f, "{id} has an Ops bound on polls, which nothing observes")
            }
            RegistryError::ViewTriggerWithoutView { id } => {
                write!(
                    f,
                    "{id}'s trigger history reads the view, so it needs the view"
                )
            }
        }
    }
}

impl std::error::Error for RegistryError {}

/// The sources a harness observes, and the ones its tier excludes with a
/// reason (properties.md, Tiers). A source in neither list is one the
/// harness should have and lacks.
#[derive(Clone, Debug, Default)]
pub struct Sources {
    observed: Vec<Source>,
    excluded: Vec<(Source, &'static str)>,
}

impl Sources {
    /// A harness that observes `observed` and excludes nothing.
    pub fn observed(observed: &[Source]) -> Self {
        Sources {
            observed: observed.to_vec(),
            excluded: Vec::new(),
        }
    }

    /// These sources, with `source` excluded for `reason`, such as "the
    /// Real tier has no view".
    pub fn excluding(mut self, source: Source, reason: &'static str) -> Self {
        self.excluded.push((source, reason));
        self
    }

    /// Whether the harness observes every source in `needs`, so a property
    /// that needs them can run there rather than report its exclusion.
    pub fn observes_all(&self, needs: &[Source]) -> bool {
        needs.iter().all(|source| self.observed.contains(source))
    }

    /// Whether the harness has `source`: `Ok` if it observes it,
    /// `Err(Some(reason))` if its tier excludes it, and `Err(None)` if it
    /// lacks it.
    fn has(&self, source: Source) -> Result<(), Option<&'static str>> {
        if self.observed.contains(&source) {
            return Ok(());
        }
        Err(self
            .excluded
            .iter()
            .find(|(excluded, _)| *excluded == source)
            .map(|(_, reason)| *reason))
    }
}

/// Evaluates a [`Registry`] as a run's checker.
///
/// A property that needs a source the harness's tier excludes never runs,
/// and its report names the exclusion. One that needs a source the harness
/// neither observes nor excludes fails the run as a harness failure, so a
/// lost source never reads as `Held` or as a silent `NotApplicable`.
///
/// A property that needs the view runs only if the view shows the state at
/// the step's boundary ([`HasView::captured`]). A stale view fails the run
/// as a harness failure.
///
/// After each step, for each property whose assumptions the scenario
/// declares, in registry order:
///
/// 1. It updates `holds_since` from the property's assumptions. When one
///    stops holding, the property's open obligations end, and the property
///    is `Inconclusive` with `assumption-lost`.
/// 2. While the assumptions hold, it runs the check and keeps the
///    obligations the check opens. Each `Holds` counts as applicable, and
///    the first `Holds` or `Violated` emits `Lifecycle::Applicable`.
/// 3. It expires each `VirtualTime` obligation whose deadline is behind the
///    clock. A discharge at exactly the deadline is in time.
///    A deferred obligation's deadline is its deferral's instant.
/// 4. It walks the step's observations in order. A point first counts
///    toward each `Ops` bound it reaches, and expires that obligation once
///    past `max`. Then the observation discharges the open obligations
///    whose trigger it follows and that it matches: the oldest one of
///    [`Cardinality::One`], and every one of [`Cardinality::Every`].
/// 5. It walks the view-side facts the step added, in stamp order, the
///    same way, for the obligations facts answer
///    ([`FactDischarge`](super::FactDischarge)). A fact can also defer a
///    `BeforeQuiescence` obligation ([`FactAnswer::DefersTo`]). One deferred
///    to an instant already behind the clock expires there, before the
///    step's later facts.
/// 6. When no task can run after the step, it expires every
///    `BeforeQuiescence` obligation still open that isn't deferred.
///
/// At the run's end, a run that ended idle expires every obligation still
/// deferred, since nothing is left to bring the clock to its instant. The
/// scheduler's status alone can't tell that during a step, because a
/// source can still offer an action or a time boundary. A run cut off by
/// its budget leaves a deferred obligation open.
///
/// It evaluates every property before it returns the step's first new
/// violation of an `Active` property, which ends the run. A `Known` or
/// `Draft` violation is only recorded, so the run goes on. Each report
/// keeps every violation its property had.
pub struct RegistryChecker<S: HasView> {
    properties: Vec<Property<S>>,
    state: Vec<PropertyState<S::View>>,
    handle: Rc<S>,
    holds: fn(&S, Assume) -> bool,
    events: Vec<CheckEvent>,
    /// The first harness failure not yet reported.
    failure: Option<HarnessFailure>,
    /// True if the scenario declares `BoundedAdversary`.
    bounded: bool,
    /// The stamp of the last view-side fact the registry offered.
    offered: Option<u64>,
    /// Virtual time after the last step.
    last_now: SimInstant,
}

/// One property's progress through a run.
struct PropertyState<V> {
    /// True if the scenario declares every assumption the property needs.
    declared: bool,
    /// The first source the property needs that the harness's tier
    /// excludes, and why. The property never runs.
    excluded: Option<String>,
    /// Evaluations that returned `Holds`.
    applicable: u32,
    /// True once an evaluation returned `Holds` or `Violated`, and the
    /// `Applicable` event went out.
    announced: bool,
    /// Every violation, in order.
    violations: Vec<Violation>,
    /// How many of `violations`, the last ones, the run's end found: the
    /// harness's end findings and the obligations its idle end expired.
    end_found: usize,
    /// Open obligations, oldest first.
    open: Vec<Open<V>>,
    /// The step and time after which all the property's assumptions have
    /// held. `None` while one doesn't, and always for a property with none.
    holds_since: Option<(u64, SimInstant)>,
    /// True once the assumptions have all held.
    ever_held: bool,
    /// The first reason the run can't finish the check.
    inconclusive: Option<Inconclusive>,
    /// Why the latest evaluation couldn't judge its fired trigger, if so.
    unsettled: Option<Inconclusive>,
    /// Each quantity's largest value, as the check measured it.
    peaks: BTreeMap<String, u64>,
}

impl<V> PropertyState<V> {
    /// A property's state before the run, with whether the scenario
    /// `declared` its assumptions and the source it needs that the tier
    /// `excluded`.
    fn new(declared: bool, excluded: Option<String>) -> Self {
        PropertyState {
            declared,
            excluded,
            applicable: 0,
            announced: false,
            violations: Vec::new(),
            end_found: 0,
            open: Vec::new(),
            holds_since: None,
            ever_held: false,
            inconclusive: None,
            unsettled: None,
            peaks: BTreeMap::new(),
        }
    }
}

/// An open obligation.
struct Open<V> {
    obligation: Obligation<V>,
    /// The step the check opened it at.
    registered: u64,
    /// Points counted toward an `Ops` bound since then.
    ops: u32,
    /// The deadline a view-side fact deferred it to, if one did.
    deferred: Option<SimInstant>,
}

impl<V> Open<V> {
    /// True if observation `at` of `step` came after the trigger, so it may
    /// discharge the obligation.
    fn follows(&self, step: u64, at: usize) -> bool {
        step > self.registered || self.obligation.trigger.is_some_and(|trigger| at > trigger)
    }

    /// The answer of the view-side fact with `stamp`, which `step` added, or
    /// `None` if facts don't answer the obligation or the fact doesn't
    /// follow its trigger.
    fn fact(&self, step: u64, view: &V, stamp: u64) -> Option<FactAnswer> {
        let facts = self.obligation.facts.as_ref()?;
        let follows = step > self.registered || facts.after.is_some_and(|after| stamp > after);
        follows.then(|| (facts.answer)(&self.obligation.key, view, stamp))
    }

    /// The deadline the obligation expires after: its deferral's, or a
    /// `VirtualTime` bound's from the later of its trigger and
    /// `holds_since`. `None` for neither, or past the largest instant.
    fn deadline(&self, holds_since: Option<SimInstant>) -> Option<SimInstant> {
        match (self.deferred, self.obligation.bound) {
            (Some(at), _) => Some(at),
            (None, LivenessBound::VirtualTime(within)) => {
                deadline(&self.obligation, holds_since, within)
            }
            (None, _) => None,
        }
    }
}

impl<S: HasView> RegistryChecker<S> {
    /// A checker for `registry`'s properties. Checks read the harness's view
    /// through `handle`, `assumptions` says what the scenario declares, and
    /// `sources` says what the harness observes and what its tier excludes.
    /// A property that needs a source the harness lacks leaves a harness
    /// failure for the run's first [`Checker::harness_failure`] call.
    pub fn new(
        registry: Registry<S>,
        handle: Rc<S>,
        assumptions: Assumptions<S>,
        sources: &Sources,
    ) -> Self {
        let mut failure = None;
        let state = registry
            .properties
            .iter()
            .map(|property| {
                let mut excluded = None;
                for need in property.needs {
                    match sources.has(*need) {
                        Ok(()) => {}
                        Err(Some(reason)) => {
                            excluded.get_or_insert_with(|| format!("{need:?}: {reason}"));
                        }
                        Err(None) => {
                            failure.get_or_insert_with(|| HarnessFailure::MissingSource {
                                property: property.id.to_string(),
                                source: format!("{need:?}"),
                            });
                        }
                    }
                }
                PropertyState::new(
                    property.assumes.iter().all(|a| assumptions.declares(*a)),
                    excluded,
                )
            })
            .collect();
        RegistryChecker {
            properties: registry.properties,
            state,
            handle,
            holds: assumptions.holds,
            events: Vec::new(),
            failure,
            bounded: assumptions.declares(Assume::BoundedAdversary),
            offered: None,
            last_now: SimInstant::default(),
        }
    }

    /// Why the harness doesn't run property `id`: the excluded source it
    /// needs and the exclusion's reason. `None` for a property that runs,
    /// or isn't registered.
    pub fn exclusion(&self, id: &PropertyId) -> Option<&str> {
        self.properties
            .iter()
            .zip(&self.state)
            .find(|(property, _)| &property.id == id)
            .and_then(|(_, state)| state.excluded.as_deref())
    }

    /// The step after which all of property `id`'s assumptions have held so
    /// far. `None` while one doesn't hold, and for a property that assumes
    /// nothing or isn't registered.
    pub fn holds_since(&self, id: &PropertyId) -> Option<u64> {
        self.properties
            .iter()
            .zip(&self.state)
            .find(|(property, _)| &property.id == id)
            .and_then(|(_, state)| state.holds_since)
            .map(|(step, _)| step)
    }

    /// Records `found` as the registered property's with its ID, at the
    /// run's last step, `last`, beside any violation it already has. A step
    /// finding and an end finding of one ID are both kept, and so are two
    /// end findings of one ID (C12). A harness whose end replay finds a
    /// step's break again leaves it out itself
    /// ([`HasView::end_violations`]).
    fn end_violation(&mut self, found: EndViolation, last: u64) {
        let state = self
            .properties
            .iter()
            .zip(&mut self.state)
            .find(|(property, _)| property.id == found.property)
            .map(|(_, state)| state);
        if let Some(state) = state {
            state.end_found += 1;
            state.violations.push(Violation {
                property: found.property.to_string(),
                step: last,
                witness: vec![last],
                expected: found.expected,
                actual: found.actual,
            });
        }
    }

    /// Expires every obligation still deferred when the run went idle after
    /// step `last`, as the type's docs describe, emitting each expiry as the
    /// other expiry paths do. The run's end found each, so it counts among
    /// `end_found`.
    fn expire_deferred(&mut self, last: u64) {
        for (property, state) in self.properties.iter().zip(&mut self.state) {
            let (expired, open) = std::mem::take(&mut state.open)
                .into_iter()
                .partition(|open| open.deferred.is_some());
            state.open = open;
            for open in expired {
                self.events.push(CheckEvent::Lifecycle {
                    property: property.id.clone(),
                    to: Lifecycle::Expired,
                });
                state.end_found += 1;
                state
                    .violations
                    .push(idle_expiry(property, &open, last, self.last_now));
            }
        }
    }
}

impl<S: HasView> Checker for RegistryChecker<S> {
    /// Evaluates every property after a step, as the type's docs describe.
    fn after_step(&mut self, cx: &StepCtx<'_>) -> Option<Violation> {
        self.last_now = cx.now;
        let mut first = None;
        // Stamps are read only from a fresh view. A stale one reads none, and
        // the properties that need the view fail the run below.
        let captured = self.handle.captured();
        let facts = captured
            .fresh_at(cx.step)
            .then(|| self.handle.stamps_after(self.offered))
            .flatten();
        if let Some(stamps) = &facts {
            let ordered = std::iter::once(self.offered)
                .chain(stamps.iter().copied().map(Some))
                .is_sorted_by(|earlier, later| earlier < later);
            if !ordered {
                self.failure
                    .get_or_insert_with(|| HarnessFailure::SourceEvidence {
                        source: "view facts".to_owned(),
                        detail: format!(
                            "StampsOutOfOrder: after {:?}, the view reports {stamps:?}",
                            self.offered
                        ),
                    });
                return None;
            }
            if let Some(&last) = stamps.last() {
                self.offered = Some(last);
            }
        }
        let mut view = None;
        for (at, (property, state)) in self.properties.iter().zip(&mut self.state).enumerate() {
            if !state.declared || state.excluded.is_some() {
                continue;
            }
            if property.needs.contains(&Source::View) && !captured.fresh_at(cx.step) {
                self.failure
                    .get_or_insert_with(|| HarnessFailure::StaleView {
                        property: property.id.to_string(),
                        step: cx.step,
                        captured: match captured {
                            Capture::AfterStep(step) => Some(step),
                            Capture::Live | Capture::Missing => None,
                        },
                    });
                continue;
            }
            let violations = step_property(
                (&self.properties, at),
                state,
                (&self.handle, facts.as_deref(), &mut view),
                self.holds,
                &mut self.events,
                &mut self.failure,
                cx,
            );
            if property.status == Status::Active && first.is_none() {
                first = violations.first().cloned();
            }
            state.violations.extend(violations);
        }
        first
    }

    /// Tells the harness how the run ended, then reports each property's
    /// outcome. An obligation still open now leaves
    /// the property `Inconclusive`, never `Held`. A violation that ended the
    /// run outside the registry, such as the step loop's `sim/no-livelock`,
    /// belongs to the registered property with its ID, and so does each of
    /// the harness's [`HasView::end_violations`].
    fn at_end(&mut self, end: &RunEnd, log: &ObservationLog) -> Vec<PropertyReport> {
        self.handle.ended(end);
        if matches!(end.outcome, Outcome::Idle { .. }) {
            self.expire_deferred(end.steps);
        }
        if let Some(violation) = &end.violation {
            self.step_loop_violation(violation);
        }
        for found in self.handle.end_violations(log) {
            self.end_violation(found, end.steps);
        }
        self.properties
            .iter()
            .zip(&self.state)
            .map(|(property, state)| PropertyReport {
                property: property.id.to_string(),
                outcome: outcome(property, state),
                holds_since: state.holds_since.map(|(step, _)| step),
                excluded: state.excluded.clone(),
                peaks: state.peaks.clone(),
                violations: state.violations.clone(),
                end_found: state.end_found,
            })
            .collect()
    }

    /// The lifecycle and label events since the last call.
    fn events(&mut self) -> Vec<CheckEvent> {
        std::mem::take(&mut self.events)
    }

    /// The first missing source, stale view, or missing evidence, once.
    fn harness_failure(&mut self) -> Option<HarnessFailure> {
        self.failure.take()
    }

    /// False while a declared `BoundedAdversary` assumption doesn't hold.
    fn bounds_livelock(&mut self) -> bool {
        !self.bounded || (self.holds)(&self.handle, Assume::BoundedAdversary)
    }

    /// False for a registered property that isn't `Active`, whose
    /// violations are only recorded.
    fn stops(&mut self, property: &str) -> bool {
        self.properties
            .iter()
            .find(|registered| registered.id.as_str() == property)
            .is_none_or(|registered| registered.status == Status::Active)
    }

    /// Records `violation` as the registered property's with its ID, once.
    fn step_loop_violation(&mut self, violation: &Violation) {
        let found = self
            .properties
            .iter()
            .zip(&mut self.state)
            .find(|(property, _)| property.id.as_str() == violation.property);
        if let Some((_, state)) = found {
            if !state.violations.contains(violation) {
                state.violations.push(violation.clone());
            }
        }
    }
}

/// Steps one declared property, and returns its violations this step: the
/// verdict's, the ones the check recorded, then each expired obligation's.
/// Evidence the check found missing becomes `failure` unless one is already
/// pending. `facts` are the stamps of the view-side facts the step added,
/// and `view` caches the step's view for the properties that read them.
fn step_property<S: HasView>(
    (properties, at): (&[Property<S>], usize),
    state: &mut PropertyState<S::View>,
    (handle, facts, view): (&S, Option<&[u64]>, &mut Option<S::View>),
    holds: fn(&S, Assume) -> bool,
    events: &mut Vec<CheckEvent>,
    failure: &mut Option<HarnessFailure>,
    cx: &StepCtx<'_>,
) -> Vec<Violation> {
    let property = &properties[at];
    let event = |to| CheckEvent::Lifecycle {
        property: property.id.clone(),
        to,
    };
    if !property.assumes.is_empty() {
        let hold = property.assumes.iter().all(|a| holds(handle, *a));
        match (hold, state.holds_since) {
            (true, None) => {
                state.holds_since = Some((cx.step, cx.now));
                state.ever_held = true;
            }
            (false, Some(_)) => {
                state.holds_since = None;
                if !state.open.is_empty() {
                    state.open.clear();
                    state
                        .inconclusive
                        .get_or_insert(Inconclusive::AssumptionLost);
                }
            }
            _ => {}
        }
        if !hold {
            return Vec::new();
        }
    }

    let mut violations = Vec::new();
    let mut ccx = CheckCtx::new(cx, handle, (property, properties), facts);
    let verdict = (property.check)(&mut ccx);
    let parts = ccx.into_parts();
    if let Some(detail) = parts.missing {
        failure.get_or_insert_with(|| HarnessFailure::MissingEvidence {
            property: property.id.to_string(),
            step: cx.step,
            detail,
        });
    }
    let failed = matches!(verdict, Verdict::Violated(_)) || !parts.recorded.is_empty();
    if (verdict != Verdict::NotApplicable || failed) && !state.announced {
        state.announced = true;
        events.push(event(Lifecycle::Applicable));
    }
    match verdict {
        Verdict::Holds if !failed => state.applicable = state.applicable.saturating_add(1),
        Verdict::Holds | Verdict::NotApplicable => {}
        Verdict::Violated(v) => violations.push(v),
    }
    violations.extend(parts.recorded);
    state.unsettled = parts.unsettled;
    for (quantity, value) in parts.peaks {
        let peak = state.peaks.entry(quantity.to_owned()).or_default();
        *peak = (*peak).max(value);
    }
    events.extend(parts.labelled.into_iter().map(|frame| CheckEvent::Label {
        property: property.id.clone(),
        frame,
    }));
    for obligation in parts.obliged {
        events.push(event(Lifecycle::Obliged));
        state.open.push(Open {
            obligation,
            registered: cx.step,
            ops: 0,
            deferred: None,
        });
    }

    let floor = state.holds_since.map(|(_, at)| at);
    // Every observation and fact of the step happened at `cx.now`, so none
    // can discharge an obligation whose deadline is already behind it.
    state.open.retain(|open| {
        let overdue = open
            .deadline(floor)
            .is_some_and(|deadline| cx.now > deadline);
        if overdue {
            events.push(event(Lifecycle::Expired));
            violations.push(expiry(property, open, floor, cx));
        }
        !overdue
    });
    for (at, obs) in cx.observations().enumerate() {
        // A point counts toward its `Ops` bound, and can expire it, before it
        // can discharge it.
        state.open.retain_mut(|open| {
            let LivenessBound::Ops { task, op, max, .. } = open.obligation.bound else {
                return true;
            };
            if !open.follows(cx.step, at) || !reaches(obs, task, op.name()) {
                return true;
            }
            open.ops = open.ops.saturating_add(1);
            if open.ops <= max {
                return true;
            }
            events.push(event(Lifecycle::Expired));
            violations.push(expiry(property, open, floor, cx));
            false
        });
        let mut one_discharged = false;
        state.open.retain(|open| {
            let obligation = &open.obligation;
            // Facts and observations share no order, so facts alone answer
            // an obligation that facts answer.
            if obligation.facts.is_some()
                || !open.follows(cx.step, at)
                || !(obligation.discharged_by)(&obligation.key, obs)
            {
                return true;
            }
            let discharge = match obligation.cardinality {
                Cardinality::Every => true,
                Cardinality::One => !std::mem::replace(&mut one_discharged, true),
            };
            if discharge {
                events.push(event(Lifecycle::Discharged));
            }
            !discharge
        });
    }
    let stamps = facts.unwrap_or_default();
    if !stamps.is_empty()
        && state
            .open
            .iter()
            .any(|open| open.obligation.facts.is_some())
    {
        let view = &*view.get_or_insert_with(|| handle.view());
        for &stamp in stamps {
            let mut one_discharged = false;
            state
                .open
                .retain_mut(|open| match open.fact(cx.step, view, stamp) {
                    None | Some(FactAnswer::Unrelated) => true,
                    Some(FactAnswer::Discharges) => {
                        let discharge = match open.obligation.cardinality {
                            Cardinality::Every => true,
                            Cardinality::One => !std::mem::replace(&mut one_discharged, true),
                        };
                        if discharge {
                            events.push(event(Lifecycle::Discharged));
                        }
                        !discharge
                    }
                    Some(FactAnswer::DefersTo(at)) => {
                        assert_eq!(
                            open.obligation.bound,
                            LivenessBound::BeforeQuiescence,
                            "only a BeforeQuiescence obligation defers"
                        );
                        open.deferred = Some(at);
                        open.obligation.witness.push(cx.step);
                        // A deadline already behind the clock expires before a
                        // later fact of the step can discharge it.
                        let overdue = cx.now > at;
                        if overdue {
                            events.push(event(Lifecycle::Expired));
                            violations.push(expiry(property, open, floor, cx));
                        }
                        !overdue
                    }
                });
        }
    }

    state.open.retain(|open| {
        let gone = open.obligation.bound == LivenessBound::BeforeQuiescence
            && open.deferred.is_none()
            && cx.status != zakura_scheduler::Status::Running;
        if gone {
            events.push(event(Lifecycle::Expired));
            violations.push(expiry(property, open, floor, cx));
        }
        !gone
    });
    violations
}

/// True if `obs` is `task` reaching a point of the operation named `op`.
fn reaches(obs: &Observation, task: &str, op: &str) -> bool {
    matches!(
        obs,
        Observation::TaskReached { task: t, at, .. }
            if t == task && at.split('#').next() == Some(op)
    )
}

/// A `VirtualTime` obligation's deadline: `within` after the later of its
/// trigger and `holds_since`. `None` if that passes the largest instant.
fn deadline<V>(
    obligation: &Obligation<V>,
    holds_since: Option<SimInstant>,
    within: std::time::Duration,
) -> Option<SimInstant> {
    let origin = holds_since.map_or(obligation.opened_time, |at| at.max(obligation.opened_time));
    origin.checked_add(within)
}

/// The violation a deferred obligation reports when the run went idle after
/// step `last`, at `now`.
fn idle_expiry<S: HasView>(
    property: &Property<S>,
    open: &Open<S::View>,
    last: u64,
    now: SimInstant,
) -> Violation {
    let obligation = &open.obligation;
    let at = open
        .deferred
        .expect("only a deferred obligation expires at an idle end");
    let mut witness = obligation.witness.clone();
    witness.push(last);
    witness.sort_unstable();
    witness.dedup();
    Violation {
        property: property.id.to_string(),
        step: last,
        witness,
        expected: format!(
            "{:?} discharged by its deferred deadline, {} ns",
            obligation.key.subject,
            at.since_start().as_nanos()
        ),
        actual: format!(
            "the run went idle at {} ns with it still open",
            now.since_start().as_nanos()
        ),
    }
}

/// The violation an expired obligation reports.
fn expiry<S: HasView>(
    property: &Property<S>,
    open: &Open<S::View>,
    holds_since: Option<SimInstant>,
    cx: &StepCtx<'_>,
) -> Violation {
    let obligation = &open.obligation;
    let subject = &obligation.key.subject;
    let (expected, actual) = match (open.deferred, obligation.bound) {
        (Some(at), _) => (
            format!(
                "{subject:?} discharged by its deferred deadline, {} ns",
                at.since_start().as_nanos()
            ),
            format!("still open at {} ns", cx.now.since_start().as_nanos()),
        ),
        (None, LivenessBound::BeforeQuiescence) => (
            format!("{subject:?} discharged before quiescence"),
            format!(
                "the scheduler is {:?} after step {} with the obligation from step {} open",
                cx.status, cx.step, obligation.opened_at
            ),
        ),
        (None, LivenessBound::VirtualTime(within)) => (
            format!(
                "{subject:?} discharged by {} ns",
                deadline(obligation, holds_since, within)
                    .map_or(u128::MAX, |at| at.since_start().as_nanos())
            ),
            format!("still open at {} ns", cx.now.since_start().as_nanos()),
        ),
        (None, LivenessBound::Ops { task, op, max, .. }) => (
            format!(
                "{subject:?} discharged within {max} {} points of {task}",
                op.name()
            ),
            format!("still open after {}", open.ops),
        ),
    };
    let mut witness = obligation.witness.clone();
    witness.push(cx.step);
    witness.sort_unstable();
    witness.dedup();
    Violation {
        property: property.id.to_string(),
        step: cx.step,
        witness,
        expected,
        actual,
    }
}

/// A property's run outcome, from strongest to weakest: violated,
/// inconclusive, held, then not applicable.
fn outcome<S: HasView>(property: &Property<S>, state: &PropertyState<S::View>) -> PropertyOutcome {
    if let Some(violation) = state.violations.first() {
        return PropertyOutcome::Violated {
            witness: violation.witness.clone(),
        };
    }
    if !state.declared {
        return PropertyOutcome::NotApplicable;
    }
    if state.excluded.is_some() {
        return PropertyOutcome::NotApplicable;
    }
    let reason = if !property.assumes.is_empty() && !state.ever_held {
        Some(Inconclusive::AssumptionsNeverHeld)
    } else {
        state
            .inconclusive
            .or((!state.open.is_empty()).then_some(Inconclusive::ObligationOpen))
            .or(state.unsettled)
    };
    match reason {
        Some(reason) => PropertyOutcome::Inconclusive {
            reason: reason.to_string(),
        },
        None if state.applicable > 0 => PropertyOutcome::Held {
            applicable: state.applicable,
        },
        None => PropertyOutcome::NotApplicable,
    }
}
