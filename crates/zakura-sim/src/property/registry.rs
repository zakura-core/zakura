//! The registry and its checker.

use std::{collections::BTreeSet, fmt, rc::Rc};

use zakura_scheduler::{Op, SimInstant};

use super::{
    Assume, Assumptions, CheckCtx, HasView, Inconclusive, Kind, LivenessBound, Obligation,
    Property, PropertyId, Source, Status, Verdict,
};
use crate::{
    check::{Checker, PropertyOutcome, PropertyReport, StepCtx, Violation},
    end::RunEnd,
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
}

/// A harness's properties, with unique IDs.
pub struct Registry<S: HasView> {
    properties: Vec<Property<S>>,
}

impl<S: HasView> Registry<S> {
    /// A registry of `properties`, in order. Fails on a duplicate ID, or on
    /// an `Ops` bound that names an object or counts polls.
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
    /// An `Ops` bound names an object, which needs zakura#1643.
    OpsObject {
        /// The property.
        id: PropertyId,
    },
    /// An `Ops` bound counts `Op::Poll`, which no observation reports.
    OpsPoll {
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
                "{id} has an Ops bound that names an object, which waits for zakura#1643"
            ),
            RegistryError::OpsPoll { id } => {
                write!(f, "{id} has an Ops bound on polls, which nothing observes")
            }
        }
    }
}

impl std::error::Error for RegistryError {}

/// Evaluates a [`Registry`] as a run's checker.
///
/// After each step, for each property whose assumptions the scenario
/// declares and whose sources the harness observes, in registry order:
///
/// 1. It updates `holds_since` from the property's assumptions. When one
///    stops holding, the property's open obligations end, and the property
///    is `Inconclusive` with `assumption-lost`.
/// 2. While the assumptions hold, it runs the check and keeps the
///    obligations the check opens. Each `Holds` counts as applicable, and
///    the first `Holds` or `Violated` emits `Lifecycle::Applicable`.
/// 3. It expires each `VirtualTime` obligation whose deadline is behind the
///    clock. A discharge at exactly the deadline is in time.
/// 4. It walks the step's observations in order. A point first counts
///    toward each `Ops` bound it reaches, and expires that obligation once
///    past `max`. Then the observation discharges the oldest open obligation
///    whose trigger it follows and that it matches.
/// 5. When no task can run after the step, it expires every
///    `BeforeQuiescence` obligation still open.
///
/// It returns the step's first new violation of an `Active` property, which
/// ends the run. A `Known` or `Draft` violation is only recorded, so the run
/// goes on.
pub struct RegistryChecker<S: HasView> {
    properties: Vec<Property<S>>,
    state: Vec<PropertyState>,
    handle: Rc<S>,
    holds: fn(&S, Assume) -> bool,
    events: Vec<CheckEvent>,
}

/// One property's progress through a run.
#[derive(Default)]
struct PropertyState {
    /// True if the scenario declares every assumption the property needs.
    declared: bool,
    /// The sources the property needs that the harness doesn't observe.
    missing: Vec<Source>,
    /// Evaluations that returned `Holds`.
    applicable: u32,
    /// True once an evaluation returned `Holds` or `Violated`, and the
    /// `Applicable` event went out.
    announced: bool,
    /// The first violation.
    violation: Option<Violation>,
    /// Open obligations, oldest first.
    open: Vec<Open>,
    /// The step and time after which all the property's assumptions have
    /// held. `None` while one doesn't, and always for a property with none.
    holds_since: Option<(u64, SimInstant)>,
    /// True once the assumptions have all held.
    ever_held: bool,
    /// The first reason the run can't finish the check.
    inconclusive: Option<Inconclusive>,
}

/// An open obligation.
struct Open {
    obligation: Obligation,
    /// The step the check opened it at.
    registered: u64,
    /// Points counted toward an `Ops` bound since then.
    ops: u32,
}

impl Open {
    /// True if observation `at` of `step` came after the trigger, so it may
    /// discharge the obligation.
    fn follows(&self, step: u64, at: usize) -> bool {
        step > self.registered || self.obligation.trigger.is_some_and(|trigger| at > trigger)
    }
}

impl<S: HasView> RegistryChecker<S> {
    /// A checker for `registry`'s properties. Checks read the harness's view
    /// through `handle`, `assumptions` says what the scenario declares, and
    /// `sources` names what the harness observes. A property that needs a
    /// source outside `sources` never runs and is `NotApplicable`.
    pub fn new(
        registry: Registry<S>,
        handle: Rc<S>,
        assumptions: Assumptions<S>,
        sources: &[Source],
    ) -> Self {
        let state = registry
            .properties
            .iter()
            .map(|property| PropertyState {
                declared: property.assumes.iter().all(|a| assumptions.declares(*a)),
                missing: property
                    .needs
                    .iter()
                    .copied()
                    .filter(|need| !sources.contains(need))
                    .collect(),
                ..PropertyState::default()
            })
            .collect();
        RegistryChecker {
            properties: registry.properties,
            state,
            handle,
            holds: assumptions.holds,
            events: Vec::new(),
        }
    }

    /// The sources property `id` needs that the harness doesn't observe, in
    /// `needs` order, for the evidence report to name. Empty for a property
    /// that runs, or isn't registered.
    pub fn missing_sources(&self, id: &PropertyId) -> &[Source] {
        self.properties
            .iter()
            .zip(&self.state)
            .find(|(property, _)| &property.id == id)
            .map_or(&[], |(_, state)| &state.missing)
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
}

impl<S: HasView> Checker for RegistryChecker<S> {
    /// Evaluates every property after a step, as the type's docs describe.
    fn after_step(&mut self, cx: &StepCtx<'_>) -> Option<Violation> {
        let mut first = None;
        for (property, state) in self.properties.iter().zip(&mut self.state) {
            if !state.declared || !state.missing.is_empty() {
                continue;
            }
            let Some(violation) = step_property(
                property,
                state,
                &self.handle,
                self.holds,
                &mut self.events,
                cx,
            ) else {
                continue;
            };
            if state.violation.is_none() {
                state.violation = Some(violation.clone());
                if property.status == Status::Active && first.is_none() {
                    first = Some(violation);
                }
            }
        }
        first
    }

    /// Reports each property's outcome. An obligation still open now leaves
    /// the property `Inconclusive`, never `Held`.
    fn at_end(&mut self, _: &RunEnd, _: &ObservationLog) -> Vec<PropertyReport> {
        self.properties
            .iter()
            .zip(&self.state)
            .map(|(property, state)| PropertyReport {
                property: property.id.to_string(),
                outcome: outcome(property, state),
                holds_since: state.holds_since.map(|(step, _)| step),
            })
            .collect()
    }

    /// The lifecycle events since the last call.
    fn events(&mut self) -> Vec<CheckEvent> {
        std::mem::take(&mut self.events)
    }
}

/// Steps one declared property, and returns its first violation this step.
fn step_property<S: HasView>(
    property: &Property<S>,
    state: &mut PropertyState,
    handle: &S,
    holds: fn(&S, Assume) -> bool,
    events: &mut Vec<CheckEvent>,
    cx: &StepCtx<'_>,
) -> Option<Violation> {
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
            return None;
        }
    }

    let mut violation = None;
    let mut ccx = CheckCtx::new(cx, handle, property);
    let verdict = (property.check)(&mut ccx);
    if verdict != Verdict::NotApplicable && !state.announced {
        state.announced = true;
        events.push(event(Lifecycle::Applicable));
    }
    match verdict {
        Verdict::Holds => state.applicable = state.applicable.saturating_add(1),
        Verdict::NotApplicable => {}
        Verdict::Violated(v) => violation = Some(v),
    }
    for obligation in ccx.into_obligations() {
        events.push(event(Lifecycle::Obliged));
        state.open.push(Open {
            obligation,
            registered: cx.step,
            ops: 0,
        });
    }

    let floor = state.holds_since.map(|(_, at)| at);
    let mut expired = Vec::new();
    // Every observation of the step happened at `cx.now`, so none can
    // discharge an obligation whose deadline is already behind it.
    state.open.retain(|open| {
        let LivenessBound::VirtualTime(within) = open.obligation.bound else {
            return true;
        };
        let overdue =
            deadline(&open.obligation, floor, within).is_some_and(|deadline| cx.now > deadline);
        if overdue {
            events.push(event(Lifecycle::Expired));
            expired.push(expiry(property, open, floor, cx));
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
            expired.push(expiry(property, open, floor, cx));
            false
        });
        let matched = state.open.iter().position(|open| {
            let obligation = &open.obligation;
            open.follows(cx.step, at) && (obligation.discharged_by)(&obligation.key, obs)
        });
        if let Some(i) = matched {
            state.open.remove(i);
            events.push(event(Lifecycle::Discharged));
        }
    }

    state.open.retain(|open| {
        let gone = open.obligation.bound == LivenessBound::BeforeQuiescence
            && cx.status != zakura_scheduler::Status::Running;
        if gone {
            events.push(event(Lifecycle::Expired));
            expired.push(expiry(property, open, floor, cx));
        }
        !gone
    });
    if let Some(v) = expired.into_iter().next() {
        violation.get_or_insert(v);
    }
    violation
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
fn deadline(
    obligation: &Obligation,
    holds_since: Option<SimInstant>,
    within: std::time::Duration,
) -> Option<SimInstant> {
    let origin = holds_since.map_or(obligation.opened_time, |at| at.max(obligation.opened_time));
    origin.checked_add(within)
}

/// The violation an expired obligation reports.
fn expiry<S: HasView>(
    property: &Property<S>,
    open: &Open,
    holds_since: Option<SimInstant>,
    cx: &StepCtx<'_>,
) -> Violation {
    let obligation = &open.obligation;
    let subject = &obligation.key.subject;
    let (expected, actual) = match obligation.bound {
        LivenessBound::BeforeQuiescence => (
            format!("{subject:?} discharged before quiescence"),
            format!(
                "the scheduler is {:?} after step {} with the obligation from step {} open",
                cx.status, cx.step, obligation.opened_at
            ),
        ),
        LivenessBound::VirtualTime(within) => (
            format!(
                "{subject:?} discharged by {} ns",
                deadline(obligation, holds_since, within)
                    .map_or(u128::MAX, |at| at.since_start().as_nanos())
            ),
            format!("still open at {} ns", cx.now.since_start().as_nanos()),
        ),
        LivenessBound::Ops { task, op, max, .. } => (
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
fn outcome<S: HasView>(property: &Property<S>, state: &PropertyState) -> PropertyOutcome {
    if let Some(violation) = &state.violation {
        return PropertyOutcome::Violated {
            witness: violation.witness.clone(),
        };
    }
    if !state.declared || !state.missing.is_empty() {
        return PropertyOutcome::NotApplicable;
    }
    let reason = if !property.assumes.is_empty() && !state.ever_held {
        Some(Inconclusive::AssumptionsNeverHeld)
    } else {
        state
            .inconclusive
            .or((!state.open.is_empty()).then_some(Inconclusive::ObligationOpen))
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
