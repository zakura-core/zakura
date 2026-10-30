//! Component states and availability (M6a, the observation contract, rules
//! 4, 5 and 8).
//!
//! A [`Component`] is one piece of the node's actual state that a harness
//! reads at each step boundary, such as the chunk budget or a session
//! table. Its owner registers its read, the harness folds each read into
//! it, and a check takes its value only through [`require`],
//! [`require_every`] or [`require_coherent`], which never hand out a value
//! the boundary didn't show.

use super::{CheckCtx, HasView, Inconclusive};

/// A component a harness's view reads (M6a, Components). A property that
/// needs one names it as [`Source::Component`](super::Source::Component).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum ComponentKind {
    /// The header-sync reactor's settled snapshot.
    Reactor,
    /// The header chunk budget and its objects.
    Budget,
    /// The port calls that hold lease clones.
    CallHolders,
    /// Headers moved out of a work incarnation.
    Transfers,
    /// A session's header publisher.
    Publisher,
    /// The engine's committed snapshot.
    SelectedChain,
    /// Each stream's output lifetimes.
    Output,
    /// The supervisor's map of live connections.
    SupervisorMap,
    /// The header service's installed sessions.
    HeaderSessions,
    /// A connection loop's settled snapshot.
    Connection,
    /// A connection's session tasks and their members.
    SessionTasks,
    /// A connection's stream permits.
    StreamPermits,
    /// `SessionCapacity`'s slots.
    SessionSlots,
    /// The node's gauges.
    Gauges,
}

/// What a component holds at a boundary (rule 4). Only `Current` and
/// `Ended(Seen)` hand out a value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum State<T> {
    /// Expected, but not yet shown to exist.
    Initializing,
    /// The boundary's read showed `state`.
    Current {
        /// The state.
        state: T,
    },
    /// A task held the component across the boundary, since `since`.
    InTransition {
        /// The first boundary in transition.
        since: u64,
        /// The last current state before it, if any. Never handed out.
        last: Option<T>,
    },
    /// The component is gone. Terminal.
    Ended(Ended<T>),
}

/// How a component ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Ended<T> {
    /// Its last read before it went was current at `last_seen`. `state` is
    /// the state then, not at its destruction.
    Seen {
        /// The boundary of its last current read.
        last_seen: u64,
        /// The state at `last_seen`.
        state: T,
    },
    /// Its last read before it went was in transition, so no state is known.
    Unsettled,
}

/// One boundary read's result (rule 1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Read<T> {
    /// The protected value, read with no task holding it.
    Current(T),
    /// A parked task holds the component's lock.
    Held {
        /// The holding task, for diagnostics.
        holder: String,
    },
    /// The owning allocation dropped.
    Gone,
}

/// A tagged object's phase, derived from memory alone (rule 2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TagPhase {
    /// Registered, with no `Weak` attached yet.
    Creating,
    /// Its `Weak` upgrades.
    Live,
    /// Its `Weak` is dead, and its `Drop` is still running.
    Releasing,
}

/// A component state that lists tagged objects, so reconciliation can wait
/// out the ones mid-creation or mid-release.
pub trait Tagged {
    /// The first listed object that is `Creating` or `Releasing`, if any.
    fn in_flux(&self) -> Option<TagPhase>;
}

/// One component's fold: its read registration, its existence evidence, and
/// the state its reads showed.
#[derive(Clone, Debug)]
pub struct Component<T> {
    kind: ComponentKind,
    registered: bool,
    exists: bool,
    /// What the reads showed, whether or not the component exists yet.
    read: State<T>,
    fresh_through: Option<u64>,
    last_current: Option<u64>,
    seq: u64,
}

impl<T: Clone + PartialEq> Component<T> {
    /// A `kind` component with no read registered.
    pub fn new(kind: ComponentKind) -> Self {
        Component {
            kind,
            registered: false,
            exists: false,
            read: State::Initializing,
            fresh_through: None,
            last_current: None,
            seq: 0,
        }
    }

    /// Which component it is.
    pub fn kind(&self) -> ComponentKind {
        self.kind
    }

    /// The state at the latest boundary: `Initializing` until its existence
    /// evidence arrives, then what its reads showed.
    pub fn state(&self) -> State<&T> {
        if !self.exists {
            return State::Initializing;
        }
        match &self.read {
            State::Initializing => State::Initializing,
            State::Current { state } => State::Current { state },
            State::InTransition { since, last } => State::InTransition {
                since: *since,
                last: last.as_ref(),
            },
            State::Ended(Ended::Seen { last_seen, state }) => State::Ended(Ended::Seen {
                last_seen: *last_seen,
                state,
            }),
            State::Ended(Ended::Unsettled) => State::Ended(Ended::Unsettled),
        }
    }

    /// The latest boundary a read completed at, changed or not.
    pub fn fresh_through(&self) -> Option<u64> {
        self.fresh_through
    }

    /// How many reads changed the state.
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// True if its read is registered and it hasn't ended, so every
    /// boundary must read it.
    pub fn live(&self) -> bool {
        self.registered && !matches!(self.read, State::Ended(_))
    }

    /// Registers its read. Fails if it's registered already.
    pub fn register(&mut self) -> Result<(), String> {
        if std::mem::replace(&mut self.registered, true) {
            return Err(format!("{:?}'s read is registered twice", self.kind));
        }
        Ok(())
    }

    /// Records the evidence that it exists, seen at `step`. Fails if its
    /// read isn't registered, since a component that acted must be read.
    pub fn exists(&mut self, step: u64) -> Result<(), String> {
        if !self.registered {
            return Err(format!(
                "{:?}'s existence evidence arrived at step {step} with no read registered",
                self.kind
            ));
        }
        self.exists = true;
        Ok(())
    }

    /// Folds the read at `step`'s boundary. Fails on an unregistered read,
    /// a second read at one boundary, and a read after the component ended.
    pub fn read(&mut self, step: u64, read: Read<T>) -> Result<(), String> {
        self.readable(step)?;
        let next = match (read, &self.read) {
            (Read::Current(state), _) => {
                self.last_current = Some(step);
                State::Current { state }
            }
            (Read::Held { .. }, State::InTransition { since, last }) => State::InTransition {
                since: *since,
                last: last.clone(),
            },
            (Read::Held { .. }, State::Current { state }) => State::InTransition {
                since: step,
                last: Some(state.clone()),
            },
            (Read::Held { .. }, _) => State::InTransition {
                since: step,
                last: None,
            },
            (Read::Gone, State::Current { state }) => State::Ended(Ended::Seen {
                last_seen: self
                    .last_current
                    .expect("a current state had a current read"),
                state: state.clone(),
            }),
            (Read::Gone, _) => State::Ended(Ended::Unsettled),
        };
        self.settle(step, next);
        Ok(())
    }

    /// Ends it at `step`'s boundary, as that boundary's read, for an owner
    /// whose end its own report shows, such as a loop's final snapshot:
    /// seen at `step` with `state` if it's `Some`, or unsettled, as for a
    /// loop cancelled mid-turn. Fails as [`read`](Self::read) does.
    pub fn end(&mut self, step: u64, state: Option<T>) -> Result<(), String> {
        self.readable(step)?;
        let ended = match state {
            Some(state) => {
                self.last_current = Some(step);
                Ended::Seen {
                    last_seen: step,
                    state,
                }
            }
            None => Ended::Unsettled,
        };
        self.settle(step, State::Ended(ended));
        Ok(())
    }

    /// Fails unless `step`'s boundary may read it: registered, not read at
    /// `step` or later already, and not ended.
    fn readable(&self, step: u64) -> Result<(), String> {
        let kind = self.kind;
        if !self.registered {
            return Err(format!("{kind:?} was read at step {step} unregistered"));
        }
        if self.fresh_through.is_some_and(|at| at >= step) {
            return Err(format!("{kind:?} was read twice at step {step}"));
        }
        if matches!(self.read, State::Ended(_)) {
            return Err(format!("{kind:?} was read at step {step} after it ended"));
        }
        Ok(())
    }

    /// Records `next` as `step`'s state, counting it if it changed.
    fn settle(&mut self, step: u64, next: State<T>) {
        if next != self.read {
            self.seq += 1;
            self.read = next;
        }
        self.fresh_through = Some(step);
    }
}

/// A value [`require`] or [`require_every`] hands out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Available<'a, T> {
    /// The state this boundary's read showed.
    Current(&'a T),
    /// The component ended. Terminal.
    Ended {
        /// The boundary of its last current read.
        last_seen: u64,
        /// The state at `last_seen`.
        state: &'a T,
    },
}

/// `component`'s value at this boundary, for a claim about current
/// boundaries (rule 8). `InTransition` and `Ended(Unsettled)` record
/// `Inconclusive::Unsettled` and return `None`, so the check skips the
/// boundary. A later current boundary can still end the property `Held`,
/// with the transition gaps listed in its report. `Initializing` returns
/// `None` and records nothing: a component that doesn't exist yet isn't
/// mid-change, so a check that only ever skips it ends `NotApplicable`, as
/// a property whose source never appears does, or `obligation-open` if the
/// run ends with its obligation pending. Each `Inconclusive` reason means
/// one thing. A `Current` state whose read isn't from this boundary is
/// broken capture, so it records missing evidence, a harness failure.
pub fn require<'c, S: HasView, T: Clone + PartialEq>(
    cx: &mut CheckCtx<'_, S>,
    component: &'c Component<T>,
) -> Option<Available<'c, T>> {
    available(cx, component, Inconclusive::Unsettled)
}

/// As [`require`], for a claim that must hold at every boundary. A boundary
/// in transition records the sticky `Inconclusive::Unobserved`, so the
/// property can't end `Held` over a boundary it never saw. `Initializing`
/// records nothing, as in [`require`], since no boundary is missed before
/// the component exists.
pub fn require_every<'c, S: HasView, T: Clone + PartialEq>(
    cx: &mut CheckCtx<'_, S>,
    component: &'c Component<T>,
) -> Option<Available<'c, T>> {
    available(cx, component, Inconclusive::Unobserved)
}

/// [`require`]'s and [`require_every`]'s shared rule, with `gap` the
/// reason a transition records.
fn available<'c, S: HasView, T: Clone + PartialEq>(
    cx: &mut CheckCtx<'_, S>,
    component: &'c Component<T>,
    gap: Inconclusive,
) -> Option<Available<'c, T>> {
    match component.state() {
        State::Current { state } if component.fresh_through == Some(cx.step) => {
            Some(Available::Current(state))
        }
        State::Current { .. } => {
            stale(cx, component);
            None
        }
        State::Ended(Ended::Seen { last_seen, state }) => {
            Some(Available::Ended { last_seen, state })
        }
        State::Initializing => None,
        State::InTransition { .. } | State::Ended(Ended::Unsettled) => {
            cx.gap(component.kind);
            cx.unsettled(gap);
            None
        }
    }
}

/// Records that `component`'s current state isn't from this boundary.
fn stale<S: HasView, T>(cx: &mut CheckCtx<'_, S>, component: &Component<T>) {
    let detail = format!(
        "{:?} is current only through step {:?}, not step {}",
        component.kind, component.fresh_through, cx.step
    );
    cx.missing_evidence(detail);
}

/// One part of a reconciliation, for [`require_coherent`].
pub trait Part {
    /// Whether the part can be reconciled at `step`.
    fn coherence(&self, step: u64) -> Coherence;

    /// Which component it is.
    fn part_kind(&self) -> ComponentKind;
}

/// Whether a part can be reconciled at a boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Coherence {
    /// Current at the boundary, with no tag mid-creation or mid-release.
    Ready,
    /// Not yet shown to exist.
    Initializing,
    /// Ended after a current read: its last state is known, but it isn't
    /// current.
    Ended,
    /// In transition, ended in transition, or with a tag in flux.
    Unsettled,
    /// Current, but its read isn't from the boundary.
    Stale,
}

impl<T: Clone + PartialEq + Tagged> Part for Component<T> {
    /// `Ready` only for a fresh `Current` state with every tag `Live`.
    fn coherence(&self, step: u64) -> Coherence {
        match self.state() {
            State::Current { .. } if self.fresh_through != Some(step) => Coherence::Stale,
            State::Current { state } if state.in_flux().is_none() => Coherence::Ready,
            State::Initializing => Coherence::Initializing,
            State::Ended(Ended::Seen { .. }) => Coherence::Ended,
            _ => Coherence::Unsettled,
        }
    }

    /// Its kind.
    fn part_kind(&self) -> ComponentKind {
        self.kind
    }
}

/// True if a check across `parts`, which change at separate points, may
/// run at this boundary (rule 5): every part is current, with no tag
/// `Creating` or `Releasing`. Otherwise it returns false. A part that exists
/// but is mid-change records `Inconclusive::Unsettled` and lists a gap. A
/// part still `Initializing`, or ended with a known state, records nothing. A stale
/// part is a harness failure, as in [`require`].
pub fn require_coherent<S: HasView>(cx: &mut CheckCtx<'_, S>, parts: &[&dyn Part]) -> bool {
    let mut coherent = true;
    let mut unsettled = false;
    for part in parts {
        match part.coherence(cx.step) {
            Coherence::Ready => continue,
            Coherence::Stale => cx.missing_evidence(format!(
                "{:?} is current at step {} from an older read",
                part.part_kind(),
                cx.step
            )),
            Coherence::Initializing | Coherence::Ended => {}
            Coherence::Unsettled => {
                cx.gap(part.part_kind());
                unsettled = true;
            }
        }
        coherent = false;
    }
    if unsettled {
        cx.unsettled(Inconclusive::Unsettled);
    }
    coherent
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use super::*;
    use crate::{
        check::{Checker, Phase, StepCtx},
        property::{
            Assumptions, Kind, Property, PropertyId, Registry, RegistryChecker, Source, Sources,
            Status, Verdict,
        },
        HarnessFailure, Identity, ObservationLog,
    };

    /// A component registered and shown to exist at step 1.
    fn existing<T: Clone + PartialEq>() -> Component<T> {
        let mut component = Component::new(ComponentKind::Budget);
        component.register().expect("registered once");
        component.exists(1).expect("its read is registered");
        component
    }

    /// Held by the reactor.
    fn held<T>() -> Read<T> {
        Read::Held {
            holder: "reactor.1".to_owned(),
        }
    }

    /// A component read current at step 9 and gone at step 10 ends seen at
    /// 9, with that state. One in transition at 9 ends unsettled.
    #[test]
    fn a_terminal_state_is_labeled_by_when_it_was_seen() {
        let mut seen = existing();
        seen.read(9, Read::Current(5_u64)).expect("a first read");
        assert_eq!(seen.state(), State::Current { state: &5 });
        seen.read(10, Read::Gone)
            .expect("a read at the next boundary");
        assert_eq!(
            seen.state(),
            State::Ended(Ended::Seen {
                last_seen: 9,
                state: &5
            })
        );
        assert!(!seen.live(), "an ended component needs no more reads");

        let mut unsettled = existing();
        unsettled
            .read(8, Read::Current(5_u64))
            .expect("a first read");
        unsettled
            .read(9, held())
            .expect("a read at the next boundary");
        assert!(matches!(
            unsettled.state(),
            State::InTransition { since: 9, .. }
        ));
        unsettled
            .read(10, Read::Gone)
            .expect("a read at the next boundary");
        assert_eq!(unsettled.state(), State::Ended(Ended::Unsettled));
    }

    /// A component state listing tagged objects.
    #[derive(Clone, Debug, PartialEq)]
    struct Objects(Vec<TagPhase>);

    impl Tagged for Objects {
        fn in_flux(&self) -> Option<TagPhase> {
            self.0
                .iter()
                .copied()
                .find(|phase| *phase != TagPhase::Live)
        }
    }

    /// A harness with nothing to view.
    struct Nothing;

    impl HasView for Nothing {
        type View = ();

        fn view(&self) {}
    }

    /// Never runs.
    fn check_nothing(_: &mut CheckCtx<'_, Nothing>) -> Verdict {
        Verdict::Holds
    }

    /// `require_coherent` over `parts` at `step`, with what it recorded.
    fn coherent(step: u64, parts: &[&dyn Part]) -> (bool, Option<Inconclusive>, usize) {
        let property = Property {
            id: PropertyId::new("test/coherent"),
            claim: "a test reconciliation",
            kind: Kind::Safety,
            assumes: &[],
            needs: &[],
            status: Status::Active,
            check: check_nothing,
            witness: None,
        };
        let log = ObservationLog::default();
        let pick = Identity::peer("owner", "tick");
        let step = StepCtx {
            step,
            pick: &pick,
            enabled: &[],
            phase: Phase::Live,
            now: zakura_scheduler::SimInstant::ZERO,
            status: zakura_scheduler::Status::Running,
            log: &log,
        };
        let mut cx = CheckCtx::new(&step, &Nothing, &property, std::slice::from_ref(&property));
        let coherent = require_coherent(&mut cx, parts);
        let parts = cx.into_parts();
        assert_eq!(parts.missing, None, "every part is fresh");
        (coherent, parts.unsettled, parts.gaps.len())
    }

    /// Reconciliation waits while one part is in transition, another lists
    /// an object still being created, a third doesn't exist yet, or a
    /// fourth ended, and runs once all are ready. Only the first two record
    /// `Unsettled`.
    #[test]
    fn reconciliation_waits_for_coherent_parts() {
        let live = || Objects(vec![TagPhase::Live]);
        let mut moving = existing();
        moving.read(1, Read::Current(live())).expect("a first read");
        moving.read(2, held()).expect("a read at the next boundary");
        assert!(matches!(moving.state(), State::InTransition { .. }));
        let mut creating = existing();
        creating
            .read(
                2,
                Read::Current(Objects(vec![TagPhase::Live, TagPhase::Creating])),
            )
            .expect("a first read");
        assert_eq!(
            creating.state(),
            State::Current {
                state: &Objects(vec![TagPhase::Live, TagPhase::Creating])
            }
        );
        let mut ready = existing();
        ready.read(2, Read::Current(live())).expect("a first read");

        let unsettled = Some(Inconclusive::Unsettled);
        assert_eq!(coherent(2, &[&moving, &ready]), (false, unsettled, 1));
        assert_eq!(coherent(2, &[&creating, &ready]), (false, unsettled, 1));
        assert_eq!(coherent(2, &[&ready]), (true, None, 0));
        let initializing: Component<Objects> = Component::new(ComponentKind::Budget);
        assert_eq!(initializing.state(), State::Initializing);
        assert_eq!(
            coherent(2, &[&initializing, &ready]),
            (false, None, 0),
            "a part that doesn't exist yet isn't mid-change"
        );
        let mut ended = existing();
        ended.read(1, Read::Current(live())).expect("a first read");
        ended
            .read(2, Read::Gone)
            .expect("a read at the next boundary");
        assert!(matches!(ended.state(), State::Ended(Ended::Seen { .. })));
        assert_eq!(
            coherent(2, &[&ended, &ready]),
            (false, None, 0),
            "a part that ended with a known state isn't mid-change"
        );
        creating
            .read(3, Read::Current(Objects(vec![TagPhase::Live; 2])))
            .expect("a read at the next boundary");
        ready
            .read(3, Read::Current(live()))
            .expect("a read at the next boundary");
        assert_eq!(coherent(3, &[&creating, &ready]), (true, None, 0));
    }

    /// A harness viewing one component.
    struct One(Rc<RefCell<Component<u64>>>);

    impl HasView for One {
        type View = Rc<RefCell<Component<u64>>>;

        fn view(&self) -> Self::View {
            self.0.clone()
        }
    }

    /// Holds whenever `require` hands out the component's value.
    fn check_one(cx: &mut CheckCtx<'_, One>) -> Verdict {
        let component = cx.view();
        let component = component.borrow();
        match require(cx, &component) {
            Some(_) => Verdict::Holds,
            None => Verdict::NotApplicable,
        }
    }

    /// `require` on a current state read one boundary ago records missing
    /// evidence, a harness failure, and a fresh read at the next boundary
    /// doesn't erase it.
    #[test]
    fn require_refuses_a_stale_current_value() {
        let component = Rc::new(RefCell::new(existing()));
        let property = Property {
            id: PropertyId::new("test/require"),
            claim: "a test claim on a component",
            kind: Kind::Safety,
            assumes: &[],
            needs: &[Source::Component(ComponentKind::Budget)],
            status: Status::Active,
            check: check_one,
            witness: None,
        };
        let mut checker = RegistryChecker::new(
            Registry::new(vec![property]).expect("one property"),
            Rc::new(One(component.clone())),
            Assumptions::none(),
            &Sources::observed(&[Source::Component(ComponentKind::Budget)]),
        );
        let pick = Identity::peer("owner", "tick");
        let mut log = ObservationLog::default();
        let mut at = |checker: &mut RegistryChecker<One>, step| {
            log.begin(step);
            checker.after_step(&StepCtx {
                step,
                pick: &pick,
                enabled: &[],
                phase: Phase::Live,
                now: zakura_scheduler::SimInstant::ZERO,
                status: zakura_scheduler::Status::Running,
                log: &log,
            });
        };
        let read = |step| {
            component
                .borrow_mut()
                .read(step, Read::Current(5))
                .expect("one read per boundary");
        };
        read(1);
        at(&mut checker, 1);
        assert_eq!(
            checker.harness_failure(),
            None,
            "a fresh read is handed out"
        );
        at(&mut checker, 2);
        read(3);
        at(&mut checker, 3);
        assert_eq!(
            checker.harness_failure(),
            Some(HarnessFailure::MissingEvidence {
                property: "test/require".to_owned(),
                step: 2,
                detail: "Budget is current only through step Some(1), not step 2".to_owned(),
            })
        );
    }

    /// A teardown reason and a component in transition in one evaluation
    /// leave the teardown reason, in either order.
    #[test]
    fn a_state_reason_never_replaces_a_teardown_in_one_evaluation() {
        let mut moving = existing();
        moving.read(1, Read::Current(5_u64)).expect("a first read");
        moving.read(2, held()).expect("a read at the next boundary");
        assert!(matches!(moving.state(), State::InTransition { .. }));
        let property = Property {
            id: PropertyId::new("test/teardown"),
            claim: "a test claim on a component",
            kind: Kind::Safety,
            assumes: &[],
            needs: &[],
            status: Status::Active,
            check: check_nothing,
            witness: None,
        };
        let log = ObservationLog::default();
        let pick = Identity::peer("owner", "tick");
        let step = StepCtx {
            step: 2,
            pick: &pick,
            enabled: &[],
            phase: Phase::Live,
            now: zakura_scheduler::SimInstant::ZERO,
            status: zakura_scheduler::Status::Running,
            log: &log,
        };
        for teardown_first in [true, false] {
            let mut cx = CheckCtx::new(&step, &Nothing, &property, std::slice::from_ref(&property));
            if teardown_first {
                cx.unsettled(Inconclusive::TeardownOpen);
            }
            assert_eq!(require(&mut cx, &moving), None);
            if !teardown_first {
                cx.unsettled(Inconclusive::TeardownOpen);
            }
            assert_eq!(
                cx.into_parts().unsettled,
                Some(Inconclusive::TeardownOpen),
                "teardown recorded first: {teardown_first}"
            );
        }
    }
}
