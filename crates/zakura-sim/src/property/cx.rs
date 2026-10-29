//! What a check sees.

use std::ops::Deref;

use super::{
    Cardinality, Inconclusive, Kind, LivenessBound, Obligation, ObligationKey, Property,
    PropertyId, Subject, Verdict,
};
use crate::{check::Violation, model::LabelledFrame, Observation, StepCtx};

/// A harness's read-only view of the code under test. Checks read it through
/// [`CheckCtx::view`].
///
/// The view is actual state: what the owners hold after the step, read
/// without a guard event or a rejection. It never defines what the protocol
/// allows; a check compares it against an independent model or bound.
///
/// The registry reads the view at the boundary after each step, the
/// coherent boundary every check shares. A write that a task makes and
/// undoes with no preemption point between them is invisible there. A claim
/// about such intermediate states needs its own observation boundary.
pub trait HasView {
    /// A snapshot of the state checks may read.
    type View;

    /// The view now, after the step.
    fn view(&self) -> Self::View;

    /// When the state [`view`](HasView::view) returns was captured. The
    /// registry fails the run as a harness failure when a check that needs
    /// the view would read state captured before the step's boundary.
    fn captured(&self) -> Capture {
        Capture::Live
    }
}

/// When a view's state was captured.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Capture {
    /// `view` reads the owners' state when called, so it is always the state
    /// at the current boundary.
    Live,
    /// `view` returns the snapshot the owners took at the boundary after
    /// this step.
    AfterStep(u64),
    /// No snapshot exists yet.
    Missing,
}

impl Capture {
    /// True if the view shows the state at the boundary after `step`.
    pub fn fresh_at(self, step: u64) -> bool {
        match self {
            Capture::Live => true,
            Capture::AfterStep(at) => at == step,
            Capture::Missing => false,
        }
    }
}

/// What one property's check sees after a step: the step itself, through
/// `Deref`, plus the harness's view and the property's obligations.
pub struct CheckCtx<'a, S: HasView> {
    step: &'a StepCtx<'a>,
    handle: &'a S,
    property: &'a Property<S>,
    /// Every property in the registry, this one included.
    registered: &'a [Property<S>],
    obliged: Vec<Obligation>,
    recorded: Vec<Violation>,
    missing: Option<String>,
    labelled: Vec<LabelledFrame>,
    peaks: Vec<(&'static str, u64)>,
    unsettled: Option<Inconclusive>,
}

/// What a check left behind besides its verdict.
pub(crate) struct CheckParts {
    /// The obligations it opened, in order.
    pub(crate) obliged: Vec<Obligation>,
    /// The violations it recorded, in order.
    pub(crate) recorded: Vec<Violation>,
    /// The first evidence it found missing.
    pub(crate) missing: Option<String>,
    /// The frames it labelled, in order.
    pub(crate) labelled: Vec<LabelledFrame>,
    /// The quantities it measured, by name.
    pub(crate) peaks: Vec<(&'static str, u64)>,
    /// Why its trigger fired but it couldn't judge yet, if so.
    pub(crate) unsettled: Option<Inconclusive>,
}

impl<'a, S: HasView> CheckCtx<'a, S> {
    /// The context for `property`'s check after `step`, in a registry of
    /// `registered`.
    pub(crate) fn new(
        step: &'a StepCtx<'a>,
        handle: &'a S,
        property: &'a Property<S>,
        registered: &'a [Property<S>],
    ) -> Self {
        CheckCtx {
            step,
            handle,
            property,
            registered,
            obliged: Vec::new(),
            recorded: Vec::new(),
            missing: None,
            labelled: Vec::new(),
            peaks: Vec::new(),
            unsettled: None,
        }
    }

    /// What the check left behind besides its verdict.
    pub(crate) fn into_parts(self) -> CheckParts {
        CheckParts {
            obliged: self.obliged,
            recorded: self.recorded,
            missing: self.missing,
            labelled: self.labelled,
            peaks: self.peaks,
            unsettled: self.unsettled,
        }
    }

    /// True if the registry holds a property with ID `id`.
    pub fn is_registered(&self, id: &str) -> bool {
        self.registered
            .iter()
            .any(|property| property.id.as_str() == id)
    }

    /// The property being checked.
    pub fn id(&self) -> &PropertyId {
        &self.property.id
    }

    /// The harness's view after the step.
    pub fn view(&self) -> S::View {
        self.handle.view()
    }

    /// An obligation on `subject`, opened at this step under the property's
    /// liveness bound. `trigger` is the trigger's position in
    /// [`StepCtx::observations`], as [`Obligation::trigger`] describes. Its
    /// witness is this step, and one observation discharges only it
    /// ([`Cardinality::One`]); set `cardinality` for a shared discharge.
    ///
    /// # Panics
    ///
    /// If the property isn't a liveness property.
    pub fn obligation(
        &self,
        trigger: Option<usize>,
        subject: Subject,
        discharged_by: fn(&ObligationKey, &Observation) -> bool,
    ) -> Obligation {
        let Kind::Liveness { bound } = self.property.kind else {
            panic!(
                "{} opens an obligation, but isn't a liveness property",
                self.id()
            );
        };
        Obligation {
            key: ObligationKey {
                property: self.id().clone(),
                subject,
            },
            opened_at: self.step.step,
            opened_time: self.step.now,
            trigger,
            bound,
            discharged_by,
            cardinality: Cardinality::One,
            witness: vec![self.step.step],
        }
    }

    /// Registers something the node must still do. The registry checks it
    /// after every step and at run end.
    ///
    /// # Panics
    ///
    /// If its key names another property, or its bound is an `Ops` bound
    /// that names an object or counts polls.
    pub fn oblige(&mut self, obligation: Obligation) {
        assert_eq!(
            &obligation.key.property,
            self.id(),
            "a check opens obligations only for its own property"
        );
        assert!(
            !matches!(
                obligation.bound,
                LivenessBound::Ops {
                    object: Some(_),
                    ..
                }
            ),
            "Ops bounds that name an object wait for zakura#1643"
        );
        assert!(
            !matches!(
                obligation.bound,
                LivenessBound::Ops {
                    op: zakura_scheduler::Op::Poll,
                    ..
                }
            ),
            "no observation counts toward an Ops bound on polls"
        );
        self.obliged.push(obligation);
    }

    /// Records a frame a role model labelled. The registry passes it on as
    /// a [`CheckEvent::Label`](super::CheckEvent::Label) after the check.
    pub fn label(&mut self, frame: LabelledFrame) {
        self.labelled.push(frame);
    }

    /// A violation of the property at this step, with this step as its
    /// witness.
    pub fn violated(&self, expected: impl Into<String>, actual: impl Into<String>) -> Verdict {
        Verdict::Violated(self.violation(expected.into(), actual.into()))
    }

    /// Records one more violation at this step, after the one a `Violated`
    /// verdict carries, so a check that finds several at one boundary, such
    /// as two punished connections, reports each. The registry treats a
    /// recorded violation as the check's even if the verdict is `Holds`.
    pub fn record_violation(&mut self, expected: impl Into<String>, actual: impl Into<String>) {
        let violation = self.violation(expected.into(), actual.into());
        self.recorded.push(violation);
    }

    /// Records the value `quantity` reached by this step. The property's
    /// report keeps each quantity's largest value over the run, such as the
    /// most bytes an allowance held (properties.md, Retained bytes).
    pub fn peak(&mut self, quantity: &'static str, value: u64) {
        self.peaks.push((quantity, value));
    }

    /// Records that the property's trigger fired but the run hasn't yet
    /// shown what the check needs to judge it, such as a teardown's
    /// settlement. Only the latest evaluation counts: if the run ends after
    /// one that recorded `reason`, the property is `Inconclusive` with it,
    /// unless it was violated.
    pub fn unsettled(&mut self, reason: Inconclusive) {
        self.unsettled = Some(reason);
    }

    /// Records that a source the property needs lacks evidence the check
    /// requires, such as a judged frame with no label. The registry fails
    /// the run as a harness failure, because a check that skipped the
    /// evidence would read as `Held`.
    pub fn missing_evidence(&mut self, detail: impl Into<String>) {
        self.missing.get_or_insert_with(|| detail.into());
    }

    /// A violation at this step, with this step as its witness.
    fn violation(&self, expected: String, actual: String) -> Violation {
        Violation {
            property: self.id().to_string(),
            step: self.step.step,
            witness: vec![self.step.step],
            expected,
            actual,
        }
    }
}

impl<'a, S: HasView> Deref for CheckCtx<'a, S> {
    type Target = StepCtx<'a>;

    /// The step the check runs after.
    fn deref(&self) -> &StepCtx<'a> {
        self.step
    }
}
