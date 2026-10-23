//! What a check sees.

use std::ops::Deref;

use super::{
    Kind, LivenessBound, Obligation, ObligationKey, Property, PropertyId, Subject, Verdict,
};
use crate::{check::Violation, Observation, StepCtx};

/// A harness's read-only view of the code under test. Checks read it through
/// [`CheckCtx::view`].
pub trait HasView {
    /// A snapshot of the state checks may read.
    type View;

    /// The view now, after the step.
    fn view(&self) -> Self::View;
}

/// What one property's check sees after a step: the step itself, through
/// `Deref`, plus the harness's view and the property's obligations.
pub struct CheckCtx<'a, S: HasView> {
    step: &'a StepCtx<'a>,
    handle: &'a S,
    property: &'a Property<S>,
    obliged: Vec<Obligation>,
}

impl<'a, S: HasView> CheckCtx<'a, S> {
    /// The context for `property`'s check after `step`.
    pub(crate) fn new(step: &'a StepCtx<'a>, handle: &'a S, property: &'a Property<S>) -> Self {
        CheckCtx {
            step,
            handle,
            property,
            obliged: Vec::new(),
        }
    }

    /// The obligations the check opened, in order.
    pub(crate) fn into_obligations(self) -> Vec<Obligation> {
        self.obliged
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
    /// witness is this step.
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

    /// A violation of the property at this step, with this step as its
    /// witness.
    pub fn violated(&self, expected: impl Into<String>, actual: impl Into<String>) -> Verdict {
        Verdict::Violated(Violation {
            property: self.id().to_string(),
            step: self.step.step,
            witness: vec![self.step.step],
            expected: expected.into(),
            actual: actual.into(),
        })
    }
}

impl<'a, S: HasView> Deref for CheckCtx<'a, S> {
    type Target = StepCtx<'a>;

    /// The step the check runs after.
    fn deref(&self) -> &StepCtx<'a> {
        self.step
    }
}
