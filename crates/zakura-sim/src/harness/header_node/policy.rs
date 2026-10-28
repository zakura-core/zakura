//! The header node's outcome predicates (M4-B2): the shared grammar in
//! [`crate::harness::policy`] over header storage's operation kinds and
//! outcomes.
//!
//! [`Predicates`] counts the storage facts, reactor events and link effects
//! the run has observed. Only [`Predicates::record`],
//! [`Predicates::record_reactor`] and [`Predicates::record_link`] change it,
//! and the run calls them from `observe`.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use zakura_scheduler::SimInstant;
use zakura_storage::header::{controlled::PendingOp, Fault, Outcome};

use super::Base;
pub use crate::harness::policy::LinkEffect;
use crate::{
    harness::policy::{self, Event, Family},
    observe::{ReactorCall, ReactorFact, StorageFact, StorageRequest},
    Boundary,
};

/// A storage operation's kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OpKind {
    /// `continuation_locator`.
    Locator,
    /// `vct_repair_context`.
    VctContext,
    /// `acquire_header_path`.
    Acquire,
    /// `read_header_path`.
    Read,
    /// `release_header_path`.
    Release,
    /// `prepare_header_target`.
    Prepare,
    /// `apply_header_target`.
    Apply,
    /// `selected_chain_contains`.
    Membership,
}

impl OpKind {
    /// `request`'s kind, and the session it's attributed to, if any.
    /// Locators and membership reads carry none.
    pub(crate) fn of(request: &StorageRequest) -> (OpKind, Option<u64>) {
        match request {
            StorageRequest::ContinuationLocator => (OpKind::Locator, None),
            StorageRequest::VctRepairContext { owner, .. } => {
                (OpKind::VctContext, Some(owner.session_id))
            }
            StorageRequest::AcquireHeaderPath { session_id, .. } => {
                (OpKind::Acquire, Some(*session_id))
            }
            StorageRequest::ReadHeaderPath { lease, .. } => {
                (OpKind::Read, lease.as_ref().map(|lease| lease.session_id))
            }
            StorageRequest::ReleaseHeaderPath { lease, .. } => (
                OpKind::Release,
                lease.as_ref().map(|lease| lease.session_id),
            ),
            StorageRequest::PrepareHeaderTarget { owner, .. } => {
                (OpKind::Prepare, Some(owner.session_id))
            }
            StorageRequest::ApplyHeaderTarget { owner, .. } => {
                (OpKind::Apply, Some(owner.session_id))
            }
            StorageRequest::SelectedChainContains { .. } => (OpKind::Membership, None),
            StorageRequest::QueryNeededBlocks { .. }
            | StorageRequest::SubmitBlock { .. }
            | StorageRequest::RecordBody { .. }
            | StorageRequest::Misbehavior { .. }
            | StorageRequest::ReadRange { .. } => {
                unreachable!("a header node's storage logs only header requests")
            }
        }
    }
}

/// An outcome a hold may offer once it opens.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Allow {
    /// Run the operation against the engine.
    Engine,
    /// The adapter's timeout, legal once its deadline passes.
    Timeout,
    /// Storage unavailable.
    Unavailable,
    /// A local resource fault.
    LocalResource,
    /// The adapter's input check refused the request.
    Rejected,
}

impl Allow {
    /// The storage outcome.
    fn outcome(self) -> Outcome {
        match self {
            Allow::Engine => Outcome::Engine,
            Allow::Timeout => Outcome::Fault(Fault::Timeout),
            Allow::Unavailable => Outcome::Fault(Fault::Unavailable),
            Allow::LocalResource => Outcome::Fault(Fault::LocalResource),
            Allow::Rejected => Outcome::Rejected,
        }
    }
}

/// The header node's operations, as the shared grammar's [`Family`].
#[derive(Clone, Copy, Debug)]
pub struct Header;

impl Family for Header {
    type Kind = OpKind;
    type Allow = Allow;
    type Outcome = Outcome;
    type Key = u64;

    fn allows(allow: Allow, outcome: Outcome) -> bool {
        allow.outcome() == outcome
    }
}

/// Which header operations a hold or trigger names.
pub type Select = policy::Select<OpKind>;

/// What a header hold or control step waits for.
pub type Trigger = policy::Trigger<OpKind>;

/// A hold on header operations.
pub type Hold = policy::Hold<OpKind, Allow>;

/// The header node's holds and everything they count.
pub type Predicates = policy::Predicates<Header>;

/// Why a header scenario can't wait for `trigger`, if no event the node
/// records drives it: [`Trigger::ReplyQueued`](policy::Trigger::ReplyQueued).
/// A completion stores the reply for its call's future, so `completed`
/// counts that moment.
pub fn undriven(trigger: &Trigger) -> Option<String> {
    let found = trigger.find(&|trigger| matches!(trigger, policy::Trigger::ReplyQueued(_)))?;
    Some(format!(
        "a header scenario can't wait for {found:?}: a completion stores its reply, so \
         `completed` counts that moment"
    ))
}

impl Predicates {
    /// Counts one storage fact of the node's port.
    pub fn record(&mut self, fact: &StorageFact) {
        match fact {
            StorageFact::Submitted { id, request, .. } => {
                let (kind, session) = OpKind::of(request);
                self.submit(*id, kind, session);
            }
            StorageFact::CallerDropped { id } => self.event(Event::CallerDropped, *id),
            StorageFact::Completed { id, .. } => self.event(Event::Completed, *id),
            StorageFact::ReplyTaken { id } => self.event(Event::ReplyTaken, *id),
            _ => {}
        }
    }

    /// Counts one event of the node's reactor, as its tap logged it, with
    /// each `CallEnded`'s paired operation.
    pub fn record_reactor(&mut self, event: &ReactorFact) {
        match event {
            ReactorFact::Dispatched { call, port } => {
                let (kind, session) = match port {
                    ReactorCall::Locator { session_id, .. } => (OpKind::Locator, *session_id),
                    ReactorCall::VctContext { owner, .. } => (OpKind::VctContext, owner.session_id),
                    ReactorCall::Prepare { owner, .. } => (OpKind::Prepare, owner.session_id),
                    ReactorCall::Apply { owner, .. } => (OpKind::Apply, owner.session_id),
                };
                self.dispatched(*call, kind, session);
            }
            ReactorFact::CallEnded { call, op, .. } => self.call_ended(*call, *op),
            ReactorFact::Handled { call, .. } => self.handled(*call),
            _ => {}
        }
    }

    /// Whether `outcome` is offered for `op`: by its hold once open, or by
    /// `base` for an operation no hold picked.
    pub fn offers(&self, base: Base, op: &PendingOp, outcome: Outcome) -> bool {
        self.held_offers(op.id.0, op.since_submit, outcome)
            .unwrap_or_else(|| base.offers(&op.enabled, outcome))
    }

    /// Where time must stop for `op` at `now`: a held operation's unreached
    /// virtual times and its deadline, or else the base `bound`.
    pub fn boundaries(
        &self,
        bound: Option<Duration>,
        op: &PendingOp,
        now: SimInstant,
        out: &mut Vec<Boundary>,
    ) {
        self.op_boundaries(bound, op.id.0, op.since_submit, now, out);
    }
}
