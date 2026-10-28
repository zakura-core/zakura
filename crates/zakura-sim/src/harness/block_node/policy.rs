//! The block node's outcome predicates: the shared grammar in
//! [`crate::harness::policy`] over block storage's operations, and over the
//! mirrors' range reads, so a scenario can delay a mirror's serving too.
//!
//! A submission has two stages, and a hold picks each one afresh: its
//! admission as [`BlockKind::Admit`], then its verdict as
//! [`BlockKind::Apply`]. Its session is its owner's.
//!
//! A query's, submission's or record's reply goes to block sync's handle,
//! which queues it for the reactor. The block service sees it queued, which
//! [`Trigger::ReplyQueued`](policy::Trigger::ReplyQueued) counts, and
//! nothing the node records sees the reactor take or handle it. So a block
//! scenario can't wait for [`Trigger::ReplyTaken`](policy::Trigger::ReplyTaken)
//! on those replies, or for [`Trigger::Handled`](policy::Trigger::Handled)
//! at all: [`undriven`] rejects both. A range read's caller takes its own
//! reply, which `ReplyTaken` counts.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use zakura_scheduler::SimInstant;
use zakura_storage::block::{controlled::PendingOp, BlockOutcome, Verdict};

use super::Base;
use crate::{
    harness::policy::{self, Event, Family},
    observe::{StorageFact, StorageRequest},
    Boundary,
};

/// A block operation's kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BlockKind {
    /// `QueryNeededBlocks`.
    Query,
    /// A queued `SubmitBlock`, waiting for the apply limits.
    Admit,
    /// An admitted `SubmitBlock`, waiting for its verdict.
    Apply,
    /// One of the four body evidence records.
    Record,
    /// The node's serving range read.
    Read,
    /// A mirror's serving range read.
    MirrorRead,
}

impl BlockKind {
    /// `request`'s kind at its first stage, and the session it's attributed
    /// to, if any. `None` for a misbehavior report, which the run never
    /// completes.
    fn of(request: &StorageRequest) -> Option<(BlockKind, Option<u64>)> {
        match request {
            StorageRequest::QueryNeededBlocks { .. } => Some((BlockKind::Query, None)),
            StorageRequest::SubmitBlock { owner, .. } => {
                Some((BlockKind::Admit, Some(owner.session_id)))
            }
            StorageRequest::RecordBody { .. } => Some((BlockKind::Record, None)),
            StorageRequest::ReadRange { .. } => Some((BlockKind::Read, None)),
            StorageRequest::Misbehavior { .. } => None,
            StorageRequest::ContinuationLocator
            | StorageRequest::VctRepairContext { .. }
            | StorageRequest::AcquireHeaderPath { .. }
            | StorageRequest::ReadHeaderPath { .. }
            | StorageRequest::ReleaseHeaderPath { .. }
            | StorageRequest::PrepareHeaderTarget { .. }
            | StorageRequest::ApplyHeaderTarget { .. }
            | StorageRequest::SelectedChainContains { .. } => {
                unreachable!("a block node's storage logs only block requests")
            }
        }
    }
}

/// An outcome a block hold may offer once it opens. A verdict class allows
/// every kind within it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BlockAllow {
    /// A query's answer.
    Answer,
    /// A query's or record's state failure.
    Fail,
    /// A queued submission's admission.
    Admit,
    /// A committed verdict.
    Committed,
    /// A duplicate verdict.
    Duplicate,
    /// A payload-mismatch verdict, of any commitment.
    PayloadMismatch,
    /// A consensus-invalid verdict.
    ConsensusInvalid,
    /// A retryable verdict, of any kind.
    Retryable,
    /// A commit whose reply says the verifier was unavailable.
    Ambiguous,
    /// A record's engine transition.
    Apply,
    /// A served range read, the node's or a mirror's.
    Serve,
    /// A failed range read.
    Error,
}

/// What tells one block operation apart from every other in a run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum BlockKey {
    /// The node's block service's operation.
    Service(u64),
    /// A mirror's range read, numbered across every mirror.
    Mirror(u64),
}

/// The block node's operations, as the shared grammar's [`Family`].
#[derive(Clone, Copy, Debug)]
pub struct Block;

impl Family for Block {
    type Kind = BlockKind;
    type Allow = BlockAllow;
    type Outcome = BlockOutcome;
    type Key = BlockKey;

    fn allows(allow: BlockAllow, outcome: BlockOutcome) -> bool {
        matches!(
            (allow, outcome),
            (BlockAllow::Answer, BlockOutcome::Answer)
                | (BlockAllow::Fail, BlockOutcome::Fail)
                | (BlockAllow::Admit, BlockOutcome::Admit)
                | (BlockAllow::Apply, BlockOutcome::Apply)
                | (BlockAllow::Serve, BlockOutcome::Serve)
                | (BlockAllow::Error, BlockOutcome::Error)
                | (
                    BlockAllow::Committed,
                    BlockOutcome::Verdict(Verdict::Committed)
                )
                | (
                    BlockAllow::Duplicate,
                    BlockOutcome::Verdict(Verdict::Duplicate)
                )
                | (
                    BlockAllow::PayloadMismatch,
                    BlockOutcome::Verdict(Verdict::PayloadMismatch(_))
                )
                | (
                    BlockAllow::ConsensusInvalid,
                    BlockOutcome::Verdict(Verdict::ConsensusInvalid)
                )
                | (
                    BlockAllow::Retryable,
                    BlockOutcome::Verdict(Verdict::Retryable(_))
                )
                | (
                    BlockAllow::Ambiguous,
                    BlockOutcome::Verdict(Verdict::Ambiguous)
                )
        )
    }
}

/// Which block operations a hold or trigger names.
pub type Select = policy::Select<BlockKind>;

/// What a block hold or control step waits for.
pub type Trigger = policy::Trigger<BlockKind>;

/// A hold on block operations.
pub type Hold = policy::Hold<BlockKind, BlockAllow>;

/// The block node's holds and everything they count.
pub type Predicates = policy::Predicates<Block>;

/// Why a block scenario can't wait for `trigger`, if no event the node
/// records drives it: [`Trigger::Handled`](policy::Trigger::Handled), or
/// [`Trigger::ReplyTaken`](policy::Trigger::ReplyTaken) on anything but a
/// range read. The module doc says why.
pub fn undriven(trigger: &Trigger) -> Option<String> {
    let found = trigger.find(&|trigger| match trigger {
        policy::Trigger::Handled(_) => true,
        policy::Trigger::ReplyTaken(select) => {
            !matches!(select.kind, Some(BlockKind::Read | BlockKind::MirrorRead))
        }
        _ => false,
    })?;
    Some(match found {
        policy::Trigger::Handled(_) => format!(
            "a block scenario can't wait for {found:?}: nothing the block node records \
             witnesses block sync handling a reply"
        ),
        _ => format!(
            "a block scenario can't wait for {found:?}: only a range read's caller takes its \
             own reply, so its select must name the `read` or `mirror-read` kind. Block sync's \
             handle queues every other reply, which `reply-queued` counts"
        ),
    })
}

/// What happened to one mirror range read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MirrorEvent {
    /// Read `id` arrived.
    Submitted(u64),
    /// The run served read `id`.
    Served(u64),
    /// Read `id`'s caller took its reply.
    Taken(u64),
    /// Read `id`'s caller dropped its call or its reply.
    Dropped(u64),
}

impl Predicates {
    /// Counts one fact of the node's block storage. An admission starts a
    /// submission's verdict stage.
    pub fn record(&mut self, fact: &StorageFact) {
        let key = |id: &u64| BlockKey::Service(*id);
        match fact {
            StorageFact::BlockSubmitted { id, request, .. } => {
                if let Some((kind, session)) = BlockKind::of(request) {
                    self.submit(key(id), kind, session);
                }
            }
            StorageFact::Admitted { id } => self.restage(key(id), BlockKind::Apply),
            StorageFact::BlockCompleted { id, .. } => self.event(Event::Completed, key(id)),
            StorageFact::ReplyDelivered { id } => self.event(Event::ReplyQueued, key(id)),
            StorageFact::ReadTaken { id } => self.event(Event::ReplyTaken, key(id)),
            StorageFact::CallerDropped { id } | StorageFact::ReadDropped { id } => {
                self.event(Event::CallerDropped, key(id))
            }
            _ => {}
        }
    }

    /// Counts one mirror read event.
    pub fn record_mirror(&mut self, event: MirrorEvent) {
        match event {
            MirrorEvent::Submitted(id) => {
                self.submit(BlockKey::Mirror(id), BlockKind::MirrorRead, None)
            }
            MirrorEvent::Served(id) => self.event(Event::Completed, BlockKey::Mirror(id)),
            MirrorEvent::Taken(id) => self.event(Event::ReplyTaken, BlockKey::Mirror(id)),
            MirrorEvent::Dropped(id) => self.event(Event::CallerDropped, BlockKey::Mirror(id)),
        }
    }

    /// Whether `outcome` is offered for `op`, `since` its current stage
    /// began: by its hold once open, or by `base` for an operation no hold
    /// picked.
    pub fn offers(
        &self,
        base: Base,
        op: &PendingOp,
        since: Duration,
        outcome: BlockOutcome,
    ) -> bool {
        self.held_offers(BlockKey::Service(op.id.0), since, outcome)
            .unwrap_or_else(|| base.offers(&op.enabled, outcome))
    }

    /// Whether mirror read `id`, `since` its arrival, may be served: once
    /// its hold opens and offers [`BlockAllow::Serve`], or at once if no hold
    /// picked it.
    pub fn serves(&self, id: u64, since: Duration) -> bool {
        self.held_offers(BlockKey::Mirror(id), since, BlockOutcome::Serve)
            .unwrap_or(true)
    }

    /// Where time must stop for operation `key`, `since` its current stage
    /// began, at `now`: a held operation's unreached virtual times and its
    /// deadline, or else the base `bound`.
    pub fn boundaries(
        &self,
        bound: Option<Duration>,
        key: BlockKey,
        since: Duration,
        now: SimInstant,
        out: &mut Vec<Boundary>,
    ) {
        self.op_boundaries(bound, key, since, now, out);
    }
}
