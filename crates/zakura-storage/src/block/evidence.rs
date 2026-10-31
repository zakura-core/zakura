//! What the controlled block service publishes about each operation, its
//! actual owner state, and two models that check them.
//!
//! [`OwnerModel`] rebuilds the owner state from the facts and checks each
//! transaction against the facts its head requires, as the header model
//! does. It then compares its state with the service's actual
//! [`OwnerState`]. [`ViewModel`] checks the committed views block sync
//! receives against full state.

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU64,
};

use zakura_chain::block;
use zakura_header_chain::{
    ApplyResult, BodyRuleId, BodyUnavailableSummary, BodyWorkAuthority, BodyWorkEpoch,
    BodyWorkOwner, CommittedHeaderChainView, EvidenceId, Frontier, SourceId, StateVersion,
    TransientBodyFailureKind, VerifiedChangeCause,
};
use zakura_network::zakura::{BlockApplyResult, BlockApplyToken, BlockSyncAction, ZakuraPeerId};

use super::{
    controlled::{Completion, Fact, FactKind, OpId},
    ApplyClass, BlockOp, BlockOutcome, BlockRequest, RetryKind, Stage, Verdict,
};
use crate::full_state::VerifiedChain;

/// What an operation asks for, and who owns it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RequestIdentity {
    /// A missing-body query.
    Query {
        /// Its ID.
        query_id: NonZeroU64,
        /// The first height asked for.
        from: block::Height,
        /// How many heights.
        limit: u32,
        /// The body-work scope it was asked under.
        scope: BodyWorkAuthority,
        /// Block sync's best header height then.
        best_header_tip: block::Height,
    },
    /// A submitted body.
    Submit {
        /// Its owner.
        owner: BodyWorkOwner,
        /// Its supplier.
        source: SourceId,
        /// Its token.
        token: BlockApplyToken,
        /// Its coinbase height, which a routine always checks.
        height: Option<block::Height>,
        /// Its hash.
        hash: block::Hash,
        /// Its parent's hash.
        parent: block::Hash,
        /// The verifier it goes to.
        class: ApplyClass,
        /// The submitted block's decoded size,
        /// [`block::Block::attributed_memory_size_bytes`].
        decoded_bytes: u64,
    },
    /// A body-evidence record.
    Record {
        /// Which record.
        op: BlockOp,
        /// The version block sync observed.
        expected_version: StateVersion,
        /// The header it's about.
        hash: block::Hash,
        /// The record's evidence.
        evidence: EvidenceId,
        /// The broken rule, for an invalid body.
        rule: Option<BodyRuleId>,
        /// The supplier, for an invalid body.
        source: Option<SourceId>,
        /// The failure's kind, for an unavailable body.
        failure: Option<TransientBodyFailureKind>,
        /// The retry episode's summary, for every record but an invalid
        /// body.
        availability: Option<BodyUnavailableSummary>,
    },
    /// A misbehavior report.
    Misbehavior {
        /// The peer.
        peer: ZakuraPeerId,
        /// The reason, as its `Debug` text.
        reason: String,
    },
    /// A range read.
    Read {
        /// The first height.
        start_height: block::Height,
        /// How many blocks at most.
        count: u32,
        /// How many body bytes at most.
        max_body_bytes: u32,
    },
}

impl RequestIdentity {
    /// The identity of `request`, whose submitted body is `class`.
    pub fn of(request: &BlockRequest, class: ApplyClass) -> Self {
        match request {
            BlockRequest::Action(action) => Self::of_action(action, class),
            BlockRequest::ReadRange(read) => RequestIdentity::Read {
                start_height: read.start_height,
                count: read.count,
                max_body_bytes: read.max_body_bytes,
            },
        }
    }

    /// The identity of `action`, whose submitted body is `class`.
    pub fn of_action(action: &BlockSyncAction, class: ApplyClass) -> Self {
        match action {
            BlockSyncAction::QueryNeededBlocks {
                query_id,
                from,
                limit,
                best_header_tip,
                scope,
            } => RequestIdentity::Query {
                query_id: *query_id,
                from: *from,
                limit: *limit,
                scope: *scope,
                best_header_tip: *best_header_tip,
            },
            BlockSyncAction::SubmitBlock {
                owner,
                source,
                token,
                block,
            } => RequestIdentity::Submit {
                owner: *owner,
                source: *source,
                token: *token,
                height: block.coinbase_height(),
                hash: block.hash(),
                parent: block.header.previous_block_hash,
                class,
                decoded_bytes: block.attributed_memory_size_bytes(),
            },
            BlockSyncAction::RecordBodyUnavailable {
                expected_version,
                failure,
            } => RequestIdentity::Record {
                op: BlockOp::RecordBodyUnavailable,
                expected_version: *expected_version,
                hash: failure.hash,
                evidence: failure.evidence,
                rule: None,
                source: None,
                failure: Some(failure.kind),
                availability: Some(failure.availability),
            },
            BlockSyncAction::RecordBodyInvalid {
                expected_version,
                invalid,
            } => RequestIdentity::Record {
                op: BlockOp::RecordBodyInvalid,
                expected_version: *expected_version,
                hash: invalid.hash,
                evidence: invalid.evidence,
                rule: Some(invalid.rule.clone()),
                source: Some(invalid.source),
                failure: None,
                availability: None,
            },
            BlockSyncAction::RestartBodyAvailability {
                expected_version,
                discovery:
                    zakura_header_chain::BodySupplierDiscovered {
                        hash,
                        evidence,
                        availability,
                    },
            }
            | BlockSyncAction::RetryBodyAvailability {
                expected_version,
                retry:
                    zakura_header_chain::OperatorBodyRetry {
                        hash,
                        evidence,
                        availability,
                    },
            } => RequestIdentity::Record {
                op: BlockOp::of(action),
                expected_version: *expected_version,
                hash: *hash,
                evidence: *evidence,
                rule: None,
                source: None,
                failure: None,
                availability: Some(*availability),
            },
            BlockSyncAction::Misbehavior { peer, reason } => RequestIdentity::Misbehavior {
                peer: peer.clone(),
                reason: format!("{reason:?}"),
            },
        }
    }

    /// The submitted body's hash, for a submission.
    pub fn submitted_hash(&self) -> Option<block::Hash> {
        match self {
            RequestIdentity::Submit { hash, .. } => Some(*hash),
            _ => None,
        }
    }

    /// The operation this identity belongs to.
    pub fn op(&self) -> BlockOp {
        match self {
            RequestIdentity::Query { .. } => BlockOp::QueryNeededBlocks,
            RequestIdentity::Submit { .. } => BlockOp::SubmitBlock,
            RequestIdentity::Record { op, .. } => *op,
            RequestIdentity::Misbehavior { .. } => BlockOp::Misbehavior,
            RequestIdentity::Read { .. } => BlockOp::ReadRange,
        }
    }
}

/// What the engine said about one record attempt.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum RecordResult {
    /// It committed.
    Committed,
    /// It changed nothing.
    NoChange,
    /// Its version was stale.
    Stale,
    /// It stalled on the engine's resource limits.
    ResourceStalled,
    /// The engine refused it.
    Refused,
}

impl RecordResult {
    /// The result of an attempt the engine answered with `result`.
    pub fn of(result: &ApplyResult) -> Self {
        match result {
            ApplyResult::Committed => RecordResult::Committed,
            ApplyResult::NoChange(_) => RecordResult::NoChange,
            ApplyResult::Stale(_) => RecordResult::Stale,
            ApplyResult::ResourceStalled(_) => RecordResult::ResourceStalled,
        }
    }
}

/// What an operation's completion returned.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResultIdentity {
    /// A queued submission went to the verifier.
    Admitted,
    /// A query's answer.
    NeededBlocks {
        /// The highest body on the selected header chain.
        body_anchor: Frontier,
        /// The missing bodies' heights and hashes.
        blocks: Vec<(block::Height, block::Hash)>,
    },
    /// A query's failure.
    NeededBlocksFailed,
    /// A verdict.
    Verdict {
        /// The verdict.
        verdict: Verdict,
        /// The result block sync reads from it.
        result: BlockApplyResult,
    },
    /// A record's attempts, in order. `record_body_invalid` retries a stale
    /// version up to 3 times.
    Recorded(Vec<RecordResult>),
    /// A record's state error, which changed nothing.
    RecordFailed,
    /// A range read's blocks.
    Served {
        /// The served heights.
        heights: Vec<block::Height>,
        /// Their serialized bytes in all.
        bytes: u64,
    },
    /// A range read's error.
    ReadFailed,
}

/// A published view, as a fact records it.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ViewIdentity {
    /// The engine version it shows.
    pub state_version: StateVersion,
    /// Its body-work epoch.
    pub epoch: BodyWorkEpoch,
    /// Its hint revision.
    pub hint_revision: u64,
    /// Its verified tip.
    pub verified_best: Frontier,
    /// Its selected tip.
    pub header_best: Frontier,
}

impl ViewIdentity {
    /// The identity of `view`.
    pub fn of(view: &CommittedHeaderChainView) -> Self {
        ViewIdentity {
            state_version: view.snapshot.state_version,
            epoch: view.body_work_epoch,
            hint_revision: view.body_size_hint_revision,
            verified_best: view.snapshot.frontiers.verified_best,
            header_best: view.snapshot.frontiers.header_best,
        }
    }
}

/// Where an operation is, as [`OwnerState`] lists it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OperationStage {
    /// A query, record or read waiting for its completion. `caller_waiting`
    /// is whether a read's caller still polls it, and always `true` for an
    /// action, whose reply goes to the handle.
    Pending {
        /// Whether the caller still waits.
        caller_waiting: bool,
    },
    /// A submission waiting for admission.
    Queued,
    /// A submission at the verifier. `replaced` is whether a later
    /// admission of the same hash replaced it. Full state's queue replaced
    /// it, so the flag is history that no container holds, and stays set
    /// after the later admission finishes, unless that admission reports a
    /// duplicate: full state either replaces the older copy or refuses the
    /// newer one.
    Admitted {
        /// Whether a later admission replaced it.
        replaced: bool,
    },
    /// A read whose reply waits for its caller to take it.
    Replied,
}

impl OperationStage {
    /// The contract stage, or `None` once only the caller can end it.
    pub fn contract_stage(&self) -> Option<Stage> {
        match self {
            OperationStage::Pending { .. } => Some(Stage::Pending),
            OperationStage::Queued => Some(Stage::Queued),
            OperationStage::Admitted { .. } => Some(Stage::Admitted),
            OperationStage::Replied => None,
        }
    }
}

/// One operation the service holds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationState {
    /// Its ID.
    pub id: OpId,
    /// What it asks for.
    pub request: RequestIdentity,
    /// Where it is.
    pub stage: OperationStage,
    /// The hash of the submitted block its request holds, if any.
    pub retained: Option<block::Hash>,
}

/// Everything the service owns for its callers, read from its containers.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OwnerState {
    /// The last fact this state reflects.
    pub through_seq: u64,
    /// Every held operation, in ID order.
    pub operations: Vec<OperationState>,
    /// The actions the driver took off its channel and deferred, oldest
    /// first. Each submission among them holds its block.
    pub deferred: Vec<RequestIdentity>,
}

impl OwnerState {
    /// Every operation holding a submitted block, with the block's hash.
    pub fn retained_blocks(&self) -> Vec<(OpId, block::Hash)> {
        self.operations
            .iter()
            .filter_map(|op| op.retained.map(|hash| (op.id, hash)))
            .collect()
    }

    /// Every read holding its serving lease: a pending read, or a reply its
    /// caller hasn't taken.
    pub fn read_leases(&self) -> Vec<OpId> {
        self.operations
            .iter()
            .filter(|op| matches!(op.request, RequestIdentity::Read { .. }))
            .map(|op| op.id)
            .collect()
    }

    /// Every pending query.
    pub fn pending_queries(&self) -> Vec<OpId> {
        self.operations
            .iter()
            .filter(|op| matches!(op.request, RequestIdentity::Query { .. }))
            .map(|op| op.id)
            .collect()
    }
}

/// The first way the facts or the state disagreed with [`OwnerModel`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ModelFailure {
    /// A fact's sequence number didn't follow the last one applied, or a
    /// transaction's facts weren't contiguous.
    Gap {
        /// The first fact out of place.
        seq: u64,
    },
    /// A transaction didn't end with exactly one head fact.
    MalformedTxn {
        /// The transaction.
        txn: u64,
    },
    /// A head named an operation the model doesn't hold, or holds at a
    /// stage that head can't follow.
    UnexpectedHead {
        /// The transaction.
        txn: u64,
    },
    /// A transaction's non-head facts weren't the ones its head requires.
    Effects {
        /// The transaction.
        txn: u64,
    },
    /// The actual state reflects other facts than the model applied.
    Stale {
        /// The last fact the actual state reflects.
        through_seq: u64,
        /// The last fact the model applied.
        applied_seq: u64,
    },
    /// The actual operations differ from the model's.
    Mismatch,
}

/// The owner state a pristine service reaches through the facts applied so
/// far.
#[derive(Clone, Debug, Default)]
pub struct OwnerModel {
    applied_seq: u64,
    applied_txn: u64,
    /// The last operation ID handed out, held or not.
    last_id: u64,
    ops: BTreeMap<OpId, OperationState>,
    /// The later admissions that replaced each held submission.
    replaced_by: BTreeMap<OpId, BTreeSet<OpId>>,
    /// The driver's deferred actions.
    deferred: Vec<RequestIdentity>,
    /// A query's reply found the handle closed, so the driver's exit comes
    /// next.
    exiting: bool,
    /// The driver exited.
    exited: bool,
    /// The service shut down.
    shut_down: bool,
}

impl OwnerModel {
    /// A model of a pristine service.
    pub fn new() -> Self {
        Self::default()
    }

    /// The owner state the model expects.
    pub fn state(&self) -> OwnerState {
        OwnerState {
            through_seq: self.applied_seq,
            operations: self.ops.values().cloned().collect(),
            deferred: self.deferred.clone(),
        }
    }

    /// Whether the driver awaits a held query or record.
    fn driver_awaiting(&self) -> bool {
        self.ops.values().any(|op| op.request.op().awaited())
    }

    /// Applies `facts`, the next facts the service published, after
    /// checking that they continue the facts applied so far and that each
    /// transaction holds the facts its head requires. Stops at the first
    /// failure, leaving the transactions before it applied.
    pub fn apply(&mut self, facts: &[Fact]) -> Result<(), ModelFailure> {
        let mut start = 0;
        while start < facts.len() {
            let txn = facts[start].txn;
            let end = facts[start..]
                .iter()
                .position(|fact| fact.txn != txn)
                .map_or(facts.len(), |offset| start + offset);
            let group = &facts[start..end];
            for (offset, fact) in group.iter().enumerate() {
                // Widening usize to u64 is lossless on supported targets.
                let expected = self.applied_seq + 1 + offset as u64;
                if fact.seq != expected || txn != self.applied_txn + 1 {
                    return Err(ModelFailure::Gap { seq: fact.seq });
                }
            }
            self.apply_txn(txn, group)?;
            self.applied_seq = group.last().expect("a group holds a fact").seq;
            self.applied_txn = txn;
            start = end;
        }
        Ok(())
    }

    /// Checks one transaction and applies it on success.
    fn apply_txn(&mut self, txn: u64, group: &[Fact]) -> Result<(), ModelFailure> {
        let (head, effects) = group.split_last().expect("a group holds a fact");
        if !head.kind.is_head() || effects.iter().any(|fact| fact.kind.is_head()) {
            return Err(ModelFailure::MalformedTxn { txn });
        }
        let effects: Vec<&FactKind> = effects.iter().map(|fact| &fact.kind).collect();
        let unexpected = || ModelFailure::UnexpectedHead { txn };
        let bad_effects = || ModelFailure::Effects { txn };
        if self.exiting != matches!(head.kind, FactKind::DriverExited { .. }) {
            return Err(unexpected());
        }
        match &head.kind {
            FactKind::Submitted { id, request, stage } => {
                self.submit(txn, *id, request, *stage, &effects)?;
            }
            FactKind::Admitted { id } => {
                if !effects.is_empty() {
                    return Err(bad_effects());
                }
                let op = self.ops.get(id).ok_or_else(unexpected)?;
                if op.stage != OperationStage::Queued || self.driver_awaiting() {
                    return Err(unexpected());
                }
                let hash = op.request.submitted_hash();
                for other in self.ops.values_mut() {
                    if other.id < *id && other.request.submitted_hash() == hash {
                        if let OperationStage::Admitted { replaced } = &mut other.stage {
                            *replaced = true;
                            self.replaced_by.entry(other.id).or_default().insert(*id);
                        }
                    }
                }
                self.ops
                    .get_mut(id)
                    .expect("the operation was found above")
                    .stage = OperationStage::Admitted { replaced: false };
            }
            FactKind::Completed(completion) => {
                self.complete(txn, completion, &effects)?;
            }
            FactKind::CallerDropped { id } => {
                if !effects.is_empty() {
                    return Err(bad_effects());
                }
                let op = self.ops.get_mut(id).ok_or_else(unexpected)?;
                match &mut op.stage {
                    OperationStage::Pending { caller_waiting }
                        if *caller_waiting
                            && matches!(op.request, RequestIdentity::Read { .. }) =>
                    {
                        *caller_waiting = false;
                    }
                    _ => return Err(unexpected()),
                }
            }
            FactKind::ReadTaken { id } | FactKind::ReadDropped { id } => {
                if !effects.is_empty() {
                    return Err(bad_effects());
                }
                if self.ops.get(id).map(|op| &op.stage) != Some(&OperationStage::Replied) {
                    return Err(unexpected());
                }
                self.ops.remove(id);
            }
            FactKind::DriverExited { dropped, deferred } => {
                if !effects.is_empty() {
                    return Err(bad_effects());
                }
                if *deferred != self.deferred {
                    return Err(unexpected());
                }
                self.deferred.clear();
                let actions: Vec<OpId> = self
                    .ops
                    .values()
                    .filter(|op| !matches!(op.request, RequestIdentity::Read { .. }))
                    .map(|op| op.id)
                    .collect();
                if *dropped != actions {
                    return Err(unexpected());
                }
                for id in actions {
                    self.ops.remove(&id);
                    self.replaced_by.remove(&id);
                }
                self.exiting = false;
                self.exited = true;
            }
            FactKind::ShutDown { dropped, deferred } => {
                if !effects.is_empty() || self.shut_down {
                    return Err(bad_effects());
                }
                if !dropped.iter().copied().eq(self.ops.keys().copied())
                    || *deferred != self.deferred
                {
                    return Err(unexpected());
                }
                self.ops.clear();
                self.replaced_by.clear();
                self.deferred.clear();
                self.shut_down = true;
            }
            _ => unreachable!("only head kinds pass the check above"),
        }
        Ok(())
    }

    /// Checks an arrival and applies it. The driver takes an action only
    /// while it awaits nothing, coalescing drops only queries other than
    /// the one it takes, and the step that takes it may leave a new
    /// deferred queue.
    fn submit(
        &mut self,
        txn: u64,
        id: OpId,
        request: &RequestIdentity,
        stage: Option<Stage>,
        effects: &[&FactKind],
    ) -> Result<(), ModelFailure> {
        let unexpected = || ModelFailure::UnexpectedHead { txn };
        let read = matches!(request, RequestIdentity::Read { .. });
        let mut coalesced = Vec::new();
        let mut deferred = None;
        for effect in effects {
            match effect {
                FactKind::Coalesced { query_id } if !read && deferred.is_none() => {
                    coalesced.push(*query_id)
                }
                FactKind::Deferred { queue } if !read && deferred.is_none() => {
                    deferred = Some(queue)
                }
                _ => return Err(ModelFailure::Effects { txn }),
            }
        }
        let taken = match request {
            RequestIdentity::Query { query_id, .. } => Some(*query_id),
            _ => None,
        };
        coalesced.sort_unstable();
        if coalesced.windows(2).any(|pair| pair[0] == pair[1])
            || taken.is_some_and(|taken| coalesced.contains(&taken))
        {
            return Err(ModelFailure::Effects { txn });
        }
        let stopped = self.shut_down || (!read && (self.exited || self.driver_awaiting()));
        if stopped || id.0 != self.last_id + 1 || stage != request.op().first_stage() {
            return Err(unexpected());
        }
        self.last_id = id.0;
        if let Some(queue) = deferred {
            self.deferred = queue.clone();
        }
        let stage = match stage {
            None => return Ok(()),
            Some(Stage::Queued) => OperationStage::Queued,
            Some(_) => OperationStage::Pending {
                caller_waiting: true,
            },
        };
        self.ops.insert(
            id,
            OperationState {
                id,
                request: request.clone(),
                stage,
                retained: request.submitted_hash(),
            },
        );
        Ok(())
    }

    /// Checks a completion's effects and applies it.
    fn complete(
        &mut self,
        txn: u64,
        completion: &Completion,
        effects: &[&FactKind],
    ) -> Result<(), ModelFailure> {
        let unexpected = || ModelFailure::UnexpectedHead { txn };
        let bad_effects = || ModelFailure::Effects { txn };
        let op = self.ops.get(&completion.id).ok_or_else(unexpected)?;
        let stage = op.stage.contract_stage().ok_or_else(unexpected)?;
        // The driver takes a verdict only while it awaits nothing.
        if op.request != completion.request
            || op.request.op() != completion.op
            || stage == Stage::Queued
            || (stage.needs_free_driver() && self.driver_awaiting())
        {
            return Err(unexpected());
        }
        let mut effects = effects.iter().copied().peekable();
        // Each installed transition is published as a view of its version
        // and verified tip. Its epoch moves on exactly when the effect
        // invalidates body work, which a reset always does.
        let mut installs = 0usize;
        while let Some(FactKind::TransitionInstalled {
            state_version,
            verified_best,
            epoch_before,
            invalidates_body_work,
            cause,
        }) = effects.peek().copied()
        {
            effects.next();
            let epoch = if *invalidates_body_work {
                epoch_before.checked_next().map_err(|_| bad_effects())?
            } else {
                *epoch_before
            };
            let coherent = match effects.next() {
                Some(FactKind::ViewPublished(view)) => {
                    view.state_version == *state_version
                        && view.verified_best == *verified_best
                        && view.epoch == epoch
                }
                _ => false,
            };
            if !coherent || (*cause == Some(VerifiedChangeCause::Reset) && !invalidates_body_work) {
                return Err(bad_effects());
            }
            installs += 1;
        }
        if !self.installs_allowed(completion, installs) {
            return Err(bad_effects());
        }
        // A query replies through the handle once, and so does a verdict for
        // a body the driver can report by its coinbase height.
        let replies = match &completion.request {
            RequestIdentity::Query { .. } => true,
            RequestIdentity::Submit { height, .. } => stage == Stage::Admitted && height.is_some(),
            _ => false,
        };
        if replies {
            let id = completion.id;
            let delivered = match effects.next() {
                Some(FactKind::ReplyDelivered { id: replied }) if *replied == id => true,
                Some(FactKind::ReplyDropped { id: replied }) if *replied == id => false,
                _ => return Err(bad_effects()),
            };
            if delivered != completion.delivered {
                return Err(bad_effects());
            }
        }
        if effects.next().is_some() {
            return Err(bad_effects());
        }
        let read = matches!(completion.op, BlockOp::ReadRange);
        let caller_waiting = matches!(
            self.ops.get(&completion.id).map(|op| &op.stage),
            Some(OperationStage::Pending {
                caller_waiting: true
            })
        );
        if (read && completion.delivered != caller_waiting)
            || (!read && !replies && completion.delivered)
        {
            return Err(bad_effects());
        }
        if read && completion.delivered {
            self.ops
                .get_mut(&completion.id)
                .expect("the operation was found above")
                .stage = OperationStage::Replied;
        } else {
            self.ops.remove(&completion.id);
            self.replaced_by.remove(&completion.id);
        }
        // A copy that reports a duplicate was refused, so it replaced
        // nothing.
        if completion.outcome == BlockOutcome::Verdict(Verdict::Duplicate) {
            for (older, replacers) in &mut self.replaced_by {
                if replacers.remove(&completion.id) && replacers.is_empty() {
                    if let Some(OperationStage::Admitted { replaced }) =
                        self.ops.get_mut(older).map(|op| &mut op.stage)
                    {
                        *replaced = false;
                    }
                }
            }
        }
        // The driver returns when a query's reply finds the handle closed.
        self.exiting = completion.op == BlockOp::QueryNeededBlocks && !completion.delivered;
        Ok(())
    }

    /// Whether `completion` may install `installs` transitions. A commit
    /// installs one. A commit the engine finds stale or stalls on its
    /// resource limits commits nothing and reports a `Storage` retry; a
    /// stall can still install its alarm. A record installs once per
    /// committed attempt, and a stalled attempt may install its alarm.
    fn installs_allowed(&self, completion: &Completion, installs: usize) -> bool {
        match (completion.outcome, &completion.result) {
            (BlockOutcome::Verdict(chosen), ResultIdentity::Verdict { verdict, .. }) => {
                if !chosen.commits() {
                    *verdict == chosen && installs == 0
                } else if *verdict == chosen {
                    installs == 1
                } else {
                    *verdict == Verdict::Retryable(RetryKind::Storage) && installs <= 1
                }
            }
            (_, ResultIdentity::Recorded(attempts)) => {
                let committed = attempts
                    .iter()
                    .filter(|attempt| **attempt == RecordResult::Committed)
                    .count();
                let stalled = attempts
                    .iter()
                    .filter(|attempt| **attempt == RecordResult::ResourceStalled)
                    .count();
                (committed..=committed + stalled).contains(&installs)
            }
            _ => installs == 0,
        }
    }

    /// Compares the model's state with `actual`.
    pub fn compare(&self, actual: &OwnerState) -> Result<(), ModelFailure> {
        if actual.through_seq != self.applied_seq {
            return Err(ModelFailure::Stale {
                through_seq: actual.through_seq,
                applied_seq: self.applied_seq,
            });
        }
        if actual.operations != self.state().operations || actual.deferred != self.deferred {
            return Err(ModelFailure::Mismatch);
        }
        Ok(())
    }

    /// Applies `facts` and then compares the result with `actual`, as a
    /// checker does at each step boundary.
    pub fn check_boundary(
        &mut self,
        facts: &[Fact],
        actual: &OwnerState,
    ) -> Result<(), ModelFailure> {
        self.apply(facts)?;
        self.compare(actual)
    }
}

/// How a sequence of committed views broke committed-frontier coherence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ViewViolation {
    /// A view's version didn't follow the last one's.
    VersionWentBack {
        /// The last view's version.
        last: StateVersion,
        /// This view's.
        next: StateVersion,
    },
    /// A view's epoch was older than the last one's.
    EpochWentBack,
    /// Within one epoch, the verified tip left the last tip's chain: full
    /// state doesn't hold the new tip with the last tip as its ancestor.
    VerifiedTipLeftChain {
        /// The last view's tip.
        last: Frontier,
        /// This view's tip.
        next: Frontier,
    },
    /// A view's hint revision went back.
    HintRevisionWentBack,
}

/// Checks the committed views block sync receives, in order: versions and
/// epochs never go back, and within one epoch the verified tip only grows
/// along its chain, so a verified-chain reset must start a new epoch.
#[derive(Clone, Debug, Default)]
pub struct ViewModel {
    last: Option<ViewIdentity>,
}

impl ViewModel {
    /// A model that has seen no view.
    pub fn new() -> Self {
        Self::default()
    }

    /// Checks `view`, the next view received, against `chain`, full state
    /// when the view was published.
    pub fn observe(
        &mut self,
        view: &CommittedHeaderChainView,
        chain: &VerifiedChain,
    ) -> Result<(), ViewViolation> {
        let next = ViewIdentity::of(view);
        if let Some(last) = self.last {
            if next.state_version < last.state_version {
                return Err(ViewViolation::VersionWentBack {
                    last: last.state_version,
                    next: next.state_version,
                });
            }
            if next.epoch < last.epoch {
                return Err(ViewViolation::EpochWentBack);
            }
            if next.hint_revision < last.hint_revision {
                return Err(ViewViolation::HintRevisionWentBack);
            }
            let descends = chain.ancestor(next.verified_best, last.verified_best.height)
                == Some(last.verified_best.hash);
            if next.epoch == last.epoch && !descends {
                return Err(ViewViolation::VerifiedTipLeftChain {
                    last: last.verified_best,
                    next: next.verified_best,
                });
            }
        }
        self.last = Some(next);
        Ok(())
    }
}
