//! A replay of the controlled port's facts onto a copy of its owner state.
//!
//! [`OwnerModel`] applies each fact's recorded effect to its own copy of the
//! owner state, then compares the copy with the port's actual
//! [`OwnerState`]. It decides nothing the port decides: which lease the sweep
//! ends, which class admits an acquire, which continuation the cache evicts,
//! or when a waiter fires. The facts say what changed. The replay checks
//! only that they explain the state, so a producer that changes state
//! without a record, or records a change it didn't make, fails here.
//!
//! Beside that bookkeeping, it checks what follows from each operation's
//! request and typed result:
//! - a locator's submission takes its permit, and its completion drops it;
//! - an `Acquired` result names the lease its transaction created, at the
//!   request's target and starting at one of the request's locators;
//! - a served page is a path from the read's start, no longer than asked,
//!   and consumes the read's lease;
//! - an abandoned `Acquired` reply's lease ends;
//! - shutdown drops every operation, permit, lease and continuation.
//!
//! [`ControlledPort::owner_state`](crate::header::controlled::ControlledPort::owner_state)
//! checks the registry's invariants, and named scenarios in
//! `tests/header_controlled.rs` and `tests/header_paths.rs` pin its rules.
//!
//! The replay assumes what this adapter guarantees: finality never leaves
//! the genesis anchor, so a leased path retains every header above its
//! common ancestor.

use std::collections::{BTreeMap, BTreeSet};

use zakura_chain::block;
use zakura_header_chain::{Frontier, SourceId};

use super::{
    CallerPresence, ClassWaiters, ContinuationState, LeaseFact, LeaseKey, LeaseState,
    OperationStage, OperationState, OwnerState, PathPosition, RequestIdentity, ResultIdentity,
    RetentionEnd, Waiter,
};
use crate::header::{
    controlled::{Completion, Fact, FactKind, OpId},
    lease::LeaseEnd,
    HeaderOp, Outcome, Release,
};

/// The first way the facts or the state disagreed with the replay, in the
/// precedence [`OwnerModel::check_boundary`] checks them.
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(
    clippy::large_enum_variant,
    reason = "a failure ends a check, so its size never matters"
)]
pub enum ModelFailure {
    /// A fact's sequence number didn't follow the last one applied, or a
    /// transaction's number or facts weren't contiguous.
    Gap {
        /// The sequence number of the first fact out of place.
        seq: u64,
    },
    /// A transaction didn't end with exactly one head fact.
    MalformedTxn {
        /// The transaction.
        txn: u64,
    },
    /// A fact, or the actual state, named an owner the replay doesn't have.
    UnknownOwner {
        /// The transaction, or `None` for the state comparison.
        txn: Option<u64>,
        /// The owner.
        owner: Owner,
    },
    /// A transaction left `owner` without the fact its head or its other
    /// facts require: a lease its result names, the path or waiter of an
    /// ended lease, a permit to drop, or an owner shutdown must drop.
    MissingEffect {
        /// The transaction.
        txn: u64,
        /// The owner the missing fact is about.
        owner: Owner,
    },
    /// A transaction held a fact, or a head, its pre-state, request or
    /// result doesn't allow.
    UnexpectedEffect {
        /// The transaction.
        txn: u64,
        /// The first unexpected fact.
        found: FactKind,
    },
    /// A transaction ended a lease's path, or fired its waiter, before it
    /// ended the lease's capability.
    OrderViolation {
        /// The transaction.
        txn: u64,
    },
    /// The actual state reflects facts the replay hasn't applied.
    Stale {
        /// The last fact the actual state reflects.
        through_seq: u64,
        /// The last fact the replay applied.
        applied_seq: u64,
    },
    /// A completed operation's caller is gone but its reply is still held.
    UndisposedReply(OpId),
    /// A known owner's actual state differs from the replay's.
    Mismatch(StateField),
}

/// Something that owns storage state.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Owner {
    /// An operation.
    Operation(OpId),
    /// Storage's permit for an operation.
    Permit(OpId),
    /// A live lease.
    Lease(LeaseKey),
    /// A cached continuation.
    Continuation(LeaseKey),
}

/// A field of [`OwnerState`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum StateField {
    /// `operations`.
    Operations,
    /// `permits`.
    Permits,
    /// `leases`.
    Leases,
    /// `continuations`.
    Continuations,
    /// `class_waiters`.
    ClassWaiters,
    /// `last_lease_id`.
    LastLeaseId,
    /// `continuation_hits`.
    ContinuationHits,
}

/// The owner state a pristine port reaches through the facts applied so
/// far. See the module doc for what it checks.
#[derive(Clone, Debug, Default)]
pub struct OwnerModel {
    applied_seq: u64,
    applied_txn: u64,
    last_op: u64,
    shut_down: bool,
    ops: BTreeMap<OpId, ReplayOp>,
    permits: BTreeSet<OpId>,
    leases: BTreeMap<SourceId, ReplayLease>,
    continuations: BTreeMap<SourceId, ContinuationState>,
    class_waiters: ClassWaiters,
    last_lease_id: u64,
    /// Acquires that continued from a cached path, which `continuation_hits`
    /// counts.
    continued: u64,
    /// Cached paths an acquire discarded. A hit may end in a discard, when
    /// the continued target is gone, so hits lie between `continued` and
    /// `continued + discarded`.
    discarded: u64,
}

/// One operation the replay knows.
#[derive(Clone, Debug)]
struct ReplayOp {
    request: RequestIdentity,
    request_digest: [u8; 32],
    caller_present: bool,
    /// Whether the run committed it and holds its reply for delivery.
    committed: bool,
    /// The stored reply's identity, once completed for a waiting caller.
    replied: Option<ResultIdentity>,
}

/// One live lease, with how many hashes its path retains above its common
/// ancestor, which a continuation keeps.
#[derive(Clone, Debug)]
struct ReplayLease {
    state: LeaseState,
    retained: u64,
}

/// What one transaction's facts left open until its end.
#[derive(Debug, Default)]
struct Open {
    /// Leases whose capability ended, with whether their path ended or
    /// moved to the cache and whether their peer waiter fired.
    ended: BTreeMap<LeaseKey, Ended>,
    /// The continuation an acquire continued from, until its lease begins.
    continuing: Option<ContinuationState>,
    /// Leases the transaction created.
    leased: Vec<LeaseState>,
    /// Facts the transaction holds besides its head.
    lease_facts: usize,
}

/// A lease whose capability ended in the open transaction.
#[derive(Debug)]
struct Ended {
    lease: ReplayLease,
    release: Release,
    path_done: bool,
    /// Whether its peer waiter still has to fire.
    waiter: bool,
}

impl OwnerModel {
    /// The replay of a pristine port.
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies `facts`, the next facts the port published, after checking
    /// that they continue the facts applied so far and that each
    /// transaction's facts explain its head. Stops at the first failure,
    /// leaving the transactions before it applied.
    pub fn apply(&mut self, facts: &[Fact]) -> Result<(), ModelFailure> {
        let mut groups: Vec<&[Fact]> = Vec::new();
        let (mut seq, mut txn, mut start) = (self.applied_seq, self.applied_txn, 0);
        for (index, fact) in facts.iter().enumerate() {
            let continues = index > start && fact.txn == txn;
            let opens = fact.txn == txn.wrapping_add(1);
            if fact.seq != seq.wrapping_add(1) || !(continues || opens) {
                return Err(ModelFailure::Gap { seq: fact.seq });
            }
            if opens && index > 0 && !continues {
                groups.push(&facts[start..index]);
                start = index;
            }
            (seq, txn) = (fact.seq, fact.txn);
        }
        if start < facts.len() {
            groups.push(&facts[start..]);
        }
        for group in groups {
            let last = group.last().expect("a group holds at least one fact");
            let mut next = self.clone();
            next.apply_txn(last.txn, group)?;
            next.applied_seq = last.seq;
            next.applied_txn = last.txn;
            *self = next;
        }
        Ok(())
    }

    /// Applies one transaction's facts, then checks its head against them.
    fn apply_txn(&mut self, txn: u64, facts: &[Fact]) -> Result<(), ModelFailure> {
        let heads = facts.iter().filter(|fact| fact.kind.is_head()).count();
        let (head, effects) = facts.split_last().expect("a transaction holds a fact");
        if heads != 1 || !head.kind.is_head() {
            return Err(ModelFailure::MalformedTxn { txn });
        }
        let mut open = Open::default();
        for fact in effects {
            self.effect(txn, &fact.kind, &mut open)?;
        }
        self.close(txn, &mut open)?;
        let unexpected = || ModelFailure::UnexpectedEffect {
            txn,
            found: head.kind.clone(),
        };
        match &head.kind {
            FactKind::Submitted {
                id,
                request,
                request_digest,
            } => {
                if self.shut_down || id.0 != self.last_op.wrapping_add(1) {
                    return Err(unexpected());
                }
                self.last_op = id.0;
                // A locator's submission takes its permit, and nothing else.
                let locator = *request == RequestIdentity::ContinuationLocator;
                let taken = effects
                    .iter()
                    .any(|fact| fact.kind == FactKind::PermitTaken { id: *id });
                if locator && !taken {
                    return Err(ModelFailure::MissingEffect {
                        txn,
                        owner: Owner::Permit(*id),
                    });
                }
                if let Some(extra) = effects
                    .iter()
                    .find(|fact| !(locator && fact.kind == FactKind::PermitTaken { id: *id }))
                {
                    return Err(ModelFailure::UnexpectedEffect {
                        txn,
                        found: extra.kind.clone(),
                    });
                }
                self.ops.insert(
                    *id,
                    ReplayOp {
                        request: request.clone(),
                        request_digest: *request_digest,
                        caller_present: true,
                        committed: false,
                        replied: None,
                    },
                );
            }
            FactKind::CallerDropped { id } => {
                only_head(txn, effects)?;
                let op = self.op_mut(txn, *id)?;
                if op.replied.is_some() || !op.caller_present {
                    return Err(unexpected());
                }
                op.caller_present = false;
            }
            FactKind::ReplyTaken { id } => {
                only_head(txn, effects)?;
                if self.op_mut(txn, *id)?.replied.is_none() {
                    return Err(unexpected());
                }
                self.ops.remove(id);
            }
            FactKind::ReplyDropped { id } => {
                let result = self
                    .op_mut(txn, *id)?
                    .replied
                    .clone()
                    .ok_or_else(unexpected)?;
                self.ops.remove(id);
                self.abandoned(txn, &result, effects)?;
            }
            FactKind::Committed { id, .. } => {
                // An apply leaves the registry alone, so its commit holds no
                // other fact.
                only_head(txn, effects)?;
                let op = self.op_mut(txn, *id)?;
                if op.replied.is_some()
                    || op.committed
                    || op.request.op() != HeaderOp::ApplyHeaderTarget
                {
                    return Err(unexpected());
                }
                op.committed = true;
            }
            // A reset changes the engine's selection, which no owner holds.
            FactKind::Reset { .. } => {
                only_head(txn, effects)?;
                if self.shut_down {
                    return Err(unexpected());
                }
            }
            FactKind::Completed(completion) => {
                self.completed(txn, completion, effects, &open)
                    .map_err(|failure| failure.unwrap_or_else(unexpected))?;
            }
            FactKind::ShutDown {
                dropped,
                callers_alive,
            } => {
                if let Some(id) = dropped.iter().find(|id| !self.ops.contains_key(id)) {
                    return Err(ModelFailure::UnknownOwner {
                        txn: Some(txn),
                        owner: Owner::Operation(*id),
                    });
                }
                if self.shut_down
                    || *dropped != self.ops.keys().copied().collect::<Vec<_>>()
                    || callers_alive.iter().any(|id| !dropped.contains(id))
                {
                    return Err(unexpected());
                }
                self.shut_down = true;
                self.ops.clear();
                // Shutdown returns every resource.
                let left = self
                    .permits
                    .first()
                    .map(|id| Owner::Permit(*id))
                    .or_else(|| {
                        self.leases
                            .values()
                            .next()
                            .map(|lease| Owner::Lease(lease.state.lease))
                    })
                    .or_else(|| {
                        self.continuations
                            .values()
                            .next()
                            .map(|cached| Owner::Continuation(cached.lease))
                    });
                if let Some(owner) = left {
                    return Err(ModelFailure::MissingEffect { txn, owner });
                }
            }
            FactKind::PermitTaken { .. } | FactKind::PermitDropped { .. } | FactKind::Lease(_) => {
                unreachable!("the head was checked above")
            }
        }
        Ok(())
    }

    /// Applies one non-head fact's recorded effect.
    fn effect(&mut self, txn: u64, kind: &FactKind, open: &mut Open) -> Result<(), ModelFailure> {
        let unknown = |owner| ModelFailure::UnknownOwner {
            txn: Some(txn),
            owner,
        };
        let unexpected = || ModelFailure::UnexpectedEffect {
            txn,
            found: kind.clone(),
        };
        let fact = match kind {
            FactKind::PermitTaken { id } => {
                return match self.permits.insert(*id) {
                    true => Ok(()),
                    false => Err(unexpected()),
                }
            }
            FactKind::PermitDropped { id } => {
                return match self.permits.remove(id) {
                    true => Ok(()),
                    false => Err(unknown(Owner::Permit(*id))),
                }
            }
            FactKind::Lease(fact) => fact,
            _ => unreachable!("the caller passes only non-head facts"),
        };
        open.lease_facts += 1;
        match fact {
            LeaseFact::Leased {
                lease,
                scope,
                target,
                common_ancestor,
                position,
                capacity,
                continued_from,
            } => {
                // Lease IDs count up, and a peer holds one lease at a time.
                if lease.lease_id <= self.last_lease_id || self.leases.contains_key(&lease.peer) {
                    return Err(unexpected());
                }
                let retained = match continued_from {
                    None => u64::from(target.height.0)
                        .saturating_sub(u64::from(common_ancestor.height.0)),
                    Some(from) => match open.continuing.take() {
                        Some(cached)
                            if cached.lease.lease_id == *from
                                && cached.lease.peer == lease.peer =>
                        {
                            cached.retained
                        }
                        _ => return Err(unexpected()),
                    },
                };
                self.last_lease_id = lease.lease_id;
                let state = LeaseState {
                    lease: *lease,
                    scope: *scope,
                    target: *target,
                    last_frontier: *common_ancestor,
                    position: *position,
                    capacity: *capacity,
                    waiter: false,
                    leased_txn: Some(txn),
                };
                open.leased.push(state.clone());
                self.leases
                    .insert(lease.peer, ReplayLease { state, retained });
            }
            LeaseFact::CapabilityEnded { lease, release } => {
                let live = self
                    .live(lease)
                    .ok_or_else(|| unknown(Owner::Lease(*lease)))?;
                let lease = self
                    .leases
                    .remove(&live)
                    .expect("the live lease was found above");
                let waiter = lease.state.waiter;
                open.ended.insert(
                    lease.state.lease,
                    Ended {
                        lease,
                        release: *release,
                        path_done: false,
                        waiter,
                    },
                );
            }
            LeaseFact::RetentionEnded {
                lease,
                why: RetentionEnd::WithCapability,
            } => {
                self.end_path(txn, lease, open)?;
            }
            LeaseFact::RetentionEnded { lease, why } => {
                let cached = self
                    .continuations
                    .get(&lease.peer)
                    .filter(|cached| cached.lease == *lease)
                    .ok_or_else(|| unknown(Owner::Continuation(*lease)))?;
                let cached = cached.clone();
                self.continuations.remove(&lease.peer);
                match why {
                    RetentionEnd::Continued { into } => {
                        if open.continuing.is_some() || *into <= self.last_lease_id {
                            return Err(unexpected());
                        }
                        self.continued += 1;
                        open.continuing = Some(cached);
                    }
                    RetentionEnd::Discarded => self.discarded += 1,
                    RetentionEnd::Evicted | RetentionEnd::ShutdownCleared => {}
                    RetentionEnd::WithCapability => unreachable!("matched above"),
                }
            }
            LeaseFact::RetentionCached {
                lease,
                last_frontier,
                position,
            } => {
                let ended = self.end_path(txn, lease, open)?;
                // Only a path with headers left is cached.
                if !matches!(position, PathPosition::Retained { .. })
                    || self.continuations.contains_key(&lease.peer)
                {
                    return Err(unexpected());
                }
                let state = &ended.lease.state;
                self.continuations.insert(
                    lease.peer,
                    ContinuationState {
                        lease: *lease,
                        scope: state.scope,
                        target: state.target,
                        last_frontier: *last_frontier,
                        position: *position,
                        retained: ended.lease.retained,
                    },
                );
            }
            LeaseFact::WaiterSet(waiter) => {
                let set = match waiter {
                    Waiter::General => &mut self.class_waiters.general,
                    Waiter::Fallback => &mut self.class_waiters.fallback,
                    Waiter::Peer(lease) => {
                        let live = self
                            .live(lease)
                            .ok_or_else(|| unknown(Owner::Lease(*lease)))?;
                        &mut self
                            .leases
                            .get_mut(&live)
                            .expect("the live lease was found above")
                            .state
                            .waiter
                    }
                };
                // A waiter is recorded only when it wasn't set.
                if std::mem::replace(set, true) {
                    return Err(unexpected());
                }
            }
            LeaseFact::WaiterFired(waiter) => {
                let set = match waiter {
                    Waiter::General => &mut self.class_waiters.general,
                    Waiter::Fallback => &mut self.class_waiters.fallback,
                    Waiter::Peer(lease) => {
                        if self.live(lease).is_some() {
                            return Err(ModelFailure::OrderViolation { txn });
                        }
                        &mut open
                            .ended
                            .get_mut(lease)
                            .ok_or_else(|| unknown(Owner::Lease(*lease)))?
                            .waiter
                    }
                };
                if !std::mem::replace(set, false) {
                    return Err(unexpected());
                }
            }
        }
        Ok(())
    }

    /// Ends the path of `lease`, whose capability ended earlier in the open
    /// transaction.
    fn end_path<'o>(
        &self,
        txn: u64,
        lease: &LeaseKey,
        open: &'o mut Open,
    ) -> Result<&'o Ended, ModelFailure> {
        if self.live(lease).is_some() {
            return Err(ModelFailure::OrderViolation { txn });
        }
        match open.ended.get_mut(lease) {
            Some(ended) if !ended.path_done => {
                ended.path_done = true;
                Ok(ended)
            }
            _ => Err(ModelFailure::UnknownOwner {
                txn: Some(txn),
                owner: Owner::Lease(*lease),
            }),
        }
    }

    /// Checks what the open transaction's facts left open: every ended
    /// lease's path ended or moved to the cache and its waiter fired, and a
    /// continued path became a lease.
    fn close(&self, txn: u64, open: &mut Open) -> Result<(), ModelFailure> {
        if let Some((key, _)) = open
            .ended
            .iter()
            .find(|(_, ended)| !ended.path_done || ended.waiter)
        {
            return Err(ModelFailure::MissingEffect {
                txn,
                owner: Owner::Lease(*key),
            });
        }
        if let Some(cached) = &open.continuing {
            return Err(ModelFailure::MissingEffect {
                txn,
                owner: Owner::Continuation(cached.lease),
            });
        }
        Ok(())
    }

    /// The peer whose live lease is `key`.
    fn live(&self, key: &LeaseKey) -> Option<SourceId> {
        self.leases
            .get(&key.peer)
            .is_some_and(|lease| lease.state.lease == *key)
            .then_some(key.peer)
    }

    /// The operation `id`, or the failure for an unknown one.
    fn op_mut(&mut self, txn: u64, id: OpId) -> Result<&mut ReplayOp, ModelFailure> {
        self.ops.get_mut(&id).ok_or(ModelFailure::UnknownOwner {
            txn: Some(txn),
            owner: Owner::Operation(id),
        })
    }

    /// Checks that an abandoned reply's transaction ended its lease, if it
    /// names one, and changed nothing else.
    fn abandoned(
        &self,
        txn: u64,
        result: &ResultIdentity,
        effects: &[Fact],
    ) -> Result<(), ModelFailure> {
        let lease = match result {
            ResultIdentity::Acquired { lease, .. } => Some(*lease),
            _ => None,
        };
        if let Some(lease) = lease {
            if self.live(&lease).is_some() {
                return Err(ModelFailure::MissingEffect {
                    txn,
                    owner: Owner::Lease(lease),
                });
            }
        }
        // Only the abandoned lease's end and its waiters' fires.
        let foreign = effects.iter().find(|fact| match &fact.kind {
            FactKind::Lease(LeaseFact::CapabilityEnded {
                lease: ended,
                release,
            }) => Some(*ended) != lease || *release != Release::LeaseAbandonedAcquire,
            FactKind::Lease(LeaseFact::RetentionEnded {
                lease: ended,
                why: RetentionEnd::WithCapability,
            }) => Some(*ended) != lease,
            FactKind::Lease(LeaseFact::WaiterFired(_)) => false,
            _ => true,
        });
        match foreign {
            Some(fact) => Err(ModelFailure::UnexpectedEffect {
                txn,
                found: fact.kind.clone(),
            }),
            None => Ok(()),
        }
    }

    /// Checks a completion against its operation and its transaction's
    /// facts, and applies it. `Err(None)` is an illegal head.
    fn completed(
        &mut self,
        txn: u64,
        completion: &Completion,
        effects: &[Fact],
        open: &Open,
    ) -> Result<(), Option<ModelFailure>> {
        let id = completion.id;
        let op = self.op_mut(txn, id).map_err(Some)?.clone();
        let consistent = op.replied.is_none()
            && completion.txn == txn
            && completion.request == op.request
            && completion.op == op.request.op()
            && completion.request_digest == op.request_digest
            && completion.delivered == op.caller_present
            // A committed apply's delivery runs nothing, since its commit did.
            && (!op.committed || completion.outcome == Outcome::Engine);
        let result = &completion.result;
        let outcome_matches = match completion.outcome {
            Outcome::Fault(fault) => *result == ResultIdentity::Fault(fault),
            Outcome::Rejected => *result == ResultIdentity::Rejected,
            Outcome::Engine => {
                !matches!(result, ResultIdentity::Fault(_) | ResultIdentity::Rejected)
            }
        };
        if !consistent || !outcome_matches {
            return Err(None);
        }
        // The head names the capability ends its facts record.
        let ends: Vec<_> = effects
            .iter()
            .filter_map(|fact| match &fact.kind {
                FactKind::Lease(fact) => LeaseEnd::of(fact),
                _ => None,
            })
            .collect();
        if completion.lease_ends != ends {
            return Err(None);
        }
        // A completion drops its operation's permit, and no other.
        if let Some(fact) = effects.iter().find(|fact| {
            matches!(fact.kind, FactKind::PermitTaken { .. })
                || matches!(fact.kind, FactKind::PermitDropped { id: dropped } if dropped != id)
        }) {
            return Err(Some(ModelFailure::UnexpectedEffect {
                txn,
                found: fact.kind.clone(),
            }));
        }
        if self.permits.contains(&id) {
            return Err(Some(ModelFailure::MissingEffect {
                txn,
                owner: Owner::Permit(id),
            }));
        }
        // Only an engine run of a lease operation, or an abandoned lease,
        // touches the registry.
        let leases_op = matches!(
            op.request.op(),
            HeaderOp::AcquireHeaderPath | HeaderOp::ReadHeaderPath | HeaderOp::ReleaseHeaderPath
        );
        let abandons = !completion.delivered && matches!(result, ResultIdentity::Acquired { .. });
        let runs = completion.outcome == Outcome::Engine && !op.committed && leases_op;
        if open.lease_facts > 0 && !runs && !abandons {
            let found = effects
                .iter()
                .find(|fact| matches!(fact.kind, FactKind::Lease(_)))
                .expect("a lease fact was counted");
            return Err(Some(ModelFailure::UnexpectedEffect {
                txn,
                found: found.kind.clone(),
            }));
        }
        if runs {
            self.result_follows_request(txn, &op.request, result, open, completion.delivered)?;
        }
        if completion.delivered {
            self.ops
                .get_mut(&id)
                .expect("the operation was found above")
                .replied = Some(result.clone());
        } else {
            self.ops.remove(&id);
            self.abandoned(txn, result, &[]).map_err(Some)?;
        }
        Ok(())
    }

    /// Checks that an engine run's typed `result` follows from its
    /// `request` and from the facts its transaction recorded.
    fn result_follows_request(
        &self,
        txn: u64,
        request: &RequestIdentity,
        result: &ResultIdentity,
        open: &Open,
        delivered: bool,
    ) -> Result<(), Option<ModelFailure>> {
        match (request, result) {
            (
                RequestIdentity::AcquireHeaderPath {
                    peer,
                    session_id,
                    target_tip_hash,
                    locators,
                    ..
                },
                ResultIdentity::Acquired {
                    lease,
                    common_ancestor,
                    target,
                },
            ) => {
                // Storage starts a path at one of the locators it was given,
                // or refuses with `NoLocatorIntersection`.
                if lease.peer != *peer
                    || lease.session_id != *session_id
                    || target.hash != *target_tip_hash
                    || !locators.contains(&common_ancestor.hash)
                {
                    return Err(None);
                }
                let Some(leased) = open.leased.iter().find(|state| state.lease == *lease) else {
                    return Err(Some(ModelFailure::MissingEffect {
                        txn,
                        owner: Owner::Lease(*lease),
                    }));
                };
                if leased.target != *target || leased.last_frontier != *common_ancestor {
                    return Err(None);
                }
                if delivered && self.live(lease).is_none() {
                    return Err(Some(ModelFailure::MissingEffect {
                        txn,
                        owner: Owner::Lease(*lease),
                    }));
                }
            }
            // Only an `Acquired` result leases.
            (RequestIdentity::AcquireHeaderPath { .. }, _) if !open.leased.is_empty() => {
                return Err(None)
            }
            (
                RequestIdentity::ReadHeaderPath {
                    lease,
                    after_hash,
                    max_header_count,
                    ..
                },
                ResultIdentity::Page {
                    last,
                    count,
                    complete,
                    links,
                },
            ) => {
                let Some(lease) = lease else {
                    return Err(None);
                };
                if *count > *max_header_count || !is_path(*after_hash, links, *last, *count) {
                    return Err(None);
                }
                // A served page consumes its lease: the path ends if the
                // page reached the target, and is cached otherwise.
                let Some(ended) = open.ended.get(lease) else {
                    return Err(Some(ModelFailure::MissingEffect {
                        txn,
                        owner: Owner::Lease(*lease),
                    }));
                };
                let state = &ended.lease.state;
                let cached = self
                    .continuations
                    .get(&lease.peer)
                    .filter(|c| c.lease == *lease);
                let advanced = match state.position {
                    PathPosition::Retained { next } => next.saturating_add(u64::from(*count)),
                    PathPosition::Complete => ended.lease.retained,
                };
                let plausible = ended.release == Release::LeaseConsumedByRead
                    && state.last_frontier.hash == *after_hash
                    && u64::from(last.height.0)
                        == u64::from(state.last_frontier.height.0) + u64::from(*count)
                    && *complete == (*last == state.target)
                    && match cached {
                        None => *complete,
                        Some(cached) => {
                            !*complete
                                && cached.last_frontier == *last
                                && cached.position == PathPosition::Retained { next: advanced }
                        }
                    };
                if !plausible {
                    return Err(None);
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Compares the replay with the port's `actual` state: freshness, then
    /// owners the replay lacks, then an undisposed reply, then each field.
    pub fn compare(&self, actual: &OwnerState) -> Result<(), ModelFailure> {
        if actual.through_seq != self.applied_seq {
            return Err(ModelFailure::Stale {
                through_seq: actual.through_seq,
                applied_seq: self.applied_seq,
            });
        }
        let unknown = actual
            .operations
            .iter()
            .find(|op| !self.ops.contains_key(&op.id))
            .map(|op| Owner::Operation(op.id))
            .or_else(|| {
                actual
                    .permits
                    .iter()
                    .find(|id| !self.permits.contains(id))
                    .map(|id| Owner::Permit(*id))
            })
            .or_else(|| {
                actual
                    .leases
                    .iter()
                    .find(|lease| self.live(&lease.lease).is_none())
                    .map(|lease| Owner::Lease(lease.lease))
            })
            .or_else(|| {
                actual
                    .continuations
                    .iter()
                    .find(|cached| {
                        self.continuations
                            .get(&cached.lease.peer)
                            .is_none_or(|known| known.lease != cached.lease)
                    })
                    .map(|cached| Owner::Continuation(cached.lease))
            });
        if let Some(owner) = unknown {
            return Err(ModelFailure::UnknownOwner { txn: None, owner });
        }
        if let Some(op) = actual.operations.iter().find(|op| {
            matches!(op.stage, OperationStage::Replied { .. }) && op.caller == CallerPresence::Gone
        }) {
            return Err(ModelFailure::UndisposedReply(op.id));
        }
        // Destructured without `..`, so a new field can't go uncompared.
        let OwnerState {
            // Checked first, as freshness.
            through_seq: _,
            operations,
            permits,
            leases,
            continuations,
            class_waiters,
            last_lease_id,
            // Bounded, not replayed: see `OwnerModel::discarded`.
            continuation_hits: _,
        } = self.state();
        let hits = self.continued..=self.continued.saturating_add(self.discarded);
        let fields = [
            (StateField::Operations, operations == actual.operations),
            (StateField::Permits, permits == actual.permits),
            (StateField::Leases, leases == actual.leases),
            (
                StateField::Continuations,
                continuations == actual.continuations,
            ),
            (
                StateField::ClassWaiters,
                class_waiters == actual.class_waiters,
            ),
            (
                StateField::LastLeaseId,
                last_lease_id == actual.last_lease_id,
            ),
            (
                StateField::ContinuationHits,
                hits.contains(&actual.continuation_hits),
            ),
        ];
        match fields.into_iter().find(|(_, equal)| !equal) {
            Some((field, _)) => Err(ModelFailure::Mismatch(field)),
            None => Ok(()),
        }
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

    /// The owner state the replay reached, in [`OwnerState`]'s form. Its
    /// `continuation_hits` counts the acquires that continued, the least
    /// the port may report.
    pub fn state(&self) -> OwnerState {
        OwnerState {
            through_seq: self.applied_seq,
            operations: self
                .ops
                .iter()
                .map(|(id, op)| OperationState {
                    id: *id,
                    request: op.request.clone(),
                    stage: match &op.replied {
                        Some(result) => OperationStage::Replied {
                            result: result.clone(),
                        },
                        None if op.committed => OperationStage::Committed {
                            caller_waiting: op.caller_present,
                        },
                        None => OperationStage::Pending {
                            caller_waiting: op.caller_present,
                        },
                    },
                    caller: if op.caller_present {
                        CallerPresence::Alive
                    } else {
                        CallerPresence::Gone
                    },
                })
                .collect(),
            permits: self.permits.iter().copied().collect(),
            leases: self
                .leases
                .values()
                .map(|lease| lease.state.clone())
                .collect(),
            continuations: self.continuations.values().cloned().collect(),
            class_waiters: self.class_waiters,
            last_lease_id: self.last_lease_id,
            continuation_hits: self.continued,
        }
    }
}

/// Fails at the first fact of a transaction whose head allows none.
fn only_head(txn: u64, effects: &[Fact]) -> Result<(), ModelFailure> {
    match effects.first() {
        Some(fact) => Err(ModelFailure::UnexpectedEffect {
            txn,
            found: fact.kind.clone(),
        }),
        None => Ok(()),
    }
}

/// True if `links`, each read header's hash and its parent's, are `count`
/// headers each the child of the one before, the first of `after`, and the
/// last `last`'s, or `after` itself for an empty page.
fn is_path(
    after: block::Hash,
    links: &[(block::Hash, block::Hash)],
    last: Frontier,
    count: u32,
) -> bool {
    let mut parent = after;
    for &(header, of) in links {
        if of != parent {
            return false;
        }
        parent = header;
    }
    usize::try_from(count).is_ok_and(|count| count == links.len()) && parent == last.hash
}
