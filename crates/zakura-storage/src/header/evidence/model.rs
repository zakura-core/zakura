//! A model of the controlled port's owner state, rebuilt from its facts.
//!
//! [`OwnerModel`] applies each transaction to its own copy of the owner
//! state, after checking the transaction against what that state, the
//! request, the typed result and the caller's presence say it must
//! contain. It then compares its state with the port's actual
//! [`OwnerState`]. The expected facts come from the storage plan's
//! transaction tables, not from the facts themselves, so a producer that
//! skips a record or changes state without one fails here.
//!
//! The model assumes what this adapter guarantees: finality never leaves
//! the genesis anchor, so a walked path retains every header above its
//! common ancestor.

use std::collections::{BTreeMap, BTreeSet};

use zakura_header_chain::{Frontier, HeaderGeneration, HeaderWorkAuthority, SourceId};
use zakura_rt::time::Instant;

use super::{
    CallerPresence, CapacityClass, ClassWaiters, ContinuationState, LeaseFact, LeaseKey,
    LeaseState, OperationStage, OperationState, OwnerState, PathPosition, RequestIdentity,
    ResultIdentity, RetentionEnd, SignalClass, Waiter,
};
use crate::header::{
    controlled::{Completion, Fact, FactKind, OpId},
    lease::{
        LeaseEnd, MAX_HEADER_SYNC_HEIGHT_RANGE, MAX_RETAINED_PATH_LEASES, RETAINED_PATH_LEASE_IDLE,
    },
    Outcome, Release,
};

/// The first way the facts or the state disagreed with the model, in the
/// precedence [`OwnerModel::check_boundary`] checks them.
#[derive(Clone, Debug, Eq, PartialEq)]
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
    /// A fact, or the actual state, named an owner the model doesn't have.
    UnknownOwner {
        /// The transaction, or `None` for the state comparison.
        txn: Option<u64>,
        /// The owner.
        owner: Owner,
    },
    /// A transaction lacked a fact its pre-state and result require.
    MissingEffect {
        /// The transaction.
        txn: u64,
        /// The first missing fact.
        expected: FactKind,
    },
    /// A transaction held a fact, or a head, its pre-state doesn't allow.
    UnexpectedEffect {
        /// The transaction.
        txn: u64,
        /// The first unexpected fact.
        found: FactKind,
    },
    /// A transaction held the expected facts in another order.
    OrderViolation {
        /// The transaction.
        txn: u64,
    },
    /// The actual state reflects facts the model hasn't applied.
    Stale {
        /// The last fact the actual state reflects.
        through_seq: u64,
        /// The last fact the model applied.
        applied_seq: u64,
    },
    /// A completed operation's caller is gone but its reply is still held.
    UndisposedReply(OpId),
    /// A known owner's actual state differs from the model's.
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
    ops: BTreeMap<OpId, ModelOp>,
    permits: BTreeSet<OpId>,
    registry: ModelRegistry,
}

/// One operation the model knows.
#[derive(Clone, Debug)]
struct ModelOp {
    request: RequestIdentity,
    request_digest: [u8; 32],
    caller_present: bool,
    /// The stored reply's identity, once completed for a waiting caller.
    replied: Option<ResultIdentity>,
}

/// The model's lease registry.
#[derive(Clone, Debug, Default)]
struct ModelRegistry {
    leases: BTreeMap<SourceId, ModelLease>,
    continuations: BTreeMap<SourceId, ModelLease>,
    general_waiter: bool,
    fallback_waiter: bool,
    last_lease_id: u64,
    continuation_hits: u64,
}

/// One lease, live or cached for continuation.
#[derive(Clone, Debug)]
struct ModelLease {
    key: LeaseKey,
    scope: HeaderWorkAuthority,
    target: Frontier,
    last_frontier: Frontier,
    position: PathPosition,
    /// How many hashes its path retains above its common ancestor.
    retained: u64,
    capacity: CapacityClass,
    waiter: bool,
    leased_txn: u64,
    leased_at: Instant,
}

/// A slot class's capacity, as the registry counts it.
fn limit(class: CapacityClass) -> usize {
    match class {
        CapacityClass::General => MAX_RETAINED_PATH_LEASES.saturating_sub(1),
        CapacityClass::Bounded => MAX_RETAINED_PATH_LEASES,
    }
}

/// `Bounded` if the path from `ancestor` to `target` fits one page.
fn class_for_range(ancestor: Frontier, target: Frontier) -> CapacityClass {
    if target.height.0.saturating_sub(ancestor.height.0) <= MAX_HEADER_SYNC_HEIGHT_RANGE {
        CapacityClass::Bounded
    } else {
        CapacityClass::General
    }
}

impl ModelRegistry {
    /// How many live leases hold an ordinary slot.
    fn general_count(&self) -> usize {
        self.leases
            .values()
            .filter(|lease| lease.capacity == CapacityClass::General)
            .count()
    }

    /// Ends every live lease idle at `at`, in peer order.
    fn expire(&mut self, at: Instant, out: &mut Vec<LeaseFact>) {
        let expired: Vec<_> = self
            .leases
            .iter()
            .filter(|(_, lease)| lease.leased_at + RETAINED_PATH_LEASE_IDLE <= at)
            .map(|(peer, _)| *peer)
            .collect();
        for peer in expired {
            self.end_with_capability(peer, Release::LeaseIdleExpired, out);
        }
    }

    /// Ends `peer`'s lease and its retained path together.
    fn end_with_capability(&mut self, peer: SourceId, release: Release, out: &mut Vec<LeaseFact>) {
        if let Some(lease) = self.remove(peer, release, out) {
            out.push(LeaseFact::RetentionEnded {
                lease: lease.key,
                why: RetentionEnd::WithCapability,
            });
        }
    }

    /// Ends `peer`'s capability, fires its waiter and any class with room.
    fn remove(
        &mut self,
        peer: SourceId,
        release: Release,
        out: &mut Vec<LeaseFact>,
    ) -> Option<ModelLease> {
        let lease = self.leases.remove(&peer)?;
        out.push(LeaseFact::CapabilityEnded {
            lease: lease.key,
            release,
        });
        if lease.waiter {
            out.push(LeaseFact::WaiterFired(Waiter::Peer(lease.key)));
        }
        self.notify_capacity(out);
        Some(lease)
    }

    /// Fires each class waiter whose class has room.
    fn notify_capacity(&mut self, out: &mut Vec<LeaseFact>) {
        if self.general_count() < limit(CapacityClass::General) && self.general_waiter {
            self.general_waiter = false;
            out.push(LeaseFact::WaiterFired(Waiter::General));
        }
        if self.leases.len() < limit(CapacityClass::Bounded) && self.fallback_waiter {
            self.fallback_waiter = false;
            out.push(LeaseFact::WaiterFired(Waiter::Fallback));
        }
    }

    /// Sets a class waiter, recording it if it wasn't set.
    fn set_waiter(&mut self, waiter: Waiter, out: &mut Vec<LeaseFact>) {
        let set = match waiter {
            Waiter::General => &mut self.general_waiter,
            Waiter::Fallback => &mut self.fallback_waiter,
            Waiter::Peer(lease) => {
                &mut self
                    .leases
                    .get_mut(&lease.peer)
                    .expect("a peer waiter is set on a live lease")
                    .waiter
            }
        };
        if !*set {
            *set = true;
            out.push(LeaseFact::WaiterSet(waiter));
        }
    }

    /// The key a new lease for `peer` in `session_id` gets, or `None` once
    /// lease IDs are exhausted.
    fn next_key(&self, peer: SourceId, session_id: u64) -> Option<LeaseKey> {
        Some(LeaseKey {
            peer,
            session_id,
            lease_id: self.last_lease_id.checked_add(1)?,
        })
    }

    /// Installs `lease`, recording it.
    fn lease(&mut self, lease: ModelLease, continued_from: Option<u64>, out: &mut Vec<LeaseFact>) {
        self.last_lease_id = lease.key.lease_id;
        out.push(LeaseFact::Leased {
            lease: lease.key,
            scope: lease.scope,
            target: lease.target,
            common_ancestor: lease.last_frontier,
            position: lease.position,
            capacity: lease.capacity,
            continued_from,
        });
        self.leases.insert(lease.key.peer, lease);
    }

    /// Whether `key` names a live lease the registry holds.
    fn knows_lease(&self, key: &LeaseKey) -> bool {
        self.leases
            .get(&key.peer)
            .is_some_and(|lease| lease.key == *key)
    }

    /// Whether `key` names a continuation the registry holds.
    fn knows_continuation(&self, key: &LeaseKey) -> bool {
        self.continuations
            .get(&key.peer)
            .is_some_and(|cached| cached.key == *key)
    }
}

/// What one transaction is checked against, beside the model's state.
struct Txn<'a> {
    txn: u64,
    at: Instant,
    facts: &'a [Fact],
}

/// Why a transaction's head doesn't fit the model, before its facts are
/// compared.
enum HeadError {
    /// The head names an operation the model doesn't have.
    Unknown(Owner),
    /// The head isn't legal for the model's state.
    Unexpected,
}

impl OwnerModel {
    /// The model of a pristine port.
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies `facts`, the next facts the port published, after checking
    /// that they continue the facts applied so far and that each
    /// transaction holds exactly the facts its pre-state requires. Stops at
    /// the first failure, leaving the transactions before it applied.
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
            let txn = Txn {
                txn: last.txn,
                at: last.at,
                facts: group,
            };
            self.apply_txn(&txn)?;
            self.applied_seq = last.seq;
            self.applied_txn = last.txn;
        }
        Ok(())
    }

    /// Checks one transaction and applies it on success.
    fn apply_txn(&mut self, txn: &Txn<'_>) -> Result<(), ModelFailure> {
        let heads = txn.facts.iter().filter(|fact| fact.kind.is_head()).count();
        let head = &txn.facts.last().expect("a transaction holds a fact").kind;
        if heads != 1 || !head.is_head() {
            return Err(ModelFailure::MalformedTxn { txn: txn.txn });
        }
        let actual: Vec<_> = txn.facts[..txn.facts.len() - 1]
            .iter()
            .map(|fact| fact.kind.clone())
            .collect();
        let mut next = self.clone();
        let expected = match next.expect(txn, head) {
            Ok(expected) => expected,
            Err(HeadError::Unknown(owner)) => {
                return Err(ModelFailure::UnknownOwner {
                    txn: Some(txn.txn),
                    owner,
                })
            }
            Err(HeadError::Unexpected) => {
                // An unknown owner outranks the illegal head.
                self.unknown_owners(txn.txn, &actual, &actual)?;
                return Err(ModelFailure::UnexpectedEffect {
                    txn: txn.txn,
                    found: head.clone(),
                });
            }
        };
        self.compare_effects(txn.txn, &expected, &actual)?;
        // Checked once the facts match, so a missing end is reported as the
        // missing fact rather than as a head that disagrees with it.
        if let FactKind::Completed(completion) = head {
            let ends: Vec<_> = actual
                .iter()
                .filter_map(|fact| match fact {
                    FactKind::Lease(fact) => LeaseEnd::of(fact),
                    _ => None,
                })
                .collect();
            if completion.lease_ends != ends {
                return Err(ModelFailure::UnexpectedEffect {
                    txn: txn.txn,
                    found: head.clone(),
                });
            }
        }
        *self = next;
        Ok(())
    }

    /// Compares a transaction's actual non-head facts with the expected
    /// ones: an unknown owner first, then a missing fact, an extra fact, and
    /// finally the order.
    fn compare_effects(
        &self,
        txn: u64,
        expected: &[FactKind],
        actual: &[FactKind],
    ) -> Result<(), ModelFailure> {
        let mut missing: Vec<&FactKind> = expected.iter().collect();
        let mut extra = Vec::new();
        for fact in actual {
            match missing.iter().position(|want| *want == fact) {
                Some(index) => {
                    missing.remove(index);
                }
                None => extra.push(fact),
            }
        }
        self.unknown_owners(txn, actual, extra.iter().copied())?;
        if let Some(fact) = missing.first() {
            return Err(ModelFailure::MissingEffect {
                txn,
                expected: (*fact).clone(),
            });
        }
        if let Some(fact) = extra.first() {
            return Err(ModelFailure::UnexpectedEffect {
                txn,
                found: (*fact).clone(),
            });
        }
        if expected != actual {
            return Err(ModelFailure::OrderViolation { txn });
        }
        Ok(())
    }

    /// Fails at the first of `scanned`, facts of transaction `txn`, that
    /// names an owner the model lacks. A lease any of the transaction's
    /// `actual` non-head facts creates isn't unknown.
    fn unknown_owners<'f>(
        &self,
        txn: u64,
        actual: &[FactKind],
        scanned: impl IntoIterator<Item = &'f FactKind>,
    ) -> Result<(), ModelFailure> {
        let created: Vec<LeaseKey> = actual
            .iter()
            .filter_map(|fact| match fact {
                FactKind::Lease(LeaseFact::Leased { lease, .. }) => Some(*lease),
                _ => None,
            })
            .collect();
        match scanned
            .into_iter()
            .find_map(|fact| self.unknown_owner(fact, &created))
        {
            Some(owner) => Err(ModelFailure::UnknownOwner {
                txn: Some(txn),
                owner,
            }),
            None => Ok(()),
        }
    }

    /// The owner `fact` names that the model lacks, if any. A lease the
    /// same transaction created isn't unknown.
    fn unknown_owner(&self, fact: &FactKind, created: &[LeaseKey]) -> Option<Owner> {
        let lease = match fact {
            FactKind::PermitDropped { id } if !self.permits.contains(id) => {
                return Some(Owner::Permit(*id))
            }
            FactKind::Lease(
                LeaseFact::CapabilityEnded { lease, .. }
                | LeaseFact::RetentionCached { lease, .. }
                | LeaseFact::WaiterSet(Waiter::Peer(lease))
                | LeaseFact::WaiterFired(Waiter::Peer(lease))
                | LeaseFact::RetentionEnded {
                    lease,
                    why: RetentionEnd::WithCapability,
                },
            ) => lease,
            // Every other retention end names a cached continuation.
            FactKind::Lease(LeaseFact::RetentionEnded { lease, .. }) => {
                return (!self.registry.knows_continuation(lease))
                    .then_some(Owner::Continuation(*lease))
            }
            _ => return None,
        };
        (!self.registry.knows_lease(lease) && !created.contains(lease))
            .then_some(Owner::Lease(*lease))
    }

    /// The non-head facts `head`'s transaction must hold, applying the
    /// transaction to `self` as it goes.
    fn expect(&mut self, txn: &Txn<'_>, head: &FactKind) -> Result<Vec<FactKind>, HeadError> {
        let mut lease_facts = Vec::new();
        let mut kinds = Vec::new();
        match head {
            FactKind::Submitted {
                id,
                request,
                request_digest,
            } => {
                if self.shut_down || id.0 != self.last_op.wrapping_add(1) {
                    return Err(HeadError::Unexpected);
                }
                self.last_op = id.0;
                if *request == RequestIdentity::ContinuationLocator {
                    self.permits.insert(*id);
                    kinds.push(FactKind::PermitTaken { id: *id });
                }
                self.ops.insert(
                    *id,
                    ModelOp {
                        request: request.clone(),
                        request_digest: *request_digest,
                        caller_present: true,
                        replied: None,
                    },
                );
            }
            FactKind::CallerDropped { id } => {
                let op = self.op_mut(*id)?;
                if op.replied.is_some() || !op.caller_present {
                    return Err(HeadError::Unexpected);
                }
                op.caller_present = false;
            }
            FactKind::ReplyTaken { id } => {
                if self.op_mut(*id)?.replied.is_none() {
                    return Err(HeadError::Unexpected);
                }
                self.ops.remove(id);
            }
            FactKind::ReplyDropped { id } => {
                let result = self
                    .op_mut(*id)?
                    .replied
                    .clone()
                    .ok_or(HeadError::Unexpected)?;
                self.ops.remove(id);
                self.abandon(&result, &mut lease_facts);
            }
            FactKind::Completed(completion) => {
                self.complete(txn, completion, &mut kinds, &mut lease_facts)?;
            }
            FactKind::ShutDown { dropped } => {
                if let Some(id) = dropped.iter().find(|id| !self.ops.contains_key(id)) {
                    return Err(HeadError::Unknown(Owner::Operation(*id)));
                }
                if self.shut_down || *dropped != self.ops.keys().copied().collect::<Vec<_>>() {
                    return Err(HeadError::Unexpected);
                }
                self.shut_down = true;
                self.ops.clear();
                for id in std::mem::take(&mut self.permits) {
                    kinds.push(FactKind::PermitDropped { id });
                }
                let peers: Vec<_> = self.registry.leases.keys().copied().collect();
                for peer in peers {
                    self.registry
                        .end_with_capability(peer, Release::Shutdown, &mut lease_facts);
                }
                for (_, lease) in std::mem::take(&mut self.registry.continuations) {
                    lease_facts.push(LeaseFact::RetentionEnded {
                        lease: lease.key,
                        why: RetentionEnd::ShutdownCleared,
                    });
                }
            }
            FactKind::PermitTaken { .. } | FactKind::PermitDropped { .. } | FactKind::Lease(_) => {
                unreachable!("the caller checked that the last fact is a head")
            }
        }
        kinds.extend(lease_facts.into_iter().map(FactKind::Lease));
        Ok(kinds)
    }

    /// The operation `id`, or the head error for an unknown one.
    fn op_mut(&mut self, id: OpId) -> Result<&mut ModelOp, HeadError> {
        self.ops
            .get_mut(&id)
            .ok_or(HeadError::Unknown(Owner::Operation(id)))
    }

    /// Checks `completion` against its pending operation and applies it:
    /// the permit's drop, the execute group, then for a caller that's gone
    /// the abandoned reply's group. The slot is removed or holds the reply.
    fn complete(
        &mut self,
        txn: &Txn<'_>,
        completion: &Completion,
        kinds: &mut Vec<FactKind>,
        lease_facts: &mut Vec<LeaseFact>,
    ) -> Result<(), HeadError> {
        let id = completion.id;
        let op = self.op_mut(id)?.clone();
        if op.replied.is_some()
            || completion.txn != txn.txn
            || completion.request != op.request
            || completion.op != op.request.op()
            || completion.request_digest != op.request_digest
            || completion.delivered != op.caller_present
        {
            return Err(HeadError::Unexpected);
        }
        if self.permits.remove(&id) {
            kinds.push(FactKind::PermitDropped { id });
        }
        let result = &completion.result;
        match completion.outcome {
            Outcome::Fault(fault) if *result == ResultIdentity::Fault(fault) => {}
            Outcome::Rejected if *result == ResultIdentity::Rejected => {}
            Outcome::Engine => {
                self.execute(txn, &op.request, result, completion.generation, lease_facts)?
            }
            Outcome::Fault(_) | Outcome::Rejected => return Err(HeadError::Unexpected),
        }
        if completion.delivered {
            self.ops
                .get_mut(&id)
                .expect("the operation was found above")
                .replied = Some(result.clone());
        } else {
            self.ops.remove(&id);
            self.abandon(result, lease_facts);
        }
        Ok(())
    }

    /// Ends the lease an abandoned `Acquired` reply carries, if it's live.
    /// Other replies own nothing.
    fn abandon(&mut self, result: &ResultIdentity, out: &mut Vec<LeaseFact>) {
        if let ResultIdentity::Acquired { lease, .. } = result {
            if self
                .registry
                .leases
                .get(&lease.peer)
                .is_some_and(|live| live.key == *lease)
            {
                self.registry
                    .end_with_capability(lease.peer, Release::LeaseAbandonedAcquire, out);
            }
        }
    }

    /// The lease facts the engine outcome of `request` must record, given
    /// the typed `result`, applied to the registry. Engine-dependent steps
    /// accept the result's class and require only the facts it implies.
    fn execute(
        &mut self,
        txn: &Txn<'_>,
        request: &RequestIdentity,
        result: &ResultIdentity,
        generation: HeaderGeneration,
        out: &mut Vec<LeaseFact>,
    ) -> Result<(), HeadError> {
        match request {
            RequestIdentity::AcquireHeaderPath {
                peer,
                session_id,
                scope,
                target_tip_hash,
                locators,
            } => self.acquire(
                txn,
                Acquire {
                    peer: *peer,
                    session_id: *session_id,
                    scope: *scope,
                    target_tip_hash: *target_tip_hash,
                    locators,
                    generation,
                },
                result,
                out,
            ),
            RequestIdentity::ReadHeaderPath {
                lease,
                scope,
                after_hash,
                max_header_count,
            } => {
                let Some(lease) = lease else {
                    return expect_result(result, &ResultIdentity::PortError);
                };
                if *max_header_count == 0 || *max_header_count > MAX_HEADER_SYNC_HEIGHT_RANGE {
                    return expect_result(result, &ResultIdentity::PortError);
                }
                self.registry.expire(txn.at, out);
                let Some(live) = self
                    .registry
                    .leases
                    .get(&lease.peer)
                    .filter(|live| live.key == *lease)
                    .cloned()
                else {
                    return expect_result(result, &ResultIdentity::PageUnavailable);
                };
                if live.scope != *scope || *after_hash != live.last_frontier.hash {
                    return expect_result(result, &ResultIdentity::PageUnavailable);
                }
                match result {
                    // A retired header or an incoherent engine keeps the lease.
                    ResultIdentity::PageUnavailable | ResultIdentity::PortError => Ok(()),
                    ResultIdentity::Page {
                        last,
                        count,
                        complete,
                    } => self.finish_page(live, *last, *count, *complete, *max_header_count, out),
                    _ => Err(HeadError::Unexpected),
                }
            }
            RequestIdentity::ReleaseHeaderPath { lease, scope } => {
                let Some(lease) = lease else {
                    return expect_result(result, &ResultIdentity::PortError);
                };
                if self
                    .registry
                    .leases
                    .get(&lease.peer)
                    .is_some_and(|live| live.key == *lease && live.scope == *scope)
                {
                    self.registry
                        .end_with_capability(lease.peer, Release::LeaseReleased, out);
                }
                expect_result(result, &ResultIdentity::Released)
            }
            // These leave the registry alone, and their results depend only
            // on the engine.
            RequestIdentity::ContinuationLocator
            | RequestIdentity::VctRepairContext { .. }
            | RequestIdentity::PrepareHeaderTarget { .. }
            | RequestIdentity::ApplyHeaderTarget { .. }
            | RequestIdentity::SelectedChainContains { .. } => Ok(()),
        }
    }

    /// Checks and applies a served page of `live`: its capability ends, and
    /// its path ends with it if the page completed it, or else moves into
    /// the continuation cache, evicting the oldest entry if it's full.
    fn finish_page(
        &mut self,
        mut live: ModelLease,
        last: Frontier,
        count: u32,
        complete: bool,
        max_header_count: u32,
        out: &mut Vec<LeaseFact>,
    ) -> Result<(), HeadError> {
        let next = match live.position {
            PathPosition::Retained { next } => next,
            PathPosition::Complete => live.retained,
        };
        let served = next.saturating_add(u64::from(count));
        let plausible = count <= max_header_count
            && served <= live.retained
            && complete == (served == live.retained)
            && u64::from(last.height.0)
                == u64::from(live.last_frontier.height.0).saturating_add(u64::from(count));
        if !plausible {
            return Err(HeadError::Unexpected);
        }
        let peer = live.key.peer;
        self.registry
            .remove(peer, Release::LeaseConsumedByRead, out)
            .expect("the live lease was found above");
        if complete {
            out.push(LeaseFact::RetentionEnded {
                lease: live.key,
                why: RetentionEnd::WithCapability,
            });
            return Ok(());
        }
        if self.registry.continuations.len() >= MAX_RETAINED_PATH_LEASES {
            let oldest = self
                .registry
                .continuations
                .values()
                .min_by_key(|cached| cached.key.lease_id)
                .map(|cached| cached.key)
                .expect("a full continuation cache is nonempty");
            self.registry.continuations.remove(&oldest.peer);
            out.push(LeaseFact::RetentionEnded {
                lease: oldest,
                why: RetentionEnd::Evicted,
            });
        }
        live.last_frontier = last;
        live.position = PathPosition::Retained { next: served };
        live.waiter = false;
        out.push(LeaseFact::RetentionCached {
            lease: live.key,
            last_frontier: last,
            position: live.position,
        });
        self.registry.continuations.insert(peer, live);
        Ok(())
    }

    /// Checks and applies an acquire: the sweep, admission, scope check and
    /// continuation take from the model's state, then the walk's result
    /// class.
    fn acquire(
        &mut self,
        txn: &Txn<'_>,
        request: Acquire<'_>,
        result: &ResultIdentity,
        out: &mut Vec<LeaseFact>,
    ) -> Result<(), HeadError> {
        let Acquire {
            peer,
            session_id,
            scope,
            target_tip_hash,
            locators,
            generation,
        } = request;
        if locators.is_empty() || locators.len() > zakura_header_chain::MAX_HEADER_LOCATOR_HASHES {
            return expect_result(result, &ResultIdentity::PortError);
        }
        let registry = &mut self.registry;
        registry.expire(txn.at, out);
        if let Some(held) = registry.leases.get(&peer).map(|lease| lease.key) {
            registry.set_waiter(Waiter::Peer(held), out);
            return expect_result(result, &ResultIdentity::CapacityBusy(SignalClass::Peer));
        }
        let class = if registry.general_count() < limit(CapacityClass::General) {
            CapacityClass::General
        } else if registry.leases.len() < limit(CapacityClass::Bounded) {
            CapacityClass::Bounded
        } else {
            registry.set_waiter(Waiter::Fallback, out);
            return expect_result(result, &ResultIdentity::CapacityBusy(SignalClass::Fallback));
        };
        if scope.branch.target_tip_hash != target_tip_hash || scope.header_generation > generation {
            return expect_result(result, &ResultIdentity::Busy);
        }
        let new_lease = |registry: &ModelRegistry, common_ancestor, target, position, retained| {
            registry.next_key(peer, session_id).map(|key| ModelLease {
                key,
                scope,
                target,
                last_frontier: common_ancestor,
                position,
                retained,
                capacity: class,
                waiter: false,
                leased_txn: txn.txn,
                leased_at: txn.at,
            })
        };
        if let Some(cached) = registry.continuations.remove(&peer) {
            let continues = cached.key.session_id == session_id
                && cached.target.hash == target_tip_hash
                && locators == [cached.last_frontier.hash]
                && (class == CapacityClass::General
                    || class_for_range(cached.last_frontier, cached.target)
                        == CapacityClass::Bounded);
            if continues {
                registry.continuation_hits = registry.continuation_hits.saturating_add(1);
                let discarded = LeaseFact::RetentionEnded {
                    lease: cached.key,
                    why: RetentionEnd::Discarded,
                };
                return match result {
                    ResultIdentity::TargetNotRetained => {
                        out.push(discarded);
                        Ok(())
                    }
                    ResultIdentity::Busy if registry.next_key(peer, session_id).is_none() => {
                        out.push(discarded);
                        Ok(())
                    }
                    ResultIdentity::Acquired {
                        lease,
                        common_ancestor,
                        target,
                    } => {
                        let Some(next) = new_lease(
                            registry,
                            cached.last_frontier,
                            cached.target,
                            cached.position,
                            cached.retained,
                        ) else {
                            return Err(HeadError::Unexpected);
                        };
                        if *lease != next.key
                            || *common_ancestor != cached.last_frontier
                            || *target != cached.target
                        {
                            return Err(HeadError::Unexpected);
                        }
                        out.push(LeaseFact::RetentionEnded {
                            lease: cached.key,
                            why: RetentionEnd::Continued {
                                into: next.key.lease_id,
                            },
                        });
                        registry.lease(next, Some(cached.key.lease_id), out);
                        Ok(())
                    }
                    _ => Err(HeadError::Unexpected),
                };
            }
            out.push(LeaseFact::RetentionEnded {
                lease: cached.key,
                why: RetentionEnd::Discarded,
            });
        }
        match result {
            ResultIdentity::TargetNotRetained
            | ResultIdentity::HistoryPruned
            | ResultIdentity::NoLocatorIntersection
            | ResultIdentity::PortError => Ok(()),
            ResultIdentity::CapacityBusy(SignalClass::General)
                if class == CapacityClass::Bounded =>
            {
                registry.set_waiter(Waiter::General, out);
                registry.notify_capacity(out);
                Ok(())
            }
            ResultIdentity::Busy if registry.next_key(peer, session_id).is_none() => Ok(()),
            ResultIdentity::Acquired {
                lease,
                common_ancestor,
                target,
            } => {
                let position = if common_ancestor == target {
                    PathPosition::Complete
                } else {
                    PathPosition::Retained { next: 0 }
                };
                let retained =
                    u64::from(target.height.0).saturating_sub(u64::from(common_ancestor.height.0));
                let Some(next) = new_lease(registry, *common_ancestor, *target, position, retained)
                else {
                    return Err(HeadError::Unexpected);
                };
                if *lease != next.key || target.hash != target_tip_hash {
                    return Err(HeadError::Unexpected);
                }
                registry.lease(next, None, out);
                Ok(())
            }
            _ => Err(HeadError::Unexpected),
        }
    }

    /// Compares the model with the port's `actual` state: freshness, then
    /// owners the model lacks, then an undisposed reply, then each field.
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
                    .find(|lease| !self.registry.knows_lease(&lease.lease))
                    .map(|lease| Owner::Lease(lease.lease))
            })
            .or_else(|| {
                actual
                    .continuations
                    .iter()
                    .find(|cached| !self.registry.knows_continuation(&cached.lease))
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
            continuation_hits,
        } = self.state();
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
                continuation_hits == actual.continuation_hits,
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

    /// The owner state the model expects, in [`OwnerState`]'s form.
    pub fn state(&self) -> OwnerState {
        let registry = &self.registry;
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
            leases: registry
                .leases
                .values()
                .map(|lease| LeaseState {
                    lease: lease.key,
                    scope: lease.scope,
                    target: lease.target,
                    last_frontier: lease.last_frontier,
                    position: lease.position,
                    capacity: lease.capacity,
                    waiter: lease.waiter,
                    leased_txn: Some(lease.leased_txn),
                })
                .collect(),
            continuations: registry
                .continuations
                .values()
                .map(|cached| ContinuationState {
                    lease: cached.key,
                    scope: cached.scope,
                    target: cached.target,
                    last_frontier: cached.last_frontier,
                    position: cached.position,
                    retained: cached.retained,
                })
                .collect(),
            class_waiters: ClassWaiters {
                general: registry.general_waiter,
                fallback: registry.fallback_waiter,
            },
            last_lease_id: registry.last_lease_id,
            continuation_hits: registry.continuation_hits,
        }
    }
}

/// An acquire's request, with the engine generation its completion saw.
struct Acquire<'a> {
    peer: SourceId,
    session_id: u64,
    scope: HeaderWorkAuthority,
    target_tip_hash: zakura_chain::block::Hash,
    locators: &'a [zakura_chain::block::Hash],
    generation: HeaderGeneration,
}

/// `Ok` if `result` is `expected`, the only result the model allows.
fn expect_result(result: &ResultIdentity, expected: &ResultIdentity) -> Result<(), HeadError> {
    if result == expected {
        Ok(())
    } else {
        Err(HeadError::Unexpected)
    }
}
