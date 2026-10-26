//! Typed evidence about the controlled header port: whose work each
//! operation is and what it returned.
//!
//! Properties compare these values directly, so each keeps the full typed
//! owner and every identity the request or result names. Digests remain for
//! replay, not for comparison.

use zakura_chain::block;
use zakura_header_chain::{
    BodyWorkOwner, ErrorCategory, Frontier, HeaderGeneration, HeaderSyncWorkOwner,
    HeaderWorkAuthority, SourceId,
};
use zakura_node_services::header_chain::{
    AcquirePathReply, AdapterKey, ApplyHeaderTargetOutcome, ReadPathReply, RetainedHeaderPath,
    SelectedChainMembership, VctRepairContextReply,
};

use super::{controlled::OpId, Fault, HeaderOp, HeaderReply, HeaderRequest};

/// One retained-path lease: the peer holding it, that peer's session, and
/// the lease ID the registry issued.
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LeaseKey {
    /// The peer.
    pub peer: SourceId,
    /// The peer's session.
    pub session_id: u64,
    /// The registry's lease ID.
    pub lease_id: u64,
}

impl LeaseKey {
    /// The lease `path` names, if the adapter holding `key` issued it.
    pub(crate) fn of(path: &RetainedHeaderPath, key: &AdapterKey) -> Option<Self> {
        path.adapter_identity(key)
            .map(|(lease_id, peer, session_id)| LeaseKey {
                peer,
                session_id,
                lease_id,
            })
    }
}

/// What an operation asked for, with its owner where its contract gives it
/// one.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RequestIdentity {
    /// `continuation_locator`. Its only owned input is the permit, which
    /// isn't an owner.
    ContinuationLocator,
    /// `vct_repair_context`.
    VctRepairContext {
        /// The repair owner.
        owner: BodyWorkOwner,
        /// The repaired height.
        height: block::Height,
    },
    /// `acquire_header_path`.
    AcquireHeaderPath {
        /// The serving peer.
        peer: SourceId,
        /// Its session.
        session_id: u64,
        /// The work scope the lease is for.
        scope: HeaderWorkAuthority,
        /// The target.
        target_tip_hash: block::Hash,
        /// The locator, nearest first.
        locators: Vec<block::Hash>,
    },
    /// `read_header_path`.
    ReadHeaderPath {
        /// The lease the handle names, or `None` for another adapter's
        /// handle.
        lease: Option<LeaseKey>,
        /// The handle's scope.
        scope: HeaderWorkAuthority,
        /// Where the page starts.
        after_hash: block::Hash,
        /// The most headers asked for.
        max_header_count: u32,
    },
    /// `release_header_path`.
    ReleaseHeaderPath {
        /// The lease the handle names, or `None` for another adapter's
        /// handle.
        lease: Option<LeaseKey>,
        /// The handle's scope.
        scope: HeaderWorkAuthority,
    },
    /// `prepare_header_target`.
    PrepareHeaderTarget {
        /// The work owner.
        owner: HeaderSyncWorkOwner,
        /// The header supplier.
        source: SourceId,
        /// The parent of the first header.
        common_ancestor: Frontier,
        /// The last header.
        target: Frontier,
        /// How many headers.
        headers: u32,
    },
    /// `apply_header_target`.
    ApplyHeaderTarget {
        /// The work owner.
        owner: HeaderSyncWorkOwner,
        /// The header supplier.
        source: SourceId,
        /// The parent the sealed insertion starts from, or `None` for
        /// another adapter's target, whose insertion this port can't open.
        parent_hash: Option<block::Hash>,
        /// The target.
        target_tip_hash: block::Hash,
    },
    /// `selected_chain_contains`.
    SelectedChainContains {
        /// The header asked about.
        hash: block::Hash,
    },
}

impl RequestIdentity {
    /// The identity of `request`, for the adapter holding `key`. An apply's
    /// parent is sealed in its target, so the caller that opened the target
    /// passes it as `apply_parent`. Other requests ignore it.
    pub(crate) fn of(
        request: &HeaderRequest,
        key: &AdapterKey,
        apply_parent: Option<block::Hash>,
    ) -> Self {
        match request {
            HeaderRequest::ContinuationLocator => RequestIdentity::ContinuationLocator,
            HeaderRequest::VctRepairContext { owner, height } => {
                RequestIdentity::VctRepairContext {
                    owner: *owner,
                    height: *height,
                }
            }
            HeaderRequest::AcquireHeaderPath(acquire) => RequestIdentity::AcquireHeaderPath {
                peer: acquire.source,
                session_id: acquire.session_id,
                scope: acquire.scope,
                target_tip_hash: acquire.target_tip_hash,
                locators: acquire.locator_hashes.clone(),
            },
            HeaderRequest::ReadHeaderPath { path, request } => RequestIdentity::ReadHeaderPath {
                lease: LeaseKey::of(path, key),
                scope: path.scope,
                after_hash: request.after_hash,
                max_header_count: request.max_header_count,
            },
            HeaderRequest::ReleaseHeaderPath { path } => RequestIdentity::ReleaseHeaderPath {
                lease: LeaseKey::of(path, key),
                scope: path.scope,
            },
            HeaderRequest::PrepareHeaderTarget(prepare) => RequestIdentity::PrepareHeaderTarget {
                owner: prepare.owner,
                source: prepare.source,
                common_ancestor: prepare.common_ancestor,
                target: prepare.target,
                // A request holds at most the transition cap, far below u32.
                headers: u32::try_from(prepare.entries.len()).unwrap_or(u32::MAX),
            },
            HeaderRequest::ApplyHeaderTarget { target } => RequestIdentity::ApplyHeaderTarget {
                owner: target.owner(),
                source: target.source(),
                parent_hash: apply_parent,
                target_tip_hash: target.target_tip_hash(),
            },
            HeaderRequest::SelectedChainContains { hash } => {
                RequestIdentity::SelectedChainContains { hash: *hash }
            }
        }
    }

    /// The operation.
    pub fn op(&self) -> HeaderOp {
        match self {
            RequestIdentity::ContinuationLocator => HeaderOp::ContinuationLocator,
            RequestIdentity::VctRepairContext { .. } => HeaderOp::VctRepairContext,
            RequestIdentity::AcquireHeaderPath { .. } => HeaderOp::AcquireHeaderPath,
            RequestIdentity::ReadHeaderPath { .. } => HeaderOp::ReadHeaderPath,
            RequestIdentity::ReleaseHeaderPath { .. } => HeaderOp::ReleaseHeaderPath,
            RequestIdentity::PrepareHeaderTarget { .. } => HeaderOp::PrepareHeaderTarget,
            RequestIdentity::ApplyHeaderTarget { .. } => HeaderOp::ApplyHeaderTarget,
            RequestIdentity::SelectedChainContains { .. } => HeaderOp::SelectedChainContains,
        }
    }
}

/// Which capacity signal a `CapacityBusy` refusal handed out, read from the
/// registry when the refusal was made.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub enum SignalClass {
    /// The refused peer's own lease's signal: the peer already held a lease.
    Peer,
    /// The ordinary class's signal: the path needed an ordinary slot and
    /// only the reserve slot was free (`BeyondReserve`).
    General,
    /// The reserve class's signal: every slot was taken (`CapacityFull`).
    Fallback,
}

/// What an operation returned, as typed identities. Captured when it
/// completes and kept with an unread reply.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResultIdentity {
    /// The run chose this fault.
    Fault(Fault),
    /// The adapter rejected the request before running it.
    Rejected,
    /// The engine path returned a `PortError`.
    PortError,
    /// A locator for the selected path.
    Locator {
        /// Its first entry, the selected tip.
        tip: Frontier,
        /// The header generation it was read at.
        header_generation: HeaderGeneration,
        /// The finalized anchor it was read at.
        finalized: Frontier,
    },
    /// No locator, which the engine port never gives.
    NoLocator,
    /// The VCT repair context resolved.
    VctResolved,
    /// The VCT repair context was stale.
    VctStale,
    /// A leased path.
    Acquired {
        /// The new lease.
        lease: LeaseKey,
        /// Where its path starts.
        common_ancestor: Frontier,
        /// Its target.
        target: Frontier,
    },
    /// `CapacityBusy`, with the signal it handed out.
    CapacityBusy(SignalClass),
    /// `Busy`.
    Busy,
    /// `TargetNotRetained`.
    TargetNotRetained,
    /// `NoLocatorIntersection`.
    NoLocatorIntersection,
    /// `HistoryPruned`.
    HistoryPruned,
    /// A served page.
    Page {
        /// The last header served, or the page's start if it served none.
        last: Frontier,
        /// How many headers.
        count: u32,
        /// Whether it reached the target.
        complete: bool,
    },
    /// The read's `Unavailable` reply.
    PageUnavailable,
    /// A release, which always succeeds for this adapter's handles.
    Released,
    /// A sealed target.
    Prepared,
    /// A failed preparation.
    PrepareFailed(ErrorCategory),
    /// `Applied`.
    Applied,
    /// `ResourceStalled`.
    ResourceStalled,
    /// A failed apply.
    ApplyFailed(ErrorCategory),
    /// The header is on the selected chain here.
    Selected(Frontier),
    /// The header is retained but not selected.
    NotSelected,
    /// The header isn't retained.
    NotRetained,
}

impl ResultIdentity {
    /// The identity of an engine `reply`, for the adapter holding `key`.
    /// A locator's generation and anchor and a refusal's signal class aren't
    /// in the reply, so the caller passes what it read when the reply was
    /// made.
    pub(crate) fn of(
        reply: &HeaderReply,
        key: &AdapterKey,
        locator_read: Option<(HeaderGeneration, Frontier)>,
        refusal: Option<SignalClass>,
    ) -> Self {
        match reply {
            HeaderReply::ContinuationLocator(Ok(Some(locator))) => {
                let (header_generation, finalized) =
                    locator_read.expect("the adapter reads a locator's generation with it");
                ResultIdentity::Locator {
                    tip: *locator
                        .entries()
                        .first()
                        .expect("a selected-path locator is never empty"),
                    header_generation,
                    finalized,
                }
            }
            HeaderReply::ContinuationLocator(Ok(None)) => ResultIdentity::NoLocator,
            HeaderReply::VctRepairContext(Ok(VctRepairContextReply::Resolved(_))) => {
                ResultIdentity::VctResolved
            }
            HeaderReply::VctRepairContext(Ok(VctRepairContextReply::Stale)) => {
                ResultIdentity::VctStale
            }
            HeaderReply::AcquireHeaderPath(Ok(reply)) => match reply {
                AcquirePathReply::Acquired(path) => ResultIdentity::Acquired {
                    lease: LeaseKey::of(path, key).expect("the adapter seals its own paths"),
                    common_ancestor: path.common_ancestor,
                    target: path.target,
                },
                AcquirePathReply::CapacityBusy(_) => ResultIdentity::CapacityBusy(
                    refusal.expect("the adapter classifies each refusal it makes"),
                ),
                AcquirePathReply::Busy => ResultIdentity::Busy,
                AcquirePathReply::TargetNotRetained => ResultIdentity::TargetNotRetained,
                AcquirePathReply::NoLocatorIntersection => ResultIdentity::NoLocatorIntersection,
                AcquirePathReply::HistoryPruned => ResultIdentity::HistoryPruned,
            },
            HeaderReply::ReadHeaderPath(Ok(ReadPathReply::Page(page))) => {
                // A page holds at most `MAX_HEADER_SYNC_HEIGHT_RANGE` headers.
                let count = u32::try_from(page.headers.len()).unwrap_or(u32::MAX);
                let last = page.headers.last().map_or(page.common_ancestor, |header| {
                    Frontier::new(
                        block::Height(page.common_ancestor.height.0.saturating_add(count)),
                        header.hash(),
                    )
                });
                ResultIdentity::Page {
                    last,
                    count,
                    complete: page.complete,
                }
            }
            HeaderReply::ReadHeaderPath(Ok(ReadPathReply::Unavailable)) => {
                ResultIdentity::PageUnavailable
            }
            HeaderReply::ReleaseHeaderPath(Ok(())) => ResultIdentity::Released,
            HeaderReply::PrepareHeaderTarget(Ok(_)) => ResultIdentity::Prepared,
            HeaderReply::PrepareHeaderTarget(Err(error)) => {
                ResultIdentity::PrepareFailed(error.category)
            }
            HeaderReply::ApplyHeaderTarget(Ok(ApplyHeaderTargetOutcome::Applied)) => {
                ResultIdentity::Applied
            }
            HeaderReply::ApplyHeaderTarget(Ok(ApplyHeaderTargetOutcome::ResourceStalled(_))) => {
                ResultIdentity::ResourceStalled
            }
            HeaderReply::ApplyHeaderTarget(Err(error)) => {
                ResultIdentity::ApplyFailed(error.category)
            }
            HeaderReply::SelectedChainContains(Ok(membership)) => match membership {
                SelectedChainMembership::Selected(frontier) => ResultIdentity::Selected(*frontier),
                SelectedChainMembership::NotSelected => ResultIdentity::NotSelected,
                SelectedChainMembership::NotRetained => ResultIdentity::NotRetained,
            },
            HeaderReply::ContinuationLocator(Err(_))
            | HeaderReply::VctRepairContext(Err(_))
            | HeaderReply::AcquireHeaderPath(Err(_))
            | HeaderReply::ReadHeaderPath(Err(_))
            | HeaderReply::ReleaseHeaderPath(Err(_))
            | HeaderReply::SelectedChainContains(Err(_)) => ResultIdentity::PortError,
        }
    }
}

/// Which slots a lease uses: an ordinary slot, or one a path short enough
/// for one page may take, including the reserve slot.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub enum CapacityClass {
    /// An ordinary slot.
    General,
    /// Any slot, for a path of at most one page.
    Bounded,
}

/// A capacity signal a refused acquire waits on.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub enum Waiter {
    /// The signal on a peer's live lease, for that peer's next acquire.
    Peer(LeaseKey),
    /// The ordinary class's signal.
    General,
    /// The reserve class's signal.
    Fallback,
}

/// Where a retained path's next page starts.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub enum PathPosition {
    /// At this index of the retained hashes, counted from the one above the
    /// common ancestor.
    Retained {
        /// The index.
        next: u64,
    },
    /// Every header has been served.
    Complete,
}

/// Why a retained path stopped being held.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub enum RetentionEnd {
    /// Together with its capability: a release, an idle expiry, an abandoned
    /// acquire, shutdown, or a read that served the last header.
    WithCapability,
    /// The continuation cache was full when another consumed lease moved in,
    /// and this continuation had the lowest lease ID.
    Evicted,
    /// An acquire continued from this cached cursor, as lease `into`.
    Continued {
        /// The new lease's ID.
        into: u64,
    },
    /// An acquire took this cached cursor but didn't continue from it.
    Discarded,
    /// Shutdown cleared the continuation cache.
    ShutdownCleared,
}

/// One change the lease registry made, in the order it made them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LeaseFact {
    /// A lease began, with its path retained, or continued from a cached
    /// cursor.
    Leased {
        /// The new lease.
        lease: LeaseKey,
        /// Its work scope.
        scope: HeaderWorkAuthority,
        /// Its target.
        target: Frontier,
        /// Where its next page starts.
        common_ancestor: Frontier,
        /// The cursor's index into the retained path.
        position: PathPosition,
        /// Its slot class.
        capacity: CapacityClass,
        /// The cached lease it continued from, if any.
        continued_from: Option<u64>,
    },
    /// A lease's handle stopped working and its slot freed.
    CapabilityEnded {
        /// The lease.
        lease: LeaseKey,
        /// The event that ended it.
        release: super::Release,
    },
    /// A consumed lease's path moved into the continuation cache, with its
    /// last served header.
    RetentionCached {
        /// The consumed lease.
        lease: LeaseKey,
        /// The last header served.
        last_frontier: Frontier,
        /// The cursor's index into the retained path.
        position: PathPosition,
    },
    /// A retained path stopped being held.
    RetentionEnded {
        /// The lease the path belonged to.
        lease: LeaseKey,
        /// Why.
        why: RetentionEnd,
    },
    /// A refusal registered a capacity signal that wasn't set yet.
    WaiterSet(Waiter),
    /// A capacity signal fired and was cleared.
    WaiterFired(Waiter),
}

/// The port's actual owner state, read fresh from the containers that hold
/// it: the operation slots and permit container in the port's bookkeeping,
/// and the lease registry. Collections are in key order.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OwnerState {
    /// The last fact published when this state was read. The state reflects
    /// every fact up to it and none after.
    pub through_seq: u64,
    /// Every operation not yet taken or dropped by its caller.
    pub operations: Vec<OperationState>,
    /// The operations whose permit is in storage's container.
    pub permits: Vec<OpId>,
    /// Every live lease.
    pub leases: Vec<LeaseState>,
    /// Every cached continuation.
    pub continuations: Vec<ContinuationState>,
    /// Whether each capacity class has a waiting signal.
    pub class_waiters: ClassWaiters,
    /// The last lease ID the registry issued.
    pub last_lease_id: u64,
    /// How many acquires continued from a cached path.
    pub continuation_hits: u64,
}

/// One operation in the port's bookkeeping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationState {
    /// Its ID.
    pub id: OpId,
    /// What it asked for.
    pub request: RequestIdentity,
    /// Where it is.
    pub stage: OperationStage,
    /// Whether its caller's call still exists, read from a token the call
    /// owns, not from the port's bookkeeping.
    pub caller: CallerPresence,
}

/// Whether a caller's call still exists. A call drops its token when it
/// returns its reply or is dropped, whatever the port's bookkeeping did.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub enum CallerPresence {
    /// The call is still waiting for its reply.
    Alive,
    /// The call took its reply or was dropped.
    Gone,
}

/// Where an operation is.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OperationStage {
    /// Waiting for the run to complete it.
    Pending {
        /// Whether the port still holds the caller's waker.
        caller_waiting: bool,
    },
    /// Being completed by the run. Never seen at a step boundary.
    Running,
    /// Completed, with a reply its caller hasn't taken, and that reply's
    /// identity as captured when it was made.
    Replied {
        /// The stored reply's identity.
        result: ResultIdentity,
    },
}

/// One live lease in the registry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaseState {
    /// Its key.
    pub lease: LeaseKey,
    /// Its work scope.
    pub scope: HeaderWorkAuthority,
    /// Its target.
    pub target: Frontier,
    /// The last header served, or where the path starts.
    pub last_frontier: Frontier,
    /// The cursor's index into the retained path.
    pub position: PathPosition,
    /// Its slot class.
    pub capacity: CapacityClass,
    /// Whether its peer has a refused acquire waiting on it.
    pub waiter: bool,
    /// The transaction that created it, as the registry stored it. `None`
    /// for a lease made through the engine port directly, outside any
    /// controlled transaction.
    pub leased_txn: Option<u64>,
}

/// One consumed lease's path, cached for its peer's next page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContinuationState {
    /// The consumed lease's key.
    pub lease: LeaseKey,
    /// Its work scope.
    pub scope: HeaderWorkAuthority,
    /// Its target.
    pub target: Frontier,
    /// The last header served.
    pub last_frontier: Frontier,
    /// The cursor's index into the retained path.
    pub position: PathPosition,
    /// How many hashes the path retains above its common ancestor.
    pub retained: u64,
}

/// Whether each capacity class has a waiting signal.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct ClassWaiters {
    /// The ordinary class.
    pub general: bool,
    /// The reserve class.
    pub fallback: bool,
}
