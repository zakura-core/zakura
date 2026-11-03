//! Value-only header work policy.
//!
//! [`WorkPolicy`] decides which peer holds which work and when that work may
//! start. [`HeaderChunkUsage`] decides how counts move through the aggregate
//! budget. Both hold plain values. They own no budget handle, lock, channel, or
//! clock, and dropping them has no effect outside their own memory.
//!
//! [`super::PeerWorkQueue`] is the runtime owner. It holds the budget handles
//! whose drops release capacity, asks this policy for each decision, and applies
//! the capacity effect that the decision names.

use std::cmp::Ordering;

use zakura_header_chain::{
    AuxiliaryRequirementEpisode, EngineSnapshot, Frontier, HeaderLocator, HeaderSyncWorkOwner,
    HeaderWorkAuthority, SourceId, TargetCompletion, VctRepairContext, MAX_STAGED_TARGETS_V1,
};
use zakura_rt::collections::HashMap;

use super::super::super::{
    events::HeaderPortOperation, v9::Cursor, AuxSchema, HeaderEntry, HeaderSyncRequestId, Headers,
    Status, ZakuraPeerId, MAX_HS_RANGE,
};

/// Exact aggregate header count owned across receiving, preparation, and application.
// The conversion is safe because `MAX_HS_RANGE` is 4,000 on every supported target.
pub(in crate::zakura::header_sync) const HEADER_CHUNK_BUDGET_CAPACITY_V1: usize =
    MAX_HS_RANGE as usize;

/// Fair per-request share when all bounded target slots are active.
pub(in crate::zakura::header_sync) const MAX_HEADER_CHUNK_RESERVATION_V1: usize =
    HEADER_CHUNK_BUDGET_CAPACITY_V1 / MAX_STAGED_TARGETS_V1;

/// One started round of a peer's header work, from 1 per queue.
///
/// A resumed subscription can reuse its request ID, so the owner alone cannot tell
/// two rounds apart. No two starts share an incarnation.
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WorkIncarnation(u64);

impl WorkIncarnation {
    /// The value of a request that no queue has started.
    pub(super) const UNSTARTED: Self = Self(0);

    /// The incarnation's number.
    pub fn get(self) -> u64 {
        self.0
    }
}

/// What one header storage call asks storage for.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub enum StorageCallKind {
    /// A continuation-locator read.
    Locator,
    /// A VCT repair context read.
    VctRepairContext,
    /// A target preparation.
    Prepare,
    /// A prepared target's application.
    Apply,
}

/// One header storage call's exact identity.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub(in crate::zakura::header_sync) struct StorageCall {
    /// Counts from 1 per reactor, across accepted and refused calls.
    pub(in crate::zakura::header_sync) number: u64,
    pub(in crate::zakura::header_sync) kind: StorageCallKind,
}

/// The exact round and storage call that one prepare or apply belongs to.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(in crate::zakura::header_sync) struct WorkCall {
    pub(in crate::zakura::header_sync) incarnation: WorkIncarnation,
    pub(in crate::zakura::header_sync) call: StorageCall,
}

/// How retained headers left one storage call.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum HeaderWorkEnd {
    /// The call's future dropped before it finished, at shutdown or cancellation.
    Dropped,
    /// The reactor refused the call that would have taken the headers.
    Refused,
    /// The call's future panicked.
    Panicked,
    /// The completion no longer matched its peer's work, phase, source, owner,
    /// round, or call.
    Stale,
    /// The prepared target lost its completion authority.
    NoAuthority,
    /// The prepared target named another owner or source.
    OwnerMismatch,
    /// The prepared repair did not satisfy its repair context.
    RepairInvalid,
    /// Preparation failed.
    PrepareFailed,
    /// The same headers moved to this apply call.
    HandedToApply {
        /// The apply call's number.
        apply: u64,
    },
    /// The apply's result retired its work.
    Settled,
}

/// One peer's exact, session-bound target claim.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdvertisedHeaderTarget {
    /// Durable generation and branch that own the scheduled locator work.
    pub scope: HeaderWorkAuthority,
    /// Ordered-stream generation that supplied this status.
    pub session_id: u64,
    /// Exact advisory snapshot supplied by the peer.
    pub status: Status,
}

impl AdvertisedHeaderTarget {
    /// Whether this target still has header-generation and branch authority.
    ///
    /// Header work deliberately ignores the global state version because unrelated body commits
    /// can advance it without changing the selected header graph or finality anchor.
    pub fn is_current(&self, local: &EngineSnapshot) -> bool {
        self.scope.header_generation == local.header_generation
            && self.scope.branch.anchor_hash == local.frontiers.finalized.hash
            && self.scope.branch.target_tip_hash == self.status.selected_tip_hash
    }

    /// Compare claimed suffix work only when both snapshots use the same anchor.
    pub fn claimed_work_order(&self, local: &EngineSnapshot) -> Option<Ordering> {
        let local_anchor = local.frontiers.finalized;
        (self.status.work_anchor_height == local_anchor.height
            && self.status.work_anchor_hash == local_anchor.hash)
            .then(|| {
                self.status
                    .suffix_cumulative_work
                    .cmp(&local.header_best_score.suffix_work.as_u256())
            })
    }

    /// Whether this status names a different target that can actually serve a request.
    pub fn is_discovery_eligible(&self, local: &EngineSnapshot) -> bool {
        self.status.selected_tip_hash != local.frontiers.header_best.hash
            // Normal discovery cannot replace the finalized prefix. Historical
            // auxiliary repair uses its own explicitly selected target path.
            && self.status.selected_tip_height > local.frontiers.finalized.height
            && self.status.max_headers_per_response != 0
            && self.status.max_message_bytes != 0
    }
}

/// One published request for an exact advertised target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActiveHeaderRequest {
    /// Admission purpose carried through the shared target lifecycle.
    pub purpose: HeaderTargetPurpose,
    /// Peer whose current session owns the request.
    pub peer: ZakuraPeerId,
    /// Stable source identity used by completion ownership.
    pub source: SourceId,
    /// Exact status snapshot that the request pursues.
    pub target: AdvertisedHeaderTarget,
    /// Exact coherent state locator sent in the request.
    pub sent_locator: HeaderLocator,
    /// Nonzero request correlation identifier.
    pub request_id: HeaderSyncRequestId,
    /// Durable generation and exact branch ownership fixed by the first request.
    pub owner: HeaderSyncWorkOwner,
    /// Exact authenticated intersection fixed by the first response.
    pub common_ancestor: Option<Frontier>,
    /// Complete response pages staged without intermediate state mutation.
    pub entries: Vec<HeaderEntry>,
    /// Exact phase of complete-target processing.
    pub phase: HeaderTargetPhase,
    /// The round the queue assigned at start, or [`WorkIncarnation::UNSTARTED`] before it.
    incarnation: WorkIncarnation,
    /// The subscription cursor of the last staged page.
    pub cursor: Option<Cursor>,
    /// Negotiated page bound before temporary credit limits.
    /// Each request reserves its count against current shared headroom.
    pub max_header_count: u32,
    /// Requested auxiliary schema preserved across continuation requests.
    pub tree_aux_schema: AuxSchema,
}

/// Policy that distinguishes ordinary branch pursuit from exact auxiliary repair.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HeaderTargetPurpose {
    /// Admit a complete parent-linked branch target.
    Normal,
    /// Redeliver auxiliary metadata for an exact selected range.
    SelectedAuxiliaryRepair {
        /// Selected target fixed by the durable repair context.
        selected_target: Frontier,
        /// Durable repair-signal generation that owns this attempt.
        repair_generation: u64,
    },
}

/// Reactor-owned phase that permits each target preparation and state submission once.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum HeaderTargetPhase {
    /// The reactor receives and stages response pages.
    Receiving,
    /// A worker validates the complete target outside the reactor.
    Preparing,
    /// Sealed evidence passed the gate.
    /// One state call remains pending.
    Applying,
}

impl ActiveHeaderRequest {
    /// A request for `target` that has received nothing. Only the queue that
    /// starts it assigns its incarnation.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::zakura::header_sync) fn new(
        purpose: HeaderTargetPurpose,
        peer: ZakuraPeerId,
        source: SourceId,
        sent_locator: HeaderLocator,
        owner: HeaderSyncWorkOwner,
        target: AdvertisedHeaderTarget,
        request_id: HeaderSyncRequestId,
        max_header_count: u32,
        tree_aux_schema: AuxSchema,
    ) -> Self {
        Self {
            purpose,
            peer,
            source,
            target,
            sent_locator,
            request_id,
            owner,
            common_ancestor: None,
            entries: Vec::new(),
            phase: HeaderTargetPhase::Receiving,
            incarnation: WorkIncarnation::UNSTARTED,
            cursor: None,
            max_header_count,
            tree_aux_schema,
        }
    }

    /// The round the queue assigned at start.
    pub fn incarnation(&self) -> WorkIncarnation {
        self.incarnation
    }

    /// Whether one page preserves the active phase, target, and exact ancestry.
    pub fn matches_response_page(
        &self,
        target_tip_hash: zakura_chain::block::Hash,
        returned_ancestor: Frontier,
    ) -> bool {
        let expected_ancestor = match self.common_ancestor {
            Some(_) => self.staged_tip(),
            None => self
                .sent_locator
                .entries()
                .iter()
                .copied()
                .find(|entry| *entry == returned_ancestor),
        };
        self.phase == HeaderTargetPhase::Receiving
            && self.target.status.selected_tip_hash == target_tip_hash
            && expected_ancestor == Some(returned_ancestor)
    }

    /// Whether this work still receives the pages of subscription `id` on `session_id`.
    pub(in crate::zakura::header_sync) fn awaits_page(&self, id: u64, session_id: u64) -> bool {
        self.request_id.get() == id
            && self.owner.session_id() == session_id
            && self.phase == HeaderTargetPhase::Receiving
    }

    /// Whether one explicit outcome exactly matches the active request.
    pub fn accepts_outcome(
        &self,
        request_id: HeaderSyncRequestId,
        target_tip_hash: zakura_chain::block::Hash,
    ) -> bool {
        self.phase == HeaderTargetPhase::Receiving
            && self.request_id == request_id
            && self.target.status.selected_tip_hash == target_tip_hash
    }

    /// Select the continuation-only locator without changing this request's target.
    pub fn continuation_locator(
        &self,
        returned_suffix_tip: zakura_header_chain::Frontier,
    ) -> HeaderLocator {
        HeaderLocator::for_continuation(returned_suffix_tip)
    }

    /// Return the last staged frontier, inferred only from authenticated local heights.
    pub fn staged_tip(&self) -> Option<Frontier> {
        let ancestor = self.common_ancestor?;
        let last = self.entries.last()?;
        let count = u32::try_from(self.entries.len()).ok()?;
        let height = ancestor
            .height
            .0
            .checked_add(count)
            .filter(|height| *height <= zakura_chain::block::Height::MAX.0)
            .map(zakura_chain::block::Height)?;
        Some(Frontier::new(height, last.header.hash()))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PeerWorkState {
    AwaitingLocator {
        target: Box<AdvertisedHeaderTarget>,
        priority: PeerWorkPriority,
    },
    Active {
        request: Box<ActiveHeaderRequest>,
        /// The prepare or apply call this round waits for.
        call: Option<StorageCall>,
    },
}

/// Advisory discovery priority.
/// The scheduler maps incomparable claims to normal priority.
#[derive(Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::zakura::header_sync) enum PeerWorkPriority {
    LowerComparableWork,
    Normal,
    HigherComparableWork,
}

impl PeerWorkPriority {
    pub(in crate::zakura::header_sync) fn from_work_order(order: Option<Ordering>) -> Self {
        match order {
            Some(Ordering::Greater) => Self::HigherComparableWork,
            Some(Ordering::Less) => Self::LowerComparableWork,
            Some(Ordering::Equal) | None => Self::Normal,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(in crate::zakura::header_sync) enum QueueWorkResult {
    NeedsLocator,
    AlreadyActive,
    TargetAlreadyAssigned,
    AtCapacity,
}

/// What one peer's started work means for a new offer.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(in crate::zakura::header_sync) enum OfferedPeerWork {
    /// The peer has no started work.
    Idle,
    /// The peer's ordinary work already pursues the offered target.
    SameOrdinaryTarget,
    /// The peer's started work pursues something else.
    Other,
}

/// The facts that decide one peer's offer of a target at one instant, before
/// its admission. [`decide_offer`] reads the admission facts only when these
/// leave the offer open.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(in crate::zakura::header_sync) struct OfferFacts {
    /// A `HistoryPruned` pause holds for this peer at the selected header tip.
    pub(in crate::zakura::header_sync) pruned_pause: bool,
    /// The target's generation and branch already finished.
    pub(in crate::zakura::header_sync) covered: bool,
    /// The status names another target and can carry at least one header.
    pub(in crate::zakura::header_sync) eligible: bool,
    /// The peer's started work.
    pub(in crate::zakura::header_sync) work: OfferedPeerWork,
}

/// Why an offer that cannot yield work is spent.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(in crate::zakura::header_sync) enum SpentOffer {
    /// The same attempt would end `HistoryPruned` again, so another peer's
    /// status takes the target.
    PrunedPause,
    /// The target already finished.
    Covered,
    /// The status cannot start a request.
    Ineligible,
}

/// What one peer's offer of a target does.
#[must_use]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(in crate::zakura::header_sync) enum OfferDecision {
    /// Spend the offer and end the peer's unstarted work.
    Spend {
        /// Why no round can follow.
        cause: SpentOffer,
        /// Whether to close an idle subscription kept for the next round, which
        /// would only hold the session's connection hostage.
        close_subscription: bool,
    },
    /// Started work continues. Exact selected-range repair does not use the
    /// advertised ordinary offer. A status for another target stays unspent, so
    /// it starts once this work ends.
    AlreadyActive {
        /// Whether the offer names the target this work already pursues.
        spend: bool,
    },
    /// End the peer's unstarted work and keep the offer until the blocker clears.
    HeldBack,
    /// Close the live subscription and keep the offer, because the next `Open`
    /// must follow its terminal outcome.
    AwaitSubscription,
    /// Stage the target.
    Stage,
}

/// Decide one peer's offer. The checks run in priority order.
///
/// `held_back` says whether a resource stall or missing headroom holds
/// ordinary work back. `subscription_ready` says whether the session holds no
/// subscription, or one that a `Grant` can continue toward the target. Each
/// is read only when every earlier check passes, since reading them takes
/// locks and a pass over peers, and most status offers end before them.
pub(in crate::zakura::header_sync) fn decide_offer(
    facts: OfferFacts,
    held_back: impl FnOnce() -> bool,
    subscription_ready: impl FnOnce() -> bool,
) -> OfferDecision {
    let spent = if facts.pruned_pause {
        Some(SpentOffer::PrunedPause)
    } else if facts.covered {
        Some(SpentOffer::Covered)
    } else if !facts.eligible {
        Some(SpentOffer::Ineligible)
    } else {
        None
    };
    if let Some(cause) = spent {
        return OfferDecision::Spend {
            cause,
            close_subscription: facts.work == OfferedPeerWork::Idle,
        };
    }
    if facts.work != OfferedPeerWork::Idle {
        return OfferDecision::AlreadyActive {
            spend: facts.work == OfferedPeerWork::SameOrdinaryTarget,
        };
    }
    if held_back() {
        return OfferDecision::HeldBack;
    }
    if !subscription_ready() {
        return OfferDecision::AwaitSubscription;
    }
    OfferDecision::Stage
}

/// The facts that decide what follows one staged page.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(in crate::zakura::header_sync) struct PageFacts {
    /// Whether the page ends at the initial target.
    pub(in crate::zakura::header_sync) reaches_target: bool,
    /// Headers that this round's credit may still deliver.
    pub(in crate::zakura::header_sync) reserved: usize,
    /// Whether the staged headers form a whole refill batch for the selected tip.
    pub(in crate::zakura::header_sync) checkpoint_prefix_ready: bool,
}

/// What follows one staged page.
#[must_use]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(in crate::zakura::header_sync) enum PageNext {
    /// The rest of this round's credit is still on its way.
    WaitCredit,
    /// Validate the staged headers.
    Prepare {
        /// Whether they form a prefix of the target, because capacity bounds the round.
        bounded_prefix: bool,
    },
    /// Grant more credit toward the target from `staged_tip`.
    Continue {
        /// The last staged header.
        staged_tip: Frontier,
    },
    /// The peer's pages cannot form the target.
    Malformed {
        /// Whether the page claimed to reach the target with staged headers.
        reached_target: bool,
    },
}

impl PageNext {
    /// Whether the page reached its target, so the pages that would follow it
    /// must stop. Pages already in flight are dropped.
    pub(in crate::zakura::header_sync) fn closes_subscription(self) -> bool {
        matches!(
            self,
            Self::Prepare {
                bounded_prefix: false
            } | Self::Malformed {
                reached_target: true
            }
        )
    }
}

/// Decide what follows the page just staged on `active`.
///
/// A peer can advertise an arbitrarily distant target. Admitting a validated
/// prefix at capacity bounds response staging, and a checkpoint-sized minimum
/// lets a selected extension unblock body validation without letting short peer
/// pages force tiny prefix commits.
///
/// `budget_full` says whether the aggregate header budget is full, and
/// `durable_prefix_full` whether the durable prefix has no room for the
/// round's next grant. Each is read only when every earlier check passes,
/// since reading them takes locks, and most pages wait for credit first.
pub(in crate::zakura::header_sync) fn page_next(
    active: &ActiveHeaderRequest,
    facts: PageFacts,
    budget_full: impl FnOnce() -> bool,
    durable_prefix_full: impl FnOnce() -> bool,
) -> PageNext {
    let Some(staged_tip) = active.staged_tip() else {
        return PageNext::Malformed {
            reached_target: false,
        };
    };
    if facts.reaches_target {
        return if staged_tip.hash == active.target.status.selected_tip_hash {
            PageNext::Prepare {
                bounded_prefix: false,
            }
        } else {
            PageNext::Malformed {
                reached_target: true,
            }
        };
    }
    if facts.reserved != 0 {
        return PageNext::WaitCredit;
    }
    if facts.checkpoint_prefix_ready || budget_full() || durable_prefix_full() {
        return PageNext::Prepare {
            bounded_prefix: true,
        };
    }
    PageNext::Continue { staged_tip }
}

/// What one selected-range repair page means for its repair.
#[must_use]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(in crate::zakura::header_sync) enum RepairPage {
    /// The page has the exact shape of the selected range and carries new input.
    Accepted,
    /// The page does not have the exact shape of the selected range.
    Malformed,
    /// The supplier returned auxiliary input that this episode already excludes.
    ExcludedInput,
}

/// Judge `response` against `active`'s repair, whose evidence episode is `episode`.
///
/// `context` is the repair's assigned context, if it is still assigned.
pub(in crate::zakura::header_sync) fn repair_page(
    active: &ActiveHeaderRequest,
    episode: AuxiliaryRequirementEpisode,
    context: Option<&VctRepairContext>,
    response: &Headers,
    reaches_target: bool,
) -> RepairPage {
    let HeaderTargetPurpose::SelectedAuxiliaryRepair {
        selected_target, ..
    } = &active.purpose
    else {
        return RepairPage::Accepted;
    };
    let ancestor = Frontier::new(
        response.common_ancestor_height,
        response.common_ancestor_hash,
    );
    let returned_range: Option<Vec<_>> = response
        .entries
        .iter()
        .enumerate()
        .map(|(index, entry)| {
            let offset = u32::try_from(index).ok()?.checked_add(1)?;
            let height = ancestor.height.0.checked_add(offset)?;
            Some(Frontier::new(
                zakura_chain::block::Height(height),
                entry.header.hash(),
            ))
        })
        .collect();
    let context = context.filter(|context| {
        context.episode == episode
            && context.request_target() == *selected_target
            && returned_range
                .as_ref()
                .is_some_and(|range| context.matches_selected_range(range))
    });
    let exact_shape = response.initial_target_tip_hash == selected_target.hash
        && active.sent_locator.entries() == [ancestor]
        && reaches_target
        && response.tree_aux_schema == AuxSchema::V1
        && response
            .entries
            .iter()
            .all(|entry| entry.tree_aux.is_some());
    let (true, Some(context)) = (exact_shape, context) else {
        return RepairPage::Malformed;
    };
    let inputs: Vec<_> = response
        .entries
        .iter()
        .map(|entry| {
            entry
                .tree_aux
                .expect("a repair response has schema-1 auxiliary input")
        })
        .collect();
    if context.admits_new_input(active.source, &inputs) {
        RepairPage::Accepted
    } else {
        RepairPage::ExcludedInput
    }
}

/// Counts claimed against the aggregate header budget.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct HeaderChunkUsage {
    /// Headers that wire credit may still deliver.
    pub(super) reserved: usize,
    /// Headers delivered and held across staging, preparation, and application.
    pub(super) owned: usize,
}

impl HeaderChunkUsage {
    /// Every claimed header.
    pub(super) fn claimed(self) -> usize {
        self.reserved.saturating_add(self.owned)
    }

    /// Capacity that `capacity` leaves unclaimed.
    pub(super) fn remaining(self, capacity: usize) -> usize {
        capacity.saturating_sub(self.claimed())
    }

    /// The usage after reserving `count` of `capacity`, or `None` when an empty
    /// or oversized reservation is refused.
    pub(super) fn reserve(self, capacity: usize, count: usize) -> Option<Self> {
        if count == 0 || count > self.remaining(capacity) {
            return None;
        }
        Some(Self {
            reserved: self.reserved.checked_add(count)?,
            owned: self.owned,
        })
    }

    /// The usage after moving `reserved` headers out of reservation and `owned`
    /// of them into owned capacity, or `None` when the counts don't fit.
    pub(super) fn consume(self, reserved: usize, owned: usize) -> Option<Self> {
        if owned > reserved {
            return None;
        }
        Some(Self {
            reserved: self.reserved.checked_sub(reserved)?,
            owned: self.owned.checked_add(owned)?,
        })
    }

    /// The usage after an unspent reservation of `count` ends.
    pub(super) fn release_reserved(self, count: usize) -> Self {
        debug_assert!(self.reserved >= count);
        Self {
            reserved: self.reserved.saturating_sub(count),
            owned: self.owned,
        }
    }

    /// The usage after an owned lease of `count` ends.
    pub(super) fn release_owned(self, count: usize) -> Self {
        debug_assert!(self.owned >= count);
        Self {
            reserved: self.reserved,
            owned: self.owned.saturating_sub(count),
        }
    }
}

/// What a reservation of `reserved` keeps after a page of `returned` headers,
/// or `None` when the page doesn't fit it.
pub(super) fn reservation_after_page(reserved: usize, returned: usize) -> Option<usize> {
    reserved.checked_sub(returned).filter(|_| reserved != 0)
}

/// One ordinary request's count: `desired`, bounded by its fair share and by
/// `remaining` unclaimed capacity.
pub(super) fn fair_request_count(desired: u32, remaining: usize) -> u32 {
    repair_request_count(desired, remaining.min(MAX_HEADER_CHUNK_RESERVATION_V1))
}

/// One selected auxiliary repair's count: `desired`, bounded by `remaining`
/// unclaimed capacity.
pub(super) fn repair_request_count(desired: u32, remaining: usize) -> u32 {
    let desired = usize::try_from(desired).unwrap_or(usize::MAX);
    u32::try_from(desired.min(remaining)).expect("the header budget capacity fits u32")
}

/// Headers owned by active work in each phase.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct PhaseCounts {
    pub(super) receiving: usize,
    pub(super) preparing: usize,
    pub(super) applying: usize,
}

/// What staging a target decided.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct StageDecision {
    /// How the request to stage ended.
    pub(super) result: QueueWorkResult,
    /// The peer whose unstarted work this decision ended or replaced. The
    /// runtime owner releases that peer's capacity.
    pub(super) release: Option<ZakuraPeerId>,
}

impl StageDecision {
    fn new(result: QueueWorkResult, release: Option<ZakuraPeerId>) -> Self {
        Self { result, release }
    }
}

/// Which peer holds which header work.
///
/// Every field is a plain value. See the module doc.
#[derive(Clone, Debug, Default)]
pub(in crate::zakura::header_sync) struct WorkPolicy {
    work_by_peer: HashMap<ZakuraPeerId, PeerWorkState>,
    scheduling_revision: u64,
    repair_episodes: HashMap<HeaderSyncWorkOwner, AuxiliaryRequirementEpisode>,
    last_incarnation: u64,
}

impl WorkPolicy {
    /// Changes whenever target ownership or a peer's awaiting/active state changes.
    /// These changes invalidate cached offers. The reactor separately tracks effective
    /// virtual claims using negotiated limits.
    pub(in crate::zakura::header_sync) fn scheduling_revision(&self) -> u64 {
        self.scheduling_revision
    }

    fn bump_revision(&mut self) {
        self.scheduling_revision = self.scheduling_revision.wrapping_add(1);
    }

    pub(in crate::zakura::header_sync) fn len(&self) -> usize {
        self.work_by_peer.len()
    }

    /// Whether `peer` holds unstarted or active work.
    pub(in crate::zakura::header_sync) fn has_work(&self, peer: &ZakuraPeerId) -> bool {
        self.work_by_peer.contains_key(peer)
    }

    /// Whether any peer's unstarted or active work pursues `target_tip_hash`.
    pub(in crate::zakura::header_sync) fn is_target_assigned(
        &self,
        target_tip_hash: zakura_chain::block::Hash,
    ) -> bool {
        self.target_owners(target_tip_hash).next().is_some()
    }

    fn target_owners(
        &self,
        target_tip_hash: zakura_chain::block::Hash,
    ) -> impl Iterator<Item = &ZakuraPeerId> {
        self.work_by_peer
            .iter()
            .filter(move |(_, work)| {
                let owned_target = match work {
                    PeerWorkState::AwaitingLocator { target, .. } => {
                        target.status.selected_tip_hash
                    }
                    PeerWorkState::Active { request, .. } => {
                        request.target.status.selected_tip_hash
                    }
                };
                owned_target == target_tip_hash
            })
            .map(|(peer, _)| peer)
    }

    /// Return every peer's target that still waits for its locator.
    pub(in crate::zakura::header_sync) fn awaiting_targets(
        &self,
    ) -> impl Iterator<Item = (&ZakuraPeerId, &AdvertisedHeaderTarget)> {
        self.work_by_peer
            .iter()
            .filter_map(|(peer, work)| match work {
                PeerWorkState::AwaitingLocator { target, .. } => Some((peer, target.as_ref())),
                PeerWorkState::Active { .. } => None,
            })
    }

    /// The peers whose unstarted work `current` made obsolete.
    pub(super) fn obsolete_unstarted(&self, current: &EngineSnapshot) -> Vec<ZakuraPeerId> {
        self.work_by_peer
            .iter()
            .filter_map(|(peer, work)| match work {
                PeerWorkState::AwaitingLocator { target, .. } if !target.is_current(current) => {
                    Some(peer.clone())
                }
                _ => None,
            })
            .collect()
    }

    /// The peers whose exact body-authorized repair `current` made obsolete.
    ///
    /// Ordinary header targets remain provisionally owned until the serialized state planner
    /// proves a monotone rebase or rejects their retained ancestry.
    pub(super) fn obsolete_active(&self, current: &EngineSnapshot) -> Vec<ZakuraPeerId> {
        self.work_by_peer
            .iter()
            .filter_map(|(peer, work)| match work {
                PeerWorkState::Active { request, .. } => {
                    let header = request.owner.header_authority();
                    let obsolete = request.owner.body_authority().map_or_else(
                        || {
                            header.header_generation != current.header_generation
                                && header.branch.anchor_hash == current.frontiers.finalized.hash
                        },
                        |authority| {
                            authority.header.header_generation != current.header_generation
                                || authority.verified_generation != current.verified_generation
                                || authority.header.branch.anchor_hash
                                    != current.frontiers.finalized.hash
                        },
                    );
                    obsolete.then(|| peer.clone())
                }
                _ => None,
            })
            .collect()
    }

    /// Decide `peer`'s offer of `target`.
    ///
    /// Unstarted work takes the new target. A full queue replaces the unstarted
    /// work with the lowest priority below `priority`, and refuses otherwise.
    pub(super) fn stage(
        &mut self,
        peer: ZakuraPeerId,
        target: AdvertisedHeaderTarget,
        priority: PeerWorkPriority,
    ) -> StageDecision {
        if let Some(work) = self.work_by_peer.get_mut(&peer) {
            return match work {
                PeerWorkState::AwaitingLocator {
                    target: current,
                    priority: current_priority,
                } => {
                    // Replacing metadata or priority does not release a target.
                    // The reactor tracks changes to the negotiated locator claim.
                    let moved = current.status.selected_tip_hash != target.status.selected_tip_hash;
                    **current = target;
                    *current_priority = priority;
                    if moved {
                        self.bump_revision();
                    }
                    StageDecision::new(QueueWorkResult::NeedsLocator, Some(peer))
                }
                PeerWorkState::Active { .. } => {
                    StageDecision::new(QueueWorkResult::AlreadyActive, None)
                }
            };
        }
        let mut release = None;
        if self.work_by_peer.len() >= MAX_STAGED_TARGETS_V1 {
            let replace = self
                .work_by_peer
                .iter()
                .filter_map(|(peer, work)| match work {
                    PeerWorkState::AwaitingLocator {
                        priority: current, ..
                    } if *current < priority => Some((peer.clone(), *current)),
                    _ => None,
                })
                .min_by(|(left_peer, left_priority), (right_peer, right_priority)| {
                    left_priority
                        .cmp(right_priority)
                        .then_with(|| left_peer.as_bytes().cmp(right_peer.as_bytes()))
                })
                .map(|(peer, _)| peer);
            let Some(replace) = replace else {
                return StageDecision::new(QueueWorkResult::AtCapacity, None);
            };
            self.remove(&replace);
            release = Some(replace);
        }
        self.bump_revision();
        self.work_by_peer.insert(
            peer,
            PeerWorkState::AwaitingLocator {
                target: Box::new(target),
                priority,
            },
        );
        StageDecision::new(QueueWorkResult::NeedsLocator, release)
    }

    /// Decide `peer`'s offer of an ordinary branch target, which only one peer may own.
    ///
    /// All peers derive locators from shared local state.
    /// Two peers pursuing one target would download and authenticate overlapping prefixes.
    /// The reactor keeps the alternate peer's status unspent and stages it once the owner
    /// retires.
    pub(super) fn stage_distinct_target(
        &mut self,
        peer: ZakuraPeerId,
        target: AdvertisedHeaderTarget,
        priority: PeerWorkPriority,
    ) -> StageDecision {
        let owned_by_other_peer = self
            .target_owners(target.status.selected_tip_hash)
            .any(|owner| owner != &peer);
        if owned_by_other_peer {
            let release = self.remove_unstarted(&peer).then_some(peer);
            return StageDecision::new(QueueWorkResult::TargetAlreadyAssigned, release);
        }

        self.stage(peer, target, priority)
    }

    /// Return the peer's current unstarted target, if any.
    pub(in crate::zakura::header_sync) fn awaiting_target(
        &self,
        peer: &ZakuraPeerId,
    ) -> Option<&AdvertisedHeaderTarget> {
        match self.work_by_peer.get(peer) {
            Some(PeerWorkState::AwaitingLocator { target, .. }) => Some(target.as_ref()),
            _ => None,
        }
    }

    pub(in crate::zakura::header_sync) fn awaiting(
        &self,
        peer: &ZakuraPeerId,
        session_id: u64,
        target_tip_hash: zakura_chain::block::Hash,
        scope: HeaderWorkAuthority,
    ) -> Option<&AdvertisedHeaderTarget> {
        self.awaiting_target(peer).filter(|target| {
            target.session_id == session_id
                && target.status.selected_tip_hash == target_tip_hash
                && target.scope == scope
        })
    }

    /// Start `request` if its peer awaits exactly its target and `reserved`
    /// holds, meaning the runtime owner holds its peer's reservation.
    pub(super) fn start(&mut self, mut request: ActiveHeaderRequest, reserved: bool) -> bool {
        let peer = request.peer.clone();
        let matches = self.awaiting(
            &peer,
            request.target.session_id,
            request.target.status.selected_tip_hash,
            request.target.scope,
        ) == Some(&request.target);
        if matches && reserved {
            self.bump_revision();
            self.last_incarnation = self.last_incarnation.wrapping_add(1);
            request.incarnation = WorkIncarnation(self.last_incarnation);
            self.work_by_peer.insert(
                peer,
                PeerWorkState::Active {
                    request: Box::new(request),
                    call: None,
                },
            );
            true
        } else {
            false
        }
    }

    /// Bind `owner`'s durable evidence episode to its active repair.
    ///
    /// # Panics
    ///
    /// If `owner` already binds one, since an active repair owner binds exactly one.
    pub(super) fn bind_repair_episode(
        &mut self,
        owner: HeaderSyncWorkOwner,
        episode: AuxiliaryRequirementEpisode,
    ) {
        assert!(
            self.repair_episodes.insert(owner, episode).is_none(),
            "an active repair owner binds exactly one evidence episode"
        );
    }

    /// Return the durable evidence episode bound to one active repair request.
    pub(in crate::zakura::header_sync) fn repair_episode(
        &self,
        owner: HeaderSyncWorkOwner,
    ) -> Option<AuxiliaryRequirementEpisode> {
        self.repair_episodes.get(&owner).copied()
    }

    /// Replace `owner`'s episode for a test that seeds an active repair directly.
    #[cfg(test)]
    pub(super) fn set_repair_episode_for_test(
        &mut self,
        owner: HeaderSyncWorkOwner,
        episode: AuxiliaryRequirementEpisode,
    ) {
        self.repair_episodes.insert(owner, episode);
    }

    /// End `peer`'s work, and return its request if it had started.
    pub(super) fn remove(&mut self, peer: &ZakuraPeerId) -> Option<ActiveHeaderRequest> {
        let removed = self.work_by_peer.remove(peer);
        if removed.is_some() {
            self.bump_revision();
        }
        match removed {
            Some(PeerWorkState::Active { request, .. }) => {
                self.repair_episodes.remove(&request.owner);
                Some(*request)
            }
            Some(PeerWorkState::AwaitingLocator { .. }) | None => None,
        }
    }

    /// The peer whose active request carries exactly `owner`.
    pub(super) fn owner_peer(&self, owner: HeaderSyncWorkOwner) -> Option<ZakuraPeerId> {
        self.work_by_peer
            .iter()
            .find_map(|(peer, work)| match work {
                PeerWorkState::Active { request, .. } if request.owner == owner => {
                    Some(peer.clone())
                }
                _ => None,
            })
    }

    /// Return the active target carrying one exact durable owner.
    pub(in crate::zakura::header_sync) fn active_owner(
        &self,
        owner: HeaderSyncWorkOwner,
    ) -> Option<&ActiveHeaderRequest> {
        self.work_by_peer.values().find_map(|work| match work {
            PeerWorkState::Active { request, .. } if request.owner == owner => {
                Some(request.as_ref())
            }
            _ => None,
        })
    }

    /// End `peer`'s work if it has not started, and return whether it ended.
    pub(super) fn remove_unstarted(&mut self, peer: &ZakuraPeerId) -> bool {
        let unstarted = matches!(
            self.work_by_peer.get(peer),
            Some(PeerWorkState::AwaitingLocator { .. })
        );
        if unstarted {
            self.remove(peer);
        }
        unstarted
    }

    pub(in crate::zakura::header_sync) fn active(
        &self,
        peer: &ZakuraPeerId,
    ) -> Option<&ActiveHeaderRequest> {
        match self.work_by_peer.get(peer) {
            Some(PeerWorkState::Active { request, .. }) => Some(request),
            _ => None,
        }
    }

    /// Whether `peer`'s started work still receives pages.
    pub(super) fn is_receiving(&self, peer: &ZakuraPeerId) -> bool {
        self.active(peer)
            .is_some_and(|request| request.phase == HeaderTargetPhase::Receiving)
    }

    /// Mutable access for tests and models that seed a request's fields directly.
    #[cfg(any(test, feature = "header-fuzz"))]
    pub(super) fn active_mut(&mut self, peer: &ZakuraPeerId) -> Option<&mut ActiveHeaderRequest> {
        self.request_mut(peer)
    }

    fn request_mut(&mut self, peer: &ZakuraPeerId) -> Option<&mut ActiveHeaderRequest> {
        match self.work_by_peer.get_mut(peer) {
            Some(PeerWorkState::Active { request, .. }) => Some(request),
            _ => None,
        }
    }

    /// Stage one admitted page on `peer`'s receiving work.
    ///
    /// The first page fixes the exact `ancestor`. The page's `cursor` replaces
    /// the last one.
    pub(super) fn stage_page(
        &mut self,
        peer: &ZakuraPeerId,
        ancestor: Frontier,
        entries: Vec<HeaderEntry>,
        cursor: Cursor,
    ) -> bool {
        let Some(request) = self
            .request_mut(peer)
            .filter(|request| request.phase == HeaderTargetPhase::Receiving)
        else {
            return false;
        };
        request.common_ancestor.get_or_insert(ancestor);
        request.entries.extend(entries);
        request.cursor = Some(cursor);
        true
    }

    /// Move `peer`'s staged headers into preparation, and return the storage
    /// operation that validates them.
    ///
    /// A `bounded_prefix` round narrows an ordinary owner to its staged tip,
    /// since state admits only that prefix. Return `None` unless `peer`'s work
    /// is receiving and holds staged headers.
    #[allow(clippy::unwrap_in_result)] // Its expects are local work invariants, not peer input.
    pub(super) fn begin_prepare(
        &mut self,
        peer: &ZakuraPeerId,
        bounded_prefix: bool,
    ) -> Option<HeaderPortOperation> {
        let active = self.active(peer)?;
        let episode = self.repair_episode(active.owner);
        let request = self
            .request_mut(peer)
            .filter(|request| request.phase == HeaderTargetPhase::Receiving)?;
        let target = request.staged_tip()?;
        let common_ancestor = request.common_ancestor?;
        let completion = match request.purpose {
            HeaderTargetPurpose::Normal if bounded_prefix => {
                let owner = request
                    .owner
                    .header_owner()
                    .expect("a normal target has header authority");
                request.owner = HeaderWorkAuthority {
                    branch: zakura_header_chain::BranchId {
                        target_tip_hash: target.hash,
                        ..owner.authority.branch
                    },
                    ..owner.authority
                }
                .bind(owner.session_id, owner.request_id)
                .into();
                TargetCompletion::TargetPrefix { common_ancestor }
            }
            HeaderTargetPurpose::Normal => TargetCompletion::TargetComplete { common_ancestor },
            HeaderTargetPurpose::SelectedAuxiliaryRepair {
                selected_target, ..
            } => TargetCompletion::SelectedAuxiliaryRepair {
                common_ancestor,
                selected_target,
                episode: episode.expect("an active repair binds its evidence episode"),
            },
        };
        request.phase = HeaderTargetPhase::Preparing;
        Some(HeaderPortOperation::PrepareHeaderTarget {
            purpose: request.purpose.clone(),
            peer: peer.clone(),
            source: request.source,
            owner: request.owner,
            common_ancestor,
            target,
            completion,
            entries: std::mem::take(&mut request.entries),
        })
    }

    /// Move `peer`'s prepared work to its one state call, and return whether
    /// the work was preparing.
    pub(super) fn begin_apply(&mut self, peer: &ZakuraPeerId) -> bool {
        let Some(request) = self
            .request_mut(peer)
            .filter(|request| request.phase == HeaderTargetPhase::Preparing)
        else {
            return false;
        };
        request.phase = HeaderTargetPhase::Applying;
        true
    }

    /// Record that `work`'s round on `peer` waits for its call. Another round's
    /// call records nothing.
    pub(super) fn bind_call(&mut self, peer: &ZakuraPeerId, work: WorkCall) {
        if let Some(PeerWorkState::Active { request, call }) = self.work_by_peer.get_mut(peer) {
            if request.incarnation == work.incarnation {
                *call = Some(work.call);
            }
        }
    }

    /// Settle `work` on `peer`, and return whether it is exactly the call that
    /// `peer`'s current round waits for.
    pub(super) fn settle_call(&mut self, peer: &ZakuraPeerId, work: WorkCall) -> bool {
        match self.work_by_peer.get_mut(peer) {
            Some(PeerWorkState::Active { request, call })
                if request.incarnation == work.incarnation && *call == Some(work.call) =>
            {
                *call = None;
                true
            }
            _ => false,
        }
    }

    /// Headers owned in each phase, where `owned` reads one peer's count.
    pub(super) fn phase_counts(&self, owned: impl Fn(&ZakuraPeerId) -> usize) -> PhaseCounts {
        let mut counts = PhaseCounts::default();
        for (peer, work) in &self.work_by_peer {
            let PeerWorkState::Active { request, .. } = work else {
                continue;
            };
            let count = owned(peer);
            let phase = match request.phase {
                HeaderTargetPhase::Receiving => &mut counts.receiving,
                HeaderTargetPhase::Preparing => &mut counts.preparing,
                HeaderTargetPhase::Applying => &mut counts.applying,
            };
            *phase = phase.saturating_add(count);
        }
        counts
    }
}
