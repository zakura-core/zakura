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
    HeaderWorkAuthority, SourceId, MAX_STAGED_TARGETS_V1,
};
use zakura_rt::collections::HashMap;

use super::super::super::{
    v9::Cursor, AuxSchema, HeaderEntry, HeaderSyncRequestId, Status, ZakuraPeerId, MAX_HS_RANGE,
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
    pub const UNSTARTED: Self = Self(0);

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

impl HeaderWorkEnd {
    /// The end's metric label.
    pub(in crate::zakura::header_sync) fn label(self) -> &'static str {
        match self {
            Self::Dropped => "dropped",
            Self::Refused => "refused",
            Self::Panicked => "panicked",
            Self::Stale => "stale",
            Self::NoAuthority => "no_authority",
            Self::OwnerMismatch => "owner_mismatch",
            Self::RepairInvalid => "repair_invalid",
            Self::PrepareFailed => "prepare_failed",
            Self::HandedToApply { .. } => "handed_to_apply",
            Self::Settled => "settled",
        }
    }
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
    pub incarnation: WorkIncarnation,
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
pub(super) struct WorkPolicy {
    work_by_peer: HashMap<ZakuraPeerId, PeerWorkState>,
    scheduling_revision: u64,
    repair_episodes: HashMap<HeaderSyncWorkOwner, AuxiliaryRequirementEpisode>,
    last_incarnation: u64,
}

impl WorkPolicy {
    /// Changes whenever target ownership or a peer's awaiting/active state changes.
    pub(super) fn scheduling_revision(&self) -> u64 {
        self.scheduling_revision
    }

    fn bump_revision(&mut self) {
        self.scheduling_revision = self.scheduling_revision.wrapping_add(1);
    }

    pub(super) fn len(&self) -> usize {
        self.work_by_peer.len()
    }

    /// Whether `peer` holds unstarted or active work.
    pub(super) fn has_work(&self, peer: &ZakuraPeerId) -> bool {
        self.work_by_peer.contains_key(peer)
    }

    /// Whether any peer's unstarted or active work pursues `target_tip_hash`.
    pub(super) fn is_target_assigned(&self, target_tip_hash: zakura_chain::block::Hash) -> bool {
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
    pub(super) fn awaiting_targets(
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
    pub(super) fn awaiting_target(&self, peer: &ZakuraPeerId) -> Option<&AdvertisedHeaderTarget> {
        match self.work_by_peer.get(peer) {
            Some(PeerWorkState::AwaitingLocator { target, .. }) => Some(target.as_ref()),
            _ => None,
        }
    }

    pub(super) fn awaiting(
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
    pub(super) fn repair_episode(
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
    pub(super) fn active_owner(&self, owner: HeaderSyncWorkOwner) -> Option<&ActiveHeaderRequest> {
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

    pub(super) fn active(&self, peer: &ZakuraPeerId) -> Option<&ActiveHeaderRequest> {
        match self.work_by_peer.get(peer) {
            Some(PeerWorkState::Active { request, .. }) => Some(request),
            _ => None,
        }
    }

    pub(super) fn active_mut(&mut self, peer: &ZakuraPeerId) -> Option<&mut ActiveHeaderRequest> {
        match self.work_by_peer.get_mut(peer) {
            Some(PeerWorkState::Active { request, .. }) => Some(request),
            _ => None,
        }
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
