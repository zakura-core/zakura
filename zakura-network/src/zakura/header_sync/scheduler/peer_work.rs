use std::{cmp::Ordering, collections::HashMap};

use tokio::time::Instant;

use zakura_header_chain::{
    EngineSnapshot, Frontier, HeaderLocator, SourceId, WorkOwner, MAX_STAGED_TARGETS_V1,
};

use super::super::{AuxSchema, HeaderEntry, HeaderSyncRequestId, Status, ZakuraPeerId};

/// Exact aggregate cap for response headers awaiting one complete-target admission.
pub(in crate::zakura::header_sync) const MAX_STAGED_HEADERS_V1: usize = 4_096;

/// One peer's exact, session-bound target claim.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdvertisedHeaderTarget {
    /// Ordered-stream generation that supplied this status.
    pub session_id: u64,
    /// Local receipt time, used only for freshness and scheduling.
    pub observed_at: Instant,
    /// Exact advisory snapshot supplied by the peer.
    pub status: Status,
}

impl AdvertisedHeaderTarget {
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
            && self.status.max_headers_per_response != 0
            && self.status.max_inflight_requests != 0
            && self.status.max_message_bytes != 0
    }
}

/// One published request for an exact advertised target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActiveHeaderRequest {
    /// Peer whose current session owns the request.
    pub peer: ZakuraPeerId,
    /// Stable source identity used by completion ownership.
    pub source: SourceId,
    /// Exact status snapshot being pursued.
    pub target: AdvertisedHeaderTarget,
    /// Exact coherent state locator sent in the request.
    pub sent_locator: HeaderLocator,
    /// Nonzero request correlation identifier.
    pub request_id: HeaderSyncRequestId,
    /// Durable generation and exact branch ownership fixed by the first request.
    pub owner: WorkOwner,
    /// Exact authenticated intersection fixed by the first response.
    pub common_ancestor: Option<Frontier>,
    /// Complete response pages staged without intermediate state mutation.
    pub entries: Vec<HeaderEntry>,
    /// Exact phase of complete-target processing.
    pub phase: HeaderTargetPhase,
    /// Effective count bound preserved across continuation requests.
    pub max_header_count: u32,
    /// Requested auxiliary schema preserved across continuation requests.
    pub tree_aux_schema: AuxSchema,
}

/// Reactor-owned phase that permits each target preparation and state submission once.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum HeaderTargetPhase {
    /// Response pages are still being received and staged.
    Receiving,
    /// The complete target is being validated outside the reactor.
    Preparing,
    /// Sealed evidence has passed the gate and one state call is pending.
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
            .map(zakura_chain::block::Height)?;
        Some(Frontier::new(height, last.header.hash()))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PeerWorkState {
    AwaitingLocator {
        target: AdvertisedHeaderTarget,
        priority: PeerWorkPriority,
    },
    Active(Box<ActiveHeaderRequest>),
}

/// Advisory discovery priority. Incomparable claims deliberately map to normal.
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
    AtCapacity,
}

/// Bounded queue of exact, session-bound peer work.
#[derive(Clone, Debug, Default)]
pub(in crate::zakura::header_sync) struct PeerWorkQueue {
    work_by_peer: HashMap<ZakuraPeerId, PeerWorkState>,
}

impl PeerWorkQueue {
    #[cfg(any(test, feature = "header-fuzz"))]
    pub(in crate::zakura::header_sync) fn len(&self) -> usize {
        self.work_by_peer.len()
    }

    pub(in crate::zakura::header_sync) fn stage(
        &mut self,
        peer: ZakuraPeerId,
        target: AdvertisedHeaderTarget,
        priority: PeerWorkPriority,
    ) -> QueueWorkResult {
        if let Some(work) = self.work_by_peer.get_mut(&peer) {
            return match work {
                PeerWorkState::AwaitingLocator {
                    target: current,
                    priority: current_priority,
                } => {
                    *current = target;
                    *current_priority = priority;
                    QueueWorkResult::NeedsLocator
                }
                PeerWorkState::Active(_) => QueueWorkResult::AlreadyActive,
            };
        }
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
                return QueueWorkResult::AtCapacity;
            };
            self.work_by_peer.remove(&replace);
        }
        self.work_by_peer
            .insert(peer, PeerWorkState::AwaitingLocator { target, priority });
        QueueWorkResult::NeedsLocator
    }

    pub(in crate::zakura::header_sync) fn awaiting(
        &self,
        peer: &ZakuraPeerId,
        session_id: u64,
        target_tip_hash: zakura_chain::block::Hash,
    ) -> Option<&AdvertisedHeaderTarget> {
        match self.work_by_peer.get(peer) {
            Some(PeerWorkState::AwaitingLocator { target, .. })
                if target.session_id == session_id
                    && target.status.selected_tip_hash == target_tip_hash =>
            {
                Some(target)
            }
            _ => None,
        }
    }

    pub(in crate::zakura::header_sync) fn start(&mut self, request: ActiveHeaderRequest) -> bool {
        let peer = request.peer.clone();
        let matches = self.awaiting(
            &peer,
            request.target.session_id,
            request.target.status.selected_tip_hash,
        ) == Some(&request.target);
        if matches {
            self.work_by_peer
                .insert(peer, PeerWorkState::Active(Box::new(request)));
        }
        matches
    }

    pub(in crate::zakura::header_sync) fn remove(
        &mut self,
        peer: &ZakuraPeerId,
    ) -> Option<ActiveHeaderRequest> {
        match self.work_by_peer.remove(peer) {
            Some(PeerWorkState::Active(request)) => Some(*request),
            Some(PeerWorkState::AwaitingLocator { .. }) | None => None,
        }
    }

    pub(in crate::zakura::header_sync) fn remove_owner(
        &mut self,
        owner: WorkOwner,
    ) -> Option<ActiveHeaderRequest> {
        let peer = self
            .work_by_peer
            .iter()
            .find_map(|(peer, work)| match work {
                PeerWorkState::Active(request) if request.owner == owner => Some(peer.clone()),
                _ => None,
            })?;
        self.remove(&peer)
    }

    pub(in crate::zakura::header_sync) fn remove_unstarted(&mut self, peer: &ZakuraPeerId) {
        if matches!(
            self.work_by_peer.get(peer),
            Some(PeerWorkState::AwaitingLocator { .. })
        ) {
            self.work_by_peer.remove(peer);
        }
    }

    pub(in crate::zakura::header_sync) fn active(
        &self,
        peer: &ZakuraPeerId,
    ) -> Option<&ActiveHeaderRequest> {
        match self.work_by_peer.get(peer) {
            Some(PeerWorkState::Active(request)) => Some(request),
            _ => None,
        }
    }

    pub(in crate::zakura::header_sync) fn active_mut(
        &mut self,
        peer: &ZakuraPeerId,
    ) -> Option<&mut ActiveHeaderRequest> {
        match self.work_by_peer.get_mut(peer) {
            Some(PeerWorkState::Active(request)) => Some(request),
            _ => None,
        }
    }

    pub(in crate::zakura::header_sync) fn has_staging_capacity(
        &self,
        additional_headers: usize,
    ) -> bool {
        self.work_by_peer
            .values()
            .filter_map(|work| match work {
                PeerWorkState::Active(request) => Some(request.entries.len()),
                PeerWorkState::AwaitingLocator { .. } => None,
            })
            .fold(0usize, usize::saturating_add)
            .saturating_add(additional_headers)
            <= MAX_STAGED_HEADERS_V1
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use zakura_chain::{block, block::genesis::regtest_genesis_block, work::difficulty::U256};
    use zakura_header_chain::{
        AlarmSet, ChainScore, EngineMode, Frontier, FrontierSet, HeaderGeneration, StateVersion,
        SuffixWork, VerifiedGeneration,
    };

    fn hash(byte: u8) -> block::Hash {
        block::Hash([byte; 32])
    }

    fn snapshot() -> EngineSnapshot {
        let finalized = Frontier::new(block::Height(10), hash(10));
        let tip = Frontier::new(block::Height(100), hash(100));
        EngineSnapshot {
            mode: EngineMode::Integrated,
            state_version: StateVersion::new(1),
            header_generation: HeaderGeneration::new(1),
            verified_generation: VerifiedGeneration::new(1),
            frontiers: FrontierSet {
                finalized,
                header_best: tip,
                verified_best: finalized,
            },
            header_best_score: ChainScore::new(SuffixWork::new(U256::from(100_u32)), tip.hash),
            oldest_retained_height: finalized.height,
            alarms: AlarmSet::default(),
        }
    }

    fn advertisement(marker: u8) -> AdvertisedHeaderTarget {
        AdvertisedHeaderTarget {
            session_id: 7,
            observed_at: Instant::now(),
            status: Status {
                work_anchor_height: block::Height(10),
                work_anchor_hash: hash(10),
                selected_tip_height: block::Height(u32::from(marker)),
                selected_tip_hash: hash(marker),
                suffix_cumulative_work: U256::from(u32::from(marker)),
                oldest_retained_height: block::Height(10),
                max_headers_per_response: 1_000,
                max_inflight_requests: 1,
                max_message_bytes: 2_000_000,
                tree_aux_schema_mask: 1,
            },
        }
    }

    fn peer(marker: u8) -> ZakuraPeerId {
        ZakuraPeerId::new(vec![marker; 32]).expect("the test peer ID has the required length")
    }

    fn active_request(
        marker: u8,
        target: AdvertisedHeaderTarget,
        local: &EngineSnapshot,
        entries: Vec<HeaderEntry>,
    ) -> ActiveHeaderRequest {
        let request_id =
            HeaderSyncRequestId::new(u64::from(marker)).expect("the marker is nonzero");
        ActiveHeaderRequest {
            peer: peer(marker),
            source: SourceId::from_digest([marker; 32]),
            sent_locator: HeaderLocator::for_continuation(local.frontiers.finalized),
            owner: WorkOwner {
                state_version: local.state_version,
                header_generation: local.header_generation,
                verified_generation: None,
                branch: zakura_header_chain::BranchId::new(
                    local.frontiers.finalized.hash,
                    target.status.selected_tip_hash,
                ),
                session_id: target.session_id,
                request_id: std::num::NonZeroU64::new(request_id.get())
                    .expect("header-sync request IDs are nonzero"),
            },
            target,
            request_id,
            common_ancestor: Some(local.frontiers.finalized),
            entries,
            phase: HeaderTargetPhase::Receiving,
            max_header_count: 1_000,
            tree_aux_schema: AuxSchema::None,
        }
    }

    #[test]
    fn unknown_status_targets_remain_eligible_regardless_of_advisory_shape() {
        let local = snapshot();

        let mut same_height_fork = advertisement(1);
        same_height_fork.status.selected_tip_height = local.frontiers.header_best.height;
        same_height_fork.status.suffix_cumulative_work = U256::from(1_u32);
        assert!(same_height_fork.is_discovery_eligible(&local));
        assert_eq!(
            same_height_fork.claimed_work_order(&local),
            Some(Ordering::Less)
        );

        let mut shorter_higher_work = advertisement(2);
        shorter_higher_work.status.selected_tip_height = block::Height(90);
        shorter_higher_work.status.suffix_cumulative_work = U256::from(101_u32);
        assert!(shorter_higher_work.is_discovery_eligible(&local));
        assert_eq!(
            shorter_higher_work.claimed_work_order(&local),
            Some(Ordering::Greater)
        );

        let mut incomparable = advertisement(3);
        incomparable.status.work_anchor_hash = hash(11);
        incomparable.status.suffix_cumulative_work = U256::MAX;
        assert!(incomparable.is_discovery_eligible(&local));
        assert_eq!(incomparable.claimed_work_order(&local), None);

        let mut known = advertisement(4);
        known.status.selected_tip_hash = local.frontiers.header_best.hash;
        assert!(!known.is_discovery_eligible(&local));

        let mut pure_requester = advertisement(5);
        pure_requester.status.max_headers_per_response = 0;
        assert!(!pure_requester.is_discovery_eligible(&local));
    }

    #[test]
    fn peer_work_queue_caps_targets_and_only_supersedes_unstarted_work() {
        let mut queue = PeerWorkQueue::default();
        for marker in 1..=16 {
            assert_eq!(
                queue.stage(
                    peer(marker),
                    advertisement(marker),
                    PeerWorkPriority::Normal
                ),
                QueueWorkResult::NeedsLocator
            );
        }
        assert_eq!(
            queue.stage(peer(17), advertisement(17), PeerWorkPriority::Normal),
            QueueWorkResult::AtCapacity
        );

        let replacement = advertisement(42);
        assert_eq!(
            queue.stage(peer(1), replacement.clone(), PeerWorkPriority::Normal),
            QueueWorkResult::NeedsLocator
        );
        assert_eq!(queue.awaiting(&peer(1), 7, hash(42)), Some(&replacement));

        let local = snapshot();
        let locator = HeaderLocator::for_selected_path(&local, |height| {
            let marker = u8::try_from(height.0).expect("the test heights fit in one byte");
            Ok(Some(hash(marker)))
        })
        .expect("the test projection contains every requested frontier");
        let request = ActiveHeaderRequest {
            peer: peer(1),
            source: SourceId::from_digest([1; 32]),
            target: replacement.clone(),
            sent_locator: locator.clone(),
            request_id: HeaderSyncRequestId::new(1).expect("one is a nonzero request ID"),
            owner: WorkOwner {
                state_version: local.state_version,
                header_generation: local.header_generation,
                verified_generation: None,
                branch: zakura_header_chain::BranchId::new(
                    local.frontiers.finalized.hash,
                    replacement.status.selected_tip_hash,
                ),
                session_id: 7,
                request_id: std::num::NonZeroU64::new(1).expect("one is nonzero"),
            },
            common_ancestor: None,
            entries: Vec::new(),
            phase: HeaderTargetPhase::Receiving,
            max_header_count: 1_000,
            tree_aux_schema: AuxSchema::None,
        };
        assert!(queue.start(request.clone()));
        assert_eq!(queue.active(&peer(1)), Some(&request));
        assert_eq!(
            queue.stage(
                peer(1),
                advertisement(43),
                PeerWorkPriority::HigherComparableWork
            ),
            QueueWorkResult::AlreadyActive
        );
        assert_eq!(queue.active(&peer(1)), Some(&request));
        assert_eq!(queue.active(&peer(1)).unwrap().sent_locator, locator);
        let continuation_tip = Frontier::new(block::Height(101), hash(101));
        assert_eq!(
            request.continuation_locator(continuation_tip).entries(),
            &[continuation_tip]
        );
        assert_eq!(request.target.status.selected_tip_hash, hash(42));
        assert_eq!(
            PeerWorkPriority::from_work_order(None),
            PeerWorkPriority::Normal
        );
        assert_eq!(
            queue.stage(
                peer(17),
                advertisement(17),
                PeerWorkPriority::HigherComparableWork,
            ),
            QueueWorkResult::NeedsLocator
        );
        assert!(queue.awaiting(&peer(17), 7, hash(17)).is_some());
    }

    #[test]
    fn aggregate_staged_header_cap_spans_all_peers_and_releases_on_retirement() {
        let local = snapshot();
        let entry = HeaderEntry {
            header: Arc::new(*regtest_genesis_block().header),
            body_size: 0,
            tree_aux: None,
        };
        let mut queue = PeerWorkQueue::default();

        let first = advertisement(1);
        assert_eq!(
            queue.stage(peer(1), first.clone(), PeerWorkPriority::Normal),
            QueueWorkResult::NeedsLocator
        );
        assert!(queue.start(active_request(1, first, &local, vec![entry.clone(); 3_000],)));
        assert!(queue.has_staging_capacity(1_096));
        assert!(!queue.has_staging_capacity(1_097));

        let second = advertisement(2);
        assert_eq!(
            queue.stage(peer(2), second.clone(), PeerWorkPriority::Normal),
            QueueWorkResult::NeedsLocator
        );
        assert!(queue.start(active_request(2, second, &local, vec![entry; 1_096],)));
        assert!(queue.has_staging_capacity(0));
        assert!(!queue.has_staging_capacity(1));

        queue.remove(&peer(1));
        assert!(queue.has_staging_capacity(3_000));
    }
}
