use std::{cmp::Ordering, collections::HashMap};

use tokio::time::Instant;

use zakura_header_chain::{EngineSnapshot, HeaderLocator, MAX_STAGED_TARGETS_V1};

use super::super::{HeaderSyncRequestId, StatusV8, ZakuraPeerId};

/// One peer's exact, session-bound v8 target claim.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerTargetAdvertisement {
    /// Ordered-stream generation that supplied this status.
    pub session_id: u64,
    /// Local receipt time, used only for freshness and scheduling.
    pub observed_at: Instant,
    /// Exact advisory snapshot supplied by the peer.
    pub status: StatusV8,
}

impl PeerTargetAdvertisement {
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

/// One published request for an exact advertised v8 target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TargetPursuit {
    /// Peer whose current session owns the request.
    pub peer: ZakuraPeerId,
    /// Exact status snapshot being pursued.
    pub advertised: PeerTargetAdvertisement,
    /// Exact coherent state locator sent in the request.
    pub sent_locator: HeaderLocator,
    /// Nonzero request correlation identifier.
    pub request_id: HeaderSyncRequestId,
}

impl TargetPursuit {
    /// Select the continuation-only locator without changing this pursuit's target.
    pub fn continuation_locator(
        &self,
        returned_suffix_tip: zakura_header_chain::Frontier,
    ) -> HeaderLocator {
        HeaderLocator::for_continuation(returned_suffix_tip)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TargetSlot {
    AwaitingLocator {
        advertisement: PeerTargetAdvertisement,
        priority: TargetPriority,
    },
    Active(TargetPursuit),
}

/// Advisory discovery priority. Incomparable claims deliberately map to normal.
#[derive(Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::zakura::header_sync) enum TargetPriority {
    LowerComparableWork,
    Normal,
    HigherComparableWork,
}

impl TargetPriority {
    pub(in crate::zakura::header_sync) fn from_work_order(order: Option<Ordering>) -> Self {
        match order {
            Some(Ordering::Greater) => Self::HigherComparableWork,
            Some(Ordering::Less) => Self::LowerComparableWork,
            Some(Ordering::Equal) | None => Self::Normal,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(in crate::zakura::header_sync) enum StageTargetResult {
    QueryLocator,
    Active,
    AtCapacity,
}

/// Bounded one-target-per-peer v8 pursuit ownership.
#[derive(Clone, Debug, Default)]
pub(in crate::zakura::header_sync) struct TargetPursuitRegistry {
    slots: HashMap<ZakuraPeerId, TargetSlot>,
}

impl TargetPursuitRegistry {
    pub(in crate::zakura::header_sync) fn stage(
        &mut self,
        peer: ZakuraPeerId,
        advertisement: PeerTargetAdvertisement,
        priority: TargetPriority,
    ) -> StageTargetResult {
        if let Some(slot) = self.slots.get_mut(&peer) {
            return match slot {
                TargetSlot::AwaitingLocator {
                    advertisement: current,
                    priority: current_priority,
                } => {
                    *current = advertisement;
                    *current_priority = priority;
                    StageTargetResult::QueryLocator
                }
                TargetSlot::Active(_) => StageTargetResult::Active,
            };
        }
        if self.slots.len() >= MAX_STAGED_TARGETS_V1 {
            let replace = self
                .slots
                .iter()
                .filter_map(|(peer, slot)| match slot {
                    TargetSlot::AwaitingLocator {
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
                return StageTargetResult::AtCapacity;
            };
            self.slots.remove(&replace);
        }
        self.slots.insert(
            peer,
            TargetSlot::AwaitingLocator {
                advertisement,
                priority,
            },
        );
        StageTargetResult::QueryLocator
    }

    pub(in crate::zakura::header_sync) fn awaiting(
        &self,
        peer: &ZakuraPeerId,
        session_id: u64,
        target_tip_hash: zakura_chain::block::Hash,
    ) -> Option<&PeerTargetAdvertisement> {
        match self.slots.get(peer) {
            Some(TargetSlot::AwaitingLocator { advertisement, .. })
                if advertisement.session_id == session_id
                    && advertisement.status.selected_tip_hash == target_tip_hash =>
            {
                Some(advertisement)
            }
            _ => None,
        }
    }

    pub(in crate::zakura::header_sync) fn start(&mut self, pursuit: TargetPursuit) -> bool {
        let peer = pursuit.peer.clone();
        let matches = self.awaiting(
            &peer,
            pursuit.advertised.session_id,
            pursuit.advertised.status.selected_tip_hash,
        ) == Some(&pursuit.advertised);
        if matches {
            self.slots.insert(peer, TargetSlot::Active(pursuit));
        }
        matches
    }

    pub(in crate::zakura::header_sync) fn remove(&mut self, peer: &ZakuraPeerId) {
        self.slots.remove(peer);
    }

    pub(in crate::zakura::header_sync) fn remove_unstarted(&mut self, peer: &ZakuraPeerId) {
        if matches!(
            self.slots.get(peer),
            Some(TargetSlot::AwaitingLocator { .. })
        ) {
            self.slots.remove(peer);
        }
    }

    #[cfg(test)]
    pub(in crate::zakura::header_sync) fn active(
        &self,
        peer: &ZakuraPeerId,
    ) -> Option<&TargetPursuit> {
        match self.slots.get(peer) {
            Some(TargetSlot::Active(pursuit)) => Some(pursuit),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use zakura_chain::{block, work::difficulty::U256};
    use zakura_header_chain::{
        AlarmSet, ChainScore, EngineMode, Frontier, FrontierSet, HeaderGeneration, StateVersion,
        SuffixWork, VerifiedGeneration,
    };

    use super::*;

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

    fn advertisement(marker: u8) -> PeerTargetAdvertisement {
        PeerTargetAdvertisement {
            session_id: 7,
            observed_at: Instant::now(),
            status: StatusV8 {
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
    fn registry_caps_targets_and_only_supersedes_unstarted_work() {
        let mut registry = TargetPursuitRegistry::default();
        for marker in 1..=16 {
            assert_eq!(
                registry.stage(peer(marker), advertisement(marker), TargetPriority::Normal),
                StageTargetResult::QueryLocator
            );
        }
        assert_eq!(
            registry.stage(peer(17), advertisement(17), TargetPriority::Normal),
            StageTargetResult::AtCapacity
        );

        let replacement = advertisement(42);
        assert_eq!(
            registry.stage(peer(1), replacement.clone(), TargetPriority::Normal),
            StageTargetResult::QueryLocator
        );
        assert_eq!(registry.awaiting(&peer(1), 7, hash(42)), Some(&replacement));

        let local = snapshot();
        let locator = HeaderLocator::for_selected_path(&local, |height| {
            let marker = u8::try_from(height.0).expect("the test heights fit in one byte");
            Ok(Some(hash(marker)))
        })
        .expect("the test projection contains every requested frontier");
        let pursuit = TargetPursuit {
            peer: peer(1),
            advertised: replacement.clone(),
            sent_locator: locator.clone(),
            request_id: HeaderSyncRequestId::new(1).expect("one is a nonzero request ID"),
        };
        assert!(registry.start(pursuit.clone()));
        assert_eq!(registry.active(&peer(1)), Some(&pursuit));
        assert_eq!(
            registry.stage(
                peer(1),
                advertisement(43),
                TargetPriority::HigherComparableWork
            ),
            StageTargetResult::Active
        );
        assert_eq!(registry.active(&peer(1)), Some(&pursuit));
        assert_eq!(registry.active(&peer(1)).unwrap().sent_locator, locator);
        let continuation_tip = Frontier::new(block::Height(101), hash(101));
        assert_eq!(
            pursuit.continuation_locator(continuation_tip).entries(),
            &[continuation_tip]
        );
        assert_eq!(pursuit.advertised.status.selected_tip_hash, hash(42));
        assert_eq!(
            TargetPriority::from_work_order(None),
            TargetPriority::Normal
        );
        assert_eq!(
            registry.stage(
                peer(17),
                advertisement(17),
                TargetPriority::HigherComparableWork,
            ),
            StageTargetResult::QueryLocator
        );
        assert!(registry.awaiting(&peer(17), 7, hash(17)).is_some());
    }
}
