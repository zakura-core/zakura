//! Exact selected-path locators for fork discovery.

use zakura_chain::block;

use sha2::{Digest, Sha256};

use crate::{
    AuxiliaryInputFingerprint, EngineSnapshot, Frontier, StoreError, UntrustedAuxDeliveryRow,
    MAX_AUX_DELIVERIES_PER_HEADER_V1,
};

/// Maximum hashes in one v8 header locator.
pub const MAX_HEADER_LOCATOR_HASHES: usize = 13;

const SELECTED_PATH_OFFSETS: [u32; 12] = [0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1_000];

/// One ordered, deduplicated locator with locally authenticated heights.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderLocator(Vec<Frontier>);

/// Durable identity of the auxiliary evidence that constrains one repair.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct AuxiliaryRequirementEpisode([u8; 32]);

impl AuxiliaryRequirementEpisode {
    /// Derive one episode from the exact target and its durable rejected or disputed inputs.
    fn for_target(target: Frontier, rows: &[UntrustedAuxDeliveryRow]) -> Self {
        let mut constrained: Vec<_> = rows
            .iter()
            .copied()
            .filter(|row| matches!(row.outcome_status_code(), 2 | 3))
            .collect();
        constrained.sort_unstable_by_key(|row| row.delivery().delivery_id);

        let mut hasher = Sha256::new();
        hasher.update(b"zakura-vct-auxiliary-requirement-episode-v1");
        hasher.update(target.height.0.to_le_bytes());
        hasher.update(target.hash.0);
        for row in constrained {
            let delivery = row.delivery();
            hasher.update([row.outcome_status_code()]);
            if let Some(record) = delivery.tree_aux {
                hasher.update(AuxiliaryInputFingerprint::new(target.hash, record).digest());
            }
            for observation in row.observation_digests().into_iter().flatten() {
                hasher.update(observation);
            }
            if let Some(boundary) = row.outcome_boundary_hash() {
                hasher.update(boundary.0);
            }
        }
        Self(hasher.finalize().into())
    }
}

/// Exact selected-header request context for one auxiliary VCT repair.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VctRepairContext {
    /// Selected header whose auxiliary metadata a peer must redeliver.
    pub target: Frontier,
    /// Single-entry locator naming the target's direct selected predecessor.
    pub locator: HeaderLocator,
    /// Durable evidence episode that owns this replacement.
    pub episode: AuxiliaryRequirementEpisode,
    /// Semantic inputs that durable rejection evidence excludes from replacement.
    excluded_inputs: Vec<AuxiliaryInputFingerprint>,
}

impl VctRepairContext {
    /// Build a repair claim before any durable rejection or dispute exists.
    pub fn unconstrained(target: Frontier, locator: HeaderLocator) -> Self {
        Self {
            target,
            locator,
            episode: AuxiliaryRequirementEpisode::for_target(target, &[]),
            excluded_inputs: Vec::new(),
        }
    }

    /// Build a repair claim from one selected target and its durable auxiliary outcome rows.
    ///
    /// The claim uses rejected rows only as negative recovery constraints. It does not promote a
    /// recovered outcome into authenticated engine state.
    pub fn from_durable_rows(
        target: Frontier,
        locator: HeaderLocator,
        rows: &[UntrustedAuxDeliveryRow],
    ) -> Result<Self, StoreError> {
        if rows.len() > MAX_AUX_DELIVERIES_PER_HEADER_V1 {
            return Err(StoreError::Incoherent(
                "VCT repair evidence exceeds the per-header auxiliary limit",
            ));
        }
        let mut delivery_ids: Vec<_> = rows.iter().map(|row| row.delivery().delivery_id).collect();
        delivery_ids.sort_unstable();
        if delivery_ids.windows(2).any(|ids| ids[0] == ids[1])
            || rows.iter().any(|row| {
                let delivery = row.delivery();
                delivery.header_hash != target.hash
                    || delivery
                        .promote_recovered_outcome(
                            row.outcome_status_code(),
                            row.observation_digests(),
                            row.outcome_boundary_hash(),
                        )
                        .is_none()
            })
        {
            return Err(StoreError::Incoherent(
                "VCT repair evidence is malformed or names another target",
            ));
        }
        let mut excluded_inputs: Vec<_> = rows
            .iter()
            .filter(|row| row.outcome_status_code() == 2)
            .filter_map(|row| {
                row.delivery()
                    .tree_aux
                    .map(|record| AuxiliaryInputFingerprint::new(target.hash, record))
            })
            .collect();
        excluded_inputs.sort_unstable();
        excluded_inputs.dedup();
        Ok(Self {
            target,
            locator,
            episode: AuxiliaryRequirementEpisode::for_target(target, rows),
            excluded_inputs,
        })
    }

    /// Return whether durable rejection evidence requires different semantic repair input.
    pub fn excludes(&self, input: crate::TreeAuxRecordV1) -> bool {
        let fingerprint = AuxiliaryInputFingerprint::new(self.target.hash, input);
        self.excluded_inputs.binary_search(&fingerprint).is_ok()
    }
}

impl HeaderLocator {
    /// Build the exact fresh-pursuit locator from one committed selected projection.
    pub fn for_selected_path(
        snapshot: &EngineSnapshot,
        mut selected_hash: impl FnMut(block::Height) -> Result<Option<block::Hash>, StoreError>,
    ) -> Result<Self, StoreError> {
        let finalized = snapshot.frontiers.finalized;
        let tip = snapshot.frontiers.header_best;
        let distance =
            tip.height
                .0
                .checked_sub(finalized.height.0)
                .ok_or(StoreError::Incoherent(
                    "selected tip is below the finalized frontier",
                ))?;
        let mut entries: Vec<Frontier> = Vec::with_capacity(MAX_HEADER_LOCATOR_HASHES);
        for offset in SELECTED_PATH_OFFSETS {
            if offset > distance {
                continue;
            }
            let height = block::Height(tip.height.0 - offset);
            let hash = selected_hash(height)?.ok_or(StoreError::Incoherent(
                "selected locator height is absent from the selected projection",
            ))?;
            let frontier = Frontier::new(height, hash);
            if let Some(existing) = entries.iter().find(|entry| entry.hash == hash) {
                if *existing != frontier {
                    return Err(StoreError::Incoherent(
                        "selected locator repeats one hash at different heights",
                    ));
                }
            } else {
                entries.push(frontier);
            }
        }
        if selected_hash(finalized.height)? != Some(finalized.hash) {
            return Err(StoreError::Incoherent(
                "finalized frontier is absent from the selected projection",
            ));
        }
        if !entries.iter().any(|entry| entry.hash == finalized.hash) {
            entries.push(finalized);
        }
        if entries.len() > MAX_HEADER_LOCATOR_HASHES {
            return Err(StoreError::Incoherent(
                "selected locator exceeds its protocol entry cap",
            ));
        }
        if entries.first().copied() != Some(tip) {
            return Err(StoreError::Incoherent(
                "selected locator does not begin at the committed tip",
            ));
        }
        Ok(Self(entries))
    }

    /// Build the same-target continuation locator from the last returned suffix tip.
    pub fn for_continuation(returned_suffix_tip: Frontier) -> Self {
        Self(vec![returned_suffix_tip])
    }

    /// Ordered height/hash entries used to authenticate a returned common ancestor.
    pub fn entries(&self) -> &[Frontier] {
        &self.0
    }

    /// Ordered hashes encoded into a v8 request.
    pub fn hashes(&self) -> Vec<block::Hash> {
        self.0.iter().map(|frontier| frontier.hash).collect()
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use crate::{
        AlarmSet, AuxDelivery, BodySizeHint, BodyWorkAuthority, ChainScore, EngineMode, EvidenceId,
        FrontierSet, HeaderGeneration, SourceId, StateVersion, SuffixWork, TreeAuxRecordV1,
        VerifiedGeneration,
    };
    use zakura_chain::work::difficulty::U256;

    use super::*;

    fn hash_at(height: block::Height) -> block::Hash {
        block::Hash(
            height
                .0
                .to_le_bytes()
                .repeat(8)
                .try_into()
                .expect("eight u32 encodings are 32 bytes"),
        )
    }

    fn snapshot(tip_height: u32, finalized_height: u32) -> EngineSnapshot {
        let finalized = Frontier::new(
            block::Height(finalized_height),
            hash_at(block::Height(finalized_height)),
        );
        let tip = Frontier::new(
            block::Height(tip_height),
            hash_at(block::Height(tip_height)),
        );
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
            header_best_score: ChainScore::new(SuffixWork::new(U256::from(tip_height)), tip.hash),
            oldest_retained_height: finalized.height,
            alarms: AlarmSet::default(),
        }
    }

    #[test]
    fn selected_path_locators_match_every_offset_and_cap_boundary() {
        for tip_height in 0..=2_000 {
            let snapshot = snapshot(tip_height, 0);
            let locator =
                HeaderLocator::for_selected_path(&snapshot, |height| Ok(Some(hash_at(height))))
                    .expect("the fixture selected projection is complete");
            let expected_heights: Vec<_> = SELECTED_PATH_OFFSETS
                .into_iter()
                .filter(|offset| *offset <= tip_height)
                .map(|offset| block::Height(tip_height - offset))
                .chain(std::iter::once(block::Height(0)))
                .fold(Vec::new(), |mut heights, height| {
                    if !heights.contains(&height) {
                        heights.push(height);
                    }
                    heights
                });
            assert_eq!(
                locator
                    .entries()
                    .iter()
                    .map(|frontier| frontier.height)
                    .collect::<Vec<_>>(),
                expected_heights,
                "tip {tip_height}"
            );
            assert!(locator.entries().len() <= MAX_HEADER_LOCATOR_HASHES);
            assert_eq!(
                locator.entries().first(),
                Some(&snapshot.frontiers.header_best)
            );
            assert_eq!(
                locator.entries().last(),
                Some(&snapshot.frontiers.finalized)
            );
        }
    }

    #[test]
    fn selected_path_locator_appends_a_non_genesis_finalized_frontier() {
        let snapshot = snapshot(2_000, 750);
        let locator =
            HeaderLocator::for_selected_path(&snapshot, |height| Ok(Some(hash_at(height))))
                .expect("the fixture selected projection is complete");

        assert_eq!(
            locator.entries().last(),
            Some(&snapshot.frontiers.finalized)
        );
        assert_eq!(locator.entries().len(), MAX_HEADER_LOCATOR_HASHES);
        assert_eq!(locator.entries()[11].height, block::Height(1_000));
    }

    #[test]
    fn selected_path_locator_fails_closed_on_a_projection_gap() {
        let snapshot = snapshot(10, 0);
        assert_eq!(
            HeaderLocator::for_selected_path(&snapshot, |height| {
                if height == block::Height(8) {
                    Ok(None)
                } else {
                    Ok(Some(hash_at(height)))
                }
            }),
            Err(StoreError::Incoherent(
                "selected locator height is absent from the selected projection"
            ))
        );
    }

    #[test]
    fn repair_context_excludes_rejected_semantic_input_without_transport_identity() {
        let target = Frontier::new(block::Height(1), block::Hash([0x21; 32]));
        let locator = HeaderLocator::for_continuation(snapshot(2, 0).frontiers.finalized);
        let record = TreeAuxRecordV1 {
            height: target.height,
            sapling_root: Default::default(),
            orchard_root: Default::default(),
            ironwood_root: Default::default(),
            sapling_tx_count: 1,
            orchard_tx_count: 2,
            ironwood_tx_count: 3,
            auth_data_root: zakura_chain::block::merkle::AuthDataRoot::from([4; 32]),
        };
        let owner = BodyWorkAuthority::for_snapshot(&snapshot(2, 0))
            .bind(5, NonZeroU64::new(6).expect("six is nonzero"));
        let delivery = |identity: u8| {
            AuxDelivery::new(
                EvidenceId::from_digest([identity; 32]),
                target.hash,
                SourceId::from_digest([identity.wrapping_add(1); 32]),
                owner.into(),
                BodySizeHint::Unknown,
                Some(record),
            )
        };
        let rejected = UntrustedAuxDeliveryRow::new(
            delivery(7),
            2,
            [Some([8; 32]), None],
            Some(block::Hash([9; 32])),
        );
        let same_input_new_transport = UntrustedAuxDeliveryRow::new(
            delivery(10),
            2,
            [Some([11; 32]), None],
            Some(block::Hash([12; 32])),
        );
        let first = VctRepairContext::from_durable_rows(target, locator.clone(), &[rejected])
            .expect("the rejected row is coherent");
        let second = VctRepairContext::from_durable_rows(
            target,
            locator.clone(),
            &[same_input_new_transport],
        )
        .expect("the replacement transport row is coherent");
        assert!(first.excludes(record));
        assert!(second.excludes(record));
        assert_ne!(first.episode, second.episode);

        let disputed = UntrustedAuxDeliveryRow::new(
            delivery(13),
            3,
            [Some([14; 32]), Some([15; 32])],
            Some(block::Hash([16; 32])),
        );
        let disputed = VctRepairContext::from_durable_rows(target, locator, &[disputed])
            .expect("the disputed row is coherent");
        assert!(!disputed.excludes(record));
        assert_ne!(
            disputed.episode,
            VctRepairContext::unconstrained(target, disputed.locator.clone()).episode
        );
    }
}
