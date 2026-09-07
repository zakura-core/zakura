//! Exact selected-path locators for fork discovery.

use zakura_chain::block;

use sha2::{Digest, Sha256};

use crate::{
    semantic_payload_fingerprint, AuxiliaryInputFingerprint, EngineSnapshot, Frontier, SourceId,
    StoreError, UntrustedAuxDeliveryRow, MAX_AUX_DELIVERIES_PER_HEADER_V1,
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
    /// Derive one episode from the exact target and every durable semantic input and outcome.
    fn for_target(
        target: Frontier,
        boundary_hash: Option<block::Hash>,
        rows: &[UntrustedAuxDeliveryRow],
    ) -> Self {
        let mut constraints: Vec<[u8; 32]> = rows
            .iter()
            .copied()
            .map(|row| {
                let mut hasher = Sha256::new();
                hasher.update(b"zakura-vct-auxiliary-requirement-constraint-v1");
                hasher.update([row.outcome_status_code()]);
                match row.delivery().tree_aux {
                    Some(record) => {
                        hasher.update([1]);
                        hasher.update(
                            AuxiliaryInputFingerprint::new(
                                target.hash,
                                record,
                                row.outcome_boundary_hash(),
                            )
                            .digest(),
                        );
                    }
                    None => {
                        hasher.update([0]);
                        match row.outcome_boundary_hash() {
                            Some(boundary) => {
                                hasher.update([1]);
                                hasher.update(boundary.0);
                            }
                            None => hasher.update([0]),
                        }
                    }
                }
                hasher.finalize().into()
            })
            .collect();
        constraints.sort_unstable();
        constraints.dedup();

        let mut hasher = Sha256::new();
        hasher.update(b"zakura-vct-auxiliary-requirement-episode-v1");
        hasher.update(target.height.0.to_le_bytes());
        hasher.update(target.hash.0);
        match boundary_hash {
            Some(boundary_hash) => {
                hasher.update([1]);
                hasher.update(boundary_hash.0);
            }
            None => hasher.update([0]),
        }
        for constraint in constraints {
            hasher.update(constraint);
        }
        Self(hasher.finalize().into())
    }

    pub(crate) const fn digest(self) -> [u8; 32] {
        self.0
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
    /// State version that supplied this atomic repair context.
    pub state_version: crate::StateVersion,
    /// Selected successor that supplies the applicable authentication boundary.
    pub boundary_hash: Option<block::Hash>,
    /// Whether current committed state can admit one distinct rooted payload.
    pub admission_capacity_available: bool,
    /// Semantic inputs that durable rejection or dispute evidence excludes from replacement.
    excluded_inputs: Box<[AuxiliaryInputFingerprint]>,
    /// Every rooted semantic payload already retained for the target.
    retained_payloads: Box<[[u8; 32]]>,
    /// Sources that already supplied one retained rooted payload for this target.
    retained_sources: Box<[SourceId]>,
}

impl VctRepairContext {
    /// Build a repair claim before any durable rejection or dispute exists.
    pub fn unconstrained(
        target: Frontier,
        locator: HeaderLocator,
        boundary_hash: Option<block::Hash>,
    ) -> Self {
        Self {
            target,
            locator,
            episode: AuxiliaryRequirementEpisode::for_target(target, boundary_hash, &[]),
            state_version: crate::StateVersion::new(0),
            boundary_hash,
            admission_capacity_available: true,
            excluded_inputs: Box::new([]),
            retained_payloads: Box::new([]),
            retained_sources: Box::new([]),
        }
    }

    /// Build a repair claim from one selected target and its durable auxiliary outcome rows.
    ///
    /// The claim uses rejected rows as negative recovery constraints. Every retained row binds the
    /// episode and prevents an idempotent delivery replay from completing the repair. The claim
    /// does not promote a recovered outcome into authenticated engine state.
    pub fn from_durable_rows(
        target: Frontier,
        locator: HeaderLocator,
        state_version: crate::StateVersion,
        boundary_hash: Option<block::Hash>,
        admission_capacity_available: bool,
        rows: &[UntrustedAuxDeliveryRow],
    ) -> Result<Self, StoreError> {
        Self::validate_durable_rows(target, rows)?;
        let mut excluded_inputs: Vec<_> = rows
            .iter()
            .filter(|row| matches!(row.outcome_status_code(), 2 | 3))
            .filter_map(|row| {
                row.delivery().tree_aux.map(|record| {
                    AuxiliaryInputFingerprint::new(target.hash, record, row.outcome_boundary_hash())
                })
            })
            .collect();
        excluded_inputs.sort_unstable();
        excluded_inputs.dedup();
        let mut retained_payloads: Vec<_> = rows
            .iter()
            .filter_map(|row| {
                row.delivery()
                    .tree_aux
                    .map(|record| semantic_payload_fingerprint(target.hash, Some(record)))
            })
            .collect();
        retained_payloads.sort_unstable();
        retained_payloads.dedup();
        let mut retained_sources: Vec<_> = rows
            .iter()
            .filter(|row| row.delivery().tree_aux.is_some())
            .map(|row| row.delivery().source)
            .collect();
        retained_sources.sort_unstable();
        retained_sources.dedup();
        Ok(Self {
            target,
            locator,
            episode: AuxiliaryRequirementEpisode::for_target(target, boundary_hash, rows),
            state_version,
            boundary_hash,
            admission_capacity_available,
            excluded_inputs: excluded_inputs.into_boxed_slice(),
            retained_payloads: retained_payloads.into_boxed_slice(),
            retained_sources: retained_sources.into_boxed_slice(),
        })
    }

    /// Check one input against durable rejection or dispute evidence without transport identity.
    pub fn durable_rows_exclude(
        target: Frontier,
        boundary_hash: Option<block::Hash>,
        rows: &[UntrustedAuxDeliveryRow],
        input: crate::TreeAuxRecordV1,
    ) -> Result<bool, StoreError> {
        Self::validate_durable_rows(target, rows)?;
        let input = AuxiliaryInputFingerprint::new(target.hash, input, boundary_hash);
        Ok(rows
            .iter()
            .filter(|row| matches!(row.outcome_status_code(), 2 | 3))
            .filter_map(|row| {
                row.delivery().tree_aux.map(|record| {
                    AuxiliaryInputFingerprint::new(target.hash, record, row.outcome_boundary_hash())
                })
            })
            .any(|rejected| rejected == input))
    }

    fn validate_durable_rows(
        target: Frontier,
        rows: &[UntrustedAuxDeliveryRow],
    ) -> Result<(), StoreError> {
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
                        .tree_aux
                        .is_some_and(|record| record.height != target.height)
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
        Ok(())
    }

    /// Return whether durable rejection or dispute evidence requires different repair input.
    pub fn excludes(&self, input: crate::TreeAuxRecordV1) -> bool {
        let fingerprint =
            AuxiliaryInputFingerprint::new(self.target.hash, input, self.boundary_hash);
        self.excluded_inputs.binary_search(&fingerprint).is_ok()
    }

    /// Return whether state already retains this semantic payload under any outcome boundary.
    pub fn retains_payload(&self, input: crate::TreeAuxRecordV1) -> bool {
        let fingerprint = semantic_payload_fingerprint(self.target.hash, Some(input));
        self.retained_payloads.binary_search(&fingerprint).is_ok()
    }

    /// Return whether this source already supplied one retained rooted payload for the target.
    pub fn retains_source(&self, source: SourceId) -> bool {
        self.retained_sources.binary_search(&source).is_ok()
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
            Some(block::Hash([9; 32])),
        );
        let boundary_hash = Some(block::Hash([9; 32]));
        let first = VctRepairContext::from_durable_rows(
            target,
            locator.clone(),
            StateVersion::new(1),
            boundary_hash,
            true,
            &[rejected],
        )
        .expect("the rejected row is coherent");
        let second = VctRepairContext::from_durable_rows(
            target,
            locator.clone(),
            StateVersion::new(1),
            boundary_hash,
            true,
            &[same_input_new_transport],
        )
        .expect("the replacement transport row is coherent");
        assert!(first.excludes(record));
        assert!(first.retains_payload(record));
        assert!(first.retains_source(rejected.delivery().source));
        assert!(second.excludes(record));
        assert!(second.retains_payload(record));
        assert!(second.retains_source(same_input_new_transport.delivery().source));
        assert!(!first.retains_source(same_input_new_transport.delivery().source));
        assert_eq!(first.episode, second.episode);
        let duplicate_semantic_evidence = VctRepairContext::from_durable_rows(
            target,
            locator.clone(),
            StateVersion::new(1),
            boundary_hash,
            true,
            &[same_input_new_transport, rejected],
        )
        .expect("duplicate semantic evidence under distinct transport is coherent");
        assert_eq!(first.episode, duplicate_semantic_evidence.episode);

        let changed_boundary = VctRepairContext::from_durable_rows(
            target,
            locator.clone(),
            StateVersion::new(1),
            Some(block::Hash([12; 32])),
            true,
            &[rejected],
        )
        .expect("the old rejection remains coherent under a new selected boundary");
        assert!(!changed_boundary.excludes(record));
        assert_ne!(first.episode, changed_boundary.episode);

        let disputed = UntrustedAuxDeliveryRow::new(
            delivery(13),
            3,
            [Some([14; 32]), Some([15; 32])],
            Some(block::Hash([16; 32])),
        );
        let disputed = VctRepairContext::from_durable_rows(
            target,
            locator,
            StateVersion::new(1),
            Some(block::Hash([16; 32])),
            true,
            &[disputed],
        )
        .expect("the disputed row is coherent");
        assert!(disputed.excludes(record));
        let mut independent = record;
        independent.sapling_tx_count = independent.sapling_tx_count.saturating_add(1);
        assert!(!disputed.excludes(independent));
        assert_ne!(
            disputed.episode,
            VctRepairContext::unconstrained(
                target,
                disputed.locator.clone(),
                disputed.boundary_hash,
            )
            .episode
        );
    }
}
