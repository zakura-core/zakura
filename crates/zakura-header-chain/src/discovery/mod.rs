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

    /// Derive one episode for a contiguous selected range with no durable auxiliary rows.
    fn for_empty_selected_range(
        state_version: crate::StateVersion,
        selected_range: &[Frontier],
        terminal_boundary_hash: Option<block::Hash>,
    ) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"zakura-vct-auxiliary-range-requirement-episode-v1");
        hasher.update(state_version.get().to_le_bytes());
        for (index, target) in selected_range.iter().enumerate() {
            hasher.update(target.height.0.to_le_bytes());
            hasher.update(target.hash.0);
            let boundary_hash = selected_range
                .get(index.saturating_add(1))
                .map(|successor| successor.hash)
                .or(terminal_boundary_hash);
            match boundary_hash {
                Some(boundary_hash) => {
                    hasher.update([1]);
                    hasher.update(boundary_hash.0);
                }
                None => hasher.update([0]),
            }
            // The range builder admits a target only after it proves that no durable row exists.
            hasher.update([0]);
        }
        Self(hasher.finalize().into())
    }

    pub(crate) const fn digest(self) -> [u8; 32] {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SelectedRepairRange {
    /// Contiguous selected targets covered by this repair, beginning at the blocking target.
    frontiers: Box<[Frontier]>,
    /// Selected successor after the range, when one exists.
    terminal_boundary_hash: Option<block::Hash>,
    /// Whether the blocking target has any durable auxiliary row.
    has_durable_rows: bool,
}

/// Selected-header request context for one auxiliary VCT repair.
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
    /// Private selected-range state keeps the public context and port shapes stable.
    selected_range: Box<SelectedRepairRange>,
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
            selected_range: Box::new(SelectedRepairRange {
                frontiers: Box::new([target]),
                terminal_boundary_hash: boundary_hash,
                has_durable_rows: false,
            }),
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
            selected_range: Box::new(SelectedRepairRange {
                frontiers: Box::new([target]),
                terminal_boundary_hash: boundary_hash,
                has_durable_rows: !rows.is_empty(),
            }),
        })
    }

    /// Extend an unconstrained exact repair across a contiguous selected suffix with no rows.
    ///
    /// `suffix` begins with the selected successor of [`Self::target`]. The caller must prove
    /// that every target in the resulting range has no durable auxiliary row.
    pub fn extend_empty_selected_range(
        mut self,
        suffix: &[Frontier],
        terminal_boundary_hash: Option<block::Hash>,
    ) -> Result<Self, StoreError> {
        if self.selected_range.has_durable_rows
            || !self.admission_capacity_available
            || suffix.is_empty()
        {
            return Err(StoreError::Incoherent(
                "a constrained VCT repair context cannot become a range",
            ));
        }
        let mut previous = self.target;
        for target in suffix {
            let expected_height = previous
                .height
                .next()
                .map_err(|_| StoreError::Incoherent("VCT repair range height overflowed"))?;
            if target.height != expected_height {
                return Err(StoreError::Incoherent(
                    "VCT repair range is not height-contiguous",
                ));
            }
            previous = *target;
        }
        let mut selected_range = Vec::with_capacity(suffix.len().saturating_add(1));
        selected_range.push(self.target);
        selected_range.extend_from_slice(suffix);
        self.episode = AuxiliaryRequirementEpisode::for_empty_selected_range(
            self.state_version,
            &selected_range,
            terminal_boundary_hash,
        );
        self.selected_range = Box::new(SelectedRepairRange {
            frontiers: selected_range.into_boxed_slice(),
            terminal_boundary_hash,
            has_durable_rows: false,
        });
        Ok(self)
    }

    /// Return the number of selected headers covered by this repair context.
    pub fn selected_header_count(&self) -> usize {
        self.selected_range.frontiers.len()
    }

    /// Return the last selected target that a peer must serve for this context.
    pub fn request_target(&self) -> Frontier {
        *self
            .selected_range
            .frontiers
            .last()
            .expect("every VCT repair context contains its blocking target")
    }

    /// Return whether `headers` names the complete selected range in order.
    pub fn matches_selected_range(&self, headers: &[Frontier]) -> bool {
        self.selected_range.frontiers.as_ref() == headers
    }

    /// Shorten this context to at most `max_headers` selected headers.
    ///
    /// The returned episode binds the selected prefix, its authentication boundaries, the state
    /// version, and the absence of durable auxiliary rows. Exact constrained repairs can only
    /// return their one-header context.
    pub fn bounded_prefix(&self, max_headers: usize) -> Option<Self> {
        let prefix_len = max_headers.min(self.selected_range.frontiers.len());
        if prefix_len == 0 {
            return None;
        }
        if prefix_len == self.selected_range.frontiers.len() {
            return Some(self.clone());
        }
        let mut prefix = self.clone();
        prefix.selected_range = Box::new(SelectedRepairRange {
            frontiers: self.selected_range.frontiers[..prefix_len].into(),
            terminal_boundary_hash: Some(self.selected_range.frontiers[prefix_len].hash),
            has_durable_rows: false,
        });
        prefix.episode = if prefix_len == 1 {
            AuxiliaryRequirementEpisode::for_target(
                prefix.target,
                prefix.selected_range.terminal_boundary_hash,
                &[],
            )
        } else {
            AuxiliaryRequirementEpisode::for_empty_selected_range(
                prefix.state_version,
                &prefix.selected_range.frontiers,
                prefix.selected_range.terminal_boundary_hash,
            )
        };
        Some(prefix)
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
    fn repair_range_prefixes_bind_selection_boundaries_and_state_version() {
        let target = Frontier::new(block::Height(1), hash_at(block::Height(1)));
        let predecessor = Frontier::new(block::Height(0), hash_at(block::Height(0)));
        let suffix = [
            Frontier::new(block::Height(2), hash_at(block::Height(2))),
            Frontier::new(block::Height(3), hash_at(block::Height(3))),
        ];
        let build = |state_version| {
            VctRepairContext::from_durable_rows(
                target,
                HeaderLocator::for_continuation(predecessor),
                StateVersion::new(state_version),
                Some(suffix[0].hash),
                true,
                &[],
            )
            .expect("an empty exact repair context is coherent")
            .extend_empty_selected_range(&suffix, Some(hash_at(block::Height(4))))
            .expect("the selected empty suffix is contiguous")
        };

        let full = build(7);
        assert_eq!(full.selected_header_count(), 3);
        assert_eq!(full.request_target(), suffix[1]);
        assert!(full.matches_selected_range(&[target, suffix[0], suffix[1]]));

        let prefix = full
            .bounded_prefix(2)
            .expect("a positive repair prefix exists");
        assert_eq!(prefix.selected_header_count(), 2);
        assert_eq!(prefix.request_target(), suffix[0]);
        assert!(prefix.matches_selected_range(&[target, suffix[0]]));
        assert_ne!(prefix.episode, full.episode);
        let exact_prefix = full
            .bounded_prefix(1)
            .expect("a one-header repair prefix exists");
        let exact = VctRepairContext::from_durable_rows(
            target,
            HeaderLocator::for_continuation(predecessor),
            StateVersion::new(7),
            Some(suffix[0].hash),
            true,
            &[],
        )
        .expect("an exact empty repair context is coherent");
        assert_eq!(exact_prefix, exact);
        assert_ne!(build(8).episode, full.episode);
        assert!(full.bounded_prefix(0).is_none());
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
        assert_eq!(first.selected_header_count(), 1);
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

        let authenticated =
            UntrustedAuxDeliveryRow::new(delivery(17), 1, [Some([18; 32]), None], boundary_hash);
        let authenticated = VctRepairContext::from_durable_rows(
            target,
            locator.clone(),
            StateVersion::new(1),
            boundary_hash,
            true,
            &[authenticated],
        )
        .expect("the authenticated row is coherent");
        assert_eq!(authenticated.selected_header_count(), 1);

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
        assert_eq!(disputed.selected_header_count(), 1);
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
