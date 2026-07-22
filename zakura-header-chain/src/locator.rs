//! Exact selected-path locators for fork discovery.

use zakura_chain::block;

use crate::{EngineSnapshot, Frontier, StoreError};

/// Maximum hashes in one v8 header locator.
pub const MAX_HEADER_LOCATOR_HASHES: usize = 13;

const SELECTED_PATH_OFFSETS: [u32; 12] = [0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1_000];

/// One ordered, deduplicated locator with locally authenticated heights.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderLocator(Vec<Frontier>);

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
    use crate::{
        AlarmSet, ChainScore, EngineMode, FrontierSet, HeaderGeneration, StateVersion, SuffixWork,
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
}
