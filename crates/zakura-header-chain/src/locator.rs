//! Exact selected-path locators for fork discovery.

use zakura_chain::block;

use crate::{EngineSnapshot, Frontier, StoreError};

/// Maximum hashes in one v8 header locator.
pub const MAX_HEADER_LOCATOR_HASHES: usize = 13;

const SELECTED_PATH_OFFSETS: [u32; 12] = [0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1_000];

/// One ordered, deduplicated locator with locally authenticated heights.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderLocator(Vec<Frontier>);

/// Exact selected-header request context for one auxiliary VCT repair.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VctRepairContext {
    /// Already-selected header whose auxiliary metadata must be redelivered.
    pub target: Frontier,
    /// Single-entry locator naming the target's direct selected predecessor.
    pub locator: HeaderLocator,
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
