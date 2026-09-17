//! Block-level Tachyon consensus rules.

use std::collections::{BTreeSet, HashMap};

use zakura_chain::{block::Block, transaction::WtxId};
use zcash_tachyon::{
    stamp::StampState as _, Bundle, PointerStamp, ProofStamp, Tachygram, TachyonBundle,
    VerifyCoverageError,
};

use crate::error::BlockError;

#[cfg(test)]
mod tests;

/// A proof-stamped bundle and the pointer-stamped bundles it covers.
#[derive(Debug)]
pub(crate) struct AggregateCoverage {
    pub bundle: Bundle<ProofStamp>,
    pub adjuncts: Vec<Bundle<PointerStamp>>,
}

/// Checks block-wide Tachygram distinctness and aggregate coverage coherence.
pub(crate) fn coherence(block: &Block) -> Result<Vec<AggregateCoverage>, BlockError> {
    let mut seen_tachygrams = BTreeSet::<Tachygram>::new();
    let mut aggregates = Vec::<AggregateCoverage>::new();
    let mut aggregates_by_wtxid = HashMap::<[u8; 64], usize>::new();
    let mut adjuncts = Vec::<([u8; 64], Bundle<PointerStamp>)>::new();

    for transaction in &block.transactions {
        let Some(tachyon_shielded_data) = transaction.tachyon_shielded_data() else {
            continue;
        };

        match &tachyon_shielded_data.0 {
            TachyonBundle::NoBundle => {}
            TachyonBundle::Proven(bundle) => {
                for &tachygram in &bundle.stamp.tachygrams {
                    if !seen_tachygrams.insert(tachygram) {
                        return Err(BlockError::DuplicateTachygram);
                    }
                }

                let wtxid = WtxId::from(transaction.as_ref()).as_bytes();
                aggregates_by_wtxid.insert(wtxid, aggregates.len());
                aggregates.push(AggregateCoverage {
                    bundle: bundle.clone(),
                    adjuncts: Vec::new(),
                });
            }
            TachyonBundle::Adjunct(bundle) => {
                adjuncts.push((bundle.stamp.stamp_digest(), bundle.clone()));
            }
        }
    }

    for (target_wtxid, bundle) in adjuncts {
        let Some(&aggregate_index) = aggregates_by_wtxid.get(&target_wtxid) else {
            return Err(BlockError::TachyonAggregateNotFound);
        };
        aggregates[aggregate_index].adjuncts.push(bundle);
    }

    for aggregate in &aggregates {
        let adjunct_refs: Vec<_> = aggregate
            .adjuncts
            .iter()
            .map(|adjunct| adjunct.as_dyn())
            .collect();

        aggregate
            .bundle
            .verify_coverage(&adjunct_refs)
            .map_err(|error| match error {
                VerifyCoverageError::DuplicateActions => BlockError::TachyonDuplicateAction,
                VerifyCoverageError::StampActionsMismatch => BlockError::TachyonCoverageMismatch,
                VerifyCoverageError::TachygramArityMismatch => {
                    BlockError::TachyonTachygramArityMismatch
                }
            })?;
    }

    Ok(aggregates)
}
