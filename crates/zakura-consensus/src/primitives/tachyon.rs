//! Asynchronous verification of Tachyon proof stamps.

use crate::{block::tachyon::AggregateCoverage, error::BlockError};

use super::spawn_fifo;

/// Verifies a Tachyon aggregate's proof stamp against all covered actions.
pub async fn verify_proof_stamp(aggregate: AggregateCoverage) -> Result<(), BlockError> {
    spawn_fifo(move || {
        let adjunct_descriptors: Vec<_> = aggregate
            .adjuncts
            .iter()
            .flat_map(|adjunct| adjunct.descriptors())
            .collect();

        let adjunct_fees: Vec<_> = aggregate
            .adjuncts
            .iter()
            .map(|bundle| bundle.fee_descriptor())
            .collect();
        let (covered_descriptors, covered_fees) = aggregate
            .bundle
            .verify_coverage(&adjunct_descriptors, &adjunct_fees)
            .map_err(|error| BlockError::TachyonProofInvalid(error.to_string()))?;

        let covered_digests = covered_descriptors
            .iter()
            .map(zcash_tachyon::action::Descriptor::digest)
            .chain(
                covered_fees
                    .iter()
                    .map(zcash_tachyon::fee::Descriptor::digest),
            )
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| BlockError::TachyonProofInvalid(error.to_string()))?;

        match aggregate
            .bundle
            .verify_proof(&mut rand_10::rng(), &covered_digests)
        {
            Ok(true) => Ok(()),
            Ok(false) => Err(BlockError::TachyonProofInvalid(
                "proof stamp was disproved".to_string(),
            )),
            Err(error) => Err(BlockError::TachyonProofInvalid(error.to_string())),
        }
    })
    .await
    .map_err(|_| {
        BlockError::Other(
            "threadpool unexpectedly dropped response channel sender; is Zakura shutting down?"
                .to_string(),
        )
    })?
}
