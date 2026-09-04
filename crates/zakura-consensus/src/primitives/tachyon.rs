//! Asynchronous verification of Tachyon proof stamps.

use crate::{block::tachyon::AggregateCoverage, error::BlockError};

use super::spawn_fifo;

/// Verifies a Tachyon aggregate's proof stamp against all covered actions.
pub async fn verify_proof_stamp(aggregate: AggregateCoverage) -> Result<(), BlockError> {
    spawn_fifo(move || {
        let adjunct_refs: Vec<_> = aggregate
            .adjuncts
            .iter()
            .map(|adjunct| adjunct.as_dyn())
            .collect();

        match aggregate
            .bundle
            .verify_proof(&mut rand_10::rng(), &adjunct_refs)
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
