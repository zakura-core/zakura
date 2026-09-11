//! The release facts that decide which artifacts a node trusts.

use zakura_chain::{
    block::Height,
    parameters::{
        spentness_hints::{release_commitments, Commitment, REVOKED_COMMITMENTS},
        Network,
    },
};

use super::SpentnessError;
use crate::service::finalized_state::{
    commitment_aux::FinalFrontiers,
    vct::{embedded_final_frontiers, retained_spentness_frontiers},
};

/// Commitments, revocations, and VCT handoff frontiers from one release.
///
/// Production code uses [`ReleaseAuthority::compiled`]. Tests construct their own
/// authority for synthetic artifacts.
#[derive(Clone, Debug)]
pub(crate) struct ReleaseAuthority {
    network: Network,
    /// Recognized commitments, oldest first. The newest one starts fresh runs.
    commitments: Vec<Commitment>,
    /// Revoked artifact digests. Recognition never overrides revocation.
    revoked: Vec<[u8; 32]>,
    /// Handoff frontiers kept for older supported commitments, by artifact digest.
    ///
    /// The embedded handoff frontier covers the release's own last checkpoint.
    retained_frontiers: Vec<([u8; 32], Vec<u8>)>,
}

impl ReleaseAuthority {
    /// The authority compiled into this release.
    pub(crate) fn compiled(network: &Network) -> Self {
        Self {
            network: network.clone(),
            commitments: release_commitments(network).to_vec(),
            revoked: REVOKED_COMMITMENTS.to_vec(),
            retained_frontiers: retained_spentness_frontiers()
                .iter()
                .map(|(digest, bytes)| (*digest, bytes.to_vec()))
                .collect(),
        }
    }

    /// An authority for synthetic test artifacts.
    #[cfg(test)]
    pub(crate) fn new(
        network: &Network,
        commitments: Vec<Commitment>,
        revoked: Vec<[u8; 32]>,
        retained_frontiers: Vec<([u8; 32], Vec<u8>)>,
    ) -> Self {
        Self {
            network: network.clone(),
            commitments,
            revoked,
            retained_frontiers,
        }
    }

    /// The commitment that new runs use.
    pub(crate) fn newest(&self) -> Result<&Commitment, SpentnessError> {
        self.commitments
            .last()
            .ok_or(SpentnessError::NoReviewedCommitment)
    }

    /// Accept only a well-formed, recognized, unrevoked commitment for this chain.
    pub(crate) fn check(&self, commitment: &Commitment) -> Result<(), SpentnessError> {
        commitment.validate()?;
        if self.revoked.contains(&commitment.sha256) {
            return Err(SpentnessError::Revoked {
                digest: commitment.digest_hex(),
            });
        }
        if !self.commitments.contains(commitment) {
            return Err(SpentnessError::UnknownCommitment {
                digest: commitment.digest_hex(),
            });
        }
        let genesis = self.network.checkpoint_list().hash(Height(0));
        if genesis.map(|hash| hash.0) != Some(commitment.chain_identity) {
            return Err(SpentnessError::WrongChain);
        }
        Ok(())
    }

    /// The reviewed VCT frontiers at the commitment's terminal height.
    pub(in crate::service::finalized_state) fn handoff_frontiers(
        &self,
        commitment: &Commitment,
    ) -> Result<FinalFrontiers, SpentnessError> {
        let at_terminal =
            |frontiers: &FinalFrontiers| frontiers.height.0 == commitment.terminal_height;
        let retained = self
            .retained_frontiers
            .iter()
            .find(|(digest, _)| *digest == commitment.sha256)
            .and_then(|(_, bytes)| FinalFrontiers::from_bytes(bytes).ok());
        retained
            .filter(at_terminal)
            .or_else(|| embedded_final_frontiers(&self.network).filter(at_terminal))
            .ok_or(SpentnessError::MissingHandoffFrontiers {
                height: commitment.terminal_height,
            })
    }
}
