use zakura_chain::{
    block::{self, Commitment, CommitmentError},
    parameters::Network,
};

/// Parse and validate the height- and network-specific commitment field structure.
pub fn validate_commitment_structure(
    header: &block::Header,
    network: &Network,
    height: block::Height,
) -> Result<Commitment, CommitmentError> {
    header.commitment(network, height)
}
