use zakura_chain::{block, parameters::Network};

use super::{
    validate_commitment_structure, validate_compact_target, validate_encoding_version_hash,
    validate_hash_filter, PowPolicy,
};

pub(crate) fn validate_trusted_anchor_observables(
    header: &block::Header,
    network: &Network,
    height: block::Height,
) -> Result<block::Hash, &'static str> {
    let hash =
        validate_encoding_version_hash(header).map_err(|_| "canonical header version and hash")?;
    validate_commitment_structure(header, network, height)
        .map_err(|_| "height-dependent commitment structure")?;
    let target =
        validate_compact_target(header, network).map_err(|_| "compact target and network limit")?;
    let pow_policy =
        PowPolicy::for_network(network).map_err(|_| "authenticated proof-of-work policy")?;

    // A network seeded with unsolved blocks enforces proof of work only from its
    // configured start height. The prepare pipeline, checkpoint verification, and
    // semantic verification all make this exemption, so the trusted anchor has to
    // as well: the seeded tip is itself an unsolved block, and rejecting it stops
    // the seed chain from ever being attached.
    let skip_pow = network.should_skip_pow_at_height(height);

    if !skip_pow && !pow_policy.is_authenticated_custom_waiver() {
        validate_hash_filter(hash, target).map_err(|_| "header hash filter")?;
    }
    if !skip_pow {
        pow_policy
            .validate_solution(header)
            .map_err(|_| "Equihash solution shape or proof")?;
    }
    Ok(hash)
}
