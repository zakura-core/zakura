//! Every proof-of-work gate honours a configured `pow_start_height`.
//!
//! A private network seeded with cheap unsolved blocks enforces proof of work
//! only from one past its seeded tip. Each gate that a seed block passes
//! through has to make that exemption; one that does not rejects the seed chain
//! the whole network is built on. An 80-node fleet died at exactly this point
//! because the trusted anchor still applied the hash filter.

use super::super::*;
use zakura_chain::{
    block::{self, genesis::regtest_genesis_block},
    parameters::{
        testnet::{Parameters, RegtestParameters},
        Network,
    },
    work::difficulty::ParameterDifficulty as _,
};

/// Build a custom network that enforces proof of work from `start`.
fn network_with_pow_start(start: u32) -> Network {
    // Borrow Regtest's proof-of-work limit so the seeded header's declared
    // target is inside the network's limit. `validate_compact_target` runs
    // before the hash filter and is deliberately not part of the exemption:
    // a seed block still declares a representable, in-limit target.
    let limit = Network::new_regtest(RegtestParameters::default()).target_difficulty_limit();

    Parameters::build()
        .with_network_name("PowStartHeight")
        .expect("network name is valid")
        .with_target_difficulty_limit(limit)
        .expect("the regtest limit is a valid expanded value")
        .with_pow_start_height(block::Height(start))
        .expect("a positive start height is valid")
        .to_network()
        .expect("the configured parameters are valid")
}

#[test]
fn the_trusted_anchor_exempts_seed_blocks_below_the_start_height() {
    let network = network_with_pow_start(206);
    // The regtest genesis header carries a proposal-shaped solution and a hash
    // that does not meet any real target, which is exactly the shape of a
    // block `kresko genesis` seeds without solving Equihash.
    let header = *regtest_genesis_block().header;

    assert!(
        network.should_skip_pow_at_height(block::Height(205)),
        "205 is below the configured start height"
    );
    assert!(
        !network.should_skip_pow_at_height(block::Height(206)),
        "the start height itself is enforced"
    );

    // Below the start height the anchor accepts the unsolved header.
    validate_trusted_anchor_observables(&header, &network, block::Height(205))
        .expect("a seed block below the start height attaches");

    // At and above it, the same header is rejected: the exemption is bounded,
    // so the first live-mined block still has to do the real work.
    assert!(
        validate_trusted_anchor_observables(&header, &network, block::Height(206)).is_err(),
        "the exemption must not leak past the seeded tip"
    );
}

#[test]
fn a_network_without_a_start_height_enforces_proof_of_work_everywhere() {
    let network = Parameters::build()
        .with_network_name("NoPowStart")
        .expect("network name is valid")
        .to_network()
        .expect("the configured parameters are valid");

    assert!(!network.should_skip_pow_at_height(block::Height(0)));
    assert!(!network.should_skip_pow_at_height(block::Height(205)));
}
