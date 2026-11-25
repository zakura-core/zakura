//! Tests for the checks on metadata whose parent header is unknown.

use std::sync::Arc;

use zakura_chain::{
    block::{self, Header, Height},
    parameters::{Network, MAX_POW_AVERAGING_WINDOW},
    serialization::ZcashSerialize,
    work::{
        difficulty::{ExpandedDifficulty, ParameterDifficulty as _, U256},
        equihash::Solution,
    },
};
use zakura_header_chain::{validate_hash_filter, Frontier, HeaderContextFact, ValidationLease};

use super::{
    blocks::MetaEnvelope,
    close::Bpv1CloseRow,
    commitment::{coinbase_paths, CoinbaseBinding},
    profile::Profile,
    test_support::{block_of, build_v6_coinbase, build_v6_tx, node_id, regtest_nu7},
    unknown_parent::{
        check_binding_and_height, check_proof_of_work, target_bound_applies, TargetBound,
        UnknownParentVerdict,
    },
    wire::PROPAGATION_DEPTH,
};

const INVALID_HEADER: UnknownParentVerdict =
    UnknownParentVerdict::Invalid(Bpv1CloseRow::Row3InvalidHeader);
const INVALID_BINDING: UnknownParentVerdict =
    UnknownParentVerdict::Invalid(Bpv1CloseRow::Row4BadMetaOrCommitment);

/// A two-transaction block at `height` on a regtest-genesis header, and the
/// `HeaderMeta` envelope that binds its coinbase honestly.
fn honest_meta(height: u32) -> (Header, MetaEnvelope) {
    let at = Height(height);
    let block = block_of(
        [0x99; 32],
        vec![
            Arc::new(build_v6_coinbase(at, &[0x51], 1_000_000, Vec::new(), &[])),
            Arc::new(build_v6_tx(at, 1)),
        ],
    );
    let paths = coinbase_paths(&block).expect("the block has a coinbase");
    let header = *block.header;
    let envelope = MetaEnvelope {
        block_id: header.hash(),
        header_bytes: header
            .zcash_serialize_to_vec()
            .expect("a header serializes"),
        binding: CoinbaseBinding {
            coinbase: paths.coinbase,
            txid_siblings: paths.txid_siblings,
            auth_siblings: paths.auth_siblings,
            light_client_root: [0x99; 32],
        },
        coding: Profile::TINY
            .derive_coding(64, 2)
            .expect("TINY codes 64 bytes"),
        part_root: [0xAA; 32],
        proposer_node: node_id(1),
    };
    (header, envelope)
}

/// The header's own target.
fn target_of(header: &Header) -> ExpandedDifficulty {
    header
        .difficulty_threshold
        .to_expanded()
        .expect("the regtest genesis target expands")
}

/// An honest child whose parent is late passes every check and parks.
#[test]
fn honest_child_with_a_late_parent_parks() {
    let network = regtest_nu7();
    let (header, envelope) = honest_meta(10);
    assert_eq!(check_binding_and_height(&header, &envelope, 9), Ok(10));
    let bound = TargetBound {
        best_height: 9,
        max_target: target_of(&header),
    };
    assert_eq!(
        check_proof_of_work(&network, &header, 10, Some(bound)),
        UnknownParentVerdict::Park
    );
    assert_eq!(
        check_proof_of_work(&network, &header, 10, None),
        UnknownParentVerdict::Park
    );
}

/// A fake with a valid binding but a height at or below `top`, or too far
/// above it to enter the window soon, belongs to block sync: it neither
/// parks nor faults its sender.
#[test]
fn out_of_range_height_is_dropped_without_fault() {
    let (header, envelope) = honest_meta(40);
    assert_eq!(
        check_binding_and_height(&header, &envelope, 40),
        Err(UnknownParentVerdict::OutOfRange)
    );
    assert_eq!(
        check_binding_and_height(&header, &envelope, 41),
        Err(UnknownParentVerdict::OutOfRange)
    );
    assert_eq!(
        check_binding_and_height(&header, &envelope, 40 - PROPAGATION_DEPTH - 1),
        Err(UnknownParentVerdict::OutOfRange)
    );
    assert_eq!(
        check_binding_and_height(&header, &envelope, 40 - PROPAGATION_DEPTH),
        Ok(40)
    );

    // A best header at or above the block's height also puts it out of range.
    let bound = TargetBound {
        best_height: 40,
        max_target: target_of(&header),
    };
    assert_eq!(
        check_proof_of_work(&regtest_nu7(), &header, 40, Some(bound)),
        UnknownParentVerdict::OutOfRange
    );
}

/// A binding whose txid path misses the header's Merkle root closes its
/// sender with row 4.
#[test]
fn binding_that_misses_the_merkle_root_closes_row_4() {
    let (mut header, envelope) = honest_meta(10);
    header.merkle_root = block::merkle::Root([0x42; 32]);
    assert_eq!(
        check_binding_and_height(&header, &envelope, 9),
        Err(INVALID_BINDING)
    );

    let (header, mut envelope) = honest_meta(10);
    envelope.binding.txid_siblings[0][0] ^= 1;
    assert_eq!(
        check_binding_and_height(&header, &envelope, 9),
        Err(INVALID_BINDING)
    );
}

/// A target above the bound closes its sender with row 3, except where the
/// Testnet minimum-difficulty rule allows the proof-of-work limit.
#[test]
fn target_above_the_bound_closes_row_3() {
    let network = regtest_nu7();
    let (header, _) = honest_meta(10);
    let target = U256::from(target_of(&header));
    let tight = TargetBound {
        best_height: 9,
        max_target: ExpandedDifficulty::from(target / U256::from(2u64)),
    };
    assert!(target_bound_applies(&network, 10));
    assert_eq!(
        check_proof_of_work(&network, &header, 10, Some(tight)),
        INVALID_HEADER
    );

    // Regtest follows Testnet's minimum-difficulty rule above its start height.
    let late = 300_000;
    assert!(!target_bound_applies(&network, late));
    let tight_late = TargetBound {
        best_height: late - 1,
        ..tight
    };
    assert_eq!(
        check_proof_of_work(&network, &header, late, Some(tight_late)),
        UnknownParentVerdict::Park
    );
}

/// A header whose hash meets its target but whose Equihash solution is invalid
/// closes its sender with row 3.
#[test]
fn invalid_solution_closes_row_3() {
    let network = Network::Mainnet;
    let (mut header, _) = honest_meta(10);
    let limit = network.target_difficulty_limit();
    header.difficulty_threshold = limit.to_compact();
    // Mainnet solutions are 1344 bytes.
    header.solution = Solution::Common([0; 1344]);
    // Grind the nonce until the hash meets the limit, so only the solution fails.
    let mut nonce = 0u32;
    while validate_hash_filter(header.hash(), limit).is_err() {
        nonce = nonce.checked_add(1).expect("a nonce meets the limit soon");
        header.nonce[..4].copy_from_slice(&nonce.to_le_bytes());
    }
    assert_eq!(
        check_proof_of_work(&network, &header, 10, None),
        INVALID_HEADER
    );
}

/// The bound grows by `PoWMaxAdjustDown` per block above the best header and
/// stops at the proof-of-work limit.
#[test]
fn target_bound_grows_per_block_up_to_the_limit() {
    let bound = TargetBound {
        best_height: 100,
        max_target: ExpandedDifficulty::from(U256::from(10_000u64)),
    };
    let limit = ExpandedDifficulty::from(U256::from(1_000_000u64));
    let at = |height| bound.limit_at(height, limit).map(U256::from);
    assert_eq!(at(99), None);
    assert_eq!(at(100), None);
    assert_eq!(at(101), Some(U256::from(13_200u64)));
    assert_eq!(at(102), Some(U256::from(17_424u64)));
    assert_eq!(at(200), Some(U256::from(1_000_000u64)));
    assert_eq!(at(u32::MAX), Some(U256::from(1_000_000u64)));

    // A target near the top of the range grows without the product overflowing.
    let high = TargetBound {
        best_height: 100,
        max_target: ExpandedDifficulty::from(U256::MAX >> 3),
    };
    let max = ExpandedDifficulty::from(U256::MAX);
    let at = |height| high.limit_at(height, max).map(U256::from);
    assert!(at(101) > Some(U256::MAX >> 3));
    assert_eq!(at(120), Some(U256::MAX));
}

/// The bound reads the highest target in the widest averaging window below a
/// long enough chain, and nothing below a short one.
#[test]
fn target_bound_reads_the_widest_window_of_the_lease() {
    let network = regtest_nu7();
    let (template, _) = honest_meta(10);
    let lease = |best: u32, easiest_at: u32| {
        let count = usize::try_from(best + 1)
            .expect("a test height fits usize")
            .min(zakura_header_chain::MAX_POW_ADJUSTMENT_BLOCK_SPAN);
        let facts = (0..count)
            .map(|offset| {
                let height = best - u32::try_from(offset).expect("a test offset fits u32");
                let mut header = template;
                header.nonce[..4].copy_from_slice(&height.to_le_bytes());
                header.difficulty_threshold = if height == easiest_at {
                    network.target_difficulty_limit().to_compact()
                } else {
                    ExpandedDifficulty::from(U256::from(1u64) << 200).to_compact()
                };
                HeaderContextFact {
                    frontier: Frontier::new(Height(height), header.hash()),
                    header: Arc::new(header),
                }
            })
            .collect();
        let parent = Frontier::new(Height(best), block::Hash([0; 32]));
        ValidationLease::new(parent, facts, network.clone(), [0; 32])
    };
    let window = u32::try_from(MAX_POW_AVERAGING_WINDOW).expect("the window fits u32");
    let best = 500;

    let inside =
        TargetBound::from_lease(&lease(best, best - window + 1)).expect("a long chain has a bound");
    assert_eq!(inside.best_height, best);
    assert_eq!(
        inside.max_target.to_compact(),
        network.target_difficulty_limit().to_compact()
    );

    let outside =
        TargetBound::from_lease(&lease(best, best - window)).expect("a long chain has a bound");
    assert_eq!(
        U256::from(outside.max_target),
        U256::from(
            ExpandedDifficulty::from(U256::from(1u64) << 200)
                .to_compact()
                .to_expanded()
                .expect("expands")
        )
    );

    assert_eq!(TargetBound::from_lease(&lease(window - 1, 0)), None);
}
