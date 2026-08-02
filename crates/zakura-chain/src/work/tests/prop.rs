//! Randomised property tests for Proof of Work.

use std::{env, sync::Arc};

use proptest::{prelude::*, test_runner::Config};

use crate::{
    block::{self, Block},
    parameters::Network,
    serialization::{ZcashDeserialize, ZcashDeserializeInto, ZcashSerialize},
};

use super::super::*;

const DEFAULT_TEST_INPUT_PROPTEST_CASES: u32 = 64;

fn equihash_proptest_cases() -> u32 {
    env::var("PROPTEST_CASES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_TEST_INPUT_PROPTEST_CASES)
}

fn test_headers() -> Vec<block::Header> {
    zakura_test::vectors::BLOCKS
        .iter()
        .map(|block_bytes| {
            let block = Block::zcash_deserialize(&block_bytes[..])
                .expect("block test vector should deserialize");
            *block.header
        })
        .collect()
}

#[test]
fn equihash_solution_roundtrip() {
    let _init_guard = zakura_test::init();

    proptest!(|(solution in any::<equihash::Solution>())| {
        let data = solution
            .zcash_serialize_to_vec()
            .expect("randomized EquihashSolution should serialize");
        let solution2 = data
            .zcash_deserialize_into()
            .expect("randomized EquihashSolution should deserialize");

        prop_assert_eq![solution, solution2];
    });
}

prop_compose! {
    fn randomized_solutions(real_header: block::Header)
        (fake_solution in any::<equihash::Solution>()
            .prop_filter("solution must not be the actual solution", move |s| {
                s != &real_header.solution
            })
        ) -> Arc<block::Header> {

        let mut fake_header = real_header;
        fake_header.solution = fake_solution;

        Arc::new(fake_header)
    }
}

#[test]
fn equihash_test_vectors_validate() -> color_eyre::eyre::Result<()> {
    let _init_guard = zakura_test::init();

    for header in test_headers() {
        header.solution.check(&header, &Network::Mainnet)?;
    }

    Ok(())
}

#[test]
fn equihash_prop_test_solution() -> color_eyre::eyre::Result<()> {
    let _init_guard = zakura_test::init();

    let headers = test_headers();

    // Every test vector gets a deterministic invalid solution, so vector
    // coverage does not depend on random case selection.
    for real_header in &headers {
        let mut fake_header = *real_header;
        fake_header.solution = equihash::Solution::for_proposal();
        assert_ne!(fake_header.solution, real_header.solution);
        fake_header
            .solution
            .check(&fake_header, &Network::Mainnet)
            .expect_err("block header should not validate with the null solution");
    }

    // Randomized cases sample across the complete vector set instead of
    // multiplying the same case budget by every vector.
    let randomized_headers = proptest::sample::select(headers).prop_flat_map(randomized_solutions);
    proptest!(Config::with_cases(equihash_proptest_cases()),
        |(fake_header in randomized_headers)| {
        fake_header.solution
            .check(&fake_header, &Network::Mainnet)
            .expect_err("block header should not validate on randomized solution");
    });

    Ok(())
}

prop_compose! {
    fn randomized_nonce(real_header: block::Header)
        (fake_nonce in proptest::array::uniform32(any::<u8>())
            .prop_filter("nonce must not be the actual nonce", move |fake_nonce| {
                fake_nonce != &real_header.nonce.0
            })
        ) -> Arc<block::Header> {

        let mut fake_header = real_header;
        fake_header.nonce = fake_nonce.into();

        Arc::new(fake_header)
    }
}

#[test]
fn equihash_prop_test_nonce() -> color_eyre::eyre::Result<()> {
    let _init_guard = zakura_test::init();

    let headers = test_headers();

    for real_header in &headers {
        let mut fake_header = *real_header;
        fake_header.nonce.0[0] ^= 1;
        assert_ne!(fake_header.nonce, real_header.nonce);
        fake_header
            .solution
            .check(&fake_header, &Network::Mainnet)
            .expect_err("block header should not validate with a changed nonce");
    }

    let randomized_headers = proptest::sample::select(headers).prop_flat_map(randomized_nonce);
    proptest!(Config::with_cases(equihash_proptest_cases()),
        |(fake_header in randomized_headers)| {
        fake_header.solution
            .check(&fake_header, &Network::Mainnet)
            .expect_err("block header should not validate on randomized nonce");
    });

    Ok(())
}

prop_compose! {
    fn randomized_input(real_header: block::Header)
        (fake_header in any::<block::Header>()
            .prop_map(move |mut fake_header| {
                fake_header.nonce = real_header.nonce;
                fake_header.solution = real_header.solution;
                Arc::new(fake_header)
            })
            .prop_filter("input must not be the actual input", move |fake_header| {
                fake_header.as_ref() != &real_header
            })
        ) -> Arc<block::Header> {

        fake_header
    }
}

#[test]
fn equihash_prop_test_input() -> color_eyre::eyre::Result<()> {
    let _init_guard = zakura_test::init();

    let headers = test_headers();

    for real_header in &headers {
        let mut fake_header = *real_header;
        fake_header.previous_block_hash.0[0] ^= 1;
        assert_ne!(fake_header, *real_header);
        fake_header
            .solution
            .check(&fake_header, &Network::Mainnet)
            .expect_err("equihash solution should not validate with changed input");
    }

    let randomized_headers = proptest::sample::select(headers).prop_flat_map(randomized_input);
    proptest!(Config::with_cases(equihash_proptest_cases()),
        |(fake_header in randomized_headers)| {
        fake_header.solution
            .check(&fake_header, &Network::Mainnet)
            .expect_err("equihash solution should not validate on randomized input");
    });

    Ok(())
}
