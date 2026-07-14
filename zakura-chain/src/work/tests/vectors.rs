use crate::{
    block::{genesis::regtest_genesis_block, Block, MAX_BLOCK_BYTES},
    parameters::Network,
    serialization::{
        CompactSizeMessage, SerializationError, ZcashDeserialize, ZcashDeserializeInto,
        ZcashSerialize,
    },
    work::equihash::{Error, Solution, REGTEST_SOLUTION_SIZE, SOLUTION_SIZE},
};

use super::super::*;

/// Includes the 32-byte nonce.
const EQUIHASH_SOLUTION_BLOCK_OFFSET: usize = equihash::Solution::INPUT_LENGTH + 32;

/// Includes the 3-byte equihash length field.
const BLOCK_HEADER_LENGTH: usize = EQUIHASH_SOLUTION_BLOCK_OFFSET + 3 + equihash::SOLUTION_SIZE;

#[test]
fn equihash_solution_test_vectors() {
    let _init_guard = zakura_test::init();

    for block in zakura_test::vectors::BLOCKS.iter() {
        let solution_bytes = &block[EQUIHASH_SOLUTION_BLOCK_OFFSET..BLOCK_HEADER_LENGTH];

        let solution = solution_bytes
            .zcash_deserialize_into::<equihash::Solution>()
            .expect("Test vector EquihashSolution should deserialize");

        let mut data = Vec::new();
        solution
            .zcash_serialize(&mut data)
            .expect("Test vector EquihashSolution should serialize");

        assert_eq!(solution_bytes.len(), data.len());
        assert_eq!(solution_bytes, data.as_slice());
    }
}

#[test]
fn equihash_solution_test_vectors_are_valid() -> color_eyre::eyre::Result<()> {
    let _init_guard = zakura_test::init();

    for block in zakura_test::vectors::BLOCKS.iter() {
        let block =
            Block::zcash_deserialize(&block[..]).expect("block test vector should deserialize");

        block
            .header
            .solution
            .check(&block.header, &Network::Mainnet)?;
    }

    Ok(())
}

static EQUIHASH_SIZE_TESTS: &[usize] = &[
    0,
    1,
    REGTEST_SOLUTION_SIZE - 1,
    REGTEST_SOLUTION_SIZE,
    REGTEST_SOLUTION_SIZE + 1,
    SOLUTION_SIZE - 1,
    SOLUTION_SIZE,
    SOLUTION_SIZE + 1,
    (MAX_BLOCK_BYTES - 1) as usize,
    MAX_BLOCK_BYTES as usize,
];

#[test]
fn equihash_solution_size_field() {
    let _init_guard = zakura_test::init();

    for size in EQUIHASH_SIZE_TESTS.iter().copied() {
        let mut data = Vec::new();

        let compact_size: CompactSizeMessage = size
            .try_into()
            .expect("test size fits in MAX_PROTOCOL_MESSAGE_LEN");
        compact_size
            .zcash_serialize(&mut data)
            .expect("CompactSize should serialize");
        data.resize(data.len() + SOLUTION_SIZE, 0);

        let result = Solution::zcash_deserialize(data.as_slice());
        match size {
            REGTEST_SOLUTION_SIZE => assert!(
                matches!(
                    result.expect("Regtest size field in EquihashSolution should deserialize"),
                    Solution::Regtest(_),
                ),
                "Regtest size field should deserialize as a Regtest solution",
            ),
            SOLUTION_SIZE => assert!(
                matches!(
                    result.expect("Common size field in EquihashSolution should deserialize"),
                    Solution::Common(_),
                ),
                "Common size field should deserialize as a Common solution",
            ),
            _ => {
                result
                    .expect_err("Wrong size field in EquihashSolution should fail on deserialize");
            }
        }
    }
}

#[test]
fn equihash_solution_rejects_oversize_compactsize_before_allocating() {
    let _init_guard = zakura_test::init();

    let mut data = Vec::new();
    let oversize: CompactSizeMessage = (SOLUTION_SIZE + 1)
        .try_into()
        .expect("fits in MAX_PROTOCOL_MESSAGE_LEN");
    oversize
        .zcash_serialize(&mut data)
        .expect("CompactSize should serialize");

    let err = Solution::zcash_deserialize(data.as_slice())
        .expect_err("oversize equihash CompactSize must fail to deserialize");

    // This is fragile, but the only current way to check if the deserializer
    // rejected the size before allocating.
    // If this fails, double check if the message error has not changed.
    assert!(
        matches!(
            err,
            SerializationError::Parse("incorrect equihash solution size"),
        ),
        "expected size-rejection Parse error, got: {err:?}",
    );
}

/// Regression test for the Regtest-solution proof-of-work downgrade.
///
/// A 36-byte [`Solution::Regtest`] is only a valid proof of work under the toy
/// `(48, 5)` Equihash parameters. It must never be accepted on Mainnet or
/// Testnet, which require the memory-hard `(200, 9)` parameters. Previously the
/// `(n, k)` parameters were selected from the solution length alone, so a peer
/// could downgrade the PoW to `(48, 5)` by sending a short solution on Mainnet.
#[test]
fn regtest_solution_is_rejected_off_regtest() {
    let _init_guard = zakura_test::init();

    let block = Block::zcash_deserialize(zakura_test::vectors::BLOCKS[0])
        .expect("block test vector should deserialize");
    let mut header = *block.header;

    // A short Regtest-shaped solution, as a malicious peer would send it.
    header.solution = Solution::Regtest([0; REGTEST_SOLUTION_SIZE]);

    // Rejected on Mainnet and Testnet by the network-parameter binding, before
    // the Equihash verifier is ever reached.
    for network in [Network::Mainnet, Network::new_default_testnet()] {
        assert!(
            matches!(
                header.solution.check(&header, &network),
                Err(Error::InvalidSolutionSize { .. }),
            ),
            "a 36-byte Regtest solution must be rejected on {network}",
        );
    }

    // On Regtest the variant is permitted, so the check proceeds to Equihash
    // verification rather than rejecting on size. (The all-zero solution is not
    // a valid (48, 5) proof, so this is an Equihash error, not `Ok`, and in
    // particular not `InvalidSolutionSize`.)
    assert!(
        !matches!(
            header
                .solution
                .check(&header, &Network::new_regtest(Default::default())),
            Err(Error::InvalidSolutionSize { .. }),
        ),
        "a Regtest solution must be accepted for verification on Regtest",
    );
}

#[test]
fn real_regtest_solution_is_bound_to_regtest_parameters() {
    let _init_guard = zakura_test::init();

    let block = regtest_genesis_block();
    let header = block.header.as_ref();
    let regtest = Network::new_regtest(Default::default());

    header
        .solution
        .check(header, &regtest)
        .expect("the hard-coded Regtest genesis solution must verify on Regtest");

    for network in [Network::Mainnet, Network::new_default_testnet()] {
        assert!(
            matches!(
                header.solution.check(header, &network),
                Err(Error::InvalidSolutionSize { .. }),
            ),
            "a real Regtest proof must not verify on {network}",
        );
    }
}

/// Regression test for the reverse network-parameter mismatch.
///
/// A known-valid Mainnet `(200, 9)` proof must not be accepted under Regtest's
/// `(48, 5)` parameters.
#[test]
fn real_common_solution_is_rejected_on_regtest() {
    let _init_guard = zakura_test::init();

    let block = Block::zcash_deserialize(zakura_test::vectors::BLOCKS[0])
        .expect("block test vector should deserialize");
    let header = block.header.as_ref();
    let regtest = Network::new_regtest(Default::default());

    header
        .solution
        .check(header, &Network::Mainnet)
        .expect("the hard-coded Mainnet solution must verify on Mainnet");

    assert!(
        matches!(
            header.solution.check(header, &regtest),
            Err(Error::InvalidSolutionSize { .. }),
        ),
        "a real Common proof must not verify on Regtest",
    );
}
