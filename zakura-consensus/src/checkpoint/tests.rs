//! Tests for checkpoint-based block verification

#![allow(clippy::unwrap_in_result)]

use std::{cmp::min, time::Duration};

use color_eyre::eyre::{eyre, Report};
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::time::timeout;
use tracing_futures::Instrument;

use zakura_chain::{
    local_genesis::{generate_local_testnet_with_funded_keys, LocalTestnetGenesisOptions},
    parameters::Network::*,
    serialization::ZcashDeserialize,
};

use super::*;

/// The timeout we apply to each verify future during testing.
///
/// The checkpoint verifier uses `tokio::sync::oneshot` channels as futures.
/// If the verifier doesn't send a message on the channel, any tests that
/// await the channel future will hang.
///
/// This value is set to a large value, to avoid spurious failures due to
/// high system load.
const VERIFY_TIMEOUT_SECONDS: u64 = 10;

#[tokio::test(flavor = "multi_thread")]
async fn single_item_checkpoint_list_test() -> Result<(), Report> {
    single_item_checkpoint_list().await
}

#[spandoc::spandoc]
async fn single_item_checkpoint_list() -> Result<(), Report> {
    let _init_guard = zakura_test::init();

    let block0 =
        Arc::<Block>::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES[..])?;
    let hash0 = block0.hash();

    // Make a checkpoint list containing only the genesis block
    let genesis_checkpoint_list: BTreeMap<block::Height, block::Hash> =
        [(block0.coinbase_height().unwrap(), hash0)]
            .iter()
            .cloned()
            .collect();

    let state_service = zakura_state::init_test(&Mainnet).await;
    let mut checkpoint_verifier =
        CheckpointVerifier::from_list(genesis_checkpoint_list, &Mainnet, None, state_service)
            .map_err(|e| eyre!(e))?;

    assert_eq!(
        checkpoint_verifier.previous_checkpoint_height(),
        BeforeGenesis
    );
    assert_eq!(
        checkpoint_verifier.target_checkpoint_height(),
        WaitingForBlocks
    );
    assert_eq!(
        checkpoint_verifier.checkpoint_list.max_height(),
        block::Height(0)
    );

    /// SPANDOC: Make sure the verifier service is ready
    let ready_verifier_service = checkpoint_verifier.ready().map_err(|e| eyre!(e)).await?;
    /// SPANDOC: Set up the future for block 0
    let verify_future = timeout(
        Duration::from_secs(VERIFY_TIMEOUT_SECONDS),
        ready_verifier_service.call(block0.clone()),
    );
    /// SPANDOC: Wait for the response for block 0
    // TODO(teor || jlusby): check error kind
    let verify_response = verify_future
        .map_err(|e| eyre!(e))
        .await
        .expect("timeout should not happen")
        .expect("block should verify");

    assert_eq!(verify_response, hash0);

    assert_eq!(
        checkpoint_verifier.previous_checkpoint_height(),
        FinalCheckpoint
    );
    assert_eq!(
        checkpoint_verifier.target_checkpoint_height(),
        FinishedVerifying
    );
    assert_eq!(
        checkpoint_verifier.checkpoint_list.max_height(),
        block::Height(0)
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn multi_item_checkpoint_list_test() -> Result<(), Report> {
    multi_item_checkpoint_list().await
}

#[spandoc::spandoc]
async fn multi_item_checkpoint_list() -> Result<(), Report> {
    let _init_guard = zakura_test::init();

    // Parse all the blocks
    let mut checkpoint_data = Vec::new();
    for b in &[
        // This list is used as a checkpoint list, and as a list of blocks to
        // verify. So it must be continuous.
        &zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES[..],
        &zakura_test::vectors::BLOCK_MAINNET_1_BYTES[..],
    ] {
        let block = Arc::<Block>::zcash_deserialize(*b)?;
        let hash = block.hash();
        checkpoint_data.push((block.clone(), block.coinbase_height().unwrap(), hash));
    }

    // Make a checkpoint list containing all the blocks
    let checkpoint_list: BTreeMap<block::Height, block::Hash> = checkpoint_data
        .iter()
        .map(|(_block, height, hash)| (*height, *hash))
        .collect();

    let state_service = zakura_state::init_test(&Mainnet).await;
    let mut checkpoint_verifier =
        CheckpointVerifier::from_list(checkpoint_list, &Mainnet, None, state_service)
            .map_err(|e| eyre!(e))?;

    assert_eq!(
        checkpoint_verifier.previous_checkpoint_height(),
        BeforeGenesis
    );
    assert_eq!(
        checkpoint_verifier.target_checkpoint_height(),
        WaitingForBlocks
    );
    assert_eq!(
        checkpoint_verifier.checkpoint_list.max_height(),
        block::Height(1)
    );

    // Now verify each block
    for (block, height, hash) in checkpoint_data {
        /// SPANDOC: Make sure the verifier service is ready
        let ready_verifier_service = checkpoint_verifier.ready().map_err(|e| eyre!(e)).await?;

        /// SPANDOC: Set up the future for block {?height}
        let verify_future = timeout(
            Duration::from_secs(VERIFY_TIMEOUT_SECONDS),
            ready_verifier_service.call(block.clone()),
        );
        /// SPANDOC: Wait for the response for block {?height}
        // TODO(teor || jlusby): check error kind
        let verify_response = verify_future
            .map_err(|e| eyre!(e))
            .await
            .expect("timeout should not happen")
            .expect("future should succeed");

        assert_eq!(verify_response, hash);

        if height < checkpoint_verifier.checkpoint_list.max_height() {
            assert_eq!(
                checkpoint_verifier.previous_checkpoint_height(),
                PreviousCheckpoint(height)
            );
            assert_eq!(
                checkpoint_verifier.target_checkpoint_height(),
                WaitingForBlocks
            );
        } else {
            assert_eq!(
                checkpoint_verifier.previous_checkpoint_height(),
                FinalCheckpoint
            );
            assert_eq!(
                checkpoint_verifier.target_checkpoint_height(),
                FinishedVerifying
            );
        }
        assert_eq!(
            checkpoint_verifier.checkpoint_list.max_height(),
            block::Height(1)
        );
    }

    assert_eq!(
        checkpoint_verifier.previous_checkpoint_height(),
        FinalCheckpoint
    );
    assert_eq!(
        checkpoint_verifier.target_checkpoint_height(),
        FinishedVerifying
    );
    assert_eq!(
        checkpoint_verifier.checkpoint_list.max_height(),
        block::Height(1)
    );

    Ok(())
}

/// Every block of a generated local testnet (genesis, premine, and maturity
/// padding) must verify against the network's own configured checkpoints, and
/// the verifier must finish once the last generated block is in.
///
/// The generated network checkpoints every seed block, so this exercises the
/// checkpoint path a fresh local-genesis node uses to accept its seed chain.
#[tokio::test(flavor = "multi_thread")]
async fn generated_local_seed_chain_passes_checkpoint_verification() -> Result<(), Report> {
    let _init_guard = zakura_test::init();
    let generated = generate_local_testnet_with_funded_keys(
        vec!["alice".to_string(), "bob".to_string()],
        LocalTestnetGenesisOptions {
            maturity_padding_blocks: 2,
            ..Default::default()
        },
    )
    .map_err(|error| eyre!(error.to_string()))?;
    let network = generated.network;
    let state_service = zakura_state::init_test(&network).await;
    let mut checkpoint_verifier = CheckpointVerifier::new(&network, None, state_service);

    for block in generated.blocks {
        let block = Arc::new(block);
        let expected_hash = block.hash();
        let response = timeout(
            Duration::from_secs(VERIFY_TIMEOUT_SECONDS),
            checkpoint_verifier
                .ready()
                .map_err(|error| eyre!(error))
                .await?
                .call(block),
        )
        .await
        .expect("generated checkpoint verification should not time out")
        .expect("generated seed block should verify");

        assert_eq!(response, expected_hash);
    }

    assert_eq!(
        checkpoint_verifier.previous_checkpoint_height(),
        FinalCheckpoint
    );
    assert_eq!(
        checkpoint_verifier.target_checkpoint_height(),
        FinishedVerifying
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn continuous_blockchain_no_restart() -> Result<(), Report> {
    for network in Network::iter() {
        continuous_blockchain(None, network).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn continuous_blockchain_restart() -> Result<(), Report> {
    for height in 0..zakura_test::vectors::CONTINUOUS_MAINNET_BLOCKS.len() {
        continuous_blockchain(Some(block::Height(height.try_into().unwrap())), Mainnet).await?;
    }
    for height in 0..zakura_test::vectors::CONTINUOUS_TESTNET_BLOCKS.len() {
        continuous_blockchain(
            Some(block::Height(height.try_into().unwrap())),
            Network::new_default_testnet(),
        )
        .await?;
    }
    Ok(())
}

/// Test a continuous blockchain on `network`, restarting verification at `restart_height`.
//
// This span is far too verbose for use during normal testing.
// Turn the SPANDOC: comments into doc comments to re-enable.
//#[spandoc::spandoc]
async fn continuous_blockchain(
    restart_height: Option<block::Height>,
    network: Network,
) -> Result<(), Report> {
    let _init_guard = zakura_test::init();

    // A continuous blockchain
    let blockchain = network.blockchain_iter();

    let blockchain: Vec<_> = blockchain
        .map(|(height, b)| {
            let block = Arc::<Block>::zcash_deserialize(*b).unwrap();
            let hash = block.hash();
            let coinbase_height = block.coinbase_height().unwrap();
            assert_eq!(*height, coinbase_height.0);
            (block, coinbase_height, hash)
        })
        .collect();
    let blockchain_len = blockchain.len();

    // Use some of the blocks as checkpoints
    // We use these indexes so that we test:
    //   - checkpoints don't have to be the same length
    //   - checkpoints start at genesis
    //   - checkpoints end at the end of the range (there's no point in having extra blocks)
    let expected_max_height = block::Height((blockchain_len - 1).try_into().unwrap());
    let checkpoint_list = [
        &blockchain[0],
        &blockchain[blockchain_len / 3],
        &blockchain[blockchain_len / 2],
        &blockchain[blockchain_len - 1],
    ];
    let checkpoint_list: BTreeMap<block::Height, block::Hash> = checkpoint_list
        .iter()
        .map(|(_block, height, hash)| (*height, *hash))
        .collect();

    // SPANDOC: Verify blocks, restarting at {?restart_height} {?network}
    {
        let initial_tip = restart_height.map(|block::Height(height)| {
            (blockchain[height as usize].1, blockchain[height as usize].2)
        });
        let state_service = zakura_state::init_test(&Mainnet).await;
        let mut checkpoint_verifier = CheckpointVerifier::from_list(
            checkpoint_list,
            &network,
            initial_tip,
            state_service.clone(),
        )
        .map_err(|e| eyre!(e))?;

        // Setup checks
        if restart_height.is_some() {
            assert!(
                restart_height <= Some(checkpoint_verifier.checkpoint_list.max_height()),
                "restart heights after the final checkpoint are not supported by this test"
            );
        }
        if restart_height
            .map(|h| h == checkpoint_verifier.checkpoint_list.max_height())
            .unwrap_or(false)
        {
            assert_eq!(
                checkpoint_verifier.previous_checkpoint_height(),
                FinalCheckpoint
            );
            assert_eq!(
                checkpoint_verifier.target_checkpoint_height(),
                FinishedVerifying
            );
        } else {
            assert_eq!(
                checkpoint_verifier.previous_checkpoint_height(),
                restart_height.map(InitialTip).unwrap_or(BeforeGenesis)
            );
            assert_eq!(
                checkpoint_verifier.target_checkpoint_height(),
                WaitingForBlocks
            );
        }
        assert_eq!(
            checkpoint_verifier.checkpoint_list.max_height(),
            expected_max_height
        );

        let mut handles = FuturesUnordered::new();

        // Now verify each block
        for (block, height, _hash) in blockchain {
            // Commit directly to the state until after the (fake) restart height
            if let Some(restart_height) = restart_height {
                if height <= restart_height {
                    let mut state_service = state_service.clone();
                    // SPANDOC: Make sure the state service is ready for block {?height}
                    let ready_state_service = state_service.ready().map_err(|e| eyre!(e)).await?;

                    // SPANDOC: Add block directly to the state {?height}
                    ready_state_service
                        .call(zakura_state::Request::CommitCheckpointVerifiedBlock(
                            block.clone().into(),
                        ))
                        .await
                        .map_err(|e| eyre!(e))?;

                    // Skip verification for (fake) previous blocks
                    continue;
                }
            }

            // SPANDOC: Make sure the verifier service is ready for block {?height}
            let ready_verifier_service = checkpoint_verifier.ready().map_err(|e| eyre!(e)).await?;

            // SPANDOC: Set up the future for block {?height}
            let verify_future = timeout(
                Duration::from_secs(VERIFY_TIMEOUT_SECONDS),
                ready_verifier_service.call(block.clone()),
            );

            // SPANDOC: spawn verification future in the background for block {?height}
            let handle = tokio::spawn(verify_future.in_current_span());
            handles.push(handle);

            // Execution checks
            if height < checkpoint_verifier.checkpoint_list.max_height() {
                assert_eq!(
                    checkpoint_verifier.target_checkpoint_height(),
                    WaitingForBlocks
                );
            } else {
                assert_eq!(
                    checkpoint_verifier.previous_checkpoint_height(),
                    FinalCheckpoint
                );
                assert_eq!(
                    checkpoint_verifier.target_checkpoint_height(),
                    FinishedVerifying
                );
            }
        }

        // Check that we have the correct number of verify tasks
        if let Some(block::Height(restart_height)) = restart_height {
            let restart_height = restart_height as usize;
            if restart_height == blockchain_len - 1 {
                assert_eq!(
                    handles.len(),
                    0,
                    "unexpected number of verify tasks for restart height: {restart_height:?}",
                );
            } else {
                assert_eq!(
                    handles.len(),
                    blockchain_len - restart_height - 1,
                    "unexpected number of verify tasks for restart height: {restart_height:?}",
                );
            }
        } else {
            assert_eq!(
                handles.len(),
                blockchain_len,
                "unexpected number of verify tasks with no restart height",
            );
        }

        // SPANDOC: wait on spawned verification tasks for restart height {?restart_height} {?network}
        while let Some(result) = handles.next().await {
            result??.map_err(|e| eyre!(e))?;
        }

        // Final checks
        assert_eq!(
            checkpoint_verifier.previous_checkpoint_height(),
            FinalCheckpoint,
            "unexpected previous checkpoint for restart height: {restart_height:?}",
        );
        assert_eq!(
            checkpoint_verifier.target_checkpoint_height(),
            FinishedVerifying,
            "unexpected target checkpoint for restart height: {restart_height:?}",
        );
        assert_eq!(
            checkpoint_verifier.checkpoint_list.max_height(),
            expected_max_height,
            "unexpected max checkpoint height for restart height: {restart_height:?}",
        );
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn block_higher_than_max_checkpoint_fail_test() -> Result<(), Report> {
    block_higher_than_max_checkpoint_fail().await
}

#[spandoc::spandoc]
async fn block_higher_than_max_checkpoint_fail() -> Result<(), Report> {
    let _init_guard = zakura_test::init();

    let block0 =
        Arc::<Block>::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES[..])?;
    let block415000 =
        Arc::<Block>::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_415000_BYTES[..])?;

    // Make a checkpoint list containing only the genesis block
    let genesis_checkpoint_list: BTreeMap<block::Height, block::Hash> =
        [(block0.coinbase_height().unwrap(), block0.as_ref().into())]
            .iter()
            .cloned()
            .collect();

    let state_service = zakura_state::init_test(&Mainnet).await;
    let mut checkpoint_verifier =
        CheckpointVerifier::from_list(genesis_checkpoint_list, &Mainnet, None, state_service)
            .map_err(|e| eyre!(e))?;

    assert_eq!(
        checkpoint_verifier.previous_checkpoint_height(),
        BeforeGenesis
    );
    assert_eq!(
        checkpoint_verifier.target_checkpoint_height(),
        WaitingForBlocks
    );
    assert_eq!(
        checkpoint_verifier.checkpoint_list.max_height(),
        block::Height(0)
    );

    /// SPANDOC: Make sure the verifier service is ready
    let ready_verifier_service = checkpoint_verifier.ready().map_err(|e| eyre!(e)).await?;
    /// SPANDOC: Set up the future for block 415000
    let verify_future = timeout(
        Duration::from_secs(VERIFY_TIMEOUT_SECONDS),
        ready_verifier_service.call(block415000.clone()),
    );
    /// SPANDOC: Wait for the response for block 415000, and expect failure
    // TODO(teor || jlusby): check error kind
    let _ = verify_future
        .map_err(|e| eyre!(e))
        .await
        .expect("timeout should not happen")
        .expect_err("bad block hash should fail");

    assert_eq!(
        checkpoint_verifier.previous_checkpoint_height(),
        BeforeGenesis
    );
    assert_eq!(
        checkpoint_verifier.target_checkpoint_height(),
        WaitingForBlocks
    );
    assert_eq!(
        checkpoint_verifier.checkpoint_list.max_height(),
        block::Height(0)
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn wrong_checkpoint_hash_fail_test() -> Result<(), Report> {
    wrong_checkpoint_hash_fail().await
}

#[spandoc::spandoc]
async fn wrong_checkpoint_hash_fail() -> Result<(), Report> {
    let _init_guard = zakura_test::init();

    let good_block0 =
        Arc::<Block>::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES[..])?;
    let good_block0_hash = good_block0.hash();

    // Change the header hash
    let mut bad_block0 = good_block0.clone();
    let bad_block0_mut = Arc::make_mut(&mut bad_block0);
    Arc::make_mut(&mut bad_block0_mut.header).version = 5;

    // Make a checkpoint list containing the genesis block checkpoint
    let genesis_checkpoint_list: BTreeMap<block::Height, block::Hash> =
        [(good_block0.coinbase_height().unwrap(), good_block0_hash)]
            .iter()
            .cloned()
            .collect();

    let state_service = zakura_state::init_test(&Mainnet).await;
    let mut checkpoint_verifier =
        CheckpointVerifier::from_list(genesis_checkpoint_list, &Mainnet, None, state_service)
            .map_err(|e| eyre!(e))?;

    assert_eq!(
        checkpoint_verifier.previous_checkpoint_height(),
        BeforeGenesis
    );
    assert_eq!(
        checkpoint_verifier.target_checkpoint_height(),
        WaitingForBlocks
    );
    assert_eq!(
        checkpoint_verifier.checkpoint_list.max_height(),
        block::Height(0)
    );

    /// SPANDOC: Make sure the verifier service is ready (1/3)
    let ready_verifier_service = checkpoint_verifier.ready().map_err(|e| eyre!(e)).await?;
    /// SPANDOC: Set up the future for bad block 0 (1/3)
    // TODO(teor || jlusby): check error kind
    let bad_verify_future_1 = timeout(
        Duration::from_secs(VERIFY_TIMEOUT_SECONDS),
        ready_verifier_service.call(bad_block0.clone()),
    );
    // We can't await the future yet, because bad blocks aren't cleared
    // until the chain is verified

    assert_eq!(
        checkpoint_verifier.previous_checkpoint_height(),
        BeforeGenesis
    );
    assert_eq!(
        checkpoint_verifier.target_checkpoint_height(),
        WaitingForBlocks
    );
    assert_eq!(
        checkpoint_verifier.checkpoint_list.max_height(),
        block::Height(0)
    );

    /// SPANDOC: Make sure the verifier service is ready (2/3)
    let ready_verifier_service = checkpoint_verifier.ready().map_err(|e| eyre!(e)).await?;
    /// SPANDOC: Set up the future for bad block 0 again (2/3)
    // TODO(teor || jlusby): check error kind
    let bad_verify_future_2 = timeout(
        Duration::from_secs(VERIFY_TIMEOUT_SECONDS),
        ready_verifier_service.call(bad_block0.clone()),
    );
    // We can't await the future yet, because bad blocks aren't cleared
    // until the chain is verified

    assert_eq!(
        checkpoint_verifier.previous_checkpoint_height(),
        BeforeGenesis
    );
    assert_eq!(
        checkpoint_verifier.target_checkpoint_height(),
        WaitingForBlocks
    );
    assert_eq!(
        checkpoint_verifier.checkpoint_list.max_height(),
        block::Height(0)
    );

    /// SPANDOC: Make sure the verifier service is ready (3/3)
    let ready_verifier_service = checkpoint_verifier.ready().map_err(|e| eyre!(e)).await?;
    /// SPANDOC: Set up the future for good block 0 (3/3)
    let good_verify_future = timeout(
        Duration::from_secs(VERIFY_TIMEOUT_SECONDS),
        ready_verifier_service.call(good_block0.clone()),
    );
    /// SPANDOC: Wait for the response for good block 0, and expect success (3/3)
    // TODO(teor || jlusby): check error kind
    let verify_response = good_verify_future
        .map_err(|e| eyre!(e))
        .await
        .expect("timeout should not happen")
        .expect("future should succeed");

    assert_eq!(verify_response, good_block0_hash);

    assert_eq!(
        checkpoint_verifier.previous_checkpoint_height(),
        FinalCheckpoint
    );
    assert_eq!(
        checkpoint_verifier.target_checkpoint_height(),
        FinishedVerifying
    );
    assert_eq!(
        checkpoint_verifier.checkpoint_list.max_height(),
        block::Height(0)
    );

    // Now, await the bad futures, which should have completed

    /// SPANDOC: Wait for the response for block 0, and expect failure (1/3)
    // TODO(teor || jlusby): check error kind
    let _ = bad_verify_future_1
        .map_err(|e| eyre!(e))
        .await
        .expect("timeout should not happen")
        .expect_err("bad block hash should fail");

    assert_eq!(
        checkpoint_verifier.previous_checkpoint_height(),
        FinalCheckpoint
    );
    assert_eq!(
        checkpoint_verifier.target_checkpoint_height(),
        FinishedVerifying
    );
    assert_eq!(
        checkpoint_verifier.checkpoint_list.max_height(),
        block::Height(0)
    );

    /// SPANDOC: Wait for the response for block 0, and expect failure again (2/3)
    // TODO(teor || jlusby): check error kind
    let _ = bad_verify_future_2
        .map_err(|e| eyre!(e))
        .await
        .expect("timeout should not happen")
        .expect_err("bad block hash should fail");

    assert_eq!(
        checkpoint_verifier.previous_checkpoint_height(),
        FinalCheckpoint
    );
    assert_eq!(
        checkpoint_verifier.target_checkpoint_height(),
        FinishedVerifying
    );
    assert_eq!(
        checkpoint_verifier.checkpoint_list.max_height(),
        block::Height(0)
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn checkpoint_drop_cancel_test() -> Result<(), Report> {
    checkpoint_drop_cancel().await
}

#[spandoc::spandoc]
async fn checkpoint_drop_cancel() -> Result<(), Report> {
    let _init_guard = zakura_test::init();

    // Parse all the blocks
    let mut checkpoint_data = Vec::new();
    for b in &[
        // Continuous blocks are verified
        &zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES[..],
        &zakura_test::vectors::BLOCK_MAINNET_1_BYTES[..],
        // Other blocks can't verify, so they are rejected on drop
        &zakura_test::vectors::BLOCK_MAINNET_415000_BYTES[..],
        &zakura_test::vectors::BLOCK_MAINNET_434873_BYTES[..],
    ] {
        let block = Arc::<Block>::zcash_deserialize(*b)?;
        let hash = block.hash();
        checkpoint_data.push((block.clone(), block.coinbase_height().unwrap(), hash));
    }

    // Make a checkpoint list containing all the blocks
    let checkpoint_list: BTreeMap<block::Height, block::Hash> = checkpoint_data
        .iter()
        .map(|(_block, height, hash)| (*height, *hash))
        .collect();

    let state_service = zakura_state::init_test(&Mainnet).await;
    let mut checkpoint_verifier =
        CheckpointVerifier::from_list(checkpoint_list, &Mainnet, None, state_service)
            .map_err(|e| eyre!(e))?;

    assert_eq!(
        checkpoint_verifier.previous_checkpoint_height(),
        BeforeGenesis
    );
    assert_eq!(
        checkpoint_verifier.target_checkpoint_height(),
        WaitingForBlocks
    );
    assert_eq!(
        checkpoint_verifier.checkpoint_list.max_height(),
        block::Height(434873)
    );

    let mut futures = Vec::new();
    // Now collect verify futures for each block
    for (block, height, hash) in checkpoint_data {
        /// SPANDOC: Make sure the verifier service is ready
        let ready_verifier_service = checkpoint_verifier.ready().map_err(|e| eyre!(e)).await?;

        /// SPANDOC: Set up the future for block {?height}
        let verify_future = timeout(
            Duration::from_secs(VERIFY_TIMEOUT_SECONDS),
            ready_verifier_service.call(block.clone()),
        );

        futures.push((verify_future, height, hash));

        // Only continuous checkpoints verify
        assert_eq!(
            checkpoint_verifier.previous_checkpoint_height(),
            PreviousCheckpoint(block::Height(min(height.0, 1)))
        );
        assert_eq!(
            checkpoint_verifier.target_checkpoint_height(),
            WaitingForBlocks
        );
        assert_eq!(
            checkpoint_verifier.checkpoint_list.max_height(),
            block::Height(434873)
        );
    }

    // Now drop the verifier, to cancel the futures
    drop(checkpoint_verifier);

    for (verify_future, height, hash) in futures {
        /// SPANDOC: Check the response for block {?height}
        let verify_response = verify_future
            .map_err(|e| eyre!(e))
            .await
            .expect("timeout should not happen");

        if height <= block::Height(1) {
            let verify_hash =
                verify_response.expect("Continuous checkpoints should have succeeded before drop");
            assert_eq!(verify_hash, hash);
        } else {
            // TODO(teor || jlusby): check error kind
            verify_response.expect_err("Pending futures should fail on drop");
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn hard_coded_mainnet_test() -> Result<(), Report> {
    hard_coded_mainnet().await
}

#[spandoc::spandoc]
async fn hard_coded_mainnet() -> Result<(), Report> {
    let _init_guard = zakura_test::init();

    let block0 =
        Arc::<Block>::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES[..])?;
    let hash0 = block0.hash();

    let state_service = zakura_state::init_test(&Mainnet).await;
    // Use the hard-coded checkpoint list
    let mut checkpoint_verifier = CheckpointVerifier::new(&Network::Mainnet, None, state_service);

    assert_eq!(
        checkpoint_verifier.previous_checkpoint_height(),
        BeforeGenesis
    );
    assert_eq!(
        checkpoint_verifier.target_checkpoint_height(),
        WaitingForBlocks
    );
    assert!(checkpoint_verifier.checkpoint_list.max_height() > block::Height(0));

    /// SPANDOC: Make sure the verifier service is ready
    let ready_verifier_service = checkpoint_verifier.ready().map_err(|e| eyre!(e)).await?;
    /// SPANDOC: Set up the future for block 0
    let verify_future = timeout(
        Duration::from_secs(VERIFY_TIMEOUT_SECONDS),
        ready_verifier_service.call(block0.clone()),
    );
    /// SPANDOC: Wait for the response for block 0
    // TODO(teor || jlusby): check error kind
    let verify_response = verify_future
        .map_err(|e| eyre!(e))
        .await
        .expect("timeout should not happen")
        .expect("block should verify");

    assert_eq!(verify_response, hash0);

    assert_eq!(
        checkpoint_verifier.previous_checkpoint_height(),
        PreviousCheckpoint(block::Height(0))
    );
    assert_eq!(
        checkpoint_verifier.target_checkpoint_height(),
        WaitingForBlocks
    );
    // The lists will get bigger over time, so we just pick a recent height
    assert!(checkpoint_verifier.checkpoint_list.max_height() > block::Height(900_000));

    Ok(())
}

/// Regression: a superseded in-queue duplicate (`NewerRequest`) must not rewind
/// checkpoint progress after the current range has already verified.
///
/// Production failure mode (temp-zakura-sync-test-1 @ 494086):
/// block sync resubmits bodies still sitting in the checkpoint queue. The older
/// request fails with `NewerRequest`, and today's error path treats that as a
/// state desync and `reset_progress`es to the live tip. Those resets sit on the
/// verifier's `mpsc` and are applied on a later `call()`, after
/// `PreviousCheckpoint` has already advanced — opening a permanent queue gap so
/// the next checkpoint range never verifies.
///
/// Expected correct behavior: after range `(3, 6]` verifies, a late
/// `NewerRequest` must leave progress at `PreviousCheckpoint(6)` so `(6, 10]`
/// can still complete.
#[tokio::test(flavor = "multi_thread")]
async fn newer_request_must_not_rewind_verified_checkpoint_progress() -> Result<(), Report> {
    let _init_guard = zakura_test::init();

    let blockchain: Vec<_> = zakura_test::vectors::CONTINUOUS_MAINNET_BLOCKS
        .iter()
        .map(|(height, bytes)| {
            let block = Arc::<Block>::zcash_deserialize(*bytes).expect("block deserializes");
            let hash = block.hash();
            let coinbase_height = block.coinbase_height().expect("coinbase height");
            assert_eq!(*height, coinbase_height.0);
            (block, coinbase_height, hash)
        })
        .collect();
    assert!(
        blockchain.len() > 10,
        "continuous mainnet vectors must cover heights 0..=10"
    );

    // Three checkpoint gaps: (.., 3], (3, 6], (6, 10].
    let checkpoint_list: BTreeMap<block::Height, block::Hash> = [0usize, 3, 6, 10]
        .into_iter()
        .map(|index| {
            let (_block, height, hash) = &blockchain[index];
            (*height, *hash)
        })
        .collect();

    let state_service = zakura_state::init_test(&Mainnet).await;
    let mut checkpoint_verifier =
        CheckpointVerifier::from_list(checkpoint_list, &Mainnet, None, state_service)
            .map_err(|e| eyre!(e))?;

    // Verify through the first post-genesis checkpoint and wait for commits so
    // the state tip sits at height 3 before the buggy reset path runs.
    let mut first_range = FuturesUnordered::new();
    for (block, _height, _hash) in &blockchain[..=3] {
        let ready = checkpoint_verifier.ready().map_err(|e| eyre!(e)).await?;
        first_range.push(ready.call(block.clone()));
    }
    timeout(Duration::from_secs(VERIFY_TIMEOUT_SECONDS), async {
        while let Some(result) = first_range.next().await {
            result.map_err(|e| eyre!(e))?;
        }
        Ok::<_, Report>(())
    })
    .await
    .expect("first-range verify should not time out")?;
    assert_eq!(
        checkpoint_verifier.previous_checkpoint_height(),
        PreviousCheckpoint(block::Height(3))
    );

    let block4 = blockchain[4].0.clone();
    let block5 = blockchain[5].0.clone();
    let block6 = blockchain[6].0.clone();

    // Keep the first height-4 future pending so we can drive its NewerRequest
    // error (and the resulting reset) after the range has verified.
    let ready = checkpoint_verifier.ready().map_err(|e| eyre!(e)).await?;
    let superseded_height_4 = ready.call(block4.clone());

    let ready = checkpoint_verifier.ready().map_err(|e| eyre!(e)).await?;
    let height_5 = ready.call(block5);

    // Resubmit height 4 while it is still queued: replaces the older request
    // with NewerRequest on the superseded oneshot.
    let ready = checkpoint_verifier.ready().map_err(|e| eyre!(e)).await?;
    let replacement_height_4 = ready.call(block4);

    // Completing the continuous chain verifies (3, 6] and advances progress.
    let ready = checkpoint_verifier.ready().map_err(|e| eyre!(e)).await?;
    let height_6 = ready.call(block6);
    assert_eq!(
        checkpoint_verifier.previous_checkpoint_height(),
        PreviousCheckpoint(block::Height(6)),
        "range (3, 6] should verify before the superseded future is polled"
    );

    // Drive the superseded future now. Current code sends reset_progress(tip)
    // for NewerRequest even though the range already verified.
    let superseded_result = timeout(
        Duration::from_secs(VERIFY_TIMEOUT_SECONDS),
        superseded_height_4,
    )
    .await
    .expect("superseded verify should not time out");
    let superseded_err = superseded_result.expect_err(
        "replaced in-queue duplicate must fail the older request with NewerRequest",
    );
    assert!(
        superseded_err.is_duplicate_request(),
        "expected NewerRequest-classified duplicate, got {superseded_err:?}"
    );

    // The next call applies any queued reset. Correct behavior keeps progress
    // at PreviousCheckpoint(6) so the following range can still form.
    let ready = checkpoint_verifier.ready().map_err(|e| eyre!(e)).await?;
    let height_7 = ready.call(blockchain[7].0.clone());

    assert_eq!(
        checkpoint_verifier.previous_checkpoint_height(),
        PreviousCheckpoint(block::Height(6)),
        "NewerRequest must not rewind progress below the already-verified checkpoint"
    );

    // Finish the final range; this is what production could no longer do after
    // the rewind left a permanent queue gap.
    let mut final_range = FuturesUnordered::new();
    final_range.push(height_7);
    for (block, _height, _hash) in &blockchain[8..=10] {
        let ready = checkpoint_verifier.ready().map_err(|e| eyre!(e)).await?;
        final_range.push(ready.call(block.clone()));
    }
    // The replacement height-4 / 5 / 6 commits from the prior range should also
    // succeed; poll them alongside the final range.
    final_range.push(replacement_height_4);
    final_range.push(height_5);
    final_range.push(height_6);

    timeout(Duration::from_secs(VERIFY_TIMEOUT_SECONDS), async {
        while let Some(result) = final_range.next().await {
            result.map_err(|e| eyre!(e))?;
        }
        Ok::<_, Report>(())
    })
    .await
    .expect("post-checkpoint verifies should not time out")?;

    assert_eq!(
        checkpoint_verifier.previous_checkpoint_height(),
        FinalCheckpoint,
        "final checkpoint range must still be reachable after a NewerRequest"
    );
    assert_eq!(
        checkpoint_verifier.target_checkpoint_height(),
        FinishedVerifying
    );

    Ok(())
}

/// Duplicate block errors must stay classified as duplicate requests after the
/// state wraps them, so they don't restart the syncer during checkpoint sync.
#[test]
fn state_commit_duplicate_errors_are_duplicate_requests() {
    let duplicate = zs::CommitBlockError::Duplicate {
        hash_or_height: None,
        location: zs::KnownBlock::Finalized,
    };

    // Box the error the same way the state's `CommitCheckpointVerifiedBlock`
    // handler does. This mirrors the wrapping manually, so it won't fail
    // automatically if the state changes its error type — keep it in sync by hand.
    let source: BoxError = Box::new(zs::CommitCheckpointVerifiedError::from(duplicate));

    let err = VerifyCheckpointError::CommitCheckpointVerified(source);

    assert!(err.is_duplicate_request());
    assert_eq!(err.misbehavior_score(), 0);
}
