//! Test submitblock RPC method on Regtest.
//!
//! This test will get block templates via the `getblocktemplate` RPC method and submit them as new blocks
//! via the `submitblock` RPC method on Regtest.

use std::{sync::Arc, time::Duration};

use color_eyre::eyre::{eyre, Context, Result};
use tower::BoxError;

use zakura_chain::{
    amount::{Amount, NonNegative},
    block::{self, Block, Height},
    parameters::{testnet::ConfiguredActivationHeights, Network, NetworkKind, NetworkUpgrade},
    primitives::byte_array::increment_big_endian,
    serialization::{ZcashDeserializeInto, ZcashSerialize},
    transaction::{LockTime, Transaction},
    transparent::{self, Address, Input, OutPoint, Output, Script},
};
use zakura_node_services::rpc_client::RpcRequestClient;
use zakura_rpc::{
    client::{
        BlockProposalResponse, BlockTemplateResponse, BlockTemplateTimeSource,
        GetBlockchainInfoResponse, HexData, SubmitBlockResponse,
    },
    methods::GetBlockHashResponse,
    proposal_block_from_template,
    server::{self, OPENED_RPC_ENDPOINT_MSG},
};
use zakura_state::{
    constants::{min_pruning_retention, MAX_BLOCK_REORG_HEIGHT},
    PruningConfig, StorageMode, TransparentIndexesUnavailable,
};
use zakura_test::args;

use crate::common::{
    config::{os_assigned_rpc_port_config, read_listen_addr_from_logs, testdir},
    launch::{ZakuradTestDirExt, EXTENDED_LAUNCH_DELAY},
};

/// The early NU5 boundary used to cover pre-NU5 and NU5 block submissions.
const NU5_ACTIVATION_HEIGHT: u32 = 5;

/// Number of blocks submitted across the configured NU5 boundary.
const NUM_BLOCKS_TO_SUBMIT: usize = 10;

pub(crate) async fn submit_blocks_test() -> Result<()> {
    let _init_guard = zakura_test::init();

    let net = Network::new_regtest(
        ConfiguredActivationHeights {
            nu5: Some(NU5_ACTIVATION_HEIGHT),
            ..Default::default()
        }
        .into(),
    );
    let mut config = os_assigned_rpc_port_config(false, &net)?;
    config.mempool.debug_enable_at_height = Some(0);

    let mut zakurad = testdir()?
        .with_config(&mut config)?
        .spawn_child(args!["start"])?
        .with_timeout(EXTENDED_LAUNCH_DELAY);

    let rpc_address = read_listen_addr_from_logs(&mut zakurad, OPENED_RPC_ENDPOINT_MSG)?;
    zakurad.expect_stdout_line_matches("activating mempool")?;

    let client = RpcRequestClient::new(rpc_address);

    for _ in 1..=NUM_BLOCKS_TO_SUBMIT {
        let (mut block, height) = client.block_from_template(&net).await?;

        while !net.disable_pow()
            && zakura_consensus::difficulty_is_valid(&block.header, &net, &height, &block.hash())
                .is_err()
        {
            increment_big_endian(Arc::make_mut(&mut block.header).nonce.as_mut());
        }

        client.submit_block(block).await?;
    }

    zakurad.kill(false)?;

    let output = zakurad.wait_with_output()?;
    let output = output.assert_failure()?;

    output
        .assert_was_killed()
        .wrap_err("Possible port conflict. Are there other acceptance tests running?")
}

/// HASH160 of the one-byte redeem script `OP_TRUE`.
///
/// Coinbase outputs paid to this P2SH script can be spent with the unlock script
/// `[0x01, OP_TRUE]`, so the test needs no signing keys.
const OP_TRUE_SCRIPT_HASH: [u8; 20] = [
    0xda, 0x17, 0x45, 0xe9, 0xb5, 0x49, 0xbd, 0x0b, 0xfa, 0x1a, 0x56, 0x99, 0x71, 0xc7, 0x7e, 0xba,
    0x30, 0xcd, 0x5a, 0x4b,
];

/// The ZIP-317 conventional fee for a one-input, one-output transparent transaction.
const SPEND_FEE: i64 = 10_000;

/// The message every transparent index RPC returns on a node without those indexes.
const TRANSPARENT_INDEXES_UNAVAILABLE: &str =
    "transparent address and spender indexes are unavailable";

/// Seed the unchanged v29 chain encoding, then exercise migration through normal node startup.
pub(crate) async fn startup_upgrades_previous_database_test() -> Result<()> {
    let _init_guard = zakura_test::init();
    let net = Network::new_regtest(
        ConfiguredActivationHeights {
            nu5: Some(1),
            ..Default::default()
        }
        .into(),
    );
    let mut config = os_assigned_rpc_port_config(false, &net)?;
    config.state.ephemeral = false;
    config.state.storage_mode = StorageMode::Pruned(PruningConfig {
        tx_retention: min_pruning_retention(&net),
    });
    config.consensus.checkpoint_sync = false;
    let test_dir = testdir()?.with_config(&mut config)?;
    let mut seed_node = test_dir
        .spawn_child(args!["start"])?
        .with_timeout(EXTENDED_LAUNCH_DELAY);
    let seed_rpc = read_listen_addr_from_logs(&mut seed_node, OPENED_RPC_ENDPOINT_MSG)?;
    seed_node.expect_stdout_line_matches("activating mempool")?;
    let saved_block = RpcRequestClient::new(seed_rpc)
        .mine_proposed_block(&net)
        .await?;
    seed_node.kill(false)?;
    let mut output = seed_node.wait_with_output()?.assert_failure()?;
    output.assert_was_killed()?;
    let test_dir = output.dir.take().expect("the saved database is kept");
    let state_config = config.mirrored_state_config();
    let current_version = zakura_state::state_database_format_version_in_code();
    let previous_version = semver::Version::new(29, 1, 0);
    let kind = zakura_state::constants::STATE_DATABASE_KIND;
    let current_path = state_config.db_path(kind, current_version.major, &net);
    let previous_path = state_config.db_path(kind, previous_version.major, &net);
    {
        let mut state = zakura_state::FinalizedState::new(&state_config, &net)?;
        // The mined block is within the reorg window; finalize it to seed a nonzero saved tip.
        state.commit_finalized_direct(
            zakura_state::CheckpointVerifiedBlock::from(Arc::new(saved_block.clone())).into(),
            None,
            None,
            "startup upgrade test",
        )?;
        // v30 changes the version boundary, while retaining v29's chain row encodings.
        zakura_state::write_database_format_version_to_disk(
            &state_config,
            kind,
            current_version.major,
            &previous_version,
            &net,
        )
        .map_err(|error| eyre!("{error}"))?;
    }
    std::fs::create_dir_all(
        previous_path
            .parent()
            .expect("the database path has a version directory"),
    )?;
    std::fs::rename(&current_path, &previous_path)?;

    let mut zakurad = test_dir
        .spawn_child(args!["start"])?
        .with_timeout(EXTENDED_LAUNCH_DELAY);
    zakurad.expect_stdout_line_matches("loaded Zakura state cache.*finalized_tip_height=1")?;
    let rpc_address = read_listen_addr_from_logs(&mut zakurad, OPENED_RPC_ENDPOINT_MSG)?;
    zakurad.expect_stdout_line_matches("activating mempool")?;
    let client = RpcRequestClient::new(rpc_address);
    assert!(
        !previous_path.exists(),
        "startup reuses the previous database"
    );
    assert!(
        current_path.exists(),
        "startup opens the current version path"
    );
    assert_eq!(
        zakura_state::database_format_version_on_disk(
            &state_config,
            kind,
            current_version.major,
            &net,
        )
        .map_err(|error| eyre!("{error}"))?,
        Some(current_version),
        "startup completes the format upgrade before RPC opens"
    );
    assert_eq!(client.blockchain_info().await?.blocks(), Height(1));
    let saved_hash: GetBlockHashResponse = client
        .json_result_from_call("getblockhash", "[1]".to_owned())
        .await
        .map_err(|error| eyre!("{error}"))?;
    assert_eq!(saved_hash.hash(), saved_block.hash());
    let mined = client.mine_proposed_block(&net).await?;
    assert_eq!(mined.header.previous_block_hash, saved_block.hash());
    assert_eq!(client.blockchain_info().await?.blocks(), Height(2));

    zakurad.kill(false)?;
    zakurad
        .wait_with_output()?
        .assert_failure()?
        .assert_was_killed()?;
    Ok(())
}

/// Mines and spends on a pruned, checkpoint-syncing Regtest node, which creates a state database
/// without transparent address and spender indexes.
///
/// An indexed node first finalizes coinbase outputs, then restarts in pruned checkpoint mode
/// using the existing database. The node generates block templates, validates each as a proposal, and commits it with
/// `submitblock`. A mature coinbase output is spent through the mempool and mined, while a
/// double-spend, an immature coinbase spend, and a missing input are rejected. Every block is
/// above the Regtest checkpoint, so these commits are fully verified, and the spending blocks
/// are finalized in the state database. The node then restarts with checkpoint sync disabled,
/// keeps the same database, and mines another spend. Address index RPCs fail with an explicit
/// error throughout, and the offline database is checked after shutdown.
pub(crate) async fn pruned_checkpoint_sync_mining_without_transparent_indexes_test() -> Result<()> {
    let _init_guard = zakura_test::init();

    let net = Network::new_regtest(
        ConfiguredActivationHeights {
            nu5: Some(1),
            ..Default::default()
        }
        .into(),
    );
    let miner = Address::from_script_hash(NetworkKind::Regtest, OP_TRUE_SCRIPT_HASH);
    let maturity = transparent::MIN_TRANSPARENT_COINBASE_MATURITY;

    let mut config = os_assigned_rpc_port_config(false, &net)?;
    config.mempool.debug_enable_at_height = Some(0);
    config.mining.miner_address = Some(miner.to_string().parse()?);
    config.state.ephemeral = false;
    config.state.storage_mode = StorageMode::Pruned(PruningConfig {
        tx_retention: min_pruning_retention(&net),
    });
    // Restore every non-finalized block after the restart below.
    config.state.debug_skip_non_finalized_state_backup_task = true;
    config.consensus.checkpoint_sync = false;

    let mut zakurad = testdir()?
        .with_config(&mut config)?
        .spawn_child(args!["start"])?
        .with_timeout(EXTENDED_LAUNCH_DELAY);
    let cache_dir = config.state.cache_dir.clone();
    let rpc_address = read_listen_addr_from_logs(&mut zakurad, OPENED_RPC_ENDPOINT_MSG)?;
    zakurad.expect_stdout_line_matches("activating mempool")?;
    let client = RpcRequestClient::new(rpc_address);

    // Start with a complete indexed database and finalize at least the height 1 coinbase.
    let mut blocks = Vec::new();
    for _ in 0..maturity + MAX_BLOCK_REORG_HEIGHT {
        blocks.push(client.mine_proposed_block(&net).await?);
    }

    let indexed_tip = client.blockchain_info().await?.blocks();
    zakurad.kill(false)?;
    let mut output = zakurad.wait_with_output()?.assert_failure()?;
    output.assert_was_killed()?;
    let test_dir = output.dir.take().expect("the indexed database is kept");
    tokio::time::sleep(Duration::from_secs(3)).await;
    let first_skipped_height;
    {
        let state = zakura_state::FinalizedState::new(&config.mirrored_state_config(), &net)?;
        assert!(!state.db.skips_transparent_indexes());
        assert!(
            state.db.address_balance(&miner)?.is_some(),
            "the existing database has address index rows"
        );
        first_skipped_height = (state
            .db
            .finalized_tip_height()
            .expect("the indexed database has a finalized tip")
            + 1)
        .expect("the short test chain is below the maximum height");
    }
    config.consensus.checkpoint_sync = true;
    let mut zakurad = test_dir
        .replace_config(&mut config)?
        .spawn_child(args!["start"])?
        .with_timeout(EXTENDED_LAUNCH_DELAY);
    let rpc_address = read_listen_addr_from_logs(&mut zakurad, OPENED_RPC_ENDPOINT_MSG)?;
    zakurad.expect_stdout_line_matches("activating mempool")?;
    let client = RpcRequestClient::new(rpc_address);
    assert_eq!(
        client.blockchain_info().await?.blocks(),
        indexed_tip,
        "switching index writes off keeps the synced node and its backup"
    );

    let first_coinbase = op_true_coinbase_output(&blocks[0], &miner)?;
    let first_spend = spend_op_true_output(first_coinbase.0, first_coinbase.1, &miner)?;
    client.send_raw_transaction(&first_spend).await?;

    let spending_block = client.mine_proposed_block(&net).await?;
    assert!(
        spending_block
            .transactions
            .iter()
            .any(|transaction| transaction.hash() == first_spend.hash()),
        "the mined block includes the mature coinbase spend"
    );
    blocks.push(spending_block);

    // Invalid spends are rejected by the mempool's contextual checks.
    let double_spend = spend_op_true_output(first_coinbase.0, first_coinbase.1, &miner)
        .map(|transaction| with_extra_fee(transaction, 1))?;
    let immature = op_true_coinbase_output(blocks.last().expect("blocks were mined"), &miner)?;
    let immature_spend = spend_op_true_output(immature.0, immature.1, &miner)?;
    let missing_spend = spend_op_true_output(
        OutPoint {
            hash: zakura_chain::transaction::Hash([0xab; 32]),
            index: 0,
        },
        immature.1,
        &miner,
    )?;
    for (description, transaction) in [
        ("double-spend", double_spend),
        ("immature coinbase spend", immature_spend),
        ("missing input", missing_spend),
    ] {
        assert!(
            client.send_raw_transaction(&transaction).await.is_err(),
            "the {description} is rejected"
        );
    }

    // Mine past the reorg window, so the spending block is committed to the finalized state.
    for _ in 0..=MAX_BLOCK_REORG_HEIGHT {
        blocks.push(client.mine_proposed_block(&net).await?);
    }
    assert_transparent_index_rpcs_fail(&client, &miner).await?;

    let tip_before_restart = client.blockchain_info().await?.blocks();

    // Restart with checkpoint sync disabled. The node keeps the database without indexes.
    zakurad.kill(false)?;
    let mut output = zakurad.wait_with_output()?.assert_failure()?;
    output.assert_was_killed()?;
    let test_dir = output.dir.take().expect("the test directory is kept");
    tokio::time::sleep(Duration::from_secs(3)).await;

    config.consensus.checkpoint_sync = false;
    let mut zakurad = test_dir
        .replace_config(&mut config)?
        .spawn_child(args!["start"])?
        .with_timeout(EXTENDED_LAUNCH_DELAY);
    let rpc_address = read_listen_addr_from_logs(&mut zakurad, OPENED_RPC_ENDPOINT_MSG)?;
    zakurad.expect_stdout_line_matches("activating mempool")?;
    let client = RpcRequestClient::new(rpc_address);

    assert_eq!(
        client.blockchain_info().await?.blocks(),
        tip_before_restart,
        "the restarted node keeps its finalized and backed-up blocks"
    );

    let second_coinbase = op_true_coinbase_output(&blocks[1], &miner)?;
    let second_spend = spend_op_true_output(second_coinbase.0, second_coinbase.1, &miner)?;
    client.send_raw_transaction(&second_spend).await?;
    let spending_block = client.mine_proposed_block(&net).await?;
    assert!(
        spending_block
            .transactions
            .iter()
            .any(|transaction| transaction.hash() == second_spend.hash()),
        "the block mined after the restart includes the second spend"
    );
    for _ in 0..=MAX_BLOCK_REORG_HEIGHT {
        client.mine_proposed_block(&net).await?;
    }
    assert_transparent_index_rpcs_fail(&client, &miner).await?;
    let final_tip = client.blockchain_info().await?.blocks();

    zakurad.kill(false)?;
    let mut output = zakurad.wait_with_output()?.assert_failure()?;
    output.assert_was_killed()?;
    let test_dir = output.dir.take().expect("the test directory is kept");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Inspect the database offline.
    let major_version = zakura_state::state_database_format_version_in_code().major;
    let mut state_config = config.mirrored_state_config();
    state_config.cache_dir = cache_dir;
    assert!(state_config
        .db_path(
            zakura_state::constants::STATE_DATABASE_KIND,
            major_version,
            &net
        )
        .exists());

    let (_read_state, db, _sender) = zakura_state::init_read_only(state_config, &net)?;
    assert!(db.skips_transparent_indexes());
    assert_eq!(
        db.transparent_indexes_skipped_from(),
        Some(first_skipped_height)
    );
    assert!(db
        .finalized_tip_height()
        .is_some_and(|height| height.0 + MAX_BLOCK_REORG_HEIGHT >= final_tip.0));
    assert_eq!(
        db.address_balance(&miner),
        Err(TransparentIndexesUnavailable)
    );
    for (spend, spent) in [
        (&first_spend, first_coinbase.0),
        (&second_spend, second_coinbase.0),
    ] {
        assert!(db.utxo(&spent).is_none(), "the spent coinbase is gone");
        assert!(
            db.utxo(&OutPoint {
                hash: spend.hash(),
                index: 0
            })
            .is_some(),
            "the spend output is in the finalized UTXO set"
        );
    }

    drop(db);
    drop(test_dir);

    Ok(())
}

/// Returns the outpoint and value of `block`'s coinbase output to `miner`.
fn op_true_coinbase_output(
    block: &Block,
    miner: &Address,
) -> Result<(OutPoint, Amount<NonNegative>)> {
    let coinbase = block
        .transactions
        .first()
        .ok_or_else(|| eyre!("blocks have a coinbase transaction"))?;
    let (index, output) = coinbase
        .outputs()
        .iter()
        .enumerate()
        .find(|(_, output)| output.lock_script == miner.script())
        .ok_or_else(|| eyre!("the coinbase pays the miner address"))?;

    Ok((
        OutPoint {
            hash: coinbase.hash(),
            index: u32::try_from(index)?,
        },
        output.value(),
    ))
}

/// Returns a v5 transaction that spends an `OP_TRUE` P2SH output back to `miner`.
fn spend_op_true_output(
    outpoint: OutPoint,
    value: Amount<NonNegative>,
    miner: &Address,
) -> Result<Transaction> {
    let value = (value - Amount::<NonNegative>::try_from(SPEND_FEE)?)?.constrain()?;

    Ok(Transaction::V5 {
        network_upgrade: NetworkUpgrade::Nu5,
        lock_time: LockTime::unlocked(),
        expiry_height: Height(0),
        inputs: vec![Input::PrevOut {
            outpoint,
            unlock_script: Script::new(&[0x01, 0x51]),
            sequence: u32::MAX,
        }],
        outputs: vec![Output::new(value, miner.script())],
        sapling_shielded_data: None,
        orchard_shielded_data: None,
    })
}

/// Returns `transaction` with its output reduced by `extra` zatoshis, so it has a different hash.
fn with_extra_fee(mut transaction: Transaction, extra: i64) -> Transaction {
    if let Transaction::V5 { outputs, .. } = &mut transaction {
        let value = (outputs[0].value() - Amount::<NonNegative>::try_from(extra).expect("valid"))
            .expect("the output covers the extra fee")
            .constrain()
            .expect("the output stays non-negative");
        outputs[0] = Output::new(value, outputs[0].lock_script.clone());
    }
    transaction
}

/// Checks that every transparent address index RPC returns the explicit unavailable error.
async fn assert_transparent_index_rpcs_fail(
    client: &RpcRequestClient,
    miner: &Address,
) -> Result<()> {
    let misc_error: i32 = server::error::LegacyCode::Misc.into();
    let addresses = format!(r#"[{{"addresses": ["{miner}"]}}]"#);

    for method in ["getaddressbalance", "getaddressutxos", "getaddresstxids"] {
        let response: serde_json::Value =
            serde_json::from_str(&client.text_from_call(method, addresses.clone()).await?)?;
        let error = response
            .get("error")
            .filter(|error| !error.is_null())
            .ok_or_else(|| eyre!("{method} returns a JSON-RPC error: {response}"))?;

        assert_eq!(
            error.get("code").and_then(serde_json::Value::as_i64),
            Some(i64::from(misc_error)),
            "{method} error code"
        );
        let message = error
            .get("message")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        assert!(
            message.contains(TRANSPARENT_INDEXES_UNAVAILABLE),
            "{method} explains that the indexes are unavailable: {message}"
        );
    }

    Ok(())
}

#[allow(dead_code)]
pub trait MiningRpcMethods {
    async fn block_from_template(&self, net: &Network) -> Result<(Block, Height)>;
    async fn submit_block(&self, block: Block) -> Result<()>;
    async fn get_block(&self, height: i32) -> Result<Option<Arc<Block>>, BoxError>;
    async fn generate(&self, num_blocks: u32) -> Result<Vec<block::Hash>>;
    async fn blockchain_info(&self) -> Result<GetBlockchainInfoResponse>;
    async fn mine_proposed_block(&self, net: &Network) -> Result<Block>;
    async fn send_raw_transaction(&self, transaction: &Transaction) -> Result<()>;
}

impl MiningRpcMethods for RpcRequestClient {
    async fn block_from_template(&self, net: &Network) -> Result<(Block, Height)> {
        let block_template: BlockTemplateResponse = self
            .json_result_from_call("getblocktemplate", "[]".to_string())
            .await
            .expect("response should be success output with a serialized `GetBlockTemplate`");

        Ok((
            proposal_block_from_template(&block_template, BlockTemplateTimeSource::default(), net)?,
            Height(block_template.height()),
        ))
    }

    async fn submit_block(&self, block: Block) -> Result<()> {
        let block_data = hex::encode(block.zcash_serialize_to_vec()?);

        let submit_block_response: SubmitBlockResponse = self
            .json_result_from_call("submitblock", format!(r#"["{block_data}"]"#))
            .await
            .map_err(|err| eyre!(err))?;

        match submit_block_response {
            SubmitBlockResponse::Accepted => Ok(()),
            SubmitBlockResponse::ErrorResponse(err) => {
                Err(eyre!("block submission failed: {err:?}"))
            }
        }
    }

    async fn get_block(&self, height: i32) -> Result<Option<Arc<Block>>, BoxError> {
        match self
            .json_result_from_call("getblock", format!(r#"["{height}", 0]"#))
            .await
        {
            Ok(HexData(raw_block)) => {
                let block = raw_block.zcash_deserialize_into::<Block>()?;
                Ok(Some(Arc::new(block)))
            }
            Err(err)
                if err
                    .downcast_ref::<jsonrpsee_types::ErrorObject>()
                    .is_some_and(|err| {
                        let error: i32 = server::error::LegacyCode::InvalidParameter.into();
                        err.code() == error
                    }) =>
            {
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }

    async fn generate(&self, num_blocks: u32) -> Result<Vec<block::Hash>> {
        self.json_result_from_call("generate", format!("[{num_blocks}]"))
            .await
            .map(|response: Vec<GetBlockHashResponse>| {
                response.into_iter().map(|rsp| rsp.hash()).collect()
            })
            .map_err(|err| eyre!(err))
    }

    async fn blockchain_info(&self) -> Result<GetBlockchainInfoResponse> {
        self.json_result_from_call("getblockchaininfo", "[]")
            .await
            .map_err(|err| eyre!(err))
    }

    /// Gets a block template, checks it with `getblocktemplate` proposal mode, then submits it.
    async fn mine_proposed_block(&self, net: &Network) -> Result<Block> {
        let (block, _height) = self.block_from_template(net).await?;
        let block_data = hex::encode(block.zcash_serialize_to_vec()?);

        let proposal: BlockProposalResponse = self
            .json_result_from_call(
                "getblocktemplate",
                format!(r#"[{{"mode":"proposal","data":"{block_data}"}}]"#),
            )
            .await
            .map_err(|err| eyre!(err))?;
        if proposal != BlockProposalResponse::Valid {
            return Err(eyre!("block proposal was rejected: {proposal:?}"));
        }

        self.submit_block(block.clone()).await?;

        Ok(block)
    }

    async fn send_raw_transaction(&self, transaction: &Transaction) -> Result<()> {
        let transaction_data = hex::encode(transaction.zcash_serialize_to_vec()?);

        self.json_result_from_call::<serde_json::Value>(
            "sendrawtransaction",
            format!(r#"["{transaction_data}"]"#),
        )
        .await
        .map(|_| ())
        .map_err(|err| eyre!(err))
    }
}
