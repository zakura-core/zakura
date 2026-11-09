//! Test submitblock RPC method on Regtest.
//!
//! This test will get block templates via the `getblocktemplate` RPC method and submit them as new blocks
//! via the `submitblock` RPC method on Regtest.

use std::{sync::Arc, time::Duration};

use eyre::{eyre, Context, Result};
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
use zakura_node_services::rpc_client::{RpcError, RpcRequestClient};
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
                if err.downcast_ref::<RpcError>().is_some_and(|err| {
                    let error: i32 = server::error::LegacyCode::InvalidParameter.into();
                    err.code == error
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

/// Exercises actual RPC admission, template selection, and mining across local PQ activation.
#[cfg(feature = "pq")]
pub(crate) async fn pq_mempool_test() -> Result<()> {
    pq_spend_test(PqSpendScenario::Mempool).await
}

/// Sends PQ authorizations over legacy P2P, then verifies the peer's mined block and RPC bytes.
#[cfg(feature = "pq")]
pub(crate) async fn pq_p2p_test() -> Result<()> {
    pq_spend_test(PqSpendScenario::Peer).await
}

/// Revalidates PQ spending after a rollback below activation and an alternate chain.
#[cfg(feature = "pq")]
pub(crate) async fn pq_activation_reorg_test() -> Result<()> {
    pq_spend_test(PqSpendScenario::Reorg).await
}

#[cfg(feature = "pq")]
enum PqSpendScenario {
    Mempool,
    Peer,
    Reorg,
}

#[cfg(feature = "pq")]
enum PqTestKey {
    Fast(zcash_transparent::pq::sha2_128f::SigningKey),
    Small(zcash_transparent::pq::sha2_192s::SigningKey),
}

#[cfg(feature = "pq")]
impl PqTestKey {
    fn new(parameters: zcash_transparent::pq::ParameterSet, seed: u8) -> Self {
        use zcash_transparent::pq::{self, ParameterSet};
        match parameters {
            ParameterSet::Sha2_128f => Self::Fast(pq::sha2_128f::SigningKey::from_seed(
                &[seed; pq::sha2_128f::SEED_BYTES],
            )),
            ParameterSet::Sha2_192s => Self::Small(pq::sha2_192s::SigningKey::from_seed(
                &[seed; pq::sha2_192s::SEED_BYTES],
            )),
        }
    }
    fn public_key(&self) -> Vec<u8> {
        match self {
            Self::Fast(key) => key.public_key().to_vec(),
            Self::Small(key) => key.public_key().to_vec(),
        }
    }
    fn sign(&self, message: &[u8], context: &[u8]) -> Vec<u8> {
        match self {
            Self::Fast(key) => key.sign(message, context).unwrap(),
            Self::Small(key) => key.sign(message, context).unwrap(),
        }
    }
}

/// What one funded output is, and what a spend of it reveals and runs.
#[cfg(feature = "pq")]
enum PqFundedKind {
    /// A post-quantum output, which commits to the script its spend reveals.
    Pq {
        parameters: zcash_transparent::pq::ParameterSet,
        body: zcash_script::script::Code,
    },
    /// An ordinary pay-to-script-hash output, so every fixture spends a legacy ECDSA
    /// input beside the post-quantum ones.
    Legacy { redeem: zcash_script::script::Code },
    /// A script in the post-quantum namespace that no rule defines. A block below the
    /// activation height may still create it, and nothing can spend it afterwards.
    Restricted {
        script: Vec<u8>,
        revealed: zcash_script::script::Code,
    },
}

/// One funded output of the fixture that all three scenarios share.
#[cfg(feature = "pq")]
struct PqFunded {
    kind: PqFundedKind,
    /// Which of this scheme's keys sign, in the order the revealed script expects. More
    /// than one signer means a multisig script, whose input also carries the dummy
    /// element legacy `OP_CHECKMULTISIG` consumes.
    signers: Vec<usize>,
}

#[cfg(feature = "pq")]
impl PqFunded {
    /// The `scriptPubKey` the funding transaction pays to.
    fn lock_script(&self) -> Script {
        match &self.kind {
            PqFundedKind::Pq { parameters, body } => Script::new(
                &zcash_transparent::pq::output::output_script(*parameters, body)
                    .expect("the fixture bodies are within the script size bound")
                    .0,
            ),
            PqFundedKind::Legacy { redeem } => Address::from_script_hash(
                NetworkKind::Regtest,
                zcash_transparent::util::hash160::hash(&redeem.0),
            )
            .script(),
            PqFundedKind::Restricted { script, .. } => Script::new(script),
        }
    }

    /// The script a spend reveals, which is what it runs and what its signatures cover.
    fn revealed(&self) -> &zcash_script::script::Code {
        match &self.kind {
            PqFundedKind::Pq { body, .. } => body,
            PqFundedKind::Legacy { redeem } => redeem,
            PqFundedKind::Restricted { revealed, .. } => revealed,
        }
    }

    /// The scheme a post-quantum spend of this output signs under, and `None` for the
    /// legacy output. The restricted shape is signed as though its scheme were defined,
    /// so that rejecting it is the output's doing rather than a missing signature.
    fn parameters(&self) -> Option<zcash_transparent::pq::ParameterSet> {
        match &self.kind {
            PqFundedKind::Pq { parameters, .. } => Some(*parameters),
            PqFundedKind::Restricted { .. } => Some(zcash_transparent::pq::ParameterSet::Sha2_128f),
            PqFundedKind::Legacy { .. } => None,
        }
    }

    /// One signature-sized placeholder per signer, so the serialized size, and with it the
    /// fee, is known before anything is signed.
    fn placeholders(&self) -> Vec<Vec<u8>> {
        // A legacy DER signature with its hash-type byte is at most seventy-three bytes.
        let bytes = match self.parameters() {
            Some(parameters) => parameters.signature_bytes() + 1,
            None => 73,
        };
        vec![vec![0; bytes]; self.signers.len()]
    }

    /// The complete argument list an input carries, ahead of the revealed script.
    fn arguments(&self, signatures: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
        if self.signers.len() > 1 {
            let mut arguments = vec![Vec::new()];
            arguments.extend(signatures);
            arguments
        } else {
            signatures
        }
    }
}

/// Which of the fixture's two key sets belongs to this scheme.
#[cfg(feature = "pq")]
fn pq_scheme_index(parameters: zcash_transparent::pq::ParameterSet) -> usize {
    usize::from(matches!(
        parameters,
        zcash_transparent::pq::ParameterSet::Sha2_192s
    ))
}

/// The fixture entries whose outputs only the post-quantum rules can spend.
#[cfg(feature = "pq")]
const PQ_SPENDS: [usize; 5] = [0, 1, 4, 5, 6];

/// Every fixture entry with a spend of its own, which adds the legacy input to those.
#[cfg(feature = "pq")]
const ALL_SPENDS: [usize; 6] = [0, 1, 3, 4, 5, 6];

/// Shares real signed fixtures across admission, propagation, and activation reorganization.
// Both nodes' logs are printed, so a failure here can be read from the test output.
#[allow(clippy::print_stdout)]
#[cfg(feature = "pq")]
async fn pq_spend_test(scenario: PqSpendScenario) -> Result<()> {
    use zakura_chain::{parameters::testnet::RegtestParameters, transaction::HashType};
    use zcash_protocol::consensus::Parameters;
    use zcash_script::script::Code;
    use zcash_transparent::pq::{output, single_key, ParameterSet};

    /// Pushes `bytes` with the shortest opcode that can carry them, which is the only
    /// encoding a post-quantum input may use.
    fn push(bytes: &[u8]) -> Vec<u8> {
        let mut encoded = match bytes.len() {
            0 => vec![0x00],
            length @ 1..=0x4b => vec![u8::try_from(length).unwrap()],
            length @ 0x4c..=0xff => vec![0x4c, u8::try_from(length).unwrap()],
            length => {
                let mut prefix = vec![0x4d];
                prefix.extend(u16::try_from(length).unwrap().to_le_bytes());
                prefix
            }
        };
        encoded.extend(bytes);
        encoded
    }
    /// A script requiring `required` of these public keys, in the legacy multisig shape.
    fn multisig_body(keys: &[Vec<u8>], required: u8) -> Code {
        let number = |value: u8| {
            assert!(value <= 16, "the fixtures stay within the small opcodes");
            vec![0x50 + value]
        };
        let mut bytes = number(required);
        for key in keys {
            bytes.extend(push(key));
        }
        bytes.extend(number(u8::try_from(keys.len()).unwrap()));
        bytes.push(0xae);
        Code(bytes)
    }
    fn transaction(outpoint: OutPoint, value: Amount<NonNegative>, output: Script) -> Transaction {
        Transaction::V5 {
            network_upgrade: NetworkUpgrade::Nu5,
            lock_time: LockTime::unlocked(),
            expiry_height: Height(0),
            inputs: vec![Input::PrevOut {
                outpoint,
                unlock_script: Script::new(&[]),
                sequence: u32::MAX,
            }],
            outputs: vec![Output::new(value, output)],
            sapling_shielded_data: None,
            orchard_shielded_data: None,
        }
    }
    fn install(tx: &mut Transaction, index: usize, arguments: &[Vec<u8>], revealed: &Code) {
        let mut script = Vec::new();
        for argument in arguments {
            script.extend(push(argument));
        }
        script.extend(push(&revealed.0));
        if let Transaction::V5 { inputs, .. } = tx {
            if let Input::PrevOut { unlock_script, .. } = &mut inputs[index] {
                *unlock_script = Script::new(&script);
            }
        }
    }

    let _guard = zakura_test::init();
    let maturity = transparent::MIN_TRANSPARENT_COINBASE_MATURITY;
    let activation = Height(maturity + 3);
    let net = Network::new_regtest(RegtestParameters {
        pq_activation_height: Some(activation),
        activation_heights: ConfiguredActivationHeights {
            nu5: Some(1),
            ..Default::default()
        },
        ..Default::default()
    });
    let miner = Address::from_script_hash(NetworkKind::Regtest, OP_TRUE_SCRIPT_HASH);
    let schemes = [ParameterSet::Sha2_128f, ParameterSet::Sha2_192s];
    let keys: Vec<Vec<PqTestKey>> = schemes
        .iter()
        .map(|&set| (1..=3).map(|seed| PqTestKey::new(set, seed)).collect())
        .collect();
    let context: Vec<Vec<u8>> = schemes
        .iter()
        .map(|&set| output::signing_context(net.network_type(), set).to_vec())
        .collect();

    let single = |set: ParameterSet| -> Result<PqFunded> {
        let index = pq_scheme_index(set);
        Ok(PqFunded {
            kind: PqFundedKind::Pq {
                parameters: set,
                body: single_key::body(set, &keys[index][0].public_key())?,
            },
            signers: vec![0],
        })
    };
    let two_of_three = |set: ParameterSet| -> PqFunded {
        let index = pq_scheme_index(set);
        let public: Vec<Vec<u8>> = keys[index].iter().map(PqTestKey::public_key).collect();
        PqFunded {
            kind: PqFundedKind::Pq {
                parameters: set,
                body: multisig_body(&public, 2),
            },
            // The second signature comes from the last key, so matching also has to skip one.
            signers: vec![0, 2],
        }
    };
    let secret = secp256k1::SecretKey::from_secret_bytes([4; 32])?;
    let public = secp256k1::PublicKey::from_secret_key(&secret).serialize();
    let mut legacy = push(&public);
    legacy.push(0xac);
    // A scheme byte no parameter set defines, which puts this script inside the namespace
    // and outside everything the format gives rules for.
    let mut restricted = vec![0x00, 0x60, 0x20];
    restricted.extend([0x11; 32]);
    let fifteen: Vec<Vec<u8>> = vec![keys[0][0].public_key(); 15];
    let large = PqFunded {
        kind: PqFundedKind::Pq {
            parameters: schemes[0],
            body: multisig_body(&fifteen, 15),
        },
        signers: vec![0; 15],
    };
    let fixture = vec![
        single(schemes[0])?,
        two_of_three(schemes[0]),
        PqFunded {
            kind: PqFundedKind::Restricted {
                script: restricted,
                revealed: single_key::body(schemes[0], &keys[0][0].public_key())?,
            },
            signers: vec![0],
        },
        PqFunded {
            kind: PqFundedKind::Legacy {
                redeem: Code(legacy),
            },
            signers: vec![0],
        },
        single(schemes[1])?,
        two_of_three(schemes[1]),
        single(schemes[0])?,
        single(schemes[1])?,
        large,
    ];
    let mut config = os_assigned_rpc_port_config(false, &net)?;
    config.mempool.debug_enable_at_height = Some(0);
    config.mining.miner_address = Some(miner.to_string().parse()?);
    config.consensus.checkpoint_sync = false;
    config.network.initial_testnet_peers.clear();
    config.network.cache_dir = false.into();
    // Writing and reading TOML is part of starting this real node.
    let mut node = testdir()?
        .with_config(&mut config)?
        .spawn_child(args!["start"])?
        .with_timeout(EXTENDED_LAUNCH_DELAY);
    let peer_address = read_listen_addr_from_logs(&mut node, "Opened Zcash protocol endpoint at")?;
    let rpc = read_listen_addr_from_logs(&mut node, OPENED_RPC_ENDPOINT_MSG)?;
    node.expect_stdout_line_matches("activating mempool")?;
    // Keep child pipes drained while RPC/P2P work runs; a full log pipe can stall a node.
    let mut log_readers = Vec::new();
    for output in [node.stdout.take(), node.stderr.take()]
        .into_iter()
        .flatten()
    {
        log_readers.push(std::thread::spawn(move || {
            for line in output {
                println!("node A: {}", line.expect("read node log"));
            }
        }));
    }
    let client = RpcRequestClient::new(rpc);
    let mut matured = Vec::new();
    for _ in 0..maturity {
        matured.push(client.mine_proposed_block(&net).await?);
    }
    let (outpoint, value) = op_true_coinbase_output(&matured[0], &miner)?;
    let funded_value: Amount<NonNegative> = 10_000_000u64.try_into()?;
    let mut funding = spend_op_true_output(outpoint, value, &miner)?;
    if let Transaction::V5 { outputs, .. } = &mut funding {
        outputs.clear();
        for entry in &fixture {
            outputs.push(Output::new(funded_value, entry.lock_script()));
        }
        outputs.push(Output::new(Amount::zero(), miner.script()));
    }
    let funding_fee = zakura_chain::transaction::zip317::conventional_fee(&funding);
    let change: Amount<NonNegative> = (i64::from(value)
        - i64::from(funded_value) * i64::try_from(fixture.len())?
        - i64::from(funding_fee))
    .try_into()?;
    if let Transaction::V5 { outputs, .. } = &mut funding {
        let last = outputs.len() - 1;
        outputs[last] = Output::new(change, miner.script());
    }
    // A payment to a post-quantum output is not relayed before the rules activate, so the
    // fixture's own funding has to reach the chain in a block rather than through the
    // mempool. A block below the activation height may carry it, including the one shape
    // no rules define.
    assert!(
        client.send_raw_transaction(&funding).await.is_err(),
        "paying to a post-quantum output is non-standard before activation"
    );
    let block = pq_mine_block_including(&client, &net, &[funding.clone()], funding_fee).await?;
    assert!(block
        .transactions
        .iter()
        .any(|tx| tx.hash() == funding.hash()));

    let mut spends = Vec::new();
    for index in 0..7 {
        // Entry seven is spent as the second input of entry six's transaction, so one
        // fixture also covers two post-quantum inputs of different schemes side by side.
        let paired = (index == 6).then_some(7);
        let inputs: Vec<usize> = std::iter::once(index).chain(paired).collect();
        let previous: Vec<Output> = inputs
            .iter()
            .map(|&entry| Output::new(funded_value, fixture[entry].lock_script()))
            .collect();
        let mut tx = transaction(
            OutPoint {
                hash: funding.hash(),
                index: u32::try_from(index)?,
            },
            funded_value,
            miner.script(),
        );
        if let Some(second) = paired {
            if let Transaction::V5 {
                inputs, outputs, ..
            } = &mut tx
            {
                inputs.push(Input::PrevOut {
                    outpoint: OutPoint {
                        hash: funding.hash(),
                        index: u32::try_from(second)?,
                    },
                    unlock_script: Script::new(&[]),
                    sequence: u32::MAX,
                });
                outputs[0] = Output::new((funded_value + funded_value)?, miner.script());
            }
        }
        // Exact-length placeholders establish serialized size before fee calculation/signing.
        for (input_index, &entry) in inputs.iter().enumerate() {
            let entry = &fixture[entry];
            install(
                &mut tx,
                input_index,
                &entry.arguments(entry.placeholders()),
                entry.revealed(),
            );
        }
        let fee = zakura_chain::transaction::zip317::conventional_fee(&tx);
        if let Transaction::V5 { outputs, .. } = &mut tx {
            outputs[0] = Output::new((outputs[0].value() - fee)?.constrain()?, miner.script());
        }
        let hasher = tx.sighasher(NetworkUpgrade::Nu5, Arc::new(previous.clone()))?;
        for (input_index, &entry) in inputs.iter().enumerate() {
            let entry = &fixture[entry];
            let digest = hasher.sighash(
                HashType::ALL,
                Some((input_index, entry.revealed().0.clone())),
            );
            let signatures = match entry.parameters() {
                None => {
                    let mut signature = secret
                        .sign_ecdsa(secp256k1::Message::from_digest(digest.0))
                        .serialize_der()
                        .to_vec();
                    signature.push(1);
                    vec![signature]
                }
                Some(parameters) => {
                    let scheme = pq_scheme_index(parameters);
                    entry
                        .signers
                        .iter()
                        .map(|&signer| {
                            let mut signature =
                                keys[scheme][signer].sign(&digest.0, &context[scheme]);
                            signature.push(1);
                            signature
                        })
                        .collect()
                }
            };
            install(
                &mut tx,
                input_index,
                &entry.arguments(signatures),
                entry.revealed(),
            );
        }
        assert_eq!(
            zakura_chain::transaction::zip317::conventional_fee(&tx),
            fee,
            "final encoding retains the fee estimate"
        );
        spends.push(tx);
    }
    for index in PQ_SPENDS {
        assert!(
            client.send_raw_transaction(&spends[index]).await.is_err(),
            "both post-quantum schemes are refused before activation"
        );
    }
    client.mine_proposed_block(&net).await?;
    assert_eq!(
        client.blockchain_info().await?.blocks(),
        Height(activation.0 - 1),
        "admission checks the next block height"
    );
    // A valid fifteen-signature spend is accepted by consensus and refused by relay, which
    // caps how large a transaction it will forward.
    let oversized_entry = &fixture[8];
    let mut oversized = transaction(
        OutPoint {
            hash: funding.hash(),
            index: 8,
        },
        funded_value,
        miner.script(),
    );
    install(
        &mut oversized,
        0,
        &oversized_entry.arguments(oversized_entry.placeholders()),
        oversized_entry.revealed(),
    );
    let large_fee = zakura_chain::transaction::zip317::conventional_fee(&oversized);
    if let Transaction::V5 { outputs, .. } = &mut oversized {
        outputs[0] = Output::new((funded_value - large_fee)?.constrain()?, miner.script());
    }
    let previous = Output::new(funded_value, oversized_entry.lock_script());
    let digest = oversized
        .sighasher(NetworkUpgrade::Nu5, Arc::new(vec![previous.clone()]))?
        .sighash(
            HashType::ALL,
            Some((0, oversized_entry.revealed().0.clone())),
        );
    let mut signature = keys[0][0].sign(&digest.0, &context[0]);
    signature.push(1);
    install(
        &mut oversized,
        0,
        &oversized_entry.arguments(vec![signature; 15]),
        oversized_entry.revealed(),
    );
    assert!(oversized.zcash_serialize_to_vec()?.len() > 250_000);
    zakura_script::CachedFfiTransaction::new_for_network(
        Arc::new(oversized.clone()),
        Arc::new(vec![previous]),
        &net,
        activation,
    )?
    .is_valid(0)?;
    let error = client
        .send_raw_transaction(&oversized)
        .await
        .expect_err("valid fifteen-signature 128f spend exceeds default relay size");
    assert!(
        error.to_string().contains("configured mempool maximum"),
        "relay rejects the size, not the valid signatures: {error}"
    );
    let mut forged = spends[0].clone();
    if let Transaction::V5 { inputs, .. } = &mut forged {
        if let Input::PrevOut { unlock_script, .. } = &mut inputs[0] {
            let mut bytes = unlock_script.as_raw_bytes().to_vec();
            bytes[10] ^= 1;
            *unlock_script = Script::new(&bytes);
        }
    }
    assert!(
        client.send_raw_transaction(&forged).await.is_err(),
        "forged signature is rejected"
    );
    assert!(
        client.send_raw_transaction(&spends[2]).await.is_err(),
        "an output no rules define cannot be spent"
    );
    // Node B starts with an empty database and learns the funding chain only from A.
    // No funding block or spending transaction is submitted to B through RPC.
    let mut peer = if matches!(scenario, PqSpendScenario::Peer) {
        let mut peer_config = config.clone();
        peer_config.network.initial_testnet_peers = [peer_address.to_string()].into();
        let mut peer_node = testdir()?
            .with_config(&mut peer_config)?
            .spawn_child(args!["start"])?
            .with_timeout(EXTENDED_LAUNCH_DELAY);
        let peer_rpc = read_listen_addr_from_logs(&mut peer_node, OPENED_RPC_ENDPOINT_MSG)?;
        peer_node.expect_stdout_line_matches("activating mempool")?;
        for output in [peer_node.stdout.take(), peer_node.stderr.take()]
            .into_iter()
            .flatten()
        {
            log_readers.push(std::thread::spawn(move || {
                for line in output {
                    println!("node B: {}", line.expect("read peer log"));
                }
            }));
        }
        let peer_client = RpcRequestClient::new_with_timeout(peer_rpc, Duration::from_secs(10));
        let tip: String = client
            .json_result_from_call("getbestblockhash", "[]")
            .await
            .map_err(|e| eyre!(e))?;
        wait_for_pq_tip(&peer_client, tip.parse()?).await?;
        let mempool: Vec<String> = peer_client
            .json_result_from_call("getrawmempool", "[]")
            .await
            .map_err(|e| eyre!(e))?;
        assert!(mempool.is_empty(), "the peer starts without the PQ spends");
        Some((peer_node, peer_client))
    } else {
        None
    };

    for index in ALL_SPENDS {
        let mempool: Vec<String> = client
            .json_result_from_call("getrawmempool", "[]")
            .await
            .map_err(|e| eyre!(e))?;
        if mempool.contains(&spends[index].hash().to_string()) {
            // The RPC retry queue can resubmit pre-activation transactions after the tip changes.
            assert_pq_rpc_bytes(&client, &spends[index]).await?;
        } else {
            client.send_raw_transaction(&spends[index]).await?;
        }
        if let Some((_, peer_client)) = &peer {
            // The node caps concurrent downloads from one peer at five. Await each
            // admission so this fixture tests relay rather than overflowing that queue.
            let expected = spends[index].hash().to_string();
            let mut last_mempool = Vec::new();
            // Allow a complete 73-second periodic mempool crawl in this two-node fixture.
            tokio::time::timeout(Duration::from_secs(90), async {
                loop {
                    let mempool: Vec<String> = peer_client
                        .json_result_from_call("getrawmempool", "[]")
                        .await
                        .map_err(|e| eyre!(e))?;
                    if mempool.contains(&expected) {
                        break Ok::<_, eyre::Report>(());
                    }
                    last_mempool = mempool;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
            .await
            .wrap_err_with(|| format!("node B did not receive spend {index} ({expected}) through P2P; mempool: {last_mempool:?}"))??;
        }
    }
    if matches!(scenario, PqSpendScenario::Mempool) {
        // Once the rules apply, a payment to a defined post-quantum output is an ordinary
        // relayable transaction, and a payment to a shape no rules define is refused at
        // the moment it is made rather than when someone tries to spend it.
        let (outpoint, value) = op_true_coinbase_output(&matured[1], &miner)?;
        let payment = spend_op_true_output(outpoint, value, &miner)?;
        let mut defined = payment.clone();
        if let Transaction::V5 { outputs, .. } = &mut defined {
            outputs[0] = Output::new(outputs[0].value(), fixture[0].lock_script());
        }
        client.send_raw_transaction(&defined).await?;
        let mut undefined = payment;
        if let Transaction::V5 { outputs, .. } = &mut undefined {
            outputs[0] = Output::new(outputs[0].value(), fixture[2].lock_script());
        }
        let error = client
            .send_raw_transaction(&undefined)
            .await
            .expect_err("a payment to a restricted output is refused after activation");
        assert!(
            error
                .to_string()
                .contains("nothing defines how to spend it"),
            "the refusal explains that no rules define the output: {error}"
        );
    }
    let mempool: Vec<String> = client
        .json_result_from_call("getrawmempool", "[]")
        .await
        .map_err(|e| eyre!(e))?;
    let (template, _) = client.block_from_template(&net).await?;
    for index in ALL_SPENDS {
        let tx = &spends[index];
        assert!(mempool.contains(&tx.hash().to_string()));
        assert!(template
            .transactions
            .iter()
            .any(|candidate| candidate.hash() == tx.hash()));
    }
    let mined = if let Some((_, peer_client)) = &peer {
        let (template, _) = peer_client.block_from_template(&net).await?;
        for index in ALL_SPENDS {
            assert!(
                template
                    .transactions
                    .iter()
                    .any(|tx| tx.hash() == spends[index].hash()),
                "the peer selects the relayed spend for mining"
            );
            assert_pq_rpc_bytes(peer_client, &spends[index]).await?;
        }
        let mined = peer_client.mine_proposed_block(&net).await?;
        wait_for_pq_tip(&client, mined.hash()).await?;
        let received = client
            .get_block(i32::try_from(activation.0)?)
            .await
            .map_err(|e| eyre!(e))?
            .expect("node A accepted the peer's block");
        assert_eq!(
            received.zcash_serialize_to_vec()?,
            mined.zcash_serialize_to_vec()?,
            "block propagation preserves authorization bytes"
        );
        for index in ALL_SPENDS {
            assert_pq_rpc_bytes(&client, &spends[index]).await?;
            assert_pq_rpc_bytes(peer_client, &spends[index]).await?;
        }
        for rpc in [&client, peer_client] {
            // State commitment and the mempool's tip notification run independently.
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let mempool: Vec<String> = rpc
                        .json_result_from_call("getrawmempool", "[]")
                        .await
                        .map_err(|e| eyre!(e))?;
                    if mempool.is_empty() {
                        return Ok::<_, eyre::Report>(());
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
            .await
            .wrap_err("mined spends did not leave a node's mempool")??;
        }
        mined
    } else if matches!(scenario, PqSpendScenario::Reorg) {
        pq_reorg_spends(&client, &net, &spends, activation, block.hash()).await?
    } else {
        client.mine_proposed_block(&net).await?
    };
    let expected_mined: &[usize] = if matches!(scenario, PqSpendScenario::Reorg) {
        &PQ_SPENDS
    } else {
        &ALL_SPENDS
    };
    for &index in expected_mined {
        assert!(mined
            .transactions
            .iter()
            .any(|tx| tx.hash() == spends[index].hash()));
    }
    if let Some((peer_node, _)) = peer.take() {
        let mut peer_node = peer_node;
        peer_node.kill(false)?;
        peer_node
            .wait_with_output()?
            .assert_failure()?
            .assert_was_killed()?;
    }
    node.kill(false)?;
    node.wait_with_output()?
        .assert_failure()?
        .assert_was_killed()?;
    for reader in log_readers {
        reader.join().expect("node log reader finished");
    }
    Ok(())
}

/// Mines a block that carries `extra`, without offering those transactions to the mempool.
///
/// Relay policy refuses a payment to a post-quantum output before the rules activate, so a
/// fixture that needs one in the chain below that height puts it in a block directly. The
/// coinbase claims the fees those transactions pay, keeping the block's reward exact.
#[cfg(feature = "pq")]
async fn pq_mine_block_including(
    client: &RpcRequestClient,
    network: &Network,
    extra: &[Transaction],
    fees: Amount<NonNegative>,
) -> Result<Block> {
    use zakura_chain::{
        block::{merkle::AuthDataRoot, ChainHistoryBlockTxAuthCommitmentHash},
        serialization::BytesInDisplayOrder,
    };

    let template: BlockTemplateResponse = client
        .json_result_from_call("getblocktemplate", "[]")
        .await
        .map_err(|e| eyre!(e))?;
    let mut block =
        proposal_block_from_template(&template, BlockTemplateTimeSource::default(), network)?;
    for transaction in extra {
        block.transactions.push(Arc::new(transaction.clone()));
    }
    if let Transaction::V5 { outputs, .. } = Arc::make_mut(&mut block.transactions[0]) {
        outputs[0] = Output::new((outputs[0].value() + fees)?, outputs[0].lock_script.clone());
    }
    let auth_root: AuthDataRoot = block.transactions.iter().collect();
    let header = Arc::make_mut(&mut block.header);
    header.merkle_root = block.transactions.iter().collect();
    header.commitment_bytes = ChainHistoryBlockTxAuthCommitmentHash::from_commitments(
        &template.default_roots().chain_history_root(),
        &auth_root,
    )
    .bytes_in_serialized_order()
    .into();
    client.submit_block(block.clone()).await?;
    wait_for_pq_tip(client, block.hash()).await?;
    Ok(block)
}

/// Waits for an exact peer tip, including semantic block verification and state commitment.
#[cfg(feature = "pq")]
async fn wait_for_pq_tip(client: &RpcRequestClient, expected: block::Hash) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            let tip: String = client
                .json_result_from_call("getbestblockhash", "[]")
                .await
                .map_err(|e| eyre!(e))?;
            if tip == expected.to_string() {
                return Ok::<_, eyre::Report>(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .wrap_err("peer did not accept the expected chain tip")?
}

/// Checks the full serialized transaction, because its v5 transaction ID excludes authorization.
#[cfg(feature = "pq")]
async fn assert_pq_rpc_bytes(client: &RpcRequestClient, transaction: &Transaction) -> Result<()> {
    let raw: HexData = client
        .json_result_from_call(
            "getrawtransaction",
            format!(r#"["{}", 0]"#, transaction.hash()),
        )
        .await
        .map_err(|e| eyre!(e))?;
    assert_eq!(
        raw.0,
        transaction.zcash_serialize_to_vec()?,
        "RPC preserves complete authorization bytes"
    );
    let decoded: Transaction = raw.0.as_slice().zcash_deserialize_into()?;
    assert_eq!(decoded, *transaction);
    Ok(())
}

/// Rolls back mined authorizations, refuses them below activation, then mines an alternate chain.
#[cfg(feature = "pq")]
async fn pq_reorg_spends(
    client: &RpcRequestClient,
    network: &Network,
    spends: &[Transaction],
    activation: Height,
    funding_tip: block::Hash,
) -> Result<Block> {
    use zakura_chain::{
        block::{merkle::AuthDataRoot, ChainHistoryBlockTxAuthCommitmentHash},
        serialization::BytesInDisplayOrder,
    };

    let old_block = client.mine_proposed_block(network).await?;
    assert_eq!(old_block.coinbase_height(), Some(activation));
    for index in ALL_SPENDS {
        assert!(old_block
            .transactions
            .iter()
            .any(|tx| tx.hash() == spends[index].hash()));
    }
    let fork_parent_height = i32::try_from(activation.0 - 1)?;
    let old_parent = client
        .get_block(fork_parent_height)
        .await
        .map_err(|e| eyre!(e))?
        .expect("activation block has a parent");
    let params = serde_json::to_string(&vec![old_parent.hash().to_string()])?;
    let _: () = client
        .json_result_from_call("invalidateblock", params)
        .await
        .map_err(|e| eyre!(e))?;
    wait_for_pq_tip(client, funding_tip).await?;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let pool: Vec<String> = client
                .json_result_from_call("getrawmempool", "[]")
                .await
                .map_err(|e| eyre!(e))?;
            if pool == vec![spends[3].hash().to_string()] {
                return Ok::<_, eyre::Report>(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .wrap_err("rollback did not remove PQ spends while preserving the legacy spend")??;
    assert_eq!(
        client.blockchain_info().await?.blocks(),
        Height(activation.0 - 2)
    );
    for index in PQ_SPENDS {
        assert!(
            client.send_raw_transaction(&spends[index]).await.is_err(),
            "PQ spends are ineligible below activation after rollback"
        );
    }

    let template: BlockTemplateResponse = client
        .json_result_from_call("getblocktemplate", "[]")
        .await
        .map_err(|e| eyre!(e))?;
    assert_eq!(template.height(), activation.0 - 1);
    let mut candidate =
        proposal_block_from_template(&template, BlockTemplateTimeSource::default(), network)?;
    assert_eq!(
        candidate.transactions.len(),
        2,
        "only the legacy spend remains eligible below activation"
    );
    assert_eq!(candidate.transactions[1].hash(), spends[3].hash());
    // A different nonce avoids reusing the explicitly invalidated parent's hash.
    Arc::make_mut(&mut candidate.header).nonce = [7; 32].into();
    let proposal = |block: &Block| -> Result<String> {
        Ok(serde_json::json!([{"mode": "proposal", "data": hex::encode(block.zcash_serialize_to_vec()?)}]).to_string())
    };
    let valid: BlockProposalResponse = client
        .json_result_from_call("getblocktemplate", proposal(&candidate)?)
        .await
        .map_err(|e| eyre!(e))?;
    assert!(
        valid.is_valid(),
        "the pre-activation control block is valid: {valid:?}"
    );
    let mut forbidden = candidate.clone();
    let mut fees = Amount::<NonNegative>::zero();
    for index in PQ_SPENDS {
        fees = (fees + zakura_chain::transaction::zip317::conventional_fee(&spends[index]))?;
        forbidden.transactions.push(Arc::new(spends[index].clone()));
    }
    // Preserve the exact reward and both commitments so rejection reaches script validation.
    if let Transaction::V5 { outputs, .. } = Arc::make_mut(&mut forbidden.transactions[0]) {
        outputs[0] = Output::new((outputs[0].value() + fees)?, outputs[0].lock_script.clone());
    }
    let auth_root: AuthDataRoot = forbidden.transactions.iter().collect();
    let header = Arc::make_mut(&mut forbidden.header);
    header.merkle_root = forbidden.transactions.iter().collect();
    header.commitment_bytes = ChainHistoryBlockTxAuthCommitmentHash::from_commitments(
        &template.default_roots().chain_history_root(),
        &auth_root,
    )
    .bytes_in_serialized_order()
    .into();
    let rejected: BlockProposalResponse = client
        .json_result_from_call("getblocktemplate", proposal(&forbidden)?)
        .await
        .map_err(|e| eyre!(e))?;
    match rejected {
        BlockProposalResponse::Rejected(reason) => assert!(
            reason.contains("script"),
            "the otherwise valid block must fail script validation: {reason}"
        ),
        BlockProposalResponse::Valid => panic!("PQ spending block was accepted before activation"),
    }
    assert!(
        client.submit_block(forbidden).await.is_err(),
        "block submission also rejects pre-activation PQ authorizations"
    );
    assert_eq!(
        client.blockchain_info().await?.blocks(),
        Height(activation.0 - 2)
    );
    client.submit_block(candidate.clone()).await?;
    wait_for_pq_tip(client, candidate.hash()).await?;
    assert_ne!(
        candidate.hash(),
        old_parent.hash(),
        "the node follows an alternate branch"
    );
    for index in PQ_SPENDS {
        client.send_raw_transaction(&spends[index]).await?;
    }
    let (restored, height) = client.block_from_template(network).await?;
    assert_eq!(height, activation);
    for index in PQ_SPENDS {
        assert!(
            restored
                .transactions
                .iter()
                .any(|tx| tx.hash() == spends[index].hash()),
            "spends become eligible again on the alternate branch"
        );
    }
    let mined = client.mine_proposed_block(network).await?;
    assert_ne!(mined.hash(), old_block.hash());
    for index in ALL_SPENDS {
        assert_pq_rpc_bytes(client, &spends[index]).await?;
    }
    Ok(mined)
}
