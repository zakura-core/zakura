//! NU7 activation test bodies for the zcashd-compat integration suite.
//!
//! A sidecar with NU7 support must follow zakurad across NU7 on regtest: the same blocks,
//! the same ZIP 235 fee burn and ZIP 237 NSM value balance, and a clean resync and restart.

use std::time::Duration;

use color_eyre::eyre::{ensure, eyre, Result};
use serde_json::Value;

use super::{
    config::{RegtestProfile, MINER_PRIV_WIF, NU7_TEST_INITIAL_NSM_VALUE_BALANCE},
    launch::{spawn_zakurad_with_zcashd_compat_profile, wait_for_zcashd_rpc, ZcashdCompatSetup},
    wait_for_zcashd_height, zakura_skip_zcashd_compat_tests, TEST_ZCASHD_COMPAT_NU7,
};
use crate::common::regtest::MiningRpcMethods;

/// The regtest NU7 activation height of these tests.
const NU7_HEIGHT: u32 = 210;

/// The NU6.3 and NU7 consensus branch ids, as `getblockchaininfo` reports them.
const NU6_3_BRANCH_ID: &str = "37a5165b";
const NU7_BRANCH_ID: &str = "77190ad9";

/// The fee each test transaction pays. From NU7, floor(6 * fees / 10) of a block's fees
/// leaves circulation, so two of these burn 12,001 zatoshis.
const FEE_ZAT: i64 = 10_001;

/// Spawns zakurad and a sidecar with NU7 at [`NU7_HEIGHT`], or returns `None` if the
/// zcashd-compat tests or the NU7 tests are disabled.
#[allow(clippy::print_stderr)]
async fn setup_nu7() -> Result<Option<ZcashdCompatSetup>> {
    if zakura_skip_zcashd_compat_tests() {
        return Ok(None);
    }
    if std::env::var_os(TEST_ZCASHD_COMPAT_NU7).is_none() {
        eprintln!(
            "Skipped NU7 zcashd-compat test; set {TEST_ZCASHD_COMPAT_NU7}=1 and TEST_ZCASHD_PATH \
             to a zcashd with NU7 support to run"
        );
        return Ok(None);
    }
    spawn_zakurad_with_zcashd_compat_profile(RegtestProfile::Nu7At(NU7_HEIGHT), |_| {})
        .await
        .map(Some)
}

/// The chain state that zakurad and the sidecar must agree on.
#[derive(Debug, PartialEq)]
struct ChainState {
    height: u64,
    tip: String,
    branch: String,
    nsm_value_balance: Option<i64>,
    chain_supply: Option<i64>,
}

/// Reads a [`ChainState`] from a `getblockchaininfo` result.
fn chain_state(info: &Value) -> Result<ChainState> {
    Ok(ChainState {
        height: info["blocks"]
            .as_u64()
            .ok_or_else(|| eyre!("getblockchaininfo has no blocks: {info}"))?,
        tip: info["bestblockhash"]
            .as_str()
            .ok_or_else(|| eyre!("getblockchaininfo has no bestblockhash: {info}"))?
            .to_string(),
        branch: info["consensus"]["chaintip"]
            .as_str()
            .ok_or_else(|| eyre!("getblockchaininfo has no consensus.chaintip: {info}"))?
            .to_string(),
        nsm_value_balance: info["nsmValueBalanceZat"].as_i64(),
        chain_supply: info["chainSupply"]["chainValueZat"].as_i64(),
    })
}

/// Returns the chain state that both nodes report, which must be the same.
async fn agreed_chain_state(setup: &ZcashdCompatSetup) -> Result<ChainState> {
    let zakura: Value = setup
        .zakura_client
        .json_result_from_call("getblockchaininfo", "[]")
        .await
        .map_err(|e| eyre!("zakurad getblockchaininfo: {e}"))?;
    let zcashd: Value = setup
        .zcashd_client
        .json_result_from_call("getblockchaininfo", "[]")
        .await
        .map_err(|e| eyre!("zcashd getblockchaininfo: {e}"))?;
    let (zakura, zcashd) = (chain_state(&zakura)?, chain_state(&zcashd)?);
    ensure!(
        zakura == zcashd,
        "zakurad and zcashd disagree: {zakura:?} vs {zcashd:?}"
    );
    Ok(zakura)
}

/// Mines `blocks` on zakurad, waits for the sidecar, and returns the agreed chain state.
async fn mine_and_agree(setup: &ZcashdCompatSetup, blocks: u32) -> Result<ChainState> {
    setup.zakura_client.generate(blocks).await?;
    let height: u64 = setup
        .zakura_client
        .json_result_from_call("getblockcount", "[]")
        .await
        .map_err(|e| eyre!("zakurad getblockcount: {e}"))?;
    wait_for_zcashd_height(&setup.zcashd_client, height).await?;
    agreed_chain_state(setup).await
}

/// Returns an amount in ZEC from an RPC result as zatoshis.
fn zatoshis(value: &Value) -> Result<i64> {
    let zec = value
        .as_f64()
        .ok_or_else(|| eyre!("not an amount: {value}"))?;
    // RPC amounts have at most 8 decimal places, so rounding recovers the exact value.
    #[allow(clippy::cast_possible_truncation)]
    Ok((zec * 100_000_000.0).round() as i64)
}

/// Checks that both nodes report the same `getblocksubsidy` at `height`, and returns the
/// total block subsidy in zatoshis.
async fn agreed_block_subsidy(setup: &ZcashdCompatSetup, height: u32) -> Result<i64> {
    let params = format!("[{height}]");
    let zakura: Value = setup
        .zakura_client
        .json_result_from_call("getblocksubsidy", &params)
        .await
        .map_err(|e| eyre!("zakurad getblocksubsidy {height}: {e}"))?;
    let zcashd: Value = setup
        .zcashd_client
        .json_result_from_call("getblocksubsidy", &params)
        .await
        .map_err(|e| eyre!("zcashd getblocksubsidy {height}: {e}"))?;
    for field in [
        "miner",
        "totalblocksubsidy",
        "fundingstreamstotal",
        "lockboxtotal",
    ] {
        ensure!(
            zatoshis(&zakura[field])? == zatoshis(&zcashd[field])?,
            "getblocksubsidy {height} {field} differs: zakurad {} vs zcashd {}",
            zakura[field],
            zcashd[field]
        );
    }
    zatoshis(&zakura["totalblocksubsidy"])
}

/// Returns the `(version, versiongroupid, expiryheight)` of a raw transaction that zcashd
/// builds for the next block.
async fn new_transaction_shape(setup: &ZcashdCompatSetup) -> Result<(i64, String, i64)> {
    let dummy_input = r#"[[{"txid":"0000000000000000000000000000000000000000000000000000000000000001","vout":0}], {}]"#;
    let raw: String = setup
        .zcashd_client
        .json_result_from_call("createrawtransaction", dummy_input)
        .await
        .map_err(|e| eyre!("zcashd createrawtransaction: {e}"))?;
    let decoded: Value = setup
        .zcashd_client
        .json_result_from_call("decoderawtransaction", format!(r#"["{raw}"]"#))
        .await
        .map_err(|e| eyre!("zcashd decoderawtransaction: {e}"))?;
    Ok((
        decoded["version"].as_i64().unwrap_or_default(),
        decoded["versiongroupid"]
            .as_str()
            .unwrap_or_default()
            .to_string(),
        decoded["expiryheight"].as_i64().unwrap_or_default(),
    ))
}

/// Sends a transaction from a mature coinbase output in zcashd's wallet back to the miner
/// address, paying [`FEE_ZAT`], and returns its txid.
async fn send_fee_transaction(setup: &ZcashdCompatSetup, input: &Value) -> Result<String> {
    let txid = input["txid"]
        .as_str()
        .ok_or_else(|| eyre!("unspent output has no txid: {input}"))?;
    let vout = input["vout"]
        .as_u64()
        .ok_or_else(|| eyre!("unspent output has no vout: {input}"))?;
    let address = input["address"]
        .as_str()
        .ok_or_else(|| eyre!("unspent output has no address: {input}"))?;
    let value = zatoshis(&input["amount"])? - FEE_ZAT;
    #[allow(clippy::cast_precision_loss)]
    let value = value as f64 / 100_000_000.0;
    let raw: String = setup
        .zcashd_client
        .json_result_from_call(
            "createrawtransaction",
            format!(r#"[[{{"txid":"{txid}","vout":{vout}}}], {{"{address}": {value:.8}}}]"#),
        )
        .await
        .map_err(|e| eyre!("zcashd createrawtransaction: {e}"))?;
    let signed: Value = setup
        .zcashd_client
        .json_result_from_call("signrawtransaction", format!(r#"["{raw}"]"#))
        .await
        .map_err(|e| eyre!("zcashd signrawtransaction: {e}"))?;
    ensure!(
        signed["complete"].as_bool() == Some(true),
        "zcashd could not sign: {signed}"
    );
    setup
        .zcashd_client
        .json_result_from_call(
            "sendrawtransaction",
            format!(r#"["{}"]"#, signed["hex"].as_str().unwrap_or_default()),
        )
        .await
        .map_err(|e| eyre!("zcashd sendrawtransaction: {e}"))
}

/// Waits up to 30 seconds for zakurad's mempool to contain every one of `txids`.
async fn wait_for_zakura_mempool(setup: &ZcashdCompatSetup, txids: &[String]) -> Result<()> {
    for _ in 0..30 {
        let mempool: Vec<String> = setup
            .zakura_client
            .json_result_from_call("getrawmempool", "[]")
            .await
            .map_err(|e| eyre!("zakurad getrawmempool: {e}"))?;
        if txids.iter().all(|txid| mempool.contains(txid)) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Err(eyre!("zakurad's mempool did not receive {txids:?}"))
}

/// The sidecar follows zakurad across NU7 activation: the same blocks, branch ids, NSM value
/// balance, chain supply and block subsidy.
pub async fn activation_follows_zakurad() -> Result<()> {
    let Some(setup) = setup_nu7().await? else {
        return Ok(());
    };

    let before = mine_and_agree(&setup, NU7_HEIGHT - 1).await?;
    ensure!(
        before.branch == NU6_3_BRANCH_ID,
        "unexpected branch before NU7: {before:?}"
    );
    ensure!(
        before.nsm_value_balance == Some(NU7_TEST_INITIAL_NSM_VALUE_BALANCE),
        "the NSM value balance after the block before NU7 is not the configured seed: {before:?}"
    );

    let activation = mine_and_agree(&setup, 1).await?;
    ensure!(
        activation.branch == NU7_BRANCH_ID,
        "NU7 did not activate at {NU7_HEIGHT}: {activation:?}"
    );

    // ZIP 218: the subsidy is a third of the pre-NU7 one from activation.
    let pre_nu7_subsidy = agreed_block_subsidy(&setup, NU7_HEIGHT - 1).await?;
    let nu7_subsidy = agreed_block_subsidy(&setup, NU7_HEIGHT).await?;
    ensure!(
        nu7_subsidy == pre_nu7_subsidy / 3,
        "unexpected NU7 subsidy {nu7_subsidy} after {pre_nu7_subsidy}"
    );
    agreed_block_subsidy(&setup, NU7_HEIGHT + 1).await?;

    mine_and_agree(&setup, 5).await?;
    setup.teardown()
}

/// From NU7 zcashd builds v5 transactions with the post-NU7 expiry, and a block's fees are
/// split as ZIP 235 requires on both nodes, which then agree on the NSM value balance.
pub async fn fee_burn_and_wallet_transactions() -> Result<()> {
    let Some(setup) = setup_nu7().await? else {
        return Ok(());
    };

    // Before NU7, a new transaction expires no later than the last pre-NU7 block.
    mine_and_agree(&setup, NU7_HEIGHT - 3).await?;
    let (version, _, expiry) = new_transaction_shape(&setup).await?;
    ensure!(
        version == 5 && expiry == i64::from(NU7_HEIGHT) - 1,
        "unexpected pre-NU7 transaction: version {version}, expiry {expiry}"
    );

    // From NU7, the default expiry is 120 blocks (40 * 3) after the next block.
    let tip = mine_and_agree(&setup, 103).await?;
    let (version, group, expiry) = new_transaction_shape(&setup).await?;
    #[allow(clippy::cast_possible_wrap)]
    let next_height = tip.height as i64 + 1;
    ensure!(
        version == 5 && group == "26a7270a" && expiry == next_height + 120,
        "unexpected post-NU7 transaction: version {version}, group {group}, expiry {expiry}"
    );

    // Spend two mature coinbase outputs, each paying FEE_ZAT.
    let _: Value = setup
        .zcashd_client
        .json_result_from_call(
            "importprivkey",
            format!(r#"["{MINER_PRIV_WIF}", "", true]"#),
        )
        .await
        .map_err(|e| eyre!("zcashd importprivkey: {e}"))?;
    let unspent: Vec<Value> = setup
        .zcashd_client
        .json_result_from_call("listunspent", "[100]")
        .await
        .map_err(|e| eyre!("zcashd listunspent: {e}"))?;
    let coinbase: Vec<&Value> = unspent
        .iter()
        .filter(|output| output["generated"].as_bool() == Some(true))
        .take(2)
        .collect();
    ensure!(
        coinbase.len() == 2,
        "zcashd's wallet has fewer than two mature coinbase outputs"
    );
    let mut txids = Vec::new();
    for input in coinbase {
        txids.push(send_fee_transaction(&setup, input).await?);
    }
    wait_for_zakura_mempool(&setup, &txids).await?;

    let before = agreed_chain_state(&setup).await?;
    let after = mine_and_agree(&setup, 1).await?;
    let subsidy = agreed_block_subsidy(&setup, u32::try_from(after.height)?).await?;
    let burned = 6 * 2 * FEE_ZAT / 10;
    let nsm = |state: &ChainState| {
        state
            .nsm_value_balance
            .ok_or_else(|| eyre!("no NSM value balance: {state:?}"))
    };
    let supply = |state: &ChainState| {
        state
            .chain_supply
            .ok_or_else(|| eyre!("no chain supply: {state:?}"))
    };
    ensure!(
        nsm(&after)? == nsm(&before)? + burned,
        "the NSM value balance did not grow by {burned}: {before:?} then {after:?}"
    );
    ensure!(
        supply(&after)? == supply(&before)? + subsidy - burned,
        "the chain supply did not grow by the subsidy minus {burned}: {before:?} then {after:?}"
    );

    setup.teardown()
}

/// The sidecar syncs a burst of blocks across NU7 without dropping its connection, and after
/// a restart it recomputes the same NSM value balance.
pub async fn burst_sync_and_restart() -> Result<()> {
    let Some(setup) = setup_nu7().await? else {
        return Ok(());
    };

    // zakurad serves at most 16 blocks per request, so a sidecar that asks for more
    // would stall until its block download timeout disconnected its only peer.
    let peers_before: Vec<Value> = setup
        .zcashd_client
        .json_result_from_call("getpeerinfo", "[]")
        .await
        .map_err(|e| eyre!("zcashd getpeerinfo: {e}"))?;
    let state = mine_and_agree(&setup, 300).await?;
    ensure!(
        state.branch == NU7_BRANCH_ID,
        "unexpected branch: {state:?}"
    );
    let peers_after: Vec<Value> = setup
        .zcashd_client
        .json_result_from_call("getpeerinfo", "[]")
        .await
        .map_err(|e| eyre!("zcashd getpeerinfo: {e}"))?;
    ensure!(
        peers_before.len() == 1
            && peers_after.len() == 1
            && peers_before[0]["id"] == peers_after[0]["id"],
        "the sidecar reconnected during the burst: {peers_before:?} then {peers_after:?}"
    );

    // The supervisor restarts the stopped sidecar, which recomputes its in-memory NSM
    // value balance from the block index.
    let _: Value = setup
        .zcashd_client
        .json_result_from_call("stop", "[]")
        .await
        .map_err(|e| eyre!("zcashd stop: {e}"))?;
    tokio::time::sleep(Duration::from_secs(10)).await;
    wait_for_zcashd_rpc(&setup.zcashd_client).await?;
    wait_for_zcashd_height(&setup.zcashd_client, state.height).await?;
    let restarted = agreed_chain_state(&setup).await?;
    ensure!(
        restarted == state,
        "the restarted sidecar disagrees: {state:?} then {restarted:?}"
    );

    mine_and_agree(&setup, 2).await?;
    setup.teardown()
}
