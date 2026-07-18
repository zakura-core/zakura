//! Transaction flow test bodies for the zcashd-compat integration test suite.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use color_eyre::eyre::{eyre, Result};
use serde::Deserialize;
use tokio::time::sleep;
use zakura_chain::{
    block::ChainHistoryBlockTxAuthCommitmentHash,
    parameters::NetworkKind,
    serialization::{BytesInDisplayOrder, ZcashDeserializeInto},
    transaction::Transaction,
};
use zakura_rpc::{
    client::{BlockTemplateResponse, BlockTemplateTimeSource},
    proposal_block_from_template,
};
use zakura_test::net::random_known_port;

use super::{
    config::{read_test_network_kind, MINER_PRIV_WIF},
    launch::{spawn_zakurad_with_zcashd_compat_config, ZcashdCompatSetup},
    setup_zcashd_compat, wait_for_zcashd_height, zakura_skip_zcashd_compat_tests,
};
use crate::common::regtest::MiningRpcMethods;

const OVERSIZED_TRANSACTION_LIMIT: u64 = 1;
const OVERSIZED_REJECTION_METRIC: &str = "mempool_rejected_transactions_total";
const PEER_MISBEHAVIOR_FLUSH_WAIT: Duration = Duration::from_secs(40);

#[derive(Deserialize)]
struct FundRawTransactionResponse {
    hex: String,
}

#[derive(Deserialize)]
struct SignRawTransactionResponse {
    hex: String,
    complete: bool,
}

/// Imports the deterministic miner private key into zcashd's wallet (with a
/// rescan), making the mined coinbase outputs spendable via `sendtoaddress`.
async fn import_miner_key(setup: &ZcashdCompatSetup) -> Result<()> {
    let _: serde_json::Value = setup
        .zcashd_client
        .json_result_from_call(
            "importprivkey",
            &format!(r#"["{MINER_PRIV_WIF}", "", true]"#),
        )
        .await
        .map_err(|e| eyre!("importprivkey: {e}"))?;
    Ok(())
}

/// Builds and signs a transparent wallet transaction without broadcasting it.
async fn signed_transparent_transaction(setup: &ZcashdCompatSetup) -> Result<(String, String)> {
    let addr: String = setup
        .zcashd_client
        .json_result_from_call("getnewaddress", "[]")
        .await
        .map_err(|e| eyre!("getnewaddress: {e}"))?;

    let mut outputs = serde_json::Map::new();
    outputs.insert(addr, serde_json::json!(0.001));
    let create_params = serde_json::json!([[], outputs]).to_string();
    let raw: String = setup
        .zcashd_client
        .json_result_from_call("createrawtransaction", create_params)
        .await
        .map_err(|e| eyre!("createrawtransaction: {e}"))?;

    let fund_params = serde_json::json!([raw]).to_string();
    let funded: FundRawTransactionResponse = setup
        .zcashd_client
        .json_result_from_call("fundrawtransaction", fund_params)
        .await
        .map_err(|e| eyre!("fundrawtransaction: {e}"))?;

    let sign_params = serde_json::json!([funded.hex]).to_string();
    let signed: SignRawTransactionResponse = match setup
        .zcashd_client
        .json_result_from_call("signrawtransactionwithwallet", &sign_params)
        .await
    {
        Ok(signed) => signed,
        Err(with_wallet_error) => setup
            .zcashd_client
            .json_result_from_call("signrawtransaction", sign_params)
            .await
            .map_err(|legacy_error| {
                eyre!(
                    "signrawtransactionwithwallet: {with_wallet_error}; signrawtransaction: {legacy_error}"
                )
            })?,
    };

    if !signed.complete {
        return Err(eyre!("zcashd did not completely sign the transaction"));
    }

    let decode_params = serde_json::json!([&signed.hex]).to_string();
    let decoded: serde_json::Value = setup
        .zcashd_client
        .json_result_from_call("decoderawtransaction", decode_params)
        .await
        .map_err(|e| eyre!("decoderawtransaction: {e}"))?;
    let txid = decoded
        .get("txid")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| eyre!("decoderawtransaction response is missing `txid`: {decoded}"))?
        .to_string();

    Ok((signed.hex, txid))
}

async fn single_zcashd_peer(setup: &ZcashdCompatSetup) -> Result<serde_json::Value> {
    let mut peers: Vec<serde_json::Value> = setup
        .zcashd_client
        .json_result_from_call("getpeerinfo", "[]")
        .await
        .map_err(|error| eyre!("zcashd getpeerinfo: {error}"))?;

    if peers.len() != 1 {
        return Err(eyre!(
            "the sidecar must have exactly one Zakura peer, got: {peers:?}"
        ));
    }

    Ok(peers.pop().expect("peer count was checked"))
}

fn peer_connection_identity(peer: &serde_json::Value) -> Result<(u64, u64, String)> {
    if peer.get("inbound").and_then(serde_json::Value::as_bool) != Some(false) {
        return Err(eyre!(
            "the sidecar connection to Zakura must be outbound: {peer}"
        ));
    }

    let id = peer
        .get("id")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| eyre!("zcashd peer is missing its connection id: {peer}"))?;
    let connected_at = peer
        .get("conntime")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| eyre!("zcashd peer is missing its connection time: {peer}"))?;
    let addr = peer
        .get("addr")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| eyre!("zcashd peer is missing its address: {peer}"))?
        .to_string();

    Ok((id, connected_at, addr))
}

async fn single_zakura_peer_addr(setup: &ZcashdCompatSetup) -> Result<String> {
    let mut peers: Vec<serde_json::Value> = setup
        .zakura_client
        .json_result_from_call("getpeerinfo", "[]")
        .await
        .map_err(|error| eyre!("Zakura getpeerinfo: {error}"))?;

    if peers.len() != 1 {
        return Err(eyre!(
            "Zakura must have exactly one sidecar peer, got: {peers:?}"
        ));
    }

    let peer = peers.pop().expect("peer count was checked");
    if peer.get("inbound").and_then(serde_json::Value::as_bool) != Some(true) {
        return Err(eyre!("the sidecar must be inbound to Zakura: {peer}"));
    }

    peer.get("addr")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| eyre!("Zakura peer is missing its address: {peer}"))
}

async fn oversized_rejection_count(
    client: &reqwest::Client,
    metrics_addr: SocketAddr,
) -> Result<f64> {
    let body = client
        .get(format!("http://{metrics_addr}"))
        .send()
        .await
        .map_err(|error| eyre!("fetching Zakura metrics: {error}"))?
        .error_for_status()
        .map_err(|error| eyre!("Zakura metrics response: {error}"))?
        .text()
        .await
        .map_err(|error| eyre!("reading Zakura metrics: {error}"))?;

    Ok(body
        .lines()
        .find_map(|line| {
            if line.starts_with(OVERSIZED_REJECTION_METRIC)
                && line.contains("reason=\"transaction_too_large\"")
            {
                line.split_whitespace().last()?.parse::<f64>().ok()
            } else {
                None
            }
        })
        .unwrap_or(0.0))
}

async fn wait_for_oversized_rejection(
    client: &reqwest::Client,
    metrics_addr: SocketAddr,
    previous_count: f64,
) -> Result<()> {
    let mut last_seen = previous_count;

    for attempt in 1..=60u32 {
        last_seen = oversized_rejection_count(client, metrics_addr).await?;
        if last_seen >= previous_count + 1.0 {
            return Ok(());
        }

        if attempt < 60 {
            sleep(Duration::from_secs(1)).await;
        }
    }

    Err(eyre!(
        "Zakura did not record an oversized peer transaction within 60 s (before: {previous_count}, last seen: {last_seen})"
    ))
}

/// Submits a valid transparent transaction larger than Zakura's configured
/// mempool limit over P2P, keeps the peer connected, then proves the same
/// transaction is accepted inside a block.
pub async fn oversized_transparent_tx_rejected() -> Result<()> {
    if zakura_skip_zcashd_compat_tests() {
        return Ok(());
    }

    if read_test_network_kind()? != NetworkKind::Regtest {
        return Ok(());
    }

    let metrics_addr = SocketAddr::from(([127, 0, 0, 1], random_known_port()));
    let setup = spawn_zakurad_with_zcashd_compat_config(|config| {
        config.mempool.max_transaction_bytes = OVERSIZED_TRANSACTION_LIMIT;
        config.metrics.endpoint_addr = Some(metrics_addr);
    })
    .await?;

    setup.zakura_client.generate(110).await?;
    wait_for_zcashd_height(&setup.zcashd_client, 110).await?;
    import_miner_key(&setup).await?;

    let (p2p_raw, p2p_txid) = signed_transparent_transaction(&setup).await?;
    let p2p_raw_bytes = hex::decode(&p2p_raw)
        .map_err(|error| eyre!("zcashd returned invalid transaction hex: {error}"))?;
    let p2p_transaction_bytes = p2p_raw_bytes.len();
    let p2p_transaction_bytes_u64 = u64::try_from(p2p_transaction_bytes)
        .expect("transaction length fits in u64 on supported platforms");
    assert!(
        p2p_transaction_bytes_u64 > OVERSIZED_TRANSACTION_LIMIT,
        "test transaction must exceed the configured limit"
    );

    let peer_identity = peer_connection_identity(&single_zcashd_peer(&setup).await?)?;
    let zakura_peer_addr = single_zakura_peer_addr(&setup).await?;
    let metrics_client = reqwest::Client::new();
    let rejection_count = oversized_rejection_count(&metrics_client, metrics_addr).await?;
    let relayed_txid: String = setup
        .zcashd_client
        .json_result_from_call(
            "sendrawtransaction",
            serde_json::json!([&p2p_raw]).to_string(),
        )
        .await
        .map_err(|error| eyre!("zcashd sendrawtransaction: {error}"))?;
    assert_eq!(relayed_txid, p2p_txid, "zcashd returned an unexpected txid");

    let zcashd_mempool: Vec<String> = setup
        .zcashd_client
        .json_result_from_call("getrawmempool", "[]")
        .await
        .map_err(|error| eyre!("zcashd getrawmempool: {error}"))?;
    assert!(
        zcashd_mempool.iter().any(|entry| entry == &p2p_txid),
        "zcashd must accept the transaction before relaying it"
    );

    wait_for_oversized_rejection(&metrics_client, metrics_addr, rejection_count).await?;
    assert_eq!(
        peer_connection_identity(&single_zcashd_peer(&setup).await?)?,
        peer_identity,
        "Zakura must not disconnect the peer that relayed the oversized transaction"
    );

    let mempool: Vec<String> = setup
        .zakura_client
        .json_result_from_call("getrawmempool", "[]")
        .await
        .map_err(|error| eyre!("Zakura getrawmempool: {error}"))?;
    assert!(
        !mempool.iter().any(|entry| entry == &p2p_txid),
        "oversized peer transaction must not enter Zakura's mempool"
    );

    // Mempool misbehavior updates are applied to the address book in 30-second batches.
    sleep(PEER_MISBEHAVIOR_FLUSH_WAIT).await;
    assert_eq!(
        peer_connection_identity(&single_zcashd_peer(&setup).await?)?,
        peer_identity,
        "Zakura must not ban or reconnect the peer after the misbehavior flush"
    );
    assert_eq!(
        single_zakura_peer_addr(&setup).await?,
        zakura_peer_addr,
        "Zakura must retain the same sidecar connection after the policy rejection"
    );

    let (rpc_raw, _rpc_txid) = signed_transparent_transaction(&setup).await?;
    let rpc_transaction_bytes = hex::decode(&rpc_raw)
        .map_err(|error| eyre!("zcashd returned invalid transaction hex: {error}"))?
        .len();
    assert!(
        u64::try_from(rpc_transaction_bytes)
            .expect("transaction length fits in u64 on supported platforms")
            > OVERSIZED_TRANSACTION_LIMIT,
        "RPC test transaction must exceed the configured limit"
    );

    for attempt in 1..=2 {
        let response_text = setup
            .zakura_client
            .text_from_call(
                "sendrawtransaction",
                serde_json::json!([&rpc_raw]).to_string(),
            )
            .await?;
        let response: serde_json::Value = serde_json::from_str(&response_text)?;
        let error = response
            .get("error")
            .filter(|error| !error.is_null())
            .ok_or_else(|| {
                eyre!("sendrawtransaction attempt {attempt} unexpectedly succeeded: {response}")
            })?;

        assert_eq!(
            error.get("code").and_then(serde_json::Value::as_i64),
            Some(-25),
            "unexpected sendrawtransaction error on attempt {attempt}: {error}"
        );
        let message = error
            .get("message")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| eyre!("sendrawtransaction error is missing `message`: {error}"))?;
        assert!(
            message.contains(&format!("transaction is {rpc_transaction_bytes} bytes")),
            "error must include the actual serialized size: {message}"
        );
        assert!(
            message.contains("exceeding the configured mempool maximum of 1 bytes"),
            "error must include the configured limit: {message}"
        );
    }

    let block_template: BlockTemplateResponse = setup
        .zakura_client
        .json_result_from_call("getblocktemplate", "[]")
        .await
        .map_err(|error| eyre!("getblocktemplate: {error}"))?;
    let mut block = proposal_block_from_template(
        &block_template,
        BlockTemplateTimeSource::CurTime,
        &setup.network,
    )?;
    let transaction: Arc<Transaction> = p2p_raw_bytes.zcash_deserialize_into()?;
    block.transactions.push(transaction);

    let merkle_root = block.transactions.iter().map(|tx| tx.hash()).collect();
    let auth_data_root = block.auth_data_root();
    let chain_history_root = block_template.default_roots().chain_history_root();
    let header = Arc::make_mut(&mut block.header);
    header.merkle_root = merkle_root;
    header.commitment_bytes = ChainHistoryBlockTxAuthCommitmentHash::from_commitments(
        &chain_history_root,
        &auth_data_root,
    )
    .bytes_in_serialized_order()
    .into();

    setup.zakura_client.submit_block(block).await?;
    wait_for_zcashd_height(&setup.zcashd_client, 111).await?;
    assert_eq!(
        peer_connection_identity(&single_zcashd_peer(&setup).await?)?,
        peer_identity,
        "the same peer connection must remain live after the policy rejection"
    );
    let block_count: u64 = setup
        .zakura_client
        .json_result_from_call("getblockcount", "[]")
        .await
        .map_err(|e| eyre!("getblockcount: {e}"))?;
    assert_eq!(block_count, 111, "Zakura must accept the submitted block");

    let tx_info: serde_json::Value = setup
        .zakura_client
        .json_result_from_call(
            "getrawtransaction",
            serde_json::json!([p2p_txid, 1]).to_string(),
        )
        .await
        .map_err(|error| eyre!("getrawtransaction: {error}"))?;
    assert!(
        tx_info
            .get("confirmations")
            .and_then(serde_json::Value::as_u64)
            .is_some_and(|confirmations| confirmations >= 1),
        "the oversized transaction must be accepted through block validation: {tx_info}"
    );

    setup.teardown()
}

/// Sends a transparent transaction via zcashd and confirms it appears in
/// zakurad's mempool.
///
/// In managed (regtest) mode: funds the wallet by mining coinbase, sends a
/// transaction, and polls zakurad's `getrawmempool` until the txid appears.
///
/// In external mode: skips the send and instead validates the structural shape
/// of `getmempoolinfo` on zakurad.
pub async fn transparent_tx_in_mempool() -> Result<()> {
    let Some(setup) = setup_zcashd_compat().await? else {
        return Ok(());
    };

    if !setup.can_mutate() {
        // On live networks, just check that getmempoolinfo has the expected fields.
        let info: serde_json::Value = setup
            .zakura_client
            .json_result_from_call("getmempoolinfo", "[]")
            .await
            .map_err(|e| eyre!("getmempoolinfo: {e}"))?;

        for field in &["size", "bytes"] {
            assert!(
                info.get(field).is_some(),
                "getmempoolinfo missing field `{field}`: {info}"
            );
        }
        return setup.teardown();
    }

    // Mine enough blocks to have spendable coinbase (maturity = 100 on regtest).
    setup.zakura_client.generate(110).await?;
    wait_for_zcashd_height(&setup.zcashd_client, 110).await?;
    import_miner_key(&setup).await?;

    // Get a fresh address and send some ZEC to it.
    let addr: String = setup
        .zcashd_client
        .json_result_from_call("getnewaddress", "[]")
        .await
        .map_err(|e| eyre!("getnewaddress: {e}"))?;

    let txid: String = setup
        .zcashd_client
        .json_result_from_call("sendtoaddress", &format!(r#"["{addr}", 0.001]"#))
        .await
        .map_err(|e| eyre!("sendtoaddress: {e}"))?;

    wait_for_zakura_mempool_tx(&setup, &txid).await?;

    setup.teardown()
}

/// Polls zakurad's `getrawmempool` until `txid` appears (up to 30 s).
async fn wait_for_zakura_mempool_tx(setup: &ZcashdCompatSetup, txid: &str) -> Result<()> {
    for attempt in 1..=30u32 {
        let mempool: Vec<String> = setup
            .zakura_client
            .json_result_from_call("getrawmempool", "[]")
            .await
            .map_err(|e| eyre!("getrawmempool: {e}"))?;

        if mempool.iter().any(|entry| entry == txid) {
            return Ok(());
        }

        if attempt == 30 {
            return Err(eyre!(
                "txid {txid} never appeared in zakurad mempool after 30 s"
            ));
        }
        sleep(Duration::from_secs(1)).await;
    }

    Ok(())
}

/// Sends a transparent transaction via zcashd, mines a block, and confirms the
/// transaction via zakurad's `getrawtransaction`.
///
/// Only runs in managed (regtest) mode; skipped on external networks.
pub async fn transparent_tx_confirms() -> Result<()> {
    let Some(setup) = setup_zcashd_compat().await? else {
        return Ok(());
    };

    if !setup.can_mutate() {
        return setup.teardown();
    }

    // Mine enough blocks to have spendable coinbase.
    setup.zakura_client.generate(110).await?;
    wait_for_zcashd_height(&setup.zcashd_client, 110).await?;
    import_miner_key(&setup).await?;

    let addr: String = setup
        .zcashd_client
        .json_result_from_call("getnewaddress", "[]")
        .await
        .map_err(|e| eyre!("getnewaddress: {e}"))?;

    let txid: String = setup
        .zcashd_client
        .json_result_from_call("sendtoaddress", &format!(r#"["{addr}", 0.001]"#))
        .await
        .map_err(|e| eyre!("sendtoaddress: {e}"))?;

    // Wait for the transaction to relay from zcashd to zakurad over P2P before
    // mining: zcashd trickles tx invs to peers, so mining immediately would
    // build a block that misses the transaction.
    wait_for_zakura_mempool_tx(&setup, &txid).await?;

    // Mine a block to confirm the transaction.
    setup.zakura_client.generate(1).await?;

    // Verify via zakurad that the transaction has at least one confirmation.
    let tx_info: serde_json::Value = setup
        .zakura_client
        .json_result_from_call("getrawtransaction", &format!(r#"["{txid}", 1]"#))
        .await
        .map_err(|e| eyre!("getrawtransaction: {e}"))?;

    let confirmations = tx_info["confirmations"]
        .as_u64()
        .ok_or_else(|| eyre!("missing `confirmations` in getrawtransaction response: {tx_info}"))?;

    assert!(
        confirmations >= 1,
        "expected at least 1 confirmation, got {confirmations}"
    );

    setup.teardown()
}
