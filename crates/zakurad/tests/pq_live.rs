//! Funds and spends a post-quantum output on a running node, for operator verification.
//!
//! This is not part of CI: it talks to a live node over its RPC, which `PQ_LIVE_RPC` names
//! (`127.0.0.1:18232`, over an SSH tunnel to the deployment host). It needs a chain that has
//! activated the rules and one spendable output, which it finds by asking the node for the
//! unspent outputs of `PQ_LIVE_ADDRESS`. On the isolated activation testnet that address is
//! the `OP_TRUE` pay-to-script-hash the miner is paid to, so anyone can run this.
//!
//! ```sh
//! ssh -N -L 18232:127.0.0.1:18232 zakura-pq-testnet-1 &
//! PQ_LIVE_RPC=127.0.0.1:18232 PQ_LIVE_ADDRESS=t2SRyAR26tXTnZHfpa3jPqeyYmxCbAZxUnh \
//!   cargo test -p zakura --features pq --test pq_live -- --nocapture --ignored
//! ```
#![cfg(feature = "pq")]
// An operator reads this fixture's progress as it runs, so it reports on stdout.
#![allow(clippy::print_stdout)]

use std::{net::SocketAddr, sync::Arc, time::Duration};

use eyre::{eyre, Result};
use hex::FromHex;
use zakura_chain::{
    amount::{Amount, NonNegative},
    parameters::NetworkUpgrade,
    serialization::{ZcashDeserializeInto, ZcashSerialize},
    transaction::{HashType, LockTime, Transaction},
    transparent::{Address, Input, OutPoint, Output, Script},
};
use zakura_node_services::rpc_client::RpcRequestClient;
use zakura_rpc::client::HexData;
use zcash_protocol::consensus::NetworkType;
use zcash_transparent::pq::{output, sha2_128f, single_key, ParameterSet};

/// The scheme this run demonstrates. Both schemes take the same path through the node.
const PARAMETERS: ParameterSet = ParameterSet::Sha2_128f;

/// `OP_TRUE`, and the input that satisfies its pay-to-script-hash output.
const OP_TRUE: [u8; 1] = [0x51];

/// Pushes `bytes` with the shortest opcode that can carry them, which is the only encoding a
/// post-quantum input may use.
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

#[derive(serde::Deserialize)]
struct Utxo {
    txid: String,
    #[serde(rename = "outputIndex")]
    output_index: u32,
    satoshis: i64,
    height: u32,
}

#[derive(serde::Deserialize)]
struct NetworkParameters {
    network: String,
    #[serde(rename = "pqActivationHeight")]
    pq_activation_height: Option<u32>,
}

async fn wait_for_mined(client: &RpcRequestClient, transaction: &Transaction) -> Result<u32> {
    let id = transaction.hash().to_string();
    println!("waiting for {id} to be mined");
    // Blocks are mined on a target spacing of seconds, so minutes is a generous wait.
    for _ in 0..240 {
        // The node answers with an error while it is still verifying what it was just sent,
        // and reports no height while the transaction only sits in the mempool. Both mean
        // "not yet", so neither ends the wait.
        let raw = client
            .json_result_from_call::<serde_json::Value>(
                "getrawtransaction",
                format!(r#"["{id}", 1]"#),
            )
            .await
            .unwrap_or_default();
        if let Some(height) = raw.get("height").and_then(serde_json::Value::as_u64) {
            // The exact bytes the node returns must match what we signed and sent.
            let hex: HexData = client
                .json_result_from_call("getrawtransaction", format!(r#"["{id}", 0]"#))
                .await
                .map_err(|error| eyre!(error))?;
            assert_eq!(
                hex.0,
                transaction.zcash_serialize_to_vec()?,
                "the node keeps the authorization bytes it was sent"
            );
            assert_eq!(
                hex.0.as_slice().zcash_deserialize_into::<Transaction>()?,
                *transaction
            );
            return Ok(u32::try_from(height)?);
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    Err(eyre!("{id} was not mined in time"))
}

/// Pays a post-quantum output on a live chain, then spends it, and reports both heights.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "talks to a live node named by PQ_LIVE_RPC"]
async fn pq_live_spend() -> Result<()> {
    let rpc: SocketAddr = std::env::var("PQ_LIVE_RPC")
        .unwrap_or_else(|_| "127.0.0.1:18232".to_string())
        .parse()?;
    let address: Address = std::env::var("PQ_LIVE_ADDRESS")
        .map_err(|_| eyre!("PQ_LIVE_ADDRESS must name the address holding the coins to spend"))?
        .parse()?;
    let client = RpcRequestClient::new_with_timeout(rpc, Duration::from_secs(30));

    let parameters: NetworkParameters = client
        .json_result_from_call("getnetworkparameters", "[]")
        .await
        .map_err(|error| eyre!(error))?;
    let activation = parameters
        .pq_activation_height
        .ok_or_else(|| eyre!("{} does not configure the rules", parameters.network))?;
    println!(
        "network {} activates the post-quantum rules at {activation}",
        parameters.network
    );

    let chain: serde_json::Value = client
        .json_result_from_call("getblockchaininfo", "[]")
        .await
        .map_err(|error| eyre!(error))?;
    let tip = u32::try_from(
        chain
            .get("blocks")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| eyre!("the node reports its height"))?,
    )?;
    // A v5 transaction names the upgrade whose rules it is made under, and the node refuses
    // one that is not the upgrade in force, so take it from the chain rather than assuming.
    let next_branch = chain
        .pointer("/consensus/nextblock")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| eyre!("the node reports the next block's consensus branch"))?;
    let branch = zakura_chain::parameters::ConsensusBranchId::from_hex(next_branch)
        .map_err(|error| eyre!("{next_branch} is not a consensus branch id: {error}"))?;
    let upgrade = [
        NetworkUpgrade::Nu7,
        NetworkUpgrade::Nu6_3,
        NetworkUpgrade::Nu6_2,
        NetworkUpgrade::Nu6_1,
        NetworkUpgrade::Nu6,
        NetworkUpgrade::Nu5,
    ]
    .into_iter()
    .find(|candidate| candidate.branch_id() == Some(branch))
    .ok_or_else(|| eyre!("no known upgrade has consensus branch {next_branch}"))?;
    println!("the next block is under {upgrade:?} (branch {next_branch})");
    let utxos: Vec<Utxo> = client
        .json_result_from_call(
            "getaddressutxos",
            format!(r#"[{{"addresses": ["{address}"]}}]"#),
        )
        .await
        .map_err(|error| eyre!(error))?;
    // A coinbase output is spendable only once the chain is a maturity window past it, and on
    // a fresh activation run every coin is a coinbase, so this waits for that window.
    let maturity = zakura_chain::transparent::MIN_TRANSPARENT_COINBASE_MATURITY;
    let candidates = utxos.len();
    let funding_source = utxos
        .into_iter()
        .filter(|utxo| utxo.height + maturity <= tip)
        .max_by_key(|utxo| utxo.satoshis)
        .ok_or_else(|| {
            eyre!(
                "none of {address}'s {candidates} outputs is mature at height {tip}; \
                 the chain needs to be {maturity} blocks past one of them"
            )
        })?;
    let value: Amount<NonNegative> = funding_source.satoshis.try_into()?;
    println!(
        "spending {value:?} from {}:{} mined at {}",
        funding_source.txid, funding_source.output_index, funding_source.height
    );

    // The body one signer holds, which is what a wallet writes for an address it derives.
    let key = sha2_128f::SigningKey::from_seed(&[11; sha2_128f::SEED_BYTES]);
    let body = single_key::body(PARAMETERS, &key.public_key())?;
    let pq_script = Script::new(&output::output_script(PARAMETERS, &body)?.0);

    let transaction = |outpoint, unlock: Vec<u8>, outputs| Transaction::V5 {
        network_upgrade: upgrade,
        lock_time: LockTime::unlocked(),
        expiry_height: zakura_chain::block::Height(0),
        inputs: vec![Input::PrevOut {
            outpoint,
            unlock_script: Script::new(&unlock),
            sequence: u32::MAX,
        }],
        outputs,
        sapling_shielded_data: None,
        orchard_shielded_data: None,
    };

    // The payment: half into a post-quantum output, the rest back where it came from.
    let half = Amount::<NonNegative>::try_from(i64::from(value) / 2)?;
    let mut funding = transaction(
        OutPoint {
            hash: funding_source.txid.parse()?,
            index: funding_source.output_index,
        },
        push(&OP_TRUE),
        vec![
            Output::new(half, pq_script.clone()),
            Output::new(half, address.script()),
        ],
    );
    let fee = zakura_chain::transaction::zip317::conventional_fee(&funding);
    if let Transaction::V5 { outputs, .. } = &mut funding {
        outputs[1] = Output::new((half - fee)?.constrain()?, address.script());
    }
    client
        .json_result_from_call::<serde_json::Value>(
            "sendrawtransaction",
            format!(r#"["{}"]"#, hex::encode(funding.zcash_serialize_to_vec()?)),
        )
        .await
        .map_err(|error| eyre!(error))?;
    let funded_at = wait_for_mined(&client, &funding).await?;
    println!("post-quantum output created at height {funded_at}");

    // The spend: reveal the body, sign the digest the node computes over it.
    let previous = vec![Output::new(half, pq_script)];
    let mut spend = transaction(
        OutPoint {
            hash: funding.hash(),
            index: 0,
        },
        Vec::new(),
        vec![Output::new(half, address.script())],
    );
    let install = |spend: &mut Transaction, signature: &[u8]| {
        let mut input = push(signature);
        input.extend(push(&body.0));
        if let Transaction::V5 { inputs, .. } = spend {
            if let Input::PrevOut { unlock_script, .. } = &mut inputs[0] {
                *unlock_script = Script::new(&input);
            }
        }
    };
    // A signature-sized placeholder fixes the serialized size, so the fee is known first.
    install(&mut spend, &vec![0; sha2_128f::SIGNATURE_BYTES + 1]);
    let fee = zakura_chain::transaction::zip317::conventional_fee(&spend);
    if let Transaction::V5 { outputs, .. } = &mut spend {
        outputs[0] = Output::new((half - fee)?.constrain()?, address.script());
    }
    let digest = spend
        .sighasher(upgrade, Arc::new(previous))?
        .sighash(HashType::ALL, Some((0, body.0.clone())));
    let mut signature = key.sign(
        &digest.0,
        output::signing_context(NetworkType::Test, PARAMETERS),
    )?;
    signature.push(u8::try_from(HashType::ALL.bits())?);
    install(&mut spend, &signature);
    assert!(
        spend.zcash_serialize_to_vec()?.len() > sha2_128f::SIGNATURE_BYTES,
        "the signature travels with the transaction"
    );
    client
        .json_result_from_call::<serde_json::Value>(
            "sendrawtransaction",
            format!(r#"["{}"]"#, hex::encode(spend.zcash_serialize_to_vec()?)),
        )
        .await
        .map_err(|error| eyre!(error))?;
    let spent_at = wait_for_mined(&client, &spend).await?;
    println!(
        "post-quantum output {} spent by {} at height {spent_at}",
        funding.hash(),
        spend.hash()
    );
    assert!(spent_at >= activation);
    Ok(())
}

/// Keeps the single-signer body and the output script agreeing with the library, offline.
#[test]
fn the_fixture_commits_to_the_body_it_reveals() {
    let key = sha2_128f::SigningKey::from_seed(&[11; sha2_128f::SEED_BYTES]);
    let body = single_key::body(PARAMETERS, &key.public_key()).expect("the key is the right size");
    let script = output::output_script(PARAMETERS, &body).expect("the body is small enough");
    let parsed = output::parse_output(&script)
        .expect("the script is a post-quantum output")
        .expect("the script is in the namespace");
    assert_eq!(parsed.parameters(), PARAMETERS);
    let mut input = push(&[0; sha2_128f::SIGNATURE_BYTES + 1]);
    input.extend(push(&body.0));
    assert_eq!(
        parsed
            .authenticate_spend(&input)
            .expect("the input reveals the committed body")
            .body(),
        body.0.as_slice()
    );
    assert_eq!(
        script.0,
        single_key::output_script(PARAMETERS, &key.public_key())
            .expect("one script")
            .0
    );
}
