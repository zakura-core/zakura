//! LAB ONLY: builds Regtest blocks of a chosen size for the announcement-window lab.
//!
//! ```text
//! labctl address
//! labctl lane-init <rpc> <lane-file>
//! labctl prepare <rpc> <size-bytes> <lane-file> <out-hex-file> [--invalid]
//! ```
//!
//! `--invalid` overpays the coinbase by one zatoshi: the block decodes and its header passes, but
//! verification rejects it, which exercises a mined block that fails after it was announced.
//!
//! A "lane" is one `OP_TRUE` P2SH output that funds padding transactions. Each padding
//! transaction pays the lane's value back to the same script with no fee, and carries the
//! padding in an unspendable second output, so one lane funds any number of blocks.

use std::{fs, net::SocketAddr, sync::Arc};

use eyre::{eyre, Result};
use rand_10::Rng;

use zakura_chain::{
    amount::{Amount, NonNegative},
    block::{
        merkle::{AuthDataRoot, Root},
        Block, ChainHistoryBlockTxAuthCommitmentHash, Height,
    },
    parameters::{testnet::ConfiguredActivationHeights, Network, NetworkKind, NetworkUpgrade},
    serialization::{BytesInDisplayOrder, ZcashDeserializeInto, ZcashSerialize},
    transaction::{self, LockTime, Transaction},
    transparent::{Address, Input, OutPoint, Output, Script},
};
use zakura_node_services::rpc_client::RpcRequestClient;
use zakura_rpc::{
    client::{BlockTemplateResponse, BlockTemplateTimeSource, HexData},
    proposal_block_from_template,
};

/// HASH160 of the one-byte redeem script `OP_TRUE`.
const OP_TRUE_SCRIPT_HASH: [u8; 20] = [
    0xda, 0x17, 0x45, 0xe9, 0xb5, 0x49, 0xbd, 0x0b, 0xfa, 0x1a, 0x56, 0x99, 0x71, 0xc7, 0x7e, 0xba,
    0x30, 0xcd, 0x5a, 0x4b,
];

fn network() -> Network {
    Network::new_regtest(
        ConfiguredActivationHeights {
            nu5: Some(1),
            ..Default::default()
        }
        .into(),
    )
}

fn miner() -> Address {
    Address::from_script_hash(NetworkKind::Regtest, OP_TRUE_SCRIPT_HASH)
}

/// One candidate lane output: `txid:index:zatoshis`.
fn parse_lane(line: &str) -> Result<(OutPoint, Amount<NonNegative>)> {
    let mut parts = line.trim().split(':');
    let mut field = || {
        parts
            .next()
            .ok_or_else(|| eyre!("malformed lane line: {line}"))
    };
    let hash: transaction::Hash = field()?.parse().map_err(|_| eyre!("bad txid in {line}"))?;
    let index: u32 = field()?.parse()?;
    let value: i64 = field()?.parse()?;
    Ok((OutPoint { hash, index }, Amount::try_from(value)?))
}

fn lane_line(outpoint: &OutPoint, value: Amount<NonNegative>) -> String {
    format!("{}:{}:{}", outpoint.hash, outpoint.index, i64::from(value))
}

/// The newest lane candidate that is unspent on the node's best chain.
async fn live_lane(
    client: &RpcRequestClient,
    lane_file: &str,
) -> Result<(OutPoint, Amount<NonNegative>)> {
    let lanes = fs::read_to_string(lane_file)?;
    for line in lanes.lines().rev().filter(|line| !line.trim().is_empty()) {
        let (outpoint, value) = parse_lane(line)?;
        let unspent: serde_json::Value = serde_json::from_str(
            &client
                .text_from_call(
                    "gettxout",
                    format!(r#"["{}", {}, false]"#, outpoint.hash, outpoint.index),
                )
                .await?,
        )?;
        if unspent
            .get("result")
            .is_some_and(|result| !result.is_null())
        {
            return Ok((outpoint, value));
        }
    }
    Err(eyre!("no lane candidate in {lane_file} is unspent"))
}

/// A zero-fee transaction that pays the lane back to itself and carries `padding` bytes.
fn padding_transaction(
    lane: OutPoint,
    value: Amount<NonNegative>,
    network_upgrade: NetworkUpgrade,
    padding: usize,
) -> Transaction {
    // `OP_RETURN` followed by random bytes. Output scripts are not executed when created.
    let mut script = vec![0x6a; padding.max(1)];
    rand_10::rng().fill_bytes(&mut script[1..]);

    Transaction::V5 {
        network_upgrade,
        lock_time: LockTime::unlocked(),
        expiry_height: Height(0),
        inputs: vec![Input::PrevOut {
            outpoint: lane,
            unlock_script: Script::new(&[0x01, 0x51]),
            sequence: u32::MAX,
        }],
        outputs: vec![
            Output::new(value, miner().script()),
            Output::new(Amount::zero(), Script::new(&script)),
        ],
        sapling_shielded_data: None,
        orchard_shielded_data: None,
    }
}

/// `block` with `extra` appended and its header commitments recomputed.
fn with_transaction(
    mut block: Block,
    extra: Transaction,
    template: &BlockTemplateResponse,
) -> Block {
    block.transactions.push(Arc::new(extra));
    with_commitments(block, template)
}

/// `block` with its coinbase paying one zatoshi more than the template allows.
fn overpaying_coinbase(mut block: Block, template: &BlockTemplateResponse) -> Result<Block> {
    let coinbase = Arc::make_mut(&mut block.transactions[0]);
    let output = coinbase
        .outputs_mut()
        .first_mut()
        .ok_or_else(|| eyre!("the template coinbase has no transparent output"))?;
    output.value = (output.value + Amount::try_from(1)?)?;
    Ok(with_commitments(block, template))
}

/// `block` with its merkle root and auth data commitment recomputed from its transactions.
fn with_commitments(mut block: Block, template: &BlockTemplateResponse) -> Block {
    let merkle_root: Root = block.transactions.iter().collect();
    let auth_data_root: AuthDataRoot = block.transactions.iter().collect();
    let commitment = ChainHistoryBlockTxAuthCommitmentHash::from_commitments(
        &template.default_roots().chain_history_root(),
        &auth_data_root,
    );
    let header = Arc::make_mut(&mut block.header);
    header.merkle_root = merkle_root;
    header.commitment_bytes = commitment.bytes_in_serialized_order().into();
    block
}

async fn prepare(
    rpc: SocketAddr,
    size: usize,
    lane_file: &str,
    out: &str,
    invalid: bool,
) -> Result<()> {
    let client = RpcRequestClient::new(rpc);
    let net = network();
    let template: BlockTemplateResponse = client
        .json_result_from_call("getblocktemplate", "[]")
        .await
        .map_err(|error| eyre!(error))?;
    let mut block =
        proposal_block_from_template(&template, BlockTemplateTimeSource::default(), &net)?;
    let height = Height(template.height());
    let base = block.zcash_serialized_size();

    let mut next_lane = None;
    if size > base {
        let (lane, value) = live_lane(&client, lane_file).await?;
        let upgrade = NetworkUpgrade::current(&net, height);
        // Size the padding from a first build, since the script length prefix varies.
        let overhead = with_transaction(
            block.clone(),
            padding_transaction(lane, value, upgrade, 1),
            &template,
        )
        .zcash_serialized_size()
            - 1;
        let transaction =
            padding_transaction(lane, value, upgrade, size.saturating_sub(overhead + 4));
        next_lane = Some((
            OutPoint {
                hash: transaction.hash(),
                index: 0,
            },
            value,
        ));
        block = with_transaction(block, transaction, &template);
    }
    if invalid {
        block = overpaying_coinbase(block, &template)?;
    }

    fs::write(out, hex::encode(block.zcash_serialize_to_vec()?))?;
    if let Some((outpoint, value)) = next_lane {
        let mut lanes = fs::read_to_string(lane_file)?;
        lanes.push_str(&lane_line(&outpoint, value));
        lanes.push('\n');
        fs::write(lane_file, lanes)?;
    }
    println!(
        "{{\"hash\":\"{}\",\"height\":{},\"bytes\":{},\"parent\":\"{}\"}}",
        block.hash(),
        height.0,
        block.zcash_serialized_size(),
        block.header.previous_block_hash,
    );
    Ok(())
}

/// Starts a lane from the height 1 coinbase, which must already be mature.
async fn lane_init(rpc: SocketAddr, lane_file: &str) -> Result<()> {
    let client = RpcRequestClient::new(rpc);
    let HexData(raw) = client
        .json_result_from_call("getblock", r#"["1", 0]"#)
        .await
        .map_err(|error| eyre!(error))?;
    let block: Block = raw.zcash_deserialize_into()?;
    let coinbase = &block.transactions[0];
    let script = miner().script();
    let (index, output) = coinbase
        .outputs()
        .iter()
        .enumerate()
        .find(|(_, output)| output.lock_script == script)
        .ok_or_else(|| eyre!("the height 1 coinbase does not pay the lab miner address"))?;
    let outpoint = OutPoint {
        hash: coinbase.hash(),
        index: u32::try_from(index)?,
    };
    fs::write(lane_file, lane_line(&outpoint, output.value()) + "\n")?;
    println!("{}", lane_line(&outpoint, output.value()));
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    match args.as_slice() {
        ["address"] => {
            println!("{}", miner());
            Ok(())
        }
        ["lane-init", rpc, lane_file] => lane_init(rpc.parse()?, lane_file).await,
        ["prepare", rpc, size, lane_file, out] => {
            prepare(rpc.parse()?, size.parse()?, lane_file, out, false).await
        }
        ["prepare", rpc, size, lane_file, out, "--invalid"] => {
            prepare(rpc.parse()?, size.parse()?, lane_file, out, true).await
        }
        _ => Err(eyre!(
            "usage: labctl address | lane-init <rpc> <lane-file> | \
             prepare <rpc> <size> <lane-file> <out> [--invalid]"
        )),
    }
}
