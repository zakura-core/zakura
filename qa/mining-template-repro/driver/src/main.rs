//! Deterministic equal-height non-finalized reorg driver for the zakura#1080 repro.
//!
//! Builds two sibling blocks on the current tip. Siblings at the same height carry
//! the same difficulty threshold, so their chain work is equal, and
//! `Chain::cmp` breaks the tie on raw internal tip-hash bytes. Submitting the
//! lesser-hash sibling first and the greater-hash sibling second therefore forces
//! the best chain to move *sideways* at the same height — the exact condition
//! issue #1080 describes. Submitting in the opposite order is the control: the
//! same two blocks arrive, but the best tip never changes.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{anyhow, bail, Context, Result};
use zakura_chain::{
    block::{Block, Hash},
    parameters::{testnet::ConfiguredActivationHeights, Network},
    serialization::ZcashSerialize,
    work::equihash::Solution,
};
use zakura_node_services::rpc_client::RpcRequestClient;
use zakura_rpc::{
    client::{BlockTemplateResponse, BlockTemplateTimeSource},
    proposal_block_from_template,
};

struct Args {
    rpc: SocketAddr,
    /// Submit the lesser-hash sibling first (forces the flip) or last (control).
    lesser_first: bool,
    /// How many template builds to have in flight when the flip lands.
    in_flight_calls: u32,
    /// Gap between starting those builds and submitting the second sibling.
    inject_delay_ms: u64,
    rounds: u32,
    nu5_height: u32,
    /// Gap between the two sibling submissions. It has to be long enough for a
    /// template to be built on the first sibling, otherwise every in-flight
    /// template is still built on the shared parent and the second submission
    /// reads as an ordinary forward advance rather than a sideways flip.
    flip_delay_ms: u64,
}

fn parse_args() -> Result<Args> {
    let mut args = Args {
        rpc: "127.0.0.1:18232".parse()?,
        lesser_first: true,
        in_flight_calls: 32,
        inject_delay_ms: 3,
        rounds: 1,
        nu5_height: 5,
        flip_delay_ms: 250,
    };
    let mut argv = std::env::args().skip(1);
    while let Some(flag) = argv.next() {
        let mut value = || argv.next().ok_or_else(|| anyhow!("{flag} needs a value"));
        match flag.as_str() {
            "--rpc" => args.rpc = value()?.parse()?,
            "--order" => match value()?.as_str() {
                "lesser-first" => args.lesser_first = true,
                "greater-first" => args.lesser_first = false,
                other => bail!("--order must be lesser-first or greater-first, got {other}"),
            },
            "--rounds" => args.rounds = value()?.parse()?,
            "--nu5-height" => args.nu5_height = value()?.parse()?,
            "--flip-delay-ms" => args.flip_delay_ms = value()?.parse()?,
            "--in-flight-calls" => args.in_flight_calls = value()?.parse()?,
            "--inject-delay-ms" => args.inject_delay_ms = value()?.parse()?,
            other => bail!("unknown argument {other}"),
        }
    }
    Ok(args)
}

async fn call_json<T: serde::de::DeserializeOwned>(
    client: &RpcRequestClient,
    method: &str,
    params: &str,
) -> Result<T> {
    client
        .json_result_from_call(method, params.to_string())
        .await
        .map_err(|error| anyhow!("{method} failed: {error}"))
}

/// Polls until getblocktemplate reports it is building on `parent`, or times out.
async fn wait_for_parent(client: &RpcRequestClient, parent: &str, timeout_ms: u64) -> Result<bool> {
    let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms.max(50));
    while std::time::Instant::now() < deadline {
        if let Ok(template) = call_json::<BlockTemplateResponse>(client, "getblocktemplate", "[]").await {
            if template.previous_block_hash().to_string() == parent {
                return Ok(true);
            }
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    Ok(false)
}

async fn best_tip(client: &RpcRequestClient) -> Result<String> {
    call_json(client, "getbestblockhash", "[]").await
}

/// Makes a distinct sibling of `block` on the same parent, re-solving if PoW is on.
fn sibling(block: &Block, network: &Network, salt: u8) -> Result<Block> {
    let mut sibling = block.clone();
    let header = Arc::make_mut(&mut sibling.header);
    header.nonce.0[0] ^= salt;

    if !network.disable_pow() {
        let solved = Solution::solve(header.clone(), || Ok(()))
            .map_err(|_| anyhow!("equihash solver was cancelled"))?;
        *header = solved.first().clone();
    }

    Ok(sibling)
}

async fn submit(client: &RpcRequestClient, block: &Block, label: &str) -> Result<()> {
    let data = hex::encode(block.zcash_serialize_to_vec()?);
    let response = client
        .call("submitblock", format!(r#"["{data}"]"#))
        .await
        .with_context(|| format!("submitting {label}"))?
        .text()
        .await?;
    println!("  submit {label} {} -> {}", block.hash(), response.trim());
    Ok(())
}

async fn round(client: &RpcRequestClient, network: &Network, args: &Args, index: u32) -> Result<()> {
    let before = best_tip(client).await?;

    let template: BlockTemplateResponse = call_json(client, "getblocktemplate", "[]").await?;
    let height = template.height();
    let base = proposal_block_from_template(&template, BlockTemplateTimeSource::default(), network)?;

    // Two siblings, neither of which is the unmodified template block, so the run
    // is symmetric: the control submits exactly the same pair in the other order.
    let left = sibling(&base, network, 0x01)?;
    let right = sibling(&base, network, 0x02)?;

    let (lesser, greater) = if raw_order(left.hash()) < raw_order(right.hash()) {
        (left, right)
    } else {
        (right, left)
    };
    let (first, second, first_label, second_label) = if args.lesser_first {
        (&lesser, &greater, "lesser", "greater")
    } else {
        (&greater, &lesser, "greater", "lesser")
    };

    println!(
        "round {index}: height {height}, tip before {before}, order {}",
        if args.lesser_first { "lesser-first (expect a sideways flip)" } else { "greater-first (control, expect no flip)" }
    );

    submit(client, first, first_label).await?;
    let middle = best_tip(client).await?;
    println!("  tip after first  {middle}");

    // Let templates be built on the first sibling before the second one orphans it.
    tokio::time::sleep(Duration::from_millis(args.flip_delay_ms)).await;

    // Wait until the node actually serves work built on the first sibling. Until
    // it does, every template is still built on the shared parent, and the second
    // submission reads as a forward advance instead of a sideways flip.
    let switched = wait_for_parent(client, &first.hash().to_string(), args.flip_delay_ms).await?;
    println!("  templates now built on {first_label}: {switched}");

    // Fan out concurrent builds, then land the second sibling while they are still
    // running. One build is a coin flip against the build latency; a fan-out makes
    // it near-certain that some are in flight at the moment the tip moves.
    let mut in_flight = Vec::new();
    for _ in 0..args.in_flight_calls {
        let client = client.clone();
        in_flight.push(tokio::spawn(async move {
            call_json::<BlockTemplateResponse>(&client, "getblocktemplate", "[]").await
        }));
    }
    tokio::time::sleep(Duration::from_millis(args.inject_delay_ms)).await;

    submit(client, second, second_label).await?;
    let after = best_tip(client).await?;
    println!("  tip after second {after}");

    let mut served = 0;
    let mut withheld = 0;
    for handle in in_flight {
        match handle.await? {
            Ok(_) => served += 1,
            Err(_) => withheld += 1,
        }
    }
    println!("  in-flight templates: {served} served, {withheld} withheld");

    let flipped = after != middle;
    let expected = args.lesser_first;
    println!("  sideways flip: {flipped} (expected {expected})");
    if flipped != expected {
        bail!("round {index}: expected flip={expected}, observed {flipped}");
    }

    Ok(())
}

/// Orders by the raw internal hash bytes, matching `ChainScore::cmp`.
fn raw_order(hash: Hash) -> [u8; 32] {
    hash.0
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args()?;
    let network = Network::new_regtest(
        ConfiguredActivationHeights {
            nu5: Some(args.nu5_height),
            ..Default::default()
        }
        .into(),
    );
    let client = RpcRequestClient::new(args.rpc);

    for index in 1..=args.rounds {
        round(&client, &network, &args, index).await?;
    }
    println!("all {} rounds behaved as expected", args.rounds);
    Ok(())
}
