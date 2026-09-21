//! External miner for the NU7 fork testnet.
//!
//! The fork is isolated by its own network magic, so no public Testnet miner
//! produces its blocks. This sidecar does, without the deployed `zakurad`
//! needing the `internal-miner` feature: it asks the node for a block template,
//! solves Equihash locally, and submits the block back.
//!
//! # Pacing
//!
//! Proof of work stays enabled on the fork, so blocks must clear the real
//! difficulty — except that Testnet resets difficulty to the network's PoW limit
//! whenever a block arrives more than `target spacing * 6` after its parent (see
//! `NetworkUpgrade::minimum_difficulty_spacing_for_height`). That gap is 450s
//! before NU7 and 150s after it.
//!
//! A solo miner on an isolated fork therefore waits out the gap and then solves
//! a minimum-difficulty block. The wait must happen *before* the template is
//! requested, because the node computes the target from the template's own
//! timestamp.

use std::{path::PathBuf, time::Duration};

use clap::Parser;
use color_eyre::eyre::{bail, eyre, Context, Result};
use tokio::time::sleep;

use zakura_chain::{
    block::{Block, Height},
    parameters::{Network, NetworkUpgrade},
    serialization::ZcashSerialize,
    work::equihash::Solution,
};
use zakura_node_services::rpc_client::RpcRequestClient;
use zakura_rpc::{
    client::{
        BlockTemplateResponse, BlockTemplateTimeSource, GetBlockchainInfoResponse,
        SubmitBlockResponse,
    },
    proposal_block_from_template,
};

/// Multiplier applied to the target spacing to reach the Testnet
/// minimum-difficulty gap, matching `TESTNET_MINIMUM_DIFFICULTY_GAP_MULTIPLIER`.
const MINIMUM_DIFFICULTY_GAP_MULTIPLIER: u32 = 6;

#[derive(Parser, Debug)]
#[command(about, long_about = None)]
struct Args {
    /// The fork node's JSON-RPC address.
    #[arg(long, env = "ZAKURA_FORK_RPC", default_value = "127.0.0.1:18232")]
    rpc: String,

    /// The node's rendered config, read to recover the exact fork `Network`.
    ///
    /// Parsing the node's own config keeps the miner's consensus parameters
    /// identical to the node's, instead of restating the fork's activation
    /// heights in a second place where they could drift.
    #[arg(long, default_value = "/etc/zakura/zakura.toml")]
    config: PathBuf,

    /// Stop after this many blocks. 0 mines until interrupted.
    #[arg(long, default_value_t = 0)]
    blocks: u32,

    /// Extra seconds added to the minimum-difficulty gap before requesting a
    /// template, absorbing clock skew between this miner and the node.
    #[arg(long, default_value_t = 5)]
    gap_margin_secs: u32,

    /// Mine at whatever difficulty the template names, without waiting for the
    /// minimum-difficulty gap. Only practical if the fork's difficulty has
    /// already decayed to the PoW limit.
    #[arg(long)]
    no_gap_wait: bool,
}

/// Recover the fork's `Network` by deserializing the node's own config section.
fn network_from_config(path: &PathBuf) -> Result<Network> {
    let text = std::fs::read_to_string(path)
        .wrap_err_with(|| format!("could not read the node config at {}", path.display()))?;
    let document: toml::Value =
        toml::from_str(&text).wrap_err_with(|| format!("{} is not valid TOML", path.display()))?;
    let section = document
        .get("network")
        .ok_or_else(|| eyre!("{} has no [network] section", path.display()))?
        .clone();
    let config: zakura_network::Config = section
        .try_into()
        .wrap_err("the [network] section did not deserialize into a network config")?;
    Ok(config.network)
}

/// The minimum-difficulty gap that applies to the block after `tip`.
fn minimum_difficulty_gap(network: &Network, tip: Height) -> Duration {
    let next = Height(tip.0.saturating_add(1));
    let spacing = NetworkUpgrade::current(network, next).target_spacing();
    // Safe: `max(0)` clamps away the only values a u64 cannot represent, and
    // every target spacing is a small positive number of seconds.
    let seconds = spacing.num_seconds().max(0) as u64;
    Duration::from_secs(seconds.saturating_mul(u64::from(MINIMUM_DIFFICULTY_GAP_MULTIPLIER)))
}

async fn blockchain_info(client: &RpcRequestClient) -> Result<GetBlockchainInfoResponse> {
    client
        .json_result_from_call("getblockchaininfo", "[]")
        .await
        .map_err(|error| eyre!("getblockchaininfo failed: {error}"))
}

async fn block_from_template(
    client: &RpcRequestClient,
    network: &Network,
) -> Result<(Block, Height)> {
    let template: BlockTemplateResponse = client
        .json_result_from_call("getblocktemplate", "[]".to_string())
        .await
        .map_err(|error| eyre!("getblocktemplate failed: {error}"))?;
    let height = Height(template.height());
    let block =
        proposal_block_from_template(&template, BlockTemplateTimeSource::default(), network)
            .wrap_err("could not build a block from the template")?;
    Ok((block, height))
}

async fn submit_block(client: &RpcRequestClient, block: Block) -> Result<()> {
    let data = hex::encode(block.zcash_serialize_to_vec()?);
    let response: SubmitBlockResponse = client
        .json_result_from_call("submitblock", format!(r#"["{data}"]"#))
        .await
        .map_err(|error| eyre!("submitblock failed: {error}"))?;
    match response {
        SubmitBlockResponse::Accepted => Ok(()),
        SubmitBlockResponse::ErrorResponse(error) => {
            bail!("the node rejected the block: {error:?}")
        }
    }
}

/// Solve Equihash for `block`'s header on a blocking thread.
async fn solve(block: Block) -> Result<Block> {
    tokio::task::spawn_blocking(move || {
        let header = *block.header;
        let solved = Solution::solve(header, || Ok(()))
            .map_err(|_| eyre!("the Equihash solver was cancelled"))?;
        let header = solved
            .into_iter()
            .next()
            .ok_or_else(|| eyre!("the Equihash solver returned no solutions"))?;
        Ok(Block {
            header: header.into(),
            transactions: block.transactions,
        })
    })
    .await
    .wrap_err("the Equihash solver panicked")?
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    let network = network_from_config(&args.config)?;
    let client = RpcRequestClient::new(
        args.rpc
            .parse()
            .wrap_err_with(|| format!("{} is not a socket address", args.rpc))?,
    );

    let activation = NetworkUpgrade::Nu7
        .activation_height(&network)
        .ok_or_else(|| {
            eyre!(
                "{network} has no NU7 activation height; the node config or the \
                 binary is not a NU7 build"
            )
        })?;
    tracing::info!(%network, nu7 = activation.0, "mining the fork");

    let mut mined = 0u32;
    loop {
        let info = blockchain_info(&client).await?;
        let tip = info.blocks();

        if !args.no_gap_wait {
            let gap = minimum_difficulty_gap(&network, tip)
                + Duration::from_secs(u64::from(args.gap_margin_secs));
            tracing::info!(
                tip = tip.0,
                wait_secs = gap.as_secs(),
                "waiting out the minimum-difficulty gap"
            );
            sleep(gap).await;
        }

        let (block, height) = block_from_template(&client, &network).await?;
        tracing::info!(height = height.0, "solving");
        let block = solve(block).await?;
        submit_block(&client, block).await?;

        mined = mined.saturating_add(1);
        let status = if height >= activation {
            "NU7"
        } else {
            "pre-NU7"
        };
        tracing::info!(height = height.0, mined, status, "block accepted");

        if height == activation {
            tracing::info!(height = height.0, "NU7 activated on this fork");
        }
        if args.blocks != 0 && mined >= args.blocks {
            return Ok(());
        }
    }
}
