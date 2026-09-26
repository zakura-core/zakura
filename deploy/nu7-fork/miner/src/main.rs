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
//! By default, a solo miner waits out the gap and then solves a
//! minimum-difficulty block. The wait must happen *before* the template is
//! requested, because the node computes the target from the template's own
//! timestamp. `--no-gap-wait` mines continuously and refreshes its template
//! as the tip and block time change.

use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use clap::Parser;
use color_eyre::eyre::{bail, eyre, Context, Result};
use tokio::time::{interval, sleep, Instant};

use zakura_chain::{
    block::{Block, Height},
    parameters::{Network, NetworkUpgrade},
    serialization::ZcashSerialize,
    transaction::Transaction,
    work::equihash::{Solution, SolverCancelled},
};
use zakura_node_services::rpc_client::RpcRequestClient;
use zakura_rpc::{
    client::{BlockTemplateResponse, BlockTemplateTimeSource, SubmitBlockResponse},
    proposal_block_from_template,
};

/// Multiplier applied to the target spacing to reach the Testnet
/// minimum-difficulty gap, matching `TESTNET_MINIMUM_DIFFICULTY_GAP_MULTIPLIER`.
const MINIMUM_DIFFICULTY_GAP_MULTIPLIER: u32 = 6;
const TIP_POLL_INTERVAL: Duration = Duration::from_secs(1);

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

    /// Mine continuously, refreshing work when the tip or template time changes.
    #[arg(long)]
    no_gap_wait: bool,

    /// Claim this many extra zatoshi in the coinbase, over what the subsidy and the
    /// miner's fee share allow.
    ///
    /// This is an attack, used to check that consensus rejects a miner taking the NSM
    /// share of the block's fees. A correct node rejects any non-zero value.
    #[arg(long, default_value_t = 0)]
    steal_zats: u64,

    /// Maximum age of a mining template in continuous mode, in seconds.
    #[arg(long, default_value_t = 15)]
    template_refresh_secs: u64,

    /// Unique nonce prefix for this miner when multiple miners share a template.
    #[arg(long, default_value_t = 0)]
    solver_id: u8,
}

/// Inflates the coinbase's first output by `zats` and repairs the merkle root.
///
/// The merkle root has to be recomputed, otherwise the block is rejected for an
/// inconsistent root and never reaches the fee check being tested.
fn steal_fees(block: Block, zats: u64) -> Result<Block> {
    use zakura_chain::{amount::Amount, block::merkle};

    let Block {
        header,
        mut transactions,
    } = block;

    let coinbase = std::sync::Arc::make_mut(
        transactions
            .first_mut()
            .ok_or_else(|| eyre!("the block has no coinbase transaction"))?,
    );

    let outputs = match coinbase {
        Transaction::V5 { outputs, .. } | Transaction::V6 { outputs, .. } => outputs,
        _ => bail!("the coinbase is not a V5 or V6 transaction"),
    };
    let output = outputs
        .first_mut()
        .ok_or_else(|| eyre!("the coinbase has no outputs"))?;
    output.value = (output.value + Amount::try_from(i64::try_from(zats)?)?)
        .map_err(|error| eyre!("the inflated coinbase output overflows: {error:?}"))?;

    let merkle_root = transactions.iter().collect::<merkle::Root>();
    let mut header = *header;
    header.merkle_root = merkle_root;

    Ok(Block {
        header: std::sync::Arc::new(header),
        transactions,
    })
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

/// Read the tip height with `getblockcount` rather than `getblockchaininfo`.
///
/// `getblockchaininfo` renders every value pool balance as an f64 `chainValue`.
/// Some zatoshi amounts have no exact f64 form -- after NU7 the lockbox pool is
/// one -- so the node emits a response its own `GetBlockchainInfoResponse`
/// deserializer refuses with "floating point had fractional zatoshis". The
/// miner only needs the height, and `getblockcount` returns a plain integer.
async fn tip_height(client: &RpcRequestClient) -> Result<Height> {
    let count: u32 = client
        .json_result_from_call("getblockcount", "[]".to_string())
        .await
        .map_err(|error| eyre!("getblockcount failed: {error}"))?;
    Ok(Height(count))
}

/// Include the hash so a same-height reorganization also cancels old work.
async fn tip_hash(client: &RpcRequestClient) -> Result<String> {
    client
        .json_result_from_call("getbestblockhash", "[]".to_string())
        .await
        .map_err(|error| eyre!("getbestblockhash failed: {error}"))
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

/// Solve Equihash for `block`'s header until a solution is found or cancelled.
fn start_solver(
    block: Block,
    cancel: Arc<AtomicBool>,
    solver_id: u8,
    solver_round: u64,
) -> tokio::task::JoinHandle<Result<Block, SolverCancelled>> {
    tokio::task::spawn_blocking(move || {
        let mut header = *block.header;
        *header.nonce = solver_nonce(solver_id, solver_round);
        let solved = Solution::solve(header, || {
            if cancel.load(Ordering::Relaxed) {
                Err(SolverCancelled)
            } else {
                Ok(())
            }
        })?;
        let header = solved
            .into_iter()
            .next()
            .expect("Equihash solver returns at least one header by its return type");
        Ok(Block {
            header: header.into(),
            transactions: block.transactions,
        })
    })
}

/// Start a distinct nonce range even when a refresh returns an unchanged template.
fn solver_nonce(solver_id: u8, solver_round: u64) -> [u8; 32] {
    let mut nonce = [0; 32];
    nonce[0] = solver_id;
    nonce[1..9].copy_from_slice(&solver_round.to_be_bytes());
    // The solver increments the nonce in big-endian order, leaving 23 bytes
    // for work within this round before it could overlap the next range.
    nonce
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
    if !(1..=300).contains(&args.template_refresh_secs) {
        bail!("template-refresh-secs must be between 1 and 300 seconds");
    }
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
    let mut solver_round = 0u64;
    'mining: loop {
        let tip = tip_height(&client).await?;
        let parent_hash = tip_hash(&client).await?;

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
        if height.0 != tip.0.saturating_add(1) || tip_hash(&client).await? != parent_hash {
            tracing::info!("tip changed while requesting a mining template");
            continue;
        }
        let block = if args.steal_zats > 0 {
            tracing::warn!(
                steal_zats = args.steal_zats,
                "inflating the coinbase: consensus must reject this block"
            );
            steal_fees(block, args.steal_zats)?
        } else {
            block
        };
        tracing::info!(height = height.0, "solving");
        let cancel = Arc::new(AtomicBool::new(false));
        solver_round = solver_round
            .checked_add(1)
            .ok_or_else(|| eyre!("the miner exhausted its nonce ranges"))?;
        let mut solver = start_solver(block, cancel.clone(), args.solver_id, solver_round);
        let refresh_at = Instant::now() + Duration::from_secs(args.template_refresh_secs);
        let mut tip_poll = interval(TIP_POLL_INTERVAL);
        tip_poll.tick().await;

        let block = loop {
            tokio::select! {
                result = &mut solver => {
                    let result = result.wrap_err("the Equihash solver panicked")?;
                    break result.map_err(|_| eyre!("the Equihash solver was cancelled"))?;
                }
                _ = tip_poll.tick(), if args.no_gap_wait => {
                    let tip_changed = tip_hash(&client).await? != parent_hash;
                    if tip_changed || Instant::now() >= refresh_at {
                        cancel.store(true, Ordering::Relaxed);
                        let _ = solver.await.wrap_err("the Equihash solver panicked")?;
                        tracing::info!(
                            height = height.0,
                            tip_changed,
                            "refreshing mining template"
                        );
                        continue 'mining;
                    }
                }
            }
        };

        if tip_hash(&client).await? != parent_hash {
            tracing::info!(height = height.0, "tip changed before block submission");
            continue;
        }
        if let Err(error) = submit_block(&client, block).await {
            if tip_hash(&client).await? != parent_hash {
                tracing::info!(height = height.0, "tip changed during block submission");
                continue;
            }
            return Err(error);
        }

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refreshed_templates_use_distinct_nonce_ranges() {
        let first = solver_nonce(1, 1);
        let refreshed = solver_nonce(1, 2);
        let other_miner = solver_nonce(2, 1);
        assert_ne!(first, refreshed);
        assert_ne!(first, other_miner);
        // Even exhausting a round's low bytes cannot reach the next range.
        let mut last_in_round = first;
        last_in_round[9..].fill(u8::MAX);
        assert!(last_in_round < refreshed);
        assert_eq!(&solver_nonce(1, u64::MAX)[1..9], &u64::MAX.to_be_bytes());
    }

    /// The deployer's rendered fork node config, kept in sync with the renderer by
    /// `test_fork.py`. Loading it here proves zakurad accepts the shape the deployer
    /// writes, which a TOML parser alone cannot.
    #[test]
    fn rendered_fork_config_loads_in_zakurad() -> Result<()> {
        let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("testdata/fork-node.toml");

        let network = network_from_config(&fixture)?;

        assert_eq!(network.to_string(), "Nu7Fork");
        assert!(!network.is_default_testnet());
        assert_eq!(
            NetworkUpgrade::Nu7.activation_height(&network),
            Some(Height(4_400_010)),
        );
        // The public Testnet upgrades survive: a partial activation list would
        // silently drop them.
        assert_eq!(
            NetworkUpgrade::Nu6_3.activation_height(&network),
            Some(Height(4_134_000)),
        );
        Ok(())
    }
}
