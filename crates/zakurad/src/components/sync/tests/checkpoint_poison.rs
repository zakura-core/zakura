//! Legacy-sync reproductions for same-hash checkpoint body poisoning.
//!
//! From NU5, transaction IDs do not commit to authorizing data. A peer can change a
//! transparent unlocking script without changing the block hash. The checkpoint verifier accepts
//! that body, and only the state's `hashBlockCommitments` check rejects it.
//!
//! Near the final checkpoint, the state records each checkpoint block hash in its sent-hash set
//! before the write. A rejected write does not remove the hash, so `Request::KnownBlock` keeps
//! reporting the rejected block as known. These tests drive a real `ChainSync`, router, and state
//! to check whether a round restart then stops the syncer from downloading the block again.

#![allow(clippy::unwrap_in_result)]

use std::{
    collections::HashMap,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
    time::Duration,
};

use color_eyre::eyre::{eyre, Report};
use futures::{future::BoxFuture, Future};
use tokio::time::timeout;
use tower::{buffer::Buffer, util::BoxService, Service, ServiceExt};

use zakura_chain::{
    amount::Amount,
    block::{
        self, genesis::regtest_genesis_block, Block, ChainHistoryBlockTxAuthCommitmentHash,
        CHAIN_HISTORY_ACTIVATION_RESERVED,
    },
    chain_tip::ChainTip,
    history_tree::HistoryTree,
    parameters::{
        testnet::{ConfiguredActivationHeights, ConfiguredCheckpoints, Parameters},
        Network, NetworkUpgrade,
    },
    transaction::{LockTime, Transaction},
    transparent,
};
use zakura_network::{self as zn, InventoryResponse};
use zakura_state as zs;

use crate::{
    components::sync::{ChainSync, BLOCK_VERIFY_TIMEOUT},
    config::ZakuradConfig,
    BoxError,
};

/// The production default for `sync.checkpoint_verify_concurrency_limit`.
const CHECKPOINT_VERIFY_CONCURRENCY_LIMIT: usize = 1000;

/// The last block in the generated chain, which is also the final checkpoint.
const RANGE_END: u32 = 20;

/// A generous bound for one sync round in this small chain.
const ROUND_LIMIT: Duration = Duration::from_secs(120);

/// A generated NU5 chain whose blocks spend the previous block's coinbase output.
struct PoisonFixture {
    network: Network,
    blocks: Vec<Arc<Block>>,
}

impl PoisonFixture {
    /// Builds `len` blocks after genesis, with checkpoints at genesis and `len`.
    fn new(len: u32) -> Result<Self, Report> {
        let genesis = regtest_genesis_block();
        let target_difficulty_limit = genesis
            .header
            .difficulty_threshold
            .to_expanded()
            .ok_or_else(|| eyre!("regtest genesis difficulty threshold is valid"))?;
        let build = |checkpoints: Vec<(block::Height, block::Hash)>| {
            Parameters::build()
                .with_network_name("PoisonTest")?
                .with_genesis_hash(genesis.hash())?
                .with_activation_heights(ConfiguredActivationHeights {
                    nu5: Some(1),
                    ..Default::default()
                })?
                .clear_funding_streams()
                .with_slow_start_interval(block::Height::MIN)
                .with_disable_pow(true)
                .with_target_difficulty_limit(target_difficulty_limit)?
                .with_checkpoints(ConfiguredCheckpoints::HeightsAndHashes(checkpoints))?
                .to_network()
                .map_err(|error| eyre!("{error:?}"))
        };

        // History tree updates depend only on the activation heights.
        let chain_network = build(vec![(block::Height(0), genesis.hash())])?;

        let sapling_root = zakura_chain::sapling::tree::NoteCommitmentTree::default().root();
        let orchard_root = zakura_chain::orchard::tree::NoteCommitmentTree::default().root();
        let ironwood_root = zakura_chain::ironwood::tree::NoteCommitmentTree::default().root();

        let mut history_tree = HistoryTree::default();
        let mut blocks = vec![genesis.clone()];
        for height in 1..=len {
            let parent = blocks.last().expect("genesis is present").clone();
            let mut child = Block::clone(&parent);

            let mut coinbase = child.transactions[0].clone();
            match Arc::make_mut(&mut coinbase).inputs_mut().get_mut(0) {
                Some(transparent::Input::Coinbase { height: h, .. }) => *h = block::Height(height),
                _ => return Err(eyre!("the regtest genesis transaction is a coinbase")),
            }
            child.transactions = vec![coinbase];

            // Genesis outputs are unspendable, so spending starts at height 2.
            if height >= 2 {
                let outpoint = transparent::OutPoint {
                    hash: parent.transactions[0].hash(),
                    index: 0,
                };
                let spend = Transaction::V5 {
                    network_upgrade: NetworkUpgrade::Nu5,
                    lock_time: LockTime::unlocked(),
                    expiry_height: block::Height(0),
                    inputs: vec![transparent::Input::PrevOut {
                        outpoint,
                        unlock_script: transparent::Script::new(&[0x51]),
                        sequence: u32::MAX,
                    }],
                    outputs: vec![transparent::Output::new(
                        Amount::try_from(0)?,
                        transparent::Script::new(&[0x51]),
                    )],
                    sapling_shielded_data: None,
                    orchard_shielded_data: None,
                };
                child.transactions.push(Arc::new(spend));
            }

            let header = Arc::make_mut(&mut child.header);
            header.previous_block_hash = parent.hash();
            header.merkle_root = child.transactions.iter().cloned().collect();

            let history_root = if height == 1 {
                CHAIN_HISTORY_ACTIVATION_RESERVED.into()
            } else {
                history_tree
                    .hash()
                    .ok_or_else(|| eyre!("the history tree starts at height 1"))?
            };
            let commitment: [u8; 32] = ChainHistoryBlockTxAuthCommitmentHash::from_commitments(
                &history_root,
                &child.auth_data_root(),
            )
            .into();
            Arc::make_mut(&mut child.header).commitment_bytes = commitment.into();

            let child = Arc::new(child);
            history_tree.push(
                &chain_network,
                child.clone(),
                &sapling_root,
                &orchard_root,
                &ironwood_root,
            )?;
            blocks.push(child);
        }

        let checkpoints = vec![
            (block::Height(0), blocks[0].hash()),
            (block::Height(len), blocks[len as usize].hash()),
        ];
        let network = build(checkpoints)?;

        Ok(Self { network, blocks })
    }

    /// Returns `blocks[height]` with a changed unlocking script.
    ///
    /// The change keeps the transaction ID and the block hash, but changes the auth-data root.
    fn forge(&self, height: u32) -> Arc<Block> {
        let honest = &self.blocks[height as usize];
        let mut forged = Block::clone(honest);
        let spend = Arc::make_mut(&mut forged.transactions[1]);
        let Some(transparent::Input::PrevOut { unlock_script, .. }) = spend.inputs_mut().get_mut(0)
        else {
            unreachable!("blocks above height 1 have a transparent spend");
        };
        *unlock_script = transparent::Script::new(&[0x52]);
        let forged = Arc::new(forged);

        assert_eq!(
            forged.hash(),
            honest.hash(),
            "the forgery keeps the block hash"
        );
        assert_ne!(
            forged.auth_data_root(),
            honest.auth_data_root(),
            "the forgery changes the auth-data root",
        );
        forged
    }
}

/// A peer set that serves the fixture chain, and a forged body while forging is enabled.
#[derive(Clone)]
struct FixturePeers {
    blocks: Arc<Vec<Arc<Block>>>,
    heights: Arc<HashMap<block::Hash, u32>>,
    forged: Arc<Block>,
    forging: Arc<AtomicBool>,
    /// The number of `BlocksByHash` requests for each height.
    requests: Arc<Mutex<HashMap<u32, usize>>>,
}

impl FixturePeers {
    fn new(fixture: &PoisonFixture, forged_height: u32) -> Self {
        let heights = fixture
            .blocks
            .iter()
            .enumerate()
            .map(|(height, block)| (block.hash(), height as u32))
            .collect();
        Self {
            blocks: Arc::new(fixture.blocks.clone()),
            heights: Arc::new(heights),
            forged: fixture.forge(forged_height),
            forging: Arc::new(AtomicBool::new(true)),
            requests: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn requests(&self, height: u32) -> usize {
        self.requests
            .lock()
            .expect("lock is not poisoned")
            .get(&height)
            .copied()
            .unwrap_or_default()
    }

    fn respond(&self, request: zn::Request) -> Result<zn::Response, BoxError> {
        match request {
            zn::Request::FindBlocks { known_blocks, .. } => {
                let start = known_blocks
                    .iter()
                    .find_map(|hash| self.heights.get(hash))
                    .map_or(1, |height| height + 1) as usize;
                let hashes = self
                    .blocks
                    .iter()
                    .skip(start)
                    .take(500)
                    .map(|block| block.hash())
                    .collect();
                Ok(zn::Response::BlockHashes(hashes))
            }
            zn::Request::BlocksByHash(hashes) => {
                let mut blocks = Vec::new();
                for hash in hashes {
                    let Some(&height) = self.heights.get(&hash) else {
                        blocks.push(InventoryResponse::Missing(hash));
                        continue;
                    };
                    *self
                        .requests
                        .lock()
                        .expect("lock is not poisoned")
                        .entry(height)
                        .or_default() += 1;
                    let block = if hash == self.forged.hash() && self.forging.load(Ordering::SeqCst)
                    {
                        self.forged.clone()
                    } else {
                        self.blocks[height as usize].clone()
                    };
                    blocks.push(InventoryResponse::Available((block, None)));
                }
                Ok(zn::Response::Blocks(blocks))
            }
            request => Err(format!("fixture peers do not answer {request:?}").into()),
        }
    }
}

impl Service<zn::Request> for FixturePeers {
    type Response = zn::Response;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<zn::Response, BoxError>> + Send + 'static>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: zn::Request) -> Self::Future {
        let response = self.respond(request);
        Box::pin(async move { response })
    }
}

/// What a scenario observed after the forging peer stopped.
#[derive(Debug)]
struct Outcome {
    first_round_restarted: bool,
    forged_height_requests_before_restart: usize,
    forged_height_requests_after_restart: usize,
    tip: Option<block::Height>,
    known: Option<zs::KnownBlock>,
}

/// Runs a sync round with a forging peer, then `honest_rounds` rounds with only honest peers.
///
/// If `final_checkpoint_window` is true, the state treats every height as close to the final
/// checkpoint, as it does for the last `checkpoint_verify_concurrency_limit` checkpoint blocks in
/// production. Otherwise the state never tracks checkpoint block hashes.
async fn run_scenario(
    forged_height: u32,
    final_checkpoint_window: bool,
    honest_rounds: usize,
) -> Result<Outcome, Report> {
    let fixture = PoisonFixture::new(RANGE_END)?;
    let state_max_checkpoint = if final_checkpoint_window {
        block::Height(RANGE_END)
    } else {
        block::Height::MAX
    };
    let (state, _, latest_chain_tip, _) = zs::init(
        zs::Config::ephemeral(),
        &fixture.network,
        state_max_checkpoint,
        CHECKPOINT_VERIFY_CONCURRENCY_LIMIT,
    )
    .await
    .map_err(|error| eyre!("{error:?}"))?;
    let state: Buffer<zs::Request, BoxFuture<'static, Result<zs::Response, BoxError>>> =
        Buffer::new(BoxService::new(state), 50);

    let (router, _, _, max_checkpoint_height) = zakura_consensus::router::init_test(
        zakura_consensus::Config::default(),
        &fixture.network,
        state.clone(),
    )
    .await;
    assert_eq!(max_checkpoint_height, block::Height(RANGE_END));

    let mut config = ZakuradConfig::default();
    config.network.network = fixture.network.clone();
    let peers = FixturePeers::new(&fixture, forged_height);
    let (misbehavior_sender, _misbehavior_receiver) = tokio::sync::mpsc::channel(100);
    let (mut chain_sync, _sync_status) = ChainSync::new(
        &config,
        max_checkpoint_height,
        peers.clone(),
        router,
        state.clone(),
        latest_chain_tip.clone(),
        misbehavior_sender,
    );

    timeout(ROUND_LIMIT, chain_sync.request_genesis()).await??;

    // The forging peer answers every request for the forged height until the round restarts.
    let first_round = timeout(ROUND_LIMIT, chain_sync.try_to_sync(None))
        .await
        .map_err(|_| eyre!("the first round did not finish"))?;
    tracing::info!(?first_round, "first sync round finished");
    // `ChainSync::sync` cancels the round's downloads after an error, before it restarts.
    if first_round.is_err() {
        chain_sync.downloads.cancel_all();
    }
    peers.forging.store(false, Ordering::SeqCst);
    let forged_height_requests_before_restart = peers.requests(forged_height);

    for round in 0..honest_rounds {
        let result = timeout(ROUND_LIMIT, chain_sync.try_to_sync(None))
            .await
            .map_err(|_| eyre!("honest round {round} did not finish"))?;
        tracing::info!(?round, ?result, tip = ?latest_chain_tip.best_tip_height(), "honest sync round finished");
        if result.is_err() {
            chain_sync.downloads.cancel_all();
        }
    }

    let known = match state
        .clone()
        .oneshot(zs::Request::KnownBlock(
            fixture.blocks[forged_height as usize].hash(),
        ))
        .await
        .map_err(|error| eyre!(error))?
    {
        zs::Response::KnownBlock(known) => known,
        response => return Err(eyre!("unexpected KnownBlock response {response:?}")),
    };

    Ok(Outcome {
        first_round_restarted: first_round.is_err(),
        forged_height_requests_before_restart,
        forged_height_requests_after_restart: peers.requests(forged_height)
            - forged_height_requests_before_restart,
        tip: latest_chain_tip.best_tip_height(),
        known,
    })
}

/// Root cause B: after a restart, the syncer never downloads the rejected final-checkpoint block.
///
/// The forging peer exhausts the poisoned-body retry budget, which restarts the round. Every later
/// `FindBlocks` response starts with the rejected hash, which the state still reports as
/// `WriteChannel`, so `ChainSync` treats it as known and schedules nothing.
#[tokio::test(flavor = "multi_thread")]
async fn rejected_hash_in_final_checkpoint_window_blocks_redownload_after_restart(
) -> Result<(), Report> {
    let _init_guard = zakura_test::init();

    let outcome = run_scenario(RANGE_END, true, 3).await?;
    tracing::info!(?outcome, "final checkpoint window outcome");

    assert!(outcome.first_round_restarted, "{outcome:?}");
    assert!(
        outcome.forged_height_requests_before_restart > 1,
        "the syncer retries the forged body before it restarts: {outcome:?}",
    );
    assert_eq!(
        outcome.forged_height_requests_after_restart, 0,
        "the syncer never downloads the rejected height again: {outcome:?}",
    );
    assert_eq!(
        outcome.tip,
        Some(block::Height(RANGE_END - 1)),
        "{outcome:?}"
    );
    assert_eq!(
        outcome.known,
        Some(zs::KnownBlock::WriteChannel),
        "{outcome:?}"
    );

    Ok(())
}

/// Control for the test above: outside the final checkpoint window, the restart re-downloads
/// the honest body and checkpoint sync completes.
#[tokio::test(flavor = "multi_thread")]
async fn rejected_hash_outside_final_checkpoint_window_redownloads_after_restart(
) -> Result<(), Report> {
    let _init_guard = zakura_test::init();

    let outcome = run_scenario(RANGE_END, false, 3).await?;
    tracing::info!(?outcome, "outside final checkpoint window outcome");

    assert!(outcome.first_round_restarted, "{outcome:?}");
    assert!(
        outcome.forged_height_requests_after_restart > 0,
        "{outcome:?}"
    );
    assert_eq!(outcome.tip, Some(block::Height(RANGE_END)), "{outcome:?}");

    Ok(())
}

/// The same lockout for a forged block inside the final checkpoint range.
///
/// The forged block's descendants are also in the sent-hash set, so `ChainSync` skips the whole
/// rest of the range.
#[tokio::test(flavor = "multi_thread")]
async fn rejected_mid_range_hash_in_final_checkpoint_window_blocks_redownload_after_restart(
) -> Result<(), Report> {
    let _init_guard = zakura_test::init();

    const FORGED: u32 = RANGE_END - 5;
    let outcome = run_scenario(FORGED, true, 3).await?;
    tracing::info!(?outcome, "mid-range outcome");

    assert!(outcome.first_round_restarted, "{outcome:?}");
    assert_eq!(
        outcome.forged_height_requests_after_restart, 0,
        "{outcome:?}"
    );
    assert_eq!(outcome.tip, Some(block::Height(FORGED - 1)), "{outcome:?}");
    assert_eq!(
        outcome.known,
        Some(zs::KnownBlock::WriteChannel),
        "{outcome:?}"
    );

    Ok(())
}

/// Keeps `BLOCK_VERIFY_TIMEOUT` referenced, since a stranded body in these scenarios would wait
/// for it instead of `ROUND_LIMIT`.
#[test]
fn round_limit_is_shorter_than_the_verify_timeout() {
    assert!(ROUND_LIMIT < BLOCK_VERIFY_TIMEOUT);
}
