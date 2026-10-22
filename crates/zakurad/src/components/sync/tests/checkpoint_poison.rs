//! Legacy-sync regression tests for same-hash checkpoint body poisoning.
//!
//! From NU5, transaction IDs do not commit to authorizing data. A peer can change a
//! transparent unlocking script without changing the block hash. The checkpoint verifier accepts
//! that body, and only the state's `hashBlockCommitments` check rejects it.
//!
//! Near the final checkpoint, the state records each checkpoint block hash in its sent-hash set
//! before the write. A rejected write used to leave the hash there, so `Request::KnownBlock` kept
//! reporting the rejected block as known, and a sync round restart stopped the syncer from
//! downloading it again until a process restart. These tests drive a real `ChainSync`, router,
//! and state through that attack.

#![allow(clippy::unwrap_in_result)]

use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
    time::Duration,
};

use eyre::{eyre, Report};
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
    components::sync::{
        ChainSync, BLOCK_VERIFY_TIMEOUT, POISONED_BLOCK_RETRY_LIMIT, SYNC_RESTART_SLEEP,
    },
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
    /// Builds `len` blocks after genesis, with checkpoints at genesis and `checkpoint_heights`.
    fn new(len: u32, checkpoint_heights: &[u32]) -> Result<Self, Report> {
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

        let checkpoints = std::iter::once(0)
            .chain(checkpoint_heights.iter().copied())
            .map(|height| (block::Height(height), blocks[height as usize].hash()))
            .collect();
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
    let fixture = PoisonFixture::new(RANGE_END, &[RANGE_END])?;
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

/// Root cause B: after a restart, the syncer downloads the rejected final-checkpoint block again.
///
/// The forging peer exhausts the poisoned-body retry budget, which restarts the round. Every later
/// `FindBlocks` response starts with the rejected hash. The state used to report that hash as
/// `WriteChannel`, so `ChainSync` treated it as known and scheduled nothing.
#[tokio::test(flavor = "multi_thread")]
async fn rejected_hash_in_final_checkpoint_window_redownloads_after_restart() -> Result<(), Report>
{
    let _init_guard = zakura_test::init();

    let outcome = run_scenario(RANGE_END, true, 3).await?;
    tracing::info!(?outcome, "final checkpoint window outcome");

    assert!(outcome.first_round_restarted, "{outcome:?}");
    assert!(
        outcome.forged_height_requests_before_restart > 1,
        "the syncer retries the forged body before it restarts: {outcome:?}",
    );
    assert!(
        outcome.forged_height_requests_after_restart > 0,
        "the syncer downloads the rejected height again: {outcome:?}",
    );
    assert_eq!(outcome.tip, Some(block::Height(RANGE_END)), "{outcome:?}");
    assert_eq!(
        outcome.known,
        Some(zs::KnownBlock::Finalized),
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

/// The same recovery for a forged block inside the final checkpoint range.
///
/// The forged block's descendants were also in the sent-hash set, so `ChainSync` used to skip the
/// whole rest of the range.
#[tokio::test(flavor = "multi_thread")]
async fn rejected_mid_range_hash_in_final_checkpoint_window_redownloads_after_restart(
) -> Result<(), Report> {
    let _init_guard = zakura_test::init();

    const FORGED: u32 = RANGE_END - 5;
    let outcome = run_scenario(FORGED, true, 3).await?;
    tracing::info!(?outcome, "mid-range outcome");

    assert!(outcome.first_round_restarted, "{outcome:?}");
    assert!(
        outcome.forged_height_requests_after_restart > 0,
        "{outcome:?}"
    );
    assert_eq!(outcome.tip, Some(block::Height(RANGE_END)), "{outcome:?}");
    assert_eq!(
        outcome.known,
        Some(zs::KnownBlock::Finalized),
        "{outcome:?}"
    );

    Ok(())
}

/// The final checkpoint of the generated Mainnet-shaped chain.
const SYBIL_CHAIN_END: u32 = 1_500;

/// The checkpoint spacing of the generated chain. Mainnet checkpoints are about 400 blocks apart.
const SYBIL_CHECKPOINT_SPACING: u32 = 400;

/// The forged block: two blocks after a checkpoint, inside the final checkpoint window. Heights
/// just after a checkpoint stranded descendants most often before the fix.
const SYBIL_FORGED_HEIGHT: u32 = 2 * SYBIL_CHECKPOINT_SPACING + 2;

/// The number of honest peers in the sybil scenarios.
const HONEST_PEERS: usize = 4;

/// How long a sybil scenario may take to reach the final checkpoint.
///
/// Recovery needs no verify timeout, so this is far shorter than `BLOCK_VERIFY_TIMEOUT`. Before
/// the fix, a restarted round never reached the final checkpoint.
const SYBIL_SYNC_LIMIT: Duration = Duration::from_secs(5 * 60);

/// A peer set of honest peers and sybils. Every sybil serves a forged body for one block, and
/// answers each request for that block first, as a low-latency attacker would. Other requests go
/// to a random unbanned peer. The peer set bans each address the syncer reports.
#[derive(Clone)]
struct SybilPeers {
    blocks: Arc<Vec<Arc<Block>>>,
    heights: Arc<HashMap<block::Hash, u32>>,
    forged: Arc<Block>,
    honest: usize,
    sybils: usize,
    banned: Arc<Mutex<HashSet<zn::PeerSocketAddr>>>,
    rng: Arc<Mutex<u64>>,
    forged_bodies_served: Arc<AtomicUsize>,
}

impl SybilPeers {
    fn new(fixture: &PoisonFixture, forged_height: u32, honest: usize, sybils: usize) -> Self {
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
            honest,
            sybils,
            banned: Arc::new(Mutex::new(HashSet::new())),
            rng: Arc::new(Mutex::new(0x9e37_79b9_7f4a_7c15 ^ sybils as u64)),
            forged_bodies_served: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn addr(index: usize) -> zn::PeerSocketAddr {
        let index = u16::try_from(index).expect("the test uses few peers");
        std::net::SocketAddr::from(([10, 0, 0, 1], 10_000 + index)).into()
    }

    fn is_sybil(index: usize, honest: usize) -> bool {
        index >= honest
    }

    /// Bans every address that the syncer reports for misbehavior.
    fn enforce_bans(
        &self,
        mut misbehavior: tokio::sync::mpsc::Receiver<(zn::PeerSocketAddr, u32)>,
    ) {
        let banned = self.banned.clone();
        tokio::spawn(async move {
            while let Some((addr, _score)) = misbehavior.recv().await {
                banned.lock().expect("lock is not poisoned").insert(addr);
            }
        });
    }

    fn banned_sybils(&self) -> usize {
        let banned = self.banned.lock().expect("lock is not poisoned");
        (self.honest..self.honest + self.sybils)
            .filter(|index| banned.contains(&Self::addr(*index)))
            .count()
    }

    fn banned_honest_peers(&self) -> usize {
        let banned = self.banned.lock().expect("lock is not poisoned");
        (0..self.honest)
            .filter(|index| banned.contains(&Self::addr(*index)))
            .count()
    }

    /// Picks a random unbanned peer, or `None` if every candidate is banned.
    ///
    /// A request for the forged block goes to a sybil while any sybil is unbanned.
    fn pick(&self, wants_forged_block: bool) -> Option<usize> {
        let banned = self.banned.lock().expect("lock is not poisoned");
        let unbanned_in = |peers: std::ops::Range<usize>| -> Vec<usize> {
            peers
                .filter(|index| !banned.contains(&Self::addr(*index)))
                .collect()
        };
        let mut unbanned = if wants_forged_block {
            unbanned_in(self.honest..self.honest + self.sybils)
        } else {
            Vec::new()
        };
        if unbanned.is_empty() {
            unbanned = unbanned_in(0..self.honest + self.sybils);
        }
        let mut state = self.rng.lock().expect("lock is not poisoned");
        // xorshift64: deterministic for one sybil count, and good enough to spread requests.
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        // The modulus is below the peer count, so it fits in usize.
        let choice = (*state % unbanned.len().max(1) as u64) as usize;
        unbanned.get(choice).copied()
    }

    fn respond(&self, request: zn::Request) -> Result<zn::Response, BoxError> {
        let wants_forged_block = matches!(&request, zn::Request::BlocksByHash(hashes)
            if hashes.contains(&self.forged.hash()));
        let peer = self
            .pick(wants_forged_block)
            .ok_or("every peer is banned")?;
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
                    let block = if hash == self.forged.hash() && Self::is_sybil(peer, self.honest) {
                        self.forged_bodies_served.fetch_add(1, Ordering::SeqCst);
                        self.forged.clone()
                    } else {
                        self.blocks[height as usize].clone()
                    };
                    blocks.push(InventoryResponse::Available((
                        block,
                        Some(Self::addr(peer)),
                    )));
                }
                Ok(zn::Response::Blocks(blocks))
            }
            request => Err(format!("sybil peers do not answer {request:?}").into()),
        }
    }
}

impl Service<zn::Request> for SybilPeers {
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

/// What a sybil scenario observed.
#[derive(Debug)]
struct SybilOutcome {
    tip: Option<block::Height>,
    restarts: usize,
    forged_bodies_served: usize,
    banned_sybils: usize,
    banned_honest_peers: usize,
}

/// Checkpoint-syncs a Mainnet-shaped chain from honest peers and `sybils` forging peers.
///
/// The chain has checkpoints every 400 blocks. The state uses the production final checkpoint
/// window, which covers the last 1,000 blocks. The test runs sync rounds the way
/// [`ChainSync::sync`] does, until the tip reaches the final checkpoint or the time limit expires.
async fn run_sybil_scenario(sybils: usize) -> Result<SybilOutcome, Report> {
    let checkpoint_heights: Vec<u32> = (SYBIL_CHECKPOINT_SPACING..SYBIL_CHAIN_END)
        .step_by(SYBIL_CHECKPOINT_SPACING as usize)
        .chain(std::iter::once(SYBIL_CHAIN_END))
        .collect();
    let fixture = PoisonFixture::new(SYBIL_CHAIN_END, &checkpoint_heights)?;
    let final_checkpoint = block::Height(SYBIL_CHAIN_END);
    assert!(
        SYBIL_FORGED_HEIGHT as usize
            >= SYBIL_CHAIN_END as usize - CHECKPOINT_VERIFY_CONCURRENCY_LIMIT,
        "the forged block is in the final checkpoint window",
    );

    let (state, _, latest_chain_tip, _) = zs::init(
        zs::Config::ephemeral(),
        &fixture.network,
        final_checkpoint,
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
    assert_eq!(max_checkpoint_height, final_checkpoint);

    let mut config = ZakuradConfig::default();
    config.network.network = fixture.network.clone();
    let peers = SybilPeers::new(&fixture, SYBIL_FORGED_HEIGHT, HONEST_PEERS, sybils);
    let (misbehavior_sender, misbehavior_receiver) = tokio::sync::mpsc::channel(100);
    peers.enforce_bans(misbehavior_receiver);
    let (mut chain_sync, _sync_status) = ChainSync::new(
        &config,
        max_checkpoint_height,
        peers.clone(),
        router,
        state.clone(),
        latest_chain_tip.clone(),
        misbehavior_sender,
    );

    let mut restarts = 0;
    let synced = timeout(SYBIL_SYNC_LIMIT, async {
        chain_sync.request_genesis().await?;
        while latest_chain_tip.best_tip_height() < Some(final_checkpoint) {
            if chain_sync.try_to_sync(None).await.is_err() {
                chain_sync.downloads.cancel_all();
                restarts += 1;
            }
            tracing::info!(
                tip = ?latest_chain_tip.best_tip_height(),
                restarts,
                "sybil sync round finished"
            );
            if latest_chain_tip.best_tip_height() < Some(final_checkpoint) {
                tokio::time::sleep(SYNC_RESTART_SLEEP).await;
            }
        }
        Ok::<_, Report>(())
    })
    .await;
    if let Ok(result) = synced {
        result?;
    }

    Ok(SybilOutcome {
        tip: latest_chain_tip.best_tip_height(),
        restarts,
        forged_bodies_served: peers.forged_bodies_served.load(Ordering::SeqCst),
        banned_sybils: peers.banned_sybils(),
        banned_honest_peers: peers.banned_honest_peers(),
    })
}

/// Checks that `sybils` forging peers cannot stop checkpoint sync. Each sybil serves the forged
/// body until its ban lands. More sybils than the poisoned-body retry budget restart the round.
async fn assert_sybils_cannot_stall_checkpoint_sync(sybils: usize) -> Result<(), Report> {
    let outcome = run_sybil_scenario(sybils).await?;
    tracing::info!(?outcome, sybils, "sybil scenario outcome");

    // Bans land asynchronously, so a sybil can serve the forged body more than once.
    assert!(
        outcome.forged_bodies_served >= sybils,
        "every sybil serves the forged body: {outcome:?}"
    );
    assert_eq!(outcome.banned_sybils, sybils, "{outcome:?}");
    assert_eq!(
        outcome.restarts > 0,
        sybils > POISONED_BLOCK_RETRY_LIMIT,
        "only an exhausted retry budget restarts the round: {outcome:?}",
    );
    assert_eq!(
        outcome.tip,
        Some(block::Height(SYBIL_CHAIN_END)),
        "{sybils} sybils cannot stop checkpoint sync: {outcome:?}",
    );
    assert_eq!(outcome.banned_honest_peers, 0, "{outcome:?}");

    Ok(())
}

/// One forging peer among honest peers cannot stop checkpoint sync inside the final window.
#[tokio::test(flavor = "multi_thread")]
async fn one_sybil_cannot_stall_final_window_checkpoint_sync() -> Result<(), Report> {
    let _init_guard = zakura_test::init();
    assert_sybils_cannot_stall_checkpoint_sync(1).await
}

/// Three forging peers cannot stop checkpoint sync inside the final window.
#[tokio::test(flavor = "multi_thread")]
async fn three_sybils_cannot_stall_final_window_checkpoint_sync() -> Result<(), Report> {
    let _init_guard = zakura_test::init();
    assert_sybils_cannot_stall_checkpoint_sync(3).await
}

/// Nine forging peers can exhaust the poisoned-body retry budget and restart the round. That
/// restart used to stall checkpoint sync until a process restart.
#[tokio::test(flavor = "multi_thread")]
async fn nine_sybils_cannot_stall_final_window_checkpoint_sync() -> Result<(), Report> {
    let _init_guard = zakura_test::init();
    assert_sybils_cannot_stall_checkpoint_sync(9).await
}

/// Keeps `BLOCK_VERIFY_TIMEOUT` referenced, since a stranded body in these scenarios would wait
/// for it instead of `ROUND_LIMIT`.
#[test]
fn round_limit_is_shorter_than_the_verify_timeout() {
    assert!(ROUND_LIMIT < BLOCK_VERIFY_TIMEOUT);
}
