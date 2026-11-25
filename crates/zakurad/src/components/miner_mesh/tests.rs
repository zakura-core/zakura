//! How the tracker reads the window, follows the tip and repairs itself after
//! reorgs, against a best chain the tests rewrite.

use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use chrono::{DateTime, Utc};
use tokio::sync::watch;
use tower::Service;
use zakura_chain::{
    block::{self, Block, Height},
    chain_tip::mock::MockChainTip,
    miner_mesh::{
        GatewayBlocks, GatewayId, MinerMeshCommitment, MinerSetSnapshot, QUALIFYING_DEPTH,
    },
    parameters::Network,
    serialization::ZcashDeserializeInto,
    transaction::{self, Transaction},
};
use zakura_network::zakura::MinerGatewayRanking;
use zakura_state::{
    ChainTipBlock, ChainTipSender, HashOrHeight, MinedTx, ReadRequest, ReadResponse,
};
use zakura_test::vectors::BLOCK_MAINNET_1_BYTES;

use super::{
    ranking::{ranking_channel, MIN_GATEWAY_BLOCKS},
    tracker::{gateway_metrics, run, GatewayMetrics, TipMove, Tracker, REPUBLISH_INTERVAL},
    LocalGateway,
};
use crate::BoxError;

/// The tip used by most tests. On Regtest, whose blocks are 75 seconds apart,
/// its window is the 51 heights from genesis to 50.
const TIP: u32 = 50 + QUALIFYING_DEPTH;

/// One block of the fake best chain.
#[derive(Copy, Clone)]
struct FakeBlock {
    hash: block::Hash,
    /// Whether the block's transactions were pruned. Its hash stays readable.
    pruned: bool,
    /// The first byte of the gateway ID the coinbase names, if it names one.
    gateway: Option<u8>,
}

/// A best chain the tests rewrite, answering the reads the tracker makes.
///
/// A block's coinbase transaction ID is its block hash, so the chain can find
/// the coinbase from either.
#[derive(Clone, Default)]
struct FakeChain {
    blocks: Arc<Mutex<BTreeMap<Height, FakeBlock>>>,
    /// Whether every read fails.
    failing: Arc<AtomicBool>,
    /// The heights of the best-chain hash reads, in the order they were made.
    hash_reads: Arc<Mutex<Vec<Height>>>,
    /// Runs once, when the tracker first reads the best-chain hash at this
    /// height, before the read is answered.
    on_hash_read: Arc<Mutex<Option<OnHashRead>>>,
}

type OnHashRead = (Height, Box<dyn FnOnce() + Send>);

impl FakeChain {
    /// Puts the blocks of `fork` at `heights` on the best chain, each naming
    /// `gateway`.
    fn set(&self, heights: impl IntoIterator<Item = u32>, fork: u8, gateway: Option<u8>) {
        let mut blocks = self
            .blocks
            .lock()
            .expect("no test thread panics while holding it");
        for height in heights {
            blocks.insert(
                Height(height),
                FakeBlock {
                    hash: hash(fork, height),
                    pruned: false,
                    gateway,
                },
            );
        }
    }

    /// Drops the transactions of the block at `height`, as pruning does. The
    /// best chain still lists the block's hash.
    fn prune(&self, height: u32) {
        if let Some(block) = self
            .blocks
            .lock()
            .expect("no test thread panics while holding it")
            .get_mut(&Height(height))
        {
            block.pruned = true;
        }
    }

    fn fail_reads(&self) {
        self.failing.store(true, Ordering::Relaxed);
    }

    /// Returns the heights of the best-chain hash reads so far, and forgets them.
    fn take_hash_reads(&self) -> Vec<u32> {
        std::mem::take(
            &mut *self
                .hash_reads
                .lock()
                .expect("no test thread panics while holding it"),
        )
        .into_iter()
        .map(|height| height.0)
        .collect()
    }

    fn on_hash_read(&self, height: u32, action: impl FnOnce() + Send + 'static) {
        *self
            .on_hash_read
            .lock()
            .expect("no test thread panics while holding it") =
            Some((Height(height), Box::new(action)));
    }

    fn service(
        &self,
    ) -> impl Service<
        ReadRequest,
        Response = ReadResponse,
        Error = BoxError,
        Future = impl Send + 'static,
    > + Clone
           + Send
           + 'static {
        let chain = self.clone();
        tower::service_fn(move |request| {
            let response = chain.answer(request);
            async move { response }
        })
    }

    fn answer(&self, request: ReadRequest) -> Result<ReadResponse, BoxError> {
        if self.failing.load(Ordering::Relaxed) {
            return Err("the state is unavailable".into());
        }

        if let ReadRequest::BestChainBlockHash(height) = request {
            self.hash_reads
                .lock()
                .map_err(|_| "a test thread panicked while holding the read log")?
                .push(height);
            let action = self
                .on_hash_read
                .lock()
                .map_err(|_| "a test thread panicked while holding the read action")?
                .take_if(|(at, _)| *at == height);
            if let Some((_, action)) = action {
                action();
            }
        }

        let blocks = self
            .blocks
            .lock()
            .map_err(|_| "a test thread panicked while holding the fake chain")?;
        let by_hash = |hash: block::Hash| blocks.values().find(|block| block.hash == hash);

        Ok(match request {
            ReadRequest::BestChainBlockHash(height) => {
                ReadResponse::BlockHash(blocks.get(&height).map(|block| block.hash))
            }
            ReadRequest::TransactionIdsForBlock(HashOrHeight::Hash(hash)) => {
                ReadResponse::TransactionIdsForBlock(
                    by_hash(hash)
                        .filter(|block| !block.pruned)
                        .map(|block| Arc::from([transaction::Hash(block.hash.0)])),
                )
            }
            ReadRequest::Transaction(id) => ReadResponse::Transaction(
                by_hash(block::Hash(id.0)).map(|block| mined_coinbase(block.gateway)),
            ),
            request => {
                return Err(format!("the tracker made an unexpected read: {request:?}").into())
            }
        })
    }
}

/// The hash of the block at `height` on `fork`.
fn hash(fork: u8, height: u32) -> block::Hash {
    let mut bytes = [fork; 32];
    bytes[..4].copy_from_slice(&height.to_le_bytes());
    block::Hash(bytes)
}

/// A coinbase that names the gateway whose ID starts with `gateway`, if any.
fn mined_coinbase(gateway: Option<u8>) -> MinedTx {
    let block: Block = BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .expect("the test vector is a valid block");
    let mut coinbase: Transaction = (*block.transactions[0]).clone();
    if let Some(gateway) = gateway {
        coinbase
            .outputs_mut()
            .push(MinerMeshCommitment::new(GatewayId::from_bytes([gateway; 20])).to_output());
    }
    MinedTx::new(
        Arc::new(coinbase).into(),
        Height(0),
        1,
        DateTime::<Utc>::UNIX_EPOCH,
    )
}

fn regtest() -> Network {
    Network::new_regtest(Default::default())
}

fn tracker(
    chain: &FakeChain,
) -> Tracker<
    impl Service<
            ReadRequest,
            Response = ReadResponse,
            Error = BoxError,
            Future = impl Send + 'static,
        > + Clone
        + Send
        + 'static,
> {
    Tracker::new(regtest(), chain.service())
}

/// The tip block the state publishes for the block at `height` on `fork`.
fn tip_block(fork: u8, height: u32) -> ChainTipBlock {
    ChainTipBlock {
        hash: hash(fork, height),
        height: Height(height),
        time: Utc::now(),
        transactions: Vec::new(),
        transaction_hashes: Arc::new([]),
        previous_block_hash: hash(fork, height - 1),
    }
}

/// A tip at `height` on `fork`, reached by a reorg or by skipped tip changes.
fn reset(fork: u8, height: u32) -> TipMove {
    TipMove {
        height: Height(height),
        hash: hash(fork, height),
        may_have_reorged: true,
    }
}

/// A tip at `height` on `fork` that extends the previous tip.
fn grow(fork: u8, height: u32) -> TipMove {
    TipMove {
        height: Height(height),
        hash: hash(fork, height),
        may_have_reorged: false,
    }
}

/// The gateways the tracker counts, as (first ID byte, blocks) in rank order.
fn counts<S>(tracker: &Tracker<S>) -> Vec<(u8, u32)>
where
    S: Service<ReadRequest, Response = ReadResponse, Error = BoxError> + Clone + Send + 'static,
    S::Future: Send,
{
    tracker
        .snapshot()
        .gateways
        .iter()
        .map(|gateway| (gateway.gateway_id.as_bytes()[0], gateway.blocks))
        .collect()
}

/// The gateways in a published ranking, as (first ID byte, blocks).
fn ranked(ranking: &watch::Receiver<MinerGatewayRanking>) -> Vec<(u8, u32)> {
    ranking
        .borrow()
        .gateways
        .iter()
        .map(|gateway| (gateway.gateway_id.as_bytes()[0], gateway.blocks))
        .collect()
}

/// Reads until the tracker has nothing more it can read.
async fn read_all<S>(tracker: &mut Tracker<S>)
where
    S: Service<ReadRequest, Response = ReadResponse, Error = BoxError> + Clone + Send + 'static,
    S::Future: Send,
{
    while tracker.next_unread().is_some() {
        tracker
            .read_next()
            .await
            .expect("the fake chain answers every read");
    }
}

#[tokio::test]
async fn reads_the_window_newest_first_and_counts_each_gateway() {
    let chain = FakeChain::default();
    chain.set(0..=TIP, 1, None);
    chain.set(0..=9, 1, Some(7));
    let mut tracker = tracker(&chain);

    tracker
        .follow_tip(reset(1, TIP))
        .await
        .expect("the fake chain answers");
    assert_eq!(tracker.next_unread(), Some(Height(50)));
    assert!(!tracker.snapshot().is_complete());

    read_all(&mut tracker).await;

    assert_eq!(counts(&tracker), [(7, 10)]);
    assert!(tracker.snapshot().is_complete());
}

#[tokio::test]
async fn a_new_tip_reads_the_block_that_reached_the_qualifying_depth() {
    let chain = FakeChain::default();
    chain.set(0..=TIP, 1, None);
    let mut tracker = tracker(&chain);
    tracker
        .follow_tip(reset(1, TIP))
        .await
        .expect("the fake chain answers");
    read_all(&mut tracker).await;

    chain.set([TIP + 1], 1, None);
    chain.set([51], 1, Some(8));
    tracker
        .follow_tip(grow(1, TIP + 1))
        .await
        .expect("a grow reads nothing");

    assert_eq!(tracker.next_unread(), Some(Height(51)));
    read_all(&mut tracker).await;
    assert_eq!(counts(&tracker), [(8, 1)]);
    assert!(tracker.snapshot().is_complete());
}

#[tokio::test]
async fn a_reorg_that_reaches_counted_blocks_replaces_the_displaced_counts() {
    let chain = FakeChain::default();
    chain.set(0..=TIP, 1, Some(7));
    let mut tracker = tracker(&chain);
    tracker
        .follow_tip(reset(1, TIP))
        .await
        .expect("the fake chain answers");
    read_all(&mut tracker).await;
    assert_eq!(counts(&tracker), [(7, 51)]);

    // A second chain forks off above height 19 and becomes the best chain.
    chain.set(20..=TIP + 10, 2, Some(9));
    tracker
        .follow_tip(reset(2, TIP + 10))
        .await
        .expect("the fake chain answers");
    assert_eq!(counts(&tracker), [(7, 20)]);

    read_all(&mut tracker).await;
    assert_eq!(counts(&tracker), [(9, 41), (7, 20)]);
    assert!(tracker.snapshot().is_complete());
}

/// A block read from the new chain before the reset arrives sits above
/// displaced blocks, so the repair cannot stop at the first block that matches.
#[tokio::test]
async fn a_block_read_from_the_new_chain_before_the_reset_does_not_hide_the_reorg() {
    let chain = FakeChain::default();
    chain.set(0..=TIP + 1, 1, Some(7));
    let mut tracker = tracker(&chain);
    tracker
        .follow_tip(reset(1, TIP))
        .await
        .expect("the fake chain answers");
    read_all(&mut tracker).await;
    tracker
        .follow_tip(grow(1, TIP + 1))
        .await
        .expect("a grow reads nothing");

    // The best chain changes, and the tracker reads the new block at 51
    // before it hears of the reorg.
    chain.set(20..=TIP + 1, 2, Some(9));
    tracker.read_next().await.expect("the fake chain answers");
    assert_eq!(counts(&tracker), [(7, 51), (9, 1)]);

    tracker
        .follow_tip(reset(2, TIP + 1))
        .await
        .expect("the fake chain answers");
    assert_eq!(counts(&tracker), [(7, 20)]);

    read_all(&mut tracker).await;
    assert_eq!(counts(&tracker), [(9, 32), (7, 20)]);
}

/// A pruned block's hash is still on the best chain but its coinbase is gone.
/// The tracker stops reading below it, and the next tip change tries again
/// (see `a_new_tip_retries_the_block_that_could_not_be_read`).
#[tokio::test]
async fn a_pruned_block_stops_reading_below_it() {
    let chain = FakeChain::default();
    chain.set(0..=TIP, 1, Some(7));
    chain.prune(30);
    let mut tracker = tracker(&chain);
    tracker
        .follow_tip(reset(1, TIP))
        .await
        .expect("the fake chain answers");

    read_all(&mut tracker).await;
    assert_eq!(counts(&tracker), [(7, 20)]);
    assert_eq!(tracker.next_unread(), None);
    assert!(!tracker.snapshot().is_complete());
}

/// A reorg to a lower tip reaches at most 1,000 blocks below that tip, so a
/// repair that cannot read the chain keeps the blocks below that.
#[tokio::test]
async fn a_failed_repair_after_a_reorg_to_a_lower_tip_keeps_unreachable_blocks() {
    let chain = FakeChain::default();
    chain.set(0..=1_400, 1, Some(7));
    let mut tracker = tracker(&chain);
    tracker
        .follow_tip(reset(1, 1_400))
        .await
        .expect("the fake chain answers");
    read_all(&mut tracker).await;

    chain.fail_reads();
    tracker
        .follow_tip(reset(2, 1_300))
        .await
        .expect_err("the repair cannot read the chain");

    // Heights 0 to 299 are below 1,300 - 1,000.
    assert_eq!(counts(&tracker), [(7, 300)]);
}

/// When the tracker cannot tell which blocks a reorg displaced, it forgets
/// every block a reorg could have reached, so no displaced block keeps counting.
#[tokio::test]
async fn a_repair_that_cannot_read_the_chain_forgets_every_reachable_block() {
    let chain = FakeChain::default();
    chain.set(0..=TIP, 1, Some(7));
    let mut tracker = tracker(&chain);
    tracker
        .follow_tip(reset(1, TIP))
        .await
        .expect("the fake chain answers");
    read_all(&mut tracker).await;

    chain.fail_reads();
    tracker
        .follow_tip(reset(1, TIP))
        .await
        .expect_err("the repair cannot read the chain");

    // Only the genesis block, which no reorg reaches, is left.
    assert_eq!(counts(&tracker), [(7, 1)]);
    assert_eq!(tracker.snapshot().checked, 1);
}

/// A reorg reaches at most 1,000 blocks below the tip it replaced, even when
/// the new tip is higher, so a repair that cannot read the chain keeps the
/// blocks below that and forgets the rest.
#[tokio::test]
async fn a_failed_repair_keeps_the_blocks_no_reorg_can_reach() {
    let chain = FakeChain::default();
    chain.set(0..=1_300, 1, Some(7));
    let mut tracker = tracker(&chain);
    tracker
        .follow_tip(reset(1, 1_300))
        .await
        .expect("the fake chain answers");
    read_all(&mut tracker).await;
    // Every height from genesis to the qualifying depth below 1,300 counts.
    assert_eq!(counts(&tracker), [(7, 1_301 - QUALIFYING_DEPTH)]);

    chain.set(1_301..=1_400, 1, Some(7));
    chain.fail_reads();
    tracker
        .follow_tip(reset(1, 1_400))
        .await
        .expect_err("the repair cannot read the chain");

    // Heights 0 to 299 are below 1,300 - 1,000.
    assert_eq!(counts(&tracker), [(7, 300)]);
}

#[tokio::test]
async fn a_failed_read_leaves_the_height_to_read_again() {
    let chain = FakeChain::default();
    chain.set(0..=TIP, 1, Some(7));
    let mut tracker = tracker(&chain);
    tracker
        .follow_tip(reset(1, TIP))
        .await
        .expect("the fake chain answers");

    chain.fail_reads();
    tracker
        .read_next()
        .await
        .expect_err("the fake chain fails every read");

    assert_eq!(tracker.next_unread(), Some(Height(50)));
}

/// The task follows the tip it is given, reads the whole window, and ends
/// cleanly when the state stops publishing tip changes.
#[tokio::test]
async fn the_task_reads_the_window_and_ends_when_the_state_stops() {
    let chain = FakeChain::default();
    chain.set(0..=TIP, 1, Some(7));
    let (mut tip_sender, latest_tip, tip_change) = ChainTipSender::new(None, &regtest());
    tip_sender.set_finalized_tip(Some(tip_block(1, TIP)));
    drop(tip_sender);
    let (ranking, publisher) = ranking_channel(true, regtest(), latest_tip);
    let publisher = publisher.expect("a mining node gets a publisher");

    let mut tracker = tracker(&chain);
    tokio::time::timeout(
        Duration::from_secs(10),
        run(&mut tracker, tip_change, &publisher),
    )
    .await
    .expect("the task ends once the tip sender is gone");

    assert_eq!(counts(&tracker), [(7, 51)]);
    assert!(tracker.snapshot().is_complete());
    assert_eq!(ranked(&ranking), [(7, 51)]);
}

/// A repair compares every read block a reorg could reach, below the lowest
/// block that still matches too.
#[tokio::test]
async fn a_repair_compares_every_read_block_a_reorg_could_reach() {
    let chain = FakeChain::default();
    chain.set(0..=TIP, 1, Some(7));
    let mut tracker = tracker(&chain);
    tracker
        .follow_tip(reset(1, TIP))
        .await
        .expect("the fake chain answers");
    read_all(&mut tracker).await;
    chain.take_hash_reads();

    chain.set(45..=TIP, 2, Some(9));
    tracker
        .follow_tip(reset(2, TIP))
        .await
        .expect("the fake chain answers");

    // The old tip first, then every read block above the genesis block.
    let mut expected = vec![TIP];
    expected.extend((1..=50).rev());
    assert_eq!(chain.take_hash_reads(), expected);
    assert_eq!(counts(&tracker), [(7, 45)]);
}

/// A reset to a tip that extends the one followed before needs one read.
#[tokio::test]
async fn a_reset_that_extends_the_previous_tip_compares_nothing_else() {
    let chain = FakeChain::default();
    chain.set(0..=TIP + 5, 1, Some(7));
    let mut tracker = tracker(&chain);
    tracker
        .follow_tip(reset(1, TIP))
        .await
        .expect("the fake chain answers");
    read_all(&mut tracker).await;
    chain.take_hash_reads();

    tracker
        .follow_tip(reset(1, TIP + 5))
        .await
        .expect("the fake chain answers");

    assert_eq!(chain.take_hash_reads(), [TIP]);
    assert_eq!(counts(&tracker), [(7, 51)]);
}

/// A comparison that fails part-way forgets every block a reorg could reach,
/// including those it had already found on the best chain.
#[tokio::test]
async fn a_comparison_that_fails_part_way_forgets_every_block_it_could_not_clear() {
    let chain = FakeChain::default();
    chain.set(0..=TIP, 1, Some(7));
    let mut tracker = tracker(&chain);
    tracker
        .follow_tip(reset(1, TIP))
        .await
        .expect("the fake chain answers");
    read_all(&mut tracker).await;

    chain.set(TIP..=TIP, 2, Some(7));
    let failing = chain.clone();
    // The read at 45 is answered; the one at 44 fails.
    chain.on_hash_read(45, move || failing.fail_reads());
    tracker
        .follow_tip(reset(2, TIP))
        .await
        .expect_err("the comparison stops at height 44");

    // Only the genesis block, which no reorg reaches, is left.
    assert_eq!(counts(&tracker), [(7, 1)]);
}

/// A tip change that arrives while the task fills the window is handled before
/// the next older block, so the block that just reached the qualifying depth is
/// read next.
#[tokio::test]
async fn the_task_follows_a_new_tip_before_reading_older_blocks() {
    let chain = FakeChain::default();
    chain.set(0..=TIP + 1, 1, None);
    let (mut tip_sender, latest_tip, tip_change) = ChainTipSender::new(None, &regtest());
    tip_sender.set_finalized_tip(Some(tip_block(1, TIP)));
    chain.on_hash_read(40, move || {
        tip_sender.set_best_non_finalized_tip(Some(tip_block(1, TIP + 1)));
    });
    let (_ranking, publisher) = ranking_channel(true, regtest(), latest_tip);
    let publisher = publisher.expect("a mining node gets a publisher");

    let mut tracker = tracker(&chain);
    tokio::time::timeout(
        Duration::from_secs(10),
        run(&mut tracker, tip_change, &publisher),
    )
    .await
    .expect("the task ends once the tip sender is gone");

    let reads = chain.take_hash_reads();
    assert_eq!(
        reads[..12],
        [50, 49, 48, 47, 46, 45, 44, 43, 42, 41, 40, 51]
    );
    assert!(tracker.snapshot().is_complete());
}

/// A block that could not be read may have just left the best chain, so the
/// next tip change lets the tracker read below it again.
#[tokio::test]
async fn a_new_tip_retries_the_block_that_could_not_be_read() {
    let chain = FakeChain::default();
    chain.set(0..=TIP + 1, 1, Some(7));
    chain.prune(30);
    let mut tracker = tracker(&chain);
    tracker
        .follow_tip(reset(1, TIP))
        .await
        .expect("the fake chain answers");
    read_all(&mut tracker).await;
    assert_eq!(tracker.next_unread(), None);

    chain.set([30], 1, Some(7));
    tracker
        .follow_tip(grow(1, TIP + 1))
        .await
        .expect("a grow reads nothing");
    read_all(&mut tracker).await;

    assert_eq!(counts(&tracker), [(7, 52)]);
    assert!(tracker.snapshot().is_complete());
}

/// A gateway newly mined blocks name reaches the network once the tenth of
/// them is [`QUALIFYING_DEPTH`] deep, and not before.
///
/// The clock is paused, so each sleep returns only once the task has handled
/// everything it was sent.
#[tokio::test(start_paused = true)]
async fn a_mined_gateway_reaches_the_network_once_its_tenth_block_counts() {
    let chain = FakeChain::default();
    let mined = TIP + 1;
    let tenth = mined + MIN_GATEWAY_BLOCKS - 1;
    chain.set(0..=tenth + QUALIFYING_DEPTH, 1, None);
    chain.set(mined..=tenth, 1, Some(7));

    let (mut tip_sender, latest_tip, tip_change) = ChainTipSender::new(None, &regtest());
    tip_sender.set_finalized_tip(Some(tip_block(1, TIP)));
    let (mut ranking, publisher) = ranking_channel(true, regtest(), latest_tip);
    let publisher = publisher.expect("a mining node gets a publisher");
    let task = tokio::spawn(async move {
        let mut tracker = tracker(&chain);
        run(&mut tracker, tip_change, &publisher).await;
    });

    // Mine the blocks that name the gateway, then enough more that nine of
    // them count and the tenth is one block short of counting.
    for height in mined..tenth + QUALIFYING_DEPTH {
        tip_sender.set_best_non_finalized_tip(Some(tip_block(1, height)));
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(ranked(&ranking).is_empty());

    // One more block puts the tenth at the qualifying depth.
    tip_sender.set_best_non_finalized_tip(Some(tip_block(1, tenth + QUALIFYING_DEPTH)));
    tokio::time::timeout(
        Duration::from_secs(10),
        ranking.wait_for(|ranking| !ranking.gateways.is_empty()),
    )
    .await
    .expect("the gateway is published once its tenth block counts")
    .expect("the tracker is still running");
    assert_eq!(ranked(&ranking), [(7, MIN_GATEWAY_BLOCKS)]);

    drop(tip_sender);
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("the task ends once the tip sender is gone")
        .expect("the task does not panic");
}

/// Whether the tip is recent enough depends on the clock, so a node whose tip
/// grows too old stops preferring gateways without a new tip.
#[tokio::test(start_paused = true)]
async fn the_task_publishes_an_empty_ranking_once_the_tip_is_too_old() {
    let chain = FakeChain::default();
    chain.set(0..=TIP, 1, Some(7));
    let (mut tip_sender, _latest_tip, tip_change) = ChainTipSender::new(None, &Network::Mainnet);
    tip_sender.set_finalized_tip(Some(tip_block(1, TIP)));
    // The tip the sync check reads, which the test can age without a tip change.
    let (sync_tip, sync_tip_sender) = MockChainTip::new();
    sync_tip_sender.send_best_tip_height(Height(TIP));
    sync_tip_sender.send_best_tip_block_time(Utc::now());
    let (mut ranking, publisher) = ranking_channel(true, Network::Mainnet, sync_tip);
    let publisher = publisher.expect("a mining node gets a publisher");
    let task = tokio::spawn(async move {
        let mut tracker = Tracker::new(Network::Mainnet, chain.service());
        run(&mut tracker, tip_change, &publisher).await;
    });

    tokio::time::timeout(
        Duration::from_secs(10),
        ranking.wait_for(|ranking| !ranking.gateways.is_empty()),
    )
    .await
    .expect("a synced node publishes the counted gateways")
    .expect("the tracker is still running");
    assert_eq!(ranked(&ranking), [(7, 51)]);

    // More than six hours behind the local clock.
    sync_tip_sender.send_best_tip_block_time(Utc::now() - chrono::Duration::hours(7));
    tokio::time::timeout(
        REPUBLISH_INTERVAL * 2,
        ranking.wait_for(|ranking| ranking.gateways.is_empty()),
    )
    .await
    .expect("the task publishes again within the republish interval")
    .expect("the tracker is still running");

    drop(tip_sender);
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("the task ends once the tip sender is gone")
        .expect("the task does not panic");
}

/// A snapshot that names the gateways `[(first ID byte, blocks)]`, most blocks first.
fn snapshot_naming(gateways: &[(u8, u32)]) -> MinerSetSnapshot {
    MinerSetSnapshot {
        tip: None,
        gateways: gateways
            .iter()
            .map(|&(id, blocks)| GatewayBlocks {
                gateway_id: GatewayId::from_bytes([id; 20]),
                blocks,
            })
            .collect(),
        checked: 0,
        window: None,
    }
}

/// A node that announces the gateway with ID bytes `id`.
fn announcing(id: u8) -> LocalGateway {
    let gateway = Some(GatewayId::from_bytes([id; 20]));
    LocalGateway {
        announced: gateway,
        stands_for: gateway,
    }
}

#[test]
fn gateway_metrics_report_the_busiest_gateway_and_the_local_one() {
    let snapshot = snapshot_naming(&[(7, 5), (3, 2), (9, 1)]);

    assert_eq!(
        gateway_metrics(&snapshot, announcing(3)),
        GatewayMetrics {
            top: 5,
            announcing: true,
            stood_for_blocks: Some(2)
        },
        "the local gateway is counted by its own blocks, not the busiest gateway's"
    );
    assert_eq!(
        gateway_metrics(&snapshot, announcing(7)).stood_for_blocks,
        Some(5),
    );
}

#[test]
fn a_relay_announces_nothing_but_counts_the_blocks_that_name_it() {
    let relay = LocalGateway {
        announced: None,
        stands_for: Some(GatewayId::from_bytes([3; 20])),
    };

    assert_eq!(
        gateway_metrics(&snapshot_naming(&[(7, 5), (3, 2)]), relay),
        GatewayMetrics {
            top: 5,
            announcing: false,
            stood_for_blocks: Some(2)
        },
    );
}

#[test]
fn gateway_metrics_are_zero_when_nothing_matches() {
    assert_eq!(
        gateway_metrics(&snapshot_naming(&[]), announcing(3)),
        GatewayMetrics {
            top: 0,
            announcing: true,
            stood_for_blocks: Some(0)
        },
        "an empty window names no gateway, including this node's"
    );
    assert_eq!(
        gateway_metrics(&snapshot_naming(&[(7, 5)]), announcing(3)).stood_for_blocks,
        Some(0),
        "a local gateway that no block names has no blocks"
    );
    assert_eq!(
        gateway_metrics(&snapshot_naming(&[(7, 5)]), LocalGateway::default()),
        GatewayMetrics {
            top: 5,
            announcing: false,
            stood_for_blocks: None
        },
        "a node that stands for no gateway has no local gateway to count"
    );
}
