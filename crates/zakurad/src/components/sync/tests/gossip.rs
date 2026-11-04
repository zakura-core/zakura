//! Integration tests for block hash gossip.

#![allow(clippy::unwrap_in_result)]

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::{task::JoinHandle, time::timeout};
use tower::{
    builder::ServiceBuilder,
    util::{BoxCloneService, BoxService},
    Service, ServiceExt,
};
use tracing::Instrument;

use zakura_chain::{
    block::{Block, Height},
    fmt::humantime_seconds,
    parameters::Network::Mainnet,
    serialization::{ZcashDeserializeInto, ZcashSerialize},
};
use zakura_network::{AdvertisedBlock, NoEligibleBlockPeers, Request, Response};
use zakura_rpc::{MinedBlockEvent, MinedBlockSubmission, PendingBlockSignal, SubmitBlockChannel};
use zakura_state::{
    ChainTipChange, ChainTipSender, CheckpointVerifiedBlock, Config as StateConfig,
    CHAIN_TIP_UPDATE_WAIT_LIMIT,
};
use zakura_test::mock_service::{MockService, PanicAssertion};

use crate::components::sync::{self, gossip::BLOCK_GOSSIP_TIMEOUT, BlockGossipError, SyncStatus};

const MAX_PEER_SET_REQUEST_DELAY: Duration = Duration::from_secs(30);

/// The advertisement of `block` with its serialized size, as mined-block events and committed
/// tips that the state can size carry it.
fn with_size(block: &Block) -> AdvertisedBlock {
    AdvertisedBlock::new(block.hash(), block.zcash_serialized_size())
}

/// The advertisement of a committed tip whose size the state does not have.
fn without_size(block: &Block) -> AdvertisedBlock {
    AdvertisedBlock {
        size: None,
        ..with_size(block)
    }
}

/// A read state that has no block sizes, for tests that publish tips without a state.
fn read_state_without_sizes() -> ReadState {
    BoxCloneService::new(tower::service_fn(|request| async move {
        match request {
            zakura_state::ReadRequest::BlockInfo(_) => {
                Ok(zakura_state::ReadResponse::BlockInfo(None))
            }
            request => unreachable!("gossip only reads block sizes, not {request:?}"),
        }
    }))
}

type ReadState =
    BoxCloneService<zakura_state::ReadRequest, zakura_state::ReadResponse, crate::BoxError>;

struct GossipTestSetup {
    peer_set: MockService<Request, Response, PanicAssertion>,
    submitblock_sender: tokio::sync::mpsc::UnboundedSender<MinedBlockEvent>,
    gossip_task_handle: JoinHandle<Result<(), BlockGossipError>>,
}

// Paused time must not race the state's OS writer thread. Mocked gossip scenarios
// publish tips synchronously through the same channel the writer uses.
async fn setup_gossip_test() -> (ChainTipSender, GossipTestSetup) {
    let _init_guard = zakura_test::init();
    let block_one: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let tip = CheckpointVerifiedBlock::from(block_one.clone()).into();
    let (chain_tip_sender, _latest_chain_tip, chain_tip_change) =
        ChainTipSender::new(Some(tip), &Mainnet);
    let setup = start_gossip_test(
        chain_tip_change,
        read_state_without_sizes(),
        without_size(&block_one),
    )
    .await;
    (chain_tip_sender, setup)
}

async fn setup_state_gossip_test() -> (
    BoxService<zakura_state::Request, zakura_state::Response, crate::BoxError>,
    GossipTestSetup,
) {
    let _init_guard = zakura_test::init();

    let network = Mainnet;
    let state_config = StateConfig::ephemeral();
    let (state, read_only_state, _latest_chain_tip, mut chain_tip_change) =
        zakura_state::init(state_config, &network, Height::MAX, 0)
            .await
            .expect("ephemeral state initialization succeeds");

    let mut state_service = ServiceBuilder::new().buffer(1).service(state);

    let genesis_block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
        .zcash_deserialize_into()
        .unwrap();
    state_service
        .ready()
        .await
        .unwrap()
        .call(zakura_state::Request::CommitCheckpointVerifiedBlock(
            genesis_block.into(),
        ))
        .await
        .unwrap();

    if let Err(timeout_error) = timeout(
        CHAIN_TIP_UPDATE_WAIT_LIMIT,
        chain_tip_change.wait_for_tip_change(),
    )
    .await
    .map(|change_result| change_result.expect("unexpected chain tip update failure"))
    {
        panic!(
            "timeout waiting for genesis chain tip change after {}: {timeout_error:?}",
            humantime_seconds(CHAIN_TIP_UPDATE_WAIT_LIMIT),
        );
    }

    let block_one: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .unwrap();
    state_service
        .clone()
        .oneshot(zakura_state::Request::CommitCheckpointVerifiedBlock(
            block_one.clone().into(),
        ))
        .await
        .unwrap();

    let setup = start_gossip_test(
        chain_tip_change,
        BoxCloneService::new(read_only_state),
        with_size(&block_one),
    )
    .await;
    (BoxService::new(state_service), setup)
}

async fn start_gossip_test(
    chain_tip_change: ChainTipChange,
    read_state: ReadState,
    initial_tip: AdvertisedBlock,
) -> GossipTestSetup {
    let (sync_status, mut recent_syncs) = SyncStatus::new();
    SyncStatus::sync_close_to_tip(&mut recent_syncs);

    let mut peer_set = MockService::build()
        .with_max_request_delay(MAX_PEER_SET_REQUEST_DELAY)
        .for_unit_tests();

    let submitblock_channel = SubmitBlockChannel::new();
    let submitblock_sender = submitblock_channel.sender();
    let gossip_task_handle = tokio::spawn(
        sync::gossip_best_tip_block_hashes(
            sync_status,
            chain_tip_change,
            Some(peer_set.clone()),
            None,
            read_state,
            Some(submitblock_channel.receiver()),
        )
        .in_current_span(),
    );

    // Block 1 is the initial tip when the task starts.
    peer_set
        .expect_request(Request::AdvertiseBlock(initial_tip, None))
        .await
        .respond(Response::Nil);

    GossipTestSetup {
        peer_set,
        submitblock_sender,
        gossip_task_handle,
    }
}

/// After a successful mined block broadcast, the gossip task marks the tip as seen and does not
/// send a duplicate committed-tip gossip for the same hash.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn mined_block_marks_tip_after_successful_broadcast() {
    let (
        mut chain_tip_sender,
        GossipTestSetup {
            mut peer_set,
            submitblock_sender,
            gossip_task_handle: _gossip_task_handle,
        },
    ) = setup_gossip_test().await;

    let block_two: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_2_BYTES
        .zcash_deserialize_into()
        .unwrap();

    chain_tip_sender.set_finalized_tip(Some(
        CheckpointVerifiedBlock::from(block_two.clone()).into(),
    ));

    submitblock_sender
        .send(MinedBlockEvent::Committed {
            hash: block_two.hash(),
            height: block_two.coinbase_height().unwrap(),
            size: block_two.zcash_serialized_size(),
        })
        .expect("mined block notification should be accepted");

    peer_set
        .expect_request(Request::AdvertiseMinedBlock(with_size(&block_two)))
        .await
        .respond(Response::Nil);

    // The committed tip gossip path should not advertise the same hash again.
    peer_set.expect_no_requests().await;
}

/// A successful mined-block broadcast still suppresses the committed-tip fallback for that hash
/// even when another mined-block notification is already queued.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn mined_block_mark_survives_pending_submit_queue() {
    let (
        mut chain_tip_sender,
        GossipTestSetup {
            mut peer_set,
            submitblock_sender,
            gossip_task_handle: _gossip_task_handle,
        },
    ) = setup_gossip_test().await;

    let block_two: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_2_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let height = block_two.coinbase_height().unwrap();
    let hash = block_two.hash();

    chain_tip_sender.set_finalized_tip(Some(
        CheckpointVerifiedBlock::from(block_two.clone()).into(),
    ));

    // Hold the first mined broadcast response open.
    submitblock_sender
        .send(MinedBlockEvent::Committed {
            hash,
            height,
            size: block_two.zcash_serialized_size(),
        })
        .expect("mined block notification should be accepted");

    let first_broadcast = peer_set
        .expect_request(Request::AdvertiseMinedBlock(with_size(&block_two)))
        .await;

    // Queue a second notification while the first broadcast is still in flight so the
    // submit-block channel is nonempty when the first mark arrives.
    submitblock_sender
        .send(MinedBlockEvent::Committed {
            hash,
            height,
            size: block_two.zcash_serialized_size(),
        })
        .expect("second mined block notification should be accepted");

    first_broadcast.respond(Response::Nil);

    // The queued mined notification starts another broadcast.
    peer_set
        .expect_request(Request::AdvertiseMinedBlock(with_size(&block_two)))
        .await
        .respond(Response::Nil);

    // Without unconditional marking, the first mark would be dropped while the queue was
    // nonempty and the committed-tip path could still AdvertiseBlock(hash).
    peer_set.expect_no_requests().await;
}

/// If a mined block broadcast times out, the committed tip gossip path should still advertise the
/// hash as a fallback.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn mined_block_broadcast_timeout_uses_committed_tip_fallback() {
    let (
        mut chain_tip_sender,
        GossipTestSetup {
            mut peer_set,
            submitblock_sender,
            gossip_task_handle: _gossip_task_handle,
        },
    ) = setup_gossip_test().await;

    let block_two: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_2_BYTES
        .zcash_deserialize_into()
        .unwrap();

    chain_tip_sender.set_finalized_tip(Some(
        CheckpointVerifiedBlock::from(block_two.clone()).into(),
    ));

    submitblock_sender
        .send(MinedBlockEvent::Committed {
            hash: block_two.hash(),
            height: block_two.coinbase_height().unwrap(),
            size: block_two.zcash_serialized_size(),
        })
        .expect("mined block notification should be accepted");

    let slow_broadcast = peer_set
        .expect_request(Request::AdvertiseMinedBlock(with_size(&block_two)))
        .await;

    // Hold the mined block broadcast open past the gossip timeout so it fails without marking.
    tokio::time::sleep(BLOCK_GOSSIP_TIMEOUT + Duration::from_secs(1)).await;
    drop(slow_broadcast);

    // The committed tip gossip path should advertise the same hash as a fallback.
    peer_set
        .expect_request(Request::AdvertiseMinedBlock(with_size(&block_two)))
        .await
        .respond(Response::Nil);
}

/// An early announcement must not suppress the committed-tip fallback when the later
/// committed broadcast fails. Its body was still subject to contextual validation.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn early_broadcast_does_not_suppress_the_committed_tip_fallback() {
    let (
        mut chain_tip_sender,
        GossipTestSetup {
            mut peer_set,
            submitblock_sender,
            gossip_task_handle: _gossip_task_handle,
        },
    ) = setup_gossip_test().await;

    let block_two: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_2_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let hash = block_two.hash();
    let height = block_two.coinbase_height().unwrap();

    // The early broadcast succeeds, which is what would mark the tip as already gossiped.
    submitblock_sender
        .send(MinedBlockEvent::Early {
            hash,
            height,
            size: block_two.zcash_serialized_size(),
            submitted_at: tokio::time::Instant::now().into_std(),
            submission: zakura_rpc::MinedBlockSubmission::FullBlock,
            pending: PendingBlockSignal::valid_for_tests(),
        })
        .expect("the early mined block notification is accepted");

    peer_set
        .expect_request(Request::AdvertiseMinedBlock(with_size(&block_two)))
        .await
        .respond(Response::Nil);

    // Let the spawned early broadcast finish before the block commits.
    tokio::time::sleep(Duration::from_millis(200)).await;

    chain_tip_sender.set_finalized_tip(Some(
        CheckpointVerifiedBlock::from(block_two.clone()).into(),
    ));

    submitblock_sender
        .send(MinedBlockEvent::Committed {
            hash,
            height,
            size: block_two.zcash_serialized_size(),
        })
        .expect("the committed mined block notification is accepted");

    // Hold the committed broadcast open past the gossip timeout so it fails without marking.
    let slow_broadcast = peer_set
        .expect_request(Request::AdvertiseMinedBlock(with_size(&block_two)))
        .await;
    tokio::time::sleep(BLOCK_GOSSIP_TIMEOUT + Duration::from_secs(1)).await;
    drop(slow_broadcast);

    // Nothing has advertised a body this node can serve, so the fallback must still run.
    peer_set
        .expect_request(Request::AdvertiseMinedBlock(with_size(&block_two)))
        .await
        .respond(Response::Nil);
}

/// Consecutive committed blocks are gossiped as soon as they commit, with no delay between them.
///
/// This test uses real time: with a paused clock, the state commit auto-advances time by minutes,
/// which would hide a gossip delay.
#[tokio::test(flavor = "current_thread")]
async fn consecutive_committed_blocks_are_gossiped_without_delay() {
    /// Well below the removed 7 second delay, and far above the expected latency.
    const MAX_GOSSIP_LATENCY: Duration = Duration::from_secs(3);

    let (
        mut state_service,
        GossipTestSetup {
            mut peer_set,
            submitblock_sender: _submitblock_sender,
            gossip_task_handle: _gossip_task_handle,
        },
    ) = setup_state_gossip_test().await;

    for block_bytes in [
        &*zakura_test::vectors::BLOCK_MAINNET_2_BYTES,
        &*zakura_test::vectors::BLOCK_MAINNET_3_BYTES,
    ] {
        let block: Arc<Block> = block_bytes.zcash_deserialize_into().unwrap();

        state_service
            .ready()
            .await
            .unwrap()
            .call(zakura_state::Request::CommitCheckpointVerifiedBlock(
                block.clone().into(),
            ))
            .await
            .unwrap();
        let committed_at = tokio::time::Instant::now();

        peer_set
            .expect_request(Request::AdvertiseBlock(with_size(&block), None))
            .await
            .respond(Response::Nil);

        assert!(
            committed_at.elapsed() < MAX_GOSSIP_LATENCY,
            "block gossip waited {:?} after the commit",
            committed_at.elapsed(),
        );
    }
}

/// While a mined block broadcast is in flight, the committed-tip path does not advertise the same
/// hash.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn in_flight_mined_block_broadcast_suppresses_committed_tip_gossip() {
    let (
        mut chain_tip_sender,
        GossipTestSetup {
            mut peer_set,
            submitblock_sender,
            gossip_task_handle: _gossip_task_handle,
        },
    ) = setup_gossip_test().await;

    let block_two: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_2_BYTES
        .zcash_deserialize_into()
        .unwrap();

    chain_tip_sender.set_finalized_tip(Some(
        CheckpointVerifiedBlock::from(block_two.clone()).into(),
    ));

    submitblock_sender
        .send(MinedBlockEvent::Committed {
            hash: block_two.hash(),
            height: block_two.coinbase_height().unwrap(),
            size: block_two.zcash_serialized_size(),
        })
        .expect("mined block notification should be accepted");

    let in_flight_broadcast = peer_set
        .expect_request(Request::AdvertiseMinedBlock(with_size(&block_two)))
        .await;

    // Hold the broadcast open for less than its timeout, so it is still in flight when the
    // committed-tip path sees the tip change.
    tokio::time::sleep(BLOCK_GOSSIP_TIMEOUT / 2).await;
    in_flight_broadcast.respond(Response::Nil);

    peer_set.expect_no_requests().await;
}

/// Checkpoint commits acknowledge before publishing the tip, unlike non-finalized commits.
/// A mined broadcast that is still in flight must suppress that later notification.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn in_flight_mined_block_broadcast_handles_delayed_tip_notification() {
    let (
        mut chain_tip_sender,
        GossipTestSetup {
            mut peer_set,
            submitblock_sender,
            gossip_task_handle: _gossip_task_handle,
        },
    ) = setup_gossip_test().await;

    let block_two: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_2_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let hash = block_two.hash();

    submitblock_sender
        .send(MinedBlockEvent::Committed {
            hash,
            height: block_two.coinbase_height().unwrap(),
            size: block_two.zcash_serialized_size(),
        })
        .expect("mined block notification should be accepted");
    let in_flight_broadcast = peer_set
        .expect_request(Request::AdvertiseMinedBlock(with_size(&block_two)))
        .await;

    // Publish the tip only after the mined notification has started its broadcast.
    chain_tip_sender.set_finalized_tip(Some(CheckpointVerifiedBlock::from(block_two).into()));
    tokio::time::sleep(BLOCK_GOSSIP_TIMEOUT / 2).await;
    in_flight_broadcast.respond(Response::Nil);

    peer_set.expect_no_requests().await;

    // A dead gossip task would also send no duplicate. Require it to relay the next tip.
    let block_three: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_3_BYTES
        .zcash_deserialize_into()
        .unwrap();
    chain_tip_sender.set_finalized_tip(Some(
        CheckpointVerifiedBlock::from(block_three.clone()).into(),
    ));
    peer_set
        .expect_request(Request::AdvertiseBlock(without_size(&block_three), None))
        .await
        .respond(Response::Nil);
}

/// How long the gossip task needs to see a committed tip, with paused time.
const TIP_PROCESSING_DELAY: Duration = Duration::from_millis(1);

/// The mainnet block at `height`, from the test vectors.
fn mainnet_block(height: u32) -> Arc<Block> {
    (*zakura_test::vectors::MAINNET_BLOCKS
        .get(&height)
        .expect("the test vectors have this mainnet block"))
    .zcash_deserialize_into()
    .unwrap()
}

/// A block advertisement service whose connected peers the test controls, standing in for a
/// protocol that has nobody to advertise to.
struct GatedPeers {
    /// Receives the advertisements that reach a peer.
    inner: MockService<Request, Response, PanicAssertion>,

    /// Set while a peer can receive an advertisement.
    has_peers: tokio::sync::watch::Receiver<bool>,

    /// How many dispatches report that every peer disappeared before the advertisement was sent.
    vanishing: Arc<AtomicUsize>,

    /// The wait for a peer, while one is in progress.
    waiting: Option<futures::future::BoxFuture<'static, ()>>,
}

impl Clone for GatedPeers {
    /// Clones the handle without its wait, like the node's block advertiser.
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            has_peers: self.has_peers.clone(),
            vanishing: self.vanishing.clone(),
            waiting: None,
        }
    }
}

impl Service<Request> for GatedPeers {
    type Response = Response;
    type Error = crate::BoxError;
    type Future = futures::future::BoxFuture<'static, Result<Response, crate::BoxError>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        if self.waiting.is_none() {
            let mut has_peers = self.has_peers.clone();
            self.waiting = Some(Box::pin(async move {
                while !*has_peers.borrow_and_update() {
                    has_peers.changed().await.expect("the test owns the sender");
                }
            }));
        }
        let waiting = self.waiting.as_mut().expect("a wait was just installed");
        std::task::ready!(futures::FutureExt::poll_unpin(waiting, cx));
        self.waiting = None;
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request) -> Self::Future {
        // These tests are single threaded, so a load and a store report each vanishing dispatch
        // exactly once.
        if self.vanishing.load(Ordering::SeqCst) > 0 {
            self.vanishing.fetch_sub(1, Ordering::SeqCst);
            return Box::pin(std::future::ready(Err(
                Box::new(NoEligibleBlockPeers) as crate::BoxError
            )));
        }
        Box::pin(self.inner.clone().oneshot(request))
    }
}

/// A gossip task whose legacy peers the test controls.
struct GatedGossipSetup {
    /// Connects and disconnects the legacy protocol's peers.
    peers: tokio::sync::watch::Sender<bool>,

    /// How many legacy dispatches find that every peer has gone.
    vanishing: Arc<AtomicUsize>,

    /// The advertisements that reach a legacy peer.
    legacy: MockService<Request, Response, PanicAssertion>,

    /// The advertisements that reach a Zakura peer.
    native: MockService<Request, Response, PanicAssertion>,

    chain_tip_sender: ChainTipSender,
    submitblock_sender: tokio::sync::mpsc::UnboundedSender<MinedBlockEvent>,
    _gossip_task_handle: JoinHandle<Result<(), BlockGossipError>>,
}

impl GatedGossipSetup {
    /// Commits `block` as the best chain tip, and lets the gossip task see it.
    async fn commit(&mut self, block: &Arc<Block>) {
        self.chain_tip_sender
            .set_finalized_tip(Some(CheckpointVerifiedBlock::from(block.clone()).into()));
        tokio::time::sleep(TIP_PROCESSING_DELAY).await;
    }

    /// Reports that this node mined `block`, which has committed.
    fn mined(&self, block: &Arc<Block>) {
        self.submitblock_sender
            .send(MinedBlockEvent::Committed {
                hash: block.hash(),
                height: block.coinbase_height().unwrap(),
                size: block.zcash_serialized_size(),
            })
            .expect("the gossip task is running");
    }

    /// Reports that this node submitted `block`, which is not committed yet, and returns the
    /// handle that rejects it.
    fn submitted(&self, block: &Arc<Block>) -> zakura_rpc::PendingBlockFailure {
        let (pending, failure) = PendingBlockSignal::waiting_for_tests();
        self.submitblock_sender
            .send(MinedBlockEvent::Early {
                hash: block.hash(),
                height: block.coinbase_height().unwrap(),
                size: block.zcash_serialized_size(),
                submitted_at: std::time::Instant::now(),
                submission: MinedBlockSubmission::FullBlock,
                pending,
            })
            .expect("the gossip task is running");
        failure
    }

    /// Lets the legacy protocol's peers receive advertisements.
    fn connect_peers(&self) {
        self.peers.send_replace(true);
    }

    /// Takes the legacy protocol's peers away, so its next announcement waits for one.
    fn disconnect_peers(&self) {
        self.peers.send_replace(false);
    }
}

/// Starts a gossip task with no legacy peer connected, and Zakura peers only `with_native`.
async fn setup_gated_gossip_test(with_native: bool) -> GatedGossipSetup {
    let _init_guard = zakura_test::init();

    let (chain_tip_sender, _latest_chain_tip, chain_tip_change) =
        ChainTipSender::new(None, &Mainnet);
    let (peers, has_peers) = tokio::sync::watch::channel(false);
    let vanishing = Arc::new(AtomicUsize::new(0));

    let legacy = MockService::build()
        .with_max_request_delay(MAX_PEER_SET_REQUEST_DELAY)
        .for_unit_tests();
    let native = MockService::build()
        .with_max_request_delay(MAX_PEER_SET_REQUEST_DELAY)
        .for_unit_tests();

    let legacy_service = BoxCloneService::new(GatedPeers {
        inner: legacy.clone(),
        has_peers,
        vanishing: vanishing.clone(),
        waiting: None,
    });
    let native_service = with_native.then(|| BoxCloneService::new(native.clone()));

    let (sync_status, mut recent_syncs) = SyncStatus::new();
    SyncStatus::sync_close_to_tip(&mut recent_syncs);

    let submitblock_channel = SubmitBlockChannel::new();
    let submitblock_sender = submitblock_channel.sender();
    let _gossip_task_handle = tokio::spawn(
        sync::gossip_best_tip_block_hashes(
            sync_status,
            chain_tip_change,
            Some(legacy_service),
            native_service,
            read_state_without_sizes(),
            Some(submitblock_channel.receiver()),
        )
        .in_current_span(),
    );

    GatedGossipSetup {
        peers,
        vanishing,
        legacy,
        native,
        chain_tip_sender,
        submitblock_sender,
        _gossip_task_handle,
    }
}

/// A run of commits with nobody to advertise to leaves one announcement per slot: the first peer
/// to connect is sent the current tip and this node's own block, not the whole backlog.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn commits_without_peers_keep_only_the_latest_tip_and_mined_block() {
    /// Repeats of mainnet blocks 2 to 10, so the task sees many more tip changes than slots.
    const CYCLES: u32 = 20;

    let mut setup = setup_gated_gossip_test(false).await;

    for _ in 0..CYCLES {
        for height in 2..=10 {
            let block = mainnet_block(height);
            setup.commit(&block).await;
            // The last block is not mined, so both slots hold a different block.
            if height < 10 {
                setup.mined(&block);
            }
        }
    }

    setup.legacy.expect_no_requests().await;

    setup.connect_peers();

    setup
        .legacy
        .expect_request(Request::AdvertiseMinedBlock(with_size(&mainnet_block(9))))
        .await
        .respond(Response::Nil);
    setup
        .legacy
        .expect_request(Request::AdvertiseBlock(
            without_size(&mainnet_block(10)),
            None,
        ))
        .await
        .respond(Response::Nil);
    setup.legacy.expect_no_requests().await;
}

/// A protocol with no peers does not delay a protocol that has them.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn legacy_without_peers_does_not_delay_zakura() {
    let mut setup = setup_gated_gossip_test(true).await;
    let block_two = mainnet_block(2);

    setup.commit(&block_two).await;

    setup
        .native
        .expect_request(Request::AdvertiseBlock(without_size(&block_two), None))
        .await
        .respond(Response::Nil);
    setup.legacy.expect_no_requests().await;
}

/// An announcement whose peers all disappear before it is sent waits for the next peer instead of
/// reporting a delivery or accumulating another task.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn an_announcement_without_peers_at_dispatch_waits_for_the_next_peer() {
    let mut setup = setup_gated_gossip_test(false).await;
    let block_two = mainnet_block(2);

    setup.vanishing.store(1, Ordering::SeqCst);
    setup.connect_peers();
    setup.commit(&block_two).await;

    setup
        .legacy
        .expect_request(Request::AdvertiseBlock(without_size(&block_two), None))
        .await
        .respond(Response::Nil);
    setup.legacy.expect_no_requests().await;
}

/// A waiting announcement that runs out of time is dropped, without being retried later.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_waiting_announcement_expires_without_retrying() {
    let mut setup = setup_gated_gossip_test(false).await;
    let block_two = mainnet_block(2);

    setup.commit(&block_two).await;
    tokio::time::sleep(BLOCK_GOSSIP_TIMEOUT + Duration::from_secs(1)).await;

    setup.connect_peers();
    setup.legacy.expect_no_requests().await;
}

/// A submitted block that fails contextual verification while it waits for a peer is not
/// announced.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_rejected_submission_is_not_announced() {
    let mut setup = setup_gated_gossip_test(false).await;
    let block_two = mainnet_block(2);

    let failure = setup.submitted(&block_two);
    tokio::time::sleep(TIP_PROCESSING_DELAY).await;
    failure.fail();

    setup.connect_peers();
    setup.legacy.expect_no_requests().await;
}

/// A submitted block is announced while its parent is still the committed tip: a block that is
/// not the tip is still worth advertising.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_submitted_child_is_announced_while_its_parent_is_the_tip() {
    let mut setup = setup_gated_gossip_test(false).await;
    let block_two = mainnet_block(2);
    let block_three = mainnet_block(3);

    setup.connect_peers();
    setup.commit(&block_two).await;
    setup
        .legacy
        .expect_request(Request::AdvertiseBlock(without_size(&block_two), None))
        .await
        .respond(Response::Nil);

    // Block three is submitted, so block two is still the committed tip.
    let _failure = setup.submitted(&block_three);
    setup
        .legacy
        .expect_request(Request::AdvertiseMinedBlock(with_size(&block_three)))
        .await
        .respond(Response::Nil);
}

/// Committing a block that was already announced as a submission announces it again: the early
/// announcement does not satisfy the committed one.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_committed_block_is_announced_after_its_submission() {
    let mut setup = setup_gated_gossip_test(false).await;
    let block_two = mainnet_block(2);

    setup.connect_peers();
    let _failure = setup.submitted(&block_two);
    setup
        .legacy
        .expect_request(Request::AdvertiseMinedBlock(with_size(&block_two)))
        .await
        .respond(Response::Nil);

    setup.mined(&block_two);
    setup
        .legacy
        .expect_request(Request::AdvertiseMinedBlock(with_size(&block_two)))
        .await
        .respond(Response::Nil);
}

/// A newer tip replaces the waiting announcement, and leaves an announcement that is already
/// being sent to finish.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_newer_tip_does_not_cancel_an_active_announcement() {
    let mut setup = setup_gated_gossip_test(false).await;
    let block_two = mainnet_block(2);
    let block_three = mainnet_block(3);

    setup.connect_peers();
    setup.commit(&block_two).await;
    let active = setup
        .legacy
        .expect_request(Request::AdvertiseBlock(without_size(&block_two), None))
        .await;

    setup.commit(&block_three).await;
    active.respond(Response::Nil);

    setup
        .legacy
        .expect_request(Request::AdvertiseBlock(without_size(&block_three), None))
        .await
        .respond(Response::Nil);
    setup.legacy.expect_no_requests().await;
}

/// A mined block announcement whose peers all disappear before it is sent waits for the next
/// peer, like a committed tip's does.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_mined_announcement_without_peers_at_dispatch_waits_for_the_next_peer() {
    let mut setup = setup_gated_gossip_test(false).await;
    let block_two = mainnet_block(2);

    setup.vanishing.store(1, Ordering::SeqCst);
    setup.connect_peers();
    setup.mined(&block_two);

    setup
        .legacy
        .expect_request(Request::AdvertiseMinedBlock(with_size(&block_two)))
        .await
        .respond(Response::Nil);
    setup.legacy.expect_no_requests().await;
}

/// A slower announcement of an older mined block finishing after a newer block's does not make
/// the committed-tip path announce the newer block again.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_late_mined_completion_does_not_reannounce_the_newer_block_as_the_tip() {
    let mut setup = setup_gated_gossip_test(false).await;
    let block_two = mainnet_block(2);
    let block_three = mainnet_block(3);
    let block_four = mainnet_block(4);

    setup.connect_peers();
    setup.mined(&block_two);
    let older = setup
        .legacy
        .expect_request(Request::AdvertiseMinedBlock(with_size(&block_two)))
        .await;
    setup.mined(&block_three);
    let newer = setup
        .legacy
        .expect_request(Request::AdvertiseMinedBlock(with_size(&block_three)))
        .await;

    // The newer block's announcement finishes first.
    newer.respond(Response::Nil);
    tokio::time::sleep(TIP_PROCESSING_DELAY).await;
    older.respond(Response::Nil);
    tokio::time::sleep(TIP_PROCESSING_DELAY).await;

    // A checkpoint commit publishes its tip after the mined block event.
    setup.commit(&block_two).await;
    setup.commit(&block_three).await;
    setup.legacy.expect_no_requests().await;

    // A dead gossip task would also send nothing. Require it to relay the next tip.
    setup.commit(&block_four).await;
    setup
        .legacy
        .expect_request(Request::AdvertiseBlock(without_size(&block_four), None))
        .await
        .respond(Response::Nil);
}

/// A submission that is rejected while it waits for a peer does not keep a block that commits in
/// the meantime from being announced.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_committed_block_replaces_a_rejected_submission_that_is_still_waiting() {
    let mut setup = setup_gated_gossip_test(false).await;
    let block_two = mainnet_block(2);
    let block_three = mainnet_block(3);

    let failure = setup.submitted(&block_three);
    tokio::time::sleep(TIP_PROCESSING_DELAY).await;

    // Rejecting the submission wakes nothing, so the committed block's event is what finds the
    // rejected submission still waiting. Nothing yields between these steps.
    failure.fail();
    setup.mined(&block_two);
    setup.connect_peers();

    setup
        .legacy
        .expect_request(Request::AdvertiseMinedBlock(with_size(&block_two)))
        .await
        .respond(Response::Nil);
    setup.legacy.expect_no_requests().await;
}

/// The retry of a failed mined block announcement keeps its mined priority when the block's own
/// tip event reaches it while it waits for a peer.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_late_committed_tip_keeps_the_mined_retry_waiting() {
    let mut setup = setup_gated_gossip_test(false).await;
    let block_two = mainnet_block(2);

    setup.connect_peers();
    setup.mined(&block_two);
    let failing = setup
        .legacy
        .expect_request(Request::AdvertiseMinedBlock(with_size(&block_two)))
        .await;

    // The peers go away, so the retry waits for one, and the tip is published only now, so its
    // event reaches the waiting retry.
    setup.disconnect_peers();
    setup.chain_tip_sender.set_finalized_tip(Some(
        CheckpointVerifiedBlock::from(block_two.clone()).into(),
    ));
    failing.respond_error("the peer went away".into());
    tokio::time::sleep(TIP_PROCESSING_DELAY * 2).await;

    setup.connect_peers();
    setup
        .legacy
        .expect_request(Request::AdvertiseMinedBlock(with_size(&block_two)))
        .await
        .respond(Response::Nil);
    setup.legacy.expect_no_requests().await;
}
