//! Today's pool and the pool core at the service boundary.
//!
//! These tests drive the mempool only through `Service<Request>` and its
//! collaborators: the verifier, the peer set, the state and the sync status.
//! They pin what callers see today, and hold the pool core's adapter to the
//! same answers. [`pool`] is the one place that builds the service under
//! test, for either [`Mode`].
//!
//! Each test runs against both pools, through [`both_pools!`]. Where the
//! core answers differently on purpose, the test says so and checks the
//! core's answer.

#![allow(clippy::unwrap_in_result)]

use std::{
    collections::HashSet,
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

use futures::future::BoxFuture;
use tokio::{
    sync::{oneshot, watch},
    time::{self, timeout},
};
use tower::{
    util::{BoxCloneService, BoxCloneSyncService, BoxService},
    Service, ServiceBuilder, ServiceExt,
};

use zakura_chain::{
    block::{Block, Height},
    chain_tip::ChainTip,
    parameters::Network,
    serialization::ZcashDeserializeInto,
    transaction::{self, UnminedTxId, VerifiedUnminedTx},
    transparent::OutPoint,
};
use zakura_consensus::{error::TransactionError, transaction as tx};
use zakura_network as zn;
use zakura_node_services::mempool::{
    CreatedOrSpent, Gossip, MempoolChange, MempoolChangeKind, MempoolDisabledError,
    MempoolTxSubscriber, QueueSource, Request, Response,
};
use zakura_state::{self as zs, ChainTipChange, LatestChainTip};
use zakura_test::mock_service::{MockService, PanicAssertion};

use super::super::{
    core::{self, Exempt, MempoolService},
    downloads::MAX_INBOUND_CONCURRENCY,
    downloads::MAX_INBOUND_CONCURRENCY_PER_PEER,
    pending_outputs::{PendingOutputsFull, MAX_PENDING_OUTPUTS},
    Config, ExactTipRejectionError, Mempool, MempoolError, SameEffectsChainRejectionError,
    SameEffectsTipRejectionError,
};
use super::UnboxMempoolError;
use crate::{components::sync::RecentSyncLengths, BoxError};

/// A [`MockService`] standing in for the peer set.
type MockPeerSet = MockService<zn::Request, zn::Response, PanicAssertion>;

/// A [`MockService`] standing in for the transaction verifier.
type MockTxVerifier = MockService<tx::Request, tx::Response, PanicAssertion, TransactionError>;

/// The real state service, as the mempool takes it.
type StateService =
    tower::buffer::Buffer<zs::Request, BoxFuture<'static, Result<zs::Response, zs::BoxError>>>;

/// The receiver a `Queue` item's second answer arrives on.
type Receiver = oneshot::Receiver<Result<(), BoxError>>;

/// How long a double waits for a request the pool should make.
const REQUEST_DELAY: Duration = Duration::from_secs(10);

/// How long a test waits for an answer it expects.
const ANSWER_WAIT: Duration = Duration::from_secs(10);

/// How long a test waits before it concludes that an answer is not coming.
const SILENCE: Duration = Duration::from_millis(300);

/// Which peer set the pool talks to.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Peers {
    /// A [`MockPeerSet`] the test answers.
    Mock,
    /// A peer set that never answers, so every fetch stays in flight.
    Silent,
}

/// Which pool a test runs against.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Mode {
    /// Today's pool.
    Classic,
    /// The pool core, behind its adapter.
    Core,
}

/// The service under test.
enum PoolUnderTest {
    /// Today's pool, which the test drives directly.
    Classic(Box<Mempool>),
    /// The pool core's adapter, and the switch that turns the core on.
    Core {
        /// The adapter, buffered as `start.rs` buffers it.
        service: MempoolService,
        /// The transaction verifier's pool handle.
        verifier_pool: MempoolService,
        /// Whether the core is on. The test sets it, where the node would
        /// decide it from its sync status.
        active: watch::Sender<bool>,
    },
}

/// Runs a test, an async function that takes a [`Mode`], against both pools.
macro_rules! both_pools {
    ($($test:ident),* $(,)?) => {
        $(
            mod $test {
                use super::Mode;

                #[tokio::test(flavor = "multi_thread")]
                async fn classic() {
                    super::$test(Mode::Classic).await;
                }

                #[tokio::test(flavor = "multi_thread")]
                async fn core() {
                    super::$test(Mode::Core).await;
                }
            }
        )*
    };
}

/// The pool under test and the controls a test drives it with.
struct Harness {
    /// The service under test.
    pool: PoolUnderTest,
    /// The peer set, when it is [`Peers::Mock`].
    peers: MockPeerSet,
    /// The verifier.
    verifier: MockTxVerifier,
    /// The state the pool reads, for committing blocks.
    state: StateService,
    /// The state's tip changes, for waiting on a commit.
    tip_change: ChainTipChange,
    /// The state's latest tip.
    latest_tip: LatestChainTip,
    /// The sync status the pool turns on from.
    syncs: RecentSyncLengths,
    /// The indexer's change stream.
    changes: MempoolTxSubscriber,
}

/// Build today's pool on Mainnet with the genesis block committed. The pool
/// starts inactive.
async fn pool(config: Config, peers: Peers, mode: Mode) -> Harness {
    let network = Network::Mainnet;
    let mock_peers: MockPeerSet = MockService::build()
        .with_max_request_delay(REQUEST_DELAY)
        .for_unit_tests();
    let verifier: MockTxVerifier = MockService::build()
        .with_max_request_delay(REQUEST_DELAY)
        .for_unit_tests();

    let zs::InitializedState {
        state,
        read_state,
        latest_chain_tip,
        chain_tip_change,
        ..
    } = zs::init(zs::Config::ephemeral(), &network, Height::MAX, 0)
        .await
        .expect("ephemeral state initialization succeeds");
    let state = ServiceBuilder::new().buffer(10).service(state);

    let outbound = match peers {
        Peers::Mock => BoxCloneSyncService::new(tower::buffer::Buffer::new(
            BoxService::new(mock_peers.clone()),
            1,
        )),
        Peers::Silent => BoxCloneSyncService::new(tower::service_fn(|_: zn::Request| {
            futures::future::pending::<Result<zn::Response, zn::BoxError>>()
        })),
    };

    let (sync_status, syncs) = crate::components::sync::SyncStatus::new();
    let tx_verifier = tower::buffer::Buffer::new(BoxService::new(verifier.clone()), 1);
    let (pool, subscriber) = match mode {
        Mode::Classic => {
            let (pool, subscriber) = Mempool::new(
                &config,
                false,
                outbound,
                state.clone(),
                BoxCloneService::new(read_state),
                tx_verifier,
                sync_status,
                latest_chain_tip.clone(),
                chain_tip_change.clone(),
                tokio::sync::mpsc::channel(1).0,
                Vec::new(),
                zn::UnconditionalPeers::default(),
            );
            (PoolUnderTest::Classic(Box::new(pool)), subscriber)
        }
        Mode::Core => {
            let transitions = read_state
                .chain_transitions()
                .expect("a writable state publishes chain transitions")
                .clone();
            let (active, active_rx) = watch::channel(false);
            // The announcer's advertisements are answered here, so the peer
            // set only sees the requests these tests name.
            let outbound = BoxCloneSyncService::new(tower::service_fn(move |request| {
                let outbound = outbound.clone();
                async move {
                    match request {
                        zn::Request::AdvertiseTransactionIds(..) => Ok(zn::Response::Nil),
                        request => outbound.oneshot(request).await,
                    }
                }
            }));
            let assembled = core::assemble(
                &config,
                false,
                outbound,
                BoxCloneService::new(read_state),
                &transitions,
                tx_verifier,
                network.clone(),
                Arc::new(AtomicBool::new(true)),
                tokio::sync::mpsc::channel(1).0,
                Exempt::default(),
                active_rx,
            )
            .expect("the test is the only consumer of the chain transitions");
            tokio::spawn(assembled.core.run());
            tokio::spawn(assembled.follower.run());
            (
                PoolUnderTest::Core {
                    service: assembled.mempool,
                    verifier_pool: assembled.verifier_pool,
                    active,
                },
                assembled.subscriber,
            )
        }
    };
    let mut changes = subscriber.subscribe();
    tokio::spawn(async move { while changes.recv().await.is_ok() {} });

    let mut harness = Harness {
        pool,
        peers: mock_peers,
        verifier,
        state,
        tip_change: chain_tip_change,
        latest_tip: latest_chain_tip,
        syncs,
        changes: subscriber,
    };
    harness
        .commit(zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES.as_slice())
        .await;
    harness
}

/// [`pool`] with the default config and a [`Peers::Mock`] peer set, turned on.
async fn active_pool(mode: Mode) -> Harness {
    let mut harness = pool(Config::default(), Peers::Mock, mode).await;
    harness.activate().await;
    harness
}

impl Harness {
    /// Turn the pool on, as a node close to the tip does.
    async fn activate(&mut self) {
        match &mut self.pool {
            PoolUnderTest::Classic(pool) => pool.enable(&mut self.syncs).await,
            PoolUnderTest::Core { active, .. } => {
                active.send_replace(true);
                self.caught_up().await;
            }
        }
        assert!(self.is_enabled().await, "the pool turns on near the tip");
    }

    /// Whether the pool is on, as callers see it: an inactive pool refuses
    /// `FullTransactions`.
    async fn is_enabled(&mut self) -> bool {
        match &self.pool {
            PoolUnderTest::Classic(pool) => pool.is_enabled(),
            PoolUnderTest::Core { .. } => self.call(Request::FullTransactions).await.is_ok(),
        }
    }

    /// Waits until an active core has applied the state's tip. Today's pool
    /// applies it on its next call instead.
    async fn caught_up(&mut self) {
        for _ in 0..(ANSWER_WAIT.as_millis() / 10) {
            let tip = self.latest_tip.best_tip_hash();
            if let Ok(Response::FullTransactions {
                last_seen_tip_hash, ..
            }) = self.call(Request::FullTransactions).await
            {
                if Some(last_seen_tip_hash) == tip {
                    return;
                }
            }
            time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the pool core applies the state's tip within {ANSWER_WAIT:?}");
    }

    /// Starts `request` without awaiting it. Today's pool registers what the
    /// request asks for when it is called. The core does when the returned
    /// future is first polled.
    async fn start_call(
        &mut self,
        request: Request,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Response, BoxError>> + Send>>
    {
        match &mut self.pool {
            PoolUnderTest::Classic(pool) => pool
                .ready()
                .await
                .expect("the pool is always ready")
                .call(request),
            PoolUnderTest::Core { service, .. } => Box::pin(
                service
                    .ready()
                    .await
                    .expect("the pool is always ready")
                    .call(request),
            ),
        }
    }

    /// Commit the serialized block `bytes` and wait for the tip to change.
    async fn commit(&mut self, bytes: &[u8]) {
        let block: Arc<Block> = bytes
            .zcash_deserialize_into()
            .expect("block test vectors are valid");
        self.state
            .ready()
            .await
            .expect("the state is ready")
            .call(zs::Request::CommitCheckpointVerifiedBlock(block.into()))
            .await
            .expect("the test vector commits");
        timeout(ANSWER_WAIT, self.tip_change.wait_for_tip_change())
            .await
            .expect("the tip changes after a commit")
            .expect("the tip channel stays open");
        match &mut self.pool {
            PoolUnderTest::Classic(pool) => pool.dummy_call().await,
            PoolUnderTest::Core { .. } => {
                if self.is_enabled().await {
                    self.caught_up().await;
                }
            }
        }
    }

    /// Send `request` and return its answer.
    async fn call(&mut self, request: Request) -> Result<Response, BoxError> {
        self.start_call(request).await.await
    }

    /// Send `request` and return its answer, which must be `Ok`.
    async fn ask(&mut self, request: Request) -> Response {
        self.call(request).await.expect("the pool answers")
    }

    /// Queue `items` and return the first-stage answer for each.
    async fn queue(&mut self, items: Vec<Gossip>) -> Vec<Result<Receiver, BoxError>> {
        let Response::Queued(answers) = self.ask(Request::Queue(items)).await else {
            panic!("a queue request is answered with `Queued`");
        };
        answers
    }

    /// Queue one item and return its first-stage answer.
    async fn queue_one(&mut self, item: impl Into<Gossip>) -> Result<Receiver, BoxError> {
        let mut answers = self.queue(vec![item.into()]).await;
        assert_eq!(answers.len(), 1, "one answer for each queued item");
        answers.remove(0)
    }

    /// Wait for a queued item's second answer, polling today's pool
    /// meanwhile, as the queue checker does. The core needs no polling.
    async fn outcome(&mut self, mut receiver: Receiver) -> Result<(), BoxError> {
        for _ in 0..(ANSWER_WAIT.as_millis() / 50) {
            if let PoolUnderTest::Classic(pool) = &mut self.pool {
                pool.dummy_call().await;
            }
            if let Ok(answer) = timeout(Duration::from_millis(50), &mut receiver).await {
                return answer.expect("the pool answers each queued item");
            }
        }
        panic!("a queued item is answered within {ANSWER_WAIT:?}");
    }

    /// Answer the next verification with `answer`.
    async fn verify(&mut self, answer: Result<tx::Response, TransactionError>) {
        self.verifier
            .expect_request_that(|request| matches!(request, tx::Request::Mempool { .. }))
            .await
            .respond(answer);
    }

    /// Queue `tx`'s body and pass its verification, claiming it spends
    /// `pool_outpoints` from pooled transactions. Returns the second answer.
    async fn submit(
        &mut self,
        tx: &VerifiedUnminedTx,
        pool_outpoints: Vec<OutPoint>,
    ) -> Result<(), BoxError> {
        let receiver = self
            .queue_one(tx.transaction.clone())
            .await
            .expect("a new body passes the first stage");
        self.verify(Ok(passed(tx, pool_outpoints))).await;
        self.outcome(receiver).await
    }

    /// Queue `tx`'s body and pass its verification. It must be admitted.
    async fn admit(&mut self, tx: &VerifiedUnminedTx) {
        self.submit(tx, Vec::new())
            .await
            .expect("a verified transaction is admitted");
    }

    /// The pool's transaction ids.
    async fn ids(&mut self) -> HashSet<UnminedTxId> {
        let Response::TransactionIds(ids) = self.ask(Request::TransactionIds).await else {
            panic!("`TransactionIds` is answered with ids");
        };
        ids
    }

    /// Which of `ids` a cached rejection covers.
    async fn rejected(
        &mut self,
        ids: impl IntoIterator<Item = UnminedTxId>,
    ) -> HashSet<UnminedTxId> {
        let request = Request::RejectedTransactionIds(ids.into_iter().collect());
        let Response::RejectedTransactionIds(ids) = self.ask(request).await else {
            panic!("`RejectedTransactionIds` is answered with ids");
        };
        ids
    }

    /// The `FullTransactions` answer: the transactions and the tip hash.
    async fn full(&mut self) -> (Vec<VerifiedUnminedTx>, zakura_chain::block::Hash) {
        let Response::FullTransactions {
            transactions,
            last_seen_tip_hash,
            ..
        } = self.ask(Request::FullTransactions).await
        else {
            panic!("`FullTransactions` is answered with transactions");
        };
        (transactions, last_seen_tip_hash)
    }

    /// The current best tip hash.
    fn tip_hash(&self) -> zakura_chain::block::Hash {
        self.latest_tip
            .best_tip_hash()
            .expect("the state has a tip")
    }
}

/// The verifier's passing answer for `tx`.
fn passed(tx: &VerifiedUnminedTx, spent_mempool_outpoints: Vec<OutPoint>) -> tx::Response {
    tx::Response::Mempool {
        transaction: tx.clone(),
        spent_mempool_outpoints,
    }
}

/// Distinct Mainnet transactions that create at least one transparent output.
fn samples(count: usize) -> Vec<VerifiedUnminedTx> {
    let samples: Vec<_> = Network::Mainnet
        .unmined_transactions_in_blocks(1..=10)
        .filter(|tx| !tx.transaction.transaction().outputs().is_empty())
        .take(count)
        .collect();
    assert_eq!(samples.len(), count, "the test vectors hold enough samples");
    samples
}

/// The first output `tx` creates.
fn first_output(tx: &VerifiedUnminedTx) -> OutPoint {
    OutPoint::from_usize(tx.transaction.id().mined_id(), 0)
}

/// A transaction id that no test vector has.
fn unknown_id(index: usize) -> UnminedTxId {
    let mut bytes = [0xee; 32];
    bytes[..8].copy_from_slice(&u64::try_from(index).expect("small index").to_le_bytes());
    UnminedTxId::Legacy(transaction::Hash(bytes))
}

/// A legacy peer.
fn legacy_peer(last_octet: u8) -> QueueSource {
    QueueSource::LegacySocket(([192, 0, 2, last_octet], 8233).into())
}

/// Whether `error` is the disabled pool's error.
fn is_disabled(error: &BoxError) -> bool {
    error.downcast_ref::<MempoolDisabledError>().is_some()
}

async fn inactive_pool_answers_each_request_as_today(mode: Mode) {
    let mut harness = pool(Config::default(), Peers::Mock, mode).await;
    assert!(!harness.is_enabled().await, "a far-behind pool starts off");
    let sample = samples(1).remove(0);
    let id = sample.transaction.id();
    let outpoint = first_output(&sample);

    for request in [
        Request::TransactionIds,
        Request::TakePendingGossipTransactionIds { limit: 10 },
    ] {
        assert!(
            matches!(harness.ask(request).await, Response::TransactionIds(ids) if ids.is_empty())
        );
    }
    for request in [
        Request::TransactionsById([id].into()),
        Request::TransactionsByMinedId([id.mined_id()].into()),
    ] {
        assert!(
            matches!(harness.ask(request).await, Response::Transactions(txs) if txs.is_empty())
        );
    }
    for request in [
        Request::TransactionWithDepsByMinedId(id.mined_id()),
        Request::AwaitOutput(outpoint),
        Request::UnspentOutput(outpoint),
        Request::FullTransactions,
    ] {
        let error = harness
            .call(request)
            .await
            .expect_err("an inactive pool refuses this request");
        assert!(is_disabled(&error), "{error:?}");
    }
    assert!(matches!(
        harness.ask(Request::RejectedTransactionIds([id].into())).await,
        Response::RejectedTransactionIds(ids) if ids.is_empty()
    ));

    // Each queued item is refused at the first stage, in order.
    let answers = harness
        .queue(vec![id.into(), sample.transaction.clone().into()])
        .await;
    assert_eq!(answers.len(), 2);
    for answer in answers {
        assert_eq!(answer.unbox_mempool_error(), MempoolError::Disabled);
    }

    let request = Request::QueueFromPeer {
        transactions: vec![id.into()],
        source: legacy_peer(1),
    };
    assert!(matches!(harness.ask(request).await, Response::Queued(answers) if answers.is_empty()));
    assert!(matches!(
        harness.ask(Request::CheckForVerifiedTransactions).await,
        Response::CheckedForVerifiedTransactions
    ));
    assert!(matches!(
        harness.ask(Request::QueueStats).await,
        Response::QueueStats {
            size: 0,
            bytes: 0,
            usage: 0,
            fully_notified: None,
        }
    ));

    // Nothing reached the peer set or the verifier.
    harness.peers.expect_no_requests().await;
    harness.verifier.expect_no_requests().await;
}

async fn empty_active_pool_answers_each_query_as_today(mode: Mode) {
    let mut harness = active_pool(mode).await;
    let sample = samples(1).remove(0);
    let id = sample.transaction.id();
    let outpoint = first_output(&sample);

    assert!(harness.ids().await.is_empty());
    assert!(matches!(
        harness.ask(Request::TakePendingGossipTransactionIds { limit: 10 }).await,
        Response::TransactionIds(ids) if ids.is_empty()
    ));
    for request in [
        Request::TransactionsById([id].into()),
        Request::TransactionsByMinedId([id.mined_id()].into()),
    ] {
        assert!(
            matches!(harness.ask(request).await, Response::Transactions(txs) if txs.is_empty())
        );
    }
    let error = harness
        .call(Request::TransactionWithDepsByMinedId(id.mined_id()))
        .await
        .expect_err("an absent transaction has no dependencies");
    assert_eq!(error.to_string(), "transaction not found in mempool");

    let (transactions, tip_hash) = harness.full().await;
    assert!(transactions.is_empty());
    assert_eq!(tip_hash, harness.tip_hash(), "the tip the pool last saw");

    assert!(harness.rejected([id]).await.is_empty());
    assert!(matches!(
        harness.ask(Request::QueueStats).await,
        Response::QueueStats {
            size: 0,
            bytes: 0,
            usage: 0,
            fully_notified: None,
        }
    ));
    assert!(matches!(
        harness.ask(Request::UnspentOutput(outpoint)).await,
        Response::TransparentOutput(None)
    ));
    assert!(matches!(
        harness.ask(Request::CheckForVerifiedTransactions).await,
        Response::CheckedForVerifiedTransactions
    ));

    // A wait for an output nobody created stays open.
    let wait = harness.start_call(Request::AwaitOutput(outpoint)).await;
    assert!(timeout(SILENCE, wait).await.is_err());
}

async fn admitted_body_is_served_by_every_query(mode: Mode) {
    let mut harness = active_pool(mode).await;
    let sample = samples(1).remove(0);
    let id = sample.transaction.id();
    let outpoint = first_output(&sample);
    let output = sample.transaction.transaction().outputs()[0].clone();

    // A waiter registered before the admission is answered by it.
    let waiter = tokio::spawn(harness.start_call(Request::AwaitOutput(outpoint)).await);
    harness.admit(&sample).await;
    let answer = timeout(ANSWER_WAIT, waiter)
        .await
        .expect("the waiter is answered at admission")
        .expect("the waiter task does not panic")
        .expect("the waiter gets the output");
    assert!(matches!(answer, Response::UnspentOutput(answered) if answered == output));

    // A wait for an output already in the pool is answered at once.
    let answer = timeout(ANSWER_WAIT, harness.call(Request::AwaitOutput(outpoint)))
        .await
        .expect("a present output is answered at once")
        .expect("the output is present");
    assert!(matches!(answer, Response::UnspentOutput(answered) if answered == output));

    assert_eq!(harness.ids().await, [id].into());
    for request in [
        Request::TransactionsById([id, unknown_id(0)].into()),
        Request::TransactionsByMinedId([id.mined_id()].into()),
    ] {
        assert!(matches!(
            harness.ask(request).await,
            Response::Transactions(txs) if txs == vec![sample.transaction.clone()]
        ));
    }
    let Response::TransactionWithDeps {
        transaction,
        dependencies,
    } = harness
        .ask(Request::TransactionWithDepsByMinedId(id.mined_id()))
        .await
    else {
        panic!("a pooled transaction is answered with its dependencies");
    };
    assert_eq!(transaction.transaction, sample.transaction);
    assert!(dependencies.is_empty());

    let (transactions, tip_hash) = harness.full().await;
    assert_eq!(
        transactions
            .iter()
            .map(|tx| tx.transaction.id())
            .collect::<Vec<_>>(),
        vec![id]
    );
    assert_eq!(tip_hash, harness.tip_hash());

    let expected_bytes = sample.transaction.size();
    assert!(matches!(
        harness.ask(Request::QueueStats).await,
        Response::QueueStats { size: 1, bytes, usage, fully_notified: None }
            if bytes == expected_bytes && usage == expected_bytes
    ));
    let tip = harness.tip_hash();
    assert!(matches!(
        harness.ask(Request::UnspentOutput(outpoint)).await,
        Response::TransparentOutput(Some(CreatedOrSpent::Created { output: answered, tx_version, last_seen_hash }))
            if answered == output
                && tx_version == sample.transaction.transaction().version()
                && last_seen_hash == tip
    ));

    // Today's admission waits for gossip until it is taken, a limit at a
    // time. The core announces from its change log, so it has nothing to
    // take here: a difference made on purpose.
    let gossiped: HashSet<UnminedTxId> = match mode {
        Mode::Classic => [id].into(),
        Mode::Core => HashSet::new(),
    };
    assert!(matches!(
        harness.ask(Request::TakePendingGossipTransactionIds { limit: 0 }).await,
        Response::TransactionIds(ids) if ids.is_empty()
    ));
    assert!(matches!(
        harness.ask(Request::TakePendingGossipTransactionIds { limit: 1 }).await,
        Response::TransactionIds(ids) if ids == gossiped
    ));
    assert!(matches!(
        harness.ask(Request::TakePendingGossipTransactionIds { limit: 1 }).await,
        Response::TransactionIds(ids) if ids.is_empty()
    ));

    // A pooled transaction is refused at the first stage, as a body or an id.
    assert_eq!(
        harness
            .queue_one(sample.transaction.clone())
            .await
            .unbox_mempool_error(),
        MempoolError::InMempool
    );
    assert_eq!(
        harness.queue_one(id).await.unbox_mempool_error(),
        MempoolError::InMempool
    );

    // A peer that offers it again is not asked for it.
    let request = Request::QueueFromPeer {
        transactions: vec![id.into()],
        source: legacy_peer(1),
    };
    assert!(matches!(harness.ask(request).await, Response::Queued(answers) if answers.is_empty()));
    harness.peers.expect_no_requests().await;
    harness.verifier.expect_no_requests().await;
}

async fn queued_id_is_fetched_then_verified(mode: Mode) {
    let mut harness = active_pool(mode).await;
    let sample = samples(1).remove(0);
    let id = sample.transaction.id();

    let receiver = harness
        .queue_one(id)
        .await
        .expect("a new id passes the first stage");
    harness
        .peers
        .expect_request(zn::Request::TransactionsById([id].into()))
        .await
        .respond(zn::Response::Transactions(vec![
            zn::InventoryResponse::Available((sample.transaction.clone(), None)),
        ]));
    harness.verify(Ok(passed(&sample, Vec::new()))).await;
    harness
        .outcome(receiver)
        .await
        .expect("a fetched and verified transaction is admitted");
    assert_eq!(harness.ids().await, [id].into());
}

async fn failed_fetch_answers_the_receiver_and_caches_nothing(mode: Mode) {
    let mut harness = active_pool(mode).await;
    let id = samples(1).remove(0).transaction.id();

    let receiver = harness
        .queue_one(id)
        .await
        .expect("a new id passes the first stage");
    harness
        .peers
        .expect_request(zn::Request::TransactionsById([id].into()))
        .await
        .respond(zn::Response::Transactions(Vec::new()));
    let error = harness
        .outcome(receiver)
        .await
        .expect_err("a fetch that returns nothing fails");
    assert!(
        error.to_string().starts_with("failed to validate tx: "),
        "{error}"
    );

    assert!(harness.rejected([id]).await.is_empty());
    harness
        .queue_one(id)
        .await
        .expect("a failed fetch leaves the id free to queue again");
}

async fn failed_verification_is_answered_then_cached_for_the_tip(mode: Mode) {
    let mut harness = active_pool(mode).await;
    let sample = samples(1).remove(0);
    let id = sample.transaction.id();

    let receiver = harness
        .queue_one(sample.transaction.clone())
        .await
        .expect("a new body passes the first stage");
    harness.verify(Err(TransactionError::BadBalance)).await;
    let error = harness
        .outcome(receiver)
        .await
        .expect_err("an invalid transaction is refused");
    assert!(
        error.to_string().starts_with("failed to validate tx: "),
        "{error}"
    );

    assert_eq!(harness.rejected([id, unknown_id(0)]).await, [id].into());
    for item in [Gossip::from(id), sample.transaction.clone().into()] {
        assert!(matches!(
            harness.queue_one(item).await.unbox_mempool_error(),
            MempoolError::StorageExactTip(ExactTipRejectionError::FailedVerification(
                TransactionError::BadBalance
            ))
        ));
    }
    assert!(harness.ids().await.is_empty());
}

async fn too_large_rejection_is_answered_at_the_receiver_stage(mode: Mode) {
    let config = Config {
        max_transaction_bytes: 1,
        ..Config::default()
    };
    let mut harness = pool(config, Peers::Mock, mode).await;
    harness.activate().await;
    let sample = samples(1).remove(0);
    let id = sample.transaction.id();

    let receiver = harness
        .queue_one(sample.transaction.clone())
        .await
        .expect("the size check comes after the first stage");
    let error = harness
        .outcome(receiver)
        .await
        .expect_err("an oversized transaction is refused");
    assert!(
        error
            .to_string()
            .contains("exceeding the configured mempool maximum"),
        "{error}"
    );
    assert_eq!(harness.rejected([id]).await, [id].into());

    // The cached rejection still passes the first stage, as a body or an id,
    // and completes the receiver with the cached error.
    for item in [sample.transaction.clone().into(), Gossip::from(id)] {
        let receiver = harness
            .queue_one(item)
            .await
            .expect("a cached size rejection is a receiver-stage answer");
        let error = harness
            .outcome(receiver)
            .await
            .expect_err("the cached rejection is the answer");
        assert!(
            error
                .to_string()
                .contains("exceeding the configured mempool maximum"),
            "{error}"
        );
    }
    harness.peers.expect_no_requests().await;
    harness.verifier.expect_no_requests().await;
}

async fn pool_dependency_limits_are_answered_then_cached_for_the_tip(mode: Mode) {
    let mut harness = active_pool(mode).await;
    let [a, b, c, d, e, f]: [VerifiedUnminedTx; 6] = samples(6).try_into().expect("six samples");

    // A parent the pool does not hold. Today's pool refuses it and caches
    // the refusal. The core verifies it again instead, because the parent
    // may have been mined since the verifier looked: a difference made on
    // purpose, which the machine tests cover.
    if mode == Mode::Classic {
        let error = harness
            .submit(&a, vec![OutPoint::from_usize(unknown_id(0).mined_id(), 0)])
            .await
            .expect_err("a missing pool parent is refused");
        let missing = MempoolError::StorageEffectsTip(SameEffectsTipRejectionError::MissingOutput);
        assert_eq!(error.unbox_mempool_error(), missing);
        assert_eq!(
            harness
                .queue_one(a.transaction.id())
                .await
                .unbox_mempool_error(),
            missing
        );
    }

    // A chain of three: b, then c spending b, then d spending c.
    harness.admit(&b).await;
    harness
        .submit(&c, vec![first_output(&b)])
        .await
        .expect("one pooled ancestor is allowed");
    harness
        .submit(&d, vec![first_output(&c)])
        .await
        .expect("two pooled ancestors are allowed");
    let Response::TransactionWithDeps { dependencies, .. } = harness
        .ask(Request::TransactionWithDepsByMinedId(
            d.transaction.id().mined_id(),
        ))
        .await
    else {
        panic!("a pooled transaction is answered with its dependencies");
    };
    assert_eq!(dependencies, [c.transaction.id().mined_id()].into());

    // A third ancestor is refused.
    let error = harness
        .submit(&e, vec![first_output(&d)])
        .await
        .expect_err("three pooled ancestors are refused");
    assert_eq!(
        error.unbox_mempool_error(),
        MempoolError::StorageEffectsTip(SameEffectsTipRejectionError::TooManyAncestors)
    );

    // A fourth member of a full package is refused.
    let error = harness
        .submit(&f, vec![first_output(&b)])
        .await
        .expect_err("a package holds at most three transactions");
    let package =
        MempoolError::StorageEffectsTip(SameEffectsTipRejectionError::TooManyPackageTransactions);
    assert_eq!(error.unbox_mempool_error(), package);
    assert_eq!(
        harness
            .queue_one(f.transaction.clone())
            .await
            .unbox_mempool_error(),
        package
    );

    assert_eq!(
        harness.ids().await,
        [b.transaction.id(), c.transaction.id(), d.transaction.id()].into()
    );
}

async fn mined_transaction_is_rejected_for_the_chain_and_tip_rejections_end(mode: Mode) {
    let mut harness = pool(Config::default(), Peers::Silent, mode).await;
    harness.activate().await;
    // Block 1 starts an upgrade, which resets the pool, so the test starts
    // after it.
    harness
        .commit(zakura_test::vectors::BLOCK_MAINNET_1_BYTES.as_slice())
        .await;
    let mined = Network::Mainnet
        .unmined_transactions_in_blocks(2..=2)
        .next()
        .expect("block 2 has a transaction");
    let mined_id = mined.transaction.id();
    let invalid = Network::Mainnet
        .unmined_transactions_in_blocks(3..=3)
        .next()
        .expect("block 3 has a transaction");
    let invalid_id = invalid.transaction.id();

    harness.admit(&mined).await;
    let receiver = harness
        .queue_one(invalid.transaction.clone())
        .await
        .expect("a new body passes the first stage");
    harness.verify(Err(TransactionError::BadBalance)).await;
    harness
        .outcome(receiver)
        .await
        .expect_err("an invalid transaction is refused");
    assert_eq!(
        harness.rejected([mined_id, invalid_id]).await,
        [invalid_id].into()
    );

    harness
        .commit(zakura_test::vectors::BLOCK_MAINNET_2_BYTES.as_slice())
        .await;

    // The mined transaction left the pool and is rejected while the chain
    // grows. The tip-scoped rejection ended with the tip.
    assert!(harness.ids().await.is_empty());
    assert_eq!(
        harness.rejected([mined_id, invalid_id]).await,
        [mined_id].into()
    );
    assert_eq!(
        harness.queue_one(mined_id).await.unbox_mempool_error(),
        MempoolError::StorageEffectsChain(SameEffectsChainRejectionError::Mined)
    );
    harness
        .queue_one(invalid_id)
        .await
        .expect("a tip-scoped rejection ends with the tip");

    let (transactions, tip_hash) = harness.full().await;
    assert!(transactions.is_empty());
    assert_eq!(tip_hash, harness.tip_hash(), "the new tip's hash");
}

async fn same_id_twice_is_already_queued_at_the_first_stage(mode: Mode) {
    let mut harness = pool(Config::default(), Peers::Silent, mode).await;
    harness.activate().await;
    let sample = samples(1).remove(0);
    let id = sample.transaction.id();

    let mut answers = harness.queue(vec![id.into(), id.into()]).await;
    assert_eq!(answers.len(), 2);
    assert_eq!(
        answers.pop().expect("two answers").unbox_mempool_error(),
        MempoolError::AlreadyQueued
    );
    answers
        .pop()
        .expect("two answers")
        .expect("the first copy is queued");

    assert_eq!(
        harness.queue_one(id).await.unbox_mempool_error(),
        MempoolError::AlreadyQueued
    );
    // Today's pool refuses a body of an id it is fetching. The core joins the
    // body to that id instead, and answers it with the same verdict at the
    // second stage: the one difference the adapter makes on purpose.
    let body = harness.queue_one(sample.transaction.clone()).await;
    match mode {
        Mode::Classic => {
            assert_eq!(body.unbox_mempool_error(), MempoolError::AlreadyQueued)
        }
        Mode::Core => {
            body.expect("a body of an undecided id joins it");
        }
    }
}

async fn full_queue_is_refused_at_the_first_stage(mode: Mode) {
    let mut harness = pool(Config::default(), Peers::Silent, mode).await;
    harness.activate().await;

    let answers = harness
        .queue(
            (0..MAX_INBOUND_CONCURRENCY)
                .map(|i| unknown_id(i).into())
                .collect(),
        )
        .await;
    assert_eq!(answers.len(), MAX_INBOUND_CONCURRENCY);
    assert!(answers.iter().all(Result::is_ok), "500 fetches fit");

    let extra = MAX_INBOUND_CONCURRENCY;
    for item in [
        Gossip::from(unknown_id(extra)),
        samples(1).remove(0).transaction.into(),
    ] {
        assert_eq!(
            harness.queue_one(item).await.unbox_mempool_error(),
            MempoolError::FullQueue
        );
    }
    // A peer's offer is dropped without an answer.
    let request = Request::QueueFromPeer {
        transactions: vec![unknown_id(extra).into()],
        source: legacy_peer(1),
    };
    assert!(matches!(harness.ask(request).await, Response::Queued(answers) if answers.is_empty()));
    assert!(harness.ids().await.is_empty());
}

async fn one_peer_holds_at_most_five_fetches(mode: Mode) {
    let mut harness = pool(Config::default(), Peers::Silent, mode).await;
    harness.activate().await;
    let offered: Vec<_> = (0..=MAX_INBOUND_CONCURRENCY_PER_PEER)
        .map(unknown_id)
        .collect();

    let request = Request::QueueFromPeer {
        transactions: offered.iter().copied().map(Gossip::from).collect(),
        source: legacy_peer(1),
    };
    assert!(matches!(harness.ask(request).await, Response::Queued(answers) if answers.is_empty()));

    // The first five are in flight. The sixth was dropped, so the crawler can
    // still queue it.
    assert_eq!(
        harness.queue_one(offered[0]).await.unbox_mempool_error(),
        MempoolError::AlreadyQueued
    );
    harness
        .queue_one(offered[MAX_INBOUND_CONCURRENCY_PER_PEER])
        .await
        .expect("the peer's sixth offer was not queued");

    // Another peer has its own five.
    let others: Vec<_> = (100..100 + MAX_INBOUND_CONCURRENCY_PER_PEER)
        .map(unknown_id)
        .collect();
    let request = Request::QueueFromPeer {
        transactions: others.iter().copied().map(Gossip::from).collect(),
        source: legacy_peer(2),
    };
    harness.ask(request).await;
    for id in others {
        assert_eq!(
            harness.queue_one(id).await.unbox_mempool_error(),
            MempoolError::AlreadyQueued
        );
    }
}

/// Today's pool only. Its waits register when the request is called, so the
/// test can hold 65,536 of them without polling. The core's register when
/// the call is first polled, and the core drops abandoned waits before it
/// refuses a new outpoint, which is a difference made on purpose: see the
/// port's `PoolCommand::AwaitOutput`.
async fn output_waits_are_limited_to_distinct_outpoints(mode: Mode) {
    if mode == Mode::Core {
        return;
    }
    let mut harness = active_pool(mode).await;
    let outpoint = |index: usize| OutPoint::from_usize(unknown_id(index).mined_id(), 0);

    // Each wait holds its place while its future is alive.
    let mut waits = Vec::with_capacity(MAX_PENDING_OUTPUTS);
    for index in 0..MAX_PENDING_OUTPUTS {
        waits.push(
            harness
                .start_call(Request::AwaitOutput(outpoint(index)))
                .await,
        );
    }

    let error = harness
        .call(Request::AwaitOutput(outpoint(MAX_PENDING_OUTPUTS)))
        .await
        .expect_err("a new outpoint past the limit is refused");
    assert!(
        error.downcast_ref::<PendingOutputsFull>().is_some(),
        "{error:?}"
    );

    // Another wait for an outpoint already waited on is accepted.
    let again = harness.start_call(Request::AwaitOutput(outpoint(0))).await;
    assert!(timeout(SILENCE, again).await.is_err());

    // Abandoned waits keep their places until the next scheduled prune, so a
    // new outpoint is still refused right after every waiter left.
    drop(waits);
    time::sleep(Duration::from_millis(10)).await;
    let error = harness
        .call(Request::AwaitOutput(outpoint(MAX_PENDING_OUTPUTS)))
        .await
        .expect_err("abandoned waits are not pruned before the refusal");
    assert!(
        error.downcast_ref::<PendingOutputsFull>().is_some(),
        "{error:?}"
    );
}

/// The indexer's change stream reports an admission as added and a refused
/// verification as invalidated, one event each.
async fn change_stream_reports_admissions_and_refusals(mode: Mode) {
    let mut harness = active_pool(mode).await;
    let mut changes = harness.changes.subscribe();
    let [admitted, refused]: [VerifiedUnminedTx; 2] = samples(2).try_into().expect("two samples");

    harness.admit(&admitted).await;
    let receiver = harness
        .queue_one(refused.transaction.clone())
        .await
        .expect("a new body passes the first stage");
    harness.verify(Err(TransactionError::BadBalance)).await;
    harness
        .outcome(receiver)
        .await
        .expect_err("an invalid transaction is refused");

    let mut events: Vec<(MempoolChangeKind, HashSet<UnminedTxId>)> = Vec::new();
    while events.len() < 2 {
        // Today's pool reports a refusal on its next poll, after it answered
        // the receiver, so it is polled as the queue checker polls it.
        harness.ask(Request::CheckForVerifiedTransactions).await;
        let change: MempoolChange = timeout(ANSWER_WAIT, changes.recv())
            .await
            .expect("the change stream reports both")
            .expect("the change stream stays open");
        events.push((change.kind(), change.tx_ids().clone()));
    }
    assert_eq!(
        events,
        vec![
            (MempoolChangeKind::Added, [admitted.transaction.id()].into()),
            (
                MempoolChangeKind::Invalidated,
                [refused.transaction.id()].into()
            ),
        ]
    );
}

/// The core only: a body whose id is already being verified joins that
/// verification, and both submissions get its verdict at the second stage.
/// Today's pool refuses the second one as already queued.
#[tokio::test(flavor = "multi_thread")]
async fn a_body_joins_an_undecided_id_and_gets_its_verdict() {
    let mut harness = active_pool(Mode::Core).await;
    let sample = samples(1).remove(0);

    let first = harness
        .queue_one(sample.transaction.clone())
        .await
        .expect("a new body passes the first stage");
    let second = harness
        .queue_one(sample.transaction.clone())
        .await
        .expect("a second body joins the first");
    harness.verify(Ok(passed(&sample, Vec::new()))).await;
    harness
        .outcome(first)
        .await
        .expect("the transaction is admitted");
    harness
        .outcome(second)
        .await
        .expect("the joined body gets the same verdict");
    // One verification served both.
    harness.verifier.expect_no_requests().await;
}

/// The core only: the transaction verifier's pool handle answers an output
/// lookup at once, with the output or `OutputNotInPool`, where today's pool
/// makes it wait.
#[tokio::test(flavor = "multi_thread")]
async fn the_verifiers_pool_handle_answers_at_once() {
    let mut harness = active_pool(Mode::Core).await;
    let sample = samples(1).remove(0);
    harness.admit(&sample).await;
    let PoolUnderTest::Core { verifier_pool, .. } = &harness.pool else {
        unreachable!("the harness runs the core");
    };

    let present = verifier_pool
        .clone()
        .oneshot(Request::AwaitOutput(first_output(&sample)))
        .await
        .expect("a pooled output is answered");
    assert!(matches!(present, Response::UnspentOutput(_)));

    let missing = OutPoint::from_usize(unknown_id(0).mined_id(), 0);
    let error = timeout(
        SILENCE,
        verifier_pool.clone().oneshot(Request::AwaitOutput(missing)),
    )
    .await
    .expect("a missing output is answered at once")
    .expect_err("nothing creates it");
    assert!(
        error
            .downcast_ref::<zakura_node_services::mempool::OutputNotInPool>()
            .is_some(),
        "{error:?}"
    );
}

both_pools!(
    change_stream_reports_admissions_and_refusals,
    inactive_pool_answers_each_request_as_today,
    empty_active_pool_answers_each_query_as_today,
    admitted_body_is_served_by_every_query,
    queued_id_is_fetched_then_verified,
    failed_fetch_answers_the_receiver_and_caches_nothing,
    failed_verification_is_answered_then_cached_for_the_tip,
    too_large_rejection_is_answered_at_the_receiver_stage,
    pool_dependency_limits_are_answered_then_cached_for_the_tip,
    mined_transaction_is_rejected_for_the_chain_and_tip_rejections_end,
    same_id_twice_is_already_queued_at_the_first_stage,
    full_queue_is_refused_at_the_first_stage,
    one_peer_holds_at_most_five_fetches,
    output_waits_are_limited_to_distinct_outpoints,
);
