//! `tx-relay/adapter-off-while-live`: once a peer's live `tx-relay` entry is
//! published, the legacy adapter moves none of that peer's transactions. It
//! refuses what the peer sends as it arrives, and does not choose the peer
//! for its own adverts and requests. Once the session ends the adapter
//! carries the peer's transactions again. Block adverts and block requests
//! keep flowing for the same peer throughout.
//!
//! The arrival test drives the real adapter's sink and worker through the
//! interface the transport uses, and holds the live-session record itself.
//! The last test runs two real nodes, so it also covers a node handing
//! `tx-relay`'s record to its adapter.

use std::{
    collections::HashSet,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};

use tokio::{sync::mpsc, task::JoinHandle};
use tower::ServiceExt;
use zakura_chain::{
    block,
    parameters::NetworkUpgrade,
    serialization::MAX_PROTOCOL_MESSAGE_LEN,
    transaction::{LockTime, Transaction, UnminedTx},
};

use super::{
    test_node::{Counters, RelayNode, TestPool},
    TxRelaySessions,
};
use crate::{
    zakura::{
        testkit::{await_until, TEST_NET_TIMEOUT},
        LegacyGossipAdapter, LegacyGossipFrame, LegacyGossipSink, LegacyRequestAdapter,
        LegacyRequestFrame, LegacyResponseCodec, Service as _, ZakuraPeerId,
        ZakuraSupervisorHandle, ZAKURA_STREAM_GOSSIP, ZAKURA_STREAM_LEGACY_REQUESTS,
    },
    BoxError, InventoryResponse, PeerSource, Request, Response,
};

/// A test peer id from one byte.
fn peer(byte: u8) -> ZakuraPeerId {
    ZakuraPeerId::new(vec![byte; 32]).expect("32-byte node id is valid")
}

/// An empty v5 transaction, told apart from the others by `n`.
fn transaction(n: u32) -> UnminedTx {
    UnminedTx::from(Transaction::V5 {
        network_upgrade: NetworkUpgrade::Nu5,
        lock_time: LockTime::min_lock_time_timestamp(),
        expiry_height: block::Height(n),
        inputs: Vec::new(),
        outputs: Vec::new(),
        sapling_shielded_data: None,
        orchard_shielded_data: None,
    })
}

/// A block hash from one byte.
fn block_hash(byte: u8) -> block::Hash {
    block::Hash([byte; 32])
}

/// The node behind the adapter: it reports each request it is handed, and
/// answers as a node that holds `stock` and no blocks.
#[derive(Clone)]
struct Node {
    seen: mpsc::UnboundedSender<Request>,
    stock: Arc<Vec<UnminedTx>>,
}

impl Node {
    /// A node holding `stock`, and the requests it is handed.
    fn new(stock: Vec<UnminedTx>) -> (Self, mpsc::UnboundedReceiver<Request>) {
        let (seen, requests) = mpsc::unbounded_channel();
        let node = Self {
            seen,
            stock: Arc::new(stock),
        };
        (node, requests)
    }

    /// What this node answers `request` with: a request the adapter
    /// serves as its frame says, and anything else with nothing.
    fn answer(&self, request: &Request) -> Response {
        match LegacyRequestFrame::from_request(request.clone()) {
            Ok(frame) => self.answer_frame(&frame),
            Err(_) => Response::Nil,
        }
    }

    /// What this node answers the request `frame` with.
    fn answer_frame(&self, frame: &LegacyRequestFrame) -> Response {
        match frame {
            LegacyRequestFrame::TransactionsById(ids) => Response::Transactions(
                ids.iter()
                    .map(|id| match self.stock.iter().find(|tx| tx.id() == *id) {
                        Some(tx) => InventoryResponse::Available((tx.clone(), None)),
                        None => InventoryResponse::Missing(*id),
                    })
                    .collect(),
            ),
            LegacyRequestFrame::BlocksByHash(hashes) => Response::Blocks(
                hashes
                    .iter()
                    .copied()
                    .map(InventoryResponse::Missing)
                    .collect(),
            ),
            LegacyRequestFrame::FindBlocks { .. } => Response::BlockHashes(Vec::new()),
            LegacyRequestFrame::FindHeaders { .. } => Response::BlockHeaders(Vec::new()),
            LegacyRequestFrame::MempoolTransactionIds => {
                Response::TransactionIds(self.stock.iter().map(UnminedTx::id).collect())
            }
            LegacyRequestFrame::Ping | LegacyRequestFrame::PushTransaction(_) => Response::Nil,
        }
    }
}

impl tower::Service<Request> for Node {
    type Response = Response;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Response, BoxError>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let _ = self.seen.send(request.clone());
        let answer = self.answer(&request);
        Box::pin(async move { Ok(answer) })
    }
}

/// Waits for the next request `node` is handed.
async fn next_seen(requests: &mut mpsc::UnboundedReceiver<Request>) -> Result<Request, BoxError> {
    tokio::time::timeout(TEST_NET_TIMEOUT, requests.recv())
        .await?
        .ok_or_else(|| "the node stand-in is alive".into())
}

/// Every request `node` has been handed so far.
fn all_seen(requests: &mut mpsc::UnboundedReceiver<Request>) -> Vec<Request> {
    std::iter::from_fn(|| requests.try_recv().ok()).collect()
}

/// Whether `request` is transaction work.
fn is_transaction_work(request: &Request) -> bool {
    matches!(
        request,
        Request::AdvertiseTransactionIds(..)
            | Request::TransactionsById(..)
            | Request::TransactionsByIdFrom { .. }
            | Request::MempoolTransactionIds
            | Request::PushTransaction(..)
    )
}

/// A supervisor whose adapters read `record`.
fn supervisor(record: &TxRelaySessions) -> ZakuraSupervisorHandle {
    ZakuraSupervisorHandle::new(8).with_tx_relay_sessions(Some(record.clone()))
}

/// The adapter's inbound side on `supervisor`: its sink, with its worker in
/// front of `node`.
fn sink(supervisor: &ZakuraSupervisorHandle, node: Node) -> Arc<LegacyGossipSink> {
    Arc::new(LegacyGossipSink::spawn(node, supervisor.clone()))
}

/// Hands `frame` from `from` to `sink`, as the gossip stream's reader does.
fn deliver(
    sink: &LegacyGossipSink,
    from: &ZakuraPeerId,
    frame: LegacyGossipFrame,
) -> Result<(), BoxError> {
    sink.deliver_frame(from.clone(), ZAKURA_STREAM_GOSSIP, frame.encode_frame()?)
        .map_err(|_| "the sink accepts a well-formed advert without a fault".into())
}

/// Starts `request` from `from` on `sink`, as the request stream's worker
/// does, and returns its decoded answer once it comes. The sink must answer
/// without rejecting it, so the peer is not penalised.
fn ask(
    sink: &Arc<LegacyGossipSink>,
    from: &ZakuraPeerId,
    request: LegacyRequestFrame,
) -> JoinHandle<Result<Response, BoxError>> {
    static NEXT_REQUEST: AtomicU64 = AtomicU64::new(1);
    let request_id = NEXT_REQUEST.fetch_add(1, Ordering::Relaxed);
    let (sink, from) = (sink.clone(), from.clone());
    tokio::spawn(async move {
        let cap = u32::try_from(MAX_PROTOCOL_MESSAGE_LEN)?;
        let frames = sink
            .as_request_response()
            .ok_or("the sink serves requests")?
            .request_frame(
                from,
                ZAKURA_STREAM_LEGACY_REQUESTS,
                request_id,
                cap,
                cap,
                request.encode_frame()?,
            )
            .await
            .map_err(|_| "the sink answers without rejecting the request")?;
        Ok(request.decode_answer(request_id, frames)?)
    })
}

/// Waits for an answer [`ask`] started.
async fn answer(asked: JoinHandle<Result<Response, BoxError>>) -> Result<Response, BoxError> {
    tokio::time::timeout(TEST_NET_TIMEOUT, asked).await??
}

/// The transaction requests a peer can send the adapter, and the answer
/// each gets in place of service while `tx-relay` owns that peer.
fn transaction_requests() -> Vec<(LegacyRequestFrame, Response)> {
    let id = transaction(90).id();
    vec![
        (
            LegacyRequestFrame::TransactionsById(vec![id]),
            Response::Transactions(vec![InventoryResponse::Missing(id)]),
        ),
        (
            LegacyRequestFrame::MempoolTransactionIds,
            Response::TransactionIds(Vec::new()),
        ),
        (
            LegacyRequestFrame::PushTransaction(transaction(91)),
            Response::Nil,
        ),
    ]
}

/// Each transaction message the adapter receives counts once by its wire
/// type as it decodes: a mempool answer that ends with no frame counts
/// nothing, an empty one sent as `Nil` counts as an id list (type 16), two
/// missing lists count two, and a transaction counts one.
#[test]
fn each_received_transaction_message_counts_once_by_its_type() -> Result<(), BoxError> {
    let counters = Counters::default();
    let _recorder = metrics::set_default_local_recorder(&counters);
    let count = |message_type: &str| {
        counters.get(
            "zakura.p2p.legacy.inbound.tx_messages",
            &[("type", message_type)],
        )
    };
    let cap = u32::try_from(MAX_PROTOCOL_MESSAGE_LEN)?;
    let encode = |response: Response| LegacyResponseCodec::encode_response(1, response, cap, cap);

    LegacyRequestFrame::MempoolTransactionIds.decode_answer(1, Vec::new())?;
    assert_eq!(count("16"), 0, "an empty stream carried no message");
    LegacyRequestFrame::MempoolTransactionIds.decode_answer(1, encode(Response::Nil)?)?;
    assert_eq!(count("16"), 1, "a Nil mempool answer counts as an id list");
    assert_eq!(count("18"), 0);

    let (first, second, held) = (transaction(70), transaction(71), transaction(72));
    let mut frames = encode(Response::Transactions(vec![InventoryResponse::Missing(
        first.id(),
    )]))?;
    frames.extend(encode(Response::Transactions(vec![
        InventoryResponse::Missing(second.id()),
    ]))?);
    frames.extend(encode(Response::Transactions(vec![
        InventoryResponse::Available((held.clone(), None)),
    ]))?);
    LegacyRequestFrame::TransactionsById(vec![first.id(), second.id(), held.id()])
        .decode_answer(1, frames)?;
    assert_eq!(count("8"), 2, "each missing list counts");
    assert_eq!(count("6"), 1, "the transaction counts once");
    Ok(())
}

/// A transaction id advert.
fn tx_advert(n: u32) -> LegacyGossipFrame {
    LegacyGossipFrame::AdvertiseTransactionIds(vec![transaction(n).id()])
}

/// With a peer's entry published, everything of that peer's that moves
/// transactions stops when it arrives, with no penalty: a transaction id
/// advert (type 2) is dropped, a `TransactionsById` request (type 4) is
/// answered as missing, a mempool id request (type 11) is answered empty,
/// and a pushed transaction (type 13) is answered as done and its body
/// dropped. None of it reaches the node. The same peer's block advert and
/// block requests still do, and so does another peer's transaction advert.
#[tokio::test]
async fn transaction_work_from_a_live_peer_stops_on_arrival() -> Result<(), BoxError> {
    let record = TxRelaySessions::default();
    let (node, mut requests) = Node::new(Vec::new());
    let sink = sink(&supervisor(&record), node);
    let (live, other) = (peer(1), peer(2));
    let _entry = record.open(live.clone(), 1, 1);

    deliver(&sink, &live, tx_advert(1))?;
    deliver(&sink, &other, tx_advert(2))?;
    deliver(
        &sink,
        &live,
        LegacyGossipFrame::AdvertiseBlock(block_hash(1)),
    )?;
    for (request, expected) in transaction_requests() {
        assert_eq!(
            answer(ask(&sink, &live, request.clone())).await?,
            expected,
            "{request}"
        );
    }
    let blocks = answer(ask(
        &sink,
        &live,
        LegacyRequestFrame::BlocksByHash(vec![block_hash(2)]),
    ))
    .await?;
    let found = answer(ask(
        &sink,
        &live,
        LegacyRequestFrame::FindBlocks {
            known_blocks: vec![block_hash(3)],
            stop: None,
        },
    ))
    .await?;

    assert_eq!(
        blocks,
        Response::Blocks(vec![InventoryResponse::Missing(block_hash(2))]),
        "the node answers the live peer's block request"
    );
    assert_eq!(found, Response::BlockHashes(Vec::new()));
    let seen = all_seen(&mut requests);
    assert!(
        matches!(
            seen.as_slice(),
            [
                Request::AdvertiseTransactionIds(_, Some(PeerSource::Zakura(from))),
                Request::AdvertiseBlock(advert, Some(PeerSource::Zakura(_))),
                Request::BlocksByHash(_),
                Request::FindBlocks { .. },
            ] if *from == other && advert.hash == block_hash(1)
        ),
        "only the other peer's transactions and the live peer's blocks reach the node: {seen:?}"
    );
    Ok(())
}

/// Broadcasts `frame` through the adapter on `supervisor`, as the node's
/// mempool and block gossip do.
async fn broadcast(
    supervisor: &ZakuraSupervisorHandle,
    frame: LegacyGossipFrame,
) -> Result<(), BoxError> {
    let request = match frame {
        LegacyGossipFrame::AdvertiseBlock(hash) => Request::AdvertiseBlock(hash.into(), None),
        LegacyGossipFrame::AdvertiseTransactionIds(ids) => {
            Request::AdvertiseTransactionIds(ids.into_iter().collect(), None)
        }
    };
    LegacyGossipAdapter::new(supervisor.clone())
        .oneshot(request)
        .await?;
    Ok(())
}

/// The node's transaction requests to a peer: by id, its mempool ids, and a
/// push.
fn outbound_transaction_requests() -> Vec<Request> {
    vec![
        Request::TransactionsById(HashSet::from([transaction(1).id()])),
        Request::MempoolTransactionIds,
        Request::PushTransaction(transaction(2), None),
    ]
}

/// On two real nodes that both run `tx-relay` beside the adapter, once each
/// has the other's session published, neither adapter moves transactions
/// between them: a transaction advert is not sent, and transaction
/// requests choose no peer. Block adverts and block requests still flow
/// between them, and the connection stays up. When one node comes back on
/// a build without `tx-relay`, the other's adapter carries its transactions
/// again.
#[tokio::test]
async fn two_real_nodes_keep_blocks_on_the_adapter_and_transactions_off_it() -> Result<(), BoxError>
{
    let _guard = zakura_test::init();
    let held = transaction(1);
    let (a_node, mut a_requests) = Node::new(vec![held.clone()]);
    let a_pool = TestPool::new(Default::default());
    let a = RelayNode::spawn_loaded(91, Some(a_pool.handle.clone()), a_node, None, |builder| {
        builder
    })
    .await?;
    let mut b = RelayNode::spawn(92, Some(TestPool::new(Default::default()))).await?;
    let b_requests = &mut b.inbound;
    a.node.connect_native(&b.node, TEST_NET_TIMEOUT).await?;
    let a_id = ZakuraPeerId::new(a.node.node_addr().await.id.as_bytes().to_vec())?;
    let b_id = ZakuraPeerId::new(b.node.node_addr().await.id.as_bytes().to_vec())?;
    let (a_record, b_record) = (
        a.sessions.clone().ok_or("A runs tx-relay")?,
        b.sessions.clone().ok_or("B runs tx-relay")?,
    );
    await_until("both sessions are live", TEST_NET_TIMEOUT, || {
        a_record.is_live(&b_id) && b_record.is_live(&a_id)
    })
    .await?;
    // A's gossip session to B may open after tx-relay's, so wait for a
    // block advert to cross before checking that a transaction advert
    // does not.
    await_gossip(&a.node.supervisor(), b_requests).await?;

    broadcast(&a.node.supervisor(), tx_advert(2)).await?;
    broadcast(
        &a.node.supervisor(),
        LegacyGossipFrame::AdvertiseBlock(block_hash(2)),
    )
    .await?;
    let seen = until_block(b_requests, block_hash(2)).await?;
    assert!(
        !seen.iter().any(is_transaction_work),
        "A's transaction advert does not reach B: {seen:?}"
    );

    let b_client = LegacyRequestAdapter::new(b.node.supervisor());
    for request in outbound_transaction_requests() {
        let refused = b_client.clone().oneshot(request.clone()).await;
        assert!(
            refused.is_err_and(|error| error.to_string().contains("no ready Zakura peer")),
            "B does not choose A for {request:?}"
        );
    }
    let blocks = b_client
        .clone()
        .oneshot(Request::BlocksByHash([block_hash(3)].into_iter().collect()))
        .await?;
    assert_eq!(
        blocks,
        Response::Blocks(vec![InventoryResponse::Missing(block_hash(3))])
    );
    let at_a = all_seen(&mut a_requests);
    assert!(
        !at_a.iter().any(is_transaction_work)
            && at_a
                .iter()
                .any(|request| matches!(request, Request::BlocksByHash(_))),
        "only B's block request reaches A: {at_a:?}"
    );
    assert!(
        b.node
            .supervisor()
            .outbound_peer_handles()
            .await
            .iter()
            .any(|handle| *handle.peer_id() == a_id),
        "A stays connected"
    );

    // A comes back on a build without tx-relay.
    a.node.shutdown().await;
    await_until("B's session for A ends", TEST_NET_TIMEOUT, || {
        !b_record.is_live(&a_id)
    })
    .await?;
    let (a_node, mut a_requests) = Node::new(vec![held.clone()]);
    let a = RelayNode::spawn_loaded(91, None, a_node, None, |builder| builder).await?;
    a.node.connect_native(&b.node, TEST_NET_TIMEOUT).await?;
    await_gossip(&a.node.supervisor(), b_requests).await?;
    broadcast(&a.node.supervisor(), tx_advert(5)).await?;
    let carried = loop {
        // Block adverts from the wait may still be arriving.
        match next_seen(b_requests).await? {
            Request::AdvertiseBlock(..) => continue,
            request => break request,
        }
    };
    assert!(
        matches!(&carried, Request::AdvertiseTransactionIds(ids, _) if ids.contains(&transaction(5).id())),
        "B's adapter carries A's transaction advert again, got {carried:?}"
    );
    let fetched = b_client
        .oneshot(Request::TransactionsById(HashSet::from([held.id()])))
        .await?;
    assert_eq!(
        fetched,
        Response::Transactions(vec![InventoryResponse::Available((held, None))])
    );
    let at_a = all_seen(&mut a_requests);
    assert!(
        at_a.iter()
            .any(|request| matches!(request, Request::TransactionsById(_))),
        "B's request reaches A again: {at_a:?}"
    );
    a.node.shutdown().await;
    b.node.shutdown().await;
    Ok(())
}

/// Broadcasts fresh block adverts from `from` until `to` sees one, since the
/// gossip session can open after the connection does.
async fn await_gossip(
    from: &ZakuraSupervisorHandle,
    to: &mut mpsc::UnboundedReceiver<Request>,
) -> Result<(), BoxError> {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    tokio::time::timeout(TEST_NET_TIMEOUT, async {
        loop {
            let mut bytes = [0xb0; 32];
            bytes[..8].copy_from_slice(&NEXT.fetch_add(1, Ordering::Relaxed).to_le_bytes());
            let hash = block::Hash(bytes);
            // With no session open yet the broadcast fails, and a later one
            // goes once it opens.
            let _ = broadcast(from, LegacyGossipFrame::AdvertiseBlock(hash)).await;
            let seen = tokio::time::timeout(Duration::from_millis(200), until_block(to, hash));
            if seen.await.is_ok() {
                return Ok::<(), BoxError>(());
            }
        }
    })
    .await?
}

/// Every request `to` is handed up to and including block advert `hash`.
async fn until_block(
    to: &mut mpsc::UnboundedReceiver<Request>,
    hash: block::Hash,
) -> Result<Vec<Request>, BoxError> {
    let mut seen = Vec::new();
    loop {
        let request = next_seen(to).await?;
        let done = matches!(&request, Request::AdvertiseBlock(advert, _) if advert.hash == hash);
        seen.push(request);
        if done {
            return Ok(seen);
        }
    }
}
