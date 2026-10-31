//! Block `getdata` routing around connections that are awaiting a transaction response.
//!
//! A legacy connection handles one outbound request at a time. While it awaits a response, it
//! reads peer messages but does not take the next request from its client channel. These tests use
//! real connections, so they observe when a routed block request is actually written to a peer.

use std::{collections::HashSet, iter, sync::Arc, time::Duration};

use futures::{
    channel::mpsc,
    future::BoxFuture,
    stream::{self, BoxStream},
    FutureExt, SinkExt, StreamExt,
};
use tower::{discover::Change, Service, ServiceExt};

use zakura_chain::{
    block::{self, Block},
    chain_tip::mock::MockChainTip,
    parameters::Network,
    serialization::ZcashDeserializeInto,
    transaction::{self, UnminedTx, UnminedTxId},
};

use crate::{
    peer::{ConnectedClient, LoadTrackedClient, MinimumPeerVersion},
    peer_set::{inventory_registry::InventoryStatus, PeerSet},
    protocol::external::{InventoryHash, Message},
    BoxError, PeerSocketAddr, Request, Response,
};

use zakura_test::mock_service::{MockService, PanicAssertion};

use super::{PeerSetBuilder, PeerSetGuard};

/// The block that peers advertise in these tests.
const BLOCK_HASH: block::Hash = block::Hash([0x77; 32]);

/// A peer connection under test: the peer set's service plus the remote peer's channels.
struct TestPeer {
    addr: PeerSocketAddr,
    /// The peer's service, until it is handed to the peer set.
    client: Option<LoadTrackedClient>,
    peer_inbound: mpsc::Sender<Result<Message, zakura_chain::serialization::SerializationError>>,
    peer_outbound: mpsc::Receiver<Message>,
    inbound_service: MockService<Request, Response, PanicAssertion>,
}

impl TestPeer {
    fn spawn(port: u16) -> Self {
        let addr: PeerSocketAddr = format!("127.0.0.1:{port}")
            .parse()
            .expect("test peer address is valid");
        let ConnectedClient {
            client,
            peer_inbound,
            peer_outbound,
            inbound_service,
        } = ConnectedClient::spawn(addr);

        TestPeer {
            addr,
            client: Some(client.into()),
            peer_inbound,
            peer_outbound,
            inbound_service,
        }
    }

    /// Hands this peer's service to the peer set.
    fn discovered(&mut self) -> (PeerSocketAddr, LoadTrackedClient) {
        let client = self.client.take().expect("peer is discovered once");
        (self.addr, client)
    }

    /// Returns the next message the connection has already written to the peer, if any.
    async fn written_now(&mut self) -> Option<Message> {
        // Let the connection task run without advancing the paused clock.
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        self.peer_outbound.try_recv().ok()
    }

    /// Sends a request through this peer's own load-tracked client, outside the peer set, and
    /// waits until the connection writes it.
    async fn send_direct(
        &mut self,
        request: Request,
    ) -> (
        impl std::future::Future<Output = Result<Response, crate::SharedPeerError>>,
        Message,
    ) {
        let response = self
            .client
            .as_mut()
            .expect("peer is not yet in the peer set")
            .ready()
            .await
            .expect("test connection is ready")
            .call(request);
        let written = self
            .peer_outbound
            .next()
            .await
            .expect("connection writes the request");
        (response, written)
    }
}

/// Returns a transaction to use as the delayed response.
fn test_transaction() -> UnminedTx {
    let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .expect("test vector is a valid block");
    block.transactions[0].clone().into()
}

/// Puts `peers` in a discovery stream for the peer set.
fn discover(peers: Vec<(PeerSocketAddr, LoadTrackedClient)>) -> TestDiscover {
    stream::iter(
        peers
            .into_iter()
            .map(|(addr, client)| Ok(Change::Insert(addr, client))),
    )
    .chain(stream::pending())
    .boxed()
}

/// The peer discovery stream used by these tests.
type TestDiscover = BoxStream<'static, Result<Change<PeerSocketAddr, LoadTrackedClient>, BoxError>>;

/// One source: the only advertiser of a block is awaiting a transaction response.
///
/// The peer set treats that client as ready, because its request channel has capacity. The block
/// request is routed to it, but the connection cannot write the `getdata` until the transaction
/// response arrives. Routing cannot avoid this wait, because no other peer advertised the block,
/// so the peer set falls back to the busy advertiser.
#[tokio::test(start_paused = true)]
async fn block_getdata_waits_behind_tx_response_on_only_advertiser() {
    let _init_guard = zakura_test::init();

    let mut source = TestPeer::spawn(1);
    let tx = test_transaction();

    let (tx_response, tx_getdata) = source
        .send_direct(Request::TransactionsById(iter::once(tx.id()).collect()))
        .await;
    assert_eq!(tx_getdata, Message::GetData(vec![tx.id().into()]));

    // The source advertises a block while its transaction response is outstanding.
    // The connection still reads peer messages, so the advert reaches the inbound service.
    source
        .peer_inbound
        .send(Ok(Message::Inv(vec![BLOCK_HASH.into()])))
        .await
        .expect("connection is open");
    source
        .inbound_service
        .expect_request_that(
            |request| matches!(request, Request::AdvertiseBlock(hash, _) if *hash == BLOCK_HASH),
        )
        .await
        .respond(Response::Nil);

    let (minimum_peer_version, _tip) = MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);
    let (mut peer_set, mut guard) = PeerSetBuilder::new()
        .with_discover(discover(vec![source.discovered()]))
        .with_minimum_peer_version(minimum_peer_version)
        .build();
    guard
        .inventory_sender()
        .as_mut()
        .expect("guard has an inventory sender")
        .send(InventoryStatus::new_available(
            InventoryHash::Block(BLOCK_HASH),
            source.addr,
        ))
        .expect("peer set receives inventory");

    let peer_set = peer_set.ready().await.expect("peer set is ready");
    assert!(
        peer_set.ready_services.contains_key(&source.addr),
        "a client awaiting a response still reports ready",
    );
    assert!(
        !peer_set.ready_services[&source.addr].can_send_request_now(),
        "but its connection cannot write a request now",
    );

    let _block_response = peer_set.call(Request::BlocksByHash(iter::once(BLOCK_HASH).collect()));

    tokio::time::advance(Duration::from_millis(300)).await;
    assert_eq!(
        source.written_now().await,
        None,
        "block getdata is not written while the transaction response is outstanding",
    );

    source
        .peer_inbound
        .send(Ok(Message::Tx(tx.clone())))
        .await
        .expect("connection is open");
    let tx_response = tx_response.await.expect("transaction response arrives");
    assert!(matches!(tx_response, Response::Transactions(_)));

    assert_eq!(
        source.written_now().await,
        Some(Message::GetData(vec![BLOCK_HASH.into()])),
        "block getdata is written as soon as the transaction response arrives",
    );
}

/// Peers that both advertised `inventory`: one awaiting a transaction response, one idle.
struct BusyAndIdleAdvertisers {
    peer_set: PeerSet<TestDiscover, MockChainTip>,
    _guard: PeerSetGuard,
    busy: TestPeer,
    idle: TestPeer,
    /// The transaction the busy peer has not answered yet.
    tx: UnminedTx,
    tx_response: BoxFuture<'static, Result<Response, crate::SharedPeerError>>,
}

/// Connects a busy and an idle advertiser of `inventory`, and adds them to a peer set.
///
/// Both peers have been connected for a while. The idle peer's last response took one second, so
/// its peak-EWMA load is higher than the busy peer's load, even though the busy peer has one request
/// outstanding. Load alone therefore selects the busy peer.
async fn busy_and_idle_advertisers(inventory: InventoryHash) -> BusyAndIdleAdvertisers {
    let mut busy = TestPeer::spawn(1);
    let mut idle = TestPeer::spawn(2);
    let tx = test_transaction();

    // Let the default RTT estimates decay, then give the idle peer a slow round trip.
    tokio::time::advance(Duration::from_secs(1000)).await;
    let nonce = Default::default();
    let (pong, ping) = idle.send_direct(Request::Ping(nonce)).await;
    assert_eq!(ping, Message::Ping(nonce));
    tokio::time::advance(Duration::from_secs(1)).await;
    idle.peer_inbound
        .send(Ok(Message::Pong(nonce)))
        .await
        .expect("connection is open");
    assert!(matches!(pong.await, Ok(Response::Pong(_))));

    let (tx_response, tx_getdata) = busy
        .send_direct(Request::TransactionsById(iter::once(tx.id()).collect()))
        .await;
    assert_eq!(tx_getdata, Message::GetData(vec![tx.id().into()]));

    let (minimum_peer_version, _tip) = MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);
    let (mut peer_set, mut guard) = PeerSetBuilder::new()
        .with_discover(discover(vec![busy.discovered(), idle.discovered()]))
        .with_minimum_peer_version(minimum_peer_version)
        .max_conns_per_ip(2)
        .build();

    // The test inventory channel holds one change, so register each advert before the next.
    for addr in [busy.addr, idle.addr] {
        guard
            .inventory_sender()
            .as_mut()
            .expect("guard has an inventory sender")
            .send(InventoryStatus::new_available(inventory, addr))
            .expect("peer set receives inventory");
        peer_set.ready().await.expect("peer set is ready");
    }

    assert_eq!(
        peer_set
            .inventory_registry
            .advertising_peers(inventory)
            .copied()
            .collect::<HashSet<_>>(),
        HashSet::from([busy.addr, idle.addr]),
    );
    assert_eq!(
        peer_set
            .ready_services
            .keys()
            .copied()
            .collect::<HashSet<_>>(),
        HashSet::from([busy.addr, idle.addr]),
        "a client awaiting a response still reports ready",
    );
    assert!(
        peer_set.query_load(&busy.addr) < peer_set.query_load(&idle.addr),
        "load alone prefers the busy peer",
    );
    assert!(!peer_set.ready_services[&busy.addr].can_send_request_now());
    assert!(peer_set.ready_services[&idle.addr].can_send_request_now());

    BusyAndIdleAdvertisers {
        peer_set,
        _guard: guard,
        busy,
        idle,
        tx,
        tx_response: tx_response.boxed(),
    }
}

/// Alternate source: one advertiser is awaiting a transaction response, and another is idle.
///
/// The block `getdata` leaves immediately through the idle advertiser, and the busy peer's
/// transaction response still completes.
#[tokio::test(start_paused = true)]
async fn block_getdata_routes_to_idle_alternate_advertiser() {
    let _init_guard = zakura_test::init();

    let BusyAndIdleAdvertisers {
        mut peer_set,
        _guard,
        mut busy,
        mut idle,
        tx,
        tx_response,
    } = busy_and_idle_advertisers(InventoryHash::Block(BLOCK_HASH)).await;

    let _block_response = peer_set
        .ready()
        .await
        .expect("peer set is ready")
        .call(Request::BlocksByHash(iter::once(BLOCK_HASH).collect()));

    assert_eq!(
        idle.written_now().await,
        Some(Message::GetData(vec![BLOCK_HASH.into()])),
        "block getdata leaves through the idle advertiser without waiting",
    );

    busy.peer_inbound
        .send(Ok(Message::Tx(tx.clone())))
        .await
        .expect("connection is open");
    assert!(matches!(tx_response.await, Ok(Response::Transactions(_))));
    assert_eq!(
        busy.written_now().await,
        None,
        "the busy advertiser never receives the block request",
    );
}

/// Transaction requests keep today's load-based routing, so they still use a busy advertiser.
///
/// Only block requests avoid connections that are awaiting a response, so transaction downloads
/// are not pushed onto a smaller set of peers.
#[tokio::test(start_paused = true)]
async fn tx_getdata_routing_ignores_request_slot() {
    let _init_guard = zakura_test::init();

    let other_txid = UnminedTxId::Legacy(transaction::Hash([0x55; 32]));
    let BusyAndIdleAdvertisers {
        mut peer_set,
        _guard,
        mut busy,
        mut idle,
        tx,
        tx_response,
    } = busy_and_idle_advertisers(other_txid.into()).await;

    let _other_tx_response = peer_set
        .ready()
        .await
        .expect("peer set is ready")
        .call(Request::TransactionsById(iter::once(other_txid).collect()));

    assert_eq!(idle.written_now().await, None);

    busy.peer_inbound
        .send(Ok(Message::Tx(tx.clone())))
        .await
        .expect("connection is open");
    assert!(matches!(tx_response.await, Ok(Response::Transactions(_))));
    assert_eq!(
        busy.written_now().await,
        Some(Message::GetData(vec![other_txid.into()])),
        "the transaction request is written after the earlier response",
    );
}
