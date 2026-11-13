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
    chain_tip::mock::{MockChainTip, MockChainTipSender},
    parameters::Network,
    serialization::ZcashDeserializeInto,
    transaction::{self, UnminedTx, UnminedTxId},
};

use crate::{
    peer::{ConnectedClient, LoadTrackedClient, MinimumPeerVersion},
    peer_set::{inventory_registry::InventoryStatus, InventoryChange, PeerSet},
    protocol::external::{types::PeerServices, InventoryHash, Message},
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
        Self::spawn_with_services(port, PeerServices::NODE_NETWORK, None)
    }

    fn spawn_with_services(
        port: u16,
        services: PeerServices,
        inventory: Option<tokio::sync::broadcast::Sender<InventoryChange>>,
    ) -> Self {
        let addr: PeerSocketAddr = format!("127.0.0.1:{port}")
            .parse()
            .expect("test peer address is valid");
        let ConnectedClient {
            mut client,
            peer_inbound,
            peer_outbound,
            inbound_service,
        } = ConnectedClient::spawn_with_services(addr, services);
        if let Some(inventory) = inventory {
            client.inv_collector = inventory;
        }

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

/// Tells the peer set that this node has reached the network tip.
///
/// A mock chain tip starts with no distance estimate, which the peer set reads as still syncing.
fn reach_network_tip(tip: &MockChainTipSender) {
    tip.send_best_tip_height(block::Height(1));
    tip.send_estimated_distance_to_network_chain_tip(Some(0));
}

/// An archive and a pruned peer connected through the real request/response machinery.
///
/// The node is still syncing, as its chain tip has no distance estimate.
struct StoragePeers {
    set: PeerSet<TestDiscover, MockChainTip>,
    _guard: PeerSetGuard,
    archive: TestPeer,
    pruned: TestPeer,
    inventory: tokio::sync::broadcast::Sender<InventoryChange>,
}

impl StoragePeers {
    async fn new() -> Self {
        let (inventory, receiver) = tokio::sync::broadcast::channel(16);
        let mut archive =
            TestPeer::spawn_with_services(1, PeerServices::NODE_NETWORK, Some(inventory.clone()));
        let mut pruned = TestPeer::spawn_with_services(
            2,
            PeerServices::NODE_NETWORK | PeerServices::NODE_PRUNED,
            Some(inventory.clone()),
        );
        let (minimum, _tip) = MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);
        let mut builder = PeerSetBuilder::new();
        builder.inv_stream = Some(receiver);
        let (mut set, guard) = builder
            .with_discover(discover(vec![archive.discovered(), pruned.discovered()]))
            .with_minimum_peer_version(minimum)
            .max_conns_per_ip(2)
            .build();
        set.ready().await.expect("both peers are ready");
        assert_eq!(set.ready_services.len(), 2);
        Self {
            set,
            _guard: guard,
            archive,
            pruned,
            inventory,
        }
    }

    async fn next_message(peer: &mut TestPeer) -> Message {
        tokio::time::timeout(Duration::from_secs(1), peer.peer_outbound.next())
            .await
            .expect("the selected peer receives the request promptly")
            .expect("the connection stays open")
    }

    fn block_request() -> Request {
        Request::BlocksByHash(iter::once(BLOCK_HASH).collect())
    }
}

#[tokio::test(start_paused = true)]
async fn pruned_routing_prefers_archive_without_an_advertiser() {
    let _init_guard = zakura_test::init();
    let mut peers = StoragePeers::new().await;
    let _response = peers.set.call(StoragePeers::block_request());
    assert_eq!(
        StoragePeers::next_message(&mut peers.archive).await,
        Message::GetData(vec![BLOCK_HASH.into()])
    );
    assert_eq!(peers.pruned.written_now().await, None);
}

#[tokio::test(start_paused = true)]
async fn pruned_routing_falls_back_without_an_archive() {
    let _init_guard = zakura_test::init();
    let mut peers = StoragePeers::new().await;
    peers
        .set
        .call(Request::DisconnectPeer(peers.archive.addr))
        .await
        .unwrap();
    peers.set.ready().await.unwrap();
    let _response = peers.set.call(StoragePeers::block_request());
    assert_eq!(
        StoragePeers::next_message(&mut peers.pruned).await,
        Message::GetData(vec![BLOCK_HASH.into()])
    );
}

#[tokio::test(start_paused = true)]
async fn pruned_routing_batch_prefers_archive_and_falls_back() {
    let _init_guard = zakura_test::init();
    for archive_present in [true, false] {
        let mut peers = StoragePeers::new().await;
        if !archive_present {
            peers
                .set
                .call(Request::DisconnectPeer(peers.archive.addr))
                .await
                .unwrap();
            peers.set.ready().await.unwrap();
        }
        let hashes = [BLOCK_HASH, block::Hash([0x88; 32])];
        let _response = peers
            .set
            .call(Request::BlocksByHash(hashes.into_iter().collect()));
        let selected = if archive_present {
            &mut peers.archive
        } else {
            &mut peers.pruned
        };
        let Message::GetData(inventory) = StoragePeers::next_message(selected).await else {
            panic!("a batch block request writes getdata");
        };
        assert_eq!(
            inventory.into_iter().collect::<HashSet<_>>(),
            hashes.into_iter().map(Into::into).collect()
        );
    }
}

#[tokio::test(start_paused = true)]
async fn pruned_routing_preserves_advertiser_and_known_source_priority() {
    let _init_guard = zakura_test::init();
    for advertised in [false, true] {
        let mut peers = StoragePeers::new().await;
        if advertised {
            peers
                .inventory
                .send(InventoryStatus::new_available(
                    BLOCK_HASH.into(),
                    peers.pruned.addr,
                ))
                .unwrap();
        }
        let request = if advertised {
            StoragePeers::block_request()
        } else {
            Request::BlocksByHashFrom {
                hashes: iter::once(BLOCK_HASH).collect(),
                source: crate::PeerSource::LegacySocket(peers.pruned.addr),
            }
        };
        let _response = peers.set.ready().await.unwrap().call(request);
        assert_eq!(
            StoragePeers::next_message(&mut peers.pruned).await,
            Message::GetData(vec![BLOCK_HASH.into()])
        );
        assert_eq!(peers.archive.written_now().await, None);
    }
}

#[tokio::test(start_paused = true)]
async fn pruned_routing_notfound_excludes_archive_but_keeps_connection() {
    let _init_guard = zakura_test::init();
    let mut peers = StoragePeers::new().await;
    let response = peers.set.call(StoragePeers::block_request());
    StoragePeers::next_message(&mut peers.archive).await;
    peers
        .archive
        .peer_inbound
        .send(Ok(Message::NotFound(vec![BLOCK_HASH.into()])))
        .await
        .unwrap();
    assert!(response.await.is_err());
    let _retry = peers
        .set
        .ready()
        .await
        .unwrap()
        .call(StoragePeers::block_request());
    assert_eq!(
        StoragePeers::next_message(&mut peers.pruned).await,
        Message::GetData(vec![BLOCK_HASH.into()])
    );
    assert!(
        peers.set.ready_services.contains_key(&peers.archive.addr),
        "notfound does not disconnect the archive"
    );
    assert_eq!(peers.archive.written_now().await, None);
}

/// `getblocks` returns block hashes, which pruned peers keep, so it has no storage preference: a
/// syncing node sends it to pruned peers too.
#[tokio::test(start_paused = true)]
async fn pruned_routing_findblocks_reaches_pruned_peers_while_syncing() {
    let _init_guard = zakura_test::init();
    const REQUESTS: usize = 32;

    let mut peers = StoragePeers::new().await;
    let request = Request::FindBlocks {
        known_blocks: vec![BLOCK_HASH],
        stop: None,
    };
    // An `inv` with several blocks answers `getblocks` without advertising any of them.
    let found = vec![BLOCK_HASH.into(), block::Hash([0x88; 32]).into()];

    // Both peers have the same load, so each request picks one of them at random.
    let mut pruned_requests = 0;
    for _ in 0..REQUESTS {
        let response = peers.set.ready().await.unwrap().call(request.clone());
        let selected = if peers.pruned.written_now().await.is_some() {
            pruned_requests += 1;
            &mut peers.pruned
        } else {
            assert!(matches!(
                StoragePeers::next_message(&mut peers.archive).await,
                Message::GetBlocks { .. }
            ));
            &mut peers.archive
        };
        selected
            .peer_inbound
            .send(Ok(Message::Inv(found.clone())))
            .await
            .unwrap();
        response.await.unwrap();
    }

    assert!(
        pruned_requests > 0,
        "the pruned peer received none of {REQUESTS} getblocks requests"
    );
}

#[tokio::test(start_paused = true)]
async fn pruned_routing_pruned_notfound_retries_archive_without_disconnect() {
    let _init_guard = zakura_test::init();
    let mut peers = StoragePeers::new().await;
    peers
        .inventory
        .send(InventoryStatus::new_available(
            BLOCK_HASH.into(),
            peers.pruned.addr,
        ))
        .unwrap();
    let response = peers
        .set
        .ready()
        .await
        .unwrap()
        .call(StoragePeers::block_request());
    StoragePeers::next_message(&mut peers.pruned).await;
    peers
        .pruned
        .peer_inbound
        .send(Ok(Message::NotFound(vec![BLOCK_HASH.into()])))
        .await
        .unwrap();
    assert!(response.await.is_err());
    let _retry = peers
        .set
        .ready()
        .await
        .unwrap()
        .call(StoragePeers::block_request());
    assert_eq!(
        StoragePeers::next_message(&mut peers.archive).await,
        Message::GetData(vec![BLOCK_HASH.into()])
    );
    assert!(peers.set.ready_services.contains_key(&peers.pruned.addr));
    assert_eq!(peers.pruned.written_now().await, None);
}

/// One archive peer among several pruned peers, all ready and equally loaded.
struct ArchiveAmongPruned {
    set: PeerSet<TestDiscover, MockChainTip>,
    _guard: PeerSetGuard,
    tip: MockChainTipSender,
    archive: PeerSocketAddr,
    candidates: HashSet<PeerSocketAddr>,
    /// The peers' ends of the connections, which keep them open.
    _peers: Vec<TestPeer>,
}

impl ArchiveAmongPruned {
    const PRUNED_PEERS: u16 = 9;

    async fn new() -> Self {
        let mut peers = vec![TestPeer::spawn_with_services(
            1,
            PeerServices::NODE_NETWORK,
            None,
        )];
        peers.extend((0..Self::PRUNED_PEERS).map(|i| {
            TestPeer::spawn_with_services(
                2 + i,
                PeerServices::NODE_NETWORK | PeerServices::NODE_PRUNED,
                None,
            )
        }));
        let archive = peers[0].addr;
        let (minimum, tip) = MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);
        let (mut set, guard) = PeerSetBuilder::new()
            .with_discover(discover(
                peers.iter_mut().map(TestPeer::discovered).collect(),
            ))
            .with_minimum_peer_version(minimum)
            .max_conns_per_ip(peers.len())
            .build();
        set.ready().await.expect("all peers are ready");
        let candidates: HashSet<_> = set.ready_services.keys().copied().collect();
        assert_eq!(candidates.len(), peers.len());

        Self {
            set,
            _guard: guard,
            tip,
            archive,
            candidates,
            _peers: peers,
        }
    }

    /// Returns how many of `selections` block body selections pick the archive peer.
    fn archive_selections(&self, selections: usize) -> usize {
        (0..selections)
            .filter(|_| self.set.select_block_body_peer(&self.candidates) == Some(self.archive))
            .count()
    }
}

/// A syncing node asks for blocks that pruned peers have deleted, so while a peer without the
/// pruned hint is ready, no block request goes to a pruned peer.
#[tokio::test(start_paused = true)]
async fn pruned_routing_skips_pruned_peers_while_syncing() {
    let _init_guard = zakura_test::init();
    const SELECTIONS: usize = 200;

    let peers = ArchiveAmongPruned::new().await;

    assert_eq!(
        peers.archive_selections(SELECTIONS),
        SELECTIONS,
        "a pruned peer was selected while an archive peer was ready",
    );
}

/// Near the network tip pruned peers hold every block this node asks for, so they share block
/// requests with the archive peer like any other peer.
#[tokio::test(start_paused = true)]
async fn pruned_routing_treats_pruned_peers_alike_near_the_tip() {
    let _init_guard = zakura_test::init();
    const SELECTIONS: usize = 4000;

    let peers = ArchiveAmongPruned::new().await;
    reach_network_tip(&peers.tip);

    // The archive is one of 10 equally loaded peers, so expect about 400 of 4000 selections.
    // If it also won whenever it was one of the two sampled peers, it would take about 800.
    // The bound is many standard deviations away from both.
    let archive_selections = peers.archive_selections(SELECTIONS);
    assert!(
        archive_selections < SELECTIONS * 3 / 20,
        "the archive took {archive_selections} of {SELECTIONS} selections near the tip"
    );
}

/// While syncing, an archive peer that is busy with another response still gets the block request,
/// although an idle pruned peer could take it at once: the pruned peer would answer `notfound`.
#[tokio::test(start_paused = true)]
async fn pruned_routing_keeps_busy_archive_over_idle_pruned_while_syncing() {
    let _init_guard = zakura_test::init();

    let BusyAndIdleAdvertisers {
        mut peer_set,
        _guard,
        busy: mut archive,
        idle: mut pruned,
        tx,
        tx_response,
    } = busy_and_idle_peers(
        PeerServices::NODE_NETWORK,
        PeerServices::NODE_NETWORK | PeerServices::NODE_PRUNED,
        None,
    )
    .await;

    let _block_response = peer_set
        .ready()
        .await
        .expect("peer set is ready")
        .call(StoragePeers::block_request());

    assert_eq!(
        pruned.written_now().await,
        None,
        "the idle pruned peer is not asked for the block",
    );

    archive
        .peer_inbound
        .send(Ok(Message::Tx(tx)))
        .await
        .expect("connection is open");
    assert!(matches!(tx_response.await, Ok(Response::Transactions(_))));
    assert_eq!(
        archive.written_now().await,
        Some(Message::GetData(vec![BLOCK_HASH.into()])),
        "the archive peer gets the block request once its earlier response arrives",
    );
}

/// A peer that advertises no services, such as an older Zakura pruned node, lacks the pruned hint,
/// so a syncing node treats it as an archive candidate and asks it instead of a peer with the hint.
#[tokio::test(start_paused = true)]
async fn pruned_routing_treats_peers_without_services_as_archive() {
    let _init_guard = zakura_test::init();
    const SELECTIONS: usize = 200;

    let mut no_services = TestPeer::spawn_with_services(1, PeerServices::empty(), None);
    let mut pruned = TestPeer::spawn_with_services(
        2,
        PeerServices::NODE_NETWORK | PeerServices::NODE_PRUNED,
        None,
    );
    let (minimum, _tip) = MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);
    let (mut set, _guard) = PeerSetBuilder::new()
        .with_discover(discover(vec![
            no_services.discovered(),
            pruned.discovered(),
        ]))
        .with_minimum_peer_version(minimum)
        .max_conns_per_ip(2)
        .build();
    set.ready().await.expect("both peers are ready");
    let candidates: HashSet<_> = set.ready_services.keys().copied().collect();
    assert_eq!(candidates.len(), 2);

    let no_services_selections = (0..SELECTIONS)
        .filter(|_| set.select_block_body_peer(&candidates) == Some(no_services.addr))
        .count();

    // While syncing, the peer with the pruned hint is left out of the candidates.
    assert_eq!(
        no_services_selections, SELECTIONS,
        "the peer without services must win every selection against a pruned peer",
    );
}

#[tokio::test(start_paused = true)]
async fn pruned_routing_transactions_still_use_pruned_advertisers() {
    let _init_guard = zakura_test::init();
    let mut peers = StoragePeers::new().await;
    let txid = test_transaction().id();
    peers
        .inventory
        .send(InventoryStatus::new_available(
            txid.into(),
            peers.pruned.addr,
        ))
        .unwrap();
    let _response = peers
        .set
        .ready()
        .await
        .unwrap()
        .call(Request::TransactionsById(iter::once(txid).collect()));
    assert_eq!(
        StoragePeers::next_message(&mut peers.pruned).await,
        Message::GetData(vec![txid.into()])
    );
    assert_eq!(peers.archive.written_now().await, None);
}

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
            |request| matches!(request, Request::AdvertiseBlock(advert, _) if advert.hash == BLOCK_HASH),
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

/// Two connected peers: one awaiting a transaction response, one idle.
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
    busy_and_idle_peers(
        PeerServices::NODE_NETWORK,
        PeerServices::NODE_NETWORK,
        Some(inventory),
    )
    .await
}

/// Connects a busy and an idle peer with the given services, as
/// [`busy_and_idle_advertisers`] does, with both peers advertising `inventory` if it is given.
async fn busy_and_idle_peers(
    busy_services: PeerServices,
    idle_services: PeerServices,
    inventory: Option<InventoryHash>,
) -> BusyAndIdleAdvertisers {
    let mut busy = TestPeer::spawn_with_services(1, busy_services, None);
    let mut idle = TestPeer::spawn_with_services(2, idle_services, None);
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

    peer_set.ready().await.expect("peer set is ready");
    if let Some(inventory) = inventory {
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
    }
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

/// A client marks an idle connection's slot busy when it sends a request.
///
/// Once the connection takes the request, the client is ready again, and the peer set can read the
/// slot before the connection task runs its next instruction.
#[tokio::test(start_paused = true)]
async fn sending_a_request_marks_an_idle_connection_busy() {
    let _init_guard = zakura_test::init();

    let mut peer = TestPeer::spawn(1);
    assert_eq!(peer.written_now().await, None);

    let client = peer.client.as_mut().expect("peer is not in a peer set");
    assert!(client.can_send_request_now());

    let _response = client
        .ready()
        .await
        .expect("test connection is ready")
        .call(Request::BlocksByHash(iter::once(BLOCK_HASH).collect()));

    // This test task has not yielded, so the connection task has not run.
    assert!(!client.can_send_request_now());
}

/// A peer that left its last block request unanswered does not win over a busy advertiser.
///
/// The peer's connection is free again after the request times out. Load balancing still applies,
/// and the timeout gave the peer a high load, so the block request goes to the busy advertiser.
#[tokio::test(start_paused = true)]
async fn withholding_advertiser_is_not_preferred_over_busy_advertiser() {
    let _init_guard = zakura_test::init();

    let mut busy = TestPeer::spawn(1);
    let mut withholder = TestPeer::spawn(2);
    let tx = test_transaction();

    // Let the default RTT estimates decay, so the timeout decides the load difference.
    tokio::time::advance(Duration::from_secs(1000)).await;

    let (withheld_response, withheld_getdata) = withholder
        .send_direct(Request::BlocksByHash(iter::once(BLOCK_HASH).collect()))
        .await;
    assert_eq!(withheld_getdata, Message::GetData(vec![BLOCK_HASH.into()]));
    tokio::time::advance(crate::constants::REQUEST_TIMEOUT).await;
    assert!(withheld_response.await.is_err());

    let (tx_response, _tx_getdata) = busy
        .send_direct(Request::TransactionsById(iter::once(tx.id()).collect()))
        .await;

    let inventory = InventoryHash::Block(BLOCK_HASH);
    let (minimum_peer_version, _tip) = MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);
    let (mut peer_set, mut guard) = PeerSetBuilder::new()
        .with_discover(discover(vec![busy.discovered(), withholder.discovered()]))
        .with_minimum_peer_version(minimum_peer_version)
        .max_conns_per_ip(2)
        .build();
    for addr in [busy.addr, withholder.addr] {
        guard
            .inventory_sender()
            .as_mut()
            .expect("guard has an inventory sender")
            .send(InventoryStatus::new_available(inventory, addr))
            .expect("peer set receives inventory");
        peer_set.ready().await.expect("peer set is ready");
    }
    assert!(peer_set.ready_services[&withholder.addr].can_send_request_now());
    assert!(!peer_set.ready_services[&busy.addr].can_send_request_now());

    let _block_response = peer_set
        .ready()
        .await
        .expect("peer set is ready")
        .call(Request::BlocksByHash(iter::once(BLOCK_HASH).collect()));

    assert_eq!(
        withholder.written_now().await,
        None,
        "the withholding advertiser does not receive the retry",
    );

    busy.peer_inbound
        .send(Ok(Message::Tx(tx.clone())))
        .await
        .expect("connection is open");
    assert!(matches!(tx_response.await, Ok(Response::Transactions(_))));
    assert_eq!(
        busy.written_now().await,
        Some(Message::GetData(vec![BLOCK_HASH.into()])),
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
