//! Tests for compact block relay negotiation, high-bandwidth source slots, `cmpctblock`
//! announcements, `getblocktxn` serving, and receiving and rebuilding `cmpctblock`.

use std::{sync::Arc, time::Duration};

use futures::{channel::mpsc, sink::SinkMapErr, SinkExt, StreamExt};
use indexmap::IndexSet;
use tokio::sync::{oneshot, watch, Semaphore};

use zakura_chain::{
    block::{self, Block, MAX_BLOCK_BYTES},
    serialization::{SerializationError, ZcashDeserializeInto, ZcashSerialize},
    transaction::UnminedTx,
};
use zakura_test::{
    mock_service::{MockService, PanicAssertion},
    vectors::{BLOCK_MAINNET_1687107_BYTES, BLOCK_MAINNET_1_BYTES},
};

use super::super::compact_relay::{RECOVERY_TIMEOUT, RETENTION};
use crate::{
    constants::REQUEST_TIMEOUT,
    peer::{
        block_gossip::{
            compact_slots, BlockAdvert, BlockAdvertError, BlockGossipPeer, BlockUploads,
            BLOCK_ADVERT_QUEUE, BLOCK_REQUEST_WAIT, COMPACT_ANNOUNCEMENTS_PER_BLOCK,
        },
        connection::Connection,
        ClientRequest, ConnectedAddr, ErrorSlot,
    },
    protocol::external::{
        compact::{BlockTransactions, BlockTransactionsRequest, CompactBlock},
        InventoryHash, Message,
    },
    InventoryResponse, PeerSocketAddr, PeerSource, Request, Response, SharedPeerError,
};

/// A test connection with a mock inbound service and a channel peer writer.
type TestConnection = Connection<
    MockService<Request, Response, PanicAssertion>,
    SinkMapErr<mpsc::Sender<Message>, fn(mpsc::SendError) -> SerializationError>,
>;

/// A running test connection, and the channels that drive and observe it.
struct RunningConnection {
    /// The connection's peer message sender. The connection fails if it is dropped.
    _peer_tx: mpsc::Sender<Result<Message, SerializationError>>,
    /// The connection's client request sender. The connection shuts down if it is dropped.
    _client_tx: mpsc::Sender<ClientRequest>,
    /// The messages the connection writes to its peer.
    outbound: mpsc::Receiver<Message>,
    /// The peer set's high-bandwidth source selection for this connection.
    slots_tx: watch::Sender<Option<Arc<Semaphore>>>,
    /// The connection task.
    task: tokio::task::JoinHandle<()>,
}

impl RunningConnection {
    /// Returns the next message the connection writes to its peer.
    async fn next_written(&mut self) -> Message {
        self.outbound
            .next()
            .await
            .expect("the running connection keeps its peer writer open")
    }

    /// Asserts that the connection has written nothing it hasn't already returned.
    async fn assert_nothing_written(&mut self) {
        settle().await;
        assert!(
            self.outbound.try_recv().is_err(),
            "the connection wrote an unexpected message"
        );
    }
}

impl Drop for RunningConnection {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Starts `connection` with `slots` as its initial high-bandwidth source selection.
fn run(
    mut connection: TestConnection,
    client_tx: mpsc::Sender<ClientRequest>,
    outbound: mpsc::Receiver<Message>,
    slots: Option<Arc<Semaphore>>,
) -> RunningConnection {
    let (slots_tx, slots_rx) = watch::channel(slots);
    connection.compact_receive_rx = slots_rx;
    let (peer_tx, peer_rx) = mpsc::channel(4);
    let task = tokio::spawn(connection.run(peer_rx));
    RunningConnection {
        _peer_tx: peer_tx,
        _client_tx: client_tx,
        outbound,
        slots_tx,
        task,
    }
}

/// Starts a connection that negotiated compact blocks, with `slots` as its initial
/// high-bandwidth source selection.
fn run_negotiated(slots: Option<Arc<Semaphore>>) -> RunningConnection {
    let (connection, client_tx, _inbound, outbound, _error_slot) =
        super::new_early_relay_test_connection::<PanicAssertion>();
    run(connection, client_tx, outbound, slots)
}

/// Returns a `sendcmpct` message for version 1 with `announce`.
fn sendcmpct(announce: bool) -> Message {
    Message::SendCompact {
        announce,
        version: 1,
    }
}

/// Lets spawned connection tasks run.
async fn settle() {
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
}

/// A negotiated connection that the peer set hasn't selected asks the peer not to announce
/// blocks with `cmpctblock`.
#[tokio::test]
async fn unselected_connection_sends_low_bandwidth_at_start() {
    let _init_guard = zakura_test::init();

    let mut connection = run_negotiated(None);

    assert_eq!(connection.next_written().await, sendcmpct(false));
    connection.assert_nothing_written().await;
}

/// A negotiated connection that the peer set selected before it started takes a slot and asks
/// the peer to announce blocks with `cmpctblock`.
#[tokio::test]
async fn selected_connection_sends_high_bandwidth_at_start() {
    let _init_guard = zakura_test::init();

    let slots = Arc::new(Semaphore::new(3));
    let mut connection = run_negotiated(Some(slots.clone()));

    assert_eq!(connection.next_written().await, sendcmpct(true));
    assert_eq!(
        slots.available_permits(),
        2,
        "the connection keeps its slot"
    );
    connection.assert_nothing_written().await;
}

/// A selected connection starts in low-bandwidth mode when every slot is taken.
#[tokio::test]
async fn selected_connection_without_free_slot_sends_low_bandwidth_at_start() {
    let _init_guard = zakura_test::init();

    let slots = Arc::new(Semaphore::new(1));
    let held = slots.clone().try_acquire_owned().expect("the slot is free");
    let mut connection = run_negotiated(Some(slots.clone()));

    assert_eq!(connection.next_written().await, sendcmpct(false));

    // The connection takes the slot as soon as it is free.
    drop(held);
    assert_eq!(connection.next_written().await, sendcmpct(true));
    assert_eq!(slots.available_permits(), 0);
}

/// Promotion takes a slot, then writes `sendcmpct(1, 1)`.
#[tokio::test]
async fn promotion_writes_high_bandwidth() {
    let _init_guard = zakura_test::init();

    let slots = Arc::new(Semaphore::new(3));
    let mut connection = run_negotiated(None);
    assert_eq!(connection.next_written().await, sendcmpct(false));

    connection.slots_tx.send_replace(Some(slots.clone()));

    assert_eq!(connection.next_written().await, sendcmpct(true));
    assert_eq!(
        slots.available_permits(),
        2,
        "the connection keeps its slot"
    );
    connection.assert_nothing_written().await;
}

/// Demotion writes `sendcmpct(0, 1)`, and releases the slot only once that write completes. The
/// next selected connection then takes the slot.
#[tokio::test]
async fn demotion_writes_low_bandwidth_then_releases_slot() {
    let _init_guard = zakura_test::init();

    let slots = Arc::new(Semaphore::new(1));
    let mut demoted = run_negotiated(Some(slots.clone()));
    let mut promoted = run_negotiated(None);
    assert_eq!(demoted.next_written().await, sendcmpct(true));
    assert_eq!(promoted.next_written().await, sendcmpct(false));

    // The test peer writer finishes a write only when the test reads it, so the demotion
    // write stays incomplete until `sendcmpct(0, 1)` is read.
    settle().await;
    demoted.slots_tx.send_replace(None);
    promoted.slots_tx.send_replace(Some(slots.clone()));
    settle().await;
    assert_eq!(
        slots.available_permits(),
        0,
        "the demoted connection keeps its slot until its write completes"
    );
    promoted.assert_nothing_written().await;

    assert_eq!(demoted.next_written().await, sendcmpct(false));
    assert_eq!(promoted.next_written().await, sendcmpct(true));
    assert_eq!(
        slots.available_permits(),
        0,
        "the promoted connection keeps its slot"
    );
    demoted.assert_nothing_written().await;
}

/// A connection without compact blocks writes no `sendcmpct`, even if it is offered slots.
#[tokio::test]
async fn non_negotiated_connection_sends_nothing() {
    let _init_guard = zakura_test::init();

    let slots = Arc::new(Semaphore::new(3));
    let (connection, client_tx, _inbound, outbound, _error_slot) =
        super::new_test_connection::<PanicAssertion>();
    let mut connection = run(connection, client_tx, outbound, Some(slots.clone()));

    connection.assert_nothing_written().await;
    connection.slots_tx.send_replace(None);
    connection.slots_tx.send_replace(Some(slots.clone()));
    connection.assert_nothing_written().await;
    assert_eq!(slots.available_permits(), 3);
}

/// The peer's `sendcmpct` turns `cmpctblock` announcements on for version 1 only. The connection
/// consumes the message without an inbound request or a penalty.
#[tokio::test]
async fn received_sendcmpct_sets_send_for_version_1_only() {
    let _init_guard = zakura_test::init();

    let (mut connection, _client_tx, mut inbound, _outbound, error_slot) =
        super::new_early_relay_test_connection::<PanicAssertion>();

    for (announce, version, send) in [
        (true, 1, true),
        (true, 2, false),
        (true, 1, true),
        (false, 1, false),
        (true, 0, false),
        (false, 2, false),
    ] {
        let unused = connection
            .handle_message_as_request(Message::SendCompact { announce, version })
            .await;
        assert_eq!(unused, None, "the connection consumes sendcmpct");
        assert_eq!(
            connection.compact.send, send,
            "announce={announce} version={version}"
        );
    }

    inbound.expect_no_requests().await;
    assert!(
        error_slot.try_get_error().is_none(),
        "sendcmpct is not misbehavior"
    );
}

/// A running connection that negotiated compact blocks, and the channels that drive it and
/// observe its announcements.
struct AnnouncingConnection {
    /// The connection's peer message sender.
    peer_tx: mpsc::Sender<Result<Message, SerializationError>>,
    /// The connection's client request sender. The connection shuts down if it is dropped.
    _client_tx: mpsc::Sender<ClientRequest>,
    /// The connection's inbound service.
    inbound: MockService<Request, Response, PanicAssertion>,
    /// The messages the connection writes to its peer.
    outbound: mpsc::Receiver<Message>,
    /// The connection's block advert queue.
    adverts: tokio::sync::mpsc::Sender<BlockAdvert>,
    /// The connection's upload tracking.
    uploads: BlockUploads,
    /// The slots that bound decoded bodies, in place of the global ones.
    slots: Arc<Semaphore>,
    /// The connection's error.
    error_slot: ErrorSlot,
    /// The connection task.
    task: tokio::task::JoinHandle<()>,
}

/// The decoded body slots of a test connection, as many as the global ones.
const TEST_SLOTS: usize = 6;

impl AnnouncingConnection {
    /// Starts a negotiated connection. If `send` is true, the peer then asks for version 1
    /// `cmpctblock` announcements.
    async fn start(send: bool) -> Self {
        let (mut connection, client_tx, inbound, mut outbound, error_slot) =
            super::new_early_relay_test_connection::<PanicAssertion>();
        let slots = Arc::new(Semaphore::new(TEST_SLOTS));
        connection.compact.slots = slots.clone();
        let (adverts, advert_rx) = tokio::sync::mpsc::channel(BLOCK_ADVERT_QUEUE);
        connection.block_advert_rx = advert_rx;
        let uploads = connection.block_uploads.clone();
        let (mut peer_tx, peer_rx) = mpsc::channel(4);
        let task = tokio::spawn(connection.run(peer_rx));

        assert_eq!(outbound.next().await, Some(sendcmpct(false)));
        if send {
            peer_tx
                .send(Ok(sendcmpct(true)))
                .await
                .expect("the connection is open");
            settle().await;
        }

        Self {
            peer_tx,
            _client_tx: client_tx,
            inbound,
            outbound,
            adverts,
            uploads,
            slots,
            error_slot,
            task,
        }
    }

    /// Queues an advert for `hash` that shares `compact_slots` with its broadcast, and returns
    /// the receiver for its write result.
    fn advertise(
        &self,
        hash: block::Hash,
        compact_slots: Arc<Semaphore>,
    ) -> oneshot::Receiver<Result<(), BlockAdvertError>> {
        let (advert, written) = BlockAdvert::new(hash, compact_slots);
        self.adverts
            .try_send(advert)
            .expect("the advert queue has room");
        written
    }

    /// Answers the connection's early-relay request for `block` with the block.
    async fn serve_early_block(&mut self, block: &Arc<Block>) {
        self.inbound
            .expect_request(Request::EarlyBlocksByHash(IndexSet::from([block.hash()])))
            .await
            .respond(Response::Blocks(vec![InventoryResponse::Available((
                block.clone().into(),
                None,
            ))]));
    }

    /// Announces `block` with `cmpctblock`, and returns the message.
    async fn announce_compact(&mut self, block: &Arc<Block>) -> Arc<CompactBlock> {
        let written = self.advertise(block.hash(), compact_slots());
        self.serve_early_block(block).await;
        let Message::CompactBlock(compact) = self.next_written().await else {
            panic!("the connection announces the block with cmpctblock");
        };
        assert!(matches!(written.await, Ok(Ok(()))));
        compact
    }

    /// Sends `message` from the peer.
    async fn receive(&mut self, message: Message) {
        self.peer_tx
            .send(Ok(message))
            .await
            .expect("the connection is open");
    }

    /// Returns the next message the connection writes to its peer.
    async fn next_written(&mut self) -> Message {
        self.outbound
            .next()
            .await
            .expect("the running connection keeps its peer writer open")
    }

    /// Asserts that the connection has written nothing it hasn't already returned.
    async fn assert_nothing_written(&mut self) {
        settle().await;
        assert!(
            self.outbound.try_recv().is_err(),
            "the connection wrote an unexpected message"
        );
    }

    /// Waits until `permits` decoded body slots are free. The connection builds `cmpctblock`
    /// on a blocking thread, so yielding to it is not enough.
    async fn wait_for_free_slots(&self, permits: usize) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while self.slots.available_permits() != permits {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the connection releases its slot");
    }

    /// Asserts that the connection is still open.
    fn assert_open(&self) {
        assert!(
            self.error_slot.try_get_error().is_none(),
            "the connection is open"
        );
    }
}

impl Drop for AnnouncingConnection {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Returns a block whose `cmpctblock` is smaller than the block.
fn block_with_transactions() -> Arc<Block> {
    let block: Arc<Block> = BLOCK_MAINNET_1687107_BYTES
        .zcash_deserialize_into()
        .expect("the test vector is a valid block");
    assert!(
        block.transactions.len() >= 4,
        "the tests request several transactions"
    );
    block
}

/// Returns a block with only a coinbase transaction, whose `cmpctblock` is larger than the
/// block.
fn coinbase_only_block() -> Arc<Block> {
    let block: Arc<Block> = BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .expect("the test vector is a valid block");
    assert_eq!(block.transactions.len(), 1);
    block
}

/// Returns a block `inv` for `hash`.
fn inv(hash: block::Hash) -> Message {
    Message::Inv(vec![InventoryHash::Block(hash)])
}

/// Returns a `getblocktxn` for `indexes` of the block `hash`.
fn getblocktxn(hash: block::Hash, indexes: Vec<usize>) -> Message {
    Message::GetBlockTransactions(BlockTransactionsRequest {
        block_hash: hash,
        indexes,
    })
}

/// Returns the `blocktxn` that answers a request for `indexes` of `block`.
fn blocktxn(block: &Block, indexes: &[usize]) -> Message {
    Message::BlockTransactions(BlockTransactions {
        block_hash: block.hash(),
        transactions: indexes
            .iter()
            .map(|index| block.transactions[*index].clone())
            .collect(),
    })
}

/// A peer that asked for `cmpctblock` gets one with the coinbase transaction prefilled and a
/// short ID for each other transaction. The connection holds the block in a decoded body slot,
/// and the write uses up one of the broadcast's announcements.
#[tokio::test]
async fn send_peer_gets_cmpctblock_with_coinbase_prefilled() {
    let _init_guard = zakura_test::init();

    let block = block_with_transactions();
    let mut connection = AnnouncingConnection::start(true).await;
    let announcements = compact_slots();
    let written = connection.advertise(block.hash(), announcements.clone());
    connection.serve_early_block(&block).await;

    let Message::CompactBlock(compact) = connection.next_written().await else {
        panic!("the connection announces the block with cmpctblock");
    };
    assert!(matches!(written.await, Ok(Ok(()))));
    assert_eq!(compact.block_hash(), block.hash());
    assert_eq!(compact.prefilled.len(), 1);
    assert_eq!(compact.prefilled[0].index, 0);
    assert_eq!(compact.prefilled[0].transaction, block.transactions[0]);
    assert_eq!(compact.short_ids.len(), block.transactions.len() - 1);
    assert_eq!(connection.slots.available_permits(), TEST_SLOTS - 1);
    assert_eq!(
        announcements.available_permits(),
        COMPACT_ANNOUNCEMENTS_PER_BLOCK - 1
    );
    connection.assert_nothing_written().await;
    connection.assert_open();
}

/// A peer that did not ask for `cmpctblock` gets an `inv`, without an early-relay request.
#[tokio::test]
async fn non_send_peer_gets_inv() {
    let _init_guard = zakura_test::init();

    let block = block_with_transactions();
    let mut connection = AnnouncingConnection::start(false).await;
    let written = connection.advertise(block.hash(), compact_slots());

    assert_eq!(connection.next_written().await, inv(block.hash()));
    assert!(matches!(written.await, Ok(Ok(()))));
    connection.inbound.expect_no_requests().await;
    assert_eq!(connection.slots.available_permits(), TEST_SLOTS);
}

/// A broadcast without announcements left goes as an `inv`, without an early-relay request.
#[tokio::test]
async fn broadcast_without_announcements_left_gets_inv() {
    let _init_guard = zakura_test::init();

    let block = block_with_transactions();
    let mut connection = AnnouncingConnection::start(true).await;
    let written = connection.advertise(block.hash(), Arc::new(Semaphore::new(0)));

    assert_eq!(connection.next_written().await, inv(block.hash()));
    assert!(matches!(written.await, Ok(Ok(()))));
    connection.inbound.expect_no_requests().await;
}

/// A block that the inbound service doesn't return for an early-relay request, or an inbound
/// error, goes as an `inv` and releases the slot.
#[tokio::test]
async fn missing_early_block_gets_inv() {
    let _init_guard = zakura_test::init();

    let block = block_with_transactions();
    let mut connection = AnnouncingConnection::start(true).await;
    let request = Request::EarlyBlocksByHash(IndexSet::from([block.hash()]));

    let written = connection.advertise(block.hash(), compact_slots());
    connection
        .inbound
        .expect_request(request.clone())
        .await
        .respond(Response::Blocks(vec![InventoryResponse::Missing(
            block.hash(),
        )]));
    assert_eq!(connection.next_written().await, inv(block.hash()));
    assert!(matches!(written.await, Ok(Ok(()))));

    let written = connection.advertise(block.hash(), compact_slots());
    connection
        .inbound
        .expect_request(request)
        .await
        .respond_error("inbound service failed".into());
    assert_eq!(connection.next_written().await, inv(block.hash()));
    assert!(matches!(written.await, Ok(Ok(()))));

    assert_eq!(connection.slots.available_permits(), TEST_SLOTS);
    connection.assert_open();
}

/// A block whose `cmpctblock` is not smaller than the block goes as an `inv`, and releases
/// the slot.
#[tokio::test]
async fn compact_form_that_is_not_smaller_gets_inv() {
    let _init_guard = zakura_test::init();

    let block = coinbase_only_block();
    let mut connection = AnnouncingConnection::start(true).await;
    let announcements = compact_slots();
    let written = connection.advertise(block.hash(), announcements.clone());
    connection.serve_early_block(&block).await;

    assert_eq!(connection.next_written().await, inv(block.hash()));
    assert!(matches!(written.await, Ok(Ok(()))));
    assert_eq!(connection.slots.available_permits(), TEST_SLOTS);
    assert_eq!(
        announcements.available_permits(),
        COMPACT_ANNOUNCEMENTS_PER_BLOCK,
        "an inv leaves the broadcast's announcement to another connection"
    );
}

/// An advert whose result receiver closes while the connection builds its `cmpctblock` writes
/// nothing. The early-relay task closes it when it withdraws the block.
#[tokio::test]
async fn closed_advert_result_writes_nothing() {
    let _init_guard = zakura_test::init();

    let block = block_with_transactions();
    let mut connection = AnnouncingConnection::start(true).await;
    let announcements = compact_slots();
    let written = connection.advertise(block.hash(), announcements.clone());
    let early_request = connection
        .inbound
        .expect_request(Request::EarlyBlocksByHash(IndexSet::from([block.hash()])))
        .await;
    drop(written);
    early_request.respond(Response::Blocks(vec![InventoryResponse::Available((
        block.clone().into(),
        None,
    ))]));

    connection.wait_for_free_slots(TEST_SLOTS).await;
    connection.assert_nothing_written().await;
    assert_eq!(
        announcements.available_permits(),
        COMPACT_ANNOUNCEMENTS_PER_BLOCK
    );
    connection.assert_open();

    // The connection holds no block, so it announces the next one with `cmpctblock`.
    connection.announce_compact(&block).await;
}

/// While the connection holds a sent block, it announces the next block with `inv`.
#[tokio::test]
async fn second_advert_while_holding_a_block_gets_inv() {
    let _init_guard = zakura_test::init();

    let block = block_with_transactions();
    let next = block::Hash([9; 32]);
    let mut connection = AnnouncingConnection::start(true).await;
    connection.announce_compact(&block).await;

    let written = connection.advertise(next, compact_slots());
    assert_eq!(connection.next_written().await, inv(next));
    assert!(matches!(written.await, Ok(Ok(()))));
    connection.inbound.expect_no_requests().await;
    assert_eq!(connection.slots.available_permits(), TEST_SLOTS - 1);
}

/// The connection answers `getblocktxn` for the sent block with the requested transactions,
/// in the requested order.
#[tokio::test]
async fn getblocktxn_is_served_for_the_requested_indexes() {
    let _init_guard = zakura_test::init();

    let block = block_with_transactions();
    let mut connection = AnnouncingConnection::start(true).await;
    connection.announce_compact(&block).await;

    for indexes in [vec![1, 3], vec![], vec![0, 2]] {
        connection
            .receive(getblocktxn(block.hash(), indexes.clone()))
            .await;
        assert_eq!(connection.next_written().await, blocktxn(&block, &indexes));
    }
    connection.inbound.expect_no_requests().await;
    connection.assert_open();
}

/// The connection ignores `getblocktxn` for another block, with an out-of-range index, or with
/// indexes that do not strictly increase. None of them is misbehavior.
#[tokio::test]
async fn invalid_getblocktxn_is_ignored_without_a_penalty() {
    let _init_guard = zakura_test::init();

    let block = block_with_transactions();
    let count = block.transactions.len();
    let mut connection = AnnouncingConnection::start(true).await;
    connection.announce_compact(&block).await;

    for request in [
        getblocktxn(block::Hash([9; 32]), vec![1]),
        getblocktxn(block.hash(), vec![count]),
        getblocktxn(block.hash(), vec![1, usize::MAX]),
        getblocktxn(block.hash(), vec![2, 1]),
        getblocktxn(block.hash(), vec![1, 1]),
    ] {
        connection.receive(request).await;
        connection.assert_nothing_written().await;
        connection.assert_open();
    }

    // The connection still serves a valid request.
    connection.receive(getblocktxn(block.hash(), vec![1])).await;
    assert_eq!(connection.next_written().await, blocktxn(&block, &[1]));
}

/// Repeated `getblocktxn` requests for one announcement share a block's worth of bytes, and
/// each request costs its overhead, even an empty one. The connection ignores the request that
/// would exceed them, without a penalty.
#[tokio::test]
async fn getblocktxn_byte_budget_spans_repeated_requests() {
    let _init_guard = zakura_test::init();

    let block = block_with_transactions();
    let (largest, size) = block
        .transactions
        .iter()
        .map(|transaction| transaction.zcash_serialized_size())
        .enumerate()
        .max_by_key(|(_, size)| *size)
        .expect("the block has transactions");
    // A request costs the block hash, a one-byte index count, and its transactions.
    let overhead = 32 + 1;
    let cost = overhead + size;
    let budget = usize::try_from(MAX_BLOCK_BYTES).expect("2,000,000 fits in usize");
    let mut connection = AnnouncingConnection::start(true).await;
    connection.announce_compact(&block).await;

    for _ in 0..budget / cost {
        connection
            .receive(getblocktxn(block.hash(), vec![largest]))
            .await;
        assert_eq!(
            connection.next_written().await,
            blocktxn(&block, &[largest])
        );
    }
    connection
        .receive(getblocktxn(block.hash(), vec![largest]))
        .await;
    connection.assert_nothing_written().await;

    for _ in 0..budget % cost / overhead {
        connection.receive(getblocktxn(block.hash(), vec![])).await;
        assert_eq!(connection.next_written().await, blocktxn(&block, &[]));
    }
    connection.receive(getblocktxn(block.hash(), vec![])).await;
    connection.assert_nothing_written().await;
    connection.assert_open();
}

/// After its retention ends, the connection releases the sent block's slot, even while idle,
/// and ignores `getblocktxn` for it.
#[tokio::test(start_paused = true)]
async fn sent_block_expires_after_retention() {
    let _init_guard = zakura_test::init();

    let block = block_with_transactions();
    let mut connection = AnnouncingConnection::start(true).await;
    connection.announce_compact(&block).await;
    assert_eq!(connection.slots.available_permits(), TEST_SLOTS - 1);

    tokio::time::advance(RETENTION - Duration::from_millis(1)).await;
    settle().await;
    assert_eq!(
        connection.slots.available_permits(),
        TEST_SLOTS - 1,
        "the connection holds the block until its retention ends"
    );

    tokio::time::advance(Duration::from_millis(1)).await;
    settle().await;
    assert_eq!(connection.slots.available_permits(), TEST_SLOTS);

    connection.receive(getblocktxn(block.hash(), vec![1])).await;
    connection.assert_nothing_written().await;
    connection.assert_open();

    // The connection holds no block, so it announces the next one with `cmpctblock`.
    connection.announce_compact(&block).await;
}

/// A served `getblocktxn` counts as the peer's request for the advertised block. The advert
/// then succeeds once the `blocktxn` write completes, before its wait for a request ends.
#[tokio::test(start_paused = true)]
async fn served_getblocktxn_counts_as_the_block_request() {
    let _init_guard = zakura_test::init();

    let block = block_with_transactions();
    let mut connection = AnnouncingConnection::start(true).await;
    let gossip = gossip_peer(&connection);
    let start = tokio::time::Instant::now();
    let advert = tokio::spawn(gossip.advertise(block.hash()));
    connection.serve_early_block(&block).await;
    assert!(matches!(
        connection.next_written().await,
        Message::CompactBlock(_)
    ));

    connection.receive(getblocktxn(block.hash(), vec![1])).await;
    assert_eq!(connection.next_written().await, blocktxn(&block, &[1]));
    assert!(advert.await.expect("the advert task does not panic"));
    assert!(
        start.elapsed() < BLOCK_REQUEST_WAIT,
        "the blocktxn write ends the advert's tracking"
    );
}

/// A `blocktxn` write that doesn't finish before the sent block expires fails the connection,
/// and the advert's upload with it.
#[tokio::test(start_paused = true)]
async fn stalled_blocktxn_write_fails_the_connection_and_the_upload() {
    let _init_guard = zakura_test::init();

    let block = block_with_transactions();
    let mut connection = AnnouncingConnection::start(true).await;
    let gossip = gossip_peer(&connection);
    let advert = tokio::spawn(gossip.advertise(block.hash()));
    connection.serve_early_block(&block).await;
    assert!(matches!(
        connection.next_written().await,
        Message::CompactBlock(_)
    ));

    // The test peer writer finishes a write only when the test reads it, so the `blocktxn`
    // write stalls. Without the request, the advert would succeed once its request wait ends.
    connection.receive(getblocktxn(block.hash(), vec![1])).await;
    assert!(!advert.await.expect("the advert task does not panic"));
    assert!(connection.error_slot.try_get_error().is_some());
    assert_eq!(connection.slots.available_permits(), TEST_SLOTS);
}

/// Returns the gossip peer that advertises blocks through `connection`.
fn gossip_peer(connection: &AnnouncingConnection) -> BlockGossipPeer {
    BlockGossipPeer {
        announced: Default::default(),
        sender: connection.adverts.clone(),
        address: None,
        uploads: connection.uploads.clone(),
        registry: None,
        version: crate::constants::CURRENT_NETWORK_PROTOCOL_VERSION,
        sidecar: false,
        early_block_relay: true,
    }
}

/// How long a receiving test waits for the connection to write or answer, so a broken
/// connection fails the test instead of hanging it.
const TEST_WAIT: Duration = Duration::from_secs(60);

/// The address of the peer in the receiving tests.
fn receiving_peer_addr() -> PeerSocketAddr {
    "127.0.0.1:8233"
        .parse::<std::net::SocketAddr>()
        .expect("the literal is a socket address")
        .into()
}

/// The codec's view of the `blocktxn` a connection waits for.
type RecoveryWatch = watch::Receiver<Option<(block::Hash, tokio::time::Instant)>>;

/// A running connection that negotiated compact blocks and receives `cmpctblock`, and the
/// channels that drive and observe it.
struct ReceivingConnection {
    /// The connection's peer message sender.
    peer_tx: mpsc::Sender<Result<Message, SerializationError>>,
    /// The connection's client request sender.
    client_tx: mpsc::Sender<ClientRequest>,
    /// The connection's inbound service.
    inbound: MockService<Request, Response, PanicAssertion>,
    /// The messages the connection writes to its peer.
    outbound: mpsc::Receiver<Message>,
    /// The `blocktxn` the connection tells its codec to accept.
    recovery: RecoveryWatch,
    /// The slots that bound decoded bodies, in place of the global ones.
    slots: Arc<Semaphore>,
    /// The connection's error.
    error_slot: ErrorSlot,
    /// The connection task.
    task: tokio::task::JoinHandle<()>,
}

impl ReceivingConnection {
    /// Starts a negotiated connection to [`receiving_peer_addr`].
    async fn start() -> Self {
        let (mut connection, client_tx, inbound, mut outbound, error_slot) =
            super::new_early_relay_test_connection::<PanicAssertion>();
        Arc::make_mut(&mut connection.connection_info).connected_addr =
            ConnectedAddr::new_outbound_direct(receiving_peer_addr());
        let slots = Arc::new(Semaphore::new(TEST_SLOTS));
        connection.compact.slots = slots.clone();
        let (recovery_tx, recovery) = watch::channel(None);
        connection = connection.with_compact_recovery(Some(recovery_tx));
        let (peer_tx, peer_rx) = mpsc::channel(4);
        let task = tokio::spawn(connection.run(peer_rx));
        assert_eq!(outbound.next().await, Some(sendcmpct(false)));

        Self {
            peer_tx,
            client_tx,
            inbound,
            outbound,
            recovery,
            slots,
            error_slot,
            task,
        }
    }

    /// Sends `message` from the peer.
    async fn receive(&mut self, message: Message) {
        self.peer_tx
            .send(Ok(message))
            .await
            .expect("the connection is open");
    }

    /// Sends `compact` from the peer, and answers the header screen with a pass if `screened`.
    /// Then expects the download to start with this peer as its source.
    async fn receive_screened(&mut self, compact: &Arc<CompactBlock>, screened: bool) {
        self.receive(Message::CompactBlock(compact.clone())).await;
        let screen = self
            .inbound
            .expect_request(Request::CheckCompactHeader(compact.clone()))
            .await;
        if screened {
            screen.respond(Response::Nil);
        } else {
            screen.respond_error("the header failed screening".into());
        }
        self.expect_advertise(compact.block_hash()).await;
    }

    /// Expects the inbound request that starts the download of `hash` from this peer.
    async fn expect_advertise(&mut self, hash: block::Hash) {
        self.inbound
            .expect_request(Request::AdvertiseBlock(
                hash.into(),
                Some(PeerSource::LegacySocket(receiving_peer_addr())),
            ))
            .await
            .respond(Response::Nil);
    }

    /// Sends the downloader's request for the block `hash` from this peer, and returns the
    /// receiver for its response.
    async fn request_block(
        &mut self,
        hash: block::Hash,
    ) -> futures::channel::oneshot::Receiver<Result<Response, SharedPeerError>> {
        let (tx, rx) = futures::channel::oneshot::channel();
        self.client_tx
            .send(ClientRequest {
                request: Request::BlocksByHashFrom {
                    hashes: IndexSet::from([hash]),
                    source: receiving_peer_addr().into(),
                },
                tx,
                inv_collector: None,
                transient_addr: None,
                span: tracing::Span::current(),
            })
            .await
            .expect("the connection is open");
        rx
    }

    /// Answers the connection's mempool request for `compact` with `candidates`.
    async fn serve_candidates(&mut self, compact: &Arc<CompactBlock>, candidates: Vec<UnminedTx>) {
        self.inbound
            .expect_request(Request::CompactBlockTransactions(compact.clone()))
            .await
            .respond(Response::CompactBlockTransactions(candidates));
    }

    /// Answers the connection's check of the rebuilt `block` with a pass if `passes`.
    async fn check_block(&mut self, block: &Arc<Block>, passes: bool) {
        let check = self
            .inbound
            .expect_request(Request::CheckCompactBlock(block.clone()))
            .await;
        if passes {
            check.respond(Response::Nil);
        } else {
            check.respond_error("the rebuilt block failed its checks".into());
        }
    }

    /// Returns the next message the connection writes to its peer.
    async fn next_written(&mut self) -> Message {
        tokio::time::timeout(TEST_WAIT, self.outbound.next())
            .await
            .expect("the connection writes a message")
            .expect("the running connection keeps its peer writer open")
    }

    /// Asserts that the connection has written nothing it hasn't already returned.
    async fn assert_nothing_written(&mut self) {
        settle().await;
        assert!(
            self.outbound.try_recv().is_err(),
            "the connection wrote an unexpected message"
        );
    }

    /// Waits until `permits` decoded body slots are free.
    async fn wait_for_free_slots(&self, permits: usize) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while self.slots.available_permits() != permits {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the connection releases its slot");
    }

    /// Asserts that the connection is still open.
    fn assert_open(&self) {
        assert!(
            self.error_slot.try_get_error().is_none(),
            "the connection is open"
        );
    }
}

impl Drop for ReceivingConnection {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Returns the `cmpctblock` that announces `block`.
fn compact_block(block: &Arc<Block>) -> Arc<CompactBlock> {
    Arc::new(CompactBlock::from_block(block, 0x0123_4567_89ab_cdef).expect("the block fits"))
}

/// Returns the transactions of `block` except the coinbase and those at `missing`, as
/// mempool candidates.
fn candidates_except(block: &Block, missing: &[usize]) -> Vec<UnminedTx> {
    block
        .transactions
        .iter()
        .enumerate()
        .skip(1)
        .filter(|(index, _)| !missing.contains(index))
        .map(|(_, transaction)| transaction.clone().into())
        .collect()
}

/// Returns the response that delivers `block` from the receiving peer.
fn early_block_response(block: &Arc<Block>) -> Response {
    Response::EarlyBlocks(vec![InventoryResponse::Available((
        block.clone().into(),
        Some(receiving_peer_addr()),
    ))])
}

/// Returns the successful response that `response` receives.
async fn answer(
    response: futures::channel::oneshot::Receiver<Result<Response, SharedPeerError>>,
) -> Response {
    tokio::time::timeout(TEST_WAIT, response)
        .await
        .expect("the connection answers in time")
        .expect("the connection answers")
        .expect("the request succeeds")
}

/// Returns a `getdata` for the block `hash`.
fn getdata(hash: block::Hash) -> Message {
    Message::GetData(vec![InventoryHash::Block(hash)])
}

/// A `cmpctblock` that passes screening takes a decoded body slot, and starts the download
/// with this peer as its source.
#[tokio::test]
async fn screened_cmpctblock_is_held_and_advertised_from_this_peer() {
    let _init_guard = zakura_test::init();

    let block = block_with_transactions();
    let compact = compact_block(&block);
    let mut connection = ReceivingConnection::start().await;

    connection.receive_screened(&compact, true).await;
    settle().await;

    assert_eq!(connection.slots.available_permits(), TEST_SLOTS - 1);
    connection.assert_nothing_written().await;
    connection.assert_open();
}

/// A `cmpctblock` that fails screening holds no slot, and starts the download as an `inv`
/// would. The connection then downloads the full block, without a penalty.
#[tokio::test]
async fn failed_screen_becomes_a_plain_advertise() {
    let _init_guard = zakura_test::init();

    let block = block_with_transactions();
    let compact = compact_block(&block);
    let mut connection = ReceivingConnection::start().await;

    connection.receive_screened(&compact, false).await;
    settle().await;
    assert_eq!(connection.slots.available_permits(), TEST_SLOTS);

    let response = connection.request_block(block.hash()).await;
    assert_eq!(connection.next_written().await, getdata(block.hash()));
    connection
        .receive(Message::Block(block.clone().into()))
        .await;
    assert_eq!(answer(response).await, early_block_response(&block));
    connection.inbound.expect_no_requests().await;
    connection.assert_open();
}

/// A connection without a free decoded body slot advertises a screened `cmpctblock` without
/// holding it.
#[tokio::test]
async fn screened_cmpctblock_without_a_free_slot_is_not_held() {
    let _init_guard = zakura_test::init();

    let block = block_with_transactions();
    let compact = compact_block(&block);
    let mut connection = ReceivingConnection::start().await;
    let held = Arc::clone(&connection.slots)
        .try_acquire_many_owned(TEST_SLOTS.try_into().expect("6 fits in u32"))
        .expect("the slots are free");

    connection.receive_screened(&compact, true).await;
    drop(held);

    let _response = connection.request_block(block.hash()).await;
    assert_eq!(connection.next_written().await, getdata(block.hash()));
    connection.inbound.expect_no_requests().await;
}

/// A second `cmpctblock` within [`RETENTION`] of a screen is not screened, and becomes a plain
/// advertise. The cooldown starts even when the screen fails.
#[tokio::test(start_paused = true)]
async fn second_cmpctblock_within_retention_is_a_plain_advertise() {
    let _init_guard = zakura_test::init();

    let block = block_with_transactions();
    let compact = compact_block(&block);
    let mut connection = ReceivingConnection::start().await;

    connection.receive_screened(&compact, false).await;

    tokio::time::advance(RETENTION - Duration::from_millis(1)).await;
    connection
        .receive(Message::CompactBlock(compact.clone()))
        .await;
    connection.expect_advertise(block.hash()).await;
    settle().await;
    assert_eq!(connection.slots.available_permits(), TEST_SLOTS);

    // The cooldown has ended, so the connection screens the next one.
    tokio::time::advance(Duration::from_millis(1)).await;
    connection.receive_screened(&compact, true).await;
    settle().await;
    assert_eq!(connection.slots.available_permits(), TEST_SLOTS - 1);
    connection.assert_open();
}

/// A held `cmpctblock` that the mempool fills answers the download with the rebuilt block,
/// without writing `getdata`. The connection keeps the rebuilt block, and answers a second
/// request for it at once.
#[tokio::test]
async fn full_reconstruction_answers_without_getdata() {
    let _init_guard = zakura_test::init();

    let block = block_with_transactions();
    let compact = compact_block(&block);
    let mut connection = ReceivingConnection::start().await;
    connection.receive_screened(&compact, true).await;

    let response = connection.request_block(block.hash()).await;
    connection
        .serve_candidates(&compact, candidates_except(&block, &[]))
        .await;
    connection.check_block(&block, true).await;
    assert_eq!(answer(response).await, early_block_response(&block));
    connection.assert_nothing_written().await;
    assert_eq!(
        connection.slots.available_permits(),
        TEST_SLOTS - 1,
        "the rebuilt block keeps the received block's slot"
    );

    // A second downloader gets the held block without any inbound request or write.
    let response = connection.request_block(block.hash()).await;
    assert_eq!(answer(response).await, early_block_response(&block));
    connection.inbound.expect_no_requests().await;
    connection.assert_nothing_written().await;
    connection.assert_open();
}

/// A held `cmpctblock` with transactions missing from the mempool writes `getblocktxn` for
/// them, and tells the codec to accept the matching `blocktxn` for [`RECOVERY_TIMEOUT`]. The
/// matching `blocktxn` finishes the download.
#[tokio::test]
async fn partial_reconstruction_recovers_missing_transactions() {
    let _init_guard = zakura_test::init();

    let block = block_with_transactions();
    let compact = compact_block(&block);
    let missing = [1, 3];
    let mut connection = ReceivingConnection::start().await;
    connection.receive_screened(&compact, true).await;

    let response = connection.request_block(block.hash()).await;
    connection
        .serve_candidates(&compact, candidates_except(&block, &missing))
        .await;
    let requested = tokio::time::Instant::now();
    assert_eq!(
        connection.next_written().await,
        getblocktxn(block.hash(), missing.to_vec())
    );
    // The connection sets the watch right after its write completes.
    settle().await;
    let (hash, deadline) = connection
        .recovery
        .borrow()
        .expect("the connection waits for blocktxn");
    assert_eq!(hash, block.hash());
    assert!(deadline >= requested + RECOVERY_TIMEOUT);
    assert!(deadline <= tokio::time::Instant::now() + RECOVERY_TIMEOUT);

    connection.receive(blocktxn(&block, &missing)).await;
    connection.check_block(&block, true).await;
    assert_eq!(answer(response).await, early_block_response(&block));
    assert_eq!(*connection.recovery.borrow(), None);
    connection.assert_nothing_written().await;
    connection.assert_open();
}

/// A `blocktxn` for another block leaves the pending recovery waiting, without a penalty.
#[tokio::test]
async fn blocktxn_for_another_block_is_ignored() {
    let _init_guard = zakura_test::init();

    let block = block_with_transactions();
    let compact = compact_block(&block);
    let mut connection = ReceivingConnection::start().await;
    connection.receive_screened(&compact, true).await;

    let mut response = connection.request_block(block.hash()).await;
    connection
        .serve_candidates(&compact, candidates_except(&block, &[2]))
        .await;
    assert_eq!(
        connection.next_written().await,
        getblocktxn(block.hash(), vec![2])
    );

    connection
        .receive(Message::BlockTransactions(BlockTransactions {
            block_hash: block::Hash([9; 32]),
            transactions: vec![block.transactions[2].clone()],
        }))
        .await;
    connection.inbound.expect_no_requests().await;
    connection.assert_nothing_written().await;
    assert!(matches!(response.try_recv(), Ok(None)));
    assert!(connection.recovery.borrow().is_some());

    connection.receive(blocktxn(&block, &[2])).await;
    connection.check_block(&block, true).await;
    assert_eq!(answer(response).await, early_block_response(&block));
    connection.assert_open();
}

/// A recovery that times out after [`RECOVERY_TIMEOUT`] downloads the full block from the same
/// peer, without failing the request.
#[tokio::test(start_paused = true)]
async fn recovery_timeout_falls_back_to_getdata() {
    let _init_guard = zakura_test::init();

    let block = block_with_transactions();
    let compact = compact_block(&block);
    let mut connection = ReceivingConnection::start().await;
    connection.receive_screened(&compact, true).await;

    let mut response = connection.request_block(block.hash()).await;
    connection
        .serve_candidates(&compact, candidates_except(&block, &[2]))
        .await;
    assert_eq!(
        connection.next_written().await,
        getblocktxn(block.hash(), vec![2])
    );
    let requested = tokio::time::Instant::now();

    assert_eq!(connection.next_written().await, getdata(block.hash()));
    let waited = requested.elapsed();
    assert!(waited >= RECOVERY_TIMEOUT, "{waited:?}");
    assert!(waited < REQUEST_TIMEOUT, "{waited:?}");
    assert_eq!(*connection.recovery.borrow(), None);
    assert!(matches!(response.try_recv(), Ok(None)));
    // The fallback releases the received block's slot.
    connection.wait_for_free_slots(TEST_SLOTS).await;

    connection
        .receive(Message::Block(block.clone().into()))
        .await;
    assert_eq!(answer(response).await, early_block_response(&block));
    connection.assert_open();
}

/// A rebuilt block that fails its checks downloads the full block from the same peer. A short
/// ID can match the wrong transaction, so the failure is not misbehavior.
#[tokio::test]
async fn failed_block_check_falls_back_to_getdata() {
    let _init_guard = zakura_test::init();

    let block = block_with_transactions();
    let compact = compact_block(&block);
    let mut connection = ReceivingConnection::start().await;
    connection.receive_screened(&compact, true).await;

    let response = connection.request_block(block.hash()).await;
    connection
        .serve_candidates(&compact, candidates_except(&block, &[]))
        .await;
    connection.check_block(&block, false).await;
    assert_eq!(connection.next_written().await, getdata(block.hash()));
    connection.wait_for_free_slots(TEST_SLOTS).await;

    connection
        .receive(Message::Block(block.clone().into()))
        .await;
    assert_eq!(answer(response).await, early_block_response(&block));
    connection.assert_open();
}

/// A recovered block that fails its checks downloads the full block from the same peer.
#[tokio::test]
async fn failed_recovered_block_check_falls_back_to_getdata() {
    let _init_guard = zakura_test::init();

    let block = block_with_transactions();
    let compact = compact_block(&block);
    let mut connection = ReceivingConnection::start().await;
    connection.receive_screened(&compact, true).await;

    let mut response = connection.request_block(block.hash()).await;
    connection
        .serve_candidates(&compact, candidates_except(&block, &[2]))
        .await;
    assert_eq!(
        connection.next_written().await,
        getblocktxn(block.hash(), vec![2])
    );
    connection.receive(blocktxn(&block, &[2])).await;
    connection.check_block(&block, false).await;

    assert_eq!(connection.next_written().await, getdata(block.hash()));
    assert_eq!(*connection.recovery.borrow(), None);
    assert!(matches!(response.try_recv(), Ok(None)));
    connection
        .receive(Message::Block(block.clone().into()))
        .await;
    assert_eq!(answer(response).await, early_block_response(&block));
    connection.assert_open();
}

/// A connection that didn't negotiate compact blocks ignores `cmpctblock`, without an inbound
/// request or a penalty.
#[tokio::test]
async fn non_negotiated_connection_ignores_cmpctblock() {
    let _init_guard = zakura_test::init();

    let block = block_with_transactions();
    let (mut connection, _client_tx, mut inbound, _outbound, error_slot) =
        super::new_test_connection::<PanicAssertion>();

    let unused = tokio::time::timeout(
        TEST_WAIT,
        connection.handle_message_as_request(Message::CompactBlock(compact_block(&block))),
    )
    .await
    .expect("the connection ignores the message without an inbound request");

    assert_eq!(unused, None);
    inbound.expect_no_requests().await;
    assert!(error_slot.try_get_error().is_none());
    assert_eq!(connection.compact.expiry(), None);
}

/// After its retention ends, the connection releases a received block's slot, and downloads
/// the full block.
#[tokio::test(start_paused = true)]
async fn received_block_expires_after_retention() {
    let _init_guard = zakura_test::init();

    let block = block_with_transactions();
    let compact = compact_block(&block);
    let mut connection = ReceivingConnection::start().await;
    connection.receive_screened(&compact, true).await;
    settle().await;
    assert_eq!(connection.slots.available_permits(), TEST_SLOTS - 1);

    tokio::time::advance(RETENTION).await;
    settle().await;
    assert_eq!(connection.slots.available_permits(), TEST_SLOTS);

    let _response = connection.request_block(block.hash()).await;
    assert_eq!(connection.next_written().await, getdata(block.hash()));
    connection.inbound.expect_no_requests().await;
}

/// Shutting down a connection that waits for `blocktxn` tells the codec to stop accepting it,
/// and releases the slot.
#[tokio::test]
async fn shutdown_clears_the_recovery() {
    let _init_guard = zakura_test::init();

    let block = block_with_transactions();
    let compact = compact_block(&block);
    let mut connection = ReceivingConnection::start().await;
    connection.receive_screened(&compact, true).await;
    let _response = connection.request_block(block.hash()).await;
    connection
        .serve_candidates(&compact, candidates_except(&block, &[2]))
        .await;
    assert_eq!(
        connection.next_written().await,
        getblocktxn(block.hash(), vec![2])
    );
    settle().await;
    assert!(connection.recovery.borrow().is_some());

    // Closing the peer stream fails the connection.
    connection.peer_tx.close_channel();
    connection.wait_for_free_slots(TEST_SLOTS).await;
    assert_eq!(*connection.recovery.borrow(), None);
}
