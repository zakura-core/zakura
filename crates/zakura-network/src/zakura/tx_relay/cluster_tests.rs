//! `tx-relay` on a cluster of real v2 nodes, each with the legacy adapter
//! beside it: the cases the rollout names and the ones review added.
//!
//! Most cases face a node with a raw peer: a real QUIC connection and
//! handshake, with `tx-relay`'s two streams opened by hand, so a case can
//! send any message, read either stream or leave it unread, and pace its
//! writes. A raw peer only ever dials, so the node takes its sessions as
//! the acceptor.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex as StdMutex, Weak},
    time::Duration,
};

use tokio::{sync::mpsc, task::JoinHandle, time::Instant};
use zakura_chain::{
    amount::Amount,
    block::Height,
    parameters::Network,
    serialization::ZcashSerialize,
    transaction::{LockTime, Transaction, UnminedTx, UnminedTxId, VerifiedUnminedTx},
    transparent,
};
use zakura_node_services::mempool::port::{
    Change, Charge, Rejection, RejectionScope, Verdict as PortVerdict,
};
use zakura_quic::{QuicConfig, RecvStream, SendStream};

use super::{
    machine::{ADVERT_DELAY, MAX_SERVES, SMALL_TX_BYTES, WINDOW_SMALL},
    service::MayCool,
    test_node::{Held, RelayNode, TestPool},
    tx_relay_service,
    wire::{
        TxRelayMessage, MAX_TX_BYTES, TX_RELAY_DATA_KIND, TX_RELAY_STREAMS, ZAKURA_CAP_TX_RELAY,
    },
};
use crate::{
    zakura::{
        testkit::{await_until, stream_conformance::RawLayoutPeer, HostilePeer, TEST_NET_TIMEOUT},
        wire_codec::{decode_frame, encode_frame},
        CloseOutcome, CloseRow, LegacyGossipFrame, SessionStep, TransportEvent, TransportObserver,
        ZakuraConnId, ZakuraLocalLimits, ZakuraPeerId, ZAKURA_CAP_DISCOVERY,
        ZAKURA_DISCOVERY_STREAM_VERSION, ZAKURA_SAME_IP_DUPLICATE_EVICT_MIN_AGE,
        ZAKURA_STREAM_DISCOVERY, ZAKURA_STREAM_GOSSIP,
    },
    BoxError, Config, Request,
};

/// A fault of `row` on the `tx-relay` session's first stream.
fn fault(row: CloseRow) -> CloseOutcome {
    let [primary, _] = TX_RELAY_STREAMS;
    CloseOutcome::fault(primary.kind, primary.version, row)
}

/// The bound on every wait for something that should happen.
const NET: Duration = TEST_NET_TIMEOUT;

/// How long a negative check watches for an ask that must not come. An ask
/// goes out as soon as the node handles the announcement, so this is slack.
const NO_ASK: Duration = Duration::from_millis(1_500);

/// A v5 Mainnet transaction.
fn transaction() -> VerifiedUnminedTx {
    Network::Mainnet
        .unmined_transactions_in_blocks(..)
        .find(|tx| tx.transaction.id().auth_digest().is_some())
        .expect("the Mainnet vectors hold a v5 transaction")
}

/// `count` distinct Mainnet transactions.
fn vectors(count: usize) -> Vec<VerifiedUnminedTx> {
    let mut seen = std::collections::HashSet::new();
    let found: Vec<VerifiedUnminedTx> = Network::Mainnet
        .unmined_transactions_in_blocks(..)
        .filter(|tx| seen.insert(tx.transaction.id()))
        .take(count)
        .collect();
    assert_eq!(found.len(), count, "the Mainnet vectors hold enough");
    found
}

/// A synthetic transaction of `bytes` script bytes and a few bytes more,
/// distinct for each `tag`: one transparent output whose script repeats
/// `tag`. It decodes like any other, and the stand-in core never verifies
/// it, so it stands in for a large body.
fn large(tag: u8, bytes: usize) -> VerifiedUnminedTx {
    let transaction = Transaction::V1 {
        inputs: Vec::new(),
        outputs: vec![transparent::Output {
            value: Amount::zero(),
            lock_script: transparent::Script::new(&vec![tag; bytes]),
        }],
        lock_time: LockTime::unlocked(),
    };
    VerifiedUnminedTx::new(
        UnminedTx::from(transaction),
        Amount::try_from(1_000_000_000).expect("a valid test fee"),
        0,
        0,
        Arc::new(Vec::new()),
    )
    .expect("the test fee pays for any body up to the protocol's size")
}

/// The id of `tx`.
fn id(tx: &VerifiedUnminedTx) -> UnminedTxId {
    tx.transaction.id()
}

/// The serialized size of `tx`, as an `Inv` announces it.
fn size(tx: &VerifiedUnminedTx) -> u32 {
    u32::try_from(tx.transaction.size()).expect("test bodies fit the protocol's size")
}

/// The `Inv` entry for `tx`.
fn entry(tx: &VerifiedUnminedTx) -> (UnminedTxId, u32) {
    (id(tx), size(tx))
}

/// The `Tx` answer carrying `tx`.
fn answer(tx: &VerifiedUnminedTx) -> TxRelayMessage {
    TxRelayMessage::Tx {
        id: id(tx),
        body: tx
            .transaction
            .transaction()
            .zcash_serialize_to_vec()
            .expect("a test transaction serializes"),
    }
}

/// An id no pool holds.
fn unknown(n: u32) -> UnminedTxId {
    let mut hash = [0xAB; 32];
    hash[..4].copy_from_slice(&n.to_le_bytes());
    UnminedTxId::from_legacy_id(zakura_chain::transaction::Hash(hash))
}

/// Adds `tx` to `pool`'s visible transactions and publishes its admission.
fn admit(pool: &TestPool, tx: &VerifiedUnminedTx) {
    let added = tx.clone();
    pool.change(
        move |held| held.transactions.push(added),
        vec![Change::Added(id(tx))],
    );
}

/// A rejection that earns its supplier `charge`, decided at `tip`.
fn rejected(charge: Charge, tip: Height) -> PortVerdict {
    PortVerdict::Rejected(Rejection {
        scope: RejectionScope::ExactTip,
        charge,
        tip,
        detail: Arc::new(std::io::Error::other("a test rejection")),
    })
}

/// A `may_cool` that lets every verdict cool.
fn cools() -> MayCool {
    Arc::new(|_| true)
}

/// The test node's default limits with room for `connections` peers.
fn limits_for(connections: usize) -> ZakuraLocalLimits {
    let mut limits = ZakuraLocalLimits::from_config(&Config::default());
    limits.max_connections = connections;
    limits.max_pending_handshakes = 8;
    limits.max_open_streams = 16;
    limits.max_inbound_queue_depth = 64;
    limits
}

/// QUIC settings for a peer whose `tx-relay` stream windows are `window`
/// bytes, so a node writing to a stream it does not read is held back by
/// flow control within that many bytes.
fn narrow(victim: &RelayNode, window: u32) -> QuicConfig {
    let mut quic = victim.node.limits().quic.clone();
    quic.stream_receive_window_bytes = window;
    quic
}

/// The identity of `node`.
async fn peer_id(node: &RelayNode) -> Result<ZakuraPeerId, BoxError> {
    Ok(ZakuraPeerId::new(
        node.node.node_addr().await.id.as_bytes().to_vec(),
    )?)
}

/// What a node's transport reported: each connection's close cause and how
/// many `tx-relay` session setups expired.
#[derive(Debug, Default)]
struct Observed(StdMutex<Seen>);

/// The events [`Observed`] keeps.
#[derive(Debug, Default)]
struct Seen {
    /// Each connection's id, by the TLS-derived id both ends share.
    connections: HashMap<[u8; 16], ZakuraConnId>,
    /// Each connection's first close cause.
    causes: HashMap<ZakuraConnId, Option<CloseOutcome>>,
    /// `tx-relay` setups that expired before they completed.
    expired: usize,
    /// How many `tx-relay` sessions each connection's transport saw exit.
    exited: HashMap<ZakuraConnId, usize>,
}

impl TransportObserver for Observed {
    /// Keeps registrations, close causes, and expired and exited `tx-relay`
    /// sessions.
    fn observe(&self, event: TransportEvent) {
        let mut seen = self.0.lock().expect("no test panics while holding it");
        match event {
            TransportEvent::Registered {
                conn_id, shared_id, ..
            } => {
                seen.connections.insert(shared_id, conn_id);
            }
            TransportEvent::CloseDecided {
                conn_id: Some(conn_id),
                cause,
                ..
            } => {
                seen.causes.entry(conn_id).or_insert(cause);
            }
            TransportEvent::SessionLifecycle {
                kind: TX_RELAY_DATA_KIND,
                step: SessionStep::OfferExpired { .. },
                ..
            } => seen.expired += 1,
            TransportEvent::SessionLifecycle {
                conn_id,
                kind: TX_RELAY_DATA_KIND,
                step: SessionStep::GenerationExited { .. },
            } => *seen.exited.entry(conn_id).or_default() += 1,
            _ => {}
        }
    }
}

impl Observed {
    /// The cause the node recorded for closing `peer`'s connection.
    fn cause(&self, peer: &RawRelay) -> Option<CloseOutcome> {
        let seen = self.0.lock().expect("no test panics while holding it");
        let conn_id = seen.connections.get(&peer.raw.connection.shared_id())?;
        seen.causes.get(conn_id).copied().flatten()
    }

    /// `tx-relay` setups that expired.
    fn expired(&self) -> usize {
        self.0
            .lock()
            .expect("no test panics while holding it")
            .expired
    }

    /// How many `tx-relay` sessions on `peer`'s connection the transport
    /// saw exit. Until it sees one, it refuses the peer's next offer as a
    /// duplicate.
    fn exited(&self, peer: &RawRelay) -> usize {
        let seen = self.0.lock().expect("no test panics while holding it");
        let conn_id = seen.connections.get(&peer.raw.connection.shared_id());
        conn_id
            .and_then(|conn_id| seen.exited.get(conn_id).copied())
            .unwrap_or(0)
    }
}

/// A node with `pool` and an [`Observed`] transport, with room for
/// `connections` peers.
async fn observed_node(
    seed: u64,
    pool: TestPool,
    may_cool: MayCool,
    connections: usize,
) -> Result<(RelayNode, Arc<Observed>), BoxError> {
    let observed = Arc::new(Observed::default());
    let observer = observed.clone();
    let node = RelayNode::spawn_with(seed, Some(pool), may_cool, move |builder| {
        builder
            .limits(limits_for(connections))
            .max_connections_per_ip(connections)
            .transport_observer(observer)
    })
    .await?;
    Ok((node, observed))
}

/// What a raw peer's stream reader hands the test.
enum Got {
    /// A decoded frame.
    Message(TxRelayMessage),
    /// The stream ended, by FIN, reset or the connection's close.
    Ended,
}

/// A raw `tx-relay` peer of a node.
struct RawRelay {
    raw: RawLayoutPeer,
    max_frame: u32,
    data: Option<SendStream>,
    state: Option<SendStream>,
    /// A stream's receive side, until [`Self::read_data`] or
    /// [`Self::read_state`] hands it to a reader. Unread, it holds the
    /// node's writes back once its window is full.
    data_recv: Option<RecvStream>,
    state_recv: Option<RecvStream>,
    data_rx: Option<mpsc::UnboundedReceiver<Got>>,
    state_rx: Option<mpsc::UnboundedReceiver<Got>>,
    /// The readers of the data and state streams, once started.
    data_reader: Option<JoinHandle<()>>,
    state_reader: Option<JoinHandle<()>>,
}

impl RawRelay {
    /// Connects to `victim` as peer `seed`, offering `tx-relay` only.
    async fn connect(victim: &RelayNode, seed: u64) -> Result<Self, BoxError> {
        Self::connect_with(victim, seed, ZAKURA_CAP_TX_RELAY, None).await
    }

    /// Connects to `victim` as peer `seed`, offering `capabilities`, with
    /// `quic` as its transport settings if given.
    async fn connect_with(
        victim: &RelayNode,
        seed: u64,
        capabilities: u64,
        quic: Option<QuicConfig>,
    ) -> Result<Self, BoxError> {
        let mut limits = victim.node.limits().clone();
        if let Some(quic) = quic {
            limits.quic = quic;
        }
        let raw =
            RawLayoutPeer::connect(victim.node.node_addr().await, &limits, seed, capabilities)
                .await?;
        Ok(Self {
            raw,
            max_frame: limits.max_frame_bytes,
            data: None,
            state: None,
            data_recv: None,
            state_recv: None,
            data_rx: None,
            state_rx: None,
            data_reader: None,
            state_reader: None,
        })
    }

    /// Connects and starts a session as [`Self::start`] does, retrying
    /// while the node still holds a connection of the same identity, which
    /// it refuses a duplicate of until that one is gone.
    async fn reconnect(victim: &RelayNode, seed: u64) -> Result<Self, BoxError> {
        let started = Instant::now();
        loop {
            let attempt = async {
                let mut peer = Self::connect(victim, seed).await?;
                peer.start().await?;
                Ok::<Self, BoxError>(peer)
            };
            match attempt.await {
                Ok(peer) => return Ok(peer),
                Err(error) if started.elapsed() > NET => return Err(error),
                // A bounded retry while the old connection goes away.
                Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
            }
        }
    }

    /// The peer's identity.
    fn id(&self) -> ZakuraPeerId {
        self.raw.id()
    }

    /// Opens `members` of the layout, data first, as one session offer.
    async fn offer(&mut self, members: usize) -> Result<(), BoxError> {
        self.stop_readers();
        self.data_rx = None;
        self.state_rx = None;
        let wire_id = rand_10::random::<u64>().max(1);
        let mut opened = Vec::new();
        for member in TX_RELAY_STREAMS.iter().take(members) {
            let (mut send, recv) = self.raw.open_stream(member.kind, member.version).await?;
            send.write_all(&wire_id.to_le_bytes()).await?;
            opened.push((send, recv));
        }
        let mut opened = opened.into_iter();
        if let Some((send, recv)) = opened.next() {
            self.data = Some(send);
            self.data_recv = Some(recv);
        }
        if let Some((send, recv)) = opened.next() {
            self.state = Some(send);
            self.state_recv = Some(recv);
        }
        Ok(())
    }

    /// Opens a session's two streams and reads neither.
    async fn open(&mut self) -> Result<(), BoxError> {
        self.offer(TX_RELAY_STREAMS.len()).await
    }

    /// Starts reading the data stream.
    fn read_data(&mut self) {
        if let Some(recv) = self.data_recv.take() {
            let (rx, reader) = spawn_reader(recv, self.max_frame);
            self.data_rx = Some(rx);
            self.data_reader = Some(reader);
        }
    }

    /// Starts reading the state stream.
    fn read_state(&mut self) {
        if let Some(recv) = self.state_recv.take() {
            let (rx, reader) = spawn_reader(recv, self.max_frame);
            self.state_rx = Some(rx);
            self.state_reader = Some(reader);
        }
    }

    /// Stops both readers, which drops their receive sides.
    fn stop_readers(&mut self) {
        for reader in [self.data_reader.take(), self.state_reader.take()]
            .into_iter()
            .flatten()
        {
            reader.abort();
        }
    }

    /// Sends our `Hello` and waits for the node's.
    async fn hello(&mut self) -> Result<(), BoxError> {
        self.send(&TxRelayMessage::Hello {
            serves: 16,
            max_tx_bytes: MAX_TX_BYTES,
        })
        .await?;
        match self.next_state().await? {
            TxRelayMessage::Hello { serves, .. } => {
                assert_eq!(usize::from(serves), MAX_SERVES);
                Ok(())
            }
            other => Err(format!("expected the node's Hello first, got {other:?}").into()),
        }
    }

    /// Opens a session, reads both streams and exchanges `Hello`s.
    async fn start(&mut self) -> Result<(), BoxError> {
        self.open().await?;
        self.read_data();
        self.read_state();
        self.hello().await
    }

    /// Sends `message` on the stream it travels on.
    async fn send(&mut self, message: &TxRelayMessage) -> Result<(), BoxError> {
        let bytes = encode_frame(message)?.encode(self.max_frame)?;
        let stream = if message.stream_kind() == TX_RELAY_DATA_KIND {
            &mut self.data
        } else {
            &mut self.state
        };
        let stream = stream.as_mut().ok_or("the session is not open")?;
        tokio::time::timeout(NET, stream.write_all(&bytes))
            .await
            .map_err(|_| "timed out writing a frame")??;
        Ok(())
    }

    /// Writes `messages` on the data stream at about `rate` bytes a second,
    /// as a link of that speed would deliver them.
    async fn send_paced(
        &mut self,
        messages: &[TxRelayMessage],
        rate: usize,
    ) -> Result<(), BoxError> {
        let mut bytes = Vec::new();
        for message in messages {
            bytes.extend(encode_frame(message)?.encode(self.max_frame)?);
        }
        let stream = self.data.as_mut().ok_or("the session is not open")?;
        // A tenth of a second's bytes every tenth of a second.
        let mut ticks = tokio::time::interval(Duration::from_millis(100));
        for chunk in bytes.chunks(rate / 10) {
            ticks.tick().await;
            tokio::time::timeout(NET, stream.write_all(chunk))
                .await
                .map_err(|_| "timed out writing a paced chunk")??;
        }
        Ok(())
    }

    /// The next message on the state stream.
    async fn next_state(&mut self) -> Result<TxRelayMessage, BoxError> {
        next(self.state_rx.as_mut().ok_or("the state stream is unread")?).await
    }

    /// The next message on the data stream.
    async fn next_data(&mut self) -> Result<TxRelayMessage, BoxError> {
        next(self.data_rx.as_mut().ok_or("the data stream is unread")?).await
    }

    /// Waits for the node's `GetTxs` for `id` on the state stream, skipping
    /// anything else.
    async fn asked(&mut self, id: UnminedTxId) -> Result<(), BoxError> {
        loop {
            if self.next_state().await? == TxRelayMessage::GetTxs(id) {
                return Ok(());
            }
        }
    }

    /// Waits for the node's `GetTxs` for every one of `ids`, in any order,
    /// skipping anything else.
    async fn asked_all(&mut self, ids: &[UnminedTxId]) -> Result<(), BoxError> {
        let mut waiting: Vec<UnminedTxId> = ids.to_vec();
        while !waiting.is_empty() {
            if let TxRelayMessage::GetTxs(asked) = self.next_state().await? {
                waiting.retain(|id| *id != asked);
            }
        }
        Ok(())
    }

    /// Checks the node does not ask for `id` within [`NO_ASK`]. Other state
    /// messages are skipped.
    async fn not_asked(&mut self, id: UnminedTxId) -> Result<(), BoxError> {
        let rx = self.state_rx.as_mut().ok_or("the state stream is unread")?;
        let deadline = Instant::now() + NO_ASK;
        while let Ok(got) = tokio::time::timeout_at(deadline, rx.recv()).await {
            match got {
                Some(Got::Message(TxRelayMessage::GetTxs(asked))) if asked == id => {
                    return Err(format!("the node asked for {id:?}").into());
                }
                Some(Got::Message(_)) => {}
                Some(Got::Ended) | None => return Err("the state stream ended".into()),
            }
        }
        Ok(())
    }

    /// Waits for an `Inv` that names `id` on the data stream, skipping
    /// anything else.
    async fn announced(&mut self, id: UnminedTxId) -> Result<(), BoxError> {
        loop {
            if let TxRelayMessage::Inv(entries) = self.next_data().await? {
                if entries.iter().any(|(announced, _)| *announced == id) {
                    return Ok(());
                }
            }
        }
    }

    /// Resets the data stream both ways, which retires the session.
    fn reset(&mut self) -> Result<(), BoxError> {
        if let Some(send) = self.data.as_mut() {
            send.reset(0u32.into())?;
        }
        // Dropping the receive side, here or in its reader, stops it.
        if let Some(reader) = self.data_reader.take() {
            reader.abort();
        }
        self.data_recv = None;
        self.data_rx = None;
        Ok(())
    }

    /// Waits up to `within` until the node ends the data stream, which it
    /// reads, so the session is over.
    async fn data_ended(&mut self, within: Duration) -> Result<(), BoxError> {
        let rx = self.data_rx.as_mut().ok_or("the data stream is unread")?;
        tokio::time::timeout(within, async {
            while let Some(got) = rx.recv().await {
                if matches!(got, Got::Ended) {
                    return;
                }
            }
        })
        .await
        .map_err(|_| "timed out waiting for the data stream to end")?;
        Ok(())
    }

    /// Waits until the node ends the state stream, so the transport has
    /// retired the session.
    async fn state_ended(&mut self) -> Result<(), BoxError> {
        let rx = self.state_rx.as_mut().ok_or("the state stream is unread")?;
        tokio::time::timeout(NET, async {
            while let Some(got) = rx.recv().await {
                if matches!(got, Got::Ended) {
                    return;
                }
            }
        })
        .await
        .map_err(|_| "timed out waiting for the state stream to end")?;
        Ok(())
    }

    /// Whether the node closed the connection.
    fn is_closed(&self) -> bool {
        self.raw.is_closed()
    }

    /// Waits until the node closes the connection.
    async fn closed(&self) -> Result<(), BoxError> {
        self.raw.closed().await
    }

    /// Closes the connection.
    async fn shutdown(mut self) {
        self.stop_readers();
        self.raw.shutdown().await;
    }
}

/// Reads frames from `recv` and hands them on decoded, then [`Got::Ended`].
fn spawn_reader(
    mut recv: RecvStream,
    max_frame: u32,
) -> (mpsc::UnboundedReceiver<Got>, JoinHandle<()>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let reader = tokio::spawn(async move {
        while let Ok(frame) = HostilePeer::read_frame(&mut recv, max_frame).await {
            match decode_frame::<TxRelayMessage>(&frame) {
                Ok(message) => {
                    if tx.send(Got::Message(message)).is_err() {
                        return;
                    }
                }
                Err(_) => break,
            }
        }
        let _ = tx.send(Got::Ended);
    });
    (rx, reader)
}

/// The next message from a reader, within [`NET`].
async fn next(rx: &mut mpsc::UnboundedReceiver<Got>) -> Result<TxRelayMessage, BoxError> {
    match tokio::time::timeout(NET, rx.recv()).await {
        Ok(Some(Got::Message(message))) => Ok(message),
        Ok(Some(Got::Ended) | None) => Err("the stream ended".into()),
        Err(_) => Err("timed out waiting for a frame".into()),
    }
}

/// With the adapter running beside `tx-relay` on both nodes, a transaction
/// still moves on `tx-relay`, and the session is published as live, which
/// takes the peer's transactions off the adapter.
#[tokio::test]
async fn a_transaction_moves_with_the_adapter_on_both_nodes() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let tx = transaction();
    let id = tx.transaction.id();
    let a = RelayNode::spawn(
        81,
        Some(TestPool::new(Held {
            transactions: vec![tx],
            ..Held::default()
        })),
    )
    .await?;
    let b = RelayNode::spawn(82, Some(TestPool::new(Held::default()))).await?;
    a.node
        .connect_native(&b.node, Duration::from_secs(5))
        .await?;
    let pool_b = b.pool.as_ref().expect("B runs tx-relay");
    await_until(
        "B's pool receives A's transaction",
        Duration::from_secs(20),
        || pool_b.received().contains(&id),
    )
    .await?;
    let sessions = b.sessions.as_ref().expect("B runs tx-relay");
    assert!(sessions.is_live(&peer_id(&a).await?));
    a.node.shutdown().await;
    b.node.shutdown().await;
    Ok(())
}

/// One side without `tx-relay`: no session opens, the adapter keeps the
/// peer's transactions, and the connection stays up.
#[tokio::test]
async fn a_peer_without_tx_relay_keeps_the_adapter() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let a = RelayNode::spawn(83, Some(TestPool::new(Held::default()))).await?;
    let b = RelayNode::spawn(84, None).await?;
    a.node
        .connect_native(&b.node, Duration::from_secs(5))
        .await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let sessions = a.sessions.as_ref().expect("A runs tx-relay");
    assert!(!sessions.is_live(&peer_id(&b).await?));
    a.node.shutdown().await;
    b.node.shutdown().await;
    Ok(())
}

/// A setup the peer never completes, opening the data stream alone,
/// expires at the transport's setup deadline: no session opens and the
/// connection stays. A complete setup afterwards opens one.
#[tokio::test]
async fn a_session_setup_that_expires_opens_no_session() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let (victim, observed) =
        observed_node(101, TestPool::new(Held::default()), cools(), 16).await?;
    let sessions = victim.sessions.clone().expect("the node runs tx-relay");
    let mut peer = RawRelay::connect(&victim, 1_101).await?;

    peer.offer(1).await?;
    await_until("the half-open setup expires", NET, || {
        observed.expired() > 0
    })
    .await?;
    assert!(!sessions.is_live(&peer.id()));
    assert!(!peer.is_closed(), "an expired setup keeps the connection");

    // The transport defers the service's next offer by the setup deadline
    // after an expiry, so the complete offer waits that long.
    tokio::time::sleep(victim.node.limits().prelude_timeout).await;
    peer.start().await?;
    await_until("the complete setup is live", NET, || {
        sessions.is_live(&peer.id())
    })
    .await?;

    peer.shutdown().await;
    victim.node.shutdown().await;
    Ok(())
}

/// `tx-relay` opts into reopening. A session that ends while its connection
/// stays, here because the peer resets its data stream, leaves the live
/// record. The peer's next session on the same connection is live again
/// and is announced the pool afresh.
#[tokio::test]
async fn a_session_that_ends_and_comes_back_is_live_again() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let (handle, _inbox) = zakura_node_services::mempool::port::pool_port();
    assert!(
        tx_relay_service(handle, cools(), &Default::default(), Default::default())
            .service
            .session_policy()
            .reopen
    );

    let tx = transaction();
    let pool = TestPool::new(Held {
        transactions: vec![tx.clone()],
        ..Held::default()
    });
    let victim = RelayNode::spawn(102, Some(pool)).await?;
    let sessions = victim.sessions.clone().expect("the node runs tx-relay");
    let mut peer = RawRelay::connect(&victim, 1_102).await?;
    peer.start().await?;
    peer.announced(id(&tx)).await?;
    await_until("the first session is live", NET, || {
        sessions.is_live(&peer.id())
    })
    .await?;

    peer.reset()?;
    peer.state_ended().await?;
    await_until("the ended session leaves the record", NET, || {
        !sessions.is_live(&peer.id())
    })
    .await?;
    assert!(!peer.is_closed(), "ending a session keeps the connection");

    peer.start().await?;
    peer.announced(id(&tx)).await?;
    await_until("the session that came back is live", NET, || {
        sessions.is_live(&peer.id())
    })
    .await?;
    assert!(!peer.is_closed());

    peer.shutdown().await;
    victim.node.shutdown().await;
    Ok(())
}

/// A session that ends with our request unanswered can be reopened on the
/// same connection: the new session has its own streams, so the honest
/// peer's late answer on the old stream reaches nothing. It is not taken as
/// an answer nobody asked for, charges nothing, and leaves the new session
/// open.
#[tokio::test]
async fn a_late_answer_on_an_ended_sessions_stream_reaches_nothing() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let pool = TestPool::new(Held::default());
    let (victim, observed) = observed_node(143, pool, cools(), 16).await?;
    let pool = victim.pool.as_ref().expect("the node runs tx-relay");
    let sessions = victim.sessions.clone().expect("the node runs tx-relay");
    let tx = transaction();

    let mut peer = RawRelay::connect(&victim, 1_143).await?;
    peer.start().await?;
    peer.send(&TxRelayMessage::Inv(vec![entry(&tx)])).await?;
    peer.asked(id(&tx)).await?;

    // The peer ends the session with the request unanswered, keeping the
    // old data stream to answer on later.
    let mut old_data = peer.data.take().ok_or("the session is open")?;
    peer.state
        .take()
        .ok_or("the session is open")?
        .reset(0u32.into())?;
    peer.state_ended().await?;
    await_until("the session ends", NET, || !sessions.is_live(&peer.id())).await?;
    assert!(!peer.is_closed(), "ending a session keeps the connection");

    peer.stop_readers();
    peer.start().await?;
    await_until("the reopened session is live", NET, || {
        sessions.is_live(&peer.id())
    })
    .await?;

    // The late answer, on the ended session's stream. The node may have
    // stopped reading it, so the write itself may fail.
    let late = encode_frame(&answer(&tx))?.encode(peer.max_frame)?;
    let _ = tokio::time::timeout(NET, old_data.write_all(&late)).await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    assert!(!peer.is_closed(), "the late answer closes nothing");
    assert_eq!(observed.cause(&peer), None);
    assert!(
        !pool.received().contains(&id(&tx)),
        "the late body never reaches the pool"
    );
    assert!(sessions.is_live(&peer.id()), "the new session stays open");

    peer.shutdown().await;
    victim.node.shutdown().await;
    Ok(())
}

/// A peer that redials evicts its old connection, and the session on its
/// new connection takes over: it is live once the old one has ended, and a
/// transaction it announces is fetched from it and reaches the pool.
///
/// The transport refuses the redial that evicts the old connection, so
/// the replacement session only starts once the old connection is gone.
/// The order the case names, an old session ending after its replacement
/// started, cannot be reached on loopback: within a connection the
/// transport admits a new session only after the old one's workers end,
/// and across connections only after the old one is gone. The machine's
/// tests and `sessions.rs` cover that order.
#[tokio::test]
async fn a_redial_that_evicts_the_old_connection_takes_over_its_session() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let pool = TestPool::new(Held::default());
    let victim = RelayNode::spawn(103, Some(pool)).await?;
    let sessions = victim.sessions.clone().expect("the node runs tx-relay");
    let mut old = RawRelay::connect(&victim, 1_103).await?;
    old.start().await?;
    await_until("the old session is live", NET, || {
        sessions.is_live(&old.id())
    })
    .await?;

    // The node evicts a same-address duplicate only once the incumbent is
    // this old, so the redial waits for that.
    tokio::time::sleep(ZAKURA_SAME_IP_DUPLICATE_EVICT_MIN_AGE + Duration::from_millis(200)).await;
    let mut new = RawRelay::reconnect(&victim, 1_103).await?;
    old.closed().await?;
    await_until("the replacement is live", NET, || {
        sessions.is_live(&new.id())
    })
    .await?;

    let tx = transaction();
    new.send(&TxRelayMessage::Inv(vec![entry(&tx)])).await?;
    new.asked(id(&tx)).await?;
    new.send(&answer(&tx)).await?;
    let pool = victim.pool.as_ref().expect("the node runs tx-relay");
    await_until("the replacement's body reaches the pool", NET, || {
        pool.received().contains(&id(&tx))
    })
    .await?;
    assert!(sessions.is_live(&new.id()));

    new.shutdown().await;
    victim.node.shutdown().await;
    Ok(())
}

/// A connection that carries only discovery and `tx-relay` stays up while
/// the pool is still attaching and through a gap between sessions, since
/// `tx-relay` owns it for 10 seconds past its last session. A connection
/// with discovery alone is closed by discovery in the same time, which
/// marks when discovery has looked.
#[tokio::test]
async fn discovery_keeps_a_tx_relay_connection_while_the_pool_attaches_and_between_sessions(
) -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let pool = TestPool::detached(Held::default());
    let (victim, observed) = observed_node(104, pool, cools(), 16).await?;
    let sessions = victim.sessions.clone().expect("the node runs tx-relay");

    let mut peer = RawRelay::connect_with(
        &victim,
        1_104,
        ZAKURA_CAP_DISCOVERY | ZAKURA_CAP_TX_RELAY,
        None,
    )
    .await?;
    // A discovery session that never answers, so discovery's first look
    // comes at its initial exchange timeout.
    let _discovery = peer
        .raw
        .open_stream(ZAKURA_STREAM_DISCOVERY, ZAKURA_DISCOVERY_STREAM_VERSION)
        .await?;
    peer.start().await?;
    assert!(
        !sessions.is_live(&peer.id()),
        "a session is not live before the pool attaches"
    );
    // The session ends, so discovery looks during the reopen gap.
    peer.reset()?;
    peer.state_ended().await?;

    let control = RawRelay::connect_with(&victim, 1_204, ZAKURA_CAP_DISCOVERY, None).await?;
    let _control_discovery = control
        .raw
        .open_stream(ZAKURA_STREAM_DISCOVERY, ZAKURA_DISCOVERY_STREAM_VERSION)
        .await?;
    // The control's exchange times out. Discovery owns the connection alone,
    // so it closes it.
    control.closed().await?;
    assert_eq!(
        observed.cause(&control).map(CloseOutcome::label),
        Some("discovery_exchange_timed_out")
    );
    // The control connected after the peer, so the peer's look came first;
    // a short bounded window covers scheduling.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!peer.is_closed(), "tx-relay owns the connection in its gap");

    peer.start().await?;
    victim
        .pool
        .as_ref()
        .expect("the node runs tx-relay")
        .attach();
    await_until("the session is live once the pool attaches", NET, || {
        sessions.is_live(&peer.id())
    })
    .await?;
    assert!(!peer.is_closed());

    peer.shutdown().await;
    control.shutdown().await;
    victim.node.shutdown().await;
    Ok(())
}

/// A peer that restarts on a build without `tx-relay` leaves the live
/// record when it goes. Its new connection opens no session, and the
/// adapter carries the transactions it advertises.
///
/// The restarted build is a raw peer with the same identity that speaks
/// only legacy gossip, so the case can make it advertise a transaction.
#[tokio::test]
async fn a_peer_that_restarts_without_tx_relay_falls_back_to_the_adapter() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let mut a = RelayNode::spawn(105, Some(TestPool::new(Held::default()))).await?;
    let b = RelayNode::spawn(106, Some(TestPool::new(Held::default()))).await?;
    let sessions = a.sessions.clone().expect("A runs tx-relay");
    let b_id = peer_id(&b).await?;
    a.node.connect_native(&b.node, NET).await?;
    await_until("B is live at A", NET, || sessions.is_live(&b_id)).await?;

    b.node.shutdown().await;
    drop(b);
    await_until("B leaves A's record", NET, || !sessions.is_live(&b_id)).await?;
    let supervisor = a.node.supervisor();
    tokio::time::timeout(NET, async {
        while supervisor.registered_ids().await.contains(&b_id) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .map_err(|_| "B's old connection stays registered")?;

    let restarted = HostilePeer::connect_native(&a.node, 106).await?;
    assert_eq!(restarted.id()?, b_id);
    let tx = transaction();
    restarted
        .send_raw_frame(
            ZAKURA_STREAM_GOSSIP,
            LegacyGossipFrame::AdvertiseTransactionIds(vec![id(&tx)]).encode_frame()?,
        )
        .await?;
    let advertised = tokio::time::timeout(NET, async {
        while let Some(request) = a.inbound.recv().await {
            if let Request::AdvertiseTransactionIds(ids, _) = request {
                return ids.contains(&id(&tx));
            }
        }
        false
    })
    .await
    .map_err(|_| "the adapter never handed on the advert")?;
    assert!(
        advertised,
        "the adapter carries the restarted peer's advert"
    );
    assert!(!sessions.is_live(&b_id));

    restarted.shutdown().await;
    a.node.shutdown().await;
    Ok(())
}

/// A peer that connects before the pool attaches keeps the adapter until
/// it does: its session is not live, and nothing is fetched from it. Once
/// the pool attaches, the session goes live and the peer's transaction
/// moves on `tx-relay`.
#[tokio::test]
async fn a_peer_that_connects_before_the_pool_attaches_goes_live_when_it_does(
) -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let tx = transaction();
    let a = RelayNode::spawn(107, Some(TestPool::detached(Held::default()))).await?;
    let b = RelayNode::spawn(
        108,
        Some(TestPool::new(Held {
            transactions: vec![tx.clone()],
            ..Held::default()
        })),
    )
    .await?;
    let sessions = a.sessions.clone().expect("A runs tx-relay");
    let pool = a.pool.as_ref().expect("A runs tx-relay");
    let b_id = peer_id(&b).await?;
    b.node.connect_native(&a.node, NET).await?;

    // Past B's opening announcement, with slack.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        !sessions.is_live(&b_id),
        "not live before the pool attaches"
    );
    assert!(pool.received().is_empty(), "nothing fetched before it");

    pool.attach();
    await_until("B is live once A's pool attaches", NET, || {
        sessions.is_live(&b_id)
    })
    .await?;
    await_until("B's transaction reaches A's pool", NET, || {
        pool.received().contains(&id(&tx))
    })
    .await?;

    a.node.shutdown().await;
    b.node.shutdown().await;
    Ok(())
}

/// Bodies a node serves while its data stream to a peer is held back.
const BLOCKED_BODY: usize = 1_000_000;

/// A node holding `count` large bodies, and a peer that requested
/// `requests` of them and does not read its data stream, whose window holds
/// a fraction of one body. The node's data writer is stuck on the first
/// body, the transport's queue for the stream holds the next few, and the
/// rest of the serve jobs wait on the writer. The peer reads its state
/// stream.
async fn blocked_uploads(
    seed: u64,
    count: usize,
    requests: usize,
) -> Result<(RelayNode, Arc<Observed>, RawRelay, Vec<VerifiedUnminedTx>), BoxError> {
    let bodies: Vec<VerifiedUnminedTx> = (0..count)
        .map(|n| large(u8::try_from(n).expect("few bodies"), BLOCKED_BODY))
        .collect();
    let pool = TestPool::new(Held {
        transactions: bodies.clone(),
        ..Held::default()
    });
    let (victim, observed) = observed_node(seed, pool, cools(), 16).await?;
    let quic = narrow(&victim, 64 * 1024);
    let mut peer =
        RawRelay::connect_with(&victim, seed + 1_000, ZAKURA_CAP_TX_RELAY, Some(quic)).await?;
    peer.open().await?;
    peer.read_state();
    peer.hello().await?;
    for body in bodies.iter().take(requests) {
        peer.send(&TxRelayMessage::GetTxs(id(body))).await?;
    }
    Ok((victim, observed, peer, bodies))
}

/// Wants are not blocked: while a peer pulls large bodies from this node
/// and reads none of them, this node's own request to that peer goes out,
/// and the peer's answer reaches the pool.
#[tokio::test]
async fn this_nodes_wants_flow_while_a_peer_pulls_large_bodies() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let (victim, _observed, mut peer, bodies) =
        blocked_uploads(110, MAX_SERVES, MAX_SERVES).await?;

    let wanted = transaction();
    peer.send(&TxRelayMessage::Inv(vec![entry(&wanted)]))
        .await?;
    peer.asked(id(&wanted)).await?;
    peer.send(&answer(&wanted)).await?;
    let pool = victim.pool.as_ref().expect("the node runs tx-relay");
    await_until("the wanted body reaches the pool", NET, || {
        pool.received().contains(&id(&wanted))
    })
    .await?;
    assert!(!peer.is_closed());

    // The serve jobs waited, and every one is answered once the peer reads.
    peer.read_data();
    let mut served = 0;
    while served < bodies.len() {
        if let TxRelayMessage::Tx { .. } = peer.next_data().await? {
            served += 1;
        }
    }

    peer.shutdown().await;
    victim.node.shutdown().await;
    Ok(())
}

/// Answers on two streams: with the data stream to a peer held back, this
/// node's `NotFound` answers to that peer and its own requests to it still
/// arrive on the state stream.
#[tokio::test]
async fn not_found_answers_and_wants_flow_while_the_data_stream_is_held_back(
) -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let unanswerable = 4;
    let (victim, _observed, mut peer, bodies) =
        blocked_uploads(112, MAX_SERVES - unanswerable, MAX_SERVES - unanswerable).await?;

    let mut unanswered: Vec<UnminedTxId> = (0..unanswerable)
        .map(|n| unknown(u32::try_from(n).expect("few ids")))
        .collect();
    for id in &unanswered {
        peer.send(&TxRelayMessage::GetTxs(*id)).await?;
    }
    // Each `NotFound` answers one of the unknown ids, not a held body, whose
    // serve job waits on the data stream.
    while !unanswered.is_empty() {
        match peer.next_state().await? {
            TxRelayMessage::NotFound(id) if unanswered.contains(&id) => {
                unanswered.retain(|left| *left != id);
            }
            other => {
                return Err(format!("expected a NotFound of an unknown id, got {other:?}").into())
            }
        }
    }

    let wanted = transaction();
    peer.send(&TxRelayMessage::Inv(vec![entry(&wanted)]))
        .await?;
    peer.asked(id(&wanted)).await?;
    assert!(!peer.is_closed());

    // The held bodies' serve jobs waited behind the data stream all along:
    // once the peer reads it, every one is answered with its body.
    peer.read_data();
    let mut held: Vec<UnminedTxId> = bodies
        .iter()
        .take(MAX_SERVES - unanswerable)
        .map(id)
        .collect();
    while !held.is_empty() {
        if let TxRelayMessage::Tx { id, .. } = peer.next_data().await? {
            assert!(held.contains(&id), "{id} answers a held request once");
            held.retain(|left| *left != id);
        }
    }

    peer.shutdown().await;
    victim.node.shutdown().await;
    Ok(())
}

/// The slowest link a request's deadline allows for.
const SLOW_RATE: usize = 100_000;

/// `count` `Inv` frames of 1,024 entries each, all naming `held`, which the
/// node already holds and so never requests.
fn inv_backlog(held: &VerifiedUnminedTx, count: usize) -> Vec<TxRelayMessage> {
    (0..count)
        .map(|_| TxRelayMessage::Inv(vec![entry(held); 1_024]))
        .collect()
}

/// Slow but honest: five bodies from one peer over a link of 100 KB/s, the
/// slowest a deadline allows for, answered in reverse order behind a
/// backlog of `Inv` frames, are all accepted, with no charge.
///
/// The link is simulated by the peer pacing its own data stream writes at
/// that rate. The bodies are 200 KB, so the case takes about 15 seconds.
/// That is still longer than a request's fixed 8 seconds, so only the part
/// of the deadline that grows with the bytes outstanding lets the last
/// bodies through. The backlog stays within the sender allowance, which the
/// deadline also covers.
#[tokio::test]
async fn a_slow_honest_peer_has_every_body_accepted() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let script = 200_000;
    let held = transaction();
    let pool = TestPool::new(Held {
        transactions: vec![held.clone()],
        ..Held::default()
    });
    let victim = RelayNode::spawn(114, Some(pool)).await?;
    let mut peer = RawRelay::connect(&victim, 1_114).await?;
    peer.start().await?;

    let bodies: Vec<VerifiedUnminedTx> = (0..5).map(|n| large(n, script)).collect();
    peer.send(&TxRelayMessage::Inv(bodies.iter().map(entry).collect()))
        .await?;
    peer.asked_all(&bodies.iter().map(id).collect::<Vec<_>>())
        .await?;
    // Seven frames of about 70 KB, under the 512 KiB sender allowance.
    let mut paced = inv_backlog(&held, 7);
    paced.extend(bodies.iter().rev().map(answer));
    peer.send_paced(&paced, SLOW_RATE).await?;

    let pool = victim.pool.as_ref().expect("the node runs tx-relay");
    let wait = NET.max(std::time::Duration::from_secs(
        u64::try_from(script * 5 / SLOW_RATE)? * 2,
    ));
    await_until("every slow body reaches the pool", wait, || {
        let received = pool.received();
        bodies.iter().all(|body| received.contains(&id(body)))
    })
    .await?;
    assert!(!peer.is_closed(), "a slow honest peer is not closed");

    peer.shutdown().await;
    victim.node.shutdown().await;
    Ok(())
}

/// Slow but honest: a sole announcer on a link of half the slowest rate,
/// whose answer sits behind more `Inv` bytes than the deadline covers,
/// misses its deadline. Its late body is dropped unread and nothing is
/// charged, and when it announces the id again it is asked again and its
/// body is accepted.
///
/// The link is simulated by the peer pacing its data stream writes at
/// 50 KB/s.
#[tokio::test]
async fn a_sole_announcer_behind_a_deep_backlog_is_asked_again_without_charge(
) -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let held = transaction();
    let pool = TestPool::new(Held {
        transactions: vec![held.clone()],
        ..Held::default()
    });
    let victim = RelayNode::spawn(116, Some(pool)).await?;
    let mut peer = RawRelay::connect(&victim, 1_116).await?;
    peer.start().await?;

    let body = large(9, 10_000);
    peer.send(&TxRelayMessage::Inv(vec![entry(&body)])).await?;
    peer.asked(id(&body)).await?;
    // About 770 KB at 50 KB/s, so the body arrives about 15.5 seconds after
    // the ask: past its 13.3 second deadline, before the second one.
    let mut paced = inv_backlog(&held, 11);
    paced.push(answer(&body));
    paced.push(TxRelayMessage::Inv(vec![entry(&body)]));
    peer.send_paced(&paced, SLOW_RATE / 2).await?;

    peer.asked(id(&body)).await?;
    peer.send(&answer(&body)).await?;
    let pool = victim.pool.as_ref().expect("the node runs tx-relay");
    await_until("the body asked again reaches the pool", NET, || {
        pool.received().contains(&id(&body))
    })
    .await?;
    assert_eq!(
        pool.received()
            .iter()
            .filter(|received| **received == id(&body))
            .count(),
        1,
        "the late body never reached the pool"
    );
    assert!(!peer.is_closed(), "a late body is not charged");

    peer.shutdown().await;
    victim.node.shutdown().await;
    Ok(())
}

/// Unread `NotFound`s, part one: a peer that stops reading holds at most
/// the 16 requests our `Hello` serves, each counted until its answer is
/// queued for the transport. With its data stream held back, the transport
/// takes only the first few bodies into its own queue, which answers their
/// requests. How many depends on timing, so the peer keeps asking for pool
/// bodies, whose answers wait behind the others, up to far more than those
/// queues hold, and is closed for passing the limit. An id nobody has would
/// not do: its `NotFound` goes out on the state stream at once, which the
/// peer reads, so it never stays unanswered.
///
/// The exact 17th request is checked at the actor in `service/tests.rs`,
/// where nothing takes an answer.
#[tokio::test]
async fn requests_past_our_hello_with_their_answers_waiting_close_the_peer() -> Result<(), BoxError>
{
    let _guard = zakura_test::init();
    let (victim, observed, mut peer, bodies) =
        blocked_uploads(132, 3 * MAX_SERVES, MAX_SERVES).await?;
    for body in &bodies[MAX_SERVES..] {
        // A write after the close fails, which ends the requests.
        if peer.send(&TxRelayMessage::GetTxs(id(body))).await.is_err() {
            break;
        }
    }
    peer.closed().await?;
    assert_eq!(
        observed.cause(&peer).map(CloseOutcome::label),
        Some("tx-relay: too many requests")
    );

    victim.node.shutdown().await;
    Ok(())
}

/// Unread `NotFound`s, part two: a peer that stops reading its state stream
/// and keeps asking for ids nobody has is answered `NotFound` at once, which
/// frees each request's place, so it never breaks our `Hello`'s limit. The
/// answers fill the state writer's queue instead. That is the peer's first
/// stall, so its session is parked: it ends, and the connection stays.
#[tokio::test]
async fn a_peer_that_never_reads_its_not_founds_is_parked() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let (victim, observed) =
        observed_node(134, TestPool::new(Held::default()), cools(), 16).await?;
    let sessions = victim.sessions.clone().expect("the node runs tx-relay");
    let mut peer = flood_unread(&victim, 1_134).await?;
    peer.data_ended(NET).await?;
    assert!(!peer.is_closed(), "the connection stays");
    assert_eq!(observed.cause(&peer), None);
    assert!(!sessions.is_live(&peer.id()));

    victim.node.shutdown().await;
    Ok(())
}

/// A peer with the smallest stream windows QUIC allows, which never reads
/// its state stream, has sent its `Hello` and asks for ids nobody has until
/// the node closes it.
async fn flood_unread(victim: &RelayNode, seed: u64) -> Result<RawRelay, BoxError> {
    let quic = narrow(victim, 64 * 1024);
    let mut peer = RawRelay::connect_with(victim, seed, ZAKURA_CAP_TX_RELAY, Some(quic)).await?;
    peer.open().await?;
    peer.read_data();
    peer.send(&TxRelayMessage::Hello {
        serves: 16,
        max_tx_bytes: MAX_TX_BYTES,
    })
    .await?;
    // Far more answers than the queues between the node and the peer hold.
    for n in 0..2_000 {
        // A write after the close fails, which ends the requests.
        if peer
            .send(&TxRelayMessage::GetTxs(unknown(n)))
            .await
            .is_err()
        {
            break;
        }
    }
    Ok(peer)
}

/// A stall costs a peer its session, never its connection or a charge. Three
/// peers stall for the first time: one past the transport's write deadline,
/// one by filling a writer queue, and one by leaving a request unanswered
/// for a second deadline. So their sessions are parked: they end, their
/// connections stay, and nothing is recorded as a close. The first one's
/// next session is refused at once. The machine's scenario follows a park
/// to its readmission.
#[tokio::test]
async fn a_stall_costs_the_session_not_the_connection() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let body = large(1, BLOCKED_BODY);
    let pool = TestPool::new(Held {
        transactions: vec![body.clone()],
        ..Held::default()
    });
    let (victim, observed) = observed_node(136, pool, cools(), 16).await?;
    let sessions = victim.sessions.clone().expect("the node runs tx-relay");
    let [silent_tx]: [VerifiedUnminedTx; 1] = vectors(1)
        .try_into()
        .expect("vectors returns what it is asked for");

    // The node lets one handshake run at a time from an address, so the
    // peers connect in turn.
    let flooded = flood_unread(&victim, 1_137).await?;
    let quic = narrow(&victim, 64 * 1024);
    let stalled = RawRelay::connect_with(&victim, 1_136, ZAKURA_CAP_TX_RELAY, Some(quic)).await?;
    let silent = RawRelay::connect(&victim, 1_138).await?;

    let stalled = async {
        // It asks for a large body and reads none of it, so the node's
        // write stalls until the transport's write deadline retires the
        // session.
        let mut peer = stalled;
        peer.open().await?;
        peer.read_state();
        peer.hello().await?;
        peer.send(&TxRelayMessage::GetTxs(id(&body))).await?;
        peer.state_ended().await?;
        assert!(!peer.is_closed(), "the write deadline retires the session");
        assert_eq!(observed.cause(&peer), None);
        await_until("the stalled session ends", NET, || {
            !sessions.is_live(&peer.id())
        })
        .await?;
        // The session ran its stall step before it left the live record,
        // so the park stands. Once the transport saw the session exit, it
        // takes the next one, which `tx-relay` ends as it opens.
        await_until("the transport sees the session exit", NET, || {
            observed.exited(&peer) >= 1
        })
        .await?;
        peer.open().await?;
        peer.read_data();
        peer.data_ended(NET).await?;
        assert!(!peer.is_closed(), "a parked session leaves the connection");
        assert_eq!(observed.cause(&peer), None);
        peer.shutdown().await;
        Ok::<(), BoxError>(())
    };

    let flooding = async {
        let mut peer = flooded;
        peer.data_ended(NET).await?;
        assert!(!peer.is_closed(), "the flooding peer keeps its connection");
        assert_eq!(observed.cause(&peer), None);
        peer.shutdown().await;
        Ok::<(), BoxError>(())
    };

    let silent = async {
        let mut peer = silent;
        peer.start().await?;
        peer.send(&TxRelayMessage::Inv(vec![entry(&silent_tx)]))
            .await?;
        peer.asked(id(&silent_tx)).await?;
        // Two deadlines of about 13 seconds each.
        peer.data_ended(Duration::from_secs(60)).await?;
        assert!(!peer.is_closed(), "the silent peer keeps its connection");
        assert_eq!(observed.cause(&peer), None);
        peer.shutdown().await;
        Ok::<(), BoxError>(())
    };

    tokio::try_join!(stalled, flooding, silent)?;

    victim.node.shutdown().await;
    Ok(())
}

/// Idle announce: with the peer idle, a transaction the pool admits is
/// announced with no other traffic.
#[tokio::test]
async fn an_idle_peer_hears_of_an_admission() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let pool = TestPool::new(Held::default());
    let victim = RelayNode::spawn(118, Some(pool)).await?;
    let pool = victim.pool.as_ref().expect("the node runs tx-relay");
    let mut peer = RawRelay::connect(&victim, 1_118).await?;
    peer.start().await?;

    let tx = transaction();
    admit(pool, &tx);
    peer.announced(id(&tx)).await?;

    peer.shutdown().await;
    victim.node.shutdown().await;
    Ok(())
}

/// Fan-out: with 40 peers, a small transaction goes at once to the 16
/// sessions of its window and to the other 24 after the advert delay.
///
/// The node has no round-trip times yet, so its ranking is session order
/// with a random session in every fourth place. The peers connect one at a
/// time, so the first twelve to connect always fill the window's places
/// that are not random.
#[tokio::test]
async fn a_small_transaction_reaches_the_window_at_once_and_the_rest_after_the_delay(
) -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    const PEERS: usize = 40;
    let (victim, _observed) =
        observed_node(120, TestPool::new(Held::default()), cools(), 64).await?;
    let mut peers = Vec::with_capacity(PEERS);
    for n in 0..PEERS {
        let mut peer = RawRelay::connect(&victim, 1_200 + u64::try_from(n)?).await?;
        peer.start().await?;
        peers.push(peer);
    }

    let tx = transaction();
    assert!(
        size(&tx) <= SMALL_TX_BYTES,
        "a transaction of the widest window"
    );
    let admitted = Instant::now();
    admit(victim.pool.as_ref().expect("the node runs tx-relay"), &tx);
    let waits = peers.iter_mut().map(|peer| async {
        peer.announced(id(&tx)).await?;
        Ok::<Duration, BoxError>(admitted.elapsed())
    });
    let heard = futures::future::try_join_all(waits).await?;

    let at_once: Vec<usize> = (0..PEERS)
        .filter(|n| heard[*n] < ADVERT_DELAY / 2)
        .collect();
    assert_eq!(at_once.len(), WINDOW_SMALL, "heard: {heard:?}");
    assert!(
        (0..12).all(|n| at_once.contains(&n)),
        "the window's places in session order: {at_once:?}"
    );
    for (n, wait) in heard.iter().enumerate() {
        if !at_once.contains(&n) {
            assert!(
                *wait >= ADVERT_DELAY - Duration::from_millis(100),
                "peer {n} heard after {wait:?}"
            );
        }
    }

    for peer in peers {
        peer.shutdown().await;
    }
    victim.node.shutdown().await;
    Ok(())
}

/// Waits until `victim`'s pool is handed `tx`.
async fn reaches(victim: &RelayNode, tx: &VerifiedUnminedTx) -> Result<(), BoxError> {
    let pool = victim.pool.as_ref().expect("the node runs tx-relay");
    await_until("the body reaches the pool", NET, || {
        pool.received().contains(&id(tx))
    })
    .await?;
    Ok(())
}

/// A peer announces `tx`, is asked for it and answers with its body.
async fn supply(peer: &mut RawRelay, tx: &VerifiedUnminedTx) -> Result<(), BoxError> {
    peer.send(&TxRelayMessage::Inv(vec![entry(tx)])).await?;
    peer.asked(id(tx)).await?;
    peer.send(&answer(tx)).await
}

/// Charges: a `Cooldown` verdict cools the peer that supplied the body and
/// not the one that only announced it, and the cooldown holds across the
/// supplier's reconnect.
#[tokio::test]
async fn a_cooldown_cools_the_supplier_not_the_announcer_across_a_reconnect() -> Result<(), BoxError>
{
    let _guard = zakura_test::init();
    let pool = TestPool::new(Held {
        verdict: rejected(Charge::Cooldown, Height(0)),
        ..Held::default()
    });
    let victim = RelayNode::spawn(122, Some(pool)).await?;
    let pool = victim.pool.as_ref().expect("the node runs tx-relay");
    let sessions = victim.sessions.clone().expect("the node runs tx-relay");
    let mut announcer = RawRelay::connect(&victim, 1_122).await?;
    announcer.start().await?;
    let mut supplier = RawRelay::connect(&victim, 1_123).await?;
    supplier.start().await?;
    let [bad, later, after]: [VerifiedUnminedTx; 3] = vectors(3)
        .try_into()
        .expect("vectors returns what it is asked for");

    // The announcer is asked first and has nothing, so the supplier is
    // asked next and supplies the body.
    announcer
        .send(&TxRelayMessage::Inv(vec![entry(&bad)]))
        .await?;
    announcer.asked(id(&bad)).await?;
    supplier
        .send(&TxRelayMessage::Inv(vec![entry(&bad)]))
        .await?;
    announcer.send(&TxRelayMessage::NotFound(id(&bad))).await?;
    supplier.asked(id(&bad)).await?;
    supplier.send(&answer(&bad)).await?;
    await_until("the bad body reaches the pool", NET, || {
        pool.received().contains(&id(&bad))
    })
    .await?;

    // The supplier cools: its announcement is not fetched. The announcer
    // does not.
    supplier
        .send(&TxRelayMessage::Inv(vec![entry(&later)]))
        .await?;
    supplier.not_asked(id(&later)).await?;
    announcer
        .send(&TxRelayMessage::Inv(vec![entry(&later)]))
        .await?;
    announcer.asked(id(&later)).await?;

    // The cooldown holds across a reconnect.
    let supplier_id = supplier.id();
    supplier.shutdown().await;
    await_until("the supplier's session ends", NET, || {
        !sessions.is_live(&supplier_id)
    })
    .await?;
    let mut supplier = RawRelay::reconnect(&victim, 1_123).await?;
    supplier
        .send(&TxRelayMessage::Inv(vec![entry(&after)]))
        .await?;
    supplier.not_asked(id(&after)).await?;

    announcer.shutdown().await;
    supplier.shutdown().await;
    victim.node.shutdown().await;
    Ok(())
}

/// Charges: a cooldown needs the node to be current at the verdict's tip,
/// which `may_cool` answers. A `Cooldown` verdict decided at another tip
/// starts none, and one decided at the node's tip does.
#[tokio::test]
async fn a_cooldown_waits_for_a_current_node_at_the_verdicts_tip() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let tip = Height(5);
    let pool = TestPool::new(Held {
        verdict: rejected(Charge::Cooldown, Height(4)),
        ..Held::default()
    });
    let victim = RelayNode::spawn_cooling(124, Some(pool), Arc::new(move |at| at == tip)).await?;
    let pool = victim.pool.as_ref().expect("the node runs tx-relay");
    let mut peer = RawRelay::connect(&victim, 1_124).await?;
    peer.start().await?;
    let [stale, current, after]: [VerifiedUnminedTx; 3] = vectors(3)
        .try_into()
        .expect("vectors returns what it is asked for");

    supply(&mut peer, &stale).await?;
    await_until("the stale verdict's body reaches the pool", NET, || {
        pool.received().contains(&id(&stale))
    })
    .await?;
    pool.change(
        |held| held.verdict = rejected(Charge::Cooldown, tip),
        Vec::new(),
    );
    // Not cooling, so the next one is fetched.
    supply(&mut peer, &current).await?;
    await_until("the current verdict's body reaches the pool", NET, || {
        pool.received().contains(&id(&current))
    })
    .await?;

    peer.send(&TxRelayMessage::Inv(vec![entry(&after)])).await?;
    peer.not_asked(id(&after)).await?;

    peer.shutdown().await;
    victim.node.shutdown().await;
    Ok(())
}

/// Charges: a `Ban` verdict closes the supplier's connection, also when it
/// arrives after the supplier's session was replaced by one on a new
/// connection.
#[tokio::test]
async fn a_ban_closes_the_supplier_even_after_its_session_was_replaced() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let pool = TestPool::new(Held {
        verdict: rejected(Charge::Ban, Height(0)),
        ..Held::default()
    });
    let (victim, observed) = observed_node(126, pool, cools(), 16).await?;
    let pool = victim.pool.as_ref().expect("the node runs tx-relay");
    let sessions = victim.sessions.clone().expect("the node runs tx-relay");
    let [banned, later]: [VerifiedUnminedTx; 2] = vectors(2)
        .try_into()
        .expect("vectors returns what it is asked for");

    let mut peer = RawRelay::connect(&victim, 1_126).await?;
    peer.start().await?;
    supply(&mut peer, &banned).await?;
    peer.closed().await?;
    assert_eq!(
        observed.cause(&peer).map(CloseOutcome::label),
        Some("tx-relay: banned transaction")
    );

    // The verdict waits while the supplier's session is replaced.
    pool.change(|held| held.slow = true, Vec::new());
    let mut peer = RawRelay::reconnect(&victim, 1_127).await?;
    supply(&mut peer, &later).await?;
    await_until("the body reaches the slow pool", NET, || {
        pool.received().contains(&id(&later))
    })
    .await?;
    let peer_id = peer.id();
    peer.shutdown().await;
    await_until("the supplier's session ends", NET, || {
        !sessions.is_live(&peer_id)
    })
    .await?;
    let replacement = RawRelay::reconnect(&victim, 1_127).await?;
    pool.change(|held| held.slow = false, Vec::new());
    replacement.closed().await?;
    assert_eq!(
        observed.cause(&replacement).map(CloseOutcome::label),
        Some("tx-relay: banned transaction")
    );

    victim.node.shutdown().await;
    Ok(())
}

/// Charges: a wire violation closes the connection. One peer sends a
/// second `Hello`, another a body of another size than it announced.
#[tokio::test]
async fn a_wire_violation_closes_the_connection() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let (victim, observed) =
        observed_node(128, TestPool::new(Held::default()), cools(), 16).await?;

    let mut twice = RawRelay::connect(&victim, 1_128).await?;
    twice.start().await?;
    twice
        .send(&TxRelayMessage::Hello {
            serves: 16,
            max_tx_bytes: MAX_TX_BYTES,
        })
        .await?;
    twice.closed().await?;
    assert_eq!(observed.cause(&twice), Some(fault(CloseRow::RELAY_CLOSE_4)));

    let tx = transaction();
    let mut resized = RawRelay::connect(&victim, 1_129).await?;
    resized.start().await?;
    resized
        .send(&TxRelayMessage::Inv(vec![(id(&tx), size(&tx) + 1)]))
        .await?;
    resized.asked(id(&tx)).await?;
    resized.send(&answer(&tx)).await?;
    resized.closed().await?;
    assert_eq!(
        observed.cause(&resized),
        Some(fault(CloseRow::RELAY_CLOSE_10))
    );

    victim.node.shutdown().await;
    Ok(())
}

/// Charges: a body that isn't exactly one transaction's encoding closes the
/// connection and never reaches the pool. One peer sends bytes that don't
/// decode, another a transaction with a byte after it.
#[tokio::test]
async fn an_undecodable_body_closes_the_connection() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let (victim, observed) =
        observed_node(145, TestPool::new(Held::default()), cools(), 16).await?;
    let pool = victim.pool.as_ref().expect("the node runs tx-relay");
    let undecodable = fault(CloseRow::RELAY_CLOSE_12);

    let garbage = unknown(1);
    let mut peer = RawRelay::connect(&victim, 1_145).await?;
    peer.start().await?;
    peer.send(&TxRelayMessage::Inv(vec![(garbage, 200)]))
        .await?;
    peer.asked(garbage).await?;
    peer.send(&TxRelayMessage::Tx {
        id: garbage,
        body: vec![0xff; 200],
    })
    .await?;
    peer.closed().await?;
    assert_eq!(observed.cause(&peer), Some(undecodable));

    let tx = transaction();
    let mut body = tx
        .transaction
        .transaction()
        .zcash_serialize_to_vec()
        .expect("a test transaction serializes");
    body.push(0);
    let mut trailing = RawRelay::connect(&victim, 1_146).await?;
    trailing.start().await?;
    trailing
        .send(&TxRelayMessage::Inv(vec![(id(&tx), size(&tx) + 1)]))
        .await?;
    trailing.asked(id(&tx)).await?;
    trailing
        .send(&TxRelayMessage::Tx { id: id(&tx), body })
        .await?;
    trailing.closed().await?;
    assert_eq!(observed.cause(&trailing), Some(undecodable));
    assert!(pool.received().is_empty(), "neither body reaches the pool");

    victim.node.shutdown().await;
    Ok(())
}

/// Charges: an answer the node didn't ask for closes the connection, and
/// its body never reaches the pool. One peer answers a request with a `Tx`
/// and then a `NotFound`, another sends the same `Tx` twice, and a third
/// sends a large body within the size limit that nobody asked for.
#[tokio::test]
async fn an_answer_nobody_asked_for_closes_the_connection() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    // Verification is held, so an answered request stays answered: an
    // admission the stand-in never publishes would leave the entry open and
    // ask the peer again, and that ask would make a later `NotFound` fair.
    let pool = TestPool::new(Held {
        slow: true,
        ..Held::default()
    });
    let (victim, observed) = observed_node(131, pool, cools(), 16).await?;
    let pool = victim.pool.as_ref().expect("the node runs tx-relay");
    let txs = vectors(2);
    let count = |tx: &VerifiedUnminedTx| {
        pool.received()
            .iter()
            .filter(|received| **received == id(tx))
            .count()
    };

    // A `Tx`, then a `NotFound` for the same request.
    let mut both = RawRelay::connect(&victim, 1_131).await?;
    both.start().await?;
    both.send(&TxRelayMessage::Inv(vec![entry(&txs[0])]))
        .await?;
    both.asked(id(&txs[0])).await?;
    both.send(&answer(&txs[0])).await?;
    // The two answers travel on different streams, so the body is let land
    // first: a `NotFound` that overtook it would make the body the extra one.
    reaches(&victim, &txs[0]).await?;
    both.send(&TxRelayMessage::NotFound(id(&txs[0]))).await?;
    both.closed().await?;
    assert_eq!(observed.cause(&both), Some(fault(CloseRow::RELAY_CLOSE_9)));
    assert_eq!(
        count(&txs[0]),
        1,
        "the answer asked for reaches the pool once"
    );

    // The same `Tx` twice.
    let mut twice = RawRelay::connect(&victim, 1_132).await?;
    twice.start().await?;
    twice
        .send(&TxRelayMessage::Inv(vec![entry(&txs[1])]))
        .await?;
    twice.asked(id(&txs[1])).await?;
    twice.send(&answer(&txs[1])).await?;
    twice.send(&answer(&txs[1])).await?;
    twice.closed().await?;
    assert_eq!(observed.cause(&twice), Some(fault(CloseRow::RELAY_CLOSE_9)));
    assert_eq!(
        count(&txs[1]),
        1,
        "the repeated body never reaches the pool"
    );

    // A large body nobody asked for.
    let unasked = large(3, 1_500_000);
    let mut pushy = RawRelay::connect(&victim, 1_133).await?;
    pushy.start().await?;
    pushy.send(&answer(&unasked)).await?;
    pushy.closed().await?;
    assert_eq!(observed.cause(&pushy), Some(fault(CloseRow::RELAY_CLOSE_9)));
    assert_eq!(
        count(&unasked),
        0,
        "a body nobody asked for never reaches the pool"
    );

    victim.node.shutdown().await;
    Ok(())
}

/// Two large responses arriving at once from one peer while the pool is
/// slow to verify. tx-relay holds no decoded body: each goes to the core in
/// the step that decodes it, so while neither has a verdict both are in the
/// core's hands and the core holds the only reference to each, and the node
/// keeps answering the peer meanwhile.
///
/// A full core queue, where `submit_now` drops the body with `NoRoom`,
/// needs 1,024 queued commands and is left to the port's own tests.
#[tokio::test]
async fn two_large_bodies_at_once_while_the_pool_is_slow_are_both_handed_over(
) -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let pool = TestPool::new(Held {
        slow: true,
        ..Held::default()
    });
    let victim = RelayNode::spawn(130, Some(pool)).await?;
    let pool = victim.pool.as_ref().expect("the node runs tx-relay");
    let mut peer = RawRelay::connect(&victim, 1_130).await?;
    peer.start().await?;

    let bodies = [large(1, 1_500_000), large(2, 1_500_000)];
    peer.send(&TxRelayMessage::Inv(bodies.iter().map(entry).collect()))
        .await?;
    peer.asked_all(&bodies.iter().map(id).collect::<Vec<_>>())
        .await?;
    let data = peer.data.as_mut().expect("the session is open");
    let mut both = Vec::new();
    for body in &bodies {
        both.extend(encode_frame(&answer(body))?.encode(peer.max_frame)?);
    }
    data.write_all(&both).await?;

    await_until("both bodies are in the core's hands", NET, || {
        let received = pool.received();
        bodies.iter().all(|body| received.contains(&id(body)))
    })
    .await?;
    // The count of decoded bodies outside the core, against the plan's bound
    // of one per session: tx-relay keeps none.
    let holders: Vec<usize> = pool
        .held
        .lock()
        .expect("the test never poisons the lock")
        .bodies
        .iter()
        .map(Weak::strong_count)
        .collect();
    assert_eq!(holders, vec![1, 1], "only the core holds each decoded body");
    peer.send(&TxRelayMessage::GetTxs(unknown(1))).await?;
    assert_eq!(
        peer.next_state().await?,
        TxRelayMessage::NotFound(unknown(1))
    );

    pool.change(|held| held.slow = false, Vec::new());
    await_until("both are admitted", NET, || {
        let held = pool.held.lock().expect("the test never poisons the lock");
        bodies
            .iter()
            .all(|body| held.transactions.iter().any(|tx| id(tx) == id(body)))
    })
    .await?;
    assert!(!peer.is_closed());

    peer.shutdown().await;
    victim.node.shutdown().await;
    Ok(())
}
