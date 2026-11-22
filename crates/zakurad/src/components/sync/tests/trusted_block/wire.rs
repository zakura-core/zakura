//! Wire-level accounting across trusted relay and committed announcements.

mod tls_upload;

use std::{
    io,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zakura_chain::serialization::ZcashDeserialize;
use zakura_network::types::InventoryHash;

use super::*;

#[derive(Clone, Debug)]
struct WireEvent {
    at: std::time::Instant,
    direction: &'static str,
    kind: &'static str,
    hash: zakura_chain::block::Hash,
    bytes: usize,
}

/// Decodes copies of the actual socket bytes, leaving the production transport unchanged.
struct Capture {
    buffered: BytesMut,
    direction: &'static str,
    events: Arc<Mutex<Vec<WireEvent>>>,
}

impl Capture {
    fn new(
        _network: &Network,
        direction: &'static str,
        events: Arc<Mutex<Vec<WireEvent>>>,
    ) -> Self {
        Self {
            buffered: BytesMut::new(),
            direction,
            events,
        }
    }

    fn record(&mut self, bytes: &[u8]) {
        self.buffered.extend_from_slice(bytes);
        while self.buffered.len() >= 24 {
            let payload = usize::try_from(u32::from_le_bytes(
                self.buffered[16..20].try_into().unwrap(),
            ))
            .unwrap();
            assert!(
                payload <= 2_000_000,
                "fixture messages fit the legacy payload bound"
            );
            let bytes = 24 + payload;
            if self.buffered.len() < bytes {
                break;
            }
            let frame = self.buffered.split_to(bytes);
            let command = &frame[4..16];
            let mut body = &frame[24..];
            let (kind, hashes) = if command.starts_with(b"inv\0") {
                (
                    "inv",
                    block_hashes(Vec::<InventoryHash>::zcash_deserialize(&mut body).unwrap()),
                )
            } else if command.starts_with(b"getdata\0") {
                (
                    "getdata",
                    block_hashes(Vec::<InventoryHash>::zcash_deserialize(&mut body).unwrap()),
                )
            } else if command.starts_with(b"block\0") {
                let block = Block::zcash_deserialize_bounded(&mut body).unwrap();
                ("block", vec![block.hash()])
            } else {
                continue;
            };
            for hash in hashes {
                self.events.lock().unwrap().push(WireEvent {
                    at: std::time::Instant::now(),
                    direction: self.direction,
                    kind,
                    hash,
                    bytes,
                });
            }
        }
    }
}

fn block_hashes(items: Vec<InventoryHash>) -> Vec<zakura_chain::block::Hash> {
    items
        .into_iter()
        .filter_map(|item| match item {
            InventoryHash::Block(hash) => Some(hash),
            _ => None,
        })
        .collect()
}

struct ObservedSocket {
    socket: tokio::net::TcpStream,
    received: Capture,
    sent: Capture,
}

impl AsyncRead for ObservedSocket {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.socket).poll_read(cx, buf);
        self.received.record(&buf.filled()[before..]);
        result
    }
}

impl AsyncWrite for ObservedSocket {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.socket).poll_write(cx, buf);
        if let Poll::Ready(Ok(written)) = result {
            self.sent.record(&buf[..written]);
        }
        result
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_shutdown(cx)
    }
}

struct WirePeer {
    client: Peers,
    events: Arc<Mutex<Vec<WireEvent>>>,
    advertisements: mpsc::UnboundedReceiver<zakura_chain::block::Hash>,
}

impl WirePeer {
    async fn connect(relay: &Node) -> Self {
        let events = Arc::new(Mutex::new(Vec::new()));
        let socket = ObservedSocket {
            socket: tokio::net::TcpStream::connect(relay.p2p).await.unwrap(),
            received: Capture::new(&relay.network, "received", events.clone()),
            sent: Capture::new(&relay.network, "sent", events.clone()),
        };
        let (advertised, advertisements) = mpsc::unbounded_channel();
        let client = zakura_network::connect_isolated_with_inbound(
            &relay.network,
            socket,
            "wire observer".into(),
            tower::service_fn(move |request| {
                if let Request::AdvertiseBlock(block, _) = request {
                    let _ = advertised.send(block.hash);
                }
                async { Ok::<_, BoxError>(Response::Nil) }
            }),
        )
        .await
        .unwrap();
        let client = BoxCloneSyncService::new(Buffer::new(client, 4));
        client
            .clone()
            .oneshot(Request::Ping(zakura_network::types::Nonce(123)))
            .await
            .unwrap();
        relay.connected().await;
        Self {
            client,
            events,
            advertisements,
        }
    }

    async fn announced(&mut self, hash: zakura_chain::block::Hash) {
        timeout(Duration::from_secs(20), async {
            while self.advertisements.recv().await.unwrap() != hash {}
        })
        .await
        .unwrap();
    }

    async fn fetch(&self, block: &Block) {
        let Response::Blocks(blocks) = timeout(
            Duration::from_secs(20),
            self.client
                .clone()
                .oneshot(Request::BlocksByHash([block.hash()].into_iter().collect())),
        )
        .await
        .unwrap()
        .unwrap() else {
            panic!("peer returns a block response")
        };
        assert_eq!(blocks.len(), 1);
        let fetched = blocks.into_iter().next().unwrap().available().unwrap();
        assert_eq!(
            fetched.0.zcash_serialize_to_vec().unwrap(),
            block.zcash_serialize_to_vec().unwrap()
        );
    }

    fn count(&self, hash: zakura_chain::block::Hash, kind: &str) -> usize {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.hash == hash && event.kind == kind)
            .count()
    }

    async fn settled(&self) {
        // Cover the 2s no-request expiry and 750ms upload grace, then prove the socket is live.
        tokio::time::sleep(Duration::from_millis(2200)).await;
        timeout(
            Duration::from_secs(10),
            self.client
                .clone()
                .oneshot(Request::Ping(zakura_network::types::Nonce(123))),
        )
        .await
        .unwrap()
        .unwrap();
    }
}

/// Commit before a request, during a held upload, after a completed upload, or with no request.
#[derive(Clone, Copy, Debug)]
enum CommitAt {
    BeforeRequest,
    DuringUpload,
    AfterUpload,
    NoRequest,
}

async fn commit_handoff(at: CommitAt) {
    let _init = zakura_test::init();
    let mut relay = Node::new(None).await;
    let mut peer = WirePeer::connect(&relay).await;
    let block = relay.template().await;
    relay.gate.hold();
    let submission = tokio::spawn(submit(
        relay.rpc,
        relay.auth.clone(),
        block.zcash_serialize_to_vec().unwrap(),
        true,
    ));
    assert_eq!(relay.gate.entered().await, block.hash());
    peer.announced(block.hash()).await;
    assert_eq!(relay.tip.best_tip_height(), Some(Height(0)));
    assert!(!submission.is_finished());
    assert_eq!(peer.count(block.hash(), "inv"), 1);
    match at {
        CommitAt::DuringUpload => {
            relay.hold_body.send_replace(true);
            let fetch = peer.fetch(&block);
            tokio::pin!(fetch);
            assert!(fetch.as_mut().now_or_never().is_none());
            assert_eq!(
                timeout(Duration::from_secs(10), relay.body_requests.recv())
                    .await
                    .unwrap(),
                Some(block.hash())
            );
            relay.gate.release();
            assert_eq!(submission.await.unwrap(), "accepted");
            relay.hold_body.send_replace(false);
            fetch.await;
        }
        CommitAt::AfterUpload => {
            peer.fetch(&block).await;
            peer.settled().await;
            relay.gate.release();
            assert_eq!(submission.await.unwrap(), "accepted");
        }
        CommitAt::BeforeRequest | CommitAt::NoRequest => {
            relay.gate.release();
            assert_eq!(submission.await.unwrap(), "accepted");
            if matches!(at, CommitAt::BeforeRequest) {
                peer.fetch(&block).await;
            }
        }
    }
    peer.settled().await;
    assert_eq!(
        peer.count(block.hash(), "inv"),
        1,
        "commit must join the early announcement, not write another inv: {at:?}"
    );
    let expected_bodies = usize::from(!matches!(at, CommitAt::NoRequest));
    assert_eq!(peer.count(block.hash(), "block"), expected_bodies);
    assert_eq!(peer.count(block.hash(), "getdata"), expected_bodies);
    assert!(relay.pending.get(block.hash()).is_none());
    let events = peer.events.lock().unwrap();
    let inv = events
        .iter()
        .find(|event| event.kind == "inv" && event.hash == block.hash())
        .unwrap();
    assert_eq!(inv.direction, "received");
    assert_eq!(inv.bytes, 61, "24-byte header plus one 37-byte inventory");
    for body in events
        .iter()
        .filter(|event| event.kind == "block" && event.hash == block.hash())
    {
        assert!(body.at >= inv.at);
        assert_eq!(
            body.bytes,
            24 + block.zcash_serialize_to_vec().unwrap().len()
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn trusted_commit_reuses_the_inventory_already_written_to_tcp() {
    commit_handoff(CommitAt::BeforeRequest).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn trusted_commit_preserves_an_upload_waiting_for_its_body() {
    commit_handoff(CommitAt::DuringUpload).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn trusted_commit_reuses_a_completed_upload() {
    commit_handoff(CommitAt::AfterUpload).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn trusted_commit_does_not_repeat_inventory_for_a_peer_that_never_requests() {
    commit_handoff(CommitAt::NoRequest).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn trusted_commit_reaches_a_peer_connected_after_early_fanout() {
    let _init = zakura_test::init();
    let mut relay = Node::new(None).await;
    let mut early_peer = WirePeer::connect(&relay).await;
    let block = relay.template().await;
    relay.gate.hold();
    let submission = tokio::spawn(submit(
        relay.rpc,
        relay.auth.clone(),
        block.zcash_serialize_to_vec().unwrap(),
        true,
    ));
    relay.gate.entered().await;
    early_peer.announced(block.hash()).await;
    // The early fan-out has already selected its recipients.
    let mut late_peer = WirePeer::connect(&relay).await;
    relay.gate.release();
    assert_eq!(submission.await.unwrap(), "accepted");
    late_peer.announced(block.hash()).await;
    tokio::join!(early_peer.fetch(&block), late_peer.fetch(&block));
    early_peer.settled().await;
    assert_eq!(early_peer.count(block.hash(), "inv"), 1);
    assert_eq!(late_peer.count(block.hash(), "inv"), 1);
}

/// A nearly full block with an unspendable coinbase output. Both arms validate these exact bytes.
fn large_block(mut block: Block) -> Block {
    let mut script = vec![0; 1_990_000];
    script[0] = 0x6a; // OP_RETURN: padding is data in an output, never executed.
    Arc::make_mut(&mut block.transactions[0]).outputs_mut()[0].lock_script =
        zakura_chain::transparent::Script::new(&script);
    Arc::make_mut(&mut block.header).merkle_root = block.transactions.iter().collect();
    block
}

#[expect(
    clippy::print_stdout,
    reason = "explicit accounting runs emit machine-readable measurements"
)]
async fn measured_submission(
    relay: &mut Node,
    peer: &mut WirePeer,
    block: &Block,
    trusted: bool,
    trial: usize,
) {
    let start = std::time::Instant::now();
    let submission = tokio::spawn(submit(
        relay.rpc,
        relay.auth.clone(),
        block.zcash_serialize_to_vec().unwrap(),
        trusted,
    ));
    peer.announced(block.hash()).await;
    peer.fetch(block).await;
    assert_eq!(submission.await.unwrap(), "accepted");
    peer.settled().await;
    assert_eq!(peer.count(block.hash(), "inv"), 1);
    assert_eq!(peer.count(block.hash(), "getdata"), 1);
    assert_eq!(peer.count(block.hash(), "block"), 1);
    let events = peer.events.lock().unwrap();
    let relevant: Vec<_> = events.iter().filter(|e| e.hash == block.hash()).collect();
    let inv = relevant.iter().find(|e| e.kind == "inv").unwrap();
    let body = relevant.iter().find(|e| e.kind == "block").unwrap();
    println!(
        "{}",
        serde_json::json!({
            "trial": trial, "method": if trusted {"submittrustedblock"} else {"submitblock"},
            "hash": block.hash().to_string(), "connections": 1,
            "inv_count": 1, "body_count": 1, "body_payload_bytes": body.bytes - 24,
            "relay_wire_bytes": relevant.iter().filter(|e| e.direction == "received").map(|e| e.bytes).sum::<usize>(),
            "first_inv_ms": inv.at.duration_since(start).as_secs_f64() * 1000.0,
            "body_received_ms": body.at.duration_since(start).as_secs_f64() * 1000.0,
        })
    );
}

/// Protocol accounting on loopback, not a production throughput benchmark.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "30 paired large-block trials, run explicitly when changing relay handoff"]
async fn thirty_paired_submissions_send_one_inventory_and_one_body() {
    let _init = zakura_test::init();
    let mut ordinary = Node::new(None).await;
    let mut trusted = Node::new(None).await;
    let mut ordinary_peer = WirePeer::connect(&ordinary).await;
    let mut trusted_peer = WirePeer::connect(&trusted).await;
    for trial in 0..30 {
        let block = large_block(ordinary.template().await);
        assert_eq!(ordinary.tip.best_tip_hash(), trusted.tip.best_tip_hash());
        tokio::join!(
            measured_submission(&mut ordinary, &mut ordinary_peer, &block, false, trial),
            measured_submission(&mut trusted, &mut trusted_peer, &block, true, trial),
        );
    }
}
