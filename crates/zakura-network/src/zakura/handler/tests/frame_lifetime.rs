//! Real QUIC frames reach their stream workers' frame gates and worker slots.

use super::read_close_cause::Link;
use super::*;
use crate::zakura::{
    legacy_gossip::{LegacyResponseCodec, MSG_RESPONSE_PONG},
    regulation::OutputByteBudget,
    RequestResponseService,
};
use zakura_chain::{
    block::Block,
    serialization::{ZcashDeserializeInto as _, ZcashSerialize as _},
    transaction::UnminedTx,
};
use zakura_rt::sync::counter::{AtomicUsize, Ordering};

const TEST_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a test watches for something that must not happen.
const HELD: Duration = Duration::from_millis(150);

fn frame_bytes(message_type: u16, payload_len: usize) -> Vec<u8> {
    let frame = Frame {
        message_type,
        flags: 0,
        payload: vec![1; payload_len],
    };
    frame.encode(1024).expect("the test frame fits the cap")
}

fn prelude(request_id: Option<u64>) -> StreamPrelude {
    StreamPrelude {
        magic: STREAM_PRELUDE_MAGIC,
        stream_kind: 900,
        stream_version: 1,
        request_id,
        max_frame_bytes: 1024,
    }
}

/// A raw gate of `capacity` bytes, with the budget that observes it.
fn raw_gate(capacity: u64) -> (FrameGate, OutputByteBudget) {
    let budget = OutputByteBudget::new(capacity).unwrap();
    let wide = OutputByteBudget::new(1 << 30).unwrap();
    (FrameGate::new(budget.clone(), wide), budget)
}

/// The reader stops after a checked header until capacity returns. A worker's
/// frame stays charged until its consumer receives and releases it.
#[tokio::test]
async fn a_saturated_gate_holds_the_payload_until_capacity_returns() -> Result<(), BoxError> {
    let mut link = Link::connect().await?;
    let mut context = tabled_context(None, 128);
    let (gate, budget) = raw_gate(64);
    let shared = gate.try_admit(64).unwrap();
    let _floor = gate.try_admit(64).unwrap();
    context.resources.raw = gate.clone();

    let bytes = frame_bytes(1, 16);
    let (_peer_send, _peer_recv, send, recv) = link.open(&bytes).await?;
    let (inbound_tx, mut inbound_rx) = mpsc::channel(4);
    let (_outbound_tx, outbound_rx) = worker_framed_channel(1);
    let cancel = context.connection_token.clone();
    context.precheck.start();
    let worker = AbortOnDropHandle::new(tokio::spawn(persistent_stream_worker(
        send,
        recv,
        prelude(None),
        context,
        inbound_tx,
        outbound_rx,
        4,
    )));
    assert!(
        timeout(HELD, inbound_rx.recv()).await.is_err(),
        "no frame passes the saturated gate"
    );

    drop(shared);
    let admitted = timeout(TEST_TIMEOUT, inbound_rx.recv())
        .await?
        .ok_or("the freed capacity admits the frame")?;
    assert_eq!(admitted.frame.payload, vec![1; 16]);
    assert!(admitted.grant.waited() > Duration::ZERO);
    assert_eq!(budget.granted(), 24, "the queued frame keeps its charge");
    drop(admitted);
    assert_eq!(budget.granted(), 0);

    cancel.cancel();
    timeout(TEST_TIMEOUT, worker).await??;
    link.close().await;
    Ok(())
}

/// One stream's unfunded frame stays in QUIC's buffers, where the peer memory budget
/// funds it, while a second stream on the same saturated connection delivers its floor
/// frame. Neither frame adds to the node's raw charge, so nothing counts twice.
#[tokio::test]
async fn a_saturated_node_delivers_another_streams_floor_frame() -> Result<(), BoxError> {
    let mut link = Link::connect().await?;
    let node = NodeResources::new(ResourceEnvelope::new(TransportLimits::with_windows(
        1 << 20,
        1,
    )));
    let connection = node.connection();
    let (raw_hold, output_hold) = node.hold_node_budgets();
    let held_raw = node.usage().raw_bytes;

    let mut paused = tabled_context(None, 128);
    paused.resources = connection.stream();
    let paused_floor = paused.resources.raw.try_admit(1).unwrap();
    let mut flowing = tabled_context(None, 128);
    flowing.resources = connection.stream();
    assert_eq!(node.usage().owners.setup, 2);

    let bytes = frame_bytes(1, 16);
    let (_paused_peer_send, _paused_peer_recv, send, recv) = link.open(&bytes).await?;
    let (paused_tx, mut paused_rx) = mpsc::channel(4);
    let (_paused_outbound, outbound_rx) = worker_framed_channel(1);
    let paused_cancel = paused.connection_token.clone();
    paused.precheck.start();
    let paused_worker = AbortOnDropHandle::new(tokio::spawn(persistent_stream_worker(
        send,
        recv,
        prelude(None),
        paused,
        paused_tx,
        outbound_rx,
        4,
    )));

    let (_flowing_peer_send, _flowing_peer_recv, send, recv) = link.open(&bytes).await?;
    let (flowing_tx, mut flowing_rx) = mpsc::channel(4);
    let (_flowing_outbound, outbound_rx) = worker_framed_channel(1);
    let flowing_cancel = flowing.connection_token.clone();
    flowing.precheck.start();
    let flowing_worker = AbortOnDropHandle::new(tokio::spawn(persistent_stream_worker(
        send,
        recv,
        prelude(None),
        flowing,
        flowing_tx,
        outbound_rx,
        4,
    )));

    let delivered = timeout(TEST_TIMEOUT, flowing_rx.recv())
        .await?
        .ok_or("the second stream's floor funds its frame")?;
    assert_eq!(delivered.frame.payload, vec![1; 16]);
    assert!(
        timeout(HELD, paused_rx.recv()).await.is_err(),
        "the first stream's floor is taken, so its frame stays in QUIC"
    );
    assert_eq!(
        node.usage().raw_bytes,
        held_raw,
        "unread and floor frames add no raw charge"
    );

    drop(paused_floor);
    let released = timeout(TEST_TIMEOUT, paused_rx.recv())
        .await?
        .ok_or("the returned floor slot admits the held frame")?;
    assert_eq!(released.frame.payload, vec![1; 16]);

    drop((delivered, released));
    paused_cancel.cancel();
    flowing_cancel.cancel();
    timeout(TEST_TIMEOUT, paused_worker).await??;
    timeout(TEST_TIMEOUT, flowing_worker).await??;
    assert_eq!(node.usage().owners.total(), 0);
    drop((raw_hold, output_hold));
    assert_eq!(node.usage().raw_bytes, 0);
    link.close().await;
    Ok(())
}

/// A peer that declares a large frame and stops holds its admitted bytes only
/// until cancellation ends the reader. Each stream's owner leaves the count.
#[tokio::test]
async fn cancelling_a_partial_frame_returns_its_admission() -> Result<(), BoxError> {
    let mut link = Link::connect().await?;
    let mut context = tabled_context(None, 128);
    let node = NodeResources::new(ResourceEnvelope::new(TransportLimits::with_windows(
        1 << 20,
        1,
    )));
    let (gate, budget) = raw_gate(1 << 20);
    context.resources = node.connection().stream();
    context.resources.raw = gate;
    context.resources.owner.advance(OwnerPhase::Installed);
    assert_eq!(node.owners().counts().installed, 1);

    let mut bytes = frame_bytes(1, 512);
    bytes.truncate(100);
    let (_peer_send, _peer_recv, send, recv) = link.open(&bytes).await?;
    let (inbound_tx, _inbound_rx) = mpsc::channel(4);
    let (_outbound_tx, outbound_rx) = worker_framed_channel(1);
    let cancel = context.connection_token.clone();
    context.precheck.start();
    let worker = AbortOnDropHandle::new(tokio::spawn(persistent_stream_worker(
        send,
        recv,
        prelude(None),
        context,
        inbound_tx,
        outbound_rx,
        4,
    )));
    timeout(TEST_TIMEOUT, async {
        while budget.granted() != 520 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;

    cancel.cancel();
    timeout(TEST_TIMEOUT, worker).await??;
    assert_eq!(budget.granted(), 0);
    assert_eq!(node.owners().counts().total(), 0);
    link.close().await;
    Ok(())
}

/// A request service that answers only after the test releases it.
#[derive(Debug)]
struct WaitingRequests {
    streams: [Stream; 1],
    calls: Arc<Semaphore>,
}

impl Service for WaitingRequests {
    fn name(&self) -> &'static str {
        "waiting-requests"
    }
    fn streams(&self) -> &[Stream] {
        &self.streams
    }
    fn as_request_response(&self) -> Option<&dyn RequestResponseService> {
        Some(self)
    }
    fn add_peer(&self, _peer: Peer) {}
    fn remove_peer(&self, _peer: &ZakuraPeerId, _conn_id: ZakuraConnId) {}
}

impl RequestResponseService for WaitingRequests {
    fn request_frame<'a>(
        &'a self,
        _peer_id: ZakuraPeerId,
        _stream_kind: u16,
        _request_id: u64,
        _max_frame_bytes: u32,
        _max_message_bytes: u32,
        _frame: Frame,
    ) -> BoxRunFuture<'a, Result<Vec<Frame>, SinkReject>> {
        Box::pin(async move {
            let _call = self.calls.acquire().await;
            Ok(Vec::new())
        })
    }
}

/// Every open request runs at once, with no execution slot to wait for. Each
/// keeps its raw frame's charge until its call ends or its connection cancels.
#[tokio::test]
async fn concurrent_requests_all_run_and_settle_their_raw_charges() -> Result<(), BoxError> {
    // The test link allows two open streams.
    const REQUESTS: u64 = 2;
    let mut link = Link::connect().await?;
    let (gate, budget) = raw_gate(1 << 20);
    let calls = Arc::new(Semaphore::new(0));
    let registry = Arc::new(ServiceRegistry::new(vec![Arc::new(WaitingRequests {
        streams: [Stream {
            kind: 900,
            version: 1,
            frame_cap: 1024,
            capability: 1 << 20,
            ..Stream::REQUEST_RESPONSE
        }],
        calls: calls.clone(),
    })])?);

    let bytes = frame_bytes(1, 16);
    let mut held = Vec::new();
    let mut workers = Vec::new();
    let mut cancel = None;
    for request in 0..REQUESTS {
        let mut context = tabled_context(None, 128);
        context.resources.raw = gate.clone();
        cancel = Some(context.connection_token.clone());
        let (peer_send, peer_recv, send, recv) = link.open(&bytes).await?;
        held.push((peer_send, peer_recv));
        workers.push(AbortOnDropHandle::new(tokio::spawn(request_stream_worker(
            send,
            recv,
            prelude(Some(request)),
            context,
            registry.clone(),
        ))));
    }
    timeout(TEST_TIMEOUT, async {
        while budget.granted() != REQUESTS * 24 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    tokio::time::sleep(HELD).await;
    assert_eq!(
        budget.granted(),
        REQUESTS * 24,
        "the calls keep their frames"
    );

    // Cancellation ends the last call, and the release ends the other.
    let cancelled = workers.pop().ok_or("the test opened requests")?;
    cancel.ok_or("the test opened requests")?.cancel();
    timeout(TEST_TIMEOUT, cancelled).await??;
    assert_eq!(budget.granted(), (REQUESTS - 1) * 24);

    calls.add_permits(usize::try_from(REQUESTS)?);
    for worker in workers {
        timeout(TEST_TIMEOUT, worker).await??;
    }
    assert_eq!(budget.granted(), 0);
    link.close().await;
    Ok(())
}

/// A legacy request service that counts its calls, waits for the test's release, and
/// answers with `answer`.
#[derive(Debug)]
struct CountedRequests {
    streams: [Stream; 1],
    started: Arc<AtomicUsize>,
    release: Arc<Semaphore>,
    answer: Vec<Frame>,
}

impl Service for CountedRequests {
    fn name(&self) -> &'static str {
        "counted-requests"
    }
    fn streams(&self) -> &[Stream] {
        &self.streams
    }
    fn as_request_response(&self) -> Option<&dyn RequestResponseService> {
        Some(self)
    }
    fn add_peer(&self, _peer: Peer) {}
    fn remove_peer(&self, _peer: &ZakuraPeerId, _conn_id: ZakuraConnId) {}
}

impl RequestResponseService for CountedRequests {
    fn request_frame<'a>(
        &'a self,
        _peer_id: ZakuraPeerId,
        _stream_kind: u16,
        _request_id: u64,
        _max_frame_bytes: u32,
        _max_message_bytes: u32,
        _frame: Frame,
    ) -> BoxRunFuture<'a, Result<Vec<Frame>, SinkReject>> {
        Box::pin(async move {
            self.started.fetch_add(1, Ordering::Relaxed);
            let _release = self.release.acquire().await;
            Ok(self.answer.clone())
        })
    }
}

/// One link's legacy request workers, each with its own floor over a shared output
/// budget, as a connection's streams have.
struct LegacyRequests {
    link: Link,
    connection: OutputByteBudget,
    node: OutputByteBudget,
    raw: FrameGate,
    raw_budget: OutputByteBudget,
    registry: Arc<ServiceRegistry>,
    started: Arc<AtomicUsize>,
    release: Arc<Semaphore>,
    held: Vec<(SendStream, RecvStream)>,
}

impl LegacyRequests {
    async fn connect(output_bytes: u64, answer: Vec<Frame>) -> Result<Self, BoxError> {
        let started = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Semaphore::new(0));
        let registry = Arc::new(ServiceRegistry::new(vec![Arc::new(CountedRequests {
            streams: [Stream {
                kind: LEGACY_REQUEST_STREAM_KIND,
                version: 1,
                frame_cap: 1024,
                capability: 1 << 20,
                ..Stream::REQUEST_RESPONSE
            }],
            started: started.clone(),
            release: release.clone(),
            answer,
        })])?);
        let (raw, raw_budget) = raw_gate(1 << 20);
        Ok(Self {
            link: Link::connect().await?,
            connection: OutputByteBudget::new(output_bytes)?,
            node: OutputByteBudget::new(1 << 30)?,
            raw,
            raw_budget,
            registry,
            started,
            release,
            held: Vec::new(),
        })
    }

    fn output_gate(&self) -> FrameGate {
        FrameGate::new(self.connection.clone(), self.node.clone())
    }

    /// Start a worker on a new stream for this request, with its cancellation.
    async fn start(
        &mut self,
        message_type: u16,
        payload: Vec<u8>,
    ) -> Result<(AbortOnDropHandle<()>, CancellationToken), BoxError> {
        let frame = Frame {
            message_type,
            flags: 0,
            payload,
        };
        let (peer_send, peer_recv, send, recv) = self.link.open(&frame.encode(1024)?).await?;
        self.held.push((peer_send, peer_recv));
        let mut context = tabled_context(None, 128);
        context.resources.raw = self.raw.clone();
        context.resources.output = self.output_gate();
        let cancel = context.connection_token.clone();
        let worker = AbortOnDropHandle::new(tokio::spawn(request_stream_worker(
            send,
            recv,
            StreamPrelude {
                stream_kind: LEGACY_REQUEST_STREAM_KIND,
                ..prelude(Some(7))
            },
            context,
            self.registry.clone(),
        )));
        Ok((worker, cancel))
    }

    fn calls(&self) -> usize {
        self.started.load(Ordering::Relaxed)
    }

    async fn until_calls(&self, calls: usize) -> Result<(), BoxError> {
        timeout(TEST_TIMEOUT, async {
            while self.calls() != calls {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;
        Ok(())
    }

    /// Wait until the raw budget shows a worker's request frame.
    async fn until_raw(&self, bytes: u64) -> Result<(), BoxError> {
        timeout(TEST_TIMEOUT, async {
            while self.raw_budget.granted() != bytes {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;
        Ok(())
    }
}

/// A request for one block: a compact count and its hash.
fn one_block_request() -> Vec<u8> {
    let mut payload = vec![1];
    payload.extend_from_slice(&[7; 32]);
    payload
}

/// The bound of `one_block_request` on this file's streams.
fn one_block_bound() -> ResponseBound {
    let frame = Frame {
        message_type: LEGACY_REQUEST_BLOCKS_BY_HASH,
        flags: 0,
        payload: one_block_request(),
    };
    response_bound(
        LEGACY_REQUEST_STREAM_KIND,
        &frame,
        test_connection_limits(),
        1024,
    )
}

fn pong() -> Vec<Frame> {
    vec![Frame {
        message_type: MSG_RESPONSE_PONG,
        flags: 0,
        payload: 7u64.to_le_bytes().to_vec(),
    }]
}

/// With the connection's output saturated, a further block request reads its frame
/// and stops. Its service never runs, so no result exists to retain. Returned output
/// lets the request run under its own reservation.
#[tokio::test]
async fn a_saturated_output_budget_holds_a_request_before_its_service() -> Result<(), BoxError> {
    const OUTPUT: u64 = 8 << 20;
    let mut requests = LegacyRequests::connect(OUTPUT, Vec::new()).await?;
    let saturating = requests
        .output_gate()
        .admit_result(2, OUTPUT)
        .now_or_never()
        .ok_or("an idle budget funds the charge")?;
    let frame_bytes = 8 + one_block_request().len() as u64;

    let (worker, _cancel) = requests
        .start(LEGACY_REQUEST_BLOCKS_BY_HASH, one_block_request())
        .await?;
    requests.until_raw(frame_bytes).await?;
    tokio::time::sleep(HELD).await;
    assert_eq!(requests.calls(), 0, "an unfunded request does not execute");
    assert_eq!(requests.connection.granted(), OUTPUT);
    assert_eq!(
        requests.raw_budget.granted(),
        frame_bytes,
        "the waiting request keeps its raw frame's funding"
    );

    drop(saturating);
    requests.until_calls(1).await?;
    assert_eq!(
        requests.connection.granted(),
        one_block_bound().bytes,
        "the call runs under its whole reservation"
    );

    requests.release.add_permits(1);
    timeout(TEST_TIMEOUT, worker).await??;
    assert_eq!(requests.connection.granted(), 0);
    assert_eq!(requests.raw_budget.granted(), 0);
    requests.link.close().await;
    Ok(())
}

/// A ping runs in the room a large reservation leaves, and on a full budget it runs
/// on its own stream's floor slot.
#[tokio::test]
async fn a_small_request_runs_beside_a_large_reservation() -> Result<(), BoxError> {
    const OUTPUT: u64 = 8 << 20;
    const LEFT: u64 = 1024;
    let mut requests = LegacyRequests::connect(OUTPUT, pong()).await?;
    requests.release.add_permits(2);

    let large = requests
        .output_gate()
        .admit_result(2, OUTPUT - LEFT)
        .now_or_never()
        .ok_or("an idle budget funds the reservation")?;
    let (beside, _cancel) = requests.start(LEGACY_REQUEST_PING, Vec::new()).await?;
    timeout(TEST_TIMEOUT, beside).await??;
    assert_eq!(requests.calls(), 1);
    assert_eq!(requests.connection.granted(), OUTPUT - LEFT);

    let rest = requests
        .output_gate()
        .admit_result(2, LEFT)
        .now_or_never()
        .ok_or("the reservation left this room")?;
    let (on_floor, _cancel) = requests.start(LEGACY_REQUEST_PING, Vec::new()).await?;
    timeout(TEST_TIMEOUT, on_floor).await??;
    assert_eq!(requests.calls(), 2);
    assert_eq!(requests.connection.granted(), OUTPUT);

    drop((large, rest));
    requests.link.close().await;
    Ok(())
}

/// Cancelling the connection returns a reservation that funds a running call, and a
/// cancelled wait for a reservation takes nothing.
#[tokio::test]
async fn cancelling_a_request_returns_its_reservation() -> Result<(), BoxError> {
    const OUTPUT: u64 = 8 << 20;
    let mut requests = LegacyRequests::connect(OUTPUT, Vec::new()).await?;

    let (running, cancel_running) = requests
        .start(LEGACY_REQUEST_BLOCKS_BY_HASH, one_block_request())
        .await?;
    requests.until_calls(1).await?;
    let reserved = one_block_bound().bytes;
    assert_eq!(requests.connection.granted(), reserved);
    cancel_running.cancel();
    timeout(TEST_TIMEOUT, running).await??;
    assert_eq!(requests.connection.granted(), 0);
    assert_eq!(requests.raw_budget.granted(), 0);

    let saturating = requests
        .output_gate()
        .admit_result(2, OUTPUT)
        .now_or_never()
        .ok_or("cancellation returned the budget")?;
    let (waiting, cancel_waiting) = requests
        .start(LEGACY_REQUEST_BLOCKS_BY_HASH, one_block_request())
        .await?;
    requests
        .until_raw(8 + one_block_request().len() as u64)
        .await?;
    cancel_waiting.cancel();
    timeout(TEST_TIMEOUT, waiting).await??;
    assert_eq!(requests.calls(), 1, "the cancelled wait never ran its call");
    assert_eq!(requests.connection.granted(), OUTPUT);
    assert_eq!(requests.raw_budget.granted(), 0);

    drop(saturating);
    assert_eq!(requests.connection.granted(), 0);
    requests.link.close().await;
    Ok(())
}

/// Each kind reserves from its own limit. Small kinds stay small, and a request with
/// no derivable limit gets the compatibility response cap.
#[test]
fn each_request_kind_reserves_its_own_bound() {
    let limits = test_connection_limits();
    const CAP: u32 = 1 << 20;
    let bound = |kind: u16, message_type: u16, payload: Vec<u8>| {
        let frame = Frame {
            message_type,
            flags: 0,
            payload,
        };
        response_bound(kind, &frame, limits, CAP)
    };
    let legacy = |message_type, payload| bound(LEGACY_REQUEST_STREAM_KIND, message_type, payload);
    let hashes = |count: u8| {
        let mut payload = vec![count];
        payload.extend(std::iter::repeat_n(7, usize::from(count) * 32));
        payload
    };
    let max = ResponseBound::MAX;

    for small in [LEGACY_REQUEST_PING, LEGACY_REQUEST_PUSH_TRANSACTION] {
        assert_eq!(
            legacy(small, Vec::new()),
            ResponseBound {
                frames: 1,
                bytes: 16
            }
        );
    }
    let hashes_bound = legacy(LEGACY_REQUEST_FIND_BLOCKS, Vec::new());
    assert_eq!(hashes_bound.frames, 1);
    assert!(hashes_bound.bytes <= 17 + 500 * 32 + 8);
    for one_frame in [
        LEGACY_REQUEST_FIND_HEADERS,
        LEGACY_REQUEST_MEMPOOL_TRANSACTION_IDS,
    ] {
        let bound = legacy(one_frame, Vec::new());
        assert_eq!(bound.frames, 1);
        assert!(bound.bytes <= u64::from(CAP));
    }

    let one_block = legacy(LEGACY_REQUEST_BLOCKS_BY_HASH, hashes(1));
    assert!(one_block.frames > 1, "a block may need several frames");
    assert!(one_block.bytes > MAX_PROTOCOL_MESSAGE_LEN as u64);
    assert!(one_block.bytes < 2 * MAX_PROTOCOL_MESSAGE_LEN as u64);
    let many_blocks = legacy(LEGACY_REQUEST_BLOCKS_BY_HASH, hashes(200));
    assert!(many_blocks.bytes > max.bytes, "headers add to the cap");
    assert!(many_blocks.bytes < max.bytes + 64 * 1024);

    assert_eq!(bound(900, LEGACY_REQUEST_PING, Vec::new()), max);
    assert_eq!(legacy(LEGACY_REQUEST_BLOCKS_BY_HASH, vec![9; 3]), max);
    assert_eq!(legacy(u16::MAX, Vec::new()), max);
}

/// A real block's response under a peer's small negotiated caps stays within its
/// reservation, so a peer cannot shrink a reservation by negotiating them down.
#[test]
fn the_bound_covers_an_encoded_response_under_small_caps() -> Result<(), BoxError> {
    let block: Arc<Block> =
        Arc::new(zakura_test::vectors::BLOCK_TESTNET_141042_BYTES.zcash_deserialize_into()?);
    let transaction = UnminedTx::from(block.transactions[0].clone());
    for (frame_cap, message_cap) in [(1 << 20, 4 << 20), (1 << 20, 64), (64, 1 << 20), (64, 64)] {
        let mut limits = test_connection_limits();
        limits.max_message_bytes = message_cap;
        let responses = [
            Response::Blocks(vec![
                InventoryResponse::Available((block.clone().into(), None)),
                InventoryResponse::Missing(block.hash()),
            ]),
            Response::Transactions(vec![InventoryResponse::Available((
                transaction.clone(),
                None,
            ))]),
        ];
        for (message_type, response) in [
            LEGACY_REQUEST_BLOCKS_BY_HASH,
            LEGACY_REQUEST_TRANSACTIONS_BY_ID,
        ]
        .into_iter()
        .zip(responses)
        {
            let items = if message_type == LEGACY_REQUEST_BLOCKS_BY_HASH {
                2
            } else {
                1
            };
            let mut payload = vec![items];
            if message_type == LEGACY_REQUEST_BLOCKS_BY_HASH {
                payload.extend_from_slice(&[7; 64]);
            } else {
                InventoryHash::from(transaction.id()).zcash_serialize(&mut payload)?;
            }
            let frame = Frame {
                message_type,
                flags: 0,
                payload,
            };
            let bound = response_bound(LEGACY_REQUEST_STREAM_KIND, &frame, limits, frame_cap);
            let frames = LegacyResponseCodec::encode_response(7, response, frame_cap, message_cap)?;
            let bytes: u64 = frames
                .iter()
                .map(|frame| frame_wire_bytes(frame.payload.len()))
                .sum();
            assert!(
                bytes <= bound.bytes && frames.len() <= bound.frames,
                "{} frames of {bytes} bytes exceed {bound:?}",
                frames.len(),
            );
        }
    }
    Ok(())
}

/// An unpolled stream-setup future's resources are its own. Dropping the value
/// that carries them leaves nothing, as the connection loop's teardown does.
#[test]
fn dropped_stream_resources_leave_no_owner() {
    let node = NodeResources::new(ResourceEnvelope::new(TransportLimits::with_windows(
        1 << 20,
        1,
    )));
    let resources = node.connection().stream();
    let setup = async move {
        let _resources = resources;
        future::pending::<()>().await
    };
    assert_eq!(node.owners().counts().setup, 1);
    drop(setup);
    assert_eq!(node.owners().counts().total(), 0);
}
