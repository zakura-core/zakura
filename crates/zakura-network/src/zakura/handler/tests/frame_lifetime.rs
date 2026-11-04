//! Real QUIC frames reach their stream workers' frame gates and worker slots.

use super::read_close_cause::Link;
use super::*;
use crate::zakura::{
    legacy_gossip::{LegacyResponseCodec, LEGACY_REQUESTS_BUDGET, MSG_RESPONSE_PONG},
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
    prelude_of(900, request_id)
}

fn prelude_of(stream_kind: u16, request_id: Option<u64>) -> StreamPrelude {
    StreamPrelude {
        magic: STREAM_PRELUDE_MAGIC,
        stream_kind,
        stream_version: 1,
        request_id,
        max_frame_bytes: 1024,
    }
}

/// A test stream of `kind` on its own capability bit.
const fn test_stream(kind: u16, mode: StreamMode) -> Stream {
    Stream {
        kind,
        version: 1,
        frame_cap: 1024,
        capability: 1 << (kind - 880),
        mode,
        ..Stream::PERSISTENT
    }
}

/// One connection's gates for `streams`, where each kind's raw share is
/// `receive` bytes and its output share is `output` bytes.
fn connection_gates(streams: &[Stream], receive: u64, output: u64) -> ConnectionResources {
    let shares = ReactorShares {
        control_receive_bytes: 1 << 20,
        kinds: Vec::new(),
        other: ReactorBudget { receive, output },
    };
    let capabilities = streams
        .iter()
        .fold(0, |bits, stream| bits | stream.capability);
    NodeResources::new(shares, streams.iter().copied(), 1024).connection(capabilities)
}

/// The resources of a new stream of `kind` on `connection`.
fn stream_of(connection: &ConnectionResources, kind: u16) -> StreamResources {
    connection
        .stream(kind, connection.owner())
        .expect("the test connection negotiated the kind")
}

/// A raw gate of `capacity` bytes, with the budget that observes it.
fn raw_gate(capacity: u64) -> (FrameGate, OutputByteBudget) {
    let budget = OutputByteBudget::new(capacity).unwrap();
    (FrameGate::over(budget.clone()), budget)
}

/// A persistent stream worker on `send` and `recv`, its inbound frames, its
/// sender, and its cancellation.
fn spawn_stream_worker(
    send: SendStream,
    recv: RecvStream,
    context: StreamWorkerContext,
) -> (
    AbortOnDropHandle<()>,
    mpsc::Receiver<AdmittedFrame>,
    FramedSend,
    CancellationToken,
) {
    let (inbound_tx, inbound_rx) = mpsc::channel(4);
    let (outbound_tx, outbound_rx) = worker_framed_channel(1);
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
    (worker, inbound_rx, outbound_tx, cancel)
}

/// The reader stops after a checked header until capacity returns. A worker's
/// frame stays charged until its consumer receives and releases it.
#[tokio::test]
async fn a_saturated_gate_holds_the_payload_until_capacity_returns() -> Result<(), BoxError> {
    let mut link = Link::connect().await?;
    let mut context = tabled_context(None, 128);
    let (gate, budget) = raw_gate(64);
    let shared = gate.try_admit(64).unwrap();
    context.resources.raw = gate.clone();

    let bytes = frame_bytes(1, 16);
    let (_peer_send, _peer_recv, send, recv) = link.open(&bytes).await?;
    let (worker, mut inbound_rx, _outbound_tx, cancel) = spawn_stream_worker(send, recv, context);
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

/// One kind's unfunded frame stays in QUIC's buffers, inside its connection's
/// receive window, while a stream of another kind on the same connection
/// delivers its frame. The unread frame adds nothing to the usage report, so
/// nothing counts twice.
#[tokio::test]
async fn a_full_kind_leaves_its_unread_frame_out_of_the_usage_report() -> Result<(), BoxError> {
    const PAUSED: Stream = test_stream(907, StreamMode::Persistent);
    const FLOWING: Stream = test_stream(908, StreamMode::Persistent);
    let mut link = Link::connect().await?;
    let shares = ReactorShares {
        control_receive_bytes: 1 << 20,
        kinds: Vec::new(),
        other: ReactorBudget {
            receive: 1024,
            output: 1024,
        },
    };
    let node = NodeResources::new(shares, [PAUSED, FLOWING], 1024);
    let connection = node.connection(PAUSED.capability | FLOWING.capability);

    let mut paused = tabled_context(None, 128);
    paused.resources = stream_of(&connection, PAUSED.kind);
    let full = paused.resources.raw.try_admit(1024).unwrap();
    let mut flowing = tabled_context(None, 128);
    flowing.resources = stream_of(&connection, FLOWING.kind);
    assert_eq!(node.usage().owners.setup, 2);
    assert_eq!(node.usage().raw_bytes(), 1024);

    let bytes = frame_bytes(1, 16);
    let (_paused_peer_send, _paused_peer_recv, send, recv) = link.open(&bytes).await?;
    let (paused_worker, mut paused_rx, _paused_tx, paused_cancel) =
        spawn_stream_worker(send, recv, paused);
    let (_flowing_peer_send, _flowing_peer_recv, send, recv) = link.open(&bytes).await?;
    let (flowing_worker, mut flowing_rx, _flowing_tx, flowing_cancel) =
        spawn_stream_worker(send, recv, flowing);

    let delivered = timeout(TEST_TIMEOUT, flowing_rx.recv())
        .await?
        .ok_or("the other kind's gate funds its frame")?;
    assert_eq!(delivered.frame.payload, vec![1; 16]);
    assert!(
        timeout(HELD, paused_rx.recv()).await.is_err(),
        "the full kind's frame stays in QUIC"
    );
    let usage = node.usage();
    assert_eq!(
        usage.raw_bytes(),
        1024 + frame_wire_bytes(16),
        "the unread frame adds no raw charge"
    );
    assert_eq!(
        usage.kinds.iter().map(|kind| kind.raw_bound).sum::<u64>(),
        2048
    );

    drop(full);
    let released = timeout(TEST_TIMEOUT, paused_rx.recv())
        .await?
        .ok_or("the freed gate admits the held frame")?;
    assert_eq!(released.frame.payload, vec![1; 16]);

    drop((delivered, released));
    paused_cancel.cancel();
    flowing_cancel.cancel();
    timeout(TEST_TIMEOUT, paused_worker).await??;
    timeout(TEST_TIMEOUT, flowing_worker).await??;
    assert_eq!(node.usage().owners.total(), 0);
    assert_eq!(node.usage().raw_bytes(), 0);
    link.close().await;
    Ok(())
}

/// A peer that declares a large frame and stops holds its admitted bytes only
/// until cancellation ends the reader. Each stream's owner leaves the count.
#[tokio::test]
async fn cancelling_a_partial_frame_returns_its_admission() -> Result<(), BoxError> {
    let mut link = Link::connect().await?;
    let mut context = tabled_context(None, 128);
    let stream = test_stream(900, StreamMode::Persistent);
    let node = NodeResources::new(
        ReactorShares::from_quic(&QuicConfig::default()),
        [stream],
        1024,
    );
    let (gate, budget) = raw_gate(1 << 20);
    let connection = node.connection(stream.capability);
    context.resources = stream_of(&connection, stream.kind);
    context.resources.raw = gate;
    context.resources.owner.advance(OwnerPhase::Installed);
    assert_eq!(node.owners().counts().installed, 1);

    let mut bytes = frame_bytes(1, 512);
    bytes.truncate(100);
    let (_peer_send, _peer_recv, send, recv) = link.open(&bytes).await?;
    let (worker, _inbound_rx, _outbound_tx, cancel) = spawn_stream_worker(send, recv, context);
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

/// A full header gate pauses only header frames: block frames on the same
/// connection keep flowing, and the header frame follows once its gate frees.
#[tokio::test]
async fn a_full_kind_gate_leaves_other_kinds_flowing() -> Result<(), BoxError> {
    const HEADERS: Stream = test_stream(905, StreamMode::Persistent);
    const BLOCKS: Stream = test_stream(906, StreamMode::Persistent);
    let mut link = Link::connect().await?;
    let connection = connection_gates(&[HEADERS, BLOCKS], 1024, 1024);
    let headers = stream_of(&connection, HEADERS.kind);
    let full = headers.raw.try_admit(1024).unwrap();

    let bytes = frame_bytes(1, 16);
    let mut header_context = tabled_context(None, 128);
    header_context.resources = headers;
    let (_peer_send, _peer_recv, send, recv) = link.open(&bytes).await?;
    let (header_worker, mut header_rx, _header_tx, header_cancel) =
        spawn_stream_worker(send, recv, header_context);
    let mut block_context = tabled_context(None, 128);
    block_context.resources = stream_of(&connection, BLOCKS.kind);
    let (_block_peer_send, _block_peer_recv, send, recv) = link.open(&bytes).await?;
    let (block_worker, mut block_rx, _block_tx, block_cancel) =
        spawn_stream_worker(send, recv, block_context);

    let block = timeout(TEST_TIMEOUT, block_rx.recv())
        .await?
        .ok_or("the block frame passes its own gate")?;
    assert_eq!(block.frame.payload, vec![1; 16]);
    assert!(
        timeout(HELD, header_rx.recv()).await.is_err(),
        "the header frame waits on its full gate"
    );
    drop(full);
    timeout(TEST_TIMEOUT, header_rx.recv())
        .await?
        .ok_or("the freed header gate admits the frame")?;

    header_cancel.cancel();
    block_cancel.cancel();
    timeout(TEST_TIMEOUT, header_worker).await??;
    timeout(TEST_TIMEOUT, block_worker).await??;
    link.close().await;
    Ok(())
}

/// A request service that answers every request with `frames` frames.
#[derive(Debug)]
struct AnsweringRequests {
    streams: [Stream; 2],
    frames: usize,
}

impl Service for AnsweringRequests {
    fn name(&self) -> &'static str {
        "answering-requests"
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

impl RequestResponseService for AnsweringRequests {
    fn request_frame<'a>(
        &'a self,
        _peer_id: ZakuraPeerId,
        _stream_kind: u16,
        _request_id: u64,
        _max_frame_bytes: u32,
        _max_message_bytes: u32,
        _frame: Frame,
    ) -> BoxRunFuture<'a, Result<Vec<Frame>, SinkReject>> {
        let frames = (0..self.frames)
            .map(|_| Frame {
                message_type: 2,
                flags: 0,
                payload: vec![2; 1000],
            })
            .collect();
        Box::pin(async move { Ok(frames) })
    }
}

/// Open a request stream for `prelude` on `link`, and start its worker on
/// `connection`'s gates, with `output` in place of the kind's output gate.
async fn spawn_request(
    link: &mut Link,
    connection: &ConnectionResources,
    registry: &Arc<ServiceRegistry>,
    prelude: StreamPrelude,
    output: Option<FrameGate>,
) -> Result<(SendStream, RecvStream, AbortOnDropHandle<()>), BoxError> {
    let mut context = tabled_context(None, 128);
    context.resources = stream_of(connection, prelude.stream_kind);
    if let Some(output) = output {
        context.resources.output = output;
    }
    let (peer_send, peer_recv, send, recv) = link.open(&frame_bytes(1, 16)).await?;
    let worker = AbortOnDropHandle::new(tokio::spawn(request_stream_worker(
        send,
        recv,
        prelude,
        context,
        registry.clone(),
    )));
    Ok((peer_send, peer_recv, worker))
}

/// A peer that stops reading a long response strands at most its kind's
/// output share. The stalled response holds its bytes of the gate, so the kind's
/// next response, which reserves the whole share, waits, while another kind's
/// response goes out. Ending the stalled stream returns the share. The test link
/// carries two streams.
#[tokio::test]
async fn a_stalled_response_strands_at_most_its_kinds_output_share() -> Result<(), BoxError> {
    const SLOW: Stream = test_stream(910, StreamMode::RequestResponse);
    const FAST: Stream = test_stream(911, StreamMode::RequestResponse);
    const OUTPUT_SHARE: u64 = 512 * 1024;
    const RESPONSE_FRAMES: usize = 400;
    // The peer's small stream window stalls a response of 400 frames.
    let peer = LocalEndpointFactory::new().quic_config(QuicConfig {
        stream_receive_window_bytes: 64 * 1024,
        receive_window_bytes: 128 * 1024,
        ..QuicConfig::default()
    });
    let mut link = Link::connect_with(peer).await?;
    let connection = connection_gates(&[SLOW, FAST], 1 << 20, OUTPUT_SHARE);
    let registry = Arc::new(ServiceRegistry::new(vec![Arc::new(AnsweringRequests {
        streams: [SLOW, FAST],
        frames: RESPONSE_FRAMES,
    })])?);
    let response_bytes = u64::try_from(RESPONSE_FRAMES)? * response_frame_bytes(1000);
    assert!(response_bytes <= OUTPUT_SHARE);
    // The slow kind's gate, over a budget the test reads.
    let slow_output = OutputByteBudget::new(OUTPUT_SHARE)?;
    let slow_gate = FrameGate::over(slow_output.clone());
    let (_stalled_send, mut stalled_recv, stalled) = spawn_request(
        &mut link,
        &connection,
        &registry,
        prelude_of(SLOW.kind, Some(1)),
        Some(slow_gate.clone()),
    )
    .await?;
    timeout(TEST_TIMEOUT, async {
        while slow_output.granted() != response_bytes {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    tokio::time::sleep(HELD).await;
    assert!(
        !stalled.is_finished(),
        "the peer's window stalls the response"
    );
    assert_eq!(
        slow_output.granted(),
        response_bytes,
        "it strands its own bytes"
    );

    assert!(
        slow_gate.try_admit(OUTPUT_SHARE).is_none(),
        "the kind's next response waits for its gate"
    );
    let (_fast_send, mut fast_recv, fast) = spawn_request(
        &mut link,
        &connection,
        &registry,
        prelude_of(FAST.kind, Some(3)),
        None,
    )
    .await?;
    let answer = timeout(TEST_TIMEOUT, fast_recv.read_to_end(usize::MAX)).await??;
    assert_eq!(
        answer.len(),
        RESPONSE_FRAMES * 1008,
        "another kind answers in full"
    );
    timeout(TEST_TIMEOUT, fast).await??;

    stalled_recv.stop(0u32.into())?;
    timeout(TEST_TIMEOUT, stalled).await??;
    assert_eq!(slow_output.granted(), 0);
    assert!(slow_gate.try_admit(OUTPUT_SHARE).is_some());
    link.close().await;
    Ok(())
}

/// A custom kind's request has no derivable bound, so it reserves its whole output
/// share. A result above the share resets the stream before any write, instead of
/// going out under a charge of the share while it holds more.
#[tokio::test]
async fn an_oversize_custom_result_is_refused_not_charged_its_share() -> Result<(), BoxError> {
    const CUSTOM: Stream = test_stream(912, StreamMode::RequestResponse);
    const OTHER: Stream = test_stream(913, StreamMode::RequestResponse);
    const OUTPUT_SHARE: u64 = 4096;
    let mut link = Link::connect().await?;
    let connection = connection_gates(&[CUSTOM, OTHER], 1 << 20, OUTPUT_SHARE);
    let registry = Arc::new(ServiceRegistry::new(vec![Arc::new(AnsweringRequests {
        streams: [CUSTOM, OTHER],
        frames: 400,
    })])?);
    let output = OutputByteBudget::new(OUTPUT_SHARE)?;
    let (_peer_send, mut peer_recv, worker) = spawn_request(
        &mut link,
        &connection,
        &registry,
        prelude_of(CUSTOM.kind, Some(1)),
        Some(FrameGate::over(output.clone())),
    )
    .await?;

    timeout(TEST_TIMEOUT, worker).await??;
    assert!(
        timeout(TEST_TIMEOUT, peer_recv.read_to_end(usize::MAX))
            .await?
            .is_err(),
        "the worker resets the stream instead of writing the result"
    );
    assert_eq!(output.granted(), 0);
    link.close().await;
    Ok(())
}

/// Cancelling the connection ends a response whose write waits on the peer's
/// send credit, and returns its output grant, long before the write timeout.
/// Teardown's drain then doesn't wait on the peer.
#[tokio::test]
async fn cancelling_the_connection_ends_a_stalled_response_write() -> Result<(), BoxError> {
    const SLOW: Stream = test_stream(912, StreamMode::RequestResponse);
    const OTHER: Stream = test_stream(913, StreamMode::RequestResponse);
    // The peer's small stream window stalls a response of 400 frames.
    let peer = LocalEndpointFactory::new().quic_config(QuicConfig {
        stream_receive_window_bytes: 64 * 1024,
        receive_window_bytes: 128 * 1024,
        ..QuicConfig::default()
    });
    let mut link = Link::connect_with(peer).await?;
    let connection = connection_gates(&[SLOW, OTHER], 1 << 20, 1 << 20);
    let registry = Arc::new(ServiceRegistry::new(vec![Arc::new(AnsweringRequests {
        streams: [SLOW, OTHER],
        frames: 400,
    })])?);
    let output = OutputByteBudget::new(1 << 20)?;
    let mut context = tabled_context(None, 128);
    context.resources = stream_of(&connection, SLOW.kind);
    context.resources.output = FrameGate::over(output.clone());
    let cancel = context.connection_token.clone();
    let (_peer_send, _peer_recv, send, recv) = link.open(&frame_bytes(1, 16)).await?;
    let worker = AbortOnDropHandle::new(tokio::spawn(request_stream_worker(
        send,
        recv,
        prelude_of(SLOW.kind, Some(1)),
        context,
        registry,
    )));
    timeout(TEST_TIMEOUT, async {
        while output.granted() == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    tokio::time::sleep(HELD).await;
    assert!(
        !worker.is_finished(),
        "the peer's window stalls the response"
    );

    cancel.cancel();
    timeout(HELD, worker)
        .await
        .map_err(|_| "cancellation ends the stalled write")??;
    assert_eq!(output.granted(), 0);
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

/// One link's legacy request workers, which share their kind's output gate, as a
/// connection's streams do.
struct LegacyRequests {
    link: Link,
    output: OutputByteBudget,
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
            output: OutputByteBudget::new(output_bytes)?,
            raw,
            raw_budget,
            registry,
            started,
            release,
            held: Vec::new(),
        })
    }

    fn output_gate(&self) -> FrameGate {
        FrameGate::over(self.output.clone())
    }

    /// Start a worker on a new stream for this request, with its cancellation.
    async fn start(
        &mut self,
        message_type: u16,
        payload: Vec<u8>,
    ) -> Result<(AbortOnDropHandle<()>, CancellationToken), BoxError> {
        self.start_capped(message_type, payload, 1024).await
    }

    /// Start a worker for this request whose peer accepts response frames of at most
    /// `frame_cap` bytes.
    async fn start_capped(
        &mut self,
        message_type: u16,
        payload: Vec<u8>,
        frame_cap: u32,
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
        context.outbound_frame_cap = frame_cap;
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
        u64::MAX,
    )
    .expect("an unlimited gate fits every bound")
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
        .admit(OUTPUT)
        .now_or_never()
        .ok_or("an idle budget funds the charge")?;
    let frame_bytes = 8 + one_block_request().len() as u64;

    let (worker, _cancel) = requests
        .start(LEGACY_REQUEST_BLOCKS_BY_HASH, one_block_request())
        .await?;
    requests.until_raw(frame_bytes).await?;
    tokio::time::sleep(HELD).await;
    assert_eq!(requests.calls(), 0, "an unfunded request does not execute");
    assert_eq!(requests.output.granted(), OUTPUT);
    assert_eq!(
        requests.raw_budget.granted(),
        frame_bytes,
        "the waiting request keeps its raw frame's funding"
    );

    drop(saturating);
    requests.until_calls(1).await?;
    assert_eq!(
        requests.output.granted(),
        one_block_bound().bytes,
        "the call runs under its whole reservation"
    );

    requests.release.add_permits(1);
    timeout(TEST_TIMEOUT, worker).await??;
    assert_eq!(requests.output.granted(), 0);
    assert_eq!(requests.raw_budget.granted(), 0);
    requests.link.close().await;
    Ok(())
}

/// A ping runs in the room a large reservation leaves, and on a full gate it waits
/// for that room.
#[tokio::test]
async fn a_small_request_runs_beside_a_large_reservation() -> Result<(), BoxError> {
    const OUTPUT: u64 = 8 << 20;
    const LEFT: u64 = 1024;
    let mut requests = LegacyRequests::connect(OUTPUT, pong()).await?;
    requests.release.add_permits(2);

    let large = requests
        .output_gate()
        .admit(OUTPUT - LEFT)
        .now_or_never()
        .ok_or("an idle budget funds the reservation")?;
    let (beside, _cancel) = requests.start(LEGACY_REQUEST_PING, Vec::new()).await?;
    timeout(TEST_TIMEOUT, beside).await??;
    assert_eq!(requests.calls(), 1);
    assert_eq!(requests.output.granted(), OUTPUT - LEFT);

    let rest = requests
        .output_gate()
        .admit(LEFT)
        .now_or_never()
        .ok_or("the reservation left this room")?;
    let (waiting, _cancel) = requests.start(LEGACY_REQUEST_PING, Vec::new()).await?;
    tokio::time::sleep(HELD).await;
    assert_eq!(
        requests.calls(),
        1,
        "a full gate holds the ping before its call"
    );
    assert_eq!(requests.output.granted(), OUTPUT);

    drop(rest);
    timeout(TEST_TIMEOUT, waiting).await??;
    assert_eq!(requests.calls(), 2);
    assert_eq!(requests.output.granted(), OUTPUT - LEFT);

    drop(large);
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
    assert_eq!(requests.output.granted(), reserved);
    cancel_running.cancel();
    timeout(TEST_TIMEOUT, running).await??;
    assert_eq!(requests.output.granted(), 0);
    assert_eq!(requests.raw_budget.granted(), 0);

    let saturating = requests
        .output_gate()
        .admit(OUTPUT)
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
    assert_eq!(requests.output.granted(), OUTPUT);
    assert_eq!(requests.raw_budget.granted(), 0);

    drop(saturating);
    assert_eq!(requests.output.granted(), 0);
    requests.link.close().await;
    Ok(())
}

/// A peer's small frame cap does not shrink a request's reservation. The mempool
/// encoder builds its whole one-frame answer before any write checks the cap, so
/// the call runs under the request's full bound, and a gate that cannot hold that
/// bound refuses the request before its service runs.
#[tokio::test]
async fn a_small_frame_cap_request_funds_its_full_bound_before_the_service() -> Result<(), BoxError>
{
    const SMALL_CAP: u32 = 64;
    let request = Frame {
        message_type: LEGACY_REQUEST_MEMPOOL_TRANSACTION_IDS,
        flags: 0,
        payload: Vec::new(),
    };
    let bound = response_bound(
        LEGACY_REQUEST_STREAM_KIND,
        &request,
        test_connection_limits(),
        SMALL_CAP,
        u64::MAX,
    )
    .ok_or("an unlimited gate fits every bound")?;
    assert!(
        bound.bytes > u64::from(SMALL_CAP),
        "the bound ignores the cap"
    );

    let mut requests = LegacyRequests::connect(8 << 20, Vec::new()).await?;
    let (worker, _cancel) = requests
        .start_capped(
            LEGACY_REQUEST_MEMPOOL_TRANSACTION_IDS,
            Vec::new(),
            SMALL_CAP,
        )
        .await?;
    requests.until_calls(1).await?;
    assert_eq!(
        requests.output.granted(),
        bound.bytes,
        "the call runs under the full bound"
    );
    requests.release.add_permits(1);
    timeout(TEST_TIMEOUT, worker).await??;
    assert_eq!(requests.output.granted(), 0);
    requests.link.close().await;

    let mut requests = LegacyRequests::connect(bound.bytes - 1, Vec::new()).await?;
    let (worker, _cancel) = requests
        .start_capped(
            LEGACY_REQUEST_MEMPOOL_TRANSACTION_IDS,
            Vec::new(),
            SMALL_CAP,
        )
        .await?;
    timeout(TEST_TIMEOUT, worker).await??;
    assert_eq!(
        requests.calls(),
        0,
        "a gate below the bound refuses the call"
    );
    assert_eq!(requests.output.granted(), 0);
    assert_eq!(requests.raw_budget.granted(), 0);
    requests.link.close().await;
    Ok(())
}

/// A peer's tiny frame cap would split one block into a frame per object byte. The
/// bound counts each frame's overhead, so it exceeds the kind's output share, and
/// the request is refused before its service runs.
#[tokio::test]
async fn a_tiny_frame_cap_block_request_is_refused_before_its_service() -> Result<(), BoxError> {
    // A frame header, a response chunk header, and one object byte.
    const TINY_CAP: u32 = 18;
    let share = LEGACY_REQUESTS_BUDGET.output;
    let request = Frame {
        message_type: LEGACY_REQUEST_BLOCKS_BY_HASH,
        flags: 0,
        payload: one_block_request(),
    };
    let limits = test_connection_limits();
    let unlimited = response_bound(
        LEGACY_REQUEST_STREAM_KIND,
        &request,
        limits,
        TINY_CAP,
        u64::MAX,
    )
    .ok_or("an unlimited gate fits every bound")?;
    assert!(
        unlimited.bytes > share,
        "the frames' overhead exceeds the share"
    );
    assert_eq!(
        response_bound(
            LEGACY_REQUEST_STREAM_KIND,
            &request,
            limits,
            TINY_CAP,
            share
        ),
        None
    );

    let mut requests = LegacyRequests::connect(share, Vec::new()).await?;
    let (worker, _cancel) = requests
        .start_capped(LEGACY_REQUEST_BLOCKS_BY_HASH, one_block_request(), TINY_CAP)
        .await?;
    timeout(TEST_TIMEOUT, worker).await??;
    assert_eq!(requests.calls(), 0, "the service never runs");
    assert_eq!(requests.output.granted(), 0);
    assert_eq!(requests.raw_budget.granted(), 0);
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
        response_bound(kind, &frame, limits, CAP, u64::MAX)
            .expect("an unlimited gate fits every bound")
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
                bytes: response_frame_bytes(8)
            }
        );
    }
    let hashes_bound = legacy(LEGACY_REQUEST_FIND_BLOCKS, Vec::new());
    assert!(hashes_bound.bytes <= response_frame_bytes(17 + 500 * 32));
    for one_frame in [
        LEGACY_REQUEST_FIND_HEADERS,
        LEGACY_REQUEST_MEMPOOL_TRANSACTION_IDS,
    ] {
        let bound = legacy(one_frame, Vec::new());
        assert!(
            bound.bytes > u64::from(CAP),
            "a one-frame bound ignores the frame cap"
        );
        assert!(bound.bytes <= response_frame_bytes(8 + MAX_PROTOCOL_MESSAGE_LEN));
    }

    let one_block = legacy(LEGACY_REQUEST_BLOCKS_BY_HASH, hashes(1));
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
    for (frame_cap, message_cap) in [
        (1 << 20, 4 << 20),
        (1 << 20, 64),
        (64, 1 << 20),
        (64, 64),
        (32, 1 << 20),
    ] {
        let mut limits = test_connection_limits();
        limits.max_message_bytes = message_cap;
        let mut transaction_request = vec![1];
        InventoryHash::from(transaction.id()).zcash_serialize(&mut transaction_request)?;
        let mut block_request = vec![2];
        block_request.extend_from_slice(&[7; 64]);
        for (message_type, payload, response) in [
            (
                LEGACY_REQUEST_BLOCKS_BY_HASH,
                block_request,
                Response::Blocks(vec![
                    InventoryResponse::Available((block.clone().into(), None)),
                    InventoryResponse::Missing(block.hash()),
                ]),
            ),
            (
                LEGACY_REQUEST_TRANSACTIONS_BY_ID,
                transaction_request,
                Response::Transactions(vec![InventoryResponse::Available((
                    transaction.clone(),
                    None,
                ))]),
            ),
        ] {
            let frame = Frame {
                message_type,
                flags: 0,
                payload,
            };
            let bound = response_bound(
                LEGACY_REQUEST_STREAM_KIND,
                &frame,
                limits,
                frame_cap,
                u64::MAX,
            )
            .ok_or("an unlimited gate fits every bound")?;
            let frames = LegacyResponseCodec::encode_response(7, response, frame_cap, message_cap)?;
            let bytes: u64 = frames
                .iter()
                .map(|frame| response_frame_bytes(frame.payload.len()))
                .sum();
            assert!(
                bytes <= bound.bytes,
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
    let stream = test_stream(900, StreamMode::Persistent);
    let node = NodeResources::new(
        ReactorShares::from_quic(&QuicConfig::default()),
        [stream],
        1024,
    );
    let connection = node.connection(stream.capability);
    let resources = stream_of(&connection, stream.kind);
    let setup = async move {
        let _resources = resources;
        future::pending::<()>().await
    };
    assert_eq!(node.owners().counts().setup, 1);
    drop(setup);
    assert_eq!(node.owners().counts().total(), 0);
}
