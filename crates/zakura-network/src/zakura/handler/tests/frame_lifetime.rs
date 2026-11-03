//! Real QUIC frames reach their stream workers' frame gates and worker slots.

use super::read_close_cause::Link;
use super::*;
use crate::zakura::{regulation::OutputByteBudget, RequestResponseService};

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
    let connection = connection_gates(&[HEADERS, BLOCKS], 64, 1024);
    let headers = stream_of(&connection, HEADERS.kind);
    let full = headers.raw.try_admit(64).unwrap();

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
/// output share. The stalled response holds the whole gate, so the kind's
/// next response waits, while another kind's response goes out. Ending the
/// stalled stream returns the share. The test link carries two streams.
#[tokio::test]
async fn a_stalled_response_strands_at_most_its_kinds_output_share() -> Result<(), BoxError> {
    const SLOW: Stream = test_stream(910, StreamMode::RequestResponse);
    const FAST: Stream = test_stream(911, StreamMode::RequestResponse);
    const OUTPUT_SHARE: u64 = 4096;
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
        frames: 400,
    })])?);
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
        while slow_output.granted() == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    tokio::time::sleep(HELD).await;
    assert!(
        !stalled.is_finished(),
        "the peer's window stalls the response"
    );
    assert_eq!(slow_output.granted(), OUTPUT_SHARE, "it strands the share");

    assert!(
        slow_gate.try_admit(1).is_none(),
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
    assert_eq!(answer.len(), 400 * 1008, "another kind answers in full");
    timeout(TEST_TIMEOUT, fast).await??;

    stalled_recv.stop(0u32.into())?;
    timeout(TEST_TIMEOUT, stalled).await??;
    assert_eq!(slow_output.granted(), 0);
    assert!(slow_gate.try_admit(OUTPUT_SHARE).is_some());
    link.close().await;
    Ok(())
}
