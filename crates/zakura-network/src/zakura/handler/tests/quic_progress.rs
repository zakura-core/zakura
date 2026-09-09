//! Exercise the pinned transport, including paused bounded frame queues.

use super::*;
use crate::zakura::{
    AuxSchema, BlockSyncMessage, GetHeaders, HeaderSyncCodec, HeaderSyncMessage,
    ZakuraBlockSyncConfig,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio_util::task::AbortOnDropHandle;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bidirectional_transfers_exceed_flow_control_windows() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    const ALPN: &[u8] = b"/zakura/testkit/quic-progress/0";
    // Larger than the default send, stream, and connection windows. Both sides
    // must receive acknowledgements and new flow-control credit to finish.
    const BYTES: usize = 64 * 1024 * 1024;
    let limits = ZakuraLocalLimits::from_config(&Config::default());
    let server = LocalEndpointFactory::with_transport_config(limits.transport_config())
        .endpoint(92341)
        .await?;
    let client = LocalEndpointFactory::with_transport_config(limits.transport_config())
        .endpoint(92342)
        .await?;
    let (connection_tx, mut connection_rx) = mpsc::channel(1);
    let (stream_tx, mut stream_rx) = mpsc::channel(1);
    let router = Router::builder(server)
        .accept(
            ALPN,
            CaptureConnection {
                connection_tx,
                stream_tx,
            },
        )
        .spawn();
    let address = LocalEndpointFactory::node_addr(router.endpoint()).await;
    let connection = timeout(Duration::from_secs(10), client.connect(address, ALPN)).await??;
    let remote = timeout(Duration::from_secs(5), connection_rx.recv())
        .await?
        .unwrap();
    let (mut send_a, recv_a) = connection.open_bi().await?;
    // Make the stream visible to accept_bi before starting the bulk transfer.
    send_a.write_all(&[42]).await?;
    let (mut send_b, recv_b) = timeout(Duration::from_secs(5), stream_rx.recv())
        .await?
        .unwrap();

    let transfer = async {
        tokio::try_join!(
            async {
                send_a.write_all(&vec![42; BYTES]).await?;
                send_a.finish()?;
                Ok::<_, BoxError>(())
            },
            async {
                send_b.write_all(&vec![43; BYTES]).await?;
                send_b.finish()?;
                Ok::<_, BoxError>(())
            },
            drain_stream(recv_a, BYTES, 43),
            drain_stream(recv_b, BYTES + 1, 42),
        )?;
        Ok::<_, BoxError>(())
    };
    timeout(Duration::from_secs(30), transfer)
        .await
        .unwrap_or_else(|_| {
            panic!(
                "QUIC transfer stalled: local={:?}; remote={:?}",
                connection.stats(),
                remote.stats()
            )
        })?;
    connection.close(0u32.into(), b"done");
    client.close().await;
    router.shutdown().await?;
    Ok(())
}

async fn drain_stream(mut recv: RecvStream, expected: usize, byte: u8) -> Result<(), BoxError> {
    let mut buffer = vec![0; 64 * 1024];
    let mut received = 0;
    while let Some(count) = recv.read(&mut buffer).await? {
        assert!(buffer[..count].iter().all(|value| *value == byte));
        received += count;
        assert!(received <= expected);
    }
    assert_eq!(received, expected);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_way_transfer_with_no_paused_streams() -> Result<(), BoxError> {
    check_paused_stream_credit(0, None).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_way_transfer_with_one_paused_stream() -> Result<(), BoxError> {
    check_paused_stream_credit(1, None).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_paused_streams_exhaust_default_connection_credit() -> Result<(), BoxError> {
    check_paused_stream_credit(2, None).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn paused_frame_workers_exhaust_default_connection_credit() -> Result<(), BoxError> {
    check_paused_stream_credit(
        2,
        Some(usize::try_from(DEFAULT_ZAKURA_STREAM_RECEIVE_WINDOW).unwrap()),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(
    clippy::print_stderr,
    reason = "report the workload for standalone measurements"
)]
async fn advertised_requests_and_one_paused_service_leave_data_credit() -> Result<(), BoxError> {
    let requests = ZakuraBlockSyncConfig::default().advertised_max_inflight_requests();
    let bytes = usize::try_from(requests)? * get_blocks_frame()?.encode(MAX_BS_FRAME_BYTES)?.len();
    assert!(bytes < usize::try_from(DEFAULT_ZAKURA_STREAM_RECEIVE_WINDOW)?);
    eprintln!("advertised request probe: requests={requests}, request_bytes={bytes}");
    check_paused_stream_credit(2, Some(bytes)).await
}

/// Keep streams available to the test until it has established the data reader.
#[derive(Debug, Clone)]
struct CaptureCreditConnection(mpsc::Sender<Connection>);

impl ProtocolHandler for CaptureCreditConnection {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let _ = self.0.send(connection.clone()).await;
        connection.closed().await;
        Ok(())
    }
}

enum PausedReader {
    Raw(RecvStream),
    Framed {
        _recv: mpsc::Receiver<Frame>,
        _send: FramedSend,
        _worker: AbortOnDropHandle<()>,
    },
}

#[allow(
    clippy::print_stderr,
    reason = "report transfer measurements independently of log filtering"
)]
async fn check_paused_stream_credit(
    paused_streams: u64,
    framed_request_bytes: Option<usize>,
) -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    const ALPN: &[u8] = b"/zakura/testkit/paused-stream-credit/0";
    const BYTES: usize = 64 * 1024 * 1024;
    const DEADLINE: Duration = Duration::from_secs(30);
    assert!(paused_streams <= 2);
    let window = usize::try_from(DEFAULT_ZAKURA_STREAM_RECEIVE_WINDOW).unwrap();
    assert_eq!(
        DEFAULT_ZAKURA_RECEIVE_WINDOW,
        2 * DEFAULT_ZAKURA_STREAM_RECEIVE_WINDOW
    );
    let limits = ZakuraLocalLimits::from_config(&Config::default());
    let server = LocalEndpointFactory::with_transport_config(limits.transport_config())
        .endpoint(92400 + paused_streams)
        .await?;
    let client = LocalEndpointFactory::with_transport_config(limits.transport_config())
        .endpoint(92410 + paused_streams)
        .await?;
    let (connection_tx, mut connection_rx) = mpsc::channel(1);
    let router = Router::builder(server)
        .accept(ALPN, CaptureCreditConnection(connection_tx))
        .spawn();
    let address = LocalEndpointFactory::node_addr(router.endpoint()).await;
    let supplier = timeout(Duration::from_secs(10), client.connect(address, ALPN)).await??;
    let downloader = timeout(Duration::from_secs(5), connection_rx.recv())
        .await?
        .expect("the protocol handler publishes the accepted connection");

    // Open from A (downloader) so setup consumes none of A's receive credit.
    let (mut request_send, data_recv) = downloader.open_bi().await?;
    request_send.write_all(&[42]).await?;
    let (mut data_send, mut request_recv) =
        timeout(Duration::from_secs(5), supplier.accept_bi()).await??;
    let mut marker = [0];
    timeout(Duration::from_secs(5), request_recv.read_exact(&mut marker)).await??;
    assert_eq!(marker, [42]);

    let cancel = CancellationToken::new();
    let mut unread = Vec::new();
    for index in 0..paused_streams {
        let reader = if let Some(request_bytes) = framed_request_bytes {
            timeout(
                Duration::from_secs(10),
                fill_paused_frame_worker(
                    &supplier,
                    &downloader,
                    &limits,
                    &cancel,
                    index,
                    request_bytes,
                ),
            )
            .await??
        } else {
            PausedReader::Raw(
                timeout(
                    Duration::from_secs(10),
                    fill_unread_stream(&supplier, &downloader, window),
                )
                .await??,
            )
        };
        unread.push(reader);
    }

    let received = AtomicUsize::new(0);
    let transfer = async {
        tokio::try_join!(
            async {
                data_send.write_all(&vec![43; BYTES]).await?;
                data_send.finish()?;
                Ok::<_, BoxError>(())
            },
            async {
                let mut recv = data_recv;
                let mut buffer = vec![0; 64 * 1024];
                while let Some(count) = recv.read(&mut buffer).await? {
                    assert!(buffer[..count].iter().all(|value| *value == 43));
                    let total = received.fetch_add(count, Ordering::Relaxed) + count;
                    assert!(total <= BYTES);
                }
                assert_eq!(received.load(Ordering::Relaxed), BYTES);
                Ok::<_, BoxError>(())
            },
        )?;
        Ok::<_, BoxError>(())
    };
    tokio::pin!(transfer);
    let started = Instant::now();
    if paused_streams == 2 && framed_request_bytes.is_none_or(|bytes| bytes == window) {
        assert!(timeout(DEADLINE, &mut transfer).await.is_err());
        assert_eq!(
            received.load(Ordering::Relaxed),
            0,
            "a ready data reader cannot bypass exhausted connection receive credit"
        );
        assert!(
            !cancel.is_cancelled(),
            "the production readers remain healthy"
        );
        eprintln!("default-window probe: two paused streams, 0 data bytes after {DEADLINE:?}");

        // Retain the same transfer: releasing a sibling must restore progress
        // without resetting or reopening the stalled data stream.
        let sibling = unread.pop().expect("two paused streams were retained");
        timeout(DEADLINE, async {
            let release = async {
                match sibling {
                    PausedReader::Raw(recv) => drain_stream(recv, window, 17).await,
                    // Dropping the header worker stops its receive half and
                    // releases credit, without touching the data stream.
                    framed => {
                        drop(framed);
                        Ok(())
                    }
                }
            };
            tokio::try_join!(release, transfer.as_mut())?;
            Ok::<_, BoxError>(())
        })
        .await??;
    } else {
        timeout(DEADLINE, &mut transfer).await??;
    }
    assert_eq!(received.load(Ordering::Relaxed), BYTES);
    eprintln!(
        "default-window probe: paused_streams={paused_streams}, framed_request_bytes={framed_request_bytes:?}, received={BYTES}, elapsed={:?}",
        started.elapsed()
    );
    supplier.close(0u32.into(), b"done");
    client.close().await;
    router.shutdown().await?;
    Ok(())
}

/// Fill real bounded frame queues, then retain the remaining bytes inside QUIC.
/// The services are deliberately paused; this does not run their reactors or
/// establish that a well-behaved requester generates this volume of traffic.
async fn fill_paused_frame_worker(
    supplier: &Connection,
    downloader: &Connection,
    limits: &ZakuraLocalLimits,
    cancel: &CancellationToken,
    index: u64,
    request_bytes: usize,
) -> Result<PausedReader, BoxError> {
    let (kind, version, frame) = if index == 0 {
        (
            ZAKURA_STREAM_BLOCK_SYNC,
            ZAKURA_BLOCK_SYNC_STREAM_VERSION,
            get_blocks_frame()?,
        )
    } else {
        let codec = HeaderSyncCodec::new(
            Network::Mainnet,
            u32::try_from(MAX_HS_MESSAGE_BYTES).unwrap(),
            1,
            0,
        );
        (
            ZAKURA_STREAM_HEADER_SYNC,
            ZAKURA_HEADER_SYNC_STREAM_VERSION,
            codec.encode_frame(&HeaderSyncMessage::GetHeaders(GetHeaders {
                request_id: 1,
                target_tip_hash: block::Hash([1; 32]),
                locator_hashes: vec![block::Hash([0; 32])],
                max_header_count: 1,
                tree_aux_schema: AuxSchema::None,
            }))?,
        )
    };
    let frame_cap = if index == 0 {
        MAX_BS_FRAME_BYTES
    } else {
        u32::try_from(MAX_HS_MESSAGE_BYTES + FRAME_HEADER_BYTES)?
    };
    let encoded = frame.encode(frame_cap)?;
    let bytes = if index == 0 {
        request_bytes
    } else {
        usize::try_from(DEFAULT_ZAKURA_STREAM_RECEIVE_WINDOW)?
    };
    // The final partial frame remains unread. Only the first two complete,
    // valid frames reach parsing: one fills the queue, the next waits to enter.
    let burst: Vec<_> = encoded.iter().copied().cycle().take(bytes).collect();
    let (mut send, _recv) = supplier.open_bi().await?;
    send.write_all(&burst[..1]).await?;
    let (worker_send, worker_recv) = downloader.accept_bi().await?;
    let (inbound_tx, inbound_rx) = mpsc::channel(1);
    let (outbound_tx, outbound_rx) = worker_framed_channel(1);
    let (freshness_tx, _freshness_rx) = watch::channel(Instant::now());
    let block_sync = BlockSyncService::new(ZakuraBlockSyncConfig::default());
    let payload_limits = if index == 0 {
        block_sync.message_payload_limits(block_sync.streams()[0])
    } else {
        &[]
    };
    let context = StreamWorkerContext {
        queue_depths: None,
        session_resources: None,
        conn: ZakuraConnTrace::without_peer(1),
        peer_id: test_peer(73),
        stream_id: index + 2,
        _permit: Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
        limits: limits.clamp(&limits.initial_limits()),
        inbound_frame_cap: frame_cap,
        message_payload_limits: payload_limits,
        message_types: None,
        outbound_frame_cap: frame_cap,
        message_bucket: Arc::new(std::sync::Mutex::new(TokenBucket::new(
            limits.message_rate_per_second,
        ))),
        connection_token: cancel.clone(),
        stream_token: cancel.child_token(),
        close_cause: CloseCause::new(),
        freshness_tx,
    };
    let prelude = StreamPrelude {
        magic: STREAM_PRELUDE_MAGIC,
        stream_kind: kind,
        stream_version: version,
        request_id: None,
        max_frame_bytes: frame_cap,
    };
    let worker = AbortOnDropHandle::new(tokio::spawn(persistent_stream_worker(
        worker_send,
        worker_recv,
        prelude,
        context,
        inbound_tx,
        outbound_rx,
        1,
    )));
    send.write_all(&burst[1..]).await?;
    send.finish()?;
    assert!(send.stopped().await?.is_none());
    await_until(
        "the bounded frame queue fills",
        Duration::from_secs(5),
        || inbound_rx.len() == 1,
    )
    .await?;
    assert!(!cancel.is_cancelled());
    Ok(PausedReader::Framed {
        _recv: inbound_rx,
        _send: outbound_tx,
        _worker: worker,
    })
}

fn get_blocks_frame() -> Result<Frame, BoxError> {
    Ok(BlockSyncMessage::GetBlocks {
        start_height: block::Height(1),
        count: 1,
    }
    .encode_frame()?)
}

/// FIN acknowledgment proves QUIC received the bytes, while the application
/// retains them unread and therefore does not replenish connection credit.
async fn fill_unread_stream(
    supplier: &Connection,
    downloader: &Connection,
    bytes: usize,
) -> Result<RecvStream, BoxError> {
    let (mut send, _recv) = supplier.open_bi().await?;
    send.write_all(&[17]).await?;
    let (_send, recv) = downloader.accept_bi().await?;
    send.write_all(&vec![17; bytes - 1]).await?;
    send.finish()?;
    assert!(send.stopped().await?.is_none());
    Ok(recv)
}
