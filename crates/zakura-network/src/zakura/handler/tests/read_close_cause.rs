//! Frame-read errors record a close cause that tells peer faults from
//! timeouts and transport loss, without changing resets or metrics.

use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
};

use super::*;
use crate::zakura::{
    handler::trace::ConnectionTraceEvent,
    regulation::test_family::{message_type, RULES},
};

const ALPN: &[u8] = b"/zakura/testkit/read-close-cause/0";
const TEST_TIMEOUT: Duration = Duration::from_secs(10);
/// The idle timeout of the cases that stall.
const STALL: Duration = Duration::from_millis(300);
/// A message type no row of [`RULES`] declares.
const UNKNOWN_TYPE: u16 = 99;

/// How a peer breaks a frame read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Case {
    /// Send a header the stream's table rejects.
    Rejected,
    /// Declare a frame larger than the receiver's frame cap.
    OversizeFrame,
    /// End the stream with a FIN partway through a payload.
    FinishedEarly,
    /// Send half a header, then go silent.
    Timeout,
    /// Lose the connection after a frame header.
    Transport,
    /// Send a payload over the message cap (ordered admission).
    AdmissionOversize,
    /// Exceed the stream's message rate (ordered admission).
    RateLimited,
}

impl Case {
    const ORDERED: [Self; 7] = [
        Self::Rejected,
        Self::OversizeFrame,
        Self::FinishedEarly,
        Self::Timeout,
        Self::Transport,
        Self::AdmissionOversize,
        Self::RateLimited,
    ];

    /// The cause the node must record for this case on an ordered stream.
    fn ordered_cause(self) -> &'static str {
        match self {
            Self::Rejected | Self::OversizeFrame | Self::FinishedEarly => "ordered_read_rejected",
            Self::Timeout => "ordered_read_timeout",
            Self::Transport => "ordered_read_transport",
            Self::AdmissionOversize => "ordered_oversize",
            Self::RateLimited => "ordered_rate_limited",
        }
    }
}

/// What the node did after a read error.
#[derive(Debug)]
struct Outcome {
    cause: &'static str,
    /// The reset code the peer read, or `None` when the connection was gone.
    reset: Option<u32>,
    cancelled: bool,
}

fn header(message_type: u16, payload_len: u32) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&message_type.to_le_bytes());
    bytes.extend_from_slice(&0u16.to_le_bytes());
    bytes.extend_from_slice(&payload_len.to_le_bytes());
    bytes
}

fn status(value: u32) -> Vec<u8> {
    let mut bytes = header(message_type::STATUS, 4);
    bytes.extend_from_slice(&value.to_le_bytes());
    bytes
}

struct Link {
    server: QuicEndpoint,
    client: QuicEndpoint,
    connection: Connection,
    streams: mpsc::Receiver<(SendStream, RecvStream)>,
}

impl Link {
    async fn connect() -> Result<Self, BoxError> {
        let server = LocalEndpointFactory::new().endpoint(95101).await?;
        let (connection_tx, _connections) = mpsc::channel(1);
        let (stream_tx, streams) = mpsc::channel(2);
        server.serve(CaptureConnection {
            alpn: ALPN,
            connection_tx,
            stream_tx,
        })?;
        let client = LocalEndpointFactory::new().endpoint(95102).await?;
        let connection = timeout(
            TEST_TIMEOUT,
            client.connect(LocalEndpointFactory::node_addr(&server).await, &[ALPN]),
        )
        .await??;
        Ok(Self {
            server,
            client,
            connection,
            streams,
        })
    }

    /// Open a stream, send `bytes` so the server sees it, and accept it.
    async fn open(
        &mut self,
        bytes: &[u8],
    ) -> Result<(SendStream, RecvStream, SendStream, RecvStream), BoxError> {
        let (mut peer_send, peer_recv) = timeout(TEST_TIMEOUT, self.connection.open_bi()).await??;
        timeout(TEST_TIMEOUT, peer_send.write_all(bytes)).await??;
        let (send, recv) = timeout(TEST_TIMEOUT, self.streams.recv())
            .await?
            .ok_or("missing stream")?;
        Ok((peer_send, peer_recv, send, recv))
    }

    async fn close(self) {
        self.connection.close(0u32.into(), b"done");
        let _ = timeout(TEST_TIMEOUT, self.client.shutdown()).await;
        let _ = timeout(TEST_TIMEOUT, self.server.shutdown()).await;
    }
}

/// Read the reset code the node sent on the peer's receive half.
async fn read_reset(peer_recv: &mut RecvStream) -> Result<u32, BoxError> {
    let mut byte = [0; 1];
    match timeout(TEST_TIMEOUT, peer_recv.read_exact(&mut byte)).await? {
        Err(zakura_quic::ReadExactError::ReadError(zakura_quic::ReadError::Reset(code))) => {
            Ok(u32::try_from(code.into_inner())?)
        }
        other => Err(format!("expected a stream reset, got {other:?}").into()),
    }
}

/// Run one case against the production ordered stream worker.
async fn run_ordered(case: Case) -> Result<Outcome, BoxError> {
    let mut link = Link::connect().await?;
    let messages = match case {
        Case::OversizeFrame | Case::AdmissionOversize | Case::RateLimited => None,
        _ => Some(RULES),
    };
    let rate = if case == Case::RateLimited { 1 } else { 128 };
    let mut context = tabled_context(messages, rate);
    if case == Case::Timeout {
        context.limits.idle_timeout = STALL;
    }
    if case == Case::AdmissionOversize {
        context.limits.max_message_bytes = 8;
    }
    let close_cause = context.close_cause.clone();
    let cancel = context.connection_token.clone();

    let bytes = match case {
        Case::Rejected => header(UNKNOWN_TYPE, 4),
        // The test context's frame cap is 1024 bytes.
        Case::OversizeFrame => header(1, 2048),
        Case::FinishedEarly => [header(message_type::STATUS, 4), vec![0; 2]].concat(),
        Case::Timeout => header(message_type::STATUS, 4)[..3].to_vec(),
        Case::Transport => [status(1), header(message_type::STATUS, 4)].concat(),
        Case::AdmissionOversize => [header(1, 16), vec![0; 16]].concat(),
        Case::RateLimited => [header(1, 4), vec![0; 4], header(1, 4), vec![0; 4]].concat(),
    };
    let (mut peer_send, mut peer_recv, send, recv) = link.open(&bytes).await?;
    let prelude = StreamPrelude {
        magic: STREAM_PRELUDE_MAGIC,
        stream_kind: 900,
        stream_version: 1,
        request_id: None,
        max_frame_bytes: 1024,
    };
    let (inbound_tx, mut inbound_rx) = mpsc::channel(1);
    let (_outbound_tx, outbound_rx) = worker_framed_channel(1);
    let mut worker = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(
        persistent_stream_worker(send, recv, prelude, context, inbound_tx, outbound_rx, 1),
    ));
    match case {
        Case::FinishedEarly => peer_send.finish()?,
        Case::Transport => {
            // The complete frame proves the trailing header arrived with it.
            timeout(TEST_TIMEOUT, inbound_rx.recv())
                .await?
                .ok_or("missing frame")?;
            tokio::time::sleep(Duration::from_millis(100)).await;
            link.connection.close(0u32.into(), b"lost");
        }
        _ => {}
    }
    timeout(TEST_TIMEOUT, &mut worker).await??;
    let reset = if case == Case::Transport {
        None
    } else {
        Some(read_reset(&mut peer_recv).await?)
    };
    drop(inbound_rx);
    link.close().await;
    Ok(Outcome {
        cause: close_cause.get_or("unset"),
        reset,
        cancelled: cancel.is_cancelled(),
    })
}

/// How a peer breaks a request stream's request frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RequestCase {
    /// Send a response row as the request.
    Rejected,
    /// Send nothing after the prelude.
    Silent,
    /// Reset the stream after the prelude.
    Reset,
}

/// Run one case against the production request stream responder.
async fn run_request(case: RequestCase) -> Result<Outcome, BoxError> {
    let mut link = Link::connect().await?;
    let prelude = StreamPrelude {
        magic: STREAM_PRELUDE_MAGIC,
        stream_kind: 900,
        stream_version: 1,
        request_id: Some(42),
        max_frame_bytes: 1024,
    };
    let mut bytes = prelude.encode()?;
    if case == RequestCase::Rejected {
        bytes.extend(header(message_type::PART, 4));
    }
    let (mut peer_send, mut peer_recv, send, mut recv) = link.open(&bytes).await?;
    read_stream_prelude(&mut recv, TEST_TIMEOUT).await?;
    if case == RequestCase::Reset {
        peer_send.reset(0u32.into())?;
    }
    let mut context = tabled_context(Some(RULES), 128);
    if case == RequestCase::Silent {
        context.limits.idle_timeout = STALL;
    }
    let close_cause = context.close_cause.clone();
    let cancel = context.connection_token.clone();
    timeout(
        TEST_TIMEOUT,
        request_stream_worker(
            send,
            recv,
            prelude,
            context,
            Arc::new(ServiceRegistry::new(vec![])?),
        ),
    )
    .await?;
    let reset = read_reset(&mut peer_recv).await?;
    link.close().await;
    Ok(Outcome {
        cause: close_cause.get_or("unset"),
        reset: Some(reset),
        cancelled: cancel.is_cancelled(),
    })
}

#[test]
fn read_close_cause_maps_each_error() {
    use zakura_quic::{ConnectionError, ReadError, ReadExactError};

    let lost = || ReadExactError::ReadError(ReadError::ConnectionLost(ConnectionError::TimedOut));
    let cases: Vec<(ZakuraHandlerError, &str, &str)> = vec![
        (
            ZakuraHandlerError::RejectedFrame {
                message_type: UNKNOWN_TYPE,
                rejection: FrameRejection::UnknownMessageType,
            },
            "ordered_read_rejected",
            "request_read_rejected",
        ),
        (
            ZakuraHandlerError::OversizeFrame {
                payload_len: 2048,
                frame_len: 2056,
                max_frame_bytes: 1024,
            },
            "ordered_read_rejected",
            "request_read_rejected",
        ),
        (
            ZakuraHandlerError::QuicRead(ReadExactError::FinishedEarly(2)),
            "ordered_read_rejected",
            "request_read_rejected",
        ),
        (
            ZakuraHandlerError::Timeout("frame header"),
            "ordered_read_timeout",
            "request_read_timeout",
        ),
        (
            ZakuraHandlerError::Timeout("frame payload"),
            "ordered_read_timeout",
            "request_read_timeout",
        ),
        (
            ZakuraHandlerError::QuicRead(lost()),
            "ordered_read_transport",
            "request_read_transport",
        ),
        (
            ZakuraHandlerError::QuicRead(ReadExactError::ReadError(ReadError::ClosedStream)),
            "ordered_read_transport",
            "request_read_transport",
        ),
        (
            ZakuraHandlerError::Io(std::io::Error::other("local buffer")),
            "ordered_read_transport",
            "request_read_transport",
        ),
        // The ordered path never records `Closed`; see `read_close_cause`.
        (
            ZakuraHandlerError::Closed,
            "ordered_read_transport",
            "request_read_transport",
        ),
    ];
    for (error, ordered, request) in &cases {
        assert_eq!(
            read_close_cause(ReadPath::Ordered, error),
            *ordered,
            "{error:?}"
        );
        assert_eq!(
            read_close_cause(ReadPath::Request, error),
            *request,
            "{error:?}"
        );
    }
}

#[tokio::test]
async fn ordered_reader_records_rejection_cause() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    for case in [Case::Rejected, Case::OversizeFrame, Case::FinishedEarly] {
        assert_eq!(
            run_ordered(case).await?.cause,
            "ordered_read_rejected",
            "{case:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn ordered_reader_records_timeout_cause() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    assert_eq!(
        run_ordered(Case::Timeout).await?.cause,
        "ordered_read_timeout"
    );
    Ok(())
}

#[tokio::test]
async fn ordered_reader_records_transport_cause() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    assert_eq!(
        run_ordered(Case::Transport).await?.cause,
        "ordered_read_transport"
    );
    Ok(())
}

#[tokio::test]
async fn request_reader_records_each_cause() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    for (case, cause) in [
        (RequestCase::Rejected, "request_read_rejected"),
        (RequestCase::Silent, "request_read_timeout"),
        (RequestCase::Reset, "request_read_transport"),
    ] {
        assert_eq!(run_request(case).await?.cause, cause, "{case:?}");
    }
    Ok(())
}

/// The reader records right after forwarding, so it records first on either
/// runtime, and the worker's record is a no-op here.
/// `ordered_worker_closes_each_forwarded_error` pins the worker's cause.
#[test]
fn reader_and_worker_record_one_cause() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let current = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let multi = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    for runtime in [&current, &multi] {
        for case in Case::ORDERED {
            let outcome = runtime.block_on(run_ordered(case))?;
            assert_eq!(outcome.cause, case.ordered_cause(), "{case:?}");
        }
    }
    Ok(())
}

/// The worker's close for each error its reader forwards: the reset code and
/// the cause it records. The reader wins the race to record in the tests
/// above, so only this test pins the worker's side.
#[test]
fn ordered_worker_closes_each_forwarded_error() {
    use zakura_quic::{ConnectionError, ReadError, ReadExactError};

    let lost = || ReadExactError::ReadError(ReadError::ConnectionLost(ConnectionError::TimedOut));
    let cases = [
        (
            ZakuraHandlerError::Oversize,
            Some((ZAKURA_CLOSE_OVERSIZE, "ordered_oversize")),
        ),
        (
            ZakuraHandlerError::RateLimited,
            Some((ZAKURA_CLOSE_RATE_LIMIT, "ordered_rate_limited")),
        ),
        (
            ZakuraHandlerError::RejectedFrame {
                message_type: UNKNOWN_TYPE,
                rejection: FrameRejection::UnknownMessageType,
            },
            Some((ZAKURA_CLOSE_BAD_PRELUDE, "ordered_read_rejected")),
        ),
        (
            ZakuraHandlerError::Timeout("frame header"),
            Some((ZAKURA_CLOSE_BAD_PRELUDE, "ordered_read_timeout")),
        ),
        (
            ZakuraHandlerError::QuicRead(lost()),
            Some((ZAKURA_CLOSE_BAD_PRELUDE, "ordered_read_transport")),
        ),
        (ZakuraHandlerError::Closed, None),
    ];
    for (error, close) in &cases {
        assert_eq!(ordered_read_close(error), *close, "{error:?}");
    }
}

/// Read errors reset and cancel as on `main`. This pins the current-thread
/// ordering of the ordered path: the reader cancels the connection before the
/// worker resumes, so the worker's biased select takes that cancellation and
/// the peer reads the neutral reset. On a multi-thread runtime the worker can
/// receive the error first and send its own reset code, and that first reset
/// stands.
#[tokio::test]
async fn read_errors_keep_reset_codes() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    for case in Case::ORDERED {
        let outcome = run_ordered(case).await?;
        // A lost connection carries no stream reset to observe.
        let expected = (case != Case::Transport).then_some(ZAKURA_CLOSE_NEUTRAL);
        assert_eq!(outcome.reset, expected, "{case:?}");
        assert!(outcome.cancelled, "{case:?}");
    }
    for case in [
        RequestCase::Rejected,
        RequestCase::Silent,
        RequestCase::Reset,
    ] {
        let outcome = run_request(case).await?;
        assert_eq!(outcome.reset, Some(ZAKURA_CLOSE_BAD_PRELUDE), "{case:?}");
        assert!(outcome.cancelled, "{case:?}");
    }
    Ok(())
}

/// Counts every counter increment by name and label set.
#[derive(Default)]
struct CounterRecorder(Mutex<BTreeMap<(String, Vec<(String, String)>), Arc<AtomicU64>>>);

impl CounterRecorder {
    /// The labels and counts recorded under `name`.
    fn counts(&self, name: &str) -> Vec<(Vec<(String, String)>, u64)> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|((key, _), _)| key == name)
            .map(|((_, labels), count)| (labels.clone(), count.load(Ordering::Relaxed)))
            .collect()
    }
}

impl metrics::Recorder for CounterRecorder {
    fn describe_counter(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn describe_gauge(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn describe_histogram(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        let labels = key
            .labels()
            .map(|label| (label.key().to_owned(), label.value().to_owned()))
            .collect();
        let count = self
            .0
            .lock()
            .unwrap()
            .entry((key.name().to_owned(), labels))
            .or_default()
            .clone();
        metrics::Counter::from_arc(count)
    }
    fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::noop()
    }
    fn register_histogram(
        &self,
        _: &metrics::Key,
        _: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

#[test]
fn read_close_causes_stay_trace_compatible() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    // Each new cause keeps the old prefix and lands in the same row field.
    let mut causes: Vec<_> = Case::ORDERED
        .iter()
        .filter(|case| !matches!(case, Case::AdmissionOversize | Case::RateLimited))
        .map(|case| case.ordered_cause())
        .collect();
    causes.extend([
        "request_read_rejected",
        "request_read_timeout",
        "request_read_transport",
    ]);
    for cause in causes {
        assert!(
            cause.starts_with("ordered_read_") || cause.starts_with("request_read_"),
            "{cause}"
        );
        let row = serde_json::to_value(
            ConnectionTraceEvent::new("closed.neutral", 1, None).reason(cause),
        )?;
        assert_eq!(row["event"], "closed.neutral");
        assert_eq!(row["reason"], cause);
    }

    // The read paths emit the same counters, under the same labels.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    for case in Case::ORDERED {
        let recorder = CounterRecorder::default();
        let local = metrics::set_default_local_recorder(&recorder);
        runtime.block_on(run_ordered(case))?;
        drop(local);
        let rejected = recorder.counts("zakura.p2p.frame.rejected");
        let expected = if case == Case::Rejected {
            vec![(
                vec![("reason".to_owned(), "unknown_message_type".to_owned())],
                1,
            )]
        } else {
            vec![]
        };
        assert_eq!(rejected, expected, "{case:?}");
        let oversize = recorder.counts("zakura.p2p.ratelimit.frame.oversize");
        let expected = if case == Case::OversizeFrame {
            vec![(vec![], 1)]
        } else {
            vec![]
        };
        assert_eq!(oversize, expected, "{case:?}");
        // The connection loop owns this counter, not the read paths.
        assert_eq!(
            recorder.counts("zakura.p2p.conn.closed.neutral"),
            vec![],
            "{case:?}"
        );
    }
    Ok(())
}
