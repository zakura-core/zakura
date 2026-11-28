//! Frame-read failures distinguish service stalls from connection closes.

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

/// A fault of `row` on the test stream, kind 900 at version 1.
fn fault(row: CloseRow) -> CloseOutcome {
    CloseOutcome::fault(900, 1, row)
}

/// A neutral transport failure at `detail`.
fn transport(detail: &'static str) -> CloseOutcome {
    CloseOutcome::neutral(NeutralCause::IoFailure, detail)
}

/// How a peer breaks a frame read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Case {
    /// Send a header the stream's table rejects.
    Rejected,
    /// Declare a frame larger than the receiver's frame cap.
    OversizeFrame,
    /// End the stream with a FIN partway through a payload.
    FinishedEarly,
    /// Start a funded payload, then go silent.
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

    /// The outcome the node must record for this case on an ordered stream.
    fn ordered_cause(self) -> Option<CloseOutcome> {
        match self {
            Self::Rejected => fault(CloseRow::REG_CLOSE_2),
            Self::OversizeFrame => fault(CloseRow::REG_CLOSE_1),
            Self::FinishedEarly | Self::AdmissionOversize => fault(CloseRow::CONN_CLOSE_18),
            Self::Timeout => return None,
            Self::Transport => transport("ordered_read_transport"),
            Self::RateLimited => CloseOutcome::ORDERED_RATE_LIMITED,
        }
        .into()
    }
}

/// What the node did after a read error.
#[derive(Debug)]
struct Outcome {
    cause: Option<CloseOutcome>,
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

pub(super) struct Link {
    server: QuicEndpoint,
    client: QuicEndpoint,
    pub(super) connection: Connection,
    streams: mpsc::Receiver<(SendStream, RecvStream)>,
}

impl Link {
    pub(super) async fn connect() -> Result<Self, BoxError> {
        Self::connect_with(LocalEndpointFactory::new()).await
    }

    /// [`Self::connect`] with `client` building the peer's endpoint.
    pub(super) async fn connect_with(client: LocalEndpointFactory) -> Result<Self, BoxError> {
        Self::connect_pair(LocalEndpointFactory::new(), client).await
    }

    /// [`Self::connect`] with custom factories for both endpoints.
    pub(super) async fn connect_pair(
        server: LocalEndpointFactory,
        client: LocalEndpointFactory,
    ) -> Result<Self, BoxError> {
        let server = server.endpoint(95101).await?;
        let (connection_tx, _connections) = mpsc::channel(1);
        let (stream_tx, streams) = mpsc::channel(2);
        server.serve(CaptureConnection {
            alpn: ALPN,
            connection_tx,
            stream_tx,
        })?;
        let client = client.endpoint(95102).await?;
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
    pub(super) async fn open(
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

    pub(super) async fn close(self) {
        self.connection.close(0u32.into(), b"done");
        let _ = timeout(TEST_TIMEOUT, self.client.shutdown()).await;
        let _ = timeout(TEST_TIMEOUT, self.server.shutdown()).await;
    }

    /// Exchange a frame with a sibling worker on the existing QUIC connection.
    pub(super) async fn check_sibling(
        &mut self,
        cancel: &CancellationToken,
    ) -> Result<(), BoxError> {
        let (_peer_send, mut peer_recv, send, recv) = self.open(&status(2)).await?;
        let mut context = tabled_context(Some(RULES), 128);
        context.connection_token = cancel.clone();
        context.stream_token = cancel.child_token();
        context.kind_version = (901, 1);
        let prelude = StreamPrelude {
            magic: STREAM_PRELUDE_MAGIC,
            stream_kind: 901,
            stream_version: 1,
            request_id: None,
            max_frame_bytes: 1024,
        };
        let (inbound_tx, mut inbound_rx) = mpsc::channel(1);
        let (outbound_tx, outbound_rx) = worker_framed_channel(1);
        let _worker = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(
            persistent_stream_worker(send, recv, prelude, context, inbound_tx, outbound_rx, 1),
        ));
        let admitted = timeout(TEST_TIMEOUT, inbound_rx.recv())
            .await?
            .ok_or("sibling EOF")?;
        outbound_tx.try_send(admitted.frame)?;
        let mut echoed = [0; FRAME_HEADER_BYTES + 4];
        timeout(TEST_TIMEOUT, peer_recv.read_exact(&mut echoed)).await??;
        assert_eq!(echoed.as_slice(), status(2));
        Ok(())
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
    let session = context.stream_token.clone();
    let gate = context.resources.raw.clone();
    let failure = OrderedStreamFailureCause::default();

    let bytes = match case {
        Case::Rejected => header(UNKNOWN_TYPE, 4),
        // The test context's frame cap is 1024 bytes.
        Case::OversizeFrame => header(1, 2048),
        Case::FinishedEarly => [header(message_type::STATUS, 4), vec![0; 2]].concat(),
        Case::Timeout => [header(message_type::STATUS, 4), vec![0; 2]].concat(),
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
        persistent_stream_worker_with_policy(
            send,
            recv,
            prelude,
            context,
            inbound_tx,
            outbound_rx,
            1,
            Some(failure.clone()),
            None,
        ),
    ));
    match case {
        Case::Timeout => {
            timeout(STALL, async {
                while gate.try_admit(gate.capacity()).is_some() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await?;
        }
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
    if case == Case::Timeout {
        assert!(session.is_cancelled());
        assert_eq!(failure.get(), Some(OrderedStreamFailure::ReadTimeout));
        assert!(gate.try_admit(gate.capacity()).is_some());
        link.check_sibling(&cancel).await?;
    }
    drop(inbound_rx);
    link.close().await;
    Ok(Outcome {
        cause: close_cause.get(),
        reset,
        cancelled: cancel.is_cancelled(),
    })
}

/// How a peer breaks a request stream's request frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RequestCase {
    /// Send a response row as the request.
    Rejected,
    /// Start a funded request payload, then go silent.
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
    match case {
        RequestCase::Rejected => bytes.extend(header(message_type::PART, 4)),
        RequestCase::Silent => bytes.extend([header(message_type::GET, 4), vec![0; 2]].concat()),
        RequestCase::Reset => {}
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
    let gate = context.resources.raw.clone();
    let worker = tokio::spawn(request_stream_worker(
        send,
        recv,
        prelude,
        context,
        Arc::new(ServiceRegistry::new(vec![])?),
    ));
    if case == RequestCase::Silent {
        // The header funds the payload, so the gate holds its bytes.
        timeout(TEST_TIMEOUT, async {
            while gate.try_admit(gate.capacity()).is_some() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;
    }
    timeout(TEST_TIMEOUT, worker).await??;
    let reset = read_reset(&mut peer_recv).await?;
    if case == RequestCase::Silent {
        assert!(gate.try_admit(gate.capacity()).is_some());
        link.check_sibling(&cancel).await?;
    }
    link.close().await;
    Ok(Outcome {
        cause: close_cause.get(),
        reset: Some(reset),
        cancelled: cancel.is_cancelled(),
    })
}

#[test]
fn read_close_outcome_maps_each_error() {
    use zakura_quic::{ConnectionError, ReadError, ReadExactError};

    let lost = || ReadExactError::ReadError(ReadError::ConnectionLost(ConnectionError::TimedOut));
    let ordered_transport = transport("ordered_read_transport");
    let request_transport = transport("request_read_transport");
    let cases: Vec<(ZakuraHandlerError, CloseOutcome, CloseOutcome)> = vec![
        (
            ZakuraHandlerError::RejectedFrame {
                message_type: UNKNOWN_TYPE,
                rejection: FrameRejection::UnknownMessageType,
            },
            fault(CloseRow::REG_CLOSE_2),
            fault(CloseRow::REG_CLOSE_2),
        ),
        (
            ZakuraHandlerError::RejectedFrame {
                message_type: message_type::PART,
                rejection: FrameRejection::AboveOwed {
                    frames: 0,
                    bytes: 0,
                },
            },
            fault(CloseRow::REG_CLOSE_8),
            fault(CloseRow::REG_CLOSE_8),
        ),
        (
            ZakuraHandlerError::OversizeFrame {
                payload_len: 2048,
                frame_len: 2056,
                max_frame_bytes: 1024,
            },
            fault(CloseRow::REG_CLOSE_1),
            fault(CloseRow::REG_CLOSE_1),
        ),
        (
            ZakuraHandlerError::QuicRead(ReadExactError::FinishedEarly(2)),
            fault(CloseRow::CONN_CLOSE_18),
            fault(CloseRow::CONN_CLOSE_18),
        ),
        (
            ZakuraHandlerError::Oversize,
            fault(CloseRow::CONN_CLOSE_18),
            request_transport,
        ),
        (
            ZakuraHandlerError::ByteRateOverrun,
            fault(CloseRow::CONN_CLOSE_18),
            request_transport,
        ),
        (
            ZakuraHandlerError::RateLimited,
            CloseOutcome::ORDERED_RATE_LIMITED,
            request_transport,
        ),
        (
            ZakuraHandlerError::QuicRead(lost()),
            ordered_transport,
            request_transport,
        ),
        (
            ZakuraHandlerError::QuicRead(ReadExactError::ReadError(ReadError::ClosedStream)),
            ordered_transport,
            request_transport,
        ),
        (
            ZakuraHandlerError::Io(std::io::Error::other("local buffer")),
            ordered_transport,
            request_transport,
        ),
        // The ordered path never records `Closed`; see `read_close_outcome`.
        (
            ZakuraHandlerError::Closed,
            ordered_transport,
            request_transport,
        ),
    ];
    for (error, ordered, request) in &cases {
        assert_eq!(
            read_close_outcome(ReadPath::Ordered, (900, 1), error),
            *ordered,
            "{error:?}"
        );
        assert_eq!(
            read_close_outcome(ReadPath::Request, (900, 1), error),
            *request,
            "{error:?}"
        );
    }
}

#[tokio::test]
async fn ordered_reader_records_rejection_cause() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    for case in [Case::Rejected, Case::OversizeFrame, Case::FinishedEarly] {
        let cause = run_ordered(case).await?.cause;
        assert_eq!(cause, case.ordered_cause(), "{case:?}");
        assert!(cause.is_some_and(CloseOutcome::is_fault), "{case:?}");
    }
    Ok(())
}

#[tokio::test]
async fn ordered_read_timeout_releases_frame_and_keeps_sibling() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    assert_eq!(run_ordered(Case::Timeout).await?.cause, None);
    Ok(())
}

#[tokio::test]
async fn ordered_reader_records_transport_cause() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    assert_eq!(
        run_ordered(Case::Transport).await?.cause,
        Some(transport("ordered_read_transport"))
    );
    Ok(())
}

#[tokio::test]
async fn request_reader_records_each_cause() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    for (case, cause) in [
        (RequestCase::Rejected, Some(fault(CloseRow::REG_CLOSE_2))),
        (RequestCase::Silent, None),
        (
            RequestCase::Reset,
            Some(transport("request_read_transport")),
        ),
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
            Some((ZAKURA_CLOSE_OVERSIZE, fault(CloseRow::CONN_CLOSE_18))),
        ),
        (
            ZakuraHandlerError::RateLimited,
            Some((ZAKURA_CLOSE_RATE_LIMIT, CloseOutcome::ORDERED_RATE_LIMITED)),
        ),
        (
            ZakuraHandlerError::RejectedFrame {
                message_type: UNKNOWN_TYPE,
                rejection: FrameRejection::UnknownMessageType,
            },
            Some((ZAKURA_CLOSE_BAD_PRELUDE, fault(CloseRow::REG_CLOSE_2))),
        ),
        (ZakuraHandlerError::Timeout("frame header"), None),
        (
            ZakuraHandlerError::QuicRead(lost()),
            Some((
                ZAKURA_CLOSE_BAD_PRELUDE,
                transport("ordered_read_transport"),
            )),
        ),
        (ZakuraHandlerError::Closed, None),
    ];
    for (error, close) in &cases {
        assert_eq!(ordered_read_close((900, 1), error), *close, "{error:?}");
    }
}

/// Connection errors preserve their reset codes. This pins the current-thread
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
        assert_eq!(outcome.cancelled, case != Case::Timeout, "{case:?}");
    }
    for case in [
        RequestCase::Rejected,
        RequestCase::Silent,
        RequestCase::Reset,
    ] {
        let outcome = run_request(case).await?;
        let stalled = case == RequestCase::Silent;
        let code = if stalled {
            ZAKURA_CLOSE_NEUTRAL
        } else {
            ZAKURA_CLOSE_BAD_PRELUDE
        };
        assert_eq!(outcome.reset, Some(code), "{case:?}");
        assert_eq!(outcome.cancelled, !stalled, "{case:?}");
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
    // A neutral read close keeps its old label, and a fault traces its row,
    // in the same row field.
    let mut causes: Vec<_> = Case::ORDERED
        .iter()
        .filter(|case| !matches!(case, Case::AdmissionOversize | Case::RateLimited))
        .filter_map(|case| case.ordered_cause())
        .collect();
    causes.push(transport("request_read_transport"));
    for cause in causes {
        match cause {
            CloseOutcome::Fault { row, .. } => assert_eq!(cause.label(), row.id()),
            CloseOutcome::Neutral { detail, .. } => assert!(
                detail.starts_with("ordered_read_") || detail.starts_with("request_read_"),
                "{cause}"
            ),
        }
        let row = serde_json::to_value(
            ConnectionTraceEvent::new("closed.neutral", 1, None).reason(cause.label()),
        )?;
        assert_eq!(row["event"], "closed.neutral");
        assert_eq!(row["reason"], cause.label());
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
