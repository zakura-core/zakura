//! The ordered stream worker reports each frame's read stages, its writes,
//! the subscriber precheck's verdicts and the close decision to an installed
//! [`TransportObserver`](crate::zakura::TransportObserver).

use std::{cell::Cell, sync::Mutex};

use super::read_close_cause::Link;
use super::*;
use crate::zakura::{
    header_sync::v9::{self, Cursor, RegtestMessage},
    regulation::{Publications, SharedSubscriptions, SubscriptionLimits, Subscriptions},
    transport::{sim_chain, transport_observe},
    ConnTap, Credit, HeaderCheck, RegulationTap, StreamTap, Transition, TransportEvent,
    TransportObserver, ZAKURA_STREAM_HEADER_SYNC,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a test waits for a worker that should have ended.
const END_WAIT: Duration = Duration::from_secs(3);
/// The connection ID the tests register.
const CONN: ZakuraConnId = 7;
/// The remote address of the tests' connection.
const REMOTE: SocketAddr = SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 9);

/// A connection tap reporting to `observer`, registered as [`CONN`].
fn registered_tap(observer: Arc<dyn TransportObserver>) -> Arc<ConnTap> {
    let tap = Arc::new(ConnTap::new(observer, REMOTE));
    tap.registered(CONN);
    tap
}

/// Records every event in order.
#[derive(Debug, Default)]
struct Recorder(Mutex<Vec<TransportEvent>>);

impl TransportObserver for Recorder {
    fn observe(&self, event: TransportEvent) {
        self.0
            .lock()
            .expect("no test panics while holding it")
            .push(event);
    }
}

impl Recorder {
    /// The events recorded so far.
    fn events(&self) -> Vec<TransportEvent> {
        self.0
            .lock()
            .expect("no test panics while holding it")
            .clone()
    }
}

/// The Regtest version 9 rows.
fn rows() -> &'static [MessageRule] {
    &RegtestMessage::ROWS
}

/// The `headers` row's largest payload.
fn largest_page() -> usize {
    MessageRule::find(rows(), v9::message_type::HEADERS)
        .expect("the table has a headers row")
        .payload
        .max()
}

/// A frame header for `message_type` that declares `payload_len` bytes.
fn header(message_type: u16, payload_len: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&message_type.to_le_bytes());
    bytes.extend_from_slice(&0u16.to_le_bytes());
    bytes.extend_from_slice(
        &u32::try_from(payload_len)
            .expect("the test payloads fit u32")
            .to_le_bytes(),
    );
    bytes
}

/// A subscriber table reporting through `tap`, with one subscription
/// holding the row's whole credit window, so any page the row allows fits it.
fn subscribed(tap: Option<RegulationTap<u64>>) -> SharedSubscriptions<u64, Cursor> {
    let limits = SubscriptionLimits::from_rule(&v9::SUBSCRIBE_HEADERS)
        .expect("SUBSCRIBE_HEADERS is a subscription row");
    let tools = SharedSubscriptions::new(Subscriptions::new(limits, rows()).observed_by(tap));
    tools
        .lock()
        .open(
            1,
            Credit {
                objects: v9::MAX_HS_PUSH_CREDIT_HEADERS,
                bytes: v9::MAX_HS_PUSH_CREDIT_BYTES,
            },
            Cursor {
                height: block::Height(0),
                hash: block::Hash([0; 32]),
                headers: 0,
                bytes: 0,
            },
        )
        .expect("the window fits the row's credit");
    tools
}

/// What one worker run recorded.
struct Run {
    events: Vec<TransportEvent>,
    /// Whether the worker ended within [`END_WAIT`].
    ended: bool,
    quic_stream: u64,
}

/// Runs the version 9 ordered stream worker with an observer, sends `bytes`
/// from the peer, then `outbound` from the service, and waits for the worker
/// to end. With `subscription`, the reader's precheck holds one subscription
/// with the row's whole credit.
async fn run_worker(
    bytes: &[u8],
    subscription: bool,
    outbound: Option<Frame>,
) -> Result<Run, BoxError> {
    let mut link = Link::connect().await?;
    let mut context = tabled_context(Some(rows()), 128);
    context.inbound_frame_cap = RegtestMessage::LAYOUT[0].frame_cap;
    let recorder = Arc::new(Recorder::default());
    let conn = registered_tap(recorder.clone());
    context.close_cause.observe_with(conn.clone());

    let (peer_send, _peer_recv, send, recv) = link.open(bytes).await?;
    let quic_stream = recv.id();
    assert_eq!(quic_stream, peer_send.id(), "both ends share the stream ID");
    let tap = Arc::new(StreamTap::new(conn, quic_stream));
    context.precheck.observe_with(tap.clone());
    if subscription {
        let tools = subscribed(Some(RegulationTap::new(tap.clone(), |id| *id)));
        context.precheck.attach(Arc::new(tools.admitting()));
    }
    let prelude = StreamPrelude {
        magic: STREAM_PRELUDE_MAGIC,
        stream_kind: ZAKURA_STREAM_HEADER_SYNC,
        stream_version: ZAKURA_HEADER_SYNC_STREAM_VERSION,
        request_id: None,
        max_frame_bytes: context.inbound_frame_cap,
    };
    let (inbound_tx, mut inbound_rx) = mpsc::channel(1);
    let (outbound_tx, outbound_rx) = worker_framed_channel(1);
    let cancel = context.connection_token.clone();
    let mut worker = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(
        persistent_stream_worker(send, recv, prelude, context, inbound_tx, outbound_rx, 1),
    ));
    if let Some(frame) = outbound {
        timeout(TEST_TIMEOUT, outbound_tx.send(frame)).await??;
        timeout(TEST_TIMEOUT, inbound_rx.recv())
            .await?
            .ok_or("the worker forwards the peer's frame")?;
        // The write's events follow its completion, which the peer's read
        // doesn't wait for, so wait for them here.
        timeout(TEST_TIMEOUT, async {
            while !recorder
                .events()
                .iter()
                .any(|event| matches!(event, TransportEvent::FrameWritten { .. }))
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        cancel.cancel();
    }
    let ended = timeout(END_WAIT, &mut worker).await.is_ok();
    drop(inbound_rx);
    drop(peer_send);
    link.close().await;
    Ok(Run {
        events: recorder.events(),
        ended,
        quic_stream,
    })
}

/// A page one byte over the row's maximum is refused by the row's length
/// guard alone: the precheck has credit for it and reserves it, the reader
/// never reads its payload, and the close cause is the reader's rejection.
///
/// On the wire, a page this size always exceeds the credit a subscriber
/// grants too, so only this unit test isolates the length guard.
#[tokio::test]
async fn the_length_guard_alone_closes_an_oversize_page() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let len = largest_page() + 1;
    let bytes = [header(v9::message_type::HEADERS, len), vec![0; len]].concat();
    let run = run_worker(&bytes, true, None).await?;
    let quic_stream = run.quic_stream;
    assert!(
        run.ended,
        "the refused page ends the worker: {:?}",
        run.events
    );
    assert_eq!(
        run.events,
        vec![
            TransportEvent::Regulated {
                conn_id: CONN,
                quic_stream,
                transition: Transition::OpenIssued {
                    subscription: 1,
                    objects: v9::MAX_HS_PUSH_CREDIT_HEADERS,
                    bytes: v9::MAX_HS_PUSH_CREDIT_BYTES,
                },
            },
            TransportEvent::HeaderRead {
                conn_id: CONN,
                quic_stream,
                seq: 0,
                message_type: v9::message_type::HEADERS,
                flags: 0,
                payload_len: u32::try_from(len)?,
            },
            TransportEvent::Regulated {
                conn_id: CONN,
                quic_stream,
                transition: Transition::HeaderChecked {
                    seq: 0,
                    outcome: HeaderCheck::Reserved {
                        objects: 1,
                        bytes: u64::try_from(len)?,
                    },
                },
            },
            TransportEvent::CloseDecided {
                remote: REMOTE,
                conn_id: Some(CONN),
                cause: Some("ordered_read_rejected"),
            },
        ]
    );
    Ok(())
}

/// A header over the stream's frame cap is read, then closes the
/// connection with the reader's rejection, before any payload is read.
#[tokio::test]
async fn oversize_header_is_read_then_closed() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let frame_cap = usize::try_from(RegtestMessage::LAYOUT[0].frame_cap)?;
    let bytes = header(v9::message_type::STATUS, frame_cap);
    let run = run_worker(&bytes, false, None).await?;
    assert!(run.ended, "the oversize header ends the worker");
    assert_eq!(
        run.events,
        vec![
            TransportEvent::HeaderRead {
                conn_id: CONN,
                quic_stream: run.quic_stream,
                seq: 0,
                message_type: v9::message_type::STATUS,
                flags: 0,
                payload_len: u32::try_from(frame_cap)?,
            },
            TransportEvent::CloseDecided {
                remote: REMOTE,
                conn_id: Some(CONN),
                cause: Some("ordered_read_rejected"),
            },
        ]
    );
    Ok(())
}

/// A whole frame gets both read stages, and a service frame gets both write
/// events, each numbered from 0 on its own direction of the stream.
#[tokio::test]
async fn worker_reports_each_frame_stage() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let status = MessageRule::find(rows(), v9::message_type::STATUS)
        .expect("the table has a status row")
        .payload
        .min();
    let bytes = [header(v9::message_type::STATUS, status), vec![0; status]].concat();
    let outbound = Frame {
        message_type: v9::message_type::STATUS,
        flags: 0,
        payload: vec![0; status],
    };
    let run = run_worker(&bytes, false, Some(outbound)).await?;
    let quic_stream = run.quic_stream;
    assert!(run.ended, "cancellation ends the worker");
    // The reader and the writer run independently, so only each direction's
    // own order is fixed.
    let (writes, reads): (Vec<_>, Vec<_>) = run.events.into_iter().partition(|event| {
        matches!(
            event,
            TransportEvent::FrameWriteStarted { .. } | TransportEvent::FrameWritten { .. }
        )
    });
    assert_eq!(
        reads,
        vec![
            TransportEvent::HeaderRead {
                conn_id: CONN,
                quic_stream,
                seq: 0,
                message_type: v9::message_type::STATUS,
                flags: 0,
                payload_len: u32::try_from(status)?,
            },
            TransportEvent::FrameRead {
                conn_id: CONN,
                quic_stream,
                seq: 0,
            },
        ]
    );
    assert_eq!(
        writes,
        vec![
            TransportEvent::FrameWriteStarted {
                conn_id: CONN,
                quic_stream,
                seq: 0,
                message_type: v9::message_type::STATUS,
            },
            TransportEvent::FrameWritten {
                conn_id: CONN,
                quic_stream,
                seq: 0,
                message_type: v9::message_type::STATUS,
            },
        ]
    );
    Ok(())
}

/// Only the first recorded cause is reported, and a cancellation after it
/// reports nothing more. The cause-free decisions need the simulator token's
/// hook, so `tests/sim_header_sync.rs` covers them.
#[test]
fn close_is_decided_once() {
    let recorder = Arc::new(Recorder::default());
    let cause = CloseCause::new();
    cause.observe_with(registered_tap(recorder.clone()));
    cause.record("first");
    cause.record("second");
    cause.cancelled_without_cause();
    cause.torn_down();
    assert_eq!(
        recorder.events(),
        vec![
            TransportEvent::CloseDecided {
                remote: REMOTE,
                conn_id: Some(CONN),
                cause: Some("first"),
            },
            TransportEvent::TornDown { conn_id: CONN },
        ]
    );
}

/// A registration refusal and a cause-free cancellation make one decision
/// between them, whichever comes first. A refusal after the cancellation
/// isn't the decision, and a cancellation after the refusal reports nothing.
#[test]
fn a_refusal_and_a_cancellation_decide_once() {
    let recorder = Arc::new(Recorder::default());
    let cause = CloseCause::new();
    cause.observe_with(Arc::new(ConnTap::new(recorder.clone(), REMOTE)));
    cause.cancelled_without_cause();
    let tap = cause.tap().expect("the tap is attached");
    assert!(!tap.refused(), "the cancellation decided first");
    assert_eq!(
        recorder.events(),
        vec![TransportEvent::CloseDecided {
            remote: REMOTE,
            conn_id: None,
            cause: None,
        }]
    );

    let recorder = Arc::new(Recorder::default());
    let cause = CloseCause::new();
    cause.observe_with(Arc::new(ConnTap::new(recorder.clone(), REMOTE)));
    let tap = cause.tap().expect("the tap is attached");
    assert!(tap.refused(), "the refusal is the decision");
    cause.cancelled_without_cause();
    assert_eq!(recorder.events(), vec![]);
}

/// Without an observer, `transport_observe!` never evaluates its event.
#[test]
fn no_observer_evaluates_nothing() {
    let evaluated = Cell::new(0);
    let event = || {
        evaluated.set(evaluated.get() + 1);
        TransportEvent::TornDown { conn_id: CONN }
    };
    let mut observer: Option<Arc<dyn TransportObserver>> = None;
    transport_observe!(observer, event());
    assert_eq!(evaluated.get(), 0);

    let recorder = Arc::new(Recorder::default());
    observer = Some(recorder.clone());
    transport_observe!(observer, event());
    assert_eq!(evaluated.get(), 1);
    assert_eq!(
        recorder.events(),
        vec![TransportEvent::TornDown { conn_id: CONN }]
    );

    // `sim_chain!` attaches nothing when there is nothing to attach.
    let filter = sim_chain!(FrameFilter::new(None, InboundReader::Persistent) => with_tap(None));
    filter.header_read(1, 0, 0);
}

/// Panics in `observe`.
#[derive(Debug)]
struct Panics;

impl TransportObserver for Panics {
    fn observe(&self, _event: TransportEvent) {
        panic!("observer boom");
    }
}

/// An observer panic unwinds its caller with the observer's own payload.
#[test]
fn observer_panic_unwinds_with_its_own_payload() {
    let cause = CloseCause::new();
    cause.observe_with(registered_tap(Arc::new(Panics)));
    let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        cause.record("fault");
    }))
    .expect_err("the observer panics");
    assert_eq!(payload.downcast_ref::<&str>(), Some(&"observer boom"));
}

/// A cursor at `height`, whose hash is the height repeated.
fn cursor(height: u8) -> Cursor {
    Cursor {
        height: block::Height(u32::from(height)),
        hash: block::Hash([height; 32]),
        headers: u64::from(height),
        bytes: 0,
    }
}

/// Each subscription table reports every transition it applies, with the
/// frame that caused it where a reader tagged one, and only once it applies.
#[test]
fn regulation_tables_report_each_transition() {
    use v9::message_type::{HEADERS, HEADERS_OUTCOME, SUBSCRIBE_HEADERS};

    let recorder = Arc::new(Recorder::default());
    let tap = Arc::new(StreamTap::new(registered_tap(recorder.clone()), 4));
    tap.tag_frames();
    let limits = SubscriptionLimits::from_rule(&v9::SUBSCRIBE_HEADERS)
        .expect("SUBSCRIBE_HEADERS is a subscription row");
    let credit = |objects, bytes| Credit { objects, bytes };
    // The reader queues the frame whose header it read last, and the service
    // takes it.
    let serve = || {
        tap.queue_tag();
        tap.take_tag();
    };

    let mut subscriptions = Subscriptions::new(limits, rows())
        .observed_by(Some(RegulationTap::new(tap.clone(), |id| *id)));
    subscriptions
        .open(1, credit(2, 1_000), cursor(0))
        .expect("the open fits");
    tap.header_read(HEADERS, 0, 300);
    subscriptions.admit(HEADERS, 300).expect("the page fits");
    serve();
    subscriptions
        .claim_admitted_page(&1, 1, 300, cursor(1))
        .expect("the reserved page claims");
    subscriptions
        .accept(&1, &cursor(1))
        .expect("the page was received");
    subscriptions
        .grant(&1, credit(1, 300))
        .expect("the grant fits the window");
    tap.header_read(HEADERS_OUTCOME, 0, 41);
    subscriptions
        .admit(HEADERS_OUTCOME, 41)
        .expect("a live subscription takes its outcome");
    serve();
    subscriptions.claim_end(&1).expect("the subscription ends");
    tap.header_read(HEADERS, 0, 300);
    assert!(subscriptions.admit(HEADERS, 300).is_err());

    let mut publications =
        Publications::new(limits).observed_by(Some(RegulationTap::new(tap.clone(), |id| *id)));
    tap.header_read(SUBSCRIBE_HEADERS, 0, 100);
    serve();
    publications
        .open(9, 0, credit(2, 1_000), cursor(0))
        .expect("the open applies");
    publications
        .grant(&9, 1, &cursor(0), credit(1, 100))
        .expect("the grant applies");
    publications
        .close(&9, 2, &cursor(0))
        .expect("the close applies");
    assert!(publications.end(&9).is_some());
    // A refused update changes nothing, so it reports nothing.
    assert!(publications
        .grant(&9, 3, &cursor(0), credit(1, 100))
        .is_err());

    let regulated: Vec<_> = recorder
        .events()
        .into_iter()
        .filter_map(|event| match event {
            TransportEvent::Regulated {
                conn_id: CONN,
                quic_stream: 4,
                transition,
            } => Some(transition),
            _ => None,
        })
        .collect();
    assert_eq!(
        regulated,
        vec![
            Transition::OpenIssued {
                subscription: 1,
                objects: 2,
                bytes: 1_000,
            },
            Transition::HeaderChecked {
                seq: 0,
                outcome: HeaderCheck::Reserved {
                    objects: 1,
                    bytes: 300,
                },
            },
            Transition::PageClaimed {
                seq: Some(0),
                objects: 1,
                bytes: 300,
            },
            Transition::GrantIssued {
                subscription: 1,
                objects: 1,
                bytes: 300,
            },
            Transition::HeaderChecked {
                seq: 1,
                outcome: HeaderCheck::AdmittedTerminal,
            },
            Transition::EndClaimed { subscription: 1 },
            Transition::HeaderChecked {
                seq: 2,
                outcome: HeaderCheck::Refused {
                    reason: FrameRejection::Unsolicited,
                },
            },
            Transition::OpenApplied {
                subscription: 9,
                objects: 2,
                bytes: 1_000,
                seq: Some(3),
            },
            Transition::GrantApplied {
                subscription: 9,
                objects: 1,
                bytes: 100,
            },
            Transition::CloseApplied { subscription: 9 },
            Transition::PublicationEnded { subscription: 9 },
        ]
    );
}

/// An `Open` retracted before its first byte is reported, so a replayed
/// ledger drops the subscription too. Retracting an unknown key reports
/// nothing.
#[test]
fn a_retracted_open_is_reported() {
    let recorder = Arc::new(Recorder::default());
    let tap = Arc::new(StreamTap::new(registered_tap(recorder.clone()), 4));
    let limits = SubscriptionLimits::from_rule(&v9::SUBSCRIBE_HEADERS)
        .expect("SUBSCRIBE_HEADERS is a subscription row");
    let mut subscriptions = Subscriptions::new(limits, rows())
        .observed_by(Some(RegulationTap::new(tap.clone(), |id| *id)));
    subscriptions
        .open(
            1,
            Credit {
                objects: 2,
                bytes: 1_000,
            },
            cursor(0),
        )
        .expect("the open fits");
    assert!(subscriptions.retract(&1));
    assert!(!subscriptions.retract(&1));
    let regulated: Vec<_> = recorder
        .events()
        .into_iter()
        .filter_map(|event| match event {
            TransportEvent::Regulated { transition, .. } => Some(transition),
            _ => None,
        })
        .collect();
    assert_eq!(
        regulated,
        vec![
            Transition::OpenIssued {
                subscription: 1,
                objects: 2,
                bytes: 1_000,
            },
            Transition::OpenRetracted { subscription: 1 },
        ]
    );
}

/// Records whether a fenced subscription's exchange was still open when its
/// `EndClaimed` or `OpenRetracted` arrived.
#[derive(Debug, Default)]
struct EndWatcher {
    writer: Mutex<Option<crate::zakura::regulation::ExchangeWriter>>,
    open_at_end: Mutex<Option<bool>>,
}

impl TransportObserver for EndWatcher {
    fn observe(&self, event: TransportEvent) {
        if let TransportEvent::Regulated {
            transition: Transition::EndClaimed { .. } | Transition::OpenRetracted { .. },
            ..
        } = event
        {
            let writer = self.writer.lock().expect("no test panics holding it");
            // An exchange that already ended or was abandoned refuses to
            // publish.
            let open = writer.as_ref().map(|writer| writer.publish(|| {}));
            *self.open_at_end.lock().expect("no test panics holding it") = open;
        }
    }
}

/// `EndClaimed` is reported before the fenced exchange ends, since ending it
/// takes the fence's locks, which are scheduling points in a simulated run.
#[test]
fn end_claimed_precedes_the_exchange_end() {
    let watcher = Arc::new(EndWatcher::default());
    let tap = Arc::new(StreamTap::new(registered_tap(watcher.clone()), 4));
    let fence =
        crate::zakura::regulation::WriterFence::new(CancellationToken::new(), CloseCause::new());
    let exchange = fence.open().expect("a fresh fence opens");
    *watcher.writer.lock().expect("no test panics holding it") = Some(exchange.writer());
    let limits = SubscriptionLimits::from_rule(&v9::SUBSCRIBE_HEADERS)
        .expect("SUBSCRIBE_HEADERS is a subscription row");
    let mut subscriptions =
        Subscriptions::new(limits, rows()).observed_by(Some(RegulationTap::new(tap, |id| *id)));
    subscriptions
        .open_fenced(
            1,
            Credit {
                objects: 1,
                bytes: 1_000,
            },
            cursor(0),
            exchange,
        )
        .expect("the open fits");
    subscriptions.claim_end(&1).expect("the subscription ends");
    assert_eq!(
        *watcher
            .open_at_end
            .lock()
            .expect("no test panics holding it"),
        Some(true)
    );
}

/// `OpenRetracted` is reported before the retracted subscription's exchange
/// is dropped, since that drop takes the fence's locks, which are scheduling
/// points in a simulated run.
#[test]
fn open_retracted_precedes_the_exchange_drop() {
    let watcher = Arc::new(EndWatcher::default());
    let tap = Arc::new(StreamTap::new(registered_tap(watcher.clone()), 4));
    let fence =
        crate::zakura::regulation::WriterFence::new(CancellationToken::new(), CloseCause::new());
    let exchange = fence.open().expect("a fresh fence opens");
    *watcher.writer.lock().expect("no test panics holding it") = Some(exchange.writer());
    let limits = SubscriptionLimits::from_rule(&v9::SUBSCRIBE_HEADERS)
        .expect("SUBSCRIBE_HEADERS is a subscription row");
    let mut subscriptions =
        Subscriptions::new(limits, rows()).observed_by(Some(RegulationTap::new(tap, |id| *id)));
    subscriptions
        .open_fenced(
            1,
            Credit {
                objects: 1,
                bytes: 1_000,
            },
            cursor(0),
            exchange,
        )
        .expect("the open fits");
    assert!(subscriptions.retract(&1));
    assert_eq!(
        *watcher
            .open_at_end
            .lock()
            .expect("no test panics holding it"),
        Some(true)
    );
}

/// An observer that panics on every call.
#[derive(Debug)]
struct AlwaysPanics;

impl TransportObserver for AlwaysPanics {
    fn observe(&self, _event: TransportEvent) {
        panic!("observer boom");
    }
}

/// An observer panic in a supervised pipe unwinds that task with its own
/// payload. The pipe's panic hook records a close cause during the unwind,
/// and the observer, which already panicked, isn't called again, so the
/// process doesn't abort on a second panic.
#[tokio::test]
async fn an_observer_panic_in_a_pipe_unwinds_once() {
    let cause = CloseCause::new();
    let conn = registered_tap(Arc::new(AlwaysPanics));
    cause.observe_with(conn.clone());
    let hook_cause = cause.clone();
    let pipe = crate::zakura::spawn_supervised_pipe(
        "observed_pipe",
        test_peer(52),
        CancellationToken::new(),
        || {},
        move || hook_cause.record("service_panic"),
        async move {
            conn.emit(TransportEvent::TornDown { conn_id: CONN });
        },
    );
    let error = pipe.await.expect_err("the observer panic ends the pipe");
    assert!(error.is_panic());
    let payload = error.into_panic();
    assert_eq!(payload.downcast_ref::<&str>(), Some(&"observer boom"));
    assert_eq!(cause.get_or("unset"), "service_panic");
}

/// An inbound connection that finds no admission permit left when the
/// handler takes it over is refused before registration, keyed by its
/// remote address.
#[tokio::test]
async fn an_inbound_connection_without_a_permit_is_refused() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let link = Link::connect().await?;
    let config = crate::Config::default();
    let recorder = Arc::new(Recorder::default());
    let handler = ZakuraProtocolHandler::new(
        ZakuraSupervisorHandle::new(8),
        config.network.clone(),
        ZakuraHandshakeConfig::for_network(&config.network),
        ZakuraLocalLimits::from_config(&config),
    )
    .with_transport_observer(recorder.clone());
    let connection = link.connection.clone();
    handler.serve_inbound(connection.clone(), None).await;
    assert_eq!(
        recorder.events(),
        vec![TransportEvent::HandshakeRefused {
            remote: connection.admitted_addr(),
            reason: "admission".to_owned(),
        }]
    );
    link.close().await;
    Ok(())
}

/// Over real QUIC, the peer's `Open` and its page each carry the read
/// sequence number of their own frame through the reader and the
/// header-sync session: `OpenApplied` and `PageClaimed` name the `HeaderRead`
/// of the frame that caused them. A `Status` before each puts other frames
/// between them, so a tag taken twice or not at all shows.
///
/// `tests/sim_header_sync.rs` can't host this: the node opens a subscription
/// only after its locator query is answered, and that crate can't name the
/// query without `zakura-testkit`.
#[tokio::test]
async fn regulation_events_name_the_frames_that_caused_them() -> Result<(), BoxError> {
    use crate::zakura::{
        header_sync::v9::{CommonMessage, Cursor as V9Cursor, Operation, SubscribeHeaders},
        testkit::HostilePeer,
        AuxSchema, HeaderEntry, HeaderSyncAction, Headers, Status, ZAKURA_STREAM_HEADER_SYNC,
    };
    use zakura_chain::{parameters::Network, serialization::ZcashDeserializeInto};
    use zakura_test::vectors::{BLOCK_MAINNET_1_BYTES, BLOCK_MAINNET_GENESIS_BYTES};

    let _guard = zakura_test::init();
    let block = |bytes: &[u8]| -> Arc<Block> {
        Arc::new(bytes.zcash_deserialize_into().expect("block vector parses"))
    };
    let genesis = block(&BLOCK_MAINNET_GENESIS_BYTES);
    let block_one = block(&BLOCK_MAINNET_1_BYTES);
    let anchor = (block::Height(0), genesis.hash());
    let target = (block::Height(1), block_one.hash());
    let recorder = Arc::new(Recorder::default());
    let node = ZakuraTestNode::builder(80)
        .header_sync_driver(
            Network::Mainnet,
            anchor,
            crate::zakura::FullStateFrontiers {
                finalized_height: anchor.0,
                verified_block_tip: anchor.0,
                verified_block_hash: anchor.1,
            },
            Some(anchor),
        )
        .transport_observer(recorder.clone())
        .spawn()
        .await?;
    let mut actions = node
        .take_header_sync_actions()
        .await
        .expect("the external header driver is enabled");
    let header_sync = node.header_sync().expect("the header service is enabled");
    let peer =
        HostilePeer::connect_native_with_capabilities(&node, 81, ZAKURA_CAP_HEADER_SYNC).await?;
    let peer_id = peer.id()?;
    let status = CommonMessage::Status(Status {
        work_anchor_height: anchor.0,
        work_anchor_hash: anchor.1,
        selected_tip_height: target.0,
        selected_tip_hash: target.1,
        suffix_cumulative_work: zakura_chain::work::difficulty::U256::from(1_u8),
        oldest_retained_height: anchor.0,
        max_headers_per_response: 16,
        max_subscriptions: 1,
        max_message_bytes: 1_000_000,
        tree_aux_schema_mask: 0,
    });
    let at_anchor = V9Cursor {
        height: anchor.0,
        hash: anchor.1,
        headers: 0,
        bytes: 0,
    };

    peer.send_header_sync_message(&status).await?;
    peer.send_header_sync_message(&CommonMessage::SubscribeHeaders(SubscribeHeaders {
        operation: Operation::Open,
        subscription_id: 5,
        update_sequence: 0,
        initial_target_tip_hash: anchor.1,
        acknowledged: at_anchor,
        locator_hashes: vec![anchor.1],
        added_credit: Credit {
            objects: 1,
            bytes: 10_000,
        },
        tree_aux_schema: AuxSchema::None,
    }))
    .await?;
    let (session_id, scope) = match timeout(TEST_TIMEOUT, actions.recv())
        .await?
        .ok_or("the action channel stays open")?
    {
        HeaderSyncAction::QueryHeaderLocator {
            session_id, scope, ..
        } => (session_id, scope),
        action => return Err(format!("unexpected header action: {action:?}").into()),
    };
    header_sync
        .send(crate::zakura::Event::HeaderLocatorReady {
            peer: peer_id,
            session_id,
            target_tip_hash: target.1,
            scope,
            locator: Some(zakura_header_chain::HeaderLocator::for_continuation(
                zakura_header_chain::Frontier::new(anchor.0, anchor.1),
            )),
        })
        .await?;
    let open = timeout(TEST_TIMEOUT, async {
        loop {
            if let CommonMessage::SubscribeHeaders(open) = peer.recv_header_sync_message().await? {
                return Ok::<_, BoxError>(open);
            }
        }
    })
    .await??;
    assert_eq!(open.operation, Operation::Open);

    peer.send_header_sync_message(&status).await?;
    peer.send_header_sync_message(&CommonMessage::Headers(Headers {
        subscription_id: open.subscription_id,
        initial_target_tip_hash: target.1,
        common_ancestor_height: anchor.0,
        common_ancestor_hash: anchor.1,
        reaches_initial_target: true,
        tree_aux_schema: AuxSchema::None,
        entries: vec![HeaderEntry {
            header: block_one.header.clone(),
            body_size: 1_000,
            tree_aux: None,
        }],
    }))
    .await?;
    let claimed = |events: &[TransportEvent]| {
        events.iter().any(|event| {
            matches!(
                event,
                TransportEvent::Regulated {
                    transition: Transition::PageClaimed { .. },
                    ..
                }
            )
        })
    };
    timeout(TEST_TIMEOUT, async {
        while !claimed(&recorder.events()) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;

    let events = recorder.events();
    let quic_stream = events
        .iter()
        .find_map(|event| match event {
            TransportEvent::Admitted {
                quic_stream,
                kind: ZAKURA_STREAM_HEADER_SYNC,
                ..
            } => Some(*quic_stream),
            _ => None,
        })
        .expect("the header stream was admitted");
    let read_types: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            TransportEvent::HeaderRead {
                quic_stream: read,
                seq,
                message_type,
                ..
            } if *read == quic_stream => Some((*seq, *message_type)),
            _ => None,
        })
        .collect();
    assert_eq!(
        read_types,
        [
            (0, v9::message_type::STATUS),
            (1, v9::message_type::SUBSCRIBE_HEADERS),
            (2, v9::message_type::STATUS),
            (3, v9::message_type::HEADERS),
        ]
    );
    let tagged: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            TransportEvent::Regulated {
                quic_stream: regulated,
                transition:
                    Transition::OpenApplied { seq, .. } | Transition::PageClaimed { seq, .. },
                ..
            } if *regulated == quic_stream => Some(*seq),
            _ => None,
        })
        .collect();
    assert_eq!(tagged, [Some(1), Some(3)]);
    Ok(())
}
