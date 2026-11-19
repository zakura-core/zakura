//! A reader parks a frame whose row's cadence bucket is empty (reg-5,
//! disco-13). The wait never closes, cancellation ends it, and a sibling
//! stream keeps moving.

use std::sync::atomic::{AtomicU32, Ordering};

use super::*;
use crate::zakura::{
    regulation::test_family::{decode, message_type, Probe, RULES, STATUS},
    Cadence, MessageRole,
};

/// Two back to back, then one every 500 ms.
const FAST: Cadence = Cadence {
    capacity: 2,
    refill_interval: Duration::from_millis(500),
    send_interval: Duration::from_secs(1),
};

/// The test family's `Status` at [`FAST`].
const FAST_STATUS: MessageRule = MessageRule {
    role: MessageRole::Announcement { cadence: FAST },
    ..STATUS
};

/// A table whose every row is paced, so its reader parks.
const PARKING: &[MessageRule] = &[FAST_STATUS];

fn status(value: u32) -> Frame {
    Frame {
        message_type: message_type::STATUS,
        flags: 0,
        payload: value.to_le_bytes().to_vec(),
    }
}

/// Lets every task that the last clock step woke run to its next wait.
async fn settle() {
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
}

/// A flood of one row parks at the bucket. With a paused clock, each parked
/// frame goes exactly when its token returns, not a nanosecond before, so the
/// frames dispatched never exceed `capacity + elapsed / refill_interval`.
/// Cancelling the stream ends the wait without a close. A sibling layout
/// on the same connection buckets keeps admitting throughout.
#[tokio::test(start_paused = true)]
async fn a_parked_frame_goes_at_its_exact_token_boundary() {
    const FLOOD: u32 = 7;
    let context = Arc::new(tabled_context(Some(PARKING), 1));
    let cadence = context
        .cadence
        .clone()
        .expect("a tabled stream has a scope");
    assert!(cadence.parks);
    // An honest stream of another service that shares the connection's
    // buckets. Its table doesn't park.
    let honest = StreamWorkerContext {
        cadence: Some(CadenceScope {
            layout: 901,
            parks: false,
            ..cadence.clone()
        }),
        ..tabled_context(Some(RULES), 1)
    };
    let start = Instant::now();
    let dispatched = Arc::new(AtomicU32::new(0));
    let reader = {
        let (context, dispatched) = (Arc::clone(&context), Arc::clone(&dispatched));
        tokio::spawn(async move {
            for value in 0..FLOOD {
                let frame = status(value);
                match admit_inbound_message(&frame, &context, 900) {
                    InboundMessageAdmission::Admit => {}
                    InboundMessageAdmission::Park => {
                        if !context.park_for_cadence(&frame).await {
                            return;
                        }
                    }
                    refused => panic!("a cadence row is never refused: {refused:?}"),
                }
                dispatched.fetch_add(1, Ordering::SeqCst);
            }
        })
    };
    settle().await;
    let tokens = || {
        cadence
            .buckets
            .lock()
            .unwrap()
            .tokens(900, message_type::STATUS)
    };
    assert_eq!(dispatched.load(Ordering::SeqCst), FAST.capacity);
    assert_eq!(tokens(), Some(0));

    // Each token returns exactly one refill interval after the last.
    let mut boundary = start;
    for earned in 1..FLOOD - FAST.capacity {
        boundary += FAST.refill_interval;
        tokio::time::advance(boundary - Duration::from_nanos(1) - Instant::now()).await;
        settle().await;
        assert_eq!(
            dispatched.load(Ordering::SeqCst),
            FAST.capacity + earned - 1,
            "a parked frame went before its token"
        );
        // The honest service isn't held back by the parked reader.
        assert_eq!(
            admit_inbound_message(&status(earned), &honest, 901),
            InboundMessageAdmission::Admit
        );
        tokio::time::advance(Duration::from_nanos(1)).await;
        settle().await;
        let elapsed = Instant::now() - start;
        let bound = FAST.capacity
            + u32::try_from(elapsed.as_nanos() / FAST.refill_interval.as_nanos()).unwrap();
        assert_eq!(dispatched.load(Ordering::SeqCst), FAST.capacity + earned);
        assert!(dispatched.load(Ordering::SeqCst) <= bound);
        // The parked frame paid exactly the token it waited for.
        assert_eq!(tokens(), Some(0));
    }

    // The last frame is parked. Cancelling its stream ends the wait, and the
    // frame never goes.
    let parked = dispatched.load(Ordering::SeqCst);
    assert_eq!(parked, FLOOD - 1);
    context.stream_token.cancel();
    settle().await;
    assert!(reader.is_finished());
    assert_eq!(dispatched.load(Ordering::SeqCst), parked);
    // A wait is never a close, for the parked stream or its sibling.
    for context in [&*context, &honest] {
        assert!(!context.connection_token.is_cancelled());
        assert_eq!(context.close_cause.get(), None);
    }
}

/// Over a real connection, a flooded stream's reader forwards no faster than
/// its bucket allows, while a sibling stream on the same connection delivers
/// at once. Neither stream closes.
#[tokio::test]
async fn a_parked_reader_holds_its_stream_while_a_sibling_keeps_moving() -> Result<(), BoxError> {
    const ALPN: &[u8] = b"/zakura/testkit/cadence-park/0";
    const FLOOD: u32 = 4;
    const SIBLING: u32 = 16;
    let _guard = zakura_test::init();
    let server = LocalEndpointFactory::new().endpoint(95201).await?;
    let (conn_tx, _conn_rx) = mpsc::channel(8);
    let (stream_tx, mut stream_rx) = mpsc::channel(8);
    server.serve(CaptureConnection {
        alpn: ALPN,
        connection_tx: conn_tx,
        stream_tx,
    })?;
    let client = LocalEndpointFactory::new().endpoint(95202).await?;
    let connection = timeout(
        Duration::from_secs(5),
        client.connect(LocalEndpointFactory::node_addr(&server).await, &[ALPN]),
    )
    .await??;
    let frames = |values: std::ops::Range<u32>| {
        let mut bytes = Vec::new();
        for value in values {
            bytes.extend_from_slice(&message_type::STATUS.to_le_bytes());
            bytes.extend_from_slice(&0u16.to_le_bytes());
            bytes.extend_from_slice(&4u32.to_le_bytes());
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        bytes
    };
    let mut workers = JoinSet::new();
    let (exit_tx, _exit_rx) = mpsc::unbounded_channel();
    let (mut flooded_send, _flooded_recv) =
        timeout(Duration::from_secs(2), connection.open_bi()).await??;
    let start = Instant::now();
    timeout(
        Duration::from_secs(2),
        flooded_send.write_all(&frames(0..FLOOD)),
    )
    .await??;
    let (mut flooded, flooded_owner) =
        accept_stream(&mut stream_rx, &mut workers, exit_tx.clone(), PARKING, 900).await?;
    let mut arrived = Vec::new();
    for value in 0..FAST.capacity {
        let frame = timeout(Duration::from_secs(2), flooded.streams[0].recv.recv())
            .await?
            .expect("the bucket's capacity goes at once");
        assert_eq!(decode(&frame), Probe::Status(value));
        arrived.push(Instant::now());
    }

    // The flooded reader is parked now. The sibling's frames all arrive
    // before the parked frame's token returns.
    let (mut sibling_send, _sibling_recv) =
        timeout(Duration::from_secs(2), connection.open_bi()).await??;
    timeout(
        Duration::from_secs(2),
        sibling_send.write_all(&frames(0..SIBLING)),
    )
    .await??;
    let (mut sibling, sibling_owner) =
        accept_stream(&mut stream_rx, &mut workers, exit_tx.clone(), RULES, 901).await?;
    for value in 0..SIBLING {
        let frame = timeout(Duration::from_secs(2), sibling.streams[0].recv.recv())
            .await?
            .expect("the sibling reader forwards every frame");
        assert_eq!(decode(&frame), Probe::Status(value));
    }
    let sibling_done = Instant::now();

    for value in FAST.capacity..FLOOD {
        let frame = timeout(Duration::from_secs(2), flooded.streams[0].recv.recv())
            .await?
            .expect("a parked frame goes once its token returns");
        assert_eq!(decode(&frame), Probe::Status(value));
        arrived.push(Instant::now());
    }
    // The bucket is made at the first charge, after `start`. So by any instant
    // `t`, at most `capacity + (t - start) / refill_interval` frames arrived.
    for (index, at) in arrived.iter().enumerate() {
        let earned = (*at - start).as_nanos() / FAST.refill_interval.as_nanos();
        assert!(
            u128::try_from(index).unwrap() < u128::from(FAST.capacity) + earned,
            "frame {index} arrived {:?} after the first write",
            *at - start
        );
    }
    assert!(sibling_done < arrived[usize::try_from(FAST.capacity).unwrap()]);
    for (token, cause) in [&flooded_owner, &sibling_owner] {
        assert!(!token.is_cancelled());
        assert_eq!(cause.get(), None);
    }
    for (token, _) in [&flooded_owner, &sibling_owner] {
        token.cancel();
    }
    while timeout(Duration::from_secs(2), workers.join_next())
        .await?
        .is_some()
    {}
    connection.close(0u32.into(), b"done");
    client.shutdown().await;
    server.shutdown().await;
    Ok(())
}

/// Accepts one captured stream that carries `rules`, and runs its worker.
/// Returns its admitted session, and its context's connection token and close
/// cause.
async fn accept_stream(
    stream_rx: &mut mpsc::Receiver<(SendStream, RecvStream)>,
    workers: &mut JoinSet<()>,
    exit_tx: mpsc::UnboundedSender<SessionExit>,
    rules: &'static [MessageRule],
    kind: u16,
) -> Result<(AdmittedSession, (CancellationToken, CloseCause)), BoxError> {
    let (send, recv) = timeout(Duration::from_secs(2), stream_rx.recv())
        .await?
        .expect("the capture forwards each stream");
    let stream = Stream {
        kind,
        messages: Some(rules),
        ..Stream::PERSISTENT
    };
    let context = tabled_context(Some(rules), 1);
    let observed = (
        context.connection_token.clone(),
        context.close_cause.clone(),
    );
    let prelude = StreamPrelude {
        magic: STREAM_PRELUDE_MAGIC,
        stream_kind: stream.kind,
        stream_version: stream.version,
        request_id: None,
        max_frame_bytes: stream.frame_cap,
    };
    let admitted = spawn_persistent_stream_worker(
        workers, send, recv, stream, prelude, context, 8, false, exit_tx,
    );
    Ok((admitted, observed))
}
