//! Plain tests of [`OutputLife`] (M6a-6): every queueing call makes its
//! output's life before it returns, and the life's `Drop` ends it once,
//! from the phase it reached, on every path. The writer's paths and the
//! peers' receipts run under the simulator, in `zakura-sim`'s
//! `header_wire_output` tests.

use std::{
    future::Future,
    net::{Ipv4Addr, SocketAddr},
    pin::pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use super::*;
use crate::zakura::{
    frame_fingerprint, ConnTap, OutputOutcome, StreamTap, TransportEvent, TransportObserver,
    Unwritten,
};

/// The connection the tests' taps register.
const CONN: u64 = 1;
/// The QUIC stream the tests' taps name.
const STREAM: u64 = 4;

/// Records every event in order.
#[derive(Debug, Default)]
pub(crate) struct Recorder(Mutex<Vec<TransportEvent>>);

impl TransportObserver for Recorder {
    fn observe(&self, event: TransportEvent) {
        self.0
            .lock()
            .expect("no test panics holding it")
            .push(event);
    }
}

impl Recorder {
    /// The output events recorded so far, as `(output, what)`.
    pub(crate) fn lives(&self) -> Vec<(u64, Life)> {
        self.0
            .lock()
            .expect("no test panics holding it")
            .iter()
            .filter_map(|event| match *event {
                TransportEvent::OutputAllocated { output, .. } => Some((output, Life::Allocated)),
                TransportEvent::OutputEnqueued { output, order, .. } => {
                    Some((output, Life::Enqueued(order)))
                }
                TransportEvent::OutputDequeued { output, .. } => Some((output, Life::Dequeued)),
                TransportEvent::OutputEnded {
                    output, outcome, ..
                } => Some((output, Life::Ended(outcome))),
                _ => None,
            })
            .collect()
    }
}

/// One output event, in [`Recorder::lives`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Life {
    Allocated,
    Enqueued(u64),
    Dequeued,
    Ended(OutputOutcome),
}

/// A tapped worker queue of `depth` frames, as `spawn_service_session`
/// builds it, and what its tap records.
pub(crate) fn tapped(depth: usize) -> (FramedSend, FramedWorkerRecv, Arc<Recorder>) {
    let recorder = Arc::new(Recorder::default());
    let conn = Arc::new(ConnTap::new(
        recorder.clone(),
        SocketAddr::from((Ipv4Addr::LOCALHOST, 9)),
    ));
    conn.registered(CONN);
    let tap = Arc::new(StreamTap::new(conn, STREAM));
    let (send, recv) = worker_framed_channel(depth);
    (send.observed(Some(tap)), recv, recorder)
}

pub(crate) fn frame(payload: u8) -> Frame {
    Frame {
        message_type: 3,
        flags: 0,
        payload: vec![payload; 8],
    }
}

/// Polls `future` once with a no-op waker.
pub(crate) fn poll_once<F: Future>(future: std::pin::Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(std::task::Waker::noop()))
}

/// `send` makes its life before it returns the future, so a future
/// dropped before its first poll, or while it waits for queue space, still
/// ends its output, as `not-queued`.
///
/// This fails if the life is made inside the async body.
#[test]
fn a_send_dropped_before_its_insert_ends_not_queued() {
    let (send, _recv, recorder) = tapped(1);
    drop(send.send(frame(1)));
    assert_eq!(
        recorder.lives(),
        [
            (0, Life::Allocated),
            (0, Life::Ended(OutputOutcome::NotQueued))
        ]
    );

    // Fill the queue, then wait on it.
    send.try_send(frame(2)).expect("the queue has a slot");
    {
        let mut waiting = pin!(send.send(frame(3)));
        assert!(
            poll_once(waiting.as_mut()).is_pending(),
            "the queue is full"
        );
        assert_eq!(
            recorder.lives().last(),
            Some(&(2, Life::Allocated)),
            "a send waiting for space is still allocated"
        );
    }
    assert_eq!(
        recorder.lives()[2..],
        [
            (1, Life::Allocated),
            (1, Life::Enqueued(0)),
            (2, Life::Allocated),
            (2, Life::Ended(OutputOutcome::NotQueued)),
        ]
    );
}

/// Each output's life follows its frame: a refused `try_send` ends
/// `not-queued`, a frame queued through a reserved slot whose receiver then
/// closes ends `dropped` with the queue, and a dequeued frame the writer
/// writes ends `written`. Each ends once, from its own phase.
///
/// This fails if `Enqueued` is set before the insert returns, since the
/// refused frame would end `dropped`.
#[test]
fn the_life_follows_the_frame_through_the_channel() {
    let (send, recv, recorder) = tapped(1);
    send.try_send(frame(1)).expect("the queue has a slot");
    assert!(matches!(
        send.try_send(frame(2)),
        Err(mpsc::error::TrySendError::Full(_))
    ));
    drop(recv);
    assert_eq!(
        recorder.lives(),
        [
            (0, Life::Allocated),
            (0, Life::Enqueued(0)),
            (1, Life::Allocated),
            (1, Life::Ended(OutputOutcome::NotQueued)),
            (0, Life::Ended(OutputOutcome::Dropped)),
        ]
    );

    let (send, recv, recorder) = tapped(2);
    let slot = send.try_reserve_guarded().expect("the queue has a slot");
    drop(recv);
    slot.send(frame(1), FrameGuard::new(Arc::new(())));
    assert_eq!(
        recorder.lives(),
        [(0, Life::Allocated), (0, Life::Enqueued(0))],
        "Tokio keeps a frame sent through a permit after its receiver closes"
    );
    drop(send);
    assert_eq!(
        recorder.lives().last(),
        Some(&(0, Life::Ended(OutputOutcome::Dropped)))
    );
}

/// The writer's outcomes: a written frame, one that fails to encode, an
/// obsolete request claim it skips, and one whose stream is cancelled
/// mid-write. Each ends once with its outcome.
///
/// This fails if the life stays inside the write future, since the
/// cancelled write would then end `abandoned`.
#[tokio::test(flavor = "current_thread")]
async fn the_writer_ends_each_output_with_its_outcome() {
    /// A request claim that's already obsolete.
    #[derive(Debug)]
    struct Obsolete;
    impl FrameWriteClaim for Obsolete {
        fn try_start(&self) -> bool {
            false
        }
        fn written(&self) {}
    }

    let (send, mut recv, recorder) = tapped(4);
    send.send(frame(1)).await.expect("the queue is open");
    send.send(frame(2)).await.expect("the queue is open");
    let slot = send.try_reserve_guarded().expect("the queue has a slot");
    assert!(slot.send_request(frame(3), Arc::new(Obsolete)));
    send.send(frame(4)).await.expect("the queue is open");
    let mut outcomes = Vec::new();
    for case in 0..4 {
        let mut queued = recv.recv().await.expect("four frames are queued");
        let life = queued.take_life().expect("the queue is tapped");
        life.dequeued();
        let result = {
            let write = queued.encode_and_write_with(
                |frame| if case == 1 { Err("too big") } else { Ok(frame) },
                |_| async {
                    life.started();
                    if case == 3 {
                        std::future::pending::<()>().await;
                    }
                    Ok(())
                },
            );
            let mut write = pin!(write);
            match poll_once(write.as_mut()) {
                Poll::Ready(result) => result.is_ok(),
                Poll::Pending => {
                    // As the writer's cancellation arms do before they break.
                    OutputLife::cancelled(Some(&life));
                    false
                }
            }
        };
        outcomes.push(result);
        drop(life);
    }
    assert_eq!(outcomes, [true, false, true, false]);
    let ends: Vec<_> = recorder
        .lives()
        .into_iter()
        .filter_map(|(output, life)| match life {
            Life::Ended(outcome) => Some((output, outcome)),
            _ => None,
        })
        .collect();
    assert_eq!(
        ends,
        [
            (0, OutputOutcome::Written),
            (1, OutputOutcome::Unwritten(Unwritten::Encode)),
            (2, OutputOutcome::Unwritten(Unwritten::Obsolete)),
            (3, OutputOutcome::Unwritten(Unwritten::Cancelled)),
        ]
    );
}

/// Enqueue order is the order frames reach the queue, not the order their
/// outputs were made: output 1, sent through the second reserved slot
/// first, is at place 0.
///
/// This fails if the order is the output number.
#[test]
fn enqueue_order_is_the_insert_order() {
    let (send, _recv, recorder) = tapped(2);
    let first = send.try_reserve_guarded().expect("the queue has a slot");
    let second = send.try_reserve_guarded().expect("the queue has a slot");
    let (a, b) = (frame(1), frame(2));
    let a_life = send.allocate(&a);
    let b_life = send.allocate(&b);
    second.send_with_life(b, FrameGuard::new(Arc::new(())), b_life);
    first.send_with_life(a, FrameGuard::new(Arc::new(())), a_life);
    assert_eq!(
        recorder.lives(),
        [
            (0, Life::Allocated),
            (1, Life::Allocated),
            (1, Life::Enqueued(0)),
            (0, Life::Enqueued(1)),
        ]
    );
}

/// A queue with no tap, such as `framed_channel`'s, makes no lives.
#[test]
fn an_untapped_queue_makes_no_life() {
    let (send, _recv) = framed_channel(1);
    assert!(send.allocate(&frame(1)).is_none());
    send.try_send(frame(1)).expect("the queue has a slot");
}

/// The fingerprint covers the message type, the flags and the payload.
#[test]
fn the_fingerprint_covers_type_flags_and_payload() {
    let base = frame(1);
    let fingerprint = frame_fingerprint(&base);
    assert_eq!(fingerprint, frame_fingerprint(&base.clone()));
    for changed in [
        Frame {
            message_type: 4,
            ..base.clone()
        },
        Frame {
            flags: 1,
            ..base.clone()
        },
        Frame {
            payload: vec![2; 8],
            ..base.clone()
        },
        Frame {
            payload: vec![1; 9],
            ..base.clone()
        },
    ] {
        assert_ne!(frame_fingerprint(&changed), fingerprint, "{changed:?}");
    }
}
