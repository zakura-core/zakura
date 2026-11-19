//! Writer fence: #978's six unit tests and lifecycle proptest, renamed, and
//! the fenced send.

use std::time::Duration;

use proptest::prelude::*;

use super::*;
use crate::zakura::{framed_channel, transport::worker_framed_channel};

/// A fence over a session of a fresh connection, with both tokens.
fn fence() -> (WriterFence, CancellationToken, CancellationToken) {
    let connection = CancellationToken::new();
    let session = connection.child_token();
    (WriterFence::new(session.clone()), session, connection)
}

#[test]
fn retirement_fences_prepared_and_queued_writes_without_resetting() {
    for queued in [false, true] {
        let (fence, session, _) = fence();
        let exchange = fence.open().unwrap();
        let writer = exchange.writer();
        if queued {
            assert!(writer.publish(|| {}));
        }
        assert!(fence.retire());
        assert!(fence.open().is_none());
        assert!(!writer.publish(|| panic!("a retired fence cannot publish")));
        assert!(!writer.try_start(|| panic!("a retired fence cannot start")));
        drop(exchange);
        assert!(!session.is_cancelled());
    }
}

#[test]
fn owner_drop_fences_unstarted_writes_and_resets_started_exchanges() {
    for started in [false, true] {
        let (fence, session, connection) = fence();
        let exchange = fence.open().unwrap();
        let writer = exchange.writer();
        assert!(writer.publish(|| {}));
        if started {
            assert!(writer.try_start(|| true));
        }
        drop(exchange);
        assert!(!writer.try_start(|| panic!("the exchange's owner has gone")));
        assert_eq!(session.is_cancelled(), started);
        assert_eq!(fence.retire(), !started);
        if started {
            assert!(!connection.is_cancelled(), "a reset keeps the connection");
        }
    }
}

#[test]
fn only_endings_release_started_exchanges() {
    for end_both in [false, true] {
        let (fence, session, _) = fence();
        let mut first = fence.open().unwrap();
        let mut second = fence.open().unwrap();
        let first_writer = first.writer();
        let second_writer = second.writer();
        for writer in [&first_writer, &second_writer] {
            assert!(writer.publish(|| {}));
            assert!(writer.try_start(|| true));
        }
        first.end();
        first.end();
        assert!(!first_writer.try_start(|| panic!("an ended exchange cannot restart")));
        if end_both {
            second.end();
        }
        assert_eq!(fence.retire(), end_both);
        assert_eq!(session.is_cancelled(), !end_both);
    }
}

#[test]
fn a_failed_work_claim_leaves_the_session_running() {
    let (fence, session, _) = fence();
    let exchange = fence.open().unwrap();
    let writer = exchange.writer();
    assert!(writer.publish(|| {}));
    assert!(!writer.try_start(|| false));
    assert!(fence.retire());
    assert!(!session.is_cancelled());
}

#[test]
fn retirement_and_first_write_have_one_winner() {
    for _ in 0..128 {
        let (fence, session, _) = fence();
        let exchange = fence.open().unwrap();
        let writer = exchange.writer();
        assert!(writer.publish(|| {}));
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let writer_barrier = barrier.clone();
        let racer = std::thread::spawn(move || {
            writer_barrier.wait();
            writer.try_start(|| true)
        });
        barrier.wait();
        let reusable = fence.retire();
        let started = racer.join().unwrap();
        assert_eq!(session.is_cancelled(), started);
        assert_eq!(reusable, !started);
    }
}

#[test]
fn publication_is_complete_before_retirement_returns() {
    use std::sync::mpsc;

    let (fence, _, _) = fence();
    let exchange = fence.open().unwrap();
    let writer = exchange.writer();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (published_tx, published_rx) = mpsc::channel();
    let publisher = std::thread::spawn(move || {
        assert!(writer.publish(|| {
            entered_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            published_tx.send(()).unwrap();
        }));
    });
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let retirement = std::thread::spawn(move || {
        assert!(fence.retire());
        published_rx
            .try_recv()
            .expect("retirement waits for the publication");
        fence
    });
    release_tx.send(()).unwrap();
    publisher.join().unwrap();
    let fence = retirement.join().unwrap();
    assert!(fence.open().is_none());
    assert!(!exchange.writer().try_start(|| true));
}

#[tokio::test]
async fn a_fenced_send_writes_only_before_retirement() {
    let frame = || Frame {
        message_type: 1,
        flags: 0,
        payload: vec![7],
    };
    for retire_first in [false, true] {
        let (fence, session, _) = fence();
        let (send, mut output) = framed_channel(2);
        let mut exchange = fence.open().unwrap();
        send.send_fenced(frame(), &exchange.writer()).await.unwrap();
        assert_eq!(
            send.send_fenced(frame(), &exchange.writer()).await,
            Err(FencedSendError::Fenced),
            "an exchange publishes once"
        );
        if retire_first {
            assert!(fence.retire());
            // The queued frame's claim fails, so the reader skips it.
            drop(send);
            assert_eq!(output.recv().await, None);
            assert!(!session.is_cancelled());
        } else {
            assert_eq!(output.recv().await, Some(frame()));
            exchange.end();
            assert!(fence.retire());
            assert!(!session.is_cancelled());
        }
    }
}

/// A fenced request that fails to encode starts no exchange, so retiring its
/// session leaves the session running.
#[tokio::test]
async fn a_fenced_request_that_fails_to_encode_starts_nothing() {
    for encodes in [false, true] {
        let (fence, session, connection) = fence();
        let (send, mut output) = worker_framed_channel(1);
        let exchange = fence.open().unwrap();
        let frame = Frame {
            message_type: 1,
            flags: 0,
            payload: vec![7],
        };
        send.send_fenced(frame, &exchange.writer()).await.unwrap();
        let queued = output.recv().await.unwrap();
        let result = queued
            .encode_and_write_with(
                |frame| {
                    if encodes {
                        Ok(frame)
                    } else {
                        Err("does not fit")
                    }
                },
                |_| async { Ok(()) },
            )
            .await;
        assert_eq!(result.is_ok(), encodes);
        assert_eq!(fence.retire(), !encodes);
        assert_eq!(session.is_cancelled(), encodes);
        if encodes {
            assert!(!connection.is_cancelled(), "a reset keeps the connection");
        }
    }
}

/// A frame queued to follow a request goes out only after the request's
/// first byte and before retirement, so retirement skips it whether the
/// request was skipped or already ended. An ending that arrives before the
/// request's first byte skips both.
#[tokio::test]
async fn a_following_frame_needs_its_request_started() {
    let frame = || Frame {
        message_type: 1,
        flags: 0,
        payload: vec![7],
    };
    // Retire before the request, retire after the request ended, never
    // retire, or end the exchange before the request's first byte.
    for case in 0..4 {
        let (fence, session, _) = fence();
        let (send, mut output) = framed_channel(2);
        let mut exchange = fence.open().unwrap();
        let writer = exchange.writer();
        send.send_fenced(frame(), &writer).await.unwrap();
        send.send_following(frame(), &writer).await.unwrap();
        if case == 0 {
            assert!(fence.retire());
        } else if case == 3 {
            exchange.end();
            assert_eq!(output.try_recv().ok(), None, "the request is skipped");
        } else {
            assert_eq!(output.recv().await, Some(frame()));
            exchange.end();
            if case == 1 {
                assert!(fence.retire(), "an ended exchange leaves it running");
            }
        }
        drop(send);
        let follow_up = output.recv().await;
        assert_eq!(follow_up.is_some(), case == 2, "case {case}");
        assert!(!session.is_cancelled());
    }
}

/// A following frame's claim holds the fence until its write finishes, so
/// retirement in between resets the session, even after the request's
/// exchange ended. A finished write leaves the session running.
#[test]
fn retirement_during_a_following_write_resets_the_session() {
    for written in [false, true] {
        let (fence, session, connection) = fence();
        let mut exchange = fence.open().unwrap();
        let writer = exchange.writer();
        assert!(writer.publish(|| {}));
        assert!(writer.try_start(|| true));
        exchange.end();
        let follow = FollowingWrite {
            writer,
            written_at: None,
        };
        assert!(follow.try_start());
        if written {
            follow.written();
        }
        assert_eq!(fence.retire(), written);
        assert_eq!(session.is_cancelled(), !written);
        if !written {
            assert!(!connection.is_cancelled(), "a reset keeps the connection");
        }
    }
}

/// Whether `generation`'s session ended: the connection closed or the
/// session was reset.
fn ended(closed: bool, reset: &[bool], generation: usize) -> bool {
    closed || reset[generation]
}

#[derive(Clone, Copy, Debug)]
struct ExpectedExchange {
    generation: usize,
    published: bool,
    sent: bool,
    ended: bool,
}

proptest! {
    /// Random histories of eight exchanges over retiring fences, one session
    /// per fence, on one connection. After every step, every writer of a
    /// retired fence or of an ended session stays fenced, a session is reset
    /// exactly when a started exchange lost its owner or its fence without an
    /// ending, and no reset reaches the connection.
    #[test]
    fn exchange_histories_keep_old_writers_fenced(
        actions in prop::collection::vec((0u8..7, 0usize..8, any::<bool>()), 1..192),
    ) {
        let connection = CancellationToken::new();
        let mut sessions = vec![connection.child_token()];
        let mut fences = vec![WriterFence::new(sessions[0].clone())];
        // Whether each generation's session was reset.
        let mut reset = vec![false];
        let mut owners: [Option<Exchange>; 8] = std::array::from_fn(|_| None);
        // Writers outlive owners, endings, and retirement.
        let mut writers: [Option<ExchangeWriter>; 8] = std::array::from_fn(|_| None);
        let mut expected: [Option<ExpectedExchange>; 8] = [None; 8];
        let mut closed = false;

        for (action, index, work_available) in actions {
            let generation = fences.len() - 1;
            match action {
                0 if owners[index].is_none() => {
                    let opened = fences[generation].open();
                    prop_assert_eq!(opened.is_some(), !ended(closed, &reset, generation));
                    if let Some(opened) = opened {
                        if let Some(old_writer) = &writers[index] {
                            prop_assert!(!old_writer.try_start(|| true));
                        }
                        writers[index] = Some(opened.writer());
                        owners[index] = Some(opened);
                        expected[index] = Some(ExpectedExchange {
                            generation,
                            published: false,
                            sent: false,
                            ended: false,
                        });
                    }
                }
                1 => {
                    if let (Some(writer), Some(exchange)) = (&writers[index], &mut expected[index]) {
                        let allowed = owners[index].is_some() && !ended(closed, &reset, generation)
                            && exchange.generation == generation && !exchange.published
                            && !exchange.ended;
                        let mut published = false;
                        let actual = writer.publish(|| published = true);
                        prop_assert_eq!(actual, allowed);
                        prop_assert_eq!(published, allowed);
                        exchange.published |= allowed;
                    }
                }
                2 => {
                    if let (Some(writer), Some(exchange)) = (&writers[index], &mut expected[index]) {
                        let eligible = owners[index].is_some() && !ended(closed, &reset, generation)
                            && exchange.generation == generation && exchange.published
                            && !exchange.sent && !exchange.ended;
                        let mut claimed = false;
                        let actual = writer.try_start(|| { claimed = true; work_available });
                        prop_assert_eq!(claimed, eligible);
                        prop_assert_eq!(actual, eligible && work_available);
                        exchange.sent |= eligible && work_available;
                    }
                }
                3 => {
                    if let (Some(owner), Some(exchange)) = (&mut owners[index], &mut expected[index]) {
                        // The reactor validated an ending for a sent exchange.
                        if exchange.sent {
                            owner.end();
                            exchange.ended = true;
                        }
                    }
                }
                4 => {
                    if owners[index].is_some() {
                        if let Some(exchange) = expected[index] {
                            reset[exchange.generation] |= exchange.sent && !exchange.ended;
                        }
                    }
                    drop(owners[index].take());
                }
                5 => {
                    // Only a started, unended exchange resets the session.
                    reset[generation] |= expected.iter().zip(&owners).any(|(exchange, owner)| {
                        owner.is_some() && exchange.is_some_and(|exchange| {
                            exchange.generation == generation && exchange.sent && !exchange.ended
                        })
                    });
                    prop_assert_eq!(fences[generation].retire(), !ended(closed, &reset, generation));
                    prop_assert!(fences[generation].open().is_none());
                    // A replacement session always opens on the same connection.
                    sessions.push(connection.child_token());
                    fences.push(WriterFence::new(sessions[generation + 1].clone()));
                    reset.push(false);
                }
                6 => {
                    connection.cancel();
                    closed = true;
                }
                _ => {}
            }
            prop_assert_eq!(connection.is_cancelled(), closed);
            for (session, reset) in sessions.iter().zip(&reset) {
                prop_assert_eq!(session.is_cancelled(), closed || *reset);
            }
            // Every old generation stays fenced while its writers and owners live.
            for (writer, exchange) in writers.iter().zip(&expected) {
                if let (Some(writer), Some(exchange)) = (writer, exchange) {
                    if ended(closed, &reset, exchange.generation) || exchange.generation < fences.len() - 1 {
                        prop_assert!(!writer.try_start(|| panic!("a fenced writer reached local work")));
                    }
                }
            }
        }

        for (exchange, owner) in expected.iter().zip(&owners) {
            if let (Some(exchange), Some(_)) = (exchange, owner) {
                reset[exchange.generation] |= exchange.sent && !exchange.ended;
            }
        }
        drop(owners);
        prop_assert_eq!(connection.is_cancelled(), closed);
        for (session, reset) in sessions.iter().zip(&reset) {
            prop_assert_eq!(session.is_cancelled(), closed || *reset);
        }
        for writer in writers.iter().flatten() {
            prop_assert!(!writer.try_start(|| panic!("dropping the owner fences its writer")));
        }
    }
}
