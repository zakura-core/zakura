//! Writer fence: #978's six unit tests and lifecycle proptest, renamed, and
//! the fenced send.

use std::time::Duration;

use proptest::prelude::*;

use super::*;
use crate::zakura::framed_channel;

fn fence() -> (WriterFence, CancellationToken, CloseCause) {
    let connection = CancellationToken::new();
    let cause = CloseCause::new();
    (
        WriterFence::new(connection.clone(), cause.clone()),
        connection,
        cause,
    )
}

#[test]
fn retirement_fences_prepared_and_queued_writes_without_closing() {
    for queued in [false, true] {
        let (fence, connection, _) = fence();
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
        assert!(!connection.is_cancelled());
    }
}

#[test]
fn owner_drop_fences_unstarted_writes_and_closes_started_exchanges() {
    for started in [false, true] {
        let (fence, connection, cause) = fence();
        let exchange = fence.open().unwrap();
        let writer = exchange.writer();
        assert!(writer.publish(|| {}));
        if started {
            assert!(writer.try_start(|| true));
        }
        drop(exchange);
        assert!(!writer.try_start(|| panic!("the exchange's owner has gone")));
        assert_eq!(connection.is_cancelled(), started);
        assert_eq!(fence.retire(), !started);
        if started {
            assert_eq!(cause.get_or("unset"), UNFINISHED_EXCHANGE);
        }
    }
}

#[test]
fn only_endings_release_started_exchanges() {
    for end_both in [false, true] {
        let (fence, connection, _) = fence();
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
        assert_eq!(connection.is_cancelled(), !end_both);
    }
}

#[test]
fn a_failed_work_claim_leaves_the_connection_reusable() {
    let (fence, connection, _) = fence();
    let exchange = fence.open().unwrap();
    let writer = exchange.writer();
    assert!(writer.publish(|| {}));
    assert!(!writer.try_start(|| false));
    assert!(fence.retire());
    assert!(!connection.is_cancelled());
}

#[test]
fn retirement_and_first_write_have_one_winner() {
    for _ in 0..128 {
        let (fence, connection, _) = fence();
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
        assert_eq!(connection.is_cancelled(), started);
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
        let (fence, connection, _) = fence();
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
            assert!(!connection.is_cancelled());
        } else {
            assert_eq!(output.recv().await, Some(frame()));
            exchange.end();
            assert!(fence.retire());
            assert!(!connection.is_cancelled());
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct ExpectedExchange {
    generation: usize,
    published: bool,
    sent: bool,
    ended: bool,
}

proptest! {
    /// Random histories of eight exchanges over retiring fences on one
    /// connection. After every step, every writer of a retired fence or of a
    /// closed connection stays fenced, and the connection is closed exactly
    /// when a started exchange lost its owner or its fence without an ending.
    #[test]
    fn exchange_histories_keep_old_writers_fenced(
        actions in prop::collection::vec((0u8..7, 0usize..8, any::<bool>()), 1..192),
    ) {
        let connection = CancellationToken::new();
        let cause = CloseCause::new();
        let mut fences = vec![WriterFence::new(connection.clone(), cause.clone())];
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
                    prop_assert_eq!(opened.is_some(), !closed);
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
                        let allowed = owners[index].is_some() && !closed
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
                        let eligible = owners[index].is_some() && !closed
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
                        closed |= expected[index].is_some_and(|exchange| exchange.sent && !exchange.ended);
                    }
                    drop(owners[index].take());
                }
                5 => {
                    // Only a started, unended exchange stops a replacement on
                    // the same connection.
                    closed |= expected.iter().zip(&owners).any(|(exchange, owner)| {
                        owner.is_some() && exchange.is_some_and(|exchange| {
                            exchange.generation == generation && exchange.sent && !exchange.ended
                        })
                    });
                    prop_assert_eq!(fences[generation].retire(), !closed);
                    prop_assert!(fences[generation].open().is_none());
                    if !closed {
                        fences.push(WriterFence::new(connection.clone(), cause.clone()));
                    }
                }
                6 => {
                    connection.cancel();
                    closed = true;
                }
                _ => {}
            }
            prop_assert_eq!(connection.is_cancelled(), closed);
            // Every old generation stays fenced while its writers and owners live.
            for (writer, exchange) in writers.iter().zip(&expected) {
                if let (Some(writer), Some(exchange)) = (writer, exchange) {
                    if closed || exchange.generation < fences.len() - 1 {
                        prop_assert!(!writer.try_start(|| panic!("a fenced writer reached local work")));
                    }
                }
            }
        }

        closed |= expected.iter().zip(&owners).any(|(exchange, owner)| {
            owner.is_some() && exchange.is_some_and(|exchange| exchange.sent && !exchange.ended)
        });
        drop(owners);
        prop_assert_eq!(connection.is_cancelled(), closed);
        for writer in writers.iter().flatten() {
            prop_assert!(!writer.try_start(|| panic!("dropping the owner fences its writer")));
        }
    }
}
