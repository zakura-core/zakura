//! Session table: #945's churn and stale-key tests, #978's fencing scenarios
//! on the table, and an operation-sequence proptest.

use proptest::prelude::*;

use super::*;
use crate::zakura::{
    framed_channel,
    regulation::{FencedSendError, UNFINISHED_EXCHANGE},
    CloseCause, Frame, FramedRecv, FramedSend,
};

fn peer(byte: u8) -> ZakuraPeerId {
    ZakuraPeerId::new(vec![byte; 32]).unwrap()
}

/// One connection: its token and close cause.
#[derive(Clone)]
struct Connection {
    cancel: CancellationToken,
    cause: CloseCause,
}

impl Connection {
    fn new() -> Self {
        Self {
            cancel: CancellationToken::new(),
            cause: CloseCause::new(),
        }
    }

    /// A session on this connection, with its request stream.
    fn session(
        &self,
        conn_id: ZakuraConnId,
        session_id: u64,
    ) -> (Current<u64>, FramedSend, FramedRecv) {
        let (send, recv) = framed_channel(4);
        (
            Current {
                key: SessionKey {
                    conn_id,
                    session_id,
                },
                cancel: self.cancel.child_token(),
                fence: WriterFence::new(self.cancel.clone(), self.cause.clone()),
                session: session_id,
            },
            send,
            recv,
        )
    }
}

fn request() -> Frame {
    Frame {
        message_type: 1,
        flags: 0,
        payload: vec![1],
    }
}

#[tokio::test]
async fn churn_coalesces_and_changes_during_reconciliation_stay_visible() {
    let table = SessionTable::default();
    let mut changed = table.subscribe();
    let peer = peer(211);
    let mut old = Vec::new();
    for conn_id in 1..=1000 {
        let (current, _, _) = Connection::new().session(conn_id, conn_id);
        old.push(current.cancel.clone());
        table.replace(peer.clone(), current);
    }
    changed.changed().await.unwrap();
    let snapshot = table.snapshot(&mut changed);
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].2, 1000);
    assert!(old[..999].iter().all(CancellationToken::is_cancelled));
    assert!(!old[999].is_cancelled());
    assert!(
        !changed.has_changed().unwrap(),
        "one observation consumes the coalesced change"
    );
    table.remove(&peer, snapshot[0].1);
    assert!(
        changed.has_changed().unwrap(),
        "a change after the snapshot schedules another pass"
    );
    assert!(table.snapshot(&mut changed).is_empty());
}

#[test]
fn a_stale_key_cannot_remove_a_newer_session() {
    let table = SessionTable::default();
    let peer = peer(91);
    let (old, _, _) = Connection::new().session(1, 1);
    let old_key = old.key;
    let (new, _, _) = Connection::new().session(2, 2);
    let new_cancel = new.cancel.clone();
    table.replace(peer.clone(), old);
    table.replace(peer.clone(), new);
    assert!(table.remove(&peer, old_key).is_none());
    assert!(!new_cancel.is_cancelled());
    assert_eq!(table.get(&peer).map(|(_, session)| session), Some(2));
}

#[tokio::test]
async fn replacement_fences_the_old_publication_and_queued_first_write() {
    for queued in [false, true] {
        let table = SessionTable::default();
        let peer = peer(71);
        let connection = Connection::new();
        let (old, old_send, mut old_output) = connection.session(1, 1);
        let (old_fence, old_cancel) = (old.fence.clone(), old.cancel.clone());
        table.replace(peer.clone(), old);
        let exchange = old_fence.open().unwrap();
        if queued {
            old_send
                .send_fenced(request(), &exchange.writer())
                .await
                .unwrap();
        }

        let (new, new_send, mut new_output) = connection.session(1, 2);
        let new_fence = new.fence.clone();
        assert!(matches!(
            table.replace(peer.clone(), new),
            Replaced::Replaced(_)
        ));
        assert!(old_cancel.is_cancelled());
        assert!(old_fence.open().is_none());
        if queued {
            drop(old_send);
            assert_eq!(
                old_output.recv().await,
                None,
                "a fenced queued request writes no bytes"
            );
        } else {
            assert_eq!(
                old_send.send_fenced(request(), &exchange.writer()).await,
                Err(FencedSendError::Fenced)
            );
        }
        drop(exchange);
        assert!(!connection.cancel.is_cancelled());

        let mut next = new_fence.open().unwrap();
        new_send
            .send_fenced(request(), &next.writer())
            .await
            .unwrap();
        assert_eq!(new_output.recv().await, Some(request()));
        next.end();
        assert!(!connection.cancel.is_cancelled());
    }
}

#[tokio::test]
async fn replacement_closes_a_started_exchange_even_after_its_write_finished() {
    let table = SessionTable::default();
    let peer = peer(72);
    let connection = Connection::new();
    let (old, old_send, mut old_output) = connection.session(1, 1);
    let old_fence = old.fence.clone();
    table.replace(peer.clone(), old);
    let exchange = old_fence.open().unwrap();
    old_send
        .send_fenced(request(), &exchange.writer())
        .await
        .unwrap();
    // The in-process reader claims the first byte and finishes the write.
    assert_eq!(old_output.recv().await, Some(request()));

    let (new, _new_send, _new_output) = connection.session(1, 2);
    let new_cancel = new.cancel.clone();
    assert!(matches!(
        table.replace(peer.clone(), new),
        Replaced::Refused
    ));
    assert!(connection.cancel.is_cancelled());
    assert_eq!(connection.cause.get_or("unset"), UNFINISHED_EXCHANGE);
    assert!(
        new_cancel.is_cancelled(),
        "the refused session is cancelled"
    );
    assert_eq!(
        table.get(&peer).map(|(_, session)| session),
        Some(1),
        "the old session stays until its own teardown"
    );
    drop(exchange);
}

#[tokio::test]
async fn an_ended_exchange_or_a_new_connection_allows_replacement() {
    for ended in [false, true] {
        let table = SessionTable::default();
        let peer = peer(73);
        let old_connection = Connection::new();
        let (old, old_send, mut old_output) = old_connection.session(1, 1);
        let old_fence = old.fence.clone();
        table.replace(peer.clone(), old);
        let mut exchange = old_fence.open().unwrap();
        old_send
            .send_fenced(request(), &exchange.writer())
            .await
            .unwrap();
        assert_eq!(old_output.recv().await, Some(request()));
        let (next_conn_id, next_connection) = if ended {
            exchange.end();
            (1, old_connection.clone())
        } else {
            (2, Connection::new())
        };
        let (new, _, _) = next_connection.session(next_conn_id, 2);
        let new_fence = new.fence.clone();
        assert!(matches!(
            table.replace(peer.clone(), new),
            Replaced::Replaced(_)
        ));
        assert_eq!(old_connection.cancel.is_cancelled(), !ended);
        assert!(!next_connection.cancel.is_cancelled());
        assert!(new_fence.open().is_some());
    }
}

#[tokio::test]
async fn removing_a_session_fences_its_writers_before_erasing_it() {
    for started in [false, true] {
        let table = SessionTable::default();
        let peer = peer(74);
        let connection = Connection::new();
        let (session, send, mut output) = connection.session(1, 1);
        let (key, fence) = (session.key, session.fence.clone());
        table.replace(peer.clone(), session);
        let exchange = fence.open().unwrap();
        send.send_fenced(request(), &exchange.writer())
            .await
            .unwrap();
        if started {
            assert_eq!(output.recv().await, Some(request()));
        }
        let removed = table.remove(&peer, key).unwrap();
        assert!(removed.cancel.is_cancelled());
        assert!(table.get(&peer).is_none());
        assert!(fence.open().is_none());
        assert_eq!(connection.cancel.is_cancelled(), started);
        if !started {
            drop(send);
            assert_eq!(output.recv().await, None, "the queued request is skipped");
        }
        drop(exchange);
    }
}

/// One step of an operation sequence over two peers and two connections.
#[derive(Clone, Debug)]
enum Op {
    /// Replace peer `peer`'s session with a new one on connection `conn`.
    Replace { peer: u8, conn: u8 },
    /// Remove peer `peer`'s session by its current key or by a stale one.
    Remove { peer: u8, stale: bool },
    /// Open an exchange on the current session and write its first byte.
    Start { peer: u8 },
    /// End the peer's oldest started exchange.
    End { peer: u8 },
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => (0u8..2, 0u8..2).prop_map(|(peer, conn)| Op::Replace { peer, conn }),
        2 => (0u8..2, any::<bool>()).prop_map(|(peer, stale)| Op::Remove { peer, stale }),
        2 => (0u8..2).prop_map(|peer| Op::Start { peer }),
        2 => (0u8..2).prop_map(|peer| Op::End { peer }),
    ]
}

/// What the model expects of one session.
struct ModelSession {
    key: SessionKey,
    conn: usize,
    fence: WriterFence,
    /// Started exchanges without an ending.
    started: Vec<crate::zakura::regulation::Exchange>,
}

proptest! {
    /// After every step: at most one session per peer, a stale key removes
    /// nothing, every replaced or removed session's fence is retired, and a
    /// replacement on the same connection is refused exactly when the old
    /// session had a started exchange without an ending.
    #[test]
    fn operation_sequences_keep_one_fenced_session_per_peer(
        ops in prop::collection::vec(op(), 1..64),
    ) {
        let table = SessionTable::default();
        let connections = [Connection::new(), Connection::new()];
        let peers = [peer(1), peer(2)];
        let mut current: [Option<ModelSession>; 2] = [None, None];
        let mut retired: Vec<WriterFence> = Vec::new();
        let mut stale_keys: [Vec<SessionKey>; 2] = [Vec::new(), Vec::new()];
        let mut next_session = 1u64;
        for op in ops {
            match op {
                Op::Replace { peer, conn } => {
                    let (peer, conn) = (usize::from(peer), usize::from(conn));
                    if connections[conn].cancel.is_cancelled() {
                        continue;
                    }
                    // Connection ids are distinct per connection.
                    let conn_id = u64::try_from(conn).unwrap() + 1;
                    let (session, _, _) = connections[conn].session(conn_id, next_session);
                    next_session += 1;
                    let (key, fence) = (session.key, session.fence.clone());
                    let expect_refused = current[peer].as_ref().is_some_and(|old| {
                        old.conn == conn && !old.started.is_empty()
                    });
                    let outcome = table.replace(peers[peer].clone(), session);
                    prop_assert_eq!(matches!(outcome, Replaced::Refused), expect_refused);
                    match outcome {
                        Replaced::Refused => {
                            prop_assert!(connections[conn].cancel.is_cancelled());
                        }
                        Replaced::Inserted => prop_assert!(current[peer].is_none()),
                        Replaced::Replaced(_) => {
                            let old = current[peer].take().expect("the table replaced a session");
                            if !old.started.is_empty() {
                                prop_assert!(connections[old.conn].cancel.is_cancelled());
                            }
                            stale_keys[peer].push(old.key);
                            retired.push(old.fence);
                        }
                    }
                    if expect_refused {
                        retired.push(fence);
                    } else {
                        current[peer] = Some(ModelSession { key, conn, fence, started: Vec::new() });
                    }
                }
                Op::Remove { peer, stale } => {
                    let peer = usize::from(peer);
                    let key = if stale {
                        stale_keys[peer].last().copied()
                    } else {
                        current[peer].as_ref().map(|session| session.key)
                    };
                    let Some(key) = key else { continue };
                    let removed = table.remove(&peers[peer], key);
                    prop_assert_eq!(removed.is_some(), !stale);
                    if !stale {
                        let old = current[peer].take().expect("the key named the current session");
                        if !old.started.is_empty() {
                            prop_assert!(connections[old.conn].cancel.is_cancelled());
                        }
                        stale_keys[peer].push(old.key);
                        retired.push(old.fence);
                    }
                }
                Op::Start { peer } => {
                    let Some(session) = current[usize::from(peer)].as_mut() else { continue };
                    if let Some(exchange) = session.fence.open() {
                        let writer = exchange.writer();
                        let published = writer.publish(|| {});
                        prop_assert!(published);
                        prop_assert!(writer.try_start(|| true));
                        session.started.push(exchange);
                    }
                }
                Op::End { peer } => {
                    let Some(session) = current[usize::from(peer)].as_mut() else { continue };
                    if !session.started.is_empty() {
                        session.started.remove(0).end();
                    }
                }
            }
            for (peer, session) in current.iter().enumerate() {
                prop_assert_eq!(
                    table.get(&peers[peer]).map(|(key, _)| key),
                    session.as_ref().map(|session| session.key)
                );
            }
            for fence in &retired {
                prop_assert!(fence.open().is_none(), "a retired fence opens nothing");
            }
        }
    }
}
