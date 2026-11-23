//! Connection tests for warm-up pings.

use std::{net::Ipv4Addr, pin::pin, sync::Arc};

use futures::{channel::oneshot, poll, SinkExt, StreamExt};
use indexmap::IndexSet;
use tokio::sync::{mpsc, watch, OwnedSemaphorePermit, Semaphore};
use tracing::Span;

use zakura_chain::block;
use zakura_test::mock_service::PanicAssertion;

use crate::{
    config::TcpWarmupConfig,
    peer::{
        tcp_warmup::{TcpWarmup, WarmupChunk, WARMUP_QUEUE},
        ClientRequest,
    },
    protocol::external::{InventoryHash, Message},
    types::Nonce,
    Request, Response,
};

use super::new_test_connection;

/// The permit and receivers that a warm-up task holds for one ping.
struct WarmupWaits {
    _permit: Arc<OwnedSemaphorePermit>,
    written: tokio::sync::oneshot::Receiver<()>,
    pong: tokio::sync::oneshot::Receiver<()>,
}

/// Returns a warm-up ping and the receivers that fire on its write and its pong.
fn warmup_chunk(nonce: Nonce, padding: u32) -> (WarmupChunk, WarmupWaits) {
    let permit = Arc::new(Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap());
    let (written, written_rx) = tokio::sync::oneshot::channel();
    let (pong, pong_rx) = tokio::sync::oneshot::channel();
    (
        WarmupChunk {
            nonce,
            padding,
            permit: Arc::downgrade(&permit),
            written,
            pong,
        },
        WarmupWaits {
            _permit: permit,
            written: written_rx,
            pong: pong_rx,
        },
    )
}

#[tokio::test(start_paused = true)]
async fn a_stalled_warmup_write_keeps_its_permit_past_the_pong_deadline() {
    let config = TcpWarmupConfig {
        max_concurrent: 1,
        ..TcpWarmupConfig::default()
    };
    let (_tip_tx, tip_rx) = watch::channel(None);
    let warmup = TcpWarmup::new(config, tip_rx);

    let (first_tx, mut first_rx) = mpsc::channel(WARMUP_QUEUE);
    let first = tokio::spawn(
        warmup
            .clone()
            .run(Ipv4Addr::new(192, 0, 2, 1).into(), first_tx),
    );
    let probe = first_rx.recv().await.unwrap();
    assert_eq!(probe.padding, 0);
    probe.written.send(()).unwrap();
    probe.pong.send(()).unwrap();

    let padded = first_rx.recv().await.unwrap();
    assert!(padded.padding > 0);
    let (mut connection, _client_tx, _inbound, mut outbound, _error_slot) =
        new_test_connection::<PanicAssertion>();
    let mut write = pin!(connection.send_warmup_ping(padded));
    assert!(
        poll!(&mut write).is_pending(),
        "the peer has not read the ping"
    );
    first.await.unwrap();

    let (second_tx, mut second_rx) = mpsc::channel(WARMUP_QUEUE);
    let mut second = pin!(warmup.run(Ipv4Addr::new(192, 0, 2, 2).into(), second_tx));
    assert!(poll!(&mut second).is_pending());
    assert_eq!(
        second_rx.try_recv().unwrap_err(),
        mpsc::error::TryRecvError::Empty,
        "the next warm-up cannot start while the first write is pending"
    );

    assert!(matches!(
        outbound.next().await,
        Some(Message::PaddedPing { .. })
    ));
    write.await;
    assert!(poll!(&mut second).is_pending());
    assert_eq!(second_rx.try_recv().unwrap().padding, 0);
}

/// A warm-up pong that arrives while a heartbeat ping awaits its own pong reaches the warm-up,
/// and the heartbeat still completes with its own pong.
#[tokio::test]
async fn warmup_pong_is_delivered_while_a_heartbeat_is_pending() {
    let _init_guard = zakura_test::init();

    let (mut peer_tx, peer_rx) = futures::channel::mpsc::channel(1);
    let (mut connection, mut client_tx, _inbound, mut outbound, error_slot) =
        new_test_connection::<PanicAssertion>();
    let (warmup_tx, warmup_rx) = tokio::sync::mpsc::channel(1);
    connection.warmup_rx = warmup_rx;
    let connection = tokio::spawn(connection.run(peer_rx));

    let heartbeat_nonce = Nonce(1);
    let (heartbeat_tx, heartbeat_rx) = oneshot::channel();
    client_tx
        .send(ClientRequest {
            advertised_block: None,
            request: Request::Ping(heartbeat_nonce),
            tx: heartbeat_tx,
            inv_collector: None,
            transient_addr: None,
            span: Span::none(),
        })
        .await
        .unwrap();
    assert_eq!(outbound.next().await, Some(Message::Ping(heartbeat_nonce)));

    let warmup_nonce = Nonce(2);
    let (chunk, warmup) = warmup_chunk(warmup_nonce, 100);
    warmup_tx.send(chunk).await.unwrap();
    assert_eq!(
        outbound.next().await,
        Some(Message::PaddedPing {
            nonce: warmup_nonce,
            padding: 100
        })
    );

    peer_tx.send(Ok(Message::Pong(warmup_nonce))).await.unwrap();
    warmup
        .written
        .await
        .expect("the connection reports the write");
    warmup.pong.await.expect("the warm-up pong is delivered");

    peer_tx
        .send(Ok(Message::Pong(heartbeat_nonce)))
        .await
        .unwrap();
    assert!(matches!(heartbeat_rx.await.unwrap(), Ok(Response::Pong(_))));
    assert!(error_slot.try_get_error().is_none());

    drop(peer_tx);
    connection.await.unwrap();
}

/// A block advert and a warm-up ping queued together are written advert first.
#[tokio::test]
async fn block_advert_is_written_before_a_queued_warmup_ping() {
    let _init_guard = zakura_test::init();

    let (peer_tx, peer_rx) = futures::channel::mpsc::channel::<Result<Message, _>>(1);
    let (mut connection, _client_tx, _inbound, mut outbound, _error_slot) =
        new_test_connection::<PanicAssertion>();
    let (warmup_tx, warmup_rx) = tokio::sync::mpsc::channel(1);
    connection.warmup_rx = warmup_rx;
    let (advert_tx, advert_rx) =
        tokio::sync::mpsc::channel(crate::peer::block_gossip::BLOCK_ADVERT_QUEUE);
    connection.block_advert_rx = advert_rx;

    let (chunk, _warmup) = warmup_chunk(Nonce(3), 1000);
    warmup_tx.try_send(chunk).unwrap();
    let hash = block::Hash([7; 32]);
    let (result, _delivery) = tokio::sync::oneshot::channel();
    advert_tx
        .try_send(crate::peer::block_gossip::BlockAdvert {
            hash,
            push: false,
            pending_send: None,
            result,
        })
        .unwrap();

    let connection = tokio::spawn(connection.run(peer_rx));

    assert_eq!(
        outbound.next().await,
        Some(Message::Inv(vec![InventoryHash::Block(hash)]))
    );
    assert!(matches!(
        outbound.next().await,
        Some(Message::PaddedPing { padding: 1000, .. })
    ));

    drop(peer_tx);
    connection.await.unwrap();
}

/// A warm-up pong that arrives while a block request awaits its response reaches the warm-up,
/// and the block request still waits for its block.
#[tokio::test]
async fn warmup_pong_is_delivered_while_a_block_request_is_pending() {
    let _init_guard = zakura_test::init();

    let (mut peer_tx, peer_rx) = futures::channel::mpsc::channel(1);
    let (mut connection, mut client_tx, _inbound, mut outbound, error_slot) =
        new_test_connection::<PanicAssertion>();
    let (warmup_tx, warmup_rx) = tokio::sync::mpsc::channel(1);
    connection.warmup_rx = warmup_rx;
    let connection = tokio::spawn(connection.run(peer_rx));

    let hash = block::Hash([9; 32]);
    let (block_tx, mut block_rx) = oneshot::channel();
    client_tx
        .send(ClientRequest {
            advertised_block: None,
            request: Request::BlocksByHash(IndexSet::from([hash])),
            tx: block_tx,
            inv_collector: None,
            transient_addr: None,
            span: Span::none(),
        })
        .await
        .unwrap();
    assert_eq!(
        outbound.next().await,
        Some(Message::GetData(vec![InventoryHash::Block(hash)]))
    );

    let warmup_nonce = Nonce(4);
    let (chunk, warmup) = warmup_chunk(warmup_nonce, 10);
    warmup_tx.send(chunk).await.unwrap();
    assert!(matches!(
        outbound.next().await,
        Some(Message::PaddedPing { .. })
    ));

    peer_tx.send(Ok(Message::Pong(warmup_nonce))).await.unwrap();
    warmup
        .written
        .await
        .expect("the connection reports the write");
    warmup.pong.await.expect("the warm-up pong is delivered");
    assert!(
        block_rx.try_recv().unwrap().is_none(),
        "the block request still awaits its block"
    );
    assert!(error_slot.try_get_error().is_none());

    drop(peer_tx);
    connection.await.unwrap();
}

/// A warm-up ping whose warm-up stopped while it was queued is not written, and the next
/// ping still is.
#[tokio::test]
async fn warmup_ping_is_skipped_after_its_warmup_stops() {
    let _init_guard = zakura_test::init();

    let (peer_tx, peer_rx) = futures::channel::mpsc::channel::<Result<Message, _>>(1);
    let (mut connection, _client_tx, _inbound, mut outbound, error_slot) =
        new_test_connection::<PanicAssertion>();
    let (warmup_tx, warmup_rx) = tokio::sync::mpsc::channel(1);
    connection.warmup_rx = warmup_rx;

    let (stale, stopped) = warmup_chunk(Nonce(5), 1000);
    drop(stopped);
    warmup_tx.try_send(stale).unwrap();
    let connection = tokio::spawn(connection.run(peer_rx));

    let (live, warmup) = warmup_chunk(Nonce(6), 2000);
    warmup_tx.send(live).await.unwrap();
    assert_eq!(
        outbound.next().await,
        Some(Message::PaddedPing {
            nonce: Nonce(6),
            padding: 2000
        }),
        "the stopped ping is skipped"
    );
    warmup
        .written
        .await
        .expect("the connection reports the write");
    assert!(error_slot.try_get_error().is_none());

    drop(peer_tx);
    connection.await.unwrap();
}

/// A pong that arrives after its warm-up stopped waiting leaves the connection open.
#[tokio::test]
async fn late_warmup_pong_keeps_the_connection_open() {
    let _init_guard = zakura_test::init();

    let (mut peer_tx, peer_rx) = futures::channel::mpsc::channel(1);
    let (mut connection, _client_tx, _inbound, mut outbound, error_slot) =
        new_test_connection::<PanicAssertion>();
    let (warmup_tx, warmup_rx) = tokio::sync::mpsc::channel(1);
    connection.warmup_rx = warmup_rx;
    let connection = tokio::spawn(connection.run(peer_rx));

    let (chunk, warmup) = warmup_chunk(Nonce(7), 1000);
    warmup_tx.send(chunk).await.unwrap();
    assert!(matches!(
        outbound.next().await,
        Some(Message::PaddedPing { .. })
    ));
    warmup
        .written
        .await
        .expect("the connection reports the write");
    drop(warmup.pong);

    peer_tx.send(Ok(Message::Pong(Nonce(7)))).await.unwrap();
    peer_tx.send(Ok(Message::Pong(Nonce(8)))).await.unwrap();
    assert!(error_slot.try_get_error().is_none());

    drop(peer_tx);
    connection.await.unwrap();
}
