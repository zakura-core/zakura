//! Connection tests for warm-up pings.

use futures::{channel::oneshot, SinkExt, StreamExt};
use indexmap::IndexSet;
use tracing::Span;

use zakura_chain::block;
use zakura_test::mock_service::PanicAssertion;

use crate::{
    peer::{tcp_warmup::WarmupChunk, ClientRequest},
    protocol::external::{InventoryHash, Message},
    types::Nonce,
    Request, Response,
};

use super::new_test_connection;

/// Returns a warm-up ping and the receiver that fires on its pong.
fn warmup_chunk(nonce: Nonce, padding: u32) -> (WarmupChunk, tokio::sync::oneshot::Receiver<()>) {
    let (pong, pong_rx) = tokio::sync::oneshot::channel();
    (
        WarmupChunk {
            nonce,
            padding,
            pong,
        },
        pong_rx,
    )
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
    let (chunk, warmup_pong) = warmup_chunk(warmup_nonce, 100);
    warmup_tx.send(chunk).await.unwrap();
    assert_eq!(
        outbound.next().await,
        Some(Message::PaddedPing {
            nonce: warmup_nonce,
            padding: 100
        })
    );

    peer_tx.send(Ok(Message::Pong(warmup_nonce))).await.unwrap();
    warmup_pong.await.expect("the warm-up pong is delivered");

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

    let (chunk, _warmup_pong) = warmup_chunk(Nonce(3), 1000);
    warmup_tx.try_send(chunk).unwrap();
    let hash = block::Hash([7; 32]);
    let (result, _delivery) = tokio::sync::oneshot::channel();
    advert_tx
        .try_send(crate::peer::block_gossip::BlockAdvert { hash, result })
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
    let (chunk, warmup_pong) = warmup_chunk(warmup_nonce, 10);
    warmup_tx.send(chunk).await.unwrap();
    assert!(matches!(
        outbound.next().await,
        Some(Message::PaddedPing { .. })
    ));

    peer_tx.send(Ok(Message::Pong(warmup_nonce))).await.unwrap();
    warmup_pong.await.expect("the warm-up pong is delivered");
    assert!(
        block_rx.try_recv().unwrap().is_none(),
        "the block request still awaits its block"
    );
    assert!(error_slot.try_get_error().is_none());

    drop(peer_tx);
    connection.await.unwrap();
}
