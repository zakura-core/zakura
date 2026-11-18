//! Cancellation at the production connection's last check before a TCP write.

use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures::{Future, FutureExt, Sink, StreamExt};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
    time::timeout,
};
use tokio_util::codec::Framed;
use zakura_chain::{block, serialization::SerializationError};
use zakura_node_services::block_relay::{PendingBlockSendPermit, PendingBlockStatus, ServedBlock};
use zakura_test::mock_service::PanicAssertion;

use crate::{
    peer::{block_gossip::BlockAdvert, Connection, ErrorSlot},
    peer_set::ActiveConnectionCounter,
    protocol::external::{Codec, InventoryHash, Message},
};

/// Holds writer readiness without changing serialization or the TCP transport.
struct HeldWriter<S> {
    inner: S,
    release: Option<oneshot::Receiver<()>>,
    entered: Option<oneshot::Sender<()>>,
}

impl<S: Sink<Message, Error = SerializationError> + Unpin> Sink<Message> for HeldWriter<S> {
    type Error = SerializationError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if let Some(entered) = self.entered.take() {
            let _ = entered.send(());
        }
        if let Some(release) = self.release.as_mut() {
            std::task::ready!(Pin::new(release).poll(cx)).expect("the test releases the writer");
            self.release = None;
        }
        Pin::new(&mut self.inner).poll_ready(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, message: Message) -> Result<(), Self::Error> {
        Pin::new(&mut self.inner).start_send(message)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_close(cx)
    }
}

/// Withdrawal while the writer is blocked must suppress the stale inventory on the socket,
/// including when the next submission has the same header hash. It must leave the peer usable.
#[tokio::test]
async fn cancelled_block_advert_is_not_written_after_writer_readiness() {
    queued_advert(None).await;
}

/// Rejection must reach the writer without waiting for the gossip owner to drop its receiver.
#[tokio::test]
async fn rejected_block_advert_is_not_written_while_owner_is_still_waiting() {
    queued_advert(Some(PendingBlockStatus::Failed)).await;
}

#[tokio::test]
async fn committed_block_advert_still_writes_after_writer_readiness() {
    queued_advert(Some(PendingBlockStatus::Committed)).await;
}

async fn queued_advert(resolution: Option<PendingBlockStatus>) {
    let _init = zakura_test::init();
    timeout(Duration::from_secs(10), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (remote, _) = listener.accept().await.unwrap();
        let mut received = Framed::new(remote, Codec::builder().finish());
        let (release, held) = oneshot::channel();
        let writer = HeldWriter {
            inner: Framed::new(local, Codec::builder().finish()),
            release: Some(held),
            entered: None,
        };
        let (template, _client, inbound, _outbound, _) =
            super::new_test_connection::<PanicAssertion>();
        let (_client_tx, client_rx) = futures::channel::mpsc::channel(0);
        let errors = ErrorSlot::default();
        let info = template.connection_info.clone();
        let mut connection = Connection::new(
            inbound,
            super::test_connection_channels(client_rx),
            errors.clone(),
            writer,
            ActiveConnectionCounter::new_counter().track_connection(),
            info.clone(),
            info.connected_addr.get_transient_addr_label(),
            Vec::new(),
            crate::inbound_resources::InboundBudget::new(1 << 30),
        );
        let hash = block::Hash([42; 32]);
        let (status, permit) = PendingBlockSendPermit::channel();
        let (result, delivery) = oneshot::channel();
        let mut delivery = Some(delivery);
        let mut write = Box::pin(connection.send_block_advert(
            BlockAdvert {
                hash,
                result,
                push: false,
                pending_send: Some(permit),
            },
            None,
        ));
        assert!(
            write.as_mut().now_or_never().is_none(),
            "the live advert reaches the held writer"
        );

        if let Some(resolution) = resolution {
            status.send_replace(resolution);
        } else {
            drop(delivery.take());
        }
        release.send(()).unwrap();
        write.await;
        if let Some(delivery) = delivery {
            let result = delivery.await.unwrap();
            assert_eq!(
                result.is_ok(),
                resolution == Some(PendingBlockStatus::Committed)
            );
        }

        let (result, delivery) = oneshot::channel();
        connection
            .send_block_advert(
                BlockAdvert {
                    hash,
                    result,
                    push: false,
                    pending_send: None,
                },
                None,
            )
            .await;
        delivery.await.unwrap().unwrap();
        if resolution == Some(PendingBlockStatus::Committed) {
            assert_eq!(
                received.next().await.unwrap().unwrap(),
                Message::Inv(vec![InventoryHash::Block(hash)])
            );
        }
        let marker = Message::Ping(crate::types::Nonce(123));
        connection.peer_tx.send(marker.clone()).await.unwrap();
        assert_eq!(
            received.next().await.unwrap().unwrap(),
            Message::Inv(vec![InventoryHash::Block(hash)])
        );
        assert_eq!(
            received.next().await.unwrap().unwrap(),
            marker,
            "only the live retry is advertised; the cancelled write never reached TCP"
        );
        assert!(
            errors.try_get_error().is_none(),
            "cancellation keeps the connection healthy"
        );
    })
    .await
    .expect("writer cancellation and live retry complete without stalling");
}

/// A body retained in a response remains revocable while its TCP writer is blocked.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_body_rechecks_rejection_and_preserves_committed_delivery() {
    use crate::{InventoryResponse, Request, Response};
    use zakura_chain::serialization::ZcashDeserializeInto;
    let _init = zakura_test::init();
    timeout(Duration::from_secs(10), async {
        for outcome in [PendingBlockStatus::Failed, PendingBlockStatus::Committed] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let local = TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let (remote, _) = listener.accept().await.unwrap();
            let mut received = Framed::new(remote, post_handshake_codec());
            let (release, held) = oneshot::channel();
            let (entered, writing) = oneshot::channel();
            let writer = HeldWriter {
                inner: Framed::new(local, post_handshake_codec()),
                release: Some(held),
                entered: Some(entered),
            };
            let (template, _client, mut inbound, _outbound, _) =
                super::new_test_connection::<PanicAssertion>();
            let (_client_tx, client_rx) = futures::channel::mpsc::channel(0);
            let errors = ErrorSlot::default();
            let info = template.connection_info.clone();
            let mut connection = Connection::new(
                inbound.clone(),
                super::test_connection_channels(client_rx),
                errors.clone(),
                writer,
                ActiveConnectionCounter::new_counter().track_connection(),
                info.clone(),
                info.connected_addr.get_transient_addr_label(),
                Vec::new(),
                crate::inbound_resources::InboundBudget::new(1 << 30),
            );
            let block: std::sync::Arc<block::Block> =
                zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
                    .zcash_deserialize_into()
                    .unwrap();
            let hash = block.hash();
            let (status, permit) = PendingBlockSendPermit::channel();
            let request = Request::BlocksByHash([hash].into_iter().collect());
            let drive = async {
                connection.drive_peer_request(request).await;
                connection
                    .peer_tx
                    .send(Message::Ping(crate::types::Nonce(456)))
                    .await
                    .unwrap();
            };
            let resolve = async {
                inbound
                    .expect_request_that(|request| matches!(request, Request::BlocksByHash(_)))
                    .await
                    .respond(Response::Blocks(vec![InventoryResponse::Available((
                        ServedBlock {
                            block: block.clone().into(),
                            pending_send: Some(permit),
                        }
                        .into(),
                        None,
                    ))]));
                writing.await.unwrap();
                status.send_replace(outcome);
                release.send(()).unwrap();
            };
            tokio::join!(drive, resolve);
            let expected = if outcome == PendingBlockStatus::Failed {
                Message::NotFound(vec![InventoryHash::Block(hash)])
            } else {
                Message::Block(block.clone().into())
            };
            assert_eq!(received.next().await.unwrap().unwrap(), expected);
            assert_eq!(
                received.next().await.unwrap().unwrap(),
                Message::Ping(crate::types::Nonce(456))
            );
            assert!(errors.try_get_error().is_none());
            // A fresh attempt with the same header is independent of the rejected body.
            let (_, retry) = PendingBlockSendPermit::channel();
            assert!(connection
                .peer_tx
                .send_authorized(Message::Block(block.clone().into()), || true, Some(retry))
                .await
                .unwrap());
            assert_eq!(
                received.next().await.unwrap().unwrap(),
                Message::Block(block.into())
            );
        }
    })
    .await
    .expect("withdrawal, commitment, and retry keep the connection usable");
}

/// The fixture represents an established connection, whose codec accepts full block bodies.
fn post_handshake_codec() -> Codec {
    let mut codec = Codec::builder().finish();
    codec.reconfigure_full_body_len();
    codec
}

/// Real TCP decoding checks both temporary sends and a committed send under one header.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn temporary_body_checksums_do_not_poison_same_header_retries() {
    use zakura_chain::serialization::{ZcashDeserializeInto, ZcashSerialize};
    let _init = zakura_test::init();
    timeout(Duration::from_secs(20), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (remote, _) = listener.accept().await.unwrap();
        let mut sender = crate::peer::connection::peer_tx::PeerTx::from(Framed::new(
            local,
            post_handshake_codec(),
        ));
        let mut received = Framed::new(remote, post_handshake_codec());
        let valid: std::sync::Arc<block::Block> = zakura_test::vectors::BLOCK_TESTNET_141042_BYTES
            .zcash_deserialize_into()
            .unwrap();
        let mut changed = (*valid).clone();
        std::sync::Arc::make_mut(&mut changed.transactions[0]).outputs_mut()[0].value =
            zakura_chain::amount::Amount::try_from(1u64).unwrap();
        let changed = std::sync::Arc::new(changed);
        assert_eq!(valid.hash(), changed.hash());
        assert_ne!(valid, changed);
        assert_eq!(
            valid.zcash_serialized_size(),
            changed.zcash_serialized_size()
        );

        let (rejected, first) = PendingBlockSendPermit::channel();
        let (retry_status, retry) = PendingBlockSendPermit::channel();
        let (_, later) = PendingBlockSendPermit::channel();
        for (index, (body, permit)) in [
            (changed.clone(), Some(first)),
            (valid.clone(), Some(retry)),
            (valid.clone(), None),
            (changed.clone(), Some(later)),
        ]
        .into_iter()
        .enumerate()
        {
            let message = Message::Block(body.into());
            let (sent, decoded) = tokio::join!(
                sender.send_authorized(message.clone(), || true, permit),
                received.next(),
            );
            assert!(sent.unwrap());
            assert_eq!(
                decoded
                    .unwrap()
                    .expect("each block has the checksum of its own bytes"),
                message
            );
            if index == 0 {
                rejected.send_replace(PendingBlockStatus::Failed);
            } else if index == 1 {
                retry_status.send_replace(PendingBlockStatus::Committed);
            }
        }
    })
    .await
    .expect("large temporary and committed bodies remain decodable");
}
