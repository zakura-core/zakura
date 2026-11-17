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
}

impl<S: Sink<Message, Error = SerializationError> + Unpin> Sink<Message> for HeldWriter<S> {
    type Error = SerializationError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
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
        let (result, delivery) = oneshot::channel();
        let mut write = Box::pin(connection.send_block_advert(BlockAdvert { hash, result }, None));
        assert!(
            write.as_mut().now_or_never().is_none(),
            "the live advert reaches the held writer"
        );

        // This receiver is dropped by the broadcast owner when local validation resolves.
        drop(delivery);
        release.send(()).unwrap();
        write.await;

        let (result, delivery) = oneshot::channel();
        connection
            .send_block_advert(BlockAdvert { hash, result }, None)
            .await;
        delivery.await.unwrap().unwrap();
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
