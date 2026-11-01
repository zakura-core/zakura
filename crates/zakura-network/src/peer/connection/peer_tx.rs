//! The peer message sender channel.

use futures::{FutureExt, Sink, SinkExt};

use zakura_chain::serialization::SerializationError;

use crate::{constants::REQUEST_TIMEOUT, protocol::external::Message, PeerError};

/// A wrapper type for a peer connection message sender.
///
/// Used to apply a timeout to send messages.
#[derive(Clone, Debug)]
pub struct PeerTx<Tx>
where
    Tx: Sink<Message, Error = SerializationError> + Unpin,
{
    /// A channel for sending Zcash messages to the connected peer.
    ///
    /// This channel accepts [`Message`]s.
    inner: Tx,
}

impl<Tx> PeerTx<Tx>
where
    Tx: Sink<Message, Error = SerializationError> + Unpin,
{
    /// Sends `msg` on `self.inner`, returning a timeout error if it takes too long.
    pub async fn send(&mut self, msg: Message) -> Result<(), PeerError> {
        tokio::time::timeout(REQUEST_TIMEOUT, self.inner.send(msg))
            .await
            .map_err(|_| PeerError::ConnectionSendTimeout)?
            .map_err(Into::into)
    }

    /// Sends `msg` if `authorized()` still holds once the sink is ready, rechecking it
    /// immediately before starting the local write.
    ///
    /// Returns `Ok(false)` without writing anything if authorization was revoked, and a timeout
    /// error if readiness and the write together take too long.
    pub(crate) async fn send_authorized(
        &mut self,
        msg: Message,
        authorized: impl FnOnce() -> bool,
    ) -> Result<bool, PeerError> {
        tokio::time::timeout(REQUEST_TIMEOUT, async {
            futures::future::poll_fn(|cx| std::pin::Pin::new(&mut self.inner).poll_ready(cx))
                .await?;
            if !authorized() {
                return Ok(false);
            }
            std::pin::Pin::new(&mut self.inner).start_send(msg)?;
            self.inner.flush().await?;
            Ok(true)
        })
        .await
        .map_err(|_| PeerError::ConnectionSendTimeout)?
    }

    /// Flush any remaining output and close this [`PeerTx`], if necessary.
    ///
    /// Returns a timeout error if flushing takes too long.
    pub async fn close(&mut self) -> Result<(), PeerError> {
        tokio::time::timeout(REQUEST_TIMEOUT, self.inner.close())
            .await
            .map_err(|_| PeerError::ConnectionSendTimeout)?
            .map_err(Into::into)
    }
}

impl<Tx> From<Tx> for PeerTx<Tx>
where
    Tx: Sink<Message, Error = SerializationError> + Unpin,
{
    fn from(tx: Tx) -> Self {
        PeerTx { inner: tx }
    }
}

impl<Tx> Drop for PeerTx<Tx>
where
    Tx: Sink<Message, Error = SerializationError> + Unpin,
{
    fn drop(&mut self) {
        // Do a last-ditch close attempt on the sink
        self.inner.close().now_or_never();
    }
}
