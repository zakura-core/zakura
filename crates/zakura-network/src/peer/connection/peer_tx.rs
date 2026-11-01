//! The peer message sender channel.

use futures::{FutureExt, Sink, SinkExt};

use zakura_chain::serialization::SerializationError;

use crate::{
    constants::REQUEST_TIMEOUT, lifecycle_trace::ConnectionTrace, protocol::external::Message,
    PeerError,
};

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

    /// Records every send attempt and its write completion in the `p2p` trace table.
    trace: ConnectionTrace,
}

impl<Tx> PeerTx<Tx>
where
    Tx: Sink<Message, Error = SerializationError> + Unpin,
{
    /// Creates a sender for `tx` whose sends are traced with `trace`.
    pub(crate) fn new(tx: Tx, trace: ConnectionTrace) -> Self {
        PeerTx { inner: tx, trace }
    }

    /// Sends `msg` on `self.inner`, returning a timeout error if it takes too long.
    pub async fn send(&mut self, msg: Message) -> Result<(), PeerError> {
        let send = self.trace.p2p_send_start(&msg);
        let result = tokio::time::timeout(REQUEST_TIMEOUT, self.inner.send(msg))
            .await
            .map_err(|_| PeerError::ConnectionSendTimeout)
            .and_then(|sent| sent.map_err(Into::into));
        self.trace.p2p_send_done(send, &result);
        result
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
        PeerTx::new(tx, ConnectionTrace::untraced())
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
