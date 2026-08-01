//! The peer message sender channel.

use futures::{FutureExt, Sink, SinkExt};

use zakura_chain::serialization::SerializationError;

use crate::{
    constants::REQUEST_TIMEOUT,
    protocol::external::{codec::InvGossipState, InventoryHash, Message},
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
    inv_gossip_state: InvGossipState,
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

    /// Sends a single-block inventory with the trailing block-gossip tag.
    pub async fn send_block_gossip(
        &mut self,
        hash: zakura_chain::block::Hash,
    ) -> Result<(), PeerError> {
        self.inv_gossip_state.tag_next_outbound_inv();
        self.send(Message::Inv(vec![InventoryHash::Block(hash)]))
            .await
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

impl<Tx> PeerTx<Tx>
where
    Tx: Sink<Message, Error = SerializationError> + Unpin,
{
    pub fn new(inner: Tx, inv_gossip_state: InvGossipState) -> Self {
        PeerTx {
            inner,
            inv_gossip_state,
        }
    }
}

impl<Tx> From<Tx> for PeerTx<Tx>
where
    Tx: Sink<Message, Error = SerializationError> + Unpin,
{
    fn from(inner: Tx) -> Self {
        Self::new(inner, InvGossipState::default())
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
