//! The peer message sender channel.

use futures::{FutureExt, Sink, SinkExt};

use zakura_chain::serialization::{SerializationError, ZcashSerialize};

use crate::{
    constants::REQUEST_TIMEOUT,
    lifecycle_trace::{ConnectionTrace, SendTrace},
    peer::tcp_progress::{UploadProgress, UploadWatch},
    protocol::external::{codec::HEADER_LEN, Message},
    PeerError,
};

/// A wrapper type for a peer connection message sender.
///
/// Used to apply a timeout to send messages.
#[derive(Debug)]
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

    /// Watches the delivery of block bodies, when the `p2p` trace table records it and the
    /// platform supports it.
    pub(super) upload_progress: Option<UploadProgress>,
}

impl<Tx> PeerTx<Tx>
where
    Tx: Sink<Message, Error = SerializationError> + Unpin,
{
    /// Creates a sender for `tx` whose sends are traced with `trace`.
    pub(crate) fn new(tx: Tx, trace: ConnectionTrace) -> Self {
        PeerTx {
            inner: tx,
            trace,
            upload_progress: None,
        }
    }

    /// Records the delivery of every block body sent on this connection with `progress`.
    ///
    /// `progress` must be the only owner of the connection's socket handle, so that the handle
    /// closes with this sender.
    pub(crate) fn set_upload_progress(&mut self, progress: Option<UploadProgress>) {
        self.upload_progress = progress;
    }

    /// Sends `msg` on `self.inner`, returning a timeout error if it takes too long.
    pub async fn send(&mut self, msg: Message) -> Result<(), PeerError> {
        let send = self.trace.p2p_send_start(&msg);
        let watched = self.watch_body_upload(&msg, send.as_ref());
        let result = tokio::time::timeout(REQUEST_TIMEOUT, self.inner.send(msg))
            .await
            .map_err(|_| PeerError::ConnectionSendTimeout)
            .and_then(|sent| sent.map_err(Into::into));
        let tcp = watched
            .then(|| self.upload_progress.as_ref()?.sample())
            .flatten();
        self.trace.p2p_send_done(send, &result, tcp.as_ref());
        result
    }

    /// Starts watching the delivery of `msg` if it is a block body and its send is traced, and
    /// returns whether it did.
    ///
    /// The watch runs in its own task, so the connection never waits for it. It ends when the
    /// peer acknowledges the body, the connection closes, or the watch times out, and then
    /// writes its result to the `p2p` table.
    fn watch_body_upload(&self, msg: &Message, send: Option<&SendTrace>) -> bool {
        let (Some(progress), Some(send), Message::Block(block)) =
            (&self.upload_progress, send, msg)
        else {
            return false;
        };
        // A block's encoded size is far below `u64::MAX`.
        let message_len = (block.zcash_serialized_size() + HEADER_LEN) as u64;
        let Some(watch) = progress.start(message_len) else {
            return false;
        };

        tokio::spawn(report_body_upload(self.trace.clone(), send.seq(), watch));
        true
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

/// Waits for `watch` to end, then records its result for the send with sequence number `seq`.
async fn report_body_upload(trace: ConnectionTrace, seq: u64, watch: UploadWatch) {
    let report = watch.wait_acked().await;
    trace.p2p_body_upload(seq, &report);
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
