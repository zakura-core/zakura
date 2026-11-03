//! A stand-in for a QUIC stream's reader on an in-memory channel.
//!
//! A QUIC stream's reader checks each frame's header against the stream's
//! message table and the service's response precheck before it reads the
//! payload, and holds that precheck until the connection closes. An
//! in-memory [`framed_channel`] has no reader, so the service authorizes its
//! frames itself and the precheck it would attach goes away with the
//! service. A simulator relay that forwards frames into a channel from
//! [`read_framed_channel`] holds a [`StreamReader`] and checks each frame
//! with it, as the QUIC reader would.

use std::sync::{Arc, OnceLock};

use zakura_rt::sync::CancellationToken;

use super::{
    framed_channel, CloseCause, Frame, FrameFilter, FramedRecv, FramedSend, InboundReader, Stream,
};
use crate::zakura::{regulation::PrecheckSlot, FRAME_HEADER_BYTES};

/// The close cause a QUIC stream reader records for a refused header.
const READ_REJECTED: &str = "ordered_read_rejected";

/// The reader of a [`read_framed_channel`]'s receiving end.
#[derive(Clone, Debug)]
pub struct StreamReader {
    stream: Stream,
    precheck: PrecheckSlot,
    /// The connection's close cause and token, once a peer takes the reader.
    connection: Arc<OnceLock<(CloseCause, CancellationToken)>>,
    /// Cancelled once a peer takes the reader.
    bound: CancellationToken,
}

impl StreamReader {
    /// Waits until the service attaches its precheck or starts receiving
    /// without one. A QUIC reader reads no header before then.
    pub async fn ready(&self) {
        self.precheck.ready().await;
    }

    /// Waits until the connection closes, from when a
    /// [`Peer::read_through`](super::Peer::read_through) binds this reader
    /// to it.
    pub async fn closed(&self) {
        self.bound.cancelled().await;
        self.connection().1.cancelled().await;
    }

    /// True if a QUIC stream reader would admit `frame`'s header: its row,
    /// flags, minimum length and frame cap, then the attached response
    /// precheck. [`StreamReader::refuse`] a frame it doesn't admit.
    pub fn admits(&self, frame: &Frame) -> bool {
        let frame_cap =
            usize::try_from(self.stream.frame_cap).expect("a u32 frame cap fits in usize");
        FrameFilter::new(self.stream.messages, InboundReader::Persistent)
            .with_precheck(self.precheck.get())
            .check_header(
                frame.message_type,
                frame.flags,
                frame.payload.len(),
                frame_cap,
            )
            .is_ok_and(|cap| FRAME_HEADER_BYTES.saturating_add(frame.payload.len()) <= cap)
    }

    /// Closes the connection for a refused header, with the cause a QUIC
    /// stream reader records: `ordered_read_rejected`.
    ///
    /// # Panics
    ///
    /// If no [`Peer::read_through`](super::Peer::read_through) bound this
    /// reader to its connection.
    pub fn refuse(&self) {
        let (cause, token) = self.connection();
        cause.record(READ_REJECTED);
        token.cancel();
    }

    /// Binds this reader to a connection's close cause and token.
    pub(super) fn bind(&self, cause: CloseCause, token: CancellationToken) {
        assert!(
            self.connection.set((cause, token)).is_ok(),
            "a stream reader reads one connection"
        );
        self.bound.cancel();
    }

    fn connection(&self) -> &(CloseCause, CancellationToken) {
        self.connection
            .get()
            .expect("the harness binds each reader to its peer before the peer sends")
    }
}

/// A bounded in-memory channel for `stream`'s frames whose receiving end
/// has a transport reader: the sender's end, the receiver's, and the reader
/// a relay between them admits frames through.
///
/// The receiver's service attaches its response precheck to the reader, as
/// it would to a QUIC stream's. Until it does, or starts receiving without
/// one, [`StreamReader::ready`] waits.
pub fn read_framed_channel(depth: usize, stream: Stream) -> (FramedSend, FramedRecv, StreamReader) {
    let (sender, receiver) = framed_channel(depth);
    let precheck = PrecheckSlot::default();
    if stream.messages.is_some() {
        precheck.pause();
    }
    let reader = StreamReader {
        stream,
        precheck: precheck.clone(),
        connection: Arc::default(),
        bound: CancellationToken::new(),
    };
    (sender, receiver.with_precheck(precheck), reader)
}
