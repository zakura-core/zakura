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
//!
//! A QUIC stream's worker then admits each frame the reader passed: it finds
//! the frame's row and charges the row's cadence bucket. The relay runs that
//! step through a [`StreamAdmission`].

use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};

use zakura_rt::sync::CancellationToken;

use super::{
    framed_channel, CloseCause, Frame, FrameFilter, FramedRecv, FramedSend, InboundReader,
    MessageRule, Stream,
};
use crate::zakura::{
    handler::admit_tabled_message,
    regulation::{CadenceBuckets, PrecheckSlot},
    FRAME_HEADER_BYTES,
};

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
        self.close(READ_REJECTED);
    }

    /// Closes the connection with `cause`.
    ///
    /// # Panics
    ///
    /// If no [`Peer::read_through`](super::Peer::read_through) bound this
    /// reader to its connection.
    pub fn close(&self, cause: &'static str) {
        let (close_cause, token) = self.connection();
        close_cause.record(cause);
        token.cancel();
    }

    /// The admission step for the frames this reader admits, with fresh
    /// cadence buckets.
    ///
    /// # Panics
    ///
    /// If the reader's stream has no message table.
    pub fn admission(&self) -> StreamAdmission {
        StreamAdmission {
            rules: self
                .stream
                .messages
                .expect("an admission step reads a stream with a message table"),
            layout: self.stream.kind,
            buckets: CadenceBuckets::default(),
        }
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

/// The admission step a QUIC stream's worker runs on each frame its reader
/// admitted, for one tabled stream of one connection.
///
/// It finds the frame's row and charges the row's cadence bucket, as
/// `admit_inbound_message` does for a tabled stream. It skips that step's
/// size check, because an in-memory link negotiates no connection limits.
///
/// The worker's cadence buckets sit behind a simulator mutex, whose lock is
/// a scheduling point. A `StreamAdmission` owns its buckets, so it adds no
/// scheduling point to a simulated run. They key the stream's rows by the
/// stream's kind as its layout.
#[derive(Debug)]
pub struct StreamAdmission {
    rules: &'static [MessageRule],
    layout: u16,
    buckets: CadenceBuckets,
}

impl StreamAdmission {
    /// Admits `frame`, which the stream's [`StreamReader`] admitted. Returns
    /// the close cause the worker records if it refuses the frame.
    ///
    /// # Panics
    ///
    /// If the stream's table has no row for the frame's type, as the worker
    /// does.
    pub fn admit(&mut self, frame: &Frame) -> Result<(), &'static str> {
        let buckets = &mut self.buckets;
        let layout = self.layout;
        let admission = admit_tabled_message(
            frame,
            self.rules,
            |rule| buckets.charge(layout, rule),
            || {},
        );
        admission.ordered_close_cause().map_or(Ok(()), Err)
    }

    /// Credits a pause in which the relay waited for the stream's receiver,
    /// as the worker credits its reader's pauses.
    pub fn credit_pause(&mut self, paused: Duration) {
        if !paused.is_zero() {
            self.buckets.credit_pause(self.layout, self.rules, paused);
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zakura::{
        handler::InboundMessageAdmission,
        regulation::test_family::{message_type, EVERY_30_SECONDS, RULES},
    };

    /// A burst past a row's cadence empties its bucket, and the admission
    /// step still admits every frame, as the worker does.
    #[test]
    fn the_admission_step_admits_a_burst_past_the_cadence() {
        let stream = Stream {
            kind: 900,
            messages: Some(RULES),
            ..Stream::PERSISTENT
        };
        let (_send, _recv, reader) = read_framed_channel(1, stream);
        let mut admission = reader.admission();
        let status = Frame {
            message_type: message_type::STATUS,
            flags: 0,
            payload: vec![0; 4],
        };
        for _ in 0..=EVERY_30_SECONDS.capacity {
            assert_eq!(admission.admit(&status), Ok(()));
        }
        assert_eq!(
            admission.buckets.tokens(stream.kind, message_type::STATUS),
            Some(0)
        );
    }

    /// Each refusal maps to the cause the worker records for it.
    #[test]
    fn refusals_carry_the_workers_close_causes() {
        assert_eq!(InboundMessageAdmission::Admit.ordered_close_cause(), None);
        assert_eq!(
            InboundMessageAdmission::Oversize.ordered_close_cause(),
            Some("ordered_oversize")
        );
        assert_eq!(
            InboundMessageAdmission::Throttled.ordered_close_cause(),
            Some("ordered_rate_limited")
        );
    }
}
