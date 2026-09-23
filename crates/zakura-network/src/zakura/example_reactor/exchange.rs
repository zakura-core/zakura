//! The example reactor serving and downloading through the exchange tools.
//!
//! One function, [`range_cap`], bounds a range's response for both sides: the
//! server's [`Serve`] reserves output for it, and the client's
//! [`Reservations`] admit no more than it. The handler is a plain `match` from
//! message to tool. Nothing in it depends on the layout, so the same node runs
//! over [`SINGLE`](super::SINGLE) and [`PAIRED`](super::PAIRED).

use std::{collections::BTreeMap, sync::Arc};

use futures::{stream::SelectAll, StreamExt};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::{message_type, ExampleMessage, ItemPayload, ItemRange, RangePayload, GET_ITEMS};
use crate::zakura::{
    framed_channel,
    regulation::{
        CadenceSendError, CadenceSender, ClaimRefused, PoolEntry, Produce, Reservations,
        ReserveRefused, Responded, ResponseCap, ResponseSink, Serve, ServeCapacity, ServeEnd,
        ServeViolation, SinkProgress, WorkLease,
    },
    wire_codec::{decode_frame, encode_frame, Wire, WireError, WireMessage},
    Frame, FramedRecv, FramedSend, MessageRole, MessageRule, Stream, ZakuraPeerId,
};
use zakura_chain::block::Height;

#[cfg(test)]
mod tests;

/// The most a range's response may carry: one item per height, then the
/// ending. Both the server and the client bound the response with it.
pub(crate) fn range_cap(range: ItemRange) -> ResponseCap {
    // Widening usize to u64 is lossless on supported targets.
    let item = ItemPayload::MAX_LEN as u64;
    let ending = RangePayload::MAX_LEN as u64;
    ResponseCap {
        frames: range.count,
        bytes: u64::from(range.count) * item + ending,
    }
}

/// The items one node serves: every height in `low..=high`.
#[derive(Debug)]
pub(crate) struct Store {
    pub(crate) low: Height,
    pub(crate) high: Height,
}

/// An item's bytes: its height, repeated to a length that varies with it.
pub(crate) fn item_bytes(height: Height) -> Vec<u8> {
    let len = 1 + usize::try_from(height.0 % 8).expect("a remainder of 8 fits usize");
    height.0.to_le_bytes().repeat(len)
}

impl Produce for Store {
    type Request = ItemRange;
    type Message = ExampleMessage;

    fn response_cap(&self, range: &ItemRange) -> ResponseCap {
        range_cap(*range)
    }

    async fn produce(
        &self,
        range: &ItemRange,
        lease: WorkLease,
        mut sink: ResponseSink<ExampleMessage>,
    ) -> Result<Responded, ServeEnd> {
        let last = Height(range.start.0 + (range.count - 1));
        if range.start < self.low || last > self.high {
            return sink
                .finish(&ExampleMessage::RangeUnavailable(*range))
                .map_err(|error| ServeEnd::LocalFault(error.to_string()));
        }
        for offset in 0..range.count {
            if lease.is_cancelled() {
                return Err(ServeEnd::Cancelled);
            }
            let height = Height(range.start.0 + offset);
            sink.send(&ExampleMessage::Item {
                height,
                bytes: item_bytes(height),
            })
            .map_err(|error| ServeEnd::LocalFault(error.to_string()))?;
        }
        sink.finish(&ExampleMessage::ItemsDone {
            start: range.start,
            returned: range.count,
        })
        .map_err(|error| ServeEnd::LocalFault(error.to_string()))
    }

    fn local_failure(&self, range: &ItemRange, sent: SinkProgress) -> ExampleMessage {
        if sent.frames == 0 {
            ExampleMessage::RangeUnavailable(*range)
        } else {
            ExampleMessage::ItemsDone {
                start: range.start,
                returned: sent.frames,
            }
        }
    }
}

/// One end of an in-process connection: a channel pair per layout stream.
#[derive(Debug)]
pub(crate) struct Link {
    pub(crate) sends: Vec<FramedSend>,
    pub(crate) recvs: Vec<FramedRecv>,
}

/// Connect two ends over `layout`, one channel per stream and direction.
///
/// In-process channels stand in for the transport, so they buffer a fixed 64
/// frames. The stream conformance suite runs the layouts over QUIC with
/// their declared queue depths.
pub(crate) fn connect(layout: &[Stream]) -> (Link, Link) {
    let mut a = Link {
        sends: Vec::new(),
        recvs: Vec::new(),
    };
    let mut b = Link {
        sends: Vec::new(),
        recvs: Vec::new(),
    };
    for _ in layout {
        let (to_b, from_a) = framed_channel(64);
        let (to_a, from_b) = framed_channel(64);
        a.sends.push(to_b);
        a.recvs.push(from_b);
        b.sends.push(to_a);
        b.recvs.push(from_a);
    }
    (a, b)
}

/// Merge a link's receivers into one stream of frames.
pub(crate) fn inbound(
    recvs: Vec<FramedRecv>,
) -> SelectAll<futures::stream::BoxStream<'static, Frame>> {
    futures::stream::select_all(recvs.into_iter().map(|recv| {
        futures::stream::unfold(recv, |mut recv| async move {
            recv.recv().await.map(|frame| (frame, recv))
        })
        .boxed()
    }))
}

/// Why the example node disconnects its peer. Every case is a protocol
/// violation that no conformant peer can cause.
#[derive(Debug, Error, PartialEq)]
pub(crate) enum Violation {
    #[error("undecodable message: {0}")]
    Decode(#[from] WireError),
    #[error(transparent)]
    Serve(#[from] ServeViolation),
    #[error(transparent)]
    Claim(#[from] ClaimRefused),
    #[error("an ending reports {reported} items after {received}")]
    Count { reported: u32, received: u32 },
}

/// A range this node requested, and the items it received so far.
#[derive(Debug)]
struct Download {
    range: ItemRange,
    items: Vec<(Height, Vec<u8>)>,
}

/// One example node on one connection.
#[derive(Debug)]
pub(crate) struct ExampleNode {
    layout: &'static [Stream],
    sends: Vec<FramedSend>,
    /// Ranges this node may have outstanding: the request row's limit.
    max_in_flight: usize,
    serve: Serve<Store>,
    reservations: Reservations<Height>,
    downloads: BTreeMap<Height, Download>,
    status: CadenceSender<ExampleMessage>,
    /// Items of every finished download, in arrival order.
    pub(crate) received: Vec<(Height, Vec<u8>)>,
    /// Ranges the peer reported unavailable.
    pub(crate) unavailable: Vec<ItemRange>,
    /// The peer's latest servable range.
    pub(crate) peer_status: Option<(Height, Height)>,
}

impl ExampleNode {
    /// Serve `store` to `peer` and download from it over `layout`.
    pub(crate) fn new(
        layout: &'static [Stream],
        capacity: &ServeCapacity,
        store: Arc<Store>,
        peer: &ZakuraPeerId,
        sends: Vec<FramedSend>,
        cancel: CancellationToken,
    ) -> Self {
        let MessageRole::Request { max_in_flight, .. } = GET_ITEMS.role else {
            unreachable!("GET_ITEMS is a request row");
        };
        let response_stream = stream_for(layout, message_type::ITEM);
        let serve = capacity.session(
            store,
            peer,
            max_in_flight,
            sends[response_stream].clone(),
            cancel,
        );
        let max_in_flight = usize::try_from(max_in_flight).expect("a small limit fits usize");
        Self {
            layout,
            sends,
            max_in_flight,
            serve,
            reservations: Reservations::new(ExampleMessage::RULES, max_in_flight),
            downloads: BTreeMap::new(),
            status: CadenceSender::new(),
            received: Vec::new(),
            unavailable: Vec::new(),
            peer_status: None,
        }
    }

    /// Whether another request fits this node's reservations.
    pub(crate) fn has_room(&self) -> bool {
        self.reservations.len() < self.max_in_flight
    }

    /// Whether every requested range has ended.
    pub(crate) fn idle(&self) -> bool {
        self.downloads.is_empty()
    }

    /// Reserve `range`'s response, then request it.
    pub(crate) fn request(
        &mut self,
        range: ItemRange,
        entry: PoolEntry,
    ) -> Result<(), ReserveRefused> {
        self.reservations
            .reserve(range.start, GET_ITEMS.message_type, range_cap(range), entry)?;
        self.downloads.insert(
            range.start,
            Download {
                range,
                items: Vec::new(),
            },
        );
        let frame = encode_frame(&ExampleMessage::GetItems(range))
            .expect("a range this node requests encodes");
        let stream = stream_for(self.layout, message_type::GET_ITEMS);
        self.sends[stream]
            .try_send(frame)
            .expect("the request queue holds one request per reservation");
        Ok(())
    }

    /// Announce this node's servable range; the sender paces it.
    pub(crate) fn announce(
        &mut self,
        low: Height,
        high: Height,
    ) -> Result<usize, CadenceSendError> {
        self.status.update(ExampleMessage::Status { low, high })?;
        let stream = stream_for(self.layout, message_type::STATUS);
        self.status.send_due(&self.sends[stream])
    }

    /// Handle one inbound frame. An error disconnects the peer.
    ///
    /// On a transport stream, the reader runs the response precheck before it
    /// allocates the payload. In process, the node runs it here, still before
    /// the decode.
    pub(crate) fn handle(&mut self, frame: Frame) -> Result<(), Violation> {
        if matches!(
            MessageRule::find(ExampleMessage::RULES, frame.message_type).map(|row| row.role),
            Some(MessageRole::Response { .. })
        ) {
            self.reservations
                .precheck(frame.message_type, frame.payload.len())?;
        }
        let len = frame.payload.len();
        match decode_frame::<ExampleMessage>(&frame)? {
            ExampleMessage::Status { low, high } => self.peer_status = Some((low, high)),
            ExampleMessage::GetItems(range) => self.serve.admit(range)?,
            ExampleMessage::Item { height, bytes } => {
                let start = self.range_of(height, frame.message_type)?;
                self.reservations
                    .claim_frame(&start, frame.message_type, len)?;
                self.downloads
                    .get_mut(&start)
                    .expect("a live reservation has a download")
                    .items
                    .push((height, bytes));
            }
            ExampleMessage::ItemsDone { start, returned } => {
                let ended = self
                    .reservations
                    .claim_end(&start, frame.message_type, len)?;
                if ended.frames != returned {
                    return Err(Violation::Count {
                        reported: returned,
                        received: ended.frames,
                    });
                }
                let download = self
                    .downloads
                    .remove(&start)
                    .expect("a live reservation has a download");
                self.received.extend(download.items);
            }
            ExampleMessage::RangeUnavailable(range) => {
                let ended = self
                    .reservations
                    .claim_end(&range.start, frame.message_type, len)?;
                if ended.frames != 0 {
                    return Err(Violation::Count {
                        reported: 0,
                        received: ended.frames,
                    });
                }
                let download = self
                    .downloads
                    .remove(&range.start)
                    .expect("a live reservation has a download");
                self.unavailable.push(download.range);
            }
        }
        Ok(())
    }

    /// The start of the live download containing `height`.
    fn range_of(&self, height: Height, message_type: u16) -> Result<Height, ClaimRefused> {
        self.downloads
            .range(..=height)
            .next_back()
            .filter(|(_, download)| height.0 - download.range.start.0 < download.range.count)
            .map(|(start, _)| *start)
            .ok_or(ClaimRefused::Unsolicited { message_type })
    }

    /// Open requests on this node's serving session.
    pub(crate) fn serving_open(&self) -> u32 {
        self.serve.open()
    }
}

/// The index of the layout stream that carries `message_type`.
fn stream_for(layout: &[Stream], message_type: u16) -> usize {
    layout
        .iter()
        .position(|stream| {
            stream
                .messages
                .is_some_and(|rules| MessageRule::find(rules, message_type).is_some())
        })
        .expect("every example message has a row in each layout")
}
