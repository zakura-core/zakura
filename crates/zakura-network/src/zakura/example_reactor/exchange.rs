//! The example reactor serving and downloading through the exchange tools.
//!
//! One function, [`range_cap`], bounds a range's response for both sides: the
//! server's [`Serve`] reserves output for it, and the client's
//! [`Reservations`] admit no more than it. The handler is a plain `match` from
//! message to tool. Nothing in it depends on the layout, so the same node runs
//! over [`SINGLE`](super::SINGLE) and [`PAIRED`](super::PAIRED).
//!
//! A watch pushes each item above its start as the next page. The node checks
//! what the tools cannot: each page's height follows the previous one, and
//! each end reason arrives in its window. One publisher task per session
//! writes every page and outcome, so a watch's outcome follows its pages.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex, PoisonError},
};

use futures::{stream::SelectAll, StreamExt};
use thiserror::Error;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::{
    message_type, EndReason, ExampleMessage, ItemPayload, ItemRange, RangePayload, WatchOp,
    WatchUpdate, GET_ITEMS, WATCH,
};
use crate::zakura::{
    framed_channel,
    regulation::{
        Applied, CadenceSendError, CadenceSender, ClaimRefused, PoolEntry, Produce, Publications,
        Push, Reservations, ReserveRefused, Responded, ResponseCap, ResponseSink, Serve,
        ServeCapacity, ServeEnd, ServeViolation, SinkProgress, SubscribeRefused, SubscriptionFault,
        SubscriptionLimits, Subscriptions, Update, WorkLease,
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
    #[error(transparent)]
    Subscription(#[from] SubscriptionFault),
    #[error("a pushed item at {got:?} does not follow {received:?}")]
    Linkage { received: Height, got: Height },
    #[error("a watch ended as {0:?} outside that reason's window")]
    OutcomeWindow(EndReason),
}

/// Why this node could not request a range. Each case is local.
#[derive(Debug, Error, PartialEq)]
pub(crate) enum RequestFailed {
    #[error(transparent)]
    Reserve(#[from] ReserveRefused),
    #[error("the request did not encode: {0}")]
    Encode(#[from] WireError),
    #[error("the request queue is full")]
    QueueFull,
}

/// Why this node could not change its watch. Each case is local.
#[derive(Debug, Error, PartialEq)]
pub(crate) enum WatchFailed {
    #[error(transparent)]
    Refused(#[from] SubscribeRefused),
    #[error("the update did not encode: {0}")]
    Encode(#[from] WireError),
}

/// The watches peers hold on this node, shared with the publisher task.
#[derive(Debug)]
struct Watches {
    publications: Publications<u32, Height>,
    /// Watches this node ends as superseded.
    superseded: Vec<u32>,
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
    /// This node's watches on the peer.
    subscriptions: Subscriptions<u32, Height>,
    next_watch: u32,
    /// Updates recorded but not yet queued, oldest first. They leave in order
    /// once the stream has room.
    unsent: VecDeque<Frame>,
    /// The peer's watches on this node.
    watches: Arc<Mutex<Watches>>,
    publisher: Arc<Notify>,
    /// Watched items, in arrival order.
    pub(crate) watched: Vec<(Height, Vec<u8>)>,
    /// How each of this node's watches ended.
    pub(crate) watch_ended: Vec<EndReason>,
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
            store.clone(),
            peer,
            max_in_flight,
            sends[response_stream].clone(),
            cancel.clone(),
        );
        let limits = SubscriptionLimits::from_rule(&WATCH).expect("WATCH is a subscription row");
        let watches = Arc::new(Mutex::new(Watches {
            publications: Publications::new(limits),
            superseded: Vec::new(),
        }));
        let publisher = Arc::new(Notify::new());
        tokio::spawn(publish(
            watches.clone(),
            publisher.clone(),
            capacity.push(peer),
            store,
            sends[stream_for(layout, message_type::PUSHED)].clone(),
            cancel,
        ));
        let max_in_flight = usize::try_from(max_in_flight).expect("a small limit fits usize");
        Self {
            layout,
            sends,
            max_in_flight,
            serve,
            reservations: Reservations::new(ExampleMessage::RULES, max_in_flight),
            downloads: BTreeMap::new(),
            status: CadenceSender::new(),
            subscriptions: Subscriptions::new(limits, ExampleMessage::RULES),
            next_watch: 0,
            unsent: VecDeque::new(),
            watches,
            publisher,
            watched: Vec::new(),
            watch_ended: Vec::new(),
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
    ///
    /// A request that is never written gives its reservation back.
    pub(crate) fn request(
        &mut self,
        range: ItemRange,
        entry: PoolEntry,
    ) -> Result<(), RequestFailed> {
        let frame = encode_frame(&ExampleMessage::GetItems(range))?;
        self.reservations
            .reserve(range.start, GET_ITEMS.message_type, range_cap(range), entry)?;
        let stream = stream_for(self.layout, message_type::GET_ITEMS);
        if self.sends[stream].try_send(frame).is_err() {
            self.reservations.retract(&range.start);
            return Err(RequestFailed::QueueFull);
        }
        self.downloads.insert(
            range.start,
            Download {
                range,
                items: Vec::new(),
            },
        );
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
        let len = frame.payload.len();
        match MessageRule::find(ExampleMessage::RULES, frame.message_type).map(|row| row.role) {
            Some(MessageRole::Response { request, .. }) if request == WATCH.message_type => {
                self.subscriptions.precheck(frame.message_type, len)?;
            }
            Some(MessageRole::Response { .. }) => {
                self.reservations.precheck(frame.message_type, len)?;
            }
            _ => {}
        }
        let handled = self.dispatch(frame, len);
        self.flush();
        handled
    }

    fn dispatch(&mut self, frame: Frame, len: usize) -> Result<(), Violation> {
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
            ExampleMessage::Watch(update) => {
                let mut watches = self.watches.lock().unwrap_or_else(PoisonError::into_inner);
                let publications = &mut watches.publications;
                let applied = match update.op {
                    WatchOp::Open => publications
                        .open(
                            update.id,
                            update.sequence,
                            update.added,
                            update.acknowledged,
                        )
                        .map(|()| Applied::Live),
                    WatchOp::Grant => publications.grant(
                        &update.id,
                        update.sequence,
                        &update.acknowledged,
                        update.added,
                    ),
                    WatchOp::Close => {
                        publications.close(&update.id, update.sequence, &update.acknowledged)
                    }
                }?;
                // A crossed update has nothing left to change.
                if applied == Applied::Live {
                    self.publisher.notify_one();
                }
            }
            ExampleMessage::Pushed { id, height, bytes } => {
                let &received = self
                    .subscriptions
                    .received(&id)
                    .ok_or(SubscriptionFault::Unknown)?;
                if received.next().ok() != Some(height) {
                    return Err(Violation::Linkage {
                        received,
                        got: height,
                    });
                }
                self.subscriptions.claim_page(&id, 1, len, height)?;
                // The handler accepts each item as it arrives.
                self.watched.push((height, bytes));
                let accepted = self.subscriptions.accept(&id, &height);
                debug_assert!(
                    accepted.is_ok(),
                    "the page was just claimed and not yet accepted"
                );
                self.renew(id);
            }
            ExampleMessage::WatchEnded { id, reason } => {
                let ended = self.subscriptions.claim_end(&id)?;
                let in_window = match reason {
                    EndReason::Unavailable => ended.pages == 0,
                    EndReason::Superseded => true,
                    EndReason::Closed => ended.close_sent,
                };
                if !in_window {
                    return Err(Violation::OutcomeWindow(reason));
                }
                self.watch_ended.push(reason);
            }
        }
        Ok(())
    }

    /// Watch the peer's items above `from`, with the whole credit window.
    pub(crate) fn watch(&mut self, from: Height) -> Result<u32, WatchFailed> {
        let id = self.next_watch;
        let MessageRole::Subscription { credit, .. } = WATCH.role else {
            unreachable!("WATCH is a subscription row");
        };
        let update = self.subscriptions.open(id, credit, from)?;
        let frame = match encode_frame(&watch_update(WatchOp::Open, id, update)) {
            Ok(frame) => frame,
            Err(error) => {
                self.subscriptions.retract(&id);
                return Err(error.into());
            }
        };
        self.next_watch += 1;
        self.unsent.push_back(frame);
        self.flush();
        Ok(id)
    }

    /// Ask the peer to end watch `id`.
    pub(crate) fn close_watch(&mut self, id: u32) -> Result<(), WatchFailed> {
        let update = self.subscriptions.close(&id)?;
        self.queue_update(WatchOp::Close, id, update)
    }

    /// End every watch the peer holds on this node, as superseded.
    pub(crate) fn supersede_watches(&self) {
        let mut watches = self.watches.lock().unwrap_or_else(PoisonError::into_inner);
        let live: Vec<u32> = watches.publications.keys().copied().collect();
        watches.superseded.extend(live);
        self.publisher.notify_one();
    }

    /// Grant the window back once half of it is free.
    fn renew(&mut self, id: u32) {
        let Some(room) = self.subscriptions.grantable(&id) else {
            return;
        };
        if room.objects < super::WATCH_ITEMS / 2 {
            return;
        }
        if let Ok(update) = self.subscriptions.grant(&id, room) {
            self.queue_update(WatchOp::Grant, id, update)
                .expect("a recorded update encodes");
        }
    }

    /// Queue an update the tool already recorded. It must reach the peer, in
    /// order, or the peer sees a sequence gap.
    fn queue_update(
        &mut self,
        op: WatchOp,
        id: u32,
        update: Update<Height>,
    ) -> Result<(), WatchFailed> {
        self.unsent
            .push_back(encode_frame(&watch_update(op, id, update))?);
        self.flush();
        Ok(())
    }

    /// Move queued updates to the stream while it has room.
    pub(crate) fn flush(&mut self) {
        let stream = stream_for(self.layout, message_type::WATCH);
        while let Some(frame) = self.unsent.pop_front() {
            if let Err(error) = self.sends[stream].try_send(frame) {
                let frame = match error {
                    tokio::sync::mpsc::error::TrySendError::Full(frame)
                    | tokio::sync::mpsc::error::TrySendError::Closed(frame) => frame,
                };
                self.unsent.push_front(frame);
                return;
            }
        }
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

fn watch_update(op: WatchOp, id: u32, update: Update<Height>) -> ExampleMessage {
    ExampleMessage::Watch(WatchUpdate {
        op,
        id,
        sequence: update.sequence,
        acknowledged: update.acknowledged,
        added: update.added,
    })
}

/// Push watched items until the session ends.
///
/// Each round ends the watches that must end, in the order
/// [`Publications::end`] returns them, then pushes one page. A page waits for
/// the same capacity a served response takes. A watch update wakes the task,
/// so `Close` never waits behind a page that waits for capacity.
async fn publish(
    watches: Arc<Mutex<Watches>>,
    wake: Arc<Notify>,
    push: Push,
    store: Arc<Store>,
    send: FramedSend,
    cancel: CancellationToken,
) {
    loop {
        let (ended, page) = {
            let mut watches = watches.lock().unwrap_or_else(PoisonError::into_inner);
            let ended = end_due(&mut watches, &store);
            (ended, next_page(&watches.publications, &store))
        };
        for (id, reason, terminal) in ended {
            let frame = encode_frame(&ExampleMessage::WatchEnded { id, reason })
                .expect("an end reason always encodes");
            if !terminal.send(&send, frame).await {
                return;
            }
        }
        let Some((id, frame, height)) = page else {
            tokio::select! {
                () = cancel.cancelled() => return,
                () = wake.notified() => continue,
            }
        };
        let len = frame.payload.len();
        let permit = tokio::select! {
            () = cancel.cancelled() => return,
            () = wake.notified() => continue,
            permit = push.acquire(len) => permit,
        };
        let reserved = watches
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .publications
            .reserve_page(&id, 1, len, height);
        // A stall means the watch changed while this page waited; retry.
        if reserved.is_ok() && !permit.send(&send, frame).await {
            return;
        }
    }
}

/// End every watch that is closing, superseded, or unservable, and return
/// its outcome and terminal permit.
fn end_due(
    watches: &mut Watches,
    store: &Store,
) -> Vec<(u32, EndReason, crate::zakura::regulation::TerminalPermit)> {
    let publications = &watches.publications;
    let mut due: Vec<(u32, EndReason)> = publications
        .keys()
        .filter_map(|&id| {
            let first = publications.credit(&id)?.consumed().objects == 0;
            let next = publications.last_sent(&id)?.next();
            if publications.is_closing(&id) {
                Some((id, EndReason::Closed))
            } else if watches.superseded.contains(&id) {
                Some((id, EndReason::Superseded))
            } else if first && next.is_ok_and(|next| next < store.low) {
                Some((id, EndReason::Unavailable))
            } else {
                None
            }
        })
        .collect();
    due.sort_unstable_by_key(|&(id, _)| id);
    watches.superseded.clear();
    due.into_iter()
        .filter_map(|(id, reason)| {
            let terminal = watches.publications.end(&id)?;
            Some((id, reason, terminal))
        })
        .collect()
}

/// The next page of the first watch with an item to push and credit for it.
fn next_page(
    publications: &Publications<u32, Height>,
    store: &Store,
) -> Option<(u32, Frame, Height)> {
    let mut ids: Vec<u32> = publications.keys().copied().collect();
    ids.sort_unstable();
    ids.into_iter().find_map(|id| {
        let height = publications.last_sent(&id)?.next().ok()?;
        if height < store.low || height > store.high {
            return None;
        }
        let frame = encode_frame(&ExampleMessage::Pushed {
            id,
            height,
            bytes: item_bytes(height),
        })
        .expect("a stored item always encodes");
        let unspent = publications.unspent(&id)?;
        // Widening usize to u64 is lossless on supported targets.
        (unspent.objects >= 1 && unspent.bytes >= frame.payload.len() as u64)
            .then_some((id, frame, height))
    })
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
