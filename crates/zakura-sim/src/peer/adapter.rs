//! The sender adapter (M5-E2): labels each frame the node writes on a
//! header stream under the sender's own rules, causally, as
//! [`Observation::NodeFrame`].
//!
//! A frame is matched to the `frame-write-started` that began it, by
//! connection, QUIC stream and `seq`, and judged against a sender model
//! that has applied exactly the peer updates the node had applied when
//! that write started: the `Open`s and `Grant`s of the peer's
//! subscriptions (`open-applied`, `grant-applied`), and the endings of the
//! node's own (`end-claimed`). Updates the node applied later wait. So a
//! grant the peer sent, or the node applied after the write began, never
//! authorizes the frame, whenever the frame arrives. The node's own frames
//! change the model in write order: its pages spend their subscription's
//! credit, its endings end it, and its `Open`s hold a slot.
//!
//! The rules are the sender's, with none of the receiver's margins
//! (properties.md, "Generated properties", per row, sender side):
//!
//! - An `Open` while the node already holds `max_live` subscriptions on the
//!   stream is `OverLive`.
//! - A page or ending for a subscription the peer never opened, or that the
//!   node already ended, is `Unsolicited`.
//! - A page past its subscription's unspent object or byte credit is
//!   `OverCredit`, which the subscription row enforces.
//! - A conformant frame of a row with a cadence is charged to its
//!   connection's bucket when the peer reads it, and an empty bucket marks
//!   it `exhausted`. Every header stream of a connection shares the bucket,
//!   as production's `Status` cadence does, so a reopened stream can't reset
//!   it.
//!
//! A frame labelled with a fault changes no model state, as P3a's models
//! do for the peers' own frames (P3a, Scope) and the block-sync relay does
//! for the node's: an over-credit page spends nothing, so a later page is
//! judged on the credit the conformant pages left.
//!
//! A `Close` the node applied doesn't stop its pages: a page committed to
//! the stream before the `Close` arrived is still within credit, so the
//! subscription stays live until the node's ending. The node's `Grant`s and
//! `Close`s are conformant, since no sender template reads them.

use std::{
    collections::{BTreeMap, VecDeque},
    time::Duration,
};

use zakura_network::zakura::{
    v9::{Message, Operation, RegtestMessage},
    wire_codec::decode_frame,
    Frame, MessageRole, MessageRule,
};

use crate::{
    model::{CadenceModel, Charge, FrameHeader, FrameModel, Label, ReaderEnd, Totals},
    observe::{StreamId, TransitionFact, TransportFact},
    table::rule_row,
    Observation,
};

/// A header stream, by connection and QUIC stream.
type StreamKey = (u64, u64);

/// Labels the node's frames on every header stream, from the node's
/// transport events and the frames the peers read.
#[derive(Debug, Default)]
pub struct SenderAdapter {
    streams: BTreeMap<StreamKey, Ledger>,
    /// The cadence buckets, by connection, shared by its streams.
    cadence: CadenceModel,
}

impl SenderAdapter {
    /// No streams yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Takes the node's transport `event` on `connection`, in the order the
    /// tap logged it. Only `frame-write-started` and the peer updates the
    /// node applied matter. The others are ignored.
    pub fn node(&mut self, connection: u64, event: &TransportFact) {
        let (quic_stream, pending) = match event {
            TransportFact::FrameWriteStarted {
                quic_stream,
                seq,
                message_type,
            } => (
                *quic_stream,
                Pending::Started {
                    seq: *seq,
                    message_type: *message_type,
                },
            ),
            TransportFact::Regulated {
                quic_stream,
                transition,
            } => match Applied::of(transition) {
                Some(applied) => (*quic_stream, Pending::Applied(applied)),
                None => return,
            },
            _ => return,
        };
        self.streams
            .entry((connection, quic_stream))
            .or_default()
            .pending
            .push_back(pending);
    }

    /// A peer read `frame`, the node's `seq`-th on `connection`'s
    /// `quic_stream`, at `at` since the run's start. Returns its
    /// `NodeFrame`.
    ///
    /// # Errors
    ///
    /// A harness failure if the node never started writing `seq` with that
    /// frame's type, or an earlier frame on the stream was never read,
    /// since then the frame has no causal history to judge it by.
    pub fn received(
        &mut self,
        connection: u64,
        quic_stream: u64,
        seq: u64,
        frame: &Frame,
        at: Duration,
    ) -> Result<Observation, String> {
        let ledger = self.streams.entry((connection, quic_stream)).or_default();
        let unstarted = || {
            format!(
                "UnstartedFrame: a peer read the node's frame {seq} on connection {connection}'s \
                 stream {quic_stream} with no frame-write-started for it"
            )
        };
        loop {
            match ledger.pending.pop_front().ok_or_else(unstarted)? {
                Pending::Applied(applied) => ledger.apply(applied),
                Pending::Started {
                    seq: started,
                    message_type,
                } if started == seq => {
                    if message_type != frame.message_type {
                        return Err(format!(
                            "MismatchedFrame: the node started frame {seq} on connection \
                             {connection}'s stream {quic_stream} as type {message_type}, and a \
                             peer read type {}",
                            frame.message_type
                        ));
                    }
                    break;
                }
                Pending::Started { seq: started, .. } => {
                    return Err(format!(
                        "UnreadFrame: a peer read the node's frame {seq} on connection \
                         {connection}'s stream {quic_stream} before its frame {started}"
                    ))
                }
            }
        }
        Ok(ledger.label(&mut self.cadence, connection, quic_stream, seq, frame, at))
    }
}

/// What the tap logged on one stream that a later frame waits behind.
#[derive(Clone, Copy, Debug)]
enum Pending {
    /// A peer update the node applied.
    Applied(Applied),
    /// The node started writing frame `seq` of `message_type`.
    Started { seq: u64, message_type: u16 },
}

/// A peer update the node applied that changes what it may send.
#[derive(Clone, Copy, Debug)]
enum Applied {
    /// The peer's `Open` of `subscription`, with its initial credit.
    Open { subscription: u64, credit: Totals },
    /// The peer's `Grant` on `subscription`.
    Grant { subscription: u64, credit: Totals },
    /// The peer's ending of `subscription`, one the node opened.
    Ended { subscription: u64 },
}

impl Applied {
    /// The update `transition` applied, if it's one the sender rules read.
    fn of(transition: &TransitionFact) -> Option<Applied> {
        let totals = |objects: u32, bytes: u32| Totals {
            objects: objects.into(),
            bytes: bytes.into(),
        };
        Some(match *transition {
            TransitionFact::OpenApplied {
                subscription,
                objects,
                bytes,
                ..
            } => Applied::Open {
                subscription,
                credit: totals(objects, bytes),
            },
            TransitionFact::GrantApplied {
                subscription,
                objects,
                bytes,
            } => Applied::Grant {
                subscription,
                credit: totals(objects, bytes),
            },
            TransitionFact::EndClaimed { subscription } => Applied::Ended { subscription },
            _ => return None,
        })
    }
}

/// A subscription the peer opened on the node: the credit the node had
/// applied, and what its pages spent.
#[derive(Clone, Copy, Debug, Default)]
struct Publication {
    granted: Totals,
    consumed: Totals,
}

impl Publication {
    /// Spends a page of `objects` and `bytes` if it has one object or more
    /// and the unspent credit covers it in both units. False otherwise.
    fn spend(&mut self, objects: u64, bytes: u64) -> bool {
        let spent = Totals {
            objects: self.consumed.objects.saturating_add(objects),
            bytes: self.consumed.bytes.saturating_add(bytes),
        };
        let covered = spent.objects <= self.granted.objects && spent.bytes <= self.granted.bytes;
        if objects == 0 || !covered {
            return false;
        }
        self.consumed = spent;
        true
    }
}

/// One header stream's sender model, and what waits to be applied.
#[derive(Debug)]
struct Ledger {
    /// The tap's events since the last frame labelled, oldest first.
    pending: VecDeque<Pending>,
    /// The peer's live subscriptions on the node, by ID.
    publications: BTreeMap<u64, Publication>,
    /// The node's live subscriptions on the peer, by ID.
    subscriptions: Vec<u64>,
    frames: FrameModel,
}

impl Default for Ledger {
    fn default() -> Self {
        Ledger {
            pending: VecDeque::new(),
            publications: BTreeMap::new(),
            subscriptions: Vec::new(),
            frames: FrameModel::new(&RegtestMessage::ROWS, ReaderEnd::Persistent),
        }
    }
}

impl Ledger {
    /// Applies a peer update the node applied.
    fn apply(&mut self, applied: Applied) {
        match applied {
            Applied::Open {
                subscription,
                credit,
            } => {
                self.publications.insert(
                    subscription,
                    Publication {
                        granted: credit,
                        consumed: Totals::default(),
                    },
                );
            }
            Applied::Grant {
                subscription,
                credit,
            } => {
                // The node applies a grant only to a live publication, which
                // the ledger holds unless the node's ending came first.
                if let Some(publication) = self.publications.get_mut(&subscription) {
                    publication.granted.objects =
                        publication.granted.objects.saturating_add(credit.objects);
                    publication.granted.bytes =
                        publication.granted.bytes.saturating_add(credit.bytes);
                }
            }
            Applied::Ended { subscription } => {
                self.subscriptions.retain(|live| *live != subscription);
            }
        }
    }

    /// The `NodeFrame` of `frame`, the node's `seq`-th on `connection`'s
    /// `quic_stream`, read at `at`. A conformant frame changes the model as
    /// its sending does, and charges its row's bucket in `cadence`.
    fn label(
        &mut self,
        cadence: &mut CadenceModel,
        connection: u64,
        quic_stream: u64,
        seq: u64,
        frame: &Frame,
        at: Duration,
    ) -> Observation {
        let header = FrameHeader {
            message_type: frame.message_type,
            flags: frame.flags,
            payload_len: frame.payload.len(),
        };
        let envelope = self.frames.labels(&header);
        let row = self.frames.row(header.message_type);
        let label = match row {
            Some(_) if envelope.is_conformant() => match decode_frame::<RegtestMessage>(frame) {
                Ok(message) => self.role(&message, header.payload_len),
                Err(_) => Label::BadContent,
            },
            _ => envelope.first(),
        };
        let stream = &RegtestMessage::LAYOUT[0];
        let exhausted = row.is_some_and(|row| {
            label == Label::Conformant
                && cadence.charge(connection, stream.version, row, at) == Charge::Exhausted
        });
        let (rule_row, rule_stream) = match row {
            Some(row) => {
                let (rule_stream, rule) = rule_row(&RegtestMessage::LAYOUT, stream, row, label);
                (Some(rule.name.to_owned()), id(rule_stream))
            }
            None => (None, id(stream)),
        };
        Observation::NodeFrame {
            connection,
            stream: id(stream),
            quic_stream: Some(quic_stream),
            seq,
            row: rule_row,
            rule_stream,
            label,
            exhausted,
        }
    }

    /// The sender label of the node sending `message`, `payload_len` bytes
    /// long. A conformant one is applied.
    fn role(&mut self, message: &RegtestMessage, payload_len: usize) -> Label {
        match message {
            Message::Status(_) => Label::Conformant,
            Message::SubscribeHeaders(update) => match update.operation {
                Operation::Open => {
                    let id = update.subscription_id;
                    if self.subscriptions.len() >= max_live() {
                        Label::OverLive
                    } else if self.subscriptions.contains(&id) {
                        Label::BadCursor
                    } else {
                        self.subscriptions.push(id);
                        Label::Conformant
                    }
                }
                Operation::Grant | Operation::Close => Label::Conformant,
            },
            Message::Headers(page) => {
                let Some(publication) = self.publications.get_mut(&page.subscription_id) else {
                    return Label::Unsolicited;
                };
                // A page holds at most 4,000 entries and 2 MiB.
                let objects = page.entries.len() as u64;
                let bytes = payload_len as u64;
                if publication.spend(objects, bytes) {
                    Label::Conformant
                } else {
                    Label::OverCredit
                }
            }
            Message::HeadersOutcome(outcome) => {
                match self.publications.remove(&outcome.subscription_id) {
                    Some(_) => Label::Conformant,
                    None => Label::Unsolicited,
                }
            }
        }
    }
}

/// The ID of `stream`.
fn id(stream: &zakura_network::zakura::Stream) -> StreamId {
    StreamId {
        kind: stream.kind,
        version: stream.version,
    }
}

/// Header sync's `max_live`: the subscriptions one session may hold.
fn max_live() -> usize {
    let row: &MessageRule = RegtestMessage::ROWS
        .iter()
        .find(|row| matches!(row.role, MessageRole::Subscription { .. }))
        .expect("header sync has a subscription row");
    let MessageRole::Subscription { max_live, .. } = row.role else {
        unreachable!("found by its role");
    };
    usize::try_from(max_live).unwrap_or(usize::MAX)
}
