//! The subscription model: one subscription row and the pages and terminal
//! outcomes that answer it.

use std::collections::{BTreeMap, VecDeque};

use zakura_network::zakura::{Credit, MessageRole, MessageRule};

use super::{Cursor, ExchangeKey, Label, Labels, SessionId};

/// An update the node writes as subscriber: the fields that go on the wire.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Update {
    /// Zero for `Open`, then one more for each update.
    pub sequence: u32,
    /// The start for `Open`, then the last page the handler accepted.
    pub acknowledged: Cursor,
    /// The credit the update adds. `Close` adds none.
    pub added: Credit,
}

/// Cumulative objects and payload bytes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Totals {
    /// Objects, such as headers.
    pub objects: u64,
    /// Payload bytes, frame headers excluded.
    pub bytes: u64,
}

impl Totals {
    /// `self + credit`, or `None` on overflow.
    fn plus(self, credit: Credit) -> Option<Totals> {
        Some(Totals {
            objects: self.objects.checked_add(credit.objects.into())?,
            bytes: self.bytes.checked_add(credit.bytes.into())?,
        })
    }

    /// `self - spent`, or `None` if `spent` exceeds `self` in either unit.
    fn minus(self, spent: Totals) -> Option<Totals> {
        Some(Totals {
            objects: self.objects.checked_sub(spent.objects)?,
            bytes: self.bytes.checked_sub(spent.bytes)?,
        })
    }

    /// True if `self` covers `objects` and `bytes`.
    fn covers(self, objects: u64, bytes: u64) -> bool {
        objects <= self.objects && bytes <= self.bytes
    }
}

/// A subscription the node publishes to a peer.
#[derive(Clone, Debug)]
struct Publication {
    sequence: u32,
    granted: Totals,
    consumed: Totals,
    /// The acknowledged cursor and the consumed totals through it.
    acknowledged: (Cursor, Totals),
    /// Pages sent after the acknowledged one, oldest first, with the
    /// consumed totals through each.
    sent: VecDeque<(Cursor, Totals)>,
    closing: bool,
}

/// An ended publication that later updates may still name.
#[derive(Clone, Debug)]
struct Tombstone {
    key: ExchangeKey,
    sequence: u32,
    granted: Totals,
    consumed: Totals,
    close_received: bool,
}

/// The publications on one session.
#[derive(Clone, Debug, Default)]
struct Publisher {
    live: BTreeMap<ExchangeKey, Publication>,
    /// Oldest first.
    tombstones: VecDeque<Tombstone>,
    /// Ended subscriptions whose terminal outcome the node is still writing,
    /// oldest first. Each holds its slot until the write finishes, and the
    /// peer counts it as live until then.
    writing: Vec<ExchangeKey>,
}

/// A subscription the node opened on a peer.
#[derive(Clone, Debug)]
struct Subscription {
    sequence: u32,
    /// The cursor the next update acknowledges: the start, then the last
    /// accepted page.
    acknowledged: Cursor,
    granted: Totals,
    consumed: Totals,
    /// Received pages the handler hasn't accepted, oldest first, with the
    /// consumed totals through each.
    unaccepted: VecDeque<(Cursor, Totals)>,
    /// The consumed totals through the last accepted page.
    accepted: Totals,
    close_sent: bool,
}

/// The subscriptions the node opened on one session.
#[derive(Clone, Debug, Default)]
struct Subscriber {
    live: BTreeMap<ExchangeKey, Subscription>,
    /// The latest terminal outcomes' keys, newest last.
    ended: VecDeque<ExchangeKey>,
}

/// Labels the frames of one subscription row's subscriptions: `OverLive`,
/// `AfterClose`, and `BadCursor` for the updates a peer sends to the node as
/// publisher, and `Unsolicited` and `OverCredit` for the pages and terminal
/// outcomes it sends to the node as subscriber.
///
/// The rules come from the toolkit's subscription decisions (T2 D9):
///
/// - Granted minus acknowledged credit never exceeds the row's credit, in
///   objects and in bytes.
/// - A session holds at most `max_live` subscriptions, with a margin of
///   twice `max_live`, because a terminal outcome can cross an `Open`.
/// - An ended subscription leaves a tombstone. A `Grant` or `Close` that
///   crossed the terminal outcome is conformant if it would have been
///   before the end, and a crossed `Grant`'s credit fits the consumed
///   credit plus the row's credit.
/// - The subscriber's `Open` proves it received the oldest terminal
///   outcomes, so the publisher keeps at most `max_live - 1` live
///   subscriptions and tombstones once it applies an `Open`.
/// - A subscription's slot stays held from its `Open` until the node
///   finishes writing its terminal outcome ([`SubscriptionModel::end`],
///   [`SubscriptionModel::terminal_written`]).
#[derive(Clone, Debug)]
pub struct SubscriptionModel {
    row: &'static MessageRule,
    max_live: usize,
    credit: Credit,
    history: usize,
    publishers: BTreeMap<SessionId, Publisher>,
    subscribers: BTreeMap<SessionId, Subscriber>,
}

impl SubscriptionModel {
    /// The model of `rules`' subscription row `subscription_type`, or `None`
    /// if that row isn't a subscription.
    pub fn new(rules: &'static [MessageRule], subscription_type: u16) -> Option<Self> {
        let row = rules
            .iter()
            .find(|row| row.message_type == subscription_type)?;
        let MessageRole::Subscription {
            max_live,
            credit,
            cursor_history,
            ..
        } = row.role
        else {
            return None;
        };
        Some(SubscriptionModel {
            row,
            max_live: usize::try_from(max_live).unwrap_or(usize::MAX),
            credit,
            history: usize::try_from(cursor_history).unwrap_or(usize::MAX),
            publishers: BTreeMap::new(),
            subscribers: BTreeMap::new(),
        })
    }

    /// The subscription row.
    pub fn row(&self) -> &'static MessageRule {
        self.row
    }

    // The node as publisher: the peer's updates.

    /// Labels the peer's `Open` on `session`, and applies it if conformant.
    pub fn open_frame(
        &mut self,
        session: SessionId,
        key: ExchangeKey,
        sequence: u32,
        credit: Credit,
        start: Cursor,
    ) -> Labels {
        let max_live = self.max_live;
        let publisher = self.publishers.entry(session).or_default();
        let reused = publisher.live.contains_key(&key)
            || publisher.tombstones.iter().any(|dead| dead.key == key);
        let live = publisher.live.len();
        let held = live + publisher.writing.len();
        let partial = credit.objects == 0 || credit.bytes == 0;
        let labels = grant_faults(self.credit, Totals::default(), credit, Totals::default())
            .with_if(held >= max_live.saturating_mul(2), Label::OverLive)
            .with_if(sequence != 0, Label::BadCursor)
            .with_if(reused, Label::BadCursor)
            .with_if(partial, Label::BadCursor);
        if !labels.is_conformant() {
            return labels;
        }
        let keep = max_live.saturating_sub(1).saturating_sub(live);
        while publisher.tombstones.len() > keep {
            publisher.tombstones.pop_front();
        }
        let granted = Totals::default()
            .plus(credit)
            .expect("u32 credit fits u64 totals");
        publisher.live.insert(
            key,
            Publication {
                sequence: 0,
                granted,
                consumed: Totals::default(),
                acknowledged: (start, Totals::default()),
                sent: VecDeque::new(),
                closing: false,
            },
        );
        labels
    }

    /// Labels the peer's `Grant` on `session`, and applies it if conformant.
    pub fn grant_frame(
        &mut self,
        session: SessionId,
        key: ExchangeKey,
        sequence: u32,
        acknowledged: Cursor,
        added: Credit,
    ) -> Labels {
        let credit = self.credit;
        let publisher = self.publishers.entry(session).or_default();
        if let Some(live) = publisher.live.get_mut(&key) {
            let through = acknowledged_through(live, acknowledged);
            let labels = Labels::NONE
                .with_if(live.closing, Label::AfterClose)
                .with_if(!follows(live.sequence, sequence), Label::BadCursor)
                .union(match through {
                    Some((_, from)) => grant_faults(credit, live.granted, added, from),
                    None => Labels::of(Label::BadCursor),
                });
            if labels.is_conformant() {
                live.sequence = sequence;
                live.granted = live.granted.plus(added).expect("checked above");
                acknowledge(live, through.and_then(|(index, _)| index));
            }
            return labels;
        }
        let Some(dead) = publisher.tombstones.iter_mut().find(|dead| dead.key == key) else {
            return Labels::of(Label::BadCursor);
        };
        // The subscriber acknowledged at most every page sent before the
        // end, so its window starts at or before the consumed credit.
        let labels = grant_faults(credit, dead.granted, added, dead.consumed)
            .with_if(dead.close_received, Label::AfterClose)
            .with_if(!follows(dead.sequence, sequence), Label::BadCursor);
        if labels.is_conformant() {
            dead.sequence = sequence;
            dead.granted = dead.granted.plus(added).expect("checked above");
        }
        labels
    }

    /// Labels the peer's `Close` on `session`, and applies it if conformant.
    /// A `Close` that crossed the terminal outcome consumes the tombstone.
    pub fn close_frame(
        &mut self,
        session: SessionId,
        key: ExchangeKey,
        sequence: u32,
        acknowledged: Cursor,
    ) -> Labels {
        let publisher = self.publishers.entry(session).or_default();
        if let Some(live) = publisher.live.get_mut(&key) {
            let through = acknowledged_through(live, acknowledged);
            let labels = Labels::NONE
                .with_if(live.closing, Label::AfterClose)
                .with_if(!follows(live.sequence, sequence), Label::BadCursor)
                .with_if(through.is_none(), Label::BadCursor);
            if labels.is_conformant() {
                live.sequence = sequence;
                live.closing = true;
                acknowledge(live, through.and_then(|(index, _)| index));
            }
            return labels;
        }
        let Some(index) = publisher.tombstones.iter().position(|dead| dead.key == key) else {
            return Labels::of(Label::BadCursor);
        };
        let dead = &publisher.tombstones[index];
        let labels = Labels::NONE
            .with_if(dead.close_received, Label::AfterClose)
            .with_if(!follows(dead.sequence, sequence), Label::BadCursor);
        if labels.is_conformant() {
            publisher.tombstones.remove(index);
        }
        labels
    }

    /// The node sends a page of `objects` and `bytes` at `cursor`, if the
    /// subscription is live and not closing, the page carries an object, the
    /// unspent credit covers it, and the cursor history has room. False if
    /// the node may not send it now.
    pub fn send_page(
        &mut self,
        session: SessionId,
        key: ExchangeKey,
        objects: u32,
        bytes: usize,
        cursor: Cursor,
    ) -> bool {
        let history = self.history;
        let Some(live) = self
            .publishers
            .get_mut(&session)
            .and_then(|publisher| publisher.live.get_mut(&key))
        else {
            return false;
        };
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        let unspent = live.granted.minus(live.consumed).unwrap_or_default();
        if live.closing
            || objects == 0
            || !unspent.covers(objects.into(), bytes)
            || live.sent.len() >= history
        {
            return false;
        }
        live.consumed = Totals {
            objects: live.consumed.objects + u64::from(objects),
            bytes: live.consumed.bytes + bytes,
        };
        live.sent.push_back((cursor, live.consumed));
        true
    }

    /// The node ends `key` on `session` and starts writing its terminal
    /// outcome. The subscription leaves a tombstone now, and its slot stays
    /// held until [`Self::terminal_written`]. False if it isn't live.
    pub fn end(&mut self, session: SessionId, key: ExchangeKey) -> bool {
        let bound = self.max_live.saturating_mul(2);
        let Some(publisher) = self.publishers.get_mut(&session) else {
            return false;
        };
        let Some(live) = publisher.live.remove(&key) else {
            return false;
        };
        publisher.writing.push(key);
        publisher.tombstones.push_back(Tombstone {
            key,
            sequence: live.sequence,
            granted: live.granted,
            consumed: live.consumed,
            close_received: live.closing,
        });
        while publisher.tombstones.len() > bound {
            publisher.tombstones.pop_front();
        }
        true
    }

    /// The node finished writing `key`'s terminal outcome on `session`,
    /// which frees its slot. False if no such write is pending.
    pub fn terminal_written(&mut self, session: SessionId, key: ExchangeKey) -> bool {
        let Some(publisher) = self.publishers.get_mut(&session) else {
            return false;
        };
        let Some(at) = publisher.writing.iter().position(|pending| *pending == key) else {
            return false;
        };
        publisher.writing.remove(at);
        true
    }

    /// Tombstones the node holds on `session`.
    pub fn tombstones(&self, session: SessionId) -> usize {
        self.publishers
            .get(&session)
            .map_or(0, |publisher| publisher.tombstones.len())
    }

    // The node as subscriber: its own updates, and the peer's pages.

    /// The `Open` the node writes to open `key` on `session` with `credit`
    /// from `start`. `None` if the session holds `max_live` subscriptions,
    /// the key is live or among the last `max_live` ended, or the credit
    /// lacks a unit or exceeds the row's.
    pub fn subscribe(
        &mut self,
        session: SessionId,
        key: ExchangeKey,
        credit: Credit,
        start: Cursor,
    ) -> Option<Update> {
        let empty_unit = credit.objects == 0 || credit.bytes == 0;
        let above = credit.objects > self.credit.objects || credit.bytes > self.credit.bytes;
        let subscriber = self.subscribers.entry(session).or_default();
        if subscriber.live.len() >= self.max_live
            || subscriber.live.contains_key(&key)
            || subscriber.ended.contains(&key)
            || empty_unit
            || above
        {
            return None;
        }
        let granted = Totals {
            objects: credit.objects.into(),
            bytes: credit.bytes.into(),
        };
        subscriber.live.insert(
            key,
            Subscription {
                sequence: 0,
                acknowledged: start,
                granted,
                consumed: Totals::default(),
                unaccepted: VecDeque::new(),
                accepted: Totals::default(),
                close_sent: false,
            },
        );
        Some(Update {
            sequence: 0,
            acknowledged: start,
            added: credit,
        })
    }

    /// The `Grant` the node writes to add `added` on `key`. `None` if `key`
    /// isn't live, `Close` was sent, `added` is empty, or the window from
    /// the accepted pages would exceed the row's credit.
    pub fn grant(&mut self, session: SessionId, key: ExchangeKey, added: Credit) -> Option<Update> {
        let credit = self.credit;
        let live = self
            .subscribers
            .get_mut(&session)
            .and_then(|subscriber| subscriber.live.get_mut(&key))?;
        let granted = live.granted.plus(added)?;
        let within = within_window(credit, granted, live.accepted);
        let sequence = live.sequence.checked_add(1)?;
        if live.close_sent || (added.objects == 0 && added.bytes == 0) || !within {
            return None;
        }
        live.sequence = sequence;
        live.granted = granted;
        Some(Update {
            sequence,
            acknowledged: live.acknowledged,
            added,
        })
    }

    /// The `Close` the node writes on `key`. `None` if `key` isn't live or
    /// `Close` was sent.
    pub fn close(&mut self, session: SessionId, key: ExchangeKey) -> Option<Update> {
        let live = self
            .subscribers
            .get_mut(&session)
            .and_then(|subscriber| subscriber.live.get_mut(&key))
            .filter(|live| !live.close_sent)?;
        let sequence = live.sequence.checked_add(1)?;
        live.sequence = sequence;
        live.close_sent = true;
        Some(Update {
            sequence,
            acknowledged: live.acknowledged,
            added: Credit {
                objects: 0,
                bytes: 0,
            },
        })
    }

    /// The node's handler accepted every page of `key` through `cursor`.
    /// False if no unaccepted page has that cursor.
    pub fn accept(&mut self, session: SessionId, key: ExchangeKey, cursor: Cursor) -> bool {
        let Some(live) = self
            .subscribers
            .get_mut(&session)
            .and_then(|subscriber| subscriber.live.get_mut(&key))
        else {
            return false;
        };
        let Some(index) = live.unaccepted.iter().position(|(at, _)| *at == cursor) else {
            return false;
        };
        let (_, through) = live
            .unaccepted
            .drain(..=index)
            .next_back()
            .expect("the drained range ends at a found index");
        live.accepted = through;
        live.acknowledged = cursor;
        true
    }

    /// Labels the peer's page for `key` on `session`, and spends its credit
    /// if conformant. A page after the node's `Close` within the credit is
    /// conformant.
    pub fn page_frame(
        &mut self,
        session: SessionId,
        key: ExchangeKey,
        objects: u32,
        payload_len: usize,
        cursor: Cursor,
    ) -> Labels {
        let Some(live) = self
            .subscribers
            .get_mut(&session)
            .and_then(|subscriber| subscriber.live.get_mut(&key))
        else {
            return Labels::of(Label::Unsolicited);
        };
        let bytes = u64::try_from(payload_len).unwrap_or(u64::MAX);
        let unspent = live.granted.minus(live.consumed).unwrap_or_default();
        if objects == 0 || !unspent.covers(objects.into(), bytes) {
            return Labels::of(Label::OverCredit);
        }
        live.consumed = Totals {
            objects: live.consumed.objects + u64::from(objects),
            bytes: live.consumed.bytes + bytes,
        };
        live.unaccepted.push_back((cursor, live.consumed));
        Labels::NONE
    }

    /// The credit the node granted on its live subscription `key` and the
    /// peer's conformant pages haven't spent, in each unit. `None` if `key`
    /// isn't live.
    pub fn unspent(&self, session: SessionId, key: ExchangeKey) -> Option<Totals> {
        let live = self.subscribers.get(&session)?.live.get(&key)?;
        Some(live.granted.minus(live.consumed).unwrap_or_default())
    }

    /// Labels the peer's terminal outcome for `key` on `session`, and ends
    /// the subscription if conformant.
    pub fn ending_frame(&mut self, session: SessionId, key: ExchangeKey) -> Labels {
        let max_live = self.max_live;
        let Some(subscriber) = self.subscribers.get_mut(&session) else {
            return Labels::of(Label::Unsolicited);
        };
        if subscriber.live.remove(&key).is_none() {
            return Labels::of(Label::Unsolicited);
        }
        subscriber.ended.push_back(key);
        while subscriber.ended.len() > max_live {
            subscriber.ended.pop_front();
        }
        Labels::NONE
    }

    /// `session` ended: its subscriptions and tombstones go.
    pub fn end_session(&mut self, session: SessionId) {
        self.publishers.remove(&session);
        self.subscribers.remove(&session);
    }
}

/// True if `granted` minus `acknowledged` fits `credit`, in each unit.
fn within_window(credit: Credit, granted: Totals, acknowledged: Totals) -> bool {
    granted.minus(acknowledged).is_none_or(|window| {
        window.objects <= u64::from(credit.objects) && window.bytes <= u64::from(credit.bytes)
    })
}

/// The faults of granting `added` on top of `granted`, with the window
/// starting at `from`: no credit added, or a window beyond `credit`.
fn grant_faults(credit: Credit, granted: Totals, added: Credit, from: Totals) -> Labels {
    let empty = added.objects == 0 && added.bytes == 0;
    let above = granted
        .plus(added)
        .is_none_or(|granted| !within_window(credit, granted, from));
    Labels::NONE
        .with_if(empty, Label::BadCursor)
        .with_if(above, Label::BadCursor)
}

/// True if `sequence` is one more than `previous`.
fn follows(previous: u32, sequence: u32) -> bool {
    previous.checked_add(1) == Some(sequence)
}

/// Where `acknowledged` sits: `(None, totals)` for the current
/// acknowledgement, `(Some(index), totals)` for a sent page, or `None` if it
/// names neither.
fn acknowledged_through(
    live: &Publication,
    acknowledged: Cursor,
) -> Option<(Option<usize>, Totals)> {
    if live.acknowledged.0 == acknowledged {
        return Some((None, live.acknowledged.1));
    }
    live.sent
        .iter()
        .position(|(cursor, _)| *cursor == acknowledged)
        .map(|index| (Some(index), live.sent[index].1))
}

/// Forgets every sent page through `through`.
fn acknowledge(live: &mut Publication, through: Option<usize>) {
    if let Some(index) = through {
        live.acknowledged = live
            .sent
            .drain(..=index)
            .next_back()
            .expect("the drained range ends at a found index");
    }
}
