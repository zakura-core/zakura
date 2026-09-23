//! The publisher's side: subscriptions a peer opened on this session.

use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
    sync::Arc,
};

use super::{
    credit::{ResponseCredit, Totals},
    is_empty, SubscriptionFault, SubscriptionLimits,
};
use crate::zakura::{
    regulation::{SlotBudget, SlotPermit},
    Credit, Frame, FrameGuard, FramedSend,
};

/// How the publisher applied an update that matched.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum Applied {
    /// The update changed a live subscription.
    Live,
    /// The update crossed its subscription's terminal outcome and matched the
    /// tombstone. Drop it.
    Crossed,
}

/// Why the publisher cannot produce a page now. It is local, never a fault.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum PageStall {
    /// No live subscription has this key.
    Ended,
    /// The subscriber sent `Close`; end the subscription instead.
    Closing,
    /// The unspent credit does not cover the page. Wait for a `Grant`.
    NoCredit,
    /// The cursor history is full. The credit window makes this unreachable
    /// for a conformant subscriber.
    HistoryFull,
    /// A page must carry at least one object.
    EmptyPage,
}

/// A terminal outcome's reserved output, and the subscription's slot.
///
/// Send the outcome with this permit and drop it when the write finishes.
#[derive(Debug)]
#[must_use = "dropping the terminal permit frees the subscription's slot"]
pub(crate) struct TerminalPermit {
    _slot: SlotPermit,
}

impl TerminalPermit {
    /// Queue the terminal outcome `frame` on `send`, holding this permit
    /// until the frame's transport write finishes. It needs no output grant
    /// and no execution slot.
    ///
    /// Returns false if the stream is closed.
    pub(crate) async fn send(self, send: &FramedSend, frame: Frame) -> bool {
        match send.reserve_guarded().await {
            Ok(slot) => {
                slot.send(frame, FrameGuard::new(Arc::new(self)));
                true
            }
            Err(_) => false,
        }
    }
}

#[derive(Debug)]
struct Published<C> {
    /// The last update sequence applied.
    sequence: u32,
    credit: ResponseCredit,
    /// The acknowledged cursor and the consumed totals through it.
    acknowledged: (C, Totals),
    /// Pages sent after the acknowledged cursor, oldest first, each with the
    /// consumed totals through it.
    sent: VecDeque<(C, Totals)>,
    closing: bool,
    slot: SlotPermit,
}

#[derive(Debug)]
struct Tombstone<K> {
    key: K,
    sequence: u32,
}

/// The subscriptions a peer opened on one session, keyed by `K`, with
/// cursors `C`.
///
/// Cursors of one subscription must be distinct; the tools compare them by
/// equality only.
#[derive(Debug)]
pub(crate) struct Publications<K, C> {
    limits: SubscriptionLimits,
    live: HashMap<K, Published<C>>,
    /// Ended subscriptions, oldest first.
    tombstones: VecDeque<Tombstone<K>>,
    /// Live and closing subscriptions: `2 × max_live` slots.
    slots: SlotBudget,
    slot_count: usize,
    over_limit: u64,
}

impl<K: Eq + Hash + Clone, C: Clone + Eq> Publications<K, C> {
    /// An empty table for the subscription row `limits`.
    pub(crate) fn new(limits: SubscriptionLimits) -> Self {
        let slots = limits.max_live().saturating_mul(2);
        Self {
            limits,
            live: HashMap::new(),
            tombstones: VecDeque::new(),
            slots: SlotBudget::new(slots)
                .expect("the validator requires max_live >= 1, and twice it fits a semaphore"),
            slot_count: slots,
            over_limit: 0,
        }
    }

    /// Apply `Open`.
    pub(crate) fn open(
        &mut self,
        key: K,
        sequence: u32,
        credit: Credit,
        start: C,
    ) -> Result<(), SubscriptionFault> {
        let message_type = self.limits.message_type;
        let fault = move |fault: SubscriptionFault| fault.counted(message_type);
        if sequence != 0 {
            return Err(fault(SubscriptionFault::Sequence {
                expected: 0,
                got: sequence,
            }));
        }
        if self.live.contains_key(&key) || self.tombstones.iter().any(|dead| dead.key == key) {
            return Err(fault(SubscriptionFault::ReusedKey));
        }
        if credit.objects == 0 || credit.bytes == 0 {
            return Err(fault(SubscriptionFault::EmptyCredit));
        }
        let mut granted = ResponseCredit::new(Credit {
            objects: 0,
            bytes: 0,
        });
        granted
            .grant(credit, Totals::default(), self.limits.credit)
            .map_err(|_| fault(SubscriptionFault::AboveWindow))?;
        let held = self.slot_count - self.slots.available();
        let Some(slot) = self.slots.try_reserve() else {
            return Err(fault(SubscriptionFault::NoSlot {
                held,
                limit: self.limits.max_live,
            }));
        };
        if held >= self.limits.max_live() {
            self.count_over_limit(held);
        }
        // The subscriber received the oldest terminal outcomes; see the
        // module docs.
        let keep = self
            .limits
            .max_live()
            .saturating_sub(1)
            .saturating_sub(self.live.len());
        while self.tombstones.len() > keep {
            self.tombstones.pop_front();
        }
        self.live.insert(
            key,
            Published {
                sequence: 0,
                credit: granted,
                acknowledged: (start, Totals::default()),
                sent: VecDeque::new(),
                closing: false,
                slot,
            },
        );
        Ok(())
    }

    /// Apply `Grant`: check the sequence and the acknowledgement, then add
    /// the credit. A refused grant changes nothing.
    pub(crate) fn grant(
        &mut self,
        key: &K,
        sequence: u32,
        acknowledged: &C,
        added: Credit,
    ) -> Result<Applied, SubscriptionFault> {
        let message_type = self.limits.message_type;
        let limit = self.limits.credit;
        let fault = move |fault: SubscriptionFault| fault.counted(message_type);
        let Some(published) = self.live.get_mut(key) else {
            return self.crossed(key, sequence, false);
        };
        if published.closing {
            return Err(fault(SubscriptionFault::AfterClose));
        }
        next_sequence(published.sequence, sequence).map_err(fault)?;
        let through = find_acknowledged(published, acknowledged).map_err(fault)?;
        if is_empty(added) {
            return Err(fault(SubscriptionFault::EmptyCredit));
        }
        let totals = through.map_or(published.acknowledged.1, |index| published.sent[index].1);
        published
            .credit
            .grant(added, totals, limit)
            .map_err(|_| fault(SubscriptionFault::AboveWindow))?;
        published.sequence = sequence;
        acknowledge(published, through);
        Ok(Applied::Live)
    }

    /// Apply `Close`: check the sequence and the acknowledgement, then stop
    /// producing pages. End the subscription once the pages already queued
    /// are written.
    pub(crate) fn close(
        &mut self,
        key: &K,
        sequence: u32,
        acknowledged: &C,
    ) -> Result<Applied, SubscriptionFault> {
        let message_type = self.limits.message_type;
        let fault = move |fault: SubscriptionFault| fault.counted(message_type);
        let Some(published) = self.live.get_mut(key) else {
            return self.crossed(key, sequence, true);
        };
        if published.closing {
            return Err(fault(SubscriptionFault::AfterClose));
        }
        next_sequence(published.sequence, sequence).map_err(fault)?;
        let through = find_acknowledged(published, acknowledged).map_err(fault)?;
        published.sequence = sequence;
        published.closing = true;
        acknowledge(published, through);
        Ok(Applied::Live)
    }

    /// Match an update for no live subscription against the tombstones. A
    /// `Close` consumes its tombstone; a `Grant` keeps it.
    fn crossed(
        &mut self,
        key: &K,
        sequence: u32,
        close: bool,
    ) -> Result<Applied, SubscriptionFault> {
        let message_type = self.limits.message_type;
        let fault = move |fault: SubscriptionFault| fault.counted(message_type);
        let index = self
            .tombstones
            .iter()
            .position(|dead| dead.key == *key)
            .ok_or_else(|| fault(SubscriptionFault::Unknown))?;
        next_sequence(self.tombstones[index].sequence, sequence).map_err(fault)?;
        if close {
            self.tombstones.remove(index);
        } else {
            self.tombstones[index].sequence = sequence;
        }
        Ok(Applied::Crossed)
    }

    /// Reserve credit and a cursor for the next page before producing it.
    ///
    /// `bytes` is the page's encoded payload length. The page spends exactly
    /// `objects` and `bytes`.
    pub(crate) fn reserve_page(
        &mut self,
        key: &K,
        objects: u32,
        bytes: usize,
        cursor: C,
    ) -> Result<(), PageStall> {
        let history = usize::try_from(self.limits.cursor_history).unwrap_or(usize::MAX);
        let published = self.live.get_mut(key).ok_or(PageStall::Ended)?;
        if published.closing {
            return Err(PageStall::Closing);
        }
        if objects == 0 {
            return Err(PageStall::EmptyPage);
        }
        // Widening usize to u64 is lossless on supported targets.
        let bytes = bytes as u64;
        published
            .credit
            .check(u64::from(objects), bytes)
            .map_err(|_| PageStall::NoCredit)?;
        if published.sent.len() >= history {
            return Err(PageStall::HistoryFull);
        }
        // The check above found enough unspent credit, so this spends it.
        published
            .credit
            .consume(u64::from(objects), bytes)
            .map_err(|_| PageStall::NoCredit)?;
        published
            .sent
            .push_back((cursor, published.credit.consumed()));
        Ok(())
    }

    /// End the subscription, after `Close` or on this node's own outcome.
    ///
    /// Free the key, keep a tombstone, and return the terminal permit. Write
    /// terminal outcomes in the order this returns them. The outcome spends
    /// no credit.
    pub(crate) fn end(&mut self, key: &K) -> Option<TerminalPermit> {
        let published = self.live.remove(key)?;
        self.tombstones.push_back(Tombstone {
            key: key.clone(),
            sequence: published.sequence,
        });
        let bound = self.limits.max_live().saturating_mul(2);
        while self.tombstones.len() > bound {
            self.tombstones.pop_front();
        }
        Some(TerminalPermit {
            _slot: published.slot,
        })
    }

    /// The cursor of the last page sent, or the acknowledged cursor if none
    /// is unacknowledged. The next page follows it.
    pub(crate) fn last_sent(&self, key: &K) -> Option<&C> {
        let published = self.live.get(key)?;
        Some(
            published
                .sent
                .back()
                .map_or(&published.acknowledged.0, |(cursor, _)| cursor),
        )
    }

    /// Unspent credit of `key`, if it is live and not closing.
    pub(crate) fn unspent(&self, key: &K) -> Option<Totals> {
        self.live
            .get(key)
            .filter(|published| !published.closing)
            .map(|published| published.credit.unspent())
    }

    /// Whether `key` is live and the subscriber sent `Close`.
    pub(crate) fn is_closing(&self, key: &K) -> bool {
        self.live
            .get(key)
            .is_some_and(|published| published.closing)
    }

    /// Live and closing subscriptions.
    pub(crate) fn keys(&self) -> impl Iterator<Item = &K> {
        self.live.keys()
    }

    /// Sent pages the subscriber has not acknowledged.
    pub(crate) fn unacknowledged(&self, key: &K) -> Option<usize> {
        self.live.get(key).map(|published| published.sent.len())
    }

    /// Tombstones held.
    pub(crate) fn tombstones(&self) -> usize {
        self.tombstones.len()
    }

    /// Opens admitted above `max_live`, within the margin.
    pub(crate) fn over_limit_count(&self) -> u64 {
        self.over_limit
    }

    /// The credit `key` granted and consumed so far.
    pub(crate) fn credit(&self, key: &K) -> Option<&ResponseCredit> {
        self.live.get(key).map(|published| &published.credit)
    }

    fn count_over_limit(&mut self, held: usize) {
        self.over_limit += 1;
        metrics::counter!(
            "zakura.p2p.subscription.over_limit",
            "message_type" => self.limits.message_type.to_string(),
        )
        .increment(1);
        tracing::debug!(
            message_type = self.limits.message_type,
            held,
            limit = self.limits.max_live,
            "peer holds more subscriptions than the limit; admitting within the margin"
        );
    }
}

/// Check that `sequence` follows `previous` by exactly one.
fn next_sequence(previous: u32, sequence: u32) -> Result<(), SubscriptionFault> {
    match previous.checked_add(1) {
        Some(expected) if expected == sequence => Ok(()),
        expected => Err(SubscriptionFault::Sequence {
            expected: expected.unwrap_or(u32::MAX),
            got: sequence,
        }),
    }
}

/// Find `acknowledged`: `None` for the current acknowledgement, or the index
/// of a sent page.
fn find_acknowledged<C: Eq>(
    published: &Published<C>,
    acknowledged: &C,
) -> Result<Option<usize>, SubscriptionFault> {
    if published.acknowledged.0 == *acknowledged {
        return Ok(None);
    }
    published
        .sent
        .iter()
        .position(|(cursor, _)| cursor == acknowledged)
        .map(Some)
        .ok_or(SubscriptionFault::UnknownAcknowledgement)
}

/// Forget every sent page through `through`.
fn acknowledge<C>(published: &mut Published<C>, through: Option<usize>) {
    if let Some(index) = through {
        published.acknowledged = published
            .sent
            .drain(..=index)
            .last()
            .expect("the drained range ends at a found index");
    }
}
