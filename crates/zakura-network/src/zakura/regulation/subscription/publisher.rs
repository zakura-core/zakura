//! The publisher's side: subscriptions a peer opened on this session.
//!
//! [`PublicationLedger`] holds the protocol rules as values: credit, cursors,
//! update sequences, and tombstones. [`PublicationSlots`] owns the runtime
//! slot permits, and names each one by a [`SlotId`] the ledger can hold. A
//! pure owner keeps the ledger in its state and its runtime owner keeps the
//! slots. [`Publications`] joins the two for an owner with no such split.

use std::{
    collections::{BTreeMap, VecDeque},
    hash::Hash,
    sync::Arc,
};

use zakura_rt::collections::HashMap;

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

/// The identity of one held subscription slot, unique within its
/// [`PublicationSlots`]. A reused subscription key gets a new one.
#[derive(Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct SlotId(u64);

/// What the slot owner found for one `Open`: the slots held before it, and
/// the slot it reserved, if one was free. The owner reserves the slot before
/// the ledger decides, so a snapshot never grants capacity.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct SlotOffer {
    held: usize,
    slot: Option<SlotId>,
}

/// The slots of one session's subscriptions: `2 × max_live` permits, each
/// held from `Open` until its terminal outcome's write finishes.
#[derive(Debug)]
pub(crate) struct PublicationSlots {
    budget: SlotBudget,
    next: u64,
    owned: BTreeMap<SlotId, SlotPermit>,
    capacity: usize,
}

impl PublicationSlots {
    /// The slots for the subscription row `limits`.
    pub(crate) fn new(limits: &SubscriptionLimits) -> Self {
        let capacity = limits.max_live().saturating_mul(2);
        Self {
            budget: SlotBudget::new(capacity)
                .expect("the validator requires max_live >= 1, and twice it fits a semaphore"),
            next: 0,
            owned: BTreeMap::new(),
            capacity,
        }
    }

    /// Reserve a slot for one `Open`, if one is free. Give an offer the ledger
    /// refused back to [`Self::refused`].
    pub(crate) fn offer(&mut self) -> SlotOffer {
        let held = self.held();
        let slot = self.budget.try_reserve().map(|permit| {
            let id = SlotId(self.next);
            self.next = self.next.wrapping_add(1);
            self.owned.insert(id, permit);
            id
        });
        SlotOffer { held, slot }
    }

    /// Free the slot of the latest `offer`, which the ledger refused. Nothing
    /// else named its identity, so the next offer reuses it, and a refused
    /// `Open` changes nothing.
    pub(crate) fn refused(&mut self, offer: SlotOffer) {
        if let Some(slot) = offer.slot {
            self.owned.remove(&slot);
            self.next = slot.0;
        }
    }

    /// Move held slot `slot` into its terminal outcome's permit.
    pub(crate) fn terminal(&mut self, slot: SlotId) -> Option<TerminalPermit> {
        self.owned
            .remove(&slot)
            .map(|permit| TerminalPermit { _slot: permit })
    }

    /// Slots held now: by a subscription, or by a terminal outcome until its
    /// write finishes.
    pub(crate) fn held(&self) -> usize {
        self.capacity - self.budget.available()
    }
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
    slot: SlotId,
}

/// An ended subscription; see the module docs for the rules it enforces.
#[derive(Debug)]
struct Tombstone<K> {
    key: K,
    /// The last update sequence applied.
    sequence: u32,
    /// The ended subscription's credit. Crossed grants add to it.
    credit: ResponseCredit,
    /// Whether the subscriber sent `Close` before the end.
    close_received: bool,
}

/// The protocol rules of the subscriptions a peer opened on one session,
/// keyed by `K`, with cursors `C`. It holds values only: each subscription
/// names its slot by [`SlotId`], and the slot's owner holds the permit.
///
/// Cursors of one subscription must be distinct; the ledger compares them by
/// equality only.
#[derive(Debug)]
pub(crate) struct PublicationLedger<K, C> {
    limits: SubscriptionLimits,
    live: HashMap<K, Published<C>>,
    /// Ended subscriptions, oldest first.
    tombstones: VecDeque<Tombstone<K>>,
    over_limit: u64,
    /// Reports this table's transitions, in test and simulator builds.
    #[cfg(any(test, zakura_sim))]
    tap: Option<crate::zakura::RegulationTap<K>>,
}

impl<K: Eq + Hash + Clone, C: Clone + Eq> PublicationLedger<K, C> {
    /// An empty ledger for the subscription row `limits`.
    pub(crate) fn new(limits: SubscriptionLimits) -> Self {
        Self {
            limits,
            live: HashMap::new(),
            tombstones: VecDeque::new(),
            over_limit: 0,
            #[cfg(any(test, zakura_sim))]
            tap: None,
        }
    }

    /// This table, reporting its transitions through `tap` if there is one.
    /// Attach it before sharing the table, since locking a shared table is a
    /// scheduling point in a simulated run.
    #[cfg(any(test, zakura_sim))]
    pub(crate) fn observed_by(mut self, tap: Option<crate::zakura::RegulationTap<K>>) -> Self {
        self.tap = tap;
        self
    }

    /// Report `transition` if a tap is attached. `transition` is built only
    /// then.
    #[cfg(any(test, zakura_sim))]
    fn regulated(
        &self,
        transition: impl FnOnce(&crate::zakura::RegulationTap<K>) -> crate::zakura::Transition,
    ) {
        if let Some(tap) = &self.tap {
            tap.stream().regulated(transition(tap));
        }
    }

    /// Apply `Open` under the slot `offer`. On success the subscription holds
    /// the offered slot. On a refusal, give the offer back to its owner.
    pub(crate) fn open(
        &mut self,
        key: K,
        sequence: u32,
        credit: Credit,
        start: C,
        offer: SlotOffer,
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
        let held = offer.held;
        let Some(slot) = offer.slot else {
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
        #[cfg(any(test, zakura_sim))]
        self.regulated(|tap| crate::zakura::Transition::OpenApplied {
            subscription: tap.id(&key),
            objects: credit.objects,
            bytes: credit.bytes,
            seq: tap.stream().serving(),
        });
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
            return self.crossed(key, sequence, Some(added));
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
        #[cfg(any(test, zakura_sim))]
        if let Some(tap) = &self.tap {
            tap.stream()
                .regulated(crate::zakura::Transition::GrantApplied {
                    subscription: tap.id(key),
                    objects: added.objects,
                    bytes: added.bytes,
                });
        }
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
            return self.crossed(key, sequence, None);
        };
        if published.closing {
            return Err(fault(SubscriptionFault::AfterClose));
        }
        next_sequence(published.sequence, sequence).map_err(fault)?;
        let through = find_acknowledged(published, acknowledged).map_err(fault)?;
        published.sequence = sequence;
        published.closing = true;
        acknowledge(published, through);
        #[cfg(any(test, zakura_sim))]
        if let Some(tap) = &self.tap {
            tap.stream()
                .regulated(crate::zakura::Transition::CloseApplied {
                    subscription: tap.id(key),
                });
        }
        Ok(Applied::Live)
    }

    /// Match an update for no live subscription against the tombstones; see
    /// the module docs. `added` is a `Grant`'s credit, or `None` for `Close`.
    fn crossed(
        &mut self,
        key: &K,
        sequence: u32,
        added: Option<Credit>,
    ) -> Result<Applied, SubscriptionFault> {
        let message_type = self.limits.message_type;
        let limit = self.limits.credit;
        let fault = move |fault: SubscriptionFault| fault.counted(message_type);
        let index = self
            .tombstones
            .iter()
            .position(|dead| dead.key == *key)
            .ok_or_else(|| fault(SubscriptionFault::Unknown))?;
        let dead = &mut self.tombstones[index];
        if dead.close_received {
            return Err(fault(SubscriptionFault::AfterClose));
        }
        next_sequence(dead.sequence, sequence).map_err(fault)?;
        let Some(added) = added else {
            self.tombstones.remove(index);
            return Ok(Applied::Crossed);
        };
        if is_empty(added) {
            return Err(fault(SubscriptionFault::EmptyCredit));
        }
        // The subscriber acknowledged at most every page sent before the end,
        // so its window starts at or before the consumed credit.
        let consumed = dead.credit.consumed();
        dead.credit
            .grant(added, consumed, limit)
            .map_err(|_| fault(SubscriptionFault::CrossedAboveWindow))?;
        dead.sequence = sequence;
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
    /// Free the key, keep a tombstone, and return the subscription's slot.
    /// Write terminal outcomes in the order this returns them. The outcome
    /// spends no credit.
    pub(crate) fn end(&mut self, key: &K) -> Option<SlotId> {
        let published = self.live.remove(key)?;
        self.tombstones.push_back(Tombstone {
            key: key.clone(),
            sequence: published.sequence,
            credit: published.credit,
            close_received: published.closing,
        });
        let bound = self.limits.max_live().saturating_mul(2);
        while self.tombstones.len() > bound {
            self.tombstones.pop_front();
        }
        #[cfg(any(test, zakura_sim))]
        self.regulated(|tap| crate::zakura::Transition::PublicationEnded {
            subscription: tap.id(key),
        });
        Some(published.slot)
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

/// The subscriptions a peer opened on one session: a [`PublicationLedger`]
/// and the [`PublicationSlots`] it names, under one owner.
#[derive(Debug)]
pub(crate) struct Publications<K, C> {
    ledger: PublicationLedger<K, C>,
    slots: PublicationSlots,
}

impl<K: Eq + Hash + Clone, C: Clone + Eq> Publications<K, C> {
    /// An empty table for the subscription row `limits`.
    pub(crate) fn new(limits: SubscriptionLimits) -> Self {
        Self {
            slots: PublicationSlots::new(&limits),
            ledger: PublicationLedger::new(limits),
        }
    }

    /// This table, reporting its transitions through `tap` if there is one.
    /// Attach it before sharing the table, since locking a shared table is a
    /// scheduling point in a simulated run.
    #[cfg(any(test, zakura_sim))]
    pub(crate) fn observed_by(mut self, tap: Option<crate::zakura::RegulationTap<K>>) -> Self {
        self.ledger = self.ledger.observed_by(tap);
        self
    }

    /// Apply `Open`: reserve a slot, then apply the ledger's rules. A refused
    /// `Open` frees the slot.
    pub(crate) fn open(
        &mut self,
        key: K,
        sequence: u32,
        credit: Credit,
        start: C,
    ) -> Result<(), SubscriptionFault> {
        let offer = self.slots.offer();
        let opened = self.ledger.open(key, sequence, credit, start, offer);
        if opened.is_err() {
            self.slots.refused(offer);
        }
        opened
    }

    /// See [`PublicationLedger::grant`].
    pub(crate) fn grant(
        &mut self,
        key: &K,
        sequence: u32,
        acknowledged: &C,
        added: Credit,
    ) -> Result<Applied, SubscriptionFault> {
        self.ledger.grant(key, sequence, acknowledged, added)
    }

    /// See [`PublicationLedger::close`].
    pub(crate) fn close(
        &mut self,
        key: &K,
        sequence: u32,
        acknowledged: &C,
    ) -> Result<Applied, SubscriptionFault> {
        self.ledger.close(key, sequence, acknowledged)
    }

    /// See [`PublicationLedger::reserve_page`].
    pub(crate) fn reserve_page(
        &mut self,
        key: &K,
        objects: u32,
        bytes: usize,
        cursor: C,
    ) -> Result<(), PageStall> {
        self.ledger.reserve_page(key, objects, bytes, cursor)
    }

    /// End the subscription, after `Close` or on this node's own outcome, and
    /// return the terminal permit. See [`PublicationLedger::end`].
    pub(crate) fn end(&mut self, key: &K) -> Option<TerminalPermit> {
        let slot = self.ledger.end(key)?;
        self.slots.terminal(slot)
    }

    /// See [`PublicationLedger::last_sent`].
    pub(crate) fn last_sent(&self, key: &K) -> Option<&C> {
        self.ledger.last_sent(key)
    }

    /// See [`PublicationLedger::unspent`].
    pub(crate) fn unspent(&self, key: &K) -> Option<Totals> {
        self.ledger.unspent(key)
    }

    /// See [`PublicationLedger::is_closing`].
    pub(crate) fn is_closing(&self, key: &K) -> bool {
        self.ledger.is_closing(key)
    }

    /// See [`PublicationLedger::keys`].
    pub(crate) fn keys(&self) -> impl Iterator<Item = &K> {
        self.ledger.keys()
    }

    /// See [`PublicationLedger::unacknowledged`].
    pub(crate) fn unacknowledged(&self, key: &K) -> Option<usize> {
        self.ledger.unacknowledged(key)
    }

    /// See [`PublicationLedger::tombstones`].
    pub(crate) fn tombstones(&self) -> usize {
        self.ledger.tombstones()
    }

    /// See [`PublicationLedger::over_limit_count`].
    pub(crate) fn over_limit_count(&self) -> u64 {
        self.ledger.over_limit_count()
    }

    /// See [`PublicationLedger::credit`].
    pub(crate) fn credit(&self, key: &K) -> Option<&ResponseCredit> {
        self.ledger.credit(key)
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
