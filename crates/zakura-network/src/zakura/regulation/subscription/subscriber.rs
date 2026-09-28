//! The subscriber's side: subscriptions this session opened.

use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
    sync::{Arc, Mutex, PoisonError},
};

use thiserror::Error;

use super::{
    credit::{ResponseCredit, Totals},
    is_empty, SubscriptionFault, SubscriptionLimits,
};
use crate::zakura::{
    regulation::{Exchange, ResponsePrecheck},
    Credit, FrameRejection, MessageRole, MessageRule,
};

/// An update to write, recorded before it is written.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Update<C> {
    /// The update sequence: zero for `Open`, then one more for each update.
    pub(crate) sequence: u32,
    /// The acknowledged cursor: the start for `Open`, then the last page the
    /// handler accepted.
    pub(crate) acknowledged: C,
    /// Credit the update adds. `Close` adds none.
    pub(crate) added: Credit,
}

/// Why this node did not send an update. The peer is not at fault.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum SubscribeRefused {
    /// The session holds `max_live` subscriptions.
    #[error("the session holds its maximum of live subscriptions")]
    AtCapacity,
    /// The key is live, or the publisher may still hold its tombstone.
    #[error("the key is live or may still be tombstoned")]
    KeyInUse,
    /// No live subscription has this key.
    #[error("no live subscription has this key")]
    Unknown,
    /// `Close` was already sent.
    #[error("the subscription is closing")]
    Closing,
    /// The update adds no credit, or an `Open` lacks a unit.
    #[error("the update adds no credit")]
    EmptyCredit,
    /// The grant would push the window past the row's credit.
    #[error("the grant exceeds the credit window")]
    AboveWindow,
    /// The cursor names no page the handler received and has not accepted.
    #[error("no received page has this cursor")]
    UnknownCursor,
    /// The update sequence would pass `u32::MAX`.
    #[error("the update sequence is exhausted")]
    SequenceExhausted,
}

/// A consumed subscription, with the facts an outcome window needs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SubscriptionEnded<C> {
    /// Pages received.
    pub(crate) pages: u64,
    /// Whether this node sent `Close`.
    pub(crate) close_sent: bool,
    /// The cursor of the last page received, or the start.
    pub(crate) received: C,
}

#[derive(Debug)]
struct Subscribed<C> {
    credit: ResponseCredit,
    /// The last update sequence recorded.
    sequence: u32,
    received: C,
    pages: u64,
    /// Received pages the handler has not accepted, oldest first, each with
    /// the consumed totals through it.
    unaccepted: VecDeque<(C, Totals)>,
    accepted: (C, Totals),
    close_sent: bool,
    /// The `Open`'s exchange, if the reactor fences its writers. The terminal
    /// outcome ends it.
    exchange: Option<Exchange>,
}

/// The subscriptions one session opened, keyed by `K`, with cursors `C`.
///
/// Cursors of one subscription must be distinct; the tools compare them by
/// equality only.
#[derive(Debug)]
pub(crate) struct Subscriptions<K, C> {
    limits: SubscriptionLimits,
    rules: &'static [MessageRule],
    live: HashMap<K, Subscribed<C>>,
    /// Keys of the latest terminal outcomes, newest last. The publisher may
    /// still hold their tombstones, so `open` refuses them.
    ended: VecDeque<K>,
}

impl<K: Eq + Hash + Clone, C: Clone + Eq> Subscriptions<K, C> {
    /// An empty table for the subscription row `limits` of the family `rules`.
    pub(crate) fn new(limits: SubscriptionLimits, rules: &'static [MessageRule]) -> Self {
        Self {
            limits,
            rules,
            live: HashMap::new(),
            ended: VecDeque::new(),
        }
    }

    /// Record `Open` before writing it.
    pub(crate) fn open(
        &mut self,
        key: K,
        credit: Credit,
        start: C,
    ) -> Result<Update<C>, SubscribeRefused> {
        self.insert(key, credit, start, None)
    }

    /// Record `Open` as [`Self::open`] does, and keep `exchange` until the
    /// terminal outcome ends it.
    ///
    /// The session owns this table, so the session's end drops every live
    /// exchange. One whose `Open` was written then closes the connection.
    pub(crate) fn open_fenced(
        &mut self,
        key: K,
        credit: Credit,
        start: C,
        exchange: Exchange,
    ) -> Result<Update<C>, SubscribeRefused> {
        self.insert(key, credit, start, Some(exchange))
    }

    fn insert(
        &mut self,
        key: K,
        credit: Credit,
        start: C,
        exchange: Option<Exchange>,
    ) -> Result<Update<C>, SubscribeRefused> {
        if self.live.len() >= self.limits.max_live() {
            return Err(SubscribeRefused::AtCapacity);
        }
        if self.live.contains_key(&key) || self.ended.contains(&key) {
            return Err(SubscribeRefused::KeyInUse);
        }
        if credit.objects == 0 || credit.bytes == 0 {
            return Err(SubscribeRefused::EmptyCredit);
        }
        let mut granted = ResponseCredit::new(Credit {
            objects: 0,
            bytes: 0,
        });
        granted
            .grant(credit, Totals::default(), self.limits.credit)
            .map_err(|_| SubscribeRefused::AboveWindow)?;
        self.live.insert(
            key,
            Subscribed {
                credit: granted,
                sequence: 0,
                received: start.clone(),
                pages: 0,
                unaccepted: VecDeque::new(),
                accepted: (start.clone(), Totals::default()),
                close_sent: false,
                exchange,
            },
        );
        Ok(Update {
            sequence: 0,
            acknowledged: start,
            added: credit,
        })
    }

    /// Remove a subscription whose `Open` was never written.
    ///
    /// Use it only when the send failed locally before the first byte.
    pub(crate) fn retract(&mut self, key: &K) -> bool {
        self.live.remove(key).is_some()
    }

    /// Record `Grant` before writing it. It acknowledges the last page the
    /// handler accepted.
    pub(crate) fn grant(&mut self, key: &K, added: Credit) -> Result<Update<C>, SubscribeRefused> {
        let limit = self.limits.credit;
        let subscribed = self.live.get_mut(key).ok_or(SubscribeRefused::Unknown)?;
        if subscribed.close_sent {
            return Err(SubscribeRefused::Closing);
        }
        if is_empty(added) {
            return Err(SubscribeRefused::EmptyCredit);
        }
        let sequence = subscribed
            .sequence
            .checked_add(1)
            .ok_or(SubscribeRefused::SequenceExhausted)?;
        subscribed
            .credit
            .grant(added, subscribed.accepted.1, limit)
            .map_err(|_| SubscribeRefused::AboveWindow)?;
        subscribed.sequence = sequence;
        Ok(Update {
            sequence: subscribed.sequence,
            acknowledged: subscribed.accepted.0.clone(),
            added,
        })
    }

    /// The most a `Grant` may add now: the window left after the accepted
    /// progress.
    pub(crate) fn grantable(&self, key: &K) -> Option<Credit> {
        let subscribed = self.live.get(key)?;
        let granted = subscribed.credit.granted();
        let accepted = subscribed.accepted.1;
        let left = |limit: u32, granted: u64, accepted: u64| {
            let window = granted.saturating_sub(accepted);
            u32::try_from(u64::from(limit).saturating_sub(window)).unwrap_or(u32::MAX)
        };
        Some(Credit {
            objects: left(
                self.limits.credit.objects,
                granted.objects,
                accepted.objects,
            ),
            bytes: left(self.limits.credit.bytes, granted.bytes, accepted.bytes),
        })
    }

    /// Record `Close` before writing it. Existing credit stays, and pages
    /// within it still reach the handler.
    pub(crate) fn close(&mut self, key: &K) -> Result<Update<C>, SubscribeRefused> {
        let subscribed = self.live.get_mut(key).ok_or(SubscribeRefused::Unknown)?;
        if subscribed.close_sent {
            return Err(SubscribeRefused::Closing);
        }
        subscribed.sequence = subscribed
            .sequence
            .checked_add(1)
            .ok_or(SubscribeRefused::SequenceExhausted)?;
        subscribed.close_sent = true;
        Ok(Update {
            sequence: subscribed.sequence,
            acknowledged: subscribed.accepted.0.clone(),
            added: Credit {
                objects: 0,
                bytes: 0,
            },
        })
    }

    /// Record that the handler accepted every page through `cursor`. The
    /// next update acknowledges it.
    pub(crate) fn accept(&mut self, key: &K, cursor: &C) -> Result<(), SubscribeRefused> {
        let subscribed = self.live.get_mut(key).ok_or(SubscribeRefused::Unknown)?;
        let through = subscribed
            .unaccepted
            .iter()
            .position(|(page, _)| page == cursor)
            .ok_or(SubscribeRefused::UnknownCursor)?;
        let accepted = subscribed
            .unaccepted
            .drain(..=through)
            .last()
            .expect("the drained range ends at a found index");
        subscribed.accepted = accepted;
        Ok(())
    }

    /// Check a page or terminal header before its payload is read.
    ///
    /// A page needs a live subscription with `payload_len` unspent bytes and
    /// an unspent object; a terminal outcome needs a live subscription.
    pub(crate) fn precheck(
        &self,
        message_type: u16,
        payload_len: usize,
    ) -> Result<(), SubscriptionFault> {
        let ends = match MessageRule::find(self.rules, message_type).map(|row| row.role) {
            Some(MessageRole::Response {
                request,
                ends_exchange,
            }) if request == self.limits.message_type => ends_exchange,
            _ => return Err(SubscriptionFault::Unknown),
        };
        if self.live.is_empty() {
            return Err(SubscriptionFault::Unknown);
        }
        // Widening usize to u64 is lossless on supported targets.
        let len = payload_len as u64;
        if ends
            || self
                .live
                .values()
                .any(|live| live.credit.check(1, len).is_ok())
        {
            return Ok(());
        }
        Err(SubscriptionFault::OverCredit)
    }

    /// Admit a decoded page whose linkage the reactor checked. Spend its exact
    /// credit and advance the receive cursor, before the handler starts.
    ///
    /// A page after `Close` within existing credit is admitted.
    pub(crate) fn claim_page(
        &mut self,
        key: &K,
        objects: u32,
        payload_len: usize,
        cursor: C,
    ) -> Result<(), SubscriptionFault> {
        let message_type = self.limits.message_type;
        let subscribed = self
            .live
            .get_mut(key)
            .ok_or_else(|| SubscriptionFault::Unknown.counted(message_type))?;
        // Widening usize to u64 is lossless on supported targets.
        let bytes = payload_len as u64;
        if objects == 0 {
            return Err(SubscriptionFault::OverCredit.counted(message_type));
        }
        subscribed
            .credit
            .consume(u64::from(objects), bytes)
            .map_err(|_| SubscriptionFault::OverCredit.counted(message_type))?;
        subscribed.pages += 1;
        subscribed.received = cursor.clone();
        subscribed
            .unaccepted
            .push_back((cursor, subscribed.credit.consumed()));
        Ok(())
    }

    /// Consume the subscription with its terminal outcome. It spends no
    /// credit and ends a fenced `Open`'s exchange.
    pub(crate) fn claim_end(&mut self, key: &K) -> Result<SubscriptionEnded<C>, SubscriptionFault> {
        let mut subscribed = self
            .live
            .remove(key)
            .ok_or_else(|| SubscriptionFault::Unknown.counted(self.limits.message_type))?;
        if let Some(exchange) = &mut subscribed.exchange {
            exchange.end();
        }
        self.ended.push_back(key.clone());
        while self.ended.len() > self.limits.max_live() {
            self.ended.pop_front();
        }
        Ok(SubscriptionEnded {
            pages: subscribed.pages,
            close_sent: subscribed.close_sent,
            received: subscribed.received,
        })
    }

    /// The cursor of the last page received from `key`, or its start. The
    /// reactor checks a page's linkage against it.
    pub(crate) fn received(&self, key: &K) -> Option<&C> {
        self.live.get(key).map(|subscribed| &subscribed.received)
    }

    /// Live subscriptions.
    pub(crate) fn len(&self) -> usize {
        self.live.len()
    }

    /// Whether `key` is live.
    pub(crate) fn contains(&self, key: &K) -> bool {
        self.live.contains_key(key)
    }

    /// The credit `key` granted and consumed so far.
    pub(crate) fn credit(&self, key: &K) -> Option<&ResponseCredit> {
        self.live.get(key).map(|subscribed| &subscribed.credit)
    }
}

/// A session's subscriptions, shared between the reactor and its readers.
#[derive(Debug)]
pub(crate) struct SharedSubscriptions<K, C>(Arc<Mutex<Subscriptions<K, C>>>);

impl<K, C> Clone for SharedSubscriptions<K, C> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<K, C> SharedSubscriptions<K, C> {
    pub(crate) fn new(subscriptions: Subscriptions<K, C>) -> Self {
        Self(Arc::new(Mutex::new(subscriptions)))
    }

    /// Lock the table. No holder panics, so the lock is never poisoned.
    pub(crate) fn lock(&self) -> std::sync::MutexGuard<'_, Subscriptions<K, C>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<K, C> ResponsePrecheck for SharedSubscriptions<K, C>
where
    K: Eq + Hash + Clone + std::fmt::Debug + Send,
    C: Clone + Eq + std::fmt::Debug + Send,
{
    fn check(&self, message_type: u16, payload_len: usize) -> Result<(), FrameRejection> {
        let subscriptions = self.lock();
        subscriptions
            .precheck(message_type, payload_len)
            .map_err(|fault| match fault {
                SubscriptionFault::OverCredit => FrameRejection::AboveReservation {
                    // The largest unspent byte credit is at most the row's.
                    bytes: u64::from(subscriptions.limits.credit.bytes),
                },
                _ => FrameRejection::Unsolicited,
            })
    }
}
