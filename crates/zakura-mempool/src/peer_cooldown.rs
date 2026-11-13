//! Temporary cooldowns for peers that relay invalid transactions.
//!
//! [`CooldownTable`] is plain data, keyed by whatever names a peer, so the pool
//! core can own one. Today's pool wraps a table keyed by IP address in a lock
//! its download tasks share.
//!
//! A transaction can fail verification because of this node's own view of the
//! chain: its tip can lag the relaying peer's tip, or sit on the other side of a
//! network upgrade activation. The failure is then not evidence that the peer
//! misbehaved. So the mempool ignores that peer's transactions for a while.
//! A failure that does not depend on the tip bans the peer instead. Lock time
//! and coinbase maturity failures only depend on the tip, so they start no
//! cooldown.
//!
//! A cooldown still bounds the verification work a malicious peer can cause.
//! During a cooldown, the mempool ignores the peer's advertisements, and the
//! download tasks do not verify transactions the peer serves, including
//! transactions the crawler requested. Only the peer's transactions that were
//! already in verification when the cooldown started still get verified. That
//! is up to `MAX_INBOUND_CONCURRENCY_PER_PEER` advertised or pushed
//! transactions, or up to `MAX_INBOUND_CONCURRENCY` transactions the crawler
//! requested. Repeated failures double the cooldown and disconnect the peer.

use std::{collections::BTreeSet, hash::Hash, time::Duration};

use zakura_rt::collections::HashMap;

use crate::Instant;

#[cfg(test)]
mod tests;

/// The cooldown after a peer's first invalid transaction.
pub const BASE_COOLDOWN: Duration = Duration::from_secs(10 * 60);

/// The longest cooldown.
///
/// Each invalid transaction that starts a new cooldown doubles the previous
/// cooldown, up to this limit. A peer's history is forgotten once it has gone
/// this long after its last cooldown ended.
pub const MAX_COOLDOWN: Duration = Duration::from_secs(2 * 60 * 60);

/// The maximum number of peers with a cooldown history.
///
/// Matches the network's `MAX_BANNED_IPS` bound.
pub const MAX_COOLDOWN_PEERS: usize = 20_000;

/// The cooldown history of each peer, keyed by `K`.
#[derive(Clone, Debug)]
pub struct CooldownTable<K> {
    peers: HashMap<K, Cooldown>,
    expirations: BTreeSet<(Instant, K)>,
}

impl<K> Default for CooldownTable<K> {
    fn default() -> Self {
        Self {
            peers: HashMap::default(),
            expirations: BTreeSet::new(),
        }
    }
}

/// One peer's cooldown history.
#[derive(Copy, Clone, Debug)]
struct Cooldown {
    /// The time the current or most recent cooldown ends.
    until: Instant,

    /// The number of cooldowns started since the history was last forgotten.
    strikes: u32,
}

impl Cooldown {
    /// Returns true if the history is old enough to forget at `now`.
    fn is_forgotten(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.until) >= MAX_COOLDOWN
    }
}

impl<K: Clone + Eq + Hash + Ord> CooldownTable<K> {
    /// Returns true if transactions from `peer` should be ignored at `now`.
    pub fn is_cooling_down(&self, peer: &K, now: Instant) -> bool {
        self.peers
            .get(peer)
            .is_some_and(|cooldown| now < cooldown.until)
    }

    /// The time `peer`'s current cooldown ends, if it is cooling down at `now`.
    pub fn cooling_until(&self, peer: &K, now: Instant) -> Option<Instant> {
        self.peers
            .get(peer)
            .map(|cooldown| cooldown.until)
            .filter(|until| now < *until)
    }

    /// Records an invalid transaction from `peer` at `now`.
    ///
    /// Returns the new cooldown's length, or `None` if `peer` was already
    /// cooling down, or every tracked peer is cooling down and the history is
    /// full.
    pub fn record_invalid_transaction(&mut self, peer: K, now: Instant) -> Option<Duration> {
        if !self.peers.contains_key(&peer) && self.peers.len() >= MAX_COOLDOWN_PEERS {
            let (_until, earliest) = self.expirations.pop_first()?;
            self.peers.remove(&earliest);
        }

        let mut cooldown = self.peers.get(&peer).copied().unwrap_or(Cooldown {
            until: now,
            strikes: 0,
        });

        if now < cooldown.until {
            return None;
        }

        if cooldown.is_forgotten(now) {
            cooldown.strikes = 0;
        }

        self.expirations.remove(&(cooldown.until, peer.clone()));
        cooldown.strikes = cooldown.strikes.saturating_add(1);
        let length = cooldown_length(cooldown.strikes);
        cooldown.until = now + length;
        self.expirations.insert((cooldown.until, peer.clone()));
        self.peers.insert(peer, cooldown);

        Some(length)
    }

    /// The peers that are cooling down at `now`, and when each cooldown ends.
    pub fn cooling(&self, now: Instant) -> impl Iterator<Item = (&K, Instant)> + '_ {
        self.peers
            .iter()
            .filter(move |(_, cooldown)| now < cooldown.until)
            .map(|(peer, cooldown)| (peer, cooldown.until))
    }

    /// The number of peers with a cooldown history.
    pub fn len(&self) -> usize {
        self.peers.len()
    }

    /// Whether no peer has a cooldown history.
    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }
}

/// Returns the cooldown length for a peer's `strikes`-th cooldown.
fn cooldown_length(strikes: u32) -> Duration {
    let doublings = strikes.saturating_sub(1);

    1u32.checked_shl(doublings)
        .and_then(|multiplier| BASE_COOLDOWN.checked_mul(multiplier))
        .map_or(MAX_COOLDOWN, |length| length.min(MAX_COOLDOWN))
}
