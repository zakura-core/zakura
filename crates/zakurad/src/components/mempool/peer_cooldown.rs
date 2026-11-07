//! Temporary cooldowns for peers that relay invalid transactions.
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

use std::{
    collections::{BTreeSet, HashMap},
    net::IpAddr,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

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

/// The maximum number of IP addresses with a cooldown history.
///
/// Matches the network's `MAX_BANNED_IPS` bound.
pub const MAX_COOLDOWN_PEERS: usize = 20_000;

/// Per-IP transaction cooldowns.
///
/// Clones share the same cooldowns, so the mempool and its download tasks see
/// the same state.
#[derive(Clone, Debug, Default)]
pub struct PeerCooldowns {
    peers: Arc<Mutex<CooldownState>>,
}

#[derive(Debug, Default)]
struct CooldownState {
    peers: HashMap<IpAddr, Cooldown>,
    expirations: BTreeSet<(Instant, IpAddr)>,
}

/// One IP address's cooldown history.
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

impl PeerCooldowns {
    /// Returns true if transactions from `ip` should be ignored at `now`.
    pub fn is_cooling_down(&self, ip: IpAddr, now: Instant) -> bool {
        self.peers()
            .peers
            .get(&ip.to_canonical())
            .is_some_and(|cooldown| now < cooldown.until)
    }

    /// Records an invalid transaction from `ip` at `now`.
    ///
    /// Returns the length of the cooldown this failure started, or `None` if
    /// `ip` was already cooling down. Failures during a cooldown come from
    /// transactions queued before it started, so they do not add strikes.
    ///
    /// When the history is full, a new IP address replaces the history whose
    /// cooldown ends first, even if that cooldown is still active. Refusing
    /// new addresses instead would let an attacker who fills the history
    /// pause every honest peer it does not hold. An attacker with enough
    /// addresses to evict a chosen entry can already rotate to fresh
    /// addresses, so eviction gives it nothing new.
    pub fn record_invalid_transaction(&self, ip: IpAddr, now: Instant) -> Option<Duration> {
        let ip = ip.to_canonical();
        let mut state = self.peers();

        if !state.peers.contains_key(&ip) && state.peers.len() >= MAX_COOLDOWN_PEERS {
            // Each cooldown has one expiration, so a full history has a first
            // expiration.
            let (_until, earliest) = state.expirations.pop_first()?;
            state.peers.remove(&earliest);
        }

        let mut cooldown = state.peers.get(&ip).copied().unwrap_or(Cooldown {
            until: now,
            strikes: 0,
        });

        if now < cooldown.until {
            return None;
        }

        if cooldown.is_forgotten(now) {
            cooldown.strikes = 0;
        }

        state.expirations.remove(&(cooldown.until, ip));
        cooldown.strikes = cooldown.strikes.saturating_add(1);
        let length = cooldown_length(cooldown.strikes);
        cooldown.until = now + length;
        state.expirations.insert((cooldown.until, ip));
        state.peers.insert(ip, cooldown);

        Some(length)
    }

    /// Returns the number of IP addresses with a cooldown history.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.peers().peers.len()
    }

    /// Locks the cooldowns.
    fn peers(&self) -> MutexGuard<'_, CooldownState> {
        self.peers
            .lock()
            .expect("no code panics while holding the peer cooldowns lock")
    }
}

/// Returns the cooldown length for a peer's `strikes`-th cooldown.
fn cooldown_length(strikes: u32) -> Duration {
    let doublings = strikes.saturating_sub(1);

    1u32.checked_shl(doublings)
        .and_then(|multiplier| BASE_COOLDOWN.checked_mul(multiplier))
        .map_or(MAX_COOLDOWN, |length| length.min(MAX_COOLDOWN))
}
