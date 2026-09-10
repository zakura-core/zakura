//! Temporary cooldowns for peers that relay invalid transactions.
//!
//! A transaction can fail verification because of this node's own view of the
//! chain: its tip can lag the relaying peer's tip, or sit on the other side of a
//! network upgrade activation. The failure is then not evidence that the peer
//! misbehaved. So the mempool never bans a peer for an invalid transaction.
//! Instead, it ignores that peer's transaction advertisements for a while.
//!
//! A cooldown still bounds the verification work a malicious peer can cause: each
//! IP address gets roughly one invalid transaction verified per cooldown, plus the
//! transactions it already had in flight. Repeated failures double the cooldown.

use std::{
    collections::HashMap,
    net::IpAddr,
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
#[derive(Debug, Default)]
pub struct PeerCooldowns {
    peers: HashMap<IpAddr, Cooldown>,
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
    /// Returns true if transaction advertisements from `ip` should be ignored at `now`.
    pub fn is_cooling_down(&self, ip: IpAddr, now: Instant) -> bool {
        self.peers
            .get(&ip.to_canonical())
            .is_some_and(|cooldown| now < cooldown.until)
    }

    /// Records an invalid transaction from `ip` at `now`.
    ///
    /// Returns the length of the cooldown this failure started, or `None` if
    /// `ip` was already cooling down. Failures during a cooldown come from
    /// transactions queued before it started, so they do not add strikes.
    pub fn record_invalid_transaction(&mut self, ip: IpAddr, now: Instant) -> Option<Duration> {
        let ip = ip.to_canonical();

        if !self.peers.contains_key(&ip) && self.peers.len() >= MAX_COOLDOWN_PEERS {
            self.evict(now);
        }

        let cooldown = self.peers.entry(ip).or_insert(Cooldown {
            until: now,
            strikes: 0,
        });

        if now < cooldown.until {
            return None;
        }

        if cooldown.is_forgotten(now) {
            cooldown.strikes = 0;
        }

        cooldown.strikes = cooldown.strikes.saturating_add(1);
        let length = cooldown_length(cooldown.strikes);
        cooldown.until = now + length;

        Some(length)
    }

    /// Makes room for one new IP address.
    ///
    /// Drops forgotten histories first. If the map is still full, drops the
    /// history whose cooldown ended earliest.
    fn evict(&mut self, now: Instant) {
        self.peers
            .retain(|_ip, cooldown| !cooldown.is_forgotten(now));

        if self.peers.len() < MAX_COOLDOWN_PEERS {
            return;
        }

        if let Some(earliest) = self
            .peers
            .iter()
            .min_by_key(|(_ip, cooldown)| cooldown.until)
            .map(|(ip, _cooldown)| *ip)
        {
            self.peers.remove(&earliest);
        }
    }

    /// Returns the number of IP addresses with a cooldown history.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.peers.len()
    }
}

/// Returns the cooldown length for a peer's `strikes`-th cooldown.
fn cooldown_length(strikes: u32) -> Duration {
    let doublings = strikes.saturating_sub(1);

    1u32.checked_shl(doublings)
        .and_then(|multiplier| BASE_COOLDOWN.checked_mul(multiplier))
        .map_or(MAX_COOLDOWN, |length| length.min(MAX_COOLDOWN))
}
