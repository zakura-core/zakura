//! Temporary cooldowns for peers that relay invalid transactions.
//!
//! A transaction can fail verification because of this node's own view of the
//! chain: its tip can lag the relaying peer's tip, or sit on the other side of a
//! network upgrade activation. The failure is then not evidence that the peer
//! misbehaved. So the mempool never bans a peer for an invalid transaction.
//! Instead, it ignores that peer's transactions for a while. Lock time and
//! coinbase maturity failures only depend on the tip, so they start no
//! cooldown.
//!
//! A cooldown still bounds the verification work a malicious peer can cause.
//! During a cooldown, the mempool ignores the peer's advertisements, and the
//! download tasks do not verify transactions the peer serves, including
//! transactions the crawler requested. Only the peer's transactions that were
//! already in verification when the cooldown started still get verified. That
//! is up to `MAX_INBOUND_CONCURRENCY_PER_PEER` advertised or pushed
//! transactions, or up to `MAX_INBOUND_CONCURRENCY` transactions the crawler
//! requested. Repeated failures double the cooldown.

use std::{
    collections::HashMap,
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
    peers: Arc<Mutex<HashMap<IpAddr, Cooldown>>>,
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
            .get(&ip.to_canonical())
            .is_some_and(|cooldown| now < cooldown.until)
    }

    /// Records an invalid transaction from `ip` at `now`.
    ///
    /// Returns the length of the cooldown this failure started, or `None` if
    /// `ip` was already cooling down. Failures during a cooldown come from
    /// transactions queued before it started, so they do not add strikes.
    pub fn record_invalid_transaction(&self, ip: IpAddr, now: Instant) -> Option<Duration> {
        let ip = ip.to_canonical();
        let mut peers = self.peers();

        if !peers.contains_key(&ip) && peers.len() >= MAX_COOLDOWN_PEERS {
            evict(&mut peers, now);
        }

        let cooldown = peers.entry(ip).or_insert(Cooldown {
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

    /// Returns the number of IP addresses with a cooldown history.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.peers().len()
    }

    /// Locks the cooldowns.
    fn peers(&self) -> MutexGuard<'_, HashMap<IpAddr, Cooldown>> {
        self.peers
            .lock()
            .expect("no code panics while holding the peer cooldowns lock")
    }
}

/// Makes room in `peers` for one new IP address.
///
/// Drops forgotten histories first. If the map is still full, drops the
/// history whose cooldown ended earliest.
fn evict(peers: &mut HashMap<IpAddr, Cooldown>, now: Instant) {
    peers.retain(|_ip, cooldown| !cooldown.is_forgotten(now));

    if peers.len() < MAX_COOLDOWN_PEERS {
        return;
    }

    if let Some(earliest) = peers
        .iter()
        .min_by_key(|(_ip, cooldown)| cooldown.until)
        .map(|(ip, _cooldown)| *ip)
    {
        peers.remove(&earliest);
    }
}

/// Returns the cooldown length for a peer's `strikes`-th cooldown.
fn cooldown_length(strikes: u32) -> Duration {
    let doublings = strikes.saturating_sub(1);

    1u32.checked_shl(doublings)
        .and_then(|multiplier| BASE_COOLDOWN.checked_mul(multiplier))
        .map_or(MAX_COOLDOWN, |length| length.min(MAX_COOLDOWN))
}
