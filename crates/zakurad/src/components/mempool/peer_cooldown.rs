//! The cooldown table today's pool shares with its download tasks.
//!
//! The rules are in [`zakura_mempool::peer_cooldown`]. This wraps a table keyed
//! by canonical IP address in a lock, so the mempool and its download tasks see
//! the same cooldowns.

use std::{
    net::IpAddr,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use zakura_mempool::peer_cooldown::CooldownTable;

pub use zakura_mempool::peer_cooldown::BASE_COOLDOWN;

#[cfg(test)]
mod tests;

/// Per-IP transaction cooldowns.
///
/// Clones share the same cooldowns, so the mempool and its download tasks see
/// the same state.
#[derive(Clone, Debug, Default)]
pub struct PeerCooldowns {
    peers: Arc<Mutex<CooldownTable<IpAddr>>>,
}

impl PeerCooldowns {
    /// Returns true if transactions from `ip` should be ignored at `now`.
    pub fn is_cooling_down(&self, ip: IpAddr, now: Instant) -> bool {
        self.peers()
            .is_cooling_down(&ip.to_canonical(), zakura_mempool::Instant::from_std(now))
    }

    /// Records an invalid transaction from `ip` at `now`. See
    /// [`CooldownTable::record_invalid_transaction`].
    pub fn record_invalid_transaction(&self, ip: IpAddr, now: Instant) -> Option<Duration> {
        self.peers()
            .record_invalid_transaction(ip.to_canonical(), zakura_mempool::Instant::from_std(now))
    }

    /// Locks the table.
    fn peers(&self) -> MutexGuard<'_, CooldownTable<IpAddr>> {
        self.peers
            .lock()
            .expect("no code panics while holding the peer cooldowns lock")
    }
}
