//! Serving's per-peer sub-pool (`02 §5`, `#1888`).
//!
//! Serving applies the pool rule one level down. Each serving peer has equal
//! weight, a floor of one largest response, and a per-peer cap. A floor
//! protects a peer's usage up to the floor; an idle peer's unused floor stays
//! available to the other peers, as cgroup v2's `memory.min` does. While a peer
//! below its part of serving waits, no peer at or above its part gets a new
//! lease, so a stalled reader holds at most its fair part.
//!
//! The sub-pool checks only caps and shares. Every serving byte is also a
//! `Consumer::Serving` byte in [`MemoryPoolMachine`], which checks room.
//! [`try_serving_lease`] applies both levels as one decision.

use std::{borrow::Borrow, cmp::Ordering, collections::BTreeMap, sync::Arc};

use super::{
    pool::{check_pool_rule, MemoryPoolMachine, PoolRuleInput, Refusal},
    Consumer, LeaseDenied,
};
use crate::zakura::ZakuraPeerId;

/// A serving peer's authenticated id, shared by the peer's leases without
/// copying the id's bytes.
#[derive(Clone, Debug)]
pub(crate) struct PeerKey(Arc<ZakuraPeerId>);

impl PeerKey {
    /// The peer this key names.
    pub(crate) fn peer(&self) -> &ZakuraPeerId {
        &self.0
    }
}

impl PartialEq for PeerKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_bytes() == other.0.as_bytes()
    }
}

impl Eq for PeerKey {}

impl PartialOrd for PeerKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PeerKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.as_bytes().cmp(other.0.as_bytes())
    }
}

impl Borrow<[u8]> for PeerKey {
    fn borrow(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PeerState {
    floor: u64,
    /// Per-peer cap, `>= floor`.
    cap: u64,
    leased: u64,
    /// Live connections registered for this peer.
    registrations: u32,
    /// Waiters that wait on contention for this peer.
    waiters: u32,
}

impl PeerState {
    /// The part of the floor this peer uses.
    #[inline]
    fn floor_use(&self) -> u64 {
        self.floor.min(self.leased)
    }

    #[inline]
    fn is_below_share(&self, share_of_s: u64) -> bool {
        self.leased < self.floor.saturating_add(share_of_s).min(self.cap)
    }
}

/// Pure sub-pool dividing serving's memory equally across serving peers.
///
/// Lease, release and registration update every sum in O(log peers). Only the
/// rule-3 check walks the peers, and only while some peer waits.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct PeerSubPool {
    peers: BTreeMap<PeerKey, PeerState>,
    /// `Σ min(floor, leased)`: the floor bytes peers use. Only these leave the
    /// shared part (`02 §5`, fix-6).
    floor_usage: u64,
    /// Peers with at least one contention waiter.
    waiting_peers: usize,
}

impl PeerSubPool {
    /// Create an empty serving sub-pool.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Register one live connection of `peer`, raising its floor to `floor`.
    /// A new peer starts with cap `cap`.
    pub(crate) fn register(&mut self, peer: &ZakuraPeerId, floor: u64, cap: u64) {
        self.ensure(peer, floor, cap);
        if let Some(state) = self.peers.get_mut(peer.as_bytes()) {
            state.registrations = state.registrations.saturating_add(1);
        }
    }

    /// Unregister one connection of `peer`. The peer leaves once it has no
    /// registration, lease or waiter.
    pub(crate) fn unregister(&mut self, peer: &ZakuraPeerId) {
        if let Some(state) = self.peers.get_mut(peer.as_bytes()) {
            state.registrations = state.registrations.saturating_sub(1);
        }
        self.remove_if_idle(peer);
    }

    /// Add `peer` if it is missing, and raise its floor to `floor`. A new peer
    /// starts with cap `cap`.
    pub(crate) fn ensure(&mut self, peer: &ZakuraPeerId, floor: u64, cap: u64) {
        if self.peers.contains_key(peer.as_bytes()) {
            self.update(peer, |state| {
                if floor > state.floor {
                    state.floor = floor;
                    state.cap = state.cap.max(floor);
                }
            });
        } else {
            self.peers.insert(
                PeerKey(Arc::new(peer.clone())),
                PeerState {
                    floor,
                    cap: cap.max(floor),
                    leased: 0,
                    registrations: 0,
                    waiters: 0,
                },
            );
        }
    }

    /// Set `peer`'s cap, floored at its floor. Returns whether the cap rose.
    pub(crate) fn set_cap(&mut self, peer: &ZakuraPeerId, cap: u64) -> bool {
        let Some(state) = self.peers.get_mut(peer.as_bytes()) else {
            return false;
        };
        let next = cap.max(state.floor);
        let raised = next > state.cap;
        state.cap = next;
        raised
    }

    /// The key a lease of `peer` keeps, if `peer` is present.
    pub(crate) fn key(&self, peer: &ZakuraPeerId) -> Option<PeerKey> {
        self.peers
            .get_key_value(peer.as_bytes())
            .map(|(key, _)| key.clone())
    }

    /// Bytes leased across every serving peer.
    #[cfg(test)]
    pub(crate) fn total_leased(&self) -> u64 {
        self.peers.values().map(|state| state.leased).sum()
    }

    /// Bytes leased by `peer` (0 if absent).
    pub(crate) fn peer_leased(&self, peer: &ZakuraPeerId) -> u64 {
        self.peers
            .get(peer.as_bytes())
            .map_or(0, |state| state.leased)
    }

    /// Each peer's equal part of the bytes above the used floors, at
    /// `ceiling`.
    pub(crate) fn share_of_s(&self, ceiling: u64) -> u64 {
        let count = u64::try_from(self.peers.len()).unwrap_or(u64::MAX);
        if count == 0 {
            return 0;
        }
        ceiling.saturating_sub(self.floor_usage) / count
    }

    /// Whether `peer` is below both its protected share and its cap.
    pub(crate) fn is_below_share(&self, peer: &ZakuraPeerId, ceiling: u64) -> bool {
        self.peers
            .get(peer.as_bytes())
            .is_some_and(|state| state.is_below_share(self.share_of_s(ceiling)))
    }

    /// Whether a peer other than `peer` waits on contention below its share.
    fn other_below_share_waiting(&self, peer: &ZakuraPeerId, share_of_s: u64) -> bool {
        self.waiting_peers > 0
            && self.peers.iter().any(|(key, state)| {
                state.waiters > 0
                    && key.peer().as_bytes() != peer.as_bytes()
                    && state.is_below_share(share_of_s)
            })
    }

    /// Check caps and shares for `bytes` more on `peer` at `ceiling` without
    /// charging anything. Zero bytes always pass.
    ///
    /// An absent peer is refused: callers [`Self::ensure`] it first.
    pub(crate) fn check(
        &self,
        peer: &ZakuraPeerId,
        bytes: u64,
        ceiling: u64,
    ) -> Result<(), Refusal> {
        if bytes == 0 {
            return Ok(());
        }
        let Some(state) = self.peers.get(peer.as_bytes()) else {
            return Err(Refusal::Final(LeaseDenied::CapExceeded));
        };
        let share_of_s = self.share_of_s(ceiling);
        check_pool_rule(PoolRuleInput {
            leased: state.leased,
            request: bytes,
            floor: state.floor,
            cap: state.cap,
            share_of_s,
            shared_free: None,
            reachable: state.cap,
            can_borrow: true,
            long_lived: false,
            other_below_share_waiting: self.other_below_share_waiting(peer, share_of_s),
        })
        .map(|_| ())
    }

    /// Charge `bytes` to `peer` after [`Self::check`] passed. Returns the key
    /// the lease keeps.
    ///
    /// # Panics
    ///
    /// If `peer` is absent: [`Self::check`] refuses an absent peer.
    pub(crate) fn charge(&mut self, peer: &ZakuraPeerId, bytes: u64) -> PeerKey {
        let key = self
            .peers
            .get_key_value(peer.as_bytes())
            .expect("check refuses an absent peer, so a charged peer is present")
            .0
            .clone();
        self.update(peer, |state| state.leased += bytes);
        key
    }

    /// Release `bytes` leased by `peer`. Returns the bytes released.
    pub(crate) fn release(&mut self, peer: &ZakuraPeerId, bytes: u64) -> u64 {
        let leased = self.peer_leased(peer);
        debug_assert!(
            bytes <= leased,
            "peer sub-pool release underflow for peer {peer:?}: releasing {bytes} > leased {leased}"
        );
        let actual = bytes.min(leased);
        self.update(peer, |state| state.leased -= actual);
        self.remove_if_idle(peer);
        actual
    }

    /// Register one contention waiter for `peer`.
    pub(crate) fn add_waiter(&mut self, peer: &ZakuraPeerId) {
        if let Some(state) = self.peers.get_mut(peer.as_bytes()) {
            if state.waiters == 0 {
                self.waiting_peers += 1;
            }
            state.waiters = state.waiters.saturating_add(1);
        }
    }

    /// Unregister one contention waiter for `peer`.
    pub(crate) fn remove_waiter(&mut self, peer: &ZakuraPeerId) {
        if let Some(state) = self.peers.get_mut(peer.as_bytes()) {
            state.waiters = state.waiters.saturating_sub(1);
            if state.waiters == 0 {
                self.waiting_peers -= 1;
            }
        }
        self.remove_if_idle(peer);
    }

    /// Change `peer`'s state, keeping `floor_usage` in step.
    fn update(&mut self, peer: &ZakuraPeerId, change: impl FnOnce(&mut PeerState)) {
        if let Some(state) = self.peers.get_mut(peer.as_bytes()) {
            self.floor_usage -= state.floor_use();
            change(state);
            self.floor_usage += state.floor_use();
        }
    }

    /// Remove `peer` once it has no registration, lease or waiter.
    pub(crate) fn remove_if_idle(&mut self, peer: &ZakuraPeerId) {
        let idle = self.peers.get(peer.as_bytes()).is_some_and(|state| {
            state.registrations == 0 && state.leased == 0 && state.waiters == 0
        });
        if idle {
            self.peers.remove(peer.as_bytes());
        }
    }
}

/// Serving's live ceiling at the node level: what `Consumer::Serving` may hold
/// now, and never less than its protected share.
pub(crate) fn serving_ceiling(machine: &MemoryPoolMachine) -> u64 {
    machine
        .available_ceiling(Consumer::Serving)
        .max(machine.protected_share(Consumer::Serving))
}

/// Which level refused a serving lease.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Level {
    /// The node rule for `Consumer::Serving`.
    Node,
    /// The peer's cap or share.
    Peer,
}

/// Serving's two-level rule as one decision (`02 §5`): the peer's cap and
/// share, then the node rule for `Consumer::Serving`. Charges both levels or
/// neither. `peer` must be present ([`PeerSubPool::ensure`]).
pub(crate) fn try_serving_lease(
    machine: &mut MemoryPoolMachine,
    peers: &mut PeerSubPool,
    peer: &ZakuraPeerId,
    bytes: u64,
) -> Result<PeerKey, (Refusal, Level)> {
    peers
        .check(peer, bytes, serving_ceiling(machine))
        .map_err(|refusal| (refusal, Level::Peer))?;
    machine
        .try_lease(Consumer::Serving, bytes)
        .map_err(|refusal| (refusal, Level::Node))?;
    Ok(peers.charge(peer, bytes))
}
