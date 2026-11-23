//! Serving's two-level pool: the node-wide [`MemoryPool`] (`Consumer::Serving`)
//! and the per-peer [`super::peer_pool::PeerSubPool`] (`02 §4–5`, `#1888`).
//!
//! Serving output leases (`Serve`, `Push`) and per-connection send-window growth
//! leases (`ConnectionCredit`) both draw from one node-wide [`ServingPool`].
//! Every serving peer has equal weight, a floor of one largest response, and a
//! dynamic per-peer cap (`max(largest_response, 2 × cwnd)` plus send window,
//! bounded by `[memory.cap] serving_per_peer`).
//!
//! The sub-pool lives inside the [`MemoryPool`] lock, so both levels decide as
//! one. The sub-pool keys peers by their authenticated [`ZakuraPeerId`], so a
//! reconnecting peer's retiring and replacement sessions share one part.

use super::{pool::ServingTarget, LeaseDenied, MemoryPool, PoolLease};
use crate::zakura::{ZakuraConnId, ZakuraPeerId};

/// Serving's two-level pool, funding `Consumer::Serving` output and
/// send-window leases per peer (`02 §4–5`).
#[derive(Clone, Debug)]
pub struct ServingPool {
    pool: MemoryPool,
    default_per_peer_cap: u64,
}

impl ServingPool {
    /// Create a serving pool backed by `pool` with `default_per_peer_cap`.
    pub fn new(pool: MemoryPool, default_per_peer_cap: u64) -> Self {
        Self {
            pool,
            default_per_peer_cap: default_per_peer_cap.max(1),
        }
    }

    /// Effective per-peer cap configured by the startup plan (`[memory.cap] serving_per_peer`).
    pub(crate) fn default_per_peer_cap(&self) -> u64 {
        self.default_per_peer_cap
    }

    /// Register a live connection of `peer` with the floor `floor` and the
    /// starting cap `cap`. Dropping the returned guard unregisters the
    /// connection; the peer leaves once its leases drain.
    pub(crate) fn register_connection(
        &self,
        peer: &ZakuraPeerId,
        floor: u64,
        cap: u64,
    ) -> ServingPeerRegistration {
        self.pool
            .register_serving_peer(peer, floor, cap.min(self.default_per_peer_cap));
        ServingPeerRegistration {
            pool: self.pool.clone(),
            peer: peer.clone(),
        }
    }

    /// Set `peer`'s dynamic cap, bounded by the plan's per-peer cap.
    pub(crate) fn set_peer_cap(&self, peer: &ZakuraPeerId, dynamic_cap: u64) {
        self.pool
            .set_serving_peer_cap(peer, dynamic_cap.min(self.default_per_peer_cap));
    }

    /// Try to lease `bytes` for `peer` from both levels without waiting. A
    /// refusal marks nothing waiting.
    pub(crate) fn try_acquire(
        &self,
        peer: &ZakuraPeerId,
        conn_id: Option<ZakuraConnId>,
        peer_floor: u64,
        bytes: u64,
    ) -> Result<ServingLease, LeaseDenied> {
        self.pool
            .try_acquire_serving(self.target(peer, peer_floor), conn_id, bytes)
            .map(ServingLease)
    }

    /// Wait until `bytes` can be leased for `peer` from both levels, without
    /// holding any lock across an await (`02 §5`). Refuses at once when
    /// waiting can never help, such as `bytes` above the peer's cap.
    pub(crate) async fn acquire(
        &self,
        peer: &ZakuraPeerId,
        conn_id: Option<ZakuraConnId>,
        peer_floor: u64,
        bytes: u64,
    ) -> Result<ServingLease, LeaseDenied> {
        self.pool
            .acquire_serving(self.target(peer, peer_floor), conn_id, bytes)
            .await
            .map(ServingLease)
    }

    /// Bytes `peer` holds in the per-peer sub-pool.
    #[cfg(test)]
    pub(crate) fn peer_leased(&self, peer: &ZakuraPeerId) -> u64 {
        self.pool.serving_peer_leased(peer)
    }

    /// Bytes every peer holds in the per-peer sub-pool.
    #[cfg(test)]
    pub(crate) fn total_leased(&self) -> u64 {
        self.pool.serving_total_leased()
    }

    fn target<'a>(&self, peer: &'a ZakuraPeerId, floor: u64) -> ServingTarget<'a> {
        ServingTarget {
            peer,
            floor,
            cap: self.default_per_peer_cap,
        }
    }
}

/// Keeps a connected peer registered in [`ServingPool`] until disconnect.
#[derive(Debug)]
pub(crate) struct ServingPeerRegistration {
    pool: MemoryPool,
    peer: ZakuraPeerId,
}

impl Drop for ServingPeerRegistration {
    fn drop(&mut self) {
        self.pool.unregister_serving_peer(&self.peer);
    }
}

/// A `Consumer::Serving` lease charged to one peer's part of serving.
#[derive(Debug)]
#[must_use = "dropping a ServingLease releases its bytes back to the serving pool"]
pub(crate) struct ServingLease(PoolLease);

impl ServingLease {
    /// Bytes currently held by this lease.
    #[inline]
    pub(crate) fn bytes(&self) -> u64 {
        self.0.bytes()
    }

    /// Shrink this lease down to `new_bytes`, returning the surplus to both
    /// levels at once (`decision-11`).
    pub(crate) fn shrink_to(&mut self, new_bytes: u64) {
        self.0.shrink_to(new_bytes);
    }

    /// Split `bytes` off this lease into a new child lease of the same peer
    /// without changing either level's usage.
    pub(crate) fn split(&mut self, bytes: u64) -> Option<Self> {
        self.0.split(bytes).map(Self)
    }

    /// Merge `other` (for the same peer) into `self` without releasing or
    /// reacquiring bytes.
    pub(crate) fn merge(&mut self, other: Self) {
        self.0.merge(other.0);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::zakura::memory::{Consumer, ConsumerSpec, PoolConfig};

    const MIB: u64 = 1024 * 1024;

    fn peer(byte: u8) -> ZakuraPeerId {
        ZakuraPeerId::new(vec![byte; 32]).expect("32 bytes is a valid peer id")
    }

    fn serving_waiting(pool: &MemoryPool) -> bool {
        pool.snapshot().consumers[Consumer::Serving.index()].waiting
    }

    #[test]
    fn serving_pool_caps_stalled_reader_and_shrinks_cleanly() {
        // Parent pool: Serving floor = 2 MiB, S = 18 MiB -> Serving ceiling = 20 MiB.
        let pool_cfg = PoolConfig::new(20 * MIB, 0)
            .with_consumer(Consumer::Serving, ConsumerSpec::bulk(2 * MIB, 40 * MIB, 1));
        let mem_pool = MemoryPool::new(pool_cfg);
        let serving = ServingPool::new(mem_pool.clone(), 16 * MIB);
        let (p1, p2) = (peer(1), peer(2));

        let _reg1 = serving.register_connection(&p1, 2 * MIB, 16 * MIB);
        let _reg2 = serving.register_connection(&p2, 2 * MIB, 16 * MIB);

        // Peer 1 (stalled reader) borrows 14 MiB while peer 2 is idle; peer 2's
        // unused floor is lent too.
        let mut stalled = serving
            .try_acquire(&p1, Some(1), 2 * MIB, 14 * MIB)
            .expect("peer 1 can borrow when peer 2 is idle");
        assert_eq!(serving.peer_leased(&p1), 14 * MIB);
        assert_eq!(mem_pool.consumer_leased(Consumer::Serving), 14 * MIB);

        // Peer 2 wants 8 MiB (within its part) but only 6 MiB remain. A plain
        // try marks nothing waiting.
        assert_eq!(
            serving
                .try_acquire(&p2, Some(2), 2 * MIB, 8 * MIB)
                .unwrap_err(),
            LeaseDenied::PoolFull
        );
        assert!(!serving_waiting(&mem_pool));

        // Shrinking peer 1's lease frees bytes on both levels.
        stalled.shrink_to(2 * MIB);
        assert_eq!(serving.peer_leased(&p1), 2 * MIB);
        assert_eq!(mem_pool.consumer_leased(Consumer::Serving), 2 * MIB);

        let peer2 = serving
            .try_acquire(&p2, Some(2), 2 * MIB, 8 * MIB)
            .expect("peer 2 fits after the shrink");
        assert_eq!(serving.total_leased(), 10 * MIB);

        drop(stalled);
        drop(peer2);
        assert_eq!(serving.total_leased(), 0);
        assert_eq!(mem_pool.consumer_leased(Consumer::Serving), 0);
    }

    #[tokio::test]
    async fn waiting_peer_below_its_part_holds_back_a_peer_above_its_part() {
        let pool_cfg = PoolConfig::new(20 * MIB, 0)
            .with_consumer(Consumer::Serving, ConsumerSpec::bulk(2 * MIB, 40 * MIB, 1));
        let mem_pool = MemoryPool::new(pool_cfg);
        let serving = ServingPool::new(mem_pool.clone(), 16 * MIB);
        let (p1, p2) = (peer(1), peer(2));
        let _reg1 = serving.register_connection(&p1, 2 * MIB, 16 * MIB);
        let _reg2 = serving.register_connection(&p2, 2 * MIB, 16 * MIB);

        let mut stalled = serving
            .try_acquire(&p1, Some(1), 2 * MIB, 14 * MIB)
            .expect("peer 1 can borrow when peer 2 is idle");

        let waiter = {
            let serving = serving.clone();
            let p2 = p2.clone();
            tokio::spawn(async move { serving.acquire(&p2, Some(2), 2 * MIB, 8 * MIB).await })
        };
        while !serving_waiting(&mem_pool) {
            tokio::task::yield_now().await;
        }

        // Peer 1 is above its part while peer 2 waits below its part (#1888).
        assert_eq!(
            serving.try_acquire(&p1, Some(1), 2 * MIB, MIB).unwrap_err(),
            LeaseDenied::BelowShareWaiting
        );

        stalled.shrink_to(2 * MIB);
        let lease = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("the shrink wakes peer 2")
            .expect("the waiter task doesn't panic")
            .expect("peer 2 gets its lease");
        assert_eq!(lease.bytes(), 8 * MIB);
        assert!(!serving_waiting(&mem_pool));
    }

    #[tokio::test]
    async fn serving_below_share_waiter_holds_back_block_sync_above_its_share() {
        // Equal weights: S = 16 MiB -> each gets 8 MiB share above 2 MiB floor (10 MiB protected).
        let pool_cfg = PoolConfig::new(20 * MIB, 0)
            .with_consumer(
                Consumer::BlockSync,
                ConsumerSpec::bulk(2 * MIB, 20 * MIB, 1),
            )
            .with_consumer(Consumer::Serving, ConsumerSpec::bulk(2 * MIB, 20 * MIB, 1));
        let mem_pool = MemoryPool::new(pool_cfg);
        let serving = ServingPool::new(mem_pool.clone(), 16 * MIB);
        let (p1, p2) = (peer(1), peer(2));
        let _reg1 = serving.register_connection(&p1, 2 * MIB, 16 * MIB);
        let _reg2 = serving.register_connection(&p2, 2 * MIB, 16 * MIB);

        // BlockSync borrows all of S (18 MiB = 2 MiB floor + 16 MiB S).
        let bs = mem_pool
            .try_acquire(Consumer::BlockSync, Some(10), 18 * MIB)
            .expect("block sync borrows S");
        let s1 = serving
            .try_acquire(&p1, Some(1), 2 * MIB, 2 * MIB)
            .expect("serving's floor fits");

        let waiter = {
            let serving = serving.clone();
            let p2 = p2.clone();
            tokio::spawn(async move { serving.acquire(&p2, Some(2), 2 * MIB, 2 * MIB).await })
        };
        while !serving_waiting(&mem_pool) {
            tokio::task::yield_now().await;
        }

        // BlockSync, above its share, can't borrow more while Serving waits.
        assert_eq!(
            mem_pool
                .try_acquire(Consumer::BlockSync, Some(10), MIB)
                .unwrap_err(),
            LeaseDenied::BelowShareWaiting
        );

        drop(bs);
        let s2 = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("BlockSync's release wakes serving")
            .expect("the waiter task doesn't panic")
            .expect("serving peer 2 gets its lease");
        drop(s1);
        drop(s2);
        assert_eq!(mem_pool.consumer_leased(Consumer::Serving), 0);
        assert_eq!(serving.total_leased(), 0);
    }

    #[tokio::test]
    async fn request_above_the_peer_cap_is_refused_at_once() {
        let pool_cfg = PoolConfig::new(20 * MIB, 0)
            .with_consumer(Consumer::Serving, ConsumerSpec::bulk(2 * MIB, 20 * MIB, 1));
        let serving = ServingPool::new(MemoryPool::new(pool_cfg), 4 * MIB);
        let p1 = peer(1);
        let _reg = serving.register_connection(&p1, MIB, 4 * MIB);

        let refused = serving.acquire(&p1, Some(1), MIB, 5 * MIB).await;
        assert_eq!(refused.unwrap_err(), LeaseDenied::CapExceeded);
        assert_eq!(serving.total_leased(), 0);
    }
}
