//! A bounded cache of Halo2 Orchard Action proofs that have already verified.
//!
//! A transaction's Orchard proof is verified once when the transaction arrives over mempool
//! gossip, and again when it arrives inside a block. This service skips the second verification
//! when it can prove the two are the same computation.
//!
//! # Why this is not the mempool bypass
//!
//! Zebra used to skip *whole-transaction* verification for block transactions already accepted
//! into the mempool. That was removed upstream as a security fix (PR #10494), after two rounds of
//! patching, because the cached proposition was `valid(tx, height, block time, spent outputs)` —
//! not a function of the key it was stored under. A transaction valid at one height is not
//! necessarily valid at another: expiry, lock time, the consensus branch id, the Orchard soft-fork
//! gates and the proof-size rule all move with height, and the network upgrade even selects which
//! Orchard circuit verifying key applies. Zakura keeps the regression tests for both failures
//! (`block_with_garbage_orchard_proofs_is_rejected` and
//! `mempool_cached_result_bypasses_expiry_check_for_block_at_next_height`).
//!
//! What is cached here is `verify(bundle, sighash, vk) -> bool`, which *is* a pure function.
//! On a hit, `transaction::Verifier::call` still runs end to end at the block's height, with the
//! block's time and the block's spent outputs; the only thing that does not re-run is a
//! deterministic computation whose every input is pinned by the key.
//!
//! That makes key completeness the whole of the safety argument:
//!
//!   * `bundle` and `sighash` are committed to by [`Item::cache_key`], which destructures [`Item`]
//!     exhaustively so that adding a field is a compile error until someone decides whether it
//!     belongs in the key;
//!   * `vk` is committed to structurally, by giving each Orchard circuit era its own cache. Eras
//!     cannot mix in a cache for the same reason they cannot mix in a batch — see
//!     [`super::verifier_for`].
//!
//! Only `Ok` results are recorded. A batch error is not per-item evidence, because
//! [`Fallback`](tower_fallback::Fallback) resolves batch failures by re-verifying each item
//! singly; and an error out of the service need not be a verdict at all — it can report that the
//! batch worker shut down. Recording that as "this proof is invalid" would make the node reject a
//! valid block, which is a chain split in the other direction.

use std::{
    collections::{HashSet, VecDeque},
    future,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use futures::{future::BoxFuture, FutureExt};
use tower::{Service, ServiceExt};

use crate::BoxError;

use super::Item;

/// The number of verified-proof keys retained per Orchard circuit era.
///
/// Sized to hold several blocks of history plus a full mempool, so that a transaction gossiped
/// well before the block that mines it is still remembered. At 32-byte keys this is on the order
/// of a megabyte per era.
pub const CACHE_CAPACITY: usize = 20_000;

/// A key that determines every input to one [`Item`]'s proof verification.
///
/// See [`Item::cache_key`] for the construction and for why completeness matters.
pub type CacheKey = [u8; 32];

/// A bounded set of keys for items that have already verified successfully.
///
/// Eviction is first-in-first-out rather than least-recently-used: the working set is the
/// mempool, which turns over in arrival order anyway, and FIFO needs no bookkeeping on the read
/// path. Evicting an entry only costs a re-verification, never correctness.
#[derive(Debug)]
struct VerifiedProofs {
    /// The keys currently remembered.
    keys: HashSet<CacheKey>,

    /// The same keys in insertion order, so the oldest can be evicted.
    insertion_order: VecDeque<CacheKey>,

    /// The maximum number of keys to retain.
    capacity: usize,
}

impl VerifiedProofs {
    /// Creates an empty cache that retains at most `capacity` keys.
    fn new(capacity: usize) -> Self {
        Self {
            keys: HashSet::with_capacity(capacity),
            insertion_order: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    /// Returns `true` if `key` has already verified.
    fn contains(&self, key: &CacheKey) -> bool {
        self.keys.contains(key)
    }

    /// Records that `key` has verified, evicting the oldest keys if that exceeds the capacity.
    fn insert(&mut self, key: CacheKey) {
        // Concurrent verifications of the same item both miss and both insert. The second is a
        // no-op, and must not push a duplicate into the eviction queue.
        if !self.keys.insert(key) {
            return;
        }

        self.insertion_order.push_back(key);
        metrics::counter!("zakura.consensus.halo2.cache.insert").increment(1);

        while self.insertion_order.len() > self.capacity {
            let evicted = self
                .insertion_order
                .pop_front()
                .expect("queue is longer than the capacity, which is at least one");
            self.keys.remove(&evicted);
            metrics::counter!("zakura.consensus.halo2.cache.evict").increment(1);
        }

        // Cast is safe: the length is bounded by `capacity`, far below f64's exact integer range.
        metrics::gauge!("zakura.consensus.halo2.cache.size").set(self.keys.len() as f64);
    }
}

/// A service that skips inner verification for items whose proof has already verified.
///
/// This wraps one Orchard circuit era's batch-and-fallback stack. The cache is shared between
/// clones, so every handle to a global verifier sees the same set of verified proofs.
pub struct Cached<S> {
    /// The verification service to consult on a miss.
    inner: S,

    /// The keys of items that have already verified under this era's key.
    verified: Arc<Mutex<VerifiedProofs>>,
}

impl<S: Clone> Clone for Cached<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            verified: self.verified.clone(),
        }
    }
}

impl<S> Cached<S> {
    /// Wraps `inner` in a cache that retains at most `capacity` verified-proof keys.
    pub(super) fn new(inner: S, capacity: usize) -> Self {
        Self {
            inner,
            verified: Arc::new(Mutex::new(VerifiedProofs::new(capacity))),
        }
    }

    /// Returns the wrapped verification service.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Returns a cache sharing this one's remembered keys, but consulting `inner` on a miss.
    ///
    /// Test-only. It exists so a test can warm the cache through a healthy service and then swap
    /// in a broken one, which is how the hit path is exercised in isolation from the inner
    /// service.
    #[cfg(test)]
    pub(super) fn with_inner<T>(&self, inner: T) -> Cached<T> {
        Cached {
            inner,
            verified: self.verified.clone(),
        }
    }
}

impl<S> Service<Item> for Cached<S>
where
    S: Service<Item, Response = (), Error = BoxError> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = ();
    type Error = BoxError;
    type Future = BoxFuture<'static, Result<(), BoxError>>;

    /// Always ready.
    ///
    /// This deliberately does **not** delegate to the inner service, because at this point the
    /// item is not yet known and so neither is whether the inner service will be used at all.
    /// Delegating would reserve inner capacity for every request, including the hits that never
    /// spend it:
    ///
    ///   * [`Batch::poll_ready`](tower_batch_control::Batch) holds a semaphore permit in the
    ///     service until [`Batch::call`] consumes it. A hit returns without calling the inner
    ///     service, so that permit is only released when the handle is dropped — until then it
    ///     is capacity denied to a genuine miss.
    ///
    ///   * More importantly, `Batch::poll_ready` returns an error when its worker has exited or
    ///     its channel has closed. Callers poll readiness *before* `call`, so that error would
    ///     be returned for an item whose result this cache already holds — reporting a verified
    ///     proof as a verification failure, which is exactly the "an error need not be a verdict"
    ///     case the module docs are about. Returning ready here means a hit is answered from the
    ///     cache whatever state the batch worker is in.
    ///
    /// Readiness is instead awaited inside [`Self::call`], on the miss path, where it belongs.
    /// The semaphore still bounds concurrent batch requests: a miss acquires its permit before
    /// calling, exactly as before. What changes is only *when* the wait happens, which for the
    /// `oneshot` call sites this service has is the same instant either way.
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, item: Item) -> Self::Future {
        // Derived once here, outside `Fallback`, which clones every request eagerly.
        let key = item.cache_key();

        if self
            .verified
            .lock()
            .expect("verified proof cache mutex should not be poisoned")
            .contains(&key)
        {
            metrics::counter!("zakura.consensus.halo2.cache.hit").increment(1);
            return future::ready(Ok(())).boxed();
        }

        metrics::counter!("zakura.consensus.halo2.cache.miss").increment(1);

        let verified = self.verified.clone();
        let mut inner = self.inner.clone();

        async move {
            // Readiness is acquired here rather than in `poll_ready` so that only misses reserve
            // inner capacity. See `poll_ready`.
            let result = match inner.ready().await {
                Ok(inner) => inner.call(item).await,
                Err(error) => Err(error),
            };

            // Only successes are recorded: see the module docs.
            if result.is_ok() {
                verified
                    .lock()
                    .expect("verified proof cache mutex should not be poisoned")
                    .insert(key);
            }

            result
        }
        .boxed()
    }
}
