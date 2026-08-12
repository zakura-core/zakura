//! A bounded cache of Halo2 Orchard Action proofs that have already verified.
//!
//! Zakura verifies an Orchard proof when its transaction arrives over mempool gossip, and again
//! when the transaction arrives in a block. This service skips the second verification.
//!
//! # Why this is not the mempool bypass
//!
//! Zebra once skipped whole-transaction verification for transactions already in the mempool, and
//! removed it as a security fix (PR #10494). Transaction validity depends on height, block time
//! and spent outputs, and that cache's key named none of them.
//!
//! This cache holds `verify(bundle, sighash, vk)`, a pure function of its key. A hit still runs
//! the whole transaction verifier against the block's height, time and spent outputs, and skips
//! only the proof check. [`Item::cache_key`] commits to `bundle` and `sighash`; each Orchard
//! circuit era has its own cache, which commits to `vk` (see [`super::verifier_for`]).
//!
//! Only `Ok` results are cached. A batch error is not per-item evidence, because
//! [`Fallback`](tower_fallback::Fallback) re-verifies failures singly, and it may not be a verdict
//! at all — a shut-down batch worker reports the same way. Caching it would reject a valid block.

use std::{
    collections::{HashSet, VecDeque},
    future,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use futures::{future::BoxFuture, FutureExt};
use tower::{Service, ServiceExt};

use crate::BoxError;

use super::{CacheKey, Item};

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
///
/// This type is public only because it appears in existing public verifier signatures. The
/// private `cache` module does not re-export it, and its constructor and accessors are private.
pub struct Cached<S> {
    /// The verification service to consult on a miss.
    inner: S,

    /// The keys of items that have already verified under this era's key.
    verified: Arc<Mutex<VerifiedProofs>>,

    /// The keys of the items that reached the inner service, in call order.
    ///
    /// Test-only. See [`Self::inner_calls_for`].
    #[cfg(test)]
    inner_calls: Arc<Mutex<Vec<CacheKey>>>,
}

impl<S: Clone> Clone for Cached<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            verified: self.verified.clone(),
            #[cfg(test)]
            inner_calls: self.inner_calls.clone(),
        }
    }
}

impl<S> Cached<S> {
    /// Wraps `inner` in a cache that retains at most `capacity` verified-proof keys.
    pub(super) fn new(inner: S, capacity: usize) -> Self {
        Self {
            inner,
            verified: Arc::new(Mutex::new(VerifiedProofs::new(capacity))),
            #[cfg(test)]
            inner_calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Returns the wrapped verification service.
    pub(super) fn inner(&self) -> &S {
        &self.inner
    }

    /// Returns how many times an item equivalent to `item` has reached the inner service.
    ///
    /// Test-only. It counts one item rather than all calls because the global verifiers are shared
    /// by every test in the process, so a plain call counter would also see verifications a test
    /// did not make. Counting one transaction's own item isolates a test from the rest.
    ///
    /// Readiness failures are not counted: an item that never reached the inner service was never
    /// verified by it.
    #[cfg(test)]
    pub(super) fn inner_calls_for(&self, item: &Item) -> usize {
        let key = item.cache_key();

        self.inner_calls
            .lock()
            .expect("inner call record mutex should not be poisoned")
            .iter()
            .filter(|called| **called == key)
            .count()
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
            inner_calls: self.inner_calls.clone(),
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
    /// This does not delegate to the inner service, because the item is not known yet, so neither
    /// is whether the inner service will be used at all. Delegating would reserve inner capacity
    /// for every request, including the hits that never spend it:
    ///
    ///   * [`Batch::poll_ready`](tower_batch_control::Batch) holds a semaphore permit until
    ///     `Batch::call` consumes it. A hit never calls, so it holds that permit until the handle
    ///     drops, denying capacity to a genuine miss.
    ///   * `Batch::poll_ready` also errors once its worker exits. Callers poll before they call,
    ///     so that error would surface for an item this cache already holds, reporting a verified
    ///     proof as a verification failure.
    ///
    /// [`Self::call`] awaits readiness on the miss path instead. The semaphore still bounds
    /// concurrent batch requests; only the timing of the wait changes.
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

        #[cfg(test)]
        let inner_calls = self.inner_calls.clone();

        async move {
            // Readiness is acquired here rather than in `poll_ready` so that only misses reserve
            // inner capacity. See `poll_ready`.
            let result = match inner.ready().await {
                Ok(inner) => {
                    #[cfg(test)]
                    inner_calls
                        .lock()
                        .expect("inner call record mutex should not be poisoned")
                        .push(key);

                    inner.call(item).await
                }
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
