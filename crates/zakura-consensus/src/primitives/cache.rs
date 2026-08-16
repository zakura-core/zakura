//! A bounded cache of shielded bundle verifications that have already succeeded.
//!
//! Zakura verifies a transaction's shielded proofs and signatures when the transaction arrives
//! over mempool gossip, and again when it arrives in a block. This service skips the second
//! verification. The Halo2 Orchard and Ironwood verifiers ([`super::halo2`]) and the Sapling
//! verifier ([`super::sapling`]) share it, and key their entries the same way.
//!
//! # Why this is not the mempool bypass
//!
//! Zebra once skipped whole-transaction verification for transactions already in the mempool, and
//! removed it as a security fix (PR #10494). Transaction validity depends on height, block time
//! and spent outputs, and that cache's key named none of them.
//!
//! This cache remembers successful bundle verification by transaction ID, sighash and shielded
//! pool. A hit still runs the whole transaction verifier against the block's height, time and
//! spent outputs, and skips only the proof and signature checks. Each Orchard circuit era has its
//! own cache, which binds an entry to its verifying key (see [`super::halo2::verifier_for`]);
//! Sapling has one verifying key pair for all of history, so one cache covers it.
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
use zakura_chain::transaction::UnminedTxId;

use crate::BoxError;

/// The number of verified-bundle keys retained per cache.
///
/// Sized to hold several blocks of history plus a full mempool, so that a transaction gossiped
/// well before the block that mines it is still remembered. A transaction ID, sighash and pool
/// tag use about 2 MB per cache before collection overhead.
pub(super) const CACHE_CAPACITY: usize = 20_000;

/// The label naming which verifier's cache a metric belongs to.
///
/// One cache instance per Orchard circuit era and one for Sapling all report under the same
/// metric names, so every series carries this label. The values are the names those verifiers
/// already use in their batch metrics and flush logs, so the two can be joined.
const VERIFIER_LABEL: &str = "verifier";

/// Counts verifications answered from a cache.
const CACHE_HIT: &str = "zakura.consensus.cache.hit";

/// Counts verifications that reached a cache's inner service.
const CACHE_MISS: &str = "zakura.consensus.cache.miss";

/// Counts keys recorded as verified.
const CACHE_INSERT: &str = "zakura.consensus.cache.insert";

/// Counts keys dropped to stay within a cache's capacity.
const CACHE_EVICT: &str = "zakura.consensus.cache.evict";

/// Reports how many keys a cache currently remembers.
const CACHE_SIZE: &str = "zakura.consensus.cache.size";

/// The shielded bundle slot a cache entry was verified for.
///
/// One v6 transaction has an Orchard bundle, an Ironwood bundle and a Sapling bundle, all under
/// one transaction ID and one sighash, so the key names which one it stands for. The Orchard and
/// Ironwood caches at NU6.3 onward are the same cache, so this tag is what keeps their entries
/// apart; Sapling has its own cache and its tag is defence in depth.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) enum ShieldedPool {
    /// The Sapling value pool.
    Sapling,

    /// The Orchard value pool.
    Orchard,

    /// The Ironwood value pool.
    Ironwood,
}

impl From<orchard::ValuePool> for ShieldedPool {
    fn from(pool: orchard::ValuePool) -> Self {
        match pool {
            orchard::ValuePool::Orchard => Self::Orchard,
            orchard::ValuePool::Ironwood => Self::Ironwood,
        }
    }
}

/// A transaction, sighash and shielded pool whose bundle has verified.
///
/// # Correctness
///
/// A hit replaces a verification, so the key must determine every input that verification reads:
/// the bundle and the sighash.
///
/// The transaction ID determines the bundle, in both of the forms it takes:
///
///   * [`UnminedTxId::Witnessed`] carries a [`WtxId`](zakura_chain::transaction::WtxId), whose
///     txid commits to the transaction's effecting data and whose ZIP 244 authorizing-data digest
///     commits to its proofs and signatures. The txid alone would not: it excludes authorizing
///     data, which is what CVE-2026-34377 exploited.
///   * [`UnminedTxId::Legacy`] is a v1-v4 transaction ID, the hash of the whole serialized
///     transaction, so it commits to the Sapling proofs and signatures directly. V4 transactions
///     have no witnessed ID, and this is why they do not need one here.
///
/// The sighash is named separately because it is not a function of the transaction alone: the
/// amounts and `scriptPubKey`s of the spent transparent outputs enter it, and those come from the
/// verification context.
///
/// The verifying key is absent on purpose. Each Orchard circuit era has its own cache, so an
/// entry is only ever read back under the key it was written against, and Sapling has one key
/// pair for all of history.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct CacheKey {
    /// The ID of the transaction the bundle was parsed from.
    tx_id: UnminedTxId,

    /// The signature digest used to verify the bundle's signatures.
    sighash: [u8; 32],

    /// The bundle slot verified for this transaction.
    pool: ShieldedPool,
}

impl CacheKey {
    /// Returns the key for `pool`'s bundle in the transaction identified by `tx_id`, verified
    /// against `sighash`.
    pub(super) fn new(tx_id: UnminedTxId, sighash: [u8; 32], pool: ShieldedPool) -> Self {
        Self {
            tx_id,
            sighash,
            pool,
        }
    }
}

/// An item whose successful verification can be remembered.
///
/// Items without a key are verified normally and never cached, which is how a caller that has no
/// transaction identity to offer stays correct.
pub(super) trait CachedItem {
    /// Returns this item's cache key, if it has one.
    fn cache_key(&self) -> Option<CacheKey>;
}

#[cfg(test)]
mod tests;

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

    /// The `verifier` label this cache reports its metrics under.
    verifier: &'static str,
}

impl VerifiedProofs {
    /// Creates an empty cache that retains at most `capacity` keys.
    fn new(capacity: usize, verifier: &'static str) -> Self {
        Self {
            keys: HashSet::with_capacity(capacity),
            insertion_order: VecDeque::with_capacity(capacity),
            capacity,
            verifier,
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
        metrics::counter!(CACHE_INSERT, VERIFIER_LABEL => self.verifier).increment(1);

        while self.insertion_order.len() > self.capacity {
            let evicted = self
                .insertion_order
                .pop_front()
                .expect("queue is longer than the capacity, which is at least one");
            self.keys.remove(&evicted);
            metrics::counter!(CACHE_EVICT, VERIFIER_LABEL => self.verifier).increment(1);
        }

        // Cast is safe: the length is bounded by `capacity`, far below f64's exact integer range.
        metrics::gauge!(CACHE_SIZE, VERIFIER_LABEL => self.verifier).set(self.keys.len() as f64);
    }
}

/// A service that skips inner verification for items whose bundle has already verified.
///
/// This wraps one verifier's batch-and-fallback stack. The cache is shared between clones, so
/// every handle to a global verifier sees the same set of verified bundles.
///
/// This type is public only because it appears in existing public verifier signatures. The
/// private `cache` module is not re-exported, and its constructor and accessors are private.
pub struct Cached<S> {
    /// The verification service to consult on a miss.
    inner: S,

    /// The keys of items that have already verified under this cache's verifying key.
    verified: Arc<Mutex<VerifiedProofs>>,

    /// The `verifier` label this cache reports its metrics under.
    verifier: &'static str,

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
            verifier: self.verifier,
            #[cfg(test)]
            inner_calls: self.inner_calls.clone(),
        }
    }
}

impl<S> Cached<S> {
    /// Wraps `inner` in a cache that retains at most `capacity` verified-bundle keys and reports
    /// its metrics under the `verifier` label.
    pub(super) fn new(inner: S, capacity: usize, verifier: &'static str) -> Self {
        Self {
            inner,
            verified: Arc::new(Mutex::new(VerifiedProofs::new(capacity, verifier))),
            verifier,
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
    pub(super) fn inner_calls_for<I: CachedItem>(&self, item: &I) -> usize {
        let Some(key) = item.cache_key() else {
            return 0;
        };

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
            verifier: self.verifier,
            inner_calls: self.inner_calls.clone(),
        }
    }
}

impl<S, I> Service<I> for Cached<S>
where
    // `Send + 'static` because a miss moves the item into the boxed future that awaits inner
    // readiness — see `poll_ready`.
    I: CachedItem + Send + 'static,
    S: Service<I, Response = (), Error = BoxError> + Clone + Send + 'static,
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

    fn call(&mut self, item: I) -> Self::Future {
        // Copied once here, outside `Fallback`, which clones every request eagerly.
        let key = item.cache_key();

        if let Some(key) = key {
            if self
                .verified
                .lock()
                .expect("verified proof cache mutex should not be poisoned")
                .contains(&key)
            {
                metrics::counter!(CACHE_HIT, VERIFIER_LABEL => self.verifier).increment(1);
                return future::ready(Ok(())).boxed();
            }
        }

        metrics::counter!(CACHE_MISS, VERIFIER_LABEL => self.verifier).increment(1);

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
                    if let Some(key) = key {
                        inner_calls
                            .lock()
                            .expect("inner call record mutex should not be poisoned")
                            .push(key);
                    }

                    inner.call(item).await
                }
                Err(error) => Err(error),
            };

            // Only successes are recorded: see the module docs.
            if let (Ok(()), Some(key)) = (&result, key) {
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
