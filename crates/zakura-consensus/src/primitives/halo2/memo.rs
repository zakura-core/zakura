//! A bounded memo of Halo2 Orchard Action proofs that have already verified.
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
//! What is memoized here is `verify(bundle, sighash, vk) -> bool`, which *is* a pure function.
//! On a hit, `transaction::Verifier::call` still runs end to end at the block's height, with the
//! block's time and the block's spent outputs; the only thing that does not re-run is a
//! deterministic computation whose every input is pinned by the key.
//!
//! That makes key completeness the whole of the safety argument:
//!
//!   * `bundle` and `sighash` are committed to by [`Item::cache_key`], which destructures [`Item`]
//!     exhaustively so that adding a field is a compile error until someone decides whether it
//!     belongs in the key;
//!   * `vk` is committed to structurally, by giving each Orchard circuit era its own memo. Eras
//!     cannot mix in a memo for the same reason they cannot mix in a batch — see
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
use tower::Service;

use crate::BoxError;

use super::Item;

/// The number of verified-proof keys retained per Orchard circuit era.
///
/// Sized to hold several blocks of history plus a full mempool, so that a transaction gossiped
/// well before the block that mines it is still remembered. At 32-byte keys this is on the order
/// of a megabyte per era.
pub const MEMO_CAPACITY: usize = 20_000;

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
    /// Creates an empty memo that retains at most `capacity` keys.
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
        metrics::counter!("zakura.consensus.halo2.memo.insert").increment(1);

        while self.insertion_order.len() > self.capacity {
            let evicted = self
                .insertion_order
                .pop_front()
                .expect("queue is longer than the capacity, which is at least one");
            self.keys.remove(&evicted);
            metrics::counter!("zakura.consensus.halo2.memo.evict").increment(1);
        }

        // Cast is safe: the length is bounded by `capacity`, far below f64's exact integer range.
        metrics::gauge!("zakura.consensus.halo2.memo.size").set(self.keys.len() as f64);
    }
}

/// A service that skips inner verification for items whose proof has already verified.
///
/// This wraps one Orchard circuit era's batch-and-fallback stack. The memo is shared between
/// clones, so every handle to a global verifier sees the same set of verified proofs.
pub struct Memoized<S> {
    /// The verification service to consult on a miss.
    inner: S,

    /// The keys of items that have already verified under this era's key.
    verified: Arc<Mutex<VerifiedProofs>>,
}

impl<S: Clone> Clone for Memoized<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            verified: self.verified.clone(),
        }
    }
}

impl<S> Memoized<S> {
    /// Wraps `inner` in a memo that retains at most `capacity` verified-proof keys.
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
}

impl<S> Service<Item> for Memoized<S>
where
    S: Service<Item, Response = (), Error = BoxError>,
    S::Future: Send + 'static,
{
    type Response = ();
    type Error = BoxError;
    type Future = BoxFuture<'static, Result<(), BoxError>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, item: Item) -> Self::Future {
        // Derived once here, outside `Fallback`, which clones every request eagerly.
        let key = item.cache_key();

        if self
            .verified
            .lock()
            .expect("verified proof memo mutex should not be poisoned")
            .contains(&key)
        {
            metrics::counter!("zakura.consensus.halo2.memo.hit").increment(1);
            return future::ready(Ok(())).boxed();
        }

        metrics::counter!("zakura.consensus.halo2.memo.miss").increment(1);

        let verified = self.verified.clone();
        let response = self.inner.call(item);

        async move {
            let result = response.await;

            // Only successes are recorded: see the module docs.
            if result.is_ok() {
                verified
                    .lock()
                    .expect("verified proof memo mutex should not be poisoned")
                    .insert(key);
            }

            result
        }
        .boxed()
    }
}
