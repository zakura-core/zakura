//! Block-level flushes of the shielded batch verifiers, once a block's shielded work is queued.
//!
//! The block verifier registers how many shielded transactions it is about to dispatch. Each
//! shielded transaction holds a [`Queueing`] guard while it builds its checks, and each batch
//! request it builds takes its own guard, which it releases once the request is in its batch
//! queue. When the last guard drops, the batch workers that received items are flushed, so a
//! partial batch starts verifying without waiting for the block's transparent checks or for the
//! batch latency bound.
//!
//! A request waiting for batch queue capacity holds its guard, so it delays the flush instead of
//! arriving after it.

use std::{
    collections::HashMap,
    sync::{Arc, LazyLock, Mutex},
};

use crate::BoxError;

use super::{ed25519, halo2, sapling};

#[cfg(test)]
mod tests;

/// One registered block's progress, shared by its guards.
type Pending = Arc<Mutex<Progress>>;

/// Registered blocks, keyed by the address of an allocation shared by their transaction requests.
static BLOCKS: LazyLock<Mutex<HashMap<usize, Pending>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

tokio::task_local! {
    /// The block progress of the shielded transaction whose checks are being built.
    static CURRENT: Pending;
}

/// The batch workers a block's shielded transactions queue items in.
///
/// Sapling and Halo2 verify their bundles' signatures with their proofs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Verifier {
    Ed25519,
    Sapling,
    Halo2PreNu6_2,
    Halo2Nu6_2,
    Halo2Nu6_3Onward,
}

impl Verifier {
    /// Every verifier, in the order flushes are queued.
    const ALL: [Self; 5] = [
        Self::Ed25519,
        Self::Sapling,
        Self::Halo2PreNu6_2,
        Self::Halo2Nu6_2,
        Self::Halo2Nu6_3Onward,
    ];

    /// Returns this verifier's bit in [`Progress::queued`].
    fn bit(self) -> u8 {
        1 << self as u8
    }

    /// Queues an explicit flush of this verifier's batch worker, returning `Ok(false)` when its
    /// queue is full and so already flushing on size.
    fn try_flush(self) -> Result<bool, BoxError> {
        match self {
            Self::Ed25519 => ed25519::VERIFIER.primary().clone().try_flush(),
            Self::Sapling => sapling::try_flush(&sapling::VERIFIER),
            Self::Halo2PreNu6_2 => halo2::try_flush(&halo2::VERIFIER_PRE_NU6_2),
            Self::Halo2Nu6_2 => halo2::try_flush(&halo2::VERIFIER_NU6_2),
            Self::Halo2Nu6_3Onward => halo2::try_flush(&halo2::VERIFIER_NU6_3_ONWARD),
        }
    }
}

/// A registered block's queueing that has not finished, and where it went.
#[derive(Debug)]
struct Progress {
    /// Shielded transactions still building their checks, plus requests not yet queued.
    pending: usize,

    /// The [`Verifier::bit`]s of the batch workers that received an item.
    queued: u8,
}

impl Progress {
    /// Releases one pending guard, returning the [`Verifier::bit`]s to flush if it was the last.
    fn release(&mut self) -> Option<u8> {
        self.pending -= 1;
        (self.pending == 0).then_some(self.queued)
    }
}

/// A block's registration, held until its transaction checks have finished or been dropped.
#[derive(Debug)]
pub(crate) struct BlockFlush {
    key: usize,
}

impl BlockFlush {
    /// Registers a block with `shielded_transactions` shielded transactions, keyed by
    /// `context`, an allocation every one of its transaction requests shares.
    ///
    /// The count is taken before dispatch, so a transaction whose service call is delayed still
    /// holds the flush back.
    pub(crate) fn register<T>(context: &Arc<T>, shielded_transactions: usize) -> Self {
        let key = key(context);
        let pending = Arc::new(Mutex::new(Progress {
            pending: shielded_transactions,
            queued: 0,
        }));

        BLOCKS
            .lock()
            .expect("block flush registry mutex should not be poisoned")
            .insert(key, pending);

        Self { key }
    }
}

impl Drop for BlockFlush {
    fn drop(&mut self) {
        BLOCKS
            .lock()
            .expect("block flush registry mutex should not be poisoned")
            .remove(&self.key);
    }
}

/// Returns the registry key for `context`.
fn key<T>(context: &Arc<T>) -> usize {
    // The allocation address is an opaque identity, never dereferenced or used for arithmetic.
    Arc::as_ptr(context) as usize
}

/// A shielded transaction or batch request of a registered block that has not finished queueing.
///
/// Dropping the last one flushes the batch workers the block queued items in.
#[derive(Debug)]
pub(crate) struct Queueing(Pending);

impl Queueing {
    /// Returns the guard of a shielded transaction in the block registered under `context`, or
    /// `None` if no block is registered under it.
    ///
    /// The registration already counts the transaction.
    pub(crate) fn for_transaction<T>(context: &Arc<T>) -> Option<Self> {
        BLOCKS
            .lock()
            .expect("block flush registry mutex should not be poisoned")
            .get(&key(context))
            .cloned()
            .map(Self)
    }

    /// Runs `f`, the synchronous construction of this transaction's checks, so that the batch
    /// requests it builds take guards from this transaction's block.
    pub(crate) fn scope<T>(&self, f: impl FnOnce() -> T) -> T {
        CURRENT.sync_scope(self.0.clone(), f)
    }

    /// Returns a guard for a batch request built inside [`Self::scope`], or `None` outside it.
    pub(crate) fn for_request() -> Option<Self> {
        CURRENT
            .try_with(|pending| {
                pending
                    .lock()
                    .expect("block flush progress mutex should not be poisoned")
                    .pending += 1;
                Self(pending.clone())
            })
            .ok()
    }

    /// Records that this request's item is in `verifier`'s batch queue, and releases the guard.
    pub(crate) fn enqueued(self, verifier: Verifier) {
        self.0
            .lock()
            .expect("block flush progress mutex should not be poisoned")
            .queued |= verifier.bit();
    }
}

impl Drop for Queueing {
    fn drop(&mut self) {
        let Some(queued) = self
            .0
            .lock()
            .expect("block flush progress mutex should not be poisoned")
            .release()
        else {
            return;
        };

        // The progress lock is released first: queueing a flush can wait on a batch service lock.
        for verifier in Verifier::ALL {
            if queued & verifier.bit() == 0 {
                continue;
            }

            match verifier.try_flush() {
                Ok(true) => {}
                Ok(false) => {
                    tracing::trace!(?verifier, "batch queue saturated, skipping block flush");
                }
                Err(error) => {
                    tracing::trace!(?verifier, ?error, "could not queue block flush");
                }
            }
        }
    }
}
