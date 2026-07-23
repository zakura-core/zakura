//! Asynchronous verification of cryptographic primitives.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use once_cell::sync::Lazy;
use tokio::sync::oneshot::error::RecvError;

use crate::BoxError;

pub mod ed25519;
pub mod groth16;
pub mod halo2;
pub mod redjubjub;
pub mod redpallas;
pub mod sapling;

/// The maximum batch size for any of the batch verifiers.
const MAX_BATCH_SIZE: usize = 64;

/// The maximum latency bound for any of the batch verifiers.
const MAX_BATCH_LATENCY: std::time::Duration = std::time::Duration::from_millis(100);

/// Registered block transaction-verification batches waiting for an explicit
/// cryptographic flush.
static BLOCK_VERIFIER_BATCH_FLUSHES: Lazy<Mutex<HashMap<BlockVerifierBatchFlushKey, BlockFlush>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Opaque identity for a semantic block verifier's transaction batch.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct BlockVerifierBatchFlushKey(usize);

impl BlockVerifierBatchFlushKey {
    /// Returns a key for a value shared by all transaction requests in one
    /// block verification.
    fn new<T>(shared_block_context: &Arc<T>) -> Self {
        // This cast stores the Arc allocation address as an opaque identity
        // key. The address is never dereferenced or used for pointer arithmetic.
        Self(Arc::as_ptr(shared_block_context) as usize)
    }
}

/// Registration guard for one semantic block verifier's crypto batch flush.
#[derive(Debug)]
pub(crate) struct BlockVerifierBatchFlushGuard {
    key: BlockVerifierBatchFlushKey,
}

impl Drop for BlockVerifierBatchFlushGuard {
    fn drop(&mut self) {
        BLOCK_VERIFIER_BATCH_FLUSHES
            .lock()
            .expect("block verifier batch flush registry mutex should not be poisoned")
            .remove(&self.key);
    }
}

/// Tracks transaction verifier progress toward one block-level flush.
#[derive(Debug)]
struct BlockFlush {
    expected_transactions: usize,
    started_transactions: usize,
    flush_queued: bool,
}

/// Registers a block's transaction verifier batch for one explicit crypto
/// flush once every transaction has started its asynchronous checks.
pub(crate) fn register_block_verifier_batch_flush<T>(
    shared_block_context: &Arc<T>,
    expected_transactions: usize,
) -> BlockVerifierBatchFlushGuard {
    let key = BlockVerifierBatchFlushKey::new(shared_block_context);

    BLOCK_VERIFIER_BATCH_FLUSHES
        .lock()
        .expect("block verifier batch flush registry mutex should not be poisoned")
        .insert(
            key,
            BlockFlush {
                expected_transactions,
                started_transactions: 0,
                flush_queued: expected_transactions == 0,
            },
        );

    BlockVerifierBatchFlushGuard { key }
}

/// Returns the block-level batch flush key for `shared_block_context`.
pub(crate) fn block_verifier_batch_flush_key<T>(
    shared_block_context: &Arc<T>,
) -> BlockVerifierBatchFlushKey {
    BlockVerifierBatchFlushKey::new(shared_block_context)
}

/// Records that one block transaction has started its async checks, and
/// flushes the shared crypto batches when all transactions in the block have
/// reached that boundary.
pub(crate) fn start_block_transaction_async_checks(key: BlockVerifierBatchFlushKey) {
    if block_verifier_batch_flush_ready(key) {
        flush_block_verifier_batches();
    }
}

/// Returns `true` if recording `key` made its block ready to flush.
fn block_verifier_batch_flush_ready(key: BlockVerifierBatchFlushKey) -> bool {
    let mut flushes = BLOCK_VERIFIER_BATCH_FLUSHES
        .lock()
        .expect("block verifier batch flush registry mutex should not be poisoned");

    let Some(flush) = flushes.get_mut(&key) else {
        return false;
    };

    flush.started_transactions = flush.started_transactions.saturating_add(1);

    if flush.flush_queued || flush.started_transactions < flush.expected_transactions {
        return false;
    }

    flush.flush_queued = true;
    true
}

/// Explicitly flushes the shared crypto batch services used by semantic block
/// transaction verification.
///
/// The block verifier still awaits every transaction verifier future before
/// committing a block; this just starts pending batch work before
/// [`MAX_BATCH_LATENCY`] expires. Saturated queues are skipped: a full queue
/// is already flushing batches on size, and waiting for capacity would couple
/// block latency to unrelated verifier traffic.
fn flush_block_verifier_batches() {
    if let Some(verifier) = Lazy::get(&ed25519::VERIFIER) {
        queue_batch_flush("ed25519", verifier.primary().clone().try_flush());
    }

    if let Some(verifier) = Lazy::get(&sapling::VERIFIER) {
        queue_batch_flush("sapling", verifier.primary().clone().try_flush());
    }

    if let Some(verifier) = Lazy::get(&halo2::VERIFIER_PRE_NU6_2) {
        queue_batch_flush("halo2_pre_nu6_2", verifier.primary().clone().try_flush());
    }

    if let Some(verifier) = Lazy::get(&halo2::VERIFIER_NU6_2) {
        queue_batch_flush("halo2_nu6_2", verifier.primary().clone().try_flush());
    }

    if let Some(verifier) = Lazy::get(&halo2::VERIFIER_NU6_3_ONWARD) {
        queue_batch_flush("halo2_nu6_3_onward", verifier.primary().clone().try_flush());
    }
}

/// Logs best-effort flush queueing skips and failures without changing
/// verification semantics.
fn queue_batch_flush(verifier: &'static str, result: Result<bool, BoxError>) {
    match result {
        Ok(true) => {}
        Ok(false) => {
            tracing::trace!(verifier, "batch queue saturated, skipping explicit flush");
        }
        Err(error) => {
            tracing::trace!(
                ?error,
                verifier,
                "could not queue explicit block verifier batch flush"
            );
        }
    }
}

/// Fires off a task into the Rayon threadpool, awaits the result through a oneshot channel,
/// then converts the error to a [`BoxError`].
pub async fn spawn_fifo_and_convert<
    E: 'static + std::error::Error + Into<BoxError> + Sync + Send,
    F: 'static + FnOnce() -> Result<(), E> + Send,
>(
    f: F,
) -> Result<(), BoxError> {
    spawn_fifo(f)
        .await
        .map_err(|_| {
            "threadpool unexpectedly dropped response channel sender. Is Zakura shutting down?"
        })?
        .map_err(BoxError::from)
}

/// Fires off a task into the Rayon threadpool and awaits the result through a oneshot channel.
pub async fn spawn_fifo<T: 'static + Send, F: 'static + FnOnce() -> T + Send>(
    f: F,
) -> Result<T, RecvError> {
    // Rayon doesn't have a spawn function that returns a value,
    // so we use a oneshot channel instead.
    let (rsp_tx, rsp_rx) = tokio::sync::oneshot::channel();

    rayon::spawn_fifo(move || {
        let _ = rsp_tx.send(f());
    });

    rsp_rx.await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_flush_is_ready_when_expected_transactions_start_checks() {
        let shared_block_context = Arc::new(());
        let key = block_verifier_batch_flush_key(&shared_block_context);
        let _guard = register_block_verifier_batch_flush(&shared_block_context, 2);

        assert!(!block_verifier_batch_flush_ready(key));
        assert!(block_verifier_batch_flush_ready(key));
        assert!(!block_verifier_batch_flush_ready(key));
    }

    #[test]
    fn block_flush_guard_unregisters_batch() {
        let shared_block_context = Arc::new(());
        let key = block_verifier_batch_flush_key(&shared_block_context);

        {
            let _guard = register_block_verifier_batch_flush(&shared_block_context, 1);
            assert!(block_verifier_batch_flush_ready(key));
        }

        assert!(!block_verifier_batch_flush_ready(key));
    }

    #[test]
    fn block_flush_registration_starts_fresh_after_guard_drop() {
        let shared_block_context = Arc::new(());
        let key = block_verifier_batch_flush_key(&shared_block_context);

        {
            let _guard = register_block_verifier_batch_flush(&shared_block_context, 2);
            assert!(!block_verifier_batch_flush_ready(key));
        }

        let _guard = register_block_verifier_batch_flush(&shared_block_context, 2);
        assert!(!block_verifier_batch_flush_ready(key));
        assert!(block_verifier_batch_flush_ready(key));
    }
}
