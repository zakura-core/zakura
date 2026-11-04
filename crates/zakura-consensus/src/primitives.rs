//! Asynchronous verification of cryptographic primitives.

use std::sync::LazyLock;

use tokio::sync::oneshot::error::RecvError;

use crate::BoxError;

pub(crate) mod batch_control;
mod cache;
pub mod ed25519;
pub(crate) mod fallback;
pub(crate) mod flush;
pub mod groth16;
pub mod halo2;
pub mod redjubjub;
pub mod redpallas;
pub mod sapling;
#[cfg(zcash_unstable = "nutachyon")]
pub mod tachyon;

/// The maximum batch size for any of the batch verifiers.
const MAX_BATCH_SIZE: usize = 64;

/// The maximum latency bound for any of the batch verifiers.
const MAX_BATCH_LATENCY: std::time::Duration = std::time::Duration::from_millis(100);

/// Forgets every shielded bundle verification this process has cached.
///
/// # Benchmarks only
///
/// The worst case Zakura is sized against is a block full of transactions the node has never
/// seen, so a benchmark that replays one workload must start each iteration with cold caches or
/// it measures cache hits instead of proof and signature verification. Nothing in the node calls
/// this.
///
/// It is always safe to call: forgetting a key costs a re-verification and can never accept a
/// bundle that was not verified.
#[doc(hidden)]
pub fn clear_shielded_verification_caches() {
    if let Some(verifier) = LazyLock::get(&sapling::VERIFIER) {
        verifier.clear();
    }

    if let Some(verifier) = LazyLock::get(&halo2::VERIFIER_PRE_NU6_2) {
        verifier.clear();
    }

    if let Some(verifier) = LazyLock::get(&halo2::VERIFIER_NU6_2) {
        verifier.clear();
    }

    if let Some(verifier) = LazyLock::get(&halo2::VERIFIER_NU6_3_ONWARD) {
        verifier.clear();
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
