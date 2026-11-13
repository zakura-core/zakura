//! Asynchronous verification of cryptographic primitives.

use std::sync::LazyLock;

use tokio::sync::oneshot::error::RecvError;
use zakura_chain::{
    parameters::NetworkUpgrade,
    transaction::{SigHash, UnminedTxId},
};

use crate::BoxError;

pub(crate) use cache::ShieldedPool;
use cache::{CacheKey, Cached};

pub(crate) mod batch_control;
mod cache;
#[cfg(feature = "sprout")]
pub mod ed25519;
pub(crate) mod fallback;
pub(crate) mod flush;
#[cfg(feature = "sprout")]
pub mod groth16;
pub mod halo2;
pub mod redjubjub;
pub mod redpallas;
pub mod sapling;

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

/// Returns `true` if the transaction `tx_id` has a bundle in each of `pools`, and every one of
/// them has already verified against `sighash`.
///
/// The transaction verifier asks this before it builds the bundles, because building them
/// means reading the whole transaction a second time. Each pool is looked up in the cache of the
/// verifier that checks it: the Sapling verifier, or the Halo2 verifier for the Orchard circuit
/// in force at `network_upgrade`. A verifier that has not been built yet remembers nothing, and
/// the lookup does not build it. An empty `pools` is never verified, because no cache entry
/// stands for a transaction without shielded bundles.
///
/// A `true` counts one cache hit per pool, in place of the verifier calls the caller skips.
/// A `false` counts nothing: the caller then calls the verifiers, which count for themselves.
///
/// # Correctness
///
/// The lookup uses the key the verifiers record a success under, so it accepts exactly the
/// bundles their own cache would: see [`CacheKey`] for why the transaction ID, sighash and pool
/// determine a bundle and its verification.
pub(crate) fn shielded_bundles_are_verified(
    network_upgrade: NetworkUpgrade,
    tx_id: UnminedTxId,
    sighash: &SigHash,
    pools: impl Iterator<Item = ShieldedPool> + Clone,
) -> bool {
    let verified_keys_for = |pool: ShieldedPool| match pool {
        ShieldedPool::Sapling => LazyLock::get(&sapling::VERIFIER).map(Cached::verified_keys),
        ShieldedPool::Orchard | ShieldedPool::Ironwood => {
            halo2::verified_keys_if_built(network_upgrade)
        }
    };
    let is_verified = |pool: ShieldedPool| {
        verified_keys_for(pool)
            .is_some_and(|keys| keys.contains(&CacheKey::new(tx_id, sighash.0, pool)))
    };

    if pools.clone().next().is_none() || !pools.clone().all(is_verified) {
        return false;
    }

    pools
        .filter_map(verified_keys_for)
        .for_each(|keys| keys.count_hit());

    true
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
