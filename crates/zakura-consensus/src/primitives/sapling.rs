//! Async Sapling batch verifier service

use core::fmt;
use std::{
    future::Future,
    mem,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{future::BoxFuture, FutureExt};
use once_cell::sync::Lazy;
use rand::thread_rng;
use tokio::sync::watch;
use tower::{util::ServiceFn, Service};
use tower_batch_control::{Batch, BatchControl, RequestWeight};
use tower_fallback::Fallback;

use sapling_crypto::{bundle::Authorized, BatchValidator, Bundle};
use zakura_chain::transaction::{ParseDigest, SigHash};
use zcash_proofs::prover::LocalTxProver;
use zcash_protocol::value::ZatBalance;

use crate::{error::TransactionError, BoxError};

use super::memo::{CacheKey, Memoized, MemoizedItem, MEMO_CAPACITY};

#[cfg(test)]
mod tests;

/// The BLAKE2b personalization for [`Item`] memo cache keys.
///
/// Domain-separates these keys from every other BLAKE2b-256 hash in the protocol, so that a
/// memo key can never be confused with a txid, an auth digest, a sighash, or a Halo2 memo key.
const SAPLING_MEMO_PERSONALIZATION: &[u8; 16] = b"ZakuraSapngMemo1";

/// Sapling prover containing spend and output params for the Sapling circuit.
///
/// Used to:
///
/// - construct Sapling outputs in coinbase txs, and
/// - verify Sapling shielded data in the tx verifier.
static SAPLING: Lazy<LocalTxProver> = Lazy::new(LocalTxProver::bundled);

/// Returns the process-wide Sapling prover for constructing Sapling proofs, initializing it on
/// first use.
///
/// The bundled Sapling spend and output proving parameters are parsed once, then the same prover is
/// reused for proof construction and verification for the lifetime of the process.
pub fn sapling_prover() -> &'static LocalTxProver {
    Lazy::force(&SAPLING)
}

#[derive(Clone)]
pub struct Item {
    /// The bundle containing the Sapling shielded data to verify.
    bundle: Bundle<Authorized, ZatBalance>,
    /// The sighash of the transaction that contains the Sapling shielded data.
    sighash: SigHash,
    /// A digest of the input `librustzcash` parsed to produce [`Self::bundle`].
    ///
    /// Only used to derive [`Item::cache_key`]; verification never reads it.
    parse_digest: ParseDigest,
}

impl Item {
    /// Creates a new [`Item`] from a Sapling bundle, its parse digest, and the sighash.
    ///
    /// `bundle` and `parse_digest` must come from the *same* parse — take them together from
    /// [`SigHasher::sapling_bundle_and_parse_digest`], which is what makes that structural.
    ///
    /// [`SigHasher::sapling_bundle_and_parse_digest`]: zakura_chain::transaction::SigHasher::sapling_bundle_and_parse_digest
    pub fn new(
        bundle: Bundle<Authorized, ZatBalance>,
        parse_digest: ParseDigest,
        sighash: SigHash,
    ) -> Self {
        Self {
            bundle,
            sighash,
            parse_digest,
        }
    }
}

impl MemoizedItem for Item {
    /// Returns a key that determines every input to this item's bundle verification.
    ///
    /// [`Memoized`] reuses a previous `Ok` result whenever this key matches, so the key must
    /// commit to everything [`BatchValidator::check_bundle`] reads: the bundle and the sighash.
    ///
    /// # Why this commits to the transaction bytes rather than the bundle
    ///
    /// The Halo2 memo hashes its bundle's own consensus encoding, using `orchard`'s `pub`
    /// `write_v5_bundle`. Sapling has no such encoder: `zcash_primitives`' Sapling
    /// `write_v5_bundle` is `pub(crate)`, `sapling-crypto` has no bundle serialization at all,
    /// and `zakura-chain`'s `ZcashSerialize` impl is for its own `sapling::ShieldedData`, not for
    /// the `sapling_crypto::Bundle` the verifier holds. Hand-writing one would reach the bundle's
    /// fields through getters, so an upstream field addition would be a silent omission from the
    /// key rather than a compile error — precisely the failure this design exists to prevent.
    ///
    /// So this commits to [`ParseDigest`] instead. Every bundle the verifiers see is the output
    /// of `zcash_primitives::transaction::Transaction::read` applied to exactly the bytes and
    /// branch id that digest covers, so the digest determines the bundle **by construction**.
    /// The argument is not "we enumerated the bundle's fields correctly", it is "the bundle is a
    /// pure function of these inputs" — strictly stronger, and immune to upstream adding a field.
    ///
    /// The sighash is committed to separately rather than derived, because the shielded sighash
    /// depends on the spent transparent outputs (their values and `scriptPubKey`s enter the
    /// ZIP 244 transparent sig digest), which are not in the transaction bytes.
    ///
    /// This over-commits: two transactions sharing a Sapling bundle would miss. That is
    /// irrelevant in practice — the sighash is in the key regardless and already commits to
    /// nearly the whole transaction, and the memo's job is to recognise the *same* transaction
    /// arriving twice (mempool, then block), where the bytes are identical.
    ///
    /// The verifying keys are *not* in the key. Unlike Orchard, Sapling has one spend and one
    /// output verifying key for all of history, from the bundled [`LocalTxProver`] parameters,
    /// so there are no eras to separate and one memo suffices.
    fn cache_key(&self) -> CacheKey {
        // Destructured exhaustively on purpose: adding a field to `Item` is a compile error here
        // until someone decides whether it belongs in the key. `bundle` is deliberately not
        // hashed — `parse_digest` already determines it, which is the whole point of the
        // construction above.
        let Item {
            bundle: _,
            sighash,
            parse_digest,
        } = self;

        let mut hasher = blake2b_simd::Params::new()
            .hash_length(32)
            .personal(SAPLING_MEMO_PERSONALIZATION)
            .to_state();

        hasher.update(parse_digest.as_bytes());
        hasher.update(&sighash.0);

        hasher
            .finalize()
            .as_bytes()
            .try_into()
            .expect("hash_length(32) produces exactly 32 bytes")
    }
}

impl RequestWeight for Item {
    fn request_weight(&self) -> usize {
        self.bundle
            .shielded_spends()
            .len()
            .saturating_add(self.bundle.shielded_outputs().len())
    }
}

/// A service that verifies Sapling shielded data in batches.
///
/// Handles batching incoming requests, driving batches to completion, and reporting results.
#[derive(Default)]
pub struct Verifier {
    /// A batch verifier for Sapling shielded data.
    batch: BatchValidator,

    /// A channel for broadcasting the verification result of the batch.
    ///
    /// Each batch gets a newly created channel, so there is only ever one result sent per channel.
    /// Tokio doesn't have a oneshot multi-consumer channel, so we use a watch channel.
    tx: watch::Sender<Option<bool>>,
}

impl fmt::Debug for Verifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Verifier")
            .field("batch", &"..")
            .field("tx", &self.tx)
            .finish()
    }
}

impl Drop for Verifier {
    // Flush the current batch in case there are still any pending futures.
    //
    // Flushing the batch means we need to validate it. This function fires off the validation and
    // returns immediately, usually before the validation finishes.
    fn drop(&mut self) {
        let batch = mem::take(&mut self.batch);
        let tx = mem::take(&mut self.tx);

        // The validation is CPU-intensive; do it on a dedicated thread so it does not block.
        rayon::spawn_fifo(move || {
            let (spend_vk, output_vk) = SAPLING.verifying_keys();

            // Validate the batch and send the result through the channel.
            let res = batch.validate(&spend_vk, &output_vk, thread_rng());
            let _ = tx.send(Some(res));
        });
    }
}

impl Service<BatchControl<Item>> for Verifier {
    type Response = ();
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: BatchControl<Item>) -> Self::Future {
        match req {
            BatchControl::Item(item) => {
                let mut rx = self.tx.subscribe();

                let bundle_check = self
                    .batch
                    .check_bundle(item.bundle, item.sighash.into())
                    .then_some(())
                    .ok_or(TransactionError::SaplingVerificationFailed);

                async move {
                    bundle_check.map_err(BoxError::from)?;

                    rx.changed()
                        .await
                        .map_err(|_| BoxError::from("verifier was dropped without flushing"))?;

                    // We use a new channel for each batch, so we always get the correct
                    // batch result here.
                    let is_valid = *rx.borrow().as_ref().ok_or_else(|| {
                        Box::<dyn std::error::Error + Send + Sync>::from(
                            "threadpool unexpectedly dropped channel sender",
                        )
                    })?;

                    if is_valid {
                        metrics::counter!("proofs.sapling.verified").increment(1);
                        Ok(())
                    } else {
                        metrics::counter!("proofs.sapling.invalid").increment(1);
                        Err(BoxError::from(TransactionError::SaplingVerificationFailed))
                    }
                }
                .boxed()
            }

            BatchControl::Flush => {
                let batch = mem::take(&mut self.batch);
                let tx = mem::take(&mut self.tx);

                async move {
                    let start = std::time::Instant::now();
                    let spawn_result = tokio::task::spawn_blocking(move || {
                        let (spend_vk, output_vk) = SAPLING.verifying_keys();
                        batch.validate(&spend_vk, &output_vk, thread_rng())
                    })
                    .await;
                    let duration = start.elapsed().as_secs_f64();

                    let result_label = match &spawn_result {
                        Ok(true) => "success",
                        _ => "failure",
                    };
                    metrics::histogram!(
                        "zakura.consensus.batch.duration_seconds",
                        "verifier" => "groth16_sapling",
                        "result" => result_label
                    )
                    .record(duration);

                    // Extract the value before consuming spawn_result
                    let is_valid = spawn_result.as_ref().ok().copied();
                    let _ = tx.send(is_valid);
                    spawn_result.map(|_| ()).map_err(Self::Error::from)
                }
                .boxed()
            }
        }
    }
}

/// Verifies a single [`Item`].
pub fn verify_single(
    item: Item,
) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send>> {
    async move {
        let mut verifier = Verifier::default();

        let check = verifier
            .batch
            .check_bundle(item.bundle, item.sighash.into())
            .then_some(())
            .ok_or(TransactionError::SaplingVerificationFailed);
        check.map_err(BoxError::from)?;

        let is_valid = tokio::task::spawn_blocking(move || {
            let (spend_vk, output_vk) = SAPLING.verifying_keys();

            mem::take(&mut verifier.batch).validate(&spend_vk, &output_vk, thread_rng())
        })
        .await
        .map_err(|_| BoxError::from("Sapling bundle validation thread panicked"))?;

        if is_valid {
            Ok(())
        } else {
            Err(BoxError::from(TransactionError::SaplingVerificationFailed))
        }
    }
    .boxed()
}

/// The batching-and-fallback stack for Sapling bundle verification, before memoization.
type BatchFallbackService = Fallback<
    Batch<Verifier, Item>,
    ServiceFn<fn(Item) -> BoxFuture<'static, Result<(), Box<dyn std::error::Error + Send + Sync>>>>,
>;

/// The concrete type of the global Sapling verification service.
type VerifierService = Memoized<BatchFallbackService>;

/// Global batch verification context for Sapling shielded data.
///
/// The stack is wrapped in a [`Memoized`] so that a bundle verified when its transaction was
/// gossiped into the mempool does not have to be verified again when the block that mines it
/// arrives. Sapling needs only one memo: unlike Orchard, its spend and output verifying keys are
/// the same for all of history, so there are no circuit eras to keep apart.
pub static VERIFIER: Lazy<VerifierService> =
    Lazy::new(|| Memoized::new(batch_fallback_verifier(), MEMO_CAPACITY, "groth16_sapling"));

/// Builds the un-memoized batching-and-fallback stack.
fn batch_fallback_verifier() -> BatchFallbackService {
    Fallback::new(
        Batch::new(
            Verifier::default(),
            super::MAX_BATCH_SIZE,
            None,
            super::MAX_BATCH_LATENCY,
        ),
        tower::service_fn(verify_single as fn(Item) -> _),
    )
}
