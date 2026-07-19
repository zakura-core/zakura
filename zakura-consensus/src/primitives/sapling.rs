//! Async Sapling batch verifier service

use core::fmt;
use std::{
    fs,
    future::Future,
    io::{self, Write},
    mem,
    path::{Path, PathBuf},
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
use tracing::{info, warn};

use sapling_crypto::{bundle::Authorized, BatchValidator, Bundle};
use zakura_chain::transaction::SigHash;
use zcash_proofs::{prover::LocalTxProver, SAPLING_OUTPUT_NAME, SAPLING_SPEND_NAME};
use zcash_protocol::value::ZatBalance;

use crate::{error::TransactionError, BoxError};

/// Sapling prover containing spend and output params for the Sapling circuit.
///
/// Used to:
///
/// - construct Sapling outputs in coinbase txs, and
/// - verify Sapling shielded data in the tx verifier.
///
/// Verification only ever reads this prover's [verifying keys][LocalTxProver::verifying_keys], so
/// it keeps using the parameters bundled in the binary and never touches the disk — this keeps the
/// consensus-critical verification path independent of any on-disk parameter files. Constructing
/// Sapling proofs (for coinbase transactions) instead goes through [`sapling_prover`], which caches
/// the proving parameters to disk on first use.
static SAPLING: Lazy<LocalTxProver> = Lazy::new(LocalTxProver::bundled);

/// The exact size in bytes of a valid `sapling-spend.params` file.
///
/// This is a fixed protocol constant (the Sapling circuit and its parameters never change); it
/// mirrors the private `SAPLING_SPEND_BYTES` constant in `zcash_proofs`. It is used to detect a
/// truncated or otherwise wrong-sized on-disk cache file before loading it, so a corrupt cache is
/// rewritten instead of triggering a panic inside [`LocalTxProver::new`].
const SAPLING_SPEND_BYTES: u64 = 47_958_396;

/// The exact size in bytes of a valid `sapling-output.params` file. See [`SAPLING_SPEND_BYTES`].
const SAPLING_OUTPUT_BYTES: u64 = 3_592_860;

/// The Sapling prover used to *construct* Sapling proofs, for example the shielded outputs of a
/// coinbase transaction built by `getblocktemplate`.
///
/// Unlike [`SAPLING`], this prover is only initialized when the node actually needs to create
/// Sapling proofs, and its proving parameters are cached to disk on first use so they don't need to
/// be re-parsed from the bundled copy on every template refresh.
static COINBASE_SAPLING_PROVER: Lazy<LocalTxProver> = Lazy::new(init_coinbase_sapling_prover);

/// Returns the process-wide Sapling prover for constructing Sapling proofs, initializing it on
/// first use.
///
/// The Sapling spend and output proving parameters are cached to disk (in the default Zcash
/// parameters folder) the first time they are needed — for example, when `getblocktemplate` builds
/// a coinbase transaction. On later uses — including after a restart — they are loaded from that
/// on-disk cache instead of being re-parsed from the bundled copy in the binary on every call.
pub fn sapling_prover() -> &'static LocalTxProver {
    Lazy::force(&COINBASE_SAPLING_PROVER)
}

/// Builds the coinbase Sapling prover, caching its proving parameters to disk on first use.
///
/// Preference order:
///
/// 1. Cache the parameters to the default Zcash parameters folder (writing the bundled parameters
///    there if a valid cache file isn't already present), then load them from disk.
/// 2. If the parameters can't be cached to disk (for example, the parameters folder can't be
///    determined or written to), fall back to the bundled parameters in memory so that proof
///    construction still works.
fn init_coinbase_sapling_prover() -> LocalTxProver {
    match cache_bundled_sapling_params() {
        Ok((spend_path, output_path)) => {
            info!(
                ?spend_path,
                ?output_path,
                "loading Sapling proving parameters from the on-disk cache"
            );
            LocalTxProver::new(&spend_path, &output_path)
        }
        Err(error) => {
            warn!(
                %error,
                "could not cache Sapling proving parameters to disk, \
                 using the parameters bundled in the binary"
            );
            LocalTxProver::bundled()
        }
    }
}

/// Ensures the bundled Sapling spend and output parameters are cached to disk in the default Zcash
/// parameters folder, returning their on-disk paths.
///
/// Cache files that already exist with the correct size are reused; missing or wrong-sized files
/// are (re)written from the bundled parameters. The bundled parameters are only loaded into memory
/// when a cache file actually needs writing.
fn cache_bundled_sapling_params() -> io::Result<(PathBuf, PathBuf)> {
    let params_dir = zcash_proofs::default_params_folder().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "could not determine the default Zcash parameters folder",
        )
    })?;
    fs::create_dir_all(&params_dir)?;

    let spend_path = params_dir.join(SAPLING_SPEND_NAME);
    let output_path = params_dir.join(SAPLING_OUTPUT_NAME);

    if !is_cached(&spend_path, SAPLING_SPEND_BYTES)
        || !is_cached(&output_path, SAPLING_OUTPUT_BYTES)
    {
        let (spend_bytes, output_bytes) = wagyu_zcash_parameters::load_sapling_parameters();
        write_cache_file(&spend_path, &spend_bytes, SAPLING_SPEND_BYTES)?;
        write_cache_file(&output_path, &output_bytes, SAPLING_OUTPUT_BYTES)?;
    }

    Ok((spend_path, output_path))
}

/// Returns whether `path` already holds a cache file of exactly `expected_len` bytes.
fn is_cached(path: &Path, expected_len: u64) -> bool {
    fs::metadata(path)
        .map(|metadata| metadata.is_file() && metadata.len() == expected_len)
        .unwrap_or(false)
}

/// Atomically writes `bytes` to `path`, unless a correctly-sized file already exists there.
///
/// The bytes are written to a temporary file in the same directory and then renamed into place, so
/// that a partially-written file can never be observed (or loaded) as a valid parameter file.
fn write_cache_file(path: &Path, bytes: &[u8], expected_len: u64) -> io::Result<()> {
    if is_cached(path, expected_len) {
        return Ok(());
    }

    // Include the process id in the temporary file name so concurrent nodes don't clobber each
    // other's in-progress writes before the atomic rename.
    let tmp_path = path.with_extension(format!("tmp-{}", std::process::id()));
    let mut tmp_file = fs::File::create(&tmp_path)?;
    tmp_file.write_all(bytes)?;
    tmp_file.sync_all()?;
    fs::rename(&tmp_path, path)?;

    Ok(())
}

#[derive(Clone)]
pub struct Item {
    /// The bundle containing the Sapling shielded data to verify.
    bundle: Bundle<Authorized, ZatBalance>,
    /// The sighash of the transaction that contains the Sapling shielded data.
    sighash: SigHash,
}

impl Item {
    /// Creates a new [`Item`] from a Sapling bundle and sighash.
    pub fn new(bundle: Bundle<Authorized, ZatBalance>, sighash: SigHash) -> Self {
        Self { bundle, sighash }
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

/// Global batch verification context for Sapling shielded data.
pub static VERIFIER: Lazy<
    Fallback<
        Batch<Verifier, Item>,
        ServiceFn<
            fn(Item) -> BoxFuture<'static, Result<(), Box<dyn std::error::Error + Send + Sync>>>,
        >,
    >,
> = Lazy::new(|| {
    Fallback::new(
        Batch::new(
            Verifier::default(),
            super::MAX_BATCH_SIZE,
            None,
            super::MAX_BATCH_LATENCY,
        ),
        tower::service_fn(verify_single),
    )
});

#[cfg(test)]
mod tests {
    use super::{is_cached, write_cache_file};

    /// The on-disk parameter cache is written atomically, reused when present with the correct
    /// size, and treated as stale when the size doesn't match.
    #[test]
    fn write_cache_file_is_idempotent_and_size_checked() {
        let dir =
            std::env::temp_dir().join(format!("zakura-sapling-cache-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("can create the temp cache dir");
        let path = dir.join("sapling-spend.params");
        let bytes = b"pretend sapling parameters";
        let len = bytes.len() as u64;

        // Missing file is not cached.
        assert!(!is_cached(&path, len));

        // First write populates the cache.
        write_cache_file(&path, bytes, len).expect("writes the cache file");
        assert!(is_cached(&path, len));
        assert_eq!(std::fs::read(&path).expect("reads the cache file"), bytes);

        // A file with an unexpected size is treated as a stale cache.
        assert!(!is_cached(&path, len + 1));

        // Re-writing with the expected size present is a no-op that leaves the contents intact.
        write_cache_file(&path, b"different contents", len).expect("no-op write");
        assert_eq!(
            std::fs::read(&path).expect("reads the cache file"),
            bytes,
            "an already-cached file of the correct size must not be overwritten"
        );

        std::fs::remove_dir_all(&dir).ok();
    }
}
