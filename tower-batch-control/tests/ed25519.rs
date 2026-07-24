//! Test batching using ed25519 verification.

#![allow(clippy::unwrap_in_result)]

use std::{
    mem,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use color_eyre::{eyre::eyre, Report};
use ed25519_zebra::{batch, Error, Signature, SigningKey, VerificationKeyBytes};
use futures::stream::{FuturesOrdered, StreamExt};
use futures::FutureExt;
use futures_core::Future;
use rand::thread_rng;
use tokio::sync::{oneshot::error::RecvError, watch};
use tower::{Service, ServiceExt};
use tower_batch_control::{Batch, BatchControl, RequestWeight};
use tower_fallback::Fallback;

// ============ service impl ============

/// A boxed [`std::error::Error`].
type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Keeps partial batches pending unless a test explicitly releases them.
const LONG_BATCH_LATENCY: Duration = Duration::from_secs(1000);

/// The type of the batch verifier.
type BatchVerifier = batch::Verifier;

/// The type of verification results.
type VerifyResult = Result<(), Error>;

/// The type of the batch sender channel.
type Sender = watch::Sender<Option<VerifyResult>>;

/// The type of the batch item.
/// A newtype around an `Ed25519Item` which implements [`RequestWeight`].
#[derive(Clone, Debug)]
struct Item(batch::Item);

impl RequestWeight for Item {}

impl<'msg, M: AsRef<[u8]> + ?Sized> From<(VerificationKeyBytes, Signature, &'msg M)> for Item {
    fn from(tup: (VerificationKeyBytes, Signature, &'msg M)) -> Self {
        Self(batch::Item::from(tup))
    }
}

impl Item {
    fn verify_single(self) -> VerifyResult {
        self.0.verify_single()
    }
}

/// Ed25519 signature verifier service
struct Verifier {
    /// A batch verifier for ed25519 signatures.
    batch: BatchVerifier,

    /// A channel for broadcasting the result of a batch to the futures for each batch item.
    ///
    /// Each batch gets a newly created channel, so there is only ever one result sent per channel.
    /// Tokio doesn't have a oneshot multi-consumer channel, so we use a watch channel.
    tx: Sender,
}

impl Default for Verifier {
    fn default() -> Self {
        let batch = BatchVerifier::default();
        let (tx, _) = watch::channel(None);
        Self { batch, tx }
    }
}

impl Verifier {
    /// Returns the batch verifier and channel sender from `self`,
    /// replacing them with a new empty batch.
    fn take(&mut self) -> (BatchVerifier, Sender) {
        // Use a new verifier and channel for each batch.
        let batch = mem::take(&mut self.batch);

        let (tx, _) = watch::channel(None);
        let tx = mem::replace(&mut self.tx, tx);

        (batch, tx)
    }

    /// Synchronously process the batch, and send the result using the channel sender.
    /// This function blocks until the batch is completed.
    fn verify(batch: BatchVerifier, tx: Sender) {
        let result = batch.verify(thread_rng());
        let _ = tx.send(Some(result));
    }

    /// Flush the batch using a thread pool, and return the result via the channel.
    /// This returns immediately, usually before the batch is completed.
    fn flush_blocking(&mut self) {
        let (batch, tx) = self.take();

        // Correctness: Do CPU-intensive work on a dedicated thread, to avoid blocking other futures.
        //
        // We don't care about execution order here, because this method is only called on drop.
        tokio::task::block_in_place(|| rayon::spawn_fifo(|| Self::verify(batch, tx)));
    }

    /// Flush the batch using a thread pool, and return the result via the channel.
    /// This function returns a future that becomes ready when the batch is completed.
    async fn flush_spawning(batch: BatchVerifier, tx: Sender) {
        // Correctness: Do CPU-intensive work on a dedicated thread, to avoid blocking other futures.
        let _ = tx.send(spawn_fifo(move || batch.verify(thread_rng())).await.ok());
    }
}

impl Service<BatchControl<Item>> for Verifier {
    type Response = ();
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send + 'static>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: BatchControl<Item>) -> Self::Future {
        match req {
            BatchControl::Item(Item(item)) => {
                tracing::trace!("got ed25519 item");
                self.batch.queue(item);
                let mut rx = self.tx.subscribe();

                Box::pin(async move {
                    match rx.changed().await {
                        Ok(()) => {
                            // We use a new channel for each batch,
                            // so we always get the correct batch result here.
                            let result = rx.borrow()
                                .ok_or("threadpool unexpectedly dropped response channel sender. Is Zakura shutting down?")?;

                            if result.is_ok() {
                                tracing::trace!(?result, "validated ed25519 signature");
                            } else {
                                tracing::trace!(?result, "invalid ed25519 signature");
                            }
                            result.map_err(BoxError::from)
                        }
                        Err(_recv_error) => panic!("ed25519 verifier was dropped without flushing"),
                    }
                })
            }

            BatchControl::Flush => {
                tracing::trace!("got ed25519 flush command");

                let (batch, tx) = self.take();

                Box::pin(Self::flush_spawning(batch, tx).map(Ok))
            }
        }
    }
}

impl Drop for Verifier {
    fn drop(&mut self) {
        // We need to flush the current batch in case there are still any pending futures.
        // This returns immediately, usually before the batch is completed.
        self.flush_blocking();
    }
}

/// Fires off a task into the Rayon threadpool and awaits the result through a oneshot channel.
async fn spawn_fifo<
    E: 'static + std::error::Error + Sync + Send,
    F: 'static + FnOnce() -> Result<(), E> + Send,
>(
    f: F,
) -> Result<Result<(), E>, RecvError> {
    // Rayon doesn't have a spawn function that returns a value,
    // so we use a oneshot channel instead.
    let (rsp_tx, rsp_rx) = tokio::sync::oneshot::channel();

    rayon::spawn_fifo(move || {
        let _ = rsp_tx.send(f());
    });

    rsp_rx.await
}

// =============== testing code ========

fn signed_item(is_valid: bool) -> Item {
    let sk = SigningKey::new(thread_rng());
    let vk_bytes = VerificationKeyBytes::from(&sk);
    let msg = b"BatchVerifyTest";
    let sig = if is_valid {
        sk.sign(&msg[..])
    } else {
        sk.sign(b"badmsg")
    };

    (vk_bytes, sig, msg).into()
}

async fn sign_and_verify<V>(
    mut verifier: V,
    n: usize,
    bad_index: Option<usize>,
) -> Result<(), V::Error>
where
    V: Service<Item, Response = ()>,
{
    let mut results = FuturesOrdered::new();
    for i in 0..n {
        let span = tracing::trace_span!("sig", i);
        let sk = SigningKey::new(thread_rng());
        let vk_bytes = VerificationKeyBytes::from(&sk);
        let msg = b"BatchVerifyTest";
        let sig = if Some(i) == bad_index {
            sk.sign(b"badmsg")
        } else {
            sk.sign(&msg[..])
        };

        verifier.ready().await?;
        results.push_back(span.in_scope(|| verifier.call((vk_bytes, sig, msg).into())))
    }

    let mut numbered_results = results.enumerate();
    while let Some((i, result)) = numbered_results.next().await {
        if Some(i) == bad_index {
            assert!(result.is_err());
        } else {
            result?;
        }
    }

    Ok(())
}

async fn sign_and_verify_after_try_flush(
    mut verifier: Batch<Verifier, Item>,
    n: usize,
) -> Result<(), BoxError> {
    let mut results = FuturesOrdered::new();
    for _ in 0..n {
        let sk = SigningKey::new(thread_rng());
        let vk_bytes = VerificationKeyBytes::from(&sk);
        let msg = b"BatchVerifyTest";
        let sig = sk.sign(&msg[..]);

        verifier.ready().await?;
        results.push_back(verifier.call((vk_bytes, sig, msg).into()));
    }

    if !verifier.try_flush()? {
        return Err("try_flush should queue a flush on a quiet batch".into());
    }

    while let Some(result) = results.next().await {
        result?;
    }

    Ok(())
}

async fn explicit_flush_fallback_matches_single(
    n: usize,
    bad_index: usize,
) -> Result<(), BoxError> {
    let mut verifier = Fallback::new(
        Batch::new(Verifier::default(), 100, 1, LONG_BATCH_LATENCY),
        tower::service_fn(|item: Item| async move { item.verify_single().map_err(BoxError::from) }),
    );
    let mut expected_results = Vec::new();
    let mut batch_results = FuturesOrdered::new();

    for i in 0..n {
        let item = signed_item(i != bad_index);
        expected_results.push(item.clone().verify_single().is_ok());

        verifier.ready().await?;
        batch_results.push_back(verifier.call(item));
    }

    let mut primary = verifier.primary().clone();
    if !primary.try_flush()? {
        return Err("try_flush should queue a flush on a quiet batch".into());
    }

    let actual_results: Vec<_> = batch_results.map(|result| result.is_ok()).collect().await;
    assert_eq!(
        actual_results, expected_results,
        "explicit batch flush plus fallback must match single verification"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn batch_flushes_on_max_items_weight() -> Result<(), Report> {
    use tokio::time::timeout;
    let _init_guard = zakura_test::init();

    // Use a very long max_latency and a short timeout to check that
    // flushing is happening based on hitting max_items.
    //
    // Create our own verifier, so we don't shut down a shared verifier used by other tests.
    let verifier = Batch::new(Verifier::default(), 10, 5, LONG_BATCH_LATENCY);
    timeout(Duration::from_secs(1), sign_and_verify(verifier, 100, None))
        .await
        .map_err(|e| eyre!(e))?
        .map_err(|e| eyre!(e))?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn batch_flushes_on_max_latency() -> Result<(), Report> {
    use tokio::time::timeout;
    let _init_guard = zakura_test::init();

    // Use a very high max_items and a short timeout to check that
    // flushing is happening based on hitting max_latency.
    //
    // Create our own verifier, so we don't shut down a shared verifier used by other tests.
    let verifier = Batch::new(Verifier::default(), 100, 10, Duration::from_millis(500));
    timeout(Duration::from_secs(1), sign_and_verify(verifier, 10, None))
        .await
        .map_err(|e| eyre!(e))?
        .map_err(|e| eyre!(e))?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn try_flush_on_empty_batch_is_noop() -> Result<(), Report> {
    use tokio::time::timeout;
    let _init_guard = zakura_test::init();

    let mut verifier = Batch::new(Verifier::default(), 100, 10, LONG_BATCH_LATENCY);
    assert!(
        verifier.try_flush().map_err(|error| eyre!(error))?,
        "an empty batch flush must be queued"
    );

    timeout(
        Duration::from_secs(1),
        sign_and_verify_after_try_flush(verifier, 10),
    )
    .await
    .map_err(|e| eyre!(e))?
    .map_err(|e| eyre!(e))?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn batch_flushes_on_try_flush() -> Result<(), Report> {
    use tokio::time::timeout;
    let _init_guard = zakura_test::init();

    // Use a very high max_items and long max_latency. Without the non-blocking
    // flush, this verification would wait for the latency timer.
    let verifier = Batch::new(Verifier::default(), 100, 10, LONG_BATCH_LATENCY);
    timeout(
        Duration::from_secs(1),
        sign_and_verify_after_try_flush(verifier, 10),
    )
    .await
    .map_err(|e| eyre!(e))?
    .map_err(|e| eyre!(e))?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn batch_try_flush_uses_existing_readiness_permit() -> Result<(), Report> {
    let _init_guard = zakura_test::init();

    let mut verifier = Batch::new(Verifier::default(), 1, 1, LONG_BATCH_LATENCY);
    verifier.ready().await.map_err(|e| eyre!(e))?;
    assert!(
        verifier.try_flush().map_err(|error| eyre!(error))?,
        "try_flush must reuse an existing readiness permit"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn fallback_verification() -> Result<(), Report> {
    let _init_guard = zakura_test::init();

    // Create our own verifier, so we don't shut down a shared verifier used by other tests.
    let verifier = Fallback::new(
        Batch::new(Verifier::default(), 10, 1, Duration::from_millis(100)),
        tower::service_fn(|item: Item| async move { item.verify_single() }),
    );

    sign_and_verify(verifier, 100, Some(39))
        .await
        .map_err(|e| eyre!(e))?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn explicit_flush_fallback_matches_single_verification() -> Result<(), Report> {
    use tokio::time::timeout;
    let _init_guard = zakura_test::init();

    timeout(Duration::from_secs(5), async {
        // Cover an invalid singleton, then every invalid position in a mixed
        // batch. These batches are below the size threshold and have a long
        // latency, so only the explicit flush can drive them to completion.
        for (n, bad_index) in [(1, 0), (3, 0), (3, 1), (3, 2)] {
            explicit_flush_fallback_matches_single(n, bad_index).await?;
        }

        Ok::<_, BoxError>(())
    })
    .await
    .map_err(|error| eyre!(error))?
    .map_err(|error| eyre!(error))?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn explicit_flush_isolates_interleaved_request_groups() -> Result<(), Report> {
    use tokio::time::timeout;
    let _init_guard = zakura_test::init();

    let verifier = Fallback::new(
        Batch::new(Verifier::default(), 100, 1, LONG_BATCH_LATENCY),
        tower::service_fn(|item: Item| async move { item.verify_single().map_err(BoxError::from) }),
    );
    let mut block_a = verifier.clone();
    let mut block_b = verifier.clone();

    block_a.ready().await.map_err(|error| eyre!(error))?;
    let valid_a1 = block_a.call(signed_item(true));
    block_b.ready().await.map_err(|error| eyre!(error))?;
    let invalid_b = block_b.call(signed_item(false));
    block_a.ready().await.map_err(|error| eyre!(error))?;
    let valid_a2 = block_a.call(signed_item(true));

    let mut primary = verifier.primary().clone();
    assert!(
        primary.try_flush().map_err(|error| eyre!(error))?,
        "the shared batch must have capacity for an explicit flush"
    );

    let (valid_a1, invalid_b, valid_a2) = timeout(
        Duration::from_secs(5),
        futures::future::join3(valid_a1, invalid_b, valid_a2),
    )
    .await
    .map_err(|error| eyre!(error))?;

    valid_a1.map_err(|error| eyre!(error))?;
    assert!(
        invalid_b.is_err(),
        "an invalid request from another group must not inherit a valid result"
    );
    valid_a2.map_err(|error| eyre!(error))?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn saturated_try_flush_still_rejects_after_latency() -> Result<(), Report> {
    use tokio::time::timeout;
    let _init_guard = zakura_test::init();

    let (batch, worker) = Batch::pair(Verifier::default(), 2, 1, Duration::from_millis(10));
    let mut verifier = Fallback::new(
        batch.clone(),
        tower::service_fn(|item: Item| async move { item.verify_single().map_err(BoxError::from) }),
    );

    verifier.ready().await.map_err(|error| eyre!(error))?;
    let invalid_result = verifier.call(signed_item(false));

    // Hold the remaining queue permit so the best-effort flush is skipped.
    let mut reservation = batch.clone();
    reservation.ready().await.map_err(|error| eyre!(error))?;
    let mut primary = batch.clone();
    assert!(
        !primary.try_flush().map_err(|error| eyre!(error))?,
        "try_flush must report the saturated queue"
    );

    drop(reservation);
    tokio::spawn(worker.run());

    let invalid_result = timeout(Duration::from_secs(5), invalid_result)
        .await
        .map_err(|error| eyre!(error))?;
    assert!(
        invalid_result.is_err(),
        "the latency fallback must reject an invalid signature"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_flush_still_rejects_via_single_fallback() -> Result<(), Report> {
    use tokio::time::timeout;
    let _init_guard = zakura_test::init();

    let (batch, worker) = Batch::pair(Verifier::default(), 100, 1, LONG_BATCH_LATENCY);
    let mut verifier = Fallback::new(
        batch.clone(),
        tower::service_fn(|item: Item| async move { item.verify_single().map_err(BoxError::from) }),
    );

    verifier.ready().await.map_err(|error| eyre!(error))?;
    let invalid_result = verifier.call(signed_item(false));

    // Dropping the worker fails the queued batch request and closes the flush
    // channel. Fallback must still verify the item singly and reject it.
    drop(worker);
    let mut primary = batch.clone();
    assert!(
        primary.try_flush().is_err(),
        "flushing a closed worker must fail"
    );

    let invalid_result = timeout(Duration::from_secs(5), invalid_result)
        .await
        .map_err(|error| eyre!(error))?;
    assert!(
        invalid_result.is_err(),
        "a failed flush must not turn an invalid signature into success"
    );

    Ok(())
}
