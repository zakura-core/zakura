//! Transaction downloader and verifier.
//!
//! The main struct [`Downloads`] allows downloading and verifying transactions.
//! It is used by the mempool to get transactions into it. It is also able to
//! just verify transactions that were directly pushed.
//!
//! The verification itself is done by the [`zakura_consensus`] crate.
//!
//! Verified transactions are returned to the caller in [`Downloads::poll_next`].
//! This is in contrast to the block downloader and verifiers which don't
//! return anything and forward the verified blocks to the state themselves.
//!
//! # Correctness
//!
//! The mempool downloader doesn't send verified transactions to the [`Mempool`]
//! service. So Zebra must spawn a task that regularly polls the downloader for
//! ready transactions. (To ensure that transactions propagate across the entire
//! network in each 75s block interval, the polling interval should be around
//! 5-10 seconds.)
//!
//! Polling the downloader from [`Mempool::poll_ready`] is not sufficient.
//! [`Service::poll_ready`] is only called when there is a service request.
//! But we want to download and gossip transactions,
//! even when there are no other service requests.
//!
//! [`Mempool`]: super::Mempool
//! [`Mempool::poll_ready`]: super::Mempool::poll_ready
use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures::{
    future::TryFutureExt,
    ready,
    stream::{FuturesUnordered, Stream},
    FutureExt,
};
use pin_project::{pin_project, pinned_drop};
use thiserror::Error;
use tokio::{sync::oneshot, task::JoinHandle};
use tower::{Service, ServiceExt};
use tracing_futures::Instrument;

use zakura_chain::{
    block::Height,
    transaction::{self, UnminedTxId, VerifiedUnminedTx},
    transparent,
};
use zakura_consensus::transaction as tx;
use zakura_network::{self as zn, PeerSocketAddr};
use zakura_node_services::mempool::{Gossip, QueueSource};
use zakura_state::{self as zs, CloneError};

use crate::components::{
    mempool::crawler::RATE_LIMIT_DELAY,
    sync::{BLOCK_DOWNLOAD_TIMEOUT, BLOCK_VERIFY_TIMEOUT},
};

use super::{storage::NonStandardTransactionError, MempoolError};

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

fn peer_source_from_queue_source(source: &QueueSource) -> Option<zn::PeerSource> {
    match source {
        QueueSource::LegacySocket(_) => None,
        QueueSource::Zakura(peer_id) => zn::zakura::ZakuraPeerId::new(peer_id.clone())
            .ok()
            .map(zn::PeerSource::Zakura),
    }
}

/// Returns the peer address to attribute a peer-pushed transaction's verification
/// failure to, for the mempool misbehavior channel.
///
/// Only legacy-socket peers carry a routable [`PeerSocketAddr`], which is the key
/// the misbehavior channel bans on. Zakura peers yield `None`, matching the
/// advertised-download path, which also delivers Zakura-served transactions with
/// no advertiser address (see `legacy_gossip.rs`).
fn misbehavior_addr_from_queue_source(source: &QueueSource) -> Option<PeerSocketAddr> {
    match source {
        QueueSource::LegacySocket(addr) => Some(PeerSocketAddr::from(*addr)),
        QueueSource::Zakura(_) => None,
    }
}

/// Controls how long we wait for a transaction download request to complete.
///
/// This is currently equal to [`BLOCK_DOWNLOAD_TIMEOUT`] for
/// consistency, even though parts of the rationale used for defining the value
/// don't apply here (e.g. we can drop transactions hashes when the queue is full).
pub(crate) const TRANSACTION_DOWNLOAD_TIMEOUT: Duration = BLOCK_DOWNLOAD_TIMEOUT;

/// Controls how long we wait for a transaction verify request to complete.
///
/// This is currently equal to [`BLOCK_VERIFY_TIMEOUT`] for
/// consistency.
///
/// This timeout may lead to denial of service, which will be handled in
/// [#2694](https://github.com/ZcashFoundation/zebra/issues/2694)
pub(crate) const TRANSACTION_VERIFY_TIMEOUT: Duration = BLOCK_VERIFY_TIMEOUT;

/// The maximum number of concurrent inbound download and verify tasks.
///
/// We expect the mempool crawler to download and verify most mempool transactions, so this bound
/// can be small. But it should be at least the default `network.peerset_initial_target_size` config,
/// to avoid disconnecting peers on startup.
///
/// ## Security
///
/// We use a small concurrency limit, to prevent memory denial-of-service
/// attacks.
///
/// The maximum transaction size is 2 million bytes. A deserialized malicious
/// transaction with ~225_000 transparent outputs can take up 9MB of RAM.
/// (See #1880 for more details.)
///
/// Malicious transactions will eventually timeout or fail validation.
/// Once validation fails, the transaction is dropped, and its memory is deallocated.
///
/// Since Zebra keeps an `inv` index, inbound downloads for malicious transactions
/// will be directed to the malicious node that originally gossiped the hash.
/// Therefore, this attack can be carried out by a single malicious node.
//
// TODO: replace with the configured value of network.peerset_initial_target_size
pub const MAX_INBOUND_CONCURRENCY: usize = 500;

/// The maximum number of concurrent inbound download tasks attributable to a
/// single advertising peer.
///
/// Caps how many slots of [`MAX_INBOUND_CONCURRENCY`] one peer's `Inv`
/// advertisements can occupy, so a single peer cannot saturate the global
/// queue with fake txids and deny gossip-path mempool admission for honest
/// peers. See `GHSA-4fc2-h7jh-287c`. Crawler-driven and locally-pushed
/// transactions have no source peer and are not counted against the cap.
pub const MAX_INBOUND_CONCURRENCY_PER_PEER: usize = 5;

/// A marker struct for the oneshot channels which cancel a pending download and verify.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct CancelDownloadAndVerify;

/// Errors that can occur while downloading and verifying a transaction.
#[derive(Error, Debug, Clone)]
#[allow(dead_code)]
pub enum TransactionDownloadVerifyError {
    #[error("transaction is already in state")]
    InState,

    #[error("error in state service: {0}")]
    StateError(#[source] CloneError),

    #[error("error downloading transaction: {0}")]
    DownloadFailed(#[source] CloneError),

    #[error("transaction download / verification was cancelled")]
    Cancelled,

    #[error("transaction did not pass mempool policy: {0}")]
    PolicyRejected(#[source] NonStandardTransactionError),

    #[error("transaction did not pass consensus validation: {error}")]
    Invalid {
        error: zakura_consensus::error::TransactionError,
        advertiser_addr: Option<PeerSocketAddr>,
    },
}

/// Represents a [`Stream`] of download and verification tasks.
#[pin_project(PinnedDrop)]
#[derive(Debug)]
pub struct Downloads<ZN, ZV, ZS>
where
    ZN: Service<zn::Request, Response = zn::Response, Error = BoxError> + Send + Clone + 'static,
    ZN::Future: Send,
    ZV: Service<tx::Request, Response = tx::Response, Error = BoxError> + Send + Clone + 'static,
    ZV::Future: Send,
    ZS: Service<zs::Request, Response = zs::Response, Error = BoxError> + Send + Clone + 'static,
    ZS::Future: Send,
{
    // Services
    /// A service that forwards requests to connected peers, and returns their
    /// responses.
    network: ZN,

    /// A service that verifies downloaded transactions.
    verifier: ZV,

    /// A service that manages cached blockchain state.
    state: ZS,

    /// The maximum serialized size of a transaction accepted into the mempool.
    max_transaction_bytes: u64,

    // Internal downloads state
    /// A list of pending transaction download and verify tasks.
    #[pin]
    pending: FuturesUnordered<
        JoinHandle<
            Result<
                Result<
                    (
                        VerifiedUnminedTx,
                        Vec<transparent::OutPoint>,
                        Option<Height>,
                        Option<oneshot::Sender<Result<(), BoxError>>>,
                    ),
                    Box<(TransactionDownloadVerifyError, UnminedTxId)>,
                >,
                (UnminedTxId, tokio::time::error::Elapsed),
            >,
        >,
    >,

    /// A list of channels that can be used to cancel pending transaction
    /// download and verify tasks. Each entry also stores the corresponding
    /// gossip request and the announcing peer (when known), so completion can
    /// release the per-peer slot by `UnminedTxId` lookup.
    cancel_handles: HashMap<
        UnminedTxId,
        (
            oneshot::Sender<CancelDownloadAndVerify>,
            Gossip,
            Option<QueueSource>,
        ),
    >,

    /// The number of currently in-flight download tasks per advertising peer.
    ///
    /// Invariant: a peer is present here iff some entry in [`Self::cancel_handles`]
    /// has it as the third tuple element. Enforces
    /// [`MAX_INBOUND_CONCURRENCY_PER_PEER`]. See `GHSA-4fc2-h7jh-287c`.
    pending_per_peer: HashMap<QueueSource, usize>,
}

impl<ZN, ZV, ZS> Stream for Downloads<ZN, ZV, ZS>
where
    ZN: Service<zn::Request, Response = zn::Response, Error = BoxError> + Send + Clone + 'static,
    ZN::Future: Send,
    ZV: Service<tx::Request, Response = tx::Response, Error = BoxError> + Send + Clone + 'static,
    ZV::Future: Send,
    ZS: Service<zs::Request, Response = zs::Response, Error = BoxError> + Send + Clone + 'static,
    ZS::Future: Send,
{
    type Item = Result<
        Result<
            (
                VerifiedUnminedTx,
                Vec<transparent::OutPoint>,
                Option<Height>,
                Option<oneshot::Sender<Result<(), BoxError>>>,
            ),
            Box<(UnminedTxId, TransactionDownloadVerifyError)>,
        >,
        (UnminedTxId, tokio::time::error::Elapsed),
    >;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let this = self.project();
        // CORRECTNESS
        //
        // The current task must be scheduled for wakeup every time we return
        // `Poll::Pending`.
        //
        // If no download and verify tasks have exited since the last poll, this
        // task is scheduled for wakeup when the next task becomes ready.
        //
        // TODO: this would be cleaner with poll_map (#2693)
        let item = if let Some(join_result) = ready!(this.pending.poll_next(cx)) {
            let result = join_result.expect("transaction download and verify tasks must not panic");
            let (result, completed_txid) = match result {
                Ok(Ok((tx, spent_mempool_outpoints, tip_height, rsp_tx))) => {
                    let hash = tx.transaction.id;
                    (
                        Ok(Ok((tx, spent_mempool_outpoints, tip_height, rsp_tx))),
                        Some(hash),
                    )
                }
                Ok(Err(boxed_err)) => {
                    let (e, hash) = *boxed_err;
                    (Ok(Err(Box::new((hash, e)))), Some(hash))
                }
                Err((txid, elapsed)) => {
                    // Remove the cancel handle so the spawned task's queued `Gossip`
                    // doesn't stay resident in `cancel_handles` after a verification
                    // timeout. Without this, a peer that gets each transaction to
                    // hit `RATE_LIMIT_DELAY` can leak ~2 MB per tx until OOM.
                    if let Some((_, _gossip, Some(source))) = this.cancel_handles.remove(&txid) {
                        Self::release_peer_slot(this.pending_per_peer, source);
                    }
                    (Err((txid, elapsed)), None)
                }
            };

            if let Some(hash) = completed_txid {
                if let Some((_, _gossip, Some(source))) = this.cancel_handles.remove(&hash) {
                    Self::release_peer_slot(this.pending_per_peer, source);
                }
            }

            Some(result)
        } else {
            None
        };

        Poll::Ready(item)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.pending.size_hint()
    }
}

impl<ZN, ZV, ZS> Downloads<ZN, ZV, ZS>
where
    ZN: Service<zn::Request, Response = zn::Response, Error = BoxError> + Send + Clone + 'static,
    ZN::Future: Send,
    ZV: Service<tx::Request, Response = tx::Response, Error = BoxError> + Send + Clone + 'static,
    ZV::Future: Send,
    ZS: Service<zs::Request, Response = zs::Response, Error = BoxError> + Send + Clone + 'static,
    ZS::Future: Send,
{
    /// Initialize a new download stream with the provided services.
    ///
    /// `network` is used to download transactions.
    /// `verifier` is used to verify transactions.
    /// `state` is used to check if transactions are already in the state.
    ///
    /// The [`Downloads`] stream is agnostic to the network policy, so retry and
    /// timeout limits should be applied to the `network` service passed into
    /// this constructor.
    pub fn new(network: ZN, verifier: ZV, state: ZS, max_transaction_bytes: u64) -> Self {
        Self {
            network,
            verifier,
            state,
            max_transaction_bytes,
            pending: FuturesUnordered::new(),
            cancel_handles: HashMap::new(),
            pending_per_peer: HashMap::new(),
        }
    }

    /// Queue a transaction for download (if needed) and verification.
    ///
    /// Returns the action taken in response to the queue request.
    ///
    /// When `source` is `Some`, the per-peer cap
    /// [`MAX_INBOUND_CONCURRENCY_PER_PEER`] is enforced; crawler-driven and
    /// locally-pushed transactions pass `None` and are not capped per peer.
    #[instrument(skip(self, gossiped_tx), fields(txid = %gossiped_tx.id()))]
    #[allow(clippy::unwrap_in_result)]
    pub fn download_if_needed_and_verify(
        &mut self,
        gossiped_tx: Gossip,
        source: Option<QueueSource>,
        mut rsp_tx: Option<oneshot::Sender<Result<(), BoxError>>>,
    ) -> Result<(), MempoolError> {
        let txid = gossiped_tx.id();

        if self.cancel_handles.contains_key(&txid) {
            debug!(
                ?txid,
                queue_len = self.pending.len(),
                ?MAX_INBOUND_CONCURRENCY,
                "transaction id already queued for inbound download: ignored transaction"
            );
            metrics::gauge!("mempool.currently.queued.transactions",)
                .set(self.pending.len() as f64);

            return Err(MempoolError::AlreadyQueued);
        }

        if self.pending.len() >= MAX_INBOUND_CONCURRENCY {
            debug!(
                ?txid,
                queue_len = self.pending.len(),
                ?MAX_INBOUND_CONCURRENCY,
                "too many transactions queued for inbound download: ignored transaction"
            );
            metrics::gauge!("mempool.currently.queued.transactions",)
                .set(self.pending.len() as f64);

            return Err(MempoolError::FullQueue);
        }

        // Per-peer cap: a single advertising peer cannot saturate the queue
        // with attacker-supplied fake txids. See `GHSA-4fc2-h7jh-287c`.
        if let Some(source) = &source {
            let count = self.pending_per_peer.get(source).copied().unwrap_or(0);
            if count >= MAX_INBOUND_CONCURRENCY_PER_PEER {
                debug!(
                    ?txid,
                    peer_queue_len = count,
                    ?MAX_INBOUND_CONCURRENCY_PER_PEER,
                    "too many transactions queued for this peer: ignored transaction"
                );
                metrics::counter!("mempool.full_queue.per_peer.total").increment(1);
                return Err(MempoolError::FullQueue);
            }
        }

        // This oneshot is used to signal cancellation to the download task.
        let (cancel_tx, mut cancel_rx) = oneshot::channel::<CancelDownloadAndVerify>();

        let network = self.network.clone();
        let verifier = self.verifier.clone();
        let mut state = self.state.clone();
        let download_source = source.as_ref().and_then(peer_source_from_queue_source);
        let pushed_advertiser_addr = source.as_ref().and_then(misbehavior_addr_from_queue_source);
        let max_transaction_bytes = self.max_transaction_bytes;

        let gossiped_tx_req = gossiped_tx.clone();

        let fut = async move {
            if let Gossip::Tx(tx) = &gossiped_tx {
                Self::check_transaction_size(tx, max_transaction_bytes)?;
            }

            // Don't download/verify if the transaction is already in the best chain.
            Self::transaction_in_best_chain(&mut state, txid).await?;

            trace!(?txid, "transaction is not in best chain");

            let (tip_height, next_height) = match state.oneshot(zs::Request::Tip).await {
                Ok(zs::Response::Tip(None)) => Ok((None, Height(0))),
                Ok(zs::Response::Tip(Some((height, _hash)))) => {
                    let next_height =
                        (height + 1).expect("valid heights are far below the maximum");
                    Ok((Some(height), next_height))
                }
                Ok(_) => unreachable!("wrong response"),
                Err(e) => Err(TransactionDownloadVerifyError::StateError(e.into())),
            }?;

            trace!(?txid, ?next_height, "got next height");

            let (tx, advertiser_addr) = match gossiped_tx {
                Gossip::Id(txid) => {
                    let request_ids = std::iter::once(txid).collect();
                    let req = match download_source {
                        Some(source) => zn::Request::TransactionsByIdFrom {
                            ids: request_ids,
                            source,
                        },
                        None => zn::Request::TransactionsById(request_ids),
                    };

                    let tx = match network
                        .oneshot(req)
                        .await
                        .map_err(CloneError::from)
                        .map_err(TransactionDownloadVerifyError::DownloadFailed)?
                    {
                        zn::Response::Transactions(mut txs) => txs.pop().ok_or_else(|| {
                            TransactionDownloadVerifyError::DownloadFailed(
                                BoxError::from("no transactions returned").into(),
                            )
                        })?,
                        _ => unreachable!("wrong response to transaction request"),
                    };

                    let (tx, advertiser_addr) = match tx {
                        zn::InventoryResponse::Available(tx) => tx,
                        zn::InventoryResponse::Missing(_) => {
                            return Err(TransactionDownloadVerifyError::DownloadFailed(
                                BoxError::from("transaction was missing from peer response").into(),
                            ));
                        }
                    };

                    metrics::counter!(
                        "mempool.downloaded.transactions.total",
                        "version" => format!("{}",tx.transaction.version()),
                    ).increment(1);
                    Self::check_transaction_size(&tx, max_transaction_bytes)?;
                    (tx, advertiser_addr)
                }
                Gossip::Tx(tx) => {
                    metrics::counter!(
                        "mempool.pushed.transactions.total",
                        "version" => format!("{}",tx.transaction.version()),
                    ).increment(1);
                    (tx, pushed_advertiser_addr)
                }
            };

            trace!(?txid, "got tx");

            let result = verifier
                .oneshot(tx::Request::Mempool {
                    transaction: tx.clone(),
                    height: next_height,
                })
                .map_ok(|rsp| {
                    let tx::Response::Mempool { transaction, spent_mempool_outpoints } = rsp else {
                        panic!("unexpected non-mempool response to mempool request")
                    };

                    (transaction, spent_mempool_outpoints, tip_height)
                })
                .await;

            // Hide the transaction data to avoid filling the logs
            trace!(?txid, result = ?result.as_ref().map(|_tx| ()), "verified transaction for the mempool");

            result.map_err(|e| TransactionDownloadVerifyError::Invalid { error: e.into(), advertiser_addr } )
        }
        .map_ok(|(tx, spent_mempool_outpoints, tip_height)| {
            metrics::counter!(
                "mempool.verified.transactions.total",
                "version" => format!("{}", tx.transaction.transaction.version()),
            ).increment(1);
            (tx, spent_mempool_outpoints, tip_height)
        })
        // Tack the hash onto the error so we can remove the cancel handle
        // on failure as well as on success.
        .map_err(move |e| Box::new((e, txid)))
        .inspect(move |result| {
            // Hide the transaction data to avoid filling the logs
            let result = result.as_ref().map(|_tx| txid);
            debug!("mempool transaction result: {result:?}");
        })
        .in_current_span();

        let task = tokio::spawn(async move {
            let fut = tokio::time::timeout(RATE_LIMIT_DELAY, fut);

            // Prefer the cancel handle if both are ready.
            let result = tokio::select! {
                biased;
                _ = &mut cancel_rx => {
                    trace!("task cancelled prior to completion");
                    metrics::counter!("mempool.cancelled.verify.tasks.total").increment(1);
                    if let Some(rsp_tx) = rsp_tx.take() {
                        let _ = rsp_tx.send(Err("verification cancelled".into()));
                    }

                    Ok(Err(Box::new((TransactionDownloadVerifyError::Cancelled, txid))))
                }
                verification = fut => {
                    verification
                        .inspect_err(|_elapsed| {
                            if let Some(rsp_tx) = rsp_tx.take() {
                                let _ = rsp_tx.send(Err("timeout waiting for verification result".into()));
                            }
                        })
                        .map_err(|elapsed| (txid, elapsed))
                        .map(|inner_result| {
                            match inner_result {
                                Ok((transaction, spent_mempool_outpoints, tip_height)) => Ok((transaction, spent_mempool_outpoints, tip_height, rsp_tx)),
                                Err(boxed_err) => {
                                    let (tx_verifier_error, tx_id) = *boxed_err;
                                    if let Some(rsp_tx) = rsp_tx.take() {
                                        let error_msg = format!(
                                            "failed to validate tx: {tx_id}, error: {tx_verifier_error}"
                                        );
                                        let _ = rsp_tx.send(Err(error_msg.into()));
                                    };

                                    Err(Box::new((tx_verifier_error, tx_id)))
                                }
                            }
                        })
                },
            };

            result
        });

        self.pending.push(task);
        if let Some(source) = &source {
            // The per-peer cap check above ensures this can't exceed
            // `MAX_INBOUND_CONCURRENCY_PER_PEER`.
            *self.pending_per_peer.entry(source.clone()).or_insert(0) += 1;
        }
        assert!(
            self.cancel_handles
                .insert(txid, (cancel_tx, gossiped_tx_req, source))
                .is_none(),
            "transactions are only queued once"
        );

        debug!(
            ?txid,
            queue_len = self.pending.len(),
            ?MAX_INBOUND_CONCURRENCY,
            "queued transaction hash for download"
        );
        metrics::gauge!("mempool.currently.queued.transactions",).set(self.pending.len() as f64);
        metrics::counter!("mempool.queued.transactions.total").increment(1);

        Ok(())
    }

    /// Cancel download/verification tasks of transactions with the
    /// given transaction hash (see [`UnminedTxId::mined_id`]).
    pub fn cancel(&mut self, mined_ids: &HashSet<transaction::Hash>) {
        // TODO: this can be simplified with [`HashMap::drain_filter`] which
        // is currently nightly-only experimental API.
        let removed_txids: Vec<UnminedTxId> = self
            .cancel_handles
            .keys()
            .filter(|txid| mined_ids.contains(&txid.mined_id()))
            .cloned()
            .collect();

        for txid in removed_txids {
            if let Some((cancel_tx, _gossip, source)) = self.cancel_handles.remove(&txid) {
                let _ = cancel_tx.send(CancelDownloadAndVerify);
                if let Some(source) = source {
                    Self::release_peer_slot(&mut self.pending_per_peer, source);
                }
            }
        }
    }

    /// Cancel all running tasks and reset the downloader state.
    // Note: copied from zakurad/src/components/sync/downloads.rs
    pub fn cancel_all(&mut self) {
        // Replace the pending task list with an empty one and drop it.
        let _ = std::mem::take(&mut self.pending);
        // Signal cancellation to all running tasks.
        // Since we already dropped the JoinHandles above, they should
        // fail silently.
        for (_hash, (cancel_tx, _gossip, _source)) in self.cancel_handles.drain() {
            let _ = cancel_tx.send(CancelDownloadAndVerify);
        }
        self.pending_per_peer.clear();
        assert!(self.pending.is_empty());
        assert!(self.cancel_handles.is_empty());
        metrics::gauge!("mempool.currently.queued.transactions",).set(self.pending.len() as f64);
    }

    /// Decrement the per-peer pending count for `source`, removing the entry
    /// when it reaches zero.
    fn release_peer_slot(pending_per_peer: &mut HashMap<QueueSource, usize>, source: QueueSource) {
        if let Some(count) = pending_per_peer.get_mut(&source) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                pending_per_peer.remove(&source);
            }
        }
    }

    /// Get the number of currently in-flight download tasks.
    #[allow(dead_code)]
    pub fn in_flight(&self) -> usize {
        self.pending.len()
    }

    /// Get a list of the currently pending transaction requests.
    pub fn transaction_requests(&self) -> impl Iterator<Item = &Gossip> {
        self.cancel_handles
            .iter()
            .map(|(_tx_id, (_handle, tx, _source))| tx)
    }

    /// Reject transactions that exceed the configured serialized size limit.
    fn check_transaction_size(
        transaction: &transaction::UnminedTx,
        max_transaction_bytes: u64,
    ) -> Result<(), TransactionDownloadVerifyError> {
        if usize::try_from(max_transaction_bytes)
            .is_ok_and(|max_transaction_bytes| transaction.size > max_transaction_bytes)
        {
            return Err(TransactionDownloadVerifyError::PolicyRejected(
                NonStandardTransactionError::TransactionTooLarge {
                    actual_bytes: transaction.size,
                    max_bytes: max_transaction_bytes,
                },
            ));
        }

        Ok(())
    }

    /// Check if transaction is already in the best chain.
    async fn transaction_in_best_chain(
        state: &mut ZS,
        txid: UnminedTxId,
    ) -> Result<(), TransactionDownloadVerifyError> {
        match state
            .ready()
            .await
            .map_err(CloneError::from)
            .map_err(TransactionDownloadVerifyError::StateError)?
            .call(zs::Request::Transaction(txid.mined_id()))
            .await
        {
            Ok(zs::Response::Transaction(None)) => Ok(()),
            Ok(zs::Response::Transaction(Some(_))) => Err(TransactionDownloadVerifyError::InState),
            Ok(_) => unreachable!("wrong response"),
            Err(e) => Err(TransactionDownloadVerifyError::StateError(e.into())),
        }?;

        Ok(())
    }
}

#[pinned_drop]
impl<ZN, ZV, ZS> PinnedDrop for Downloads<ZN, ZV, ZS>
where
    ZN: Service<zn::Request, Response = zn::Response, Error = BoxError> + Send + Clone + 'static,
    ZN::Future: Send,
    ZV: Service<tx::Request, Response = tx::Response, Error = BoxError> + Send + Clone + 'static,
    ZV::Future: Send,
    ZS: Service<zs::Request, Response = zs::Response, Error = BoxError> + Send + Clone + 'static,
    ZS::Future: Send,
{
    fn drop(mut self: Pin<&mut Self>) {
        self.cancel_all();

        metrics::gauge!("mempool.currently.queued.transactions").set(0 as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt as _;
    use std::{
        future,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };
    use tower::{service_fn, util::BoxCloneService};

    type PendingNetwork = BoxCloneService<zn::Request, zn::Response, BoxError>;
    type PendingVerifier = BoxCloneService<tx::Request, tx::Response, BoxError>;
    type PendingState = BoxCloneService<zs::Request, zs::Response, BoxError>;

    fn tx_id(index: u64) -> UnminedTxId {
        let mut bytes = [0; 32];
        bytes[..8].copy_from_slice(&index.to_le_bytes());
        UnminedTxId::from_legacy_id(transaction::Hash(bytes))
    }

    fn empty_v5_transaction(byte: u8) -> transaction::UnminedTx {
        use zakura_chain::{
            block,
            parameters::NetworkUpgrade,
            transaction::{LockTime, Transaction},
        };

        transaction::UnminedTx::from(Transaction::V5 {
            network_upgrade: NetworkUpgrade::Nu5,
            lock_time: LockTime::min_lock_time_timestamp(),
            expiry_height: block::Height(u32::from(byte)),
            inputs: Vec::new(),
            outputs: Vec::new(),
            sapling_shielded_data: None,
            orchard_shielded_data: None,
        })
    }

    fn pending_downloads() -> Downloads<PendingNetwork, PendingVerifier, PendingState> {
        Downloads::new(
            BoxCloneService::new(service_fn(|_request| {
                future::pending::<Result<zn::Response, BoxError>>()
            })),
            BoxCloneService::new(service_fn(|_request| {
                future::pending::<Result<tx::Response, BoxError>>()
            })),
            BoxCloneService::new(service_fn(|_request| {
                future::pending::<Result<zs::Response, BoxError>>()
            })),
            u64::MAX,
        )
    }

    #[tokio::test]
    async fn zakura_queue_source_is_counted_by_per_peer_cap() {
        let mut downloads = pending_downloads();
        let zakura_source = QueueSource::Zakura(vec![7; 32]);

        for index in 0..MAX_INBOUND_CONCURRENCY_PER_PEER {
            downloads
                .download_if_needed_and_verify(
                    Gossip::Id(tx_id(u64::try_from(index).expect("test index fits u64"))),
                    Some(zakura_source.clone()),
                    None,
                )
                .expect("within per-peer cap");
        }

        assert!(matches!(
            downloads.download_if_needed_and_verify(
                Gossip::Id(tx_id(100)),
                Some(zakura_source.clone()),
                None,
            ),
            Err(MempoolError::FullQueue)
        ));

        downloads
            .download_if_needed_and_verify(
                Gossip::Id(tx_id(101)),
                Some(QueueSource::Zakura(vec![8; 32])),
                None,
            )
            .expect("different Zakura peer has a separate cap");
        downloads
            .download_if_needed_and_verify(
                Gossip::Id(tx_id(102)),
                Some(QueueSource::LegacySocket(([127, 0, 0, 1], 8233).into())),
                None,
            )
            .expect("legacy socket has a separate cap");
        downloads.cancel_all();
    }

    #[tokio::test]
    async fn zakura_queue_source_is_sent_to_network_download_request() {
        let txid = tx_id(7);
        let peer_id = vec![7; 32];
        let (network_tx, mut network_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut downloads = Downloads::new(
            BoxCloneService::new(service_fn(move |request| {
                let network_tx = network_tx.clone();
                async move {
                    network_tx.send(request)?;
                    future::pending::<Result<zn::Response, BoxError>>().await
                }
            })),
            BoxCloneService::new(service_fn(|_request| {
                future::pending::<Result<tx::Response, BoxError>>()
            })),
            BoxCloneService::new(service_fn(|request| async move {
                match request {
                    zs::Request::Transaction(_) => Ok(zs::Response::Transaction(None)),
                    zs::Request::Tip => Ok(zs::Response::Tip(None)),
                    request => Err(format!("unexpected state request: {request:?}").into()),
                }
            })),
            u64::MAX,
        );

        downloads
            .download_if_needed_and_verify(
                Gossip::Id(txid),
                Some(QueueSource::Zakura(peer_id.clone())),
                None,
            )
            .expect("download is queued");
        let poll_task = tokio::spawn(async move {
            let _ = downloads.next().await;
        });
        let request = tokio::time::timeout(Duration::from_secs(1), network_rx.recv())
            .await
            .expect("network request is sent")
            .expect("network request channel is open");
        assert_eq!(
            request,
            zn::Request::TransactionsByIdFrom {
                ids: HashSet::from([txid]),
                source: zn::PeerSource::Zakura(
                    zn::zakura::ZakuraPeerId::new(peer_id).expect("test peer id is within bounds")
                ),
            }
        );
        poll_task.abort();
    }

    #[tokio::test]
    async fn missing_transaction_response_is_download_failure() {
        let txid = tx_id(7);
        let mut downloads = Downloads::new(
            BoxCloneService::new(service_fn(move |request| async move {
                assert!(matches!(request, zn::Request::TransactionsById(_)));
                Ok(zn::Response::Transactions(vec![
                    zn::InventoryResponse::Missing(txid),
                ]))
            })),
            BoxCloneService::new(service_fn(|_request| async move {
                panic!("missing transaction responses must not be verified");
            })),
            BoxCloneService::new(service_fn(|request| async move {
                match request {
                    zs::Request::Transaction(_) => Ok(zs::Response::Transaction(None)),
                    zs::Request::Tip => Ok(zs::Response::Tip(None)),
                    request => Err(format!("unexpected state request: {request:?}").into()),
                }
            })),
            u64::MAX,
        );

        downloads
            .download_if_needed_and_verify(Gossip::Id(txid), None, None)
            .expect("download is queued");

        let result = tokio::time::timeout(Duration::from_secs(1), downloads.next())
            .await
            .expect("missing transaction response should complete")
            .expect("download stream should yield an item")
            .expect("missing transaction response should not time out");

        assert!(matches!(
            result,
            Err(error)
                if error.0 == txid
                    && matches!(error.1, TransactionDownloadVerifyError::DownloadFailed(_))
        ));
    }

    #[tokio::test]
    async fn pushed_transaction_at_size_limit_is_verified() {
        let transaction = empty_v5_transaction(1);
        let max_transaction_bytes = u64::try_from(transaction.size)
            .expect("serialized transaction sizes fit in u64 on supported platforms");
        let verifier_calls = Arc::new(AtomicUsize::new(0));
        let verifier_calls_for_service = verifier_calls.clone();

        let mut downloads = Downloads::new(
            BoxCloneService::new(service_fn(|_request| async move {
                panic!("pushed transactions must not be downloaded");
            })),
            BoxCloneService::new(service_fn(move |request| {
                let verifier_calls = verifier_calls_for_service.clone();
                async move {
                    verifier_calls.fetch_add(1, Ordering::SeqCst);
                    let tx::Request::Mempool { transaction, .. } = request else {
                        panic!("unexpected transaction verifier request: {request:?}");
                    };
                    let miner_fee = transaction.conventional_fee;
                    let transaction =
                        VerifiedUnminedTx::new(transaction, miner_fee, 0, 0, Arc::new(Vec::new()))
                            .expect("test transaction pays its conventional fee");

                    Ok::<_, BoxError>(tx::Response::Mempool {
                        transaction,
                        spent_mempool_outpoints: Vec::new(),
                    })
                }
            })),
            BoxCloneService::new(service_fn(|request| async move {
                match request {
                    zs::Request::Transaction(_) => Ok(zs::Response::Transaction(None)),
                    zs::Request::Tip => Ok(zs::Response::Tip(None)),
                    request => Err(format!("unexpected state request: {request:?}").into()),
                }
            })),
            max_transaction_bytes,
        );

        downloads
            .download_if_needed_and_verify(Gossip::Tx(transaction), None, None)
            .expect("transaction at the configured limit is queued");

        let result = tokio::time::timeout(Duration::from_secs(1), downloads.next())
            .await
            .expect("pushed transaction should complete")
            .expect("download stream should yield an item")
            .expect("pushed transaction should not time out");

        assert!(result.is_ok(), "transaction at the limit should verify");
        assert_eq!(verifier_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn pushed_transaction_over_size_limit_skips_state_and_verifier() {
        let transaction = empty_v5_transaction(1);
        let actual_bytes = transaction.size;
        let max_bytes = u64::try_from(
            actual_bytes
                .checked_sub(1)
                .expect("test transaction is not empty"),
        )
        .expect("serialized transaction sizes fit in u64 on supported platforms");
        let state_calls = Arc::new(AtomicUsize::new(0));
        let state_calls_for_service = state_calls.clone();
        let verifier_calls = Arc::new(AtomicUsize::new(0));
        let verifier_calls_for_service = verifier_calls.clone();
        let peer_addr = PeerSocketAddr::from(([203, 0, 113, 7], 8233));
        let (rsp_tx, rsp_rx) = oneshot::channel();

        let mut downloads = Downloads::new(
            BoxCloneService::new(service_fn(|_request| async move {
                panic!("pushed transactions must not be downloaded");
            })),
            BoxCloneService::new(service_fn(move |_request| {
                let verifier_calls = verifier_calls_for_service.clone();
                async move {
                    verifier_calls.fetch_add(1, Ordering::SeqCst);
                    panic!("oversized pushed transactions must not be verified");
                }
            })),
            BoxCloneService::new(service_fn(move |_request| {
                let state_calls = state_calls_for_service.clone();
                async move {
                    state_calls.fetch_add(1, Ordering::SeqCst);
                    panic!("oversized pushed transactions must not query state");
                }
            })),
            max_bytes,
        );

        downloads
            .download_if_needed_and_verify(
                Gossip::Tx(transaction),
                Some(QueueSource::LegacySocket(
                    peer_addr.remove_socket_addr_privacy(),
                )),
                Some(rsp_tx),
            )
            .expect("oversized transaction policy check is queued");

        let result = tokio::time::timeout(Duration::from_secs(1), downloads.next())
            .await
            .expect("pushed transaction should complete")
            .expect("download stream should yield an item")
            .expect("pushed transaction should not time out");
        let error = result
            .expect_err("oversized pushed transaction should be rejected")
            .1;

        assert!(matches!(
            error,
            TransactionDownloadVerifyError::PolicyRejected(
                NonStandardTransactionError::TransactionTooLarge {
                    actual_bytes: error_actual_bytes,
                    max_bytes: error_max_bytes,
                }
            ) if error_actual_bytes == actual_bytes && error_max_bytes == max_bytes
        ));
        assert_eq!(state_calls.load(Ordering::SeqCst), 0);
        assert_eq!(verifier_calls.load(Ordering::SeqCst), 0);

        let responder_error = rsp_rx
            .await
            .expect("responder should receive the policy result")
            .expect_err("responder should receive a policy rejection")
            .to_string();
        assert!(responder_error.contains(&format!(
            "transaction is {actual_bytes} bytes, exceeding the configured mempool maximum of {max_bytes} bytes"
        )));
    }

    #[tokio::test]
    async fn downloaded_transaction_over_size_limit_skips_verifier() {
        let transaction = empty_v5_transaction(1);
        let txid = transaction.id;
        let actual_bytes = transaction.size;
        let max_bytes = u64::try_from(
            actual_bytes
                .checked_sub(1)
                .expect("test transaction is not empty"),
        )
        .expect("serialized transaction sizes fit in u64 on supported platforms");
        let verifier_calls = Arc::new(AtomicUsize::new(0));
        let verifier_calls_for_service = verifier_calls.clone();

        let mut downloads = Downloads::new(
            BoxCloneService::new(service_fn(move |request| {
                let transaction = transaction.clone();
                async move {
                    assert!(matches!(request, zn::Request::TransactionsById(_)));
                    Ok::<_, BoxError>(zn::Response::Transactions(vec![
                        zn::InventoryResponse::Available((transaction, None)),
                    ]))
                }
            })),
            BoxCloneService::new(service_fn(move |_request| {
                let verifier_calls = verifier_calls_for_service.clone();
                async move {
                    verifier_calls.fetch_add(1, Ordering::SeqCst);
                    panic!("oversized downloaded transactions must not be verified");
                }
            })),
            BoxCloneService::new(service_fn(|request| async move {
                match request {
                    zs::Request::Transaction(_) => Ok(zs::Response::Transaction(None)),
                    zs::Request::Tip => Ok(zs::Response::Tip(None)),
                    request => Err(format!("unexpected state request: {request:?}").into()),
                }
            })),
            max_bytes,
        );

        downloads
            .download_if_needed_and_verify(Gossip::Id(txid), None, None)
            .expect("oversized advertised transaction is queued");

        let result = tokio::time::timeout(Duration::from_secs(1), downloads.next())
            .await
            .expect("downloaded transaction should complete")
            .expect("download stream should yield an item")
            .expect("downloaded transaction should not time out");
        let error = result
            .expect_err("oversized downloaded transaction should be rejected")
            .1;

        assert!(matches!(
            error,
            TransactionDownloadVerifyError::PolicyRejected(
                NonStandardTransactionError::TransactionTooLarge {
                    actual_bytes: error_actual_bytes,
                    max_bytes: error_max_bytes,
                }
            ) if error_actual_bytes == actual_bytes && error_max_bytes == max_bytes
        ));
        assert_eq!(verifier_calls.load(Ordering::SeqCst), 0);
    }

    /// A directly pushed transaction from a legacy-socket peer must keep that
    /// peer's address on the `Invalid` verification error, so the mempool can
    /// score the peer's misbehavior. Regression test for the push-path
    /// attribution gap.
    #[tokio::test]
    async fn pushed_transaction_attributes_invalid_error_to_peer() {
        use zakura_consensus::error::TransactionError;

        let peer_addr = PeerSocketAddr::from(([203, 0, 113, 7], 8233));
        let transaction = empty_v5_transaction(1);

        let mut downloads = Downloads::new(
            // The network service must never be called for a pushed transaction.
            BoxCloneService::new(service_fn(|_request| async move {
                panic!("pushed transactions must not be downloaded");
            })),
            // Reject with a consensus error that carries a nonzero misbehavior score.
            BoxCloneService::new(service_fn(|_request| async move {
                Err(Box::new(TransactionError::WrongVersion) as BoxError)
            })),
            BoxCloneService::new(service_fn(|request| async move {
                match request {
                    zs::Request::Transaction(_) => Ok(zs::Response::Transaction(None)),
                    zs::Request::Tip => Ok(zs::Response::Tip(None)),
                    request => Err(format!("unexpected state request: {request:?}").into()),
                }
            })),
            u64::MAX,
        );

        downloads
            .download_if_needed_and_verify(
                Gossip::Tx(transaction),
                Some(QueueSource::LegacySocket(
                    peer_addr.remove_socket_addr_privacy(),
                )),
                None,
            )
            .expect("download is queued");

        let result = tokio::time::timeout(Duration::from_secs(1), downloads.next())
            .await
            .expect("pushed transaction should complete")
            .expect("download stream should yield an item")
            .expect("pushed transaction should not time out");

        let error = result
            .expect_err("invalid pushed transaction should fail verification")
            .1;
        assert!(
            matches!(
                error,
                TransactionDownloadVerifyError::Invalid {
                    advertiser_addr: Some(addr),
                    ..
                } if addr == peer_addr
            ),
            "expected the pushed transaction failure to carry the peer address, got {error:?}"
        );
    }
}
