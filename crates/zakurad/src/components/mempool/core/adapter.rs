//! The `mempool::Request` service, served on top of the pool core's port.
//!
//! Every component that takes the mempool calls this, so its answers are
//! today's pool's answers: the boundary tests in `tests/boundary.rs` pin
//! them. Queries read the published snapshot. Submissions reserve a slot
//! first, fetch an advertised id only under that reservation, and submit
//! the body, so the core's limits count a transaction from its fetch to its
//! verdict, as today's one count does.
//!
//! One answer changes on purpose. A body whose id is already being verified
//! joins that verification and gets its verdict at the second stage, up to
//! the core's waiter limit, where today's pool refuses it at the first stage
//! as already queued.

use std::{
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::FutureExt;
use tokio::sync::{mpsc, oneshot};
use tower::{timeout::Timeout, Service};

use zakura_chain::transaction::UnminedTxId;
use zakura_network::{self as zn, PeerSocketAddr};
use zakura_node_services::mempool::{
    port::{
        AwaitOutputError, Charge, Intent, LocalFault, PoolHandle, PoolSnapshot, Reservation,
        Source, Supplier, Unreserved, Verdict,
    },
    CreatedOrSpent, Gossip, MempoolDisabledError, QueueSource, Request, Response,
};

use crate::BoxError;

use super::{
    super::{
        downloads::{
            fetch_transaction, peer_source_from_queue_source, TRANSACTION_DOWNLOAD_TIMEOUT,
        },
        pending_outputs::PendingOutputsFull,
        queue_source_log_label, ExactTipRejectionError, MempoolError, Outbound,
    },
    Exempt,
};
use zakura_mempool::storage::NonStandardTransactionError;

/// A queued item's second answer.
type Receiver = oneshot::Receiver<Result<(), BoxError>>;

/// What every submission task shares.
struct Shared {
    handle: PoolHandle,
    outbound: Timeout<Outbound>,
    expose_peer_addresses: bool,
    misbehavior_sender: mpsc::Sender<(PeerSocketAddr, u32)>,
    exempt: Arc<Exempt>,
}

/// The `mempool::Request` service in core mode.
#[derive(Clone)]
pub(crate) struct CoreMempool {
    shared: Arc<Shared>,
}

impl CoreMempool {
    /// A service on `handle` that fetches through `outbound` and bans through
    /// `misbehavior_sender`. It never charges a peer in `exempt`.
    pub fn new(
        handle: PoolHandle,
        outbound: Timeout<Outbound>,
        expose_peer_addresses: bool,
        misbehavior_sender: mpsc::Sender<(PeerSocketAddr, u32)>,
        exempt: Arc<Exempt>,
    ) -> Self {
        Self {
            shared: Arc::new(Shared {
                handle,
                outbound,
                expose_peer_addresses,
                misbehavior_sender,
                exempt,
            }),
        }
    }
}

impl Service<Request> for CoreMempool {
    type Response = Response;
    type Error = BoxError;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let shared = self.shared.clone();
        let snapshot = shared.handle.snapshot().filter(|snapshot| snapshot.active);
        match snapshot {
            Some(snapshot) => answer(shared, snapshot, request).boxed(),
            None => async move { inactive_answer(request) }.boxed(),
        }
    }
}

/// Today's answer to `request` while the pool is off.
fn inactive_answer(request: Request) -> Result<Response, BoxError> {
    // An error would make a peer connection disconnect, so queries get empty
    // answers. The requests that need the pool get today's error.
    Ok(match request {
        Request::TransactionIds | Request::TakePendingGossipTransactionIds { .. } => {
            Response::TransactionIds(Default::default())
        }
        Request::TransactionsById(_) | Request::TransactionsByMinedId(_) => {
            Response::Transactions(Default::default())
        }
        Request::TransactionWithDepsByMinedId(_)
        | Request::AwaitOutput(_)
        | Request::UnspentOutput(_)
        | Request::FullTransactions => return Err(MempoolDisabledError.into()),
        Request::RejectedTransactionIds(_) => Response::RejectedTransactionIds(Default::default()),
        Request::Queue(items) => Response::Queued(
            items
                .iter()
                .map(|_| Err(BoxError::from(MempoolError::Disabled)))
                .collect(),
        ),
        Request::QueueFromPeer { .. } => Response::Queued(Vec::new()),
        Request::CheckForVerifiedTransactions => Response::CheckedForVerifiedTransactions,
        Request::QueueStats => Response::QueueStats {
            size: 0,
            bytes: 0,
            usage: 0,
            fully_notified: None,
        },
    })
}

/// The answer to `request` while the pool is on, from `snapshot` where a
/// query can be answered from it.
async fn answer(
    shared: Arc<Shared>,
    snapshot: Arc<PoolSnapshot>,
    request: Request,
) -> Result<Response, BoxError> {
    Ok(match request {
        Request::TransactionIds => Response::TransactionIds(
            snapshot
                .transactions
                .iter()
                .map(|tx| tx.transaction.id())
                .collect(),
        ),
        // The core announces from its change log, so nothing is pending here.
        Request::TakePendingGossipTransactionIds { .. } => {
            Response::TransactionIds(Default::default())
        }
        Request::TransactionsById(ids) => Response::Transactions(
            ids.into_iter()
                .filter_map(|id| {
                    snapshot
                        .transaction(&id.mined_id())
                        .filter(|tx| tx.transaction.id() == id)
                        .map(|tx| tx.transaction.clone())
                })
                .collect(),
        ),
        Request::TransactionsByMinedId(ids) => Response::Transactions(
            ids.into_iter()
                .filter_map(|id| snapshot.transaction(&id).map(|tx| tx.transaction.clone()))
                .collect(),
        ),
        Request::TransactionWithDepsByMinedId(id) => {
            let Some(transaction) = snapshot.transaction(&id).cloned() else {
                return Err("transaction not found in mempool".into());
            };
            Response::TransactionWithDeps {
                transaction,
                dependencies: snapshot
                    .dependencies
                    .dependencies()
                    .get(&id)
                    .cloned()
                    .unwrap_or_default(),
            }
        }
        Request::AwaitOutput(outpoint) => match shared.handle.await_output(outpoint).await {
            Ok(output) => Response::UnspentOutput(output),
            Err(AwaitOutputError::TooManyWaiters) => return Err(PendingOutputsFull.into()),
            Err(AwaitOutputError::Inactive) => return Err(MempoolDisabledError.into()),
            Err(AwaitOutputError::Stopped) => return Err("the pool core has stopped".into()),
        },
        Request::FullTransactions => Response::FullTransactions {
            transactions: snapshot.transactions.clone(),
            transaction_dependencies: snapshot.dependencies.clone(),
            last_seen_tip_hash: snapshot.tip.hash,
        },
        Request::RejectedTransactionIds(ids) => {
            Response::RejectedTransactionIds(shared.handle.rejected_ids(ids).await)
        }
        Request::Queue(items) => {
            let mut answers = Vec::with_capacity(items.len());
            for item in items {
                answers.push(queue(shared.clone(), item, None).await);
            }
            Response::Queued(answers)
        }
        Request::QueueFromPeer {
            transactions,
            source,
        } => {
            tracing::trace!(
                req_count = ?transactions.len(),
                source = %queue_source_log_label(&source, shared.expose_peer_addresses),
                "got mempool QueueFromPeer request"
            );
            // Today's pool drops what it cannot queue from a peer.
            for item in transactions {
                let _ = queue(shared.clone(), item, Some(source.clone())).await;
            }
            Response::Queued(Vec::new())
        }
        Request::CheckForVerifiedTransactions => Response::CheckedForVerifiedTransactions,
        Request::QueueStats => {
            let bytes = snapshot
                .transactions
                .iter()
                .map(|tx| tx.transaction.size())
                .sum();
            Response::QueueStats {
                size: snapshot.transactions.len(),
                bytes,
                // Today's pool reports the serialized size here too.
                usage: bytes,
                fully_notified: None,
            }
        }
        Request::UnspentOutput(outpoint) => {
            if snapshot.spends(&outpoint) {
                return Ok(Response::TransparentOutput(Some(CreatedOrSpent::Spent)));
            }
            Response::TransparentOutput(snapshot.transaction(&outpoint.hash).and_then(|tx| {
                let transaction = tx.transaction.transaction();
                let index = usize::try_from(outpoint.index).ok()?;
                transaction
                    .outputs()
                    .get(index)
                    .map(|output| CreatedOrSpent::Created {
                        output: output.clone(),
                        tx_version: transaction.version(),
                        last_seen_hash: snapshot.tip.hash,
                    })
            }))
        }
    })
}

/// Queues one item: reserves its slot and answers the first stage, and
/// leaves a task that brings the body in and answers the second.
async fn queue(
    shared: Arc<Shared>,
    item: Gossip,
    from: Option<QueueSource>,
) -> Result<Receiver, BoxError> {
    let id = item.id();
    let intent = match item {
        Gossip::Tx(_) => Intent::Deliver,
        Gossip::Id(_) => Intent::Fetch,
    };
    let source = from.clone().map_or(Source::Node, Source::Peer);
    let reservation = match shared.handle.reserve(id, source, intent).await {
        Ok(reservation) => reservation,
        Err(refused) => return first_stage_refusal(refused, from.is_some()),
    };
    let (answer, receiver) = oneshot::channel();
    tokio::spawn(async move {
        let result = bring_in(&shared, reservation, item, from).await;
        let _ = answer.send(result);
    });
    Ok(receiver)
}

/// Today's first-stage answer for a refused reservation, or a completed
/// receiver where today's pool answers at the second stage.
fn first_stage_refusal(refused: Unreserved, from_peer: bool) -> Result<Receiver, BoxError> {
    let error = match refused {
        Unreserved::Inactive => MempoolError::Disabled,
        Unreserved::Pooled => MempoolError::InMempool,
        Unreserved::Pending => MempoolError::AlreadyQueued,
        Unreserved::NoRoom(_) => MempoolError::FullQueue,
        Unreserved::CoolingDown => {
            if from_peer {
                metrics::counter!("mempool.peer_cooldown.ignored.transactions.total").increment(1);
            }
            return Err("the peer that sent the transaction is in a cooldown".into());
        }
        Unreserved::Stopped => return Err("the pool core has stopped".into()),
        Unreserved::Rejected(rejection) => {
            let Some(error) = rejection.detail.downcast_ref::<MempoolError>().cloned() else {
                return Err(rejection.detail.to_string().into());
            };
            // A cached policy rejection is a completed admission result, not a
            // failure to queue the request, as today.
            if matches!(
                error,
                MempoolError::StorageExactTip(ExactTipRejectionError::FailedStandard(
                    NonStandardTransactionError::TransactionTooLarge { .. }
                ))
            ) {
                let (answer, receiver) = oneshot::channel();
                let _ = answer.send(Err(error.into()));
                return Ok(receiver);
            }
            error
        }
    };
    Err(error.into())
}

/// Brings `item`'s body in under `reservation`, submits it, applies a ban,
/// and returns today's second-stage answer.
async fn bring_in(
    shared: &Shared,
    reservation: Reservation,
    item: Gossip,
    from: Option<QueueSource>,
) -> Result<(), BoxError> {
    let tx_id = item.id();
    let (tx, supplier) = match item {
        Gossip::Tx(tx) => {
            let supplier = match &from {
                Some(QueueSource::LegacySocket(addr)) => {
                    shared.supplier(PeerSocketAddr::from(*addr))
                }
                // v2 peers through the adapter are not charged, as today.
                Some(QueueSource::Zakura(_)) | None => Supplier::Nobody,
            };
            metrics::counter!(
                "mempool.pushed.transactions.total",
                "version" => format!("{}", tx.transaction().version()),
            )
            .increment(1);
            (tx, supplier)
        }
        Gossip::Id(_) => {
            let source = from.as_ref().and_then(peer_source_from_queue_source);
            // Boxing works around a compiler limitation: without it, the
            // spawned task cannot be proven `Send`.
            match fetch_within(shared.outbound.clone(), tx_id, source).await {
                Ok((tx, served_by)) => {
                    // A v2 peer's body is not charged, as today.
                    let supplier = match (&from, served_by) {
                        (Some(QueueSource::Zakura(_)), _) | (_, None) => Supplier::Nobody,
                        (_, Some(addr)) => shared.supplier(addr),
                    };
                    (tx, supplier)
                }
                Err(error) => {
                    metrics::counter!(
                        "mempool.failed.verify.tasks.total",
                        "reason" => "download_failed",
                    )
                    .increment(1);
                    shared.handle.fetch_failed(reservation).await;
                    return Err(format!("failed to validate tx: {tx_id}, error: {error}").into());
                }
            }
        }
    };
    let verdict = shared
        .handle
        .submit(reservation, tx, supplier.clone())
        .await;
    if let (Verdict::Rejected(rejection), Supplier::Peer(QueueSource::LegacySocket(addr))) =
        (&verdict, &supplier)
    {
        if rejection.charge == Charge::Ban {
            let _ = shared.misbehavior_sender.try_send((
                PeerSocketAddr::from(*addr),
                zn::constants::MAX_PEER_MISBEHAVIOR_SCORE,
            ));
        }
    }
    second_stage(tx_id, verdict)
}

impl Shared {
    /// The supplier for a body `peer` served: the peer, unless the pool
    /// must never charge it.
    fn supplier(&self, peer: PeerSocketAddr) -> Supplier {
        if self.exempt.contains(peer) {
            return Supplier::Nobody;
        }
        let addr: SocketAddr = *peer;
        Supplier::Peer(QueueSource::LegacySocket(addr))
    }
}

/// Today's second-stage answer for `verdict`.
fn second_stage(tx_id: UnminedTxId, verdict: Verdict) -> Result<(), BoxError> {
    let failed = |reason: &dyn std::fmt::Display| -> BoxError {
        format!("failed to validate tx: {tx_id}, error: {reason}").into()
    };
    match verdict {
        Verdict::Admitted => Ok(()),
        Verdict::Rejected(rejection) => {
            let Some(error) = rejection.detail.downcast_ref::<MempoolError>() else {
                // A verifier answer that is not a pool rejection, such as a
                // transaction already in the chain.
                metrics::counter!("mempool.failed.verify.tasks.total", "reason" => "in_state")
                    .increment(1);
                return Err(failed(&rejection.detail));
            };
            match error {
                MempoolError::StorageExactTip(ExactTipRejectionError::FailedVerification(
                    error,
                )) => {
                    metrics::counter!("mempool.failed.verify.tasks.total", "reason" => "invalid")
                        .increment(1);
                    Err(failed(&format!(
                        "transaction did not pass consensus validation: {error}"
                    )))
                }
                MempoolError::StorageExactTip(ExactTipRejectionError::FailedStandard(error)) => {
                    metrics::counter!(
                        "mempool.rejected.transactions.total",
                        "reason" => "transaction_too_large"
                    )
                    .increment(1);
                    Err(failed(&format!(
                        "transaction did not pass mempool policy: {error}"
                    )))
                }
                // Today's insertion errors reach the caller as they are.
                error => Err(error.clone().into()),
            }
        }
        Verdict::NoRoom(_) => Err(MempoolError::FullQueue.into()),
        Verdict::CoolingDown => {
            metrics::counter!("mempool.peer_cooldown.ignored.transactions.total").increment(1);
            Err(failed(
                &"transaction was served by a peer in a transaction cooldown",
            ))
        }
        Verdict::LocalFault(LocalFault::VerifyTimeout) => {
            metrics::counter!("mempool.failed.verify.tasks.total", "reason" => "timeout")
                .increment(1);
            Err("timeout waiting for verification result".into())
        }
        Verdict::LocalFault(LocalFault::StateUnavailable) => {
            metrics::counter!("mempool.failed.verify.tasks.total", "reason" => "state_error")
                .increment(1);
            Err(failed(&"error in state service"))
        }
        Verdict::LocalFault(fault) => {
            Err(format!("the pool core could not decide transaction {tx_id}: {fault:?}").into())
        }
    }
}

/// Fetches `tx_id` within [`TRANSACTION_DOWNLOAD_TIMEOUT`], the wait for the
/// network included.
///
/// The service's own timeout starts only once the network is ready, and the
/// caller's reservation holds a core slot the whole time. So the whole fetch
/// has a deadline, as today's download task does, and a fetch that misses it
/// fails, which frees its slot.
async fn fetch_within(
    outbound: Timeout<Outbound>,
    tx_id: UnminedTxId,
    source: Option<zn::PeerSource>,
) -> Result<(zakura_chain::transaction::UnminedTx, Option<PeerSocketAddr>), BoxError> {
    // Boxing works around a compiler limitation: without it, the spawned
    // task cannot be proven `Send`.
    let fetch: futures::future::BoxFuture<'static, _> =
        fetch_transaction(outbound, tx_id, source).boxed();
    match tokio::time::timeout(TRANSACTION_DOWNLOAD_TIMEOUT, fetch).await {
        Ok(fetched) => fetched.map_err(BoxError::from),
        Err(elapsed) => Err(BoxError::from(elapsed)),
    }
}

#[cfg(test)]
mod tests {
    use std::task::{Context, Poll};

    use tower::util::BoxCloneSyncService;

    use super::*;

    /// A peer set that never becomes ready.
    #[derive(Clone)]
    struct NeverReady;

    impl Service<zn::Request> for NeverReady {
        type Response = zn::Response;
        type Error = zn::BoxError;
        type Future = futures::future::BoxFuture<'static, Result<zn::Response, zn::BoxError>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }

        fn call(&mut self, _request: zn::Request) -> Self::Future {
            unreachable!("a service that is never ready is never called")
        }
    }

    /// A fetch from a network that never becomes ready fails at its
    /// deadline, so the reservation it holds is freed.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_fetch_from_an_unready_network_fails_at_its_deadline() {
        let outbound = Timeout::new(
            BoxCloneSyncService::new(NeverReady),
            TRANSACTION_DOWNLOAD_TIMEOUT,
        );
        let started = tokio::time::Instant::now();
        let fetched = fetch_within(
            outbound,
            UnminedTxId::Legacy(zakura_chain::transaction::Hash([7; 32])),
            None,
        )
        .await;
        assert!(fetched.is_err(), "the fetch fails");
        assert_eq!(started.elapsed(), TRANSACTION_DOWNLOAD_TIMEOUT);
    }
}
