//! The `mempool::Request` service, served on top of the pool core's port.
//!
//! Every component that takes the mempool calls this, so its answers are
//! today's pool's answers: the boundary tests in `tests/boundary.rs` pin
//! them. Queries read the published snapshot. Submissions reserve a slot
//! first, fetch an advertised id only under that reservation, and submit
//! the body, so the core's limits count a transaction from its fetch to its
//! verdict, as today's one count does.
//!
//! The core knows nothing about peers, so the per-peer state is here, as
//! today's pool keeps it: at most [`MAX_INBOUND_CONCURRENCY_PER_PEER`]
//! undecided transactions from one peer, and the legacy peers' transaction
//! cooldowns. A verdict says what its body earned, and this applies it to the
//! legacy peer that served the body. A v2 peer reaching the pool through here
//! is counted and never charged, as today.
//!
//! One answer changes on purpose. A body whose id is already being verified
//! joins that verification and gets its verdict at the second stage, up to
//! the core's waiter limit, where today's pool refuses it at the first stage
//! as already queued.

use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

use futures::FutureExt;
use tokio::sync::{mpsc, oneshot};
use tower::{timeout::Timeout, Service};

use zakura_chain::transaction::UnminedTxId;
use zakura_network::{self as zn, PeerSocketAddr};
use zakura_node_services::mempool::{
    port::{
        AwaitOutputError, Charge, Intent, LocalFault, PoolHandle, PoolSnapshot, Reservation,
        Unreserved, Verdict,
    },
    CreatedOrSpent, Gossip, MempoolDisabledError, QueueSource, Request, Response,
};

use crate::BoxError;

use super::{
    super::{
        crawler::RATE_LIMIT_DELAY,
        downloads::{
            fetch_transaction, peer_source_from_queue_source, transaction_in_best_chain,
            TransactionDownloadVerifyError, MAX_INBOUND_CONCURRENCY_PER_PEER,
        },
        peer_cooldown::PeerCooldowns,
        pending_outputs::PendingOutputsFull,
        queue_source_log_label, start_peer_cooldown,
        storage::NonStandardTransactionError,
        ExactTipRejectionError, MempoolError, Outbound, ReadState, SameEffectsChainRejectionError,
        SameEffectsTipRejectionError,
    },
    Exempt,
};

/// The core's rejection detail, with the node's verifier error filled in.
type CoreMempoolError = zakura_mempool::MempoolError<zakura_consensus::error::TransactionError>;

/// A queued item's second answer.
type Receiver = oneshot::Receiver<Result<(), BoxError>>;

/// What every submission task shares.
struct Shared {
    handle: PoolHandle,
    outbound: Timeout<Outbound>,
    /// The peer set without the fetch timeout, for disconnecting a peer
    /// after repeated cooldowns.
    peers: Outbound,
    /// The legacy peers in a transaction cooldown, keyed by IP address.
    cooldowns: PeerCooldowns,
    /// Whether the node is current now. Today's pool starts a cooldown only
    /// then, and asks when it handles the failure.
    current: Arc<dyn Fn() -> bool + Send + Sync>,
    /// Undecided transactions from each peer, counted from the reservation to
    /// the verdict.
    per_peer: std::sync::Mutex<HashMap<QueueSource, usize>>,
    /// For the already-mined check before a fetch. The read state service
    /// is not `Sync`, so each fetch clones it under this lock.
    read_state: std::sync::Mutex<ReadState>,
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
    /// `misbehavior_sender`. It never charges a peer in `exempt`, and starts
    /// a cooldown only while `current` says the node is current.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        handle: PoolHandle,
        outbound: Timeout<Outbound>,
        peers: Outbound,
        current: Arc<dyn Fn() -> bool + Send + Sync>,
        read_state: ReadState,
        expose_peer_addresses: bool,
        misbehavior_sender: mpsc::Sender<(PeerSocketAddr, u32)>,
        exempt: Arc<Exempt>,
    ) -> Self {
        Self {
            shared: Arc::new(Shared {
                handle,
                outbound,
                peers,
                cooldowns: PeerCooldowns::default(),
                current,
                per_peer: std::sync::Mutex::new(HashMap::new()),
                read_state: std::sync::Mutex::new(read_state),
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
            // Today's pool ignores everything a cooling legacy peer sends.
            if let QueueSource::LegacySocket(addr) = &source {
                if shared.cooldowns.is_cooling_down(addr.ip(), Instant::now()) {
                    metrics::counter!("mempool.peer_cooldown.ignored.transactions.total")
                        .increment(u64::try_from(transactions.len()).unwrap_or(u64::MAX));
                    return Ok(Response::Queued(Vec::new()));
                }
            }
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
    // Today's order: the node's limit, then the peer's. Both answer
    // `FullQueue`, so the peer's is checked first here, before a slot is
    // taken.
    let slot = match &from {
        Some(peer) => Some(PeerSlot::take(&shared, peer).ok_or(MempoolError::FullQueue)?),
        None => None,
    };
    let reservation = match shared.handle.reserve(id, intent).await {
        Ok(reservation) => reservation,
        Err(refused) => return first_stage_refusal(refused),
    };
    // A joined submission takes no core slot, but it still counts against
    // its peer until its verdict, so one peer cannot hold more than its
    // share of waiters.
    let (answer, receiver) = oneshot::channel();
    tokio::spawn(async move {
        // Today's download task has one deadline, `RATE_LIMIT_DELAY`, for the
        // fetch and the verification together, and its peer's slot is free
        // once it ends. So does this.
        let deadline = tokio::time::Instant::now() + RATE_LIMIT_DELAY;
        let result = bring_in(&shared, reservation, item, from, deadline).await;
        drop(slot);
        let _ = answer.send(result);
    });
    Ok(receiver)
}

/// One undecided transaction counted against a peer, from its reservation
/// until it is decided or dropped. Dropping it frees the count.
struct PeerSlot {
    shared: Arc<Shared>,
    peer: QueueSource,
}

impl PeerSlot {
    /// Counts one more transaction against `peer`, or `None` if the peer
    /// already has [`MAX_INBOUND_CONCURRENCY_PER_PEER`].
    fn take(shared: &Arc<Shared>, peer: &QueueSource) -> Option<Self> {
        let mut counts = shared
            .per_peer
            .lock()
            .expect("no code panics while holding the per-peer counts");
        let count = counts.entry(peer.clone()).or_default();
        if *count >= MAX_INBOUND_CONCURRENCY_PER_PEER {
            return None;
        }
        *count += 1;
        Some(Self {
            shared: shared.clone(),
            peer: peer.clone(),
        })
    }
}

impl Drop for PeerSlot {
    /// Frees the count, and forgets a peer with none.
    fn drop(&mut self) {
        let mut counts = self
            .shared
            .per_peer
            .lock()
            .expect("no code panics while holding the per-peer counts");
        if let Some(count) = counts.get_mut(&self.peer) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                counts.remove(&self.peer);
            }
        }
    }
}

/// Today's first-stage answer for a refused reservation, or a completed
/// receiver where today's pool answers at the second stage.
fn first_stage_refusal(refused: Unreserved) -> Result<Receiver, BoxError> {
    let error = match refused {
        Unreserved::Inactive => MempoolError::Disabled,
        Unreserved::Pooled => MempoolError::InMempool,
        Unreserved::Pending => MempoolError::AlreadyQueued,
        Unreserved::NoRoom => MempoolError::FullQueue,
        Unreserved::Stopped => return Err("the pool core has stopped".into()),
        Unreserved::Rejected(rejection) => {
            let Some(error) = rejection
                .detail
                .downcast_ref::<CoreMempoolError>()
                .map(classic_error)
            else {
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

/// Brings `item`'s body in under `reservation`, submits it, applies its
/// charge to the legacy peer that served it, and returns today's
/// second-stage answer, all by `deadline`.
///
/// A fetch still out at the deadline is reported as failed, which frees
/// the slot and reaches the change stream, as today's timeout does. A body
/// the core already holds when the deadline passes stays under the core's
/// own deadlines and node-wide slot, and its outcome reaches the change
/// stream when the core decides it.
async fn bring_in(
    shared: &Shared,
    reservation: Reservation,
    item: Gossip,
    from: Option<QueueSource>,
    deadline: tokio::time::Instant,
) -> Result<(), BoxError> {
    let timed_out = || -> BoxError {
        metrics::counter!("mempool.failed.verify.tasks.total", "reason" => "timeout").increment(1);
        "timeout waiting for verification result".into()
    };
    let tx_id = item.id();
    // The body, and the legacy peer whose bytes it is, if any.
    let (tx, sender) = match item {
        Gossip::Tx(tx) => {
            let sender = match &from {
                Some(QueueSource::LegacySocket(addr)) => Some(PeerSocketAddr::from(*addr)),
                Some(QueueSource::Zakura(_)) | None => None,
            };
            metrics::counter!(
                "mempool.pushed.transactions.total",
                "version" => format!("{}", tx.transaction().version()),
            )
            .increment(1);
            (tx, sender)
        }
        Gossip::Id(_) => {
            let source = from.as_ref().and_then(peer_source_from_queue_source);
            // Boxing works around a compiler limitation: without it, the
            // spawned task cannot be proven `Send`.
            let read_state = shared
                .read_state
                .lock()
                .expect("no code panics while holding the read state lock")
                .clone();
            let cancel = reservation.cancellation();
            let fetched = tokio::select! {
                fetched = tokio::time::timeout_at(
                    deadline,
                    fetch_unless_mined(read_state, shared.outbound.clone(), tx_id, source),
                ) => fetched,
                () = cancel.cancelled() => {
                    // A block mined it while it was fetched. Dropping the
                    // reservation frees its slot, as today's pool cancels
                    // the download.
                    drop(reservation);
                    metrics::counter!("mempool.cancelled.verify.tasks.total").increment(1);
                    return Err(format!(
                        "failed to validate tx: {tx_id}, error: {}",
                        TransactionDownloadVerifyError::Cancelled
                    )
                    .into());
                }
            };
            let Ok(fetched) = fetched else {
                shared.handle.fetch_failed(reservation).await;
                return Err(timed_out());
            };
            match fetched {
                Ok((tx, served_by)) => (tx, served_by),
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
    // Today's downloader does not verify a body from a cooling peer, pushed
    // or served, and checks just before verification. So does this, after
    // any wait for the reservation or the fetch, so a cooldown that started
    // meanwhile still applies. The failure reaches the change stream, and
    // the slot is freed.
    if sender.is_some_and(|addr| shared.cooldowns.is_cooling_down(addr.ip(), Instant::now())) {
        metrics::counter!("mempool.peer_cooldown.ignored.transactions.total").increment(1);
        shared.handle.fetch_failed(reservation).await;
        return Err(format!(
            "failed to validate tx: {tx_id}, error: {}",
            TransactionDownloadVerifyError::PeerCoolingDown
        )
        .into());
    }
    // A v2 peer's body is not charged, as today, and nor is an exempt peer's.
    let supplier = match &from {
        Some(QueueSource::Zakura(_)) => None,
        _ => sender.and_then(|addr| shared.supplier(addr)),
    };
    let Ok(verdict) =
        tokio::time::timeout_at(deadline, shared.handle.submit(reservation, tx)).await
    else {
        return Err(timed_out());
    };
    if let (Verdict::Rejected(rejection), Some(peer)) = (&verdict, supplier) {
        match rejection.charge {
            Charge::Cooldown if (shared.current)() => start_peer_cooldown(
                &shared.cooldowns,
                &shared.peers,
                peer,
                tx_id,
                shared.expose_peer_addresses,
            ),
            Charge::None | Charge::Cooldown => {}
            Charge::Ban => {
                let _ = shared
                    .misbehavior_sender
                    .try_send((peer, zn::constants::MAX_PEER_MISBEHAVIOR_SCORE));
            }
        }
    }
    second_stage(tx_id, verdict)
}

impl Shared {
    /// The legacy peer to charge for a body `peer` served: the peer, unless
    /// the pool must never charge it.
    fn supplier(&self, peer: PeerSocketAddr) -> Option<PeerSocketAddr> {
        (!self.exempt.contains(peer)).then_some(peer)
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
            let Some(error) = rejection
                .detail
                .downcast_ref::<CoreMempoolError>()
                .map(classic_error)
            else {
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
                error => Err(error.into()),
            }
        }
        Verdict::NoRoom => Err(MempoolError::FullQueue.into()),
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

/// Fetches `tx_id`, unless the best chain already holds it.
///
/// Today's downloader checks the chain before it asks the network, so an id
/// that is already mined is answered `InState` without a download. The
/// service's own timeout starts only once the network is ready, so the whole
/// task's deadline in [`queue`] is what bounds a fetch from a network that
/// never gets ready.
async fn fetch_unless_mined(
    read_state: ReadState,
    outbound: Timeout<Outbound>,
    tx_id: UnminedTxId,
    source: Option<zn::PeerSource>,
) -> Result<(zakura_chain::transaction::UnminedTx, Option<PeerSocketAddr>), BoxError> {
    // Boxing works around a compiler limitation: without it, the spawned
    // task cannot be proven `Send`.
    let lookup: futures::future::BoxFuture<'static, _> =
        transaction_in_best_chain(read_state, tx_id).boxed();
    let download: futures::future::BoxFuture<'static, _> =
        fetch_transaction(outbound, tx_id, source).boxed();
    lookup.await?;
    download.await.map_err(BoxError::from)
}

/// Today's pool's error for the core's rejection `error`. Callers of the
/// mempool service downcast to today's types, and the core keeps its own, so
/// the adapter converts each one variant for variant.
fn classic_error(error: &CoreMempoolError) -> MempoolError {
    use zakura_mempool::{
        ExactTipRejectionError as Exact, MempoolError as Core,
        SameEffectsChainRejectionError as Chain, SameEffectsTipRejectionError as Tip,
    };
    match error {
        Core::StorageExactTip(error) => MempoolError::StorageExactTip(match error {
            Exact::FailedVerification(error) => {
                ExactTipRejectionError::FailedVerification(error.clone())
            }
            Exact::FailedStandard(error) => {
                ExactTipRejectionError::FailedStandard(classic_non_standard(error))
            }
            Exact::Evicted => ExactTipRejectionError::Evicted,
            Exact::BelowEvictionCost => ExactTipRejectionError::BelowEvictionCost,
        }),
        Core::StorageEffectsTip(error) => MempoolError::StorageEffectsTip(match error {
            Tip::SpendConflict => SameEffectsTipRejectionError::SpendConflict,
            Tip::MissingOutput => SameEffectsTipRejectionError::MissingOutput,
            Tip::TooManyAncestors => SameEffectsTipRejectionError::TooManyAncestors,
            Tip::TooManyPackageTransactions => {
                SameEffectsTipRejectionError::TooManyPackageTransactions
            }
        }),
        Core::StorageEffectsChain(error) => MempoolError::StorageEffectsChain(match error {
            Chain::Expired => SameEffectsChainRejectionError::Expired,
            Chain::DuplicateSpend => SameEffectsChainRejectionError::DuplicateSpend,
            Chain::Mined => SameEffectsChainRejectionError::Mined,
        }),
        Core::InMempool => MempoolError::InMempool,
        Core::AlreadyQueued => MempoolError::AlreadyQueued,
        Core::FullQueue => MempoolError::FullQueue,
        Core::Disabled => MempoolError::Disabled,
        Core::NonStandardTransaction(error) => {
            MempoolError::NonStandardTransaction(classic_non_standard(error))
        }
    }
}

/// Today's pool's standardness error for the core's `error`. See
/// [`classic_error`].
fn classic_non_standard(
    error: &zakura_mempool::NonStandardTransactionError,
) -> NonStandardTransactionError {
    use zakura_mempool::NonStandardTransactionError as Core;
    match error {
        Core::TransactionTooLarge {
            actual_bytes,
            max_bytes,
        } => NonStandardTransactionError::TransactionTooLarge {
            actual_bytes: *actual_bytes,
            max_bytes: *max_bytes,
        },
        Core::IsDust => NonStandardTransactionError::IsDust,
        Core::ScriptSigTooLarge => NonStandardTransactionError::ScriptSigTooLarge,
        Core::ScriptSigNotPushOnly => NonStandardTransactionError::ScriptSigNotPushOnly,
        Core::ScriptPubKeyNonStandard => NonStandardTransactionError::ScriptPubKeyNonStandard,
        Core::BareMultiSig => NonStandardTransactionError::BareMultiSig,
        Core::MultiOpReturn => NonStandardTransactionError::MultiOpReturn,
        Core::DataCarrierTooLarge => NonStandardTransactionError::DataCarrierTooLarge,
        Core::TooManySigops => NonStandardTransactionError::TooManySigops,
        Core::NonStandardInputs => NonStandardTransactionError::NonStandardInputs,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        task::{Context, Poll},
        time::Duration,
    };

    use tower::util::BoxCloneSyncService;
    use zakura_chain::{block::Height, parameters::Network};
    use zakura_state as zs;

    use super::super::super::{
        downloads::{TransactionDownloadVerifyError, TRANSACTION_DOWNLOAD_TIMEOUT},
        ReadState,
    };

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

    /// A read state that finds `mined` in the best chain and nothing else.
    fn chain_holding(mined: Option<UnminedTxId>) -> ReadState {
        tower::util::BoxCloneService::new(tower::service_fn(move |request| async move {
            match request {
                zs::ReadRequest::Transaction(hash) => Ok(zs::ReadResponse::Transaction(
                    mined
                        .filter(|mined| mined.mined_id() == hash)
                        .map(|_| Network::Mainnet.unmined_transactions_in_blocks(1..=1))
                        .and_then(|mut txs| txs.next())
                        .map(|tx| {
                            zs::MinedTx::new(
                                zakura_node_services::block_memory::TransactionHandle::Shared(
                                    tx.transaction.transaction().clone(),
                                ),
                                Height(1),
                                1,
                                chrono::Utc::now(),
                            )
                        }),
                )),
                _ => unreachable!("the fetch asks only for a transaction"),
            }
        }))
    }

    /// An id the chain already mined is answered at once, without asking a
    /// network that would never answer.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn an_already_mined_id_is_not_fetched() {
        let mined = UnminedTxId::Legacy(zakura_chain::transaction::Hash([7; 32]));
        let outbound = Timeout::new(
            BoxCloneSyncService::new(NeverReady),
            TRANSACTION_DOWNLOAD_TIMEOUT,
        );
        let started = tokio::time::Instant::now();
        let fetched = fetch_unless_mined(chain_holding(Some(mined)), outbound, mined, None).await;
        let error = fetched.expect_err("a mined id is not fetched");
        assert!(matches!(
            error.downcast_ref::<TransactionDownloadVerifyError>(),
            Some(TransactionDownloadVerifyError::InState)
        ));
        assert_eq!(started.elapsed(), Duration::ZERO, "no wait for the network");
    }

    /// The adapter on `handle`, with a network that is never ready, an
    /// empty chain, no exempt peers, and a node that is current.
    fn adapter_on(handle: PoolHandle) -> CoreMempool {
        adapter_when(handle, true)
    }

    /// [`adapter_on`], with a node whose currentness is `current`.
    fn adapter_when(handle: PoolHandle, current: bool) -> CoreMempool {
        let outbound = Timeout::new(
            BoxCloneSyncService::new(NeverReady),
            TRANSACTION_DOWNLOAD_TIMEOUT,
        );
        CoreMempool::new(
            handle,
            outbound,
            BoxCloneSyncService::new(NeverReady),
            Arc::new(move || current),
            chain_holding(None),
            false,
            mpsc::channel(1).0,
            Arc::new(Exempt::default()),
        )
    }

    /// A legacy peer.
    fn peer() -> QueueSource {
        QueueSource::LegacySocket(std::net::SocketAddr::from(([192, 0, 2, 1], 8233)))
    }

    /// A fetch from a network that never becomes ready ends at today's
    /// whole-task deadline. It is reported as failed, which frees the core's
    /// slot and reaches the change stream as today's timeout does, and its
    /// peer's slot is free again.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_fetch_from_an_unready_network_ends_at_the_task_deadline() {
        use zakura_node_services::mempool::port::{pool_port, PoolCommand, ReservationId};

        let (handle, mut inbox) = pool_port();
        let core = adapter_on(handle);
        let id = UnminedTxId::Legacy(zakura_chain::transaction::Hash([7; 32]));
        let started = tokio::time::Instant::now();
        let queued = tokio::spawn(queue(core.shared.clone(), Gossip::Id(id), Some(peer())));
        let Some(PoolCommand::Reserve { reply, .. }) = inbox.next().await else {
            panic!("the fetch reserves first");
        };
        let _ = reply.send(Ok(inbox.grant(
            ReservationId(1),
            id,
            false,
            true,
            Default::default(),
        )));
        let receiver = queued
            .await
            .expect("the task does not panic")
            .expect("the id passes the first stage");

        let error = receiver
            .await
            .expect("the fetch is answered")
            .expect_err("a fetch that never happens fails");
        assert_eq!(error.to_string(), "timeout waiting for verification result");
        assert_eq!(
            started.elapsed(),
            RATE_LIMIT_DELAY,
            "today's whole-task deadline"
        );
        let Some(PoolCommand::FetchFailed(reservation)) = inbox.next().await else {
            panic!("the timed-out fetch is reported as failed");
        };
        assert_eq!(reservation.accept(), ReservationId(1));
        assert!(core.shared.per_peer.lock().expect("no panics").is_empty());
    }

    /// A body whose verdict takes too long ends at the same deadline, which
    /// covers the fetch and the verification together, as today's download
    /// task does. Its peer's slot is free again.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_verdict_that_takes_too_long_ends_at_the_task_deadline() {
        use zakura_node_services::mempool::port::{pool_port, PoolCommand, ReservationId};

        let (handle, mut inbox) = pool_port();
        let core = adapter_on(handle);
        let body = Network::Mainnet
            .unmined_transactions_in_blocks(1..=1)
            .next()
            .expect("a vector transaction")
            .transaction;
        let started = tokio::time::Instant::now();
        let queued = tokio::spawn(queue(
            core.shared.clone(),
            Gossip::Tx(body.clone()),
            Some(peer()),
        ));
        let Some(PoolCommand::Reserve { tx_id, reply, .. }) = inbox.next().await else {
            panic!("the push reserves first");
        };
        let _ = reply.send(Ok(inbox.grant(
            ReservationId(1),
            tx_id,
            false,
            true,
            Default::default(),
        )));
        let receiver = queued
            .await
            .expect("the task does not panic")
            .expect("the push passes the first stage");
        // The core takes the body and never answers.
        let Some(PoolCommand::Submit {
            reservation,
            reply: _verdict,
            ..
        }) = inbox.next().await
        else {
            panic!("the body is submitted");
        };
        let _accepted = reservation.accept();

        let error = receiver
            .await
            .expect("the push is answered")
            .expect_err("a verdict that never comes fails");
        assert_eq!(error.to_string(), "timeout waiting for verification result");
        assert_eq!(
            started.elapsed(),
            RATE_LIMIT_DELAY,
            "today's whole-task deadline"
        );
        assert!(core.shared.per_peer.lock().expect("no panics").is_empty());
    }

    /// A peer that starts cooling down while its pushed body waits for its
    /// reservation does not get that body verified: the body never reaches
    /// the core, and its slot is freed through `fetch_failed`, as today's
    /// downloader checks just before verification.
    #[tokio::test(flavor = "current_thread")]
    async fn a_push_from_a_peer_that_cools_down_while_it_waits_is_not_verified() {
        use zakura_node_services::mempool::port::{pool_port, PoolCommand, ReservationId};

        let (handle, mut inbox) = pool_port();
        let outbound = Timeout::new(
            BoxCloneSyncService::new(NeverReady),
            TRANSACTION_DOWNLOAD_TIMEOUT,
        );
        let core = CoreMempool::new(
            handle,
            outbound,
            BoxCloneSyncService::new(NeverReady),
            Arc::new(|| true),
            chain_holding(None),
            false,
            mpsc::channel(1).0,
            Arc::new(Exempt::default()),
        );
        let body = Network::Mainnet
            .unmined_transactions_in_blocks(1..=1)
            .next()
            .expect("a vector transaction")
            .transaction;
        let peer = std::net::SocketAddr::from(([192, 0, 2, 1], 8233));
        let queued = tokio::spawn(queue(
            core.shared.clone(),
            Gossip::Tx(body.clone()),
            Some(QueueSource::LegacySocket(peer)),
        ));

        // The reservation is asked for. Before it is granted, another of the
        // peer's transactions starts its cooldown.
        let Some(PoolCommand::Reserve { tx_id, reply, .. }) = inbox.next().await else {
            panic!("the push reserves first");
        };
        core.shared
            .cooldowns
            .record_invalid_transaction(peer.ip(), Instant::now());
        let _ = reply.send(Ok(inbox.grant(
            ReservationId(1),
            tx_id,
            false,
            true,
            Default::default(),
        )));

        let receiver = queued
            .await
            .expect("the task does not panic")
            .expect("the push passes the first stage");
        let Some(PoolCommand::FetchFailed(reservation)) = inbox.next().await else {
            panic!("the body is not submitted");
        };
        assert_eq!(reservation.accept(), ReservationId(1));
        let error = receiver
            .await
            .expect("the push is answered")
            .expect_err("a cooling peer's body is refused");
        assert!(error.to_string().contains("cooldown"), "{error}");
    }

    /// A cooldown charge cools the peer that pushed the body only while the
    /// node is current, asked when the verdict arrives, as today's pool asks
    /// when it handles the failure.
    #[tokio::test(flavor = "current_thread")]
    async fn a_cooldown_charge_cools_its_peer_only_while_the_node_is_current() {
        use zakura_node_services::mempool::port::{
            pool_port, PoolCommand, Rejection, RejectionScope, ReservationId,
        };

        for current in [true, false] {
            let (handle, mut inbox) = pool_port();
            let core = adapter_when(handle, current);
            let body = Network::Mainnet
                .unmined_transactions_in_blocks(1..=1)
                .next()
                .expect("a vector transaction")
                .transaction;
            let queued = tokio::spawn(queue(core.shared.clone(), Gossip::Tx(body), Some(peer())));
            let Some(PoolCommand::Reserve { tx_id, reply, .. }) = inbox.next().await else {
                panic!("the push reserves first");
            };
            let _ = reply.send(Ok(inbox.grant(
                ReservationId(1),
                tx_id,
                false,
                true,
                Default::default(),
            )));
            let receiver = queued
                .await
                .expect("the task does not panic")
                .expect("the push passes the first stage");
            let Some(PoolCommand::Submit {
                reservation, reply, ..
            }) = inbox.next().await
            else {
                panic!("the body is submitted");
            };
            let _accepted = reservation.accept();
            let _ = reply.send(Verdict::Rejected(Rejection {
                scope: RejectionScope::ExactTip,
                charge: Charge::Cooldown,
                detail: Arc::new(std::io::Error::other("invalid")),
            }));
            let _ = receiver.await.expect("the push is answered");

            let QueueSource::LegacySocket(addr) = peer() else {
                unreachable!("the test peer is a legacy peer");
            };
            assert_eq!(
                core.shared
                    .cooldowns
                    .is_cooling_down(addr.ip(), Instant::now()),
                current,
                "cooling down only while current"
            );
        }
    }
}
