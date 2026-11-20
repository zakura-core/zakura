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
//! cooldowns. A verdict says what its body earned, and this applies it to
//! the legacy peer that served the body. A v2 peer reaching the pool through
//! here is counted and never charged, as today.
//!
//! Each item has today's one deadline, [`RATE_LIMIT_DELAY`] from its
//! request, for its reservation, its fetch and its verdict.

use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

use futures::FutureExt;
use tokio::sync::oneshot;
use tower::{timeout::Timeout, Service};

use zakura_chain::{
    block::{self, Height},
    transaction::UnminedTxId,
};
use zakura_network::{self as zn, PeerSocketAddr};
use zakura_node_services::mempool::{
    port::{Charge, LocalFault, PoolHandle, PoolSnapshot, Reservation, Unreserved, Verdict},
    CreatedOrSpent, Gossip, MempoolDisabledError, OutputNotInPool, QueueSource, Request, Response,
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
        queue_source_log_label, start_peer_cooldown,
        storage::NonStandardTransactionError,
        ExactTipRejectionError, MempoolError, Outbound, ReadState, SameEffectsChainRejectionError,
    },
    Exempt,
};

/// A queued item's second answer.
type Receiver = oneshot::Receiver<Result<(), BoxError>>;

/// The longest a request waits for the core to apply the state's best tip.
/// The core applies a block within milliseconds, so this is only reached if
/// the core is stuck, and the request then gets the pool the core has.
const MAX_TIP_WAIT: std::time::Duration = std::time::Duration::from_secs(1);

/// How often a waiting request checks the tips again when no new snapshot
/// arrives, since the state's best tip can move too.
const TIP_RECHECK: std::time::Duration = std::time::Duration::from_millis(2);

/// What every submission task shares.
struct Shared {
    handle: PoolHandle,
    outbound: Timeout<Outbound>,
    /// The peer set without the fetch timeout, for disconnecting a peer
    /// after repeated cooldowns.
    peers: Outbound,
    /// The legacy peers in a transaction cooldown, keyed by IP address.
    cooldowns: PeerCooldowns,
    /// Whether a cooldown earned against a tip at this height may start now.
    /// Today's pool starts one only while the node is current and its best
    /// tip is still the one the failure was verified at, and asks when it
    /// handles the failure.
    may_cool: Arc<dyn Fn(Height) -> bool + Send + Sync>,
    /// The state's best tip, if it has one. See [`caught_up`].
    best_tip: Arc<dyn Fn() -> Option<block::Hash> + Send + Sync>,
    /// The largest body the pool takes, which today's downloader checks
    /// before a cooldown.
    max_transaction_bytes: u64,
    /// Undecided transactions from each peer, counted from the reservation to
    /// the verdict.
    per_peer: std::sync::Mutex<HashMap<QueueSource, usize>>,
    /// Items this adapter is bringing in, by id. See [`Fetching`].
    fetching: std::sync::Mutex<HashMap<UnminedTxId, usize>>,
    /// For the already-mined check before a fetch. The read state service
    /// is not `Sync`, so each fetch clones it under this lock.
    read_state: std::sync::Mutex<ReadState>,
    expose_peer_addresses: bool,
    misbehavior: zn::MisbehaviorReporter,
    exempt: Arc<Exempt>,
}

/// The `mempool::Request` service in core mode.
#[derive(Clone)]
pub(crate) struct CoreMempool {
    shared: Arc<Shared>,
}

impl CoreMempool {
    /// A service on `handle` that fetches through `outbound` and bans through
    /// `misbehavior`. It never charges a peer in `exempt`, starts a
    /// cooldown only when `may_cool` allows it for the verdict's tip, and
    /// answers from a pool of the tip `best_tip` names. See [`caught_up`].
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        handle: PoolHandle,
        outbound: Timeout<Outbound>,
        peers: Outbound,
        may_cool: Arc<dyn Fn(Height) -> bool + Send + Sync>,
        best_tip: Arc<dyn Fn() -> Option<block::Hash> + Send + Sync>,
        read_state: ReadState,
        max_transaction_bytes: u64,
        expose_peer_addresses: bool,
        misbehavior: zn::MisbehaviorReporter,
        exempt: Arc<Exempt>,
    ) -> Self {
        Self {
            shared: Arc::new(Shared {
                handle,
                outbound,
                peers,
                cooldowns: PeerCooldowns::default(),
                may_cool,
                best_tip,
                max_transaction_bytes,
                per_peer: std::sync::Mutex::new(HashMap::new()),
                fetching: std::sync::Mutex::new(HashMap::new()),
                read_state: std::sync::Mutex::new(read_state),
                expose_peer_addresses,
                misbehavior,
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
            Some(snapshot) if reads_snapshot(&request) && shared.is_behind(&snapshot) => {
                async move {
                    match caught_up(&shared).await {
                        Some(snapshot) => answer(shared, snapshot, request).await,
                        None => inactive_answer(request),
                    }
                }
                .boxed()
            }
            Some(snapshot) => match request {
                Request::Queue(items) => queue_request(shared, items).boxed(),
                Request::QueueFromPeer {
                    transactions,
                    source,
                } => peer_batch(shared, snapshot, transactions, source).boxed(),
                request => answer(shared, snapshot, request).boxed(),
            },
            // Answered here, so the request and any body in it are dropped
            // in the call, as today's pool drops them.
            None => futures::future::ready(inactive_answer(request)).boxed(),
        }
    }
}

/// Answers a local `Queue` request: each item's first-stage answer, in the
/// request's order.
async fn queue_request(shared: Arc<Shared>, items: Vec<Gossip>) -> Result<Response, BoxError> {
    let deadline = tokio::time::Instant::now() + RATE_LIMIT_DELAY;
    let mut answers = Vec::with_capacity(items.len());
    for item in items {
        let (fetching, _) = Fetching::start(&shared, item.id());
        answers.push(queue(shared.clone(), item, None, None, fetching, deadline).await);
    }
    Ok(Response::Queued(answers))
}

/// Answers a peer's `QueueFromPeer` batch, as today's pool does: nothing
/// from a cooling legacy peer, and whatever it cannot queue dropped.
async fn peer_batch(
    shared: Arc<Shared>,
    snapshot: Arc<PoolSnapshot>,
    transactions: Vec<Gossip>,
    source: QueueSource,
) -> Result<Response, BoxError> {
    tracing::trace!(
        req_count = ?transactions.len(),
        source = %queue_source_log_label(&source, shared.expose_peer_addresses),
        "got mempool QueueFromPeer request"
    );
    // Today's pool ignores everything a cooling legacy peer sends.
    let cooling = match &source {
        QueueSource::LegacySocket(addr) => {
            shared.cooldowns.is_cooling_down(addr.ip(), Instant::now())
        }
        QueueSource::Zakura(_) => false,
    };
    if cooling {
        metrics::counter!("mempool.peer_cooldown.ignored.transactions.total")
            .increment(u64::try_from(transactions.len()).unwrap_or(u64::MAX));
        return Ok(Response::Queued(Vec::new()));
    }
    let deadline = tokio::time::Instant::now() + RATE_LIMIT_DELAY;
    // Today's pool checks what it already knows about each item before its
    // limits, at no cost. Here that is one look at the snapshot and one core
    // command for the whole batch. A pooled transaction is known by its
    // effects, as `reserve` knows it.
    let fresh: Vec<Gossip> = transactions
        .into_iter()
        .filter(|item| snapshot.transaction(&item.id().mined_id()).is_none())
        .collect();
    // A core that does not answer by the deadline gets the batch dropped, as
    // a reservation that does not come back in time would be.
    let Ok(rejected) = shared
        .handle
        .rejected_ids(fresh.iter().map(Gossip::id).collect(), deadline)
        .await
    else {
        return Ok(Response::Queued(Vec::new()));
    };
    // The rest take their peer's slots now, in one pass, as today's pool
    // queues a batch at once: at most the peer's limit of them, and the rest
    // are dropped without asking the core. Slots that free up while the
    // batch is worked on take nothing more from it.
    let mut held = Vec::new();
    for item in fresh {
        let id = item.id();
        if rejected.contains(&id) {
            continue;
        }
        // Today's pool answers an id it is already bringing in as queued,
        // before its limits, so such an id takes none of the peer's slots and
        // is none of its refusals. This looks after the lookup, so a fetch
        // that ended while it waited no longer hides the id.
        let (fetching, already) = Fetching::start(&shared, id);
        if already {
            continue;
        }
        match PeerSlot::take(&shared, &source) {
            Some(slot) => held.push((item, slot, fetching)),
            None => metrics::counter!("mempool.full_queue.per_peer.total").increment(1),
        }
    }
    for (item, slot, fetching) in held {
        let _ = queue(
            shared.clone(),
            item,
            Some(source.clone()),
            Some(slot),
            fetching,
            deadline,
        )
        .await;
    }
    Ok(Response::Queued(Vec::new()))
}

/// Whether `request` is answered from the snapshot alone. Only these wait
/// for the core to apply the state's best tip. See [`caught_up`].
fn reads_snapshot(request: &Request) -> bool {
    matches!(
        request,
        Request::TransactionIds
            | Request::TransactionsById(_)
            | Request::TransactionsByMinedId(_)
            | Request::TransactionWithDepsByMinedId(_)
            | Request::FullTransactions
            | Request::QueueStats
            | Request::UnspentOutput(_)
    )
}

/// The latest snapshot once it is of the state's best tip, or once
/// [`MAX_TIP_WAIT`] has passed or the core has stopped, or `None` if the pool
/// turned off meanwhile.
///
/// Today's pool applies a new tip before it answers any request, so a caller
/// that just saw a block in the state never gets the pool from before it. An
/// RPC that looks a mined transaction up in the pool first would otherwise
/// find it still there. The core applies tips in its own task, so a request
/// that arrives in between waits for it.
async fn caught_up(shared: &Shared) -> Option<Arc<PoolSnapshot>> {
    let deadline = tokio::time::Instant::now() + MAX_TIP_WAIT;
    let mut reader = shared.handle.reader();
    loop {
        let snapshot = reader.snapshot().filter(|snapshot| snapshot.active)?;
        if !shared.is_behind(&snapshot) || tokio::time::Instant::now() >= deadline {
            return Some(snapshot);
        }
        // The state's best tip can move too, so look again soon even without
        // a new snapshot.
        if let Ok(false) = tokio::time::timeout(TIP_RECHECK, reader.snapshot_changed()).await {
            return Some(snapshot);
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
                .transactions()
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
        // Nothing waits for an output here. The transaction verifier is the
        // one caller of this request, and it has a handle of its own that
        // answers at once, as this does.
        Request::AwaitOutput(outpoint) => match snapshot.created_output(&outpoint) {
            Some(output) => Response::UnspentOutput(output.clone()),
            None => return Err(OutputNotInPool.into()),
        },
        Request::FullTransactions => Response::FullTransactions {
            transactions: snapshot.transactions().cloned().collect(),
            transaction_dependencies: snapshot.dependencies.clone(),
            last_seen_tip_hash: snapshot.tip.hash,
        },
        Request::RejectedTransactionIds(ids) => {
            // A core that does not answer in time knows of no rejection, as a
            // stopped one.
            let deadline = tokio::time::Instant::now() + RATE_LIMIT_DELAY;
            Response::RejectedTransactionIds(
                shared
                    .handle
                    .rejected_ids(ids, deadline)
                    .await
                    .unwrap_or_default(),
            )
        }
        Request::Queue(_) | Request::QueueFromPeer { .. } => {
            unreachable!("`CoreMempool::call` answers submissions itself")
        }
        Request::CheckForVerifiedTransactions => Response::CheckedForVerifiedTransactions,
        Request::QueueStats => {
            let bytes = snapshot
                .transactions()
                .map(|tx| tx.transaction.size())
                .sum();
            Response::QueueStats {
                size: snapshot.len(),
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

/// Applies `verdict`'s charge to `supplier`, the legacy peer that served the
/// body, if any: a ban through the misbehavior channel, or today's cooldown
/// while the node is current at the tip the failure was earned at.
fn charge(
    shared: &Shared,
    verdict: &Verdict,
    supplier: Option<PeerSocketAddr>,
    tx_id: UnminedTxId,
) {
    let (Verdict::Rejected(rejection), Some(peer)) = (verdict, supplier) else {
        return;
    };
    match rejection.charge {
        Charge::Cooldown if (shared.may_cool)(rejection.tip) => start_peer_cooldown(
            &shared.cooldowns,
            &shared.peers,
            peer,
            tx_id,
            shared.expose_peer_addresses,
        ),
        Charge::None | Charge::Cooldown => {}
        Charge::Ban => {
            shared
                .misbehavior
                .report(peer, zn::constants::MAX_PEER_MISBEHAVIOR_SCORE);
        }
    }
}

/// Today's answer for an item whose task reached its deadline.
fn timed_out() -> BoxError {
    metrics::counter!("mempool.failed.verify.tasks.total", "reason" => "timeout").increment(1);
    "timeout waiting for verification result".into()
}

/// Queues `item`: reserves its slot in the core and answers the first stage,
/// and leaves a task that brings the body in and answers the second. `slot`
/// is its peer's slot and `fetching` its record as being brought in, both
/// held until the second stage is answered. Every wait for the core ends at
/// `deadline`.
async fn queue(
    shared: Arc<Shared>,
    item: Gossip,
    from: Option<QueueSource>,
    slot: Option<PeerSlot>,
    fetching: Fetching,
    deadline: tokio::time::Instant,
) -> Result<Receiver, BoxError> {
    let reserved = shared.handle.reserve(item.id(), deadline).await;
    let reservation = match reserved {
        Ok(Ok(reservation)) => reservation,
        Ok(Err(refused)) => return first_stage_refusal(refused),
        Err(_elapsed) => return Err(timed_out()),
    };
    metrics::counter!("mempool.queued.transactions.total").increment(1);
    let (answer, receiver) = oneshot::channel();
    tokio::spawn(async move {
        // Today's download task has one deadline for the fetch and the
        // verification together, and its peer's slot is free once it ends.
        // So does this.
        let result = bring_in(shared, reservation, item, from, deadline).await;
        drop(slot);
        drop(fetching);
        let _ = answer.send(result);
    });
    Ok(receiver)
}

/// One item this adapter is bringing in, from when it takes its slots until
/// its second stage is answered: today's downloader's queued set, which it
/// checks before a peer's limit. Dropping it forgets the item.
struct Fetching {
    shared: Arc<Shared>,
    id: UnminedTxId,
}

impl Fetching {
    /// Records that an item with id `id` is being brought in, and says
    /// whether another item already was.
    fn start(shared: &Arc<Shared>, id: UnminedTxId) -> (Self, bool) {
        let mut fetching = shared
            .fetching
            .lock()
            .expect("no code panics while holding the fetching ids");
        let count = fetching.entry(id).or_default();
        let already = *count > 0;
        *count += 1;
        (
            Self {
                shared: shared.clone(),
                id,
            },
            already,
        )
    }
}

impl Drop for Fetching {
    /// Forgets the item, and the id once no item has it.
    fn drop(&mut self) {
        let mut fetching = self
            .shared
            .fetching
            .lock()
            .expect("no code panics while holding the fetching ids");
        if let Some(count) = fetching.get_mut(&self.id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                fetching.remove(&self.id);
            }
        }
    }
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

/// Brings `item`'s body in under `reservation`, submits it, applies its
/// charge to the legacy peer that served it, and returns today's
/// second-stage answer, all by `deadline`.
///
/// A fetch still out at the deadline is reported as failed, which frees
/// the slot and reaches the change stream, as today's timeout does. A
/// verdict that comes after the deadline charges nobody, as today.
async fn bring_in(
    shared: Arc<Shared>,
    reservation: Reservation,
    item: Gossip,
    from: Option<QueueSource>,
    deadline: tokio::time::Instant,
) -> Result<(), BoxError> {
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
                    return Err(cancelled());
                }
            };
            let Ok(fetched) = fetched else {
                shared.handle.fetch_failed(reservation, deadline).await;
                return Err(timed_out());
            };
            match fetched {
                Ok((tx, served_by)) => (tx, served_by),
                Err(error) => {
                    // Today's labels: the state lookup before the fetch can
                    // fail too, and is counted as such.
                    let reason = error
                        .downcast_ref::<TransactionDownloadVerifyError>()
                        .map_or(
                            "download_failed",
                            TransactionDownloadVerifyError::metric_label,
                        );
                    metrics::counter!("mempool.failed.verify.tasks.total", "reason" => reason)
                        .increment(1);
                    shared.handle.fetch_failed(reservation, deadline).await;
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
    //
    // Today's downloader checks the size first, so an oversized body is
    // refused and remembered as too large whoever sent it. Such a body goes
    // on to the core, whose verifier refuses it for its size before any
    // other check, caches that for its exact id and charges nobody.
    let oversized = usize::try_from(shared.max_transaction_bytes)
        .is_ok_and(|max_transaction_bytes| tx.size() > max_transaction_bytes);
    if !oversized
        && sender.is_some_and(|addr| shared.cooldowns.is_cooling_down(addr.ip(), Instant::now()))
    {
        metrics::counter!("mempool.peer_cooldown.ignored.transactions.total").increment(1);
        shared.handle.fetch_failed(reservation, deadline).await;
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
    let Ok(verdict) = shared.handle.submit(reservation, tx, deadline).await else {
        return Err(timed_out());
    };
    // The charge goes to the peer that served the body, here, in the
    // component that owns peer state.
    charge(&shared, &verdict, supplier, tx_id);
    second_stage(tx_id, verdict)
}

impl Shared {
    /// Whether `snapshot` is of a tip other than the state's best tip. A
    /// state with no tip yet has nothing to be behind.
    fn is_behind(&self, snapshot: &PoolSnapshot) -> bool {
        (self.best_tip)().is_some_and(|best| best != snapshot.tip.hash)
    }

    /// The legacy peer to charge for a body `peer` served: the peer, unless
    /// the pool must never charge it.
    fn supplier(&self, peer: PeerSocketAddr) -> Option<PeerSocketAddr> {
        (!self.exempt.contains(peer)).then_some(peer)
    }
}

/// Today's second-stage answer for a fetch or verification a block ended by
/// mining its transaction.
fn cancelled() -> BoxError {
    "verification cancelled".into()
}

/// Today's second-stage answer for `verdict`.
fn second_stage(tx_id: UnminedTxId, verdict: Verdict) -> Result<(), BoxError> {
    let failed = |reason: &dyn std::fmt::Display| -> BoxError {
        format!("failed to validate tx: {tx_id}, error: {reason}").into()
    };
    match verdict {
        Verdict::Admitted => Ok(()),
        Verdict::Rejected(rejection) => {
            let Some(error) = rejection.detail.downcast_ref::<MempoolError>().cloned() else {
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
                // A block mined it while it was verified, which today's pool
                // answers as a cancelled task.
                MempoolError::StorageEffectsChain(SameEffectsChainRejectionError::Mined) => {
                    metrics::counter!("mempool.cancelled.verify.tasks.total").increment(1);
                    Err(cancelled())
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
    use crate::components::test_misbehavior::{self, reported_score};

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

    /// Queues one item from `from` as a request would: takes its peer's
    /// slot, then reserves and answers its first stage.
    async fn queue(
        shared: Arc<Shared>,
        item: Gossip,
        from: Option<QueueSource>,
    ) -> Result<Receiver, BoxError> {
        let slot = match &from {
            Some(peer) => Some(PeerSlot::take(&shared, peer).ok_or(MempoolError::FullQueue)?),
            None => None,
        };
        let (fetching, _) = Fetching::start(&shared, item.id());
        let deadline = tokio::time::Instant::now() + RATE_LIMIT_DELAY;
        super::queue(shared, item, from, slot, fetching, deadline).await
    }

    /// A distinct id for the `n`th id a test needs.
    fn filler_id(n: usize) -> UnminedTxId {
        let mut hash = [0xf0; 32];
        hash[..8].copy_from_slice(
            &u64::try_from(n)
                .expect("a test's id count fits")
                .to_be_bytes(),
        );
        UnminedTxId::Legacy(zakura_chain::transaction::Hash(hash))
    }

    /// `peer`'s whole share of undecided transactions, taken by the test.
    fn fill_share(core: &CoreMempool, peer: &QueueSource) -> Vec<PeerSlot> {
        (0..MAX_INBOUND_CONCURRENCY_PER_PEER)
            .map(|_| PeerSlot::take(&core.shared, peer).expect("room in the share"))
            .collect()
    }

    /// An id this adapter is already bringing in takes no slot of a peer
    /// that advertises it too and counts no full-queue refusal, as today's
    /// pool answers it as queued before its limits.
    #[tokio::test(flavor = "current_thread")]
    async fn an_id_being_brought_in_takes_no_slot() {
        use zakura_node_services::mempool::port::{pool_port, PoolCommand};

        let readings = super::super::super::tests::Readings::default();
        let _recording = metrics::set_default_local_recorder(&readings);
        let (handle, mut inbox) = pool_port();
        let mut core = adapter_on(handle);
        publish_empty(&inbox);
        let peer = QueueSource::LegacySocket(([192, 0, 2, 1], 8233).into());
        let (_fetching, _) = Fetching::start(&core.shared, filler_id(1));
        let _share = fill_share(&core, &peer);
        let request = tokio::spawn(core.call(Request::QueueFromPeer {
            transactions: vec![Gossip::Id(filler_id(1))],
            source: peer.clone(),
        }));
        let Some(PoolCommand::RejectedIds { reply, .. }) = inbox.next().await else {
            panic!("the batch looks its id up");
        };
        let _ = reply.send(Default::default());
        let answer = request.await.expect("the task does not panic");
        assert!(matches!(answer, Ok(Response::Queued(_))));
        assert_eq!(readings.get("mempool.full_queue.per_peer.total"), None);
        assert_eq!(
            core.shared
                .per_peer
                .lock()
                .expect("no test panics holding the per-peer counts")
                .get(&peer)
                .copied(),
            Some(MAX_INBOUND_CONCURRENCY_PER_PEER),
            "only the test's own share is taken"
        );
    }

    /// An id whose fetch ends while the batch waits on its lookup is no
    /// longer being brought in, so the batch reserves it.
    #[tokio::test(flavor = "current_thread")]
    async fn a_fetch_that_ends_during_the_lookup_does_not_hide_its_id() {
        use zakura_node_services::mempool::port::{pool_port, PoolCommand};

        let (handle, mut inbox) = pool_port();
        let mut core = adapter_on(handle);
        publish_empty(&inbox);
        let peer = QueueSource::LegacySocket(([192, 0, 2, 1], 8233).into());
        let (fetching, _) = Fetching::start(&core.shared, filler_id(1));
        let request = tokio::spawn(core.call(Request::QueueFromPeer {
            transactions: vec![Gossip::Id(filler_id(1))],
            source: peer,
        }));
        let Some(PoolCommand::RejectedIds { reply, .. }) = inbox.next().await else {
            panic!("the batch looks its id up");
        };
        // The other fetch fails while the lookup waits.
        drop(fetching);
        let _ = reply.send(Default::default());
        let reserve = tokio::time::timeout(std::time::Duration::from_secs(5), inbox.next())
            .await
            .expect("the batch reserves the id rather than skipping it");
        let Some(PoolCommand::Reserve { tx_id, .. }) = reserve else {
            panic!("the batch reserves the id");
        };
        assert_eq!(tx_id, filler_id(1));
        // The reply is dropped, so the core reads as stopped and the batch ends.
        let answer = request.await.expect("the task does not panic");
        assert!(matches!(answer, Ok(Response::Queued(_))));
    }

    /// A snapshot at height 1 with nothing pooled, so the adapter is on.
    fn publish_empty(inbox: &zakura_node_services::mempool::port::PoolInbox) {
        use zakura_node_services::mempool::port::{ChainTip, PoolSnapshot};

        inbox.publish([], |last| {
            PoolSnapshot::new(
                ChainTip {
                    hash: block::Hash([1; 32]),
                    height: Height(1),
                },
                last,
                Vec::new(),
                Default::default(),
            )
        });
    }

    /// The adapter on `handle`, with a network that is never ready, an
    /// empty chain, no exempt peers, and a node that is current.
    fn adapter_on(handle: PoolHandle) -> CoreMempool {
        adapter_when(handle, |_| true)
    }

    /// [`adapter_on`], where `may_cool` says whether a cooldown earned at a
    /// height may start.
    fn adapter_when(
        handle: PoolHandle,
        may_cool: impl Fn(Height) -> bool + Send + Sync + 'static,
    ) -> CoreMempool {
        adapter_with(
            handle,
            may_cool,
            super::super::super::Config::default().max_transaction_bytes,
        )
    }

    /// [`adapter_when`], taking bodies of at most `max_transaction_bytes`.
    fn adapter_with(
        handle: PoolHandle,
        may_cool: impl Fn(Height) -> bool + Send + Sync + 'static,
        max_transaction_bytes: u64,
    ) -> CoreMempool {
        adapter_fetching(
            handle,
            may_cool,
            max_transaction_bytes,
            BoxCloneSyncService::new(NeverReady),
        )
    }

    /// [`adapter_with`], fetching through `network`.
    fn adapter_fetching(
        handle: PoolHandle,
        may_cool: impl Fn(Height) -> bool + Send + Sync + 'static,
        max_transaction_bytes: u64,
        network: Outbound,
    ) -> CoreMempool {
        let outbound = Timeout::new(network, TRANSACTION_DOWNLOAD_TIMEOUT);
        CoreMempool::new(
            handle,
            outbound,
            BoxCloneSyncService::new(NeverReady),
            Arc::new(may_cool),
            Arc::new(|| None),
            chain_holding(None),
            max_transaction_bytes,
            false,
            test_misbehavior::reporter(),
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
        let _ = reply.send(Ok(inbox.grant(ReservationId(1), id, Default::default())));
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
        let _ = reply.send(Ok(inbox.grant(ReservationId(1), tx_id, Default::default())));
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
        let core = adapter_on(handle);
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
        let _ = reply.send(Ok(inbox.grant(ReservationId(1), tx_id, Default::default())));

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

    /// What one push from [`peer`] costs that peer, when the core answers
    /// its submission with a cooldown charge earned at `tip`, and `may_cool`
    /// says whether a cooldown earned at a height may start. Returns whether
    /// the peer is cooling down afterward.
    async fn cooled_by_one_push(
        tip: Height,
        may_cool: impl Fn(Height) -> bool + Send + Sync + 'static,
    ) -> bool {
        use zakura_node_services::mempool::port::{
            pool_port, PoolCommand, Rejection, RejectionScope, ReservationId,
        };

        let (handle, mut inbox) = pool_port();
        let core = adapter_when(handle, may_cool);
        let body = Network::Mainnet
            .unmined_transactions_in_blocks(1..=1)
            .next()
            .expect("a vector transaction")
            .transaction;
        let queued = tokio::spawn(queue(core.shared.clone(), Gossip::Tx(body), Some(peer())));
        let Some(PoolCommand::Reserve { tx_id, reply, .. }) = inbox.next().await else {
            panic!("the push reserves first");
        };
        let _ = reply.send(Ok(inbox.grant(ReservationId(1), tx_id, Default::default())));
        let receiver = queued
            .await
            .expect("the task does not panic")
            .expect("the push passes the first stage");
        let (reservation, reply) = match inbox.next().await {
            Some(PoolCommand::Submit {
                reservation, reply, ..
            }) => (reservation, reply),
            other => panic!("the push is submitted: {other:?}"),
        };
        let _accepted = reservation.accept();
        let _ = reply.send(Verdict::Rejected(Rejection {
            scope: RejectionScope::ExactTip,
            charge: Charge::Cooldown,
            tip,
            detail: Arc::new(std::io::Error::other("invalid")),
        }));
        let _ = receiver.await.expect("the push is answered");
        let QueueSource::LegacySocket(addr) = peer() else {
            unreachable!("the test peer is a legacy peer");
        };
        core.shared
            .cooldowns
            .is_cooling_down(addr.ip(), Instant::now())
    }

    /// What one push from `peer` that the core rejects with `charge` did to
    /// that peer, with `exempt` peers and a node for which `may_cool` says
    /// whether a cooldown may start: whether it is cooling down, and the
    /// misbehavior reported for it, if any.
    async fn charged_by_one_push(
        peer: std::net::SocketAddr,
        charge: Charge,
        exempt: Exempt,
        may_cool: bool,
    ) -> (bool, Option<(PeerSocketAddr, u32)>) {
        use zakura_node_services::mempool::port::{
            pool_port, PoolCommand, Rejection, RejectionScope, ReservationId,
        };

        let (handle, mut inbox) = pool_port();
        let misbehavior = test_misbehavior::reporter();
        let core = CoreMempool::new(
            handle,
            Timeout::new(
                BoxCloneSyncService::new(NeverReady),
                TRANSACTION_DOWNLOAD_TIMEOUT,
            ),
            BoxCloneSyncService::new(NeverReady),
            Arc::new(move |_| may_cool),
            Arc::new(|| None),
            chain_holding(None),
            super::super::super::Config::default().max_transaction_bytes,
            false,
            misbehavior.clone(),
            Arc::new(exempt),
        );
        let body = Network::Mainnet
            .unmined_transactions_in_blocks(1..=1)
            .next()
            .expect("a vector transaction")
            .transaction;
        let queued = tokio::spawn(queue(
            core.shared.clone(),
            Gossip::Tx(body),
            Some(QueueSource::LegacySocket(peer)),
        ));
        let Some(PoolCommand::Reserve { tx_id, reply, .. }) = inbox.next().await else {
            panic!("the push reserves first");
        };
        let _ = reply.send(Ok(inbox.grant(ReservationId(1), tx_id, Default::default())));
        let receiver = queued
            .await
            .expect("the task does not panic")
            .expect("the push passes the first stage");
        let Some(PoolCommand::Submit {
            reservation, reply, ..
        }) = inbox.next().await
        else {
            panic!("the push is submitted");
        };
        let _accepted = reservation.accept();
        let _ = reply.send(Verdict::Rejected(Rejection {
            scope: RejectionScope::ExactTip,
            charge,
            tip: Height(0),
            detail: Arc::new(std::io::Error::other("invalid")),
        }));
        let _ = receiver.await.expect("the push is answered");
        (
            core.shared
                .cooldowns
                .is_cooling_down(peer.ip(), Instant::now()),
            match reported_score(&misbehavior, peer.into()) {
                0 => None,
                score => Some((peer.into(), score)),
            },
        )
    }

    /// A ban charge bans the legacy peer that pushed the body with today's
    /// full misbehavior score, whether or not the node is current, as today's
    /// `transaction_data_failure_bans_peer_without_a_current_tip` pins for
    /// today's pool. It starts no cooldown.
    #[tokio::test(flavor = "current_thread")]
    async fn a_ban_charge_bans_its_peer_even_when_the_node_is_not_current() {
        let peer = std::net::SocketAddr::from(([192, 0, 2, 1], 8233));
        for current in [true, false] {
            let (cooling, report) =
                charged_by_one_push(peer, Charge::Ban, Exempt::default(), current).await;
            assert!(!cooling, "a ban starts no cooldown");
            assert_eq!(
                report,
                Some((peer.into(), zn::constants::MAX_PEER_MISBEHAVIOR_SCORE)),
                "the peer is banned, current: {current}"
            );
        }
    }

    /// A zcashd-compat sidecar and a configured unconditional peer are never
    /// banned or cooled down, including through an IPv4-mapped address, as
    /// today's `zcashd_compat_peer_is_never_penalized` and
    /// `unconditional_peers_are_never_penalized` pin for today's pool. A
    /// peer outside both is charged.
    #[tokio::test(flavor = "current_thread")]
    async fn exempt_peers_are_never_charged() {
        let unconditional: zn::Config = toml::from_str("unconditional_peers = ['198.51.100.0/24']")
            .expect("a valid network config");
        let exempt = || Exempt {
            zcashd_compat: [std::net::IpAddr::from([192, 0, 2, 1])]
                .into_iter()
                .collect(),
            unconditional: unconditional.unconditional_peers.clone(),
        };
        for (address, charged) in [
            ("192.0.2.1:8233", false),
            ("[::ffff:192.0.2.1]:8233", false),
            ("198.51.100.7:8233", false),
            ("[::ffff:198.51.100.7]:8233", false),
            ("203.0.113.9:8233", true),
        ] {
            let peer: std::net::SocketAddr = address.parse().expect("a valid address");
            let (_, report) = charged_by_one_push(peer, Charge::Ban, exempt(), true).await;
            assert_eq!(report.is_some(), charged, "banned: {address}");
            let (cooling, report) =
                charged_by_one_push(peer, Charge::Cooldown, exempt(), true).await;
            assert_eq!(cooling, charged, "cooling down: {address}");
            assert!(report.is_none(), "a cooldown is not a ban: {address}");
        }
    }

    /// A query that arrives after the state has a new best tip, but before
    /// the core has applied it, waits for the core's pool of that tip, since
    /// today's pool applies a new tip before it answers. Without that, an RPC
    /// that looks a just-mined transaction up in the pool first finds it
    /// still there. A core that never catches up is answered with the pool it
    /// has after [`MAX_TIP_WAIT`].
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_query_waits_for_the_core_to_apply_the_states_best_tip() {
        use zakura_node_services::mempool::port::{pool_port, ChainTip, PoolSnapshot};

        let (handle, inbox) = pool_port();
        let best = Arc::new(std::sync::Mutex::new(block::Hash([2; 32])));
        let mut core = CoreMempool::new(
            handle,
            Timeout::new(
                BoxCloneSyncService::new(NeverReady),
                TRANSACTION_DOWNLOAD_TIMEOUT,
            ),
            BoxCloneSyncService::new(NeverReady),
            Arc::new(|_| true),
            {
                let best = best.clone();
                Arc::new(move || Some(*best.lock().expect("not poisoned")))
            },
            chain_holding(None),
            super::super::super::Config::default().max_transaction_bytes,
            false,
            test_misbehavior::reporter(),
            Arc::new(Exempt::default()),
        );
        let tip = |byte: u8| ChainTip {
            hash: block::Hash([byte; 32]),
            height: Height(u32::from(byte)),
        };
        let mined = Network::Mainnet
            .unmined_transactions_in_blocks(1..=1)
            .next()
            .expect("a vector transaction");
        // The core still holds a transaction the state's tip 2 mined.
        inbox.publish([], |last| {
            PoolSnapshot::new(tip(1), last, vec![mined], Default::default())
        });
        let mut query = core.call(Request::TransactionIds);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut query)
                .await
                .is_err(),
            "the query waits while the core is a tip behind"
        );
        inbox.publish([], |last| {
            PoolSnapshot::new(tip(2), last, Vec::new(), Default::default())
        });
        let Ok(Response::TransactionIds(ids)) = query.await else {
            panic!("`TransactionIds` is answered with ids");
        };
        assert!(ids.is_empty(), "answered from the pool of the state's tip");

        *best.lock().expect("not poisoned") = block::Hash([3; 32]);
        let started = tokio::time::Instant::now();
        let Ok(Response::TransactionIds(_)) = core.call(Request::TransactionIds).await else {
            panic!("`TransactionIds` is answered with ids");
        };
        assert!(
            started.elapsed() >= MAX_TIP_WAIT,
            "a core that never catches up is answered at the bound"
        );
    }

    /// A cooldown charge cools the peer that pushed the body only while the
    /// node is current and its best tip is still the one the failure was
    /// verified at, asked when the verdict arrives, as today's pool asks when
    /// it handles the failure.
    #[tokio::test(flavor = "current_thread")]
    async fn a_cooldown_charge_cools_its_peer_only_at_the_tip_it_was_earned_at() {
        let still_at_100 = |height| height == Height(100);
        assert!(cooled_by_one_push(Height(100), still_at_100).await);
        assert!(
            !cooled_by_one_push(Height(99), still_at_100).await,
            "the tip moved on after the failure was verified"
        );
        assert!(
            !cooled_by_one_push(Height(100), |_| false).await,
            "the node is not current"
        );
    }

    /// An oversized body from a cooling peer still reaches the core, which
    /// refuses it for its size and remembers that for its exact id, as
    /// today's downloader checks the size before the cooldown.
    #[tokio::test(flavor = "current_thread")]
    async fn an_oversized_body_from_a_cooling_peer_is_still_refused_for_its_size() {
        use zakura_node_services::mempool::port::{pool_port, PoolCommand, ReservationId};

        let (handle, mut inbox) = pool_port();
        let core = adapter_with(handle, |_| true, 1);
        let QueueSource::LegacySocket(addr) = peer() else {
            unreachable!("the test peer is a legacy peer");
        };
        core.shared
            .cooldowns
            .record_invalid_transaction(addr.ip(), Instant::now());
        let body = Network::Mainnet
            .unmined_transactions_in_blocks(1..=1)
            .next()
            .expect("a vector transaction")
            .transaction;
        let queued = tokio::spawn(queue(core.shared.clone(), Gossip::Tx(body), Some(peer())));
        let Some(PoolCommand::Reserve { tx_id, reply, .. }) = inbox.next().await else {
            panic!("the push reserves first");
        };
        let _ = reply.send(Ok(inbox.grant(ReservationId(1), tx_id, Default::default())));
        let _receiver = queued
            .await
            .expect("the task does not panic")
            .expect("the push passes the first stage");
        let Some(PoolCommand::Submit { reservation, .. }) = inbox.next().await else {
            panic!("the oversized body goes on to the core, which checks its size");
        };
        let _accepted = reservation.accept();
    }

    /// The adapter on `handle` with a network that is never ready, whose
    /// state's best tip is `best_tip`.
    fn adapter_at(handle: PoolHandle, best_tip: Option<block::Hash>) -> CoreMempool {
        CoreMempool::new(
            handle,
            Timeout::new(
                BoxCloneSyncService::new(NeverReady),
                TRANSACTION_DOWNLOAD_TIMEOUT,
            ),
            BoxCloneSyncService::new(NeverReady),
            Arc::new(|_| true),
            Arc::new(move || best_tip),
            chain_holding(None),
            super::super::super::Config::default().max_transaction_bytes,
            false,
            test_misbehavior::reporter(),
            Arc::new(Exempt::default()),
        )
    }

    /// A pushed body is reserved at once while the core is a tip behind the
    /// state: only queries wait for the core to catch up.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_push_does_not_wait_for_the_core_to_catch_up() {
        use zakura_node_services::mempool::port::{pool_port, ChainTip, PoolCommand, PoolSnapshot};

        let (handle, mut inbox) = pool_port();
        let mut core = adapter_at(handle, Some(block::Hash([2; 32])));
        inbox.publish([], |last| {
            PoolSnapshot::new(
                ChainTip {
                    hash: block::Hash([1; 32]),
                    height: Height(1),
                },
                last,
                Vec::new(),
                Default::default(),
            )
        });
        let body = Network::Mainnet
            .unmined_transactions_in_blocks(1..=1)
            .next()
            .expect("a vector transaction")
            .transaction;
        let push = tokio::spawn(core.call(Request::QueueFromPeer {
            transactions: vec![Gossip::Tx(body)],
            source: peer(),
        }));
        let soon = std::time::Duration::from_millis(1);
        let Ok(Some(PoolCommand::RejectedIds { reply, .. })) =
            tokio::time::timeout(soon, inbox.next()).await
        else {
            panic!("the push asks the core without waiting for the tip");
        };
        let _ = reply.send(Default::default());
        let Ok(Some(PoolCommand::Reserve { .. })) = tokio::time::timeout(soon, inbox.next()).await
        else {
            panic!("the push reserves without waiting for the tip");
        };
        push.abort();
    }

    /// A peer's batch of ids the pool already knows costs the core one
    /// command, and once the peer's limit is reached the rest of the batch
    /// is refused without asking the core, however long the batch is.
    #[tokio::test(flavor = "current_thread")]
    async fn a_peer_batch_costs_the_core_a_bounded_number_of_commands() {
        use zakura_node_services::mempool::port::{
            pool_port, ChainTip, PoolCommand, PoolSnapshot, ReservationId,
        };

        let (handle, mut inbox) = pool_port();
        let mut core = adapter_on(handle);
        let pooled = Network::Mainnet
            .unmined_transactions_in_blocks(..)
            .find(|tx| tx.transaction.id().auth_digest().is_some())
            .expect("the vectors have a witnessed transaction");
        let pooled_id = pooled.transaction.id();
        // The pooled transaction's effects under other authorizations, which
        // `reserve` would answer as pooled too.
        let other_authorizations: Vec<UnminedTxId> = (1..=10)
            .map(|n| {
                let mut id = pooled_id;
                id.auth_digest_mut().expect("a witnessed id").0[0] ^= n;
                id
            })
            .collect();
        inbox.publish([], |last| {
            PoolSnapshot::new(
                ChainTip {
                    hash: block::Hash([1; 32]),
                    height: Height(1),
                },
                last,
                vec![pooled],
                Default::default(),
            )
        });
        let id = |n: u16| {
            let [high, low] = n.to_be_bytes();
            let mut hash = [7; 32];
            hash[0] = high;
            hash[1] = low;
            UnminedTxId::Legacy(zakura_chain::transaction::Hash(hash))
        };
        let rejected_id = id(0);
        let mut batch = vec![Gossip::Id(pooled_id), Gossip::Id(rejected_id)];
        batch.extend(other_authorizations.iter().copied().map(Gossip::Id));
        batch.extend((1..1_000).map(|n| Gossip::Id(id(n))));
        let call = tokio::spawn(core.call(Request::QueueFromPeer {
            transactions: batch,
            source: peer(),
        }));

        let Some(PoolCommand::RejectedIds {
            ids: looked_up,
            reply,
        }) = inbox.next().await
        else {
            panic!("the batch asks once which ids are rejected");
        };
        let _ = reply.send(
            looked_up
                .intersection(&[rejected_id].into())
                .copied()
                .collect(),
        );
        assert!(
            !looked_up.contains(&pooled_id)
                && other_authorizations
                    .iter()
                    .all(|other| !looked_up.contains(other)),
            "a pooled transaction's effects are answered from the snapshot"
        );
        assert_eq!(looked_up.len(), 1_000, "every other id is looked up once");
        for granted in 0..MAX_INBOUND_CONCURRENCY_PER_PEER {
            let Some(PoolCommand::Reserve { tx_id, reply, .. }) = inbox.next().await else {
                panic!("the peer's limit of fetches reserves");
            };
            assert_ne!(tx_id, rejected_id, "a rejected id is not reserved");
            let id = ReservationId(u64::try_from(granted).expect("few"));
            let _ = reply.send(Ok(inbox.grant(id, tx_id, Default::default())));
        }
        let answer = tokio::time::timeout(std::time::Duration::from_secs(5), call)
            .await
            .expect("the rest of the batch is answered without the core")
            .expect("the task does not panic");
        assert!(matches!(answer, Ok(Response::Queued(_))));
        assert!(
            inbox.try_next().is_none(),
            "the rest of the batch is refused without the core"
        );
    }

    /// A submission to a core that takes no reservation is answered at
    /// today's whole-task deadline, so a stuck core cannot hold a caller
    /// forever.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_reservation_the_core_never_answers_ends_at_the_task_deadline() {
        use zakura_node_services::mempool::port::{pool_port, PoolCommand};

        let (handle, mut inbox) = pool_port();
        let core = adapter_on(handle);
        let body = Network::Mainnet
            .unmined_transactions_in_blocks(1..=1)
            .next()
            .expect("a vector transaction")
            .transaction;
        let started = tokio::time::Instant::now();
        let queued = tokio::spawn(queue(core.shared.clone(), Gossip::Tx(body), None));
        let Some(PoolCommand::Reserve { reply: _held, .. }) = inbox.next().await else {
            panic!("the body reserves");
        };
        let error = tokio::time::timeout(RATE_LIMIT_DELAY * 2, queued)
            .await
            .expect("the stuck reservation ends by the deadline")
            .expect("the task does not panic")
            .expect_err("the stuck reservation is refused");
        assert_eq!(error.to_string(), "timeout waiting for verification result");
        assert!(started.elapsed() >= RATE_LIMIT_DELAY, "at today's deadline");
    }

    /// A v2 peer through the adapter is held to the same five undecided
    /// transactions as a legacy peer: of six bodies pushed by one v2 peer,
    /// five take its slots and the sixth is dropped without asking the core.
    #[tokio::test(flavor = "current_thread")]
    async fn a_v2_peer_is_held_to_five_undecided_transactions() {
        use zakura_chain::transaction::{LockTime, Transaction, UnminedTx};
        use zakura_node_services::mempool::port::{pool_port, PoolCommand};

        let template = Network::Mainnet
            .unmined_transactions_in_blocks(1..=1)
            .next()
            .expect("a vector transaction")
            .transaction;
        let bodies: Vec<Gossip> = (1..=6)
            .map(|height| {
                let mut transaction = Transaction::clone(template.transaction());
                if let Transaction::V1 { lock_time, .. }
                | Transaction::V2 { lock_time, .. }
                | Transaction::V3 { lock_time, .. }
                | Transaction::V4 { lock_time, .. }
                | Transaction::V5 { lock_time, .. } = &mut transaction
                {
                    *lock_time = LockTime::Height(Height(height));
                }
                Gossip::Tx(UnminedTx::from(transaction))
            })
            .collect();
        let (handle, mut inbox) = pool_port();
        let mut core = adapter_on(handle);
        publish_empty(&inbox);
        let request = tokio::spawn(core.call(Request::QueueFromPeer {
            transactions: bodies,
            source: QueueSource::Zakura(vec![7; 32]),
        }));
        // The batch asks once which of its bodies the core has rejected.
        let Some(PoolCommand::RejectedIds { reply, .. }) = inbox.next().await else {
            panic!("the batch asks which ids are rejected");
        };
        let _ = reply.send(Default::default());
        // Its bodies are queued one after another. Each within the peer's
        // limit reserves, and the core says it is already pooled.
        for _ in 0..MAX_INBOUND_CONCURRENCY_PER_PEER {
            let Some(PoolCommand::Reserve { reply, .. }) = inbox.next().await else {
                panic!("each body within the peer's limit reserves");
            };
            let _ = reply.send(Err(Unreserved::Pooled));
        }
        let answer = tokio::time::timeout(std::time::Duration::from_secs(5), request)
            .await
            .expect("the batch is answered")
            .expect("the task does not panic");
        assert!(matches!(answer, Ok(Response::Queued(_))));
        assert!(
            inbox.try_next().is_none(),
            "the sixth body was dropped without asking the core"
        );
    }

    /// An id a v2 peer advertises is fetched from that peer, and a body it
    /// serves that the core rejects with a ban charges nobody, as today's
    /// pool never charges v2 peers.
    #[tokio::test(flavor = "current_thread")]
    async fn a_v2_advert_is_fetched_from_its_peer_and_never_charged() {
        use zakura_node_services::mempool::port::{
            pool_port, PoolCommand, Rejection, RejectionScope, ReservationId,
        };

        let body = Network::Mainnet
            .unmined_transactions_in_blocks(1..=1)
            .next()
            .expect("a vector transaction")
            .transaction;
        let id = body.id();
        let served_from: PeerSocketAddr = std::net::SocketAddr::from(([192, 0, 2, 7], 8233)).into();
        let asked: Arc<std::sync::Mutex<Vec<zn::Request>>> = Arc::default();
        let network = {
            let (asked, body) = (asked.clone(), body.clone());
            BoxCloneSyncService::new(tower::service_fn(move |request: zn::Request| {
                asked.lock().expect("not poisoned").push(request);
                let body = body.clone();
                async move {
                    Ok::<_, BoxError>(zn::Response::Transactions(vec![
                        zn::InventoryResponse::Available((body, Some(served_from))),
                    ]))
                }
            }))
        };
        let (handle, mut inbox) = pool_port();
        let misbehavior = test_misbehavior::reporter();
        let mut core = CoreMempool::new(
            handle,
            Timeout::new(network, TRANSACTION_DOWNLOAD_TIMEOUT),
            BoxCloneSyncService::new(NeverReady),
            Arc::new(|_| true),
            Arc::new(|| None),
            chain_holding(None),
            super::super::super::Config::default().max_transaction_bytes,
            false,
            misbehavior.clone(),
            Arc::new(Exempt::default()),
        );
        publish_empty(&inbox);
        let v2_peer = vec![7; 32];
        let request = tokio::spawn(core.call(Request::QueueFromPeer {
            transactions: vec![Gossip::Id(id)],
            source: QueueSource::Zakura(v2_peer.clone()),
        }));
        let Some(PoolCommand::RejectedIds { reply, .. }) = inbox.next().await else {
            panic!("the batch asks which ids are rejected");
        };
        let _ = reply.send(Default::default());
        let Some(PoolCommand::Reserve { tx_id, reply, .. }) = inbox.next().await else {
            panic!("the advert reserves");
        };
        let _ = reply.send(Ok(inbox.grant(ReservationId(1), tx_id, Default::default())));
        let Some(PoolCommand::Submit {
            reservation, reply, ..
        }) = inbox.next().await
        else {
            panic!("the fetched body is submitted");
        };
        let _accepted = reservation.accept();
        let _ = reply.send(Verdict::Rejected(Rejection {
            scope: RejectionScope::ExactTip,
            charge: Charge::Ban,
            tip: Height(0),
            detail: Arc::new(std::io::Error::other("invalid")),
        }));
        let _ = request.await;
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        let asked = asked.lock().expect("not poisoned");
        assert!(
            matches!(
                asked.as_slice(),
                [zn::Request::TransactionsByIdFrom { source: zn::PeerSource::Zakura(peer), .. }]
                    if peer.as_bytes() == v2_peer.as_slice()
            ),
            "fetched from the advertising v2 peer: {asked:?}"
        );
        assert_eq!(
            reported_score(&misbehavior, served_from),
            0,
            "a v2 peer is never charged"
        );
    }

    /// The shortcut for a body the pool holds needs a snapshot of the
    /// state's best tip: with the core a tip behind, a newer block may have
    /// mined it, so the body goes to the core, which knows.
    #[tokio::test(flavor = "current_thread")]
    async fn a_pooled_body_on_a_stale_snapshot_goes_to_the_core() {
        use zakura_node_services::mempool::port::{pool_port, ChainTip, PoolCommand, PoolSnapshot};

        let pooled = Network::Mainnet
            .unmined_transactions_in_blocks(1..=1)
            .next()
            .expect("a vector transaction");
        let body = pooled.transaction.clone();
        let (handle, mut inbox) = pool_port();
        let mut core = adapter_at(handle, Some(block::Hash([2; 32])));
        inbox.publish([], |last| {
            PoolSnapshot::new(
                ChainTip {
                    hash: block::Hash([1; 32]),
                    height: Height(1),
                },
                last,
                vec![pooled],
                Default::default(),
            )
        });
        let request = tokio::spawn(core.call(Request::Queue(vec![Gossip::Tx(body)])));
        let Ok(Some(PoolCommand::Reserve { reply, .. })) =
            tokio::time::timeout(std::time::Duration::from_secs(5), inbox.next()).await
        else {
            panic!("the body goes to the core");
        };
        let _ = reply.send(Err(Unreserved::Pooled));
        let Ok(Ok(Response::Queued(answers))) = request.await else {
            panic!("`Queue` is answered with `Queued`");
        };
        assert!(matches!(
            answers[0]
                .as_ref()
                .map_err(|error| error.downcast_ref::<MempoolError>()),
            Err(Some(MempoolError::InMempool))
        ));
    }

    /// While the pool is off, a submission is answered in the call itself,
    /// so its body is dropped there and an unpolled response holds nothing,
    /// as today's pool drops it in the call.
    #[tokio::test(flavor = "current_thread")]
    async fn an_inactive_pool_drops_a_submission_in_the_call() {
        use zakura_node_services::mempool::port::pool_port;

        let body = Network::Mainnet
            .unmined_transactions_in_blocks(1..=1)
            .next()
            .expect("a vector transaction")
            .transaction;
        let (handle, _inbox) = pool_port();
        let mut core = adapter_on(handle);
        let unpolled = core.call(Request::Queue(vec![Gossip::Tx(body.clone())]));
        // The body shares its transaction with this copy, so the copy is the
        // only holder once the call has dropped the request's.
        assert_eq!(
            std::sync::Arc::strong_count(body.transaction()),
            1,
            "the call dropped the request's body"
        );
        drop(unpolled);
    }

    /// A peer's batch takes at most the peer's limit of ids, in one pass, as
    /// today's pool queues a batch at once: even when each fetch ends at
    /// once and frees its slot, the rest of the batch is not worked through.
    #[tokio::test(flavor = "current_thread")]
    async fn a_peer_batch_takes_at_most_five_ids_however_fast_they_finish() {
        use zakura_node_services::mempool::port::{pool_port, PoolCommand};

        let (handle, mut inbox) = pool_port();
        let mut core = adapter_on(handle);
        publish_empty(&inbox);
        let ids: Vec<Gossip> = (0..20u8)
            .map(|n| {
                let mut hash = [6; 32];
                hash[0] = n;
                Gossip::Id(UnminedTxId::Legacy(zakura_chain::transaction::Hash(hash)))
            })
            .collect();
        let request = tokio::spawn(core.call(Request::QueueFromPeer {
            transactions: ids,
            source: peer(),
        }));
        let Some(PoolCommand::RejectedIds { reply, .. }) = inbox.next().await else {
            panic!("the batch asks which ids are rejected");
        };
        let _ = reply.send(Default::default());
        let mut reserved = 0;
        // Each reservation is answered at once, freeing its slot.
        while let Ok(Some(PoolCommand::Reserve { reply, .. })) =
            tokio::time::timeout(std::time::Duration::from_millis(200), inbox.next()).await
        {
            reserved += 1;
            let _ = reply.send(Err(Unreserved::Pooled));
        }
        assert_eq!(reserved, MAX_INBOUND_CONCURRENCY_PER_PEER, "only five");
        let _ = request.await;
    }
}
