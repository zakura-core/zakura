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
//! cooldowns. Today's node-wide [`MAX_INBOUND_CONCURRENCY`] is here too, so a
//! body waiting for a slow core's reservation still counts against it. A verdict says what its body earned, and this applies it to the
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

use zakura_chain::{
    block::{self, Height},
    transaction::UnminedTxId,
};
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
            TransactionDownloadVerifyError, MAX_INBOUND_CONCURRENCY,
            MAX_INBOUND_CONCURRENCY_PER_PEER,
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

/// The longest a request waits for the core to apply the state's best tip.
/// The core applies a block within milliseconds, so this is only reached if
/// the core is stuck, and the request then gets the pool the core has.
const MAX_TIP_WAIT: std::time::Duration = std::time::Duration::from_secs(1);

/// The longest a request waits for the core to answer a lookup, such as
/// which ids it has rejected. Today's pool answers these at once, so this is
/// reached only if the core takes commands too slowly, and the request then
/// answers as if nothing were rejected.
///
/// It fits well inside the inbound service's own five-second request
/// budget, so a stalled lookup reaches its fallback before inbound gives up.
const MAX_LOOKUP_WAIT: std::time::Duration = std::time::Duration::from_secs(1);

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
    /// Queued items not yet answered at their second stage, at most
    /// [`MAX_INBOUND_CONCURRENCY`]. See [`QueueSlot`].
    queued: std::sync::Mutex<usize>,
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
    /// `misbehavior_sender`. It never charges a peer in `exempt`, starts a
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
        misbehavior_sender: mpsc::Sender<(PeerSocketAddr, u32)>,
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
                queued: std::sync::Mutex::new(0),
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
            Some(snapshot) if reads_snapshot(&request) && shared.is_behind(&snapshot) => {
                async move {
                    match caught_up(&shared).await {
                        Some(snapshot) => answer(shared, snapshot, request).await,
                        None => inactive_answer(request),
                    }
                }
                .boxed()
            }
            Some(snapshot) => answer(shared, snapshot, request).boxed(),
            None => async move { inactive_answer(request) }.boxed(),
        }
    }
}

/// Which of `ids` the core has rejected, or none if it does not answer
/// within [`MAX_LOOKUP_WAIT`].
async fn rejected_ids(
    shared: &Shared,
    ids: std::collections::HashSet<UnminedTxId>,
) -> std::collections::HashSet<UnminedTxId> {
    tokio::time::timeout(MAX_LOOKUP_WAIT, shared.handle.rejected_ids(ids))
        .await
        .unwrap_or_default()
}

/// Whether `request` is answered from the snapshot alone. Only these wait
/// for the core to apply the state's best tip, so a submission takes its
/// limits before it waits for anything. See [`caught_up`].
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
            Response::RejectedTransactionIds(rejected_ids(&shared, ids).await)
        }
        Request::Queue(items) => {
            // Every body takes its slots before anything here waits, and the
            // bodies are queued before the ids, so a body waits only for the
            // bodies before it, all under deadlines that started together.
            // An id takes its slots at its turn, as today's pool checks each
            // in order. The answers keep the request's order.
            let (bodies, ids): (Vec<(usize, Gossip)>, Vec<(usize, Gossip)>) = items
                .into_iter()
                .enumerate()
                .partition(|(_, item)| matches!(item, Gossip::Tx(_)));
            let mut answers: Vec<Option<Result<Receiver, BoxError>>> =
                std::iter::repeat_with(|| None)
                    .take(bodies.len() + ids.len())
                    .collect();
            // A body the pool already holds is answered before the limits, as
            // today's pool answers it, so it cannot take a fresh body's slot.
            let bodies: Vec<(usize, Staged)> = bodies
                .into_iter()
                .filter_map(|(at, item)| {
                    if snapshot.transaction(&item.id().mined_id()).is_some() {
                        answers[at] = Some(Err(MempoolError::InMempool.into()));
                        return None;
                    }
                    Some((at, Staged::new(&shared, item, &None)))
                })
                .collect();
            // Bodies holding slots go before the ones over a limit, whose
            // probes have deadlines of their own.
            let (held, over): (Vec<(usize, Staged)>, Vec<(usize, Staged)>) = bodies
                .into_iter()
                .partition(|(_, body)| matches!(body, Staged::Held { .. }));
            for (at, body) in held.into_iter().chain(over) {
                answers[at] = Some(body.queue(shared.clone(), None).await);
            }
            for (at, id) in ids {
                answers[at] = Some(
                    Staged::new(&shared, id, &None)
                        .queue(shared.clone(), None)
                        .await,
                );
            }
            Response::Queued(
                answers
                    .into_iter()
                    .map(|answer| answer.expect("every item is answered"))
                    .collect(),
            )
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
            let from = Some(source.clone());
            let (bodies, ids): (Vec<Gossip>, Vec<Gossip>) = transactions
                .into_iter()
                .partition(|item| matches!(item, Gossip::Tx(_)));
            // A body the pool already holds is skipped before the limits, as
            // today's pool skips it, so it cannot take a fresh body's slot.
            let bodies: Vec<Gossip> = bodies
                .into_iter()
                .filter(|body| snapshot.transaction(&body.id().mined_id()).is_none())
                .collect();
            // Every body takes its slots before anything here waits. Today's
            // pool drops what it cannot queue from a peer, so a body over a
            // limit is dropped here without asking the core.
            let bodies: Vec<Staged> = bodies
                .into_iter()
                .map(|item| Staged::new(&shared, item, &from))
                .collect();
            for body in bodies {
                match body {
                    Staged::Over { peer_full, .. } => {
                        if peer_full {
                            metrics::counter!("mempool.full_queue.per_peer.total").increment(1);
                        }
                    }
                    held => {
                        let _ = held.queue(shared.clone(), from.clone()).await;
                    }
                }
            }
            // Today's pool checks what it already knows about each id before
            // its limits, at no cost. Here that is one look at the snapshot
            // and one core command for the whole batch, so a batch of known
            // ids costs the core one command, not one for each. A pooled
            // transaction is known by its effects, as `reserve` knows it.
            let fresh: Vec<UnminedTxId> = ids
                .iter()
                .map(Gossip::id)
                .filter(|id| snapshot.transaction(&id.mined_id()).is_none())
                .collect();
            let rejected = if fresh.is_empty() {
                Default::default()
            } else {
                rejected_ids(&shared, fresh.iter().copied().collect()).await
            };
            let mut fresh = fresh
                .into_iter()
                .filter(|id| !rejected.contains(id))
                .peekable();
            // Once either limit is reached, the rest of the batch is refused
            // here, as today's would refuse each item, without asking the
            // core.
            while let Some(id) = fresh.next() {
                if !shared.has_room(&source) {
                    if !shared.node_has_room() {
                        break;
                    }
                    let refused = 1 + fresh.count();
                    metrics::counter!("mempool.full_queue.per_peer.total")
                        .increment(u64::try_from(refused).unwrap_or(u64::MAX));
                    break;
                }
                let _ = queue(shared.clone(), Gossip::Id(id), from.clone()).await;
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
    Staged::new(&shared, item, &from)
        .queue(shared.clone(), from)
        .await
}

/// An item that has taken, or failed to take, its slots: the node's
/// limit, then its peer's, as today, both before the core is asked for a
/// slot, so a body never waits for the core outside them.
enum Staged {
    /// Within both limits.
    Held {
        item: Gossip,
        queue_slot: QueueSlot,
        peer_slot: Option<PeerSlot>,
        /// Today's one deadline for the item, `RATE_LIMIT_DELAY` from when it
        /// took its slots. Every wait the item makes runs under it.
        deadline: tokio::time::Instant,
    },
    /// Over the node's limit, or its peer's if `peer_full`. Its body, if
    /// it had one, is already dropped.
    Over {
        id: UnminedTxId,
        intent: Intent,
        peer_full: bool,
    },
}

impl Staged {
    /// Takes `item`'s slots now, for an item from `from`.
    fn new(shared: &Arc<Shared>, item: Gossip, from: &Option<QueueSource>) -> Self {
        let over = |item: Gossip, peer_full| Self::Over {
            id: item.id(),
            intent: intent_of(&item),
            peer_full,
        };
        let Some(queue_slot) = QueueSlot::take(shared) else {
            return over(item, false);
        };
        let peer_slot = match from {
            Some(peer) => match PeerSlot::take(shared, peer) {
                Some(slot) => Some(slot),
                None => return over(item, true),
            },
            None => None,
        };
        Self::Held {
            item,
            queue_slot,
            peer_slot,
            deadline: tokio::time::Instant::now() + RATE_LIMIT_DELAY,
        }
    }

    /// Reserves the held item's slot in the core and answers its first
    /// stage, leaving a task that answers its second. An item over a limit
    /// is answered by [`known_or_full`].
    async fn queue(
        self,
        shared: Arc<Shared>,
        from: Option<QueueSource>,
    ) -> Result<Receiver, BoxError> {
        match self {
            Self::Held {
                item,
                queue_slot,
                peer_slot,
                deadline,
            } => queue_held(shared, item, queue_slot, peer_slot, from, deadline).await,
            Self::Over {
                id,
                intent,
                peer_full,
            } => known_or_full(&shared, id, intent, peer_full).await,
        }
    }
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
            let _ = shared
                .misbehavior_sender
                .try_send((peer, zn::constants::MAX_PEER_MISBEHAVIOR_SCORE));
        }
    }
}

/// Tells the core that `reservation`'s fetch failed, from a task of its own,
/// so a core slow to take commands cannot hold the caller's answer or its
/// slots past the deadline. The failure still reaches the change stream when
/// the core takes the command. Each such task holds only the reservation,
/// which the core granted, so there are at most as many as its slots.
fn report_failed(shared: &Shared, reservation: Reservation) {
    let handle = shared.handle.clone();
    tokio::spawn(async move { handle.fetch_failed(reservation).await });
}

/// Today's answer for an item whose task reached its deadline.
fn timed_out() -> BoxError {
    metrics::counter!("mempool.failed.verify.tasks.total", "reason" => "timeout").increment(1);
    "timeout waiting for verification result".into()
}

/// What a reservation for `item` asks of the core.
fn intent_of(item: &Gossip) -> Intent {
    match item {
        Gossip::Tx(_) => Intent::Deliver,
        Gossip::Id(_) => Intent::Fetch,
    }
}

/// Queues `item` under its slots: reserves its core slot and answers the
/// first stage, and leaves a task that brings the body in and answers the
/// second, all by `deadline`, which started when the item took its slots.
/// So a core that takes no commands cannot hold the slots and the caller
/// past it, however long the item waited for its turn.
async fn queue_held(
    shared: Arc<Shared>,
    item: Gossip,
    queue_slot: QueueSlot,
    slot: Option<PeerSlot>,
    from: Option<QueueSource>,
    deadline: tokio::time::Instant,
) -> Result<Receiver, BoxError> {
    // An item whose deadline passed while earlier items were queued starts
    // nothing.
    if tokio::time::Instant::now() >= deadline {
        return Err(timed_out());
    }
    // The reservation is asked for by a task of its own that holds the
    // item's node-wide slot, so a stalled core cannot gather more of these
    // than the node's limit, however its callers leave. The grant comes back
    // as a `Handoff`: a caller that timed out or was cancelled leaves it to
    // be dropped, which reports the fetch as failed, as today's timeout is
    // reported.
    let (granted, grant) = oneshot::channel();
    tokio::spawn({
        let shared = shared.clone();
        let (id, intent) = (item.id(), intent_of(&item));
        async move {
            let answer = match shared.handle.reserve(id, intent).await {
                Ok(reservation) => Ok(Handoff {
                    reservation: Some(reservation),
                    queue_slot: Some(queue_slot),
                    shared: shared.clone(),
                }),
                Err(refused) => Err(refused),
            };
            let _ = granted.send(answer);
        }
    });
    let (reservation, queue_slot) = match tokio::time::timeout_at(deadline, grant).await {
        Ok(Ok(Ok(handoff))) => handoff.take(),
        Ok(Ok(Err(refused))) => return first_stage_refusal(refused),
        Ok(Err(_lost)) => return Err("the pool core's reservation task failed".into()),
        Err(_elapsed) => return Err(timed_out()),
    };
    // A joined submission takes no core slot, but it still counts against
    // its peer until its verdict, so one peer cannot hold more than its
    // share of waiters.
    metrics::counter!("mempool.queued.transactions.total").increment(1);
    let (answer, receiver) = oneshot::channel();
    tokio::spawn(async move {
        // Today's download task has one deadline for the fetch and the
        // verification together, and its peer's slot is free once it ends.
        // So does this.
        let result = bring_in(shared, reservation, item, from, deadline, queue_slot).await;
        drop(slot);
        let _ = answer.send(result);
    });
    Ok(receiver)
}

/// A granted reservation on its way to its caller, with the item's
/// node-wide slot. Dropped without being taken, because its caller timed
/// out or was cancelled first, it reports the fetch as failed and frees the
/// slot.
struct Handoff {
    reservation: Option<Reservation>,
    queue_slot: Option<QueueSlot>,
    shared: Arc<Shared>,
}

impl Handoff {
    /// The reservation and the slot, for a caller that is still waiting.
    fn take(mut self) -> (Reservation, QueueSlot) {
        (
            self.reservation
                .take()
                .expect("a handoff holds its reservation until it is taken"),
            self.queue_slot
                .take()
                .expect("a handoff holds its slot until it is taken"),
        )
    }
}

impl Drop for Handoff {
    /// Reports a reservation nobody took as a failed fetch. The slot goes
    /// with it.
    fn drop(&mut self) {
        if let Some(reservation) = self.reservation.take() {
            report_failed(&self.shared, reservation);
        }
    }
}

/// The first-stage answer for an item with id `id` when the node's limit,
/// or its peer's if `peer_full`, is reached. Today's pool answers what it
/// already knows about an id before it checks its limits: in the pool,
/// already queued, or a cached rejection, some of which it answers at the
/// second stage. So the core is asked about the id alone, the body having
/// been dropped, and an id it does not know is refused as a full queue.
async fn known_or_full(
    shared: &Shared,
    id: UnminedTxId,
    intent: Intent,
    peer_full: bool,
) -> Result<Receiver, BoxError> {
    let full = || {
        if peer_full {
            metrics::counter!("mempool.full_queue.per_peer.total").increment(1);
        }
        Err(MempoolError::FullQueue.into())
    };
    let Ok(reserved) =
        tokio::time::timeout(RATE_LIMIT_DELAY, shared.handle.reserve(id, intent)).await
    else {
        return Err(timed_out());
    };
    match reserved {
        // A body for an id another caller is bringing in would join it.
        Ok(reservation) if reservation.joined() => Err(MempoolError::AlreadyQueued.into()),
        // A slot the core grants here is given back at once.
        Ok(_) | Err(Unreserved::NoRoom) => full(),
        Err(refused) => first_stage_refusal(refused),
    }
}

/// One of today's [`MAX_INBOUND_CONCURRENCY`] node-wide slots, held from the
/// request until its item is answered at the second stage, as today's
/// downloader counts a queued item. That bounds the bodies this adapter holds,
/// including ones waiting for a reservation. Dropping it frees the slot.
struct QueueSlot {
    shared: Arc<Shared>,
}

impl QueueSlot {
    /// Takes a slot, or `None` if all are in use.
    fn take(shared: &Arc<Shared>) -> Option<Self> {
        let mut queued = shared
            .queued
            .lock()
            .expect("no code panics while holding the queued count");
        if *queued >= MAX_INBOUND_CONCURRENCY {
            set_queued_gauge(*queued);
            return None;
        }
        *queued += 1;
        set_queued_gauge(*queued);
        Some(Self {
            shared: shared.clone(),
        })
    }
}

impl Drop for QueueSlot {
    /// Frees the slot.
    fn drop(&mut self) {
        let mut queued = self
            .shared
            .queued
            .lock()
            .expect("no code panics while holding the queued count");
        *queued = queued.saturating_sub(1);
        set_queued_gauge(*queued);
    }
}

/// Sets today's `mempool.currently.queued.transactions` gauge. Called under
/// the count's lock, so the last value written is the current count.
fn set_queued_gauge(queued: usize) {
    // A count of at most `MAX_INBOUND_CONCURRENCY`, far below where `f64`
    // loses precision.
    metrics::gauge!("mempool.currently.queued.transactions").set(queued as f64);
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
/// on its way to the core when the deadline passes keeps `queue_slot`, the
/// node-wide slot that bounds the bodies here, until the core has decided
/// it, and its outcome reaches the change stream then.
async fn bring_in(
    shared: Arc<Shared>,
    reservation: Reservation,
    item: Gossip,
    from: Option<QueueSource>,
    deadline: tokio::time::Instant,
    queue_slot: QueueSlot,
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
                report_failed(&shared, reservation);
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
                    report_failed(&shared, reservation);
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
        report_failed(&shared, reservation);
        return Err(format!(
            "failed to validate tx: {tx_id}, error: {}",
            TransactionDownloadVerifyError::PeerCoolingDown
        )
        .into());
    }
    // A v2 peer's body is not charged, as today, and nor is an exempt peer's.
    // A joined caller whose copy the core did not need is not charged
    // either: the verdict is about another caller's body.
    let supplier = match &from {
        Some(QueueSource::Zakura(_)) => None,
        _ => sender.and_then(|addr| shared.supplier(addr)),
    }
    .filter(|_| !reservation.joined() || reservation.wants_body());
    // The body goes to the core from a task of its own, which the deadline
    // does not cancel: a caller that times out first gets its answer and
    // frees its peer's slot, and the core still takes the body when it can,
    // so its outcome reaches the change stream as today's timeout does. The
    // task keeps the body's node-wide slot until then, so these bodies stay
    // within the node's limit.
    let submitting = tokio::spawn({
        let shared = shared.clone();
        async move {
            let verdict = shared.handle.submit(reservation, tx).await;
            // Whenever the verdict arrives, its charge goes to the peer
            // that served the body, as the component that owns peer state.
            charge(&shared, &verdict, supplier, tx_id);
            drop(queue_slot);
            verdict
        }
    });
    let verdict = match tokio::time::timeout_at(deadline, submitting).await {
        Ok(Ok(verdict)) => verdict,
        Ok(Err(_panicked)) => return Err("the pool core's submission task failed".into()),
        Err(_elapsed) => return Err(timed_out()),
    };
    second_stage(tx_id, verdict)
}

impl Shared {
    /// Whether the node's limit and `peer`'s both have room for one more
    /// item now.
    fn has_room(&self, peer: &QueueSource) -> bool {
        self.node_has_room()
            && self
                .per_peer
                .lock()
                .expect("no code panics while holding the per-peer counts")
                .get(peer)
                .is_none_or(|count| *count < MAX_INBOUND_CONCURRENCY_PER_PEER)
    }

    /// Whether the node's limit has room for one more item now.
    fn node_has_room(&self) -> bool {
        *self
            .queued
            .lock()
            .expect("no code panics while holding the queued count")
            < MAX_INBOUND_CONCURRENCY
    }

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

    /// What one push from [`peer`] costs that peer, when the core answers
    /// its reservation with `joined` and `wants_body` and its submission
    /// with a cooldown charge earned at `tip`, and `may_cool` says whether a
    /// cooldown earned at a height may start. Returns whether the peer is
    /// cooling down afterward.
    async fn cooled_by_one_push(
        joined: bool,
        wants_body: bool,
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
        let _ = reply.send(Ok(inbox.grant(
            ReservationId(1),
            tx_id,
            joined,
            wants_body,
            Default::default(),
        )));
        let receiver = queued
            .await
            .expect("the task does not panic")
            .expect("the push passes the first stage");
        let (reservation, reply) = match inbox.next().await {
            Some(PoolCommand::Submit {
                reservation, reply, ..
            })
            | Some(PoolCommand::Join { reservation, reply }) => (reservation, reply),
            other => panic!("the push is submitted or joined: {other:?}"),
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
        let (misbehavior, mut reports) = mpsc::channel(1);
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
            misbehavior,
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
            reports.try_recv().ok(),
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
        use zakura_node_services::mempool::port::{pool_port, ChainTip, Generation, PoolSnapshot};

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
            mpsc::channel(1).0,
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
            PoolSnapshot::new(tip(1), Generation(1), last, vec![mined], Default::default())
        });
        let mut query = core.call(Request::TransactionIds);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut query)
                .await
                .is_err(),
            "the query waits while the core is a tip behind"
        );
        inbox.publish([], |last| {
            PoolSnapshot::new(tip(2), Generation(2), last, Vec::new(), Default::default())
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
        assert!(cooled_by_one_push(false, true, Height(100), still_at_100).await);
        assert!(
            !cooled_by_one_push(false, true, Height(99), still_at_100).await,
            "the tip moved on after the failure was verified"
        );
        assert!(
            !cooled_by_one_push(false, true, Height(100), |_| false).await,
            "the node is not current"
        );
    }

    /// A joined push whose copy the core did not need is not charged for the
    /// verdict on another caller's body. One the core asked for its body is.
    #[tokio::test(flavor = "current_thread")]
    async fn a_joined_push_is_charged_only_for_its_own_body() {
        assert!(!cooled_by_one_push(true, false, Height(100), |_| true).await);
        assert!(cooled_by_one_push(true, true, Height(100), |_| true).await);
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
        let _ = reply.send(Ok(inbox.grant(
            ReservationId(1),
            tx_id,
            false,
            true,
            Default::default(),
        )));
        let _receiver = queued
            .await
            .expect("the task does not panic")
            .expect("the push passes the first stage");
        let Some(PoolCommand::Submit { reservation, .. }) = inbox.next().await else {
            panic!("the oversized body goes on to the core, which checks its size");
        };
        let _accepted = reservation.accept();
    }

    /// The decoded-body handoff gate: with the core's slots full and its
    /// task no longer taking commands, completed fetches hold no more
    /// decoded bodies than today's bound, one per slot. Every offer past the
    /// slots is refused before anything is fetched.
    #[tokio::test(flavor = "current_thread")]
    async fn decoded_bodies_stay_within_the_slots_while_the_core_is_paused() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use zakura_chain::transaction::{LockTime, Transaction, UnminedTx};
        use zakura_node_services::mempool::port::{
            pool_port, PoolCommand, ReservationId, MAX_PENDING,
        };

        let offered = MAX_PENDING + 100;
        let template = Network::Mainnet
            .unmined_transactions_in_blocks(1..=1)
            .next()
            .expect("a vector transaction")
            .transaction;
        // One distinct body for each offer.
        let bodies: Arc<HashMap<UnminedTxId, UnminedTx>> = Arc::new(
            (0..offered)
                .map(|n| {
                    let mut transaction = Transaction::clone(template.transaction());
                    let height = u32::try_from(n + 1).expect("few offers");
                    if let Transaction::V1 { lock_time, .. }
                    | Transaction::V2 { lock_time, .. }
                    | Transaction::V3 { lock_time, .. }
                    | Transaction::V4 { lock_time, .. }
                    | Transaction::V5 { lock_time, .. } = &mut transaction
                    {
                        *lock_time = LockTime::Height(Height(height));
                    }
                    let body = UnminedTx::from(transaction);
                    (body.id(), body)
                })
                .collect(),
        );
        assert_eq!(bodies.len(), offered, "each offer has its own body");
        // A network that serves every body it is asked for, and counts them.
        let served = Arc::new(AtomicUsize::new(0));
        let network = {
            let (bodies, served) = (bodies.clone(), served.clone());
            BoxCloneSyncService::new(tower::service_fn(move |request: zn::Request| {
                let (bodies, served) = (bodies.clone(), served.clone());
                async move {
                    let ids = match request {
                        zn::Request::TransactionsById(ids)
                        | zn::Request::TransactionsByIdFrom { ids, .. } => ids,
                        other => unreachable!("only bodies are fetched: {other:?}"),
                    };
                    let found: Vec<_> = ids
                        .iter()
                        .filter_map(|id| bodies.get(id).cloned())
                        .map(|body| zn::InventoryResponse::Available((body, None)))
                        .collect();
                    served.fetch_add(found.len(), Ordering::SeqCst);
                    Ok::<_, BoxError>(zn::Response::Transactions(found))
                }
            }))
        };

        let (handle, mut inbox) = pool_port();
        let core = adapter_fetching(
            handle,
            |_| true,
            super::super::super::Config::default().max_transaction_bytes,
            network,
        );
        // Five offers from each of many peers, so no peer's limit is reached.
        let peer = |n: usize| {
            let n = u16::try_from(n / MAX_INBOUND_CONCURRENCY_PER_PEER).expect("few peers");
            let [high, low] = n.to_be_bytes();
            QueueSource::LegacySocket(std::net::SocketAddr::from(([10, 1, high, low], 8233)))
        };
        let ids: Vec<UnminedTxId> = bodies.keys().copied().collect();
        let queued: Vec<_> = ids
            .iter()
            .enumerate()
            .map(|(n, id)| tokio::spawn(queue(core.shared.clone(), Gossip::Id(*id), Some(peer(n)))))
            .collect();

        // The core grants a slot to each offer that took the adapter's, and
        // answers the offers past the node's limit, which ask about their ids
        // alone, that it has no room, then stops taking commands. The
        // reservations come from tasks of their own, so they can arrive in
        // any order.
        let within: std::collections::HashSet<UnminedTxId> =
            ids[..MAX_PENDING].iter().copied().collect();
        let mut granted = 0;
        for _ in 0..offered {
            let Some(PoolCommand::Reserve { tx_id, reply, .. }) = inbox.next().await else {
                panic!("each offer reserves, or asks about its id");
            };
            let answer = if within.contains(&tx_id) {
                granted += 1;
                let id = ReservationId(granted);
                Ok(inbox.grant(id, tx_id, false, true, Default::default()))
            } else {
                Err(Unreserved::NoRoom)
            };
            let _ = reply.send(answer);
        }
        let mut refused = 0;
        for task in queued {
            if task.await.expect("the task does not panic").is_err() {
                refused += 1;
            }
        }
        assert_eq!(refused, offered - MAX_PENDING, "the offers past the slots");

        // Every granted fetch completes while the core takes nothing.
        for _ in 0..1_000 {
            if served.load(Ordering::SeqCst) == MAX_PENDING {
                break;
            }
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(
            served.load(Ordering::SeqCst),
            MAX_PENDING,
            "one decoded body per slot, and none for a refused offer"
        );
        let mut waiting = 0;
        while let Some(command) = inbox.try_next() {
            let PoolCommand::Submit { reservation, .. } = command else {
                panic!("only bodies wait for the core: {command:?}");
            };
            let _accepted = reservation.accept();
            waiting += 1;
        }
        assert_eq!(waiting, MAX_PENDING, "each body waits under its slot");
    }

    /// Bodies pushed while the core takes no commands wait for their
    /// reservations under today's node-wide limit, so the adapter holds no
    /// more of them than today's pool would. The pushes past the limit are
    /// refused at once as a full queue.
    #[tokio::test(flavor = "current_thread")]
    async fn pushed_bodies_waiting_for_a_paused_core_stay_within_the_node_limit() {
        use zakura_chain::transaction::{LockTime, Transaction, UnminedTx};
        use zakura_node_services::mempool::port::{pool_port, PoolCommand};

        let pushed = MAX_INBOUND_CONCURRENCY + 100;
        let template = Network::Mainnet
            .unmined_transactions_in_blocks(1..=1)
            .next()
            .expect("a vector transaction")
            .transaction;
        let (handle, mut inbox) = pool_port();
        let core = adapter_on(handle);
        // Five pushes from each of many peers, so no peer's limit is reached.
        let pushes: Vec<_> = (0..pushed)
            .map(|n| {
                let mut transaction = Transaction::clone(template.transaction());
                let height = u32::try_from(n + 1).expect("few pushes");
                if let Transaction::V1 { lock_time, .. }
                | Transaction::V2 { lock_time, .. }
                | Transaction::V3 { lock_time, .. }
                | Transaction::V4 { lock_time, .. }
                | Transaction::V5 { lock_time, .. } = &mut transaction
                {
                    *lock_time = LockTime::Height(Height(height));
                }
                let peer = u16::try_from(n / MAX_INBOUND_CONCURRENCY_PER_PEER).expect("few peers");
                let [high, low] = peer.to_be_bytes();
                let peer = QueueSource::LegacySocket(std::net::SocketAddr::from((
                    [10, 2, high, low],
                    8233,
                )));
                let body = UnminedTx::from(transaction);
                let id = body.id();
                (
                    id,
                    tokio::spawn(queue(core.shared.clone(), Gossip::Tx(body), Some(peer))),
                )
            })
            .collect();
        // The pushes that took the adapter's slots, in staging order.
        let within: std::collections::HashSet<UnminedTxId> = pushes[..MAX_INBOUND_CONCURRENCY]
            .iter()
            .map(|(id, _)| *id)
            .collect();
        let pushes: Vec<_> = pushes.into_iter().map(|(_, push)| push).collect();

        // The core answers nothing to a push within the limit, which waits
        // for its reservation holding its body. Each push past the limit
        // drops its body and asks about its id alone, without a slot, and the
        // core knows none of them. The two can arrive in any order.
        let mut waiting = Vec::new();
        for _ in 0..pushed {
            let Some(PoolCommand::Reserve { tx_id, reply, .. }) = inbox.next().await else {
                panic!("each push reserves, or asks about its id");
            };
            assert_eq!(
                queued_count(&core),
                MAX_INBOUND_CONCURRENCY,
                "only the pushes within the limit hold slots"
            );
            if within.contains(&tx_id) {
                waiting.push(reply);
            } else {
                let _ = reply.send(Err(Unreserved::NoRoom));
            }
        }
        for _ in 0..1_000 {
            if pushes.iter().filter(|push| push.is_finished()).count()
                == pushed - MAX_INBOUND_CONCURRENCY
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        let mut refused = 0;
        for push in pushes {
            if push.is_finished() {
                let answer = push.await.expect("the task does not panic");
                assert!(
                    matches!(
                        answer.map_err(|error| error.downcast::<MempoolError>().map(|e| *e)),
                        Err(Ok(MempoolError::FullQueue))
                    ),
                    "a push past the limit is a full queue"
                );
                refused += 1;
            }
        }
        assert_eq!(
            refused,
            pushed - MAX_INBOUND_CONCURRENCY,
            "the pushes past the limit"
        );
        assert!(
            inbox.try_next().is_none(),
            "no push past the limit is submitted"
        );
        assert_eq!(
            queued_count(&core),
            MAX_INBOUND_CONCURRENCY,
            "the waiting pushes fill the node's limit"
        );
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
            mpsc::channel(1).0,
            Arc::new(Exempt::default()),
        )
    }

    /// A pushed body is reserved at once while the core is a tip behind the
    /// state: only queries wait for the core to catch up, so a body never
    /// waits for it outside the limits.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_push_does_not_wait_for_the_core_to_catch_up() {
        use zakura_node_services::mempool::port::{
            pool_port, ChainTip, Generation, PoolCommand, PoolSnapshot,
        };

        let (handle, mut inbox) = pool_port();
        let mut core = adapter_at(handle, Some(block::Hash([2; 32])));
        inbox.publish([], |last| {
            PoolSnapshot::new(
                ChainTip {
                    hash: block::Hash([1; 32]),
                    height: Height(1),
                },
                Generation(1),
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
        let Ok(Some(PoolCommand::Reserve { .. })) = tokio::time::timeout(soon, inbox.next()).await
        else {
            panic!("the push reserves without waiting for the tip");
        };
        assert_eq!(
            queued_count(&core),
            1,
            "the push holds a node-wide slot while it waits"
        );
        push.abort();
    }

    /// With the node's limit reached, an item is still answered as today's
    /// pool answers what it already knows about the id: a cached oversized
    /// body at the second stage, an id in the pool or already queued at the
    /// first. Only an id the core does not know is refused as a full queue.
    #[tokio::test(flavor = "current_thread")]
    async fn a_known_id_is_answered_as_today_when_the_queue_is_full() {
        use zakura_node_services::mempool::port::{
            pool_port, PoolCommand, Rejection, RejectionScope,
        };

        let (handle, mut inbox) = pool_port();
        let core = adapter_on(handle);
        // Fill the node's limit with fetches the core never answers.
        let fill: Vec<_> = (0..MAX_INBOUND_CONCURRENCY)
            .map(|n| {
                let id = UnminedTxId::Legacy(zakura_chain::transaction::Hash([
                    u8::try_from(n % 256).expect("a byte"),
                    u8::try_from(n / 256).expect("a byte"),
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                ]));
                tokio::spawn(queue(core.shared.clone(), Gossip::Id(id), None))
            })
            .collect();
        let mut held = Vec::new();
        for _ in 0..MAX_INBOUND_CONCURRENCY {
            let Some(PoolCommand::Reserve { reply, .. }) = inbox.next().await else {
                panic!("each fetch reserves");
            };
            held.push(reply);
        }

        let body = Network::Mainnet
            .unmined_transactions_in_blocks(1..=1)
            .next()
            .expect("a vector transaction")
            .transaction;
        let too_large = Unreserved::Rejected(Rejection {
            scope: RejectionScope::ExactTip,
            charge: Charge::None,
            tip: Height(0),
            detail: Arc::new(CoreMempoolError::StorageExactTip(
                zakura_mempool::ExactTipRejectionError::FailedStandard(
                    zakura_mempool::NonStandardTransactionError::TransactionTooLarge {
                        actual_bytes: 3_000_000,
                        max_bytes: 2_000_000,
                    },
                ),
            )),
        });
        for (answer, expected) in [
            (Err(too_large), "receiver: too large"),
            (Err(Unreserved::Pooled), "first: in the pool"),
            (Err(Unreserved::Pending), "first: already queued"),
            (Err(Unreserved::NoRoom), "first: full queue"),
        ] {
            let queued = tokio::spawn(queue(core.shared.clone(), Gossip::Tx(body.clone()), None));
            let Some(PoolCommand::Reserve { reply, .. }) = inbox.next().await else {
                panic!("an item past the limit asks about its id");
            };
            let _ = reply.send(answer);
            let first = queued.await.expect("the task does not panic");
            let seen = match first {
                Ok(receiver) => match receiver.await.expect("answered") {
                    Err(error)
                        if matches!(
                            error.downcast_ref::<MempoolError>(),
                            Some(MempoolError::StorageExactTip(
                                ExactTipRejectionError::FailedStandard(
                                    NonStandardTransactionError::TransactionTooLarge { .. }
                                )
                            ))
                        ) =>
                    {
                        "receiver: too large"
                    }
                    other => panic!("unexpected second stage: {other:?}"),
                },
                Err(error) => match error.downcast_ref::<MempoolError>() {
                    Some(MempoolError::InMempool) => "first: in the pool",
                    Some(MempoolError::AlreadyQueued) => "first: already queued",
                    Some(MempoolError::FullQueue) => "first: full queue",
                    other => panic!("unexpected first stage: {other:?}"),
                },
            };
            assert_eq!(seen, expected);
        }
        drop((fill, held));
    }

    /// A peer's batch of ids the pool already knows costs the core one
    /// command, and once the peer's limit is reached the rest of the batch
    /// is refused without asking the core, however long the batch is.
    #[tokio::test(flavor = "current_thread")]
    async fn a_peer_batch_costs_the_core_a_bounded_number_of_commands() {
        use zakura_node_services::mempool::port::{
            pool_port, ChainTip, Generation, PoolCommand, PoolSnapshot, ReservationId,
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
                Generation(1),
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

        let Some(PoolCommand::RejectedIds { ids, reply }) = inbox.next().await else {
            panic!("the batch asks once which ids are rejected");
        };
        assert!(
            !ids.contains(&pooled_id)
                && other_authorizations
                    .iter()
                    .all(|other| !ids.contains(other)),
            "a pooled transaction's effects are answered from the snapshot"
        );
        assert_eq!(ids.len(), 1_000, "every other id, in one command");
        let _ = reply.send([rejected_id].into_iter().collect());
        for granted in 0..MAX_INBOUND_CONCURRENCY_PER_PEER {
            let Some(PoolCommand::Reserve { tx_id, reply, .. }) = inbox.next().await else {
                panic!("the peer's limit of fetches reserves");
            };
            assert_ne!(tx_id, rejected_id, "a rejected id is not reserved");
            let id = ReservationId(u64::try_from(granted).expect("few"));
            let _ = reply.send(Ok(inbox.grant(id, tx_id, false, true, Default::default())));
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

    /// Bodies pushed through the service while the core takes no commands
    /// wait for their reservations under today's node-wide limit, and the
    /// pushes past it are dropped without the core, as today's pool drops
    /// what it cannot queue from a peer. No body waits for anything before
    /// it holds its slots.
    #[tokio::test(flavor = "current_thread")]
    async fn pushes_through_the_service_wait_only_within_the_node_limit() {
        use zakura_chain::transaction::{LockTime, Transaction, UnminedTx};
        use zakura_node_services::mempool::port::{
            pool_port, ChainTip, Generation, PoolCommand, PoolSnapshot,
        };

        let pushed = MAX_INBOUND_CONCURRENCY + 100;
        let template = Network::Mainnet
            .unmined_transactions_in_blocks(1..=1)
            .next()
            .expect("a vector transaction")
            .transaction;
        let (handle, mut inbox) = pool_port();
        let core = adapter_on(handle);
        inbox.publish([], |last| {
            PoolSnapshot::new(
                ChainTip {
                    hash: block::Hash([1; 32]),
                    height: Height(1),
                },
                Generation(1),
                last,
                Vec::new(),
                Default::default(),
            )
        });
        let pushes: Vec<_> = (0..pushed)
            .map(|n| {
                let mut transaction = Transaction::clone(template.transaction());
                let height = u32::try_from(n + 1).expect("few pushes");
                if let Transaction::V1 { lock_time, .. }
                | Transaction::V2 { lock_time, .. }
                | Transaction::V3 { lock_time, .. }
                | Transaction::V4 { lock_time, .. }
                | Transaction::V5 { lock_time, .. } = &mut transaction
                {
                    *lock_time = LockTime::Height(Height(height));
                }
                let peer = u16::try_from(n).expect("few peers");
                let [high, low] = peer.to_be_bytes();
                let source = QueueSource::LegacySocket(std::net::SocketAddr::from((
                    [10, 3, high, low],
                    8233,
                )));
                let mut core = core.clone();
                tokio::spawn(core.call(Request::QueueFromPeer {
                    transactions: vec![Gossip::Tx(UnminedTx::from(transaction))],
                    source,
                }))
            })
            .collect();
        let mut waiting = Vec::new();
        for _ in 0..MAX_INBOUND_CONCURRENCY {
            let Some(PoolCommand::Reserve { reply, .. }) = inbox.next().await else {
                panic!("each push within the limit reserves, and nothing else is asked");
            };
            waiting.push(reply);
        }
        for _ in 0..1_000 {
            if pushes.iter().filter(|push| push.is_finished()).count()
                == pushed - MAX_INBOUND_CONCURRENCY
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            pushes.iter().filter(|push| push.is_finished()).count(),
            pushed - MAX_INBOUND_CONCURRENCY,
            "the pushes past the limit are dropped at once"
        );
        assert!(inbox.try_next().is_none(), "the core is asked nothing else");
        assert_eq!(
            queued_count(&core),
            MAX_INBOUND_CONCURRENCY,
            "the waiting pushes fill the node's limit"
        );
        drop(waiting);
    }

    /// Every body in one `Queue` request takes its slots before the first
    /// reservation waits, so the request's later bodies are not held
    /// outside the limits while it waits.
    #[tokio::test(flavor = "current_thread")]
    async fn a_queued_batch_takes_every_bodys_slots_first() {
        use zakura_chain::transaction::{LockTime, Transaction, UnminedTx};
        use zakura_node_services::mempool::port::{
            pool_port, ChainTip, Generation, PoolCommand, PoolSnapshot,
        };

        let template = Network::Mainnet
            .unmined_transactions_in_blocks(1..=1)
            .next()
            .expect("a vector transaction")
            .transaction;
        let bodies: Vec<Gossip> = (1..=3)
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
        inbox.publish([], |last| {
            PoolSnapshot::new(
                ChainTip {
                    hash: block::Hash([1; 32]),
                    height: Height(1),
                },
                Generation(1),
                last,
                Vec::new(),
                Default::default(),
            )
        });
        let request = tokio::spawn(core.call(Request::Queue(bodies)));
        let Some(PoolCommand::Reserve { .. }) = inbox.next().await else {
            panic!("the first body reserves");
        };
        assert_eq!(
            queued_count(&core),
            3,
            "every body holds its slot while the first waits"
        );
        request.abort();
    }

    /// A submission to a core that takes no reservation is answered at
    /// today's whole-task deadline, and frees its slots, so a stuck core
    /// cannot hold a caller or its slots forever.
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
        let Some(PoolCommand::Reserve { reply, .. }) = inbox.next().await else {
            panic!("the body reserves");
        };
        let error = tokio::time::timeout(RATE_LIMIT_DELAY * 2, queued)
            .await
            .expect("the stuck reservation ends by the deadline")
            .expect("the task does not panic")
            .expect_err("the stuck reservation is refused");
        assert_eq!(error.to_string(), "timeout waiting for verification result");
        assert!(started.elapsed() >= RATE_LIMIT_DELAY, "at today's deadline");
        assert_eq!(
            queued_count(&core),
            1,
            "the reservation still outstanding keeps the item's node-wide slot"
        );
        // The core answers at last, and the slot is free.
        drop(reply);
        for _ in 0..100 {
            if queued_count(&core) == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(queued_count(&core), 0, "free once the core has answered");
    }

    /// The adapter's queued count, read without holding its lock, so a
    /// failed assertion on it does not poison the lock for the slots.
    fn queued_count(core: &CoreMempool) -> usize {
        *core
            .shared
            .queued
            .lock()
            .expect("no test panics holding the queued count")
    }

    /// A snapshot at height 1 with nothing pooled, so the adapter is on.
    fn publish_empty(inbox: &zakura_node_services::mempool::port::PoolInbox) {
        use zakura_node_services::mempool::port::{ChainTip, Generation, PoolSnapshot};

        inbox.publish([], |last| {
            PoolSnapshot::new(
                ChainTip {
                    hash: block::Hash([1; 32]),
                    height: Height(1),
                },
                Generation(1),
                last,
                Vec::new(),
                Default::default(),
            )
        });
    }

    /// A batch of ids takes each id's slots at its turn, as today's pool
    /// checks each in order: 500 pooled ids, each answered by the core and
    /// freeing its slots, leave room for a fresh id after them.
    #[tokio::test(flavor = "current_thread")]
    async fn ids_take_their_slots_at_their_turn() {
        use zakura_node_services::mempool::port::{pool_port, PoolCommand, ReservationId};

        let (handle, mut inbox) = pool_port();
        let mut core = adapter_on(handle);
        publish_empty(&inbox);
        let ids: Vec<Gossip> = (0..=MAX_INBOUND_CONCURRENCY)
            .map(|n| {
                let mut hash = [5; 32];
                hash[..8].copy_from_slice(&u64::try_from(n).expect("few").to_le_bytes());
                Gossip::Id(UnminedTxId::Legacy(zakura_chain::transaction::Hash(hash)))
            })
            .collect();
        let request = tokio::spawn(core.call(Request::Queue(ids)));
        for answered in 0..=MAX_INBOUND_CONCURRENCY {
            let Some(PoolCommand::Reserve { tx_id, reply, .. }) = inbox.next().await else {
                panic!("each id reserves at its turn");
            };
            let answer = if answered < MAX_INBOUND_CONCURRENCY {
                Err(Unreserved::Pooled)
            } else {
                Ok(inbox.grant(ReservationId(1), tx_id, false, true, Default::default()))
            };
            let _ = reply.send(answer);
        }
        let Ok(Ok(Response::Queued(answers))) = request.await else {
            panic!("`Queue` is answered with `Queued`");
        };
        assert!(answers[..MAX_INBOUND_CONCURRENCY]
            .iter()
            .all(|answer| matches!(
                answer
                    .as_ref()
                    .map_err(|error| error.downcast_ref::<MempoolError>()),
                Err(Some(MempoolError::InMempool))
            )));
        assert!(
            answers[MAX_INBOUND_CONCURRENCY].is_ok(),
            "the fresh id after them is queued"
        );
    }

    /// Bodies staged together share deadlines that start when they take
    /// their slots: with a core that answers no reservation, a batch of two
    /// is answered by one task deadline, not one deadline after the other.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn staged_bodies_reach_their_deadlines_together() {
        use zakura_chain::transaction::{LockTime, Transaction, UnminedTx};
        use zakura_node_services::mempool::port::{pool_port, PoolCommand};

        let template = Network::Mainnet
            .unmined_transactions_in_blocks(1..=1)
            .next()
            .expect("a vector transaction")
            .transaction;
        let bodies: Vec<Gossip> = (1..=2)
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
        // The core takes each reservation and never answers it.
        let silent = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Some(command) = inbox.next().await {
                if let PoolCommand::Reserve { reply, .. } = command {
                    held.push(reply);
                }
            }
        });
        let started = tokio::time::Instant::now();
        let answered = tokio::time::timeout(
            RATE_LIMIT_DELAY + std::time::Duration::from_secs(10),
            core.call(Request::Queue(bodies)),
        )
        .await
        .expect("both bodies are answered by one task deadline");
        let Ok(Response::Queued(answers)) = answered else {
            panic!("`Queue` is answered with `Queued`");
        };
        assert!(answers.iter().all(Result::is_err), "both time out");
        assert!(started.elapsed() >= RATE_LIMIT_DELAY);
        silent.abort();
    }

    /// A fetch that reaches its deadline is answered, and frees its slots,
    /// even when the core's command queue is full, so a core slow to take
    /// commands cannot hold them past the deadline.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_timed_out_fetch_is_answered_while_the_core_is_full_of_commands() {
        use zakura_node_services::mempool::port::{
            pool_port, PoolCommand, ReservationId, COMMAND_QUEUE,
        };

        let (handle, mut inbox) = pool_port();
        let core = adapter_on(handle.clone());
        let id = UnminedTxId::Legacy(zakura_chain::transaction::Hash([8; 32]));
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
            .expect("the fetch is queued");
        // Fill the core's command queue, and keep it full.
        let fillers: Vec<_> = (0..COMMAND_QUEUE + 10)
            .map(|_| {
                let handle = handle.clone();
                tokio::spawn(async move { handle.rejected_ids(Default::default()).await })
            })
            .collect();
        let answer = tokio::time::timeout(RATE_LIMIT_DELAY * 2, receiver)
            .await
            .expect("the fetch is answered at its deadline")
            .expect("answered");
        assert!(answer.is_err(), "the fetch timed out");
        assert_eq!(queued_count(&core), 0, "its slots are free");
        for filler in fillers {
            filler.abort();
        }
        drop(inbox);
    }

    /// A rejection lookup the core never answers ends within
    /// [`MAX_LOOKUP_WAIT`], answering that nothing is rejected.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_rejection_lookup_the_core_never_answers_ends_in_time() {
        use zakura_node_services::mempool::port::pool_port;

        let (handle, inbox) = pool_port();
        let mut core = adapter_on(handle);
        publish_empty(&inbox);
        let id = UnminedTxId::Legacy(zakura_chain::transaction::Hash([4; 32]));
        let answered = tokio::time::timeout(
            MAX_LOOKUP_WAIT * 2,
            core.call(Request::RejectedTransactionIds([id].into_iter().collect())),
        )
        .await
        .expect("the lookup ends in time");
        let Ok(Response::RejectedTransactionIds(rejected)) = answered else {
            panic!("answered with rejected ids");
        };
        assert!(rejected.is_empty());
        drop(inbox);
    }

    /// In a batch with ids before a body, the body is queued first, so its
    /// deadline does not wait behind each earlier id's.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_body_after_ids_is_queued_first() {
        use zakura_node_services::mempool::port::{pool_port, PoolCommand};

        let body = Network::Mainnet
            .unmined_transactions_in_blocks(1..=1)
            .next()
            .expect("a vector transaction")
            .transaction;
        let id = |byte| UnminedTxId::Legacy(zakura_chain::transaction::Hash([byte; 32]));
        let (handle, mut inbox) = pool_port();
        let mut core = adapter_on(handle);
        publish_empty(&inbox);
        let body_id = body.id();
        let request = tokio::spawn(core.call(Request::Queue(vec![
            Gossip::Id(id(1)),
            Gossip::Id(id(2)),
            Gossip::Tx(body),
        ])));
        let Some(PoolCommand::Reserve { tx_id, reply, .. }) = inbox.next().await else {
            panic!("the batch reserves");
        };
        assert_eq!(tx_id, body_id, "the body is queued before the ids");
        request.abort();
        drop(reply);
    }

    /// A submission that times out before the core takes the body leaves the
    /// body with the core, so the core can still decide it and its outcome
    /// reaches the change stream, where cancelling it would release the
    /// reservation with nothing logged.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_submission_that_times_out_still_reaches_the_core() {
        use zakura_node_services::mempool::port::{pool_port, PoolCommand, ReservationId};

        let (handle, mut inbox) = pool_port();
        let core = adapter_on(handle);
        let body = Network::Mainnet
            .unmined_transactions_in_blocks(1..=1)
            .next()
            .expect("a vector transaction")
            .transaction;
        let queued = tokio::spawn(queue(core.shared.clone(), Gossip::Tx(body), None));
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
        // The core takes nothing until the caller's deadline has passed.
        let answer = tokio::time::timeout(RATE_LIMIT_DELAY * 2, receiver)
            .await
            .expect("the caller is answered at its deadline")
            .expect("answered");
        assert_eq!(
            answer.expect_err("timed out").to_string(),
            "timeout waiting for verification result"
        );
        let Some(PoolCommand::Submit { reply, .. }) = inbox.next().await else {
            panic!("the body still reaches the core");
        };
        assert!(
            !reply.is_closed(),
            "the core can still decide it, so its outcome is logged"
        );
    }

    /// A body whose caller timed out before the core took it keeps its
    /// node-wide slot until the core decides it, so the bodies waiting for
    /// the core and the bodies the adapter takes in after them stay within
    /// one limit.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_timed_out_submission_keeps_its_node_slot() {
        use zakura_node_services::mempool::port::{pool_port, PoolCommand, ReservationId};

        let (handle, mut inbox) = pool_port();
        let core = adapter_on(handle);
        let body = Network::Mainnet
            .unmined_transactions_in_blocks(1..=1)
            .next()
            .expect("a vector transaction")
            .transaction;
        let queued = tokio::spawn(queue(core.shared.clone(), Gossip::Tx(body), None));
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
        let answer = tokio::time::timeout(RATE_LIMIT_DELAY * 2, receiver)
            .await
            .expect("the caller is answered at its deadline")
            .expect("answered");
        assert!(answer.is_err(), "the caller timed out");
        assert_eq!(
            queued_count(&core),
            1,
            "the body waiting for the core keeps its slot"
        );
        // The core takes the body and decides it, and the slot is free.
        let Some(PoolCommand::Submit { reply, .. }) = inbox.next().await else {
            panic!("the body reaches the core");
        };
        let _ = reply.send(Verdict::Admitted);
        for _ in 0..100 {
            if queued_count(&core) == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(queued_count(&core), 0, "free once the core decided it");
    }

    /// A reservation the core grants only after its caller timed out is
    /// reported as failed, so the change stream hears of the timeout as
    /// today's pool reports one.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_reservation_granted_after_its_deadline_is_reported_as_failed() {
        use zakura_node_services::mempool::port::{pool_port, PoolCommand, ReservationId};

        let (handle, mut inbox) = pool_port();
        let core = adapter_on(handle);
        let id = UnminedTxId::Legacy(zakura_chain::transaction::Hash([3; 32]));
        let queued = tokio::spawn(queue(core.shared.clone(), Gossip::Id(id), None));
        let Some(PoolCommand::Reserve { tx_id, reply, .. }) = inbox.next().await else {
            panic!("the fetch reserves first");
        };
        let answer = tokio::time::timeout(RATE_LIMIT_DELAY * 2, queued)
            .await
            .expect("the caller is answered at its deadline")
            .expect("the task does not panic");
        assert!(answer.is_err(), "the caller timed out");
        assert_eq!(
            queued_count(&core),
            1,
            "the outstanding reservation keeps its node-wide slot"
        );
        let _ = reply.send(Ok(inbox.grant(
            ReservationId(1),
            tx_id,
            false,
            true,
            Default::default(),
        )));
        let Some(PoolCommand::FetchFailed(reservation)) = inbox.next().await else {
            panic!("the late grant is reported as failed");
        };
        let _accepted = reservation.accept();
        for _ in 0..100 {
            if queued_count(&core) == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(queued_count(&core), 0, "free once the failure is reported");
    }

    /// A verdict that arrives after its caller timed out still charges the
    /// legacy peer that served the body, since the core did check it.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_late_verdict_still_charges_its_peer() {
        use zakura_node_services::mempool::port::{
            pool_port, PoolCommand, Rejection, RejectionScope, ReservationId,
        };

        let (handle, mut inbox) = pool_port();
        let (misbehavior, mut reports) = mpsc::channel(1);
        let core = CoreMempool::new(
            handle,
            Timeout::new(
                BoxCloneSyncService::new(NeverReady),
                TRANSACTION_DOWNLOAD_TIMEOUT,
            ),
            BoxCloneSyncService::new(NeverReady),
            Arc::new(|_| true),
            Arc::new(|| None),
            chain_holding(None),
            super::super::super::Config::default().max_transaction_bytes,
            false,
            misbehavior,
            Arc::new(Exempt::default()),
        );
        let body = Network::Mainnet
            .unmined_transactions_in_blocks(1..=1)
            .next()
            .expect("a vector transaction")
            .transaction;
        let QueueSource::LegacySocket(addr) = peer() else {
            unreachable!("the test peer is a legacy peer");
        };
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
        let answer = tokio::time::timeout(RATE_LIMIT_DELAY * 2, receiver)
            .await
            .expect("the caller is answered at its deadline")
            .expect("answered");
        assert!(answer.is_err(), "the caller timed out");
        let Some(PoolCommand::Submit {
            reservation, reply, ..
        }) = inbox.next().await
        else {
            panic!("the body still reaches the core");
        };
        let _accepted = reservation.accept();
        let _ = reply.send(Verdict::Rejected(Rejection {
            scope: RejectionScope::ExactTip,
            charge: Charge::Ban,
            tip: Height(0),
            detail: Arc::new(std::io::Error::other("invalid")),
        }));
        let report = tokio::time::timeout(std::time::Duration::from_secs(1), reports.recv())
            .await
            .expect("the late verdict's charge is applied");
        assert_eq!(
            report,
            Some((addr.into(), zn::constants::MAX_PEER_MISBEHAVIOR_SCORE))
        );
    }

    /// A staged body whose deadline passed while an earlier body waited out
    /// the shared deadline starts no work: the core sees one reservation,
    /// not two, and both are answered as timed out.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn an_expired_staged_body_starts_nothing() {
        use zakura_chain::transaction::{LockTime, Transaction, UnminedTx};
        use zakura_node_services::mempool::port::{pool_port, PoolCommand};

        let template = Network::Mainnet
            .unmined_transactions_in_blocks(1..=1)
            .next()
            .expect("a vector transaction")
            .transaction;
        let bodies: Vec<Gossip> = (1..=2)
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
        let request = tokio::spawn(core.call(Request::Queue(bodies)));
        let Some(PoolCommand::Reserve { reply, .. }) = inbox.next().await else {
            panic!("the first body reserves");
        };
        let Ok(Ok(Ok(Response::Queued(answers)))) =
            tokio::time::timeout(RATE_LIMIT_DELAY * 2, request).await
        else {
            panic!("`Queue` is answered with `Queued` by the deadline");
        };
        assert!(answers.iter().all(Result::is_err), "both time out");
        assert!(
            inbox.try_next().is_none(),
            "the expired second body asked the core for nothing"
        );
        drop(reply);
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
        // The batch's bodies are queued one after another. Each within the
        // peer's limit reserves, and the core says it is already pooled.
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
        let asked: Arc<std::sync::Mutex<Vec<zn::Request>>> = Arc::default();
        let network = {
            let (asked, body) = (asked.clone(), body.clone());
            BoxCloneSyncService::new(tower::service_fn(move |request: zn::Request| {
                asked.lock().expect("not poisoned").push(request);
                let body = body.clone();
                async move {
                    Ok::<_, BoxError>(zn::Response::Transactions(vec![
                        zn::InventoryResponse::Available((body, None)),
                    ]))
                }
            }))
        };
        let (handle, mut inbox) = pool_port();
        let (misbehavior, mut reports) = mpsc::channel(1);
        let mut core = CoreMempool::new(
            handle,
            Timeout::new(network, TRANSACTION_DOWNLOAD_TIMEOUT),
            BoxCloneSyncService::new(NeverReady),
            Arc::new(|_| true),
            Arc::new(|| None),
            chain_holding(None),
            super::super::super::Config::default().max_transaction_bytes,
            false,
            misbehavior,
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
        let _ = reply.send(Ok(inbox.grant(
            ReservationId(1),
            tx_id,
            false,
            true,
            Default::default(),
        )));
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
        assert!(reports.try_recv().is_err(), "a v2 peer is never charged");
    }

    /// A request cancelled while its reservation waits, as inbound cancels a
    /// request after its own timeout, leaves the reservation holding the
    /// item's node-wide slot, and a late grant is reported as failed, so
    /// cancelled requests cannot gather work beyond the node's limit or lose
    /// their change-stream event.
    #[tokio::test(flavor = "current_thread")]
    async fn a_cancelled_request_leaves_its_reservation_counted_and_reported() {
        use zakura_node_services::mempool::port::{pool_port, PoolCommand, ReservationId};

        let body = Network::Mainnet
            .unmined_transactions_in_blocks(1..=1)
            .next()
            .expect("a vector transaction")
            .transaction;
        let (handle, mut inbox) = pool_port();
        let mut core = adapter_on(handle);
        publish_empty(&inbox);
        let request = tokio::spawn(core.call(Request::QueueFromPeer {
            transactions: vec![Gossip::Tx(body)],
            source: peer(),
        }));
        let Some(PoolCommand::Reserve { tx_id, reply, .. }) = inbox.next().await else {
            panic!("the push reserves");
        };
        request.abort();
        let _ = request.await;
        assert_eq!(
            queued_count(&core),
            1,
            "the reservation keeps the item's node-wide slot"
        );
        let _ = reply.send(Ok(inbox.grant(
            ReservationId(1),
            tx_id,
            false,
            true,
            Default::default(),
        )));
        let Some(PoolCommand::FetchFailed(reservation)) = inbox.next().await else {
            panic!("the grant nobody takes is reported as failed");
        };
        let _accepted = reservation.accept();
        for _ in 0..100 {
            if queued_count(&core) == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(queued_count(&core), 0, "free once it is reported");
    }

    /// Bodies the pool already holds are skipped before the limits, as
    /// today's pool skips them, so five of them ahead of a fresh body in one
    /// peer's batch cannot take that peer's five slots: only the fresh body
    /// reserves.
    #[tokio::test(flavor = "current_thread")]
    async fn pooled_bodies_do_not_take_a_fresh_bodys_slot() {
        use zakura_node_services::mempool::port::{
            pool_port, ChainTip, Generation, PoolCommand, PoolSnapshot,
        };

        let mut vectors = Network::Mainnet.unmined_transactions_in_blocks(..);
        let pooled: Vec<_> = vectors
            .by_ref()
            .take(MAX_INBOUND_CONCURRENCY_PER_PEER)
            .collect();
        let fresh = vectors
            .next()
            .expect("another vector transaction")
            .transaction;
        let fresh_id = fresh.id();
        let mut batch: Vec<Gossip> = pooled
            .iter()
            .map(|tx| Gossip::Tx(tx.transaction.clone()))
            .collect();
        batch.push(Gossip::Tx(fresh));
        let (handle, mut inbox) = pool_port();
        let mut core = adapter_on(handle);
        inbox.publish([], |last| {
            PoolSnapshot::new(
                ChainTip {
                    hash: block::Hash([1; 32]),
                    height: Height(1),
                },
                Generation(1),
                last,
                pooled,
                Default::default(),
            )
        });
        let request = tokio::spawn(core.call(Request::QueueFromPeer {
            transactions: batch,
            source: peer(),
        }));
        let Some(PoolCommand::Reserve { tx_id, reply, .. }) = inbox.next().await else {
            panic!("the fresh body reserves");
        };
        assert_eq!(tx_id, fresh_id, "only the fresh body reserves");
        let _ = reply.send(Err(Unreserved::Pooled));
        let _ = request.await;
        assert!(inbox.try_next().is_none(), "nothing else is asked");
    }
}
