//! The pool port: how every caller reaches the pool core.
//!
//! The pool core is one task that owns the node's unmined transactions. The
//! legacy fetcher, RPC, wallets and `tx-relay` all reach it through a
//! [`PoolHandle`]. The core holds the matching [`PoolInbox`].
//!
//! # The contract
//!
//! A caller reserves before it fetches, and submits with the reservation:
//!
//! 1. [`PoolHandle::reserve`] takes one of the node's [`MAX_PENDING`] slots
//!    for one transaction id. It answers [`Unreserved`] when the id is already
//!    known or there is no room. Nothing is fetched without a reservation, so
//!    one count covers a transaction from its fetch to its verdict.
//! 2. [`PoolHandle::submit`] hands over the body. It answers exactly one
//!    [`Verdict`], which says what the body earned its supplier.
//! 3. Dropping a [`Reservation`] that was never submitted frees its slot.
//!
//! The core knows nothing about peers. Whoever sees a peer owns that peer's
//! state: its share of the slots, its cooldown, and who supplied a body. The
//! legacy adapter keeps today's per-peer limit and cooldowns, and `tx-relay`
//! keeps its own. Each applies a verdict's [`Charge`] to the peer that sent
//! the body, and charges nobody for a body that came from this node.
//!
//! A slot is held until the caller that reserved it submits its body or drops
//! the reservation, and at least until the verdict. So if a block mines the
//! id, or the pool turns off, while the caller's fetch is still out, that
//! fetch still holds its slot.
//!
//! A caller drops a reservation when it stops waiting for the body. If its
//! peer can still answer after that, the caller owns the bound on those late
//! bodies, and drops each one before it reaches the core. Holding the slot
//! instead would let peers that never answer fill the core.
//!
//! A caller whose reservation outlives its admission gets that admission's
//! verdict when it submits, as if it had come in time.
//!
//! Readers follow the pool without asking the core for work: the latest
//! [`PoolSnapshot`] and the numbered [`ChangeEntry`] log.

use std::{collections::VecDeque, fmt, sync::Arc, time::Duration};

use zakura_chain::{
    block,
    transaction::{self, UnminedTx, UnminedTxId, VerifiedUnminedTx},
    transparent,
};
use zakura_rt::{
    collections::{HashMap, HashSet},
    sync::{mpsc, oneshot, watch, CancellationToken, Mutex, PoisonError},
    time::{self, error::Elapsed, Instant},
};

use super::TransactionDependencies;
use crate::block_memory::TransactionHandle;

#[cfg(test)]
mod tests;

/// Undecided transactions the core holds at once, node-wide. Today's
/// `MAX_INBOUND_CONCURRENCY`.
pub const MAX_PENDING: usize = 500;

/// Slots that pooled transactions can hold while they are verified again
/// after a reset. The rest of [`MAX_PENDING`] stays free for new admissions.
/// Provisional.
pub const MAX_READMITTING: usize = 400;

/// Entries the change log keeps. A reader that falls further behind
/// resynchronizes from the snapshot. The pool's 8,000 transactions plus a
/// ring of 16,384 removals.
pub const CHANGE_LOG_CAPACITY: usize = 24_384;

/// How long one verification can take. Provisional.
pub const T_VERIFY: Duration = Duration::from_secs(60);

/// How long a transaction waits for a missing parent, counted from the
/// first time one of its inputs is found missing. Today's wait for a pool
/// parent.
///
/// The wait is flat. It does not stop while a parent is being verified, and
/// waking the transaction or checking it again does not restart it. When it
/// ends, a parked transaction is rejected, and one whose inputs keep
/// arriving during its checks gets one more check and no more.
pub const T_PARK: Duration = Duration::from_secs(60);

/// Commands that can wait for the core. A full queue makes
/// [`PoolHandle::reserve`] and [`PoolHandle::submit`] wait.
pub const COMMAND_QUEUE: usize = 1_024;

/// Names one reservation to the core.
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ReservationId(pub u64);

/// A slot in the core for one transaction id.
///
/// Dropping it frees the slot, unless the core has accepted it. That holds
/// while it waits inside a [`PoolCommand`] too, so a caller that is cancelled
/// in the middle of [`PoolHandle::submit`] leaks nothing.
///
/// A caller holds one for the length of one fetch or one delivery and then
/// submits or drops it. A drop puts one id on the port's release queue,
/// which the core empties before each command. The queue is as long as the
/// reservations callers dropped since then, so a caller that hoards
/// reservations and drops them together is the caller's bug to bound.
#[must_use = "dropping a reservation frees its slot"]
pub struct Reservation {
    id: ReservationId,
    tx_id: UnminedTxId,
    release: Option<mpsc::UnboundedSender<ReservationId>>,
    /// Cancelled when the core no longer needs this reservation's body.
    cancel: CancellationToken,
}

impl Reservation {
    /// Build the reservation the core granted. `release` is the port's
    /// release queue.
    pub fn granted(
        id: ReservationId,
        tx_id: UnminedTxId,
        release: mpsc::UnboundedSender<ReservationId>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            id,
            tx_id,
            release: Some(release),
            cancel,
        }
    }

    /// Cancelled when the core no longer needs the body this reservation
    /// was taken for, such as when a block mines the transaction while it is
    /// fetched. A caller that is fetching stops and drops the reservation,
    /// which frees its slot, as today's pool cancels the download.
    pub fn cancellation(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// The core's name for this reservation.
    pub fn id(&self) -> ReservationId {
        self.id
    }

    /// The transaction id it was reserved for.
    pub fn tx_id(&self) -> UnminedTxId {
        self.tx_id
    }

    /// The core takes over: the outcome is now the core's to deliver, and
    /// dropping this no longer frees anything. Only the core calls this,
    /// when it takes the reservation out of a command.
    pub fn accept(mut self) -> ReservationId {
        self.release = None;
        self.id
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            // The core is gone if this fails, and its slots went with it.
            let _ = release.send(self.id);
        }
    }
}

impl fmt::Debug for Reservation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reservation")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

/// Why [`PoolHandle::reserve`] granted nothing. The caller fetches nothing.
#[derive(Clone, Debug)]
pub enum Unreserved {
    /// The pool is off, because the node is far from the tip.
    Inactive,
    /// The transaction is in the pool.
    Pooled,
    /// The id is already undecided: another caller reserved it and is
    /// bringing its body in. Today's `AlreadyQueued`.
    Pending,
    /// A cached rejection covers the id.
    Rejected(Rejection),
    /// [`MAX_PENDING`] is reached. The caller can try again later.
    NoRoom,
    /// The core has stopped.
    Stopped,
}

/// How long the core remembers a rejection, which is today's three lists.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum RejectionScope {
    /// These exact bytes, until the tip changes.
    ExactTip,
    /// Any transaction with these effects, until the tip changes.
    EffectsTip,
    /// Any transaction with these effects, while this chain keeps growing.
    /// Each reason has its own list, as today.
    EffectsChain(ChainReason),
    /// Not remembered: a parent that never arrived, or a transaction a block
    /// already mined.
    Uncached,
}

/// Why a rejection holds for as long as the chain keeps growing. Today's
/// three chain-scoped lists.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub enum ChainReason {
    /// The tip reached the transaction's expiry height.
    Expired,
    /// The best chain spent one of its inputs or revealed one of its
    /// nullifiers.
    DuplicateSpend,
    /// The best chain mined it.
    Mined,
}

/// What a rejected body earned the peer that sent it. The caller that knows
/// that peer applies it. A body that came from this node charges nobody,
/// whatever its charge.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Charge {
    /// Nothing. The failure does not show the supplier misbehaved.
    None,
    /// The caller refuses the supplier's transactions for a while, as
    /// today's cooldown does.
    Cooldown,
    /// The caller bans or disconnects the supplier.
    Ban,
}

/// The error a rejection carries, kept as today's callers see it.
pub type RejectionDetail = Arc<dyn std::error::Error + Send + Sync + 'static>;

/// A transaction the core refused, and why.
#[derive(Clone, Debug)]
pub struct Rejection {
    /// How long the rejection is remembered.
    pub scope: RejectionScope,
    /// What the body earned its supplier. See [`Charge`].
    pub charge: Charge,
    /// The height of the tip the core held when it decided. A charge was
    /// earned against this tip, so a caller that applies a cooldown can skip
    /// it once the best tip has moved on, as today's pool skips a cooldown
    /// for a failure verified at another height.
    pub tip: block::Height,
    /// Today's error value, for RPC and logs.
    pub detail: RejectionDetail,
}

/// A failure that says nothing about the transaction. Nobody is charged and
/// nothing is cached. The caller can submit again.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum LocalFault {
    /// The verification did not finish before its deadline.
    VerifyTimeout,
    /// The verification job panicked.
    VerifyPanicked,
    /// A state lookup failed.
    StateUnavailable,
    /// The pool turned off while the transaction was undecided.
    Deactivated,
    /// The reservation was no longer valid when the body arrived.
    StaleReservation,
    /// The body is not the transaction the reservation names.
    WrongTransaction,
    /// The core has stopped.
    Stopped,
}

/// The one answer to a submission.
#[derive(Clone, Debug)]
pub enum Verdict {
    /// The transaction is in the pool.
    Admitted,
    /// The core refused the transaction.
    Rejected(Rejection),
    /// [`MAX_PENDING`] was reached. Only a submission without a slot can get
    /// this.
    NoRoom,
    /// A failure on this node.
    LocalFault(LocalFault),
}

/// A best-chain tip.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ChainTip {
    /// The tip block's hash.
    pub hash: block::Hash,
    /// The tip block's height.
    pub height: block::Height,
}

/// A block that joined the best chain.
///
/// It carries its transactions, not only their ids. The core removes the
/// pool transactions that spend what these spend, and an id cannot find a
/// conflict with a transaction that was never in the pool. They are the
/// handles the state already holds, not copies.
#[derive(Clone, Debug)]
pub struct ConnectedBlock {
    /// The block's hash.
    pub hash: block::Hash,
    /// The block's height.
    pub height: block::Height,
    /// The block's transactions, in block order.
    pub transactions: Vec<TransactionHandle>,
    /// The mined ids of `transactions`, in the same order.
    pub transaction_hashes: Arc<[transaction::Hash]>,
}

/// One change to the best chain, as the state's tip watch reports it.
#[derive(Clone, Debug)]
pub enum ChainEvent {
    /// The best chain grew by this one block.
    Grow(ConnectedBlock),
    /// The tip changed some other way: a reorg, several blocks at once, or a
    /// block whose successor is verified under new rules. Nothing names the
    /// blocks that changed, so the core checks every pooled and parked
    /// transaction again at this tip.
    Reset(ChainTip),
}

/// A position in the change log. The first entry is 1, and 0 is "before any
/// entry".
#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ChangeSeq(pub u64);

/// Why a transaction left the pool.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum RemovalReason {
    /// A block on the best chain mined it.
    Mined,
    /// A block mined a transaction that spends what it spends.
    Conflicted,
    /// Its expiry height passed.
    Expired,
    /// The pool was full and it had the lowest fee rate.
    Evicted,
    /// A transaction it depends on left the pool without being mined.
    Invalidated,
    /// The tip was reset. The core verifies it again, and adds it again if
    /// it passes.
    Reset,
    /// The pool turned off.
    Deactivated,
}

/// One change, in the order the core made it.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Change {
    /// The transaction entered the pool.
    Added(UnminedTxId),
    /// The transaction left the pool.
    Removed {
        /// The transaction.
        id: UnminedTxId,
        /// Why it left.
        reason: RemovalReason,
    },
    /// A submitted transaction was rejected and never entered the pool.
    /// Today's change stream reports this as `Invalidated`.
    ///
    /// A fetch that its reservation's owner reports as failed is logged as
    /// `Refused` for that id too, so a caller that was told another caller
    /// is fetching it ([`Unreserved::Pending`]) learns the id is free
    /// again. tx-relay ends such a wait when it reads this.
    Refused(UnminedTxId),
}

/// One numbered entry of the change log.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ChangeEntry {
    /// The entry's position.
    pub seq: ChangeSeq,
    /// The batch it was published in. A batch is one input to the core.
    pub batch: u64,
    /// The change.
    pub change: Change,
}

/// What a reader gets for its cursor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChangesSince {
    /// Every entry after the cursor, in order. Empty when the reader is up
    /// to date.
    Entries(Vec<ChangeEntry>),
    /// The reader fell off the log. It reads the snapshot and continues from
    /// the snapshot's [`PoolSnapshot::last_change`].
    ///
    /// The snapshot cannot give back a lost [`Change::Refused`], because a
    /// refused transaction was never in the pool. A reader that owes its
    /// subscribers every refusal ends their stream here, as today's change
    /// stream does when a subscriber lags.
    Resync,
}

/// The numbered changes the core has published, newest last.
#[derive(Debug, Default)]
pub struct ChangeLog {
    entries: VecDeque<ChangeEntry>,
    last: ChangeSeq,
    batches: u64,
}

impl ChangeLog {
    /// Append one batch and return the position of its last entry. An empty
    /// batch still counts as a batch.
    pub fn append(&mut self, changes: impl IntoIterator<Item = Change>) -> ChangeSeq {
        self.batches = self.batches.saturating_add(1);
        for change in changes {
            self.last = ChangeSeq(self.last.0.saturating_add(1));
            if self.entries.len() == CHANGE_LOG_CAPACITY {
                self.entries.pop_front();
            }
            self.entries.push_back(ChangeEntry {
                seq: self.last,
                batch: self.batches,
                change,
            });
        }
        self.last
    }

    /// The position of the newest entry.
    pub fn last(&self) -> ChangeSeq {
        self.last
    }

    /// Every entry after `cursor`, up to and including `upto`.
    pub fn between(&self, cursor: ChangeSeq, upto: ChangeSeq) -> ChangesSince {
        match self.since(cursor) {
            ChangesSince::Entries(mut entries) => {
                entries.retain(|entry| entry.seq <= upto);
                ChangesSince::Entries(entries)
            }
            ChangesSince::Resync => ChangesSince::Resync,
        }
    }

    /// Every entry after `cursor`. A read visits only those entries: the log
    /// numbers its entries one after another, so the first one after
    /// `cursor` sits at a known offset from the oldest.
    pub fn since(&self, cursor: ChangeSeq) -> ChangesSince {
        if cursor >= self.last {
            return ChangesSince::Entries(Vec::new());
        }
        let oldest = self.entries.front().map_or(self.last, |entry| entry.seq);
        if cursor.0.saturating_add(1) < oldest.0 {
            return ChangesSince::Resync;
        }
        // The checks above put `cursor + 1` in `oldest..=last`, and the log
        // holds at most `CHANGE_LOG_CAPACITY` entries, so the offset fits.
        let start = usize::try_from(cursor.0.saturating_add(1).saturating_sub(oldest.0))
            .unwrap_or(self.entries.len())
            .min(self.entries.len());
        ChangesSince::Entries(self.entries.range(start..).copied().collect())
    }
}

/// The pool at one moment. Immutable once published.
#[derive(Clone, Debug)]
pub struct PoolSnapshot {
    /// Whether the pool is on. An inactive pool is empty, and a reader
    /// answers as today's inactive pool does, which is not how an empty
    /// active pool answers.
    pub active: bool,
    /// The tip the pool is valid for.
    pub tip: ChainTip,
    /// The newest change this snapshot includes.
    pub last_change: ChangeSeq,
    /// The transactions, in mining order.
    pub transactions: Vec<VerifiedUnminedTx>,
    /// The dependencies between them.
    pub dependencies: TransactionDependencies,
    by_mined_id: HashMap<transaction::Hash, usize>,
    spent: HashSet<transparent::OutPoint>,
}

impl PoolSnapshot {
    /// Build a snapshot and its lookup indexes.
    pub fn new(
        tip: ChainTip,
        last_change: ChangeSeq,
        transactions: Vec<VerifiedUnminedTx>,
        dependencies: TransactionDependencies,
    ) -> Self {
        let mut by_mined_id = HashMap::new();
        let mut spent = HashSet::new();
        for (index, tx) in transactions.iter().enumerate() {
            by_mined_id.insert(tx.transaction.id().mined_id(), index);
            spent.extend(tx.transaction.transaction().spent_outpoints());
        }
        Self {
            active: true,
            tip,
            last_change,
            transactions,
            dependencies,
            by_mined_id,
            spent,
        }
    }

    /// The snapshot of a pool that is off. The core publishes one when it
    /// turns off, and a new active one when it turns on, though no
    /// transaction changed.
    pub fn inactive(tip: ChainTip, last_change: ChangeSeq) -> Self {
        Self {
            active: false,
            ..Self::new(
                tip,
                last_change,
                Vec::new(),
                TransactionDependencies::default(),
            )
        }
    }

    /// The pooled transaction with this mined id.
    pub fn transaction(&self, id: &transaction::Hash) -> Option<&VerifiedUnminedTx> {
        self.by_mined_id
            .get(id)
            .and_then(|index| self.transactions.get(*index))
    }

    /// The output a pooled transaction creates at `outpoint`.
    pub fn created_output(&self, outpoint: &transparent::OutPoint) -> Option<&transparent::Output> {
        let index = usize::try_from(outpoint.index).ok()?;
        self.transaction(&outpoint.hash)?
            .transaction
            .transaction()
            .outputs()
            .get(index)
    }

    /// Whether a pooled transaction spends `outpoint`.
    pub fn spends(&self, outpoint: &transparent::OutPoint) -> bool {
        self.spent.contains(outpoint)
    }
}

/// What the core takes from callers.
#[derive(Debug)]
pub enum PoolCommand {
    /// [`PoolHandle::reserve`].
    Reserve {
        /// The transaction id.
        tx_id: UnminedTxId,
        /// Where the answer goes.
        reply: oneshot::Sender<Result<Reservation, Unreserved>>,
    },
    /// [`PoolHandle::submit`].
    Submit {
        /// The reservation the body arrives under. Still armed: if this
        /// command is dropped before the core takes it, the slot is freed.
        reservation: Reservation,
        /// The body.
        transaction: UnminedTx,
        /// Where the verdict goes.
        reply: oneshot::Sender<Verdict>,
    },
    /// [`PoolHandle::fetch_failed`]. The reservation is still armed.
    FetchFailed(Reservation),
    /// A reservation was dropped before the core accepted it. No caller
    /// sends this. [`PoolInbox::next`] yields it.
    Released(ReservationId),
    /// [`PoolHandle::rejected_ids`].
    RejectedIds {
        /// The ids to check.
        ids: HashSet<UnminedTxId>,
        /// The ids a cached rejection covers.
        reply: oneshot::Sender<HashSet<UnminedTxId>>,
    },
}

/// The pool as one reader sees it: a snapshot, and the changes that lead up
/// to exactly that snapshot from the reader's cursor.
#[derive(Clone, Debug)]
pub struct PoolView {
    /// The snapshot.
    pub snapshot: Arc<PoolSnapshot>,
    /// The entries after the reader's cursor, none newer than the
    /// snapshot's [`PoolSnapshot::last_change`].
    pub changes: ChangesSince,
}

/// What readers share with the core.
#[derive(Debug, Default)]
struct Shared {
    log: Mutex<ChangeLog>,
}

/// The callers' side of the port. Cheap to clone.
///
/// Every command takes an absolute `deadline` and answers `Err(Elapsed)` if
/// the core has not taken the command and replied by then. The command is
/// tried once first, so a core with room still answers at a passed deadline.
/// A command the core already took still runs: the deadline only ends the
/// caller's wait.
#[derive(Clone, Debug)]
pub struct PoolHandle {
    commands: mpsc::Sender<PoolCommand>,
    snapshot: watch::Receiver<Option<Arc<PoolSnapshot>>>,
    shared: Arc<Shared>,
}

/// Read-only access to the pool: the latest snapshot and the change log.
/// Unlike a [`PoolHandle`], it sends no commands, so holding one does not
/// keep the core running. A verifier the core owns, and a reader that waits
/// for the core to stop, use one.
#[derive(Clone, Debug)]
pub struct PoolReader {
    snapshot: watch::Receiver<Option<Arc<PoolSnapshot>>>,
    shared: Arc<Shared>,
}

impl PoolReader {
    /// The latest snapshot, or `None` until the core has published one.
    pub fn snapshot(&self) -> Option<Arc<PoolSnapshot>> {
        self.snapshot.borrow().clone()
    }

    /// Wait for a snapshot newer than the last one this reader saw. Returns
    /// `false` when the core has stopped.
    pub async fn snapshot_changed(&mut self) -> bool {
        self.snapshot.changed().await.is_ok()
    }

    /// See [`PoolHandle::view`].
    pub fn view(&self, cursor: ChangeSeq) -> Option<PoolView> {
        view(&self.shared, &self.snapshot, cursor)
    }
}

/// The latest snapshot with the changes after `cursor` that it includes.
fn view(
    shared: &Shared,
    snapshot: &watch::Receiver<Option<Arc<PoolSnapshot>>>,
    cursor: ChangeSeq,
) -> Option<PoolView> {
    let log = shared.log.lock().unwrap_or_else(PoisonError::into_inner);
    // The core publishes under this lock, so the snapshot read here is the
    // one the log's newest entry belongs to, or an older one.
    let snapshot = snapshot.borrow().clone()?;
    let changes = log.between(cursor, snapshot.last_change);
    Some(PoolView { snapshot, changes })
}

/// The core's side of the port.
#[derive(Debug)]
pub struct PoolInbox {
    commands: mpsc::Receiver<PoolCommand>,
    releases: mpsc::UnboundedReceiver<ReservationId>,
    release_handle: mpsc::UnboundedSender<ReservationId>,
    snapshot: watch::Sender<Option<Arc<PoolSnapshot>>>,
    shared: Arc<Shared>,
}

/// Create the port. The network takes the handle at startup, and the core
/// takes the inbox when it starts. Until the core publishes its first
/// snapshot, [`PoolHandle::snapshot`] is `None` and commands wait.
pub fn pool_port() -> (PoolHandle, PoolInbox) {
    let (commands_tx, commands_rx) = mpsc::channel(COMMAND_QUEUE);
    let (release_tx, release_rx) = mpsc::unbounded_channel();
    let (snapshot_tx, snapshot_rx) = watch::channel(None);
    let shared = Arc::new(Shared::default());
    (
        PoolHandle {
            commands: commands_tx,
            snapshot: snapshot_rx,
            shared: shared.clone(),
        },
        PoolInbox {
            commands: commands_rx,
            releases: release_rx,
            release_handle: release_tx,
            snapshot: snapshot_tx,
            shared,
        },
    )
}

/// `wait`, unless `deadline` passes first. Tokio's timeout polls `wait`
/// before its timer, so an answer that is ready still arrives.
async fn by<T>(
    deadline: Instant,
    wait: impl std::future::Future<Output = T>,
) -> Result<T, Elapsed> {
    time::timeout(deadline.saturating_duration_since(Instant::now()), wait).await
}

impl PoolHandle {
    /// Reserve one of the node's slots for `tx_id` before fetching or
    /// delivering its body, waiting until `deadline`. See [`PoolHandle`].
    pub async fn reserve(
        &self,
        tx_id: UnminedTxId,
        deadline: Instant,
    ) -> Result<Result<Reservation, Unreserved>, Elapsed> {
        by(deadline, async {
            let (reply, answer) = oneshot::channel();
            let command = PoolCommand::Reserve { tx_id, reply };
            if self.commands.send(command).await.is_err() {
                return Err(Unreserved::Stopped);
            }
            answer.await.unwrap_or(Err(Unreserved::Stopped))
        })
        .await
    }

    /// Submit the body `reservation` was taken for, and wait until
    /// `deadline` for its verdict. See [`PoolHandle`].
    ///
    /// Cancelling this before the core takes the command frees the slot.
    pub async fn submit(
        &self,
        reservation: Reservation,
        transaction: UnminedTx,
        deadline: Instant,
    ) -> Result<Verdict, Elapsed> {
        if transaction.id() != reservation.tx_id() {
            // Dropping the reservation frees its slot.
            return Ok(Verdict::LocalFault(LocalFault::WrongTransaction));
        }
        by(deadline, async {
            let (reply, verdict) = oneshot::channel();
            let command = PoolCommand::Submit {
                reservation,
                transaction,
                reply,
            };
            if self.commands.send(command).await.is_err() {
                return Verdict::LocalFault(LocalFault::Stopped);
            }
            verdict
                .await
                .unwrap_or(Verdict::LocalFault(LocalFault::Stopped))
        })
        .await
    }

    /// Submit as [`Self::submit`] does, but hand the body to the core's
    /// queue now or not at all, so the caller holds no body once this
    /// returns. With the queue full the body and the reservation are
    /// dropped, which frees the slot, and the verdict is `NoRoom`.
    pub fn submit_now(
        &self,
        reservation: Reservation,
        transaction: UnminedTx,
    ) -> impl std::future::Future<Output = Verdict> + Send + 'static {
        /// What became of the command, without the body.
        enum Sent {
            Queued(oneshot::Receiver<Verdict>),
            Answered(Verdict),
        }
        let sent = if transaction.id() != reservation.tx_id() {
            Sent::Answered(Verdict::LocalFault(LocalFault::WrongTransaction))
        } else {
            let (reply, verdict) = oneshot::channel();
            let command = PoolCommand::Submit {
                reservation,
                transaction,
                reply,
            };
            match self.commands.try_send(command) {
                Ok(()) => Sent::Queued(verdict),
                Err(mpsc::error::TrySendError::Full(_)) => Sent::Answered(Verdict::NoRoom),
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    Sent::Answered(Verdict::LocalFault(LocalFault::Stopped))
                }
            }
        };
        async move {
            match sent {
                Sent::Queued(verdict) => verdict
                    .await
                    .unwrap_or(Verdict::LocalFault(LocalFault::Stopped)),
                Sent::Answered(verdict) => verdict,
            }
        }
    }

    /// Report that the fetch `reservation` was taken for ended without a
    /// body. The slot is freed, as by a drop, and the failure reaches the
    /// change log, as a failed download reaches today's change stream.
    ///
    /// A caller that only stops waiting, and may still get the body, drops
    /// the reservation and does not call this.
    ///
    /// If `deadline` passes first, the reservation is dropped, which still
    /// frees the slot, and only the refusal goes unlogged. See [`PoolHandle`].
    pub async fn fetch_failed(&self, reservation: Reservation, deadline: Instant) {
        // If the core is gone the reservation is dropped with the command.
        let _ = by(
            deadline,
            self.commands.send(PoolCommand::FetchFailed(reservation)),
        )
        .await;
    }

    /// The members of `ids` that a cached rejection covers, waiting until
    /// `deadline`. Takes no slot and answers the same whether or not the core
    /// has room. Empty when the core has stopped. See [`PoolHandle`].
    pub async fn rejected_ids(
        &self,
        ids: HashSet<UnminedTxId>,
        deadline: Instant,
    ) -> Result<HashSet<UnminedTxId>, Elapsed> {
        by(deadline, async {
            let (reply, answer) = oneshot::channel();
            let command = PoolCommand::RejectedIds { ids, reply };
            if self.commands.send(command).await.is_err() {
                return HashSet::new();
            }
            answer.await.unwrap_or_default()
        })
        .await
    }

    /// The latest snapshot, or `None` until the core has attached and
    /// published one.
    pub fn snapshot(&self) -> Option<Arc<PoolSnapshot>> {
        self.snapshot.borrow().clone()
    }

    /// A reader of the snapshot that does not keep the core running.
    pub fn reader(&self) -> PoolReader {
        PoolReader {
            snapshot: self.snapshot.clone(),
            shared: self.shared.clone(),
        }
    }

    /// Wait for a snapshot newer than the last one this handle saw. Returns
    /// `false` when the core has stopped.
    pub async fn snapshot_changed(&mut self) -> bool {
        self.snapshot.changed().await.is_ok()
    }

    /// The latest snapshot with the changes after `cursor` that it
    /// includes, or `None` until the core has published one.
    ///
    /// A reader that announces or serves from the log uses this, so it never
    /// sees an addition ahead of the snapshot that holds the transaction.
    /// Its next cursor is the snapshot's [`PoolSnapshot::last_change`].
    pub fn view(&self, cursor: ChangeSeq) -> Option<PoolView> {
        view(&self.shared, &self.snapshot, cursor)
    }
}

impl PoolInbox {
    /// The next thing a caller did, or `None` once every handle is gone.
    ///
    /// A dropped reservation arrives as [`PoolCommand::Released`] and wakes
    /// this as any command does, so an idle core still frees the slot and
    /// answers anyone waiting on it. Releases come first, so a freed slot is
    /// seen by the next reservation.
    pub async fn next(&mut self) -> Option<PoolCommand> {
        zakura_rt::select! {
            biased;
            Some(released) = self.releases.recv() => Some(PoolCommand::Released(released)),
            command = self.commands.recv() => command,
        }
    }

    /// What [`Self::next`] would yield now, without waiting: a dropped
    /// reservation first, then a queued command.
    pub fn try_next(&mut self) -> Option<PoolCommand> {
        if let Ok(released) = self.releases.try_recv() {
            return Some(PoolCommand::Released(released));
        }
        self.commands.try_recv().ok()
    }

    /// The next command, or `None` once every handle is gone. This does not
    /// wake for a dropped reservation. The core's task uses [`Self::next`].
    #[cfg(test)]
    pub(crate) async fn command(&mut self) -> Option<PoolCommand> {
        self.commands.recv().await
    }

    /// A reservation that was dropped before the core accepted it, if one is
    /// waiting. The core drains these before it answers a command, so a
    /// freed slot is seen by the next reservation.
    #[cfg(test)]
    pub(crate) fn released(&mut self) -> Option<ReservationId> {
        self.releases.try_recv().ok()
    }

    /// Grant a reservation whose drop the core will hear about, and which
    /// the core cancels through `cancel`.
    pub fn grant(
        &self,
        id: ReservationId,
        tx_id: UnminedTxId,
        cancel: CancellationToken,
    ) -> Reservation {
        Reservation::granted(id, tx_id, self.release_handle.clone(), cancel)
    }

    /// Publish one batch: append `changes` to the log, then publish the
    /// snapshot `build` makes for the position of the batch's last entry.
    ///
    /// Both happen under the log's lock, so [`PoolHandle::view`] never pairs
    /// a snapshot with entries newer than it.
    ///
    /// The core publishes a batch before it starts the jobs that batch
    /// calls for. A verification that looks an unmined output up therefore
    /// reads a pool at least as new as the one that started it.
    pub fn publish(
        &self,
        changes: impl IntoIterator<Item = Change>,
        build: impl FnOnce(ChangeSeq) -> PoolSnapshot,
    ) {
        self.publish_batches([changes], build);
    }

    /// Publish several batches, in order, and one snapshot for the position
    /// of the last batch's last entry. Readers see the batches as separate
    /// batches, and no snapshot between them. See [`Self::publish`].
    pub fn publish_batches<C: IntoIterator<Item = Change>>(
        &self,
        batches: impl IntoIterator<Item = C>,
        build: impl FnOnce(ChangeSeq) -> PoolSnapshot,
    ) {
        let mut log = self
            .shared
            .log
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut last = log.last();
        for changes in batches {
            last = log.append(changes);
        }
        let snapshot = Arc::new(build(last));
        self.snapshot.send_replace(Some(snapshot));
    }
}
