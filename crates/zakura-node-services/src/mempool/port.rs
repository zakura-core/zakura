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
//! 1. [`PoolHandle::reserve`] takes a slot for one transaction id, counted
//!    against a [`Source`]. It answers [`Unreserved`] when the id is already
//!    known or there is no room. Nothing is fetched without a reservation, so
//!    one count covers a transaction from its fetch to its verdict.
//! 2. [`PoolHandle::submit`] hands over the body and names the [`Supplier`]
//!    whose bytes these are. It answers exactly one [`Verdict`].
//! 3. Dropping a [`Reservation`] that was never submitted frees its slot.
//!
//! A slot is counted against the source that reserved it until the verdict,
//! whoever supplies the body. So a caller that asks a second peer for the same
//! id keeps its reservation and names that second peer as the supplier. No
//! operation moves a reservation between sources.
//!
//! Readers follow the pool without asking the core for work: the latest
//! [`PoolSnapshot`] and the numbered [`ChangeEntry`] log.
//!
//! [`model`] restates these rules as an independent model.

use std::{collections::VecDeque, fmt, sync::Arc};

use zakura_chain::{
    block,
    transaction::{self, Transaction, UnminedTx, UnminedTxId, VerifiedUnminedTx},
    transparent,
};
use zakura_rt::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc, oneshot, watch, Mutex, PoisonError,
    },
};

use super::{QueueSource, TransactionDependencies};

#[cfg(any(test, feature = "test-support"))]
pub mod model;
#[cfg(test)]
mod tests;

/// Undecided transactions the core holds at once, node-wide. Today's
/// `MAX_INBOUND_CONCURRENCY`.
pub const MAX_PENDING: usize = 500;

/// Undecided transactions counted against one peer source. Today's
/// `MAX_INBOUND_CONCURRENCY_PER_PEER`.
pub const MAX_PENDING_PER_SOURCE: usize = 5;

/// Submissions that can wait on one undecided transaction. Provisional.
pub const MAX_WAITERS: usize = 8;

/// Slots that revalidation after a reorg can hold. The rest of
/// [`MAX_PENDING`] stays free for new admissions. Provisional.
pub const MAX_REVALIDATING: usize = 400;

/// Entries the change log keeps. A reader that falls further behind
/// resynchronizes from the snapshot. The pool's 8,000 transactions plus a
/// ring of 16,384 removals.
pub const CHANGE_LOG_CAPACITY: usize = 24_384;

/// Commands that can wait for the core. A full queue makes
/// [`PoolHandle::reserve`] and [`PoolHandle::submit`] wait.
pub const COMMAND_QUEUE: usize = 1_024;

/// Who an undecided transaction is counted against.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Source {
    /// A peer. At most [`MAX_PENDING_PER_SOURCE`] undecided transactions
    /// count against it.
    Peer(QueueSource),
    /// This node: RPC, wallets and the crawler. Counted against
    /// [`MAX_PENDING`] only.
    Node,
}

/// Whose bytes a submission is. A verdict that charges anyone charges the
/// supplier.
///
/// The source and the supplier can differ. A v2 peer that reaches the core
/// through the legacy adapter is a source with no supplier. A `tx-relay`
/// reservation is counted against the first peer asked, and the supplier is
/// the peer that answered.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Supplier {
    /// The peer that sent the body.
    Peer(QueueSource),
    /// A local source, or a body nobody can be charged for.
    Nobody,
}

/// Why the caller is reserving.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Intent {
    /// The caller will fetch the body. An id that another caller is already
    /// bringing in answers [`Unreserved::Pending`], so it is not fetched
    /// twice.
    Fetch,
    /// The caller holds the body. An id that is already undecided is joined:
    /// the submission waits for that verdict and takes no slot.
    Deliver,
}

/// Names one reservation to the core.
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ReservationId(pub u64);

/// A slot in the core for one transaction id, or a place among the waiters
/// of an id that is already undecided.
///
/// Dropping it before [`PoolHandle::submit`] frees the slot.
#[must_use = "dropping a reservation frees its slot"]
pub struct Reservation {
    id: ReservationId,
    tx_id: UnminedTxId,
    joined: bool,
    release: Option<mpsc::UnboundedSender<ReservationId>>,
}

impl Reservation {
    /// Build the reservation the core granted. `release` is the port's
    /// release queue.
    pub fn granted(
        id: ReservationId,
        tx_id: UnminedTxId,
        joined: bool,
        release: mpsc::UnboundedSender<ReservationId>,
    ) -> Self {
        Self {
            id,
            tx_id,
            joined,
            release: Some(release),
        }
    }

    /// The core's name for this reservation.
    pub fn id(&self) -> ReservationId {
        self.id
    }

    /// The transaction id it was reserved for.
    pub fn tx_id(&self) -> UnminedTxId {
        self.tx_id
    }

    /// Whether it joined an id that was already undecided. A joined
    /// reservation holds no slot.
    pub fn joined(&self) -> bool {
        self.joined
    }

    /// Give up the release-on-drop, because the core now owns the outcome.
    fn disarm(mut self) -> ReservationId {
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
            .field("joined", &self.joined)
            .finish_non_exhaustive()
    }
}

/// Which limit refused a reservation.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum NoRoom {
    /// [`MAX_PENDING`] is reached.
    Node,
    /// The source holds [`MAX_PENDING_PER_SOURCE`].
    Source,
}

/// Why [`PoolHandle::reserve`] granted nothing. The caller fetches nothing.
#[derive(Clone, Debug)]
pub enum Unreserved {
    /// The pool is off, because the node is far from the tip.
    Inactive,
    /// The source is in a cooldown.
    CoolingDown,
    /// The transaction is in the pool.
    Pooled,
    /// The id is undecided and this reservation cannot join it: the intent
    /// was [`Intent::Fetch`], the source already waits on it, or
    /// [`MAX_WAITERS`] is reached. Today's `AlreadyQueued`.
    Pending,
    /// A cached rejection covers the id.
    Rejected(Rejection),
    /// A limit is reached. The caller can try again later.
    NoRoom(NoRoom),
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
    /// Any transaction with these effects, on this chain.
    EffectsChain,
    /// Not remembered: a parent that never arrived, or a transaction a block
    /// already mined.
    Uncached,
}

/// What a rejection costs the supplier.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Charge {
    /// Nothing. The failure does not show the supplier misbehaved.
    None,
    /// The core refuses the supplier's transactions for a while. The core
    /// has already started the cooldown.
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
    /// What it costs this submission's supplier. A local source and
    /// [`Supplier::Nobody`] are never charged.
    pub charge: Charge,
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
    /// A limit was reached. Only a submission without a slot can get this.
    NoRoom(NoRoom),
    /// A failure on this node.
    LocalFault(LocalFault),
}

/// A wire violation by a peer that submitted nothing. The core starts the
/// peer's cooldown.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Violation {
    /// The peer.
    pub peer: QueueSource,
    /// A fixed label for metrics and logs.
    pub cause: &'static str,
}

/// Why [`PoolHandle::await_output`] cannot wait.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum AwaitOutputError {
    /// Too many outpoints are being waited on.
    TooManyWaiters,
    /// The pool is off.
    Inactive,
    /// The core has stopped.
    Stopped,
}

/// A stable number for the best chain as state readers see it.
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Generation(pub u64);

/// One reading of the [`GenerationCell`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum GenerationRead {
    /// Readers see this generation, and no change has begun.
    Stable(Generation),
    /// A change is becoming visible. Readers can see the old chain or the
    /// new one.
    Changing,
}

impl GenerationRead {
    /// Whether `self` and `after` bracket work that saw only generation
    /// `applied`: both are the same stable number, and it is `applied`.
    pub fn brackets(self, after: Self, applied: Generation) -> bool {
        self == after && self == Self::Stable(applied)
    }
}

/// The best chain's generation, shared by the state writer and its readers.
///
/// The writer calls [`Self::begin_change`] before any new best-chain state
/// becomes readable, and [`Self::finish_change`] once the tip is published.
/// A reader that sees the same stable number before and after its lookups
/// knows no change became visible in between.
#[derive(Clone, Debug, Default)]
pub struct GenerationCell(Arc<AtomicU64>);

impl GenerationCell {
    /// A cell at generation zero, stable.
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark the generation as changing. Only the state writer calls this.
    ///
    /// # Panics
    ///
    /// If a change has already begun.
    pub fn begin_change(&self) {
        let before = self.0.fetch_add(1, Ordering::SeqCst);
        assert!(
            before.is_multiple_of(2),
            "the one state writer finishes each change before it begins the next"
        );
    }

    /// Mark the next generation as stable, and return it. Only the state
    /// writer calls this.
    ///
    /// # Panics
    ///
    /// If no change has begun.
    pub fn finish_change(&self) -> Generation {
        let before = self.0.fetch_add(1, Ordering::SeqCst);
        assert!(
            !before.is_multiple_of(2),
            "the one state writer begins a change before it finishes one"
        );
        Generation(before.saturating_add(1) / 2)
    }

    /// Read the generation.
    pub fn read(&self) -> GenerationRead {
        let raw = self.0.load(Ordering::SeqCst);
        if raw.is_multiple_of(2) {
            GenerationRead::Stable(Generation(raw / 2))
        } else {
            GenerationRead::Changing
        }
    }
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
/// conflict with a transaction that was never in the pool.
#[derive(Clone, Debug)]
pub struct ConnectedBlock {
    /// The block's hash.
    pub hash: block::Hash,
    /// The block's height.
    pub height: block::Height,
    /// The block's transactions, in block order.
    pub transactions: Vec<Arc<Transaction>>,
    /// The mined ids of `transactions`, in the same order.
    pub transaction_hashes: Arc<[transaction::Hash]>,
}

/// One change to the best chain: the blocks that left it, then the blocks
/// that joined it.
#[derive(Clone, Debug)]
pub struct ChainTransition {
    /// The tip before, or `None` for the first block.
    pub before: Option<ChainTip>,
    /// The blocks that left the best chain, tip first. Empty unless this is
    /// a reorg.
    pub disconnected: Vec<ChainTip>,
    /// The blocks that joined, lowest first.
    pub connected: Vec<ConnectedBlock>,
    /// The tip after.
    pub after: ChainTip,
    /// The stable generation this transition ends on.
    pub generation: Generation,
}

impl ChainTransition {
    /// The blocks this transition names, which is what the queue's bound
    /// counts.
    pub fn blocks(&self) -> usize {
        self.disconnected.len().saturating_add(self.connected.len())
    }

    /// Whether any block left the best chain.
    pub fn is_reorg(&self) -> bool {
        !self.disconnected.is_empty()
    }
}

/// What the core takes from the state's transition queue.
#[derive(Clone, Debug)]
pub enum ChainEvent {
    /// The next transition, none skipped.
    Transition(ChainTransition),
    /// The queue overflowed. The core no longer knows which blocks changed,
    /// so it checks every pooled and parked transaction again at this tip.
    Resync {
        /// The tip now.
        tip: ChainTip,
        /// Its stable generation.
        generation: Generation,
    },
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
    /// It failed when it was verified again, or a transaction it depends on
    /// left the pool.
    Invalidated,
    /// The pool turned off.
    Deactivated,
}

/// One change, in the order the core made it.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Change {
    /// The transaction entered the pool and is visible.
    Added(UnminedTxId),
    /// The transaction left the pool.
    Removed {
        /// The transaction.
        id: UnminedTxId,
        /// Why it left.
        reason: RemovalReason,
    },
    /// The transaction is still in the pool but hidden from snapshots while
    /// it is verified again after a reorg.
    Hidden(UnminedTxId),
    /// A hidden transaction passed and is visible again. Announcers skip
    /// this, because peers already heard of it.
    Revealed(UnminedTxId),
    /// A submitted transaction was rejected and never entered the pool.
    /// Today's change stream reports this as `Invalidated`.
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

    /// Every entry after `cursor`.
    pub fn since(&self, cursor: ChangeSeq) -> ChangesSince {
        if cursor >= self.last {
            return ChangesSince::Entries(Vec::new());
        }
        let oldest = self.entries.front().map_or(self.last, |entry| entry.seq);
        if cursor.0.saturating_add(1) < oldest.0 {
            return ChangesSince::Resync;
        }
        ChangesSince::Entries(
            self.entries
                .iter()
                .filter(|entry| entry.seq > cursor)
                .copied()
                .collect(),
        )
    }
}

/// The visible pool at one moment. Immutable once published.
#[derive(Clone, Debug)]
pub struct PoolSnapshot {
    /// The tip the pool is valid for.
    pub tip: ChainTip,
    /// The generation of that tip.
    pub generation: Generation,
    /// The newest change this snapshot includes.
    pub last_change: ChangeSeq,
    /// The visible transactions, in mining order.
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
        generation: Generation,
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
            tip,
            generation,
            last_change,
            transactions,
            dependencies,
            by_mined_id,
            spent,
        }
    }

    /// The visible transaction with this mined id.
    pub fn transaction(&self, id: &transaction::Hash) -> Option<&VerifiedUnminedTx> {
        self.by_mined_id
            .get(id)
            .and_then(|index| self.transactions.get(*index))
    }

    /// The output a visible transaction creates at `outpoint`.
    pub fn created_output(&self, outpoint: &transparent::OutPoint) -> Option<&transparent::Output> {
        let index = usize::try_from(outpoint.index).ok()?;
        self.transaction(&outpoint.hash)?
            .transaction
            .transaction()
            .outputs()
            .get(index)
    }

    /// Whether a visible transaction spends `outpoint`.
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
        /// Who the slot counts against.
        source: Source,
        /// Whether the caller will fetch or already holds the body.
        intent: Intent,
        /// Where the answer goes.
        reply: oneshot::Sender<Result<Reservation, Unreserved>>,
    },
    /// [`PoolHandle::submit`].
    Submit {
        /// The reservation the body arrives under.
        reservation: ReservationId,
        /// The body.
        transaction: UnminedTx,
        /// Whose bytes it is.
        supplier: Supplier,
        /// Where the verdict goes.
        reply: oneshot::Sender<Verdict>,
    },
    /// [`PoolHandle::report_violation`].
    ReportViolation(Violation),
    /// [`PoolHandle::await_output`]. The core looks the output up and, if it
    /// is missing, registers the waiter, in one step of its task. So an
    /// admission cannot fall between the lookup and the registration.
    AwaitOutput {
        /// The outpoint.
        outpoint: transparent::OutPoint,
        /// Where the output goes when a transaction that creates it is
        /// admitted, or the reason the core will not wait.
        reply: oneshot::Sender<Result<transparent::Output, AwaitOutputError>>,
    },
}

/// What readers share with the core.
#[derive(Debug, Default)]
struct Shared {
    log: Mutex<ChangeLog>,
}

/// The callers' side of the port. Cheap to clone.
#[derive(Clone, Debug)]
pub struct PoolHandle {
    commands: mpsc::Sender<PoolCommand>,
    snapshot: watch::Receiver<Option<Arc<PoolSnapshot>>>,
    shared: Arc<Shared>,
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

impl PoolHandle {
    /// Reserve a slot for `tx_id`, counted against `source`, before fetching
    /// or delivering its body.
    pub async fn reserve(
        &self,
        tx_id: UnminedTxId,
        source: Source,
        intent: Intent,
    ) -> Result<Reservation, Unreserved> {
        let (reply, answer) = oneshot::channel();
        let command = PoolCommand::Reserve {
            tx_id,
            source,
            intent,
            reply,
        };
        if self.commands.send(command).await.is_err() {
            return Err(Unreserved::Stopped);
        }
        answer.await.unwrap_or(Err(Unreserved::Stopped))
    }

    /// Submit the body `reservation` was taken for, and wait for its verdict.
    ///
    /// The slot stays counted against the reservation's source. `supplier`
    /// is whose bytes these are, which can be another peer.
    pub async fn submit(
        &self,
        reservation: Reservation,
        transaction: UnminedTx,
        supplier: Supplier,
    ) -> Verdict {
        if transaction.id() != reservation.tx_id() {
            // Dropping the reservation frees its slot.
            return Verdict::LocalFault(LocalFault::WrongTransaction);
        }
        let (reply, verdict) = oneshot::channel();
        let command = PoolCommand::Submit {
            reservation: reservation.disarm(),
            transaction,
            supplier,
            reply,
        };
        if self.commands.send(command).await.is_err() {
            return Verdict::LocalFault(LocalFault::Stopped);
        }
        verdict
            .await
            .unwrap_or(Verdict::LocalFault(LocalFault::Stopped))
    }

    /// Report a wire violation by a peer that submitted nothing, so its
    /// cooldown starts. Waits only while the command queue is full.
    pub async fn report_violation(&self, violation: Violation) {
        let _ = self
            .commands
            .send(PoolCommand::ReportViolation(violation))
            .await;
    }

    /// Wait for a pool transaction to create `outpoint`. Answers at once if
    /// one already has.
    pub async fn await_output(
        &self,
        outpoint: transparent::OutPoint,
    ) -> Result<transparent::Output, AwaitOutputError> {
        let (reply, answer) = oneshot::channel();
        let command = PoolCommand::AwaitOutput { outpoint, reply };
        if self.commands.send(command).await.is_err() {
            return Err(AwaitOutputError::Stopped);
        }
        answer.await.unwrap_or(Err(AwaitOutputError::Stopped))
    }

    /// The latest snapshot, or `None` until the core has attached and
    /// published one.
    pub fn snapshot(&self) -> Option<Arc<PoolSnapshot>> {
        self.snapshot.borrow().clone()
    }

    /// Wait for a snapshot newer than the last one this handle saw. Returns
    /// `false` when the core has stopped.
    pub async fn snapshot_changed(&mut self) -> bool {
        self.snapshot.changed().await.is_ok()
    }

    /// Every change after `cursor`.
    pub fn changes_since(&self, cursor: ChangeSeq) -> ChangesSince {
        self.shared
            .log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .since(cursor)
    }
}

impl PoolInbox {
    /// The next command, or `None` once every handle is gone.
    pub async fn command(&mut self) -> Option<PoolCommand> {
        self.commands.recv().await
    }

    /// A reservation a caller dropped without submitting, if one is waiting.
    /// The core drains these before it answers a command, so a freed slot is
    /// seen by the next reservation.
    pub fn released(&mut self) -> Option<ReservationId> {
        self.releases.try_recv().ok()
    }

    /// Grant a reservation whose drop the core will hear about.
    pub fn grant(&self, id: ReservationId, tx_id: UnminedTxId, joined: bool) -> Reservation {
        Reservation::granted(id, tx_id, joined, self.release_handle.clone())
    }

    /// The position of the newest change, which the next snapshot records as
    /// its [`PoolSnapshot::last_change`].
    pub fn last_change(&self) -> ChangeSeq {
        self.shared
            .log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .last()
    }

    /// Append one batch to the change log and return the position of its
    /// last entry.
    pub fn log(&self, changes: impl IntoIterator<Item = Change>) -> ChangeSeq {
        self.shared
            .log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .append(changes)
    }

    /// Publish the snapshot that includes every change logged so far.
    /// Readers see the log entries before the snapshot that includes them.
    pub fn publish(&self, snapshot: PoolSnapshot) {
        self.snapshot.send_replace(Some(Arc::new(snapshot)));
    }
}
