//! An independent model of the pool core, as callers see it through the port.
//!
//! The model restates the documented rules and never calls the code it
//! checks. The core's machine tests compare the machine with it, and
//! `tx-relay`'s tests use it as their pool.
//!
//! It takes the same inputs as the core: port calls, verification outcomes,
//! chain events and time. Each input returns the [`Step`] it caused. A
//! verification is not run here. The model says which job to start, and the
//! test feeds back the [`Outcome`].
//!
//! Three things are left out, because they are rules of the storage code
//! that moves unchanged and keeps its own tests. Eviction at the pool's cost
//! limit: the model assumes the pool stays below it. The snapshot's mining
//! order, by ancestor-package fee: the model has no fees, and
//! [`PoolModel::visible`] is oldest first, which is not that order. And
//! cooldown escalation: a cooldown lasts [`COOLDOWN`] each time.
//!
//! A slot outlives its admission while the caller that reserved it is still
//! fetching. If another caller's body decides the id first, the reserving
//! caller's slot stays taken until it submits its body or drops the
//! reservation. Otherwise a source could have more fetches out than slots.
//!
//! Two orderings are decided on purpose and are not gaps.
//!
//! A deadline is decided on what the core has applied. A parent mined in a
//! block the core has not read yet can lose that race by a moment. The
//! transaction is then answered as today's timeout would answer it, and the
//! tip change that follows forgets the rejection.
//!
//! While the chain changes faster than a check completes, pooled
//! transactions stay hidden, because no check of them can be trusted. They
//! hold no slot between checks. Today each of those changes empties the
//! pool.
//!
//! A hidden transaction queued behind a hidden parent is waiting for that
//! parent's check, not for a missing parent, and is decided when the parent
//! is. A parent that keeps hitting local faults keeps both hidden. That is
//! the rule for a pooled transaction on a node whose verifier is failing.
//!
//! Two rules are for the core's task, not the machine. A worker waits for a
//! stable generation before its first lookup. And the core publishes a
//! batch's snapshot before it starts that batch's jobs, so a verification
//! reads a pool at least as new as the one that started it.
//!
//! The model is synchronous, so it has no bodies in transit. What it bounds
//! is what the core holds: one body for each undecided transaction. A body
//! on its way to the core is its caller's to bound. A fetched body travels
//! under a slot. A body a caller held before it reserved, such as one a
//! peer pushed, is held by that caller whatever the core answers, and the
//! core asks at most one such caller at a time to send it.

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    time::Duration,
};

use zakura_chain::{
    block::Height,
    transaction::{self, UnminedTxId},
    transparent::OutPoint,
};

use super::{
    ChainReason, Change, ChangeEntry, ChangeSeq, Charge, CooldownKey, Generation, GenerationRead,
    Intent, LocalFault, NoRoom, RejectionScope, RemovalReason, ReservationId, Source, Supplier,
    MAX_COOLDOWNS, MAX_PENDING, MAX_PENDING_PER_SOURCE, MAX_REJECTIONS, MAX_REVALIDATING,
    MAX_WAITERS,
};
use crate::mempool::QueueSource;

/// Time since the model started.
pub type ModelTime = Duration;

/// How long one verification can take. Provisional.
pub const T_VERIFY: Duration = Duration::from_secs(60);

/// How long a transaction waits for a missing parent, counted from the
/// first time one of its inputs is found missing. Today's wait for a pool
/// parent.
///
/// The wait is flat. It does not stop while a parent is being verified, and
/// waking the transaction or checking it again does not restart it. So a
/// transaction with a missing input is answered within this long plus one
/// verification, whatever its parents, the chain or its peers do.
pub const T_PARK: Duration = Duration::from_secs(60);

/// The longest an undecided transaction holds its slot after its body
/// arrives: the wait for a parent plus one verification.
///
/// Every other deadline is about one stage, and a transaction can move
/// between stages for as long as something keeps moving it: a chain that
/// changes during every check, or inputs that arrive one at a time. This one
/// is about the slot. When it ends the submission is answered with a local
/// fault, nothing is cached and nobody is charged, because the core never
/// got a trustworthy answer.
pub const MAX_UNDECIDED: Duration = Duration::from_secs(120);

/// How long the core refuses a peer after a cooldown charge or a violation.
/// Today's base cooldown.
pub const COOLDOWN: Duration = Duration::from_secs(10 * 60);

/// How long a chain-scoped rejection is remembered unless the model is told
/// otherwise. Today's default `eviction_memory_time`.
pub const REJECTION_MEMORY: Duration = Duration::from_secs(60 * 60);

/// The most distinct outpoints callers can wait for at once. Today's
/// `MAX_PENDING_OUTPUTS`.
pub const MAX_OUTPUT_WAITS: usize = 65_536;

/// The most pooled ancestors a new transaction can have. Today's
/// `MAX_MEMPOOL_ANCESTORS`.
pub const MAX_ANCESTORS: usize = 2;

/// The most transactions in one connected group of pooled dependencies.
/// Today's `MAX_MEMPOOL_PACKAGE_TRANSACTIONS`.
pub const MAX_PACKAGE: usize = 3;

/// The first wait before a revalidation that hit a local fault is tried
/// again. It doubles up to [`REVALIDATE_RETRY_MAX`]. Provisional.
pub const REVALIDATE_RETRY: Duration = Duration::from_secs(1);

/// The longest wait between revalidation tries. Provisional.
pub const REVALIDATE_RETRY_MAX: Duration = Duration::from_secs(60);

/// What the model needs to know about a transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelTx {
    /// The exact id.
    pub id: UnminedTxId,
    /// The transparent outputs it spends.
    pub spends: Vec<OutPoint>,
    /// Its shielded spends, as opaque keys.
    pub nullifiers: Vec<u64>,
    /// How many transparent outputs it creates.
    pub outputs: u32,
    /// Its expiry height, if it has one.
    pub expiry: Option<Height>,
}

impl ModelTx {
    /// A transaction with no inputs, one output and no expiry.
    pub fn new(id: UnminedTxId) -> Self {
        Self {
            id,
            spends: Vec::new(),
            nullifiers: Vec::new(),
            outputs: 1,
            expiry: None,
        }
    }

    /// The same transaction, also spending output `index` of `parent`.
    pub fn spending(mut self, parent: transaction::Hash, index: u32) -> Self {
        self.spends.push(OutPoint {
            hash: parent,
            index,
        });
        self
    }

    fn parents(&self) -> impl Iterator<Item = transaction::Hash> + '_ {
        self.spends.iter().map(|outpoint| outpoint.hash)
    }

    fn conflicts_with(&self, other: &ModelTx) -> bool {
        self.spends.iter().any(|spend| other.spends.contains(spend))
            || self
                .nullifiers
                .iter()
                .any(|nullifier| other.nullifiers.contains(nullifier))
    }
}

/// What a verification found.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Outcome {
    /// The transaction is valid at the tip it was verified against, and
    /// every input it spends is on the chain.
    Pass,
    /// The transaction is valid, and these inputs came from the pool the
    /// verifier saw, not from the chain.
    PassUsing(Vec<OutPoint>),
    /// A block on the best chain already mined the transaction.
    AlreadyMined,
    /// The transaction is invalid.
    Fail {
        /// How long the rejection is remembered.
        scope: RejectionScope,
        /// What it costs a peer supplier.
        charge: Charge,
    },
    /// These transparent inputs are in neither the chain nor the visible
    /// pool.
    MissingInputs(Vec<OutPoint>),
    /// The verification failed on this node.
    LocalFault(LocalFault),
}

/// Names one submission that waits for a verdict.
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WaiterId(pub u64);

/// Names one verification job.
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct JobId(pub u64);

/// The model's verdict: [`super::Verdict`] without the error detail.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ModelVerdict {
    /// The transaction is in the pool.
    Admitted,
    /// The core refused the transaction.
    Rejected {
        /// How long the rejection is remembered.
        scope: RejectionScope,
        /// What it costs this waiter's supplier.
        charge: Charge,
    },
    /// A limit was reached.
    NoRoom(NoRoom),
    /// The supplier is in a cooldown, so its body was not verified.
    CoolingDown,
    /// A failure on this node.
    LocalFault(LocalFault),
}

/// The model's answer to a refused reservation: [`super::Unreserved`]
/// without the error detail.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ModelUnreserved {
    /// The pool is off.
    Inactive,
    /// The source is in a cooldown.
    CoolingDown,
    /// The transaction is in the pool.
    Pooled,
    /// The id is undecided and cannot be joined.
    Pending,
    /// A cached rejection covers the id.
    Rejected(RejectionScope),
    /// A limit is reached.
    NoRoom(NoRoom),
}

/// A granted reservation.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ModelReservation {
    /// Its name.
    pub id: ReservationId,
    /// Whether it joined an id that was already undecided.
    pub joined: bool,
    /// Whether the core needs the body from this caller.
    pub wants_body: bool,
}

/// One thing the core does because of an input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Effect {
    /// Start a verification of this transaction.
    Start {
        /// The job.
        job: JobId,
        /// The transaction.
        tx: UnminedTxId,
        /// Whether it is a pooled transaction being verified again.
        revalidation: bool,
    },
    /// Cancel a verification. Its outcome is ignored.
    Cancel(JobId),
    /// Answer a submission.
    Verdict {
        /// The submission.
        waiter: WaiterId,
        /// Its verdict.
        verdict: ModelVerdict,
    },
    /// The transparent outputs of a transaction that just became visible
    /// are available, in the same step.
    Outputs {
        /// The transaction.
        tx: transaction::Hash,
        /// How many outputs it creates.
        count: u32,
    },
    /// A caller waiting for an output is answered with it. The answer is
    /// the output at `outpoint` of the transaction that just became visible.
    /// The model carries no output values, so a comparison reads that value
    /// from the transaction's body and checks the machine returned it.
    OutputReady {
        /// The waiting caller.
        waiter: WaiterId,
        /// The output.
        outpoint: OutPoint,
    },
    /// A caller waiting for an output is told the pool turned off.
    OutputWaitEnded(WaiterId),
}

/// The answer to [`PoolModel::await_output`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum OutputAnswer {
    /// A visible pooled transaction creates the output, and the answer is
    /// that output: the one at the outpoint asked for.
    Ready,
    /// Nothing visible creates it. The caller waits under this name.
    Waiting(WaiterId),
    /// Too many distinct outpoints are being waited on.
    TooManyWaiters,
    /// The pool is off.
    Inactive,
}

/// What one input caused, in order.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Step {
    /// What the core does.
    pub effects: Vec<Effect>,
    /// The batch for the change log.
    pub changes: Vec<Change>,
}

impl Step {
    /// The verdict this step gave `waiter`, if it gave one.
    pub fn verdict(&self, waiter: WaiterId) -> Option<ModelVerdict> {
        self.effects.iter().find_map(|effect| match effect {
            Effect::Verdict { waiter: w, verdict } if *w == waiter => Some(*verdict),
            _ => None,
        })
    }

    /// The jobs this step started.
    pub fn started(&self) -> Vec<JobId> {
        self.effects
            .iter()
            .filter_map(|effect| match effect {
                Effect::Start { job, .. } => Some(*job),
                _ => None,
            })
            .collect()
    }
}

/// A block that joined the best chain, as the model sees it.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ModelBlock {
    /// The block's transactions.
    pub transactions: Vec<ModelTx>,
}

/// One change to the best chain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelTransition {
    /// How many blocks left the best chain.
    pub disconnected: usize,
    /// The blocks that joined, lowest first.
    pub connected: Vec<ModelBlock>,
    /// The height of the tip after.
    pub tip: Height,
    /// The stable generation the transition ends on.
    pub generation: Generation,
    /// Whether the rules change at this tip.
    pub rules_change: bool,
}

/// How a reader of the change log decides what to announce. A `tx-relay`
/// session is one.
///
/// It announces an addition only when the snapshot it reads with holds the
/// transaction. An addition it could not announce, because the transaction
/// was hidden by then, is deferred and announced when it is revealed. So
/// what it skips on a reveal is exactly what it has announced.
#[derive(Clone, Debug)]
pub struct ModelReader {
    /// The newest entry it has read.
    cursor: ChangeSeq,
    /// The position of the snapshot it last announced in full.
    baseline: ChangeSeq,
    /// Additions it has read and not announced.
    deferred: HashSet<UnminedTxId>,
}

impl ModelReader {
    /// Opens on a snapshot and announces all of it.
    pub fn open(visible: &[UnminedTxId], last_change: ChangeSeq) -> (Self, Vec<UnminedTxId>) {
        let reader = Self {
            cursor: last_change,
            baseline: last_change,
            deferred: HashSet::new(),
        };
        (reader, visible.to_vec())
    }

    /// The reader fell off the log. It cannot know which additions it lost,
    /// so it announces the whole snapshot again and starts from it.
    pub fn resync(&mut self, visible: &[UnminedTxId], last_change: ChangeSeq) -> Vec<UnminedTxId> {
        self.cursor = last_change;
        self.baseline = last_change;
        self.deferred.clear();
        visible.to_vec()
    }

    /// The newest entry it has read, which is its cursor for the next view.
    pub fn cursor(&self) -> ChangeSeq {
        self.cursor
    }

    /// Reads one view: the entries after its cursor, and the visible pool of
    /// the snapshot they lead to. Returns what it announces.
    pub fn read(&mut self, entries: &[ChangeEntry], visible: &[UnminedTxId]) -> Vec<UnminedTxId> {
        let mut announced = Vec::new();
        for entry in entries {
            match entry.change {
                Change::Added(id) => {
                    if visible.contains(&id) {
                        announced.push(id);
                    } else {
                        self.deferred.insert(id);
                    }
                }
                Change::Revealed { id, hidden_at } => {
                    let unannounced =
                        self.deferred.contains(&id) || self.baseline.announces_reveal(hidden_at);
                    if unannounced && visible.contains(&id) {
                        self.deferred.remove(&id);
                        announced.push(id);
                    } else if unannounced {
                        self.deferred.insert(id);
                    }
                }
                Change::Removed { id, .. } | Change::Refused(id) => {
                    self.deferred.remove(&id);
                }
                Change::Hidden(_) => {}
            }
            self.cursor = entry.seq;
        }
        announced
    }
}

#[derive(Clone, Debug)]
struct Waiter {
    id: WaiterId,
    source: Source,
    supplier: Supplier,
}

#[derive(Clone, Debug)]
enum Stage {
    /// No body has arrived.
    Reserved,
    /// A job is running.
    Verifying { job: JobId, deadline: ModelTime },
    /// The last outcome was stale, and the chain event that made it stale has
    /// not been applied yet. The job runs again when it has.
    AwaitingChain,
    /// Parents are missing.
    Parked { on: Vec<transaction::Hash> },
}

#[derive(Clone, Debug)]
struct Admission {
    tx_id: UnminedTxId,
    owner: ReservationId,
    /// Whether the owner can still submit. False once it has submitted or
    /// released.
    owner_waiting: bool,
    source: Source,
    body: Option<ModelTx>,
    waiters: Vec<Waiter>,
    stage: Stage,
    /// When one of its inputs was first found missing. Its wait for a
    /// parent runs from here and is never restarted.
    missing_since: Option<ModelTime>,
    /// Whether the one check allowed after that wait ended has been used.
    last_check_used: bool,
    /// When its body arrived. Its slot is held at most [`MAX_UNDECIDED`]
    /// from here.
    body_at: Option<ModelTime>,
}

#[derive(Clone, Debug)]
enum Hidden {
    /// Waiting for a slot, or for a hidden parent to pass first.
    Queued,
    Running {
        job: JobId,
        deadline: ModelTime,
    },
    AwaitingChain,
    Retry {
        at: ModelTime,
    },
    /// Parents are missing.
    Parked {
        on: Vec<transaction::Hash>,
    },
}

#[derive(Clone, Debug)]
struct Pooled {
    tx: ModelTx,
    hidden: Option<Hidden>,
    /// The wait before the next try after a local fault.
    backoff: Duration,
    /// The model's event count when it last became visible.
    visible_at: u64,
    /// The log position of its latest `Hidden` entry.
    hidden_at: ChangeSeq,
    /// When one of its inputs was first found missing since it was last
    /// visible. Its wait for a parent runs from here.
    missing_since: Option<ModelTime>,
    /// Whether the one check allowed after that wait ended has been used.
    last_check_used: bool,
}

#[derive(Copy, Clone, Debug)]
enum JobTarget {
    Admission(UnminedTxId),
    Revalidation(transaction::Hash),
}

#[derive(Clone, Debug)]
struct JoinTicket {
    /// The admission it joined. A later admission of the same id is another
    /// one, and this ticket does not reach it.
    admission: u64,
    source: Source,
    /// Whether the core asked this caller for the body.
    brings: bool,
}

/// A slot kept for a fetch that is still out after its id was decided.
#[derive(Clone, Debug)]
struct Lease {
    source: Source,
    tx_id: UnminedTxId,
}

/// One chain-scoped rejection list: when each entry was made, oldest first.
#[derive(Debug, Default)]
struct ChainList {
    at: HashMap<transaction::Hash, ModelTime>,
    order: VecDeque<transaction::Hash>,
}

/// The documented rules of the pool core.
#[derive(Debug)]
pub struct PoolModel {
    active: bool,
    now: ModelTime,
    applied: Generation,
    tip: Height,
    next_id: u64,
    /// Undecided transactions, oldest first.
    admissions: BTreeMap<u64, Admission>,
    admission_of: HashMap<UnminedTxId, u64>,
    owners: HashMap<ReservationId, u64>,
    /// Reservations whose id was decided through someone else's body while
    /// their own fetch was still out. Each keeps its slot until its caller
    /// submits or drops it, so a source cannot have more fetches out than
    /// slots.
    leases: HashMap<ReservationId, Lease>,
    tickets: BTreeMap<ReservationId, JoinTicket>,
    /// Joined reservations whose admission ended before their caller came
    /// back. Each keeps its id, so a wrong body under it is still told
    /// apart, as the handle would.
    ///
    /// This is the model's stand-in for the id the caller's reservation
    /// carries. The core keeps nothing for it: the handle makes that check
    /// before it sends anything. An entry lasts as long as its caller holds
    /// the reservation.
    ended_joins: HashMap<ReservationId, UnminedTxId>,
    /// The pool, oldest first.
    pool: BTreeMap<u64, Pooled>,
    pooled: HashMap<transaction::Hash, u64>,
    /// Each job, with the event count when it started.
    jobs: HashMap<JobId, (JobTarget, u64)>,
    rejected_exact: HashSet<UnminedTxId>,
    rejected_effects_tip: HashSet<transaction::Hash>,
    /// One list for each reason, as today.
    rejected_chain: HashMap<ChainReason, ChainList>,
    rejection_memory: Duration,
    cooldowns: HashMap<CooldownKey, ModelTime>,
    max_pending: usize,
    max_per_source: usize,
    max_revalidating: usize,
    /// Callers waiting for an output, by outpoint, in the order they came.
    output_waiters: HashMap<OutPoint, Vec<WaiterId>>,
    /// Changes returned in earlier steps. Each step is one batch of the log,
    /// so this is the log position of the last one.
    logged: u64,
    step: Step,
}

impl PoolModel {
    /// An active, empty pool at `tip` and `generation`.
    pub fn new(tip: Height, generation: Generation) -> Self {
        Self {
            active: true,
            now: Duration::ZERO,
            applied: generation,
            tip,
            next_id: 0,
            admissions: BTreeMap::new(),
            admission_of: HashMap::new(),
            owners: HashMap::new(),
            leases: HashMap::new(),
            tickets: BTreeMap::new(),
            ended_joins: HashMap::new(),
            pool: BTreeMap::new(),
            pooled: HashMap::new(),
            jobs: HashMap::new(),
            rejected_exact: HashSet::new(),
            rejected_effects_tip: HashSet::new(),
            rejected_chain: HashMap::new(),
            rejection_memory: REJECTION_MEMORY,
            cooldowns: HashMap::new(),
            max_pending: MAX_PENDING,
            max_per_source: MAX_PENDING_PER_SOURCE,
            max_revalidating: MAX_REVALIDATING,
            output_waiters: HashMap::new(),
            logged: 0,
            step: Step::default(),
        }
    }

    /// The same model with smaller limits, so a short random run reaches
    /// every full state. The node's limit, one source's, and revalidation's
    /// share of the node's.
    pub fn with_limits(mut self, pending: usize, per_source: usize, revalidating: usize) -> Self {
        self.max_pending = pending;
        self.max_per_source = per_source;
        self.max_revalidating = revalidating;
        self
    }

    /// The same model with the configured `eviction_memory_time`.
    pub fn with_rejection_memory(mut self, memory: Duration) -> Self {
        self.rejection_memory = memory;
        self
    }

    // Inputs.

    /// [`super::PoolHandle::reserve`].
    pub fn reserve(
        &mut self,
        tx_id: UnminedTxId,
        source: Source,
        intent: Intent,
        now: ModelTime,
    ) -> Result<ModelReservation, ModelUnreserved> {
        self.advance(now);
        if !self.active {
            return Err(ModelUnreserved::Inactive);
        }
        if let Source::Peer(peer) = &source {
            if self.cooling(peer, now) {
                return Err(ModelUnreserved::CoolingDown);
            }
        }
        if self.pooled.contains_key(&tx_id.mined_id()) {
            return Err(ModelUnreserved::Pooled);
        }
        if let Some(scope) = self.cached_rejection(tx_id, now) {
            return Err(ModelUnreserved::Rejected(scope));
        }
        if let Some(key) = self.admission_of.get(&tx_id).copied() {
            return self.join(key, source, intent);
        }
        // Today's order: the node's limit, then the source's.
        if self.slots_in_use() >= self.max_pending {
            return Err(ModelUnreserved::NoRoom(NoRoom::Node));
        }
        if matches!(source, Source::Peer(_)) && self.counted_against(&source) >= self.max_per_source
        {
            return Err(ModelUnreserved::NoRoom(NoRoom::Source));
        }
        let owner = ReservationId(self.fresh());
        let key = self.fresh();
        self.admissions.insert(
            key,
            Admission {
                tx_id,
                owner,
                owner_waiting: true,
                source,
                body: None,
                waiters: Vec::new(),
                stage: Stage::Reserved,
                missing_since: None,
                last_check_used: false,
                body_at: None,
            },
        );
        self.admission_of.insert(tx_id, key);
        self.owners.insert(owner, key);
        Ok(ModelReservation {
            id: owner,
            joined: false,
            wants_body: true,
        })
    }

    /// A reservation dropped before the core accepted it.
    pub fn release(&mut self, reservation: ReservationId, now: ModelTime) -> Step {
        self.advance(now);
        self.drop_reservation(reservation);
        self.start_revalidations();
        self.take_step()
    }

    /// [`super::PoolHandle::fetch_failed`]: the caller's fetch ended without
    /// a body. Unlike a plain drop, this is logged, because today's change
    /// stream reports a download that failed.
    pub fn fetch_failed(&mut self, reservation: ReservationId, now: ModelTime) -> Step {
        self.advance(now);
        // An id that another caller's body already decided has its outcome
        // in the log. A late failure of this fetch adds nothing to it.
        if let Some(tx_id) = self.undecided_for(reservation) {
            self.step.changes.push(Change::Refused(tx_id));
        }
        self.drop_reservation(reservation);
        self.start_revalidations();
        self.take_step()
    }

    /// [`super::PoolHandle::submit`] with a body. Returns the waiter the
    /// verdict will name, which can be in the returned step.
    pub fn submit(
        &mut self,
        reservation: ReservationId,
        tx: ModelTx,
        supplier: Supplier,
        now: ModelTime,
    ) -> (WaiterId, Step) {
        let tx_id = tx.id;
        self.arrive(reservation, tx_id, Some(tx), supplier, now)
    }

    /// [`super::PoolHandle::submit`] under a joined reservation that does
    /// not want the body: the caller waits for the verdict of the body the
    /// core has or is being brought.
    pub fn wait(
        &mut self,
        reservation: ReservationId,
        tx_id: UnminedTxId,
        supplier: Supplier,
        now: ModelTime,
    ) -> (WaiterId, Step) {
        self.arrive(reservation, tx_id, None, supplier, now)
    }

    fn arrive(
        &mut self,
        reservation: ReservationId,
        tx_id: UnminedTxId,
        body: Option<ModelTx>,
        supplier: Supplier,
        now: ModelTime,
    ) -> (WaiterId, Step) {
        self.advance(now);
        let waiter = WaiterId(self.fresh());
        // The handle checks the body against the reservation before it
        // sends anything. A wrong body drops the reservation.
        if self
            .reserved_for(reservation)
            .is_some_and(|reserved| reserved != tx_id)
        {
            self.drop_reservation(reservation);
            self.step.effects.push(Effect::Verdict {
                waiter,
                verdict: ModelVerdict::LocalFault(LocalFault::WrongTransaction),
            });
            self.start_revalidations();
            return (waiter, self.take_step());
        }
        let (key, ticket_source) = match self.tickets.remove(&reservation) {
            Some(ticket) => (Some(ticket.admission), Some(ticket.source)),
            None => (self.owners.remove(&reservation), None),
        };
        // The admission the reservation was granted for, if it is still
        // undecided. A later admission of the same id has another key.
        let key = key.filter(|key| {
            self.admissions
                .get(key)
                .is_some_and(|admission| admission.tx_id == tx_id)
        });
        let Some(key) = key else {
            // The fetch that held this slot is over.
            self.leases.remove(&reservation);
            self.ended_joins.remove(&reservation);
            let verdict = self.stale_verdict(tx_id);
            self.step.effects.push(Effect::Verdict { waiter, verdict });
            self.start_revalidations();
            return (waiter, self.take_step());
        };
        // The source was checked when it reserved. The supplier can be
        // another peer, or have entered a cooldown since.
        if matches!(&supplier, Supplier::Peer(peer) if self.cooling(peer, now)) {
            if self.admission_at(key).owner == reservation {
                self.admission_at(key).owner_waiting = false;
            }
            self.step.effects.push(Effect::Verdict {
                waiter,
                verdict: ModelVerdict::CoolingDown,
            });
            self.settle_bodyless(key);
            self.start_revalidations();
            return (waiter, self.take_step());
        }
        let start = {
            let admission = self.admission_at(key);
            if admission.owner == reservation {
                admission.owner_waiting = false;
            }
            let source = ticket_source.unwrap_or_else(|| admission.source.clone());
            admission.waiters.push(Waiter {
                id: waiter,
                source,
                supplier,
            });
            match body {
                Some(tx) if admission.body.is_none() => {
                    admission.body = Some(tx);
                    admission.body_at = Some(now);
                    true
                }
                // The core already holds these bytes, or the caller was not
                // asked for them.
                _ => false,
            }
        };
        if start {
            self.start_admission(key);
        } else {
            self.settle_bodyless(key);
        }
        (waiter, self.take_step())
    }

    /// The outcome of `job`, with the generation read before its first state
    /// lookup and after its last.
    pub fn outcome(
        &mut self,
        job: JobId,
        outcome: Outcome,
        before: GenerationRead,
        after: GenerationRead,
        now: ModelTime,
    ) -> Step {
        self.advance(now);
        let Some((target, started_at)) = self.jobs.remove(&job) else {
            // Cancelled, or timed out.
            return self.take_step();
        };
        let fresh = before.brackets(after, self.applied);
        // A stale outcome waits for the chain event only when that event is
        // certainly still to come: a reading is past what the core has
        // applied, or a change was in flight on top of exactly what it has
        // applied. In every other case the job runs again at once, so no
        // work waits for an event that has already been applied.
        //
        // A worker waits for a stable reading before its first lookup, so a
        // "changing" first reading does not happen in practice. It is
        // handled as unknown.
        use GenerationRead::{Changing, Stable};
        let chain_is_ahead = match (before, after) {
            (Stable(read), _) if read > self.applied => true,
            (_, Stable(read)) if read > self.applied => true,
            (Stable(read), Changing) => read == self.applied,
            _ => false,
        };
        match target {
            JobTarget::Admission(tx_id) => {
                let key = self.admission_of[&tx_id];
                if !fresh {
                    if chain_is_ahead {
                        self.admission_at(key).stage = Stage::AwaitingChain;
                    } else {
                        self.start_admission(key);
                    }
                } else {
                    self.admission_outcome(key, outcome, started_at);
                }
            }
            JobTarget::Revalidation(hash) => {
                let key = self.pooled[&hash];
                if !fresh {
                    if chain_is_ahead {
                        self.pooled_at(key).hidden = Some(Hidden::AwaitingChain);
                    } else {
                        self.pooled_at(key).hidden = Some(Hidden::Queued);
                    }
                } else {
                    self.revalidation_outcome(key, hash, outcome, started_at);
                }
            }
        }
        self.start_revalidations();
        self.take_step()
    }

    /// The next chain transition.
    pub fn transition(&mut self, transition: ModelTransition, now: ModelTime) -> Step {
        self.advance(now);
        if !self.active {
            return self.take_step();
        }
        self.applied = transition.generation;
        self.tip = transition.tip;
        // After a reorg or an activation the pool is verified again, and a
        // rejection made on the old chain or under the old rules says
        // nothing. Today's reset drops every list too.
        let verify_again = transition.disconnected > 0 || transition.rules_change;
        if verify_again {
            self.clear_all_rejections();
        } else {
            self.clear_tip_rejections();
        }
        // What the whole batch mined, and what in the pool it conflicts
        // with, are worked out before anything is removed. A pooled
        // transaction that conflicts with a later block transaction is then
        // found even if an earlier one already took its parent away.
        let block_txs: Vec<&ModelTx> = transition
            .connected
            .iter()
            .flat_map(|block| &block.transactions)
            .collect();
        let mined: Vec<transaction::Hash> = block_txs.iter().map(|tx| tx.id.mined_id()).collect();
        let conflicting: Vec<UnminedTxId> = self
            .pool
            .values()
            .filter(|pooled| !mined.contains(&pooled.tx.id.mined_id()))
            .filter(|pooled| block_txs.iter().any(|tx| pooled.tx.conflicts_with(tx)))
            .map(|pooled| pooled.tx.id)
            .collect();
        for block_tx in &block_txs {
            if let Some(key) = self.pooled.get(&block_tx.id.mined_id()).copied() {
                // Its dependents stay. Their parent is on the chain now.
                self.remove_one(key, RemovalReason::Mined);
            }
            self.cache(
                block_tx.id,
                RejectionScope::EffectsChain(ChainReason::Mined),
            );
        }
        for conflict in conflicting {
            self.remove_with_dependents(conflict.mined_id(), RemovalReason::Conflicted);
            self.cache(
                conflict,
                RejectionScope::EffectsChain(ChainReason::DuplicateSpend),
            );
        }
        // Every block of this transition was seen, and what they mined is
        // gone already. So an expired transaction here was not mined, and
        // what depends on it goes with it.
        self.remove_expired(true);
        if verify_again {
            self.hide_all();
            // A block that left the chain may have spent an output a parked
            // transaction is waiting for. Nothing names which, so each one
            // looks again.
            self.rerun_parked();
        }
        for hash in mined {
            self.wake(hash);
        }
        self.chain_applied();
        self.take_step()
    }

    /// The transition queue overflowed. The core no longer knows which
    /// blocks changed, so every pooled and parked transaction is checked
    /// again at this tip.
    pub fn resync(&mut self, tip: Height, generation: Generation, now: ModelTime) -> Step {
        self.advance(now);
        if !self.active {
            return self.take_step();
        }
        self.applied = generation;
        self.tip = tip;
        self.clear_all_rejections();
        // Expiry needs no verifier, only the new tip. Only the expired
        // transaction goes. A block the core missed may have mined it
        // first, and then what spends its outputs is still valid, so its
        // dependents are checked again like everything else.
        self.remove_expired(false);
        self.hide_all();
        self.rerun_parked();
        self.chain_applied();
        self.take_step()
    }

    /// Time passed. Applies every deadline at or before `now`.
    pub fn tick(&mut self, now: ModelTime) -> Step {
        self.advance(now);
        let mut timed_out = Vec::new();
        let mut unparked = Vec::new();
        for (key, admission) in &self.admissions {
            // The slot's own deadline comes first, whatever the stage.
            if admission
                .body_at
                .is_some_and(|at| at.saturating_add(MAX_UNDECIDED) <= now)
            {
                timed_out.push(*key);
                continue;
            }
            match &admission.stage {
                Stage::Verifying { deadline, .. } if *deadline <= now => timed_out.push(*key),
                Stage::Parked { .. }
                    if admission
                        .missing_since
                        .is_some_and(|since| since.saturating_add(T_PARK) <= now) =>
                {
                    unparked.push(*key)
                }
                _ => {}
            }
        }
        for key in timed_out {
            self.cancel_admission_job(key);
            self.fail_locally(key, LocalFault::VerifyTimeout);
        }
        for key in unparked {
            // Today a parent that never arrived is a missing-input failure,
            // remembered for these exact bytes until the tip changes.
            self.reject(key, RejectionScope::ExactTip, Charge::None);
        }
        let keys: Vec<u64> = self.pool.keys().copied().collect();
        for key in keys {
            // An earlier removal in this loop can have taken this one with
            // it, as a dependent.
            let Some((hidden, parked_out)) = self.pool.get(&key).map(|pooled| {
                (
                    pooled.hidden.clone(),
                    pooled
                        .missing_since
                        .is_some_and(|since| since.saturating_add(T_PARK) <= now),
                )
            }) else {
                continue;
            };
            match hidden {
                Some(Hidden::Running { job, deadline }) if deadline <= now => {
                    self.jobs.remove(&job);
                    self.step.effects.push(Effect::Cancel(job));
                    self.retry_later(key);
                }
                Some(Hidden::Retry { at }) if at <= now => {
                    self.pooled_at(key).hidden = Some(Hidden::Queued);
                }
                // Its parent never came back.
                Some(Hidden::Parked { .. }) if parked_out => {
                    let tx_id = self.pooled_at(key).tx.id;
                    self.remove_with_dependents(tx_id.mined_id(), RemovalReason::Invalidated);
                    // Remembered like a new transaction's missing parent.
                    self.cache(tx_id, RejectionScope::ExactTip);
                }
                _ => {}
            }
        }
        self.start_revalidations();
        self.take_step()
    }

    /// [`super::PoolHandle::await_output`]. The lookup and the registration
    /// are one input, so no admission can fall between them.
    pub fn await_output(&mut self, outpoint: OutPoint, now: ModelTime) -> OutputAnswer {
        self.advance(now);
        if !self.active {
            return OutputAnswer::Inactive;
        }
        if self.offers(&outpoint) {
            return OutputAnswer::Ready;
        }
        // Today's limit is on distinct outpoints. Another caller for one
        // already waited on is not refused.
        if !self.output_waiters.contains_key(&outpoint)
            && self.output_waiters.len() >= MAX_OUTPUT_WAITS
        {
            return OutputAnswer::TooManyWaiters;
        }
        let waiter = WaiterId(self.fresh());
        self.output_waiters
            .entry(outpoint)
            .or_default()
            .push(waiter);
        OutputAnswer::Waiting(waiter)
    }

    /// A caller stopped waiting for an output. Its place no longer counts
    /// toward the limit.
    pub fn cancel_output_wait(&mut self, waiter: WaiterId) {
        self.output_waiters.retain(|_, waiters| {
            waiters.retain(|waiting| *waiting != waiter);
            !waiters.is_empty()
        });
    }

    /// Distinct outpoints callers are waiting for.
    pub fn output_waits(&self) -> usize {
        self.output_waiters.len()
    }

    /// Callers waiting for an output, over every outpoint. A caller that
    /// stopped waiting is not one of them: nothing is kept for it.
    pub fn output_waiters(&self) -> usize {
        self.output_waiters.values().map(Vec::len).sum()
    }

    /// [`super::PoolHandle::report_violation`].
    pub fn violation(&mut self, peer: QueueSource, now: ModelTime) {
        self.advance(now);
        self.cool(&peer);
    }

    /// The node fell far behind the tip. Every undecided submission gets a
    /// verdict, and the pool empties.
    pub fn deactivate(&mut self, now: ModelTime) -> Step {
        self.advance(now);
        let keys: Vec<u64> = self.admissions.keys().copied().collect();
        for key in keys {
            self.cancel_admission_job(key);
            self.decide(key, ModelVerdict::LocalFault(LocalFault::Deactivated));
        }
        self.tickets.clear();
        self.owners.clear();
        let keys: Vec<u64> = self.pool.keys().copied().collect();
        for key in keys {
            self.remove_one(key, RemovalReason::Deactivated);
        }
        self.clear_all_rejections();
        // Nobody is left waiting on a pool that is off.
        let mut waiting: Vec<WaiterId> = self
            .output_waiters
            .drain()
            .flat_map(|(_, waiters)| waiters)
            .collect();
        waiting.sort();
        self.step
            .effects
            .extend(waiting.into_iter().map(Effect::OutputWaitEnded));
        self.active = false;
        self.take_step()
    }

    /// The node is near the tip again. The pool starts empty.
    pub fn activate(&mut self, tip: Height, generation: Generation, now: ModelTime) {
        self.advance(now);
        self.active = true;
        self.tip = tip;
        self.applied = generation;
    }

    // Observations.

    /// Undecided transactions, plus running revalidations.
    pub fn slots_in_use(&self) -> usize {
        self.admissions
            .len()
            .saturating_add(self.leases.len())
            .saturating_add(self.revalidating())
    }

    /// Slots counted against `source`: its undecided transactions, and its
    /// fetches that are still out for ids decided through someone else.
    pub fn counted_against(&self, source: &Source) -> usize {
        self.admissions
            .values()
            .map(|admission| &admission.source)
            .chain(self.leases.values().map(|lease| &lease.source))
            .filter(|counted| *counted == source)
            .count()
    }

    /// Reservations callers still hold, of every kind. Zero once every
    /// caller has submitted or dropped what it was granted.
    pub fn caller_held(&self) -> usize {
        self.owners
            .len()
            .saturating_add(self.tickets.len())
            .saturating_add(self.leases.len())
            .saturating_add(self.ended_joins.len())
    }

    /// Undecided transactions that hold a body.
    pub fn bodies_held(&self) -> usize {
        self.admissions
            .values()
            .filter(|admission| admission.body.is_some())
            .count()
    }

    /// The visible pool, oldest first.
    pub fn visible(&self) -> Vec<UnminedTxId> {
        self.pool
            .values()
            .filter(|pooled| pooled.hidden.is_none())
            .map(|pooled| pooled.tx.id)
            .collect()
    }

    /// The pooled transactions hidden while they are verified again.
    pub fn hidden(&self) -> Vec<UnminedTxId> {
        self.pool
            .values()
            .filter(|pooled| pooled.hidden.is_some())
            .map(|pooled| pooled.tx.id)
            .collect()
    }

    /// Whether `tx_id` is parked on a missing parent.
    pub fn is_parked(&self, tx_id: UnminedTxId) -> bool {
        self.admission_of
            .get(&tx_id)
            .and_then(|key| self.admissions.get(key))
            .is_some_and(|admission| matches!(admission.stage, Stage::Parked { .. }))
    }

    /// Whether the core refuses `peer` at `now`. A legacy peer is matched by
    /// its canonical IP address. This reads the cooldown's own end time, so
    /// it is right whichever input last moved the model's clock.
    pub fn cooling(&self, peer: &QueueSource, now: ModelTime) -> bool {
        self.cooldowns
            .get(&CooldownKey::from(peer))
            .is_some_and(|until| *until > now)
    }

    /// [`super::PoolHandle::rejected_ids`]: the members of `ids` a cached
    /// rejection covers at `now`. Takes no slot.
    pub fn rejected_ids(&self, ids: &HashSet<UnminedTxId>, now: ModelTime) -> HashSet<UnminedTxId> {
        ids.iter()
            .copied()
            .filter(|id| self.cached_rejection(*id, now).is_some())
            .collect()
    }

    /// Checks the invariants every reachable state keeps.
    ///
    /// # Panics
    ///
    /// If one is broken.
    pub fn assert_invariants(&self) {
        assert!(
            self.slots_in_use() <= self.max_pending,
            "pool/admissions-within-limits: the node's limit"
        );
        let mut sources: HashMap<&Source, usize> = HashMap::new();
        for lease in self.leases.values() {
            *sources.entry(&lease.source).or_default() += 1;
        }
        for admission in self.admissions.values() {
            *sources.entry(&admission.source).or_default() += 1;
            assert!(
                admission.waiters.len() <= MAX_WAITERS,
                "the waiter limit of one admission"
            );
            assert_eq!(
                admission.body.is_none(),
                matches!(admission.stage, Stage::Reserved),
                "an admission has a body exactly when it has left the reserved stage"
            );
        }
        for (source, count) in sources {
            if matches!(source, Source::Peer(_)) {
                assert!(
                    count <= self.max_per_source,
                    "pool/admissions-within-limits: one source's limit"
                );
            }
        }
        assert!(
            self.revalidating() <= self.max_revalidating,
            "the revalidation share"
        );
        assert!(
            self.rejected_exact.len() <= MAX_REJECTIONS
                && self.rejected_effects_tip.len() <= MAX_REJECTIONS
                && self
                    .rejected_chain
                    .values()
                    .all(|list| list.at.len() <= MAX_REJECTIONS),
            "the rejection lists' limits"
        );
        assert!(self.cooldowns.len() <= MAX_COOLDOWNS, "the cooldown limit");
        for pooled in self.pool.values().filter(|pooled| pooled.hidden.is_none()) {
            let seed: HashSet<transaction::Hash> = [pooled.tx.id.mined_id()].into();
            assert!(
                self.dependency_group(&seed).len() <= MAX_PACKAGE,
                "pool/consistent: no visible dependency group past its limit"
            );
        }
        for ticket in self.tickets.values() {
            assert!(
                self.admissions.contains_key(&ticket.admission),
                "a joined reservation names a live admission"
            );
        }
        let entries: Vec<&Pooled> = self.pool.values().collect();
        for (index, pooled) in entries.iter().enumerate() {
            assert!(
                pooled.tx.expiry.is_none_or(|expiry| self.tip < expiry),
                "pool/consistent: no expired transaction"
            );
            for other in &entries[index.saturating_add(1)..] {
                assert!(
                    !pooled.tx.conflicts_with(&other.tx),
                    "pool/consistent: no two pooled transactions conflict"
                );
            }
        }
    }

    // Rules.

    /// The id a reservation was granted for, while its admission is
    /// undecided.
    fn undecided_for(&self, reservation: ReservationId) -> Option<UnminedTxId> {
        let key = self
            .tickets
            .get(&reservation)
            .map(|ticket| ticket.admission)
            .or_else(|| self.owners.get(&reservation).copied())?;
        self.admissions.get(&key).map(|admission| admission.tx_id)
    }

    /// The id a reservation was granted for, for as long as the model knows
    /// the reservation: while its admission is undecided, and while it
    /// holds a lease after that.
    fn reserved_for(&self, reservation: ReservationId) -> Option<UnminedTxId> {
        self.undecided_for(reservation)
            .or_else(|| self.leases.get(&reservation).map(|lease| lease.tx_id))
            .or_else(|| self.ended_joins.get(&reservation).copied())
    }

    /// Ends a reservation that brought no body: frees its lease, or its
    /// place in an admission, and ends that admission if nobody can still
    /// bring its body.
    fn drop_reservation(&mut self, reservation: ReservationId) {
        let key = match self.tickets.remove(&reservation) {
            Some(ticket) => Some(ticket.admission),
            None => self.owners.remove(&reservation).inspect(|key| {
                if let Some(admission) = self.admissions.get_mut(key) {
                    admission.owner_waiting = false;
                }
            }),
        };
        if let Some(key) = key {
            self.settle_bodyless(key);
        }
        self.leases.remove(&reservation);
        self.ended_joins.remove(&reservation);
    }

    /// Joins an id that is already undecided, if `source` may.
    fn join(
        &mut self,
        key: u64,
        source: Source,
        intent: Intent,
    ) -> Result<ModelReservation, ModelUnreserved> {
        if intent == Intent::Fetch {
            return Err(ModelUnreserved::Pending);
        }
        let tickets: Vec<&JoinTicket> = self
            .tickets
            .values()
            .filter(|ticket| ticket.admission == key)
            .collect();
        let admission = &self.admissions[&key];
        let waiting = admission
            .waiters
            .len()
            .saturating_add(tickets.len())
            .saturating_add(usize::from(admission.owner_waiting));
        // A peer source waits on an id once. The reserving source counts
        // while its body can still arrive.
        let already_on_it = matches!(source, Source::Peer(_))
            && ((admission.owner_waiting && admission.source == source)
                || admission
                    .waiters
                    .iter()
                    .any(|waiter| waiter.source == source)
                || tickets.iter().any(|ticket| ticket.source == source));
        if waiting >= MAX_WAITERS || already_on_it {
            return Err(ModelUnreserved::Pending);
        }
        // One joined caller is asked for the body, and only while the core
        // has none. Every other one waits for that body's verdict.
        let brings = admission.body.is_none() && !tickets.iter().any(|ticket| ticket.brings);
        let id = ReservationId(self.fresh());
        self.tickets.insert(
            id,
            JoinTicket {
                admission: key,
                source,
                brings,
            },
        );
        Ok(ModelReservation {
            id,
            joined: true,
            wants_body: brings,
        })
    }

    fn admission_outcome(&mut self, key: u64, outcome: Outcome, started_at: u64) {
        let admission = self.admission_at(key).clone();
        let tx = admission.body.expect("a verified admission holds its body");
        let hash = tx.id.mined_id();
        match outcome {
            Outcome::Pass | Outcome::PassUsing(_) => {
                let pool_inputs = match outcome {
                    Outcome::PassUsing(inputs) => inputs,
                    _ => Vec::new(),
                };
                // The tip has reached its expiry height. The verifier should
                // have said so. The pool does not rely on that: nothing
                // expired gets in.
                if tx.expiry.is_some_and(|expiry| self.tip >= expiry) {
                    self.reject(
                        key,
                        RejectionScope::EffectsChain(ChainReason::Expired),
                        Charge::None,
                    );
                    return;
                }
                // Today's insertion checks, in today's order. First, another
                // set of authorizing data for the same effects got in while
                // this one was verified. That is a duplicate, not a
                // conflict, and nothing is cached against the transaction
                // the pool holds.
                if self.pooled.contains_key(&hash) {
                    // Today's change stream reports an insertion the pool
                    // refused, this one included.
                    self.step.changes.push(Change::Refused(tx.id));
                    self.decide(
                        key,
                        ModelVerdict::Rejected {
                            scope: RejectionScope::Uncached,
                            charge: Charge::None,
                        },
                    );
                    return;
                }
                if self
                    .pool
                    .values()
                    .any(|pooled| pooled.tx.conflicts_with(&tx))
                {
                    // The pool took a conflicting transaction while this one
                    // was verified.
                    self.reject(key, RejectionScope::EffectsTip, Charge::None);
                    return;
                }
                // The verifier used a pool output that the pool no longer
                // offers. The generation readings cannot show this: the
                // state was stable, and the pool the verifier read was older
                // than the core's. Today this is a rejection. Here the job
                // runs again, because the parent may have been mined, and
                // the child is then valid against the chain.
                if !pool_inputs.iter().all(|input| self.offers(input)) {
                    self.start_admission(key);
                    return;
                }
                let parents: HashSet<transaction::Hash> =
                    pool_inputs.iter().map(|input| input.hash).collect();
                let ancestors = self.ancestors(parents);
                if ancestors.len() > MAX_ANCESTORS
                    || self.dependency_group(&ancestors).len() >= MAX_PACKAGE
                {
                    self.reject(key, RejectionScope::EffectsTip, Charge::None);
                    return;
                }
                let pool_key = self.fresh();
                self.step.changes.push(Change::Added(tx.id));
                self.offer_outputs(hash, tx.outputs);
                self.pool.insert(
                    pool_key,
                    Pooled {
                        tx,
                        hidden: None,
                        backoff: REVALIDATE_RETRY,
                        visible_at: pool_key,
                        hidden_at: ChangeSeq(0),
                        missing_since: None,
                        last_check_used: false,
                    },
                );
                self.pooled.insert(hash, pool_key);
                self.decide(key, ModelVerdict::Admitted);
                self.wake(hash);
            }
            // Found in the best chain by this job's own lookup. Today that
            // leaves the rejection lists alone, so a second submission gets
            // the same answer at the same stage. Only a block the core sees
            // connect fills the mined list.
            Outcome::AlreadyMined => self.reject(key, RejectionScope::Uncached, Charge::None),
            Outcome::Fail { scope, charge } => self.reject(key, scope, charge),
            Outcome::MissingInputs(missing) => {
                // One of the missing outputs became available after this job
                // started looking: its transaction is visible now, creates
                // that output, and became visible after the job began. Its
                // admission has already been announced, so nothing would
                // wake this transaction. It runs again at once.
                //
                // A parent that was visible all along, or that has no such
                // output, is not a reason to run again: the answer would be
                // the same.
                let arrived = missing
                    .iter()
                    .any(|outpoint| self.arrived_since(outpoint, started_at));
                // Its wait runs from the first time an input was missing,
                // and is never restarted.
                let now = self.now;
                let admission = self.admission_at(key);
                let since = *admission.missing_since.get_or_insert(now);
                let wait_over = since.saturating_add(T_PARK) <= now;
                // After the wait, an input that arrived during this check
                // earns one more check and no more. Otherwise inputs that
                // keep arriving one at a time could hold the slot for as
                // long as they kept coming.
                let last_check = wait_over && arrived && !admission.last_check_used;
                if last_check {
                    admission.last_check_used = true;
                }
                if (arrived && !wait_over) || last_check {
                    self.start_admission(key);
                    return;
                }
                if wait_over {
                    // Today a parent that never arrived is a missing-input
                    // failure, remembered for these exact bytes until the
                    // tip changes.
                    self.reject(key, RejectionScope::ExactTip, Charge::None);
                    return;
                }
                let mut on: Vec<transaction::Hash> = Vec::new();
                for outpoint in missing {
                    if !on.contains(&outpoint.hash) {
                        on.push(outpoint.hash);
                    }
                }
                self.admission_at(key).stage = Stage::Parked { on };
            }
            Outcome::LocalFault(fault) => self.fail_locally(key, fault),
        }
    }

    fn revalidation_outcome(
        &mut self,
        key: u64,
        hash: transaction::Hash,
        outcome: Outcome,
        started_at: u64,
    ) {
        match outcome {
            Outcome::Pass | Outcome::PassUsing(_) => {
                let pool_inputs = match outcome {
                    Outcome::PassUsing(inputs) => inputs,
                    _ => Vec::new(),
                };
                if !pool_inputs.iter().all(|input| self.offers(input)) {
                    // As for a new transaction: verified against a pool that
                    // has since lost that output. It runs again.
                    self.pooled_at(key).hidden = Some(Hidden::Queued);
                    return;
                }
                // Nothing expired becomes visible either.
                let expired = self.pool[&key]
                    .tx
                    .expiry
                    .is_some_and(|expiry| self.tip >= expiry);
                if expired {
                    let tx_id = self.pool[&key].tx.id;
                    self.pooled_at(key).hidden = Some(Hidden::Queued);
                    self.remove_with_dependents(hash, RemovalReason::Expired);
                    self.cache(tx_id, RejectionScope::EffectsChain(ChainReason::Expired));
                    return;
                }
                // Today's insertion limits, as for a new transaction. A
                // parent that was mined, then put back by a reorg, can have
                // gained more pooled children than one group holds.
                let parents: HashSet<transaction::Hash> =
                    pool_inputs.iter().map(|input| input.hash).collect();
                let ancestors = self.ancestors(parents);
                if ancestors.len() > MAX_ANCESTORS
                    || self.dependency_group(&ancestors).len() >= MAX_PACKAGE
                {
                    let tx_id = self.pool[&key].tx.id;
                    self.pooled_at(key).hidden = Some(Hidden::Queued);
                    self.remove_with_dependents(hash, RemovalReason::Invalidated);
                    // Remembered until the tip changes, as an insertion the
                    // pool refuses for this reason is today.
                    self.cache(tx_id, RejectionScope::EffectsTip);
                    return;
                }
                let visible_at = self.fresh();
                let pooled = self.pooled_at(key);
                pooled.hidden = None;
                pooled.backoff = REVALIDATE_RETRY;
                pooled.visible_at = visible_at;
                pooled.missing_since = None;
                pooled.last_check_used = false;
                let (id, hidden_at, count) = (pooled.tx.id, pooled.hidden_at, pooled.tx.outputs);
                self.step.changes.push(Change::Revealed { id, hidden_at });
                // Its outputs are available again, to waiters as well.
                self.offer_outputs(hash, count);
                self.wake(hash);
            }
            // A block the core never saw mined it. Its dependents stay and
            // are checked against the chain when their turn comes.
            Outcome::AlreadyMined => {
                // Its job has reported, so there is nothing to cancel.
                self.pooled_at(key).hidden = Some(Hidden::Queued);
                self.remove_one(key, RemovalReason::Mined);
                // Found by a lookup, not in a block the core saw connect, so
                // it is not cached. A later submission is looked up again.
                self.wake(hash);
            }
            // Its parent is in neither the chain nor the visible pool. After
            // a reorg that can be a mined parent that is on its way back, so
            // it waits as a new transaction would, still hidden, and by the
            // same rules.
            Outcome::MissingInputs(missing) => {
                let arrived = missing
                    .iter()
                    .any(|outpoint| self.arrived_since(outpoint, started_at));
                let now = self.now;
                let pooled = self.pooled_at(key);
                let since = *pooled.missing_since.get_or_insert(now);
                let wait_over = since.saturating_add(T_PARK) <= now;
                let last_check = wait_over && arrived && !pooled.last_check_used;
                if last_check {
                    pooled.last_check_used = true;
                }
                if (arrived && !wait_over) || last_check {
                    self.pooled_at(key).hidden = Some(Hidden::Queued);
                    return;
                }
                if wait_over {
                    let tx_id = self.pooled_at(key).tx.id;
                    // Its job has reported, so there is nothing to cancel.
                    self.pooled_at(key).hidden = Some(Hidden::Queued);
                    self.remove_with_dependents(hash, RemovalReason::Invalidated);
                    self.cache(tx_id, RejectionScope::ExactTip);
                    return;
                }
                let mut on: Vec<transaction::Hash> = Vec::new();
                for outpoint in missing {
                    if !on.contains(&outpoint.hash) {
                        on.push(outpoint.hash);
                    }
                }
                self.pooled_at(key).hidden = Some(Hidden::Parked { on });
            }
            Outcome::Fail { scope, .. } => {
                let tx_id = self.pooled_at(key).tx.id;
                // Its job has reported, so there is nothing to cancel.
                self.pooled_at(key).hidden = Some(Hidden::Queued);
                self.remove_with_dependents(hash, RemovalReason::Invalidated);
                // Nobody is charged for a transaction that passed once. The
                // rejection is still remembered, as today's retry after a
                // reset remembers it, so it is not fetched again at once.
                self.cache(tx_id, scope);
            }
            Outcome::LocalFault(_) => self.retry_later(key),
        }
    }

    /// Rejects an admission: caches the rejection, logs it, and answers
    /// every waiter.
    fn reject(&mut self, key: u64, scope: RejectionScope, charge: Charge) {
        let tx_id = self.admission_at(key).tx_id;
        self.cache(tx_id, scope);
        self.step.changes.push(Change::Refused(tx_id));
        self.decide(key, ModelVerdict::Rejected { scope, charge });
    }

    /// Ends an admission: every waiter gets the verdict, and the slot is
    /// free. A charge reaches only a waiter with a peer supplier.
    fn decide(&mut self, key: u64, verdict: ModelVerdict) {
        let admission = self
            .admissions
            .remove(&key)
            .expect("the caller names a live admission");
        self.admission_of.remove(&admission.tx_id);
        self.owners.remove(&admission.owner);
        if admission.owner_waiting {
            // The reserving caller's fetch is still out. Its body will
            // arrive decoded, so its slot is not free yet.
            self.leases.insert(
                admission.owner,
                Lease {
                    source: admission.source.clone(),
                    tx_id: admission.tx_id,
                },
            );
        }
        // A reservation that joined this admission dies with it. Its caller
        // may still come back with a body.
        let tx_id = admission.tx_id;
        let ended: Vec<ReservationId> = self
            .tickets
            .iter()
            .filter(|(_, ticket)| ticket.admission == key)
            .map(|(reservation, _)| *reservation)
            .collect();
        for reservation in ended {
            self.tickets.remove(&reservation);
            self.ended_joins.insert(reservation, tx_id);
        }
        for waiter in admission.waiters {
            let verdict = match (verdict, &waiter.supplier) {
                (ModelVerdict::Rejected { scope, charge }, Supplier::Peer(peer)) => {
                    if charge == Charge::Cooldown {
                        self.cool(peer);
                    }
                    ModelVerdict::Rejected { scope, charge }
                }
                (ModelVerdict::Rejected { scope, .. }, Supplier::Nobody) => {
                    ModelVerdict::Rejected {
                        scope,
                        charge: Charge::None,
                    }
                }
                (other, _) => other,
            };
            self.step.effects.push(Effect::Verdict {
                waiter: waiter.id,
                verdict,
            });
        }
    }

    /// The answer to a body whose reservation no longer names an admission.
    fn stale_verdict(&self, tx_id: UnminedTxId) -> ModelVerdict {
        if self.pooled.contains_key(&tx_id.mined_id()) {
            return ModelVerdict::Rejected {
                scope: RejectionScope::Uncached,
                charge: Charge::None,
            };
        }
        if let Some(scope) = self.cached_rejection(tx_id, self.now) {
            return ModelVerdict::Rejected {
                scope,
                charge: Charge::None,
            };
        }
        ModelVerdict::LocalFault(LocalFault::StaleReservation)
    }

    fn start_admission(&mut self, key: u64) {
        // No new check starts once the slot's time is up.
        if self.undecided_too_long(key) {
            self.fail_locally(key, LocalFault::VerifyTimeout);
            return;
        }
        let job = JobId(self.fresh());
        let deadline = self.now.saturating_add(T_VERIFY);
        let admission = self.admission_at(key);
        admission.stage = Stage::Verifying { job, deadline };
        let tx_id = admission.tx_id;
        self.jobs.insert(job, (JobTarget::Admission(tx_id), job.0));
        self.step.effects.push(Effect::Start {
            job,
            tx: tx_id,
            revalidation: false,
        });
    }

    /// Whether an admission has held its slot for [`MAX_UNDECIDED`] since
    /// its body arrived.
    fn undecided_too_long(&self, key: u64) -> bool {
        self.admissions[&key]
            .body_at
            .is_some_and(|at| at.saturating_add(MAX_UNDECIDED) <= self.now)
    }

    /// Cancels the job of an admission that is being verified.
    fn cancel_admission_job(&mut self, key: u64) {
        let job = match &self.admission_at(key).stage {
            Stage::Verifying { job, .. } => *job,
            _ => return,
        };
        self.jobs.remove(&job);
        self.step.effects.push(Effect::Cancel(job));
    }

    /// Runs every admission parked on `parent` again.
    fn wake(&mut self, parent: transaction::Hash) {
        let woken: Vec<u64> = self
            .admissions
            .iter()
            .filter(|(_, admission)| match &admission.stage {
                Stage::Parked { on, .. } => on.contains(&parent),
                _ => false,
            })
            .map(|(key, _)| *key)
            .collect();
        for key in woken {
            self.start_admission(key);
        }
        for pooled in self.pool.values_mut() {
            if matches!(&pooled.hidden, Some(Hidden::Parked { on }) if on.contains(&parent)) {
                pooled.hidden = Some(Hidden::Queued);
            }
        }
    }

    /// The outputs of a transaction that just became visible are available.
    /// Every caller waiting for one of them is answered, in the order the
    /// callers came, in this step.
    fn offer_outputs(&mut self, tx: transaction::Hash, count: u32) {
        self.step.effects.push(Effect::Outputs { tx, count });
        for index in 0..count {
            let outpoint = OutPoint { hash: tx, index };
            for waiter in self.output_waiters.remove(&outpoint).unwrap_or_default() {
                self.step
                    .effects
                    .push(Effect::OutputReady { waiter, outpoint });
            }
        }
    }

    /// Removes every pooled transaction whose expiry height the tip has
    /// reached, and remembers each as expired. With `with_dependents`, what
    /// depends on it goes too.
    fn remove_expired(&mut self, with_dependents: bool) {
        let expired: Vec<(UnminedTxId, bool)> = self
            .pool
            .values()
            .filter(|pooled| pooled.tx.expiry.is_some_and(|expiry| self.tip >= expiry))
            .map(|pooled| (pooled.tx.id, pooled.hidden.is_none()))
            .collect();
        for (tx_id, visible) in expired {
            // A hidden transaction has not been checked against the chain
            // since a reorg or a missed event. It may have been mined in a
            // block the core did not see, and then what spends its outputs
            // is still valid. So only a visible one takes its dependents
            // with it. A hidden one's dependents are hidden too, and are
            // checked when their turn comes.
            if with_dependents && visible {
                self.remove_with_dependents(tx_id.mined_id(), RemovalReason::Expired);
            } else if let Some(key) = self.pooled.get(&tx_id.mined_id()).copied() {
                self.remove_one(key, RemovalReason::Expired);
            }
            self.cache(tx_id, RejectionScope::EffectsChain(ChainReason::Expired));
        }
    }

    /// Runs every parked admission again.
    fn rerun_parked(&mut self) {
        let parked: Vec<u64> = self
            .admissions
            .iter()
            .filter(|(_, admission)| matches!(admission.stage, Stage::Parked { .. }))
            .map(|(key, _)| *key)
            .collect();
        for key in parked {
            self.start_admission(key);
        }
    }

    /// Ends an admission for a failure on this node. Today's change stream
    /// reports a verification that failed or timed out, so this is logged
    /// too. Nothing is cached and nobody is charged.
    fn fail_locally(&mut self, key: u64, fault: LocalFault) {
        let tx_id = self.admission_at(key).tx_id;
        self.step.changes.push(Change::Refused(tx_id));
        self.decide(key, ModelVerdict::LocalFault(fault));
    }

    /// Hides every visible pooled transaction, to be verified again.
    fn hide_all(&mut self) {
        for pooled in self.pool.values_mut() {
            match pooled.hidden {
                None => {
                    let pending =
                        u64::try_from(self.step.changes.len()).expect("a batch fits in u64");
                    pooled.hidden = Some(Hidden::Queued);
                    pooled.hidden_at =
                        ChangeSeq(self.logged.saturating_add(pending).saturating_add(1));
                    self.step.changes.push(Change::Hidden(pooled.tx.id));
                }
                // A running job read the old chain, so its outcome will be
                // stale and it will run again.
                Some(Hidden::Running { .. }) => {}
                Some(_) => pooled.hidden = Some(Hidden::Queued),
            }
        }
    }

    /// The model has applied a chain event: everything that waited for it
    /// runs again.
    fn chain_applied(&mut self) {
        let waiting: Vec<u64> = self
            .admissions
            .iter()
            .filter(|(_, admission)| matches!(admission.stage, Stage::AwaitingChain))
            .map(|(key, _)| *key)
            .collect();
        for key in waiting {
            self.start_admission(key);
        }
        for pooled in self.pool.values_mut() {
            if matches!(pooled.hidden, Some(Hidden::AwaitingChain)) {
                pooled.hidden = Some(Hidden::Queued);
            }
        }
        self.start_revalidations();
    }

    /// Starts queued revalidations, parents first, while the revalidation
    /// share and the node's limit allow.
    fn start_revalidations(&mut self) {
        let queued: Vec<u64> = self
            .pool
            .iter()
            .filter(|(_, pooled)| matches!(pooled.hidden, Some(Hidden::Queued)))
            .map(|(key, _)| *key)
            .collect();
        for key in queued {
            if self.revalidating() >= self.max_revalidating
                || self.slots_in_use() >= self.max_pending
            {
                break;
            }
            let tx = self.pool[&key].tx.clone();
            let parent_hidden = tx.parents().any(|parent| {
                self.pooled
                    .get(&parent)
                    .is_some_and(|parent| self.pool[parent].hidden.is_some())
            });
            if parent_hidden {
                continue;
            }
            let job = JobId(self.fresh());
            let deadline = self.now.saturating_add(T_VERIFY);
            self.pooled_at(key).hidden = Some(Hidden::Running { job, deadline });
            self.jobs
                .insert(job, (JobTarget::Revalidation(tx.id.mined_id()), job.0));
            self.step.effects.push(Effect::Start {
                job,
                tx: tx.id,
                revalidation: true,
            });
        }
    }

    /// A revalidation hit a local fault. The transaction stays hidden and is
    /// tried again later.
    fn retry_later(&mut self, key: u64) {
        let at = self.now;
        let pooled = self.pooled_at(key);
        let backoff = pooled.backoff;
        pooled.hidden = Some(Hidden::Retry {
            at: at.saturating_add(backoff),
        });
        pooled.backoff = backoff.saturating_mul(2).min(REVALIDATE_RETRY_MAX);
    }

    /// Removes one pooled transaction and logs it.
    fn remove_one(&mut self, key: u64, reason: RemovalReason) {
        let Some(pooled) = self.pool.remove(&key) else {
            return;
        };
        self.pooled.remove(&pooled.tx.id.mined_id());
        if let Some(Hidden::Running { job, .. }) = pooled.hidden {
            self.jobs.remove(&job);
            self.step.effects.push(Effect::Cancel(job));
        }
        self.step.changes.push(Change::Removed {
            id: pooled.tx.id,
            reason,
        });
    }

    /// Removes a pooled transaction and everything in the pool that spends
    /// its outputs. The dependents leave as invalidated.
    fn remove_with_dependents(&mut self, hash: transaction::Hash, reason: RemovalReason) {
        let Some(key) = self.pooled.get(&hash).copied() else {
            return;
        };
        self.remove_one(key, reason);
        let dependents: Vec<transaction::Hash> = self
            .pool
            .values()
            .filter(|pooled| pooled.tx.parents().any(|parent| parent == hash))
            .map(|pooled| pooled.tx.id.mined_id())
            .collect();
        for dependent in dependents {
            self.remove_with_dependents(dependent, RemovalReason::Invalidated);
        }
    }

    /// Ends an admission that holds no body once nobody can still bring one:
    /// its owner submitted nothing and released, and no joined caller was
    /// asked for the body. Callers already waiting on it get a local fault.
    fn settle_bodyless(&mut self, key: u64) {
        let Some(admission) = self.admissions.get(&key) else {
            return;
        };
        let body_coming = admission.body.is_some()
            || admission.owner_waiting
            || self
                .tickets
                .values()
                .any(|ticket| ticket.admission == key && ticket.brings);
        if !body_coming {
            self.decide(key, ModelVerdict::LocalFault(LocalFault::NoBody));
        }
    }

    /// Moves time to `now`. A cooldown or a remembered rejection ends at its
    /// time, whichever input comes next.
    fn advance(&mut self, now: ModelTime) {
        if now <= self.now {
            return;
        }
        self.now = now;
        self.cooldowns.retain(|_, until| *until > now);
        let memory = self.rejection_memory;
        for list in self.rejected_chain.values_mut() {
            while let Some(oldest) = list.order.front().copied() {
                let forgotten = list
                    .at
                    .get(&oldest)
                    .is_none_or(|at| at.saturating_add(memory) <= now);
                if !forgotten {
                    break;
                }
                list.order.pop_front();
                list.at.remove(&oldest);
            }
        }
    }

    fn clear_tip_rejections(&mut self) {
        self.rejected_exact.clear();
        self.rejected_effects_tip.clear();
    }

    fn clear_all_rejections(&mut self) {
        self.clear_tip_rejections();
        self.rejected_chain.clear();
    }

    /// Remembers a rejection in the list its scope names. A tip-scoped list
    /// that grows past its limit is cleared. A chain-scoped list drops its
    /// oldest entry.
    fn cache(&mut self, tx_id: UnminedTxId, scope: RejectionScope) {
        match scope {
            RejectionScope::ExactTip => {
                self.rejected_exact.insert(tx_id);
                if self.rejected_exact.len() > MAX_REJECTIONS {
                    self.rejected_exact.clear();
                }
            }
            RejectionScope::EffectsTip => {
                self.rejected_effects_tip.insert(tx_id.mined_id());
                if self.rejected_effects_tip.len() > MAX_REJECTIONS {
                    self.rejected_effects_tip.clear();
                }
            }
            RejectionScope::EffectsChain(reason) => {
                let hash = tx_id.mined_id();
                let now = self.now;
                let list = self.rejected_chain.entry(reason).or_default();
                // An id already in the list keeps its first entry, so its
                // time and its place in the order stay in step. Today's list
                // is never given an id twice.
                if let std::collections::hash_map::Entry::Vacant(entry) = list.at.entry(hash) {
                    entry.insert(now);
                    list.order.push_back(hash);
                }
                if list.at.len() > MAX_REJECTIONS {
                    if let Some(oldest) = list.order.pop_front() {
                        list.at.remove(&oldest);
                    }
                }
            }
            RejectionScope::Uncached => {}
        }
    }

    fn cached_rejection(&self, tx_id: UnminedTxId, now: ModelTime) -> Option<RejectionScope> {
        let hash = tx_id.mined_id();
        if self.rejected_exact.contains(&tx_id) {
            return Some(RejectionScope::ExactTip);
        }
        if self.rejected_effects_tip.contains(&hash) {
            return Some(RejectionScope::EffectsTip);
        }
        [
            ChainReason::Expired,
            ChainReason::DuplicateSpend,
            ChainReason::Mined,
        ]
        .into_iter()
        .find(|reason| {
            self.rejected_chain
                .get(reason)
                .and_then(|list| list.at.get(&hash))
                .is_some_and(|at| at.saturating_add(self.rejection_memory) > now)
        })
        .map(RejectionScope::EffectsChain)
    }

    /// Starts a peer's cooldown. A new peer past the limit replaces the one
    /// whose cooldown ends first.
    fn cool(&mut self, peer: &QueueSource) {
        let key = CooldownKey::from(peer);
        if !self.cooldowns.contains_key(&key) && self.cooldowns.len() >= MAX_COOLDOWNS {
            let earliest = self
                .cooldowns
                .iter()
                .min_by_key(|(key, until)| (**until, (*key).clone()))
                .map(|(key, _)| key.clone());
            if let Some(earliest) = earliest {
                self.cooldowns.remove(&earliest);
            }
        }
        self.cooldowns
            .insert(key, self.now.saturating_add(COOLDOWN));
    }

    /// Whether `outpoint` became available after the event count
    /// `started_at`: its transaction is visible now, creates that output,
    /// and became visible after that count.
    fn arrived_since(&self, outpoint: &OutPoint, started_at: u64) -> bool {
        self.offers(outpoint)
            && self
                .pooled
                .get(&outpoint.hash)
                .is_some_and(|key| self.pool[key].visible_at > started_at)
    }

    /// Whether the visible pool offers `outpoint`: its transaction is
    /// visible and creates that output.
    fn offers(&self, outpoint: &OutPoint) -> bool {
        self.pooled.get(&outpoint.hash).is_some_and(|key| {
            let pooled = &self.pool[key];
            pooled.hidden.is_none() && outpoint.index < pooled.tx.outputs
        })
    }

    /// Whether `hash` is pooled and visible. The dependency limits are
    /// about the visible pool, as today's are about the verified set.
    fn is_visible(&self, hash: &transaction::Hash) -> bool {
        self.pooled
            .get(hash)
            .is_some_and(|key| self.pool[key].hidden.is_none())
    }

    /// The visible parents of a pooled transaction.
    fn pooled_parents(&self, hash: &transaction::Hash) -> Vec<transaction::Hash> {
        self.pooled
            .get(hash)
            .map(|key| {
                self.pool[key]
                    .tx
                    .parents()
                    .filter(|parent| self.is_visible(parent))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// `parents` and every pooled transaction they depend on.
    fn ancestors(&self, parents: HashSet<transaction::Hash>) -> HashSet<transaction::Hash> {
        let mut ancestors = HashSet::new();
        let mut next: Vec<transaction::Hash> = parents.into_iter().collect();
        while let Some(hash) = next.pop() {
            if self.is_visible(&hash) && ancestors.insert(hash) {
                next.extend(self.pooled_parents(&hash));
            }
        }
        ancestors
    }

    /// Every pooled transaction connected to `seeds` through parents,
    /// children or both.
    fn dependency_group(&self, seeds: &HashSet<transaction::Hash>) -> HashSet<transaction::Hash> {
        let mut group = HashSet::new();
        let mut next: Vec<transaction::Hash> = seeds.iter().copied().collect();
        while let Some(hash) = next.pop() {
            if !self.is_visible(&hash) || !group.insert(hash) {
                continue;
            }
            next.extend(self.pooled_parents(&hash));
            next.extend(
                self.pool
                    .values()
                    .filter(|pooled| pooled.hidden.is_none())
                    .filter(|pooled| pooled.tx.parents().any(|parent| parent == hash))
                    .map(|pooled| pooled.tx.id.mined_id()),
            );
        }
        group
    }

    fn revalidating(&self) -> usize {
        self.pool
            .values()
            .filter(|pooled| matches!(pooled.hidden, Some(Hidden::Running { .. })))
            .count()
    }

    fn admission_at(&mut self, key: u64) -> &mut Admission {
        self.admissions
            .get_mut(&key)
            .expect("the caller names a live admission")
    }

    fn pooled_at(&mut self, key: u64) -> &mut Pooled {
        self.pool
            .get_mut(&key)
            .expect("the caller names a pooled transaction")
    }

    fn fresh(&mut self) -> u64 {
        self.next_id = self.next_id.saturating_add(1);
        self.next_id
    }

    fn take_step(&mut self) -> Step {
        let step = std::mem::take(&mut self.step);
        let changes = u64::try_from(step.changes.len()).expect("a batch fits in u64");
        self.logged = self.logged.saturating_add(changes);
        step
    }
}
