//! The pool core's state machine.
//!
//! [`MempoolState`] holds the node's unmined transactions and decides every
//! submission. It is pure. Each input carries the time and returns what it
//! caused as a [`Step`]. It runs no verification and touches no channel.
//! `MempoolIO` runs it behind the port in
//! `zakura_node_services::mempool::port`.
//!
//! Its rules are the port's, in `zakura_node_services::mempool::port`. Two
//! more belong to storage: eviction at the cost limit and mining order.
//!
//! It knows nothing about peers. Per-peer limits and cooldowns belong to the
//! callers that see the peer. A verdict says what its body earned, and the
//! caller applies that to whoever sent it.
//!
//! [`Storage`] holds the pool. After a reset, every pooled transaction
//! leaves it and comes back through admission, like a new one, a share of
//! the slots at a time.
//!
//! The pool is on while the node is near a tip the chain has named. The
//! machine is told both, by [`MempoolState::near_tip`] and the chain inputs,
//! and turns itself on and off.
//!
//! Every input enters the same way: it moves the clock and applies every
//! deadline that is due. Every input leaves the same way too, through
//! [`MempoolState::take_step`], which starts the queued re-admissions that
//! can start. An input that answers its caller, such as
//! [`MempoolState::reserve`], returns the answer with its step.

use std::{collections::BTreeMap, sync::Arc};

use chrono::{DateTime, Utc};
use zakura_chain::{
    block::{self, Height},
    ironwood, orchard, sapling, sprout,
    transaction::{self, Transaction, UnminedTx, UnminedTxId, VerifiedUnminedTx},
    transparent::OutPoint,
};
use zakura_node_services::mempool::port::{
    ChainReason, ChainTip, Change, ChangeSeq, Charge, ConnectedBlock, LocalFault, PoolSnapshot,
    Rejection, RejectionDetail, RejectionScope, RemovalReason, ReservationId, Unreserved, Verdict,
    MAX_PENDING, MAX_READMITTING, T_PARK, T_VERIFY,
};
use zakura_rt::collections::{HashMap, HashSet};

use crate::{
    config::Config,
    storage::{
        ExactTipRejectionError, RejectionError, SameEffectsChainRejectionError,
        SameEffectsTipRejectionError, Storage, StorageStats,
    },
    Instant, MempoolError,
};

#[cfg(test)]
pub(crate) mod tests;

/// What a verification error must be for the machine to cache it and hand it
/// to callers.
pub trait VerifyError: std::error::Error + Clone + Send + Sync + 'static {}

impl<T: std::error::Error + Clone + Send + Sync + 'static> VerifyError for T {}

/// Names one submission that waits for a verdict, or one caller waiting for
/// an output.
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WaiterId(pub u64);

/// Names one verification job.
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct JobId(pub u64);

/// What a verification found. The core's task classifies the verifier's
/// answer into one of these.
#[derive(Clone, Debug)]
pub enum Outcome<E> {
    /// The transaction is valid at the tip it was verified against.
    /// `pool_inputs` are the inputs it spends from the pool the verifier saw.
    Pass {
        /// The verified transaction.
        tx: VerifiedUnminedTx,
        /// The inputs that came from the pool, not the chain.
        pool_inputs: Vec<OutPoint>,
    },
    /// A block on the best chain already mined the transaction. The detail is
    /// what callers are told.
    AlreadyMined(RejectionDetail),
    /// The transaction is invalid. The error's kind says how long it is
    /// remembered.
    Fail {
        /// The rejection to cache.
        error: RejectionError<E>,
        /// What it costs a peer supplier.
        charge: Charge,
    },
    /// These transparent inputs are in neither the chain nor the pool.
    /// `error` is the verifier's error, cached for these exact bytes if
    /// the wait for a parent ends.
    MissingInputs {
        /// The missing inputs.
        missing: Vec<OutPoint>,
        /// The verifier's error for them.
        error: E,
    },
    /// The verification failed on this node.
    LocalFault(LocalFault),
}

/// One thing the core does because of an input.
#[derive(Clone, Debug)]
pub enum Effect {
    /// Start a verification of this transaction for the block after `tip`.
    Start {
        /// The job.
        job: JobId,
        /// The transaction.
        tx: UnminedTx,
        /// The machine's tip.
        tip: ChainTip,
        /// Whether the chain was reset since its body arrived, by a reorg,
        /// skipped blocks or a rules change. Today's pool charges no cooldown
        /// for a transaction it retries after a tip reset, and neither does
        /// the core's verifier. Bans still apply.
        after_reset: bool,
    },
    /// Cancel a verification. Its outcome is ignored.
    Cancel(JobId),
    /// Tell the caller fetching under this reservation that the body is no
    /// longer needed, because a block mined the transaction. See
    /// `Reservation::cancellation`.
    CancelFetch(ReservationId),
    /// Answer a submission.
    Verdict {
        /// The submission.
        waiter: WaiterId,
        /// Its verdict.
        verdict: Verdict,
    },
}

/// What one input caused, in order.
#[derive(Clone, Debug, Default)]
pub struct Step {
    /// What the core does.
    pub effects: Vec<Effect>,
    /// The batch for the change log.
    pub changes: Vec<Change>,
}

impl Step {
    /// Whether the input caused nothing.
    pub fn is_empty(&self) -> bool {
        self.effects.is_empty() && self.changes.is_empty()
    }

    /// The verdict this step gave `waiter`, if it gave one.
    #[cfg(test)]
    pub(crate) fn verdict(&self, waiter: WaiterId) -> Option<&Verdict> {
        self.effects.iter().find_map(|effect| match effect {
            Effect::Verdict { waiter: w, verdict } if *w == waiter => Some(verdict),
            _ => None,
        })
    }
}

/// What the pool holds, for its size gauges. See
/// [`MempoolState::pool_counts`].
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct PoolCounts {
    /// Transactions in the pool, which readers and templates see.
    pub pooled: usize,
    /// Transactions a reset took out of the pool that are not back yet:
    /// waiting for a slot, or being verified again.
    pub readmitting: usize,
}

/// The machine's limits. Production uses the port's constants. Tests use
/// smaller ones so a short run reaches every full state.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Limits {
    /// Undecided transactions node-wide, [`MAX_PENDING`].
    pub pending: usize,
    /// Slots for pooled transactions verified again after a reset,
    /// [`MAX_READMITTING`].
    pub readmitting: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            pending: MAX_PENDING,
            readmitting: MAX_READMITTING,
        }
    }
}

/// Where an undecided transaction is.
#[derive(Clone, Debug)]
enum Stage {
    /// No body has arrived.
    Reserved,
    /// A job is running.
    Verifying { job: JobId, deadline: Instant },
    /// Parents are missing.
    Parked { on: Vec<transaction::Hash> },
}

/// An undecided transaction and the submission waiting on it.
#[derive(Clone, Debug)]
struct Admission<E> {
    tx_id: UnminedTxId,
    /// The reservation it was granted under, whose caller is fetching the
    /// body until `body` is set. `None` for a pooled transaction that a
    /// reset sent back through admission: nobody waits for its verdict.
    owner: Option<ReservationId>,
    body: Option<UnminedTx>,
    /// The submission that brought the body, which gets the verdict.
    waiter: Option<WaiterId>,
    stage: Stage,
    /// When one of its inputs was first found missing. Its wait for a parent
    /// runs from here and is never restarted.
    missing_since: Option<Instant>,
    /// The verifier's error for its missing inputs, cached if the wait ends.
    missing_error: Option<E>,
    /// Whether the one check allowed after that wait ended has been used.
    last_check_used: bool,
    /// Whether the chain was reset since it was reserved. See
    /// [`Effect::Start`].
    after_reset: bool,
}

/// What the machine keeps about a pooled transaction, whose body is in
/// storage.
#[derive(Clone, Debug)]
struct Pooled {
    id: UnminedTxId,
    /// The transactions whose outputs it spends.
    parents: Vec<transaction::Hash>,
    expiry: Option<Height>,
    /// The machine's event count when it entered the pool.
    added_at: u64,
}

/// A pooled transaction that a reset took out of the pool, waiting for a
/// slot to be verified again.
#[derive(Clone, Debug)]
struct Queued {
    tx: UnminedTx,
    /// What its body counted for in the pool.
    cost: u64,
}

/// A slot kept for a fetch that is still out after its id was decided.
#[derive(Clone, Debug)]
struct Lease {
    tx_id: UnminedTxId,
    /// The verdict its admission got, which its caller gets too.
    verdict: Verdict,
}

/// The spends and nullifiers of a set of transactions, to find what conflicts
/// with them.
#[derive(Default)]
struct Spends {
    outpoints: HashSet<OutPoint>,
    sprout: HashSet<sprout::Nullifier>,
    sapling: HashSet<sapling::Nullifier>,
    orchard: HashSet<orchard::Nullifier>,
    ironwood: HashSet<ironwood::Nullifier>,
}

impl Spends {
    /// The spends of `transactions`.
    fn of<'a>(transactions: impl IntoIterator<Item = &'a Transaction>) -> Self {
        let mut spends = Self::default();
        for tx in transactions {
            spends.add(tx);
        }
        spends
    }

    /// Adds the spends of `tx`.
    fn add(&mut self, tx: &Transaction) {
        self.outpoints.extend(tx.spent_outpoints());
        self.sprout.extend(tx.sprout_nullifiers().copied());
        self.sapling.extend(tx.sapling_nullifiers().copied());
        self.orchard.extend(tx.orchard_nullifiers().copied());
        self.ironwood.extend(tx.ironwood_nullifiers().copied());
    }

    /// Whether `tx` spends an output or reveals a nullifier these do.
    fn conflict_with(&self, tx: &Transaction) -> bool {
        tx.spent_outpoints()
            .any(|outpoint| self.outpoints.contains(&outpoint))
            || tx
                .sprout_nullifiers()
                .any(|nullifier| self.sprout.contains(nullifier))
            || tx
                .sapling_nullifiers()
                .any(|nullifier| self.sapling.contains(nullifier))
            || tx
                .orchard_nullifiers()
                .any(|nullifier| self.orchard.contains(nullifier))
            || tx
                .ironwood_nullifiers()
                .any(|nullifier| self.ironwood.contains(nullifier))
    }
}

/// The tip before the chain names one. The pool is off until then.
const NO_TIP: ChainTip = ChainTip {
    hash: block::Hash([0; 32]),
    height: Height(0),
};

/// The pool core's rules, over the node's real transactions and storage.
pub struct MempoolState<E> {
    /// Whether the pool is on: the node is near a tip the chain has named.
    active: bool,
    /// Whether the node is near the tip.
    near_tip: bool,
    /// Whether the chain has named a tip.
    has_tip: bool,
    now: Instant,
    tip: ChainTip,
    /// The event count when the tip last changed. A job that started before
    /// it verified for another tip.
    tip_at: u64,
    next_id: u64,
    limits: Limits,
    /// The pool, and the rejection lists.
    storage: Storage<E>,
    /// Undecided transactions, oldest first.
    admissions: BTreeMap<u64, Admission<E>>,
    admission_of: HashMap<UnminedTxId, u64>,
    owners: HashMap<ReservationId, u64>,
    /// Reservations whose id was decided through someone else's body while
    /// their own fetch was still out. Each keeps its slot until its caller
    /// submits or drops it, so a source cannot have more fetches out than
    /// slots.
    leases: HashMap<ReservationId, Lease>,
    /// What a reset took out of the pool and no slot has verified again
    /// yet, oldest first, so a parent is verified before the children that
    /// came after it.
    queue: BTreeMap<u64, Queued>,
    /// The queue by mined id.
    queued: HashMap<transaction::Hash, u64>,
    /// What the queued bodies cost. They count against the pool's cost limit
    /// with what storage holds, so emptying the pool and refilling it cannot
    /// grow memory past the limit.
    queued_cost: u64,
    /// The re-admissions under way. Each holds a slot.
    readmitting: usize,
    /// Every pooled transaction, oldest first.
    pool: BTreeMap<u64, Pooled>,
    pooled: HashMap<transaction::Hash, u64>,
    /// The pooled transactions that spend each transaction's outputs, so a
    /// removal finds its dependents without a scan. Keyed by pool key, so a
    /// removal is logarithmic however many children one confirmed parent
    /// has, and dependents are found oldest first.
    children: HashMap<transaction::Hash, BTreeMap<u64, transaction::Hash>>,
    /// Each job's transaction, with the event count when it started.
    jobs: HashMap<JobId, (UnminedTxId, u64)>,
    /// Changes returned in earlier steps. Each step is one batch of the log,
    /// so this is the log position of the last one.
    logged: u64,
    step: Step,
}

impl<E: VerifyError> MempoolState<E> {
    /// An empty pool that is off: the chain has named no tip yet, and the
    /// node is not known to be near one.
    pub fn new(config: &Config, now: Instant) -> Self {
        Self {
            active: false,
            near_tip: false,
            has_tip: false,
            now,
            tip: NO_TIP,
            tip_at: 0,
            next_id: 0,
            limits: Limits::default(),
            storage: Storage::new(config),
            admissions: BTreeMap::new(),
            admission_of: HashMap::default(),
            owners: HashMap::default(),
            leases: HashMap::default(),
            queue: BTreeMap::new(),
            queued: HashMap::default(),
            queued_cost: 0,
            readmitting: 0,
            pool: BTreeMap::new(),
            pooled: HashMap::default(),
            children: HashMap::default(),
            jobs: HashMap::default(),
            logged: 0,
            step: Step::default(),
        }
    }

    /// The same machine with other limits.
    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }

    // Inputs.

    /// Reserve one of the node's slots for `tx_id`. See
    /// `PoolHandle::reserve`.
    pub fn reserve(
        &mut self,
        tx_id: UnminedTxId,
        now: Instant,
    ) -> (Result<ReservationId, Unreserved>, Step) {
        self.enter(now);
        let answer = self.grant(tx_id);
        (answer, self.take_step())
    }

    /// Grants a reservation for `tx_id`, or says why not.
    fn grant(&mut self, tx_id: UnminedTxId) -> Result<ReservationId, Unreserved> {
        if !self.active {
            return Err(Unreserved::Inactive);
        }
        // A slot a deadline just freed goes to a re-admission that was
        // already waiting for one, before this reservation counts slots.
        self.start_queued();
        if self.pooled.contains_key(&tx_id.mined_id()) {
            return Err(Unreserved::Pooled);
        }
        if let Some(error) = self.cached_rejection(tx_id) {
            return Err(Unreserved::Rejected(rejection(
                self.tip,
                scope_of(&error),
                Charge::None,
                error,
            )));
        }
        // Today's `AlreadyQueued`: an undecided id is brought in once. That
        // covers a pooled transaction waiting to be verified again after a
        // reset, so nobody fetches a body the node holds.
        if self.admission_of.contains_key(&tx_id) || self.queued.contains_key(&tx_id.mined_id()) {
            return Err(Unreserved::Pending);
        }
        if self.slots_in_use() >= self.limits.pending {
            return Err(Unreserved::NoRoom);
        }
        let owner = ReservationId(self.fresh());
        let key = self.fresh();
        self.admissions.insert(
            key,
            Admission {
                tx_id,
                owner: Some(owner),
                body: None,
                waiter: None,
                stage: Stage::Reserved,
                missing_since: None,
                missing_error: None,
                last_check_used: false,
                after_reset: false,
            },
        );
        self.admission_of.insert(tx_id, key);
        self.owners.insert(owner, key);
        Ok(owner)
    }

    /// A reservation dropped before the core accepted it.
    pub fn release(&mut self, reservation: ReservationId, now: Instant) -> Step {
        self.enter(now);
        self.drop_reservation(reservation);
        self.take_step()
    }

    /// The caller's fetch ended without a body. Unlike a plain drop, this is
    /// logged, because today's change stream reports a download that failed.
    pub fn fetch_failed(&mut self, reservation: ReservationId, now: Instant) -> Step {
        self.enter(now);
        // An admission that ended while this fetch was out has its outcome
        // in the log. A late failure of the fetch adds nothing to it.
        if let Some(tx_id) = self.undecided_for(reservation) {
            self.step.changes.push(Change::Refused(tx_id));
        }
        self.drop_reservation(reservation);
        self.take_step()
    }

    /// A body under `reservation`. Returns the waiter the verdict will name,
    /// which can be in the returned step.
    pub fn submit(
        &mut self,
        reservation: ReservationId,
        tx: UnminedTx,
        now: Instant,
    ) -> (WaiterId, Step) {
        self.enter(now);
        let tx_id = tx.id();
        let waiter = WaiterId(self.fresh());
        // The handle checks the body against the reservation before it sends
        // anything. A wrong body drops the reservation.
        if self
            .reserved_for(reservation)
            .is_some_and(|reserved| reserved != tx_id)
        {
            self.drop_reservation(reservation);
            self.step.effects.push(Effect::Verdict {
                waiter,
                verdict: Verdict::LocalFault(LocalFault::WrongTransaction),
            });
            return (waiter, self.take_step());
        }
        let Some(key) = self.owners.remove(&reservation) else {
            // The admission this reservation was granted for ended while
            // its fetch was out. The caller gets that admission's verdict,
            // as if it had come in time.
            let verdict = match self.leases.remove(&reservation) {
                Some(lease) => lease.verdict,
                None => self.stale_verdict(tx_id),
            };
            self.step.effects.push(Effect::Verdict { waiter, verdict });
            return (waiter, self.take_step());
        };
        let admission = self.admission_at(key);
        admission.waiter = Some(waiter);
        admission.body = Some(tx);
        self.start_admission(key);
        (waiter, self.take_step())
    }

    /// The outcome of `job`. `wall` is the wall-clock time, stamped on a
    /// transaction that enters the pool.
    ///
    /// A job verifies for the block after the tip it started at, so its
    /// outcome counts only while that is still the tip, as today's pool
    /// checks the tip before it inserts. Otherwise the job runs again.
    pub fn outcome(
        &mut self,
        job: JobId,
        outcome: Outcome<E>,
        now: Instant,
        wall: DateTime<Utc>,
    ) -> Step {
        self.enter(now);
        let Some((tx_id, started_at)) = self.jobs.remove(&job) else {
            // Cancelled, or timed out.
            return self.take_step();
        };
        let key = self.admission_of[&tx_id];
        if started_at < self.tip_at {
            self.start_admission(key);
        } else {
            self.admission_outcome(key, outcome, started_at, wall);
        }
        self.take_step()
    }

    /// The best chain grew by `block`.
    pub fn grow(&mut self, block: ConnectedBlock, now: Instant) -> Step {
        self.enter(now);
        let was_active = self.active;
        self.set_tip(ChainTip {
            hash: block.hash,
            height: block.height,
        });
        if !was_active {
            return self.take_step();
        }
        let now = self.now;
        self.storage.clear_tip_rejections();
        // What the block mined, and what in the pool conflicts with it, are
        // worked out before anything is removed. A pooled transaction that
        // conflicts with a later block transaction is then found even if an
        // earlier one already took its parent away.
        let mined = block.transaction_hashes;
        let mined_set: HashSet<transaction::Hash> = mined.iter().copied().collect();
        let spends = Spends::of(block.transactions.iter().map(|tx| &**tx));
        let conflicting: Vec<UnminedTxId> = self
            .pool
            .values()
            .filter(|pooled| !mined_set.contains(&pooled.id.mined_id()))
            .filter(|pooled| spends.conflict_with(self.transaction_of(pooled)))
            .map(|pooled| pooled.id)
            .collect();
        for hash in mined.iter() {
            if let Some(key) = self.pooled.get(hash).copied() {
                // Its dependents stay. Their parent is on the chain now.
                self.remove_one(key, RemovalReason::Mined);
            }
            // Its removal at the reset is logged already.
            if let Some(key) = self.queued.get(hash).copied() {
                self.unqueue(key);
            }
            self.storage
                .reject_effects_chain(*hash, SameEffectsChainRejectionError::Mined, now);
        }
        // Today's pool cancels the download and verification of anything a
        // block mined. The undecided ones end here, and a fetch still out is
        // told to stop, so its slot is freed once its caller drops it.
        let ended: Vec<u64> = self
            .admissions
            .iter()
            .filter(|(_, admission)| mined_set.contains(&admission.tx_id.mined_id()))
            .map(|(key, _)| *key)
            .collect();
        for key in ended {
            let admission = self.admission_at(key);
            if let (None, Some(owner)) = (&admission.body, admission.owner) {
                self.step.effects.push(Effect::CancelFetch(owner));
            }
            self.cancel_admission_job(key);
            self.reject(
                key,
                SameEffectsChainRejectionError::Mined.into(),
                Charge::None,
            );
        }
        for conflict in conflicting {
            self.remove_with_dependents(conflict.mined_id(), RemovalReason::Conflicted);
            self.storage.reject_effects_chain(
                conflict.mined_id(),
                SameEffectsChainRejectionError::DuplicateSpend,
                now,
            );
        }
        // The block was seen, and what it mined is gone already. So an
        // expired transaction here was not mined, and what depends on it goes
        // with it.
        self.remove_expired();
        // One pass over the parked lists, which can be long: an unknown input
        // parks a transaction on every transparent spend.
        self.wake(|parent| mined_set.contains(parent));
        self.take_step()
    }

    /// The tip changed some other way than by one block: a reorg, several
    /// blocks at once, or new rules for the next block. The core does not
    /// know which blocks changed, so every pooled and parked transaction is
    /// checked again at `tip`. A rejection made on the old chain or under the
    /// old rules says nothing, so today's reset drops every list too.
    ///
    /// Each pooled transaction leaves the pool and waits to come back
    /// through admission, as today's reset queues the pool to be verified
    /// again.
    pub fn reset(&mut self, tip: ChainTip, now: Instant) -> Step {
        self.enter(now);
        let was_active = self.active;
        self.set_tip(tip);
        if !was_active {
            return self.take_step();
        }
        self.mark_reset();
        self.storage.clear_rejections();
        let mut bodies: HashMap<transaction::Hash, VerifiedUnminedTx> = self
            .storage
            .take_all()
            .into_iter()
            .map(|tx| (tx.transaction.id().mined_id(), tx))
            .collect();
        self.pooled.clear();
        self.children.clear();
        for pooled in std::mem::take(&mut self.pool).into_values() {
            let hash = pooled.id.mined_id();
            let body = bodies
                .remove(&hash)
                .expect("storage holds every pooled transaction");
            // Expiry needs no verifier, only the new tip.
            if self.reached(pooled.expiry) {
                self.step.changes.push(Change::Removed {
                    id: pooled.id,
                    reason: RemovalReason::Expired,
                });
                self.storage.reject_effects_chain(
                    hash,
                    SameEffectsChainRejectionError::Expired,
                    self.now,
                );
                continue;
            }
            self.step.changes.push(Change::Removed {
                id: pooled.id,
                reason: RemovalReason::Reset,
            });
            let key = self.fresh();
            let cost = body.cost();
            self.queue.insert(
                key,
                Queued {
                    tx: body.transaction,
                    cost,
                },
            );
            self.queued.insert(hash, key);
            self.queued_cost = self.queued_cost.saturating_add(cost);
        }
        // A block that left the chain may have spent an output a parked
        // transaction is waiting for. Nothing names which, so each one looks
        // again.
        self.rerun_parked();
        self.take_step()
    }

    /// Time passed. Applies every deadline at or before `now`.
    pub fn tick(&mut self, now: Instant) -> Step {
        self.enter(now);
        self.take_step()
    }

    /// The node moved near the tip, or away from it. Near a tip the chain
    /// has named, the pool turns on, empty. Away from it, the pool turns off.
    pub fn near_tip(&mut self, near: bool, now: Instant) -> Step {
        self.enter(now);
        self.near_tip = near;
        self.switch();
        self.take_step()
    }

    /// The members of `ids` a cached rejection covers at `now`. Takes no
    /// slot and changes nothing.
    pub fn rejected_ids(&self, ids: HashSet<UnminedTxId>, now: Instant) -> HashSet<UnminedTxId> {
        self.storage
            .rejected_transactions(ids, now.max(self.now))
            .collect()
    }

    // Observations.

    /// Whether the pool is on.
    pub fn is_active(&self) -> bool {
        self.active
    }

    /// The tip the pool is valid for, or the zero tip before the chain
    /// names one.
    pub fn tip(&self) -> ChainTip {
        self.tip
    }

    /// The pool as a snapshot, for the change log position `last_change`.
    pub fn snapshot(&self, last_change: ChangeSeq) -> PoolSnapshot {
        if !self.active {
            return PoolSnapshot::inactive(self.tip, last_change);
        }
        PoolSnapshot::new(
            self.tip,
            last_change,
            self.storage.mining_transactions().cloned().collect(),
            self.storage.transaction_dependencies().clone(),
        )
    }

    /// What storage holds, for the pool's metrics.
    pub fn storage_stats(&self) -> StorageStats {
        self.storage.stats()
    }

    /// What the pool holds, for its size gauges.
    pub fn pool_counts(&self) -> PoolCounts {
        PoolCounts {
            pooled: self.pool.len(),
            readmitting: self.queue.len().saturating_add(self.readmitting),
        }
    }

    /// The earliest time an input would apply a deadline, if any is set.
    pub fn next_deadline(&self) -> Option<Instant> {
        self.admissions
            .values()
            .filter_map(|admission| match &admission.stage {
                Stage::Verifying { deadline, .. } => Some(*deadline),
                Stage::Parked { .. } => admission.missing_since.map(|since| since + T_PARK),
                Stage::Reserved => None,
            })
            .min()
    }

    /// Undecided transactions, plus leases.
    fn slots_in_use(&self) -> usize {
        self.admissions.len().saturating_add(self.leases.len())
    }

    /// Reservations callers still hold, of every kind.
    #[cfg(test)]
    pub(crate) fn caller_held(&self) -> usize {
        self.owners.len().saturating_add(self.leases.len())
    }

    /// The pool, oldest first.
    #[cfg(test)]
    pub(crate) fn pooled_ids(&self) -> Vec<UnminedTxId> {
        self.pool.values().map(|pooled| pooled.id).collect()
    }

    /// How many transactions are pooled.
    pub fn pooled_count(&self) -> usize {
        self.pool.len()
    }

    /// How many transactions are undecided: reserved, fetched or being
    /// verified.
    pub fn undecided_count(&self) -> usize {
        self.admissions.len()
    }

    /// How many transactions wait for a slot to be verified again after a
    /// reset.
    pub fn queued_count(&self) -> usize {
        self.queue.len()
    }

    /// The transactions waiting for a slot to be verified again after a
    /// reset, oldest first.
    #[cfg(test)]
    pub(crate) fn queued_ids(&self) -> Vec<UnminedTxId> {
        self.queue.values().map(|queued| queued.tx.id()).collect()
    }

    /// Whether any undecided transaction is parked on a missing parent.
    #[cfg(test)]
    pub(crate) fn is_parked_any(&self) -> bool {
        self.admissions
            .values()
            .any(|admission| matches!(admission.stage, Stage::Parked { .. }))
    }

    /// Checks the invariants every reachable state keeps.
    ///
    /// # Panics
    ///
    /// If one is broken.
    #[cfg(test)]
    pub(crate) fn assert_invariants(&self) {
        // A snapshot is the storage's mining transactions, in its order and
        // with the stamps it gave them, at the machine's tip.
        let snapshot = self.snapshot(ChangeSeq(self.logged));
        assert_eq!(snapshot.tip, self.tip, "a snapshot is of the machine's tip");
        if self.active {
            assert!(
                snapshot
                    .transactions
                    .iter()
                    .eq(self.storage.mining_transactions()),
                "a snapshot holds the storage's mining transactions, in mining order, with their stamps"
            );
        }
        assert!(
            self.step.is_empty(),
            "an input left effects behind: {:?}",
            self.step
        );
        assert_eq!(
            self.active,
            self.near_tip && self.has_tip,
            "the pool is on exactly while the node is near a tip the chain has named"
        );
        assert!(
            self.slots_in_use() <= self.limits.pending,
            "pool/admissions-within-limits: the node's limit"
        );
        assert_eq!(
            self.admission_of.len(),
            self.admissions.len(),
            "each admission is indexed by its id"
        );
        for (key, admission) in &self.admissions {
            assert_eq!(
                self.admission_of.get(&admission.tx_id),
                Some(key),
                "each admission is indexed by its id"
            );
            assert_eq!(
                admission.body.is_none(),
                matches!(admission.stage, Stage::Reserved),
                "an admission has a body exactly when it has left the reserved stage"
            );
            match admission.owner {
                Some(_) => assert_eq!(
                    admission.body.is_some(),
                    admission.waiter.is_some(),
                    "the submission that brought the body waits for its verdict"
                ),
                None => assert!(
                    admission.body.is_some() && admission.waiter.is_none(),
                    "a re-admission holds its body, and nobody waits for it"
                ),
            }
        }
        assert_eq!(
            self.queued.len(),
            self.queue.len(),
            "the queue is indexed by mined id"
        );
        for (key, queued) in &self.queue {
            assert_eq!(
                self.queued.get(&queued.tx.id().mined_id()),
                Some(key),
                "the queue is indexed by mined id"
            );
            assert!(
                !self.pooled.contains_key(&queued.tx.id().mined_id()),
                "nothing pooled also waits to be verified again"
            );
        }
        assert_eq!(
            self.queue.values().map(|queued| queued.cost).sum::<u64>(),
            self.queued_cost,
            "the queued cost matches the queue"
        );
        let readmitting = self
            .admissions
            .values()
            .filter(|admission| admission.owner.is_none())
            .count();
        assert_eq!(
            readmitting, self.readmitting,
            "the re-admission count matches the admissions"
        );
        assert!(
            self.readmitting <= self.limits.readmitting,
            "pool/readmission-within-limit: the re-admission share"
        );
        assert!(
            self.storage.total_cost().saturating_add(self.queued_cost) <= self.storage.cost_limit(),
            "pool/cost-within-limit: the pool and the queue together stay within the cost limit"
        );
        for (key, pooled) in &self.pool {
            let hash = pooled.id.mined_id();
            let expected: BTreeMap<u64, transaction::Hash> = self
                .pool
                .iter()
                .filter(|(_, child)| child.parents.contains(&hash))
                .map(|(child_key, child)| (*child_key, child.id.mined_id()))
                .collect();
            let indexed = self.children.get(&hash).cloned().unwrap_or_default();
            assert_eq!(
                indexed, expected,
                "the children index matches the pool at {key}"
            );
            assert_eq!(
                self.pooled.get(&hash),
                Some(key),
                "the pool is indexed by id"
            );
            assert!(
                pooled.expiry.is_none_or(|expiry| self.tip.height < expiry),
                "pool/consistent: no expired transaction"
            );
        }
        assert_eq!(
            self.storage.transaction_count(),
            self.pool.len(),
            "storage holds exactly the pool"
        );
        let mut spends = Spends::default();
        for pooled in self.pool.values() {
            let tx = self.transaction_of(pooled);
            assert!(
                !spends.conflict_with(tx),
                "pool/consistent: no two pooled transactions conflict"
            );
            spends.add(tx);
        }
        self.storage.assert_invariants();
        // Nothing that can start is left waiting, and nothing past its
        // deadline is left as it was. The input that freed a slot started
        // the next queued re-admission, and the first input to see a time
        // applied every deadline due by then.
        if self.active {
            let room = self.readmitting < self.limits.readmitting
                && self.slots_in_use() < self.limits.pending;
            assert!(
                !room || self.queue.is_empty(),
                "a re-admission that could start was left queued"
            );
            for admission in self.admissions.values() {
                if let Stage::Verifying { deadline, .. } = admission.stage {
                    assert!(
                        deadline > self.now,
                        "pool/slot-time-within-limit: a verification past its deadline was left running"
                    );
                }
                if matches!(admission.stage, Stage::Parked { .. }) {
                    assert!(
                        admission
                            .missing_since
                            .is_some_and(|since| since + T_PARK > self.now),
                        "pool/slot-time-within-limit: a transaction was left parked past its wait for a parent"
                    );
                }
            }
        }
    }

    // Rules.

    /// Moves time to `now` and applies every deadline that is due, before the
    /// input that brought the time is handled.
    fn enter(&mut self, now: Instant) {
        // The machine's own clock never goes back.
        if now > self.now {
            self.now = now;
        }
        let now = self.now;
        self.storage.prune_rejections(now);
        let mut timed_out = Vec::new();
        let mut unparked = Vec::new();
        for (key, admission) in &self.admissions {
            match &admission.stage {
                Stage::Verifying { deadline, .. } if *deadline <= now => timed_out.push(*key),
                Stage::Parked { .. }
                    if admission
                        .missing_since
                        .is_some_and(|since| since + T_PARK <= now) =>
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
            let error = self
                .admission_at(key)
                .missing_error
                .clone()
                .expect("a parked admission holds the verifier's missing-input error");
            self.reject(
                key,
                ExactTipRejectionError::FailedVerification(error).into(),
                Charge::None,
            );
        }
    }

    /// The one way out of an input that returns a step. Queued re-admissions
    /// that can start are started first, so no input can free a slot and
    /// leave it unused until something else happens.
    fn take_step(&mut self) -> Step {
        if self.active {
            self.start_queued();
        }
        let step = std::mem::take(&mut self.step);
        let changes = u64::try_from(step.changes.len()).expect("a batch fits in u64");
        self.logged = self.logged.saturating_add(changes);
        step
    }

    /// The id a reservation was granted for, while its admission is
    /// undecided.
    fn undecided_for(&self, reservation: ReservationId) -> Option<UnminedTxId> {
        let key = self.owners.get(&reservation)?;
        self.admissions.get(key).map(|admission| admission.tx_id)
    }

    /// The id a reservation was granted for, for as long as the machine knows
    /// the reservation.
    fn reserved_for(&self, reservation: ReservationId) -> Option<UnminedTxId> {
        self.undecided_for(reservation)
            .or_else(|| self.leases.get(&reservation).map(|lease| lease.tx_id))
    }

    /// Ends a reservation that brought no body: frees its lease, or ends its
    /// admission. Nobody else can bring that body, and no submission waits
    /// on it.
    fn drop_reservation(&mut self, reservation: ReservationId) {
        if let Some(key) = self.owners.remove(&reservation) {
            self.end(key);
        }
        self.leases.remove(&reservation);
    }

    /// Applies a fresh outcome to an undecided transaction.
    fn admission_outcome(
        &mut self,
        key: u64,
        outcome: Outcome<E>,
        started_at: u64,
        wall: DateTime<Utc>,
    ) {
        let now = self.now;
        match outcome {
            Outcome::Pass { tx, pool_inputs } => {
                let hash = tx.transaction.id().mined_id();
                // Today's insertion checks the rejection lists first. A
                // rejection made while this one was verified, such as a block
                // that mined it, still holds. It is not cached again.
                if let Some(error) = self.cached_rejection(tx.transaction.id()) {
                    self.refuse(key, error);
                    return;
                }
                // The verifier should have said the tip reached its expiry
                // height. The pool does not rely on that.
                if self.reached(tx.transaction.transaction().expiry_height()) {
                    self.reject(
                        key,
                        SameEffectsChainRejectionError::Expired.into(),
                        Charge::None,
                    );
                    return;
                }
                // Today's insertion checks, in today's order. First, another
                // set of authorizing data for the same effects got in while
                // this one was verified. That is a duplicate, not a conflict,
                // and nothing is cached against the transaction the pool holds.
                if self.pooled.contains_key(&hash) {
                    self.refuse(key, MempoolError::InMempool);
                    return;
                }
                // Today's insertion checks standardness before conflicts, so a
                // body that is both non-standard and conflicting is refused as
                // non-standard, for these exact bytes. Insertion checks it
                // again, one more pass over the scripts, which is small beside
                // the verification this outcome came from.
                if let Err(error) = self.storage.standardness(&tx) {
                    self.storage
                        .reject_exact(tx.transaction.id(), error.clone().into());
                    self.refuse(key, MempoolError::NonStandardTransaction(error));
                    return;
                }
                if self.storage.has_spend_conflicts(&tx.transaction) {
                    // The pool took a conflicting transaction while this one
                    // was verified.
                    self.reject(
                        key,
                        SameEffectsTipRejectionError::SpendConflict.into(),
                        Charge::None,
                    );
                    return;
                }
                // The verifier used a pool output that the pool no longer
                // offers. The parent may have been mined, and the child is then
                // valid against the chain, so the job runs again.
                if !pool_inputs.iter().all(|input| self.offers(input)) {
                    self.start_admission(key);
                    return;
                }
                let (result, evicted) = self.storage.insert_with_evicted_ids(
                    tx.clone(),
                    pool_inputs,
                    Some(self.tip.height),
                    now,
                    wall,
                );
                self.forget_evicted(evicted);
                if let Err(error) = result {
                    // Storage has cached what it refused.
                    self.refuse(key, error);
                    return;
                }
                self.step.changes.push(Change::Added(tx.transaction.id()));
                let pool_key = self.fresh();
                let parents = parents_of(tx.transaction.transaction());
                for parent in &parents {
                    self.children
                        .entry(*parent)
                        .or_default()
                        .insert(pool_key, hash);
                }
                self.pool.insert(
                    pool_key,
                    Pooled {
                        id: tx.transaction.id(),
                        parents,
                        expiry: tx.transaction.transaction().expiry_height(),
                        added_at: pool_key,
                    },
                );
                self.pooled.insert(hash, pool_key);
                self.decide(key, Verdict::Admitted);
                // Another authorization of these effects that a reset left
                // waiting has nothing to come back to.
                if let Some(queued) = self.queued.get(&hash).copied() {
                    self.unqueue(queued);
                }
                self.trim_queue();
                self.wake(|parent| *parent == hash);
            }
            // Found in the best chain by this job's own lookup. Today that
            // leaves the rejection lists alone. Only a block the core sees
            // connect fills the mined list.
            Outcome::AlreadyMined(detail) => {
                let tx_id = self.admission_at(key).tx_id;
                self.step.changes.push(Change::Refused(tx_id));
                self.decide(
                    key,
                    Verdict::Rejected(Rejection {
                        scope: RejectionScope::Uncached,
                        charge: Charge::None,
                        tip: self.tip.height,
                        detail,
                    }),
                );
            }
            Outcome::Fail { error, charge } => self.reject(key, error, charge),
            Outcome::MissingInputs { missing, error } => {
                // One of the missing outputs became available after this job
                // started looking. Its admission has already been announced, so
                // nothing would wake this transaction. It runs again at once.
                let arrived = missing
                    .iter()
                    .any(|outpoint| self.arrived_since(outpoint, started_at));
                let admission = self.admission_at(key);
                admission.missing_error = Some(error.clone());
                let since = *admission.missing_since.get_or_insert(now);
                let wait_over = since + T_PARK <= now;
                // After the wait, an input that arrived during this check earns
                // one more check and no more.
                let last_check = wait_over && arrived && !admission.last_check_used;
                if last_check {
                    admission.last_check_used = true;
                }
                if (arrived && !wait_over) || last_check {
                    self.start_admission(key);
                    return;
                }
                if wait_over && arrived {
                    // There is no check left to give, and this answer is
                    // already out of date.
                    self.fail_locally(key, LocalFault::VerifyTimeout);
                    return;
                }
                if wait_over {
                    // Today a parent that never arrived is a missing-input
                    // failure, remembered for these exact bytes until the tip
                    // changes.
                    self.reject(
                        key,
                        ExactTipRejectionError::FailedVerification(error).into(),
                        Charge::None,
                    );
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

    /// Rejects an admission: caches the rejection, logs it, and answers every
    /// waiter.
    fn reject(&mut self, key: u64, error: RejectionError<E>, charge: Charge) {
        let tx_id = self.admission_at(key).tx_id;
        self.storage.reject(tx_id, error.clone(), self.now);
        let error = MempoolError::from(error);
        self.step.changes.push(Change::Refused(tx_id));
        self.decide(
            key,
            Verdict::Rejected(rejection(self.tip, scope_of(&error), charge, error)),
        );
    }

    /// Refuses an admission with an error storage has already cached, or one
    /// nothing caches.
    fn refuse(&mut self, key: u64, error: MempoolError<E>) {
        let tx_id = self.admission_at(key).tx_id;
        self.step.changes.push(Change::Refused(tx_id));
        self.decide(
            key,
            Verdict::Rejected(rejection(self.tip, scope_of(&error), Charge::None, error)),
        );
    }

    /// Ends an admission: its submission gets the verdict, and its slot is
    /// free. A charge reaches only a submission with a peer supplier.
    fn decide(&mut self, key: u64, verdict: Verdict) {
        let admission = self.end(key);
        if let Some(owner) = admission.owner {
            self.owners.remove(&owner);
            if admission.body.is_none() {
                // The reserving caller's fetch is still out. Its body will
                // arrive decoded, so its slot is not free yet.
                self.leases.insert(
                    owner,
                    Lease {
                        tx_id: admission.tx_id,
                        verdict: verdict.clone(),
                    },
                );
            }
        }
        if let Some(waiter) = admission.waiter {
            self.step.effects.push(Effect::Verdict { waiter, verdict });
        }
    }

    /// Takes an admission out of the machine.
    fn end(&mut self, key: u64) -> Admission<E> {
        let admission = self
            .admissions
            .remove(&key)
            .expect("the caller names a live admission");
        self.admission_of.remove(&admission.tx_id);
        if admission.owner.is_none() {
            self.readmitting = self.readmitting.saturating_sub(1);
        }
        admission
    }

    /// Starts queued re-admissions, oldest first, while their share of the
    /// slots and the node's limit allow. Each goes through admission like a
    /// new transaction, with nobody waiting for its verdict.
    fn start_queued(&mut self) {
        while self.readmitting < self.limits.readmitting
            && self.slots_in_use() < self.limits.pending
        {
            let Some((&key, _)) = self.queue.first_key_value() else {
                break;
            };
            let tx = self.unqueue(key);
            let tx_id = tx.id();
            self.admissions.insert(
                key,
                Admission {
                    tx_id,
                    owner: None,
                    body: Some(tx),
                    waiter: None,
                    stage: Stage::Reserved,
                    missing_since: None,
                    missing_error: None,
                    last_check_used: false,
                    after_reset: true,
                },
            );
            self.admission_of.insert(tx_id, key);
            self.readmitting = self.readmitting.saturating_add(1);
            self.start_admission(key);
        }
    }

    /// Takes the body at `key` out of the queue.
    fn unqueue(&mut self, key: u64) -> UnminedTx {
        let queued = self
            .queue
            .remove(&key)
            .expect("the caller names a queued transaction");
        self.queued.remove(&queued.tx.id().mined_id());
        self.queued_cost = self.queued_cost.saturating_sub(queued.cost);
        queued.tx
    }

    /// Drops the newest queued bodies until the pool and the queue are
    /// within the cost limit together. Each left the pool at the reset, and
    /// the log says so.
    fn trim_queue(&mut self) {
        while self.storage.total_cost().saturating_add(self.queued_cost) > self.storage.cost_limit()
        {
            let Some((&key, _)) = self.queue.last_key_value() else {
                break;
            };
            self.unqueue(key);
        }
    }

    /// The answer to a body whose reservation no longer names an admission.
    fn stale_verdict(&self, tx_id: UnminedTxId) -> Verdict {
        if self.pooled.contains_key(&tx_id.mined_id()) {
            return Verdict::Rejected(rejection(
                self.tip,
                RejectionScope::Uncached,
                Charge::None,
                MempoolError::<E>::InMempool,
            ));
        }
        if let Some(error) = self.cached_rejection(tx_id) {
            return Verdict::Rejected(rejection(self.tip, scope_of(&error), Charge::None, error));
        }
        Verdict::LocalFault(LocalFault::StaleReservation)
    }

    /// Starts a verification of an admission that holds its body.
    fn start_admission(&mut self, key: u64) {
        let job = JobId(self.fresh());
        let deadline = self.now + T_VERIFY;
        let admission = self.admission_at(key);
        admission.stage = Stage::Verifying { job, deadline };
        let tx_id = admission.tx_id;
        let after_reset = admission.after_reset;
        let tx = admission
            .body
            .clone()
            .expect("an admission is verified only once it holds its body");
        self.jobs.insert(job, (tx_id, job.0));
        self.step.effects.push(Effect::Start {
            job,
            tx,
            tip: self.tip,
            after_reset,
        });
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

    /// Runs again every admission parked on a parent that `arrived` accepts.
    fn wake(&mut self, arrived: impl Fn(&transaction::Hash) -> bool) {
        let woken: Vec<u64> = self
            .admissions
            .iter()
            .filter(|(_, admission)| match &admission.stage {
                Stage::Parked { on } => on.iter().any(&arrived),
                _ => false,
            })
            .map(|(key, _)| *key)
            .collect();
        for key in woken {
            self.start_admission(key);
        }
    }

    /// Removes every pooled transaction whose expiry height the tip has
    /// reached, with what depends on it, and remembers each as expired.
    fn remove_expired(&mut self) {
        let expired: Vec<transaction::Hash> = self
            .pool
            .values()
            .filter(|pooled| self.reached(pooled.expiry))
            .map(|pooled| pooled.id.mined_id())
            .collect();
        for hash in expired {
            self.remove_with_dependents(hash, RemovalReason::Expired);
            self.storage.reject_effects_chain(
                hash,
                SameEffectsChainRejectionError::Expired,
                self.now,
            );
        }
    }

    /// Marks every undecided transaction as reset. See [`Effect::Start`].
    fn mark_reset(&mut self) {
        for admission in self.admissions.values_mut() {
            admission.after_reset = true;
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
        self.decide(key, Verdict::LocalFault(fault));
    }

    /// Takes the pooled transaction at `key` off its parents' lists of
    /// children.
    fn forget_parents(&mut self, key: u64, parents: &[transaction::Hash]) {
        for parent in parents {
            if let Some(children) = self.children.get_mut(parent) {
                children.remove(&key);
                if children.is_empty() {
                    self.children.remove(parent);
                }
            }
        }
    }

    /// Removes one pooled transaction and logs it. What depends on it stays.
    fn remove_one(&mut self, key: u64, reason: RemovalReason) {
        let Some(pooled) = self.pool.remove(&key) else {
            return;
        };
        let hash = pooled.id.mined_id();
        self.pooled.remove(&hash);
        self.forget_parents(key, &pooled.parents);
        self.storage.remove_one(&hash);
        self.step.changes.push(Change::Removed {
            id: pooled.id,
            reason,
        });
    }

    /// Removes a pooled transaction and everything in the pool that spends its
    /// outputs. The dependents leave as invalidated.
    fn remove_with_dependents(&mut self, hash: transaction::Hash, reason: RemovalReason) {
        let Some(key) = self.pooled.get(&hash).copied() else {
            return;
        };
        self.remove_one(key, reason);
        let dependents: Vec<transaction::Hash> = self
            .children
            .get(&hash)
            .map(|children| children.values().copied().collect())
            .unwrap_or_default();
        for dependent in dependents {
            self.remove_with_dependents(dependent, RemovalReason::Invalidated);
        }
    }

    /// Forgets the transactions storage evicted to make room, and logs them.
    /// Storage has removed them and cached each one.
    fn forget_evicted(&mut self, evicted: HashSet<UnminedTxId>) {
        let mut keys: Vec<u64> = evicted
            .iter()
            .filter_map(|id| self.pooled.get(&id.mined_id()).copied())
            .collect();
        keys.sort_unstable();
        for key in keys {
            let pooled = self
                .pool
                .remove(&key)
                .expect("storage evicts only what the pool holds");
            let hash = pooled.id.mined_id();
            self.pooled.remove(&hash);
            self.forget_parents(key, &pooled.parents);
            self.step.changes.push(Change::Removed {
                id: pooled.id,
                reason: RemovalReason::Evicted,
            });
        }
    }

    /// The cached rejection that covers `tx_id` now, if any.
    fn cached_rejection(&self, tx_id: UnminedTxId) -> Option<MempoolError<E>> {
        self.storage.rejection(&tx_id, self.now)
    }

    /// Whether the tip has reached `expiry`.
    fn reached(&self, expiry: Option<Height>) -> bool {
        expiry.is_some_and(|expiry| self.tip.height >= expiry)
    }

    /// Whether `outpoint` became available after the event count
    /// `started_at`: its transaction is pooled now, creates that output, and
    /// entered the pool after that count.
    fn arrived_since(&self, outpoint: &OutPoint, started_at: u64) -> bool {
        self.offers(outpoint)
            && self
                .pooled
                .get(&outpoint.hash)
                .is_some_and(|key| self.pool[key].added_at > started_at)
    }

    /// Whether the pool offers `outpoint`.
    fn offers(&self, outpoint: &OutPoint) -> bool {
        self.storage.created_output(outpoint).is_some()
    }

    /// A pooled transaction's body, which storage holds.
    fn transaction_of(&self, pooled: &Pooled) -> &Transaction {
        self.storage
            .transactions()
            .get(&pooled.id.mined_id())
            .expect("storage holds every pooled transaction")
            .transaction
            .transaction()
    }

    fn admission_at(&mut self, key: u64) -> &mut Admission<E> {
        self.admissions
            .get_mut(&key)
            .expect("the caller names a live admission")
    }

    /// Moves the tip. Every running job verified for the old one.
    fn set_tip(&mut self, tip: ChainTip) {
        self.tip = tip;
        self.tip_at = self.fresh();
        self.has_tip = true;
        self.switch();
    }

    /// Turns the pool on or off, if what it was told calls for that.
    fn switch(&mut self) {
        let on = self.near_tip && self.has_tip;
        if on && !self.active {
            self.active = true;
        } else if !on && self.active {
            self.turn_off();
        }
    }

    /// Turns the pool off. Every undecided submission gets a verdict, and
    /// the pool empties.
    fn turn_off(&mut self) {
        let keys: Vec<u64> = self.admissions.keys().copied().collect();
        for key in keys {
            self.cancel_admission_job(key);
            self.decide(key, Verdict::LocalFault(LocalFault::Deactivated));
        }
        // Today's pool drops its downloads when it turns off. Every fetch
        // still out is told to stop, and its lease stays counted until its
        // caller drops the reservation.
        let mut fetching: Vec<ReservationId> = self.leases.keys().copied().collect();
        fetching.sort();
        self.step
            .effects
            .extend(fetching.into_iter().map(Effect::CancelFetch));
        self.owners.clear();
        let keys: Vec<u64> = self.pool.keys().copied().collect();
        for key in keys {
            self.remove_one(key, RemovalReason::Deactivated);
        }
        // What a reset took out left the pool then, and that is logged.
        self.queue.clear();
        self.queued.clear();
        self.queued_cost = 0;
        // Every pooled transaction was removed, so storage is empty and only
        // its rejection lists are left.
        self.storage.clear_rejections();
        debug_assert_eq!(
            self.storage.transaction_count(),
            0,
            "storage held only the pool"
        );
        self.active = false;
    }

    fn fresh(&mut self) -> u64 {
        self.next_id = self.next_id.saturating_add(1);
        self.next_id
    }
}

/// A rejection as callers see it.
fn rejection<E: VerifyError>(
    tip: ChainTip,
    scope: RejectionScope,
    charge: Charge,
    error: MempoolError<E>,
) -> Rejection {
    Rejection {
        scope,
        charge,
        tip: tip.height,
        detail: Arc::new(error),
    }
}

/// How long the rejection `error` describes is remembered.
fn scope_of<E>(error: &MempoolError<E>) -> RejectionScope {
    match error {
        MempoolError::StorageExactTip(_) | MempoolError::NonStandardTransaction(_) => {
            RejectionScope::ExactTip
        }
        MempoolError::StorageEffectsTip(_) => RejectionScope::EffectsTip,
        MempoolError::StorageEffectsChain(reason) => RejectionScope::EffectsChain(match reason {
            SameEffectsChainRejectionError::Expired => ChainReason::Expired,
            SameEffectsChainRejectionError::DuplicateSpend => ChainReason::DuplicateSpend,
            SameEffectsChainRejectionError::Mined => ChainReason::Mined,
        }),
        MempoolError::InMempool
        | MempoolError::AlreadyQueued
        | MempoolError::FullQueue
        | MempoolError::Disabled => RejectionScope::Uncached,
    }
}

/// The transactions whose outputs `tx` spends.
fn parents_of(tx: &Transaction) -> Vec<transaction::Hash> {
    let mut parents: Vec<transaction::Hash> = Vec::new();
    for outpoint in tx.spent_outpoints() {
        if !parents.contains(&outpoint.hash) {
            parents.push(outpoint.hash);
        }
    }
    parents
}
