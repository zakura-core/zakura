//! The pool core's state machine.
//!
//! [`MempoolState`] holds the node's unmined transactions and decides every
//! submission. It is pure. Each input carries the time and returns what it
//! caused as a [`Step`]. It runs no verification and touches no channel. The
//! core's task runs it behind the port in `zakura_node_services::mempool::port`.
//!
//! Its rules are the port's, which `zakura_node_services::mempool::port::model`
//! restates as an independent model. The machine tests drive both with the
//! same inputs and compare them. Two rules belong to storage and the model
//! leaves them out: eviction at the cost limit and mining order.
//!
//! It knows nothing about peers. Per-peer limits and cooldowns belong to the
//! callers that see the peer. A verdict says what its body earned, and the
//! caller applies that to whoever sent it.
//!
//! [`Storage`] holds exactly the visible pool. A transaction hidden while it is
//! verified again leaves storage and waits here with its body. So the
//! dependency limits, the conflicts storage checks and the snapshot are about
//! the visible pool only, as the model's are.
//!
//! Every input enters the same way. It panics if the last input's effects were
//! not taken, moves the clock, and applies every deadline that is due. Every
//! input leaves the same way too, through [`MempoolState::take_step`], which
//! starts the revalidations that can start. An input that answers its caller,
//! such as [`MempoolState::reserve`], leaves its other effects for
//! [`MempoolState::effects`], which the caller takes straight after.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use chrono::{DateTime, Utc};
use zakura_chain::{
    block::Height,
    ironwood, orchard, sapling, sprout,
    transaction::{self, Transaction, UnminedTx, UnminedTxId, VerifiedUnminedTx},
    transparent::{self, OutPoint},
};
use zakura_node_services::mempool::port::{
    ChainReason, ChainTip, ChainTransition, Change, ChangeSeq, Charge, Generation, GenerationRead,
    Intent, LocalFault, PoolSnapshot, Rejection, RejectionDetail, RejectionScope, RemovalReason,
    ReservationId, Unreserved, Verdict, MAX_OUTPUT_WAITS, MAX_PENDING, MAX_REVALIDATING,
    MAX_UNDECIDED, MAX_WAITERS, REVALIDATE_RETRY, REVALIDATE_RETRY_MAX, T_PARK, T_VERIFY,
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
    /// These transparent inputs are in neither the chain nor the visible
    /// pool. `error` is the verifier's error, cached for these exact bytes if
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

/// A granted reservation. The core's task turns it into a
/// `zakura_node_services::mempool::port::Reservation`.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Grant {
    /// Its name.
    pub id: ReservationId,
    /// Whether it joined an id that was already undecided.
    pub joined: bool,
    /// Whether the core needs the body from this caller.
    pub wants_body: bool,
}

/// One thing the core does because of an input.
#[derive(Clone, Debug)]
pub enum Effect {
    /// Start a verification of this transaction at the machine's tip.
    Start {
        /// The job.
        job: JobId,
        /// The transaction.
        tx: UnminedTx,
        /// Whether it is a pooled transaction being verified again.
        revalidation: bool,
        /// Whether the chain was reset since its body arrived, by a reorg, a
        /// rules change or a missed event. Today's pool charges no cooldown
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
    /// Answer a caller waiting for an output, with the output a transaction
    /// that just became visible creates.
    OutputReady {
        /// The waiting caller.
        waiter: WaiterId,
        /// The outpoint it waited for.
        outpoint: OutPoint,
        /// The output.
        output: transparent::Output,
    },
    /// Tell a caller waiting for an output that the pool turned off.
    OutputWaitEnded(WaiterId),
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
    pub fn verdict(&self, waiter: WaiterId) -> Option<&Verdict> {
        self.effects.iter().find_map(|effect| match effect {
            Effect::Verdict { waiter: w, verdict } if *w == waiter => Some(verdict),
            _ => None,
        })
    }
}

/// The answer to [`MempoolState::await_output`].
#[derive(Clone, Debug)]
pub enum OutputAnswer {
    /// A visible pooled transaction creates the output.
    Ready(transparent::Output),
    /// Nothing visible creates it. The caller waits under this name.
    Waiting(WaiterId),
    /// Too many distinct outpoints are being waited on.
    TooManyWaiters,
    /// The pool is off.
    Inactive,
}

/// The machine's limits. Production uses the port's constants. Tests use
/// smaller ones so a short run reaches every full state.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Limits {
    /// Undecided transactions node-wide, [`MAX_PENDING`].
    pub pending: usize,
    /// Running revalidations, [`MAX_REVALIDATING`].
    pub revalidating: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            pending: MAX_PENDING,
            revalidating: MAX_REVALIDATING,
        }
    }
}

/// One submission waiting for a verdict.
#[derive(Clone, Debug)]
struct Waiter {
    id: WaiterId,
}

/// Where an undecided transaction is.
#[derive(Clone, Debug)]
enum Stage {
    /// No body has arrived.
    Reserved,
    /// A job is running.
    Verifying { job: JobId, deadline: Instant },
    /// The last outcome was stale, and the chain event that made it stale has
    /// not been applied yet. The job runs again when it has.
    AwaitingChain,
    /// Parents are missing.
    Parked { on: Vec<transaction::Hash> },
}

/// An undecided transaction and everyone waiting on it.
#[derive(Clone, Debug)]
struct Admission<E> {
    tx_id: UnminedTxId,
    owner: ReservationId,
    /// Whether the owner can still submit. False once it has submitted or
    /// released.
    owner_waiting: bool,
    body: Option<UnminedTx>,
    waiters: Vec<Waiter>,
    stage: Stage,
    /// When one of its inputs was first found missing. Its wait for a parent
    /// runs from here and is never restarted.
    missing_since: Option<Instant>,
    /// The verifier's error for its missing inputs, cached if the wait ends.
    missing_error: Option<E>,
    /// Whether the one check allowed after that wait ended has been used.
    last_check_used: bool,
    /// When its body arrived. Its slot is held at most [`MAX_UNDECIDED`] from
    /// here.
    body_at: Option<Instant>,
    /// Whether the chain was reset since it was reserved. See
    /// [`Effect::Start`].
    after_reset: bool,
}

/// Where a hidden pooled transaction is.
#[derive(Clone, Debug)]
enum Hidden {
    /// Waiting for a slot, or for a hidden parent to pass first.
    Queued,
    Running {
        job: JobId,
        deadline: Instant,
    },
    AwaitingChain,
    Retry {
        at: Instant,
    },
    /// Parents are missing.
    Parked {
        on: Vec<transaction::Hash>,
    },
}

/// What the machine keeps about a pooled transaction. A visible one's body is
/// in storage. A hidden one's body is here.
#[derive(Clone, Debug)]
struct Pooled<E> {
    id: UnminedTxId,
    /// The transactions whose outputs it spends.
    parents: Vec<transaction::Hash>,
    expiry: Option<Height>,
    hidden: Option<Hidden>,
    /// The body while it is hidden.
    body: Option<VerifiedUnminedTx>,
    /// The wait before the next try after a local fault.
    backoff: Duration,
    /// The machine's event count when it last became visible.
    visible_at: u64,
    /// The log position of its latest `Hidden` entry.
    hidden_at: ChangeSeq,
    /// When one of its inputs was first found missing since it was last
    /// visible. Its wait for a parent runs from here.
    missing_since: Option<Instant>,
    /// The verifier's error for its missing inputs.
    missing_error: Option<E>,
}

/// What a job verifies.
#[derive(Copy, Clone, Debug)]
enum JobTarget {
    Admission(UnminedTxId),
    Revalidation(transaction::Hash),
}

/// A reservation that joined an undecided id.
#[derive(Clone, Debug)]
struct JoinTicket {
    /// The admission it joined. A later admission of the same id is another
    /// one, and this ticket does not reach it.
    admission: u64,
    /// Whether the core asked this caller for the body.
    brings: bool,
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
            spends.outpoints.extend(tx.spent_outpoints());
            spends.sprout.extend(tx.sprout_nullifiers().copied());
            spends.sapling.extend(tx.sapling_nullifiers().copied());
            spends.orchard.extend(tx.orchard_nullifiers().copied());
            spends.ironwood.extend(tx.ironwood_nullifiers().copied());
        }
        spends
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

/// The pool core's rules, over the node's real transactions and storage.
pub struct MempoolState<E> {
    active: bool,
    now: Instant,
    applied: Generation,
    tip: ChainTip,
    next_id: u64,
    limits: Limits,
    /// The visible pool, and the rejection lists.
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
    tickets: BTreeMap<ReservationId, JoinTicket>,
    /// Joined reservations whose admission ended before their caller came
    /// back, with the verdict the admission got, which the caller gets when
    /// it comes back. Each keeps its id too, so a wrong body under it is
    /// still told apart.
    ended_joins: HashMap<ReservationId, (UnminedTxId, Verdict)>,
    /// Every pooled transaction, visible or hidden, oldest first.
    pool: BTreeMap<u64, Pooled<E>>,
    pooled: HashMap<transaction::Hash, u64>,
    /// The hidden pooled transactions, which are the only ones with
    /// deadlines. Empty in steady state.
    hidden: BTreeSet<u64>,
    /// The hidden pooled transactions queued for a revalidation.
    queued: BTreeSet<u64>,
    /// Running revalidations.
    running: usize,
    /// The cost of the hidden transactions' bodies. They count against the
    /// pool's cost limit with what storage holds, so hiding the pool and
    /// refilling it cannot grow memory past the limit.
    hidden_cost: u64,
    /// The pooled transactions that spend each transaction's outputs,
    /// visible or hidden, so a removal finds its dependents without a scan.
    /// Keyed by pool key, so a removal is logarithmic however many children
    /// one confirmed parent has, and dependents are found oldest first.
    children: HashMap<transaction::Hash, BTreeMap<u64, transaction::Hash>>,
    /// Each job, with the event count when it started.
    jobs: HashMap<JobId, (JobTarget, u64)>,
    /// Callers waiting for an output, by outpoint, in the order they came.
    output_waiters: HashMap<OutPoint, Vec<WaiterId>>,
    /// Changes returned in earlier steps. Each step is one batch of the log,
    /// so this is the log position of the last one.
    logged: u64,
    step: Step,
}

impl<E: VerifyError> MempoolState<E> {
    /// An active, empty pool at `tip` and `generation`.
    pub fn new(config: &Config, tip: ChainTip, generation: Generation, now: Instant) -> Self {
        Self {
            active: true,
            now,
            applied: generation,
            tip,
            next_id: 0,
            limits: Limits::default(),
            storage: Storage::new(config),
            admissions: BTreeMap::new(),
            admission_of: HashMap::default(),
            owners: HashMap::default(),
            leases: HashMap::default(),
            tickets: BTreeMap::new(),
            ended_joins: HashMap::default(),
            pool: BTreeMap::new(),
            pooled: HashMap::default(),
            hidden: BTreeSet::new(),
            queued: BTreeSet::new(),
            running: 0,
            hidden_cost: 0,
            children: HashMap::default(),
            jobs: HashMap::default(),
            output_waiters: HashMap::default(),
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
    /// `PoolHandle::reserve`. What else this caused is in [`Self::effects`].
    pub fn reserve(
        &mut self,
        tx_id: UnminedTxId,
        intent: Intent,
        now: Instant,
    ) -> Result<Grant, Unreserved> {
        self.enter(now);
        if !self.active {
            return Err(Unreserved::Inactive);
        }
        // A slot a deadline just freed goes to a revalidation that was
        // already waiting for one, before this reservation counts slots.
        self.start_revalidations();
        if self.pooled.contains_key(&tx_id.mined_id()) {
            return Err(Unreserved::Pooled);
        }
        if let Some(error) = self.cached_rejection(tx_id) {
            return Err(Unreserved::Rejected(rejection(
                scope_of(&error),
                Charge::None,
                error,
            )));
        }
        if let Some(key) = self.admission_of.get(&tx_id).copied() {
            return self.join(key, intent);
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
                owner,
                owner_waiting: true,
                body: None,
                waiters: Vec::new(),
                stage: Stage::Reserved,
                missing_since: None,
                missing_error: None,
                last_check_used: false,
                body_at: None,
                after_reset: false,
            },
        );
        self.admission_of.insert(tx_id, key);
        self.owners.insert(owner, key);
        Ok(Grant {
            id: owner,
            joined: false,
            wants_body: true,
        })
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
        // An id that another caller's body already decided has its outcome
        // in the log. A late failure of this fetch adds nothing to it. Only
        // the reserving caller's failure ends the admission, so only it is
        // logged: a joined caller that leaves while the admission goes on
        // changes nothing a reader sees.
        let owner = self.owners.contains_key(&reservation);
        if let Some(tx_id) = self.undecided_for(reservation).filter(|_| owner) {
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
        let tx_id = tx.id();
        self.arrive(reservation, tx_id, Some(tx), now)
    }

    /// A joined reservation that does not want the body: the caller waits for
    /// the verdict of the body the core has or is being brought.
    pub fn wait(
        &mut self,
        reservation: ReservationId,
        tx_id: UnminedTxId,
        now: Instant,
    ) -> (WaiterId, Step) {
        self.arrive(reservation, tx_id, None, now)
    }

    /// The outcome of `job`, with the generation read before its first state
    /// lookup and after its last. `wall` is the wall-clock time, stamped on a
    /// transaction that enters the pool.
    pub fn outcome(
        &mut self,
        job: JobId,
        outcome: Outcome<E>,
        before: GenerationRead,
        after: GenerationRead,
        now: Instant,
        wall: DateTime<Utc>,
    ) -> Step {
        self.enter(now);
        let Some((target, started_at)) = self.jobs.remove(&job) else {
            // Cancelled, or timed out.
            return self.take_step();
        };
        let fresh = before.brackets(after, self.applied);
        // A stale outcome waits for the chain event only when that event is
        // certainly still to come. In every other case the job runs again at
        // once, so no work waits for an event that has already been applied.
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
                    self.admission_outcome(key, outcome, started_at, wall);
                }
            }
            JobTarget::Revalidation(hash) => {
                let key = self.pooled[&hash];
                if !fresh {
                    let next = if chain_is_ahead {
                        Hidden::AwaitingChain
                    } else {
                        Hidden::Queued
                    };
                    self.set_hidden(key, Some(next));
                } else {
                    self.revalidation_outcome(key, hash, outcome, started_at, wall);
                }
            }
        }
        self.take_step()
    }

    /// The next chain transition.
    pub fn transition(&mut self, transition: ChainTransition, now: Instant) -> Step {
        self.enter(now);
        if !self.active {
            return self.take_step();
        }
        let now = self.now;
        self.applied = transition.generation;
        self.tip = transition.after;
        // After a reorg or an activation the pool is verified again, and a
        // rejection made on the old chain or under the old rules says
        // nothing. Today's reset drops every list too.
        let verify_again = transition.is_reorg() || transition.rules_change;
        if verify_again {
            self.mark_reset();
            self.storage.clear_rejections();
        } else {
            self.storage.clear_tip_rejections();
        }
        // What the whole batch mined, and what in the pool conflicts with it,
        // are worked out before anything is removed. A pooled transaction that
        // conflicts with a later block transaction is then found even if an
        // earlier one already took its parent away.
        let mined: Vec<transaction::Hash> = transition
            .connected
            .iter()
            .flat_map(|block| block.transaction_hashes.iter().copied())
            .collect();
        let mined_set: HashSet<transaction::Hash> = mined.iter().copied().collect();
        let spends = Spends::of(
            transition
                .connected
                .iter()
                .flat_map(|block| block.transactions.iter().map(|tx| &**tx)),
        );
        let conflicting: Vec<UnminedTxId> = self
            .pool
            .values()
            .filter(|pooled| !mined_set.contains(&pooled.id.mined_id()))
            .filter(|pooled| {
                self.pooled_transaction(pooled)
                    .is_some_and(|tx| spends.conflict_with(tx))
            })
            .map(|pooled| pooled.id)
            .collect();
        for hash in &mined {
            if let Some(key) = self.pooled.get(hash).copied() {
                // Its dependents stay. Their parent is on the chain now.
                self.remove_one(key, RemovalReason::Mined);
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
            if admission.owner_waiting {
                let owner = admission.owner;
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
        // Every block of this transition was seen, and what they mined is gone
        // already. So an expired transaction here was not mined, and what
        // depends on it goes with it.
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

    /// The transition queue overflowed. The core no longer knows which blocks
    /// changed, so every pooled and parked transaction is checked again at
    /// this tip.
    pub fn resync(&mut self, tip: ChainTip, generation: Generation, now: Instant) -> Step {
        self.enter(now);
        if !self.active {
            return self.take_step();
        }
        self.applied = generation;
        self.tip = tip;
        self.mark_reset();
        self.storage.clear_rejections();
        // Expiry needs no verifier, only the new tip. Only the expired
        // transaction goes. A block the core missed may have mined it first,
        // and then what spends its outputs is still valid, so its dependents
        // are checked again like everything else.
        self.remove_expired(false);
        self.hide_all();
        self.rerun_parked();
        self.chain_applied();
        self.take_step()
    }

    /// Time passed. Applies every deadline at or before `now`.
    pub fn tick(&mut self, now: Instant) -> Step {
        self.enter(now);
        self.take_step()
    }

    /// Wait for a visible pooled transaction to create `outpoint`. The lookup
    /// and the registration are one input, so no admission can fall between
    /// them. What else this caused is in [`Self::effects`].
    ///
    /// `closed` says which earlier waiters have stopped waiting. Those for
    /// `outpoint` are dropped before this one registers, and all of them are
    /// dropped before a new outpoint is refused for the limit.
    pub fn await_output(
        &mut self,
        outpoint: OutPoint,
        now: Instant,
        mut closed: impl FnMut(WaiterId) -> bool,
    ) -> OutputAnswer {
        self.enter(now);
        self.start_revalidations();
        if !self.active {
            return OutputAnswer::Inactive;
        }
        if let Some(output) = self.storage.created_output(&outpoint) {
            return OutputAnswer::Ready(output);
        }
        if let Some(waiters) = self.output_waiters.get_mut(&outpoint) {
            waiters.retain(|waiter| !closed(*waiter));
        }
        if !self.output_waiters.contains_key(&outpoint)
            && self.output_waiters.len() >= MAX_OUTPUT_WAITS
        {
            self.output_waiters.retain(|_, waiters| {
                waiters.retain(|waiter| !closed(*waiter));
                !waiters.is_empty()
            });
            if self.output_waiters.len() >= MAX_OUTPUT_WAITS {
                return OutputAnswer::TooManyWaiters;
            }
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

    /// The node fell far behind the tip. Every undecided submission gets a
    /// verdict, and the pool empties.
    pub fn deactivate(&mut self, now: Instant) -> Step {
        self.enter(now);
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
        self.tickets.clear();
        self.owners.clear();
        let keys: Vec<u64> = self.pool.keys().copied().collect();
        for key in keys {
            self.remove_one(key, RemovalReason::Deactivated);
        }
        // Every pooled transaction was removed, so storage is empty and only
        // its rejection lists are left.
        self.storage.clear_rejections();
        debug_assert_eq!(
            self.storage.transaction_count(),
            0,
            "storage held only the pool"
        );
        debug_assert_eq!(self.hidden_cost, 0, "every hidden transaction was removed");
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

    /// The node is near the tip again. The pool starts empty. What this
    /// caused is in [`Self::effects`].
    pub fn activate(&mut self, tip: ChainTip, generation: Generation, now: Instant) {
        self.enter(now);
        self.active = true;
        self.tip = tip;
        self.applied = generation;
        self.start_revalidations();
    }

    /// The members of `ids` a cached rejection covers. Takes no slot. What
    /// else this caused is in [`Self::effects`].
    pub fn rejected_ids(
        &mut self,
        ids: HashSet<UnminedTxId>,
        now: Instant,
    ) -> HashSet<UnminedTxId> {
        self.enter(now);
        self.start_revalidations();
        self.storage.rejected_transactions(ids, self.now).collect()
    }

    /// What the last input that answers its caller caused beside its answer:
    /// the verdicts of deadlines it found due, and the jobs it started. The
    /// caller takes this straight after [`Self::reserve`],
    /// [`Self::await_output`], [`Self::rejected_ids`] and
    /// [`Self::activate`]. The next input panics if it was not taken.
    pub fn effects(&mut self) -> Step {
        self.take_step()
    }

    // Observations.

    /// Whether the pool is on.
    pub fn is_active(&self) -> bool {
        self.active
    }

    /// The tip the pool is valid for.
    pub fn tip(&self) -> ChainTip {
        self.tip
    }

    /// The generation the machine has applied.
    pub fn generation(&self) -> Generation {
        self.applied
    }

    /// The visible pool as a snapshot, for the change log position
    /// `last_change`.
    pub fn snapshot(&self, last_change: ChangeSeq) -> PoolSnapshot {
        if !self.active {
            return PoolSnapshot::inactive(self.tip, self.applied, last_change);
        }
        PoolSnapshot::new(
            self.tip,
            self.applied,
            last_change,
            self.storage.mining_transactions().cloned().collect(),
            self.storage.transaction_dependencies().clone(),
        )
    }

    /// What storage holds, for the pool's metrics.
    pub fn storage_stats(&self) -> StorageStats {
        self.storage.stats()
    }

    /// The earliest time an input would apply a deadline, if any is set.
    pub fn next_deadline(&self) -> Option<Instant> {
        let admissions = self.admissions.values().flat_map(|admission| {
            let stage = match &admission.stage {
                Stage::Verifying { deadline, .. } => Some(*deadline),
                Stage::Parked { .. } => admission.missing_since.map(|since| since + T_PARK),
                _ => None,
            };
            [stage, admission.body_at.map(|at| at + MAX_UNDECIDED)]
        });
        let hidden = self.hidden.iter().filter_map(|key| {
            let pooled = &self.pool[key];
            match &pooled.hidden {
                Some(Hidden::Running { deadline, .. }) => Some(*deadline),
                Some(Hidden::Retry { at }) => Some(*at),
                Some(Hidden::Parked { .. }) => pooled.missing_since.map(|since| since + T_PARK),
                _ => None,
            }
        });
        admissions.flatten().chain(hidden).min()
    }

    /// Undecided transactions, plus leases, plus running revalidations.
    pub fn slots_in_use(&self) -> usize {
        self.admissions
            .len()
            .saturating_add(self.leases.len())
            .saturating_add(self.running)
    }

    /// Reservations callers still hold, of every kind.
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
            .map(|pooled| pooled.id)
            .collect()
    }

    /// The pooled transactions hidden while they are verified again.
    pub fn hidden(&self) -> Vec<UnminedTxId> {
        self.hidden.iter().map(|key| self.pool[key].id).collect()
    }

    /// Whether `tx_id` is parked on a missing parent.
    pub fn is_parked(&self, tx_id: UnminedTxId) -> bool {
        self.admission_of
            .get(&tx_id)
            .and_then(|key| self.admissions.get(key))
            .is_some_and(|admission| matches!(admission.stage, Stage::Parked { .. }))
    }

    /// Whether the chain was reset since the body of `job`'s transaction
    /// arrived, as of now. The core asks this when it starts the job, since
    /// a reset can land after the job's start effect was made and before the
    /// core carries it out. False for a revalidation or a job that is gone.
    pub fn job_after_reset(&self, job: JobId) -> bool {
        match self.jobs.get(&job) {
            Some((JobTarget::Admission(tx_id), _)) => self
                .admission_of
                .get(tx_id)
                .and_then(|key| self.admissions.get(key))
                .is_some_and(|admission| admission.after_reset),
            _ => false,
        }
    }

    /// Whether any undecided transaction is parked on a missing parent.
    pub fn is_parked_any(&self) -> bool {
        self.admissions
            .values()
            .any(|admission| matches!(admission.stage, Stage::Parked { .. }))
    }

    /// Distinct outpoints callers are waiting for.
    pub fn output_waits(&self) -> usize {
        self.output_waiters.len()
    }

    /// Checks the invariants every reachable state keeps.
    ///
    /// # Panics
    ///
    /// If one is broken.
    pub fn assert_invariants(&self) {
        assert!(
            self.step.is_empty(),
            "an input's effects were left uncollected: {:?}",
            self.step
        );
        assert!(
            self.slots_in_use() <= self.limits.pending,
            "pool/admissions-within-limits: the node's limit"
        );
        for admission in self.admissions.values() {
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
        assert!(
            self.running <= self.limits.revalidating,
            "the revalidation share"
        );
        let hidden: BTreeSet<u64> = self
            .pool
            .iter()
            .filter(|(_, pooled)| pooled.hidden.is_some())
            .map(|(key, _)| *key)
            .collect();
        assert_eq!(hidden, self.hidden, "the hidden index matches the pool");
        let queued: BTreeSet<u64> = self
            .pool
            .iter()
            .filter(|(_, pooled)| matches!(pooled.hidden, Some(Hidden::Queued)))
            .map(|(key, _)| *key)
            .collect();
        assert_eq!(queued, self.queued, "the queue index matches the pool");
        let running = self
            .pool
            .values()
            .filter(|pooled| matches!(pooled.hidden, Some(Hidden::Running { .. })))
            .count();
        assert_eq!(running, self.running, "the running count matches the pool");
        let hidden_cost: u64 = self
            .pool
            .values()
            .filter_map(|pooled| pooled.body.as_ref())
            .map(VerifiedUnminedTx::cost)
            .sum();
        assert_eq!(
            hidden_cost, self.hidden_cost,
            "the hidden cost matches the pool"
        );
        assert!(
            self.storage.total_cost().saturating_add(self.hidden_cost) <= self.storage.cost_limit(),
            "the visible and hidden pool together stay within the cost limit"
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
        }
        for pooled in self.pool.values() {
            assert_eq!(
                pooled.body.is_some(),
                pooled.hidden.is_some(),
                "a hidden transaction holds its body, and a visible one is in storage"
            );
            assert_eq!(
                self.storage.contains(&pooled.id.mined_id()),
                pooled.hidden.is_none(),
                "storage holds exactly the visible pool"
            );
            assert!(
                pooled.expiry.is_none_or(|expiry| self.tip.height < expiry),
                "pool/consistent: no expired transaction"
            );
        }
        assert_eq!(
            self.storage.transaction_count(),
            self.pool.len() - self.hidden.len(),
            "storage holds exactly the visible pool"
        );
        for ticket in self.tickets.values() {
            assert!(
                self.admissions.contains_key(&ticket.admission),
                "a joined reservation names a live admission"
            );
        }
    }

    // Rules.

    /// Moves time to `now` and applies every deadline that is due, before the
    /// input that brought the time is handled.
    fn enter(&mut self, now: Instant) {
        // An input's effects belong to that input. If the caller has not taken
        // the last one's, they would be handed over as part of this one, and
        // nothing could tell that they came late.
        assert!(
            self.step.is_empty(),
            "the previous input's effects were not collected before the next input: {:?}",
            self.step
        );
        // The machine's own clock never goes back.
        if now > self.now {
            self.now = now;
        }
        let now = self.now;
        self.storage.prune_rejections(now);
        // Each admission can have a deadline for its stage and one for its
        // slot. Whichever was due first decides it. When both were due at the
        // same moment, the stage's own deadline decides.
        let mut timed_out = Vec::new();
        let mut unparked = Vec::new();
        for (key, admission) in &self.admissions {
            let slot_due = admission.body_at.map(|at| at + MAX_UNDECIDED);
            let (stage_due, parked) = match &admission.stage {
                Stage::Verifying { deadline, .. } => (Some(*deadline), false),
                Stage::Parked { .. } => (admission.missing_since.map(|since| since + T_PARK), true),
                _ => (None, false),
            };
            let stage_is_due = stage_due.is_some_and(|due| due <= now);
            let slot_is_due = slot_due.is_some_and(|due| due <= now);
            let stage_first = match (stage_due, slot_due) {
                (Some(stage), Some(slot)) => stage <= slot,
                (Some(_), None) => true,
                _ => false,
            };
            if stage_is_due && (stage_first || !slot_is_due) {
                if parked {
                    unparked.push(*key);
                } else {
                    timed_out.push(*key);
                }
            } else if slot_is_due {
                timed_out.push(*key);
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
        let keys: Vec<u64> = self.hidden.iter().copied().collect();
        for key in keys {
            // An earlier removal in this loop can have taken this one with it,
            // as a dependent.
            let Some((hidden, parked_out)) = self.pool.get(&key).map(|pooled| {
                (
                    pooled.hidden.clone(),
                    pooled
                        .missing_since
                        .is_some_and(|since| since + T_PARK <= now),
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
                    self.set_hidden(key, Some(Hidden::Queued));
                }
                // Its parent never came back.
                Some(Hidden::Parked { .. }) if parked_out => {
                    let pooled = self.pooled_at(key);
                    let tx_id = pooled.id;
                    let error = pooled
                        .missing_error
                        .clone()
                        .expect("a parked transaction holds the verifier's missing-input error");
                    self.remove_with_dependents(tx_id.mined_id(), RemovalReason::Invalidated);
                    // Remembered like a new transaction's missing parent.
                    self.storage
                        .reject_exact(tx_id, ExactTipRejectionError::FailedVerification(error));
                }
                _ => {}
            }
        }
    }

    /// The one way out of an input that returns a step. Queued revalidations
    /// that can start are started first, so no input can free a slot, or
    /// bring a retry due, and leave it unused until something else happens.
    fn take_step(&mut self) -> Step {
        if self.active {
            self.start_revalidations();
        }
        let step = std::mem::take(&mut self.step);
        let changes = u64::try_from(step.changes.len()).expect("a batch fits in u64");
        self.logged = self.logged.saturating_add(changes);
        step
    }

    /// A body, or a wait for one, under `reservation`.
    fn arrive(
        &mut self,
        reservation: ReservationId,
        tx_id: UnminedTxId,
        body: Option<UnminedTx>,
        now: Instant,
    ) -> (WaiterId, Step) {
        self.enter(now);
        let now = self.now;
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
        let (key, body) = match self.tickets.remove(&reservation) {
            // A joined caller that was not asked for the body sends none, as
            // the handle drops it.
            Some(ticket) => (Some(ticket.admission), body.filter(|_| ticket.brings)),
            None => (self.owners.remove(&reservation), body),
        };
        // The admission the reservation was granted for, if it is still
        // undecided. A later admission of the same id has another key.
        let key = key.filter(|key| {
            self.admissions
                .get(key)
                .is_some_and(|admission| admission.tx_id == tx_id)
        });
        let Some(key) = key else {
            // The admission this reservation was granted for ended before
            // its caller came back. The caller gets that admission's verdict,
            // as if it had come in time.
            let ended = self
                .leases
                .remove(&reservation)
                .map(|lease| lease.verdict)
                .or_else(|| {
                    self.ended_joins
                        .remove(&reservation)
                        .map(|(_, verdict)| verdict)
                });
            let verdict = ended.unwrap_or_else(|| self.stale_verdict(tx_id));
            self.step.effects.push(Effect::Verdict { waiter, verdict });
            return (waiter, self.take_step());
        };
        let start = {
            let admission = self.admission_at(key);
            if admission.owner == reservation {
                admission.owner_waiting = false;
            }
            admission.waiters.push(Waiter { id: waiter });
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

    /// The id a reservation was granted for, for as long as the machine knows
    /// the reservation.
    fn reserved_for(&self, reservation: ReservationId) -> Option<UnminedTxId> {
        self.undecided_for(reservation)
            .or_else(|| self.leases.get(&reservation).map(|lease| lease.tx_id))
            .or_else(|| self.ended_joins.get(&reservation).map(|(id, _)| *id))
    }

    /// Ends a reservation that brought no body: frees its lease, or its place
    /// in an admission, and ends that admission if nobody can still bring its
    /// body.
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

    /// Joins an id that is already undecided, if there is a place among its
    /// waiters. Whether one peer may wait on an id twice is its caller's
    /// rule.
    fn join(&mut self, key: u64, intent: Intent) -> Result<Grant, Unreserved> {
        if intent == Intent::Fetch {
            return Err(Unreserved::Pending);
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
        if waiting >= MAX_WAITERS {
            return Err(Unreserved::Pending);
        }
        // A joined caller is asked for the body only when nobody else can
        // bring it: the core has none, the reserving caller is not still
        // fetching or delivering it, and no other joined caller was asked.
        // Otherwise a second decoded body would wait under the same slot.
        // So a delivery
        // that joins while the fetch is out waits for the fetch's verdict,
        // and its own copy is dropped, as today drops it.
        let brings = admission.body.is_none()
            && !admission.owner_waiting
            && !tickets.iter().any(|ticket| ticket.brings);
        let id = ReservationId(self.fresh());
        self.tickets.insert(
            id,
            JoinTicket {
                admission: key,
                brings,
            },
        );
        Ok(Grant {
            id,
            joined: true,
            wants_body: brings,
        })
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
                if self.conflicts_with_hidden(tx.transaction.transaction()) {
                    // It conflicts with a transaction that is hidden because
                    // nobody knows yet whether it is still valid. If that one
                    // fails, this one was fine. So nothing is remembered.
                    self.fail_locally(key, LocalFault::PoolUnsettled);
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
                        hidden: None,
                        body: None,
                        backoff: REVALIDATE_RETRY,
                        visible_at: pool_key,
                        hidden_at: ChangeSeq(0),
                        missing_since: None,
                        missing_error: None,
                    },
                );
                self.pooled.insert(hash, pool_key);
                self.offer_outputs(tx.transaction.transaction(), hash);
                self.decide(key, Verdict::Admitted);
                self.wake(hash);
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

    /// Applies a fresh outcome to a hidden pooled transaction.
    fn revalidation_outcome(
        &mut self,
        key: u64,
        hash: transaction::Hash,
        outcome: Outcome<E>,
        started_at: u64,
        wall: DateTime<Utc>,
    ) {
        let now = self.now;
        match outcome {
            Outcome::Pass { tx, pool_inputs } => {
                if !pool_inputs.iter().all(|input| self.offers(input)) {
                    // Verified against a pool that has since lost that output.
                    self.set_hidden(key, Some(Hidden::Queued));
                    return;
                }
                let pooled = self.pooled_at(key);
                let (tx_id, expiry) = (pooled.id, pooled.expiry);
                // Keep the time it first entered the pool, for mining order.
                let time = pooled
                    .body
                    .as_ref()
                    .and_then(|body| body.time)
                    .unwrap_or(wall);
                // Nothing expired becomes visible.
                if self.reached(expiry) {
                    self.set_hidden(key, Some(Hidden::Queued));
                    self.remove_with_dependents(hash, RemovalReason::Expired);
                    self.storage.reject_effects_chain(
                        hash,
                        SameEffectsChainRejectionError::Expired,
                        now,
                    );
                    return;
                }
                // Today's insertion limits, as for a new transaction. A parent
                // that was mined, then put back by a reorg, can have gained
                // more pooled children than one group holds.
                // While storage decides, this body is not also counted as
                // held outside it. Its cost moves to storage only if storage
                // takes it, and a removal below takes it off once otherwise.
                let cost = self
                    .pooled_at(key)
                    .body
                    .as_ref()
                    .map_or(0, VerifiedUnminedTx::cost);
                self.storage
                    .set_outside_cost(self.hidden_cost.saturating_sub(cost));
                // A rejection cached for another authorization of these
                // effects while this one was pooled says nothing about it, so
                // the lists are not checked, as the model's reveal does not.
                let (result, evicted) = self.storage.reinsert_with_evicted_ids(
                    tx.clone(),
                    pool_inputs,
                    Some(self.tip.height),
                    now,
                    time,
                );
                self.forget_evicted(evicted);
                if result.is_err() {
                    self.storage.set_outside_cost(self.hidden_cost);
                    // Storage has cached what it refused.
                    self.set_hidden(key, Some(Hidden::Queued));
                    self.remove_with_dependents(hash, RemovalReason::Invalidated);
                    return;
                }
                self.add_hidden_cost(cost, false);
                let visible_at = self.fresh();
                self.set_hidden(key, None);
                let pooled = self.pooled_at(key);
                // Storage holds it now, and counts its cost.
                pooled.body = None;
                pooled.backoff = REVALIDATE_RETRY;
                pooled.visible_at = visible_at;
                pooled.missing_since = None;
                pooled.missing_error = None;
                let hidden_at = pooled.hidden_at;
                self.step.changes.push(Change::Revealed {
                    id: tx_id,
                    hidden_at,
                });
                // Its outputs are available again, to waiters as well.
                self.offer_outputs(tx.transaction.transaction(), hash);
                self.wake(hash);
            }
            // A block the core never saw mined it. Its dependents stay and are
            // checked against the chain when their turn comes.
            Outcome::AlreadyMined(_) => {
                // Its job has reported, so there is nothing to cancel.
                self.set_hidden(key, Some(Hidden::Queued));
                self.remove_one(key, RemovalReason::Mined);
                // Found by a lookup, not in a block the core saw connect, so it
                // is not cached.
                self.wake(hash);
            }
            // Its parent is in neither the chain nor the visible pool. After a
            // reorg that can be a mined parent that is on its way back, so it
            // waits as a new transaction would, still hidden.
            Outcome::MissingInputs { missing, error } => {
                let arrived = missing
                    .iter()
                    .any(|outpoint| self.arrived_since(outpoint, started_at));
                let pooled = self.pooled_at(key);
                pooled.missing_error = Some(error.clone());
                let since = *pooled.missing_since.get_or_insert(now);
                let wait_over = since + T_PARK <= now;
                let tx_id = pooled.id;
                // An input arrived during this check, so its answer is out of
                // date. A hidden transaction holds no slot, so it is checked
                // again, however late.
                if arrived {
                    self.set_hidden(key, Some(Hidden::Queued));
                    return;
                }
                if wait_over {
                    self.set_hidden(key, Some(Hidden::Queued));
                    self.remove_with_dependents(hash, RemovalReason::Invalidated);
                    self.storage
                        .reject_exact(tx_id, ExactTipRejectionError::FailedVerification(error));
                    return;
                }
                let mut on: Vec<transaction::Hash> = Vec::new();
                for outpoint in missing {
                    if !on.contains(&outpoint.hash) {
                        on.push(outpoint.hash);
                    }
                }
                self.set_hidden(key, Some(Hidden::Parked { on }));
            }
            Outcome::Fail { error, .. } => {
                let tx_id = self.pooled_at(key).id;
                self.set_hidden(key, Some(Hidden::Queued));
                self.remove_with_dependents(hash, RemovalReason::Invalidated);
                // Nobody is charged for a transaction that passed once. The
                // rejection is still remembered, so it is not fetched again at
                // once.
                self.storage.reject(tx_id, error, now);
            }
            Outcome::LocalFault(_) => self.retry_later(key),
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
            Verdict::Rejected(rejection(scope_of(&error), charge, error)),
        );
    }

    /// Refuses an admission with an error storage has already cached, or one
    /// nothing caches.
    fn refuse(&mut self, key: u64, error: MempoolError<E>) {
        let tx_id = self.admission_at(key).tx_id;
        self.step.changes.push(Change::Refused(tx_id));
        self.decide(
            key,
            Verdict::Rejected(rejection(scope_of(&error), Charge::None, error)),
        );
    }

    /// Ends an admission: every waiter gets the verdict, and the slot is free.
    /// A charge reaches only a waiter with a peer supplier.
    fn decide(&mut self, key: u64, verdict: Verdict) {
        let admission = self
            .admissions
            .remove(&key)
            .expect("the caller names a live admission");
        self.admission_of.remove(&admission.tx_id);
        self.owners.remove(&admission.owner);
        if admission.owner_waiting {
            // The reserving caller's fetch is still out. Its body will arrive
            // decoded, so its slot is not free yet.
            self.leases.insert(
                admission.owner,
                Lease {
                    tx_id: admission.tx_id,
                    verdict: verdict.clone(),
                },
            );
        }
        // A reservation that joined this admission dies with it. Its caller may
        // still come back with a body.
        let tx_id = admission.tx_id;
        let ended: Vec<ReservationId> = self
            .tickets
            .iter()
            .filter(|(_, ticket)| ticket.admission == key)
            .map(|(reservation, _)| *reservation)
            .collect();
        for reservation in ended {
            self.tickets.remove(&reservation);
            self.ended_joins
                .insert(reservation, (tx_id, verdict.clone()));
        }
        for waiter in admission.waiters {
            self.step.effects.push(Effect::Verdict {
                waiter: waiter.id,
                verdict: verdict.clone(),
            });
        }
    }

    /// The answer to a body whose reservation no longer names an admission.
    fn stale_verdict(&self, tx_id: UnminedTxId) -> Verdict {
        if self.pooled.contains_key(&tx_id.mined_id()) {
            return Verdict::Rejected(rejection(
                RejectionScope::Uncached,
                Charge::None,
                MempoolError::<E>::InMempool,
            ));
        }
        if let Some(error) = self.cached_rejection(tx_id) {
            return Verdict::Rejected(rejection(scope_of(&error), Charge::None, error));
        }
        Verdict::LocalFault(LocalFault::StaleReservation)
    }

    /// Starts a verification of an admission that holds its body.
    fn start_admission(&mut self, key: u64) {
        // No new check starts once the slot's time is up.
        if self.undecided_too_long(key) {
            self.fail_locally(key, LocalFault::VerifyTimeout);
            return;
        }
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
        self.jobs.insert(job, (JobTarget::Admission(tx_id), job.0));
        self.step.effects.push(Effect::Start {
            job,
            tx,
            revalidation: false,
            after_reset,
        });
    }

    /// Whether an admission has held its slot for [`MAX_UNDECIDED`] since its
    /// body arrived.
    fn undecided_too_long(&self, key: u64) -> bool {
        self.admissions[&key]
            .body_at
            .is_some_and(|at| at + MAX_UNDECIDED <= self.now)
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

    /// Runs every admission parked on `parent` again, and queues every hidden
    /// transaction parked on it.
    fn wake(&mut self, parent: transaction::Hash) {
        let woken: Vec<u64> = self
            .admissions
            .iter()
            .filter(|(_, admission)| match &admission.stage {
                Stage::Parked { on } => on.contains(&parent),
                _ => false,
            })
            .map(|(key, _)| *key)
            .collect();
        for key in woken {
            self.start_admission(key);
        }
        let parked: Vec<u64> = self
            .hidden
            .iter()
            .copied()
            .filter(|key| {
                matches!(&self.pool[key].hidden, Some(Hidden::Parked { on }) if on.contains(&parent))
            })
            .collect();
        for key in parked {
            self.set_hidden(key, Some(Hidden::Queued));
        }
    }

    /// The outputs of `tx`, which just became visible, are available. Every
    /// caller waiting for one of them is answered, in the order the callers
    /// came, in this step.
    fn offer_outputs(&mut self, tx: &Transaction, hash: transaction::Hash) {
        for (index, output) in tx.outputs().iter().enumerate() {
            let outpoint = OutPoint::from_usize(hash, index);
            for waiter in self.output_waiters.remove(&outpoint).unwrap_or_default() {
                self.step.effects.push(Effect::OutputReady {
                    waiter,
                    outpoint,
                    output: output.clone(),
                });
            }
        }
    }

    /// Removes every pooled transaction whose expiry height the tip has
    /// reached, and remembers each as expired. With `with_dependents`, what
    /// depends on a visible one goes too.
    fn remove_expired(&mut self, with_dependents: bool) {
        let expired: Vec<(UnminedTxId, bool)> = self
            .pool
            .values()
            .filter(|pooled| self.reached(pooled.expiry))
            .map(|pooled| (pooled.id, pooled.hidden.is_none()))
            .collect();
        for (tx_id, visible) in expired {
            // A hidden transaction has not been checked against the chain since
            // a reorg or a missed event. It may have been mined in a block the
            // core did not see, and then what spends its outputs is still
            // valid. So only a visible one takes its dependents with it.
            if with_dependents && visible {
                self.remove_with_dependents(tx_id.mined_id(), RemovalReason::Expired);
            } else if let Some(key) = self.pooled.get(&tx_id.mined_id()).copied() {
                self.remove_one(key, RemovalReason::Expired);
            }
            self.storage.reject_effects_chain(
                tx_id.mined_id(),
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

    /// Hides every visible pooled transaction, to be verified again. Each one
    /// leaves storage and keeps its body here.
    fn hide_all(&mut self) {
        let mut bodies: HashMap<transaction::Hash, VerifiedUnminedTx> = self
            .storage
            .take_all()
            .into_iter()
            .map(|tx| (tx.transaction.id().mined_id(), tx))
            .collect();
        let keys: Vec<u64> = self.pool.keys().copied().collect();
        for key in keys {
            match self.pool[&key].hidden {
                None => {
                    let pending =
                        u64::try_from(self.step.changes.len()).expect("a batch fits in u64");
                    let hidden_at =
                        ChangeSeq(self.logged.saturating_add(pending).saturating_add(1));
                    let pooled = self.pooled_at(key);
                    let id = pooled.id;
                    let body = bodies
                        .remove(&id.mined_id())
                        .expect("storage holds every visible pooled transaction");
                    let cost = body.cost();
                    pooled.body = Some(body);
                    pooled.hidden_at = hidden_at;
                    self.add_hidden_cost(cost, true);
                    self.set_hidden(key, Some(Hidden::Queued));
                    self.step.changes.push(Change::Hidden(id));
                }
                // A running job read the old chain, so its outcome will be
                // stale and it will run again.
                Some(Hidden::Running { .. }) => {}
                Some(_) => self.set_hidden(key, Some(Hidden::Queued)),
            }
        }
        debug_assert!(bodies.is_empty(), "storage held only pooled transactions");
    }

    /// The machine has applied a chain event: everything that waited for it
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
        let waiting: Vec<u64> = self
            .hidden
            .iter()
            .copied()
            .filter(|key| matches!(self.pool[key].hidden, Some(Hidden::AwaitingChain)))
            .collect();
        for key in waiting {
            self.set_hidden(key, Some(Hidden::Queued));
        }
        self.start_revalidations();
    }

    /// Starts queued revalidations, parents first, while the revalidation
    /// share and the node's limit allow.
    fn start_revalidations(&mut self) {
        // Walk the queue lazily, so a full share costs nothing and a start
        // costs only the keys it passes.
        let mut after = None;
        loop {
            if self.running >= self.limits.revalidating
                || self.slots_in_use() >= self.limits.pending
            {
                break;
            }
            let next = match after {
                None => self.queued.iter().next(),
                Some(previous) => self
                    .queued
                    .range((
                        std::ops::Bound::Excluded(previous),
                        std::ops::Bound::Unbounded,
                    ))
                    .next(),
            };
            let Some(&key) = next else {
                break;
            };
            after = Some(key);
            let pooled = &self.pool[&key];
            let parent_hidden = pooled.parents.iter().any(|parent| {
                self.pooled
                    .get(parent)
                    .is_some_and(|parent| self.pool[parent].hidden.is_some())
            });
            if parent_hidden {
                continue;
            }
            let tx = pooled
                .body
                .as_ref()
                .expect("a hidden transaction holds its body")
                .transaction
                .clone();
            let hash = pooled.id.mined_id();
            let job = JobId(self.fresh());
            let deadline = self.now + T_VERIFY;
            self.set_hidden(key, Some(Hidden::Running { job, deadline }));
            self.jobs
                .insert(job, (JobTarget::Revalidation(hash), job.0));
            self.step.effects.push(Effect::Start {
                job,
                tx,
                revalidation: true,
                // A revalidation is never charged.
                after_reset: false,
            });
        }
    }

    /// A revalidation hit a local fault. The transaction stays hidden and is
    /// tried again later.
    fn retry_later(&mut self, key: u64) {
        let at = self.now;
        let pooled = self.pooled_at(key);
        let backoff = pooled.backoff;
        pooled.backoff = backoff.saturating_mul(2).min(REVALIDATE_RETRY_MAX);
        self.set_hidden(key, Some(Hidden::Retry { at: at + backoff }));
    }

    /// Sets a pooled transaction's hidden state, keeping the indexes in step.
    fn set_hidden(&mut self, key: u64, hidden: Option<Hidden>) {
        let pooled = self
            .pool
            .get_mut(&key)
            .expect("the caller names a pooled transaction");
        if matches!(pooled.hidden, Some(Hidden::Running { .. })) {
            self.running -= 1;
        }
        if matches!(hidden, Some(Hidden::Running { .. })) {
            self.running += 1;
        }
        if matches!(hidden, Some(Hidden::Queued)) {
            self.queued.insert(key);
        } else {
            self.queued.remove(&key);
        }
        if hidden.is_some() {
            self.hidden.insert(key);
        } else {
            self.hidden.remove(&key);
        }
        pooled.hidden = hidden;
    }

    /// Adds or takes away `cost` from the hidden cost, and tells storage.
    fn add_hidden_cost(&mut self, cost: u64, add: bool) {
        self.hidden_cost = if add {
            self.hidden_cost.saturating_add(cost)
        } else {
            self.hidden_cost.saturating_sub(cost)
        };
        self.storage.set_outside_cost(self.hidden_cost);
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
        if !self.pool.contains_key(&key) {
            return;
        }
        // Clear the indexes, and cancel a running job.
        let running = match self.pool[&key].hidden {
            Some(Hidden::Running { job, .. }) => Some(job),
            _ => None,
        };
        let visible = self.pool[&key].hidden.is_none();
        self.set_hidden(key, None);
        let pooled = self
            .pool
            .remove(&key)
            .expect("the transaction was pooled a moment ago");
        let hash = pooled.id.mined_id();
        self.pooled.remove(&hash);
        self.forget_parents(key, &pooled.parents);
        if visible {
            self.storage.remove_one(&hash);
        } else if let Some(body) = &pooled.body {
            self.add_hidden_cost(body.cost(), false);
        }
        if let Some(job) = running {
            self.jobs.remove(&job);
            self.step.effects.push(Effect::Cancel(job));
        }
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
                .expect("an evicted transaction was pooled and visible");
            let hash = pooled.id.mined_id();
            self.pooled.remove(&hash);
            self.forget_parents(key, &pooled.parents);
            self.step.changes.push(Change::Removed {
                id: pooled.id,
                reason: RemovalReason::Evicted,
            });
        }
    }

    /// Ends an admission that holds no body once nobody can still bring one.
    /// Callers already waiting on it get a local fault.
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
            self.decide(key, Verdict::LocalFault(LocalFault::NoBody));
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
    /// `started_at`: its transaction is visible now, creates that output, and
    /// became visible after that count.
    fn arrived_since(&self, outpoint: &OutPoint, started_at: u64) -> bool {
        self.offers(outpoint)
            && self
                .pooled
                .get(&outpoint.hash)
                .is_some_and(|key| self.pool[key].visible_at > started_at)
    }

    /// Whether the visible pool offers `outpoint`.
    fn offers(&self, outpoint: &OutPoint) -> bool {
        self.storage.created_output(outpoint).is_some()
    }

    /// Whether `tx` spends what a hidden pooled transaction spends.
    fn conflicts_with_hidden(&self, tx: &Transaction) -> bool {
        if self.hidden.is_empty() {
            return false;
        }
        let spends = Spends::of([tx]);
        self.hidden.iter().any(|key| {
            self.pool[key]
                .body
                .as_ref()
                .is_some_and(|body| spends.conflict_with(body.transaction.transaction()))
        })
    }

    /// A pooled transaction's body, from storage if it is visible.
    fn pooled_transaction<'a>(&'a self, pooled: &'a Pooled<E>) -> Option<&'a Transaction> {
        match &pooled.body {
            Some(body) => Some(body.transaction.transaction().as_ref()),
            None => self
                .storage
                .transactions()
                .get(&pooled.id.mined_id())
                .map(|tx| tx.transaction.transaction().as_ref()),
        }
    }

    fn admission_at(&mut self, key: u64) -> &mut Admission<E> {
        self.admissions
            .get_mut(&key)
            .expect("the caller names a live admission")
    }

    fn pooled_at(&mut self, key: u64) -> &mut Pooled<E> {
        self.pool
            .get_mut(&key)
            .expect("the caller names a pooled transaction")
    }

    fn fresh(&mut self) -> u64 {
        self.next_id = self.next_id.saturating_add(1);
        self.next_id
    }
}

/// A rejection as callers see it.
fn rejection<E: VerifyError>(
    scope: RejectionScope,
    charge: Charge,
    error: MempoolError<E>,
) -> Rejection {
    Rejection {
        scope,
        charge,
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
