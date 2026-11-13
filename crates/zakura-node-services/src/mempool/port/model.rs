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
//! Two things are left out. Eviction at the pool's cost limit is the moved
//! storage code's rule, so the model assumes the pool stays below it. A
//! cooldown lasts [`COOLDOWN`] each time and does not escalate.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    time::Duration,
};

use zakura_chain::{
    block::Height,
    transaction::{self, UnminedTxId},
    transparent::OutPoint,
};

use super::{
    Change, Charge, Generation, GenerationRead, Intent, LocalFault, NoRoom, RejectionScope,
    RemovalReason, ReservationId, Source, Supplier, MAX_PENDING, MAX_PENDING_PER_SOURCE,
    MAX_REVALIDATING, MAX_WAITERS,
};
use crate::mempool::QueueSource;

/// Time since the model started.
pub type ModelTime = Duration;

/// How long one verification can take. Provisional.
pub const T_VERIFY: Duration = Duration::from_secs(60);

/// How long a transaction waits for a parent nobody is bringing in. Today's
/// wait for a pool parent.
pub const T_PARK: Duration = Duration::from_secs(60);

/// How long the core refuses a peer after a cooldown charge or a violation.
/// Today's base cooldown.
pub const COOLDOWN: Duration = Duration::from_secs(10 * 60);

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
    /// The transaction is valid at the tip it was verified against.
    Pass,
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
    /// The transparent outputs of a newly admitted transaction reach their
    /// waiters, in the same step as the admission.
    Outputs {
        /// The transaction.
        tx: transaction::Hash,
        /// How many outputs it creates.
        count: u32,
    },
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
    /// Parents are missing. `waited` counts only the time nobody was
    /// bringing one in.
    Parked {
        on: Vec<transaction::Hash>,
        waited: Duration,
    },
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
}

#[derive(Clone, Debug)]
struct Pooled {
    tx: ModelTx,
    hidden: Option<Hidden>,
    /// The wait before the next try after a local fault.
    backoff: Duration,
}

#[derive(Copy, Clone, Debug)]
enum JobTarget {
    Admission(UnminedTxId),
    Revalidation(transaction::Hash),
}

#[derive(Clone, Debug)]
struct JoinTicket {
    tx_id: UnminedTxId,
    source: Source,
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
    owners: HashMap<ReservationId, UnminedTxId>,
    tickets: BTreeMap<ReservationId, JoinTicket>,
    /// The pool, oldest first.
    pool: BTreeMap<u64, Pooled>,
    pooled: HashMap<transaction::Hash, u64>,
    jobs: HashMap<JobId, JobTarget>,
    rejected_exact: HashMap<UnminedTxId, RejectionScope>,
    rejected_effects: HashMap<transaction::Hash, RejectionScope>,
    cooldowns: HashMap<QueueSource, ModelTime>,
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
            tickets: BTreeMap::new(),
            pool: BTreeMap::new(),
            pooled: HashMap::new(),
            jobs: HashMap::new(),
            rejected_exact: HashMap::new(),
            rejected_effects: HashMap::new(),
            cooldowns: HashMap::new(),
            step: Step::default(),
        }
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
            if self.cooldowns.contains_key(peer) {
                return Err(ModelUnreserved::CoolingDown);
            }
        }
        if self.pooled.contains_key(&tx_id.mined_id()) {
            return Err(ModelUnreserved::Pooled);
        }
        if let Some(scope) = self.cached_rejection(tx_id) {
            return Err(ModelUnreserved::Rejected(scope));
        }
        if let Some(key) = self.admission_of.get(&tx_id).copied() {
            return self.join(key, tx_id, source, intent);
        }
        // Today's order: the node's limit, then the source's.
        if self.slots_in_use() >= MAX_PENDING {
            return Err(ModelUnreserved::NoRoom(NoRoom::Node));
        }
        if matches!(source, Source::Peer(_))
            && self.counted_against(&source) >= MAX_PENDING_PER_SOURCE
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
            },
        );
        self.admission_of.insert(tx_id, key);
        self.owners.insert(owner, tx_id);
        Ok(ModelReservation {
            id: owner,
            joined: false,
        })
    }

    /// A reservation dropped without a submission.
    pub fn release(&mut self, reservation: ReservationId, now: ModelTime) -> Step {
        self.advance(now);
        if self.tickets.remove(&reservation).is_some() {
            // A ticket holds no slot. Its admission may now have nobody left.
        } else if let Some(tx_id) = self.owners.remove(&reservation) {
            if let Some(admission) = self.admission_mut(tx_id) {
                admission.owner_waiting = false;
            }
        }
        self.drop_abandoned();
        self.start_revalidations();
        self.take_step()
    }

    /// [`super::PoolHandle::submit`]. Returns the waiter the verdict will
    /// name, which can be in the returned step.
    pub fn submit(
        &mut self,
        reservation: ReservationId,
        tx: ModelTx,
        supplier: Supplier,
        now: ModelTime,
    ) -> (WaiterId, Step) {
        self.advance(now);
        let waiter = WaiterId(self.fresh());
        let (tx_id, ticket_source) = match self.tickets.remove(&reservation) {
            Some(ticket) => (Some(ticket.tx_id), Some(ticket.source)),
            None => (self.owners.remove(&reservation), None),
        };
        let key = tx_id
            .filter(|id| *id == tx.id)
            .and_then(|id| self.admission_of.get(&id).copied());
        let Some(key) = key else {
            // The id was decided, or the pool turned off, after the
            // reservation was granted.
            let verdict = self.stale_verdict(tx.id);
            self.step.effects.push(Effect::Verdict { waiter, verdict });
            return (waiter, self.take_step());
        };
        let start = {
            let admission = self
                .admissions
                .get_mut(&key)
                .expect("the index names a live admission");
            if admission.owner == reservation {
                admission.owner_waiting = false;
            }
            let source = ticket_source.unwrap_or_else(|| admission.source.clone());
            admission.waiters.push(Waiter {
                id: waiter,
                source,
                supplier,
            });
            if admission.body.is_none() {
                admission.body = Some(tx);
                true
            } else {
                false
            }
        };
        if start {
            self.start_admission(key);
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
        let Some(target) = self.jobs.remove(&job) else {
            // Cancelled, or timed out.
            return self.take_step();
        };
        let fresh = before.brackets(after, self.applied);
        // A stale outcome waits for the chain event when that event has not
        // been applied yet. Otherwise the job can run again at once.
        let chain_is_ahead = [before, after].iter().any(|read| match read {
            GenerationRead::Changing => true,
            GenerationRead::Stable(generation) => *generation > self.applied,
        });
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
                    self.admission_outcome(key, outcome);
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
                    self.revalidation_outcome(key, hash, outcome);
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
        self.clear_tip_rejections();
        let mut mined = Vec::new();
        for block in &transition.connected {
            for block_tx in &block.transactions {
                let hash = block_tx.id.mined_id();
                if let Some(key) = self.pooled.get(&hash).copied() {
                    // Its dependents stay. Their parent is on the chain now.
                    self.remove_one(key, RemovalReason::Mined);
                }
                let conflicting: Vec<transaction::Hash> = self
                    .pool
                    .values()
                    .filter(|pooled| pooled.tx.conflicts_with(block_tx))
                    .map(|pooled| pooled.tx.id.mined_id())
                    .collect();
                for conflict in conflicting {
                    self.remove_with_dependents(conflict, RemovalReason::Conflicted);
                }
                mined.push(hash);
            }
        }
        let expired: Vec<transaction::Hash> = self
            .pool
            .values()
            .filter(|pooled| pooled.tx.expiry.is_some_and(|expiry| self.tip >= expiry))
            .map(|pooled| pooled.tx.id.mined_id())
            .collect();
        for hash in expired {
            self.remove_with_dependents(hash, RemovalReason::Expired);
        }
        if transition.disconnected > 0 {
            self.hide_all();
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
        self.clear_tip_rejections();
        self.hide_all();
        let parked: Vec<u64> = self
            .admissions
            .iter()
            .filter(|(_, admission)| matches!(admission.stage, Stage::Parked { .. }))
            .map(|(key, _)| *key)
            .collect();
        for key in parked {
            self.start_admission(key);
        }
        self.chain_applied();
        self.take_step()
    }

    /// Time passed. Applies every deadline at or before `now`.
    pub fn tick(&mut self, now: ModelTime) -> Step {
        self.advance(now);
        let mut timed_out = Vec::new();
        let mut unparked = Vec::new();
        for (key, admission) in &self.admissions {
            match &admission.stage {
                Stage::Verifying { deadline, .. } if *deadline <= now => timed_out.push(*key),
                Stage::Parked { waited, .. } if *waited >= T_PARK => unparked.push(*key),
                _ => {}
            }
        }
        for key in timed_out {
            self.cancel_admission_job(key);
            self.decide(key, ModelVerdict::LocalFault(LocalFault::VerifyTimeout));
        }
        for key in unparked {
            let tx_id = self.admission_at(key).tx_id;
            self.step.changes.push(Change::Refused(tx_id));
            self.decide(
                key,
                ModelVerdict::Rejected {
                    scope: RejectionScope::Uncached,
                    charge: Charge::None,
                },
            );
        }
        let keys: Vec<u64> = self.pool.keys().copied().collect();
        for key in keys {
            let hidden = self.pooled_at(key).hidden.clone();
            match hidden {
                Some(Hidden::Running { job, deadline }) if deadline <= now => {
                    self.jobs.remove(&job);
                    self.step.effects.push(Effect::Cancel(job));
                    self.retry_later(key);
                }
                Some(Hidden::Retry { at }) if at <= now => {
                    self.pooled_at(key).hidden = Some(Hidden::Queued);
                }
                _ => {}
            }
        }
        self.cooldowns.retain(|_, until| *until > now);
        self.start_revalidations();
        self.take_step()
    }

    /// [`super::PoolHandle::report_violation`].
    pub fn violation(&mut self, peer: QueueSource, now: ModelTime) {
        self.advance(now);
        self.cooldowns.insert(peer, now.saturating_add(COOLDOWN));
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
        self.rejected_exact.clear();
        self.rejected_effects.clear();
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
        self.admissions.len().saturating_add(self.revalidating())
    }

    /// Undecided transactions counted against `source`.
    pub fn counted_against(&self, source: &Source) -> usize {
        self.admissions
            .values()
            .filter(|admission| admission.source == *source)
            .count()
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

    /// Checks the invariants every reachable state keeps.
    ///
    /// # Panics
    ///
    /// If one is broken.
    pub fn assert_invariants(&self) {
        assert!(
            self.slots_in_use() <= MAX_PENDING,
            "pool/admissions-within-limits: the node's limit"
        );
        let mut sources: HashMap<&Source, usize> = HashMap::new();
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
                    count <= MAX_PENDING_PER_SOURCE,
                    "pool/admissions-within-limits: one source's limit"
                );
            }
        }
        assert!(
            self.revalidating() <= MAX_REVALIDATING,
            "the revalidation share"
        );
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

    /// Joins an id that is already undecided, if `source` may.
    fn join(
        &mut self,
        key: u64,
        tx_id: UnminedTxId,
        source: Source,
        intent: Intent,
    ) -> Result<ModelReservation, ModelUnreserved> {
        if intent == Intent::Fetch {
            return Err(ModelUnreserved::Pending);
        }
        let tickets: Vec<&JoinTicket> = self
            .tickets
            .values()
            .filter(|ticket| ticket.tx_id == tx_id)
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
        let id = ReservationId(self.fresh());
        self.tickets.insert(id, JoinTicket { tx_id, source });
        Ok(ModelReservation { id, joined: true })
    }

    fn admission_outcome(&mut self, key: u64, outcome: Outcome) {
        let admission = self.admission_at(key).clone();
        let tx = admission.body.expect("a verified admission holds its body");
        let hash = tx.id.mined_id();
        match outcome {
            Outcome::Pass => {
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
                let pool_key = self.fresh();
                self.step.changes.push(Change::Added(tx.id));
                self.step.effects.push(Effect::Outputs {
                    tx: hash,
                    count: tx.outputs,
                });
                self.pool.insert(
                    pool_key,
                    Pooled {
                        tx,
                        hidden: None,
                        backoff: REVALIDATE_RETRY,
                    },
                );
                self.pooled.insert(hash, pool_key);
                self.decide(key, ModelVerdict::Admitted);
                self.wake(hash);
            }
            Outcome::Fail { scope, charge } => self.reject(key, scope, charge),
            Outcome::MissingInputs(missing) => {
                let mut on: Vec<transaction::Hash> = Vec::new();
                for outpoint in missing {
                    if !on.contains(&outpoint.hash) {
                        on.push(outpoint.hash);
                    }
                }
                // A parent that became visible after the verifier looked is
                // no reason to park. Its admission has already been
                // announced, so nothing would wake this transaction.
                if on.iter().any(|parent| self.is_visible(parent)) {
                    self.start_admission(key);
                } else {
                    self.admission_at(key).stage = Stage::Parked {
                        on,
                        waited: Duration::ZERO,
                    };
                }
            }
            Outcome::LocalFault(fault) => self.decide(key, ModelVerdict::LocalFault(fault)),
        }
    }

    fn revalidation_outcome(&mut self, key: u64, hash: transaction::Hash, outcome: Outcome) {
        match outcome {
            Outcome::Pass => {
                let pooled = self.pooled_at(key);
                pooled.hidden = None;
                pooled.backoff = REVALIDATE_RETRY;
                let tx_id = pooled.tx.id;
                self.step.changes.push(Change::Revealed(tx_id));
                self.wake(hash);
            }
            // A pooled transaction whose parent left the pool fails with its
            // parent.
            Outcome::Fail { .. } | Outcome::MissingInputs(_) => {
                // Its job has reported, so there is nothing to cancel.
                self.pooled_at(key).hidden = Some(Hidden::Queued);
                self.remove_with_dependents(hash, RemovalReason::Invalidated);
            }
            Outcome::LocalFault(_) => self.retry_later(key),
        }
    }

    /// Rejects an admission: caches the rejection, logs it, and answers
    /// every waiter.
    fn reject(&mut self, key: u64, scope: RejectionScope, charge: Charge) {
        let tx_id = self.admission_at(key).tx_id;
        match scope {
            RejectionScope::ExactTip => {
                self.rejected_exact.insert(tx_id, scope);
            }
            RejectionScope::EffectsTip | RejectionScope::EffectsChain => {
                self.rejected_effects.insert(tx_id.mined_id(), scope);
            }
            RejectionScope::Uncached => {}
        }
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
        for waiter in admission.waiters {
            let verdict = match (verdict, &waiter.supplier) {
                (ModelVerdict::Rejected { scope, charge }, Supplier::Peer(peer)) => {
                    if charge == Charge::Cooldown {
                        self.cooldowns
                            .insert(peer.clone(), self.now.saturating_add(COOLDOWN));
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
        if let Some(scope) = self.cached_rejection(tx_id) {
            return ModelVerdict::Rejected {
                scope,
                charge: Charge::None,
            };
        }
        ModelVerdict::LocalFault(LocalFault::StaleReservation)
    }

    fn start_admission(&mut self, key: u64) {
        let job = JobId(self.fresh());
        let deadline = self.now.saturating_add(T_VERIFY);
        let admission = self.admission_at(key);
        admission.stage = Stage::Verifying { job, deadline };
        let tx_id = admission.tx_id;
        self.jobs.insert(job, JobTarget::Admission(tx_id));
        self.step.effects.push(Effect::Start {
            job,
            tx: tx_id,
            revalidation: false,
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
    }

    /// Hides every visible pooled transaction, to be verified again.
    fn hide_all(&mut self) {
        for pooled in self.pool.values_mut() {
            match pooled.hidden {
                None => {
                    pooled.hidden = Some(Hidden::Queued);
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
            if self.revalidating() >= MAX_REVALIDATING || self.slots_in_use() >= MAX_PENDING {
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
                .insert(job, JobTarget::Revalidation(tx.id.mined_id()));
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

    /// Removes admissions that hold no body and that nobody can still bring
    /// one to.
    fn drop_abandoned(&mut self) {
        let tickets: HashSet<UnminedTxId> =
            self.tickets.values().map(|ticket| ticket.tx_id).collect();
        let abandoned: Vec<u64> = self
            .admissions
            .iter()
            .filter(|(_, admission)| {
                admission.body.is_none()
                    && !admission.owner_waiting
                    && !tickets.contains(&admission.tx_id)
            })
            .map(|(key, _)| *key)
            .collect();
        for key in abandoned {
            if let Some(admission) = self.admissions.remove(&key) {
                self.admission_of.remove(&admission.tx_id);
            }
        }
    }

    /// Moves time to `now`. A parked transaction's wait counts only the time
    /// nobody was bringing one of its parents in: no admission held the
    /// parent's body, and the parent was not hidden in the pool.
    fn advance(&mut self, now: ModelTime) {
        let elapsed = now.saturating_sub(self.now);
        self.now = self.now.max(now);
        if elapsed.is_zero() {
            return;
        }
        let arriving: HashSet<transaction::Hash> = self
            .admissions
            .values()
            .filter_map(|admission| admission.body.as_ref())
            .map(|tx| tx.id.mined_id())
            .chain(self.pooled.keys().copied())
            .collect();
        for admission in self.admissions.values_mut() {
            if let Stage::Parked { on, waited } = &mut admission.stage {
                if !on.iter().any(|parent| arriving.contains(parent)) {
                    *waited = waited.saturating_add(elapsed);
                }
            }
        }
    }

    fn clear_tip_rejections(&mut self) {
        self.rejected_exact.clear();
        self.rejected_effects
            .retain(|_, scope| *scope == RejectionScope::EffectsChain);
    }

    fn cached_rejection(&self, tx_id: UnminedTxId) -> Option<RejectionScope> {
        self.rejected_exact
            .get(&tx_id)
            .or_else(|| self.rejected_effects.get(&tx_id.mined_id()))
            .copied()
    }

    fn is_visible(&self, hash: &transaction::Hash) -> bool {
        self.pooled
            .get(hash)
            .is_some_and(|key| self.pool[key].hidden.is_none())
    }

    fn revalidating(&self) -> usize {
        self.pool
            .values()
            .filter(|pooled| matches!(pooled.hidden, Some(Hidden::Running { .. })))
            .count()
    }

    fn admission_mut(&mut self, tx_id: UnminedTxId) -> Option<&mut Admission> {
        let key = self.admission_of.get(&tx_id)?;
        self.admissions.get_mut(key)
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
        std::mem::take(&mut self.step)
    }
}
