//! An independent model of `tx-relay`'s node-level reactor: which peer is
//! asked for which transaction, and how the reactor uses the pool port.
//!
//! The model restates the documented rules and never calls the code it
//! checks. It drives [`PoolModel`] the way the reactor drives the pool core.
//!
//! The rules:
//!
//! - **Reserve, then ask.** No `GetTxs` goes out for an id without a
//!   reservation at the core. A full core means no request and a retry
//!   after a delay.
//! - **One reservation for the life of the fetch.** It is counted against
//!   the first peer asked. A later announcer that answers is named as the
//!   supplier. Nothing moves the reservation.
//! - **One preferred attempt.** A `NotFound` or a timeout moves the id to
//!   its next announcer. A request that timed out stays outstanding and
//!   keeps its slot until its peer answers or its session ends.
//! - **The first valid body wins.** A later body for the same id is dropped
//!   with no penalty, because it answered a request this node sent.
//! - **A verdict ends the fetch** for every announcer.
//! - **A session owns what it was asked.** A request belongs to the session
//!   it was sent on until that session answers it or ends, even after the
//!   node has stopped waiting, given up or been told about the id again. A
//!   body a session supplied counts against it until the verdict.
//!
//! Left out: the wire rules of one session (`Hello`, the byte rate, serving),
//! which belong to the per-session machine.

use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};

use zakura_chain::transaction::UnminedTxId;
use zakura_node_services::mempool::{
    port::{
        model::{
            Effect, JobId, ModelTime, ModelTx, ModelUnreserved, ModelVerdict, PoolModel, Step,
            WaiterId,
        },
        Charge, Intent, NoRoom, ReservationId, Source, Supplier,
    },
    QueueSource,
};

/// Ids requested from one peer or supplied by it and awaiting a verdict, at
/// most. A peer's `Hello` can lower how many of them are requests.
pub(crate) const MAX_REQUESTS_PER_PEER: usize = 5;

/// How long one attempt waits for its answer.
pub(crate) const REQUEST_TIMEOUT: Duration = Duration::from_secs(8);

/// Announcers asked for one id before the node gives up on it.
pub(crate) const MAX_ATTEMPTS: usize = 3;

/// Undecided ids one peer has announced, at most. Provisional, and open: a
/// whole pool is up to 8,000 ids, so an honest opening announcement can
/// overflow this.
pub(crate) const PENDING_PER_PEER: usize = 5_000;

/// The first wait before an id is tried again after the core had no room or
/// a local fault. It doubles up to [`RETRY_MAX`]. Provisional.
pub(crate) const RETRY_FIRST: Duration = Duration::from_secs(1);

/// The longest wait between tries.
pub(crate) const RETRY_MAX: Duration = Duration::from_secs(30);

/// Times one id is fetched again after its body hit a local fault at the
/// core. Each fetch gets a fresh [`MAX_ATTEMPTS`]. Provisional.
pub(crate) const MAX_REFETCHES: usize = 2;

/// Names one session. A peer that reconnects or replaces its session gets a
/// new one, and the old one stays itself until it ends.
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct SessionId(pub(crate) u64);

/// What the reactor does on the wire.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RelayEffect {
    /// Send `GetTxs` for `id` on `session`.
    Ask {
        /// The session.
        session: SessionId,
        /// The id.
        id: UnminedTxId,
    },
    /// Close the connection of `session` for a violation.
    Close(SessionId),
}

#[derive(Debug)]
struct Session {
    peer: QueueSource,
    dialed: bool,
    max_requests: usize,
    /// Requests sent on this session with no answer yet, including ones the
    /// node stopped waiting for.
    outstanding: Vec<UnminedTxId>,
    /// Bodies this session supplied that are awaiting a verdict.
    verifying: usize,
    /// Undecided ids this session has announced.
    pending: usize,
}

impl Session {
    /// Whether one more id can be requested: within the peer's `Hello`, and
    /// within the limit on ids requested or awaiting a verdict.
    fn has_room(&self) -> bool {
        self.outstanding.len() < self.max_requests
            && self.outstanding.len().saturating_add(self.verifying) < MAX_REQUESTS_PER_PEER
    }
}

#[derive(Clone, Debug)]
enum Fetch {
    /// No attempt is in progress.
    Idle,
    /// `session` was asked and has until `deadline`.
    Asking {
        session: SessionId,
        deadline: ModelTime,
    },
    /// A body is at the core.
    Submitted {
        waiter: WaiterId,
        supplier: SessionId,
    },
    /// Waiting to try again.
    Backoff { until: ModelTime },
}

#[derive(Debug)]
struct Wanted {
    id: UnminedTxId,
    /// The sessions that announced it, in the order they did.
    announcers: Vec<SessionId>,
    /// The announcers already asked in this fetch.
    tried: Vec<SessionId>,
    /// Announcers asked in this fetch.
    attempts: usize,
    /// Times the id was fetched again after a local fault.
    refetches: usize,
    /// The slot at the core, from the first ask until the body is submitted
    /// or the node gives up.
    reservation: Option<ReservationId>,
    fetch: Fetch,
    /// The next retry delay.
    retry: Duration,
}

/// The documented fetch rules.
#[derive(Debug)]
pub(crate) struct ReactorModel {
    max_tx_bytes: u32,
    next: u64,
    sessions: BTreeMap<SessionId, Session>,
    /// Every id this node wants, oldest first.
    wanted: BTreeMap<u64, Wanted>,
    wanted_of: HashMap<UnminedTxId, u64>,
    effects: Vec<RelayEffect>,
    /// Every step the pool returned to a call this reactor made.
    pool_steps: Vec<Step>,
    /// Announced ids dropped at a full pending set.
    pub(crate) overflowed: u64,
}

impl ReactorModel {
    /// A reactor that accepts transactions of at most `max_tx_bytes`.
    pub(crate) fn new(max_tx_bytes: u32) -> Self {
        Self {
            max_tx_bytes,
            next: 0,
            sessions: BTreeMap::new(),
            wanted: BTreeMap::new(),
            wanted_of: HashMap::new(),
            effects: Vec::new(),
            pool_steps: Vec::new(),
            overflowed: 0,
        }
    }

    // Inputs.

    /// A session is live. `requests_served` is what the peer's `Hello`
    /// stated. An earlier session of the same peer stays until it ends.
    pub(crate) fn connect(
        &mut self,
        peer: QueueSource,
        dialed: bool,
        requests_served: usize,
    ) -> SessionId {
        let session = SessionId(self.fresh());
        self.sessions.insert(
            session,
            Session {
                peer,
                dialed,
                max_requests: requests_served.min(MAX_REQUESTS_PER_PEER),
                outstanding: Vec::new(),
                verifying: 0,
                pending: 0,
            },
        );
        session
    }

    /// `session` announced `id` with serialized size `size`.
    pub(crate) fn announce(
        &mut self,
        session: SessionId,
        id: UnminedTxId,
        size: u32,
        now: ModelTime,
        pool: &mut PoolModel,
    ) -> Vec<RelayEffect> {
        if !self.sessions.contains_key(&session) || size > self.max_tx_bytes {
            return Vec::new();
        }
        let already_announced = self
            .wanted_of
            .get(&id)
            .is_some_and(|key| self.wanted[key].announcers.contains(&session));
        if already_announced {
            return Vec::new();
        }
        let announcer = self.session_at(session);
        if announcer.pending >= PENDING_PER_PEER {
            // Honest announcers overlap, so this is dropped and counted.
            self.overflowed = self.overflowed.saturating_add(1);
            return Vec::new();
        }
        announcer.pending = announcer.pending.saturating_add(1);
        match self.wanted_of.get(&id).copied() {
            Some(key) => self.wanted_at(key).announcers.push(session),
            None => {
                let key = self.fresh();
                self.wanted.insert(
                    key,
                    Wanted {
                        id,
                        announcers: vec![session],
                        tried: Vec::new(),
                        attempts: 0,
                        refetches: 0,
                        reservation: None,
                        fetch: Fetch::Idle,
                        retry: RETRY_FIRST,
                    },
                );
                self.wanted_of.insert(id, key);
            }
        }
        self.pump(now, pool);
        self.take_effects()
    }

    /// `session` sent a `Tx` that decodes to `tx`.
    pub(crate) fn tx(
        &mut self,
        session: SessionId,
        tx: ModelTx,
        now: ModelTime,
        pool: &mut PoolModel,
    ) -> Vec<RelayEffect> {
        if !self.answered(session, tx.id, now, pool) {
            return self.take_effects();
        }
        let id = tx.id;
        let peer = self.sessions[&session].peer.clone();
        if let Some(key) = self.wanted_of.get(&id).copied() {
            let wanted = self.wanted_at(key);
            // A body is already at the core: this one answered an earlier
            // attempt. It is dropped with no penalty.
            if !matches!(wanted.fetch, Fetch::Submitted { .. }) {
                let supplier = Supplier::Peer(peer.clone());
                let arrival = match wanted.reservation.take() {
                    Some(reservation) => Some(pool.submit(reservation, tx, supplier, now)),
                    // The slot was used by a body that hit a local fault, or
                    // the id was announced again after the node gave up.
                    // This body is in hand, so it is delivered.
                    None => match pool.reserve(id, Source::Peer(peer), Intent::Deliver, now) {
                        Ok(joined) if joined.wants_body => {
                            Some(pool.submit(joined.id, tx, supplier, now))
                        }
                        Ok(joined) => Some(pool.wait(joined.id, id, supplier, now)),
                        Err(_) => None,
                    },
                };
                if let Some((waiter, step)) = arrival {
                    self.wanted_at(key).fetch = Fetch::Submitted {
                        waiter,
                        supplier: session,
                    };
                    self.session_at(session).verifying =
                        self.session_at(session).verifying.saturating_add(1);
                    self.route(step, now, pool);
                }
            }
        }
        self.pump(now, pool);
        self.take_effects()
    }

    /// `session` sent a `NotFound` for `id`.
    pub(crate) fn not_found(
        &mut self,
        session: SessionId,
        id: UnminedTxId,
        now: ModelTime,
        pool: &mut PoolModel,
    ) -> Vec<RelayEffect> {
        if !self.answered(session, id, now, pool) {
            return self.take_effects();
        }
        if let Some(key) = self.wanted_of.get(&id).copied() {
            let asking_it = matches!(
                self.wanted[&key].fetch,
                Fetch::Asking { session: asked, .. } if asked == session
            );
            if asking_it {
                self.move_on(key, now, pool);
            }
        }
        self.pump(now, pool);
        self.take_effects()
    }

    /// Something the pool did after another input: verdicts for bodies this
    /// reactor submitted.
    pub(crate) fn pool_step(&mut self, step: &Step, now: ModelTime, pool: &mut PoolModel) {
        self.verdicts(step, now, pool);
        self.pump(now, pool);
    }

    /// Time passed.
    pub(crate) fn tick(&mut self, now: ModelTime, pool: &mut PoolModel) -> Vec<RelayEffect> {
        let keys: Vec<u64> = self.wanted.keys().copied().collect();
        for key in keys {
            let Some(wanted) = self.wanted.get(&key) else {
                continue;
            };
            match wanted.fetch {
                Fetch::Asking { deadline, .. } if deadline <= now => self.move_on(key, now, pool),
                Fetch::Backoff { until } if until <= now => {
                    self.wanted_at(key).fetch = Fetch::Idle;
                }
                _ => {}
            }
        }
        self.pump(now, pool);
        self.take_effects()
    }

    /// `session` ended. Only that session goes: a replacement of the same
    /// peer is another session.
    pub(crate) fn end(
        &mut self,
        session: SessionId,
        now: ModelTime,
        pool: &mut PoolModel,
    ) -> Vec<RelayEffect> {
        if self.sessions.remove(&session).is_none() {
            return Vec::new();
        }
        let keys: Vec<u64> = self.wanted.keys().copied().collect();
        for key in keys {
            let Some(wanted) = self.wanted.get_mut(&key) else {
                continue;
            };
            wanted.announcers.retain(|announcer| *announcer != session);
            wanted.tried.retain(|tried| *tried != session);
            let asking_it =
                matches!(wanted.fetch, Fetch::Asking { session: asked, .. } if asked == session);
            let submitted = matches!(wanted.fetch, Fetch::Submitted { .. });
            if wanted.announcers.is_empty() && !submitted {
                self.give_up(key, now, pool);
            } else if asking_it {
                self.move_on(key, now, pool);
            }
        }
        self.pump(now, pool);
        self.take_effects()
    }

    // Observations.

    /// Every step the pool returned to a call this reactor made, since the
    /// last call. They can hold verdicts for other callers.
    pub(crate) fn take_pool_steps(&mut self) -> Vec<Step> {
        std::mem::take(&mut self.pool_steps)
    }

    /// The jobs the pool started for this reactor since the last call.
    pub(crate) fn take_started(&mut self) -> Vec<JobId> {
        self.take_pool_steps()
            .iter()
            .flat_map(Step::started)
            .collect()
    }

    /// Whether the node still wants `id`.
    pub(crate) fn wants(&self, id: UnminedTxId) -> bool {
        self.wanted_of.contains_key(&id)
    }

    /// Requests outstanding on `session`.
    pub(crate) fn outstanding(&self, session: SessionId) -> usize {
        self.sessions
            .get(&session)
            .map_or(0, |session| session.outstanding.len())
    }

    /// Bodies `session` supplied that are awaiting a verdict.
    pub(crate) fn verifying(&self, session: SessionId) -> usize {
        self.sessions
            .get(&session)
            .map_or(0, |session| session.verifying)
    }

    /// Whether `session` is live.
    pub(crate) fn is_live(&self, session: SessionId) -> bool {
        self.sessions.contains_key(&session)
    }

    /// Checks the invariants every reachable state keeps.
    ///
    /// # Panics
    ///
    /// If one is broken.
    pub(crate) fn assert_invariants(&self) {
        for session in self.sessions.values() {
            assert!(
                session.outstanding.len() <= session.max_requests,
                "tx-relay/in-flight-within-caps: requests outstanding on one session"
            );
            assert!(
                session.outstanding.len().saturating_add(session.verifying)
                    <= MAX_REQUESTS_PER_PEER,
                "tx-relay/in-flight-within-caps: ids requested or awaiting a verdict"
            );
            assert!(
                session.pending <= PENDING_PER_PEER,
                "tx-relay/pending-within-bound"
            );
            for (index, id) in session.outstanding.iter().enumerate() {
                assert!(
                    !session.outstanding[index.saturating_add(1)..].contains(id),
                    "no id is outstanding twice on one session"
                );
            }
        }
        for wanted in self.wanted.values() {
            assert!(wanted.attempts <= MAX_ATTEMPTS, "attempts in one fetch");
            assert!(wanted.refetches <= MAX_REFETCHES, "fetches of one id");
            if let Fetch::Asking { session, .. } = wanted.fetch {
                assert!(
                    self.sessions[&session].outstanding.contains(&wanted.id),
                    "the session being asked has the request outstanding"
                );
                assert!(
                    wanted.reservation.is_some(),
                    "no request without a reservation"
                );
            }
        }
    }

    // Rules.

    /// Checks that `id` answers a request outstanding on `session`, and
    /// frees that request. Anything else is a violation: the connection
    /// closes and the core starts the peer's cooldown.
    fn answered(
        &mut self,
        session: SessionId,
        id: UnminedTxId,
        now: ModelTime,
        pool: &mut PoolModel,
    ) -> bool {
        let Some(asked) = self.sessions.get_mut(&session) else {
            return false;
        };
        match asked.outstanding.iter().position(|asked| *asked == id) {
            Some(index) => {
                asked.outstanding.remove(index);
                true
            }
            None => {
                pool.violation(asked.peer.clone(), now);
                self.effects.push(RelayEffect::Close(session));
                false
            }
        }
    }

    /// Records a step the pool returned to this reactor's own call, and
    /// applies its verdicts.
    fn route(&mut self, step: Step, now: ModelTime, pool: &mut PoolModel) {
        self.verdicts(&step, now, pool);
        self.pool_steps.push(step);
    }

    /// Applies the verdicts in `step` that answer bodies this reactor
    /// submitted.
    fn verdicts(&mut self, step: &Step, now: ModelTime, pool: &mut PoolModel) {
        for effect in &step.effects {
            let Effect::Verdict { waiter, verdict } = effect else {
                continue;
            };
            let found = self
                .wanted
                .iter()
                .find_map(|(key, wanted)| match wanted.fetch {
                    Fetch::Submitted {
                        waiter: submitted,
                        supplier,
                    } if submitted == *waiter => Some((*key, supplier)),
                    _ => None,
                });
            let Some((key, supplier)) = found else {
                continue;
            };
            // The body is no longer awaiting a verdict.
            if let Some(session) = self.sessions.get_mut(&supplier) {
                session.verifying = session.verifying.saturating_sub(1);
            }
            match verdict {
                ModelVerdict::Admitted => self.forget(key),
                ModelVerdict::Rejected { charge, .. } => {
                    self.forget(key);
                    if *charge == Charge::Ban && self.sessions.contains_key(&supplier) {
                        self.effects.push(RelayEffect::Close(supplier));
                    }
                }
                // The body is gone and so is its slot. The transaction may
                // be fine, so it is fetched again later, with every
                // announcer eligible again. Only so many times.
                ModelVerdict::LocalFault(_) => {
                    let wanted = self.wanted_at(key);
                    if wanted.refetches >= MAX_REFETCHES {
                        self.forget(key);
                    } else {
                        wanted.refetches = wanted.refetches.saturating_add(1);
                        wanted.attempts = 0;
                        wanted.tried.clear();
                        Self::back_off(wanted, now);
                    }
                }
                // The supplier entered a cooldown, so its body was not
                // verified and its slot is gone. This attempt failed like
                // any other.
                ModelVerdict::CoolingDown => self.move_on(key, now, pool),
                ModelVerdict::NoRoom(_) => {
                    panic!("a body submitted under a reservation cannot find the core full")
                }
            }
        }
    }

    /// Starts an attempt for every idle id that has someone to ask.
    fn pump(&mut self, now: ModelTime, pool: &mut PoolModel) {
        // An idle id is waiting for a request slot. With none free, nothing
        // can start.
        if !self.sessions.values().any(Session::has_room) {
            return;
        }
        let idle: Vec<u64> = self
            .wanted
            .iter()
            .filter(|(_, wanted)| matches!(wanted.fetch, Fetch::Idle))
            .map(|(key, _)| *key)
            .collect();
        for key in idle {
            self.ask(key, now, pool);
        }
    }

    /// Asks the next announcer for an id, reserving at the core first.
    fn ask(&mut self, key: u64, now: ModelTime, pool: &mut PoolModel) {
        let Some(wanted) = self.wanted.get(&key) else {
            return;
        };
        let id = wanted.id;
        let untried: Vec<SessionId> = wanted
            .announcers
            .iter()
            .copied()
            .filter(|session| !wanted.tried.contains(session))
            .collect();
        if wanted.attempts >= MAX_ATTEMPTS || untried.is_empty() {
            // Nobody is left to ask in this fetch.
            self.give_up(key, now, pool);
            return;
        }
        // Untried announcers that can take one more request and do not
        // already have this id outstanding from an earlier fetch. Peers this
        // node dialed first, then in the order they announced.
        let mut candidates: Vec<SessionId> = untried
            .into_iter()
            .filter(|session| {
                let session = &self.sessions[session];
                session.has_room() && !session.outstanding.contains(&id)
            })
            .collect();
        candidates.sort_by_key(|session| !self.sessions[session].dialed);

        let mut refused = false;
        for session in candidates {
            let peer = self.sessions[&session].peer.clone();
            if self.wanted[&key].reservation.is_none() {
                match pool.reserve(id, Source::Peer(peer), Intent::Fetch, now) {
                    Ok(reservation) => self.wanted_at(key).reservation = Some(reservation.id),
                    // The slot cannot be counted against this peer. Another
                    // announcer may have room.
                    Err(ModelUnreserved::NoRoom(NoRoom::Source) | ModelUnreserved::CoolingDown) => {
                        refused = true;
                        continue;
                    }
                    // The id is decided.
                    Err(ModelUnreserved::Pooled | ModelUnreserved::Rejected(_)) => {
                        self.forget(key);
                        return;
                    }
                    // The core is full or off, or another source is bringing
                    // the id in. Nothing is requested.
                    Err(
                        ModelUnreserved::NoRoom(NoRoom::Node)
                        | ModelUnreserved::Pending
                        | ModelUnreserved::Inactive,
                    ) => {
                        Self::back_off(self.wanted_at(key), now);
                        return;
                    }
                }
            }
            self.session_at(session).outstanding.push(id);
            let wanted = self.wanted_at(key);
            wanted.tried.push(session);
            wanted.attempts = wanted.attempts.saturating_add(1);
            wanted.fetch = Fetch::Asking {
                session,
                deadline: now.saturating_add(REQUEST_TIMEOUT),
            };
            self.effects.push(RelayEffect::Ask { session, id });
            return;
        }
        // Nobody was asked. If the core refused every announcer that had
        // room, try again after a delay. Otherwise the id waits for a
        // request slot, which an answer or a session end frees, and either
        // of those runs this again.
        if refused {
            Self::back_off(self.wanted_at(key), now);
        }
    }

    /// An attempt ended without a body. The id goes to its next announcer,
    /// or the node gives up on it.
    fn move_on(&mut self, key: u64, now: ModelTime, pool: &mut PoolModel) {
        self.wanted_at(key).fetch = Fetch::Idle;
        self.ask(key, now, pool);
    }

    /// Stops fetching an id and frees its slot at the core. The id is
    /// fetched again only if a peer announces it again.
    fn give_up(&mut self, key: u64, now: ModelTime, pool: &mut PoolModel) {
        if let Some(reservation) = self.wanted_at(key).reservation.take() {
            // Another caller may be waiting on this slot's body. Its verdict
            // is in the step, which is kept for it.
            let step = pool.release(reservation, now);
            self.pool_steps.push(step);
        }
        self.forget(key);
    }

    /// Drops an id from every announcer's pending set.
    fn forget(&mut self, key: u64) {
        let Some(wanted) = self.wanted.remove(&key) else {
            return;
        };
        self.wanted_of.remove(&wanted.id);
        for announcer in wanted.announcers {
            if let Some(session) = self.sessions.get_mut(&announcer) {
                session.pending = session.pending.saturating_sub(1);
            }
        }
    }

    fn back_off(wanted: &mut Wanted, now: ModelTime) {
        wanted.fetch = Fetch::Backoff {
            until: now.saturating_add(wanted.retry),
        };
        wanted.retry = wanted.retry.saturating_mul(2).min(RETRY_MAX);
    }

    fn session_at(&mut self, session: SessionId) -> &mut Session {
        self.sessions
            .get_mut(&session)
            .expect("the caller names a live session")
    }

    fn wanted_at(&mut self, key: u64) -> &mut Wanted {
        self.wanted
            .get_mut(&key)
            .expect("the caller names a wanted id")
    }

    fn fresh(&mut self) -> u64 {
        self.next = self.next.saturating_add(1);
        self.next
    }

    fn take_effects(&mut self) -> Vec<RelayEffect> {
        std::mem::take(&mut self.effects)
    }
}
