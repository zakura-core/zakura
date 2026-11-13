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

/// Requests outstanding to one peer, at most. A peer's `Hello` can lower it.
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

/// What the reactor does on the wire.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RelayEffect {
    /// Send `GetTxs` for `id` to `peer`.
    Ask {
        /// The peer.
        peer: QueueSource,
        /// The id.
        id: UnminedTxId,
    },
    /// Close the peer's connection for a violation.
    Close(QueueSource),
}

#[derive(Debug)]
struct Peer {
    id: QueueSource,
    dialed: bool,
    max_requests: usize,
    /// Requests on the wire with no answer yet, including ones that timed
    /// out.
    outstanding: Vec<UnminedTxId>,
    /// Undecided ids this peer has announced.
    pending: usize,
}

#[derive(Clone, Debug)]
enum Fetch {
    /// No attempt is in progress.
    Idle,
    /// `peer` was asked and has until `deadline`.
    Asking { peer: u64, deadline: ModelTime },
    /// A body is at the core.
    Submitted {
        waiter: WaiterId,
        supplier: QueueSource,
    },
    /// Waiting to try again.
    Backoff { until: ModelTime },
}

#[derive(Debug)]
struct Wanted {
    id: UnminedTxId,
    /// The peers that announced it, in the order they did.
    announcers: Vec<u64>,
    /// The announcers already asked.
    tried: Vec<u64>,
    attempts: usize,
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
    peers: BTreeMap<u64, Peer>,
    peer_of: HashMap<QueueSource, u64>,
    /// Every id this node wants, oldest first.
    wanted: BTreeMap<u64, Wanted>,
    wanted_of: HashMap<UnminedTxId, u64>,
    effects: Vec<RelayEffect>,
    started: Vec<JobId>,
    /// Announced ids dropped at a full pending set.
    pub(crate) overflowed: u64,
}

impl ReactorModel {
    /// A reactor that accepts transactions of at most `max_tx_bytes`.
    pub(crate) fn new(max_tx_bytes: u32) -> Self {
        Self {
            max_tx_bytes,
            next: 0,
            peers: BTreeMap::new(),
            peer_of: HashMap::new(),
            wanted: BTreeMap::new(),
            wanted_of: HashMap::new(),
            effects: Vec::new(),
            started: Vec::new(),
            overflowed: 0,
        }
    }

    // Inputs.

    /// A session is live. `requests_served` is what the peer's `Hello`
    /// stated.
    pub(crate) fn connect(&mut self, peer: QueueSource, dialed: bool, requests_served: usize) {
        let key = self.fresh();
        self.peer_of.insert(peer.clone(), key);
        self.peers.insert(
            key,
            Peer {
                id: peer,
                dialed,
                max_requests: requests_served.min(MAX_REQUESTS_PER_PEER),
                outstanding: Vec::new(),
                pending: 0,
            },
        );
    }

    /// `peer` announced `id` with serialized size `size`.
    pub(crate) fn announce(
        &mut self,
        peer: &QueueSource,
        id: UnminedTxId,
        size: u32,
        now: ModelTime,
        pool: &mut PoolModel,
    ) -> Vec<RelayEffect> {
        let Some(peer_key) = self.peer_of.get(peer).copied() else {
            return Vec::new();
        };
        if size > self.max_tx_bytes {
            return Vec::new();
        }
        let already_announced = self
            .wanted_of
            .get(&id)
            .is_some_and(|key| self.wanted[key].announcers.contains(&peer_key));
        if already_announced {
            return Vec::new();
        }
        let announcer = self.peer_at(peer_key);
        if announcer.pending >= PENDING_PER_PEER {
            // Honest announcers overlap, so this is dropped and counted.
            self.overflowed = self.overflowed.saturating_add(1);
            return Vec::new();
        }
        announcer.pending = announcer.pending.saturating_add(1);
        match self.wanted_of.get(&id).copied() {
            Some(key) => self.wanted_at(key).announcers.push(peer_key),
            None => {
                let key = self.fresh();
                self.wanted.insert(
                    key,
                    Wanted {
                        id,
                        announcers: vec![peer_key],
                        tried: Vec::new(),
                        attempts: 0,
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

    /// `peer` sent a `Tx` that decodes to `tx`.
    pub(crate) fn tx(
        &mut self,
        peer: &QueueSource,
        tx: ModelTx,
        now: ModelTime,
        pool: &mut PoolModel,
    ) -> Vec<RelayEffect> {
        if !self.answered(peer, tx.id, now, pool) {
            return self.take_effects();
        }
        let id = tx.id;
        if let Some(key) = self.wanted_of.get(&id).copied() {
            let wanted = self.wanted_at(key);
            // A body is already at the core: this one answered an earlier
            // attempt. It is dropped with no penalty.
            if !matches!(wanted.fetch, Fetch::Submitted { .. }) {
                let supplier = Supplier::Peer(peer.clone());
                let arrival = match wanted.reservation.take() {
                    Some(reservation) => Some(pool.submit(reservation, tx, supplier, now)),
                    // The slot was used by a body that hit a local fault.
                    // This body is in hand, so it is delivered.
                    None => {
                        match pool.reserve(id, Source::Peer(peer.clone()), Intent::Deliver, now) {
                            Ok(joined) if joined.wants_body => {
                                Some(pool.submit(joined.id, tx, supplier, now))
                            }
                            Ok(joined) => Some(pool.wait(joined.id, id, supplier, now)),
                            Err(_) => None,
                        }
                    }
                };
                if let Some((waiter, step)) = arrival {
                    self.wanted_at(key).fetch = Fetch::Submitted {
                        waiter,
                        supplier: peer.clone(),
                    };
                    self.pool_step(&step, now, pool);
                }
            }
        }
        self.pump(now, pool);
        self.take_effects()
    }

    /// `peer` sent a `NotFound` for `id`.
    pub(crate) fn not_found(
        &mut self,
        peer: &QueueSource,
        id: UnminedTxId,
        now: ModelTime,
        pool: &mut PoolModel,
    ) -> Vec<RelayEffect> {
        if !self.answered(peer, id, now, pool) {
            return self.take_effects();
        }
        let peer_key = self.peer_of[peer];
        if let Some(key) = self.wanted_of.get(&id).copied() {
            if matches!(self.wanted[&key].fetch, Fetch::Asking { peer, .. } if peer == peer_key) {
                self.move_on(key, now, pool);
            }
        }
        self.pump(now, pool);
        self.take_effects()
    }

    /// Something the pool did: verdicts for bodies this reactor submitted,
    /// and jobs it started.
    pub(crate) fn pool_step(&mut self, step: &Step, now: ModelTime, pool: &mut PoolModel) {
        self.started.extend(step.started());
        for effect in &step.effects {
            let Effect::Verdict { waiter, verdict } = effect else {
                continue;
            };
            let found = self
                .wanted
                .iter()
                .find_map(|(key, wanted)| match &wanted.fetch {
                    Fetch::Submitted {
                        waiter: submitted,
                        supplier,
                    } if submitted == waiter => Some((*key, supplier.clone())),
                    _ => None,
                });
            let Some((key, supplier)) = found else {
                continue;
            };
            match verdict {
                ModelVerdict::Admitted => self.forget(key),
                ModelVerdict::Rejected { charge, .. } => {
                    self.forget(key);
                    if *charge == Charge::Ban {
                        self.effects.push(RelayEffect::Close(supplier));
                    }
                }
                // The body is gone and so is its slot. Fetch it again later,
                // from any announcer.
                ModelVerdict::LocalFault(_) => {
                    let wanted = self.wanted_at(key);
                    wanted.tried.clear();
                    Self::back_off(wanted, now);
                }
                // The supplier entered a cooldown, so its body was not
                // verified. Another announcer is asked after a delay.
                ModelVerdict::CoolingDown => Self::back_off(self.wanted_at(key), now),
                ModelVerdict::NoRoom(_) => {
                    panic!("a body submitted under a reservation cannot find the core full")
                }
            }
        }
        self.pump(now, pool);
    }

    /// Time passed.
    pub(crate) fn tick(&mut self, now: ModelTime, pool: &mut PoolModel) -> Vec<RelayEffect> {
        let keys: Vec<u64> = self.wanted.keys().copied().collect();
        for key in keys {
            match self.wanted[&key].fetch {
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

    /// `peer`'s session ended.
    pub(crate) fn end(
        &mut self,
        peer: &QueueSource,
        now: ModelTime,
        pool: &mut PoolModel,
    ) -> Vec<RelayEffect> {
        let Some(peer_key) = self.peer_of.remove(peer) else {
            return Vec::new();
        };
        self.peers.remove(&peer_key);
        let keys: Vec<u64> = self.wanted.keys().copied().collect();
        for key in keys {
            let wanted = self.wanted_at(key);
            wanted.announcers.retain(|announcer| *announcer != peer_key);
            wanted.tried.retain(|tried| *tried != peer_key);
            let asking_it = matches!(wanted.fetch, Fetch::Asking { peer, .. } if peer == peer_key);
            let nobody_left =
                wanted.announcers.is_empty() && !matches!(wanted.fetch, Fetch::Submitted { .. });
            if nobody_left {
                self.give_up(key, now, pool);
            } else if asking_it {
                self.move_on(key, now, pool);
            }
        }
        self.pump(now, pool);
        self.take_effects()
    }

    // Observations.

    /// The jobs the pool started since the last call.
    pub(crate) fn take_started(&mut self) -> Vec<JobId> {
        std::mem::take(&mut self.started)
    }

    /// Whether the node still wants `id`.
    pub(crate) fn wants(&self, id: UnminedTxId) -> bool {
        self.wanted_of.contains_key(&id)
    }

    /// Requests outstanding to `peer`.
    pub(crate) fn outstanding(&self, peer: &QueueSource) -> usize {
        self.peer_of
            .get(peer)
            .map_or(0, |key| self.peers[key].outstanding.len())
    }

    /// Checks the invariants every reachable state keeps.
    ///
    /// # Panics
    ///
    /// If one is broken.
    pub(crate) fn assert_invariants(&self) {
        for peer in self.peers.values() {
            assert!(
                peer.outstanding.len() <= peer.max_requests,
                "tx-relay/in-flight-within-caps: requests outstanding to one peer"
            );
            assert!(
                peer.pending <= PENDING_PER_PEER,
                "tx-relay/pending-within-bound"
            );
            for (index, id) in peer.outstanding.iter().enumerate() {
                assert!(
                    !peer.outstanding[index.saturating_add(1)..].contains(id),
                    "no id is outstanding twice to one peer"
                );
            }
        }
        for wanted in self.wanted.values() {
            assert!(wanted.attempts <= MAX_ATTEMPTS, "attempts for one id");
            if let Fetch::Asking { peer, .. } = wanted.fetch {
                assert!(
                    self.peers[&peer].outstanding.contains(&wanted.id),
                    "the peer being asked has the request outstanding"
                );
                assert!(
                    wanted.reservation.is_some(),
                    "no request without a reservation"
                );
            }
        }
    }

    // Rules.

    /// Checks that `id` answers a request outstanding to `peer`, and frees
    /// that request. Anything else is a violation: the connection closes and
    /// the core starts the peer's cooldown.
    fn answered(
        &mut self,
        peer: &QueueSource,
        id: UnminedTxId,
        now: ModelTime,
        pool: &mut PoolModel,
    ) -> bool {
        let Some(peer_key) = self.peer_of.get(peer).copied() else {
            return false;
        };
        let outstanding = &mut self.peer_at(peer_key).outstanding;
        match outstanding.iter().position(|asked| *asked == id) {
            Some(index) => {
                outstanding.remove(index);
                true
            }
            None => {
                pool.violation(peer.clone(), now);
                self.effects.push(RelayEffect::Close(peer.clone()));
                false
            }
        }
    }

    /// Starts an attempt for every idle id that has someone to ask.
    fn pump(&mut self, now: ModelTime, pool: &mut PoolModel) {
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
        let id = self.wanted[&key].id;
        // Announcers not yet asked that have a free request slot, peers this
        // node dialed first, then in the order they announced.
        let mut candidates: Vec<u64> = self.wanted[&key]
            .announcers
            .iter()
            .copied()
            .filter(|peer| !self.wanted[&key].tried.contains(peer))
            .filter(|peer| {
                let peer = &self.peers[peer];
                peer.outstanding.len() < peer.max_requests
            })
            .collect();
        candidates.sort_by_key(|peer| !self.peers[peer].dialed);

        for peer_key in candidates {
            let peer = self.peers[&peer_key].id.clone();
            if self.wanted[&key].reservation.is_none() {
                match pool.reserve(id, Source::Peer(peer.clone()), Intent::Fetch, now) {
                    Ok(reservation) => self.wanted_at(key).reservation = Some(reservation.id),
                    // The slot cannot be counted against this peer. Another
                    // announcer may have room.
                    Err(ModelUnreserved::NoRoom(NoRoom::Source) | ModelUnreserved::CoolingDown) => {
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
            self.peer_at(peer_key).outstanding.push(id);
            let wanted = self.wanted_at(key);
            wanted.tried.push(peer_key);
            wanted.attempts = wanted.attempts.saturating_add(1);
            wanted.fetch = Fetch::Asking {
                peer: peer_key,
                deadline: now.saturating_add(REQUEST_TIMEOUT),
            };
            self.effects.push(RelayEffect::Ask { peer, id });
            return;
        }
    }

    /// An attempt ended without a body. The id goes to its next announcer,
    /// or the node gives up on it.
    fn move_on(&mut self, key: u64, now: ModelTime, pool: &mut PoolModel) {
        let wanted = &self.wanted[&key];
        let untried = wanted
            .announcers
            .iter()
            .any(|announcer| !wanted.tried.contains(announcer));
        if wanted.attempts >= MAX_ATTEMPTS || !untried {
            self.give_up(key, now, pool);
        } else {
            self.wanted_at(key).fetch = Fetch::Idle;
        }
    }

    /// Stops fetching an id and frees its slot at the core.
    fn give_up(&mut self, key: u64, now: ModelTime, pool: &mut PoolModel) {
        if let Some(reservation) = self.wanted_at(key).reservation.take() {
            let step = pool.release(reservation, now);
            self.started.extend(step.started());
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
            if let Some(peer) = self.peers.get_mut(&announcer) {
                peer.pending = peer.pending.saturating_sub(1);
            }
        }
    }

    fn back_off(wanted: &mut Wanted, now: ModelTime) {
        wanted.fetch = Fetch::Backoff {
            until: now.saturating_add(wanted.retry),
        };
        wanted.retry = wanted.retry.saturating_mul(2).min(RETRY_MAX);
    }

    fn peer_at(&mut self, key: u64) -> &mut Peer {
        self.peers
            .get_mut(&key)
            .expect("the caller names a live peer")
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
