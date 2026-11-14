//! The model of one peer routine: everything about one peer while its
//! session is live.

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    time::Duration,
};

use indexmap::IndexMap;
use zakura_chain::transaction::UnminedTxId;
use zakura_node_services::mempool::port::{model::WaiterId, ReservationId};

use super::{
    EntryGen, PeerKey, Time, BASE_DEADLINE, KNOWN_CAP, MIN_RATE, PENDING_PER_PEER,
    SENDER_ALLOWANCE, TRIED_PER_PEER,
};

/// What the peer said in its `Hello`.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct Hello {
    /// Requests it serves at once.
    pub(crate) serves: usize,
    /// The largest transaction it accepts.
    pub(crate) max_tx_bytes: u32,
}

/// One id announced by this peer and not yet tried.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct Pending {
    /// The id.
    pub(crate) id: UnminedTxId,
    /// The table entry it refers to.
    pub(crate) generation: EntryGen,
    /// The size this peer announced. Only this peer's body is held to it.
    pub(crate) size: u32,
}

/// A reserve call this session made and the pool has not answered.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct Reserving {
    /// The id's pending entry.
    pub(crate) pending: Pending,
    /// The backoff whose probe this call is, if it is one.
    pub(crate) probe: Option<u64>,
}

/// A `GetTxs` this session sent with no answer yet.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct Request {
    /// The entry it was sent for.
    pub(crate) generation: EntryGen,
    /// The size the answer must have.
    pub(crate) size: u32,
    /// When another announcer may try. It only moves later.
    pub(crate) deadline: Time,
    /// The pool's reservation, while the node waits for this answer. `None`
    /// once the deadline passed or the pool cancelled the fetch: the request
    /// stays outstanding, and its answer is dropped unread.
    pub(crate) reservation: Option<ReservationId>,
    /// How many earlier requests from this session for this id timed out.
    /// Each doubles the deadline.
    pub(crate) repeats: u32,
}

/// One peer's session.
#[derive(Clone, Debug)]
pub(crate) struct PeerModel {
    /// The peer.
    pub(crate) peer: PeerKey,
    /// Whether this node dialed it.
    pub(crate) dialed: bool,
    /// The peer's `Hello`, once it arrived.
    pub(crate) hello: Option<Hello>,
    /// Ids it announced and has not answered `NotFound` for, oldest first.
    /// A requested id stays here, so its peer can be asked again after a
    /// late answer.
    pub(crate) pending: Vec<Pending>,
    /// Ids it answered `NotFound` for, oldest first, by entry generation.
    pub(crate) tried: VecDeque<(UnminedTxId, EntryGen)>,
    /// How often its request for a pending id timed out. Only pending ids
    /// have an entry, so this is bounded by the pending set.
    pub(crate) timed_out: HashMap<(UnminedTxId, EntryGen), u32>,
    /// Reserve calls the pool has not answered.
    pub(crate) reserving: IndexMap<UnminedTxId, Reserving>,
    /// Its outstanding requests, oldest first.
    pub(crate) requests: IndexMap<UnminedTxId, Request>,
    /// Bodies it supplied that wait for a verdict.
    pub(crate) submissions: BTreeMap<WaiterId, (UnminedTxId, EntryGen)>,
    /// Ids this node announced to it and has not seen leave the pool, with
    /// the change-log position it announced them at. Full ids.
    pub(crate) announced: HashMap<UnminedTxId, u64>,
    /// Ids it announced to us or supplied, oldest first, with the change-log
    /// position when it did, to skip adverts. A removal earlier in the log
    /// does not erase a later announcement. The real set keeps keyed short
    /// ids, whose collisions can only make it skip more. This one keeps full
    /// ids.
    pub(crate) known: VecDeque<(UnminedTxId, u64)>,
    /// Adverts held for the advert delay, with when they are due.
    pub(crate) held: Vec<(UnminedTxId, Time)>,
    /// Ids announced before this session had the go, with their sizes,
    /// distinct and within the pending bound. They enter the table when it
    /// gets the go.
    pub(crate) waiting: IndexMap<UnminedTxId, u32>,
    /// The last change-log position this session read.
    pub(crate) cursor: u64,
    /// Ids dropped because the pending set was full.
    pub(crate) overflow: u64,
}

impl PeerModel {
    /// A new session of `peer`.
    pub(crate) fn new(peer: PeerKey, dialed: bool) -> Self {
        Self {
            peer,
            dialed,
            hello: None,
            pending: Vec::new(),
            tried: VecDeque::new(),
            timed_out: HashMap::new(),
            reserving: IndexMap::new(),
            requests: IndexMap::new(),
            submissions: BTreeMap::new(),
            announced: HashMap::new(),
            known: VecDeque::new(),
            held: Vec::new(),
            waiting: IndexMap::new(),
            cursor: 0,
            overflow: 0,
        }
    }

    /// Whether it holds `id` pending in `generation`.
    pub(crate) fn holds(&self, id: UnminedTxId, generation: EntryGen) -> bool {
        self.pending
            .iter()
            .any(|pending| pending.id == id && pending.generation == generation)
    }

    /// Whether it already answered `NotFound` for `id` in `generation`.
    pub(crate) fn has_tried(&self, id: UnminedTxId, generation: EntryGen) -> bool {
        self.tried.contains(&(id, generation))
    }

    /// Room left in its pending set.
    pub(crate) fn room(&self) -> usize {
        PENDING_PER_PEER.saturating_sub(self.pending.len())
    }

    /// Takes `id` out of the pending set if it is pending in `generation`.
    pub(crate) fn take_pending_of(
        &mut self,
        id: UnminedTxId,
        generation: EntryGen,
    ) -> Option<Pending> {
        if !self.holds(id, generation) {
            return None;
        }
        self.take_pending(id)
    }

    /// Takes `id` out of the pending set, returning what it held. Its timeout
    /// history goes with it.
    pub(crate) fn take_pending(&mut self, id: UnminedTxId) -> Option<Pending> {
        let index = self.pending.iter().position(|pending| pending.id == id)?;
        let pending = self.pending.remove(index);
        self.timed_out.remove(&(pending.id, pending.generation));
        Some(pending)
    }

    /// Drops every pending reference to `id` in `generation`, because it was
    /// decided.
    pub(crate) fn drop_decided(&mut self, id: UnminedTxId, generation: EntryGen) {
        self.pending
            .retain(|pending| !(pending.id == id && pending.generation == generation));
        self.timed_out.remove(&(id, generation));
    }

    /// Records that it answered `NotFound` for `id`. The tried set forgets
    /// its oldest entry when full, which only lets that peer be asked once
    /// more.
    pub(crate) fn mark_tried(&mut self, id: UnminedTxId, generation: EntryGen) {
        if self.has_tried(id, generation) {
            return;
        }
        if self.tried.len() >= TRIED_PER_PEER {
            self.tried.pop_front();
        }
        self.tried.push_back((id, generation));
    }

    /// Records that it announced or supplied `id`, at change-log position
    /// `seq`. Capped, forgetting the oldest, which only costs one redundant
    /// advert.
    pub(crate) fn mark_known(&mut self, id: UnminedTxId, seq: u64) {
        if let Some(entry) = self.known.iter_mut().find(|(known, _)| *known == id) {
            entry.1 = entry.1.max(seq);
            return;
        }
        if self.known.len() >= KNOWN_CAP {
            self.known.pop_front();
        }
        self.known.push_back((id, seq));
    }

    /// Whether it announced or supplied `id`.
    pub(crate) fn knows(&self, id: UnminedTxId) -> bool {
        self.known.iter().any(|(known, _)| *known == id)
    }

    /// `id` left the pool at change-log position `seq`. What this session
    /// learned about it before then goes. What it learned later stays.
    pub(crate) fn forget(&mut self, id: UnminedTxId, seq: u64) {
        if self.announced.get(&id).is_some_and(|at| *at < seq) {
            self.announced.remove(&id);
        }
        self.known
            .retain(|(known, at)| !(*known == id && *at < seq));
        self.held.retain(|(held, _)| *held != id);
    }

    /// Ids reserving, requested or awaiting a verdict on this session.
    pub(crate) fn in_flight(&self) -> usize {
        self.reserving
            .len()
            .saturating_add(self.requests.len())
            .saturating_add(self.submissions.len())
    }

    /// Sends a request for `pending` at `now`. Every outstanding request's
    /// deadline moves out to at least `now` plus the time to move the sender
    /// allowance and every body now outstanding, so the new work never eats
    /// into an older request's time. Deadlines never move earlier.
    pub(crate) fn send(&mut self, pending: Pending, reservation: ReservationId, now: Time) {
        let repeats = self
            .timed_out
            .get(&(pending.id, pending.generation))
            .copied()
            .unwrap_or(0);
        self.requests.insert(
            pending.id,
            Request {
                generation: pending.generation,
                size: pending.size,
                deadline: now,
                reservation: Some(reservation),
                repeats,
            },
        );
        let outstanding: u64 = self
            .requests
            .values()
            .map(|request| u64::from(request.size))
            .sum();
        let bytes = outstanding.saturating_add(SENDER_ALLOWANCE);
        let transfer = Duration::from_millis(bytes.saturating_mul(1_000) / MIN_RATE);
        for request in self.requests.values_mut() {
            let span = BASE_DEADLINE
                .saturating_add(transfer)
                .saturating_mul(2u32.saturating_pow(request.repeats));
            request.deadline = request.deadline.max(now.saturating_add(span));
        }
    }
}
