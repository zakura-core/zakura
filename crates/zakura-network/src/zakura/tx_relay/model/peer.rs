//! The model of one peer routine: everything about one peer while its
//! session is live.

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
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

/// A `GetTxs` this session sent with no answer yet.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct Request {
    /// The entry it was sent for.
    pub(crate) generation: EntryGen,
    /// The size the answer must have.
    pub(crate) size: u32,
    /// When it was sent.
    pub(crate) sent: Time,
    /// When another announcer may try. It only moves later.
    pub(crate) deadline: Time,
    /// The pool's reservation, while the node waits for this answer. `None`
    /// once the deadline passed or the pool cancelled the fetch: the request
    /// stays outstanding, and its answer is dropped.
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
    /// Ids it announced and has not been asked about, oldest first.
    pub(crate) pending: Vec<Pending>,
    /// Ids it answered `NotFound` for, oldest first, by entry generation.
    pub(crate) tried: VecDeque<(UnminedTxId, EntryGen)>,
    /// How often its request for an id timed out, by entry generation.
    pub(crate) timed_out: HashMap<(UnminedTxId, EntryGen), u32>,
    /// Its outstanding requests, oldest first.
    pub(crate) requests: IndexMap<UnminedTxId, Request>,
    /// Bodies it supplied that wait for a verdict.
    pub(crate) submissions: BTreeMap<WaiterId, (UnminedTxId, EntryGen)>,
    /// Ids this node announced to it, still in the pool. Full ids.
    pub(crate) announced: HashSet<UnminedTxId>,
    /// Ids it announced to us or supplied, oldest first, to skip adverts.
    /// The real set keeps keyed short ids. This one keeps full ids, which a
    /// collision can only make skip more.
    pub(crate) known: VecDeque<UnminedTxId>,
    /// Adverts held for the advert delay, with when they are due.
    pub(crate) held: Vec<(UnminedTxId, Time)>,
    /// Ids announced before this session had the go, with their sizes.
    /// They enter the table when it gets the go.
    pub(crate) waiting: Vec<(UnminedTxId, u32)>,
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
            requests: IndexMap::new(),
            submissions: BTreeMap::new(),
            announced: HashSet::new(),
            known: VecDeque::new(),
            held: Vec::new(),
            waiting: Vec::new(),
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

    /// Takes `id` out of the pending set, returning what it held.
    pub(crate) fn take_pending(&mut self, id: UnminedTxId) -> Option<Pending> {
        let index = self.pending.iter().position(|pending| pending.id == id)?;
        Some(self.pending.remove(index))
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

    /// Records that it announced or supplied `id`. Capped, forgetting the
    /// oldest, which only costs one redundant advert.
    pub(crate) fn mark_known(&mut self, id: UnminedTxId) {
        if self.known.contains(&id) {
            return;
        }
        if self.known.len() >= KNOWN_CAP {
            self.known.pop_front();
        }
        self.known.push_back(id);
    }

    /// Whether it announced or supplied `id`.
    pub(crate) fn knows(&self, id: UnminedTxId) -> bool {
        self.known.contains(&id)
    }

    /// Forgets `id` from every announcement set, because it left the pool.
    pub(crate) fn forget(&mut self, id: UnminedTxId) {
        self.announced.remove(&id);
        self.known.retain(|known| *known != id);
        self.held.retain(|(held, _)| *held != id);
    }

    /// Ids requested or awaiting a verdict on this session.
    pub(crate) fn in_flight(&self) -> usize {
        self.requests.len().saturating_add(self.submissions.len())
    }

    /// Sends a request for `pending` at `now` and moves every outstanding
    /// deadline out to cover the bytes now outstanding. Deadlines never move
    /// earlier.
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
                sent: now,
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
                .saturating_mul(1 << request.repeats.min(8));
            request.deadline = request.deadline.max(request.sent.saturating_add(span));
        }
    }
}
