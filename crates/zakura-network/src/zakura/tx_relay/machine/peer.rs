//! One peer routine's state: everything about one peer while its session
//! is live.

use std::{
    collections::{BTreeMap, VecDeque},
    ops::Bound,
    time::Duration,
};

use indexmap::{IndexMap, IndexSet};
use zakura_chain::transaction::UnminedTxId;
use zakura_node_services::mempool::port::ReservationId;
use zakura_rt::collections::{HashMap, HashSet};

#[cfg(doc)]
use super::MAX_SERVES;
use super::{
    CallId, EntryGen, PeerKey, Time, WaiterId, BASE_DEADLINE, INITIAL_WINDOW, KNOWN_CAP,
    MAX_OUTSTANDING_BYTES, MIN_RATE, PENDING_PER_PEER, SENDER_ALLOWANCE,
};

/// What the peer said in its `Hello`.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct Hello {
    /// Requests it serves at once.
    pub(crate) serves: usize,
    /// The largest transaction it accepts.
    pub(crate) max_tx_bytes: u32,
}

/// One id announced by this peer and not yet answered `NotFound` for.
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
    /// Names the call.
    pub(crate) call: CallId,
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
    /// When another announcer may try. It only moves later, and never past
    /// [`max_span`] after `sent`.
    pub(crate) deadline: Time,
    /// The pool's reservation, while the node waits for this answer. `None`
    /// once the deadline passed or the pool cancelled the fetch: the request
    /// stays outstanding, holding its slot, and its answer is dropped unread.
    pub(crate) reservation: Option<ReservationId>,
    /// Set when it misses its deadline: one more deadline later. If the
    /// answer has still not come by then, the session ends, which is the
    /// only way its slot frees, since a request on the wire stays written
    /// until it is answered. Later requests never move it.
    pub(crate) gives_up_at: Option<Time>,
    /// When this session first read the id's admission while the request
    /// was out, and so skipped its advert. If the peer then answers
    /// `NotFound`, the advert's delay counts from here.
    pub(crate) admission_seen: Option<Time>,
}

/// One peer's session.
#[derive(Clone, Debug)]
pub(crate) struct PeerState {
    /// The peer.
    pub(crate) peer: PeerKey,
    /// The peer's `Hello`, once it arrived.
    pub(crate) hello: Option<Hello>,
    /// Ids it announced and its peer has not tried, oldest first. A
    /// requested id stays here until the attempt ends.
    pub(crate) pending: PendingSet,
    /// Reserve calls the pool has not answered.
    pub(crate) reserving: IndexMap<UnminedTxId, Reserving>,
    /// Its outstanding requests, oldest first.
    pub(crate) requests: IndexMap<UnminedTxId, Request>,
    /// Its requests to us that we serve, until the IO starts writing each
    /// answer: at most what our `Hello` serves, [`MAX_SERVES`]. A request
    /// answered `NotFound` is answered in its own step, so it never stays.
    pub(crate) serving: HashSet<UnminedTxId>,
    /// Bodies it supplied that wait for a verdict.
    pub(crate) submissions: Submissions,
    /// Ids this node announced to it and has not seen leave the pool, with
    /// the change-log position it announced them at, and which of them are
    /// still queued for an `Inv`. Full ids.
    pub(crate) announced: Announced,
    /// The short ids of what it announced to us or supplied, oldest first,
    /// with the change-log position when it did, to skip adverts. A removal
    /// earlier in the log does not erase a later announcement. The set knows
    /// only short ids, so a collision has two costs, both limited to this
    /// peer: one advert skipped that should have been sent, or, after one of
    /// the colliding ids leaves the pool, one redundant advert of the other.
    pub(crate) known: Known,
    /// Adverts held for the advert delay, with when they are due.
    pub(crate) held: Held,
    /// Ids announced before this session had both the go and its `Hello`,
    /// with their sizes, distinct and within the pending bound. They enter
    /// the table once it has both.
    pub(crate) waiting: IndexMap<UnminedTxId, u32>,
    /// The last change-log position this session read.
    pub(crate) cursor: u64,
    /// Ids it announced again while an old request for them was
    /// outstanding, with the size it announced, so that request's
    /// `NotFound` leaves what the peer told this node alone and the
    /// announcement is taken afresh once the request ends. At most one per
    /// outstanding request.
    pub(crate) reannounced: HashMap<UnminedTxId, u32>,
    /// Ids dropped because the pending set was full.
    pub(crate) overflow: u64,
    /// The request window: how many ids it may have reserving, requested
    /// or awaiting a verdict, with what its peer's ended sessions left. See
    /// the module docs' request limit.
    pub(crate) window: usize,
}

impl PeerState {
    /// A new session of `peer`.
    pub(crate) fn new(peer: PeerKey) -> Self {
        Self {
            peer,
            hello: None,
            pending: PendingSet::default(),
            reserving: IndexMap::new(),
            requests: IndexMap::new(),
            serving: HashSet::default(),
            submissions: Submissions::default(),
            announced: Announced::default(),
            known: Known::default(),
            held: Held::default(),
            waiting: IndexMap::new(),
            cursor: 0,
            reannounced: HashMap::default(),
            overflow: 0,
            window: INITIAL_WINDOW,
        }
    }

    /// Whether it holds `id` pending in `generation`.
    pub(crate) fn holds(&self, id: UnminedTxId, generation: EntryGen) -> bool {
        self.pending.holds(id, generation)
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

    /// Takes `id` out of the pending set, returning what it held.
    pub(crate) fn take_pending(&mut self, id: UnminedTxId) -> Option<Pending> {
        self.pending.take(id)
    }

    /// Drops every pending reference to `id` in `generation`, because it was
    /// decided.
    pub(crate) fn drop_decided(&mut self, id: UnminedTxId, generation: EntryGen) {
        self.pending.drop_entry(id, generation);
    }

    /// Records that it announced or supplied `id`, whose short id is
    /// `short`, at change-log position `seq`, as the newest entry. Capped,
    /// forgetting the one least recently recorded, which only costs one
    /// redundant advert.
    pub(crate) fn mark_known(&mut self, id: UnminedTxId, short: u64, seq: u64) {
        self.mark_all_known([(id, short)], seq);
    }

    /// Records each of `ids`, given with its short id, as
    /// [`Self::mark_known`] does one. An advert of one still queued for an
    /// `Inv` would go back to a peer that has it, so those are withdrawn, in
    /// one pass over the queue however many an `Inv` names. One already
    /// sent stays, so the peer may still request the id.
    pub(crate) fn mark_all_known(
        &mut self,
        ids: impl IntoIterator<Item = (UnminedTxId, u64)>,
        seq: u64,
    ) {
        let mut queued: HashSet<UnminedTxId> = HashSet::default();
        for (id, short) in ids {
            self.known.mark(short, seq);
            if self.announced.contains_key(&id) && !self.announced.is_sent(&id) {
                queued.insert(id);
            }
        }
        if !queued.is_empty() {
            self.announced.retain(|id, _| !queued.contains(id));
        }
    }

    /// Forgets that it announced an id with short id `short`.
    pub(crate) fn forget_known(&mut self, short: u64) {
        self.known.forget(short);
    }

    /// Whether it announced or supplied an id with short id `short`.
    pub(crate) fn knows(&self, short: u64) -> bool {
        self.known.contains(short)
    }

    /// Whether this peer must not be told about `id`, with short id `short`:
    /// it announced or supplied it, as the skip set says, or it still has it
    /// pending, waiting for the go, or in flight with this node. Pending and in-flight ids are
    /// checked directly, so announcements that overflow the skip set never
    /// drop them.
    pub(crate) fn skips(&self, id: UnminedTxId, short: u64) -> bool {
        self.knows(short)
            || self.pending.contains(id)
            || self.waiting.contains_key(&id)
            || self.reserving.contains_key(&id)
            || self.requests.contains_key(&id)
            || self.submissions.contains(id)
    }

    /// `id`, with short id `short`, left the pool at change-log position
    /// `seq`. What this session learned about it before then goes. What it
    /// learned later stays. A re-admission is a new one, so an outstanding
    /// request forgets when it saw the old one.
    pub(crate) fn forget(&mut self, id: UnminedTxId, short: u64, seq: u64) {
        if self.announced.get(&id).is_some_and(|at| *at < seq) {
            self.announced.remove(&id);
        }
        if let Some(request) = self.requests.get_mut(&id) {
            request.admission_seen = None;
        }
        self.known.forget_before(short, seq);
        self.held.remove(id);
    }

    /// The announced bytes of what it is asked for or reserving.
    pub(crate) fn asked_bytes(&self) -> u64 {
        let requested = self.requests.values().map(|request| request.size);
        let reserving = self.reserving.values().map(|call| call.pending.size);
        requested.chain(reserving).map(u64::from).sum()
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
    /// allowance and every body now outstanding, but never past its own sent
    /// time plus [`max_span`], so a slow peer that sends later bodies first
    /// can time out on an earlier one. Deadlines never move earlier.
    pub(crate) fn send(&mut self, pending: Pending, reservation: ReservationId, now: Time) {
        self.requests.insert(
            pending.id,
            Request {
                generation: pending.generation,
                size: pending.size,
                sent: now,
                deadline: now,
                reservation: Some(reservation),
                gives_up_at: None,
                admission_seen: None,
            },
        );
        let outstanding: u64 = self
            .requests
            .values()
            .map(|request| u64::from(request.size))
            .sum();
        let bytes = outstanding.saturating_add(SENDER_ALLOWANCE);
        let transfer = Duration::from_millis(bytes.saturating_mul(1_000) / MIN_RATE);
        // A request that missed its deadline keeps the one it missed.
        for request in self
            .requests
            .values_mut()
            .filter(|request| request.gives_up_at.is_none())
        {
            let span = BASE_DEADLINE.saturating_add(transfer);
            // Later requests can push a deadline out, but only as far as the
            // most bytes a peer can have outstanding at once, so a peer that
            // keeps adding requests cannot hold an id forever.
            let cap = request.sent.saturating_add(max_span());
            request.deadline = request.deadline.max(now.saturating_add(span)).min(cap);
        }
    }
}

/// The longest a request can wait after it was sent before another
/// announcer may try: the fixed part plus the time to move the sender
/// allowance and the most bytes a session asks for at once, at the slowest
/// supported rate.
pub(crate) fn max_span() -> Duration {
    let bytes = MAX_OUTSTANDING_BYTES.saturating_add(SENDER_ALLOWANCE);
    let transfer = Duration::from_millis(bytes.saturating_mul(1_000) / MIN_RATE);
    BASE_DEADLINE.saturating_add(transfer)
}

/// The ids a node announced to one peer: when it announced each, and which
/// are still queued for an `Inv` frame.
///
/// An announcement is queued when the machine decides it and sent when
/// [`Announced::take`] puts it in a frame. Only a sent id is served. Every
/// change to the set goes through here, so a queued id leaves the queue
/// exactly when it stops being announced.
#[derive(Clone, Debug, Default)]
pub(crate) struct Announced {
    at: HashMap<UnminedTxId, u64>,
    queued: IndexSet<UnminedTxId>,
}

impl Announced {
    /// Whether `id` is announced, queued or sent.
    pub(crate) fn contains_key(&self, id: &UnminedTxId) -> bool {
        self.at.contains_key(id)
    }

    /// The change-log position `id` was announced at.
    pub(crate) fn get(&self, id: &UnminedTxId) -> Option<&u64> {
        self.at.get(id)
    }

    /// Announces `id` at change-log position `seq`, queued for an `Inv`.
    pub(crate) fn insert(&mut self, id: UnminedTxId, seq: u64) {
        self.at.insert(id, seq);
        self.queued.insert(id);
    }

    /// Withdraws `id`'s announcement, sent or not.
    pub(crate) fn remove(&mut self, id: &UnminedTxId) {
        if self.at.remove(id).is_some() {
            self.queued.shift_remove(id);
        }
    }

    /// Keeps only the announcements `keep` accepts.
    pub(crate) fn retain(&mut self, mut keep: impl FnMut(&UnminedTxId, &mut u64) -> bool) {
        self.at.retain(|id, seq| keep(id, seq));
        let at = &self.at;
        self.queued.retain(|id| at.contains_key(id));
    }

    /// Every announcement and its change-log position.
    #[cfg(test)]
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&UnminedTxId, &u64)> {
        self.at.iter()
    }

    /// Whether `id` went out in an `Inv`.
    pub(crate) fn is_sent(&self, id: &UnminedTxId) -> bool {
        self.at.contains_key(id) && !self.queued.contains(id)
    }

    /// How many announcements wait for an `Inv`.
    pub(crate) fn queued(&self) -> usize {
        self.queued.len()
    }

    /// Takes up to `max` queued ids that `size` gives a size for, oldest
    /// first, and marks them sent. An id `size` skips stays queued, and so
    /// does one with a parent in `parents` that still waits to go out to
    /// this peer: queued with a size and not taken before it, or one
    /// `held` says is held. So a child never goes out ahead of its parent.
    pub(crate) fn take(
        &mut self,
        max: usize,
        size: impl Fn(&UnminedTxId) -> Option<u32>,
        parents: impl Fn(&UnminedTxId) -> Vec<UnminedTxId>,
        held: impl Fn(&UnminedTxId) -> bool,
    ) -> Vec<(UnminedTxId, u32)> {
        let mut taken = Vec::new();
        let mut taken_ids: HashSet<UnminedTxId> = HashSet::default();
        for id in &self.queued {
            if taken.len() == max {
                break;
            }
            let Some(id_size) = size(id) else {
                continue;
            };
            let waits = |parent: &UnminedTxId| {
                held(parent)
                    || (self.queued.contains(parent)
                        && !taken_ids.contains(parent)
                        && size(parent).is_some())
            };
            if parents(id).iter().any(waits) {
                continue;
            }
            taken.push((*id, id_size));
            taken_ids.insert(*id);
        }
        self.queued.retain(|id| !taken_ids.contains(id));
        taken
    }
}

/// Where a pending entry stands in line to be claimed: when it became ready,
/// then its place among entries ready at the same time. See
/// `TxRelayMachine::ready_at`.
pub(crate) type ReadyKey = (Time, u64);

/// A session's pending ids: one reference per id, keyed by id, so a
/// membership check is one lookup.
///
/// It also files the pending entries this session may claim now by their
/// [`ReadyKey`], so the next claim is the first filed entry, not a scan.
/// The machine files and unfiles them as entries change; see
/// `TxRelayMachine::refile`. An entry is filed at most once per id, and
/// only while this set holds it in the filed generation.
///
/// Both maps are B-trees: at a full set a hash map rounds up to twice the
/// buckets it needs, and the ordered walk keeps every pass over the set
/// deterministic.
#[derive(Clone, Debug, Default)]
pub(crate) struct PendingSet {
    held: BTreeMap<OrdId, PendingSlot>,
    ready: BTreeMap<ReadyKey, UnminedTxId>,
}

/// An id ordered by its bytes, mined id first, so a B-tree can key on it.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct OrdId(UnminedTxId);

impl Ord for OrdId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let bytes = |id: &UnminedTxId| (id.mined_id().0, id.auth_digest().map(|digest| digest.0));
        bytes(&self.0).cmp(&bytes(&other.0))
    }
}

impl PartialOrd for OrdId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// What a [`PendingSet`] keeps for one id.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct PendingSlot {
    generation: EntryGen,
    size: u32,
    /// Where it is filed, if it is.
    filed: Option<ReadyKey>,
}

impl PendingSlot {
    /// The pending entry `id` names.
    fn pending(&self, id: UnminedTxId) -> Pending {
        Pending {
            id,
            generation: self.generation,
            size: self.size,
        }
    }
}

impl PendingSet {
    /// The pending ids, in id order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = Pending> + '_ {
        self.held.iter().map(|(id, held)| held.pending(id.0))
    }

    /// How many are pending.
    pub(crate) fn len(&self) -> usize {
        self.held.len()
    }

    /// Adds `pending`, unfiled until the machine files it. The set must not
    /// already hold its id: a stale reference goes first.
    pub(crate) fn push(&mut self, pending: Pending) {
        let old = self.held.insert(
            OrdId(pending.id),
            PendingSlot {
                generation: pending.generation,
                size: pending.size,
                filed: None,
            },
        );
        debug_assert!(old.is_none(), "one reference per id");
        if let Some(key) = old.and_then(|old| old.filed) {
            self.ready.remove(&key);
        }
    }

    /// Whether `id` is pending, in any generation.
    pub(crate) fn contains(&self, id: UnminedTxId) -> bool {
        self.held.contains_key(&OrdId(id))
    }

    /// Whether `id` is pending in `generation`.
    pub(crate) fn holds(&self, id: UnminedTxId, generation: EntryGen) -> bool {
        self.held
            .get(&OrdId(id))
            .is_some_and(|held| held.generation == generation)
    }

    /// The pending reference to `id` in `generation`.
    pub(crate) fn get(&self, id: UnminedTxId, generation: EntryGen) -> Option<Pending> {
        self.of(id)
            .filter(|pending| pending.generation == generation)
    }

    /// The pending reference to `id`, in any generation.
    pub(crate) fn of(&self, id: UnminedTxId) -> Option<Pending> {
        self.held.get(&OrdId(id)).map(|held| held.pending(id))
    }

    /// Takes the pending reference to `id` out, and unfiles it.
    pub(crate) fn take(&mut self, id: UnminedTxId) -> Option<Pending> {
        let held = self.held.remove(&OrdId(id))?;
        if let Some(key) = held.filed {
            self.ready.remove(&key);
        }
        Some(held.pending(id))
    }

    /// Drops the pending reference to `id` if it is in `generation`.
    pub(crate) fn drop_entry(&mut self, id: UnminedTxId, generation: EntryGen) {
        if self.holds(id, generation) {
            self.take(id);
        }
    }

    /// Files `pending` at `key`, in place of any earlier filing of its id.
    /// The set holds `pending`.
    pub(crate) fn file(&mut self, pending: Pending, key: ReadyKey) {
        let Some(held) = self.held.get_mut(&OrdId(pending.id)) else {
            return;
        };
        if let Some(old) = held.filed.replace(key) {
            if old == key {
                return;
            }
            self.ready.remove(&old);
        }
        self.ready.insert(key, pending.id);
    }

    /// Unfiles `id`, if it is filed.
    pub(crate) fn unfile(&mut self, id: UnminedTxId) {
        if let Some(key) = self
            .held
            .get_mut(&OrdId(id))
            .and_then(|held| held.filed.take())
        {
            self.ready.remove(&key);
        }
    }

    /// The filed entries, first in line first.
    pub(crate) fn ready(&self) -> impl Iterator<Item = (ReadyKey, Pending)> + '_ {
        self.ready.iter().map(|(key, id)| {
            let held = self.held.get(&OrdId(*id)).expect("every filed id is held");
            (*key, held.pending(*id))
        })
    }

    /// When the first entry not yet ready at `now` becomes ready, found
    /// without walking the entries that already are.
    pub(crate) fn next_ready_after(&self, now: Time) -> Option<Time> {
        let after = (Bound::Excluded((now, u64::MAX)), Bound::Unbounded);
        self.ready.range(after).next().map(|((ready, _), _)| *ready)
    }

    /// Each filed id, its place in line, and what the line holds there.
    #[cfg(test)]
    pub(crate) fn filed(&self) -> Vec<(UnminedTxId, ReadyKey, Option<Pending>)> {
        self.held
            .iter()
            .filter_map(|(id, held)| Some((id.0, held.filed?)))
            .map(|(id, key)| {
                let at = self.ready.get(&key).and_then(|at| self.of(*at));
                (id, key, at)
            })
            .collect()
    }
}

/// Lowers `key`'s count in the ordered `counts`, removing it at zero.
fn decrement_ordered<K: Ord>(counts: &mut BTreeMap<K, usize>, key: K) {
    if let Some(count) = counts.get_mut(&key) {
        *count = count.saturating_sub(1);
        if *count == 0 {
            counts.remove(&key);
        }
    }
}

/// Lowers `key`'s count in `counts`, removing it at zero.
fn decrement<K: std::hash::Hash + Eq>(counts: &mut HashMap<K, usize>, key: K) {
    if let Some(count) = counts.get_mut(&key) {
        *count = count.saturating_sub(1);
        if *count == 0 {
            counts.remove(&key);
        }
    }
}

/// The short ids a peer announced to us or supplied, oldest first, with
/// the change-log position when it did, indexed by short id.
#[derive(Clone, Debug, Default)]
pub(crate) struct Known {
    /// Short ids by when they were last recorded.
    order: BTreeMap<u64, u64>,
    /// Each short id's place in `order` and its change-log position.
    by_short: HashMap<u64, (u64, u64)>,
    next: u64,
}

impl Known {
    /// Records `short` at `seq` as the newest, keeping the later position if
    /// it was already known, and forgetting the oldest past [`KNOWN_CAP`].
    pub(crate) fn mark(&mut self, short: u64, seq: u64) {
        let mut seq = seq;
        if let Some((place, old)) = self.by_short.remove(&short) {
            self.order.remove(&place);
            seq = seq.max(old);
        }
        if self.by_short.len() >= KNOWN_CAP {
            if let Some((_, oldest)) = self.order.pop_first() {
                self.by_short.remove(&oldest);
            }
        }
        self.next = self.next.saturating_add(1);
        self.order.insert(self.next, short);
        self.by_short.insert(short, (self.next, seq));
    }

    /// Whether `short` is known.
    pub(crate) fn contains(&self, short: u64) -> bool {
        self.by_short.contains_key(&short)
    }

    /// How many short ids are known.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.by_short.len()
    }

    /// Forgets `short`.
    pub(crate) fn forget(&mut self, short: u64) {
        if let Some((place, _)) = self.by_short.remove(&short) {
            self.order.remove(&place);
        }
    }

    /// Forgets `short` if it was recorded before change-log position `seq`.
    pub(crate) fn forget_before(&mut self, short: u64, seq: u64) {
        if self.by_short.get(&short).is_some_and(|(_, at)| *at < seq) {
            self.forget(short);
        }
    }
}

/// Adverts held for the advert delay, with when they are due and their
/// ids' short ids, in the order they come due, indexed by id. Taking the due
/// ones reads only them, and an advert due no earlier than the last, as one
/// held for the fixed delay is, joins at the back without shifting the
/// others.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Held {
    list: VecDeque<(UnminedTxId, Time, u64)>,
    ids: HashMap<UnminedTxId, usize>,
    /// How many are due at each time.
    dues: BTreeMap<Time, usize>,
}

impl Held {
    /// Holds `id`, whose short id is `short`, until `due`. Returns whether
    /// no other held advert was due then.
    pub(crate) fn push(&mut self, id: UnminedTxId, due: Time, short: u64) -> bool {
        *self.ids.entry(id).or_default() += 1;
        let at_due = self.dues.entry(due).or_default();
        *at_due += 1;
        let first_at_due = *at_due == 1;
        if self.list.back().is_none_or(|(_, last, _)| *last <= due) {
            self.list.push_back((id, due, short));
        } else {
            // After every advert due no later, so equal times keep their
            // order.
            let at = self.list.partition_point(|(_, held, _)| *held <= due);
            self.list.insert(at, (id, due, short));
        }
        first_at_due
    }

    /// Whether `id` is held.
    pub(crate) fn contains(&self, id: UnminedTxId) -> bool {
        self.ids.contains_key(&id)
    }

    /// Stops holding `id`.
    pub(crate) fn remove(&mut self, id: UnminedTxId) {
        if self.ids.remove(&id).is_some() {
            let dues = &mut self.dues;
            self.list.retain(|(held, due, _)| {
                let kept = *held != id;
                if !kept {
                    decrement_ordered(dues, *due);
                }
                kept
            });
        }
    }

    /// Keeps the adverts `keep` accepts.
    pub(crate) fn retain(&mut self, mut keep: impl FnMut(&UnminedTxId) -> bool) {
        let (ids, dues) = (&mut self.ids, &mut self.dues);
        self.list.retain(|(id, due, _)| {
            let kept = keep(id);
            if !kept {
                decrement(ids, *id);
                decrement_ordered(dues, *due);
            }
            kept
        });
    }

    /// Takes the adverts due at `now`, earliest due first, with their short
    /// ids.
    pub(crate) fn take_due(&mut self, now: Time) -> Vec<(UnminedTxId, u64)> {
        let count = self.list.partition_point(|(_, at, _)| *at <= now);
        let due: Vec<(UnminedTxId, Time, u64)> = self.list.drain(..count).collect();
        for (id, at, _) in &due {
            decrement(&mut self.ids, *id);
            decrement_ordered(&mut self.dues, *at);
        }
        due.into_iter().map(|(id, _, short)| (id, short)).collect()
    }

    /// Whether a held advert is due at `at`.
    pub(crate) fn has_due(&self, at: Time) -> bool {
        self.dues.contains_key(&at)
    }

    /// When the first held advert is due, if one is held.
    pub(crate) fn first_due(&self) -> Option<Time> {
        self.list.front().map(|(_, at, _)| *at)
    }

    /// The held adverts and when each is due, earliest due first.
    #[cfg(test)]
    pub(crate) fn iter(&self) -> impl Iterator<Item = (UnminedTxId, Time)> + '_ {
        self.list.iter().map(|(id, due, _)| (*id, *due))
    }
}

/// The bodies a session supplied that wait for a verdict, by waiter,
/// indexed by id and by entry.
#[derive(Clone, Debug, Default)]
pub(crate) struct Submissions {
    by_waiter: BTreeMap<WaiterId, (UnminedTxId, EntryGen)>,
    by_id: HashMap<UnminedTxId, usize>,
    by_entry: HashMap<(UnminedTxId, EntryGen), usize>,
}

impl Submissions {
    /// Records `waiter`'s submission of `id` in `generation`.
    pub(crate) fn insert(&mut self, waiter: WaiterId, (id, generation): (UnminedTxId, EntryGen)) {
        if let Some(old) = self.by_waiter.insert(waiter, (id, generation)) {
            self.unindex(old);
        }
        *self.by_id.entry(id).or_default() += 1;
        *self.by_entry.entry((id, generation)).or_default() += 1;
    }

    /// Takes `waiter`'s submission out.
    pub(crate) fn remove(&mut self, waiter: &WaiterId) -> Option<(UnminedTxId, EntryGen)> {
        let submitted = self.by_waiter.remove(waiter)?;
        self.unindex(submitted);
        Some(submitted)
    }

    /// Whether a body for `id` waits for a verdict, in any generation.
    pub(crate) fn contains(&self, id: UnminedTxId) -> bool {
        self.by_id.contains_key(&id)
    }

    /// Whether a body for `id` in `generation` waits for a verdict.
    pub(crate) fn holds(&self, id: UnminedTxId, generation: EntryGen) -> bool {
        self.by_entry.contains_key(&(id, generation))
    }

    /// How many bodies wait.
    pub(crate) fn len(&self) -> usize {
        self.by_waiter.len()
    }

    /// Whether `waiter` names one of these submissions.
    #[cfg(test)]
    pub(crate) fn contains_waiter(&self, waiter: WaiterId) -> bool {
        self.by_waiter.contains_key(&waiter)
    }

    /// The waiters, in order.
    #[cfg(test)]
    pub(crate) fn keys(&self) -> impl Iterator<Item = &WaiterId> {
        self.by_waiter.keys()
    }

    /// Every waiter and its submission, in order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&WaiterId, &(UnminedTxId, EntryGen))> {
        self.by_waiter.iter()
    }

    /// Takes a submission out of the indexes.
    fn unindex(&mut self, (id, generation): (UnminedTxId, EntryGen)) {
        decrement(&mut self.by_id, id);
        decrement(&mut self.by_entry, (id, generation));
    }
}
