//! `WantedIds`: the node-wide table of ids this node wants.
//!
//! An entry is shared state about one id. It holds no peer facts: not who
//! announced the id, not who tried it, not any peer's announced size. Those
//! live in each peer routine. The entry has only its claim, how many pending
//! sets hold it, and its timing. The table also holds the pool's `NoRoom`
//! backoff and its one probe permit, which are shared too.

use std::{collections::BTreeMap, time::Duration};

use zakura_chain::transaction::UnminedTxId;
use zakura_rt::collections::HashMap;

use super::{EntryGen, SessionId, Time, NO_ROOM_FIRST, NO_ROOM_MAX, RETRY_FIRST, RETRY_MAX};

/// One wanted id.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Entry {
    /// Which life of the id this is. An id that leaves and comes back gets a
    /// new generation, so a stale reference never changes a newer entry.
    pub(crate) generation: EntryGen,
    /// The session fetching it, from its reserve call to its verdict.
    pub(crate) claim: Option<SessionId>,
    /// How many pending sets hold it. A number, not a list.
    pub(crate) holders: usize,
    /// When the first announcement of this generation arrived.
    pub(crate) first_seen: Time,
    /// Nobody claims it before this.
    pub(crate) retry_at: Time,
    /// The last retry delay, which doubles on each retry.
    pub(crate) retry_delay: Option<Duration>,
    /// When its last claim ended without a decision, other than by a
    /// `NoRoom` answer. It is ready to claim again only from then, so an id
    /// whose attempts keep ending undecided, however they end, queues
    /// behind work that was already waiting.
    pub(crate) requeued_at: Option<Time>,
    /// Its place among entries ready at the same time: set when the entry
    /// is made and moved to the back on every undecided end of a claim
    /// other than `NoRoom`, as `requeued_at` is.
    pub(crate) order: u64,
    /// Its retry delay waits for another caller, such as the legacy
    /// adapter, that the pool said is fetching it. If that caller's attempt
    /// ends, the pool logs it as refused, and the delay ends at once.
    pub(crate) waits_for_other: bool,
}

/// The pool answered `NoRoom`, so nobody claims until `until`. After that
/// one session at a time holds the probe permit and may reserve.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct Backoff {
    until: Time,
    delay: Duration,
    /// Names this backoff, so a stale probe answer changes nothing.
    generation: u64,
    /// The session whose reserve call is the probe.
    probe: Option<SessionId>,
}

/// What the probe permit allows a session now.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum Admission {
    /// No backoff: reserve freely.
    Open,
    /// The backoff ended and nobody probes: this session may take the
    /// permit, for the backoff with this generation.
    Probe(u64),
    /// Claim nothing now.
    Closed,
}

/// The table.
#[derive(Clone, Debug, Default)]
pub(crate) struct WantedIds {
    entries: HashMap<UnminedTxId, Entry>,
    next_generation: u64,
    /// The next place in line to hand out.
    next_order: u64,
    backoff: Option<Backoff>,
    next_backoff: u64,
    /// Ids whose entry may have changed how a holder files it: claimed or
    /// released, re-timed, decided or dropped. The machine takes them with
    /// [`Self::take_changed`].
    changed: Vec<UnminedTxId>,
    /// Every retry time set, in order, for [`Self::next_wake`]. A later
    /// change to the entry leaves the old time here, and readers check it.
    retries: BTreeMap<(Time, u64), UnminedTxId>,
    next_retry: u64,
    /// Entries that ended, decided or dropped, for [`Self::take_ended`].
    ended: Vec<(UnminedTxId, EntryGen)>,
}

impl WantedIds {
    /// The entry for `id`, if the node wants it.
    pub(crate) fn entry(&self, id: UnminedTxId) -> Option<&Entry> {
        self.entries.get(&id)
    }

    /// The entry for `id` if it is still `generation`.
    pub(crate) fn current(&self, id: UnminedTxId, generation: EntryGen) -> Option<&Entry> {
        self.entries
            .get(&id)
            .filter(|entry| entry.generation == generation)
    }

    /// How many ids the node wants.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    /// A pending set takes `id`. Makes a new entry and generation if there is
    /// none, and counts one more holder.
    pub(crate) fn hold(&mut self, id: UnminedTxId, now: Time) -> EntryGen {
        let next = &mut self.next_generation;
        let order = &mut self.next_order;
        let entry = self.entries.entry(id).or_insert_with(|| {
            *next = next.saturating_add(1);
            Entry {
                generation: EntryGen(*next),
                claim: None,
                holders: 0,
                first_seen: now,
                retry_at: now,
                retry_delay: None,
                requeued_at: None,
                waits_for_other: false,
                order: {
                    *order = order.saturating_add(1);
                    *order
                },
            }
        });
        entry.holders = entry.holders.saturating_add(1);
        entry.generation
    }

    /// A pending set gives `id` up: it tried it, or its session ended. An
    /// entry nobody holds or claims leaves the table.
    pub(crate) fn unhold(&mut self, id: UnminedTxId, generation: EntryGen) {
        let Some(entry) = self
            .entries
            .get_mut(&id)
            .filter(|entry| entry.generation == generation)
        else {
            return;
        };
        entry.holders = entry
            .holders
            .checked_sub(1)
            .expect("a pending reference counts as one holder");
        self.collect(id);
    }

    /// `session` claims `id`.
    ///
    /// # Panics
    ///
    /// If someone else holds the claim. Callers check first.
    pub(crate) fn claim(&mut self, id: UnminedTxId, generation: EntryGen, session: SessionId) {
        let entry = self
            .entries
            .get_mut(&id)
            .filter(|entry| entry.generation == generation)
            .expect("only a current entry is claimed");
        assert!(entry.claim.is_none(), "one claim per id");
        entry.claim = Some(session);
        entry.waits_for_other = false;
        self.changed.push(id);
    }

    /// `session`'s claim on `id` ends because another caller is fetching
    /// it: as [`Self::release`] with a retry delay, which waits for that
    /// caller. See [`Entry::waits_for_other`].
    pub(crate) fn release_pending(
        &mut self,
        id: UnminedTxId,
        generation: EntryGen,
        session: SessionId,
        now: Time,
    ) {
        let claimed = self
            .entries
            .get(&id)
            .is_some_and(|entry| entry.generation == generation && entry.claim == Some(session));
        self.release(id, generation, session, now, true);
        if claimed {
            if let Some(entry) = self.entries.get_mut(&id) {
                entry.waits_for_other = true;
            }
        }
    }

    /// The pool logged `id` as refused: whoever else was fetching it gave
    /// up, or its body was turned away. An entry whose retry waits for that
    /// caller may be claimed at once.
    pub(crate) fn other_ended(&mut self, id: UnminedTxId, now: Time) {
        if let Some(entry) = self.entries.get_mut(&id) {
            if entry.waits_for_other && entry.claim.is_none() {
                entry.retry_at = entry.retry_at.min(now);
                entry.waits_for_other = false;
                self.changed.push(id);
            }
        }
    }

    /// `session`'s claim on `id` ends without a decision, and the id goes to
    /// the back of the line. With `retry`, nobody claims it for a delay that
    /// doubles each time. A claim of another generation or session is left
    /// alone.
    pub(crate) fn release(
        &mut self,
        id: UnminedTxId,
        generation: EntryGen,
        session: SessionId,
        now: Time,
        retry: bool,
    ) {
        self.end_claim(id, generation, session, now, retry, true);
    }

    /// `session`'s claim on `id` ends because the pool had no room. The id
    /// keeps its place in line: the backoff, not the id, waits.
    pub(crate) fn release_no_room(
        &mut self,
        id: UnminedTxId,
        generation: EntryGen,
        session: SessionId,
        now: Time,
    ) {
        self.end_claim(id, generation, session, now, false, false);
    }

    /// Ends `session`'s claim on `id` in `generation`, requeueing the id
    /// and starting a retry delay as asked. See [`Self::release`].
    fn end_claim(
        &mut self,
        id: UnminedTxId,
        generation: EntryGen,
        session: SessionId,
        now: Time,
        retry: bool,
        requeue: bool,
    ) {
        let Some(entry) = self
            .entries
            .get_mut(&id)
            .filter(|entry| entry.generation == generation && entry.claim == Some(session))
        else {
            return;
        };
        entry.claim = None;
        if requeue {
            entry.requeued_at = Some(now);
            self.next_order = self.next_order.saturating_add(1);
            entry.order = self.next_order;
        }
        if retry {
            let delay = entry
                .retry_delay
                .map_or(RETRY_FIRST, |delay| delay.saturating_mul(2).min(RETRY_MAX));
            entry.retry_delay = Some(delay);
            entry.retry_at = now.saturating_add(delay);
            self.next_retry = self.next_retry.saturating_add(1);
            self.retries.insert((entry.retry_at, self.next_retry), id);
        }
        self.changed.push(id);
        self.collect(id);
    }

    /// `id` is decided in `generation`: in the pool, rejected, or mined. Its
    /// entry leaves, whoever holds it. Returns whether that generation was
    /// the current one.
    pub(crate) fn decide(&mut self, id: UnminedTxId, generation: EntryGen) -> bool {
        if self
            .entries
            .get(&id)
            .is_some_and(|entry| entry.generation == generation)
        {
            self.entries.remove(&id);
            self.changed.push(id);
            self.ended.push((id, generation));
            true
        } else {
            false
        }
    }

    /// What `session` may do about reserving now.
    pub(crate) fn admission(&self, session: SessionId, now: Time) -> Admission {
        match self.backoff {
            None => Admission::Open,
            Some(backoff) if now < backoff.until => Admission::Closed,
            Some(backoff) => match backoff.probe {
                None => Admission::Probe(backoff.generation),
                Some(holder) if holder == session => Admission::Closed,
                Some(_) => Admission::Closed,
            },
        }
    }

    /// `session` takes the probe permit of the backoff named `generation`.
    pub(crate) fn take_probe(&mut self, session: SessionId, generation: u64) {
        let backoff = self
            .backoff
            .as_mut()
            .filter(|backoff| backoff.generation == generation && backoff.probe.is_none())
            .expect("the permit is taken only when it is free");
        backoff.probe = Some(session);
    }

    /// The probe of the backoff named `generation` ended without an answer
    /// about room, or its session ended. The permit is free again.
    pub(crate) fn return_probe(&mut self, generation: u64) {
        if let Some(backoff) = self
            .backoff
            .as_mut()
            .filter(|backoff| backoff.generation == generation)
        {
            backoff.probe = None;
        }
    }

    /// When the backoff ends, if it has not.
    pub(crate) fn backoff_until(&self, now: Time) -> Option<Time> {
        self.backoff
            .map(|backoff| backoff.until)
            .filter(|until| *until > now)
    }

    /// The pool answered `NoRoom`. `probe` names the backoff whose probe got
    /// it: that backoff doubles. A reserve that was already in flight when
    /// the backoff began changes nothing, and one with no backoff set starts
    /// one.
    ///
    /// Answers are not ordered across callers, so a `NoRoom` the pool decided
    /// before a later grant can still start a backoff, and a grant decided
    /// before a later `NoRoom` can still clear one. Either costs at most one
    /// backoff's pause or one extra probe. The real routine has no way to
    /// order them better through the port.
    pub(crate) fn no_room(&mut self, now: Time, probe: Option<u64>) {
        match (self.backoff, probe) {
            (Some(backoff), Some(generation)) if backoff.generation == generation => {
                let delay = backoff.delay.saturating_mul(2).min(NO_ROOM_MAX);
                self.set_backoff(now, delay);
            }
            (Some(_), _) => {}
            (None, _) => self.set_backoff(now, NO_ROOM_FIRST),
        }
    }

    /// The pool showed room: it granted a reservation, or a verdict freed a
    /// slot.
    pub(crate) fn room(&mut self) {
        self.backoff = None;
    }

    /// Starts a backoff of `delay` from `now`, with a fresh name and a free
    /// probe permit.
    fn set_backoff(&mut self, now: Time, delay: Duration) {
        self.next_backoff = self.next_backoff.saturating_add(1);
        self.backoff = Some(Backoff {
            until: now.saturating_add(delay),
            delay,
            generation: self.next_backoff,
            probe: None,
        });
    }

    /// Drops `id`'s entry once nobody holds or claims it.
    fn collect(&mut self, id: UnminedTxId) {
        if self
            .entries
            .get(&id)
            .is_some_and(|entry| entry.holders == 0 && entry.claim.is_none())
        {
            if let Some(entry) = self.entries.remove(&id) {
                self.ended.push((id, entry.generation));
            }
            self.changed.push(id);
        }
    }

    /// Takes the entries that ended since the last call.
    pub(crate) fn take_ended(&mut self) -> Vec<(UnminedTxId, EntryGen)> {
        std::mem::take(&mut self.ended)
    }

    /// Takes the ids marked changed since the last call. An id may repeat.
    pub(crate) fn take_changed(&mut self) -> Vec<UnminedTxId> {
        std::mem::take(&mut self.changed)
    }

    /// The earliest time after `now` that an entry's retry delay ends, over
    /// every wanted entry.
    pub(crate) fn next_wake(&self, now: Time) -> Option<Time> {
        self.retries
            .iter()
            .find(|((at, _), id)| *at > now && self.retries_at(**id, *at))
            .map(|((at, _), _)| *at)
    }

    /// Drops retry times that passed or no longer stand from the front.
    pub(crate) fn drop_stale(&mut self, now: Time) {
        while let Some(((at, _), id)) = self.retries.first_key_value() {
            if *at > now && self.retries_at(*id, *at) {
                break;
            }
            self.retries.pop_first();
        }
    }

    /// Whether `id`'s entry still waits until `at` to retry.
    fn retries_at(&self, id: UnminedTxId, at: Time) -> bool {
        self.entries
            .get(&id)
            .is_some_and(|entry| entry.retry_at == at)
    }
}

/// Read-only views for the machine's invariants.
#[cfg(test)]
impl WantedIds {
    /// Every entry.
    pub(crate) fn entries(&self) -> impl Iterator<Item = (&UnminedTxId, &Entry)> {
        self.entries.iter()
    }

    /// The session holding the probe permit, with its backoff's name.
    pub(crate) fn probe(&self) -> Option<(SessionId, u64)> {
        let backoff = self.backoff?;
        Some((backoff.probe?, backoff.generation))
    }

    /// Whether [`Self::next_wake`] knows that `id` retries at `at`.
    pub(crate) fn retry_indexed(&self, id: UnminedTxId, at: Time) -> bool {
        self.retries
            .range((at, 0)..=(at, u64::MAX))
            .any(|(_, indexed)| *indexed == id)
    }

    /// How many retry times the wake-up index keeps, standing or not.
    pub(crate) fn retries_len(&self) -> usize {
        self.retries.len()
    }
}
