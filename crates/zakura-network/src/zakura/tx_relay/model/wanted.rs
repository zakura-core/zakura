//! The model of `WantedIds`: the node-wide table of ids this node wants.
//!
//! An entry is shared state about one id. It holds no peer facts: not who
//! announced the id, not who tried it, not any peer's announced size. Those
//! live in each peer routine. The entry has only its claim, how many pending
//! sets hold it, and its timing. The table also holds the pool's `NoRoom`
//! backoff and its one probe permit, which are shared too.

use std::{collections::HashMap, time::Duration};

use zakura_chain::transaction::UnminedTxId;

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
    /// When the first announcement of this generation arrived. An inbound
    /// announcer waits [`super::INBOUND_DELAY`] after it.
    pub(crate) first_seen: Time,
    /// Nobody claims it before this.
    pub(crate) retry_at: Time,
    /// The last retry delay, which doubles on each retry.
    pub(crate) retry_delay: Option<Duration>,
    /// When its claim was last released. A session whose own request for it
    /// timed out waits [`super::INBOUND_DELAY`] after this.
    pub(crate) released_at: Option<Time>,
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
    /// The pool's position for the `NoRoom` that set it. An answer the pool
    /// decided before that cannot clear it.
    set_by: u64,
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
pub(crate) struct WantedModel {
    entries: HashMap<UnminedTxId, Entry>,
    next_generation: u64,
    backoff: Option<Backoff>,
    next_backoff: u64,
    /// The pool's position for the newest answer that showed room. A
    /// `NoRoom` the pool decided before it is stale.
    room_seen: u64,
}

impl WantedModel {
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

    /// Every wanted id and its entry.
    pub(crate) fn entries(&self) -> impl Iterator<Item = (&UnminedTxId, &Entry)> {
        self.entries.iter()
    }

    /// How many ids the node wants.
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    /// A pending set takes `id`. Makes a new entry and generation if there is
    /// none, and counts one more holder.
    pub(crate) fn hold(&mut self, id: UnminedTxId, now: Time) -> EntryGen {
        let next = &mut self.next_generation;
        let entry = self.entries.entry(id).or_insert_with(|| {
            *next = next.saturating_add(1);
            Entry {
                generation: EntryGen(*next),
                claim: None,
                holders: 0,
                first_seen: now,
                retry_at: now,
                retry_delay: None,
                released_at: None,
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
    }

    /// `session`'s claim on `id` ends without a decision. With `retry`,
    /// nobody claims it for a delay that doubles each time. A claim of
    /// another generation or session is left alone.
    pub(crate) fn release(
        &mut self,
        id: UnminedTxId,
        generation: EntryGen,
        session: SessionId,
        now: Time,
        retry: bool,
    ) {
        let Some(entry) = self
            .entries
            .get_mut(&id)
            .filter(|entry| entry.generation == generation && entry.claim == Some(session))
        else {
            return;
        };
        entry.claim = None;
        entry.released_at = Some(now);
        if retry {
            let delay = entry
                .retry_delay
                .map_or(RETRY_FIRST, |delay| delay.saturating_mul(2).min(RETRY_MAX));
            entry.retry_delay = Some(delay);
            entry.retry_at = now.saturating_add(delay);
        }
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

    /// Who holds the probe permit, and for which backoff.
    pub(crate) fn probe(&self) -> Option<(SessionId, u64)> {
        self.backoff
            .and_then(|backoff| backoff.probe.map(|probe| (probe, backoff.generation)))
    }

    /// When the backoff ends, if it has not.
    pub(crate) fn backoff_until(&self, now: Time) -> Option<Time> {
        self.backoff
            .map(|backoff| backoff.until)
            .filter(|until| *until > now)
    }

    /// Whether a backoff is set, ended or not.
    pub(crate) fn in_backoff(&self) -> bool {
        self.backoff.is_some()
    }

    /// The pool answered `NoRoom`, at position `seq` in its own order.
    /// `probe` names the backoff whose probe got it: that backoff doubles. A
    /// reserve that was already in flight when the backoff began changes
    /// nothing, one with no backoff set starts one, and one the pool decided
    /// before it last showed room is stale.
    pub(crate) fn no_room(&mut self, now: Time, probe: Option<u64>, seq: u64) {
        if seq < self.room_seen {
            return;
        }
        match (self.backoff, probe) {
            (Some(backoff), Some(generation)) if backoff.generation == generation => {
                let delay = backoff.delay.saturating_mul(2).min(NO_ROOM_MAX);
                self.set_backoff(now, delay, seq);
            }
            (Some(_), _) => {}
            (None, _) => self.set_backoff(now, NO_ROOM_FIRST, seq),
        }
    }

    /// The pool showed room at position `seq`: it granted a reservation, or
    /// a verdict freed a slot. A backoff set by a later `NoRoom` stays.
    pub(crate) fn room(&mut self, seq: u64) {
        self.room_seen = self.room_seen.max(seq);
        if self.backoff.is_some_and(|backoff| backoff.set_by < seq) {
            self.backoff = None;
        }
    }

    fn set_backoff(&mut self, now: Time, delay: Duration, seq: u64) {
        self.next_backoff = self.next_backoff.saturating_add(1);
        self.backoff = Some(Backoff {
            until: now.saturating_add(delay),
            delay,
            generation: self.next_backoff,
            probe: None,
            set_by: seq,
        });
    }

    /// Drops `id`'s entry once nobody holds or claims it.
    fn collect(&mut self, id: UnminedTxId) {
        if self
            .entries
            .get(&id)
            .is_some_and(|entry| entry.holders == 0 && entry.claim.is_none())
        {
            self.entries.remove(&id);
        }
    }
}
