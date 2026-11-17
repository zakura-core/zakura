//! `tx-relay`'s reactor: node-wide state only.
//!
//! It keeps the v2 cooldowns and the parks of peers that stalled, keyed by
//! peer, which session of each peer may fetch, the submissions and reserve
//! calls of sessions that ended, and the fan-out ranking. It never picks a
//! peer to ask for an id.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use zakura_chain::transaction::UnminedTxId;
use zakura_rt::collections::HashMap;

use super::{
    EntryGen, PeerKey, SessionId, Time, WaiterId, BASE_COOLDOWN, MAX_COOLDOWN, MAX_COOLDOWN_PEERS,
    RANDOM_EVERY, RANK_INTERVAL,
};

/// One peer's cooldown history, as today's `PeerCooldowns` keeps it.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct Cooldown {
    until: Time,
    strikes: u32,
}

impl Cooldown {
    /// Whether the history is forgotten at `now`: [`MAX_COOLDOWN`] or more
    /// after its cooldown ended.
    fn is_forgotten(&self, now: Time) -> bool {
        now.saturating_sub(self.until) >= MAX_COOLDOWN
    }
}

/// What [`Reactor::record_invalid`] did with a body's cooldown.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum Recorded {
    /// The peer already cools, so the body adds no strike.
    AlreadyCooling,
    /// A cooldown of this length started.
    Started(Duration),
    /// The table is full of other peers' histories, so nothing was
    /// recorded.
    TableFull,
}

/// How a new session of a peer is let in, after any park of the peer.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum SessionAdmission {
    /// The peer's park runs: the session ends before it begins.
    Parked,
    /// The peer's park ended, and this is the session it lets in. Its own
    /// stall closes the connection.
    Readmitted,
    /// No park of the peer.
    Fresh,
}

/// The reactor's state.
#[derive(Clone, Debug, Default)]
pub(crate) struct Reactor {
    cooldowns: HashMap<PeerKey, Cooldown>,
    /// Peers whose session stalled, with when the park ends. A park ends
    /// in the next session of the peer it lets in, or is dropped once over
    /// when the table needs room.
    parks: HashMap<PeerKey, Time>,
    /// Every session, by peer, oldest first.
    sessions: BTreeMap<PeerKey, BTreeSet<SessionId>>,
    /// The one session of each peer that may announce and fetch.
    go: HashMap<PeerKey, SessionId>,
    /// Submissions whose session ended before the verdict, by peer, with
    /// the entry generation they were fetched in. No peer has an empty map.
    unsettled: HashMap<PeerKey, BTreeMap<WaiterId, (UnminedTxId, EntryGen)>>,
    /// Reserve calls whose session ended before the pool answered, by peer.
    /// They count against the peer's 5 until answered, so a peer that ends
    /// and reopens sessions while the pool is slow can't pile up calls. No
    /// peer has a count of zero.
    ended_calls: HashMap<PeerKey, usize>,
    /// Each live session's place in the ranking: lowest round-trip time
    /// first, with a random one in every fourth place. Ranked again when a
    /// session starts or ends, and on the refresh timer, so it is sticky in
    /// between.
    places: HashMap<SessionId, usize>,
    /// Each session's smoothed round-trip time, as its routine last
    /// reported it.
    rtts: HashMap<SessionId, Duration>,
    /// The deterministic source of the random places.
    random: u64,
    /// When the ranking is next refreshed.
    next_refresh: Option<Time>,
}

impl Reactor {
    /// How a new session of `peer` is let in at `now`. An ended park lets
    /// in this session and is gone.
    pub(crate) fn admit(&mut self, peer: PeerKey, now: Time) -> SessionAdmission {
        match self.parks.get(&peer) {
            Some(until) if now < *until => SessionAdmission::Parked,
            Some(_) => {
                self.parks.remove(&peer);
                SessionAdmission::Readmitted
            }
            None => SessionAdmission::Fresh,
        }
    }

    /// Parks `peer` until `until`: its sessions are refused until then.
    /// Returns false, parking nothing, when the table holds
    /// [`MAX_COOLDOWN_PEERS`] parks still running.
    pub(crate) fn park(&mut self, peer: PeerKey, until: Time, now: Time) -> bool {
        if !self.parks.contains_key(&peer) && self.parks.len() >= MAX_COOLDOWN_PEERS {
            self.parks.retain(|_, park| now < *park);
            if self.parks.len() >= MAX_COOLDOWN_PEERS {
                return false;
            }
        }
        self.parks.insert(peer, until);
        true
    }

    /// A new session of `peer`. Returns whether it has the go at once,
    /// which it does when it is the peer's only session.
    pub(crate) fn connect(&mut self, peer: PeerKey, session: SessionId) -> bool {
        self.sessions.entry(peer).or_default().insert(session);
        let granted = self.grant(peer) == Some(session);
        self.rerank();
        granted
    }

    /// `session` of `peer` ended, and its unsettled submissions move here in
    /// the same step. Returns the session that now has the go, if one just
    /// got it.
    pub(crate) fn end(
        &mut self,
        peer: PeerKey,
        session: SessionId,
        submissions: impl IntoIterator<Item = (WaiterId, (UnminedTxId, EntryGen))>,
    ) -> Option<SessionId> {
        let mut submissions = submissions.into_iter().peekable();
        if submissions.peek().is_some() {
            self.unsettled.entry(peer).or_default().extend(submissions);
        }
        if let Some(sessions) = self.sessions.get_mut(&peer) {
            sessions.remove(&session);
            if sessions.is_empty() {
                self.sessions.remove(&peer);
            }
        }
        self.rtts.remove(&session);
        let next = if self.go.get(&peer) == Some(&session) {
            self.go.remove(&peer);
            self.grant(peer)
        } else {
            None
        };
        self.rerank();
        next
    }

    /// Gives the go to `peer`'s oldest session if none has it. Every older
    /// session already handed off in [`Self::end`], because a session only
    /// reaches the front once every older one ended.
    fn grant(&mut self, peer: PeerKey) -> Option<SessionId> {
        if self.go.contains_key(&peer) {
            return None;
        }
        let oldest = *self.sessions.get(&peer)?.iter().next()?;
        self.go.insert(peer, oldest);
        Some(oldest)
    }

    /// Whether `session` may announce and fetch.
    pub(crate) fn has_go(&self, peer: PeerKey, session: SessionId) -> bool {
        self.go.get(&peer) == Some(&session)
    }

    /// Whether `peer` has a session.
    pub(crate) fn has_sessions(&self, peer: PeerKey) -> bool {
        self.sessions.contains_key(&peer)
    }

    /// Every session of `peer`.
    pub(crate) fn sessions_of(&self, peer: PeerKey) -> Vec<SessionId> {
        self.sessions
            .get(&peer)
            .map(|sessions| sessions.iter().copied().collect())
            .unwrap_or_default()
    }

    /// The ids of `peer`'s submissions from ended sessions still waiting
    /// for a verdict.
    pub(crate) fn unsettled_ids(&self, peer: PeerKey) -> Vec<UnminedTxId> {
        self.unsettled
            .get(&peer)
            .into_iter()
            .flat_map(|waiting| waiting.values().map(|(id, _)| *id))
            .collect()
    }

    /// How many of `peer`'s 5 its ended sessions still hold: submissions
    /// waiting for a verdict and reserve calls waiting for an answer.
    pub(crate) fn unsettled_count(&self, peer: PeerKey) -> usize {
        self.unsettled
            .get(&peer)
            .map_or(0, BTreeMap::len)
            .saturating_add(self.ended_calls.get(&peer).copied().unwrap_or(0))
    }

    /// `peer`'s ended session left `calls` reserve calls unanswered.
    pub(crate) fn hand_calls(&mut self, peer: PeerKey, calls: usize) {
        if calls > 0 {
            let count = self.ended_calls.entry(peer).or_default();
            *count = count.saturating_add(calls);
        }
    }

    /// One of `peer`'s handed reserve calls was answered.
    pub(crate) fn call_answered(&mut self, peer: PeerKey) {
        if let Some(count) = self.ended_calls.get_mut(&peer) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.ended_calls.remove(&peer);
            }
        }
    }

    /// Whether `peer` supplied `id` in a body the reactor still holds,
    /// waiting for its verdict.
    pub(crate) fn holds_for(&self, peer: PeerKey, id: UnminedTxId) -> bool {
        self.unsettled
            .get(&peer)
            .is_some_and(|waiting| waiting.values().any(|(held, _)| *held == id))
    }

    /// Takes `waiter` out of the unsettled submissions. Returns its peer, id
    /// and entry generation if the reactor held it.
    pub(crate) fn settle(&mut self, waiter: WaiterId) -> Option<(PeerKey, UnminedTxId, EntryGen)> {
        let peer = *self
            .unsettled
            .iter()
            .find(|(_, waiting)| waiting.contains_key(&waiter))?
            .0;
        let waiting = self.unsettled.get_mut(&peer)?;
        let (id, generation) = waiting.remove(&waiter)?;
        if waiting.is_empty() {
            self.unsettled.remove(&peer);
        }
        Some((peer, id, generation))
    }

    /// Whether `peer` is cooling down at `now`.
    pub(crate) fn is_cooling(&self, peer: PeerKey, now: Time) -> bool {
        self.cooldowns
            .get(&peer)
            .is_some_and(|cooldown| now < cooldown.until)
    }

    /// When `peer`'s cooldown ends, if it is cooling.
    pub(crate) fn cooling_until(&self, peer: PeerKey, now: Time) -> Option<Time> {
        self.cooldowns
            .get(&peer)
            .map(|cooldown| cooldown.until)
            .filter(|until| now < *until)
    }

    /// A body from `peer` earned a cooldown. Today's
    /// `record_invalid_transaction`: no strike while it already cools, the
    /// history is forgotten [`MAX_COOLDOWN`] after a cooldown ends, and the
    /// length doubles from [`BASE_COOLDOWN`] up to [`MAX_COOLDOWN`]. Unlike
    /// today's, a full table drops no cooldown: see [`Self::has_room`].
    pub(crate) fn record_invalid(&mut self, peer: PeerKey, now: Time) -> Recorded {
        if !self.has_room(peer, now) {
            return Recorded::TableFull;
        }
        let mut cooldown = self.cooldowns.get(&peer).copied().unwrap_or(Cooldown {
            until: now,
            strikes: 0,
        });
        if now < cooldown.until {
            return Recorded::AlreadyCooling;
        }
        if cooldown.is_forgotten(now) {
            cooldown.strikes = 0;
        }
        cooldown.strikes = cooldown.strikes.saturating_add(1);
        let doublings = cooldown.strikes.saturating_sub(1).min(16);
        let length = BASE_COOLDOWN
            .saturating_mul(1 << doublings)
            .min(MAX_COOLDOWN);
        cooldown.until = now.saturating_add(length);
        self.cooldowns.insert(peer, cooldown);
        Recorded::Started(length)
    }

    /// A body from `peer` earned a ban: a cooldown, counted as
    /// [`Self::record_invalid`] counts one, that lasts [`MAX_COOLDOWN`] from
    /// `now`. A full table records nothing.
    pub(crate) fn ban(&mut self, peer: PeerKey, now: Time) {
        if self.record_invalid(peer, now) == Recorded::TableFull {
            return;
        }
        self.cooldowns
            .get_mut(&peer)
            .expect("record_invalid keeps an entry for every peer it does not refuse")
            .until = now.saturating_add(MAX_COOLDOWN);
    }

    /// Whether `peer` has, or can be given, a place in the cooldown table at
    /// `now`. A full table makes room only by dropping histories already
    /// forgotten, which hold no cooldown and no strike that counts, so fresh
    /// identities can't shed another peer's cooldown by filling the table.
    fn has_room(&mut self, peer: PeerKey, now: Time) -> bool {
        if self.cooldowns.contains_key(&peer) || self.cooldowns.len() < MAX_COOLDOWN_PEERS {
            return true;
        }
        self.cooldowns
            .retain(|_, cooldown| !cooldown.is_forgotten(now));
        self.cooldowns.len() < MAX_COOLDOWN_PEERS
    }

    /// `session`'s routine reports its connection's smoothed round-trip
    /// time. It counts at the next ranking.
    pub(crate) fn observe_rtt(&mut self, peer: PeerKey, session: SessionId, rtt: Duration) {
        if self
            .sessions
            .get(&peer)
            .is_some_and(|sessions| sessions.contains(&session))
        {
            self.rtts.insert(session, rtt);
        }
    }

    /// Whether the refresh timer is due at `now`. The first refresh is
    /// scheduled by the first call.
    pub(crate) fn refresh_due(&mut self, now: Time) -> bool {
        match self.next_refresh {
            None => {
                self.next_refresh = Some(now.saturating_add(RANK_INTERVAL));
                false
            }
            Some(at) => now >= at,
        }
    }

    /// The refresh timer fired: rank again and schedule the next one.
    pub(crate) fn refresh(&mut self, now: Time) {
        self.rerank();
        self.next_refresh = Some(now.saturating_add(RANK_INTERVAL));
    }

    /// Ranks the live sessions: lowest round-trip time first, unmeasured
    /// ones last, and in every fourth place a session picked at random from
    /// those not yet placed.
    fn rerank(&mut self) {
        let mut remaining: Vec<SessionId> = self.go.values().copied().collect();
        remaining.sort_by_key(|session| {
            let rtt = self.rtts.get(session).copied();
            (rtt.is_none(), rtt, *session)
        });
        let mut ranking = Vec::with_capacity(remaining.len());
        while !remaining.is_empty() {
            let place = ranking.len().saturating_add(1);
            let index = if place % RANDOM_EVERY == 0 && remaining.len() > 1 {
                self.random = self
                    .random
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                let len = u64::try_from(remaining.len()).unwrap_or(u64::MAX);
                usize::try_from((self.random >> 33) % len).unwrap_or(0)
            } else {
                0
            };
            ranking.push(remaining.remove(index));
        }
        self.places = ranking
            .iter()
            .enumerate()
            .map(|(place, session)| (*session, place))
            .collect();
    }

    /// `session`'s place in the ranking, from 0.
    pub(crate) fn rank(&self, session: SessionId) -> Option<usize> {
        self.places.get(&session).copied()
    }
}

/// Read-only views for the machine's invariants.
#[cfg(test)]
impl Reactor {
    /// Every session, by peer, oldest first.
    pub(crate) fn by_peer(&self) -> &BTreeMap<PeerKey, BTreeSet<SessionId>> {
        &self.sessions
    }

    /// The session of each peer that has the go.
    pub(crate) fn go(&self) -> &HashMap<PeerKey, SessionId> {
        &self.go
    }

    /// The sessions the ranking places.
    pub(crate) fn ranked(&self) -> impl Iterator<Item = SessionId> + '_ {
        self.places.keys().copied()
    }

    /// The submissions of ended sessions, by peer.
    pub(crate) fn unsettled(
        &self,
    ) -> &HashMap<PeerKey, BTreeMap<WaiterId, (UnminedTxId, EntryGen)>> {
        &self.unsettled
    }

    /// The reserve calls of ended sessions, by peer.
    pub(crate) fn ended_calls(&self) -> &HashMap<PeerKey, usize> {
        &self.ended_calls
    }

    /// How many peers the cooldown table holds.
    pub(crate) fn cooldowns_len(&self) -> usize {
        self.cooldowns.len()
    }
}
