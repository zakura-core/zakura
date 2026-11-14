//! The model of `tx-relay`'s reactor: node-wide state only.
//!
//! It keeps the v2 cooldowns, keyed by peer, which session of each peer may
//! fetch, the submissions of sessions that ended before their verdict, and
//! the fan-out ranking. It never picks a peer to ask for an id.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    time::Duration,
};

use zakura_chain::transaction::UnminedTxId;
use zakura_node_services::mempool::port::model::WaiterId;

use super::{
    EntryGen, PeerKey, SessionId, Time, BASE_COOLDOWN, MAX_COOLDOWN, MAX_COOLDOWN_PEERS,
    RANDOM_EVERY, RANK_INTERVAL,
};

/// One peer's cooldown history, as today's `PeerCooldowns` keeps it.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct Cooldown {
    until: Time,
    strikes: u32,
}

/// The reactor's state.
#[derive(Clone, Debug, Default)]
pub(crate) struct ReactorModel {
    cooldowns: HashMap<PeerKey, Cooldown>,
    /// Every session, by peer, oldest first.
    sessions: BTreeMap<PeerKey, BTreeSet<SessionId>>,
    /// The one session of each peer that may announce and fetch.
    go: HashMap<PeerKey, SessionId>,
    /// Submissions whose session ended before the verdict, by peer, with
    /// the entry generation they were fetched in. No peer has an empty map.
    unsettled: HashMap<PeerKey, BTreeMap<WaiterId, (UnminedTxId, EntryGen)>>,
    /// Live sessions, lowest round-trip time first, with a random one in
    /// every fourth place. Ranked again when a session starts or ends, and on
    /// the refresh timer, so it is sticky in between.
    ranking: Vec<SessionId>,
    /// Each session's smoothed round-trip time, as its routine last
    /// reported it.
    rtts: HashMap<SessionId, Duration>,
    /// The deterministic source of the random places.
    random: u64,
    /// When the ranking is next refreshed.
    next_refresh: Option<Time>,
}

impl ReactorModel {
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

    /// Whether `peer` has a session that may carry its transactions. The
    /// legacy adapter reads this.
    pub(crate) fn is_live(&self, peer: PeerKey) -> bool {
        self.go.contains_key(&peer)
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
            .map(|waiting| waiting.values().map(|(id, _)| *id).collect())
            .unwrap_or_default()
    }

    /// How many of `peer`'s 5 its ended sessions still hold.
    pub(crate) fn unsettled_count(&self, peer: PeerKey) -> usize {
        self.unsettled.get(&peer).map_or(0, BTreeMap::len)
    }

    /// Whether `waiter` is an unsettled submission here.
    pub(crate) fn holds_unsettled(&self, waiter: WaiterId) -> bool {
        self.unsettled
            .values()
            .any(|waiting| waiting.contains_key(&waiter))
    }

    /// The id and entry an unsettled submission is for.
    pub(crate) fn unsettled_entry(&self, waiter: WaiterId) -> Option<(UnminedTxId, EntryGen)> {
        self.unsettled
            .values()
            .find_map(|waiting| waiting.get(&waiter).copied())
    }

    /// How many unsettled submissions there are, over every peer.
    pub(crate) fn unsettled_total(&self) -> usize {
        self.unsettled.values().map(BTreeMap::len).sum()
    }

    /// How many peers have unsettled submissions.
    pub(crate) fn unsettled_peers(&self) -> usize {
        self.unsettled.len()
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
    /// history is forgotten [`MAX_COOLDOWN`] after a cooldown ends, the
    /// length doubles from [`BASE_COOLDOWN`] up to [`MAX_COOLDOWN`], and a
    /// full history drops the cooldown that ends first. Returns the length of
    /// the cooldown it started.
    pub(crate) fn record_invalid(&mut self, peer: PeerKey, now: Time) -> Option<Duration> {
        if !self.cooldowns.contains_key(&peer) && self.cooldowns.len() >= MAX_COOLDOWN_PEERS {
            let earliest = self
                .cooldowns
                .iter()
                .min_by_key(|(key, cooldown)| (cooldown.until, **key))
                .map(|(key, _)| *key)?;
            self.cooldowns.remove(&earliest);
        }
        let mut cooldown = self.cooldowns.get(&peer).copied().unwrap_or(Cooldown {
            until: now,
            strikes: 0,
        });
        if now < cooldown.until {
            return None;
        }
        if now.saturating_sub(cooldown.until) >= MAX_COOLDOWN {
            cooldown.strikes = 0;
        }
        cooldown.strikes = cooldown.strikes.saturating_add(1);
        let doublings = cooldown.strikes.saturating_sub(1).min(16);
        let length = BASE_COOLDOWN
            .saturating_mul(1 << doublings)
            .min(MAX_COOLDOWN);
        cooldown.until = now.saturating_add(length);
        self.cooldowns.insert(peer, cooldown);
        Some(length)
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
        self.ranking = ranking;
    }

    /// The ranking, best first.
    pub(crate) fn ranking(&self) -> &[SessionId] {
        &self.ranking
    }

    /// `session`'s place in the ranking, from 0.
    pub(crate) fn rank(&self, session: SessionId) -> Option<usize> {
        self.ranking.iter().position(|ranked| *ranked == session)
    }

    /// Checks the reactor's own rules.
    pub(crate) fn assert_invariants(&self) {
        for (peer, waiting) in &self.unsettled {
            assert!(!waiting.is_empty(), "no empty record for {peer:?}");
            assert!(waiting.len() <= super::MAX_REQUESTS_PER_PEER);
        }
        let mut ranked: Vec<SessionId> = self.ranking.clone();
        ranked.sort();
        let mut live: Vec<SessionId> = self.go.values().copied().collect();
        live.sort();
        assert_eq!(ranked, live, "the ranking holds exactly the live sessions");
        for session in self.rtts.keys() {
            assert!(
                self.sessions
                    .values()
                    .any(|sessions| sessions.contains(session)),
                "a round-trip time belongs to a registered session"
            );
        }
        for (peer, session) in &self.go {
            assert!(
                self.sessions
                    .get(peer)
                    .and_then(|sessions| sessions.iter().next())
                    == Some(session),
                "the go is the peer's oldest session"
            );
        }
    }
}
