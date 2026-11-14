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

use super::{EntryGen, PeerKey, SessionId, Time, BASE_COOLDOWN, MAX_COOLDOWN, MAX_COOLDOWN_PEERS};

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
    /// every fourth place. The test sets it, because the model has no clock
    /// of round trips or source of randomness.
    ranking: Vec<SessionId>,
}

impl ReactorModel {
    /// A new session of `peer`. Returns whether it has the go at once,
    /// which it does when it is the peer's only session.
    pub(crate) fn connect(&mut self, peer: PeerKey, session: SessionId) -> bool {
        self.sessions.entry(peer).or_default().insert(session);
        self.grant(peer) == Some(session)
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
        self.ranking.retain(|ranked| *ranked != session);
        if self.go.get(&peer) == Some(&session) {
            self.go.remove(&peer);
            return self.grant(peer);
        }
        None
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

    /// Sets the ranking. Sessions it does not name are outside every window.
    pub(crate) fn set_ranking(&mut self, ranking: Vec<SessionId>) {
        self.ranking = ranking;
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
