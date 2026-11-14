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

use super::{PeerKey, SessionId, Time, BASE_COOLDOWN, MAX_COOLDOWN, MAX_COOLDOWN_PEERS};

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
    /// Submissions whose session ended before the verdict, by peer.
    unsettled: HashMap<PeerKey, BTreeMap<WaiterId, UnminedTxId>>,
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

    /// `session` of `peer` ended, and its unsettled submissions move here.
    /// Returns the session that now has the go, if one just got it.
    pub(crate) fn end(
        &mut self,
        peer: PeerKey,
        session: SessionId,
        submissions: impl IntoIterator<Item = (WaiterId, UnminedTxId)>,
    ) -> Option<SessionId> {
        self.unsettled.entry(peer).or_default().extend(submissions);
        if let Some(sessions) = self.sessions.get_mut(&peer) {
            sessions.remove(&session);
            if sessions.is_empty() {
                self.sessions.remove(&peer);
            }
        }
        self.ranking.retain(|ranked| *ranked != session);
        let had_go = self.go.get(&peer) == Some(&session);
        if had_go {
            self.go.remove(&peer);
            return self.grant(peer);
        }
        None
    }

    /// Gives the go to `peer`'s oldest session if none has it. The handoff
    /// in [`Self::end`] already happened for every older session, because a
    /// session only reaches the front once every older one ended.
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

    /// The submissions of `peer`'s ended sessions still waiting for a
    /// verdict.
    pub(crate) fn unsettled(
        &self,
        peer: PeerKey,
    ) -> impl Iterator<Item = (WaiterId, UnminedTxId)> + '_ {
        self.unsettled
            .get(&peer)
            .into_iter()
            .flat_map(|waiting| waiting.iter().map(|(waiter, id)| (*waiter, *id)))
    }

    /// How many of `peer`'s 5 its ended sessions still hold.
    pub(crate) fn unsettled_count(&self, peer: PeerKey) -> usize {
        self.unsettled.get(&peer).map_or(0, BTreeMap::len)
    }

    /// Takes `waiter` out of the unsettled submissions. Returns its peer and
    /// id if the reactor held it.
    pub(crate) fn settle(&mut self, waiter: WaiterId) -> Option<(PeerKey, UnminedTxId)> {
        let peer = *self
            .unsettled
            .iter()
            .find(|(_, waiting)| waiting.contains_key(&waiter))?
            .0;
        let waiting = self.unsettled.get_mut(&peer)?;
        let id = waiting.remove(&waiter)?;
        if waiting.is_empty() {
            self.unsettled.remove(&peer);
        }
        Some((peer, id))
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
}
