//! Service-session policy for one connection, without runtime resources.
//!
//! [`PendingState`] assembles the members of an inbound service session. It
//! owns no stream, permit, or reservation: the connection loop holds those and
//! applies the decisions it returns. Time enters as an explicit argument.

use std::time::Duration;

use zakura_rt::{collections::HashMap, time::Instant};

use crate::zakura::{transport::SessionLayout, Stream, ZakuraHandlerError};

#[cfg(test)]
mod tests;

/// One incomplete offer: its wire identifier, the members that finished setup,
/// and the deadline that the first member's arrival set.
#[derive(Clone, Debug)]
struct PendingMetadata {
    id: u64,
    members: Vec<Stream>,
    deadline: Instant,
    /// How long expiry defers the next offer of this service.
    retry_delay: Duration,
}

/// Where an arriving member's service reservation comes from.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(in crate::zakura::handler) enum MemberReservation {
    /// An expired or conflicting offer defers this service. Refuse the member.
    Deferred,
    /// Share the incomplete offer's reservation.
    Share,
    /// Reserve a new session from the service.
    Reserve,
}

/// What an arriving member did to its service's offer.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(in crate::zakura::handler) enum MemberArrival {
    /// The offer waits for its remaining members.
    Waiting,
    /// Every member has arrived. Admit the offer as one session.
    Complete,
    /// The member names another offer than the incomplete one. Drop both
    /// offers; the service defers new offers briefly.
    Conflict,
}

/// The incomplete inbound service sessions of one connection: at most one
/// offer per service.
///
/// The caller retains each arrived member's stream and reservation under the
/// same service kind and drops them whenever this state drops the offer.
#[derive(Debug, Default)]
pub(in crate::zakura::handler) struct PendingState {
    sessions: HashMap<u16, PendingMetadata>,
    retry_after: HashMap<u16, Instant>,
}

impl PendingState {
    /// Where a member of `kind`'s session gets its reservation at `now`.
    pub(in crate::zakura::handler) fn reservation(
        &self,
        kind: u16,
        now: Instant,
    ) -> MemberReservation {
        if self
            .retry_after
            .get(&kind)
            .is_some_and(|retry| *retry > now)
        {
            MemberReservation::Deferred
        } else if self.sessions.contains_key(&kind) {
            MemberReservation::Share
        } else {
            MemberReservation::Reserve
        }
    }

    /// The earliest deadline of an incomplete offer.
    pub(in crate::zakura::handler) fn deadline(&self) -> Option<Instant> {
        self.sessions.values().map(|session| session.deadline).min()
    }

    /// Drop every offer whose deadline has passed and defer another offer of
    /// its service. Returns the kinds of the dropped offers.
    pub(in crate::zakura::handler) fn expire(&mut self, now: Instant) -> Vec<u16> {
        self.retry_after.retain(|_, retry| *retry > now);
        let mut expired = Vec::new();
        for (kind, session) in self
            .sessions
            .extract_if(|_, session| session.deadline <= now)
        {
            self.retry_after.insert(kind, now + session.retry_delay);
            expired.push(kind);
        }
        expired
    }

    /// Record that `member` of `layout` finished setup at `now` under wire
    /// identifier `id`.
    ///
    /// The first member's arrival sets the offer's deadline to `now +
    /// setup_timeout`; later members keep it. An error means the peer sent an
    /// invalid offer; the offer is dropped in every case except
    /// [`MemberArrival::Waiting`].
    pub(in crate::zakura::handler) fn arrive(
        &mut self,
        layout: &SessionLayout,
        id: u64,
        member: Stream,
        setup_timeout: Duration,
        now: Instant,
    ) -> Result<MemberArrival, ZakuraHandlerError> {
        let kind = layout.primary().kind;
        // Remove first so every invalid continuation releases the existing offer.
        let pending = self.sessions.remove(&kind);
        if (layout.is_multi_stream() && id == 0) || !layout.streams.contains(&member) {
            return Err(ZakuraHandlerError::InvalidServiceSession);
        }
        let mut pending = pending.unwrap_or_else(|| PendingMetadata {
            id,
            members: Vec::with_capacity(layout.streams.len()),
            deadline: now + setup_timeout,
            retry_delay: setup_timeout,
        });
        if pending.id != id {
            // A retry can arrive before the abandoned offer's reset. Reject both
            // offers and back off without closing unrelated service sessions.
            self.retry_after.insert(kind, now + setup_timeout);
            return Ok(MemberArrival::Conflict);
        }
        if now >= pending.deadline
            || pending
                .members
                .iter()
                .any(|arrived| arrived.kind == member.kind || !layout.streams.contains(arrived))
        {
            return Err(ZakuraHandlerError::InvalidServiceSession);
        }
        pending.members.push(member);
        if pending.members.len() == layout.streams.len() {
            Ok(MemberArrival::Complete)
        } else {
            self.sessions.insert(kind, pending);
            Ok(MemberArrival::Waiting)
        }
    }
}
