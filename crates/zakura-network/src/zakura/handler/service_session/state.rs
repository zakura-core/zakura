//! Service-session policy for one connection, without runtime resources.
//!
//! [`PendingState`] assembles the members of an inbound service session.
//! [`ServiceSessionState`] tracks one service's active generations and decides
//! when to reopen it. Neither owns a stream, permit, reservation, cancellation
//! token, worker, or wait future: the connection loop holds those and applies
//! the decisions these types return. Time enters as an explicit argument.

use std::time::Duration;

use zakura_rt::{collections::HashMap, time::Instant};

use crate::zakura::{
    transport::SessionLayout, SessionDemand, SessionOpening, SessionPolicy, Stream, StreamMode,
    ZakuraHandlerError,
};

#[cfg(any(test, feature = "p2p-fuzz"))]
mod fuzz;
#[cfg(test)]
mod tests;

#[cfg(any(test, feature = "p2p-fuzz"))]
pub use fuzz::{replay_service_session_bytes, ServiceSessionReplaySummary};

/// Initial delay before a locally parked ordered stream is offered again.
pub(in crate::zakura::handler) const ORDERED_STREAM_REOPEN_BACKOFF: Duration =
    Duration::from_millis(250);
/// Maximum delay between local re-offers of one ordered stream kind.
pub(in crate::zakura::handler) const ORDERED_STREAM_REOPEN_BACKOFF_CAP: Duration =
    Duration::from_secs(8);

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
        if (layout.is_multi_stream() && id == 0) || !layout.streams().contains(&member) {
            return Err(ZakuraHandlerError::InvalidServiceSession);
        }
        let mut pending = pending.unwrap_or_else(|| PendingMetadata {
            id,
            members: Vec::with_capacity(layout.streams().len()),
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
                .any(|arrived| arrived.kind == member.kind || !layout.streams().contains(arrived))
        {
            return Err(ZakuraHandlerError::InvalidServiceSession);
        }
        pending.members.push(member);
        if pending.members.len() == layout.streams().len() {
            Ok(MemberArrival::Complete)
        } else {
            self.sessions.insert(kind, pending);
            Ok(MemberArrival::Waiting)
        }
    }
}

/// An admitted session's workers have all finished.
#[derive(Copy, Clone, Debug)]
pub(in crate::zakura::handler) struct SessionExit {
    pub(in crate::zakura::handler) stream: Stream,
    pub(in crate::zakura::handler) session_id: u64,
    pub(in crate::zakura::handler) opened_locally: bool,
}

/// This endpoint's roles on its connection, which select a service's opener.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(in crate::zakura::handler) struct ConnectionRoles {
    /// Whether this endpoint dialed the connection.
    pub(in crate::zakura::handler) is_initiator: bool,
    /// Whether this endpoint wins the node-id tiebreak for either-side services.
    pub(in crate::zakura::handler) i_open_collision_winner: bool,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(in crate::zakura::handler) enum SessionWaitReason {
    Demand,
    Transport,
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub(in crate::zakura::handler) enum SessionReopenState {
    #[default]
    Idle,
    Waiting(SessionWaitReason),
    Retired,
}

/// The policy-relevant part of a service's [`SessionDemand`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(in crate::zakura::handler) enum DemandWait {
    /// Open now: wait for nothing.
    OpenNow,
    /// Re-check after the demand's own deadline or state change.
    Later,
    /// Never reopen this service on this connection.
    Retire,
}

impl From<&SessionDemand> for DemandWait {
    fn from(demand: &SessionDemand) -> Self {
        match demand {
            SessionDemand::OpenNow => Self::OpenNow,
            SessionDemand::RetryAt(_) | SessionDemand::WaitForChange(_) => Self::Later,
            SessionDemand::Retire => Self::Retire,
        }
    }
}

/// How the caller changes a service's reopen wait after a demand.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(in crate::zakura::handler) enum DemandChange {
    /// Keep the current wait and drop the new demand's wait.
    Keep,
    /// Replace any current wait with the demand's wait.
    Start,
    /// Drop any current wait.
    Cancel,
}

/// How a complete inbound session joins its service.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(in crate::zakura::handler) enum RemoteCompletion {
    /// This endpoint wins the collision with its own session. Park the peer's.
    ParkCollision,
    /// This endpoint lost a collision. Adopt the peer's session; its demand is
    /// implied by this endpoint's own open.
    AdoptCollision,
    /// Adopt the peer's session if the service's reserved demand is `OpenNow`.
    CheckDemand,
}

/// Connection-owned lifecycle for one negotiated ordered service.
///
/// The service has at most one active generation in each opening direction.
/// Only the exit of the exact recorded generation clears it.
#[derive(Debug)]
pub(in crate::zakura::handler) struct ServiceSessionState {
    stream: Stream,
    policy: SessionPolicy,
    roles: ConnectionRoles,
    local_session_id: Option<u64>,
    remote_session_id: Option<u64>,
    // Keep cumulative transport re-offers across successful opens: a remote
    // service can immediately park the stream again, and resetting here would
    // turn persistent refusal into a base-delay reopen loop.
    reopen_attempts: u32,
    reopen_state: SessionReopenState,
}

impl ServiceSessionState {
    /// State for the primary `stream` of a service whose static policy is `policy`.
    pub(in crate::zakura::handler) fn new(
        stream: Stream,
        policy: SessionPolicy,
        roles: ConnectionRoles,
    ) -> Self {
        Self {
            stream,
            policy,
            roles,
            local_session_id: None,
            remote_session_id: None,
            reopen_attempts: 0,
            reopen_state: SessionReopenState::Idle,
        }
    }

    /// The service's primary stream.
    pub(in crate::zakura::handler) fn stream(&self) -> Stream {
        self.stream
    }

    /// Whether this endpoint proactively opens the service's session.
    pub(in crate::zakura::handler) fn opens_locally(&self) -> bool {
        opens_ordered_stream_locally(
            self.policy,
            self.roles.is_initiator,
            self.roles.i_open_collision_winner,
        )
    }

    fn has_active_session(&self) -> bool {
        self.local_session_id.is_some() || self.remote_session_id.is_some()
    }

    /// Whether a peer-opened stream must be dropped before reserving capacity:
    /// a remote session still owns its slot, or this side wins the collision
    /// with its own session.
    pub(in crate::zakura::handler) fn rejects_remote_offer(&self) -> bool {
        self.remote_session_id.is_some()
            || (self.roles.i_open_collision_winner && self.local_session_id.is_some())
    }

    /// Whether a fired reopen wait may check demand: the connection is live,
    /// this endpoint opens the service, and no generation is active.
    pub(in crate::zakura::handler) fn may_reopen(&self, connection_cancelled: bool) -> bool {
        !connection_cancelled && self.opens_locally() && !self.has_active_session()
    }

    /// Adopt a locally opened generation. The caller drops any reopen wait.
    pub(in crate::zakura::handler) fn opened(&mut self, session_id: u64) {
        self.cancel_wait();
        self.local_session_id = Some(session_id);
    }

    /// Record a complete inbound generation, unless this endpoint wins the
    /// collision with its own.
    pub(in crate::zakura::handler) fn remote_completed(
        &mut self,
        session_id: u64,
    ) -> RemoteCompletion {
        let is_collision = self.local_session_id.is_some();
        // The deterministic winner keeps its own stream and parks the peer's.
        // The loser falls through and adopts the peer's stream.
        if is_collision && self.roles.i_open_collision_winner {
            return RemoteCompletion::ParkCollision;
        }
        self.remote_session_id = Some(session_id);
        if is_collision {
            RemoteCompletion::AdoptCollision
        } else {
            RemoteCompletion::CheckDemand
        }
    }

    /// Adopt the recorded inbound generation. The caller drops any reopen wait.
    pub(in crate::zakura::handler) fn remote_adopted(&mut self) {
        self.cancel_wait();
    }

    /// Park the inbound generation `session_id` because its service has no
    /// demand. Frees the service for a later offer.
    ///
    /// Returns the demand's wait change if this endpoint may reopen the
    /// service. Otherwise the entitled remote opener observes the parked
    /// stream and retries.
    pub(in crate::zakura::handler) fn remote_parked(
        &mut self,
        session_id: u64,
        demand: DemandWait,
    ) -> Option<DemandChange> {
        self.remove_active_session(false, session_id);
        (self.policy.reopen && self.opens_locally()).then(|| self.schedule_demand(demand))
    }

    /// Record that `exit`'s generation ended. A stale generation changes
    /// nothing.
    ///
    /// Returns the transport backoff to wait before reopening, if the matching
    /// generation was the last active one and this endpoint reopens the
    /// service.
    pub(in crate::zakura::handler) fn exited(
        &mut self,
        exit: SessionExit,
        connection_cancelled: bool,
    ) -> Option<Duration> {
        let removed = self.remove_active_session(exit.opened_locally, exit.session_id);
        if removed
            && !self.has_active_session()
            && should_reopen_ordered_session(
                exit,
                self.policy,
                self.roles.is_initiator,
                self.roles.i_open_collision_winner,
                connection_cancelled,
            )
        {
            self.schedule_transport_backoff()
        } else {
            None
        }
    }

    fn remove_active_session(&mut self, opened_locally: bool, session_id: u64) -> bool {
        let active_session = if opened_locally {
            &mut self.local_session_id
        } else {
            &mut self.remote_session_id
        };
        if *active_session != Some(session_id) {
            return false;
        }

        *active_session = None;
        true
    }

    /// Apply a service's demand for an absent session.
    ///
    /// Repeated deferred demand keeps the first wait. Any demand replaces a
    /// transport backoff, and immediate demand replaces every wait.
    pub(in crate::zakura::handler) fn schedule_demand(
        &mut self,
        demand: DemandWait,
    ) -> DemandChange {
        match demand {
            DemandWait::OpenNow => {
                self.reopen_state = SessionReopenState::Waiting(SessionWaitReason::Demand);
                DemandChange::Start
            }
            DemandWait::Later => {
                if self.reopen_state == SessionReopenState::Waiting(SessionWaitReason::Demand) {
                    return DemandChange::Keep;
                }
                self.reopen_state = SessionReopenState::Waiting(SessionWaitReason::Demand);
                DemandChange::Start
            }
            DemandWait::Retire => {
                self.reopen_state = SessionReopenState::Retired;
                DemandChange::Cancel
            }
        }
    }

    /// Start a transport backoff if the service has no wait and is not retired.
    /// Returns its delay.
    pub(in crate::zakura::handler) fn schedule_transport_backoff(&mut self) -> Option<Duration> {
        if self.reopen_state != SessionReopenState::Idle {
            return None;
        }
        let delay = ordered_session_reopen_backoff(self.reopen_attempts);
        self.reopen_attempts = self.reopen_attempts.saturating_add(1);
        self.reopen_state = SessionReopenState::Waiting(SessionWaitReason::Transport);
        Some(delay)
    }

    /// The service's reopen wait fired. The caller removes it.
    pub(in crate::zakura::handler) fn wait_finished(&mut self) {
        self.cancel_wait();
    }

    /// Whether the caller holds a reopen wait for this service.
    fn is_waiting(&self) -> bool {
        matches!(self.reopen_state, SessionReopenState::Waiting(_))
    }

    fn cancel_wait(&mut self) {
        if self.is_waiting() {
            self.reopen_state = SessionReopenState::Idle;
        }
    }
}

/// Whether this side may open `kind` itself.
///
/// The initiator opens ordinary ordered streams. Block sync is symmetric, so
/// either side may open it and simultaneous offers use the connection's
/// deterministic collision tiebreak.
pub(in crate::zakura::handler) fn may_open_ordered_stream(
    policy: SessionPolicy,
    is_initiator: bool,
) -> bool {
    is_initiator || policy.opening == SessionOpening::EitherSide
}

/// Whether this endpoint proactively opens an ordered stream.
///
/// Either-side services use the connection's mirror-stable node-id tiebreak so
/// exactly one endpoint opens the bidirectional session. Both endpoints still
/// accept either-side streams, preserving compatibility with peers that race
/// an offer during upgrade.
pub(in crate::zakura::handler) fn opens_ordered_stream_locally(
    policy: SessionPolicy,
    is_initiator: bool,
    i_open_collision_winner: bool,
) -> bool {
    match policy.opening {
        SessionOpening::InitiatorOnly => is_initiator,
        SessionOpening::EitherSide => i_open_collision_winner,
    }
}

/// An ordered service session can be locally parked while its connection and
/// sibling services remain healthy. The endpoint designated by the transport
/// policy keeps offering a replacement with bounded backoff.
pub(in crate::zakura::handler) fn should_reopen_ordered_session(
    exited: SessionExit,
    policy: SessionPolicy,
    is_initiator: bool,
    i_open_collision_winner: bool,
    connection_cancelled: bool,
) -> bool {
    exited.stream.mode == StreamMode::Persistent
        && policy.reopen
        && opens_ordered_stream_locally(policy, is_initiator, i_open_collision_winner)
        && !connection_cancelled
}

/// Exponential reopen delay for one ordered stream kind, capped so a
/// persistently failing local open cannot spin the connection loop.
pub(in crate::zakura::handler) fn ordered_session_reopen_backoff(attempts: u32) -> Duration {
    // The shift is safe: attempts is clamped so the multiplier fits in u32.
    ORDERED_STREAM_REOPEN_BACKOFF
        .saturating_mul(1u32 << attempts.min(8))
        .min(ORDERED_STREAM_REOPEN_BACKOFF_CAP)
}
