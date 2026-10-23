//! Service-session policy: unit tests of each decision, the reopen-wait
//! contract with [`ServiceSessionIO`], and a model-based proptest of pending
//! assembly and the active generations.

use std::sync::Arc;

use futures::StreamExt as _;
use proptest::prelude::*;
use zakura_rt::{sync::watch, time::timeout};

use super::*;
use crate::zakura::{
    handler::ServiceSessionIO, ZAKURA_CAP_BLOCK_SYNC_REGULATED, ZAKURA_CAP_HEADER_SYNC,
    ZAKURA_HEADER_SYNC_STREAM_VERSION, ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION,
    ZAKURA_STREAM_BLOCK_SYNC, ZAKURA_STREAM_HEADER_SYNC,
};

const REOPEN: SessionPolicy = SessionPolicy {
    opening: SessionOpening::InitiatorOnly,
    reopen: true,
};
const INITIATOR: ConnectionRoles = ConnectionRoles {
    is_initiator: true,
    i_open_collision_winner: false,
};

fn test_ordered_stream() -> Stream {
    Stream {
        kind: ZAKURA_STREAM_HEADER_SYNC,
        version: ZAKURA_HEADER_SYNC_STREAM_VERSION,
        frame_cap: 1,
        capability: ZAKURA_CAP_HEADER_SYNC,
        ..Stream::PERSISTENT
    }
}

/// State for a reopening service that this endpoint opens.
fn state(stream: Stream) -> ServiceSessionState {
    ServiceSessionState::new(stream, REOPEN, INITIATOR)
}

fn retry_later() -> SessionDemand {
    SessionDemand::RetryAt(Instant::now() + Duration::from_secs(60))
}

// Opening and reopen rules.

/// Reopen a header-sync stream after the responder parks it on a healthy connection.
/// The transport opening roles determine the reopen policy.
#[test]
fn ordered_session_reopen_follows_transport_opening_policy() {
    let initiator_opened = Stream {
        kind: ZAKURA_STREAM_HEADER_SYNC,
        version: ZAKURA_HEADER_SYNC_STREAM_VERSION,
        frame_cap: 1,
        capability: ZAKURA_CAP_HEADER_SYNC,
        ..Stream::PERSISTENT
    };
    let exit = SessionExit {
        stream: initiator_opened,
        session_id: 1,
        opened_locally: true,
    };
    let initiator_policy = SessionPolicy {
        opening: SessionOpening::InitiatorOnly,
        reopen: true,
    };
    assert!(should_reopen_ordered_session(
        exit,
        initiator_policy,
        true,
        false,
        false
    ));
    assert!(!should_reopen_ordered_session(
        exit,
        initiator_policy,
        false,
        true,
        false
    ));
    assert!(!should_reopen_ordered_session(
        exit,
        initiator_policy,
        true,
        false,
        true
    ));
    assert!(!should_reopen_ordered_session(
        exit,
        SessionPolicy::default(),
        true,
        false,
        false,
    ));

    // The authorized side replaces an accepted stream too; replacement does
    // not depend on which physical generation just exited.
    assert!(should_reopen_ordered_session(
        SessionExit {
            opened_locally: false,
            ..exit
        },
        initiator_policy,
        true,
        false,
        false,
    ));

    let either_peer = SessionExit {
        stream: Stream {
            kind: ZAKURA_STREAM_BLOCK_SYNC,
            ..initiator_opened
        },
        ..exit
    };
    let either_policy = SessionPolicy {
        opening: SessionOpening::EitherSide,
        reopen: true,
    };
    assert!(should_reopen_ordered_session(
        either_peer,
        either_policy,
        true,
        true,
        false
    ));
    assert!(!should_reopen_ordered_session(
        either_peer,
        either_policy,
        false,
        false,
        false
    ));
    assert!(opens_ordered_stream_locally(either_policy, false, true));
    assert!(!opens_ordered_stream_locally(either_policy, true, false));

    let request_response = SessionExit {
        stream: Stream {
            kind: 102,
            mode: StreamMode::RequestResponse,
            ..initiator_opened
        },
        ..exit
    };
    assert!(!should_reopen_ordered_session(
        request_response,
        either_policy,
        true,
        true,
        false,
    ));
}

#[test]
fn either_side_session_has_one_proactive_opener_across_connection_roles() {
    let policy = SessionPolicy {
        opening: SessionOpening::EitherSide,
        reopen: true,
    };
    let exit = SessionExit {
        stream: Stream {
            kind: ZAKURA_STREAM_BLOCK_SYNC,
            version: ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION,
            frame_cap: 1,
            capability: ZAKURA_CAP_BLOCK_SYNC_REGULATED,
            ..Stream::PERSISTENT
        },
        session_id: 1,
        opened_locally: false,
    };

    for winner_is_initiator in [false, true] {
        let opens = [
            opens_ordered_stream_locally(policy, winner_is_initiator, true),
            opens_ordered_stream_locally(policy, !winner_is_initiator, false),
        ];
        assert_eq!(opens, [true, false]);
        assert_eq!(opens.into_iter().filter(|opens| *opens).count(), 1);

        let reopens = [
            should_reopen_ordered_session(exit, policy, winner_is_initiator, true, false),
            should_reopen_ordered_session(exit, policy, !winner_is_initiator, false, false),
        ];
        assert_eq!(reopens, [true, false]);
        assert_eq!(reopens.into_iter().filter(|reopens| *reopens).count(), 1);
    }

    // Both endpoints continue accepting an offer for interoperability with
    // peers that still proactively open either-side streams themselves.
    assert!(may_open_ordered_stream(policy, true));
    assert!(may_open_ordered_stream(policy, false));
}

#[test]
fn ordered_session_reopen_backoff_grows_and_caps() {
    assert_eq!(
        ordered_session_reopen_backoff(0),
        ORDERED_STREAM_REOPEN_BACKOFF
    );
    assert_eq!(
        ordered_session_reopen_backoff(1),
        ORDERED_STREAM_REOPEN_BACKOFF * 2
    );
    assert_eq!(
        ordered_session_reopen_backoff(5),
        ORDERED_STREAM_REOPEN_BACKOFF_CAP
    );
    assert_eq!(
        ordered_session_reopen_backoff(u32::MAX),
        ORDERED_STREAM_REOPEN_BACKOFF_CAP
    );
}

// Active generations.

#[test]
fn stale_ordered_session_exit_keeps_newer_active_generation() {
    let mut session = state(test_ordered_stream());
    session.remote_session_id = Some(2);

    assert!(!session.remove_active_session(false, 1));
    assert_eq!(session.remote_session_id, Some(2));
    assert!(session.remove_active_session(false, 2));
    assert_eq!(session.remote_session_id, None);
}

#[test]
fn losing_or_duplicate_remote_offers_are_rejected_before_reservation() {
    let winner = ConnectionRoles {
        is_initiator: false,
        i_open_collision_winner: true,
    };
    let mut winning = ServiceSessionState::new(test_ordered_stream(), REOPEN, winner);
    let mut losing = state(test_ordered_stream());
    assert!(!winning.rejects_remote_offer());
    winning.local_session_id = Some(1);
    losing.local_session_id = Some(1);
    assert!(
        winning.rejects_remote_offer(),
        "the collision winner keeps its own session"
    );
    assert!(
        !losing.rejects_remote_offer(),
        "the collision loser adopts the peer's stream"
    );
    losing.local_session_id = None;
    losing.remote_session_id = Some(2);
    assert!(losing.rejects_remote_offer());
}

/// A parked generation's late exit cannot remove the replacement adopted after it.
#[test]
fn an_old_generation_exit_leaves_the_replacement_active() {
    let stream = test_ordered_stream();
    let mut session = state(stream);
    assert_eq!(session.remote_completed(1), RemoteCompletion::CheckDemand);
    assert_eq!(
        session.remote_parked(1, DemandWait::Later),
        Some(DemandChange::Start)
    );
    assert!(!session.has_active_session(), "parking frees the service");
    session.wait_finished();
    assert_eq!(session.remote_completed(2), RemoteCompletion::CheckDemand);
    session.remote_adopted();

    let old = SessionExit {
        stream,
        session_id: 1,
        opened_locally: false,
    };
    assert_eq!(session.exited(old, false), None);
    assert_eq!(session.remote_session_id, Some(2));
    assert!(!session.is_waiting(), "a stale exit schedules no reopen");

    let current = SessionExit {
        session_id: 2,
        ..old
    };
    assert_eq!(
        session.exited(current, false),
        Some(ORDERED_STREAM_REOPEN_BACKOFF)
    );
    assert!(!session.has_active_session());
}

/// The collision loser adopts the peer's generation. Its own generation's exit
/// then leaves the adopted one active and schedules no reopen.
#[test]
fn a_lost_collision_keeps_the_adopted_generation_after_the_local_exit() {
    let stream = test_ordered_stream();
    let mut session = state(stream);
    session.opened(1);
    assert_eq!(
        session.remote_completed(2),
        RemoteCompletion::AdoptCollision
    );
    session.remote_adopted();

    let local = SessionExit {
        stream,
        session_id: 1,
        opened_locally: true,
    };
    assert_eq!(session.exited(local, false), None);
    assert_eq!(session.local_session_id, None);
    assert_eq!(session.remote_session_id, Some(2));
}

// Pending assembly.

const MEMBER_A: Stream = Stream {
    kind: 64,
    version: 1,
    frame_cap: 1024,
    capability: 1 << 16,
    ..Stream::PERSISTENT
};
const MEMBER_B: Stream = Stream {
    kind: 65,
    ..MEMBER_A
};
const MEMBER_C: Stream = Stream {
    kind: 67,
    ..MEMBER_A
};
/// A stream outside the layout.
const FOREIGN: Stream = Stream {
    kind: 70,
    ..MEMBER_A
};
const SETUP_TIMEOUT: Duration = Duration::from_millis(100);

fn layout() -> SessionLayout {
    SessionLayout {
        streams: Arc::from([MEMBER_A, MEMBER_B, MEMBER_C]),
    }
}

/// A member arriving just before the deadline keeps the first member's
/// deadline, and the offer expires on it.
#[test]
fn a_late_member_keeps_the_first_arrivals_deadline() {
    let layout = layout();
    let start = Instant::now();
    let deadline = start + SETUP_TIMEOUT;
    let mut pending = PendingState::default();
    assert_eq!(
        pending.reservation(MEMBER_A.kind, start),
        MemberReservation::Reserve
    );
    assert_eq!(
        pending
            .arrive(&layout, 7, MEMBER_A, SETUP_TIMEOUT, start)
            .ok(),
        Some(MemberArrival::Waiting)
    );
    let late = deadline - Duration::from_millis(1);
    assert_eq!(
        pending.reservation(MEMBER_A.kind, late),
        MemberReservation::Share
    );
    assert_eq!(
        pending
            .arrive(&layout, 7, MEMBER_B, SETUP_TIMEOUT, late)
            .ok(),
        Some(MemberArrival::Waiting)
    );
    assert_eq!(pending.deadline(), Some(deadline));
    assert_eq!(pending.expire(late), Vec::<u16>::new());
    assert_eq!(pending.expire(deadline), vec![MEMBER_A.kind]);
    assert_eq!(pending.deadline(), None);
    assert_eq!(
        pending.reservation(MEMBER_A.kind, deadline),
        MemberReservation::Deferred,
        "expiry defers the next offer"
    );
    assert_eq!(
        pending.reservation(MEMBER_A.kind, deadline + SETUP_TIMEOUT),
        MemberReservation::Reserve
    );
}

/// A duplicate member near the deadline is invalid: it drops the offer rather
/// than restarting the deadline.
#[test]
fn a_duplicate_member_drops_the_offer_without_restarting_its_deadline() {
    let layout = layout();
    let start = Instant::now();
    let mut pending = PendingState::default();
    assert_eq!(
        pending
            .arrive(&layout, 7, MEMBER_A, SETUP_TIMEOUT, start)
            .ok(),
        Some(MemberArrival::Waiting)
    );
    let late = start + SETUP_TIMEOUT - Duration::from_millis(1);
    assert!(matches!(
        pending.arrive(&layout, 7, MEMBER_A, SETUP_TIMEOUT, late),
        Err(ZakuraHandlerError::InvalidServiceSession)
    ));
    assert_eq!(pending.deadline(), None);
}

#[test]
fn a_foreign_member_or_a_zero_id_is_invalid_and_drops_the_offer() {
    let layout = layout();
    let start = Instant::now();
    let mut pending = PendingState::default();
    assert_eq!(
        pending
            .arrive(&layout, 7, MEMBER_A, SETUP_TIMEOUT, start)
            .ok(),
        Some(MemberArrival::Waiting)
    );
    assert!(matches!(
        pending.arrive(&layout, 7, FOREIGN, SETUP_TIMEOUT, start),
        Err(ZakuraHandlerError::InvalidServiceSession)
    ));
    assert_eq!(
        pending.deadline(),
        None,
        "an invalid member drops the offer"
    );
    assert!(matches!(
        pending.arrive(&layout, 0, MEMBER_A, SETUP_TIMEOUT, start),
        Err(ZakuraHandlerError::InvalidServiceSession)
    ));
    assert_eq!(pending.deadline(), None);
}

#[test]
fn a_conflicting_offer_drops_both_offers_and_defers_the_service() {
    let layout = layout();
    let start = Instant::now();
    let mut pending = PendingState::default();
    assert_eq!(
        pending
            .arrive(&layout, 7, MEMBER_A, SETUP_TIMEOUT, start)
            .ok(),
        Some(MemberArrival::Waiting)
    );
    assert_eq!(
        pending
            .arrive(&layout, 8, MEMBER_B, SETUP_TIMEOUT, start)
            .ok(),
        Some(MemberArrival::Conflict)
    );
    assert_eq!(pending.deadline(), None);
    assert_eq!(
        pending.reservation(MEMBER_A.kind, start),
        MemberReservation::Deferred
    );
}

#[test]
fn every_member_completes_the_offer_in_any_order() {
    let layout = layout();
    let start = Instant::now();
    let mut pending = PendingState::default();
    for (member, arrival) in [
        (MEMBER_C, MemberArrival::Waiting),
        (MEMBER_A, MemberArrival::Waiting),
        (MEMBER_B, MemberArrival::Complete),
    ] {
        assert_eq!(
            pending
                .arrive(&layout, 7, member, SETUP_TIMEOUT, start)
                .ok(),
            Some(arrival)
        );
    }
    assert_eq!(pending.deadline(), None);
    assert_eq!(
        pending.reservation(MEMBER_A.kind, start),
        MemberReservation::Reserve
    );
}

// The reopen-wait contract between the state and `ServiceSessionIO`.

#[tokio::test(start_paused = true)]
async fn session_waits_deduplicate_demand() {
    let mut session = state(test_ordered_stream());
    let mut io = ServiceSessionIO::default();
    let (change_tx, change_rx) = watch::channel(());

    io.schedule_demand(
        &mut session,
        SessionDemand::WaitForChange(Box::pin(async move {
            let mut change_rx = change_rx;
            let _ = change_rx.changed().await;
        })),
    );
    io.schedule_demand(&mut session, retry_later());

    assert_eq!(io.waits.len(), 1);
    assert_eq!(
        session.reopen_state,
        SessionReopenState::Waiting(SessionWaitReason::Demand)
    );
    assert_eq!(
        change_tx.receiver_count(),
        1,
        "repeated demand must retain the original reactor wait"
    );
}

#[tokio::test(start_paused = true)]
async fn session_demand_replaces_transport_backoff() {
    let mut session = state(test_ordered_stream());
    let mut io = ServiceSessionIO::default();

    assert_eq!(
        io.schedule_transport_backoff(&mut session),
        Some(ORDERED_STREAM_REOPEN_BACKOFF)
    );
    io.schedule_demand(&mut session, retry_later());

    assert_eq!(session.reopen_attempts, 1);
    assert_eq!(io.waits.len(), 1);
    assert_eq!(
        session.reopen_state,
        SessionReopenState::Waiting(SessionWaitReason::Demand)
    );
}

#[tokio::test(start_paused = true)]
async fn ordered_session_exit_keeps_existing_demand_wait() {
    let mut session = state(test_ordered_stream());
    let mut io = ServiceSessionIO::default();

    io.schedule_demand(&mut session, retry_later());

    assert_eq!(io.schedule_transport_backoff(&mut session), None);
    assert_eq!(session.reopen_attempts, 0);
    assert_eq!(io.waits.len(), 1);
    assert_eq!(
        session.reopen_state,
        SessionReopenState::Waiting(SessionWaitReason::Demand)
    );
}

#[tokio::test]
async fn ordered_session_retirement_cancels_and_blocks_transport_waits() {
    let mut session = state(test_ordered_stream());
    let mut io = ServiceSessionIO::default();

    io.schedule_demand(&mut session, retry_later());
    io.schedule_demand(&mut session, SessionDemand::Retire);

    assert!(io.waits.is_empty());
    assert_eq!(session.reopen_state, SessionReopenState::Retired);
    assert_eq!(io.schedule_transport_backoff(&mut session), None);
    assert_eq!(session.reopen_attempts, 0);
}

#[tokio::test]
async fn ordered_session_adoption_drops_pending_wait() {
    let stream = test_ordered_stream();
    let mut session = state(stream);
    let mut io = ServiceSessionIO::default();

    io.schedule_demand(&mut session, SessionDemand::OpenNow);
    session.opened(1);
    io.cancel(stream.kind);

    assert!(io.waits.is_empty());
    assert_eq!(session.reopen_state, SessionReopenState::Idle);
}

#[tokio::test]
async fn session_demand_change_yields_exactly_one_reopen() {
    let stream = test_ordered_stream();
    let mut session = state(stream);
    let mut io = ServiceSessionIO::default();
    let (change_tx, mut change_rx) = watch::channel(());

    io.schedule_demand(
        &mut session,
        SessionDemand::WaitForChange(Box::pin(async move {
            let _ = change_rx.changed().await;
        })),
    );
    change_tx.send_replace(());
    let (kind, ()) = timeout(Duration::from_secs(1), io.waits.next())
        .await
        .expect("reactor change is observed before timeout")
        .expect("connection still owns a pending session wait");

    assert_eq!(kind, stream.kind);
    io.finish(kind);
    session.wait_finished();
    assert!(
        io.waits.is_empty(),
        "one state change yields one reopen attempt"
    );
    assert!(session.may_reopen(false));
}

#[tokio::test(start_paused = true)]
async fn session_demand_deadline_yields_exactly_one_reopen() {
    let stream = test_ordered_stream();
    let mut session = state(stream);
    let mut io = ServiceSessionIO::default();

    io.schedule_demand(&mut session, retry_later());
    tokio::time::advance(Duration::from_secs(60)).await;
    let (kind, ()) = io
        .waits
        .next()
        .await
        .expect("reactor deadline keeps a pending session wait");

    assert_eq!(kind, stream.kind);
    io.finish(kind);
    session.wait_finished();
    assert!(
        io.waits.is_empty(),
        "one deadline yields one reopen attempt"
    );
}

#[test]
fn ordered_session_connection_teardown_drops_pending_waits() {
    let mut session = state(test_ordered_stream());
    let mut io = ServiceSessionIO::default();
    let (change_tx, mut change_rx) = watch::channel(());

    io.schedule_demand(
        &mut session,
        SessionDemand::WaitForChange(Box::pin(async move {
            let _ = change_rx.changed().await;
        })),
    );
    assert_eq!(change_tx.receiver_count(), 1);
    drop(io);

    assert_eq!(
        change_tx.receiver_count(),
        0,
        "dropping the connection-owned wait map must drop every pending wait"
    );
}

// Model-based sequences.

/// The layout members, then the foreign stream.
const MEMBERS: [Stream; 4] = [MEMBER_A, MEMBER_B, MEMBER_C, FOREIGN];

/// One input the connection loop delivers, in the order it would deliver it.
#[derive(Clone, Debug)]
enum Op {
    /// A member finishes inbound setup after `advance`. A complete offer is
    /// adopted if `open_now`, else parked under a deferred demand.
    Arrive {
        member: usize,
        id: u64,
        advance: Duration,
        open_now: bool,
    },
    /// The pending deadline timer fires after `advance`.
    Expire { advance: Duration },
    /// The service reports demand outside any open.
    Demand(DemandWait),
    /// The service's reopen wait fires. The service then reports
    /// `OpenNow` if `open_now`, and a local open succeeds if `opens`.
    WaitFires { open_now: bool, opens: bool },
    /// Every worker of the `generation`-th admitted generation finishes.
    Exit { generation: usize },
}

fn demand_wait() -> impl Strategy<Value = DemandWait> {
    prop_oneof![
        Just(DemandWait::OpenNow),
        Just(DemandWait::Later),
        Just(DemandWait::Retire),
    ]
}

/// Mostly layout members; sometimes the foreign stream.
fn member() -> impl Strategy<Value = usize> {
    prop_oneof![9 => 0..3usize, 1 => Just(3usize)]
}

/// Mostly one of two offers; sometimes the invalid zero identifier.
fn wire_id() -> impl Strategy<Value = u64> {
    prop_oneof![9 => 1..3u64, 1 => Just(0u64)]
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => (member(), wire_id(), 0..60u64, any::<bool>()).prop_map(
            |(member, id, advance, open_now)| Op::Arrive {
                member,
                id,
                advance: Duration::from_millis(advance),
                open_now,
            }
        ),
        1 => (0..150u64).prop_map(|advance| Op::Expire {
            advance: Duration::from_millis(advance),
        }),
        1 => demand_wait().prop_map(Op::Demand),
        2 => (any::<bool>(), any::<bool>())
            .prop_map(|(open_now, opens)| Op::WaitFires { open_now, opens }),
        2 => (0..8usize).prop_map(|generation| Op::Exit { generation }),
    ]
}

/// One input, or a whole offer whose members arrive back to back.
fn ops() -> impl Strategy<Value = Vec<Op>> {
    let offer = (wire_id(), any::<bool>()).prop_map(|(id, open_now)| {
        (0..3)
            .map(|member| Op::Arrive {
                member,
                id,
                advance: Duration::ZERO,
                open_now,
            })
            .collect()
    });
    prop::collection::vec(
        prop_oneof![3 => op().prop_map(|op| vec![op]), 1 => offer],
        1..48,
    )
    .prop_map(|chunks| chunks.concat())
}

fn policy() -> impl Strategy<Value = (SessionPolicy, ConnectionRoles)> {
    (any::<bool>(), any::<bool>(), any::<bool>(), any::<bool>()).prop_map(
        |(either_side, reopen, is_initiator, i_open_collision_winner)| {
            (
                SessionPolicy {
                    opening: if either_side {
                        SessionOpening::EitherSide
                    } else {
                        SessionOpening::InitiatorOnly
                    },
                    reopen,
                },
                ConnectionRoles {
                    is_initiator,
                    i_open_collision_winner,
                },
            )
        },
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ModelWait {
    Idle,
    Demand,
    Transport,
    Retired,
}

/// An independent statement of the session rules for one three-member service.
#[derive(Debug)]
struct Model {
    opens_locally: bool,
    reopens: bool,
    winner: bool,
    /// The incomplete offer: its id, arrived member indexes, and deadline.
    offer: Option<(u64, Vec<usize>, Instant)>,
    deferred_until: Option<Instant>,
    local: Option<u64>,
    remote: Option<u64>,
    attempts: u32,
    wait: ModelWait,
}

impl Model {
    fn new(policy: SessionPolicy, roles: ConnectionRoles) -> Self {
        let opens_locally = if policy.opening == SessionOpening::EitherSide {
            roles.i_open_collision_winner
        } else {
            roles.is_initiator
        };
        Self {
            opens_locally,
            reopens: policy.reopen,
            winner: roles.i_open_collision_winner,
            offer: None,
            deferred_until: None,
            local: None,
            remote: None,
            attempts: 0,
            wait: ModelWait::Idle,
        }
    }

    fn rejects_offer(&self) -> bool {
        self.remote.is_some() || (self.winner && self.local.is_some())
    }

    fn reservation(&self, now: Instant) -> MemberReservation {
        match (self.deferred_until, &self.offer) {
            (Some(until), _) if until > now => MemberReservation::Deferred,
            (_, Some(_)) => MemberReservation::Share,
            _ => MemberReservation::Reserve,
        }
    }

    /// `None` means the offer is invalid and the connection closes.
    fn arrive(&mut self, member: usize, id: u64, now: Instant) -> Option<MemberArrival> {
        let offer = self.offer.take();
        if member == 3 || id == 0 {
            return None;
        }
        let (offer_id, mut members, deadline) =
            offer.unwrap_or((id, Vec::new(), now + SETUP_TIMEOUT));
        if offer_id != id {
            self.deferred_until = Some(now + SETUP_TIMEOUT);
            return Some(MemberArrival::Conflict);
        }
        if now >= deadline || members.contains(&member) {
            return None;
        }
        members.push(member);
        if members.len() == 3 {
            return Some(MemberArrival::Complete);
        }
        self.offer = Some((offer_id, members, deadline));
        Some(MemberArrival::Waiting)
    }

    fn expire(&mut self, now: Instant) -> bool {
        if self.deferred_until.is_some_and(|until| until <= now) {
            self.deferred_until = None;
        }
        let expired = self.offer.as_ref().is_some_and(|offer| offer.2 <= now);
        if expired {
            self.offer = None;
            self.deferred_until = Some(now + SETUP_TIMEOUT);
        }
        expired
    }

    fn demand(&mut self, demand: DemandWait) -> DemandChange {
        match (demand, self.wait) {
            (DemandWait::Retire, _) => {
                self.wait = ModelWait::Retired;
                DemandChange::Cancel
            }
            (DemandWait::Later, ModelWait::Demand) => DemandChange::Keep,
            _ => {
                self.wait = ModelWait::Demand;
                DemandChange::Start
            }
        }
    }

    fn backoff(&mut self) -> Option<Duration> {
        if self.wait != ModelWait::Idle {
            return None;
        }
        let delay = (Duration::from_millis(250) * 2u32.pow(self.attempts.min(8)))
            .min(Duration::from_secs(8));
        self.attempts += 1;
        self.wait = ModelWait::Transport;
        Some(delay)
    }

    fn clear_wait(&mut self) {
        if matches!(self.wait, ModelWait::Demand | ModelWait::Transport) {
            self.wait = ModelWait::Idle;
        }
    }

    fn exit(&mut self, opened_locally: bool, id: u64) -> Option<Duration> {
        let slot = if opened_locally {
            &mut self.local
        } else {
            &mut self.remote
        };
        if *slot != Some(id) {
            return None;
        }
        *slot = None;
        let active = self.local.is_some() || self.remote.is_some();
        if !active && self.reopens && self.opens_locally {
            self.backoff()
        } else {
            None
        }
    }
}

/// The connection loop's one wait per service, as `ServiceSessionIO` keeps it.
#[derive(Debug, Default)]
struct WaitSlot(Option<DemandOrBackoff>);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DemandOrBackoff {
    Demand,
    Backoff,
}

impl WaitSlot {
    fn apply(&mut self, change: DemandChange) {
        match change {
            DemandChange::Keep => {}
            DemandChange::Cancel => self.0 = None,
            DemandChange::Start => self.0 = Some(DemandOrBackoff::Demand),
        }
    }

    fn backoff(&mut self, delay: Option<Duration>) -> Result<(), TestCaseError> {
        if delay.is_some() {
            prop_assert!(self.0.is_none(), "a backoff starts only without a wait");
            self.0 = Some(DemandOrBackoff::Backoff);
        }
        Ok(())
    }
}

proptest! {
    /// After every input the session state agrees with an independent model:
    /// one incomplete offer whose deadline only its first member sets, one
    /// active generation per opening direction that only its own exit clears,
    /// a reopen wait held exactly while the state is waiting, and backoffs
    /// that grow and cap.
    #[test]
    fn session_sequences_follow_the_model(
        (policy, roles) in policy(),
        ops in ops(),
    ) {
        let layout = layout();
        let kind = MEMBER_A.kind;
        let stream = MEMBER_A;
        let mut now = Instant::now();
        let mut pending = PendingState::default();
        let mut session = ServiceSessionState::new(stream, policy, roles);
        let mut wait = WaitSlot::default();
        let mut model = Model::new(policy, roles);
        // Every admitted generation: its id, direction, and whether it exited.
        let mut generations: Vec<(u64, bool, bool)> = Vec::new();
        let mut next_id = 1u64;

        for op in ops {
            match op {
                Op::Arrive { member, id, advance, open_now } => {
                    now += advance;
                    prop_assert_eq!(session.rejects_remote_offer(), model.rejects_offer());
                    if model.rejects_offer() {
                        continue;
                    }
                    let reservation = pending.reservation(kind, now);
                    prop_assert_eq!(reservation, model.reservation(now));
                    if reservation == MemberReservation::Deferred {
                        continue;
                    }
                    let arrival =
                        pending.arrive(&layout, id, MEMBERS[member], SETUP_TIMEOUT, now);
                    let expected = model.arrive(member, id, now);
                    prop_assert_eq!(arrival.as_ref().ok().copied(), expected);
                    // Production closes the connection after an invalid
                    // offer. The state stays defined, so keep exploring.
                    if arrival.is_err() {
                        continue;
                    }
                    if expected != Some(MemberArrival::Complete) {
                        continue;
                    }
                    let generation = next_id;
                    next_id += 1;
                    // A parked generation's exit still arrives later.
                    generations.push((generation, false, false));
                    let completion = session.remote_completed(generation);
                    let collision = model.local.is_some();
                    if collision && model.winner {
                        prop_assert_eq!(completion, RemoteCompletion::ParkCollision);
                        continue;
                    }
                    model.remote = Some(generation);
                    if collision {
                        prop_assert_eq!(completion, RemoteCompletion::AdoptCollision);
                    } else {
                        prop_assert_eq!(completion, RemoteCompletion::CheckDemand);
                    }
                    if collision || open_now {
                        session.remote_adopted();
                        wait.apply(DemandChange::Cancel);
                        model.clear_wait();
                    } else {
                        let change = session.remote_parked(generation, DemandWait::Later);
                        model.remote = None;
                        let expected = (model.reopens && model.opens_locally)
                            .then(|| model.demand(DemandWait::Later));
                        prop_assert_eq!(change, expected);
                        if let Some(change) = change {
                            wait.apply(change);
                        }
                    }
                }
                Op::Expire { advance } => {
                    now += advance;
                    let expired = pending.expire(now);
                    let expected = model.expire(now);
                    prop_assert_eq!(expired, if expected { vec![kind] } else { Vec::new() });
                }
                Op::Demand(demand) => {
                    let change = session.schedule_demand(demand);
                    prop_assert_eq!(change, model.demand(demand));
                    wait.apply(change);
                }
                Op::WaitFires { open_now, opens } => {
                    if wait.0.take().is_none() {
                        continue;
                    }
                    session.wait_finished();
                    model.clear_wait();
                    let may_reopen = model.opens_locally
                        && model.local.is_none()
                        && model.remote.is_none();
                    prop_assert_eq!(session.may_reopen(false), may_reopen);
                    if !may_reopen {
                        continue;
                    }
                    if !open_now {
                        let change = session.schedule_demand(DemandWait::Later);
                        prop_assert_eq!(change, model.demand(DemandWait::Later));
                        wait.apply(change);
                    } else if opens {
                        let generation = next_id;
                        next_id += 1;
                        session.opened(generation);
                        wait.apply(DemandChange::Cancel);
                        model.clear_wait();
                        model.local = Some(generation);
                        generations.push((generation, true, false));
                    } else {
                        let delay = session.schedule_transport_backoff();
                        prop_assert_eq!(delay, model.backoff());
                        wait.backoff(delay)?;
                    }
                }
                Op::Exit { generation } => {
                    let Some((id, opened_locally, exited)) = generations.get_mut(generation)
                    else {
                        continue;
                    };
                    if *exited {
                        continue;
                    }
                    *exited = true;
                    let before = (session.local_session_id, session.remote_session_id);
                    let stale = if *opened_locally {
                        model.local != Some(*id)
                    } else {
                        model.remote != Some(*id)
                    };
                    let exit = SessionExit {
                        stream,
                        session_id: *id,
                        opened_locally: *opened_locally,
                    };
                    let delay = session.exited(exit, false);
                    prop_assert_eq!(delay, model.exit(*opened_locally, *id));
                    if stale {
                        prop_assert_eq!(
                            (session.local_session_id, session.remote_session_id),
                            before,
                            "a stale exit leaves the active generations"
                        );
                    }
                    wait.backoff(delay)?;
                }
            }
            prop_assert_eq!(pending.deadline(), model.offer.as_ref().map(|offer| offer.2));
            prop_assert_eq!(session.local_session_id, model.local);
            prop_assert_eq!(session.remote_session_id, model.remote);
            prop_assert_eq!(session.reopen_attempts, model.attempts);
            prop_assert_eq!(session.is_waiting(), wait.0.is_some());
            prop_assert_eq!(
                session.reopen_state,
                match model.wait {
                    ModelWait::Idle => SessionReopenState::Idle,
                    ModelWait::Demand => SessionReopenState::Waiting(SessionWaitReason::Demand),
                    ModelWait::Transport => {
                        SessionReopenState::Waiting(SessionWaitReason::Transport)
                    }
                    ModelWait::Retired => SessionReopenState::Retired,
                }
            );
        }
    }
}
