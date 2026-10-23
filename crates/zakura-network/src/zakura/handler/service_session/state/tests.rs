//! Service-session policy: unit tests of each decision, the reopen-wait
//! contract with [`ServiceSessionIO`], and the machine test of pending
//! assembly and the active generations from [`super::fuzz`].

use futures::StreamExt as _;
use proptest::prelude::*;
use zakura_rt::{sync::watch, time::timeout};

use super::{
    fuzz::{layout, FOREIGN, MEMBER_A, MEMBER_B, MEMBER_C, SETUP_TIMEOUT},
    *,
};
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

// Machine test.

proptest! {
    /// After every input the session state agrees with the independent model
    /// in [`super::fuzz`].
    #[test]
    fn session_sequences_follow_the_model(
        bytes in prop::collection::vec(any::<u8>(), 0..512),
    ) {
        replay_service_session_bytes(&bytes);
    }
}

/// Fixed seeds run the fuzz entry in every test run, and together exercise
/// every rule the replay counts.
#[test]
fn fixed_seeds_exercise_every_session_rule() {
    let mut total = ServiceSessionReplaySummary::default();
    for seed in 1..=32u64 {
        // A xorshift stream: reproducible bytes without a fuzzing engine.
        let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let bytes: Vec<u8> = (0..512)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state.to_le_bytes()[0]
            })
            .collect();
        let summary = replay_service_session_bytes(&bytes);
        total.completed_offers += summary.completed_offers;
        total.invalid_members += summary.invalid_members;
        total.conflicting_members += summary.conflicting_members;
        total.expired_offers += summary.expired_offers;
        total.deferred_members += summary.deferred_members;
        total.parked_collisions += summary.parked_collisions;
        total.adopted_collisions += summary.adopted_collisions;
        total.local_opens += summary.local_opens;
        total.stale_exits += summary.stale_exits;
        total.reopen_backoffs += summary.reopen_backoffs;
        total.retirements += summary.retirements;
    }
    let ServiceSessionReplaySummary {
        completed_offers,
        invalid_members,
        conflicting_members,
        expired_offers,
        deferred_members,
        parked_collisions,
        adopted_collisions,
        local_opens,
        stale_exits,
        reopen_backoffs,
        retirements,
    } = total.clone();
    for (rule, count) in [
        ("completed offers", completed_offers),
        ("invalid members", invalid_members),
        ("conflicting members", conflicting_members),
        ("expired offers", expired_offers),
        ("deferred members", deferred_members),
        ("parked collisions", parked_collisions),
        ("adopted collisions", adopted_collisions),
        ("local opens", local_opens),
        ("stale exits", stale_exits),
        ("reopen backoffs", reopen_backoffs),
        ("retirements", retirements),
    ] {
        assert!(count > 0, "no fixed seed exercised {rule}: {total:?}");
    }
}
