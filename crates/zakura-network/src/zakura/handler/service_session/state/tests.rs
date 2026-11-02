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
    handler::{CloseCause, ServiceSessionIO, ZakuraConnTrace},
    ZAKURA_CAP_HEADER_SYNC, ZAKURA_HEADER_SYNC_STREAM_VERSION, ZAKURA_STREAM_HEADER_SYNC,
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

/// The connection's session IO for that one service.
fn session_io(stream: Stream) -> ServiceSessionIO {
    ServiceSessionIO::new(
        [(stream.kind, state(stream))].into_iter().collect(),
        ZakuraConnTrace::without_peer(1),
        CloseCause::new(),
    )
}

fn retry_later() -> SessionDemand {
    SessionDemand::RetryAt(Instant::now() + Duration::from_secs(60))
}

// Opening and reopen rules.

/// The transport opening roles select the one endpoint that opens and reopens
/// a service, whichever generation just exited. Both endpoints still accept an
/// either-side offer, for interoperability with peers that open one themselves.
#[test]
fn ordered_session_reopen_follows_transport_opening_policy() {
    const fn roles(is_initiator: bool, i_open_collision_winner: bool) -> ConnectionRoles {
        ConnectionRoles {
            is_initiator,
            i_open_collision_winner,
        }
    }
    const EITHER: SessionPolicy = SessionPolicy {
        opening: SessionOpening::EitherSide,
        reopen: true,
    };
    let stream = test_ordered_stream();
    let local = SessionExit {
        stream,
        session_id: 1,
        opened_locally: true,
    };
    let remote = SessionExit {
        opened_locally: false,
        ..local
    };
    let request_response = SessionExit {
        stream: Stream {
            kind: 102,
            mode: StreamMode::RequestResponse,
            ..stream
        },
        ..local
    };

    // Exit, policy, roles, connection cancelled, and whether the service reopens.
    for (exit, policy, roles, cancelled, reopens) in [
        (local, REOPEN, roles(true, false), false, true),
        (remote, REOPEN, roles(true, false), false, true),
        (local, REOPEN, roles(false, true), false, false),
        (local, REOPEN, roles(true, false), true, false),
        (
            local,
            SessionPolicy::default(),
            roles(true, false),
            false,
            false,
        ),
        (local, EITHER, roles(true, true), false, true),
        (local, EITHER, roles(false, false), false, false),
        (remote, EITHER, roles(false, true), false, true),
        (remote, EITHER, roles(true, false), false, false),
        (remote, EITHER, roles(true, true), false, true),
        (remote, EITHER, roles(false, false), false, false),
        (request_response, EITHER, roles(true, true), false, false),
    ] {
        assert_eq!(
            should_reopen_ordered_session(exit, policy, roles, cancelled),
            reopens,
            "{exit:?} {policy:?} {roles:?} cancelled={cancelled}"
        );
    }

    // The collision winner alone opens an either-side service, whoever dialed.
    for winner_is_initiator in [false, true] {
        assert!(opens_ordered_stream_locally(
            EITHER,
            roles(winner_is_initiator, true)
        ));
        assert!(!opens_ordered_stream_locally(
            EITHER,
            roles(!winner_is_initiator, false)
        ));
    }
    assert!(may_open_ordered_stream(EITHER, true));
    assert!(may_open_ordered_stream(EITHER, false));
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
        session.remote_demand(1, DemandWait::Later),
        RemoteDemand::Park(Some(DemandChange::Start))
    );
    assert!(!session.has_active_session(), "parking frees the service");
    session.clear_wait();
    assert_eq!(session.remote_completed(2), RemoteCompletion::CheckDemand);
    assert_eq!(
        session.remote_demand(2, DemandWait::OpenNow),
        RemoteDemand::Adopt
    );

    let old = SessionExit {
        stream,
        session_id: 1,
        opened_locally: false,
    };
    assert_eq!(session.exited(old, false).reopen_after, None);
    assert_eq!(session.remote_session_id, Some(2));
    assert!(!session.is_waiting(), "a stale exit schedules no reopen");

    let current = SessionExit {
        session_id: 2,
        ..old
    };
    assert_eq!(
        session.exited(current, false).reopen_after,
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
    assert_eq!(
        session.remote_demand(2, DemandWait::OpenNow),
        RemoteDemand::Adopt
    );

    let local = SessionExit {
        stream,
        session_id: 1,
        opened_locally: true,
    };
    assert_eq!(session.exited(local, false).reopen_after, None);
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
    assert_eq!(pending.expire(late), Vec::new());
    assert_eq!(pending.expire(deadline), vec![(MEMBER_A.kind, 7)]);
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

const KIND: u16 = ZAKURA_STREAM_HEADER_SYNC;

#[tokio::test(start_paused = true)]
async fn session_waits_deduplicate_demand() {
    let mut io = session_io(test_ordered_stream());
    let (change_tx, change_rx) = watch::channel(());

    io.schedule_demand(
        KIND,
        SessionDemand::WaitForChange(Box::pin(async move {
            let mut change_rx = change_rx;
            let _ = change_rx.changed().await;
        })),
    );
    io.schedule_demand(KIND, retry_later());

    assert_eq!(io.waits.len(), 1);
    assert_eq!(
        io.session(KIND).reopen_state,
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
    let mut io = session_io(test_ordered_stream());

    assert_eq!(
        io.schedule_transport_backoff(KIND),
        Some(ORDERED_STREAM_REOPEN_BACKOFF)
    );
    io.schedule_demand(KIND, retry_later());

    assert_eq!(io.session(KIND).reopen_attempts, 1);
    assert_eq!(io.waits.len(), 1);
    assert_eq!(
        io.session(KIND).reopen_state,
        SessionReopenState::Waiting(SessionWaitReason::Demand)
    );
}

#[tokio::test(start_paused = true)]
async fn ordered_session_exit_keeps_existing_demand_wait() {
    let mut io = session_io(test_ordered_stream());

    io.schedule_demand(KIND, retry_later());

    assert_eq!(io.schedule_transport_backoff(KIND), None);
    assert_eq!(io.session(KIND).reopen_attempts, 0);
    assert_eq!(io.waits.len(), 1);
    assert_eq!(
        io.session(KIND).reopen_state,
        SessionReopenState::Waiting(SessionWaitReason::Demand)
    );
}

#[tokio::test]
async fn ordered_session_retirement_cancels_and_blocks_transport_waits() {
    let mut io = session_io(test_ordered_stream());

    io.schedule_demand(KIND, retry_later());
    io.schedule_demand(KIND, SessionDemand::Retire);

    assert!(io.waits.is_empty());
    assert_eq!(io.session(KIND).reopen_state, SessionReopenState::Retired);
    assert_eq!(io.schedule_transport_backoff(KIND), None);
    assert_eq!(io.session(KIND).reopen_attempts, 0);
}

#[tokio::test]
async fn ordered_session_adoption_drops_pending_wait() {
    let mut io = session_io(test_ordered_stream());

    io.schedule_demand(KIND, SessionDemand::OpenNow);
    let attempt = io.begin_attempt(KIND);
    io.on_opened(KIND, 1, attempt);

    assert!(io.waits.is_empty());
    assert_eq!(io.session(KIND).reopen_state, SessionReopenState::Idle);
}

#[tokio::test]
async fn session_demand_change_yields_exactly_one_reopen() {
    let mut io = session_io(test_ordered_stream());
    let (change_tx, mut change_rx) = watch::channel(());

    io.schedule_demand(
        KIND,
        SessionDemand::WaitForChange(Box::pin(async move {
            let _ = change_rx.changed().await;
        })),
    );
    change_tx.send_replace(());
    let (kind, ()) = timeout(Duration::from_secs(1), io.waits.next())
        .await
        .expect("reactor change is observed before timeout")
        .expect("connection still owns a pending session wait");

    assert_eq!(kind, KIND);
    assert_eq!(io.on_wait_fired(kind, false), Some(test_ordered_stream()));
    assert!(
        io.waits.is_empty(),
        "one state change yields one reopen attempt"
    );
}

#[tokio::test(start_paused = true)]
async fn session_demand_deadline_yields_exactly_one_reopen() {
    let mut io = session_io(test_ordered_stream());

    io.schedule_demand(KIND, retry_later());
    tokio::time::advance(Duration::from_secs(60)).await;
    let (kind, ()) = io
        .waits
        .next()
        .await
        .expect("reactor deadline keeps a pending session wait");

    assert_eq!(kind, KIND);
    assert!(io.on_wait_fired(kind, false).is_some());
    assert!(
        io.waits.is_empty(),
        "one deadline yields one reopen attempt"
    );
}

#[test]
fn ordered_session_connection_teardown_drops_pending_waits() {
    let mut io = session_io(test_ordered_stream());
    let (change_tx, mut change_rx) = watch::channel(());

    io.schedule_demand(
        KIND,
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
#[allow(
    clippy::print_stdout,
    reason = "witnesses print applicability counters"
)]
fn fixed_seeds_exercise_every_session_rule() {
    let mut total = std::collections::BTreeMap::new();
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
        for (rule, count) in replay_rules(&replay_service_session_bytes(&bytes)) {
            *total.entry(rule).or_insert(0) += count;
        }
    }
    println!("tier=Machine target=service_session decoder=1 {total:?}");
    for (rule, count) in &total {
        assert!(*count > 0, "no fixed seed exercised {rule}: {total:?}");
    }
}

/// Every rule a replay counts, by name. The destructure makes a new counter a
/// compile error here.
fn replay_rules(summary: &ServiceSessionReplaySummary) -> [(&'static str, usize); 11] {
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
    } = *summary;
    [
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
    ]
}

/// Attempts count every local open, and only the exit of the exact generation
/// is current.
#[test]
fn attempts_name_local_generations_and_stale_exits_keep_them() {
    let roles = ConnectionRoles {
        is_initiator: true,
        i_open_collision_winner: true,
    };
    let mut session = ServiceSessionState::new(MEMBER_A, REOPEN, roles);
    let exit = |session_id, opened_locally| SessionExit {
        stream: MEMBER_A,
        session_id,
        opened_locally,
    };

    assert_eq!(session.begin_attempt(), 1);
    assert_eq!(session.begin_attempt(), 2, "a failed open keeps its number");
    session.opened(40);

    for stale in [exit(39, true), exit(40, false)] {
        let outcome = session.exited(stale, false);
        assert!(!outcome.current);
        assert_eq!(outcome.reopen_after, None);
        assert_eq!(session.local_session_id, Some(40));
    }

    let outcome = session.exited(exit(40, true), false);
    assert!(outcome.current);
    assert!(outcome.reopen_after.is_some());
    assert_eq!(session.begin_attempt(), 3, "reopens keep counting");
}
