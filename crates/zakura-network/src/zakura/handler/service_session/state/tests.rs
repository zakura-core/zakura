//! Pending service-session assembly: deadlines, conflicts, expiry, and
//! completion.

use std::sync::Arc;

use super::*;

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
