use super::*;
use crate::zakura::machine_test::machine_target_tests;

/// The collision winner's history: an adopted offer, an exit that backs
/// off, a racing offer that parks against a local open, a stale exit, an
/// invalid member, a conflict, a deferred member, an expiry and a
/// retirement.
const WITNESS: &[u8] = &[
    // Either-side, reopening; this endpoint dialed and wins collisions.
    0b1111, //
    5, 0, 1, 1, // offer 1 completes and is adopted
    14, 0, // its exit starts a backoff
    5, 0, 1, 0, // offer 1 races a local open and parks
    14, 2, // the parked generation's exit is stale
    14, 1, // the local exit starts a second backoff
    0, 9, 0, 0, 0, // a foreign member is invalid
    0, 0, 0, 0, 0, // member A of offer 1 waits
    0, 1, 1, 0, 0, // member B of offer 2 conflicts
    0, 0, 0, 0, 0, // member A is deferred
    8, 149, // the deferral ends
    0, 0, 0, 0, 0, // member A of offer 1 waits
    8, 100, // and expires
    9, 3, // the service retires
];

machine_target_tests! {
    replay: replay_service_session_bytes,
    target: "service_session",
    decoder: 1,
    witness: WITNESS => ServiceSessionReplaySummary {
        completed_offers: 2,
        invalid_members: 1,
        conflicting_members: 1,
        expired_offers: 1,
        deferred_members: 1,
        parked_collisions: 1,
        adopted_collisions: 0,
        local_opens: 1,
        stale_exits: 1,
        reopen_backoffs: 2,
        retirements: 1,
    },
}

/// The collision loser adopts the peer's offer beside its own generation.
#[test]
fn the_losing_witness_adopts_a_collision() {
    // Initiator-only, reopening; this endpoint dialed and loses collisions.
    let witness = [
        0b0110, //
        9, 0, // demand opens now
        10, 1, 0, // the wait fires and a local open succeeds
        5, 0, 0, 1, // offer 1 completes beside it
        14, 0, // the local exit leaves the adopted generation
    ];
    assert_eq!(
        replay_service_session_bytes(&witness),
        ServiceSessionReplaySummary {
            completed_offers: 1,
            adopted_collisions: 1,
            local_opens: 1,
            ..Default::default()
        }
    );
}
