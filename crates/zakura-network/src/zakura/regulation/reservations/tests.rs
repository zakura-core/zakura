//! Reservation suite: run once, shared by every requester that uses the map.

use std::collections::HashSet;

use proptest::prelude::*;

use super::*;

const CAP: usize = 4;

fn reservations() -> Reservations<u64, &'static str> {
    Reservations::new(CAP)
}

#[test]
fn an_unsolicited_response_is_refused() {
    let mut map = reservations();
    assert_eq!(map.claim(&1, 0), Err(ClaimRefused::Unsolicited));
}

#[test]
fn a_duplicate_response_is_refused() {
    let mut map = reservations();
    map.reserve(1, 10, "first").expect("an empty map has room");
    assert_eq!(map.claim(&1, 10), Ok("first"));
    assert_eq!(map.claim(&1, 10), Err(ClaimRefused::Unsolicited));
}

#[test]
fn a_response_above_its_reservation_is_refused() {
    let mut map = reservations();
    map.reserve(1, 10, "credit").expect("an empty map has room");
    assert_eq!(
        map.claim(&1, 11),
        Err(ClaimRefused::OverReservation {
            reserved: 10,
            actual: 11
        })
    );
}

#[tokio::test(start_paused = true)]
async fn time_alone_never_removes_a_reservation() {
    let mut map = reservations();
    map.reserve(1, 10, "slow").expect("an empty map has room");
    tokio::time::advance(std::time::Duration::from_secs(24 * 60 * 60)).await;
    assert_eq!(map.claim(&1, 10), Ok("slow"));
}

#[test]
fn a_full_map_refuses_locally_and_recovers_after_a_claim() {
    let mut map = reservations();
    for key in 0..4u64 {
        map.reserve(key, 1, "live")
            .expect("the map has room below its cap");
    }
    assert_eq!(map.reserve(99, 1, "extra"), Err(ReserveRefused::AtCapacity));
    assert_eq!(map.reserve(0, 1, "again"), Err(ReserveRefused::KeyLive));
    map.claim(&0, 1).expect("the reservation is live");
    map.reserve(99, 1, "extra").expect("a claim frees one slot");
}

#[test]
fn retract_frees_an_unsent_request_and_nothing_else() {
    let mut map = reservations();
    map.reserve(1, 1, "unsent").expect("an empty map has room");
    assert_eq!(map.retract(&1), Some("unsent"));
    assert_eq!(map.retract(&1), None);
    assert_eq!(map.claim(&1, 1), Err(ClaimRefused::Unsolicited));
}

#[test]
fn a_new_session_starts_without_reservations() {
    let mut map = reservations();
    map.reserve(1, 1, "old session")
        .expect("an empty map has room");
    drop(map);
    let mut next_session = reservations();
    assert_eq!(next_session.claim(&1, 1), Err(ClaimRefused::Unsolicited));
}

#[derive(Clone, Debug)]
enum Op {
    Reserve(u8),
    Retract(u8),
    Claim(u8, bool),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        (0..8u8).prop_map(Op::Reserve),
        (0..8u8).prop_map(Op::Retract),
        (0..8u8, any::<bool>()).prop_map(|(key, oversized)| Op::Claim(key, oversized)),
    ]
}

proptest! {
    /// A ghost set of live keys predicts every result; the map never exceeds its cap.
    #[test]
    fn operations_agree_with_a_ghost_key_set(ops in proptest::collection::vec(op(), 0..64)) {
        let mut map = Reservations::<u8, u8>::new(CAP);
        let mut ghost = HashSet::new();
        for op in ops {
            match op {
                Op::Reserve(key) => {
                    let expected = if ghost.contains(&key) {
                        Err(ReserveRefused::KeyLive)
                    } else if ghost.len() >= CAP {
                        Err(ReserveRefused::AtCapacity)
                    } else {
                        ghost.insert(key);
                        Ok(())
                    };
                    prop_assert_eq!(map.reserve(key, 10, key), expected);
                }
                Op::Retract(key) => {
                    let expected = ghost.remove(&key).then_some(key);
                    prop_assert_eq!(map.retract(&key), expected);
                }
                Op::Claim(key, oversized) => {
                    let live = ghost.remove(&key);
                    let result = map.claim(&key, if oversized { 11 } else { 10 });
                    match (live, oversized) {
                        (false, _) => prop_assert_eq!(result, Err(ClaimRefused::Unsolicited)),
                        (true, false) => prop_assert_eq!(result, Ok(key)),
                        (true, true) => {
                            let over = matches!(result, Err(ClaimRefused::OverReservation { .. }));
                            prop_assert!(over);
                        }
                    }
                }
            }
            prop_assert_eq!(map.len(), ghost.len());
            prop_assert!(map.len() <= CAP);
        }
    }
}
