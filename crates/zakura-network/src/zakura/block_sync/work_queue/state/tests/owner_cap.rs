//! The per-session owner cap over [`WorkQueueState`].
//!
//! The proptest drives legal take, admission, publication and settlement
//! sequences. It reads each session's charge from the item maps, never from
//! the state's tally, and checks after every step that the tally matches it
//! and that no session holds more than the cap's floor total.

use super::*;

/// The cap the property admits against: above-floor requests may bring a
/// session to 600 bytes, and the floor overdraft adds 300 more.
const CAP: OwnerReservationCap = OwnerReservationCap {
    above_floor: 600,
    floor: 900,
};

/// Sessions the generated inputs use.
const SESSIONS: u64 = 3;

#[derive(Clone, Debug)]
enum CapOp {
    /// Queue `low..=low + len` at `size` bytes each.
    Extend { low: u32, len: u32, size: u64 },
    /// Take up to `count` heights from `low` for `session`, admit them at
    /// floor or above-floor priority, and publish or return them.
    Request {
        session: u64,
        low: u32,
        count: usize,
        floor: bool,
    },
    /// The `index`th reserved height of `session` arrives.
    Receive { session: u64, index: usize },
    /// The `index`th request of `session` returns its unreceived heights.
    Return { session: u64, index: usize },
    /// The download floor rises to `floor`.
    Advance { floor: u32 },
    /// The frontier resets above `floor`.
    Reset { floor: u32 },
}

fn cap_op() -> impl Strategy<Value = CapOp> {
    let session = 0..SESSIONS;
    prop_oneof![
        1 => (1..=MAX_HEIGHT, 0..6u32, 1..=300u64)
            .prop_map(|(low, len, size)| CapOp::Extend { low, len, size }),
        3 => (session.clone(), 1..=MAX_HEIGHT, 1..4usize, any::<bool>()).prop_map(
            |(session, low, count, floor)| CapOp::Request {
                session,
                low,
                count,
                floor,
            }
        ),
        2 => (session.clone(), any::<usize>())
            .prop_map(|(session, index)| CapOp::Receive { session, index }),
        1 => (session, any::<usize>())
            .prop_map(|(session, index)| CapOp::Return { session, index }),
        1 => (0..=MAX_HEIGHT).prop_map(|floor| CapOp::Advance { floor }),
        1 => (0..=MAX_HEIGHT).prop_map(|floor| CapOp::Reset { floor }),
    ]
}

/// Each session's reserved bytes, read from the item maps.
fn scanned(state: &WorkQueueState) -> BTreeMap<u64, u64> {
    let mut sessions = BTreeMap::new();
    for item in state.ownership() {
        let Some(owner) = item.owner else {
            continue;
        };
        if item.reserved_bytes > 0 {
            *sessions.entry(owner.session_id).or_insert(0) += item.reserved_bytes;
        }
    }
    sessions
}

/// The in-flight reserved heights of `session`, with their owners.
fn reserved_heights(state: &WorkQueueState, session: u64) -> Vec<(block::Height, BodyWorkOwner)> {
    state
        .ownership()
        .into_iter()
        .filter(|item| item.reserved_bytes > 0)
        .filter_map(|item| Some((item.height, item.owner?)))
        .filter(|(_, owner)| owner.session_id == session)
        .collect()
}

fn check_tally(state: &WorkQueueState) {
    let scanned = scanned(state);
    for session in 0..SESSIONS {
        let held = scanned.get(&session).copied().unwrap_or(0);
        assert_eq!(
            state.session_reserved(session),
            held,
            "session {session}'s tally matches its items"
        );
        assert!(
            held <= CAP.floor,
            "session {session} holds {held} bytes, past the floor total {}",
            CAP.floor
        );
    }
    assert_eq!(state.ledger_fault(), None);
}

/// Run one op. `next_request` numbers each session's requests.
fn apply_cap_op(state: &mut WorkQueueState, next_request: &mut u64, op: CapOp) {
    match op {
        CapOp::Extend { low, len, size } => {
            let high = (low + len).min(MAX_HEIGHT);
            let heights: Vec<_> = (low..=high).map(|raw| (raw, size)).collect();
            extend(state, authority(0), &heights);
        }
        CapOp::Request {
            session,
            low,
            count,
            floor,
        } => {
            *next_request += 1;
            let request = NonZeroU64::new(*next_request).expect("request ids start at one");
            let items = state.take_budgeted(
                height(low),
                height(MAX_HEIGHT),
                count,
                u64::MAX,
                (session, request),
            );
            if items.is_empty() {
                return;
            }
            let bytes: u64 = items.iter().map(|(_, item)| item.estimated_bytes).sum();
            let priority = if floor {
                RequestPriority::Floor
            } else {
                RequestPriority::AboveFloor
            };
            let held = state.session_reserved(session);
            let limit = if floor { CAP.floor } else { CAP.above_floor };
            let admitted = CAP.admits(held, bytes, priority);
            assert_eq!(
                admitted,
                held + bytes <= limit,
                "admission follows the limit"
            );
            if floor && !admitted {
                assert!(
                    !CAP.admits(held, bytes, RequestPriority::AboveFloor),
                    "the floor overdraft admits everything an above-floor request may"
                );
            }
            if admitted {
                assert_eq!(publish(state, &items), bytes);
                assert_eq!(state.session_reserved(session), held + bytes);
            } else {
                state.return_unpublished(&items);
                assert_eq!(state.session_reserved(session), held);
            }
        }
        CapOp::Receive { session, index } => {
            let heights = reserved_heights(state, session);
            if heights.is_empty() {
                return;
            }
            let (height, owner) = heights[index % heights.len()];
            let held = state.session_reserved(session);
            let released = state
                .end_reservation_at_receipt(owner, height, false)
                .expect("the owner holds a reservation on its height");
            assert_eq!(state.session_reserved(session), held - released);
        }
        CapOp::Return { session, index } => {
            let heights = reserved_heights(state, session);
            if heights.is_empty() {
                return;
            }
            let owner = heights[index % heights.len()].1;
            let owned = heights
                .iter()
                .filter(|(_, other)| *other == owner)
                .map(|(height, _)| *height)
                .collect();
            let (_, _) = state.return_owned(owner, owned);
        }
        CapOp::Advance { floor } => {
            let _ = state.advance_floor(height(floor));
        }
        CapOp::Reset { floor } => {
            let _ = state.reset_above(height(floor));
        }
    }
}

proptest! {
    /// Each session's charge matches its items after every step and never
    /// passes the cap's floor total, and the floor overdraft admits above
    /// the above-floor total only up to the floor total.
    #[test]
    fn owner_cap_bounds_each_sessions_charge(ops in prop::collection::vec(cap_op(), 1..120)) {
        let mut state = new_state();
        let mut next_request = 0;
        for op in ops {
            apply_cap_op(&mut state, &mut next_request, op);
            check_tally(&state);
        }
    }
}

#[test]
fn floor_overdraft_admits_past_the_above_floor_total_only() {
    let mut state = new_state();
    extend(
        &mut state,
        authority(0),
        &[(1, 300), (2, 300), (3, 300), (4, 300)],
    );
    let mut next_request = 0;
    for (low, floor) in [(1, false), (2, false), (3, false)] {
        apply_cap_op(
            &mut state,
            &mut next_request,
            CapOp::Request {
                session: 0,
                low,
                count: 1,
                floor,
            },
        );
    }
    // Two above-floor requests reach 600; the third waits.
    assert_eq!(state.session_reserved(0), 600);
    assert!(state.pending_contains(height(3)));
    // A floor request may overdraw to 900, and no further.
    apply_cap_op(
        &mut state,
        &mut next_request,
        CapOp::Request {
            session: 0,
            low: 3,
            count: 1,
            floor: true,
        },
    );
    assert_eq!(state.session_reserved(0), 900);
    apply_cap_op(
        &mut state,
        &mut next_request,
        CapOp::Request {
            session: 0,
            low: 4,
            count: 1,
            floor: true,
        },
    );
    assert_eq!(state.session_reserved(0), 900);
    assert!(state.pending_contains(height(4)));
    // Another session's cap is separate.
    apply_cap_op(
        &mut state,
        &mut next_request,
        CapOp::Request {
            session: 1,
            low: 4,
            count: 1,
            floor: false,
        },
    );
    assert_eq!(state.session_reserved(1), 300);
    check_tally(&state);
    // Settlement empties the tally.
    let _ = state.reset_above(height(0));
    assert_eq!(state.session_reserved(0), 0);
    assert_eq!(state.session_reserved(1), 0);
}
