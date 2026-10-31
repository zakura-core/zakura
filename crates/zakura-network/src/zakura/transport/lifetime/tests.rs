//! Frame gates, grants, and owner counts, with no transport.

use futures::FutureExt as _;
use proptest::prelude::*;

use super::*;

fn gate(connection: u64, node: u64) -> (FrameGate, OutputByteBudget, OutputByteBudget) {
    let connection = OutputByteBudget::new(connection).unwrap();
    let node = OutputByteBudget::new(node).unwrap();
    (
        FrameGate::new(connection.clone(), node.clone()),
        connection,
        node,
    )
}

#[test]
fn a_grant_charges_both_budgets_until_it_drops() {
    let (gate, connection, node) = gate(100, 200);
    let grant = gate.try_admit(40).unwrap();
    assert_eq!((connection.granted(), node.granted()), (40, 40));
    drop(grant);
    assert_eq!((connection.granted(), node.granted()), (0, 0));
}

#[test]
fn a_saturated_stream_still_has_one_floor_frame() {
    let (gate, connection, _node) = gate(10, 200);
    let shared = gate.try_admit(10).unwrap();
    let floor = gate.try_admit(10).expect("the floor slot admits one frame");
    assert_eq!(
        connection.granted(),
        10,
        "the floor charges no shared bytes"
    );
    assert!(gate.try_admit(10).is_none());
    drop(floor);
    assert!(gate.try_admit(10).is_some(), "the floor slot returned");
    drop(shared);
}

#[test]
fn one_streams_retained_frames_never_use_another_streams_floor() {
    let connection = OutputByteBudget::new(10).unwrap();
    let node = OutputByteBudget::new(100).unwrap();
    let busy = FrameGate::new(connection.clone(), node.clone());
    let idle = FrameGate::new(connection, node);
    let _held = (busy.try_admit(10).unwrap(), busy.try_admit(10).unwrap());
    assert!(busy.try_admit(10).is_none());
    assert!(idle.try_admit(10).is_some());
}

#[test]
fn a_frame_above_a_budget_clamps_to_it_instead_of_waiting_forever() {
    let (gate, connection, node) = gate(10, 20);
    let grant = gate.try_admit(1_000).unwrap();
    assert_eq!((connection.granted(), node.granted()), (10, 20));
    drop(grant);
}

/// A failed node grant returns the connection's bytes at once.
#[test]
fn a_refused_shared_admission_keeps_nothing() {
    let (gate, connection, node) = gate(100, 10);
    let _node = node.hold_free().unwrap();
    let _floor = gate.try_admit(10).expect("the floor slot admits one frame");
    assert!(gate.try_admit(10).is_none());
    assert_eq!(connection.granted(), 0);
}

#[tokio::test]
async fn a_cancelled_wait_takes_nothing_and_a_release_wakes_the_next_wait() {
    let (gate, connection, _node) = gate(10, 100);
    let first = gate.try_admit(10).unwrap();
    let floor = gate.try_admit(10).unwrap();

    let mut cancelled = Box::pin(gate.admit(10));
    assert!(futures::poll!(&mut cancelled).is_pending());
    drop(cancelled);
    assert_eq!(connection.granted(), 10);

    let mut waiting = Box::pin(gate.admit(10));
    assert!(futures::poll!(&mut waiting).is_pending());
    drop(floor);
    let grant = waiting.await;
    assert_eq!(
        connection.granted(),
        10,
        "the wait took the freed floor slot"
    );
    drop((first, grant));
    assert_eq!(connection.granted(), 0);
}

#[tokio::test]
async fn an_unpolled_admission_future_holds_nothing() {
    let (gate, connection, node) = gate(10, 100);
    let unpolled = gate.admit(10);
    assert_eq!((connection.granted(), node.granted()), (0, 0));
    drop(unpolled);
    assert_eq!((connection.granted(), node.granted()), (0, 0));
    assert!(gate.admit(10).now_or_never().is_some());
}

#[test]
fn owners_move_between_phases_and_leave_on_drop() {
    let ledger = OwnerLedger::default();
    let setup = ledger.enter(OwnerPhase::Setup);
    let handshake = ledger.enter(OwnerPhase::Handshake);
    assert_eq!(
        ledger.counts(),
        OwnerCounts {
            handshake: 1,
            setup: 1,
            installed: 0,
            retiring: 0,
        }
    );
    setup.advance(OwnerPhase::Installed);
    drop(handshake);
    setup.advance(OwnerPhase::Retiring);
    assert_eq!(
        ledger.counts(),
        OwnerCounts {
            retiring: 1,
            ..OwnerCounts::default()
        }
    );
    drop(setup);
    assert_eq!(ledger.counts(), OwnerCounts::default());
}

#[test]
fn stream_resources_count_from_setup_and_share_the_connection_budget() {
    let node = NodeResources::new(ResourceEnvelope::new(10, 10, 4));
    let connection = node.connection();
    let (first, second) = (connection.stream(), connection.stream());
    assert_eq!(node.owners().counts().setup, 2);
    let _charge = first.raw.try_admit(20).unwrap();
    assert_eq!(connection.raw.granted(), 20);
    assert!(
        node.connection().stream().raw.try_admit(20).is_some(),
        "another connection has its own budget"
    );
    // The second stream's shared admission finds the connection's bytes gone,
    // so it takes its own floor slot, and its next admission must wait.
    let _floor = second.raw.try_admit(20).unwrap();
    assert!(second.raw.try_admit(20).is_none());
    drop((first, second, connection));
    assert_eq!(node.owners().counts(), OwnerCounts::default());
}

#[test]
fn the_envelope_doubles_each_window_and_stays_within_the_node_budget() {
    let envelope = ResourceEnvelope::new(32 << 20, 32 << 20, 100);
    assert_eq!(envelope.connection_raw_bytes, 64 << 20);
    assert_eq!(envelope.connection_output_bytes, 64 << 20);
    assert_eq!(envelope.node_raw_bytes, sizing::node_output_bytes());
    assert_eq!(envelope.request_workers, 200);
    // A window larger than the node default raises the node bound with it, so a
    // grant that clamps to its connection's budget can also finish at the node.
    let large = ResourceEnvelope::new(1 << 40, 1 << 40, 1);
    assert_eq!(large.node_raw_bytes, large.connection_raw_bytes);
    assert_eq!(
        envelope.retained_bytes_bound(100, 16, 1 << 20),
        u128::from(envelope.node_raw_bytes)
            + u128::from(envelope.node_output_bytes)
            + u128::from(envelope.compat_response_bytes)
            + 200 * COMPAT_RESPONSE_MAX_AGGREGATE_BYTES as u128
            + 2 * 100 * 16 * (1 << 20),
    );
}

/// A saturated stream's floor funds one frame. A multi-frame result waits for the
/// shared budgets, and a one-frame result still finishes through the floor.
#[test]
fn only_a_one_frame_result_uses_the_floor() {
    let (gate, connection, _node) = gate(10, 200);
    let saturating = gate.try_admit(10).unwrap();
    assert!(
        gate.admit_result(2, 10).now_or_never().is_none(),
        "the floor does not fund a multi-frame result"
    );
    let floor = gate
        .admit_result(1, 10)
        .now_or_never()
        .expect("the floor funds one frame");
    assert_eq!(connection.granted(), 10, "the floor charges no shared bytes");
    drop((floor, saturating));
    let shared = gate
        .admit_result(2, 10)
        .now_or_never()
        .expect("released shared bytes fund the result");
    assert_eq!(connection.granted(), 10);
    drop(shared);
    assert_eq!(connection.granted(), 0);
}

/// The compatibility partition never charges or waits for native raw or output
/// bytes, and a waiter that gives up takes nothing.
#[test]
fn compat_exchanges_use_their_own_partition() {
    let node = NodeResources::new(ResourceEnvelope::new(10, 10, 4));
    let connection = node.connection();
    let partition = connection.compat_responses().clone();
    let capacity = ResourceEnvelope::new(10, 10, 4).compat_response_bytes;

    // Native budgets of ten bytes do not limit a whole exchange.
    let held = connection
        .admit_compat_response(capacity)
        .now_or_never()
        .expect("an idle partition funds one exchange");
    assert_eq!(partition.granted(), capacity);
    assert_eq!(node.owners().counts().installed, 1);

    assert!(connection
        .admit_compat_response(1)
        .now_or_never()
        .is_none());
    assert_eq!(partition.granted(), capacity, "the waiter took nothing");

    drop(held);
    assert_eq!(partition.granted(), 0);
    assert_eq!(node.owners().counts().total(), 0);

    // A request larger than the partition clamps to it, so it can still finish.
    let clamped = connection
        .admit_compat_response(u64::MAX)
        .now_or_never()
        .expect("the grant clamps to the partition");
    assert_eq!(partition.granted(), capacity);
    drop(clamped);
}

/// The partition funds at least one worst-case exchange on every configuration.
#[test]
fn the_compat_partition_funds_a_worst_case_exchange() {
    for (window, connections) in [(1, 1), (32 << 20, 100), (1 << 40, 1 << 16)] {
        let envelope = ResourceEnvelope::new(window, window, connections);
        assert!(envelope.compat_response_bytes >= COMPAT_EXCHANGE_MAX_BYTES);
    }
}

#[derive(Debug, Clone)]
enum Op {
    Admit(u8),
    Release(u8),
}

proptest! {
    /// However admissions and releases interleave, the budgets match an
    /// independent ledger of the live grants, and at most one grant is on the
    /// floor. Releasing every grant empties both budgets.
    #[test]
    fn grants_match_an_independent_ledger(
        ops in proptest::collection::vec(
            prop_oneof![
                (1u8..40).prop_map(Op::Admit),
                any::<u8>().prop_map(Op::Release),
            ],
            0..64,
        ),
    ) {
        let (gate, connection, node) = gate(64, 1_000);
        let mut live: Vec<(FrameGrant, Option<u64>)> = Vec::new();
        for op in ops {
            match op {
                Op::Admit(bytes) => {
                    let bytes = u64::from(bytes);
                    let shared: u64 = live.iter().filter_map(|(_, bytes)| *bytes).sum();
                    let floored = live.iter().any(|(_, bytes)| bytes.is_none());
                    let expected = if shared + bytes <= 64 {
                        Some(Some(bytes))
                    } else if !floored {
                        Some(None)
                    } else {
                        None
                    };
                    match (gate.try_admit(bytes), expected) {
                        (Some(grant), Some(charged)) => live.push((grant, charged)),
                        (None, None) => {}
                        (got, expected) => {
                            prop_assert!(false, "got {got:?}, expected {expected:?}");
                        }
                    }
                }
                Op::Release(index) => {
                    if !live.is_empty() {
                        drop(live.swap_remove(usize::from(index) % live.len()));
                    }
                }
            }
            let shared: u64 = live.iter().filter_map(|(_, bytes)| *bytes).sum();
            prop_assert_eq!(connection.granted(), shared);
            prop_assert_eq!(node.granted(), shared);
        }
        live.clear();
        prop_assert_eq!(connection.granted(), 0);
        prop_assert_eq!(node.granted(), 0);
    }
}
