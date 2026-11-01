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
    let node = NodeResources::new(ResourceEnvelope::new(TransportLimits::with_windows(10, 4)));
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

const MIB: u32 = 1 << 20;

#[test]
fn the_envelope_derives_each_bound_from_windows_connections_and_streams() {
    let envelope = ResourceEnvelope::new(TransportLimits {
        receive_window: 32 * MIB,
        send_window: u64::from(16 * MIB),
        max_connections: 256,
        max_pending_handshakes: 32,
        max_open_streams: 1024,
        max_frame_bytes: MIB,
    });
    assert_eq!(envelope.connection_raw_bytes, 64 << 20);
    assert_eq!(envelope.connection_output_bytes, 32 << 20);
    assert_eq!(envelope.node_raw_bytes, 256 * (64 << 20));
    assert_eq!(envelope.node_output_bytes, 256 * (32 << 20));
    assert_eq!(envelope.request_executions, 256 * 1024);
    assert_eq!(envelope.floor_frame_bytes, 2 * 256 * 1024 * (1 << 20));
    assert_eq!(envelope.transport_credit_bytes, 288 * (48 << 20));
    assert_eq!(
        envelope.transport_memory_bytes(),
        u128::from(envelope.node_raw_bytes)
            + u128::from(envelope.node_output_bytes)
            + envelope.floor_frame_bytes
            + envelope.transport_credit_bytes,
    );
}

/// The peer memory budget funds exactly the credit that the transport can hold
/// at its default windows, and raising a frame budget takes nothing from it.
#[test]
fn the_peer_memory_budget_funds_credit_and_no_frame_budget() {
    let limits = TransportLimits {
        receive_window: crate::zakura::DEFAULT_ZAKURA_RECEIVE_WINDOW,
        send_window: crate::zakura::DEFAULT_ZAKURA_SEND_WINDOW,
        max_connections: crate::zakura::DEFAULT_ZAKURA_MAX_CONNECTIONS,
        max_pending_handshakes: crate::zakura::DEFAULT_ZAKURA_MAX_PENDING_HANDSHAKES,
        max_open_streams: 1024,
        max_frame_bytes: MIB,
    };
    let envelope = ResourceEnvelope::new(limits);
    let budget = u128::from(crate::zakura::DEFAULT_ZAKURA_PEER_MEMORY_BUDGET_BYTES);
    assert!(envelope.transport_credit_bytes <= budget);
    assert!(
        budget - envelope.transport_credit_bytes
            < u128::from(zakura_quic::connection_memory_bytes(
                limits.receive_window,
                limits.send_window,
            )) + u128::from(zakura_quic::SOCKET_DATAGRAM_QUEUE_BYTES),
        "the budget funds every connection it can",
    );
    let more_frames = ResourceEnvelope::new(TransportLimits {
        max_frame_bytes: 2 * MIB,
        ..limits
    });
    assert_eq!(
        more_frames.transport_credit_bytes,
        envelope.transport_credit_bytes
    );
}

/// Every connection fills both budgets at once. None of it reaches a floor slot
/// or waits, so no connection's traffic can pause another's.
#[test]
fn every_connection_can_fill_its_budgets_at_once() {
    const CONNECTIONS: usize = 256;
    let window = 32 * MIB;
    let envelope = ResourceEnvelope::new(TransportLimits::with_windows(window, CONNECTIONS));
    let node = NodeResources::new(envelope);
    let full = 2 * u64::from(window);
    let streams: Vec<_> = (0..CONNECTIONS)
        .map(|_| node.connection().stream())
        .collect();
    let grants: Vec<_> = streams
        .iter()
        .map(|stream| {
            (
                stream.raw.try_admit(full).unwrap(),
                stream.output.try_admit(full).unwrap(),
            )
        })
        .collect();
    assert_eq!(node.raw.granted(), CONNECTIONS as u64 * full);
    assert_eq!(node.output.granted(), CONNECTIONS as u64 * full);
    assert_eq!(node.raw.granted(), envelope.node_raw_bytes);
    drop(grants);
    assert_eq!(node.raw.granted(), 0);
}

/// Many connections move several full windows of frames through consumers that
/// lag one window behind. The node never pauses any of their readers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_conformant_connections_at_full_window_make_progress() {
    const CONNECTIONS: usize = 256;
    const FRAME_BYTES: u64 = 1 << 20;
    let window = 32 * MIB;
    let frames_per_window = u64::from(window) / FRAME_BYTES;
    let rounds = 4;
    let node = NodeResources::new(ResourceEnvelope::new(TransportLimits::with_windows(
        window,
        CONNECTIONS,
    )));

    // Every connection holds a full window before any consumer starts.
    let all_full = Arc::new(tokio::sync::Barrier::new(CONNECTIONS));
    let mut connections = Vec::new();
    for _ in 0..CONNECTIONS {
        let stream = node.connection().stream();
        let all_full = all_full.clone();
        connections.push(tokio::spawn(async move {
            let mut retained = std::collections::VecDeque::new();
            let mut waited = Duration::ZERO;
            for frame in 1..=rounds * frames_per_window {
                let grant = stream.raw.admit(FRAME_BYTES).await;
                waited += grant.waited();
                retained.push_back(grant);
                if frame == frames_per_window {
                    all_full.wait().await;
                }
                // The consumer lags one window behind this reader.
                // Widening usize to u64 is lossless on supported targets.
                if retained.len() as u64 > frames_per_window {
                    retained.pop_front();
                }
            }
            waited
        }));
    }
    let progress = async {
        for connection in connections {
            assert_eq!(connection.await.unwrap(), Duration::ZERO);
        }
    };
    tokio::time::timeout(Duration::from_secs(10), progress)
        .await
        .expect("no reader waits, so every connection finishes");
    assert_eq!(node.raw.granted(), 0);
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
