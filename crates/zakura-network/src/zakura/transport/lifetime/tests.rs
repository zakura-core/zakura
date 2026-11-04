//! Frame gates, grants, and owner counts, with no transport.

use futures::FutureExt as _;
use proptest::prelude::*;

use super::*;

const MIB: u64 = 1 << 20;

fn gate(capacity: u64) -> (FrameGate, OutputByteBudget) {
    let budget = OutputByteBudget::new(capacity).unwrap();
    (FrameGate::over(budget.clone()), budget)
}

/// Two kinds on separate capability bits, each with its own share.
const HEADERS: Stream = Stream {
    kind: 5,
    capability: 1 << 5,
    frame_cap: 20,
    ..Stream::PERSISTENT
};
const BLOCKS: Stream = Stream {
    kind: 6,
    capability: 1 << 6,
    frame_cap: 20,
    ..Stream::PERSISTENT
};

fn shares() -> ReactorShares {
    ReactorShares {
        control_receive_bytes: 100,
        kinds: vec![(
            HEADERS.kind,
            ReactorBudget {
                receive: 20,
                output: 1024,
            },
        )],
        other: ReactorBudget {
            receive: 30,
            output: 2048,
        },
    }
}

fn node() -> NodeResources {
    NodeResources::new(shares(), [HEADERS, BLOCKS], 4096)
}

#[test]
fn a_frame_above_its_gate_takes_the_whole_gate_instead_of_waiting_forever() {
    let (gate, budget) = gate(10);
    let grant = gate.try_admit(1_000).unwrap();
    assert_eq!(budget.granted(), 10);
    assert!(gate.try_admit(1).is_none());
    drop(grant);
    assert_eq!(budget.granted(), 0);
}

#[tokio::test]
async fn a_cancelled_wait_takes_nothing_and_a_release_wakes_the_next_wait() {
    let (gate, budget) = gate(10);
    let first = gate.try_admit(10).unwrap();

    let mut cancelled = Box::pin(gate.admit(10));
    assert!(futures::poll!(&mut cancelled).is_pending());
    drop(cancelled);
    assert_eq!(budget.granted(), 10);

    let mut waiting = Box::pin(gate.admit(10));
    assert!(futures::poll!(&mut waiting).is_pending());
    drop(first);
    let grant = waiting.await;
    assert_eq!(budget.granted(), 10, "the wait took the freed bytes");
    assert!(grant.waited() > Duration::ZERO);
    drop(grant);
    assert_eq!(budget.granted(), 0);
}

#[tokio::test]
async fn an_unpolled_admission_future_holds_nothing() {
    let (gate, budget) = gate(10);
    let unpolled = gate.admit(10);
    assert_eq!(budget.granted(), 0);
    drop(unpolled);
    assert_eq!(budget.granted(), 0);
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

/// Streams of one kind share its gates on their connection. A full kind
/// leaves the connection's other kinds and other connections untouched.
#[test]
fn a_full_kind_leaves_other_kinds_and_connections_free() {
    let node = node();
    let connection = node.connection(HEADERS.capability | BLOCKS.capability);
    let first = connection.stream(HEADERS.kind, connection.owner()).unwrap();
    let second = connection.stream(HEADERS.kind, connection.owner()).unwrap();
    let blocks = connection.stream(BLOCKS.kind, connection.owner()).unwrap();
    assert_eq!(node.owners().counts().setup, 3);

    let _full = first.raw.try_admit(20).unwrap();
    assert!(second.raw.try_admit(1).is_none(), "one kind, one gate");
    assert!(blocks.raw.try_admit(30).is_some(), "the block gate is free");
    let other = node.connection(HEADERS.capability);
    let other = other.stream(HEADERS.kind, other.owner()).unwrap();
    assert!(
        other.raw.try_admit(20).is_some(),
        "another connection's gate"
    );

    drop((first, second, blocks, other));
    assert_eq!(node.owners().counts(), OwnerCounts::default());
}

/// A connection gates only the kinds it negotiated, and its receive window is
/// the handshake share plus each of those kinds' receive shares.
#[test]
fn a_connection_window_sums_its_negotiated_kinds() {
    let node = node();
    let headers_only = node.connection(HEADERS.capability);
    assert_eq!(headers_only.receive_window(), 100 + 20);
    assert!(headers_only
        .stream(BLOCKS.kind, headers_only.owner())
        .is_none());
    assert_eq!(
        node.connection(HEADERS.capability | BLOCKS.capability)
            .receive_window(),
        100 + 20 + 30
    );
    assert_eq!(node.connection(0).receive_window(), 100);
    assert_eq!(shares().stream_receive_window(), 30);
}

/// The default shares keep a node's QUIC windows: a handshake gets the
/// connection window, and one stream can use its kind's whole share.
#[test]
fn quic_shares_keep_the_configured_windows() {
    let quic = QuicConfig::default();
    let shares = ReactorShares::from_quic(&quic);
    assert_eq!(
        shares.control_receive_bytes,
        u64::from(quic.receive_window_bytes)
    );
    assert_eq!(
        shares.stream_receive_window(),
        u64::from(quic.stream_receive_window_bytes)
    );
    assert_eq!(shares.share(7).output, DEFAULT_REACTOR_OUTPUT_BYTES);
    assert_eq!(
        shares.receive_window([5, 6]),
        u64::from(quic.receive_window_bytes) + 2 * u64::from(quic.stream_receive_window_bytes)
    );
}

#[test]
#[should_panic(expected = "output share of 19 bytes must hold its largest frame")]
fn an_output_share_below_its_largest_frame_is_refused() {
    let small = ReactorShares {
        other: ReactorBudget {
            receive: 20,
            output: 19,
        },
        ..shares()
    };
    let _ = NodeResources::new(small, [BLOCKS], 4096);
}

/// A frame above its raw gate takes the whole gate, so a receive share below
/// the kind's largest frame would charge that frame less than it holds.
#[test]
#[should_panic(expected = "receive share of 19 bytes must hold its largest frame")]
fn a_receive_share_below_its_largest_frame_is_refused() {
    let small = ReactorShares {
        other: ReactorBudget {
            receive: 19,
            output: 20,
        },
        ..shares()
    };
    let _ = NodeResources::new(small, [BLOCKS], 4096);
}

#[test]
fn the_envelope_sums_the_shares_of_every_connection() {
    let envelope = ResourceEnvelope::new(TransportLimits {
        max_open_streams: 1024,
        control_receive_bytes: 32 * MIB,
        connection_shares: ReactorBudget {
            receive: 48 * MIB,
            output: 96 * MIB,
        },
        compat_exchange_bytes: 20 * MIB,
        send_window: 16 * MIB,
        max_connections: 256,
        max_pending_handshakes: 32,
    });
    assert_eq!(envelope.connection_raw_bytes, 48 * MIB);
    assert_eq!(envelope.connection_output_bytes, 96 * MIB);
    assert_eq!(envelope.node_raw_bytes, 256 * 48 * MIB);
    assert_eq!(envelope.node_output_bytes, 256 * 96 * MIB);
    assert_eq!(
        envelope.compat_response_bytes,
        20 * MIB,
        "the limits give the exchange bytes for the whole node"
    );
    assert_eq!(envelope.request_executions, 256 * 1024);
    assert_eq!(
        envelope.transport_credit_bytes,
        u128::from(256 * (32 + 48 + 16) * MIB + 32 * (32 + 16) * MIB)
    );
    assert_eq!(
        envelope.transport_memory_bytes(),
        u128::from(envelope.node_raw_bytes)
            + u128::from(envelope.node_output_bytes)
            + u128::from(envelope.compat_response_bytes)
            + envelope.transport_credit_bytes,
    );
}

/// A shrink returns the unused bytes to the gate at once, and they wake a waiter
/// that the whole reservation had blocked.
#[tokio::test]
async fn shrinking_a_reservation_frees_room_for_a_waiter() {
    let (gate, budget) = gate(100);
    let mut reservation = gate.admit(80).await;
    assert_eq!(budget.granted(), 80);
    let mut waiter = Box::pin(gate.admit(50));
    assert!(futures::poll!(&mut waiter).is_pending());

    reservation.shrink_to(10);
    let waiting = tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("the returned bytes fund the waiter");
    assert_eq!(budget.granted(), 60);

    reservation.shrink_to(1_000);
    assert_eq!(budget.granted(), 60, "a shrink never grows a grant");
    drop((reservation, waiting));
    assert_eq!(budget.granted(), 0);
}

/// A connection's exchange gate never charges or waits for its kinds' gates or
/// another connection's exchanges, and a waiter that gives up takes nothing.
#[test]
fn compat_exchanges_use_their_connections_own_gate() {
    let node = node();
    let connection = node.connection(HEADERS.capability | BLOCKS.capability);
    let headers = connection.stream(HEADERS.kind, connection.owner()).unwrap();
    let capacity = COMPAT_EXCHANGE_BUDGET.receive;

    // A kind share of twenty bytes does not limit a whole exchange.
    let held = connection
        .admit_compat_response(capacity)
        .now_or_never()
        .expect("an idle gate funds one exchange");
    assert_eq!(node.owners().counts().installed, 1);
    assert!(headers.raw.try_admit(20).is_some(), "the kinds stay free");
    assert!(headers.output.try_admit(1024).is_some());

    assert_eq!(connection.compat_exchanges().granted(), capacity);
    assert!(connection.admit_compat_response(1).now_or_never().is_none());
    assert_eq!(
        connection.compat_exchanges().granted(),
        capacity,
        "the waiter took nothing"
    );
    let other = node.connection(HEADERS.capability);
    assert!(
        other
            .admit_compat_response(capacity)
            .now_or_never()
            .is_some(),
        "another connection's exchanges have their own gate"
    );

    drop(held);
    assert_eq!(connection.compat_exchanges().granted(), 0);
    drop(headers);
    assert_eq!(node.owners().counts().total(), 0);

    // An exchange larger than the gate clamps to it, so it can still finish.
    let clamped = connection
        .admit_compat_response(u64::MAX)
        .now_or_never()
        .expect("the grant clamps to the gate");
    assert_eq!(connection.compat_exchanges().granted(), capacity);
    drop(clamped);
}

/// One connection's exchange gate holds one worst-case exchange, and the
/// envelope counts it once per concurrent request of the compatibility client.
#[test]
fn the_compat_exchange_budget_holds_one_worst_case_exchange() {
    assert_eq!(COMPAT_EXCHANGE_MAX_BYTES, 20 * MIB);
    assert_eq!(
        COMPAT_EXCHANGE_BUDGET,
        ReactorBudget {
            receive: 20 * MIB,
            output: 0,
        }
    );
    let limits = |compat_exchange_bytes| TransportLimits {
        max_open_streams: 1024,
        control_receive_bytes: MIB,
        connection_shares: ReactorBudget::ZERO,
        compat_exchange_bytes,
        send_window: MIB,
        max_connections: 92,
        max_pending_handshakes: 32,
    };
    // Widening usize to u64 is lossless on supported targets.
    let client_requests = crate::zakura::legacy_gossip::LEGACY_CLIENT_REQUEST_SLOTS as u64;
    let envelope = ResourceEnvelope::new(limits(client_requests * COMPAT_EXCHANGE_MAX_BYTES));
    assert_eq!(envelope.compat_response_bytes, 288 * 20 * MIB);
    assert_eq!(
        ResourceEnvelope::new(limits(0)).compat_response_bytes,
        0,
        "a node without the legacy request kind holds no exchanges"
    );
}

/// The peer memory budget funds every admitted connection's whole window at the
/// default shares, its send window, every handshake, and the socket queue. The
/// transport envelope's QUIC term is the budget's charge without the socket.
#[test]
fn the_peer_memory_budget_funds_the_whole_windows() {
    let config = crate::Config::default();
    let limits = crate::zakura::ZakuraLocalLimits::from_config(&config);
    let shares = limits.reactor_shares();
    let envelope = ResourceEnvelope::new(
        limits.transport_limits_for_kinds(shares.kinds.iter().map(|(kind, _)| *kind)),
    );
    let quic = config
        .zakura
        .quic_memory_bytes(&config.network, limits.max_connections, &[]);
    assert_eq!(
        envelope.transport_credit_bytes + u128::from(zakura_quic::SOCKET_DATAGRAM_QUEUE_BYTES),
        quic
    );
    let budget = u128::from(crate::zakura::DEFAULT_ZAKURA_PEER_MEMORY_BUDGET_BYTES);
    assert!(quic <= budget, "{quic} of {budget}");
}

#[derive(Debug, Clone)]
enum Op {
    Admit(u8),
    Release(u8),
}

proptest! {
    /// However admissions and releases interleave, the gate's budget matches
    /// an independent ledger of the live grants, and admission succeeds
    /// exactly when the ledger has room. Releasing every grant empties it.
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
        let (gate, budget) = gate(64);
        let mut live: Vec<(FrameGrant, u64)> = Vec::new();
        for op in ops {
            match op {
                Op::Admit(bytes) => {
                    let bytes = u64::from(bytes);
                    let held: u64 = live.iter().map(|(_, bytes)| bytes).sum();
                    match gate.try_admit(bytes) {
                        Some(grant) => {
                            prop_assert!(held + bytes <= 64);
                            live.push((grant, bytes));
                        }
                        None => prop_assert!(held + bytes > 64),
                    }
                }
                Op::Release(index) => {
                    if !live.is_empty() {
                        drop(live.swap_remove(usize::from(index) % live.len()));
                    }
                }
            }
            let held: u64 = live.iter().map(|(_, bytes)| bytes).sum();
            prop_assert_eq!(budget.granted(), held);
        }
        live.clear();
        prop_assert_eq!(budget.granted(), 0);
    }
}
