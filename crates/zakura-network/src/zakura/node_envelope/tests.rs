//! Transport owners share one node without one kind's saturation stopping another.
//!
//! These tests hold only [`NodeResources`] owners. Other families' owners share no state
//! with it, so holding them here could not fail. No test holds several families' real
//! owners together: the integrated saturation witness is an open gap.

use std::{num::NonZeroU64, time::Duration};

use futures::FutureExt;
use zakura_rt::time::timeout;

use super::*;
use crate::{
    config::Config,
    zakura::{
        transport::{
            ConnectionResources, NodeResources, OwnerCounts, OwnerPhase, StreamResources,
            TransportLimits,
        },
        ReactorBudget, ReactorShares, ReceiveBudget, Stream,
    },
};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);
const WINDOW: u64 = 64;
const MAX_CONNECTIONS: usize = 2;
const MAX_OPEN_STREAMS: u16 = 4;
const TCP_INGRESS_BYTES: u64 = 3 << 20;

/// Three kinds on separate capability bits, each with a `WINDOW` share.
const BLOCKS: Stream = test_stream(6);
const HEADERS: Stream = test_stream(5);
const DISCOVERY: Stream = test_stream(4);

const fn test_stream(kind: u16) -> Stream {
    Stream {
        kind,
        capability: 1 << kind,
        frame_cap: WINDOW as u32,
        ..Stream::PERSISTENT
    }
}

fn shares() -> ReactorShares {
    ReactorShares {
        control_receive_bytes: WINDOW,
        kinds: Vec::new(),
        other: ReactorBudget {
            receive: ReceiveBudget::unsolicited_only(WINDOW),
            output: WINDOW,
        },
    }
}

fn limits() -> TransportLimits {
    TransportLimits {
        max_open_streams: MAX_OPEN_STREAMS,
        control_receive_bytes: WINDOW,
        connection_shares: ReactorBudget {
            receive: ReceiveBudget::unsolicited_only(3 * WINDOW),
            output: 3 * WINDOW,
        },
        compat_output: 0,
        compat_exchange_bytes: 0,
        send_window: WINDOW,
        handshake_send_window: WINDOW,
        max_connections: MAX_CONNECTIONS,
        max_pending_handshakes: 1,
    }
}

fn transport() -> ResourceEnvelope {
    ResourceEnvelope::new(limits())
}

fn envelope(limits: TransportLimits) -> Result<NodeEnvelope, NodeEnvelopeError> {
    NodeEnvelope::new(
        limits,
        1 << 20,
        TCP_INGRESS_BYTES,
        NonZeroU64::MIN,
        1 << 30,
        1 << 40,
    )
}

fn bound() -> OwnerBound {
    OwnerBound::new(1, MAX_CONNECTIONS, MAX_OPEN_STREAMS).expect("the test bound fits")
}

fn retained_bound() -> u128 {
    transport().retained_frame_bytes()
}

fn node() -> NodeResources {
    NodeResources::new(shares(), [BLOCKS, HEADERS, DISCOVERY], 1024)
}

fn connection(node: &NodeResources) -> ConnectionResources {
    node.connection(BLOCKS.capability | HEADERS.capability | DISCOVERY.capability)
}

fn stream(connection: &ConnectionResources, kind: Stream) -> StreamResources {
    connection
        .stream(kind.kind, connection.owner())
        .expect("the test connection negotiated every kind")
}

#[test]
fn the_default_node_config_composes() {
    let config = Config::default();
    let limits = ZakuraLocalLimits::from_config(&config);
    let envelope =
        NodeEnvelope::for_memory_plan(&config, &limits, &[], 16).expect("the default config fits");

    assert_eq!(envelope.owners, limits.owner_bound());
    assert_eq!(
        envelope.decoded_blocks,
        config
            .zakura
            .block_sync
            .decoded_block_allowance(16)
            .expect("the default allowance fits")
            .get()
    );
    // Block sync and header sync each fund `limits.total_connections()` sessions × 64 MiB;
    // discovery adds its answers.
    let sessions = limits.total_connections() as u128;
    assert!(envelope.serve_output_bytes >= 2 * sessions * (64 << 20));
    assert!(envelope.serve_output_bytes < 2 * sessions * (64 << 20) + (1 << 30));
    // The QUIC term is the derived one.
    assert_eq!(
        envelope.quic_bytes,
        config
            .zakura
            .quic_memory_bytes(&config.network, limits.total_connections(), &[])
    );
    assert_eq!(
        envelope.retained_bytes_bound(),
        envelope.transport_bytes
            + envelope.quic_bytes
            + u128::from(config.tcp_ingress_bound_bytes())
            + envelope.serve_output_bytes
    );
}

/// The default node's envelope, term by term: every connection at every reactor's
/// share on Mainnet.
#[test]
fn the_default_envelope_sums_the_reactor_shares() {
    const MIB: u64 = 1 << 20;
    let config = Config::default();
    let limits = ZakuraLocalLimits::from_config(&config);
    let shares = limits.reactor_shares();
    let share = |kind| shares.share(kind);
    assert_eq!(share(2), crate::zakura::LEGACY_GOSSIP_BUDGET);
    assert_eq!(share(3), crate::zakura::LEGACY_REQUESTS_BUDGET);
    assert_eq!(share(4), crate::zakura::DISCOVERY_BUDGET);
    assert_eq!(share(5), crate::zakura::HEADER_SYNC_BUDGET);
    assert_eq!(share(6), crate::zakura::BLOCK_SYNC_REACTOR_BUDGET);
    assert_eq!(
        share(9),
        crate::zakura::tx_relay::wire::TX_RELAY_DATA_BUDGET
    );
    assert_eq!(
        share(10),
        crate::zakura::tx_relay::wire::TX_RELAY_STATE_BUDGET
    );
    assert_eq!(
        shares.control_receive_bytes,
        crate::zakura::CONTROL_RECEIVE_BYTES
    );
    // The connection window and the stream window that carry the shares.
    assert_eq!(
        shares.receive_window(shares.kinds.iter().map(|(kind, _)| *kind)),
        crate::zakura::CONTROL_RECEIVE_BYTES + 79_750_859
    );
    assert_eq!(
        u64::from(limits.quic.stream_receive_window_bytes),
        crate::zakura::BLOCK_SYNC_REACTOR_BUDGET.receive_total()
    );
    assert_eq!(
        u64::from(limits.quic.receive_window()),
        crate::zakura::CONTROL_RECEIVE_BYTES
    );
    assert_eq!(
        limits.quic.send_window_bytes,
        crate::zakura::HANDSHAKE_SEND_WINDOW_BYTES
    );

    let envelope =
        NodeEnvelope::for_memory_plan(&config, &limits, &[], 16).expect("the default config fits");
    let transport = ResourceEnvelope::new(
        limits.transport_limits_for_kinds(shares.kinds.iter().map(|(kind, _)| *kind)),
    );
    // Per connection: 79,750,859 B of receive shares with `tx-relay` on
    // (counted inside QUIC receive credit via CreditHold), and unguarded output
    // shares: 24 MiB for legacy gossip and legacy requests plus `tx-relay`'s
    // 4,259,840 B. Discovery, header sync and block sync use guarded
    // Serve/Push output counted in `serve_output_bytes`.
    assert_eq!(transport.connection_raw_bytes, 79_750_859);
    assert_eq!(transport.connection_output_bytes, 24 * MIB + 4_259_840);
    // The node's exchange allowance caps the client's 288 exchanges.
    assert_eq!(transport.compat_response_bytes, 64 * MIB);
    let conns = limits.total_connections() as u128;
    // F-900: the compatibility output and exchanges stop at the node's 128 MiB
    // allowance instead of growing by 128 MiB per connection.
    assert_eq!(
        envelope.transport_bytes,
        conns * 4_259_840 + u128::from(crate::zakura::transport::COMPAT_NODE_BYTES)
    );
    assert_eq!(crate::zakura::transport::COMPAT_NODE_BYTES, 128 * MIB);
    // `conns` × 113,370,827 + 32 × 128 KiB + 256 MiB.
    assert_eq!(
        envelope.quic_bytes,
        conns * 113_370_827 + 32 * u128::from(128u64 << 10) + u128::from(256 * MIB)
    );
}

/// A custom stream kind takes the `other` share on a live connection, so every
/// connection's QUIC term grows by it, and the envelope counts its gates.
#[test]
fn custom_stream_kinds_take_the_other_share() {
    const CUSTOM: &[u16] = &[40];
    let config = Config::default();
    let network = config.network.clone();
    let limits = ZakuraLocalLimits::from_config(&config);
    let connections = limits.total_connections();

    // Widening usize to u128 is lossless on supported targets.
    let other = u128::from(config.zakura.reactor_shares(&network).other.receive_total());
    assert_eq!(
        config
            .zakura
            .quic_memory_bytes(&network, connections, CUSTOM),
        config.zakura.quic_memory_bytes(&network, connections, &[]) + connections as u128 * other
    );

    let envelope = NodeEnvelope::for_memory_plan(&config, &limits, CUSTOM, 16)
        .expect("the default config fits");
    let native_kinds = || {
        limits
            .reactor_shares()
            .kinds
            .into_iter()
            .map(|(kind, _)| kind)
    };
    let retained = |kinds: Vec<u16>| {
        ResourceEnvelope::new(limits.transport_limits_for_kinds(kinds)).retained_frame_bytes()
    };
    let with_custom = retained(native_kinds().chain(CUSTOM.iter().copied()).collect());
    assert_eq!(envelope.transport_bytes, with_custom);
    assert!(with_custom > retained(native_kinds().collect()));
}

#[test]
fn the_constructor_refuses_an_overflowing_owner_bound() {
    let mut overflowing = limits();
    overflowing.max_connections = usize::MAX;
    assert_eq!(envelope(overflowing), Err(NodeEnvelopeError::OwnerOverflow));
}

#[test]
fn the_byte_bound_adds_each_owner_once() {
    let envelope = envelope(limits()).expect("the limits fit");

    assert_eq!(envelope.transport_bytes, retained_bound());
    assert_eq!(envelope.decoded_pipeline_bytes, 1 << 30);
    assert_eq!(envelope.serve_output_bytes, 1 << 40);
    assert_eq!(
        envelope.retained_bytes_bound(),
        retained_bound() + (1 << 20) + u128::from(TCP_INGRESS_BYTES) + (1 << 40)
    );
}

/// A configured serving budget replaces each family's derived one in the envelope.
#[test]
fn configured_serving_budgets_set_the_serve_output_bound() {
    let mut config = Config::default();
    let limits = ZakuraLocalLimits::from_config(&config);
    let derived = NodeEnvelope::for_memory_plan(&config, &limits, &[], 16)
        .expect("the default config fits")
        .serve_output_bytes;
    config.zakura.block_sync.node_serve_output_bytes = Some(1 << 30);
    config.zakura.header_sync.node_serve_output_bytes = Some(1 << 30);
    let configured = NodeEnvelope::for_memory_plan(&config, &limits, &[], 16)
        .expect("the configured budgets fit")
        .serve_output_bytes;
    assert_eq!(
        derived - configured,
        2 * (limits.total_connections() as u128 * (64 << 20) - (1 << 30))
    );
}

/// With `tx-relay` on, the serve output bound adds, for each admitted connection,
/// the built `Tx` and the unwritten `Inv` a session holds outside its output gate.
/// The default config still fits, and with a fixed connection cap the admitted
/// connections are unchanged.
#[test]
fn tx_relay_counts_what_it_holds_outside_its_output_gate() {
    let mut config = Config::default();
    config.zakura.max_connections = Some(crate::zakura::DEFAULT_ZAKURA_MAX_CONNECTIONS);
    config.zakura.tx_relay.enabled = false;
    let off_limits = ZakuraLocalLimits::from_config(&config);
    let off = NodeEnvelope::for_memory_plan(&config, &off_limits, &[], 16)
        .expect("the default config fits")
        .serve_output_bytes;
    config.zakura.tx_relay.enabled = true;
    let limits = ZakuraLocalLimits::from_config(&config);
    assert_eq!(limits.total_connections(), off_limits.total_connections());
    let on = NodeEnvelope::for_memory_plan(&config, &limits, &[], 16)
        .expect("the default config fits with tx-relay on")
        .serve_output_bytes;
    let per_session = u128::from(crate::zakura::tx_relay::wire::SESSION_HELD_OUTPUT_BYTES);
    let connections = u128::try_from(limits.total_connections()).expect("a connection count fits");
    assert_eq!(on - off, connections * per_session);
}

/// Each serving family funds one session per connection that can serve at once: the
/// inbound plus outbound session slots, clamped to the admitted connections. The
/// retained bound counts that output, because guarded serving frames skip the
/// transport output gate.
#[test]
fn serving_output_counts_admitted_sessions() {
    const SESSION: u64 = 64 << 20;
    let mut config = Config::default();
    config.zakura.max_connections = Some(crate::zakura::DEFAULT_ZAKURA_MAX_CONNECTIONS);
    let limits = ZakuraLocalLimits::from_config(&config);
    let connections = limits.total_connections();
    // Widening usize to u64 and u128 is lossless on supported targets.
    let (sessions, discovery) = (
        connections as u64,
        u128::from(discovery_serving_output_bytes(connections)),
    );
    assert!(config.zakura.block_sync.peer_limits.max_inbound() > connections);

    // The reactors build their pools from the node's configs.
    let block_sync = config
        .zakura
        .block_sync
        .clone()
        .for_node_connections(connections);
    assert_eq!(
        getblocks_serving_limits(&block_sync).node_output_bytes,
        sessions * SESSION
    );
    let headers = config
        .zakura
        .header_sync
        .clone()
        .for_node_connections(connections);
    assert_eq!(headers.node_serve_output_bytes, Some(sessions * SESSION));

    let envelope =
        NodeEnvelope::for_memory_plan(&config, &limits, &[], 16).expect("the default config fits");
    // `tx-relay` is on by default, so each connection adds what it holds
    // outside its output gate.
    let tx_relay =
        u128::from(sessions) * u128::from(crate::zakura::tx_relay::wire::SESSION_HELD_OUTPUT_BYTES);
    assert_eq!(
        envelope.serve_output_bytes,
        2 * u128::from(sessions * SESSION) + discovery + tx_relay
    );
    assert_eq!(
        envelope.retained_bytes_bound(),
        envelope.transport_bytes
            + envelope.quic_bytes
            + u128::from(envelope.tcp_ingress_bytes)
            + envelope.serve_output_bytes
    );

    // Fewer session slots than connections bound the pools instead.
    for peer_limits in [
        &mut config.zakura.block_sync.peer_limits,
        &mut config.zakura.header_sync.peer_limits,
    ] {
        peer_limits.max_inbound_peers = Some(3);
        peer_limits.max_outbound_peers = Some(2);
    }
    let envelope =
        NodeEnvelope::for_memory_plan(&config, &limits, &[], 16).expect("the smaller pools fit");
    assert_eq!(
        envelope.serve_output_bytes,
        2 * u128::from(5 * SESSION) + discovery + tx_relay
    );
}

/// A reconnect gets fresh connection gates, but it finds the old generation's
/// retiring owners and their held bytes still counted.
#[test]
fn retiring_generations_count_against_the_node_across_reconnects() {
    let node = node();
    let mut retiring = Vec::new();
    let mut charges = Vec::new();
    let mut held = 0;

    for generation in 1..=3_usize {
        let connection = connection(&node);
        let stream = stream(&connection, BLOCKS);
        let charge = stream
            .raw
            .try_admit(WINDOW)
            .expect("a new connection's gate funds its first frame");
        held += WINDOW;
        stream.owner.advance(OwnerPhase::Retiring);
        retiring.push(stream);
        charges.push(charge);
        // The connection's handle ends while its retiring owner remains.
        drop(connection);

        let usage = node.report_usage(bound());
        assert_eq!(usage.owners.retiring, generation);
        assert_eq!(usage.owners.streams(), generation);
        assert_eq!(usage.raw_bytes(), held, "a reconnect resets no held bytes");
        assert!(bound().admits(&usage.owners));
    }

    drop(charges);
    drop(retiring);
    let usage = node.usage();
    assert_eq!(usage.owners, OwnerCounts::default());
    assert_eq!(usage.raw_bytes(), 0, "the node returns to its baseline");
    assert!(
        usage.kinds.iter().all(|kind| kind.raw_bound == 0),
        "the report drops the gates of ended connections"
    );
}

/// A generation that outlives its admission slot breaks the bound. The reader reports
/// that and refuses nothing.
#[test]
#[cfg(not(zakura_sim))]
fn owners_beyond_the_admission_bound_are_visible_and_keep_running() {
    let node = node();
    let connection = connection(&node);
    let streams: Vec<_> = (0..=bound().streams)
        .map(|_| stream(&connection, BLOCKS))
        .collect();

    let usage = node.report_usage(bound());

    assert!(!bound().admits(&usage.owners));
    assert_eq!(usage.owners.setup, bound().streams + 1);
    drop(streams);
    assert!(bound().admits(&node.usage().owners));

    let handshakes: Vec<_> = (0..2)
        .map(|_| node.owners().enter(OwnerPhase::Handshake))
        .collect();
    assert!(!bound().admits(&node.usage().owners));
    drop(handshakes);
}

/// Property conn-17: in the simulator, `report_usage` asserts when stream
/// owners exceed `OwnerBound`.
#[cfg(zakura_sim)]
#[test]
#[should_panic(expected = "Zakura owners exceed their admission bound")]
fn report_usage_asserts_when_stream_owners_exceed_bound() {
    let node = node();
    let connection = connection(&node);
    let _streams: Vec<_> = (0..=bound().streams)
        .map(|_| stream(&connection, BLOCKS))
        .collect();
    let _ = node.report_usage(bound());
}

/// Property conn-17: in the simulator, `report_usage` asserts when handshake
/// owners exceed `OwnerBound`.
#[cfg(zakura_sim)]
#[test]
#[should_panic(expected = "Zakura owners exceed their admission bound")]
fn report_usage_asserts_when_handshake_owners_exceed_bound() {
    let node = node();
    let _handshakes: Vec<_> = (0..=bound().handshakes)
        .map(|_| node.owners().enter(OwnerPhase::Handshake))
        .collect();
    let _ = node.report_usage(bound());
}

/// One kind fills its raw and output gates on one connection. Every other kind on
/// that connection, and the same kind on another connection, still admits a frame,
/// and the sum stays inside the composed bound.
#[tokio::test]
async fn mixed_family_saturation_leaves_every_other_owner_available() {
    timeout(TEST_TIMEOUT, async {
        let node = node();
        let first = connection(&node);
        let second = connection(&node);
        let blocks = stream(&first, BLOCKS);
        let headers = stream(&first, HEADERS);
        let discovery = stream(&first, DISCOVERY);

        // One kind saturates its gates.
        let _block_raw = blocks.raw.try_admit(WINDOW).expect("the gate is free");
        let _block_output = blocks.output.try_admit(WINDOW).expect("the gate is free");
        assert!(blocks.raw.try_admit(1).is_none());
        assert!(blocks.output.try_admit(1).is_none());

        // Other kinds, and the same kind elsewhere, each still make progress.
        let header_frame = headers.raw.try_admit(WINDOW);
        let discovery_frame = discovery.output.try_admit(WINDOW);
        let other_blocks = stream(&second, BLOCKS).raw.try_admit(WINDOW);
        assert!(header_frame.is_some() && discovery_frame.is_some() && other_blocks.is_some());

        let compat = first
            .admit_compat_response(1)
            .now_or_never()
            .expect("the compatibility gate is separate from every kind's gates");

        let usage = node.report_usage(bound());
        assert!(bound().admits(&usage.owners));
        assert_eq!(usage.raw_bytes(), 3 * WINDOW);
        assert_eq!(usage.output_bytes(), 2 * WINDOW);
        assert_eq!(usage.compat_response_bytes, 1);
        assert!(
            u128::from(usage.raw_bytes())
                <= u128::from(transport().connection_raw_bytes) * MAX_CONNECTIONS as u128
        );
        assert!(
            u128::from(usage.output_bytes()) + u128::from(usage.compat_response_bytes)
                <= retained_bound()
        );

        drop(compat);
    })
    .await
    .expect("the composition never waits on a saturated owner");
}

/// A frame on a full kind waits for that kind's own release. Its cancelled wait takes
/// nothing, and another kind's frame on the same connection admits at once.
#[tokio::test]
async fn a_full_kind_delays_only_its_own_frames() {
    let node = node();
    let connection = connection(&node);
    let blocks = stream(&connection, BLOCKS);
    let full = blocks.output.try_admit(WINDOW).expect("the gate is free");

    let control = stream(&connection, HEADERS);
    let close_frame = control
        .output
        .admit(WINDOW)
        .now_or_never()
        .expect("another kind's gate admits the control frame at once");
    assert!(close_frame.waited().is_zero());

    // A second frame of the full kind waits, and its cancelled wait takes nothing.
    {
        let waiting = blocks.output.admit(WINDOW);
        futures::pin_mut!(waiting);
        assert!(waiting.as_mut().now_or_never().is_none());
    }
    assert_eq!(node.usage().output_bytes(), 2 * WINDOW);
    drop(full);
    let next = timeout(TEST_TIMEOUT, blocks.output.admit(WINDOW))
        .await
        .expect("the released gate admits the next frame");
    assert_eq!(node.usage().output_bytes(), 2 * WINDOW);
    drop((next, close_frame));
    assert_eq!(node.usage().output_bytes(), 0);
}
