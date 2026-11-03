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
        ReactorBudget, ReactorShares, Stream,
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
            receive: WINDOW,
            output: WINDOW,
        },
    }
}

fn limits() -> TransportLimits {
    TransportLimits {
        max_open_streams: MAX_OPEN_STREAMS,
        control_receive_bytes: WINDOW,
        connection_shares: ReactorBudget {
            receive: 3 * WINDOW,
            output: 3 * WINDOW,
        },
        compat_exchange_bytes: 0,
        send_window: WINDOW,
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
    let envelope = NodeEnvelope::for_node(&config, &limits, 16).expect("the default config fits");

    assert_eq!(limits.max_connections, crate::zakura::DEFAULT_ZAKURA_MAX_CONNECTIONS);
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
    // Block sync and header sync each fund 512 sessions × 64 MiB; discovery adds its
    // answers.
    assert!(envelope.serve_output_bytes >= 2 * 512 * (64 << 20));
    assert!(envelope.serve_output_bytes < 2 * 512 * (64 << 20) + (1 << 30));
    // The QUIC term is the derived one, within the peer memory budget.
    assert_eq!(
        envelope.quic_bytes,
        config
            .zakura
            .quic_memory_bytes(&config.network, limits.max_connections)
    );
    assert!(envelope.quic_bytes <= u128::from(config.zakura.peer_memory_budget_bytes));
    assert_eq!(
        envelope.retained_bytes_bound(),
        envelope.transport_bytes
            + envelope.quic_bytes
            + u128::from(config.tcp_ingress_bound_bytes())
    );
}

/// The default node's envelope, term by term: 92 connections at every reactor's
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
    assert_eq!(shares.control_receive_bytes, 32 * MIB);
    // The connection window and the stream window that carry the shares.
    assert_eq!(
        shares.receive_window(shares.kinds.iter().map(|(kind, _)| *kind)),
        32 * MIB + 74_828_489
    );
    assert_eq!(
        u64::from(limits.quic.stream_receive_window_bytes),
        crate::zakura::BLOCK_SYNC_REACTOR_BUDGET.receive
    );
    assert_eq!(u64::from(limits.quic.receive_window_bytes), 32 * MIB);

    let envelope = NodeEnvelope::for_node(&config, &limits, 16).expect("the default config fits");
    let transport = ResourceEnvelope::new(
        limits.transport_limits_for_kinds(shares.kinds.iter().map(|(kind, _)| *kind)),
    );
    // Per connection: 74,828,489 B of receive shares and 180,303,803 B of output shares.
    assert_eq!(transport.connection_raw_bytes, 74_828_489);
    assert_eq!(transport.connection_output_bytes, 180_303_803);
    assert_eq!(transport.compat_response_bytes, 92 * 20 * MIB);
    assert_eq!(
        envelope.transport_bytes,
        92 * u128::from(74_828_489_u64 + 180_303_803 + 20 * MIB)
    );
    // 92 × 141,937,353 + 32 × 64 MiB + 256 MiB.
    assert_eq!(envelope.quic_bytes, 15_474_155_580);
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
        retained_bound() + (1 << 20) + u128::from(TCP_INGRESS_BYTES)
    );
}

/// A configured serving budget replaces each family's derived one in the envelope.
#[test]
fn configured_serving_budgets_set_the_serve_output_bound() {
    let mut config = Config::default();
    let limits = ZakuraLocalLimits::from_config(&config);
    let derived = NodeEnvelope::for_node(&config, &limits, 16)
        .expect("the default config fits")
        .serve_output_bytes;
    config.zakura.block_sync.node_serve_output_bytes = Some(1 << 30);
    config.zakura.header_sync.node_serve_output_bytes = Some(1 << 30);
    let configured = NodeEnvelope::for_node(&config, &limits, 16)
        .expect("the configured budgets fit")
        .serve_output_bytes;
    assert_eq!(derived - configured, 2 * (512 * (64 << 20) - (1 << 30)));
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
                + u128::from(usage.output_bytes())
                + u128::from(usage.compat_response_bytes)
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
