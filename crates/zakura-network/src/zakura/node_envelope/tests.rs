//! Owner families share one node without one family's saturation stopping another.
//!
//! These tests hold real owners together. The header, discovery, and legacy partitions
//! keep their private owners, so each of their modules tests them against a saturated
//! [`NodeResources`].

use std::{num::NonZeroU64, time::Duration};

use futures::FutureExt;
use zakura_chain::{block::Block, serialization::ZcashDeserializeInto};
use zakura_node_services::block_memory::{Admission, DecodedBlockPool};
use zakura_rt::time::timeout;

use super::*;
use crate::{
    config::Config,
    zakura::{
        regulation::{test_family, OutputByteBudget, ServeCapacity, ServeLimits, SessionCapacity},
        transport::{
            ByteBudget, FrameGate, NodeResources, NodeUsage, OwnerCounts, OwnerPhase,
            TransportLimits,
        },
        ServicePeerDirection, ServicePeerLimits,
    },
};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);
const WINDOW: u64 = 64;
const MAX_CONNECTIONS: usize = 2;
const MAX_OPEN_STREAMS: u16 = 4;
const MAX_FRAME_BYTES: u32 = 1024;

const SERVE_LIMITS: ServeLimits = ServeLimits {
    node_execution: 2,
    peer_execution: 2,
    peer_output_responses: 2,
    node_output_responses: 2,
    peer_output_bytes: 1 << 10,
    node_output_bytes: 1 << 10,
};

fn transport() -> ResourceEnvelope {
    ResourceEnvelope::new(TransportLimits {
        receive_window: WINDOW as u32,
        send_window: WINDOW,
        max_connections: MAX_CONNECTIONS,
        max_pending_handshakes: 1,
        max_open_streams: MAX_OPEN_STREAMS,
        max_frame_bytes: MAX_FRAME_BYTES,
    })
}

fn inputs() -> NodeEnvelopeInputs {
    NodeEnvelopeInputs {
        transport: transport(),
        receive_window_bytes: WINDOW,
        send_window_bytes: WINDOW,
        max_connections: MAX_CONNECTIONS,
        max_pending_handshakes: 1,
        max_open_streams: MAX_OPEN_STREAMS,
        max_frame_bytes: MAX_FRAME_BYTES,
        peer_memory_budget_bytes: 1 << 20,
        tcp_connections: 3,
        decoded_pipeline_bytes: 1 << 30,
    }
}

fn bound() -> OwnerBound {
    OwnerBound::new(1, MAX_CONNECTIONS, MAX_OPEN_STREAMS)
}

fn retained_bound() -> u128 {
    transport().retained_frame_bytes()
}

fn block() -> Block {
    zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .expect("the test vector is a valid block")
}

fn one_block_pool() -> DecodedBlockPool {
    DecodedBlockPool::for_block_count(NonZeroU64::MIN)
}

#[test]
fn the_default_node_config_composes() {
    let config = Config::default();
    let limits = ZakuraLocalLimits::from_config(&config);
    let envelope = NodeEnvelope::for_node(&config, &limits, 16).expect("the default config fits");

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
    // The transport bound depends on the host's memory, so only its parts are fixed.
    assert_eq!(
        envelope.retained_bytes_bound(),
        envelope.transport_bytes
            + u128::from(config.zakura.peer_memory_budget_bytes)
            + u128::from(config.tcp_ingress_bound_bytes())
    );
}

#[test]
fn the_constructor_refuses_a_limit_that_funds_nothing() {
    let refused = [
        ("max_connections", {
            let mut inputs = inputs();
            inputs.max_connections = 0;
            inputs
        }),
        ("max_pending_handshakes", {
            let mut inputs = inputs();
            inputs.max_pending_handshakes = 0;
            inputs
        }),
        ("max_open_streams", {
            let mut inputs = inputs();
            inputs.max_open_streams = 0;
            inputs
        }),
        ("max_frame_bytes", {
            let mut inputs = inputs();
            inputs.max_frame_bytes = 0;
            inputs
        }),
        ("receive_window_bytes", {
            let mut inputs = inputs();
            inputs.receive_window_bytes = 0;
            inputs
        }),
        ("send_window_bytes", {
            let mut inputs = inputs();
            inputs.send_window_bytes = 0;
            inputs
        }),
        ("peer_memory_budget_bytes", {
            let mut inputs = inputs();
            inputs.peer_memory_budget_bytes = 0;
            inputs
        }),
    ];
    for (limit, inputs) in refused {
        assert_eq!(
            NodeEnvelope::new(inputs, NonZeroU64::MIN),
            Err(NodeEnvelopeError::ZeroLimit { limit })
        );
    }
}

#[test]
fn the_constructor_refuses_an_overflowing_bound() {
    let mut owners = inputs();
    owners.max_connections = usize::MAX;
    assert_eq!(
        NodeEnvelope::new(owners, NonZeroU64::MIN),
        Err(NodeEnvelopeError::Overflow {
            bound: "stream owners"
        })
    );

    let mut tcp = inputs();
    tcp.tcp_connections = usize::MAX;
    assert_eq!(
        NodeEnvelope::new(tcp, NonZeroU64::MIN),
        Err(NodeEnvelopeError::Overflow {
            bound: "tcp ingress bytes"
        })
    );
}

#[test]
fn the_byte_bound_adds_each_owner_once() {
    let envelope = NodeEnvelope::new(inputs(), NonZeroU64::MIN).expect("the inputs fit");

    assert_eq!(envelope.transport_bytes, retained_bound());
    assert_eq!(
        envelope.request_executions,
        MAX_CONNECTIONS * usize::from(MAX_OPEN_STREAMS)
    );
    assert_eq!(envelope.decoded_pipeline_bytes, 1 << 30);
    assert_eq!(
        envelope.retained_bytes_bound(),
        retained_bound() + (1 << 20) + 3 * MAX_INCOMPLETE_MESSAGE_BYTES as u128
    );
}

/// A reconnect gets fresh connection budgets, but it finds the old generation's
/// retiring owners and node-wide charges still counted.
#[test]
fn retiring_generations_count_against_the_node_across_reconnects() {
    let node = NodeResources::new(transport());
    let mut retiring = Vec::new();
    let mut charges = Vec::new();
    let mut held = 0;

    for generation in 1..=3_usize {
        let connection = node.connection();
        let stream = connection.stream();
        let charge = stream
            .raw
            .try_admit(WINDOW)
            .expect("a new connection's budget funds its first frame");
        held += WINDOW;
        stream.owner.advance(OwnerPhase::Retiring);
        retiring.push(stream);
        charges.push(charge);
        // The connection's handle ends while its retiring owner remains.
        drop(connection);

        let usage = node.report_usage(bound());
        assert_eq!(usage.owners.retiring, generation);
        assert_eq!(usage.owners.streams(), generation);
        assert_eq!(usage.raw_bytes, held, "a reconnect resets no node charge");
        assert!(bound().admits(&usage.owners));
        assert!(u128::from(usage.raw_bytes) <= retained_bound());
    }

    drop(charges);
    drop(retiring);
    let usage = node.usage();
    assert_eq!(usage.owners, OwnerCounts::default());
    assert_eq!(usage.raw_bytes, 0, "the node returns to its baseline");
}

/// A generation that outlives its admission slot breaks the bound. The reader reports
/// that and refuses nothing.
#[test]
fn owners_beyond_the_admission_bound_are_visible_and_keep_running() {
    let node = NodeResources::new(transport());
    let connection = node.connection();
    let streams: Vec<_> = (0..=bound().streams).map(|_| connection.stream()).collect();

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

/// Every metric label comes from fixed tables, so churn cannot grow their cardinality.
#[test]
fn usage_diagnostics_keep_a_fixed_vocabulary() {
    let node = NodeResources::new(transport());
    let labels = |usage: NodeUsage| usage.budget_bytes().map(|(label, _)| label);
    let empty = labels(node.usage());

    let streams: Vec<_> = (0..64).map(|_| node.connection().stream()).collect();
    let grants: Vec<_> = streams
        .iter()
        .map(|stream| stream.raw.try_admit(8).expect("the first frame is funded"))
        .collect();

    assert_eq!(labels(node.usage()), empty);
    assert_eq!(empty, ["raw", "output", "compat_response"]);
    drop(grants);
}

/// One family holds all shared transport bytes and its own owner. Every other family
/// keeps its own capacity, and the sum stays inside the composed bound.
#[tokio::test]
async fn mixed_family_saturation_leaves_every_other_owner_available() {
    timeout(TEST_TIMEOUT, async {
        let node = NodeResources::new(transport());
        let connection = node.connection();
        let blocks = connection.stream();
        let headers = connection.stream();
        let discovery = connection.stream();

        // Blocks saturate the shared budgets and their own floors.
        let (_raw_hold, _output_hold) = node.hold_node_budgets();
        let _block_raw_floor = blocks.raw.try_admit(WINDOW).expect("the floor is free");
        let _block_output_floor = blocks.output.try_admit(WINDOW).expect("the floor is free");
        assert!(blocks.raw.try_admit(1).is_none());
        assert!(blocks.output.try_admit(1).is_none());

        // The block family's own owners saturate too.
        let pool = one_block_pool();
        let retained = pool
            .try_admit(block(), Admission::WithinLimit)
            .expect("the pool funds one block");
        assert!(pool.try_admit(block(), Admission::WithinLimit).is_err());

        // Other families each still make progress on their own capacity.
        let header_frame = headers.raw.try_admit(WINDOW);
        let discovery_frame = discovery.output.try_admit(WINDOW);
        assert!(header_frame.is_some() && discovery_frame.is_some());

        let compat = connection
            .admit_compat_response(1)
            .now_or_never()
            .expect("the compatibility partition is separate from raw bytes");

        let backend = ByteBudget::new(1 << 10);
        let reservation = backend
            .reserve_owned(1 << 10)
            .expect("the byte budget is empty");
        let serve = ServeCapacity::new("mixed", &test_family::GET, SERVE_LIMITS)
            .expect("the limits are valid");
        assert_eq!(serve.node_output_held(), 0);
        let session = SessionCapacity::new("mixed", &ServicePeerLimits::default());
        let _session_slot = session
            .reserve(ServicePeerDirection::Outbound)
            .expect("the session capacity is free");

        let usage = node.report_usage(bound());
        assert!(bound().admits(&usage.owners));
        assert!(
            u128::from(usage.raw_bytes)
                + u128::from(usage.output_bytes)
                + u128::from(usage.compat_response_bytes)
                <= retained_bound()
        );

        drop(compat);
        drop(reservation);
        drop(retained);
        assert_eq!(pool.reserved_bytes(), 0);
    })
    .await
    .expect("the composition never waits on a saturated owner");
}

/// A saturated decoded pool refuses its own blocks and touches nothing else. Closing it
/// leaves its holders' charges in place until they drop.
#[tokio::test]
async fn backend_contention_stays_inside_the_decoded_pool() {
    timeout(TEST_TIMEOUT, async {
        let node = NodeResources::new(transport());
        let stream = node.connection().stream();
        let pool = one_block_pool();
        let held = pool
            .try_admit(block(), Admission::WithinLimit)
            .expect("the pool funds one block");
        let attributed = pool.reserved_bytes();
        assert!(attributed > 0);

        assert!(pool.try_admit(block(), Admission::WithinLimit).is_err());
        assert_eq!(
            pool.reserved_bytes(),
            attributed,
            "a refusal charges nothing"
        );
        assert_eq!(node.usage().raw_bytes, 0, "the pool charges no frame bytes");
        let frame = stream.raw.try_admit(WINDOW);
        assert!(frame.is_some(), "a saturated backend holds no frame");

        pool.close();
        assert!(pool.try_admit(block(), Admission::WithinLimit).is_err());
        assert_eq!(
            pool.reserved_bytes(),
            attributed,
            "a holder keeps its charge"
        );
        drop(held);
        assert_eq!(pool.reserved_bytes(), 0);
    })
    .await
    .expect("nothing here waits on the backend");
}

/// Close and reject frames use the output floor, so a saturated node delays a control
/// frame by at most one frame on its own stream.
#[tokio::test]
async fn control_frames_progress_through_saturated_output() {
    let node = NodeResources::new(transport());
    let connection = node.connection();
    let (_raw_hold, _output_hold) = node.hold_node_budgets();
    let held_output = node.usage().output_bytes;

    let control = connection.stream();
    let close_frame = control
        .output
        .admit(WINDOW)
        .now_or_never()
        .expect("the floor slot admits the control frame without shared bytes");
    assert!(close_frame.waited().is_zero());
    assert_eq!(
        node.usage().output_bytes,
        held_output,
        "a floor frame charges no shared bytes"
    );

    // A second frame on the same stream waits, and its cancelled wait takes nothing.
    {
        let waiting = control.output.admit(WINDOW);
        futures::pin_mut!(waiting);
        assert!(waiting.as_mut().now_or_never().is_none());
    }
    drop(close_frame);
    let next = timeout(TEST_TIMEOUT, control.output.admit(WINDOW))
        .await
        .expect("the returned floor slot admits the next frame");
    assert_eq!(node.usage().output_bytes, held_output);
    drop(next);
}

/// An unpolled admission holds nothing, and an abandoned wait leaves every budget as it
/// found it.
#[test]
fn an_unpolled_admission_holds_nothing() {
    let shared = OutputByteBudget::new(WINDOW).expect("the budget size is valid");
    let node = OutputByteBudget::new(WINDOW).expect("the budget size is valid");
    let gate = FrameGate::new(shared.clone(), node.clone());

    drop(gate.admit(WINDOW));
    assert_eq!(shared.granted(), 0);
    assert_eq!(node.granted(), 0);
}
