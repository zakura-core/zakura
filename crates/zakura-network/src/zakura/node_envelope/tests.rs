//! Transport owners share one node without one stream's saturation stopping another.
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
    zakura::transport::{NodeResources, OwnerCounts, OwnerPhase, TransportLimits},
};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);
const WINDOW: u64 = 64;
const MAX_CONNECTIONS: usize = 2;
const MAX_OPEN_STREAMS: u16 = 4;
const MAX_FRAME_BYTES: u32 = 1024;
const TCP_INGRESS_BYTES: u64 = 3 << 20;

fn limits() -> TransportLimits {
    TransportLimits {
        receive_window: WINDOW as u32,
        send_window: WINDOW,
        max_connections: MAX_CONNECTIONS,
        max_pending_handshakes: 1,
        max_open_streams: MAX_OPEN_STREAMS,
        max_frame_bytes: MAX_FRAME_BYTES,
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
    // Block sync and header sync each fund 512 sessions × 64 MiB; discovery adds its
    // answers.
    assert!(envelope.serve_output_bytes >= 2 * 512 * (64 << 20));
    assert!(envelope.serve_output_bytes < 2 * 512 * (64 << 20) + (1 << 30));
    // The transport bound depends on the host's memory, so only its parts are fixed.
    assert_eq!(
        envelope.retained_bytes_bound(),
        envelope.transport_bytes
            + u128::from(config.zakura.peer_memory_budget_bytes)
            + u128::from(config.tcp_ingress_bound_bytes())
    );
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

/// One stream holds all shared transport bytes and its own floors. Every other stream
/// keeps its floor, and the sum stays inside the composed bound.
#[tokio::test]
async fn mixed_family_saturation_leaves_every_other_owner_available() {
    timeout(TEST_TIMEOUT, async {
        let node = NodeResources::new(transport());
        let connection = node.connection();
        let blocks = connection.stream();
        let headers = connection.stream();
        let discovery = connection.stream();

        // One stream saturates the shared budgets and its own floors.
        let (_raw_hold, _output_hold) = node.hold_node_budgets();
        let _block_raw_floor = blocks.raw.try_admit(WINDOW).expect("the floor is free");
        let _block_output_floor = blocks.output.try_admit(WINDOW).expect("the floor is free");
        assert!(blocks.raw.try_admit(1).is_none());
        assert!(blocks.output.try_admit(1).is_none());

        // Other streams each still make progress on their own floors.
        let header_frame = headers.raw.try_admit(WINDOW);
        let discovery_frame = discovery.output.try_admit(WINDOW);
        assert!(header_frame.is_some() && discovery_frame.is_some());

        let compat = connection
            .admit_compat_response(1)
            .now_or_never()
            .expect("the compatibility partition is separate from raw bytes");

        let usage = node.report_usage(bound());
        assert!(bound().admits(&usage.owners));
        assert!(
            u128::from(usage.raw_bytes)
                + u128::from(usage.output_bytes)
                + u128::from(usage.compat_response_bytes)
                <= retained_bound()
        );

        drop(compat);
    })
    .await
    .expect("the composition never waits on a saturated owner");
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
