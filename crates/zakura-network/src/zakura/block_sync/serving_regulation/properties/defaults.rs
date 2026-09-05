//! Executable sizing examples on unmodified production defaults.
//!
//! These tests transfer byte charges without allocating block bodies. The real
//! encoder and writer are covered separately by the response-write witnesses.

use std::time::Duration;

use super::super::*;

// Independent expectations, deliberately not computed by `serving_cost`.
const RESPONSE_BYTES: u64 = 2_000_010;
const CHARGE: u64 = 2_065_546;
const PEER_BURST: u64 = 33_620_105;
const NODE_BURST: u64 = 134_217_728;

fn defaults() -> GetBlocksServingRegulator {
    let config = ZakuraBlockSyncConfig::default();
    validate_config(&config).unwrap();
    assert_eq!(
        config.get_blocks_regulation.peer_rate_capacity_bytes,
        PEER_BURST
    );
    assert_eq!(
        config.get_blocks_regulation.node_rate_capacity_bytes,
        NODE_BURST
    );
    assert_eq!(
        serving_cost(&config, 1).unwrap().response_cap,
        RESPONSE_BYTES
    );
    assert_eq!(serving_cost(&config, 1).unwrap().charge, CHARGE);
    GetBlocksServingRegulator::new(config)
}

fn session(regulator: &GetBlocksServingRegulator, identity: u8) -> GetBlocksServingSession {
    regulator.session(
        ZakuraPeerId::new(vec![identity; 32]).unwrap(),
        u64::from(identity),
    )
}

/// Leave only the actual response bytes owned by transport; release active work.
fn queued_response(session: &GetBlocksServingSession) -> FrameLease {
    let mut permit = session.try_admit(1).unwrap().commit();
    permit.transfer_frame(RESPONSE_BYTES)
}

/// A failed attempt must return all earlier rate, active and byte reservations.
fn assert_blocked(session: &GetBlocksServingSession, kind: BoundKind) {
    let regulator = &session.regulator;
    let before = regulator.snapshot();
    let peer_credit = session.peer_rate_available();
    assert_eq!(session.try_admit(1).unwrap_err().kind(), kind);
    let after = regulator.snapshot();
    assert_eq!(after.node_rate_available, before.node_rate_available);
    assert_eq!(session.peer_rate_available(), peer_credit);
    assert_eq!(after.node_active, before.node_active);
    assert_eq!(after.node_outstanding, before.node_outstanding);
    assert_eq!(after.peer_outstanding, before.peer_outstanding);
}

#[test]
fn default_response_count_caps_large_requests_at_one_block() {
    let config = ZakuraBlockSyncConfig::default();
    for count in [1, 128, u32::MAX] {
        let cost = serving_cost(&config, count).unwrap();
        assert_eq!(cost.count, 1);
        assert_eq!(cost.response_cap, RESPONSE_BYTES);
        assert_eq!(cost.charge, CHARGE);
    }
}

#[tokio::test(start_paused = true)]
async fn default_settlement_refunds_unused_capacity_after_the_last_query_owner() {
    // No queued response, terminal only, a 1,617-byte body plus framing, and
    // a maximum-size body plus framing. These are accounting sizes, not blocks.
    for payload_bytes in [0, 9, 1_627, RESPONSE_BYTES] {
        let regulator = defaults();
        let peer = session(&regulator, 1);
        let attempt = peer.try_admit(1).unwrap();
        assert_eq!(peer.peer_rate_available(), PEER_BURST - CHARGE);
        drop(attempt);
        assert_eq!(peer.peer_rate_available(), PEER_BURST);
        assert_eq!(regulator.snapshot().node_rate_available, NODE_BURST);

        let mut permit = peer.try_admit(1).unwrap().commit();
        let query = permit.query_lease();
        assert!(query.try_start());
        let frame = permit.transfer_frame(payload_bytes);
        drop(permit);
        assert_eq!(peer.peer_rate_available(), PEER_BURST - CHARGE);
        assert_eq!(regulator.snapshot().node_outstanding, RESPONSE_BYTES);
        drop(query);
        let spent = 65_536 + payload_bytes;
        assert_eq!(peer.peer_rate_available(), PEER_BURST - spent);
        assert_eq!(regulator.snapshot().node_rate_available, NODE_BURST - spent);
        assert_eq!(regulator.snapshot().node_active, 0);
        assert_eq!(regulator.snapshot().node_outstanding, payload_bytes);
        drop(frame);
        assert_eq!(regulator.snapshot().node_outstanding, 0);
        assert_eq!(peer.peer_rate_available(), PEER_BURST - spent);
    }
}

#[tokio::test(start_paused = true)]
async fn default_peer_burst_serves_sixteen_maximum_blocks_then_refills() {
    let regulator = defaults();
    let peer = session(&regulator, 1);
    for _ in 0..16 {
        drop(queued_response(&peer));
    }
    assert_eq!(peer.peer_rate_available(), PEER_BURST - 16 * CHARGE);
    assert_eq!(regulator.snapshot().node_active, 0);
    assert_eq!(regulator.snapshot().node_outstanding, 0);
    assert_blocked(&peer, BoundKind::PeerRate);

    // The first nanosecond with enough whole credit for request 17, using the
    // published 16 MiB/s refill, independently of the limiter's retry delay.
    let deficit = 17 * CHARGE - PEER_BURST;
    let nanos = (deficit * 1_000_000_000).div_ceil(16 * 1024 * 1024);
    tokio::time::advance(Duration::from_nanos(nanos - 1)).await;
    assert_blocked(&peer, BoundKind::PeerRate);
    tokio::time::advance(Duration::from_nanos(1)).await;
    drop(queued_response(&peer));
}

#[tokio::test(start_paused = true)]
async fn default_node_burst_serves_sixty_four_maximum_blocks_without_active_owners() {
    let regulator = defaults();
    let peers: Vec<_> = (0..5).map(|id| session(&regulator, id)).collect();
    for request in 0..64 {
        drop(queued_response(&peers[request % peers.len()]));
    }
    assert_eq!(
        regulator.snapshot().node_rate_available,
        NODE_BURST - 64 * CHARGE
    );
    assert_eq!(regulator.snapshot().node_active, 0);
    assert_eq!(regulator.snapshot().node_outstanding, 0);
    assert_blocked(&peers[4], BoundKind::NodeRate);

    let deficit = 65 * CHARGE - NODE_BURST;
    let nanos = (deficit * 1_000_000_000).div_ceil(64 * 1024 * 1024);
    tokio::time::advance(Duration::from_nanos(nanos - 1)).await;
    assert_blocked(&peers[4], BoundKind::NodeRate);
    tokio::time::advance(Duration::from_nanos(1)).await;
    drop(queued_response(&peers[4]));
}

#[tokio::test(start_paused = true)]
async fn default_active_limit_remains_sixty_four_after_rates_refill() {
    let regulator = defaults();
    let peers: Vec<_> = (0..3).map(|id| session(&regulator, id)).collect();
    let mut owners = Vec::new();
    for request in 0..64 {
        owners.push(peers[request % peers.len()].try_admit(1).unwrap().commit());
        tokio::time::advance(Duration::from_secs(3)).await;
    }
    assert_eq!(regulator.snapshot().node_active, 64);
    assert_blocked(&peers[0], BoundKind::NodeActive);
    owners.pop();
    let recovered = peers[0].try_admit(1).unwrap().commit();
    assert_eq!(regulator.snapshot().node_active, 64);
    drop((recovered, owners));
    assert_eq!(regulator.snapshot().node_active, 0);
    assert_eq!(regulator.snapshot().node_outstanding, 0);
}

#[tokio::test(start_paused = true)]
async fn default_session_bytes_hold_thirty_three_maximum_responses_until_writes_end() {
    let regulator = defaults();
    let peer = session(&regulator, 1);
    let mut frames = Vec::new();
    for _ in 0..33 {
        frames.push(queued_response(&peer));
        tokio::time::advance(Duration::from_secs(3)).await;
    }
    assert_eq!(regulator.snapshot().node_active, 0);
    assert_eq!(regulator.snapshot().node_outstanding, 33 * RESPONSE_BYTES);
    assert_blocked(&peer, BoundKind::PeerOutstanding);
    frames.pop();
    frames.push(queued_response(&peer));
    assert_eq!(regulator.snapshot().node_outstanding, 33 * RESPONSE_BYTES);
    drop(frames);
    assert_eq!(regulator.snapshot().node_outstanding, 0);
}

#[tokio::test(start_paused = true)]
async fn default_node_bytes_hold_one_hundred_thirty_four_maximum_responses() {
    let regulator = defaults();
    let peers: Vec<_> = (0..5).map(|id| session(&regulator, id)).collect();
    let mut frames = Vec::new();
    for request in 0..134 {
        frames.push(queued_response(&peers[request % peers.len()]));
        tokio::time::advance(Duration::from_secs(3)).await;
    }
    assert_eq!(regulator.snapshot().node_active, 0);
    assert_eq!(regulator.snapshot().node_outstanding, 134 * RESPONSE_BYTES);
    assert_blocked(&peers[4], BoundKind::NodeOutstanding);
    frames.pop();
    frames.push(queued_response(&peers[4]));
    assert_eq!(regulator.snapshot().node_outstanding, 134 * RESPONSE_BYTES);
    drop(frames);
    assert_eq!(regulator.snapshot().node_outstanding, 0);
}

#[tokio::test(start_paused = true)]
async fn default_pending_bounds_hold_sixty_four_per_session_and_one_thousand_twenty_four_total() {
    let regulator = defaults();
    let peers: Vec<_> = (0..17).map(|id| session(&regulator, id)).collect();
    let mut inputs = Vec::new();
    for peer in &peers[..16] {
        for _ in 0..64 {
            inputs.push(peer.try_retain_input(block::Height(1), 1).unwrap());
        }
        assert_eq!(
            peer.try_retain_input(block::Height(1), 1).unwrap_err().kind,
            PendingBoundKind::Session
        );
    }
    assert_eq!(regulator.snapshot().node_pending, 1024);
    assert_eq!(
        peers[16]
            .try_retain_input(block::Height(1), 1)
            .unwrap_err()
            .kind,
        PendingBoundKind::Node
    );
    assert_eq!(regulator.snapshot().session_pending, 1024);
    inputs.pop();
    inputs.push(peers[16].try_retain_input(block::Height(1), 1).unwrap());
    assert_eq!(regulator.snapshot().node_pending, 1024);
    drop(inputs);
    assert_eq!(regulator.snapshot().node_pending, 0);
    assert_eq!(regulator.snapshot().session_pending, 0);
}
