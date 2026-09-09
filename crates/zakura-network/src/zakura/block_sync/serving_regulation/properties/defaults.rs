//! Default response and producer bounds, independent of production accounting.

use super::super::*;
use std::time::Duration;

const RESPONSE_BYTES: u64 = 2_000_010;

fn defaults() -> GetBlocksServingRegulator {
    let config = ZakuraBlockSyncConfig::default();
    validate_config(&config).unwrap();
    GetBlocksServingRegulator::new(config)
}

fn session(regulator: &GetBlocksServingRegulator, identity: u8) -> GetBlocksServingSession {
    regulator.session(ZakuraPeerId::new(vec![identity; 32]).unwrap())
}

fn queued_response(session: &GetBlocksServingSession) -> FrameGuard {
    let mut permit = session.admit_now(1).unwrap().commit();
    permit.frame_guard(RESPONSE_BYTES)
}

#[test]
fn default_response_count_caps_large_requests_at_one_block() {
    let config = ZakuraBlockSyncConfig::default();
    for count in [1, 128, u32::MAX] {
        let cap = GetBlocksPolicy::new(&config)
            .response_cap_for_count(count)
            .unwrap();
        assert_eq!(config.initial_status().max_blocks_per_response, 1);
        assert_eq!(cap, RESPONSE_BYTES);
    }
}

#[test]
fn producer_waits_for_both_query_and_writer_owners() {
    let regulator = defaults();
    let peer = session(&regulator, 1);
    let mut permit = peer.admit_now(1).unwrap().commit();
    let query = permit.work_lease();
    assert!(query.try_start());
    let frame = permit.frame_guard(9);
    drop(permit);
    drop(frame);
    assert_eq!(regulator.snapshot().node_active, 1);
    assert!(peer.admit_now(1).is_none());
    drop(query);
    assert!(peer.admit_now(1).is_some());
}

#[tokio::test(start_paused = true)]
async fn default_producer_limits_hold_until_writes_finish() {
    let regulator = defaults();
    let peers: Vec<_> = (0..65).map(|id| session(&regulator, id)).collect();
    let mut frames = Vec::new();
    for peer in &peers[..64] {
        frames.push(queued_response(peer));
    }
    tokio::time::advance(Duration::from_secs(60)).await;
    let before = regulator.snapshot();
    assert_eq!(before.node_active, 64);
    assert!(peers[0].admit_now(1).is_none());
    assert!(peers[64].admit_now(1).is_none());
    assert_eq!(regulator.snapshot(), before);
    frames.pop();
    frames.push(queued_response(&peers[64]));
    assert_eq!(regulator.snapshot().node_active, 64);
    drop(frames);
    assert_eq!(regulator.snapshot().node_active, 0);
}
