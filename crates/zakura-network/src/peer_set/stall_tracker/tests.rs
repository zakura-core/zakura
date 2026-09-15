//! Unit tests for [`FindResponseStallTracker`].

use super::*;

fn test_addr(last_octet: u8) -> PeerSocketAddr {
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    SocketAddr::V4(SocketAddrV4::new(
        Ipv4Addr::new(127, 0, 0, last_octet),
        8233,
    ))
    .into()
}

#[test]
fn disconnects_after_threshold() {
    let mut tracker = FindResponseStallTracker::new();
    let addr = test_addr(1);

    assert!(!tracker.record_stall(addr));
    assert!(!tracker.record_stall(addr));

    // Third stall: at threshold.
    assert!(tracker.record_stall(addr));

    // Entry cleared on threshold — next stall starts fresh.
    assert!(!tracker.record_stall(addr));
}

#[test]
fn clear_resets_count() {
    let mut tracker = FindResponseStallTracker::new();
    let addr = test_addr(1);

    assert!(!tracker.record_stall(addr));
    assert!(!tracker.record_stall(addr));

    tracker.clear(addr);

    // Back to zero: needs a full threshold's worth of stalls again.
    assert!(!tracker.record_stall(addr));
    assert!(!tracker.record_stall(addr));
    assert!(tracker.record_stall(addr));
}

#[test]
fn independent_per_peer() {
    let mut tracker = FindResponseStallTracker::new();
    let addr_a = test_addr(1);
    let addr_b = test_addr(2);

    assert!(!tracker.record_stall(addr_a));
    assert!(!tracker.record_stall(addr_a));
    assert!(!tracker.record_stall(addr_b));
    assert!(tracker.record_stall(addr_a));

    assert!(!tracker.record_stall(addr_b));
    assert!(tracker.record_stall(addr_b));
}

fn drain(tracker: &mut FindResponseStallTracker) -> Vec<PeerSocketAddr> {
    tracker.drain(&Context::from_waker(futures::task::noop_waker_ref()))
}

#[tokio::test]
async fn feedback_orders_progress_before_newer_stalls() {
    let mut tracker = FindResponseStallTracker::new();
    let addr = test_addr(1);
    tracker.connection(addr, 1);
    assert!(!tracker.record_stall(addr));
    let earlier = tracker.start(addr).unwrap();
    let later = tracker.start(addr).unwrap();
    later.no_progress();
    assert!(drain(&mut tracker).is_empty());
    earlier.verified();
    assert!(drain(&mut tracker).is_empty());
    assert_eq!(tracker.counts[&addr], 1);
}

#[tokio::test]
async fn cancelling_the_last_clone_unblocks_ordering_without_clearing_stalls() {
    let mut tracker = FindResponseStallTracker::new();
    let addr = test_addr(1);
    tracker.connection(addr, 1);
    tracker.record_stall(addr);
    let first = tracker.start(addr).unwrap();
    let clone = first.clone();
    let second = tracker.start(addr).unwrap();
    second.no_progress();
    drop(first);
    assert!(drain(&mut tracker).is_empty());
    drop(clone);
    assert!(drain(&mut tracker).is_empty());
    assert_eq!(tracker.counts[&addr], 2);
}

#[tokio::test(start_paused = true)]
async fn retained_feedback_expires_and_replacement_ignores_old_completion() {
    let mut tracker = FindResponseStallTracker::new();
    let addr = test_addr(1);
    tracker.connection(addr, 1);
    let first = tracker.start(addr).unwrap();
    let second = tracker.start(addr).unwrap();
    assert!(tracker.start(addr).is_none());
    assert!(
        tracker.eligible(&addr),
        "feedback capacity must not prevent completing a checkpoint range"
    );
    tokio::time::advance(FEEDBACK_LIFETIME).await;
    assert!(drain(&mut tracker).is_empty());
    assert!(!tracker.eligible(&addr));
    tracker.connection(addr, 2);
    tracker.record_stall(addr);
    first.verified();
    second.no_progress();
    assert!(drain(&mut tracker).is_empty());
    assert_eq!(tracker.counts[&addr], 1);
    assert!(tracker.eligible(&addr));
}

#[tokio::test(start_paused = true)]
async fn consumer_expiry_rotates_without_striking_or_clearing_stalls() {
    let mut tracker = FindResponseStallTracker::new();
    let addr = test_addr(1);
    tracker.connection(addr, 1);
    tracker.record_stall(addr);
    let feedback = tracker.start(addr).unwrap();
    feedback.expired();
    drop(feedback);
    assert!(drain(&mut tracker).is_empty());
    assert!(!tracker.eligible(&addr));
    assert_eq!(tracker.counts[&addr], 1);
    tokio::time::advance(REPROBE_DELAY).await;
    assert!(tracker.eligible(&addr));
}

#[tokio::test(start_paused = true)]
async fn verified_completion_wins_against_later_expiry() {
    let mut tracker = FindResponseStallTracker::new();
    let addr = test_addr(1);
    tracker.connection(addr, 1);
    tracker.record_stall(addr);
    let feedback = tracker.start(addr).unwrap();
    feedback.verified();
    feedback.expired();
    tokio::time::advance(FEEDBACK_LIFETIME).await;
    assert!(drain(&mut tracker).is_empty());
    assert!(tracker.eligible(&addr));
    assert!(!tracker.counts.contains_key(&addr));
}
