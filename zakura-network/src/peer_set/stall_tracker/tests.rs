//! Unit tests for [`FindResponseStallTracker`].

use super::*;

fn locator(value: u8) -> block::Hash {
    [value; 32].into()
}

#[test]
fn disconnects_after_threshold() {
    let mut tracker = FindResponseStallTracker::new();
    let generation = 1;
    let locator = locator(1);

    assert!(!tracker.record_stall(generation, locator));
    assert!(!tracker.record_stall(generation, locator));

    // Third stall: at threshold.
    assert!(tracker.record_stall(generation, locator));

    // Entry cleared on threshold — next stall starts fresh.
    assert!(!tracker.record_stall(generation, locator));
}

#[test]
fn clear_resets_count() {
    let mut tracker = FindResponseStallTracker::new();
    let generation = 1;
    let locator = locator(1);

    assert!(!tracker.record_stall(generation, locator));
    assert!(!tracker.record_stall(generation, locator));

    tracker.clear();

    // Back to zero: needs a full threshold's worth of stalls again.
    assert!(!tracker.record_stall(generation, locator));
    assert!(!tracker.record_stall(generation, locator));
    assert!(tracker.record_stall(generation, locator));
}

#[test]
fn independent_per_peer() {
    let mut tracker = FindResponseStallTracker::new();
    let locator = locator(1);

    assert!(!tracker.record_stall(1, locator));
    assert!(!tracker.record_stall(1, locator));
    assert!(!tracker.record_stall(2, locator));
    assert!(tracker.record_stall(1, locator));

    assert!(!tracker.record_stall(2, locator));
    assert!(tracker.record_stall(2, locator));
}

#[test]
fn locator_and_generation_changes_start_fresh() {
    let mut tracker = FindResponseStallTracker::new();

    assert!(!tracker.record_stall(1, locator(1)));
    assert!(!tracker.record_stall(1, locator(1)));
    assert!(!tracker.record_stall(1, locator(2)));
    assert!(!tracker.record_stall(2, locator(1)));

    tracker.clear_generation(1);
    assert!(!tracker.record_stall(1, locator(1)));
}
