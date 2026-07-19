//! Finalized state tests.

#![allow(clippy::unwrap_in_result)]

use std::sync::atomic::AtomicU32;

use zakura_chain::block::Height;

mod prop;
mod rollback;
mod transparent;
mod vectors;

#[test]
fn checkpoint_prune_range_retains_current_height_when_range_ends_before_it() {
    let current_height = Height(9);

    assert!(
        super::checkpoint_prune_range_retains_current_height(
            current_height,
            Some((Height(1), current_height)),
        ),
        "raw transactions are still needed when the prune range ends before the current height"
    );

    assert!(
        !super::checkpoint_prune_range_retains_current_height(
            current_height,
            Some((Height(1), Height(10))),
        ),
        "raw transactions can be skipped when the prune range covers the current height"
    );

    assert!(
        !super::checkpoint_prune_range_retains_current_height(current_height, None),
        "no checkpoint prune range means there is no archive backlog to drain"
    );
}

#[test]
fn compatibility_watermark_is_an_exclusive_pruning_limit() {
    assert_eq!(
        super::compatibility_prune_until(None),
        Height(u32::MAX),
        "unconfigured compatibility mode must not change pruning"
    );

    let watermark = AtomicU32::new(0);
    assert_eq!(
        super::compatibility_prune_until(Some(&watermark)),
        Height(1)
    );

    watermark.store(42, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        super::compatibility_prune_until(Some(&watermark)),
        Height(43)
    );
}

#[test]
fn compatibility_watermark_caps_pruning_ranges() {
    assert_eq!(
        super::cap_prune_range((Height(10), Height(20)), Height(15)),
        Some((Height(10), Height(15)))
    );
    assert_eq!(
        super::cap_prune_range((Height(10), Height(20)), Height(20)),
        Some((Height(10), Height(20)))
    );
    assert_eq!(
        super::cap_prune_range((Height(10), Height(20)), Height(10)),
        None
    );
}
