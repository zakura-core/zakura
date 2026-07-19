//! Inbound service tests.

use std::time::{Duration, Instant};

use super::{PrunedBlockNotFoundLogger, ZCASHD_COMPAT_PRUNED_BLOCK_LOG_INTERVAL};

mod fake_peer_set;
mod real_peer_set;

#[test]
fn pruned_block_not_found_log_is_rate_limited() {
    let logger = PrunedBlockNotFoundLogger::new(Some(10_000));
    let start = Instant::now();

    assert_eq!(logger.reserve_log_at(start), Some(10_000));
    assert_eq!(logger.reserve_log_at(start + Duration::from_secs(1)), None);
    assert_eq!(
        logger.reserve_log_at(start + ZCASHD_COMPAT_PRUNED_BLOCK_LOG_INTERVAL),
        Some(10_000)
    );
}

#[test]
fn pruned_block_not_found_log_is_disabled_without_compat_pruning() {
    let logger = PrunedBlockNotFoundLogger::new(None);

    assert_eq!(logger.reserve_log_at(Instant::now()), None);
}
