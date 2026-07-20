//! Inbound service tests.

use std::{
    net::{IpAddr, Ipv4Addr},
    time::{Duration, Instant},
};

use super::{canonical_ip, PrunedBlockNotFoundLogger, ZCASHD_COMPAT_PRUNED_BLOCK_LOG_INTERVAL};

mod fake_peer_set;
mod real_peer_set;

#[test]
fn pruned_block_not_found_log_is_rate_limited() {
    let logger = PrunedBlockNotFoundLogger::new(Some(10_000), Vec::new());
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
    let logger = PrunedBlockNotFoundLogger::new(None, Vec::new());

    assert_eq!(logger.reserve_log_at(Instant::now()), None);
}

#[test]
fn pruned_block_not_found_peer_ips_canonicalize_mapped_ipv6() {
    let ipv4 = Ipv4Addr::new(192, 0, 2, 1);

    assert_eq!(
        canonical_ip(IpAddr::V6(ipv4.to_ipv6_mapped())),
        IpAddr::V4(ipv4)
    );
}
