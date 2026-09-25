//! Capacity defaults, derived from one throughput target.
//!
//! Limits must never cost throughput. Every capacity default is derived from
//! the target below, never from a memory guess, and a test checks each
//! default against its derivation. A local capacity limit makes this node
//! wait; it never faults a peer.
//!
//! **Target:** 10 Gbps per connection at 500 ms round-trip time, a
//! bandwidth-delay product of 625 MB.
//!
//! **Transport ceiling.** QUIC flow control binds first today. A connection's
//! send window is [`DEFAULT_ZAKURA_SEND_WINDOW`] (32 MiB), which carries about
//! 540 Mbps at 500 ms, and reaches 10 Gbps only below about 27 ms. The
//! budgets here are at least twice the transport's windows, so the transport
//! stays the binding limit until its windows grow.

use std::time::Duration;

use super::ServeLimits;
use crate::zakura::DEFAULT_ZAKURA_SEND_WINDOW;

/// Target throughput of one connection, in bits per second.
pub(crate) const TARGET_BITS_PER_SECOND: u64 = 10_000_000_000;

/// Round-trip time at which one connection must reach the target.
pub(crate) const TARGET_RTT: Duration = Duration::from_millis(500);

/// Bytes in flight on one connection at the target: 625 MB.
pub(crate) const fn bandwidth_delay_bytes() -> u64 {
    // Widening u128 milliseconds to u64 is lossless for a 500 ms target.
    TARGET_BITS_PER_SECOND / 8 * (TARGET_RTT.as_millis() as u64) / 1000
}

/// Unsent response bytes one peer may hold: twice the connection's send
/// window, so the transport, not the toolkit, limits one connection.
pub(crate) const fn peer_output_bytes() -> u64 {
    2 * DEFAULT_ZAKURA_SEND_WINDOW
}

/// Unsent response bytes the whole node may hold: twice the bandwidth-delay
/// product, so the node can fill the target link.
///
/// This is a memory bound as well: frames a non-reading peer has not taken
/// stay queued until its stream's write deadline. Serving fairness between
/// peers that stop reading belongs to prioritization, not to this bound.
pub(crate) const fn node_output_bytes() -> u64 {
    2 * bandwidth_delay_bytes()
}

/// Node execution slots for responses of `response_bytes` that take
/// `produce_time` each.
///
/// By Little's law, filling the target link needs
/// `target bytes per second × produce_time / response_bytes` concurrent
/// `produce` steps. The default doubles it.
///
/// # Panics
///
/// If `response_bytes` is zero.
pub(crate) const fn node_execution(response_bytes: u64, produce_time: Duration) -> usize {
    let bytes_per_second = (TARGET_BITS_PER_SECOND / 8) as u128;
    let running = (bytes_per_second * produce_time.as_nanos())
        .div_ceil(1_000_000_000 * response_bytes as u128);
    // Widening casts above are lossless. The minimum is one slot, and the
    // clamp keeps the narrowing cast lossless.
    let doubled = if running == 0 { 1 } else { running * 2 };
    if doubled > usize::MAX as u128 {
        usize::MAX
    } else {
        doubled as usize
    }
}

/// Serving limits for responses of at most `largest_response` bytes that
/// take `produce_time` each.
///
/// One peer may use half the node's execution slots: the node default is
/// already twice what one connection needs, so a single peer still reaches
/// the target, and a stalled peer cannot hold every slot.
pub(crate) const fn serve_limits(largest_response: u64, produce_time: Duration) -> ServeLimits {
    let node_execution = node_execution(largest_response, produce_time);
    ServeLimits {
        node_execution,
        peer_execution: node_execution.div_ceil(2),
        peer_output_bytes: max(peer_output_bytes(), largest_response),
        node_output_bytes: max(node_output_bytes(), largest_response),
    }
}

/// Requester reservation entries for requests whose responses reserve
/// `reserved_bytes` each: enough to keep twice the bandwidth-delay product
/// reserved.
///
/// # Panics
///
/// If `reserved_bytes` is zero.
pub(crate) const fn reservation_entries(reserved_bytes: u64) -> usize {
    let entries = (2 * bandwidth_delay_bytes()).div_ceil(reserved_bytes);
    // A u64 count fits usize on the 64-bit targets this node supports; the
    // clamp keeps the cast lossless elsewhere.
    if entries > usize::MAX as u64 {
        usize::MAX
    } else {
        entries as usize
    }
}

const fn max(a: u64, b: u64) -> u64 {
    if a > b {
        a
    } else {
        b
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_target_is_625_megabytes_in_flight() {
        assert_eq!(bandwidth_delay_bytes(), 625_000_000);
    }

    #[test]
    fn the_transport_window_binds_before_the_toolkit() {
        // 32 MiB per 500 ms is about 537 Mbps.
        let window_bits = DEFAULT_ZAKURA_SEND_WINDOW * 8;
        // Widening u128 milliseconds to u64 is lossless for a 500 ms target.
        let rtt_ms = TARGET_RTT.as_millis() as u64;
        assert_eq!(window_bits * 1000 / rtt_ms / 1_000_000, 536);
        assert!(peer_output_bytes() >= 2 * DEFAULT_ZAKURA_SEND_WINDOW);
    }

    #[test]
    fn execution_follows_littles_law_with_twice_the_margin() {
        // 1.25 GB/s for 10 ms is 12.5 MB in flight: 12 one-megabyte responses
        // round up to 13, doubled to 26.
        assert_eq!(node_execution(1_000_000, Duration::from_millis(10)), 26);
        assert_eq!(node_execution(u64::MAX, Duration::ZERO), 1);
    }

    #[test]
    fn serve_limits_meet_every_minimum() {
        for (largest, produce_time) in [
            (1, Duration::from_micros(1)),
            (8 * 1024, Duration::from_millis(1)),
            (32 << 20, Duration::from_millis(50)),
            (4 << 30, Duration::from_secs(1)),
        ] {
            let limits = serve_limits(largest, produce_time);
            assert!(limits.node_execution >= node_execution(largest, produce_time));
            // One peer alone still gets the undoubled Little's-law count.
            assert!(2 * limits.peer_execution >= limits.node_execution);
            assert!(limits.peer_output_bytes >= peer_output_bytes());
            assert!(limits.peer_output_bytes >= largest);
            assert!(limits.node_output_bytes >= node_output_bytes());
            assert!(limits.node_output_bytes >= largest);
        }
    }

    #[test]
    fn reservations_keep_twice_the_bandwidth_delay_product_reserved() {
        for reserved in [1, 8 * 1024, 32 << 20] {
            let entries = reservation_entries(reserved) as u64;
            assert!(entries * reserved >= 2 * bandwidth_delay_bytes());
            assert!((entries - 1) * reserved < 2 * bandwidth_delay_bytes());
        }
    }
}
