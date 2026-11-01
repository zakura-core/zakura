//! Capacity defaults for encoded output, response bookkeeping, and execution.
//!
//! Byte and execution defaults derive from the target below. Response slots
//! separately bound bookkeeping for tiny responses. A local capacity limit
//! makes this node wait; it never faults a peer.
//!
//! **Target:** 10 Gbps per connection at 500 ms round-trip time, a
//! bandwidth-delay product of 625 MB.
//!
//! **Transport ceiling.** QUIC flow control binds first today. A connection's
//! send window is [`DEFAULT_ZAKURA_SEND_WINDOW`] (32 MiB), which carries about
//! 540 Mbps at 500 ms, and reaches 10 Gbps only below about 27 ms. The
//! budgets here are at least twice the transport's windows, so the transport
//! stays the byte limit until its windows grow. Response slots can limit tiny
//! responses sooner; adoption must measure their throughput.

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

/// Unsent response bytes the whole node may hold for `peers` serving peers
/// that may each hold `peer_bytes`: all of it at once, so a conformant peer
/// can't pause another one. A grant reserves a response's whole cap until the
/// write, so peers that read at full speed still hold their budget.
///
/// This bounds encoded bytes, not allocator or queue overhead. Response slots
/// separately bound per-response bookkeeping. A non-reading peer's frames
/// stay queued until its stream's write deadline. The node budget can only
/// bind on responses of a session that released its slot.
pub(crate) const fn node_output_bytes(peers: usize, peer_bytes: u64) -> u64 {
    let peers = if peers == 0 { 1 } else { peers };
    // Widening usize to u64 is lossless on supported targets.
    peer_bytes.saturating_mul(peers as u64)
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

/// Serving limits for at most `peers` serving peers and responses of at most
/// `largest_response` bytes that take `produce_time` each.
///
/// One peer may use half the node's execution slots: the node default is
/// already twice what one connection needs, so a single peer still reaches
/// the target, and a stalled peer cannot hold every slot.
pub(crate) const fn serve_limits(
    largest_response: u64,
    produce_time: Duration,
    peers: usize,
) -> ServeLimits {
    let node_execution = node_execution(largest_response, produce_time);
    let peer_bytes = max(peer_output_bytes(), largest_response);
    let node_bytes = node_output_bytes(peers, peer_bytes);
    ServeLimits {
        node_output_responses: output_responses(node_bytes),
        peer_output_responses: output_responses(peer_bytes),
        node_execution,
        peer_execution: node_execution.div_ceil(2),
        peer_output_bytes: peer_bytes,
        node_output_bytes: node_bytes,
    }
}

/// One queued response per 4 KiB of output budget, rounded up.
///
/// This count limits bookkeeping independently of encoded bytes. The 4 KiB
/// ratio is a sizing policy, not an estimate of an allocator's exact overhead.
/// Frames within a response remain bounded by its frame and byte caps.
const fn output_responses(bytes: u64) -> usize {
    let slots = bytes.div_ceil(4096);
    // Clamp before narrowing on targets where usize is smaller than u64.
    if slots > usize::MAX as u64 {
        usize::MAX
    } else {
        slots as usize
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
            let limits = serve_limits(largest, produce_time, 92);
            assert!(limits.node_execution >= node_execution(largest, produce_time));
            // One peer alone still gets the undoubled Little's-law count.
            assert!(2 * limits.peer_execution >= limits.node_execution);
            assert!(limits.peer_output_bytes >= peer_output_bytes());
            assert!(limits.peer_output_bytes >= largest);
            assert_eq!(limits.node_output_bytes, 92 * limits.peer_output_bytes);
            assert!(limits.node_output_bytes >= largest);
        }
    }

    #[test]
    fn response_counts_bound_bookkeeping_separately_from_encoded_bytes() {
        assert_eq!(output_responses(1), 1);
        assert_eq!(output_responses(4096), 1);
        assert_eq!(output_responses(4097), 2);
        let limits = serve_limits(12, Duration::from_micros(1), 2);
        assert_eq!(limits.peer_output_responses, 16_384);
        assert_eq!(limits.node_output_responses, 32_768);
    }

    #[test]
    fn no_peers_still_fund_one_peers_output() {
        let limits = serve_limits(12, Duration::from_micros(1), 0);
        assert_eq!(limits.node_output_bytes, limits.peer_output_bytes);
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
