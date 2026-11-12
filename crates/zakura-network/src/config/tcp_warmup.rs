//! Settings for warming new legacy TCP connections with padded pings.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::constants::{MAX_PING_PADDING, REQUEST_TIMEOUT};

/// Settings for warming the send direction of new legacy TCP connections.
///
/// A new TCP connection starts with a small congestion window, so the first full block it
/// carries takes several extra round trips. When enabled, each new connection sends a short
/// series of `ping` messages padded with zero bytes. Each ping is sent once the peer's `pong`
/// for the previous one arrives, and each is twice the size of the previous one, so the window
/// reaches block size within seconds of the handshake.
///
/// The warmth lasts only while the kernel keeps the window across idle periods, which needs
/// `net.ipv4.tcp_slow_start_after_idle = 0`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct TcpWarmupConfig {
    /// Whether new legacy TCP connections are warmed.
    pub enabled: bool,

    /// The padding bytes in the first padded ping.
    ///
    /// The default fits the first ping, with its header and nonce, in a new connection's
    /// initial window of 10 segments (about 14,000 bytes). Each padded ping then fits the
    /// window that the previous one doubled, so each takes one round trip. A larger first ping
    /// spills past the window, and every step takes two round trips.
    pub first_chunk_bytes: u32,

    /// The padding bytes in the last padded ping. Doubling stops at this size.
    ///
    /// The largest allowed value is the protocol message limit minus the 8-byte nonce.
    pub max_chunk_bytes: u32,

    /// The number of connections this node warms at the same time.
    pub max_concurrent: usize,

    /// The minimum time between two warm-ups of connections to the same peer IP.
    ///
    /// A restart forgets earlier warm-ups.
    #[serde(with = "zakura_chain::serialization::serde_adapters::duration")]
    pub peer_cooldown: Duration,

    /// How long the best chain tip must stay unchanged before a padded ping is sent, so that
    /// padding never queues ahead of a new block's relay.
    #[serde(with = "zakura_chain::serialization::serde_adapters::duration")]
    pub tip_quiet: Duration,

    /// How long a connection waits for each `pong` before it stops warming. A timeout never
    /// closes the connection.
    ///
    /// It must be less than half of [`REQUEST_TIMEOUT`], the time a connection allows for each
    /// write. Each ping is twice the size of the last one, so a peer that answered the last
    /// one within this time takes at most about twice this time to accept the next one.
    #[serde(with = "zakura_chain::serialization::serde_adapters::duration")]
    pub chunk_timeout: Duration,
}

impl Default for TcpWarmupConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            first_chunk_bytes: 14_000,
            max_chunk_bytes: MAX_PING_PADDING,
            max_concurrent: 16,
            peer_cooldown: Duration::from_secs(24 * 60 * 60),
            tip_quiet: Duration::from_secs(2),
            chunk_timeout: Duration::from_secs(5),
        }
    }
}

impl TcpWarmupConfig {
    /// Checks that the settings describe a warm-up that peers accept and that never fails a
    /// connection.
    pub fn validate(&self) -> Result<(), String> {
        if self.first_chunk_bytes == 0 {
            return Err("network.tcp_warmup.first_chunk_bytes must be greater than 0".into());
        }
        if self.first_chunk_bytes > self.max_chunk_bytes {
            return Err(
                "network.tcp_warmup.first_chunk_bytes must not exceed max_chunk_bytes".into(),
            );
        }
        if self.max_chunk_bytes > MAX_PING_PADDING {
            return Err(format!(
                "network.tcp_warmup.max_chunk_bytes must be at most {MAX_PING_PADDING}, \
                 because peers disconnect on larger messages"
            ));
        }
        if self.max_concurrent == 0 {
            return Err("network.tcp_warmup.max_concurrent must be greater than 0".into());
        }
        if self.chunk_timeout.is_zero() || self.chunk_timeout * 2 >= REQUEST_TIMEOUT {
            return Err(format!(
                "network.tcp_warmup.chunk_timeout must be greater than 0 and less than {:?}, \
                 so that writing the next ping never times out the connection",
                REQUEST_TIMEOUT / 2
            ));
        }
        Ok(())
    }

    /// Returns the padding of each ping in a warm-up, in sending order.
    ///
    /// The first ping has no padding. Its `pong` shows that the peer has finished the
    /// handshake and accepts full-size messages. The padded pings then double from
    /// `first_chunk_bytes` and end with one ping of `max_chunk_bytes`.
    pub fn schedule(&self) -> Vec<u32> {
        let mut schedule = vec![0];
        let mut padding = self.first_chunk_bytes;
        while padding < self.max_chunk_bytes {
            schedule.push(padding);
            padding = padding.saturating_mul(2);
        }
        schedule.push(self.max_chunk_bytes);
        schedule
    }
}
