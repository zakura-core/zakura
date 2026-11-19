//! Settings for warming new legacy TCP connections with padded pings.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::constants::{MAX_PING_PADDING, REQUEST_TIMEOUT};

/// Settings for warming the send direction of new legacy TCP connections.
///
/// A new TCP connection starts with a small congestion window, so the first full block it
/// carries takes several extra round trips. When enabled, each new connection sends two `ping`
/// messages padded with zero bytes, the second once the peer's `pong` for the first arrives.
/// In slow start the window grows by the bytes the peer acknowledges, so the defaults, 2.4 MB
/// in total, grow it past a full 2 MB block within seconds of the handshake. A ping can carry
/// at most about 2 MiB, so a warm-up larger than that needs two.
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
    /// A peer too slow to answer it within [`Self::chunk_timeout`] is not sent the second.
    pub first_chunk_bytes: u32,

    /// The padding bytes in the second and last padded ping. If it equals
    /// [`Self::first_chunk_bytes`], the warm-up sends only one padded ping.
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
    /// write. With the defaults the second ping is twice the size of the first, so a peer whose
    /// link stays as fast as its first answer showed takes at most about twice this time to
    /// accept the second.
    #[serde(with = "zakura_chain::serialization::serde_adapters::duration")]
    pub chunk_timeout: Duration,
}

impl Default for TcpWarmupConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            first_chunk_bytes: 800_000,
            max_chunk_bytes: 1_600_000,
            max_concurrent: 16,
            peer_cooldown: Duration::from_secs(24 * 60 * 60),
            tip_quiet: Duration::from_secs(2),
            chunk_timeout: Duration::from_secs(5),
        }
    }
}

impl TcpWarmupConfig {
    /// Checks that the settings describe a warm-up that peers accept and that fits each ping
    /// inside the connection's send timeout.
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
                 so that each ping fits inside the connection's send timeout",
                REQUEST_TIMEOUT / 2
            ));
        }
        Ok(())
    }

    /// Returns the padding of each ping in a warm-up, in sending order.
    ///
    /// The first ping has no padding. Its `pong` shows that the peer has finished the
    /// handshake and accepts full-size messages. Then come a ping of `first_chunk_bytes` and,
    /// if it is larger, a ping of `max_chunk_bytes`.
    pub fn schedule(&self) -> Vec<u32> {
        let mut schedule = vec![0, self.first_chunk_bytes];
        if self.max_chunk_bytes > self.first_chunk_bytes {
            schedule.push(self.max_chunk_bytes);
        }
        schedule
    }
}
