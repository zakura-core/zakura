//! Settings for warming new legacy TCP connections with padded pings.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::constants::{MAX_PING_PADDING, REQUEST_TIMEOUT};

/// Settings for warming the send direction of new legacy TCP connections.
///
/// A new TCP connection starts with a small congestion window. When enabled, the connection
/// sends padded `ping` messages so that the window can grow before the first full block. The
/// default schedule sends 2.4 MB of padding in two pings. The resulting congestion window and
/// first-block latency depend on the TCP sender, the receiver's window, and network conditions.
///
/// The warmth lasts only while the kernel keeps the window across idle periods, which needs
/// `net.ipv4.tcp_slow_start_after_idle = 0`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(from = "TcpWarmupConfigInput")]
pub struct TcpWarmupConfig {
    /// Whether new legacy TCP connections are warmed.
    pub enabled: bool,

    /// The padding bytes in the first padded ping.
    ///
    /// If only [`Self::max_chunk_bytes`] is set, the first ping uses the historical 14,000 bytes.
    pub first_chunk_bytes: u32,

    /// The padding bytes in the last padded ping. If it equals
    /// [`Self::first_chunk_bytes`], the warm-up sends only one padded ping.
    ///
    /// The largest allowed value is the protocol message limit minus the 8-byte nonce. If only
    /// [`Self::first_chunk_bytes`] is set, the last ping uses this limit.
    pub max_chunk_bytes: u32,

    /// The number of connections this node warms at the same time, including writes still
    /// finishing after a warm-up stops waiting.
    pub max_concurrent: usize,

    /// The minimum time between two warm-ups of connections to the same peer IP.
    ///
    /// A restart forgets earlier warm-ups.
    #[serde(with = "zakura_chain::serialization::serde_adapters::duration")]
    pub peer_cooldown: Duration,

    /// How long the best chain tip must stay unchanged before a padded ping is queued. This
    /// gives a new block time to relay before more padding is sent.
    #[serde(with = "zakura_chain::serialization::serde_adapters::duration")]
    pub tip_quiet: Duration,

    /// How long the warm-up waits for each `pong`. An already started write still has its own
    /// connection send deadline.
    ///
    /// It must be less than half of [`REQUEST_TIMEOUT`], the time a connection allows for each
    /// write. Each padded ping is at most twice the previous padded ping's size, limiting the
    /// increase in write time when the connection's speed holds steady.
    #[serde(with = "zakura_chain::serialization::serde_adapters::duration")]
    pub chunk_timeout: Duration,
}

impl Default for TcpWarmupConfig {
    fn default() -> Self {
        TcpWarmupConfigInput::default().into()
    }
}

/// Retains which ping sizes were supplied so partial configs keep their historical defaults.
#[derive(Deserialize)]
#[serde(deny_unknown_fields, default)]
struct TcpWarmupConfigInput {
    enabled: bool,
    first_chunk_bytes: Option<u32>,
    max_chunk_bytes: Option<u32>,
    max_concurrent: usize,
    #[serde(with = "zakura_chain::serialization::serde_adapters::duration")]
    peer_cooldown: Duration,
    #[serde(with = "zakura_chain::serialization::serde_adapters::duration")]
    tip_quiet: Duration,
    #[serde(with = "zakura_chain::serialization::serde_adapters::duration")]
    chunk_timeout: Duration,
}

impl Default for TcpWarmupConfigInput {
    fn default() -> Self {
        Self {
            enabled: false,
            first_chunk_bytes: None,
            max_chunk_bytes: None,
            max_concurrent: 16,
            peer_cooldown: Duration::from_secs(24 * 60 * 60),
            tip_quiet: Duration::from_secs(2),
            chunk_timeout: Duration::from_secs(5),
        }
    }
}

impl From<TcpWarmupConfigInput> for TcpWarmupConfig {
    fn from(input: TcpWarmupConfigInput) -> Self {
        let (first_chunk_bytes, max_chunk_bytes) =
            match (input.first_chunk_bytes, input.max_chunk_bytes) {
                (None, None) => (800_000, 1_600_000),
                (first, max) => (first.unwrap_or(14_000), max.unwrap_or(MAX_PING_PADDING)),
            };
        Self {
            enabled: input.enabled,
            first_chunk_bytes,
            max_chunk_bytes,
            max_concurrent: input.max_concurrent,
            peer_cooldown: input.peer_cooldown,
            tip_quiet: input.tip_quiet,
            chunk_timeout: input.chunk_timeout,
        }
    }
}

impl TcpWarmupConfig {
    /// Checks the size, concurrency, and timeout limits for a warm-up.
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
                 to leave time for the next, larger write",
                REQUEST_TIMEOUT / 2
            ));
        }
        Ok(())
    }

    /// Returns the padding of each ping in a warm-up, in sending order.
    ///
    /// The first ping has no padding. Its `pong` shows that the peer can answer a ping. The
    /// padded pings double from `first_chunk_bytes` up to `max_chunk_bytes`.
    pub fn schedule(&self) -> Vec<u32> {
        let mut schedule = vec![0];
        let mut padding = self.first_chunk_bytes;
        while padding > 0 && padding < self.max_chunk_bytes {
            schedule.push(padding);
            padding = padding.saturating_mul(2);
        }
        schedule.push(self.max_chunk_bytes);
        schedule
    }
}
