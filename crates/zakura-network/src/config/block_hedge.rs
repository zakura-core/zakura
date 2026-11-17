//! Settings for a second request when a gossiped legacy block is slow to arrive.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// The shortest allowed [`BlockHedgeConfig::delay`].
pub const MIN_BLOCK_HEDGE_DELAY: Duration = Duration::from_millis(5);

/// The longest allowed [`BlockHedgeConfig::delay`].
pub const MAX_BLOCK_HEDGE_DELAY: Duration = Duration::from_secs(5);

/// Settings for hedging the download of a block a legacy peer announced.
///
/// A gossiped block is requested a few milliseconds after its first `inv`, when the first peer
/// that announced it is usually the only candidate. That peer may be far away, while closer peers
/// announce the same block a few milliseconds later. When enabled, a block that hasn't arrived
/// `delay` after it was requested is also requested from one other peer that announced it, has no
/// request outstanding, and answered our last heartbeat ping fastest. The first copy to arrive
/// is used.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct BlockHedgeConfig {
    /// Whether slow gossiped block downloads get a second request.
    pub enabled: bool,

    /// How long to wait for the first request before sending the second.
    #[serde(with = "zakura_chain::serialization::serde_adapters::duration")]
    pub delay: Duration,
}

impl Default for BlockHedgeConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            delay: Duration::from_millis(50),
        }
    }
}

impl BlockHedgeConfig {
    /// Checks that the delay is in the supported range.
    pub fn validate(&self) -> Result<(), String> {
        if self.delay < MIN_BLOCK_HEDGE_DELAY || self.delay > MAX_BLOCK_HEDGE_DELAY {
            return Err(format!(
                "network.block_hedge.delay must be between {MIN_BLOCK_HEDGE_DELAY:?} and \
                 {MAX_BLOCK_HEDGE_DELAY:?}"
            ));
        }
        Ok(())
    }

    /// The delay before the second request, or `None` when hedging is disabled.
    pub fn enabled_delay(&self) -> Option<Duration> {
        self.enabled.then_some(self.delay)
    }
}
