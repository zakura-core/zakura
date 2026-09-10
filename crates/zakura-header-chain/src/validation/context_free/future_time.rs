use chrono::{DateTime, Utc};
use zakura_chain::block;

/// Apply the shared local two-hour future-time rule at an explicit injected clock value.
pub fn validate_future_time(
    header: &block::Header,
    now: DateTime<Utc>,
    height: block::Height,
    hash: block::Hash,
) -> Result<(), block::BlockTimeError> {
    header.time_is_valid_at(now, &height, &hash)
}
