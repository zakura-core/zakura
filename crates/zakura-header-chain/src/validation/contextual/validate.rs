use chrono::{DateTime, Duration, Utc};
use thiserror::Error;
use zakura_chain::{
    parameters::{Network, NetworkUpgrade},
    work::difficulty::{CompactDifficulty, ParameterDifficulty as _},
};

use super::{AdjustedDifficulty, BLOCK_MAX_TIME_SINCE_MEDIAN};

/// Contextual candidate time or difficulty failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ContextualValidationError {
    /// Candidate time is not strictly greater than median-time-past.
    #[error("block time {candidate_time:?} is less than or equal to median-time-past {median_time_past:?}")]
    TimeTooEarly {
        /// Candidate header time.
        candidate_time: DateTime<Utc>,
        /// Median of the preceding header times.
        median_time_past: DateTime<Utc>,
    },
    /// Candidate time exceeds the active 90-minute median-time-past limit.
    #[error("block time {candidate_time:?} exceeds maximum {block_time_max:?}")]
    TimeTooLate {
        /// Candidate header time.
        candidate_time: DateTime<Utc>,
        /// Inclusive maximum candidate time.
        block_time_max: DateTime<Utc>,
    },
    /// Candidate compact target does not match the contextual expected target.
    #[error(
        "block difficulty {difficulty_threshold:?} does not match expected {expected_difficulty:?}"
    )]
    InvalidDifficultyThreshold {
        /// Candidate header compact target.
        difficulty_threshold: CompactDifficulty,
        /// Compact target calculated from the branch-local context.
        expected_difficulty: CompactDifficulty,
    },
}

/// Validate contextual median-time and compact-target rules using exact branch-local context.
pub fn validate_contextual_difficulty_and_time(
    difficulty_threshold: CompactDifficulty,
    difficulty_adjustment: AdjustedDifficulty,
) -> Result<(), ContextualValidationError> {
    let candidate_height = difficulty_adjustment.candidate_height();
    let candidate_time = difficulty_adjustment.candidate_time();
    let network = difficulty_adjustment.network();
    let median_time_past = difficulty_adjustment.median_time_past();
    let block_time_max = median_time_past + Duration::seconds(BLOCK_MAX_TIME_SINCE_MEDIAN.into());

    let genesis_height = NetworkUpgrade::Genesis
        .activation_height(&network)
        .expect("Zakura always has a genesis height available");

    if candidate_time <= median_time_past && candidate_height != genesis_height {
        return Err(ContextualValidationError::TimeTooEarly {
            candidate_time,
            median_time_past,
        });
    }

    // The production Mainnet consensus rule starts at height 2. Configured
    // Testnet and Regtest networks can activate it at height 1.
    let is_mainnet_height_one = matches!(network, Network::Mainnet) && candidate_height.0 == 1;
    if candidate_height != genesis_height
        && !is_mainnet_height_one
        && network.is_max_block_time_enforced(candidate_height)
        && candidate_time > block_time_max
    {
        return Err(ContextualValidationError::TimeTooLate {
            candidate_time,
            block_time_max,
        });
    }

    if network.disable_pow() {
        if difficulty_threshold.to_work().is_none() {
            return Err(ContextualValidationError::InvalidDifficultyThreshold {
                difficulty_threshold,
                expected_difficulty: network.target_difficulty_limit().to_compact(),
            });
        }
    } else {
        let expected_difficulty = difficulty_adjustment.expected_difficulty_threshold();
        if difficulty_threshold != expected_difficulty {
            return Err(ContextualValidationError::InvalidDifficultyThreshold {
                difficulty_threshold,
                expected_difficulty,
            });
        }
    }

    Ok(())
}
