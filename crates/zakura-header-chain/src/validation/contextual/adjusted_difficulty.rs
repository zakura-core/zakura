use std::cmp::{max, min};

use chrono::{DateTime, Duration, Utc};
use thiserror::Error;
use zakura_chain::{
    block::{self, Block},
    parameters::{Network, NetworkUpgrade, POW_AVERAGING_WINDOW},
    work::difficulty::{CompactDifficulty, ExpandedDifficulty, ParameterDifficulty as _, U256},
    BoundedVec,
};

use super::{
    POW_ADJUSTMENT_BLOCK_SPAN, POW_DAMPING_FACTOR, POW_MAX_ADJUST_DOWN_PERCENT,
    POW_MAX_ADJUST_UP_PERCENT, POW_MEDIAN_BLOCK_SPAN,
};

/// The difficulty context calculates a block's adjusted difficulty.
pub struct AdjustedDifficulty {
    /// The `header.time` field from the candidate block
    candidate_time: DateTime<Utc>,
    /// The coinbase height from the candidate block
    ///
    /// Header validation calculates this field from the previous block height.
    candidate_height: block::Height,
    /// The configured network
    network: Network,
    /// The `header.difficulty_threshold`s from the previous
    /// `PoWAveragingWindow + PoWMedianBlockSpan` (28) blocks, in reverse height
    /// order.
    relevant_difficulty_thresholds: BoundedVec<CompactDifficulty, 1, POW_ADJUSTMENT_BLOCK_SPAN>,
    /// The `header.time`s from the previous
    /// `PoWAveragingWindow + PoWMedianBlockSpan` (28) blocks, in reverse height
    /// order.
    ///
    /// The calculation uses only the first and last `PoWMedianBlockSpan` times.
    /// The calculation ignores times `11..=16`.
    relevant_times: BoundedVec<DateTime<Utc>, 1, POW_ADJUSTMENT_BLOCK_SPAN>,
}

/// Invalid branch context supplied to a difficulty calculation.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum AdjustedDifficultyError {
    /// A full block did not expose its consensus height.
    #[error("candidate block has no coinbase height")]
    MissingCoinbaseHeight,
    /// The caller requested contextual validation for genesis.
    #[error("genesis has no predecessor difficulty context")]
    Genesis,
    /// The predecessor height could not produce a candidate height.
    #[error("candidate height overflows the block-height range")]
    HeightOverflow,
    /// The context did not contain exactly the height-dependent predecessor span.
    #[error("difficulty context has {actual} entries, expected exactly {expected}")]
    ContextLength {
        /// Required predecessor count.
        expected: usize,
        /// Supplied predecessor count, capped at one beyond the maximum span.
        actual: usize,
    },
}

impl AdjustedDifficulty {
    /// Create an `AdjustedDifficulty` from a `candidate_block`, `network`, and `context`.
    ///
    /// The caller supplies the previous
    /// `PoWAveragingWindow + PoWMedianBlockSpan` (28) `difficulty_threshold`s and
    /// `time`s from the relevant chain for `candidate_block`, in reverse height
    /// order, starting with the previous block.
    ///
    /// Miners supply block times.
    /// The `time` values might not follow reverse chronological order.
    pub fn new_from_block<C>(
        candidate_block: &Block,
        network: &Network,
        context: C,
    ) -> Result<AdjustedDifficulty, AdjustedDifficultyError>
    where
        C: IntoIterator<Item = (CompactDifficulty, DateTime<Utc>)>,
    {
        let candidate_block_height = candidate_block
            .coinbase_height()
            .ok_or(AdjustedDifficultyError::MissingCoinbaseHeight)?;
        let previous_block_height =
            (candidate_block_height - 1).ok_or(AdjustedDifficultyError::Genesis)?;

        AdjustedDifficulty::new_from_header_time(
            candidate_block.header.time,
            previous_block_height,
            network,
            context,
        )
    }

    /// Create an [`AdjustedDifficulty`] from header time, parent height, network, and context.
    ///
    /// Header validation uses this constructor before the node downloads the full block.
    ///
    /// See [`Self::new_from_block`] for detailed information about the `context`.
    ///
    pub fn new_from_header_time<C>(
        candidate_header_time: DateTime<Utc>,
        previous_block_height: block::Height,
        network: &Network,
        context: C,
    ) -> Result<AdjustedDifficulty, AdjustedDifficultyError>
    where
        C: IntoIterator<Item = (CompactDifficulty, DateTime<Utc>)>,
    {
        let candidate_height =
            (previous_block_height + 1).ok_or(AdjustedDifficultyError::HeightOverflow)?;

        let (thresholds, times) = context
            .into_iter()
            .take(POW_ADJUSTMENT_BLOCK_SPAN + 1)
            .unzip::<_, _, Vec<_>, Vec<_>>();

        let span = u32::try_from(POW_ADJUSTMENT_BLOCK_SPAN)
            .map_err(|_| AdjustedDifficultyError::HeightOverflow)?;
        let expected = usize::try_from(candidate_height.0.min(span))
            .map_err(|_| AdjustedDifficultyError::HeightOverflow)?;
        if thresholds.len() != expected {
            return Err(AdjustedDifficultyError::ContextLength {
                expected,
                actual: thresholds.len(),
            });
        }

        let actual = thresholds.len();
        let relevant_difficulty_thresholds: BoundedVec<
            CompactDifficulty,
            1,
            POW_ADJUSTMENT_BLOCK_SPAN,
        > = thresholds
            .try_into()
            .map_err(|_| AdjustedDifficultyError::ContextLength { expected, actual })?;
        let relevant_times: BoundedVec<DateTime<Utc>, 1, POW_ADJUSTMENT_BLOCK_SPAN> = times
            .try_into()
            .map_err(|_| AdjustedDifficultyError::ContextLength { expected, actual })?;

        Ok(AdjustedDifficulty {
            candidate_time: candidate_header_time,
            candidate_height,
            network: network.clone(),
            relevant_difficulty_thresholds,
            relevant_times,
        })
    }

    /// Returns the candidate block's height.
    pub fn candidate_height(&self) -> block::Height {
        self.candidate_height
    }

    /// Returns the candidate block's time field.
    pub fn candidate_time(&self) -> DateTime<Utc> {
        self.candidate_time
    }

    /// Returns the configured network.
    pub fn network(&self) -> Network {
        self.network.clone()
    }

    /// Calculate the expected `difficulty_threshold` from the candidate block's time and height,
    /// the network, and the
    /// `difficulty_threshold`s and `time`s from the previous
    /// `PoWAveragingWindow + PoWMedianBlockSpan` (28) blocks in the relevant chain.
    ///
    /// The difficulty calculation implements `ThresholdBits` from the Zcash specification and the Testnet
    /// minimum difficulty adjustment from ZIPs 205 and 208.
    pub fn expected_difficulty_threshold(&self) -> CompactDifficulty {
        if NetworkUpgrade::is_testnet_min_difficulty_block(
            &self.network,
            self.candidate_height,
            self.candidate_time,
            *self.relevant_times.first(),
        ) {
            assert!(
                self.network.is_a_test_network(),
                "invalid network: the minimum difficulty rule only applies on test networks"
            );
            self.network.target_difficulty_limit().to_compact()
        } else {
            self.threshold_bits()
        }
    }

    /// Calculate a candidate block's `difficulty_threshold` from its height, network, and context.
    ///
    /// See [`Self::expected_difficulty_threshold`] for details.
    ///
    /// The difficulty calculation implements `ThresholdBits` from the Zcash specification.
    /// `ThresholdBits` excludes the Testnet minimum difficulty adjustment.
    fn threshold_bits(&self) -> CompactDifficulty {
        let averaging_window_height = u32::try_from(POW_AVERAGING_WINDOW)
            .expect("averaging window is much smaller than u32::MAX");

        if self.candidate_height.0 <= averaging_window_height {
            // # Consensus
            //
            // `ThresholdBits(height)` is `PoWLimit` for `height <= PoWAveragingWindow`.
            // Zakura starts full-block contextual validation after the mandatory checkpoint on
            // Mainnet and Testnet. Only header sync and non-checkpointed test networks reach this
            // early-chain path.
            return self.network.target_difficulty_limit().to_compact();
        }

        let averaging_window_timespan = NetworkUpgrade::averaging_window_timespan_for_height(
            &self.network,
            self.candidate_height,
        );

        let bounded_timespan = self.median_timespan_bounded().num_seconds();
        let scaled_mean = self.mean_target_difficulty() / averaging_window_timespan.num_seconds();
        let target_limit = self.network.target_difficulty_limit();
        let threshold = if scaled_mean > target_limit / bounded_timespan {
            target_limit
        } else {
            min(target_limit, scaled_mean * bounded_timespan)
        };

        threshold.to_compact()
    }

    /// Calculate the arithmetic mean of the expanded `difficulty_threshold` values from the
    /// previous `PoWAveragingWindow` blocks in the relevant chain.
    ///
    /// Implements `MeanTarget` from the Zcash specification.
    fn mean_target_difficulty(&self) -> ExpandedDifficulty {
        // `threshold_bits` returns `PoWLimit` before it calls this function at early-chain heights.
        // A valid relevant chain contains at least 17 blocks at later heights.

        let averaging_window_thresholds =
            &self.relevant_difficulty_thresholds.as_slice()[0..POW_AVERAGING_WINDOW];

        let divisor: U256 = POW_AVERAGING_WINDOW.into();
        let mut quotient_total = U256::zero();
        let mut remainder_total = U256::zero();
        for compact in averaging_window_thresholds {
            let target: U256 = compact
                .to_expanded()
                .expect("difficulty thresholds in previously verified blocks are valid")
                .into();
            quotient_total = quotient_total
                .checked_add(target / divisor)
                .expect("the sum of divided targets is at most U256::MAX");
            remainder_total = remainder_total
                .checked_add(target % divisor)
                .expect("17 remainders smaller than 17 fit in U256");
        }
        ExpandedDifficulty::from(
            quotient_total
                .checked_add(remainder_total / divisor)
                .expect("the exact mean of U256 targets is at most U256::MAX"),
        )
    }

    /// Calculate the bounded median timespan.
    /// The calculation subtracts the medians of the `time` values from
    /// the previous `PoWAveragingWindow + PoWMedianBlockSpan` (28) blocks in the
    /// relevant chain.
    ///
    /// The difficulty calculation uses the candidate block's height and network to calculate the
    /// `AveragingWindowTimespan` for that block.
    ///
    /// `PoWDampingFactor` damps the median timespan.
    /// `PoWMaxAdjustDown` and `PoWMaxAdjustUp` bound the median timespan.
    ///
    /// Implements `ActualTimespanBounded` from the Zcash specification.
    ///
    /// The calculation uses only `PoWMedianBlockSpan` times at each end of the timespan.
    /// The calculation ignores times `11..=16`.
    fn median_timespan_bounded(&self) -> Duration {
        let averaging_window_timespan = NetworkUpgrade::averaging_window_timespan_for_height(
            &self.network,
            self.candidate_height,
        );
        // The duration value is exact. The calculation must truncate its nanoseconds component.
        let damped_variance =
            (self.median_timespan() - averaging_window_timespan) / POW_DAMPING_FACTOR;
        // `num_seconds` truncates negative values toward zero as the Zcash specification requires.
        let damped_variance = Duration::seconds(damped_variance.num_seconds());

        // `ActualTimespanDamped` in the Zcash specification
        let median_timespan_damped = averaging_window_timespan + damped_variance;

        // `MinActualTimespan` and `MaxActualTimespan` in the Zcash spec
        let min_median_timespan =
            averaging_window_timespan * (100 - POW_MAX_ADJUST_UP_PERCENT) / 100;
        let max_median_timespan =
            averaging_window_timespan * (100 + POW_MAX_ADJUST_DOWN_PERCENT) / 100;

        // `ActualTimespanBounded` in the Zcash specification
        max(
            min_median_timespan,
            min(max_median_timespan, median_timespan_damped),
        )
    }

    /// Calculate the median timespan.
    /// The calculation subtracts the medians of the `time` values from
    /// `PoWAveragingWindow + PoWMedianBlockSpan` (28) blocks in the relevant chain.
    ///
    /// Implements `ActualTimespan` from the Zcash specification.
    ///
    /// See [`Self::median_timespan_bounded`] for details.
    fn median_timespan(&self) -> Duration {
        let newer_median = self.median_time_past();

        // MedianTime(height : N) := median([ nTime(𝑖) for 𝑖 from max(0, height − PoWMedianBlockSpan) up to max(0, height − 1) ])
        let older_median = if self.relevant_times.len() > POW_AVERAGING_WINDOW {
            let older_times: Vec<_> = self
                .relevant_times
                .iter()
                .skip(POW_AVERAGING_WINDOW)
                .cloned()
                .take(POW_MEDIAN_BLOCK_SPAN)
                .collect();

            AdjustedDifficulty::median_time(older_times)
        } else {
            *self.relevant_times.last()
        };

        // `ActualTimespan` in the Zcash specification
        newer_median - older_median
    }

    /// Calculate the median of the `time`s from the previous
    /// `PoWMedianBlockSpan` (11) blocks in the relevant chain.
    ///
    /// The median-time calculation implements `median-time-past` and `MedianTime(candidate_height)` from the
    /// Zcash specification. Both specification functions produce the same result.
    pub fn median_time_past(&self) -> DateTime<Utc> {
        let median_times: Vec<DateTime<Utc>> = self
            .relevant_times
            .iter()
            .take(POW_MEDIAN_BLOCK_SPAN)
            .cloned()
            .collect();

        AdjustedDifficulty::median_time(median_times)
    }

    /// Calculate the median of the `median_block_span_times`: the `time`s from a
    /// Vec of `PoWMedianBlockSpan` (11) or fewer blocks in the relevant chain.
    ///
    /// Implements `MedianTime` from the Zcash specification.
    ///
    /// # Panics
    ///
    /// The median-time calculation panics if the caller provides an empty `Vec`.
    pub fn median_time(mut median_block_span_times: Vec<DateTime<Utc>>) -> DateTime<Utc> {
        median_block_span_times.sort_unstable();

        // > median(𝑆) := sorted(𝑆)_{ceiling((length(𝑆)+1)/2)}
        // <https://zips.z.cash/protocol/protocol.pdf>, section 7.7.3, Difficulty Adjustment (p. 132)
        let median_idx = median_block_span_times.len() / 2;
        median_block_span_times[median_idx]
    }
}

#[cfg(test)]
#[path = "tests/arithmetic.rs"]
mod tests;
