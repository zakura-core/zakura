//! Block difficulty adjustment calculations for contextual validation.
//!
//! This module supports the following consensus rule calculations:
//!  * `ThresholdBits` from the Zcash Specification,
//!  * the Testnet minimum difficulty adjustment from ZIPs 205 and 208, and
//!  * `median-time-past`.

use std::cmp::{max, min};

use chrono::{DateTime, Duration, Utc};
use thiserror::Error;

use zakura_chain::{
    block::{self, Block},
    parameters::{Network, NetworkUpgrade, POW_AVERAGING_WINDOW},
    work::difficulty::{CompactDifficulty, ExpandedDifficulty, ParameterDifficulty as _, U256},
    BoundedVec,
};

/// The median block span for time median calculations.
///
/// `PoWMedianBlockSpan` in the Zcash specification.
pub const POW_MEDIAN_BLOCK_SPAN: usize = 11;

/// The overall block span used for adjusting Zcash block difficulty.
///
/// `PoWAveragingWindow + PoWMedianBlockSpan` in the Zcash specification based on
/// > ActualTimespan(height : N) := MedianTime(height) − MedianTime(height − PoWAveragingWindow)
pub const POW_ADJUSTMENT_BLOCK_SPAN: usize = POW_AVERAGING_WINDOW + POW_MEDIAN_BLOCK_SPAN;

/// The damping factor for median timespan variance.
///
/// `PoWDampingFactor` in the Zcash specification.
pub const POW_DAMPING_FACTOR: i32 = 4;

/// The maximum upward adjustment percentage for median timespan variance.
///
/// `PoWMaxAdjustUp * 100` in the Zcash specification.
pub const POW_MAX_ADJUST_UP_PERCENT: i32 = 16;

/// The maximum downward adjustment percentage for median timespan variance.
///
/// `PoWMaxAdjustDown * 100` in the Zcash specification.
pub const POW_MAX_ADJUST_DOWN_PERCENT: i32 = 32;

/// The maximum number of seconds between the `median-time-past` of a block,
/// and the block's `time` field.
///
/// Part of the block header consensus rules in the Zcash specification.
pub const BLOCK_MAX_TIME_SINCE_MEDIAN: u32 = 90 * 60;

/// Contains the context needed to calculate the adjusted difficulty for a block.
pub struct AdjustedDifficulty {
    /// The `header.time` field from the candidate block
    candidate_time: DateTime<Utc>,
    /// The coinbase height from the candidate block
    ///
    /// If we only have the header, this field is calculated from the previous
    /// block height.
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
    /// Only the first and last `PoWMedianBlockSpan` times are used. Times
    /// `11..=16` are ignored.
    relevant_times: BoundedVec<DateTime<Utc>, 1, POW_ADJUSTMENT_BLOCK_SPAN>,
}

impl AdjustedDifficulty {
    /// Initialise and return a new `AdjustedDifficulty` using a `candidate_block`,
    /// `network`, and a `context`.
    ///
    /// The `context` contains the previous
    /// `PoWAveragingWindow + PoWMedianBlockSpan` (28) `difficulty_threshold`s and
    /// `time`s from the relevant chain for `candidate_block`, in reverse height
    /// order, starting with the previous block.
    ///
    /// Note that the `time`s might not be in reverse chronological order, because
    /// block times are supplied by miners.
    ///
    /// # Panics
    ///
    /// This function may panic in the following cases:
    /// - The `candidate_block` has no coinbase height (should never happen for valid blocks).
    /// - The `candidate_block` is the genesis block, so `previous_block_height` cannot be computed.
    /// - `AdjustedDifficulty::new_from_header_time` panics.
    pub fn new_from_block<C>(
        candidate_block: &Block,
        network: &Network,
        context: C,
    ) -> AdjustedDifficulty
    where
        C: IntoIterator<Item = (CompactDifficulty, DateTime<Utc>)>,
    {
        let candidate_block_height = candidate_block
            .coinbase_height()
            .expect("semantically valid blocks have a coinbase height");
        let previous_block_height = (candidate_block_height - 1)
            .expect("contextual validation is never run on the genesis block");

        AdjustedDifficulty::new_from_header_time(
            candidate_block.header.time,
            previous_block_height,
            network,
            context,
        )
    }

    /// Initialise and return a new [`AdjustedDifficulty`] using a
    /// `candidate_header_time`, `previous_block_height`, `network`, and a `context`.
    ///
    /// Designed for use when validating block headers, where the full block has not
    /// been downloaded yet.
    ///
    /// See [`Self::new_from_block`] for detailed information about the `context`.
    ///
    /// # Panics
    ///
    /// This function may panic in the following cases:
    /// - The next block height is invalid.
    /// - The `context` iterator is empty, because at least one difficulty threshold
    ///   and block time are required to construct the `Bounded` vectors.
    /// - The context iterator is empty, because at least one difficulty threshold and block time are required.
    pub fn new_from_header_time<C>(
        candidate_header_time: DateTime<Utc>,
        previous_block_height: block::Height,
        network: &Network,
        context: C,
    ) -> AdjustedDifficulty
    where
        C: IntoIterator<Item = (CompactDifficulty, DateTime<Utc>)>,
    {
        let candidate_height = (previous_block_height + 1).expect("next block height is valid");

        let (thresholds, times) = context
            .into_iter()
            .take(POW_ADJUSTMENT_BLOCK_SPAN)
            .unzip::<_, _, Vec<_>, Vec<_>>();

        let relevant_difficulty_thresholds: BoundedVec<
            CompactDifficulty,
            1,
            POW_ADJUSTMENT_BLOCK_SPAN,
        > = thresholds
            .try_into()
            .expect("context must provide a bounded number of difficulty thresholds");
        let relevant_times: BoundedVec<DateTime<Utc>, 1, POW_ADJUSTMENT_BLOCK_SPAN> = times
            .try_into()
            .expect("context must provide a bounded number of block times");

        AdjustedDifficulty {
            candidate_time: candidate_header_time,
            candidate_height,
            network: network.clone(),
            relevant_difficulty_thresholds,
            relevant_times,
        }
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

    /// Calculate the expected `difficulty_threshold` for a candidate block, based
    /// on the `candidate_time`, `candidate_height`, `network`, and the
    /// `difficulty_threshold`s and `time`s from the previous
    /// `PoWAveragingWindow + PoWMedianBlockSpan` (28) blocks in the relevant chain.
    ///
    /// Implements `ThresholdBits` from the Zcash specification, and the Testnet
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

    /// Calculate the `difficulty_threshold` for a candidate block, based on the
    /// `candidate_height`, `network`, and the relevant `difficulty_threshold`s and
    /// `time`s.
    ///
    /// See [`Self::expected_difficulty_threshold`] for details.
    ///
    /// Implements `ThresholdBits` from the Zcash specification. (Which excludes the
    /// Testnet minimum difficulty adjustment.)
    fn threshold_bits(&self) -> CompactDifficulty {
        let averaging_window_height = u32::try_from(POW_AVERAGING_WINDOW)
            .expect("averaging window is much smaller than u32::MAX");

        if self.candidate_height.0 <= averaging_window_height {
            // # Consensus
            //
            // `ThresholdBits(height)` is `PoWLimit` for `height <= PoWAveragingWindow`.
            // Zebra's full-block contextual validation on Mainnet and Testnet
            // starts after the mandatory checkpoint, so this early-chain path is
            // only reachable through header sync and non-checkpointed test networks.
            return self.network.target_difficulty_limit().to_compact();
        }

        let averaging_window_timespan = NetworkUpgrade::averaging_window_timespan_for_height(
            &self.network,
            self.candidate_height,
        );

        let threshold = (self.mean_target_difficulty() / averaging_window_timespan.num_seconds())
            * self.median_timespan_bounded().num_seconds();
        let threshold = min(self.network.target_difficulty_limit(), threshold);

        threshold.to_compact()
    }

    /// Calculate the arithmetic mean of the averaging window thresholds: the
    /// expanded `difficulty_threshold`s from the previous `PoWAveragingWindow` (17)
    /// blocks in the relevant chain.
    ///
    /// Implements `MeanTarget` from the Zcash specification.
    fn mean_target_difficulty(&self) -> ExpandedDifficulty {
        // `threshold_bits` returns `PoWLimit` before calling this function for
        // early-chain heights. At later heights, a valid relevant chain contains
        // at least 17 blocks.

        let averaging_window_thresholds =
            if self.relevant_difficulty_thresholds.len() >= POW_AVERAGING_WINDOW {
                &self.relevant_difficulty_thresholds.as_slice()[0..POW_AVERAGING_WINDOW]
            } else {
                return self.network.target_difficulty_limit();
            };

        // Since the PoWLimits are `2^251 − 1` for Testnet, and `2^243 − 1` for
        // Mainnet, the sum of 17 `ExpandedDifficulty` will be less than or equal
        // to: `(2^251 − 1) * 17 = 2^255 + 2^251 - 17`. Therefore, the sum can
        // not overflow a u256 value.
        let total: ExpandedDifficulty = averaging_window_thresholds
            .iter()
            .map(|compact| {
                compact
                    .to_expanded()
                    .expect("difficulty thresholds in previously verified blocks are valid")
            })
            .sum();

        let divisor: U256 = POW_AVERAGING_WINDOW.into();
        total / divisor
    }

    /// Calculate the bounded median timespan. The median timespan is the
    /// difference of medians of the timespan times, which are the `time`s from
    /// the previous `PoWAveragingWindow + PoWMedianBlockSpan` (28) blocks in the
    /// relevant chain.
    ///
    /// Uses the candidate block's `height' and `network` to calculate the
    /// `AveragingWindowTimespan` for that block.
    ///
    /// The median timespan is damped by the `PoWDampingFactor`, and bounded by
    /// `PoWMaxAdjustDown` and `PoWMaxAdjustUp`.
    ///
    /// Implements `ActualTimespanBounded` from the Zcash specification.
    ///
    /// Note: This calculation only uses `PoWMedianBlockSpan` (11) times at the
    /// start and end of the timespan times. timespan times `[11..=16]` are ignored.
    fn median_timespan_bounded(&self) -> Duration {
        let averaging_window_timespan = NetworkUpgrade::averaging_window_timespan_for_height(
            &self.network,
            self.candidate_height,
        );
        // This value is exact, but we need to truncate its nanoseconds component
        let damped_variance =
            (self.median_timespan() - averaging_window_timespan) / POW_DAMPING_FACTOR;
        // num_seconds truncates negative values towards zero, matching the Zcash specification
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

    /// Calculate the median timespan. The median timespan is the difference of
    /// medians of the timespan times, which are the `time`s from the previous
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
    /// Implements `median-time-past` and `MedianTime(candidate_height)` from the
    /// Zcash specification. (These functions are identical, but they are
    /// specified in slightly different ways.)
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
    /// If provided an empty Vec
    pub fn median_time(mut median_block_span_times: Vec<DateTime<Utc>>) -> DateTime<Utc> {
        median_block_span_times.sort_unstable();

        // > median(𝑆) := sorted(𝑆)_{ceiling((length(𝑆)+1)/2)}
        // <https://zips.z.cash/protocol/protocol.pdf>, section 7.7.3, Difficulty Adjustment (p. 132)
        let median_idx = median_block_span_times.len() / 2;
        median_block_span_times[median_idx]
    }
}

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

    // Mainnet height 1 and Testnet heights below 653,606 are outside this rule.
    if candidate_height.0 >= 2
        && network.is_max_block_time_enforced(candidate_height)
        && candidate_time > block_time_max
    {
        return Err(ContextualValidationError::TimeTooLate {
            candidate_time,
            block_time_max,
        });
    }

    let expected_difficulty = difficulty_adjustment.expected_difficulty_threshold();
    if network.disable_pow() {
        if difficulty_threshold.to_work().is_none() {
            return Err(ContextualValidationError::InvalidDifficultyThreshold {
                difficulty_threshold,
                expected_difficulty,
            });
        }
    } else if difficulty_threshold != expected_difficulty {
        return Err(ContextualValidationError::InvalidDifficultyThreshold {
            difficulty_threshold,
            expected_difficulty,
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::parameters::testnet::RegtestParameters;

    fn compact_half_limit(network: &Network) -> CompactDifficulty {
        (network.target_difficulty_limit() / U256::from(2_u8)).to_compact()
    }

    fn context(
        network: &Network,
        candidate_time: DateTime<Utc>,
        spacing: Duration,
        len: usize,
    ) -> Vec<(CompactDifficulty, DateTime<Utc>)> {
        let difficulty = compact_half_limit(network);
        (1..=len)
            .map(|offset| {
                let offset = i32::try_from(offset).expect("test context length fits in i32");
                (difficulty, candidate_time - spacing * offset)
            })
            .collect()
    }

    fn validate_with_expected_target(
        network: &Network,
        candidate_height: block::Height,
        candidate_time: DateTime<Utc>,
        context: &[(CompactDifficulty, DateTime<Utc>)],
    ) -> Result<(), ContextualValidationError> {
        let previous_height = (candidate_height - 1).expect("test candidate is not genesis");
        let expected = AdjustedDifficulty::new_from_header_time(
            candidate_time,
            previous_height,
            network,
            context.iter().copied(),
        )
        .expected_difficulty_threshold();
        validate_contextual_difficulty_and_time(
            expected,
            AdjustedDifficulty::new_from_header_time(
                candidate_time,
                previous_height,
                network,
                context.iter().copied(),
            ),
        )
    }

    #[test]
    fn difficulty_windows_upgrades_testnet_minimum_and_partitions_match() {
        let candidate_time =
            DateTime::from_timestamp(2_000_000_000, 0).expect("test timestamp is in range");

        for network in Network::iter() {
            for len in [1, 11, 16, 17, 27, 28] {
                let candidate_height = block::Height(700_000);
                let spacing = NetworkUpgrade::target_spacing_for_height(&network, candidate_height);
                let context = context(&network, candidate_time, spacing, len);
                let previous_height = (candidate_height - 1).expect("height is positive");
                let expected = AdjustedDifficulty::new_from_header_time(
                    candidate_time,
                    previous_height,
                    &network,
                    context.iter().copied(),
                )
                .expected_difficulty_threshold();

                for split in 0..=context.len() {
                    let partitioned = context[..split].iter().chain(&context[split..]).copied();
                    assert_eq!(
                        AdjustedDifficulty::new_from_header_time(
                            candidate_time,
                            previous_height,
                            &network,
                            partitioned,
                        )
                        .expected_difficulty_threshold(),
                        expected,
                        "response partitions must not affect difficulty for {network:?}, len {len}, split {split}"
                    );
                }
            }

            for (height, _) in network.activation_list() {
                if height == block::Height(0) {
                    continue;
                }
                let spacing = NetworkUpgrade::target_spacing_for_height(&network, height);
                let context_len = usize::try_from(height.0.min(28))
                    .expect("bounded test context length fits in usize");
                let context = context(&network, candidate_time, spacing, context_len);
                validate_with_expected_target(&network, height, candidate_time, &context)
                    .expect("the shared result validates at every configured upgrade boundary");
            }
        }

        let testnet = Network::new_default_testnet();
        let activation_height = block::Height(299_188);
        let spacing = NetworkUpgrade::target_spacing_for_height(&testnet, activation_height);
        let previous_time = candidate_time - spacing * 6;
        let mut exact_gap = context(&testnet, candidate_time, spacing, 28);
        exact_gap[0].1 = previous_time;
        let previous_height = (activation_height - 1).expect("height is positive");
        let exact_gap_target = AdjustedDifficulty::new_from_header_time(
            candidate_time,
            previous_height,
            &testnet,
            exact_gap,
        )
        .expected_difficulty_threshold();
        assert_ne!(
            exact_gap_target,
            testnet.target_difficulty_limit().to_compact()
        );

        let minimum_time = candidate_time + Duration::seconds(1);
        let minimum_context = context(&testnet, minimum_time, spacing, 28)
            .into_iter()
            .enumerate()
            .map(|(index, (difficulty, time))| {
                if index == 0 {
                    (difficulty, previous_time)
                } else {
                    (difficulty, time)
                }
            });
        assert_eq!(
            AdjustedDifficulty::new_from_header_time(
                minimum_time,
                previous_height,
                &testnet,
                minimum_context,
            )
            .expected_difficulty_threshold(),
            testnet.target_difficulty_limit().to_compact(),
            "ZIP 205/208 minimum difficulty begins strictly above six target spacings"
        );
    }

    #[test]
    fn difficulty_damping_bounds_are_exact() {
        let network = Network::Mainnet;
        let candidate_height = block::Height(700_000);
        let previous_height = (candidate_height - 1).expect("height is positive");
        let candidate_time =
            DateTime::from_timestamp(2_000_000_000, 0).expect("test timestamp is in range");
        let averaging_timespan =
            NetworkUpgrade::averaging_window_timespan_for_height(&network, candidate_height);
        let mean_target = compact_half_limit(&network)
            .to_expanded()
            .expect("the test target is valid");

        let fast_context = context(
            &network,
            candidate_time,
            Duration::seconds(1),
            POW_ADJUSTMENT_BLOCK_SPAN,
        );
        let fast = AdjustedDifficulty::new_from_header_time(
            candidate_time,
            previous_height,
            &network,
            fast_context,
        )
        .expected_difficulty_threshold();
        let minimum_timespan = averaging_timespan * (100 - POW_MAX_ADJUST_UP_PERCENT) / 100;
        assert_eq!(
            fast,
            ((mean_target / averaging_timespan.num_seconds()) * minimum_timespan.num_seconds())
                .to_compact(),
            "fast blocks are clipped at the 16% upward-adjustment bound"
        );

        let slow_context = context(
            &network,
            candidate_time,
            Duration::seconds(10_000),
            POW_ADJUSTMENT_BLOCK_SPAN,
        );
        let slow = AdjustedDifficulty::new_from_header_time(
            candidate_time,
            previous_height,
            &network,
            slow_context,
        )
        .expected_difficulty_threshold();
        let maximum_timespan = averaging_timespan * (100 + POW_MAX_ADJUST_DOWN_PERCENT) / 100;
        assert_eq!(
            slow,
            ((mean_target / averaging_timespan.num_seconds()) * maximum_timespan.num_seconds())
                .to_compact(),
            "slow blocks are clipped at the 32% downward-adjustment bound"
        );
    }

    #[test]
    fn median_and_production_max_time_boundaries_are_exact() {
        let base = DateTime::from_timestamp(1_600_000_000, 0).expect("test timestamp is in range");
        let difficulty = Network::Mainnet.target_difficulty_limit().to_compact();

        for len in 1..=POW_ADJUSTMENT_BLOCK_SPAN {
            let times: Vec<_> = (0..len)
                .map(|offset| base + Duration::seconds(i64::try_from(offset).expect("fits")))
                .rev()
                .collect();
            let adjustment = AdjustedDifficulty::new_from_header_time(
                base + Duration::hours(1),
                block::Height(100),
                &Network::Mainnet,
                times.iter().copied().map(|time| (difficulty, time)),
            );
            let mut expected: Vec<_> = times.into_iter().take(POW_MEDIAN_BLOCK_SPAN).collect();
            expected.sort_unstable();
            assert_eq!(adjustment.median_time_past(), expected[expected.len() / 2]);
        }

        for (network, height, max_is_active) in [
            (Network::Mainnet, block::Height(1), false),
            (Network::Mainnet, block::Height(2), true),
            (
                Network::new_default_testnet(),
                block::Height(653_605),
                false,
            ),
            (Network::new_default_testnet(), block::Height(653_606), true),
        ] {
            let context = [(network.target_difficulty_limit().to_compact(), base)];
            assert!(matches!(
                validate_with_expected_target(&network, height, base, &context),
                Err(ContextualValidationError::TimeTooEarly { .. })
            ));

            let equality = base + Duration::minutes(90);
            validate_with_expected_target(&network, height, equality, &context)
                .expect("the 90-minute equality boundary is inclusive");

            let one_second_above = equality + Duration::seconds(1);
            let result =
                validate_with_expected_target(&network, height, one_second_above, &context);
            assert_eq!(
                matches!(result, Err(ContextualValidationError::TimeTooLate { .. })),
                max_is_active,
                "unexpected max-time activation for {network:?} at {height:?}"
            );
        }
    }

    #[test]
    fn disabled_pow_never_waives_median_time() {
        let network = Network::new_regtest(RegtestParameters::default());
        assert!(network.disable_pow());
        let time = DateTime::from_timestamp(1_700_000_000, 0).expect("test timestamp is in range");
        let context = [(network.target_difficulty_limit().to_compact(), time)];
        let adjustment =
            AdjustedDifficulty::new_from_header_time(time, block::Height(0), &network, context);
        assert!(matches!(
            validate_contextual_difficulty_and_time(
                network.target_difficulty_limit().to_compact(),
                adjustment,
            ),
            Err(ContextualValidationError::TimeTooEarly { .. })
        ));
    }
}
