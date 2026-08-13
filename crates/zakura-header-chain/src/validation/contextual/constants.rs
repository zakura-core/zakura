use zakura_chain::parameters::POW_AVERAGING_WINDOW;

/// The median block span for time median calculations.
///
/// `PoWMedianBlockSpan` in the Zcash specification.
pub const POW_MEDIAN_BLOCK_SPAN: usize = 11;

/// The overall block span used for adjusting Zcash block difficulty.
///
/// `PoWAveragingWindow + PoWMedianBlockSpan` in the Zcash specification based on
/// > ActualTimespan(height : N) := MedianTime(height) − MedianTime(height − PoWAveragingWindow)
pub const POW_ADJUSTMENT_BLOCK_SPAN: usize = POW_AVERAGING_WINDOW + POW_MEDIAN_BLOCK_SPAN;

/// Durable predecessors needed below a separately retained parent frontier.
pub const POW_PREDECESSOR_CONTEXT_SPAN: usize = POW_ADJUSTMENT_BLOCK_SPAN - 1;

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
