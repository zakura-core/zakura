//! Shared block difficulty adjustment and median-time calculations.

pub(crate) use zakura_header_chain::{
    pow_adjustment_block_span_for_height, AdjustedDifficulty, BLOCK_MAX_TIME_SINCE_MEDIAN,
    POW_MEDIAN_BLOCK_SPAN,
};

#[cfg(test)]
pub(crate) use zakura_header_chain::{MAX_POW_ADJUSTMENT_BLOCK_SPAN, POW_ADJUSTMENT_BLOCK_SPAN};
