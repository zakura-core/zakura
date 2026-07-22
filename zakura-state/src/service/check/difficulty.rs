//! Shared block difficulty adjustment and median-time calculations.

pub(crate) use zakura_header_chain::{
    AdjustedDifficulty, BLOCK_MAX_TIME_SINCE_MEDIAN, POW_ADJUSTMENT_BLOCK_SPAN,
    POW_MEDIAN_BLOCK_SPAN,
};
