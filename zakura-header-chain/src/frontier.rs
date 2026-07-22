//! Hash-qualified frontiers and exact chain-work ordering.

use std::cmp::Ordering;

use thiserror::Error;
use zakura_chain::{
    block,
    work::difficulty::{Work, U256},
};

/// One exact height/hash frontier.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct Frontier {
    /// Frontier height.
    pub height: block::Height,
    /// Exact block hash at `height`.
    pub hash: block::Hash,
}

impl Frontier {
    /// Construct a hash-qualified frontier.
    pub const fn new(height: block::Height, hash: block::Hash) -> Self {
        Self { height, hash }
    }
}

/// The three independently meaningful durable engine frontiers.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct FrontierSet {
    /// Irreversible local finality frontier.
    pub finalized: Frontier,
    /// Best locally header-valid frontier.
    pub header_best: Frontier,
    /// Best body-verified frontier on the selected path.
    pub verified_best: Frontier,
}

/// Exact cumulative work of a suffix after one shared anchor.
#[derive(Copy, Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct SuffixWork(U256);

impl SuffixWork {
    /// Construct exact suffix work from its 256-bit integer representation.
    pub const fn new(work: U256) -> Self {
        Self(work)
    }

    /// Return a value representing no suffix work.
    pub fn zero() -> Self {
        Self(U256::zero())
    }

    /// Return the exact 256-bit integer representation.
    pub const fn as_u256(self) -> U256 {
        self.0
    }

    /// Add exact per-block work, failing closed at the 2^256 boundary.
    pub fn checked_add(self, work: Work) -> Option<Self> {
        self.0.checked_add(work.as_u256()).map(Self)
    }
}

impl From<U256> for SuffixWork {
    fn from(work: U256) -> Self {
        Self::new(work)
    }
}

/// The only fork-selection score: exact suffix work, then raw internal tip hash.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ChainScore {
    /// Exact cumulative work after the shared comparison anchor.
    pub suffix_work: SuffixWork,
    /// Raw internal hash of the candidate tip.
    pub tip_hash: block::Hash,
}

impl ChainScore {
    /// Construct a score whose work is already rebased to a shared anchor.
    pub const fn new(suffix_work: SuffixWork, tip_hash: block::Hash) -> Self {
        Self {
            suffix_work,
            tip_hash,
        }
    }
}

impl Ord for ChainScore {
    fn cmp(&self, other: &Self) -> Ordering {
        self.suffix_work
            .cmp(&other.suffix_work)
            .then_with(|| self.tip_hash.0.cmp(&other.tip_hash.0))
    }
}

impl PartialOrd for ChainScore {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Checked cumulative work from one immutable bootstrap origin.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct WorkCoordinate {
    origin_hash: block::Hash,
    cumulative: U256,
}

impl WorkCoordinate {
    /// Construct a coordinate from a trusted origin and exact cumulative work.
    pub const fn new(origin_hash: block::Hash, cumulative: U256) -> Self {
        Self {
            origin_hash,
            cumulative,
        }
    }

    /// Return the immutable work origin hash.
    pub const fn origin_hash(self) -> block::Hash {
        self.origin_hash
    }

    /// Add one block's exact work, failing closed on overflow.
    pub fn checked_add(self, work: Work) -> Result<Self, WorkCoordinateError> {
        let cumulative = self
            .cumulative
            .checked_add(work.as_u256())
            .ok_or(WorkCoordinateError::Overflow)?;
        Ok(Self { cumulative, ..self })
    }

    /// Subtract an ancestor coordinate to produce comparable suffix work.
    pub fn suffix_after(self, anchor: Self) -> Result<SuffixWork, WorkCoordinateError> {
        if self.origin_hash != anchor.origin_hash {
            return Err(WorkCoordinateError::OriginMismatch {
                anchor: anchor.origin_hash,
                tip: self.origin_hash,
            });
        }
        self.cumulative
            .checked_sub(anchor.cumulative)
            .map(SuffixWork)
            .ok_or(WorkCoordinateError::Underflow)
    }
}

/// Failure to maintain or compare exact work coordinates.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub enum WorkCoordinateError {
    /// Coordinates name different immutable bootstrap origins.
    #[error("work-coordinate origin mismatch: anchor {anchor:?}, tip {tip:?}")]
    OriginMismatch {
        /// Anchor coordinate origin.
        anchor: block::Hash,
        /// Tip coordinate origin.
        tip: block::Hash,
    },
    /// Exact work accumulation crossed the 2^256 boundary.
    #[error("work-coordinate accumulation overflow")]
    Overflow,
    /// An alleged descendant had less cumulative work than its anchor.
    #[error("work-coordinate suffix subtraction underflow")]
    Underflow,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chain_score_breaks_equal_work_ties_using_raw_hash_bytes() {
        let work = SuffixWork::from(U256::from(7));
        let lower = ChainScore::new(
            work,
            block::Hash([
                0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0,
            ]),
        );
        let higher = ChainScore::new(
            work,
            block::Hash([
                1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0,
            ]),
        );
        assert!(
            higher > lower,
            "raw byte order, not display order, breaks ties"
        );
    }

    #[test]
    fn work_coordinates_rebase_only_with_a_shared_origin() {
        let origin = block::Hash([1; 32]);
        let anchor = WorkCoordinate::new(origin, U256::from(10));
        let tip = WorkCoordinate::new(origin, U256::from(17));
        assert_eq!(
            tip.suffix_after(anchor),
            Ok(SuffixWork::from(U256::from(7)))
        );
        assert_eq!(
            anchor.suffix_after(tip),
            Err(WorkCoordinateError::Underflow)
        );
        assert!(matches!(
            tip.suffix_after(WorkCoordinate::new(block::Hash([2; 32]), U256::from(1))),
            Err(WorkCoordinateError::OriginMismatch { .. })
        ));
    }
}
