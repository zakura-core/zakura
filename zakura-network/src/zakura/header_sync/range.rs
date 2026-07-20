use super::{
    error::HeaderSyncWireError,
    validation::{
        validate_body_sizes_len, validate_tree_aux_root_heights, validate_tree_aux_roots_len,
    },
    *,
};

/// A non-empty inclusive header-height range whose endpoint cannot overflow.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct CheckedHeaderRange {
    start: block::Height,
    end: block::Height,
}

impl CheckedHeaderRange {
    /// Construct a checked range from its first height and non-zero count.
    pub fn from_count(start: block::Height, count: u32) -> Option<Self> {
        let offset = count.checked_sub(1)?;
        let end = start.0.checked_add(offset).map(block::Height)?;
        Some(Self { start, end })
    }

    /// Construct a non-empty checked range from inclusive bounds.
    pub fn from_bounds(start: block::Height, end: block::Height) -> Option<Self> {
        (start <= end).then_some(Self { start, end })
    }

    /// Return the first height.
    pub fn start(self) -> block::Height {
        self.start
    }

    /// Return the inclusive final height.
    pub fn end(self) -> block::Height {
        self.end
    }

    /// Return the number of heights.
    pub fn count(self) -> u32 {
        self.end
            .0
            .checked_sub(self.start.0)
            .and_then(|difference| difference.checked_add(1))
            .expect("checked range bounds are ascending")
    }

    /// Return the part of this range through `included_end`.
    pub fn prefix_through(self, included_end: block::Height) -> Option<Self> {
        Self::from_bounds(self.start, self.end.min(included_end))
    }

    /// Return the part of this range strictly after `covered_through`.
    pub fn suffix_after(self, covered_through: block::Height) -> Option<Self> {
        if covered_through < self.start {
            return Some(self);
        }
        let start = covered_through.0.checked_add(1).map(block::Height)?;
        Self::from_bounds(start, self.end)
    }
}

/// A non-empty delivered header range with structurally aligned per-height data.
#[derive(Clone, Debug)]
pub struct HeaderRangePayload {
    range: CheckedHeaderRange,
    headers: Vec<Arc<block::Header>>,
    body_sizes: Vec<u32>,
    tree_aux_roots: Option<Vec<BlockCommitmentRoots>>,
}

impl HeaderRangePayload {
    /// Validate and construct an aligned payload.
    pub fn new(
        start: block::Height,
        headers: Vec<Arc<block::Header>>,
        body_sizes: Vec<u32>,
        tree_aux_roots: Option<Vec<BlockCommitmentRoots>>,
    ) -> Result<Self, HeaderSyncWireError> {
        validate_body_sizes_len(headers.len(), body_sizes.len())?;
        if let Some(roots) = tree_aux_roots.as_ref() {
            validate_tree_aux_roots_len(headers.len(), roots.len())?;
            validate_tree_aux_root_heights(start, roots)?;
        }

        let count =
            u32::try_from(headers.len()).map_err(|_| HeaderSyncWireError::HeaderCountLimit {
                actual: headers.len(),
                max: usize::try_from(u32::MAX).unwrap_or(usize::MAX),
            })?;
        let range = CheckedHeaderRange::from_count(start, count)
            .ok_or(HeaderSyncWireError::InvalidRangeGeometry { start, count })?;

        Ok(Self {
            range,
            headers,
            body_sizes,
            tree_aux_roots,
        })
    }

    /// Return the delivered height range.
    pub fn range(&self) -> CheckedHeaderRange {
        self.range
    }

    /// Return the delivered headers.
    pub fn headers(&self) -> &[Arc<block::Header>] {
        &self.headers
    }

    /// Return the aligned body-size hints.
    pub fn body_sizes(&self) -> &[u32] {
        &self.body_sizes
    }

    /// Return the aligned roots when the request asked for them.
    pub fn tree_aux_roots(&self) -> Option<&[BlockCommitmentRoots]> {
        self.tree_aux_roots.as_deref()
    }

    /// Keep only the part of this payload strictly after `covered_through`.
    pub fn suffix_after(mut self, covered_through: block::Height) -> Option<Self> {
        let suffix = self.range.suffix_after(covered_through)?;
        if suffix == self.range {
            return Some(self);
        }

        let covered_count = usize::try_from(suffix.start().0 - self.range.start().0)
            .expect("payload length fits in usize");
        self.headers = self.headers.split_off(covered_count);
        self.body_sizes = self.body_sizes.split_off(covered_count);
        if let Some(roots) = self.tree_aux_roots.as_mut() {
            *roots = roots.split_off(covered_count);
        }
        self.range = suffix;
        Some(self)
    }

    /// Keep only the part of this payload through `included_end`.
    pub fn prefix_through(mut self, included_end: block::Height) -> Option<Self> {
        let prefix = self.range.prefix_through(included_end)?;
        let retained_count = usize::try_from(prefix.count()).expect("payload length fits in usize");
        self.headers.truncate(retained_count);
        self.body_sizes.truncate(retained_count);
        if let Some(roots) = self.tree_aux_roots.as_mut() {
            roots.truncate(retained_count);
        }
        self.range = prefix;
        Some(self)
    }

    /// Consume this payload into state request parts.
    pub fn into_parts(
        self,
    ) -> (
        CheckedHeaderRange,
        Vec<Arc<block::Header>>,
        Vec<u32>,
        Option<Vec<BlockCommitmentRoots>>,
    ) {
        (
            self.range,
            self.headers,
            self.body_sizes,
            self.tree_aux_roots,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_range_rejects_empty_reversed_and_overflowing_geometry() {
        assert_eq!(CheckedHeaderRange::from_count(block::Height(1), 0), None);
        assert_eq!(
            CheckedHeaderRange::from_count(block::Height(u32::MAX), 2),
            None
        );
        assert_eq!(
            CheckedHeaderRange::from_bounds(block::Height(2), block::Height(1)),
            None
        );
    }

    #[test]
    fn checked_range_suffix_preserves_maximum_height() {
        let range =
            CheckedHeaderRange::from_bounds(block::Height(u32::MAX - 1), block::Height(u32::MAX))
                .expect("bounds are ascending");

        let suffix = range
            .suffix_after(block::Height(u32::MAX - 1))
            .expect("maximum height remains");

        assert_eq!(suffix.start(), block::Height(u32::MAX));
        assert_eq!(suffix.end(), block::Height(u32::MAX));
        assert_eq!(suffix.count(), 1);
        assert_eq!(suffix.suffix_after(block::Height(u32::MAX)), None);
        assert_eq!(
            range.prefix_through(block::Height(u32::MAX - 1)),
            CheckedHeaderRange::from_count(block::Height(u32::MAX - 1), 1)
        );
    }

    #[test]
    fn payload_rejects_misaligned_body_sizes_before_buffering() {
        let error = HeaderRangePayload::new(block::Height(1), Vec::new(), vec![1], None)
            .expect_err("body sizes must align with headers");

        assert!(matches!(
            error,
            HeaderSyncWireError::BodySizeCountMismatch {
                headers: 0,
                body_sizes: 1,
            }
        ));
    }
}
