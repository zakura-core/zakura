use super::{
    error::HeaderSyncWireError,
    validation::{validate_body_sizes_len, validate_tree_aux_roots_len},
    *,
};

/// One header and all data associated with its height.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderRangeEntry {
    /// Authoritative height for this entry.
    pub height: block::Height,
    /// Header at this entry's height.
    pub header: Arc<block::Header>,
    /// Advisory serialized body-size hint.
    pub body_size: u32,
    /// Commitment roots when the response includes roots.
    pub tree_aux_root: Option<BlockCommitmentRoots>,
}

impl HeaderRangeEntry {
    /// Convert parallel vectors into aligned records at a system boundary.
    pub fn from_parallel(
        start: block::Height,
        headers: Vec<Arc<block::Header>>,
        body_sizes: Vec<u32>,
        tree_aux_roots: Vec<BlockCommitmentRoots>,
    ) -> Result<Vec<Self>, HeaderSyncWireError> {
        validate_body_sizes_len(headers.len(), body_sizes.len())?;
        validate_tree_aux_roots_len(headers.len(), tree_aux_roots.len())?;
        let count =
            u32::try_from(headers.len()).map_err(|_| HeaderSyncWireError::HeaderCountLimit {
                actual: headers.len(),
                max: usize::try_from(u32::MAX).unwrap_or(usize::MAX),
            })?;
        if count != 0 {
            CheckedHeaderRange::from_count(start, count)
                .ok_or(HeaderSyncWireError::InvalidRangeGeometry { start, count })?;
        }
        let mut roots = if tree_aux_roots.is_empty() {
            None
        } else {
            Some(tree_aux_roots.into_iter())
        };
        Ok(headers
            .into_iter()
            .zip(body_sizes)
            .enumerate()
            .map(|(offset, (header, body_size))| {
                let offset =
                    u32::try_from(offset).expect("header count was validated to fit in u32");
                let height = block::Height(
                    start
                        .0
                        .checked_add(offset)
                        .expect("header range endpoint was checked before assigning heights"),
                );
                Self {
                    height,
                    header,
                    body_size,
                    tree_aux_root: roots.as_mut().and_then(Iterator::next),
                }
            })
            .collect())
    }

    /// Split aligned records for APIs that still require parallel vectors.
    pub fn into_parallel(
        entries: Vec<Self>,
    ) -> (Vec<Arc<block::Header>>, Vec<u32>, Vec<BlockCommitmentRoots>) {
        let mut headers = Vec::with_capacity(entries.len());
        let mut body_sizes = Vec::with_capacity(entries.len());
        let mut roots = Vec::with_capacity(entries.len());
        for entry in entries {
            headers.push(entry.header);
            body_sizes.push(entry.body_size);
            roots.extend(entry.tree_aux_root);
        }
        (headers, body_sizes, roots)
    }
}

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
    entries: Vec<HeaderRangeEntry>,
}

impl HeaderRangePayload {
    /// Validate and construct an aligned payload.
    pub fn new(entries: Vec<HeaderRangeEntry>) -> Result<Self, HeaderSyncWireError> {
        let first = entries
            .first()
            .ok_or(HeaderSyncWireError::EmptyHeaderRangePayload)?;
        let count =
            u32::try_from(entries.len()).map_err(|_| HeaderSyncWireError::HeaderCountLimit {
                actual: entries.len(),
                max: usize::try_from(u32::MAX).unwrap_or(usize::MAX),
            })?;
        CheckedHeaderRange::from_count(first.height, count).ok_or(
            HeaderSyncWireError::InvalidRangeGeometry {
                start: first.height,
                count,
            },
        )?;

        for (offset, adjacent) in entries.windows(2).enumerate() {
            let expected_height = adjacent[0]
                .height
                .0
                .checked_add(1)
                .map(block::Height)
                .ok_or(HeaderSyncWireError::InvalidRangeGeometry {
                    start: first.height,
                    count,
                })?;
            if adjacent[1].height != expected_height {
                return Err(HeaderSyncWireError::EntryHeightMismatch {
                    offset: offset + 1,
                    expected_height,
                    entry_height: adjacent[1].height,
                });
            }
        }

        let root_count = entries
            .iter()
            .filter(|entry| entry.tree_aux_root.is_some())
            .count();
        if root_count != 0 && root_count != entries.len() {
            return Err(HeaderSyncWireError::TreeAuxRootCountMismatch {
                headers: entries.len(),
                roots: root_count,
            });
        }
        if root_count != 0 {
            let first_root_height = entries
                .first()
                .and_then(|entry| entry.tree_aux_root.as_ref())
                .expect("all payload entries have roots")
                .height;
            let last_root_height = entries
                .last()
                .and_then(|entry| entry.tree_aux_root.as_ref())
                .expect("all payload entries have roots")
                .height;
            for (offset, entry) in entries.iter().enumerate() {
                let root_height = entry
                    .tree_aux_root
                    .as_ref()
                    .expect("all payload entries have roots")
                    .height;
                if root_height != entry.height {
                    return Err(HeaderSyncWireError::TreeAuxRootHeightMismatch {
                        offset,
                        expected_height: entry.height,
                        root_height,
                        first_root_height,
                        last_root_height,
                    });
                }
            }
        }

        Ok(Self { entries })
    }

    /// Return the delivered height range.
    pub fn range(&self) -> CheckedHeaderRange {
        CheckedHeaderRange::from_bounds(
            self.entries
                .first()
                .expect("validated payload is non-empty")
                .height,
            self.entries
                .last()
                .expect("validated payload is non-empty")
                .height,
        )
        .expect("validated payload entry heights are contiguous")
    }

    /// Return the structurally aligned entries.
    pub fn entries(&self) -> &[HeaderRangeEntry] {
        &self.entries
    }

    /// Iterate over the delivered headers without cloning.
    pub fn headers(
        &self,
    ) -> impl DoubleEndedIterator<Item = &Arc<block::Header>> + ExactSizeIterator {
        self.entries.iter().map(|entry| &entry.header)
    }

    /// Iterate over aligned body-size hints.
    pub fn body_sizes(&self) -> impl DoubleEndedIterator<Item = u32> + ExactSizeIterator + '_ {
        self.entries.iter().map(|entry| entry.body_size)
    }

    /// Iterate over aligned roots when every entry includes one.
    pub fn tree_aux_roots(
        &self,
    ) -> Option<impl DoubleEndedIterator<Item = &BlockCommitmentRoots> + ExactSizeIterator> {
        self.has_tree_aux_roots().then(|| {
            self.entries.iter().map(|entry| {
                entry
                    .tree_aux_root
                    .as_ref()
                    .expect("rooted payload entries all have roots")
            })
        })
    }

    /// Return whether every entry includes a tree-aux root.
    pub fn has_tree_aux_roots(&self) -> bool {
        self.entries
            .first()
            .is_some_and(|entry| entry.tree_aux_root.is_some())
    }

    /// Keep only the part of this payload strictly after `covered_through`.
    pub fn suffix_after(mut self, covered_through: block::Height) -> Option<Self> {
        let split_index = self
            .entries
            .partition_point(|entry| entry.height <= covered_through);
        if split_index == self.entries.len() {
            return None;
        }
        if split_index == 0 {
            return Some(self);
        }

        self.entries = self.entries.split_off(split_index);
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
        let range = self.range();
        let has_roots = self.has_tree_aux_roots();
        let mut headers = Vec::with_capacity(self.entries.len());
        let mut body_sizes = Vec::with_capacity(self.entries.len());
        let mut roots = has_roots.then(|| Vec::with_capacity(self.entries.len()));
        for entry in self.entries {
            headers.push(entry.header);
            body_sizes.push(entry.body_size);
            if let (Some(roots), Some(root)) = (roots.as_mut(), entry.tree_aux_root) {
                roots.push(root);
            }
        }
        (range, headers, body_sizes, roots)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::{orchard, sapling};
    use zakura_test::vectors::BLOCK_MAINNET_1_BYTES;

    fn header() -> Arc<block::Header> {
        Arc::new(
            block::Header::zcash_deserialize(&BLOCK_MAINNET_1_BYTES[..])
                .expect("test header parses"),
        )
    }

    fn root(height: block::Height) -> BlockCommitmentRoots {
        BlockCommitmentRoots {
            height,
            sapling_root: sapling::tree::NoteCommitmentTree::default().root(),
            orchard_root: orchard::tree::NoteCommitmentTree::default().root(),
            ironwood_root: zakura_chain::ironwood::tree::NoteCommitmentTree::default().root(),
            sapling_tx: 0,
            orchard_tx: 0,
            ironwood_tx: 0,
            auth_data_root: block::merkle::AuthDataRoot::from([0u8; 32]),
        }
    }

    fn entry(height: u32, body_size: u32, root_height: Option<u32>) -> HeaderRangeEntry {
        HeaderRangeEntry {
            height: block::Height(height),
            header: header(),
            body_size,
            tree_aux_root: root_height.map(|height| root(block::Height(height))),
        }
    }

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
    }

    #[test]
    fn entry_conversion_rejects_misaligned_body_sizes() {
        let error =
            HeaderRangeEntry::from_parallel(block::Height(1), Vec::new(), vec![1], Vec::new())
                .expect_err("body sizes must align with headers");

        assert!(matches!(
            error,
            HeaderSyncWireError::BodySizeCountMismatch {
                headers: 0,
                body_sizes: 1,
            }
        ));
    }

    #[test]
    fn entry_conversion_rejects_missing_roots_for_non_empty_headers() {
        let error =
            HeaderRangeEntry::from_parallel(block::Height(1), vec![header()], vec![1], Vec::new())
                .expect_err("roots must align with non-empty headers");

        assert!(matches!(
            error,
            HeaderSyncWireError::TreeAuxRootCountMismatch {
                headers: 1,
                roots: 0,
            }
        ));
    }

    #[test]
    fn entry_conversion_assigns_checked_contiguous_heights() {
        let entries = HeaderRangeEntry::from_parallel(
            block::Height(7),
            vec![header(), header()],
            vec![10, 20],
            vec![root(block::Height(7)), root(block::Height(8))],
        )
        .expect("range geometry and parallel vectors are valid");

        assert_eq!(
            entries.iter().map(|entry| entry.height).collect::<Vec<_>>(),
            vec![block::Height(7), block::Height(8)]
        );
    }

    #[test]
    fn payload_rejects_discontinuous_entry_heights() {
        let error = HeaderRangePayload::new(vec![entry(7, 10, None), entry(9, 20, None)])
            .expect_err("entry heights must be contiguous");

        assert!(matches!(
            error,
            HeaderSyncWireError::EntryHeightMismatch {
                offset: 1,
                expected_height: block::Height(8),
                entry_height: block::Height(9),
            }
        ));
    }

    #[test]
    fn payload_rejects_root_height_different_from_entry_height() {
        let error = HeaderRangePayload::new(vec![entry(7, 10, Some(8))])
            .expect_err("root height must equal its entry height");

        assert!(matches!(
            error,
            HeaderSyncWireError::TreeAuxRootHeightMismatch {
                offset: 0,
                expected_height: block::Height(7),
                root_height: block::Height(8),
                ..
            }
        ));
    }

    #[test]
    fn payload_suffix_preserves_aligned_entry_data() {
        let payload = HeaderRangePayload::new(vec![
            entry(7, 10, None),
            entry(8, 20, None),
            entry(9, 30, None),
        ])
        .expect("entries form a valid payload");

        let suffix = payload
            .suffix_after(block::Height(7))
            .expect("two-entry suffix remains");

        assert_eq!(
            suffix.range(),
            CheckedHeaderRange::from_bounds(block::Height(8), block::Height(9))
                .expect("bounds are ascending")
        );
        assert_eq!(
            suffix
                .entries()
                .iter()
                .map(|entry| (entry.height, entry.body_size))
                .collect::<Vec<_>>(),
            vec![(block::Height(8), 20), (block::Height(9), 30)]
        );
    }
}
