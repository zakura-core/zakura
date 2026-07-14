use super::{state::*, wire::BLOCK_SYNC_MESSAGE_TYPE_BYTES, *};

#[derive(Clone, Debug)]
pub(crate) struct ReorderBuffer {
    blocks: BTreeMap<block::Height, BufferedBlock>,
    buffered_bytes: u64,
    decoded_deep_bytes: u64,
}

impl ReorderBuffer {
    pub(super) fn new() -> Self {
        Self {
            blocks: BTreeMap::new(),
            buffered_bytes: 0,
            decoded_deep_bytes: 0,
        }
    }

    pub(super) fn buffered_bytes(&self) -> u64 {
        self.buffered_bytes
    }

    pub(super) fn decoded_deep_bytes(&self) -> u64 {
        self.decoded_deep_bytes
    }

    #[cfg(test)]
    pub(super) fn decoded_deep_bytes_scanned(&self) -> u64 {
        self.blocks
            .values()
            .map(|buffered| buffered.body.decoded_deep_size_bytes())
            .fold(0u64, u64::saturating_add)
    }

    pub(super) fn len(&self) -> usize {
        self.blocks.len()
    }

    /// Highest buffered height, if any. The shed-for-floor-starvation path drops
    /// this (the body furthest from the committed floor) to free budget for a
    /// lower, commit-unblocking request.
    pub(super) fn max_height(&self) -> Option<block::Height> {
        self.blocks.keys().next_back().copied()
    }

    pub(super) fn contains(&self, height: block::Height) -> bool {
        self.blocks.contains_key(&height)
    }

    pub(super) fn contains_at_or_above(&self, height: block::Height) -> bool {
        self.blocks.range(height..).next().is_some()
    }

    pub(super) fn hash(&self, height: block::Height) -> Option<block::Hash> {
        self.blocks.get(&height).map(|buffered| buffered.hash)
    }

    /// Buffer a received body that already owns its `bytes` reservation.
    ///
    /// The caller reserved worst-case bytes for this height at send time and
    /// shrank that reservation to `bytes` on receipt, so the reorder buffer takes
    /// ownership of the existing reservation without touching the budget and can
    /// never fail on budget. A `Duplicate` height is left to the caller to release.
    #[cfg(test)]
    pub(super) fn insert(
        &mut self,
        height: block::Height,
        block: Arc<block::Block>,
        bytes: u64,
        source_peer: ZakuraPeerId,
    ) -> ReorderInsertResult {
        self.insert_body(
            height,
            block.hash(),
            BufferedBlockBody::from_decoded_block(block, None),
            bytes,
            source_peer,
        )
    }

    /// Buffer a received body, keeping raw block bytes when the peer routine can
    /// provide them so non-contiguous backlog does not retain decoded blocks.
    pub(super) fn insert_body(
        &mut self,
        height: block::Height,
        hash: block::Hash,
        body: BufferedBlockBody,
        bytes: u64,
        source_peer: ZakuraPeerId,
    ) -> ReorderInsertResult {
        if self.blocks.contains_key(&height) {
            return ReorderInsertResult::Duplicate;
        }

        let decoded_deep_size_bytes = body.decoded_deep_size_bytes();
        self.blocks.insert(
            height,
            BufferedBlock {
                hash,
                body,
                bytes,
                source_peer,
            },
        );
        self.buffered_bytes = self.buffered_bytes.saturating_add(bytes);
        self.decoded_deep_bytes = self
            .decoded_deep_bytes
            .saturating_add(decoded_deep_size_bytes);
        ReorderInsertResult::Inserted
    }

    pub(super) fn drain_contiguous_prefix(
        &mut self,
        verified_block_tip: block::Height,
    ) -> Vec<(block::Height, Arc<block::Block>, u64, u64, ZakuraPeerId)> {
        let mut released = Vec::new();
        let mut next = match next_height(verified_block_tip) {
            Some(next) => next,
            None => return released,
        };

        while let Some(buffered) = self.blocks.remove(&next) {
            self.buffered_bytes = self.buffered_bytes.saturating_sub(buffered.bytes);
            self.decoded_deep_bytes = self
                .decoded_deep_bytes
                .saturating_sub(buffered.body.decoded_deep_size_bytes());
            let (block, decoded_deep_size_bytes) = buffered.body.into_block();
            released.push((
                next,
                block,
                buffered.bytes,
                decoded_deep_size_bytes,
                buffered.source_peer,
            ));
            let Some(after) = next_height(next) else {
                break;
            };
            next = after;
        }

        released
    }

    /// Drop every buffered body and return the total bytes they held, so the
    /// caller releases exactly that reservation. The reorder buffer is owned by
    /// the `Sequencer`, which does not touch the byte budget; it returns the
    /// freed bytes to the reactor instead.
    pub(crate) fn clear(&mut self) -> u64 {
        self.drop_from(block::Height::MIN)
    }

    /// Drop buffered bodies at or below `through` and return the bytes they held.
    pub(crate) fn drop_through(&mut self, through: block::Height) -> u64 {
        let heights: Vec<_> = self
            .blocks
            .range(..=through)
            .map(|(height, _)| *height)
            .collect();
        let mut released = 0u64;
        for height in heights {
            if let Some(buffered) = self.blocks.remove(&height) {
                self.buffered_bytes = self.buffered_bytes.saturating_sub(buffered.bytes);
                self.decoded_deep_bytes = self
                    .decoded_deep_bytes
                    .saturating_sub(buffered.body.decoded_deep_size_bytes());
                released = released.saturating_add(buffered.bytes);
            }
        }
        released
    }

    /// Drop buffered bodies at or above `from` and return the bytes they held.
    pub(crate) fn drop_from(&mut self, from: block::Height) -> u64 {
        let heights: Vec<_> = self
            .blocks
            .range(from..)
            .map(|(height, _)| *height)
            .collect();
        let mut released = 0u64;
        for height in heights {
            if let Some(buffered) = self.blocks.remove(&height) {
                self.buffered_bytes = self.buffered_bytes.saturating_sub(buffered.bytes);
                self.decoded_deep_bytes = self
                    .decoded_deep_bytes
                    .saturating_sub(buffered.body.decoded_deep_size_bytes());
                released = released.saturating_add(buffered.bytes);
            }
        }
        released
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum ReorderInsertResult {
    Inserted,
    Duplicate,
}

#[derive(Clone, Debug)]
struct BufferedBlock {
    hash: block::Hash,
    body: BufferedBlockBody,
    bytes: u64,
    /// The peer that delivered this body, so an apply rejection can be attributed
    /// back to it for misbehavior scoring.
    source_peer: ZakuraPeerId,
}

#[derive(Clone, Debug)]
pub(super) enum BufferedBlockBody {
    RawFramePayload(Arc<[u8]>),
    Decoded {
        block: Arc<block::Block>,
        decoded_deep_size_bytes: u64,
    },
    DecodedWithRawFramePayload {
        block: Arc<block::Block>,
        raw_frame_payload: Arc<[u8]>,
        decoded_deep_size_bytes: u64,
    },
}

impl BufferedBlockBody {
    #[cfg(any(test, feature = "internal-bench"))]
    pub(super) fn from_decoded_block(
        block: Arc<block::Block>,
        raw_frame_payload: Option<Arc<[u8]>>,
    ) -> Self {
        let decoded_deep_size_bytes = block.deep_owned_size_bytes();
        Self::from_measured_decoded_block(block, raw_frame_payload, decoded_deep_size_bytes)
    }

    pub(super) fn from_measured_decoded_block(
        block: Arc<block::Block>,
        raw_frame_payload: Option<Arc<[u8]>>,
        decoded_deep_size_bytes: u64,
    ) -> Self {
        match raw_frame_payload {
            Some(raw_frame_payload) => BufferedBlockBody::DecodedWithRawFramePayload {
                block,
                raw_frame_payload,
                decoded_deep_size_bytes,
            },
            None => BufferedBlockBody::Decoded {
                block,
                decoded_deep_size_bytes,
            },
        }
    }

    pub(super) fn decoded_deep_size_bytes(&self) -> u64 {
        match self {
            BufferedBlockBody::RawFramePayload(_) => 0,
            BufferedBlockBody::Decoded {
                decoded_deep_size_bytes,
                ..
            }
            | BufferedBlockBody::DecodedWithRawFramePayload {
                decoded_deep_size_bytes,
                ..
            } => *decoded_deep_size_bytes,
        }
    }

    // Drop the decoded body for the backlog.
    // This is used to save memory when the body is not the next block in the sequence.
    // DecodedWithRawFramePayload may hold the parsed block as well as the raw frame payload,
    // so we retain just the raw frame payload.
    pub(super) fn retain_for_backlog(self) -> Self {
        match self {
            BufferedBlockBody::DecodedWithRawFramePayload {
                raw_frame_payload, ..
            } => BufferedBlockBody::RawFramePayload(raw_frame_payload),
            body => body,
        }
    }

    fn into_block(self) -> (Arc<block::Block>, u64) {
        match self {
            BufferedBlockBody::Decoded {
                block,
                decoded_deep_size_bytes,
            }
            | BufferedBlockBody::DecodedWithRawFramePayload {
                block,
                decoded_deep_size_bytes,
                ..
            } => (block, decoded_deep_size_bytes),
            BufferedBlockBody::RawFramePayload(payload) => {
                let mut reader = Cursor::new(&payload[BLOCK_SYNC_MESSAGE_TYPE_BYTES..]);
                let block = Arc::new(
                    block::Block::zcash_deserialize(&mut reader)
                        .expect("raw block bytes deserialize because the peer routine decoded them before buffering"),
                );
                let decoded_deep_size_bytes = block.deep_owned_size_bytes();
                let serialized_bytes = payload.len().saturating_sub(BLOCK_SYNC_MESSAGE_TYPE_BYTES);
                // Metrics accepts f64 samples; these lossy conversions are observability-only.
                metrics::histogram!(
                    "sync.block.body.decoded.deep_size_bytes",
                    "stage" => "reorder"
                )
                .record(decoded_deep_size_bytes as f64);
                if serialized_bytes > 0 {
                    metrics::histogram!(
                        "sync.block.body.decoded.to_serialized_ratio",
                        "stage" => "reorder"
                    )
                    .record(decoded_deep_size_bytes as f64 / serialized_bytes as f64);
                }
                (block, decoded_deep_size_bytes)
            }
        }
    }
}
