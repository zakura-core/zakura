use super::{state::*, wire::BLOCK_SYNC_MESSAGE_TYPE_BYTES, *};

#[derive(Clone, Debug)]
pub(crate) struct ReorderBuffer {
    blocks: BTreeMap<block::Height, BufferedBlock>,
    buffered_bytes: u64,
}

impl ReorderBuffer {
    pub(super) fn new() -> Self {
        Self {
            blocks: BTreeMap::new(),
            buffered_bytes: 0,
        }
    }

    pub(super) fn buffered_bytes(&self) -> u64 {
        self.buffered_bytes
    }

    pub(super) fn len(&self) -> usize {
        self.blocks.len()
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

    /// Buffer a received body with its wire `bytes` size.
    ///
    /// Retained bodies carry no wire-budget charge (the resident look-ahead gate
    /// bounds them via `buffered_bytes`), so an insert can never fail on budget.
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
            BufferedBlockBody::Decoded(block),
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
        ReorderInsertResult::Inserted
    }

    pub(super) fn drain_contiguous_prefix(
        &mut self,
        verified_block_tip: block::Height,
    ) -> Vec<(block::Height, Arc<block::Block>, u64, ZakuraPeerId)> {
        let mut released = Vec::new();
        let mut next = match next_height(verified_block_tip) {
            Some(next) => next,
            None => return released,
        };

        while let Some(buffered) = self.blocks.remove(&next) {
            self.buffered_bytes = self.buffered_bytes.saturating_sub(buffered.bytes);
            released.push((
                next,
                buffered.body.into_block(),
                buffered.bytes,
                buffered.source_peer,
            ));
            let Some(after) = next_height(next) else {
                break;
            };
            next = after;
        }

        released
    }

    /// Drop every buffered body and return the total bytes they held (the
    /// retained-size accounting the resident view reads; not a budget charge).
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
    Decoded(Arc<block::Block>),
    DecodedWithRawFramePayload {
        block: Arc<block::Block>,
        raw_frame_payload: Arc<[u8]>,
    },
}

impl BufferedBlockBody {
    pub(super) fn from_decoded_block(
        block: Arc<block::Block>,
        raw_frame_payload: Option<Arc<[u8]>>,
    ) -> Self {
        match raw_frame_payload {
            Some(raw_frame_payload) => BufferedBlockBody::DecodedWithRawFramePayload {
                block,
                raw_frame_payload,
            },
            None => BufferedBlockBody::Decoded(block),
        }
    }

    // Drop the raw frame payload for the backlog.
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

    fn into_block(self) -> Arc<block::Block> {
        match self {
            BufferedBlockBody::Decoded(block) => block,
            BufferedBlockBody::DecodedWithRawFramePayload { block, .. } => block,
            BufferedBlockBody::RawFramePayload(payload) => {
                let mut reader = Cursor::new(&payload[BLOCK_SYNC_MESSAGE_TYPE_BYTES..]);
                Arc::new(
                    block::Block::zcash_deserialize(&mut reader)
                        .expect("raw block bytes deserialize because the peer routine decoded them before buffering"),
                )
            }
        }
    }
}
