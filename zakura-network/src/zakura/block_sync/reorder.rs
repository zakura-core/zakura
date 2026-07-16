use super::{
    retained_memory::{InFlightMemoryReservation, RetainedBodyMemoryTracker, RetainedCharge},
    state::*,
    wire::BLOCK_SYNC_MESSAGE_TYPE_BYTES,
    *,
};

#[derive(Debug)]
pub(crate) struct ReorderBuffer {
    blocks: BTreeMap<block::Height, BufferedBlock>,
    buffered_bytes: u64,
    decoded_attributed_memory_bytes: u64,
}

impl ReorderBuffer {
    pub(super) fn new() -> Self {
        Self {
            blocks: BTreeMap::new(),
            buffered_bytes: 0,
            decoded_attributed_memory_bytes: 0,
        }
    }

    pub(super) fn buffered_bytes(&self) -> u64 {
        self.buffered_bytes
    }

    pub(super) fn decoded_attributed_memory_bytes(&self) -> u64 {
        self.decoded_attributed_memory_bytes
    }

    #[cfg(test)]
    pub(super) fn decoded_attributed_memory_bytes_scanned(&self) -> u64 {
        self.blocks
            .values()
            .map(|buffered| buffered.body.decoded_attributed_memory_size_bytes())
            .fold(0u64, u64::saturating_add)
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
        let previous_block_hash = block.header.previous_block_hash;
        self.insert_body(
            height,
            block.hash(),
            previous_block_hash,
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
        previous_block_hash: block::Hash,
        body: BufferedBlockBody,
        bytes: u64,
        source_peer: ZakuraPeerId,
    ) -> ReorderInsertResult {
        if self.blocks.contains_key(&height) {
            return ReorderInsertResult::Duplicate;
        }

        let decoded_attributed_memory_size_bytes = body.decoded_attributed_memory_size_bytes();
        self.blocks.insert(
            height,
            BufferedBlock {
                hash,
                previous_block_hash,
                body,
                bytes,
                source_peer,
            },
        );
        self.buffered_bytes = self.buffered_bytes.saturating_add(bytes);
        self.decoded_attributed_memory_bytes = self
            .decoded_attributed_memory_bytes
            .saturating_add(decoded_attributed_memory_size_bytes);
        ReorderInsertResult::Inserted
    }

    pub(super) fn drain_contiguous_prefix(
        &mut self,
        verified_block_tip: block::Height,
    ) -> Vec<DrainedBlock> {
        let mut released = Vec::new();
        let mut next = match next_height(verified_block_tip) {
            Some(next) => next,
            None => return released,
        };

        while let Some(buffered) = self.blocks.remove(&next) {
            self.buffered_bytes = self.buffered_bytes.saturating_sub(buffered.bytes);
            self.decoded_attributed_memory_bytes = self
                .decoded_attributed_memory_bytes
                .saturating_sub(buffered.body.decoded_attributed_memory_size_bytes());
            released.push(DrainedBlock {
                height: next,
                hash: buffered.hash,
                previous_block_hash: buffered.previous_block_hash,
                body: buffered.body,
                bytes: buffered.bytes,
                source_peer: buffered.source_peer,
            });
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
                self.decoded_attributed_memory_bytes = self
                    .decoded_attributed_memory_bytes
                    .saturating_sub(buffered.body.decoded_attributed_memory_size_bytes());
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
                self.decoded_attributed_memory_bytes = self
                    .decoded_attributed_memory_bytes
                    .saturating_sub(buffered.body.decoded_attributed_memory_size_bytes());
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

/// One block released from the contiguous reorder prefix, still in whatever
/// buffered form it was held in (raw bytes for backlog entries), so draining
/// into `applying` never forces a decode.
#[derive(Debug)]
pub(super) struct DrainedBlock {
    pub(super) height: block::Height,
    pub(super) hash: block::Hash,
    pub(super) previous_block_hash: block::Hash,
    pub(super) body: BufferedBlockBody,
    pub(super) bytes: u64,
    pub(super) source_peer: ZakuraPeerId,
}

#[derive(Debug)]
struct BufferedBlock {
    hash: block::Hash,
    previous_block_hash: block::Hash,
    body: BufferedBlockBody,
    bytes: u64,
    /// The peer that delivered this body, so an apply rejection can be attributed
    /// back to it for misbehavior scoring.
    source_peer: ZakuraPeerId,
}

#[derive(Debug)]
pub(super) struct BufferedBlockBody {
    representation: BufferedBlockRepresentation,
    retained_charge: RetainedCharge,
}

#[derive(Debug)]
enum BufferedBlockRepresentation {
    RawFramePayload(Arc<[u8]>),
    Decoded {
        block: Arc<block::Block>,
        decoded_attributed_memory_size_bytes: u64,
    },
    DecodedWithRawFramePayload {
        block: Arc<block::Block>,
        raw_frame_payload: Arc<[u8]>,
        decoded_attributed_memory_size_bytes: u64,
    },
}

impl BufferedBlockBody {
    #[cfg(any(test, feature = "internal-bench"))]
    pub(super) fn from_decoded_block(
        block: Arc<block::Block>,
        raw_frame_payload: Option<Arc<[u8]>>,
    ) -> Self {
        let decoded_attributed_memory_size_bytes = block.attributed_memory_size_bytes();
        let retained_memory = RetainedBodyMemoryTracker::new(u64::MAX);
        Self::from_measured_decoded_block(
            block,
            raw_frame_payload,
            decoded_attributed_memory_size_bytes,
            &retained_memory,
        )
    }

    pub(super) fn from_measured_decoded_block(
        block: Arc<block::Block>,
        raw_frame_payload: Option<Arc<[u8]>>,
        decoded_attributed_memory_size_bytes: u64,
        retained_memory: &RetainedBodyMemoryTracker,
    ) -> Self {
        let retained_bytes = Self::decoded_retained_memory_size_bytes(
            decoded_attributed_memory_size_bytes,
            raw_frame_payload.as_ref(),
        );
        let retained_charge = retained_memory.charge(retained_bytes);
        Self::from_parts(
            block,
            raw_frame_payload,
            decoded_attributed_memory_size_bytes,
            retained_charge,
        )
    }

    /// Constructs a decoded body and reconciles the request's memory reservation
    /// to the exact retained size of the decoded block and optional raw payload.
    pub(super) fn from_decoded_block_reconciling_memory_reservation(
        block: Arc<block::Block>,
        raw_frame_payload: Option<Arc<[u8]>>,
        decoded_attributed_memory_size_bytes: u64,
        in_flight_memory_reservation: InFlightMemoryReservation,
    ) -> Self {
        let retained_bytes = Self::decoded_retained_memory_size_bytes(
            decoded_attributed_memory_size_bytes,
            raw_frame_payload.as_ref(),
        );
        let retained_charge = in_flight_memory_reservation.reconcile_exact(retained_bytes);
        Self::from_parts(
            block,
            raw_frame_payload,
            decoded_attributed_memory_size_bytes,
            retained_charge,
        )
    }

    pub(super) fn try_from_decoded_block(
        block: Arc<block::Block>,
        raw_frame_payload: Option<Arc<[u8]>>,
        decoded_attributed_memory_size_bytes: u64,
        retained_memory: &RetainedBodyMemoryTracker,
    ) -> Option<Self> {
        let retained_bytes = Self::decoded_retained_memory_size_bytes(
            decoded_attributed_memory_size_bytes,
            raw_frame_payload.as_ref(),
        );
        let retained_charge = retained_memory.try_charge(retained_bytes)?;
        Some(Self::from_parts(
            block,
            raw_frame_payload,
            decoded_attributed_memory_size_bytes,
            retained_charge,
        ))
    }

    pub(super) fn decoded_retained_memory_size_bytes(
        decoded_attributed_memory_size_bytes: u64,
        raw_frame_payload: Option<&Arc<[u8]>>,
    ) -> u64 {
        decoded_attributed_memory_size_bytes
            .saturating_add(raw_frame_payload.map_or(0, raw_payload_bytes))
    }

    fn from_parts(
        block: Arc<block::Block>,
        raw_frame_payload: Option<Arc<[u8]>>,
        decoded_attributed_memory_size_bytes: u64,
        retained_charge: RetainedCharge,
    ) -> Self {
        let representation = match raw_frame_payload {
            Some(raw_frame_payload) => BufferedBlockRepresentation::DecodedWithRawFramePayload {
                block,
                raw_frame_payload,
                decoded_attributed_memory_size_bytes,
            },
            None => BufferedBlockRepresentation::Decoded {
                block,
                decoded_attributed_memory_size_bytes,
            },
        };
        Self {
            representation,
            retained_charge,
        }
    }

    pub(super) fn decoded_attributed_memory_size_bytes(&self) -> u64 {
        match &self.representation {
            BufferedBlockRepresentation::RawFramePayload(_) => 0,
            BufferedBlockRepresentation::Decoded {
                decoded_attributed_memory_size_bytes,
                ..
            }
            | BufferedBlockRepresentation::DecodedWithRawFramePayload {
                decoded_attributed_memory_size_bytes,
                ..
            } => *decoded_attributed_memory_size_bytes,
        }
    }

    pub(super) fn retained_charge(&self) -> RetainedCharge {
        self.retained_charge.clone()
    }

    // Drop the decoded body for the backlog.
    // This is used to save memory when the body is not the next block in the sequence.
    // DecodedWithRawFramePayload may hold the parsed block as well as the raw frame payload,
    // so we retain just the raw frame payload.
    pub(super) fn retain_for_backlog(mut self) -> Self {
        self.retain_for_backlog_in_place();
        self
    }

    /// In-place [`Self::retain_for_backlog`]: drop the decoded copy when raw
    /// bytes are retained, so a body leaving the bounded decode window releases
    /// its decoded heap footprint.
    pub(super) fn retain_for_backlog_in_place(&mut self) {
        if let BufferedBlockRepresentation::DecodedWithRawFramePayload {
            raw_frame_payload, ..
        } = &self.representation
        {
            let raw_frame_payload = raw_frame_payload.clone();
            self.representation = BufferedBlockRepresentation::RawFramePayload(raw_frame_payload);
            self.retained_charge
                .resize(self.representation_retained_memory_size_bytes());
        }
    }

    /// Keep only the decoded allocation represented by an in-flight submission.
    pub(super) fn retain_for_driver_in_place(&mut self) {
        if let BufferedBlockRepresentation::DecodedWithRawFramePayload {
            block,
            decoded_attributed_memory_size_bytes,
            ..
        } = &self.representation
        {
            let block = block.clone();
            let decoded_attributed_memory_size_bytes = *decoded_attributed_memory_size_bytes;
            self.representation = BufferedBlockRepresentation::Decoded {
                block,
                decoded_attributed_memory_size_bytes,
            };
            self.retained_charge
                .resize(self.representation_retained_memory_size_bytes());
        }
    }

    fn representation_retained_memory_size_bytes(&self) -> u64 {
        match &self.representation {
            BufferedBlockRepresentation::RawFramePayload(payload) => raw_payload_bytes(payload),
            BufferedBlockRepresentation::Decoded {
                decoded_attributed_memory_size_bytes,
                ..
            } => *decoded_attributed_memory_size_bytes,
            BufferedBlockRepresentation::DecodedWithRawFramePayload {
                raw_frame_payload,
                decoded_attributed_memory_size_bytes,
                ..
            } => raw_payload_bytes(raw_frame_payload)
                .saturating_add(*decoded_attributed_memory_size_bytes),
        }
    }

    /// Whether this body currently holds a decoded block.
    #[cfg(test)]
    pub(super) fn is_decoded(&self) -> bool {
        !matches!(
            self.representation,
            BufferedBlockRepresentation::RawFramePayload(_)
        )
    }

    /// Return the decoded block, decoding from the retained raw bytes if needed
    /// and caching the decoded copy in place (the entry enters the decode
    /// window; `retain_for_backlog_in_place` is the matching downgrade).
    pub(super) fn decoded_block(&mut self) -> Arc<block::Block> {
        match &self.representation {
            BufferedBlockRepresentation::Decoded { block, .. }
            | BufferedBlockRepresentation::DecodedWithRawFramePayload { block, .. } => {
                block.clone()
            }
            BufferedBlockRepresentation::RawFramePayload(payload) => {
                let payload = payload.clone();
                let block = decode_raw_frame_payload(&payload);
                let decoded_attributed_memory_size_bytes = block.attributed_memory_size_bytes();
                let serialized_bytes = payload.len().saturating_sub(BLOCK_SYNC_MESSAGE_TYPE_BYTES);
                // Metrics accepts f64 samples; these lossy conversions are observability-only.
                metrics::histogram!(
                    "sync.block.body.decoded.attributed_memory_size_bytes",
                    "stage" => "reorder"
                )
                .record(decoded_attributed_memory_size_bytes as f64);
                if serialized_bytes > 0 {
                    metrics::histogram!(
                        "sync.block.body.decoded.to_serialized_ratio",
                        "stage" => "reorder"
                    )
                    .record(decoded_attributed_memory_size_bytes as f64 / serialized_bytes as f64);
                }
                self.retained_charge.resize(
                    raw_payload_bytes(&payload)
                        .saturating_add(decoded_attributed_memory_size_bytes),
                );
                self.representation = BufferedBlockRepresentation::DecodedWithRawFramePayload {
                    block: block.clone(),
                    raw_frame_payload: payload,
                    decoded_attributed_memory_size_bytes,
                };
                block
            }
        }
    }
}

fn raw_payload_bytes(payload: &Arc<[u8]>) -> u64 {
    u64::try_from(payload.len()).unwrap_or(u64::MAX)
}

fn decode_raw_frame_payload(payload: &Arc<[u8]>) -> Arc<block::Block> {
    let mut reader = Cursor::new(&payload[BLOCK_SYNC_MESSAGE_TYPE_BYTES..]);
    Arc::new(block::Block::zcash_deserialize(&mut reader).expect(
        "raw block bytes deserialize because the peer routine decoded them before buffering",
    ))
}
