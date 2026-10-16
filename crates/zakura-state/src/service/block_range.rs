//! Height-range reads that keep resource reservations with the database work.
//!
//! Cancelling an async caller does not stop a blocking read already in progress.
//! For example, releasing a serving permit on disconnect could free a slot while
//! the database is still reading blocks for that request. These reads move the
//! reservation into the blocking job, then return it alongside the blocks.
//!
//! Blocks are returned as their serialized bytes. Finalized blocks are copied
//! from their stored bytes without being decoded, so the retained memory equals
//! the serialized size.
//!
//! The generic resource type lets callers supply their own reservations without
//! making the state service depend on network serving policy.

use std::sync::Arc;

use futures::future::BoxFuture;
use tower::ServiceExt;
use tracing::Span;
use zakura_chain::{block, diagnostic::CodeTimer, serialization::ZcashSerialize};

use super::{finalized_state::ZakuraDb, non_finalized_state::Chain, read, ReadStateService};
use crate::{request::TimedSpan, BoxError, ReadRequest};

/// One retained chain, capped at its last body on the selected header path.
pub(super) struct BlockRangeSnapshot {
    chain: Option<Arc<Chain>>,
    tip: Option<block::Height>,
}

impl BlockRangeSnapshot {
    pub(super) fn block_and_size(
        &self,
        db: &ZakuraDb,
        height: block::Height,
    ) -> Option<(Arc<block::Block>, usize)> {
        if height > self.tip? {
            return None;
        }
        read::block_and_size(self.chain.clone(), db, height.into())
    }

    /// Returns the serialized block at `height` on this branch, or `None` if it
    /// is unavailable or longer than `max_bytes`.
    ///
    /// Both stores record each block's serialized size, so a block that cannot
    /// fit is rejected before it is read or serialized. Finalized blocks are
    /// copied from their stored bytes and are never decoded. Non-finalized
    /// blocks exist only decoded in memory, so each is serialized once, and
    /// that serialization is both checked and returned.
    pub(super) fn serialized_block_within(
        &self,
        db: &ZakuraDb,
        height: block::Height,
        max_bytes: usize,
    ) -> Option<Vec<u8>> {
        if height > self.tip? {
            return None;
        }
        // The in-memory chain only holds heights above the finalized tip, so
        // this lookup order matches `read::block_and_size`.
        if let Some(chain) = self.chain.as_ref() {
            if let Some(contextual) = chain.block(height.into()) {
                // The chain records each block's serialized size, so a block that
                // cannot fit is skipped before it is serialized.
                if chain.block_info(height.into()).is_some_and(|info| {
                    usize::try_from(info.size()).is_ok_and(|size| size > max_bytes)
                }) {
                    return None;
                }
                let bytes = contextual
                    .block
                    .zcash_serialize_to_vec()
                    .expect("a committed block serializes because writing to a Vec is infallible");
                return (bytes.len() <= max_bytes).then_some(bytes);
            }
        }
        db.raw_block_bytes_within(height.into(), max_bytes)
    }
}

/// A bounded prefix of serialized blocks together with the caller's resource
/// reservations.
///
/// The blocking read transfers its resources into this result. Dropping the
/// async caller cannot release them while that read is running. Dropping an
/// undelivered result releases its blocks before its resources.
#[derive(Debug)]
pub struct OwnedBlockRange<R> {
    // Field order keeps resources alive while the retained blocks are dropped.
    blocks: Vec<(block::Height, Vec<u8>)>,
    resources: R,
}

impl<R> OwnedBlockRange<R> {
    /// Transfer the serialized blocks and their resources to the next owner.
    ///
    /// The caller must retain the resources for as long as its resource policy
    /// requires, including while it holds or processes the returned blocks.
    pub fn into_parts(self) -> (Vec<(block::Height, Vec<u8>)>, R) {
        (self.blocks, self.resources)
    }
}

impl ReadStateService {
    /// Capture the download branch independently of the receipt-selected mining branch.
    /// The header writer lock keeps the retained bodies and selected path coherent.
    pub(super) fn block_range_snapshot(&self) -> Result<BlockRangeSnapshot, BoxError> {
        let reader = self.header_chain_reader_receiver.borrow().clone();
        let Some(reader) = reader else {
            let chain = self.latest_best_chain();
            return Ok(BlockRangeSnapshot {
                tip: read::tip_height(chain.clone(), &self.db),
                chain,
            });
        };
        let (non_finalized, _, selected) = reader.with_selected_overlap(
            || self.latest_non_finalized_state(),
            |state| {
                state
                    .chain_iter()
                    .map(|chain| chain.non_finalized_tip().0)
                    .max()
                    .or_else(|| self.db.finalized_tip_height())
            },
        )?;
        for frontier in selected.iter().rev() {
            if let Some(chain) =
                non_finalized.find_chain(|chain| chain.contains_block_hash(frontier.hash))
            {
                return Ok(BlockRangeSnapshot {
                    chain: Some(chain),
                    tip: Some(frontier.height),
                });
            }
        }

        // No selected non-finalized body is available. Serve only the finalized prefix.
        let finalized = selected
            .first()
            .expect("the coherent full-state overlap includes the finalized header");
        Ok(BlockRangeSnapshot {
            chain: None,
            tip: Some(finalized.height),
        })
    }

    /// Read a bounded contiguous prefix of serialized blocks while the database
    /// job owns `resources`.
    ///
    /// This uses the same readiness checks, chain snapshot, and missing-block
    /// behavior as [`ReadRequest::BlocksByHeightRange`], with an additional byte
    /// cap. Each block's size is checked against the remaining cap before it is
    /// copied, and the prefix ends at the first block that does not fit. See
    /// [`BlockRangeSnapshot::serialized_block_within`] for how finalized and
    /// non-finalized blocks are read. One blocking job transfers its resources
    /// into the returned result.
    ///
    /// `is_cancelled` is checked before the first lookup and between lookups.
    /// Cancellation stops further lookups and returns the prefix already read.
    /// It cannot interrupt a database lookup already in progress. Dropping or
    /// aborting the caller discards delivery, but the job retains its resources
    /// until it finishes and drops its undelivered result.
    pub async fn read_owned_block_range<R: Send + 'static>(
        &mut self,
        start: block::Height,
        count: u32,
        max_response_bytes: u32,
        resources: R,
        is_cancelled: impl FnMut(&R) -> bool + Send + 'static,
    ) -> Result<OwnedBlockRange<R>, BoxError> {
        self.ready().await?;
        ReadRequest::BlocksByHeightRange { start, count }.count_metric();
        let state = self.clone();
        spawn_owned_block_range(
            start,
            count,
            max_response_bytes,
            resources,
            is_cancelled,
            move || {
                let snapshot = state.block_range_snapshot()?;
                Ok(move |height, remaining| {
                    snapshot.serialized_block_within(&state.db, height, remaining)
                })
            },
        )
        .await
    }
}

/// Spawn one blocking read that owns `resources` until it returns the blocks.
///
/// Dropping the returned future stops waiting for the result, but does not stop
/// the blocking job. Capture `resources` inside that job so caller cancellation
/// cannot release reservations still needed by the read. Move them into
/// [`OwnedBlockRange`] on completion so they remain with the returned blocks.
fn spawn_owned_block_range<R, G>(
    start: block::Height,
    count: u32,
    max_response_bytes: u32,
    resources: R,
    mut is_cancelled: impl FnMut(&R) -> bool + Send + 'static,
    prepare_read: impl FnOnce() -> Result<G, BoxError> + Send + 'static,
) -> BoxFuture<'static, Result<OwnedBlockRange<R>, BoxError>>
where
    R: Send + 'static,
    G: FnMut(block::Height, usize) -> Option<Vec<u8>>,
{
    let timed_span = TimedSpan::new(
        CodeTimer::start_desc("blocks_by_height_range"),
        Span::current(),
    );
    timed_span.spawn_blocking(move || {
        if is_cancelled(&resources) {
            return Ok(OwnedBlockRange {
                blocks: Vec::new(),
                resources,
            });
        }
        let mut get_block = prepare_read()?;
        let blocks =
            collect_bounded_height_range(start, count, max_response_bytes, |height, remaining| {
                if is_cancelled(&resources) {
                    return None;
                }
                let bytes = get_block(height, remaining)?;
                let size = bytes.len();
                Some((bytes, size))
            })
            .into_iter()
            .map(|(height, bytes, _)| (height, bytes))
            .collect();
        Ok(OwnedBlockRange { blocks, resources })
    })
}

/// Read a contiguous prefix without retaining more encoded block bytes than
/// the caller permits.
///
/// Each lookup receives the remaining byte budget and should return `None`,
/// without materializing the block, when that block does not fit. A lookup
/// that returns an oversized block anyway still ends the prefix, and that
/// block never enters the returned response.
fn collect_bounded_height_range<T>(
    start: block::Height,
    count: u32,
    max_response_bytes: u32,
    mut get_block: impl FnMut(block::Height, usize) -> Option<(T, usize)>,
) -> Vec<(block::Height, T, usize)> {
    let mut remaining =
        usize::try_from(max_response_bytes).expect("u32 fits usize on supported targets");
    (0..count)
        .map_while(|offset| {
            let height = start.0.checked_add(offset).map(block::Height)?;
            let (block, size) = get_block(height, remaining)?;
            remaining = remaining.checked_sub(size)?;
            Some((height, block, size))
        })
        .collect()
}

#[cfg(test)]
mod storage_probe;
#[cfg(test)]
mod tests;
