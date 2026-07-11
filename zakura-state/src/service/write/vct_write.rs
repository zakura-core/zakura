//! Look-ahead buffering and root-stall tracking for the checkpoint write loop's
//! verified-commitment-trees (vct) fast path.

use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use tokio::sync::mpsc::UnboundedReceiver;
use tracing::info;
use zakura_chain::block::Height;

use crate::service::{
    finalized_state::{FinalizedState, NextVctBlock},
    queued_blocks::QueuedCheckpointVerified,
};

/// Delay between retryable VCT root-miss commit attempts. Nothing actively re-requests a
/// missing root, so this only polls for a re-delivery of the same header range (for example
/// another fanout peer's response); the slow poll keeps a persistent hole cheap to wait on.
const VCT_ROOT_RETRY_WAIT: Duration = Duration::from_millis(500);

/// Delay between retryable VCT await-successor commit attempts. Shorter than
/// [`VCT_ROOT_RETRY_WAIT`]: the root is already cached and only the next block needs to be
/// downloaded into the look-ahead, so a tighter poll keeps the one-block commit lag small.
const VCT_AWAIT_SUCCESSOR_WAIT: Duration = Duration::from_millis(20);

/// How long a single checkpoint height may stay stuck on a retryable VCT root stall before
/// the committer escalates to an error-level log and a `state.vct.root.stalled.height` gauge.
/// Transient waits (a successor still downloading, a fanout re-delivery still in flight)
/// clear well within this; staying stuck past it means no verifiable root is available for a
/// height the frozen frontier requires, and — by design — the committer will not recompute
/// against the stale frontier, so the node cannot advance. Surfacing that loudly is the
/// operator's only signal.
const VCT_ROOT_STALL_WARN_AFTER: Duration = Duration::from_secs(30);

/// Look-ahead buffering and root-stall tracking for the checkpoint write
/// loop's verified-commitment-trees (vct) fast path. Bundles the state the
/// loop needs to authenticate a fast block's supplied roots against its
/// successor and to retry/escalate a stuck height, so their invariants
/// (single log per stall, look-ahead cleared on reset) live next to the data
/// they guard.
#[derive(Default)]
pub(super) struct VctWriteManager {
    /// One-block look-ahead: the current block's supplied roots are
    /// authenticated by the successor's header commitment.
    lookahead: VecDeque<QueuedCheckpointVerified>,
    /// A block parked for retry (awaiting a successor, or a missing root)
    /// instead of going through the invalid-block reset path.
    retry: Option<QueuedCheckpointVerified>,
    /// `(height, first-seen)` of the height currently stuck retrying, if any.
    stall: Option<(Height, Instant)>,
    /// Whether the current stall has already been escalated to an
    /// error-level log and gauge.
    stall_logged: bool,
}

impl VctWriteManager {
    /// Takes the next locally-buffered block ready to commit (a parked retry,
    /// then the look-ahead), if any.
    pub(super) fn take_ready(&mut self) -> Option<QueuedCheckpointVerified> {
        self.retry.take().or_else(|| self.lookahead.pop_front())
    }

    /// Clears the look-ahead and any cached successor prevalidation, for a
    /// queue reset (wrong-height block, or a hard commit failure).
    pub(super) fn reset(&mut self, finalized_state: &mut FinalizedState) {
        self.lookahead.clear();
        finalized_state.clear_vct_prevalidated_next();
    }

    /// Buffers the direct successor of `current` into the look-ahead, if available.
    ///
    /// Discards any buffered block that does not extend `current`: it cannot
    /// witness this commit or be committed next. Since retries are taken before
    /// the look-ahead, leaving a non-successor parked there could wedge the retry
    /// loop against the wrong witness. Dropping its response sender lets upstream
    /// redeliver the block.
    pub(super) fn fill_successor(
        &mut self,
        receiver: &mut UnboundedReceiver<QueuedCheckpointVerified>,
        current: &QueuedCheckpointVerified,
    ) {
        loop {
            let front_links = self
                .lookahead
                .front()
                .map(|next| next.0.block.header.previous_block_hash == current.0.hash);

            match front_links {
                Some(true) => break,
                Some(false) => {
                    let dropped = self
                        .lookahead
                        .pop_front()
                        .expect("the front entry was just inspected");
                    tracing::debug!(
                        current_height = ?current.0.height,
                        current_hash = ?current.0.hash,
                        dropped_height = ?dropped.0.height,
                        dropped_hash = ?dropped.0.hash,
                        "dropping a buffered block that does not extend the block being \
                         committed. Assuming a parent block failed, and dropping this block",
                    );
                }
                None => match receiver.try_recv() {
                    Ok(next) => self.lookahead.push_back(next),
                    Err(_) => break,
                },
            }
        }
    }

    /// `true` when no block is buffered as the look-ahead successor.
    pub(super) fn is_lookahead_empty(&self) -> bool {
        self.lookahead.is_empty()
    }

    /// The buffered successor block's header data, used to verify the
    /// current block's supplied vct roots before trusting them.
    pub(super) fn next_vct_block(&self) -> Option<NextVctBlock> {
        self.lookahead.front().map(|next| NextVctBlock {
            block: next.0.block.clone(),
            auth_data_root: next.0.auth_data_root,
        })
    }

    /// Parks `block` for retry instead of committing it now.
    pub(super) fn defer(&mut self, block: QueuedCheckpointVerified) {
        self.retry = Some(block);
    }

    /// A successful commit clears any vct root stall: logs recovery and
    /// resets the stalled-height gauge if the stall had been escalated.
    pub(super) fn on_commit_success(&mut self) {
        if self.stall.is_some() {
            if self.stall_logged {
                info!(
                    stalled_height = ?self.stall.map(|(h, _)| h),
                    "VCT: checkpoint commit recovered; the stalled height now has a verifiable supplied root"
                );
                metrics::gauge!("state.vct.root.stalled.height").set(0.0);
            }
            self.stall = None;
            self.stall_logged = false;
        }
    }

    /// Tracks and, past the warn threshold, escalates a retryable vct root
    /// stall at `height`, parks `block` for retry, and returns how long the
    /// caller should park before retrying.
    pub(super) fn on_retryable_error(
        &mut self,
        height: Height,
        root_unavailable: bool,
        block: QueuedCheckpointVerified,
    ) -> Duration {
        metrics::counter!("state.vct.root.retry.count").increment(1);

        // Escalate a stall that persists on the same height past the warn
        // threshold: a transient wait resolves in a few polls and stays
        // quiet, but a height stuck longer means no root the frozen frontier
        // requires is available — roots are not individually re-requested, so
        // the node will not advance (it will not, by design, recompute against
        // the stale frontier). Surface it loudly.
        match self.stall {
            Some((stuck, _)) if stuck == height => {}
            _ => {
                self.stall = Some((height, Instant::now()));
                self.stall_logged = false;
            }
        }
        if !self.stall_logged
            && self
                .stall
                .is_some_and(|(_, since)| since.elapsed() >= VCT_ROOT_STALL_WARN_AFTER)
        {
            tracing::error!(
                ?height,
                root_unavailable,
                stalled_for = ?VCT_ROOT_STALL_WARN_AFTER,
                "VCT: checkpoint commit stalled with no verifiable supplied root; \
                 roots are not re-requested, so the node cannot advance without a \
                 re-delivery of this header range (it will not recompute against \
                 the frozen frontier)"
            );
            metrics::gauge!("state.vct.root.stalled.height").set(f64::from(height.0));
            self.stall_logged = true;
        } else {
            tracing::warn!(
                ?height,
                block_height = ?block.0.height,
                block_hash = ?block.0.hash,
                root_unavailable,
                "VCT: supplied root not yet verifiable; retrying checkpoint commit in place"
            );
        }

        self.retry = Some(block);

        if root_unavailable {
            VCT_ROOT_RETRY_WAIT
        } else {
            VCT_AWAIT_SUCCESSOR_WAIT
        }
    }
}

#[cfg(test)]
mod tests;
