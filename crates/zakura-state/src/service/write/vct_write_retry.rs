//! Retry and repair state for the committer's verified-commitment-trees (VCT) path.

use std::time::{Duration, Instant};

use tokio::sync::watch;
use tracing::info;
use zakura_chain::block::Height;

use crate::service::{
    finalized_state::FinalizedState,
    queued_blocks::QueuedCheckpointVerified,
    write::{VctRootRepairState, VctRootRepairStatus},
};

/// Delay between commit attempts that lack a VCT root.
///
/// The repair request asks header sync to replace the metadata. The slow poll limits checkpoint
/// committer work while the replacement remains unavailable.
const VCT_ROOT_RETRY_WAIT: Duration = Duration::from_millis(500);

/// Delay between commit attempts that lack a VCT successor witness.
///
/// The root already exists. The shorter delay limits the one-block commit lag while the state
/// waits for the successor header.
const VCT_AWAIT_SUCCESSOR_WAIT: Duration = Duration::from_millis(20);

/// Maximum time a checkpoint height may remain in a retryable VCT root stall.
/// The committer reports longer stalls through an error-level log and the
/// `state.vct.root.stalled.height` gauge. Successor downloads and fanout deliveries should finish
/// within this interval. A longer stall means the frozen frontier requires a height without a
/// verifiable root. The committer will not recompute against the stale frontier. The node
/// cannot advance until a peer supplies a verifiable root. The log and gauge notify the operator.
const VCT_ROOT_STALL_WARN_AFTER: Duration = Duration::from_secs(30);

/// Manages retryable checkpoint blocks and VCT metadata repair requests.
pub(super) struct VctWriteRetryManager {
    /// Checkpoint block that the writer parked until VCT metadata becomes verifiable.
    retryable_block: Option<QueuedCheckpointVerified>,
    /// Height and start time for the active VCT metadata stall.
    root_stall: Option<(Height, Instant)>,
    /// Whether the manager reported the active stall at error level.
    root_stall_reported: bool,
    /// Broadcasts missing-root repair needs to node orchestration.
    root_repair_sender: watch::Sender<VctRootRepairStatus>,
    /// Last repair status that the manager published.
    root_repair_status: VctRootRepairStatus,
    /// Lowest metadata height that blocks the committer.
    committer_repair_height: Option<Height>,
    /// Lowest metadata height that blocks the authentication sweep.
    ///
    /// The sweep runs far above the committer, so this height usually exceeds the committer repair
    /// height. The manager tracks both heights because a successful block
    /// commit must not clear a repair that the sweep still needs.
    sweep_repair_height: Option<Height>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum VctRepairRequester {
    Committer,
    Sweep,
}

/// VCT metadata condition that makes a checkpoint block retryable.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum VctWriteRetryCause {
    /// The committer cannot obtain a verifiable root.
    MissingRoot {
        /// The writer rejected or disputed an existing delivery before requesting replacement.
        replacement_required: bool,
    },
    /// The state has not stored the successor header that authenticates the root.
    MissingSuccessor,
}

impl Default for VctWriteRetryManager {
    fn default() -> Self {
        let (root_repair_sender, _root_repair_receiver) =
            watch::channel(VctRootRepairStatus::default());
        Self::new(root_repair_sender)
    }
}

impl VctWriteRetryManager {
    /// Creates a retry manager that publishes repair requests through `root_repair_sender`.
    pub(super) fn new(root_repair_sender: watch::Sender<VctRootRepairStatus>) -> Self {
        Self {
            retryable_block: None,
            root_stall: None,
            root_stall_reported: false,
            root_repair_sender,
            root_repair_status: VctRootRepairStatus::default(),
            committer_repair_height: None,
            sweep_repair_height: None,
        }
    }

    /// Requests replacement metadata for the sweep at `height`.
    pub(super) fn request_sweep_repair(&mut self, height: Height) {
        let starts_new_episode = self.sweep_repair_height != Some(height);
        self.sweep_repair_height = Some(height);
        self.publish_effective_repair_status(
            starts_new_episode.then_some(VctRepairRequester::Sweep),
        );
    }

    /// Clears the sweep repair request after the sweep re-verifies the requested height.
    pub(super) fn clear_sweep_repair(&mut self) {
        self.sweep_repair_height = None;
        self.publish_effective_repair_status(None);
    }

    pub(super) fn sweep_repair_height(&self) -> Option<Height> {
        self.sweep_repair_height
    }

    /// Raises the committer repair height without parking a block.
    #[cfg(test)]
    pub(super) fn request_committer_repair_for_test(&mut self, height: Height) {
        self.request_committer_repair(height, true);
    }

    /// Takes the checkpoint block that the writer parked for retry.
    pub(super) fn take_retryable_block(&mut self) -> Option<QueuedCheckpointVerified> {
        self.retryable_block.take()
    }

    /// Clears cached successor prevalidation after a queue reset.
    ///
    /// The reset also clears the committer repair request. The next commit attempt starts
    /// a new repair generation when the metadata remains unavailable.
    pub(super) fn reset(&mut self, finalized_state: &mut FinalizedState) {
        finalized_state.clear_vct_prevalidated_next();
        self.clear_committer_repair();
    }

    /// Clears the committer stall and repair request after a successful commit.
    ///
    /// The manager also clears the stalled-height gauge when it previously reported the stall.
    pub(super) fn on_commit_success(&mut self) {
        if self.root_stall.is_some() {
            if self.root_stall_reported {
                info!(
                    stalled_height = ?self.root_stall.map(|(height, _)| height),
                    "VCT: checkpoint commit recovered; the stalled height now has a verifiable supplied root"
                );
                metrics::gauge!("state.vct.root.stalled.height").set(0.0);
            }
            self.root_stall = None;
            self.root_stall_reported = false;
        }
        self.clear_committer_repair();
    }

    /// Parks `block` and records a retryable VCT metadata stall at `height`.
    ///
    /// The manager reports a persistent stall after [`VCT_ROOT_STALL_WARN_AFTER`]. The returned
    /// duration tells the committer when to retry the block.
    pub(super) fn on_retryable_error(
        &mut self,
        height: Height,
        retry_cause: VctWriteRetryCause,
        block: QueuedCheckpointVerified,
    ) -> Duration {
        metrics::counter!("state.vct.root.retry.count").increment(1);
        if let VctWriteRetryCause::MissingRoot {
            replacement_required,
        } = retry_cause
        {
            self.request_committer_repair(height, replacement_required);
        }

        // The manager reports only stalls that exceed the warning threshold. Transient stalls stay
        // below error level.
        let new_stall = match self.root_stall {
            Some((stalled_height, _)) if stalled_height == height => false,
            _ => {
                self.root_stall = Some((height, Instant::now()));
                self.root_stall_reported = false;
                true
            }
        };
        if !self.root_stall_reported
            && self
                .root_stall
                .is_some_and(|(_, since)| since.elapsed() >= VCT_ROOT_STALL_WARN_AFTER)
        {
            tracing::error!(
                ?height,
                ?retry_cause,
                stalled_for = ?VCT_ROOT_STALL_WARN_AFTER,
                "VCT: checkpoint commit stalled waiting for a verifiable supplied root \
                 or successor witness; the node will not recompute against the frozen frontier"
            );
            metrics::gauge!("state.vct.root.stalled.height").set(f64::from(height.0));
            self.root_stall_reported = true;
        } else if new_stall {
            tracing::warn!(
                ?height,
                block_height = ?block.0.height,
                block_hash = ?block.0.hash,
                ?retry_cause,
                "VCT: supplied root not yet verifiable; retrying checkpoint commit in place"
            );
        } else {
            tracing::trace!(
                ?height,
                block_height = ?block.0.height,
                block_hash = ?block.0.hash,
                ?retry_cause,
                "VCT: supplied root still not verifiable; retrying checkpoint commit in place"
            );
        }

        self.retryable_block = Some(block);

        match retry_cause {
            VctWriteRetryCause::MissingRoot { .. } => VCT_ROOT_RETRY_WAIT,
            VctWriteRetryCause::MissingSuccessor => VCT_AWAIT_SUCCESSOR_WAIT,
        }
    }

    fn request_committer_repair(&mut self, height: Height, replacement_required: bool) {
        self.committer_repair_height = Some(height);
        // Replacing a candidate starts a new repair episode even at the same height; a bare
        // re-poll of the same stall must not.
        self.publish_effective_repair_status(
            replacement_required.then_some(VctRepairRequester::Committer),
        );
    }

    fn clear_committer_repair(&mut self) {
        self.committer_repair_height = None;
        self.publish_effective_repair_status(None);
    }

    /// Publishes the lowest outstanding repair height.
    ///
    /// The repair channel stores one latest value. The manager publishes the lower height because
    /// its replacement unblocks the higher repair.
    fn publish_effective_repair_status(
        &mut self,
        requester_with_new_episode: Option<VctRepairRequester>,
    ) {
        let effective_repair = match (self.committer_repair_height, self.sweep_repair_height) {
            (Some(committer), Some(sweep)) if committer <= sweep => {
                Some((committer, VctRepairRequester::Committer))
            }
            (Some(_), Some(sweep)) => Some((sweep, VctRepairRequester::Sweep)),
            (Some(committer), None) => Some((committer, VctRepairRequester::Committer)),
            (None, Some(sweep)) => Some((sweep, VctRepairRequester::Sweep)),
            (None, None) => None,
        };
        let repair_state = effective_repair.map_or(VctRootRepairState::Idle, |(height, _)| {
            VctRootRepairState::Unavailable { height }
        });
        let effective_requester = effective_repair.map(|(_, requester)| requester);
        let effective_episode_changed = requester_with_new_episode.is_some_and(|requester| {
            effective_requester == Some(requester) && repair_state == self.root_repair_status.state
        });
        if repair_state == self.root_repair_status.state && !effective_episode_changed {
            return;
        }

        let repair_requested = repair_state != VctRootRepairState::Idle;
        self.root_repair_status = VctRootRepairStatus {
            state: repair_state,
            generation: if repair_requested {
                self.root_repair_status.generation.saturating_add(1)
            } else {
                self.root_repair_status.generation
            },
        };
        let _ = self.root_repair_sender.send(self.root_repair_status);
        if repair_requested {
            metrics::counter!("state.vct.root.repair.requested").increment(1);
        }
    }
}

#[cfg(test)]
mod tests;
