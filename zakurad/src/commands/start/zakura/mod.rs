use std::time::Duration;

/// Hands the Zakura block-sync bulk-apply pipeline over to legacy `ChainSync`
/// fallback.
///
/// Two sync engines submitting bulk commits concurrently race in the applying
/// queue, so fallback must be a commit barrier: once yielded to legacy sync,
/// the block-sync driver starts no new applies, and the watchdog waits for
/// in-flight applies to finish before resuming legacy sync. The Zakura reactors
/// stay alive throughout; only bulk body applies are gated.
#[derive(Debug)]
pub(crate) struct BlockSyncHandoff {
    yielded_to_legacy: std::sync::atomic::AtomicBool,
    in_flight: std::sync::atomic::AtomicUsize,
    drained: tokio::sync::Notify,
}

/// Tracks one in-flight Zakura block apply; dropping it releases the slot and
/// wakes a pending [`BlockSyncHandoff::yield_to_legacy`].
#[derive(Debug)]
pub(crate) struct BlockApplyPermit(std::sync::Arc<BlockSyncHandoff>);

impl BlockSyncHandoff {
    pub(crate) fn new() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            yielded_to_legacy: std::sync::atomic::AtomicBool::new(false),
            in_flight: std::sync::atomic::AtomicUsize::new(0),
            drained: tokio::sync::Notify::new(),
        })
    }

    /// Whether the pipeline has been yielded to legacy sync.
    pub(crate) fn is_yielded_to_legacy(&self) -> bool {
        self.yielded_to_legacy
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Returns a permit for one block apply, or `None` once the pipeline has
    /// been yielded to legacy sync.
    pub(crate) fn begin_apply(self: &std::sync::Arc<Self>) -> Option<BlockApplyPermit> {
        if self.is_yielded_to_legacy() {
            return None;
        }

        self.in_flight
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // Load-bearing invariant: reserve before the second yielded check so a
        // concurrent fallback either sees this apply in `in_flight` or rejects
        // the permit and releases it here. That makes the drain a real commit
        // barrier without locking the hot path.
        if self.is_yielded_to_legacy() {
            self.release();
            return None;
        }

        Some(BlockApplyPermit(self.clone()))
    }

    fn release(&self) {
        if self
            .in_flight
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst)
            == 1
        {
            self.drained.notify_waiters();
        }
    }

    /// Yields the apply pipeline to legacy sync and waits until in-flight
    /// applies drain, bounded by `timeout`.
    pub(crate) async fn yield_to_legacy(&self, timeout: Duration) {
        self.stop_new_applies();
        self.wait_for_applies(timeout).await;
    }

    fn stop_new_applies(&self) {
        self.yielded_to_legacy
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    async fn wait_for_applies(&self, timeout: Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let drained = self.drained.notified();
            tokio::pin!(drained);
            drained.as_mut().enable();

            let in_flight = self.in_flight.load(std::sync::atomic::Ordering::SeqCst);
            if in_flight == 0 {
                return;
            }

            if tokio::time::timeout_at(deadline, drained).await.is_err() {
                tracing::warn!(
                    in_flight,
                    "timed out draining Zakura block applies before legacy fallback; \
                     remaining applies resolve through their own driver timeouts"
                );
                return;
            }
        }
    }
}

impl Drop for BlockApplyPermit {
    fn drop(&mut self) {
        self.0.release();
    }
}

pub(crate) mod block_sync_driver;
pub(crate) mod frontier;
pub(crate) mod header_sync_driver;
pub(crate) mod throughput_probe;
pub(crate) mod trace;

pub(crate) use block_sync_driver::drive_block_sync_actions;
#[cfg(test)]
pub(crate) use block_sync_driver::{
    abandoned_block_apply_finished_event, apply_block_sync_body, block_apply_class,
    block_sync_missing_body_window, block_sync_needed_blocks_from_state,
    coalesce_ready_needed_block_queries, coalesce_stale_needed_block_queries,
    commit_block_sync_body, query_block_sync_needed_blocks, BlockApplyClass,
    ZAKURA_BLOCK_SYNC_CHECKPOINT_FRONTIER_REFRESH_INTERVAL, ZAKURA_BLOCK_SYNC_MISSING_BODY_WINDOW,
};
pub(crate) use frontier::{query_block_sync_frontiers, verified_block_tip_from_state};
#[cfg(test)]
pub(crate) use header_sync_driver::{
    block_roots_cover_range, block_sync_chain_tip_event, body_sizes_for_served_header_range,
    chain_tip_mirror_frontier_change, header_range_commit_error_label,
    header_range_commit_failure_kind, notify_block_sync_header_tip,
    root_covered_query_best_header_tip, tree_aux_roots_for_served_header_range,
};
pub(crate) use header_sync_driver::{
    drive_vct_root_repairs, drive_zakura_header_sync_actions, mirror_zakura_full_block_commits,
    zakura_header_sync_driver_startup, ZakuraHeaderSyncDriverHandles,
};
pub(crate) use throughput_probe::{BlocksyncThroughputProbe, BlocksyncThroughputSummary};

pub(crate) const ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) fn block_verify_error_is_duplicate<Error>(error: &Error) -> bool
where
    Error: std::fmt::Debug + Send + Sync + 'static,
{
    let error = error as &dyn std::any::Any;

    error
        .downcast_ref::<zakura_consensus::RouterError>()
        .is_some_and(zakura_consensus::RouterError::is_duplicate_request)
        || error
            .downcast_ref::<zakura_consensus::VerifyBlockError>()
            .is_some_and(zakura_consensus::VerifyBlockError::is_duplicate_request)
        || error
            .downcast_ref::<zakura_consensus::BoxError>()
            .is_some_and(|error| {
                error
                    .downcast_ref::<zakura_consensus::RouterError>()
                    .is_some_and(zakura_consensus::RouterError::is_duplicate_request)
                    || error
                        .downcast_ref::<zakura_consensus::VerifyBlockError>()
                        .is_some_and(zakura_consensus::VerifyBlockError::is_duplicate_request)
            })
}
