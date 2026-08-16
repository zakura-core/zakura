use std::time::Duration;

pub(crate) mod block_sync_driver;
mod coordinator;
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
    ZAKURA_BLOCK_SYNC_MISSING_BODY_WINDOW,
};
pub(crate) use coordinator::{
    BlockApplyOperation, BlockApplyTerminal, LegacyFallbackLease, SyncCoordinator,
};
pub(crate) use frontier::{query_block_sync_frontiers, verified_block_tip_from_state};
pub(crate) use header_sync_driver::zakura_header_sync_driver_startup;
#[cfg(test)]
pub(crate) use header_sync_driver::{block_roots_cover_range, root_covered_query_best_header_tip};
pub(crate) use throughput_probe::{BlocksyncThroughputProbe, BlocksyncThroughputSummary};

pub(crate) const ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT: Duration = Duration::from_secs(30);
pub(crate) const ZAKURA_HEADER_SYNC_DRIVER_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) fn block_verify_error_class<Error>(
    error: &Error,
) -> zakura_header_chain::BodyVerificationClass
where
    Error: std::fmt::Debug + Send + Sync + 'static,
{
    use zakura_header_chain::{BodyVerificationClass, TransientBodyFailureKind};

    fn classify(error: &(dyn std::any::Any + Send + Sync)) -> Option<BodyVerificationClass> {
        error
            .downcast_ref::<zakura_consensus::RouterError>()
            .map(zakura_consensus::RouterError::body_verification_class)
            .or_else(|| {
                error
                    .downcast_ref::<zakura_consensus::VerifyBlockError>()
                    .map(zakura_consensus::VerifyBlockError::body_verification_class)
            })
            .or_else(|| {
                error
                    .downcast_ref::<zakura_consensus::VerifyCheckpointError>()
                    .map(zakura_consensus::VerifyCheckpointError::body_verification_class)
            })
    }

    fn classify_box(error: &zakura_consensus::BoxError) -> Option<BodyVerificationClass> {
        error
            .downcast_ref::<zakura_consensus::RouterError>()
            .map(zakura_consensus::RouterError::body_verification_class)
            .or_else(|| {
                error
                    .downcast_ref::<zakura_consensus::VerifyBlockError>()
                    .map(zakura_consensus::VerifyBlockError::body_verification_class)
            })
            .or_else(|| {
                error
                    .downcast_ref::<zakura_consensus::VerifyCheckpointError>()
                    .map(zakura_consensus::VerifyCheckpointError::body_verification_class)
            })
    }

    let error = error as &(dyn std::any::Any + Send + Sync);
    classify(error)
        .or_else(|| {
            error
                .downcast_ref::<zakura_consensus::BoxError>()
                .and_then(classify_box)
        })
        .unwrap_or(BodyVerificationClass::Retryable(
            TransientBodyFailureKind::VerifierUnavailable,
        ))
}

pub(crate) fn block_verify_error_diagnostic<Error>(error: &Error) -> Option<String>
where
    Error: std::fmt::Debug + Send + Sync + 'static,
{
    fn error_chain(error: &(dyn std::error::Error + 'static)) -> String {
        let mut messages = Vec::new();
        let mut current = Some(error);
        while let Some(error) = current {
            messages.push(error.to_string());
            current = error.source();
        }
        messages.join(": ")
    }

    fn diagnose(error: &(dyn std::any::Any + Send + Sync)) -> Option<String> {
        error
            .downcast_ref::<zakura_consensus::RouterError>()
            .map(|error| error_chain(error))
            .or_else(|| {
                error
                    .downcast_ref::<zakura_consensus::VerifyBlockError>()
                    .map(|error| error_chain(error))
            })
            .or_else(|| {
                error
                    .downcast_ref::<zakura_consensus::VerifyCheckpointError>()
                    .map(|error| error_chain(error))
            })
    }

    let error = error as &(dyn std::any::Any + Send + Sync);
    diagnose(error).or_else(|| {
        error
            .downcast_ref::<zakura_consensus::BoxError>()
            .map(|error| error_chain(error.as_ref()))
    })
}
