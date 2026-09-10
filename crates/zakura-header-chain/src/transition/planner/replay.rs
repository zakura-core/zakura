//! Replay identity, async ownership, and version-freshness binding.

use crate::{
    EngineSnapshot, HeaderChainEngine, TransitionDomain, TransitionEffect, TransitionInput,
};

use super::admission::{
    rebase_header_insertion, validate_body_owner, validate_header_sync_owner, AdmittedRequest,
    HeaderInsertionRebase,
};
use super::TransitionFailure;

/// Admitted event after replay and freshness checks (or a short-circuit no-change).
pub(super) struct BoundRequest {
    pub(super) event: crate::TransitionEvent,
    pub(super) domain: TransitionDomain,
    pub(super) header_rebase: HeaderInsertionRebase,
    pub(super) no_change_effect: Option<TransitionEffect>,
    pub(super) full_state_authorization_version: Option<crate::StateVersion>,
}

/// Check replay identity, async ownership, and version freshness for an admitted request.
///
/// Returns `no_change_effect` when the event matches the last committed fingerprint or finality
/// already consumed the prepared headers; otherwise returns the event for further planning.
/// Conflicting replay keys or stale versions fail.
pub(super) fn bind_replay_and_freshness(
    engine: &HeaderChainEngine,
    input: &TransitionInput,
    snapshot_before_commit: &EngineSnapshot,
    metadata: &crate::EngineMetadata,
    request: AdmittedRequest,
) -> Result<BoundRequest, TransitionFailure> {
    let AdmittedRequest {
        mut event,
        expected_version,
        full_state_authorization_version,
    } = request;
    let domain = event.domain();
    let header_rebase =
        rebase_header_insertion(&mut event, snapshot_before_commit, engine.graph(), input)?;
    if let Some(owner) = event.header_sync_owner() {
        validate_header_sync_owner(owner, snapshot_before_commit)?;
    }
    if let Some(owner) = event.body_owner() {
        validate_body_owner(owner, snapshot_before_commit)?;
    }
    let fingerprint = event.fingerprint();
    // A conflicting replay key outranks every no-change short circuit: the same evidence carrying
    // a different payload is a distinct request, whether or not its headers are already applied.
    if metadata
        .last_transition
        .zip(fingerprint)
        .is_some_and(|(previous, current)| previous.conflicts_with(current))
    {
        return Err(TransitionFailure::ConflictingReplay);
    }
    if matches!(
        header_rebase.header_work_effect(),
        Some(crate::HeaderWorkEffect::AlreadyApplied)
    ) {
        return Ok(BoundRequest {
            event,
            domain,
            header_rebase,
            no_change_effect: Some(TransitionEffect::header_work_already_applied()),
            full_state_authorization_version,
        });
    }
    if fingerprint.is_some() && metadata.last_transition == fingerprint {
        return Ok(BoundRequest {
            event,
            domain,
            header_rebase,
            no_change_effect: Some(TransitionEffect::event()),
            full_state_authorization_version,
        });
    }
    let has_async_authority = event.header_sync_owner().is_some() || event.body_owner().is_some();
    if !has_async_authority {
        let Some(expected_version) = expected_version else {
            return Err(TransitionFailure::Stale {
                current: snapshot_before_commit.state_version,
            });
        };
        if expected_version != snapshot_before_commit.state_version {
            return Err(TransitionFailure::Stale {
                current: snapshot_before_commit.state_version,
            });
        }
    }
    Ok(BoundRequest {
        event,
        domain,
        header_rebase,
        no_change_effect: None,
        full_state_authorization_version,
    })
}
