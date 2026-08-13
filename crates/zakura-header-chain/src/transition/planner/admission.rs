//! Bounded admission, authentication, and header-insertion rebasing.

use std::collections::{HashMap, HashSet};

use zakura_chain::block;

use super::{InvalidTransitionEvidence, LimitViolation, TransitionFailure};
use crate::{
    BodyWorkOwner, EngineLimits, EngineMetadata, EngineMode, EngineSnapshot, EventAdmission,
    EvidenceId, FinalityRecord, Frontier, HeaderChainEngine, HeaderSyncWorkOwner, MemHeaderStore,
    TargetCompletion, TransitionContext, TransitionEvent, TransitionInput,
};

/// Request admitted against its original authority and resource bounds.
///
/// Header work can still be canonically rebased before freshness binding, so
/// this type does not claim that its event bytes remain authenticated.
#[derive(Debug)]
pub(super) struct AdmittedRequest {
    /// Admitted event ready for canonical rebasing and freshness binding.
    pub(super) event: TransitionEvent,
    /// Caller-observed durable version when the input is version-qualified.
    pub(super) expected_version: Option<crate::StateVersion>,
}

/// How a pure header insertion related to newer monotone finality.
///
/// Maps to published [`crate::HeaderWorkEffect`] via [`Self::header_work_effect`].
/// Replay binding uses that conversion when publishing an early no-change outcome.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum HeaderInsertionRebase {
    /// The insertion already targeted the current finality anchor.
    Current,
    /// The insertion was rewritten onto newer monotone finality.
    Rebased,
    /// Newer finality fully consumed the prepared batch.
    AlreadyApplied,
}

impl HeaderInsertionRebase {
    /// Authoritative conversion into the published header-work effect.
    ///
    /// - [`Self::Current`] contributes no header-work effect by itself (finality
    ///   settlement may still set [`crate::HeaderWorkEffect::Rebased`] when work
    ///   coordinates rebase independently).
    /// - [`Self::Rebased`] → [`crate::HeaderWorkEffect::Rebased`]
    /// - [`Self::AlreadyApplied`] → [`crate::HeaderWorkEffect::AlreadyApplied`]
    ///   (surfaced as a verified no-change before settlement).
    pub(super) const fn header_work_effect(self) -> Option<crate::HeaderWorkEffect> {
        match self {
            Self::Current => None,
            Self::Rebased => Some(crate::HeaderWorkEffect::Rebased),
            Self::AlreadyApplied => Some(crate::HeaderWorkEffect::AlreadyApplied),
        }
    }
}

/// Authenticate the caller and admit the event before any projection work.
pub(super) fn authenticate_and_admit(
    engine: &HeaderChainEngine,
    input: &TransitionInput,
    context: &TransitionContext<'_>,
) -> Result<(EngineSnapshot, EngineMetadata, AdmittedRequest), TransitionFailure> {
    let snapshot_before_commit = engine.snapshot();
    let metadata = engine.metadata().clone();
    validate_snapshot(&snapshot_before_commit, &metadata, context)?;
    if context.retention_references.len() > context.config.limits.max_retention_references.get() {
        return Err(
            InvalidTransitionEvidence::Limit(LimitViolation::RetentionReferencesExceeded).into(),
        );
    }
    let event = input.event();
    validate_event_resource_bounds(engine, &event, context.config.limits)?;
    validate_authority(&event, context)?;
    Ok((
        snapshot_before_commit,
        metadata,
        AdmittedRequest {
            event,
            expected_version: input.expected_version(),
        },
    ))
}

/// Validate snapshot and persisted metadata against the active configuration.
pub(super) fn validate_snapshot(
    snapshot: &EngineSnapshot,
    metadata: &EngineMetadata,
    context: &TransitionContext<'_>,
) -> Result<(), TransitionFailure> {
    if snapshot.mode != context.config.mode
        || metadata.mode != context.config.mode
        || metadata.network_id != context.config.network.kind()
        || metadata.anchor_manifest_digest != context.config.trust_anchor_digest()
        || snapshot.state_version != metadata.state_version
        || snapshot.frontiers != metadata.frontiers
    {
        return Err(TransitionFailure::ConfigurationMismatch);
    }
    Ok(())
}

fn validate_event_resource_bounds(
    engine: &HeaderChainEngine,
    event: &TransitionEvent,
    limits: EngineLimits,
) -> Result<(), TransitionFailure> {
    let TransitionEvent::InsertHeaders(insert) = event else {
        return Ok(());
    };
    // Authoritative runtime batch-size gate against the active engine limits.
    // `PreparedHeaderBatch::new` also rejects batches above the frozen
    // `MAX_HEADERS_PER_TRANSITION_V1` constant at construction; unifying those
    // checks is deferred—treat this limits-aware check as planning authority.
    if insert.batch.headers().len() > limits.max_headers_per_transition.get() {
        return Err(
            InvalidTransitionEvidence::Limit(LimitViolation::PreparedHeadersExceeded).into(),
        );
    }
    if insert.aux.len() > limits.max_aux_deliveries_total.get() {
        return Err(TransitionFailure::AuxiliaryLimitExceeded);
    }
    let mut additions = HashMap::<block::Hash, HashSet<EvidenceId>>::new();
    for delivery in &insert.aux {
        additions
            .entry(delivery.header_hash)
            .or_default()
            .insert(delivery.delivery_id);
    }
    let mut new_total = engine.aux_delivery_count();
    for (hash, delivery_ids) in additions {
        let existing = engine.aux_deliveries(hash);
        let new_count = delivery_ids
            .iter()
            .filter(|id| !existing.iter().any(|row| row.delivery_id == **id))
            .count();
        if existing.len().saturating_add(new_count) > limits.max_aux_deliveries_per_header.get() {
            return Err(TransitionFailure::AuxiliaryLimitExceeded);
        }
        new_total = new_total.saturating_add(new_count);
    }
    if new_total > limits.max_aux_deliveries_total.get() {
        return Err(TransitionFailure::AuxiliaryLimitExceeded);
    }
    Ok(())
}

/// Enforce the event's admission gate against trusted authorities.
pub(super) fn validate_authority(
    event: &TransitionEvent,
    context: &TransitionContext<'_>,
) -> Result<(), TransitionFailure> {
    match event.admission() {
        EventAdmission::AnyMode => Ok(()),
        EventAdmission::IntegratedFullState if context.config.mode != EngineMode::Integrated => {
            Err(TransitionFailure::Mode)
        }
        EventAdmission::IntegratedFullState => {
            if context
                .full_state_authority
                .is_some_and(|authority| authority.authorizes_full_state(event))
            {
                Ok(())
            } else {
                Err(TransitionFailure::Authority)
            }
        }
        EventAdmission::RegisteredScheduler => {
            let TransitionEvent::OperatorBodyRetry(retry) = event else {
                return Err(TransitionFailure::Authority);
            };
            if context
                .full_state_authority
                .is_some_and(|authority| authority.authorizes_scheduler_retry(retry))
            {
                Ok(())
            } else {
                Err(TransitionFailure::Authority)
            }
        }
        EventAdmission::RegisteredHeaderCompletion => {
            let TransitionEvent::InsertHeaders(insert) = event else {
                return Err(TransitionFailure::Authority);
            };
            if context
                .full_state_authority
                .is_some_and(|authority| authority.authorizes_header_completion(insert))
            {
                Ok(())
            } else {
                Err(TransitionFailure::Authority)
            }
        }
    }
}

pub(super) fn validate_header_sync_owner(
    owner: HeaderSyncWorkOwner,
    snapshot_before_commit: &EngineSnapshot,
) -> Result<(), TransitionFailure> {
    let header = owner.header_authority();
    if header.header_generation != snapshot_before_commit.header_generation
        || owner.body_authority().is_some_and(|authority| {
            authority.verified_generation != snapshot_before_commit.verified_generation
        })
        || header.branch.anchor_hash != snapshot_before_commit.frontiers.finalized.hash
    {
        return Err(TransitionFailure::Stale {
            current: snapshot_before_commit.state_version,
        });
    }
    Ok(())
}

pub(super) fn validate_body_owner(
    owner: BodyWorkOwner,
    snapshot_before_commit: &EngineSnapshot,
) -> Result<(), TransitionFailure> {
    if owner.header_generation != snapshot_before_commit.header_generation
        || owner.verified_generation != snapshot_before_commit.verified_generation
        || owner.branch.anchor_hash != snapshot_before_commit.frontiers.finalized.hash
    {
        return Err(TransitionFailure::Stale {
            current: snapshot_before_commit.state_version,
        });
    }
    Ok(())
}

pub(super) fn rebase_header_insertion(
    event: &mut TransitionEvent,
    current: &EngineSnapshot,
    graph: &MemHeaderStore,
    input: &TransitionInput,
) -> Result<HeaderInsertionRebase, TransitionFailure> {
    let TransitionEvent::InsertHeaders(insert) = event else {
        return Ok(HeaderInsertionRebase::Current);
    };
    if matches!(
        insert.completion,
        TargetCompletion::SelectedAuxiliaryRepair { .. }
    ) {
        return Ok(HeaderInsertionRebase::Current);
    }
    let Some(original_owner) = insert.owner.header_owner() else {
        return Ok(HeaderInsertionRebase::Current);
    };
    let original = original_owner.authority;
    if original.branch.target_tip_hash != insert.target_tip_hash {
        return Err(TransitionFailure::Stale {
            current: current.state_version,
        });
    }
    if original.header_generation == current.header_generation
        && original.branch.anchor_hash == current.frontiers.finalized.hash
    {
        return Ok(HeaderInsertionRebase::Current);
    }
    if original.branch.anchor_hash == current.frontiers.finalized.hash {
        return Err(TransitionFailure::Stale {
            current: current.state_version,
        });
    }
    if original.header_generation.get() >= current.header_generation.get() {
        return Err(TransitionFailure::Stale {
            current: current.state_version,
        });
    }
    let Some(finality_rebase_history) = input.finality_rebase_history() else {
        return Err(TransitionFailure::Stale {
            current: current.state_version,
        });
    };
    if finality_rebase_history.is_empty() {
        return Err(TransitionFailure::Stale {
            current: current.state_version,
        });
    }
    validate_finality_rebase_path(
        original.branch.anchor_hash,
        current.frontiers.finalized,
        finality_rebase_history,
    )?;

    let finalized = current.frontiers.finalized;
    let parent_is_current_descendant = match graph.header_node(insert.parent_hash) {
        Some(parent) if parent.height >= finalized.height => {
            graph.header_ancestor(insert.parent_hash, finalized.height)? == Some(finalized)
        }
        Some(_) | None => false,
    };
    let mut removed = 0usize;
    if !parent_is_current_descendant {
        if insert
            .batch
            .headers()
            .iter()
            .any(|header| Frontier::new(header.height, header.hash) == finalized)
        {
            removed = insert
                .batch
                .rebase_after(finalized)
                .map_err(|_| TransitionFailure::StalePreparation)?;
            insert.parent_hash = finalized.hash;
            insert
                .completion
                .rebase_common_ancestor(finalized)
                .map_err(|_| TransitionFailure::StalePreparation)?;
        } else {
            let prepared_tip = insert
                .batch
                .headers()
                .last()
                .map(|header| Frontier::new(header.height, header.hash))
                .ok_or(TransitionFailure::StalePreparation)?;
            let prepared_tip_was_finalized = finality_rebase_history
                .iter()
                .any(|record| record.previous == prepared_tip || record.current == prepared_tip);
            if prepared_tip_was_finalized && prepared_tip.height <= finalized.height {
                insert.batch.clear_already_applied();
                removed = insert.aux.len();
            } else {
                return Err(TransitionFailure::Stale {
                    current: current.state_version,
                });
            }
        }
    }

    let authority = crate::HeaderWorkAuthority {
        header_generation: current.header_generation,
        branch: crate::BranchId::new(finalized.hash, original.branch.target_tip_hash),
    };
    insert.owner = insert
        .owner
        .rebase_header(authority)
        .ok_or(TransitionFailure::Authority)?;
    for delivery in &mut insert.aux {
        delivery.owner = delivery
            .owner
            .rebase_header(authority)
            .ok_or(TransitionFailure::Authority)?;
    }
    if removed != 0 {
        let retained: HashSet<_> = insert
            .batch
            .headers()
            .iter()
            .map(|header| header.hash)
            .collect();
        insert
            .aux
            .retain(|delivery| retained.contains(&delivery.header_hash));
    }
    if insert.batch.headers().is_empty() {
        return Ok(HeaderInsertionRebase::AlreadyApplied);
    }
    Ok(HeaderInsertionRebase::Rebased)
}

/// Validate a durable finality-history suffix used to rebase header work.
pub(super) fn validate_finality_rebase_path(
    original_anchor: block::Hash,
    current_finalized: Frontier,
    path: &[FinalityRecord],
) -> Result<(), TransitionFailure> {
    if original_anchor == current_finalized.hash {
        return path
            .is_empty()
            .then_some(())
            .ok_or(TransitionFailure::StalePreparation);
    }
    let mut expected_frontier = None;
    let mut expected_epoch = None;
    for record in path {
        let predecessor_matches = expected_frontier.map_or_else(
            || record.previous.hash == original_anchor,
            |expected| record.previous == expected,
        );
        if !predecessor_matches
            || expected_epoch.is_some_and(|epoch| record.epoch != epoch)
            || record.current.height < record.previous.height
        {
            return Err(TransitionFailure::StalePreparation);
        }
        expected_frontier = Some(record.current);
        expected_epoch = Some(record.epoch.checked_next()?);
    }
    if expected_frontier != Some(current_finalized) {
        return Err(TransitionFailure::StalePreparation);
    }
    Ok(())
}
