//! Finality, retention, and projection settlement after event evidence.

use std::borrow::Cow;

use super::{
    FinalityViolation, InvalidTransitionEvidence, PlannerCoherenceViolation, TransitionFailure,
};
use crate::graph::HeaderGraphView;
use crate::{
    AuxiliaryEffect, EngineMetadata, EngineMode, EngineSnapshot, FinalityEffect, FinalityRecord,
    FinalitySource, Frontier, HeaderChainEngine, HeaderWorkEffect, TransitionContext,
    TransitionEffect, TransitionEvent,
};

use super::admission::HeaderInsertionRebase;
use super::projected_state::{path, ProjectedTransitionState, SettledProjectedState};
use super::retention::RetentionPlan;

/// Inputs required to settle finality and retention after event evidence.
pub(super) struct SettlementInputs<'engine, 'ctx> {
    pub(super) engine: &'engine HeaderChainEngine,
    pub(super) projected: ProjectedTransitionState<'engine>,
    pub(super) metadata: EngineMetadata,
    pub(super) snapshot_before_commit: &'ctx EngineSnapshot,
    pub(super) event: &'ctx TransitionEvent,
    pub(super) header_rebase: HeaderInsertionRebase,
    pub(super) context: &'ctx TransitionContext<'ctx>,
    pub(super) old_selected: &'engine [Frontier],
}

/// Settled projections ready for write-set assembly.
pub(super) struct SettledTransition<'a> {
    /// Projected graph after finality and retention.
    pub(super) projected: SettledProjectedState<'a>,
    /// Selected projection after settlement.
    pub(super) selected: Cow<'a, [Frontier]>,
    /// Optional finality record to append.
    pub(super) finality_append: Option<FinalityRecord>,
    /// Retention outcome.
    pub(super) retention: RetentionPlan,
    /// Orthogonal transition effects.
    pub(super) effect: TransitionEffect,
    /// Updated metadata carrying work-origin and alarm side effects.
    pub(super) metadata: EngineMetadata,
}

/// Outcome of finality and retention settlement.
pub(super) enum FinalityRetentionOutcome<'a> {
    /// Settlement completed and write assembly may proceed.
    Settled(Box<SettledTransition<'a>>),
    /// Protected paths made limits unenforceable; discard event effects.
    ResourceStalled,
}

/// Decide and apply finality, enforce retention, and settle projections.
pub(super) fn derive_finality_and_retention<'engine, 'ctx>(
    inputs: SettlementInputs<'engine, 'ctx>,
) -> Result<FinalityRetentionOutcome<'engine>, TransitionFailure> {
    let SettlementInputs {
        engine,
        mut projected,
        mut metadata,
        snapshot_before_commit,
        event,
        header_rebase,
        context,
        old_selected,
    } = inputs;
    let work_rebased = projected.work_coordinates_rebased();
    if work_rebased {
        metadata.work_origin = projected.graph().view_finalized_frontier();
    }
    projected.refresh_verified_after_operator_change()?;

    let (mut selected_tip, _) = projected.graph().view_select_best_header_chain()?;
    let full_state_finalized = match event {
        TransitionEvent::FullStateFinalized(event) => {
            Some((event.new_finalized, event.full_state_transition_id))
        }
        TransitionEvent::VerifiedChainChanged(event)
            if event.cause == crate::VerifiedChangeCause::CheckpointFinalizedGrow =>
        {
            event.new_path.last().map(|header| {
                (
                    Frontier::new(header.height, header.hash),
                    event.full_state_transition_id,
                )
            })
        }
        _ => None,
    };

    let mut finality = None;
    if let Some((new_finalized, evidence)) = full_state_finalized {
        if new_finalized.height < snapshot_before_commit.frontiers.finalized.height {
            return Err(InvalidTransitionEvidence::Finality(FinalityViolation::Retreated).into());
        }
        if !projected.verified().contains(&new_finalized) {
            return Err(InvalidTransitionEvidence::Finality(
                FinalityViolation::OutsideVerifiedProjection,
            )
            .into());
        }
        finality = Some((new_finalized, FinalitySource::FullState { evidence }));
    } else if context.config.mode == EngineMode::HeadersOnly {
        let depth = context.config.limits.local_finality_depth.get();
        if selected_tip
            .height
            .0
            .saturating_sub(projected.graph().view_finalized_frontier().height.0)
            > depth
        {
            let height = zakura_chain::block::Height(selected_tip.height.0 - depth);
            let new_finalized = projected
                .graph()
                .view_header_ancestor(selected_tip.hash, height)?
                .ok_or(TransitionFailure::InvalidEvidence(
                    InvalidTransitionEvidence::Planner(
                        PlannerCoherenceViolation::IncompleteSelectedAncestry,
                    ),
                ))?;
            finality = Some((
                new_finalized,
                FinalitySource::HeadersOnlyDepth { selected_tip },
            ));
        }
    }

    let mut effect = TransitionEffect::none();
    debug_assert_ne!(
        header_rebase,
        HeaderInsertionRebase::AlreadyApplied,
        "already-applied header work returns during replay binding before settlement"
    );
    effect.header_work = settlement_header_work_effect(work_rebased, header_rebase);
    if matches!(
        event,
        TransitionEvent::VerifiedChainChanged(ref event)
            if event.cause == crate::VerifiedChangeCause::CheckpointFinalizedGrow
    ) {
        effect.finality = Some(FinalityEffect::Checkpoint);
    } else if matches!(event, TransitionEvent::AuxEvidence(_)) {
        effect.auxiliary = Some(AuxiliaryEffect::Authentication);
    } else if matches!(event, TransitionEvent::FullStateFinalized(_)) {
        // Finality effect is set when a record is actually appended below.
    }

    let mut finality_append = None;
    if let Some((new_finalized, source)) = finality {
        if new_finalized != projected.graph().view_finalized_frontier() {
            let previous = projected.graph().view_finalized_frontier();
            let epoch = metadata.finality_epoch.checked_next()?;
            projected.advance_finality(new_finalized)?;
            finality_append = Some(FinalityRecord {
                previous,
                current: new_finalized,
                source,
                epoch,
            });
            selected_tip = projected.graph().view_select_best_header_chain()?.0;
            match source {
                FinalitySource::HeadersOnlyDepth { .. } => {
                    effect.finality = Some(FinalityEffect::HeadersOnlyDepth);
                }
                FinalitySource::FullState { .. } => {
                    if effect.finality.is_none() {
                        effect.finality = Some(FinalityEffect::FullState);
                    }
                }
                FinalitySource::MigratedHeadersOnly => {}
            }
        }
    }

    if context.config.mode == EngineMode::HeadersOnly {
        projected.force_headers_only_verified();
    }

    let authoritative_full_state_fork_set = matches!(
        event,
        TransitionEvent::VerifiedChainChanged(_) | TransitionEvent::VerifiedBlockAccepted(_)
    ) && context
        .full_state_authority
        .is_some_and(|authority| authority.authorizes_full_state(event));
    let retention = projected.enforce_retention(
        selected_tip,
        !authoritative_full_state_fork_set,
        context.retention_references.iter().copied(),
        context.config.limits,
    )?;
    if retention.admission_refused {
        return Ok(FinalityRetentionOutcome::ResourceStalled);
    }

    selected_tip = projected.graph().view_select_best_header_chain()?.0;
    let selected = if effect.is_checkpoint_finality()
        && selected_tip == snapshot_before_commit.frontiers.header_best
        && finality_append.is_some_and(|record| {
            old_selected
                .binary_search_by_key(&record.current.height, |frontier| frontier.height)
                .ok()
                .is_some_and(|index| old_selected[index] == record.current)
        }) {
        let Some(record) = finality_append else {
            return Err(InvalidTransitionEvidence::Planner(
                PlannerCoherenceViolation::MissingCheckpointRecord,
            )
            .into());
        };
        let index = old_selected
            .binary_search_by_key(&record.current.height, |frontier| frontier.height)
            .map_err(|_| {
                InvalidTransitionEvidence::Planner(
                    PlannerCoherenceViolation::CheckpointOutsideSelection,
                )
            })?;
        Cow::Borrowed(&old_selected[index..])
    } else {
        Cow::Owned(path(projected.graph(), selected_tip)?)
    };

    let projected = projected.finish_after_retention(engine)?;
    Ok(FinalityRetentionOutcome::Settled(Box::new(
        SettledTransition {
            projected,
            selected,
            finality_append,
            retention,
            effect,
            metadata,
        },
    )))
}

/// Apply a migrated-pin refutation alarm before settlement when durable facts authenticate it.
pub(super) fn apply_migrated_pin_alarm(metadata: &mut EngineMetadata, pin: Option<Frontier>) {
    if let Some(pin) = pin {
        metadata.alarms.migrated_pin_refuted = Some(pin);
    }
}

fn settlement_header_work_effect(
    work_rebased: bool,
    header_rebase: HeaderInsertionRebase,
) -> Option<HeaderWorkEffect> {
    (work_rebased || header_rebase == HeaderInsertionRebase::Rebased)
        .then_some(HeaderWorkEffect::Rebased)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_work_effect_preserves_settlement_fallbacks() {
        assert_eq!(
            settlement_header_work_effect(false, HeaderInsertionRebase::Current),
            None
        );
        assert_eq!(
            settlement_header_work_effect(false, HeaderInsertionRebase::Rebased),
            Some(HeaderWorkEffect::Rebased)
        );
        assert_eq!(
            settlement_header_work_effect(false, HeaderInsertionRebase::AlreadyApplied),
            None
        );

        for header_rebase in [
            HeaderInsertionRebase::Current,
            HeaderInsertionRebase::Rebased,
            HeaderInsertionRebase::AlreadyApplied,
        ] {
            assert_eq!(
                settlement_header_work_effect(true, header_rebase),
                Some(HeaderWorkEffect::Rebased)
            );
        }
    }
}
