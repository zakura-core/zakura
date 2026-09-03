//! Finality, retention, and projection settlement after event evidence.

use std::borrow::Cow;

use super::{
    FinalityViolation, InvalidTransitionEvidence, PlannerCoherenceViolation, TransitionFailure,
};
use crate::graph::HeaderGraphView;
use crate::{
    AuxiliaryEffect, EngineMetadata, EngineMode, EngineSnapshot, FinalityAncestryHeader,
    FinalityEffect, FinalityRecord, FinalitySource, Frontier, FullStateFinalityKind,
    FullStateFinalityProvenance, HeaderChainEngine, HeaderWorkEffect, TransitionContext,
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
    pub(super) old_verified: &'engine [Frontier],
    pub(super) full_state_authorization_version: Option<crate::StateVersion>,
}

/// Evidence that an appended finality record continues the prior projections.
///
/// Appending a finality record trims every projection entry below the new frontier.
/// When the trim removes a complete projection, the retained frontiers no longer show
/// whether the new lineage continues the old one, so body-work classification consults
/// this ancestry instead. The planner captures it before the finality advance deletes
/// the headers the ancestry walk reads.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct FinalityLineage {
    /// The new finalized frontier descends from the prior selected tip.
    pub(super) continues_selected: bool,
    /// The new finalized frontier descends from the prior verified tip.
    pub(super) continues_verified: bool,
}

/// Settled projections ready for write-set assembly.
pub(super) struct SettledTransition<'a> {
    /// Projected graph after finality and retention.
    pub(super) projected: SettledProjectedState<'a>,
    /// Selected projection after settlement.
    pub(super) selected: Cow<'a, [Frontier]>,
    /// Optional finality record to append.
    pub(super) finality_append: Option<FinalityRecord>,
    /// Ancestry evidence for the projections the finality append trims away.
    pub(super) finality_lineage: FinalityLineage,
    /// Canonical ancestry carried by a headers-only depth record.
    pub(super) finality_ancestry: crate::FinalityWitnessProof,
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
        old_verified,
        full_state_authorization_version,
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
        let kind = if checkpoint_finality_event(event) {
            FullStateFinalityKind::CheckpointGrow
        } else {
            FullStateFinalityKind::Finalized
        };
        finality = Some((
            new_finalized,
            FinalitySource::FullState {
                provenance: FullStateFinalityProvenance {
                    evidence,
                    state_version: full_state_authorization_version
                        .unwrap_or(snapshot_before_commit.state_version),
                    kind,
                },
            },
        ));
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

    let checkpoint_finality = matches!(
        event,
        TransitionEvent::VerifiedChainChanged(ref event)
            if event.cause == crate::VerifiedChangeCause::CheckpointFinalizedGrow
    );
    let mut effect = TransitionEffect::none();
    debug_assert_ne!(
        header_rebase,
        HeaderInsertionRebase::AlreadyApplied,
        "already-applied header work returns during replay binding before settlement"
    );
    effect.header_work = settlement_header_work_effect(work_rebased, header_rebase);
    if matches!(event, TransitionEvent::AuxEvidence(_)) {
        effect.auxiliary = Some(AuxiliaryEffect::Authentication);
    } else if matches!(event, TransitionEvent::FullStateFinalized(_)) {
        // Finality effect is set when a record is actually appended below.
    }

    let mut finality_append = None;
    let mut finality_lineage = FinalityLineage::default();
    let mut finality_ancestry = crate::FinalityWitnessProof::default();
    if let Some((new_finalized, source)) = finality {
        if new_finalized != projected.graph().view_finalized_frontier() {
            let previous = projected.graph().view_finalized_frontier();
            let epoch = metadata.finality_epoch.checked_next()?;
            finality_lineage = FinalityLineage {
                continues_selected: finality_continues_projection(
                    projected.graph(),
                    old_selected,
                    new_finalized,
                )?,
                continues_verified: finality_continues_projection(
                    projected.graph(),
                    old_verified,
                    new_finalized,
                )?,
            };
            if let FinalitySource::HeadersOnlyDepth { selected_tip } = source {
                finality_ancestry =
                    headers_only_finality_ancestry(projected.graph(), previous, selected_tip)?;
            }
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
                    effect.finality = Some(if checkpoint_finality {
                        FinalityEffect::Checkpoint
                    } else {
                        FinalityEffect::FullState
                    });
                }
                FinalitySource::MigratedHeadersOnly | FinalitySource::DiskMigration { .. } => {}
            }
        }
    }

    if context.config.mode == EngineMode::HeadersOnly {
        projected.force_headers_only_verified();
    }

    let retention = projected.enforce_retention(
        selected_tip,
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
    projected.validate_auxiliary_limits(engine, context.config.limits)?;
    Ok(FinalityRetentionOutcome::Settled(Box::new(
        SettledTransition {
            projected,
            selected,
            finality_append,
            finality_lineage,
            finality_ancestry,
            retention,
            effect,
            metadata,
        },
    )))
}

fn headers_only_finality_ancestry(
    graph: &impl HeaderGraphView,
    previous: Frontier,
    selected_tip: Frontier,
) -> Result<crate::FinalityWitnessProof, TransitionFailure> {
    let mut ancestry = Vec::new();
    let mut cursor = selected_tip;
    while cursor.height > previous.height {
        let node = graph
            .view_header_node(cursor.hash)
            .ok_or(crate::GraphError::UnknownHeaderNode(cursor.hash))?;
        if node.height != cursor.height || node.header.hash() != cursor.hash {
            return Err(crate::GraphError::UnknownHeaderNode(cursor.hash).into());
        }
        ancestry.push(FinalityAncestryHeader {
            header: node.header.clone(),
            frontier: cursor,
        });
        cursor = Frontier::new(
            zakura_chain::block::Height(cursor.height.0 - 1),
            node.parent_hash,
        );
    }
    if cursor != previous {
        return Err(crate::GraphError::FinalizedFrontierNotDescendant {
            current: previous.hash,
            candidate: selected_tip.hash,
        }
        .into());
    }
    ancestry.reverse();
    Ok(crate::FinalityWitnessProof::new(ancestry))
}

fn checkpoint_finality_event(event: &TransitionEvent) -> bool {
    matches!(
        event,
        TransitionEvent::VerifiedChainChanged(event)
            if event.cause == crate::VerifiedChangeCause::CheckpointFinalizedGrow
    )
}

/// Return whether the new finalized frontier descends from the projection's tip.
///
/// Call this before the finality advance deletes the headers below the new frontier,
/// because the walk reads them. An empty projection has no lineage to continue, and a
/// new frontier at or below the projection tip leaves retained entries that carry the
/// evidence directly, so both cases answer true without a walk. The walk itself spans
/// the same heights the finality advance already traverses, so it adds no new bound.
fn finality_continues_projection<G: HeaderGraphView>(
    graph: &G,
    old: &[Frontier],
    new_finalized: Frontier,
) -> Result<bool, TransitionFailure> {
    let Some(tip) = old.last() else {
        return Ok(true);
    };
    if new_finalized.height <= tip.height {
        return Ok(true);
    }
    if *tip == graph.view_finalized_frontier() {
        // The finality advance rejects a frontier that does not descend from the current
        // finalized frontier, so a projection ending there needs no separate walk. This
        // is the headers-only verified projection, which collapses to finality.
        return Ok(true);
    }
    Ok(graph.view_header_ancestor(new_finalized.hash, tip.height)? == Some(*tip))
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
