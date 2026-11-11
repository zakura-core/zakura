//! Shared retained-context and contextual header checks for event handlers.

use chrono::{DateTime, Utc};
use zakura_chain::{block, parameters::Network, work::difficulty::CompactDifficulty};

use super::super::{
    HeaderValidationCheck, HeaderValidationSource, HeaderViolation, InvalidTransitionEvidence,
    TransitionFailure,
};
use crate::graph::HeaderGraphView;
use crate::{
    EligibilityReason, Frontier, HeaderValidationFacts, HeaderValidationState, StoreError,
    TransitionContext,
};

/// One header of difficulty/time context: its threshold, its time, and where it sits.
type ContextEntry = (CompactDifficulty, DateTime<Utc>, Frontier);

/// The number of headers contextual validation reads from `parent` downward, `parent` included.
fn required_context_len(parent: Frontier) -> Result<usize, StoreError> {
    Ok(usize::try_from(parent.height.0)
        .map_err(|_| StoreError::Unavailable("retained parent height does not fit in memory"))?
        .checked_add(1)
        .ok_or(StoreError::Unavailable(
            "retained parent context length overflowed",
        ))?
        .min(crate::MAX_POW_ADJUSTMENT_BLOCK_SPAN))
}

/// Walk the retained graph from `parent` toward finality, newest first.
///
/// Stops after `required` headers, or earlier at the first header the graph does not retain.
fn retained_graph_context<G: HeaderGraphView>(
    graph: &G,
    parent: Frontier,
    required: usize,
) -> Vec<ContextEntry> {
    let mut context = Vec::with_capacity(required);
    let mut hash = parent.hash;
    while context.len() < required {
        let Some(node) = graph.view_header_node(hash) else {
            break;
        };
        context.push((
            node.header.difficulty_threshold,
            node.header.time,
            Frontier::new(node.height, node.hash),
        ));
        hash = node.parent_hash;
    }
    context
}

/// Returns whether the retained graph alone supplies the difficulty/time context for `parent`.
///
/// When it does, [`retained_header_context`] never reads a durable lease for a walk that starts
/// at `parent` or passes through it. A `parent` the graph does not retain returns `false`.
pub(in crate::transition::planner) fn graph_retains_header_context<G: HeaderGraphView>(
    graph: &G,
    parent: block::Hash,
) -> bool {
    let Some(node) = graph.view_header_node(parent) else {
        return false;
    };
    let parent = Frontier::new(node.height, node.hash);
    required_context_len(parent)
        .is_ok_and(|required| retained_graph_context(graph, parent, required).len() == required)
}

/// Reconstruct difficulty/time context for a retained parent, splicing durable leases when needed.
pub(in crate::transition::planner) fn retained_header_context<G: HeaderGraphView>(
    graph: &G,
    parent: Frontier,
    facts: Option<&crate::HeaderValidationFacts>,
    transition: &TransitionContext<'_>,
) -> Result<Vec<(CompactDifficulty, DateTime<Utc>)>, TransitionFailure> {
    let required = required_context_len(parent)?;
    let mut context = retained_graph_context(graph, parent, required);
    if context.len() < required {
        splice_durable_lease(&mut context, parent, required, facts, transition)?;
    }
    Ok(context
        .into_iter()
        .map(|(difficulty, time, _)| (difficulty, time))
        .collect())
}

/// Complete a context the retained graph cut short, from one authorized durable lease.
///
/// The lease must start at `parent` or at a header already in `context`, and agree with every
/// header the two share.
fn splice_durable_lease(
    context: &mut Vec<ContextEntry>,
    parent: Frontier,
    required: usize,
    facts: Option<&crate::HeaderValidationFacts>,
    transition: &TransitionContext<'_>,
) -> Result<(), TransitionFailure> {
    let Some(facts) = facts else {
        return Err(TransitionFailure::MissingDurableFacts(
            "retained predecessor context is incomplete",
        ));
    };
    let authorized_lease = facts.validation_leases.iter().find_map(|lease| {
        if !lease.is_coherent(
            transition.config.network(),
            transition.config.trust_anchor_digest(),
        ) || !transition
            .full_state_authority
            .is_some_and(|authority| authority.authorizes_validation_lease(lease))
        {
            return None;
        }
        let overlap = context
            .iter()
            .position(|(_, _, frontier)| *frontier == lease.parent())
            .or_else(|| {
                context
                    .is_empty()
                    .then_some(0)
                    .filter(|_| lease.parent() == parent)
            })?;
        let graph_overlap = &context[overlap..];
        let lease_overlap = lease.predecessors().get(..graph_overlap.len())?;
        graph_overlap
            .iter()
            .zip(lease_overlap)
            .all(|((_, _, frontier), fact)| *frontier == fact.frontier)
            .then_some((lease, overlap))
    });
    let Some((lease, overlap)) = authorized_lease else {
        return Err(TransitionFailure::MissingDurableFacts(
            "durable predecessor context is incoherent",
        ));
    };
    context.truncate(overlap);
    context.extend(
        lease
            .predecessors()
            .iter()
            .take(required.saturating_sub(context.len()))
            .map(|fact| {
                (
                    fact.header.difficulty_threshold,
                    fact.header.time,
                    fact.frontier,
                )
            }),
    );
    if context.len() != required {
        return Err(TransitionFailure::MissingDurableFacts(
            "durable predecessor context is incomplete",
        ));
    }
    Ok(())
}

/// Apply branch-local difficulty and median-time rules for one parent-linked header.
pub(in crate::transition::planner) fn check_contextual_difficulty_and_time(
    header_time: DateTime<Utc>,
    difficulty_threshold: CompactDifficulty,
    parent_height: block::Height,
    network: &Network,
    predecessor_context: impl IntoIterator<Item = (CompactDifficulty, DateTime<Utc>)>,
    source: HeaderValidationSource,
) -> Result<(), TransitionFailure> {
    let adjustment = crate::AdjustedDifficulty::new_from_header_time(
        header_time,
        parent_height,
        network,
        predecessor_context,
    )
    .map_err(|_| {
        InvalidTransitionEvidence::Header(HeaderViolation::Validation {
            source,
            check: HeaderValidationCheck::IncompleteDifficultyContext,
        })
    })?;
    crate::validate_contextual_difficulty_and_time(difficulty_threshold, adjustment).map_err(
        |_| {
            InvalidTransitionEvidence::Header(HeaderViolation::Validation {
                source,
                check: HeaderValidationCheck::ContextualValidation,
            })
        },
    )?;
    Ok(())
}

/// Validate a full-state header against observable and retained contextual rules.
pub(super) fn validate_full_state_header<G: HeaderGraphView>(
    graph: &G,
    parent: Frontier,
    header: &crate::VerifiedHeaderRef,
    facts: Option<&HeaderValidationFacts>,
    context: &TransitionContext<'_>,
) -> Result<zakura_chain::work::difficulty::Work, TransitionFailure> {
    let rules = crate::HeaderRules::from_engine_config(context.config).map_err(|_| {
        InvalidTransitionEvidence::full_state_header(HeaderValidationCheck::PolicyIncoherent)
    })?;
    let headers = [header.header.clone()];
    let prepared = crate::prepare_headers(
        crate::HeaderBatchInput::new(&headers),
        parent,
        &rules,
        context.clock,
    )
    .map_err(|_| {
        InvalidTransitionEvidence::full_state_header(HeaderValidationCheck::ObservableValidation)
    })?;
    let prepared = prepared
        .headers()
        .first()
        .ok_or(TransitionFailure::InvalidEvidence(
            InvalidTransitionEvidence::full_state_header(HeaderValidationCheck::NoResult),
        ))?;
    if prepared.hash != header.hash
        || prepared.height != header.height
        || prepared.validation != HeaderValidationState::Valid
    {
        return Err(InvalidTransitionEvidence::full_state_header(
            HeaderValidationCheck::IdentityOrLocalTime,
        )
        .into());
    }
    let contextual = retained_header_context(graph, parent, facts, context)?;
    check_contextual_difficulty_and_time(
        header.header.time,
        header.header.difficulty_threshold,
        parent.height,
        context.config.network(),
        contextual,
        HeaderValidationSource::FullState,
    )?;
    Ok(prepared.block_work)
}

/// Configured settled-pin and checkpoint conflict reasons for an admitted header.
pub(in crate::transition::planner) fn anchor_reasons(
    context: &TransitionContext<'_>,
    height: zakura_chain::block::Height,
    hash: zakura_chain::block::Hash,
) -> Vec<EligibilityReason> {
    let mut reasons = Vec::new();
    if let Some(pin) = context
        .config
        .settled_manifest()
        .pin_for_network(context.config.network())
    {
        if pin.activation.height == height && pin.activation.hash != hash {
            reasons.push(EligibilityReason::SettledUpgradeConflict {
                height,
                expected: pin.activation.hash,
            });
        }
    }
    if let Some(expected) = context.config.local_checkpoints().hash(height) {
        if expected != hash {
            reasons.push(EligibilityReason::CheckpointConflict { height, expected });
        }
    }
    reasons
}
