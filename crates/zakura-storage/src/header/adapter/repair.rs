//! [`EnginePort`](super::EnginePort)'s VCT repair: the context a repair
//! reads, and the checks a repair completion passes before it applies, as
//! zakura-state's `HeaderChainStore` makes them in
//! `vct_repair_context_bounded` and `apply`, on the engine instead of the
//! database.
//!
//! Production reads each header's auxiliary rows from disk. This port
//! rebuilds them from the engine's live deliveries ([`durable_rows`]). The
//! two agree in every state a simulation reaches: they differ only after
//! recovery, which demotes a durable outcome to an unauthenticated live
//! delivery, and nothing here recovers from disk.

use zakura_chain::block;
use zakura_header_chain::{
    AuxDelivery, AuxiliaryRequirementEpisode, BodyWorkAuthority, BodyWorkOwner, EngineLimits,
    Frontier, HeaderChainEngine, HeaderLocator, HeaderNode, InsertHeaders, StoreError,
    TargetCompletion, TransitionFailure, UntrustedAuxDeliveryRow, VctRepairContext,
};

/// Why [`check_insert`] refuses an insertion.
#[derive(Debug)]
pub(super) enum Refusal {
    /// It no longer matches the engine: production's `ApplyResult::Stale`.
    Stale,
    /// The engine can't admit its auxiliary input.
    Failure(TransitionFailure),
    /// The engine is incoherent.
    Store(StoreError),
}

impl From<StoreError> for Refusal {
    /// An incoherent engine.
    fn from(error: StoreError) -> Self {
        Refusal::Store(error)
    }
}

/// The context of a repair at `height` for `owner`, or `None` when the
/// owner or height is no longer current, as `vct_repair_context_bounded`
/// resolves it. `ceiling` is the highest checkpoint's height, which no
/// repair passes.
pub(super) fn context(
    engine: &HeaderChainEngine,
    limits: EngineLimits,
    owner: BodyWorkOwner,
    height: block::Height,
    ceiling: block::Height,
) -> Result<Option<VctRepairContext>, StoreError> {
    let snapshot = engine.snapshot();
    if owner.authority != BodyWorkAuthority::for_snapshot(&snapshot)
        || height <= snapshot.frontiers.finalized.height
        || height > snapshot.frontiers.header_best.height
        || height > ceiling
    {
        return Ok(None);
    }
    let selected = engine.selected_projection();
    let (index, target) = selected_node(engine, height)?.ok_or(StoreError::Incoherent(
        "VCT repair height is absent from the selected projection",
    ))?;
    let parent_height = block::Height(height.0.checked_sub(1).ok_or(StoreError::Incoherent(
        "non-finalized VCT repair header has no predecessor height",
    ))?);
    if selected_node(engine, parent_height)?.map(|(_, node)| node.hash) != Some(target.parent_hash)
    {
        return Err(StoreError::Incoherent(
            "selected VCT repair header does not extend its selected predecessor",
        ));
    }
    let parent = Frontier::new(parent_height, target.parent_hash);
    let selected_target = Frontier::new(height, target.hash);
    let boundary_hash = if height < snapshot.frontiers.header_best.height {
        let successor_height = height
            .next()
            .map_err(|_| StoreError::Incoherent("VCT repair successor height overflowed"))?;
        let (_, successor) = selected_node(engine, successor_height)?.ok_or(
            StoreError::Incoherent("VCT repair successor is absent from the selected path"),
        )?;
        if successor.parent_hash != target.hash {
            return Err(StoreError::Incoherent(
                "VCT repair successor does not extend its selected target",
            ));
        }
        Some(successor.hash)
    } else {
        None
    };
    let rows = durable_rows(engine, target.hash);
    let repair_capacity = engine.auxiliary_repair_capacity(target.hash, limits);
    let admission_capacity_available = repair_capacity > 0 && !snapshot.alarms.resource_stalled;
    let context = VctRepairContext::from_durable_rows(
        selected_target,
        HeaderLocator::for_continuation(parent),
        snapshot.state_version,
        boundary_hash,
        admission_capacity_available,
        &rows,
    )?;
    if let Some(ambiguous) = ambiguous_context(
        engine,
        limits,
        &context,
        &rows,
        boundary_hash.filter(|_| height < ceiling),
    )? {
        return Ok(Some(ambiguous));
    }
    if VctRepairContext::rows_constrain_repair(&rows) || !admission_capacity_available {
        return Ok(Some(context));
    }
    let range_limit = repair_capacity.min(limits.max_headers_per_transition.get());
    if range_limit <= 1 {
        return Ok(Some(context));
    }
    let mut suffix: Vec<Frontier> = Vec::new();
    for candidate in selected
        .iter()
        .copied()
        .skip(index.saturating_add(1))
        .take(range_limit.saturating_sub(1))
    {
        if candidate.height > ceiling {
            break;
        }
        let node = engine
            .graph()
            .header_node(candidate.hash)
            .ok_or(StoreError::Incoherent(
                "selected VCT repair range references a missing node",
            ))?;
        if node.height != candidate.height {
            return Err(StoreError::Incoherent(
                "selected VCT repair range disagrees with its node",
            ));
        }
        let previous = suffix.last().map_or(selected_target.hash, |last| last.hash);
        if node.parent_hash != previous {
            return Err(StoreError::Incoherent(
                "selected VCT repair range is not parent-contiguous",
            ));
        }
        if VctRepairContext::rows_constrain_repair(&durable_rows(engine, candidate.hash))
            || !bucket_admits(engine, candidate.hash, limits)
        {
            break;
        }
        suffix.push(candidate);
    }
    let Some(request_target) = suffix.last().copied() else {
        return Ok(Some(context));
    };
    let terminal_boundary_hash = if request_target.height < snapshot.frontiers.header_best.height {
        selected
            .get(index.saturating_add(suffix.len()).saturating_add(1))
            .map(|successor| successor.hash)
    } else {
        None
    };
    Ok(Some(context.extend_empty_selected_range(
        &suffix,
        terminal_boundary_hash,
    )?))
}

/// Checks `insert` against the engine before it plans, as `apply` does: a
/// repair completion must re-derive to its own episode and bring a new
/// payload, and no auxiliary input may be one its header's rows exclude.
pub(super) fn check_insert(
    engine: &HeaderChainEngine,
    limits: EngineLimits,
    insert: &InsertHeaders,
) -> Result<(), Refusal> {
    let mut ambiguous_repair = false;
    if let TargetCompletion::SelectedAuxiliaryRepair {
        common_ancestor,
        selected_target,
        episode,
    } = insert.completion
    {
        ambiguous_repair = check_repair(
            engine,
            limits,
            insert,
            common_ancestor,
            selected_target,
            episode,
        )?;
    }
    check_excluded_inputs(engine, insert, ambiguous_repair)
}

/// The repair half of [`check_insert`]: re-derives the repair's context
/// over the headers it brings and refuses it unless that context is the
/// completion's own `episode` and some payload is new. Returns whether the
/// repair is an ambiguous boundary repair, whose retained payloads the
/// exclusion check skips.
fn check_repair(
    engine: &HeaderChainEngine,
    limits: EngineLimits,
    insert: &InsertHeaders,
    common_ancestor: Frontier,
    selected_target: Frontier,
    episode: AuxiliaryRequirementEpisode,
) -> Result<bool, Refusal> {
    let snapshot = engine.snapshot();
    let headers = insert.batch.headers();
    let Some(first_header) = headers.first() else {
        return Err(Refusal::Stale);
    };
    let range: Vec<_> = headers
        .iter()
        .map(|header| Frontier::new(header.height, header.hash))
        .collect();
    if range.last().copied() != Some(selected_target)
        || range.len() != insert.aux.len()
        || insert
            .aux
            .iter()
            .any(|delivery| delivery.tree_aux.is_none())
    {
        return Err(Refusal::Stale);
    }
    let selected = engine.selected_projection();
    let Some(index) = selected
        .binary_search_by_key(&first_header.height, |frontier| frontier.height)
        .ok()
        .filter(|index| {
            index
                .checked_sub(1)
                .and_then(|parent| selected.get(parent))
                .copied()
                == Some(common_ancestor)
                && selected.get(*index..index.saturating_add(range.len())) == Some(range.as_slice())
        })
    else {
        return Err(Refusal::Stale);
    };
    let terminal_boundary_hash = selected
        .get(index.saturating_add(range.len()))
        .map(|successor| {
            let expected = selected_target
                .height
                .next()
                .map_err(|_| StoreError::Incoherent("VCT repair successor height overflowed"))?;
            let node = engine
                .graph()
                .header_node(successor.hash)
                .ok_or(StoreError::Incoherent(
                    "selected VCT repair successor is absent from the graph",
                ))?;
            if successor.height != expected
                || node.height != successor.height
                || node.parent_hash != selected_target.hash
            {
                return Err(StoreError::Incoherent(
                    "selected VCT repair successor is not contiguous",
                ));
            }
            Ok(successor.hash)
        })
        .transpose()?;
    let rows: Vec<_> = range
        .iter()
        .map(|target| durable_rows(engine, target.hash))
        .collect();
    let first_target = range[0];
    let first_boundary_hash = range
        .get(1)
        .map(|successor| successor.hash)
        .or(terminal_boundary_hash);
    let mut current = VctRepairContext::from_durable_rows(
        first_target,
        HeaderLocator::for_continuation(common_ancestor),
        snapshot.state_version,
        first_boundary_hash,
        !snapshot.alarms.resource_stalled,
        &rows[0],
    )?;
    let mut ambiguous = false;
    if let [_, successor] = range.as_slice() {
        if let Some(context) = current.clone().extend_ambiguous_boundary(
            *successor,
            terminal_boundary_hash,
            current.admission_capacity_available,
            &rows[0],
            &rows[1],
        )? {
            current = context;
            ambiguous = true;
        }
    }
    // An ambiguous repair may add a new payload at both headers. An empty
    // range shares the aggregate slots across its headers.
    let capacity_suffices = match (ambiguous, range.as_slice()) {
        (true, [target, successor]) => {
            ambiguous_capacity_suffices(engine, [target.hash, successor.hash], limits)
        }
        _ => engine.auxiliary_repair_capacity(first_target.hash, limits) >= range.len(),
    };
    if !capacity_suffices {
        return Err(Refusal::Failure(TransitionFailure::AuxiliaryLimitExceeded));
    }
    if !ambiguous {
        if range.len() > 1 {
            if rows
                .iter()
                .any(|rows| VctRepairContext::rows_constrain_repair(rows))
                || range
                    .iter()
                    .any(|target| !bucket_admits(engine, target.hash, limits))
                || snapshot.alarms.resource_stalled
            {
                return Err(Refusal::Stale);
            }
            current = current.extend_empty_selected_range(&range[1..], terminal_boundary_hash)?;
        } else if !VctRepairContext::rows_constrain_repair(&rows[0])
            && current.admission_capacity_available
            && current.episode != episode
        {
            current = current.extend_empty_selected_range(&[], terminal_boundary_hash)?;
        }
    }
    if current.episode != episode {
        return Err(Refusal::Stale);
    }
    // An ambiguous boundary repair needs one new payload. Admission drops
    // the other.
    let mut admits_new_input = false;
    for delivery in &insert.aux {
        let live = engine.aux_deliveries(delivery.header_hash);
        let repeats_retained_payload = live
            .iter()
            .any(|retained| retained.semantic_fingerprint() == delivery.semantic_fingerprint());
        let retained_source = live
            .iter()
            .any(|retained| retained.tree_aux.is_some() && retained.source == delivery.source);
        admits_new_input |= !repeats_retained_payload && !retained_source;
        if (repeats_retained_payload || retained_source) && !ambiguous {
            return Err(Refusal::Stale);
        }
    }
    if !admits_new_input {
        return Err(Refusal::Stale);
    }
    Ok(ambiguous)
}

/// The exclusion half of [`check_insert`], for any insertion: refuses one
/// whose auxiliary input its header's rows exclude. An ambiguous boundary
/// repair's retained payload is skipped, since admission drops it.
fn check_excluded_inputs(
    engine: &HeaderChainEngine,
    insert: &InsertHeaders,
    ambiguous_repair: bool,
) -> Result<(), Refusal> {
    let headers = insert.batch.headers();
    let selected = engine.selected_projection();
    for delivery in &insert.aux {
        let Some(input) = delivery.tree_aux else {
            continue;
        };
        let Some(index) = headers
            .iter()
            .position(|header| header.hash == delivery.header_hash)
        else {
            continue;
        };
        let header = &headers[index];
        let target = Frontier::new(header.height, header.hash);
        let boundary_hash = headers
            .get(index.saturating_add(1))
            .map(|successor| successor.hash)
            .or_else(|| {
                selected
                    .binary_search_by_key(&target.height, |frontier| frontier.height)
                    .ok()
                    .filter(|selected_index| selected[*selected_index] == target)
                    .and_then(|selected_index| selected.get(selected_index.saturating_add(1)))
                    .map(|successor| successor.hash)
            });
        if ambiguous_repair
            && engine
                .aux_deliveries(delivery.header_hash)
                .iter()
                .any(|retained| retained.semantic_fingerprint() == delivery.semantic_fingerprint())
        {
            continue;
        }
        let rows = durable_rows(engine, target.hash);
        if VctRepairContext::durable_rows_exclude(target, boundary_hash, &rows, input)? {
            return Err(Refusal::Stale);
        }
    }
    Ok(())
}

/// Extends a one-header context over its successor when one observation
/// disputed both, as `ambiguous_vct_repair_context` does. `None` when
/// there's no successor or the rows show no such dispute.
fn ambiguous_context(
    engine: &HeaderChainEngine,
    limits: EngineLimits,
    context: &VctRepairContext,
    rows: &[UntrustedAuxDeliveryRow],
    successor_hash: Option<block::Hash>,
) -> Result<Option<VctRepairContext>, StoreError> {
    let Some(successor_hash) = successor_hash else {
        return Ok(None);
    };
    let successor_height = context
        .target
        .height
        .next()
        .map_err(|_| StoreError::Incoherent("VCT repair successor height overflowed"))?;
    let successor = Frontier::new(successor_height, successor_hash);
    if engine.graph().header_node(successor_hash).is_none() {
        return Err(StoreError::Incoherent(
            "VCT repair successor references a missing node",
        ));
    }
    let selected = engine.selected_projection();
    let terminal_boundary_hash = selected
        .binary_search_by_key(&successor_height, |frontier| frontier.height)
        .ok()
        .filter(|index| selected[*index] == successor)
        .ok_or(StoreError::Incoherent(
            "VCT repair successor disagrees with the selected projection",
        ))
        .map(|index| selected.get(index.saturating_add(1)).map(|next| next.hash))?;
    let successor_capacity_available = context.admission_capacity_available
        && ambiguous_capacity_suffices(engine, [context.target.hash, successor_hash], limits);
    context.clone().extend_ambiguous_boundary(
        successor,
        terminal_boundary_hash,
        successor_capacity_available,
        rows,
        &durable_rows(engine, successor_hash),
    )
}

/// Whether an ambiguous boundary repair can keep a new payload at both
/// headers, as zakura-state's `ambiguous_repair_capacity_suffices` judges.
/// A full bucket replaces one of its own unauthenticated rows. Each other
/// header needs an aggregate slot, and the engine reports the same shared
/// slots for every such header, so the slots must cover them together.
fn ambiguous_capacity_suffices(
    engine: &HeaderChainEngine,
    targets: [block::Hash; 2],
    limits: EngineLimits,
) -> bool {
    let mut needed_slots = 0;
    let mut shared_slots = usize::MAX;
    for target in targets {
        let capacity = engine.auxiliary_repair_capacity(target, limits);
        if engine.aux_deliveries(target).len() >= limits.max_aux_deliveries_per_header.get() {
            if capacity == 0 {
                return false;
            }
        } else {
            needed_slots += 1;
            shared_slots = capacity;
        }
    }
    shared_slots >= needed_slots
}

/// Whether a repair can add input to, or replace input in, `hash`'s
/// bucket, as zakura-state's `repair_bucket_admits` judges.
fn bucket_admits(engine: &HeaderChainEngine, hash: block::Hash, limits: EngineLimits) -> bool {
    let deliveries = engine.aux_deliveries(hash);
    deliveries.len() < limits.max_aux_deliveries_per_header.get()
        || deliveries
            .iter()
            .any(|delivery| !delivery.is_authenticated())
}

/// The selected node at `height`, with its index in the selected
/// projection, as zakura-state's `coherent_selected_node` reads it.
fn selected_node(
    engine: &HeaderChainEngine,
    height: block::Height,
) -> Result<Option<(usize, &HeaderNode)>, StoreError> {
    let selected = engine.selected_projection();
    let Ok(index) = selected.binary_search_by_key(&height, |frontier| frontier.height) else {
        return Ok(None);
    };
    let frontier = selected[index];
    let node = engine
        .graph()
        .header_node(frontier.hash)
        .filter(|node| node.height == frontier.height)
        .ok_or(StoreError::Incoherent(
            "selected projection disagrees with its node",
        ))?;
    Ok(Some((index, node)))
}

/// `hash`'s auxiliary rows as zakura-state's disk holds them, in delivery
/// order: each live delivery's base fields, with its outcome as the raw
/// status code, observation digests and boundary production writes.
fn durable_rows(engine: &HeaderChainEngine, hash: block::Hash) -> Vec<UntrustedAuxDeliveryRow> {
    let mut rows: Vec<_> = engine
        .aux_deliveries(hash)
        .iter()
        .map(|delivery| durable_row(*delivery))
        .collect();
    rows.sort_unstable_by_key(|row| row.delivery().delivery_id);
    rows
}

/// `delivery` as zakura-state writes its row.
fn durable_row(delivery: AuxDelivery) -> UntrustedAuxDeliveryRow {
    let base = AuxDelivery::new(
        delivery.delivery_id,
        delivery.header_hash,
        delivery.source,
        delivery.owner,
        delivery.body_size,
        delivery.tree_aux,
    );
    let status = if delivery.is_unauthenticated() {
        0
    } else if delivery.is_authenticated() {
        1
    } else if delivery.is_rejected() {
        2
    } else {
        3
    };
    if status == 0 {
        return UntrustedAuxDeliveryRow::new(base, 0, [None, None], None);
    }
    UntrustedAuxDeliveryRow::new(
        base,
        status,
        delivery
            .observation_ids()
            .map(|observation| observation.map(|observation| observation.digest())),
        delivery.outcome_boundary_hash(),
    )
}
