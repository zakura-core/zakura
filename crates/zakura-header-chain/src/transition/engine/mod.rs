//! Stateful ownership boundary for coherent header-chain planning.

mod input;
mod install;
mod recovery;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use thiserror::Error;
use zakura_chain::block;

use crate::{
    AuxDelivery, EngineMetadata, EngineSnapshot, EngineTransition, GraphError, MemHeaderStore,
    TransitionContext, TransitionFailure, UntrustedAuxDeliveryRow,
};

use super::planner::derive_transition_plan;
use install::{merge_auxiliary_delivery_changes, merge_projection_delta, verify_projection};
pub(crate) use recovery::validate_recovered_auxiliary_rows;

pub use input::{HeaderInsertionFacts, HeaderValidationFacts, TransitionInput};

/// Incoherent state supplied while hydrating an audited engine.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum EngineHydrationError {
    /// The supplied graph, projections, metadata, or auxiliary rows disagree.
    #[error("incoherent audited header-chain engine state: {0}")]
    Incoherent(&'static str),
    /// The supplied graph is internally incoherent.
    #[error(transparent)]
    Graph(#[from] GraphError),
}

/// The engine could not install a verified transition on its original in-memory source.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum CommittedTransitionError {
    /// The transition does not belong to this exact engine source revision.
    #[error("committed header transition no longer matches its source engine revision")]
    StaleSource,
    /// The process-local source revision cannot advance.
    #[error("header-chain engine source revision is exhausted")]
    RevisionExhausted,
    /// The graph rejected the verified delta.
    #[error(transparent)]
    Graph(#[from] GraphError),
}

/// Owns coherent in-memory header-chain state and decides its valid transitions.
///
/// The engine determines whether evidence is admissible, stale, or replayed;
/// maintains header eligibility; selects the best fork; applies finality and
/// retention policy; and derives the exact graph, projection, alarm, and
/// metadata write set for each transition.
///
/// It performs no durable writes or publication; the runtime commits, installs,
/// and publishes each verified transition.
#[derive(Debug)]
pub struct HeaderChainEngine {
    /// Process-local capability that distinguishes equal public snapshots.
    instance_capability: Arc<()>,
    /// Process-local revision that consumes each installed transition once.
    source_revision: u64,
    /// Complete retained header graph.
    graph: MemHeaderStore,
    metadata: EngineMetadata,
    /// Selected header path from finality to the selected tip.
    selected_projection: Vec<crate::Frontier>,
    /// Body-verified path from finality to the verified tip.
    verified_projection: Vec<crate::Frontier>,
    /// Auxiliary deliveries keyed by retained header hash.
    aux_deliveries: HashMap<block::Hash, Vec<AuxDelivery>>,
    /// Retained header hash keyed by globally unique delivery identity.
    aux_delivery_index: HashMap<crate::EvidenceId, block::Hash>,
}

/// Private identity of one exact in-memory transition source.
#[derive(Clone, Debug)]
pub(crate) struct EngineTransitionSource {
    instance_capability: Arc<()>,
    revision: u64,
}

impl EngineTransitionSource {
    fn matches(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.instance_capability, &other.instance_capability)
            && self.revision == other.revision
    }
}

impl HeaderChainEngine {
    /// The engine constructs coherent state from a successful durable recovery audit.
    ///
    /// Rechecks the coherence required for safe transition planning:
    /// - graph finality agrees with metadata;
    /// - selected and verified projections are contiguous finalized-rooted paths;
    /// - the selected tip and score agree with graph fork choice;
    /// - verified-path body state agrees with the engine mode; and
    /// - auxiliary deliveries agree with the graph's delivery index.
    ///
    /// Auxiliary deliveries are normalized into delivery-ID order. Returns an
    /// error if any supplied view disagrees with the graph or metadata.
    ///
    /// This constructor does not replace the exhaustive durable recovery audit.
    pub fn from_audited_state(
        graph: MemHeaderStore,
        metadata: EngineMetadata,
        selected: Vec<crate::Frontier>,
        verified: Vec<crate::Frontier>,
        deliveries: impl IntoIterator<Item = AuxDelivery>,
    ) -> Result<Self, EngineHydrationError> {
        let deliveries: Vec<_> = deliveries.into_iter().collect();
        if deliveries
            .iter()
            .any(|delivery| !delivery.is_unauthenticated())
        {
            return Err(EngineHydrationError::Incoherent(
                "authoritative auxiliary outcomes require recovery validation",
            ));
        }
        Self::from_validated_state(graph, metadata, selected, verified, deliveries)
    }

    /// Validate untrusted durable auxiliary rows and hydrate one recovered engine.
    ///
    /// The decoder cannot construct an authoritative auxiliary outcome. This recovery entry point
    /// checks row structure, global delivery identity, retained-header ownership, replay identity,
    /// and derived-boundary topology before it promotes any outcome.
    pub fn from_untrusted_durable_state(
        graph: MemHeaderStore,
        metadata: EngineMetadata,
        selected: Vec<crate::Frontier>,
        verified: Vec<crate::Frontier>,
        deliveries: impl IntoIterator<Item = UntrustedAuxDeliveryRow>,
    ) -> Result<Self, EngineHydrationError> {
        let deliveries = validate_recovered_auxiliary_rows(&graph, deliveries)?;
        Self::from_validated_state(graph, metadata, selected, verified, deliveries)
    }

    fn from_validated_state(
        graph: MemHeaderStore,
        metadata: EngineMetadata,
        selected: Vec<crate::Frontier>,
        verified: Vec<crate::Frontier>,
        deliveries: Vec<AuxDelivery>,
    ) -> Result<Self, EngineHydrationError> {
        if graph.finalized_frontier() != metadata.frontiers.finalized {
            return Err(EngineHydrationError::Incoherent(
                "graph finality disagrees with metadata",
            ));
        }
        if graph
            .header_nodes()
            .any(|node| node.work_coordinate().origin_hash() != metadata.work_origin.hash)
        {
            return Err(EngineHydrationError::Incoherent(
                "graph work origin disagrees with metadata",
            ));
        }
        verify_projection(&graph, &selected, metadata.frontiers.header_best, false)?;
        verify_projection(
            &graph,
            &verified,
            metadata.frontiers.verified_best,
            metadata.mode == crate::EngineMode::Integrated,
        )?;
        if metadata.mode == crate::EngineMode::HeadersOnly
            && verified.as_slice() != [metadata.frontiers.finalized]
        {
            return Err(EngineHydrationError::Incoherent(
                "headers-only verified projection extends past finality",
            ));
        }
        let (selected_frontier, score) = graph.select_best_header_chain()?;
        if selected_frontier != metadata.frontiers.header_best
            || score != metadata.header_best_score
        {
            return Err(EngineHydrationError::Incoherent(
                "selected frontier or score disagrees with graph",
            ));
        }

        let mut aux_deliveries: HashMap<_, Vec<_>> = HashMap::new();
        let mut delivery_ids = HashSet::new();
        let mut aux_delivery_index = HashMap::new();
        for delivery in deliveries {
            let node =
                graph
                    .header_node(delivery.header_hash)
                    .ok_or(EngineHydrationError::Incoherent(
                        "auxiliary delivery has no retained header",
                    ))?;
            if !delivery_ids.insert(delivery.delivery_id)
                || !node.aux_delivery_ids.contains(&delivery.delivery_id)
            {
                return Err(EngineHydrationError::Incoherent(
                    "auxiliary delivery index disagrees with graph",
                ));
            }
            aux_deliveries
                .entry(delivery.header_hash)
                .or_default()
                .push(delivery);
            aux_delivery_index.insert(delivery.delivery_id, delivery.header_hash);
        }
        for node in graph.header_nodes() {
            if node.aux_delivery_ids.iter().any(|delivery_id| {
                !aux_deliveries
                    .get(&node.hash)
                    .is_some_and(|rows| rows.iter().any(|row| row.delivery_id == *delivery_id))
            }) {
                return Err(EngineHydrationError::Incoherent(
                    "graph auxiliary index has no delivery",
                ));
            }
        }
        for rows in aux_deliveries.values_mut() {
            rows.sort_unstable_by_key(|delivery| delivery.delivery_id);
        }

        Ok(Self {
            instance_capability: Arc::new(()),
            source_revision: 0,
            graph,
            metadata,
            selected_projection: selected,
            verified_projection: verified,
            aux_deliveries,
            aux_delivery_index,
        })
    }

    /// Plan one verified transition without mutating this engine.
    ///
    /// Callers own everything after this returns: persist
    /// [`EngineTransition::change_set`], then
    /// [`Self::install_committed_transition`], then publish
    /// [`EngineTransition::snapshot_after_commit`]. This method never writes durable state or
    /// publishes watches.
    ///
    /// # Returns
    ///
    /// - [`Ok`] — an invariant-verified [`EngineTransition`]. Inspect
    ///   [`EngineTransition::is_no_change`] and [`EngineTransition::effect`]
    ///   before assuming a mutation: valid evidence may produce no durable
    ///   change, or only a resource-stall alarm.
    /// - [`Err`] — [`TransitionFailure`]: reject with zero durable effects
    ///   (stale version/owner, invalid evidence, mode/authority refusal, etc.).
    ///   Do not confuse this with a verified no-change plan.
    ///
    /// # Install contract
    ///
    /// Install succeeds only if this engine's snapshot is still
    /// [`EngineTransition::snapshot_before_commit`]. Concurrent commits make the plan stale;
    /// re-plan against the current engine rather than forcing install.
    pub fn plan_transition(
        &self,
        input: TransitionInput,
        context: &TransitionContext<'_>,
    ) -> Result<EngineTransition, TransitionFailure> {
        derive_transition_plan(self, input, context)
    }

    /// Install a verified transition after its durable batch has committed.
    ///
    /// Callers must already have persisted [`EngineTransition::change_set`].
    /// This advances the in-memory engine only; it does not write durable state
    /// or publish watches—publish [`EngineTransition::snapshot_after_commit`] after success.
    ///
    /// # Returns
    ///
    /// - [`Ok`] — graph, projections, metadata, and auxiliary deliveries match
    ///   the committed write set and this engine is ready to publish.
    /// - [`Err`] — [`CommittedTransitionError::StaleSource`] if another
    ///   transition changed this engine after planning, or
    ///   [`CommittedTransitionError::Graph`] if the verified delta cannot apply.
    ///   On either error the engine is unchanged, but durable state may already
    ///   be ahead of memory; fail closed and recover from durable state.
    pub fn install_committed_transition(
        &mut self,
        transition: EngineTransition,
    ) -> Result<(), CommittedTransitionError> {
        if !self
            .transition_source()
            .matches(transition.transition_source())
            || self.snapshot() != *transition.snapshot_before_commit()
        {
            return Err(CommittedTransitionError::StaleSource);
        }
        let next_revision = self
            .source_revision
            .checked_add(1)
            .ok_or(CommittedTransitionError::RevisionExhausted)?;
        self.install_verified_plan(&transition)?;
        self.source_revision = next_revision;
        Ok(())
    }

    /// Return the private identity of this exact in-memory source revision.
    pub(crate) fn transition_source(&self) -> EngineTransitionSource {
        EngineTransitionSource {
            instance_capability: self.instance_capability.clone(),
            revision: self.source_revision,
        }
    }

    #[cfg(test)]
    pub(crate) fn exhaust_source_revision_for_test(&mut self) {
        self.source_revision = u64::MAX;
    }

    /// Install a verified plan's write set into this engine's in-memory state.
    ///
    /// Caller must have already checked the snapshot before commit; graph apply is the
    /// only fallible step and leaves the engine unchanged on error.
    fn install_verified_plan(&mut self, plan: &EngineTransition) -> Result<(), GraphError> {
        self.graph.apply_delta(plan.graph_delta())?;
        self.metadata = plan.change_set().metadata.clone();
        merge_projection_delta(
            &mut self.selected_projection,
            &plan.change_set().selected_projection,
        );
        merge_projection_delta(
            &mut self.verified_projection,
            &plan.change_set().verified_projection,
        );
        merge_auxiliary_delivery_changes(
            &mut self.aux_deliveries,
            &mut self.aux_delivery_index,
            &plan.change_set().aux_changes,
        );
        Ok(())
    }

    /// Return the atomic externally meaningful snapshot.
    pub fn snapshot(&self) -> EngineSnapshot {
        self.metadata.snapshot()
    }

    /// Return complete engine metadata.
    pub const fn metadata(&self) -> &EngineMetadata {
        &self.metadata
    }

    /// Return the engine-owned retained graph.
    pub const fn graph(&self) -> &MemHeaderStore {
        &self.graph
    }

    /// This method returns the frontier for each selected chain prefix from finality to the tip.
    pub fn selected_projection(&self) -> &[crate::Frontier] {
        &self.selected_projection
    }

    /// This method returns the frontier for each verified chain prefix from finality to the tip.
    pub fn verified_projection(&self) -> &[crate::Frontier] {
        &self.verified_projection
    }

    /// Return bounded auxiliary deliveries for one retained header.
    pub fn aux_deliveries(&self, hash: block::Hash) -> &[AuxDelivery] {
        self.aux_deliveries
            .get(&hash)
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    /// Return the total number of retained auxiliary deliveries.
    pub(crate) fn aux_delivery_count(&self) -> usize {
        self.aux_deliveries.values().map(Vec::len).sum()
    }

    /// Return the retained auxiliary delivery with the exact global identity.
    pub(crate) fn aux_delivery(&self, delivery_id: crate::EvidenceId) -> Option<&AuxDelivery> {
        let header_hash = self.aux_delivery_index.get(&delivery_id)?;
        self.aux_deliveries(*header_hash)
            .iter()
            .find(|delivery| delivery.delivery_id == delivery_id)
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU64, sync::Arc};

    use zakura_chain::{
        block::{self, genesis::regtest_genesis_block},
        parameters::NetworkKind,
    };

    use super::*;
    use crate::{
        AlarmSet, BodySizeHint, BodyValidationState, BranchId, EligibilityReason, EngineMode,
        EvidenceId, FinalityEpoch, Frontier, FrontierSet, HeaderChainDiskVersion, HeaderGeneration,
        HeaderValidationState, HeaderWorkAuthority, InsertResult, SourceId, StateVersion,
        VerifiedGeneration,
    };

    #[derive(Clone)]
    struct AuditedView {
        graph: MemHeaderStore,
        metadata: EngineMetadata,
        selected: Vec<Frontier>,
        verified: Vec<Frontier>,
        deliveries: Vec<AuxDelivery>,
    }

    type HydrationCase = (&'static str, fn(&mut AuditedView), EngineHydrationError);

    fn audited_view() -> AuditedView {
        let genesis = regtest_genesis_block();
        let anchor = Frontier::new(block::Height(0), genesis.hash());
        let work = genesis
            .header
            .difficulty_threshold
            .to_work()
            .expect("the regtest target has valid work");
        let mut graph = MemHeaderStore::new(anchor, genesis.header.clone(), work, work.as_u256())
            .expect("the anchor is coherent");
        let mut header = *genesis.header;
        header.previous_block_hash = anchor.hash;
        header.nonce = [1; 32].into();
        let child = match graph
            .insert(
                Arc::new(header),
                HeaderValidationState::Valid,
                [],
                BodyValidationState::Unknown,
            )
            .expect("the child inserts")
        {
            InsertResult::Inserted(frontier) | InsertResult::AlreadyPresent(frontier) => frontier,
        };
        let score = graph
            .header_chain_score(child.hash)
            .expect("the child is retained");
        AuditedView {
            graph,
            metadata: EngineMetadata {
                disk_format: HeaderChainDiskVersion::CURRENT,
                mode: EngineMode::Integrated,
                network_id: NetworkKind::Testnet,
                network_policy_digest: [2; 32],
                anchor_manifest_digest: [1; 32],
                work_origin: anchor,
                state_version: StateVersion::new(0),
                header_generation: HeaderGeneration::new(0),
                verified_generation: VerifiedGeneration::new(0),
                finality_epoch: FinalityEpoch::new(0),
                headers_only_migration_epoch: None,
                frontiers: FrontierSet {
                    finalized: anchor,
                    header_best: child,
                    verified_best: anchor,
                },
                header_best_score: score,
                oldest_retained_height: anchor.height,
                alarms: AlarmSet::default(),
                last_transition: None,
            },
            selected: vec![anchor, child],
            verified: vec![anchor],
            deliveries: Vec::new(),
        }
    }

    fn delivery(id: u8, header_hash: block::Hash) -> AuxDelivery {
        let owner = HeaderWorkAuthority {
            header_generation: HeaderGeneration::new(0),
            branch: BranchId::new(header_hash, header_hash),
        }
        .bind(
            1,
            NonZeroU64::new(1).expect("the fixture request ID is nonzero"),
        );
        AuxDelivery::new(
            EvidenceId::from_digest([id; 32]),
            header_hash,
            SourceId::from_digest([id.saturating_add(1); 32]),
            owner.into(),
            BodySizeHint::Unknown,
            None,
        )
    }

    fn finality_disagrees(view: &mut AuditedView) {
        view.metadata.frontiers.finalized =
            Frontier::new(block::Height(0), block::Hash([0xf0; 32]));
    }

    fn projection_endpoints_disagree(view: &mut AuditedView) {
        view.selected.clear();
    }

    fn projection_height_disagrees(view: &mut AuditedView) {
        let child = *view
            .selected
            .last()
            .expect("the fixture selected projection is nonempty");
        let wrong = Frontier::new(block::Height(2), child.hash);
        *view
            .selected
            .last_mut()
            .expect("the fixture selected projection is nonempty") = wrong;
        view.metadata.frontiers.header_best = wrong;
    }

    fn projection_is_disconnected(view: &mut AuditedView) {
        let anchor = view.metadata.frontiers.finalized;
        view.selected.insert(1, anchor);
    }

    fn verified_projection_has_unverified_body(view: &mut AuditedView) {
        let child = view.metadata.frontiers.header_best;
        view.verified.push(child);
        view.metadata.frontiers.verified_best = child;
    }

    fn verified_projection_has_ineligible_header(view: &mut AuditedView) {
        let child = view.metadata.frontiers.header_best;
        view.graph
            .set_body_validation_state(
                child.hash,
                BodyValidationState::Verified {
                    evidence: EvidenceId::from_digest([0xe1; 32]),
                },
            )
            .expect("the child accepts verified body state");
        view.graph
            .add_eligibility_reason(
                child.hash,
                EligibilityReason::operator_invalid(
                    child.hash,
                    crate::OperatorInvalidationId::new([0xe2; 16]),
                    EvidenceId::from_digest([0xe3; 32]),
                ),
            )
            .expect("the child accepts an operator invalidation");
        view.verified.push(child);
        view.metadata.frontiers.verified_best = child;
    }

    fn headers_only_verified_projection_extends(view: &mut AuditedView) {
        view.metadata.mode = EngineMode::HeadersOnly;
        let child = view.metadata.frontiers.header_best;
        view.verified.push(child);
        view.metadata.frontiers.verified_best = child;
    }

    fn selected_frontier_disagrees(view: &mut AuditedView) {
        let anchor = view.metadata.frontiers.finalized;
        view.selected = vec![anchor];
        view.metadata.frontiers.header_best = anchor;
        view.metadata.header_best_score = view
            .graph
            .header_chain_score(anchor.hash)
            .expect("the anchor is retained");
    }

    fn selected_score_disagrees(view: &mut AuditedView) {
        view.metadata.header_best_score.tip_hash = block::Hash([0xf1; 32]);
    }

    fn delivery_has_no_header(view: &mut AuditedView) {
        view.deliveries.push(delivery(1, block::Hash([0xf2; 32])));
    }

    fn delivery_id_is_duplicated(view: &mut AuditedView) {
        let child = view.metadata.frontiers.header_best;
        let row = delivery(2, child.hash);
        view.graph
            .record_auxiliary_evidence_delivery(child.hash, row.delivery_id)
            .expect("the child is retained");
        view.deliveries.extend([row, row]);
    }

    fn delivery_is_absent_from_graph_index(view: &mut AuditedView) {
        let child = view.metadata.frontiers.header_best;
        view.deliveries.push(delivery(3, child.hash));
    }

    fn graph_index_has_no_delivery(view: &mut AuditedView) {
        let child = view.metadata.frontiers.header_best;
        view.graph
            .record_auxiliary_evidence_delivery(child.hash, EvidenceId::from_digest([4; 32]))
            .expect("the child is retained");
    }

    #[test]
    fn from_audited_state_rejects_each_incoherent_view() {
        let cases: &[HydrationCase] = &[
            (
                "finality",
                finality_disagrees,
                EngineHydrationError::Incoherent("graph finality disagrees with metadata"),
            ),
            (
                "projection endpoints",
                projection_endpoints_disagree,
                EngineHydrationError::Incoherent("projection endpoints disagree with metadata"),
            ),
            (
                "projection height",
                projection_height_disagrees,
                EngineHydrationError::Incoherent("projection frontier height disagrees with graph"),
            ),
            (
                "projection connectivity",
                projection_is_disconnected,
                EngineHydrationError::Incoherent("projection is not a contiguous graph path"),
            ),
            (
                "verified body",
                verified_projection_has_unverified_body,
                EngineHydrationError::Incoherent(
                    "verified projection contains an ineligible or unverified header",
                ),
            ),
            (
                "verified eligibility",
                verified_projection_has_ineligible_header,
                EngineHydrationError::Incoherent(
                    "verified projection contains an ineligible or unverified header",
                ),
            ),
            (
                "headers-only verified projection",
                headers_only_verified_projection_extends,
                EngineHydrationError::Incoherent(
                    "headers-only verified projection extends past finality",
                ),
            ),
            (
                "selected frontier",
                selected_frontier_disagrees,
                EngineHydrationError::Incoherent("selected frontier or score disagrees with graph"),
            ),
            (
                "selected score",
                selected_score_disagrees,
                EngineHydrationError::Incoherent("selected frontier or score disagrees with graph"),
            ),
            (
                "delivery header",
                delivery_has_no_header,
                EngineHydrationError::Incoherent("auxiliary delivery has no retained header"),
            ),
            (
                "duplicate delivery ID",
                delivery_id_is_duplicated,
                EngineHydrationError::Incoherent("auxiliary delivery index disagrees with graph"),
            ),
            (
                "delivery missing from node index",
                delivery_is_absent_from_graph_index,
                EngineHydrationError::Incoherent("auxiliary delivery index disagrees with graph"),
            ),
            (
                "node index missing delivery",
                graph_index_has_no_delivery,
                EngineHydrationError::Incoherent("graph auxiliary index has no delivery"),
            ),
        ];

        for (name, corrupt, expected) in cases {
            let mut view = audited_view();
            corrupt(&mut view);
            let result = HeaderChainEngine::from_audited_state(
                view.graph,
                view.metadata,
                view.selected,
                view.verified,
                view.deliveries,
            );
            assert_eq!(
                result.expect_err("the corrupted audited view must be rejected"),
                expected.clone(),
                "{name}",
            );
        }
    }

    #[test]
    fn from_audited_state_normalizes_delivery_order() {
        let mut view = audited_view();
        let child = view.metadata.frontiers.header_best;
        let first = delivery(1, child.hash);
        let second = delivery(2, child.hash);
        for row in [second, first] {
            view.graph
                .record_auxiliary_evidence_delivery(child.hash, row.delivery_id)
                .expect("the child is retained");
            view.deliveries.push(row);
        }

        let engine = HeaderChainEngine::from_audited_state(
            view.graph,
            view.metadata,
            view.selected,
            view.verified,
            view.deliveries,
        )
        .expect("the matching audited views are coherent");

        assert_eq!(engine.aux_deliveries(child.hash), &[first, second]);
    }

    #[test]
    fn durable_auxiliary_outcomes_require_checked_recovery_promotion() {
        let mut view = audited_view();
        let child = view.metadata.frontiers.header_best;
        let row = delivery(3, child.hash);
        view.graph
            .record_auxiliary_evidence_delivery(child.hash, row.delivery_id)
            .expect("the child is retained");
        let caller_selected = row
            .promote_recovered_outcome(
                1,
                [Some([4; 32]), None],
                Some(view.metadata.frontiers.finalized.hash),
            )
            .expect("the raw outcome has a structurally valid shape");

        assert!(matches!(
            HeaderChainEngine::from_audited_state(
                view.graph.clone(),
                view.metadata.clone(),
                view.selected.clone(),
                view.verified.clone(),
                [caller_selected],
            ),
            Err(EngineHydrationError::Incoherent(
                "authoritative auxiliary outcomes require recovery validation"
            ))
        ));
        assert!(matches!(
            HeaderChainEngine::from_untrusted_durable_state(
                view.graph,
                view.metadata,
                view.selected,
                view.verified,
                [UntrustedAuxDeliveryRow::new(
                    row,
                    1,
                    [Some([4; 32]), None],
                    Some(block::Hash([0xf4; 32])),
                )],
            ),
            Err(EngineHydrationError::Incoherent(
                "derived auxiliary boundary is not retained"
            ))
        ));
    }
}
