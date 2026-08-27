//! Auxiliary delivery leaf checks.

use std::collections::{HashMap, HashSet};

use crate::graph::HeaderGraphView;
use crate::{AuxDelta, HeaderChainEngine, HeaderNode, PlanCandidate};

use super::super::{InvariantViolation, VerificationMode};

pub(crate) fn verify_aux<G: HeaderGraphView>(
    engine_before_commit: &HeaderChainEngine,
    graph: &G,
    plan: &PlanCandidate,
    mode: VerificationMode,
) -> Result<(), InvariantViolation> {
    let mut put_ids = HashSet::new();
    for change in &plan.change_set.aux_changes {
        let AuxDelta::Put(delivery) = change else {
            continue;
        };
        if !put_ids.insert(delivery.delivery_id)
            || engine_before_commit
                .aux_delivery(delivery.delivery_id)
                .is_some_and(|existing| existing.header_hash != delivery.header_hash)
        {
            return Err(InvariantViolation::Auxiliary(delivery.header_hash));
        }
    }
    let deletes: Vec<_> = plan
        .change_set
        .aux_changes
        .iter()
        .filter_map(|change| match change {
            AuxDelta::Delete {
                header_hash,
                delivery_id,
            } => Some((*header_hash, *delivery_id)),
            AuxDelta::Put(_) => None,
        })
        .collect();
    for (header_hash, delivery_id) in &deletes {
        let exists = engine_before_commit
            .aux_deliveries(*header_hash)
            .iter()
            .any(|delivery| delivery.delivery_id == *delivery_id);
        if !exists {
            return Err(InvariantViolation::Auxiliary(*header_hash));
        }
    }
    let deleted_ids: HashSet<_> = deletes
        .into_iter()
        .map(|(_, delivery_id)| delivery_id)
        .collect();
    let puts: HashMap<_, _> = plan
        .change_set
        .aux_changes
        .iter()
        .filter_map(|change| match change {
            AuxDelta::Put(delivery) => Some((delivery.delivery_id, delivery.as_ref())),
            AuxDelta::Delete { .. } => None,
        })
        .collect();
    let mut nodes: Vec<&HeaderNode> = match mode {
        #[cfg(any(test, feature = "fuzz-impl"))]
        VerificationMode::Exhaustive => graph.view_header_nodes(),
        #[cfg(any(test, not(feature = "fuzz-impl")))]
        VerificationMode::Production => {
            let mut hashes: HashSet<_> = plan
                .change_set
                .put_nodes
                .iter()
                .map(|changed| changed.hash)
                .collect();
            hashes.extend(
                plan.change_set
                    .aux_changes
                    .iter()
                    .map(|change| match change {
                        AuxDelta::Put(delivery) => delivery.header_hash,
                        AuxDelta::Delete { header_hash, .. } => *header_hash,
                    }),
            );
            hashes
                .into_iter()
                .filter_map(|hash| graph.view_header_node(hash))
                .collect()
        }
    };
    nodes.sort_unstable_by_key(|node| node.hash.0);
    for node in nodes {
        let mut deliveries = engine_before_commit.aux_deliveries(node.hash).to_vec();
        deliveries.retain(|delivery| !deleted_ids.contains(&delivery.delivery_id));
        for delivery in puts
            .values()
            .filter(|delivery| delivery.header_hash == node.hash)
        {
            deliveries.retain(|existing| existing.delivery_id != delivery.delivery_id);
            deliveries.push(**delivery);
        }
        for delivery in &deliveries {
            if delivery.header_hash != node.hash
                || !node.aux_delivery_ids.contains(&delivery.delivery_id)
            {
                return Err(InvariantViolation::Auxiliary(node.hash));
            }
        }
        if node.aux_delivery_ids.iter().any(|delivery_id| {
            !deliveries
                .iter()
                .any(|delivery| delivery.delivery_id == *delivery_id)
        }) {
            return Err(InvariantViolation::Auxiliary(node.hash));
        }
    }
    for delivery in puts.values() {
        if graph
            .view_header_node(delivery.header_hash)
            .is_none_or(|node| !node.aux_delivery_ids.contains(&delivery.delivery_id))
        {
            return Err(InvariantViolation::Auxiliary(delivery.header_hash));
        }
    }
    for hash in &plan.change_set.delete_nodes {
        for delivery in engine_before_commit.aux_deliveries(*hash) {
            if !deleted_ids.contains(&delivery.delivery_id) {
                return Err(InvariantViolation::Auxiliary(*hash));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::super::test_support::{
        candidate_with_delta, delivery, fixture, no_change_candidate, projected_graph,
    };
    use super::*;
    use crate::graph::GraphOverlay;
    use crate::{EngineMode, EvidenceId};

    fn verify_in_both_modes(
        fixture: &super::super::super::test_support::Fixture,
        plan: &PlanCandidate,
    ) -> [Result<(), InvariantViolation>; 2] {
        let graph = projected_graph(&fixture.engine, plan);
        [
            verify_aux(&fixture.engine, &graph, plan, VerificationMode::Production),
            verify_aux(&fixture.engine, &graph, plan, VerificationMode::Exhaustive),
        ]
    }

    #[test]
    fn node_delivery_id_requires_a_projected_delivery_row() {
        let fixture = fixture(EngineMode::HeadersOnly);
        let delivery_id = EvidenceId::from_digest([0x71; 32]);
        let mut overlay = GraphOverlay::new(fixture.engine.graph());
        overlay
            .record_auxiliary_evidence_delivery(fixture.child.hash, delivery_id)
            .expect("the fixture node accepts an auxiliary delivery identity");
        let plan = candidate_with_delta(&fixture.engine, overlay.delta());

        assert_eq!(
            verify_in_both_modes(&fixture, &plan),
            [
                Err(InvariantViolation::Auxiliary(fixture.child.hash)),
                Err(InvariantViolation::Auxiliary(fixture.child.hash)),
            ]
        );
    }

    #[test]
    fn delivery_row_requires_the_projected_node_to_reference_its_id() {
        let fixture = fixture(EngineMode::HeadersOnly);
        let delivery_id = EvidenceId::from_digest([0x72; 32]);
        let mut plan = no_change_candidate(&fixture.engine);
        plan.change_set
            .aux_changes
            .push(AuxDelta::Put(Box::new(delivery(
                &fixture.engine,
                fixture.child.hash,
                delivery_id,
            ))));

        assert_eq!(
            verify_in_both_modes(&fixture, &plan),
            [
                Err(InvariantViolation::Auxiliary(fixture.child.hash)),
                Err(InvariantViolation::Auxiliary(fixture.child.hash)),
            ]
        );
    }
}
