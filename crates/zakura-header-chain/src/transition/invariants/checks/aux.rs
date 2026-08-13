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
    let nodes: Vec<&HeaderNode> = match mode {
        #[cfg(any(test, feature = "fuzz-impl"))]
        VerificationMode::Exhaustive => graph.view_header_nodes(),
        #[cfg(any(test, not(feature = "fuzz-impl")))]
        VerificationMode::Production => plan
            .change_set
            .put_nodes
            .iter()
            .filter_map(|changed| graph.view_header_node(changed.hash))
            .collect(),
    };
    for node in nodes {
        let mut deliveries = engine_before_commit.aux_deliveries(node.hash).to_vec();
        deliveries.retain(|delivery| !deleted_ids.contains(&delivery.delivery_id));
        deliveries.extend(
            puts.values()
                .filter(|delivery| delivery.header_hash == node.hash)
                .map(|delivery| **delivery),
        );
        for delivery in deliveries {
            if delivery.header_hash != node.hash
                || !node.aux_delivery_ids.contains(&delivery.delivery_id)
            {
                return Err(InvariantViolation::Auxiliary(node.hash));
            }
        }
    }
    for delivery in puts.values() {
        if graph.view_header_node(delivery.header_hash).is_none() {
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
