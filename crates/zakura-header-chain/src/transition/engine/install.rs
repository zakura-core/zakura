//! In-memory install helpers for verified projections and auxiliary deliveries.

use std::collections::HashMap;

use zakura_chain::block;

use crate::{AuxDelivery, AuxDelta, Frontier, MemHeaderStore, ProjectionDelta};

use super::EngineHydrationError;

/// Verifies that a projection is a coherent, contiguous path through the graph.
///
/// The projection must begin at the graph's finalized frontier, end at `tip`,
/// reference graph nodes at matching heights, and advance one parent-linked
/// height at a time. When `require_verified_bodies` is set, every frontier
/// after finality must be recorded as accepted by full state.
///
/// Returns [`EngineHydrationError::Incoherent`] on the first violated
/// projection invariant.
pub(super) fn verify_projection(
    graph: &MemHeaderStore,
    projection: &[Frontier],
    tip: Frontier,
    require_verified_bodies: bool,
) -> Result<(), EngineHydrationError> {
    if projection.first().copied() != Some(graph.finalized_frontier())
        || projection.last().copied() != Some(tip)
    {
        return Err(EngineHydrationError::Incoherent(
            "projection endpoints disagree with metadata",
        ));
    }

    for frontier in projection {
        let node = graph
            .header_node(frontier.hash)
            .filter(|node| node.height == frontier.height)
            .ok_or(EngineHydrationError::Incoherent(
                "projection frontier height disagrees with graph",
            ))?;
        if require_verified_bodies
            && *frontier != graph.finalized_frontier()
            && !matches!(
                node.body_validation_state,
                crate::BodyValidationState::Verified { .. }
            )
        {
            return Err(EngineHydrationError::Incoherent(
                "verified projection contains an unverified body",
            ));
        }
    }

    for pair in projection.windows(2) {
        if pair[1].height.0 != pair[0].height.0 + 1
            || graph
                .header_node(pair[1].hash)
                .is_none_or(|node| node.parent_hash != pair[0].hash)
        {
            return Err(EngineHydrationError::Incoherent(
                "projection is not a contiguous graph path",
            ));
        }
    }
    Ok(())
}

/// Merges a verified replacement into a height-ordered frontier projection.
///
/// Retires entries below `remove_before`, removes the old suffix beginning at
/// `remove_from`, then appends the replacement suffix from `put`.
///
/// Assumes transition planning has validated that `put` preserves ascending,
/// contiguous projection order.
pub(super) fn merge_projection_delta(projection: &mut Vec<Frontier>, delta: &ProjectionDelta) {
    if let Some(height) = delta.remove_before {
        projection.retain(|frontier| frontier.height >= height);
    }
    if let Some(height) = delta.remove_from {
        projection.retain(|frontier| frontier.height < height);
    }
    projection.extend(delta.put.iter().copied());
}

/// Merges verified auxiliary-delivery changes into the in-memory index.
///
/// Changes are merged in order. A `Put` upserts by delivery ID within its
/// header bucket and keeps that bucket sorted. A `Delete` is a no-op when the
/// delivery is absent and removes the bucket when it becomes empty.
///
/// Assumes transition planning has validated retained headers and global
/// delivery-ID uniqueness.
pub(super) fn merge_auxiliary_delivery_changes(
    aux: &mut HashMap<block::Hash, Vec<AuxDelivery>>,
    changes: &[AuxDelta],
) {
    for change in changes {
        match change {
            AuxDelta::Put(delivery) => {
                let rows = aux.entry(delivery.header_hash).or_default();
                rows.retain(|row| row.delivery_id != delivery.delivery_id);
                rows.push(**delivery);
                rows.sort_unstable_by_key(|row| row.delivery_id);
            }
            AuxDelta::Delete {
                header_hash,
                delivery_id,
            } => {
                if let Some(rows) = aux.get_mut(header_hash) {
                    rows.retain(|row| row.delivery_id != *delivery_id);
                    if rows.is_empty() {
                        aux.remove(header_hash);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, num::NonZeroU64, sync::Arc};

    use zakura_chain::block;
    use zakura_chain::block::genesis::regtest_genesis_block;

    use super::*;
    use crate::{
        AuxAuthentication, BodySizeHint, BodyValidationState, BranchId, HeaderGeneration,
        HeaderValidationState, HeaderWorkAuthority, InsertResult, SourceId,
    };

    fn graph_with_child() -> (MemHeaderStore, Frontier) {
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
        let header = Arc::new(header);
        let child = match graph
            .insert(
                header,
                HeaderValidationState::Valid,
                [],
                BodyValidationState::Unknown,
            )
            .expect("the child inserts")
        {
            InsertResult::Inserted(frontier) | InsertResult::AlreadyPresent(frontier) => frontier,
        };
        (graph, child)
    }

    fn delivery(
        delivery_id: crate::EvidenceId,
        header_hash: block::Hash,
        source: SourceId,
    ) -> AuxDelivery {
        let owner = HeaderWorkAuthority {
            header_generation: HeaderGeneration::new(0),
            branch: BranchId::new(header_hash, header_hash),
        }
        .bind(
            1,
            NonZeroU64::new(1).expect("the fixture request ID is nonzero"),
        );
        AuxDelivery {
            delivery_id,
            header_hash,
            source,
            owner: owner.into(),
            body_size: BodySizeHint::Unknown,
            tree_aux: None,
            authentication: AuxAuthentication::Unauthenticated,
        }
    }

    #[test]
    fn projection_validation_accepts_only_a_contiguous_graph_path() {
        let (graph, child) = graph_with_child();
        let anchor = graph.finalized_frontier();
        assert_eq!(
            verify_projection(&graph, &[anchor, child], child, false),
            Ok(())
        );
        assert_eq!(
            verify_projection(&graph, &[child], child, false),
            Err(EngineHydrationError::Incoherent(
                "projection endpoints disagree with metadata"
            ))
        );
        assert_eq!(
            verify_projection(&graph, &[anchor, child], anchor, false),
            Err(EngineHydrationError::Incoherent(
                "projection endpoints disagree with metadata"
            ))
        );
        assert_eq!(
            verify_projection(&graph, &[anchor, anchor, child], child, false),
            Err(EngineHydrationError::Incoherent(
                "projection is not a contiguous graph path"
            ))
        );
        assert_eq!(
            verify_projection(&graph, &[anchor, child], child, true),
            Err(EngineHydrationError::Incoherent(
                "verified projection contains an unverified body"
            ))
        );
    }

    #[test]
    fn projection_delta_retires_prefix_and_replaces_suffix() {
        let hashes = [
            block::Hash([1; 32]),
            block::Hash([2; 32]),
            block::Hash([3; 32]),
            block::Hash([4; 32]),
        ];
        let mut projection = vec![
            Frontier::new(block::Height(1), hashes[0]),
            Frontier::new(block::Height(2), hashes[1]),
            Frontier::new(block::Height(3), hashes[2]),
        ];
        merge_projection_delta(
            &mut projection,
            &ProjectionDelta {
                remove_before: Some(block::Height(2)),
                remove_from: Some(block::Height(3)),
                put: vec![Frontier::new(block::Height(3), hashes[3])],
            },
        );
        assert_eq!(
            projection,
            vec![
                Frontier::new(block::Height(2), hashes[1]),
                Frontier::new(block::Height(3), hashes[3]),
            ]
        );
    }

    #[test]
    fn auxiliary_delta_upserts_sorts_and_removes_empty_buckets() {
        let hash = block::Hash([0x11; 32]);
        let first_id = crate::EvidenceId::from_digest([1; 32]);
        let second_id = crate::EvidenceId::from_digest([2; 32]);
        let original = delivery(first_id, hash, SourceId::from_digest([3; 32]));
        let replacement = delivery(first_id, hash, SourceId::from_digest([4; 32]));
        let second = delivery(second_id, hash, SourceId::from_digest([5; 32]));
        let mut aux = HashMap::from([(hash, vec![original])]);

        merge_auxiliary_delivery_changes(
            &mut aux,
            &[
                AuxDelta::Put(Box::new(second)),
                AuxDelta::Put(Box::new(replacement)),
            ],
        );
        assert_eq!(aux[&hash], vec![replacement, second]);

        merge_auxiliary_delivery_changes(
            &mut aux,
            &[
                AuxDelta::Delete {
                    header_hash: hash,
                    delivery_id: first_id,
                },
                AuxDelta::Delete {
                    header_hash: hash,
                    delivery_id: second_id,
                },
            ],
        );
        assert!(!aux.contains_key(&hash));
    }
}
