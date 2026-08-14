//! Validation for untrusted auxiliary rows loaded during recovery.

use std::collections::{HashMap, HashSet};

use zakura_chain::block;

use crate::{
    AuxDelivery, AuxObservationId, AuxOutcomeStatus, EngineHydrationError, MemHeaderStore,
    UntrustedAuxDeliveryRow,
};

/// Validate auxiliary outcome fields and their retained-graph relationships.
///
/// This function rejects duplicate delivery identities, rows outside the graph
/// index, malformed outcomes, invalid boundary topology, and incomplete disputed
/// observation pairs. It promotes outcomes only after every row-level check passes.
pub(crate) fn validate_recovered_auxiliary_rows(
    graph: &MemHeaderStore,
    rows: impl IntoIterator<Item = UntrustedAuxDeliveryRow>,
) -> Result<Vec<AuxDelivery>, EngineHydrationError> {
    let mut delivery_ids = HashSet::new();
    let mut observation_members: HashMap<AuxObservationId, Vec<(block::Hash, block::Hash)>> =
        HashMap::new();
    let mut validated_deliveries = Vec::new();

    for untrusted_row in rows {
        let (delivery, status_code, observation_digests, boundary_hash) =
            untrusted_row.into_parts();
        if !delivery.is_unauthenticated() || !delivery_ids.insert(delivery.delivery_id) {
            return Err(EngineHydrationError::Incoherent(
                "untrusted auxiliary row has duplicate or authoritative base data",
            ));
        }
        let header_node =
            graph
                .header_node(delivery.header_hash)
                .ok_or(EngineHydrationError::Incoherent(
                    "untrusted auxiliary row has no retained header",
                ))?;
        if !header_node.aux_delivery_ids.contains(&delivery.delivery_id) {
            return Err(EngineHydrationError::Incoherent(
                "untrusted auxiliary row disagrees with the delivery index",
            ));
        }
        let validated_delivery = delivery
            .promote_recovered_outcome(status_code, observation_digests, boundary_hash)
            .ok_or(EngineHydrationError::Incoherent(
                "untrusted auxiliary outcome is malformed",
            ))?;
        match validated_delivery.outcome().status() {
            AuxOutcomeStatus::Unauthenticated => {}
            outcome_status => {
                let outcome_boundary_hash = validated_delivery.outcome().boundary_hash().ok_or(
                    EngineHydrationError::Incoherent("derived auxiliary outcome has no boundary"),
                )?;
                let outcome_boundary_node = graph.header_node(outcome_boundary_hash).ok_or(
                    EngineHydrationError::Incoherent("derived auxiliary boundary is not retained"),
                )?;
                let boundary_is_direct_successor =
                    outcome_boundary_node.parent_hash == delivery.header_hash;
                let valid_boundary = match outcome_status {
                    AuxOutcomeStatus::Authenticated => boundary_is_direct_successor,
                    AuxOutcomeStatus::Rejected | AuxOutcomeStatus::Disputed => {
                        outcome_boundary_hash == delivery.header_hash
                            || boundary_is_direct_successor
                    }
                    AuxOutcomeStatus::Unauthenticated => true,
                };
                if !valid_boundary {
                    return Err(EngineHydrationError::Incoherent(
                        "derived auxiliary boundary has invalid topology",
                    ));
                }
                for observation_id in validated_delivery.observation_ids().into_iter().flatten() {
                    observation_members
                        .entry(observation_id)
                        .or_default()
                        .push((delivery.header_hash, outcome_boundary_hash));
                }
            }
        }
        validated_deliveries.push(validated_delivery);
    }

    for disputed_delivery in validated_deliveries
        .iter()
        .filter(|delivery| delivery.outcome().status() == AuxOutcomeStatus::Disputed)
    {
        let has_paired_observation = disputed_delivery
            .observation_ids()
            .into_iter()
            .flatten()
            .any(|observation_id| {
                let Some(members) = observation_members.get(&observation_id) else {
                    return false;
                };
                members.len() == 2
                    && members[0].1 == members[1].1
                    && members
                        .iter()
                        .any(|(header_hash, _)| *header_hash == disputed_delivery.header_hash)
                    && members.iter().any(|(header_hash, member_boundary_hash)| {
                        *header_hash == *member_boundary_hash
                    })
            });
        if !has_paired_observation {
            return Err(EngineHydrationError::Incoherent(
                "disputed auxiliary outcome lacks its paired observation",
            ));
        }
    }

    Ok(validated_deliveries)
}
