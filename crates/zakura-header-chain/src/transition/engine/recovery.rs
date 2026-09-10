//! Validation for untrusted auxiliary rows loaded during recovery.

use std::collections::HashSet;

use crate::{AuxDelivery, EngineHydrationError, MemHeaderStore, UntrustedAuxDeliveryRow};

/// Validate durable auxiliary delivery rows and discard unauthenticated outcome claims.
///
/// This function rejects duplicate delivery identities, rows outside the graph
/// index, and malformed outcome encodings. The durable format does not retain the
/// full-state observation that derived an outcome, so recovery never promotes one.
pub(crate) fn validate_recovered_auxiliary_rows(
    graph: &MemHeaderStore,
    rows: impl IntoIterator<Item = UntrustedAuxDeliveryRow>,
) -> Result<Vec<AuxDelivery>, EngineHydrationError> {
    let mut delivery_ids = HashSet::new();
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
        let outcome_has_valid_shape = delivery
            .promote_recovered_outcome(status_code, observation_digests, boundary_hash)
            .is_some();
        if !outcome_has_valid_shape {
            return Err(EngineHydrationError::Incoherent(
                "untrusted auxiliary outcome is malformed",
            ));
        }
        validated_deliveries.push(delivery);
    }
    Ok(validated_deliveries)
}
