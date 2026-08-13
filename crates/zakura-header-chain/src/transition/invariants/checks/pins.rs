//! Trust-pin leaf checks against projections and changed nodes.

use crate::{EligibilityReason, Frontier, HeaderNode};

use super::super::InvariantViolation;

pub(crate) fn verify_pins(
    pins: &[Frontier],
    selected: &[Frontier],
    verified: &[Frontier],
    changed_nodes: &[HeaderNode],
) -> Result<(), InvariantViolation> {
    for pin in pins {
        for projection in [selected, verified] {
            if let Ok(index) =
                projection.binary_search_by_key(&pin.height, |frontier| frontier.height)
            {
                let frontier = projection[index];
                if frontier.hash != pin.hash {
                    return Err(InvariantViolation::TrustPin(pin.height));
                }
            }
        }
        for node in changed_nodes
            .iter()
            .filter(|node| node.height == pin.height && node.hash != pin.hash)
        {
            let has_reason = node.eligibility.direct_reasons.iter().any(|reason| {
                matches!(reason,
                    EligibilityReason::SettledUpgradeConflict { height, expected }
                    | EligibilityReason::CheckpointConflict { height, expected }
                    if *height == pin.height && *expected == pin.hash)
            });
            if !has_reason {
                return Err(InvariantViolation::TrustPin(pin.height));
            }
        }
    }
    Ok(())
}
