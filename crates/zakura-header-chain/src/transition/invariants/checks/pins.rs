//! Trust-pin leaf checks against projections and changed nodes.

use crate::{EligibilityReason, Frontier, HeaderNode};

use super::super::InvariantViolation;

pub(crate) fn verify_pins(
    pins: &[Frontier],
    selected: &[Frontier],
    verified: &[Frontier],
    changed_nodes: &[HeaderNode],
) -> Result<(), InvariantViolation> {
    // Configuration stores pins in height order. No pin outside the affected
    // projections and changed nodes can match one of the checks below.
    debug_assert!(
        pins.is_sorted_by(|left, right| left.height < right.height),
        "trust pins must be strictly ascending by height"
    );
    let mut heights = selected
        .first()
        .into_iter()
        .chain(selected.last())
        .chain(verified.first())
        .chain(verified.last())
        .map(|frontier| frontier.height)
        .chain(changed_nodes.iter().map(|node| node.height));
    let Some(first) = heights.next() else {
        return Ok(());
    };
    let (lowest, highest) = heights.fold((first, first), |(lowest, highest), height| {
        (lowest.min(height), highest.max(height))
    });
    let start = pins.partition_point(|pin| pin.height < lowest);
    let end = pins.partition_point(|pin| pin.height <= highest);
    for pin in &pins[start..end] {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EngineMode;
    use proptest::prelude::*;
    use zakura_chain::block;

    fn exhaustive(
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

    #[test]
    fn changed_node_outside_projections_still_requires_a_matching_conflict_reason() {
        let fixture = super::super::super::test_support::fixture(EngineMode::Integrated);
        let mut node = fixture
            .engine
            .graph()
            .header_node(fixture.child.hash)
            .expect("the fixture retains its child")
            .clone();
        node.height = block::Height(10);
        let pin = Frontier::new(node.height, block::Hash([0xff; 32]));
        let selected = [Frontier::new(block::Height(100), block::Hash([0x55; 32]))];
        assert_eq!(
            verify_pins(&[pin], &selected, &[], &[node.clone()]),
            Err(InvariantViolation::TrustPin(pin.height))
        );
        for reason in [
            EligibilityReason::CheckpointConflict {
                height: pin.height,
                expected: pin.hash,
            },
            EligibilityReason::SettledUpgradeConflict {
                height: pin.height,
                expected: pin.hash,
            },
        ] {
            node.eligibility.direct_reasons.clear();
            node.eligibility.direct_reasons.insert(reason);
            assert_eq!(verify_pins(&[pin], &selected, &[], &[node.clone()]), Ok(()));
        }
        node.eligibility.direct_reasons.clear();
        node.eligibility
            .direct_reasons
            .insert(EligibilityReason::CheckpointConflict {
                height: pin.height,
                expected: block::Hash([0xee; 32]),
            });
        assert_eq!(
            verify_pins(&[pin], &selected, &[], &[node]),
            Err(InvariantViolation::TrustPin(pin.height))
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]
        #[test]
        fn bounded_pins_match_exhaustive_checks(
            pins in proptest::collection::btree_map(0u32..200, any::<u8>(), 0..80),
            selected in proptest::collection::btree_map(0u32..200, any::<u8>(), 0..50),
            verified in proptest::collection::btree_map(0u32..200, any::<u8>(), 0..20),
            changed in proptest::collection::vec((0u32..200, any::<u8>(), 0u8..3), 0..12),
        ) {
            let frontier = |(height, hash)| Frontier::new(block::Height(height), block::Hash([hash; 32]));
            let pins: Vec<_> = pins.into_iter().map(frontier).collect();
            let selected: Vec<_> = selected.into_iter().map(frontier).collect();
            let verified: Vec<_> = verified.into_iter().map(frontier).collect();
            let fixture = super::super::super::test_support::fixture(EngineMode::Integrated);
            let template = fixture.engine.graph().header_node(fixture.child.hash).expect("the fixture retains its child");
            let changed: Vec<_> = changed.into_iter().map(|(height, hash, reason)| {
                let mut node = template.clone();
                node.height = block::Height(height);
                node.hash = block::Hash([hash; 32]);
                node.eligibility.direct_reasons.clear();
                if let Some(pin) = pins.iter().find(|pin| pin.height == node.height) {
                    let conflict = match reason {
                        1 => Some(EligibilityReason::CheckpointConflict { height: node.height, expected: pin.hash }),
                        2 => Some(EligibilityReason::SettledUpgradeConflict { height: node.height, expected: pin.hash }),
                        _ => None,
                    };
                    node.eligibility.direct_reasons.extend(conflict);
                }
                node
            }).collect();
            prop_assert_eq!(verify_pins(&pins, &selected, &verified, &changed),
                exhaustive(&pins, &selected, &verified, &changed));
        }
    }
}
