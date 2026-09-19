//! Fixed test vectors for chain tip distance checks.

use crate::{
    block::Height,
    chain_tip::{is_at_or_near_tip_with_spacing_changes, mock::MockChainTip, ChainTip},
    parameters::{testnet::ConfiguredActivationHeights, Network},
};

/// The near-tip threshold preserves existing behavior for configured upgrades.
#[test]
fn near_tip_threshold_preserves_current_behavior() {
    let _init_guard = zakura_test::init();

    const NU7: u32 = 1_000;
    let network = Network::new_regtest(
        ConfiguredActivationHeights {
            nu7: Some(NU7),
            ..Default::default()
        }
        .into(),
    );

    let (chain_tip, sender) = MockChainTip::new();
    let is_near = |tip: u32, distance| {
        sender.send_best_tip_height(Height(tip));
        sender.send_estimated_distance_to_network_chain_tip(distance);
        chain_tip.is_at_or_near_network_tip(&network)
    };

    // Before NU7, the threshold is unchanged.
    assert!(is_near(100, 16));
    assert!(!is_near(100, 17));

    // NU7 currently preserves the 75-second target spacing.
    assert!(is_near(2_000, 16));
    assert!(!is_near(2_000, 17));
}

/// A spacing change inside the estimated distance uses each segment's spacing.
#[test]
fn near_tip_threshold_handles_spacing_change_boundaries() {
    const ACTIVATION: Height = Height(1_000);
    const OLD_SPACING: i64 = 75;
    const NEW_SPACING: i64 = 25;

    let crosses_activation = |local_tip: u32, estimated_tip: u32| {
        is_at_or_near_tip_with_spacing_changes(
            Height(local_tip),
            Height(estimated_tip),
            OLD_SPACING,
            [(ACTIVATION, NEW_SPACING)],
        )
    };

    // Sixteen old-spacing blocks are exactly the baseline 20-minute window.
    assert!(crosses_activation(ACTIVATION.0 - 16, ACTIVATION.0));
    assert!(!crosses_activation(ACTIVATION.0 - 16, ACTIVATION.0 + 1));

    // Do not apply the new 48-block threshold to old-spacing blocks.
    assert!(!crosses_activation(ACTIVATION.0 - 48, ACTIVATION.0));

    // A mixed range sums the old- and new-spacing segments.
    assert!(crosses_activation(ACTIVATION.0 - 10, ACTIVATION.0 + 18));
    assert!(!crosses_activation(ACTIVATION.0 - 10, ACTIVATION.0 + 19));

    // Entirely post-activation, 48 blocks are 20 minutes.
    assert!(is_at_or_near_tip_with_spacing_changes(
        ACTIVATION,
        Height(ACTIVATION.0 + 48),
        NEW_SPACING,
        [],
    ));
    assert!(!is_at_or_near_tip_with_spacing_changes(
        ACTIVATION,
        Height(ACTIVATION.0 + 49),
        NEW_SPACING,
        [],
    ));

    // Longer spacings retain the baseline block-count window.
    assert!(is_at_or_near_tip_with_spacing_changes(
        Height(100),
        Height(116),
        150,
        [],
    ));
    assert!(!is_at_or_near_tip_with_spacing_changes(
        Height(100),
        Height(117),
        150,
        [],
    ));
}
