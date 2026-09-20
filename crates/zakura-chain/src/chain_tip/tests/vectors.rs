//! Fixed test vectors for chain tip distance checks.

use chrono::{DateTime, Duration};

use crate::{
    block::Height,
    chain_tip::{
        is_at_or_near_tip_with_spacing_changes, mock::MockChainTip, ChainTip,
        NetworkChainTipHeightEstimator,
    },
    parameters::{testnet::ConfiguredActivationHeights, Network},
};

/// The near-tip threshold preserves its time window when NU7 shortens the target spacing.
#[test]
fn near_tip_threshold_follows_nu7_spacing() {
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

    // After NU7, 48 blocks at 25 seconds cover the same 20-minute window.
    assert!(is_near(2_000, 48));
    assert!(!is_near(2_000, 49));
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

    // The activation block already uses the new spacing.
    assert!(crosses_activation(ACTIVATION.0 - 16, ACTIVATION.0));
    assert!(crosses_activation(ACTIVATION.0 - 16, ACTIVATION.0 + 2));
    assert!(!crosses_activation(ACTIVATION.0 - 16, ACTIVATION.0 + 3));

    // Do not apply the new 48-block threshold to old-spacing blocks.
    assert!(!crosses_activation(ACTIVATION.0 - 48, ACTIVATION.0));

    // A mixed range sums the old- and new-spacing segments.
    assert!(crosses_activation(ACTIVATION.0 - 10, ACTIVATION.0 + 18));
    assert!(crosses_activation(ACTIVATION.0 - 10, ACTIVATION.0 + 20));
    assert!(!crosses_activation(ACTIVATION.0 - 10, ACTIVATION.0 + 21));

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

/// The activation block is the first block mined at the new target spacing.
#[test]
fn network_tip_estimator_switches_spacing_before_the_activation_block() {
    const ACTIVATION: u32 = 1_000;
    let network = Network::new_regtest(
        ConfiguredActivationHeights {
            nu7: Some(ACTIVATION),
            ..Default::default()
        }
        .into(),
    );
    let current_time =
        DateTime::from_timestamp(2_000_000_000, 0).expect("test timestamp is in range");
    let estimate_after = |seconds| {
        NetworkChainTipHeightEstimator::new(current_time, Height(ACTIVATION - 1), &network)
            .estimate_height_at(current_time + Duration::seconds(seconds))
    };

    assert_eq!(estimate_after(24), Height(ACTIVATION - 1));
    assert_eq!(estimate_after(25), Height(ACTIVATION));
    assert_eq!(estimate_after(49), Height(ACTIVATION));
    assert_eq!(estimate_after(50), Height(ACTIVATION + 1));
}
