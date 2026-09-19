//! Fixed test vectors for chain tip distance checks.

use crate::{
    block::Height,
    chain_tip::{at_or_near_tip_threshold, mock::MockChainTip, ChainTip, AT_OR_NEAR_TIP_THRESHOLD},
    parameters::{testnet::ConfiguredActivationHeights, Network},
};

/// The near-tip threshold keeps about 20 minutes when NU7 shortens the target spacing.
#[test]
fn at_or_near_tip_threshold_follows_the_target_spacing() {
    let _init_guard = zakura_test::init();

    const NU7: u32 = 1_000;
    let network = Network::new_regtest(
        ConfiguredActivationHeights {
            nu7: Some(NU7),
            ..Default::default()
        }
        .into(),
    );

    assert_eq!(
        at_or_near_tip_threshold(&Network::Mainnet, Height(3_000_000)),
        AT_OR_NEAR_TIP_THRESHOLD
    );
    assert_eq!(
        at_or_near_tip_threshold(&network, Height(NU7 - 1)),
        AT_OR_NEAR_TIP_THRESHOLD
    );
    assert_eq!(at_or_near_tip_threshold(&network, Height(NU7)), 48);

    let (chain_tip, sender) = MockChainTip::new();
    let is_near = |tip: u32, distance| {
        sender.send_best_tip_height(Height(tip));
        sender.send_estimated_distance_to_network_chain_tip(distance);
        chain_tip.is_at_or_near_network_tip(&network)
    };

    // Before NU7, the threshold is unchanged.
    assert!(is_near(100, 16));
    assert!(!is_near(100, 17));

    // After NU7, 48 blocks at 25 seconds cover the same 20 minutes.
    assert!(is_near(2_000, 47));
    assert!(is_near(2_000, 48));
    assert!(!is_near(2_000, 49));

    // The spacing at the estimated network tip decides the threshold.
    assert!(is_near(NU7 - 10, 20));
}
