use chrono::{DateTime, Duration};
use zakura_chain::{
    block,
    parameters::testnet::Parameters,
    work::difficulty::{ExpandedDifficulty, ParameterDifficulty as _, U256},
};

use super::super::{AdjustedDifficulty, POW_ADJUSTMENT_BLOCK_SPAN};
use zakura_chain::parameters::NetworkUpgrade;

#[test]
fn custom_target_scaling_clamps_before_overflowing_u256() {
    let candidate_time =
        DateTime::from_timestamp(2_000_000_000, 0).expect("test timestamp is in range");
    let compact = ExpandedDifficulty::from(U256::MAX).to_compact();
    let network = Parameters::build()
        .with_target_difficulty_limit(U256::MAX)
        .expect("the maximum compact-representable target is valid")
        .to_network()
        .expect("the custom network parameters are valid");
    // The recent averaging window is tightly spaced and everything older is far
    // apart, so the actual timespan is large enough to clamp the scaled mean.
    // The context always spans `POW_ADJUSTMENT_BLOCK_SPAN` blocks, which can be
    // wider than the averaging window in force at this height.
    let candidate_height = block::Height(700_000);
    let averaging_window = NetworkUpgrade::averaging_window_for_height(&network, candidate_height);
    let mut context = vec![(compact, candidate_time - Duration::seconds(1)); averaging_window];
    context.extend(vec![
        (compact, candidate_time - Duration::seconds(100_000));
        POW_ADJUSTMENT_BLOCK_SPAN - averaging_window
    ]);
    let adjustment = AdjustedDifficulty::new_from_header_time(
        candidate_time,
        block::Height(699_999),
        &network,
        context,
    )
    .expect("the complete context is accepted");

    assert_eq!(
        adjustment.expected_difficulty_threshold(),
        network.target_difficulty_limit().to_compact()
    );
}
