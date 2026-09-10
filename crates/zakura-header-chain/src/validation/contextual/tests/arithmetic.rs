use chrono::{DateTime, Duration};
use zakura_chain::{
    block,
    parameters::testnet::Parameters,
    work::difficulty::{ExpandedDifficulty, ParameterDifficulty as _, U256},
};

use super::super::AdjustedDifficulty;

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
    let mut context = vec![(compact, candidate_time - Duration::seconds(1)); 17];
    context.extend(vec![
        (compact, candidate_time - Duration::seconds(100_000));
        11
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
