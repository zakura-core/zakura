use chrono::Duration;
use proptest::prelude::*;

use crate::{
    block,
    chain_tip::{mock::MockChainTip, ChainTip},
    parameters::{Network, NetworkUpgrade},
    serialization::arbitrary::datetime_u32,
};

proptest! {
    /// Test network chain tip height estimation.
    ///
    /// Given a pair of block heights, estimate the time difference and use it with the lowest
    /// height to check if the estimation of the height is correct.
    #[test]
    fn network_chain_tip_height_estimation_is_correct(
        network in any::<Network>(),
        mut block_heights in any::<[block::Height; 2]>(),
        current_block_time in datetime_u32(),
        time_displacement_factor in 0.0..1.0_f64,
    ) {
        let (chain_tip, mock_chain_tip_sender) = MockChainTip::new();
        block_heights.sort();
        let current_height = block_heights[0];
        let network_height = block_heights[1];

        mock_chain_tip_sender.send_best_tip_height(current_height);
        mock_chain_tip_sender.send_best_tip_block_time(current_block_time);

        let estimated_time_difference =
            estimate_time_difference(&network, current_height, network_height);

        let time_displacement = calculate_time_displacement(
            time_displacement_factor,
            NetworkUpgrade::current(&network, network_height),
        );

        let mock_local_time = current_block_time + estimated_time_difference + time_displacement;

        assert_eq!(
            chain_tip.estimate_network_chain_tip_height(&network, mock_local_time),
            Some(network_height)
        );
    }
}

/// Estimate the time necessary for the chain to progress from `start_height` to `end_height`,
/// assuming each block is produced at exactly its height-dependent target spacing.
fn estimate_time_difference(
    network: &Network,
    start_height: block::Height,
    end_height: block::Height,
) -> Duration {
    if end_height <= start_height {
        return Duration::zero();
    }
    let mut segment_start = start_height
        .next()
        .expect("an end height above the start means the start is below Height::MAX");
    let mut segment_spacing =
        NetworkUpgrade::target_spacing_for_height(network, start_height).num_seconds();
    let mut elapsed_seconds = 0;

    for (change_height, next_spacing) in NetworkUpgrade::target_spacings(network) {
        if change_height < segment_start {
            segment_spacing = next_spacing.num_seconds();
        } else if change_height <= end_height {
            elapsed_seconds += (change_height - segment_start) * segment_spacing;
            segment_start = change_height;
            segment_spacing = next_spacing.num_seconds();
        }
    }

    elapsed_seconds += (end_height - segment_start + 1) * segment_spacing;
    Duration::seconds(elapsed_seconds)
}

/// Use `displacement` to get a displacement duration between zero and the target spacing of the
/// specified `network_upgrade`.
///
/// This is used to "displace" the time used in the test so that the test inputs aren't exact
/// multiples of the target spacing.
fn calculate_time_displacement(displacement: f64, network_upgrade: NetworkUpgrade) -> Duration {
    let target_spacing = network_upgrade.target_spacing();

    let nanoseconds = target_spacing
        .num_nanoseconds()
        .expect("Target spacing nanoseconds fit in a i64");

    let displaced_nanoseconds = (displacement * nanoseconds as f64)
        .round()
        .clamp(i64::MIN as f64, i64::MAX as f64) as i64;

    Duration::nanoseconds(displaced_nanoseconds)
}
