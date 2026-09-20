use proptest::prelude::*;

use super::super::Network;
use crate::{
    block::Height,
    parameters::{NetworkUpgrade, TESTNET_MAX_TIME_START_HEIGHT},
};

proptest! {
    /// Check that the mandatory checkpoint is immediately before Canopy activation.
    #[test]
    fn mandatory_checkpoint_is_immediately_before_canopy(network in any::<Network>()) {
        let _init_guard = zakura_test::init();

        let pre_canopy_activation = NetworkUpgrade::Canopy
            .activation_height(&network)
            .expect("Canopy activation height is set")
            .previous()
            .expect("Canopy activation should be above min height");

        assert!(network.mandatory_checkpoint_height() >= pre_canopy_activation);
    }
    #[test]
    /// Asserts that the activation height is correct for the block
    /// maximum time rule on Testnet is correct.
    fn max_block_times_correct_enforcement(height in any::<Height>()) {
        let _init_guard = zakura_test::init();

        assert!(Network::Mainnet.is_max_block_time_enforced(height));
        assert_eq!(Network::new_default_testnet().is_max_block_time_enforced(height), TESTNET_MAX_TIME_START_HEIGHT <= height);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn cumulative_schedule_matches_direct_sum(height in 0u32..25_000, regtest in any::<bool>()) {
        use crate::parameters::subsidy::{halving_block_subsidy, scheduled_issuance_zatoshis};
        let network = if regtest { Network::new_regtest(Default::default()) } else { Network::Mainnet };
        let direct: u128 = (1..=height).map(|h| {
            u128::try_from(i64::from(halving_block_subsidy(Height(h), &network).unwrap())).unwrap()
        }).sum();
        prop_assert_eq!(scheduled_issuance_zatoshis(Height(height), &network).unwrap(), direct);
    }
}
