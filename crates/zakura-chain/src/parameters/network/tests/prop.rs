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
    #![proptest_config(ProptestConfig::with_cases(std::env::var("NSM_ARITHMETIC_CASES").ok().and_then(|value| value.parse().ok()).unwrap_or(1024)))]

    #[test]
    fn reissuance_matches_integer_oracle(
        balance in prop_oneof![Just(0i64), Just(1i64), Just(crate::amount::MAX_MONEY), 0i64..=crate::amount::MAX_MONEY],
        height in prop_oneof![Just(2u32), Just(3u32), Just(4u32), Just(u32::MAX), 3u32..10_000_000],
    ) {
        use crate::{
            amount::Amount,
            parameters::{
                subsidy::{block_subsidy, block_subsidy_fraction_numerator, halving_block_subsidy},
                testnet::{ConfiguredActivationHeights, RegtestParameters},
            },
        };

        let network = Network::new_regtest(RegtestParameters {
            activation_heights: ConfiguredActivationHeights { nu7: Some(2), ..Default::default() },
            nsm_reissuance_height: Some(Height(3)),
            ..Default::default()
        });
        let scheduled = i64::from(halving_block_subsidy(Height(height), &network).unwrap());
        let actual = i64::from(block_subsidy(Height(height), &network, Some(Amount::try_from(balance).unwrap())).unwrap());
        let bonus = if cfg!(feature = "nu7") && height >= 3 {
            let numerator = i128::try_from(block_subsidy_fraction_numerator(Height(height), &network)).unwrap();
            (i128::from(balance) * numerator + 9_999_999_999) / 10_000_000_000
        } else {
            0
        };
        prop_assert_eq!(i128::from(actual), i128::from(scheduled) + bonus);
        prop_assert!(bonus >= 0 && bonus <= i128::from(balance));
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
