//! Read-only consensus metadata for a specified block height.

use serde_json::{json, Value};
use zakura_chain::{
    amount::Amount,
    block::Height,
    parameters::{
        subsidy::{halving_block_subsidy, miner_fee_share, nsm_reissuance_height},
        Network, NetworkUpgrade, GLOBAL_SHIELDED_BUDGET, ORCHARD_PROTOCOL_BLOCK_ACTION_LIMIT,
        SAPLING_BLOCK_IO_LIMIT, SPROUT_BLOCK_JOINSPLIT_LIMIT,
    },
    transparent::MIN_TRANSPARENT_COINBASE_MATURITY,
    work::difficulty::ParameterDifficulty,
};
use zakura_header_chain::{
    POW_DAMPING_FACTOR, POW_MAX_ADJUST_DOWN_PERCENT, POW_MAX_ADJUST_UP_PERCENT,
    POW_MEDIAN_BLOCK_SPAN,
};
use zakura_network::Version;

/// Returns the rules consensus validation applies to a block at `height`.
///
/// A collector can request tip height and tip height + 1 to expose the activation
/// boundary without inferring rules from wall-clock time. Configured-network
/// parameters are deliberately preserved, so consumers must check network magic
/// and chain history before identifying this response as public Testnet.
pub(crate) fn network_parameters(network: &Network, height: Height, build_version: &str) -> Value {
    let upgrade = NetworkUpgrade::current(network, height);
    let spacing = upgrade.target_spacing().num_seconds();
    let gap = NetworkUpgrade::minimum_difficulty_spacing_for_height(network, height)
        .map(|duration| duration.num_seconds());
    let pow_limit = network.target_difficulty_limit();
    let nu7_active = NetworkUpgrade::is_nu7_active(network, height);
    // A 100-zatoshi aggregate has no fractional rounding, so this gives the
    // percentage from the same allocation function used by block validation.
    let miner_fee_percent = i64::from(miner_fee_share(
        height,
        network,
        Amount::try_from(100).expect("100 zatoshis is a valid nonnegative amount"),
    ));

    json!({
        "schemaVersion": 1,
        "network": network.to_string(),
        "networkMagic": hex::encode(network.magic().0),
        "effectiveHeight": height.0,
        "buildVersion": build_version,
        "activationHeight": NetworkUpgrade::Nu7.activation_height(network).map(|height| height.0),
        "branchId": upgrade.branch_id().map(|id| id.to_string()),
        "nu7BranchId": NetworkUpgrade::Nu7.branch_id().map(|id| id.to_string()),
        "nu7Active": nu7_active,
        "targetSpacingSeconds": spacing,
        "minimumProtocolVersion": Version::min_remote_for_height(network, Some(height)).0,
        "nsmReissuanceHeight": nsm_reissuance_height(network).map(|height| height.0),
        "coinbaseMaturityBlocks": MIN_TRANSPARENT_COINBASE_MATURITY,
        "baseSubsidyZat": halving_block_subsidy(height, network).ok().map(i64::from),
        "fees": {
            "minerPercent": miner_fee_percent,
            "nsmPercent": 100 - miner_fee_percent,
            "nsmRounding": "floor-on-block-aggregate",
        },
        "difficulty": {
            "averagingWindowBlocks": upgrade.averaging_window(),
            "averagingTimespanSeconds": upgrade.averaging_window_timespan().num_seconds(),
            "powLimit": pow_limit.to_string(),
            "powLimitCompact": pow_limit.to_compact().to_string(),
            "dampingFactor": POW_DAMPING_FACTOR,
            "maxAdjustUpPercent": POW_MAX_ADJUST_UP_PERCENT,
            "maxAdjustDownPercent": POW_MAX_ADJUST_DOWN_PERCENT,
            "medianTimeSpanBlocks": POW_MEDIAN_BLOCK_SPAN,
            "minimumDifficultyGapSeconds": gap,
            "minimumDifficultyGapMultiplier": gap.map(|seconds| seconds / spacing),
            "minimumDifficultyStrictlyGreater": true,
        },
        "actionLimits": nu7_active.then(|| json!({
            "orchardActionsPerPool": ORCHARD_PROTOCOL_BLOCK_ACTION_LIMIT,
            "saplingSpendsAndOutputs": SAPLING_BLOCK_IO_LIMIT,
            "sproutJoinSplits": SPROUT_BLOCK_JOINSPLIT_LIMIT,
            "globalShieldedBudget": GLOBAL_SHIELDED_BUDGET,
        })),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone, Utc};

    #[test]
    fn public_testnet_rules_follow_activation_boundary() {
        let network = Network::new_default_testnet();
        let activation = NetworkUpgrade::Nu7.activation_height(&network).unwrap();
        let before = network_parameters(&network, Height(activation.0 - 1), "test-build");
        let at = network_parameters(&network, activation, "test-build");
        let after = network_parameters(&network, Height(activation.0 + 1), "test-build");
        assert_eq!(before["targetSpacingSeconds"], 75);
        assert_eq!(before["difficulty"]["averagingWindowBlocks"], 17);
        assert_eq!(before["difficulty"]["minimumDifficultyGapMultiplier"], 6);
        assert!(before["actionLimits"].is_null());
        assert_eq!(before["fees"]["minerPercent"], 100);
        assert_eq!(before["fees"]["nsmPercent"], 0);
        for rules in [at, after] {
            assert_eq!(rules["networkMagic"], "fa1af9bf");
            assert_eq!(rules["targetSpacingSeconds"], 25);
            assert_eq!(rules["difficulty"]["averagingWindowBlocks"], 102);
            assert_eq!(rules["difficulty"]["minimumDifficultyGapSeconds"], 450);
            assert_eq!(rules["difficulty"]["minimumDifficultyGapMultiplier"], 18);
            assert_eq!(rules["branchId"], "77190ad9");
            assert_eq!(rules["fees"]["minerPercent"], 40);
            assert_eq!(rules["fees"]["nsmPercent"], 60);
            assert_eq!(rules["actionLimits"]["globalShieldedBudget"], 330);
        }
        let parent_time = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        for (gap, expected) in [(449, false), (450, false), (451, true)] {
            assert_eq!(
                NetworkUpgrade::is_testnet_min_difficulty_block(
                    &network,
                    activation,
                    parent_time + Duration::seconds(gap),
                    parent_time,
                ),
                expected
            );
        }
    }

    #[test]
    fn mainnet_and_early_testnet_do_not_enable_minimum_difficulty() {
        for network in [Network::Mainnet, Network::new_default_testnet()] {
            let rules = network_parameters(&network, Height(0), "test-build");
            assert!(rules["difficulty"]["minimumDifficultyGapSeconds"].is_null());
            assert!(rules["difficulty"]["minimumDifficultyGapMultiplier"].is_null());
        }
    }
}
