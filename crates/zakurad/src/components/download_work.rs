//! A fixed Mainnet work floor for downloaded block admission.
//!
//! This is a resource policy, separate from consensus validation. If Mainnet
//! difficulty falls below this floor, otherwise valid downloaded blocks will be
//! refused until the policy is updated. Rejections must not score peers or mark
//! blocks consensus-invalid.
//!
//! Historical downloads are exempt only while our committed tip is below the fixed
//! activation height (or absent) and the block's claimed height does not exceed it;
//! after activation, no claimed height bypasses the floor.

use zakura_chain::{block::Height, parameters::Network, work::difficulty::CompactDifficulty};

/// Fixed Mainnet activation height for the download work floor.
///
/// Historical downloads can be exempt only until our committed tip reaches this height.
/// This boundary is independent of checkpoint lists and checkpoint-sync settings.
pub(super) const MAINNET_WORK_FLOOR_HEIGHT: Height = Height(3_535_411);

/// Target of Mainnet block 3,538,452, observed on November 1, 2026 UTC.
///
/// Block hash:
/// `0000000000044089828cb43eee6f528cb2e333cc1380b4308fc07a7babd01bd1`.
/// Source: <https://ccnodes.net/explorer/v1/zcash/blocks>.
const MAINNET_REFERENCE_TARGET: CompactDifficulty =
    CompactDifficulty::from_le_bytes(0x1b6aac57_u32.to_le_bytes());

/// Allow one tenth of the reference difficulty: targets at most ten times larger.
const MAINNET_TARGET_MULTIPLIER: u32 = 10;

/// Whether a downloaded header's claimed target meets the admission work floor.
///
/// This does not verify the header hash or Equihash solution, or replace the
/// parent-dependent difficulty check. A Mainnet download is exempt only when its
/// claimed height is at or below `MAINNET_WORK_FLOOR_HEIGHT` and our committed tip
/// is absent or below that height. An unverified height can restrict the exemption,
/// but cannot grant one after our tip reaches the fixed height. Other networks
/// retain their existing validation rules.
pub(super) fn has_sufficient_work(
    network: &Network,
    committed_tip_height: Option<Height>,
    claimed_height: Height,
    target: CompactDifficulty,
) -> bool {
    if !matches!(network, Network::Mainnet)
        || (claimed_height <= MAINNET_WORK_FLOOR_HEIGHT
            && committed_tip_height.is_none_or(|tip| tip < MAINNET_WORK_FLOOR_HEIGHT))
    {
        return true;
    }

    // The fixed reference is valid, and multiplying its 215-bit target by ten
    // fits in the 256-bit expanded target representation.
    let max_target = MAINNET_REFERENCE_TARGET
        .to_expanded()
        .expect("the fixed Mainnet reference target has a valid compact encoding")
        * MAINNET_TARGET_MULTIPLIER;

    target
        .to_expanded()
        .is_some_and(|target| target <= max_target)
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::work::difficulty::ParameterDifficulty as _;

    #[test]
    fn mainnet_work_floor_requires_both_historical_heights_for_exemption() {
        let network = Network::Mainnet;
        let easiest_target = network.target_difficulty_limit().to_compact();
        for (tip, expected_exemptions) in [
            (None, [true, true, true, false]),
            (
                Some(MAINNET_WORK_FLOOR_HEIGHT.previous().unwrap()),
                [true, true, true, false],
            ),
            (Some(MAINNET_WORK_FLOOR_HEIGHT), [false; 4]),
            (Some(MAINNET_WORK_FLOOR_HEIGHT.next().unwrap()), [false; 4]),
        ] {
            for (claimed_height, exempt) in [
                Height(0),
                MAINNET_WORK_FLOOR_HEIGHT.previous().unwrap(),
                MAINNET_WORK_FLOOR_HEIGHT,
                MAINNET_WORK_FLOOR_HEIGHT.next().unwrap(),
            ]
            .into_iter()
            .zip(expected_exemptions)
            {
                assert_eq!(
                    has_sufficient_work(&network, tip, claimed_height, easiest_target),
                    exempt,
                    "tip {tip:?}, claimed height {claimed_height:?}",
                );
                assert!(has_sufficient_work(
                    &network,
                    tip,
                    claimed_height,
                    MAINNET_REFERENCE_TARGET,
                ));
            }
        }
    }

    #[test]
    fn mainnet_work_floor_preserves_historical_targets_in_both_sync_modes() {
        let network = Network::Mainnet;
        let historical_target = CompactDifficulty::from_le_bytes(0x1c07239f_u32.to_le_bytes());
        for checkpoint_sync in [true, false] {
            let config = zakura_consensus::Config {
                checkpoint_sync,
                ..Default::default()
            };
            let (_, checkpoint) = zakura_consensus::router::init_checkpoint_list(config, &network);
            for tip in [
                Height(1_290_707),
                Height(2_898_025),
                MAINNET_WORK_FLOOR_HEIGHT.previous().unwrap(),
            ] {
                if !checkpoint_sync {
                    assert!(tip > checkpoint);
                }
                assert!(has_sufficient_work(
                    &network,
                    Some(tip),
                    tip.next().unwrap(),
                    historical_target
                ));
            }
            assert!(!has_sufficient_work(
                &network,
                Some(MAINNET_WORK_FLOOR_HEIGHT),
                Height(1_290_708),
                historical_target
            ));
        }
    }

    #[test]
    fn mainnet_work_floor_preserves_other_networks() {
        for network in [
            Network::new_default_testnet(),
            Network::new_regtest(Default::default()),
        ] {
            let easiest_target = network.target_difficulty_limit().to_compact();
            for tip in [
                None,
                Some(MAINNET_WORK_FLOOR_HEIGHT),
                Some(MAINNET_WORK_FLOOR_HEIGHT.next().unwrap()),
            ] {
                for claimed_height in [Height(0), MAINNET_WORK_FLOOR_HEIGHT.next().unwrap()] {
                    assert!(has_sufficient_work(
                        &network,
                        tip,
                        claimed_height,
                        easiest_target
                    ));
                }
            }
        }
    }

    #[test]
    fn mainnet_work_floor_compares_expanded_targets_at_the_boundary() {
        let tip = Some(MAINNET_WORK_FLOOR_HEIGHT);
        let max_target =
            MAINNET_REFERENCE_TARGET.to_expanded().unwrap() * MAINNET_TARGET_MULTIPLIER;
        let easiest_allowed_encoding = max_target.to_compact();
        let easier_encoding = CompactDifficulty::from_le_bytes(
            (u32::from_le_bytes(easiest_allowed_encoding.to_le_bytes()) + 1).to_le_bytes(),
        );

        assert!(has_sufficient_work(
            &Network::Mainnet,
            tip,
            MAINNET_WORK_FLOOR_HEIGHT,
            MAINNET_REFERENCE_TARGET
        ));
        assert!(has_sufficient_work(
            &Network::Mainnet,
            tip,
            MAINNET_WORK_FLOOR_HEIGHT,
            easiest_allowed_encoding
        ));
        assert!(!has_sufficient_work(
            &Network::Mainnet,
            tip,
            MAINNET_WORK_FLOOR_HEIGHT,
            easier_encoding
        ));
        assert!(!has_sufficient_work(
            &Network::Mainnet,
            tip,
            MAINNET_WORK_FLOOR_HEIGHT,
            CompactDifficulty::from_le_bytes([0; 4]),
        ));
    }
}
