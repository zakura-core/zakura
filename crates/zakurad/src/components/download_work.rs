//! A fixed Mainnet work floor for downloaded block admission.
//!
//! This is a resource policy, separate from consensus validation. If Mainnet
//! difficulty falls below this floor, otherwise valid downloaded blocks will be
//! refused until the policy is updated. Rejections must not score peers or mark
//! blocks consensus-invalid.

use std::sync::Arc;

use zakura_chain::{
    block::{Block, Height},
    parameters::Network,
    work::difficulty::CompactDifficulty,
};

/// Checks context-free header work off the async executor before scheduling effects.
pub(super) async fn check_header_work(
    block: &Block,
    network: &Network,
    height: Height,
) -> Result<(), zakura_consensus::VerifyBlockError> {
    let header = Arc::clone(&block.header);
    let network = network.clone();
    let hash = block.hash();
    tokio::task::spawn_blocking(move || {
        zakura_consensus::check_proof_of_work(&header, &network, &height, &hash)
    })
    .await
    .map_err(|error| zakura_consensus::BlockError::Other(error.to_string()))?
}

/// Last Mainnet checkpoint when the admission floor was introduced.
///
/// Historical downloads are exempt only until the committed tip reaches this
/// boundary. A peer's claimed height cannot disable the floor after that point.
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

/// Validates the target encoding and network limit before applying resource policy.
///
/// Invalid headers retain their consensus error; only a valid target can produce
/// an unscored policy rejection or qualify for the historical exemption.
pub(super) fn check_admission_work(
    network: &Network,
    block: &Block,
    height: Height,
    committed_tip_height: Option<Height>,
) -> Result<bool, zakura_consensus::BlockError> {
    zakura_consensus::difficulty_threshold_is_valid(
        &block.header,
        network,
        &height,
        &block.hash(),
    )?;
    Ok(has_sufficient_work(
        network,
        height,
        committed_tip_height,
        block.header.difficulty_threshold,
    ))
}

/// Whether a downloaded header's claimed target meets the admission work floor.
///
/// This does not verify the header hash or Equihash solution, or replace the
/// parent-dependent difficulty check. Historical Mainnet downloads remain exempt
/// while the committed tip is below the boundary or absent. This preserves
/// historical sync without trusting a peer's old height claim near the tip.
fn has_sufficient_work(
    network: &Network,
    height: Height,
    committed_tip_height: Option<Height>,
    target: CompactDifficulty,
) -> bool {
    if height <= MAINNET_WORK_FLOOR_HEIGHT
        && committed_tip_height.is_none_or(|tip| tip < MAINNET_WORK_FLOOR_HEIGHT)
    {
        return true;
    }

    target_meets_work_floor(network, target)
}

/// Whether the claimed target can authorize Mainnet missing-parent recovery.
///
/// Recovery has no historical exemption: an unverified height must never
/// authorize an extra download. Other networks retain their existing rules.
pub(super) fn target_meets_work_floor(network: &Network, target: CompactDifficulty) -> bool {
    if !matches!(network, Network::Mainnet) {
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
    use zakura_chain::serialization::ZcashDeserializeInto as _;
    use zakura_chain::work::difficulty::ParameterDifficulty as _;

    #[tokio::test]
    async fn header_work_distinguishes_invalid_targets_and_unearned_work() {
        let canonical: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .unwrap();
        check_header_work(&canonical, &Network::Mainnet, Height(1))
            .await
            .unwrap();
        for bits in [0_u32, 0x1d80ffff, 0x23010000, 0x2100ffff] {
            let mut candidate = (*canonical).clone();
            Arc::make_mut(&mut candidate.header).difficulty_threshold =
                CompactDifficulty::from_le_bytes(bits.to_le_bytes());
            assert!(check_admission_work(&Network::Mainnet, &candidate, Height(1), None).is_err());
            let error = check_header_work(&candidate, &Network::Mainnet, Height(1))
                .await
                .unwrap_err();
            assert_eq!(error.misbehavior_score(), 100);
        }
        let mut candidate = (*canonical).clone();
        Arc::make_mut(&mut candidate.header).previous_block_hash =
            zakura_chain::block::Hash([0x55; 32]);
        let error = check_header_work(&candidate, &Network::Mainnet, Height(1))
            .await
            .unwrap_err();
        assert_eq!(error.misbehavior_score(), 100);
    }

    #[tokio::test]
    async fn header_work_checks_equihash_and_limits_custom_network_waivers() {
        use zakura_chain::{parameters::testnet, work::equihash::Solution};

        let network = testnet::Parameters::build().to_network().unwrap();
        let canonical: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .unwrap();
        let mut candidate = (*canonical).clone();
        let target = network.target_difficulty_limit();
        let header = Arc::make_mut(&mut candidate.header);
        header.difficulty_threshold = target.to_compact();
        header.solution = Solution::Common([0; 1344]);
        // The custom network's easy target bounds this cheap hash-filter search.
        let nonce = (0..4096_u32).find(|nonce| {
            Arc::make_mut(&mut candidate.header).nonce.0[..4].copy_from_slice(&nonce.to_le_bytes());
            zakura_header_chain::validate_hash_filter(candidate.hash(), target).is_ok()
        });
        assert!(
            nonce.is_some(),
            "the bounded search finds a hash below the easy target"
        );
        let error = check_header_work(&candidate, &network, Height(1))
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            zakura_consensus::VerifyBlockError::Equihash { .. }
        ));

        let waived = testnet::Parameters::build()
            .with_disable_pow(true)
            .to_network()
            .unwrap();
        check_header_work(&candidate, &waived, Height(1))
            .await
            .unwrap();
        Arc::make_mut(&mut candidate.header).difficulty_threshold =
            CompactDifficulty::from_le_bytes([0; 4]);
        assert!(check_header_work(&candidate, &waived, Height(1))
            .await
            .is_err());
    }

    #[test]
    fn mainnet_work_floor_covers_the_default_semantic_path() {
        assert!(MAINNET_WORK_FLOOR_HEIGHT <= Network::Mainnet.checkpoint_list().max_height());
    }

    #[test]
    fn mainnet_work_floor_uses_the_committed_tip_despite_an_old_height_claim() {
        let easiest_target = Network::Mainnet.target_difficulty_limit().to_compact();
        for height in [Height(0), MAINNET_WORK_FLOOR_HEIGHT] {
            for tip in [
                MAINNET_WORK_FLOOR_HEIGHT,
                MAINNET_WORK_FLOOR_HEIGHT.next().unwrap(),
            ] {
                assert!(!has_sufficient_work(
                    &Network::Mainnet,
                    height,
                    Some(tip),
                    easiest_target,
                ));
            }
            assert!(has_sufficient_work(
                &Network::Mainnet,
                height,
                Some(MAINNET_WORK_FLOOR_HEIGHT.previous().unwrap()),
                easiest_target,
            ));
        }
        assert!(!target_meets_work_floor(&Network::Mainnet, easiest_target));
    }

    #[test]
    fn mainnet_work_floor_preserves_earlier_blocks_and_other_networks() {
        let easiest_target = Network::Mainnet.target_difficulty_limit().to_compact();
        for height in [Height(0), MAINNET_WORK_FLOOR_HEIGHT] {
            assert!(has_sufficient_work(
                &Network::Mainnet,
                height,
                None,
                easiest_target
            ));
        }
        let height = MAINNET_WORK_FLOOR_HEIGHT.next().unwrap();
        assert!(!has_sufficient_work(
            &Network::Mainnet,
            height,
            None,
            easiest_target
        ));
        for network in [
            Network::new_default_testnet(),
            Network::new_regtest(Default::default()),
        ] {
            assert!(has_sufficient_work(
                &network,
                height,
                Some(height),
                easiest_target
            ));
            assert!(target_meets_work_floor(&network, easiest_target));
        }
    }

    #[test]
    fn mainnet_work_floor_compares_expanded_targets_at_the_boundary() {
        let height = MAINNET_WORK_FLOOR_HEIGHT.next().unwrap();
        let max_target =
            MAINNET_REFERENCE_TARGET.to_expanded().unwrap() * MAINNET_TARGET_MULTIPLIER;
        let easiest_allowed_encoding = max_target.to_compact();
        let easier_encoding = CompactDifficulty::from_le_bytes(
            (u32::from_le_bytes(easiest_allowed_encoding.to_le_bytes()) + 1).to_le_bytes(),
        );

        assert!(has_sufficient_work(
            &Network::Mainnet,
            height,
            None,
            MAINNET_REFERENCE_TARGET
        ));
        assert!(has_sufficient_work(
            &Network::Mainnet,
            height,
            None,
            easiest_allowed_encoding
        ));
        assert!(!has_sufficient_work(
            &Network::Mainnet,
            height,
            None,
            easier_encoding
        ));
        assert!(!has_sufficient_work(
            &Network::Mainnet,
            height,
            None,
            CompactDifficulty::from_le_bytes([0; 4]),
        ));
    }
}
