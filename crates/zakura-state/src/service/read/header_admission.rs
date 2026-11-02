//! Early difficulty checks using the candidate's committed parent branch.

use std::sync::Arc;

use zakura_chain::{block, parameters::Network};
use zakura_header_chain::{
    pow_adjustment_block_span_for_height, validate_contextual_difficulty_and_time,
    AdjustedDifficulty,
};

use crate::{
    service::{block_iter::any_chain_ancestor_iter, finalized_state::ZakuraDb, NonFinalizedState},
    BoxError,
};

/// Read only the bounded predecessor context for this header, on any committed branch.
/// Missing or concurrently disconnected context is unavailable, not invalid.
pub(crate) fn check(
    non_finalized: &NonFinalizedState,
    db: &ZakuraDb,
    network: &Network,
    header: &block::Header,
) -> Result<Option<block::Height>, BoxError> {
    let ancestors =
        any_chain_ancestor_iter::<block::Header>(non_finalized, db, header.previous_block_hash);
    let Some(parent_height) = ancestors.height else {
        return Ok(None);
    };
    check_context(network, header, parent_height, ancestors)
}

fn check_context(
    network: &Network,
    header: &block::Header,
    parent_height: block::Height,
    ancestors: impl IntoIterator<Item = Arc<block::Header>>,
) -> Result<Option<block::Height>, BoxError> {
    let height = parent_height.next()?;
    let required = pow_adjustment_block_span_for_height(network, height).min(height.as_usize());
    let mut expected_hash = header.previous_block_hash;
    let mut context = Vec::with_capacity(required);
    for ancestor in ancestors.into_iter().take(required) {
        // The state iterator spans a frozen non-finalized chain and the live database.
        // A concurrent finalization can disconnect them: never calculate difficulty
        // using a mixture of two forks at the same heights.
        if ancestor.hash() != expected_hash {
            return Ok(None);
        }
        expected_hash = ancestor.previous_block_hash;
        context.push((ancestor.difficulty_threshold, ancestor.time));
    }
    if context.len() != required {
        return Ok(None);
    }
    let adjustment =
        AdjustedDifficulty::new_from_header_time(header.time, parent_height, network, context)?;
    validate_contextual_difficulty_and_time(header.difficulty_threshold, adjustment)?;
    Ok(Some(height))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Duration};
    use zakura_chain::{
        parameters::{
            testnet::{self, ConfiguredActivationHeights},
            NetworkUpgrade,
        },
        serialization::ZcashDeserializeInto,
        work::difficulty::{CompactDifficulty, ParameterDifficulty},
    };

    fn target(bits: u32) -> CompactDifficulty {
        CompactDifficulty::from_bytes_in_display_order(&bits.to_be_bytes()).unwrap()
    }

    // Context fixtures need hash linkage and target/time history, but not mined solutions:
    // contextual admission deliberately leaves PoW to its caller.
    fn branch(
        network: &Network,
        height: block::Height,
    ) -> (block::Header, Vec<Arc<block::Header>>) {
        let template: block::Block = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .unwrap();
        let mut header = (*template.header).clone();
        let spacing = NetworkUpgrade::target_spacing_for_height(network, height);
        let count = pow_adjustment_block_span_for_height(network, height).min(height.as_usize());
        let candidate_time = DateTime::from_timestamp(2_000_000_000, 0).unwrap();
        let mut context = Vec::new();
        for distance in (1..=count).rev() {
            header.time = candidate_time - spacing * i32::try_from(distance).unwrap();
            header.difficulty_threshold = target(0x1e0ffff0);
            context.push(Arc::new(header.clone()));
            header.previous_block_hash = header.hash();
        }
        context.reverse();
        header.time = candidate_time;
        set_expected_target(network, height, &mut header, &context);
        (header, context)
    }

    fn set_expected_target(
        network: &Network,
        height: block::Height,
        header: &mut block::Header,
        context: &[Arc<block::Header>],
    ) {
        header.difficulty_threshold = AdjustedDifficulty::new_from_header_time(
            header.time,
            height.previous().unwrap(),
            network,
            context.iter().map(|h| (h.difficulty_threshold, h.time)),
        )
        .unwrap()
        .expected_difficulty_threshold();
    }

    #[test]
    fn header_admission_requires_complete_hash_linked_branch_context() {
        let network = Network::Mainnet;
        let height = block::Height(1_000_000);
        let (header, context) = branch(&network, height);
        let parent_height = height.previous().unwrap();
        assert_eq!(
            check_context(&network, &header, parent_height, context.clone()).unwrap(),
            Some(height)
        );
        assert_eq!(
            check_context(
                &network,
                &header,
                parent_height,
                context[..context.len() - 1].iter().cloned()
            )
            .unwrap(),
            None
        );
        let mut disconnected = context.clone();
        Arc::make_mut(&mut disconnected[1]).nonce[0] ^= 1;
        assert_eq!(
            check_context(&network, &header, parent_height, disconnected).unwrap(),
            None
        );
        let mut wrong_target = header;
        wrong_target.difficulty_threshold = target(0x1d00ffff);
        assert!(check_context(&network, &wrong_target, parent_height, context).is_err());
    }

    #[test]
    fn header_admission_uses_active_window_and_testnet_minimum_difficulty() {
        let network = testnet::Parameters::build()
            .with_activation_heights(ConfiguredActivationHeights {
                blossom: Some(1),
                nu7: Some(300_001),
                ..Default::default()
            })
            .unwrap()
            .clear_funding_streams()
            .to_network()
            .unwrap();
        for (height, expected_count) in [(300_000, 28), (300_001, 113), (300_002, 113)] {
            let height = block::Height(height);
            let (mut header, context) = branch(&network, height);
            assert_eq!(context.len(), expected_count);
            assert_eq!(
                check_context(
                    &network,
                    &header,
                    height.previous().unwrap(),
                    context.clone()
                )
                .unwrap(),
                Some(height)
            );
            let spacing = NetworkUpgrade::target_spacing_for_height(&network, height);
            // The exception uses its network-defined gap, including NU7's 450 seconds.
            let gap = if expected_count == 113 {
                Duration::seconds(451)
            } else {
                spacing * 6 + Duration::seconds(1)
            };
            header.time = context[0].time + gap;
            header.difficulty_threshold = network.target_difficulty_limit().to_compact();
            assert_eq!(
                check_context(&network, &header, height.previous().unwrap(), context).unwrap(),
                Some(height)
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn header_admission_state_read_authenticates_only_committed_parents() {
        use crate::{Request, Response};
        use tower::ServiceExt;
        let _guard = zakura_test::init();
        let state = crate::init_test(&Network::Mainnet).await;
        let genesis: Arc<block::Block> = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
            .zcash_deserialize_into()
            .unwrap();
        let child: Arc<block::Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .unwrap();
        assert!(matches!(
            state
                .clone()
                .oneshot(Request::CheckHeaderAdmission(child.header.clone()))
                .await
                .unwrap(),
            Response::HeaderAdmission(None)
        ));
        state
            .clone()
            .oneshot(Request::CommitCheckpointVerifiedBlock(genesis.into()))
            .await
            .unwrap();
        // This real historical target is much easier than the removed sampled floor.
        assert!(matches!(
            state
                .clone()
                .oneshot(Request::CheckHeaderAdmission(child.header.clone()))
                .await
                .unwrap(),
            Response::HeaderAdmission(Some(block::Height(1)))
        ));
        let mut invalid = (*child.header).clone();
        invalid.difficulty_threshold = target(0x1e0ffff0);
        assert!(state
            .clone()
            .oneshot(Request::CheckHeaderAdmission(Arc::new(invalid)))
            .await
            .is_err());
        // Rejecting a header is read-only: a subsequent valid attempt still succeeds.
        assert!(matches!(
            state
                .oneshot(Request::CheckHeaderAdmission(child.header.clone()))
                .await
                .unwrap(),
            Response::HeaderAdmission(Some(block::Height(1)))
        ));
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn header_admission_reads_a_non_best_committed_branch() {
        use crate::{
            arbitrary::Prepare, service::StateService, tests::FakeChainHelper,
            CheckpointVerifiedBlock, Config, Request, Response,
        };
        use tower::ServiceExt;
        let _guard = zakura_test::init();
        let network = Network::Mainnet;
        let (mut state, _, _, _) =
            StateService::new(Config::ephemeral(), &network, block::Height::MAX, 0)
                .await
                .unwrap();
        for (_, bytes) in zakura_test::vectors::MAINNET_BLOCKS.range(0..=1) {
            let block: Arc<block::Block> = bytes.zcash_deserialize_into().unwrap();
            state
                .queue_and_commit_to_finalized_state(CheckpointVerifiedBlock::from(block))
                .await
                .unwrap()
                .unwrap();
        }
        let mut parent: block::Block = zakura_test::vectors::BLOCK_MAINNET_2_BYTES
            .zcash_deserialize_into()
            .unwrap();
        // The non-finalized state fixture requires V4 coinbases even at these early heights.
        parent.transactions = vec![Arc::new(crate::tests::setup::transaction_v4_from_coinbase(
            &parent.transactions[0],
        ))];
        let siblings = Arc::new(parent).make_fake_siblings(2);
        for sibling in &siblings {
            state
                .queue_and_commit_to_non_finalized_state(sibling.clone().prepare(), None)
                .await
                .unwrap()
                .unwrap();
        }
        let best_hash = state
            .read_service
            .latest_non_finalized_state()
            .best_tip()
            .unwrap()
            .1;
        let side_parent = siblings
            .iter()
            .find(|parent| parent.hash() != best_hash)
            .unwrap();
        let child: block::Block = zakura_test::vectors::BLOCK_MAINNET_3_BYTES
            .zcash_deserialize_into()
            .unwrap();
        let mut header = *child.header;
        header.previous_block_hash = side_parent.hash();
        assert!(matches!(
            (&mut state)
                .oneshot(Request::CheckHeaderAdmission(Arc::new(header)))
                .await
                .unwrap(),
            Response::HeaderAdmission(Some(block::Height(3)))
        ));
    }
}
