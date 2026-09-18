//! Get context and calculate difficulty for the next block.

use std::sync::Arc;

use chrono::{DateTime, Utc};

use zakura_chain::{
    block::{self, Hash, Height},
    history_tree::HistoryTree,
    parameters::{Network, NetworkUpgrade},
    serialization::{DateTime32, Duration32},
    work::difficulty::{CompactDifficulty, PartialCumulativeWork, Work, U256},
};

use crate::{
    service::{
        block_iter::any_chain_ancestor_iter,
        check::{
            difficulty::{
                BLOCK_MAX_TIME_SINCE_MEDIAN, POW_ADJUSTMENT_BLOCK_SPAN, POW_MEDIAN_BLOCK_SPAN,
            },
            AdjustedDifficulty,
        },
        finalized_state::ZakuraDb,
        read::{self, tree::history_tree, FINALIZED_STATE_QUERY_RETRIES},
        NonFinalizedState,
    },
    BoxError, GetBlockTemplateChainInfo,
};

/// The number of target block spacings we allow for a miner to mine a standard difficulty block
/// on testnet.
///
/// This is a Zebra-specific standard rule.
const EXTRA_SPACINGS_TO_MINE_A_BLOCK: i32 = 2;

/// Returns the amount of extra time we allow for a miner to mine a standard difficulty block on
/// testnet, for a block at `height`.
///
/// The time scales with the target spacing at `height`, so it stays below the minimum difficulty
/// gap when a future upgrade shortens the spacing. At 75-second spacing it is 150 seconds.
fn extra_time_to_mine_a_block(network: &Network, height: Height) -> Result<Duration32, BoxError> {
    let extra_time =
        NetworkUpgrade::target_spacing_for_height(network, height) * EXTRA_SPACINGS_TO_MINE_A_BLOCK;

    Ok(extra_time.try_into()?)
}

fn finalized_state_query_interrupted_error() -> BoxError {
    "Zakura is committing too many blocks to the state, \
     wait until it syncs to the chain tip"
        .into()
}

/// Returns the [`GetBlockTemplateChainInfo`] for the current best chain.
///
/// Returns an error if the state cannot supply the complete difficulty context.
pub fn get_block_template_chain_info(
    non_finalized_state: &NonFinalizedState,
    db: &ZakuraDb,
    network: &Network,
) -> Result<GetBlockTemplateChainInfo, BoxError> {
    let mut best_relevant_chain_and_history_tree_result =
        best_relevant_chain_and_history_tree(non_finalized_state, db);

    // Retry the finalized state query if it was interrupted by a finalizing block.
    //
    // TODO: refactor this into a generic retry(finalized_closure, process_and_check_closure) fn
    for _ in 0..FINALIZED_STATE_QUERY_RETRIES {
        if best_relevant_chain_and_history_tree_result.is_ok() {
            break;
        }

        best_relevant_chain_and_history_tree_result =
            best_relevant_chain_and_history_tree(non_finalized_state, db);
    }

    let (best_tip_height, best_tip_hash, best_relevant_chain, best_tip_history_tree) =
        best_relevant_chain_and_history_tree_result?;

    difficulty_time_and_history_tree(
        best_relevant_chain,
        best_tip_height,
        best_tip_hash,
        network,
        best_tip_history_tree,
    )
}

/// Accepts a `non_finalized_state`, [`ZakuraDb`], `num_blocks`, and a block hash to start at.
///
/// Iterates over up to the last `num_blocks` blocks, summing up their total work.
/// Divides that total by the number of seconds between the timestamp of the
/// first block in the iteration and 1 block below the last block.
///
/// Returns the solution rate per second for the current best chain, or `None` if
/// the `start_hash` and at least 1 block below it are not found in the chain.
#[allow(unused)]
pub fn solution_rate(
    non_finalized_state: &NonFinalizedState,
    db: &ZakuraDb,
    num_blocks: usize,
    start_hash: Hash,
) -> Option<U256> {
    // Take 1 extra header for calculating the number of seconds between when mining on the first
    // block likely started. The work for the extra header is not added to `total_work`.
    //
    // Since we can't take more headers than are actually in the chain, this automatically limits
    // `num_blocks` to the chain length, like `zcashd` does.
    let mut header_iter =
        any_chain_ancestor_iter::<block::Header>(non_finalized_state, db, start_hash)
            .take(num_blocks.checked_add(1).unwrap_or(num_blocks))
            .peekable();

    let get_work = |header: &block::Header| {
        header
            .difficulty_threshold
            .to_work()
            .expect("work has already been validated")
    };

    // If there are no blocks in the range, we can't return a useful result.
    let last_header = header_iter.peek()?;

    // Initialize the cumulative variables.
    let mut min_time = last_header.time;
    let mut max_time = last_header.time;

    let mut last_work = Work::zero();
    let mut total_work = PartialCumulativeWork::zero();

    for header in header_iter {
        min_time = min_time.min(header.time);
        max_time = max_time.max(header.time);

        last_work = get_work(&header);
        total_work = total_work.checked_add(last_work)?;
    }

    // We added an extra header so we could estimate when mining on the first block
    // in the window of `num_blocks` likely started. But we don't want to add the work
    // for that header.
    total_work -= last_work;

    let work_duration = (max_time - min_time).num_seconds();

    // Avoid division by zero errors and negative average work.
    // This also handles the case where there's only one block in the range.
    if work_duration <= 0 {
        return None;
    }

    let work_duration =
        u64::try_from(work_duration).expect("positive i64 work duration always fits in u64");

    Some(total_work.as_u256() / U256::from(work_duration))
}

/// Do a consistency check by checking the finalized tip before and after all other database
/// queries.
///
/// Returns the best chain tip, recent block headers in reverse height order from the tip,
/// and the tip history tree.
/// Returns an error if the tip obtained before and after is not the same.
///
/// # Panics
///
/// - If we don't have enough blocks in the state.
fn best_relevant_chain_and_history_tree(
    non_finalized_state: &NonFinalizedState,
    db: &ZakuraDb,
) -> Result<
    (
        Height,
        block::Hash,
        Vec<Arc<block::Header>>,
        Arc<HistoryTree>,
    ),
    BoxError,
> {
    let state_tip_before_queries = read::best_tip(non_finalized_state, db).ok_or_else(|| {
        BoxError::from("Zakura's state is empty, wait until it syncs to the chain tip")
    })?;

    let best_relevant_chain: Vec<_> = any_chain_ancestor_iter::<block::Header>(
        non_finalized_state,
        db,
        state_tip_before_queries.1,
    )
    .take(POW_ADJUSTMENT_BLOCK_SPAN)
    .collect();

    if best_relevant_chain.is_empty() {
        return Err("missing genesis block, wait until it is committed".into());
    };

    let history_tree = history_tree(
        non_finalized_state.best_chain(),
        db,
        state_tip_before_queries.into(),
    )
    .ok_or_else(finalized_state_query_interrupted_error)?;

    let state_tip_after_queries =
        read::best_tip(non_finalized_state, db).expect("already checked for an empty tip");

    if state_tip_before_queries != state_tip_after_queries {
        return Err(finalized_state_query_interrupted_error());
    }

    Ok((
        state_tip_before_queries.0,
        state_tip_before_queries.1,
        best_relevant_chain,
        history_tree,
    ))
}

/// Returns the [`GetBlockTemplateChainInfo`] for the supplied `relevant_chain`, tip, `network`,
/// and `history_tree`.
///
/// The `relevant_chain` has recent block headers in reverse height order from the tip.
///
/// See [`get_block_template_chain_info()`] for details.
fn difficulty_time_and_history_tree(
    relevant_chain: Vec<Arc<block::Header>>,
    tip_height: Height,
    tip_hash: block::Hash,
    network: &Network,
    history_tree: Arc<HistoryTree>,
) -> Result<GetBlockTemplateChainInfo, BoxError> {
    if relevant_chain.is_empty() {
        return Err("mining template difficulty context is empty".into());
    }
    let relevant_data: Vec<(CompactDifficulty, DateTime<Utc>)> = relevant_chain
        .iter()
        .map(|header| (header.difficulty_threshold, header.time))
        .collect();

    let cur_time = DateTime32::now();

    // > For each block other than the genesis block , nTime MUST be strictly greater than
    // > the median-time-past of that block.
    // https://zips.z.cash/protocol/protocol.pdf#blockheader
    let median_time_past = DateTime32::try_from(AdjustedDifficulty::median_time(
        relevant_chain
            .iter()
            .take(POW_MEDIAN_BLOCK_SPAN)
            .map(|header| header.time)
            .collect(),
    ))?;

    let min_time = median_time_past
        .checked_add(Duration32::from_seconds(1))
        .expect("a valid block time plus a small constant is in-range");

    // > For each block at block height 2 or greater on Mainnet, or block height 653606 or greater on Testnet, nTime
    // > MUST be less than or equal to the median-time-past of that block plus 90 * 60 seconds.
    //
    // We ignore the height as we are checkpointing on Canopy or higher in Mainnet and Testnet.
    let max_time = median_time_past
        .checked_add(Duration32::from_seconds(BLOCK_MAX_TIME_SINCE_MEDIAN))
        .expect("a valid block time plus a small constant is in-range");

    let cur_time = cur_time.clamp(min_time, max_time);

    // Now that we have a valid time, get the difficulty for that time.
    let difficulty_adjustment = AdjustedDifficulty::new_from_header_time(
        cur_time.into(),
        tip_height,
        network,
        relevant_data.iter().cloned(),
    )?;
    let expected_difficulty = difficulty_adjustment.expected_difficulty_threshold();

    let mut result = GetBlockTemplateChainInfo {
        tip_hash,
        tip_height,
        chain_history_root: history_tree.hash(),
        expected_difficulty,
        cur_time,
        min_time,
        max_time,
    };

    adjust_difficulty_and_time_for_testnet(&mut result, network, tip_height, relevant_data)?;

    Ok(result)
}

/// Adjust the difficulty and time for the testnet minimum difficulty rule.
///
/// The `relevant_data` has recent block difficulties and times in reverse order from the tip.
fn adjust_difficulty_and_time_for_testnet(
    result: &mut GetBlockTemplateChainInfo,
    network: &Network,
    previous_block_height: Height,
    relevant_data: Vec<(CompactDifficulty, DateTime<Utc>)>,
) -> Result<(), BoxError> {
    if network == &Network::Mainnet {
        return Ok(());
    }

    // On testnet, changing the block time can also change the difficulty,
    // due to the minimum difficulty consensus rule:
    // > if the block time of a block at height `height ≥ 299188`
    // > is greater than 6 * PoWTargetSpacing(height) seconds after that of the preceding block,
    // > then the block is a minimum-difficulty block.
    //
    // The max time is always a minimum difficulty block, because the minimum difficulty
    // gap is 7.5 minutes at 75-second spacing, but the maximum gap is 90 minutes.
    // This means that testnet blocks have two valid time ranges with different
    // difficulties:
    // * 1s - 7m30s: standard difficulty
    // * 7m31s - 90m: minimum difficulty
    //
    // In rare cases, this could make some testnet miners produce invalid blocks,
    // if they use the full 90 minute time gap in the consensus rules.
    // (The zcashd getblocktemplate RPC reference doesn't have a max_time field,
    // so there is no standard way of telling miners that the max_time is smaller.)
    //
    // So Zebra adjusts the min or max times to produce a valid time range for the difficulty.
    // There is still a small chance that miners will produce an invalid block, if they are
    // just below the max time, and don't check it.

    // The tip is the first relevant data block, because they are in reverse order.
    let previous_block_time = relevant_data.first().expect("has at least one block").1;
    let previous_block_time: DateTime32 = previous_block_time.try_into()?;

    // The consensus rule uses the spacing at the candidate block's height, which differs from the
    // previous block's spacing at an upgrade that changes the spacing.
    let candidate_height = (previous_block_height + 1).ok_or("candidate height is out of range")?;

    let Some(minimum_difficulty_spacing) =
        NetworkUpgrade::minimum_difficulty_spacing_for_height(network, candidate_height)
    else {
        // Returns early if the testnet minimum difficulty consensus rule is not active
        return Ok(());
    };

    let minimum_difficulty_spacing: Duration32 = minimum_difficulty_spacing.try_into()?;

    // The first minimum difficulty time is strictly greater than the spacing.
    let std_difficulty_max_time = previous_block_time
        .checked_add(minimum_difficulty_spacing)
        .expect("a valid block time plus a small constant is in-range");
    let min_difficulty_min_time = std_difficulty_max_time
        .checked_add(Duration32::from_seconds(1))
        .expect("a valid block time plus a small constant is in-range");

    // If a miner is likely to find a block with the cur_time and standard difficulty
    // within a target block interval or two, keep the original difficulty.
    // Otherwise, try to use the minimum difficulty.
    //
    // This is a Zebra-specific standard rule.
    //
    // We don't need to undo the clamping here:
    // - if cur_time is clamped to min_time, then we're more likely to have a minimum
    //    difficulty block, which makes mining easier;
    // - if cur_time gets clamped to max_time, this is almost always a minimum difficulty block.
    let local_std_difficulty_limit = std_difficulty_max_time
        .checked_sub(extra_time_to_mine_a_block(network, candidate_height)?)
        .expect("a valid block time minus a small constant is in-range");

    if result.cur_time <= local_std_difficulty_limit {
        // Standard difficulty: the cur and max time need to exclude min difficulty blocks

        // The maximum time can only be decreased, and only as far as min_time.
        // The old minimum is still required by other consensus rules.
        result.max_time = std_difficulty_max_time.clamp(result.min_time, result.max_time);

        // The current time only needs to be decreased if the max_time decreased past it.
        // Decreasing the current time can't change the difficulty.
        result.cur_time = result.cur_time.clamp(result.min_time, result.max_time);
    } else {
        // Minimum difficulty: the min and cur time need to exclude std difficulty blocks

        // The minimum time can only be increased, and only as far as max_time.
        // The old maximum is still required by other consensus rules.
        result.min_time = min_difficulty_min_time.clamp(result.min_time, result.max_time);

        // The current time only needs to be increased if the min_time increased past it.
        result.cur_time = result.cur_time.clamp(result.min_time, result.max_time);

        // And then the difficulty needs to be updated for cur_time.
        result.expected_difficulty = AdjustedDifficulty::new_from_header_time(
            result.cur_time.into(),
            previous_block_height,
            network,
            relevant_data.iter().cloned(),
        )?
        .expected_difficulty_threshold();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::{
        parameters::testnet::ConfiguredActivationHeights, serialization::ZcashDeserializeInto,
        work::difficulty::ParameterDifficulty,
    };

    /// Returns the highest offset from the tip time where the testnet template for the block after
    /// `tip_height` keeps the standard difficulty.
    fn last_standard_difficulty_offset(network: &Network, tip_height: Height) -> u32 {
        let tip_time = DateTime32::from(1_700_000_000);
        let difficulty = network.target_difficulty_limit().to_compact();
        let relevant_data = vec![(difficulty, tip_time.to_chrono()); POW_ADJUSTMENT_BLOCK_SPAN];

        let is_standard = |offset: u32| {
            let mut result = GetBlockTemplateChainInfo {
                tip_hash: block::Hash([0; 32]),
                tip_height,
                chain_history_root: None,
                expected_difficulty: difficulty,
                cur_time: tip_time.saturating_add(Duration32::from_seconds(offset)),
                min_time: tip_time.saturating_add(Duration32::from_seconds(1)),
                max_time: tip_time
                    .saturating_add(Duration32::from_seconds(BLOCK_MAX_TIME_SINCE_MEDIAN)),
            };
            adjust_difficulty_and_time_for_testnet(
                &mut result,
                network,
                tip_height,
                relevant_data.clone(),
            )
            .expect("the template context is complete");

            // Only the minimum difficulty branch raises the minimum time.
            result.min_time == tip_time.saturating_add(Duration32::from_seconds(1))
        };

        let offset = (1..BLOCK_MAX_TIME_SINCE_MEDIAN)
            .find(|&offset| !is_standard(offset))
            .expect("the maximum time is a minimum difficulty time");
        offset - 1
    }

    #[test]
    fn testnet_template_standard_difficulty_window_follows_target_spacing() {
        let _init_guard = zakura_test::init();

        const NU7: u32 = 400_000;
        let regtest = Network::new_regtest(
            ConfiguredActivationHeights {
                nu7: Some(NU7),
                ..Default::default()
            }
            .into(),
        );

        // The minimum difficulty gap is 6 target spacings, and the template keeps the standard
        // difficulty for the first 4 of them.
        let pre_nu7_offset = 4 * 75;
        let post_nu7_offset = 4 * 75;

        let testnet = Network::new_default_testnet();
        assert_eq!(
            last_standard_difficulty_offset(&testnet, Height(3_000_000)),
            pre_nu7_offset
        );
        assert_eq!(
            last_standard_difficulty_offset(&regtest, Height(NU7 - 2)),
            pre_nu7_offset
        );

        // Configuring NU7 does not yet change its 75-second spacing.
        for tip in [NU7 - 1, NU7, NU7 + 1_000] {
            assert_eq!(
                last_standard_difficulty_offset(&regtest, Height(tip)),
                post_nu7_offset,
                "tip={tip}",
            );
        }
    }

    #[test]
    fn mining_template_rejects_incomplete_difficulty_context() {
        let block: Arc<block::Block> = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
            .zcash_deserialize_into()
            .expect("the genesis vector is valid");

        // The retained context is bounded independently of the active window.
        let span = u32::try_from(POW_ADJUSTMENT_BLOCK_SPAN).unwrap();

        for network in [Network::Mainnet, Network::new_default_testnet()] {
            for tip in [0, 1, span - 2, span - 1, span, 3_474_810] {
                let required = usize::try_from((tip + 1).min(span)).unwrap();
                for count in 0..=required {
                    let result = difficulty_time_and_history_tree(
                        vec![block.header.clone(); count],
                        Height(tip),
                        block.hash(),
                        &network,
                        Arc::new(HistoryTree::default()),
                    );
                    assert_eq!(
                        result.is_ok(),
                        count == required,
                        "network={network:?}, tip={tip}, count={count}: {result:?}"
                    );
                }
            }
        }
    }
}
