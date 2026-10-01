//! Get context and calculate difficulty for the next block.

use std::sync::Arc;

use chrono::{DateTime, Utc};

use zakura_chain::{
    amount::NonNegative,
    block::{self, Hash, Height},
    history_tree::HistoryTree,
    parameters::{subsidy::is_zip234_active, Network, NetworkUpgrade},
    serialization::{DateTime32, Duration32},
    value_balance::ValueBalance,
    work::difficulty::{CompactDifficulty, PartialCumulativeWork, Work, U256},
};

use crate::{
    service::{
        block_iter::any_chain_ancestor_iter,
        check::{
            difficulty::{BLOCK_MAX_TIME_SINCE_MEDIAN, POW_MEDIAN_BLOCK_SPAN},
            difficulty_context, AdjustedDifficulty,
        },
        finalized_state::ZakuraDb,
        read::{self, tree::history_tree, FINALIZED_STATE_QUERY_RETRIES},
        NonFinalizedState,
    },
    BoxError, GetBlockTemplateChainInfo,
};

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
        best_relevant_chain_and_history_tree(non_finalized_state, db, network);

    // Retry the finalized state query if it was interrupted by a finalizing block.
    //
    // TODO: refactor this into a generic retry(finalized_closure, process_and_check_closure) fn
    for _ in 0..FINALIZED_STATE_QUERY_RETRIES {
        if best_relevant_chain_and_history_tree_result.is_ok() {
            break;
        }

        best_relevant_chain_and_history_tree_result =
            best_relevant_chain_and_history_tree(non_finalized_state, db, network);
    }

    let (best_tip_height, best_tip_hash, best_relevant_chain, best_tip_history_tree) =
        best_relevant_chain_and_history_tree_result?;

    // A candidate block's ZIP 234 subsidy comes from the money reserve after its parent,
    // which is this tip.
    let tip_info = read::block_info(non_finalized_state.best_chain(), db, best_tip_hash.into());
    let value_pools = match tip_info {
        Some(block_info) => *block_info.value_pools(),
        None if best_tip_height
            .next()
            .is_ok_and(|height| is_zip234_active(network, height)) =>
        {
            return Err("missing chain value pools for the ZIP 234 candidate block parent".into());
        }
        None => ValueBalance::zero(),
    };

    difficulty_time_and_history_tree(
        best_relevant_chain,
        best_tip_height,
        best_tip_hash,
        network,
        DateTime32::now(),
        best_tip_history_tree,
        value_pools,
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
    network: &Network,
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

    // The template's candidate block, one above the tip, selects the averaging window.
    let candidate_height = state_tip_before_queries
        .0
        .next()
        .map_err(|_| BoxError::from("the best chain tip is at the maximum height"))?;
    let best_relevant_chain = difficulty_context(
        network,
        candidate_height,
        any_chain_ancestor_iter::<block::Header>(
            non_finalized_state,
            db,
            state_tip_before_queries.1,
        ),
    );

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
/// local clock time `cur_time`, and `history_tree`.
///
/// The `relevant_chain` has recent block headers in reverse height order from the tip.
///
/// See [`get_block_template_chain_info()`] for details.
fn difficulty_time_and_history_tree(
    relevant_chain: Vec<Arc<block::Header>>,
    tip_height: Height,
    tip_hash: block::Hash,
    network: &Network,
    cur_time: DateTime32,
    history_tree: Arc<HistoryTree>,
    value_pools: ValueBalance<NonNegative>,
) -> Result<GetBlockTemplateChainInfo, BoxError> {
    if relevant_chain.is_empty() {
        return Err("mining template difficulty context is empty".into());
    }
    let relevant_data: Vec<(CompactDifficulty, DateTime<Utc>)> = relevant_chain
        .iter()
        .map(|header| (header.difficulty_threshold, header.time))
        .collect();

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
        value_pools,
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
    // > is greater than the minimum difficulty gap for that height after the preceding block,
    // > then the block is a minimum-difficulty block.
    //
    // When the first minimum difficulty time fits within the consensus maximum time, testnet
    // blocks have two valid time ranges with different difficulties, shown here before NU7:
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

    // Offer minimum difficulty work only once `cur_time` is past the last standard difficulty
    // time. Switching earlier would mean moving `cur_time` into the future, and each extra
    // minimum difficulty block drags the averaged difficulty far below its target.
    //
    // The caller already clamped `cur_time` to the consensus range, so the difficulty
    // selected here is valid for it.
    if result.cur_time <= std_difficulty_max_time {
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
    use crate::service::check::difficulty::{
        pow_adjustment_block_span_for_height, POW_ADJUSTMENT_BLOCK_SPAN,
    };
    use zakura_chain::{
        parameters::testnet::{
            ConfiguredActivationHeights, ConfiguredCheckpoints, Parameters, RegtestParameters,
        },
        serialization::ZcashDeserializeInto,
        work::difficulty::ParameterDifficulty,
    };

    /// Returns the highest offset from the tip time where the testnet template for the block after
    /// `tip_height` keeps the standard difficulty.
    fn last_standard_difficulty_offset(network: &Network, tip_height: Height) -> u32 {
        let tip_time = DateTime32::from(1_700_000_000);
        let difficulty = network.target_difficulty_limit().to_compact();
        let span = pow_adjustment_block_span_for_height(
            network,
            tip_height
                .next()
                .expect("the test tip is below the maximum height"),
        );
        let relevant_data = vec![(difficulty, tip_time.to_chrono()); span];

        let is_standard = |offset: u32| {
            let mut result = GetBlockTemplateChainInfo {
                value_pools: ValueBalance::zero(),
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
    fn testnet_template_standard_difficulty_window_uses_nu7_consensus_gap() {
        let _init_guard = zakura_test::init();

        const NU7: u32 = 400_000;
        let regtest = Network::new_regtest(
            ConfiguredActivationHeights {
                nu7: Some(NU7),
                ..Default::default()
            }
            .into(),
        );

        // The template keeps standard difficulty for the whole minimum difficulty gap.
        let pre_nu7_offset = 6 * 75;
        let post_nu7_offset = 18 * 25;

        let testnet = Network::new_default_testnet();
        assert_eq!(
            last_standard_difficulty_offset(&testnet, Height(3_000_000)),
            pre_nu7_offset
        );
        assert_eq!(
            last_standard_difficulty_offset(&regtest, Height(NU7 - 2)),
            pre_nu7_offset
        );

        // The block at NU7 already uses the NU7 spacing.
        for tip in [NU7 - 1, NU7, NU7 + 1_000] {
            assert_eq!(
                last_standard_difficulty_offset(&regtest, Height(tip)),
                post_nu7_offset,
                "tip={tip}",
            );
        }
    }

    #[test]
    fn testnet_template_uses_candidate_spacing_at_blossom() {
        let _init_guard = zakura_test::init();
        const BLOSSOM: u32 = 400_000;
        let genesis = Network::new_regtest(Default::default()).genesis_hash();
        let network = Network::new_regtest(RegtestParameters {
            activation_heights: ConfiguredActivationHeights {
                blossom: Some(BLOSSOM),
                ..Default::default()
            },
            // Canopy defaults to Blossom, so the checkpoints must cover the block before it.
            checkpoints: Some(ConfiguredCheckpoints::HeightsAndHashes(vec![
                (Height(0), genesis),
                (Height(BLOSSOM - 1), block::Hash([1; 32])),
            ])),
            ..Default::default()
        });
        assert_eq!(
            last_standard_difficulty_offset(&network, Height(BLOSSOM - 2)),
            900
        );
        // The parent is pre-Blossom but the candidate already uses 75 seconds.
        assert_eq!(
            last_standard_difficulty_offset(&network, Height(BLOSSOM - 1)),
            450
        );
        assert_eq!(
            last_standard_difficulty_offset(&network, Height(BLOSSOM)),
            450
        );
    }

    /// Testnet templates keep `cur_time` at the clock, switch to minimum difficulty only after
    /// the consensus gap, and advertise only timestamps that are valid for the template bits.
    #[test]
    fn mining_template_testnet_uses_clock_at_difficulty_timeout() {
        let _init_guard = zakura_test::init();
        const NU7: u32 = 800_000;
        let configured_testnet = Parameters::build()
            .with_activation_heights(ConfiguredActivationHeights {
                blossom: Some(1),
                nu7: Some(NU7),
                ..Default::default()
            })
            .unwrap()
            .clear_funding_streams()
            .to_network()
            .unwrap();
        let block: block::Block = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
            .zcash_deserialize_into()
            .expect("the genesis vector is valid");
        let tip_time = DateTime32::from(1_700_000_000);

        for (network, tip_height) in [
            (Network::new_default_testnet(), Height(3_000_000)),
            (configured_testnet.clone(), Height(NU7 - 2)),
            (configured_testnet.clone(), Height(NU7 - 1)),
            (configured_testnet, Height(NU7)),
        ] {
            assert!(!network.disable_pow());
            let candidate_height = tip_height.next().unwrap();
            let gap: Duration32 =
                NetworkUpgrade::minimum_difficulty_spacing_for_height(&network, candidate_height)
                    .expect("the minimum difficulty rule is active at the test height")
                    .try_into()
                    .unwrap();
            let spacing: Duration32 =
                NetworkUpgrade::target_spacing_for_height(&network, candidate_height)
                    .try_into()
                    .unwrap();
            let limit = network.target_difficulty_limit();
            let mut header = *block.header;
            header.time = tip_time.to_chrono();
            header.difficulty_threshold = (limit / 4_u32).to_compact();
            let span = pow_adjustment_block_span_for_height(&network, candidate_height);
            let relevant_chain: Vec<_> = (0..span)
                .map(|index| {
                    let mut header = header;
                    let age = u32::try_from(index).unwrap() * spacing.seconds();
                    header.time = tip_time
                        .checked_sub(Duration32::from_seconds(age))
                        .unwrap()
                        .to_chrono();
                    Arc::new(header)
                })
                .collect();
            let median_time = tip_time
                .checked_sub(Duration32::from_seconds(
                    u32::try_from(POW_MEDIAN_BLOCK_SPAN / 2).unwrap() * spacing.seconds(),
                ))
                .unwrap();
            let consensus_min = median_time
                .checked_add(Duration32::from_seconds(1))
                .unwrap();
            let consensus_max = median_time
                .checked_add(Duration32::from_seconds(BLOCK_MAX_TIME_SINCE_MEDIAN))
                .unwrap();
            let last_standard_time = tip_time.checked_add(gap).unwrap();
            let first_minimum_time = last_standard_time
                .checked_add(Duration32::from_seconds(1))
                .unwrap();

            // Cover the old two-spacing and one-spacing early-switch windows, and the strict
            // consensus timeout.
            for offset in [
                gap.seconds() - 2 * spacing.seconds() + 1,
                gap.seconds() - spacing.seconds() + 1,
                gap.seconds() - 1,
                gap.seconds(),
                gap.seconds() + 1,
            ] {
                let now = tip_time
                    .checked_add(Duration32::from_seconds(offset))
                    .unwrap();
                let result = difficulty_time_and_history_tree(
                    relevant_chain.clone(),
                    tip_height,
                    block.hash(),
                    &network,
                    now,
                    Arc::new(HistoryTree::default()),
                    ValueBalance::zero(),
                )
                .expect("the template context is complete");

                assert_eq!(result.cur_time, now, "tip={tip_height:?}, offset={offset}");
                assert_eq!(
                    result.min_time,
                    if offset > gap.seconds() {
                        first_minimum_time
                    } else {
                        consensus_min
                    }
                );
                assert_eq!(
                    result.max_time,
                    if offset > gap.seconds() {
                        consensus_max
                    } else {
                        last_standard_time
                    }
                );
                assert_eq!(
                    result.expected_difficulty == limit.to_compact(),
                    offset > gap.seconds(),
                    "tip={tip_height:?}, offset={offset}"
                );

                // Miners may mutate time without changing the template's bits.
                // Validate both bounds and the clock against the shared header
                // validator, including the candidate block's upgrade spacing.
                for time in [result.min_time, result.cur_time, result.max_time] {
                    let adjustment = AdjustedDifficulty::new_from_header_time(
                        time.to_chrono(),
                        tip_height,
                        &network,
                        relevant_chain
                            .iter()
                            .map(|header| (header.difficulty_threshold, header.time)),
                    )
                    .unwrap();
                    zakura_header_chain::validate_contextual_difficulty_and_time(
                        result.expected_difficulty,
                        adjustment,
                    )
                    .expect("every advertised timestamp must match the template bits");
                }
            }
        }
    }

    /// Testnet selects the same template time as mainnet, including when the clock is clamped
    /// to the consensus range.
    #[test]
    fn mining_template_time_matches_on_mainnet_and_testnet() {
        let _init_guard = zakura_test::init();
        let block: block::Block = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
            .zcash_deserialize_into()
            .expect("the genesis vector is valid");
        let tip_time = DateTime32::from(1_700_000_000);
        let after_tip = |seconds: u32| {
            tip_time
                .checked_add(Duration32::from_seconds(seconds))
                .unwrap()
        };
        let min_time = after_tip(1);
        let max_time = after_tip(BLOCK_MAX_TIME_SINCE_MEDIAN);
        let tip_height = Height(3_000_000);
        let mut header = *block.header;
        header.time = tip_time.to_chrono();
        let header = Arc::new(header);

        for network in [Network::Mainnet, Network::new_default_testnet()] {
            let span = pow_adjustment_block_span_for_height(&network, tip_height.next().unwrap());
            for (now, expected_time) in [
                (tip_time, min_time),
                (min_time, min_time),
                // Inside the old two-spacing and one-spacing early-switch windows, and the
                // first minimum difficulty time.
                (after_tip(360), after_tip(360)),
                (after_tip(400), after_tip(400)),
                (after_tip(451), after_tip(451)),
                (max_time, max_time),
                (after_tip(BLOCK_MAX_TIME_SINCE_MEDIAN + 1), max_time),
            ] {
                let result = difficulty_time_and_history_tree(
                    vec![header.clone(); span],
                    tip_height,
                    block.hash(),
                    &network,
                    now,
                    Arc::new(HistoryTree::default()),
                    ValueBalance::zero(),
                )
                .expect("the template context is complete");

                assert_eq!(
                    result.cur_time, expected_time,
                    "network={network}, now={now:?}"
                );
                assert!(result.min_time <= result.cur_time);
                assert!(result.cur_time <= result.max_time);
                if network == Network::Mainnet {
                    assert_eq!(result.min_time, min_time);
                    assert_eq!(result.max_time, max_time);
                }
            }
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
                        DateTime32::now(),
                        Arc::new(HistoryTree::default()),
                        ValueBalance::zero(),
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
