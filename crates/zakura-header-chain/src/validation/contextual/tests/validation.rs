use std::{
    cmp::{max, min},
    hint::black_box,
    time::Instant,
};

use chrono::{DateTime, Duration, Utc};
use zakura_chain::{
    block,
    parameters::{
        testnet::{Parameters, RegtestParameters},
        Network, NetworkUpgrade, POW_AVERAGING_WINDOW,
    },
    work::difficulty::{CompactDifficulty, ExpandedDifficulty, ParameterDifficulty as _, U256},
};

use super::super::*;

fn compact_half_limit(network: &Network) -> CompactDifficulty {
    (network.target_difficulty_limit() / U256::from(2_u8)).to_compact()
}

fn context(
    network: &Network,
    candidate_time: DateTime<Utc>,
    spacing: Duration,
    len: usize,
) -> Vec<(CompactDifficulty, DateTime<Utc>)> {
    let difficulty = compact_half_limit(network);
    (1..=len)
        .map(|offset| {
            let offset = i32::try_from(offset).expect("test context length fits in i32");
            (difficulty, candidate_time - spacing * offset)
        })
        .collect()
}

fn validate_with_expected_target(
    network: &Network,
    candidate_height: block::Height,
    candidate_time: DateTime<Utc>,
    context: &[(CompactDifficulty, DateTime<Utc>)],
) -> Result<(), ContextualValidationError> {
    let previous_height = (candidate_height - 1).expect("test candidate is not genesis");
    let adjustment = match AdjustedDifficulty::new_from_header_time(
        candidate_time,
        previous_height,
        network,
        context.iter().copied(),
    ) {
        Ok(adjustment) => adjustment,
        Err(error) => panic!("the helper requires an exact height-dependent context: {error}"),
    };
    let expected = adjustment.expected_difficulty_threshold();
    validate_contextual_difficulty_and_time(expected, adjustment)
}

#[test]
fn difficulty_windows_upgrades_testnet_minimum_and_partitions_match() {
    let candidate_time =
        DateTime::from_timestamp(2_000_000_000, 0).expect("test timestamp is in range");

    for network in Network::iter() {
        for len in [1, 11, 16, 17, 27] {
            let candidate_height = block::Height(700_000);
            let spacing = NetworkUpgrade::target_spacing_for_height(&network, candidate_height);
            let context = context(&network, candidate_time, spacing, len);
            let previous_height = (candidate_height - 1).expect("height is positive");
            assert!(
                matches!(
                    AdjustedDifficulty::new_from_header_time(
                        candidate_time,
                        previous_height,
                        &network,
                        context,
                    ),
                    Err(AdjustedDifficultyError::ContextLength {
                        expected: POW_ADJUSTMENT_BLOCK_SPAN,
                        actual,
                    }) if actual == len
                ),
                "late-chain contexts must not fail open for {network:?}"
            );
        }

        let candidate_height = block::Height(700_000);
        let spacing = NetworkUpgrade::target_spacing_for_height(&network, candidate_height);
        let late_context = context(&network, candidate_time, spacing, POW_ADJUSTMENT_BLOCK_SPAN);
        let previous_height = (candidate_height - 1).expect("height is positive");
        let expected = AdjustedDifficulty::new_from_header_time(
            candidate_time,
            previous_height,
            &network,
            late_context.iter().copied(),
        )
        .expect("the late-chain context contains the complete span")
        .expected_difficulty_threshold();
        for split in 0..=late_context.len() {
            let partitioned = late_context[..split]
                .iter()
                .chain(&late_context[split..])
                .copied();
            assert_eq!(
                AdjustedDifficulty::new_from_header_time(
                    candidate_time,
                    previous_height,
                    &network,
                    partitioned,
                )
                .expect("partitioning preserves the complete context")
                .expected_difficulty_threshold(),
                expected,
                "response partitions must not affect difficulty for {network:?}, split {split}"
            );
        }

        for (height, _) in network.activation_list() {
            if height == block::Height(0) {
                continue;
            }
            let spacing = NetworkUpgrade::target_spacing_for_height(&network, height);
            let context_len = usize::try_from(height.0.min(28))
                .expect("bounded test context length fits in usize");
            let context = context(&network, candidate_time, spacing, context_len);
            validate_with_expected_target(&network, height, candidate_time, &context)
                .expect("the shared result validates at every configured upgrade boundary");
        }
    }

    let testnet = Network::new_default_testnet();
    let activation_height = block::Height(299_188);
    let spacing = NetworkUpgrade::target_spacing_for_height(&testnet, activation_height);
    let previous_time = candidate_time - spacing * 6;
    let mut exact_gap = context(&testnet, candidate_time, spacing, 28);
    exact_gap[0].1 = previous_time;
    let previous_height = (activation_height - 1).expect("height is positive");
    let exact_gap_target = AdjustedDifficulty::new_from_header_time(
        candidate_time,
        previous_height,
        &testnet,
        exact_gap,
    )
    .expect("the test supplies the complete late-chain context")
    .expected_difficulty_threshold();
    assert_ne!(
        exact_gap_target,
        testnet.target_difficulty_limit().to_compact()
    );

    let minimum_time = candidate_time + Duration::seconds(1);
    let minimum_context = context(&testnet, minimum_time, spacing, 28)
        .into_iter()
        .enumerate()
        .map(|(index, (difficulty, time))| {
            if index == 0 {
                (difficulty, previous_time)
            } else {
                (difficulty, time)
            }
        });
    assert_eq!(
        AdjustedDifficulty::new_from_header_time(
            minimum_time,
            previous_height,
            &testnet,
            minimum_context,
        )
        .expect("the test supplies the complete late-chain context")
        .expected_difficulty_threshold(),
        testnet.target_difficulty_limit().to_compact(),
        "ZIP 205/208 minimum difficulty begins strictly above six target spacings"
    );
}

#[test]
fn difficulty_damping_bounds_are_exact() {
    let network = Network::Mainnet;
    let candidate_height = block::Height(700_000);
    let previous_height = (candidate_height - 1).expect("height is positive");
    let candidate_time =
        DateTime::from_timestamp(2_000_000_000, 0).expect("test timestamp is in range");
    let averaging_timespan =
        NetworkUpgrade::averaging_window_timespan_for_height(&network, candidate_height);
    let mean_target = compact_half_limit(&network)
        .to_expanded()
        .expect("the test target is valid");

    let fast_context = context(
        &network,
        candidate_time,
        Duration::seconds(1),
        POW_ADJUSTMENT_BLOCK_SPAN,
    );
    let fast = AdjustedDifficulty::new_from_header_time(
        candidate_time,
        previous_height,
        &network,
        fast_context,
    )
    .expect("the test supplies the complete late-chain context")
    .expected_difficulty_threshold();
    let minimum_timespan = averaging_timespan * (100 - POW_MAX_ADJUST_UP_PERCENT) / 100;
    assert_eq!(
        fast,
        ((mean_target / averaging_timespan.num_seconds()) * minimum_timespan.num_seconds())
            .to_compact(),
        "fast blocks are clipped at the 16% upward-adjustment bound"
    );

    let slow_context = context(
        &network,
        candidate_time,
        Duration::seconds(10_000),
        POW_ADJUSTMENT_BLOCK_SPAN,
    );
    let slow = AdjustedDifficulty::new_from_header_time(
        candidate_time,
        previous_height,
        &network,
        slow_context,
    )
    .expect("the test supplies the complete late-chain context")
    .expected_difficulty_threshold();
    let maximum_timespan = averaging_timespan * (100 + POW_MAX_ADJUST_DOWN_PERCENT) / 100;
    assert_eq!(
        slow,
        ((mean_target / averaging_timespan.num_seconds()) * maximum_timespan.num_seconds())
            .to_compact(),
        "slow blocks are clipped at the 32% downward-adjustment bound"
    );
}

#[test]
fn median_and_production_max_time_boundaries_are_exact() {
    let base = DateTime::from_timestamp(1_600_000_000, 0).expect("test timestamp is in range");
    let difficulty = Network::Mainnet.target_difficulty_limit().to_compact();

    for len in 1..=POW_ADJUSTMENT_BLOCK_SPAN {
        let times: Vec<_> = (0..len)
            .map(|offset| base + Duration::seconds(i64::try_from(offset).expect("fits")))
            .rev()
            .collect();
        let adjustment = AdjustedDifficulty::new_from_header_time(
            base + Duration::hours(1),
            block::Height(u32::try_from(len - 1).expect("the bounded context length fits in u32")),
            &Network::Mainnet,
            times.iter().copied().map(|time| (difficulty, time)),
        )
        .expect("early-chain context length equals the candidate height");
        let mut expected: Vec<_> = times.into_iter().take(POW_MEDIAN_BLOCK_SPAN).collect();
        expected.sort_unstable();
        assert_eq!(adjustment.median_time_past(), expected[expected.len() / 2]);
    }

    let configured_testnet_height_one = Parameters::build()
        .with_max_block_time_start_height(block::Height(1))
        .to_network()
        .expect("the configured Testnet height-one policy is valid");
    let configured_regtest_height_one = Network::new_regtest(RegtestParameters {
        max_block_time_start_height: Some(block::Height(1)),
        ..Default::default()
    });

    for (network, height, max_is_active) in [
        (Network::Mainnet, block::Height(1), false),
        (Network::Mainnet, block::Height(2), true),
        (
            Network::new_default_testnet(),
            block::Height(653_605),
            false,
        ),
        (Network::new_default_testnet(), block::Height(653_606), true),
        (configured_testnet_height_one, block::Height(1), true),
        (configured_regtest_height_one, block::Height(1), true),
    ] {
        let context = vec![
            (network.target_difficulty_limit().to_compact(), base);
            usize::try_from(height.0.min(28)).expect("bounded height fits in usize")
        ];
        assert!(matches!(
            validate_with_expected_target(&network, height, base, &context),
            Err(ContextualValidationError::TimeTooEarly { .. })
        ));

        let equality = base + Duration::minutes(90);
        validate_with_expected_target(&network, height, equality, &context)
            .expect("the 90-minute equality boundary is inclusive");

        let one_second_above = equality + Duration::seconds(1);
        let result = validate_with_expected_target(&network, height, one_second_above, &context);
        assert_eq!(
            matches!(result, Err(ContextualValidationError::TimeTooLate { .. })),
            max_is_active,
            "unexpected max-time activation for {network:?} at {height:?}"
        );
    }
}

#[test]
fn disabled_pow_never_waives_median_time() {
    let network = Network::new_regtest(RegtestParameters::default());
    assert!(network.disable_pow());
    let time = DateTime::from_timestamp(1_700_000_000, 0).expect("test timestamp is in range");
    let context = [(network.target_difficulty_limit().to_compact(), time)];
    let adjustment =
        AdjustedDifficulty::new_from_header_time(time, block::Height(0), &network, context)
            .expect("height one requires exactly one predecessor");
    assert!(matches!(
        validate_contextual_difficulty_and_time(
            network.target_difficulty_limit().to_compact(),
            adjustment,
        ),
        Err(ContextualValidationError::TimeTooEarly { .. })
    ));
}

#[test]
fn custom_mtp_and_max_time_use_local_parameters_with_pow_on_or_off() {
    let base = DateTime::from_timestamp(1_700_000_000, 0).expect("test timestamp is in range");
    let activation = block::Height(10);

    for disable_pow in [false, true] {
        let network = Parameters::build()
            .with_network_name(if disable_pow {
                "CustomTimePowOff"
            } else {
                "CustomTimePowOn"
            })
            .expect("the custom network name is valid")
            .with_disable_pow(disable_pow)
            .with_max_block_time_start_height(activation)
            .to_network()
            .expect("the custom-network parameters are valid");
        let context_before = vec![(network.target_difficulty_limit().to_compact(), base); 9];
        let context_at = vec![(network.target_difficulty_limit().to_compact(), base); 10];

        assert!(matches!(
            validate_with_expected_target(&network, block::Height(9), base, &context_before),
            Err(ContextualValidationError::TimeTooEarly { .. })
        ));
        assert!(
            validate_with_expected_target(
                &network,
                block::Height(9),
                base + Duration::minutes(90) + Duration::seconds(1),
                &context_before,
            )
            .is_ok(),
            "the local maximum-time rule is not active before its configured height"
        );
        assert!(matches!(
            validate_with_expected_target(
                &network,
                activation,
                base + Duration::minutes(90) + Duration::seconds(1),
                &context_at,
            ),
            Err(ContextualValidationError::TimeTooLate { .. })
        ));
    }

    let regtest = Network::new_regtest(RegtestParameters::default());
    assert!(
        !regtest.is_max_block_time_enforced(block::Height(1))
            && regtest.is_max_block_time_enforced(block::Height(2)),
        "Regtest must use its local policy rather than public Testnet height 653,606"
    );
}

#[allow(clippy::unwrap_in_result)]
fn validate_contextual_sequence(
    network: &Network,
    mut parent_height: block::Height,
    predecessors: &[(CompactDifficulty, DateTime<Utc>)],
    candidates: &[(CompactDifficulty, DateTime<Utc>)],
) -> Result<(), ContextualValidationError> {
    let mut context = predecessors.to_vec();
    for (difficulty, time) in candidates {
        validate_contextual_difficulty_and_time(
            *difficulty,
            AdjustedDifficulty::new_from_header_time(
                *time,
                parent_height,
                network,
                context.iter().copied(),
            )
            .expect("the benchmark retains the complete late-chain context"),
        )?;
        parent_height = parent_height
            .next()
            .expect("the benchmark range is far below the height limit");
        context.insert(0, (*difficulty, *time));
        context.truncate(POW_ADJUSTMENT_BLOCK_SPAN);
    }
    Ok(())
}

#[test]
fn disabled_pow_low_target_window_does_not_calculate_expected_difficulty() {
    let network = Network::new_regtest(RegtestParameters::default());
    let base = DateTime::from_timestamp(1_700_000_000, 0).expect("test timestamp is in range");
    let limit = network.target_difficulty_limit().to_compact();
    let low_target = ExpandedDifficulty::from(U256::one()).to_compact();
    let candidates = (1..=18)
        .map(|offset| (low_target, base + Duration::seconds(offset)))
        .collect::<Vec<_>>();

    validate_contextual_sequence(&network, block::Height(0), &[(limit, base)], &candidates)
        .expect("disabled PoW accepts valid targets without calculating an unenforced threshold");

    let poisoned_context = candidates[..17]
        .iter()
        .rev()
        .copied()
        .chain([(limit, base)])
        .collect::<Vec<_>>();
    let invalid_target = CompactDifficulty::from_le_bytes([0; 4]);
    let result = validate_contextual_difficulty_and_time(
        invalid_target,
        AdjustedDifficulty::new_from_header_time(
            base + Duration::seconds(18),
            block::Height(17),
            &network,
            poisoned_context,
        )
        .expect("height 18 has complete predecessor context"),
    );
    assert!(matches!(
        result,
        Err(ContextualValidationError::InvalidDifficultyThreshold {
            difficulty_threshold,
            expected_difficulty,
        }) if difficulty_threshold == invalid_target && expected_difficulty == limit
    ));
}

#[derive(Clone, Copy, Debug)]
enum SimulationTargetSamples {
    Constant,
    Alternating,
}

#[derive(Clone, Copy, Debug)]
enum SimulationTimeMutation {
    None,
    OneExtreme,
    SixRecent,
    ParentGap(i64),
}

#[derive(Clone, Copy, Debug)]
enum SimulationNetwork {
    Mainnet,
    Testnet,
}

impl SimulationNetwork {
    fn network(self) -> Network {
        match self {
            Self::Mainnet => Network::Mainnet,
            Self::Testnet => Network::new_default_testnet(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct DifficultySimulationCase {
    name: &'static str,
    network: SimulationNetwork,
    averaging_window: usize,
    median_span: usize,
    target_spacing_seconds: i64,
    observed_spacing_seconds: i64,
    target_samples: SimulationTargetSamples,
    time_mutation: SimulationTimeMutation,
    expected_actual_timespan_seconds: i64,
    expected_minimum_difficulty: bool,
    compare_with_production: bool,
}

#[derive(Clone, Copy, Debug)]
struct DifficultySimulationResult {
    mean_target: ExpandedDifficulty,
    actual_timespan: Duration,
    damped_timespan: Duration,
    bounded_timespan: Duration,
    expanded_target: ExpandedDifficulty,
    expected_nbits: CompactDifficulty,
    minimum_difficulty: bool,
}

fn simulation_context(
    case: DifficultySimulationCase,
    network: &Network,
    candidate_time: DateTime<Utc>,
) -> Vec<(CompactDifficulty, DateTime<Utc>)> {
    let context_len = case
        .averaging_window
        .checked_add(case.median_span)
        .expect("simulation windows are small");
    let easy_target = compact_half_limit(network);
    let harder_target = (network.target_difficulty_limit() / U256::from(4_u8)).to_compact();
    let observed_spacing = Duration::seconds(case.observed_spacing_seconds);
    let mut context = (1..=context_len)
        .map(|offset| {
            let offset = i32::try_from(offset).expect("simulation context length fits in i32");
            let target = match case.target_samples {
                SimulationTargetSamples::Constant => easy_target,
                SimulationTargetSamples::Alternating if offset % 2 == 0 => harder_target,
                SimulationTargetSamples::Alternating => easy_target,
            };
            (target, candidate_time - observed_spacing * offset)
        })
        .collect::<Vec<_>>();

    match case.time_mutation {
        SimulationTimeMutation::None => {}
        SimulationTimeMutation::OneExtreme => {
            context[0].1 = candidate_time + Duration::hours(24);
        }
        SimulationTimeMutation::SixRecent => {
            for (_, time) in context.iter_mut().take(6) {
                *time = candidate_time - Duration::seconds(1);
            }
        }
        SimulationTimeMutation::ParentGap(seconds) => {
            context[0].1 = candidate_time - Duration::seconds(seconds);
        }
    }

    context
}

fn simulation_median(mut times: Vec<DateTime<Utc>>) -> DateTime<Utc> {
    times.sort_unstable();
    times[times.len() / 2]
}

fn simulate_difficulty(
    case: DifficultySimulationCase,
    network: &Network,
    candidate_height: block::Height,
    candidate_time: DateTime<Utc>,
    context: &[(CompactDifficulty, DateTime<Utc>)],
) -> DifficultySimulationResult {
    let expected_context_len = case
        .averaging_window
        .checked_add(case.median_span)
        .expect("simulation windows are small");
    assert_eq!(
        context.len(),
        expected_context_len,
        "the simulation requires one complete target and median window"
    );

    let divisor: U256 = case.averaging_window.into();
    let mut quotient_total = U256::zero();
    let mut remainder_total = U256::zero();
    for (compact, _) in &context[..case.averaging_window] {
        let target: U256 = compact
            .to_expanded()
            .expect("simulation targets are valid")
            .into();
        quotient_total = quotient_total
            .checked_add(target / divisor)
            .expect("divided simulation targets fit in U256");
        remainder_total = remainder_total
            .checked_add(target % divisor)
            .expect("simulation target remainders fit in U256");
    }
    let mean_target = ExpandedDifficulty::from(
        quotient_total
            .checked_add(remainder_total / divisor)
            .expect("the exact simulation target mean fits in U256"),
    );

    let newer_median = simulation_median(
        context[..case.median_span]
            .iter()
            .map(|(_, time)| *time)
            .collect(),
    );
    let older_median = simulation_median(
        context[case.averaging_window..expected_context_len]
            .iter()
            .map(|(_, time)| *time)
            .collect(),
    );
    let actual_timespan = newer_median - older_median;

    let averaging_window =
        i32::try_from(case.averaging_window).expect("simulation averaging window fits in i32");
    let expected_timespan = Duration::seconds(case.target_spacing_seconds) * averaging_window;
    let damped_variance = (actual_timespan - expected_timespan) / POW_DAMPING_FACTOR;
    let damped_timespan = expected_timespan + Duration::seconds(damped_variance.num_seconds());
    let minimum_timespan = expected_timespan * (100 - POW_MAX_ADJUST_UP_PERCENT) / 100;
    let maximum_timespan = expected_timespan * (100 + POW_MAX_ADJUST_DOWN_PERCENT) / 100;
    let bounded_timespan = max(minimum_timespan, min(maximum_timespan, damped_timespan));

    let target_limit = network.target_difficulty_limit();
    let bounded_seconds = bounded_timespan.num_seconds();
    let scaled_mean = mean_target / expected_timespan.num_seconds();
    let adjusted_target = if scaled_mean > target_limit / bounded_seconds {
        target_limit
    } else {
        min(target_limit, scaled_mean * bounded_seconds)
    };
    let minimum_difficulty = NetworkUpgrade::is_testnet_min_difficulty_block(
        network,
        candidate_height,
        candidate_time,
        context[0].1,
    );
    let expanded_target = if minimum_difficulty {
        target_limit
    } else {
        adjusted_target
    };

    DifficultySimulationResult {
        mean_target,
        actual_timespan,
        damped_timespan,
        bounded_timespan,
        expanded_target,
        expected_nbits: expanded_target.to_compact(),
        minimum_difficulty,
    }
}

/// Run with:
/// `cargo test -p zakura-header-chain -- --ignored --nocapture table_driven_difficulty_simulator`
#[test]
#[ignore = "manual table-driven exploration of difficulty adjustment scenarios"]
#[allow(clippy::print_stdout)]
fn table_driven_difficulty_simulator() {
    const POST_BLOSSOM_SPACING_SECONDS: i64 = 75;

    let cases = [
        DifficultySimulationCase {
            name: "regular 75-second blocks",
            network: SimulationNetwork::Mainnet,
            averaging_window: POW_AVERAGING_WINDOW,
            median_span: POW_MEDIAN_BLOCK_SPAN,
            target_spacing_seconds: POST_BLOSSOM_SPACING_SECONDS,
            observed_spacing_seconds: 75,
            target_samples: SimulationTargetSamples::Constant,
            time_mutation: SimulationTimeMutation::None,
            expected_actual_timespan_seconds: 1_275,
            expected_minimum_difficulty: false,
            compare_with_production: true,
        },
        DifficultySimulationCase {
            name: "fast 50-second blocks",
            network: SimulationNetwork::Mainnet,
            averaging_window: POW_AVERAGING_WINDOW,
            median_span: POW_MEDIAN_BLOCK_SPAN,
            target_spacing_seconds: POST_BLOSSOM_SPACING_SECONDS,
            observed_spacing_seconds: 50,
            target_samples: SimulationTargetSamples::Constant,
            time_mutation: SimulationTimeMutation::None,
            expected_actual_timespan_seconds: 850,
            expected_minimum_difficulty: false,
            compare_with_production: true,
        },
        DifficultySimulationCase {
            name: "slow 100-second blocks",
            network: SimulationNetwork::Mainnet,
            averaging_window: POW_AVERAGING_WINDOW,
            median_span: POW_MEDIAN_BLOCK_SPAN,
            target_spacing_seconds: POST_BLOSSOM_SPACING_SECONDS,
            observed_spacing_seconds: 100,
            target_samples: SimulationTargetSamples::Constant,
            time_mutation: SimulationTimeMutation::None,
            expected_actual_timespan_seconds: 1_700,
            expected_minimum_difficulty: false,
            compare_with_production: true,
        },
        DifficultySimulationCase {
            name: "one extreme timestamp",
            network: SimulationNetwork::Mainnet,
            averaging_window: POW_AVERAGING_WINDOW,
            median_span: POW_MEDIAN_BLOCK_SPAN,
            target_spacing_seconds: POST_BLOSSOM_SPACING_SECONDS,
            observed_spacing_seconds: 75,
            target_samples: SimulationTargetSamples::Constant,
            time_mutation: SimulationTimeMutation::OneExtreme,
            expected_actual_timespan_seconds: 1_275,
            expected_minimum_difficulty: false,
            compare_with_production: true,
        },
        DifficultySimulationCase {
            name: "six recent timestamps moved",
            network: SimulationNetwork::Mainnet,
            averaging_window: POW_AVERAGING_WINDOW,
            median_span: POW_MEDIAN_BLOCK_SPAN,
            target_spacing_seconds: POST_BLOSSOM_SPACING_SECONDS,
            observed_spacing_seconds: 75,
            target_samples: SimulationTargetSamples::Constant,
            time_mutation: SimulationTimeMutation::SixRecent,
            expected_actual_timespan_seconds: 1_724,
            expected_minimum_difficulty: false,
            compare_with_production: true,
        },
        DifficultySimulationCase {
            name: "sudden hash-rate doubling",
            network: SimulationNetwork::Mainnet,
            averaging_window: POW_AVERAGING_WINDOW,
            median_span: POW_MEDIAN_BLOCK_SPAN,
            target_spacing_seconds: POST_BLOSSOM_SPACING_SECONDS,
            observed_spacing_seconds: 38,
            target_samples: SimulationTargetSamples::Constant,
            time_mutation: SimulationTimeMutation::None,
            expected_actual_timespan_seconds: 646,
            expected_minimum_difficulty: false,
            compare_with_production: true,
        },
        DifficultySimulationCase {
            name: "alternating target samples",
            network: SimulationNetwork::Mainnet,
            averaging_window: POW_AVERAGING_WINDOW,
            median_span: POW_MEDIAN_BLOCK_SPAN,
            target_spacing_seconds: POST_BLOSSOM_SPACING_SECONDS,
            observed_spacing_seconds: 75,
            target_samples: SimulationTargetSamples::Alternating,
            time_mutation: SimulationTimeMutation::None,
            expected_actual_timespan_seconds: 1_275,
            expected_minimum_difficulty: false,
            compare_with_production: true,
        },
        DifficultySimulationCase {
            name: "Testnet gap exactly six spacings",
            network: SimulationNetwork::Testnet,
            averaging_window: POW_AVERAGING_WINDOW,
            median_span: POW_MEDIAN_BLOCK_SPAN,
            target_spacing_seconds: POST_BLOSSOM_SPACING_SECONDS,
            observed_spacing_seconds: 75,
            target_samples: SimulationTargetSamples::Constant,
            time_mutation: SimulationTimeMutation::ParentGap(450),
            expected_actual_timespan_seconds: 1_275,
            expected_minimum_difficulty: false,
            compare_with_production: true,
        },
        DifficultySimulationCase {
            name: "Testnet gap six spacings plus one",
            network: SimulationNetwork::Testnet,
            averaging_window: POW_AVERAGING_WINDOW,
            median_span: POW_MEDIAN_BLOCK_SPAN,
            target_spacing_seconds: POST_BLOSSOM_SPACING_SECONDS,
            observed_spacing_seconds: 75,
            target_samples: SimulationTargetSamples::Constant,
            time_mutation: SimulationTimeMutation::ParentGap(451),
            expected_actual_timespan_seconds: 1_274,
            expected_minimum_difficulty: true,
            compare_with_production: true,
        },
        DifficultySimulationCase {
            name: "experimental 34-target window",
            network: SimulationNetwork::Mainnet,
            averaging_window: 34,
            median_span: POW_MEDIAN_BLOCK_SPAN,
            target_spacing_seconds: POST_BLOSSOM_SPACING_SECONDS,
            observed_spacing_seconds: 75,
            target_samples: SimulationTargetSamples::Constant,
            time_mutation: SimulationTimeMutation::None,
            expected_actual_timespan_seconds: 2_550,
            expected_minimum_difficulty: false,
            compare_with_production: false,
        },
    ];

    let candidate_height = block::Height(700_000);
    let previous_height = (candidate_height - 1).expect("simulation height is positive");
    let candidate_time =
        DateTime::from_timestamp(2_000_000_000, 0).expect("simulation timestamp is in range");

    println!(
        "{:<40} {:>6} {:>8} {:>8} {:>8} {:>12} {:>8}",
        "case", "window", "actual", "damped", "bounded", "nBits", "min-diff"
    );
    for case in cases {
        let network = case.network.network();
        let context = simulation_context(case, &network, candidate_time);
        let result =
            simulate_difficulty(case, &network, candidate_height, candidate_time, &context);

        assert_eq!(
            result.actual_timespan.num_seconds(),
            case.expected_actual_timespan_seconds,
            "{} has an unexpected median timespan",
            case.name
        );
        assert_eq!(
            result.minimum_difficulty, case.expected_minimum_difficulty,
            "{} has an unexpected Testnet minimum-difficulty classification",
            case.name
        );

        if case.compare_with_production {
            assert_eq!(
                case.averaging_window, POW_AVERAGING_WINDOW,
                "production comparisons require the active averaging window"
            );
            assert_eq!(
                case.median_span, POW_MEDIAN_BLOCK_SPAN,
                "production comparisons require the active median span"
            );
            let production = AdjustedDifficulty::new_from_header_time(
                candidate_time,
                previous_height,
                &network,
                context.iter().copied(),
            )
            .expect("the production comparison supplies complete context")
            .expected_difficulty_threshold();
            assert_eq!(
                result.expected_nbits, production,
                "{} differs from the production difficulty calculation",
                case.name
            );
        }

        let nbits = u32::from_le_bytes(result.expected_nbits.to_le_bytes());
        println!(
            "{:<40} {:>6} {:>7}s {:>7}s {:>7}s {:#012x} {:>8}",
            case.name,
            case.averaging_window,
            result.actual_timespan.num_seconds(),
            result.damped_timespan.num_seconds(),
            result.bounded_timespan.num_seconds(),
            nbits,
            result.minimum_difficulty,
        );
        println!(
            "    mean_target={:?}\n    expanded_target={:?}",
            result.mean_target, result.expanded_target
        );
    }
}

/// Run with:
/// `cargo test -p zakura-header-chain --release -- --ignored --nocapture contextual_writer_hold_microbench`
#[test]
#[ignore = "manual release-mode benchmark for the R5 writer-boundary gate"]
#[allow(clippy::print_stdout)]
fn contextual_writer_hold_microbench() {
    const TYPICAL_HEADERS: usize = 32;
    const MAX_HEADERS: usize = 4_096;
    const TYPICAL_ITERATIONS: u32 = 2_000;
    const MAX_ITERATIONS: u32 = 20;
    const MAX_ACCEPTABLE_WRITER_HOLD: std::time::Duration = std::time::Duration::from_millis(25);

    let network = Network::new_regtest(RegtestParameters::default());
    let parent_height = block::Height(700_000);
    let base = DateTime::from_timestamp(2_000_000_000, 0).expect("benchmark timestamp is in range");
    let spacing = Duration::seconds(75);
    let difficulty = network.target_difficulty_limit().to_compact();
    let predecessors = context(&network, base, spacing, POW_ADJUSTMENT_BLOCK_SPAN);
    let candidates = |count: usize| {
        (1..=count)
            .map(|offset| {
                let offset =
                    i32::try_from(offset).expect("the maximum batch length fits in an i32");
                (difficulty, base + spacing * offset)
            })
            .collect::<Vec<_>>()
    };
    let typical = candidates(TYPICAL_HEADERS);
    let maximum = candidates(MAX_HEADERS);

    println!(
        "R5 contextual writer-hold gate: max 4,096-header average < {:?}",
        MAX_ACCEPTABLE_WRITER_HOLD
    );
    for mode in ["integrated", "headers-only"] {
        for (case, batch, iterations) in [
            ("typical-pass", typical.as_slice(), TYPICAL_ITERATIONS),
            ("maximum-pass", maximum.as_slice(), MAX_ITERATIONS),
        ] {
            let started = Instant::now();
            for _ in 0..iterations {
                validate_contextual_sequence(
                    &network,
                    parent_height,
                    &predecessors,
                    black_box(batch),
                )
                .expect("the benchmark sequence is contextually valid");
            }
            let average = started.elapsed() / iterations;
            println!(
                "mode={mode} case={case} headers={} average={average:?} per_header={:?}",
                batch.len(),
                average / u32::try_from(batch.len()).expect("benchmark length fits in u32"),
            );
            if case == "maximum-pass" {
                assert!(
                    average < MAX_ACCEPTABLE_WRITER_HOLD,
                    "{mode} maximum batch held the prototype writer gate for {average:?}"
                );
            }
        }

        for (case, invalid_offset) in [("invalid-first", 0), ("invalid-last", MAX_HEADERS - 1)] {
            let mut invalid = maximum.clone();
            invalid[invalid_offset].1 = predecessors[5].1;
            let started = Instant::now();
            let result = validate_contextual_sequence(
                &network,
                parent_height,
                &predecessors,
                black_box(&invalid),
            );
            let elapsed = started.elapsed();
            assert!(result.is_err(), "{case} must exercise contextual rejection");
            println!(
                "mode={mode} case={case} headers_examined={} elapsed={elapsed:?}",
                invalid_offset + 1,
            );
        }
    }
}
