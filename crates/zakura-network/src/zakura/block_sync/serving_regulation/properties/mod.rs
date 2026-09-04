//! GetBlocks ownership histories with an independent model and concrete JSON replay.

mod model;
mod negative_controls;
mod production;
mod scenario;
mod witnesses;

use std::{sync::Arc, time::Duration};

use proptest::prelude::*;
use zakura_chain::{
    block::Block,
    serialization::{ZcashDeserializeInto, ZcashSerialize},
};

use model::Model;
use production::Production;
use scenario::*;

fn fixture() -> Arc<Block> {
    zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .unwrap()
}

fn block_payload_bytes() -> u64 {
    // Independent of BlockSyncMessage's frame encoder: one discriminator plus
    // the canonical block bytes from the committed fixture.
    u64::try_from(fixture().zcash_serialize_to_vec().unwrap().len()).unwrap() + 1
}

/// Replay requires concrete applicable actions; it never reconstructs choices.
fn replay(scenario: &Scenario) -> Result<Vec<Observation>, String> {
    if scenario.version != 1 {
        return Err(format!("unsupported scenario version {}", scenario.version));
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .map_err(|error| format!("create replay runtime: {error}"))?;
    runtime.block_on(replay_actions(scenario))
}

async fn replay_actions(scenario: &Scenario) -> Result<Vec<Observation>, String> {
    let mut model = Model::new(scenario.limit, block_payload_bytes());
    let mut production = Production::new(scenario.limit, fixture());
    let mut observations = Vec::new();
    for (step, action) in scenario.actions.iter().enumerate() {
        if !model.is_enabled(action) {
            return Err(format!("step {step}: invalid action {action:?}"));
        }
        let before = tokio::time::Instant::now();
        let expected_outcome = model.apply(action);
        let actual_outcome = if let Action::Advance { millis } = action {
            tokio::time::advance(Duration::from_millis(*millis)).await;
            Outcome::Done
        } else {
            production.apply(action)
        };
        let elapsed = if let Action::Advance { millis } = action {
            Duration::from_millis(*millis)
        } else {
            Duration::ZERO
        };
        assert_eq!(
            tokio::time::Instant::now() - before,
            elapsed,
            "only scenario actions may advance time"
        );
        let expected = model.snapshot();
        let actual = production.snapshot();
        if actual_outcome != expected_outcome || actual != expected {
            return Err(format!("step {step}, {action:?}\nexpected {expected_outcome:?} {expected:?}\nobserved {actual_outcome:?} {actual:?}"));
        }
        observations.push(Observation {
            outcome: actual_outcome,
            resources: actual,
        });
    }
    Ok(observations)
}

fn materialize(limit: Limit, choices: &[usize]) -> Scenario {
    let mut model = Model::new(limit, block_payload_bytes());
    let mut actions = Vec::new();
    for choice in choices {
        let enabled = model.actions();
        let action = enabled[choice % enabled.len()].clone();
        model.apply(&action);
        actions.push(action);
    }
    actions.extend(model.cleanup());
    Scenario {
        version: 1,
        limit,
        actions,
    }
}

fn checked_replay(scenario: &Scenario) -> Result<(), String> {
    let json = serde_json::to_string_pretty(scenario)
        .map_err(|error| format!("serialize replay: {error}"))?;
    let restored =
        serde_json::from_str(&json).map_err(|error| format!("deserialize replay: {error}"))?;
    let first = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| replay(scenario)))
        .map_err(|_| format!("replay panicked\nReplay scenario:\n{json}"))?
        .map_err(|error| format!("{error}\nReplay scenario:\n{json}"))?;
    let second = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| replay(&restored)))
        .map_err(|_| format!("replay panicked\nReplay scenario:\n{json}"))?
        .map_err(|error| format!("{error}\nReplay scenario:\n{json}"))?;
    if first != second {
        return Err(format!("nondeterministic replay:\n{json}"));
    }
    if let Some(observation) = first.last() {
        let state = &observation.resources;
        if state.node_bytes != 0 || state.node_active != 0 || state.node_pending != 0 {
            return Err(format!("unfinished ownership:\n{json}"));
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig { max_shrink_iters: 4096, ..ProptestConfig::default() })]
    #[test]
    fn serving_histories_match_independent_ownership(
        limit in 0usize..Limit::ALL.len(),
        choices in prop::collection::vec(0usize..4096, 1..64),
    ) {
        let scenario = materialize(Limit::ALL[limit], &choices);
        let result = checked_replay(&scenario);
        prop_assert!(result.is_ok(), "{}", result.unwrap_err());
    }
}
