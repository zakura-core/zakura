//! Run an intentionally broken writer through the same ownership comparator.

use proptest::test_runner::{Config, RngAlgorithm, TestCaseError, TestError, TestRng, TestRunner};

use super::*;

#[test]
fn missing_write_ownership_reduces_to_a_concrete_replay() {
    let fixture: Scenario =
        serde_json::from_str(include_str!("writing_after_reconnect.json")).unwrap();
    checked_replay(&fixture).unwrap();
    let mut runner = TestRunner::new_with_rng(
        Config {
            cases: 1,
            failure_persistence: None,
            max_shrink_iters: 256,
            ..Config::default()
        },
        TestRng::deterministic_rng(RngAlgorithm::ChaCha),
    );
    let failure = runner.run(&prop::collection::vec(0u64..100, 0..16), |advances| {
        let mut scenario = fixture.clone();
        let mut actions: Vec<_> = advances
            .iter()
            .map(|millis| Action::Advance { millis: *millis })
            .collect();
        actions.extend(scenario.actions);
        scenario.actions = actions;
        // The mutation drops the actual frame guard before a controlled pending
        // write. Expected observations and production capacity limits stay intact.
        replay_with_writer(&scenario, true).map_err(TestCaseError::fail)?;
        Ok(())
    });
    let Err(TestError::Fail(reason, minimized)) = failure else {
        panic!("the writer that releases capacity too early must fail")
    };
    assert!(reason.to_string().contains("expected"));
    assert!(reason.to_string().contains("observed"));
    assert!(minimized.is_empty(), "irrelevant time advances shrink away");
    assert!(replay_with_writer(&fixture, true).is_err());
}
