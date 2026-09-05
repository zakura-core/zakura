//! Checker sensitivity uses faulty observations, never weakened production limits.

use proptest::test_runner::{Config, RngAlgorithm, TestCaseError, TestError, TestRng, TestRunner};

use super::*;

#[test]
fn missing_write_ownership_reduces_to_a_concrete_replay() {
    let fixture: Scenario =
        serde_json::from_str(include_str!("writing_after_reconnect.json")).unwrap();
    let mut runner = TestRunner::new_with_rng(
        Config {
            cases: 1,
            failure_persistence: None,
            max_shrink_iters: 256,
            ..Config::default()
        },
        TestRng::deterministic_rng(RngAlgorithm::ChaCha),
    );
    let choices = prop::collection::vec(0u64..100, 0..16);
    let failure = runner.run(&choices, |advances| {
        let mut scenario = fixture.clone();
        let mut actions: Vec<_> = advances
            .iter()
            .map(|millis| Action::Advance { millis: *millis })
            .collect();
        actions.extend(scenario.actions);
        scenario.actions = actions;
        let observations = replay(&scenario).map_err(TestCaseError::fail)?;
        let expected = resources_after_reconnect(&scenario, &observations);
        let mut missing_write = expected.clone();
        missing_write.node_bytes = 0;
        prop_assert_ne!(expected.node_bytes, 0);
        // This is the same structural equality used by the model comparison.
        // The deliberately incomplete ledger must be rejected even after all
        // unrelated time advances have been shrunk away.
        prop_assert_eq!(missing_write, expected.clone(), "missing write ownership");
        Ok(())
    });
    let Err(TestError::Fail(reason, minimized)) = failure else {
        panic!("the faulty observation must fail")
    };
    assert!(reason.to_string().contains("missing write ownership"));
    assert!(minimized.is_empty());
    checked_replay(&fixture).unwrap();
}

#[test]
fn observation_comparison_rejects_compensating_and_wrong_session_errors() {
    let scenario: Scenario =
        serde_json::from_str(include_str!("writing_after_reconnect.json")).unwrap();
    let observations = replay(&scenario).unwrap();
    let expected = resources_after_reconnect(&scenario, &observations);
    let mut wrong_session = expected.clone();
    wrong_session.session_bytes.swap(0, 2);
    assert_eq!(wrong_session.node_bytes, expected.node_bytes);
    assert_ne!(
        wrong_session, *expected,
        "a correct aggregate cannot hide wrong ownership"
    );
    let mut duplicate_refund = expected.clone();
    duplicate_refund.node_rate += 1;
    assert_ne!(
        duplicate_refund, *expected,
        "a one-unit duplicate refund must be visible"
    );
}

fn resources_after_reconnect<'a>(
    scenario: &Scenario,
    observations: &'a [Observation],
) -> &'a Snapshot {
    let step = scenario
        .actions
        .iter()
        .position(|action| matches!(action, Action::Reconnect { .. }))
        .expect("the witness includes a reconnect while writing");
    &observations[step].resources
}
