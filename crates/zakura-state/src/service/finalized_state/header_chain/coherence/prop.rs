//! Random operation sequences over the fixed coherence universe.

use std::env;

use proptest::prelude::*;

use super::{
    fabricate::TRUNK_LEN,
    harness::{Anchor, Harness, Op, Source},
};

fn source_strategy() -> impl Strategy<Value = Source> {
    prop_oneof![
        1 => Just(Source::Trunk),
        4 => (0..4usize).prop_map(Source::Branch),
    ]
}

fn anchor_strategy() -> impl Strategy<Value = Anchor> {
    prop_oneof![
        9 => Just(Anchor::Natural),
        1 => Just(Anchor::Genesis),
        1 => (1..=u32::try_from(TRUNK_LEN).expect("the trunk length fits in u32"))
            .prop_map(Anchor::TrunkAt),
    ]
}

fn operation_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        15 => (
            source_strategy(),
            0..TRUNK_LEN,
            1..=12usize,
            anchor_strategy(),
        )
            .prop_map(|(source, offset, len, anchor)| Op::InsertHeaders {
                source,
                offset,
                len,
                anchor,
            }),
        3 => (source_strategy(), 0..TRUNK_LEN)
            .prop_map(|(source, index)| Op::Verify { source, index }),
        2 => (1..=8usize).prop_map(|count| Op::Finalize { count }),
        1 => (source_strategy(), 0..TRUNK_LEN)
            .prop_map(|(source, index)| Op::MalformedVerify { source, index }),
        1 => Just(Op::Reopen),
    ]
}

fn operation_sequence_strategy() -> impl Strategy<Value = Vec<Op>> {
    proptest::collection::vec(operation_strategy(), 1..40)
}

/// `PROPTEST_CASES`, or `default` when it is unset.
fn proptest_cases(default: u32) -> u32 {
    env::var("PROPTEST_CASES")
        .ok()
        .and_then(|cases| cases.parse().ok())
        .unwrap_or(default)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(proptest_cases(8)))]

    /// Eight sequences, on every pull request.
    #[test]
    fn random_fork_aware_writer_sequences_uphold_store_invariants(
        operations in operation_sequence_strategy(),
    ) {
        let _init_guard = zakura_test::init();
        Harness::new().run_all(&operations);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(proptest_cases(64)))]

    /// Sixty-four sequences, in full-coverage CI runs.
    #[test]
    #[ignore = "broad property run; full-coverage CI passes --run-ignored"]
    fn many_random_fork_aware_writer_sequences_uphold_store_invariants(
        operations in operation_sequence_strategy(),
    ) {
        let _init_guard = zakura_test::init();
        Harness::new().run_all(&operations);
    }
}
