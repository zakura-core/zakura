//! Discovery proptest: random op sequences over the fixed universe, shrunk on
//! any audit failure to a minimal, transcribable counterexample.
//!
//! The sweep runs 64 cases in CI by default; widen it manually with:
//!
//! ```text
//! PROPTEST_CASES=4096 cargo test -p zakura-state --lib \
//!     header_store_coherence::prop -- --nocapture
//! ```
//!
//! Every shrunk counterexample must be transcribed into `scenarios.rs` as a
//! hardcoded scenario (the primary, seed-independent pinning mechanism); the
//! proptest regression file under `zakura-state/proptest-regressions/` pins
//! the seed as a backstop.

use std::env;

use proptest::prelude::*;

use super::{
    fabricate::TRUNK_LEN,
    ops::{universe, Anchor, Harness, Op, Source},
};

fn source_strategy() -> impl Strategy<Value = Source> {
    let branch_count = universe().branches.len();
    prop_oneof![
        1 => Just(Source::Trunk),
        4 => (0..branch_count).prop_map(Source::Branch),
    ]
}

fn anchor_strategy() -> impl Strategy<Value = Anchor> {
    let branch_count = universe().branches.len();
    prop_oneof![
        // Mostly the natural anchor, so sequences build interesting states.
        9 => Just(Anchor::Auto),
        // Sometimes adversarial: cross-chain and stale anchors.
        1 => prop_oneof![
            (1..=TRUNK_LEN as u32).prop_map(Anchor::TrunkAt),
            (0..branch_count, 0..32usize).prop_map(|(branch, index)| Anchor::BranchAt(branch, index)),
            Just(Anchor::Genesis),
        ],
    ]
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        9 => (source_strategy(), 0..TRUNK_LEN, 1..=12usize, anchor_strategy()).prop_map(
            |(source, offset, len, anchor)| Op::CommitHeaderRange {
                source,
                offset,
                len,
                anchor,
            }
        ),
        2 => (source_strategy(), 0..40usize)
            .prop_map(|(source, index)| Op::CommitBody { source, index }),
        2 => (source_strategy(), 0..40usize).prop_map(|(source, index)| Op::Seed { source, index }),
        3 => (1..8usize).prop_map(|count| Op::Finalize { count }),
        1 => Just(Op::Reopen),
    ]
}

fn ops_strategy() -> impl Strategy<Value = Vec<Op>> {
    proptest::collection::vec(op_strategy(), 1..40)
}

fn proptest_cases() -> u32 {
    env::var("PROPTEST_CASES")
        .ok()
        .and_then(|cases| cases.parse().ok())
        .unwrap_or(64)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(proptest_cases()))]

    /// The permanent invariant sweep: any failure is a store-corruption bug.
    #[test]
    fn prop_random_sequences_uphold_invariants(ops in ops_strategy()) {
        let _init_guard = zakura_test::init();
        let mut harness = Harness::new();
        if let Err(report) = harness.run_all(&ops) {
            prop_assert!(false, "store invariants violated:\n{report:#?}");
        }
    }
}
