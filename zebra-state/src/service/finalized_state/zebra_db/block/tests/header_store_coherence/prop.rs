//! Discovery proptest: random op sequences over the fixed universe, shrunk on
//! any audit failure to a minimal, transcribable counterexample.
//!
//! Both tests are `#[ignore]`d: the store has known corruption bugs today (the
//! `corruption_repro_*` scenarios), so an always-on random sweep cannot be a CI
//! gate yet. Run discovery manually:
//!
//! ```text
//! PROPTEST_CASES=4096 cargo test -p zebra-state --lib \
//!     header_store_coherence::prop -- --ignored --nocapture
//! ```
//!
//! Every shrunk counterexample must be transcribed into `scenarios.rs` as a
//! hardcoded `corruption_repro_*` / `#[ignore]`d-invariant pair (the primary,
//! seed-independent pinning mechanism); the proptest regression file under
//! `zebra-state/proptest-regressions/` pins the seed as a backstop.
//!
//! When the write-path fixes land, un-ignore
//! `prop_random_sequences_uphold_invariants` as a permanent regression sweep.

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

    /// The full invariant sweep. Fails today on the known corruption shapes;
    /// becomes the permanent regression gate once the write paths are fixed.
    #[test]
    #[ignore = "known zakura header-store corruption: un-ignore with the write-path fix"]
    fn prop_random_sequences_uphold_invariants(ops in ops_strategy()) {
        let _init_guard = zebra_test::init();
        let mut harness = Harness::new();
        if let Err(report) = harness.run_all(&ops) {
            prop_assert!(false, "store invariants violated:\n{report:#?}");
        }
    }

    /// Discovery beyond the known bugs: the harness skips the known
    /// corruption shapes, so any failure here is a *new* violation class.
    /// Run manually with a large PROPTEST_CASES while the known bugs are open.
    #[test]
    #[ignore = "discovery sweep: run manually"]
    fn prop_discovery_avoiding_known_corruptions(ops in ops_strategy()) {
        let _init_guard = zebra_test::init();
        let mut harness = Harness::new_avoiding_known_corruptions();
        if let Err(report) = harness.run_all(&ops) {
            prop_assert!(false, "new store-corruption class found:\n{report:#?}");
        }
    }
}
