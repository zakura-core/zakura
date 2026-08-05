//! Fail-closed reads from deliberately corrupted fork-aware column families.

use super::harness::{Anchor, Harness, Op, Source};

fn trunk_harness() -> Harness {
    let mut harness = Harness::new();
    harness.run_all(&[Op::InsertHeaders {
        source: Source::Trunk,
        offset: 0,
        len: 20,
        anchor: Anchor::Natural,
    }]);
    harness
}

#[test]
fn selected_projection_node_disagreement_never_produces_a_hash_or_locator() {
    let _init_guard = zakura_test::init();
    let harness = trunk_harness();
    let tip = harness.trunk_frontier(20);
    let foreign = harness.trunk_frontier(5);

    harness.corrupt_selected_hash(tip.height, foreign.hash);
    harness.assert_selected_hash_fails_closed(tip.height);
    harness.assert_selected_locator_fails_closed();
}

#[test]
fn selected_projection_gap_within_published_bounds_is_incoherent() {
    let _init_guard = zakura_test::init();
    let harness = trunk_harness();
    let interior = harness.trunk_frontier(10);

    harness.delete_selected_hash(interior.height);
    harness.assert_selected_hash_fails_closed(interior.height);
}

#[test]
fn missing_selected_successor_is_incoherent_instead_of_end_of_chain() {
    let _init_guard = zakura_test::init();
    let harness = trunk_harness();
    let parent = harness.trunk_frontier(19);
    let successor = harness.trunk_frontier(20);

    harness.delete_node(successor.hash);
    harness.assert_selected_successor_fails_closed(parent);
}

#[test]
fn foreign_node_value_never_enters_a_validation_lease() {
    let _init_guard = zakura_test::init();
    let harness = trunk_harness();
    let target = harness.trunk_frontier(20);
    let donor = harness.trunk_frontier(19);

    harness.replace_node_value(target.hash, donor.hash);
    harness.assert_validation_context_fails_closed(target.hash);
}
