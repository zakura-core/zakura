use super::*;
use crate::zakura::{
    block_sync::{tests::window_request_range, ZakuraBlockSyncConfig},
    regulation::ResponseCredit,
};
use proptest::prelude::*;
use std::collections::BTreeMap;

#[test]
fn advancing_keys_preserves_ambiguity_and_the_original_ending() {
    let mut window = DownloadWindow::new(&ZakuraBlockSyncConfig::default());
    window.push_outstanding(window_request_range(1, 2));
    let mut other = window_request_range(4, 1);
    other.request.expected_blocks[0].hash = block::Hash([2; 32]);
    other.response = ResponseCredit::new(1, 5);
    window.push_outstanding(other);
    // A later part of the first range is not yet its next response key.
    assert_eq!(
        window.response_for_hash(block::Hash([2; 32])),
        ResponseMatch::Unique(1)
    );
    window.consume_response(0, 1).unwrap();
    assert_eq!(
        window.response_for_hash(block::Hash([1; 32])),
        ResponseMatch::Missing
    );
    assert_eq!(
        window.response_for_hash(block::Hash([2; 32])),
        ResponseMatch::Ambiguous
    );
    assert!(window.consume_response(1, 6).is_err());
    assert_eq!(
        window.response_for_hash(block::Hash([2; 32])),
        ResponseMatch::Ambiguous
    );
    window.remove_outstanding(1);
    assert_eq!(
        window.response_for_hash(block::Hash([2; 32])),
        ResponseMatch::Unique(0)
    );
    window.consume_response(0, 1).unwrap();
    assert_eq!(
        window.response_for_hash(block::Hash([2; 32])),
        ResponseMatch::Missing
    );
    assert_eq!(
        window.outstanding_index_for_start(block::Height(1)),
        Some(0)
    );
    window.remove_outstanding(0);
    assert_eq!(window.outstanding_index_for_start(block::Height(1)), None);
}

#[test]
fn removing_an_earlier_range_repairs_the_moved_ranges_keys() {
    let mut window = DownloadWindow::new(&ZakuraBlockSyncConfig::default());
    for start in [1, 4, 7] {
        window.push_outstanding(window_request_range(start, 2));
    }
    window.consume_response(2, 1).unwrap();
    window.remove_outstanding(0);
    window.push_outstanding(window_request_range(10, 1));
    assert_eq!(window.outstanding_index_for_start(block::Height(1)), None);
    assert_eq!(
        window.outstanding_index_for_start(block::Height(10)),
        Some(2)
    );
    assert_eq!(
        window.response_for_hash(block::Hash([7; 32])),
        ResponseMatch::Missing
    );
    assert_eq!(
        window.response_for_hash(block::Hash([8; 32])),
        ResponseMatch::Unique(0)
    );
    assert_eq!(
        window.outstanding_index_for_start(block::Height(7)),
        Some(0)
    );
    assert_eq!(
        window.outstanding_index_for_start(block::Height(4)),
        Some(1)
    );
    window.consume_response(0, 1).unwrap();
    window.remove_outstanding(1);
    assert_eq!(window.outstanding_index_for_start(block::Height(4)), None);
    assert_eq!(
        window.outstanding_index_for_start(block::Height(10)),
        Some(1)
    );
    // Completed ranges have no next hash, but their ending still needs an index.
    assert_eq!(
        window.outstanding_index_for_start(block::Height(7)),
        Some(0)
    );
    window.clear_outstanding();
    assert_eq!(
        window.response_for_hash(block::Hash([8; 32])),
        ResponseMatch::Missing
    );
    assert_eq!(window.outstanding_index_for_start(block::Height(7)), None);
}

#[derive(Clone, Copy, Debug)]
enum Action {
    Consume,
    Detach,
    Finish,
}

proptest! {
    #[test]
    fn generated_window_changes_match_original_response_keys(
        keys in prop::collection::vec(prop::collection::vec(0u8..8, 1..5), 1..17),
        history in prop::collection::vec((any::<usize>(), prop_oneof![
            Just(Action::Consume), Just(Action::Detach), Just(Action::Finish)
        ]), 1..128),
    ) {
        let mut window = DownloadWindow::new(&ZakuraBlockSyncConfig::default());
        // The oracle retains identities independently of the vector's positions.
        let mut remaining = BTreeMap::new();
        for (id, keys) in keys.into_iter().enumerate() {
            let start = u32::try_from(id * 8 + 1).unwrap();
            let mut range = window_request_range(start, u32::try_from(keys.len()).unwrap());
            for (expected, key) in range.request.expected_blocks.iter_mut().zip(&keys) {
                expected.hash = block::Hash([*key; 32]);
            }
            remaining.insert(block::Height(start), keys);
            window.push_outstanding(range);
        }
        let starts: Vec<_> = remaining.keys().copied().collect();
        for (choice, action) in history {
            if !window.outstanding.is_empty() {
                let index = choice % window.outstanding.len();
                let start = window.outstanding[index].request.start_height;
                match action {
                    Action::Consume => {
                        let keys = remaining.get_mut(&start).unwrap();
                        if keys.is_empty() {
                            prop_assert!(window.consume_response(index, 1).is_err());
                        } else {
                            window.consume_response(index, 1).unwrap();
                            keys.remove(0);
                        }
                    }
                    Action::Detach => window.outstanding[index].local_work_active = false,
                    Action::Finish => {
                        remaining.remove(&start);
                        window.remove_outstanding(index);
                    }
                }
            }
            prop_assert_eq!(window.outstanding.len(), remaining.len());
            for start in &starts {
                prop_assert_eq!(window.outstanding_index_for_start(*start).is_some(), remaining.contains_key(start));
            }
            for (start, keys) in &remaining {
                let index = window.outstanding_index_for_start(*start).unwrap();
                prop_assert_eq!(window.outstanding[index].request.start_height, *start);
                prop_assert_eq!(window.outstanding[index].next_response_hash(), keys.first().map(|key| block::Hash([*key; 32])));
            }
            for key in 0..=8 {
                let owners: Vec<_> = remaining.iter().filter_map(|(start, keys)| (keys.first() == Some(&key)).then_some(*start)).collect();
                match (owners.as_slice(), window.response_for_hash(block::Hash([key; 32]))) {
                    ([], ResponseMatch::Missing) => {},
                    ([owner], ResponseMatch::Unique(index)) => prop_assert_eq!(window.outstanding[index].request.start_height, *owner),
                    ([_, _, ..], ResponseMatch::Ambiguous) => {},
                    (owners, actual) => prop_assert!(false, "key {key}: owners {owners:?}, match {actual:?}"),
                }
            }
        }
    }
}
