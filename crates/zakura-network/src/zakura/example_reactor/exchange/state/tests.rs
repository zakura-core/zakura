//! The example exchange state against an independent model, and its watch
//! rules over every input combination.

use proptest::prelude::*;

use super::*;
use crate::zakura::example_reactor::{message_type, MAX_ITEMS_PER_REQUEST};

/// Ranges one node may have outstanding: the `GetItems` row's limit.
const MAX_IN_FLIGHT: usize = 4;

/// One input IO delivers to the state, in the order it would deliver it.
#[derive(Clone, Debug)]
enum Op {
    /// IO reserved and wrote a request for `count` items from `start`.
    Request { start: u32, count: u32 },
    /// An item at `height` arrives.
    Item { height: u32 },
    /// The `index`-th outstanding range ends with `ItemsDone`, reporting
    /// `miscount` more items than arrived.
    Done { index: usize, miscount: u32 },
    /// The `index`-th outstanding range ends with `RangeUnavailable`.
    Unavailable { index: usize },
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        2 => (0..64u32, 1..=MAX_ITEMS_PER_REQUEST)
            .prop_map(|(start, count)| Op::Request { start, count }),
        6 => (0..72u32).prop_map(|height| Op::Item { height }),
        2 => (0..MAX_IN_FLIGHT, prop_oneof![3 => Just(0u32), 1 => 1..3u32])
            .prop_map(|(index, miscount)| Op::Done { index, miscount }),
        1 => (0..MAX_IN_FLIGHT).prop_map(|index| Op::Unavailable { index }),
    ]
}

/// An item's bytes. Any value works; the state must return it unchanged.
fn bytes(height: u32) -> Vec<u8> {
    height.to_le_bytes().to_vec()
}

/// The outstanding ranges, in request order, with the items each received.
#[derive(Debug, Default)]
struct Model {
    outstanding: Vec<(ItemRange, Vec<(Height, Vec<u8>)>)>,
}

impl Model {
    /// The position of the outstanding range containing `height`.
    fn containing(&self, height: u32) -> Option<usize> {
        self.outstanding
            .iter()
            .position(|(range, _)| range.start.0 <= height && height < range.start.0 + range.count)
    }

    /// Whether a new range overlaps no outstanding range.
    fn is_disjoint(&self, start: u32, count: u32) -> bool {
        self.outstanding.iter().all(|(range, _)| {
            start + count <= range.start.0 || range.start.0 + range.count <= start
        })
    }
}

proptest! {
    /// After every input the state agrees with the model: an item belongs to
    /// the one outstanding range containing it, an ending succeeds exactly when
    /// its count matches the items that arrived, and a successful ending
    /// returns them in arrival order and retires the range.
    #[test]
    fn exchange_state_follows_the_model(ops in prop::collection::vec(op(), 1..64)) {
        let mut state = ExchangeState::default();
        let mut model = Model::default();
        for op in ops {
            match op {
                Op::Request { start, count } => {
                    // IO's reservations admit at most the row's limit, and
                    // this node requests disjoint ranges.
                    if model.outstanding.len() == MAX_IN_FLIGHT
                        || !model.is_disjoint(start, count)
                    {
                        continue;
                    }
                    let range = ItemRange { start: Height(start), count };
                    state.requested(range);
                    model.outstanding.push((range, Vec::new()));
                }
                Op::Item { height } => {
                    let found = state.range_of(Height(height), message_type::ITEM);
                    let expected = model.containing(height);
                    prop_assert_eq!(
                        found,
                        expected
                            .map(|index| model.outstanding[index].0.start)
                            .ok_or(ClaimRefused::Unsolicited {
                                message_type: message_type::ITEM,
                            })
                    );
                    let (Ok(start), Some(index)) = (found, expected) else {
                        continue;
                    };
                    let (range, items) = &mut model.outstanding[index];
                    // The reservation admits at most one frame per height.
                    if items.len() == usize::try_from(range.count).expect("a small count fits usize") {
                        continue;
                    }
                    state.item(start, Height(height), bytes(height));
                    items.push((Height(height), bytes(height)));
                }
                Op::Done { index, miscount } => {
                    if model.outstanding.is_empty() {
                        continue;
                    }
                    let index = index % model.outstanding.len();
                    let (range, items) = &model.outstanding[index];
                    let claimed = u32::try_from(items.len()).expect("at most eight items");
                    let returned = claimed + miscount;
                    // The codec refuses an ending that reports no item.
                    if returned == 0 {
                        continue;
                    }
                    let ended = state.items_done(range.start, returned, claimed);
                    if miscount == 0 {
                        let (_, items) = model.outstanding.remove(index);
                        prop_assert_eq!(ended, Ok(items));
                    } else {
                        prop_assert_eq!(
                            ended,
                            Err(Violation::Count { reported: returned, received: claimed })
                        );
                    }
                }
                Op::Unavailable { index } => {
                    if model.outstanding.is_empty() {
                        continue;
                    }
                    let index = index % model.outstanding.len();
                    let (range, items) = &model.outstanding[index];
                    let range = *range;
                    let claimed = u32::try_from(items.len()).expect("at most eight items");
                    let ended = state.range_unavailable(range.start, claimed);
                    if claimed == 0 {
                        model.outstanding.remove(index);
                        prop_assert_eq!(ended, Ok(range));
                    } else {
                        prop_assert_eq!(
                            ended,
                            Err(Violation::Count { reported: 0, received: claimed })
                        );
                    }
                }
            }
            prop_assert_eq!(state.idle(), model.outstanding.is_empty());
            let mut expected: Vec<ItemRange> =
                model.outstanding.iter().map(|(range, _)| *range).collect();
            expected.sort_by_key(|range| range.start);
            let ranges: Vec<ItemRange> =
                state.downloads.values().map(|download| download.range).collect();
            prop_assert_eq!(ranges, expected);
        }
    }
}

#[test]
fn a_pushed_item_follows_only_the_next_height() {
    for received in [0, 1, 7, Height::MAX.0 - 1, Height::MAX.0] {
        for height in received.saturating_sub(1)..=received.saturating_add(2) {
            let next = received < Height::MAX.0 && height == received + 1;
            let expected = if next {
                Ok(())
            } else {
                Err(Violation::Linkage {
                    received: Height(received),
                    got: Height(height),
                })
            };
            assert_eq!(follows(Height(received), Height(height)), expected);
        }
    }
}

#[test]
fn each_end_reason_arrives_only_in_its_window() {
    for reason in [
        EndReason::Unavailable,
        EndReason::Superseded,
        EndReason::Closed,
    ] {
        for pages in [0, 1, 16] {
            for close_sent in [false, true] {
                let in_window = match (reason, pages, close_sent) {
                    (EndReason::Unavailable, 0, _) => true,
                    (EndReason::Unavailable, _, _) => false,
                    (EndReason::Superseded, _, _) => true,
                    (EndReason::Closed, _, sent) => sent,
                };
                assert_eq!(
                    end_in_window(reason, pages, close_sent),
                    if in_window {
                        Ok(())
                    } else {
                        Err(Violation::OutcomeWindow(reason))
                    },
                    "{reason:?} after {pages} pages, close sent: {close_sent}"
                );
            }
        }
    }
}

#[test]
fn the_publisher_ends_a_watch_for_the_first_reason_that_applies() {
    let low = Height(10);
    for closing in [false, true] {
        for superseded in [false, true] {
            for first in [false, true] {
                for next in [None, Some(Height(9)), Some(low), Some(Height(11))] {
                    let unservable = first && matches!(next, Some(Height(9)));
                    let expected = match (closing, superseded, unservable) {
                        (true, _, _) => Some(EndReason::Closed),
                        (false, true, _) => Some(EndReason::Superseded),
                        (false, false, true) => Some(EndReason::Unavailable),
                        (false, false, false) => None,
                    };
                    assert_eq!(end_reason(closing, superseded, first, next, low), expected);
                }
            }
        }
    }
}

#[test]
fn half_a_free_window_renews_the_watch() {
    for objects in 0..=WATCH_ITEMS {
        let room = Credit { objects, bytes: 0 };
        assert_eq!(renews(room), objects >= 8, "{objects} free items");
    }
}
