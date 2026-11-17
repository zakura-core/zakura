//! Tests for the example exchange state and its watch rules.
//!
//! The proptest drives the production [`ExchangeState`]. It checks the state's
//! invariants after every input, and it compares each output only with facts
//! that follow from the inputs the test chose: which range an item was sent
//! for, how many items arrived, and what count the peer reported. The test
//! never re-derives a decision the state makes. Named cases pin the watch
//! rules.

use proptest::prelude::*;

use super::*;
use crate::zakura::example_reactor::{message_type, MAX_ITEMS_PER_REQUEST};

/// Ranges one node may have outstanding: the `GetItems` row's limit.
const MAX_IN_FLIGHT: usize = 4;

/// Heights between the starts of two slots. A range in one slot never reaches
/// the next slot, so the ranges this node requests are disjoint, as IO's are.
const SLOT: u32 = 2 * MAX_ITEMS_PER_REQUEST;

/// Slots a request may use.
const SLOTS: u32 = 4;

/// One input IO delivers to the state, in the order it would deliver it.
#[derive(Clone, Debug)]
enum Op {
    /// IO reserved and wrote a request for `count` items, starting `shift`
    /// heights into `slot`.
    Request { slot: u32, shift: u32, count: u32 },
    /// An item at `offset` heights into the `index`-th outstanding range
    /// arrives.
    Item { index: usize, offset: u32 },
    /// An item arrives at a `height` the test does not tie to any range.
    Stray { height: u32 },
    /// The `index`-th outstanding range ends with `ItemsDone`, reporting
    /// `miscount` more items than arrived.
    Done { index: usize, miscount: u32 },
    /// The `index`-th outstanding range ends with `RangeUnavailable`.
    Unavailable { index: usize },
}

fn op() -> impl Strategy<Value = Op> {
    let count = 1..=MAX_ITEMS_PER_REQUEST;
    prop_oneof![
        2 => (0..SLOTS, 0..=SLOT - MAX_ITEMS_PER_REQUEST, count.clone())
            .prop_map(|(slot, shift, count)| Op::Request { slot, shift, count }),
        5 => (0..MAX_IN_FLIGHT, 0..MAX_ITEMS_PER_REQUEST)
            .prop_map(|(index, offset)| Op::Item { index, offset }),
        2 => (0..SLOTS * SLOT).prop_map(|height| Op::Stray { height }),
        2 => (0..MAX_IN_FLIGHT, prop_oneof![3 => Just(0u32), 1 => 1..3u32])
            .prop_map(|(index, miscount)| Op::Done { index, miscount }),
        1 => (0..MAX_IN_FLIGHT).prop_map(|index| Op::Unavailable { index }),
    ]
}

/// An item's bytes. Any value works; the state must return it unchanged.
fn bytes(height: u32) -> Vec<u8> {
    height.to_le_bytes().to_vec()
}

/// What the test sent: each outstanding range, in request order, with the
/// items the test delivered for it.
#[derive(Debug, Default)]
struct Sent {
    outstanding: Vec<(ItemRange, Vec<(Height, Vec<u8>)>)>,
}

impl Sent {
    /// Pick the outstanding range `index` names, if any range is outstanding.
    fn pick(&self, index: usize) -> Option<usize> {
        (!self.outstanding.is_empty()).then(|| index % self.outstanding.len())
    }

    /// Deliver an item the state placed in the range at `start`.
    fn deliver(&mut self, state: &mut ExchangeState, start: Height, height: Height) {
        let (range, items) = self
            .outstanding
            .iter_mut()
            .find(|(range, _)| range.start == start)
            .expect("the state names only outstanding ranges");
        // The reservation admits at most `count` frames.
        if items.len() == usize::try_from(range.count).expect("a count fits usize") {
            return;
        }
        state.item(start, height, bytes(height.0));
        items.push((height, bytes(height.0)));
    }
}

proptest! {
    /// The state keeps its invariants after every input. An item the test sent
    /// for a range lands in that range. An ending succeeds exactly when the
    /// peer reports the count that arrived, returns the items in arrival
    /// order, and retires the range.
    #[test]
    fn exchange_state_keeps_its_invariants(ops in prop::collection::vec(op(), 1..64)) {
        let mut state = ExchangeState::default();
        let mut sent = Sent::default();
        for op in ops {
            match op {
                Op::Request { slot, shift, count } => {
                    let start = Height(slot * SLOT + shift);
                    // IO's reservations admit at most the row's limit, and
                    // this node requests at most one range per slot.
                    if sent.outstanding.len() == MAX_IN_FLIGHT
                        || sent.outstanding.iter().any(|(range, _)| range.start.0 / SLOT == slot)
                    {
                        continue;
                    }
                    let range = ItemRange { start, count };
                    state.requested(range);
                    sent.outstanding.push((range, Vec::new()));
                }
                Op::Item { index, offset } => {
                    let Some(index) = sent.pick(index) else { continue };
                    let range = sent.outstanding[index].0;
                    if offset >= range.count {
                        continue;
                    }
                    let height = Height(range.start.0 + offset);
                    prop_assert_eq!(state.range_of(height, message_type::ITEM), Ok(range.start));
                    sent.deliver(&mut state, range.start, height);
                }
                Op::Stray { height } => {
                    // The invariants check that a found range contains the item.
                    if let Ok(start) = state.range_of(Height(height), message_type::ITEM) {
                        sent.deliver(&mut state, start, Height(height));
                    }
                }
                Op::Done { index, miscount } => {
                    let Some(index) = sent.pick(index) else { continue };
                    let (range, items) = &sent.outstanding[index];
                    let claimed = u32::try_from(items.len()).expect("at most eight items");
                    let returned = claimed + miscount;
                    // The codec refuses an ending that reports no item.
                    if returned == 0 {
                        continue;
                    }
                    let ended = state.items_done(range.start, returned, claimed);
                    if miscount == 0 {
                        let (_, items) = sent.outstanding.remove(index);
                        prop_assert_eq!(ended, Ok(items));
                    } else {
                        prop_assert_eq!(
                            ended,
                            Err(Violation::Count { reported: returned, received: claimed })
                        );
                    }
                }
                Op::Unavailable { index } => {
                    let Some(index) = sent.pick(index) else { continue };
                    let (range, items) = &sent.outstanding[index];
                    let range = *range;
                    let claimed = u32::try_from(items.len()).expect("at most eight items");
                    let ended = state.range_unavailable(range.start, claimed);
                    if claimed == 0 {
                        sent.outstanding.remove(index);
                        prop_assert_eq!(ended, Ok(range));
                    } else {
                        prop_assert_eq!(
                            ended,
                            Err(Violation::Count { reported: 0, received: claimed })
                        );
                    }
                }
            }
            state.assert_invariants();
            prop_assert_eq!(state.idle(), sent.outstanding.is_empty());
        }
    }
}

#[test]
fn an_item_past_a_range_is_unsolicited() {
    let mut state = ExchangeState::default();
    state.requested(ItemRange {
        start: Height(10),
        count: 3,
    });
    for (height, found) in [(9, None), (10, Some(10)), (12, Some(10)), (13, None)] {
        assert_eq!(
            state.range_of(Height(height), message_type::ITEM),
            found.map(Height).ok_or(ClaimRefused::Unsolicited {
                message_type: message_type::ITEM,
            }),
            "item at {height}"
        );
    }
}

#[test]
fn a_pushed_item_follows_only_the_next_height() {
    let max = Height::MAX.0;
    for (received, height, follows_it) in [
        (5, 6, true),
        (5, 5, false),
        (5, 7, false),
        (5, 4, false),
        (0, 1, true),
        (max - 1, max, true),
        (max, max, false),
    ] {
        assert_eq!(
            follows(Height(received), Height(height)).is_ok(),
            follows_it,
            "{height} after {received}"
        );
    }
    assert_eq!(
        follows(Height(5), Height(7)),
        Err(Violation::Linkage {
            received: Height(5),
            got: Height(7),
        })
    );
}

#[test]
fn each_end_reason_arrives_only_in_its_window() {
    use EndReason::{Closed, Superseded, Unavailable};
    for (reason, pages, close_sent, in_window) in [
        (Unavailable, 0, false, true),
        (Unavailable, 0, true, true),
        (Unavailable, 1, false, false),
        (Superseded, 0, false, true),
        (Superseded, 16, true, true),
        (Closed, 0, true, true),
        (Closed, 16, true, true),
        (Closed, 16, false, false),
    ] {
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

#[test]
fn the_publisher_ends_a_watch_for_the_first_reason_that_applies() {
    use EndReason::{Closed, Superseded, Unavailable};
    let low = Height(10);
    let below = Some(Height(9));
    for (closing, superseded, first, next, expected) in [
        (true, true, true, below, Some(Closed)),
        (false, true, true, below, Some(Superseded)),
        (false, false, true, below, Some(Unavailable)),
        (false, false, true, Some(low), None),
        (false, false, true, None, None),
        (false, false, false, below, None),
    ] {
        assert_eq!(
            end_reason(closing, superseded, first, next, low),
            expected,
            "closing: {closing}, superseded: {superseded}, first: {first}, next: {next:?}"
        );
    }
}

#[test]
fn half_a_free_window_renews_the_watch() {
    for (objects, renewed) in [(0, false), (7, false), (8, true), (16, true)] {
        let room = Credit { objects, bytes: 0 };
        assert_eq!(renews(room), renewed, "{objects} free items");
    }
}
