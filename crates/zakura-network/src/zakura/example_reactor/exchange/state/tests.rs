//! Tests for the example exchange state and its watch rules.
//!
//! [`replay_exchange_bytes`] is the template for a machine target (see
//! [`machine_test`](crate::zakura::machine_test)). It drives the production
//! [`ExchangeState`], checks the state's invariants after every input, and
//! compares each output only with facts that follow from the inputs it chose:
//! which range an item was sent for, how many items arrived, and what count the
//! peer reported. It never re-derives a decision the state makes. Named cases
//! pin the watch rules.

use super::*;
use crate::zakura::{
    example_reactor::{message_type, MAX_ITEMS_PER_REQUEST},
    machine_test::{machine_target_tests, MAX_HISTORY_BYTES},
};

/// Ranges one node may have outstanding: the `GetItems` row's limit.
const MAX_IN_FLIGHT: usize = 4;

/// Heights between the starts of two slots. A range in one slot never reaches
/// the next slot, so the ranges this node requests are disjoint, as IO's are.
const SLOT: u32 = 2 * MAX_ITEMS_PER_REQUEST;

/// Slots a request may use.
const SLOTS: u32 = 4;

/// One input IO delivers to the state, in the order it would deliver it.
#[derive(Clone, Copy, Debug)]
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

/// Decoder v1: each pair of bytes is one input. The first byte's low three
/// bits choose the input; the second byte and the first byte's high bits
/// choose its arguments.
fn decode(data: &[u8]) -> impl Iterator<Item = Op> + '_ {
    data[..data.len().min(MAX_HISTORY_BYTES)]
        .chunks_exact(2)
        .map(|pair| {
            let (kind, high, arg) = (pair[0] & 7, u32::from(pair[0] >> 3), pair[1]);
            let index = usize::from(arg & 3);
            match kind {
                0 => Op::Request {
                    slot: u32::from(arg & 3),
                    shift: u32::from(arg >> 2) % (SLOT - MAX_ITEMS_PER_REQUEST + 1),
                    count: high % MAX_ITEMS_PER_REQUEST + 1,
                },
                1..=3 => Op::Item {
                    index,
                    offset: u32::from(arg >> 2) % MAX_ITEMS_PER_REQUEST,
                },
                4 => Op::Stray {
                    height: u32::from(arg) % (SLOTS * SLOT),
                },
                5 | 6 => Op::Done {
                    index,
                    miscount: (high % 4).saturating_sub(1),
                },
                _ => Op::Unavailable { index },
            }
        })
}

/// An item's bytes. Any value works; the state must return it unchanged.
fn bytes(height: u32) -> Vec<u8> {
    height.to_le_bytes().to_vec()
}

/// Transitions one history reached.
#[derive(Debug, Default, Eq, PartialEq)]
struct ExchangeCounters {
    requested: usize,
    items: usize,
    strays_refused: usize,
    done: usize,
    miscounts_refused: usize,
    unavailable: usize,
    late_unavailable_refused: usize,
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
    fn deliver(&mut self, state: &mut ExchangeState, start: Height, height: Height) -> bool {
        let (range, items) = self
            .outstanding
            .iter_mut()
            .find(|(range, _)| range.start == start)
            .expect("the state names only outstanding ranges");
        // The reservation admits at most `count` frames.
        if items.len() == usize::try_from(range.count).expect("a count fits usize") {
            return false;
        }
        state.item(start, height, bytes(height.0));
        items.push((height, bytes(height.0)));
        true
    }
}

/// Replay at most [`MAX_HISTORY_BYTES`] bytes through [`ExchangeState`].
///
/// The state keeps its invariants after every input. An item the test sent for
/// a range lands in that range. An ending succeeds exactly when the peer
/// reports the count that arrived, returns the items in arrival order, and
/// retires the range.
fn replay_exchange_bytes(data: &[u8]) -> ExchangeCounters {
    let mut state = ExchangeState::default();
    let mut sent = Sent::default();
    let mut counters = ExchangeCounters::default();
    for op in decode(data) {
        match op {
            Op::Request { slot, shift, count } => {
                // IO's reservations admit at most the row's limit, and this
                // node requests at most one range per slot.
                if sent.outstanding.len() == MAX_IN_FLIGHT
                    || sent
                        .outstanding
                        .iter()
                        .any(|(range, _)| range.start.0 / SLOT == slot)
                {
                    continue;
                }
                let range = ItemRange {
                    start: Height(slot * SLOT + shift),
                    count,
                };
                state.requested(range);
                sent.outstanding.push((range, Vec::new()));
                counters.requested += 1;
            }
            Op::Item { index, offset } => {
                let Some(index) = sent.pick(index) else {
                    continue;
                };
                let range = sent.outstanding[index].0;
                if offset >= range.count {
                    continue;
                }
                let height = Height(range.start.0 + offset);
                assert_eq!(
                    state.range_of(height, message_type::ITEM),
                    Ok(range.start),
                    "an item lands in the range it was sent for"
                );
                if sent.deliver(&mut state, range.start, height) {
                    counters.items += 1;
                }
            }
            Op::Stray { height } => {
                // The invariants check that a found range contains the item.
                match state.range_of(Height(height), message_type::ITEM) {
                    Ok(start) => {
                        if sent.deliver(&mut state, start, Height(height)) {
                            counters.items += 1;
                        }
                    }
                    Err(_) => counters.strays_refused += 1,
                }
            }
            Op::Done { index, miscount } => {
                let Some(index) = sent.pick(index) else {
                    continue;
                };
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
                    assert_eq!(ended, Ok(items), "an ending returns what arrived");
                    counters.done += 1;
                } else {
                    assert_eq!(
                        ended,
                        Err(Violation::Count {
                            reported: returned,
                            received: claimed
                        })
                    );
                    counters.miscounts_refused += 1;
                }
            }
            Op::Unavailable { index } => {
                let Some(index) = sent.pick(index) else {
                    continue;
                };
                let (range, items) = &sent.outstanding[index];
                let range = *range;
                let claimed = u32::try_from(items.len()).expect("at most eight items");
                let ended = state.range_unavailable(range.start, claimed);
                if claimed == 0 {
                    sent.outstanding.remove(index);
                    assert_eq!(ended, Ok(range));
                    counters.unavailable += 1;
                } else {
                    assert_eq!(
                        ended,
                        Err(Violation::Count {
                            reported: 0,
                            received: claimed
                        })
                    );
                    counters.late_unavailable_refused += 1;
                }
            }
        }
        state.assert_invariants();
        assert_eq!(state.idle(), sent.outstanding.is_empty());
    }
    counters
}

/// Requests two ranges, fills and ends one, refuses a miscount, a stray and a
/// late `RangeUnavailable`, ends the other, then ends a third as unavailable.
const WITNESS: &[u8] = &[
    // Request three items at 0 and two items at 18.
    0b10_000,
    0b0000_0000,
    0b01_000,
    0b0000_1001,
    // Items at offsets 0, 1 and 2 of range 0.
    1,
    0b0000_0000,
    1,
    0b0000_0100,
    1,
    0b0000_1000,
    // Height 40 is in no range.
    4,
    40,
    // Range 0 reports one item too many, then the right count.
    0b10_101,
    0,
    5,
    0,
    // Range 18 gets one item, refuses `RangeUnavailable`, then ends with one
    // item.
    1,
    0,
    7,
    0,
    5,
    0,
    // A one-item range at 32 ends as unavailable.
    0,
    0b0000_0010,
    7,
    0,
];

machine_target_tests! {
    replay: replay_exchange_bytes,
    target: "example_exchange",
    decoder: 1,
    witness: WITNESS => ExchangeCounters {
        requested: 3,
        items: 4,
        strays_refused: 1,
        done: 2,
        miscounts_refused: 1,
        unavailable: 1,
        late_unavailable_refused: 1,
    },
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
