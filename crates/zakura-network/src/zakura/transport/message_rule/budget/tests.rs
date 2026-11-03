//! Receive bounds per role and side, and the budget's checked arithmetic.

use std::time::Duration;

use proptest::prelude::*;

use super::*;
use crate::zakura::{Cadence, Credit, PayloadLen};

const CADENCE: Cadence = Cadence {
    capacity: 22,
    refill_interval: Duration::from_secs(15),
    send_interval: Duration::from_secs(30),
};

const STATUS: MessageRule = MessageRule {
    name: "status",
    message_type: 1,
    payload: PayloadLen::exact(8),
    role: MessageRole::Announcement { cadence: CADENCE },
};

const GET: MessageRule = MessageRule {
    name: "get",
    message_type: 2,
    payload: PayloadLen::exact(8),
    role: MessageRole::Request {
        max_in_flight: 2,
        cadence: None,
    },
};

const fn response(
    name: &'static str,
    message_type: u16,
    request: u16,
    ends_exchange: bool,
    max: usize,
) -> MessageRule {
    MessageRule {
        name,
        message_type,
        payload: PayloadLen::between(1, max),
        role: MessageRole::Response {
            request,
            ends_exchange,
        },
    }
}

const DONE: MessageRule = response("done", 3, 2, true, 100);
const MISSING: MessageRule = response("missing", 4, 2, true, 9);
const PART: MessageRule = response("part", 5, 2, false, 100);

const fn watch(cadence: Option<Cadence>) -> MessageRule {
    MessageRule {
        name: "watch",
        message_type: 6,
        payload: PayloadLen::exact(8),
        role: MessageRole::Subscription {
            max_live: 2,
            credit: Credit {
                objects: 16,
                bytes: 4096,
            },
            cursor_history: 16,
            cadence,
        },
    }
}

const PAGE: MessageRule = response("page", 7, 6, false, 100);
const ENDED: MessageRule = response("ended", 8, 6, true, 9);

const fn stream(kind: u16, messages: Option<&'static [MessageRule]>) -> Stream {
    Stream {
        kind,
        version: 1,
        frame_cap: 1024,
        capability: 1 << 16,
        messages,
        ..Stream::PERSISTENT
    }
}

fn bounds(layout: &[Stream]) -> [Result<u64, BudgetError>; 3] {
    [LocalRole::Requester, LocalRole::Responder, LocalRole::Both]
        .map(|local| Stream::receive_bound(layout, local))
}

#[test]
fn announcements_count_their_cadence_on_every_side() {
    // 22 messages of 8 bytes.
    assert_eq!(bounds(&[stream(64, Some(&[STATUS]))]), [Ok(176); 3]);
}

#[test]
fn requests_count_inbound_requests_and_the_largest_ending_response() {
    let layout = [stream(64, Some(&[GET, DONE, MISSING]))];
    // Serving: 2 requests of 8 bytes. Requesting: 2 endings of at most 100.
    assert_eq!(bounds(&layout), [Ok(200), Ok(16), Ok(216)]);
}

#[test]
fn a_response_on_another_stream_counts_toward_its_request() {
    let layout = [stream(64, Some(&[DONE, MISSING])), stream(65, Some(&[GET]))];
    assert_eq!(bounds(&layout), [Ok(200), Ok(16), Ok(216)]);
}

#[test]
fn a_request_with_unending_responses_has_no_requester_bound() {
    let layout = [stream(64, Some(&[GET, PART, DONE]))];
    let unbounded = Err(BudgetError::UnboundedResponses { message_type: 2 });
    assert_eq!(bounds(&layout), [unbounded, Ok(16), unbounded]);
}

#[test]
fn subscriptions_count_credit_and_one_response_per_live_subscription() {
    const UNPACED: [MessageRule; 3] = [watch(None), PAGE, ENDED];
    const PACED: [MessageRule; 3] = [watch(Some(CADENCE)), PAGE, ENDED];
    let layout = [stream(64, Some(&UNPACED))];
    let unbounded = Err(BudgetError::UnboundedUpdates { message_type: 6 });
    // Subscribing: 2 live × (4096 credit bytes + a 100-byte page).
    assert_eq!(bounds(&layout), [Ok(8392), unbounded, unbounded]);

    let layout = [stream(64, Some(&PACED))];
    // Publishing: 22 updates of 8 bytes.
    assert_eq!(bounds(&layout), [Ok(8392), Ok(176), Ok(8568)]);
}

#[test]
fn a_layout_without_tables_or_streams_has_no_bound() {
    assert_eq!(bounds(&[stream(64, None)]), [Err(BudgetError::NoTables); 3]);
    assert_eq!(
        bounds(&[]),
        [Err(BudgetError::Layout(LayoutError::Empty)); 3]
    );
}

#[test]
fn a_bound_past_u64_is_an_overflow() {
    // Widening u32 to usize is lossless on every supported target.
    const HUGE: usize = u32::MAX as usize - crate::zakura::FRAME_HEADER_BYTES;
    const BIG_GET: MessageRule = MessageRule {
        payload: PayloadLen::exact(HUGE),
        role: MessageRole::Request {
            max_in_flight: u32::MAX,
            cadence: None,
        },
        ..GET
    };
    const BIG_DONE: MessageRule = response("done", 3, 2, true, HUGE);
    let layout = [Stream {
        frame_cap: u32::MAX,
        ..stream(64, Some(&[BIG_GET, BIG_DONE]))
    }];
    // Each side alone fits, below 2^64. Together they do not.
    // Widening usize to u64 is lossless on every supported target.
    let one_side = u64::from(u32::MAX) * HUGE as u64;
    assert_eq!(
        bounds(&layout),
        [
            Ok(one_side),
            Ok(one_side),
            Err(BudgetError::Overflow { message_type: 2 })
        ]
    );
}

#[test]
fn a_receive_share_below_the_frame_cap_is_refused() {
    let budget = ReactorBudget {
        receive: 1023,
        output: 0,
    };
    assert_eq!(
        budget.check_frame_cap(1024),
        Err(BudgetError::BelowFrameCap {
            receive: 1023,
            frame_cap: 1024,
        })
    );
    assert_eq!(budget.check_frame_cap(1023), Ok(budget));

    const EXACT: ReactorBudget = ReactorBudget {
        receive: 1024,
        output: 0,
    }
    .assert_frame_cap(1024);
    assert_eq!(EXACT.receive, 1024);
}

/// Budgets that mix small values, whose sums fit, with any `u64`.
fn budget() -> impl Strategy<Value = ReactorBudget> {
    let bytes = || prop_oneof![0..1u64 << 40, any::<u64>()];
    (bytes(), bytes()).prop_map(|(receive, output)| ReactorBudget { receive, output })
}

/// `value` if it fits in `u64`.
fn fits(value: u128) -> Option<u64> {
    u64::try_from(value).ok()
}

proptest! {
    #[test]
    fn checked_sums_are_exact_or_none(budgets in prop::collection::vec(budget(), 0..4)) {
        let receive: u128 = budgets.iter().map(|budget| u128::from(budget.receive)).sum();
        let output: u128 = budgets.iter().map(|budget| u128::from(budget.output)).sum();
        let expected = fits(receive)
            .zip(fits(output))
            .map(|(receive, output)| ReactorBudget { receive, output });
        prop_assert_eq!(ReactorBudget::checked_sum(&budgets), expected);
    }

    #[test]
    fn checked_products_are_exact_or_none(budget in budget(), connections in any::<u32>()) {
        // Widening u32 to usize and u128 is lossless.
        let times = |bytes: u64| fits(u128::from(bytes) * u128::from(connections));
        let expected = times(budget.receive)
            .zip(times(budget.output))
            .map(|(receive, output)| ReactorBudget { receive, output });
        prop_assert_eq!(budget.checked_mul(connections as usize), expected);
    }
}
