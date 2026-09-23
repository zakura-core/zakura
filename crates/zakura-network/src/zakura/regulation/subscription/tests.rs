//! Subscription tools: credit, each side alone, and both sides joined.
//!
//! The credit tests port #978's `renewable_credit_histories_…` and
//! `credit_grant_overflow_…` to the window rule. The idle, crossed-`Close`,
//! and retirement tests port #978's subscription adapter tests to the tools.

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use futures::FutureExt;
use proptest::prelude::*;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::zakura::{
    framed_channel,
    regulation::{ServeCapacity, ServeLimits, WriterFence, UNFINISHED_EXCHANGE},
    CloseCause, Frame, PayloadLen, ZakuraPeerId,
};

mod message_type {
    pub(super) const WATCH: u16 = 1;
    pub(super) const PAGE: u16 = 2;
    pub(super) const ENDED: u16 = 3;
}

/// The window of every test row: 4 objects and 64 bytes.
const LIMIT: Credit = Credit {
    objects: 4,
    bytes: 64,
};

const WATCH: MessageRule = MessageRule {
    message_type: message_type::WATCH,
    payload: PayloadLen::exact(8),
    role: MessageRole::Subscription {
        max_live: 1,
        credit: LIMIT,
        cursor_history: LIMIT.objects,
        cadence: None,
    },
};

const RULES: &[MessageRule] = &[
    WATCH,
    MessageRule {
        message_type: message_type::PAGE,
        payload: PayloadLen::between(1, 64),
        role: MessageRole::Response {
            request: message_type::WATCH,
            ends_exchange: false,
        },
    },
    MessageRule {
        message_type: message_type::ENDED,
        payload: PayloadLen::exact(1),
        role: MessageRole::Response {
            request: message_type::WATCH,
            ends_exchange: true,
        },
    },
];

const fn credit(objects: u32, bytes: u32) -> Credit {
    Credit { objects, bytes }
}

/// The test row's limits with `max_live` live subscriptions. The cursor
/// history is exactly the object credit, the smallest the validator allows.
fn limits(max_live: u32) -> SubscriptionLimits {
    SubscriptionLimits {
        max_live,
        ..SubscriptionLimits::from_rule(&WATCH).expect("WATCH is a subscription row")
    }
}

fn totals(objects: u64, bytes: u64) -> Totals {
    Totals { objects, bytes }
}

// Credit.

#[derive(Clone, Debug)]
enum CreditOp {
    Grant(u32, u32),
    Consume(u64, u64),
    /// Acknowledge this share, out of 255, of the unacknowledged consumption.
    Acknowledge(u8),
}

fn credit_op() -> impl Strategy<Value = CreditOp> {
    prop_oneof![
        (0u32..6, 0u32..81).prop_map(|(objects, bytes)| CreditOp::Grant(objects, bytes)),
        (0u64..6, 0u64..81).prop_map(|(objects, bytes)| CreditOp::Consume(objects, bytes)),
        any::<u8>().prop_map(CreditOp::Acknowledge),
    ]
}

proptest! {
    /// A grant succeeds exactly when the window from the acknowledgement stays
    /// within the limit. Consumption is cumulative, and `check` agrees with a
    /// model after every step.
    #[test]
    fn renewable_credit_histories_bound_the_window_and_preserve_consumption(
        ops in prop::collection::vec(credit_op(), 1..100),
    ) {
        let limit = credit(4, 40);
        let mut credit_ = ResponseCredit::new(limit);
        let mut granted = totals(4, 40);
        let mut consumed = Totals::default();
        let mut acknowledged = Totals::default();
        for op in ops {
            match op {
                CreditOp::Grant(objects, bytes) => {
                    let after = totals(
                        granted.objects + u64::from(objects),
                        granted.bytes + u64::from(bytes),
                    );
                    let fits = after.objects - acknowledged.objects <= 4
                        && after.bytes - acknowledged.bytes <= 40;
                    prop_assert_eq!(
                        credit_.grant(credit(objects, bytes), acknowledged, limit).is_ok(),
                        fits
                    );
                    if fits {
                        granted = after;
                    }
                }
                CreditOp::Consume(objects, bytes) => {
                    let fits = objects <= granted.objects - consumed.objects
                        && bytes <= granted.bytes - consumed.bytes;
                    prop_assert_eq!(credit_.consume(objects, bytes).is_ok(), fits);
                    if fits {
                        consumed = totals(consumed.objects + objects, consumed.bytes + bytes);
                    }
                }
                CreditOp::Acknowledge(share) => {
                    let step = |from: u64, to: u64| from + (to - from) * u64::from(share) / 255;
                    acknowledged = totals(
                        step(acknowledged.objects, consumed.objects),
                        step(acknowledged.bytes, consumed.bytes),
                    );
                }
            }
            prop_assert_eq!(credit_.granted(), granted);
            prop_assert_eq!(credit_.consumed(), consumed);
            prop_assert!(within_window(granted, acknowledged, limit));
            let unspent = credit_.unspent();
            prop_assert!(credit_.check(unspent.objects, unspent.bytes).is_ok());
            prop_assert!(credit_.check(unspent.objects + 1, 0).is_err());
            prop_assert!(credit_.check(0, unspent.bytes + 1).is_err());
        }
    }
}

#[test]
fn credit_grant_overflow_and_limit_failure_are_atomic() {
    for added in [credit(1, 0), credit(0, 1), credit(1, 1)] {
        let full = totals(u64::MAX, u64::MAX);
        let mut credit_ = ResponseCredit::with_totals(full, full);
        let limit = credit(u32::MAX, u32::MAX);
        assert_eq!(credit_.grant(added, full, limit), Err(CreditExceeded));
        assert_eq!(credit_.consumed(), full);
        assert!(credit_.check(0, 0).is_ok());
        assert!(credit_.check(1, 0).is_err());
        assert!(credit_.check(0, 1).is_err());
    }
    let mut credit_ = ResponseCredit::new(credit(1, 10));
    assert_eq!(
        credit_.grant(credit(2, 31), Totals::default(), credit(4, 40)),
        Err(CreditExceeded)
    );
    assert!(credit_.check(1, 10).is_ok());
    assert!(credit_.check(2, 0).is_err());
    assert!(credit_.check(0, 11).is_err());
}

#[test]
fn exact_bounds_and_refused_spends_preserve_consumption() {
    let mut credit_ = ResponseCredit::new(credit(2, 10));
    credit_.consume(1, 6).unwrap();
    assert!(credit_.consume(1, 5).is_err());
    assert_eq!(credit_.consumed(), totals(1, 6));
    credit_.consume(1, 4).unwrap();
    assert!(credit_.consume(1, 0).is_err());
    assert!(credit_.consume(0, 1).is_err());
}

// The subscriber.

#[derive(Clone, Debug)]
enum SubscriberOp {
    Open {
        key: u8,
        objects: u32,
        bytes: u32,
    },
    Grant {
        key: u8,
        objects: u32,
        bytes: u32,
    },
    Close {
        key: u8,
    },
    Page {
        key: u8,
        objects: u32,
        bytes: usize,
    },
    /// Accept through the unaccepted page at this index, if any.
    Accept {
        key: u8,
        index: usize,
    },
    End {
        key: u8,
    },
    Precheck {
        ends: bool,
        bytes: usize,
    },
}

fn subscriber_op() -> impl Strategy<Value = SubscriberOp> {
    let key = 0u8..4;
    prop_oneof![
        (key.clone(), 0u32..6, 0u32..80).prop_map(|(key, objects, bytes)| SubscriberOp::Open {
            key,
            objects,
            bytes
        }),
        (key.clone(), 0u32..4, 0u32..40).prop_map(|(key, objects, bytes)| SubscriberOp::Grant {
            key,
            objects,
            bytes
        }),
        key.clone().prop_map(|key| SubscriberOp::Close { key }),
        (key.clone(), 0u32..3, 0usize..40).prop_map(|(key, objects, bytes)| SubscriberOp::Page {
            key,
            objects,
            bytes
        }),
        (key.clone(), 0usize..4).prop_map(|(key, index)| SubscriberOp::Accept { key, index }),
        key.prop_map(|key| SubscriberOp::End { key }),
        (any::<bool>(), 0usize..80)
            .prop_map(|(ends, bytes)| SubscriberOp::Precheck { ends, bytes }),
    ]
}

/// The subscriber's model of one subscription.
#[derive(Debug, Default)]
struct Modeled {
    granted: Totals,
    consumed: Totals,
    accepted: Totals,
    /// Received pages not yet accepted: cursor and consumed totals through it.
    unaccepted: VecDeque<(u32, Totals)>,
    next_cursor: u32,
    closed: bool,
    pages: u64,
}

proptest! {
    /// Every subscriber operation succeeds or fails exactly as a model says.
    /// Pages spend their exact credit, pages after `Close` within credit are
    /// admitted, a terminal outcome spends nothing, and unsolicited or
    /// over-credit pages fault. Local refusals change nothing.
    #[test]
    fn subscriber_operation_sequences_follow_the_model(
        max_live in 1u32..3,
        ops in prop::collection::vec(subscriber_op(), 1..120),
    ) {
        let limits = limits(max_live);
        let mut tool = Subscriptions::<u8, u32>::new(limits, RULES);
        let mut model: HashMap<u8, Modeled> = HashMap::new();
        let mut ended: VecDeque<u8> = VecDeque::new();
        for op in ops {
            match op {
                SubscriberOp::Open { key, objects, bytes } => {
                    let expected = if model.len() >= max_live as usize {
                        Err(SubscribeRefused::AtCapacity)
                    } else if model.contains_key(&key) || ended.contains(&key) {
                        Err(SubscribeRefused::KeyInUse)
                    } else if objects == 0 || bytes == 0 {
                        Err(SubscribeRefused::EmptyCredit)
                    } else if objects > LIMIT.objects || bytes > LIMIT.bytes {
                        Err(SubscribeRefused::AboveWindow)
                    } else {
                        Ok(Update { sequence: 0, acknowledged: 0, added: credit(objects, bytes) })
                    };
                    prop_assert_eq!(tool.open(key, credit(objects, bytes), 0), expected.clone());
                    if expected.is_ok() {
                        model.insert(key, Modeled {
                            granted: totals(objects.into(), bytes.into()),
                            next_cursor: 1,
                            ..Modeled::default()
                        });
                    }
                }
                SubscriberOp::Grant { key, objects, bytes } => {
                    let result = tool.grant(&key, credit(objects, bytes));
                    let Some(modeled) = model.get_mut(&key) else {
                        prop_assert_eq!(result, Err(SubscribeRefused::Unknown));
                        continue;
                    };
                    let after = totals(
                        modeled.granted.objects + u64::from(objects),
                        modeled.granted.bytes + u64::from(bytes),
                    );
                    let expected = if modeled.closed {
                        Err(SubscribeRefused::Closing)
                    } else if objects == 0 && bytes == 0 {
                        Err(SubscribeRefused::EmptyCredit)
                    } else if !within_window(after, modeled.accepted, LIMIT) {
                        Err(SubscribeRefused::AboveWindow)
                    } else {
                        Ok(())
                    };
                    prop_assert_eq!(result.as_ref().map(|_| ()), expected.as_ref().map(|_| ()));
                    if expected.is_ok() {
                        modeled.granted = after;
                    }
                }
                SubscriberOp::Close { key } => {
                    let result = tool.close(&key);
                    match model.get_mut(&key) {
                        None => prop_assert_eq!(result, Err(SubscribeRefused::Unknown)),
                        Some(modeled) if modeled.closed => {
                            prop_assert_eq!(result, Err(SubscribeRefused::Closing));
                        }
                        Some(modeled) => {
                            prop_assert!(result.is_ok());
                            modeled.closed = true;
                        }
                    }
                }
                SubscriberOp::Page { key, objects, bytes } => {
                    let cursor = model.get(&key).map_or(0, |modeled| modeled.next_cursor);
                    let result = tool.claim_page(&key, objects, bytes, cursor);
                    let Some(modeled) = model.get_mut(&key) else {
                        prop_assert_eq!(result, Err(SubscriptionFault::Unknown));
                        continue;
                    };
                    let fits = objects >= 1
                        && u64::from(objects) <= modeled.granted.objects - modeled.consumed.objects
                        && bytes as u64 <= modeled.granted.bytes - modeled.consumed.bytes;
                    if !fits {
                        prop_assert_eq!(result, Err(SubscriptionFault::OverCredit));
                        continue;
                    }
                    // A page after `Close` within credit is admitted.
                    prop_assert_eq!(result, Ok(()));
                    modeled.consumed = totals(
                        modeled.consumed.objects + u64::from(objects),
                        modeled.consumed.bytes + bytes as u64,
                    );
                    modeled.unaccepted.push_back((cursor, modeled.consumed));
                    modeled.next_cursor += 1;
                    modeled.pages += 1;
                }
                SubscriberOp::Accept { key, index } => {
                    let Some(modeled) = model.get_mut(&key) else {
                        prop_assert_eq!(tool.accept(&key, &0), Err(SubscribeRefused::Unknown));
                        continue;
                    };
                    let Some(&(cursor, through)) = modeled.unaccepted.get(index) else {
                        prop_assert_eq!(
                            tool.accept(&key, &u32::MAX),
                            Err(SubscribeRefused::UnknownCursor)
                        );
                        continue;
                    };
                    prop_assert_eq!(tool.accept(&key, &cursor), Ok(()));
                    modeled.unaccepted.drain(..=index);
                    modeled.accepted = through;
                }
                SubscriberOp::End { key } => {
                    let result = tool.claim_end(&key);
                    let Some(modeled) = model.remove(&key) else {
                        prop_assert_eq!(result, Err(SubscriptionFault::Unknown));
                        continue;
                    };
                    // The terminal outcome needs no credit.
                    prop_assert_eq!(result, Ok(SubscriptionEnded {
                        pages: modeled.pages,
                        close_sent: modeled.closed,
                        received: modeled.next_cursor - 1,
                    }));
                    ended.push_back(key);
                    if ended.len() > max_live as usize {
                        ended.pop_front();
                    }
                }
                SubscriberOp::Precheck { ends, bytes } => {
                    let message_type = if ends { message_type::ENDED } else { message_type::PAGE };
                    let expected = if model.is_empty() {
                        Err(SubscriptionFault::Unknown)
                    } else if ends || model.values().any(|modeled| {
                        modeled.granted.objects > modeled.consumed.objects
                            && bytes as u64 <= modeled.granted.bytes - modeled.consumed.bytes
                    }) {
                        Ok(())
                    } else {
                        Err(SubscriptionFault::OverCredit)
                    };
                    prop_assert_eq!(tool.precheck(message_type, bytes), expected);
                }
            }
            prop_assert_eq!(tool.len(), model.len());
            for (key, modeled) in &model {
                let credit_ = tool.credit(key).expect("the model and the tool hold the same keys");
                prop_assert_eq!(credit_.granted(), modeled.granted);
                prop_assert_eq!(credit_.consumed(), modeled.consumed);
                prop_assert!(within_window(modeled.granted, modeled.accepted, LIMIT));
            }
        }
    }
}

// The publisher.

/// Open `key` with the whole window, starting at cursor 0.
fn opened(max_live: u32, keys: &[u8]) -> Publications<u8, u32> {
    let mut publications = Publications::new(limits(max_live));
    for &key in keys {
        publications.open(key, 0, LIMIT, 0).unwrap();
    }
    publications
}

#[test]
fn every_publisher_outcome_row() {
    use SubscriptionFault as F;

    // `Open` with no free slot: two slots per live subscription allowed, and
    // an ended one keeps its slot until its terminal permit drops.
    let mut publications = opened(1, &[1]);
    let first = publications.end(&1).unwrap();
    publications.open(2, 0, LIMIT, 0).unwrap();
    assert_eq!(publications.over_limit_count(), 1);
    let second = publications.end(&2).unwrap();
    assert_eq!(
        publications.open(3, 0, LIMIT, 0),
        Err(F::NoSlot { held: 2, limit: 1 })
    );
    drop((first, second));
    publications.open(3, 0, LIMIT, 0).unwrap();

    // `Open` reusing a live or tombstoned key.
    let mut publications = opened(2, &[1]);
    assert_eq!(publications.open(1, 0, LIMIT, 0), Err(F::ReusedKey));
    let _permit = publications.end(&1).unwrap();
    assert_eq!(publications.open(1, 0, LIMIT, 0), Err(F::ReusedKey));

    // `Open` or `Grant` with no credit, or above the window.
    let mut publications = opened(1, &[]);
    assert_eq!(
        publications.open(1, 0, credit(0, 8), 0),
        Err(F::EmptyCredit)
    );
    assert_eq!(
        publications.open(1, 0, credit(8, 0), 0),
        Err(F::EmptyCredit)
    );
    assert_eq!(
        publications.open(1, 0, credit(5, 8), 0),
        Err(F::AboveWindow)
    );
    assert_eq!(
        publications.open(1, 1, LIMIT, 0),
        Err(F::Sequence {
            expected: 0,
            got: 1
        })
    );
    let mut publications = opened(1, &[1]);
    assert_eq!(
        publications.grant(&1, 1, &0, credit(0, 0)),
        Err(F::EmptyCredit)
    );
    assert_eq!(
        publications.grant(&1, 1, &0, credit(1, 0)),
        Err(F::AboveWindow)
    );

    // A sequence that is not the previous plus one.
    assert_eq!(
        publications.grant(&1, 2, &0, credit(1, 0)),
        Err(F::Sequence {
            expected: 1,
            got: 2
        })
    );

    // An acknowledgement that names no sent page.
    publications.reserve_page(&1, 1, 8, 1).unwrap();
    assert_eq!(
        publications.grant(&1, 1, &7, credit(1, 0)),
        Err(F::UnknownAcknowledgement)
    );
    assert_eq!(
        publications.grant(&1, 1, &1, credit(1, 0)),
        Ok(Applied::Live)
    );

    // `Grant` or a second `Close` after `Close`.
    assert_eq!(publications.close(&1, 2, &1), Ok(Applied::Live));
    assert_eq!(
        publications.grant(&1, 3, &1, credit(1, 0)),
        Err(F::AfterClose)
    );
    assert_eq!(publications.close(&1, 3, &1), Err(F::AfterClose));
    assert_eq!(
        publications.reserve_page(&1, 1, 8, 2),
        Err(PageStall::Closing)
    );

    // A crossed `Grant` is dropped and keeps the tombstone; a crossed `Close`
    // is dropped and consumes it. Both still follow the sequence.
    let mut publications = opened(1, &[1]);
    let _permit = publications.end(&1).unwrap();
    assert_eq!(
        publications.grant(&1, 1, &0, credit(1, 0)),
        Ok(Applied::Crossed)
    );
    assert_eq!(publications.tombstones(), 1);
    assert_eq!(
        publications.close(&1, 1, &0),
        Err(F::Sequence {
            expected: 2,
            got: 1
        })
    );
    assert_eq!(publications.close(&1, 2, &0), Ok(Applied::Crossed));
    assert_eq!(publications.tombstones(), 0);

    // An update for no live or tombstoned key.
    assert_eq!(publications.grant(&1, 3, &0, credit(1, 0)), Err(F::Unknown));
    assert_eq!(publications.close(&9, 1, &0), Err(F::Unknown));

    // No credit, or a closing subscription, stalls production without fault.
    let mut publications = opened(1, &[1]);
    publications.reserve_page(&1, 4, 8, 1).unwrap();
    assert_eq!(
        publications.reserve_page(&1, 1, 1, 2),
        Err(PageStall::NoCredit)
    );
    assert_eq!(
        publications.reserve_page(&1, 0, 1, 2),
        Err(PageStall::EmptyPage)
    );
    assert_eq!(
        publications.reserve_page(&9, 1, 1, 2),
        Err(PageStall::Ended)
    );
}

#[test]
fn the_next_open_clears_tombstones_the_subscriber_has_passed() {
    // One live subscription: the next `Open` clears the tombstone, so the old
    // key may return after it, as header sync version 9 allows.
    let mut publications = opened(1, &[1]);
    drop(publications.end(&1));
    publications.open(2, 0, LIMIT, 0).unwrap();
    assert_eq!(publications.tombstones(), 0);
    drop(publications.end(&2));
    publications.open(1, 0, LIMIT, 0).unwrap();

    // Two live: with one other subscription live, an `Open` proves the
    // subscriber received every earlier terminal outcome.
    let mut publications = opened(2, &[1, 2]);
    drop(publications.end(&1));
    publications.open(3, 0, LIMIT, 0).unwrap();
    assert_eq!(publications.tombstones(), 0);
    // With none live, the newest tombstone stays: its terminal outcome may be
    // in flight while the subscriber opens its second subscription.
    drop(publications.end(&2));
    drop(publications.end(&3));
    publications.open(4, 0, LIMIT, 0).unwrap();
    assert_eq!(publications.tombstones(), 1);
    assert_eq!(
        publications.grant(&3, 1, &0, credit(1, 0)),
        Ok(Applied::Crossed)
    );
    assert_eq!(
        publications.grant(&2, 1, &0, credit(1, 0)),
        Err(SubscriptionFault::Unknown)
    );
}

#[derive(Clone, Debug)]
enum PublisherOp {
    Open {
        key: u8,
        sequence: u32,
        objects: u32,
        bytes: u32,
        start: u32,
    },
    Grant {
        key: u8,
        sequence: u32,
        acknowledged: u32,
        objects: u32,
        bytes: u32,
    },
    Close {
        key: u8,
        sequence: u32,
        acknowledged: u32,
    },
    Page {
        key: u8,
        objects: u32,
        bytes: usize,
        cursor: u32,
    },
    End {
        key: u8,
    },
    /// Drop the oldest terminal permit: its write finished.
    Written,
}

fn publisher_op() -> impl Strategy<Value = PublisherOp> {
    let key = 0u8..4;
    let sequence = 0u32..4;
    let cursor = 0u32..8;
    prop_oneof![
        (key.clone(), 0u32..2, 0u32..6, 0u32..80, cursor.clone()).prop_map(
            |(key, sequence, objects, bytes, start)| PublisherOp::Open {
                key,
                sequence,
                objects,
                bytes,
                start
            }
        ),
        (
            key.clone(),
            sequence.clone(),
            cursor.clone(),
            0u32..3,
            0u32..40
        )
            .prop_map(
                |(key, sequence, acknowledged, objects, bytes)| PublisherOp::Grant {
                    key,
                    sequence,
                    acknowledged,
                    objects,
                    bytes
                }
            ),
        (key.clone(), sequence, cursor.clone()).prop_map(|(key, sequence, acknowledged)| {
            PublisherOp::Close {
                key,
                sequence,
                acknowledged,
            }
        }),
        (key.clone(), 0u32..3, 0usize..40, cursor).prop_map(|(key, objects, bytes, cursor)| {
            PublisherOp::Page {
                key,
                objects,
                bytes,
                cursor,
            }
        }),
        key.prop_map(|key| PublisherOp::End { key }),
        Just(PublisherOp::Written),
    ]
}

proptest! {
    /// Under any update sequence, a hostile one included, a refused update
    /// changes nothing, and live subscriptions, tombstones, and the cursor
    /// history stay within their bounds.
    #[test]
    fn publisher_operation_sequences_keep_every_bound(
        max_live in 1u32..3,
        ops in prop::collection::vec(publisher_op(), 1..150),
    ) {
        let bound = 2 * max_live as usize;
        let mut publications = Publications::<u8, u32>::new(limits(max_live));
        let mut permits = VecDeque::new();
        for op in ops {
            let before = format!("{publications:?}");
            let result = match op {
                PublisherOp::Open { key, sequence, objects, bytes, start } => publications
                    .open(key, sequence, credit(objects, bytes), start)
                    .map(|()| Applied::Live),
                PublisherOp::Grant { key, sequence, acknowledged, objects, bytes } => {
                    publications.grant(&key, sequence, &acknowledged, credit(objects, bytes))
                }
                PublisherOp::Close { key, sequence, acknowledged } => {
                    publications.close(&key, sequence, &acknowledged)
                }
                PublisherOp::Page { key, objects, bytes, cursor } => {
                    let _ = publications.reserve_page(&key, objects, bytes, cursor);
                    Ok(Applied::Live)
                }
                PublisherOp::End { key } => {
                    permits.extend(publications.end(&key));
                    Ok(Applied::Live)
                }
                PublisherOp::Written => {
                    permits.pop_front();
                    Ok(Applied::Live)
                }
            };
            if result.is_err() {
                prop_assert_eq!(format!("{publications:?}"), before);
            }
            prop_assert!(publications.keys().count() + permits.len() <= bound);
            prop_assert!(publications.tombstones() <= bound);
            for key in publications.keys() {
                prop_assert!(
                    publications.unacknowledged(key).unwrap() <= LIMIT.objects as usize
                );
            }
        }
    }
}

// Both sides.

/// A frame from the publisher to the subscriber.
#[derive(Debug)]
enum Down {
    Page {
        key: u8,
        objects: u32,
        bytes: usize,
        cursor: u32,
    },
    /// The permit stands for the terminal outcome's pending write.
    Ended { key: u8, _permit: TerminalPermit },
}

/// An update from the subscriber to the publisher.
#[derive(Debug)]
enum Up {
    Open { key: u8, update: Update<u32> },
    Grant { key: u8, update: Update<u32> },
    Close { key: u8, update: Update<u32> },
}

#[derive(Clone, Debug)]
enum Step {
    /// The subscriber opens its next key.
    Open {
        objects: u32,
        bytes: u32,
    },
    /// The subscriber tries to grant this credit; its tool refuses a grant
    /// beyond the window.
    Grant {
        pick: usize,
        objects: u32,
        bytes: u32,
    },
    Close {
        pick: usize,
    },
    /// The handler accepts every received page of one subscription.
    Accept {
        pick: usize,
    },
    /// The publisher produces a page.
    Produce {
        pick: usize,
        objects: u32,
        bytes: usize,
    },
    /// The publisher ends a subscription: a closing one if any, or its own
    /// choice.
    End {
        pick: usize,
    },
    DeliverUp,
    DeliverDown,
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        1 => (1u32..5, 1u32..65).prop_map(|(objects, bytes)| Step::Open { objects, bytes }),
        3 => (any::<usize>(), 0u32..5, 0u32..65)
            .prop_map(|(pick, objects, bytes)| Step::Grant { pick, objects, bytes }),
        1 => any::<usize>().prop_map(|pick| Step::Close { pick }),
        1 => any::<usize>().prop_map(|pick| Step::Accept { pick }),
        4 => (any::<usize>(), 1u32..3, 1usize..24)
            .prop_map(|(pick, objects, bytes)| Step::Produce { pick, objects, bytes }),
        1 => any::<usize>().prop_map(|pick| Step::End { pick }),
        4 => Just(Step::DeliverUp),
        4 => Just(Step::DeliverDown),
    ]
}

fn pick<T: Copy + Ord>(keys: impl Iterator<Item = T>, pick: usize) -> Option<T> {
    let mut keys: Vec<T> = keys.collect();
    keys.sort_unstable();
    (!keys.is_empty()).then(|| keys[pick % keys.len()])
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// A conformant subscriber and publisher, joined by two ordered queues,
    /// never fault each other under any interleaving of updates, pages, and
    /// terminal outcomes. The cursor history never fills, and both ledgers
    /// agree once the queues drain.
    #[test]
    fn crossed_updates_never_fault_a_conformant_peer(
        max_live in 1u32..4,
        steps in prop::collection::vec(step(), 1..250),
    ) {
        let limits = limits(max_live);
        let mut subscriptions = Subscriptions::<u8, u32>::new(limits, RULES);
        let mut publications = Publications::<u8, u32>::new(limits);
        let mut up = VecDeque::new();
        let mut down = VecDeque::new();
        let mut next_key = 0u8;
        let mut next_cursor: HashMap<u8, u32> = HashMap::new();
        let mut received: HashMap<u8, u32> = HashMap::new();
        let deliver_up = |publications: &mut Publications<u8, u32>, update: Up| match update {
            Up::Open { key, update } => publications
                .open(key, update.sequence, update.added, update.acknowledged)
                .map(|()| Applied::Live),
            Up::Grant { key, update } => {
                publications.grant(&key, update.sequence, &update.acknowledged, update.added)
            }
            Up::Close { key, update } => {
                publications.close(&key, update.sequence, &update.acknowledged)
            }
        };
        let deliver_down = |subscriptions: &mut Subscriptions<u8, u32>,
                            received: &mut HashMap<u8, u32>,
                            frame: Down| match frame {
            Down::Page { key, objects, bytes, cursor } => {
                subscriptions.precheck(message_type::PAGE, bytes)?;
                subscriptions.claim_page(&key, objects, bytes, cursor)?;
                received.insert(key, cursor);
                Ok::<_, SubscriptionFault>(())
            }
            Down::Ended { key, .. } => {
                subscriptions.precheck(message_type::ENDED, 1)?;
                subscriptions.claim_end(&key)?;
                received.remove(&key);
                Ok(())
            }
        };
        let steps = steps.into_iter().chain(
            std::iter::repeat_n(Step::DeliverUp, 300).chain(std::iter::repeat_n(Step::DeliverDown, 300)),
        );
        for step in steps {
            match step {
                Step::Open { objects, bytes } => {
                    if let Ok(update) = subscriptions.open(next_key, credit(objects, bytes), 0) {
                        up.push_back(Up::Open { key: next_key, update });
                        next_cursor.insert(next_key, 1);
                    }
                    next_key = next_key.wrapping_add(1);
                }
                Step::Grant { pick: choice, objects, bytes } => {
                    let Some(key) = pick(received.keys().copied(), choice) else { continue };
                    if let Ok(update) = subscriptions.grant(&key, credit(objects, bytes)) {
                        up.push_back(Up::Grant { key, update });
                    }
                }
                Step::Close { pick: choice } => {
                    let Some(key) = pick(received.keys().copied(), choice) else { continue };
                    if let Ok(update) = subscriptions.close(&key) {
                        up.push_back(Up::Close { key, update });
                    }
                }
                Step::Accept { pick: choice } => {
                    let Some(key) = pick(received.keys().copied(), choice) else { continue };
                    let cursor = received[&key];
                    // The start names no page, and an accepted page is gone.
                    if cursor != 0 {
                        let accepted = subscriptions.accept(&key, &cursor);
                        prop_assert!(matches!(
                            accepted,
                            Ok(()) | Err(SubscribeRefused::UnknownCursor)
                        ));
                    }
                }
                Step::Produce { pick: choice, objects, bytes } => {
                    let Some(key) = pick(publications.keys().copied(), choice) else { continue };
                    let cursor = next_cursor[&key];
                    match publications.reserve_page(&key, objects, bytes, cursor) {
                        Ok(()) => {
                            next_cursor.insert(key, cursor + 1);
                            down.push_back(Down::Page { key, objects, bytes, cursor });
                        }
                        Err(PageStall::HistoryFull) => {
                            prop_assert!(false, "the cursor history filled");
                        }
                        Err(_) => {}
                    }
                }
                Step::End { pick: choice } => {
                    let closing: Vec<u8> = publications
                        .keys()
                        .copied()
                        .filter(|key| publications.is_closing(key))
                        .collect();
                    let key = match pick(closing.into_iter(), choice) {
                        Some(key) => key,
                        None => match pick(publications.keys().copied(), choice) {
                            Some(key) => key,
                            None => continue,
                        },
                    };
                    let permit = publications.end(&key).expect("a live key ends once");
                    down.push_back(Down::Ended { key, _permit: permit });
                }
                Step::DeliverUp => {
                    if let Some(update) = up.pop_front() {
                        let result = deliver_up(&mut publications, update);
                        prop_assert!(result.is_ok(), "the publisher faulted: {:?}", result);
                    }
                }
                Step::DeliverDown => {
                    if let Some(frame) = down.pop_front() {
                        let result = deliver_down(&mut subscriptions, &mut received, frame);
                        prop_assert!(result.is_ok(), "the subscriber faulted: {:?}", result);
                    }
                }
            }
            // `received` tracks the subscriber's live keys, pages or not.
            for key in subscriptions_keys(&subscriptions, next_key) {
                received.entry(key).or_insert(0);
            }
        }
        prop_assert!(up.is_empty() && down.is_empty());
        for key in publications.keys() {
            let sent = publications.credit(key).unwrap();
            let got = subscriptions.credit(key).expect("a live publication is a live subscription");
            prop_assert_eq!(sent.consumed(), got.consumed());
            prop_assert_eq!(sent.granted(), got.granted());
        }
    }
}

fn subscriptions_keys(subscriptions: &Subscriptions<u8, u32>, next_key: u8) -> Vec<u8> {
    (0..next_key)
        .filter(|key| subscriptions.contains(key))
        .collect()
}

#[test]
fn a_grant_beyond_the_window_faults() {
    let limits = limits(1);
    let mut subscriptions = Subscriptions::<u8, u32>::new(limits, RULES);
    let mut publications = Publications::<u8, u32>::new(limits);
    subscriptions.open(1, LIMIT, 0).unwrap();
    publications.open(1, 0, LIMIT, 0).unwrap();
    for cursor in 1..=2 {
        publications.reserve_page(&1, 1, 8, cursor).unwrap();
        subscriptions.claim_page(&1, 1, 8, cursor).unwrap();
    }
    // Two pages are unspent credit to neither side and unacknowledged to both:
    // the window is full.
    assert_eq!(subscriptions.grantable(&1), Some(credit(0, 0)));
    assert_eq!(
        subscriptions.grant(&1, credit(1, 0)),
        Err(SubscribeRefused::AboveWindow)
    );
    assert_eq!(
        publications.grant(&1, 1, &0, credit(1, 0)),
        Err(SubscriptionFault::AboveWindow)
    );
    // Accepting a page reopens the window by that page, on both sides.
    subscriptions.accept(&1, &1).unwrap();
    assert_eq!(subscriptions.grantable(&1), Some(credit(1, 8)));
    let update = subscriptions.grant(&1, credit(1, 8)).unwrap();
    assert_eq!(update.acknowledged, 1);
    assert_eq!(
        publications.grant(&1, update.sequence, &update.acknowledged, update.added),
        Ok(Applied::Live)
    );
    assert_eq!(
        publications.grant(&1, 2, &1, credit(0, 1)),
        Err(SubscriptionFault::AboveWindow)
    );
}

#[test]
fn a_slow_acknowledger_is_held_by_the_window() {
    let limits = limits(1);
    let mut subscriptions = Subscriptions::<u8, u32>::new(limits, RULES);
    let mut publications = Publications::<u8, u32>::new(limits);
    subscriptions.open(1, LIMIT, 0).unwrap();
    publications.open(1, 0, LIMIT, 0).unwrap();
    let mut cursor = 0;
    for round in 0..8 {
        // The publisher sends until credit runs out, never past the history.
        loop {
            match publications.reserve_page(&1, 1, 1, cursor + 1) {
                Ok(()) => {
                    cursor += 1;
                    subscriptions.claim_page(&1, 1, 1, cursor).unwrap();
                }
                Err(stall) => {
                    assert_eq!(stall, PageStall::NoCredit);
                    break;
                }
            }
            assert!(publications.unacknowledged(&1).unwrap() <= LIMIT.objects as usize);
        }
        // The handler accepts only every other round; until it does, the
        // window allows no grant.
        if round % 2 == 1 {
            subscriptions.accept(&1, &cursor).unwrap();
        }
        match subscriptions.grant(&1, credit(LIMIT.objects, 0)) {
            Ok(update) => assert_eq!(
                publications.grant(&1, update.sequence, &update.acknowledged, update.added),
                Ok(Applied::Live)
            ),
            Err(refused) => assert_eq!(refused, SubscribeRefused::AboveWindow),
        }
    }
    assert!(cursor >= 16);
}

fn peer() -> ZakuraPeerId {
    ZakuraPeerId::new(vec![7; 32]).expect("32 bytes is a valid peer id")
}

fn capacity() -> ServeCapacity {
    ServeCapacity::new(
        "test",
        &RULES[0],
        ServeLimits {
            node_execution: 1,
            peer_execution: 1,
            peer_output_bytes: 1024,
            node_output_bytes: 1024,
        },
    )
    .expect("the limits are positive")
}

fn frame(message_type: u16) -> Frame {
    Frame {
        message_type,
        flags: 0,
        payload: vec![0],
    }
}

#[tokio::test(start_paused = true)]
async fn an_idle_subscription_holds_nothing_and_needs_no_outcome() {
    let limits = limits(1);
    let capacity = capacity();
    let push = capacity.push(&peer());
    let mut subscriptions = Subscriptions::<u8, u32>::new(limits, RULES);
    let mut publications = Publications::<u8, u32>::new(limits);
    let update = subscriptions.open(1, credit(1, 10), 0).unwrap();
    publications
        .open(1, update.sequence, update.added, update.acknowledged)
        .unwrap();
    assert_eq!(
        subscriptions.grant(&1, credit(0, 0)),
        Err(SubscribeRefused::EmptyCredit)
    );

    // One page, then an idle day with no page and no outcome.
    let permit = push.acquire(10).await;
    publications.reserve_page(&1, 1, 10, 1).unwrap();
    let (send, mut recv) = framed_channel(4);
    assert!(permit.send(&send, frame(message_type::PAGE)).await);
    subscriptions.claim_page(&1, 1, 10, 1).unwrap();
    assert_eq!(
        subscriptions.claim_page(&1, 1, 1, 2),
        Err(SubscriptionFault::OverCredit)
    );
    drop(recv.recv().await);
    tokio::time::advance(Duration::from_secs(24 * 60 * 60)).await;
    assert_eq!(capacity.node_execution_held(), 0);
    assert_eq!(capacity.peer_held(&peer()), (0, 0));
    assert_eq!(
        publications.reserve_page(&1, 1, 1, 2),
        Err(PageStall::NoCredit)
    );

    // The subscription is still live on both sides and renews.
    subscriptions.accept(&1, &1).unwrap();
    let update = subscriptions.grant(&1, credit(2, 20)).unwrap();
    assert_eq!(
        publications.grant(&1, update.sequence, &update.acknowledged, update.added),
        Ok(Applied::Live)
    );
    let terminal = publications.end(&1).unwrap();
    assert!(publications.end(&1).is_none());
    drop(terminal);
    publications.open(2, 0, LIMIT, 0).unwrap();
}

#[tokio::test]
async fn close_needs_no_credit_and_no_execution() {
    let limits = limits(1);
    let capacity = capacity();
    let mut subscriptions = Subscriptions::<u8, u32>::new(limits, RULES);
    let mut publications = Publications::<u8, u32>::new(limits);
    let update = subscriptions.open(1, credit(2, 20), 0).unwrap();
    publications
        .open(1, update.sequence, update.added, update.acknowledged)
        .unwrap();
    // The publisher spends every credit on a page that crosses `Close`.
    publications.reserve_page(&1, 2, 20, 1).unwrap();
    let close = subscriptions.close(&1).unwrap();
    assert_eq!(
        subscriptions.grant(&1, credit(1, 1)),
        Err(SubscribeRefused::Closing)
    );
    assert_eq!(
        publications.close(&1, close.sequence, &close.acknowledged),
        Ok(Applied::Live)
    );
    assert_eq!(
        publications.reserve_page(&1, 1, 1, 2),
        Err(PageStall::Closing)
    );
    // The crossed page reaches the handler.
    subscriptions.precheck(message_type::PAGE, 20).unwrap();
    subscriptions.claim_page(&1, 2, 20, 1).unwrap();

    // Execution and output are held, yet the terminal outcome is written.
    let _held = capacity.hold_node_for_test();
    let push = capacity.push(&peer());
    assert!(push.acquire(1).now_or_never().is_none());
    let (send, mut recv) = framed_channel(1);
    let terminal = publications.end(&1).unwrap();
    assert_eq!(
        terminal
            .send(&send, frame(message_type::ENDED))
            .now_or_never(),
        Some(true)
    );
    subscriptions.precheck(message_type::ENDED, 1).unwrap();
    let ended = subscriptions.claim_end(&1).unwrap();
    assert!(ended.close_sent);
    assert_eq!(ended.pages, 1);
    assert_eq!(
        subscriptions.claim_page(&1, 1, 1, 2),
        Err(SubscriptionFault::Unknown)
    );

    // The slot returns once the terminal outcome's write finishes.
    publications.open(2, 0, LIMIT, 0).unwrap();
    assert_eq!(publications.over_limit_count(), 1);
    drop(recv.recv().await);
    drop(publications.end(&2));
    publications.open(3, 0, LIMIT, 0).unwrap();
    assert_eq!(publications.over_limit_count(), 1);
}

#[test]
fn retirement_with_a_live_subscription_closes_the_connection() {
    for ended in [false, true] {
        let connection = CancellationToken::new();
        let cause = CloseCause::new();
        let fence = WriterFence::new(connection.clone(), cause.clone());
        let mut subscriptions = Subscriptions::<u8, u32>::new(limits(1), RULES);
        let exchange = fence.open().unwrap();
        let writer = exchange.writer();
        subscriptions
            .open_fenced(1, credit(1, 1), 0, exchange)
            .unwrap();
        // The `Open` reaches its first byte.
        assert!(writer.publish(|| {}));
        assert!(writer.try_start(|| true));
        if ended {
            subscriptions.claim_end(&1).unwrap();
        }
        assert_eq!(fence.retire(), ended);
        assert_eq!(connection.is_cancelled(), !ended);
        if !ended {
            assert_eq!(cause.get_or("unset"), UNFINISHED_EXCHANGE);
        }
        assert_eq!(
            subscriptions.grant(&1, credit(1, 1)).is_ok(),
            !ended,
            "the table stays usable; only the connection closes"
        );
    }
}
