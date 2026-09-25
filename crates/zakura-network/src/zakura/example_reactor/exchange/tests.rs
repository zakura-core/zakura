//! The example reactor exchanging items in process over both layouts.

use std::{collections::VecDeque, time::Duration};

use super::*;
use crate::zakura::{
    example_reactor::{PAIRED, SINGLE},
    regulation::{sizing, ReservationPool},
};

fn peer(n: u8) -> ZakuraPeerId {
    ZakuraPeerId::new(vec![n; 32]).expect("32 bytes is a valid peer id")
}

fn ranges(heights: std::ops::Range<u32>) -> VecDeque<ItemRange> {
    heights
        .step_by(8)
        .map(|start| ItemRange {
            start: Height(start),
            count: 8,
        })
        .collect()
}

/// The node defaults, derived from the throughput target.
fn capacity() -> ServeCapacity {
    let largest = range_cap(ItemRange {
        start: Height(0),
        count: super::super::MAX_ITEMS_PER_REQUEST,
    });
    let limits = sizing::serve_limits(largest.output_bytes(), Duration::from_millis(1));
    ServeCapacity::new("example", &GET_ITEMS, limits).expect("derived limits are valid")
}

fn pool() -> ReservationPool {
    let smallest = range_cap(ItemRange {
        start: Height(0),
        count: 1,
    });
    ReservationPool::new(sizing::reservation_entries(smallest.bytes))
        .expect("derived entries are valid")
}

/// Two nodes that each download the other's items at the same time.
async fn exchange(layout: &'static [Stream]) {
    let capacity = capacity();
    let pool = pool();
    let (a_link, b_link) = connect(layout);
    let cancel = CancellationToken::new();
    let store = |low, high| {
        Arc::new(Store {
            low: Height(low),
            high: Height(high),
        })
    };
    let mut a = ExampleNode::new(
        layout,
        &capacity,
        store(0, 95),
        &peer(2),
        a_link.sends,
        cancel.clone(),
    );
    let mut b = ExampleNode::new(
        layout,
        &capacity,
        store(96, 191),
        &peer(1),
        b_link.sends,
        cancel.clone(),
    );
    let mut a_in = inbound(a_link.recvs);
    let mut b_in = inbound(b_link.recvs);
    let mut a_wants = ranges(96..192);
    // One range B does not hold.
    a_wants.push_back(ItemRange {
        start: Height(300),
        count: 8,
    });
    let mut b_wants = ranges(0..96);
    assert_eq!(b.announce(Height(96), Height(191)), Ok(1));

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            for (node, wants) in [(&mut a, &mut a_wants), (&mut b, &mut b_wants)] {
                while node.has_room() {
                    let Some(range) = wants.pop_front() else {
                        break;
                    };
                    node.request(range, pool.entry().await)
                        .expect("a node requests only within its room");
                }
            }
            if a_wants.is_empty() && b_wants.is_empty() && a.idle() && b.idle() {
                break;
            }
            tokio::select! {
                Some(frame) = a_in.next() => a.handle(frame).expect("B is conformant"),
                Some(frame) = b_in.next() => b.handle(frame).expect("A is conformant"),
            }
        }
    })
    .await
    .expect("both downloads finish");

    let items = |heights: std::ops::Range<u32>| -> Vec<_> {
        heights
            .map(|h| (Height(h), item_bytes(Height(h))))
            .collect()
    };
    assert_eq!(a.received, items(96..192));
    assert_eq!(b.received, items(0..96));
    assert_eq!(
        a.unavailable,
        [ItemRange {
            start: Height(300),
            count: 8
        }]
    );
    assert_eq!(a.peer_status, Some((Height(96), Height(191))));
    assert_eq!((a.serving_open(), b.serving_open()), (0, 0));
    assert_eq!(pool.held(), 0);
    tokio::time::timeout(Duration::from_secs(5), async {
        while capacity.node_output_held() != 0 || capacity.node_execution_held() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("every output grant returns once its frames are delivered");
    cancel.cancel();
}

#[tokio::test]
async fn nodes_exchange_items_over_one_stream() {
    exchange(&SINGLE).await;
}

#[tokio::test]
async fn nodes_exchange_items_over_a_stream_pair() {
    exchange(&PAIRED).await;
}

fn node(layout: &'static [Stream]) -> (ExampleNode, Link, ServeCapacity) {
    let capacity = capacity();
    let (link, peer_link) = connect(layout);
    let node = ExampleNode::new(
        layout,
        &capacity,
        Arc::new(Store {
            low: Height(0),
            high: Height(99),
        }),
        &peer(2),
        link.sends,
        CancellationToken::new(),
    );
    (node, peer_link, capacity)
}

fn frame(message: &ExampleMessage) -> Frame {
    encode_frame(message).expect("the test message is valid")
}

fn item(height: u32) -> Frame {
    frame(&ExampleMessage::Item {
        height: Height(height),
        bytes: vec![1],
    })
}

#[tokio::test]
async fn an_unsolicited_item_is_refused_before_decode() {
    let (mut node, _peer, _capacity) = node(&SINGLE);
    assert_eq!(
        node.handle(item(5)),
        Err(Violation::Claim(ClaimRefused::Unsolicited {
            message_type: message_type::ITEM
        }))
    );
}

#[tokio::test]
async fn an_item_outside_every_requested_range_is_unsolicited() {
    let (mut node, _peer, _capacity) = node(&SINGLE);
    let pool = pool();
    node.request(
        ItemRange {
            start: Height(0),
            count: 8,
        },
        pool.try_entry().unwrap(),
    )
    .unwrap();
    assert_eq!(node.handle(item(7)), Ok(()));
    assert_eq!(
        node.handle(item(8)),
        Err(Violation::Claim(ClaimRefused::Unsolicited {
            message_type: message_type::ITEM
        }))
    );
}

#[tokio::test]
async fn an_ending_with_the_wrong_count_is_a_violation() {
    let (mut node, _peer, _capacity) = node(&PAIRED);
    let pool = pool();
    node.request(
        ItemRange {
            start: Height(0),
            count: 8,
        },
        pool.try_entry().unwrap(),
    )
    .unwrap();
    node.handle(item(0)).unwrap();
    assert_eq!(
        node.handle(frame(&ExampleMessage::ItemsDone {
            start: Height(0),
            returned: 2,
        })),
        Err(Violation::Count {
            reported: 2,
            received: 1,
        })
    );
}

#[tokio::test]
async fn requests_beyond_twice_the_limit_are_a_violation() {
    let (mut node, _peer, capacity) = node(&SINGLE);
    let request = frame(&ExampleMessage::GetItems(ItemRange {
        start: Height(0),
        count: 8,
    }));
    // The serving tasks have not run, so no ending has freed a commitment.
    for _ in 0..8 {
        node.handle(request.clone()).unwrap();
    }
    assert_eq!(capacity_over_limit(&capacity), 4);
    assert_eq!(
        node.handle(request),
        Err(Violation::Serve(ServeViolation::OverCommitted {
            open: 9,
            limit: 4
        }))
    );
}

fn capacity_over_limit(capacity: &ServeCapacity) -> u64 {
    capacity.over_limit_count()
}
