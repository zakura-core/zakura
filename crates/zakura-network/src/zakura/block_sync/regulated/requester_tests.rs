use tokio_util::sync::CancellationToken;
use zakura_chain::block::{Hash, Height};

use super::{
    requester::{RequestError, Requester, ResponseError},
    wire::{Message, Range},
};
use crate::zakura::{
    regulation::{ReservationPool, WriterFence},
    CloseCause,
};

fn fence() -> (WriterFence, CancellationToken) {
    let connection = CancellationToken::new();
    (
        WriterFence::new(connection.clone(), CloseCause::default()),
        connection,
    )
}

fn range(start: u32, count: u32) -> Range {
    Range::new(Height(start), count).unwrap()
}
fn hash(byte: u8) -> Hash {
    Hash([byte; 32])
}

#[test]
fn rejects_overlap_until_the_old_range_ends_even_after_abandonment() {
    let (fence, _) = fence();
    let pool = ReservationPool::new(3).unwrap();
    let mut book = Requester::new(3);
    book.reserve(
        range(10, 3),
        &[hash(1), hash(2), hash(3)],
        100,
        pool.try_entry().unwrap(),
        fence.open().unwrap(),
    )
    .unwrap();
    book.abandon(Height(10));
    assert!(matches!(
        book.reserve(
            range(12, 2),
            &[hash(4), hash(5)],
            100,
            pool.try_entry().unwrap(),
            fence.open().unwrap()
        ),
        Err(RequestError::Overlap)
    ));
    assert_eq!(pool.held(), 1);
    assert!(book.claim_block(hash(1), 3).unwrap().1.abandoned);
    let (_, ended) = book
        .finish(&Message::BlocksDone {
            start: Height(10),
            returned: 1,
        })
        .unwrap();
    assert!(ended.abandoned);
    assert_eq!(pool.held(), 0);
    book.reserve(
        range(12, 2),
        &[hash(4), hash(5)],
        100,
        pool.try_entry().unwrap(),
        fence.open().unwrap(),
    )
    .unwrap();
}

#[test]
fn only_the_next_hash_can_claim_each_range() {
    let (fence, _) = fence();
    let pool = ReservationPool::new(2).unwrap();
    let mut book = Requester::new(2);
    book.reserve(
        range(10, 2),
        &[hash(1), hash(2)],
        100,
        pool.try_entry().unwrap(),
        fence.open().unwrap(),
    )
    .unwrap();
    book.reserve(
        range(20, 1),
        &[hash(3)],
        100,
        pool.try_entry().unwrap(),
        fence.open().unwrap(),
    )
    .unwrap();
    assert!(matches!(
        book.claim_block(hash(2), 3),
        Err(ResponseError::Identity)
    ));
    assert_eq!(book.claim_block(hash(3), 3).unwrap().0, Height(20));
    assert_eq!(book.claim_block(hash(1), 3).unwrap().0, Height(10));
    assert!(matches!(
        book.claim_block(hash(1), 3),
        Err(ResponseError::Identity)
    ));
    assert_eq!(book.claim_block(hash(2), 3).unwrap().0, Height(11));
    book.finish(&Message::BlocksDone {
        start: Height(10),
        returned: 2,
    })
    .unwrap();
    assert!(matches!(
        book.claim_block(hash(2), 3),
        Err(ResponseError::Identity)
    ));
}

#[test]
fn rejects_body_overrun_even_if_unused_tag_allowance_would_fit_it() {
    let (fence, _) = fence();
    let pool = ReservationPool::new(1).unwrap();
    let mut book = Requester::new(1);
    book.reserve(
        range(1, 3),
        &[hash(1), hash(2), hash(3)],
        10,
        pool.try_entry().unwrap(),
        fence.open().unwrap(),
    )
    .unwrap();
    // The generic cap has 3 tag bytes, but the first body still cannot use 11.
    assert!(matches!(
        book.claim_block(hash(1), 12),
        Err(ResponseError::BodyBytes)
    ));
    assert_eq!(book.claim_block(hash(1), 11).unwrap().0, Height(1));
    assert!(matches!(
        book.claim_block(hash(2), 2),
        Err(ResponseError::BodyBytes)
    ));
    book.finish(&Message::BlocksDone {
        start: Height(1),
        returned: 1,
    })
    .unwrap();
    assert_eq!(pool.held(), 0);
}

#[test]
fn invalid_endings_preserve_authorization_and_started_exchange_ownership() {
    let (fence, connection) = fence();
    let pool = ReservationPool::new(1).unwrap();
    let mut book = Requester::new(1);
    let writer = book
        .reserve(
            range(1, 2),
            &[hash(1), hash(2)],
            100,
            pool.try_entry().unwrap(),
            fence.open().unwrap(),
        )
        .unwrap();
    assert!(writer.publish(|| {}));
    assert!(writer.try_start(|| true));
    assert!(book
        .finish(&Message::RangeUnavailable(range(1, 1)))
        .is_err());
    assert!(book
        .finish(&Message::BlocksDone {
            start: Height(1),
            returned: 1
        })
        .is_err());
    assert_eq!(pool.held(), 1);
    assert!(!connection.is_cancelled());
    book.claim_block(hash(1), 3).unwrap();
    assert!(book
        .finish(&Message::RangeUnavailable(range(1, 2)))
        .is_err());
    assert!(book
        .finish(&Message::BlocksDone {
            start: Height(1),
            returned: 2
        })
        .is_err());
    drop(book);
    assert!(
        connection.is_cancelled(),
        "an invalid ending must not end the writer fence"
    );
}

#[test]
fn valid_ending_allows_replacement_and_failed_publication_releases_capacity() {
    let (fence, connection) = fence();
    let pool = ReservationPool::new(1).unwrap();
    let mut book = Requester::new(1);
    book.reserve(
        range(1, 1),
        &[hash(1)],
        100,
        pool.try_entry().unwrap(),
        fence.open().unwrap(),
    )
    .unwrap();
    book.retract(Height(1));
    assert_eq!(pool.held(), 0);
    let writer = book
        .reserve(
            range(1, 1),
            &[hash(1)],
            100,
            pool.try_entry().unwrap(),
            fence.open().unwrap(),
        )
        .unwrap();
    assert!(writer.publish(|| {}));
    assert!(writer.try_start(|| true));
    book.claim_block(hash(1), 3).unwrap();
    book.finish(&Message::BlocksDone {
        start: Height(1),
        returned: 1,
    })
    .unwrap();
    assert!(fence.retire());
    assert!(!connection.is_cancelled());
    assert_eq!(pool.held(), 0);
}

#[test]
fn pool_and_session_counts_bound_bookkeeping_without_byte_funding() {
    let (fence, _) = fence();
    let pool = ReservationPool::new(2).unwrap();
    let mut a = Requester::new(1);
    let mut b = Requester::new(2);
    let expected: Vec<_> = (0..128).map(hash).collect();
    a.reserve(
        range(1, 128),
        &expected,
        100,
        pool.try_entry().unwrap(),
        fence.open().unwrap(),
    )
    .unwrap();
    assert!(matches!(
        a.reserve(
            range(129, 1),
            &[hash(2)],
            100,
            pool.try_entry().unwrap(),
            fence.open().unwrap()
        ),
        Err(RequestError::Capacity(_))
    ));
    b.reserve(
        range(1, 128),
        &expected,
        100,
        pool.try_entry().unwrap(),
        fence.open().unwrap(),
    )
    .unwrap();
    assert!(pool.try_entry().is_none());
    a.abandon(Height(1));
    assert!(pool.try_entry().is_none());
    drop(a);
    assert_eq!(pool.held(), 1);
}
