use zakura_chain::block::{Hash, Height};
use zakura_rt::sync::CancellationToken;

use super::{
    requester::{RequestError, Requester, ResponseError},
    wire::{Range, RangeEnding},
};
use crate::zakura::regulation::{ReservationPool, WriterFence};

fn fence() -> (WriterFence, CancellationToken) {
    let connection = CancellationToken::new();
    (WriterFence::new(connection.clone()), connection)
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
    let completed = book.finish(RangeEnding::done(Height(10), 1)).unwrap();
    assert_eq!(
        completed.range,
        range(10, 3),
        "the ending names the original request"
    );
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
    book.finish(RangeEnding::done(Height(10), 2)).unwrap();
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
    book.finish(RangeEnding::done(Height(1), 1)).unwrap();
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
    assert!(book.finish(RangeEnding::unavailable(Height(1), 3)).is_err());
    assert!(book.finish(RangeEnding::done(Height(1), 1)).is_err());
    assert_eq!(pool.held(), 1);
    assert!(!connection.is_cancelled());
    book.claim_block(hash(1), 3).unwrap();
    assert!(book.finish(RangeEnding::unavailable(Height(1), 2)).is_err());
    assert!(book.finish(RangeEnding::done(Height(1), 2)).is_err());
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
    book.finish(RangeEnding::done(Height(1), 1)).unwrap();
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

#[test]
fn unreceived_snapshot_stops_at_its_limit_in_height_order() {
    let (fence, _connection) = fence();
    let pool = ReservationPool::new(2).unwrap();
    let mut book = Requester::new(2);
    for (start, hashes) in [(20, [hash(4), hash(5)]), (10, [hash(1), hash(2)])] {
        book.reserve(
            range(start, 2),
            &hashes,
            100,
            pool.try_entry().unwrap(),
            fence.open().unwrap(),
        )
        .unwrap();
    }
    assert_eq!(
        book.unreceived_expected(3),
        [
            (Height(10), hash(1)),
            (Height(11), hash(2)),
            (Height(20), hash(4)),
        ]
    );
}

#[test]
fn shortened_unavailable_ends_the_whole_requested_range() {
    let (fence, connection) = fence();
    let pool = ReservationPool::new(1).unwrap();
    let mut book = Requester::new(1);
    let requested = range(10, 3);
    let writer = book
        .reserve(
            requested,
            &[hash(1), hash(2), hash(3)],
            100,
            pool.try_entry().unwrap(),
            fence.open().unwrap(),
        )
        .unwrap();
    assert!(writer.publish(|| {}));
    assert!(writer.try_start(|| true));
    assert!(book
        .finish(RangeEnding::unavailable(Height(10), 0))
        .is_err());
    assert_eq!(pool.held(), 1);

    let completed = book
        .finish(RangeEnding::unavailable(Height(10), 1))
        .unwrap();
    assert_eq!(completed.range, requested);
    assert_eq!(pool.held(), 0);
    assert!(fence.retire());
    assert!(!connection.is_cancelled());
}

#[test]
fn unfinished_ranges_keep_capacity_and_do_not_extend_their_retirement_deadline() {
    use std::time::Duration;

    let (fence, _) = fence();
    let pool = ReservationPool::new(2).unwrap();
    let mut book = Requester::new(2);
    for start in 1..=2 {
        let byte = u8::try_from(start).unwrap();
        book.reserve(
            range(start, 1),
            &[hash(byte)],
            100,
            pool.try_entry().unwrap(),
            fence.open().unwrap(),
        )
        .unwrap();
    }
    let grace = Duration::from_secs(32);
    book.abandon(Height(1));
    let deadline = book.retirement_deadline(grace).unwrap();
    book.abandon(Height(1));
    assert_eq!(book.retirement_deadline(grace), Some(deadline));
    book.claim_block(hash(2), 2).unwrap();
    assert_eq!(book.retirement_deadline(grace), Some(deadline));
    assert!(book.at_capacity());

    book.claim_block(hash(1), 2).unwrap();
    assert_eq!(book.retirement_deadline(grace), Some(deadline));
    assert!(book.blocks_retry(Height(1)));
    assert_eq!(book.retirement_deadline(grace), Some(deadline));
    book.finish(RangeEnding::done(Height(1), 1)).unwrap();
    assert!(!book.at_capacity());
    assert!(book.retirement_deadline(grace).is_some());
    assert!(!book.blocks_retry(Height(1)));
    book.finish(RangeEnding::done(Height(2), 1)).unwrap();
    assert!(book.retirement_deadline(grace).is_none());
}

/// An honest server's ready-order output can put one body from each other
/// running job between a range's last body and its ending. So the ending's
/// clock starts after those bodies' floor-rate transfer time, at most
/// `PEER_EXECUTION - 1` protocol-maximum bodies. With no other live range,
/// it starts at the claim.
#[test]
fn an_endings_clock_waits_for_the_bodies_that_can_precede_it() {
    use std::time::Duration;
    use zakura_chain::block::MAX_BLOCK_BYTES;
    use zakura_rt::time::Instant;

    use super::super::admission::min_rate_transfer_time;
    use super::session::PEER_EXECUTION;

    let grace = Duration::from_secs(32);
    let max_body = u32::try_from(2 * MAX_BLOCK_BYTES).unwrap();
    let ranges = u32::try_from(PEER_EXECUTION + 2).unwrap();
    let (fence, _) = fence();
    let pool = ReservationPool::new(usize::try_from(ranges).unwrap()).unwrap();
    let mut book = Requester::new(usize::try_from(ranges).unwrap());
    for start in 1..=ranges {
        let byte = u8::try_from(start).unwrap();
        book.reserve(
            range(start, 1),
            &[hash(byte)],
            max_body,
            pool.try_entry().unwrap(),
            fence.open().unwrap(),
        )
        .unwrap();
    }
    let before = Instant::now();
    book.claim_block(hash(1), 3).unwrap();
    let after = Instant::now();
    let ahead = min_rate_transfer_time((PEER_EXECUTION as u64 - 1) * MAX_BLOCK_BYTES);
    let deadline = book.retirement_deadline(grace).unwrap();
    assert!(before + grace + ahead <= deadline && deadline <= after + grace + ahead);

    book.finish(RangeEnding::done(Height(1), 1)).unwrap();
    for start in 3..=ranges {
        book.retract(Height(start));
    }
    let before = Instant::now();
    book.claim_block(hash(2), 3).unwrap();
    let after = Instant::now();
    let deadline = book.retirement_deadline(grace).unwrap();
    assert!(before + grace <= deadline && deadline <= after + grace);
}
