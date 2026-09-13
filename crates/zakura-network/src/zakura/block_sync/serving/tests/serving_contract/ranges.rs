//! Check the response prefix and its ending across count and byte boundaries.
//! Expected hashes and limits come from the stored fixtures. Waiting requests
//! must use current serving limits when they finally gain a worker slot.

use super::*;
use proptest::prelude::*;

async fn check_serving(
    start: u32,
    count: u32,
    available: u32,
    depth: usize,
    cap_count: u32,
    large: bool,
) {
    let source = ControlledSource::new(start, count, large, false);
    let mut f = source.fixture(1, depth, 1);
    f.status.send_modify(|status| {
        // A status prefix models a gap at the end of the committed range.
        status.servable_high = block::Height(start + available.saturating_sub(1));
        status.max_blocks_per_response = cap_count;
        if available == 0 {
            status.servable_low = block::Height(start + count);
            status.servable_high = status.servable_low;
        }
    });
    request(&f, start, count).await;
    let expected = available.min(count).min(cap_count);
    let returned = if expected == 0 {
        assert_eq!(
            f.next().await,
            BlockSyncMessage::RangeUnavailable {
                start_height: block::Height(start),
                count
            }
        );
        0
    } else {
        response(
            &mut f,
            &source,
            start,
            count,
            config(1).max_response_bytes,
            expected,
        )
        .await
    };
    assert!(returned <= usize::try_from(expected).unwrap());
    assert!(!f.session.cancel_token().is_cancelled());
    f.finish().await;
}

#[tokio::test]
async fn c07_maximal_count_and_byte_limited_serving_use_real_frames() {
    check_serving(100, 128, 128, 1, 128, false).await;
    check_serving(10_000, 20, 20, 2, 128, true).await;
    check_serving(block::Height::MAX.0 - 127, 128, 128, 3, 128, false).await;
    check_serving(100, 3, 0, 1, 128, false).await;
    check_serving(100, 3, 1, 1, 128, false).await;
}

#[tokio::test]
async fn c07_waiting_request_uses_the_latest_serving_limits() {
    let source = ControlledSource::new(100, 3, false, false);
    let mut f = source.fixture(1, 1, 1);
    let other = f.regulator.session(ZakuraPeerId::new(vec![2; 32]).unwrap());
    let held_request = other
        .decode_request(
            BlockSyncMessage::GetBlocks {
                start_height: block::Height(200),
                count: 1,
            }
            .encode_frame()
            .unwrap(),
        )
        .unwrap();
    let held = other.admit_request(&held_request).await;
    request(&f, 100, 3).await;
    tokio::task::yield_now().await;
    assert_eq!(source.probe.snapshot().started, 0);
    f.status
        .send_modify(|status| status.max_blocks_per_response = 1);
    drop(held);
    response(&mut f, &source, 100, 3, config(1).max_response_bytes, 1).await;
    f.finish().await;
}

proptest! {
    #[test]
    fn c07_generated_serving_range_boundaries(start in 1u32..10_000, count in 1u32..=128,
        available in 0u32..=128, depth in 1usize..=3, cap_count in 1u32..=128) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
            .block_on(check_serving(start, count, available.min(count), depth, cap_count, false));
    }
}

#[tokio::test]
async fn r02_buffered_overlap_shapes_are_not_admitted_while_a_terminal_is_owned() {
    // Identical, containing, contained, suffix/endpoint, adjacent and disjoint.
    // Bytes may wait behind backpressure. Admission becomes legal only after
    // the old ending finishes, so this does not call buffered bytes a violation.
    for (start, count) in [
        (100, 3),
        (98, 7),
        (99, 3),
        (101, 1),
        (101, 3),
        (102, 3),
        (103, 2),
        (105, 1),
    ] {
        let source = ControlledSource::new(98, 9, false, false);
        let mut f = source.fixture(1, 1, 1);
        request(&f, 100, 3).await;
        for _ in 0..3 {
            assert!(matches!(f.next().await, BlockSyncMessage::Block(_)));
        }
        let ending = time::timeout(DEADLINE, f.data.recv())
            .await
            .unwrap()
            .unwrap();
        let mut held_write = Box::pin(ending.write_with(|frame| async move {
            assert_eq!(
                BlockSyncMessage::decode_frame(frame).unwrap(),
                BlockSyncMessage::BlocksDone {
                    start_height: block::Height(100),
                    returned: 3
                }
            );
            std::future::pending::<Result<(), ()>>().await
        }));
        assert!(futures::poll!(&mut held_write).is_pending());
        request(&f, start, count).await;
        tokio::task::yield_now().await;
        assert_eq!(
            source.probe.snapshot().started,
            1,
            "R02 premature second admission for {start}/{count}"
        );
        drop(held_write);
        response(
            &mut f,
            &source,
            start,
            count,
            config(1).max_response_bytes,
            128,
        )
        .await;
        f.finish().await;
    }
}

#[tokio::test]
async fn c07_storage_gaps_exact_byte_fits_and_first_body_too_large() {
    for cap_case in 0..4 {
        let mut source = ControlledSource::new(100, 3, false, false);
        if cap_case == 0 {
            Arc::make_mut(&mut Arc::get_mut(&mut source).unwrap().encoded)
                .remove(&block::Height(101));
        }
        let bytes = source.encoded[&block::Height(100)].len();
        let cap = u32::try_from(match cap_case {
            2 => bytes - 1,
            3 => 2 * bytes,
            _ => bytes,
        })
        .unwrap();
        let mut f = source.fixture(1, 1, 1);
        f.status
            .send_modify(|status| status.max_response_bytes = cap);
        request(&f, 100, 3).await;
        response(&mut f, &source, 100, 3, cap, 128).await;
        f.finish().await;
    }
}
