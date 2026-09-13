//! Compare generated ending counts with the received prefix and original request.

use super::*;

async fn check_done_count(start: u32, requested: u32, consumed: u32, returned: u32) {
    let mut f = Fixture::new(start, requested);
    f.publish().await;
    for index in 0..usize::try_from(consumed).unwrap() {
        f.body(index).await;
    }
    if returned == consumed && consumed > 0 {
        f.deliver(f.done(returned)).await.unwrap();
        f.assert_no_peer_fault();
        assert!(f.routine.window.outstanding.is_empty());
        for offset in consumed..requested {
            assert!(f
                .routine
                .work
                .pending_contains(block::Height(start + offset)));
        }
    } else {
        f.rejects(f.done(returned), "R06 generated terminal identity/count")
            .await;
    }
}

proptest! {
    #[test]
    fn r06_generated_terminal_counts(
        start in 1u32..10_000,
        requested in 1u32..=128,
        prefix in 0u32..=128,
        returned in 1u32..=128,
    ) {
        let consumed = prefix.min(requested);
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
            .block_on(check_done_count(start, requested, consumed, returned));
    }

    #[test]
    fn r07_generated_unavailable_identity(start in 1u32..10_000, count in 1u32..=128, delta in 1u32..=127) {
        // Shrinking preserves the count violation, including at both boundaries.
        let wrong = (count - 1 + delta) % 128 + 1;
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let mut f = Fixture::new(start, count);
            f.publish().await;
            f.rejects(f.unavailable(wrong), "R07 generated wrong count").await;
        });
    }
}
