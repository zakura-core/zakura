//! Local storage, encoder and output failures must release their work correctly.
//! An error before any block may return RangeUnavailable. After a block has been
//! sent, an encoding error must not invent a successful or empty ending.

use super::super::super::super::tests::fake_blocks_in_range;
use super::*;

#[tokio::test]
async fn c06_storage_error_returns_a_legal_empty_response_without_a_peer_fault() {
    let mut source = ControlledSource::new(100, 3, false, false);
    Arc::get_mut(&mut source).unwrap().fail = true;
    let mut f = source.fixture(1, 1, 1);
    request(&f, 100, 3).await;
    response(&mut f, &source, 100, 3, config(1).max_response_bytes, 128).await;
    assert!(!f.session.cancel_token().is_cancelled());
    source.probe.wait_finished(1).await;
    f.finish().await;
}

#[derive(Debug)]
struct FaultSource {
    before_execution: bool,
    bodies: Vec<Arc<block::Block>>,
    starts: Arc<AtomicUsize>,
}

impl BlockRangeSource for FaultSource {
    fn read_range(
        &self,
        request: BlockRangeRead,
    ) -> BoxFuture<'static, Result<BlockRangeReadResult, crate::BoxError>> {
        let (start, _, _, lease) = request.into_parts();
        let before = self.before_execution;
        let starts = self.starts.clone();
        let mut bodies = self.bodies.clone();
        Box::pin(async move {
            if before {
                return Err(
                    io::Error::other("storage readiness failed before starting a job").into(),
                );
            }
            tokio::task::spawn_blocking(move || {
                assert!(lease.try_start());
                starts.fetch_add(1, Ordering::Relaxed);
                // Invalid local storage data: the second encoding exceeds the
                // actual Block codec limit after a legal prefix was delivered.
                let last = Arc::make_mut(&mut bodies[1]);
                let tx = last.transactions[0].clone();
                last.transactions =
                    vec![tx; 2_100_000 / last.transactions[0].zcash_serialized_size() + 1];
                let result = bodies
                    .into_iter()
                    .enumerate()
                    .map(|(index, body)| {
                        (
                            block::Height(start.0 + u32::try_from(index).unwrap()),
                            body.clone(),
                            body.zcash_serialized_size(),
                        )
                    })
                    .collect();
                BlockRangeReadResult::new(result, lease)
            })
            .await
            .map_err(Into::into)
        })
    }
}

#[tokio::test]
async fn c06_readiness_failure_starts_no_job_and_is_not_peer_misconduct() {
    let starts = Arc::new(AtomicUsize::new(0));
    let source = Arc::new(FaultSource {
        before_execution: true,
        bodies: Vec::new(),
        starts: starts.clone(),
    });
    let mut f = Fixture::new(source);
    f.session.mark_status_received();
    f.request().await;
    assert_eq!(
        f.next().await,
        BlockSyncMessage::RangeUnavailable {
            start_height: block::Height(1),
            count: 2
        }
    );
    assert_eq!(starts.load(Ordering::Relaxed), 0);
    assert!(!f.session.cancel_token().is_cancelled());
    f.finish().await;
}

#[tokio::test]
async fn c06_encoding_failure_after_a_prefix_does_not_send_an_invalid_ending() {
    let starts = Arc::new(AtomicUsize::new(0));
    let source = Arc::new(FaultSource {
        before_execution: false,
        bodies: fake_blocks_in_range(1, 2),
        starts: starts.clone(),
    });
    let mut f = Fixture::new(source);
    f.session.mark_status_received();
    f.request().await;
    assert!(matches!(f.next().await, BlockSyncMessage::Block(_)));
    assert!(matches!(
        time::timeout(DEADLINE, &mut f.task).await.unwrap().unwrap(),
        Err(crate::zakura::SinkReject::Local(_))
    ));
    assert!(
        time::timeout(Duration::from_millis(20), f.data.recv())
            .await
            .is_err(),
        "C06 no RangeUnavailable after a body and no invented terminal"
    );
    assert_eq!(starts.load(Ordering::Relaxed), 1);
    assert_eq!(f.regulator.snapshot().node_active, 0);
    assert!(!f.session.cancel_token().is_cancelled());
}

#[tokio::test]
async fn c06_closed_output_releases_finished_resources_before_and_after_a_body() {
    for after_prefix in [false, true] {
        let source = ControlledSource::new(100, 3, false, false);
        let mut f = source.fixture(1, 1, 1);
        request(&f, 100, 3).await;
        if after_prefix {
            assert!(matches!(f.next().await, BlockSyncMessage::Block(_)));
        }
        drop(f.data);
        assert!(matches!(
            time::timeout(DEADLINE, &mut f.task).await.unwrap().unwrap(),
            Err(crate::zakura::SinkReject::Local(_))
        ));
        source.probe.wait_finished(1).await;
        assert_eq!(source.live_decoded_bytes(), 0);
        assert_eq!(f.regulator.snapshot().node_active, 0);
        assert!(!f.session.cancel_token().is_cancelled());
    }
}
