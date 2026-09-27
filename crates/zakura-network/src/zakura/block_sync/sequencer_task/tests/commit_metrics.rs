use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(feature = "sync-metrics")]
#[test]
fn committed_rate_survives_frequent_queue_updates() {
    let start = Instant::now();
    let interval = Duration::from_secs(1);
    let mut meter = ThroughputMeter::new(start);
    for elapsed_ms in 1..1000 {
        if elapsed_ms == 1 || elapsed_ms == 500 {
            meter.record(1_000_000);
        }
        meter.sample_at_interval(start + Duration::from_millis(elapsed_ms), interval);
    }
    meter.sample_at_interval(start + interval, interval);
    assert_eq!(meter.bytes_per_sec(), 2_000_000);
    assert_eq!(meter.blocks_per_sec(), 2);

    // A queue update with no completion keeps the last full window visible.
    meter.sample_at_interval(start + Duration::from_millis(1001), interval);
    assert_eq!(meter.bytes_per_sec(), 2_000_000);
    assert_eq!(meter.blocks_per_sec(), 2);

    meter.sample_at_interval(start + Duration::from_secs(2), interval);
    assert_eq!(meter.bytes_per_sec(), 0);
    assert_eq!(meter.blocks_per_sec(), 0);
}

#[derive(Default)]
struct CommitRecorder {
    bytes: Arc<AtomicU64>,
    registrations: AtomicU64,
}

impl metrics::Recorder for CommitRecorder {
    fn describe_counter(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn describe_gauge(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn describe_histogram(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        if key.name() == "sync.block.payload.committed.bytes" {
            self.registrations.fetch_add(1, Ordering::Relaxed);
            metrics::Counter::from_arc(self.bytes.clone())
        } else {
            metrics::Counter::noop()
        }
    }

    fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::noop()
    }

    fn register_histogram(
        &self,
        _: &metrics::Key,
        _: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

#[tokio::test(flavor = "current_thread")]
async fn committed_payload_counts_once_across_frontier_and_completion_orderings() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for frontier_first in [false, true] {
            for semantic_current in [false, true] {
                for result in [
                    BlockApplyResult::Committed,
                    BlockApplyResult::Duplicate,
                    BlockApplyResult::Rejected,
                    BlockApplyResult::Unavailable,
                    BlockApplyResult::TimedOut,
                ] {
                    let recorder = CommitRecorder::default();
                    let _guard = metrics::set_default_local_recorder(&recorder);
                    let frontiers = BlockSyncFrontiers {
                        finalized_height: block::Height(0),
                        verified_block_tip: block::Height(0),
                        verified_block_hash: block::Hash([0; 32]),
                    };
                    let input_bytes = Arc::new(AtomicU64::new(0));
                    let input_decoded_bytes = Arc::new(AtomicU64::new(0));
                    let (_body_tx, body_rx) = mpsc::channel(1);
                    let (_control_tx, control_rx) = mpsc::unbounded_channel();
                    let (actions, mut actions_rx) = mpsc::channel(16);
                    let (view_tx, _view_rx) = watch::channel(initial_view(frontiers));
                    let meter_start = Instant::now();
                    let mut task = SequencerTask::new(
                        Sequencer::new(block::Height(0), 1),
                        ByteBudget::new(123),
                        Arc::new(WorkQueue::new(block::Height(0))),
                        Arc::new(PeerRegistry::new()),
                        actions,
                        ThroughputMeter::new(meter_start),
                        frontiers,
                        Some(super::super::test_work_scope()),
                        crate::zakura::header_sync::SeededRetryJitter::new([0; 32]),
                        body_rx,
                        control_rx,
                        input_bytes.clone(),
                        input_decoded_bytes.clone(),
                        view_tx,
                        Duration::from_secs(1),
                        ZakuraTrace::noop(),
                    );
                    let mut body = queued_test_body(input_bytes, input_decoded_bytes);
                    body.leave_queue();
                    task.handle_accept_body(body);
                    task.submit_pending_blocks().await;
                    let BlockSyncAction::SubmitBlock {
                        owner,
                        source,
                        token,
                        block,
                    } = actions_rx.recv().await.expect("body was submitted")
                    else {
                        panic!("expected a body submission");
                    };
                    let height = block.coinbase_height().expect("test block has a height");
                    let hash = block.hash();
                    if frontier_first {
                        task.handle_frontier_advance(
                            BlockSyncFrontiers {
                                finalized_height: height,
                                verified_block_tip: height,
                                verified_block_hash: hash,
                            },
                            true,
                        )
                        .await;
                        assert_eq!(task.sequencer.applying_len(), 0);
                    }
                    assert_eq!(task.sequencer.in_flight_submission_count(), 1);

                    // A mismatched completion must leave the real submission
                    // available to its later exact completion.
                    let wrong_source = zakura_header_chain::SourceId::from_digest([9; 32]);
                    let wrong_hash = block::Hash([9; 32]);
                    let wrong_owner = super::super::test_work_scope()
                        .bind(99, std::num::NonZeroU64::new(99).expect("99 is nonzero"));
                    for identity in [
                        (wrong_owner, source, token, height, hash),
                        (owner, wrong_source, token, height, hash),
                        (owner, source, token + 1, height, hash),
                        (owner, source, token, block::Height(99), hash),
                        (owner, source, token, height, wrong_hash),
                    ] {
                        let mut outcome =
                            super::super::test_block_apply_outcome(BlockApplyResult::Committed);
                        task.handle_apply_finished(
                            identity.0,
                            identity.1,
                            identity.2,
                            identity.3,
                            identity.4,
                            &mut outcome,
                            BTreeSet::new(),
                            None,
                            None,
                        )
                        .await;
                        assert_eq!(recorder.bytes.load(Ordering::Relaxed), 0);
                        assert_eq!(task.sequencer.in_flight_submission_count(), 1);
                    }

                    let semantic = semantic_current
                        .then_some((owner, zakura_header_chain::StateVersion::new(1)));
                    for _ in 0..2 {
                        let mut outcome = super::super::test_block_apply_outcome(result);
                        task.handle_apply_finished(
                            owner,
                            source,
                            token,
                            height,
                            hash,
                            &mut outcome,
                            BTreeSet::new(),
                            None,
                            semantic,
                        )
                        .await;
                        assert_eq!(task.sequencer.in_flight_submission_count(), 0);
                    }
                    let counted = cfg!(feature = "sync-metrics")
                        && matches!(result, BlockApplyResult::Committed);
                    assert_eq!(
                        recorder.bytes.load(Ordering::Relaxed),
                        if counted { 123 } else { 0 }
                    );
                    assert_eq!(
                        recorder.registrations.load(Ordering::Relaxed),
                        u64::from(counted)
                    );
                    if cfg!(feature = "sync-metrics") {
                        task.committed_throughput
                            .sample(meter_start + Duration::from_secs(1));
                        assert_eq!(
                            task.committed_throughput.bytes_per_sec(),
                            if counted { 123 } else { 0 }
                        );
                        assert_eq!(
                            task.committed_throughput.blocks_per_sec(),
                            u64::from(counted)
                        );
                    }
                }
            }
        }
    })
    .await
    .expect("completion accounting finishes within the test deadline");
}
