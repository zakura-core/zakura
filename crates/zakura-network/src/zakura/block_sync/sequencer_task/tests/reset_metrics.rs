use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

/// Captures `sync.block.reorg.reset` and ignores every other metric.
#[derive(Default)]
struct ResetRecorder {
    resets: Arc<AtomicU64>,
}

impl metrics::Recorder for ResetRecorder {
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
        if key.name() == "sync.block.reorg.reset" {
            metrics::Counter::from_arc(self.resets.clone())
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

/// A task at verified tip 0 whose body at height 1 is applying and links to that tip.
/// The channel ends are held so the task's channels stay open.
struct Fixture {
    task: SequencerTask,
    frontiers: BlockSyncFrontiers,
    _body_tx: mpsc::Sender<SequencedBody>,
    _control_tx: mpsc::UnboundedSender<SequencerControlInput>,
    _actions_rx: mpsc::Receiver<BlockSyncAction>,
    _view_rx: watch::Receiver<SequencerView>,
}

impl Fixture {
    /// Builds the [`Fixture`] task and accepts its height-1 body.
    fn new() -> Self {
        let frontiers = BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: test_block().header.previous_block_hash,
        };
        let input_bytes = Arc::new(AtomicU64::new(0));
        let input_decoded_bytes = Arc::new(AtomicU64::new(0));
        let (body_tx, body_rx) = mpsc::channel(1);
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let (actions, actions_rx) = mpsc::channel(1);
        let (view_tx, view_rx) = watch::channel(initial_view(frontiers));
        let mut task = SequencerTask::new(
            Sequencer::new(block::Height(0), 1),
            ByteBudget::new(123),
            Arc::new(WorkQueue::new(block::Height(0))),
            Arc::new(PeerRegistry::new()),
            actions,
            ThroughputMeter::new(Instant::now()),
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
        assert_eq!(task.sequencer.applying_len(), 1);

        Self {
            task,
            frontiers,
            _body_tx: body_tx,
            _control_tx: control_tx,
            _actions_rx: actions_rx,
            _view_rx: view_rx,
        }
    }

    /// Delivers a chain-tip reset with no conflicting peer requests at the target tip.
    async fn frontier_reset(
        &mut self,
        frontiers: BlockSyncFrontiers,
        preserve_active_successors: bool,
    ) {
        assert!(
            self.task
                .handle_control_input(SequencerControlInput::FrontierReset {
                    frontiers,
                    preserve_active_successors,
                    peer_has_successor_after: false,
                    peer_outstanding_conflicts_at_tip: false,
                })
                .await
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn reorg_reset_counts_each_reset_that_discards_body_work() {
    tokio::time::timeout(Duration::from_secs(10), async {
        // A header-driven reorg arrives as a body work epoch change.
        let recorder = ResetRecorder::default();
        let guard = metrics::set_default_local_recorder(&recorder);
        let mut fixture = Fixture::new();
        let mut authority = super::super::test_work_scope();
        authority.body_work_epoch = zakura_header_chain::BodyWorkEpoch::new(1);
        assert!(
            fixture
                .task
                .handle_control_input(SequencerControlInput::BodyWorkEpochChanged {
                    authority,
                    frontiers: fixture.frontiers,
                })
                .await
        );
        assert_eq!(fixture.task.sequencer.applying_len(), 0);
        assert_eq!(fixture.task.reset_epoch, 1);
        assert_eq!(recorder.resets.load(Ordering::Relaxed), 1);
        drop(guard);

        // A chain-tip reset that does not preserve successors clears them.
        let recorder = ResetRecorder::default();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let mut fixture = Fixture::new();
        fixture.frontier_reset(fixture.frontiers, false).await;
        assert_eq!(fixture.task.sequencer.applying_len(), 0);
        assert_eq!(fixture.task.reset_epoch, 1);
        assert_eq!(recorder.resets.load(Ordering::Relaxed), 1);
    })
    .await
    .expect("reset metric test finishes promptly");
}

#[tokio::test(flavor = "current_thread")]
async fn reorg_reset_ignores_resets_that_keep_body_work() {
    tokio::time::timeout(Duration::from_secs(10), async {
        // A stale reset below an applying successor that links to the tip keeps it.
        let recorder = ResetRecorder::default();
        let guard = metrics::set_default_local_recorder(&recorder);
        let mut fixture = Fixture::new();
        fixture.frontier_reset(fixture.frontiers, true).await;
        assert_eq!(fixture.task.sequencer.applying_len(), 1);
        assert_eq!(fixture.task.reset_epoch, 0);
        assert_eq!(recorder.resets.load(Ordering::Relaxed), 0);
        drop(guard);

        // A reset that moves the tip onto the applying body is growth.
        let recorder = ResetRecorder::default();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let mut fixture = Fixture::new();
        let grown = BlockSyncFrontiers {
            verified_block_tip: block::Height(1),
            verified_block_hash: test_block().hash(),
            ..fixture.frontiers
        };
        fixture.frontier_reset(grown, false).await;
        assert_eq!(fixture.task.reset_epoch, 0);
        assert_eq!(recorder.resets.load(Ordering::Relaxed), 0);
    })
    .await
    .expect("reset metric test finishes promptly");
}
