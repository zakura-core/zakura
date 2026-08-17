use super::*;

struct TestStalledMaintenance {
    stalled_version: Mutex<Option<StateVersion>>,
    clear_on_reevaluation: bool,
    reevaluations: AtomicUsize,
}

impl HeaderChainMaintenance for TestStalledMaintenance {
    fn resource_stalled_version(&self) -> Option<StateVersion> {
        *self
            .stalled_version
            .lock()
            .expect("the stalled-version test lock is available")
    }

    fn earliest_deferred(
        &self,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>, HeaderChainStoreError> {
        Ok(None)
    }

    fn now(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }

    fn reevaluate_deferred(&self) -> Result<(), HeaderChainStoreError> {
        self.reevaluations.fetch_add(1, Ordering::SeqCst);
        if self.clear_on_reevaluation {
            *self
                .stalled_version
                .lock()
                .map_err(|_| HeaderChainStoreError::WriterPoisoned)? = None;
        }
        Ok(())
    }
}

#[test]
fn deferred_maintenance_wakes_an_idle_writer_at_the_deadline() {
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let now = chrono::Utc::now();
    let maintenance = TestDeferredMaintenance {
        deadlines: Mutex::new(VecDeque::from([now + chrono::Duration::milliseconds(20)])),
        sender: Mutex::new(Some(sender)),
        reevaluations: AtomicUsize::new(0),
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("the test deadline runtime is available");
    let started = Instant::now();
    let message = receive_until_deferred_deadline(&mut receiver, Some(&maintenance), &runtime)
        .expect("deadline maintenance succeeds");

    assert!(message.is_none());
    assert_eq!(maintenance.reevaluations.load(Ordering::SeqCst), 1);
    assert!(started.elapsed() >= Duration::from_millis(20));
}

#[test]
fn deferred_maintenance_advances_to_the_next_deadline() {
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let now = chrono::Utc::now();
    let maintenance = TestDeferredMaintenance {
        deadlines: Mutex::new(VecDeque::from([
            now + chrono::Duration::milliseconds(10),
            now + chrono::Duration::milliseconds(30),
        ])),
        sender: Mutex::new(Some(sender)),
        reevaluations: AtomicUsize::new(0),
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("the test deadline runtime is available");
    let message = receive_until_deferred_deadline(&mut receiver, Some(&maintenance), &runtime)
        .expect("both deadline maintenance passes succeed");

    assert!(message.is_none());
    assert_eq!(maintenance.reevaluations.load(Ordering::SeqCst), 2);
}

#[test]
fn state_messages_preempt_the_deferred_deadline() {
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let (rsp_tx, _rsp_rx) = oneshot::channel();
    let delayed_sender = sender.clone();
    let delayed_send = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        delayed_sender
            .send(NonFinalizedWriteMessage::Invalidate {
                hash: block::Hash([7; 32]),
                rsp_tx,
            })
            .expect("the delayed state message is queued");
    });
    let now = chrono::Utc::now();
    let maintenance = TestDeferredMaintenance {
        deadlines: Mutex::new(VecDeque::from([now + chrono::Duration::hours(1)])),
        sender: Mutex::new(Some(sender)),
        reevaluations: AtomicUsize::new(0),
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("the test deadline runtime is available");
    let started = Instant::now();
    let message = receive_until_deferred_deadline(&mut receiver, Some(&maintenance), &runtime)
        .expect("the queued state message wins");
    delayed_send
        .join()
        .expect("the delayed test sender does not panic");

    assert!(matches!(
        message,
        Some(NonFinalizedWriteMessage::Invalidate { hash, .. })
            if hash == block::Hash([7; 32])
    ));
    assert_eq!(maintenance.reevaluations.load(Ordering::SeqCst), 0);
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[test]
fn writer_reevaluates_a_resource_stall_without_a_deferred_deadline() {
    let maintenance = TestStalledMaintenance {
        stalled_version: Mutex::new(Some(StateVersion::new(7))),
        clear_on_reevaluation: true,
        reevaluations: AtomicUsize::new(0),
    };
    let mut last_resource_stall_recovery = None;

    recover_resource_stall(Some(&maintenance), &mut last_resource_stall_recovery)
        .expect("resource-stall maintenance succeeds");

    assert_eq!(maintenance.reevaluations.load(Ordering::SeqCst), 1);
    assert_eq!(last_resource_stall_recovery, None);
}

#[test]
fn writer_reevaluates_a_resource_stall_once_per_state_version() {
    let maintenance = TestStalledMaintenance {
        stalled_version: Mutex::new(Some(StateVersion::new(7))),
        clear_on_reevaluation: false,
        reevaluations: AtomicUsize::new(0),
    };
    let mut last_resource_stall_recovery = None;

    for (iteration, expected_reevaluations) in [(1u8, 1usize), (2, 1), (3, 2)] {
        if iteration == 3 {
            *maintenance
                .stalled_version
                .lock()
                .expect("the stalled-version test lock is available") = Some(StateVersion::new(8));
        }
        recover_resource_stall(Some(&maintenance), &mut last_resource_stall_recovery)
            .expect("bounded resource-stall maintenance succeeds");

        assert_eq!(
            maintenance.reevaluations.load(Ordering::SeqCst),
            expected_reevaluations
        );
    }
}

#[test]
fn idle_writer_promotes_a_due_persisted_deferred_header() {
    #[derive(Copy, Clone)]
    struct FixedClock(chrono::DateTime<chrono::Utc>);

    impl zakura_header_chain::Clock for FixedClock {
        fn now(&self) -> chrono::DateTime<chrono::Utc> {
            self.0
        }
    }

    let _init_guard = zakura_test::init();
    let network = Network::new_regtest(Default::default());
    let finalized_state = FinalizedState::new(&Config::ephemeral(), &network)
        .expect("the deferred fixture finalized state opens");
    let anchor = regtest_genesis_block();
    let anchor_height = anchor
        .coinbase_height()
        .expect("the deferred fixture anchor has a height");
    let writer = header_writer(&finalized_state, &network, anchor_height, &anchor);
    let initial = writer.runtime.publisher().snapshot();
    let lease = writer
        .runtime
        .reader()
        .validation_context(anchor.hash())
        .expect("the anchor validation context read succeeds")
        .expect("the anchor validation context exists");
    let rules = HeaderRules::for_validation_lease(&lease)
        .expect("the deferred fixture validation rules are coherent");
    let preparation_clock = FixedClock(anchor.header.time);
    let mut future_header = *anchor.header;
    future_header.previous_block_hash = anchor.hash();
    future_header.time += chrono::Duration::hours(3);
    future_header.nonce.0[0] = 0xd0;
    let future_header = Arc::new(future_header);
    let batch = zakura_header_chain::prepare_headers(
        HeaderBatchInput::new(std::slice::from_ref(&future_header)),
        lease.parent(),
        &rules,
        &preparation_clock,
    )
    .expect("the locally future header is prepared as deferred");
    let deferred_until = future_header.time - chrono::Duration::hours(2);
    assert!(deferred_until < chrono::Utc::now());
    let future = Frontier::new(
        anchor_height
            .next()
            .expect("the genesis anchor has a next height"),
        future_header.hash(),
    );
    let owner = header_owner(&initial, future.hash, 41, 42);
    let insertion_context = TransitionContext {
        config: &writer.config,
        clock: &preparation_clock,
        full_state_authority: None,
        retention_references: &[],
    };
    writer
        .runtime
        .apply(
            TransitionRequest {
                expected_version: initial.state_version,
                event: TransitionEvent::InsertHeaders(Box::new(InsertHeaders {
                    owner,
                    source: SourceId::from_digest([0xd1; 32]),
                    parent_hash: anchor.hash(),
                    target_tip_hash: future.hash,
                    completion: TargetCompletion::TargetComplete {
                        common_ancestor: Frontier::new(anchor_height, anchor.hash()),
                    },
                    batch,
                    aux: Vec::new(),
                })),
            },
            &insertion_context,
        )
        .expect("the deferred header insertion commits");
    assert_eq!(
        writer
            .runtime
            .earliest_deferred()
            .expect("the deferred deadline is readable"),
        Some(deferred_until)
    );

    let (sender, mut receiver) = mpsc::unbounded_channel();
    let close_sender = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        drop(sender);
    });
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("the test deadline runtime is available");
    let message = receive_until_deferred_deadline(&mut receiver, Some(&writer), &runtime)
        .expect("idle deferred maintenance succeeds");
    close_sender
        .join()
        .expect("the delayed channel closer does not panic");

    assert!(message.is_none());
    assert_eq!(
        writer.runtime.publisher().snapshot().frontiers.header_best,
        future
    );
    assert_eq!(
        writer
            .runtime
            .earliest_deferred()
            .expect("the promoted deadline index is readable"),
        None
    );
}
