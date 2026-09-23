use super::*;

#[test]
fn startup_waits_for_initialization_and_late_workers_then_stays_ready() {
    let (recorder, _, _) = recorder();
    for second in 0..=70 {
        recorder.observe_startup(second * 1_000_000, true);
    }
    assert_eq!(recorder.ready_us.load(Ordering::Acquire), u64::MAX);
    recorder.initialized.store(true, Ordering::Release);
    let root = begin_with(&recorder, block()).unwrap();
    let worker = root.context();
    root.finish(Outcome::Success);
    for second in 71..=140 {
        recorder.observe_startup(second * 1_000_000, true);
    }
    assert_eq!(recorder.ready_us.load(Ordering::Acquire), u64::MAX);
    drop(worker);
    for second in 141..=201 {
        recorder.observe_startup(second * 1_000_000, true);
    }
    assert_eq!(recorder.ready_us.load(Ordering::Acquire), 201_000_000);
    let _later_block = begin_with(&recorder, block()).unwrap();
    recorder.observe_startup(202_000_000, false);
    assert_eq!(recorder.ready_us.load(Ordering::Acquire), 201_000_000);
}

#[test]
fn startup_requires_stable_fresh_sync_checks_and_quiet_workers() {
    let mut gate = StartupGate::default();
    // Initialization, sync lag, or active finalization all make the observation ineligible.
    for second in 0..=70 {
        assert!(!gate.observe(second * 1_000_000, false, 0));
    }
    for second in 71..131 {
        assert!(!gate.observe(second * 1_000_000, true, 0));
    }
    assert!(gate.observe(131_000_000, true, 0));

    let mut gate = StartupGate::default();
    for second in 0..60 {
        // Work that began and ended between observations must restart the settling period.
        let activity = if second >= 30 { 30_000_000 } else { 0 };
        assert!(!gate.observe(second * 1_000_000, true, activity));
    }
    for second in 60..90 {
        assert!(!gate.observe(second * 1_000_000, true, 30_000_000));
    }
    assert!(gate.observe(90_000_000, true, 30_000_000));
}

#[test]
fn startup_does_not_treat_missing_observations_as_readiness() {
    let mut gate = StartupGate::default();
    assert!(!gate.observe(0, true, 0));
    assert!(!gate.observe(90_000_000, true, 0));
    assert!(!gate.observe(110_000_000, true, 0));
    assert!(!gate.observe(120_000_000, false, 0));
    for second in 121..181 {
        assert!(!gate.observe(second * 1_000_000, true, 0));
    }
    assert!(gate.observe(181_000_000, true, 0));
}

fn block() -> Block {
    Block {
        hash: [1; 32],
        parent: [0; 32],
        height: Some(1),
        transactions: 1,
        mode: Mode::Semantic,
    }
}

#[test]
fn detail_pressure_does_not_displace_completion() {
    let (r, detail, summary) = recorder();
    let root = begin_with(&r, block()).unwrap();
    let context = root.context();
    for _ in 0..MAX_SPANS + 10 {
        drop(context.span(Stage::BlockChecks));
    }
    root.finish(Outcome::Success);
    assert_eq!(detail.len(), usize::try_from(MAX_SPANS).unwrap());
    let events: Vec<_> = summary.try_iter().collect();
    assert!(matches!(
        events[1],
        Event::Finish {
            outcome: Outcome::Success,
            dropped: 10,
            ..
        }
    ));
    assert!(std::mem::size_of::<Event>() * (DETAIL_CAPACITY + SUMMARY_CAPACITY) < 64 * 1024 * 1024);
}

#[test]
fn active_workers_keep_the_context_budget_until_done() {
    let (r, _, _) = recorder();
    let mut roots: Vec<_> = (0..MAX_ATTEMPTS)
        .map(|_| begin_with(&r, block()).unwrap())
        .collect();
    let worker = roots[0].context();
    assert!(begin_with(&r, block()).is_none());
    drop(roots.remove(0));
    assert!(begin_with(&r, block()).is_none());
    drop(worker);
    assert!(begin_with(&r, block()).is_some());
}

#[test]
fn context_is_restored_after_each_poll_and_panic() {
    let (r, _, _) = recorder();
    let root = begin_with(&r, block()).unwrap();
    let context = root.context();
    let mut polls = 0;
    let future = std::future::poll_fn(|cx| {
        assert!(Context::current().0.is_some());
        polls += 1;
        if polls == 1 {
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    });
    futures::executor::block_on(context.wrap(future));
    assert!(Context::current().0.is_none());
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
        || context.in_scope(|| panic!("test unwind"))
    ))
    .is_err());
    assert!(Context::current().0.is_none());
}

#[test]
fn retry_and_abandonment_have_independent_identity() {
    let (r, _, summary) = recorder();
    drop(begin_with(&r, block()).unwrap());
    begin_with(&r, block()).unwrap().finish(Outcome::Success);
    let events: Vec<_> = summary.try_iter().collect();
    assert!(matches!(
        events[1],
        Event::Finish {
            attempt: 1,
            outcome: Outcome::Abandoned,
            ..
        }
    ));
    assert!(matches!(
        events[4],
        Event::Finish {
            attempt: 2,
            outcome: Outcome::Success,
            ..
        }
    ));
}

#[test]
fn full_queues_drop_without_waiting() {
    let (r, _, summary) = recorder();
    let started = Instant::now();
    for _ in 0..SUMMARY_CAPACITY {
        drop(begin_with(&r, block()));
    }
    assert_eq!(summary.len(), SUMMARY_CAPACITY);
    assert_eq!(
        r.dropped.load(Ordering::Relaxed),
        u64::try_from(SUMMARY_CAPACITY * 2).unwrap()
    );
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[test]
fn transaction_fanout_cannot_displace_writer_phases() {
    let (r, detail, _) = recorder();
    let root = begin_with(&r, block()).unwrap();
    for _ in 0..MAX_FINE_SPANS + 10 {
        drop(root.context().span(Stage::Transaction));
    }
    drop(root.context().span(Stage::WriterOccupied));
    let events: Vec<_> = detail.try_iter().collect();
    assert_eq!(events.len(), usize::try_from(MAX_FINE_SPANS).unwrap() + 1);
    assert!(matches!(
        events.last(),
        Some(Event::Span {
            stage: Stage::WriterOccupied,
            ..
        })
    ));
}

#[test]
fn finalization_children_keep_their_parent_after_caller_completion() {
    let (r, detail, summary) = recorder();
    let root = begin_with(&r, block()).unwrap();
    let context = root.context();
    root.finish(Outcome::Success);
    let finalization = context.span(Stage::Finalization);
    finalization.context().in_scope(|| {
        let commit = Context::current().span(Stage::FinalizedCommit);
        commit.context().in_scope(|| {
            drop(Context::current().span(Stage::RocksdbWrite));
        });
    });
    drop(finalization);
    drop(context);
    let events: Vec<_> = detail.try_iter().collect();
    assert_eq!(events.len(), 3);
    let mut links = Vec::new();
    for event in events {
        let encoded = serde_json::to_vec(&event).unwrap();
        let Event::Span {
            span,
            parent,
            stage,
            ..
        } = serde_json::from_slice(&encoded).unwrap()
        else {
            panic!("finalization emits span events");
        };
        links.push((span, parent, stage));
    }
    assert_eq!(links[0], (3, 2, Stage::RocksdbWrite));
    assert_eq!(links[1], (2, 1, Stage::FinalizedCommit));
    assert_eq!(links[2], (1, 0, Stage::Finalization));
    assert!(matches!(
        summary.try_iter().last(),
        Some(Event::Seal {
            spans: 3,
            dropped: 0,
            ..
        })
    ));
}

#[test]
fn overlapping_transactions_keep_indexes_and_parents_across_polls_and_workers() {
    let (r, detail, summary) = recorder();
    let root = begin_with(&r, block()).unwrap();
    let envelope = root.context().span(Stage::Transactions);
    let first = envelope
        .context()
        .for_transaction(0)
        .transaction_span([1; 32]);
    let second = envelope
        .context()
        .for_transaction(1)
        .transaction_span([2; 32]);
    let late_worker = first.context();
    let task = |span: Span| async move {
        let context = span.context();
        let mut polled = false;
        context
            .wrap(std::future::poll_fn(move |cx| {
                drop(Context::current().span(Stage::TransactionChecks));
                if polled {
                    Poll::Ready(())
                } else {
                    polled = true;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }))
            .await;
        drop(span);
    };
    futures::executor::block_on(futures::future::join(task(first), task(second)));
    drop(envelope);
    root.finish(Outcome::Success);
    late_worker.in_scope(|| drop(Context::current().span(Stage::WorkerExecution)));
    drop(late_worker);
    let events: Vec<_> = detail.try_iter().collect();
    assert_eq!(events.len(), 8);
    for event in events {
        let encoded = serde_json::to_vec(&event).unwrap();
        let Event::Span {
            span,
            parent,
            stage,
            transaction_index,
            transaction_hash,
            ..
        } = serde_json::from_slice(&encoded).unwrap()
        else {
            panic!("detail contains spans");
        };
        if stage == Stage::Transaction {
            assert_eq!(
                transaction_hash,
                Some([u8::try_from(span - 1).unwrap(); 32])
            );
        } else {
            assert_eq!(transaction_hash, None);
            assert!(!String::from_utf8(encoded)
                .unwrap()
                .contains("transaction_hash"));
        }
        match stage {
            Stage::Transactions => assert_eq!((span, parent, transaction_index), (1, 0, None)),
            Stage::Transaction => assert_eq!(
                (parent, transaction_index),
                (1, Some(u32::try_from(span - 2).unwrap()))
            ),
            Stage::TransactionChecks | Stage::WorkerExecution => {
                assert!(parent == 2 || parent == 3);
                assert_eq!(transaction_index, Some(u32::try_from(parent - 2).unwrap()));
            }
            _ => panic!("unexpected stage"),
        }
    }
    assert!(matches!(
        summary.try_iter().last(),
        Some(Event::Seal {
            spans: 8,
            dropped: 0,
            ..
        })
    ));
}

#[test]
fn older_transaction_spans_decode_without_a_hash() {
    let (r, detail, _) = recorder();
    let root = begin_with(&r, block()).unwrap();
    drop(root.context().for_transaction(0).span(Stage::Transaction));
    let encoded = serde_json::to_string(&detail.try_recv().unwrap()).unwrap();
    assert!(!encoded.contains("transaction_hash"));
    assert!(matches!(
        serde_json::from_str::<Event>(&encoded).unwrap(),
        Event::Span {
            transaction_index: Some(0),
            transaction_hash: None,
            ..
        }
    ));
}

#[test]
fn full_detail_queue_never_blocks_or_loses_the_root_summary() {
    let (r, detail, summary) = recorder();
    let started = Instant::now();
    for _ in 0..3 {
        let root = begin_with(&r, block()).unwrap();
        for _ in 0..MAX_SPANS {
            drop(root.context().span(Stage::BlockChecks));
        }
        root.finish(Outcome::Success);
    }
    assert_eq!(detail.len(), DETAIL_CAPACITY);
    assert_eq!(r.dropped.load(Ordering::Relaxed), MAX_SPANS);
    assert_eq!(
        summary
            .try_iter()
            .filter(|event| matches!(event, Event::Finish { .. }))
            .count(),
        3
    );
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[cfg(unix)]
mod transport {
    use super::*;
    use std::os::unix::net::UnixDatagram;

    fn run() -> Run {
        Run {
            id: "11111111111111111111111111111111".into(),
            node: "test".into(),
            session: "synthetic".into(),
            network: "regtest".into(),
            build: "test".into(),
            source: None,
            storage: "pruned".into(),
            pid: 1,
            utc_start_ms: 0,
            monotonic_start_us: None,
            clock_error_us: 1,
            startup_gate: false,
            verification_detail_version: 1,
        }
    }

    #[test]
    fn burst_survives_a_temporarily_full_collector_socket() {
        const SPANS: usize = 1024;
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("collector.sock");
        let socket = UnixDatagram::bind(&path).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let (recorder, detail, summary) = recorder();
        let runtime = Runtime {
            recorder: recorder.clone(),
        };
        let root = begin_with(&recorder, block()).unwrap();
        for _ in 0..SPANS {
            drop(root.context().span(Stage::BlockChecks));
        }
        root.finish(Outcome::Success);
        let worker = std::thread::spawn(move || export(path, run(), recorder, detail, summary));
        let mut bytes = [0; 8193];
        let n = socket.recv(&mut bytes).unwrap();
        assert!(matches!(
            serde_json::from_slice::<Frame>(&bytes[..n]).unwrap(),
            Frame::Run { .. }
        ));
        // Let the exporter fill the socket while the collector is busy elsewhere.
        std::thread::sleep(Duration::from_millis(100));
        let mut sequence = 0;
        let mut spans = std::collections::BTreeSet::new();
        let deadline = Instant::now() + Duration::from_secs(5);
        while sequence < u64::try_from(SPANS + 3).unwrap() {
            assert!(Instant::now() < deadline, "exporter must drain the burst");
            let n = socket.recv(&mut bytes).unwrap();
            match serde_json::from_slice::<Frame>(&bytes[..n]).unwrap() {
                Frame::Event {
                    sequence: next,
                    data,
                    ..
                } => {
                    assert_eq!(next, sequence + 1, "no dropped or duplicate events");
                    sequence = next;
                    if let Event::Span { span, .. } = data {
                        assert!(spans.insert(span));
                    }
                }
                Frame::Health {
                    transport_dropped, ..
                } => assert_eq!(transport_dropped, 0),
                Frame::Run { .. } => {}
            }
        }
        assert_eq!(spans, (1..=u64::try_from(SPANS).unwrap()).collect());
        drop(runtime);
        worker.join().unwrap();
    }

    fn fill_socket(socket: &UnixDatagram, path: &std::path::Path) {
        for _ in 0..10_000 {
            match socket.send_to(&[0], path) {
                Ok(_) => {}
                Err(error) if retryable_send_error(&error) => return,
                Err(error) => panic!("unexpected socket error: {error}"),
            }
        }
        panic!("test must reach socket backpressure");
    }

    #[test]
    fn permanently_full_socket_has_a_retry_deadline() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("collector.sock");
        let _receiver = UnixDatagram::bind(&path).unwrap();
        let socket = UnixDatagram::unbound().unwrap();
        socket.set_nonblocking(true).unwrap();
        fill_socket(&socket, &path);
        let frame = Frame::Run {
            schema: SCHEMA_VERSION,
            run: run(),
        };
        let started = Instant::now();
        assert!(!send_frame(&socket, &path, &frame, &AtomicBool::new(false)));
        assert!(started.elapsed() >= SEND_RETRY_TIMEOUT);
        assert!(started.elapsed() < SEND_RETRY_TIMEOUT + Duration::from_secs(1));
    }

    #[test]
    fn shutdown_interrupts_a_pending_socket_retry() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("collector.sock");
        let _receiver = UnixDatagram::bind(&path).unwrap();
        let socket = UnixDatagram::unbound().unwrap();
        socket.set_nonblocking(true).unwrap();
        fill_socket(&socket, &path);
        let frame = Frame::Run {
            schema: SCHEMA_VERSION,
            run: run(),
        };
        let stopped = AtomicBool::new(false);
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| send_frame(&socket, &path, &frame, &stopped));
            std::thread::sleep(Duration::from_millis(50));
            assert!(
                !worker.is_finished(),
                "full socket must retain the pending frame"
            );
            let started = Instant::now();
            stopped.store(true, Ordering::Release);
            assert!(!worker.join().unwrap());
            assert!(started.elapsed() < Duration::from_millis(250));
        });
    }
}
