use super::*;

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
    assert!(std::mem::size_of::<Event>() * (DETAIL_CAPACITY + SUMMARY_CAPACITY) < 8 * 1024 * 1024);
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
    for _ in 0..1000 {
        drop(root.context().span(Stage::Transaction));
    }
    drop(root.context().span(Stage::WriterOccupied));
    let events: Vec<_> = detail.try_iter().collect();
    assert_eq!(events.len(), 129);
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
