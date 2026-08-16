use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

struct CountingSteppingClock {
    first: DateTime<Utc>,
    calls: AtomicUsize,
}

impl CountingSteppingClock {
    fn new(first: DateTime<Utc>) -> Self {
        Self {
            first,
            calls: AtomicUsize::new(0),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl super::super::super::Clock for CountingSteppingClock {
    fn now(&self) -> DateTime<Utc> {
        let step = self.calls.fetch_add(1, Ordering::SeqCst);
        self.first
            + chrono::Duration::seconds(
                i64::try_from(step).expect("the test clock call count fits in i64"),
            )
    }
}

fn insert_deferred_chain(
    store: &mut TestStore,
    config: &EngineConfig,
    clock: &ManualClock,
    deadlines: &[DateTime<Utc>],
) -> Vec<Frontier> {
    let count = u32::try_from(deadlines.len()).expect("the compact fixture length fits in u32");
    let mut request = insertion(store, count, EvidenceId::from_digest([0xd8; 32]));
    let TransitionEvent::InsertHeaders(insert) = &mut request.event else {
        unreachable!("the fixture constructs a header insertion")
    };
    let evidence = insert.batch.evidence();
    let mut headers = insert.batch.headers().to_vec();
    for (header, deadline) in headers.iter_mut().zip(deadlines) {
        header.validation = HeaderValidationState::DeferredUntil(*deadline);
    }
    let frontiers = headers
        .iter()
        .map(|header| Frontier::new(header.height, header.hash))
        .collect();
    insert.batch = PreparedHeaderBatch::new(
        headers,
        store.lease.parent,
        config.network.clone(),
        config.trust_anchor_digest(),
        evidence,
    )
    .expect("the deferred fixture batch remains coherent");
    let plan = apply_transition(store, request, &context(config, clock, None))
        .expect("future-time headers are retained while deferred");
    store.commit(&plan);
    frontiers
}

#[test]
fn deferred_reevaluation_uses_one_time_sample_and_exact_freshness_boundaries() {
    let (mut store, config) = TestStore::new(EngineMode::HeadersOnly);
    let deadline = Utc::now();
    let future_deadline = deadline + chrono::Duration::seconds(1);
    let insertion_clock = ManualClock(deadline - chrono::Duration::seconds(1));
    let nodes = insert_deferred_chain(
        &mut store,
        &config,
        &insertion_clock,
        &[deadline, future_deadline],
    );
    assert_eq!(
        store.metadata.frontiers.header_best,
        store.graph.finalized_frontier()
    );

    let before_due = store.snapshot();
    let stepping_clock = CountingSteppingClock::new(deadline);
    let due = apply_transition(
        &store,
        TransitionRequest {
            expected_version: before_due.state_version,
            event: TransitionEvent::ReevaluateDeferred,
        },
        &TransitionContext {
            config: &config,
            clock: &stepping_clock,
            full_state_authority: None,
            retention_references: &[],
        },
    )
    .expect("a header is due at its exact deadline");
    assert_eq!(
        stepping_clock.calls(),
        1,
        "all deferred nodes must use one transition-wide time sample"
    );
    let due_graph = projected_graph(&store.graph, &due);
    assert_eq!(
        due_graph
            .header_node(nodes[0].hash)
            .expect("the exact-deadline node remains retained")
            .validation,
        HeaderValidationState::Valid
    );
    assert_eq!(
        due_graph
            .header_node(nodes[1].hash)
            .expect("the future node remains retained")
            .validation,
        HeaderValidationState::DeferredUntil(future_deadline)
    );
    assert_eq!(due.change_set.metadata.frontiers.header_best, nodes[0]);
    assert_eq!(
        due.change_set.metadata.state_version,
        before_due
            .state_version
            .checked_next()
            .expect("the test state version has capacity")
    );
    assert_eq!(
        due.change_set.metadata.header_generation,
        before_due
            .header_generation
            .checked_next()
            .expect("the test header generation has capacity")
    );
    assert_eq!(
        due.change_set.metadata.verified_generation,
        before_due.verified_generation
    );
    store.commit(&due);

    let stale_clock = CountingSteppingClock::new(future_deadline);
    assert!(matches!(
        apply_transition(
            &store,
            TransitionRequest {
                expected_version: before_due.state_version,
                event: TransitionEvent::ReevaluateDeferred,
            },
            &TransitionContext {
                config: &config,
                clock: &stale_clock,
                full_state_authority: None,
                retention_references: &[],
            },
        ),
        Err(TransitionFailure::Stale { current }) if current == store.metadata.state_version
    ));
    assert_eq!(
        stale_clock.calls(),
        0,
        "stale work is rejected before consulting local time"
    );

    let future_only = apply_transition(
        &store,
        TransitionRequest {
            expected_version: store.metadata.state_version,
            event: TransitionEvent::ReevaluateDeferred,
        },
        &context(&config, &ManualClock(deadline), None),
    )
    .expect("a not-yet-due reevaluation is a verified no-change");
    assert!(future_only.is_no_change());
    assert_eq!(future_only.change_set.metadata, store.metadata);

    let before_future_due = store.snapshot();
    let future_due = apply_transition(
        &store,
        TransitionRequest {
            expected_version: before_future_due.state_version,
            event: TransitionEvent::ReevaluateDeferred,
        },
        &context(&config, &ManualClock(future_deadline), None),
    )
    .expect("the remaining node is due at its exact deadline");
    let future_graph = projected_graph(&store.graph, &future_due);
    assert_eq!(
        future_graph
            .header_node(nodes[1].hash)
            .expect("the future node remains retained")
            .validation,
        HeaderValidationState::Valid
    );
    assert_eq!(
        future_due.change_set.metadata.frontiers.header_best,
        nodes[1]
    );
    assert_eq!(
        future_due.change_set.metadata.header_generation,
        before_future_due
            .header_generation
            .checked_next()
            .expect("the test header generation has capacity")
    );
}
