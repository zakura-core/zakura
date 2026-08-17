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
    nonce_seed: u8,
) -> Vec<Frontier> {
    let count = u32::try_from(deadlines.len()).expect("the compact fixture length fits in u32");
    let mut request = insertion(store, count, EvidenceId::from_digest([nonce_seed; 32]));
    let TransitionEvent::InsertHeaders(insert) = &mut request.event else {
        unreachable!("the fixture constructs a header insertion")
    };
    let evidence = insert.batch.evidence();
    let mut headers = insert.batch.headers().to_vec();
    let mut parent_hash = store.lease.parent.hash;
    for (header, deadline) in headers.iter_mut().zip(deadlines) {
        let mut raw_header = *header.header;
        raw_header.previous_block_hash = parent_hash;
        raw_header.nonce.0[31] = nonce_seed;
        header.header = Arc::new(raw_header);
        header.hash = header.header.hash();
        header.validation = HeaderValidationState::DeferredUntil(*deadline);
        parent_hash = header.hash;
    }
    let frontiers = headers
        .iter()
        .map(|header| Frontier::new(header.height, header.hash))
        .collect();
    insert.batch = PreparedHeaderBatch::new(
        headers,
        store.lease.parent,
        config.network().clone(),
        config.trust_anchor_digest(),
        evidence,
    )
    .expect("the deferred fixture batch remains coherent");
    let target = insert
        .batch
        .headers()
        .last()
        .expect("the deferred batch remains nonempty")
        .hash;
    insert.owner = crate::HeaderWorkOwner {
        authority: crate::HeaderWorkAuthority {
            header_generation: store.metadata.header_generation,
            branch: BranchId::new(store.metadata.frontiers.finalized.hash, target),
        },
        session_id: 1,
        request_id: NonZeroU64::new(u64::from(nonce_seed) + 1)
            .expect("the request identity is nonzero"),
    }
    .into();
    insert.target_tip_hash = target;
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
        0,
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

#[test]
fn deferred_reevaluation_settles_headers_only_depth_finality() {
    let (mut store, config) = TestStore::new(EngineMode::HeadersOnly);
    let deadline = Utc::now();
    let insertion_clock = ManualClock(deadline - chrono::Duration::seconds(1));
    let deadlines = vec![deadline; 1_001];
    let nodes = insert_deferred_chain(&mut store, &config, &insertion_clock, &deadlines, 0);
    let old_finalized = store.metadata.frontiers.finalized;

    let transition = apply_transition(
        &store,
        TransitionRequest {
            expected_version: store.metadata.state_version,
            event: TransitionEvent::ReevaluateDeferred,
        },
        &context(&config, &ManualClock(deadline), None),
    )
    .expect("normal deferred reevaluation settles depth finality");

    assert_eq!(transition.change_set.metadata.frontiers.finalized, nodes[0]);
    assert_eq!(
        transition.change_set.metadata.frontiers.header_best,
        nodes[1_000]
    );
    assert_eq!(
        transition.change_set.metadata.frontiers.verified_best,
        nodes[0]
    );
    assert_eq!(
        transition.change_set.finality_append,
        Some(FinalityRecord {
            previous: old_finalized,
            current: nodes[0],
            source: FinalitySource::HeadersOnlyDepth {
                selected_tip: nodes[1_000],
            },
            epoch: FinalityEpoch::new(1),
        })
    );
    assert!(transition.effect().is_headers_only_finality());
    assert_eq!(transition.domain(), TransitionDomain::ReevaluateDeferred);

    store.commit(&transition);
    assert_eq!(store.selected.first(), Some(&nodes[0]));
    assert_eq!(store.selected.last(), Some(&nodes[1_000]));
    assert_eq!(store.selected.len(), 1_001);
    assert_eq!(store.verified, vec![nodes[0]]);
    assert!(store
        .graph
        .header_nodes()
        .all(|node| matches!(node.validation, HeaderValidationState::Valid)));
}

#[test]
fn deferred_reevaluation_evicts_excess_candidate_tips_deterministically() {
    let (mut store, config) = TestStore::new(EngineMode::Integrated);
    let deadline = Utc::now();
    let insertion_clock = ManualClock(deadline - chrono::Duration::seconds(1));
    let mut tips = Vec::new();
    for nonce_seed in 1..=11 {
        tips.extend(insert_deferred_chain(
            &mut store,
            &config,
            &insertion_clock,
            &[deadline],
            nonce_seed,
        ));
    }
    assert!(tips.iter().all(|tip| {
        !store
            .graph
            .header_node(tip.hash)
            .expect("every deferred candidate is retained")
            .is_eligible()
    }));
    let victim = tips
        .iter()
        .copied()
        .min_by_key(|tip| {
            store
                .graph
                .header_chain_score(tip.hash)
                .expect("every deferred candidate has an exact score")
        })
        .expect("the fixture has deferred candidates");

    let transition = apply_transition(
        &store,
        TransitionRequest {
            expected_version: store.metadata.state_version,
            event: TransitionEvent::ReevaluateDeferred,
        },
        &context(&config, &ManualClock(deadline), None),
    )
    .expect("normal settlement evicts excess newly eligible tips");
    let projected = projected_graph(&store.graph, &transition);

    assert_eq!(
        projected.eligible_header_tips().len(),
        config.limits.max_candidate_tips.get()
    );
    assert!(projected.header_node(victim.hash).is_none());
    assert!(transition.change_set.delete_nodes.contains(&victim.hash));
    assert!(projected
        .header_node(transition.change_set.metadata.frontiers.header_best.hash)
        .is_some());
    assert!(projected
        .header_nodes()
        .all(|node| matches!(node.validation, HeaderValidationState::Valid)));
}
