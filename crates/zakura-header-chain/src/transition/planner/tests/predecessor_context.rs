//! Retained and durable predecessor-context reconstruction characterization.

use super::super::event_effects::header_validation::retained_header_context;
use super::*;
use crate::{FullStateEvidenceAuthority, HeaderValidationFacts};

struct NoLeaseAuthority;
impl FullStateEvidenceAuthority for NoLeaseAuthority {
    fn authorizes_full_state(&self, _event: &TransitionEvent) -> bool {
        true
    }

    fn authorizes_validation_lease(&self, _lease: &crate::ValidationLease) -> bool {
        false
    }
}

fn build_chain(count: u32) -> (TestStore, EngineConfig, Vec<Frontier>) {
    let (mut store, config) = TestStore::new(EngineMode::Integrated);
    let tip = insert_verified_branch(
        &mut store.graph,
        store.metadata.frontiers.finalized,
        count,
        regtest_genesis_block().header.difficulty_threshold,
        0x31,
    );
    synchronize_fixture(&mut store, tip);
    let mut frontiers = path(&store.graph, tip).expect("the fixture path is retained");
    assert_eq!(frontiers[0].height, block::Height(0));
    assert_eq!(frontiers.last().copied(), Some(tip));
    // Ensure indexing by height works even if path order changes.
    frontiers.sort_by_key(|frontier| frontier.height);
    (store, config, frontiers)
}

fn header_path(
    store: &TestStore,
    parent: Frontier,
) -> Vec<(Frontier, Arc<zakura_chain::block::Header>)> {
    let mut facts = Vec::new();
    let mut current = parent;
    loop {
        let node = store
            .graph
            .header_node(current.hash)
            .expect("requested path is retained in the fixture");
        facts.push((current, node.header.clone()));
        if current.height == block::Height(0) {
            break;
        }
        current = Frontier::new(
            current
                .height
                .previous()
                .expect("non-zero fixture heights have parents"),
            node.parent_hash,
        );
    }
    facts
}

fn lease_for_path(
    store: &TestStore,
    parent: Frontier,
    path: &[(Frontier, Arc<zakura_chain::block::Header>)],
) -> ValidationLease {
    ValidationLease::new(
        parent,
        path.iter()
            .map(|(frontier, header)| HeaderContextFact {
                frontier: *frontier,
                header: header.clone(),
            })
            .collect(),
        store.lease.network().clone(),
        store.lease.trust_anchor_digest(),
    )
}

#[test]
fn retained_header_context_uses_exact_span_and_newest_to_oldest_order() {
    let (store, config, frontiers) = build_chain(35);
    let clock = ManualClock(Utc::now());
    let authority = Authority;
    let ctx = context(&config, &clock, Some(&authority));

    for &height in &[0_u32, 27, 28, 35] {
        let parent = frontiers[height as usize];
        let expected_len = usize::try_from(height)
            .expect("fixture height fits")
            .saturating_add(1)
            .min(crate::MAX_POW_ADJUSTMENT_BLOCK_SPAN);
        let reconstructed = retained_header_context(&store.graph, parent, None, &ctx)
            .expect("fully retained context succeeds without durable facts");
        assert_eq!(reconstructed.len(), expected_len, "height {height}");

        let expected: Vec<_> = header_path(&store, parent)
            .iter()
            .take(expected_len)
            .map(|(_, header)| (header.difficulty_threshold, header.time))
            .collect();
        assert_eq!(
            reconstructed, expected,
            "height {height} newest-to-oldest order"
        );
    }
}

#[test]
fn retained_header_context_splices_authorized_leases_and_rejects_bad_facts() {
    let (mut store, config, frontiers) = build_chain(35);
    let clock = ManualClock(Utc::now());
    let authority = Authority;
    let ctx = context(&config, &clock, Some(&authority));

    let parent = frontiers[30];
    let full_path = header_path(&store, parent);
    let lease_path: Vec<_> = full_path
        .iter()
        .take(crate::MAX_POW_ADJUSTMENT_BLOCK_SPAN)
        .cloned()
        .collect();
    let matching_lease = lease_for_path(&store, parent, &lease_path);
    let short_lease = lease_for_path(&store, parent, &lease_path[..3]);
    let wrong_digest = ValidationLease::new(
        parent,
        lease_path
            .iter()
            .map(|(frontier, header)| HeaderContextFact {
                frontier: *frontier,
                header: header.clone(),
            })
            .collect(),
        store.lease.network().clone(),
        [0x55; 32],
    );
    let genesis_path = header_path(&store, frontiers[0]);
    let genesis_lease = lease_for_path(&store, frontiers[0], &genesis_path);
    let expected: Vec<_> = lease_path
        .iter()
        .map(|(_, header)| (header.difficulty_threshold, header.time))
        .collect();

    // Advance finality and drop the pruned prefix from the retained graph.
    let new_finalized = frontiers[20];
    store
        .graph
        .advance_finalized_frontier(new_finalized)
        .expect("fixture finality advances along the retained path");
    store.metadata.frontiers.finalized = new_finalized;
    synchronize_fixture(&mut store, frontiers[35]);

    let cases = [
        (
            "authorized coherent lease",
            Some(HeaderValidationFacts {
                validation_leases: vec![matching_lease.clone()],
            }),
            Ok(expected.clone()),
        ),
        (
            "unrelated lease then matching lease",
            Some(HeaderValidationFacts {
                validation_leases: vec![genesis_lease, matching_lease.clone()],
            }),
            Ok(expected.clone()),
        ),
        (
            "missing durable facts",
            None,
            Err(TransitionFailure::MissingDurableFacts(
                "retained predecessor context is incomplete",
            )),
        ),
        (
            "no matching lease",
            Some(HeaderValidationFacts {
                validation_leases: vec![lease_for_path(
                    &store,
                    frontiers[20],
                    &[(
                        frontiers[20],
                        store
                            .graph
                            .header_node(frontiers[20].hash)
                            .expect("new finalized remains")
                            .header
                            .clone(),
                    )],
                )],
            }),
            Err(TransitionFailure::MissingDurableFacts(
                "durable predecessor context is incoherent",
            )),
        ),
        (
            "wrong trust digest",
            Some(HeaderValidationFacts {
                validation_leases: vec![wrong_digest],
            }),
            Err(TransitionFailure::MissingDurableFacts(
                "durable predecessor context is incoherent",
            )),
        ),
        (
            "insufficient lease span",
            Some(HeaderValidationFacts {
                validation_leases: vec![short_lease],
            }),
            Err(TransitionFailure::MissingDurableFacts(
                "durable predecessor context is incoherent",
            )),
        ),
        (
            "empty header-insertion leases",
            Some(HeaderValidationFacts {
                validation_leases: Vec::new(),
            }),
            Err(TransitionFailure::MissingDurableFacts(
                "durable predecessor context is incoherent",
            )),
        ),
    ];

    for (label, facts, expected_result) in cases {
        assert_eq!(
            retained_header_context(&store.graph, parent, facts.as_ref(), &ctx),
            expected_result,
            "{label}"
        );
    }

    let no_lease = NoLeaseAuthority;
    let unauthorized_ctx = TransitionContext {
        config: &config,
        clock: &clock,
        full_state_authority: Some(&no_lease),
        retention_references: &[],
    };
    assert_eq!(
        retained_header_context(
            &store.graph,
            parent,
            Some(&HeaderValidationFacts {
                validation_leases: vec![matching_lease],
            }),
            &unauthorized_ctx,
        ),
        Err(TransitionFailure::MissingDurableFacts(
            "durable predecessor context is incoherent",
        )),
        "coherent lease without lease authority is incoherent"
    );
}

/// The lowest parent height whose full context the graph retains once `finalized` is the
/// finalized height: the context reaches down to `finalized` and no further.
fn lowest_fully_retained_parent(finalized: u32) -> u32 {
    let span = u32::try_from(crate::MAX_POW_ADJUSTMENT_BLOCK_SPAN).expect("the span fits in u32");
    finalized + span - 1
}

/// A chain that retains headers from height `finalized` to its tip, with one durable lease for
/// each requested parent height, built before the headers below `finalized` were dropped.
fn build_finalized_chain(
    finalized: u32,
    lease_parents: &[u32],
) -> (TestStore, EngineConfig, Vec<Frontier>, Vec<ValidationLease>) {
    let (mut store, config, frontiers) = build_chain(lowest_fully_retained_parent(finalized) + 8);
    let leases = lease_parents
        .iter()
        .map(|&height| {
            let parent = frontiers[height as usize];
            let path: Vec<_> = header_path(&store, parent)
                .into_iter()
                .take(crate::MAX_POW_ADJUSTMENT_BLOCK_SPAN)
                .collect();
            lease_for_path(&store, parent, &path)
        })
        .collect();
    let new_finalized = frontiers[finalized as usize];
    store
        .graph
        .advance_finalized_frontier(new_finalized)
        .expect("fixture finality advances along the retained path");
    store.metadata.frontiers.finalized = new_finalized;
    let tip = *frontiers.last().expect("the fixture chain is nonempty");
    synchronize_fixture(&mut store, tip);
    (store, config, frontiers, leases)
}

/// An authority that vouches for one retention reference and authorizes every lease.
struct RetainingAuthority(block::Hash);
impl FullStateEvidenceAuthority for RetainingAuthority {
    fn authorizes_full_state(&self, _event: &TransitionEvent) -> bool {
        true
    }

    fn authorizes_validation_lease(&self, _lease: &ValidationLease) -> bool {
        true
    }

    fn authorizes_retention_reference(&self, reference: block::Hash) -> bool {
        reference == self.0
    }
}

/// Insert `count` headers on `parent`, which need not be the selected tip.
fn insertion_on(store: &TestStore, parent: Frontier, count: u32) -> TransitionRequest {
    let batch = batch(
        parent,
        count,
        store.lease.trust_anchor_digest(),
        EvidenceId::from_digest([0x61; 32]),
    );
    let target = batch.headers().last().expect("the batch is nonempty").hash;
    TransitionRequest {
        expected_version: store.metadata.state_version,
        event: TransitionEvent::InsertHeaders(Box::new(crate::InsertHeaders {
            owner: crate::HeaderWorkOwner {
                authority: crate::HeaderWorkAuthority {
                    header_generation: store.metadata.header_generation,
                    branch: BranchId::new(store.metadata.frontiers.finalized.hash, target),
                },
                session_id: 1,
                request_id: NonZeroU64::new(1).expect("one is nonzero"),
            }
            .into(),
            source: SourceId::from_digest([3; 32]),
            parent_hash: parent.hash,
            target_tip_hash: target,
            completion: TargetCompletion::TargetComplete {
                common_ancestor: parent,
            },
            batch,
            aux: Vec::new(),
        })),
    }
}

/// Full state extends its verified tip by one header the graph does not retain yet.
fn growth_on_verified_tip(store: &TestStore) -> TransitionRequest {
    let old_tip = store.metadata.frontiers.verified_best;
    let batch = batch(
        old_tip,
        1,
        store.lease.trust_anchor_digest(),
        EvidenceId::from_digest([0x62; 32]),
    );
    let new_path = batch
        .headers()
        .iter()
        .map(|header| crate::VerifiedHeaderRef {
            height: header.height,
            hash: header.hash,
            header: header.header.clone(),
        })
        .collect();
    TransitionRequest {
        expected_version: store.metadata.state_version,
        event: TransitionEvent::VerifiedChainChanged(crate::VerifiedChainChanged {
            full_state_transition_id: EvidenceId::from_digest([0x63; 32]),
            old_tip,
            new_path,
            cause: crate::VerifiedChangeCause::Grow,
        }),
    }
}

/// Plan `request` with exactly `leases` as its durable validation facts.
fn plan_with_leases(
    store: &TestStore,
    request: TransitionRequest,
    leases: Vec<ValidationLease>,
    context: &TransitionContext<'_>,
) -> Result<EngineTransition, TransitionFailure> {
    let facts = HeaderValidationFacts {
        validation_leases: leases,
    };
    let input = match fixture_transition_input(store, request) {
        TransitionInput::InsertHeaders { event, facts: old } => TransitionInput::InsertHeaders {
            event,
            facts: HeaderInsertionFacts {
                validation: facts,
                ..old
            },
        },
        TransitionInput::VerifiedChainChanged {
            expected_version,
            event,
            ..
        } => TransitionInput::VerifiedChainChanged {
            expected_version,
            event,
            facts,
        },
        other => other,
    };
    test_engine(store).plan_transition(input, context)
}

#[test]
fn a_lease_is_needed_exactly_when_the_graph_cuts_the_context_short() {
    let finalized = 20;
    let deep = lowest_fully_retained_parent(finalized);
    let (store, config, frontiers, _) = build_finalized_chain(finalized, &[]);
    let clock = ManualClock(Utc::now());
    let authority = Authority;
    let ctx = context(&config, &clock, Some(&authority));
    let engine = test_engine(&store);
    let needs_lease =
        |height: u32| engine.needs_validation_lease(frontiers[height as usize].hash, &ctx);

    assert!(
        !needs_lease(deep),
        "the graph retains every header the context reads from this parent"
    );
    assert!(
        !needs_lease(deep + 8),
        "a parent above a fully retained one is fully retained too"
    );
    assert!(
        needs_lease(deep - 1),
        "one retained header short of the context needs the lease"
    );
    assert!(
        needs_lease(finalized),
        "the graph retains nothing below the finalized header"
    );
    assert!(
        engine.needs_validation_lease(block::Hash([0xee; 32]), &ctx),
        "a parent the graph does not retain needs whatever the store holds"
    );
}

#[test]
fn a_walk_needs_another_headers_lease_only_when_it_runs_short_or_the_lease_vouches_for_a_reference()
{
    let finalized = 20;
    let deep = lowest_fully_retained_parent(finalized);
    let (store, config, frontiers, _) = build_finalized_chain(finalized, &[]);
    let finalized_hash = frontiers[finalized as usize].hash;
    let clock = ManualClock(Utc::now());
    let authority = Authority;
    let ctx = context(&config, &clock, Some(&authority));
    let engine = test_engine(&store);
    let needs_finalized_lease = |walk_from: u32, ctx: &TransitionContext<'_>| {
        engine.needs_validation_lease_for_walk(
            frontiers[walk_from as usize].hash,
            finalized_hash,
            ctx,
        )
    };

    assert!(
        engine.needs_validation_lease(finalized_hash, &ctx),
        "a walk from the finalized header itself runs out at once"
    );
    assert!(
        !needs_finalized_lease(deep, &ctx),
        "a walk from a fully retained parent never reaches past the finalized header"
    );
    assert!(
        needs_finalized_lease(deep - 1, &ctx),
        "a walk one retained header short continues into the finalized header's lease"
    );

    let references = [finalized_hash];
    let unvouched = TransitionContext {
        retention_references: &references,
        ..context(&config, &clock, Some(&authority))
    };
    assert!(
        needs_finalized_lease(deep, &unvouched),
        "only the lease can authenticate the finalized header as a retention reference"
    );
}

#[test]
fn a_retention_reference_only_a_lease_can_authenticate_needs_the_lease() {
    let finalized = 20;
    let (store, config, frontiers, _) = build_finalized_chain(finalized, &[]);
    let parent = frontiers[lowest_fully_retained_parent(finalized) as usize].hash;
    let clock = ManualClock(Utc::now());
    let engine = test_engine(&store);
    let references = [parent];

    let authority = Authority;
    let unvouched = TransitionContext {
        config: &config,
        clock: &clock,
        full_state_authority: Some(&authority),
        retention_references: &references,
    };
    assert!(
        engine.needs_validation_lease(parent, &unvouched),
        "the authority does not vouch for the reference, so only a lease can"
    );

    let retaining = RetainingAuthority(parent);
    let vouched = TransitionContext {
        config: &config,
        clock: &clock,
        full_state_authority: Some(&retaining),
        retention_references: &references,
    };
    assert!(
        !engine.needs_validation_lease(parent, &vouched),
        "the authority vouches for the reference and the graph retains the context"
    );
}

#[test]
fn a_plan_is_the_same_with_and_without_a_lease_the_engine_does_not_need() {
    let finalized = 20;
    let deep = lowest_fully_retained_parent(finalized);
    let tip = deep + 8;
    let (store, config, frontiers, leases) = build_finalized_chain(finalized, &[deep, tip]);
    let clock = ManualClock(Utc::now());
    let authority = Authority;
    let ctx = context(&config, &clock, Some(&authority));
    let engine = test_engine(&store);

    let requests = [
        (
            "headers inserted on the lowest fully retained parent",
            insertion_on(&store, frontiers[deep as usize], 2),
            frontiers[deep as usize],
            leases[0].clone(),
        ),
        (
            "full state grows its verified tip",
            growth_on_verified_tip(&store),
            frontiers[tip as usize],
            leases[1].clone(),
        ),
    ];
    for (label, request, parent, lease) in requests {
        assert!(!engine.needs_validation_lease(parent.hash, &ctx), "{label}");
        let with_lease = plan_with_leases(&store, request.clone(), vec![lease], &ctx)
            .unwrap_or_else(|error| panic!("{label}: plans with the lease: {error:?}"));
        let without_lease = plan_with_leases(&store, request, Vec::new(), &ctx)
            .unwrap_or_else(|error| panic!("{label}: plans without the lease: {error:?}"));

        assert!(
            !with_lease.is_no_change(),
            "{label}: the plan changes state"
        );
        assert_eq!(
            with_lease.change_set(),
            without_lease.change_set(),
            "{label}: the durable writes are the same"
        );
        assert_eq!(
            with_lease.effect(),
            without_lease.effect(),
            "{label}: the reported effect is the same"
        );
    }
}

#[test]
fn a_plan_fails_without_a_lease_the_engine_needs() {
    let finalized = 20;
    let short = lowest_fully_retained_parent(finalized) - 1;
    let (store, config, frontiers, leases) = build_finalized_chain(finalized, &[short]);
    let clock = ManualClock(Utc::now());
    let authority = Authority;
    let ctx = context(&config, &clock, Some(&authority));
    let parent = frontiers[short as usize];
    assert!(test_engine(&store).needs_validation_lease(parent.hash, &ctx));

    let request = insertion_on(&store, parent, 1);
    plan_with_leases(&store, request.clone(), leases, &ctx)
        .expect("the lease completes the context the graph cuts short");
    assert!(matches!(
        plan_with_leases(&store, request, Vec::new(), &ctx),
        Err(TransitionFailure::MissingDurableFacts(_))
    ));
}
