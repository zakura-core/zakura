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
            .min(crate::POW_ADJUSTMENT_BLOCK_SPAN);
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
        .take(crate::POW_ADJUSTMENT_BLOCK_SPAN)
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
