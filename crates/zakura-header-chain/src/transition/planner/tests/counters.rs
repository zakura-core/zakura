use super::*;

fn assert_counter_exhausted(
    result: Result<EngineTransition, TransitionFailure>,
    expected_counter: &str,
) {
    let TransitionFailure::Counter(error) =
        result.expect_err("the exhausted planner counter must fail closed")
    else {
        panic!("the planner returned a non-counter failure");
    };
    assert_eq!(
        error.to_string(),
        format!("header-chain {expected_counter} counter is exhausted at u64::MAX")
    );
}

#[test]
fn planner_fails_closed_for_each_exhausted_durable_counter() {
    let (base, config) = TestStore::new(EngineMode::HeadersOnly);
    let clock = ManualClock(Utc::now());

    let mut state_exhausted = base.clone();
    state_exhausted.metadata.state_version = StateVersion::new(u64::MAX);
    assert_counter_exhausted(
        apply_transition(
            &state_exhausted,
            insertion(&state_exhausted, 1, EvidenceId::from_digest([0x90; 32])),
            &context(&config, &clock, None),
        ),
        "state version",
    );

    let mut header_exhausted = base.clone();
    header_exhausted.metadata.header_generation = HeaderGeneration::new(u64::MAX);
    assert_counter_exhausted(
        apply_transition(
            &header_exhausted,
            insertion(&header_exhausted, 1, EvidenceId::from_digest([0x91; 32])),
            &context(&config, &clock, None),
        ),
        "header generation",
    );

    let mut finality_exhausted = base;
    finality_exhausted.metadata.finality_epoch = FinalityEpoch::new(u64::MAX);
    let mut shallow_finality = config;
    shallow_finality.limits.local_finality_depth =
        std::num::NonZeroU32::new(1).expect("one is nonzero");
    assert_counter_exhausted(
        apply_transition(
            &finality_exhausted,
            insertion(&finality_exhausted, 2, EvidenceId::from_digest([0x92; 32])),
            &context(&shallow_finality, &clock, None),
        ),
        "finality epoch",
    );

    let (mut verified_exhausted, integrated) = TestStore::new(EngineMode::Integrated);
    let inserted = apply_transition(
        &verified_exhausted,
        insertion(&verified_exhausted, 1, EvidenceId::from_digest([0x93; 32])),
        &context(&integrated, &clock, None),
    )
    .expect("the verified-counter fixture inserts one header");
    verified_exhausted.commit(&inserted);
    verified_exhausted.metadata.verified_generation = VerifiedGeneration::new(u64::MAX);
    let child = verified_exhausted.selected[1];
    let header = verified_exhausted
        .graph
        .header_node(child.hash)
        .expect("the inserted child remains retained")
        .header
        .clone();
    assert_counter_exhausted(
        apply_transition(
            &verified_exhausted,
            TransitionRequest {
                expected_version: verified_exhausted.metadata.state_version,
                event: TransitionEvent::VerifiedChainChanged(crate::VerifiedChainChanged {
                    full_state_transition_id: EvidenceId::from_digest([0x94; 32]),
                    old_tip: verified_exhausted.metadata.frontiers.verified_best,
                    new_path: vec![crate::VerifiedHeaderRef {
                        height: child.height,
                        hash: child.hash,
                        header,
                    }],
                    cause: crate::VerifiedChangeCause::Grow,
                }),
            },
            &context(&integrated, &clock, Some(&Authority)),
        ),
        "verified generation",
    );
}
