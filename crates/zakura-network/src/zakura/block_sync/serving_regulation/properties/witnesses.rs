//! Required boundaries run deterministically, independently of random coverage.

use super::*;

#[test]
fn every_admission_bound_blocks_then_recovers() {
    for limit in Limit::ALL {
        let peer = if matches!(limit, Limit::NodeActive) {
            1
        } else {
            0
        };
        let mut scenario = Scenario {
            version: 4,
            limit,
            actions: vec![
                Action::Admit {
                    peer: 0,
                    request: 0,
                },
                Action::Commit { request: 0 },
                Action::ClaimQuery { request: 0 },
                Action::Admit { peer, request: 1 },
                Action::DropLedger { request: 0 },
                Action::DropQueryLease { request: 0 },
                Action::Advance { millis: 0 },
                Action::Admit { peer, request: 1 },
                Action::DropLedger { request: 1 },
            ],
        };
        let observations = replay(&scenario).unwrap();
        assert_eq!(observations[3].outcome, Outcome::Admission(Some(limit)));
        assert_eq!(
            observations[4].resources.node_active, 1,
            "a cancelled running read still owns capacity"
        );
        assert_eq!(observations[7].outcome, Outcome::Admission(None));
        assert_eq!(observations.last().unwrap().resources.node_active, 0);
        checked_replay(&scenario).unwrap();
        // A provisional rollback must restore the original balances too.
        scenario.actions = vec![
            Action::Admit {
                peer: 0,
                request: 0,
            },
            Action::DropLedger { request: 0 },
        ];
        checked_replay(&scenario).unwrap();
    }
}

#[test]
fn replay_preserves_writing_ownership_across_session_replacement() {
    let scenario: Scenario =
        serde_json::from_str(include_str!("writing_after_reconnect.json")).unwrap();
    let observations = replay(&scenario).unwrap();
    let retained = &observations[8].resources;
    assert_eq!(retained.node_active, 1);
    assert_eq!(retained.session_active[0], 1);
    assert_eq!(retained.session_active[2], 0);
    checked_replay(&scenario).unwrap();
}

#[test]
fn replay_rejects_inapplicable_actions_and_unknown_versions() {
    let mut scenario = Scenario {
        version: 4,
        limit: Limit::NodeActive,
        actions: vec![Action::Commit { request: 0 }],
    };
    assert!(replay(&scenario).unwrap_err().contains("invalid action"));
    scenario.version = 3;
    assert!(replay(&scenario)
        .unwrap_err()
        .contains("unsupported scenario"));
}

#[test]
fn queue_failure_keeps_ownership_and_query_leases_cannot_execute_twice() {
    use Action::*;
    let mut model = Model::new(Limit::PeerActive, block_payload_bytes());
    let mut actions = vec![
        Admit {
            peer: 0,
            request: 0,
        },
        Commit { request: 0 },
        CloneQueryLease { request: 0 },
        ClaimQuery { request: 0 },
        ClaimQuery { request: 0 },
        QueueBlock { request: 0 },
        QueueTerminal { request: 0 },
        BeginWrite { session: 0 },
        QueueTerminal { request: 0 },
        DropLedger { request: 0 },
        DropQueryLease { request: 0 },
        DropQueryLease { request: 0 },
        Admit {
            peer: 0,
            request: 1,
        },
        EndWrite {
            session: 0,
            outcome: WriteEnd::Fail,
        },
    ];
    for action in &actions {
        model.apply(action);
    }
    actions.extend(model.cleanup());
    let scenario = Scenario {
        version: 4,
        limit: Limit::PeerActive,
        actions,
    };
    let observations = replay(&scenario).unwrap();
    assert_eq!(observations[3].outcome, Outcome::Started(true));
    assert_eq!(observations[4].outcome, Outcome::Started(false));
    assert_eq!(observations[6].outcome, Outcome::Queued(false));
    assert_eq!(observations[5].resources, observations[6].resources);
    assert_eq!(observations[8].outcome, Outcome::Queued(true));
    assert_eq!(
        observations[12].outcome,
        Outcome::Admission(Some(Limit::PeerActive))
    );
    checked_replay(&scenario).unwrap();
}

proptest! {
    #[test]
    fn response_cost_matches_independent_wire_arithmetic(
        requested in 1u32..=128,
        advertised_count in 1u32..=128,
        body_cap in 2_000_000u32..=33_554_432,
    ) {
        let config = super::super::ZakuraBlockSyncConfig {
            max_blocks_per_response: advertised_count,
            max_response_bytes: body_cap,
            ..Default::default()
        };
        prop_assert_eq!(config.validate(), Ok(()));
        let count = requested.min(advertised_count);
        let payload = (u64::from(count) * 2_000_000).min(u64::from(body_cap)) + u64::from(count) + 9;
        let actual = super::super::serving_cost(&config, requested).unwrap();
        prop_assert_eq!(actual.count, count);
        prop_assert_eq!(actual.response_cap, payload);
        prop_assert!(actual.response_cap >= 2_000_000 + 1 + 9,
            "every accepted configuration reserves room for any first block and its terminal");
    }
}

#[test]
fn concrete_replay_accepts_times_outside_the_generation_distribution() {
    let scenario = Scenario {
        version: 4,
        limit: Limit::NodeActive,
        actions: vec![Action::Advance { millis: 37 }],
    };
    checked_replay(&scenario).unwrap();
}

#[test]
fn reconnect_cannot_bypass_old_query_or_write_ownership() {
    use Action::*;
    // Cover a storage read alone, and each way an in-progress write can end.
    for write_end in [
        None,
        Some(WriteEnd::Complete),
        Some(WriteEnd::Fail),
        Some(WriteEnd::Cancel),
    ] {
        let mut actions = vec![
            Admit {
                peer: 0,
                request: 0,
            },
            Commit { request: 0 },
            ClaimQuery { request: 0 },
        ];
        if write_end.is_some() {
            actions.extend([
                DropQueryLease { request: 0 },
                QueueBlock { request: 0 },
                BeginWrite { session: 0 },
            ]);
        }
        actions.push(Reconnect { peer: 0 });
        let blocked_step = actions.len();
        actions.extend([
            Admit {
                peer: 0,
                request: 1,
            },
            Admit {
                peer: 1,
                request: 2,
            },
            DropLedger { request: 2 },
        ]);
        actions.push(match write_end {
            Some(outcome) => EndWrite {
                session: 0,
                outcome,
            },
            None => DropQueryLease { request: 0 },
        });
        let recovered_step = actions.len();
        actions.extend([
            Admit {
                peer: 0,
                request: 1,
            },
            DropLedger { request: 1 },
        ]);
        let scenario = Scenario {
            version: 4,
            limit: Limit::PeerActive,
            actions,
        };
        let observations = replay(&scenario).unwrap();
        assert_eq!(
            observations[blocked_step].outcome,
            Outcome::Admission(Some(Limit::PeerActive))
        );
        assert_eq!(observations[blocked_step].resources.peer_active, [1, 0]);
        assert_eq!(
            observations[blocked_step].resources.session_active,
            vec![1, 0, 0]
        );
        assert_eq!(
            observations[blocked_step + 1].outcome,
            Outcome::Admission(None)
        );
        assert_eq!(
            observations[recovered_step].outcome,
            Outcome::Admission(None)
        );
        assert_eq!(observations.last().unwrap().resources.node_active, 0);
        checked_replay(&scenario).unwrap();
    }
}
