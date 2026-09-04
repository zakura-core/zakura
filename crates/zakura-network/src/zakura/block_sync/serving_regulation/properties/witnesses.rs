//! Required boundaries run deterministically, independently of random coverage.

use super::*;

#[test]
fn every_admission_bound_blocks_then_recovers() {
    for limit in Limit::ALL {
        let peer = if matches!(
            limit,
            Limit::NodeRate | Limit::NodeActive | Limit::NodeBytes
        ) {
            1
        } else {
            0
        };
        let mut scenario = Scenario {
            version: 1,
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
                Action::Advance { millis: 1001 },
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
        assert_eq!(observations.last().unwrap().resources.node_bytes, 0);
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
fn replay_preserves_writing_bytes_across_session_replacement() {
    let scenario: Scenario =
        serde_json::from_str(include_str!("writing_after_reconnect.json")).unwrap();
    let observations = replay(&scenario).unwrap();
    let retained = &observations[7].resources;
    assert_eq!(retained.node_active, 0);
    assert_eq!(retained.node_bytes, block_payload_bytes() + 9);
    assert_eq!(retained.session_bytes[0], retained.node_bytes);
    assert_eq!(retained.session_bytes[2], 0);
    checked_replay(&scenario).unwrap();
}

#[test]
fn replay_rejects_inapplicable_actions_and_unknown_versions() {
    let mut scenario = Scenario {
        version: 1,
        limit: Limit::NodeActive,
        actions: vec![Action::Commit { request: 0 }],
    };
    assert!(replay(&scenario).unwrap_err().contains("invalid action"));
    scenario.version = 2;
    assert!(replay(&scenario)
        .unwrap_err()
        .contains("unsupported scenario"));
}

#[test]
fn pending_bounds_include_both_sessions_and_recover_after_release() {
    use Action::*;
    let actions = vec![
        RetainInput { peer: 0, input: 0 },
        RetainInput { peer: 0, input: 1 },
        RetainInput { peer: 0, input: 2 },
        RetainInput { peer: 1, input: 2 },
        RetainInput { peer: 1, input: 3 },
        DropInput { input: 0 },
        RetainInput { peer: 1, input: 3 },
        DropInput { input: 1 },
        DropInput { input: 2 },
        DropInput { input: 3 },
    ];
    let scenario = Scenario {
        version: 1,
        limit: Limit::NodeActive,
        actions,
    };
    let observations = replay(&scenario).unwrap();
    assert_eq!(observations[2].outcome, Outcome::Retained(false));
    assert_eq!(observations[4].outcome, Outcome::Retained(false));
    assert_eq!(observations[6].outcome, Outcome::Retained(true));
    checked_replay(&scenario).unwrap();
}

#[test]
fn queue_failure_spends_nothing_and_query_leases_cannot_execute_twice() {
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
        DropLedger { request: 0 },
        DropQueryLease { request: 0 },
        DropQueryLease { request: 0 },
        Admit {
            peer: 0,
            request: 1,
        },
        Commit { request: 1 },
        QueueBlock { request: 1 },
        BeginWrite { session: 0 },
        QueueBlock { request: 1 },
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
        version: 1,
        limit: Limit::PeerActive,
        actions,
    };
    let observations = replay(&scenario).unwrap();
    assert_eq!(observations[3].outcome, Outcome::Started(true));
    assert_eq!(observations[4].outcome, Outcome::Started(false));
    assert_eq!(observations[12].outcome, Outcome::Queued(false));
    assert_eq!(observations[11].resources, observations[12].resources);
    assert_eq!(observations[14].outcome, Outcome::Queued(true));
    checked_replay(&scenario).unwrap();
}

proptest! {
    #[test]
    fn response_cost_matches_independent_wire_arithmetic(
        requested in 1u32..=128,
        advertised_count in 1u32..=128,
        body_cap in 1u32..=33_554_432,
        overhead in 1u64..=1_000_000,
    ) {
        let mut config = super::super::ZakuraBlockSyncConfig {
            max_blocks_per_response: advertised_count,
            max_response_bytes: body_cap,
            ..Default::default()
        };
        config.get_blocks_regulation.request_overhead_bytes = overhead;
        let count = requested.min(advertised_count);
        let payload = (u64::from(count) * 2_000_000).min(u64::from(body_cap)) + u64::from(count) + 9;
        let actual = super::super::serving_cost(&config, requested).unwrap();
        prop_assert_eq!(actual.count, count);
        prop_assert_eq!(actual.response_cap, payload);
        prop_assert_eq!(actual.charge, payload + overhead);
    }
}

#[tokio::test(start_paused = true)]
async fn pending_wait_accounts_for_its_partial_session_reservation() {
    use super::super::GetBlocksServingRegulator;
    use crate::zakura::ZakuraPeerId;
    use futures::FutureExt;
    use zakura_chain::block::Height;

    let regulator = GetBlocksServingRegulator::new(Limit::NodeActive.config());
    let first = regulator.session(ZakuraPeerId::new(vec![1; 32]).unwrap(), 0);
    let second = regulator.session(ZakuraPeerId::new(vec![2; 32]).unwrap(), 1);
    let first_input = first.try_retain_input(Height(1), 1).unwrap();
    let other_input = first.try_retain_input(Height(2), 1).unwrap();
    let second_input = second.try_retain_input(Height(3), 1).unwrap();
    let mut waiting = Box::pin(second.retain_input(Height(4), 1));
    assert!(waiting.as_mut().now_or_never().is_none());
    assert_eq!(regulator.snapshot().node_pending, 3);
    assert_eq!(regulator.snapshot().session_pending, 4);
    drop(waiting);
    assert_eq!(regulator.snapshot().session_pending, 3);

    let mut waiting = Box::pin(second.retain_input(Height(4), 1));
    assert!(waiting.as_mut().now_or_never().is_none());
    drop(first_input);
    let admitted = waiting.as_mut().now_or_never().unwrap();
    assert_eq!(regulator.snapshot().node_pending, 3);
    assert_eq!(regulator.snapshot().session_pending, 3);
    drop((admitted, other_input, second_input));
    assert_eq!(regulator.snapshot().node_pending, 0);
    assert_eq!(regulator.snapshot().session_pending, 0);
}

#[tokio::test(start_paused = true)]
async fn identity_cache_evicts_inactive_accounts_but_preserves_live_work() {
    use super::super::GetBlocksServingRegulator;
    use crate::zakura::ZakuraPeerId;

    let mut config = Limit::PeerActive.config();
    config.peer_limits.max_inbound_peers = 1;
    config.peer_limits.max_outbound_peers = 1;
    let regulator = GetBlocksServingRegulator::new(config);
    let identity = ZakuraPeerId::new(vec![1; 32]).unwrap();
    let session = regulator.session(identity.clone(), 0);
    let held = session.try_admit(1).unwrap().commit();
    let expected = session.peer_rate_available();
    drop(session);
    for id in 2u8..16 {
        let session = regulator.session(ZakuraPeerId::new(vec![id; 32]).unwrap(), u64::from(id));
        drop(session.try_admit(1).unwrap().commit());
        drop(session);
        assert!(
            regulator.inner.peer_rates.lock().unwrap().len() <= 3,
            "the cache holds the live owner and at most two inactive accounts after insertion"
        );
    }
    let replacement = regulator.session(identity, 16);
    assert_eq!(replacement.peer_rate_available(), expected);
    drop(held);
    assert_eq!(regulator.snapshot().node_active, 0);
}

#[test]
fn concrete_replay_accepts_times_outside_the_generation_distribution() {
    let scenario = Scenario {
        version: 1,
        limit: Limit::NodeActive,
        actions: vec![Action::Advance { millis: 37 }],
    };
    checked_replay(&scenario).unwrap();
}
