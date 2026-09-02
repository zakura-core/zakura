mod commonware;
mod model;
mod real_iroh;
mod runner;
mod scenario;
mod trace;
mod turmoil;

use proptest::prelude::*;

use crate::zakura::{control_payload_length_is_admissible, MAX_CONTROL_PAYLOAD_BYTES};

use self::{
    commonware::run_commonware,
    model::{HandshakeOutcome, ReferenceModel},
    real_iroh::{check_pending_handshake_isolation, run_real_iroh},
    runner::{load_scenario, persist_failure},
    scenario::{handshake_scenarios, FaultAction, HandshakeScenario, MessageMutation},
    turmoil::run_turmoil,
};

const NAMED_MUTATIONS: [MessageMutation; 27] = [
    MessageMutation::None,
    MessageMutation::HelloZeroLength,
    MessageMutation::HelloOversizeLength,
    MessageMutation::HelloBadMagic,
    MessageMutation::HelloUnsupportedControlVersion,
    MessageMutation::HelloWrongProtocolVersion,
    MessageMutation::HelloWrongPath,
    MessageMutation::HelloWrongRole,
    MessageMutation::HelloWrongNetwork,
    MessageMutation::HelloWrongChain,
    MessageMutation::HelloWrongIdentity,
    MessageMutation::HelloUpgradeNonce,
    MessageMutation::HelloTranscript,
    MessageMutation::HelloMissingCapability,
    MessageMutation::HelloUnsupportedChannel,
    MessageMutation::HelloZeroLimit,
    MessageMutation::HelloTrailingByte,
    MessageMutation::AckBadMagic,
    MessageMutation::AckZeroLength,
    MessageMutation::AckOversizeLength,
    MessageMutation::AckWrongProtocolVersion,
    MessageMutation::AckWrongRemoteNonce,
    MessageMutation::AckExtraCapability,
    MessageMutation::AckExtraChannel,
    MessageMutation::AckZeroLimit,
    MessageMutation::AckLimitAboveRequest,
    MessageMutation::AckTrailingByte,
];

const NAMED_FAULTS: [FaultAction; 9] = [
    FaultAction::None,
    FaultAction::DelayMillis(1),
    FaultAction::ShortWrites(1),
    FaultAction::StallBeforeHello,
    FaultAction::StallBeforeAck,
    FaultAction::CloseBeforeHello,
    FaultAction::CloseBeforeAck,
    FaultAction::CancelBeforeHello,
    FaultAction::CancelBeforeAck,
];

static REAL_IROH_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 64,
        max_shrink_iters: 2_048,
        .. ProptestConfig::default()
    })]

    #[test]
    #[allow(clippy::print_stderr)]
    fn commonware_matches_the_reference_model(scenario in handshake_scenarios()) {
        let expected = ReferenceModel::evaluate(&scenario);
        let first = run_commonware(&scenario);
        let replay = run_commonware(&scenario);

        if first.outcome != expected || first.trace != replay.trace || first.outcome != replay.outcome {
            let path = persist_failure("commonware", &scenario, &first);
            eprintln!("replay with ZAKURA_HANDSHAKE_REPLAY={}", path.display());
        }

        prop_assert_eq!(&first.outcome, &expected);
        prop_assert_eq!(first.negotiated, ReferenceModel::negotiated(&scenario));
        prop_assert_eq!(&first.outcome, &replay.outcome);
        prop_assert_eq!(&first.trace, &replay.trace);
        prop_assert_eq!(&first.trace_hash, &replay.trace_hash);
        prop_assert!(first.runtime_audit.is_some());
        prop_assert!(replay.runtime_audit.is_some());
        prop_assert_eq!(first.pending_tasks, 0);
        prop_assert_eq!(first.open_streams, 0);

        if first.outcome == HandshakeOutcome::Established {
            let negotiated = first.negotiated.expect("established peers report negotiated values");
            prop_assert!(negotiated.limits.max_frame_bytes > 0);
            prop_assert!(negotiated.limits.max_frame_bytes <= scenario.initiator_frame_bytes);
            prop_assert!(negotiated.limits.max_frame_bytes <= scenario.responder_frame_bytes);
            prop_assert!(negotiated.limits.max_message_bytes > 0);
            prop_assert!(negotiated.limits.max_message_bytes <= negotiated.limits.max_frame_bytes);
            prop_assert!(negotiated.limits.max_open_streams > 0);
            prop_assert!(negotiated.limits.max_inbound_queue_depth > 0);
            prop_assert!(negotiated.limits.idle_timeout_millis > 0);
            prop_assert_eq!(negotiated.accepted_capabilities, 0b101);
        } else {
            prop_assert!(first.negotiated.is_none());
        }
    }

    #[test]
    fn control_length_admission_matches_the_bound(
        declared in any::<u32>(),
        configured_max in any::<u32>(),
    ) {
        let hard_max = u32::try_from(MAX_CONTROL_PAYLOAD_BYTES)
            .expect("the hard control payload cap fits u32");
        prop_assert_eq!(
            control_payload_length_is_admissible(declared, configured_max),
            declared != 0 && declared <= configured_max && declared <= hard_max,
        );
    }
}

#[test]
fn replay_handshake_artifact_from_env() {
    let Some(path) = std::env::var_os("ZAKURA_HANDSHAKE_REPLAY") else {
        return;
    };
    let (backend, scenario) = load_scenario(std::path::Path::new(&path));
    let expected = ReferenceModel::evaluate(&scenario);
    let result = match backend.as_str() {
        "commonware" => run_commonware(&scenario),
        "turmoil" => run_turmoil(&scenario),
        _ => panic!("unsupported handshake replay backend {backend}"),
    };
    assert_eq!(result.outcome, expected);
}

#[test]
fn turmoil_runs_the_production_core_over_simulated_tcp() {
    for (index, mutation) in NAMED_MUTATIONS.into_iter().enumerate() {
        let scenario = HandshakeScenario::conformant(
            100 + u64::try_from(index).expect("the mutation index fits u64"),
        )
        .with_mutation(mutation);
        let expected = ReferenceModel::evaluate(&scenario);
        let result = run_turmoil(&scenario);
        assert_eq!(result.outcome, expected, "scenario: {scenario:#?}");
        assert_eq!(result.pending_tasks, 0, "scenario: {scenario:#?}");
        assert_eq!(result.open_streams, 0, "scenario: {scenario:#?}");
    }

    for (index, fault) in NAMED_FAULTS.into_iter().enumerate() {
        let scenario = HandshakeScenario::conformant(
            200 + u64::try_from(index).expect("the fault index fits u64"),
        )
        .with_fault(fault);
        let expected = ReferenceModel::evaluate(&scenario);
        let result = run_turmoil(&scenario);
        assert_eq!(result.outcome, expected, "scenario: {scenario:#?}");
        assert_eq!(result.pending_tasks, 0, "scenario: {scenario:#?}");
        assert_eq!(result.open_streams, 0, "scenario: {scenario:#?}");
    }
}

#[test]
fn commonware_runs_every_named_mutation_and_fault() {
    for (index, mutation) in NAMED_MUTATIONS.into_iter().enumerate() {
        let scenario = HandshakeScenario::conformant(
            300 + u64::try_from(index).expect("the mutation index fits u64"),
        )
        .with_mutation(mutation);
        assert_eq!(
            run_commonware(&scenario).outcome,
            ReferenceModel::evaluate(&scenario),
            "scenario: {scenario:#?}",
        );
    }

    for (index, fault) in NAMED_FAULTS.into_iter().enumerate() {
        let scenario = HandshakeScenario::conformant(
            400 + u64::try_from(index).expect("the fault index fits u64"),
        )
        .with_fault(fault);
        assert_eq!(
            run_commonware(&scenario).outcome,
            ReferenceModel::evaluate(&scenario),
            "scenario: {scenario:#?}",
        );
    }
}

#[test]
fn commonware_auditor_replays_a_conformant_handshake() {
    let scenario = HandshakeScenario::conformant(92);
    let first = run_commonware(&scenario);
    let replay = run_commonware(&scenario);

    assert_eq!(first.runtime_audit, replay.runtime_audit);
}

#[test]
fn commonware_and_turmoil_agree_on_the_overlap() {
    let scenario = HandshakeScenario::conformant(91);
    let commonware = run_commonware(&scenario);
    let turmoil = run_turmoil(&scenario);

    assert_eq!(commonware.outcome, HandshakeOutcome::Established);
    assert_eq!(commonware.outcome, turmoil.outcome);
    assert_eq!(commonware.negotiated, turmoil.negotiated);
}

#[test]
fn real_iroh_matches_the_deterministic_backends() {
    let _guard = REAL_IROH_TEST_LOCK
        .lock()
        .expect("the real-Iroh test lock is not poisoned");
    let scenario = HandshakeScenario::conformant(93);
    let commonware = run_commonware(&scenario);
    let turmoil = run_turmoil(&scenario);
    let real_iroh = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the real-Iroh test runtime builds")
        .block_on(run_real_iroh(&scenario));

    assert_eq!(real_iroh.outcome, HandshakeOutcome::Established);
    assert_eq!(real_iroh.outcome, commonware.outcome);
    assert_eq!(real_iroh.outcome, turmoil.outcome);
    assert_eq!(real_iroh.negotiated, commonware.negotiated);
    assert_eq!(real_iroh.negotiated, turmoil.negotiated);
}

#[test]
fn stalled_real_iroh_handshake_does_not_starve_a_healthy_peer() {
    let _guard = REAL_IROH_TEST_LOCK
        .lock()
        .expect("the real-Iroh test lock is not poisoned");
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the real-Iroh isolation test runtime builds")
        .block_on(check_pending_handshake_isolation());
}
