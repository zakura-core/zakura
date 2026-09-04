//! Boundary-biased generators, focused histories, and coverage assertions.
//!
//! Focused cases make required contract classes deterministic. Random cases
//! then compose the same total operations into longer histories that proptest
//! can shrink and replay.

use std::cell::RefCell;

use proptest::{collection::vec, prelude::*, test_runner::TestCaseError};

use super::super::runner::{assert_contract_test_manifest, GeneratedTestConfig};
use super::{
    replay_serving_case, ByteCap, CompletionKind, DisconnectWhich, QuerySelector, ServingCase,
    ServingCoverage, ServingOp, ServingRequirement, ServingStep, StatusValidity,
};
use crate::zakura::ServicePeerDirection;

const CASES_VARIABLE: &str = "ZAKURA_SERVING_MODEL_CASES";
const SEED_VARIABLE: &str = "ZAKURA_SERVING_MODEL_SEED";
const DEFAULT_CASES: u32 = 64;

const GB_SM_TEST_MANIFEST: &[(&str, &[&str])] = &[
    (
        "GB-SM-01",
        &["gb_sm_01_replacement_cancels_previous_session"],
    ),
    (
        "GB-SM-02",
        &["gb_sm_02_stale_disconnect_preserves_current_session"],
    ),
    ("GB-SM-03", &["gb_sm_03_missing_status_is_rejected_as_spam"]),
    (
        "GB-SM-04",
        &["gb_sm_04_peer_ledgers_are_independent_and_bounded"],
    ),
    (
        "GB-SM-05",
        &["gb_sm_05_saturated_ledger_rejects_without_state_query"],
    ),
    (
        "GB-SM-06",
        &["gb_sm_06_above_tip_request_is_unavailable_without_state_query"],
    ),
    (
        "GB-SM-07",
        &["gb_sm_07_accepted_query_count_respects_all_bounds"],
    ),
    ("GB-SM-08", &["gb_sm_08_request_ids_are_nonzero_and_unique"]),
    (
        "GB-SM-09",
        &["gb_sm_09_ready_response_sends_largest_valid_prefix_and_one_terminal"],
    ),
    (
        "GB-SM-10",
        &["gb_sm_10_invalid_completion_has_no_serving_effect"],
    ),
    (
        "GB-SM-11",
        &["gb_sm_11_repeated_completion_does_not_release_live_slot"],
    ),
    (
        "GB-SM-12",
        &["gb_sm_12_ended_session_responses_do_not_reach_replacement"],
    ),
    (
        "GB-SM-13",
        &["gb_sm_13_saturated_peer_does_not_block_other_peers"],
    ),
    (
        "GB-SM-14",
        &["gb_sm_14_frames_are_attributable_to_live_request_owner"],
    ),
    (
        "GB-SM-15",
        &["gb_sm_15_delayed_older_connect_cannot_replace_newer_session"],
    ),
    (
        "GB-SM-16",
        &["gb_sm_16_peer_frames_wait_for_reactor_admission"],
    ),
    (
        "GB-SM-17",
        &["gb_sm_17_superseded_routine_request_cannot_reach_replacement_session"],
    ),
    (
        "GB-SM-18",
        &["gb_sm_18_live_unavailable_completion_sends_terminal_and_releases_slot"],
    ),
    (
        "GB-SM-19",
        &["gb_sm_19_inbound_sessions_serve_and_use_inbound_cap"],
    ),
];

#[test]
fn gb_sm_contract_manifest_names_every_requirement() {
    const EXPECTED_IDS: &[&str] = &[
        "GB-SM-01", "GB-SM-02", "GB-SM-03", "GB-SM-04", "GB-SM-05", "GB-SM-06", "GB-SM-07",
        "GB-SM-08", "GB-SM-09", "GB-SM-10", "GB-SM-11", "GB-SM-12", "GB-SM-13", "GB-SM-14",
        "GB-SM-15", "GB-SM-16", "GB-SM-17", "GB-SM-18", "GB-SM-19",
    ];
    assert_contract_test_manifest(EXPECTED_IDS, GB_SM_TEST_MANIFEST);
}

const SCENARIO_RECONNECT_AFTER_DISCONNECT: u8 = 0;
const SCENARIO_LEDGER_SATURATION: u8 = 1;
const SCENARIO_ORPHANED_RESPONSE: u8 = 2;
const SCENARIO_RETIRED_COMPLETION: u8 = 3;
const SCENARIO_ABOVE_TIP: u8 = 4;
const SCENARIO_UNKNOWN_COMPLETION: u8 = 5;
const SCENARIO_MISMATCHED_COMPLETION: u8 = 6;
const SCENARIO_CROSS_PEER_PROGRESS: u8 = 7;
const SCENARIO_CANCEL_AND_RECONNECT: u8 = 8;
const SCENARIO_FAST_ADMISSION: u8 = 9;
const SCENARIO_REPLACEMENT_ADMISSION: u8 = 10;
const SCENARIO_MISSING_STATUS: u8 = 11;
const SCENARIO_COUNT_BOUNDS: u8 = 12;
const SCENARIO_RESPONSE_BOUNDARIES: u8 = 13;
const SCENARIO_NON_CONTIGUOUS_RESPONSE: u8 = 14;
const SCENARIO_GENESIS_RESPONSE: u8 = 15;
const SCENARIO_LIVE_UNAVAILABLE: u8 = 16;
const SCENARIO_INBOUND_SERVING: u8 = 17;
const SCENARIO_DISCONNECTED_RESPONSE: u8 = 18;
const LAST_FOCUSED_SCENARIO: u8 = SCENARIO_DISCONNECTED_RESPONSE;

/// Generate total operations with extra weight on requests and completion
/// ownership, plus explicit protocol boundaries for height and count.
fn operation_strategy() -> impl Strategy<Value = ServingOp> {
    let peer = 0u8..super::LOGICAL_PEER_COUNT;
    let start = prop_oneof![
        1 => Just(0),
        6 => 1u32..=40,
        1 => Just(0x7fff_ffff),
    ];
    let count = prop_oneof![
        3 => 1u32..=8,
        1 => Just(1),
        1 => Just(127),
        1 => Just(128),
    ];
    let selector = prop_oneof![
        5 => (0u8..=15).prop_map(QuerySelector::Live),
        2 => (0u8..=15).prop_map(QuerySelector::Retired),
        2 => (0u8..=15).prop_map(QuerySelector::Orphaned),
        1 => (0u8..=15).prop_map(QuerySelector::Unknown),
        1 => (0u8..=15).prop_map(QuerySelector::MismatchedStart),
        1 => (0u8..=15).prop_map(QuerySelector::MismatchedPeer),
    ];
    let completion_kind = prop_oneof![
        3 => Just(CompletionKind::Ready),
        1 => Just(CompletionKind::ReadyOverlong),
        2 => (0u8..=15).prop_map(CompletionKind::ReadyPrefix),
        1 => Just(CompletionKind::ReadyWithGap),
        1 => Just(CompletionKind::FinishedUnavailable),
    ];
    let completion =
        (selector, completion_kind).prop_map(|(query, kind)| ServingOp::Complete { query, kind });

    prop_oneof![
        2 => peer.clone().prop_map(|peer| ServingOp::Connect { peer }),
        2 => (peer.clone(), any::<bool>()).prop_map(|(peer, current)| ServingOp::Disconnect {
            peer,
            which: if current { DisconnectWhich::Current } else { DisconnectWhich::Stale },
        }),
        1 => peer.clone().prop_map(|peer| ServingOp::Cancel { peer }),
        3 => (peer.clone(), any::<bool>()).prop_map(|(peer, valid)| ServingOp::Status {
            peer,
            validity: if valid { StatusValidity::Valid } else { StatusValidity::InvalidRange },
        }),
        8 => (peer, start, count).prop_map(|(peer, start, count)| ServingOp::GetBlocks {
            peer,
            start,
            count,
        }),
        6 => completion,
    ]
}

/// Generate singleton steps and realistic two- or three-operation races that
/// must enter the runtime before it is allowed to settle.
fn step_strategy() -> impl Strategy<Value = ServingStep> {
    let singleton = operation_strategy().prop_map(ServingStep::single);
    let peer = 0u8..super::LOGICAL_PEER_COUNT;
    let start = 1u32..=24;
    let lifecycle_race = peer.clone().prop_map(|peer| {
        ServingStep::unsettled([
            ServingOp::Disconnect {
                peer,
                which: DisconnectWhich::Current,
            },
            ServingOp::Connect { peer },
        ])
    });
    let fast_request = (peer.clone(), start.clone()).prop_map(|(peer, start)| {
        ServingStep::unsettled([
            ServingOp::Status {
                peer,
                validity: StatusValidity::Valid,
            },
            ServingOp::GetBlocks {
                peer,
                start,
                count: 1,
            },
        ])
    });
    let fast_admission = (peer.clone(), start).prop_map(|(peer, start)| {
        ServingStep::unsettled([
            ServingOp::Connect { peer },
            ServingOp::Status {
                peer,
                validity: StatusValidity::Valid,
            },
            ServingOp::GetBlocks {
                peer,
                start,
                count: 1,
            },
        ])
    });
    let replacement_admission = peer.prop_map(|peer| {
        ServingStep::unsettled([ServingOp::Connect { peer }, ServingOp::Connect { peer }])
    });

    prop_oneof![
        12 => singleton,
        2 => lifecycle_race,
        2 => fast_request,
        1 => fast_admission,
        1 => replacement_admission,
    ]
}

/// Generate a complete node configuration, focused prelude, and random history.
fn serving_case_strategy() -> impl Strategy<Value = ServingCase> {
    (
        any::<u64>(),
        4u32..=24,
        1u32..=4,
        1u32..=8,
        1usize..=4,
        any::<bool>(),
        SCENARIO_RECONNECT_AFTER_DISCONNECT..=LAST_FOCUSED_SCENARIO,
        prop_oneof![
            3 => Just(ByteCap::All),
            1 => Just(ByteCap::BeforeFirst),
            1 => Just(ByteCap::ExactlyFirst),
            1 => Just(ByteCap::ExactlyFirstTwo),
        ],
        vec(step_strategy(), 8..=32),
    )
        .prop_map(
            |(
                corpus_seed,
                tip,
                max_inflight,
                max_blocks,
                max_peers,
                inbound,
                scenario,
                byte_cap,
                random_steps,
            )| {
                let max_peers = if matches!(
                    scenario,
                    SCENARIO_CROSS_PEER_PROGRESS
                        | SCENARIO_FAST_ADMISSION
                        | SCENARIO_REPLACEMENT_ADMISSION
                        | SCENARIO_MISSING_STATUS
                ) {
                    max_peers.max(2)
                } else if scenario == SCENARIO_INBOUND_SERVING {
                    1
                } else {
                    max_peers
                };
                let direction = if scenario == SCENARIO_INBOUND_SERVING || inbound {
                    ServicePeerDirection::Inbound
                } else {
                    ServicePeerDirection::Outbound
                };
                let max_inflight = match scenario {
                    SCENARIO_RETIRED_COMPLETION => 2,
                    SCENARIO_LIVE_UNAVAILABLE => 1,
                    _ => max_inflight,
                };
                let max_blocks = if scenario == SCENARIO_COUNT_BOUNDS {
                    4
                } else {
                    max_blocks
                };
                let byte_cap = if scenario == SCENARIO_RESPONSE_BOUNDARIES {
                    ByteCap::ExactlyFirst
                } else {
                    byte_cap
                };
                let mut steps = focused_scenario(scenario, tip, max_inflight);
                steps.extend(random_steps);
                ServingCase {
                    corpus_seed,
                    tip,
                    max_inflight,
                    max_blocks,
                    direction,
                    max_peers,
                    byte_cap,
                    steps,
                }
            },
        )
}

/// Return a named, deterministic history that guarantees one important class
/// is exercised before random operations are appended.
fn focused_scenario(scenario: u8, tip: u32, max_inflight: u32) -> Vec<ServingStep> {
    match scenario {
        SCENARIO_RECONNECT_AFTER_DISCONNECT => vec![
            ServingStep::unsettled([
                ServingOp::Disconnect {
                    peer: 0,
                    which: DisconnectWhich::Current,
                },
                ServingOp::Connect { peer: 0 },
            ]),
            // The replacement must use the retained Status without a refresh.
            ServingStep::single(ServingOp::GetBlocks {
                peer: 0,
                start: 1,
                count: 1,
            }),
            ServingStep::single(ServingOp::Status {
                peer: 0,
                validity: StatusValidity::InvalidRange,
            }),
        ],
        SCENARIO_LEDGER_SATURATION => {
            let mut steps = vec![ServingStep::single(ServingOp::Status {
                peer: 0,
                validity: StatusValidity::Valid,
            })];
            steps.extend((0..=max_inflight).map(|offset| {
                ServingStep::single(ServingOp::GetBlocks {
                    peer: 0,
                    start: 1 + (offset % tip),
                    count: 1,
                })
            }));
            steps
        }
        SCENARIO_ORPHANED_RESPONSE => vec![
            ServingStep::single(ServingOp::GetBlocks {
                peer: 0,
                start: 1,
                count: 2,
            }),
            ServingStep::unsettled([
                ServingOp::Connect { peer: 0 },
                ServingOp::Disconnect {
                    peer: 0,
                    which: DisconnectWhich::Stale,
                },
            ]),
            ServingStep::single(ServingOp::Complete {
                query: QuerySelector::Orphaned(0),
                kind: CompletionKind::Ready,
            }),
        ],
        SCENARIO_DISCONNECTED_RESPONSE => vec![
            ServingStep::single(ServingOp::GetBlocks {
                peer: 0,
                start: 1,
                count: 2,
            }),
            ServingStep::single(ServingOp::Disconnect {
                peer: 0,
                which: DisconnectWhich::Current,
            }),
            ServingStep::single(ServingOp::Complete {
                query: QuerySelector::Orphaned(0),
                kind: CompletionKind::Ready,
            }),
        ],
        SCENARIO_RETIRED_COMPLETION => vec![
            ServingStep::single(ServingOp::GetBlocks {
                peer: 0,
                start: 1,
                count: 1,
            }),
            ServingStep::single(ServingOp::Complete {
                query: QuerySelector::Live(0),
                kind: CompletionKind::Ready,
            }),
            ServingStep::single(ServingOp::GetBlocks {
                peer: 0,
                start: 1,
                count: 1,
            }),
            ServingStep::single(ServingOp::GetBlocks {
                peer: 0,
                start: 2,
                count: 1,
            }),
            ServingStep::single(ServingOp::Complete {
                query: QuerySelector::Retired(u8::MAX),
                kind: CompletionKind::Ready,
            }),
            ServingStep::single(ServingOp::GetBlocks {
                peer: 0,
                start: 3,
                count: 1,
            }),
        ],
        SCENARIO_ABOVE_TIP => vec![ServingStep::single(ServingOp::GetBlocks {
            peer: 0,
            start: tip.saturating_add(1),
            count: 1,
        })],
        SCENARIO_UNKNOWN_COMPLETION => vec![
            ServingStep::single(ServingOp::GetBlocks {
                peer: 0,
                start: 1,
                count: 3,
            }),
            ServingStep::single(ServingOp::Complete {
                query: QuerySelector::Unknown(0),
                kind: CompletionKind::FinishedUnavailable,
            }),
            ServingStep::single(ServingOp::Complete {
                query: QuerySelector::Live(0),
                kind: CompletionKind::ReadyPrefix(1),
            }),
        ],
        SCENARIO_MISMATCHED_COMPLETION => vec![
            ServingStep::single(ServingOp::GetBlocks {
                peer: 0,
                start: 1,
                count: 3,
            }),
            ServingStep::single(ServingOp::Connect { peer: 1 }),
            ServingStep::single(ServingOp::Complete {
                query: QuerySelector::MismatchedStart(0),
                kind: CompletionKind::FinishedUnavailable,
            }),
            ServingStep::single(ServingOp::Complete {
                query: QuerySelector::MismatchedPeer(0),
                kind: CompletionKind::Ready,
            }),
            ServingStep::single(ServingOp::Complete {
                query: QuerySelector::Live(0),
                kind: CompletionKind::ReadyPrefix(1),
            }),
        ],
        SCENARIO_CROSS_PEER_PROGRESS => {
            let mut steps = Vec::new();
            steps.extend((0..max_inflight).map(|offset| {
                ServingStep::single(ServingOp::GetBlocks {
                    peer: 0,
                    start: 1 + (offset % tip),
                    count: 1,
                })
            }));
            steps.extend([
                ServingStep::single(ServingOp::Connect { peer: 1 }),
                ServingStep::single(ServingOp::Status {
                    peer: 1,
                    validity: StatusValidity::Valid,
                }),
                ServingStep::single(ServingOp::GetBlocks {
                    peer: 1,
                    start: 1,
                    count: 1,
                }),
                ServingStep::single(ServingOp::Connect { peer: 2 }),
            ]);
            steps
        }
        SCENARIO_CANCEL_AND_RECONNECT => vec![
            ServingStep::single(ServingOp::Cancel { peer: 0 }),
            ServingStep::single(ServingOp::Connect { peer: 0 }),
            ServingStep::unsettled([
                ServingOp::Status {
                    peer: 0,
                    validity: StatusValidity::Valid,
                },
                ServingOp::GetBlocks {
                    peer: 0,
                    start: 1,
                    count: 1,
                },
            ]),
        ],
        SCENARIO_FAST_ADMISSION => vec![ServingStep::unsettled([
            ServingOp::Connect { peer: 1 },
            ServingOp::Status {
                peer: 1,
                validity: StatusValidity::Valid,
            },
            ServingOp::GetBlocks {
                peer: 1,
                start: 1,
                count: 1,
            },
        ])],
        SCENARIO_REPLACEMENT_ADMISSION => vec![
            ServingStep::unsettled([
                ServingOp::Connect { peer: 1 },
                ServingOp::Connect { peer: 1 },
            ]),
            ServingStep::unsettled([
                ServingOp::Status {
                    peer: 1,
                    validity: StatusValidity::Valid,
                },
                ServingOp::GetBlocks {
                    peer: 1,
                    start: 1,
                    count: 1,
                },
            ]),
        ],
        SCENARIO_MISSING_STATUS => vec![
            ServingStep::single(ServingOp::Connect { peer: 1 }),
            ServingStep::single(ServingOp::GetBlocks {
                peer: 1,
                start: 1,
                count: 1,
            }),
        ],
        SCENARIO_COUNT_BOUNDS => vec![
            ServingStep::single(ServingOp::GetBlocks {
                peer: 0,
                start: 1,
                count: 2,
            }),
            ServingStep::single(ServingOp::Complete {
                query: QuerySelector::Live(0),
                kind: CompletionKind::Ready,
            }),
            ServingStep::single(ServingOp::GetBlocks {
                peer: 0,
                start: 1,
                count: 8,
            }),
            ServingStep::single(ServingOp::Complete {
                query: QuerySelector::Live(0),
                kind: CompletionKind::Ready,
            }),
            ServingStep::single(ServingOp::GetBlocks {
                peer: 0,
                start: tip.saturating_sub(1).max(1),
                count: 8,
            }),
        ],
        SCENARIO_RESPONSE_BOUNDARIES => vec![
            ServingStep::single(ServingOp::GetBlocks {
                peer: 0,
                start: 1,
                count: 3,
            }),
            ServingStep::single(ServingOp::Complete {
                query: QuerySelector::Live(0),
                kind: CompletionKind::Ready,
            }),
            ServingStep::single(ServingOp::GetBlocks {
                peer: 0,
                start: 1,
                count: 3,
            }),
            ServingStep::single(ServingOp::Complete {
                query: QuerySelector::Live(0),
                kind: CompletionKind::ReadyPrefix(0),
            }),
            ServingStep::single(ServingOp::GetBlocks {
                peer: 0,
                start: 1,
                count: 3,
            }),
            ServingStep::single(ServingOp::Complete {
                query: QuerySelector::Live(0),
                kind: CompletionKind::ReadyPrefix(1),
            }),
            ServingStep::single(ServingOp::GetBlocks {
                peer: 0,
                start: 1,
                count: 1,
            }),
            ServingStep::single(ServingOp::Complete {
                query: QuerySelector::Live(0),
                kind: CompletionKind::ReadyOverlong,
            }),
        ],
        SCENARIO_NON_CONTIGUOUS_RESPONSE => vec![
            ServingStep::single(ServingOp::GetBlocks {
                peer: 0,
                start: 1,
                count: 3,
            }),
            ServingStep::single(ServingOp::Complete {
                query: QuerySelector::Live(0),
                kind: CompletionKind::ReadyWithGap,
            }),
        ],
        SCENARIO_GENESIS_RESPONSE => vec![
            ServingStep::single(ServingOp::GetBlocks {
                peer: 0,
                start: 0,
                count: 1,
            }),
            ServingStep::single(ServingOp::Complete {
                query: QuerySelector::Live(0),
                kind: CompletionKind::Ready,
            }),
        ],
        SCENARIO_LIVE_UNAVAILABLE => vec![
            ServingStep::single(ServingOp::GetBlocks {
                peer: 0,
                start: 1,
                count: 1,
            }),
            ServingStep::single(ServingOp::Complete {
                query: QuerySelector::Live(0),
                kind: CompletionKind::FinishedUnavailable,
            }),
            ServingStep::single(ServingOp::GetBlocks {
                peer: 0,
                start: 2,
                count: 1,
            }),
        ],
        SCENARIO_INBOUND_SERVING => vec![ServingStep::single(ServingOp::Connect { peer: 1 })],
        _ => unreachable!("the case strategy generates only named focused scenarios"),
    }
}

/// Build a stable standalone case for one named focused history.
fn focused_case(scenario: u8, byte_cap: ByteCap) -> ServingCase {
    let tip = 12;
    let max_inflight = if scenario == SCENARIO_LIVE_UNAVAILABLE {
        1
    } else {
        2
    };
    ServingCase {
        corpus_seed: 0x5e7e_0000_u64.saturating_add(u64::from(scenario)),
        tip,
        max_inflight,
        max_blocks: 4,
        direction: if scenario == SCENARIO_INBOUND_SERVING {
            ServicePeerDirection::Inbound
        } else {
            ServicePeerDirection::Outbound
        },
        max_peers: if scenario == SCENARIO_INBOUND_SERVING {
            1
        } else {
            2
        },
        byte_cap,
        steps: focused_scenario(scenario, tip, max_inflight),
    }
}

const ALL_FOCUSED_CASES: &[(u8, ByteCap)] = &[
    (SCENARIO_RECONNECT_AFTER_DISCONNECT, ByteCap::All),
    (SCENARIO_LEDGER_SATURATION, ByteCap::All),
    (SCENARIO_ORPHANED_RESPONSE, ByteCap::ExactlyFirstTwo),
    (SCENARIO_RETIRED_COMPLETION, ByteCap::All),
    (SCENARIO_ABOVE_TIP, ByteCap::All),
    (SCENARIO_UNKNOWN_COMPLETION, ByteCap::ExactlyFirst),
    (SCENARIO_MISMATCHED_COMPLETION, ByteCap::ExactlyFirst),
    (SCENARIO_CROSS_PEER_PROGRESS, ByteCap::All),
    (SCENARIO_CANCEL_AND_RECONNECT, ByteCap::All),
    (SCENARIO_FAST_ADMISSION, ByteCap::All),
    (SCENARIO_REPLACEMENT_ADMISSION, ByteCap::All),
    (SCENARIO_MISSING_STATUS, ByteCap::All),
    (SCENARIO_COUNT_BOUNDS, ByteCap::All),
    (SCENARIO_RESPONSE_BOUNDARIES, ByteCap::BeforeFirst),
    (SCENARIO_RESPONSE_BOUNDARIES, ByteCap::ExactlyFirst),
    (SCENARIO_NON_CONTIGUOUS_RESPONSE, ByteCap::All),
    (SCENARIO_GENESIS_RESPONSE, ByteCap::All),
    (SCENARIO_LIVE_UNAVAILABLE, ByteCap::All),
    (SCENARIO_INBOUND_SERVING, ByteCap::All),
    (SCENARIO_DISCONNECTED_RESPONSE, ByteCap::All),
];

/// Replay focused cases and combine their verified requirement evidence.
fn focused_coverage(cases: &[(u8, ByteCap)]) -> ServingCoverage {
    let mut coverage = ServingCoverage::default();
    for (scenario, byte_cap) in cases {
        coverage += replay_serving_case(&focused_case(*scenario, *byte_cap))
            .unwrap_or_else(|error| panic!("focused serving scenario {scenario} failed: {error}"));
    }
    coverage
}

/// Require a named property to be exercised by its deterministic scenarios.
fn assert_requirement_covered(requirement: ServingRequirement, cases: &[(u8, ByteCap)]) {
    let coverage = focused_coverage(cases);
    assert!(
        coverage.requirement_is_covered(requirement),
        "{} was not fully exercised by its focused scenarios\ncoverage: {coverage}",
        requirement.id(),
    );
}

#[test]
fn gb_sm_01_replacement_cancels_previous_session() {
    assert_requirement_covered(
        ServingRequirement::ReplacementCancelsPreviousSession,
        &[(SCENARIO_REPLACEMENT_ADMISSION, ByteCap::All)],
    );
}

#[test]
fn gb_sm_02_stale_disconnect_preserves_current_session() {
    assert_requirement_covered(
        ServingRequirement::StaleDisconnectPreservesCurrentSession,
        &[
            (SCENARIO_RECONNECT_AFTER_DISCONNECT, ByteCap::All),
            (SCENARIO_ORPHANED_RESPONSE, ByteCap::ExactlyFirstTwo),
        ],
    );
}

#[test]
fn gb_sm_03_missing_status_is_rejected_as_spam() {
    assert_requirement_covered(
        ServingRequirement::MissingStatusIsRejectedAsSpam,
        &[(SCENARIO_MISSING_STATUS, ByteCap::All)],
    );
}

#[test]
fn gb_sm_04_peer_ledgers_are_independent_and_bounded() {
    assert_requirement_covered(
        ServingRequirement::PeerLedgersAreIndependentAndBounded,
        &[
            (SCENARIO_LEDGER_SATURATION, ByteCap::All),
            (SCENARIO_CROSS_PEER_PROGRESS, ByteCap::All),
        ],
    );
}

#[test]
fn gb_sm_05_saturated_ledger_rejects_without_state_query() {
    assert_requirement_covered(
        ServingRequirement::SaturatedLedgerRejectsWithoutStateQuery,
        &[(SCENARIO_LEDGER_SATURATION, ByteCap::All)],
    );
}

#[test]
fn gb_sm_06_above_tip_request_is_unavailable_without_state_query() {
    assert_requirement_covered(
        ServingRequirement::AboveTipRequestIsUnavailableWithoutStateQuery,
        &[(SCENARIO_ABOVE_TIP, ByteCap::All)],
    );
}

#[test]
fn gb_sm_07_accepted_query_count_respects_all_bounds() {
    assert_requirement_covered(
        ServingRequirement::AcceptedQueryCountRespectsAllBounds,
        &[(SCENARIO_COUNT_BOUNDS, ByteCap::All)],
    );
}

#[test]
fn gb_sm_08_request_ids_are_nonzero_and_unique() {
    assert_requirement_covered(
        ServingRequirement::RequestIdsAreNonzeroAndUnique,
        &[(SCENARIO_COUNT_BOUNDS, ByteCap::All)],
    );
}

#[test]
fn gb_sm_09_ready_response_sends_largest_valid_prefix_and_one_terminal() {
    assert_requirement_covered(
        ServingRequirement::ReadyResponseSendsLargestValidPrefixAndOneTerminal,
        &[
            (SCENARIO_RESPONSE_BOUNDARIES, ByteCap::BeforeFirst),
            (SCENARIO_RESPONSE_BOUNDARIES, ByteCap::ExactlyFirst),
            (SCENARIO_NON_CONTIGUOUS_RESPONSE, ByteCap::All),
            (SCENARIO_GENESIS_RESPONSE, ByteCap::All),
        ],
    );
}

#[test]
fn gb_sm_10_invalid_completion_has_no_serving_effect() {
    assert_requirement_covered(
        ServingRequirement::InvalidCompletionHasNoServingEffect,
        &[
            (SCENARIO_UNKNOWN_COMPLETION, ByteCap::ExactlyFirst),
            (SCENARIO_MISMATCHED_COMPLETION, ByteCap::ExactlyFirst),
            (SCENARIO_RETIRED_COMPLETION, ByteCap::All),
            (SCENARIO_ORPHANED_RESPONSE, ByteCap::ExactlyFirstTwo),
        ],
    );
}

#[test]
fn gb_sm_11_repeated_completion_does_not_release_live_slot() {
    assert_requirement_covered(
        ServingRequirement::RepeatedCompletionDoesNotReleaseLiveSlot,
        &[(SCENARIO_RETIRED_COMPLETION, ByteCap::All)],
    );
}

#[test]
fn gb_sm_12_ended_session_responses_do_not_reach_replacement() {
    assert_requirement_covered(
        ServingRequirement::EndedSessionResponsesDoNotReachReplacement,
        &[
            (SCENARIO_ORPHANED_RESPONSE, ByteCap::ExactlyFirstTwo),
            (SCENARIO_DISCONNECTED_RESPONSE, ByteCap::All),
        ],
    );
}

#[test]
fn gb_sm_13_saturated_peer_does_not_block_other_peers() {
    assert_requirement_covered(
        ServingRequirement::SaturatedPeerDoesNotBlockOtherPeers,
        &[(SCENARIO_CROSS_PEER_PROGRESS, ByteCap::All)],
    );
}

#[test]
fn gb_sm_14_frames_are_attributable_to_live_request_owner() {
    assert_requirement_covered(
        ServingRequirement::FramesAreAttributableToLiveRequestOwner,
        &[
            (SCENARIO_RESPONSE_BOUNDARIES, ByteCap::ExactlyFirst),
            (SCENARIO_ORPHANED_RESPONSE, ByteCap::ExactlyFirstTwo),
        ],
    );
}

#[test]
fn gb_sm_18_live_unavailable_completion_sends_terminal_and_releases_slot() {
    assert_requirement_covered(
        ServingRequirement::LiveUnavailableCompletionSendsTerminalAndReleasesSlot,
        &[(SCENARIO_LIVE_UNAVAILABLE, ByteCap::All)],
    );
}

#[test]
fn gb_sm_19_inbound_sessions_serve_and_use_inbound_cap() {
    assert_requirement_covered(
        ServingRequirement::InboundSessionsServeAndUseInboundCap,
        &[(SCENARIO_INBOUND_SERVING, ByteCap::All)],
    );
}

/// Prove the deterministic floor covers every model-checkable requirement,
/// then search longer generated histories for unexpected interactions.
#[test]
#[allow(clippy::print_stdout)]
fn gb_rl_13_under_budget_histories_match_pre_regulation_reference_model() {
    let generated = GeneratedTestConfig::from_env(CASES_VARIABLE, SEED_VARIABLE, DEFAULT_CASES)
        .unwrap_or_else(|error| panic!("invalid serving-model configuration: {error}"));
    generated.announce("GetBlocks serving model", CASES_VARIABLE, SEED_VARIABLE);
    let focused_coverage = focused_coverage(ALL_FOCUSED_CASES);
    let missing = focused_coverage.missing_model_requirements();
    assert!(
        missing.is_empty(),
        "focused scenarios did not cover: {}",
        missing
            .iter()
            .map(|requirement| requirement.id())
            .collect::<Vec<_>>()
            .join(", "),
    );

    let coverage = RefCell::new(focused_coverage);
    generated
        .runner(file!())
        .run(&serving_case_strategy(), |case| {
            let case_coverage = replay_serving_case(&case).map_err(TestCaseError::fail)?;
            *coverage.borrow_mut() += case_coverage;
            Ok(())
        })
        .expect("generated GetBlocks serving histories satisfy the reference model");

    let coverage = coverage.into_inner();
    println!("GetBlocks serving-model coverage: {coverage}");
    let (ledger_checks, frame_ownership_checks) = coverage.invariant_check_counts();
    let regression_only = ServingRequirement::REGRESSION_ONLY
        .iter()
        .map(|requirement| requirement.id())
        .collect::<Vec<_>>()
        .join(", ");
    println!(
        "GetBlocks requirement coverage: occurrence {}/{}, missing: none; {} ledger checks; {} frame-ownership checks; {} regression-only",
        ServingRequirement::OCCURRENCE.len(),
        ServingRequirement::OCCURRENCE.len(),
        ledger_checks,
        frame_ownership_checks,
        regression_only,
    );
    assert!(
        coverage.cases
            >= u64::from(generated.cases()).saturating_add(
                u64::try_from(ALL_FOCUSED_CASES.len()).expect("focused case count fits u64")
            )
    );
    assert!(coverage.steps >= coverage.cases);
    assert!(coverage.operations >= coverage.steps);
    assert!(coverage.multi_operation_steps > 0);
    assert!(coverage.accepted_requests >= coverage.cases);
    assert_eq!(coverage.captured_request_ids, coverage.accepted_requests);
    assert!(coverage.live_completions >= coverage.cases);
}
