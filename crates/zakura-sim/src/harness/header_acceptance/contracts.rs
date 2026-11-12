//! The relied-on contracts (Rule 23) and the entries that narrow them (Rule
//! 24).
//!
//! hdr-14 composes evidence from components it doesn't own. Each contract
//! it relies on is a [`Contract`] row with its owner, statement, proving
//! tests and [`ContractState`]. Every counted result depends on every
//! contract in [`REQUIRED_CONTRACTS`], unless a [`NARROWING`] entry whose
//! own test passed releases it.

use super::{sim_test, Approval, Expected, ExpectedResult, Key, ResultKind, TestRef};

/// A contract's ID, as the plan's table numbers them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ContractId {
    /// Run completion, zakura#1840.
    C1,
    /// Header integration with C1.
    C2,
    /// Exact replay and build equivalence.
    C3,
    /// Planted runner verdicts.
    C4,
    /// Build provenance for every execution.
    C5,
    /// The witness judge rejects an unrelated incomplete `NotApplicable` and
    /// judges nested sets.
    C6,
    /// Trigger histories.
    C7,
    /// Report capture.
    C8,
    /// The `Draft` override as run configuration.
    C9,
    /// Checkpoints.
    C10,
    /// A peer-evidence checkpoint.
    C11,
    /// The registry keeps every distinct end finding.
    C12,
    /// The fault store keeps every finding.
    C13,
    /// Harness evidence stores behind checkpoints keep what their producer
    /// tests show.
    C14,
    /// Release-wake fixtures can finish.
    C15,
    /// The registry evaluates every property at a step and keeps every
    /// violation.
    C16,
    /// Stores and the log freeze and compare equal across cleanup.
    C17,
    /// Input binding.
    C18,
    /// An unread final capacity boundary concludes.
    C19,
    /// The `NotApplicable` reason inventory.
    C20,
    /// Every header check keeps every finding at a boundary.
    C21,
    /// Finding attribution.
    C22,
}

impl ContractId {
    /// Its name, such as `C3`.
    pub fn name(self) -> String {
        format!("{self:?}")
    }
}

/// Every contract that must have exactly one row in the table, kept apart
/// from the table itself, so deleting a row doesn't delete its requirement.
pub const REQUIRED_CONTRACTS: &[ContractId] = &[
    ContractId::C1,
    ContractId::C2,
    ContractId::C3,
    ContractId::C4,
    ContractId::C5,
    ContractId::C6,
    ContractId::C7,
    ContractId::C8,
    ContractId::C9,
    ContractId::C10,
    ContractId::C11,
    ContractId::C12,
    ContractId::C13,
    ContractId::C14,
    ContractId::C15,
    ContractId::C16,
    ContractId::C17,
    ContractId::C18,
    ContractId::C19,
    ContractId::C20,
    ContractId::C21,
    ContractId::C22,
];

/// A contract's state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContractState {
    /// Delivered. It counts only once every proving test passed at the
    /// subject commit.
    Satisfied,
    /// Not delivered, or its proving test can't pass yet. Every result it
    /// applies to is blocked.
    Blocked {
        /// The delivery it waits for.
        waiting_for: &'static str,
    },
    /// Adam approved leaving the named coverage out. Every result it
    /// applies to is excluded, never verified. It needs no passing test.
    Excluded {
        /// What's left out and why.
        reason: &'static str,
        /// The approval.
        approval: Approval,
    },
}

/// One relied-on contract.
#[derive(Clone, Copy, Debug)]
pub struct Contract {
    /// Its ID.
    pub id: ContractId,
    /// Its owner.
    pub owner: &'static str,
    /// The contract, in one sentence.
    pub statement: &'static str,
    /// The tests that prove it. A `Satisfied` contract needs at least one,
    /// each passing at the subject commit.
    pub proving: &'static [TestRef],
    /// Its state.
    pub state: ContractState,
    /// The PR or ask that delivers it.
    pub delivery: &'static str,
}

/// C3's proving tests: every runnable test of its suite,
/// `tests/replay_exact.rs` (the acceptance tests check it's the whole
/// suite).
const C3_PROVING: &[TestRef] = &[
    sim_test(REPLAY_EXACT, "recorded_traces_replay_exact"),
    sim_test(REPLAY_EXACT, "non_equivalent_build_refused"),
    sim_test(REPLAY_EXACT, "a_generator_bump_does_not_block_replay"),
    sim_test(REPLAY_EXACT, "allow_build_mismatch_never_succeeds"),
    sim_test(REPLAY_EXACT, "allow_build_mismatch_counts_what_differs"),
    sim_test(REPLAY_EXACT, "diagnostic_flags_never_verify"),
    sim_test(REPLAY_EXACT, "changed_pick_diverges"),
    sim_test(REPLAY_EXACT, "option_count_mismatch_diverges"),
    sim_test(REPLAY_EXACT, "changed_bound_diverges"),
    sim_test(REPLAY_EXACT, "renamed_draw_diverges"),
    sim_test(REPLAY_EXACT, "missing_init_draw_diverges"),
    sim_test(REPLAY_EXACT, "out_of_range_draw_is_invalid"),
    sim_test(REPLAY_EXACT, "surplus_records_diverge"),
    sim_test(REPLAY_EXACT, "matching_violation_is_a_success"),
    sim_test(REPLAY_EXACT, "equivalent_build_rejects_changed_results"),
    sim_test(REPLAY_EXACT, "prefix_replays_and_is_never_complete"),
    sim_test(REPLAY_EXACT, "prefix_option_keeps_a_complete_trace_whole"),
    sim_test(REPLAY_EXACT, "observe_writes_the_full_log"),
    sim_test(REPLAY_EXACT, "drain_picks_replay"),
    sim_test(REPLAY_EXACT, "harness_failure_before_a_prefix_ends_fails"),
    sim_test(REPLAY_EXACT, "planted_host_clock_is_nondeterministic"),
    sim_test(REPLAY_EXACT, "effects_replay_child"),
    sim_test(REPLAY_EXACT, "effects_replay_in_a_fresh_process"),
    sim_test(REPLAY_EXACT, "replay_returns_the_violations_end_leaves_out"),
    sim_test(REPLAY_EXACT, "changed_effects_are_nondeterministic"),
    sim_test(
        REPLAY_EXACT,
        "observe_refuses_an_existing_log_before_running",
    ),
    sim_test(REPLAY_EXACT, "a_foreign_storage_failure_replays_exactly"),
    sim_test(REPLAY_EXACT, "work_machine_replay_child"),
    sim_test(
        REPLAY_EXACT,
        "work_machine_records_and_replays_without_transport",
    ),
    sim_test(
        REPLAY_EXACT,
        "work_machine_preserves_empty_and_short_inputs",
    ),
    sim_test(
        REPLAY_EXACT,
        "work_machine_prefix_and_missing_evidence_never_verify",
    ),
    sim_test(REPLAY_EXACT, "decoded_retention_replay_child"),
    sim_test(
        REPLAY_EXACT,
        "decoded_retention_replays_final_holders_in_a_fresh_process",
    ),
    sim_test(
        REPLAY_EXACT,
        "decoded_retention_preserves_short_inputs_and_requires_a_live_witness",
    ),
    sim_test(REPLAY_EXACT, "work_funding_replay_child"),
    sim_test(
        REPLAY_EXACT,
        "work_funding_records_and_replays_retained_runtime_owners",
    ),
    sim_test(
        REPLAY_EXACT,
        "work_funding_preserves_short_inputs_and_observes_drain",
    ),
    sim_test(
        REPLAY_EXACT,
        "work_funding_missing_evidence_and_drain_cannot_verify",
    ),
    sim_test(REPLAY_EXACT, "reactor_machine_replay_child"),
    sim_test(
        REPLAY_EXACT,
        "reactor_machine_records_and_replays_without_transport",
    ),
    sim_test(
        REPLAY_EXACT,
        "publisher_machine_records_and_replays_without_transport",
    ),
    sim_test(
        REPLAY_EXACT,
        "reactor_machine_preserves_empty_and_short_inputs",
    ),
    sim_test(REPLAY_EXACT, "block_reactor_machine_replay_child"),
    sim_test(
        REPLAY_EXACT,
        "reactor_machine_records_and_replays_query_identities",
    ),
    sim_test(REPLAY_EXACT, "discovery_book_input_is_the_encoded_witness"),
    sim_test(REPLAY_EXACT, "discovery_machine_replay_child"),
    sim_test(
        REPLAY_EXACT,
        "discovery_book_records_and_replays_signed_samples",
    ),
    sim_test(
        REPLAY_EXACT,
        "discovery_state_records_and_replays_installations",
    ),
    sim_test(
        REPLAY_EXACT,
        "discovery_machines_preserve_empty_and_short_inputs",
    ),
    sim_test(
        REPLAY_EXACT,
        "verification_capacity_replays_in_a_fresh_process",
    ),
];

/// C3's suite.
pub const REPLAY_EXACT: &str = "tests/replay_exact.rs";

/// C10's proving tests: the reactor tap's, the storage checkpoint's and the
/// transport checkpoint's controls, one per registered evidence source a
/// header harness holds.
const C10_PROVING: &[TestRef] = &[
    sim_test(
        "tests/header_controlled.rs",
        "storage_delivered_anywhere_but_the_run_log_ends_the_run",
    ),
    sim_test(
        "tests/header_controlled.rs",
        "a_skipped_observation_ends_the_run_there",
    ),
    sim_test(
        "tests/header_controlled.rs",
        "storage_the_run_never_observes_fails_at_setup",
    ),
    sim_test(
        "tests/header_controlled.rs",
        "storage_entries_must_reach_the_log_unchanged",
    ),
    sim_test(
        "tests/header_controlled.rs",
        "storage_dropped_during_the_run_fails_it",
    ),
    sim_test(
        "tests/header_controlled.rs",
        "storage_dropped_after_the_last_step_fails_the_run",
    ),
    sim_test(
        "tests/header_wire.rs",
        "a_lost_or_altered_session_row_fails_the_run",
    ),
    sim_test("tests/header_tap.rs", "each_tap_is_checked_on_its_own"),
    sim_test(
        "tests/header_tap.rs",
        "the_tap_is_checked_live_after_the_last_observation",
    ),
    sim_test(
        "tests/header_tap.rs",
        "a_tap_the_run_never_observes_fails_at_setup",
    ),
    sim_test(
        "tests/header_tap.rs",
        "a_skipped_tap_observation_ends_the_run_there",
    ),
    sim_test(
        "tests/header_tap.rs",
        "tap_events_delivered_anywhere_but_the_run_log_end_the_run",
    ),
    sim_test(
        "tests/header_tap.rs",
        "tap_events_must_reach_the_log_unchanged",
    ),
    sim_test(
        "tests/header_tap.rs",
        "a_tap_from_a_dropped_run_fails_a_later_one",
    ),
    sim_test(
        "tests/header_tap.rs",
        "a_tap_dropped_during_the_run_fails_it",
    ),
    sim_test(
        "tests/header_tap.rs",
        "an_emission_outside_the_run_fails_it",
    ),
];

/// C16's proving tests.
const C16_PROVING: &[TestRef] = &[
    sim_test(
        "tests/registry.rs",
        "known_and_active_failures_at_one_boundary",
    ),
    sim_test("tests/registry.rs", "every_violation_retained"),
];

/// C20's proving tests.
const C20_PROVING: &[TestRef] = &[sim_test(
    "tests/header_acceptance.rs",
    "the_not_applicable_inventory_lists_every_reason",
)];

/// C1's proving tests: every test of #1840's suites, `tests/drain.rs` and
/// `tests/drains_dry.rs`, and the header histories the plan names: a stop
/// picked at the cut settling in the drain, its drain-bound control, and
/// a budget cut between a release's wake and its cached-pass refill.
const C1_PROVING: &[TestRef] = &[
    sim_test(
        "tests/drain.rs",
        "rule_1_the_drain_offers_recovery_actions_and_no_adversary_act",
    ),
    sim_test(
        "tests/drain.rs",
        "the_drain_completes_an_operation_a_hold_kept_from_live",
    ),
    sim_test("tests/drain.rs", "a_live_idle_end_enters_the_drain"),
    sim_test(
        "tests/drain.rs",
        "a_held_ready_task_runs_before_its_obligation_expires",
    ),
    sim_test(
        "tests/drain.rs",
        "a_quiet_recovery_action_waits_for_no_task_ready",
    ),
    sim_test(
        "tests/drain.rs",
        "rule_2_an_obligation_opened_in_the_drain_expires_at_its_deadline",
    ),
    sim_test(
        "tests/drain.rs",
        "rule_2_a_dry_end_concludes_an_open_obligation",
    ),
    sim_test(
        "tests/drain.rs",
        "rule_2_a_drain_cut_off_with_an_obligation_open_names_it",
    ),
    sim_test(
        "tests/drain.rs",
        "rule_4_a_lost_assumption_withdraws_what_the_dry_end_would_conclude",
    ),
    sim_test(
        "tests/drain.rs",
        "rule_5_every_not_applicable_carries_its_reason",
    ),
    sim_test(
        "tests/drain.rs",
        "rule_6_a_failure_found_only_at_the_end_fails_the_run",
    ),
    sim_test(
        "tests/drain.rs",
        "a_late_datagram_opens_the_first_obligation_in_the_drain",
    ),
    sim_test(
        "tests/drain.rs",
        "an_ops_obligation_whose_task_sleeps_is_reached",
    ),
    sim_test(
        "tests/drain.rs",
        "an_earlier_timer_cancels_a_later_one_before_a_boundary",
    ),
    sim_test(
        "tests/drain.rs",
        "a_periodic_timer_past_the_cap_does_not_move_the_drain",
    ),
    sim_test("tests/drain.rs", "a_sleep_within_the_cap_is_reached"),
    sim_test(
        "tests/drain.rs",
        "a_pending_recovery_completion_discharges_before_quiescence",
    ),
    sim_test("tests/drain.rs", "a_live_hold_does_not_postpone_quiescence"),
    sim_test(
        "tests/drain.rs",
        "stranded_work_with_a_timed_obligation_open_fails_the_run",
    ),
    sim_test(
        "tests/drain.rs",
        "stranded_work_fails_the_run_before_a_quiescent_expiry",
    ),
    sim_test(
        "tests/drain.rs",
        "a_source_holding_work_at_a_dry_end_fails_the_run",
    ),
    sim_test("tests/drain.rs", "a_dry_end_leaves_a_fault_mark_unsettled"),
    sim_test(
        "tests/drain.rs",
        "a_dry_end_fails_an_unread_boundary_as_missing_evidence",
    ),
    sim_test(
        "tests/drain.rs",
        "a_dry_end_leaves_a_header_teardown_mark_unsettled",
    ),
    sim_test(
        "tests/drain.rs",
        "a_dry_end_reads_a_watch_mark_as_node_stopped",
    ),
    sim_test(
        "tests/drain.rs",
        "a_dry_end_leaves_a_block_teardown_mark_unsettled",
    ),
    sim_test(
        "tests/drain.rs",
        "a_dry_end_leaves_an_unjudged_departure_unsettled",
    ),
    sim_test(
        "tests/drain.rs",
        "a_dry_end_leaves_an_unread_trigger_unsettled",
    ),
    sim_test("tests/drain.rs", "the_drain_retires_every_generator"),
    sim_test(
        "tests/drain.rs",
        "a_worker_starved_drain_keeps_its_status_and_does_not_settle",
    ),
    sim_test(
        "tests/drain.rs",
        "a_duplicate_recovery_identity_at_a_live_idle_end_fails_the_run",
    ),
    sim_test(
        "tests/drain.rs",
        "the_drain_delivers_a_fake_datagram_in_flight",
    ),
    sim_test(
        "tests/drain.rs",
        "a_recovery_boundary_ahead_keeps_the_drain_from_quiescence",
    ),
    sim_test(
        "tests/drain.rs",
        "a_worker_starved_end_fails_rather_than_conclude",
    ),
    sim_test(
        "tests/drain.rs",
        "a_live_idle_end_enters_the_drain_before_judging",
    ),
    sim_test("tests/drains_dry.rs", "every_saved_plan_drains_dry"),
    sim_test("tests/drains_dry.rs", "misend_rejects_every_unexpected_end"),
    sim_test(
        "tests/header_node_prereqs.rs",
        "a_stop_picked_at_the_cut_settles_in_the_drain",
    ),
    sim_test(
        "tests/header_node_prereqs.rs",
        "a_run_cut_off_at_the_shutdown_pick_is_unsettled",
    ),
    sim_test(
        "tests/header_release_wake.rs",
        "a_budget_cut_between_a_wake_and_its_refill_holds_the_wake",
    ),
];

/// C7's proving tests: each template's trigger, its sibling-row controls,
/// and `sender-within-credit` refusing an applied `Open` or `Grant` alone.
const C7_PROVING: &[TestRef] = &[
    sim_test(
        "tests/generated_triggers.rs",
        "a_closes_trigger_needs_its_label_on_its_row",
    ),
    sim_test(
        "tests/generated_triggers.rs",
        "receiver_triggers_need_a_judged_frame",
    ),
    sim_test(
        "tests/generated_triggers.rs",
        "sender_response_triggers_need_the_exchange_first",
    ),
    sim_test(
        "tests/generated_triggers.rs",
        "sender_within_credit_needs_a_page_after_an_applied_grant",
    ),
    sim_test(
        "tests/generated_triggers.rs",
        "sender_within_live_needs_an_update",
    ),
    sim_test(
        "tests/generated_triggers.rs",
        "an_unknown_id_reaches_nothing",
    ),
    sim_test("tests/witness.rs", "a_trigger_sees_its_property_id"),
];

/// C8's proving tests: a sidecar refused when it differs only in a second
/// violation or one stored fact, and a planted and a machine witness
/// recording each keeping the report its judge read.
const C8_PROVING: &[TestRef] = &[
    sim_test(
        "tests/full_report.rs",
        "record_keeps_the_full_report_and_replay_matches_it",
    ),
    sim_test(
        "tests/full_report.rs",
        "a_planted_witness_recording_keeps_its_full_report",
    ),
    sim_test(
        "tests/full_report.rs",
        "a_machine_witness_recording_keeps_its_full_report",
    ),
    sim_test(
        "tests/full_report.rs",
        "a_machine_trace_keeps_its_full_report",
    ),
    sim_test(
        "tests/full_report.rs",
        "record_refuses_an_existing_full_report",
    ),
    sim_test(
        "tests/full_report.rs",
        "replay_refuses_a_report_that_isnt_a_bounded_regular_file",
    ),
    sim_test("tests/full_report.rs", "the_full_report_keeps_end_found"),
    sim_test(
        "tests/header_authority.rs",
        "a_recorded_header_run_keeps_its_stores_in_the_full_report",
    ),
    sim_test(
        "src/harness/record.rs",
        "harness::record::tests::a_finished_run_without_its_report_fails_the_recording",
    ),
    sim_test(
        "src/harness/record.rs",
        "harness::record::tests::a_report_over_the_read_limit_is_refused",
    ),
    sim_test("tests/cli.rs", "replay_report_never_overwrites"),
];

/// C9's proving tests: a `Draft` trace past its first violation replaying
/// Exact in a fresh process, and the override surviving shrinking and the
/// step loop's livelock check.
const C9_PROVING: &[TestRef] = &[
    sim_test(
        "tests/draft_override.rs",
        "a_draft_override_records_and_replays",
    ),
    sim_test(
        "tests/draft_override.rs",
        "shrinking_a_draft_trace_keeps_the_override",
    ),
    sim_test("tests/harness.rs", "a_draft_livelock_records_and_runs_on"),
];

/// C12's proving tests: a step and an end finding of one ID, two end
/// findings of one ID, and an `Active` and a `Known` ID's end findings at
/// one boundary.
const C12_PROVING: &[TestRef] = &[
    sim_test("tests/registry.rs", "every_end_finding_is_kept"),
    sim_test(
        "tests/registry.rs",
        "an_end_violation_counts_beside_earlier_violations",
    ),
    sim_test(
        "src/harness/header_sync.rs",
        "harness::header_sync::tests::the_fault_check_reports_every_break_at_a_step",
    ),
];

/// C14's proving tests: each registered store's producer test, as
/// `tests/evidence_inventory.rs`'s `REGISTERED` names it, and that
/// inventory's own checks, one of which pins this list to it.
const C14_PROVING: &[TestRef] = &[
    sim_test(
        "tests/header_despite_close.rs",
        "the_staging_companion_needs_the_rejected_page_staged",
    ),
    sim_test(
        "tests/header_node_ends.rs",
        "close_and_relink_steps_record_the_connections_they_end",
    ),
    sim_test(
        "tests/header_node_adoption.rs",
        "new_work_carries_the_adopted_generation",
    ),
    sim_test(
        "tests/header_node_sessions.rs",
        "a_close_then_a_relink_ends_or_replaces_the_first_session",
    ),
    sim_test(
        "tests/header_wire_cooldown.rs",
        "a_late_fault_cools_down_the_peer_and_refuses_its_redial",
    ),
    sim_test(
        "tests/header_publication.rs",
        "each_served_witness_serves_what_it_names",
    ),
    sim_test("tests/header_teardown.rs", "teardown_waits_for_held_lease"),
    sim_test(
        "tests/header_budget_claims.rs",
        "an_over_budget_reservation_violates_chunk_budget_bound",
    ),
    sim_test(
        "tests/header_node_capacity.rs",
        "the_probe_matches_the_accounting_through_close_relink_and_shutdown",
    ),
    sim_test(
        "tests/header_node_reservations.rs",
        "a_round_retires_with_headers_still_reserved",
    ),
    sim_test(
        "tests/header_stale.rs",
        "the_stale_prepare_input_holds_stale_completion_inert",
    ),
    sim_test(
        "tests/header_reset_invalidates.rs",
        "every_witness_holds_and_applies",
    ),
    sim_test(
        "tests/header_peer_retirement.rs",
        "every_departure_fixture_holds",
    ),
    sim_test(
        "tests/header_outstanding.rs",
        "the_limit_fixtures_reach_and_hold_their_bounds",
    ),
    sim_test(
        "tests/header_hold_release.rs",
        "every_fixture_holds_and_takes_its_path",
    ),
    sim_test(
        "tests/header_chain_selected.rs",
        "every_witness_holds_and_applies",
    ),
    sim_test(
        "tests/header_stale_chain.rs",
        "every_witness_holds_and_applies",
    ),
    sim_test(
        "tests/header_authority.rs",
        "every_witness_holds_and_applies",
    ),
    sim_test(
        "tests/header_release_wake.rs",
        "the_fixtures_settle_after_their_measured_boundary_and_not_before",
    ),
    sim_test(
        "tests/header_hostile_prefix.rs",
        "every_prefix_class_recovers",
    ),
    sim_test(
        "tests/header_recovery_prefix.rs",
        "a_typed_prefix_stops_at_the_bound",
    ),
    sim_test(
        "tests/header_fault_attribution.rs",
        "an_attributed_prepare_failure_owes_its_source_a_penalty",
    ),
    sim_test(
        "tests/header_node_session_ends.rs",
        "each_recorded_end_is_its_links_cancellation",
    ),
    sim_test(
        "tests/evidence_inventory.rs",
        "every_shared_field_is_a_registered_store_or_allowed",
    ),
    sim_test(
        "tests/evidence_inventory.rs",
        "every_registered_store_names_its_checkpoint_and_producer",
    ),
    sim_test("tests/evidence_inventory.rs", "owed_to_hf7"),
    sim_test("tests/evidence_inventory.rs", "the_scan_follows_aliases"),
    sim_test("tests/evidence_inventory.rs", "the_scan_finds_atomics"),
    sim_test(
        "tests/evidence_inventory.rs",
        "the_scan_reads_enum_variant_fields",
    ),
    sim_test(
        "tests/evidence_inventory.rs",
        "c14_is_proved_by_every_registered_store_s_producer",
    ),
];

/// C18's proving tests: a case trace refused against another fixture's
/// input, a typed trace refused against another valid scenario or other
/// Plan bytes, a scripted Plan recorded, replayed and refused against
/// another script, and a steered witness recorded and replayed through the
/// executable.
const C18_PROVING: &[TestRef] = &[
    sim_test(
        "src/harness/header_acceptance/tool.rs",
        "harness::header_acceptance::tool::tests::a_case_trace_ran_its_committed_input_and_witness",
    ),
    sim_test(
        "src/harness/header_acceptance/tool.rs",
        "harness::header_acceptance::tool::tests::a_typed_trace_binds_to_its_scenario_and_plan",
    ),
    sim_test("tests/record.rs", "recording_a_script_names_it"),
    sim_test("tests/record.rs", "recording_plan_bytes_writes_no_input"),
    sim_test("tests/cli.rs", "record_a_script_against_a_scenario"),
    sim_test(
        "tests/header_wire_prepare_fault.rs",
        "the_late_fault_fixture_records_and_replays_in_a_fresh_process",
    ),
];

/// A unit test of the planted runner, `xtask`'s `src/planted.rs`, by name.
const fn planted_test(test: &'static str) -> TestRef {
    TestRef {
        package: "xtask",
        file: "src/planted.rs",
        test,
    }
}

/// C4's proving tests: the planted runner's classification of baseline and
/// patched runs, its fresh replay of every catch, typed and fuzz-input, a
/// deadline killing a hung baseline, patched or campaign run, and its
/// set-aware judgment of a nested case. The fresh-replay and hang tests are
/// `cfg(unix)`, which the acceptance host, Linux, builds.
const C4_PROVING: &[TestRef] = &[
    planted_test("planted::tests::planted_passes_only_on_its_own_id"),
    planted_test("planted::tests::witness_flags_carry_the_co_violations"),
    planted_test("planted::tests::listed_co_violations_are_expected"),
    planted_test("planted::tests::planted_requires_the_trigger_history"),
    planted_test("planted::tests::planted_requires_passing_baseline"),
    planted_test("planted::tests::planted_patch_must_apply"),
    planted_test("planted::tests::a_patch_may_not_change_the_witness"),
    planted_test("planted::tests::campaign_failures_name_their_input"),
    planted_test("planted::tests::every_catch_needs_its_fresh_replay"),
    planted_test("planted::tests::the_runner_replays_every_caught_case"),
    planted_test("planted::tests::a_replay_miss_reads_the_exit_status"),
    planted_test("planted::tests::a_hung_patched_run_is_killed_and_never_caught"),
    planted_test("planted::tests::a_hung_baseline_or_campaign_run_is_killed_and_never_passes"),
    planted_test("planted::tests::a_nested_case_needs_exactly_its_set"),
    planted_test("planted::tests::a_typed_case_watches_and_runs_its_files"),
];

/// C17's proving tests: the freeze comparison's controls, a store whose
/// freeze is skipped and a cleanup-time append to the log each failing it,
/// the header witnesses keeping their stores and log through cleanup,
/// each registered store's post-freeze pair, the store inventory, and the
/// pairer's and the ledger's tests that each takes a fact once, the
/// ledger's fed through the tap so the simulator build runs them.
const C17_PROVING: &[TestRef] = &[
    sim_test("src/stores.rs", "stores::tests::a_registered_store_keeps_its_freeze_through_cleanup"),
    sim_test(
        "src/stores.rs",
        "stores::tests::a_store_whose_freeze_is_skipped_fails_the_comparison",
    ),
    sim_test(
        "src/stores.rs",
        "stores::tests::an_append_to_the_log_during_cleanup_fails_the_comparison",
    ),
    sim_test("src/stores.rs", "stores::tests::a_store_registered_after_the_freeze_starts_frozen"),
    sim_test("tests/header_cleanup.rs", "storage_witnesses_keep_their_stores_through_cleanup"),
    sim_test("tests/header_cleanup.rs", "the_wire_witness_keeps_its_stores_through_cleanup"),
    sim_test(
        "tests/header_cleanup.rs",
        "the_release_wake_witnesses_keep_their_stores_through_cleanup",
    ),
    sim_test("tests/header_cleanup.rs", "a_completed_storage_run_releases_its_stores"),
    sim_test(
        "src/peer/evidence.rs",
        "peer::evidence::tests::a_frozen_sent_page_store_drops_a_page",
    ),
    sim_test(
        "src/storage/header.rs",
        "storage::header::tests::a_frozen_attribution_store_drops_a_blame",
    ),
    sim_test(
        "src/harness/header_node.rs",
        "harness::header_node::tests::a_frozen_session_ends_store_drops_an_end",
    ),
    sim_test(
        "src/harness/header_node/ends.rs",
        "harness::header_node::ends::tests::a_frozen_end_store_drops_a_fact",
    ),
    sim_test(
        "src/harness/header_node/adoption.rs",
        "harness::header_node::adoption::tests::a_frozen_adoption_store_drops_a_fact",
    ),
    sim_test(
        "src/harness/header_node/session/tests.rs",
        "harness::header_node::session::tests::a_frozen_session_view_drops_an_event",
    ),
    sim_test(
        "src/harness/header_node/fault.rs",
        "harness::header_node::fault::tests::a_frozen_cooldown_store_drops_a_fact",
    ),
    sim_test(
        "src/harness/header_node/publication/tests.rs",
        "harness::header_node::publication::tests::a_frozen_publication_store_drops_a_fact",
    ),
    sim_test(
        "src/harness/header_node/claim.rs",
        "harness::header_node::claim::tests::a_frozen_settlement_store_drops_a_row",
    ),
    sim_test(
        "src/harness/header_node/claim.rs",
        "harness::header_node::claim::tests::a_frozen_claim_store_drops_a_break",
    ),
    sim_test(
        "src/harness/header_node/capacity.rs",
        "harness::header_node::capacity::tests::a_frozen_capacity_store_drops_a_boundary",
    ),
    sim_test(
        "src/harness/header_node/reservation.rs",
        "harness::header_node::reservation::tests::a_frozen_reservation_store_drops_a_fact",
    ),
    sim_test(
        "src/harness/header_node/stale/tests.rs",
        "harness::header_node::stale::tests::a_frozen_stale_store_drops_a_window",
    ),
    sim_test(
        "src/harness/header_node/round_life/tests.rs",
        "harness::header_node::round_life::tests::a_frozen_round_life_store_drops_its_applicability",
    ),
    sim_test(
        "src/harness/header_node/retirement/tests.rs",
        "harness::header_node::retirement::tests::a_frozen_retirement_store_drops_a_fact",
    ),
    sim_test(
        "src/harness/header_node/outstanding/tests.rs",
        "harness::header_node::outstanding::tests::a_frozen_outstanding_store_drops_a_peak",
    ),
    sim_test(
        "src/harness/header_node/hold_wake/tests.rs",
        "harness::header_node::hold_wake::tests::a_frozen_hold_wake_store_drops_a_fact",
    ),
    sim_test(
        "src/harness/header_node/fork_choice/tests.rs",
        "harness::header_node::fork_choice::tests::a_frozen_fork_choice_store_drops_a_settled_point",
    ),
    sim_test(
        "src/harness/header_node/chain_effect/tests.rs",
        "harness::header_node::chain_effect::tests::a_frozen_chain_effect_store_drops_its_applicability",
    ),
    sim_test(
        "src/harness/header_node/authority/tests.rs",
        "harness::header_node::authority::tests::a_frozen_authority_store_drops_its_applicability",
    ),
    sim_test(
        "src/harness/header_node/wake.rs",
        "harness::header_node::wake::tests::a_frozen_wake_store_drops_a_fact",
    ),
    sim_test(
        "src/harness/header_node/recovery.rs",
        "harness::header_node::recovery::tests::frozen_recovery_stores_drop_a_start_and_a_spend",
    ),
    sim_test("tests/evidence_inventory.rs", "every_shared_field_is_a_registered_store_or_allowed"),
    sim_test(
        "tests/evidence_inventory.rs",
        "every_registered_store_names_its_checkpoint_and_producer",
    ),
    sim_test("tests/evidence_inventory.rs", "the_scan_follows_aliases"),
    sim_test("tests/evidence_inventory.rs", "the_scan_finds_atomics"),
    sim_test("tests/evidence_inventory.rs", "the_scan_reads_enum_variant_fields"),
    sim_test(
        "src/harness/header_node/pair.rs",
        "harness::header_node::pair::tests::an_operation_or_a_call_pairs_once",
    ),
    sim_test("tests/header_pairing.rs", "a_call_dispatched_twice_is_refused"),
    sim_test("tests/header_pairing.rs", "a_call_ended_twice_is_refused"),
    sim_test("tests/header_pairing.rs", "headers_held_twice_are_refused"),
    sim_test("tests/header_pairing.rs", "held_headers_ended_twice_are_refused"),
];

/// A contract that waits for `delivery`, with no proving test yet.
const fn blocked(
    id: ContractId,
    owner: &'static str,
    statement: &'static str,
    delivery: &'static str,
) -> Contract {
    Contract {
        id,
        owner,
        statement,
        proving: &[],
        state: ContractState::Blocked {
            waiting_for: delivery,
        },
        delivery,
    }
}

/// A contract whose delivery landed but whose proving tests don't yet cover
/// it, waiting for `missing`.
const fn waiting(
    id: ContractId,
    owner: &'static str,
    statement: &'static str,
    delivery: &'static str,
    missing: &'static str,
) -> Contract {
    Contract {
        id,
        owner,
        statement,
        proving: &[],
        state: ContractState::Blocked {
            waiting_for: missing,
        },
        delivery,
    }
}

/// A delivered contract, proved by `proving`.
const fn satisfied(
    id: ContractId,
    owner: &'static str,
    statement: &'static str,
    proving: &'static [TestRef],
    delivery: &'static str,
) -> Contract {
    Contract {
        id,
        owner,
        statement,
        proving,
        state: ContractState::Satisfied,
        delivery,
    }
}

/// The contracts table, one row per contract. C3, C10 and C16 are
/// delivered on `main`, C20 by hdr-14.3, C1, C4, C7, C8, C9 and C12 by E1
/// to E10, C18 by E10 with hdr-14.7b's binding, and C14 and C17 by hf-6's
/// stores. Every other row's deliveries landed, and its state names the
/// proving test still missing.
pub fn contract_table() -> Vec<Contract> {
    use ContractId::*;
    vec![
        satisfied(
            C1,
            "Evan",
            "Run completion: the drain offers only recovery actions and advances time through \
             timer boundaries and obligation deadlines, every open obligation concludes, a \
             drain that hits its bound is a harness failure, and NotApplicable carries its \
             reason (zakura#1840).",
            C1_PROVING,
            "E1",
        ),
        waiting(
            C2,
            "Adam's harness",
            "header-fault-penalized's open penalty and teardown's unsettled rounds and stop \
             conclude under C1's drain.",
            "hf-4, after E1",
            "hf-4's owed-penalty and node-abandonment cases at the drain's end, which it left \
             to planted cases, and a teardown cut on the wire and release-wake harnesses",
        ),
        Contract {
            id: C3,
            owner: "Evan (trace-3)",
            statement: "Replay refuses a non-equivalent build, then compares the \
                        scenario-relevant meta fields, every step and the end record.",
            proving: C3_PROVING,
            state: ContractState::Satisfied,
            delivery: "on main",
        },
        satisfied(
            C4,
            "Evan (xt/planted.rs)",
            "The planted runner classifies baseline and patched runs, replays every catch \
             fresh, puts a deadline on every run and judges nested cases by their set.",
            C4_PROVING,
            "E6",
        ),
        waiting(
            C5,
            "Evan",
            "Every executed binary's source digest is computed and equals the checkout it \
             should come from, and unknown never matches.",
            "E7",
            "a stale-binary control for a standalone witness history the tool records, beside \
             the catch and contract-test controls",
        ),
        waiting(
            C6,
            "Evan",
            "The witness judge rejects any unrelated NotApplicable whose reason isn't benign, \
             except StoppedByViolation under the expected stop, or a missing report, and \
             judges a nested case's exact set in both runs, each nested finding from the \
             case's own break: the same peer and the same punished event or frame.",
            "E2",
            "a per-subject join, since E2 joins a nested finding to the case's break by step \
             only, not by peer and punished event or frame, and a nested set judged on a \
             Draft run's report",
        ),
        satisfied(
            C7,
            "Evan",
            "A trigger is handed its PropertyId, and each generated template has a receiver \
             trigger and a sender trigger that requires the guarded exchange.",
            C7_PROVING,
            "E3",
        ),
        satisfied(
            C8,
            "Evan",
            "Witness functions return their report, and record and replay write a \
             full-report and store-snapshot sidecar.",
            C8_PROVING,
            "E4",
        ),
        satisfied(
            C9,
            "Evan",
            "The Draft override is run configuration that record writes into meta and \
             replay rebuilds.",
            C9_PROVING,
            "E5",
        ),
        Contract {
            id: C10,
            owner: "Evan",
            statement: "Each registered checkpoint fails the run when its source never \
                        observes, is dropped, skips a boundary, records outside its run, \
                        publishes past what it delivered, or delivers a different log.",
            proving: C10_PROVING,
            state: ContractState::Satisfied,
            delivery: "on main",
        },
        waiting(
            C11,
            "Evan",
            "PeerRecords and WireEvidence's projections sit behind a peer-evidence \
             checkpoint.",
            "E8",
            "a control that removing the peer checkpoint fails acceptance, a dropped \
             PeerControl test, and the store inventory taking the peer fields off C11's \
             allowance",
        ),
        satisfied(
            C12,
            "Evan",
            "The registry keeps every distinct end finding, not only an ID's first.",
            C12_PROVING,
            "E9",
        ),
        waiting(
            C13,
            "Adam's harness",
            "The fault store keeps every distinct fault and every break, and a dropped call \
             clears an untaken fault only after the harness's Shutdown.",
            "hf-1, hf-2, hf-3",
            "the four fault-store histories through the final report and its sidecar, which \
             hf-1 to hf-3 test at the store and check level only",
        ),
        satisfied(
            C14,
            "each store's hdr chunk",
            "Harness evidence stores behind checkpoints keep what their producer tests show.",
            C14_PROVING,
            "hf-6's store inventory",
        ),
        // hf-5 (#2197) made the unpatched fixtures settle, which
        // `the_fixtures_settle_after_their_measured_boundary_and_not_before`
        // shows. No test yet shows a bypassed wake's patched run settling,
        // the half a Draft run needs, so C15 stays blocked.
        blocked(
            C15,
            "Adam's harness",
            "Release-wake fixtures release their remaining hold after the measured boundary, \
             including when a bypass stops the wake, so a Draft run can finish.",
            "a test that a bypassed wake's patched run settles (hf-5's remaining half)",
        ),
        Contract {
            id: C16,
            owner: "Evan",
            statement: "The registry evaluates every property at a step before it returns \
                        the first Active violation, and keeps every violation found there.",
            proving: C16_PROVING,
            state: ContractState::Satisfied,
            delivery: "on main",
        },
        satisfied(
            C17,
            "Adam's harness",
            "Evidence stores freeze with the log and snapshot equal across cleanup, the whole \
             ObservationLog at the freeze equals the one Sim::run returns, and the pairer and \
             ledger consume each fact once.",
            C17_PROVING,
            "hf-6",
        ),
        satisfied(
            C18,
            "Evan (trace-3)",
            "Each trace names exactly the scenario and input or Plan it ran, and the \
             executable records and replays scripted Plans.",
            C18_PROVING,
            "E10",
        ),
        waiting(
            C19,
            "Adam's harness",
            "An unread final capacity boundary concludes under C1's drain or fails the run.",
            "hf-4, with C2",
            "hf-4's still-unreadable chunk-budget-bound case, which it left to a planted case",
        ),
        Contract {
            id: C20,
            owner: "Adam's harness",
            statement: "NA_REASONS and NOT_HEADER together list every NaReason, each once, and \
                        NA_REASONS each reason a header ID's report can carry.",
            proving: C20_PROVING,
            state: ContractState::Satisfied,
            delivery: "hdr-14.3",
        },
        waiting(
            C21,
            "Adam's harness",
            "Every header check keeps every finding at a boundary, not just its first.",
            "hf-8",
            "a two-finding report and sidecar test for every header check, those hdr-9, \
             hdr-10 and hdr-12 added after hf-8 included",
        ),
        waiting(
            C22,
            "Adam's harness",
            "Each commit-only-authorized finding names its rule kind and its actual subject.",
            "hf-9, and E4 for its proving test",
            "the overgrant credit break at report level, and both cases through the sidecar",
        ),
    ]
}

/// Who ran an execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RunBy {
    /// The acceptance tool itself, through `zakura-sim record` and `replay`.
    Tool,
    /// `cargo xtask planted`.
    Planted,
    /// A test binary.
    Test,
}

/// What a [`NARROWING`] entry decides on: the result's kind and harness,
/// and what each of its executions was.
#[derive(Clone, Debug)]
pub struct ResultCtx {
    /// The result's kind.
    pub kind: ResultKind,
    /// The harness its executions ran on, if any.
    pub harness: Option<String>,
    /// Whether that harness is a machine, by
    /// [`HarnessEntry::machine`](crate::harness::HarnessEntry::machine).
    pub machine: bool,
    /// Per execution: whether its `Draft` override was empty, and who ran
    /// it. Empty for a result with no execution, which no entry releases.
    pub executions: Vec<(bool, RunBy)>,
}

/// How a [`Narrowing`] entry's test stands.
#[derive(Clone, Copy, Debug)]
pub enum NarrowingTest {
    /// The test that shows the released results don't read the contract's
    /// evidence.
    Test(TestRef),
    /// Not written yet. The entry releases nothing until it is.
    Pending {
        /// The PR that adds it.
        until: &'static str,
    },
}

/// One release of results from contracts (Rule 24).
#[derive(Clone, Copy, Debug)]
pub struct Narrowing {
    /// Its name, such as `N1`.
    pub entry: &'static str,
    /// The contracts it releases from.
    pub contracts: &'static [ContractId],
    /// Whether it releases a result.
    pub releases: fn(&ResultCtx) -> bool,
    /// Why.
    pub reason: &'static str,
    /// The test that proves it.
    pub test: NarrowingTest,
}

/// The narrowing entries' tests' file.
pub const NARROWING_TESTS: &str = "tests/header_acceptance_narrowing.rs";

/// The node harnesses with no peers.
const PEERLESS: &[&str] = &[
    "header-sync/smoke",
    "header-sync/storage",
    "header-sync/release-wake",
    "header-sync/flood",
    "header-sync/repair",
];

/// Whether `ctx` has executions and every one satisfies `each`.
fn every_execution(ctx: &ResultCtx, each: impl Fn(bool, RunBy) -> bool) -> bool {
    !ctx.executions.is_empty()
        && ctx
            .executions
            .iter()
            .all(|&(empty_override, run_by)| each(empty_override, run_by))
}

/// The only narrowing entries, each with its test in [`NARROWING_TESTS`],
/// which takes the results it releases from the committed manifest and
/// fails if their runs carry the contract's evidence.
pub const NARROWING: &[Narrowing] = &[
    Narrowing {
        entry: "N1",
        contracts: &[ContractId::C11],
        releases: |ctx| {
            ctx.machine
                || ctx
                    .harness
                    .as_deref()
                    .is_some_and(|harness| PEERLESS.contains(&harness))
        },
        reason: "those harnesses build no peers, so no PeerRecords exist",
        test: NarrowingTest::Test(sim_test(
            NARROWING_TESTS,
            "n1_peerless_and_machine_results_read_no_peer_evidence",
        )),
    },
    Narrowing {
        entry: "N2",
        contracts: &[ContractId::C9],
        releases: |ctx| ctx.kind != ResultKind::Caught && every_execution(ctx, |empty, _| empty),
        reason: "only Draft recordings and their replays read the override",
        test: NarrowingTest::Test(sim_test(
            NARROWING_TESTS,
            "n2_released_results_carry_no_draft_override",
        )),
    },
    Narrowing {
        entry: "N3",
        contracts: &[ContractId::C15],
        releases: |ctx| {
            ctx.machine
                || ctx
                    .harness
                    .as_deref()
                    .is_some_and(|harness| harness != "header-sync/release-wake")
        },
        reason: "only header-sync/release-wake builds the release-wake fixtures",
        test: NarrowingTest::Test(sim_test(
            NARROWING_TESTS,
            "n3_only_release_wake_runs_a_wake_fixture",
        )),
    },
    Narrowing {
        entry: "N4",
        contracts: &[
            ContractId::C1,
            ContractId::C2,
            ContractId::C10,
            ContractId::C11,
            ContractId::C14,
            ContractId::C17,
            ContractId::C19,
        ],
        releases: |ctx| ctx.machine,
        reason: "machine histories run a transport-free loop, not Sim::run, its drain, \
                 checkpoints or stores",
        test: NarrowingTest::Test(sim_test(
            NARROWING_TESTS,
            "n4_machine_results_run_no_sim_drain_or_stores",
        )),
    },
    Narrowing {
        entry: "N5",
        contracts: &[ContractId::C4],
        releases: |ctx| {
            ctx.kind != ResultKind::Caught
                && every_execution(ctx, |_, run_by| run_by == RunBy::Tool)
        },
        reason: "the planted runner performs and replays its own cases, so only \
                 independently run histories are free of it",
        test: NarrowingTest::Test(sim_test(
            NARROWING_TESTS,
            "n5_released_results_are_recorded_by_the_tool",
        )),
    },
];

/// The fingerprint of [`NARROWING`]'s source together with every `const`,
/// `static` and `fn` in this file it reaches by name, transitively (today
/// `PEERLESS` and `every_execution`), as `tests/header_acceptance.rs`'s
/// `the_narrowing_rules_match_their_pin` computes it. Each entry's
/// `releases` is a closure, which the inventory pin
/// ([`INVENTORY_FINGERPRINT`](super::inventory::INVENTORY_FINGERPRINT))
/// can't compare, so a widened release, such as a harness added to
/// `PEERLESS`, moves this one, and needs a reread of what each entry
/// releases. Callees outside this file are their owners' contracts.
pub const NARROWING_FINGERPRINT: &str = "cbe285413fc88ad5";

/// The [`Key::ContractTest`] results: one `Passes` per proving test of each
/// `Satisfied` contract.
pub fn contract_test_results(table: &[Contract]) -> Vec<ExpectedResult> {
    table
        .iter()
        .filter(|contract| contract.state == ContractState::Satisfied)
        .flat_map(|contract| {
            contract.proving.iter().map(|&test| ExpectedResult {
                keys: vec![contract_test_key(contract.id, test)],
                gates: Vec::new(),
                expected: Expected::Passes { test },
            })
        })
        .collect()
}

/// The [`Key::ContractTest`] for `contract`'s proving test `test`.
pub fn contract_test_key(contract: ContractId, test: TestRef) -> Key {
    Key::ContractTest {
        contract: contract.name(),
        package: test.package.to_owned(),
        file: test.file.to_owned(),
        test: test.test.to_owned(),
    }
}

/// A pending narrowing entry's transitional `Open` until `until`, the PR
/// that adds its test. [`Key::covers`](super::Key::covers) takes no other.
pub fn narrowing_open(until: &'static str) -> Expected {
    Expected::Open {
        reason: "the entry's test judges results the acceptance tool produces",
        owner: "Adam's side",
        approval: Approval::Pending,
        until: Some(until),
    }
}

/// The [`Key::NarrowingTest`] results: each entry's test as a `Passes`, or
/// a transitional `Open` until the PR that adds it ([`narrowing_open`]).
pub fn narrowing_test_results(entries: &[Narrowing]) -> Vec<ExpectedResult> {
    entries
        .iter()
        .map(|entry| ExpectedResult {
            keys: vec![Key::NarrowingTest {
                entry: entry.entry.to_owned(),
            }],
            gates: Vec::new(),
            expected: match entry.test {
                NarrowingTest::Test(test) => Expected::Passes { test },
                NarrowingTest::Pending { until } => narrowing_open(until),
            },
        })
        .collect()
}
