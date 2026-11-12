//! sched-1 to sched-3's property IDs and the tests their implementation
//! plans map each one to, for sched-4's certification rows (the sim-7
//! plan, "sched-4 certification").

/// One test, by package, file under the package's `tests/`, and name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TestRef {
    /// The package, such as `zakura-scheduler`.
    pub package: &'static str,
    /// The integration test file's stem, such as `fixtures`.
    pub file: &'static str,
    /// The test function's name.
    pub name: &'static str,
}

/// What proves one sched-1 to sched-3 property.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Proof {
    /// One test, run so that exactly that test executes.
    Test(TestRef),
    /// A whole `qa/suite.toml` lane, by its `id`.
    Lane(&'static str),
}

/// Every property ID in the sched-1, sched-2 and sched-3 specs' tables, in
/// order, as those specs list them on 2026-11-11.
pub const SCHED_IDS: &[&str] = &[
    "S1.1", "S1.2", "S1.3", "S1.4", "S1.5", "S1.6", "S1.7", "S1.8", "S2.1", "S2.2", "S2.3", "S2.4",
    "S2.5", "S2.6", "S2.7", "S3.1", "S3.2", "S3.3", "S3.4", "S3.5", "S3.6", "S3.7",
];

const fn scheduler(file: &'static str, name: &'static str) -> Proof {
    Proof::Test(TestRef {
        package: "zakura-scheduler",
        file,
        name,
    })
}

const fn rt(file: &'static str, name: &'static str) -> Proof {
    Proof::Test(TestRef {
        package: "zakura-rt",
        file,
        name,
    })
}

/// Each sched-1 to sched-3 property and its proofs, from the mapping
/// table in its implementation plan.
pub const SCHED_MAPPING: &[(&str, &[Proof])] = &[
    (
        "S1.1",
        &[scheduler(
            "model",
            "generated_programs_match_the_readiness_model",
        )],
    ),
    (
        "S1.2",
        &[
            scheduler("model", "generated_programs_match_the_readiness_model"),
            scheduler("fixtures", "worker_limit_gates_new_polls_but_not_resumes"),
        ],
    ),
    (
        "S1.3",
        &[
            scheduler(
                "fixtures",
                "wake_during_a_poll_survives_any_number_of_points",
            ),
            scheduler("fixtures", "duplicate_wakes_leave_one_ready_entry"),
            scheduler("fixtures", "self_wake_survives_a_preemption_point"),
            scheduler("fixtures", "wake_while_suspended_mid_poll_is_kept"),
        ],
    ),
    (
        "S1.4",
        &[
            scheduler("rt", "futures_unordered_polls_only_the_woken_child"),
            scheduler("rt", "select_branch_wakes_from_its_own_timer_or_channel"),
            scheduler("fixtures", "self_wake_and_child_wakers_complete"),
        ],
    ),
    (
        "S1.5",
        &[
            scheduler("fixtures", "stale_waker_from_an_earlier_run_is_ignored"),
            scheduler("fixtures", "waker_from_another_thread_panics"),
        ],
    ),
    (
        "S1.6",
        &[
            scheduler("fixtures", "ready_set_is_canonical_whatever_the_wake_order"),
            scheduler("fixtures", "unrelated_root_does_not_rename_task_paths"),
            scheduler("fixtures", "same_seed_replays_the_same_run"),
            scheduler("rt", "same_seed_replays_rt_programs"),
        ],
    ),
    (
        "S1.7",
        &[
            scheduler(
                "fixtures",
                "saved_schedules_expose_planted_races_and_pass_fixed_versions",
            ),
            scheduler("fixtures", "finds_planted_races_with_two_workers"),
            scheduler("fixtures", "one_worker_hides_races_inside_a_poll"),
        ],
    ),
    (
        "S1.8",
        &[
            scheduler("rt", "watch_lock_blocks_task_switches_until_it_drops"),
            rt("sim_sync", "points_are_skipped_while_a_watch_lock_is_held"),
        ],
    ),
    (
        "S2.1",
        &[
            scheduler("model", "generated_timer_sequences_match_the_ordered_model"),
            scheduler("model", "advancing_into_the_past_panics"),
        ],
    ),
    (
        "S2.2",
        &[
            scheduler("rt", "repolled_sleep_keeps_only_the_latest_waker"),
            scheduler("rt", "futures_unordered_polls_only_the_woken_child"),
            scheduler("model", "generated_timer_sequences_match_the_ordered_model"),
        ],
    ),
    (
        "S2.3",
        &[
            scheduler(
                "rt",
                "task_woken_at_the_first_deadline_cancels_a_later_timer",
            ),
            scheduler("model", "generated_timer_sequences_match_the_ordered_model"),
        ],
    ),
    (
        "S2.4",
        &[
            scheduler("rt", "mutex_release_lets_every_waiter_retry_and_one_wins"),
            scheduler(
                "rt",
                "semaphore_grants_permits_in_fifo_order_and_skips_cancelled_waiters",
            ),
            scheduler(
                "rt",
                "bounded_channel_admits_senders_in_fifo_order_and_skips_cancelled_ones",
            ),
        ],
    ),
    (
        "S2.5",
        &[
            scheduler("fixtures", "saved_schedules_for_lock_cycle_and_starvation"),
            scheduler("fixtures", "opposite_lock_order_reports_deadlock"),
            scheduler(
                "fixtures",
                "lock_cycle_is_deadlock_even_with_other_parked_tasks",
            ),
            scheduler("fixtures", "lock_held_across_await_starves_one_worker"),
            scheduler("rt", "rt_mutex_reports_a_lock_cycle_as_deadlock"),
            scheduler("rt", "rt_mutex_held_across_await_starves_one_worker"),
        ],
    ),
    (
        "S2.6",
        &[
            scheduler("fixtures", "ready_reports_each_task_point"),
            scheduler("fixtures", "only_timers_pending_is_never_deadlock"),
            scheduler("fixtures", "external_wait_is_not_deadlock"),
            scheduler("fixtures", "sim_owned_wait_is_not_an_external_wait"),
        ],
    ),
    (
        "S2.7",
        &[
            scheduler("rt", "timeout_cancels_its_timer_or_drops_its_future"),
            scheduler("rt", "biased_select_keeps_source_order_whatever_the_draws"),
            scheduler(
                "rt",
                "interval_ticks_match_tokio_for_every_missed_tick_behavior",
            ),
            scheduler("fixtures", "fixed_versions_never_fail"),
        ],
    ),
    (
        "S3.1",
        &[scheduler(
            "fixtures",
            "abort_then_join_releases_each_resource_once",
        )],
    ),
    (
        "S3.2",
        &[
            scheduler("fixtures", "completion_racing_abort_has_one_result"),
            scheduler(
                "rt",
                "rt_join_returns_the_output_when_completion_races_abort",
            ),
            scheduler("fixtures", "panic_is_isolated_and_stuck_task_is_cancelled"),
            scheduler("rt", "spawn_join_panic_and_abort_go_through_the_scheduler"),
        ],
    ),
    (
        "S3.3",
        &[
            scheduler(
                "rt",
                "join_set_abort_all_shutdown_and_drop_release_every_task",
            ),
            scheduler(
                "rt",
                "dropping_a_plain_handle_detaches_but_abort_on_drop_aborts",
            ),
            scheduler("fixtures", "abort_then_join_releases_each_resource_once"),
        ],
    ),
    (
        "S3.4",
        &[
            scheduler("fixtures", "shutdown_finishes_a_release_sequence_in_a_drop"),
            scheduler("fixtures", "shutdown_drop_can_take_a_contended_lock"),
            scheduler("fixtures", "shutdown_drops_idle_tasks_outside_an_unwind"),
            scheduler("fixtures", "shutdown_force_unwinds_only_a_lock_cycle"),
            scheduler(
                "fixtures",
                "shutdown_drops_a_spawn_from_a_destructor_unpolled",
            ),
            scheduler(
                "fixtures",
                "forced_unwind_passes_through_a_production_catch_unwind",
            ),
            scheduler("rt", "shutdown_lets_a_drop_take_a_contended_mutex"),
        ],
    ),
    (
        "S3.5",
        &[
            scheduler("fixtures", "cleanup_cannot_change_a_frozen_verdict"),
            scheduler("fixtures", "panic_in_a_leftover_drop_leaves_no_run_behind"),
        ],
    ),
    (
        "S3.6",
        &[
            scheduler(
                "repeated_runs",
                "consecutive_runs_share_nothing_and_reuse_stacks",
            ),
            scheduler("rt", "consecutive_runs_reset_rt_per_run_state"),
            scheduler("rt", "mutex_kept_across_runs_gets_a_fresh_identity"),
            scheduler("fixtures", "stale_waker_from_an_earlier_run_is_ignored"),
        ],
    ),
    (
        "S3.7",
        &[
            Proof::Lane("sim-asan/zakura-rt-zakura-scheduler"),
            scheduler(
                "fixtures",
                "task_panic_reaches_the_harness_with_a_replaced_panic_hook",
            ),
        ],
    ),
];
