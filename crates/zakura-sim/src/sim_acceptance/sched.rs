//! sched-1 to sched-3's property IDs and the tests their implementation
//! plans map each one to, for sched-4's certification rows (the sim-7
//! plan, "sched-4 certification").

pub use crate::harness::header_acceptance::TestRef;

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

/// The `zakura-scheduler` test `name` in `file`, its path in the package,
/// as a proof.
const fn scheduler(file: &'static str, name: &'static str) -> Proof {
    Proof::Test(TestRef {
        package: "zakura-scheduler",
        file,
        test: name,
    })
}

/// The `zakura-rt` test `name` in `file`, its path in the package, as a
/// proof.
const fn rt(file: &'static str, name: &'static str) -> Proof {
    Proof::Test(TestRef {
        package: "zakura-rt",
        file,
        test: name,
    })
}

/// Each sched-1 to sched-3 property and its proofs, from the mapping
/// table in its implementation plan.
pub const SCHED_MAPPING: &[(&str, &[Proof])] = &[
    (
        "S1.1",
        &[scheduler(
            "tests/model.rs",
            "generated_programs_match_the_readiness_model",
        )],
    ),
    (
        "S1.2",
        &[
            scheduler(
                "tests/model.rs",
                "generated_programs_match_the_readiness_model",
            ),
            scheduler(
                "tests/fixtures.rs",
                "worker_limit_gates_new_polls_but_not_resumes",
            ),
        ],
    ),
    (
        "S1.3",
        &[
            scheduler(
                "tests/fixtures.rs",
                "wake_during_a_poll_survives_any_number_of_points",
            ),
            scheduler("tests/fixtures.rs", "duplicate_wakes_leave_one_ready_entry"),
            scheduler("tests/fixtures.rs", "self_wake_survives_a_preemption_point"),
            scheduler("tests/fixtures.rs", "wake_while_suspended_mid_poll_is_kept"),
        ],
    ),
    (
        "S1.4",
        &[
            scheduler(
                "tests/rt.rs",
                "futures_unordered_polls_only_the_woken_child",
            ),
            scheduler(
                "tests/rt.rs",
                "select_branch_wakes_from_its_own_timer_or_channel",
            ),
            scheduler("tests/fixtures.rs", "self_wake_and_child_wakers_complete"),
        ],
    ),
    (
        "S1.5",
        &[
            scheduler(
                "tests/fixtures.rs",
                "stale_waker_from_an_earlier_run_is_ignored",
            ),
            scheduler("tests/fixtures.rs", "waker_from_another_thread_panics"),
        ],
    ),
    (
        "S1.6",
        &[
            scheduler(
                "tests/fixtures.rs",
                "ready_set_is_canonical_whatever_the_wake_order",
            ),
            scheduler(
                "tests/fixtures.rs",
                "unrelated_root_does_not_rename_task_paths",
            ),
            scheduler("tests/fixtures.rs", "same_seed_replays_the_same_run"),
            scheduler("tests/rt.rs", "same_seed_replays_rt_programs"),
        ],
    ),
    (
        "S1.7",
        &[
            scheduler(
                "tests/fixtures.rs",
                "saved_schedules_expose_planted_races_and_pass_fixed_versions",
            ),
            scheduler("tests/fixtures.rs", "finds_planted_races_with_two_workers"),
            scheduler("tests/fixtures.rs", "one_worker_hides_races_inside_a_poll"),
        ],
    ),
    (
        "S1.8",
        &[
            scheduler(
                "tests/rt.rs",
                "watch_lock_blocks_task_switches_until_it_drops",
            ),
            rt(
                "tests/sim_sync.rs",
                "points_are_skipped_while_a_watch_lock_is_held",
            ),
        ],
    ),
    (
        "S2.1",
        &[
            scheduler(
                "tests/model.rs",
                "generated_timer_sequences_match_the_ordered_model",
            ),
            scheduler("tests/model.rs", "advancing_into_the_past_panics"),
        ],
    ),
    (
        "S2.2",
        &[
            scheduler("tests/rt.rs", "repolled_sleep_keeps_only_the_latest_waker"),
            scheduler(
                "tests/rt.rs",
                "futures_unordered_polls_only_the_woken_child",
            ),
            scheduler(
                "tests/model.rs",
                "generated_timer_sequences_match_the_ordered_model",
            ),
        ],
    ),
    (
        "S2.3",
        &[
            scheduler(
                "tests/rt.rs",
                "task_woken_at_the_first_deadline_cancels_a_later_timer",
            ),
            scheduler(
                "tests/model.rs",
                "generated_timer_sequences_match_the_ordered_model",
            ),
        ],
    ),
    (
        "S2.4",
        &[
            scheduler(
                "tests/rt.rs",
                "mutex_release_lets_every_waiter_retry_and_one_wins",
            ),
            scheduler(
                "tests/rt.rs",
                "semaphore_grants_permits_in_fifo_order_and_skips_cancelled_waiters",
            ),
            scheduler(
                "tests/rt.rs",
                "bounded_channel_admits_senders_in_fifo_order_and_skips_cancelled_ones",
            ),
        ],
    ),
    (
        "S2.5",
        &[
            scheduler(
                "tests/fixtures.rs",
                "saved_schedules_for_lock_cycle_and_starvation",
            ),
            scheduler("tests/fixtures.rs", "opposite_lock_order_reports_deadlock"),
            scheduler(
                "tests/fixtures.rs",
                "lock_cycle_is_deadlock_even_with_other_parked_tasks",
            ),
            scheduler(
                "tests/fixtures.rs",
                "lock_held_across_await_starves_one_worker",
            ),
            scheduler("tests/rt.rs", "rt_mutex_reports_a_lock_cycle_as_deadlock"),
            scheduler(
                "tests/rt.rs",
                "rt_mutex_held_across_await_starves_one_worker",
            ),
        ],
    ),
    (
        "S2.6",
        &[
            scheduler("tests/fixtures.rs", "ready_reports_each_task_point"),
            scheduler("tests/fixtures.rs", "only_timers_pending_is_never_deadlock"),
            scheduler("tests/fixtures.rs", "external_wait_is_not_deadlock"),
            scheduler(
                "tests/fixtures.rs",
                "sim_owned_wait_is_not_an_external_wait",
            ),
        ],
    ),
    (
        "S2.7",
        &[
            scheduler(
                "tests/rt.rs",
                "timeout_cancels_its_timer_or_drops_its_future",
            ),
            scheduler(
                "tests/rt.rs",
                "biased_select_keeps_source_order_whatever_the_draws",
            ),
            scheduler(
                "tests/rt.rs",
                "interval_ticks_match_tokio_for_every_missed_tick_behavior",
            ),
            scheduler("tests/fixtures.rs", "fixed_versions_never_fail"),
        ],
    ),
    (
        "S3.1",
        &[scheduler(
            "tests/fixtures.rs",
            "abort_then_join_releases_each_resource_once",
        )],
    ),
    (
        "S3.2",
        &[
            scheduler(
                "tests/fixtures.rs",
                "completion_racing_abort_has_one_result",
            ),
            scheduler(
                "tests/rt.rs",
                "rt_join_returns_the_output_when_completion_races_abort",
            ),
            scheduler(
                "tests/fixtures.rs",
                "panic_is_isolated_and_stuck_task_is_cancelled",
            ),
            scheduler(
                "tests/rt.rs",
                "spawn_join_panic_and_abort_go_through_the_scheduler",
            ),
        ],
    ),
    (
        "S3.3",
        &[
            scheduler(
                "tests/rt.rs",
                "join_set_abort_all_shutdown_and_drop_release_every_task",
            ),
            scheduler(
                "tests/rt.rs",
                "dropping_a_plain_handle_detaches_but_abort_on_drop_aborts",
            ),
            scheduler(
                "tests/fixtures.rs",
                "abort_then_join_releases_each_resource_once",
            ),
        ],
    ),
    (
        "S3.4",
        &[
            scheduler(
                "tests/fixtures.rs",
                "shutdown_finishes_a_release_sequence_in_a_drop",
            ),
            scheduler(
                "tests/fixtures.rs",
                "shutdown_drop_can_take_a_contended_lock",
            ),
            scheduler(
                "tests/fixtures.rs",
                "shutdown_drops_idle_tasks_outside_an_unwind",
            ),
            scheduler(
                "tests/fixtures.rs",
                "shutdown_force_unwinds_only_a_lock_cycle",
            ),
            scheduler(
                "tests/fixtures.rs",
                "shutdown_drops_a_spawn_from_a_destructor_unpolled",
            ),
            scheduler(
                "tests/fixtures.rs",
                "forced_unwind_passes_through_a_production_catch_unwind",
            ),
            scheduler("tests/rt.rs", "shutdown_lets_a_drop_take_a_contended_mutex"),
        ],
    ),
    (
        "S3.5",
        &[
            scheduler(
                "tests/fixtures.rs",
                "cleanup_cannot_change_a_frozen_verdict",
            ),
            scheduler(
                "tests/fixtures.rs",
                "panic_in_a_leftover_drop_leaves_no_run_behind",
            ),
        ],
    ),
    (
        "S3.6",
        &[
            scheduler(
                "tests/repeated_runs.rs",
                "consecutive_runs_share_nothing_and_reuse_stacks",
            ),
            scheduler("tests/rt.rs", "consecutive_runs_reset_rt_per_run_state"),
            scheduler(
                "tests/rt.rs",
                "mutex_kept_across_runs_gets_a_fresh_identity",
            ),
            scheduler(
                "tests/fixtures.rs",
                "stale_waker_from_an_earlier_run_is_ignored",
            ),
        ],
    ),
    (
        "S3.7",
        &[
            Proof::Lane("sim-asan/zakura-rt-zakura-scheduler"),
            scheduler(
                "tests/fixtures.rs",
                "task_panic_reaches_the_harness_with_a_replaced_panic_hook",
            ),
        ],
    ),
];
