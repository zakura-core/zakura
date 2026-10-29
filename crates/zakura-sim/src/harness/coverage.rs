//! The coverage manifest: one row per property a harness reports, naming
//! the tests that show its check works and the CI job that runs them.
//!
//! `tests/harness.rs` compares the manifest with the harnesses' properties,
//! so a property without a row, or a row no harness reports, fails CI. It
//! also checks that each named test or patch exists, and that CI runs it.

/// One property's coverage.
#[derive(Clone, Copy, Debug)]
pub struct Coverage {
    /// The property's ID.
    pub property: &'static str,
    /// Where the property applies and holds.
    pub passing: Passing,
    /// Where a planted bug violates the property.
    pub bypass: Bypass,
}

/// Where a property applies and holds.
#[derive(Clone, Copy, Debug)]
pub enum Passing {
    /// A test where the property applies and holds.
    Test(Witness),
    /// No scenario reaches the property, for `reason`. The witness test
    /// shows that it stays `NotApplicable`.
    Unreachable {
        /// Why no scenario reaches it.
        reason: &'static str,
        /// The test where it stays `NotApplicable`.
        witness: Witness,
    },
}

impl Passing {
    /// The test this names.
    pub fn witness(self) -> Witness {
        match self {
            Passing::Test(witness) | Passing::Unreachable { witness, .. } => witness,
        }
    }
}

/// Where a planted bug violates a property.
#[derive(Clone, Copy, Debug)]
pub enum Bypass {
    /// A test that runs a planted variant itself, as the race fixtures do.
    Test(Witness),
    /// A patch `qa/planted/<property>/<name>.patch`, which `cargo xtask
    /// planted` applies to a scratch checkout. The name is without
    /// `.patch`.
    Planted(&'static str),
    /// No bug can violate the property yet: its check is missing what the
    /// reason names. Only a `Draft` property names one.
    Pending(&'static str),
}

/// A test that runs a property, and the CI job that runs the test.
#[derive(Clone, Copy, Debug)]
pub struct Witness {
    /// The test's file, relative to the crate.
    pub file: &'static str,
    /// The test's name.
    pub test: &'static str,
    /// The `tests-unit.yml` job that runs it. An ignored test also needs
    /// its name in that workflow.
    pub ci: &'static str,
}

/// The job that runs every `zakura-sim` test under `--cfg zakura_sim`.
const SIM_BUILD: &str = "zakura_sim build";

/// A test in [`SIM_BUILD`].
const fn sim_test(file: &'static str, test: &'static str) -> Witness {
    Witness {
        file,
        test,
        ci: SIM_BUILD,
    }
}

/// The job that runs the ignored `zakura-sim` witness tests whose runs
/// take minutes, in a release build under `--cfg zakura_sim`.
const SLOW_WITNESSES: &str = "zakura_sim slow witnesses";

/// An ignored test in [`SLOW_WITNESSES`].
const fn slow_test(file: &'static str, test: &'static str) -> Witness {
    Witness {
        file,
        test,
        ci: SLOW_WITNESSES,
    }
}

/// A [`SIM_BUILD`] test where a property applies and holds.
const fn passes(file: &'static str, test: &'static str) -> Passing {
    Passing::Test(sim_test(file, test))
}

/// A race fixture's row: its witness test runs the fix to a pass and the
/// planted race to its violation, both through the race's trigger history.
const fn race(property: &'static str, test: &'static str) -> Coverage {
    Coverage {
        property,
        passing: passes("tests/race_fixtures.rs", test),
        bypass: Bypass::Test(sim_test("tests/race_fixtures.rs", test)),
    }
}

/// A stream-6 generated property's row: its witness in
/// `tests/block_table.rs`, and the scripted bypass there that violates
/// every generated ID.
const fn table(property: &'static str, passing: &'static str) -> Coverage {
    Coverage {
        property,
        passing: passes("tests/block_table.rs", passing),
        bypass: TABLE_BYPASS,
    }
}

/// A stream-6 generated property no block-sync scenario reaches, for
/// `reason`: [`UNREACHED`] shows it stays `NotApplicable`, and the scripted
/// bypass still violates it.
const fn unreachable(property: &'static str, reason: &'static str) -> Coverage {
    Coverage {
        property,
        passing: Passing::Unreachable {
            reason,
            witness: sim_test("tests/block_table.rs", UNREACHED),
        },
        bypass: TABLE_BYPASS,
    }
}

/// The scripted bypass that violates every stream-6 generated ID.
const TABLE_BYPASS: Bypass = Bypass::Test(sim_test(
    "tests/block_table.rs",
    "scripted_bypasses_violate_every_generated_id",
));

/// An honest sync or honest serving holds it.
const HONEST: &str = "honest_runs_hold_every_generated_id_they_reach";
/// A probe fault outside the node's own exchanges reaches it.
const PROBE_FAULT: &str = "each_probe_fault_reaches_its_exact_id";
/// A probe fault inside the node's own exchange reaches it.
const EXCHANGE_FAULT: &str = "faults_inside_the_nodes_exchange_reach_their_exact_ids";
/// Requests past twice the node's serving limit reach it.
const OVER_MARGIN: &str = "requests_past_twice_the_serving_limit_reach_over_margin";
/// The witness that no scenario reaches an [`unreachable`] row.
const UNREACHED: &str = "ids_no_scenario_reaches_stay_not_applicable";

/// Every property a harness reports, by ID.
pub static COVERAGE: &[Coverage] = &[
    Coverage {
        property: super::NO_TASK_PANIC,
        passing: passes("tests/harness.rs", "a_clean_run_holds_no_task_panic"),
        bypass: Bypass::Test(sim_test(
            "tests/harness.rs",
            "task_panic_violates_no_task_panic",
        )),
    },
    Coverage {
        property: crate::sim::NO_LIVELOCK,
        passing: passes("tests/harness.rs", "a_moving_clock_holds_no_livelock"),
        bypass: Bypass::Test(sim_test("tests/harness.rs", "livelock_fires_before_budget")),
    },
    race("race/lost-update", "lost_update_witness"),
    race("race/snapshot-reserve", "snapshot_reserve_witness"),
    race("race/missed-notify", "missed_notify_witness"),
    race("race/timer-cancel", "timer_cancel_witness"),
    race("race/ab-ba", "ab_ba_witness"),
    race("race/abort-without-join", "abort_without_join_witness"),
    Coverage {
        property: crate::domain::discovery::WAIT_COMPLETE_WAKES,
        passing: passes("tests/witness.rs", "discovery_wait_complete_lost_wakeup"),
        bypass: Bypass::Planted("lost-wakeup"),
    },
    Coverage {
        property: super::header_sync::OPERATION_IDS,
        passing: passes(
            "tests/header_node.rs",
            "node_syncs_from_a_mirror_on_controlled_storage",
        ),
        bypass: Bypass::Test(sim_test(
            "tests/header_node.rs",
            "planted_operation_ids_violate_the_property",
        )),
    },
    Coverage {
        property: super::header_sync::DIRECT_DISPATCH,
        passing: passes(
            "tests/header_node.rs",
            "node_syncs_from_a_mirror_on_controlled_storage",
        ),
        bypass: Bypass::Test(sim_test(
            "tests/header_node.rs",
            "direct_dispatch_guard_reports_a_violation",
        )),
    },
    Coverage {
        property: super::block_sync::OPERATION_IDS,
        passing: passes("tests/block_node.rs", "honest_sync_reaches_the_fixture_tip"),
        bypass: Bypass::Test(sim_test(
            "tests/block_node.rs",
            "planted_block_operation_ids_violate_the_property",
        )),
    },
    table("block-sync/s6v3/block/receiver-work-authorized", HONEST),
    table("block-sync/s6v3/block/sender-answers-open", HONEST),
    table(
        "block-sync/s6v3/block/sender-within-response-quantity",
        HONEST,
    ),
    table("block-sync/s6v3/blocks-done/sender-ends-once", HONEST),
    table(
        "block-sync/s6v3/blocks-done/sender-within-response-quantity",
        HONEST,
    ),
    table("block-sync/s6v3/conformant-never-closes", HONEST),
    table(
        "block-sync/s6v3/get-blocks/receiver-serving-bounded",
        HONEST,
    ),
    table("block-sync/s6v3/get-blocks/sender-within-limit", HONEST),
    table("block-sync/s6v3/range-unavailable/sender-ends-once", HONEST),
    table(
        "block-sync/s6v3/range-unavailable/sender-within-response-quantity",
        HONEST,
    ),
    table("block-sync/s6v3/status/sender-cadence", HONEST),
    table("block-sync/s6v3/block/long-payload-closes", PROBE_FAULT),
    table("block-sync/s6v3/block/reserved-flags-closes", PROBE_FAULT),
    table("block-sync/s6v3/block/short-payload-closes", PROBE_FAULT),
    table("block-sync/s6v3/block/unsolicited-closes", PROBE_FAULT),
    table(
        "block-sync/s6v3/blocks-done/long-payload-closes",
        PROBE_FAULT,
    ),
    table(
        "block-sync/s6v3/blocks-done/reserved-flags-closes",
        PROBE_FAULT,
    ),
    table(
        "block-sync/s6v3/blocks-done/short-payload-closes",
        PROBE_FAULT,
    ),
    table(
        "block-sync/s6v3/blocks-done/unsolicited-closes",
        PROBE_FAULT,
    ),
    table("block-sync/s6v3/get-blocks/bad-content-closes", PROBE_FAULT),
    table(
        "block-sync/s6v3/get-blocks/long-payload-closes",
        PROBE_FAULT,
    ),
    table(
        "block-sync/s6v3/get-blocks/reserved-flags-closes",
        PROBE_FAULT,
    ),
    table(
        "block-sync/s6v3/get-blocks/short-payload-closes",
        PROBE_FAULT,
    ),
    table(
        "block-sync/s6v3/range-unavailable/long-payload-closes",
        PROBE_FAULT,
    ),
    table(
        "block-sync/s6v3/range-unavailable/reserved-flags-closes",
        PROBE_FAULT,
    ),
    table(
        "block-sync/s6v3/range-unavailable/short-payload-closes",
        PROBE_FAULT,
    ),
    table(
        "block-sync/s6v3/range-unavailable/unsolicited-closes",
        PROBE_FAULT,
    ),
    table("block-sync/s6v3/status/bad-content-closes", PROBE_FAULT),
    table("block-sync/s6v3/status/cadence-never-closes", PROBE_FAULT),
    table("block-sync/s6v3/status/long-payload-closes", PROBE_FAULT),
    table("block-sync/s6v3/status/reserved-flags-closes", PROBE_FAULT),
    table("block-sync/s6v3/status/short-payload-closes", PROBE_FAULT),
    table("block-sync/s6v3/undeclared-type-closes", PROBE_FAULT),
    table("block-sync/s6v3/block/bad-content-closes", EXCHANGE_FAULT),
    table("block-sync/s6v3/block/over-owed-closes", EXCHANGE_FAULT),
    table(
        "block-sync/s6v3/blocks-done/bad-content-closes",
        EXCHANGE_FAULT,
    ),
    table(
        "block-sync/s6v3/range-unavailable/bad-content-closes",
        EXCHANGE_FAULT,
    ),
    table("block-sync/s6v3/get-blocks/over-margin-closes", OVER_MARGIN),
    unreachable(
        "block-sync/s6v3/blocks-done/over-owed-closes",
        "every exchange owes room for its ending, and conformant bodies leave it",
    ),
    unreachable(
        "block-sync/s6v3/range-unavailable/over-owed-closes",
        "every exchange owes room for its ending, and conformant bodies leave it",
    ),
    unreachable(
        "block-sync/s6v3/blocks-done/receiver-work-authorized",
        "an ending carries no body, so the node admits no work from it",
    ),
    unreachable(
        "block-sync/s6v3/range-unavailable/receiver-work-authorized",
        "an ending carries no body, so the node admits no work from it",
    ),
    // The stuck floor's witness test is ignored, since one debug run takes
    // minutes. `cargo xtask planted` runs its committed input unpatched as a
    // witness test that expects a hold, before it runs the patch.
    Coverage {
        property: crate::domain::block_sync::RETAINED_BYTE_BOUND,
        passing: passes(
            "tests/block_resource_properties.rs",
            "honest_runs_hold_every_resource_bound_a_a_and_a_b_a",
        ),
        bypass: Bypass::Planted("wide-commit-window"),
    },
    Coverage {
        property: crate::domain::block_sync::RESERVED_BYTE_BOUND,
        passing: passes(
            "tests/block_resource_properties.rs",
            "the_paused_handoffs_input_holds_reserved_byte_bound",
        ),
        bypass: Bypass::Planted("above-floor-charge"),
    },
    Coverage {
        property: crate::domain::block_sync::BUDGET_LEDGERS_AGREE,
        passing: passes(
            "tests/block_resource_properties.rs",
            "the_handoff_input_holds_budget_ledgers_agree",
        ),
        bypass: Bypass::Planted("handoff-keeps-charge"),
    },
    // The above-window witness test is ignored, since one debug run takes
    // about 100 s. `cargo xtask planted` runs its committed input unpatched
    // as a witness test that expects a hold, before it runs the patch.
    Coverage {
        property: crate::domain::block_sync::LOOKAHEAD_OVERSHOOT_BOUND,
        passing: passes(
            "tests/block_resource_properties.rs",
            "the_above_window_input_holds_lookahead_overshoot_bound",
        ),
        bypass: Bypass::Planted("omit-outstanding-estimates"),
    },
    Coverage {
        property: crate::domain::block_sync::TEARDOWN_RETURNS_BASELINE,
        passing: passes(
            "tests/block_resource_properties.rs",
            "block_sync_teardown_returns_baseline",
        ),
        bypass: Bypass::Planted("skip-return-on-drop"),
    },
    Coverage {
        property: crate::domain::block_sync_semantic::SUBMIT_ONLY_REQUESTED,
        passing: passes(
            "tests/block_semantic_properties.rs",
            "the_reset_mid_flight_input_holds_submit_only_requested",
        ),
        bypass: Bypass::Planted("height-only"),
    },
    Coverage {
        property: crate::domain::block_sync_semantic::STALE_COMPLETION_INERT,
        passing: passes(
            "tests/block_semantic_properties.rs",
            "the_stale_beside_current_input_holds_stale_completion_inert",
        ),
        bypass: Bypass::Planted("release-by-height"),
    },
    // Both capacity tests are ignored, since each debug run takes about
    // eight minutes. The `zakura_sim slow witnesses` job runs them. Main's
    // sequencer is the bypass: the frontier-first input violates it (#1523).
    Coverage {
        property: crate::domain::block_sync_semantic::CAPACITY_RELEASE_WAKES,
        passing: Passing::Test(slow_test(
            "tests/block_semantic_properties.rs",
            "the_reply_first_input_holds_capacity_release_wakes",
        )),
        bypass: Bypass::Test(slow_test(
            "tests/block_semantic_properties.rs",
            "the_frontier_first_input_violates_capacity_release_wakes",
        )),
    },
    Coverage {
        property: crate::domain::block_sync_semantic::DETACHED_SUBMISSION_BOUNDED,
        passing: passes(
            "tests/block_semantic_properties.rs",
            "the_stalled_reset_input_holds_detached_submission_bounded",
        ),
        bypass: Bypass::Planted("forget-on-reset"),
    },
    // The decoded-input-stall test is ignored, since its debug run takes
    // minutes. The `zakura_sim slow witnesses` job runs it, and the planted
    // case runs the same input.
    Coverage {
        property: crate::domain::block_sync_semantic::INPUT_DECODED_BOUND,
        passing: Passing::Test(slow_test(
            "tests/block_semantic_properties.rs",
            "the_decoded_input_stall_input_holds_input_decoded_bound",
        )),
        bypass: Bypass::Planted("no-room-check"),
    },
];
