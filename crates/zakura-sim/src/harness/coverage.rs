//! The coverage manifest: one row per property a harness reports, naming
//! the tests that show its check works and the CI job that runs them.
//!
//! `tests/harness.rs` compares the manifest with the harnesses' properties,
//! so a property without a row, or a row no harness reports, fails CI. It
//! also checks that each named test exists, and that CI runs it.

/// One property's coverage.
#[derive(Clone, Copy, Debug)]
pub struct Coverage {
    /// The property's ID.
    pub property: &'static str,
    /// A test where the property applies and holds.
    pub passing: Witness,
    /// A test where a planted bug violates the property.
    pub bypass: Witness,
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

/// A race fixture's row: its test runs the fix to a pass and finds each
/// planted variant.
const fn race(property: &'static str, test: &'static str) -> Coverage {
    Coverage {
        property,
        passing: sim_test("tests/race_fixtures.rs", test),
        bypass: sim_test("tests/race_fixtures.rs", test),
    }
}

/// Every property a harness reports, by ID.
pub static COVERAGE: &[Coverage] = &[
    Coverage {
        property: super::NO_TASK_PANIC,
        passing: sim_test("tests/harness.rs", "a_clean_run_holds_no_task_panic"),
        bypass: sim_test("tests/harness.rs", "task_panic_violates_no_task_panic"),
    },
    Coverage {
        property: crate::sim::NO_LIVELOCK,
        passing: sim_test("tests/harness.rs", "a_moving_clock_holds_no_livelock"),
        bypass: sim_test("tests/harness.rs", "livelock_fires_before_budget"),
    },
    race("race/lost-update", "lost_update"),
    race("race/snapshot-reserve", "snapshot_reserve"),
    race("race/missed-notify", "missed_notify"),
    race("race/timer-cancel", "timer_cancel"),
    race("race/ab-ba", "ab_ba"),
    race("race/abort-without-join", "abort_without_join"),
    Coverage {
        property: crate::domain::discovery::WAIT_COMPLETE_WAKES,
        passing: sim_test("tests/discovery.rs", "committed_input_wakes"),
        bypass: sim_test("tests/discovery.rs", "reverted_fix_misses_the_wakeup"),
    },
    Coverage {
        property: super::header_sync::OPERATION_IDS,
        passing: sim_test(
            "tests/header_node.rs",
            "node_syncs_from_a_mirror_on_controlled_storage",
        ),
        bypass: sim_test(
            "tests/header_node.rs",
            "planted_operation_ids_violate_the_property",
        ),
    },
    Coverage {
        property: super::header_sync::DIRECT_DISPATCH,
        passing: sim_test(
            "tests/header_node.rs",
            "node_syncs_from_a_mirror_on_controlled_storage",
        ),
        bypass: sim_test(
            "tests/header_node.rs",
            "direct_dispatch_guard_reports_a_violation",
        ),
    },
];
