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
    /// A test where the property applies and holds.
    pub passing: Witness,
    /// Where a planted bug violates the property.
    pub bypass: Bypass,
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

/// A race fixture's row: its witness test runs the fix to a pass and the
/// planted race to its violation, both through the race's trigger history.
const fn race(property: &'static str, test: &'static str) -> Coverage {
    Coverage {
        property,
        passing: sim_test("tests/race_fixtures.rs", test),
        bypass: Bypass::Test(sim_test("tests/race_fixtures.rs", test)),
    }
}

/// Every property a harness reports, by ID.
pub static COVERAGE: &[Coverage] = &[
    Coverage {
        property: super::NO_TASK_PANIC,
        passing: sim_test("tests/harness.rs", "a_clean_run_holds_no_task_panic"),
        bypass: Bypass::Test(sim_test(
            "tests/harness.rs",
            "task_panic_violates_no_task_panic",
        )),
    },
    Coverage {
        property: crate::sim::NO_LIVELOCK,
        passing: sim_test("tests/harness.rs", "a_moving_clock_holds_no_livelock"),
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
        passing: sim_test("tests/witness.rs", "discovery_wait_complete_lost_wakeup"),
        bypass: Bypass::Planted("lost-wakeup"),
    },
    Coverage {
        property: super::header_sync::OPERATION_IDS,
        passing: sim_test(
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
        passing: sim_test(
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
        passing: sim_test("tests/block_node.rs", "honest_sync_reaches_the_fixture_tip"),
        bypass: Bypass::Test(sim_test(
            "tests/block_node.rs",
            "planted_block_operation_ids_violate_the_property",
        )),
    },
];
