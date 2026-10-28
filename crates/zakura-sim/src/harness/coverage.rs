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
];
