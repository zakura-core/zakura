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

/// The contracts table, one row per contract. C3, C10 and C16 are
/// delivered on `main`, and C20 by hdr-14.3. C14's proving tests are the store inventory's
/// producer tests, which hf-6 adds, so it stays blocked until then.
pub fn contract_table() -> Vec<Contract> {
    use ContractId::*;
    vec![
        blocked(
            C1,
            "Evan",
            "Run completion: the drain offers only recovery actions and advances time through \
             timer boundaries and obligation deadlines, every open obligation concludes, a \
             drain that hits its bound is a harness failure, and NotApplicable carries its \
             reason (zakura#1840).",
            "E1",
        ),
        blocked(
            C2,
            "Adam's harness",
            "header-fault-penalized's open penalty and teardown's unsettled rounds and stop \
             conclude under C1's drain.",
            "hf-4, after E1",
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
        blocked(
            C4,
            "Evan (xt/planted.rs)",
            "The planted runner classifies baseline and patched runs, replays every catch \
             fresh, puts a deadline on every run and judges nested cases by their set.",
            "E6",
        ),
        blocked(
            C5,
            "Evan",
            "Every executed binary's source digest is computed and equals the checkout it \
             should come from, and unknown never matches.",
            "E7",
        ),
        blocked(
            C6,
            "Evan",
            "The witness judge rejects any unrelated NotApplicable whose reason isn't benign, \
             except StoppedByViolation under the expected stop, or a missing report, and \
             judges a nested case's exact set in both runs, each nested finding from the \
             case's own break: the same peer and the same punished event or frame.",
            "E2",
        ),
        blocked(
            C7,
            "Evan",
            "A trigger is handed its PropertyId, and each generated template has a receiver \
             trigger and a sender trigger that requires the guarded exchange.",
            "E3",
        ),
        blocked(
            C8,
            "Evan",
            "Witness functions return their report, and record and replay write a \
             full-report and store-snapshot sidecar.",
            "E4",
        ),
        blocked(
            C9,
            "Evan",
            "The Draft override is run configuration that record writes into meta and \
             replay rebuilds.",
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
        blocked(
            C11,
            "Evan",
            "PeerRecords and WireEvidence's projections sit behind a peer-evidence \
             checkpoint.",
            "E8",
        ),
        blocked(
            C12,
            "Evan",
            "The registry keeps every distinct end finding, not only an ID's first.",
            "E9",
        ),
        blocked(
            C13,
            "Adam's harness",
            "The fault store keeps every distinct fault and every break, and a dropped call \
             clears an untaken fault only after the harness's Shutdown.",
            "hf-1, hf-2, hf-3",
        ),
        blocked(
            C14,
            "each store's hdr chunk",
            "Harness evidence stores behind checkpoints keep what their producer tests show.",
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
        blocked(
            C17,
            "Adam's harness",
            "Evidence stores freeze with the log and snapshot equal across cleanup, the whole \
             ObservationLog at the freeze equals the one Sim::run returns, and the pairer and \
             ledger consume each fact once.",
            "hf-6",
        ),
        blocked(
            C18,
            "Evan (trace-3)",
            "Each trace names exactly the scenario and input or Plan it ran, and the \
             executable records and replays scripted Plans.",
            "E10",
        ),
        blocked(
            C19,
            "Adam's harness",
            "An unread final capacity boundary concludes under C1's drain or fails the run.",
            "hf-4, with C2",
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
        blocked(
            C21,
            "Adam's harness",
            "Every header check keeps every finding at a boundary, not just its first.",
            "hf-8",
        ),
        blocked(
            C22,
            "Adam's harness",
            "Each commit-only-authorized finding names its rule kind and its actual subject.",
            "hf-9, and E4 for its proving test",
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
