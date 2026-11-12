//! sim-7's manifest: one row per requirement in the sim-7 plan, each with
//! the exact results that prove it. Families (seeds, seed witnesses, end
//! records, race schedules, generated header table IDs, sched-1 to sched-3
//! IDs) are derived from the code and the committed files at S, so a new
//! member joins by itself (the plan's Rule 7).

use std::{
    collections::BTreeSet,
    fmt,
    path::{Path, PathBuf},
};

use super::sched::{Proof, TestRef, SCHED_IDS, SCHED_MAPPING};
use crate::harness::header_acceptance::{tool::committed_required, Key, Manifest};

/// One expected result, which the tool produces and the evaluator judges.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Expected {
    /// One integration test, run so exactly it executes.
    Test(TestRef),
    /// One unit test of a package's library, by its path in the crate.
    UnitTest {
        /// The package.
        package: &'static str,
        /// The test's path, such as `sim::tests::held_borrow_is_harness_not_panic`.
        path: &'static str,
    },
    /// One `qa/suite.toml` lane, judged by Rule 11's lane contract.
    Lane(&'static str),
    /// One test in the `qa/fuzz/sim` workspace's library.
    FuzzTest(&'static str),
    /// One committed seed, run in the release sim build and judged by
    /// `owned_run_passes`.
    Seed {
        /// The seed's file, relative to the repository's root.
        file: String,
    },
    /// One witness a committed seed carries, by the suite manifest's ID.
    SeedWitness(String),
    /// One committed seed's end record, which its run must reproduce.
    EndRecord {
        /// The `*.end.json` file, relative to the repository's root.
        file: String,
    },
    /// One of sched-4's saved race schedules, and its fixed variant on the
    /// same bytes.
    RaceSchedule(String),
    /// One key in hdr-14's accepted record.
    HeaderKey(Key),
    /// One planted case's verdict, by its property.
    Planted(&'static str),
    /// One fuzz target's run over its seeds, built in the run's own
    /// directory.
    Target(&'static str),
    /// The header minimization of one planted case.
    Minimize(&'static str),
    /// One schedule-agreement check, by its pair's name.
    Agreement(String),
    /// One S4.6 command, by ID.
    Command(String),
    /// One of sim-7's own static checks and evaluator controls in
    /// `tests/sim_acceptance.rs`, by its path in that test binary.
    SelfTest(String),
    /// One measured recording, by harness.
    Measure(&'static str),
}

impl fmt::Display for Expected {
    /// The result's ID, unique in the manifest.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expected::Test(t) => write!(f, "test/{}/{}/{}", t.package, t.file, t.name),
            Expected::UnitTest { package, path } => write!(f, "unit/{package}/{path}"),
            Expected::Lane(id) => write!(f, "lane/{id}"),
            Expected::FuzzTest(name) => write!(f, "fuzz-test/{name}"),
            Expected::Seed { file } => write!(f, "seed/{file}"),
            Expected::SeedWitness(id) => write!(f, "{id}"),
            Expected::EndRecord { file } => write!(f, "end-record/{file}"),
            Expected::RaceSchedule(name) => write!(f, "race-schedule/{name}"),
            Expected::HeaderKey(key) => write!(f, "header/{key:?}"),
            Expected::Planted(property) => write!(f, "planted/{property}"),
            Expected::Target(name) => write!(f, "target/{name}"),
            Expected::Minimize(property) => write!(f, "minimize/{property}"),
            Expected::Agreement(name) => write!(f, "agreement/{name}"),
            Expected::Command(id) => write!(f, "command/{id}"),
            Expected::SelfTest(path) => write!(f, "self-test/{path}"),
            Expected::Measure(harness) => write!(f, "measure/{harness}"),
        }
    }
}

/// One requirement and its results.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Row {
    /// The requirement's ID, such as `M7.2`, `corpus/discovery` or `S4.4`.
    pub id: String,
    /// Whether an exclusion of any of its results blocks it instead.
    pub never_excluded: bool,
    /// Its results.
    pub expected: Vec<Expected>,
}

/// What the derived families are read from at S.
#[derive(Clone, Debug, Default)]
pub struct Derived {
    /// Every committed seed's file, admitted or not.
    pub seeds: Vec<String>,
    /// Every seed witness's suite-manifest ID.
    pub seed_witnesses: Vec<String>,
    /// Every committed `*.end.json` beside a seed.
    pub end_records: Vec<String>,
    /// Every saved race schedule's name under
    /// `tests/fixtures/race-schedules/`.
    pub race_schedules: Vec<String>,
    /// Every generated `header-sync/s5v9/…` property hdr-14 requires.
    pub table_properties: Vec<String>,
    /// Every `qa/suite.toml` `sim-package` row's package, each one S4.6
    /// compile command.
    pub sim_packages: Vec<String>,
    /// Every test in `tests/sim_acceptance.rs`, by its path in that binary.
    pub self_tests: Vec<String>,
}

const fn sim(file: &'static str, name: &'static str) -> Expected {
    Expected::Test(TestRef {
        package: "zakura-sim",
        file,
        name,
    })
}

/// `sim`'s tests in `file`, one result each.
fn sims(file: &'static str, names: &[&'static str]) -> Vec<Expected> {
    names.iter().map(|name| sim(file, name)).collect()
}

fn passing(property: &str) -> Expected {
    Expected::HeaderKey(Key::passing(property))
}

fn catch(property: &str) -> Expected {
    Expected::HeaderKey(Key::catch(property))
}

fn requirement(name: &str) -> Expected {
    Expected::HeaderKey(Key::Requirement {
        name: name.to_owned(),
    })
}

fn row(id: &str, never_excluded: bool, expected: Vec<Expected>) -> Row {
    Row {
        id: id.to_owned(),
        never_excluded,
        expected,
    }
}

/// The discovery pair and the six race pairs the agreement rows compare.
pub const AGREEMENTS: &[&str] = &[
    "discovery/wait-complete-wakes",
    "race/lost-update",
    "race/snapshot-reserve",
    "race/missed-notify",
    "race/timer-cancel",
    "race/ab-ba",
    "race/abort-without-join",
];

/// Every row, in the plan's order, with its families from `derived`.
pub fn rows(derived: &Derived) -> Vec<Row> {
    let mut rows = vec![
        row(
            "M7.1",
            false,
            [
                sims(
                    "trace_format",
                    &[
                        "every_record_round_trips",
                        "real_run_round_trips",
                        "equal_runs_write_equal_bytes",
                        "missing_blob",
                        "digest_mismatch",
                        "malformed_line",
                        "unsupported_format",
                        "unsupported_contract",
                        "previous_machine_formats_are_rejected",
                        "truncated_is_prefix",
                        "every_cut_near_the_end_is_truncated",
                        "strict_reader",
                    ],
                ),
                vec![sim("replay_exact", "prefix_replays_and_is_never_complete")],
            ]
            .concat(),
        ),
        row(
            "M7.2",
            false,
            [
                vec![sim("cli", "every_harness_replays_byte_for_byte_in_a_fresh_process")],
                sims(
                    "replay_exact",
                    &[
                        "recorded_traces_replay_exact",
                        "matching_violation_is_a_success",
                        "effects_replay_in_a_fresh_process",
                        "replay_returns_the_violations_end_leaves_out",
                        "planted_payload_byte_is_nondeterministic",
                        "changed_setup_observation_is_nondeterministic",
                    ],
                ),
                vec![
                    sim("net", "recorded_steps_tell_equal_length_payloads_apart"),
                    sim("full_report", "record_keeps_the_full_report_and_replay_matches_it"),
                    sim("race_schedules", "fresh_processes_reproduce_each_saved_violation"),
                ],
            ]
            .concat(),
        ),
        row(
            "M7.3",
            false,
            [
                sims(
                    "replay_exact",
                    &[
                        "non_equivalent_build_refused",
                        "allow_build_mismatch_never_succeeds",
                        "diagnostic_flags_never_verify",
                    ],
                ),
                vec![
                    sim("cli", "replay_exit_codes"),
                    sim("trace_format", "build_equivalence"),
                ],
            ]
            .concat(),
        ),
        row(
            "M7.4",
            false,
            [
                sims(
                    "minimize",
                    &[
                        "tmin_keeps_the_property",
                        "a_missing_witness_is_rejected",
                        "changed_assumption_parameters_are_rejected",
                        "the_binary_confirms_in_a_fresh_process",
                    ],
                ),
                sims(
                    "triage_minimize",
                    &[
                        "minimizer_output_that_doesnt_hold_is_not_kept",
                        "verify_holds_a_minimized_bundle_to_its_failure",
                    ],
                ),
                vec![Expected::Minimize("header-sync/storage")],
            ]
            .concat(),
        ),
        row(
            "M7.5",
            false,
            [
                sims(
                    "explore",
                    &[
                        "empty_malformed_and_exhausted_inputs_end",
                        "aba_in_one_process",
                        "abandoned_run_retires_the_worker",
                        "a_panicking_job_retires_the_worker_and_resumes",
                        "classify_table",
                    ],
                ),
                vec![
                    sim("harness", "task_panic_violates_no_task_panic"),
                    sim("record_failure", "record_failure_writes_the_same_violation"),
                    sim("record_failure", "record_failure_survives_a_run_that_leaks_tasks"),
                    sim("triage", "buckets_for_harness_crash_and_nondeterminism"),
                ],
                [
                    "fuzz::adapter::a_property_failure_saves_its_input_and_trace_and_panics_with_its_id",
                    "fuzz::adapter::a_task_panic_becomes_no_task_panic",
                    "fuzz::adapter::a_harness_failure_panics_with_harness",
                    "fuzz::adapter::a_clean_run_returns",
                    "fuzz::adapter::unwind_task_panics_lets_a_task_panic_unwind",
                    "fuzz::adapter::repeated_inputs_leave_no_run_state",
                ]
                .map(Expected::FuzzTest)
                .to_vec(),
                vec![Expected::Target("header_sync_storage")],
            ]
            .concat(),
        ),
        row(
            "M7.6",
            false,
            [
                vec![sim("suite", "the_committed_suite_covers_the_registry")],
                sims(
                    "race_fixtures",
                    &[
                        "lost_update",
                        "snapshot_reserve",
                        "missed_notify",
                        "timer_cancel",
                        "ab_ba",
                        "abort_without_join",
                        "the_fixed_harness_draws_only_the_fix",
                    ],
                ),
                vec![
                    sim("targets", "every_declared_target_runs_seeds_that_apply_its_properties"),
                    sim("explore", "classify_table"),
                    sim("witness", "harness_end_fails"),
                    Expected::Lane("sim-release/zakura-sim"),
                ],
            ]
            .concat(),
        ),
        row(
            "corpus/honest-header-sync",
            false,
            vec![passing("header-sync/honest-progress")],
        ),
        row(
            "corpus/header-table-pairs",
            false,
            derived
                .table_properties
                .iter()
                .flat_map(|property| [passing(property), catch(property)])
                .collect(),
        ),
        row(
            "corpus/delayed-storage",
            false,
            [
                "header-sync/stale-completion-inert",
                "header-sync/reset-invalidates-work",
                "header-sync/stale-chain-effect-inert",
            ]
            .iter()
            .flat_map(|property| [passing(property), catch(property)])
            .collect(),
        ),
        row(
            "corpus/resource-release",
            false,
            [
                passing("header-sync/capacity-release-wakes"),
                catch("header-sync/capacity-release-wakes"),
                passing("header-sync/hold-release-wakes"),
                catch("header-sync/hold-release-wakes"),
                requirement("gate-5/mixed-witness"),
            ]
            .to_vec(),
        ),
        row(
            "corpus/discovery-lost-wakeup",
            true,
            [
                sims(
                    "witness",
                    &[
                        "discovery_wait_complete_lost_wakeup",
                        "discovery_wait_complete_check_inside_final_mark",
                        "violated_witness_fails_while_fixed",
                        "missed_trigger_history_fails",
                    ],
                ),
                vec![
                    Expected::Planted("discovery/wait-complete-wakes"),
                    Expected::Agreement("discovery/wait-complete-wakes".to_owned()),
                ],
            ]
            .concat(),
        ),
        row(
            "corpus/timer-only-waiting",
            false,
            sims(
                "time",
                &[
                    "advances_to_earliest_boundary",
                    "deadline_blocks_time",
                    "no_repeated_zero_progress_advance",
                ],
            ),
        ),
        row(
            "corpus/storage-released-deadlock",
            false,
            sims(
                "idle",
                &["storage_completion_releases_apparent_deadlock", "idle_outcomes_distinct"],
            ),
        ),
        row(
            "corpus/saved-plan-suite",
            true,
            derived
                .seeds
                .iter()
                .map(|file| Expected::Seed { file: file.clone() })
                .chain(derived.seed_witnesses.iter().cloned().map(Expected::SeedWitness))
                .chain(
                    derived
                        .end_records
                        .iter()
                        .map(|file| Expected::EndRecord { file: file.clone() }),
                )
                .chain(derived.race_schedules.iter().cloned().map(Expected::RaceSchedule))
                .collect(),
        ),
        row(
            "downstream",
            true,
            [
                "gate-2/despite-close/caught",
                "gate-2/despite-close/close-companion",
                "gate-2/despite-close/staging-companion",
                "gate-2/same-step-preservation",
                "gate-2/end-boundary-preservation",
            ]
            .map(requirement)
            .to_vec(),
        ),
    ];
    for (id, proofs) in SCHED_MAPPING {
        rows.push(row(
            id,
            true,
            proofs
                .iter()
                .map(|proof| match proof {
                    Proof::Test(test) => Expected::Test(*test),
                    Proof::Lane(lane) => Expected::Lane(lane),
                })
                .collect(),
        ));
    }
    rows.extend([
        row(
            "S4.1",
            true,
            sims(
                "draws",
                &[
                    "each_draw_reaches_plan_once",
                    "replayed_draws_reproduce_values",
                    "setup_draws_go_to_init",
                ],
            ),
        ),
        row(
            "S4.2",
            true,
            vec![
                sim("draws", "draw_inside_run_no_borrow_failure"),
                Expected::UnitTest {
                    package: "zakura-sim",
                    path: "sim::tests::held_borrow_is_harness_not_panic",
                },
                Expected::UnitTest {
                    package: "zakura-sim",
                    path: "sim::tests::callback_does_not_outlive_run",
                },
            ],
        ),
        row(
            "S4.3",
            true,
            [
                "executor::tests::every_rt_op_maps_by_name",
                "executor::tests::every_rt_draw_kind_maps",
                "executor::tests::ids_round_trip",
                "executor::tests::oversized_rt_task_id_panics",
            ]
            .map(|path| Expected::UnitTest {
                package: "zakura-scheduler",
                path,
            })
            .into_iter()
            .chain(
                ["clamped_timer_cancels", "second_joiner_gets_stand_in"].map(|name| {
                    Expected::Test(TestRef {
                        package: "zakura-scheduler",
                        file: "rt",
                        name,
                    })
                }),
            )
            .collect(),
        ),
        row(
            "S4.4",
            true,
            [
                sims(
                    "race_fixtures",
                    &[
                        "lost_update",
                        "snapshot_reserve",
                        "missed_notify",
                        "timer_cancel",
                        "ab_ba",
                        "abort_without_join",
                    ],
                ),
                vec![sim("race_schedules", "each_race_has_a_saved_schedule")],
                AGREEMENTS
                    .iter()
                    .filter(|name| name.starts_with("race/"))
                    .map(|name| Expected::Agreement((*name).to_owned()))
                    .collect(),
            ]
            .concat(),
        ),
        row(
            "S4.5",
            true,
            vec![
                sim(
                    "race_schedules",
                    "fresh_processes_reproduce_each_saved_violation",
                ),
                sim("race_schedules", "changed_code_replay_reports_divergence"),
                sim(
                    "cli",
                    "every_harness_replays_byte_for_byte_in_a_fresh_process",
                ),
            ],
        ),
        row(
            "S4.6",
            true,
            ["cargo-tree-excludes-simulator", "check-workspace-normal"]
                .map(|id| Expected::Command(id.to_owned()))
                .into_iter()
                .chain(
                    derived
                        .sim_packages
                        .iter()
                        .map(|name| Expected::Command(format!("check-sim-package/{name}"))),
                )
                .collect(),
        ),
        row(
            "S4.7",
            true,
            vec![
                Expected::Planted("discovery/wait-complete-wakes"),
                Expected::Agreement("discovery/wait-complete-wakes".to_owned()),
                sim("discovery", "holding_the_source_back_finds_no_violation"),
                sim("discovery", "seed_sweep_wakes"),
            ],
        ),
        row(
            "sched-4/release-and-asan",
            true,
            [
                "sim-release/zakura-rt-zakura-scheduler",
                "sim-asan/zakura-rt-zakura-scheduler",
                "sim-release/zakura-sim",
                "sim-asan/zakura-sim",
            ]
            .map(Expected::Lane)
            .to_vec(),
        ),
        row(
            "sim-7/self-checks",
            true,
            derived
                .self_tests
                .iter()
                .cloned()
                .map(Expected::SelfTest)
                .chain([
                    sim("suite", "the_zakura_sim_lanes_run_every_sched4_target"),
                    sim("suite", "a_lane_without_a_sched4_target_fails"),
                ])
                .chain(
                    [
                        "sim_acceptance::run::tests::a_test_row_needs_exactly_its_test",
                        "sim_acceptance::run::tests::a_lane_needs_every_required_target_and_no_failure",
                        "sim_acceptance::run::tests::the_dependency_graph_row_reads_cargo_tree_itself",
                    ]
                    .map(|path| Expected::UnitTest {
                        package: "zakura-sim",
                        path,
                    }),
                )
                .collect(),
        ),
        row(
            "sched-4/measurements",
            true,
            crate::harness::HARNESSES
                .iter()
                .map(|entry| Expected::Measure(entry.name))
                .collect(),
        ),
    ]);
    rows
}

/// The derived families at `repo`, from the suite manifest, the seed
/// report, the race-schedule fixtures and hdr-14's required keys.
pub fn derive(repo: &Path) -> Result<Derived, Vec<String>> {
    let manifest = crate::suite::manifest(repo).map_err(|problems| problems.0)?;
    let seeds: Vec<String> = manifest
        .seed_report
        .seeds
        .iter()
        .map(|row| row.file.display().to_string())
        .collect();
    let end_records = seeds
        .iter()
        .filter_map(|file| {
            let end = PathBuf::from(file).with_extension("end.json");
            repo.join(&end).is_file().then(|| end.display().to_string())
        })
        .collect();
    let schedules = repo.join("crates/zakura-sim/tests/fixtures/race-schedules");
    let race_schedules: BTreeSet<String> = std::fs::read_dir(&schedules)
        .map_err(|error| vec![format!("{}: {error}", schedules.display())])?
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            (path.extension()? == "bin").then(|| path.file_stem()?.to_str().map(str::to_owned))?
        })
        .collect();
    let table_properties = committed_required(repo, &Manifest::committed())
        .into_keys()
        .filter_map(|key| match key {
            Key::Passing { property } if property.starts_with("header-sync/s5v9/") => {
                Some(property)
            }
            _ => None,
        })
        .collect();
    let suite: crate::suite::SuiteFile = toml::from_str(
        &std::fs::read_to_string(repo.join(crate::suite::SUITE_FILE))
            .map_err(|error| vec![format!("{}: {error}", crate::suite::SUITE_FILE)])?,
    )
    .map_err(|error| vec![format!("{}: {error}", crate::suite::SUITE_FILE)])?;
    let self_tests = self_tests(
        &std::fs::read_to_string(repo.join("crates/zakura-sim/tests/sim_acceptance.rs"))
            .map_err(|error| vec![format!("tests/sim_acceptance.rs: {error}")])?,
    );
    Ok(Derived {
        sim_packages: suite
            .sim_package
            .iter()
            .map(|row| row.name.clone())
            .collect(),
        self_tests,
        seeds,
        seed_witnesses: manifest
            .seed_witnesses
            .iter()
            .map(|w| w.id.clone())
            .collect(),
        end_records,
        race_schedules: race_schedules.into_iter().collect(),
        table_properties,
    })
}

/// Every way `rows` don't match `derived` and the pinned ID lists: a row
/// ID given twice, a requirement or sched ID without its row, or a
/// derived family (seeds, seed witnesses, end records, race schedules,
/// generated header table IDs) that its row doesn't hold exactly. The
/// evaluator refuses a manifest with any, so a missing row can't leave the
/// rest verified.
pub fn problems(rows: &[Row], derived: &Derived) -> Vec<String> {
    let mut problems = Vec::new();
    let mut ids = BTreeSet::new();
    for row in rows {
        if !ids.insert(row.id.as_str()) {
            problems.push(format!("row {} is given twice", row.id));
        }
        if row.expected.is_empty() {
            problems.push(format!("row {} has no expected result", row.id));
        }
    }
    let required = ["M7.1", "M7.2", "M7.3", "M7.4", "M7.5", "M7.6", "downstream"]
        .into_iter()
        .chain(SCHED_IDS.iter().copied())
        .chain(["S4.1", "S4.2", "S4.3", "S4.4", "S4.5", "S4.6", "S4.7"]);
    for id in required {
        if !ids.contains(id) {
            problems.push(format!("requirement {id} has no row"));
        }
    }
    let held = |id: &str| -> BTreeSet<String> {
        rows.iter()
            .find(|row| row.id == id)
            .map(|row| row.expected.iter().map(ToString::to_string).collect())
            .unwrap_or_default()
    };
    let saved: BTreeSet<String> = derived
        .seeds
        .iter()
        .map(|file| Expected::Seed { file: file.clone() })
        .chain(
            derived
                .seed_witnesses
                .iter()
                .cloned()
                .map(Expected::SeedWitness),
        )
        .chain(
            derived
                .end_records
                .iter()
                .map(|file| Expected::EndRecord { file: file.clone() }),
        )
        .chain(
            derived
                .race_schedules
                .iter()
                .cloned()
                .map(Expected::RaceSchedule),
        )
        .map(|expected| expected.to_string())
        .collect();
    if held("corpus/saved-plan-suite") != saved {
        problems.push("the saved-plan row doesn't hold exactly the derived seed set".to_owned());
    }
    let pairs: BTreeSet<String> = derived
        .table_properties
        .iter()
        .flat_map(|property| [passing(property), catch(property)])
        .map(|expected| expected.to_string())
        .collect();
    let selves: BTreeSet<String> = derived
        .self_tests
        .iter()
        .map(|path| Expected::SelfTest(path.clone()).to_string())
        .collect();
    if !selves.is_subset(&held("sim-7/self-checks")) || selves.is_empty() {
        problems.push("the self-check row doesn't hold every sim-7 static check".to_owned());
    }
    if held("corpus/header-table-pairs") != pairs {
        problems.push("the header table row doesn't hold exactly the generated IDs".to_owned());
    }
    problems
}

/// Every `#[test]` function in `text`, a test file, by its path in the
/// test binary: `name`, or `module::name` inside a top-level `mod module {`.
fn self_tests(text: &str) -> Vec<String> {
    let mut tests = Vec::new();
    let mut module: Option<String> = None;
    let mut is_test = false;
    for line in text.lines() {
        if let Some(name) = line
            .strip_prefix("mod ")
            .and_then(|rest| rest.strip_suffix(" {"))
        {
            module = Some(name.to_owned());
        } else if line == "}" {
            module = None;
        }
        let trimmed = line.trim();
        if trimmed == "#[test]" {
            is_test = true;
        } else if is_test {
            if let Some(name) = trimmed
                .strip_prefix("fn ")
                .and_then(|rest| rest.split('(').next())
            {
                tests.push(match &module {
                    Some(module) => format!("{module}::{name}"),
                    None => name.to_owned(),
                });
                is_test = false;
            }
        }
    }
    tests
}
