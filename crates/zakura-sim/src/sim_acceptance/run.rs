//! What `cargo xtask sim-acceptance` runs, and how its artifacts become the
//! sim-7 record (the sim-7 plan, "sim-7.4's tool"). The xtask only runs the
//! plan's tasks and keeps their raw output under `<artifacts>/<index>/`.
//! Every judgement is made here, from those files, so the tool's runner
//! never decides a verdict.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    deviations::DEVIATIONS,
    evaluate::{evaluate, header_refusal, Evaluation, HeaderRecordFacts, RowSpec, Verdict},
    header::{self, consumed_cleanup, verify, Published},
    judge::{owned_run_passes, Registered, RunExpect},
    manifest::{
        derive, problems, rows, Expected, DISCOVERY_CASE, DISCOVERY_SITE, LANES, RACE_SCHEDULES,
    },
};
use crate::{
    check::PropertyOutcome,
    harness::header_acceptance::tool::{Containment, Record, RunPlan},
    property::Status,
    trace::TraceReader,
};

/// One thing the xtask runs. Its artifacts go under the task's index.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum Task {
    /// One test by its exact name, through hdr-14's test runner, which
    /// writes `test.json`. A unit test's `file` is its library's `src/lib.rs`.
    Test {
        /// The expected result it produces.
        expected: String,
        /// The package.
        package: String,
        /// The test's file, relative to the package.
        file: String,
        /// The test's name, or a unit test's path.
        test: String,
    },
    /// One test in the `qa/fuzz/sim` workspace's library, with
    /// `--no-default-features`, which writes `test.json`.
    FuzzTest {
        /// The expected result it produces.
        expected: String,
        /// The test's path.
        test: String,
    },
    /// One `qa/suite.toml` lane's command, which writes `lane.json`.
    Lane {
        /// The expected result it produces.
        expected: String,
        /// The lane's ID.
        id: String,
        /// The lane's whole command line, as `qa/suite.toml`'s build
        /// prefixes it.
        command: String,
        /// The test targets it must run, as `<package>/<target>`, with
        /// `lib` for a library's unit tests.
        required: Vec<String>,
        /// The targets it selects that must list no test, as a `sim-package`
        /// row's `lib = false` says.
        empty: Vec<String>,
        /// The ignored tests `qa/suite.toml` marks `run = true` in its
        /// targets, as `<package>/<target>::<test>`, each of which must run.
        ignored_run: Vec<String>,
    },
    /// One committed seed's full input recorded in the release sim build,
    /// then replayed fresh, which writes `trace/` and `replay.json`.
    Seed {
        /// The expected result it produces.
        expected: String,
        /// The seed's file, relative to the repository's root, which its
        /// committed end record, if any, sits beside.
        file: String,
        /// The seed's harness.
        harness: String,
        /// Its full input's SHA-256, its file name in the run's corpus.
        input: String,
    },
    /// One seed witness recorded with `--witness`, then its trace replayed
    /// fresh, which writes `trace/` with its `witness.json`, and
    /// `replay.json`.
    SeedWitness {
        /// The expected result it produces.
        expected: String,
        /// The seed's harness.
        harness: String,
        /// Its full input's SHA-256.
        input: String,
        /// The witnessed property.
        property: String,
        /// The expectation, as the sidecar spells it.
        expect: String,
    },
    /// One saved race schedule (S4.4) recorded with its `violated`
    /// witness, then its trace replayed fresh, as a seed witness is.
    RaceSchedule {
        /// The expected result it produces.
        expected: String,
        /// The race fixture's harness.
        harness: String,
        /// Its full input's SHA-256, its file name in the run's corpus.
        input: String,
        /// The witnessed property.
        property: String,
        /// The expectation, as the sidecar spells it.
        expect: String,
    },
    /// One race fixture's schedule-agreement pair (S4.4): its saved
    /// schedule recorded as its violation's witness into `base/`, and the
    /// same Plan bytes on the fix as a `holds` witness into `fixed/`, each
    /// replayed fresh with `--observe` (`replay-base.json`,
    /// `replay-fixed.json`).
    Agreement {
        /// The expected result it produces.
        expected: String,
        /// The race fixture's harness, which is also its property.
        harness: String,
        /// The saved schedule's full input's SHA-256, its corpus file name.
        base_input: String,
        /// The schedule's Plan bytes, after its scenario prefix, in hex.
        plan: String,
        /// Their SHA-256, which the fixed trace's meta must name.
        plan_sha256: String,
        /// The fixed scenario, as JSON.
        fixed_scenario: String,
        /// The fixture's declared site, [`crate::harness::fixtures::Fixture::AGREEMENT_SITE`].
        site: String,
    },
    /// The discovery planted case (S4.7): `cargo xtask planted` for its
    /// verdict (`planted.json`), then S with the case's patch applied, built
    /// in its own directory and kept by SHA-256 (`patched.json`), and the
    /// case's committed input recorded by S's release build as a `holds`
    /// witness (`baseline/`) and by the patched build as a `violated`
    /// witness (`patched/`), each replayed fresh with `--observe` by the
    /// build that recorded it. The discovery agreement pair is judged from
    /// these runs.
    Planted {
        /// The expected result it produces.
        expected: String,
        /// The planted property.
        property: String,
        /// The case's harness.
        harness: String,
        /// The committed input, relative to the repository's root.
        input: String,
        /// Its SHA-256.
        input_sha256: String,
        /// The case's patch, relative to the repository's root.
        patch: String,
    },
    /// One simulator fuzz target's readiness (M7.5): the run's own
    /// `zakura-sim targets` on it, built in the run's directory, with each
    /// fuzz-target run's begin and end records under `runs/` and the report
    /// under `targets/`.
    Target {
        /// The expected result it produces.
        expected: String,
        /// The target's `[[bin]]` name.
        name: String,
        /// The harness `qa/suite.toml` maps it to.
        harness: String,
        /// The full-input SHA-256 of each admitted seed of that harness: the
        /// runs the target must make, each at least once.
        seeds: Vec<String>,
    },
    /// The header minimization (M7.4): `cargo xtask sim-minimize` on a
    /// `header-sync/storage` planted case's committed input through its
    /// fuzz target, in S with the case's patch, writing `minimize/` and
    /// every run's records under `runs/`.
    Minimize {
        /// The expected result it produces.
        expected: String,
        /// The case's property.
        property: String,
        /// The case's harness.
        harness: String,
        /// The fuzz target that runs it.
        target: String,
        /// The committed input, relative to the repository's root.
        input: String,
        /// Its SHA-256.
        input_sha256: String,
        /// The case's patch, relative to the repository's root.
        patch: String,
    },
    /// One S4.6 command, which writes `command.json`.
    Command {
        /// The expected result it produces.
        expected: String,
        /// Its arguments after `cargo`.
        args: Vec<String>,
        /// Whether it runs in the simulator build, `--cfg zakura_sim`.
        sim: bool,
    },
    /// One measured recording, which writes `trace/` and `measure.json`.
    Measure {
        /// The expected result it produces.
        expected: String,
        /// The harness.
        harness: String,
        /// The full input's SHA-256 in the run's corpus: the harness's
        /// first committed seed, else its saved race schedule, or `None` for
        /// empty input when it has neither.
        input: Option<String>,
        /// The property the measured run must violate, for a race fixture
        /// measured on its saved schedule, which fails by design. Any other
        /// measured run must pass.
        violates: Option<String>,
    },
}

impl Task {
    /// The expected result this task produces.
    pub fn expected(&self) -> &str {
        match self {
            Task::Test { expected, .. }
            | Task::FuzzTest { expected, .. }
            | Task::Lane { expected, .. }
            | Task::Seed { expected, .. }
            | Task::SeedWitness { expected, .. }
            | Task::RaceSchedule { expected, .. }
            | Task::Agreement { expected, .. }
            | Task::Planted { expected, .. }
            | Task::Target { expected, .. }
            | Task::Minimize { expected, .. }
            | Task::Command { expected, .. }
            | Task::Measure { expected, .. } => expected,
        }
    }
}

/// What the tool runs, and what it can't yet, with the manifest's
/// problems.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sim7Plan {
    /// The tasks, in order. A task's artifacts go under its index.
    pub tasks: Vec<Task>,
    /// Expected results with no task yet, and why. Each blocks.
    pub unsupported: Vec<(String, String)>,
    /// Every way the manifest doesn't match the code at S. Any refuses the
    /// record.
    pub problems: Vec<String>,
}

/// An S4.6 command, by ID, as `cargo` arguments: the dependency graph, the
/// normal workspace check, or one `sim-package` row's check in the
/// simulator build, selecting exactly what `qa/suite.toml` names for it.
pub fn command_args(repo: &Path, id: &str) -> Option<Vec<String>> {
    if let Some(name) = id.strip_prefix("check-sim-package/") {
        let text = fs::read_to_string(repo.join(crate::suite::SUITE_FILE)).ok()?;
        let suite: crate::suite::SuiteFile = toml::from_str(&text).ok()?;
        let row = suite.sim_package.iter().find(|row| row.name == name)?;
        let mut args = vec![
            "check".to_owned(),
            "--locked".to_owned(),
            "-p".to_owned(),
            name.to_owned(),
        ];
        if row.lib {
            args.push("--lib".to_owned());
        }
        match &row.tests {
            Some(tests) => {
                for test in tests {
                    args.extend(["--test".to_owned(), test.clone()]);
                }
            }
            None => args.push("--tests".to_owned()),
        }
        return Some(args);
    }
    let args: &[&str] = match id {
        // As the lint step reads it: the node's package is `zakura`.
        "cargo-tree-excludes-simulator" => &[
            "tree",
            "--locked",
            "-p",
            "zakura",
            "-e",
            "normal,build",
            "--target",
            "all",
            "--prefix",
            "none",
        ],
        "check-workspace-normal" => &["check", "--workspace", "--locked"],
        _ => return None,
    };
    Some(args.iter().map(|arg| (*arg).to_owned()).collect())
}

/// The plan at `repo`: one task per expected result the tool can run, in
/// manifest order, each listed once even when several rows share it.
pub fn plan(repo: &Path) -> Sim7Plan {
    let derived = match derive(repo) {
        Ok(derived) => derived,
        Err(problems) => {
            return Sim7Plan {
                problems,
                ..Sim7Plan::default()
            }
        }
    };
    let manifest = crate::suite::manifest(repo).ok();
    let seed_inputs: BTreeMap<String, (String, String)> = manifest
        .as_ref()
        .map(|m| {
            m.seed_report
                .seeds
                .iter()
                .filter_map(|row| {
                    Some((
                        row.file.display().to_string(),
                        (
                            row.sidecar.as_ref()?.harness.clone(),
                            row.input_sha256.clone()?,
                        ),
                    ))
                })
                .collect()
        })
        .unwrap_or_default();
    // Each saved race schedule's harness, full input and one witness.
    let race_schedules: BTreeMap<String, (String, String, String, String)> =
        crate::seed::load(&[repo.join(RACE_SCHEDULES)], None)
            .map(|report| {
                report
                    .admitted
                    .iter()
                    .filter_map(|seed| {
                        let name = seed.path.file_stem()?.to_str()?.to_owned();
                        let [witness] = &seed.sidecar.witness[..] else {
                            return None;
                        };
                        Some((
                            name,
                            (
                                seed.sidecar.harness.clone(),
                                seed.input_sha256(),
                                witness.property.clone(),
                                witness.expect.clone(),
                            ),
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default();
    // Each race fixture's agreement pair: its saved schedule's Plan bytes,
    // and the fixed scenario they also run on.
    let race_pairs: BTreeMap<String, Task> = crate::seed::load(&[repo.join(RACE_SCHEDULES)], None)
        .map(|report| {
            report
                .admitted
                .iter()
                .filter_map(|seed| {
                    use crate::harness::fixtures::{Scenario, Variant, Which};
                    let harness = seed.sidecar.harness.clone();
                    let which = Which::ALL.into_iter().find(|w| w.property() == harness)?;
                    let entry = crate::harness::find(&harness).ok()?;
                    let plan = seed.input.get(entry.scenario_bytes..)?;
                    let fixed = serde_json::to_string(&Scenario {
                        variant: Variant::Fixed,
                        workers: 2,
                    })
                    .ok()?;
                    Some((
                        harness.clone(),
                        Task::Agreement {
                            expected: Expected::Agreement(harness.clone()).to_string(),
                            harness,
                            base_input: seed.input_sha256(),
                            plan: plan.iter().map(|byte| format!("{byte:02x}")).collect(),
                            plan_sha256: crate::seed::sha256(plan),
                            fixed_scenario: fixed,
                            site: which.agreement_site().to_owned(),
                        },
                    ))
                })
                .collect()
        })
        .unwrap_or_default();
    let mut plan = Sim7Plan {
        problems: problems(&rows(&derived), &derived),
        ..Sim7Plan::default()
    };
    let mut seen = BTreeSet::new();
    for row in rows(&derived) {
        for expected in row.expected {
            let id = expected.to_string();
            if !seen.insert(id.clone()) {
                continue;
            }
            let task = match &expected {
                Expected::Test(t) => Some(Task::Test {
                    expected: id.clone(),
                    package: t.package.to_owned(),
                    file: format!("tests/{}.rs", t.file),
                    test: t.name.to_owned(),
                }),
                Expected::UnitTest { package, path } => Some(Task::Test {
                    expected: id.clone(),
                    package: (*package).to_owned(),
                    file: "src/lib.rs".to_owned(),
                    test: (*path).to_owned(),
                }),
                Expected::FuzzTest(test) => Some(Task::FuzzTest {
                    expected: id.clone(),
                    test: (*test).to_owned(),
                }),
                Expected::Lane(lane) => lane_task(repo, &id, lane),
                Expected::Seed { file } => {
                    seed_inputs.get(file).map(|(harness, input)| Task::Seed {
                        expected: id.clone(),
                        file: file.clone(),
                        harness: harness.clone(),
                        input: input.clone(),
                    })
                }
                Expected::SeedWitness(witness) => manifest.as_ref().and_then(|m| {
                    let row = m.seed_witnesses.iter().find(|w| &w.id == witness)?;
                    let (_, input) = seed_inputs.get(&row.seed)?;
                    Some(Task::SeedWitness {
                        expected: id.clone(),
                        harness: row.harness.clone(),
                        input: input.clone(),
                        property: row.property.clone(),
                        expect: row.witness.clone(),
                    })
                }),
                Expected::RaceSchedule(name) => {
                    race_schedules
                        .get(name)
                        .map(|(harness, input, property, expect)| Task::RaceSchedule {
                            expected: id.clone(),
                            harness: harness.clone(),
                            input: input.clone(),
                            property: property.clone(),
                            expect: expect.clone(),
                        })
                }
                Expected::Command(command) => {
                    command_args(repo, command).map(|args| Task::Command {
                        expected: id.clone(),
                        args,
                        sim: command.starts_with("check-sim-package/"),
                    })
                }
                Expected::SelfTest(path) => Some(Task::Test {
                    expected: id.clone(),
                    package: "zakura-sim".to_owned(),
                    file: "tests/sim_acceptance.rs".to_owned(),
                    test: path.clone(),
                }),
                Expected::Measure(harness) => {
                    let seed = seed_inputs
                        .values()
                        .find(|(seed_harness, _)| seed_harness == harness)
                        .map(|(_, input)| input.clone());
                    // A race fixture has no passing seed: its measured run is
                    // its saved schedule, with its declared violation.
                    let race = race_schedules
                        .values()
                        .find(|(race_harness, ..)| race_harness == harness);
                    Some(match (seed, race) {
                        (Some(input), _) => Task::Measure {
                            expected: id.clone(),
                            harness: (*harness).to_owned(),
                            input: Some(input),
                            violates: None,
                        },
                        (None, Some((_, input, property, _))) => Task::Measure {
                            expected: id.clone(),
                            harness: (*harness).to_owned(),
                            input: Some(input.clone()),
                            violates: Some(property.clone()),
                        },
                        (None, None) => Task::Measure {
                            expected: id.clone(),
                            harness: (*harness).to_owned(),
                            input: None,
                            violates: None,
                        },
                    })
                }
                // Judged from the header record, from seed tasks, and the
                // discovery pair from the planted case's runs.
                Expected::HeaderKey(_) | Expected::EndRecord { .. } => None,
                Expected::Agreement(name) if name == DISCOVERY_CASE.0 => None,
                Expected::Agreement(name) if race_pairs.contains_key(name) => {
                    race_pairs.get(name).cloned()
                }
                // The planted runner's verdict alone isn't evidence (Rule 11),
                // so its baseline and patched runs are recorded and judged.
                Expected::Planted(property) if *property == DISCOVERY_CASE.0 => {
                    planted_task(repo, &id)
                }
                Expected::Target(name) => manifest.as_ref().and_then(|m| {
                    let harness = m
                        .targets
                        .iter()
                        .find(|row| row.name == *name)?
                        .harness
                        .clone()?;
                    Some(Task::Target {
                        expected: id.clone(),
                        name: (*name).to_owned(),
                        seeds: seed_inputs
                            .values()
                            .filter(|(seed_harness, _)| *seed_harness == harness)
                            .map(|(_, input)| input.clone())
                            .collect(),
                        harness,
                    })
                }),
                Expected::Minimize(harness) => manifest.as_ref().and_then(|m| {
                    let target = m
                        .targets
                        .iter()
                        .find(|row| row.harness.as_deref() == Some(*harness))?
                        .name
                        .clone();
                    minimize_task(repo, &id, harness, target)
                }),
                Expected::Planted(_) | Expected::Agreement(_) => {
                    plan.unsupported
                        .push((id.clone(), "the tool can't run this result yet".to_owned()));
                    None
                }
            };
            match task {
                Some(task) => plan.tasks.push(task),
                None if matches!(
                    expected,
                    Expected::HeaderKey(_)
                        | Expected::EndRecord { .. }
                        | Expected::Planted(_)
                        | Expected::Target(_)
                        | Expected::Agreement(_)
                ) => {}
                None => plan
                    .unsupported
                    .push((id, "no task could be built for it at S".to_owned())),
            }
        }
    }
    plan
}

/// The minimization's task: the first header-sync planted case, in path
/// order under `qa/planted/header-sync/` at `repo`, whose committed input
/// runs on `harness` (the plan's "one `header-sync/storage` planted
/// catch").
fn minimize_task(repo: &Path, expected: &str, harness: &str, target: String) -> Option<Task> {
    let root = repo.join("qa/planted");
    let mut cases = Vec::new();
    let mut pending = vec![root.join("header-sync")];
    while let Some(dir) = pending.pop() {
        for entry in fs::read_dir(&dir).ok()?.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|ext| ext == "toml") {
                cases.push(path);
            }
        }
    }
    cases.sort();
    cases.into_iter().find_map(|case| {
        let toml: toml::Value = toml::from_str(&fs::read_to_string(&case).ok()?).ok()?;
        let input = toml.get("input")?.as_str()?.to_owned();
        let sidecar: toml::Value = toml::from_str(
            &fs::read_to_string(repo.join(Path::new(&input).with_extension("toml"))).ok()?,
        )
        .ok()?;
        if sidecar.get("harness")?.as_str()? != harness {
            return None;
        }
        let property = case
            .parent()?
            .strip_prefix(&root)
            .ok()?
            .to_str()?
            .to_owned();
        let patch = case.with_extension("patch");
        Some(Task::Minimize {
            expected: expected.to_owned(),
            property,
            harness: harness.to_owned(),
            target: target.clone(),
            input_sha256: crate::seed::sha256(&fs::read(repo.join(&input)).ok()?),
            input,
            patch: patch.strip_prefix(repo).ok()?.to_str()?.to_owned(),
        })
    })
}

/// The discovery planted case's task, from its `.toml` at `repo`: its
/// committed input and the harness that input's sidecar names.
fn planted_task(repo: &Path, expected: &str) -> Option<Task> {
    let (property, case) = DISCOVERY_CASE;
    let dir = format!("qa/planted/{property}");
    let toml: toml::Value =
        toml::from_str(&fs::read_to_string(repo.join(format!("{dir}/{case}.toml"))).ok()?).ok()?;
    let input = toml.get("input")?.as_str()?.to_owned();
    let sidecar: toml::Value = toml::from_str(
        &fs::read_to_string(repo.join(Path::new(&input).with_extension("toml"))).ok()?,
    )
    .ok()?;
    Some(Task::Planted {
        expected: expected.to_owned(),
        property: property.to_owned(),
        harness: sidecar.get("harness")?.as_str()?.to_owned(),
        input_sha256: crate::seed::sha256(&fs::read(repo.join(&input)).ok()?),
        input,
        patch: format!("{dir}/{case}.patch"),
    })
}

/// `lane`'s task, its command from `qa/suite.toml` at `repo` and its
/// required targets from [`LANES`], never from what the command happens to
/// select: each package's library, and the lane's named targets or else
/// every top-level `tests/*.rs` of each package. A library a `sim-package`
/// row marks `lib = false` is expected empty, so it isn't required
/// (Rule 11).
fn lane_task(repo: &Path, expected: &str, lane: &str) -> Option<Task> {
    let obligation = LANES.iter().find(|obligation| obligation.id == lane)?;
    let text = fs::read_to_string(repo.join(crate::suite::SUITE_FILE)).ok()?;
    let suite: crate::suite::SuiteFile = toml::from_str(&text).ok()?;
    let row = suite.lane.iter().find(|row| row.id == lane)?;
    let (subcommand, args) = row.args.split_once(' ').unwrap_or((&row.args, ""));
    let packages = obligation.packages;
    let named = obligation.targets.unwrap_or_default();
    let mut required = Vec::new();
    let mut empty = Vec::new();
    for package in packages {
        let empty_lib = suite
            .sim_package
            .iter()
            .any(|p| p.name == *package && !p.lib);
        if empty_lib {
            empty.push(format!("{package}/lib"));
        } else {
            required.push(format!("{package}/lib"));
        }
        if named.is_empty() {
            let tests = repo.join("crates").join(package).join("tests");
            let mut stems: Vec<String> = fs::read_dir(&tests)
                .into_iter()
                .flatten()
                .filter_map(|entry| {
                    let path = entry.ok()?.path();
                    // Cargo's integration targets: `tests/<name>.rs`, and
                    // `tests/<name>/main.rs`.
                    let target = if path.is_dir() {
                        path.join("main.rs").is_file().then_some(path.as_path())?
                    } else {
                        (path.extension()? == "rs").then_some(path.as_path())?
                    };
                    target.file_stem()?.to_str().map(str::to_owned)
                })
                .collect();
            stems.sort();
            required.extend(stems.into_iter().map(|stem| format!("{package}/{stem}")));
        }
    }
    if let [package] = packages {
        required.extend(named.iter().map(|target| format!("{package}/{target}")));
    }
    // `run = true` ignored tests in the lane's targets must run too.
    let ignored_run = suite
        .ignored
        .iter()
        .filter(|row| row.run)
        .filter_map(|row| {
            let path = Path::new(&row.file);
            let package = path.iter().nth(1)?.to_str()?;
            let stem = path.file_stem()?.to_str()?;
            let target = format!("{package}/{stem}");
            required
                .contains(&target)
                .then(|| format!("{target}::{}", row.test))
        })
        .collect();
    Some(Task::Lane {
        expected: expected.to_owned(),
        id: lane.to_owned(),
        command: row.build.command(subcommand, args),
        required,
        empty,
        ignored_run,
    })
}

/// The sim-7 record.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sim7Record {
    /// The artifacts directory, relative to the run directory, as the
    /// shared runner's record publication expects.
    pub artifacts: String,
    /// The subject commit S.
    pub subject: String,
    /// S's source digest.
    pub subject_source: String,
    /// How the run contained its children: `subreaper` or `best-effort`.
    /// Only a subreaper run is an acceptance record.
    pub containment: String,
    /// Why hdr-14's record can't be consumed, if it can't.
    pub header_refusal: Option<String>,
    /// The manifest's problems at S.
    pub problems: Vec<String>,
    /// Expected results the tool doesn't run yet.
    pub unsupported: Vec<(String, String)>,
    /// Every row's verdict.
    pub evaluation: Evaluation,
    /// Each task's expected result with its artifact directory, relative
    /// to the run directory, and the binaries that produced it.
    pub evidence: BTreeMap<String, Evidence>,
    /// Each measured recording's `measure.json`, by expected result.
    pub measurements: BTreeMap<String, Value>,
    /// The SHA-256 of hdr-14's `record.json` this record consumed.
    pub header_record_sha256: Option<String>,
    /// Every narrowing of the spec, with its quote, claim and approval,
    /// and whether it's signed off.
    pub deviations: Vec<DeviationEntry>,
    /// No row blocked, no manifest problem, the plan S's own, the run
    /// contained under a subreaper, and the header record consumed.
    pub accepted: bool,
    /// Accepted, nothing excluded, and every deviation signed off.
    pub fully_accepted: bool,
}

/// Where one expected result's evidence is.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Evidence {
    /// Where it is: a task's artifact directory, relative to the archive
    /// root (the published record's directory), or for a consumed header key each artifact its header
    /// record entry links, as `header:<path>` relative to the header run's
    /// `--out`. A header key's binaries and property outcomes are hdr-14's,
    /// in that record, which [`Sim7Record::header_record_sha256`] pins.
    pub artifacts: Vec<String>,
    /// The SHA-256 of each binary that produced it: the trace's recording
    /// executable, the test's binary, or each lane target's test binary.
    pub binaries: Vec<String>,
    /// The property applications its passing run credited.
    pub applicable: u64,
    /// Each registered property's outcome in its trace's end record, for a
    /// result with a trace.
    pub properties: BTreeMap<String, Value>,
}

/// One [`super::deviations::Deviation`] as the record exports it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviationEntry {
    /// Its ID.
    pub id: String,
    /// The spec text it narrows.
    pub spec: String,
    /// The narrower claim.
    pub claim: String,
    /// Who approved it, and when.
    pub approval: Option<String>,
    /// Evan's sign-off state.
    pub evan: String,
    /// Whether it's signed off.
    pub signed_off: bool,
}

/// The directory under the artifacts where the xtask keeps the release
/// seed inventory: `inventory.json` with the child's outcome and
/// `report.json` with what it reported.
pub const SEED_INVENTORY: &str = "seeds";

/// The directory under the artifacts where the xtask keeps the release
/// inventory of the saved race schedules, as [`SEED_INVENTORY`] does the
/// committed seeds'.
pub const RACE_INVENTORY: &str = "race-schedules";

/// The SHA-256 of empty input, which a measure with no committed seed runs.
const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// Why the release seed inventory under `inventory` doesn't admit `file`
/// with full input `input` for `harness`, or `None` if it does. With no
/// `file`, any admitted seed of `harness` with that input counts, since
/// race fixtures can share bytes. The inventory must also have exited 0.
fn seed_inventory_refusal(
    artifacts: &Path,
    inventory: &str,
    bound: &Bound<'_>,
    harness: &str,
    file: Option<&str>,
    input: &str,
) -> Option<String> {
    let dir = artifacts.join(inventory);
    let read = |name: &str| -> Option<Value> {
        serde_json::from_str(&fs::read_to_string(dir.join(name)).ok()?).ok()
    };
    let outcome = read("inventory.json");
    if !exited(outcome.clone()) {
        return Some("the release `zakura-sim seeds --report` didn't exit 0".to_owned());
    }
    // The outcome must be this run's inventory, by the run's release
    // executable, of the report beside it.
    let run = artifacts.parent().and_then(Path::file_name);
    let names = outcome.as_ref().and_then(|o| o.get("binding"));
    let field = |name: &str| names.and_then(|n| n.get(name)).and_then(Value::as_str);
    let report_sha = fs::read(dir.join("report.json"))
        .ok()
        .map(|bytes| sha256_hex(&bytes));
    if run.and_then(|run| run.to_str()) != field("run")
        || field("inventory") != Some(inventory)
        || bound.release.is_none()
        || field("executable_sha256") != bound.release.as_deref()
        || report_sha.is_none()
        || field("report_sha256") != report_sha.as_deref()
    {
        return Some(format!(
            "the {inventory} inventory isn't bound to this run's report"
        ));
    }
    let report = read("report.json").unwrap_or_default();
    let admitted = report
        .get("seeds")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|row| row.get("status").and_then(Value::as_str) == Some("admitted"))
        .any(|row| {
            row.get("input_sha256").and_then(Value::as_str) == Some(input)
                && row.pointer("/sidecar/harness").and_then(Value::as_str) == Some(harness)
                && file.is_none_or(|file| row.get("file").and_then(Value::as_str) == Some(file))
        });
    (!admitted).then(|| format!("the release seed inventory doesn't admit input {input}"))
}

/// `bytes`' SHA-256 in lowercase hex.
fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest as _, Sha256};
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// The JSON in the regular file at `path`, read without following a link
/// and within the artifact limit, if it reads and parses.
fn bounded_json(path: &Path) -> Option<Value> {
    header::read_bounded_json(path).ok()
}

/// Each registered property's outcome in the end record of the trace a
/// trace-backed task left in `dir`, as [`Evidence::properties`].
fn trace_properties(task: &Task, dir: &Path) -> BTreeMap<String, Value> {
    if !matches!(
        task,
        Task::Seed { .. }
            | Task::SeedWitness { .. }
            | Task::RaceSchedule { .. }
            | Task::Measure { .. }
    ) {
        return BTreeMap::new();
    }
    TraceReader::open(&dir.join("trace"))
        .ok()
        .and_then(|trace| trace.end)
        .map(|end| {
            end.properties
                .iter()
                .filter_map(|report| {
                    Some((
                        report.property.clone(),
                        serde_json::to_value(&report.outcome).ok()?,
                    ))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// What every file the runner writes for a task must name, under its
/// `binding`, to count for that task: the run, the task's index and its
/// `task.json`'s SHA-256. Without it, one task's file could stand in for
/// another's.
struct TaskBinding {
    /// The run's ID, its run directory's name, so a file from another run
    /// of the same plan doesn't count.
    run: String,
    /// The task's index in the plan.
    index: usize,
    /// The SHA-256 of the task's `task.json`.
    task_sha256: String,
}

/// The runner-written JSON file `name` in `dir`, if its `binding` names
/// the task `binding` describes. A file the simulator writes itself, such
/// as `measure.json`, carries no binding and is read directly.
fn runner_json(dir: &Path, name: &str, binding: &TaskBinding) -> Option<Value> {
    let value: Value = serde_json::from_str(&fs::read_to_string(dir.join(name)).ok()?).ok()?;
    let names = value.get("binding")?;
    let same = names.get("run").and_then(Value::as_str) == Some(&binding.run)
        && names.get("task").and_then(Value::as_u64) == u64::try_from(binding.index).ok()
        && names.get("task_sha256").and_then(Value::as_str) == Some(&binding.task_sha256);
    same.then_some(value)
}

/// Why the task's fresh replay of its trace in `trace` doesn't count, if it
/// doesn't: `replay`, such as `replay.json`, must be this task's, have
/// exited 0, and name the SHA-256 of the task's own `<trace>/trace.jsonl`
/// and the run's release executable. `zakura-sim
/// replay` exits 0 only when its run's complete end record and
/// observations equal the recording's, so the recording's end record,
/// judged by [`owned_run_passes`], is the replay's too.
fn replay_refusal(
    dir: &Path,
    (trace_dir, replay_file): (&str, &str),
    binding: &TaskBinding,
    bound: &Bound<'_>,
) -> Option<String> {
    let Some(replay) = runner_json(dir, replay_file, binding) else {
        return Some(format!("the task has no {replay_file} of its own"));
    };
    if !exited(Some(replay.clone())) {
        return Some("the trace didn't replay exactly in a fresh process".to_owned());
    }
    let trace = crate::trace::read::read_bounded(
        &dir.join(trace_dir).join(crate::trace::TRACE_FILE),
        header::MAX_ARTIFACT_BYTES,
    )
    .ok()
    .flatten()
    .map(|bytes| sha256_hex(&bytes));
    let names = replay.get("binding");
    let field = |name: &str| names.and_then(|n| n.get(name)).and_then(Value::as_str);
    if trace.is_none() || field("trace_sha256") != trace.as_deref() {
        return Some(format!("{replay_file} didn't replay this task's trace"));
    }
    if bound.release.is_none() || field("executable_sha256") != bound.release.as_deref() {
        return Some(format!(
            "{replay_file} wasn't replayed by the run's release zakura-sim"
        ));
    }
    None
}

/// The binaries a task's artifacts in `dir` name, as [`Evidence::binaries`].
fn task_binaries(task: &Task, dir: &Path, binding: &TaskBinding) -> Vec<String> {
    let json = |name: &str| runner_json(dir, name, binding);
    match task {
        // hdr-14's runner owns `test.json`, so the xtask keeps a test
        // row's digest beside it.
        Task::Test { .. } => json("binary.json")
            .and_then(|binary| binary.get("sha256")?.as_str().map(str::to_owned))
            .into_iter()
            .collect(),
        Task::FuzzTest { .. } => json("test.json")
            .and_then(|test| test.get("binary")?.as_str().map(str::to_owned))
            .into_iter()
            .collect(),
        Task::Lane { .. } => json("lane.json")
            .and_then(|lane| {
                serde_json::from_value::<BTreeMap<String, String>>(lane.get("binaries")?.clone())
                    .ok()
            })
            .map(|binaries| {
                binaries
                    .into_values()
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect()
            })
            .unwrap_or_default(),
        Task::Seed { .. }
        | Task::SeedWitness { .. }
        | Task::RaceSchedule { .. }
        | Task::Measure { .. } => TraceReader::open(&dir.join("trace"))
            .ok()
            .map(|trace| trace.meta.build.binary)
            .into_iter()
            .collect(),
        Task::Agreement { .. } | Task::Planted { .. } => ["base", "fixed", "baseline", "patched"]
            .iter()
            .filter_map(|side| {
                TraceReader::open(&dir.join(side))
                    .ok()
                    .map(|trace| trace.meta.build.binary)
            })
            .collect(),
        // `sim-minimize` pins the simulator and fuzz target it ran under
        // `minimize/bin/`.
        Task::Minimize { .. } => fs::read_dir(dir.join("minimize").join("bin"))
            .into_iter()
            .flatten()
            .flatten()
            .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_file()))
            .filter_map(|entry| {
                crate::trace::read::read_bounded(&entry.path(), header::MAX_ARTIFACT_BYTES)
                    .ok()
                    .flatten()
                    .map(|bytes| sha256_hex(&bytes))
            })
            .collect(),
        Task::Target { .. } => json("fuzz-binary.json")
            .and_then(|binary| binary.get("sha256")?.as_str().map(str::to_owned))
            .into_iter()
            .collect(),
        Task::Command { .. } => Vec::new(),
    }
}

/// Where the header record and its run are, as the evaluator reads them.
pub struct HeaderRun<'a> {
    /// The header run's `--out`.
    pub out: &'a Path,
}

/// Judges the plan's artifacts and returns the record: each task's
/// verdict from its files, each header key's from hdr-14's record once
/// it's bound to its run, and each unsupported result blocked.
pub fn record(
    repo: &Path,
    plan: &Sim7Plan,
    artifacts: &Path,
    header: &HeaderRun<'_>,
    subject: &str,
    subject_source: &str,
    containment: &str,
) -> Sim7Record {
    let mut problems = plan.problems.clone();
    // The plan must be S's own: a supplied plan that relabels a task or
    // drops an unsupported result would otherwise be judged as given.
    if &self::plan(repo) != plan {
        problems.push("the supplied plan isn't the plan the manifest at S makes".to_owned());
    }
    // The release executable counts only if the run kept it, at the path
    // its digest names, with exactly those bytes.
    let release = artifacts.parent().and_then(|run| {
        let digest = bounded_json(&run.join("bin/index.json"))?
            .get("release")?
            .as_str()?
            .to_owned();
        let kept = crate::trace::read::read_bounded(
            &run.join("bin").join(&digest).join("zakura-sim"),
            header::MAX_ARTIFACT_BYTES,
        )
        .ok()
        .flatten()?;
        (sha256_hex(&kept) == digest).then_some(digest)
    });
    let bound = Bound {
        repo,
        commit: subject,
        source: subject_source,
        release,
    };
    let mut verdicts: BTreeMap<String, Verdict> = BTreeMap::new();
    let mut applicable = 0u64;
    let mut seed_traces: BTreeMap<String, (std::path::PathBuf, usize)> = BTreeMap::new();
    let mut evidence = BTreeMap::new();
    let mut measurements = BTreeMap::new();
    let artifacts_name = artifacts
        .file_name()
        .map_or_else(String::new, |name| name.to_string_lossy().into_owned());
    let run_name = artifacts
        .parent()
        .and_then(Path::file_name)
        .map_or_else(String::new, |name| name.to_string_lossy().into_owned());
    // Evidence links are relative to the archive root, the published
    // record's directory, which holds `<runs>/<run>/<artifacts>` as the
    // shared runner lays it out.
    let runs_name = artifacts
        .parent()
        .and_then(Path::parent)
        .and_then(Path::file_name)
        .map_or_else(String::new, |name| name.to_string_lossy().into_owned());
    let archive_artifacts = format!("{runs_name}/{run_name}/{artifacts_name}");
    // Every file below is read only once the whole tree is safe to read.
    let unsafe_tree = header::tree_refusal(artifacts);
    // The task that changed the subject checkout, which every later task's
    // build would have read.
    let mut dirtied: Option<usize> = None;
    for (index, task) in plan.tasks.iter().enumerate() {
        let dir = artifacts.join(index.to_string());
        if let Some(why) = &unsafe_tree {
            verdicts.insert(
                task.expected().to_owned(),
                Verdict::Blocked {
                    why: format!("the artifacts aren't safe to read: {why}"),
                },
            );
            continue;
        }
        let task_text = fs::read_to_string(dir.join("task.json")).unwrap_or_default();
        let recorded = serde_json::from_str::<Task>(&task_text).ok();
        let binding = TaskBinding {
            run: run_name.clone(),
            index,
            task_sha256: sha256_hex(task_text.as_bytes()),
        };
        let clean = runner_json(&dir, "checkout.json", &binding)
            .and_then(|checkout| checkout.get("clean")?.as_bool())
            == Some(true);
        // A seed or witness counts only on an input the release build's own
        // inventory admitted.
        let refusal = match task {
            Task::Seed {
                file,
                harness,
                input,
                ..
            } => seed_inventory_refusal(
                artifacts,
                SEED_INVENTORY,
                &bound,
                harness,
                Some(file),
                input,
            ),
            Task::SeedWitness { harness, input, .. } => {
                seed_inventory_refusal(artifacts, SEED_INVENTORY, &bound, harness, None, input)
            }
            Task::Planted {
                harness,
                input,
                input_sha256,
                ..
            } => seed_inventory_refusal(
                artifacts,
                SEED_INVENTORY,
                &bound,
                harness,
                Some(input),
                input_sha256,
            ),
            Task::RaceSchedule { harness, input, .. }
            | Task::Agreement {
                harness,
                base_input: input,
                ..
            } => seed_inventory_refusal(artifacts, RACE_INVENTORY, &bound, harness, None, input),
            _ => None,
        };
        let mut credited = 0u64;
        let binaries = task_binaries(task, &dir, &binding);
        let verdict = if recorded.as_ref() != Some(task) {
            Verdict::Blocked {
                why: "the artifact directory's task.json isn't this task".to_owned(),
            }
        } else if let Some(earlier) = dirtied {
            Verdict::Blocked {
                why: format!("task {earlier} changed the subject checkout"),
            }
        } else if !clean {
            dirtied = Some(index);
            Verdict::Blocked {
                why: "the subject checkout wasn't clean after this task".to_owned(),
            }
        } else if let Some(why) = refusal {
            Verdict::Blocked { why }
        } else if binaries.is_empty() && !matches!(task, Task::Command { .. }) {
            Verdict::Blocked {
                why: "the artifacts name no binary that produced this result".to_owned(),
            }
        } else {
            judge_task(task, &dir, &binding, &bound, &mut credited)
        };
        // The suite's applicability total is the fixed corpus's.
        if matches!(task, Task::Seed { .. }) {
            applicable += credited;
        }
        // An end record is judged only from a seed run that passed.
        if let (Task::Seed { file, .. }, Verdict::Verified) = (task, &verdict) {
            seed_traces.insert(file.clone(), (dir.join("trace"), index));
        }
        if let Task::Measure { expected, .. } = task {
            if let Some(measure) = fs::read_to_string(dir.join("measure.json"))
                .ok()
                .and_then(|text| serde_json::from_str::<Value>(&text).ok())
            {
                measurements.insert(expected.clone(), measure);
            }
        }
        evidence.insert(
            task.expected().to_owned(),
            Evidence {
                artifacts: vec![format!("{archive_artifacts}/{index}")],
                binaries,
                applicable: credited,
                properties: trace_properties(task, &dir),
            },
        );
        verdicts.insert(task.expected().to_owned(), verdict);
    }
    for (id, why) in &plan.unsupported {
        verdicts.insert(id.clone(), Verdict::Blocked { why: why.clone() });
    }
    let derived = derive(repo).unwrap_or_default();
    let rows = rows(&derived);
    // End records: the seed's own trace must end exactly as committed. A
    // seed's file is its end record's, `name.end.json`, as `name.bin`.
    for row in &rows {
        for expected in &row.expected {
            if let Expected::EndRecord { file } = expected {
                let seed = file
                    .strip_suffix(".end.json")
                    .map(|stem| format!("{stem}.bin"));
                let seed = seed.and_then(|seed| Some((seed_traces.get(&seed)?.clone(), seed)));
                let verdict = match &seed {
                    Some(((trace, _), _)) => end_record_matches(repo, file, trace),
                    None => Verdict::Blocked {
                        why: "no passing seed run produced this end record's trace".to_owned(),
                    },
                };
                // Its evidence is the seed run's.
                let seed_evidence = seed
                    .as_ref()
                    .and_then(|((_, index), _)| plan.tasks.get(*index))
                    .and_then(|task| evidence.get(task.expected()))
                    .cloned();
                if let Some(seed_evidence) = seed_evidence {
                    evidence.insert(
                        expected.to_string(),
                        Evidence {
                            applicable: 0,
                            ..seed_evidence
                        },
                    );
                }
                verdicts.insert(expected.to_string(), verdict);
            }
        }
    }
    // The discovery pair: from the planted case's runs, once they passed.
    let discovery = Expected::Agreement(DISCOVERY_CASE.0.to_owned()).to_string();
    let planted = plan
        .tasks
        .iter()
        .enumerate()
        .find(|(_, task)| matches!(task, Task::Planted { .. }));
    let pair = match planted {
        Some((index, task @ Task::Planted { patch, .. })) => {
            if verdicts.get(task.expected()) == Some(&Verdict::Verified) {
                if let Some(planted_evidence) = evidence.get(task.expected()).cloned() {
                    evidence.insert(
                        discovery.clone(),
                        Evidence {
                            applicable: 0,
                            properties: BTreeMap::new(),
                            ..planted_evidence
                        },
                    );
                }
                discovery_agreement(repo, &artifacts.join(index.to_string()), patch)
            } else {
                Verdict::Blocked {
                    why: "the discovery planted case's runs didn't pass".to_owned(),
                }
            }
        }
        _ => Verdict::Blocked {
            why: "no planted task ran the discovery case".to_owned(),
        },
    };
    verdicts.insert(discovery, pair);
    let (refusal, header_record) = header_record(repo, header, subject, subject_source);
    for row in &rows {
        for expected in &row.expected {
            if let Expected::HeaderKey(key) = expected {
                let verdict = match (&refusal, &header_record) {
                    (None, Some(record)) => header_key_verdict(record, key),
                    (Some(why), _) => Verdict::Blocked { why: why.clone() },
                    (None, None) => Verdict::Blocked {
                        why: "no header record".to_owned(),
                    },
                };
                let links = header_record
                    .iter()
                    .flat_map(|record| record.verified.iter().chain(&record.excluded))
                    .find(|entry| &entry.key == key)
                    .map(|entry| {
                        let base = header_record
                            .as_ref()
                            .map_or("", |record| record.artifacts.as_str());
                        entry
                            .links
                            .artifacts
                            .iter()
                            .map(|path| format!("header:{base}/{path}"))
                            .collect()
                    })
                    .unwrap_or_default();
                evidence.insert(
                    expected.to_string(),
                    Evidence {
                        artifacts: links,
                        binaries: Vec::new(),
                        applicable: 0,
                        properties: BTreeMap::new(),
                    },
                );
                verdicts.insert(expected.to_string(), verdict);
            }
        }
    }
    let specs: Vec<RowSpec> = rows
        .iter()
        .map(|row| RowSpec {
            id: row.id.clone(),
            results: row.expected.iter().map(ToString::to_string).collect(),
            never_excluded: row.never_excluded,
        })
        .collect();
    let evaluation = evaluate(&specs, &verdicts, applicable);
    let deviations: Vec<DeviationEntry> = DEVIATIONS
        .iter()
        .map(|d| DeviationEntry {
            id: d.id.to_owned(),
            spec: d.spec.to_owned(),
            claim: d.claim.to_owned(),
            approval: d.approval.map(str::to_owned),
            evan: format!("{:?}", d.evan),
            signed_off: d.is_signed_off(),
        })
        .collect();
    let accepted = evaluation.accepted
        && problems.is_empty()
        && refusal.is_none()
        && containment == "subreaper";
    let fully_accepted =
        accepted && evaluation.fully_accepted && deviations.iter().all(|d| d.signed_off);
    Sim7Record {
        artifacts: artifacts_name,
        subject: subject.to_owned(),
        subject_source: subject_source.to_owned(),
        containment: containment.to_owned(),
        header_refusal: refusal,
        problems,
        unsupported: plan.unsupported.clone(),
        evaluation,
        evidence,
        measurements,
        header_record_sha256: crate::trace::read::read_bounded(
            &header.out.join("record.json"),
            header::MAX_ARTIFACT_BYTES,
        )
        .ok()
        .flatten()
        .map(|bytes| sha256_hex(&bytes)),
        deviations,
        accepted,
        fully_accepted,
    }
}

/// What every judged artifact must be bound to.
struct Bound<'a> {
    /// The checkout of S.
    repo: &'a Path,
    /// S's commit.
    commit: &'a str,
    /// S's source digest.
    source: &'a str,
    /// The SHA-256 of the release `zakura-sim` the run kept, which every
    /// seed, witness and measured trace must have been recorded by.
    release: Option<String>,
}

/// The patched build's SHA-256 for the planted task in `dir`, if the run
/// kept that executable with exactly those bytes under `<run>/bin/<sha>/`.
fn kept_patched(dir: &Path, binding: &TaskBinding) -> Option<String> {
    let sha = runner_json(dir, "patched.json", binding)?
        .get("sha256")?
        .as_str()?
        .to_owned();
    let run = dir.parent()?.parent()?;
    let kept = crate::trace::read::read_bounded(
        &run.join("bin").join(&sha).join("zakura-sim"),
        header::MAX_ARTIFACT_BYTES,
    )
    .ok()
    .flatten()?;
    (sha256_hex(&kept) == sha).then_some(sha)
}

/// The planted task's verdict: `cargo xtask planted` passed, the baseline
/// run on S's release build holds, and the run on the kept patched build,
/// from the same commit, violates the property, each replayed exactly by
/// its own build and passed through [`owned_run_passes`].
fn judge_planted(
    dir: &Path,
    binding: &TaskBinding,
    bound: &Bound<'_>,
    harness: &str,
    input: &str,
    applicable: &mut u64,
) -> Verdict {
    let blocked = |why: String| Verdict::Blocked { why };
    if !exited(runner_json(dir, "planted.json", binding)) {
        return blocked("`cargo xtask planted` didn't pass".to_owned());
    }
    // Every run the planted runner made, at any depth, is a consumed run:
    // its own classifier judged the outcome, and its cleanup is judged
    // here by the one rule.
    match run_records(&dir.join("planted-runs"), binding) {
        Ok(runs) if runs.is_empty() => {
            return blocked("`cargo xtask planted` recorded no runs".to_owned())
        }
        Ok(runs) => {
            if let Some(why) = planted_runs_refusal(dir, binding, harness, input, &runs) {
                return blocked(why);
            }
        }
        Err(why) => return blocked(why),
    }
    if !exited(runner_json(dir, "build-patched.json", binding)) {
        return blocked("the patched build didn't finish".to_owned());
    }
    let Some(patched_sha) = kept_patched(dir, binding) else {
        return blocked("the run didn't keep the patched build it recorded with".to_owned());
    };
    let Ok(patched) = TraceReader::open(&dir.join("patched")) else {
        return blocked("the patched run left no trace".to_owned());
    };
    // The patched build embeds its own source, S plus the patch, so its
    // trace is bound to that build and to S's commit rather than S's digest.
    let patched_bound = Bound {
        repo: bound.repo,
        commit: bound.commit,
        source: &patched.meta.build.source,
        release: Some(patched_sha),
    };
    let baseline_git = TraceReader::open(&dir.join("baseline"))
        .ok()
        .map(|trace| trace.meta.build.git);
    if baseline_git.as_deref() != Some(patched.meta.build.git.as_str())
        || patched.meta.build.source == bound.source
    {
        return blocked("the patched build isn't S's commit with the patch applied".to_owned());
    }
    for (side, recorder) in [("baseline", bound), ("patched", &patched_bound)] {
        if !exited(runner_json(dir, &format!("record-{side}.json"), binding)) {
            return blocked(format!("the {side} run's witness recording didn't exit 0"));
        }
        if let Some(why) = replay_refusal(
            dir,
            (side, &format!("replay-{side}.json")),
            binding,
            recorder,
        ) {
            return blocked(why);
        }
    }
    let property = DISCOVERY_CASE.0.to_owned();
    let (violates, passes) = (
        RunExpect::Violates {
            property: property.clone(),
            also: Vec::new(),
        },
        RunExpect::Passes,
    );
    if let Some(why) = runs_refusal(
        dir,
        binding,
        harness,
        &[
            ("record-baseline", &passes, Some(input)),
            ("replay-baseline", &passes, Some(input)),
            ("record-patched", &violates, Some(input)),
            ("replay-patched", &violates, Some(input)),
        ],
    ) {
        return blocked(why);
    }
    let baseline = judge_trace(
        &dir.join("baseline"),
        bound,
        (harness, Some(input)),
        &RunExpect::Passes,
        applicable,
    );
    if baseline != Verdict::Verified {
        return baseline;
    }
    judge_trace(
        &dir.join("patched"),
        &patched_bound,
        (harness, Some(input)),
        &RunExpect::Violates {
            property,
            also: Vec::new(),
        },
        applicable,
    )
}

/// One run's records, as [`crate::acceptance_runs`] writes them.
struct RunRecord {
    /// The records' file stem, `<pid>-<start>-<n>`.
    stem: String,
    /// The harness.
    harness: String,
    /// The full input's SHA-256, where the caller named it.
    input: Option<String>,
    /// The tool phase it ran in.
    phase: String,
    /// Its role in that phase, where its caller named one.
    role: Option<String>,
    /// The `end` record it wrote.
    end: crate::record::EndRecord,
}

/// The one check of a run's identity: its begin and end records name the
/// same full identity, its id is its records' file name `stem` (so a copy
/// under another name is refused), and its binding is this task's in this
/// run.
fn bound_identity(
    stem: &str,
    begin: Value,
    end: Value,
    binding: &TaskBinding,
) -> Result<crate::acceptance_runs::RunIdentity, String> {
    use crate::acceptance_runs::RunIdentity;
    let parse = |value: Value, which: &str| {
        serde_json::from_value::<RunIdentity>(value)
            .map_err(|error| format!("run {stem}'s {which} record: {error}"))
    };
    let (begin, end) = (parse(begin, "begin")?, parse(end, "end")?);
    if begin != end {
        return Err(format!("run {stem}'s begin and end name different runs"));
    }
    if end.id != stem {
        return Err(format!("run {stem}'s records carry another run's id"));
    }
    let field = |name: &str| end.binding.get(name);
    if field("run").and_then(Value::as_str) != Some(&binding.run)
        || field("task").and_then(Value::as_u64) != u64::try_from(binding.index).ok()
        || field("task_sha256").and_then(Value::as_str) != Some(&binding.task_sha256)
    {
        return Err(format!("run {stem} was recorded for another run or task"));
    }
    Ok(end)
}

/// Every run recorded under `dir`: each begin record must have its end,
/// each end its begin, the two must name the same harness, input, phase
/// and binding, and that binding must be this task's in this run.
fn run_records(dir: &Path, binding: &TaskBinding) -> Result<Vec<RunRecord>, String> {
    let entries = fs::read_dir(dir).map_err(|_| "the tool left no run records".to_owned())?;
    let read = |path: &Path| -> Option<Value> {
        serde_json::from_str(&fs::read_to_string(path).ok()?).ok()
    };
    let mut begins = BTreeMap::new();
    let mut ends = BTreeMap::new();
    for entry in entries.flatten() {
        let file = entry.file_name().to_string_lossy().into_owned();
        if let Some(stem) = file.strip_suffix(".begin.json") {
            begins.insert(stem.to_owned(), entry.path());
        } else if let Some(stem) = file.strip_suffix(".end.json") {
            ends.insert(stem.to_owned(), entry.path());
        } else {
            return Err(format!("{file} isn't a run record"));
        }
    }
    if let Some(stem) = begins.keys().find(|stem| !ends.contains_key(*stem)) {
        return Err(format!("run {stem} began but never ended"));
    }
    let mut runs = Vec::new();
    for (stem, path) in &ends {
        let begin = begins
            .get(stem)
            .and_then(|path| read(path))
            .ok_or_else(|| format!("run {stem} has no readable begin record"))?;
        let mut end = read(path).ok_or_else(|| format!("run {stem}'s end record doesn't read"))?;
        let record = end
            .as_object_mut()
            .and_then(|fields| fields.remove("end"))
            .and_then(|record| serde_json::from_value(record).ok())
            .ok_or_else(|| format!("run {stem} left no end record"))?;
        let identity = bound_identity(stem, begin, end, binding)?;
        runs.push(RunRecord {
            stem: stem.clone(),
            harness: identity.harness,
            input: identity.input_sha256,
            phase: identity.phase,
            role: identity.role,
            end: record,
        });
    }
    Ok(runs)
}

/// Why a task's recorded runs under `<dir>/runs` don't pass, if they
/// don't (Rule 11): every run is of `harness`, in one of `phases`, of the
/// input that phase names (or of none, for Plan bytes on a scenario), and
/// passes through [`owned_run_passes`] with that phase's expected result,
/// and every phase has exactly one run.
fn runs_refusal(
    dir: &Path,
    binding: &TaskBinding,
    harness: &str,
    phases: &[(&str, &RunExpect, Option<&str>)],
) -> Option<String> {
    let runs = match run_records(&dir.join("runs"), binding) {
        Ok(runs) => runs,
        Err(why) => return Some(why),
    };
    let Ok(entry) = crate::harness::find(harness) else {
        return Some(format!("{harness} isn't a harness at S"));
    };
    let registered = registered(entry);
    let mut seen = BTreeSet::new();
    for run in &runs {
        let Some((phase, expect, input)) = phases.iter().find(|(phase, ..)| *phase == run.phase)
        else {
            return Some(format!(
                "run {} is in an unaccounted phase {}",
                run.stem, run.phase
            ));
        };
        if run.harness != harness {
            return Some(format!("run {} isn't a run of {harness}", run.stem));
        }
        // Each run names the input its phase runs, or none for a run of
        // Plan bytes on a scenario.
        if run.input.as_deref() != *input {
            return Some(format!(
                "run {} ({phase}) isn't a run of its input",
                run.stem
            ));
        }
        if let Err(why) = owned_run_passes(&run.end, &registered, expect) {
            return Some(format!("run {} ({phase}): {why}", run.stem));
        }
        // Each phase is one child making one run: a second is unaccounted.
        if !seen.insert(*phase) {
            return Some(format!("{phase} recorded more than one run"));
        }
    }
    phases
        .iter()
        .find(|(phase, ..)| !seen.contains(phase))
        .map(|(phase, ..)| format!("no {phase} run was recorded"))
}

/// The registered properties of `entry`'s harness, with the ones a run may
/// violate without failing (`Known` and `Draft`).
fn registered(entry: &crate::harness::HarnessEntry) -> Registered {
    let properties = (entry.properties)();
    Registered {
        ids: properties.iter().map(|p| p.id.clone()).collect(),
        tolerated: properties
            .iter()
            .filter(|p| matches!(p.status, Status::Known { .. } | Status::Draft))
            .map(|p| p.id.clone())
            .collect(),
    }
}

/// Why the minimization's pinned build doesn't count, if it doesn't: both
/// executables `sim-minimize` ran, kept under `minimize/bin/`, must hash to
/// the digests its report pins, and the simulator must be a build of S's
/// commit with a change applied, the case's patch.
fn minimize_build_refusal(dir: &Path, bound: &Bound<'_>, target: &str) -> Option<String> {
    let unread = || Some("sim-minimize's report.json can't be read".to_owned());
    let Some(report) = fs::read_to_string(dir.join("minimize").join("report.json"))
        .ok()
        .and_then(|text| serde_json::from_str::<Value>(&text).ok())
    else {
        return unread();
    };
    let pinned = |field: &str| {
        report
            .pointer(&format!("/build/{field}"))
            .and_then(Value::as_str)
    };
    for (file, field) in [("zakura-sim", "sim_binary"), (target, "fuzz_binary")] {
        let kept = crate::trace::read::read_bounded(
            &dir.join("minimize").join("bin").join(file),
            header::MAX_ARTIFACT_BYTES,
        )
        .ok()
        .flatten()
        .map(|bytes| sha256_hex(&bytes));
        if kept.is_none() || kept.as_deref() != pinned(field) {
            return Some(format!(
                "minimize/bin/{file} isn't the build sim-minimize pinned"
            ));
        }
    }
    let built = |field: &str| report.pointer(&format!("/build/sim_build/{field}"));
    if built("git").and_then(Value::as_str) != Some(bound.commit)
        || built("dirty").and_then(Value::as_bool) != Some(true)
        || built("binary").and_then(Value::as_str) != pinned("sim_binary")
    {
        return Some("the minimizer's simulator isn't S with the case's patch".to_owned());
    }
    None
}

/// Why the minimization's kept bundle doesn't stand, if it doesn't: the
/// minimized input must hash to `minimized`, its confirmation must have
/// confirmed `property` on it, and its trace must be a run of it recorded by
/// the simulator the report pins.
fn minimized_bundle_refusal(
    dir: &Path,
    report: &Value,
    property: &str,
    minimized: &str,
) -> Option<String> {
    let root = dir.join("minimize");
    // A report path must be relative and stay inside the checked tree.
    let inside = |pointer: &str| -> Result<std::path::PathBuf, String> {
        let path = report
            .pointer(pointer)
            .and_then(Value::as_str)
            .ok_or_else(|| format!("sim-minimize's report has no {pointer}"))?;
        let path = Path::new(path);
        let confined = path.is_relative()
            && path
                .components()
                .all(|part| matches!(part, std::path::Component::Normal(_)));
        if confined {
            Ok(root.join(path))
        } else {
            Err(format!("sim-minimize's {pointer} leaves its directory"))
        }
    };
    let (bundle, trace) = match (inside("/minimized/dir"), inside("/minimized/trace")) {
        (Ok(bundle), Ok(trace)) => (bundle, trace),
        (Err(why), _) | (_, Err(why)) => return Some(why),
    };
    let bounded = |path: &Path| {
        crate::trace::read::read_bounded(path, header::MAX_ARTIFACT_BYTES)
            .ok()
            .flatten()
    };
    let input = bounded(&bundle.join("input.bin")).map(|bytes| sha256_hex(&bytes));
    if input.as_deref() != Some(minimized) {
        return Some("the kept minimized input isn't the one the report names".to_owned());
    }
    let confirmation: Option<Value> = bounded(&bundle.join("confirm").join("confirmation.json"))
        .and_then(|bytes| serde_json::from_slice(&bytes).ok());
    let confirmed = confirmation.as_ref().is_some_and(|c| {
        c.get("status").and_then(Value::as_str) == Some("confirmed")
            && c.get("property").and_then(Value::as_str) == Some(property)
            && c.pointer("/input/sha256").and_then(Value::as_str) == Some(minimized)
    });
    if !confirmed {
        return Some("the kept minimized input's confirmation doesn't confirm it".to_owned());
    }
    let pinned = report.pointer("/build/sim_binary").and_then(Value::as_str);
    let Ok(kept) = TraceReader::open(&trace) else {
        return Some("the kept minimized trace doesn't read".to_owned());
    };
    let bound = kept.meta.input.as_ref().map(|i| i.sha256.as_str()) == Some(minimized)
        && Some(kept.meta.build.binary.as_str()) == pinned
        && confirmation
            .as_ref()
            .and_then(|c| c.get("trace_id"))
            .and_then(Value::as_str)
            == Some(kept.id.as_str());
    if !bound {
        return Some(
            "the kept trace isn't its confirmation's run of the minimized input".to_owned(),
        );
    }
    // The trace itself passes the one judge, expecting the property.
    let Ok(entry) = crate::harness::find(&kept.meta.sim) else {
        return Some(format!(
            "the kept trace's harness {} isn't one at S",
            kept.meta.sim
        ));
    };
    let Some(end) = kept.end.as_ref() else {
        return Some("the kept minimized trace has no end record".to_owned());
    };
    owned_run_passes(
        end,
        &registered(entry),
        &RunExpect::Violates {
            property: property.to_owned(),
            also: Vec::new(),
        },
    )
    .err()
    .map(|why| format!("the kept minimized trace: {why}"))
}

/// The minimization's verdict (Rule 11): `sim-minimize` reported a
/// minimized candidate of the case's own input, property and harness, and
/// for the original and the minimized input each, the fuzz target's run,
/// every run inside `zakura-sim confirm` and the fresh `zakura-sim replay`
/// all violate the property and pass through [`owned_run_passes`], each
/// exactly once. The search's runs, and a rejected candidate's, are
/// accounted for but needn't pass. Every run must have ended and the
/// reduction completed. Any run in another phase blocks.
fn judge_minimize(
    dir: &Path,
    binding: &TaskBinding,
    bound: &Bound<'_>,
    (property, harness, target): (&str, &str, &str),
    input: &str,
) -> Verdict {
    let blocked = |why: String| Verdict::Blocked { why };
    if !exited(runner_json(dir, "minimize.json", binding)) {
        return blocked("`cargo xtask sim-minimize` didn't exit 0".to_owned());
    }
    if let Some(why) = minimize_build_refusal(dir, bound, target) {
        return blocked(why);
    }
    let Some(report) = fs::read_to_string(dir.join("minimize").join("report.json"))
        .ok()
        .and_then(|text| serde_json::from_str::<Value>(&text).ok())
    else {
        return blocked("sim-minimize left no report".to_owned());
    };
    let text = |pointer: &str| report.pointer(pointer).and_then(Value::as_str);
    if text("/status") != Some("minimized")
        || text("/property") != Some(property)
        || text("/harness") != Some(harness)
        || text("/original/sha256") != Some(input)
    {
        return blocked("sim-minimize didn't minimize the case's own input".to_owned());
    }
    // A search cut off at its deadline can leave a run unfinished, so the
    // reduction must have completed.
    if report
        .pointer("/reduction/completed")
        .and_then(Value::as_bool)
        != Some(true)
    {
        return blocked("sim-minimize's reduction didn't complete".to_owned());
    }
    let Some(minimized) = text("/minimized/sha256").map(str::to_owned) else {
        return blocked("sim-minimize's report names no minimized input".to_owned());
    };
    let runs = match run_records(&dir.join("runs"), binding) {
        Ok(runs) => runs,
        Err(why) => return blocked(why),
    };
    let Ok(entry) = crate::harness::find(harness) else {
        return blocked(format!("{harness} isn't a harness at S"));
    };
    let registered = registered(entry);
    let expect = RunExpect::Violates {
        property: property.to_owned(),
        also: Vec::new(),
    };
    // The kept bundle: the minimized input itself, its confirmation of the
    // property, and its trace, recorded by the pinned simulator.
    if let Some(why) = minimized_bundle_refusal(dir, &report, property, &minimized) {
        return blocked(why);
    }
    // Inputs `sim-minimize` checked and rejected on the way are accounted
    // for but not judged, like the search's runs.
    let rejected: BTreeSet<String> = report
        .get("candidates")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|candidate| candidate.get("sha256")?.as_str().map(str::to_owned))
        .filter(|sha| sha != input && *sha != minimized)
        .collect();
    let mut seen: BTreeMap<(String, String), usize> = BTreeMap::new();
    for run in &runs {
        if run.phase == "search" {
            continue;
        }
        let Some((phase, sha)) = ["confirm-target", "confirm", "replay"]
            .iter()
            .find_map(|phase| Some((*phase, run.phase.strip_prefix(&format!("{phase}:"))?)))
        else {
            return blocked(format!(
                "run {} is in an unaccounted phase {}",
                run.stem, run.phase
            ));
        };
        // Even a rejected candidate's run must be the harness on the input
        // its phase names.
        if run.input.as_deref() != Some(sha) || run.harness != harness {
            return blocked(format!(
                "run {} isn't the harness's run of input {sha}",
                run.stem
            ));
        }
        if rejected.contains(sha) {
            continue;
        }
        if sha != input && sha != minimized {
            return blocked(format!(
                "run {} confirms an input sim-minimize never named",
                run.stem
            ));
        }
        if let Err(why) = owned_run_passes(&run.end, &registered, &expect) {
            return blocked(format!("run {} ({}): {why}", run.stem, run.phase));
        }
        // Inside `zakura-sim confirm`, each run's role tells it apart.
        let atom = match (phase, run.role.as_deref()) {
            ("confirm", Some(role)) => format!("confirm/{role}"),
            ("confirm", None) => {
                return blocked(format!("confirmation run {} names no role", run.stem))
            }
            (phase, _) => phase.to_owned(),
        };
        *seen.entry((atom, sha.to_owned())).or_default() += 1;
    }
    // Each confirmation is one fuzz-target run, the four runs inside
    // `zakura-sim confirm` (its input run, witness rerun, recording and that
    // recording's replay) and one fresh replay, each exactly once.
    let atoms = [
        "confirm-target",
        "confirm/input",
        "confirm/witness",
        "confirm/record",
        "confirm/replay",
        "replay",
    ];
    for sha in [input, minimized.as_str()] {
        for atom in atoms {
            let got = seen
                .get(&(atom.to_owned(), sha.to_owned()))
                .copied()
                .unwrap_or(0);
            if got != 1 {
                return blocked(format!("input {sha} has {got} {atom} runs, not 1"));
            }
        }
        if seen
            .keys()
            .any(|(atom, at)| at == sha && !atoms.contains(&atom.as_str()))
        {
            return blocked(format!(
                "input {sha} has a confirmation run of an unknown role"
            ));
        }
    }
    Verdict::Verified
}

/// A fuzz target's verdict (Rule 11): `zakura-sim targets` reported it
/// ready, from a fuzz binary built from a clean checkout at S's commit
/// (its own source digest also covers the fuzz package, so it isn't S's),
/// and every run the target
/// made is accounted for ([`run_records`]): each run is of the target's
/// harness on one of its admitted seeds (libFuzzer may rerun a seed, an
/// observed atom), every seed ran both in `targets`' own process and as a
/// fuzz-target iteration, and every run passes through
/// [`owned_run_passes`], cleanup included.
fn judge_target(
    dir: &Path,
    binding: &TaskBinding,
    bound: &Bound<'_>,
    name: &str,
    harness: &str,
    seeds: &[String],
) -> Verdict {
    let blocked = |why: String| Verdict::Blocked { why };
    if !exited(runner_json(dir, "targets.json", binding)) {
        return blocked("`zakura-sim targets` didn't report the target ready".to_owned());
    }
    let readiness: Option<Value> =
        fs::read_to_string(dir.join("targets").join(crate::campaign::READINESS_FILE))
            .ok()
            .and_then(|text| serde_json::from_str(&text).ok());
    let row = readiness
        .as_ref()
        .and_then(|r| r.get("targets")?.as_array())
        .and_then(|rows| {
            rows.iter()
                .find(|row| row.get("target").and_then(Value::as_str) == Some(name))
        });
    let Some(row) = row else {
        return blocked(format!("the readiness report has no row for {name}"));
    };
    if row.get("status").and_then(Value::as_str) != Some("ready") {
        return blocked(format!("{name} isn't ready"));
    }
    // The fuzz build's source digest covers the fuzz package too, so it's
    // computed from S the same way the build computes it.
    let fuzz_source = crate::campaign::fuzz_source(bound.repo);
    if row.pointer("/build/git").and_then(Value::as_str) != Some(bound.commit)
        || row.pointer("/build/dirty").and_then(Value::as_bool) != Some(false)
        || row.pointer("/build/source").and_then(Value::as_str) != Some(fuzz_source.as_str())
    {
        return blocked(format!("{name}'s fuzz binary wasn't built from S's commit"));
    }
    let Ok(entry) = crate::harness::find(harness) else {
        return blocked(format!("{harness} isn't a harness at S"));
    };
    let registered = registered(entry);
    let runs = match run_records(&dir.join("runs"), binding) {
        Ok(runs) => runs,
        Err(why) => return blocked(why),
    };
    // Every run is the harness on one of its seeds, in `targets`' own
    // process or as a fuzz-target iteration, and passes. Each seed ran in
    // process once per seed file holding its bytes, and as an iteration at
    // least that often, and the iterations number what libFuzzer counted
    // (its reruns are observed atoms).
    let mut ran: BTreeMap<(String, String), usize> = BTreeMap::new();
    for run in &runs {
        if run.phase != "in-process" && run.phase != "fuzz-target" {
            return blocked(format!(
                "run {} is in an unaccounted phase {}",
                run.stem, run.phase
            ));
        }
        let input = run.input.as_deref();
        if run.harness != harness || !input.is_some_and(|input| seeds.iter().any(|s| s == input)) {
            return blocked(format!(
                "run {} isn't a run of one of {harness}'s seeds",
                run.stem
            ));
        }
        if let Err(why) = owned_run_passes(&run.end, &registered, &RunExpect::Passes) {
            return blocked(format!("run {}: {why}", run.stem));
        }
        *ran.entry((run.phase.clone(), input.unwrap_or_default().to_owned()))
            .or_default() += 1;
    }
    let mut wanted: BTreeMap<&str, usize> = BTreeMap::new();
    for seed in seeds {
        *wanted.entry(seed.as_str()).or_default() += 1;
    }
    let count = |phase: &str, seed: &str| {
        ran.get(&(phase.to_owned(), seed.to_owned()))
            .copied()
            .unwrap_or(0)
    };
    for (seed, files) in &wanted {
        if count("in-process", seed) != *files {
            return blocked(format!(
                "the target ran seed {seed} in process other than {files} times"
            ));
        }
        if count("fuzz-target", seed) < *files {
            return blocked(format!(
                "the fuzz target ran seed {seed} fewer than {files} times"
            ));
        }
    }
    let iterations: usize = runs.iter().filter(|run| run.phase == "fuzz-target").count();
    let counted = row.get("fuzzer_runs").and_then(Value::as_u64);
    if counted.and_then(|n| usize::try_from(n).ok()) != Some(iterations) {
        return blocked(format!(
            "the fuzz target recorded {iterations} runs, but libFuzzer counted {counted:?}"
        ));
    }
    if seeds.is_empty() {
        return blocked(format!("{harness} has no admitted seed"));
    }
    Verdict::Verified
}

/// Why the planted runner's own runs don't pass, if they don't. The case is
/// sim-7's own, so each run must be of `harness` and passes through
/// [`owned_run_passes`]; [`planted_judged`] then judges each by its phase.
fn planted_runs_refusal(
    dir: &Path,
    binding: &TaskBinding,
    harness: &str,
    input: &str,
    runs: &[RunRecord],
) -> Option<String> {
    let campaign: Vec<String> = runner_json(dir, "planted-inventory.json", binding)
        .and_then(|inventory| serde_json::from_value(inventory.get("campaign")?.clone()).ok())
        .unwrap_or_default();
    if campaign.is_empty() {
        return Some("the planted runner's campaign inventory is missing".to_owned());
    }
    let violates = RunExpect::Violates {
        property: DISCOVERY_CASE.0.to_owned(),
        also: Vec::new(),
    };
    let mut judged = Vec::with_capacity(runs.len());
    for run in runs {
        if run.harness != harness {
            return Some(format!("planted run {} isn't a run of {harness}", run.stem));
        }
        let Ok(entry) = crate::harness::find(&run.harness) else {
            return Some(format!("{} isn't a harness at S", run.harness));
        };
        let registered = registered(entry);
        judged.push(PlantedRun {
            stem: &run.stem,
            phase: &run.phase,
            input: run.input.as_deref(),
            held: owned_run_passes(&run.end, &registered, &RunExpect::Passes)
                .map_err(|why| why.to_string()),
            caught: owned_run_passes(&run.end, &registered, &violates)
                .map_err(|why| why.to_string()),
        });
    }
    planted_judged(&judged, input, &campaign)
}

/// One planted run, as [`planted_judged`] judges it.
struct PlantedRun<'a> {
    /// Its records' file stem.
    stem: &'a str,
    /// The planted runner's phase for it.
    phase: &'a str,
    /// The full input it ran, where its caller named it.
    input: Option<&'a str>,
    /// Whether it passed as a run whose properties held.
    held: Result<(), String>,
    /// Whether it passed as a run the case's property violated.
    caught: Result<(), String>,
}

/// Why `runs` aren't the planted runner's runs of the case's `input` and
/// its `campaign` inventory, if they aren't. The baseline and its fresh
/// replay (`replay-baseline`) each hold, exactly once; the patched run and
/// its fresh replay (`replay-patched`) each violate the case's property,
/// exactly once; each listed campaign input ran as often as it's listed,
/// as a run that held or one the violation stopped. No other phase is
/// allowed.
fn planted_judged(runs: &[PlantedRun<'_>], input: &str, campaign: &[String]) -> Option<String> {
    const CASE_PHASES: [&str; 4] = ["baseline", "replay-baseline", "patched", "replay-patched"];
    let mut counted: BTreeMap<(&str, Option<&str>), usize> = BTreeMap::new();
    for run in runs {
        let verdict = match run.phase {
            phase if CASE_PHASES.contains(&phase) && run.input != Some(input) => {
                return Some(format!(
                    "planted run {} isn't of the case's input",
                    run.stem
                ));
            }
            "baseline" | "replay-baseline" => run.held.clone(),
            "patched" | "replay-patched" => run.caught.clone(),
            "campaign"
                if run
                    .input
                    .is_some_and(|named| campaign.iter().any(|c| c == named)) =>
            {
                run.held.clone().or_else(|_| run.caught.clone())
            }
            "campaign" => {
                return Some(format!(
                    "planted run {} isn't of a listed campaign input",
                    run.stem
                ));
            }
            other => {
                return Some(format!(
                    "planted run {} has an unknown role {other}",
                    run.stem
                ))
            }
        };
        if let Err(why) = verdict {
            return Some(format!("planted {} run {}: {why}", run.phase, run.stem));
        }
        *counted.entry((run.phase, run.input)).or_default() += 1;
    }
    let count = |key: (&str, Option<&str>)| counted.get(&key).copied().unwrap_or(0);
    for phase in CASE_PHASES {
        if count((phase, Some(input))) != 1 {
            return Some(format!(
                "the planted runner didn't make exactly one {phase} run of the case's input"
            ));
        }
    }
    let mut wanted: BTreeMap<&str, usize> = BTreeMap::new();
    for listed in campaign {
        *wanted.entry(listed.as_str()).or_default() += 1;
    }
    for (listed, want) in &wanted {
        if count(("campaign", Some(listed))) != *want {
            return Some(format!(
                "campaign input {listed} didn't run exactly {want} times"
            ));
        }
    }
    None
}

/// The discovery agreement pair's verdict, from the planted task's runs in
/// `dir` once they passed: the patched run, its lines mapped back across
/// the patch, and the baseline run on S agree on every step before their
/// first difference, which must be at [`DISCOVERY_SITE`] on S's side.
fn discovery_agreement(repo: &Path, dir: &Path, patch: &str) -> Verdict {
    let blocked = |why: String| Verdict::Blocked { why };
    let map = match fs::read_to_string(repo.join(patch))
        .map_err(|error| error.to_string())
        .and_then(|text| super::agreement::HunkMap::parse(&text))
    {
        Ok(map) => map.inverse(),
        Err(why) => return blocked(format!("{patch}: {why}")),
    };
    let views = |side: &str| super::agreement::step_views(&dir.join(side));
    match (views("patched"), views("baseline")) {
        (Ok(patched), Ok(baseline)) => match super::agreement::agreement(
            &patched,
            &baseline,
            &map,
            &super::agreement::Site {
                at: DISCOVERY_SITE.to_owned(),
            },
        ) {
            Ok(_) => Verdict::Verified,
            Err(why) => blocked(why),
        },
        (Err(why), _) | (_, Err(why)) => blocked(why),
    }
}

/// One task's verdict from its artifacts in `dir`.
fn judge_task(
    task: &Task,
    dir: &Path,
    binding: &TaskBinding,
    bound: &Bound<'_>,
    applicable: &mut u64,
) -> Verdict {
    let source = bound.source;
    let blocked = |why: String| Verdict::Blocked { why };
    let json = |name: &str| runner_json(dir, name, binding);
    let own = |name: &str| -> Option<Value> {
        serde_json::from_str(&fs::read_to_string(dir.join(name)).ok()?).ok()
    };
    match task {
        Task::Test {
            package,
            file,
            test,
            ..
        } => {
            // hdr-14's runner writes `test.json`; its `binary.json` beside it
            // carries the binding.
            let Some(result) = own("test.json") else {
                return blocked("the test left no test.json".to_owned());
            };
            let test_sha = fs::read(dir.join("test.json"))
                .ok()
                .map(|bytes| sha256_hex(&bytes));
            let named = json("binary.json")
                .and_then(|b| b.get("test_json_sha256")?.as_str().map(str::to_owned));
            if test_sha.is_none() || named != test_sha {
                return blocked("test.json isn't the one this run's binary.json binds".to_owned());
            }
            let same = result.get("package").and_then(Value::as_str) == Some(package)
                && result.get("file").and_then(Value::as_str) == Some(file);
            if !same {
                return blocked(format!("test.json isn't {package} {file}'s"));
            }
            if result.get("checkout_source").and_then(Value::as_str) != Some(source) {
                return blocked("the test didn't run from this run's checkout of S".to_owned());
            }
            // zakura-sim's binaries embed S's source digest, so theirs must
            // match. The others carry none, and are the run's own build.
            if package == "zakura-sim"
                && result.get("binary_source").and_then(Value::as_str) != Some(source)
            {
                return blocked("the test binary doesn't embed S's source digest".to_owned());
            }
            exactly_one_passed(&result, test)
        }
        Task::FuzzTest { test, .. } => match json("test.json") {
            Some(result)
                if result.get("checkout_source").and_then(Value::as_str) != Some(source) =>
            {
                blocked("the fuzz test didn't run from this run's checkout of S".to_owned())
            }
            Some(result) => exactly_one_passed(&result, test),
            None => blocked("the test left no test.json".to_owned()),
        },
        Task::Lane {
            command,
            required,
            empty,
            ignored_run,
            ..
        } => match json("lane.json") {
            Some(lane) => match lane_bound(&lane, source, command) {
                Some(why) => blocked(why),
                None => lane_passed(&lane, required, empty, ignored_run),
            },
            None => blocked("the lane left no lane.json".to_owned()),
        },
        Task::Seed { harness, input, .. } => {
            if !exited(json("record.json")) {
                return blocked("the seed's recording didn't exit 0".to_owned());
            }
            if let Some(why) = replay_refusal(dir, ("trace", "replay.json"), binding, bound) {
                return blocked(why);
            }
            let passes = RunExpect::Passes;
            if let Some(why) = runs_refusal(
                dir,
                binding,
                harness,
                &[
                    ("record", &passes, Some(input.as_str())),
                    ("replay", &passes, Some(input.as_str())),
                ],
            ) {
                return blocked(why);
            }
            judge_trace(
                &dir.join("trace"),
                bound,
                (harness, Some(input)),
                &RunExpect::Passes,
                applicable,
            )
        }
        Task::SeedWitness {
            harness,
            input,
            property,
            expect,
            ..
        }
        | Task::RaceSchedule {
            harness,
            input,
            property,
            expect,
            ..
        } => {
            if !exited(json("record.json")) {
                return blocked("the witness recording didn't exit 0".to_owned());
            }
            if let Some(why) = replay_refusal(dir, ("trace", "replay.json"), binding, bound) {
                return blocked(why);
            }
            let Some(witness) = own("trace/witness.json") else {
                return blocked("the witness left no witness.json".to_owned());
            };
            let same = witness.get("property").and_then(Value::as_str) == Some(property)
                && witness.get("expect").and_then(Value::as_str) == Some(expect);
            if !same {
                return blocked(format!(
                    "witness.json isn't the witness of {property}, {expect}"
                ));
            }
            if witness.get("passed").and_then(Value::as_bool) != Some(true) {
                return blocked(format!("the witness of {property} didn't pass"));
            }
            let also: Vec<String> = witness
                .get("also_violates")
                .and_then(|also| serde_json::from_value(also.clone()).ok())
                .unwrap_or_default();
            let violates = matches!(expect.as_str(), "violated" | "violated-set");
            let run_expect = if violates {
                RunExpect::Violates {
                    property: property.clone(),
                    also,
                }
            } else {
                RunExpect::Passes
            };
            if let Some(why) = runs_refusal(
                dir,
                binding,
                harness,
                &[
                    ("record", &run_expect, Some(input.as_str())),
                    ("replay", &run_expect, Some(input.as_str())),
                ],
            ) {
                return blocked(why);
            }
            let verdict = judge_trace(
                &dir.join("trace"),
                bound,
                (harness, Some(input)),
                &run_expect,
                applicable,
            );
            // `witness.json` names no trace, so the claim is checked in the
            // trace's own end record: a passing witness's property held on
            // at least one application.
            if verdict == Verdict::Verified && !violates {
                let held = TraceReader::open(&dir.join("trace"))
                    .ok()
                    .and_then(|trace| trace.end)
                    .is_some_and(|end| {
                        end.properties.iter().any(|report| {
                            &report.property == property
                                && matches!(
                                    report.outcome,
                                    PropertyOutcome::Held { applicable } if applicable > 0
                                )
                        })
                    });
                if !held {
                    return blocked(format!(
                        "the trace doesn't show {property} held on an application"
                    ));
                }
            }
            verdict
        }
        Task::Agreement {
            harness,
            base_input,
            plan_sha256,
            fixed_scenario,
            site,
            ..
        } => {
            for side in ["base", "fixed"] {
                if !exited(json(&format!("record-{side}.json"))) {
                    return blocked(format!("the {side} run's witness recording didn't exit 0"));
                }
                if let Some(why) =
                    replay_refusal(dir, (side, &format!("replay-{side}.json")), binding, bound)
                {
                    return blocked(why);
                }
            }
            // Each run passes through the one judge: the planted race must
            // violate its property, and the fix must hold.
            let (violates, passes) = (
                RunExpect::Violates {
                    property: harness.clone(),
                    also: Vec::new(),
                },
                RunExpect::Passes,
            );
            if let Some(why) = runs_refusal(
                dir,
                binding,
                harness,
                &[
                    ("record-base", &violates, Some(base_input.as_str())),
                    ("replay-base", &violates, Some(base_input.as_str())),
                    // The fixed run is Plan bytes on a scenario, not a full input.
                    ("record-fixed", &passes, None),
                    ("replay-fixed", &passes, None),
                ],
            ) {
                return blocked(why);
            }
            let base = judge_trace(
                &dir.join("base"),
                bound,
                (harness, Some(base_input)),
                &RunExpect::Violates {
                    property: harness.clone(),
                    also: Vec::new(),
                },
                applicable,
            );
            if base != Verdict::Verified {
                return base;
            }
            let fixed = judge_trace(
                &dir.join("fixed"),
                bound,
                (harness, None),
                &RunExpect::Passes,
                applicable,
            );
            if fixed != Verdict::Verified {
                return fixed;
            }
            // The fixed run is the same Plan bytes on the fixed scenario.
            let same_plan = TraceReader::open(&dir.join("fixed"))
                .ok()
                .is_some_and(|trace| {
                    trace.meta.plan.as_ref().map(|plan| plan.sha256.as_str())
                        == Some(plan_sha256.as_str())
                        && serde_json::from_str::<Value>(fixed_scenario).ok()
                            == Some(trace.meta.scenario.clone())
                });
            if !same_plan {
                return blocked(
                    "the fixed run isn't the saved schedule's Plan bytes on the fix".to_owned(),
                );
            }
            // A race's fix is another code path, not a patch, so the two
            // runs compare without source lines.
            let views = |side: &str| {
                super::agreement::step_views(&dir.join(side)).map(super::agreement::without_lines)
            };
            match (views("base"), views("fixed")) {
                (Ok(base), Ok(fixed)) => match super::agreement::agreement(
                    &base,
                    &fixed,
                    &super::agreement::HunkMap::default(),
                    &super::agreement::Site { at: site.clone() },
                ) {
                    Ok(_) => Verdict::Verified,
                    Err(why) => blocked(why),
                },
                (Err(why), _) | (_, Err(why)) => blocked(why),
            }
        }
        Task::Planted {
            harness,
            input_sha256,
            ..
        } => judge_planted(dir, binding, bound, harness, input_sha256, applicable),
        Task::Target {
            name,
            harness,
            seeds,
            ..
        } => judge_target(dir, binding, bound, name, harness, seeds),
        Task::Minimize {
            property,
            harness,
            target,
            input_sha256,
            ..
        } => judge_minimize(
            dir,
            binding,
            bound,
            (property, harness, target),
            input_sha256,
        ),
        Task::Command { args, sim, .. } => match json("command.json") {
            Some(command)
                if command.get("args") != Some(&serde_json::json!(args))
                    || command.get("sim").and_then(Value::as_bool) != Some(*sim)
                    || command.get("checkout_source").and_then(Value::as_str) != Some(source) =>
            {
                blocked("command.json isn't this command's run from S".to_owned())
            }
            Some(command) => command_passed(&command, args),
            None => blocked("the command left no command.json".to_owned()),
        },
        Task::Measure {
            harness,
            input,
            violates,
            ..
        } => {
            let Some(measure) = own("measure.json") else {
                return blocked("the recording left no measure.json".to_owned());
            };
            if !exited(json("record.json")) {
                return blocked("the measured recording didn't exit 0".to_owned());
            }
            let trace = dir.join("trace");
            let ended = TraceReader::open(&trace)
                .ok()
                .and_then(|trace| trace.end.map(|end| end.steps));
            let bytes = fs::metadata(trace.join(crate::trace::TRACE_FILE))
                .ok()
                .map(|meta| meta.len());
            let whole = ["wall_ns", "steps_per_sec", "stack_pool"]
                .iter()
                .all(|field| measure.get(*field).is_some());
            if measure.get("steps").and_then(Value::as_u64) != ended
                || measure.get("trace_bytes").and_then(Value::as_u64) != bytes
                || !whole
            {
                return blocked("measure.json doesn't match its trace".to_owned());
            }
            let expect =
                violates
                    .as_ref()
                    .map_or(RunExpect::Passes, |property| RunExpect::Violates {
                        property: property.clone(),
                        also: Vec::new(),
                    });
            let planned = Some(input.as_deref().unwrap_or(EMPTY_SHA256));
            if let Some(why) = runs_refusal(dir, binding, harness, &[("record", &expect, planned)])
            {
                return blocked(why);
            }
            judge_trace(
                &trace,
                bound,
                (harness, Some(input.as_deref().unwrap_or(EMPTY_SHA256))),
                &violates
                    .as_ref()
                    .map_or(RunExpect::Passes, |property| RunExpect::Violates {
                        property: property.clone(),
                        also: Vec::new(),
                    }),
                applicable,
            )
        }
    }
}

/// A test result passes only if its binary ran, exactly one test ran, and
/// it was `test` and passed.
fn exactly_one_passed(result: &Value, test: &str) -> Verdict {
    let ran: Vec<(String, String)> = result
        .get("tests")
        .and_then(|tests| serde_json::from_value(tests.clone()).ok())
        .unwrap_or_default();
    let exited = result.get("exited_ok").and_then(Value::as_bool) == Some(true);
    match &ran[..] {
        [(name, outcome)] if exited && test_name(name) == test && outcome == "ok" => {
            Verdict::Verified
        }
        _ => Verdict::Blocked {
            why: format!("expected {test} alone to run and pass, but ran {ran:?}"),
        },
    }
}

/// A test's name as `--list` gives it: libtest reports a `#[should_panic]`
/// test's result as `name - should panic`.
fn test_name(reported: &str) -> &str {
    reported.strip_suffix(" - should panic").unwrap_or(reported)
}

/// What a lane's runner records: its command's outcome, every test each
/// target ran with its outcome, every non-ignored test each target's binary
/// listed before the run, and each target whose listing failed.
#[derive(Debug, Default, Deserialize)]
struct LaneRun {
    #[serde(default)]
    exited_ok: bool,
    #[serde(default)]
    targets: BTreeMap<String, Vec<(String, String)>>,
    #[serde(default)]
    listed: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    list_failures: Vec<String>,
}

/// Why a lane's artifacts aren't bound to S, if they aren't: the lane must
/// have run `command` from this run's checkout, and every `zakura-sim` test binary,
/// which embeds the simulator's source digest, must embed S's.
fn lane_bound(lane: &Value, source: &str, command: &str) -> Option<String> {
    if lane.pointer("/binding/command").and_then(Value::as_str) != Some(command) {
        return Some("lane.json isn't a run of this lane's command".to_owned());
    }
    if lane.get("checkout_source").and_then(Value::as_str) != Some(source) {
        return Some("the lane didn't run from this run's checkout of S".to_owned());
    }
    let sources: BTreeMap<String, String> = lane
        .get("binary_sources")
        .and_then(|sources| serde_json::from_value(sources.clone()).ok())
        .unwrap_or_default();
    let binaries: BTreeMap<String, String> = lane
        .get("binaries")
        .and_then(|binaries| serde_json::from_value(binaries.clone()).ok())
        .unwrap_or_default();
    let mut targets: BTreeSet<String> = BTreeSet::new();
    for field in ["listed", "targets"] {
        if let Some(map) = lane.get(field).and_then(Value::as_object) {
            targets.extend(map.keys().cloned());
        }
    }
    if let Some(target) = targets
        .iter()
        .find(|target| !binaries.contains_key(*target) || !sources.contains_key(*target))
    {
        return Some(format!("{target} has no recorded test binary"));
    }
    binaries
        .keys()
        .filter(|target| target.starts_with("zakura-sim/"))
        .find(|target| sources.get(*target).map(String::as_str) != Some(source))
        .map(|target| format!("{target}'s test binary doesn't embed S's source digest"))
}

/// A lane passes when its command exited 0, its binaries listed cleanly
/// before it ran, every required target was built and every test it lists
/// ran and passed, every expected-empty target listed none, every `run =
/// true` ignored test ran and passed, and no test failed (Rule 11).
fn lane_passed(
    lane: &Value,
    required: &[String],
    empty: &[String],
    ignored_run: &[String],
) -> Verdict {
    let blocked = |why: String| Verdict::Blocked { why };
    let run: LaneRun = serde_json::from_value(lane.clone()).unwrap_or_default();
    if !run.exited_ok {
        return blocked("the lane's command failed".to_owned());
    }
    if let Some(target) = run.list_failures.first() {
        return blocked(format!("{target}'s binary didn't list its tests"));
    }
    // Each atom must pass exactly once: a duplicate run is unaccounted for.
    let passed = |target: &str, name: &str| {
        run.targets.get(target).is_some_and(|tests| {
            tests
                .iter()
                .filter(|(n, outcome)| test_name(n) == name && outcome == "ok")
                .count()
                == 1
        })
    };
    for target in required {
        let Some(listed) = run.listed.get(target) else {
            return blocked(format!("required target {target} wasn't built"));
        };
        if listed.is_empty() {
            return blocked(format!("required target {target} lists no test"));
        }
    }
    for (target, listed) in &run.listed {
        if let Some(name) = listed.iter().find(|name| !passed(target, name)) {
            return blocked(format!(
                "{target}::{name} is listed but didn't run and pass"
            ));
        }
    }
    for target in empty {
        let Some(listed) = run.listed.get(target) else {
            return blocked(format!(
                "expected-empty target {target} wasn't built and listed"
            ));
        };
        let ran = run.targets.get(target).is_some_and(|t| !t.is_empty());
        if !listed.is_empty() || ran {
            return blocked(format!("{target} is expected empty but has tests"));
        }
    }
    // Every test that ran is a listed atom or a `run = true` ignored test.
    for (target, tests) in &run.targets {
        let listed = run.listed.get(target);
        let unaccounted = tests.iter().find(|(name, outcome)| {
            let name = test_name(name);
            outcome != "ignored"
                && !listed.is_some_and(|listed| listed.iter().any(|l| l == name))
                && !ignored_run.contains(&format!("{target}::{name}"))
        });
        if let Some((name, _)) = unaccounted {
            return blocked(format!("{target}::{name} ran but wasn't listed"));
        }
    }
    for test in ignored_run {
        let (target, name) = test.split_once("::").unwrap_or((test, ""));
        if !passed(target, name) {
            return blocked(format!(
                "{test} is marked `run = true` but didn't run and pass"
            ));
        }
    }
    for (target, tests) in &run.targets {
        if let Some((name, outcome)) = tests.iter().find(|(_, outcome)| outcome == "FAILED") {
            return blocked(format!("{target}: {name} {outcome}"));
        }
    }
    Verdict::Verified
}

/// An S4.6 command passes when it exited 0, and the dependency-graph one
/// also when its output shows the node's `zakura` root and none of the
/// simulator crates the lint step denies.
fn command_passed(command: &Value, args: &[String]) -> Verdict {
    let exited = command.get("exited_ok").and_then(Value::as_bool) == Some(true);
    let output = command
        .get("log")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if !exited {
        return Verdict::Blocked {
            why: format!("`cargo {}` failed", args.join(" ")),
        };
    }
    if args.first().map(String::as_str) == Some("tree") {
        if !output.lines().any(|line| line.starts_with("zakura v")) {
            return Verdict::Blocked {
                why: "`cargo tree`'s output has no zakura root".to_owned(),
            };
        }
        if let Some(denied) = [
            "zakura-scheduler ",
            "zakura-sim ",
            "zakura-storage ",
            "corosensei ",
        ]
        .iter()
        .find(|name| output.lines().any(|line| line.starts_with(*name)))
        {
            return Verdict::Blocked {
                why: format!("the production graph has {}", denied.trim()),
            };
        }
    }
    Verdict::Verified
}

/// Whether a child's `<name>.json` says it exited 0.
fn exited(child: Option<Value>) -> bool {
    child.and_then(|c| c.get("exited_ok").and_then(Value::as_bool)) == Some(true)
}

/// Judges the trace in `dir` with `owned_run_passes`, taking the
/// registered properties and their statuses from the harness registry at
/// S, not from the trace. It must be a run of `binding`'s harness, on its
/// full input's SHA-256 where one is given, recorded by the release
/// executable the run kept, from a build that embeds S's source. A passing
/// run's applicable counts are added to `applicable`.
fn judge_trace(
    dir: &Path,
    bound: &Bound<'_>,
    binding: (&str, Option<&str>),
    expect: &RunExpect,
    applicable: &mut u64,
) -> Verdict {
    let trace = match TraceReader::open(dir) {
        Ok(trace) => trace,
        Err(error) => {
            return Verdict::Blocked {
                why: format!("the trace doesn't read: {error}"),
            }
        }
    };
    let (harness, input) = binding;
    if trace.meta.sim != harness {
        return Verdict::Blocked {
            why: format!("the trace is a run of {}, not {harness}", trace.meta.sim),
        };
    }
    if let Some(input) = input {
        if trace.meta.input.as_ref().map(|i| i.sha256.as_str()) != Some(input) {
            return Verdict::Blocked {
                why: format!("the trace isn't a run of the input {input}"),
            };
        }
    }
    let source = bound.source;
    if bound.release.as_deref() != Some(trace.meta.build.binary.as_str()) {
        return Verdict::Blocked {
            why: format!(
                "the trace was recorded by {}, not the run's release zakura-sim",
                trace.meta.build.binary
            ),
        };
    }
    if trace.meta.build.source != source {
        return Verdict::Blocked {
            why: format!(
                "the trace's build has source {}, not S's {source}",
                trace.meta.build.source
            ),
        };
    }
    let Some(end) = &trace.end else {
        return Verdict::Blocked {
            why: "the trace has no end record".to_owned(),
        };
    };
    let Ok(entry) = crate::harness::find(harness) else {
        return Verdict::Blocked {
            why: format!("{harness} isn't a harness at S"),
        };
    };
    let registered = registered(entry);
    match owned_run_passes(end, &registered, expect) {
        Ok(()) => {
            *applicable += end
                .properties
                .iter()
                .map(|report| match report.outcome {
                    PropertyOutcome::Held { applicable } => u64::from(applicable),
                    _ => 0,
                })
                .sum::<u64>();
            Verdict::Verified
        }
        Err(blocked) => Verdict::Blocked {
            why: blocked.to_string(),
        },
    }
}

/// Whether the trace in `trace` ends exactly as the committed `file` says.
fn end_record_matches(repo: &Path, file: &str, trace: &Path) -> Verdict {
    let committed = fs::read_to_string(repo.join(file)).unwrap_or_default();
    let recorded = fs::read_to_string(trace.join(crate::trace::TRACE_FILE))
        .ok()
        .and_then(|text| text.lines().last().map(str::to_owned))
        .unwrap_or_default();
    if !committed.trim().is_empty() && committed.trim() == recorded.trim() {
        Verdict::Verified
    } else {
        Verdict::Blocked {
            why: "the seed's run didn't reproduce its committed end record".to_owned(),
        }
    }
}

/// hdr-14's published record at `header.out`, and why it can't be
/// consumed at `subject`, if it can't: its facts, then whether `verify`
/// binds it to its run's plan and artifacts.
fn header_record(
    repo: &Path,
    header: &HeaderRun<'_>,
    subject: &str,
    subject_source: &str,
) -> (Option<String>, Option<Record>) {
    let record: Record = match bounded_json(&header.out.join("record.json"))
        .and_then(|value| serde_json::from_value(value).ok())
    {
        Some(record) => record,
        None => return (Some("the header record doesn't read".to_owned()), None),
    };
    let artifacts = header.out.join(&record.artifacts);
    let run_dir = artifacts.parent().unwrap_or(header.out).to_owned();
    let run_id = run_dir
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let verified = header::read_bounded_json::<RunPlan>(&run_dir.join("plan.json"))
        .and_then(|plan| {
            verify(
                repo,
                &Published {
                    plan: &plan,
                    artifacts: &artifacts,
                    out: header.out,
                    run_id: &run_id,
                    subject_source,
                    record: &record,
                },
            )
        })
        .and_then(|results| consumed_cleanup(&results, &artifacts));
    let facts = HeaderRecordFacts {
        subject: record.subject.clone(),
        subreaper: record.containment == Containment::Subreaper,
        accepted: record.accepted,
        stray: record.stray.clone(),
        verified,
    };
    (header_refusal(&facts, subject), Some(record))
}

/// `key`'s verdict in an accepted header record.
fn header_key_verdict(record: &Record, key: &crate::harness::header_acceptance::Key) -> Verdict {
    if record.verified.iter().any(|entry| &entry.key == key) {
        Verdict::Verified
    } else if let Some(entry) = record.excluded.iter().find(|entry| &entry.key == key) {
        Verdict::Excluded {
            approval: entry.why.clone().unwrap_or_default(),
        }
    } else {
        Verdict::Blocked {
            why: "the header record doesn't verify this key".to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{command_passed, exactly_one_passed, lane_bound, lane_passed, Verdict};

    fn blocked(verdict: &Verdict) -> bool {
        matches!(verdict, Verdict::Blocked { .. })
    }

    /// A test row passes only when its one test ran alone and passed.
    #[test]
    fn a_test_row_needs_exactly_its_test() {
        let one = json!({ "exited_ok": true, "tests": [["t", "ok"]] });
        assert_eq!(exactly_one_passed(&one, "t"), Verdict::Verified);
        let two = json!({ "exited_ok": true, "tests": [["t", "ok"], ["u", "ok"]] });
        assert!(blocked(&exactly_one_passed(&two, "t")));
        let failed = json!({ "exited_ok": false, "tests": [["t", "FAILED"]] });
        assert!(blocked(&exactly_one_passed(&failed, "t")));
        let other = json!({ "exited_ok": true, "tests": [["u", "ok"]] });
        assert!(blocked(&exactly_one_passed(&other, "t")));
    }

    /// A lane blocks on a failed command or listing, a required target
    /// that wasn't built or lists nothing, a listed test that didn't run and
    /// pass (a `--skip`), a nonempty expected-empty target, an unrun
    /// `run = true` ignored test, or any failure.
    #[test]
    fn a_lane_needs_its_whole_inventory() {
        let required = ["zakura-sim/draws".to_owned()];
        let empty = ["zakura-rt/lib".to_owned()];
        let none: [String; 0] = [];
        let lane = |extra: serde_json::Value| {
            let mut lane = json!({
                "exited_ok": true,
                "listed": { "zakura-sim/draws": ["a"], "zakura-rt/lib": [] },
                "targets": { "zakura-sim/draws": [["a", "ok"], ["slow", "ignored"]] },
                "list_failures": [],
            });
            for (key, value) in extra.as_object().unwrap() {
                lane[key] = value.clone();
            }
            lane
        };
        let judge = |lane: &serde_json::Value, ignored_run: &[String]| {
            lane_passed(lane, &required, &empty, ignored_run)
        };
        assert_eq!(judge(&lane(json!({})), &none), Verdict::Verified);
        assert!(blocked(&judge(&lane(json!({ "exited_ok": false })), &none)));
        assert!(blocked(&judge(
            &lane(json!({ "list_failures": ["zakura-sim/draws"] })),
            &none
        )));
        assert!(blocked(&judge(
            &lane(json!({ "listed": { "zakura-rt/lib": [] } })),
            &none
        )));
        let skipped =
            lane(json!({ "listed": { "zakura-sim/draws": ["a", "b"], "zakura-rt/lib": [] } }));
        assert!(
            blocked(&judge(&skipped, &none)),
            "a --skip of one listed test blocks"
        );
        let nonempty =
            lane(json!({ "listed": { "zakura-sim/draws": ["a"], "zakura-rt/lib": ["x"] } }));
        assert!(blocked(&judge(&nonempty, &none)));
        assert!(blocked(&judge(
            &lane(json!({})),
            &["zakura-sim/draws::slow".to_owned()]
        )));
        let twice = lane(json!({ "targets": { "zakura-sim/draws": [["a", "ok"], ["a", "ok"]] } }));
        assert!(
            blocked(&judge(&twice, &none)),
            "a test that ran twice blocks"
        );
        let extra = lane(json!({ "targets": { "zakura-sim/draws": [["a", "ok"], ["z", "ok"]] } }));
        assert!(
            blocked(&judge(&extra, &none)),
            "an unlisted test that ran blocks"
        );
        let unbuilt = lane(json!({ "listed": { "zakura-sim/draws": ["a"] } }));
        assert!(
            blocked(&judge(&unbuilt, &none)),
            "an unlisted expected-empty target blocks"
        );
        let ran_empty = lane(json!({ "targets": {
            "zakura-sim/draws": [["a", "ok"]], "zakura-rt/lib": [["hidden", "ok"]] } }));
        assert!(
            blocked(&judge(&ran_empty, &none)),
            "an expected-empty target that runs a test blocks"
        );
        let panics =
            lane(json!({ "targets": { "zakura-sim/draws": [["a - should panic", "ok"]] } }));
        assert_eq!(
            judge(&panics, &none),
            Verdict::Verified,
            "a should-panic test counts as run"
        );
        let failed =
            lane(json!({ "targets": { "zakura-sim/draws": [["a", "ok"], ["c", "FAILED"]] } }));
        assert!(blocked(&judge(&failed, &none)));
    }

    /// A lane binds to S only when every target it listed or ran names its
    /// test binary, and each `zakura-sim` binary embeds S's digest.
    #[test]
    fn a_lane_needs_a_binary_for_every_target() {
        let bound = json!({
            "binding": { "command": "cargo test" },
            "checkout_source": "s",
            "listed": { "zakura-sim/draws": ["a"] },
            "targets": { "zakura-sim/draws": [["a", "ok"]] },
            "binaries": { "zakura-sim/draws": "d" },
            "binary_sources": { "zakura-sim/draws": "s" },
        });
        assert_eq!(lane_bound(&bound, "s", "cargo test"), None);
        assert!(
            lane_bound(&bound, "s", "cargo test --release").is_some(),
            "another lane's run blocks"
        );
        for field in ["binaries", "binary_sources"] {
            let mut stripped = bound.clone();
            stripped.as_object_mut().unwrap().remove(field);
            assert!(
                lane_bound(&stripped, "s", "cargo test").is_some(),
                "no {field} blocks"
            );
        }
        let mut other = bound.clone();
        other["binary_sources"]["zakura-sim/draws"] = json!("not-embedded");
        assert!(lane_bound(&other, "s", "cargo test").is_some());
        assert!(
            lane_bound(&bound, "t", "cargo test").is_some(),
            "another checkout blocks"
        );
    }

    /// A runner file counts only for its own run and task: the same task's
    /// file from another run of the same plan is refused.
    #[test]
    fn a_runner_file_from_another_run_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let binding = |run: &str| super::TaskBinding {
            run: run.to_owned(),
            index: 3,
            task_sha256: "t".to_owned(),
        };
        let file = json!({
            "exited_ok": true,
            "binding": { "run": "run-a", "task": 3, "task_sha256": "t" },
        });
        std::fs::write(dir.path().join("lane.json"), file.to_string()).unwrap();
        assert!(super::runner_json(dir.path(), "lane.json", &binding("run-a")).is_some());
        assert!(super::runner_json(dir.path(), "lane.json", &binding("run-b")).is_none());
    }

    /// The planted runner's runs pass as it writes them: one baseline and
    /// its replay that hold, one patched run and its replay that violate,
    /// and each listed campaign input once. A replay of the wrong outcome,
    /// a missing or doubled one, an old-style `replay`, a case run of
    /// another input, or an unlisted or repeated campaign input blocks.
    #[test]
    fn the_planted_runs_are_judged_by_phase() {
        use super::{planted_judged, PlantedRun};
        let run = |phase: &'static str, input: &'static str, holds: bool| PlantedRun {
            stem: phase,
            phase,
            input: Some(input),
            held: if holds {
                Ok(())
            } else {
                Err("violated".to_owned())
            },
            caught: if holds {
                Err("held".to_owned())
            } else {
                Ok(())
            },
        };
        let campaign = ["c1".to_owned()];
        let written = || {
            vec![
                run("baseline", "case", true),
                run("replay-baseline", "case", true),
                run("patched", "case", false),
                run("replay-patched", "case", false),
                run("campaign", "c1", true),
            ]
        };
        assert_eq!(planted_judged(&written(), "case", &campaign), None);
        let mut violated_baseline = written();
        violated_baseline[1] = run("replay-baseline", "case", false);
        assert!(planted_judged(&violated_baseline, "case", &campaign).is_some());
        let mut missing = written();
        missing.remove(3);
        assert!(planted_judged(&missing, "case", &campaign).is_some());
        let mut doubled = written();
        doubled.push(run("replay-patched", "case", false));
        assert!(planted_judged(&doubled, "case", &campaign).is_some());
        let mut old_style = written();
        old_style[3] = run("replay", "case", false);
        assert!(planted_judged(&old_style, "case", &campaign).is_some());
        let mut unlisted = written();
        unlisted[4] = run("campaign", "c2", true);
        assert!(planted_judged(&unlisted, "case", &campaign).is_some());
        let mut held_patched = written();
        held_patched[3] = run("replay-patched", "case", true);
        assert!(planted_judged(&held_patched, "case", &campaign).is_some());
        let mut other_input = written();
        other_input[0] = run("baseline", "c1", true);
        assert!(planted_judged(&other_input, "case", &campaign).is_some());
        let mut campaign_twice = written();
        campaign_twice.push(run("campaign", "c1", true));
        assert!(planted_judged(&campaign_twice, "case", &campaign).is_some());
    }

    /// An artifact tree holding a link isn't read.
    #[cfg(unix)]
    #[test]
    fn an_artifact_tree_with_a_link_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("task.json"), "{}").unwrap();
        assert_eq!(super::header::tree_refusal(dir.path()), None);
        std::os::unix::fs::symlink("/dev/zero", dir.path().join("trace.jsonl")).unwrap();
        assert!(super::header::tree_refusal(dir.path()).is_some());
    }

    /// `cargo tree` passes only on exit 0, with the zakura root, and no
    /// simulator crate in the graph.
    #[test]
    fn the_dependency_graph_row_reads_cargo_tree_itself() {
        let tree = ["tree".to_owned()];
        let clean =
            json!({ "exited_ok": true, "log": "zakura v5.0.0 (/repo)\nzakura-chain v5.0.0\n" });
        assert_eq!(command_passed(&clean, &tree), Verdict::Verified);
        let failed = json!({ "exited_ok": false, "log": "" });
        assert!(blocked(&command_passed(&failed, &tree)));
        let empty = json!({ "exited_ok": true, "log": "" });
        assert!(
            blocked(&command_passed(&empty, &tree)),
            "no root means it checked nothing"
        );
        let sim = json!({ "exited_ok": true, "log": "zakura v5.0.0\nzakura-sim v0.0.0\n" });
        assert!(blocked(&command_passed(&sim, &tree)));
    }
}
