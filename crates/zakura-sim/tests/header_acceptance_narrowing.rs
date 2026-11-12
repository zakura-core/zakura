//! The narrowing entries' tests (hdr-14 Rule 24, hdr-14.7b).
//!
//! A [`NARROWING`] entry releases results from a contract only once its
//! test shows those results don't read that contract's evidence. Each test
//! here takes the released results from the committed manifest through the
//! entry's own `releases` and the evaluator's own context, so a widened
//! release is tested on what it now releases, and fails if a released
//! result's runs carry the contract's evidence. Each also checks a result
//! the entry doesn't release does carry it, so the check can see it.
//!
//! N1, N3 and N4 check every released result's committed inputs: each
//! planted case's input, from the acceptance plan's own task, and every
//! committed fixture of a harness whose histories they release. A history's
//! own scenario and Plan are built by its witness test and declared only
//! when the acceptance runner runs it (C18), so each of those entries also
//! proves a structural rule over the simulator's production source that
//! holds for every input of a released harness.
#![cfg(zakura_sim)]

use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Range,
    path::{Path, PathBuf},
};

use serde_json::Value;
use zakura_sim::{
    harness::{
        self,
        header_acceptance::{
            contracts::{Narrowing, RunBy},
            evaluate::{result_context, End, Execution, ResultFile, ResultRecord},
            rows::HEADER_HARNESSES,
            tool, CaseRun, Expected, ExpectedResult, Gate, HistoryRef, Key, Manifest,
            ObligationState, ResultKind, RunId, Subject, NARROWING, NATIVE_DIR,
        },
        HarnessEntry, HarnessError,
    },
    record::{FullReport, RecordSink},
    Plan, PlanInput,
};

/// The committed narrowing entry named `name`.
fn entry(name: &str) -> &'static Narrowing {
    NARROWING
        .iter()
        .find(|entry| entry.entry == name)
        .unwrap_or_else(|| panic!("{name} is a committed entry"))
}

/// Every committed expected result `entry` releases, by the evaluator's
/// own context for it.
fn released<'m>(manifest: &'m Manifest, entry: &Narrowing) -> Vec<&'m ExpectedResult> {
    manifest
        .expected
        .iter()
        .filter(|result| (entry.releases)(&result_context(manifest, &result.expected)))
        .collect()
}

/// The harnesses of the committed results `entry` releases, and of those
/// it keeps, each among the header harnesses.
fn harnesses(manifest: &Manifest, entry: &Narrowing) -> (BTreeSet<String>, BTreeSet<String>) {
    let (mut released, mut kept) = (BTreeSet::new(), BTreeSet::new());
    for result in &manifest.expected {
        let ctx = result_context(manifest, &result.expected);
        let Some(harness) = ctx.harness.clone() else {
            continue;
        };
        if !HEADER_HARNESSES.contains(&harness.as_str()) {
            continue;
        }
        if (entry.releases)(&ctx) {
            released.insert(harness);
        } else {
            kept.insert(harness);
        }
    }
    (released, kept)
}

/// The repository root.
fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// The committed fixture inputs of `harness`, by their sidecars, sorted.
fn fixtures(harness: &str) -> Vec<PathBuf> {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let mut found: Vec<PathBuf> = std::fs::read_dir(&fixtures)
        .expect("the fixtures directory is readable")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "toml"))
        .filter(|path| {
            std::fs::read_to_string(path)
                .ok()
                .and_then(|text| toml::from_str::<toml::Table>(&text).ok())
                .is_some_and(|table| {
                    table.get("harness").and_then(toml::Value::as_str) == Some(harness)
                })
        })
        .map(|path| path.with_extension("bin"))
        .filter(|path| path.exists())
        .collect();
    found.sort();
    found
}

/// One committed input a result's execution runs on: its harness, what
/// names it, its scenario and its Plan.
#[derive(Clone)]
struct Input {
    /// The harness it runs on.
    harness: String,
    /// Its committed files, relative to the repository root.
    name: String,
    /// Its scenario, as the harness's JSON.
    scenario: Value,
    /// Its Plan.
    plan: PlanInput,
}

impl Input {
    /// The full fuzz input at `path` on `harness`, split as `record` splits
    /// it. An empty input when `path` is `None`.
    fn fuzz(harness: &str, path: Option<&Path>) -> Input {
        let entry = harness::find(harness).expect("the harness is registered");
        let bytes = path.map_or_else(Vec::new, |path| {
            std::fs::read(repo().join(path)).expect("a committed input is readable")
        });
        let split = entry.scenario_bytes.min(bytes.len());
        Input {
            harness: harness.to_owned(),
            name: path.map_or("empty input".to_owned(), |path| path.display().to_string()),
            scenario: entry.scenario(&bytes),
            plan: PlanInput::Bytes(bytes[split..].to_vec()),
        }
    }

    /// A planted case's committed input on `harness`, as its task names it.
    fn case(harness: &str, input: &tool::CaseInput) -> Input {
        let read = |path: &str| {
            std::fs::read_to_string(repo().join(path))
                .unwrap_or_else(|error| panic!("{path}: {error}"))
        };
        let json = |path: &str| -> Value {
            serde_json::from_str(&read(path)).unwrap_or_else(|error| panic!("{path}: {error}"))
        };
        match input {
            tool::CaseInput::Fuzz { input } => Input::fuzz(harness, Some(Path::new(input))),
            tool::CaseInput::Typed { scenario, plan } => Input {
                harness: harness.to_owned(),
                name: format!("{scenario} with {plan}"),
                scenario: json(scenario),
                plan: PlanInput::Bytes(
                    std::fs::read(repo().join(plan)).expect("a committed Plan is readable"),
                ),
            },
            tool::CaseInput::Scripted { scenario, script } => Input {
                harness: harness.to_owned(),
                name: format!("{scenario} with {script}"),
                scenario: json(scenario),
                plan: PlanInput::Script(
                    serde_json::from_value::<Plan>(json(script)).expect("a committed script"),
                ),
            },
        }
    }
}

/// Every execution `expected` counts, by its identity: a replay's run, a
/// history, each run of a planted case, or each run of a native mutation.
/// A result with no execution has none, and no entry releases it.
fn runs(expected: &Expected) -> Vec<RunId> {
    match *expected {
        Expected::Replays { ref run } => vec![run.clone()],
        Expected::Holds { history, .. } | Expected::StaysNotApplicable { history, .. } => {
            vec![history.into()]
        }
        Expected::Caught { case, .. } => [CaseRun::Baseline, CaseRun::Patched, CaseRun::Draft]
            .map(|run| RunId::case(case, run))
            .to_vec(),
        Expected::MachineCaught { case, .. } => [CaseRun::Baseline, CaseRun::Patched]
            .map(|run| RunId::case(case, run))
            .to_vec(),
        Expected::IntegrityCaught { patch, .. } => {
            [CaseRun::Unpatched, CaseRun::Patched, CaseRun::Restored]
                .map(|run| RunId::native(patch, run))
                .to_vec()
        }
        Expected::Passes { .. } | Expected::Open { .. } => Vec::new(),
    }
}

/// Every committed input the results `entry` releases run on, each once: a
/// planted case's run reads the input its task in `plan` names, and a
/// history's run, or a native mutation's, every committed fixture of its
/// harness, or an empty input when it has none. A history's own scenario
/// and Plan are left to the entry's structural rule.
fn released_inputs(manifest: &Manifest, plan: &tool::RunPlan, entry: &Narrowing) -> Vec<Input> {
    let mut inputs = BTreeMap::new();
    for result in released(manifest, entry) {
        let harness = result_context(manifest, &result.expected)
            .harness
            .expect("a released result has executions, on a harness");
        assert!(HEADER_HARNESSES.contains(&harness.as_str()), "{harness}");
        for run in runs(&result.expected) {
            let found = match &run {
                RunId::Case { property, name, .. } if property != NATIVE_DIR => {
                    let input = plan
                        .tasks
                        .iter()
                        .find_map(|task| match task {
                            tool::Task::Case {
                                property: p,
                                name: n,
                                input,
                                ..
                            } if (p, n) == (property, name) => Some(input),
                            _ => None,
                        })
                        .unwrap_or_else(|| panic!("{run:?} has a task"));
                    vec![Input::case(&harness, input)]
                }
                _ => {
                    let paths = fixtures(&harness);
                    if paths.is_empty() {
                        vec![Input::fuzz(&harness, None)]
                    } else {
                        let root = repo().canonicalize().expect("the repository resolves");
                        paths
                            .iter()
                            .map(|path| {
                                let path = path.canonicalize().expect("a fixture resolves");
                                Input::fuzz(&harness, Some(path.strip_prefix(&root).unwrap()))
                            })
                            .collect()
                    }
                }
            };
            for input in found {
                inputs
                    .entry((input.harness.clone(), input.name.clone()))
                    .or_insert(input);
            }
        }
    }
    inputs.into_values().collect()
}

/// The first of `inputs` whose run `carries` finds the contract's evidence
/// in, named with what it found, or `None` if no run carries any. Every
/// input runs, not just the first.
fn first_carrying(inputs: &[Input], carries: impl Fn(&Shown) -> Option<String>) -> Option<String> {
    inputs.iter().find_map(|input| {
        let found = carries(&run_input(input))?;
        Some(format!("{} on {}: {found}", input.harness, input.name))
    })
}

/// The simulator's production source files under `src/`, by their path
/// from the package root, each cut before its inline `#[cfg(test)] mod
/// tests` block. A test module's own file, `tests.rs` or one under a
/// `tests` directory, is left out.
fn production_sources() -> Vec<(String, String)> {
    fn walk(dir: &Path, root: &Path, out: &mut Vec<(String, String)>) {
        let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)
            .expect("the source directory is readable")
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .collect();
        entries.sort();
        for path in entries {
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("");
            if path.is_dir() {
                if name != "tests" {
                    walk(&path, root, out);
                }
            } else if name.ends_with(".rs") && name != "tests.rs" {
                let text = std::fs::read_to_string(&path).expect("a source file is readable");
                let text = match text.find("#[cfg(test)]\nmod tests {") {
                    Some(cut) => text[..cut].to_owned(),
                    None => text,
                };
                let relative = path.strip_prefix(root).expect("under the package root");
                out.push((relative.display().to_string(), text));
            }
        }
    }
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut out = Vec::new();
    walk(&root.join("src"), root, &mut out);
    out
}

/// Each line of `sources` that holds `needle` and that `allowed` doesn't
/// admit, as `path:line: text`. `allowed` gets the file's path, its text
/// and the line's index.
fn mentions_outside(
    sources: &[(String, String)],
    needle: &str,
    allowed: impl Fn(&str, &str, usize) -> bool,
) -> Vec<String> {
    sources
        .iter()
        .flat_map(|(path, text)| {
            text.lines()
                .enumerate()
                .filter(|(_, line)| line.contains(needle))
                .filter(|(index, _)| !allowed(path, text, *index))
                .map(move |(index, line)| format!("{path}:{}: {}", index + 1, line.trim()))
                .collect::<Vec<_>>()
        })
        .collect()
}

/// The lines of `text` from the one that starts with `header` up to the
/// next line that is only `}`, as indices, or an empty range if there's no
/// such line.
fn block(text: &str, header: &str) -> Range<usize> {
    let lines: Vec<&str> = text.lines().collect();
    let Some(start) = lines.iter().position(|line| line.starts_with(header)) else {
        return 0..0;
    };
    let end = lines[start..]
        .iter()
        .position(|line| *line == "}")
        .map_or(lines.len(), |end| start + end);
    start..end
}

/// The harness names `text`'s `Subsystem` impls register, by their `NAME`
/// constants.
fn registered_names(text: &str) -> Vec<&str> {
    text.lines()
        .filter_map(|line| {
            line.trim()
                .strip_prefix("const NAME: &'static str = \"")?
                .strip_suffix("\";")
        })
        .collect()
}

/// A sink that keeps only the run's full report.
#[derive(Default)]
struct KeepReport(Option<FullReport>);

impl RecordSink for KeepReport {
    /// Drops the record.
    fn record(&mut self, _: zakura_sim::record::Record) {}

    /// Keeps the report.
    fn report(&mut self, report: &FullReport) {
        self.0 = Some(report.clone());
    }
}

/// What one run of a harness shows: each observation's kind, whether a
/// drain started, and its full report's store snapshot.
struct Shown {
    /// Each observation's kind, as it serializes it.
    kinds: BTreeSet<String>,
    /// Whether the run started a drain.
    drained: bool,
    /// The stores' snapshot, `None` for a harness that keeps none.
    snapshot: Option<Value>,
}

/// Runs `input` on its harness as `record` does and returns what it shows.
fn run_input(input: &Input) -> Shown {
    let entry: &HarnessEntry = harness::find(&input.harness).expect("the harness is registered");
    let mut sink = KeepReport::default();
    let report = match (entry.run)(&input.scenario, input.plan.clone(), &mut sink) {
        Ok(report) => report,
        Err(HarnessError::Run { report, .. }) => *report,
        Err(error) => panic!("{} on {}: {error}", input.harness, input.name),
    };
    Shown {
        kinds: report
            .log
            .entries()
            .iter()
            .filter_map(|entry| {
                serde_json::to_value(&entry.obs)
                    .ok()?
                    .get("obs")?
                    .as_str()
                    .map(str::to_owned)
            })
            .collect(),
        drained: report.end.drain.is_some(),
        snapshot: sink.0.and_then(|report| report.snapshot),
    }
}

/// The first committed fixture input of `harness`, for a kept harness's
/// positive control.
fn first_fixture(harness: &str) -> Input {
    let path = fixtures(harness).into_iter().next();
    let root = repo().canonicalize().expect("the repository resolves");
    let path = path.map(|path| {
        path.canonicalize()
            .expect("a fixture resolves")
            .strip_prefix(&root)
            .expect("under the repository")
            .to_path_buf()
    });
    Input::fuzz(harness, path.as_deref())
}

/// The observation kinds only a scripted peer, which keeps `PeerRecords`,
/// makes: what C11's peer-evidence checkpoint would stand behind.
const PEER_KINDS: &[&str] = &[
    "peer-frame",
    "peer-body",
    "peer-transport",
    "peer-written",
    "peer-closed",
    "acts-scripted",
    "act-ran",
    "act-skipped",
    "peer-received",
];

/// The peer observations in `shown`, if any.
fn peer_kinds(shown: &Shown) -> Option<String> {
    let peers: Vec<&String> = shown
        .kinds
        .iter()
        .filter(|kind| PEER_KINDS.contains(&kind.as_str()))
        .collect();
    (!peers.is_empty()).then(|| format!("observes {peers:?}"))
}

/// Where `sources` build a scripted peer's task or its `PeerRecords`
/// outside `HeaderWire`'s `Subsystem` impl and the discovery service's
/// harness, neither of which N1 releases. Every scripted peer runs as a
/// `PeerTask` and records into a `PeerRecords`, so a harness without
/// either has no peer evidence, whatever its input.
fn peers_outside_wire(sources: &[(String, String)]) -> Vec<String> {
    let allowed = |path: &str, text: &str, line: usize| {
        path == "src/harness/discovery_service.rs"
            || (path == "src/harness/header_sync.rs"
                && block(text, "impl Subsystem for HeaderWire").contains(&line))
    };
    let mut found = mentions_outside(sources, "PeerRecords::default()", allowed);
    found.extend(
        mentions_outside(sources, "PeerTask {", allowed)
            .into_iter()
            .filter(|line| !line.contains("struct PeerTask") && !line.contains("impl PeerTask")),
    );
    found
}

/// N1: no result N1 releases from C11 reads peer evidence.
///
/// Every committed input a released result runs on makes no observation a
/// scripted peer's `PeerRecords` back, and a history's own input is covered
/// by the source rule [`peers_outside_wire`] checks: only the wire harness,
/// which N1 keeps, and the discovery service, which isn't a header
/// harness, build a scripted peer. The controls: the wire harness's input
/// does make peer observations, found as the second of two inputs, and a
/// peer built in a released harness's source breaks the rule.
#[test]
fn n1_peerless_and_machine_results_read_no_peer_evidence() {
    let manifest = Manifest::committed();
    let plan = tool::plan(&manifest, &repo()).expect("the committed manifest plans");
    let n1 = entry("N1");
    let (released, kept) = harnesses(&manifest, n1);
    assert!(!released.is_empty() && kept.contains("header-sync/wire"));
    let inputs = released_inputs(&manifest, &plan, n1);
    assert!(released
        .iter()
        .all(|harness| inputs.iter().any(|input| &input.harness == harness)));
    assert_eq!(first_carrying(&inputs, peer_kinds), None);
    let wire = first_fixture("header-sync/wire");
    let found = first_carrying(&[inputs[0].clone(), wire.clone()], peer_kinds);
    assert!(
        found
            .as_deref()
            .is_some_and(|found| found.contains(&wire.name)),
        "{found:?}"
    );

    let wire_harness = "src/harness/header_sync.rs";
    let sources = production_sources();
    assert!(sources
        .iter()
        .any(|(path, text)| path == wire_harness
            && registered_names(text).contains(&"header-sync/wire")));
    let discovery = &sources
        .iter()
        .find(|(path, _)| path == "src/harness/discovery_service.rs")
        .expect("the discovery service's harness")
        .1;
    assert!(registered_names(discovery)
        .iter()
        .all(|name| !HEADER_HARNESSES.contains(name)));
    assert_eq!(peers_outside_wire(&sources), Vec::<String>::new());
    let mut planted = sources.clone();
    planted.push((
        "src/harness/header_sync/flood.rs".to_owned(),
        "    let records = PeerRecords::default();".to_owned(),
    ));
    assert_eq!(peers_outside_wire(&planted).len(), 1);
}

/// N2: no result N2 releases from C9 counts a `Draft` run, and a recorded
/// run carrying an override, which only a `Draft` recording does, is
/// refused as any released result's run. So a released result whose run
/// read the override never counts. A `Caught`, which N2 keeps, counts one.
#[test]
fn n2_released_results_carry_no_draft_override() {
    let manifest = Manifest::committed();
    let n2 = entry("N2");
    for result in released(&manifest, n2) {
        assert_ne!(result.expected.kind(), ResultKind::Caught);
        if let Expected::Replays { run } = &result.expected {
            assert!(
                !matches!(
                    run,
                    RunId::Case {
                        run: harness::header_acceptance::CaseRun::Draft,
                        ..
                    }
                ),
                "{run:?}"
            );
        }
    }
    assert!(manifest.expected.iter().any(|result| {
        result.expected.kind() == ResultKind::Caught
            && !(n2.releases)(&result_context(&manifest, &result.expected))
    }));

    // A storage history's `Holds`, which N2 releases, on a run recorded
    // with and without an override.
    const STORAGE: &str = "header-sync/storage";
    const TARGET: &str = "header-sync/storage-operation-ids";
    let history = HistoryRef {
        harness: STORAGE,
        witness: harness::header_acceptance::sim_test("tests/t.rs", "t_holds"),
        case: "only",
    };
    let holds = ExpectedResult {
        keys: vec![Key::passing(TARGET)],
        gates: vec![Gate::G1],
        expected: Expected::Holds {
            property: TARGET,
            history,
            not_applicable: &[],
        },
    };
    let small = Manifest {
        expected: vec![holds.clone()],
        contracts: Vec::new(),
        required_contracts: Vec::new(),
        narrowing: Vec::new(),
        machines: Vec::new(),
        matrix: manifest
            .matrix
            .iter()
            .copied()
            .filter(|(on, ..)| *on == STORAGE)
            .collect(),
    };
    assert!((n2.releases)(&result_context(&small, &holds.expected)));
    let input = std::fs::read(&fixtures(STORAGE)[0]).expect("a fixture is readable");
    let entry = harness::find(STORAGE).expect("the storage harness is registered");
    let state = |draft: &[String]| {
        let dir = tempfile::tempdir().expect("a temp dir can be created");
        harness::with_draft(draft, || {
            harness::record(entry, harness::RecordInput::Input(&input), dir.path())
        })
        .expect("the storage run records");
        let run: Execution = tool::read_execution(dir.path(), history.into(), STORAGE, RunBy::Tool);
        assert_ne!(run.end, End::Killed);
        assert_eq!(run.meta.draft_override, draft);
        let results = ResultFile {
            subject: "s".into(),
            subject_source: "source".into(),
            results: vec![ResultRecord {
                expected: holds.expected.id().expect("a Holds has an identity"),
                commit: "s".into(),
                actual: harness::header_acceptance::evaluate::Actual::Run(run),
            }],
        };
        let required = [(Key::passing(TARGET), BTreeSet::from([Gate::G1]))].into();
        let evaluation = harness::header_acceptance::evaluate(
            &small,
            &required,
            &results,
            Subject {
                commit: "s",
                source: "source",
            },
        );
        match &evaluation.keys[&Key::passing(TARGET)] {
            ObligationState::Blocked { why } => why.clone(),
            other => panic!("{other:?}"),
        }
    };
    let overridden = state(&[TARGET.to_owned()]);
    assert!(overridden.contains("Draft override"), "{overridden}");
    let plain = state(&[]);
    assert!(!plain.contains("Draft override"), "{plain}");
}

/// The `wake` store's snapshot in `snapshot`, which names the wake fixture
/// the run declared, if any.
fn wake_store(snapshot: &Value) -> Option<String> {
    snapshot
        .as_array()?
        .iter()
        .find_map(|store| (store["store"] == "wake").then(|| store["snapshot"].to_string()))
}

/// The wake fixture `shown`'s wake store declares, if it declares one.
fn wake_fixture(shown: &Shown) -> Option<String> {
    let wake = shown.snapshot.as_ref().and_then(wake_store)?;
    (!wake.contains("fixture: None")).then_some(wake)
}

/// Where `sources` name a `WakeFixture` outside its own module and
/// `header-sync/release-wake`'s, which N3 keeps. A fixture can't be built,
/// read or handed on without naming its type, so no other harness declares
/// one, whatever its input.
fn wake_fixtures_outside_release_wake(sources: &[(String, String)]) -> Vec<String> {
    mentions_outside(sources, "WakeFixture", |path, _, _| {
        path == "src/harness/header_node/wake.rs"
            || path == "src/harness/header_sync/release_wake.rs"
    })
}

/// N3: no result N3 releases from C15 runs a release-wake fixture.
///
/// Every committed input a released result runs on declares no wake
/// fixture, and a history's own input is covered by the source rule
/// [`wake_fixtures_outside_release_wake`] checks: only
/// `header-sync/release-wake`, which N3 keeps, names one. The controls: the
/// release-wake input does declare one, found as the second of two inputs,
/// and a fixture named in a released harness's source breaks the rule.
#[test]
fn n3_only_release_wake_runs_a_wake_fixture() {
    let manifest = Manifest::committed();
    let plan = tool::plan(&manifest, &repo()).expect("the committed manifest plans");
    let n3 = entry("N3");
    let (released, kept) = harnesses(&manifest, n3);
    assert!(kept.contains("header-sync/release-wake"));
    let inputs = released_inputs(&manifest, &plan, n3);
    assert!(released
        .iter()
        .all(|harness| inputs.iter().any(|input| &input.harness == harness)));
    assert_eq!(first_carrying(&inputs, wake_fixture), None);
    let wakes = first_fixture("header-sync/release-wake");
    let found = first_carrying(&[inputs[0].clone(), wakes.clone()], wake_fixture);
    assert!(
        found
            .as_deref()
            .is_some_and(|found| found.contains(&wakes.name) && found.contains("fixture: Some")),
        "{found:?}"
    );

    let sources = production_sources();
    let release_wake = &sources
        .iter()
        .find(|(path, _)| path == "src/harness/header_sync/release_wake.rs")
        .expect("the release-wake harness")
        .1;
    assert_eq!(registered_names(release_wake), ["header-sync/release-wake"]);
    assert_eq!(
        wake_fixtures_outside_release_wake(&sources),
        Vec::<String>::new()
    );
    let mut planted = sources.clone();
    planted.push((
        "src/harness/header_sync/repair.rs".to_owned(),
        "    ports.wake.declare(WakeFixture { body_window: 1 });".to_owned(),
    ));
    assert_eq!(wake_fixtures_outside_release_wake(&planted).len(), 1);
}

/// What a machine's run shows of `Sim::run`: a drain, a store snapshot or an
/// observation, if any.
fn sim_evidence(shown: &Shown) -> Option<String> {
    if shown.drained {
        return Some("drained".to_owned());
    }
    if let Some(snapshot) = &shown.snapshot {
        return Some(format!("keeps stores {snapshot}"));
    }
    (!shown.kinds.is_empty()).then(|| format!("observes {:?}", shown.kinds))
}

/// What breaks the source rule behind N4 in `sources`: only
/// [`machine_entry!`](zakura_sim::machine_entry), whose `run` is
/// `harness::machine::run`, marks an entry a machine, and that module builds
/// no `Sim`, no evidence store and no drain, and never writes its log. So
/// a machine's run starts no drain, keeps no store and observes nothing,
/// whatever its input.
fn machine_rule_breaks(sources: &[(String, String)]) -> Vec<String> {
    const MACHINE: &str = "src/harness/machine.rs";
    let mut found = mentions_outside(sources, "machine: true", |path, _, _| path == MACHINE);
    let Some((_, text)) = sources.iter().find(|(path, _)| path == MACHINE) else {
        found.push(format!("{MACHINE} is missing"));
        return found;
    };
    let only = |path: &str, _: &str, _: usize| path != MACHINE;
    for banned in [
        "Sim::new(",
        "Sim::run(",
        "EvidenceStores",
        "let mut log",
        "log.push(",
        ".drain =",
        "drain: Some",
    ] {
        found.extend(mentions_outside(sources, banned, only));
    }
    for required in [
        "run: |value, input, sink| $crate::harness::machine::run(&$machine, value, input, sink),",
        "let log = ObservationLog::default();",
        "sink.report(&FullReport::new(&report.properties, None));",
    ] {
        if !text.contains(required) {
            found.push(format!("{MACHINE} lacks `{required}`"));
        }
    }
    found
}

/// N4: no result N4 releases from C1, C2, C10, C11, C14, C17 and C19 runs
/// `Sim::run`, its drain, checkpoints or stores.
///
/// Every released harness is a machine, and every committed input a
/// released result runs on starts no drain, keeps no store and observes
/// nothing. A history's own input is covered by the source rule
/// [`machine_rule_breaks`] checks. The controls: a node harness's input,
/// which N4 keeps, runs `Sim::run`, found as the second of two inputs, and
/// a drain written into the machine module breaks the rule.
#[test]
fn n4_machine_results_run_no_sim_drain_or_stores() {
    let manifest = Manifest::committed();
    let plan = tool::plan(&manifest, &repo()).expect("the committed manifest plans");
    let n4 = entry("N4");
    let (released, kept) = harnesses(&manifest, n4);
    assert!(!released.is_empty() && kept.contains("header-sync/storage"));
    for harness in &released {
        assert!(harness::find(harness).is_ok_and(|entry| entry.machine));
    }
    let inputs = released_inputs(&manifest, &plan, n4);
    assert!(released
        .iter()
        .all(|harness| inputs.iter().any(|input| &input.harness == harness)));
    assert_eq!(first_carrying(&inputs, sim_evidence), None);
    let node = first_fixture("header-sync/storage");
    let found = first_carrying(&[inputs[0].clone(), node.clone()], sim_evidence);
    assert!(
        found
            .as_deref()
            .is_some_and(|found| found.contains(&node.name)),
        "{found:?}"
    );

    let sources = production_sources();
    assert_eq!(machine_rule_breaks(&sources), Vec::<String>::new());
    let planted: Vec<(String, String)> = sources
        .iter()
        .map(|(path, text)| {
            let text = if path == "src/harness/machine.rs" {
                text.replacen("drain: None,", "drain: Some(drain),", 1)
            } else {
                text.clone()
            };
            (path.clone(), text)
        })
        .collect();
    assert_eq!(machine_rule_breaks(&planted).len(), 1);
}

/// Why a run of `results` isn't one the acceptance tool records itself in
/// `plan`, the first found: a history's run or a `Draft` run, or an
/// integrity mutation's test runs. Every result's every execution counts,
/// whatever its kind, and a run no task records must belong to a result
/// the plan lists as unsupported, which blocks.
fn not_tool_recorded(plan: &tool::RunPlan, results: &[&ExpectedResult]) -> Option<String> {
    results.iter().find_map(|result| {
        let id = result.expected.id();
        runs(&result.expected).into_iter().find_map(|run| {
            if let RunId::Case { property, name, .. } = &run {
                if property == NATIVE_DIR {
                    // Its runs are its own integrity task's test runs.
                    let tested = plan.tasks.iter().any(|task| {
                        matches!(
                            task,
                            tool::Task::Integrity { expected, patch, .. }
                                if Some(expected) == id.as_ref() && patch == name
                        )
                    });
                    return (!tested).then(|| format!("{run:?} has no integrity task"));
                }
            }
            match tool::recorded_at(&plan.tasks, &run) {
                Some((_, "run" | "draft")) => None,
                Some((index, dir)) => Some(format!("{run:?} is read from task {index}'s {dir}")),
                None => {
                    let blocked = plan
                        .unsupported
                        .iter()
                        .any(|(unsupported, _)| Some(unsupported) == id.as_ref());
                    (!blocked).then(|| format!("{run:?} has no task"))
                }
            }
        })
    })
}

/// N5: every run a result N5 releases from C4 counts is one the acceptance
/// tool records itself, a history's or a `Draft` run, or an integrity
/// mutation's test runs, never a planted case's baseline or patched run,
/// whose verdict and replays are the planted runner's. Every released
/// result's executions are checked, whatever its kind. The controls: a
/// `Caught` and a `MachineCaught`, which N5 keeps, are read from the
/// planted runner's artifacts, so releasing either fails.
#[test]
fn n5_released_results_are_recorded_by_the_tool() {
    let manifest = Manifest::committed();
    let plan = tool::plan(&manifest, &repo()).expect("the committed manifest plans");
    let n5 = entry("N5");
    let released = released(&manifest, n5);
    assert!(!released.is_empty());
    assert!(released
        .iter()
        .all(|result| !runs(&result.expected).is_empty()));
    assert_eq!(not_tool_recorded(&plan, &released), None);
    for kind in [ResultKind::Caught, ResultKind::MachineCaught] {
        let kept = manifest
            .expected
            .iter()
            .find(|result| result.expected.kind() == kind)
            .unwrap_or_else(|| panic!("the manifest has a {kind:?}"));
        assert!(!(n5.releases)(&result_context(&manifest, &kept.expected)));
        let widened: Vec<&ExpectedResult> = released.iter().copied().chain([kept]).collect();
        let found = not_tool_recorded(&plan, &widened);
        assert!(
            found
                .as_deref()
                .is_some_and(|found| found.contains("baseline")),
            "{kind:?}: {found:?}"
        );
    }
}
