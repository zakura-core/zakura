//! The narrowing entries' tests (hdr-14 Rule 24, hdr-14.7b).
//!
//! A [`NARROWING`] entry releases results from a contract only once its
//! test shows those results don't read that contract's evidence. Each test
//! here takes the released results from the committed manifest through the
//! entry's own `releases` and the evaluator's own context, so a widened
//! release is tested on what it now releases, and fails if a released
//! result's runs carry the contract's evidence. Each also checks a result
//! the entry doesn't release does carry it, so the check can see it.
#![cfg(zakura_sim)]

use std::{
    collections::BTreeSet,
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
            tool, Expected, ExpectedResult, Gate, HistoryRef, Key, Manifest, ObligationState,
            ResultKind, RunId, Subject, NARROWING,
        },
        HarnessEntry, HarnessError,
    },
    record::{FullReport, RecordSink},
    PlanInput,
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

/// The committed fixture inputs of `harness`, by their sidecars, or an
/// empty input when it has none, sorted, so each harness runs at least
/// once.
fn seeds(harness: &str) -> Vec<Vec<u8>> {
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
    match found.first() {
        Some(first) => vec![std::fs::read(first).expect("a fixture is readable")],
        None => vec![Vec::new()],
    }
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

/// Runs `harness` on `input` as `record` does and returns what it shows.
fn run(harness: &str, input: &[u8]) -> Shown {
    let entry: &HarnessEntry = harness::find(harness).expect("the harness is registered");
    let split = entry.scenario_bytes.min(input.len());
    let mut sink = KeepReport::default();
    let report = match (entry.run)(
        &entry.scenario(input),
        PlanInput::Bytes(input[split..].to_vec()),
        &mut sink,
    ) {
        Ok(report) => report,
        Err(HarnessError::Run { report, .. }) => *report,
        Err(error) => panic!("{harness}: {error}"),
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

/// N1: every header harness whose results N1 releases from C11 builds no
/// scripted peer, so its runs make no observation a peer's `PeerRecords`
/// back. A released harness that started building peers fails here. The
/// wire harness, which N1 keeps, does make them.
#[test]
fn n1_peerless_and_machine_results_read_no_peer_evidence() {
    let manifest = Manifest::committed();
    let (released, kept) = harnesses(&manifest, entry("N1"));
    assert!(!released.is_empty() && kept.contains("header-sync/wire"));
    for harness in &released {
        for input in seeds(harness) {
            let shown = run(harness, &input);
            let peers: Vec<&String> = shown
                .kinds
                .iter()
                .filter(|kind| PEER_KINDS.contains(&kind.as_str()))
                .collect();
            assert!(peers.is_empty(), "{harness} observes {peers:?}");
        }
    }
    let wire = run("header-sync/wire", &seeds("header-sync/wire")[0]);
    assert!(
        wire.kinds
            .iter()
            .any(|kind| PEER_KINDS.contains(&kind.as_str())),
        "{:?}",
        wire.kinds
    );
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
    let input = seeds(STORAGE).remove(0);
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

/// N3: every header harness whose results N3 releases from C15 declares no
/// release-wake fixture, so its runs never wait on one finishing. A
/// released harness that declared one fails here. `header-sync/release-wake`,
/// which N3 keeps, does.
#[test]
fn n3_only_release_wake_runs_a_wake_fixture() {
    let manifest = Manifest::committed();
    let (released, kept) = harnesses(&manifest, entry("N3"));
    assert!(kept.contains("header-sync/release-wake"));
    for harness in &released {
        for input in seeds(harness) {
            let shown = run(harness, &input);
            if let Some(wake) = shown.snapshot.as_ref().and_then(wake_store) {
                assert!(wake.contains("fixture: None"), "{harness}: {wake}");
            }
        }
    }
    let wakes = run(
        "header-sync/release-wake",
        &seeds("header-sync/release-wake")[0],
    );
    let wake = wakes
        .snapshot
        .as_ref()
        .and_then(wake_store)
        .expect("release-wake keeps a wake store");
    assert!(wake.contains("fixture: Some"), "{wake}");
}

/// N4: every header harness whose results N4 releases from C1, C2, C10,
/// C11, C14, C17 and C19 is a machine, whose runs start no drain, keep no
/// evidence store and observe nothing. A released harness that ran
/// `Sim::run`, with its drain, checkpoints or stores, fails here. A node
/// harness, which N4 keeps, keeps its stores.
#[test]
fn n4_machine_results_run_no_sim_drain_or_stores() {
    let manifest = Manifest::committed();
    let (released, kept) = harnesses(&manifest, entry("N4"));
    assert!(!released.is_empty() && kept.contains("header-sync/storage"));
    for harness in &released {
        assert!(harness::find(harness).is_ok_and(|entry| entry.machine));
        for input in seeds(harness) {
            let shown = run(harness, &input);
            assert!(!shown.drained, "{harness} drained");
            assert_eq!(shown.snapshot, None, "{harness}");
            assert!(shown.kinds.is_empty(), "{harness}: {:?}", shown.kinds);
        }
    }
    let node = run("header-sync/storage", &seeds("header-sync/storage")[0]);
    assert!(node.snapshot.is_some() && !node.kinds.is_empty());
}

/// N5: every run a result N5 releases from C4 counts is one the acceptance
/// tool records itself, a history's or a `Draft` run, or an integrity
/// mutation's test runs, never a planted case's baseline or patched run,
/// whose verdict and replays are the planted runner's. A released result
/// read from the planted runner's artifacts fails here. A `MachineCaught`,
/// which N5 keeps, is read from them.
#[test]
fn n5_released_results_are_recorded_by_the_tool() {
    let manifest = Manifest::committed();
    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let plan = tool::plan(&manifest, &repo).expect("the committed manifest plans");
    let n5 = entry("N5");
    let mut checked = 0;
    for result in released(&manifest, n5) {
        let runs: Vec<RunId> = match &result.expected {
            Expected::Replays { run } => vec![run.clone()],
            Expected::Holds { history, .. } | Expected::StaysNotApplicable { history, .. } => {
                vec![(*history).into()]
            }
            Expected::IntegrityCaught { .. } => {
                // Its runs are its own task's test runs.
                assert!(plan.tasks.iter().any(|task| matches!(
                    task,
                    tool::Task::Integrity { expected, .. }
                        if Some(expected) == result.expected.id().as_ref()
                )));
                checked += 1;
                continue;
            }
            _ => continue,
        };
        for run in runs {
            let Some((index, dir)) = tool::recorded_at(&plan.tasks, &run) else {
                continue;
            };
            assert!(
                ["run", "draft"].contains(&dir),
                "{run:?} is read from task {index}'s {dir}"
            );
            checked += 1;
        }
    }
    assert!(checked > 0);
    let machine = manifest
        .expected
        .iter()
        .find(|result| result.expected.kind() == ResultKind::MachineCaught)
        .expect("the manifest has a machine catch");
    assert!(!(n5.releases)(&result_context(
        &manifest,
        &machine.expected
    )));
    let Expected::MachineCaught { case, .. } = machine.expected else {
        unreachable!("found as a MachineCaught");
    };
    let patched = RunId::case(case, harness::header_acceptance::CaseRun::Patched);
    assert_eq!(
        tool::recorded_at(&plan.tasks, &patched).map(|(_, dir)| dir),
        Some("patched")
    );
}
