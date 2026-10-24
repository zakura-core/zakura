//! T1.1 and T1.2: trace files round-trip, equal runs write equal bytes, and
//! the reader rejects each kind of bad file with its own error.
#![cfg(zakura_sim)]

mod support;

#[path = "../build/provenance.rs"]
#[allow(dead_code, reason = "build.rs uses the rest")]
mod provenance;

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    time::Duration,
};

use proptest::{collection::vec, option, prelude::*};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use support::{config, inputs};
use tempfile::TempDir;
use zakura_rt::{sync::Mutex, task, time};
use zakura_sim::{
    check::{self, PropertyOutcome, PropertyReport, StepCtx},
    end::{Drain, HarnessFailure, IdleStatus},
    plan::By,
    property::{Assume, AssumeRecord, Status},
    record::{
        CleanupRecord, DrawRecord, EndRecord, InitRecord, OutcomeKind, StepRecord, ViolationRecord,
    },
    trace::{
        meta::{
            BlobRef, BudgetRecord, Derivation, DerivationTool, InputRef, PropertyRef,
            SchedulerRecord, RNG, UNKNOWN,
        },
        BlobStore, BuildInfo, FileSink, Invalid, MetaRecord, Missing, ReadError, Refusal,
        TraceReader, MAX_BLOB_BYTES, MAX_INPUT_BYTES, MAX_JSON_DEPTH, MAX_LINE_BYTES,
        MAX_TRACE_BYTES, TRACE_FILE,
    },
    Checker, HexU64, MemorySink, NoChecks, Outcome, PlanInput, Record, RecordSink, RunConfig,
    RunReport, Sim, Violation,
};

/// Three tasks that share a lock, sleep and yield, so runs take time
/// advances and the schedule shows in the observations.
fn workload(sim: &mut Sim) {
    let lock = Arc::new(Mutex::new(0u32));
    for (label, ms) in [("a", 1), ("b", 2), ("c", 3)] {
        let lock = lock.clone();
        sim.scheduler().spawn_root(label, async move {
            for _ in 0..3 {
                *lock.lock().unwrap() += 1;
                time::sleep(Duration::from_millis(ms)).await;
                task::yield_now().await;
            }
        });
    }
}

/// The run config every test here uses.
fn run_config() -> RunConfig {
    config(2, 10_000)
}

/// A `meta` record for the workload under [`run_config`].
fn meta() -> MetaRecord {
    let config = run_config();
    MetaRecord {
        format: zakura_sim::record::FORMAT_VERSION,
        contract: zakura_sim::record::CONTRACT_VERSION,
        generator: 1,
        build: BuildInfo::current(),
        sim: "test/workload".into(),
        scenario: json!({ "tasks": 3 }),
        assumptions: Vec::new(),
        scheduler: SchedulerRecord::from(config.scheduler),
        budget: BudgetRecord::from(&config),
        rng: RNG.into(),
        fixtures: Vec::new(),
        properties: Vec::new(),
        input: None,
        derived_from: None,
    }
}

/// Runs the workload under `plan` and `checker`, writing its trace to `dir`
/// after `meta`.
fn record_to(
    dir: &Path,
    meta: &MetaRecord,
    plan: PlanInput,
    checker: &mut dyn Checker,
) -> RunReport {
    let mut sim = Sim::new(run_config(), plan);
    workload(&mut sim);
    let mut sink = FileSink::create(dir, meta).unwrap();
    let report = sim.run(checker, &mut sink);
    sink.finish().unwrap();
    report
}

/// A fresh directory holding a complete trace of the workload under `plan`.
fn recorded(plan: PlanInput) -> TempDir {
    let dir = TempDir::new().unwrap();
    record_to(dir.path(), &meta(), plan, &mut NoChecks);
    dir
}

/// The workload's records under `plan`, from a memory sink.
fn in_memory(plan: PlanInput, checker: &mut dyn Checker) -> Vec<Record> {
    let mut sim = Sim::new(run_config(), plan);
    workload(&mut sim);
    let mut sink = MemorySink::default();
    sim.run(checker, &mut sink);
    sink.records
}

/// The lines of the trace in `dir`, without newlines.
fn lines(dir: &Path) -> Vec<String> {
    fs::read_to_string(dir.join(TRACE_FILE))
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect()
}

/// Replaces the trace in `dir` with `lines`, each ending in a newline.
fn write_lines(dir: &Path, lines: &[String]) {
    let text: String = lines.iter().map(|line| format!("{line}\n")).collect();
    fs::write(dir.join(TRACE_FILE), text).unwrap();
}

/// Reads `dir` as a complete trace and returns its `Invalid` reason.
fn invalid(dir: &Path) -> Invalid {
    match TraceReader::open(dir) {
        Err(ReadError::Invalid(invalid)) => invalid,
        other => panic!("expected an invalid trace, got {other:?}"),
    }
}

/// Reads `dir` as a complete trace and returns its `Missing` reason.
fn missing(dir: &Path) -> Missing {
    match TraceReader::open(dir) {
        Err(ReadError::Missing(missing)) => missing,
        other => panic!("expected a missing artifact, got {other:?}"),
    }
}

/// `bytes`' SHA-256 in lowercase hex.
fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// A plan that takes several time advances, with real choices among tasks.
fn plan() -> PlanInput {
    inputs(1, 64).remove(0)
}

/// Each step's record in `records`.
fn steps(records: &[Record]) -> Vec<StepRecord> {
    records
        .iter()
        .filter_map(|record| match record {
            Record::Step(step) => Some(step.clone()),
            _ => None,
        })
        .collect()
}

#[test]
fn real_run_round_trips() {
    let dir = recorded(plan());
    let trace = TraceReader::open(dir.path()).unwrap();
    let memory = in_memory(plan(), &mut NoChecks);

    assert_eq!(trace.meta, meta());
    assert_eq!(trace.init.map(Record::Init).as_ref(), memory.first());
    assert_eq!(trace.steps, steps(&memory));
    assert_eq!(trace.end.map(Record::End).as_ref(), memory.last());
    assert!(trace.steps.len() > 10, "the run has real steps");
    let bytes = fs::read(dir.path().join(TRACE_FILE)).unwrap();
    assert_eq!(trace.id, sha256(&bytes));
    assert!(bytes.ends_with(b"\n"));
}

#[test]
fn equal_runs_write_equal_bytes() {
    for plan in inputs(10, 64) {
        let (a, b) = (recorded(plan.clone()), recorded(plan));
        assert_eq!(
            fs::read(a.path().join(TRACE_FILE)).unwrap(),
            fs::read(b.path().join(TRACE_FILE)).unwrap()
        );
    }
}

#[test]
fn file_sink_never_overwrites() {
    let dir = recorded(plan());
    let before = lines(dir.path());
    assert!(FileSink::create(dir.path(), &meta()).is_err());
    assert_eq!(lines(dir.path()), before);
}

/// Fails `property` at step 5, naming steps 2 and 5 as evidence.
fn violates_at_5(witness: Vec<u64>) -> impl Checker {
    check::after_step(move |cx: &StepCtx<'_>| {
        (cx.step == 5).then(|| Violation {
            property: "test/fails-at-5".into(),
            step: 5,
            witness: witness.clone(),
            expected: "nothing".into(),
            actual: "step 5".into(),
        })
    })
}

#[test]
fn witness_keys_name_the_evidence_picks() {
    let dir = TempDir::new().unwrap();
    let report = record_to(dir.path(), &meta(), plan(), &mut violates_at_5(vec![2, 5]));
    let trace = TraceReader::open(dir.path()).unwrap();
    let violation = trace.end.unwrap().violation.unwrap();
    assert_eq!(violation.witness, [2, 5]);
    assert_eq!(
        violation.witness_keys,
        [
            report.picks[1].identity.clone(),
            report.picks[4].identity.clone()
        ]
    );
}

#[test]
fn evidence_outside_the_run_is_a_harness_failure() {
    for witness in [vec![0], vec![2, 6]] {
        let dir = TempDir::new().unwrap();
        let report = record_to(dir.path(), &meta(), plan(), &mut violates_at_5(witness));
        assert_eq!(report.end.outcome, Outcome::Harness);
        assert!(matches!(
            report.end.harness,
            Some(HarnessFailure::EvidenceOutsideRun { .. })
        ));
        // The trace is still readable, without the violation.
        let end = TraceReader::open(dir.path()).unwrap().end.unwrap();
        assert_eq!(end.outcome, OutcomeKind::Harness);
        assert_eq!(end.violation, None);
    }
}

/// A checker that reports one property at the run's end, built by
/// `report` from the run's step count.
fn reports_at_end(report: impl Fn(u64) -> PropertyReport + 'static) -> impl Checker {
    check::FnChecker::new(
        |_: &StepCtx<'_>| None,
        move |end: &zakura_sim::RunEnd, _: &zakura_sim::ObservationLog| vec![report(end.steps)],
    )
}

/// Records the workload under `checker`, and returns the run's report and
/// the `end` record read back from its trace.
fn run_and_read(checker: &mut dyn Checker) -> (RunReport, EndRecord) {
    let dir = TempDir::new().unwrap();
    let run = record_to(dir.path(), &meta(), plan(), checker);
    let end = TraceReader::open(dir.path()).unwrap().end.unwrap();
    (run, end)
}

/// A report for `test/at-end`.
fn at_end(outcome: PropertyOutcome, holds_since: Option<u64>) -> PropertyReport {
    PropertyReport {
        property: "test/at-end".into(),
        outcome,
        holds_since,
    }
}

#[test]
fn reports_outside_the_run_are_a_harness_failure() {
    // Evidence past the run ends it as a harness failure. The bad step is
    // left out of the report, so the trace still reads.
    let (run, end) = run_and_read(&mut reports_at_end(|_| {
        at_end(
            PropertyOutcome::Violated {
                witness: vec![2, 99_999],
            },
            None,
        )
    }));
    assert_eq!(
        run.end.harness,
        Some(HarnessFailure::EvidenceOutsideRun {
            property: "test/at-end".into(),
            step: 99_999,
        })
    );
    let kept = at_end(PropertyOutcome::Violated { witness: vec![2] }, None);
    assert_eq!(
        (&run.properties[..], &end.properties[..]),
        (&[kept.clone()][..], &[kept][..])
    );

    // So does a holds_since past the last step.
    let (run, end) = run_and_read(&mut reports_at_end(|steps| {
        at_end(PropertyOutcome::NotApplicable, Some(steps + 1))
    }));
    assert_eq!(
        run.end.harness,
        Some(HarnessFailure::HoldsSinceOutsideRun {
            property: "test/at-end".into(),
            step: run.end.steps + 1,
        })
    );
    assert_eq!(end.outcome, OutcomeKind::Harness);
    assert_eq!(
        end.properties,
        [at_end(PropertyOutcome::NotApplicable, None)]
    );

    // References to the run's own steps, including holds_since 0 and the
    // last step, are fine.
    for holds_since in [|_| 0, |steps| steps] {
        let (run, end) = run_and_read(&mut reports_at_end(move |steps: u64| {
            at_end(
                PropertyOutcome::Violated {
                    witness: vec![1, steps],
                },
                Some(holds_since(steps)),
            )
        }));
        assert_ne!(run.end.outcome, Outcome::Harness, "{:?}", run.end.harness);
        assert_eq!(end.properties, run.properties);
    }
}

#[test]
fn missing_blob() {
    let dir = TempDir::new().unwrap();
    let meta = MetaRecord {
        fixtures: vec![BlobRef {
            name: "chain".into(),
            sha256: sha256(b"never written"),
        }],
        ..meta()
    };
    record_to(dir.path(), &meta, plan(), &mut NoChecks);
    assert_eq!(
        missing(dir.path()),
        Missing::MissingBlob {
            sha256: sha256(b"never written")
        }
    );

    // So is an input.bin that meta names but nobody wrote.
    let dir = TempDir::new().unwrap();
    let meta = MetaRecord {
        input: Some(InputRef {
            sha256: sha256(b"input"),
            len: 5,
        }),
        ..self::meta()
    };
    record_to(dir.path(), &meta, plan(), &mut NoChecks);
    assert_eq!(
        missing(dir.path()),
        Missing::MissingBlob {
            sha256: sha256(b"input")
        }
    );
}

#[test]
fn digest_mismatch() {
    let dir = TempDir::new().unwrap();
    let store = BlobStore::for_trace(dir.path());
    let digest = store.put(b"fixture chain").unwrap();
    let meta = MetaRecord {
        fixtures: vec![BlobRef {
            name: "chain".into(),
            sha256: digest.clone(),
        }],
        ..meta()
    };
    record_to(dir.path(), &meta, plan(), &mut NoChecks);
    assert_eq!(store.get(&digest).unwrap(), b"fixture chain");
    TraceReader::open(dir.path()).unwrap();

    fs::write(dir.path().join("blobs").join(&digest), b"tampered").unwrap();
    assert_eq!(
        missing(dir.path()),
        Missing::DigestMismatch { sha256: digest }
    );

    // An input.bin that doesn't match meta.input is a mismatch too.
    let dir = TempDir::new().unwrap();
    let meta = MetaRecord {
        input: Some(InputRef {
            sha256: sha256(b"input"),
            len: 5,
        }),
        ..self::meta()
    };
    record_to(dir.path(), &meta, plan(), &mut NoChecks);
    fs::write(dir.path().join("input.bin"), b"other").unwrap();
    assert_eq!(
        missing(dir.path()),
        Missing::DigestMismatch {
            sha256: sha256(b"input")
        }
    );
    fs::write(dir.path().join("input.bin"), b"input").unwrap();
    TraceReader::open(dir.path()).unwrap();
}

#[test]
fn blob_store_looks_in_the_trace_first() {
    let (trace, committed) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let store = BlobStore::new(vec![trace.path().into(), committed.path().into()]);
    let only_committed = BlobStore::new(vec![committed.path().into()])
        .put(b"shared")
        .unwrap();
    assert_eq!(store.get(&only_committed).unwrap(), b"shared");

    // A bad copy in the trace isn't papered over by a good committed one.
    fs::write(trace.path().join(&only_committed), b"bad copy").unwrap();
    assert!(matches!(
        store.get(&only_committed),
        Err(ReadError::Missing(Missing::DigestMismatch { .. }))
    ));

    // A name that isn't a digest never touches the file system.
    assert!(matches!(
        store.get("../trace.jsonl"),
        Err(ReadError::Missing(Missing::MissingBlob { .. }))
    ));
    assert_eq!(
        BlobStore::for_trace(Path::new("t")),
        BlobStore::new(vec![PathBuf::from("t/blobs"), BlobStore::committed()])
    );
}

/// The reason `result` was refused, or a panic naming what it was instead.
fn refusal<T: std::fmt::Debug>(result: Result<T, ReadError>) -> Refusal {
    match result {
        Err(ReadError::Refused { reason, .. }) => reason,
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// A file at `path` that reads as `len` zero bytes, without using the disk.
fn sparse(path: &Path, len: u64) {
    fs::File::create(path).unwrap().set_len(len).unwrap();
}

#[cfg(unix)]
#[test]
fn symlinks_are_refused() {
    // A digest-named symlink whose target has that digest still isn't read.
    let (dir, outside) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let digest = BlobStore::new(vec![outside.path().into()])
        .put(b"outside the store")
        .unwrap();
    let blobs = dir.path().join("blobs");
    fs::create_dir(&blobs).unwrap();
    std::os::unix::fs::symlink(outside.path().join(&digest), blobs.join(&digest)).unwrap();
    let store = BlobStore::for_trace(dir.path());
    assert_eq!(refusal(store.get(&digest)), Refusal::NotRegularFile);

    // So is a trace file that's a symlink.
    let real = recorded(plan());
    let linked = TempDir::new().unwrap();
    std::os::unix::fs::symlink(real.path().join(TRACE_FILE), linked.path().join(TRACE_FILE))
        .unwrap();
    assert_eq!(
        refusal(TraceReader::open(linked.path())),
        Refusal::NotRegularFile
    );
}

#[cfg(unix)]
#[test]
fn symlinked_directories_are_refused() {
    // A blobs/ that links to another store is refused for reads and writes,
    // even when that store holds the right blob.
    let (dir, outside) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let digest = BlobStore::new(vec![outside.path().into()])
        .put(b"elsewhere")
        .unwrap();
    std::os::unix::fs::symlink(outside.path(), dir.path().join("blobs")).unwrap();
    let store = BlobStore::for_trace(dir.path());
    assert_eq!(refusal(store.get(&digest)), Refusal::SymlinkedDirectory);
    assert!(store.put(b"new blob").is_err());
    assert_eq!(listing(outside.path()), std::slice::from_ref(&digest));

    // So is a trace that names a blob through it.
    let meta = MetaRecord {
        fixtures: vec![BlobRef {
            name: "chain".into(),
            sha256: digest,
        }],
        ..meta()
    };
    record_to(dir.path(), &meta, plan(), &mut NoChecks);
    assert_eq!(
        refusal(TraceReader::open(dir.path())),
        Refusal::SymlinkedDirectory
    );

    // And a trace directory that is itself a symlink.
    let real = recorded(plan());
    let links = TempDir::new().unwrap();
    let linked = links.path().join("trace");
    std::os::unix::fs::symlink(real.path(), &linked).unwrap();
    assert_eq!(
        refusal(TraceReader::open(&linked)),
        Refusal::SymlinkedDirectory
    );
}

#[cfg(unix)]
#[test]
fn trailing_separators_dont_hide_a_symlinked_directory() {
    let real = recorded(plan());
    let links = TempDir::new().unwrap();
    let linked = links.path().join("trace");
    std::os::unix::fs::symlink(real.path(), &linked).unwrap();
    let spellings = [
        PathBuf::from(format!("{}/", linked.display())),
        PathBuf::from(format!("{}//", linked.display())),
        linked.join("."),
        PathBuf::from(format!("{}/./", linked.display())),
    ];
    for dir in &spellings {
        assert_eq!(
            refusal(TraceReader::open(dir)),
            Refusal::SymlinkedDirectory,
            "{}",
            dir.display()
        );
    }

    // A store whose directory is spelled the same ways refuses both ways.
    let outside = TempDir::new().unwrap();
    let digest = BlobStore::new(vec![outside.path().into()])
        .put(b"elsewhere")
        .unwrap();
    let blobs = links.path().join("blobs");
    std::os::unix::fs::symlink(outside.path(), &blobs).unwrap();
    for dir in [
        PathBuf::from(format!("{}/", blobs.display())),
        blobs.join("."),
    ] {
        let store = BlobStore::new(vec![dir.clone()]);
        assert_eq!(refusal(store.get(&digest)), Refusal::SymlinkedDirectory);
        assert!(store.put(b"new blob").is_err(), "{}", dir.display());
    }
    assert_eq!(listing(outside.path()), std::slice::from_ref(&digest));
}

#[cfg(unix)]
#[test]
fn fifo_blob_is_refused_without_blocking() {
    let dir = TempDir::new().unwrap();
    let blobs = dir.path().join("blobs");
    fs::create_dir(&blobs).unwrap();
    let digest = sha256(b"never written");
    let status = Command::new("mkfifo")
        .arg(blobs.join(&digest))
        .status()
        .unwrap();
    assert!(status.success());
    let store = BlobStore::for_trace(dir.path());
    // A read that blocks would hang the test, so it runs on its own thread.
    let (done, result) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = done.send(store.get(&digest).map_err(|error| error.to_string()));
    });
    let result = result
        .recv_timeout(Duration::from_secs(10))
        .expect("reading a FIFO blob doesn't block");
    assert!(result.unwrap_err().contains("isn't a regular file"));
}

#[test]
fn oversized_files_are_refused() {
    // Each file is sparse, so the test costs no disk and the reader must
    // refuse it before reading.
    let dir = TempDir::new().unwrap();
    let blobs = dir.path().join("blobs");
    fs::create_dir(&blobs).unwrap();
    let digest = sha256(b"huge");
    sparse(&blobs.join(&digest), MAX_BLOB_BYTES + 1);
    assert_eq!(
        refusal(BlobStore::for_trace(dir.path()).get(&digest)),
        Refusal::TooLarge {
            limit: MAX_BLOB_BYTES
        }
    );

    let dir = TempDir::new().unwrap();
    let meta = MetaRecord {
        input: Some(InputRef {
            sha256: sha256(b"input"),
            len: 5,
        }),
        ..meta()
    };
    record_to(dir.path(), &meta, plan(), &mut NoChecks);
    sparse(&dir.path().join("input.bin"), MAX_INPUT_BYTES + 1);
    assert_eq!(
        refusal(TraceReader::open(dir.path())),
        Refusal::TooLarge {
            limit: MAX_INPUT_BYTES
        }
    );

    let dir = TempDir::new().unwrap();
    sparse(&dir.path().join(TRACE_FILE), MAX_TRACE_BYTES + 1);
    assert_eq!(
        refusal(TraceReader::open(dir.path())),
        Refusal::TooLarge {
            limit: MAX_TRACE_BYTES
        }
    );
}

#[test]
fn writer_keeps_to_the_line_limit() {
    // A meta line over the limit fails before the file exists.
    let dir = TempDir::new().unwrap();
    let huge = MetaRecord {
        scenario: json!("x".repeat(MAX_LINE_BYTES)),
        ..meta()
    };
    let error = FileSink::create(dir.path(), &huge).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(!dir.path().join(TRACE_FILE).exists());

    // A step over the limit isn't written, and neither is anything after
    // it. The file stays a readable prefix, and finish reports the error.
    let dir = TempDir::new().unwrap();
    let mut sink = FileSink::create(dir.path(), &meta()).unwrap();
    let step = |step: u64, pick: String| {
        Record::Step(StepRecord {
            step,
            pick,
            key: None,
            options: 1,
            by: By::Forced,
            offset: None,
            draws: Vec::new(),
            obs: None,
        })
    };
    sink.record(Record::Init(InitRecord::default()));
    sink.record(step(1, "task:a.1:poll#1".into()));
    sink.record(step(2, "x".repeat(MAX_LINE_BYTES)));
    sink.record(step(3, "task:a.1:poll#2".into()));
    let error = sink.finish().unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    let prefix = TraceReader::open_prefix(dir.path()).unwrap();
    assert_eq!(prefix.steps.len(), 1);
    assert_eq!(invalid(dir.path()), Invalid::Truncated { steps: 1 });
}

#[test]
fn long_line_is_refused() {
    let dir = recorded(plan());
    let mut lines = lines(dir.path());
    let mut step: Value = serde_json::from_str(&lines[2]).unwrap();
    step["pick"] = json!("x".repeat(MAX_LINE_BYTES));
    lines[2] = step.to_string();
    write_lines(dir.path(), &lines);
    assert_eq!(invalid(dir.path()), Invalid::LineTooLong { line: 3 });
}

/// The files in `dir`, sorted.
fn listing(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}

#[cfg(unix)]
#[test]
fn put_never_writes_through_a_symlink() {
    let (store_dir, outside) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let victim = outside.path().join("victim");
    fs::write(&victim, b"not a blob").unwrap();
    let digest = sha256(b"payload");
    std::os::unix::fs::symlink(&victim, store_dir.path().join(&digest)).unwrap();

    let store = BlobStore::new(vec![store_dir.path().into()]);
    assert_eq!(store.put(b"payload").unwrap(), digest);
    assert_eq!(fs::read(&victim).unwrap(), b"not a blob");
    let blob = store_dir.path().join(&digest);
    assert!(blob.symlink_metadata().unwrap().is_file());
    assert_eq!(store.get(&digest).unwrap(), b"payload");
    assert_eq!(listing(store_dir.path()), [digest]);
}

#[cfg(unix)]
#[test]
fn put_keeps_a_verified_blob_and_replaces_a_corrupt_one() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TempDir::new().unwrap();
    let store = BlobStore::new(vec![dir.path().into()]);
    let digest = store.put(b"chain").unwrap();
    let blob = dir.path().join(&digest);

    // A verified copy isn't opened for writing at all, so a read-only one
    // stays as it is.
    fs::set_permissions(&blob, fs::Permissions::from_mode(0o444)).unwrap();
    assert_eq!(store.put(b"chain").unwrap(), digest);
    assert_eq!(fs::read(&blob).unwrap(), b"chain");

    // A corrupt copy is replaced whole, and no temporary file is left.
    fs::set_permissions(&blob, fs::Permissions::from_mode(0o644)).unwrap();
    fs::write(&blob, b"torn").unwrap();
    let before = blob.symlink_metadata().unwrap();
    assert_eq!(store.put(b"chain").unwrap(), digest);
    assert_eq!(store.get(&digest).unwrap(), b"chain");
    assert_ne!(
        std::os::unix::fs::MetadataExt::ino(&blob.symlink_metadata().unwrap()),
        std::os::unix::fs::MetadataExt::ino(&before),
        "the corrupt copy was renamed over, not rewritten in place"
    );
    assert_eq!(listing(dir.path()), [digest]);
}

#[test]
fn malformed_line() {
    let dir = recorded(plan());
    let good = lines(dir.path());
    let mut bad = good.clone();
    bad[2] = "{not json".into();
    write_lines(dir.path(), &bad);
    assert!(matches!(
        invalid(dir.path()),
        Invalid::Malformed { line: 3, .. }
    ));

    // An unknown field is malformed too.
    let mut step: Value = serde_json::from_str(&good[3]).unwrap();
    step["extra"] = json!(1);
    let mut bad = good.clone();
    bad[3] = step.to_string();
    write_lines(dir.path(), &bad);
    assert!(matches!(
        invalid(dir.path()),
        Invalid::Malformed { line: 4, .. }
    ));

    // A digest in meta that isn't one is malformed, since it names a file.
    let dir = TempDir::new().unwrap();
    let meta = MetaRecord {
        fixtures: vec![BlobRef {
            name: "chain".into(),
            sha256: "../../etc/passwd".into(),
        }],
        ..meta()
    };
    record_to(dir.path(), &meta, plan(), &mut NoChecks);
    assert!(matches!(
        invalid(dir.path()),
        Invalid::Malformed { line: 1, .. }
    ));
}

/// The trace in `dir` with line 1's JSON edited by `edit`.
fn edit_meta(dir: &Path, edit: impl FnOnce(&mut Value)) {
    let mut lines = lines(dir);
    let mut meta: Value = serde_json::from_str(&lines[0]).unwrap();
    edit(&mut meta["meta"]);
    lines[0] = meta.to_string();
    write_lines(dir, &lines);
}

#[test]
fn unsupported_format() {
    let dir = recorded(plan());
    // A newer format may add fields, so the version is checked first.
    edit_meta(dir.path(), |meta| {
        meta["format"] = json!(2);
        meta["requires_hints"] = json!(true);
    });
    assert_eq!(
        invalid(dir.path()),
        Invalid::UnsupportedVersion {
            field: "format",
            found: 2
        }
    );
}

#[test]
fn unsupported_contract() {
    let dir = recorded(plan());
    edit_meta(dir.path(), |meta| meta["contract"] = json!(7));
    assert_eq!(
        invalid(dir.path()),
        Invalid::UnsupportedVersion {
            field: "contract",
            found: 7
        }
    );
}

#[test]
fn truncated_is_prefix() {
    let dir = recorded(plan());
    let full = TraceReader::open(dir.path()).unwrap();
    let all = lines(dir.path());
    assert!(full.is_complete());

    // Without its end line, the file is a prefix with every step.
    write_lines(dir.path(), &all[..all.len() - 1]);
    let last = full.steps.last().unwrap().step;
    assert_eq!(invalid(dir.path()), Invalid::Truncated { steps: last });
    let prefix = TraceReader::open_prefix(dir.path()).unwrap();
    assert!(!prefix.is_complete());
    assert_eq!(prefix.steps, full.steps);
    assert_eq!((&prefix.meta, &prefix.init), (&full.meta, &full.init));

    // A crash mid-write leaves a partial last line, which never counts.
    let cut = &all[all.len() - 2];
    let text: String = all[..all.len() - 2]
        .iter()
        .map(|line| format!("{line}\n"))
        .chain([cut[..cut.len() / 2].to_owned()])
        .collect();
    fs::write(dir.path().join(TRACE_FILE), text).unwrap();
    assert_eq!(invalid(dir.path()), Invalid::Truncated { steps: last - 1 });
    let prefix = TraceReader::open_prefix(dir.path()).unwrap();
    assert_eq!(prefix.steps, full.steps[..full.steps.len() - 1]);

    // A crash right after meta leaves a prefix with no init.
    write_lines(dir.path(), &all[..1]);
    assert_eq!(invalid(dir.path()), Invalid::Truncated { steps: 0 });
    let prefix = TraceReader::open_prefix(dir.path()).unwrap();
    assert_eq!((prefix.init, prefix.steps.len()), (None, 0));
}

#[test]
fn every_cut_near_the_end_is_truncated() {
    let dir = recorded(plan());
    let full = TraceReader::open(dir.path()).unwrap();
    let last = full.steps.last().unwrap().step;
    let bytes = fs::read(dir.path().join(TRACE_FILE)).unwrap();
    let all = lines(dir.path());
    let end_line = bytes.len() - all[all.len() - 1].len() - 1;
    let step_line = end_line - all[all.len() - 2].len() - 1;
    // Every cut from the start of the last step to just before the final
    // newline, including one that drops only that newline.
    for cut in step_line..bytes.len() {
        fs::write(dir.path().join(TRACE_FILE), &bytes[..cut]).unwrap();
        let steps = if cut >= end_line { last } else { last - 1 };
        assert_eq!(
            invalid(dir.path()),
            Invalid::Truncated { steps },
            "cut at {cut} of {}",
            bytes.len()
        );
        let prefix = TraceReader::open_prefix(dir.path()).unwrap();
        assert!(!prefix.is_complete(), "cut at {cut}");
    }
    // Only meta, without its newline, is nothing written at all.
    fs::write(dir.path().join(TRACE_FILE), &all[0]).unwrap();
    assert_eq!(invalid(dir.path()), Invalid::MissingMeta);
}

#[test]
fn clock_regression() {
    let dir = recorded(plan());
    let mut lines = lines(dir.path());
    let advances: Vec<(usize, u64)> = lines
        .iter()
        .enumerate()
        .filter_map(|(index, line)| {
            let Ok(Record::Step(step)) = serde_json::from_str(line) else {
                return None;
            };
            let ns = step.pick.strip_prefix("advance-time:")?.parse().ok()?;
            Some((index, ns))
        })
        .collect();
    let first = advances.iter().position(|&(_, ns)| ns > 0).unwrap();
    let &(later, _) = advances[first + 1..].first().expect("two advances");
    let mut step: Value = serde_json::from_str(&lines[later]).unwrap();
    step["pick"] = json!("advance-time:0");
    lines[later] = step.to_string();
    write_lines(dir.path(), &lines);
    assert_eq!(
        invalid(dir.path()),
        Invalid::ClockRegression { line: later + 1 }
    );
}

#[test]
fn strict_reader() {
    let dir = TempDir::new().unwrap();
    record_to(dir.path(), &meta(), plan(), &mut violates_at_5(vec![2, 5]));
    let good = lines(dir.path());
    let n = good.len();
    let check = |lines: Vec<String>, expected: Invalid| {
        write_lines(dir.path(), &lines);
        assert_eq!(invalid(dir.path()), expected);
    };
    let with = |at: usize, line: &str| {
        let mut lines = good.clone();
        lines.insert(at, line.to_owned());
        lines
    };
    let edit_end = |edit: &dyn Fn(&mut Value)| {
        let mut lines = good.clone();
        let mut end: Value = serde_json::from_str(&lines[n - 1]).unwrap();
        edit(&mut end["end"]);
        lines[n - 1] = end.to_string();
        lines
    };

    check(
        with(2, &good[0]),
        Invalid::Duplicate {
            line: 3,
            record: "meta",
        },
    );
    check(
        with(n, &good[n - 1]),
        Invalid::Duplicate {
            line: n + 1,
            record: "end",
        },
    );
    check(with(n, &good[2]), Invalid::AfterEnd { line: n + 1 });
    check(
        with(3, &good[1]),
        Invalid::Duplicate {
            line: 4,
            record: "init",
        },
    );
    check(with(3, &good[2]), Invalid::OutOfOrder { line: 4 });
    check(
        [&good[..1], &good[2..]].concat(),
        Invalid::OutOfOrder { line: 2 },
    );
    check(with(0, &good[1]), Invalid::MissingMeta);
    write_lines(dir.path(), &with(2, r#"{"bogus":{}}"#));
    assert!(matches!(
        invalid(dir.path()),
        Invalid::Malformed { line: 3, .. }
    ));

    // The end record must name only steps the file has.
    let bad_reference = Invalid::BadReference { line: n };
    check(
        edit_end(&|end| end["violation"]["witness"] = json!([2, 99])),
        bad_reference.clone(),
    );
    check(
        edit_end(&|end| end["violation"]["step"] = json!(0)),
        bad_reference.clone(),
    );
    check(
        edit_end(&|end| end["violation"]["witness_keys"] = json!(["only one"])),
        bad_reference.clone(),
    );
    check(
        edit_end(&|end| end["steps"] = json!(1)),
        bad_reference.clone(),
    );

    // So must every property report's evidence and holds_since.
    let steps = serde_json::from_str::<Value>(&good[n - 1]).unwrap()["end"]["steps"]
        .as_u64()
        .unwrap();
    let report = |witness: u64, holds_since: u64| {
        json!([{
            "property": "test/fails-at-5",
            "outcome": { "violated": { "witness": [2, witness] } },
            "holds_since": holds_since,
        }])
    };
    check(
        edit_end(&|end| end["properties"] = report(99, steps)),
        bad_reference.clone(),
    );
    check(
        edit_end(&|end| end["properties"] = report(0, steps)),
        bad_reference.clone(),
    );
    check(
        edit_end(&|end| end["properties"] = report(steps, steps + 1)),
        bad_reference,
    );
    write_lines(
        dir.path(),
        &edit_end(&|end| end["properties"] = report(steps, steps)),
    );
    TraceReader::open(dir.path()).unwrap();
    write_lines(
        dir.path(),
        &edit_end(&|end| end["properties"] = report(5, 0)),
    );
    TraceReader::open(dir.path()).unwrap();
    check(Vec::new(), Invalid::MissingMeta);
}

#[test]
fn build_equivalence() {
    let clean = BuildInfo {
        git: "a".repeat(40),
        dirty: false,
        source: "s".repeat(64),
        lockfile: "l".repeat(64),
        rustc: "rustc 1.91.0".into(),
        target: "x86_64-unknown-linux-gnu".into(),
        cfg: vec!["debug_assertions".into(), "zakura_sim".into()],
        features: Vec::new(),
        opt_level: "0".into(),
        sanitizer: None,
        binary: "b".repeat(64),
    };
    let other_binary = |build: &BuildInfo| BuildInfo {
        binary: "c".repeat(64),
        ..build.clone()
    };
    assert!(clean.equivalent(&other_binary(&clean)));

    // Opt level and sanitizer don't count. The cfg set does.
    let asan = BuildInfo {
        opt_level: "3".into(),
        sanitizer: Some("address".into()),
        ..other_binary(&clean)
    };
    assert!(clean.equivalent(&asan));
    let release = BuildInfo {
        cfg: vec!["zakura_sim".into()],
        ..asan.clone()
    };
    assert!(!clean.equivalent(&release));

    // A dirty or unknown build matches only its own binary.
    let dirty = BuildInfo {
        dirty: true,
        ..clean.clone()
    };
    assert!(dirty.equivalent(&dirty.clone()));
    assert!(!dirty.equivalent(&other_binary(&dirty)));
    let unknown = BuildInfo {
        git: UNKNOWN.into(),
        ..clean.clone()
    };
    assert!(!unknown.equivalent(&other_binary(&unknown)));
    let no_binary = BuildInfo {
        binary: UNKNOWN.into(),
        dirty: true,
        ..clean.clone()
    };
    assert!(!no_binary.equivalent(&no_binary.clone()));
    for field in [
        |b: &mut BuildInfo| b.source = "t".repeat(64),
        |b: &mut BuildInfo| b.lockfile = "m".repeat(64),
        |b: &mut BuildInfo| b.rustc = "rustc 1.92.0".into(),
        |b: &mut BuildInfo| b.target = "aarch64-apple-darwin".into(),
        |b: &mut BuildInfo| b.features = vec!["extra".into()],
    ] {
        let mut changed = other_binary(&clean);
        field(&mut changed);
        assert!(!clean.equivalent(&changed), "{changed:?}");
    }
}

#[test]
fn current_build_is_recorded() {
    let build = BuildInfo::current();
    assert!(build.cfg.iter().any(|cfg| cfg == "zakura_sim"));
    assert_eq!(
        build.cfg.iter().any(|cfg| cfg == "debug_assertions"),
        cfg!(debug_assertions)
    );
    assert!(!build.cfg.iter().any(|cfg| cfg.starts_with("sanitize")));
    assert_ne!(build.binary, UNKNOWN);
    assert!(build.equivalent(&BuildInfo::current()));
    assert!(build.rustc.starts_with("rustc "));
}

#[test]
fn meta_bytes_ignore_key_order() {
    let mut forward = serde_json::Map::new();
    forward.insert("a".into(), json!(1));
    forward.insert("z".into(), json!({ "b": 2, "y": [{ "c": 3, "x": 4 }] }));
    let mut reverse = serde_json::Map::new();
    let mut inner = serde_json::Map::new();
    inner.insert("y".into(), json!([{ "x": 4, "c": 3 }]));
    inner.insert("b".into(), json!(2));
    reverse.insert("z".into(), Value::Object(inner));
    reverse.insert("a".into(), json!(1));
    let (forward, reverse) = (Value::Object(forward), Value::Object(reverse));
    assert_eq!(forward, reverse);
    assert_ne!(
        forward.to_string(),
        reverse.to_string(),
        "preserve_order is on, so a plain write shows insertion order"
    );

    let with = |value: &Value| MetaRecord {
        scenario: value.clone(),
        assumptions: vec![AssumeRecord {
            assume: Assume::BoundedDelivery,
            params: value.clone(),
        }],
        ..meta()
    };
    let (a, b) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    record_to(a.path(), &with(&forward), plan(), &mut NoChecks);
    record_to(b.path(), &with(&reverse), plan(), &mut NoChecks);
    let (a, b) = (
        TraceReader::open(a.path()).unwrap(),
        TraceReader::open(b.path()).unwrap(),
    );
    assert_eq!(a.id, b.id, "equal meta writes equal bytes");
    assert_eq!(a.meta, b.meta);
    let line = Record::Meta(Box::new(a.meta)).to_line();
    assert!(line.contains(r#""scenario":{"a":1,"z":{"b":2,"y":[{"c":3,"x":4}]}}"#));
}

/// `depth` arrays nested around `0`.
fn nested(depth: usize) -> Value {
    (0..depth).fold(json!(0), |inner, _| Value::Array(vec![inner]))
}

#[test]
fn json_depth_is_bounded() {
    // At the limit, in either field, a trace writes and reads back.
    let at_limit = MetaRecord {
        scenario: nested(MAX_JSON_DEPTH),
        assumptions: vec![AssumeRecord {
            assume: Assume::BoundedDelivery,
            params: json!({ "deep": nested(MAX_JSON_DEPTH - 1) }),
        }],
        ..meta()
    };
    assert_eq!(at_limit.too_deep_field(), None);
    let dir = TempDir::new().unwrap();
    record_to(dir.path(), &at_limit, plan(), &mut NoChecks);
    TraceReader::open(dir.path()).unwrap();

    // One level more fails before the file exists. That includes depths
    // serde_json itself can no longer parse inside a record.
    for depth in [MAX_JSON_DEPTH + 1, 126, 130] {
        for (meta, field) in [
            (
                MetaRecord {
                    scenario: nested(depth),
                    ..meta()
                },
                "scenario",
            ),
            (
                MetaRecord {
                    assumptions: vec![AssumeRecord {
                        assume: Assume::BoundedDelivery,
                        params: nested(depth),
                    }],
                    ..meta()
                },
                "assumptions",
            ),
        ] {
            assert_eq!(meta.too_deep_field(), Some(field), "{field} at {depth}");
            let dir = TempDir::new().unwrap();
            let error = FileSink::create(dir.path(), &meta).unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
            assert!(!dir.path().join(TRACE_FILE).exists());
        }
    }

    // A deeper value written by hand is malformed.
    let dir = recorded(plan());
    edit_meta(dir.path(), |meta| {
        meta["scenario"] = nested(MAX_JSON_DEPTH + 1)
    });
    assert!(matches!(
        invalid(dir.path()),
        Invalid::Malformed { line: 1, .. }
    ));
}

#[test]
fn floats_are_refused() {
    let nested = json!({ "link": { "delays": [1, 1.5] } });
    let in_scenario = MetaRecord {
        scenario: nested.clone(),
        ..meta()
    };
    let in_params = MetaRecord {
        assumptions: vec![AssumeRecord {
            assume: Assume::BoundedDelivery,
            params: nested,
        }],
        ..meta()
    };
    for (meta, field) in [(in_scenario, "scenario"), (in_params, "assumptions")] {
        assert_eq!(meta.float_field(), Some(field));
        let dir = TempDir::new().unwrap();
        let error = FileSink::create(dir.path(), &meta).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(!dir.path().join(TRACE_FILE).exists());
    }

    // A float written by hand is malformed.
    let dir = recorded(plan());
    edit_meta(dir.path(), |meta| {
        meta["scenario"] = json!({ "a": [{ "delay": 1.5 }] })
    });
    assert!(matches!(
        invalid(dir.path()),
        Invalid::Malformed { line: 1, .. }
    ));
    edit_meta(dir.path(), |meta| {
        meta["scenario"] = json!({});
        meta["assumptions"] = json!([{ "assume": "bounded-delivery", "params": { "d": 2e3 } }]);
    });
    assert!(matches!(
        invalid(dir.path()),
        Invalid::Malformed { line: 1, .. }
    ));
}

#[test]
fn meta_has_no_debug_output() {
    let line = Record::Meta(Box::new(meta())).to_line();
    let value: Value = serde_json::from_str(&line).unwrap();
    let mut strings = Vec::new();
    collect_strings(&value, &mut strings);
    for text in strings {
        for marker in ["Some(", "None", "{ ", "\\\"", "\":"] {
            assert!(!text.contains(marker), "{text:?} looks like Debug output");
        }
    }
}

/// Every string in `value`, keys included.
fn collect_strings(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::String(text) => out.push(text.clone()),
        Value::Array(items) => items.iter().for_each(|item| collect_strings(item, out)),
        Value::Object(map) => {
            for (key, item) in map {
                out.push(key.clone());
                collect_strings(item, out);
            }
        }
        _ => {}
    }
}

/// Runs git with a fixed identity and no signing in `dir`.
fn git(dir: &Path, args: &[&str]) {
    let status = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args([
            "-c",
            "user.name=sim",
            "-c",
            "user.email=sim@example.com",
            "-c",
            "commit.gpgsign=false",
            "-c",
            "init.defaultBranch=main",
        ])
        .args(args)
        .status()
        .unwrap();
    assert!(status.success(), "git {args:?}");
}

#[test]
fn provenance_tracks_unstaged_edits() {
    use provenance::{Provenance, SOURCE_PATHS, UNKNOWN};

    // A workspace laid out like this one. build.rs reads provenance from
    // its own crate directory, so the test does too.
    let root = TempDir::new().unwrap();
    let repo = root.path().join("repo");
    for (path, text) in [
        ("Cargo.toml", "[workspace]\n"),
        (".gitignore", "target/\n"),
        ("crates/sim/src/lib.rs", "fn a() {}\n"),
        ("crates/other/src/lib.rs", "fn b() {}\n"),
        ("docs/notes.md", "notes\n"),
    ] {
        fs::create_dir_all(repo.join(path).parent().unwrap()).unwrap();
        fs::write(repo.join(path), text).unwrap();
    }
    git(&repo, &["init", "-q"]);
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "-q", "-m", "one"]);
    let read = |checkout: &Path| Provenance::read(&checkout.join("crates/sim"), SOURCE_PATHS);

    let clean = read(&repo);
    assert!(!clean.dirty);
    assert_eq!(clean.git.len(), 40);
    assert_eq!(clean.source.len(), 64);

    // An unstaged edit to the crate itself, a sibling crate, or the
    // workspace manifest changes the source and marks the tree dirty.
    for path in [
        "crates/sim/src/lib.rs",
        "crates/other/src/lib.rs",
        "Cargo.toml",
    ] {
        let before = fs::read_to_string(repo.join(path)).unwrap();
        fs::write(repo.join(path), format!("{before}// edited\n")).unwrap();
        let edited = read(&repo);
        assert!(edited.dirty, "{path}");
        assert_eq!(edited.git, clean.git);
        assert_ne!(edited.source, clean.source, "{path}");
        fs::write(repo.join(path), before).unwrap();
        assert_eq!(read(&repo), clean, "{path} restored");
    }

    // So does an untracked source file.
    fs::write(repo.join("crates/sim/src/new.rs"), "fn c() {}\n").unwrap();
    let untracked = read(&repo);
    assert!(untracked.dirty);
    assert_ne!(untracked.source, clean.source);
    fs::remove_file(repo.join("crates/sim/src/new.rs")).unwrap();

    // Ignored files and files outside the source paths change nothing.
    fs::create_dir(repo.join("target")).unwrap();
    fs::write(repo.join("target/out"), "built").unwrap();
    fs::write(repo.join("docs/notes.md"), "edited notes\n").unwrap();
    assert_eq!(read(&repo), clean);

    // A linked worktree of the same commit has the same source, and its own
    // git dir, which is what build.rs watches.
    let linked = root.path().join("linked");
    git(&repo, &["worktree", "add", "-q", linked.to_str().unwrap()]);
    let worktree = read(&linked);
    assert_eq!((worktree.dirty, &worktree.source), (false, &clean.source));
    let git_dir = worktree.git_dir.clone().unwrap();
    assert_ne!(Some(&git_dir), clean.git_dir.as_ref());
    assert!(git_dir.join("HEAD").exists());
    fs::write(linked.join("crates/sim/src/lib.rs"), "fn d() {}\n").unwrap();
    let edited = read(&linked);
    assert!(edited.dirty);
    assert_ne!(edited.source, clean.source);

    // Outside any repository, provenance is unknown and never clean.
    let outside = TempDir::new().unwrap();
    let unknown = Provenance::read(outside.path(), SOURCE_PATHS);
    assert!(unknown.dirty);
    assert_eq!(
        (unknown.git.as_str(), unknown.source.as_str()),
        (UNKNOWN, UNKNOWN)
    );
}

// Strategies for every record type. Each mixes boundary values, empty
// collections and absent optional fields with arbitrary ones.

/// A `u64` that is often 0, 1 or `u64::MAX`.
fn edge_u64() -> impl Strategy<Value = u64> {
    prop_oneof![Just(0), Just(1), Just(u64::MAX), any::<u64>()]
}

/// A `usize` that is often 0 or `usize::MAX`.
fn edge_usize() -> impl Strategy<Value = usize> {
    prop_oneof![Just(0), Just(usize::MAX), any::<usize>()]
}

/// A string that is often empty.
fn text() -> impl Strategy<Value = String> {
    prop_oneof![Just(String::new()), any::<String>()]
}

/// Any assumption.
fn assume() -> impl Strategy<Value = Assume> {
    prop_oneof![
        Just(Assume::HonestPeer),
        Just(Assume::BoundedDelivery),
        Just(Assume::BoundedStorage),
        Just(Assume::BoundedAdversary),
        Just(Assume::StableMembership),
    ]
}

/// A list of `u64`s, often empty.
fn u64s() -> impl Strategy<Value = Vec<u64>> {
    vec(edge_u64(), 0..4)
}

/// What chose a pick or draw.
fn by() -> impl Strategy<Value = By> {
    prop_oneof![Just(By::Bytes), Just(By::Default), Just(By::Forced)]
}

/// Draws taken in one step.
fn draws() -> impl Strategy<Value = Vec<DrawRecord>> {
    let draw =
        (text(), edge_u64(), edge_u64(), by()).prop_map(|(draw, bound, value, by)| DrawRecord {
            draw,
            bound: HexU64(bound),
            value: HexU64(value),
            by,
        });
    vec(draw, 0..3)
}

/// A step record.
fn step() -> impl Strategy<Value = StepRecord> {
    (
        edge_u64(),
        text(),
        option::of(text()),
        edge_usize(),
        by(),
        option::of(edge_usize()),
        draws(),
        option::of(text()),
    )
        .prop_map(
            |(step, pick, key, options, by, offset, draws, obs)| StepRecord {
                step,
                pick,
                key,
                options,
                by,
                offset,
                draws,
                obs,
            },
        )
}

/// A property's outcome.
fn property_outcome() -> impl Strategy<Value = PropertyOutcome> {
    prop_oneof![
        any::<u32>().prop_map(|applicable| PropertyOutcome::Held { applicable }),
        Just(PropertyOutcome::NotApplicable),
        text().prop_map(|reason| PropertyOutcome::Inconclusive { reason }),
        u64s().prop_map(|witness| PropertyOutcome::Violated { witness }),
    ]
}

/// An `end` record.
fn end() -> impl Strategy<Value = EndRecord> {
    let outcome = prop_oneof![
        Just(OutcomeKind::Violation),
        Just(OutcomeKind::Budget),
        Just(OutcomeKind::Idle),
        Just(OutcomeKind::Harness),
    ];
    let status = option::of(prop_oneof![
        Just(IdleStatus::Quiescent),
        Just(IdleStatus::Deadlock),
        Just(IdleStatus::WorkerStarved),
        Just(IdleStatus::WaitingOnExternal),
    ]);
    let violation = option::of(
        (
            edge_u64(),
            text(),
            u64s(),
            vec(text(), 0..3),
            text(),
            text(),
        )
            .prop_map(
                |(step, property, witness, witness_keys, expected, actual)| ViolationRecord {
                    property,
                    step,
                    witness,
                    witness_keys,
                    expected,
                    actual,
                },
            ),
    );
    let drain = option::of(
        (edge_u64(), any::<bool>()).prop_map(|(steps, exhausted)| Drain { steps, exhausted }),
    );
    let cleanup = vec(edge_usize(), 6).prop_map(|n| CleanupRecord {
        cancelled: n[0],
        panicked: n[1],
        unwound: n[2],
        unwind_failures: n[3],
        leaked: n[4],
        timers_left: n[5],
    });
    let properties = vec(
        (text(), property_outcome(), option::of(edge_u64())).prop_map(
            |(property, outcome, holds_since)| PropertyReport {
                property,
                outcome,
                holds_since,
            },
        ),
        0..3,
    );
    (
        outcome,
        status,
        edge_u64(),
        violation,
        drain,
        option::of(text()),
        cleanup,
        properties,
    )
        .prop_map(
            |(outcome, status, steps, violation, drain, harness, cleanup, properties)| EndRecord {
                outcome,
                status,
                steps,
                violation,
                drain,
                harness,
                cleanup,
                properties,
            },
        )
}

/// A JSON value with no floats, as a scenario holds.
fn json_value() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::from),
        any::<i64>().prop_map(Value::from),
        edge_u64().prop_map(Value::from),
        text().prop_map(Value::from),
    ];
    leaf.prop_recursive(3, 16, 4, |inner| {
        prop_oneof![
            vec(inner.clone(), 0..4).prop_map(Value::Array),
            vec((text(), inner), 0..4)
                .prop_map(|entries| Value::Object(entries.into_iter().collect())),
        ]
    })
}

/// A build's provenance.
fn build() -> impl Strategy<Value = BuildInfo> {
    (
        (text(), any::<bool>(), text(), text(), text(), text()),
        (vec(text(), 0..3), vec(text(), 0..3), text()),
        (option::of(text()), text()),
    )
        .prop_map(
            |(
                (git, dirty, source, lockfile, rustc, target),
                (cfg, features, opt_level),
                (sanitizer, binary),
            )| BuildInfo {
                git,
                dirty,
                source,
                lockfile,
                rustc,
                target,
                cfg,
                features,
                opt_level,
                sanitizer,
                binary,
            },
        )
}

/// A `meta` record.
fn meta_record() -> impl Strategy<Value = MetaRecord> {
    let limits = (
        any::<u16>(),
        edge_usize(),
        any::<u32>(),
        edge_u64(),
        edge_u64(),
        edge_u64(),
        any::<bool>(),
    )
        .prop_map(
            |(workers, stack_size, max_tasks, budget, drain_bound, per_instant, livelock)| {
                (
                    SchedulerRecord {
                        workers,
                        stack_size,
                        max_tasks,
                    },
                    BudgetRecord {
                        budget,
                        drain_bound,
                        max_steps_per_instant: per_instant,
                        enforce_no_livelock: livelock,
                    },
                )
            },
        );
    let status = prop_oneof![
        Just(Status::Active),
        any::<u32>().prop_map(|issue| Status::Known { issue }),
        Just(Status::Draft),
    ];
    let tool = prop_oneof![
        Just(DerivationTool::Recover),
        Just(DerivationTool::Shrink),
        Just(DerivationTool::Search),
        Just(DerivationTool::Migrate),
    ];
    let refs = (
        vec(
            (assume(), json_value()).prop_map(|(assume, params)| AssumeRecord { assume, params }),
            0..3,
        ),
        vec(
            (text(), text()).prop_map(|(name, sha256)| BlobRef { name, sha256 }),
            0..3,
        ),
        vec(
            (text(), status).prop_map(|(id, status)| PropertyRef { id, status }),
            0..3,
        ),
        option::of((text(), edge_u64()).prop_map(|(sha256, len)| InputRef { sha256, len })),
        option::of(
            (text(), tool, any::<u32>()).prop_map(|(trace, tool, tool_version)| Derivation {
                trace,
                tool,
                tool_version,
            }),
        ),
    );
    (
        (any::<u32>(), any::<u32>(), any::<u32>()),
        build(),
        (text(), json_value(), text()),
        limits,
        refs,
    )
        .prop_map(
            |(
                (format, contract, generator),
                build,
                (sim, scenario, rng),
                (scheduler, budget),
                (assumptions, fixtures, properties, input, derived_from),
            )| MetaRecord {
                format,
                contract,
                generator,
                build,
                sim,
                scenario,
                assumptions,
                scheduler,
                budget,
                rng,
                fixtures,
                properties,
                input,
                derived_from,
            },
        )
}

/// Any record.
fn record() -> impl Strategy<Value = Record> {
    prop_oneof![
        meta_record().prop_map(|meta| Record::Meta(Box::new(meta))),
        draws().prop_map(|draws| Record::Init(InitRecord { draws })),
        step().prop_map(Record::Step),
        end().prop_map(Record::End),
    ]
}

/// Serializes `record`, reads it back, and checks that the result is equal
/// and serializes to the same bytes.
fn assert_round_trips(record: &Record) {
    let line = record.to_line();
    assert!(!line.contains('\n'), "a record is one line: {line}");
    let back: Record = serde_json::from_str(&line).unwrap();
    assert_eq!(&back, record);
    assert_eq!(back.to_line(), line);
}

proptest! {
    #[test]
    fn every_record_round_trips(record in record()) {
        assert_round_trips(&record);
    }
}

#[test]
fn round_trip_edges() {
    let empty_meta = MetaRecord {
        format: 0,
        contract: u32::MAX,
        generator: 0,
        build: BuildInfo {
            git: String::new(),
            dirty: true,
            source: String::new(),
            lockfile: String::new(),
            rustc: String::new(),
            target: String::new(),
            cfg: Vec::new(),
            features: Vec::new(),
            opt_level: String::new(),
            sanitizer: None,
            binary: String::new(),
        },
        sim: String::new(),
        scenario: Value::Null,
        assumptions: Vec::new(),
        scheduler: SchedulerRecord {
            workers: 0,
            stack_size: usize::MAX,
            max_tasks: 0,
        },
        budget: BudgetRecord {
            budget: u64::MAX,
            drain_bound: 0,
            max_steps_per_instant: 0,
            enforce_no_livelock: false,
        },
        rng: String::new(),
        fixtures: Vec::new(),
        properties: Vec::new(),
        input: None,
        derived_from: None,
    };
    let bare_step = StepRecord {
        step: u64::MAX,
        pick: String::new(),
        key: None,
        options: 0,
        by: By::Forced,
        offset: None,
        draws: Vec::new(),
        obs: None,
    };
    let bare_end = EndRecord {
        outcome: OutcomeKind::Budget,
        status: None,
        steps: 0,
        violation: None,
        drain: None,
        harness: None,
        cleanup: CleanupRecord::default(),
        properties: Vec::new(),
    };
    for record in [
        Record::Meta(Box::new(empty_meta)),
        Record::Init(InitRecord::default()),
        Record::Step(bare_step.clone()),
        Record::End(bare_end),
    ] {
        assert_round_trips(&record);
    }
    // An absent optional field is left out, not written as null, except
    // `end.drain`, which M1 always writes.
    let line = Record::Step(bare_step).to_line();
    assert_eq!(
        line,
        r#"{"step":18446744073709551615,"pick":"","options":0,"by":"forced"}"#
    );
}
