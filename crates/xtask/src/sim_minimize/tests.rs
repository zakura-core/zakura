//! `sim-minimize` against fake tools: shell scripts that stand in for
//! `zakura-sim`, the fuzz target and `cargo fuzz tmin`, and act on keywords
//! in the input. `zakura-sim`'s own tests (`tests/minimize.rs`) check what
//! a real confirmation finds; these check what the wrapper does with each
//! finding, each crash and each child that misbehaves.

use std::{os::unix::fs::PermissionsExt as _, sync::atomic::AtomicBool};

use tempfile::TempDir;

use super::*;

/// `zakura-sim`: `properties` lists `race/lost-update` and
/// `sim/no-task-panic`, `confirm` writes a confirmation whose status the
/// input's keywords pick, and `replay` fails for a trace named `bad`. A
/// keyword can also make `confirm` hang, die from a signal, fail without a
/// confirmation, or refuse its arguments. `properties` and `confirm` refuse
/// a harness other than `fixtures/all` and `discovery/wait-complete` the
/// way the real binary does.
const SIM: &str = r#"#!/bin/sh
usage() { echo "error: $1"; echo; echo "usage: zakura-sim <subcommand>"; exit 2; }
cmd=$1; shift
case "$cmd" in
properties)
  case "$2" in
    fixtures/all|discovery/wait-complete) ;;
    *) usage "no harness is named \`$2\`" >&2;;
  esac
  echo race/lost-update
  echo sim/no-task-panic
  exit 0;;
confirm)
  against=
  while [ $# -gt 0 ]; do
    case "$1" in
      --sim) sim=$2;;
      --input) input=$2;;
      --out) out=$2;;
      --against) against=$2;;
    esac
    shift 2
  done
  case "$sim" in
    fixtures/all|discovery/wait-complete) ;;
    *) usage "no harness is named \`$sim\`" >&2;;
  esac
  body=$(cat "$input")
  mkdir -p "$out"
  case "$body" in
    *hang-confirm*) sleep 30;;
    *segv-confirm*) kill -SEGV $$;;
    *fail-confirm*) echo "the trace can't go there" >&2; exit 1;;
    *refuse-confirm*) usage "fixtures/all doesn't register it" >&2;;
  esac
  status=confirmed
  case "$body" in
    *other*) status=other-property;;
    *assume*) [ -n "$against" ] && status=assumptions-changed;;
    *witness*) status=witness-lost;;
    *partial*) status=incomplete-evidence;;
  esac
  trace=traces/good
  case "$body" in *replay-bad*) trace=traces/bad;; esac
  mkdir -p "$out/$trace"
  printf '{"status":"%s","detail":"fake","trace":"%s","generator":7,"build":{"binary":"fake"}}\n' \
    "$status" "$trace" > "$out/confirmation.json"
  [ "$status" = confirmed ] || exit 1
  exit 0;;
replay)
  case "$1" in *bad*) echo "1 records differed" >&2; exit 1;; esac
  exit 0;;
esac
exit 2
"#;

/// The fuzz target: crashes the way the input's keywords say, else with the
/// filter's panic, after naming its harness: `fixtures/all`, or
/// `fixtures/none` for an input with `unregistered`. It refuses to run
/// without the filter.
const FUZZ: &str = r#"#!/bin/sh
for input; do :; done
[ -n "$ZAKURA_SIM_EXPECT" ] || { echo "no filter"; exit 3; }
body=$(cat "$input")
case "$body" in
  *abort*) echo "==1== ERROR: libFuzzer: deadly signal"; exit 77;;
  *asan*) echo "==1==ERROR: AddressSanitizer: heap-use-after-free"; exit 1;;
  *timeout*) echo "==1== ERROR: libFuzzer: timeout after 2 seconds"; exit 70;;
  *nocrash*) echo "Executed $input in 1 ms"; exit 0;;
esac
harness=fixtures/all
case "$body" in *unregistered*) harness=fixtures/none;; esac
echo "zakura-sim-fuzz harness: $harness"
printf "thread 'zakura-sim-explore' (4242) panicked at src/fuzz.rs:198:9:\n%s\n" "$ZAKURA_SIM_EXPECT"
printf "thread '<unnamed>' (4241) panicked at library/core/src/panicking.rs:225:5:\n"
echo "panic in a function that cannot unwind"
echo "==1== ERROR: libFuzzer: deadly signal"
exit 77
"#;

/// `cargo fuzz tmin <input> <artifacts>`: copies each staged file into the
/// artifacts, then optionally changes the built target, sleeps, and exits
/// with `$EXIT`.
const TMIN: &str = r#"#!/bin/sh
[ -n "$ZAKURA_SIM_EXPECT" ] || exit 3
for f in "$STAGED"/*; do
  [ -f "$f" ] && cp "$f" "$2/minimized-from-$(basename "$f")"
done
[ -n "$REBUILD" ] && echo changed >> "$REBUILD"
[ -n "$SLEEP" ] && sleep "$SLEEP"
exit "${EXIT:-0}"
"#;

/// The original every test starts from, unless it says otherwise.
const ORIGINAL: &[u8] = b"good original input with padding";

/// A fake setup: the scripts, the original and `tmin`'s staged outputs.
struct Fake {
    dir: TempDir,
    request: Request,
}

/// How the fake `tmin` behaves.
#[derive(Default)]
struct TminScript {
    /// Seconds to sleep after staging its outputs.
    sleep: Option<u32>,
    /// Its exit code.
    exit: Option<i32>,
    /// Whether it changes the built target.
    rebuild: bool,
}

impl Fake {
    /// A setup with `original` and `tmin` outputs `staged`.
    fn new(original: &[u8], staged: &[&[u8]]) -> Fake {
        let dir = TempDir::new().unwrap();
        for (name, script) in [
            ("sim", SIM),
            ("fuzz", FUZZ),
            ("built-fuzz", FUZZ),
            ("tmin", TMIN),
        ] {
            let path = dir.path().join(name);
            fs::write(&path, script).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        }
        for name in ["sim", "fuzz", "tmin"] {
            wait_until_executable(&dir.path().join(name));
        }
        let staged_dir = dir.path().join("staged");
        fs::create_dir(&staged_dir).unwrap();
        for (index, bytes) in staged.iter().enumerate() {
            fs::write(staged_dir.join(format!("{index:02}")), bytes).unwrap();
        }
        let input = dir.path().join("crash");
        fs::write(&input, original).unwrap();
        let request = Request {
            target: "race_fixtures".to_owned(),
            harness: "fixtures/all".to_owned(),
            input,
            property: "race/lost-update".to_owned(),
            out: dir.path().join("out"),
            budget: Budget {
                runs: 1,
                wall: Duration::from_secs(20),
                child: Duration::from_secs(5),
            },
            sanitizer: Sanitizer::Address,
        };
        Fake { dir, request }
    }

    /// The fake tools, with `tmin` acting as `script` says.
    fn tools(&self, script: TminScript) -> Tools {
        let dir = self.dir.path().to_path_buf();
        let tmin = move |input: &Path, artifacts: &Path| {
            let mut command = Command::new(dir.join("tmin"));
            command
                .arg(input)
                .arg(artifacts)
                .env("STAGED", dir.join("staged"));
            if let Some(sleep) = script.sleep {
                command.env("SLEEP", sleep.to_string());
            }
            if let Some(exit) = script.exit {
                command.env("EXIT", exit.to_string());
            }
            if script.rebuild {
                command.env("REBUILD", dir.join("built-fuzz"));
            }
            command
        };
        Tools {
            sim: self.dir.path().join("sim"),
            fuzz: self.dir.path().join("fuzz"),
            tmin_binary: self.dir.path().join("built-fuzz"),
            tmin: Box::new(tmin),
            source: Source {
                git: "0".repeat(40),
                dirty: false,
            },
        }
    }

    /// Runs the wrapper with `script` and `interrupt`.
    fn run_with(&self, script: TminScript, interrupt: &AtomicBool) -> Report {
        let started = Instant::now();
        let original = preserve_original(&self.request).unwrap();
        minimize_with(
            &self.request,
            &self.tools(script),
            original,
            started,
            interrupt,
        )
        .unwrap()
    }

    /// Runs the wrapper with `script`, uninterrupted.
    fn run(&self, script: TminScript) -> Report {
        self.run_with(script, &AtomicBool::new(false))
    }

    /// A file under the output directory.
    fn out(&self, path: impl AsRef<Path>) -> PathBuf {
        self.request.out.join(path)
    }

    /// The report as written.
    fn written(&self) -> Value {
        read_json(&self.out(REPORT_FILE)).unwrap()
    }
}

/// Runs `script` until it starts. A child that another test's thread forks
/// while a script is open for writing keeps it open until that child execs,
/// and running the script meanwhile fails with ETXTBSY. Once it starts, no
/// process holds it open for writing, and no test writes it again.
fn wait_until_executable(script: &Path) {
    loop {
        match Command::new(script)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
        {
            Err(error) if error.raw_os_error() == Some(libc::ETXTBSY) => {
                thread::sleep(Duration::from_millis(10));
            }
            status => {
                status.unwrap();
                return;
            }
        }
    }
}

/// Each candidate's rejection reason and its detail's first word, smallest
/// candidate first.
fn reasons(report: &Report) -> Vec<(Option<Reason>, Option<Crash>, String)> {
    report
        .candidates
        .iter()
        .filter_map(|candidate| candidate.check.as_ref())
        .map(|check| {
            let word = check
                .detail
                .as_deref()
                .and_then(|detail| detail.split([':', ' ']).next())
                .unwrap_or_default()
                .to_owned();
            (check.reason.clone(), check.crash.clone(), word)
        })
        .collect()
}

/// Checks that the original's copy is intact, and that the report says so.
fn assert_original_kept(fake: &Fake, report: &Report) {
    assert_eq!(fs::read(fake.out(&report.original.path)).unwrap(), ORIGINAL);
    assert_eq!(report.original.sha256, sha256(ORIGINAL));
    assert_eq!(report.original.len, ORIGINAL.len() as u64);
}

/// Smallest first, a candidate that switches to an abort, a sanitizer
/// report, a timeout, another property, other assumptions, a lost witness
/// or incomplete evidence is rejected for that reason, and the first that
/// reproduces is accepted into `minimized/`.
#[test]
fn accepts_the_smallest_candidate_that_reproduces() {
    let staged: [&[u8]; 9] = [
        b"asan",
        b"abort",
        b"other",
        b"assume",
        b"witness",
        b"partial",
        b"timeout1",
        b"good candidate",
        b"good but larger candidate",
    ];
    let fake = Fake::new(ORIGINAL, &staged);
    let report = fake.run(TminScript::default());
    assert_eq!(report.status, Status::Minimized, "{report:#?}");
    assert_eq!(report.reason, None);
    assert_original_kept(&fake, &report);
    let original = report.original.check.as_ref().unwrap();
    assert!(original.accepted, "{original:?}");
    assert_eq!(
        reasons(&report),
        [
            (
                Some(Reason::WrongCrash),
                Some(Crash::Sanitizer),
                "the".to_owned()
            ),
            (
                Some(Reason::WrongCrash),
                Some(Crash::Abort),
                "the".to_owned()
            ),
            (
                Some(Reason::NotConfirmed),
                Some(Crash::Expected),
                "other-property".to_owned()
            ),
            (
                Some(Reason::NotConfirmed),
                Some(Crash::Expected),
                "assumptions-changed".to_owned()
            ),
            (
                Some(Reason::NotConfirmed),
                Some(Crash::Expected),
                "witness-lost".to_owned()
            ),
            (
                Some(Reason::NotConfirmed),
                Some(Crash::Expected),
                "incomplete-evidence".to_owned()
            ),
            (
                Some(Reason::WrongCrash),
                Some(Crash::Timeout),
                "the".to_owned()
            ),
            (None, Some(Crash::Expected), String::new()),
        ]
    );
    assert_eq!(
        report.counts,
        Counts {
            produced: 9,
            checked: 8,
            accepted: 1,
            rejected: 7,
        }
    );
    // The larger candidate was never checked.
    assert_eq!(report.candidates[8].check, None);

    let minimized = report.minimized.as_ref().unwrap();
    assert_eq!(minimized.dir, Path::new(MINIMIZED_DIR));
    assert_eq!(
        fs::read(fake.out(MINIMIZED_DIR).join(INPUT_FILE)).unwrap(),
        b"good candidate"
    );
    assert_eq!(minimized.sha256, sha256(b"good candidate"));
    assert!(fake.out(&minimized.trace).is_dir(), "{minimized:?}");
    assert!(minimized.trace.starts_with(MINIMIZED_DIR));

    let written = fake.written();
    assert_eq!(written["status"], "minimized");
    assert_eq!(written["format"], REPORT_FORMAT);
    assert_eq!(written["reduction"]["end"], "finished");
    assert_eq!(written["reduction"]["completed"], true);
    assert_eq!(written["candidates"][0]["check"]["crash"], "sanitizer");
    assert_eq!(
        written["candidates"][2]["check"]["confirmation"],
        "other-property"
    );
    assert_eq!(written["build"]["generator"], 7);
    assert_eq!(written["build"]["sanitizer"], "address");
    assert_eq!(written["budget"]["child_ms"], 5_000);
}

/// When every candidate switches crash, assumptions or property, the
/// original is kept and the report says why (review E-03).
#[test]
fn tmin_rejects_crash_switches() {
    let staged: [&[u8]; 4] = [b"abort", b"timeout", b"assume", b"other"];
    let fake = Fake::new(ORIGINAL, &staged);
    let report = fake.run(TminScript::default());
    assert_eq!(report.status, Status::OriginalKept, "{report:#?}");
    assert_eq!(
        report.reason.as_deref(),
        Some("every candidate smaller than the original was rejected")
    );
    assert_eq!(report.minimized, None);
    assert!(!fake.out(MINIMIZED_DIR).exists());
    assert_original_kept(&fake, &report);
    assert_eq!(report.counts.rejected, 4);
    assert_eq!(fake.written()["status"], "original-kept");
}

/// A `tmin` output no smaller than the original isn't a candidate, and a
/// run without one keeps the original.
#[test]
fn no_smaller_candidate_keeps_the_original() {
    let fake = Fake::new(
        ORIGINAL,
        &[ORIGINAL, b"good original input with padding, longer"],
    );
    let report = fake.run(TminScript::default());
    assert_eq!(report.status, Status::OriginalKept, "{report:#?}");
    assert_eq!(
        report.reason.as_deref(),
        Some("tmin produced no candidate smaller than the original")
    );
    assert_eq!(report.counts, Counts::default());
}

/// Two outputs with the same bytes are one candidate.
#[test]
fn equal_candidates_count_once() {
    let fake = Fake::new(ORIGINAL, &[b"other", b"other", b"good"]);
    let report = fake.run(TminScript::default());
    assert_eq!(report.status, Status::Minimized, "{report:#?}");
    assert_eq!(report.counts.produced, 2);
}

/// A confirmation child that hangs is killed at `--child-secs`, and the
/// candidate is rejected for it. So is one that dies from a signal, and
/// one that fails without writing a confirmation.
#[test]
fn misbehaving_children_reject_the_candidate() {
    let staged: [&[u8]; 3] = [b"hang-confirm", b"segv-confirm", b"fail-confirm"];
    let mut fake = Fake::new(ORIGINAL, &staged);
    fake.request.budget.child = Duration::from_secs(2);
    let started = Instant::now();
    let report = fake.run(TminScript::default());
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(report.status, Status::OriginalKept, "{report:#?}");
    let found: Vec<_> = reasons(&report)
        .into_iter()
        .map(|(reason, _, _)| reason)
        .collect();
    assert_eq!(
        found,
        [
            Some(Reason::ChildTimeout),
            Some(Reason::ChildCrashed),
            Some(Reason::ChildFailed),
        ]
    );
    let failed = report.candidates[2].check.as_ref().unwrap();
    assert!(
        failed
            .detail
            .as_deref()
            .unwrap()
            .ends_with("the trace can't go there"),
        "{failed:?}"
    );
}

/// A trace that a fresh `zakura-sim replay` doesn't replay Exact rejects a
/// candidate, and rejects an original before `tmin` runs.
#[test]
fn a_fresh_replay_mismatch_rejects_the_input() {
    let fake = Fake::new(ORIGINAL, &[b"replay-bad"]);
    let report = fake.run(TminScript::default());
    assert_eq!(report.status, Status::OriginalKept, "{report:#?}");
    assert_eq!(
        reasons(&report)[0].0,
        Some(Reason::ReplayMismatch),
        "{report:#?}"
    );

    let original = b"replay-bad original input";
    let fake = Fake::new(original, &[b"good"]);
    let report = fake.run(TminScript::default());
    assert_eq!(report.status, Status::OriginalRejected, "{report:#?}");
    assert_eq!(
        report.reason.as_deref(),
        Some("replay-mismatch: zakura-sim replay exited with code 1: 1 records differed")
    );
    assert_eq!(report.reduction.end, ReductionEnd::NotRun);
    assert!(!fake.out(TMIN_DIR).exists());
    assert_eq!(fs::read(fake.out(&report.original.path)).unwrap(), original);
}

/// An original that confirms another property, or that the filter doesn't
/// crash on, is rejected with that reason, and `tmin` never runs.
#[test]
fn an_unconfirmed_original_is_rejected() {
    for (original, reason) in [
        (
            &b"other original"[..],
            "not-confirmed: other-property: fake",
        ),
        (
            b"nocrash original",
            "wrong-crash: the fuzz target ended with no crash",
        ),
    ] {
        let fake = Fake::new(original, &[b"good"]);
        let report = fake.run(TminScript::default());
        assert_eq!(report.status, Status::OriginalRejected, "{report:#?}");
        assert_eq!(report.reason.as_deref(), Some(reason));
        assert_eq!(report.candidates, []);
        assert_eq!(fake.written()["status"], "original-rejected");
    }
}

/// When the wrapper itself fails, here on a `zakura-sim confirm` that
/// refuses its arguments, the run still ends with a report: `error`, with
/// the error as its reason, and the original kept. The command still fails.
#[test]
fn a_wrapper_failure_still_writes_a_report() {
    let original = b"refuse-confirm original input";
    let fake = Fake::new(original, &[b"good"]);
    let report = fake.run(TminScript::default());
    assert_eq!(report.status, Status::Error, "{report:#?}");
    let reason = report.reason.as_deref().unwrap();
    assert!(
        reason.starts_with(
            "zakura-sim confirm refused its arguments: fixtures/all doesn't register it; see "
        ),
        "{reason}"
    );
    assert_eq!(report.reduction.end, ReductionEnd::NotRun);
    assert_eq!(fs::read(fake.out(&report.original.path)).unwrap(), original);
    let written = fake.written();
    assert_eq!(written["status"], "error");
    assert_eq!(written["reason"], reason);
    let error = finish(&fake.request, &report).unwrap_err().to_string();
    assert!(
        error.starts_with("error: zakura-sim confirm refused"),
        "{error}"
    );
}

/// A target whose mapped harness `zakura-sim` doesn't register ends the run
/// with an error that names the harness, before any check runs.
#[test]
fn an_unregistered_harness_is_an_error() {
    let original = b"unregistered original input";
    let mut fake = Fake::new(original, &[b"good"]);
    fake.request.harness = "fixtures/none".to_owned();
    let report = fake.run(TminScript::default());
    assert_eq!(report.status, Status::Error, "{report:#?}");
    let reason = report.reason.as_deref().unwrap();
    assert!(
        reason.starts_with(
            "zakura-sim properties refused its arguments: no harness is named `fixtures/none`; see "
        ),
        "{reason}"
    );
    assert_eq!(report.original.check, None);
    assert_eq!(report.reduction.end, ReductionEnd::NotRun);
    assert!(!fake.out(TMIN_DIR).exists());
}

/// A `--property` the harness doesn't register ends the run with an error
/// before the fuzz target runs, not with `original-rejected`.
#[test]
fn an_unregistered_property_is_an_error() {
    let mut fake = Fake::new(ORIGINAL, &[b"good"]);
    fake.request.property = "race/no-such-property".to_owned();
    let report = fake.run(TminScript::default());
    assert_eq!(report.status, Status::Error, "{report:#?}");
    let reason = report.reason.as_deref().unwrap();
    assert!(
        reason.starts_with("fixtures/all doesn't register property `race/no-such-property`; see "),
        "{reason}"
    );
    assert_eq!(report.original.check, None);
    assert!(!fake.out(ORIGINAL_DIR).join("fuzz.log").exists());
    assert_eq!(report.reduction.end, ReductionEnd::NotRun);
    assert!(report.build.is_some());
    assert_eq!(fake.written()["status"], "error");
}

/// A wrapper failure after `tmin`, here on a candidate that `zakura-sim
/// confirm` refuses, keeps the report as far as it got: the build, the
/// original's check, the reduction and the candidates.
#[test]
fn a_late_wrapper_failure_keeps_the_report() {
    let fake = Fake::new(ORIGINAL, &[b"refuse-confirm"]);
    let report = fake.run(TminScript::default());
    assert_eq!(report.status, Status::Error, "{report:#?}");
    let reason = report.reason.as_deref().unwrap();
    assert!(
        reason.starts_with("zakura-sim confirm refused its arguments: "),
        "{reason}"
    );
    assert!(report.build.as_ref().unwrap().sim_build.is_some());
    assert!(report.original.check.as_ref().unwrap().accepted);
    assert_eq!(report.reduction.end, ReductionEnd::Finished);
    assert_eq!(report.counts.produced, 1);
    assert_eq!(report.candidates.len(), 1);
    assert_eq!(report.candidates[0].check, None);
    let written = fake.written();
    assert_eq!(written["status"], "error");
    assert_eq!(written["reduction"]["end"], "finished");
    assert_eq!(written["candidates"].as_array().unwrap().len(), 1);
}

/// A target that runs another harness than the manifest maps it to ends
/// the run with an error, before `tmin` runs.
#[test]
fn a_target_running_another_harness_is_an_error() {
    let mut fake = Fake::new(ORIGINAL, &[b"good"]);
    fake.request.harness = "discovery/wait-complete".to_owned();
    let report = fake.run(TminScript::default());
    assert_eq!(report.status, Status::Error, "{report:#?}");
    assert_eq!(
        report.reason.as_deref(),
        Some(
            "the fuzz target `race_fixtures` ran harness `fixtures/all`, \
             not `discovery/wait-complete` as its manifest maps it"
        )
    );
    assert_eq!(report.reduction.end, ReductionEnd::NotRun);
    assert!(!fake.out(TMIN_DIR).exists());
}

/// An interrupt while `tmin` runs kills it, keeps the original and `tmin`'s
/// outputs, and ends the run as interrupted, with no candidate checked.
#[test]
fn an_interrupted_reduction_keeps_the_original() {
    let fake = Fake::new(ORIGINAL, &[b"good"]);
    let interrupt = AtomicBool::new(false);
    let started = Instant::now();
    let report = thread::scope(|scope| {
        scope.spawn(|| {
            thread::sleep(Duration::from_millis(500));
            interrupt.store(true, Ordering::SeqCst);
        });
        fake.run_with(
            TminScript {
                sleep: Some(30),
                ..TminScript::default()
            },
            &interrupt,
        )
    });
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(report.status, Status::Interrupted, "{report:#?}");
    assert_eq!(report.reduction.end, ReductionEnd::Interrupted);
    assert!(!report.reduction.completed);
    assert_original_kept(&fake, &report);
    assert!(report.original.check.as_ref().unwrap().accepted);
    assert_eq!(report.counts.produced, 1);
    assert_eq!(report.counts.checked, 0);
    assert!(fake.out(&report.candidates[0].file).is_file());
    assert_eq!(fake.written()["status"], "interrupted");
}

/// `tmin` stopped at `--wall-secs` is an incomplete reduction, and its
/// outputs so far are still checked.
#[test]
fn the_wall_budget_stops_tmin_and_keeps_its_candidates() {
    let mut fake = Fake::new(ORIGINAL, &[b"good"]);
    fake.request.budget.wall = Duration::from_millis(500);
    let report = fake.run(TminScript {
        sleep: Some(30),
        ..TminScript::default()
    });
    assert_eq!(report.reduction.end, ReductionEnd::WallBudget);
    assert!(!report.reduction.completed);
    assert!(report.reduction.elapsed < Duration::from_secs(10));
    assert_eq!(report.status, Status::Minimized, "{report:#?}");
}

/// `tmin` that exits with an error is a failed reduction; its outputs are
/// still checked.
#[test]
fn a_failed_tmin_is_reported() {
    let fake = Fake::new(ORIGINAL, &[b"other"]);
    let report = fake.run(TminScript {
        exit: Some(1),
        ..TminScript::default()
    });
    assert_eq!(report.reduction.end, ReductionEnd::Failed);
    assert_eq!(
        report.reduction.detail.as_deref(),
        Some("tmin exited with code 1")
    );
    assert_eq!(report.status, Status::OriginalKept, "{report:#?}");
}

/// When the target `tmin` ran isn't the pinned one, no candidate is
/// accepted.
#[test]
fn a_rebuilt_target_rejects_every_candidate() {
    let fake = Fake::new(ORIGINAL, &[b"good"]);
    let report = fake.run(TminScript {
        rebuild: true,
        ..TminScript::default()
    });
    assert_eq!(report.status, Status::OriginalKept, "{report:#?}");
    assert_eq!(reasons(&report)[0].0, Some(Reason::BuildChanged));
}

/// The output directory must be missing or empty, and the copy keeps the
/// input's bytes.
#[test]
fn the_original_is_copied_into_an_empty_directory() {
    let fake = Fake::new(ORIGINAL, &[]);
    let original = preserve_original(&fake.request).unwrap();
    assert_eq!(original.sha256, sha256(ORIGINAL));
    assert_eq!(fs::read(fake.out(&original.path)).unwrap(), ORIGINAL);
    let error = preserve_original(&fake.request).unwrap_err().to_string();
    assert!(error.ends_with("isn't empty"), "{error}");
}

#[test]
fn crashes_are_classified_from_libfuzzer_output() {
    let property = "race/lost-update";
    // The thread lines as Rust 1.98 writes them, with each thread's ID. The
    // filter's panic is followed by the abort of unwinding out of the target.
    let expected = "Running: input.bin\n\nthread 'zakura-sim-explore' (880381) panicked at src/fuzz.rs:198:9:\nrace/lost-update\nstack backtrace:\n\nthread '<unnamed>' (880379) panicked at library/core/src/panicking.rs:225:5:\npanic in a function that cannot unwind\nthread caused non-unwinding panic. aborting.\n==1== ERROR: libFuzzer: deadly signal\n";
    let unnamed = "thread '<unnamed>' panicked at src/fuzz.rs:198:9:\nrace/lost-update\n==1== ERROR: libFuzzer: deadly signal\n";
    let task_panic_first = "thread 'zakura-sim-run' panicked at src/x.rs:1:1:\nplanted task panic\nthread '<unnamed>' panicked at src/fuzz.rs:1:1:\nrace/lost-update\n==1== ERROR: libFuzzer: deadly signal\n";
    let other = "thread '<unnamed>' panicked at src/fuzz.rs:1:1:\nharness: boom\n==1== ERROR: libFuzzer: deadly signal\n";
    for (end, log, crash) in [
        (ChildEnd::Exited(77), expected, Crash::Expected),
        (ChildEnd::Exited(77), unnamed, Crash::Expected),
        (ChildEnd::Exited(77), task_panic_first, Crash::Expected),
        (
            ChildEnd::Exited(77),
            other,
            Crash::OtherPanic("harness: boom".to_owned()),
        ),
        (ChildEnd::Exited(0), expected, Crash::None),
        (
            ChildEnd::Exited(70),
            "==1== ERROR: libFuzzer: timeout after 5 seconds",
            Crash::Timeout,
        ),
        (
            ChildEnd::Exited(77),
            "==1== ERROR: libFuzzer: out-of-memory (used: 3000Mb; limit: 2048Mb)",
            Crash::OutOfMemory,
        ),
        (
            ChildEnd::Exited(1),
            "==1==ERROR: AddressSanitizer: stack-overflow on address",
            Crash::Sanitizer,
        ),
        (
            ChildEnd::Exited(77),
            "==1== ERROR: libFuzzer: deadly signal",
            Crash::Abort,
        ),
        (ChildEnd::Signaled(6), "", Crash::Abort),
        (
            ChildEnd::Exited(3),
            "no filter",
            Crash::Other("exited with code 3".to_owned()),
        ),
        // A panic with the property's ID but no abort after it isn't the
        // filter's crash.
        (
            ChildEnd::Exited(101),
            "thread 'main' panicked at src/main.rs:1:1:\nrace/lost-update\n",
            Crash::OtherPanic("race/lost-update".to_owned()),
        ),
    ] {
        assert_eq!(classify_crash(end, log, property), crash, "{log}");
    }
}

#[test]
fn arguments_need_explicit_budgets_and_a_known_target() {
    let root = crate::repo_root().unwrap();
    let parse_args = |args: &str| parse(&root, args.split_whitespace().map(str::to_owned));
    let full = "race_fixtures crash --property race/lost-update --out out --runs 64 --wall-secs 60 --child-secs 10";
    let request = parse_args(full).unwrap();
    assert_eq!(request.harness, "fixtures/all");
    // `tmin` runs in the fuzz package, so relative paths become absolute.
    let cwd = env::current_dir().unwrap();
    assert_eq!(request.out, cwd.join("out"));
    assert_eq!(request.input, cwd.join("crash"));
    assert_eq!(
        request.budget,
        Budget {
            runs: 64,
            wall: Duration::from_secs(60),
            child: Duration::from_secs(10),
        }
    );
    assert_eq!(request.sanitizer, Sanitizer::Address);
    let none = parse_args(&format!("{full} --sanitizer none")).unwrap();
    assert_eq!(none.sanitizer, Sanitizer::None);

    for (args, error) in [
        (
            full.replace(" --runs 64", ""),
            "`sim-minimize` needs --runs",
        ),
        (
            full.replace("--wall-secs 60", "--wall-secs 0"),
            "--wall-secs must be at least 1",
        ),
        (
            full.replace(" --child-secs 10", ""),
            "`sim-minimize` needs --child-secs",
        ),
        (
            full.replace("race_fixtures", "header_pursuit"),
            "unknown simulation fuzz target `header_pursuit`; expected one of \
             discovery_wait_complete, header_sync_storage, race_fixtures",
        ),
        (format!("{full} --runs 1"), "--runs given twice"),
        (
            format!("{full} --frobnicate 1"),
            "unknown flag `--frobnicate`",
        ),
        (
            format!("{full} extra"),
            "`sim-minimize` takes a target and an input",
        ),
    ] {
        let found = parse_args(&args).unwrap_err().to_string();
        assert_eq!(found, error, "{args}");
    }
}

/// `tmin` gets the attempts per step, and `--child-secs` as libFuzzer's
/// timeout for one input.
#[test]
fn tmin_gets_the_budget() {
    let build = FuzzBuild {
        fuzz_dir: PathBuf::from("/fuzz"),
        target_dir: PathBuf::from("/target"),
        sanitizer: Sanitizer::None,
    };
    let budget = Budget {
        runs: 64,
        wall: Duration::from_secs(60),
        child: Duration::from_secs(7),
    };
    let command = build.tmin("race_fixtures", budget, Path::new("/in"), Path::new("/out"));
    let args: Vec<_> = command
        .get_args()
        .map(|arg| arg.to_str().unwrap())
        .collect();
    let tail = args.iter().position(|arg| *arg == "tmin").unwrap();
    assert_eq!(
        args[tail..],
        [
            "tmin",
            "--fuzz-dir",
            "/fuzz",
            "--target-dir",
            "/target",
            "-s",
            "none",
            "--no-default-features",
            "--runs",
            "64",
            "race_fixtures",
            "/in",
            "--",
            "-timeout=7",
            "-artifact_prefix=/out/",
        ]
    );
}

/// The fuzz package's manifest maps its targets.
#[test]
fn the_fuzz_package_maps_its_targets() {
    let root = crate::repo_root().unwrap();
    for (target, harness) in [
        ("discovery_wait_complete", "discovery/wait-complete"),
        ("race_fixtures", "fixtures/all"),
    ] {
        assert_eq!(resolve_target(&root, target).unwrap(), harness);
    }
}

/// A checkout whose fuzz package declares `bins` and maps `mapped` to
/// harnesses.
fn fuzz_package(bins: &[&str], mapped: &[(&str, &str)]) -> TempDir {
    let root = TempDir::new().unwrap();
    let package = fuzz_dir(root.path());
    fs::create_dir_all(package.join("fuzz_targets")).unwrap();
    let mut manifest = "[package]\nname = \"zakura-sim-fuzz\"\nversion = \"0.0.0\"\n\
                        publish = false\nedition = \"2021\"\n\n\
                        [package.metadata.zakura-sim.targets]\n"
        .to_owned();
    for (target, harness) in mapped {
        manifest.push_str(&format!("{target} = \"{harness}\"\n"));
    }
    for bin in bins {
        let path = format!("fuzz_targets/{bin}.rs");
        fs::write(package.join(&path), "fn main() {}\n").unwrap();
        manifest.push_str(&format!("\n[[bin]]\nname = \"{bin}\"\npath = \"{path}\"\n"));
    }
    manifest.push_str("\n[workspace]\n");
    fs::write(package.join("Cargo.toml"), manifest).unwrap();
    root
}

/// A target that the fuzz package declares and maps resolves to its
/// harness, with no other list to edit.
#[test]
fn a_declared_and_mapped_target_resolves() {
    let bins = ["discovery_wait_complete", "race_fixtures", "header_storage"];
    let root = fuzz_package(
        &bins,
        &[
            ("discovery_wait_complete", "discovery/wait-complete"),
            ("race_fixtures", "fixtures/all"),
            ("header_storage", "header-sync/storage"),
        ],
    );
    assert_eq!(
        resolve_target(root.path(), "header_storage").unwrap(),
        "header-sync/storage"
    );
    assert_eq!(
        resolve_target(root.path(), "race_fixtures").unwrap(),
        "fixtures/all"
    );
}

/// A `[[bin]]` without a harness fails only its own minimization, with an
/// error that names it. Another target still resolves.
#[test]
fn an_unmapped_bin_blocks_only_its_own_minimization() {
    let root = fuzz_package(
        &["race_fixtures", "feedback_probe"],
        &[("race_fixtures", "fixtures/all")],
    );
    assert_eq!(
        resolve_target(root.path(), "race_fixtures").unwrap(),
        "fixtures/all"
    );
    assert_eq!(
        resolve_target(root.path(), "feedback_probe")
            .unwrap_err()
            .to_string(),
        format!(
            "{} declares fuzz target `feedback_probe` but maps it to no harness \
             in [package.metadata.zakura-sim.targets]",
            fuzz_dir(root.path()).join("Cargo.toml").display()
        )
    );
}

/// A harness without a `[[bin]]` fails every resolution with an error that
/// names it.
#[test]
fn every_harness_needs_a_bin() {
    let undeclared = fuzz_package(
        &["race_fixtures"],
        &[
            ("race_fixtures", "fixtures/all"),
            ("header_storage", "header-sync/storage"),
        ],
    );
    let manifest = fuzz_dir(undeclared.path()).join("Cargo.toml");
    assert_eq!(
        resolve_target(undeclared.path(), "race_fixtures")
            .unwrap_err()
            .to_string(),
        format!(
            "{} maps `header_storage` to a harness in \
             [package.metadata.zakura-sim.targets] but declares no such [[bin]]",
            manifest.display()
        )
    );
}

/// A target the fuzz package doesn't declare fails, and the error lists the
/// targets that map to a harness.
#[test]
fn an_undeclared_target_is_unknown() {
    let root = fuzz_package(
        &["race_fixtures", "feedback_probe"],
        &[("race_fixtures", "fixtures/all")],
    );
    assert_eq!(
        resolve_target(root.path(), "header_storage")
            .unwrap_err()
            .to_string(),
        "unknown simulation fuzz target `header_storage`; expected one of race_fixtures"
    );
}

/// Reading the checkout's provenance leaves the git dir alone, so `cargo
/// fuzz tmin` doesn't rebuild the target `zakura-sim`'s build script ties
/// to it.
#[test]
fn reading_the_source_leaves_the_git_dir_alone() {
    let dir = TempDir::new().unwrap();
    let repo = dir.path();
    let git = |args: &[&str]| {
        let status = Command::new("git")
            .args(["-c", "user.name=t", "-c", "user.email=t@t"])
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .unwrap()
            .status;
        assert!(status.success(), "git {args:?}");
    };
    git(&["init", "-q"]);
    fs::write(repo.join("a"), "a").unwrap();
    git(&["add", "a"]);
    git(&["commit", "-q", "-m", "a"]);
    let modified = || fs::metadata(repo.join(".git")).unwrap().modified().unwrap();
    let before = modified();
    let source = source(repo);
    assert_eq!(modified(), before);
    assert_eq!(source.git.len(), 40, "{source:?}");
    assert!(!source.dirty, "{source:?}");
}

/// SIGINT, SIGTERM and SIGHUP set the interrupt flag instead of killing
/// the wrapper.
#[test]
fn signals_set_the_interrupt_flag() {
    install_interrupt_handler();
    for signal in [libc::SIGINT, libc::SIGTERM, libc::SIGHUP] {
        INTERRUPTED.store(false, Ordering::SeqCst);
        // SAFETY: `raise` has no memory-safety preconditions, and the
        // handler only stores to an atomic.
        unsafe {
            libc::raise(signal);
        }
        assert!(INTERRUPTED.load(Ordering::SeqCst), "signal {signal}");
    }
}
