//! `cargo xtask sim-feedback`: checks that libFuzzer keeps an input for a
//! semantic feature alone (explorer implementation plan, E2a).
//!
//! The check builds `qa/fuzz/sim`'s `feedback_probe` target with the pinned
//! nightly and `cargo-fuzz`, then runs libFuzzer's corpus merge on probe
//! inputs. Every probe input takes the same code path, and each scenario
//! byte reaches its own feature. A merge adds an input from the second
//! directory to the first only if the input reaches a feature the first
//! directory's inputs don't.
//!
//! The first directory holds a warm-up input and input A, and the second
//! holds input B; then A and B swap. A merge's inner process runs the first
//! directory's inputs before the second's. The warm-up and A have the same
//! size, so libFuzzer doesn't fix their order. Whichever runs first takes
//! the process's one-time code paths, such as a `OnceLock`'s
//! initialization. The other runs after it, as B does, so the corpus covers
//! the code paths every later input takes. libFuzzer's fuzzing loop runs an
//! empty input first for the same reason, but a merge skips empty files.
//! Each merge runs twice:
//!
//! - With `ZAKURA_SIM_FEEDBACK=off`, the collector writes to counters
//!   libFuzzer never reads. The merge must add nothing, so libFuzzer sees
//!   the same code edges for A and B.
//! - With `ZAKURA_SIM_FEEDBACK=on`, the collector writes to libFuzzer's extra
//!   counters. The merge must add the input, with exactly one new feature:
//!   its state's.
//!
//! In both modes the merge must report no new coverage edges. Otherwise the new
//! feature could be a code edge rather than the state's.
//!
//! The target logs its feature count, and libFuzzer logs how many extra
//! counters it found. The two must agree, or the counters didn't reach
//! libFuzzer. Every mismatch fails the check with its reason.

use std::{
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use crate::{header_fuzz::PINNED_NIGHTLY, print_command, sim::SIM_RUSTFLAGS, BoxError};

/// The probe's fuzz target.
const TARGET: &str = "feedback_probe";

/// The inputs: one scenario byte each, so each is a distinct probe state.
const WARM_UP: &[u8] = &[0];
const A: &[u8] = &[1];
const B: &[u8] = &[2];

/// One merge: whether feedback is on, and which input is new.
#[derive(Clone, Copy, Debug)]
struct Case {
    /// `ZAKURA_SIM_FEEDBACK`.
    mode: &'static str,
    /// The input already in the corpus, beside the warm-up.
    kept: (&'static str, &'static [u8]),
    /// The input the merge offers.
    offered: (&'static str, &'static [u8]),
}

impl Case {
    /// How many inputs the merge must add.
    fn expected_added(self) -> usize {
        usize::from(self.mode == "on")
    }
}

/// What a merge's log says.
#[derive(Debug, PartialEq, Eq)]
struct MergeLog {
    /// The mode the target logged.
    mode: String,
    /// The feature count the target logged.
    features: usize,
    /// The extra counters libFuzzer found.
    extra_counters: usize,
    /// The inputs the merge added.
    added: usize,
    /// The features they added.
    new_features: usize,
    /// The coverage edges they added.
    new_edges: usize,
}

pub(super) fn run(repo_root: &Path) -> Result<(), BoxError> {
    let fuzz_dir = repo_root.join("qa").join("fuzz").join("sim");
    let scratch = tempfile::Builder::new()
        .prefix("zakura-sim-feedback-")
        .tempdir()?;
    let cases = [
        ("off", ("a", A), ("b", B)),
        ("off", ("b", B), ("a", A)),
        ("on", ("a", A), ("b", B)),
        ("on", ("b", B), ("a", A)),
    ]
    .map(|(mode, kept, offered)| Case {
        mode,
        kept,
        offered,
    });
    let mut report = String::new();
    let mut failures = Vec::new();
    for (i, case) in cases.into_iter().enumerate() {
        let dir = scratch.path().join(i.to_string());
        let log = merge(&fuzz_dir, &dir, case)?;
        let verdict = judge(case, &log);
        writeln!(
            report,
            "mode={} kept={} offered={} added={} new_features={} new_edges={} extra_counters={}: {}",
            case.mode,
            case.kept.0,
            case.offered.0,
            log.added,
            log.new_features,
            log.new_edges,
            log.extra_counters,
            verdict.as_deref().unwrap_or("ok"),
        )?;
        if let Some(reason) = verdict {
            failures.push(reason);
        }
    }
    print!("{report}");
    if failures.is_empty() {
        println!("libFuzzer kept each offered input for its semantic feature alone");
        Ok(())
    } else {
        Err(failures.join("; ").into())
    }
}

/// Runs `case`'s merge in `dir` and parses its log. The corpus directory
/// must end with the warm-up and the kept input, plus each added one.
fn merge(fuzz_dir: &Path, dir: &Path, case: Case) -> Result<MergeLog, BoxError> {
    let corpus = dir.join("corpus");
    let offered = dir.join("offered");
    fs::create_dir_all(&corpus)?;
    fs::create_dir_all(&offered)?;
    fs::write(corpus.join("warm-up"), WARM_UP)?;
    fs::write(corpus.join(case.kept.0), case.kept.1)?;
    fs::write(offered.join(case.offered.0), case.offered.1)?;

    let mut command = Command::new("cargo");
    command
        .arg(format!("+{PINNED_NIGHTLY}"))
        .arg("fuzz")
        .arg("run")
        .arg("--fuzz-dir")
        .arg(fuzz_dir)
        .arg(TARGET)
        .arg(&corpus)
        .arg(&offered)
        .arg("--")
        .arg("-merge=1")
        .current_dir(fuzz_dir)
        .env("RUSTFLAGS", SIM_RUSTFLAGS)
        .env("ZAKURA_SIM_FEEDBACK", case.mode);
    print_command(&command);
    let output = command.output()?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !output.status.success() {
        return Err(format!(
            "the {} merge failed with status {}:\n{stderr}",
            case.mode, output.status
        )
        .into());
    }
    let log = parse(&stderr)
        .map_err(|missing| format!("the {} merge's log has no {missing}:\n{stderr}", case.mode))?;
    let files = corpus_files(&corpus)?;
    if files != 2 + log.added {
        return Err(format!(
            "the {} merge reported {} added inputs, but the corpus holds {files} files",
            case.mode, log.added
        )
        .into());
    }
    Ok(log)
}

/// The number of files in `dir`.
fn corpus_files(dir: &Path) -> Result<usize, BoxError> {
    let mut files = 0;
    for entry in fs::read_dir(dir)? {
        let path: PathBuf = entry?.path();
        if path.is_file() {
            files += 1;
        }
    }
    Ok(files)
}

/// Why `log` fails `case`, or `None` if it passes.
fn judge(case: Case, log: &MergeLog) -> Option<String> {
    let what = format!(
        "mode={} kept={} offered={}",
        case.mode, case.kept.0, case.offered.0
    );
    if log.mode != case.mode {
        return Some(format!("{what}: the target ran with mode={}", log.mode));
    }
    if log.extra_counters != log.features {
        return Some(format!(
            "{what}: libFuzzer found {} extra counters, but the target has {} features",
            log.extra_counters, log.features
        ));
    }
    if log.new_edges != 0 {
        return Some(format!(
            "{what}: the merge added {} new coverage edges, expected 0",
            log.new_edges
        ));
    }
    let added = case.expected_added();
    if log.added != added || log.new_features != added {
        return Some(format!(
            "{what}: the merge added {} inputs with {} new features, expected {added} and {added}",
            log.added, log.new_features
        ));
    }
    None
}

/// Parses a merge's log, or names the line it lacks.
fn parse(log: &str) -> Result<MergeLog, &'static str> {
    let feedback = log
        .lines()
        .find_map(|line| line.strip_prefix("zakura-sim feedback: "))
        .ok_or("`zakura-sim feedback:` line")?;
    let field = |name: &str| {
        feedback
            .split_whitespace()
            .find_map(|pair| pair.strip_prefix(name)?.strip_prefix('='))
    };
    let mode = field("mode").ok_or("feedback mode")?.to_owned();
    let features = field("features")
        .and_then(|n| n.parse().ok())
        .ok_or("feedback feature count")?;
    let extra_counters = log
        .lines()
        .find_map(|line| {
            line.strip_prefix("INFO: ")?
                .strip_suffix(" Extra Counters")?
                .parse()
                .ok()
        })
        .ok_or("`INFO: <n> Extra Counters` line")?;
    // `MERGE-OUTER: 1 new files with 1 new features added; 0 new coverage edges`
    let (added, new_features, new_edges) = log
        .lines()
        .find_map(|line| {
            let rest = line.strip_prefix("MERGE-OUTER: ")?;
            let (added, rest) = rest.split_once(" new files with ")?;
            let (new_features, rest) = rest.split_once(" new features added; ")?;
            let new_edges = rest.strip_suffix(" new coverage edges")?;
            Some((
                added.parse().ok()?,
                new_features.parse().ok()?,
                new_edges.parse().ok()?,
            ))
        })
        .ok_or(MERGE_SUMMARY)?;
    Ok(MergeLog {
        mode,
        features,
        extra_counters,
        added,
        new_features,
        new_edges,
    })
}

/// The merge summary line `parse` requires.
const MERGE_SUMMARY: &str =
    "`MERGE-OUTER: <n> new files with <n> new features added; <n> new coverage edges` line";

#[cfg(test)]
mod tests {
    use super::*;

    const LOG: &str = "\
zakura-sim feedback: backend=libfuzzer-extra-counters version=1 features=65536 mode=on
INFO: 65536 Extra Counters
MERGE-OUTER: 2 files, 2 in the initial corpus, 0 processed earlier
MERGE-OUTER: 1 new files with 1 new features added; 0 new coverage edges
";

    fn case(mode: &'static str) -> Case {
        Case {
            mode,
            kept: ("a", A),
            offered: ("b", B),
        }
    }

    #[test]
    fn parses_a_merge_log() {
        assert_eq!(
            parse(LOG),
            Ok(MergeLog {
                mode: "on".to_owned(),
                features: 65536,
                extra_counters: 65536,
                added: 1,
                new_features: 1,
                new_edges: 0,
            })
        );
    }

    #[test]
    fn names_each_missing_line() {
        let without = |prefix: &str| {
            LOG.lines()
                .filter(|line| !line.starts_with(prefix))
                .collect::<Vec<_>>()
                .join("\n")
        };
        assert_eq!(
            parse(&without("zakura-sim")),
            Err("`zakura-sim feedback:` line")
        );
        assert_eq!(
            parse(&without("INFO")),
            Err("`INFO: <n> Extra Counters` line")
        );
        assert_eq!(parse(&without("MERGE-OUTER: 1")), Err(MERGE_SUMMARY));
        assert_eq!(
            parse(&LOG.replace("; 0 new coverage edges", "")),
            Err(MERGE_SUMMARY),
            "the coverage-edge field is required"
        );
    }

    #[test]
    fn judges_each_mode() {
        let log = parse(LOG).unwrap();
        assert_eq!(judge(case("on"), &log), None);
        assert!(judge(case("off"), &log).is_some(), "mode mismatch");
        let hidden = MergeLog {
            mode: "off".to_owned(),
            added: 0,
            new_features: 0,
            ..parse(LOG).unwrap()
        };
        assert_eq!(judge(case("off"), &hidden), None);
        let unread = MergeLog {
            extra_counters: 0,
            ..parse(LOG).unwrap()
        };
        assert!(judge(case("on"), &unread)
            .unwrap()
            .contains("found 0 extra counters"));
        let edge = MergeLog {
            new_features: 2,
            ..parse(LOG).unwrap()
        };
        assert!(judge(case("on"), &edge).is_some(), "an edge came with it");
    }

    #[test]
    fn rejects_a_new_coverage_edge() {
        let log = LOG.replace("0 new coverage edges", "1 new coverage edges");
        let verdict = judge(case("on"), &parse(&log).unwrap());
        assert!(verdict.unwrap().contains("1 new coverage edges"));
        let hidden = MergeLog {
            mode: "off".to_owned(),
            added: 0,
            new_features: 0,
            new_edges: 1,
            ..parse(LOG).unwrap()
        };
        assert!(judge(case("off"), &hidden)
            .unwrap()
            .contains("1 new coverage edges"));
    }
}
