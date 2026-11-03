//! The minimized bundle beside a confirmed `Active` failure's bundle
//! (explorer plan, E4a; traces plan, T2b).
//!
//! Triage places the failure's bundle before the minimizer starts, so a
//! minimizer that fails or is killed never costs the failure its bundle.
//! Then [`minimize`]:
//!
//! 1. keeps the first minimized bundle beside the failure's bundle that
//!    passes [`verify`], if one is there, and runs nothing;
//! 2. runs the minimizer ([`Children::minimize`]) on the bundle's
//!    [`BUNDLE_INPUT_FILE`], with its output under `<out>/.staging/`. Triage
//!    sets no time limit on it: `cargo xtask sim-minimize` bounds `tmin` and
//!    each check with its own explicit budgets, and killing it would leave
//!    its children running;
//! 3. copies the minimizer's output, but its pinned binaries, into a
//!    staging bundle's [`MINIMIZER_DIR`], and holds that output to the
//!    failure's bundle ([`check`]);
//! 4. replays the minimized trace Exact in a fresh child;
//! 5. writes [`MINIMIZED_FILE`] and [`CHECKSUMS_FILE`], and renames the
//!    bundle to `<bundle name>.minimized`, or beside it as
//!    `<bundle name>.minimized-<n>`.
//!
//! A minimized bundle never changes after its rename, as a failure's bundle
//! doesn't. Its [`MINIMIZED_FILE`] names the failure's bundle and the
//! SHA-256 of that bundle's [`CHECKSUMS_FILE`], so [`verify`] finds a
//! minimized bundle whose failure's bundle changed, or that sits beside
//! another bundle.
//!
//! The minimizer's own fresh processes confirm the candidate: the fuzz
//! target, `zakura-sim confirm --against` the original's confirmation, and
//! `zakura-sim replay`. [`check`] reads their records. It holds both
//! confirmations to the failure's bundle: the same harness, generator,
//! property and input, and an equivalent build. It also holds the
//! candidate's confirmation to the original's again with
//! [`Confirmation::against`], so the assumptions, the witness and the
//! cleanup match too.

use std::{
    fs::{self, File, OpenOptions},
    io,
    path::{Component, Path, PathBuf},
};

use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::{
    new_staging, place, run_child, verify_bundle, write_synced, Children, TriageOptions,
    BUNDLE_INPUT_FILE, CHECKSUMS_FILE, REPLAY_DIR, RUNS_DIR, TRIAGE_FILE,
};
use crate::{
    explore::ProcessExit,
    trace::{
        meta::{hex, BuildInfo},
        ConfirmStatus, Confirmation, TraceReader,
    },
};

/// What a minimized bundle holds, as JSON: the failure's bundle it belongs
/// to, the minimized input, the replay and the build.
pub const MINIMIZED_FILE: &str = "minimized.json";

/// [`MINIMIZED_FILE`]'s format version.
pub const MINIMIZED_FORMAT: u32 = 1;

/// The minimizer's output in a minimized bundle, without its pinned
/// binaries.
pub const MINIMIZER_DIR: &str = "minimizer";

/// What a minimized bundle's name adds to its failure's bundle's name,
/// before an optional `-<n>`.
pub const MINIMIZED_SUFFIX: &str = ".minimized";

/// The minimizer's report in its output directory: `cargo xtask
/// sim-minimize`'s `REPORT_FILE`.
const REPORT_FILE: &str = "report.json";

/// The minimizer's report format that [`check`] reads: `cargo xtask
/// sim-minimize`'s `REPORT_FORMAT`.
const REPORT_FORMAT: u64 = 1;

/// The minimizer's report status for an accepted candidate.
const MINIMIZED_STATUS: &str = "minimized";

/// An input's bytes in its directory in the minimizer's output: `cargo
/// xtask sim-minimize`'s `INPUT_FILE`.
const MINIMIZER_INPUT_FILE: &str = "input.bin";

/// The minimizer's pinned binaries in its output directory, which the
/// bundle leaves out.
const PINNED_DIR: &str = "bin";

/// The staging bundle in its staging directory.
const STAGING_BUNDLE: &str = "bundle";

/// The minimizer's output directory in the staging directory.
const STAGING_OUTPUT: &str = "output";

/// The minimizer's run directory under the bundle's [`RUNS_DIR`].
const MINIMIZER_RUN: &str = "minimizer";

/// The [`MINIMIZED_FILE`] fields that name a minimized bundle's result.
const RESULT_FIELDS: [&str; 7] = [
    "format",
    "harness",
    "generator",
    "provenance",
    "signature",
    "original",
    "input",
];

/// What minimizing a confirmed failure came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Minimization {
    /// A minimized bundle beside the failure's bundle holds the best
    /// candidate.
    Kept {
        /// The minimized bundle's directory.
        bundle: PathBuf,
        /// False if a minimized bundle that passes [`verify`] was already
        /// there, so the minimizer didn't run.
        archived: bool,
    },
    /// The minimizer kept no candidate, or its output failed a check, so
    /// triage archived nothing.
    NotKept {
        /// Why.
        reason: String,
        /// The staging directory that keeps the minimizer's output,
        /// without its pinned binaries.
        staging: PathBuf,
    },
}

/// Minimizes the failure in the bundle `original`, whose run violates the
/// `Active` property `property`, with the minimizer `children` gives, and
/// archives the best candidate in a minimized bundle beside it. `None` if
/// `children` gives no minimizer.
pub(super) fn minimize(
    children: &dyn Children,
    original: &Path,
    property: &str,
    out: &Path,
    options: TriageOptions,
) -> Option<Minimization> {
    let staging = match new_staging(out) {
        Ok(staging) => staging,
        Err(error) => {
            return Some(Minimization::NotKept {
                reason: format!("creating a staging directory: {error}"),
                staging: out.join(super::STAGING_DIR),
            })
        }
    };
    let output = staging.join(STAGING_OUTPUT);
    let Some(command) = children.minimize(&original.join(BUNDLE_INPUT_FILE), property, &output)
    else {
        let _ = fs::remove_dir(&staging);
        return None;
    };
    if let Some(bundle) = kept_beside(original) {
        let _ = fs::remove_dir(&staging);
        return Some(Minimization::Kept {
            bundle,
            archived: false,
        });
    }
    Some(
        match archive(children, command, original, &staging, options) {
            Ok((bundle, archived)) => {
                // The staging bundle is in place, and the rest is the
                // minimizer's output, which the bundle copied.
                let _ = fs::remove_dir_all(&staging);
                Minimization::Kept { bundle, archived }
            }
            Err(reason) => {
                // The pinned binaries take hundreds of megabytes, and each
                // triage that keeps nothing would leave its own copy. Its
                // report keeps their SHA-256.
                let _ = fs::remove_dir_all(output.join(PINNED_DIR));
                Minimization::NotKept { reason, staging }
            }
        },
    )
}

/// The first minimized bundle beside `original`, by name, that passes
/// [`verify`].
fn kept_beside(original: &Path) -> Option<PathBuf> {
    let name = original.file_name()?.to_str()?;
    let mut found: Vec<PathBuf> = fs::read_dir(original.parent()?)
        .ok()?
        .filter_map(Result::ok)
        .map(|item| item.path())
        .filter(|path| {
            path.file_name()
                .and_then(|other| other.to_str())
                .and_then(original_name)
                == Some(name)
        })
        .collect();
    found.sort();
    found
        .into_iter()
        .find(|dir| verify(dir).is_ok_and(|problems| problems.is_empty()))
}

/// Runs the minimizer's `command`, then checks, replays and places its
/// candidate as [`minimize`] describes. Returns where the minimized bundle
/// is, and true if this call placed it, or why it kept nothing.
fn archive(
    children: &dyn Children,
    command: std::process::Command,
    original: &Path,
    staging: &Path,
    options: TriageOptions,
) -> Result<(PathBuf, bool), String> {
    let io = |what: &'static str| move |error: io::Error| format!("{what}: {error}");
    let bundle = staging.join(STAGING_BUNDLE);
    let output = staging.join(STAGING_OUTPUT);
    fs::create_dir(&bundle).map_err(io("creating the staging bundle"))?;
    let (exit, _) = run_child(command, &bundle.join(RUNS_DIR).join(MINIMIZER_RUN), None)
        .map_err(io("starting the minimizer"))?;
    if exit != ProcessExit::Code(0) {
        // The minimizer exits 0 only when it accepted a candidate. Its
        // report says why it didn't, if it wrote one.
        let said = read_json(&output.join(REPORT_FILE)).map_or_else(
            |error| format!("; {error}"),
            |report| {
                format!(
                    "; its report says {}: {}",
                    report["status"].as_str().unwrap_or("no status"),
                    report["reason"].as_str().unwrap_or("no reason given")
                )
            },
        );
        return Err(format!("the minimizer {}{said}", ended(exit)));
    }

    let copy = bundle.join(MINIMIZER_DIR);
    copy_output(&output, &copy).map_err(io("copying the minimizer's output"))?;
    let checked = check(original, &copy)?;
    write_synced(&bundle.join(BUNDLE_INPUT_FILE), &checked.input)
        .map_err(io("writing the minimized input"))?;
    let (replay, _) = run_child(
        children.replay(&copy.join(&checked.trace)),
        &bundle.join(RUNS_DIR).join(REPLAY_DIR),
        Some(options.child_timeout),
    )
    .map_err(io("starting the replay"))?;
    if replay != ProcessExit::Code(0) {
        return Err(format!(
            "a fresh replay of the minimized trace {}",
            ended(replay)
        ));
    }

    let name = original
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("{} has no bundle name", original.display()))?;
    let mut report = derived(name, &checked);
    report["child_timeout_ms"] =
        json!(u64::try_from(options.child_timeout.as_millis()).unwrap_or(u64::MAX));
    report["build"] = json!(BuildInfo::current());
    write_synced(
        &bundle.join(MINIMIZED_FILE),
        format!("{report:#}\n").as_bytes(),
    )
    .map_err(io("writing minimized.json"))?;
    let sums = super::checksums(&bundle).map_err(io("hashing the minimized bundle"))?;
    write_synced(&bundle.join(CHECKSUMS_FILE), sums.as_bytes())
        .map_err(io("writing the minimized bundle's checksums"))?;
    place(
        &bundle,
        &original.with_file_name(format!("{name}{MINIMIZED_SUFFIX}")),
        MINIMIZED_FILE,
        &RESULT_FIELDS,
        &report,
    )
    .map_err(io("placing the minimized bundle"))
}

/// The [`MINIMIZED_FILE`] fields that follow from the failure's bundle,
/// named `name`, and the minimizer's output that passed [`check`]: every
/// field but `child_timeout_ms` and `build`. A minimized bundle exists only
/// if its replay verified, so its replay record is always the same.
fn derived(name: &str, checked: &Checked) -> Value {
    let triage = &checked.triage;
    json!({
        "format": MINIMIZED_FORMAT,
        "harness": triage["harness"],
        "generator": triage["generator"],
        "provenance": triage["provenance"],
        "bucket": triage["bucket"],
        "signature": triage["signature"],
        "property": checked.property,
        "original": {
            "bundle": name,
            "input": triage["input"],
            "checksums_sha256": checked.checksums,
        },
        "input": { "sha256": checked.sha256, "len": checked.input.len() },
        "minimizer": MINIMIZER_DIR,
        "trace": format!("{MINIMIZER_DIR}/{}", path_text(&checked.trace)),
        "replay": {
            "dir": format!("{RUNS_DIR}/{REPLAY_DIR}"),
            "exit": ProcessExit::Code(0),
            "verified": true,
        },
    })
}

/// What [`check`] read from a minimizer's output that passed.
struct Checked {
    /// The failure's bundle's [`TRIAGE_FILE`].
    triage: Value,
    /// The property the failure violates.
    property: String,
    /// The SHA-256 of the failure's bundle's [`CHECKSUMS_FILE`].
    checksums: String,
    /// The minimized input.
    input: Vec<u8>,
    /// Its SHA-256.
    sha256: String,
    /// Its trace, relative to the minimizer's output.
    trace: PathBuf,
}

/// Holds the minimizer's output in `dir` to the failure's bundle `original`.
/// Returns the first thing that doesn't hold.
///
/// The bundle must be a confirmed `Active` failure. The minimizer's report
/// must be one this module reads, with an accepted candidate, on the
/// bundle's harness, property and input. The original's and the
/// candidate's confirmations must be confirmed, on the bundle's harness,
/// generator and property, each on its own input, on a build equivalent to
/// the bundle's. Held to the original's again with
/// [`Confirmation::against`], the candidate's must stay confirmed. The
/// candidate's input must hash to its digest and be smaller than the
/// original, and its trace must read with that input.
fn check(original: &Path, dir: &Path) -> Result<Checked, String> {
    let triage = read_json(&original.join(TRIAGE_FILE))?;
    if triage["status"] != "confirmed" || triage["bucket"] != "active" {
        return Err(format!(
            "{} isn't a confirmed active failure's bundle",
            original.display()
        ));
    }
    let property = triage["signature"]
        .as_str()
        .and_then(|signature| signature.strip_prefix("active "))
        .ok_or_else(|| format!("{} names no property", original.display()))?
        .to_owned();
    let build: BuildInfo = serde_json::from_value(triage["build"].clone())
        .map_err(|error| format!("reading the bundle's build: {error}"))?;
    let original_sha256 = triage["input"]["sha256"].as_str().unwrap_or_default();
    let original_len = triage["input"]["len"].as_u64().unwrap_or_default();
    let checksums = fs::read(original.join(CHECKSUMS_FILE))
        .map(|sums| hex(&Sha256::digest(sums)))
        .map_err(|error| format!("reading the bundle's {CHECKSUMS_FILE}: {error}"))?;

    let report = read_json(&dir.join(REPORT_FILE))?;
    if report["format"].as_u64() != Some(REPORT_FORMAT) {
        return Err(format!(
            "the minimizer's report has format {}, not {REPORT_FORMAT}",
            report["format"]
        ));
    }
    if report["status"] != MINIMIZED_STATUS {
        return Err(format!(
            "the minimizer's report says {}, not {MINIMIZED_STATUS}",
            report["status"]
        ));
    }
    for (field, want) in [
        ("harness", &triage["harness"]),
        ("property", &Value::from(property.as_str())),
    ] {
        if &report[field] != want {
            return Err(format!(
                "the minimizer ran {field} {}, not the bundle's {want}",
                report[field]
            ));
        }
    }
    if report["original"]["sha256"] != original_sha256 {
        return Err(format!(
            "the minimizer ran input {}, not the bundle's {original_sha256}",
            report["original"]["sha256"]
        ));
    }
    let minimized = &report["minimized"];
    let sha256 = minimized["sha256"]
        .as_str()
        .ok_or("the minimizer's report names no minimized input")?
        .to_owned();
    let accepted = report["candidates"]
        .as_array()
        .and_then(|candidates| {
            candidates.iter().find(|candidate| {
                candidate["sha256"] == sha256.as_str() && candidate["check"]["accepted"] == true
            })
        })
        .ok_or("the minimizer's report lists no accepted candidate with the minimized input")?;
    let original_confirmation = confirmation(dir, &report["original"]["check"])?;
    let confirmation = confirmation(dir, &accepted["check"])?;
    for (which, confirmation, input) in [
        ("original", &original_confirmation, original_sha256),
        ("minimized", &confirmation, sha256.as_str()),
    ] {
        let problem = if confirmation.status != ConfirmStatus::Confirmed {
            Some(format!("is {}", confirmation.status))
        } else if triage["harness"] != confirmation.harness.as_str()
            || triage["generator"] != confirmation.generator
        {
            Some(format!(
                "ran on {} generator {}, not the bundle's {} generator {}",
                confirmation.harness,
                confirmation.generator,
                triage["harness"],
                triage["generator"]
            ))
        } else if confirmation.property != property {
            Some(format!("confirms {}", confirmation.property))
        } else if confirmation.input.sha256 != input {
            Some(format!("confirms input {}", confirmation.input.sha256))
        } else if !confirmation.build.equivalent(&build) {
            Some(format!(
                "ran on a build (binary {}) that isn't equivalent to the bundle's (binary {})",
                confirmation.build.binary, build.binary
            ))
        } else {
            None
        };
        if let Some(problem) = problem {
            return Err(format!("the {which} input's confirmation {problem}"));
        }
    }
    let held = confirmation.clone().against(&original_confirmation);
    if held.status != ConfirmStatus::Confirmed {
        return Err(format!(
            "held to the original's confirmation, the minimized input is {}: {}",
            held.status,
            held.detail.as_deref().unwrap_or("no detail")
        ));
    }

    let input_path = inside(&minimized["dir"])?.join(MINIMIZER_INPUT_FILE);
    let input = fs::read(dir.join(&input_path))
        .map_err(|error| format!("reading {}: {error}", input_path.display()))?;
    let found = hex(&Sha256::digest(&input));
    if found != sha256 {
        return Err(format!(
            "{} has SHA-256 {found}, not the minimized input's {sha256}",
            input_path.display()
        ));
    }
    // A usize always fits in a u64 on supported targets.
    if input.len() as u64 >= original_len {
        return Err(format!(
            "the minimized input has {} bytes, and the original {original_len}",
            input.len()
        ));
    }
    let trace = inside(&minimized["trace"])?;
    let read = TraceReader::open(&dir.join(&trace))
        .map_err(|error| format!("reading the minimized trace: {error}"))?;
    if read.meta.input.as_ref().map(|input| input.sha256.as_str()) != Some(sha256.as_str()) {
        return Err("the minimized trace holds another input".to_owned());
    }
    Ok(Checked {
        triage,
        property,
        checksums,
        input,
        sha256,
        trace,
    })
}

/// The confirmation a minimizer's `check` record names, in its output
/// directory `dir`.
fn confirmation(dir: &Path, check: &Value) -> Result<Confirmation, String> {
    let path = inside(&check["confirmation_file"])?;
    let bytes = fs::read(dir.join(&path))
        .map_err(|error| format!("reading {}: {error}", path.display()))?;
    serde_json::from_slice(&bytes).map_err(|error| format!("reading {}: {error}", path.display()))
}

/// The relative path `value` names, which may only descend.
fn inside(value: &Value) -> Result<PathBuf, String> {
    let text = value
        .as_str()
        .ok_or_else(|| format!("the minimizer's report names {value}, not a path"))?;
    let path = PathBuf::from(text);
    if text.is_empty()
        || !path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
    {
        return Err(format!(
            "the minimizer's report names {text}, which leaves its directory"
        ));
    }
    Ok(path)
}

/// `path` with `/` between its parts.
fn path_text(path: &Path) -> String {
    path.iter()
        .map(|part| part.to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

/// The JSON file at `path`.
fn read_json(path: &Path) -> Result<Value, String> {
    let bytes = fs::read(path).map_err(|error| format!("reading {}: {error}", path.display()))?;
    serde_json::from_slice(&bytes).map_err(|error| format!("reading {}: {error}", path.display()))
}

/// How a child ended, in words.
fn ended(exit: ProcessExit) -> String {
    match exit {
        ProcessExit::Code(code) => format!("exited with code {code}"),
        ProcessExit::Signal(signal) => format!("died from signal {signal}"),
        ProcessExit::TimedOut => "ran past its time limit".to_owned(),
    }
}

/// Copies the minimizer's output `from` into the new directory `to`, but
/// its [`PINNED_DIR`], syncing each file. Fails on a symlink or a special
/// file.
fn copy_output(from: &Path, to: &Path) -> io::Result<()> {
    let mut pending = vec![PathBuf::new()];
    while let Some(relative) = pending.pop() {
        fs::create_dir(to.join(&relative))?;
        for item in fs::read_dir(from.join(&relative))? {
            let item = item?;
            let path = relative.join(item.file_name());
            let kind = item.file_type()?;
            if kind.is_dir() {
                if path != Path::new(PINNED_DIR) {
                    pending.push(path);
                }
            } else if kind.is_file() {
                let mut source = File::open(from.join(&path))?;
                let mut copy = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(to.join(&path))?;
                io::copy(&mut source, &mut copy)?;
                copy.sync_all()?;
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "{} is neither a file nor a directory",
                        from.join(&path).display()
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// The failure's bundle name a minimized bundle's `name` belongs to:
/// `name` without [`MINIMIZED_SUFFIX`] and an optional `-<n>` after it.
/// `None` if `name` isn't a minimized bundle's.
pub(super) fn original_name(name: &str) -> Option<&str> {
    let (original, rest) = name.rsplit_once(MINIMIZED_SUFFIX)?;
    let numbered = rest.is_empty()
        || rest
            .strip_prefix('-')
            .is_some_and(|n| !n.is_empty() && n.bytes().all(|byte| byte.is_ascii_digit()));
    (numbered && !original.is_empty()).then_some(original)
}

/// What [`verify_minimized`] found wrong with the minimized bundle in
/// `dir`, beyond its checksums. Empty if its [`MINIMIZED_FILE`] names the
/// failure's bundle it's named after, that bundle's [`CHECKSUMS_FILE`] is
/// the one it was minimized from, its minimizer's output passes [`check`]
/// against that bundle, its [`MINIMIZED_FILE`] holds every field that
/// follows from that bundle and that output, and its [`BUNDLE_INPUT_FILE`]
/// is the minimized input.
pub fn verify_minimized(dir: &Path) -> io::Result<Vec<String>> {
    let report: Value = match fs::read(dir.join(MINIMIZED_FILE)) {
        Ok(bytes) => match serde_json::from_slice(&bytes) {
            Ok(report) => report,
            Err(error) => return Ok(vec![format!("{MINIMIZED_FILE} doesn't read: {error}")]),
        },
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(vec![format!("{MINIMIZED_FILE} is missing")]);
        }
        Err(error) => return Err(error),
    };
    let Some(original_bundle) = report["original"]["bundle"].as_str() else {
        return Ok(vec![format!("{MINIMIZED_FILE} names no failure's bundle")]);
    };
    let name = dir.file_name().and_then(|name| name.to_str());
    if name.and_then(original_name) != Some(original_bundle) {
        return Ok(vec![format!(
            "{} isn't named after the failure's bundle {original_bundle}",
            name.unwrap_or_default()
        )]);
    }
    let original = dir.with_file_name(original_bundle);
    match fs::read(original.join(CHECKSUMS_FILE)) {
        Ok(sums) if report["original"]["checksums_sha256"] == hex(&Sha256::digest(&sums)) => {}
        Ok(_) => {
            return Ok(vec![format!(
                "the failure's bundle {original_bundle} has another {CHECKSUMS_FILE} than the \
                 one it was minimized from"
            )])
        }
        Err(error) => {
            return Ok(vec![format!(
                "the failure's bundle {original_bundle} has no {CHECKSUMS_FILE} that reads: \
                 {error}"
            )])
        }
    }
    let checked = match check(&original, &dir.join(MINIMIZER_DIR)) {
        Ok(checked) => checked,
        Err(problem) => return Ok(vec![problem]),
    };
    let mut problems = Vec::new();
    let derived = derived(original_bundle, &checked);
    for (field, want) in derived.as_object().into_iter().flatten() {
        if &report[field] != want {
            problems.push(format!(
                "{MINIMIZED_FILE} names {field} {}, not {want}",
                report[field]
            ));
        }
    }
    match fs::read(dir.join(BUNDLE_INPUT_FILE)) {
        Ok(input) if input == checked.input => {}
        _ => problems.push(format!("{BUNDLE_INPUT_FILE} isn't the minimized input")),
    }
    Ok(problems)
}

/// True if the bundle in `dir` is a minimized bundle: its name is one's, or
/// it holds a [`MINIMIZED_FILE`].
pub fn is_minimized(dir: &Path) -> bool {
    dir.file_name()
        .and_then(|name| name.to_str())
        .and_then(original_name)
        .is_some()
        || fs::symlink_metadata(dir.join(MINIMIZED_FILE)).is_ok()
}

/// What [`verify`] found wrong with the bundle in `dir`: what
/// [`verify_bundle`] finds, and for a minimized bundle ([`is_minimized`]),
/// what [`verify_minimized`] finds too, each problem once.
pub fn verify(dir: &Path) -> io::Result<Vec<String>> {
    let mut problems = verify_bundle(dir)?;
    if is_minimized(dir) {
        for problem in verify_minimized(dir)? {
            if !problems.contains(&problem) {
                problems.push(problem);
            }
        }
    }
    Ok(problems)
}
