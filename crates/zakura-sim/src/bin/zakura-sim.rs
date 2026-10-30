//! The `zakura-sim` command: runs registered harnesses and writes traces.
//!
//! ```text
//! zakura-sim record --sim <name> --input <file> --out <dir>
//!     [--witness <property> --expect <holds|violated|applicable>]
//! zakura-sim record --sim <name> --scenario <json> --plan <file> --out <dir>
//! zakura-sim replay <dir> [--allow-build-mismatch] [--prefix] [--observe] [--explain]
//! zakura-sim shrink <dir> --out <dir> [--candidates <n>] [--wall-secs <s>]
//! zakura-sim triage --sim <name> --out <dir> [--child-secs <s>] <input>...
//! zakura-sim verify-archive <dir>
//! ```
//!
//! It works only in the simulator build, `RUSTFLAGS="--cfg zakura_sim"`.
//! `cargo xtask record`, `cargo xtask replay`, `cargo xtask shrink`,
//! `cargo xtask sim-triage` and `cargo xtask sim-verify-archive` build and
//! run it with that flag.

/// Prints that this build has no simulator, and exits with code 2.
#[cfg(not(zakura_sim))]
#[allow(clippy::print_stderr)]
fn main() {
    eprintln!("zakura-sim needs the simulator build: RUSTFLAGS=\"--cfg zakura_sim\"");
    std::process::exit(2);
}

/// Runs the command line, and exits with code 2 for a usage error or 1 for
/// any other failure.
#[cfg(zakura_sim)]
#[allow(clippy::print_stderr)]
fn main() {
    match cli::run(std::env::args().skip(1).collect()) {
        Ok(()) => {}
        Err(cli::Error::Usage(message)) => {
            eprintln!("error: {message}\n\n{}", cli::usage());
            std::process::exit(2);
        }
        Err(cli::Error::Failed(message)) => {
            eprintln!("error: {message}");
            std::process::exit(1);
        }
    }
}

#[cfg(zakura_sim)]
mod cli {
    use std::{fmt::Write, path::PathBuf, time::Duration};

    use serde_json::{json, Value};
    use zakura_sim::{
        explore::triage::{self, SimChildren, TriageOptions, TRIAGE_FILE},
        harness::{self, RecordInput, HARNESSES, WITNESS_FILE},
        record::{EndRecord, OutcomeKind},
        trace::{self, shrink::REPORT_FILE, ReplayOptions, ShrinkBudget, TraceReader},
        witness::Expect,
    };

    /// Why the command failed.
    pub enum Error {
        /// The arguments were wrong.
        Usage(String),
        /// The command ran and failed.
        Failed(String),
    }

    /// The usage text, with every registered harness.
    pub fn usage() -> String {
        let mut text = String::from(
            "Usage:\n  \
             zakura-sim record --sim <name> --input <file> --out <dir>\n      \
             [--witness <property> --expect <holds|violated|applicable>]\n  \
             zakura-sim record --sim <name> --scenario <json> --plan <file> --out <dir>\n  \
             zakura-sim replay <dir> [--allow-build-mismatch] [--prefix] [--observe] [--explain]\n  \
             zakura-sim shrink <dir> --out <dir> [--candidates <n>] [--wall-secs <s>]\n  \
             zakura-sim triage --sim <name> --out <dir> [--child-secs <s>] <input>...\n  \
             zakura-sim verify-archive <dir>\n\n\
             `--input` is a full fuzz input: the scenario prefix, then the Plan bytes.\n\
             `--scenario` and `--plan` run Plan bytes against a given scenario.\n\
             `--witness` judges the run as a witness test of the property, writes the\n\
             result to witness.json beside the trace, and exits 1 if the test fails.\n\
             `replay` reruns a trace Exact and exits 0 only if every pick, draw and\n\
             observation matched. `--allow-build-mismatch` replays on a build that isn't\n\
             equivalent to the recording one, for diagnosis, and always exits 1.\n\
             `--prefix` replays a trace without its end record to its last step.\n\
             `--observe` writes the full observation log to observations.jsonl.\n\
             `--explain` prints each step with its task name, draws and observations,\n\
             and marks every non-default pick and draw with `*`.\n\
             `shrink` reduces a trace that ends in a violation to fewer non-default\n\
             picks, and writes the reduced trace, the original and a reduction log to\n\
             --out. It stops after --candidates candidates (default 2000) or\n\
             --wall-secs seconds (default 600), and exits 1 if the re-record fails.\n\
             `triage` records each input twice, each time in a fresh process killed after\n\
             --child-secs seconds (default 60), replays the first trace Exact in a third,\n\
             and archives the input with its runs under --out. It exits 1 only if an input\n\
             couldn't be archived.\n\
             `verify-archive` checks every bundle under a triage archive against its\n\
             SHA256SUMS, counts the bundles under each signature, and exits 1 if any\n\
             bundle doesn't match.\n\n\
             Harnesses:\n",
        );
        for entry in HARNESSES {
            let _ = writeln!(
                text,
                "  {} (generator {}, {} scenario bytes)",
                entry.name, entry.generator, entry.scenario_bytes
            );
        }
        text
    }

    /// Runs the subcommand in `args`.
    #[allow(clippy::print_stdout)]
    pub fn run(args: Vec<String>) -> Result<(), Error> {
        let mut args = args.into_iter();
        match args.next().as_deref() {
            Some("record") => record(args.collect()),
            Some("replay") => replay(args.collect()),
            Some("shrink") => shrink(args.collect()),
            Some("triage") => triage(args.collect()),
            Some("verify-archive") => verify_archive(args.collect()),
            Some("-h" | "--help") => {
                print!("{}", usage());
                Ok(())
            }
            Some(other) => Err(Error::Usage(format!("unknown subcommand `{other}`"))),
            None => Err(Error::Usage("expected a subcommand".to_owned())),
        }
    }

    /// The flags `record` takes, as given.
    #[derive(Default)]
    struct RecordArgs {
        sim: Option<String>,
        input: Option<PathBuf>,
        scenario: Option<String>,
        plan: Option<PathBuf>,
        out: Option<PathBuf>,
        witness: Option<String>,
        expect: Option<String>,
    }

    /// `record`: runs one input on a harness and writes the trace.
    #[allow(clippy::print_stdout)]
    fn record(args: Vec<String>) -> Result<(), Error> {
        let flags = parse(args)?;
        let sim = flags
            .sim
            .ok_or_else(|| Error::Usage("`record` needs --sim".to_owned()))?;
        let out = flags
            .out
            .ok_or_else(|| Error::Usage("`record` needs --out".to_owned()))?;
        let entry = harness::find(&sim).map_err(|error| Error::Usage(error.to_string()))?;
        let read = |path: &PathBuf| {
            std::fs::read(path)
                .map_err(|error| Error::Failed(format!("reading {}: {error}", path.display())))
        };
        let witness = match (flags.witness, flags.expect) {
            (None, None) => None,
            (Some(property), Some(expect)) => {
                Some((property, expect.parse::<Expect>().map_err(Error::Usage)?))
            }
            _ => {
                return Err(Error::Usage(
                    "--witness and --expect go together".to_owned(),
                ))
            }
        };
        let input = match (flags.input, flags.scenario, flags.plan) {
            (Some(input), None, None) => read(&input)?,
            (None, Some(_), Some(_)) if witness.is_some() => {
                return Err(Error::Usage(
                    "--witness takes a committed --input, not --scenario and --plan".to_owned(),
                ))
            }
            (None, Some(scenario), Some(plan)) => {
                let scenario: Value = serde_json::from_str(&scenario)
                    .map_err(|error| Error::Usage(format!("--scenario isn't JSON: {error}")))?;
                let plan = read(&plan)?;
                let trace = harness::record(
                    entry,
                    RecordInput::Plan {
                        scenario: &scenario,
                        plan: &plan,
                    },
                    &out,
                )
                .map_err(|error| Error::Failed(error.to_string()))?;
                print_trace(&trace, &out);
                return Ok(());
            }
            _ => {
                return Err(Error::Usage(
                    "`record` takes either --input, or both --scenario and --plan".to_owned(),
                ))
            }
        };
        let Some((property, expect)) = witness else {
            let trace = harness::record(entry, RecordInput::Input(&input), &out)
                .map_err(|error| Error::Failed(error.to_string()))?;
            print_trace(&trace, &out);
            return Ok(());
        };
        let (trace, result) =
            harness::record_witness(entry, RecordInput::Input(&input), &out, &property, expect)
                .map_err(|error| Error::Failed(error.to_string()))?;
        print_trace(&trace, &out);
        let error = result.as_ref().err().map(ToString::to_string);
        let file = json!({
            "property": property,
            "expect": expect.name(),
            "passed": result.is_ok(),
            "error": error,
        });
        std::fs::write(out.join(WITNESS_FILE), format!("{file}\n"))
            .map_err(|error| Error::Failed(format!("writing {WITNESS_FILE}: {error}")))?;
        match error {
            None => {
                println!(
                    "witness test of {property}, expecting {}: passed",
                    expect.name()
                );
                Ok(())
            }
            Some(error) => Err(Error::Failed(format!(
                "witness test of {property}, expecting {}: {error}",
                expect.name()
            ))),
        }
    }

    /// Prints where `trace` went and how its run ended.
    #[allow(clippy::print_stdout)]
    fn print_trace(trace: &trace::Trace, out: &std::path::Path) {
        let end = trace
            .end
            .as_ref()
            .expect("record reads the trace back complete");
        println!(
            "trace {} in {}: {} after {} steps",
            trace.id,
            out.display(),
            outcome(end),
            end.steps
        );
    }

    /// How `end`'s run ended, in words.
    fn outcome(end: &EndRecord) -> String {
        match end.outcome {
            OutcomeKind::Violation => end.violation.as_ref().map_or_else(
                || "violation".to_owned(),
                |v| format!("violation of {}", v.property),
            ),
            OutcomeKind::Budget => "budget".to_owned(),
            OutcomeKind::Idle => "idle".to_owned(),
            OutcomeKind::Harness => format!(
                "harness failure: {}",
                end.harness.as_deref().unwrap_or("unknown")
            ),
        }
    }

    /// `replay`: reruns a trace Exact. Succeeds only for a verified replay,
    /// or a prefix that matched to its last step.
    #[allow(clippy::print_stdout)]
    fn replay(args: Vec<String>) -> Result<(), Error> {
        let mut dir = None;
        let mut options = ReplayOptions::default();
        let mut explain = false;
        for arg in args {
            let flag = match arg.as_str() {
                "--allow-build-mismatch" => &mut options.allow_build_mismatch,
                "--prefix" => &mut options.prefix,
                "--observe" => &mut options.observe,
                "--explain" => &mut explain,
                _ if arg.starts_with('-') => {
                    return Err(Error::Usage(format!("unknown flag `{arg}`")))
                }
                _ if dir.is_some() => {
                    return Err(Error::Usage(
                        "`replay` takes one trace directory".to_owned(),
                    ))
                }
                _ => {
                    dir = Some(PathBuf::from(arg));
                    continue;
                }
            };
            if std::mem::replace(flag, true) {
                return Err(Error::Usage(format!("{arg} given twice")));
            }
        }
        let dir = dir.ok_or_else(|| Error::Usage("`replay` needs a trace directory".to_owned()))?;
        let read = if options.prefix {
            TraceReader::open_prefix(&dir)
        } else {
            TraceReader::open(&dir)
        };
        let trace = read.map_err(|error| {
            Error::Failed(format!("reading the trace in {}: {error}", dir.display()))
        })?;
        let report = if explain {
            let (report, text) = trace::replay_explained(&trace, options)
                .map_err(|error| Error::Failed(error.to_string()))?;
            print!("{text}");
            report
        } else {
            trace::replay(&trace, options).map_err(|error| Error::Failed(error.to_string()))?
        };
        let ran = report.end.as_ref().map_or_else(
            || format!("{} steps", trace.steps.len()),
            |end| format!("{} after {} steps", outcome(end), end.steps),
        );
        // The flag is diagnostic, so it fails the command even when the
        // builds turn out equivalent.
        if options.allow_build_mismatch || report.build_mismatch {
            let first = report
                .first_obs_mismatch
                .map_or_else(String::new, |(step, field)| {
                    format!(", first at step {step} ({field})")
                });
            let end = report
                .end_mismatch
                .map_or_else(String::new, |field| format!("; {field} differs"));
            let build = if report.build_mismatch {
                "on a build that isn't equivalent to the recording one"
            } else {
                "with --allow-build-mismatch"
            };
            return Err(Error::Failed(format!(
                "replayed trace {} {build}: {ran}; {} records differed{first}{end}. \
                 This is never a successful replay",
                trace.id, report.obs_mismatches
            )));
        }
        if report.verified() {
            println!(
                "replayed trace {}: {ran}; every pick, draw and observation matched",
                trace.id
            );
        } else if report.complete {
            println!(
                "replayed trace {} with --prefix: {ran}; everything matched, but a \
                 --prefix replay is never verified",
                trace.id
            );
        } else {
            println!(
                "replayed prefix {}: {ran} matched. A prefix is not a complete run",
                trace.id
            );
        }
        Ok(())
    }

    /// `shrink`: reduces a trace, and prints what the shrink did. Fails if
    /// the shrink couldn't start or the re-record failed.
    #[allow(clippy::print_stdout)]
    fn shrink(args: Vec<String>) -> Result<(), Error> {
        let mut dir = None;
        let mut out = None;
        let mut budget = ShrinkBudget::default();
        let mut seen = Vec::new();
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            if !arg.starts_with('-') {
                if dir.replace(PathBuf::from(arg)).is_some() {
                    return Err(Error::Usage(
                        "`shrink` takes one trace directory".to_owned(),
                    ));
                }
                continue;
            }
            if seen.contains(&arg) {
                return Err(Error::Usage(format!("{arg} given twice")));
            }
            let value = args
                .next()
                .ok_or_else(|| Error::Usage(format!("{arg} needs a value")))?;
            let number = || {
                value
                    .parse::<u32>()
                    .map_err(|error| Error::Usage(format!("{arg}: {error}")))
            };
            match arg.as_str() {
                "--out" => out = Some(PathBuf::from(&value)),
                "--candidates" => budget.candidates = number()?,
                "--wall-secs" => budget.wall = Duration::from_secs(number()?.into()),
                _ => return Err(Error::Usage(format!("unknown flag `{arg}`"))),
            }
            seen.push(arg);
        }
        let dir = dir.ok_or_else(|| Error::Usage("`shrink` needs a trace directory".to_owned()))?;
        let out = out.ok_or_else(|| Error::Usage("`shrink` needs --out".to_owned()))?;
        let trace = TraceReader::open(&dir).map_err(|error| {
            Error::Failed(format!("reading the trace in {}: {error}", dir.display()))
        })?;
        let report = trace::shrink(&trace, budget, &out)
            .map_err(|error| Error::Failed(error.to_string()))?;
        let stopped = match report.stopped {
            None => "pass 4 reached a fixed point".to_owned(),
            Some(stop) => format!("{stop} stopped it before pass 4 reached a fixed point"),
        };
        println!(
            "shrank trace {} for {}: {} of {} candidates accepted; {stopped}",
            report.original, report.property, report.accepted, report.candidates
        );
        println!(
            "before: {} non-default picks and draws, {} events",
            report.before.non_default, report.before.events
        );
        match (&report.reduced, report.after, &report.rerecord_failure) {
            (Some(reduced), Some(after), _) => {
                println!(
                    "after: {} non-default picks and draws, {} events, in trace {reduced}",
                    after.non_default, after.events
                );
                println!("report in {}", out.join(REPORT_FILE).display());
                Ok(())
            }
            (_, _, failure) => Err(Error::Failed(format!(
                "the re-record failed: {}. The original and the best plan are in {}",
                failure.as_deref().unwrap_or("unknown"),
                out.display()
            ))),
        }
    }

    /// `triage`: confirms each input's failure in fresh processes and
    /// archives it. Fails only if an input couldn't be archived.
    #[allow(clippy::print_stdout, clippy::print_stderr)]
    fn triage(args: Vec<String>) -> Result<(), Error> {
        let mut sim = None;
        let mut out = None;
        let mut options = TriageOptions::default();
        let mut inputs = Vec::new();
        let mut seen = Vec::new();
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            if !arg.starts_with('-') {
                inputs.push(PathBuf::from(arg));
                continue;
            }
            if seen.contains(&arg) {
                return Err(Error::Usage(format!("{arg} given twice")));
            }
            let value = args
                .next()
                .ok_or_else(|| Error::Usage(format!("{arg} needs a value")))?;
            match arg.as_str() {
                "--sim" => sim = Some(value),
                "--out" => out = Some(PathBuf::from(value)),
                "--child-secs" => {
                    let secs = value
                        .parse::<u64>()
                        .ok()
                        .filter(|secs| *secs > 0)
                        .ok_or_else(|| {
                            Error::Usage(format!("{arg}: expected a positive whole number"))
                        })?;
                    options.child_timeout = Duration::from_secs(secs);
                }
                _ => return Err(Error::Usage(format!("unknown flag `{arg}`"))),
            }
            seen.push(arg);
        }
        let sim = sim.ok_or_else(|| Error::Usage("`triage` needs --sim".to_owned()))?;
        let out = out.ok_or_else(|| Error::Usage("`triage` needs --out".to_owned()))?;
        if inputs.is_empty() {
            return Err(Error::Usage("`triage` needs at least one input".to_owned()));
        }
        let entry = harness::find(&sim).map_err(|error| Error::Usage(error.to_string()))?;
        let children = SimChildren {
            exe: std::env::current_exe()
                .map_err(|error| Error::Failed(format!("finding this binary: {error}")))?,
            harness: sim,
        };
        let mut failed = 0;
        for input in &inputs {
            match triage::triage(entry, &children, input, &out, options) {
                Ok(triaged) => {
                    let class = match (&triaged.bucket, &triaged.signature) {
                        (Some(bucket), Some(signature)) => {
                            format!(" in {}: {signature}", bucket.name())
                        }
                        _ => String::new(),
                    };
                    let kept = if triaged.archived {
                        "archived in"
                    } else {
                        "already archived in"
                    };
                    println!(
                        "{}: {}{class}; {kept} {}",
                        input.display(),
                        triaged.status.name(),
                        triaged.bundle.display()
                    );
                }
                Err(error) => {
                    failed += 1;
                    eprintln!("{}: not archived: {error}", input.display());
                }
            }
        }
        match failed {
            0 => Ok(()),
            failed => Err(Error::Failed(format!(
                "{failed} of {} inputs weren't archived",
                inputs.len()
            ))),
        }
    }

    /// `verify-archive`: checks every bundle under a triage archive, and
    /// prints how many bundles each signature has. Fails if any bundle
    /// doesn't match its checksums.
    #[allow(clippy::print_stdout, clippy::print_stderr)]
    fn verify_archive(args: Vec<String>) -> Result<(), Error> {
        let [dir] = <[String; 1]>::try_from(args)
            .map_err(|_| Error::Usage("`verify-archive` takes one archive directory".to_owned()))?;
        let dir = PathBuf::from(dir);
        let read = |error: std::io::Error| {
            Error::Failed(format!("reading the archive in {}: {error}", dir.display()))
        };
        let bundles = triage::bundles(&dir).map_err(read)?;
        let mut bad = 0;
        let mut groups: std::collections::BTreeMap<PathBuf, (usize, String)> =
            std::collections::BTreeMap::new();
        for bundle in &bundles {
            let problems = triage::verify_bundle(bundle).map_err(read)?;
            for problem in &problems {
                eprintln!("{}: {problem}", bundle.display());
            }
            bad += usize::from(!problems.is_empty());
            let report: Value = std::fs::read(bundle.join(TRIAGE_FILE))
                .ok()
                .and_then(|bytes| serde_json::from_slice(&bytes).ok())
                .unwrap_or(Value::Null);
            let name = report["signature"]
                .as_str()
                .or(report["status"].as_str())
                .unwrap_or("unreadable triage.json")
                .to_owned();
            let group = bundle.parent().unwrap_or(&dir).to_owned();
            groups.entry(group).or_insert((0, name)).0 += 1;
        }
        for (group, (count, name)) in &groups {
            let group = group.strip_prefix(&dir).unwrap_or(group);
            println!("{count} {}: {name}", group.display());
        }
        match bad {
            0 => {
                println!("{} bundles match their checksums", bundles.len());
                Ok(())
            }
            bad => Err(Error::Failed(format!(
                "{bad} of {} bundles don't match their checksums",
                bundles.len()
            ))),
        }
    }

    /// Reads `record`'s flags. Each takes one value and may appear once.
    fn parse(args: Vec<String>) -> Result<RecordArgs, Error> {
        let mut flags = RecordArgs::default();
        let mut args = args.into_iter();
        while let Some(flag) = args.next() {
            let value = args
                .next()
                .ok_or_else(|| Error::Usage(format!("{flag} needs a value")))?;
            let slot_taken = match flag.as_str() {
                "--sim" => flags.sim.replace(value).is_some(),
                "--input" => flags.input.replace(value.into()).is_some(),
                "--scenario" => flags.scenario.replace(value).is_some(),
                "--plan" => flags.plan.replace(value.into()).is_some(),
                "--out" => flags.out.replace(value.into()).is_some(),
                "--witness" => flags.witness.replace(value).is_some(),
                "--expect" => flags.expect.replace(value).is_some(),
                _ => return Err(Error::Usage(format!("unknown flag `{flag}`"))),
            };
            if slot_taken {
                return Err(Error::Usage(format!("{flag} given twice")));
            }
        }
        Ok(flags)
    }
}
