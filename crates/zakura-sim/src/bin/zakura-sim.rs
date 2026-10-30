//! The `zakura-sim` command: runs registered harnesses and writes traces.
//!
//! ```text
//! zakura-sim record --sim <name> --input <file> --out <dir>
//!     [--witness <property> --expect <holds|violated|applicable>]
//! zakura-sim record --sim <name> --scenario <json> --plan <file> --out <dir>
//! zakura-sim replay <dir> [--allow-build-mismatch] [--prefix] [--observe] [--explain]
//! zakura-sim shrink <dir> --out <dir> [--candidates <n>] [--wall-secs <s>]
//! zakura-sim versions --sim <name>
//! zakura-sim seeds [--sim <name>] [--fixtures <dir>]... [--out <dir>]
//!     [--quarantine <dir>] [--report <file>]
//! zakura-sim confirm --sim <name> --input <file> --property <id> --out <dir>
//!     [--against <confirmation.json>]
//! zakura-sim properties --sim <name>
//! ```
//!
//! It works only in the simulator build, `RUSTFLAGS="--cfg zakura_sim"`.
//! `cargo xtask record`, `cargo xtask replay`, `cargo xtask shrink`,
//! `cargo xtask sim-versions` and `cargo xtask sim-seeds` build and run it
//! with that flag.

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
    use std::{
        fmt::Write,
        fs::File,
        io::Read,
        path::{Path, PathBuf},
        time::Duration,
    };

    use serde_json::{json, Value};
    use zakura_sim::{
        harness::{self, RecordInput, HARNESSES, WITNESS_FILE},
        record::{EndRecord, OutcomeKind},
        seed::{self, Failed, SeedReport, Sidecar},
        trace::{
            self, shrink::REPORT_FILE, ConfirmStatus, Confirmation, ReplayOptions, ShrinkBudget,
            TraceReader, CONFIRMATION_FILE, MAX_INPUT_BYTES,
        },
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
             zakura-sim versions --sim <name>\n  \
             zakura-sim seeds [--sim <name>] [--fixtures <dir>]... [--out <dir>]\n      \
             [--quarantine <dir>] [--report <file>]\n  \
             zakura-sim confirm --sim <name> --input <file> --property <id> --out <dir>\n      \
             [--against <confirmation.json>]\n  \
             zakura-sim properties --sim <name>\n\n\
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
             `versions` prints the versions that fix what the harness's fuzz input means,\n\
             as KEY=VALUE lines.\n\
             `seeds` validates the committed seeds in each --fixtures directory (default\n\
             crates/zakura-sim/tests/fixtures) against the current generators, and lists\n\
             each seed as admitted, quarantined or rejected. With --sim, it checks only\n\
             that harness's seeds, and --out writes each admitted seed's full fuzz input\n\
             there, named by its SHA-256. --quarantine writes each quarantined seed there\n\
             with its reason. --report writes the result as JSON. It exits 1 if any\n\
             required seed is rejected.\n\
             `confirm` checks that a full fuzz input violates the property alone, through\n\
             its trigger history, records its trace under --out and replays it Exact. It\n\
             writes confirmation.json to --out, and exits 0 only if the input is confirmed.\n\
             `--against` also holds the input to an earlier confirmation's harness,\n\
             generator, build, assumptions and cleanup.\n\
             `properties` prints the ID of each property the harness registers, one\n\
             per line.\n\n\
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
            Some("versions") => versions(args.collect()),
            Some("seeds") => seeds(args.collect()),
            Some("confirm") => confirm(args.collect()),
            Some("properties") => properties(args.collect()),
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

    /// `versions`: prints [`seed::versions`] for one harness.
    #[allow(clippy::print_stdout)]
    fn versions(args: Vec<String>) -> Result<(), Error> {
        let sim = match args.as_slice() {
            [flag, sim] if flag == "--sim" => sim,
            _ => return Err(Error::Usage("`versions` takes --sim <name>".to_owned())),
        };
        let entry = harness::find(sim).map_err(|error| Error::Usage(error.to_string()))?;
        for (key, value) in seed::versions(entry) {
            println!("{key}={value}");
        }
        Ok(())
    }

    /// The flags `seeds` takes, as given.
    #[derive(Default)]
    struct SeedArgs {
        sim: Option<String>,
        fixtures: Vec<PathBuf>,
        out: Option<PathBuf>,
        quarantine: Option<PathBuf>,
        report: Option<PathBuf>,
    }

    /// `seeds`: validates the committed seeds, prints each one's result,
    /// and writes the admitted and quarantined ones and the report where
    /// the flags say. Fails if any required seed is rejected.
    #[allow(clippy::print_stdout)]
    fn seeds(args: Vec<String>) -> Result<(), Error> {
        let mut flags = SeedArgs::default();
        let mut args = args.into_iter();
        while let Some(flag) = args.next() {
            let value = args
                .next()
                .ok_or_else(|| Error::Usage(format!("{flag} needs a value")))?;
            let slot_taken = match flag.as_str() {
                "--sim" => flags.sim.replace(value).is_some(),
                "--fixtures" => {
                    flags.fixtures.push(value.into());
                    false
                }
                "--out" => flags.out.replace(value.into()).is_some(),
                "--quarantine" => flags.quarantine.replace(value.into()).is_some(),
                "--report" => flags.report.replace(value.into()).is_some(),
                _ => return Err(Error::Usage(format!("unknown flag `{flag}`"))),
            };
            if slot_taken {
                return Err(Error::Usage(format!("{flag} given twice")));
            }
        }
        if flags.sim.is_none() && (flags.out.is_some() || flags.quarantine.is_some()) {
            return Err(Error::Usage(
                "--out and --quarantine hold one harness's seeds, so they need --sim".to_owned(),
            ));
        }
        if let Some(sim) = &flags.sim {
            harness::find(sim).map_err(|error| Error::Usage(error.to_string()))?;
        }
        if flags.fixtures.is_empty() {
            flags
                .fixtures
                .push(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures"));
        }
        let report = seed::load(&flags.fixtures, flags.sim.as_deref())
            .map_err(|error| Error::Failed(format!("listing the fixtures: {error}")))?;
        if let Some(out) = &flags.out {
            create(out)?;
            for seed in &report.admitted {
                write(&out.join(seed.input_sha256()), &seed.input)?;
            }
        }
        if let Some(quarantine) = &flags.quarantine {
            create(quarantine)?;
            for failed in &report.quarantined {
                quarantine_seed(quarantine, failed)?;
            }
        }
        for seed in &report.admitted {
            println!(
                "admitted {} ({}, generator {}): {}",
                seed.path.display(),
                seed.sidecar.harness,
                seed.sidecar.generator,
                seed.input_sha256()
            );
        }
        for failed in &report.quarantined {
            println!("quarantined {}: {}", failed.path.display(), failed.error);
        }
        for failed in &report.rejected {
            println!("rejected {}: {}", failed.path.display(), failed.error);
        }
        println!(
            "{} admitted, {} quarantined, {} rejected, {} for other harnesses",
            report.admitted.len(),
            report.quarantined.len(),
            report.rejected.len(),
            report.other_harnesses
        );
        if let Some(path) = &flags.report {
            let json = report_json(&report, flags.sim.as_deref(), &flags.fixtures)?;
            write(path, json.as_bytes())?;
        }
        match report.rejected.len() {
            0 => Ok(()),
            rejected => Err(Error::Failed(format!(
                "{rejected} required seeds failed validation"
            ))),
        }
    }

    /// `confirm`: confirms that a full fuzz input violates one property,
    /// and writes the [`Confirmation`] to `--out`. Fails unless the input is
    /// confirmed.
    #[allow(clippy::print_stdout)]
    fn confirm(args: Vec<String>) -> Result<(), Error> {
        let mut flags = [None, None, None, None, None];
        let names = ["--sim", "--input", "--property", "--out", "--against"];
        let mut args = args.into_iter();
        while let Some(flag) = args.next() {
            let slot = names
                .iter()
                .position(|name| *name == flag)
                .ok_or_else(|| Error::Usage(format!("unknown flag `{flag}`")))?;
            let value = args
                .next()
                .ok_or_else(|| Error::Usage(format!("{flag} needs a value")))?;
            if flags[slot].replace(value).is_some() {
                return Err(Error::Usage(format!("{flag} given twice")));
            }
        }
        let [sim, input, property, out, against] = flags;
        let need = |value: Option<String>, flag: &str| {
            value.ok_or_else(|| Error::Usage(format!("`confirm` needs {flag}")))
        };
        let (sim, input, property, out) = (
            need(sim, "--sim")?,
            PathBuf::from(need(input, "--input")?),
            need(property, "--property")?,
            PathBuf::from(need(out, "--out")?),
        );
        let entry = harness::find(&sim).map_err(|error| Error::Usage(error.to_string()))?;
        if entry.status(&property).is_none() {
            return Err(Error::Usage(format!("{sim} doesn't register {property}")));
        }
        let read = |path: &PathBuf| {
            std::fs::read(path)
                .map_err(|error| Error::Failed(format!("reading {}: {error}", path.display())))
        };
        let against = match against {
            None => None,
            Some(path) => {
                let path = PathBuf::from(path);
                let original: Confirmation =
                    serde_json::from_slice(&read(&path)?).map_err(|error| {
                        Error::Failed(format!("reading {}: {error}", path.display()))
                    })?;
                Some(original)
            }
        };
        let bytes = read(&input)?;
        let mut confirmation = trace::confirm(entry, &bytes, &property, &out)
            .map_err(|error| Error::Failed(error.to_string()))?;
        if let Some(original) = &against {
            confirmation = confirmation.against(original);
        }
        std::fs::create_dir_all(&out)
            .map_err(|error| Error::Failed(format!("creating {}: {error}", out.display())))?;
        let file = out.join(CONFIRMATION_FILE);
        let text = serde_json::to_string_pretty(&confirmation)
            .map_err(|error| Error::Failed(format!("writing {CONFIRMATION_FILE}: {error}")))?;
        std::fs::write(&file, format!("{text}\n"))
            .map_err(|error| Error::Failed(format!("writing {}: {error}", file.display())))?;
        let detail = confirmation
            .detail
            .as_deref()
            .map_or_else(String::new, |detail| format!(": {detail}"));
        match confirmation.status {
            ConfirmStatus::Confirmed => {
                println!(
                    "confirmed {property} on {sim}: trace {} in {}",
                    confirmation.trace_id.as_deref().unwrap_or_default(),
                    file.display()
                );
                Ok(())
            }
            status => Err(Error::Failed(format!(
                "{property} on {sim}: {status}{detail}; see {}",
                file.display()
            ))),
        }
    }

    /// Writes the quarantined seed's file, its sidecar and its reason into
    /// `dir` as `<name>.bin`, `<name>.toml` and `<name>.reason`. `<name>` is
    /// the SHA-256 of the file's SHA-256 and the sidecar's, in hex, so two
    /// seeds with the same bytes and different sidecars, such as two `plan`
    /// seeds with different prefixes, keep separate records.
    ///
    /// A seed can be quarantined before [`seed::load`] reads its file, such
    /// as for a stale generator, so nothing has bounded the file yet. A file
    /// over [`MAX_INPUT_BYTES`], the largest fuzz input a trace holds, isn't
    /// read in full or copied. Its length and path stand in for its SHA-256
    /// in `<name>`, and the reason names the limit.
    fn quarantine_seed(dir: &Path, failed: &Failed) -> Result<(), Error> {
        let sidecar_path = failed.path.with_extension(seed::SIDECAR_EXTENSION);
        let bytes = read_capped(&failed.path)?;
        let sidecar = read_capped(&sidecar_path)?;
        let mut reason = format!("{}: {}\n", failed.path.display(), failed.error);
        let mut digests = String::new();
        for (path, read) in [(&failed.path, &bytes), (&sidecar_path, &sidecar)] {
            match read {
                Ok(bytes) => digests.push_str(&seed::sha256(bytes)),
                Err(len) => {
                    // A hex digest holds no `:`, so this can't match one.
                    let _ = write!(digests, "{len}:{}:", path.display());
                    let _ = writeln!(
                        reason,
                        "{} is {len} bytes, over the {MAX_INPUT_BYTES}-byte MAX_INPUT_BYTES \
                         limit, so the quarantine holds no copy of it",
                        path.display()
                    );
                }
            }
        }
        let name = seed::sha256(digests.as_bytes());
        if let Ok(bytes) = &bytes {
            write(&dir.join(format!("{name}.{}", seed::SEED_EXTENSION)), bytes)?;
        }
        if let Ok(sidecar) = &sidecar {
            write(
                &dir.join(format!("{name}.{}", seed::SIDECAR_EXTENSION)),
                sidecar,
            )?;
        }
        write(&dir.join(format!("{name}.reason")), reason.as_bytes())
    }

    /// The file at `path` if it holds at most [`MAX_INPUT_BYTES`], or its
    /// length if it holds more. It reads at most one byte past the limit.
    fn read_capped(path: &Path) -> Result<Result<Vec<u8>, u64>, Error> {
        let failed =
            |error: std::io::Error| Error::Failed(format!("reading {}: {error}", path.display()));
        let file = File::open(path).map_err(failed)?;
        let mut bytes = Vec::new();
        (&file)
            .take(MAX_INPUT_BYTES.saturating_add(1))
            .read_to_end(&mut bytes)
            .map_err(failed)?;
        // A usize always fits in a u64 on supported targets.
        if bytes.len() as u64 <= MAX_INPUT_BYTES {
            return Ok(Ok(bytes));
        }
        let len = file.metadata().map_err(failed)?.len();
        Ok(Err(len.max(bytes.len() as u64)))
    }

    /// `seeds --report`'s JSON.
    #[derive(serde::Serialize)]
    struct Report<'a> {
        harness: Option<&'a str>,
        fixtures: Vec<PathBuf>,
        seeds: Vec<ReportRow<'a>>,
        other_harnesses: usize,
    }

    /// One seed in [`Report`]: its status, its sidecar if it parsed, and
    /// either its full input's digest or its reason.
    #[derive(serde::Serialize)]
    struct ReportRow<'a> {
        file: PathBuf,
        status: &'static str,
        sidecar: Option<&'a Sidecar>,
        #[serde(skip_serializing_if = "Option::is_none")]
        input_sha256: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    }

    /// `report` as JSON, one row per seed. It serializes structs rather than
    /// a `Value`, so its key order doesn't depend on `serde_json`'s
    /// `preserve_order`, and it gives paths in the repository relative to
    /// the repository's root, so it doesn't depend on where the checkout is.
    fn report_json(
        report: &SeedReport,
        sim: Option<&str>,
        fixtures: &[PathBuf],
    ) -> Result<String, Error> {
        let mut seeds = Vec::new();
        for seed in &report.admitted {
            seeds.push(ReportRow {
                file: from_root(&seed.path),
                status: "admitted",
                sidecar: Some(&seed.sidecar),
                input_sha256: Some(seed.input_sha256()),
                reason: None,
            });
        }
        for (status, failed) in report
            .quarantined
            .iter()
            .map(|failed| ("quarantined", failed))
            .chain(report.rejected.iter().map(|failed| ("rejected", failed)))
        {
            seeds.push(ReportRow {
                file: from_root(&failed.path),
                status,
                sidecar: failed.sidecar.as_ref(),
                input_sha256: None,
                reason: Some(failed.error.to_string()),
            });
        }
        let report = Report {
            harness: sim,
            fixtures: fixtures.iter().map(|dir| from_root(dir)).collect(),
            seeds,
            other_harnesses: report.other_harnesses,
        };
        serde_json::to_string_pretty(&report)
            .map(|json| json + "\n")
            .map_err(|error| Error::Failed(format!("serializing the report: {error}")))
    }

    /// `path` relative to the repository's root if it's in the repository,
    /// and as given if not.
    fn from_root(path: &Path) -> PathBuf {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        path.canonicalize()
            .ok()
            .zip(root.canonicalize().ok())
            .and_then(|(path, root)| path.strip_prefix(root).ok().map(Path::to_owned))
            .unwrap_or_else(|| path.to_owned())
    }

    /// Creates `dir` and its parents.
    fn create(dir: &Path) -> Result<(), Error> {
        std::fs::create_dir_all(dir)
            .map_err(|error| Error::Failed(format!("creating {}: {error}", dir.display())))
    }

    /// Writes `bytes` to a temporary file beside `path`, then renames it
    /// to `path`, so an interrupted write never leaves a partial file under
    /// a seed's digest.
    fn write(path: &Path, bytes: &[u8]) -> Result<(), Error> {
        let name = path.file_name().unwrap_or_default().to_string_lossy();
        let partial = path.with_file_name(format!(".{name}.{}.partial", std::process::id()));
        std::fs::write(&partial, bytes)
            .and_then(|()| std::fs::rename(&partial, path))
            .map_err(|error| {
                let _ = std::fs::remove_file(&partial);
                Error::Failed(format!("writing {}: {error}", path.display()))
            })
    }

    /// `properties`: prints the ID of each property `--sim` registers, one
    /// per line, in report order.
    #[allow(clippy::print_stdout)]
    fn properties(args: Vec<String>) -> Result<(), Error> {
        let sim = match <[String; 2]>::try_from(args) {
            Ok([flag, sim]) if flag == "--sim" => sim,
            _ => return Err(Error::Usage("`properties` takes --sim <name>".to_owned())),
        };
        let entry = harness::find(&sim).map_err(|error| Error::Usage(error.to_string()))?;
        for property in (entry.properties)() {
            println!("{}", property.id);
        }
        Ok(())
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
