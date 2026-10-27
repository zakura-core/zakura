//! The `zakura-sim` command: runs registered harnesses and writes traces.
//!
//! ```text
//! zakura-sim record --sim <name> --input <file> --out <dir>
//!     [--witness <property> --expect <holds|violated|applicable>]
//! zakura-sim record --sim <name> --scenario <json> --plan <file> --out <dir>
//! zakura-sim replay <dir> [--allow-build-mismatch] [--prefix] [--observe]
//! ```
//!
//! It works only in the simulator build, `RUSTFLAGS="--cfg zakura_sim"`.
//! `cargo xtask record` and `cargo xtask replay` build and run it with that
//! flag.

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
    use std::{fmt::Write, path::PathBuf};

    use serde_json::{json, Value};
    use zakura_sim::{
        harness::{self, RecordInput, HARNESSES, WITNESS_FILE},
        record::{EndRecord, OutcomeKind},
        trace::{self, ReplayOptions, TraceReader},
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
             zakura-sim replay <dir> [--allow-build-mismatch] [--prefix] [--observe]\n\n\
             `--input` is a full fuzz input: the scenario prefix, then the Plan bytes.\n\
             `--scenario` and `--plan` run Plan bytes against a given scenario.\n\
             `--witness` judges the run as a witness test of the property, writes the\n\
             result to witness.json beside the trace, and exits 1 if the test fails.\n\
             `replay` reruns a trace Exact and exits 0 only if every pick, draw and\n\
             observation matched. `--allow-build-mismatch` replays on a build that isn't\n\
             equivalent to the recording one, for diagnosis, and always exits 1.\n\
             `--prefix` replays a trace without its end record to its last step.\n\
             `--observe` writes the full observation log to observations.jsonl.\n\n\
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
        for arg in args {
            let flag = match arg.as_str() {
                "--allow-build-mismatch" => &mut options.allow_build_mismatch,
                "--prefix" => &mut options.prefix,
                "--observe" => &mut options.observe,
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
        let report =
            trace::replay(&trace, options).map_err(|error| Error::Failed(error.to_string()))?;
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
