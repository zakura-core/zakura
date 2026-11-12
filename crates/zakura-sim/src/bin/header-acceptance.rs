//! The `header-acceptance` command: the simulator side of
//! `cargo xtask header-acceptance` (hdr-14.7a).
//!
//! ```text
//! header-acceptance plan --repo <dir> --out <plan.json>
//! header-acceptance evaluate --repo <dir> --plan <plan.json> --artifacts <dir>
//!     --subject <commit> --subject-source <digest> --run-id <id>
//!     --containment <subreaper|best-effort> --out <dir>
//! header-acceptance verify --repo <dir> --plan <plan.json> --artifacts <dir>
//!     --run-id <id> --subject-source <digest> --record <record.json> --out <dir>
//! header-acceptance companions --trace <dir> --out <companions.json>
//! header-acceptance record-args --declared <dir> --harness <name> --test <name>
//!     --case <case> --run-id <id> --index <task> --role <role> --out <args.json>
//! ```
//!
//! `plan` writes the tasks xtask runs. `evaluate` reads their artifacts,
//! writes `results.json` and `record.json` under `--out`, and prints the
//! record's counts. It exits 0 whether or not acceptance passes, since the
//! record says which. `verify` rebuilds a published record from its run's
//! plan and artifacts and exits 0 only if it's the same record (sim-7.4).
//! `companions` judges a node case's recorded `Draft` run's companions
//! and writes a `CompanionsRun` to a new file. xtask runs it from the
//! case's own patched build, the one that recorded the trace.
//! `record-args` writes, as a JSON list, the `zakura-sim record` arguments
//! for the one history a witness test declared under `--declared` as
//! `--case` on `--harness`, for the run `--run-id`, task `--index` and role
//! `--role` (empty for the task's own), so the runner records it as the
//! reader binds it.
//!
//! It works only in the simulator build, `RUSTFLAGS="--cfg zakura_sim"`.

/// Prints that this build has no simulator, and exits with code 2.
#[cfg(not(zakura_sim))]
#[allow(clippy::print_stderr)]
fn main() {
    eprintln!("header-acceptance needs the simulator build: RUSTFLAGS=\"--cfg zakura_sim\"");
    std::process::exit(2);
}

/// Runs the command line, and exits with code 2 for a usage error or 1 for
/// any other failure.
#[cfg(zakura_sim)]
#[allow(clippy::print_stderr)]
fn main() {
    if let Err(message) = cli::run(std::env::args().skip(1).collect()) {
        eprintln!("error: {message}");
        std::process::exit(if message.starts_with("usage") { 2 } else { 1 });
    }
}

#[cfg(zakura_sim)]
mod cli {
    use std::{
        collections::BTreeMap,
        fs,
        path::{Path, PathBuf},
    };

    use zakura_sim::{
        harness::header_acceptance::{
            companions,
            tool::{self, CompanionVerdict, CompanionsRun, RunPlan},
            Manifest,
        },
        trace::{BuildInfo, TraceReader},
    };

    /// The usage text.
    const USAGE: &str = "usage: header-acceptance plan --repo <dir> --out <plan.json>\n       \
                         header-acceptance evaluate --repo <dir> --plan <plan.json> \
                         --artifacts <dir> --subject <commit> --subject-source <digest> \
                         --run-id <id> --containment <subreaper|best-effort> --out <dir>\n       \
                         header-acceptance verify --repo <dir> --plan <plan.json> \
                         --artifacts <dir> --run-id <id> --subject-source <digest> \
                         --record <record.json> --out <dir>\n       \
                         header-acceptance companions --trace <dir> --out <companions.json>\n       \
                         header-acceptance record-args --declared <dir> --harness <name> \
                         --test <name> --case <case> --run-id <id> --index <task> \
                         --role <role> --out <args.json>";

    /// Reads `--flag value` pairs, refusing an unknown or repeated flag.
    fn flags(args: &[String], known: &[&str]) -> Result<BTreeMap<String, String>, String> {
        let mut flags = BTreeMap::new();
        let mut args = args.iter();
        while let Some(flag) = args.next() {
            if !known.contains(&flag.as_str()) {
                return Err(format!("{USAGE}\nunknown argument {flag}"));
            }
            let value = args
                .next()
                .ok_or_else(|| format!("{USAGE}\n{flag} needs a value"))?;
            if flags.insert(flag.clone(), value.clone()).is_some() {
                return Err(format!("{USAGE}\n{flag} given twice"));
            }
        }
        for flag in known {
            if !flags.contains_key(*flag) {
                return Err(format!("{USAGE}\nmissing {flag}"));
            }
        }
        Ok(flags)
    }

    /// Runs `args`.
    #[allow(clippy::print_stdout)]
    pub fn run(args: Vec<String>) -> Result<(), String> {
        let (command, rest) = args.split_first().ok_or_else(|| USAGE.to_owned())?;
        match command.as_str() {
            "plan" => {
                let flags = flags(rest, &["--repo", "--out"])?;
                let plan = tool::plan(&Manifest::committed(), &PathBuf::from(&flags["--repo"]))?;
                let json = serde_json::to_string_pretty(&plan).map_err(|e| e.to_string())?;
                fs::write(&flags["--out"], json).map_err(|e| format!("{}: {e}", flags["--out"]))?;
                println!(
                    "{} tasks, {} expected results the tool can't produce yet",
                    plan.tasks.len(),
                    plan.unsupported.len()
                );
                Ok(())
            }
            "evaluate" => {
                let flags = flags(
                    rest,
                    &[
                        "--repo",
                        "--plan",
                        "--artifacts",
                        "--subject",
                        "--subject-source",
                        "--run-id",
                        "--containment",
                        "--out",
                    ],
                )?;
                let plan: RunPlan = serde_json::from_str(
                    &fs::read_to_string(&flags["--plan"])
                        .map_err(|e| format!("{}: {e}", flags["--plan"]))?,
                )
                .map_err(|e| format!("{}: {e}", flags["--plan"]))?;
                let artifacts = PathBuf::from(&flags["--artifacts"]);
                let run = tool::RunBinding {
                    run_id: flags["--run-id"].clone(),
                    subject: flags["--subject"].clone(),
                    subject_source: flags["--subject-source"].clone(),
                    containment: flags["--containment"].parse()?,
                };
                let (results, mut record) = tool::evaluate_artifacts(
                    &PathBuf::from(&flags["--repo"]),
                    &plan,
                    &artifacts,
                    &run,
                );
                let out = PathBuf::from(&flags["--out"]);
                record.artifacts = tool::record_artifacts(&artifacts, &out);
                fs::create_dir_all(&out).map_err(|e| format!("{}: {e}", out.display()))?;
                for (name, json) in [
                    ("results.json", serde_json::to_string_pretty(&results)),
                    ("record.json", serde_json::to_string_pretty(&record)),
                ] {
                    let json = json.map_err(|e| e.to_string())?;
                    fs::write(out.join(name), json).map_err(|e| format!("{name}: {e}"))?;
                }
                println!(
                    "subject {}: {} verified, {} excluded, {} blocked, {} stray; accepted: {}",
                    record.subject,
                    record.verified.len(),
                    record.excluded.len(),
                    record.blocked.len(),
                    record.stray.len(),
                    record.accepted
                );
                Ok(())
            }
            "verify" => {
                let flags = flags(
                    rest,
                    &[
                        "--repo",
                        "--plan",
                        "--artifacts",
                        "--run-id",
                        "--subject-source",
                        "--record",
                        "--out",
                    ],
                )?;
                use zakura_sim::sim_acceptance::header::read_bounded_json;
                let plan: RunPlan = read_bounded_json(Path::new(&flags["--plan"]))?;
                let record: tool::Record = read_bounded_json(Path::new(&flags["--record"]))?;
                let (artifacts, out) = (
                    PathBuf::from(&flags["--artifacts"]),
                    PathBuf::from(&flags["--out"]),
                );
                zakura_sim::sim_acceptance::header::verify(
                    &PathBuf::from(&flags["--repo"]),
                    &zakura_sim::sim_acceptance::header::Published {
                        plan: &plan,
                        artifacts: &artifacts,
                        out: &out,
                        run_id: &flags["--run-id"],
                        subject_source: &flags["--subject-source"],
                        record: &record,
                    },
                )?;
                println!("subject {}: the record matches its run", record.subject);
                Ok(())
            }
            "companions" => {
                let flags = flags(rest, &["--trace", "--out"])?;
                let dir = PathBuf::from(&flags["--trace"]);
                let trace = TraceReader::open(&dir)
                    .map_err(|e| format!("{}: {e}", dir.display()))?
                    .id;
                // A trace with no verdict fails every companion, with why.
                let verdicts = match companions::judge_trace(&dir) {
                    Ok(verdicts) => verdicts,
                    Err(why) => companions::COMPANIONS
                        .iter()
                        .map(|&name| (name, Err(why.clone())))
                        .collect(),
                };
                let run = CompanionsRun {
                    trace,
                    source: BuildInfo::current().source,
                    companions: verdicts
                        .into_iter()
                        .map(|(name, verdict)| CompanionVerdict {
                            name: name.to_owned(),
                            held: verdict.is_ok(),
                            why: verdict.err(),
                        })
                        .collect(),
                };
                let json = serde_json::to_string_pretty(&run).map_err(|e| e.to_string())?;
                // Never over an existing file, such as another run's.
                fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&flags["--out"])
                    .and_then(|mut out| std::io::Write::write_all(&mut out, json.as_bytes()))
                    .map_err(|e| format!("{}: {e}", flags["--out"]))?;
                println!(
                    "{} of {} companions held",
                    run.companions.iter().filter(|c| c.held).count(),
                    run.companions.len()
                );
                Ok(())
            }
            "record-args" => {
                let flags = flags(
                    rest,
                    &[
                        "--declared",
                        "--harness",
                        "--test",
                        "--case",
                        "--run-id",
                        "--index",
                        "--role",
                        "--out",
                    ],
                )?;
                let at = tool::TaskAt {
                    run_id: &flags["--run-id"],
                    index: flags["--index"]
                        .parse()
                        .map_err(|e| format!("{USAGE}\n--index: {e}"))?,
                };
                let args = tool::declared_record_args(
                    &PathBuf::from(&flags["--declared"]),
                    (at, &flags["--role"]),
                    &flags["--harness"],
                    &flags["--test"],
                    &flags["--case"],
                )?;
                let json = serde_json::to_string(&args).map_err(|e| e.to_string())?;
                fs::write(&flags["--out"], json).map_err(|e| format!("{}: {e}", flags["--out"]))?;
                Ok(())
            }
            _ => Err(USAGE.to_owned()),
        }
    }
}
