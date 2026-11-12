//! The `sim-acceptance` command: the simulator side of
//! `cargo xtask sim-acceptance` (sim-7.4).
//!
//! ```text
//! sim-acceptance plan --repo <dir> --out <plan.json>
//! sim-acceptance evaluate --repo <dir> --plan <plan.json> --artifacts <dir>
//!     --header-out <dir> --subject <commit> --subject-source <digest>
//!     --containment <subreaper|best-effort> --out <dir>
//! ```
//!
//! `plan` writes the tasks xtask runs. `evaluate` judges their artifacts,
//! and hdr-14's published record, and writes `record.json` under `--out`.
//! It exits 0 whether or not acceptance passes, since the record says which.
//!
//! It works only in the simulator build, `RUSTFLAGS="--cfg zakura_sim"`.

/// Prints that this build has no simulator, and exits with code 2.
#[cfg(not(zakura_sim))]
#[allow(clippy::print_stderr)]
fn main() {
    eprintln!("sim-acceptance needs the simulator build: RUSTFLAGS=\"--cfg zakura_sim\"");
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
    use std::{collections::BTreeMap, fs, path::PathBuf};

    use zakura_sim::sim_acceptance::run::{plan, record, HeaderRun, Sim7Plan};

    /// The usage text.
    const USAGE: &str = "usage: sim-acceptance plan --repo <dir> --out <plan.json>\n       \
                         sim-acceptance evaluate --repo <dir> --plan <plan.json> \
                         --artifacts <dir> --header-out <dir> --subject <commit> \
                         --subject-source <digest> --containment <subreaper|best-effort> \
                         --out <dir>";

    /// Reads `--flag value` pairs, refusing an unknown, repeated or
    /// missing flag.
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
                let plan = plan(&PathBuf::from(&flags["--repo"]));
                let json = serde_json::to_string_pretty(&plan).map_err(|e| e.to_string())?;
                fs::write(&flags["--out"], json).map_err(|e| format!("{}: {e}", flags["--out"]))?;
                println!(
                    "{} tasks, {} results the tool can't run yet, {} manifest problems",
                    plan.tasks.len(),
                    plan.unsupported.len(),
                    plan.problems.len()
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
                        "--header-out",
                        "--subject",
                        "--subject-source",
                        "--containment",
                        "--out",
                    ],
                )?;
                let plan: Sim7Plan = zakura_sim::sim_acceptance::header::read_bounded_json(
                    &PathBuf::from(&flags["--plan"]),
                )?;
                let header_out = PathBuf::from(&flags["--header-out"]);
                let record = record(
                    &PathBuf::from(&flags["--repo"]),
                    &plan,
                    &PathBuf::from(&flags["--artifacts"]),
                    &HeaderRun { out: &header_out },
                    &flags["--subject"],
                    &flags["--subject-source"],
                    &flags["--containment"],
                );
                let out = PathBuf::from(&flags["--out"]);
                fs::create_dir_all(&out).map_err(|e| format!("{}: {e}", out.display()))?;
                let json = serde_json::to_string_pretty(&record).map_err(|e| e.to_string())?;
                fs::write(out.join("record.json"), json)
                    .map_err(|e| format!("record.json: {e}"))?;
                let blocked = record
                    .evaluation
                    .rows
                    .iter()
                    .filter(|row| {
                        matches!(
                            row.verdict,
                            zakura_sim::sim_acceptance::evaluate::Verdict::Blocked { .. }
                        )
                    })
                    .count();
                println!(
                    "subject {}: {} rows, {blocked} blocked; accepted: {}",
                    record.subject,
                    record.evaluation.rows.len(),
                    record.accepted
                );
                Ok(())
            }
            _ => Err(USAGE.to_owned()),
        }
    }
}
