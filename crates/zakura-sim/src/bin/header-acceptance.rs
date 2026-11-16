//! The `header-acceptance` command: the simulator side of
//! `cargo xtask header-acceptance` (hdr-14.7a).
//!
//! ```text
//! header-acceptance plan --repo <dir> --out <plan.json>
//! header-acceptance evaluate --repo <dir> --plan <plan.json> --artifacts <dir>
//!     --subject <commit> --subject-source <digest> --run-id <id>
//!     --containment <subreaper|best-effort> --out <dir>
//! ```
//!
//! `plan` writes the tasks xtask runs. `evaluate` reads their artifacts,
//! writes `results.json` and `record.json` under `--out`, and prints the
//! record's counts. It exits 0 whether or not acceptance passes, since the
//! record says which.
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
    use std::{collections::BTreeMap, fs, path::PathBuf};

    use zakura_sim::harness::header_acceptance::{
        tool::{self, RunPlan},
        Manifest,
    };

    /// The usage text.
    const USAGE: &str = "usage: header-acceptance plan --repo <dir> --out <plan.json>\n       \
                         header-acceptance evaluate --repo <dir> --plan <plan.json> \
                         --artifacts <dir> --subject <commit> --subject-source <digest> \
                         --run-id <id> --containment <subreaper|best-effort> --out <dir>";

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
            _ => Err(USAGE.to_owned()),
        }
    }
}
