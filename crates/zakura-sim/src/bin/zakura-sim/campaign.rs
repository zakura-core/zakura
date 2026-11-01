//! `zakura-sim campaign`: builds, starts, resumes, closes and checks
//! durable fuzz campaigns in a store directory.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    str::FromStr,
};

use zakura_sim::{
    campaign::{
        self, build_fuzzer, Budget, CampaignError, Compaction, Digest, End, Evidence, Migration,
        Prepared, ResumeOptions, RunState, StartOptions, Status, Store, WorkerPlan, FUZZ_PACKAGE,
        SANITIZER_VARS,
    },
    explore::{stats::Counts, DEFAULT_ABANDON_LIMIT},
    harness,
    property::Status as PropertyStatus,
};

use super::cli::Error;

/// The usage text.
pub const USAGE: &str = "\
  zakura-sim campaign build --store <dir> --target <name> [--sanitizer address|none]
      [--target-dir <dir>]
  zakura-sim campaign start --store <dir> --campaign <name> --sim <name> --build <sha256>
      --seconds <n> [--workers <n>] [--seed <n>] [--cpus <list>]... [--max-len <n>]
      [--timeout <s>] [--rss-limit-mb <n>] [--max-restarts <n>] [--abandon-limit <n>]
      [--feedback on|off] [--compact-secs <s>] [--fixtures <dir>]... [--suite <file>]
      [--artifacts <uri>] [--libfuzzer-arg <flag>]... [--parent <sha256> --corpus carry|seeds]
  zakura-sim campaign resume --store <dir> [--run <sha256>] [--build <sha256>] [--seconds <n>]
      [--workers <n>]
  zakura-sim campaign close --store <dir> [--run <sha256>]
  zakura-sim campaign status --store <dir>
  zakura-sim campaign evidence --store <dir> [--run <sha256>] [--out <file>]
  zakura-sim campaign verify --store <dir>
";

/// What each subcommand does.
pub const HELP: &str = "\
`campaign` keeps a fuzz campaign in --store, a directory on durable storage.
`build` builds a fuzz target of qa/fuzz/sim with the pinned nightly and saves
the binary in the store with its build record, named by its SHA-256.
`start` writes a run manifest whose initial snapshot holds the harness's
curated seeds, then runs --workers fuzzer processes for --seconds each and
restarts one that ends early up to --max-restarts times. When they stop, it
saves the union of the initial snapshot and every worker's corpus as a new
snapshot, compacts it with libFuzzer's merge if --compact-secs is set, and
writes the run's end record. With --parent, it starts a new segment after that
run, and --corpus says whether its last snapshot carries over.
`resume` ends the last run with `close` if it's open, checks its snapshot and
failure files, and runs a new manifest from its snapshot. It refuses another
build identity, harness or feedback: those start a new segment.
`close` ends an open run whose supervisor stopped: it saves the snapshot from
what the workers left, and records the run as interrupted.
`status` lists every run. `verify` checks every record against its digest.
`evidence` sums what a run's fuzzer processes counted, per worker and for the
run: runs by outcome and end, budget exits, cleanup, each property's outcomes
and assumption losses, and the features reached. It lists the properties no
complete process saw apply, and every worker, process and stats file whose
counts fall short. --out also writes it as JSON.
`start` and `resume` exit 0 for a complete run and 1 otherwise.
";

/// Runs `campaign <subcommand> <flags>`.
pub fn run(args: Vec<String>) -> Result<(), Error> {
    let mut args = args.into_iter();
    let subcommand = args
        .next()
        .ok_or_else(|| Error::Usage("`campaign` needs a subcommand".to_owned()))?;
    let flags = Flags::parse(args.collect())?;
    let root = PathBuf::from(flags.required("--store")?);
    if !matches!(subcommand.as_str(), "build" | "start") && !root.is_dir() {
        return Err(Error::Usage(format!(
            "--store {} isn't a directory; `build` and `start` create a store",
            root.display()
        )));
    }
    let store = Store::open(&root).map_err(failed)?;
    match subcommand.as_str() {
        "build" => build(&store, &flags),
        "start" => start(&store, &flags),
        "resume" => resume(&store, &flags),
        "close" => close(&store, &flags),
        "status" => status(&store, &flags),
        "evidence" => evidence(&store, &flags),
        "verify" => verify(&store, &flags),
        other => Err(Error::Usage(format!(
            "unknown campaign subcommand `{other}`"
        ))),
    }
}

/// `build`: builds the fuzzer and saves it in the store.
#[allow(clippy::print_stdout)]
fn build(store: &Store, flags: &Flags) -> Result<(), Error> {
    flags.only(&["--store", "--target", "--sanitizer", "--target-dir"])?;
    let target = flags.required("--target")?;
    let sanitizer = flags.one("--sanitizer")?.unwrap_or("address");
    let repo = repo_root();
    let target_dir = flags
        .one("--target-dir")?
        .map_or_else(|| repo.join(FUZZ_PACKAGE).join("target"), PathBuf::from);
    let (bytes, identity) = build_fuzzer(&repo, target, sanitizer, &target_dir).map_err(failed)?;
    let build = store.save_build(&bytes, identity).map_err(failed)?;
    println!(
        "saved fuzz target {target} ({sanitizer}) from commit {}{} as build {}",
        build.identity.git,
        if build.identity.dirty { ", dirty" } else { "" },
        build.binary
    );
    Ok(())
}

/// `start`: writes a segment's first manifest, and runs it.
fn start(store: &Store, flags: &Flags) -> Result<(), Error> {
    flags.only(&[
        "--store",
        "--campaign",
        "--sim",
        "--build",
        "--seconds",
        "--workers",
        "--seed",
        "--cpus",
        "--max-len",
        "--timeout",
        "--rss-limit-mb",
        "--max-restarts",
        "--abandon-limit",
        "--feedback",
        "--compact-secs",
        "--fixtures",
        "--suite",
        "--artifacts",
        "--libfuzzer-arg",
        "--parent",
        "--corpus",
    ])?;
    let entry =
        harness::find(flags.required("--sim")?).map_err(|error| Error::Usage(error.to_string()))?;
    let workers: usize = flags.number("--workers")?.unwrap_or(1);
    let seed: u32 = flags.number("--seed")?.unwrap_or(1);
    let cpus = flags.many("--cpus");
    if !cpus.is_empty() && cpus.len() != workers {
        return Err(Error::Usage(format!(
            "--cpus gives {} CPU lists for {workers} workers; give one per worker, or none",
            cpus.len()
        )));
    }
    let parent = match (flags.one("--parent")?, flags.one("--corpus")?) {
        (None, None) => None,
        (Some(parent), Some(corpus)) => Some((
            digest(parent)?,
            match corpus {
                "carry" => Migration::Carry,
                "seeds" => Migration::Seeds,
                other => {
                    return Err(Error::Usage(format!(
                        "--corpus is `carry` or `seeds`, not `{other}`"
                    )))
                }
            },
        )),
        _ => return Err(Error::Usage("--parent and --corpus go together".to_owned())),
    };
    let mut fixtures: Vec<PathBuf> = flags.many("--fixtures").iter().map(PathBuf::from).collect();
    if fixtures.is_empty() {
        fixtures.push(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures"));
    }
    let options = StartOptions {
        campaign: flags.required("--campaign")?.to_owned(),
        harness: entry,
        build: digest(flags.required("--build")?)?,
        workers: (0..workers)
            .map(|index| WorkerPlan {
                index,
                // Truncating an index past `u32::MAX` only repeats a seed.
                seed: seed.wrapping_add(index as u32),
                cpus: cpus.get(index).map(|cpus| (*cpus).to_owned()),
            })
            .collect(),
        budget: Budget {
            seconds: flags
                .number("--seconds")?
                .ok_or_else(|| Error::Usage("`start` needs --seconds".to_owned()))?,
            max_restarts: flags.number("--max-restarts")?.unwrap_or(16),
            max_len: flags.number("--max-len")?.unwrap_or(4096),
            timeout_secs: flags.number("--timeout")?.unwrap_or(30),
            rss_limit_mb: flags.number("--rss-limit-mb")?.unwrap_or(4096),
            abandon_limit: flags
                .number("--abandon-limit")?
                .unwrap_or(DEFAULT_ABANDON_LIMIT),
            feedback_mode: flags.one("--feedback")?.map(str::to_owned),
            sanitizer_options: SANITIZER_VARS
                .iter()
                .filter_map(|name| Some(((*name).to_owned(), std::env::var(name).ok()?)))
                .collect(),
            compact_secs: flags.number("--compact-secs")?,
            libfuzzer_args: flags
                .many("--libfuzzer-arg")
                .into_iter()
                .map(str::to_owned)
                .collect(),
        },
        fixtures,
        suite: flags.one("--suite")?.map(PathBuf::from),
        artifacts: flags.one("--artifacts")?.map(str::to_owned),
        parent,
    };
    let prepared = campaign::prepare_start(store, options).map_err(failed)?;
    run_prepared(store, prepared)
}

/// `resume`: writes a manifest that resumes the last run, and runs it.
fn resume(store: &Store, flags: &Flags) -> Result<(), Error> {
    flags.only(&["--store", "--run", "--build", "--seconds", "--workers"])?;
    let options = ResumeOptions {
        parent: flags.one("--run")?.map(digest).transpose()?,
        build: flags.one("--build")?.map(digest).transpose()?,
        seconds: flags.number("--seconds")?,
        workers: flags.number("--workers")?,
    };
    let prepared = campaign::prepare_resume(store, options).map_err(failed)?;
    run_prepared(store, prepared)
}

/// Prints the manifest, runs it, and prints its end. Fails unless the run
/// is complete.
#[allow(clippy::print_stdout)]
fn run_prepared(store: &Store, prepared: Prepared) -> Result<(), Error> {
    let manifest = prepared.manifest().clone();
    println!(
        "manifest {manifest} in {}",
        store.manifest_path(&manifest).display()
    );
    let end = prepared.run(store).map_err(failed)?;
    print_end(&manifest, &end);
    match end.status {
        Status::Complete => Ok(()),
        status => Err(Error::Failed(format!(
            "run {manifest} ended {}",
            status_name(status)
        ))),
    }
}

/// `close`: ends an open run as interrupted.
#[allow(clippy::print_stdout)]
fn close(store: &Store, flags: &Flags) -> Result<(), Error> {
    flags.only(&["--store", "--run"])?;
    let run = named_run(store, flags)?;
    let end = campaign::close(store, &run).map_err(failed)?;
    print_end(&run, &end);
    Ok(())
}

/// The run `--run` names, else the only run that no run continues.
fn named_run(store: &Store, flags: &Flags) -> Result<Digest, Error> {
    match flags.one("--run")? {
        Some(run) => digest(run),
        None => match store.leaves().map_err(failed)?.as_slice() {
            [leaf] => Ok(leaf.clone()),
            leaves => Err(Error::Usage(format!(
                "the store holds {} runs that no run continues; name one with --run",
                leaves.len()
            ))),
        },
    }
}

/// `evidence`: prints a run's evidence, and writes it as JSON to `--out`.
#[allow(clippy::print_stdout)]
fn evidence(store: &Store, flags: &Flags) -> Result<(), Error> {
    flags.only(&["--store", "--run", "--out"])?;
    let run = named_run(store, flags)?;
    let evidence = campaign::evidence(store, &run).map_err(failed)?;
    if let Some(out) = flags.one("--out")? {
        let json = serde_json::to_vec_pretty(&evidence)
            .map_err(|error| Error::Failed(format!("serializing the evidence: {error}")))?;
        fs::write(out, json).map_err(|error| Error::Failed(format!("writing {out}: {error}")))?;
    }
    print!("{}", evidence_text(&evidence));
    Ok(())
}

/// `evidence` in words, one fact per line.
fn evidence_text(evidence: &Evidence) -> String {
    let mut text = format!(
        "run {}: {}, {}\n",
        evidence.run,
        evidence.harness,
        evidence.status.map_or("without an end record", status_name)
    );
    for worker in &evidence.workers {
        let attempts: Vec<String> = worker
            .attempts
            .iter()
            .map(|(end, n)| format!("{} {n}", name(end)))
            .collect();
        text += &format!(
            "worker {}: {}; processes: {}; stats files: {}; {}\n",
            worker.index,
            worker
                .outcome
                .as_ref()
                .map_or_else(|| "no outcome".to_owned(), name),
            attempts.join(", "),
            worker.processes,
            counts_line(&worker.counts),
        );
    }
    let totals = &evidence.totals;
    text += &format!("total: {}\n", counts_line(totals));
    text += &format!(
        "complete processes: {}\n",
        counts_line(&evidence.complete_totals)
    );
    text += &format!("ends: {}\n", pairs(&totals.ends));
    text += &format!(
        "budget exits: {}; drains exhausted: {}\n",
        totals.budget_exits(),
        totals.drains_exhausted
    );
    let cleanup = &totals.cleanup;
    text += &format!(
        "cleanup: abandoned {} ({} tasks leaked), unwound {}, panicked {}, unwind failures {}\n",
        cleanup.abandoned,
        cleanup.leaked_tasks,
        cleanup.unwound,
        cleanup.panicked,
        cleanup.unwind_failures
    );
    let (distinct, families) = totals.feature_totals();
    let families: Vec<String> = families
        .iter()
        .map(|(family, n)| format!("{family} {n}"))
        .collect();
    text += &format!("features: {distinct} distinct ({})\n", families.join(", "));
    for (id, counts) in &totals.properties {
        text += &format!(
            "property {id} [{}]: held {} ({} evaluations), not applicable {}, inconclusive {} \
             ({}), assumption losses {}, violated {}\n",
            property_status(counts.status),
            counts.held,
            counts.evaluations,
            counts.not_applicable,
            counts.inconclusive.values().sum::<u64>(),
            pairs(&counts.inconclusive),
            counts.assumption_losses(),
            counts.violated
        );
    }
    text += &format!("unapplied: {}\n", evidence.unapplied.join(", "));
    for incomplete in &evidence.incomplete {
        text += &format!(
            "incomplete: {}\n",
            serde_json::to_string(incomplete).expect("a gap holds only strings and integers")
        );
    }
    text
}

/// A worker's or a run's runs by outcome.
fn counts_line(counts: &Counts) -> String {
    format!("{} runs ({})", counts.runs, pairs(&counts.outcomes))
}

/// `name count` pairs, comma-separated.
fn pairs(map: &BTreeMap<String, u64>) -> String {
    map.iter()
        .map(|(key, n)| format!("{key} {n}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// A unit variant's name, as its record holds it.
fn name(value: &impl serde::Serialize) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_default()
}

/// A property status in words: `active`, `known #<issue>` or `draft`.
fn property_status(status: PropertyStatus) -> String {
    match status {
        PropertyStatus::Active => "active".to_owned(),
        PropertyStatus::Known { issue } => format!("known #{issue}"),
        PropertyStatus::Draft => "draft".to_owned(),
    }
}

/// `status`: lists every run, oldest first.
#[allow(clippy::print_stdout)]
fn status(store: &Store, flags: &Flags) -> Result<(), Error> {
    flags.only(&["--store"])?;
    let mut manifests: Vec<_> = store.manifests().map_err(failed)?.into_iter().collect();
    manifests.sort_by_key(|(digest, manifest)| (manifest.created_unix, digest.clone()));
    for (digest, manifest) in &manifests {
        let parent = manifest
            .resume_parent
            .as_ref()
            .map_or_else(String::new, |parent| format!(", resumes {parent}"));
        let state = match store.run_state(digest).map_err(failed)? {
            RunState::Running => "running".to_owned(),
            RunState::Open => "open: its supervisor stopped; close or resume it".to_owned(),
            RunState::Ended => {
                let end = campaign::read_end(store, digest).map_err(failed)?;
                format!("{}; {}", status_name(end.status), summary(&end))
            }
        };
        println!(
            "run {digest}: {} segment {}, {} on {}, {} workers x {} s{parent}: {state}",
            manifest.campaign,
            manifest.segment,
            manifest.harness.name,
            manifest.build.identity.target,
            manifest.workers.len(),
            manifest.budget.seconds
        );
    }
    println!("{} runs", manifests.len());
    Ok(())
}

/// `verify`: checks every record in the store.
#[allow(clippy::print_stdout)]
fn verify(store: &Store, flags: &Flags) -> Result<(), Error> {
    flags.only(&["--store"])?;
    let verified = campaign::verify(store).map_err(failed)?;
    println!(
        "verified {} manifests ({} ended, {} open, {} running), {} snapshots, {} builds and \
         {} failure files",
        verified.manifests,
        verified.ended,
        verified.open,
        verified.running,
        verified.snapshots,
        verified.builds,
        verified.failures
    );
    Ok(())
}

/// Prints how run `manifest` ended.
#[allow(clippy::print_stdout)]
fn print_end(manifest: &Digest, end: &End) {
    println!(
        "run {manifest} ended {}: {}",
        status_name(end.status),
        summary(end)
    );
    for worker in &end.workers {
        println!(
            "worker {}: {:?} after {} processes, {} ms",
            worker.index, worker.outcome, worker.attempts, worker.millis
        );
    }
}

/// The end record's snapshot, compaction and failures, in words.
fn summary(end: &End) -> String {
    let compaction = match &end.compaction {
        None => String::new(),
        Some(Compaction::Compacted { snapshot, inputs }) => {
            format!(", compacted to snapshot {snapshot} of {inputs} inputs")
        }
        Some(Compaction::Failed { failure }) => {
            format!(", whose compaction failed ({failure:?})")
        }
    };
    format!(
        "snapshot {} holds {} inputs, {} added{compaction}; {} failure files",
        end.snapshot,
        end.inputs,
        end.added,
        end.failures.len()
    )
}

/// `status`'s name in records.
fn status_name(status: Status) -> &'static str {
    match status {
        Status::Complete => "complete",
        Status::WorkerStopped => "worker-stopped",
        Status::Interrupted => "interrupted",
    }
}

/// `text` as a digest.
fn digest(text: &str) -> Result<Digest, Error> {
    text.parse().map_err(Error::Usage)
}

/// A campaign error as a failed command.
fn failed(error: CampaignError) -> Error {
    Error::Failed(error.to_string())
}

/// The checkout this binary was built from.
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// A subcommand's flags, each `--name value`, by name.
struct Flags(BTreeMap<String, Vec<String>>);

impl Flags {
    /// Reads `--name value` pairs.
    fn parse(args: Vec<String>) -> Result<Flags, Error> {
        let mut flags: BTreeMap<String, Vec<String>> = BTreeMap::new();
        let mut args = args.into_iter();
        while let Some(flag) = args.next() {
            if !flag.starts_with("--") {
                return Err(Error::Usage(format!("expected a flag, not `{flag}`")));
            }
            let value = args
                .next()
                .ok_or_else(|| Error::Usage(format!("{flag} needs a value")))?;
            flags.entry(flag).or_default().push(value);
        }
        Ok(Flags(flags))
    }

    /// Refuses a flag outside `known`.
    fn only(&self, known: &[&str]) -> Result<(), Error> {
        match self.0.keys().find(|flag| !known.contains(&flag.as_str())) {
            Some(flag) => Err(Error::Usage(format!("unknown flag `{flag}`"))),
            None => Ok(()),
        }
    }

    /// Every value of `flag`, in order.
    fn many(&self, flag: &str) -> Vec<&str> {
        self.0
            .get(flag)
            .map(|values| values.iter().map(String::as_str).collect())
            .unwrap_or_default()
    }

    /// The value of `flag`, which may appear once.
    fn one(&self, flag: &str) -> Result<Option<&str>, Error> {
        match self.many(flag).as_slice() {
            [] => Ok(None),
            [value] => Ok(Some(value)),
            _ => Err(Error::Usage(format!("{flag} given twice"))),
        }
    }

    /// The value of `flag`, which must appear once.
    fn required(&self, flag: &str) -> Result<&str, Error> {
        self.one(flag)?
            .ok_or_else(|| Error::Usage(format!("missing {flag}")))
    }

    /// The value of `flag` as a number, if given.
    fn number<T: FromStr>(&self, flag: &str) -> Result<Option<T>, Error>
    where
        T::Err: std::fmt::Display,
    {
        self.one(flag)?
            .map(|value| {
                value
                    .parse()
                    .map_err(|error| Error::Usage(format!("{flag}: {error}")))
            })
            .transpose()
    }
}
