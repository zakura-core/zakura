//! `zakura-sim generate`'s search (mutation plan, mut-4.1): a seed gives
//! the same result on every run, a clean search writes no end record, a
//! kill writes exactly one, and an excluded input is skipped. Each search
//! runs a race fixture, whose runs take milliseconds.
#![cfg(zakura_sim)]

use std::{collections::BTreeSet, env, fs, path::Path};

use sha2::{Digest, Sha256};
use zakura_sim::{
    explore::generate::{
        generate, generator_harnesses, inputs, read_exclusions, GenerateOptions, GenerateReport,
        Stop, REPORT_FILE,
    },
    harness::{self, HarnessEntry},
    trace::end_records::END_RECORDS_VAR,
};

/// A planted race, whose runs violate its property on some schedules.
const PLANTED: &str = "race/lost-update";

/// The race fixtures' fixed variants, whose runs violate nothing.
const CLEAN: &str = "fixtures/fixed";

/// The seed every search here starts from.
const SEED: u64 = 7;

/// The inputs a search may draw: enough for the planted race to fail.
const RUNS: u64 = 200;

fn entry(name: &str) -> &'static HarnessEntry {
    harness::find(name).unwrap()
}

fn options(out: &Path, records: Option<&Path>) -> GenerateOptions {
    GenerateOptions {
        seed: SEED,
        runs: RUNS,
        max_bytes: 64,
        exclude: BTreeSet::new(),
        out: out.to_owned(),
        records: records.map(Path::to_owned),
        abandon_limit: 1,
    }
}

/// The JSON files in `dir`, which may not exist.
fn files(dir: &Path) -> usize {
    fs::read_dir(dir).map_or(0, |entries| entries.count())
}

/// The report without the timing, which differs between runs.
fn untimed(report: &GenerateReport) -> GenerateReport {
    GenerateReport {
        millis: 0,
        input: None,
        ..report.clone()
    }
}

#[test]
fn inputs_follow_the_seed_and_stay_within_the_bound() {
    let first: Vec<Vec<u8>> = inputs(SEED, 16).take(64).collect();
    assert_eq!(first, inputs(SEED, 16).take(64).collect::<Vec<_>>());
    assert_ne!(first, inputs(SEED + 1, 16).take(64).collect::<Vec<_>>());
    assert!(first.iter().all(|input| input.len() <= 16));
    assert!(first.iter().any(|input| input.len() == 16));
}

#[test]
fn the_same_seed_gives_the_same_kill() {
    let root = tempfile::tempdir().unwrap();
    let one = generate(entry(PLANTED), &options(&root.path().join("one"), None)).unwrap();
    let two = generate(entry(PLANTED), &options(&root.path().join("two"), None)).unwrap();
    assert!(
        matches!(one.stop, Stop::Violation { .. }),
        "the planted race fails within {RUNS} runs: {one:?}"
    );
    assert_eq!(untimed(&one), untimed(&two));
    let kill = one.kill_run.unwrap();
    assert_eq!(one.executed, kill + 1, "the search stops at the kill");
    let input = fs::read(one.input.as_ref().unwrap()).unwrap();
    assert_eq!(
        Some(input),
        inputs(SEED, 64).nth(usize::try_from(kill).unwrap()),
        "the saved input is the one drawn at the kill's index"
    );
    let written: GenerateReport =
        serde_json::from_slice(&fs::read(root.path().join("one").join(REPORT_FILE)).unwrap())
            .unwrap();
    assert_eq!(written, one);
    let property = one.violated.as_deref().unwrap();
    assert!(one.properties[property].violated == 1);
}

/// With the hook's variable set, the search's runs write no records, and
/// only the stopping run's end record goes to the record directory. A
/// clean search writes none.
#[test]
fn only_the_kill_writes_an_end_record() {
    let root = tempfile::tempdir().unwrap();
    let records = root.path().join("records");
    // Every run in this binary runs inside a search, which holds the hook,
    // so setting the variable here affects no other test.
    env::set_var(END_RECORDS_VAR, &records);
    let clean = generate(
        entry(CLEAN),
        &GenerateOptions {
            runs: 20,
            ..options(&root.path().join("clean"), Some(&records))
        },
    )
    .unwrap();
    assert_eq!(clean.stop, Stop::Clean, "{clean:?}");
    assert_eq!(clean.executed, 20);
    assert_eq!(clean.kill_run, None);
    assert!(!clean.record);
    assert_eq!(files(&records), 0, "a clean search writes no record");
    assert_eq!(
        files(&root.path().join("clean")),
        1,
        "a clean search saves no input"
    );
    let killed = generate(
        entry(PLANTED),
        &options(&root.path().join("killed"), Some(&records)),
    )
    .unwrap();
    env::remove_var(END_RECORDS_VAR);
    assert!(killed.record);
    assert_eq!(files(&records), 1, "one record for the kill alone");
    let record: serde_json::Value = serde_json::from_slice(
        &fs::read(
            fs::read_dir(&records)
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .path(),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        record["end"]["violation"]["property"].as_str(),
        killed.violated.as_deref()
    );
}

#[test]
fn an_excluded_input_is_skipped() {
    let root = tempfile::tempdir().unwrap();
    let first = generate(entry(PLANTED), &options(&root.path().join("first"), None)).unwrap();
    let kill = first.kill_run.unwrap();
    let digest = first.input_sha256.clone().unwrap();
    let excluded = generate(
        entry(PLANTED),
        &GenerateOptions {
            exclude: read_exclusions(&format!("# the first kill\n\n{digest}\n")).unwrap(),
            ..options(&root.path().join("excluded"), None)
        },
    )
    .unwrap();
    assert!(excluded.skipped >= 1, "{excluded:?}");
    assert_ne!(excluded.input_sha256.as_deref(), Some(digest.as_str()));
    if let Some(later) = excluded.kill_run {
        assert!(later > kill, "the search went past the excluded input");
    }
    assert_eq!(
        excluded.executed + excluded.skipped,
        excluded.kill_run.map_or(RUNS, |kill| kill + 1)
    );
    assert!(read_exclusions("not-a-digest").is_err());
    assert_eq!(
        read_exclusions(&format!("{}\n", hex(&Sha256::digest(b"")).to_uppercase()))
            .unwrap()
            .len(),
        1
    );
}

/// The map leaves out a harness that plants its bugs, and keeps every
/// harness that reports a property.
#[test]
fn the_generator_map_leaves_out_planted_harnesses() {
    let map = generator_harnesses();
    assert!(map.values().flatten().all(|name| *name != PLANTED));
    assert!(map["sim/no-task-panic"].contains(&CLEAN));
    assert!(map["block-sync/retained-byte-bound"].contains(&"block-sync/storage"));
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
