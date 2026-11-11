//! `zakura-sim generate`'s search (mutation plan, mut-4.1): a seed gives
//! the same result on every run, a clean search writes no end record, a
//! kill writes exactly one, and an excluded input is skipped. Each search
//! runs a race fixture, whose runs take milliseconds. Every tag a harness's
//! draw matches is one of its scenario families, and the draws reach each
//! family; those tests draw scenarios and run nothing.
#![cfg(zakura_sim)]

use std::{
    collections::{BTreeSet, HashMap},
    env, fs,
    path::Path,
};

use sha2::{Digest, Sha256};
use zakura_sim::{
    explore::generate::{
        draws, generate, generator_harnesses, inputs, read_exclusions, GenerateOptions,
        GenerateReport, Stop, REPORT_FILE, UNTAGGED,
    },
    harness::{self, HarnessEntry, HARNESSES},
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
    // The race fixture's draw has no families.
    assert_eq!(one.family, None);
    assert!(one.families.is_empty());
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

/// Every harness draw's tags are listed: of all 65,536 two-byte prefixes
/// (the rest zero), each one that draws a rare scenario, one that at most
/// one prefix in 256 draws, starts with one of the harness's families. A
/// one-byte tag, such as a grammar's version byte, draws one scenario from
/// its 256 prefixes.
/// A draw that matches a new tag fails here until the harness lists it.
/// Some prefix that each listed tag starts draws a rare scenario, so no
/// family is stale.
#[test]
fn every_fixed_tag_a_draw_matches_is_a_listed_family() {
    const RARE: usize = 256;
    for entry in HARNESSES
        .iter()
        // The handshake decoder uses byte modulo ten, so it has many aliases for
        // each family. Its canonical selectors are checked separately below.
        .filter(|entry| {
            !entry.machine && entry.scenario_bytes >= 2 && entry.name != "handshake/wire"
        })
    {
        let families = (entry.families)();
        let mut drawn: Vec<([u8; 2], String)> = Vec::with_capacity(1 << 16);
        let mut counts: HashMap<String, usize> = HashMap::new();
        for first in 0..=u8::MAX {
            for second in 0..=u8::MAX {
                let scenario = entry.scenario(&[first, second]).to_string();
                *counts.entry(scenario.clone()).or_default() += 1;
                drawn.push(([first, second], scenario));
            }
        }
        let listed = |prefix: &[u8]| {
            families
                .iter()
                .any(|family| prefix.starts_with(&family.tag))
        };
        for (prefix, scenario) in &drawn {
            assert!(
                counts[scenario] > RARE || listed(prefix),
                "{}: the prefix {prefix:02x?} draws a scenario at most {RARE} prefixes \
                 draw, and no family's tag starts it; list its tag in `families`",
                entry.name
            );
        }
        for family in &families {
            assert!(
                drawn.iter().any(|(prefix, scenario)| {
                    prefix.starts_with(&family.tag) && counts[scenario] <= RARE
                }),
                "{}: no prefix family {}'s tag starts draws a rare scenario, so the tag \
                 picks nothing",
                entry.name,
                family.name
            );
        }
    }
}

/// The draws reach every family of `block-sync/storage`, and untagged
/// inputs, within a few times their count, and each tagged input draws its
/// family's scenario. A harness without families draws [`inputs`]' inputs.
#[test]
fn the_draws_reach_each_family() {
    let entry = entry("block-sync/storage");
    let families = (entry.families)();
    assert!(families.len() >= 10, "{families:?}");
    let runs = (families.len() + 1) * 8;
    let mut reached = BTreeSet::new();
    for (family, input) in draws(SEED, 512, families.clone()).take(runs) {
        let family = family.unwrap();
        reached.insert(family);
        if let Some(listed) = families.iter().find(|listed| listed.name == family) {
            assert!(input.starts_with(&listed.tag));
            assert_eq!(entry.scenario(&input), entry.scenario(&listed.tag));
        }
    }
    let mut every: BTreeSet<&str> = families.iter().map(|family| family.name).collect();
    every.insert(UNTAGGED);
    assert_eq!(reached, every, "within {runs} draws");
    let plain: Vec<Vec<u8>> = inputs(SEED, 64).take(32).collect();
    let untagged: Vec<(Option<&str>, Vec<u8>)> = draws(SEED, 64, Vec::new()).take(32).collect();
    assert!(untagged.iter().all(|(family, _)| family.is_none()));
    assert_eq!(
        untagged
            .into_iter()
            .map(|(_, input)| input)
            .collect::<Vec<_>>(),
        plain
    );
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[test]
fn handshake_family_selectors_cover_every_decoder_branch() {
    use zakura_sim::harness::{handshake_wire::Wire, Subsystem};
    let families = Wire::families();
    assert_eq!(families.len(), 10);
    for (index, family) in families.iter().enumerate() {
        assert_eq!(family.tag, [u8::try_from(index).unwrap()]);
        for alias in (index..=255).step_by(10) {
            let mut prefix = [173; Wire::SCENARIO_BYTES];
            prefix[0] = u8::try_from(alias).unwrap();
            let scenario = Wire::draw(&prefix);
            prefix[0] = family.tag[0];
            assert_eq!(scenario, Wire::draw(&prefix));
        }
    }
    let reached: BTreeSet<_> = draws(SEED, 512, families.clone())
        .take(100)
        .filter_map(|(family, _)| family)
        .collect();
    assert!(families.iter().all(|family| reached.contains(family.name)));
    let map = generator_harnesses();
    for harness in ["handshake/wire", "handshake/counts"] {
        assert!(map["handshake/pending-bounded"].contains(&harness));
    }
}
