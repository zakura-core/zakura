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
        draws, draws_with, generate, generator_harnesses, inputs, planned_draws, read_exclusions,
        Draw, DrawPolicy, GenerateOptions, GenerateReport, Schedule, Stop, REPORT_FILE, UNTAGGED,
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
        draw_policy: DrawPolicy::Uniform,
        schedule: Schedule::Plan,
        families: BTreeSet::new(),
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
/// family is stale. A tag longer than two bytes starts no such prefix, so
/// the scenario the tag itself draws must be rare instead.
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
            let picks = if family.tag.len() > 2 {
                let scenario = entry.scenario(&family.tag).to_string();
                counts.get(&scenario).is_none_or(|count| *count <= RARE)
            } else {
                drawn.iter().any(|(prefix, scenario)| {
                    prefix.starts_with(&family.tag) && counts[scenario] <= RARE
                })
            };
            assert!(
                picks,
                "{}: family {}'s tag draws no rare scenario, so the tag picks nothing",
                entry.name, family.name
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

/// The cycle draws visit every slot once per block: each family's variants
/// and one untagged run. On `handshake/wire`, whose hello, ack and prelude
/// families pick a fault by the byte after the tag, a block of runs draws
/// every fault of each, which uniform draws of the same length miss. The
/// uniform policy gives [`draws`]' inputs, and the bytes past what a slot
/// writes are [`inputs`]' own.
#[test]
fn the_cycle_draws_visit_every_variant_once_per_block() {
    use zakura_sim::harness::{handshake_wire::Wire, Subsystem};
    let families = Wire::families();
    let slots: usize = families
        .iter()
        .map(|family| usize::from(family.variants))
        .sum::<usize>()
        + 1;
    assert!(
        families.iter().any(|family| family.variants > 1),
        "{families:?}"
    );
    for block in 0..2 {
        let drawn: Vec<(Option<&str>, Vec<u8>)> =
            draws_with(SEED, 512, families.clone(), DrawPolicy::Cycle)
                .skip(block * slots)
                .take(slots)
                .collect();
        let mut seen = BTreeSet::new();
        for (family, input) in &drawn {
            let family = family.unwrap();
            let slot = match families.iter().find(|listed| listed.name == family) {
                Some(listed) => {
                    assert!(input.starts_with(&listed.tag), "{family}: {input:02x?}");
                    let variant = (listed.variants > 1).then(|| input[listed.tag.len()]);
                    if let Some(variant) = variant {
                        assert!(u16::from(variant) < listed.variants, "{family}");
                    }
                    (family, variant)
                }
                None => {
                    assert_eq!(family, UNTAGGED);
                    (family, None)
                }
            };
            assert!(seen.insert(slot), "block {block} draws {slot:?} twice");
        }
        assert_eq!(seen.len(), slots, "block {block}");
    }
    let uniform: Vec<_> = draws_with(SEED, 512, families.clone(), DrawPolicy::Uniform)
        .take(64)
        .collect();
    assert_eq!(
        uniform,
        draws(SEED, 512, families).take(64).collect::<Vec<_>>()
    );
    let plain: Vec<Vec<u8>> = inputs(SEED, 512).take(64).collect();
    let block = Wire::families();
    for ((family, input), plain) in draws_with(SEED, 512, block.clone(), DrawPolicy::Cycle)
        .take(64)
        .zip(&plain)
    {
        let written = block
            .iter()
            .find(|listed| Some(listed.name) == family)
            .map_or(0, |listed| {
                listed.tag.len() + usize::from(listed.variants > 1)
            });
        if plain.len() > written {
            assert_eq!(input[written..], plain[written..]);
        }
    }
}

/// A PCT search stops on the planted race, and the input it saves replays
/// the same violation with no steer: the zero-padded prefix, then the Plan
/// bytes the steered run recorded.
#[test]
fn a_pct_search_saves_an_input_that_replays_unsteered() {
    let root = tempfile::tempdir().unwrap();
    let entry = entry(PLANTED);
    let report = generate(
        entry,
        &GenerateOptions {
            schedule: Schedule::Pct,
            ..options(root.path(), None)
        },
    )
    .unwrap();
    assert_eq!(report.schedule, Schedule::Pct);
    let Stop::Violation { property } = &report.stop else {
        panic!("the PCT search missed the planted race: {report:?}");
    };
    let saved = fs::read(report.input.as_ref().unwrap()).unwrap();
    assert_eq!(
        hex(&Sha256::digest(&saved)),
        report.input_sha256.clone().unwrap()
    );
    let run = (entry.run_bytes)(&saved, &mut zakura_sim::NullSink, None);
    assert_eq!(
        zakura_sim::explore::classify(entry, &run).outcome,
        zakura_sim::explore::ExploreOutcome::Fail {
            property: property.clone()
        }
    );
    // The saved replay isn't the draw, so excluding the reported draw digest
    // skips that run on the same seed.
    let draw = report
        .draw_sha256
        .clone()
        .expect("a steered run's replay differs from its draw");
    let again = generate(
        entry,
        &GenerateOptions {
            schedule: Schedule::Pct,
            exclude: BTreeSet::from([draw]),
            ..options(&root.path().join("again"), None)
        },
    )
    .unwrap();
    assert_eq!(again.skipped, 1, "{again:?}");
    assert_ne!(again.kill_run, report.kill_run, "{again:?}");
}

/// A mixed search takes turns of three: a cycled draw under the Plan, a
/// cycled draw under PCT, and a free draw, under PCT on odd turns. On
/// `discovery/wire` the free draws visit each free family once per block,
/// and every draw past its tag keeps [`inputs`]' bytes.
#[test]
fn the_mixed_draws_take_turns_and_reach_every_free_family() {
    let entry = entry("discovery/wire");
    let families = (entry.families)();
    let free: BTreeSet<&str> = families
        .iter()
        .filter(|family| family.free)
        .map(|family| family.name)
        .collect();
    assert_eq!(free, BTreeSet::from(["composed", "spam"]));
    let drawn: Vec<Draw> = planned_draws(
        SEED,
        512,
        families.clone(),
        entry.untagged_free,
        DrawPolicy::Mix,
    )
    .take(3 * 8)
    .collect();
    let mut reached = BTreeSet::new();
    for (index, draw) in drawn.iter().enumerate() {
        let round = index / 3;
        let expected = match (index % 3, round % 2) {
            (1, _) | (2, 1) => Schedule::Pct,
            _ => Schedule::Plan,
        };
        assert_eq!(draw.schedule, Some(expected), "run {index}");
        if index % 3 == 2 {
            assert!(draw.free, "run {index}: {:?}", draw.family);
            reached.insert(draw.family.unwrap());
        }
        let family = families
            .iter()
            .find(|listed| Some(listed.name) == draw.family);
        if let Some(family) = family {
            assert!(draw.input.starts_with(&family.tag), "run {index}");
            assert_eq!(draw.free, family.free, "run {index}");
        }
    }
    assert_eq!(reached, free);
    // The free policy draws only free families.
    assert!(
        planned_draws(SEED, 512, families, entry.untagged_free, DrawPolicy::Free)
            .take(16)
            .all(|draw| draw.free && draw.schedule.is_none())
    );
}

/// A machine harness's untagged op tape is free, so every draw of a free
/// search on it is, and a harness with neither free families nor a free
/// tape draws no free input.
#[test]
fn a_free_search_is_free_only_where_the_prefix_is() {
    let machine = HARNESSES
        .iter()
        .find(|entry| entry.machine)
        .expect("a machine harness");
    assert!(machine.untagged_free);
    assert!(
        planned_draws(SEED, 512, (machine.families)(), true, DrawPolicy::Free)
            .take(8)
            .all(|draw| draw.free)
    );
    let fixed = entry(CLEAN);
    assert!(!fixed.untagged_free);
    assert!(
        planned_draws(SEED, 64, (fixed.families)(), false, DrawPolicy::Free)
            .take(8)
            .all(|draw| !draw.free)
    );
}

/// A mixed search counts its runs per mode, and runs each PCT turn under
/// PCT on a harness the scheduler runs. The race fixtures draw no free
/// scenario, so no run counts as random and no property has random reach.
#[test]
fn a_mixed_search_counts_its_modes() {
    let root = tempfile::tempdir().unwrap();
    let report = generate(
        entry(CLEAN),
        &GenerateOptions {
            runs: 12,
            draw_policy: DrawPolicy::Mix,
            ..options(root.path(), None)
        },
    )
    .unwrap();
    assert_eq!(report.stop, Stop::Clean, "{report:?}");
    assert_eq!(report.modes.values().sum::<u64>(), 12, "{:?}", report.modes);
    assert_eq!(
        report.modes.get("cycle/pct"),
        Some(&6),
        "{:?}",
        report.modes
    );
    assert_eq!(
        report.modes.get("cycle/plan"),
        Some(&6),
        "{:?}",
        report.modes
    );
    assert_eq!(report.random_runs, 0);
    assert!(report.random_properties.is_empty());
}

/// An enumerating search on the planted race finds the lost update within
/// its depth-2 schedules of one drawn input, and the input it saves replays
/// the violation with no steer. On the fixed fixtures it runs every
/// schedule of each input and moves on.
#[test]
fn an_enumerating_search_finds_the_race_and_exhausts_clean_inputs() {
    let root = tempfile::tempdir().unwrap();
    let entry = entry(PLANTED);
    let report = generate(
        entry,
        &GenerateOptions {
            schedule: Schedule::Enumerate,
            ..options(root.path(), None)
        },
    )
    .unwrap();
    let Stop::Violation { property } = &report.stop else {
        panic!("the enumeration missed the planted race: {report:?}");
    };
    let saved = fs::read(report.input.as_ref().unwrap()).unwrap();
    let run = (entry.run_bytes)(&saved, &mut zakura_sim::NullSink, None);
    assert_eq!(
        zakura_sim::explore::classify(entry, &run).outcome,
        zakura_sim::explore::ExploreOutcome::Fail {
            property: property.clone()
        }
    );
    let clean = generate(
        self::entry(CLEAN),
        &GenerateOptions {
            schedule: Schedule::Enumerate,
            ..options(&root.path().join("clean"), None)
        },
    )
    .unwrap();
    assert_eq!(clean.stop, Stop::Clean, "{clean:?}");
    assert!(clean.exhausted > 0, "{clean:?}");
}

/// A search restricted to one family draws only that family, and a family
/// the harness doesn't list is an error before any run.
#[test]
fn a_family_filter_draws_only_the_named_family() {
    let root = tempfile::tempdir().unwrap();
    let entry = entry("discovery/wire");
    let family = (entry.families)()[0].name;
    let report = generate(
        entry,
        &GenerateOptions {
            runs: 2,
            families: BTreeSet::from([family.to_owned()]),
            draw_policy: DrawPolicy::Cycle,
            ..options(root.path(), None)
        },
    )
    .unwrap();
    assert_eq!(report.executed, 2, "{report:?}");
    assert_eq!(
        report.families.keys().collect::<Vec<_>>(),
        [family],
        "{report:?}"
    );
    let unknown = generate(
        entry,
        &GenerateOptions {
            families: BTreeSet::from(["no-such-family".to_owned()]),
            ..options(&root.path().join("unknown"), None)
        },
    );
    assert!(unknown.is_err());
}

/// A wire harness's older input keeps its scenario: the migration widens
/// the two-byte prefix to the new one and keeps the Plan bytes, and a
/// first byte that is now a free family's tag becomes the default
/// scenario's.
#[test]
fn the_wire_harnesses_migrate_their_two_byte_prefixes() {
    for (name, old, free_tags) in [
        ("discovery/wire", 3, &[0xFD_u8, 0xFC][..]),
        ("sync/wire", 5, &[0xFA][..]),
    ] {
        let entry = entry(name);
        let plan = [9_u8, 8, 7];
        let source = [&[0x11_u8, 0x22][..], &plan].concat();
        let migrated = (entry.migrate)(&source, old).unwrap();
        assert_eq!(migrated.len(), entry.scenario_bytes + plan.len(), "{name}");
        assert_eq!(migrated[..2], [0x11, 0x22], "{name}");
        assert!(
            migrated[2..entry.scenario_bytes]
                .iter()
                .all(|byte| *byte == 0),
            "{name}"
        );
        assert_eq!(migrated[entry.scenario_bytes..], plan, "{name}");
        assert_eq!(
            entry.scenario(&migrated[..entry.scenario_bytes]),
            entry.scenario(&[0x11, 0x22]),
            "{name}"
        );
        for tag in free_tags {
            let source = [&[*tag, 0][..], &plan].concat();
            let migrated = (entry.migrate)(&source, old).unwrap();
            assert_ne!(migrated[0], *tag, "{name}");
            assert_eq!(migrated[entry.scenario_bytes..], plan, "{name}");
        }
        // A prefix with no Plan bytes stays as it was.
        assert_eq!((entry.migrate)(&[0x11], old).unwrap(), vec![0x11], "{name}");
        assert!((entry.migrate)(&source, old + 1).is_err(), "{name}");
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[test]
fn handshake_family_selectors_cover_every_decoder_branch() {
    use zakura_sim::harness::{handshake_wire::Wire, Subsystem};
    // The first byte, modulo ten, selects a one-byte family. The QUIC
    // families take two-byte tags whose first byte, 253 or 254, the decoder
    // reserves, so those bytes alias no one-byte family.
    const RESERVED: [u8; 2] = [253, 254];
    let families = Wire::families();
    assert_eq!(families.len(), 15);
    let (single, quic) = families.split_at(10);
    for family in quic {
        assert_eq!(family.tag.len(), 2);
        assert!(RESERVED.contains(&family.tag[0]), "{}", family.name);
    }
    for (index, family) in single.iter().enumerate() {
        assert_eq!(family.tag, [u8::try_from(index).unwrap()]);
        for alias in (index..=255)
            .step_by(10)
            .filter(|alias| !RESERVED.contains(&u8::try_from(*alias).unwrap()))
        {
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
