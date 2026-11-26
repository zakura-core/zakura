//! Fuzz target readiness: every target `qa/suite.toml` declares resolves
//! to its registered harness and has curated seeds or a built corpus that
//! apply the harness's own properties, and `zakura-sim targets` gives each target
//! one status (explorer implementation plan, E5a, acceptance test 1).
//!
//! The build and the fuzzer run need the pinned nightly and cargo-fuzz, so
//! the nightly `zakura_sim targets` job runs them. These tests check the
//! committed tables and seeds, and the status each run's evidence gets.
#![cfg(zakura_sim)]

mod support;

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use zakura_sim::{
    campaign::{
        applied_properties, bypasses, declared_targets, judge_run, max_len, own_applicability,
        resolve, TargetStatus, FUZZ_PACKAGE, SHARED_PROPERTIES,
    },
    explore::{
        stats::{Counts, ProcessStats, STATS_VERSION},
        ProcessExit, Worker,
    },
    harness::{
        fixtures::{All, Variant, Which},
        HarnessEntry, Subsystem, HARNESSES,
    },
    seed::{self, Seed},
    suite::{SuiteFile, SUITE_FILE},
};

/// The repository's root.
fn root() -> PathBuf {
    seed::repo_root()
}

/// Runs `input` on `entry`'s harness on `worker`, and returns the
/// explorer's outcome and the properties whose evaluation applied, as the
/// fuzz target's stats count them.
fn run_seed(
    worker: &mut Worker,
    entry: &'static HarnessEntry,
    input: &[u8],
) -> (String, BTreeSet<String>) {
    worker.explore(entry, input, move |_, run, classified| {
        let mut counts = Counts::for_harness(entry);
        counts.record(run, classified, &[]);
        let outcome = counts
            .outcomes
            .keys()
            .next()
            .cloned()
            .unwrap_or_else(|| "none".to_owned());
        (outcome, applied_properties(&counts))
    })
}

/// `qa/suite.toml`.
fn suite() -> SuiteFile {
    toml::from_str(&fs::read_to_string(root().join(SUITE_FILE)).unwrap()).unwrap()
}

/// The simulator fuzz package's manifest.
fn manifest() -> String {
    fs::read_to_string(root().join(FUZZ_PACKAGE).join("Cargo.toml")).unwrap()
}

/// Each target's own properties that no input of [`inputs`] applies. These
/// inputs run honest and single-fault scenarios, and the coverage manifest names
/// the test where each listed property applies, such as a frame-conformance
/// property on a faulted frame. A target missing from the list has inputs
/// that apply every own property. A listed property that an input now
/// applies fails the test until it leaves the list, so the list only
/// shrinks.
///
/// `memory/consensus-once-per-height` applies only at rest with a live
/// `Consensus` lease, and every `sync/composed` input rests with none.
///
/// `tx-relay/stalled-peers-parked` applies only after a parked peer's
/// readmission, three virtual minutes in. That witness runs too long for a
/// fuzz input, and `tests/tx_relay_wire.rs` applies the property.
const UNAPPLIED: &[(&str, &[&str])] = &[
    (
        "block_sync_storage",
        &[
            "block-sync/s6v3/blocks-done/over-owed-closes",
            "block-sync/s6v3/blocks-done/receiver-work-authorized",
            "block-sync/s6v3/range-unavailable/over-owed-closes",
            "block-sync/s6v3/range-unavailable/receiver-work-authorized",
            "block-sync/s6v3/range-unavailable/sender-ends-once",
            "block-sync/s6v3/range-unavailable/sender-within-response-quantity",
        ],
    ),
    (
        "discovery_composed",
        &[
            "discovery/late-response-admitted",
            "discovery/s4v2/get-peers/bad-content-closes",
            "discovery/s4v2/get-peers/cadence-never-closes",
            "discovery/s4v2/get-peers/long-payload-closes",
            "discovery/s4v2/get-peers/over-margin-closes",
            "discovery/s4v2/get-peers/reserved-flags-closes",
            "discovery/s4v2/get-peers/short-payload-closes",
            "discovery/s4v2/get-services/bad-content-closes",
            "discovery/s4v2/get-services/cadence-never-closes",
            "discovery/s4v2/get-services/long-payload-closes",
            "discovery/s4v2/get-services/over-margin-closes",
            "discovery/s4v2/get-services/reserved-flags-closes",
            "discovery/s4v2/get-services/short-payload-closes",
            "discovery/s4v2/hello/bad-content-closes",
            "discovery/s4v2/hello/cadence-never-closes",
            "discovery/s4v2/hello/long-payload-closes",
            "discovery/s4v2/hello/reserved-flags-closes",
            "discovery/s4v2/hello/short-payload-closes",
            "discovery/s4v2/peers/bad-content-closes",
            "discovery/s4v2/peers/long-payload-closes",
            "discovery/s4v2/peers/over-owed-closes",
            "discovery/s4v2/peers/reserved-flags-closes",
            "discovery/s4v2/peers/sender-ends-once",
            "discovery/s4v2/peers/sender-within-owed",
            "discovery/s4v2/peers/short-payload-closes",
            "discovery/s4v2/peers/unsolicited-closes",
            "discovery/s4v2/services/bad-content-closes",
            "discovery/s4v2/services/long-payload-closes",
            "discovery/s4v2/services/over-owed-closes",
            "discovery/s4v2/services/reserved-flags-closes",
            "discovery/s4v2/services/sender-ends-once",
            "discovery/s4v2/services/sender-within-owed",
            "discovery/s4v2/services/short-payload-closes",
            "discovery/s4v2/services/unsolicited-closes",
            "discovery/s4v2/undeclared-type-closes",
        ],
    ),
    (
        "discovery_spam",
        &[
            "discovery/late-response-admitted",
            "discovery/s4v2/get-peers/bad-content-closes",
            "discovery/s4v2/get-peers/cadence-never-closes",
            "discovery/s4v2/get-peers/long-payload-closes",
            "discovery/s4v2/get-peers/over-margin-closes",
            "discovery/s4v2/get-peers/reserved-flags-closes",
            "discovery/s4v2/get-peers/short-payload-closes",
            "discovery/s4v2/get-services/bad-content-closes",
            "discovery/s4v2/get-services/cadence-never-closes",
            "discovery/s4v2/get-services/long-payload-closes",
            "discovery/s4v2/get-services/over-margin-closes",
            "discovery/s4v2/get-services/reserved-flags-closes",
            "discovery/s4v2/get-services/short-payload-closes",
            "discovery/s4v2/hello/bad-content-closes",
            "discovery/s4v2/hello/cadence-never-closes",
            "discovery/s4v2/hello/long-payload-closes",
            "discovery/s4v2/hello/reserved-flags-closes",
            "discovery/s4v2/hello/short-payload-closes",
            "discovery/s4v2/peers/bad-content-closes",
            "discovery/s4v2/peers/long-payload-closes",
            "discovery/s4v2/peers/over-owed-closes",
            "discovery/s4v2/peers/reserved-flags-closes",
            "discovery/s4v2/peers/sender-ends-once",
            "discovery/s4v2/peers/sender-within-owed",
            "discovery/s4v2/peers/short-payload-closes",
            "discovery/s4v2/peers/unsolicited-closes",
            "discovery/s4v2/services/bad-content-closes",
            "discovery/s4v2/services/long-payload-closes",
            "discovery/s4v2/services/over-owed-closes",
            "discovery/s4v2/services/reserved-flags-closes",
            "discovery/s4v2/services/sender-ends-once",
            "discovery/s4v2/services/sender-within-owed",
            "discovery/s4v2/services/short-payload-closes",
            "discovery/s4v2/services/unsolicited-closes",
            "discovery/s4v2/undeclared-type-closes",
        ],
    ),
    (
        "discovery_wire",
        &[
            "discovery/late-response-admitted",
            "discovery/s4v2/services/over-owed-closes",
        ],
    ),
    (
        "handshake_wire",
        &[
            "handshake/ack/bad-frame-limit-refused",
            "handshake/ack/bad-idle-limit-refused",
            "handshake/ack/bad-magic-refused",
            "handshake/ack/bad-message-limit-refused",
            "handshake/ack/bad-nonce-echo-refused",
            "handshake/ack/bad-protocol-refused",
            "handshake/ack/bad-queue-limit-refused",
            "handshake/ack/bad-stream-limit-refused",
            "handshake/ack/bad-version-refused",
            "handshake/ack/limits-above-request-refused",
            "handshake/ack/oversize-refused",
            "handshake/ack/refusal-releases-slot",
            "handshake/ack/unoffered-capability-refused",
            "handshake/hello/bad-frame-limit-refused",
            "handshake/hello/bad-idle-limit-refused",
            "handshake/hello/bad-magic-refused",
            "handshake/hello/bad-message-limit-refused",
            "handshake/hello/bad-path-refused",
            "handshake/hello/bad-protocol-refused",
            "handshake/hello/bad-queue-limit-refused",
            "handshake/hello/bad-role-refused",
            "handshake/hello/bad-stream-limit-refused",
            "handshake/hello/bad-version-refused",
            "handshake/hello/identity-mismatch-refused",
            "handshake/hello/missing-capability-refused",
            "handshake/hello/nonzero-initiator-nonce-refused",
            "handshake/hello/nonzero-native-transcript-refused",
            "handshake/hello/nonzero-responder-nonce-refused",
            "handshake/hello/oversize-refused",
            "handshake/hello/refusal-releases-slot",
            "handshake/hello/wrong-chain-refused",
            "handshake/hello/wrong-network-refused",
            "handshake/prelude/bad-magic-resets",
            "handshake/prelude/ineligible-opener-closes",
            "handshake/prelude/request-without-id-closes",
            "handshake/prelude/undersized-cap-closes",
            "handshake/prelude/unexpected-request-id-closes",
            "handshake/prelude/unknown-kind-resets",
            "handshake/prelude/unnegotiated-capability-resets",
            "handshake/prelude/unselected-version-resets",
            "session/capacity-conserved",
            "session/renegotiation-covers-inflight-handshakes",
            "session/setup-slot-bounded",
        ],
    ),
    (
        "header_sync_storage",
        &["header-sync/bounded-adversary-progress"],
    ),
    (
        "header_sync_wire",
        &[
            "header-sync/expected-chain-selected",
            "header-sync/s5v9/headers-outcome/bad-content-closes",
            "header-sync/s5v9/headers-outcome/long-payload-closes",
            "header-sync/s5v9/headers-outcome/over-owed-closes",
            "header-sync/s5v9/headers-outcome/reserved-flags-closes",
            "header-sync/s5v9/headers-outcome/short-payload-closes",
            "header-sync/s5v9/headers/long-payload-closes",
            "header-sync/s5v9/headers/over-owed-closes",
            "header-sync/s5v9/headers/reserved-flags-closes",
            "header-sync/s5v9/headers/short-payload-closes",
            "header-sync/s5v9/status/bad-content-closes",
            "header-sync/s5v9/status/cadence-never-closes",
            "header-sync/s5v9/subscribe-headers/bad-content-closes",
            "header-sync/s5v9/subscribe-headers/long-payload-closes",
            "header-sync/s5v9/subscribe-headers/reserved-flags-closes",
            "header-sync/s5v9/subscribe-headers/short-payload-closes",
            "header-sync/state-publication-consistent",
        ],
    ),
    (
        "sync_composed",
        &[
            "block-sync/frontier-work-consistent",
            "memory/consensus-once-per-height",
            "session/connection-shares-bound",
            "session/credit-isolated",
            "session/replacement-preserves-authorization",
            "session/send-isolated",
        ],
    ),
    ("sync_wire", &["block-sync/frontier-work-consistent"]),
    (
        "tx_relay_wire",
        &[
            "tx-relay/s10v1/get-txs/long-payload-closes",
            "tx-relay/s10v1/get-txs/over-margin-closes",
            "tx-relay/s10v1/get-txs/reserved-flags-closes",
            "tx-relay/s10v1/get-txs/short-payload-closes",
            "tx-relay/s10v1/hello/cadence-never-closes",
            "tx-relay/s10v1/hello/long-payload-closes",
            "tx-relay/s10v1/hello/reserved-flags-closes",
            "tx-relay/s10v1/hello/short-payload-closes",
            "tx-relay/s10v1/not-found/bad-content-closes",
            "tx-relay/s10v1/not-found/long-payload-closes",
            "tx-relay/s10v1/not-found/over-owed-closes",
            "tx-relay/s10v1/not-found/reserved-flags-closes",
            "tx-relay/s10v1/not-found/short-payload-closes",
            "tx-relay/s10v1/undeclared-type-closes",
            "tx-relay/s9v1/have/long-payload-closes",
            "tx-relay/s9v1/have/over-rate-closes",
            "tx-relay/s9v1/have/reserved-flags-closes",
            "tx-relay/s9v1/have/short-payload-closes",
            "tx-relay/s9v1/tx/long-payload-closes",
            "tx-relay/s9v1/tx/over-owed-closes",
            "tx-relay/s9v1/tx/reserved-flags-closes",
            "tx-relay/s9v1/tx/short-payload-closes",
            "tx-relay/s9v1/tx/unsolicited-closes",
            "tx-relay/s9v1/undeclared-type-closes",
            "tx-relay/stalled-peers-parked",
        ],
    ),
];

/// The target's own properties that [`UNAPPLIED`] lists, sorted.
fn unapplied_allowed(target: &str) -> Vec<String> {
    let mut allowed: Vec<String> = UNAPPLIED
        .iter()
        .find(|(name, _)| *name == target)
        .map(|(_, properties)| properties.iter().map(|id| (*id).to_owned()).collect())
        .unwrap_or_default();
    allowed.sort();
    allowed
}

/// The scenario families whose runs take minutes, as `harness/family`, which
/// [`inputs`] leaves out.
const SLOW_FAMILIES: [&str; 3] = [
    "block-sync/storage/stuck-floor",
    "block-sync/storage/above-window",
    "block-sync/storage/decoded-input-stall",
];

/// How many generated op tapes [`inputs`] runs on a machine, which has no
/// scenario families.
const PLANS_WITHOUT_FAMILIES: usize = 8;

/// The longest generated Plan [`inputs`] runs, cut to a target's
/// `-max_len`.
const PLAN_BYTES: usize = 256;

/// `fixtures/all`'s prefix that picks `fixture`'s fixed variant on one
/// worker, under the empty Plan. The default scenario runs one fixture, so
/// a target covering every race fixture runs each one's fix.
fn fixed_race(fixture: Which) -> Vec<u8> {
    let index = |found: Option<usize>| {
        // Six fixtures and five variants fit a byte.
        u8::try_from(found.expect("every fixture and variant is listed")).expect("it fits a byte")
    };
    vec![
        index(Which::ALL.iter().position(|which| *which == fixture)),
        index(
            Variant::ALL
                .iter()
                .position(|which| *which == Variant::Fixed),
        ),
        0,
    ]
}

/// `entry`'s inputs, named: the empty input, which runs the default scenario
/// under the empty Plan, then each scenario family's tag under the empty
/// Plan, but the [`SLOW_FAMILIES`], then the harness's built corpus
/// ([`HarnessEntry::corpus`]). A machine runs its written op tapes
/// ([`support::machine_tapes`]) and the first [`PLANS_WITHOUT_FAMILIES`]
/// generated ones, each at most
/// `max_len` bytes, and `fixtures/all` runs each race fixture's fix
/// ([`fixed_race`]). The harnesses' seeds live in the QA store, which a
/// repository test doesn't read.
fn inputs(entry: &HarnessEntry, max_len: usize) -> Vec<(String, Vec<u8>)> {
    let mut inputs = vec![(format!("{} default", entry.name), Vec::new())];
    let families = (entry.families)();
    if entry.machine {
        for (name, tape) in support::machine_tapes::tapes(entry.name) {
            inputs.push((format!("{} {name}", entry.name), tape));
        }
        let plans = support::byte_inputs(PLANS_WITHOUT_FAMILIES, max_len.min(PLAN_BYTES));
        for (index, plan) in plans.into_iter().enumerate() {
            inputs.push((format!("{} plan {index}", entry.name), plan));
        }
    }
    if entry.name == All::NAME {
        for fixture in Which::ALL {
            inputs.push((
                format!("{} {fixture:?} fixed", entry.name),
                fixed_race(fixture),
            ));
        }
    }
    for family in families {
        let name = format!("{}/{}", entry.name, family.name);
        if !SLOW_FAMILIES.contains(&name.as_str()) {
            inputs.push((name, family.tag));
        }
    }
    for (index, input) in (entry.corpus)().into_iter().enumerate() {
        inputs.push((format!("{} corpus {index}", entry.name), input));
    }
    inputs
}

/// Each declared target resolves to its registered harness, and its
/// harness's default scenario and scenario families ([`inputs`]) apply each
/// of the harness's own properties and those of every harness the target
/// covers, except the properties [`UNAPPLIED`] lists. A target whose inputs
/// reach only the properties every harness checks fails here. The
/// joint-run droplet's `cargo xtask sim-targets` runs the store's seeds.
#[test]
fn every_declared_target_runs_inputs_that_apply_its_properties() {
    let suite = suite();
    let manifest = manifest();
    let declared = declared_targets(&suite);
    assert!(!declared.is_empty(), "{SUITE_FILE} declares sim targets");
    let mut mismatched = Vec::new();
    // Targets whose inputs apply none of their own properties.
    let mut inapplicable = Vec::new();
    for target in &declared {
        let entry = resolve(&manifest, &target.name).unwrap();
        let inputs = inputs(entry, max_len(&target.args).unwrap_or(PLAN_BYTES));
        if let Some(max) = max_len(&target.args) {
            for (name, input) in &inputs {
                assert!(
                    input.len() <= max,
                    "{}: -max_len={max} cuts {name} ({} bytes)",
                    target.name,
                    input.len()
                );
            }
        }
        let mut worker = Worker::new();
        let reached: BTreeSet<String> = inputs
            .iter()
            .flat_map(|(name, input)| {
                let (outcome, applied) = run_seed(&mut worker, entry, input);
                assert_eq!(outcome, "clean", "{name}");
                applied
            })
            .collect();
        let (applied, mut unapplied) = own_applicability(entry, &reached);
        if applied.is_empty() {
            inapplicable.push(target.name.clone());
        }
        unapplied.sort();
        if unapplied != unapplied_allowed(&target.name) {
            mismatched.push(format!("(\"{}\", &{unapplied:?}),", target.name));
        }
        let row = suite
            .target
            .iter()
            .find(|row| row.name == target.name)
            .unwrap();
        for covered in &row.covers {
            let covered = HARNESSES
                .iter()
                .find(|entry| entry.name == covered)
                .unwrap();
            let (_, unapplied) = own_applicability(covered, &reached);
            assert!(
                unapplied.is_empty(),
                "{} covers {}, and no input applies {unapplied:?}",
                target.name,
                covered.name
            );
        }
    }
    assert!(
        inapplicable.is_empty(),
        "these targets' inputs apply none of their own properties: {inapplicable:?}"
    );
    assert!(
        mismatched.is_empty(),
        "the inputs' unapplied own properties differ from UNAPPLIED; these entries match the inputs:\n{}",
        mismatched.join("\n")
    );
}

/// A target the fuzz package doesn't map, or maps to an unregistered
/// harness, doesn't resolve.
#[test]
fn unmapped_targets_dont_resolve() {
    let mapped = "[package.metadata.zakura-sim.targets]\nknown = \"discovery/wait-complete\"\n\
                  ghost = \"no/such-harness\"\n";
    assert_eq!(
        resolve(mapped, "known").unwrap().name,
        "discovery/wait-complete"
    );
    assert!(resolve(mapped, "ghost")
        .unwrap_err()
        .contains("no harness registers"));
    assert!(resolve(mapped, "missing")
        .unwrap_err()
        .contains("maps `missing` to no harness"));
}

/// Only the simulator fuzz package's rows are declared targets.
#[test]
fn declared_targets_are_the_sim_packages() {
    let suite = suite();
    let declared: Vec<String> = declared_targets(&suite)
        .into_iter()
        .map(|target| target.name)
        .collect();
    let sim: Vec<String> = suite
        .target
        .iter()
        .filter(|target| target.package == "sim")
        .map(|target| target.name.clone())
        .collect();
    assert_eq!(declared, sim);
    assert!(!declared.contains(&"fork_transitions".to_owned()));
}

/// A store's seed case is a bypass of the seeds it names, a typed or search
/// case of none, and a case that names none or more than one fails.
#[test]
fn bypasses_follow_their_seed() {
    let store = tempfile::tempdir().unwrap();
    let write = |path: &str, text: &str| {
        let path = store.path().join("qa/planted").join(path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, text).unwrap();
    };
    write(
        "discovery/wait-complete-wakes/lost-wakeup.toml",
        "seed = \"wait\"\n",
    );
    write("header-sync/storage-bound/other.toml", "seed = \"other\"\n");
    write(
        "header-sync/storage-bound/typed.toml",
        "[typed]\nharness = \"header-sync/storage\"\nscenario = \"s.json\"\n",
    );
    write(
        "header-sync/storage-bound/search.toml",
        "[search]\nharness = \"header-sync/storage\"\nseed = 1\nruns = 8\n",
    );
    let seed = |name: &str| Seed::new(name, "discovery/wait-complete", "s.rs", Vec::new());
    let found = bypasses(store.path(), &[seed("wait")]).unwrap();
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(found[0].property, "discovery/wait-complete-wakes");
    assert_eq!(
        found[0].patch,
        Path::new("qa/planted/discovery/wait-complete-wakes/lost-wakeup.patch")
    );
    assert_eq!(
        bypasses(store.path(), &[seed("other"), seed("wait")])
            .unwrap()
            .len(),
        2
    );
    assert!(bypasses(store.path(), &[]).unwrap().is_empty());
    write("header-sync/storage-bound/bad.toml", "checks = []\n");
    assert!(bypasses(store.path(), &[seed("wait")])
        .unwrap_err()
        .contains("this names 0"));
    let empty = tempfile::tempdir().unwrap();
    assert!(bypasses(empty.path(), &[seed("wait")]).unwrap().is_empty());
}

/// The harness's own properties leave out the ones every harness checks.
#[test]
fn shared_properties_never_count_as_applied() {
    let entry = HARNESSES
        .iter()
        .find(|entry| entry.name == "discovery/wait-complete")
        .unwrap();
    let reached = SHARED_PROPERTIES
        .iter()
        .map(|id| (*id).to_owned())
        .collect();
    let (applied, unapplied) = own_applicability(entry, &reached);
    assert_eq!(applied, Vec::<String>::new());
    assert_eq!(unapplied, ["discovery/wait-complete-wakes"]);
}

/// A target's stats file counts a property as applied when it held with an
/// applicable evaluation or was violated, not when it only ended not
/// applicable.
#[test]
fn applied_properties_come_from_the_targets_counts() {
    let entry = HARNESSES
        .iter()
        .find(|entry| entry.name == "discovery/wait-complete")
        .unwrap();
    let mut counts = Counts::for_harness(entry);
    let held = counts
        .properties
        .get_mut("discovery/wait-complete-wakes")
        .unwrap();
    held.held = 1;
    held.evaluations = 2;
    let ids: Vec<String> = counts.properties.keys().cloned().collect();
    let violated = ids
        .iter()
        .find(|id| id.as_str() != "discovery/wait-complete-wakes")
        .unwrap()
        .clone();
    counts.properties.get_mut(&violated).unwrap().violated = 1;
    let not_applicable = ids
        .iter()
        .find(|id| ![violated.as_str(), "discovery/wait-complete-wakes"].contains(&id.as_str()))
        .unwrap();
    counts
        .properties
        .get_mut(not_applicable)
        .unwrap()
        .not_applicable
        .insert("never-applied".to_owned(), 1);
    assert_eq!(
        applied_properties(&counts),
        BTreeSet::from(["discovery/wait-complete-wakes".to_owned(), violated])
    );
}

/// A stats file of `harness` with `runs`, complete or not.
fn stats(harness: &str, runs: u64, complete: bool) -> Result<ProcessStats, String> {
    Ok(ProcessStats {
        version: STATS_VERSION,
        harness: harness.to_owned(),
        pid: 1,
        started_unix: 0,
        written_unix: 0,
        complete,
        counts: Counts {
            runs,
            ..Counts::default()
        },
    })
}

/// libFuzzer's output for a run of `seeds`.
fn executed(seeds: &[(PathBuf, usize)]) -> Vec<u8> {
    seeds
        .iter()
        .map(|(seed, _)| format!("Running: {0}\nExecuted {0} in 1 ms\n", seed.display()))
        .collect::<String>()
        .into_bytes()
}

/// Each piece of evidence that falls short gets its own status, and only a
/// clean run with every seed counted and an own property applied is ready.
#[test]
fn each_shortfall_gets_its_own_status() {
    const HARNESS: &str = "discovery/wait-complete";
    let seeds = [(PathBuf::from("/s/a"), 10), (PathBuf::from("/s/b"), 4096)];
    let log = executed(&seeds);
    let applied = ["discovery/wait-complete-wakes".to_owned()];
    let ok = ProcessExit::Code(0);
    let judge = |exit, log: &[u8], files: &[Result<ProcessStats, String>], applied: &[String]| {
        judge_run(HARNESS, &seeds, Some(4096), exit, log, files, applied).0
    };

    // libFuzzer runs a list of files without the empty input, so a run of
    // each seed once is ready.
    assert_eq!(
        judge(ok, &log, &[stats(HARNESS, 2, true)], &applied),
        TargetStatus::Ready
    );
    // A leak check runs a seed twice.
    assert_eq!(
        judge(ok, &log, &[stats(HARNESS, 4, true)], &applied),
        TargetStatus::Ready
    );
    assert_eq!(
        judge(ok, &log, &[stats(HARNESS, 3, true)], &[]),
        TargetStatus::NoApplicableProperty
    );
    for exit in [
        ProcessExit::Code(1),
        ProcessExit::Signal(6),
        ProcessExit::TimedOut,
    ] {
        assert_eq!(
            judge(exit, &log, &[stats(HARNESS, 3, false)], &applied),
            TargetStatus::SeedsFailed,
            "{exit:?}"
        );
    }
    let mut sanitized = log.clone();
    sanitized.extend_from_slice(b"==1==ERROR: AddressSanitizer: heap-use-after-free\n");
    assert_eq!(
        judge(ok, &sanitized, &[stats(HARNESS, 3, true)], &applied),
        TargetStatus::SeedsFailed
    );
    assert_eq!(
        judge(ok, &log, &[stats("header-sync/storage", 3, true)], &applied),
        TargetStatus::WrongHarness
    );
    for files in [
        vec![],
        vec![stats(HARNESS, 3, true), stats(HARNESS, 3, true)],
        vec![Err("parsing stats-1.json: bad".to_owned())],
        vec![stats(HARNESS, 3, false)],
        vec![stats(HARNESS, 1, true)],
    ] {
        assert_eq!(
            judge(ok, &log, &files, &applied),
            TargetStatus::Unaccounted,
            "{files:?}"
        );
    }
    let one_seed = executed(&seeds[..1]);
    assert_eq!(
        judge(ok, &one_seed, &[stats(HARNESS, 3, true)], &applied),
        TargetStatus::Unaccounted
    );
}

/// The suite row's variables can't change where the target writes its
/// stats or the simulator's artifacts, or set the filter or the
/// abandonment limit, which would let a failing seed pass.
#[test]
fn row_variables_dont_override_the_checks() {
    use zakura_sim::{
        campaign::{smoke_command, DeclaredTarget},
        explore::{stats::STATS_VAR, ABANDON_LIMIT_VAR, EXPECT_VAR},
        trace::ARTIFACTS_VAR,
    };
    let target = DeclaredTarget {
        name: "discovery_wait_complete".to_owned(),
        args: Vec::new(),
        env: vec![
            format!("{STATS_VAR}=/elsewhere"),
            format!("{ARTIFACTS_VAR}=/elsewhere"),
            format!("{EXPECT_VAR}=discovery/wait-complete-wakes"),
            format!("{ABANDON_LIMIT_VAR}=1000"),
            "ASAN_OPTIONS=detect_leaks=0".to_owned(),
        ],
    };
    let dir = Path::new("/out/discovery_wait_complete");
    let command = smoke_command(
        Path::new("/out/fuzzer"),
        &target,
        &[PathBuf::from("/out/seed")],
        dir,
        std::time::Duration::from_secs(60),
    );
    let envs: std::collections::BTreeMap<_, _> = command
        .get_envs()
        .map(|(name, value)| (name.to_owned(), value.map(ToOwned::to_owned)))
        .collect();
    let get = |name: &str| envs.get(std::ffi::OsStr::new(name)).cloned();
    assert_eq!(get(STATS_VAR), Some(Some(dir.join("stats").into())));
    assert_eq!(get(ARTIFACTS_VAR), Some(Some(dir.join("sim").into())));
    assert_eq!(get(EXPECT_VAR), Some(None));
    assert_eq!(get(ABANDON_LIMIT_VAR), Some(None));
    assert_eq!(get("ASAN_OPTIONS"), Some(Some("detect_leaks=0".into())));
}

/// libFuzzer cuts an input longer than `-max_len`, even one it runs as a
/// file, so a seed longer than the row's `-max_len` never runs whole.
#[test]
fn a_seed_past_max_len_is_unaccounted() {
    const HARNESS: &str = "discovery/wait-complete";
    let args = |args: &[&str]| args.iter().map(|arg| (*arg).to_owned()).collect::<Vec<_>>();
    assert_eq!(max_len(&args(&[])), None);
    assert_eq!(max_len(&args(&["-max_len=4096"])), Some(4096));
    assert_eq!(
        max_len(&args(&["-max_len=64", "-max_len=4096"])),
        Some(4096)
    );
    assert_eq!(max_len(&args(&["-max_len=4096", "-max_len=0"])), None);
    assert_eq!(max_len(&args(&["-rss_limit_mb=4096"])), None);

    let seeds = [(PathBuf::from("/s/a"), 10), (PathBuf::from("/s/b"), 4097)];
    let log = executed(&seeds);
    let applied = ["discovery/wait-complete-wakes".to_owned()];
    let judge = |max| {
        judge_run(
            HARNESS,
            &seeds,
            max,
            ProcessExit::Code(0),
            &log,
            &[stats(HARNESS, 2, true)],
            &applied,
        )
    };
    let (status, detail) = judge(Some(4096));
    assert_eq!(status, TargetStatus::Unaccounted);
    assert!(detail.unwrap().contains("/s/b"));
    assert_eq!(judge(Some(4097)).0, TargetStatus::Ready);
    assert_eq!(judge(None).0, TargetStatus::Ready);
}

/// A checkout holding a copy of this repository's suite and fuzz manifest.
fn checkout(dir: &Path) -> PathBuf {
    let repo = dir.join("repo");
    for file in [
        PathBuf::from(SUITE_FILE),
        Path::new(FUZZ_PACKAGE).join("Cargo.toml"),
    ] {
        fs::create_dir_all(repo.join(&file).parent().unwrap()).unwrap();
        fs::copy(root().join(&file), repo.join(&file)).unwrap();
    }
    repo
}

/// The report lists each harness no target runs with its
/// `[[missing-target]]` row, so it accounts for every harness, even when
/// it checks only some targets. No missing target counts as not ready.
#[test]
fn the_report_lists_every_missing_target() {
    use zakura_sim::campaign::{check_targets, Readiness, TargetsOptions, READINESS_FILE};
    let dir = tempfile::tempdir().unwrap();
    let repo = checkout(dir.path());
    // The copied manifest maps the checked target to no harness, so its row
    // is unresolved and the check builds nothing.
    let manifest = repo.join(FUZZ_PACKAGE).join("Cargo.toml");
    let mapped = fs::read_to_string(&manifest).unwrap().replacen(
        "discovery_wait_complete = \"discovery/wait-complete\"",
        "discovery_wait_complete = \"no/such-harness\"",
        1,
    );
    fs::write(&manifest, mapped).unwrap();
    let options = TargetsOptions {
        repo,
        out: dir.path().join("out"),
        targets: vec!["discovery_wait_complete".to_owned()],
        sanitizer: "none".to_owned(),
        target_dir: dir.path().join("target"),
        child: std::time::Duration::from_secs(1),
    };
    let report = check_targets(&options, |_| {}).unwrap();
    let missing = suite().missing_target;
    let excluded = suite().excluded_harness;
    assert!(
        !excluded.is_empty(),
        "{SUITE_FILE} lists harnesses no target runs"
    );
    assert_eq!(report.missing_targets, missing);
    assert_eq!(report.excluded_harnesses, excluded);
    // The one checked target is unresolved, so it's the only row not ready.
    let not_ready: Vec<&str> = report.not_ready().map(|row| row.target.as_str()).collect();
    assert_eq!(not_ready, ["discovery_wait_complete"]);
    let written: Readiness =
        serde_json::from_slice(&fs::read(options.out.join(READINESS_FILE)).unwrap()).unwrap();
    assert_eq!(written, report);
    let json: serde_json::Value =
        serde_json::from_slice(&fs::read(options.out.join(READINESS_FILE)).unwrap()).unwrap();
    assert_eq!(
        json["excluded_harnesses"][0]["harness"],
        excluded[0].harness.as_str()
    );
}
