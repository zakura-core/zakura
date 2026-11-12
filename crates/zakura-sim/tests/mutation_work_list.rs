//! The mutation loop's work list (mutation plan, mut-7.2) covers every
//! enforced block-sync, block and discovery property exactly once, and
//! each of them has a generator harness or a recorded reason it has none
//! (mut-4.2).
#![cfg(zakura_sim)]

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::PathBuf,
};

use serde::Deserialize;
use zakura_sim::{
    explore::generate::{generator_harnesses, UNTAGGED},
    harness::HARNESSES,
    property::Status,
};

/// The work list, relative to the repository root.
const WORK_LIST: &str = "qa/native-mutations/work-list.toml";

/// The ID prefixes whose `Active` properties the loop must cover. Header
/// sync's are Adam's side, and the step loop's own (`sim/…`) guard no
/// production code.
const IN_SCOPE: &[&str] = &["block-sync/", "block/", "discovery/"];

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkList {
    schema: u32,
    entry: Vec<Entry>,
    #[serde(default)]
    excluded: Vec<Excluded>,
    #[serde(default, rename = "no-generator")]
    no_generator: Vec<NoGenerator>,
    #[serde(default, rename = "drawn-witness")]
    drawn_witness: Vec<DrawnWitness>,
}

/// The generator harness and scenario families that draw a hand-built test
/// witness's scenario, for the `cases.json` case of `patch` and `property`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DrawnWitness {
    patch: String,
    property: String,
    harness: String,
    families: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NoGenerator {
    reason: String,
    properties: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    id: String,
    kind: String,
    claim: String,
    check: Vec<PathBuf>,
    shared: Vec<PathBuf>,
    #[serde(default)]
    table: Option<String>,
    #[serde(default)]
    tables: Vec<TableWitness>,
    #[serde(default)]
    code: Vec<PathBuf>,
    #[serde(default)]
    witnesses: Vec<toml::Value>,
    #[serde(default)]
    instance: Vec<Instance>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Instance {
    property: String,
    code: Vec<PathBuf>,
    witness: toml::Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TableWitness {
    table: String,
    witness: toml::Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Excluded {
    property: String,
    reason: String,
}

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn work_list() -> WorkList {
    let text = fs::read_to_string(root().join(WORK_LIST)).unwrap();
    toml::from_str(&text).unwrap_or_else(|error| panic!("{WORK_LIST}: {error}"))
}

/// Every registered in-scope property, with its status. A property two
/// harnesses register must have one status.
fn registered() -> BTreeMap<String, Status> {
    let mut ids = BTreeMap::new();
    for harness in HARNESSES {
        for property in (harness.properties)() {
            if !IN_SCOPE.iter().any(|scope| property.id.starts_with(scope)) {
                continue;
            }
            if let Some(status) = ids.insert(property.id.clone(), property.status) {
                assert_eq!(status, property.status, "{}", property.id);
            }
        }
    }
    ids
}

/// Every `Active` in-scope ID is in exactly one entry, or is excluded with
/// a reason. An entry covers its instances' IDs, and a protocol table every
/// ID under its prefix. Each protocol table is one entry, and each table
/// the shared wire path runs against is one of them.
#[test]
fn the_work_list_covers_every_active_id_once() {
    let list = work_list();
    assert_eq!(list.schema, 1);
    let registered = registered();
    let mut tables = BTreeMap::new();
    let mut covering: BTreeMap<String, Vec<&str>> = BTreeMap::new();
    for entry in &list.entry {
        assert!(!entry.claim.trim().is_empty(), "{}: no claim", entry.id);
        assert!(!entry.check.is_empty(), "{}: no check", entry.id);
        for path in entry
            .check
            .iter()
            .chain(&entry.shared)
            .chain(&entry.code)
            .chain(entry.instance.iter().flat_map(|instance| &instance.code))
        {
            assert!(
                root().join(path).exists(),
                "{}: {} doesn't exist",
                entry.id,
                path.display()
            );
        }
        match entry.kind.as_str() {
            "shared-definition" | "single" | "machine" => {
                assert!(!entry.instance.is_empty(), "{}: no instance", entry.id);
                assert!(entry.table.is_none() && entry.tables.is_empty());
                for instance in &entry.instance {
                    assert!(instance.witness.is_table(), "{}", instance.property);
                    assert!(
                        registered.contains_key(&instance.property),
                        "{}: {} isn't registered",
                        entry.id,
                        instance.property
                    );
                    covering
                        .entry(instance.property.clone())
                        .or_default()
                        .push(&entry.id);
                }
            }
            "protocol-table" => {
                let table = entry
                    .table
                    .as_ref()
                    .expect("a protocol table names its table");
                assert!(entry.instance.is_empty() && !entry.witnesses.is_empty());
                assert!(
                    tables.insert(table.clone(), &entry.id).is_none(),
                    "{table} is in two entries"
                );
                let prefix = format!("{table}/");
                let ids: Vec<&String> = registered
                    .keys()
                    .filter(|id| id.starts_with(&prefix))
                    .collect();
                assert!(!ids.is_empty(), "{table} has no registered ID");
                for id in ids {
                    covering.entry(id.clone()).or_default().push(&entry.id);
                }
            }
            "shared-wire-path" => {
                assert!(entry.instance.is_empty() && !entry.tables.is_empty());
            }
            kind => panic!("{}: unknown kind {kind}", entry.id),
        }
    }
    for entry in list.entry.iter().filter(|e| e.kind == "shared-wire-path") {
        for TableWitness { table, witness } in &entry.tables {
            assert!(witness.is_table(), "{table}");
            assert!(
                tables.contains_key(table),
                "{}: {table} has no protocol-table entry",
                entry.id
            );
        }
    }
    for excluded in &list.excluded {
        assert!(!excluded.reason.trim().is_empty(), "{}", excluded.property);
        assert!(
            registered.contains_key(&excluded.property),
            "{} isn't registered",
            excluded.property
        );
        covering
            .entry(excluded.property.clone())
            .or_default()
            .push("excluded");
    }
    for (id, status) in &registered {
        let entries = covering.get(id).map(Vec::as_slice).unwrap_or_default();
        if *status == Status::Active {
            assert_eq!(entries.len(), 1, "{id} is in {entries:?}");
        } else {
            assert!(entries.len() <= 1, "{id} is in {entries:?}");
        }
    }
}

/// The `Active` IDs the work list's entries cover: each instance's, and
/// each registered ID under a protocol table's prefix.
fn covered(list: &WorkList, registered: &BTreeMap<String, Status>) -> BTreeSet<String> {
    let mut ids = BTreeSet::new();
    for entry in &list.entry {
        ids.extend(entry.instance.iter().map(|i| i.property.clone()));
        if let Some(table) = &entry.table {
            let prefix = format!("{table}/");
            ids.extend(
                registered
                    .keys()
                    .filter(|id| id.starts_with(&prefix))
                    .cloned(),
            );
        }
    }
    ids.retain(|id| registered.get(id) == Some(&Status::Active));
    ids
}

/// Every `Active` ID the work list covers has a generator harness, from
/// `zakura-sim generators`'s map, or a `[[no-generator]]` record with a
/// reason. Each record names covered `Active` IDs, each once, so a stale or
/// misspelled ID fails here.
#[test]
fn every_covered_id_has_a_generator_or_a_reason() {
    let list = work_list();
    let registered = registered();
    let covered = covered(&list, &registered);
    let map = generator_harnesses();
    let mut recorded = BTreeSet::new();
    for record in &list.no_generator {
        assert!(!record.reason.trim().is_empty(), "a record has no reason");
        assert!(!record.properties.is_empty(), "{}: no ID", record.reason);
        for id in &record.properties {
            assert!(
                covered.contains(id),
                "no-generator names {id}, which isn't a covered Active ID"
            );
            assert!(recorded.insert(id.clone()), "{id} is recorded twice");
        }
    }
    let holes: Vec<&String> = covered
        .iter()
        .filter(|id| map.get(*id).is_none_or(Vec::is_empty) && !recorded.contains(*id))
        .collect();
    assert!(
        holes.is_empty(),
        "no generator harness reports these IDs, and the work list records no reason: {holes:?}"
    );
}

/// The loop's retained cases, relative to the repository root.
const CASES: &str = "qa/native-mutations/cases.json";

/// Panics unless `witness`, a work-list or `cases.json` witness, names
/// something that exists: a `test` names a function in its
/// `crates/zakura-sim/tests/<test_target>.rs`, and a `planted` or `input`
/// names a file.
fn check_witness(owner: &str, witness: &serde_json::Value) {
    let field = |name: &str| witness.get(name).and_then(serde_json::Value::as_str);
    if let Some(test) = field("test") {
        let target = field("test_target")
            .unwrap_or_else(|| panic!("{owner}: test {test} names no test_target"));
        let file = root().join(format!("crates/zakura-sim/tests/{target}.rs"));
        let source = fs::read_to_string(&file)
            .unwrap_or_else(|error| panic!("{owner}: {}: {error}", file.display()));
        assert!(
            source.contains(&format!("fn {test}(")),
            "{owner}: {} has no test {test}",
            file.display()
        );
    } else {
        let path = field("planted")
            .or_else(|| field("input"))
            .unwrap_or_else(|| panic!("{owner}: unknown witness {witness}"));
        assert!(root().join(path).is_file(), "{owner}: {path} doesn't exist");
    }
}

/// Every witness the work list and `cases.json` name exists, so a renamed
/// test or moved input fails here and not as a remote run's
/// `baseline-fails`.
#[test]
fn every_named_witness_exists() {
    let as_json = |value: &toml::Value| serde_json::to_value(value).unwrap();
    for entry in work_list().entry {
        let witnesses = entry
            .instance
            .iter()
            .map(|instance| &instance.witness)
            .chain(entry.tables.iter().map(|table| &table.witness))
            .chain(&entry.witnesses);
        for witness in witnesses {
            check_witness(&entry.id, &as_json(witness));
        }
    }
    let text = fs::read_to_string(root().join(CASES)).unwrap();
    let cases: Vec<serde_json::Value> =
        serde_json::from_str(&text).unwrap_or_else(|error| panic!("{CASES}: {error}"));
    for case in &cases {
        if let Some(witness) = case.get("witness") {
            check_witness(case["patch"].as_str().unwrap_or(CASES), witness);
        }
    }
}

/// Every `cases.json` case whose witness is a hand-built test has a
/// `[[drawn-witness]]` record, or its property a `[[no-generator]]` one
/// (mutation plan, mut-6). A record's harness reports the property as a
/// generator harness, and lists each family the record names, so the
/// generator can draw the test's scenario. Each record names a test case,
/// once.
#[test]
fn every_test_witness_has_a_drawn_family_or_a_reason() {
    let list = work_list();
    let map = generator_harnesses();
    let unreached: BTreeSet<&str> = list
        .no_generator
        .iter()
        .flat_map(|record| record.properties.iter().map(String::as_str))
        .collect();
    let mut records: BTreeMap<(&str, &str), &DrawnWitness> = BTreeMap::new();
    for record in &list.drawn_witness {
        let key = (record.patch.as_str(), record.property.as_str());
        assert!(
            records.insert(key, record).is_none(),
            "{key:?} is recorded twice"
        );
        assert!(
            map.get(&record.property)
                .is_some_and(|harnesses| harnesses.contains(&record.harness.as_str())),
            "{key:?}: {} isn't a generator harness of the property",
            record.harness
        );
        let entry = HARNESSES
            .iter()
            .find(|entry| entry.name == record.harness)
            .unwrap();
        let families = (entry.families)();
        assert!(!record.families.is_empty(), "{key:?}: no family");
        for family in &record.families {
            assert!(
                family == UNTAGGED || families.iter().any(|listed| listed.name == family),
                "{key:?}: {} lists no family {family}",
                record.harness
            );
        }
    }
    let text = fs::read_to_string(root().join(CASES)).unwrap();
    let cases: Vec<serde_json::Value> = serde_json::from_str(&text).unwrap();
    let mut tested = BTreeSet::new();
    for case in &cases {
        if case.pointer("/witness/test").is_none() {
            continue;
        }
        let key = (
            case["patch"].as_str().unwrap(),
            case["property"].as_str().unwrap(),
        );
        tested.insert(key);
        assert!(
            records.contains_key(&key) || unreached.contains(key.1),
            "{key:?}: a test witness with no drawn family and no no-generator reason"
        );
    }
    for key in records.keys() {
        assert!(tested.contains(key), "{key:?} names no test-witness case");
    }
}
