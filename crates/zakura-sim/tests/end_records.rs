//! The end-record hook (mutation plan, mut-7.3): with
//! `ZAKURA_SIM_END_RECORDS` set, each harness run writes one end record and
//! each witness judgement one witness record; unset, nothing is written.
//!
//! The file holds one test, because it sets a process-wide variable.
#![cfg(zakura_sim)]

use std::{env, fs, path::Path};

use serde_json::Value;
use zakura_sim::{
    domain::discovery::WAIT_COMPLETE_WAKES,
    harness::{discovery::WaitComplete, run_harness},
    trace::end_records::END_RECORDS_VAR,
    witness::{witness, Expect, WitnessScenario},
    MemorySink, Plan,
};

/// The committed input that reaches discovery's lost-wakeup race
/// (`tests/witness.rs` runs it).
const INPUT: &[u8] = include_bytes!("fixtures/discovery-wait-complete.bin");

/// Each record file in `dir`, parsed, in the order the process wrote them.
fn records(dir: &Path) -> Vec<Value> {
    let mut names: Vec<_> = fs::read_dir(dir)
        .map(|entries| entries.map(|entry| entry.unwrap().path()).collect())
        .unwrap_or_default();
    names.sort();
    names
        .iter()
        .map(|path| serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap())
        .collect()
}

#[test]
fn the_hook_writes_one_record_per_run_and_nothing_when_unset() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("records");
    let run = || {
        run_harness::<WaitComplete>(
            &Default::default(),
            Plan::from_fuzz(INPUT),
            &mut MemorySink::default(),
            None,
        )
        .unwrap()
    };

    env::remove_var(END_RECORDS_VAR);
    run();
    assert!(!dir.exists(), "an unset variable writes nothing");

    env::set_var(END_RECORDS_VAR, &dir);
    run();
    run();
    let ends = records(&dir);
    assert_eq!(ends.len(), 2, "one end record per run: {ends:?}");
    for (seq, record) in ends.iter().enumerate() {
        assert_eq!(record["seq"], seq);
        let reports = record["end"]["properties"].as_array().unwrap();
        let report = reports
            .iter()
            .find(|report| report["property"] == WAIT_COMPLETE_WAKES)
            .unwrap();
        assert_eq!(report["outcome"]["held"]["applicable"], 1);
        assert!(record["source"].is_string());
    }

    // A witness judgement that loses its trigger history writes its run's
    // end record, then the judgement.
    let lost = witness(
        WitnessScenario::<WaitComplete> {
            scenario: Default::default(),
            input: Plan::from_fuzz(&[]),
        },
        WAIT_COMPLETE_WAKES,
        Expect::Holds,
    );
    assert!(lost.is_err());
    let all = records(&dir);
    assert_eq!(all.len(), 4, "{all:?}");
    assert!(all[2].get("end").is_some());
    let judged = &all[3]["witness"];
    assert_eq!(judged["property"], WAIT_COMPLETE_WAKES);
    assert_eq!(judged["expect"], "holds");
    assert_eq!(judged["error"], "lost-trigger-history");
    assert_eq!(judged["trigger_history"], "lost");
    assert_eq!(all[2]["thread"], all[3]["thread"]);

    env::remove_var(END_RECORDS_VAR);
    run();
    assert_eq!(records(&dir).len(), 4, "unset again, nothing more");
}
