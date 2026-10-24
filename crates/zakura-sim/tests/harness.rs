//! The harness registry: every harness has a unique name and rebuilds from
//! its serialized scenario.
#![cfg(zakura_sim)]

use std::collections::BTreeSet;

use serde_json::Value;
use zakura_sim::harness::{self, HarnessEntry, HarnessError, HARNESSES};

/// Every scenario prefix of `entry` worth trying: each first byte, with each
/// second byte's low bit, cut to the harness's length.
fn prefixes(entry: &HarnessEntry) -> BTreeSet<Vec<u8>> {
    (0..=u8::MAX)
        .flat_map(|first| [[first, 0], [first, 1]])
        .map(|bytes| {
            let mut prefix = bytes.to_vec();
            prefix.resize(entry.scenario_bytes, 0);
            prefix
        })
        .collect()
}

#[test]
fn harness_names_are_unique_and_found() {
    let names: BTreeSet<_> = HARNESSES.iter().map(|entry| entry.name).collect();
    assert_eq!(names.len(), HARNESSES.len());
    for entry in HARNESSES {
        assert_eq!(harness::find(entry.name).unwrap().name, entry.name);
    }
    assert!(harness::find("no/such-harness").is_err());
}

#[test]
fn every_harness_round_trips_its_scenario() {
    for entry in HARNESSES {
        for prefix in prefixes(entry) {
            let scenario = (entry.draw)(&prefix);
            let text = serde_json::to_string(&scenario).unwrap();
            let back: Value = serde_json::from_str(&text).unwrap();
            let config = (entry.config)(&scenario).unwrap();
            assert_eq!(
                (entry.config)(&back).unwrap(),
                config,
                "{}: {text}",
                entry.name
            );
            let meta = (entry.meta)(&back).unwrap();
            assert_eq!(meta.scenario, scenario, "{}", entry.name);
            assert_eq!(meta.sim, entry.name);
            assert_eq!(meta.generator, entry.generator);
            assert_eq!(meta.scheduler.workers, config.scheduler.workers);
            assert_eq!(meta.budget.budget, config.budget);
        }
    }
}

#[test]
fn a_scenario_that_does_not_fit_is_refused() {
    let entry = harness::find("header-sync/smoke").unwrap();
    for bad in [
        serde_json::json!({ "extra": 1 }),
        serde_json::json!("smoke"),
    ] {
        assert!(matches!((entry.meta)(&bad), Err(HarnessError::Scenario(_))));
        assert!(matches!(
            (entry.config)(&bad),
            Err(HarnessError::Scenario(_))
        ));
    }
}
