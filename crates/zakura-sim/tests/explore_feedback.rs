//! E2a: a collector's features reach the counters, start fresh for each
//! input, and leave the run unchanged. Runs with and without a collector
//! record the same trace. `cargo xtask sim-feedback` checks the other half
//! in a libFuzzer build: libFuzzer keeps an input for its feature alone.
#![cfg(zakura_sim)]

mod support;

use zakura_sim::{
    explore::{
        feedback::{
            probe_collector, probe_feature, Collect, Counters, Feature, FeatureMap, Feedback,
            NewCollector, Probe,
        },
        Worker,
    },
    harness::{self, entry, HarnessEntry, HARNESSES},
    property::CheckEvent,
    Category, MemorySink, NullSink, ObservationLog, PropertyReport, RunEnd, StepCtx,
};

/// The probe's entry, which the harness registry leaves out.
static PROBE: HarnessEntry = entry::<Probe>();

/// Fresh counters for one test. Tests run in parallel, so each gets its
/// own.
fn counters() -> &'static Counters {
    Box::leak(Box::new(Counters::new()))
}

/// A collector that reads every hook's input and hits a feature for each.
struct Everything;

impl Collect for Everything {
    fn after_step(&mut self, cx: &StepCtx<'_>, map: FeatureMap) {
        // `min` keeps the size below 256, so it fits in a `u16`.
        map.hit(Feature::new(cx.enabled.len().min(255) as u16));
        let category = match cx.pick.category {
            Category::Task => 0,
            Category::Deliver => 1,
            Category::Drop => 2,
            Category::Storage => 3,
            Category::Peer => 4,
            Category::Time => 5,
        };
        map.hit(Feature::new(256 + category));
        for _ in cx.observations() {
            map.hit(Feature::new(512));
        }
    }

    fn observe_events(&mut self, events: &[CheckEvent], map: FeatureMap) {
        for event in events {
            let kind = match event {
                CheckEvent::Lifecycle { .. } => 0,
                CheckEvent::Label { .. } => 1,
                _ => 2,
            };
            map.hit(Feature::new(768 + kind));
        }
    }

    fn at_end(&mut self, end: &RunEnd, log: &ObservationLog, map: FeatureMap) {
        map.hit(Feature::new(1024 + u16::from(end.violation.is_some())));
        map.hit(Feature::new(1026 + u16::from(log.entries().is_empty())));
    }
}

/// [`Everything`], boxed.
fn everything() -> Box<dyn Collect> {
    Box::new(Everything)
}

/// What a run recorded and reported: every record, digests included, and
/// every property report.
#[derive(Debug, PartialEq)]
struct Recorded {
    records: Vec<String>,
    properties: Vec<PropertyReport>,
    end: RunEnd,
    picks: Vec<String>,
}

/// Runs `input` on `entry` with every record kept, with `collector`'s
/// features written to `counters` if there is one.
fn recorded(
    entry: &HarnessEntry,
    input: &[u8],
    collector: Option<NewCollector>,
    counters: &'static Counters,
) -> Recorded {
    let mut sink = MemorySink::default();
    let mut collecting = collector.map(|collector| Feedback::new(counters, collector).start());
    let extra = collecting
        .as_mut()
        .map(|collecting| collecting as &mut dyn zakura_sim::Checker);
    let run = (entry.run_bytes)(input, &mut sink, extra);
    let report = run.report().expect("the harness ran the scenario");
    Recorded {
        records: sink.lines(),
        properties: report.properties.clone(),
        end: report.end.clone(),
        picks: support::picks(report)
            .into_iter()
            .map(str::to_owned)
            .collect(),
    }
}

/// The first `race/lost-update` input that finds the planted race.
fn lost_update_failure() -> Vec<u8> {
    let race = harness::find("race/lost-update").unwrap();
    support::byte_inputs(500, 64)
        .into_iter()
        .map(|plan| [&[0, 1][..], &plan].concat())
        .find(|input| {
            let run = (race.run_bytes)(input, &mut NullSink, None);
            run.report().unwrap().end.violation.is_some()
        })
        .expect("some input finds the planted race")
}

#[test]
fn collector_leaves_every_trace_unchanged() {
    let race = harness::find("race/lost-update").unwrap();
    let violation = lost_update_failure();
    let mut runs: Vec<(&HarnessEntry, Vec<u8>)> = vec![(race, violation)];
    for entry in HARNESSES.iter().chain([&PROBE]) {
        let scenario = entry.scenario_bytes;
        runs.push((entry, Vec::new()));
        runs.push((entry, vec![0xff; scenario + 16]));
        // Truncation to a byte is the point: a spread of byte values.
        runs.push((entry, (0..scenario + 32).map(|i| (i * 37) as u8).collect()));
    }
    let counters = counters();
    for (entry, input) in runs {
        let what = format!("{} on {input:?}", entry.name);
        let without = recorded(entry, &input, None, counters);
        let with = recorded(entry, &input, Some(everything), counters);
        assert!(!counters.hits().is_empty(), "{what}: the collector ran");
        assert_eq!(with, without, "{what}");
    }
    let without = recorded(&PROBE, &[7], None, counters);
    let with = recorded(&PROBE, &[7], Some(probe_collector), counters);
    assert_eq!(counters.hits(), [probe_feature(8_000_000)]);
    assert_eq!(with, without, "the probe's own collector");
}

#[test]
fn probe_states_reach_distinct_features() {
    let counters = counters();
    let feedback = Feedback::new(counters, probe_collector);
    let mut worker = Worker::new();
    let mut features = Vec::new();
    for input in [[1], [2], [1]] {
        let picks = worker.explore_with(&PROBE, &input, Some(feedback), |_, run, _| {
            let report = run.report().expect("the probe runs");
            // Each pick, with the time an advance moves to left out.
            support::picks(report)
                .into_iter()
                .map(|pick| pick.split(':').next().unwrap_or(pick).to_owned())
                .collect::<Vec<_>>()
        });
        features.push((counters.hits(), picks));
    }
    // The sleep is the scenario byte plus 1 ms, and the counters hold only
    // the last input's feature.
    assert_eq!(features[0].0, [probe_feature(2_000_000)]);
    assert_eq!(features[1].0, [probe_feature(3_000_000)]);
    assert_eq!(features[2], features[0], "no history carries over");
    // The two states come from the same schedule, but for the time.
    assert_eq!(features[0].1, features[1].1);
}

#[test]
fn counters_saturate_and_clear() {
    let counters = counters();
    let map = FeatureMap::new(counters);
    let feature = Feature::new(u16::MAX);
    for _ in 0..300 {
        map.hit(feature);
    }
    assert_eq!(counters.get(feature), u8::MAX);
    assert_eq!(counters.hits(), [feature]);
    counters.clear();
    assert!(counters.hits().is_empty());
}
