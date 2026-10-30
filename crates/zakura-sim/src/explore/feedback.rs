//! Semantic feedback: the features a run reaches, written as counters that
//! libFuzzer reads beside its code edges (explorer implementation plan,
//! E2a).
//!
//! libFuzzer keeps an input that reaches a feature no earlier input
//! reached. Its own features are code edges. A collector adds features for
//! abstract protocol states, so an input that reaches a new state through
//! code earlier inputs already covered still joins the corpus.
//!
//! The backend is libFuzzer's extra counters: one byte per feature in the
//! `__libfuzzer_extra_counters` section, which the fuzz package defines.
//! libFuzzer clears the section before each input, then reads each nonzero
//! byte as a feature, bucketed by its hit count like an edge counter. A
//! feature is an index, so features never collide, and an index means the
//! same feature in every process.
//!
//! A collector implements [`Collect`]. It reads each step, the registry's
//! events and the run's end, and writes features to a [`FeatureMap`]. It
//! can't fail, end or change a run: [`Collecting`] runs it as a checker that
//! reports nothing. The counters are `std` atomics, not `zakura-rt`'s, so a
//! write adds no scheduler point.
//!
//! [`Probe`] is a harness whose runs differ only in data, and
//! [`probe_collector`] turns that data into a feature. The fuzz package's
//! `feedback_probe` target runs the pair, and `cargo xtask sim-feedback`
//! shows that libFuzzer keeps an input for its feature alone.

use std::{
    sync::atomic::{AtomicU8, Ordering::Relaxed},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use zakura_rt::time::sleep;
use zakura_scheduler::SchedulerConfig;

use crate::{
    check::{Checker, PropertyReport, StepCtx, Violation},
    end::RunEnd,
    harness::Subsystem,
    property::{CheckEvent, HasView},
    Observation, ObservationLog, RunConfig, Sim,
};

/// The feedback backend, for campaign manifests.
pub const FEEDBACK_BACKEND: &str = "libfuzzer-extra-counters";

/// The feedback version, for campaign manifests. Bump it when a feature's
/// index changes meaning, since a corpus kept for one feature would then
/// count for another, or when the feature set changes.
///
/// - 1: [`Probe`]'s features (E2a).
/// - 2: adds the protocol-state features at [`super::state`]'s index layout
///   (E2b).
pub const FEEDBACK_VERSION: u32 = 2;

/// How many features the counters hold: every [`Feature`] index.
pub const FEATURES: usize = 1 << 16;

/// The first of the 256 features [`Probe`] uses, at the top of the index
/// space.
// `FEATURES` is 2^16, so `FEATURES - 256` fits in a `u16`.
const PROBE_FIRST: u16 = (FEATURES - 256) as u16;

/// One feature: an index into the counters.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Feature(u16);

impl Feature {
    /// The feature at `index`.
    pub const fn new(index: u16) -> Self {
        Feature(index)
    }

    /// Its index.
    pub const fn index(self) -> u16 {
        self.0
    }
}

/// One counter per feature. The fuzz package places one in libFuzzer's
/// section. libFuzzer clears the section a word at a time, so the counters
/// are word-aligned.
#[repr(C, align(64))]
pub struct Counters([AtomicU8; FEATURES]);

impl Counters {
    /// Counters that are all zero.
    pub const fn new() -> Self {
        Counters([const { AtomicU8::new(0) }; FEATURES])
    }

    /// Sets every counter to zero.
    pub fn clear(&self) {
        for counter in &self.0 {
            counter.store(0, Relaxed);
        }
    }

    /// `feature`'s count, saturated at 255.
    pub fn get(&self, feature: Feature) -> u8 {
        self.0[usize::from(feature.0)].load(Relaxed)
    }

    /// Every feature with a nonzero count, in index order.
    pub fn hits(&self) -> Vec<Feature> {
        (0..=u16::MAX)
            .map(Feature)
            .filter(|feature| self.get(*feature) > 0)
            .collect()
    }
}

impl Default for Counters {
    /// [`Counters::new`].
    fn default() -> Self {
        Counters::new()
    }
}

/// Where a collector writes the features a run reaches.
#[derive(Clone, Copy)]
pub struct FeatureMap {
    /// The counters.
    counters: &'static Counters,
}

impl FeatureMap {
    /// A map over `counters`.
    pub fn new(counters: &'static Counters) -> Self {
        FeatureMap { counters }
    }

    /// Counts one hit of `feature`, saturating at 255. The run's thread is
    /// the only writer, so a load and a store suffice.
    pub fn hit(&self, feature: Feature) {
        let counter = &self.counters.0[usize::from(feature.0)];
        counter.store(counter.load(Relaxed).saturating_add(1), Relaxed);
    }
}

/// Reads a run and writes the features it reaches. It can't fail, end or
/// change the run, since [`Collecting`] reports nothing for it.
pub trait Collect {
    /// Reads one step.
    fn after_step(&mut self, cx: &StepCtx<'_>, map: FeatureMap);

    /// Reads the events the registry emitted before this step, or before
    /// the run's end.
    fn observe_events(&mut self, _events: &[CheckEvent], _map: FeatureMap) {}

    /// Reads the run's end and its complete log.
    fn at_end(&mut self, _end: &RunEnd, _log: &ObservationLog, _map: FeatureMap) {}
}

/// Makes one input's collector.
pub type NewCollector = fn() -> Box<dyn Collect>;

/// A collector and the counters it writes, for a worker's inputs.
#[derive(Clone, Copy)]
pub struct Feedback {
    /// The counters.
    map: FeatureMap,
    /// Makes each input's collector.
    collector: NewCollector,
}

impl Feedback {
    /// Feedback that writes `collector`'s features to `counters`.
    pub fn new(counters: &'static Counters, collector: NewCollector) -> Self {
        Feedback {
            map: FeatureMap::new(counters),
            collector,
        }
    }

    /// Clears the counters and makes a fresh collector, so no feature or
    /// history carries over from an earlier input.
    pub fn start(&self) -> Collecting {
        self.map.counters.clear();
        Collecting {
            map: self.map,
            collect: (self.collector)(),
        }
    }
}

/// A collector run as a checker: it passes each hook on, and reports no
/// violation, property or harness failure.
pub struct Collecting {
    /// The counters.
    map: FeatureMap,
    /// The collector.
    collect: Box<dyn Collect>,
}

impl Checker for Collecting {
    /// Passes the step on. Never a violation.
    fn after_step(&mut self, cx: &StepCtx<'_>) -> Option<Violation> {
        self.collect.after_step(cx, self.map);
        None
    }

    /// Passes the end on. No reports.
    fn at_end(&mut self, end: &RunEnd, log: &ObservationLog) -> Vec<PropertyReport> {
        self.collect.at_end(end, log, self.map);
        Vec::new()
    }

    /// Passes the events on.
    fn observe_events(&mut self, events: &[CheckEvent]) {
        self.collect.observe_events(events, self.map);
    }
}

/// [`Probe`]'s scenario: its task sleeps `delay_ms` + 1 ms.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProbeScenario {
    /// The sleep, less 1 ms.
    pub delay_ms: u8,
}

/// A harness whose runs differ only in data. Its one task sleeps 1 to
/// 256 ms, as its scenario byte says, then exits. Every scenario takes the
/// same code path, so any two reach the same code edges. They end at
/// different virtual times, which [`probe_collector`] turns into
/// different features.
///
/// It checks only the step loop's properties, and no input fails it. The
/// harness registry leaves it out, since it checks the feedback transport
/// and searches for nothing.
pub struct Probe;

impl HasView for Probe {
    type View = ();

    /// Nothing.
    fn view(&self) {}
}

impl Subsystem for Probe {
    const NAME: &'static str = "explore/feedback-probe";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 1;
    type Scenario = ProbeScenario;
    type Prepared = ();

    /// One worker and a budget well past the sleep.
    fn config(_: &ProbeScenario) -> RunConfig {
        RunConfig {
            scheduler: SchedulerConfig {
                workers: 1,
                ..SchedulerConfig::default()
            },
            budget: 100,
            drain_bound: 100,
            ..RunConfig::default()
        }
    }

    /// Nothing to set up.
    fn prepare(_: &ProbeScenario) {}

    /// Spawns the sleeping task.
    fn build(sim: &mut Sim, scenario: &ProbeScenario, (): ()) -> Self {
        let sleep_for = Duration::from_millis(u64::from(scenario.delay_ms) + 1);
        sim.scheduler().spawn_root("probe", async move {
            sleep(sleep_for).await;
        });
        Probe
    }

    /// The prefix's byte, as is.
    fn draw(prefix: &[u8]) -> ProbeScenario {
        ProbeScenario {
            delay_ms: prefix[0],
        }
    }
}

/// [`Probe`]'s collector: each time the clock moves, the feature for the
/// new time's whole milliseconds, modulo 256.
pub fn probe_collector() -> Box<dyn Collect> {
    Box::new(ProbeCollector)
}

/// The feature [`probe_collector`] hits when the clock moves to `to_ns`.
pub fn probe_feature(to_ns: u64) -> Feature {
    // Truncation keeps the index in the probe's 256 features with no
    // branch, so every time takes the same code path.
    Feature(PROBE_FIRST + u16::from((to_ns / 1_000_000) as u8))
}

/// [`probe_collector`].
struct ProbeCollector;

impl Collect for ProbeCollector {
    /// Hits the feature for each time the step moved the clock to.
    fn after_step(&mut self, cx: &StepCtx<'_>, map: FeatureMap) {
        for observation in cx.observations() {
            if let Observation::TimeAdvanced { to_ns, .. } = observation {
                map.hit(probe_feature(*to_ns));
            }
        }
    }
}
