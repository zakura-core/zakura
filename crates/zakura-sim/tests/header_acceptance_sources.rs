//! Missing sources fail each header harness (hdr-14.3, item 6).
//!
//! [`Without`] wraps a real header harness and observes exactly the
//! sources its properties need, less one. Each test runs every header
//! harness once per needed source and requires the run to end at a harness
//! failure naming that source, with no property `Held`. The keys these
//! tests cover come from `header_acceptance::needs`, so a new need on a
//! harness is a key this file must reach.
#![cfg(zakura_sim)]

use std::{cell::RefCell, collections::BTreeSet};

use zakura_sim::{
    domain::{
        header_sync::{
            capacity_release_wakes, watch_observed_under_flood,
            watch_observed_under_lifecycle_flood,
        },
        header_sync_progress::bounded_adversary_progress,
    },
    harness::{
        header_acceptance,
        header_sync::{
            storage_properties, wire_properties, Flood, HeaderWire, ReleaseWake, RepairHarness,
            Smoke, Storage,
        },
        run_harness, HarnessError, Subsystem,
    },
    property::{Capture, EndViolation, HasView, Property, Source, Sources},
    HarnessFailure, MemorySink, ObservationLog, Outcome, Plan, PropertyOutcome, RunConfig, RunEnd,
    RunReport, Sim,
};

thread_local! {
    /// The sources the next [`Without`] run observes.
    static OBSERVED: RefCell<Vec<Source>> = const { RefCell::new(Vec::new()) };
}

/// Every source a harness can observe.
const ALL: [Source; 7] = [
    Source::Frames,
    Source::Closes,
    Source::Datagrams,
    Source::Storage,
    Source::Gauges,
    Source::View,
    Source::Schedule,
];

/// A real header harness `S`, observing exactly [`OBSERVED`], with its own
/// properties registered over this wrapper's view.
struct Without<S>(S);

/// The harness's own properties, typed for [`Without`].
trait Own: Subsystem {
    /// `Self::properties()`, over the wrapper.
    fn own() -> Vec<Property<Without<Self>>>;
}

impl Own for Smoke {
    /// None: the smoke harness has only the step loop's.
    fn own() -> Vec<Property<Without<Self>>> {
        Vec::new()
    }
}

impl Own for Storage {
    /// The storage harness's, then `bounded-adversary-progress`.
    fn own() -> Vec<Property<Without<Self>>> {
        let mut properties = storage_properties();
        properties.push(bounded_adversary_progress());
        properties
    }
}

impl Own for ReleaseWake {
    /// The storage harness's, then `capacity-release-wakes`.
    fn own() -> Vec<Property<Without<Self>>> {
        let mut properties = storage_properties();
        properties.push(capacity_release_wakes());
        properties
    }
}

impl Own for Flood {
    /// The storage harness's, then the two watch rules.
    fn own() -> Vec<Property<Without<Self>>> {
        let mut properties = storage_properties();
        properties.push(watch_observed_under_flood());
        properties.push(watch_observed_under_lifecycle_flood());
        properties
    }
}

impl Own for RepairHarness {
    /// The storage harness's.
    fn own() -> Vec<Property<Without<Self>>> {
        storage_properties()
    }
}

impl Own for HeaderWire {
    /// The wire harness's.
    fn own() -> Vec<Property<Without<Self>>> {
        wire_properties()
    }
}

impl<S: Subsystem> HasView for Without<S> {
    type View = S::View;

    /// `S`'s view.
    fn view(&self) -> S::View {
        self.0.view()
    }

    /// `S`'s capture.
    fn captured(&self) -> Capture {
        self.0.captured()
    }

    /// Hands the end to `S`.
    fn ended(&self, end: &RunEnd) {
        self.0.ended(end);
    }

    /// `S`'s end findings.
    fn end_violations(&self, log: &ObservationLog) -> Vec<EndViolation> {
        self.0.end_violations(log)
    }

    /// `S`'s stop.
    fn stopping(&self) -> bool {
        self.0.stopping()
    }

    /// `S`'s stamps.
    fn stamps_after(&self, after: Option<u64>) -> Option<Vec<u64>> {
        self.0.stamps_after(after)
    }
}

impl<S: Own> Subsystem for Without<S> {
    const NAME: &'static str = S::NAME;
    const GENERATOR: u32 = S::GENERATOR;
    const SCENARIO_BYTES: usize = S::SCENARIO_BYTES;
    type Scenario = S::Scenario;
    type Prepared = S::Prepared;

    /// `S`'s limits.
    fn config(scenario: &S::Scenario) -> RunConfig {
        S::config(scenario)
    }

    /// `S`'s preparation.
    fn prepare(scenario: &S::Scenario) -> S::Prepared {
        S::prepare(scenario)
    }

    /// `S`, built as it is.
    fn build(sim: &mut Sim, scenario: &S::Scenario, prepared: S::Prepared) -> Self {
        Without(S::build(sim, scenario, prepared))
    }

    /// `S`'s own properties.
    fn properties() -> Vec<Property<Self>> {
        S::own()
    }

    /// Exactly [`OBSERVED`].
    fn sources() -> Sources {
        OBSERVED.with(|observed| Sources::observed(&observed.borrow()))
    }

    /// `S`'s draw.
    fn draw(prefix: &[u8]) -> S::Scenario {
        S::draw(prefix)
    }
}

/// The source named `name`, its `Debug` name.
fn source(name: &str) -> Source {
    ALL.into_iter()
        .find(|source| format!("{source:?}") == name)
        .unwrap_or_else(|| panic!("no source {name}"))
}

/// Runs `T` on `scenario` with an empty plan, and returns its report,
/// whether or not it ended at a harness failure.
fn run<T: Subsystem>(scenario: &T::Scenario) -> RunReport {
    match run_harness::<T>(
        scenario,
        Plan::from_fuzz(&[]),
        &mut MemorySink::default(),
        None,
    ) {
        Ok(report) => report,
        Err(HarnessError::Run { report, .. }) => *report,
        Err(error) => panic!("{}: {error}", T::NAME),
    }
}

/// What a property registration is, for comparing the wrapper's with the
/// real harness's: its ID, status and needed sources.
fn registration<T: HasView>(property: &Property<T>) -> (String, String, Vec<String>) {
    (
        property.id.to_string(),
        format!("{:?}", property.status),
        property
            .needs
            .iter()
            .map(|need| format!("{need:?}"))
            .collect(),
    )
}

/// The source a missing-source harness failure names, if `report` ended
/// at one.
fn missing_source(report: &RunReport) -> Option<&str> {
    match &report.end.harness {
        Some(HarnessFailure::MissingSource { source, .. }) => Some(source),
        _ => None,
    }
}

/// One property's registration: its ID, status and needed sources.
type Registration = (String, String, Vec<String>);

/// Every registration `wrapped` and `real` don't share.
fn registration_errors(
    wrapped: &BTreeSet<Registration>,
    real: &BTreeSet<Registration>,
) -> Vec<String> {
    wrapped
        .symmetric_difference(real)
        .map(|registration| format!("{registration:?}"))
        .collect()
}

/// Runs `S` without each source its properties need and requires a harness
/// failure naming it, with no property `Held`. The wrapper must register
/// what `S` registers, with the same statuses and needs, or this would
/// test another harness. The real `S`, run as it is, must not fail on a
/// missing source, so its own sources cover every need.
fn fails_without_each_source<S: Own>() {
    let wrapped: BTreeSet<_> = Without::<S>::properties()
        .iter()
        .map(registration)
        .collect();
    let real: BTreeSet<_> = S::properties().iter().map(registration).collect();
    assert_eq!(
        registration_errors(&wrapped, &real),
        Vec::<String>::new(),
        "{}",
        S::NAME
    );
    // A wrapper one field off the real harness, in an ID, a status or a
    // need, is refused.
    if let Some(first) = real.iter().next().cloned() {
        for changed in [
            (
                format!("{}-other", first.0),
                first.1.clone(),
                first.2.clone(),
            ),
            (
                first.0.clone(),
                format!("{}-other", first.1),
                first.2.clone(),
            ),
            (first.0.clone(), first.1.clone(), vec!["Other".to_owned()]),
        ] {
            let mut off = real.clone();
            off.remove(&first);
            off.insert(changed);
            assert!(!registration_errors(&off, &real).is_empty(), "{}", S::NAME);
        }
    }
    let needed: Vec<Source> = header_acceptance::needs()
        .into_iter()
        .filter(|(harness, _)| harness == S::NAME)
        .map(|(_, name)| source(&name))
        .collect();
    assert!(!needed.is_empty(), "{}", S::NAME);
    let scenario = S::draw(&vec![0; S::SCENARIO_BYTES]);
    let real = run::<S>(&scenario);
    assert_eq!(missing_source(&real), None, "{}", S::NAME);
    // A source the real harness excludes isn't one it observes, so none of
    // its properties may report an exclusion.
    for property in &real.properties {
        assert_eq!(property.excluded, None, "{} {}", S::NAME, property.property);
    }
    for &dropped in &needed {
        let kept: Vec<Source> = needed.iter().copied().filter(|s| *s != dropped).collect();
        OBSERVED.with(|observed| *observed.borrow_mut() = kept);
        let report = run::<Without<S>>(&scenario);
        assert_eq!(
            report.end.outcome,
            Outcome::Harness,
            "{} {dropped:?}",
            S::NAME
        );
        assert_eq!(
            missing_source(&report),
            Some(format!("{dropped:?}").as_str()),
            "{} {dropped:?}",
            S::NAME
        );
        for property in &report.properties {
            assert!(
                !matches!(property.outcome, PropertyOutcome::Held { .. }),
                "{} {dropped:?}: {} held",
                S::NAME,
                property.property
            );
        }
    }
    // With every needed source observed, the wrapper's run doesn't fail on
    // one, so each failure above is the dropped source's.
    OBSERVED.with(|observed| *observed.borrow_mut() = needed.clone());
    assert_eq!(
        missing_source(&run::<Without<S>>(&scenario)),
        None,
        "{}",
        S::NAME
    );
}

#[test]
fn smoke_fails_without_each_source() {
    fails_without_each_source::<Smoke>();
}

#[test]
fn storage_fails_without_each_source() {
    fails_without_each_source::<Storage>();
}

#[test]
fn release_wake_fails_without_each_source() {
    fails_without_each_source::<ReleaseWake>();
}

#[test]
fn wire_fails_without_each_source() {
    fails_without_each_source::<HeaderWire>();
}

#[test]
fn flood_fails_without_each_source() {
    fails_without_each_source::<Flood>();
}

#[test]
fn repair_fails_without_each_source() {
    fails_without_each_source::<RepairHarness>();
}
