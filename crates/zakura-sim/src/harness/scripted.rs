//! A test-only harness that runs a check over hand-scripted evidence
//! through `record`, so a test can read what the check found from the
//! run's full report beside its trace and from a fresh replay of it (C21).
//!
//! A [`Script`] is a view whose stores a test fills: [`ScriptRun`] builds
//! one, adds a source that keeps the case's evidence at each boundary, and
//! runs one task for the case's steps. [`sidecar`] records a case, reads
//! the full report back and checks a fresh replay sends the same one.

use std::fmt::Debug;

use serde::{de::DeserializeOwned, Serialize};
use zakura_scheduler::SimInstant;

use super::{HarnessEntry, Subsystem};
use crate::{
    end::RunEnd,
    property::{Capture, EndViolation, HasView, Property, Sources},
    record::{FullReport, PropertyFindings},
    ActionSource, Boundary, Identity, ObservationLog, RecoveryOffer, RunConfig, Sim,
};

/// A view a test fills by hand, which [`ScriptRun`] records as a harness.
pub(crate) trait Script: HasView + Clone + Default + 'static {
    /// The harness's name, written to the trace's meta.
    const NAME: &'static str;

    /// What one run scripts, the harness's scenario.
    type Case: Serialize + DeserializeOwned + Clone + Debug;

    /// The properties the run registers, in report order.
    fn properties() -> Vec<Property<ScriptRun<Self>>>;

    /// The sources those properties read.
    fn sources() -> Sources;

    /// Keeps `case`'s evidence for the boundary after `log`'s step in this
    /// view's stores, and logs its observations there, as a tap would.
    fn observe(&self, case: &Self::Case, log: &mut ObservationLog);

    /// How many steps `case`'s one task runs for.
    fn steps(_case: &Self::Case) -> usize {
        1
    }
}

/// [`Script`] `V` as a harness: its view is `V`, and everything else it
/// reports is `V`'s.
pub(crate) struct ScriptRun<V>(V);

impl<V: Script> HasView for ScriptRun<V> {
    type View = V;

    /// The scripted view, sharing its stores.
    fn view(&self) -> V {
        self.0.clone()
    }

    /// `V`'s.
    fn captured(&self) -> Capture {
        self.0.captured()
    }

    /// Hands the end to `V`.
    fn ended(&self, end: &RunEnd) {
        self.0.ended(end);
    }

    /// `V`'s.
    fn end_violations(&self, log: &ObservationLog) -> Vec<EndViolation> {
        self.0.end_violations(log)
    }

    /// `V`'s.
    fn stopping(&self) -> bool {
        self.0.stopping()
    }

    /// `V`'s.
    fn snapshot(&self) -> Option<serde_json::Value> {
        self.0.snapshot()
    }

    /// `V`'s.
    fn stamps_after(&self, after: Option<u64>) -> Option<Vec<u64>> {
        self.0.stamps_after(after)
    }
}

impl<V: Script> Subsystem for ScriptRun<V> {
    const NAME: &'static str = V::NAME;
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = V::Case;
    type Prepared = ();

    /// The default config.
    fn config(_: &V::Case) -> RunConfig {
        RunConfig::default()
    }

    /// Nothing to set up.
    fn prepare(_: &V::Case) {}

    /// A fresh view, the source that keeps the case's evidence in it, and
    /// one task that runs for the case's steps.
    fn build(sim: &mut Sim, case: &V::Case, (): ()) -> Self {
        let view = V::default();
        sim.add_source(Box::new(Boundaries {
            view: view.clone(),
            case: case.clone(),
        }));
        let steps = V::steps(case);
        sim.scheduler().spawn_root("reactor", async move {
            for _ in 1..steps {
                zakura_rt::task::yield_now().await;
            }
        });
        ScriptRun(view)
    }

    /// `V`'s.
    fn properties() -> Vec<Property<Self>> {
        V::properties()
    }

    /// `V`'s.
    fn sources() -> Sources {
        V::sources()
    }

    /// Unused: the cases are built by hand.
    fn draw(_: &[u8]) -> V::Case {
        unreachable!("the cases are built by hand")
    }
}

/// Keeps a case's evidence in its view at each boundary.
struct Boundaries<V: Script> {
    view: V,
    case: V::Case,
}

impl<V: Script> ActionSource for Boundaries<V> {
    fn enabled(&self, _: SimInstant, _: &mut Vec<(Identity, u64)>) {}

    fn boundaries(&self, _: SimInstant, _: &mut Vec<Boundary>) {}

    fn execute(&mut self, _: u64, _: &mut ObservationLog) {
        unreachable!("the script offers no actions");
    }

    /// [`Script::observe`].
    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        self.view.observe(&self.case, log);
        Ok(())
    }

    /// Nothing: it finishes no submitted work.
    fn recovery(&self, _now: SimInstant, _out: &mut RecoveryOffer) {}

    /// Nothing: it has no holds.
    fn enter_drain(&mut self, _log: &mut ObservationLog) {}

    /// None: it holds no submitted work.
    fn holds_work(&self) -> Option<String> {
        None
    }
}

/// Records `case` on [`ScriptRun<V>`] through `record`, under the `Draft`
/// override `draft`, and returns the full report written beside the trace.
/// A fresh `replay_with` of the trace must verify and send the same full
/// report, so what's returned is what the recorded run found.
pub(crate) fn sidecar<V: Script>(case: &V::Case, draft: &[String]) -> FullReport {
    let scenario = serde_json::to_value(case).expect("a case serializes");
    recorded(&super::entry::<ScriptRun<V>>(), &scenario, draft)
}

/// [`sidecar`] for any harness `entry`: records `scenario` under the empty
/// Plan and the `Draft` override `draft`, checks a fresh replay sends the
/// same full report, and returns it.
pub(crate) fn recorded(
    entry: &HarnessEntry,
    scenario: &serde_json::Value,
    draft: &[String],
) -> FullReport {
    let dir = tempfile::tempdir().expect("a temp dir can be created");
    let trace = super::with_draft(draft, || {
        super::record(
            entry,
            super::RecordInput::Plan {
                scenario,
                plan: &[],
            },
            dir.path(),
        )
    })
    .expect("the case records");
    let text = std::fs::read_to_string(dir.path().join(crate::record::FULL_REPORT_FILE))
        .expect("record writes the full report");
    let full: FullReport = serde_json::from_str(&text).expect("the full report reads back");
    let replayed = crate::trace::replay_with(&trace, entry, crate::trace::ReplayOptions::default())
        .expect("the trace replays");
    assert!(replayed.verified(), "{scenario}: {replayed:?}");
    assert_eq!(replayed.report.as_ref(), Some(&full), "{scenario}");
    full
}

/// `property`'s findings in `full`.
pub(crate) fn findings<'f>(full: &'f FullReport, property: &str) -> &'f PropertyFindings {
    full.properties
        .iter()
        .find(|found| found.property == property)
        .unwrap_or_else(|| panic!("the report has {property}: {full:?}"))
}
