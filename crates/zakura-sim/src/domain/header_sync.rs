//! Header-sync domain properties (properties.md, Header sync).
//!
//! Each check reads one owner's view-side evidence through a small view
//! trait, so a shared property can take the check over later by
//! registering it against another view.

use crate::{
    harness::header_node::{
        capacity::{BoundaryRead, CapacityEvidence},
        claim::{Claim, ClaimEvidence},
    },
    property::{
        CheckCtx, HasView, Inconclusive, Kind, Property, PropertyId, Source, Status,
        TriggerHistory, Unit, Verdict,
    },
    ObservationLog,
};

/// The header chunk budget never holds more than its capacity, reserved
/// and owned together.
pub const CHUNK_BUDGET_BOUND: &str = "header-sync/chunk-budget-bound";

/// The model's header chunk budget capacity, from properties.md's
/// Header-sync chunk budget. A unit test pins it to production's.
pub const CHUNK_BUDGET_CAPACITY: usize = 4_000;

/// What the header budget checks read: one reactor's probe reads and the
/// claim breaks its tap found.
pub trait BudgetView {
    /// The reactor's probe reads, and how each boundary's read went.
    fn capacity(&self) -> &CapacityEvidence;

    /// The claim rules the reactor's tap found broken.
    fn claims(&self) -> &ClaimEvidence;
}

/// [`CHUNK_BUDGET_BOUND`], as a harness whose view is a [`BudgetView`]
/// registers it.
pub fn chunk_budget_bound<S>() -> Property<S>
where
    S: HasView,
    S::View: BudgetView,
{
    Property {
        id: PropertyId::new(CHUNK_BUDGET_BOUND),
        claim: "the header chunk budget never holds more than its 4,000 headers, reserved and \
                owned together",
        kind: Kind::Bound {
            unit: Unit::Headers,
        },
        assumes: &[],
        needs: &[Source::View],
        status: Status::Active,
        check: check_chunk_budget_bound::<S>,
        witness: Some(TriggerHistory::View(budget_claimed::<S::View>)),
    }
}

/// Checks [`CHUNK_BUDGET_BOUND`] at the boundary after a step.
///
/// It's violated by a probe reading over [`CHUNK_BUDGET_CAPACITY`], or by a
/// claim break the tap found at this step: `BudgetOverCapacity` on a read,
/// or `OverBudget` on a reported reservation. A boundary whose read found
/// the budget's lock held judges nothing. With the reactor stopped
/// mid-poll, as it is inside a release, that's allowed, and a run that ends
/// there is inconclusive. Otherwise no task should hold the lock, so the
/// missing read is missing evidence.
fn check_chunk_budget_bound<S>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S: HasView,
    S::View: BudgetView,
{
    let view = cx.view();
    let step = cx.step;
    let boundary = view.capacity().boundary(step);
    match boundary {
        Some(BoundaryRead::UnreadQuiet) => cx.missing_evidence(format!(
            "the header budget's lock was held at the boundary after step {step}, with the \
             reactor not stopped mid-poll"
        )),
        Some(BoundaryRead::UnreadMidPoll) => cx.unsettled(Inconclusive::UnreadBoundary),
        Some(BoundaryRead::Read) | None => {}
    }
    let records = view
        .claims()
        .at(step, &[Claim::BudgetOverCapacity, Claim::OverBudget]);
    // The tap's `BudgetOverCapacity` judges the read against the probe's
    // capacity, which a wrong production capacity would move.
    let read_judged = records
        .iter()
        .any(|record| record.claim == Claim::BudgetOverCapacity);
    let mut breaks: Vec<String> = records.into_iter().map(|record| record.detail).collect();
    if let Some(reading) = view.capacity().reading_at(step) {
        let claimed = reading.reserved.saturating_add(reading.owned);
        cx.peak(
            "claimed-headers",
            u64::try_from(claimed).unwrap_or(u64::MAX),
        );
        if claimed > CHUNK_BUDGET_CAPACITY && !read_judged {
            breaks.push(format!(
                "the probe reads {} reserved and {} owned",
                reading.reserved, reading.owned
            ));
        }
    }
    let expected = format!("at most {CHUNK_BUDGET_CAPACITY} headers reserved and owned");
    let mut breaks = breaks.into_iter();
    if let Some(first) = breaks.next() {
        for other in breaks {
            cx.record_violation(expected.clone(), other);
        }
        return cx.violated(expected, first);
    }
    match boundary {
        Some(BoundaryRead::Read) => Verdict::Holds,
        _ => Verdict::NotApplicable,
    }
}

/// [`CHUNK_BUDGET_BOUND`]'s trigger history: the probe read the budget
/// holding headers.
fn budget_claimed<V: BudgetView>(view: &V, _: &ObservationLog) -> bool {
    view.capacity()
        .readings()
        .iter()
        .any(|reading| reading.reserved.saturating_add(reading.owned) > 0)
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, rc::Rc};

    use zakura_network::zakura::observe::HEADER_CHUNK_BUDGET_CAPACITY;

    use super::{chunk_budget_bound, BudgetView, CHUNK_BUDGET_CAPACITY};
    use crate::{
        harness::header_node::{
            capacity::{BoundaryRead, CapacityEvidence, CapacityReading},
            claim::ClaimEvidence,
        },
        property::{Assumptions, HasView, Registry, RegistryChecker, Source, Sources},
        ActionSource, Boundary, HarnessFailure, Identity, MemorySink, ObservationLog, Outcome,
        PlanInput, PropertyOutcome, RunReport, Sim,
    };

    /// The model's capacity is production's.
    #[test]
    fn the_model_capacity_is_production_s() {
        assert_eq!(CHUNK_BUDGET_CAPACITY, HEADER_CHUNK_BUDGET_CAPACITY);
    }

    /// A probe's evidence with no tap behind it.
    #[derive(Clone, Default)]
    struct Probe {
        capacity: CapacityEvidence,
        claims: ClaimEvidence,
    }

    impl BudgetView for Probe {
        fn capacity(&self) -> &CapacityEvidence {
            &self.capacity
        }

        fn claims(&self) -> &ClaimEvidence {
            &self.claims
        }
    }

    impl HasView for Probe {
        type View = Probe;

        /// A copy sharing the stores.
        fn view(&self) -> Probe {
            self.clone()
        }
    }

    /// One boundary's scripted read: claimed headers, or unread.
    #[derive(Clone, Copy)]
    enum Scripted {
        Read(usize),
        Unread(BoundaryRead),
    }

    /// Records the next scripted read at each boundary, as a tap would.
    struct Boundaries {
        script: VecDeque<Scripted>,
        capacity: CapacityEvidence,
    }

    impl ActionSource for Boundaries {
        fn enabled(&self, _: zakura_scheduler::SimInstant, _: &mut Vec<(Identity, u64)>) {}

        fn boundaries(&self, _: zakura_scheduler::SimInstant, _: &mut Vec<Boundary>) {}

        fn execute(&mut self, _: u64, _: &mut ObservationLog) {
            unreachable!("the script offers no actions");
        }

        /// Records the next scripted read as the boundary after this step,
        /// from step 1, since the run also observes after setup.
        fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
            let step = log.step();
            if step == 0 {
                return Ok(());
            }
            match self.script.pop_front() {
                Some(Scripted::Read(claimed)) => self.capacity.record(CapacityReading {
                    step,
                    reserved: claimed,
                    owned: 0,
                    locator_permits: 0,
                    compared: false,
                    teardown: false,
                }),
                Some(Scripted::Unread(read)) => self.capacity.unread(step, read),
                None => {}
            }
            Ok(())
        }
    }

    /// Runs `chunk-budget-bound` over one task that yields until `script`
    /// is spent, each step's boundary read as the script says.
    fn run(script: &[Scripted]) -> RunReport {
        let probe = Probe::default();
        let mut sim = Sim::new(crate::RunConfig::default(), PlanInput::Bytes(Vec::new()));
        sim.add_source(Box::new(Boundaries {
            script: script.iter().copied().collect(),
            capacity: probe.capacity.clone(),
        }));
        let steps = script.len();
        sim.scheduler().spawn_root("reactor", async move {
            for _ in 1..steps {
                zakura_rt::task::yield_now().await;
            }
        });
        let registry =
            Registry::new(vec![chunk_budget_bound::<Probe>()]).expect("one property is unique");
        let mut checker = RegistryChecker::new(
            registry,
            Rc::new(probe),
            Assumptions::none(),
            &Sources::observed(&[Source::View]),
        );
        sim.run(&mut checker, &mut MemorySink::default())
    }

    /// Reads within the capacity hold, an unread boundary mid-poll judges
    /// nothing, and a read after it settles the property again.
    #[test]
    fn unread_mid_poll_boundaries_judge_nothing() {
        let report = run(&[
            Scripted::Read(10),
            Scripted::Unread(BoundaryRead::UnreadMidPoll),
            Scripted::Unread(BoundaryRead::UnreadMidPoll),
            Scripted::Read(CHUNK_BUDGET_CAPACITY),
        ]);
        assert_eq!(
            report.properties[0].outcome,
            PropertyOutcome::Held { applicable: 2 }
        );
        assert_eq!(
            report.properties[0].peaks.get("claimed-headers"),
            Some(&4_000)
        );
    }

    /// A run that ends at an unread boundary mid-poll is inconclusive.
    #[test]
    fn a_run_ending_unread_mid_poll_is_inconclusive() {
        let report = run(&[
            Scripted::Read(10),
            Scripted::Unread(BoundaryRead::UnreadMidPoll),
        ]);
        assert_eq!(
            report.properties[0].outcome,
            PropertyOutcome::Inconclusive {
                reason: "unread-boundary".to_owned()
            }
        );
    }

    /// An unread boundary with the reactor not mid-poll is missing
    /// evidence, which fails the run.
    #[test]
    fn an_unread_quiet_boundary_is_missing_evidence() {
        let report = run(&[
            Scripted::Read(10),
            Scripted::Unread(BoundaryRead::UnreadQuiet),
        ]);
        assert!(
            matches!(
                report.end.harness,
                Some(HarnessFailure::MissingEvidence { step: 2, .. })
            ),
            "{:?}",
            report.end
        );
    }

    /// A read over the model's capacity violates the property, even with
    /// no claim break from the tap.
    #[test]
    fn a_read_over_capacity_violates() {
        let report = run(&[
            Scripted::Read(10),
            Scripted::Read(CHUNK_BUDGET_CAPACITY + 1),
        ]);
        assert_eq!(report.end.outcome, Outcome::Violation, "{:?}", report.end);
        let violation = report.end.violation.expect("the run violated");
        assert_eq!(violation.step, 2);
        assert_eq!(
            violation.actual,
            "the probe reads 4001 reserved and 0 owned"
        );
    }
}
