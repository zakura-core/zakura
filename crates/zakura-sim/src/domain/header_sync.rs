//! Header-sync domain properties (properties.md, Header sync).
//!
//! Each check reads one owner's view-side evidence through a small view
//! trait, so a shared property can take the check over later by
//! registering it against another view.

use crate::{
    harness::header_node::{
        capacity::{BoundaryRead, CapacityEvidence},
        claim::{Claim, ClaimEvidence, SettlementEvidence},
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

/// A released round returns its reservation and leases once its calls are
/// handled, and node teardown returns every chunk-budget charge and every
/// locator permit.
pub const TEARDOWN_RETURNS_BASELINE: &str = "header-sync/teardown-returns-baseline";

/// The claim rules [`TEARDOWN_RETURNS_BASELINE`] reports: round settlement's,
/// then node settlement's.
const TEARDOWN_CLAIMS: [Claim; 7] = [
    Claim::LeakedReservation,
    Claim::LeakedLease,
    Claim::OpenOwnership,
    Claim::OpenReservation,
    Claim::TeardownReserved,
    Claim::TeardownOwned,
    Claim::TeardownLocatorPermits,
];

/// What the teardown checks read: one reactor's claim breaks and its
/// settlement at each drain.
pub trait TeardownView {
    /// The claim rules the reactor's tap found broken.
    fn claims(&self) -> &ClaimEvidence;

    /// The reactor's released rounds and its own teardown at each drain.
    fn rounds(&self) -> &SettlementEvidence;
}

/// [`TEARDOWN_RETURNS_BASELINE`], as a harness whose view is a
/// [`TeardownView`] registers it.
pub fn teardown_returns_baseline<S>() -> Property<S>
where
    S: HasView,
    S::View: TeardownView,
{
    Property {
        id: PropertyId::new(TEARDOWN_RETURNS_BASELINE),
        claim: "a released header round returns its reservation and leases once its calls are \
                handled, and node teardown returns every chunk-budget charge and every locator \
                permit",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::View],
        status: Status::Active,
        check: check_teardown::<S>,
        witness: Some(TriggerHistory::View(teardown_reached::<S::View>)),
    }
}

/// Checks [`TEARDOWN_RETURNS_BASELINE`] at the boundary after a step, at two
/// levels.
///
/// Round settlement: once the queue released a round, its peer holds no
/// reservation from it, every lease staged for it returns once no call
/// holds it, by its last call's `Handled` at the latest, and every call that
/// held its headers ended that ownership by its `Handled`. The ledger checks
/// those at the events, as `LeakedReservation`, `LeakedLease` and
/// `OpenOwnership`.
///
/// Node settlement: once a reactor the harness stopped is gone, without
/// unwinding, and every call it dropped has its drop receipt, it holds no
/// reservation, ownership or staged lease (`OpenReservation`,
/// `OpenOwnership`, `LeakedLease` in the ledger's teardown check), and the
/// budget holds nothing and every locator permit is free but those storage
/// still holds for pending reads (`TeardownReserved`, `TeardownOwned`,
/// `TeardownLocatorPermits` on the first probe read).
///
/// Any of those breaks the step's drain found violates. It applies once the
/// queue released a round or the node settled. A run that ends with a
/// released round unsettled, or with a stop begun and the node unsettled,
/// is inconclusive. A run that never stops the node is judged at round
/// level alone, so it holds once its released rounds settle: node teardown
/// didn't happen, and today no committed input can stop the node, so every
/// planted round-level case would otherwise be inconclusive.
fn check_teardown<S>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S: HasView,
    S::View: TeardownView,
{
    let view = cx.view();
    let step = cx.step;
    let row = view.rounds().at(step);
    if row.is_some_and(|row| row.unsettled > 0 || (row.shutdown && !row.settled)) {
        cx.unsettled(Inconclusive::TeardownOpen);
    }
    let expected = "a released round, and a stopped node, keeps no reservation, lease, held \
                    headers or locator permit once settled";
    let mut breaks = view.claims().at(step, &TEARDOWN_CLAIMS).into_iter();
    if let Some(first) = breaks.next() {
        for other in breaks {
            cx.record_violation(expected, other.detail);
        }
        return cx.violated(expected, first.detail);
    }
    if row.is_some_and(|row| row.released > 0 || row.settled) {
        Verdict::Holds
    } else {
        Verdict::NotApplicable
    }
}

/// [`TEARDOWN_RETURNS_BASELINE`]'s trigger history: the queue released a
/// round, or a stopped node settled.
fn teardown_reached<V: TeardownView>(view: &V, _: &ObservationLog) -> bool {
    view.rounds().any_released() || view.rounds().settled()
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

    use super::{
        chunk_budget_bound, teardown_returns_baseline, BudgetView, TeardownView,
        CHUNK_BUDGET_CAPACITY,
    };
    use crate::{
        harness::header_node::{
            capacity::{BoundaryRead, CapacityEvidence, CapacityReading},
            claim::{Claim, ClaimEvidence, ClaimRecord, Settlement, SettlementEvidence},
        },
        property::{Assumptions, HasView, Registry, RegistryChecker, Source, Sources},
        property::{Property, Status},
        ActionSource, Boundary, HarnessFailure, Identity, MemorySink, ObservationLog, Outcome,
        PlanInput, PropertyOutcome, RunReport, Sim,
    };

    /// The model's capacity is production's.
    #[test]
    fn the_model_capacity_is_production_s() {
        assert_eq!(CHUNK_BUDGET_CAPACITY, HEADER_CHUNK_BUDGET_CAPACITY);
    }

    /// A probe's and a ledger's evidence with no tap behind it.
    #[derive(Clone, Default)]
    struct Probe {
        capacity: CapacityEvidence,
        claims: ClaimEvidence,
        rounds: SettlementEvidence,
    }

    impl TeardownView for Probe {
        fn claims(&self) -> &ClaimEvidence {
            &self.claims
        }

        fn rounds(&self) -> &SettlementEvidence {
            &self.rounds
        }
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

    /// One boundary's scripted evidence: a read of claimed headers, an
    /// unread boundary, the released rounds, a stop and whether the node
    /// settled, or a claim break.
    #[derive(Clone, Copy)]
    enum Scripted {
        Read(usize),
        Unread(BoundaryRead),
        Settlement { released: u64, unsettled: usize },
        Stop { settled: bool },
        Broke(Claim),
    }

    /// Records the next scripted evidence at each boundary, as a tap would.
    struct Boundaries {
        script: VecDeque<Scripted>,
        probe: Probe,
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
                Some(Scripted::Read(claimed)) => self.probe.capacity.record(CapacityReading {
                    step,
                    reserved: claimed,
                    owned: 0,
                    locator_permits: 0,
                    compared: false,
                    teardown: false,
                }),
                Some(Scripted::Unread(read)) => self.probe.capacity.unread(step, read),
                Some(Scripted::Settlement {
                    released,
                    unsettled,
                }) => self.probe.rounds.record(Settlement {
                    step,
                    released,
                    unsettled,
                    shutdown: false,
                    settled: false,
                }),
                Some(Scripted::Stop { settled }) => self.probe.rounds.record(Settlement {
                    step,
                    released: 0,
                    unsettled: 0,
                    shutdown: true,
                    settled,
                }),
                Some(Scripted::Broke(claim)) => {
                    self.probe.claims.record(ClaimRecord {
                        step,
                        claim,
                        detail: format!("{claim:?}"),
                    });
                    self.probe.rounds.record(Settlement {
                        step,
                        released: 1,
                        unsettled: 0,
                        shutdown: false,
                        settled: false,
                    });
                }
                None => {}
            }
            Ok(())
        }
    }

    /// Runs `chunk-budget-bound` over one task that yields until `script`
    /// is spent, each step's evidence as the script says.
    fn run(script: &[Scripted]) -> RunReport {
        run_property(chunk_budget_bound::<Probe>(), script)
    }

    /// Runs `property` over one task that yields until `script` is spent,
    /// each step's evidence as the script says.
    fn run_property(property: Property<Probe>, script: &[Scripted]) -> RunReport {
        let probe = Probe::default();
        let mut sim = Sim::new(crate::RunConfig::default(), PlanInput::Bytes(Vec::new()));
        sim.add_source(Box::new(Boundaries {
            script: script.iter().copied().collect(),
            probe: probe.clone(),
        }));
        let steps = script.len();
        sim.scheduler().spawn_root("reactor", async move {
            for _ in 1..steps {
                zakura_rt::task::yield_now().await;
            }
        });
        let registry = Registry::new(vec![property]).expect("one property is unique");
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

    /// Released rounds that settle hold, the steps before the first release
    /// don't apply, and the property reports nothing unsettled.
    #[test]
    fn settled_rounds_hold_teardown() {
        let report = run_property(
            teardown_returns_baseline(),
            &[
                Scripted::Settlement {
                    released: 0,
                    unsettled: 0,
                },
                Scripted::Settlement {
                    released: 1,
                    unsettled: 1,
                },
                Scripted::Settlement {
                    released: 1,
                    unsettled: 0,
                },
            ],
        );
        assert_eq!(
            report.properties[0].outcome,
            PropertyOutcome::Held { applicable: 2 }
        );
    }

    /// A run that ends with a released round unsettled is inconclusive.
    #[test]
    fn a_run_ending_with_an_unsettled_round_is_inconclusive() {
        let report = run_property(
            teardown_returns_baseline(),
            &[Scripted::Settlement {
                released: 1,
                unsettled: 1,
            }],
        );
        assert_eq!(
            report.properties[0].outcome,
            PropertyOutcome::Inconclusive {
                reason: "teardown-open".to_owned()
            }
        );
    }

    /// Each round settlement claim violates at the step whose drain found
    /// it, and the budget's claims don't.
    #[test]
    fn round_settlement_claims_violate_teardown() {
        for claim in [
            Claim::LeakedReservation,
            Claim::LeakedLease,
            Claim::OpenOwnership,
        ] {
            let property = Property {
                status: Status::Draft,
                ..teardown_returns_baseline()
            };
            let report = run_property(
                property,
                &[
                    Scripted::Settlement {
                        released: 1,
                        unsettled: 0,
                    },
                    Scripted::Broke(claim),
                ],
            );
            let violations = &report.properties[0].violations;
            assert_eq!(violations.len(), 1, "{claim:?}: {violations:?}");
            assert_eq!(violations[0].step, 2, "{claim:?}");
        }
        let report = run_property(
            teardown_returns_baseline(),
            &[Scripted::Broke(Claim::OverBudget)],
        );
        assert!(report.properties[0].violations.is_empty());
    }

    /// A stop holds the property unsettled until the node settles, and then
    /// it applies and holds. A run cut off before is inconclusive.
    #[test]
    fn node_settlement_settles_teardown() {
        let report = run_property(
            teardown_returns_baseline(),
            &[
                Scripted::Stop { settled: false },
                Scripted::Stop { settled: true },
            ],
        );
        assert_eq!(
            report.properties[0].outcome,
            PropertyOutcome::Held { applicable: 1 }
        );
        let report = run_property(
            teardown_returns_baseline(),
            &[Scripted::Stop { settled: false }],
        );
        assert_eq!(
            report.properties[0].outcome,
            PropertyOutcome::Inconclusive {
                reason: "teardown-open".to_owned()
            }
        );
    }

    /// Each node settlement claim violates at the step whose drain found it.
    #[test]
    fn node_settlement_claims_violate_teardown() {
        for claim in [
            Claim::OpenReservation,
            Claim::TeardownReserved,
            Claim::TeardownOwned,
            Claim::TeardownLocatorPermits,
        ] {
            let property = Property {
                status: Status::Draft,
                ..teardown_returns_baseline()
            };
            let report = run_property(
                property,
                &[Scripted::Stop { settled: false }, Scripted::Broke(claim)],
            );
            let violations = &report.properties[0].violations;
            assert_eq!(violations.len(), 1, "{claim:?}: {violations:?}");
            assert_eq!(violations[0].step, 2, "{claim:?}");
        }
    }
}
