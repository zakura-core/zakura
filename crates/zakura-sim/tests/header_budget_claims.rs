//! hdr-6a: the header budget's claim rules reach
//! `header-sync/chunk-budget-bound` instead of failing the run.
//!
//! No committed input can reach `OverBudget`: the storage grammar's few
//! short chains never claim near 4,000 headers. So this run reports the
//! reservations by hand, through a tap's capacity handle, and checks that
//! the property reports the break at the step whose drain found it, with no
//! harness failure.
#![cfg(zakura_sim)]

use std::{rc::Rc, sync::Arc};

use zakura_network::zakura::{
    observe::{ReactorEvent, ReservationPurpose},
    ZakuraPeerId,
};
use zakura_sim::{
    domain::header_sync::{chunk_budget_bound, BudgetView, CHUNK_BUDGET_BOUND},
    harness::{
        header_node::{
            capacity::CapacityEvidence,
            claim::{Claim, ClaimEvidence},
            tap::ReactorTap,
            Effects, Policy, PolicySource,
        },
        header_sync::storage_config,
    },
    property::{Assumptions, HasView, Registry, RegistryChecker, Source, Sources},
    MemorySink, Outcome, PlanInput, PropertyOutcome, Sim,
};
use zakura_storage::header::{controlled::ControlledPort, fixture::HeaderFixture};

/// The tap's claim breaks, with no probe reads.
#[derive(Clone, Default)]
struct Claims {
    capacity: CapacityEvidence,
    claims: ClaimEvidence,
}

impl BudgetView for Claims {
    /// No reads.
    fn capacity(&self) -> &CapacityEvidence {
        &self.capacity
    }

    /// The tap's claim breaks.
    fn claims(&self) -> &ClaimEvidence {
        &self.claims
    }
}

impl HasView for Claims {
    type View = Claims;

    /// A copy sharing the stores.
    fn view(&self) -> Claims {
        self.clone()
    }
}

/// A peer ID from `byte`.
fn peer(byte: u8) -> ZakuraPeerId {
    ZakuraPeerId::new(vec![byte; 32]).expect("32 bytes is a valid peer ID")
}

/// An ordinary reservation within the fair share, for a peer with none,
/// that takes the budget's claims past 4,000 breaks `OverBudget`. The tap
/// records it, the run goes on, and `chunk-budget-bound` reports it at the
/// step whose drain found it.
#[test]
fn an_over_budget_reservation_violates_chunk_budget_bound() {
    let mut sim = Sim::new(storage_config(), PlanInput::Bytes(Vec::new()));
    let port = Arc::new(ControlledPort::new(&HeaderFixture::regtest()));
    let (observer, tap) = ReactorTap::new(&mut sim, &port, "node");
    let view = Claims {
        claims: tap.claims(),
        ..Claims::default()
    };
    let source =
        PolicySource::new(&mut sim, port, Policy::converging(), Effects::default()).with_tap(tap);
    sim.add_source(Box::new(source));
    sim.scheduler().spawn_root("reactor", async move {
        let capacity = observer
            .capacity_observer()
            .expect("the tap hands the queue its own capacity handle");
        capacity.observe(ReactorEvent::ReservationOpened {
            reservation: 1,
            peer: peer(1),
            purpose: ReservationPurpose::Repair,
            headers: 3_990,
            request: None,
        });
        capacity.observe(ReactorEvent::ReservationOpened {
            reservation: 2,
            peer: peer(2),
            purpose: ReservationPurpose::Ordinary,
            headers: 11,
            request: None,
        });
        // The reactor stays alive, so its open reservations aren't a
        // teardown failure.
        std::mem::forget(observer);
    });
    let registry =
        Registry::new(vec![chunk_budget_bound::<Claims>()]).expect("one property has a unique ID");
    let mut checker = RegistryChecker::new(
        registry,
        Rc::new(view.clone()),
        Assumptions::none(),
        &Sources::observed(&[Source::View]),
    );
    let report = sim.run(&mut checker, &mut MemorySink::default());

    assert_eq!(report.end.harness, None, "{:?}", report.end);
    assert_eq!(report.end.outcome, Outcome::Violation, "{:?}", report.end);
    let violation = report.end.violation.as_ref().expect("the run violated");
    assert_eq!(violation.property, CHUNK_BUDGET_BOUND);
    assert!(
        violation
            .actual
            .starts_with("OverBudget: reservation 2 holds 11 headers"),
        "{violation:?}"
    );
    let records = view.claims.records();
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(records[0].claim, Claim::OverBudget);
    assert_eq!(records[0].step, violation.step);
    assert!(matches!(
        report.properties[0].outcome,
        PropertyOutcome::Violated { .. }
    ));
}
