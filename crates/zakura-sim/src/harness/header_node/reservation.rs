//! The node under test's header capacity accounting, as view-side evidence.
//!
//! The work queue reports each reservation it opens, binds and ends, each
//! page it stages, each lease that returns, and each round it releases. The reactor tap checks them
//! as they arrive, in its [`HeaderWorkLedger`], which fails the run at the
//! next drain when a rule breaks. It also keeps them here, in the order the
//! queue reported them, for a test or a later property to read. They stay
//! out of the observation log, so pinned Plans and traces keep their format.
//!
//! [`HeaderWorkLedger`]: zakura_network::zakura::observe::HeaderWorkLedger

use zakura_network::zakura::observe::ReactorEvent;

use crate::stores::{debug_snapshot, EvidenceStores, Registered, Snapshot};

/// What [`ReservationEvidence`] holds: each accounting fact, in order.
#[derive(Debug, Default)]
pub(crate) struct Facts(Vec<ReactorEvent>);

impl Snapshot for Facts {
    /// Every fact, in order.
    fn snapshot(&self) -> serde_json::Value {
        debug_snapshot(&self.0)
    }
}

/// One reactor's accounting facts, shared by its tap and whoever reads them.
/// Cloning shares the store.
#[derive(Clone, Debug)]
pub struct ReservationEvidence(Registered<Facts>);

impl ReservationEvidence {
    /// An empty store in `stores`, the node's.
    pub fn new(stores: &EvidenceStores) -> ReservationEvidence {
        ReservationEvidence(stores.register("reservations", Facts::default()))
    }

    /// The handle that built the store, the node's.
    pub fn stores(&self) -> &EvidenceStores {
        self.0.stores()
    }

    /// Whether this store keeps `event`: a reservation's opening, binding or
    /// end, a staged page, a returned lease, or a round's release.
    pub(super) fn keeps(event: &ReactorEvent) -> bool {
        matches!(
            event,
            ReactorEvent::ReservationOpened { .. }
                | ReactorEvent::ReservationBound { .. }
                | ReactorEvent::ReservationEnded { .. }
                | ReactorEvent::PageStaged { .. }
                | ReactorEvent::LeaseReturned { .. }
                | ReactorEvent::RoundReleased { .. }
        )
    }

    /// Records `event`. Nothing else is called while the lock is held.
    pub(super) fn record(&self, event: &ReactorEvent) {
        self.0.write(|facts| facts.0.push(event.clone()));
    }

    /// Every fact so far, in the order the queue reported them.
    pub fn facts(&self) -> Vec<ReactorEvent> {
        self.0.read(|facts| facts.0.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use zakura_network::zakura::{
        observe::{ReactorEvent, ReservationEnd, ReservationPurpose},
        ZakuraPeerId,
    };
    use zakura_storage::header::{controlled::ControlledPort, fixture::HeaderFixture};

    use super::ReservationEvidence;
    use crate::{
        harness::header_node::{
            capacity::CapacityEvidence, claim::SettlementEvidence, ends::EndEvidence,
            session::SessionEvidence, stale::StaleEvidence, tap::ReactorTap,
        },
        stores::EvidenceStores,
    };

    /// A tap keeps every accounting fact it's handed, through its capacity
    /// handle or its own, in order, and nothing else.
    #[test]
    fn the_tap_keeps_accounting_facts_in_order() {
        let mut sim = crate::Sim::new(
            crate::harness::header_sync::storage_config(),
            crate::PlanInput::Bytes(Vec::new()),
        );
        let port = Arc::new(ControlledPort::new(&HeaderFixture::regtest()));
        let stores = EvidenceStores::default();
        let evidence = ReservationEvidence::new(&stores);
        let (observer, _tap) = ReactorTap::with_evidence(
            &mut sim,
            &port,
            "node",
            SessionEvidence::new(&stores),
            evidence.clone(),
            CapacityEvidence::new(&stores),
            SettlementEvidence::new(&stores),
            StaleEvidence::new(&stores),
            EndEvidence::new(&stores),
        );
        let capacity = observer
            .capacity_observer()
            .expect("the tap hands the queue its own capacity handle");
        let opened = ReactorEvent::ReservationOpened {
            reservation: 1,
            peer: ZakuraPeerId::new(vec![2; 32]).expect("32 bytes is a valid peer ID"),
            purpose: ReservationPurpose::Ordinary,
            headers: 2,
            request: None,
        };
        let ended = ReactorEvent::ReservationEnded {
            reservation: 1,
            released: 2,
            cause: ReservationEnd::Cancelled,
        };

        capacity.observe(opened.clone());
        observer.observe(ReactorEvent::StorageJoined { call: 1 });
        observer.observe(ended.clone());

        assert_eq!(evidence.facts(), [opened, ended]);
    }
}
