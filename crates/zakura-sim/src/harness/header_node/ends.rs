//! How the node's work could end without the node deciding it: the
//! harness's environment actions, and a reactor that returned by itself.
//!
//! Only an environment action may discharge an obligation or break an
//! assumption. A link's close
//! doesn't show that, since the node's own service can cancel a
//! connection too, as replacing a session with an unfinished exchange
//! does. So the harness writes [`EndFact::Environment`] where it acts: a
//! close as the harness makes it, naming the connection it closes, a relink
//! before its link exists, naming the new connection whose admission
//! replaces the peer's earlier session, and a `Shutdown` step at its
//! validated pick, since the stop has begun once the step loop picks it and
//! storage can commit before the control task runs. A cancel by the node's
//! service writes nothing here.
//!
//! The store freezes with the run's log, as every store its
//! [`EvidenceStores`] handle builds does: the scheduler's shutdown can
//! still run a harness task stopped partway, and a fact it records then
//! would rewrite what the run ended with.
//!
//! A reactor that returns without the harness stopping it is the node's
//! own end, not missing evidence, so the tap records it as
//! [`EndFact::ReactorExited`] at the drain that finds it, and the run goes
//! on with the evidence it has. A reactor that unwound is a crash, which
//! still fails the run.
//!
//! Each fact takes the node's next stamp ([`super::stamp`]). The store
//! holds the node's stamps, which the tap it's handed to shares with every
//! other store it keeps.

use super::stamp::Stamps;
use crate::stores::{debug_snapshot, EvidenceStores, Registered, Snapshot};

/// The environment action that ended something.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnvironmentAction {
    /// The harness closed the peer's newest link, as a `Close` step or a
    /// churn does.
    Close,
    /// The harness linked the peer again, as a `Relink`, churn or flood
    /// step does, so the node replaces its earlier session once it admits
    /// the new one.
    Relink,
    /// A `Shutdown` step began stopping the node under test.
    Shutdown,
}

/// A fact this store keeps.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EndFact {
    /// The harness acted on the node's environment.
    Environment {
        /// The peer the action names, or the node under test for a
        /// shutdown.
        peer: String,
        /// The connection it names: the closed link's, the new link's for a
        /// relink, or `None` for a shutdown, which closes every link.
        connection: Option<u64>,
        /// What it did.
        action: EnvironmentAction,
    },
    /// The reactor's task ended without the harness stopping it and
    /// without unwinding.
    ReactorExited {
        /// The step whose drain found it gone.
        step: u64,
    },
}

/// What [`EndEvidence`] keeps.
#[derive(Debug, Default)]
struct Store {
    /// Where its stamps come from.
    stamps: Stamps,
    /// Each fact with its stamp, in stamp order.
    facts: Vec<(u64, EndFact)>,
}

impl Snapshot for Store {
    /// The stamp counter and every fact.
    fn snapshot(&self) -> serde_json::Value {
        debug_snapshot(self)
    }
}

/// One node's end facts, shared by its tap, its harness and whoever reads
/// them. Cloning shares the store.
#[derive(Clone, Debug)]
pub struct EndEvidence(Registered<Store>);

impl EndEvidence {
    /// A store in `stores`, with new stamps.
    pub fn new(stores: &EvidenceStores) -> EndEvidence {
        EndEvidence(stores.register("ends", Store::default()))
    }

    /// The handle that built the store, the node's.
    pub fn stores(&self) -> &EvidenceStores {
        self.0.stores()
    }

    /// The node's stamps, which this store's facts take.
    pub fn stamps(&self) -> Stamps {
        self.with(|store| store.stamps.clone())
    }

    /// Runs `read` on the store. Nothing else is called while the lock is
    /// held.
    fn with<R>(&self, read: impl FnOnce(&Store) -> R) -> R {
        self.0.read(read)
    }

    /// Keeps `fact` with the next stamp, unless the store is frozen.
    pub(super) fn record(&self, fact: EndFact) {
        self.0.write(|store| {
            let stamp = store.stamps.next();
            store.facts.push((stamp, fact));
        });
    }

    /// Every fact so far, with its stamp, in stamp order.
    pub fn facts(&self) -> Vec<(u64, EndFact)> {
        self.with(|store| store.facts.clone())
    }

    /// The fact with `stamp`, if this store holds it.
    pub fn fact(&self, stamp: u64) -> Option<EndFact> {
        self.with(|store| {
            store
                .facts
                .binary_search_by_key(&stamp, |(at, _)| *at)
                .ok()
                .map(|index| store.facts[index].1.clone())
        })
    }

    /// Whether the reactor exited by itself, and at which step's drain.
    pub fn reactor_exited(&self) -> Option<u64> {
        self.with(|store| {
            store.facts.iter().find_map(|(_, fact)| match fact {
                EndFact::ReactorExited { step } => Some(*step),
                EndFact::Environment { .. } => None,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{EndEvidence, EndFact, EnvironmentAction};
    use crate::stores::EvidenceStores;

    /// Facts take the shared counter's stamps, so another store's stamps
    /// interleave with them, and a stamp the store doesn't hold finds
    /// nothing.
    #[test]
    fn end_facts_share_the_node_stamps() {
        let stores = EvidenceStores::default();
        let ends = EndEvidence::new(&stores);
        let stamps = ends.stamps();
        let close = EndFact::Environment {
            peer: "mirror-0".to_owned(),
            connection: Some(4),
            action: EnvironmentAction::Close,
        };
        ends.record(close.clone());
        assert_eq!(stamps.next(), 2, "another store's fact");
        ends.record(EndFact::ReactorExited { step: 9 });
        assert_eq!(
            ends.facts(),
            [(1, close.clone()), (3, EndFact::ReactorExited { step: 9 })]
        );
        assert_eq!(ends.fact(1), Some(close));
        assert_eq!(ends.fact(2), None);
        assert_eq!(ends.reactor_exited(), Some(9));
        stores.freeze();
        ends.record(EndFact::ReactorExited { step: 10 });
        assert_eq!(ends.facts().len(), 2, "a frozen store keeps nothing more");
        assert_eq!(
            stamps.after(Some(3)),
            Vec::<u64>::new(),
            "and takes no stamp"
        );
    }

    /// An end fact recorded after the node's stores froze is dropped.
    #[test]
    fn a_frozen_end_store_drops_a_fact() {
        crate::stores::assert_freeze_drops_writes(
            EndEvidence::new,
            |ends| ends.record(EndFact::ReactorExited { step: 1 }),
            EndEvidence::facts,
        );
    }
}
