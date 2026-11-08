//! How the node's work could end without the node deciding it: the
//! harness's environment actions, and a reactor that returned by itself.
//!
//! Only an environment action may discharge an obligation or break an
//! assumption. A link's close
//! doesn't show that, since the node's own service can cancel a
//! connection too, as replacing a session with an unfinished exchange
//! does. So the harness writes [`EndFact::Environment`] where it acts: a
//! `Close` or `Relink` control step as its task carries it out, and a
//! `Shutdown` step at its validated pick, since the stop has begun once
//! the step loop picks it and storage can commit before the control task
//! runs. A cancel by the node's service writes nothing here.
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

use std::sync::{Arc, Mutex, PoisonError};

use super::stamp::Stamps;

/// The environment action that ended something.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnvironmentAction {
    /// A `Close` step closed the peer's newest link.
    Close,
    /// A `Relink` step linked the peer again, replacing its newest link's
    /// session.
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
        /// The connection it ends: the closed or replaced link's, or `None`
        /// for a shutdown, which closes every link.
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

/// One node's end facts, shared by its tap, its harness and whoever reads
/// them. Cloning shares the store.
#[derive(Clone, Debug, Default)]
pub struct EndEvidence(Arc<Mutex<Store>>);

impl EndEvidence {
    /// The node's stamps, which this store's facts take.
    pub fn stamps(&self) -> Stamps {
        self.with(|store| store.stamps.clone())
    }

    /// Runs `read` on the store. Nothing else is called while the lock is
    /// held.
    fn with<R>(&self, read: impl FnOnce(&mut Store) -> R) -> R {
        read(&mut self.0.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// Keeps `fact` with the next stamp.
    pub(super) fn record(&self, fact: EndFact) {
        self.with(|store| {
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

    /// Facts take the shared counter's stamps, so another store's stamps
    /// interleave with them, and a stamp the store doesn't hold finds
    /// nothing.
    #[test]
    fn end_facts_share_the_node_stamps() {
        let ends = EndEvidence::default();
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
    }
}
