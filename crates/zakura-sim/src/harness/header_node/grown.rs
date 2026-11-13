//! The harness's completed growths of a mirror's chain.
//!
//! A `Grow` control step is carried out by the control task after its
//! pick, so the pick alone doesn't show the mirror grew. The harness
//! records each growth here once the mirror's published tip is the new one,
//! naming the step's position in the control script, so a history's
//! declared `Grow` is shown by its own step's fact. A growth takes no
//! stamp: it ends nothing the node decided, so it stays out of the node's
//! stamp order.
//!
//! The store freezes with the run's log, as every store its
//! [`EvidenceStores`] handle builds does, so a growth a stopped control
//! task finishes during the scheduler's shutdown isn't recorded.

use crate::stores::{EvidenceStores, Registered, Snapshot};

/// One completed growth.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Grown {
    /// The mirror it grew.
    pub peer: String,
    /// The position in the control script of the step that grew it, if a
    /// scripted step did.
    pub control: Option<usize>,
}

/// What [`GrowEvidence`] keeps.
#[derive(Debug, Default)]
struct Store {
    /// Each growth, in the order it completed.
    facts: Vec<Grown>,
}

impl Snapshot for Store {
    /// Each growth's peer and control position, in order:
    /// `{"facts": [{"peer": "<peer>", "control": <position> | null}, ...]}`.
    fn snapshot(&self) -> serde_json::Value {
        let facts: Vec<serde_json::Value> = self
            .facts
            .iter()
            .map(|grown| serde_json::json!({ "peer": grown.peer, "control": grown.control }))
            .collect();
        serde_json::json!({ "facts": facts })
    }
}

/// One node's completed growths, shared by its topology and whoever reads
/// them. Cloning shares the store.
#[derive(Clone, Debug)]
pub struct GrowEvidence(Registered<Store>);

impl GrowEvidence {
    /// An empty store in `stores`, the node's.
    pub fn new(stores: &EvidenceStores) -> GrowEvidence {
        GrowEvidence(stores.register("grown", Store::default()))
    }

    /// Keeps a completed growth of `peer` by the control step at
    /// `control`, unless the store is frozen.
    pub(crate) fn record(&self, peer: &str, control: Option<usize>) {
        self.0.write(|store| {
            store.facts.push(Grown {
                peer: peer.to_owned(),
                control,
            })
        });
    }

    /// Every growth so far, in the order it completed.
    pub fn facts(&self) -> Vec<Grown> {
        self.0.read(|store| store.facts.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::GrowEvidence;
    use crate::stores::{EvidenceStores, Snapshot};

    /// Growths keep their order, peer and control position, as the
    /// snapshot names each.
    #[test]
    fn growths_keep_their_order_and_cause() {
        let grown = GrowEvidence::new(&EvidenceStores::default());
        grown.record("mirror-1", Some(2));
        grown.record("mirror-0", None);
        let store = super::Store {
            facts: grown.facts(),
        };
        assert_eq!(
            store.snapshot(),
            serde_json::json!({ "facts": [
                { "peer": "mirror-1", "control": 2 },
                { "peer": "mirror-0", "control": null },
            ] })
        );
    }

    /// A growth recorded after the node's stores froze is dropped.
    #[test]
    fn a_frozen_grow_store_drops_a_growth() {
        crate::stores::assert_freeze_drops_writes(
            GrowEvidence::new,
            |grown| grown.record("mirror-0", Some(0)),
            GrowEvidence::facts,
        );
    }
}
