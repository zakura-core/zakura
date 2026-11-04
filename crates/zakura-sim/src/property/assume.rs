//! Scenario assumptions (properties.md, Assumptions).

use serde::{Deserialize, Serialize};

/// Something a liveness property needs the scenario to guarantee.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Assume {
    /// A named peer follows every row and serves the fixture chain.
    HonestPeer,
    /// Every datagram is delivered within a maximum delay, and loss stops
    /// once its budget is spent.
    BoundedDelivery,
    /// Every storage operation completes within a maximum latency.
    BoundedStorage,
    /// Each hostile peer takes at most a fixed number of actions per virtual
    /// second.
    BoundedAdversary,
    /// No connects or disconnects after a start step.
    StableMembership,
}

/// One assumption a scenario declares, with its parameters, as the trace's
/// `meta.assumptions` lists it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssumeRecord {
    /// The assumption.
    pub assume: Assume,
    /// Its parameters, such as the link's maximum delay. Written with sorted
    /// keys, so equal records write equal trace bytes.
    #[serde(serialize_with = "crate::trace::meta::sorted")]
    pub params: serde_json::Value,
}

/// The assumptions a scenario declares, and whether each holds after a step.
///
/// A property that assumes something the scenario doesn't declare is
/// `NotApplicable`. The harness enforces what it declares, so `holds` is
/// false only while the scenario deliberately breaks an assumption, as
/// during a fault.
pub struct Assumptions<S> {
    declared: Vec<AssumeRecord>,
    pub(crate) holds: fn(&S, Assume) -> bool,
}

impl<S> Assumptions<S> {
    /// A scenario that declares no assumptions.
    pub fn none() -> Self {
        Self::declare(Vec::new())
    }

    /// A scenario that declares `declared`, each holding after every step.
    pub fn declare(declared: Vec<AssumeRecord>) -> Self {
        Assumptions {
            declared,
            holds: always,
        }
    }

    /// Reads whether a declared assumption holds after each step from the
    /// harness's handle.
    pub fn holds_when(self, holds: fn(&S, Assume) -> bool) -> Self {
        Assumptions { holds, ..self }
    }

    /// The declared assumptions, in declaration order.
    pub fn records(&self) -> &[AssumeRecord] {
        &self.declared
    }

    /// True if the scenario declares `assume`.
    pub fn declares(&self, assume: Assume) -> bool {
        self.declared.iter().any(|record| record.assume == assume)
    }
}

/// Holds after every step.
fn always<S>(_: &S, _: Assume) -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The JSON matches the trace `meta` record's assumption entries.
    #[test]
    fn record_json() {
        let record = AssumeRecord {
            assume: Assume::BoundedDelivery,
            params: serde_json::json!({ "max_delay_ms": 200 }),
        };
        let json = serde_json::to_string(&record).expect("a record serializes");
        assert_eq!(
            json,
            r#"{"assume":"bounded-delivery","params":{"max_delay_ms":200}}"#
        );
        let back: AssumeRecord = serde_json::from_str(&json).expect("it reads back");
        assert_eq!(back, record);
    }
}
