//! Semantic evidence from transport-free machine drivers.

use serde::{Deserialize, Serialize};
use zakura_network::zakura::{BookStep, SessionStep};

/// Typed semantic evidence from one transport-free machine operation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "step",
    rename_all = "kebab-case",
    deny_unknown_fields
)]
pub enum MachineStep {
    /// The real discovery book, its independent model, and its signed samples.
    DiscoveryBook(BookStep),
    /// Discovery's real session and hint owner and its independent model.
    DiscoveryState(SessionStep),
}
