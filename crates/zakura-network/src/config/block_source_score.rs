//! Settings for choosing which legacy peer to ask for an announced block.

use serde::{Deserialize, Serialize};

/// Settings for choosing which legacy peer to ask for an announced block.
///
/// When a block is announced by more than one idle peer, the node draws two of them at random
/// and asks one. By default it asks the one whose recent requests are less loaded. When enabled,
/// it asks the one expected to answer sooner, from how long each peer has recently taken to
/// answer pings, transaction requests and small block requests. A peer that fails to deliver a
/// block it announced is expected to be slower.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct BlockSourceScoreConfig {
    /// Whether announced blocks are requested from the announcer expected to answer sooner.
    pub enabled: bool,
}
