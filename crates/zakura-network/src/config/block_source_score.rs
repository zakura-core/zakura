//! Settings for choosing which legacy peer to ask for an announced block.

use serde::{Deserialize, Serialize};

/// Settings for choosing which legacy peer to ask for an announced block.
///
/// When a block is announced by more than one idle peer, the node draws two of them at random
/// and asks one. By default it asks the one whose recent requests are less loaded. When enabled,
/// it asks the one expected to answer sooner, from how long each peer has recently taken to
/// answer pings, transaction requests and small block requests. A peer that fails to deliver a
/// block it announced is expected to be slower.
///
/// With `second_request` also on, a block that the first request has not delivered yet is also
/// requested from one other idle peer that announced it, but only when that peer is expected to
/// deliver it clearly sooner than the first, or when the first request has run well past its
/// expected time. The first copy to arrive is used. A block is never requested more than twice
/// this way.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct BlockSourceScoreConfig {
    /// Whether announced blocks are requested from the announcer expected to answer sooner.
    pub enabled: bool,

    /// Whether a block can also be requested from a second announcer expected to deliver it
    /// clearly sooner than the first. Requires `enabled`.
    pub second_request: bool,

    /// Ask up to two distinct legacy announcers in announcement order.
    /// This experiment requires both score-based settings to be off.
    pub first_two_announcers: bool,
}

impl BlockSourceScoreConfig {
    /// Checks that the experiment settings are compatible.
    pub fn validate(&self) -> Result<(), String> {
        if self.second_request && !self.enabled {
            return Err("network.block_source_score.second_request requires \
                 network.block_source_score.enabled"
                .to_string());
        }
        if self.first_two_announcers && (self.enabled || self.second_request) {
            return Err(
                "network.block_source_score.first_two_announcers requires scoring to be disabled"
                    .to_string(),
            );
        }
        Ok(())
    }
}
