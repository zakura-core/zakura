//! Settings for choosing which legacy peer to ask for an announced block.

use serde::{Deserialize, Serialize};

/// Settings for choosing which legacy peer to ask for an announced block.
///
/// When a block is announced by more than one idle peer, the node draws two of them at random
/// and asks one. By default it asks the one whose recent requests are less loaded. When enabled,
/// it asks the one expected to answer sooner, from how long each peer has recently taken to
/// answer pings and block requests. The node records missed announcements in either mode.
/// Only with scoring enabled does it pass over an announcer that missed, until a later body
/// from the same connection verifies or 10 minutes elapse.
///
/// With `second_request` also on, a block that the first request has not delivered yet is also
/// requested from one other idle peer that announced it, but only when that peer is expected to
/// deliver it clearly sooner than the first, or when the first request has run well past its
/// expected time. Both peers must have measured answer times. The first reply that passes the
/// requested-hash and tip-child height checks is tried first. If its height falls outside the
/// local window or verification rejects its commitments or duplicate transactions, the node
/// tries the other already-requested copy once.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct BlockSourceScoreConfig {
    /// Whether announced blocks are requested from the announcer expected to answer sooner.
    pub enabled: bool,

    /// Whether a block can also be requested from a second announcer expected to deliver it
    /// clearly sooner than the first. Requires `enabled`.
    pub second_request: bool,
}

impl BlockSourceScoreConfig {
    /// Checks that a second request is only enabled with the expected answer times it needs.
    pub fn validate(&self) -> Result<(), String> {
        if self.second_request && !self.enabled {
            return Err("network.block_source_score.second_request requires \
                 network.block_source_score.enabled"
                .to_string());
        }
        Ok(())
    }
}
