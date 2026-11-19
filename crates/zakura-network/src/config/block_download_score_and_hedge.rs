//! Settings for choosing which legacy peers to ask for an announced block.

use serde::{Deserialize, Serialize};

/// Settings for choosing which legacy peers to ask for an announced block.
///
/// When a block is announced by more than one idle peer, the node draws two of them at random
/// and asks one. By default it asks the one whose recent requests are less loaded. When enabled:
///
/// - it asks the one expected to answer sooner, from how long each peer has recently taken to
///   answer pings and requests for blocks small enough to arrive in one round trip (14,600
///   bytes; larger downloads are measured but do not count). The node records missed announcements in either mode, but
///   only when enabled does it pass over an announcer that missed, until a later body from the
///   same connection verifies or 10 minutes elapse;
/// - a block that the first request has not delivered yet is also requested from one other idle
///   peer that announced it, but only when that peer is expected to deliver it clearly sooner
///   than the first, or when the first request has run well past its expected time. Both peers
///   must have measured answer times. The first reply that passes the requested-hash and
///   tip-child height checks is tried first. If its height falls outside the local window or
///   verification rejects its commitments or duplicate transactions, the node tries the other
///   already-requested copy once.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct BlockDownloadScoreAndHedgeConfig {
    /// Whether announced blocks are requested from the announcer expected to answer sooner, and
    /// also from a second announcer expected to deliver them clearly sooner than the first.
    pub enabled: bool,
}
