//! Settings for choosing which legacy peers to ask for an announced block.

use serde::{Deserialize, Serialize};

/// Settings for choosing which legacy peers to ask for an announced block.
///
/// When a block is announced by more than one idle peer, the node draws two of them at random
/// and asks one. By default it asks the one whose recent requests are less loaded. When enabled:
///
/// - it estimates each peer's arrival time from its 10-minute windowed minimum heartbeat RTT
///   plus transfer time (`last_block_bytes / rate_in`), falling back to live fleet medians for
///   unmeasured peers, and passes over announcers that failed to deliver an announced block
///   within the last 10 minutes (cleared once they deliver a block);
/// - while a gossiped block's first request is outstanding, the peer set holds one optional
///   second request internally and dispatches it on peer events or discrete deadlines when
///   another idle announcer is expected to deliver clearly sooner, or when the first request
///   stalls past its expected arrival time.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct BlockDownloadScoreAndHedgeConfig {
    /// Whether announced blocks are requested from the announcer expected to answer sooner, and
    /// also from a second announcer expected to deliver them clearly sooner than the first.
    pub enabled: bool,
}
