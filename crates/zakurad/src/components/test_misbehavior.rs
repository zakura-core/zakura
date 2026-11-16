//! Helpers for tests that check which peers a component reports for misbehavior.

use zakura_network::{self as zn, PeerSocketAddr};

/// Returns a misbehavior reporter with a ban list of its own.
pub(crate) fn reporter() -> zn::MisbehaviorReporter {
    zn::MisbehaviorReporter::new(zn::BannedIps::default(), zn::UnconditionalPeers::default())
}

/// Returns the total misbehavior score reported for `addr`'s IP address.
///
/// A ban clears an IP's score, so a banned IP reports the ban threshold.
pub(crate) fn reported_score(reporter: &zn::MisbehaviorReporter, addr: PeerSocketAddr) -> u32 {
    if reporter.is_banned(addr.ip()) {
        zn::constants::MAX_PEER_MISBEHAVIOR_SCORE
    } else {
        reporter.score(addr.ip())
    }
}
