//! Step 10's progress deadline for header sync: the time by which an honest
//! peer gets the node to its tip despite k adversaries. #2604's
//! production-machine tests and the simulator's progress harness both
//! derive their deadline here, from production constants, and get no
//! figure where those constants bound nothing.

use std::time::Duration;

use super::{reactor::CACHED_PASS_REFILL, ZakuraHeaderSyncConfig};

/// Why [`header_progress_deadline`] has no figure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeaderProgressUnbounded {
    /// `max_unproductive_header_requests` is zero, so no stall ever ends an
    /// adversary's session.
    NoStallRule,
}

/// One adversary's turn under `config` and the reactor's `request_timeout`:
/// the longest a session holds a target before the stall rule ends it,
/// `max_unproductive_header_requests` request timeouts, plus one cached-pass
/// refill before the next offer stages.
pub fn header_progress_turn(
    config: &ZakuraHeaderSyncConfig,
    request_timeout: Duration,
) -> Result<Duration, HeaderProgressUnbounded> {
    let strikes = config.max_unproductive_header_requests;
    if strikes == 0 {
        return Err(HeaderProgressUnbounded::NoStallRule);
    }
    Ok(request_timeout * strikes + CACHED_PASS_REFILL)
}

/// The time by which a node must adopt the honest tip against `adversaries`
/// peers, each holding one session at a time and free to open a new one under
/// a fresh key whenever the node ends the last. The node runs `config` with
/// `request_timeout`, and admits `max_connections`.
///
/// Only one peer owns a branch target at a time, and offers take turns in
/// the order their peers last took one, so a reopened session's offer waits
/// behind the honest one. In the worst case the adversaries that waited
/// ahead of the honest offer hold the honest branch one after another, a
/// [`header_progress_turn`] each, and the honest round then finishes within
/// one more. At most the header-sync session slots, less the honest one,
/// wait ahead, so the deadline never exceeds one turn per slot.
pub fn header_progress_deadline(
    config: &ZakuraHeaderSyncConfig,
    request_timeout: Duration,
    max_connections: usize,
    adversaries: u32,
) -> Result<Duration, HeaderProgressUnbounded> {
    let turn = header_progress_turn(config, request_timeout)?;
    let slots = u32::try_from(config.serving_sessions(max_connections)).unwrap_or(u32::MAX);
    Ok(turn * adversaries.saturating_add(1).min(slots))
}
