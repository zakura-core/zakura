//! Step 10's progress deadline for block sync: the time by which an honest
//! peer gets every needed body to commit despite k adversaries. #2604's
//! production-machine tests and the simulator's progress harness both
//! derive their deadline here, from production constants, and get no
//! figure where those constants bound nothing.

use std::time::Duration;

use super::{admission, ZakuraBlockSyncConfig};

/// Why [`block_progress_deadline`] has no figure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BlockProgressUnbounded {
    /// The deadline outlasts the park cooldown, so a parked adversary can
    /// return within it. Only the per-IP handshake rate bounds how often it
    /// returns, so no figure follows from the block-sync constants.
    ParkEnds,
}

/// One session's liveness deadline when it owes `bytes`: the allowance, the
/// owed bytes at the minimum rate, and at most one local-write grace of
/// `request_timeout`. A session that delivers no body by then is parked.
pub fn block_progress_turn(config: &ZakuraBlockSyncConfig, bytes: u64) -> Duration {
    config.effective_liveness_timeout()
        + admission::min_rate_transfer_time(bytes)
        + config.request_timeout
}

/// The time by which every body must commit against `adversaries` peers
/// that each hold one session, when the honest peer must move `bytes`.
///
/// An adversary owes at most every needed body, so it's parked within one
/// [`block_progress_turn`]. In the worst case the adversaries hold work one
/// after another. The honest peer then moves every body at the minimum rate
/// within one more turn. The figure holds only while no parked adversary
/// returns, so it must end before the park cooldown does.
pub fn block_progress_deadline(
    config: &ZakuraBlockSyncConfig,
    adversaries: u32,
    bytes: u64,
) -> Result<Duration, BlockProgressUnbounded> {
    let deadline = block_progress_turn(config, bytes) * adversaries.saturating_add(1);
    if deadline > config.effective_no_progress_peer_cooldown() {
        return Err(BlockProgressUnbounded::ParkEnds);
    }
    Ok(deadline)
}
