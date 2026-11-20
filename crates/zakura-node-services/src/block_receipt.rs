//! Receipt metadata shared by the legacy network decoder and the block verifier.

use tokio::time::Instant;
use zakura_chain::block;

/// Receipt order and height context when the receiver read a legacy `block` message envelope.
/// The decoder carries this reservation until it knows the hash of the complete block.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct BlockReservation {
    /// The order reserved at the start of the transfer.
    pub order: u64,
    /// The best tip at the start of the transfer.
    pub verified_tip: block::Height,
    /// Highest verified tip observed by the claim store up to the start of the transfer.
    pub floor: block::Height,
    /// When the order was reserved.
    pub started_at: Instant,
}
