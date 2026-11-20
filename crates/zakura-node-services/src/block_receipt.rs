//! Receipt metadata shared by the legacy network decoder and the block verifier.

use tokio::time::Instant;
use zakura_chain::block;

/// Receipt order and height context when the receiver held the block header of a legacy `block`
/// message, before the rest of the block arrived.
///
/// A reservation is made for one block hash and is only ever used for that block. The hash is
/// known before the order is taken, so a place cannot be reserved for a block that does not
/// exist yet: a valid block's hash cannot be known before its proof of work is done.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct BlockReservation {
    /// The hash of the block header the receiver held when it reserved the order.
    pub hash: block::Hash,
    /// The order reserved when the block header arrived.
    pub order: u64,
    /// The best tip when the order was reserved.
    pub verified_tip: block::Height,
    /// Highest verified tip observed by the claim store up to the reservation.
    pub floor: block::Height,
    /// When the order was reserved.
    pub started_at: Instant,
}
