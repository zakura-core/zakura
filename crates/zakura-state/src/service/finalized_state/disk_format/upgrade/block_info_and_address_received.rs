//! Validates that recent blocks have block info, and that recent transparent recipients have a
//! received balance.
//!
//! The commit path writes both. The 27.0.0 format upgrade backfilled them for older databases.
//! Startup refuses databases older than 28.0.0, so only this check remains.

use std::collections::HashSet;

use crossbeam_channel::{Receiver, TryRecvError};
use zakura_chain::block::Height;

use crate::{service::finalized_state::ZakuraDb, TransactionLocation};

use super::{CancelFormatChange, FormatChangeError};

/// Checks block info for the last 1000 blocks, and received balances for their transparent
/// recipients.
#[allow(clippy::unwrap_in_result)]
pub(super) fn detailed_check(
    db: &ZakuraDb,
    cancel_receiver: &Receiver<CancelFormatChange>,
) -> Result<Result<(), String>, FormatChangeError> {
    let network = db.network();

    // Return early before the next disk read if the check was cancelled.
    if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
        return Err(CancelFormatChange.into());
    }

    // Read the finalized tip height or return early if the database is empty.
    let Some(tip_height) = db.finalized_tip_height() else {
        return Ok(Ok(()));
    };

    // Check any outputs in the last 1000 blocks.
    let start_height = (tip_height - 1_000).unwrap_or(Height::MIN);

    if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
        return Err(FormatChangeError::Cancelled);
    }

    // Check that all blocks in the range have a BlockInfo.

    for height in start_height.0..=tip_height.0 {
        if let Some(block_info) = db.block_info_cf().zs_get(&Height(height)) {
            if block_info == Default::default() {
                return Ok(Err(format!("zero block info for height: {height}")));
            }
        } else {
            return Ok(Err(format!("missing block info for height: {height}")));
        }
    }

    if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
        return Err(FormatChangeError::Cancelled);
    }

    // A database without transparent indexes never stores address balances, so there are no
    // received balances to check. The block info check above still applies.
    if db.skips_transparent_indexes() {
        return Ok(Ok(()));
    }

    // Check that all recipient addresses of transparent transfers in the range have a non-zero received balance.

    // Collect the set of addresses that received transparent funds in the last query range (last 1000 blocks).
    let tx_loc_range = TransactionLocation::min_for_height(start_height)..;
    let addresses: HashSet<_> = db
        .transactions_by_location_range(tx_loc_range)
        .flat_map(|(_, tx)| tx.outputs().to_vec())
        .filter_map(|output| {
            if output.value != 0 {
                output.address(&network)
            } else {
                None
            }
        })
        .collect();

    // Check that no address balances for that set of addresses have a received field of `0`.
    for address in addresses {
        if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
            return Err(FormatChangeError::Cancelled);
        }

        let balance = db
            .address_balance_location(&address)
            .expect("should have address balances in finalized state");

        if balance.received() == 0 {
            return Ok(Err(format!(
                "unexpected balance received for address {}: {}",
                address,
                balance.received(),
            )));
        }
    }

    Ok(Ok(()))
}
