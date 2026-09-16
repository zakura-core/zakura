//! Backfill the ZIP 234 issuance deficit into existing databases.
//!
//! Zakura used to re-derive zips#1354's `IssuanceDeficit` on every block that computed a
//! subsidy, by summing the halving schedule from genesis and subtracting the issued supply.
//! It now carries the value in the chain value pools instead, as
//! [`ValueBalance::issuance_deficit`].
//!
//! Databases written before that change have no deficit leg. Their `BlockInfo` records and
//! tip value pool decode with a zero placeholder, which would understate every reissuance
//! bonus. This migration replaces the placeholder with the specification's value.
//!
//! The backfill is exact and needs no re-sync, because the deficit is a function of data the
//! database already holds:
//!
//! `IssuanceDeficit(height) = ExpectedIssuedSupply(height) - IssuedSupply(height)`
//!
//! `ExpectedIssuedSupply` is a closed form over the halving schedule, and `IssuedSupply` is
//! the sum of the pools stored in each block's `BlockInfo`.

use crossbeam_channel::{Receiver, TryRecvError};
use semver::Version;

use zakura_chain::{
    amount::{Amount, NegativeAllowed, NonNegative},
    block::Height,
    block_info::BlockInfo,
    parameters::subsidy::expected_issued_supply,
    value_balance::ValueBalance,
};

use crate::service::finalized_state::{DiskWriteBatch, ZakuraDb};

use super::{CancelFormatChange, DiskFormatUpgrade, FormatChangeError};

/// The number of block info records to rewrite per database batch.
const BATCH_BLOCKS: u32 = 10_000;

/// Implements [`DiskFormatUpgrade`] for the issuance deficit backfill.
pub struct Upgrade;

impl DiskFormatUpgrade for Upgrade {
    fn version(&self) -> Version {
        Version::new(28, 2, 0)
    }

    fn description(&self) -> &'static str {
        "backfill the ZIP 234 issuance deficit into the chain value pools"
    }

    #[allow(clippy::unwrap_in_result)]
    fn run(
        &self,
        initial_finalized_tip_height: Option<Height>,
        db: &ZakuraDb,
        cancel_receiver: &Receiver<CancelFormatChange>,
    ) -> Result<(), FormatChangeError> {
        let Some(tip_height) = initial_finalized_tip_height else {
            // An empty database has no blocks to backfill. A genesis sync under this version
            // writes the deficit as it goes.
            return Ok(());
        };

        let network = db.network();
        let mut batch = DiskWriteBatch::new();
        let mut batched = 0;

        for height in 0..=tip_height.0 {
            check_cancelled(cancel_receiver)?;

            let height = Height(height);
            let Some(block_info) = db.block_info(height.into()) else {
                // Pruned or otherwise absent block info has nothing to rewrite. The tip pool
                // below is what the next block actually reads.
                continue;
            };

            let Some(deficit) = deficit_at(&network, height, *block_info.value_pools()) else {
                // A chain ahead of its own schedule has a negative deficit, which the
                // specification only forbids from the reissuance start height. Leave such a
                // record alone rather than clamping it to a value the schedule disagrees
                // with.
                continue;
            };

            let mut value_pools = *block_info.value_pools();
            value_pools.set_issuance_deficit_amount(deficit);
            let _ = db
                .block_info_cf()
                .with_batch_for_writing(&mut batch)
                .zs_insert(&height, &BlockInfo::new(value_pools, block_info.size()));

            batched += 1;
            if batched == BATCH_BLOCKS {
                db.write_batch(batch)
                    .expect("rewriting block info with a deficit should always succeed");
                batch = DiskWriteBatch::new();
                batched = 0;
            }
        }

        check_cancelled(cancel_receiver)?;

        // The tip value pool is stored separately from the per-block records, and it is what
        // the next block's subsidy reads.
        let tip_pools = db.finalized_value_pool();
        if let Some(deficit) = deficit_at(&network, tip_height, tip_pools) {
            let mut tip_pools = tip_pools;
            tip_pools.set_issuance_deficit_amount(deficit);
            let _ = db
                .chain_value_pools_cf()
                .with_batch_for_writing(&mut batch)
                .zs_insert(&(), &tip_pools);
        }

        db.write_batch(batch)
            .expect("rewriting the tip value pool with a deficit should always succeed");

        Ok(())
    }

    fn validate(
        &self,
        db: &ZakuraDb,
        _cancel_receiver: &Receiver<CancelFormatChange>,
    ) -> Result<Result<(), String>, FormatChangeError> {
        let Some(tip_height) = db.finalized_tip_height() else {
            return Ok(Ok(()));
        };

        let network = db.network();
        let tip_pools = db.finalized_value_pool();

        let Some(expected) = deficit_at(&network, tip_height, tip_pools) else {
            // The chain is ahead of its schedule, so there is no non-negative deficit to
            // check against.
            return Ok(Ok(()));
        };

        if tip_pools.issuance_deficit_amount() != expected {
            return Ok(Err(format!(
                "tip issuance deficit {:?} does not match the halving schedule's {expected:?} \
                 at {tip_height:?}",
                tip_pools.issuance_deficit_amount(),
            )));
        }

        Ok(Ok(()))
    }
}

/// Returns `ExpectedIssuedSupply(height) - IssuedSupply(height)`, or `None` if the chain is
/// ahead of its own schedule at `height`.
fn deficit_at(
    network: &zakura_chain::parameters::Network,
    height: Height,
    value_pools: ValueBalance<NonNegative>,
) -> Option<Amount<NegativeAllowed>> {
    let expected = expected_issued_supply(height, network).ok()?;

    (expected - value_pools.issued_supply())
        .ok()?
        .constrain()
        .ok()
}

fn check_cancelled(
    cancel_receiver: &Receiver<CancelFormatChange>,
) -> Result<(), CancelFormatChange> {
    match cancel_receiver.try_recv() {
        Err(TryRecvError::Empty) => Ok(()),
        _ => Err(CancelFormatChange),
    }
}
