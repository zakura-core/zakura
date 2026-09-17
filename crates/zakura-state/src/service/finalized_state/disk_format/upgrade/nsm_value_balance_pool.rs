//! Backfill the NSM value balance from the last block below NU7 onward.
//!
//! The balance holds `INITIAL_NSM_VALUE_BALANCE` on the last block below NU7, and from
//! NU7 it falls by each block's additional block subsidy. Earlier records carry zero.
//! `Block::nsm_value_balance_change` applies the same rule as the chain grows.

use crossbeam_channel::{Receiver, TryRecvError};
use semver::Version;

use zakura_chain::{
    amount::{Amount, NegativeAllowed, NonNegative},
    block::Height,
    block_info::BlockInfo,
    parameters::{
        subsidy::{scheduled_issuance_zatoshis, ParameterSubsidy},
        NetworkUpgrade,
    },
    value_balance::ValueBalance,
};

use crate::service::finalized_state::{DiskWriteBatch, ZakuraDb};

use super::{CancelFormatChange, DiskFormatUpgrade, FormatChangeError};

/// The number of block info records to rewrite per database batch.
const BATCH_BLOCKS: u32 = 10_000;

/// Implements [`DiskFormatUpgrade`] for the NSM value balance backfill.
pub struct Upgrade;

impl DiskFormatUpgrade for Upgrade {
    fn version(&self) -> Version {
        Version::new(29, 0, 0)
    }

    fn description(&self) -> &'static str {
        "backfill the ZIP 234 NSM value balance into the chain value pools"
    }

    #[allow(clippy::unwrap_in_result)]
    fn run(
        &self,
        initial_finalized_tip_height: Option<Height>,
        db: &ZakuraDb,
        cancel_receiver: &Receiver<CancelFormatChange>,
    ) -> Result<(), FormatChangeError> {
        backfill(initial_finalized_tip_height, db, cancel_receiver, |batch| {
            db.write_batch(batch)
                .map_err(|error| FormatChangeError::MigrationStorage(error.to_string()))
        })
    }

    fn validate(
        &self,
        db: &ZakuraDb,
        cancel_receiver: &Receiver<CancelFormatChange>,
    ) -> Result<Result<(), String>, FormatChangeError> {
        let Some(tip_height) = db.finalized_tip_height() else {
            return Ok(Ok(()));
        };

        let network = db.network();
        let tip_pools = read_tip_pools(db)?;

        check_cancelled(cancel_receiver)?;
        let tip_info = read_block_info(db, tip_height)?;
        if *tip_info.value_pools() != tip_pools {
            return Ok(Err(format!(
                "tip pools disagree with BlockInfo at {tip_height:?}"
            )));
        }
        let expected = eligible_balance(&network, tip_height, tip_pools, baseline(db)?)?;

        if tip_pools.nsm_value_balance_amount() != expected {
            return Ok(Err(format!(
                "tip NSM value balance {:?} does not match the halving schedule's {expected:?} \
                 at {tip_height:?}",
                tip_pools.nsm_value_balance_amount(),
            )));
        }

        Ok(Ok(()))
    }
}

fn backfill(
    initial_finalized_tip_height: Option<Height>,
    db: &ZakuraDb,
    cancel_receiver: &Receiver<CancelFormatChange>,
    mut write: impl FnMut(DiskWriteBatch) -> Result<(), FormatChangeError>,
) -> Result<(), FormatChangeError> {
    let Some(tip_height) = initial_finalized_tip_height else {
        // An empty database has no blocks to backfill. A genesis sync under this version
        // writes the balance as it goes.
        return Ok(());
    };

    let network = db.network();
    let baseline = baseline(db)?;
    let mut batch = DiskWriteBatch::new();
    let mut batched = 0;

    for height in 0..=tip_height.0 {
        check_cancelled(cancel_receiver)?;

        let height = Height(height);
        let block_info = read_block_info(db, height)?;

        let balance = eligible_balance(&network, height, *block_info.value_pools(), baseline)?;

        let mut value_pools = *block_info.value_pools();
        value_pools.set_nsm_value_balance_amount(balance);
        let _ = db
            .block_info_cf()
            .with_batch_for_writing(&mut batch)
            .zs_insert(&height, &BlockInfo::new(value_pools, block_info.size()));

        batched += 1;
        if batched == BATCH_BLOCKS {
            write(batch)?;
            batch = DiskWriteBatch::new();
            batched = 0;
        }
    }

    check_cancelled(cancel_receiver)?;

    // The tip value pool is stored separately from the per-block records, and it is what
    // the next block's subsidy reads.
    let tip_pools = read_tip_pools(db)?;
    let balance = eligible_balance(&network, tip_height, tip_pools, baseline)?;
    let mut tip_pools = tip_pools;
    tip_pools.set_nsm_value_balance_amount(balance);
    let _ = db
        .chain_value_pools_cf()
        .with_batch_for_writing(&mut batch)
        .zs_insert(&(), &tip_pools);

    write(batch)?;

    Ok(())
}

/// Returns the schedule's cumulative subsidy at `height` minus the value the chain holds,
/// as a signed value: below the reissuance start a chain can run ahead of its schedule.
fn balance_at(
    network: &zakura_chain::parameters::Network,
    height: Height,
    value_pools: ValueBalance<NonNegative>,
) -> Result<i128, zakura_chain::parameters::subsidy::SubsidyError> {
    let scheduled = i128::try_from(scheduled_issuance_zatoshis(height, network)?)
        .map_err(|_| zakura_chain::parameters::subsidy::SubsidyError::Overflow)?;
    Ok(scheduled - i128::from(i64::from(value_pools.total()?)))
}

/// Returns the offset that makes the backfilled balance start at
/// `INITIAL_NSM_VALUE_BALANCE` on the last block below NU7.
///
/// `balance_at` measures the whole gap between the schedule and the chain, back to
/// genesis. Subtracting this offset leaves the seed there, and leaves each later block
/// the seed minus the bonuses claimed since, which is what
/// `Block::nsm_value_balance_change` accumulates.
///
/// The offset is zero when the constant matches the chain's own history, as the measured
/// Mainnet and Testnet constants do.
fn baseline(db: &ZakuraDb) -> Result<i128, FormatChangeError> {
    let network = db.network();
    let Some(start) = NetworkUpgrade::Nu7.activation_height(&network) else {
        return Ok(0);
    };
    let Some(seed_height) = start.0.checked_sub(1).map(Height) else {
        // NU7 at genesis leaves no block to seed, so the balance starts at zero.
        return Ok(0);
    };
    if db
        .finalized_tip_height()
        .is_none_or(|tip| tip < seed_height)
    {
        return Ok(0);
    }
    let info = read_block_info(db, seed_height)?;
    let historical = balance_at(&network, seed_height, *info.value_pools()).map_err(|error| {
        FormatChangeError::InvalidPostcondition(format!(
            "invalid NU7 baseline at {seed_height:?}: {error}"
        ))
    })?;

    Ok(historical - i128::from(i64::from(network.initial_nsm_value_balance())))
}

fn eligible_balance(
    network: &zakura_chain::parameters::Network,
    height: Height,
    pools: ValueBalance<NonNegative>,
    baseline: i128,
) -> Result<Amount<NegativeAllowed>, FormatChangeError> {
    // The seed lands on the last block below NU7, so records below that carry zero.
    let seeded = NetworkUpgrade::Nu7
        .activation_height(network)
        .is_some_and(|start| height.0 >= start.0.saturating_sub(1));

    if !seeded {
        return Ok(Amount::zero());
    }
    let eligible = balance_at(network, height, pools)
        .and_then(|raw| {
            let eligible = i64::try_from(raw - baseline)
                .map_err(|_| zakura_chain::parameters::subsidy::SubsidyError::Overflow)?;
            Ok(Amount::<NegativeAllowed>::try_from(eligible)?)
        })
        .map_err(|error| {
            FormatChangeError::InvalidPostcondition(format!(
                "invalid NSM value balance at {height:?}: {error}"
            ))
        })?;
    if zakura_chain::parameters::subsidy::is_zip234_active(network, height)
        && i64::from(eligible) < 0
    {
        return Err(FormatChangeError::InvalidPostcondition(format!(
            "negative NSM value balance at active reissuance height {height:?}"
        )));
    }
    Ok(eligible)
}

fn read_tip_pools(db: &ZakuraDb) -> Result<ValueBalance<NonNegative>, FormatChangeError> {
    let bytes = db
        .raw_chain_value_pools_cf()
        .zs_get(&())
        .ok_or_else(|| FormatChangeError::InvalidPostcondition("missing tip value pools".into()))?;
    let pools = ValueBalance::<NonNegative>::from_bytes(&bytes.0).map_err(|error| {
        FormatChangeError::InvalidPostcondition(format!("invalid tip value pools: {error}"))
    })?;
    pools.total().map_err(|error| {
        FormatChangeError::InvalidPostcondition(format!("invalid tip pool total: {error}"))
    })?;
    Ok(pools)
}

fn read_block_info(db: &ZakuraDb, height: Height) -> Result<BlockInfo, FormatChangeError> {
    let bytes = db.raw_block_info_cf().zs_get(&height).ok_or_else(|| {
        FormatChangeError::InvalidPostcondition(format!("missing BlockInfo at {height:?}"))
    })?;
    let pool_len = match bytes.0.len() {
        44 => 40,
        52 => 48,
        60 => 56,
        length => {
            return Err(FormatChangeError::InvalidPostcondition(format!(
                "invalid BlockInfo length {length} at {height:?}"
            )))
        }
    };
    let pools = ValueBalance::<NonNegative>::from_bytes(&bytes.0[..pool_len]).map_err(|error| {
        FormatChangeError::InvalidPostcondition(format!(
            "invalid BlockInfo pools at {height:?}: {error}"
        ))
    })?;
    pools.total().map_err(|error| {
        FormatChangeError::InvalidPostcondition(format!(
            "invalid pool total at {height:?}: {error}"
        ))
    })?;
    let size = u32::from_le_bytes([
        bytes.0[pool_len],
        bytes.0[pool_len + 1],
        bytes.0[pool_len + 2],
        bytes.0[pool_len + 3],
    ]);
    Ok(BlockInfo::new(pools, size))
}

fn check_cancelled(
    cancel_receiver: &Receiver<CancelFormatChange>,
) -> Result<(), CancelFormatChange> {
    match cancel_receiver.try_recv() {
        Err(TryRecvError::Empty) => Ok(()),
        _ => Err(CancelFormatChange),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use zakura_chain::parameters::subsidy::expected_issued_supply;
    use zakura_chain::{amount::MAX_MONEY, parameters::Network};

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        #[test]
        fn backfill_matches_signed_definition(
            height in 0u32..4_000_000,
            issued in 0i64..=MAX_MONEY,
        ) {
            let network = Network::Mainnet;
            let pools = ValueBalance::from_transparent_amount(Amount::try_from(issued).unwrap());
            let expected = i64::from(expected_issued_supply(Height(height), &network).unwrap()) - issued;
            prop_assert_eq!(
                balance_at(&network, Height(height), pools),
                Ok(i128::from(expected)),
            );
        }
    }

    #[test]
    fn backfill_subtracts_the_baseline_before_amount_limits() {
        use zakura_chain::parameters::{
            subsidy::halving_block_subsidy,
            testnet::{ConfiguredActivationHeights, Parameters},
        };
        let start = Height(20_000_000);
        let network = Parameters::build()
            .with_activation_heights(ConfiguredActivationHeights {
                blossom: Some(1),
                canopy: Some(2),
                nu7: Some(start.0),
                ..Default::default()
            })
            .unwrap()
            .clear_funding_streams()
            .to_network()
            .unwrap();
        let baseline = i128::try_from(
            scheduled_issuance_zatoshis(start.previous().unwrap(), &network).unwrap(),
        )
        .unwrap();
        assert!(
            baseline > i128::from(MAX_MONEY),
            "the fixture must exceed the Amount limit"
        );
        let eligible = eligible_balance(&network, start, ValueBalance::zero(), baseline).unwrap();
        assert_eq!(eligible, halving_block_subsidy(start, &network).unwrap());
    }

    #[test]
    fn eligible_negative_balance_follows_reissuance_activation() {
        use zakura_chain::parameters::testnet::{ConfiguredActivationHeights, RegtestParameters};
        let network = Network::new_regtest(RegtestParameters {
            activation_heights: ConfiguredActivationHeights {
                nu7: Some(2),
                ..Default::default()
            },
            nsm_reissuance_height: Some(Height(3)),
            ..Default::default()
        });
        let baseline =
            i128::try_from(scheduled_issuance_zatoshis(Height(1), &network).unwrap()).unwrap();
        for h in [2, 3] {
            let scheduled =
                i128::try_from(scheduled_issuance_zatoshis(Height(h), &network).unwrap()).unwrap();
            let pools = ValueBalance::from_transparent_amount(
                Amount::try_from(i64::try_from(scheduled - baseline + 1).unwrap()).unwrap(),
            );
            let result = eligible_balance(&network, Height(h), pools, baseline);
            if cfg!(feature = "nu7") && h == 3 {
                assert!(matches!(
                    result,
                    Err(FormatChangeError::InvalidPostcondition(_))
                ));
            } else {
                assert_eq!(i64::from(result.unwrap()), -1);
            }
        }
    }

    #[test]
    fn backfill_seeds_the_last_block_below_nu7() {
        use zakura_chain::parameters::testnet::{ConfiguredActivationHeights, RegtestParameters};

        const SEED: i64 = 1_234_567;

        let network = Network::new_regtest(RegtestParameters {
            activation_heights: ConfiguredActivationHeights {
                nu7: Some(4),
                ..Default::default()
            },
            nsm_reissuance_height: Some(Height(20_000)),
            initial_nsm_value_balance: Some(Amount::try_from(SEED).unwrap()),
            ..Default::default()
        });

        // A chain that claimed none of its subsidy, so the whole schedule is unclaimed.
        let pools = ValueBalance::zero();
        let seed_height = Height(3);
        let baseline = balance_at(&network, seed_height, pools).unwrap() - i128::from(SEED);

        assert_eq!(
            i64::from(eligible_balance(&network, seed_height, pools, baseline).unwrap()),
            SEED,
            "the block below NU7 must hold exactly the seed",
        );
        assert_eq!(
            i64::from(eligible_balance(&network, Height(2), pools, baseline).unwrap()),
            0,
            "earlier blocks must hold nothing",
        );

        let scheduled_since = i128::try_from(
            scheduled_issuance_zatoshis(Height(4), &network).unwrap()
                - scheduled_issuance_zatoshis(seed_height, &network).unwrap(),
        )
        .unwrap();
        assert_eq!(
            i128::from(i64::from(
                eligible_balance(&network, Height(4), pools, baseline).unwrap()
            )),
            i128::from(SEED) + scheduled_since,
            "NU7 must keep the seed and add what its own block left unclaimed",
        );
    }

    #[test]
    fn backfill_preserves_negative_balance() {
        let network = Network::Mainnet;
        let height = Height(1);
        let expected = expected_issued_supply(height, &network).unwrap();
        let pools = ValueBalance::from_transparent_amount(
            Amount::try_from(i64::from(expected) + 1).unwrap(),
        );
        assert_eq!(balance_at(&network, height, pools), Ok(-1));
    }
}

#[cfg(test)]
mod database_tests {
    use super::*;
    use crate::{
        constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
        service::finalized_state::{
            disk_format::RawBytes, TypedColumnFamily, STATE_COLUMN_FAMILIES_IN_CODE,
        },
        Config,
    };
    use zakura_chain::parameters::subsidy::expected_issued_supply;
    use zakura_chain::{
        block,
        parameters::{
            testnet::{ConfiguredActivationHeights, RegtestParameters},
            Network,
        },
    };

    fn legacy_db(rows: u32, pool_len: usize) -> ZakuraDb {
        legacy_db_with_seed(rows, pool_len, 0)
    }

    fn legacy_db_with_seed(rows: u32, pool_len: usize, seed: i64) -> ZakuraDb {
        let network = Network::new_regtest(RegtestParameters {
            activation_heights: ConfiguredActivationHeights {
                nu7: Some(2),
                ..Default::default()
            },
            nsm_reissuance_height: Some(Height(20_000)),
            initial_nsm_value_balance: Some(Amount::try_from(seed).unwrap()),
            ..Default::default()
        });
        let db = ZakuraDb::new(
            &Config::ephemeral(),
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &network,
            true,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            false,
        )
        .unwrap();
        let mut batch = DiskWriteBatch::new();
        for h in 0..rows {
            let mut bytes = vec![0; pool_len];
            bytes[..8].copy_from_slice(&i64::from(h).to_le_bytes());
            bytes.extend_from_slice(&(100 + h).to_le_bytes());
            let _ = db
                .raw_block_info_cf()
                .with_batch_for_writing(&mut batch)
                .zs_insert(&Height(h), &RawBytes(bytes));
        }
        let tip = Height(rows - 1);
        let disk = db.header_chain_disk_db();
        let _ = TypedColumnFamily::<Height, block::Hash>::new(&disk, "hash_by_height")
            .unwrap()
            .with_batch_for_writing(&mut batch)
            .zs_insert(&tip, &block::Hash([0; 32]));
        let mut bytes = vec![0; pool_len];
        bytes[..8].copy_from_slice(&i64::from(tip.0).to_le_bytes());
        let _ = db
            .raw_chain_value_pools_cf()
            .with_batch_for_writing(&mut batch)
            .zs_insert(&(), &RawBytes(bytes));
        db.write_batch(batch).unwrap();
        db
    }

    fn assert_upgraded(db: &ZakuraDb, rows: u32) {
        let base = i64::from(expected_issued_supply(Height(1), &db.network()).unwrap()) - 1;
        for h in 0..rows {
            let info = read_block_info(db, Height(h)).unwrap();
            assert_eq!(info.size(), 100 + h);
            assert_eq!(i64::from(info.value_pools().issued_supply()), i64::from(h));
            let expected = if h < 2 {
                0
            } else {
                i64::from(expected_issued_supply(Height(h), &db.network()).unwrap())
                    - i64::from(h)
                    - base
            };
            assert_eq!(
                i64::from(info.value_pools().nsm_value_balance_amount()),
                expected
            );
        }
        let (_tx, rx) = crossbeam_channel::bounded(1);
        assert!(Upgrade.validate(db, &rx).unwrap().is_ok());
    }

    #[test]
    fn migration_seeds_the_last_block_below_nu7() {
        const SEED: i64 = 1_234_567;
        const ROWS: u32 = 6;

        let db = legacy_db_with_seed(ROWS, 48, SEED);
        let (_tx, rx) = crossbeam_channel::bounded(1);
        Upgrade.run(Some(Height(ROWS - 1)), &db, &rx).unwrap();

        let stored = |height: u32| {
            i64::from(
                read_block_info(&db, Height(height))
                    .unwrap()
                    .value_pools()
                    .nsm_value_balance_amount(),
            )
        };

        assert_eq!(stored(0), 0, "genesis is below the seed height");
        assert_eq!(stored(1), SEED, "the block below NU7 holds the seed");

        // The fixture issues one zatoshi per block, so each block from NU7 adds its whole
        // scheduled subsidy less that zatoshi.
        let seeded_supply = i64::from(expected_issued_supply(Height(1), &db.network()).unwrap());
        for height in 2..ROWS {
            let scheduled =
                i64::from(expected_issued_supply(Height(height), &db.network()).unwrap());
            assert_eq!(
                stored(height),
                SEED + scheduled - seeded_supply - (i64::from(height) - 1),
                "at height {height}",
            );
        }

        assert!(Upgrade.validate(&db, &rx).unwrap().is_ok());
    }

    #[test]
    fn migration_batch_boundaries_retry_and_idempotence() {
        for (rows, pool_len) in [(9_999, 40), (10_000, 48), (10_001, 48)] {
            for fail_write in [0, 1] {
                let db = legacy_db(rows, pool_len);
                let (_tx, rx) = crossbeam_channel::bounded(1);
                let mut writes = 0;
                let result = backfill(Some(Height(rows - 1)), &db, &rx, |batch| {
                    let current = writes;
                    writes += 1;
                    if current == fail_write {
                        return Err(FormatChangeError::MigrationStorage(
                            "injected failure".into(),
                        ));
                    }
                    db.write_batch(batch).unwrap();
                    Ok(())
                });
                if fail_write == 0 || rows >= 10_000 {
                    assert!(matches!(
                        result,
                        Err(FormatChangeError::MigrationStorage(_))
                    ));
                }
                Upgrade.run(Some(Height(rows - 1)), &db, &rx).unwrap();
                assert_upgraded(&db, rows);
                Upgrade.run(Some(Height(rows - 1)), &db, &rx).unwrap();
                assert_upgraded(&db, rows);
            }
        }
    }

    #[test]
    fn migration_cancellation_between_batches_is_retryable() {
        let db = legacy_db(10_001, 48);
        let (tx, rx) = crossbeam_channel::bounded(1);
        let result = backfill(Some(Height(10_000)), &db, &rx, |batch| {
            db.write_batch(batch).unwrap();
            tx.send(CancelFormatChange).unwrap();
            Ok(())
        });
        assert!(matches!(result, Err(FormatChangeError::Cancelled)));
        Upgrade.run(Some(Height(10_000)), &db, &rx).unwrap();
        assert_upgraded(&db, 10_001);
    }

    #[test]
    fn cancellation_after_tip_write_prevents_validation_and_allows_retry() {
        let db = legacy_db(4, 48);
        let (tx, rx) = crossbeam_channel::bounded(1);
        backfill(Some(Height(3)), &db, &rx, |batch| {
            db.write_batch(batch).unwrap();
            tx.send(CancelFormatChange).unwrap();
            Ok(())
        })
        .unwrap();
        assert!(matches!(
            Upgrade.validate(&db, &rx),
            Err(FormatChangeError::Cancelled)
        ));
        Upgrade.run(Some(Height(3)), &db, &rx).unwrap();
        assert_upgraded(&db, 4);
    }

    #[test]
    fn migration_rejects_corrupt_records() {
        for bytes in [vec![], vec![0; 51], vec![255; 52]] {
            let db = legacy_db(4, 48);
            let mut batch = DiskWriteBatch::new();
            let _ = db
                .raw_block_info_cf()
                .with_batch_for_writing(&mut batch)
                .zs_insert(&Height(2), &RawBytes(bytes));
            db.write_batch(batch).unwrap();
            let (_tx, rx) = crossbeam_channel::bounded(1);
            assert!(matches!(
                Upgrade.run(Some(Height(3)), &db, &rx),
                Err(FormatChangeError::InvalidPostcondition(_))
            ));
            assert_eq!(
                db.finalized_value_pool().nsm_value_balance_amount(),
                Amount::<NegativeAllowed>::zero()
            );
        }
    }
}
