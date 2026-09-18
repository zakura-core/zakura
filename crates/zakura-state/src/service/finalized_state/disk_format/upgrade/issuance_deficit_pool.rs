//! Backfill the unseeded reissuance balance from NU7 onward.
//!
//! The balance equals the signed schedule deficit minus its value immediately before
//! NU7. Earlier records carry zero. Historical funds remain excluded pending guidance.

use crossbeam_channel::{Receiver, TryRecvError};
use semver::Version;

use zakura_chain::{
    amount::{Amount, NegativeAllowed, NonNegative},
    block::Height,
    block_info::BlockInfo,
    parameters::{
        subsidy::{
            funding_stream_values, halving_block_subsidy, scheduled_issuance_zatoshis,
            FundingStreamReceiver,
        },
        Network, NetworkUpgrade,
    },
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
        Version::new(29, 0, 0)
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
        let expected = eligible_deficit(&network, tip_height, tip_pools, baseline(db)?)?;

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

fn backfill(
    initial_finalized_tip_height: Option<Height>,
    db: &ZakuraDb,
    cancel_receiver: &Receiver<CancelFormatChange>,
    mut write: impl FnMut(DiskWriteBatch) -> Result<(), FormatChangeError>,
) -> Result<(), FormatChangeError> {
    let Some(tip_height) = initial_finalized_tip_height else {
        // An empty database has no blocks to backfill. A genesis sync under this version
        // writes the deficit as it goes.
        return Ok(());
    };

    let network = db.network();
    refuse_unrepairable_history(&network, tip_height)?;
    let baseline = baseline(db)?;
    let mut batch = DiskWriteBatch::new();
    let mut batched = 0;

    for height in 0..=tip_height.0 {
        check_cancelled(cancel_receiver)?;

        let height = Height(height);
        let block_info = read_block_info(db, height)?;

        let deficit = eligible_deficit(&network, height, *block_info.value_pools(), baseline)?;

        let mut value_pools = *block_info.value_pools();
        value_pools.set_issuance_deficit_amount(deficit);
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
    let deficit = eligible_deficit(&network, tip_height, tip_pools, baseline)?;
    let mut tip_pools = tip_pools;
    tip_pools.set_issuance_deficit_amount(deficit);
    let _ = db
        .chain_value_pools_cf()
        .with_batch_for_writing(&mut batch)
        .zs_insert(&(), &tip_pools);

    write(batch)?;

    Ok(())
}

/// Refuses databases whose committed value pools older migrations cannot repair.
///
/// Before format 29, the version 27 replay omitted Deferred funding during slow start.
/// A database that ran that replay can hold undercounted Deferred balances, and its
/// version marker stops the corrected replay from running again. Block commits always
/// applied the funding, so a fresh sync produces the correct balances.
fn refuse_unrepairable_history(network: &Network, tip: Height) -> Result<(), FormatChangeError> {
    let slow_start_end = network.slow_start_interval().min(tip);
    for height in 1..=slow_start_end.0 {
        let height = Height(height);
        let invalid = |error: &dyn std::fmt::Display| {
            FormatChangeError::InvalidPostcondition(format!(
                "invalid funding streams at {height:?}: {error}"
            ))
        };
        let subsidy = halving_block_subsidy(height, network).map_err(|error| invalid(&error))?;
        let deferred = funding_stream_values(height, network, subsidy)
            .map_err(|error| invalid(&error))?
            .remove(&FundingStreamReceiver::Deferred)
            .unwrap_or_default();
        if !deferred.is_zero() {
            return Err(FormatChangeError::ResyncRequired(format!(
                "this network pays Deferred funding during slow start from {height:?}, \
                 and older replays of those blocks did not record it"
            )));
        }
    }
    Ok(())
}

/// Subtract signed operands so pre-reissuance deficits can remain negative.
fn deficit_at(
    network: &zakura_chain::parameters::Network,
    height: Height,
    value_pools: ValueBalance<NonNegative>,
) -> Result<i128, zakura_chain::parameters::subsidy::SubsidyError> {
    let scheduled = i128::try_from(scheduled_issuance_zatoshis(height, network)?)
        .map_err(|_| zakura_chain::parameters::subsidy::SubsidyError::Overflow)?;
    Ok(scheduled - i128::from(i64::from(value_pools.total()?)))
}

/// Exclude the entire pre-NU7 deficit until the historical-funds policy is confirmed.
/// Keep this baseline consistent with `Block::issuance_deficit_change`.
fn baseline(db: &ZakuraDb) -> Result<i128, FormatChangeError> {
    let network = db.network();
    let Some(start) = NetworkUpgrade::Nu7.activation_height(&network) else {
        return Ok(0);
    };
    if db.finalized_tip_height().is_none_or(|tip| tip < start) || start == Height(0) {
        return Ok(0);
    }
    let height = Height(start.0 - 1); // The zero-height case returned above.
    let info = read_block_info(db, height)?;
    deficit_at(&network, height, *info.value_pools()).map_err(|error| {
        FormatChangeError::InvalidPostcondition(format!(
            "invalid NU7 baseline at {height:?}: {error}"
        ))
    })
}

fn eligible_deficit(
    network: &zakura_chain::parameters::Network,
    height: Height,
    pools: ValueBalance<NonNegative>,
    baseline: i128,
) -> Result<Amount<NegativeAllowed>, FormatChangeError> {
    if !NetworkUpgrade::Nu7
        .activation_height(network)
        .is_some_and(|start| height >= start)
    {
        return Ok(Amount::zero());
    }
    let eligible = deficit_at(network, height, pools)
        .and_then(|raw| {
            let eligible = i64::try_from(raw - baseline)
                .map_err(|_| zakura_chain::parameters::subsidy::SubsidyError::Overflow)?;
            Ok(Amount::<NegativeAllowed>::try_from(eligible)?)
        })
        .map_err(|error| {
            FormatChangeError::InvalidPostcondition(format!(
                "invalid issuance deficit at {height:?}: {error}"
            ))
        })?;
    if zakura_chain::parameters::subsidy::is_zip234_active(network, height)
        && i64::from(eligible) < 0
    {
        return Err(FormatChangeError::InvalidPostcondition(format!(
            "negative issuance deficit at active reissuance height {height:?}"
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
                deficit_at(&network, Height(height), pools),
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
        let eligible = eligible_deficit(&network, start, ValueBalance::zero(), baseline).unwrap();
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
            zip234_start_height: Some(Height(3)),
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
            let result = eligible_deficit(&network, Height(h), pools, baseline);
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
    fn backfill_preserves_negative_deficit() {
        let network = Network::Mainnet;
        let height = Height(1);
        let expected = expected_issued_supply(height, &network).unwrap();
        let pools = ValueBalance::from_transparent_amount(
            Amount::try_from(i64::from(expected) + 1).unwrap(),
        );
        assert_eq!(deficit_at(&network, height, pools), Ok(-1));
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
        let network = Network::new_regtest(RegtestParameters {
            activation_heights: ConfiguredActivationHeights {
                nu7: Some(2),
                ..Default::default()
            },
            ..Default::default()
        });
        legacy_db_on(network, rows, pool_len)
    }

    fn legacy_db_on(network: Network, rows: u32, pool_len: usize) -> ZakuraDb {
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
                i64::from(info.value_pools().issuance_deficit_amount()),
                expected
            );
        }
        let (_tx, rx) = crossbeam_channel::bounded(1);
        assert!(Upgrade.validate(db, &rx).unwrap().is_ok());
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

    /// Older replays omitted Deferred funding during slow start, so a database on a network
    /// that pays it there must sync again.
    #[test]
    fn migration_requires_resync_after_slow_start_deferred_funding() {
        use zakura_chain::parameters::testnet::{
            self, ConfiguredFundingStreamRecipient, ConfiguredFundingStreams,
        };
        let network = testnet::Parameters::build()
            .with_activation_heights(ConfiguredActivationHeights {
                blossom: Some(1),
                canopy: Some(2),
                ..Default::default()
            })
            .unwrap()
            .with_funding_streams(vec![ConfiguredFundingStreams {
                height_range: Some(Height(2)..Height(100)),
                recipients: Some(vec![ConfiguredFundingStreamRecipient {
                    receiver: FundingStreamReceiver::Deferred,
                    numerator: 12,
                    addresses: None,
                }]),
            }])
            .to_network()
            .unwrap();
        assert!(network.slow_start_interval() > Height(3));

        // The tip is below the first Deferred payment, so the history is intact.
        let db = legacy_db_on(network.clone(), 2, 48);
        let (_tx, rx) = crossbeam_channel::bounded(1);
        Upgrade.run(Some(Height(1)), &db, &rx).unwrap();

        let db = legacy_db_on(network, 4, 48);
        let before = db.raw_block_info_cf().zs_get(&Height(3)).unwrap();
        assert!(matches!(
            Upgrade.run(Some(Height(3)), &db, &rx),
            Err(FormatChangeError::ResyncRequired(_))
        ));
        assert_eq!(
            db.raw_block_info_cf().zs_get(&Height(3)).unwrap().0,
            before.0,
            "the refusal leaves the legacy records unchanged"
        );
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
                db.finalized_value_pool().issuance_deficit_amount(),
                Amount::<NegativeAllowed>::zero()
            );
        }
    }
    #[test]
    fn migration_rejects_missing_anchor_rows_and_tip() {
        for missing in [0, 1, 2, 3, 4] {
            let db = legacy_db(4, 48);
            let mut batch = DiskWriteBatch::new();
            if missing == 4 {
                let _ = db
                    .raw_chain_value_pools_cf()
                    .with_batch_for_writing(&mut batch)
                    .zs_delete(&());
            } else {
                let _ = db
                    .raw_block_info_cf()
                    .with_batch_for_writing(&mut batch)
                    .zs_delete(&Height(missing));
            }
            db.write_batch(batch).unwrap();
            let (_tx, rx) = crossbeam_channel::bounded(1);
            assert!(
                matches!(
                    Upgrade.run(Some(Height(3)), &db, &rx),
                    Err(FormatChangeError::InvalidPostcondition(_))
                ),
                "missing {missing}"
            );
        }
    }

    #[test]
    fn migration_cancelled_before_first_write_preserves_legacy_records() {
        let db = legacy_db(4, 48);
        let original = db.raw_block_info_cf().zs_get(&Height(2)).unwrap().0;
        let (tx, rx) = crossbeam_channel::bounded(1);
        tx.send(CancelFormatChange).unwrap();
        let mut writes = 0;
        let result = backfill(Some(Height(3)), &db, &rx, |_| {
            writes += 1;
            Ok(())
        });
        assert!(matches!(result, Err(FormatChangeError::Cancelled)));
        assert_eq!(writes, 0);
        assert_eq!(
            db.raw_block_info_cf().zs_get(&Height(2)).unwrap().0,
            original
        );
        Upgrade.run(Some(Height(3)), &db, &rx).unwrap();
        assert_upgraded(&db, 4);
    }

    #[test]
    fn migration_validates_tip_agreement_and_rejects_invalid_pool_totals() {
        for invalid_total in [false, true] {
            let db = legacy_db(4, 48);
            let (_tx, rx) = crossbeam_channel::bounded(1);
            Upgrade.run(Some(Height(3)), &db, &rx).unwrap();
            let mut bytes = db.raw_chain_value_pools_cf().zs_get(&()).unwrap().0;
            if invalid_total {
                bytes[..8].copy_from_slice(&zakura_chain::amount::MAX_MONEY.to_le_bytes());
                bytes[8..16].copy_from_slice(&1i64.to_le_bytes());
            } else {
                bytes[..8].copy_from_slice(&4i64.to_le_bytes());
            }
            let mut batch = DiskWriteBatch::new();
            let _ = db
                .raw_chain_value_pools_cf()
                .with_batch_for_writing(&mut batch)
                .zs_insert(&(), &RawBytes(bytes));
            db.write_batch(batch).unwrap();
            let result = Upgrade.validate(&db, &rx);
            if invalid_total {
                assert!(result.is_err());
            } else {
                assert!(result.unwrap().is_err());
            }
        }
    }
}
