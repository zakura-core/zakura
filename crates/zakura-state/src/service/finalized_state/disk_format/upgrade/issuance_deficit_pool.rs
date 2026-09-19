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
            block_subsidy, funding_stream_values, scheduled_issuance_zatoshis,
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
        // The periodic check runs while blocks commit. A commit between the reads below
        // would pair the new tip pools with the old tip's BlockInfo, so retry until the tip
        // height is the same before and after the reads. The finalized tip only moves
        // forward, so an unchanged height means no commit landed in between.
        let (tip_height, tip_pools, tip_info) = loop {
            check_cancelled(cancel_receiver)?;
            let Some(tip_height) = db.finalized_tip_height() else {
                return Ok(Ok(()));
            };
            let tip_pools = read_tip_pools(db)?;
            let tip_info = read_block_info(db, tip_height)?;
            if db.finalized_tip_height() == Some(tip_height) {
                break (tip_height, tip_pools, tip_info);
            }
        };

        let network = db.network();
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

    check_cancelled(cancel_receiver)?;
    let network = db.network();
    let Some(start) = NetworkUpgrade::Nu7.activation_height(&network) else {
        return Ok(());
    };
    if tip_height < start {
        return Ok(());
    }

    // Legacy records already decode with the required zero pre-NU7 deficit.
    // Only the baseline immediately before activation is needed from that history.
    let baseline = baseline(db)?;
    let mut previous_deferred = if start == Height(0) {
        Amount::zero()
    } else {
        // The zero-height case above excludes subtraction underflow.
        read_block_info(db, Height(start.0 - 1))?
            .value_pools()
            .deferred_amount()
    };
    let mut batch = DiskWriteBatch::new();
    let mut batched = 0;

    for height in start.0..=tip_height.0 {
        check_cancelled(cancel_receiver)?;

        let height = Height(height);
        let block_info = read_block_info(db, height)?;
        let deferred = block_info.value_pools().deferred_amount();
        validate_deferred_change(&network, height, previous_deferred, deferred)?;
        previous_deferred = deferred;

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

/// Check the monetary changes used by NSM without auditing the pre-NU7 balance.
/// A constant historical offset cancels, but a transition from replayed to normally
/// committed records can change that offset and must not become an NSM contribution.
fn validate_deferred_change(
    network: &Network,
    height: Height,
    previous: Amount<NonNegative>,
    current: Amount<NonNegative>,
) -> Result<(), FormatChangeError> {
    let expected = if height == Height(0) {
        // Genesis does not contribute to the monetary pools.
        0
    } else {
        let invalid = |error: &dyn std::fmt::Display| {
            FormatChangeError::InvalidPostcondition(format!(
                "invalid Deferred funding at {height:?}: {error}"
            ))
        };
        let subsidy = block_subsidy(height, network).map_err(|error| invalid(&error))?;
        let funding = funding_stream_values(height, network, subsidy)
            .map_err(|error| invalid(&error))?
            .remove(&FundingStreamReceiver::Deferred)
            .unwrap_or_default();
        // Differences of two nonnegative Amounts fit in i64, including disbursements.
        i64::from(funding) - i64::from(network.lockbox_disbursement_total_amount(height))
    };
    let stored = i64::from(current) - i64::from(previous);
    if stored != expected {
        return Err(FormatChangeError::InvalidPostcondition(format!(
            "Deferred balance change at {height:?} is {stored} zatoshi, expected {expected}; \
             inconsistent monetary records must be repaired or resynced before NSM backfill"
        )));
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
        // Later formats may append fields, as `BlockInfo::from_bytes` allows.
        60.. => 56,
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
    use zakura_chain::parameters::subsidy::{
        expected_issued_supply, funding_stream_values, halving_block_subsidy, FundingStreamReceiver,
    };
    use zakura_chain::{
        block,
        parameters::{
            testnet::{ConfiguredActivationHeights, RegtestParameters},
            Network,
        },
    };

    fn legacy_db(rows: u32, pool_len: usize) -> ZakuraDb {
        legacy_db_on(accounting_network(), rows, pool_len)
    }

    fn accounting_network() -> Network {
        Network::new_regtest(RegtestParameters {
            activation_heights: ConfiguredActivationHeights {
                nu7: Some(2),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    fn legacy_db_on(network: Network, rows: u32, pool_len: usize) -> ZakuraDb {
        legacy_db_with_config(network, rows, pool_len, &Config::ephemeral())
    }

    fn legacy_db_with_config(
        network: Network,
        rows: u32,
        pool_len: usize,
        config: &Config,
    ) -> ZakuraDb {
        let db = ZakuraDb::new(
            config,
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
        if rows == 0 {
            return db;
        }
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
    fn empty_migration_does_not_write() {
        let db = legacy_db(0, 48);
        let (_tx, rx) = crossbeam_channel::bounded(1);
        backfill(None, &db, &rx, |_| panic!("empty migration must not write")).unwrap();
        assert!(Upgrade.validate(&db, &rx).unwrap().is_ok());
    }

    #[test]
    fn migration_before_activation_leaves_legacy_data_unchanged() {
        for network in [Network::Mainnet, accounting_network()] {
            let db = legacy_db_on(network, 2, 48);
            let tip = db.raw_block_info_cf().zs_get(&Height(1)).unwrap().0;
            let pools = db.raw_chain_value_pools_cf().zs_get(&()).unwrap().0;
            // A missing unrelated historical row proves neither run nor validation
            // traverses the pre-activation history.
            let mut batch = DiskWriteBatch::new();
            let _ = db
                .raw_block_info_cf()
                .with_batch_for_writing(&mut batch)
                .zs_delete(&Height(0));
            db.write_batch(batch).unwrap();
            let (tx, rx) = crossbeam_channel::bounded(1);
            backfill(Some(Height(1)), &db, &rx, |_| {
                panic!("pre-NU7 migration must not write")
            })
            .unwrap();
            assert!(Upgrade.validate(&db, &rx).unwrap().is_ok());
            assert_eq!(db.raw_block_info_cf().zs_get(&Height(1)).unwrap().0, tip);
            assert_eq!(db.raw_chain_value_pools_cf().zs_get(&()).unwrap().0, pools);
            tx.send(CancelFormatChange).unwrap();
            assert!(matches!(
                Upgrade.run(Some(Height(1)), &db, &rx),
                Err(FormatChangeError::Cancelled)
            ));
        }
    }

    #[test]
    fn migration_preserves_pre_activation_bytes() {
        for rows in [3, 4] {
            for pool_len in [40, 48, 56] {
                let db = legacy_db(rows, pool_len);
                let before: Vec<_> = (0..2)
                    .map(|h| db.raw_block_info_cf().zs_get(&Height(h)).unwrap().0)
                    .collect();
                let (_tx, rx) = crossbeam_channel::bounded(1);
                Upgrade.run(Some(Height(rows - 1)), &db, &rx).unwrap();
                assert_upgraded(&db, rows);
                for (h, bytes) in before.iter().enumerate() {
                    assert_eq!(
                        &db.raw_block_info_cf()
                            .zs_get(&Height(u32::try_from(h).unwrap()))
                            .unwrap()
                            .0,
                        bytes
                    );
                }
                for h in 2..rows {
                    assert_eq!(
                        db.raw_block_info_cf().zs_get(&Height(h)).unwrap().0.len(),
                        60
                    );
                }
                assert_eq!(
                    db.raw_chain_value_pools_cf().zs_get(&()).unwrap().0.len(),
                    56
                );
            }
        }
    }

    #[test]
    fn migration_does_not_require_rows_before_baseline() {
        let db = legacy_db(4, 48);
        let mut batch = DiskWriteBatch::new();
        let _ = db
            .raw_block_info_cf()
            .with_batch_for_writing(&mut batch)
            .zs_delete(&Height(0));
        db.write_batch(batch).unwrap();
        let (_tx, rx) = crossbeam_channel::bounded(1);
        Upgrade.run(Some(Height(3)), &db, &rx).unwrap();
        assert!(Upgrade.validate(&db, &rx).unwrap().is_ok());
    }

    #[test]
    fn migration_batch_boundaries_retry_and_idempotence() {
        for (affected, pool_len) in [(9_999, 40), (10_000, 48), (10_001, 48)] {
            let rows = affected + 2;
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
                if fail_write == 0 || affected >= BATCH_BLOCKS {
                    assert!(matches!(
                        result,
                        Err(FormatChangeError::MigrationStorage(_))
                    ));
                } else {
                    result.unwrap();
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
        let db = legacy_db(10_003, 48);
        let (tx, rx) = crossbeam_channel::bounded(1);
        let result = backfill(Some(Height(10_002)), &db, &rx, |batch| {
            db.write_batch(batch).unwrap();
            tx.send(CancelFormatChange).unwrap();
            Ok(())
        });
        assert!(matches!(result, Err(FormatChangeError::Cancelled)));
        Upgrade.run(Some(Height(10_002)), &db, &rx).unwrap();
        assert_upgraded(&db, 10_003);
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
    fn pre_nu7_migration_with_large_tip_does_not_scan_history_or_write() {
        use zakura_chain::parameters::testnet::{
            self, ConfiguredFundingStreamRecipient, ConfiguredFundingStreams,
        };
        let tip = Height(8_388_607);
        for nu7 in [None, Some(tip.0 + 1)] {
            for deferred in [false, true] {
                let network = testnet::Parameters::build()
                    .with_slow_start_interval(Height(12_500_000))
                    .with_activation_heights(ConfiguredActivationHeights {
                        blossom: Some(1),
                        canopy: Some(2),
                        nu7,
                        ..Default::default()
                    })
                    .unwrap()
                    .with_funding_streams(vec![ConfiguredFundingStreams {
                        height_range: Some(Height(2)..Height(12_500_001)),
                        recipients: Some(if deferred {
                            vec![ConfiguredFundingStreamRecipient {
                                receiver: FundingStreamReceiver::Deferred,
                                numerator: 12,
                                addresses: None,
                            }]
                        } else {
                            vec![]
                        }),
                    }])
                    .to_network()
                    .unwrap();
                let db = legacy_db_on(network, 2, 48);
                // A sparse accounting fixture: historical rows deliberately do not exist.
                let info = db.raw_block_info_cf().zs_get(&Height(1)).unwrap();
                let pools = db.raw_chain_value_pools_cf().zs_get(&()).unwrap();
                let mut batch = DiskWriteBatch::new();
                let _ = db
                    .raw_block_info_cf()
                    .with_batch_for_writing(&mut batch)
                    .zs_insert(&tip, &info);
                let disk = db.header_chain_disk_db();
                let _ = TypedColumnFamily::<Height, block::Hash>::new(&disk, "hash_by_height")
                    .unwrap()
                    .with_batch_for_writing(&mut batch)
                    .zs_insert(&tip, &block::Hash([0; 32]));
                db.write_batch(batch).unwrap();
                let (_tx, rx) = crossbeam_channel::bounded(1);
                backfill(Some(tip), &db, &rx, |_| {
                    panic!("pre-NU7 migration must not write")
                })
                .unwrap();
                assert!(Upgrade.validate(&db, &rx).unwrap().is_ok());
                assert_eq!(db.raw_block_info_cf().zs_get(&tip).unwrap().0, info.0);
                assert_eq!(
                    db.raw_chain_value_pools_cf().zs_get(&()).unwrap().0,
                    pools.0
                );
            }
        }
    }

    fn slow_start_deferred_network(nu7: Option<u32>) -> Network {
        slow_start_deferred_network_with_disbursement(nu7, 1_000_000)
    }

    fn slow_start_deferred_network_with_disbursement(
        nu7: Option<u32>,
        disbursement: u64,
    ) -> Network {
        use zakura_chain::parameters::testnet::{
            self, ConfiguredFundingStreamRecipient, ConfiguredFundingStreams,
            ConfiguredLockboxDisbursement,
        };
        testnet::Parameters::build()
            .with_slow_start_interval(Height(8))
            .with_activation_heights(ConfiguredActivationHeights {
                blossom: Some(1),
                canopy: Some(2),
                nu6_1: Some(3),
                nu7,
                ..Default::default()
            })
            .unwrap()
            .with_lockbox_disbursements(vec![ConfiguredLockboxDisbursement {
                address: "t26ovBdKAJLtrvBsE2QGF4nqBkEuptuPFZz".to_string(),
                amount: Amount::try_from(disbursement).unwrap(),
            }])
            .with_funding_streams(vec![ConfiguredFundingStreams {
                height_range: Some(Height(2)..Height(100)),
                recipients: Some(vec![ConfiguredFundingStreamRecipient {
                    receiver: FundingStreamReceiver::Deferred,
                    numerator: 12,
                    addresses: None,
                }]),
            }])
            .to_network()
            .unwrap()
    }

    /// Write the legacy balances produced by normal commits or a corrected replay.
    fn healthy_deferred_db(network: Network, rows: u32, pool_len: usize) -> ZakuraDb {
        let db = legacy_db_on(network, rows, pool_len);
        populate_deferred_balances(&db, rows);
        db
    }

    fn populate_deferred_balances(db: &ZakuraDb, rows: u32) {
        let network = db.network();
        let mut balance = Amount::<NonNegative>::zero();
        let mut batch = DiskWriteBatch::new();
        for h in 1..rows {
            let height = Height(h);
            let funding = funding_stream_values(
                height,
                &network,
                halving_block_subsidy(height, &network).unwrap(),
            )
            .unwrap()
            .remove(&FundingStreamReceiver::Deferred)
            .unwrap_or_default();
            balance =
                (balance + funding - network.lockbox_disbursement_total_amount(height)).unwrap();
            let mut bytes = db.raw_block_info_cf().zs_get(&height).unwrap();
            bytes.0[32..40].copy_from_slice(&balance.to_bytes());
            let _ = db
                .raw_block_info_cf()
                .with_batch_for_writing(&mut batch)
                .zs_insert(&height, &bytes);
        }
        let mut bytes = db.raw_chain_value_pools_cf().zs_get(&()).unwrap();
        bytes.0[32..40].copy_from_slice(&balance.to_bytes());
        let _ = db
            .raw_chain_value_pools_cf()
            .with_batch_for_writing(&mut batch)
            .zs_insert(&(), &bytes);
        db.write_batch(batch).unwrap();
    }

    #[test]
    fn migration_accepts_healthy_slow_start_deferred_history() {
        for nu7 in [None, Some(20), Some(4)] {
            for pool_len in [40, 48] {
                // Cover the first payment, disbursement, NU7, and slow-start boundary.
                for tip in [1, 2, 3, 4, 7, 8, 10] {
                    let db =
                        healthy_deferred_db(slow_start_deferred_network(nu7), tip + 1, pool_len);
                    let before = db.raw_chain_value_pools_cf().zs_get(&()).unwrap();
                    let (_tx, rx) = crossbeam_channel::bounded(1);
                    let mut writes = 0;
                    backfill(Some(Height(tip)), &db, &rx, |batch| {
                        writes += 1;
                        db.write_batch(batch).unwrap();
                        Ok(())
                    })
                    .unwrap();
                    assert!(Upgrade.validate(&db, &rx).unwrap().is_ok());
                    let after = db.raw_chain_value_pools_cf().zs_get(&()).unwrap();
                    assert_eq!(&after.0[..pool_len], &before.0);
                    if nu7.is_none_or(|start| tip < start) {
                        assert_eq!(writes, 0, "inactive NU7 requires no backfill writes");
                        assert_eq!(after.0, before.0);
                    }
                    Upgrade.run(Some(Height(tip)), &db, &rx).unwrap();
                    assert!(Upgrade.validate(&db, &rx).unwrap().is_ok());
                }
            }
        }
    }

    #[test]
    fn migration_only_needs_deferred_records_from_the_nu7_baseline() {
        let db = healthy_deferred_db(slow_start_deferred_network(Some(4)), 11, 48);
        let baseline_total = i64::from(
            read_block_info(&db, Height(3))
                .unwrap()
                .value_pools()
                .total()
                .unwrap(),
        );
        let tip_total = i64::from(read_tip_pools(&db).unwrap().total().unwrap());
        let scheduled_since_nu7: i64 = (4..=10)
            .map(|h| i64::from(halving_block_subsidy(Height(h), &db.network()).unwrap()))
            .sum();
        let mut batch = DiskWriteBatch::new();
        // This row contains a slow-start Deferred payment, but predates the baseline.
        let _ = db
            .raw_block_info_cf()
            .with_batch_for_writing(&mut batch)
            .zs_delete(&Height(2));
        db.write_batch(batch).unwrap();
        let (_tx, rx) = crossbeam_channel::bounded(1);
        Upgrade.run(Some(Height(10)), &db, &rx).unwrap();
        assert_eq!(
            i64::from(read_tip_pools(&db).unwrap().issuance_deficit_amount()),
            scheduled_since_nu7 - (tip_total - baseline_total)
        );
        assert!(Upgrade.validate(&db, &rx).unwrap().is_ok());
    }

    #[test]
    fn constant_historical_deferred_offset_does_not_change_the_deficit() {
        let network = slow_start_deferred_network(Some(4));
        let healthy = healthy_deferred_db(network.clone(), 11, 48);
        let offset = healthy_deferred_db(network, 11, 48);
        let mut batch = DiskWriteBatch::new();
        // Model an old missed payment carried unchanged through the baseline and later rows.
        // Format 29 excludes that constant offset; it does not repair monetary balances.
        for h in 2..=10 {
            let mut bytes = offset.raw_block_info_cf().zs_get(&Height(h)).unwrap();
            let balance = i64::from_le_bytes(bytes.0[32..40].try_into().unwrap());
            bytes.0[32..40].copy_from_slice(&(balance - 1).to_le_bytes());
            let _ = offset
                .raw_block_info_cf()
                .with_batch_for_writing(&mut batch)
                .zs_insert(&Height(h), &bytes);
        }
        let mut pools = offset.raw_chain_value_pools_cf().zs_get(&()).unwrap();
        let balance = i64::from_le_bytes(pools.0[32..40].try_into().unwrap());
        pools.0[32..40].copy_from_slice(&(balance - 1).to_le_bytes());
        let _ = offset
            .raw_chain_value_pools_cf()
            .with_batch_for_writing(&mut batch)
            .zs_insert(&(), &pools);
        offset.write_batch(batch).unwrap();
        let (_tx, rx) = crossbeam_channel::bounded(1);
        for db in [&healthy, &offset] {
            Upgrade.run(Some(Height(10)), db, &rx).unwrap();
            assert!(Upgrade.validate(db, &rx).unwrap().is_ok());
        }
        for h in 4..=10 {
            assert_eq!(
                read_block_info(&healthy, Height(h))
                    .unwrap()
                    .value_pools()
                    .issuance_deficit_amount(),
                read_block_info(&offset, Height(h))
                    .unwrap()
                    .value_pools()
                    .issuance_deficit_amount()
            );
        }
        assert_eq!(
            read_tip_pools(&healthy).unwrap().issuance_deficit_amount(),
            read_tip_pools(&offset).unwrap().issuance_deficit_amount()
        );
        assert_eq!(
            &offset.raw_chain_value_pools_cf().zs_get(&()).unwrap().0[..48],
            &pools.0
        );
    }

    #[test]
    fn migration_rejects_changing_deferred_offsets() {
        for bad_height in [3, 4, 7, 10] {
            let db = healthy_deferred_db(slow_start_deferred_network(Some(4)), 11, 48);
            let height = Height(bad_height);
            let mut bytes = db.raw_block_info_cf().zs_get(&height).unwrap();
            bytes.0[32..40].fill(0);
            let mut batch = DiskWriteBatch::new();
            let _ = db
                .raw_block_info_cf()
                .with_batch_for_writing(&mut batch)
                .zs_insert(&height, &bytes);
            if bad_height == 10 {
                // Keep both tip records consistent: tip agreement alone cannot catch this.
                let mut pools = db.raw_chain_value_pools_cf().zs_get(&()).unwrap();
                pools.0[32..40].fill(0);
                let _ = db
                    .raw_chain_value_pools_cf()
                    .with_batch_for_writing(&mut batch)
                    .zs_insert(&(), &pools);
            }
            db.write_batch(batch).unwrap();
            let (_tx, rx) = crossbeam_channel::bounded(1);
            let error = backfill(Some(Height(10)), &db, &rx, |_| {
                panic!("inconsistent Deferred changes in the first batch must not be written")
            })
            .unwrap_err();
            assert!(
                matches!(error, FormatChangeError::InvalidPostcondition(ref message)
                if message.contains("Deferred balance change"))
            );
        }
    }

    #[test]
    fn mixed_replay_and_commit_history_cannot_advance_format_version() {
        use super::super::DbFormatChange;

        let tempdir = tempfile::tempdir().unwrap();
        let config = Config {
            cache_dir: tempdir.path().to_owned(),
            ephemeral: false,
            ..Config::default()
        };
        let db = legacy_db_with_config(slow_start_deferred_network(Some(4)), 5, 48, &config);
        populate_deferred_balances(&db, 5);
        let old_version = Version::new(28, 1, 5);
        let running_version = state_database_format_version_in_code();
        db.update_format_version_on_disk(&old_version).unwrap();
        let baseline = db.raw_block_info_cf().zs_get(&Height(3)).unwrap();
        let mut old_replay_baseline = baseline.clone();
        old_replay_baseline.0[32..40].fill(0);
        let mut batch = DiskWriteBatch::new();
        let _ = db
            .raw_block_info_cf()
            .with_batch_for_writing(&mut batch)
            .zs_insert(&Height(3), &old_replay_baseline);
        db.write_batch(batch).unwrap();
        let original_tip = db.raw_block_info_cf().zs_get(&Height(4)).unwrap();
        let original_pools = db.raw_chain_value_pools_cf().zs_get(&()).unwrap();
        let (_tx, rx) = crossbeam_channel::bounded(1);
        let upgrade = DbFormatChange::open_database(&running_version, Some(old_version.clone()));
        for _ in 0..2 {
            let error = upgrade
                .apply_format_upgrade(&db, Some(Height(4)), &rx)
                .unwrap_err();
            assert!(
                matches!(error, FormatChangeError::InvalidPostcondition(ref message)
                if message.contains("Deferred balance change"))
            );
            assert_eq!(
                db.format_version_on_disk().unwrap(),
                Some(old_version.clone())
            );
            assert_eq!(
                db.raw_block_info_cf().zs_get(&Height(4)).unwrap().0,
                original_tip.0
            );
            assert_eq!(
                db.raw_chain_value_pools_cf().zs_get(&()).unwrap().0,
                original_pools.0
            );
        }
        // Simulate a separate repair; retrying the migration must then succeed.
        let mut batch = DiskWriteBatch::new();
        let _ = db
            .raw_block_info_cf()
            .with_batch_for_writing(&mut batch)
            .zs_insert(&Height(3), &baseline);
        db.write_batch(batch).unwrap();
        upgrade
            .apply_format_upgrade(&db, Some(Height(4)), &rx)
            .unwrap();
        assert_eq!(db.format_version_on_disk().unwrap(), Some(running_version));
        assert!(Upgrade.validate(&db, &rx).unwrap().is_ok());
    }

    #[test]
    fn migration_accepts_a_deferred_disbursement_after_the_baseline() {
        let network = slow_start_deferred_network_with_disbursement(Some(3), 70_000_000);
        let db = healthy_deferred_db(network, 5, 48);
        let before = read_block_info(&db, Height(2))
            .unwrap()
            .value_pools()
            .deferred_amount();
        let after = read_block_info(&db, Height(3))
            .unwrap()
            .value_pools()
            .deferred_amount();
        assert!(
            after < before,
            "disbursement exceeds this block's Deferred funding"
        );
        let (_tx, rx) = crossbeam_channel::bounded(1);
        Upgrade.run(Some(Height(4)), &db, &rx).unwrap();
        assert!(Upgrade.validate(&db, &rx).unwrap().is_ok());
    }

    #[test]
    fn healthy_deferred_history_advances_format_version() {
        use super::super::DbFormatChange;

        let tempdir = tempfile::tempdir().unwrap();
        let config = Config {
            cache_dir: tempdir.path().to_owned(),
            ephemeral: false,
            ..Config::default()
        };
        let db = legacy_db_with_config(slow_start_deferred_network(None), 11, 48, &config);
        populate_deferred_balances(&db, 11);
        let old_version = Version::new(28, 1, 5);
        let running_version = state_database_format_version_in_code();
        db.update_format_version_on_disk(&old_version).unwrap();
        let (_tx, rx) = crossbeam_channel::bounded(1);
        DbFormatChange::open_database(&running_version, Some(old_version))
            .apply_format_upgrade(&db, Some(Height(10)), &rx)
            .unwrap();
        assert_eq!(
            db.format_version_on_disk().unwrap(),
            Some(running_version.clone())
        );
        let network = db.network();
        drop(db);
        let db = ZakuraDb::new(
            &config,
            STATE_DATABASE_KIND,
            &running_version,
            &network,
            // The fixture contains accounting rows, not a complete chain. Exercise
            // format selection and this migration's validation directly below.
            true,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            false,
        )
        .unwrap();
        assert!(matches!(
            DbFormatChange::open_database(&running_version, db.format_version_on_disk().unwrap()),
            DbFormatChange::CheckOpenCurrent { .. }
        ));
        assert!(Upgrade.validate(&db, &rx).unwrap().is_ok());
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
        for missing in [1, 2, 3, 4] {
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
