//! Tests for funding streams.

#![allow(clippy::unwrap_in_result)]

use std::collections::HashMap;

use color_eyre::Report;
use zakura_chain::amount::Amount;
use zakura_chain::parameters::NetworkUpgrade::*;
use zakura_chain::parameters::{subsidy::FundingStreamReceiver, NetworkKind};

use super::*;

/// Checks that the Mainnet funding stream values are correct.
#[test]
fn test_funding_stream_values() -> Result<(), Report> {
    let _init_guard = zakura_test::init();
    let network = &Network::Mainnet;

    let canopy_activation_height = Canopy.activation_height(network).unwrap();
    let nu6_activation_height = Nu6.activation_height(network).unwrap();
    let nu6_1_activation_height = Nu6_1.activation_height(network).unwrap();

    let dev_fund_height_range = network.all_funding_streams()[0].height_range();
    let nu6_fund_height_range = network.all_funding_streams()[1].height_range();
    let nu6_1_fund_height_range = network.all_funding_streams()[2].height_range();

    let nu6_fund_end = Height(3_146_400);
    let nu6_1_fund_end = Height(4_406_400);

    assert_eq!(canopy_activation_height, Height(1_046_400));
    assert_eq!(nu6_activation_height, Height(2_726_400));
    assert_eq!(nu6_1_activation_height, Height(3_146_400));

    assert_eq!(dev_fund_height_range.start, canopy_activation_height);
    assert_eq!(dev_fund_height_range.end, nu6_activation_height);

    assert_eq!(nu6_fund_height_range.start, nu6_activation_height);
    assert_eq!(nu6_fund_height_range.end, nu6_fund_end);

    assert_eq!(nu6_1_fund_height_range.start, nu6_1_activation_height);
    assert_eq!(nu6_1_fund_height_range.end, nu6_1_fund_end);

    assert_eq!(dev_fund_height_range.end, nu6_fund_height_range.start);

    let mut expected_dev_fund = HashMap::new();

    expected_dev_fund.insert(FundingStreamReceiver::Ecc, Amount::try_from(21_875_000)?);
    expected_dev_fund.insert(
        FundingStreamReceiver::ZcashFoundation,
        Amount::try_from(15_625_000)?,
    );
    expected_dev_fund.insert(
        FundingStreamReceiver::MajorGrants,
        Amount::try_from(25_000_000)?,
    );
    let expected_dev_fund = expected_dev_fund;

    let mut expected_nu6_fund = HashMap::new();
    expected_nu6_fund.insert(
        FundingStreamReceiver::Deferred,
        Amount::try_from(18_750_000)?,
    );
    expected_nu6_fund.insert(
        FundingStreamReceiver::MajorGrants,
        Amount::try_from(12_500_000)?,
    );
    let expected_nu6_fund = expected_nu6_fund;

    for height in [
        dev_fund_height_range.start.previous().unwrap(),
        dev_fund_height_range.start,
        dev_fund_height_range.start.next().unwrap(),
        dev_fund_height_range.end.previous().unwrap(),
        dev_fund_height_range.end,
        dev_fund_height_range.end.next().unwrap(),
        nu6_fund_height_range.start.previous().unwrap(),
        nu6_fund_height_range.start,
        nu6_fund_height_range.start.next().unwrap(),
        nu6_fund_height_range.end.previous().unwrap(),
        nu6_fund_height_range.end,
        nu6_fund_height_range.end.next().unwrap(),
        nu6_1_fund_height_range.start.previous().unwrap(),
        nu6_1_fund_height_range.start,
        nu6_1_fund_height_range.start.next().unwrap(),
        nu6_1_fund_height_range.end.previous().unwrap(),
        nu6_1_fund_height_range.end,
        nu6_1_fund_height_range.end.next().unwrap(),
    ] {
        let fsv =
            funding_stream_values(height, network, block_subsidy(height, network, None)?).unwrap();

        if height < canopy_activation_height {
            assert!(fsv.is_empty());
        } else if height < nu6_activation_height {
            assert_eq!(fsv, expected_dev_fund);
        } else if height < nu6_1_fund_end {
            // NU6 and NU6.1 funding streams are in the same halving and expected to have the same values
            assert_eq!(fsv, expected_nu6_fund);
        } else {
            assert!(fsv.is_empty());
        }
    }

    Ok(())
}

/// Check mainnet and testnet funding stream addresses are valid transparent P2SH addresses.
#[test]
fn test_funding_stream_addresses() -> Result<(), Report> {
    let _init_guard = zakura_test::init();
    for network in Network::iter() {
        for (receiver, recipient) in network
            .all_funding_streams()
            .iter()
            .flat_map(|fs| fs.recipients())
        {
            for address in recipient.addresses() {
                let expected_network_kind = match network.kind() {
                    NetworkKind::Mainnet => NetworkKind::Mainnet,
                    // `Regtest` uses `Testnet` transparent addresses.
                    NetworkKind::Testnet | NetworkKind::Regtest => NetworkKind::Testnet,
                };

                assert_eq!(
                    address.network_kind(),
                    expected_network_kind,
                    "incorrect network for {receiver:?} funding stream address constant: {address}",
                );

                assert!(
                    address.is_script_hash(),
                    "funding stream address is not P2SH: {address}"
                );

                let _script = address.script();
            }
        }
    }

    Ok(())
}

//Test if funding streams ranges do not overlap
#[test]
fn test_funding_stream_ranges_dont_overlap() -> Result<(), Report> {
    let _init_guard = zakura_test::init();
    for network in Network::iter() {
        let funding_streams = network.all_funding_streams();
        // This is quadratic but it's fine since the number of funding streams is small.
        for i in 0..funding_streams.len() {
            for j in (i + 1)..funding_streams.len() {
                let range_a = funding_streams[i].height_range();
                let range_b = funding_streams[j].height_range();
                assert!(
                    // https://stackoverflow.com/a/325964
                    !(range_a.start < range_b.end && range_b.start < range_a.end),
                    "Funding streams {i} and {j} overlap: {range_a:?} and {range_b:?}",
                );
            }
        }
    }
    Ok(())
}

/// The Testnet NU7 activation height estimate from zakura#1059. Not final.
const TESTNET_NU7: u32 = 4_386_000;

/// Returns the default Testnet parameters with NU7 at [`TESTNET_NU7`].
fn testnet_with_nu7() -> Network {
    use zakura_chain::parameters::testnet::{ConfiguredActivationHeights, Parameters};

    let mut activation_heights: ConfiguredActivationHeights = Network::new_default_testnet()
        .parameters()
        .expect("Testnet has parameters")
        .activation_heights()
        .into();
    activation_heights.nu7 = Some(TESTNET_NU7);

    Parameters::build()
        .with_activation_heights(activation_heights)
        .expect("activation heights are valid")
        .to_network()
        .expect("configured network is valid")
}

/// Returns a block at `height` whose only transaction is a coinbase with `outputs`.
fn coinbase_block(height: Height, outputs: Vec<transparent::Output>) -> zakura_chain::block::Block {
    use std::sync::Arc;

    use zakura_chain::{
        block::Block,
        serialization::ZcashDeserialize,
        transaction::{LockTime, Transaction},
    };

    let genesis = Block::zcash_deserialize(&zakura_test::vectors::BLOCK_TESTNET_GENESIS_BYTES[..])
        .expect("the genesis block deserializes");

    Block {
        header: genesis.header,
        transactions: vec![Arc::new(Transaction::V5 {
            network_upgrade: Nu7,
            lock_time: LockTime::unlocked(),
            expiry_height: height,
            inputs: vec![transparent::Input::Coinbase {
                height,
                data: vec![],
                sequence: u32::MAX,
            }],
            outputs,
            sapling_shielded_data: None,
            orchard_shielded_data: None,
        })],
    }
}

/// After NU7, the coinbase must pay each group of three blocks exactly one post-Blossom
/// block, so the second and third blocks of a group pay the zatoshi that plain floors drop.
/// The checkpoint verifier computes the same lockbox contribution as full validation.
#[test]
fn nu7_coinbase_pays_the_exact_rounding_group_shares() -> Result<(), Report> {
    use zakura_chain::{
        amount::NonNegative,
        parameters::subsidy::{block_subsidy, SubsidyError},
    };

    use crate::{
        block::check::{miner_fees_are_valid, subsidy_is_valid},
        checkpoint::deferred_pool_balance_change as checkpoint_deferred,
        BlockError,
    };

    let _init_guard = zakura_test::init();

    let network = testnet_with_nu7();
    let miner_script = transparent::Script::new(&[0]);
    let output = |value: i64, lock_script: transparent::Script| transparent::Output {
        value: Amount::try_from(value).expect("valid amount"),
        lock_script,
    };
    let invalid_miner_fees = Err(BlockError::Transaction(
        crate::error::TransactionError::Subsidy(SubsidyError::InvalidMinerFees),
    ));
    let grants_script = |height: Height| {
        funding_stream_address(height, &network, FundingStreamReceiver::MajorGrants)
            .expect("grants have an address in the Revision 2 stream")
            .script()
    };

    // (offset from NU7, subsidy, grants, lockbox, miner): the lockbox's 18,750,000
    // divides evenly by three, the subsidy and the grants stream do not.
    let expected = [
        (0, 52_083_333, 4_166_666, 6_250_000, 41_666_667),
        (1, 52_083_333, 4_166_667, 6_250_000, 41_666_666),
        (2, 52_083_334, 4_166_667, 6_250_000, 41_666_667),
    ];
    let mut group_total = (0, 0, 0, 0);

    for (offset, subsidy_value, grants_value, lockbox_value, miner_value) in expected {
        let height = Height(TESTNET_NU7 + offset);
        let subsidy = block_subsidy(height, &network, Some(Amount::zero()))?;
        assert_eq!(i64::from(subsidy), subsidy_value, "at {height:?}");

        let block = coinbase_block(
            height,
            vec![
                output(grants_value, grants_script(height)),
                output(miner_value, miner_script.clone()),
            ],
        );
        let deferred = subsidy_is_valid(&block, &network, subsidy)?;
        assert_eq!(i64::from(deferred.value()), lockbox_value, "at {height:?}");
        assert_eq!(
            checkpoint_deferred(height, &network, Some(Amount::zero()))?,
            Some(deferred),
        );
        miner_fees_are_valid(
            &block.transactions[0],
            height,
            Amount::zero(),
            subsidy,
            deferred,
            &network,
        )?;

        // The plain floors, 4,166,666 to grants and the rest to the miner, are rejected
        // at the blocks where the exact rule pays more to the streams.
        if offset != 0 {
            let block = coinbase_block(
                height,
                vec![
                    output(4_166_666, grants_script(height)),
                    output(subsidy_value - 4_166_666 - 6_249_999, miner_script.clone()),
                ],
            );
            assert!(
                matches!(
                    subsidy_is_valid(&block, &network, subsidy),
                    Err(BlockError::Transaction(
                        crate::error::TransactionError::Subsidy(
                            SubsidyError::FundingStreamNotFound
                        )
                    ))
                ),
                "an underpaid grants output is rejected at {height:?}"
            );
        }

        // A miner output that keeps the stream's zatoshi is rejected.
        let block = coinbase_block(
            height,
            vec![
                output(grants_value, grants_script(height)),
                output(miner_value + 1, miner_script.clone()),
            ],
        );
        let deferred = subsidy_is_valid(&block, &network, subsidy)?;
        assert_eq!(
            miner_fees_are_valid(
                &block.transactions[0],
                height,
                Amount::zero(),
                subsidy,
                deferred,
                &network,
            ),
            invalid_miner_fees,
            "an overpaid miner output is rejected at {height:?}"
        );

        group_total.0 += subsidy_value;
        group_total.1 += grants_value;
        group_total.2 += lockbox_value;
        group_total.3 += miner_value;
    }

    // Three 25-second blocks pay exactly one 75-second block to every party.
    assert_eq!(
        group_total,
        (156_250_000, 12_500_000, 18_750_000, 125_000_000)
    );
    let _: Amount<NonNegative> = Amount::try_from(group_total.0)?;

    Ok(())
}
