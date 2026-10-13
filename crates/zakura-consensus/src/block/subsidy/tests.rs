//! Tests for funding streams.

#![allow(clippy::unwrap_in_result)]

use std::{collections::HashMap, sync::Arc};

use color_eyre::Report;
use zakura_chain::amount::{Amount, DeferredPoolBalanceChange};
use zakura_chain::block::Block;
use zakura_chain::parameters::NetworkUpgrade::*;
use zakura_chain::parameters::{subsidy::FundingStreamReceiver, NetworkKind};
use zakura_chain::serialization::ZcashDeserialize;
use zakura_chain::transaction::{LockTime, Transaction};

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

/// The Testnet NU7 activation height estimate from zakura#1059.
const TESTNET_NU7: u32 = 4_386_000;

/// The Testnet third halving before ZIP 218, where the Revision 2 streams used to end.
const TESTNET_THIRD_HALVING: u32 = 4_476_000;

/// The Testnet third halving after ZIP 218 with NU7 at [`TESTNET_NU7`].
const TESTNET_ZIP_218_THIRD_HALVING: u32 = 4_656_000;

/// Returns the default Testnet parameters with NU7 at [`TESTNET_NU7`] and the given
/// Revision 2 grants addresses, or the built-in ones.
fn testnet_with_nu7(grants_addresses: Option<Vec<String>>) -> Network {
    use zakura_chain::parameters::testnet::{
        ConfiguredActivationHeights, ConfiguredFundingStreamRecipient, ConfiguredFundingStreams,
        Parameters,
    };

    let mut activation_heights: ConfiguredActivationHeights = Network::new_default_testnet()
        .parameters()
        .expect("Testnet has parameters")
        .activation_heights()
        .into();
    activation_heights.nu7 = Some(TESTNET_NU7);

    let mut builder = Parameters::build()
        .with_activation_heights(activation_heights)
        .expect("activation heights are valid");

    if let Some(addresses) = grants_addresses {
        builder = builder.with_funding_streams(vec![
            ConfiguredFundingStreams::default(),
            ConfiguredFundingStreams::default(),
            ConfiguredFundingStreams {
                height_range: None,
                recipients: Some(vec![
                    ConfiguredFundingStreamRecipient {
                        receiver: FundingStreamReceiver::Deferred,
                        numerator: 12,
                        addresses: None,
                    },
                    ConfiguredFundingStreamRecipient {
                        receiver: FundingStreamReceiver::MajorGrants,
                        numerator: 8,
                        addresses: Some(addresses),
                    },
                ]),
            },
        ]);
    }

    builder.to_network().expect("configured network is valid")
}

/// Grants recipients rotate every `3 · 35,000` blocks after NU7, so the 27 built-in
/// Revision 2 address slots last exactly until the moved third halving.
#[test]
fn revision_2_grants_addresses_rotate_on_the_zip_218_schedule() {
    let _init_guard = zakura_test::init();

    let addresses: Vec<transparent::Address> = (1..=27)
        .map(|index| transparent::Address::from_script_hash(NetworkKind::Testnet, [index; 20]))
        .collect();
    let network = testnet_with_nu7(Some(addresses.iter().map(ToString::to_string).collect()));
    let address = |height: u32| {
        funding_stream_address(Height(height), &network, FundingStreamReceiver::MajorGrants)
    };

    // The stream starts at NU6.1, 3,536,500, in period 117. NU7 activates in period 141.
    assert_eq!(address(3_536_500), Some(&addresses[0]));
    assert_eq!(address(TESTNET_NU7 - 1), Some(&addresses[24]));
    assert_eq!(address(TESTNET_NU7), Some(&addresses[24]));

    // From NU7 on, the period advances when `3 · 4,950,000 + (height − NU7)` reaches the
    // next multiple of 105,000.
    assert_eq!(address(4_445_999), Some(&addresses[24]));
    assert_eq!(address(4_446_000), Some(&addresses[25]));
    assert_eq!(address(TESTNET_THIRD_HALVING), Some(&addresses[25]));
    assert_eq!(address(4_550_999), Some(&addresses[25]));
    assert_eq!(address(4_551_000), Some(&addresses[26]));
    assert_eq!(
        address(TESTNET_ZIP_218_THIRD_HALVING - 1),
        Some(&addresses[26])
    );
    assert_eq!(address(TESTNET_ZIP_218_THIRD_HALVING), None);
}

/// Returns a block at `height` whose only transaction is a coinbase with `outputs`.
fn coinbase_block(height: Height, outputs: Vec<transparent::Output>) -> Block {
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

/// The coinbase must pay the Revision 2 streams until the moved third halving, and the
/// checkpoint verifier must compute the same lockbox contribution as full validation.
#[test]
fn revision_2_funding_streams_are_required_until_the_zip_218_third_halving() -> Result<(), Report> {
    use crate::{
        block::check::{miner_fees_are_valid, subsidy_is_valid},
        checkpoint::deferred_pool_balance_change as checkpoint_deferred,
        BlockError,
    };

    let _init_guard = zakura_test::init();

    let network = testnet_with_nu7(None);
    let miner_script = transparent::Script::new(&[0]);
    let output = |value: i64, lock_script: transparent::Script| transparent::Output {
        value: Amount::try_from(value).expect("valid amount"),
        lock_script,
    };
    let invalid_miner_fees = Err(BlockError::Transaction(
        crate::error::TransactionError::Subsidy(SubsidyError::InvalidMinerFees),
    ));

    for height in [
        TESTNET_NU7,
        TESTNET_THIRD_HALVING - 1,
        TESTNET_THIRD_HALVING,
        TESTNET_ZIP_218_THIRD_HALVING - 1,
    ] {
        let height = Height(height);
        let subsidy = block_subsidy(height, &network, Some(Amount::zero()))?;
        assert_eq!(i64::from(subsidy), 52_083_333);

        let grants_script =
            funding_stream_address(height, &network, FundingStreamReceiver::MajorGrants)
                .expect("grants have an address until the moved third halving")
                .script();

        // 8% and 12% of the subsidy round down to 4,166,666 and 6,249,999 zatoshi.
        let block = coinbase_block(
            height,
            vec![
                output(4_166_666, grants_script.clone()),
                output(41_666_668, miner_script.clone()),
            ],
        );
        let deferred = subsidy_is_valid(&block, &network, subsidy)?;
        assert_eq!(i64::from(deferred.value()), 6_249_999, "at {height:?}");
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

        // A coinbase without the grants output is invalid.
        let block = coinbase_block(height, vec![output(45_833_334, miner_script.clone())]);
        assert_eq!(
            subsidy_is_valid(&block, &network, subsidy),
            Err(BlockError::Transaction(
                crate::error::TransactionError::Subsidy(SubsidyError::FundingStreamNotFound)
            )),
        );

        // A miner output that also claims the lockbox contribution is invalid.
        let block = coinbase_block(
            height,
            vec![
                output(4_166_666, grants_script),
                output(41_666_668 + 6_249_999, miner_script.clone()),
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
        );
    }

    // From the moved third halving on, the miner receives the whole subsidy.
    let height = Height(TESTNET_ZIP_218_THIRD_HALVING);
    let subsidy = block_subsidy(height, &network, Some(Amount::zero()))?;
    assert_eq!(i64::from(subsidy), 26_041_666);
    let block = coinbase_block(height, vec![output(26_041_666, miner_script)]);
    let deferred = subsidy_is_valid(&block, &network, subsidy)?;
    assert_eq!(deferred, DeferredPoolBalanceChange::zero());
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

    Ok(())
}
