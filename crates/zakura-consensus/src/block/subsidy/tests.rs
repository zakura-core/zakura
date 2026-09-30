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

/// Check public funding stream addresses have the specified network and script type.
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

                // ZIP 2008 changes only Mainnet's NU7 FPF recipient to P2PKH.
                // TODO(zip-259): verify the built-in stream contains this recipient
                // once the final Mainnet activation height is assigned.
                let zip_2008_mainnet_fpf = network.kind() == NetworkKind::Mainnet
                    && *receiver == FundingStreamReceiver::MajorGrants
                    && address.to_string() == "t1MkHnkxVjNpNbCrSs3AJ8J7ZSp6NTYiUcG";
                assert!(
                    address.is_script_hash() || zip_2008_mainnet_fpf,
                    "unexpected funding stream address type: {address}"
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

/// Test-only projected November 5 Mainnet activation, not a consensus parameter.
// TODO(zip-259): replace this fixture once the ZIP assigns Mainnet NU7.
const TEST_ONLY_MAINNET_NU7: u32 = 3_543_000;

/// A configured Testnet with Mainnet's activation schedule and synthetic P2SH
/// grants recipients. The separate Mainnet constants test checks the real ZIP
/// 2008 P2PKH recipient, which configured Testnets cannot accept.
fn test_only_mainnet_schedule_with_stream() -> (Network, Vec<transparent::Address>) {
    use zakura_chain::parameters::testnet::{
        ConfiguredActivationHeights, ConfiguredFundingStreamRecipient, ConfiguredFundingStreams,
        Parameters,
    };

    let mut activations: ConfiguredActivationHeights = Network::Mainnet.activation_list().into();
    activations.nu7 = Some(TEST_ONLY_MAINNET_NU7);
    let addresses: Vec<_> = (0..36)
        .map(|index| transparent::Address::from_script_hash(NetworkKind::Testnet, [index; 20]))
        .collect();
    let network = Parameters::build()
        .with_activation_heights(activations)
        .expect("activation heights are valid")
        .with_funding_streams(vec![ConfiguredFundingStreams {
            height_range: Some(Height(3_146_400)..Height(6_133_200)),
            recipients: Some(vec![
                ConfiguredFundingStreamRecipient {
                    receiver: FundingStreamReceiver::Deferred,
                    numerator: 12,
                    addresses: None,
                },
                ConfiguredFundingStreamRecipient {
                    receiver: FundingStreamReceiver::MajorGrants,
                    numerator: 8,
                    addresses: Some(addresses.iter().map(ToString::to_string).collect()),
                },
            ]),
        }])
        .to_network()
        .expect("test-only Mainnet schedule is valid");
    (network, addresses)
}

#[test]
fn test_only_mainnet_coinbase_boundaries_match_fixed_oracle() -> Result<(), Report> {
    use crate::{
        block::check::{miner_fees_are_valid, subsidy_is_valid},
        checkpoint::deferred_pool_balance_change as checkpoint_deferred,
        BlockError,
    };

    let _init_guard = zakura_test::init();
    const ROTATION: u32 = 3_613_200;
    const OLD_END: u32 = 4_406_400;
    const MOVED_END: u32 = 6_133_200;

    let (network, addresses) = test_only_mainnet_schedule_with_stream();
    let output = |value: i64, script: transparent::Script| transparent::Output {
        value: Amount::try_from(value).expect("oracle amount is valid"),
        lock_script: script,
    };
    let miner_script = transparent::Script::new(&[0]);

    // Height, subsidy, grants, deferred, miner, zero-based recipient slot.
    for (height, subsidy_zats, grants_zats, deferred_zats, miner_zats, slot) in [
        (
            TEST_ONLY_MAINNET_NU7 - 1,
            156_250_000,
            12_500_000,
            18_750_000,
            125_000_000,
            Some(11),
        ),
        (
            TEST_ONLY_MAINNET_NU7,
            52_083_333,
            4_166_666,
            6_249_999,
            41_666_668,
            Some(11),
        ),
        (
            TEST_ONLY_MAINNET_NU7 + 1,
            52_083_333,
            4_166_666,
            6_249_999,
            41_666_668,
            Some(11),
        ),
        (
            ROTATION - 1,
            52_083_333,
            4_166_666,
            6_249_999,
            41_666_668,
            Some(11),
        ),
        (
            ROTATION,
            52_083_333,
            4_166_666,
            6_249_999,
            41_666_668,
            Some(12),
        ),
        (
            ROTATION + 1,
            52_083_333,
            4_166_666,
            6_249_999,
            41_666_668,
            Some(12),
        ),
        (
            OLD_END - 1,
            52_083_333,
            4_166_666,
            6_249_999,
            41_666_668,
            Some(19),
        ),
        (
            OLD_END,
            52_083_333,
            4_166_666,
            6_249_999,
            41_666_668,
            Some(19),
        ),
        (
            OLD_END + 1,
            52_083_333,
            4_166_666,
            6_249_999,
            41_666_668,
            Some(19),
        ),
        (
            MOVED_END - 1,
            52_083_333,
            4_166_666,
            6_249_999,
            41_666_668,
            Some(35),
        ),
        (MOVED_END, 26_041_666, 0, 0, 26_041_666, None),
        (MOVED_END + 1, 26_041_666, 0, 0, 26_041_666, None),
    ] {
        let height = Height(height);
        let subsidy = block_subsidy(height, &network, Some(Amount::zero()))?;
        assert_eq!(i64::from(subsidy), subsidy_zats, "subsidy at {height:?}");
        let address = funding_stream_address(height, &network, FundingStreamReceiver::MajorGrants);
        assert_eq!(
            address,
            slot.map(|slot| &addresses[slot]),
            "recipient at {height:?}"
        );

        let mut outputs = vec![output(miner_zats, miner_script.clone())];
        if let Some(address) = address {
            outputs.push(output(grants_zats, address.script()));
        }
        let block = coinbase_block(height, outputs);
        let deferred = subsidy_is_valid(&block, &network, subsidy)?;
        assert_eq!(
            i64::from(deferred.value()),
            deferred_zats,
            "deferred at {height:?}"
        );
        assert_eq!(
            checkpoint_deferred(height, &network, Some(Amount::zero()))?,
            Some(deferred)
        );
        miner_fees_are_valid(
            &block.transactions[0],
            height,
            Amount::zero(),
            subsidy,
            deferred,
            &network,
        )?;

        if height.0 == ROTATION {
            let stale = coinbase_block(
                height,
                vec![
                    output(miner_zats, miner_script.clone()),
                    output(grants_zats, addresses[11].script()),
                ],
            );
            assert_eq!(
                subsidy_is_valid(&stale, &network, subsidy),
                Err(BlockError::Transaction(
                    crate::error::TransactionError::Subsidy(SubsidyError::FundingStreamNotFound)
                ))
            );
        }
    }

    // A missing or one-zatoshi-wrong grants output must not be accepted in
    // any active era, including the last block before the moved end.
    let missing_grants = Err(BlockError::Transaction(
        crate::error::TransactionError::Subsidy(SubsidyError::FundingStreamNotFound),
    ));
    for (height, grants) in [
        (TEST_ONLY_MAINNET_NU7 - 1, 12_500_000),
        (TEST_ONLY_MAINNET_NU7, 4_166_666),
        (ROTATION, 4_166_666),
        (MOVED_END - 1, 4_166_666),
    ] {
        let height = Height(height);
        let subsidy = block_subsidy(height, &network, Some(Amount::zero()))?;
        let address = funding_stream_address(height, &network, FundingStreamReceiver::MajorGrants)
            .expect("grants are active");
        let missing = coinbase_block(height, vec![output(1, miner_script.clone())]);
        assert_eq!(
            subsidy_is_valid(&missing, &network, subsidy),
            missing_grants
        );
        let wrong_amount = coinbase_block(height, vec![output(grants + 1, address.script())]);
        assert_eq!(
            subsidy_is_valid(&wrong_amount, &network, subsidy),
            missing_grants
        );
    }

    // The miner receives all 17 fee zatoshi before NU7, then only 7 after
    // floor(17 * 6 / 10) = 10 goes to NSM, even at the stream's end.
    let fees = Amount::try_from(17)?;
    for (height, fee_share, miner, grants) in [
        (TEST_ONLY_MAINNET_NU7 - 1, 17, 125_000_000, Some(12_500_000)),
        (TEST_ONLY_MAINNET_NU7, 7, 41_666_668, Some(4_166_666)),
        (MOVED_END - 1, 7, 41_666_668, Some(4_166_666)),
        (MOVED_END, 7, 26_041_666, None),
    ] {
        let height = Height(height);
        assert_eq!(
            i64::from(miner_fee_share(height, &network, fees)),
            fee_share
        );
        let subsidy = block_subsidy(height, &network, Some(Amount::zero()))?;
        let mut outputs = vec![output(miner + fee_share, miner_script.clone())];
        if let Some(grants) = grants {
            let address =
                funding_stream_address(height, &network, FundingStreamReceiver::MajorGrants)
                    .expect("grants are active");
            outputs.push(output(grants, address.script()));
        }
        let block = coinbase_block(height, outputs);
        let deferred = subsidy_is_valid(&block, &network, subsidy)?;
        miner_fees_are_valid(
            &block.transactions[0],
            height,
            fees,
            subsidy,
            deferred,
            &network,
        )?;
    }

    Ok(())
}

/// Returns the default Testnet parameters with NU7 at [`TESTNET_NU7`] and the given
/// Revision 2 grants addresses, or the built-in ones.
fn testnet_with_nu7(grants_addresses: Option<Vec<String>>) -> Network {
    testnet_with_nu7_at(TESTNET_NU7, grants_addresses)
}

fn testnet_with_nu7_at(nu7: u32, grants_addresses: Option<Vec<String>>) -> Network {
    use zakura_chain::parameters::testnet::{
        ConfiguredActivationHeights, ConfiguredFundingStreamRecipient, ConfiguredFundingStreams,
        Parameters,
    };

    let mut activation_heights: ConfiguredActivationHeights = Network::new_default_testnet()
        .parameters()
        .expect("Testnet has parameters")
        .activation_heights()
        .into();
    activation_heights.nu7 = Some(nu7);

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

/// Fixed oracle for the configured NU7 fork activation, not a final public
/// Testnet activation height. In particular, the distinct P2SH addresses make
/// address-period mistakes observable when the built-in addresses are identical.
#[test]
fn configured_nu7_fork_coinbase_boundaries_match_fixed_oracle() -> Result<(), Report> {
    use crate::{
        block::check::{miner_fees_are_valid, subsidy_is_valid},
        checkpoint::deferred_pool_balance_change as checkpoint_deferred,
        BlockError,
    };

    let _init_guard = zakura_test::init();

    const ACTIVATION: u32 = 4_398_756;
    const FIRST_ADDRESS_CHANGE: u32 = 4_420_488;
    const SECOND_ADDRESS_CHANGE: u32 = 4_525_488;
    const STREAM_END: u32 = 4_630_488;

    let addresses: Vec<transparent::Address> = (1..=27)
        .map(|index| transparent::Address::from_script_hash(NetworkKind::Testnet, [index; 20]))
        .collect();
    let network = testnet_with_nu7_at(
        ACTIVATION,
        Some(addresses.iter().map(ToString::to_string).collect()),
    );
    let stream = &network.all_funding_streams()[2];
    assert_eq!(
        stream.height_range(),
        &(Height(3_536_500)..Height(STREAM_END))
    );
    assert_eq!(height_for_halving(3, &network), Some(Height(STREAM_END)));

    let miner_script = transparent::Script::new(&[0]);
    let output = |value: i64, lock_script: transparent::Script| transparent::Output {
        value: Amount::try_from(value).expect("fixed oracle values are valid amounts"),
        lock_script,
    };

    // height, block subsidy, grants output, deferred contribution, miner output,
    // and the zero-based grants address slot. All amounts are zatoshi, with zero fees.
    let cases = [
        (
            ACTIVATION - 1,
            156_250_000,
            12_500_000,
            18_750_000,
            125_000_000,
            Some(24),
        ),
        (
            ACTIVATION,
            52_083_333,
            4_166_666,
            6_249_999,
            41_666_668,
            Some(24),
        ),
        (
            ACTIVATION + 1,
            52_083_333,
            4_166_666,
            6_249_999,
            41_666_668,
            Some(24),
        ),
        (
            FIRST_ADDRESS_CHANGE - 1,
            52_083_333,
            4_166_666,
            6_249_999,
            41_666_668,
            Some(24),
        ),
        (
            FIRST_ADDRESS_CHANGE,
            52_083_333,
            4_166_666,
            6_249_999,
            41_666_668,
            Some(25),
        ),
        (
            FIRST_ADDRESS_CHANGE + 1,
            52_083_333,
            4_166_666,
            6_249_999,
            41_666_668,
            Some(25),
        ),
        (
            4_476_000 - 1,
            52_083_333,
            4_166_666,
            6_249_999,
            41_666_668,
            Some(25),
        ),
        (
            4_476_000,
            52_083_333,
            4_166_666,
            6_249_999,
            41_666_668,
            Some(25),
        ),
        (
            4_476_000 + 1,
            52_083_333,
            4_166_666,
            6_249_999,
            41_666_668,
            Some(25),
        ),
        (
            SECOND_ADDRESS_CHANGE - 1,
            52_083_333,
            4_166_666,
            6_249_999,
            41_666_668,
            Some(25),
        ),
        (
            SECOND_ADDRESS_CHANGE,
            52_083_333,
            4_166_666,
            6_249_999,
            41_666_668,
            Some(26),
        ),
        (
            SECOND_ADDRESS_CHANGE + 1,
            52_083_333,
            4_166_666,
            6_249_999,
            41_666_668,
            Some(26),
        ),
        (
            STREAM_END - 1,
            52_083_333,
            4_166_666,
            6_249_999,
            41_666_668,
            Some(26),
        ),
        (STREAM_END, 26_041_666, 0, 0, 26_041_666, None),
        (STREAM_END + 1, 26_041_666, 0, 0, 26_041_666, None),
    ];

    for (height, subsidy_zats, grants_zats, deferred_zats, miner_zats, address_slot) in cases {
        let height = Height(height);
        let subsidy = block_subsidy(height, &network, Some(Amount::zero()))?;
        assert_eq!(i64::from(subsidy), subsidy_zats, "subsidy at {height:?}");

        let grants_address =
            funding_stream_address(height, &network, FundingStreamReceiver::MajorGrants);
        assert_eq!(
            grants_address,
            address_slot.map(|slot| &addresses[slot]),
            "address at {height:?}"
        );
        let mut outputs = vec![output(miner_zats, miner_script.clone())];
        if let Some(address) = grants_address {
            outputs.push(output(grants_zats, address.script()));
        }

        let block = coinbase_block(height, outputs);
        let deferred = subsidy_is_valid(&block, &network, subsidy)?;
        assert_eq!(
            i64::from(deferred.value()),
            deferred_zats,
            "deferred at {height:?}"
        );
        assert_eq!(
            checkpoint_deferred(height, &network, Some(Amount::zero()))?,
            Some(deferred)
        );
        miner_fees_are_valid(
            &block.transactions[0],
            height,
            Amount::zero(),
            subsidy,
            deferred,
            &network,
        )?;

        // At the first block of each new address period, the prior address is invalid.
        if height.0 == FIRST_ADDRESS_CHANGE || height.0 == SECOND_ADDRESS_CHANGE {
            let prior = if height.0 == FIRST_ADDRESS_CHANGE {
                24
            } else {
                25
            };
            let stale = coinbase_block(
                height,
                vec![
                    output(miner_zats, miner_script.clone()),
                    output(grants_zats, addresses[prior].script()),
                ],
            );
            assert_eq!(
                subsidy_is_valid(&stale, &network, subsidy),
                Err(BlockError::Transaction(
                    crate::error::TransactionError::Subsidy(SubsidyError::FundingStreamNotFound)
                )),
            );
        }
    }

    // Seventeen aggregate fee zatoshi give the miner all 17 before NU7, but
    // only 7 at and after activation: floor(17 * 6 / 10) = 10 goes to NSM.
    let fees = Amount::try_from(17)?;
    for (height, fee_share, subsidy_zats, grants_zats, miner_zats) in [
        (ACTIVATION - 1, 17, 156_250_000, 12_500_000, 125_000_000),
        (ACTIVATION, 7, 52_083_333, 4_166_666, 41_666_668),
        (ACTIVATION + 1, 7, 52_083_333, 4_166_666, 41_666_668),
    ] {
        let height = Height(height);
        assert_eq!(
            i64::from(miner_fee_share(height, &network, fees)),
            fee_share
        );
        let subsidy = block_subsidy(height, &network, Some(Amount::zero()))?;
        assert_eq!(i64::from(subsidy), subsidy_zats);
        let grants_script =
            funding_stream_address(height, &network, FundingStreamReceiver::MajorGrants)
                .expect("the grants stream is active across activation")
                .script();
        let block = coinbase_block(
            height,
            vec![
                output(grants_zats, grants_script.clone()),
                output(miner_zats + fee_share, miner_script.clone()),
            ],
        );
        let deferred = subsidy_is_valid(&block, &network, subsidy)?;
        miner_fees_are_valid(
            &block.transactions[0],
            height,
            fees,
            subsidy,
            deferred,
            &network,
        )?;

        if height.0 == ACTIVATION {
            let overclaim = coinbase_block(
                height,
                vec![
                    output(grants_zats, grants_script),
                    output(miner_zats + fee_share + 1, miner_script.clone()),
                ],
            );
            assert_eq!(
                miner_fees_are_valid(
                    &overclaim.transactions[0],
                    height,
                    fees,
                    subsidy,
                    deferred,
                    &network,
                ),
                Err(BlockError::Transaction(
                    crate::error::TransactionError::Subsidy(SubsidyError::InvalidMinerFees)
                )),
            );
        }
    }

    // An independent, whole-stream issuance check catches a schedule that is
    // individually correct at boundaries but wrong in duration.
    let issued_before = scheduled_issuance_zatoshis(Height(3_536_500 - 1), &network)?;
    let issued_through = scheduled_issuance_zatoshis(Height(STREAM_END - 1), &network)?;
    assert_eq!(issued_through - issued_before, 146_796_874_922_756);
    let mut gross_grants = 0u128;
    let mut gross_deferred = 0u128;
    for height in 3_536_500..STREAM_END {
        let height = Height(height);
        let subsidy = block_subsidy(height, &network, Some(Amount::zero()))?;
        let values = funding_stream_values(height, &network, subsidy)?;
        gross_grants += u128::try_from(i64::from(values[&FundingStreamReceiver::MajorGrants]))?;
        gross_deferred += u128::try_from(i64::from(values[&FundingStreamReceiver::Deferred]))?;
    }
    assert_eq!(gross_grants, 11_743_749_845_512);
    assert_eq!(gross_deferred, 17_615_624_768_268);

    Ok(())
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
