//! Tests for types and functions for the `getblocktemplate` RPC.

use anyhow::anyhow;
use std::iter;
use zakura_chain::amount::Amount;

use strum::IntoEnumIterator;
use zcash_keys::address::{Address, UnifiedAddress};

use zakura_chain::parameters::testnet::ConfiguredFundingStreamRecipient;

use zakura_chain::{
    block::Height,
    local_genesis::generate_local_testnet_with_funded_keys,
    parameters::{
        subsidy::FundingStreamReceiver::{Deferred, Ecc, MajorGrants, ZcashFoundation},
        testnet::{self, ConfiguredActivationHeights, ConfiguredFundingStreams},
        Network, NetworkUpgrade,
    },
    serialization::ZcashDeserializeInto,
    transaction::Transaction,
    transparent,
};

use crate::client::TransactionTemplate;
use crate::config::mining::{default_miner_address, MinerAddressType};

use super::MinerParams;

/// Tests that coinbase transactions can be generated.
///
/// This test needs to be run with the `--release` flag so that it runs for ~ 30 seconds instead of
/// ~ 90.
#[test]
#[ignore]
fn coinbase() -> anyhow::Result<()> {
    let regtest = testnet::Parameters::build()
        .with_slow_start_interval(Height::MIN)
        .with_activation_heights(ConfiguredActivationHeights {
            overwinter: Some(1),
            sapling: Some(2),
            blossom: Some(3),
            heartwood: Some(4),
            canopy: Some(5),
            nu5: Some(6),
            nu6: Some(7),
            nu6_1: Some(8),
            nu6_3: Some(9),
            ..Default::default()
        })?
        .with_funding_streams(vec![
            ConfiguredFundingStreams {
                height_range: Some(Height(1)..Height(100)),
                recipients: Some(vec![
                    ConfiguredFundingStreamRecipient::new_for(Ecc),
                    ConfiguredFundingStreamRecipient::new_for(ZcashFoundation),
                    ConfiguredFundingStreamRecipient::new_for(MajorGrants),
                ]),
            },
            ConfiguredFundingStreams {
                height_range: Some(Height(1)..Height(100)),
                recipients: Some(vec![
                    ConfiguredFundingStreamRecipient::new_for(MajorGrants),
                    ConfiguredFundingStreamRecipient {
                        receiver: Deferred,
                        numerator: 12,
                        addresses: None,
                    },
                ]),
            },
        ])
        .to_network()?;

    for net in Network::iter().chain(iter::once(regtest)) {
        for nu in NetworkUpgrade::iter().filter(|nu| nu >= &NetworkUpgrade::Sapling) {
            if let Some(height) = nu.activation_height(&net) {
                for addr_type in MinerAddressType::iter() {
                    TransactionTemplate::new_coinbase(
                        &net,
                        height,
                        &MinerParams::from(
                            Address::decode(&net, default_miner_address(net.kind(), &addr_type))
                                .ok_or(anyhow!("hard-coded addr must be valid"))?,
                        ),
                        Amount::zero(),
                    )?
                    .data()
                    .as_ref()
                    // Deserialization contains checks for elementary consensus rules, which must
                    // pass.
                    .zcash_deserialize_into::<Transaction>()?;
                }
            }
        }
    }

    Ok(())
}

/// The coinbase built for a local testnet's activation block must include the
/// network's configured zero-value lockbox marker output, so a mining node
/// can produce a block that satisfies the one-time ZIP-271 disbursement rule.
#[test]
fn local_genesis_activation_coinbase_includes_lockbox_marker() -> anyhow::Result<()> {
    let generated = generate_local_testnet_with_funded_keys(
        vec!["alice".to_string(), "bob".to_string()],
        Default::default(),
    )
    .map_err(|error| anyhow!(error.to_string()))?;
    let net = generated.network;
    let height = NetworkUpgrade::Nu6_3
        .activation_height(&net)
        .expect("the default local network activates NU6.3");
    let miner_params = MinerParams::from(
        Address::decode(
            &net,
            default_miner_address(net.kind(), &MinerAddressType::Transparent),
        )
        .ok_or(anyhow!("hard-coded address must be valid"))?,
    );
    let transaction =
        TransactionTemplate::new_coinbase(&net, height, &miner_params, Amount::zero())?
            .data()
            .as_ref()
            .zcash_deserialize_into::<Transaction>()?;
    let lockbox_disbursements = net.lockbox_disbursements(height);
    let [(lockbox_address, lockbox_amount)] = lockbox_disbursements.as_slice() else {
        return Err(anyhow!("local network must have one lockbox marker"));
    };
    let lockbox_output = transparent::Output::new(*lockbox_amount, lockbox_address.script());

    assert!(transaction.outputs().contains(&lockbox_output));

    Ok(())
}

/// The Zakura marker is always prepended, and `extra_coinbase_data` can't exceed
/// the limit.
#[test]
fn coinbase_tag_and_limit() {
    use zcash_address::ZcashAddress;

    use crate::config::mining::{
        Config, ExtraCoinbaseData, MAX_USER_COINBASE_DATA_LEN, ZAKURA_COINBASE_MARKER,
        ZAKURA_COINBASE_SEPARATOR,
    };

    // `ExtraCoinbaseData` accepts data up to the limit and rejects one byte over. Its
    // `Deserialize` impl delegates here, so an oversized `mining.extra_coinbase_data`
    // makes the config fail to load and the node refuse to start.
    assert!(ExtraCoinbaseData::try_from("x".repeat(MAX_USER_COINBASE_DATA_LEN)).is_ok());
    assert!(ExtraCoinbaseData::try_from("x".repeat(MAX_USER_COINBASE_DATA_LEN + 1)).is_err());

    let net = Network::Mainnet;
    let addr: ZcashAddress = default_miner_address(net.kind(), &MinerAddressType::Transparent)
        .parse()
        .expect("default miner address parses");

    let params = |extra: Option<ExtraCoinbaseData>| {
        MinerParams::new(
            &net,
            Config {
                miner_address: Some(addr.clone()),
                extra_coinbase_data: extra,
                ..Default::default()
            },
        )
    };

    // The marker is prepended whether or not `extra_coinbase_data` is set, so every
    // block Zakura builds is tagged. Without extra data, the coinbase data is exactly
    // the marker.
    let untagged = params(None).expect("valid config");
    let untagged = untagged.data().as_ref().expect("marker is always present");
    assert_eq!(
        untagged.value().as_slice(),
        ZAKURA_COINBASE_MARKER.as_bytes()
    );

    // With extra data, the marker and separator precede it.
    let tag = ExtraCoinbaseData::try_from("/pool/".to_string()).expect("within the limit");
    let tagged = params(Some(tag)).expect("valid config");
    let tagged = tagged.data().as_ref().expect("marker is always present");
    assert_eq!(
        tagged.value().as_slice(),
        [ZAKURA_COINBASE_MARKER, ZAKURA_COINBASE_SEPARATOR, "/pool/"]
            .concat()
            .as_bytes()
    );
}

/// Tests that a coinbase paying an Orchard-only unified address is routed to Orchard before
/// NU6.3 and to Ironwood from NU6.3 onward.
///
/// Ironwood reuses the Orchard receiver, and net-new value into Orchard is forbidden after
/// NU6.3, so the Orchard receiver is paid via Ironwood once NU6.3 is active.
///
/// Like [`coinbase`], this builds real shielded outputs, so it is ignored to keep normal
/// debug test runs fast. Run it with `--release` when intentionally checking this path.
#[test]
#[ignore]
fn coinbase_routes_orchard_only_unified_address_by_network_upgrade() {
    let net = nu6_3_testnet();
    let nu6_3_height = NetworkUpgrade::Nu6_3
        .activation_height(&net)
        .expect("NU6.3 activation height is configured");
    let nu7_height = NetworkUpgrade::Nu7
        .activation_height(&net)
        .expect("NU7 activation height is configured");
    let pre_nu6_3_height = Height(nu6_3_height.0 - 1);
    let miner_params = MinerParams::from(Address::Unified(orchard_only_unified_address()));

    // Before NU6.3, the Orchard receiver is paid via Orchard.
    let pre_tx =
        TransactionTemplate::new_coinbase(&net, pre_nu6_3_height, &miner_params, Amount::zero())
            .expect("Orchard-only unified address is paid via Orchard before NU6.3")
            .data()
            .as_ref()
            .zcash_deserialize_into::<Transaction>()
            .expect("coinbase transaction deserializes");

    assert!(
        pre_tx.orchard_shielded_data().is_some(),
        "pre-NU6.3 coinbase to an Orchard-only address should have an Orchard output"
    );
    assert!(
        pre_tx.ironwood_shielded_data().is_none(),
        "pre-NU6.3 coinbase should not have an Ironwood output"
    );

    // After NU6.3, the Orchard receiver is paid via Ironwood instead.
    let post_tx =
        TransactionTemplate::new_coinbase(&net, nu6_3_height, &miner_params, Amount::zero())
            .expect("Orchard-only unified address is paid via Ironwood after NU6.3")
            .data()
            .as_ref()
            .zcash_deserialize_into::<Transaction>()
            .expect("coinbase transaction deserializes");

    assert!(
        post_tx.ironwood_shielded_data().is_some(),
        "post-NU6.3 coinbase to an Orchard-only address should have an Ironwood output"
    );
    assert!(
        post_tx.orchard_shielded_data().is_none(),
        "post-NU6.3 coinbase should not have an Orchard output"
    );

    let later_tx =
        TransactionTemplate::new_coinbase(&net, nu7_height, &miner_params, Amount::zero())
            .expect("Orchard-only unified address is still paid via Ironwood after NU6.3")
            .data()
            .as_ref()
            .zcash_deserialize_into::<Transaction>()
            .expect("coinbase transaction deserializes");

    assert!(
        later_tx.ironwood_shielded_data().is_some(),
        "post-NU6.3 coinbase to an Orchard-only address should keep using Ironwood"
    );
    assert!(
        later_tx.orchard_shielded_data().is_none(),
        "post-NU6.3 coinbase should not have an Orchard output"
    );
}

/// Tests that unified mining addresses with multiple shielded receivers still prefer the
/// Orchard receiver before and from NU6.3 onward.
///
/// Like [`coinbase`], this builds real shielded outputs, so it is ignored to keep normal
/// debug test runs fast. Run it with `--release` when intentionally checking this path.
#[test]
#[ignore]
fn coinbase_preserves_orchard_priority_by_network_upgrade() {
    let net = nu6_3_testnet();
    let nu6_3_height = NetworkUpgrade::Nu6_3
        .activation_height(&net)
        .expect("NU6.3 activation height is configured");
    let nu7_height = NetworkUpgrade::Nu7
        .activation_height(&net)
        .expect("NU7 activation height is configured");
    let pre_nu6_3_height = Height(nu6_3_height.0 - 1);
    let miner_params = MinerParams::from(
        Address::decode(
            &net,
            default_miner_address(net.kind(), &MinerAddressType::Unified),
        )
        .expect("hard-coded unified miner address is valid"),
    );

    let pre_tx =
        TransactionTemplate::new_coinbase(&net, pre_nu6_3_height, &miner_params, Amount::zero())
            .expect("unified address is paid via Orchard before NU6.3")
            .data()
            .as_ref()
            .zcash_deserialize_into::<Transaction>()
            .expect("coinbase transaction deserializes");

    assert!(
        pre_tx.orchard_shielded_data().is_some(),
        "pre-NU6.3 coinbase to a unified address should have an Orchard output"
    );
    assert!(
        pre_tx.sapling_outputs().next().is_none(),
        "pre-NU6.3 coinbase should not prefer Sapling when Orchard is present"
    );
    assert!(
        pre_tx.ironwood_shielded_data().is_none(),
        "pre-NU6.3 coinbase should not have an Ironwood output"
    );

    let post_tx =
        TransactionTemplate::new_coinbase(&net, nu6_3_height, &miner_params, Amount::zero())
            .expect("unified address is paid via Ironwood at NU6.3")
            .data()
            .as_ref()
            .zcash_deserialize_into::<Transaction>()
            .expect("coinbase transaction deserializes");

    assert!(
        post_tx.ironwood_shielded_data().is_some(),
        "NU6.3 coinbase to a unified address should have an Ironwood output"
    );
    assert!(
        post_tx.sapling_outputs().next().is_none(),
        "NU6.3 coinbase should not prefer Sapling when Orchard is present"
    );
    assert!(
        post_tx.orchard_shielded_data().is_none(),
        "NU6.3 coinbase should not have an Orchard output"
    );

    let later_tx =
        TransactionTemplate::new_coinbase(&net, nu7_height, &miner_params, Amount::zero())
            .expect("unified address is still paid via Ironwood after NU6.3")
            .data()
            .as_ref()
            .zcash_deserialize_into::<Transaction>()
            .expect("coinbase transaction deserializes");

    assert!(
        later_tx.ironwood_shielded_data().is_some(),
        "post-NU6.3 coinbase to a unified address should keep using Ironwood"
    );
    assert!(
        later_tx.sapling_outputs().next().is_none(),
        "post-NU6.3 coinbase should not prefer Sapling when Orchard is present"
    );
    assert!(
        later_tx.orchard_shielded_data().is_none(),
        "post-NU6.3 coinbase should not have an Orchard output"
    );
}

fn nu6_3_testnet() -> Network {
    testnet::Parameters::build()
        .with_activation_heights(ConfiguredActivationHeights {
            overwinter: Some(1),
            sapling: Some(2),
            blossom: Some(3),
            heartwood: Some(4),
            canopy: Some(5),
            nu5: Some(6),
            nu6: Some(7),
            nu6_1: Some(8),
            nu6_3: Some(9),
            nu7: Some(10),
            ..Default::default()
        })
        .expect("configured activation heights are valid")
        .clear_funding_streams()
        .to_network()
        .expect("configured network is valid")
}

fn orchard_only_unified_address() -> UnifiedAddress {
    let orchard_spending_key = Option::<orchard::keys::SpendingKey>::from(
        orchard::keys::SpendingKey::from_bytes([0u8; 32]),
    )
    .expect("test Orchard spending key is valid");
    let orchard_full_viewing_key = orchard::keys::FullViewingKey::from(&orchard_spending_key);
    let orchard_address = orchard_full_viewing_key.address_at(
        orchard::keys::DiversifierIndex::from([0u8; 11]),
        orchard::keys::Scope::External,
    );

    UnifiedAddress::from_receivers(Some(orchard_address), None, None)
        .expect("Orchard-only unified addresses are valid")
}
