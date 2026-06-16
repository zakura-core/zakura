//! Tests for types and functions for the `getblocktemplate` RPC.

use anyhow::anyhow;
use std::iter;
use zebra_chain::amount::Amount;

use strum::IntoEnumIterator;
use zcash_keys::address::{Address, UnifiedAddress};

use zebra_chain::parameters::testnet::ConfiguredFundingStreamRecipient;

use zebra_chain::{
    block::Height,
    parameters::{
        subsidy::FundingStreamReceiver::{Deferred, Ecc, MajorGrants, ZcashFoundation},
        testnet::{self, ConfiguredActivationHeights, ConfiguredFundingStreams},
        Network, NetworkUpgrade,
    },
    serialization::ZcashDeserializeInto,
    transaction::Transaction,
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

#[test]
fn coinbase_errors_for_orchard_only_unified_address_after_nu6_3() {
    let net = nu6_3_testnet();
    let nu6_3_height = NetworkUpgrade::Nu6_3
        .activation_height(&net)
        .expect("NU6.3 activation height is configured");

    let miner_params = MinerParams::from(Address::Unified(orchard_only_unified_address()));

    let error = TransactionTemplate::new_coinbase(
        &net,
        nu6_3_height,
        &miner_params,
        Amount::zero(),
        #[cfg(all(zcash_unstable = "nu7", feature = "tx_v6"))]
        None,
    )
    .expect_err("Orchard-only unified addresses cannot receive coinbase rewards after NU6.3");

    assert!(
        error
            .to_string()
            .contains("must include a Sapling or transparent receiver"),
        "unexpected error: {error}"
    );
}

#[test]
fn miner_params_validate_orchard_only_unified_address_at_nu6_3() {
    let net = nu6_3_testnet();
    let nu6_3_height = NetworkUpgrade::Nu6_3
        .activation_height(&net)
        .expect("NU6.3 activation height is configured");
    let pre_nu6_3_height = Height(nu6_3_height.0 - 1);
    let miner_params = MinerParams::from(Address::Unified(orchard_only_unified_address()));

    miner_params
        .validate_coinbase_receiver(&net, pre_nu6_3_height)
        .expect("Orchard-only unified addresses are valid before NU6.3");

    let error = miner_params
        .validate_coinbase_receiver(&net, nu6_3_height)
        .expect_err("Orchard-only unified addresses are invalid after NU6.3");

    assert!(
        error
            .to_string()
            .contains("must include a Sapling or transparent receiver"),
        "unexpected error: {error}"
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
