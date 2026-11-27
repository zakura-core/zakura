//! The amounts a v7 coinbase transaction states, and the sign of its coinbase delta.

use super::*;
use crate::parameters::subsidy::FundingStreamReceiver;
use crate::parameters::testnet::{
    ConfiguredActivationHeights, ConfiguredFundingStreamRecipient, ConfiguredFundingStreams,
    RegtestParameters,
};

const HEIGHT: Height = Height(7);

/// A Regtest network with NU7 at height 5, NU7.1 at height 6, and a lockbox funding
/// stream of 12% of the subsidy.
fn network() -> Network {
    Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu6_3: Some(1),
            nu7: Some(5),
            nu7_1: Some(6),
            ..Default::default()
        },
        funding_streams: Some(vec![ConfiguredFundingStreams {
            height_range: Some(Height(1)..Height(100)),
            recipients: Some(vec![ConfiguredFundingStreamRecipient {
                receiver: FundingStreamReceiver::Deferred,
                numerator: 12,
                addresses: None,
            }]),
        }]),
        ..Default::default()
    })
}

fn amount(zatoshis: i64) -> Amount<NonNegative> {
    zatoshis.try_into().expect("valid amount")
}

#[test]
fn issuance_states_the_lockbox_deposit_and_fee_contribution() {
    let issuance = V7CoinbaseIssuance::new(HEIGHT, &network(), amount(1_000_000), amount(10_000))
        .expect("the amounts are valid");

    assert_eq!(
        issuance,
        V7CoinbaseIssuance {
            block_subsidy: amount(1_000_000),
            lockbox_value: amount(120_000),
            fees: amount(10_000),
            nsm_fee_contribution: amount(6_000),
        }
    );
    assert_eq!(
        issuance.coinbase_delta(),
        Ok(Amount::<NegativeAllowed>::try_from(874_000).expect("valid"))
    );
}

/// When the ZIP 235 fee contribution exceeds the subsidy left after the lockbox
/// deposit, the coinbase bundle removes value from the transparent pool.
#[test]
fn coinbase_delta_is_negative_when_the_fee_contribution_exceeds_the_subsidy() {
    let issuance = V7CoinbaseIssuance::new(HEIGHT, &network(), amount(1_000), amount(10_000))
        .expect("the amounts are valid");

    assert_eq!(issuance.lockbox_value, amount(120));
    assert_eq!(issuance.nsm_fee_contribution, amount(6_000));
    assert_eq!(
        issuance.coinbase_delta(),
        Ok(Amount::<NegativeAllowed>::try_from(1_000 - 120 - 6_000).expect("valid"))
    );
}
