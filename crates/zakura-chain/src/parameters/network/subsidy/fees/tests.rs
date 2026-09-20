//! Fee allocation boundaries and rounding.

use super::*;
use crate::{
    amount::MAX_MONEY,
    parameters::testnet::{ConfiguredActivationHeights, RegtestParameters},
};

fn network() -> Network {
    Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu6_3: Some(1),
            nu7: Some(5),
            ..Default::default()
        },
        test_nsm_reissuance_height: Some(Height(10)),
        ..Default::default()
    })
}

#[test]
fn nsm_fee_share_rounds_aggregate_fees_at_activation() {
    let network = network();
    for (fees, miner) in [
        (0, 0),
        (1, 1),
        (2, 1),
        (3, 2),
        (4, 2),
        (5, 2),
        (6, 3),
        (7, 3),
        (8, 4),
        (9, 4),
        (10, 4),
        (1_000, 400),
        (1_001, 401),
        (MAX_MONEY, 840_000_000_000_000i64),
        (MAX_MONEY - 1, 840_000_000_000_000),
    ] {
        let fees = Amount::try_from(fees).unwrap();
        for height in [Height(4), Height(5), Height(9), Height(10)] {
            let expected = if height >= Height(5) {
                Amount::try_from(miner).unwrap()
            } else {
                fees
            };
            assert_eq!(miner_fee_share(height, &network, fees), expected);
        }
    }
}

#[test]
fn nsm_fee_share_requires_a_configured_activation() {
    let network = Network::new_regtest(RegtestParameters::default());
    assert!(NetworkUpgrade::Nu7.activation_height(&network).is_none());
    let fees = Amount::try_from(1_000).unwrap();
    assert_eq!(miner_fee_share(Height::MAX, &network, fees), fees);
}

#[test]
fn nsm_fee_rounding_is_per_block() {
    let network = network();
    let height = Height(5);
    let one = Amount::try_from(1).unwrap();
    let two = (one + one).unwrap();
    // Two one-zatoshi fees contribute one zatoshi together, but zero separately.
    assert_eq!(miner_fee_share(height, &network, two), one);
    assert_eq!(
        (miner_fee_share(height, &network, one) + miner_fee_share(height, &network, one)).unwrap(),
        two,
    );
}
