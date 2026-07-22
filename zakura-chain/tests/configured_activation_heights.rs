//! Checks activation-height validation in production feature builds.

#[cfg(not(feature = "zakura-test"))]
use zakura_chain::parameters::testnet::{ConfiguredActivationHeights, Parameters};

#[cfg(not(feature = "zakura-test"))]
#[test]
fn nu7_activation_requires_consensus_branch_id() {
    let Err(error) = Parameters::build().with_activation_heights(ConfiguredActivationHeights {
        nu7: Some(1),
        ..Default::default()
    }) else {
        panic!("NU7 must not activate without a production consensus branch ID");
    };

    assert_eq!(
        error.to_string(),
        "configured network upgrade Nu7 must have a consensus branch ID"
    );
}
