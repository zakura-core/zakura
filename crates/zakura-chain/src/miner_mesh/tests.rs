mod prop;
mod recent_set;
mod vectors;
mod window;

use crate::{
    block::Height,
    parameters::{
        testnet::{self, ConfiguredActivationHeights},
        Network,
    },
};

/// A test network whose blocks are 75 seconds apart from height 1, and 25
/// seconds apart from `nu7` when it is set. `nu7` must be above height 2.
fn network_with_nu7(nu7: Option<Height>) -> Network {
    testnet::Parameters::build()
        .with_activation_heights(ConfiguredActivationHeights {
            blossom: Some(1),
            // Every network needs a Canopy height. It changes no spacing.
            canopy: Some(2),
            nu7: nu7.map(|height| height.0),
            ..Default::default()
        })
        .expect("the activation heights are in order")
        .clear_funding_streams()
        .to_network()
        .expect("the configured test network is valid")
}
