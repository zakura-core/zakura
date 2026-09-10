use super::super::*;
use zakura_chain::{
    block::{genesis::regtest_genesis_block, Commitment, CommitmentError},
    parameters::testnet::{ConfiguredActivationHeights, Parameters},
};

#[test]
fn custom_overlapping_activations_select_the_configured_commitment_variant() {
    let activation_height = zakura_chain::block::Height(10);
    let heartwood_canopy = Parameters::build()
        .with_network_name("OverlappingCommitments")
        .expect("the custom network name is valid")
        .with_activation_heights(ConfiguredActivationHeights {
            heartwood: Some(activation_height.0),
            canopy: Some(activation_height.0),
            ..Default::default()
        })
        .expect("same-height upgrades are valid")
        .clear_funding_streams()
        .to_network()
        .expect("the custom-network parameters are valid");
    let mut header = *regtest_genesis_block().header;
    header.commitment_bytes = [0; 32].into();
    assert_eq!(
        validate_commitment_structure(&header, &heartwood_canopy, activation_height),
        Ok(Commitment::ChainHistoryActivationReserved),
        "an overwritten Heartwood activation still requires its reserved value"
    );
    header.commitment_bytes = [1; 32].into();
    assert!(matches!(
        validate_commitment_structure(&header, &heartwood_canopy, activation_height),
        Err(CommitmentError::InvalidChainHistoryActivationReserved { .. })
    ));

    let through_nu5 = Parameters::build()
        .with_network_name("OverlappingNu5Commitment")
        .expect("the custom network name is valid")
        .with_activation_heights(ConfiguredActivationHeights {
            heartwood: Some(activation_height.0),
            canopy: Some(activation_height.0),
            nu5: Some(activation_height.0),
            ..Default::default()
        })
        .expect("same-height upgrades are valid")
        .clear_funding_streams()
        .to_network()
        .expect("the custom-network parameters are valid");
    assert!(matches!(
        validate_commitment_structure(&header, &through_nu5, activation_height),
        Ok(Commitment::ChainHistoryBlockTxAuthCommitment(_))
    ));
}
