use super::super::*;
use zakura_chain::{
    block::{self, genesis::regtest_genesis_block},
    parameters::{
        testnet::{Parameters, RegtestParameters},
        Network, NetworkKind,
    },
    work::equihash,
};

#[test]
fn pow_policy_waiver_is_derived_only_from_custom_network_identity() {
    assert_eq!(
        PowPolicy::authenticated_custom_waiver(&Network::Mainnet),
        Err(PowPolicyError::ProductionNetwork(NetworkKind::Mainnet))
    );
    assert!(!PowPolicy::for_network(&Network::Mainnet)
        .expect("mainnet always validates proof of work")
        .is_authenticated_custom_waiver());
    let testnet = Network::new_default_testnet();
    assert_eq!(
        PowPolicy::authenticated_custom_waiver(&testnet),
        Err(PowPolicyError::ProductionNetwork(NetworkKind::Testnet))
    );
    assert!(!PowPolicy::for_network(&testnet)
        .expect("default testnet always validates proof of work")
        .is_authenticated_custom_waiver());
    let regtest = Network::new_regtest(RegtestParameters::default());
    let regtest_policy =
        PowPolicy::for_network(&regtest).expect("regtest is an authenticated custom network");
    assert!(regtest_policy.is_authenticated_custom_waiver());
    assert!(validate_compact_target(&regtest_genesis_block().header, &regtest).is_ok());
    let mut wrong_shape = *regtest_genesis_block().header;
    wrong_shape.solution = equihash::Solution::for_proposal();
    assert!(matches!(
        regtest_policy.validate_solution(&wrong_shape),
        Err(equihash::Error::InvalidSolutionSize { .. })
    ));

    let pow_disabled_custom = Parameters::build()
        .with_network_name("PowDisabledCustom")
        .expect("the custom network name is valid")
        .with_disable_pow(true)
        .to_network()
        .expect("the test custom-network parameters are valid");
    let custom_policy = PowPolicy::for_network(&pow_disabled_custom)
        .expect("configured custom networks may authenticate a PoW waiver");
    assert!(custom_policy.is_authenticated_custom_waiver());
    let proposal_header = block::Header {
        solution: equihash::Solution::for_proposal(),
        ..*regtest_genesis_block().header
    };
    assert!(custom_policy.validate_solution(&proposal_header).is_ok());
}
