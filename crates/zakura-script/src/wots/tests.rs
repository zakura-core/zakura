use std::sync::Arc;

use zakura_chain::{
    parameters::{testnet::RegtestParameters, NetworkUpgrade},
    transaction::{Hash, LockTime},
};
use zcash_script::script::Evaluable as _;
use zcash_transparent::wots::{
    master_from_seed, signing_key, AccountScript, Authorization, KeySet, WOTS_SIGOP_COST,
};

use super::*;
use crate::CachedFfiTransaction;

const MASTER_SEED: &[u8] = b"zakura account rules test seed";

fn master() -> [u8; 32] {
    master_from_seed(MASTER_SEED)
}

fn account() -> AccountScript {
    AccountScript::derive(&master(), 0).unwrap()
}

fn script(bytes: &[u8]) -> transparent::Script {
    transparent::Script::new(bytes)
}

fn output(value: u64, lock_script: &[u8]) -> transparent::Output {
    transparent::Output {
        value: value.try_into().unwrap(),
        lock_script: script(lock_script),
    }
}

fn deposit(value: u64) -> transparent::Output {
    output(value, &account().address().script().to_bytes())
}

fn control(step: u32) -> transparent::Output {
    let root = KeySet::derive(&master(), 0, step).unwrap().root();
    output(
        0,
        &ControlOutput::new(account().account_hash(), root, step).to_bytes(),
    )
}

fn input(index: u8, unlock_script: &[u8]) -> transparent::Input {
    transparent::Input::PrevOut {
        outpoint: transparent::OutPoint {
            hash: Hash([index; 32]),
            index: 0,
        },
        unlock_script: script(unlock_script),
        sequence: u32::MAX,
    }
}

fn transaction(inputs: Vec<transparent::Input>, outputs: Vec<transparent::Output>) -> Transaction {
    Transaction::V5 {
        network_upgrade: NetworkUpgrade::Nu5,
        lock_time: LockTime::unlocked(),
        expiry_height: Height(500),
        inputs,
        outputs,
        sapling_shielded_data: None,
        orchard_shielded_data: None,
    }
}

/// The account's redeem script as a single push.
fn covered_sig() -> Vec<u8> {
    [&[36][..], &account().to_bytes()].concat()
}

fn regtest() -> Network {
    Network::new_regtest(RegtestParameters {
        experimental_wots_activation_height: Some(Height(200)),
        ..Default::default()
    })
}

fn cached(
    tx: &Transaction,
    previous: &[transparent::Output],
    network: &Network,
    height: Height,
) -> CachedFfiTransaction {
    CachedFfiTransaction::new(
        Arc::new(tx.clone()),
        Arc::new(previous.to_vec()),
        NetworkUpgrade::Nu5,
    )
    .unwrap()
    .with_wots_context(network, height)
}

/// A first spend: a deposit signed by step 0, spare 0, plus a covered deposit, creating the
/// step-1 control output.
fn first_spend() -> (Transaction, Vec<transparent::Output>) {
    let previous = vec![deposit(600_000), deposit(400_000)];
    let mut tx = transaction(
        vec![input(1, &[]), input(2, &covered_sig())],
        vec![output(900_000, &[0x51]), control(1)],
    );
    let digest = cached(&tx, &previous, &Network::Mainnet, Height(200))
        .sighasher()
        .sighash(HashType::ALL, Some((0, Vec::new())))
        .0;
    let keys = KeySet::derive(&master(), 0, 0).unwrap();
    let authorization =
        Authorization::sign(signing_key(&master(), 0, 0, 0).unwrap(), &keys, 0, digest).unwrap();
    let mut signature = Vec::new();
    authorization.push_into(&mut signature);
    signature.extend(covered_sig());
    if let Transaction::V5 { inputs, .. } = &mut tx {
        inputs[0] = input(1, &signature);
    }
    (tx, previous)
}

#[test]
fn account_rules_replace_the_interpreter_only_after_regtest_activation() {
    let (tx, previous) = first_spend();
    let network = regtest();

    let accepted = cached(&tx, &previous, &network, Height(200));
    assert_eq!(accepted.wots_accounts(), Ok(()));
    assert_eq!(accepted.is_valid(0), Ok(()));
    assert_eq!(accepted.is_valid(1), Ok(()));
    assert_eq!(accepted.p2sh_sigops(), WOTS_SIGOP_COST);

    // Before activation, or off regtest, the interpreter rejects the account opcode.
    for (network, height) in [(&network, Height(199)), (&Network::Mainnet, Height(200))] {
        let inactive = cached(&tx, &previous, network, height);
        assert_eq!(inactive.wots_accounts(), Ok(()));
        assert!(inactive.is_valid(0).is_err());
        assert!(inactive.is_valid(1).is_err());
    }
    assert!(!active(
        &Network::new_regtest(Default::default()),
        Height(200)
    ));
}

#[test]
fn account_rules_commit_to_the_whole_transaction() {
    let (mut tx, previous) = first_spend();
    if let Transaction::V5 { outputs, .. } = &mut tx {
        outputs[0].value = 899_999u64.try_into().unwrap();
    }
    let rejected = cached(&tx, &previous, &regtest(), Height(200));
    assert_eq!(
        rejected.wots_accounts(),
        Err(crate::Error::Accounts(AccountError::Signature))
    );
    // Every input fails, including the covered deposit, which has no signature of its own.
    assert!(rejected.is_valid(1).unwrap_err().is_script_failure());
}

#[test]
fn control_outputs_need_the_account_s_authorization() {
    // An anyone-can-spend input paying a control output, as a coinbase transaction could.
    let previous = vec![output(1_000_000, &[0x51])];
    let tx = transaction(vec![input(1, &[])], vec![control(1)]);
    let forged = cached(&tx, &previous, &regtest(), Height(200));
    assert_eq!(
        forged.wots_accounts(),
        Err(crate::Error::Accounts(
            AccountError::UnauthorizedControlOutput
        ))
    );
    assert_eq!(
        cached(&tx, &previous, &regtest(), Height(199)).wots_accounts(),
        Ok(())
    );
}

#[test]
fn policy_recognizes_account_forms_by_shape() {
    let (tx, previous) = first_spend();
    assert!(standard_input(&tx.inputs()[0], &previous[0]));
    assert!(standard_input(&tx.inputs()[1], &previous[1]));
    assert!(!standard_input(&tx.inputs()[1], &output(1, &[0x51])));
    let mut extra = covered_sig();
    extra.insert(0, 0);
    assert!(!standard_input(&input(2, &extra), &previous[1]));
    assert!(is_control_output(&control(1)));
    assert!(!is_control_output(&previous[0]));
}
