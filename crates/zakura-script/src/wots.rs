//! Explicitly activated regtest WOTS verification and bounded policy exceptions.

use zakura_chain::{block::Height, parameters::Network, transaction::Transaction, transparent};
use zcash_script::{
    interpreter::{CallbackTransactionSignatureChecker, SignatureChecker},
    script::{self, Evaluable},
    signature,
};
use zcash_transparent::wots::{self, RedeemScript};

/// Checks both network identity and the configured candidate-block activation height.
pub fn active(network: &Network, height: Height) -> bool {
    match network {
        Network::Testnet(params) if network.is_regtest() => params
            .experimental_wots_activation_height()
            .is_some_and(|activation| height >= activation),
        _ => false,
    }
}

pub(super) struct Checker<'a> {
    pub ecdsa: CallbackTransactionSignatureChecker<'a>,
    pub digest: Option<[u8; 32]>,
}

impl SignatureChecker for Checker<'_> {
    fn check_sig(&self, signature: &signature::Decoded, key: &[u8], code: &script::Code) -> bool {
        self.ecdsa.check_sig(signature, key, code)
    }
    fn check_lock_time(&self, time: i64) -> bool {
        self.ecdsa.check_lock_time(time)
    }
    fn check_wots(&self, signature: &[u8], public_key: &[u8], commitment: &[u8]) -> bool {
        self.digest
            .is_some_and(|digest| wots::verify(digest, signature, public_key, commitment))
    }
}

/// Recognizes a canonical full-key WOTS authorization for the exact P2SH template.
/// This checks its shape and destination, not its cryptographic validity.
pub fn standard_input(input: &transparent::Input, output: &transparent::Output) -> bool {
    let transparent::Input::PrevOut { unlock_script, .. } = input else {
        return false;
    };
    if unlock_script.as_raw_bytes().len() != wots::WOTS_SCRIPT_SIG_SIZE {
        return false;
    }
    let code = script::Code(unlock_script.as_raw_bytes().to_vec());
    let Some(values) = code
        .parse()
        .map(|op| match op.ok()? {
            zcash_script::opcode::PossiblyBad::Good(zcash_script::Opcode::PushValue(value)) => {
                Some(value.value())
            }
            _ => None,
        })
        .collect::<Option<Vec<_>>>()
    else {
        return false;
    };
    let Some(redeem) = values.last().and_then(|bytes| RedeemScript::parse(bytes)) else {
        return false;
    };
    if redeem.address().script().to_bytes() != output.lock_script.as_raw_bytes() {
        return false;
    }
    match values.as_slice() {
        [s0, s1, s2, s3, s4, k0, k1, k2, k3, k4, _] => {
            [s0, s1, s2, s3, k0, k1, k2, k3]
                .iter()
                .all(|v| v.len() == 512)
                && s4.len() == 96
                && k4.len() == 128
                && unlock_script.as_raw_bytes().len() == wots::WOTS_SCRIPT_SIG_SIZE
        }
        _ => false,
    }
}

/// Counts every potential execution site, including unselected branches and nonstandard scripts.
pub(super) fn sigop_cost(tx: &Transaction, previous_outputs: &[transparent::Output]) -> u32 {
    let mut count = 0u32;
    for output in tx.outputs().iter().chain(previous_outputs) {
        count = count.saturating_add(wots::sigop_cost(output.lock_script.as_raw_bytes()));
    }
    for (input, previous) in tx.inputs().iter().zip(previous_outputs) {
        if let transparent::Input::PrevOut { unlock_script, .. } = input {
            count = count.saturating_add(wots::sigop_cost(unlock_script.as_raw_bytes()));
            if script::Code(previous.lock_script.as_raw_bytes().to_vec()).is_pay_to_script_hash() {
                if let Some(redeem) = super::extract_p2sh_redeem_script(unlock_script) {
                    count = count.saturating_add(wots::sigop_cost(&redeem));
                }
            }
        }
    }
    count
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CachedFfiTransaction;
    use std::sync::Arc;
    use zakura_chain::{
        parameters::{testnet::RegtestParameters, NetworkUpgrade},
        transaction::{Hash, HashType, LockTime},
    };

    #[test]
    fn actual_signature_requires_regtest_activation_and_commits_to_the_transaction() {
        let key = wots::SigningKey::generate(&mut getrandom::SysRng).unwrap();
        let redeem = RedeemScript::new(&key.verifying_key());
        let previous = transparent::Output {
            value: 1_000_000u64.try_into().unwrap(),
            lock_script: transparent::Script::new(&redeem.address().script().to_bytes()),
        };
        let mut tx = Transaction::V5 {
            network_upgrade: NetworkUpgrade::Nu5,
            lock_time: LockTime::unlocked(),
            expiry_height: Height(500),
            inputs: vec![transparent::Input::PrevOut {
                outpoint: transparent::OutPoint {
                    hash: Hash([1; 32]),
                    index: 0,
                },
                unlock_script: transparent::Script::new(&[]),
                sequence: u32::MAX,
            }],
            outputs: vec![transparent::Output {
                value: 800_000u64.try_into().unwrap(),
                lock_script: transparent::Script::new(&[0x51]),
            }],
            sapling_shielded_data: None,
            orchard_shielded_data: None,
        };
        let cached = CachedFfiTransaction::new(
            Arc::new(tx.clone()),
            Arc::new(vec![previous.clone()]),
            NetworkUpgrade::Nu5,
        )
        .unwrap();
        let digest = cached
            .sighasher()
            .sighash(HashType::ALL, Some((0, Vec::new())))
            .0;
        let signature = redeem.sign(key, digest).unwrap();
        if let Transaction::V5 { inputs, .. } = &mut tx {
            if let transparent::Input::PrevOut { unlock_script, .. } = &mut inputs[0] {
                *unlock_script = transparent::Script::new(&signature.0 .0);
            }
        }
        let network = Network::new_regtest(RegtestParameters {
            experimental_wots_activation_height: Some(Height(200)),
            ..Default::default()
        });
        let check = |tx: Transaction, network: &Network, height| {
            CachedFfiTransaction::new(
                Arc::new(tx),
                Arc::new(vec![previous.clone()]),
                NetworkUpgrade::Nu5,
            )
            .unwrap()
            .with_wots_context(network, height)
        };
        assert!(!active(&Network::Mainnet, Height(200)));
        assert!(!active(
            &Network::new_regtest(Default::default()),
            Height(200)
        ));
        assert!(check(tx.clone(), &network, Height(199))
            .is_valid(0)
            .is_err());
        assert!(check(tx.clone(), &Network::Mainnet, Height(200))
            .is_valid(0)
            .is_err());
        let cached = check(tx.clone(), &network, Height(200));
        assert!(cached.is_valid(0).is_ok());
        assert_eq!(cached.p2sh_sigops(), wots::WOTS_SIGOP_COST);
        assert!(standard_input(&tx.inputs()[0], &previous));

        if let Transaction::V5 { outputs, .. } = &mut tx {
            outputs[0].value = 799_999u64.try_into().unwrap();
        }
        assert!(check(tx.clone(), &network, Height(200))
            .is_valid(0)
            .is_err());
        if let Transaction::V5 { outputs, .. } = &mut tx {
            outputs[0].lock_script = transparent::Script::new(&[wots::CHECKWOTS; 200]);
        }
        assert!(check(tx, &network, Height(200)).p2sh_sigops() > 20_000);
    }
}
