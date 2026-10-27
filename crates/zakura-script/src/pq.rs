//! Explicitly activated regtest post-quantum accounts and their bounded policy exceptions.

use zakura_chain::{
    block::Height,
    parameters::Network,
    transaction::{HashType, Transaction},
    transparent,
};
use zcash_transparent::pq::{self, AccountCheck, AccountError, ControlOutput};

/// Checks both network identity and the configured candidate-block activation height.
pub fn active(network: &Network, height: Height) -> bool {
    match network {
        Network::Testnet(params) if network.is_regtest() => params
            .experimental_pq_transparent_activation_height()
            .is_some_and(|activation| height >= activation),
        _ => false,
    }
}

/// Runs the account rules over a whole transaction.
///
/// Account signatures sign the ZIP 244 `SIGHASH_ALL` digest, which is only defined here for
/// v5 transactions without shielded parts; any other transaction that spends from an account
/// fails with [`AccountError::Digest`].
pub(super) fn check(
    tx: &Transaction,
    previous_outputs: &[transparent::Output],
    sighasher: &zakura_chain::transaction::SigHasher,
) -> Result<AccountCheck, AccountError> {
    let inputs: Vec<(&[u8], &[u8])> = tx
        .inputs()
        .iter()
        .enumerate()
        .map(|(index, input)| {
            let script_sig = match input {
                transparent::Input::PrevOut { unlock_script, .. } => unlock_script.as_raw_bytes(),
                transparent::Input::Coinbase { .. } => &[],
            };
            let spent = previous_outputs
                .get(index)
                .map_or(&[][..], |output| output.lock_script.as_raw_bytes());
            (script_sig, spent)
        })
        .collect();
    let outputs: Vec<&[u8]> = tx
        .outputs()
        .iter()
        .map(|output| output.lock_script.as_raw_bytes())
        .collect();
    let signable = tx.version() == 5 && !tx.has_shielded_inputs() && !tx.has_shielded_outputs();
    pq::check_accounts(&inputs, &outputs, |index| {
        signable.then(|| {
            sighasher
                .sighash(HashType::ALL, Some((index, Vec::new())))
                .0
        })
    })
}

/// Recognizes the script signature forms of an account input.
/// This checks their shape and destination, not their signatures.
pub fn standard_input(input: &transparent::Input, output: &transparent::Output) -> bool {
    let transparent::Input::PrevOut { unlock_script, .. } = input else {
        return false;
    };
    pq::classify(
        unlock_script.as_raw_bytes(),
        output.lock_script.as_raw_bytes(),
    )
    .is_some_and(|form| form.is_ok())
}

/// Recognizes a control output by its template.
pub fn is_control_output(output: &transparent::Output) -> bool {
    ControlOutput::parse(output.lock_script.as_raw_bytes()).is_some()
}

#[cfg(test)]
mod tests;
