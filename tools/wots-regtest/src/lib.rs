//! Account transaction construction using Common's normal ZIP 244 digest.

use zcash_primitives::transaction::{
    self, Transaction, TransactionData, TxVersion,
    sighash::{self, SignableInput},
    txid::TxIdDigester,
};
use zcash_protocol::{
    consensus::{BlockHeight, BranchId},
    value::Zatoshis,
};
use zcash_script::script::{Code, Evaluable};
use zcash_transparent::{
    address::Script,
    bundle::{self, OutPoint, TxIn, TxOut},
    sighash::{SighashType, TransparentAuthorizingContext},
    wots::{self, AccountScript, Authorization, ControlOutput, KeySet},
};

/// The prototype network activates NU5 at height one and no later upgrade.
pub const BRANCH: BranchId = BranchId::Nu5;
/// The bootstrap address's redeem script: `OP_TRUE`, so anyone can spend it.
pub const BOOTSTRAP_REDEEM: [u8; 1] = [0x51];

/// An unspent output and where it is.
#[derive(Clone, Debug)]
pub struct Coin {
    pub outpoint: OutPoint,
    pub output: TxOut,
}

/// One account derived from a test seed. Its spares are recomputed whenever they sign.
pub struct Account {
    master: [u8; 32],
    index: u32,
    script: AccountScript,
}

impl Account {
    pub fn new(seed: &[u8], index: u32) -> Self {
        let master = wots::master_from_seed(seed);
        let script = AccountScript::derive(&master, index).expect("derived seeds are valid");
        Self {
            master,
            index,
            script,
        }
    }

    /// The account's fixed P2SH address script.
    pub fn address_script(&self) -> Script {
        Script(Code(self.script.address().script().to_bytes()))
    }

    /// The control output naming step `step`'s keys.
    pub fn control(&self, step: u32) -> TxOut {
        let root = KeySet::derive(&self.master, self.index, step)
            .expect("derived seeds are valid")
            .root();
        let control = ControlOutput::new(self.script.account_hash(), root, step);
        output(0, Script(Code(control.to_bytes().to_vec())))
    }

    fn sign(&self, step: u32, spare: u8, digest: [u8; 32]) -> Authorization {
        let keys = KeySet::derive(&self.master, self.index, step).expect("derived seeds are valid");
        let key = wots::signing_key(&self.master, self.index, step, spare)
            .expect("derived seeds are valid");
        Authorization::sign(key, &keys, spare, digest).expect("the key is that spare")
    }

    /// The account's redeem script as a single push.
    fn redeem_push(&self) -> Vec<u8> {
        [&[36][..], &self.script.to_bytes()].concat()
    }
}

/// How one input is unlocked.
pub enum Unlock<'a> {
    /// A bootstrap coin: pushes `OP_TRUE` as the redeem script.
    Bootstrap,
    /// A deposit to an account covered by another input's authorization.
    Covered(&'a Account),
    /// The account's authorization by spare `spare` of step `step`, on a control output or,
    /// for the first spend, on a deposit.
    Authorize {
        account: &'a Account,
        step: u32,
        spare: u8,
    },
    /// An ordinary P2PKH spend.
    Ec(&'a secp256k1::SecretKey),
}

impl Unlock<'_> {
    /// The script signature's size, for the fee; DER signatures are counted at their maximum.
    fn script_sig_size(&self, spent: &TxOut) -> usize {
        match self {
            Unlock::Bootstrap => 2,
            Unlock::Covered(_) => wots::COVERED_DEPOSIT_SCRIPT_SIG_SIZE,
            Unlock::Authorize { .. } if is_control(spent) => wots::CONTROL_SCRIPT_SIG_SIZE,
            Unlock::Authorize { .. } => wots::SIGNING_DEPOSIT_SCRIPT_SIG_SIZE,
            Unlock::Ec(_) => 1 + 73 + 1 + 33,
        }
    }
}

fn is_control(output: &TxOut) -> bool {
    ControlOutput::parse(&output.script_pubkey().0.0).is_some()
}

/// Builds and signs a transaction. If `change` is set, it receives everything above `pay`
/// and the ZIP 317 conventional fee. `tamper` may alter the authorization before encoding.
pub fn build(
    inputs: &[(Coin, Unlock<'_>)],
    pay: Vec<TxOut>,
    change: Option<Script>,
    expiry: u32,
    tamper: impl FnOnce(&mut Authorization),
) -> Transaction {
    let mut outputs = pay;
    if let Some(change) = change {
        let input_bytes = inputs
            .iter()
            .map(|(coin, unlock)| wots::input_size(unlock.script_sig_size(&coin.output)))
            .sum();
        let output_bytes =
            outputs.iter().map(output_size).sum::<usize>() + 8 + 1 + change.0.0.len();
        let spent: u64 = inputs
            .iter()
            .map(|(coin, _)| coin.output.value().into_u64())
            .sum();
        let paid: u64 = outputs.iter().map(|out| out.value().into_u64()).sum();
        let value = spent
            .checked_sub(paid + fee(input_bytes, output_bytes))
            .expect("the inputs cover the payments and fee");
        outputs.push(output(value, change));
    }
    let coins: Vec<Coin> = inputs.iter().map(|(coin, _)| coin.clone()).collect();
    let mut tamper = Some(tamper);
    let script_sigs = inputs
        .iter()
        .enumerate()
        .map(|(index, (coin, unlock))| match unlock {
            Unlock::Bootstrap => Script(Code(vec![1, BOOTSTRAP_REDEEM[0]])),
            Unlock::Covered(account) => Script(Code(account.redeem_push())),
            Unlock::Authorize {
                account,
                step,
                spare,
            } => {
                let mut authorization =
                    account.sign(*step, *spare, digest(&coins, &outputs, expiry, index));
                (tamper.take().expect("one authorization per transaction"))(&mut authorization);
                let mut bytes = Vec::new();
                authorization.push_into(&mut bytes);
                if !is_control(&coin.output) {
                    bytes.extend(account.redeem_push());
                }
                Script(Code(bytes))
            }
            Unlock::Ec(key) => {
                let hash = digest(&coins, &outputs, expiry, index);
                let signature = secp256k1::ecdsa::sign(secp256k1::Message::from_digest(hash), key);
                let mut signature = signature.serialize_der().to_vec();
                signature.push(1);
                let public_key = secp256k1::PublicKey::from_secret_key(key).serialize();
                Script(Code(
                    [
                        &[u8::try_from(signature.len()).expect("DER signatures fit one byte")][..],
                        &signature,
                        &[33],
                        &public_key,
                    ]
                    .concat(),
                ))
            }
        })
        .collect::<Vec<_>>();
    let vin = coins
        .iter()
        .zip(script_sigs)
        .map(|(coin, script_sig)| TxIn::from_parts(coin.outpoint.clone(), script_sig, u32::MAX))
        .collect();
    TransactionData::<transaction::Authorized>::from_parts(
        TxVersion::V5,
        BRANCH,
        0,
        BlockHeight::from_u32(expiry),
        Some(bundle::Bundle {
            vin,
            vout: outputs,
            authorization: bundle::Authorized,
        }),
        None,
        None,
        None,
    )
    .freeze()
    .expect("the constructed transparent v5 transaction has valid encodings")
}

/// What signature digests commit to about the spent outputs.
#[derive(Debug)]
struct Effects {
    spent: Vec<TxOut>,
}

impl bundle::Authorization for Effects {
    type ScriptSig = ();
}

impl TransparentAuthorizingContext for Effects {
    fn input_amounts(&self) -> Vec<Zatoshis> {
        self.spent.iter().map(TxOut::value).collect()
    }

    fn input_scriptpubkeys(&self) -> Vec<Script> {
        self.spent
            .iter()
            .map(|output| output.script_pubkey().clone())
            .collect()
    }
}

struct Unsigned;
impl transaction::Authorization for Unsigned {
    type TransparentAuth = Effects;
    type SaplingAuth = <transaction::Authorized as transaction::Authorization>::SaplingAuth;
    type OrchardAuth = <transaction::Authorized as transaction::Authorization>::OrchardAuth;
}

/// Computes input `index`'s ZIP 244 `SIGHASH_ALL` digest. Script signatures are not part of
/// it, so it can be computed before any input is signed.
pub fn digest(coins: &[Coin], outputs: &[TxOut], expiry: u32, index: usize) -> [u8; 32] {
    let bundle = bundle::Bundle {
        vin: coins
            .iter()
            .map(|coin| TxIn::from_parts(coin.outpoint.clone(), (), u32::MAX))
            .collect(),
        vout: outputs.to_vec(),
        authorization: Effects {
            spent: coins.iter().map(|coin| coin.output.clone()).collect(),
        },
    };
    let spent = &coins[index].output;
    let input = zcash_transparent::sighash::SignableInput::from_parts(
        &bundle,
        SighashType::ALL,
        index,
        spent.script_pubkey(),
        spent.script_pubkey(),
        spent.value(),
    )
    .expect("the input exists");
    let data = TransactionData::<Unsigned>::from_parts(
        TxVersion::V5,
        BRANCH,
        0,
        BlockHeight::from_u32(expiry),
        Some(bundle),
        None,
        None,
        None,
    );
    *sighash::signature_hash(
        &data,
        &SignableInput::Transparent(input),
        &data.digest(TxIdDigester),
    )
    .as_ref()
}

fn output_size(output: &TxOut) -> usize {
    let script = output.script_pubkey().0.0.len();
    8 + if script < 253 { 1 } else { 3 } + script
}

/// The ZIP 317 conventional fee for the given transparent input and output bytes.
pub fn fee(input_bytes: usize, output_bytes: usize) -> u64 {
    let actions = input_bytes
        .div_ceil(150)
        .max(output_bytes.div_ceil(34))
        .max(2);
    5_000 * u64::try_from(actions).expect("action counts fit u64")
}

/// Creates a transparent output with a checked amount.
pub fn output(value: u64, script: Script) -> TxOut {
    TxOut::new(
        Zatoshis::from_u64(value).expect("fixture values fit the monetary range"),
        script,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_outputs_are_recognized_and_cost_their_template() {
        let account = Account::new(b"harness test seed", 0);
        let control = account.control(1);
        assert!(is_control(&control));
        assert!(!is_control(&output(0, account.address_script())));
        assert_eq!(output_size(&control), wots::CONTROL_OUTPUT_SIZE);
        // A control input alone costs sixteen 150-byte actions.
        assert_eq!(
            fee(wots::input_size(wots::CONTROL_SCRIPT_SIG_SIZE), 0),
            80_000
        );
    }
}
