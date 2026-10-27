//! Disposable-key transaction construction using Common's normal ZIP 244 digest.

use zcash_primitives::transaction::{
    self, Transaction, TransactionData, TxVersion,
    sighash::{self, SignableInput},
    txid::TxIdDigester,
};
use zcash_protocol::{
    consensus::{BlockHeight, BranchId},
    value::Zatoshis,
};
use zcash_script::script::Code;
use zcash_transparent::{
    address::Script,
    builder::TransparentBuilder,
    bundle::{self, OutPoint, TxIn, TxOut},
    sighash::SighashType,
    wots::{RedeemScript, SigningKey},
};

/// The prototype network activates NU5 at height one and no later upgrade.
pub const BRANCH: BranchId = BranchId::Nu5;
/// Expiry is ordinary transaction expiry, not a new WOTS-specific policy.
pub const EXPIRY: u32 = 500;

struct Unsigned;
impl transaction::Authorization for Unsigned {
    type TransparentAuth = zcash_transparent::builder::Unauthorized;
    type SaplingAuth = <transaction::Authorized as transaction::Authorization>::SaplingAuth;
    type OrchardAuth = <transaction::Authorized as transaction::Authorization>::OrchardAuth;
}

/// Constructs a one-input transaction with the supplied authorization bytes.
pub fn transaction(outpoint: OutPoint, destination: TxOut, script_sig: Script) -> Transaction {
    TransactionData::<transaction::Authorized>::from_parts(
        TxVersion::V5,
        BRANCH,
        0,
        BlockHeight::from_u32(EXPIRY),
        Some(bundle::Bundle {
            vin: vec![TxIn::from_parts(outpoint, script_sig, u32::MAX)],
            vout: vec![destination],
            authorization: bundle::Authorized,
        }),
        None,
        None,
        None,
    )
    .freeze()
    .expect("the constructed transparent v5 transaction has valid encodings")
}

/// Computes the existing input-specific ALL digest, supplying the actual previous output.
pub fn digest(outpoint: OutPoint, previous: TxOut, destination: TxOut, redeem: &[u8]) -> [u8; 32] {
    let mut builder = TransparentBuilder::empty();
    let redeem_code = Script(Code(redeem.to_vec()));
    let from_chain = zcash_script::script::FromChain::parse(&redeem_code.0)
        .expect("the caller supplies a parseable redeem script");
    builder
        .add_p2sh_input(from_chain, outpoint, previous.clone())
        .expect("the redeem script matches the funded P2SH output");
    input_digest(builder, previous, destination, redeem_code)
}

/// Computes the ordinary P2PKH input digest through the unchanged EC builder.
pub fn ec_digest(
    outpoint: OutPoint,
    previous: TxOut,
    destination: TxOut,
    key: secp256k1::PublicKey,
) -> [u8; 32] {
    let mut builder = TransparentBuilder::empty();
    builder
        .add_p2pkh_input(key, outpoint, previous.clone())
        .expect("the EC key matches the funded output");
    let code = previous.script_pubkey().clone();
    input_digest(builder, previous, destination, code)
}

fn input_digest(
    builder: TransparentBuilder,
    previous: TxOut,
    destination: TxOut,
    redeem_code: Script,
) -> [u8; 32] {
    let bundle = builder
        .build()
        .expect("the input makes the transparent bundle nonempty");
    let bundle = bundle::Bundle {
        vout: vec![destination],
        ..bundle
    };
    let data = TransactionData::<Unsigned>::from_parts(
        TxVersion::V5,
        BRANCH,
        0,
        BlockHeight::from_u32(EXPIRY),
        Some(bundle),
        None,
        None,
        None,
    );
    let input = zcash_transparent::sighash::SignableInput::from_parts(
        data.transparent_bundle()
            .expect("this transaction has an input"),
        SighashType::ALL,
        0,
        &redeem_code,
        previous.script_pubkey(),
        previous.value(),
    )
    .expect("the one-input bundle contains input zero");
    *sighash::signature_hash(
        &data,
        &SignableInput::Transparent(input),
        &data.digest(TxIdDigester),
    )
    .as_ref()
}

/// Spends one output with one consuming WOTS signature. The key is never saved.
pub fn spend_wots(
    outpoint: OutPoint,
    previous: TxOut,
    destination: TxOut,
    redeem: &RedeemScript,
    key: SigningKey,
) -> Result<Transaction, &'static str> {
    let digest = digest(
        outpoint.clone(),
        previous,
        destination.clone(),
        redeem.as_bytes(),
    );
    let script = redeem.sign(key, digest)?;
    Ok(transaction(outpoint, destination, script))
}

/// Changes only the authorization bytes, keeping all signed transaction effects identical.
pub fn with_script(tx: &Transaction, script: Script) -> Transaction {
    let bundle = tx
        .transparent_bundle()
        .expect("the prototype has a transparent input");
    transaction(
        bundle.vin[0].prevout().clone(),
        bundle.vout[0].clone(),
        script,
    )
}

/// Computes the experiment's fee using the actual input bytes and ZIP 317's 150-byte unit.
pub fn fee(input_size: usize) -> u64 {
    5_000 * u64::try_from(input_size.div_ceil(150).max(2)).expect("input size fits u64")
}

/// Creates a transparent destination with a checked amount.
pub fn output(value: u64, script: Script) -> TxOut {
    TxOut::new(
        Zatoshis::from_u64(value).expect("fixture values fit the monetary range"),
        script,
    )
}
