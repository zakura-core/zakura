//! Builds the v7 transactions that the ZIP 248 regtests submit.
//!
//! Valid transactions come from the `zcash_primitives` builder, the same wallet builder
//! that block templates use for coinbases, so a test that mines one shows that Zakura and
//! Common agree on its bytes and digests. Transactions that break one v7 rule are
//! assembled from Zakura's types and signed over Zakura's own v7 digest, so that rule is
//! the only one they break.

use std::{convert::Infallible, sync::Arc};

use eyre::{eyre, Result};
use incrementalmerkletree::{frontier::CommitmentTree, witness::IncrementalWitness};
use orchard::{
    keys::{FullViewingKey, Scope, SpendAuthorizingKey, SpendingKey},
    tree::MerkleHashOrchard,
    Note,
};
use secp256k1::{Message, PublicKey, SecretKey};
use zcash_primitives::transaction::{
    builder::{BuildConfig, BuildResult, Builder, BundlePadding},
    fees::zip317::FeeRule,
    TxVersion,
};
use zcash_protocol::{memo::MemoBytes, value::Zatoshis};
use zcash_transparent::{
    address::{Script as CommonScript, TransparentAddress},
    builder::TransparentSigningSet,
    bundle::{OutPoint as CommonOutPoint, TxOut},
};

use zakura_chain::{
    amount::{Amount, NonNegative},
    block::Height,
    parameters::{Network, NetworkKind, NetworkUpgrade},
    serialization::ZcashDeserializeInto,
    transaction::{v7::ValueDeltas, HashType, LockTime, SigHasher, Transaction},
    transparent::{Address, Input, OutPoint, Output, Script},
};

/// The ZIP 317 fee of a transaction with at most two transparent inputs and outputs.
pub(crate) const TRANSPARENT_FEE: u64 = 10_000;

/// The ZIP 317 fee of a transaction with one transparent input or output and one
/// Ironwood bundle, which the builder pads to two actions.
pub(crate) const IRONWOOD_FEE: u64 = 15_000;

/// `SIGHASH_ALL`, the only hash type a v7 transparent signature may use.
pub(crate) const SIGHASH_ALL: u8 = 0x01;

/// `SIGHASH_NONE`, which v7 signatures must not use.
pub(crate) const SIGHASH_NONE: u8 = 0x02;

/// A transparent output that a test spends, with where it is.
#[derive(Clone, Debug)]
pub(crate) struct Coin {
    pub outpoint: OutPoint,
    pub output: Output,
}

impl Coin {
    /// Returns the output of `tx` at `index`.
    pub fn of(tx: &Transaction, index: usize) -> Result<Self> {
        let output = tx
            .outputs()
            .get(index)
            .ok_or_else(|| eyre!("the transaction has an output at {index}"))?
            .clone();
        Ok(Self {
            outpoint: OutPoint {
                hash: tx.hash(),
                index: u32::try_from(index)?,
            },
            output,
        })
    }

    pub fn value(&self) -> u64 {
        u64::from(self.output.value())
    }
}

/// The transparent key that the tests mine to and spend from.
pub(crate) struct TestKey {
    secret: SecretKey,
    pubkey: PublicKey,
}

impl TestKey {
    pub fn new() -> Self {
        let secret = SecretKey::from_secret_bytes([0xcd; 32]).expect("valid secret key");
        Self {
            pubkey: PublicKey::from_secret_key(&secret),
            secret,
        }
    }

    pub fn pubkey(&self) -> [u8; 33] {
        self.pubkey.serialize()
    }

    /// The key's P2PKH address on Regtest.
    pub fn address(&self) -> Address {
        let TransparentAddress::PublicKeyHash(hash) = TransparentAddress::from_pubkey(&self.pubkey)
        else {
            unreachable!("a public key has a P2PKH address")
        };
        Address::from_pub_key_hash(NetworkKind::Regtest, hash)
    }

    /// Returns the first output of `tx` that pays this key.
    pub fn coin_in(&self, tx: &Transaction) -> Result<Coin> {
        let script = self.address().script();
        let index = tx
            .outputs()
            .iter()
            .position(|output| output.lock_script == script)
            .ok_or_else(|| eyre!("the transaction pays the test key"))?;
        Coin::of(tx, index)
    }

    /// Signs input `index` of the v7 transaction `tx` over Zakura's v7 digest, and appends
    /// `hash_type` to the DER signature.
    ///
    /// The v7 digest ignores the hash type byte, so a signature with any byte is a valid
    /// signature of the digest: only the `SIGHASH_ALL` rule can reject it.
    pub fn sign_v7(
        &self,
        tx: &Transaction,
        coins: &[Coin],
        index: usize,
        hash_type: u8,
    ) -> Vec<u8> {
        let spent = coins.iter().map(|coin| coin.output.clone()).collect();
        let sighash = SigHasher::new(tx, NetworkUpgrade::Nu7_1, Arc::new(spent))
            .expect("the test transaction converts to a librustzcash transaction")
            .v7_transparent_sighash(HashType::ALL, index)
            .expect("the test transaction is v7 and its spent outputs match its inputs");
        let mut signature = secp256k1::ecdsa::sign(Message::from_digest(sighash.0), &self.secret)
            .serialize_der()
            .to_vec();
        signature.push(hash_type);
        signature
    }

    /// Adds this key's P2PKH coin to a `zcash_primitives` builder and its key to
    /// `signing_set`.
    fn add_input(
        &self,
        builder: &mut Builder<Network, ()>,
        signing_set: &mut TransparentSigningSet,
        coin: &Coin,
    ) -> Result<()> {
        let script: CommonScript = TransparentAddress::from_pubkey(&self.pubkey)
            .script()
            .into();
        if coin.output.lock_script.as_raw_bytes() != script.0 .0.as_slice() {
            return Err(eyre!("the coin is a P2PKH output of the test key"));
        }
        signing_set.add_key(self.secret);
        builder
            .add_transparent_p2pkh_input(
                self.pubkey,
                CommonOutPoint::new(coin.outpoint.hash.0, coin.outpoint.index),
                TxOut::new(zatoshis(coin.value())?, script),
            )
            .map_err(|error| eyre!("{error:?}"))
    }

    fn add_output(&self, builder: &mut Builder<Network, ()>, value: u64) -> Result<()> {
        builder
            .add_transparent_output(
                &TransparentAddress::from_pubkey(&self.pubkey),
                zatoshis(value)?,
            )
            .map_err(|error| eyre!("{error:?}"))
    }
}

/// A transaction built by the `zcash_primitives` builder, decoded by Zakura, with the
/// digests Common computed for it.
pub(crate) struct CommonBuilt {
    pub tx: Transaction,
    pub common_txid: [u8; 32],
    pub common_auth_digest: [u8; 32],
    result: BuildResult,
}

impl CommonBuilt {
    fn new(result: BuildResult) -> Result<Self> {
        let mut bytes = Vec::new();
        result.transaction().write(&mut bytes)?;
        Ok(Self {
            tx: bytes.zcash_deserialize_into()?,
            common_txid: *result.transaction().txid().as_ref(),
            common_auth_digest: result
                .transaction()
                .auth_commitment()
                .as_bytes()
                .try_into()?,
            result,
        })
    }

    /// Checks that Zakura computes the same txid and authorizing data commitment as Common.
    pub fn assert_digests_agree(&self) {
        assert_eq!(
            self.tx.hash().0,
            self.common_txid,
            "Zakura and Common txids"
        );
        assert_eq!(
            self.tx.auth_digest().map(|digest| digest.0),
            Some(self.common_auth_digest),
            "Zakura and Common authorizing data commitments",
        );
    }
}

/// Builds a transaction of `version` for the block at `height`, which spends the test
/// key's `coins` as its inputs in order, and pays their value less the ZIP 317 fee back
/// to the key in `outputs` outputs.
///
/// [`TRANSPARENT_FEE`] is the fee of at most two inputs and two outputs.
pub(crate) fn transparent_transfer(
    net: &Network,
    height: Height,
    version: TxVersion,
    key: &TestKey,
    coins: &[Coin],
    outputs: u64,
) -> Result<CommonBuilt> {
    if !(1..=2).contains(&coins.len()) || !(1..=2).contains(&outputs) {
        return Err(eyre!("a transfer has one or two inputs and outputs"));
    }
    let mut builder = builder(net, height, version, None)?;
    let mut signing_set = TransparentSigningSet::new();
    for coin in coins {
        key.add_input(&mut builder, &mut signing_set, coin)?;
    }
    let paid = coins.iter().map(Coin::value).sum::<u64>() - TRANSPARENT_FEE;
    let share = paid / outputs;
    key.add_output(&mut builder, paid - share * (outputs - 1))?;
    for _ in 1..outputs {
        key.add_output(&mut builder, share)?;
    }
    build(builder, &signing_set, &[])
}

/// Builds a v7 transaction for the block at `height`, which spends the test key's `coin`
/// to [`ironwood_address`], less the ZIP 317 fee, with real proofs. Returns it with the
/// note it creates.
pub(crate) fn shield_to_ironwood(
    net: &Network,
    height: Height,
    key: &TestKey,
    coin: &Coin,
) -> Result<(CommonBuilt, IronwoodNote)> {
    let mut builder = builder(
        net,
        height,
        TxVersion::V7,
        Some(orchard::Anchor::empty_tree()),
    )?;
    let mut signing_set = TransparentSigningSet::new();
    key.add_input(&mut builder, &mut signing_set, coin)?;
    builder
        .add_ironwood_output::<Infallible>(
            None,
            ironwood_address(),
            zatoshis(coin.value() - IRONWOOD_FEE)?,
            MemoBytes::empty(),
        )
        .map_err(|error| eyre!("{error:?}"))?;
    let built = build(builder, &signing_set, &[])?;

    let bundle = built
        .result
        .transaction()
        .ironwood_bundle()
        .ok_or_else(|| eyre!("the shielding transaction has an Ironwood bundle"))?;
    let ivk = FullViewingKey::from(&ironwood_spending_key()).to_ivk(Scope::External);
    let (position, _, note, _, _) = bundle
        .decrypt_outputs_with_keys(&[ivk])
        .into_iter()
        .next()
        .ok_or_else(|| eyre!("the test key decrypts its own Ironwood output"))?;
    let commitments = bundle
        .actions()
        .iter()
        .map(|action| MerkleHashOrchard::from_cmx(action.cmx()))
        .collect();

    Ok((
        built,
        IronwoodNote {
            note,
            position,
            commitments,
        },
    ))
}

/// A note that [`shield_to_ironwood`] created, with the commitments that its block adds to
/// the Ironwood note commitment tree.
pub(crate) struct IronwoodNote {
    note: Note,
    position: usize,
    commitments: Vec<MerkleHashOrchard>,
}

impl IronwoodNote {
    pub fn value(&self) -> u64 {
        self.note.value().inner()
    }
}

/// Builds a v7 transaction for the block at `height`, which spends `note` to the test key,
/// less the ZIP 317 fee, with real proofs.
///
/// The note must be in the only block that has added to the Ironwood note commitment
/// tree, so that the tree holds just that block's commitments.
pub(crate) fn unshield_from_ironwood(
    net: &Network,
    height: Height,
    note: &IronwoodNote,
    key: &TestKey,
) -> Result<CommonBuilt> {
    let mut tree = CommitmentTree::<MerkleHashOrchard, 32>::empty();
    let mut witness = None;
    for (position, commitment) in note.commitments.iter().enumerate() {
        match &mut witness {
            None => {
                tree.append(*commitment)
                    .map_err(|_| eyre!("the tree has room"))?;
                if position == note.position {
                    witness = IncrementalWitness::from_tree(tree.clone());
                }
            }
            Some(witness) => witness
                .append(*commitment)
                .map_err(|_| eyre!("the witness has room"))?,
        }
    }
    let witness = witness.ok_or_else(|| eyre!("the note is in its block's commitments"))?;
    let path = witness
        .path()
        .ok_or_else(|| eyre!("the witness has a path"))?;

    let spending_key = ironwood_spending_key();
    let mut builder = builder(net, height, TxVersion::V7, Some(witness.root().into()))?;
    builder
        .add_ironwood_spend::<Infallible>(
            FullViewingKey::from(&spending_key),
            note.note,
            path.into(),
        )
        .map_err(|error| eyre!("{error:?}"))?;
    key.add_output(&mut builder, note.value() - IRONWOOD_FEE)?;
    build(
        builder,
        &TransparentSigningSet::new(),
        &[SpendAuthorizingKey::from(&spending_key)],
    )
}

fn ironwood_spending_key() -> SpendingKey {
    SpendingKey::from_bytes([7; 32]).expect("valid spending key")
}

/// The Ironwood address that [`shield_to_ironwood`] pays.
fn ironwood_address() -> orchard::Address {
    FullViewingKey::from(&ironwood_spending_key()).address(
        orchard::keys::Diversifier::from_bytes([0; 11]),
        Scope::External,
    )
}

fn builder(
    net: &Network,
    height: Height,
    version: TxVersion,
    ironwood_anchor: Option<orchard::Anchor>,
) -> Result<Builder<Network, ()>> {
    let mut builder = Builder::new(
        net.clone(),
        height.0.into(),
        BuildConfig::Standard {
            sapling_anchor: None,
            orchard_anchor: None,
            ironwood_anchor,
            orchard_padding: BundlePadding::DEFAULT,
            ironwood_padding: BundlePadding::DEFAULT,
        },
    );
    builder
        .propose_version::<Infallible>(version)
        .map_err(|error| eyre!("{error:?}"))?;
    Ok(builder)
}

fn build(
    builder: Builder<Network, ()>,
    signing_set: &TransparentSigningSet,
    ironwood_keys: &[SpendAuthorizingKey],
) -> Result<CommonBuilt> {
    let result = builder
        .build(
            signing_set,
            &[],
            ironwood_keys,
            rand_10::rng(),
            &NoSaplingProver,
            &NoSaplingProver,
            &FeeRule::standard(),
        )
        .map_err(|error| eyre!("the transaction builds: {error:?}"))?;
    CommonBuilt::new(result)
}

fn zatoshis(value: u64) -> Result<Zatoshis> {
    Zatoshis::from_u64(value).map_err(|error| eyre!("{error:?}"))
}

/// Assembles an unsigned v7 transfer from Zakura's types, which spends `coins` into
/// `outputs` and states `deltas`, whether or not they are right.
pub(crate) fn v7_transfer(
    coins: &[Coin],
    outputs: Vec<Output>,
    deltas: ValueDeltas,
) -> Transaction {
    Transaction::V7 {
        network_upgrade: NetworkUpgrade::Nu7_1,
        lock_time: LockTime::unlocked(),
        expiry_height: Height(0),
        inputs: coins
            .iter()
            .map(|coin| Input::PrevOut {
                outpoint: coin.outpoint,
                unlock_script: Script::new(&[]),
                sequence: u32::MAX,
            })
            .collect(),
        outputs,
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: None,
        v7_fields: Box::new(zakura_chain::transaction::v7::V7Fields {
            value_deltas: deltas,
            coinbase: None,
        }),
    }
}

/// Returns `tx` with `unlock_script` on input `index`.
///
/// v7 signatures do not sign unlock scripts, so this keeps the signatures valid.
pub(crate) fn with_unlock_script(
    mut tx: Transaction,
    index: usize,
    unlock_script: Script,
) -> Transaction {
    let Transaction::V7 { inputs, .. } = &mut tx else {
        unreachable!("the test transactions are v7")
    };
    let Input::PrevOut {
        unlock_script: script,
        ..
    } = &mut inputs[index]
    else {
        unreachable!("the test transactions spend outputs")
    };
    *script = unlock_script;
    tx
}

/// Returns a script that pushes each of `items`, all shorter than `OP_PUSHDATA1`.
pub(crate) fn push_script(items: &[&[u8]]) -> Script {
    let mut bytes = Vec::new();
    for item in items {
        // An empty item pushes the empty vector with OP_0.
        bytes.push(u8::try_from(item.len()).expect("test pushes are short"));
        bytes.extend_from_slice(item);
    }
    Script::new(&bytes)
}

/// Returns the P2SH output script of `redeem_script`.
pub(crate) fn p2sh(redeem_script: &[u8]) -> Script {
    use ripemd::{Digest, Ripemd160};
    use sha2::Sha256;

    let hash: [u8; 20] = Ripemd160::digest(Sha256::digest(redeem_script)).into();
    Address::from_script_hash(NetworkKind::Regtest, hash).script()
}

/// A transparent output of `value` zatoshis to `lock_script`.
pub(crate) fn output(value: u64, lock_script: Script) -> Result<Output> {
    Ok(Output::new(
        Amount::<NonNegative>::try_from(value)?,
        lock_script,
    ))
}

/// Satisfies the builder's Sapling prover bounds: the tests build no Sapling bundles.
struct NoSaplingProver;

impl sapling_crypto::prover::SpendProver for NoSaplingProver {
    type Proof = Infallible;

    fn prepare_circuit(
        _: sapling_crypto::ProofGenerationKey,
        _: sapling_crypto::Diversifier,
        _: sapling_crypto::Rseed,
        _: sapling_crypto::value::NoteValue,
        _: jubjub::Fr,
        _: sapling_crypto::value::ValueCommitTrapdoor,
        _: bls12_381::Scalar,
        _: sapling_crypto::MerklePath,
    ) -> Option<sapling_crypto::circuit::Spend> {
        unreachable!("the tests build no Sapling spends")
    }

    fn create_proof<R: rand_10::Rng>(
        &self,
        _: sapling_crypto::circuit::Spend,
        _: &mut R,
    ) -> Self::Proof {
        unreachable!("the tests build no Sapling spends")
    }

    fn encode_proof(proof: Self::Proof) -> sapling_crypto::bundle::GrothProofBytes {
        match proof {}
    }
}

impl sapling_crypto::prover::OutputProver for NoSaplingProver {
    type Proof = Infallible;

    fn prepare_circuit(
        _: &sapling_crypto::keys::EphemeralSecretKey,
        _: sapling_crypto::PaymentAddress,
        _: jubjub::Fr,
        _: sapling_crypto::value::NoteValue,
        _: sapling_crypto::value::ValueCommitTrapdoor,
    ) -> sapling_crypto::circuit::Output {
        unreachable!("the tests build no Sapling outputs")
    }

    fn create_proof<R: rand_10::Rng>(
        &self,
        _: sapling_crypto::circuit::Output,
        _: &mut R,
    ) -> Self::Proof {
        unreachable!("the tests build no Sapling outputs")
    }

    fn encode_proof(proof: Self::Proof) -> sapling_crypto::bundle::GrothProofBytes {
        match proof {}
    }
}
