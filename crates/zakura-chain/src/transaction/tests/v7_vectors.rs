//! V7 transaction fixtures and cross-implementation serialization vectors.

use group::{
    ff::{FromUniformBytes, PrimeField},
    CurveAffine, GroupEncoding,
};
use halo2::pasta::pallas;
use reddsa::{orchard::SpendAuth, SigningKey, VerificationKey};
use zcash_tachyon::{
    Action, Anchor, Bundle, PointerStamp, ProofStamp, Tachygram, TachygramSetPoly, TachyonBundle,
};

use crate::{
    block::Height,
    parameters::{NetworkUpgrade, TX_V7_VERSION_GROUP_ID},
    serialization::{SerializationError, ZcashDeserialize, ZcashSerialize},
    transaction::{LockTime, TachyonShieldedData, Transaction},
};

fn action_with_seed(seed: u8, signature_byte: u8) -> Action {
    let signing_key = SigningKey::<SpendAuth>::try_from(
        pallas::Scalar::from_uniform_bytes(&[seed; 64]).to_repr(),
    )
    .expect("the reduced scalar is a valid signing key");
    let verification_key = VerificationKey::<SpendAuth>::from(&signing_key);
    let verification_key = Option::<pallas::Affine>::from(pallas::Affine::from_bytes(
        &<[u8; 32]>::from(verification_key),
    ))
    .expect("the verification key is a valid curve point");

    Action {
        cv: zcash_tachyon::value::Commitment::from(pallas::Affine::generator()),
        rk: zcash_tachyon::keys::public::ActionVerificationKey::try_from(verification_key)
            .expect("a non-identity verification key is valid"),
        sig: zcash_tachyon::action::Signature::read(&[signature_byte; 64][..])
            .expect("test signature bytes are canonical"),
    }
}

fn default_anchor() -> Anchor {
    Anchor::read(&[0; 64][..]).expect("64 zero bytes encode the default Tachyon anchor")
}

fn tachygram(seed: u8) -> Tachygram {
    Tachygram::from(pallas::Base::from_uniform_bytes(&[seed; 64]))
}

fn transaction_with_tachyon_bundle(bundle: TachyonBundle) -> Transaction {
    let tachyon_shielded_data = (!bundle.is_no_bundle()).then_some(TachyonShieldedData(bundle));

    Transaction::V7 {
        network_upgrade: NetworkUpgrade::NuTachyon,
        lock_time: LockTime::unlocked(),
        expiry_height: Height(0),
        inputs: Vec::new(),
        outputs: Vec::new(),
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: None,
        tachyon_shielded_data,
    }
}

fn empty_transaction() -> Transaction {
    transaction_with_tachyon_bundle(TachyonBundle::NoBundle)
}

fn adjunct_transaction() -> Transaction {
    transaction_with_tachyon_bundle(TachyonBundle::Adjunct(Bundle {
        actions: vec![action_with_seed(0x42, 0x01)],
        value_balance: zcash_tachyon::value::Balance::ZERO,
        binding_sig: zcash_tachyon::bundle::Signature::read(&[0x02; 64][..])
            .expect("test signature bytes are canonical"),
        memo: Vec::new(),
        stamp: PointerStamp::try_from([0xEE; 64])
            .expect("a 64-byte witnessed transaction ID is valid"),
    }))
}

fn proven_transaction() -> Transaction {
    let tachygram = tachygram(0xAA);

    transaction_with_tachyon_bundle(TachyonBundle::Proven(Bundle {
        actions: vec![action_with_seed(0x42, 0x01)],
        value_balance: zcash_tachyon::value::Balance::try_from(100i64)
            .expect("100 is a valid Tachyon value balance"),
        binding_sig: zcash_tachyon::bundle::Signature::read(&[0x02; 64][..])
            .expect("test signature bytes are canonical"),
        memo: Vec::new(),
        stamp: ProofStamp {
            coverage: [0; 32],
            tachygram_set: [tachygram]
                .into_iter()
                .collect::<TachygramSetPoly>()
                .commit(),
            tachygrams: [tachygram].into_iter().collect(),
            anchor: default_anchor(),
            proof: Box::new(ragu::Proof::trivial()),
        },
    }))
}

fn multi_action_proven_transaction() -> Transaction {
    let tachygrams = [tachygram(0xAA), tachygram(0xCC), tachygram(0xDD)];

    transaction_with_tachyon_bundle(TachyonBundle::Proven(Bundle {
        actions: vec![action_with_seed(0x42, 0x01), action_with_seed(0x43, 0x03)],
        value_balance: zcash_tachyon::value::Balance::try_from(300i64)
            .expect("300 is a valid Tachyon value balance"),
        binding_sig: zcash_tachyon::bundle::Signature::read(&[0x02; 64][..])
            .expect("test signature bytes are canonical"),
        memo: Vec::new(),
        stamp: ProofStamp {
            coverage: [0; 32],
            tachygram_set: tachygrams
                .into_iter()
                .collect::<TachygramSetPoly>()
                .commit(),
            tachygrams: tachygrams.into_iter().collect(),
            anchor: default_anchor(),
            proof: Box::new(ragu::Proof::trivial()),
        },
    }))
}

fn fixtures() -> [(&'static str, Transaction); 4] {
    [
        ("V7_EMPTY", empty_transaction()),
        ("V7_TACHYON_ADJUNCT", adjunct_transaction()),
        ("V7_TACHYON_PROVEN", proven_transaction()),
        (
            "V7_TACHYON_MULTI_ACTION_PROVEN",
            multi_action_proven_transaction(),
        ),
    ]
}

#[test]
fn v7_is_nu_tachyon_gated_and_matches_zakura_primitives() {
    let _init_guard = zakura_test::init();

    assert_eq!(
        TX_V7_VERSION_GROUP_ID,
        zcash_protocol::constants::V7_VERSION_GROUP_ID,
    );
    assert_eq!(
        TX_V7_VERSION_GROUP_ID,
        zcash_primitives::transaction::TxVersion::V7.version_group_id(),
    );

    let pre_nu_tachyon = Transaction::V7 {
        network_upgrade: NetworkUpgrade::Nu6_3,
        lock_time: LockTime::unlocked(),
        expiry_height: Height(0),
        inputs: Vec::new(),
        outputs: Vec::new(),
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: None,
        tachyon_shielded_data: None,
    };
    pre_nu_tachyon
        .zcash_serialize_to_vec()
        .expect_err("V7 must not serialize before NuTachyon");

    let transaction = empty_transaction();
    let bytes = transaction
        .zcash_serialize_to_vec()
        .expect("an empty NuTachyon V7 transaction has a valid wire encoding");
    assert_eq!(&bytes[0..4], &[0x07, 0x00, 0x00, 0x80]);
    assert_eq!(&bytes[4..8], &TX_V7_VERSION_GROUP_ID.to_le_bytes());
    assert_eq!(&bytes[8..12], &0xffff_fffcu32.to_le_bytes());
    assert_eq!(transaction.version_group_id(), Some(TX_V7_VERSION_GROUP_ID));

    let decoded = Transaction::zcash_deserialize(&bytes[..])
        .expect("Zakura must deserialize its NuTachyon V7 encoding");
    assert_eq!(decoded, transaction);
    let zakura_primitives_transaction = transaction
        .to_librustzcash(NetworkUpgrade::NuTachyon)
        .expect("zakura-primitives must accept Zakura's NuTachyon V7 encoding");
    assert_eq!(
        transaction.hash().0,
        *zakura_primitives_transaction.txid().as_ref()
    );
    let zakura_primitives_auth_digest: [u8; 32] = zakura_primitives_transaction
        .auth_commitment()
        .as_ref()
        .try_into()
        .expect("the authorizing-data digest is 32 bytes");
    assert_eq!(
        transaction
            .auth_digest()
            .expect("V7 transactions have an authorizing-data digest")
            .0,
        zakura_primitives_auth_digest,
    );

    let mut pre_nu_tachyon_bytes = bytes;
    let nu6_3_branch_id = u32::from(
        NetworkUpgrade::Nu6_3
            .branch_id()
            .expect("NU6.3 has a branch ID"),
    );
    pre_nu_tachyon_bytes[8..12].copy_from_slice(&nu6_3_branch_id.to_le_bytes());
    let error = Transaction::zcash_deserialize(&pre_nu_tachyon_bytes[..])
        .expect_err("V7 must not deserialize with a pre-NuTachyon branch ID");
    assert!(matches!(
        error,
        SerializationError::Parse(message) if message.contains("NuTachyon")
    ));
}

#[test]
fn v7_tachyon_fixtures_round_trip() {
    let _init_guard = zakura_test::init();

    for (name, transaction) in fixtures() {
        let bytes = transaction
            .zcash_serialize_to_vec()
            .unwrap_or_else(|error| panic!("{name} serialization failed: {error}"));
        let decoded = Transaction::zcash_deserialize(&bytes[..])
            .unwrap_or_else(|error| panic!("{name} deserialization failed: {error}"));
        let reencoded = decoded
            .zcash_serialize_to_vec()
            .unwrap_or_else(|error| panic!("{name} reserialization failed: {error}"));

        assert_eq!(decoded, transaction, "{name} round-trip mismatch");
        assert_eq!(reencoded, bytes, "{name} reserialization differs");
        assert_eq!(decoded.hash(), transaction.hash(), "{name} txid differs");
        assert_eq!(
            decoded.auth_digest(),
            transaction.auth_digest(),
            "{name} authorization digest differs"
        );
    }
}

/// Prints the canonical encodings consumed by `zakura-primitives` tests.
#[test]
#[ignore = "prints V7 serialization vectors for zakura-primitives"]
#[allow(clippy::print_stdout)]
fn generate_v7_tachyon_serialization_vectors() {
    let _init_guard = zakura_test::init();

    for (name, transaction) in fixtures() {
        let bytes = transaction
            .zcash_serialize_to_vec()
            .unwrap_or_else(|error| panic!("{name} serialization failed: {error}"));
        let decoded = Transaction::zcash_deserialize(&bytes[..])
            .unwrap_or_else(|error| panic!("{name} deserialization failed: {error}"));

        assert_eq!(decoded, transaction, "{name} round-trip mismatch");
        println!("{name}={}", hex::encode(bytes));
    }
}
