//! Fixed test vectors for transactions.

use arbitrary::v5_transactions;
use chrono::DateTime;
use color_eyre::eyre::{Result, WrapErr};
use lazy_static::lazy_static;
use rand::{seq::IteratorRandom, thread_rng};
use std::io::ErrorKind;

use crate::{
    block::{Block, Height, MAX_BLOCK_BYTES},
    parameters::Network,
    primitives::zcash_primitives::PrecomputedTxData,
    serialization::{SerializationError, ZcashDeserialize, ZcashDeserializeInto, ZcashSerialize},
    transaction::{sighash::SigHasher, txid::TxIdBuilder},
    transparent::Script,
};

use zebra_test::{
    vectors::{ZIP143_1, ZIP143_2, ZIP243_1, ZIP243_2, ZIP243_3},
    zip0143, zip0243, zip0244,
};

use super::super::*;
use super::ironwood_v6_tx_hash;
lazy_static! {
    pub static ref EMPTY_V5_TX: Transaction = Transaction::V5 {
        network_upgrade: NetworkUpgrade::Nu5,
        lock_time: LockTime::min_lock_time_timestamp(),
        expiry_height: block::Height(0),
        inputs: Vec::new(),
        outputs: Vec::new(),
        sapling_shielded_data: None,
        orchard_shielded_data: None,
    };
}

/// Build a mock output list for pre-V5 transactions, with (index+1)
/// copies of `output`, which is used to computed the sighash.
///
/// Pre-V5, the entire output list is not used; only the output for the
/// given index is read. Therefore, we just need a list where `array[index]`
/// is the given `output`.
fn mock_pre_v5_output_list(output: transparent::Output, index: usize) -> Vec<transparent::Output> {
    std::iter::repeat_n(output, index + 1).collect()
}

#[test]
fn transactionhash_struct_from_str_roundtrip() {
    let _init_guard = zebra_test::init();

    let hash: Hash = "3166411bd5343e0b284a108f39a929fbbb62619784f8c6dafe520703b5b446bf"
        .parse()
        .unwrap();

    assert_eq!(
        format!("{hash:?}"),
        r#"transaction::Hash("3166411bd5343e0b284a108f39a929fbbb62619784f8c6dafe520703b5b446bf")"#
    );
    assert_eq!(
        hash.to_string(),
        "3166411bd5343e0b284a108f39a929fbbb62619784f8c6dafe520703b5b446bf"
    );
}

#[test]
fn auth_digest_struct_from_str_roundtrip() {
    let _init_guard = zebra_test::init();

    let digest: AuthDigest = "3166411bd5343e0b284a108f39a929fbbb62619784f8c6dafe520703b5b446bf"
        .parse()
        .unwrap();

    assert_eq!(
        format!("{digest:?}"),
        r#"AuthDigest("3166411bd5343e0b284a108f39a929fbbb62619784f8c6dafe520703b5b446bf")"#
    );
    assert_eq!(
        digest.to_string(),
        "3166411bd5343e0b284a108f39a929fbbb62619784f8c6dafe520703b5b446bf"
    );
}

#[test]
fn wtx_id_struct_from_str_roundtrip() {
    let _init_guard = zebra_test::init();

    let wtx_id: WtxId = "3166411bd5343e0b284a108f39a929fbbb62619784f8c6dafe520703b5b446bf0000000000000000000000000000000000000000000000000000000000000001"
        .parse()
        .unwrap();

    assert_eq!(
        format!("{wtx_id:?}"),
        r#"WtxId { id: transaction::Hash("3166411bd5343e0b284a108f39a929fbbb62619784f8c6dafe520703b5b446bf"), auth_digest: AuthDigest("0000000000000000000000000000000000000000000000000000000000000001") }"#
    );
    assert_eq!(
        wtx_id.to_string(),
        "3166411bd5343e0b284a108f39a929fbbb62619784f8c6dafe520703b5b446bf0000000000000000000000000000000000000000000000000000000000000001"
    );
}

#[test]
fn librustzcash_tx_deserialize_and_round_trip() {
    let _init_guard = zebra_test::init();

    let tx = Transaction::zcash_deserialize(&zebra_test::vectors::GENERIC_TESTNET_TX[..])
        .expect("transaction test vector from librustzcash should deserialize");

    let mut data2 = Vec::new();
    tx.zcash_serialize(&mut data2).expect("tx should serialize");

    assert_eq!(&zebra_test::vectors::GENERIC_TESTNET_TX[..], &data2[..]);
}

#[test]
fn librustzcash_tx_hash() {
    let _init_guard = zebra_test::init();

    let tx = Transaction::zcash_deserialize(&zebra_test::vectors::GENERIC_TESTNET_TX[..])
        .expect("transaction test vector from librustzcash should deserialize");

    // TxID taken from comment in zebra_test::vectors
    let hash = tx.hash();
    let expected = "64f0bd7fe30ce23753358fe3a2dc835b8fba9c0274c4e2c54a6f73114cb55639"
        .parse::<Hash>()
        .expect("hash should parse correctly");

    assert_eq!(hash, expected);
}

#[test]
fn v5_orchard_cross_address_flag_fails_serialization() {
    let _init_guard = zebra_test::init();

    let mut tx = Network::iter()
        .flat_map(|network| v5_transactions(network.block_iter()))
        .find(|transaction| transaction.orchard_shielded_data().is_some())
        .expect("test vectors include an Orchard transaction");

    let Transaction::V5 {
        orchard_shielded_data: Some(orchard_shielded_data),
        ..
    } = &mut tx
    else {
        unreachable!("test transaction is V5 with Orchard shielded data");
    };

    orchard_shielded_data
        .flags
        .insert(crate::orchard::Flags::ENABLE_CROSS_ADDRESS);

    let error = tx
        .zcash_serialize_to_vec()
        .expect_err("V5 Orchard flags must reject reserved cross-address bit");

    assert_eq!(error.kind(), ErrorKind::InvalidData);
}

#[test]
fn doesnt_deserialize_transaction_with_invalid_value_balance() {
    let _init_guard = zebra_test::init();

    let dummy_transaction = Transaction::V4 {
        inputs: vec![],
        outputs: vec![],
        lock_time: LockTime::Height(Height(1)),
        expiry_height: Height(10),
        joinsplit_data: None,
        sapling_shielded_data: None,
    };

    let mut input_bytes = Vec::new();
    dummy_transaction
        .zcash_serialize(&mut input_bytes)
        .expect("dummy transaction should serialize");
    // Set value balance to non-zero
    // There are 4 * 4 byte fields and 2 * 1 byte compact sizes = 18 bytes before the 8 byte amount
    // (Zcash is little-endian unless otherwise specified:
    // https://zips.z.cash/protocol/nu5.pdf#endian)
    input_bytes[18] = 1;

    let result = Transaction::zcash_deserialize(&input_bytes[..]);

    assert!(matches!(
        result,
        Err(SerializationError::BadTransactionBalance)
    ));
}

#[test]
fn zip143_deserialize_and_round_trip() {
    let _init_guard = zebra_test::init();

    let tx1 = Transaction::zcash_deserialize(&zebra_test::vectors::ZIP143_1[..])
        .expect("transaction test vector from ZIP143 should deserialize");

    let mut data1 = Vec::new();
    tx1.zcash_serialize(&mut data1)
        .expect("tx should serialize");

    assert_eq!(&zebra_test::vectors::ZIP143_1[..], &data1[..]);

    let tx2 = Transaction::zcash_deserialize(&zebra_test::vectors::ZIP143_2[..])
        .expect("transaction test vector from ZIP143 should deserialize");

    let mut data2 = Vec::new();
    tx2.zcash_serialize(&mut data2)
        .expect("tx should serialize");

    assert_eq!(&zebra_test::vectors::ZIP143_2[..], &data2[..]);
}

#[test]
fn zip243_deserialize_and_round_trip() {
    let _init_guard = zebra_test::init();

    let tx1 = Transaction::zcash_deserialize(&zebra_test::vectors::ZIP243_1[..])
        .expect("transaction test vector from ZIP243 should deserialize");

    let mut data1 = Vec::new();
    tx1.zcash_serialize(&mut data1)
        .expect("tx should serialize");

    assert_eq!(&zebra_test::vectors::ZIP243_1[..], &data1[..]);

    let tx2 = Transaction::zcash_deserialize(&zebra_test::vectors::ZIP243_2[..])
        .expect("transaction test vector from ZIP243 should deserialize");

    let mut data2 = Vec::new();
    tx2.zcash_serialize(&mut data2)
        .expect("tx should serialize");

    assert_eq!(&zebra_test::vectors::ZIP243_2[..], &data2[..]);

    let tx3 = Transaction::zcash_deserialize(&zebra_test::vectors::ZIP243_3[..])
        .expect("transaction test vector from ZIP243 should deserialize");

    let mut data3 = Vec::new();
    tx3.zcash_serialize(&mut data3)
        .expect("tx should serialize");

    assert_eq!(&zebra_test::vectors::ZIP243_3[..], &data3[..]);
}

#[test]
fn deserialize_large_transaction() {
    let _init_guard = zebra_test::init();

    // Create a dummy input and output.
    let input =
        transparent::Input::zcash_deserialize(&zebra_test::vectors::DUMMY_INPUT1[..]).unwrap();
    let output =
        transparent::Output::zcash_deserialize(&zebra_test::vectors::DUMMY_OUTPUT1[..]).unwrap();

    // Serialize the input so that we can determine its serialized size.
    let mut input_data = Vec::new();
    input
        .zcash_serialize(&mut input_data)
        .expect("input should serialize");

    // Calculate the number of inputs that fit into the transaction size limit.
    let tx_inputs_num = MAX_BLOCK_BYTES as usize / input_data.len();

    // Set the precalculated amount of inputs and a single output.
    let inputs = std::iter::repeat_n(input, tx_inputs_num).collect::<Vec<_>>();

    // Create an oversized transaction. Adding the output and lock time causes
    // the transaction to overflow the threshold.
    let oversized_tx = Transaction::V1 {
        inputs,
        outputs: vec![output],
        lock_time: LockTime::Time(DateTime::from_timestamp(61, 0).unwrap()),
    };

    // Serialize the transaction.
    let mut tx_data = Vec::new();
    oversized_tx
        .zcash_serialize(&mut tx_data)
        .expect("transaction should serialize");

    // Check that the transaction is oversized.
    assert!(tx_data.len() > MAX_BLOCK_BYTES as usize);

    // The deserialization should fail because the transaction is too big.
    Transaction::zcash_deserialize(&tx_data[..])
        .expect_err("transaction should not deserialize due to its size");
}

// Transaction V5 test vectors

/// An empty transaction v5, with no Orchard, Sapling, or Transparent data
///
/// empty transaction are invalid, but Zebra only checks this rule in
/// zebra_consensus::transaction::Verifier
#[test]
fn empty_v5_round_trip() {
    let _init_guard = zebra_test::init();

    let tx: &Transaction = &EMPTY_V5_TX;

    let data = tx.zcash_serialize_to_vec().expect("tx should serialize");
    let tx2: &Transaction = &data
        .zcash_deserialize_into()
        .expect("tx should deserialize");

    assert_eq!(tx, tx2);

    let data2 = tx2
        .zcash_serialize_to_vec()
        .expect("vec serialization is infallible");

    assert_eq!(data, data2, "data must be equal if structs are equal");
}

/// An empty transaction v4, with no Sapling, Sprout, or Transparent data
///
/// empty transaction are invalid, but Zebra only checks this rule in
/// zebra_consensus::transaction::Verifier
#[test]
fn empty_v4_round_trip() {
    let _init_guard = zebra_test::init();

    let tx = Transaction::V4 {
        inputs: Vec::new(),
        outputs: Vec::new(),
        lock_time: LockTime::min_lock_time_timestamp(),
        expiry_height: block::Height(0),
        joinsplit_data: None,
        sapling_shielded_data: None,
    };

    let data = tx.zcash_serialize_to_vec().expect("tx should serialize");
    let tx2 = data
        .zcash_deserialize_into()
        .expect("tx should deserialize");

    assert_eq!(tx, tx2);

    let data2 = tx2
        .zcash_serialize_to_vec()
        .expect("vec serialization is infallible");

    assert_eq!(data, data2, "data must be equal if structs are equal");
}

/// Check if an empty V5 transaction can be deserialized by librustzcash too.
#[test]
fn empty_v5_librustzcash_round_trip() {
    let _init_guard = zebra_test::init();

    let tx: &Transaction = &EMPTY_V5_TX;
    let nu = tx.network_upgrade().expect("network upgrade");

    tx.to_librustzcash(nu).expect(
        "librustzcash deserialization might work for empty zebra serialized transactions. \
        Hint: if empty transactions fail, but other transactions work, delete this test",
    );
}

#[test]
fn invalid_orchard_nullifier() {
    let _init_guard = zebra_test::init();

    use std::convert::TryFrom;

    // generated by proptest using something as:
    // ```rust
    // ...
    // array::uniform32(any::<u8>()).prop_map(|x| Self::try_from(x).unwrap()).boxed()
    // ...
    // ```
    let invalid_nullifier_bytes = [
        62, 157, 27, 63, 100, 228, 1, 82, 140, 16, 238, 78, 68, 19, 221, 184, 189, 207, 230, 95,
        194, 216, 165, 24, 110, 221, 139, 195, 106, 98, 192, 71,
    ];

    assert_eq!(
        orchard::Nullifier::try_from(invalid_nullifier_bytes)
            .err()
            .unwrap()
            .to_string(),
        SerializationError::Parse("Invalid pallas::Base value for orchard Nullifier").to_string()
    );
}

/// Do a round-trip test via librustzcash on fake v5 transactions created from v4 transactions
/// in the block test vectors.
/// Makes sure that zebra-serialized transactions can be deserialized by librustzcash.
#[test]
fn fake_v5_librustzcash_round_trip() {
    let _init_guard = zebra_test::init();
    for network in Network::iter() {
        fake_v5_librustzcash_round_trip_for_network(network);
    }
}

fn fake_v5_librustzcash_round_trip_for_network(network: Network) {
    let block_iter = network.block_iter();

    let overwinter_activation_height = NetworkUpgrade::Overwinter
        .activation_height(&network)
        .expect("a valid height")
        .0;

    let nu5_activation_height = NetworkUpgrade::Nu5
        .activation_height(&network)
        .unwrap_or(Height::MAX_EXPIRY_HEIGHT)
        .0;

    // skip blocks that are before overwinter as they will not have a valid consensus branch id
    // skip blocks equal or greater Nu5 activation as they are already v5 transactions
    let blocks_after_overwinter_and_before_nu5 = block_iter
        .skip_while(|(height, _)| **height < overwinter_activation_height)
        .take_while(|(height, _)| **height < nu5_activation_height);

    for (height, original_bytes) in blocks_after_overwinter_and_before_nu5 {
        let original_block = original_bytes
            .zcash_deserialize_into::<Block>()
            .expect("block is structurally valid");

        let mut fake_block = original_block.clone();
        fake_block.transactions = fake_block
            .transactions
            .iter()
            .map(AsRef::as_ref)
            .map(|t| arbitrary::transaction_to_fake_v5(t, &network, Height(*height)))
            .map(Into::into)
            .collect();

        // test each transaction
        for (original_tx, fake_tx) in original_block
            .transactions
            .iter()
            .zip(fake_block.transactions.iter())
        {
            assert_ne!(
                &original_tx, &fake_tx,
                "v1-v4 transactions must change when converted to fake v5"
            );

            let fake_bytes = fake_tx
                .zcash_serialize_to_vec()
                .expect("vec serialization is infallible");

            assert_ne!(
                &original_bytes[..],
                fake_bytes,
                "v1-v4 transaction data must change when converted to fake v5"
            );

            let nu = fake_tx.network_upgrade().expect("network upgrade");

            fake_tx
                .to_librustzcash(nu)
                .expect("librustzcash deserialization must work for zebra serialized transactions");
        }
    }
}

#[test]
fn zip244_round_trip() -> Result<()> {
    let _init_guard = zebra_test::init();

    for test in zip0244::TEST_VECTORS.iter() {
        let tx = test.tx.zcash_deserialize_into::<Transaction>()?;
        let reencoded = tx.zcash_serialize_to_vec()?;

        assert_eq!(test.tx, reencoded);

        let nu = tx.network_upgrade().expect("network upgrade");

        tx.to_librustzcash(nu)
            .expect("librustzcash deserialization must work for zebra serialized transactions");
    }

    Ok(())
}

#[test]
fn zip244_txid() -> Result<()> {
    let _init_guard = zebra_test::init();

    for test in zip0244::TEST_VECTORS.iter() {
        let txid = TxIdBuilder::new(&test.tx.zcash_deserialize_into::<Transaction>()?)
            .txid()
            .expect("txid");

        assert_eq!(txid.0, test.txid);
    }

    Ok(())
}

#[test]
fn zip244_auth_digest() -> Result<()> {
    let _init_guard = zebra_test::init();

    for test in zip0244::TEST_VECTORS.iter() {
        let transaction = test.tx.zcash_deserialize_into::<Transaction>()?;
        let auth_digest = transaction.auth_digest();
        assert_eq!(
            auth_digest
                .expect("must have auth_digest since it must be a V5 transaction")
                .0,
            test.auth_digest
        );
    }

    Ok(())
}

/// Known-answer sanity check for the native ZIP-244 digest path
/// (`transaction::zip244`): for V5, the digests it computes directly from
/// Zebra's parsed transaction must equal the txid and authorizing-data digest
/// published in the official ZIP-244 test vectors.
///
/// The `native_zip244_matches_librustzcash` property test proves the native
/// path agrees with the `librustzcash` conversion it replaces; this test pins
/// both implementations to the independently-published expected outputs, so a
/// shared bug in the two V5 computations could not pass silently.
#[test]
fn native_zip244_matches_test_vectors() -> Result<()> {
    let _init_guard = zebra_test::init();

    for test in zip0244::TEST_VECTORS.iter() {
        assert_native_zip244_matches_test_vector(
            &test.tx,
            test.txid,
            test.auth_digest,
            "ZIP-244 V5",
        )?;
    }

    for test in ironwood_v6_tx_hash::TEST_VECTORS.iter() {
        let mut tx_bytes = test.tx.to_vec();
        // These vectors were generated before the V6 version group ID and
        // NU6.3 branch ID were finalized.
        tx_bytes[4..8].copy_from_slice(&crate::parameters::TX_V6_VERSION_GROUP_ID.to_le_bytes());
        tx_bytes[8..12].copy_from_slice(
            &u32::from(
                NetworkUpgrade::Nu6_3
                    .branch_id()
                    .expect("NU6.3 has a consensus branch ID"),
            )
            .to_le_bytes(),
        );

        assert_native_zip244_matches_librustzcash_for_test_vector(&tx_bytes, test.scenario)?;
    }

    Ok(())
}

fn assert_native_zip244_matches_test_vector(
    tx_bytes: &[u8],
    expected_txid: [u8; 32],
    expected_auth_digest: [u8; 32],
    vector_name: &str,
) -> Result<()> {
    let tx = tx_bytes
        .zcash_deserialize_into::<Transaction>()
        .wrap_err_with(|| format!("failed to deserialize {vector_name}"))?;

    let (txid, auth_digest) = crate::transaction::zip244::txid_and_auth_digest(&tx)
        .expect("test vectors are v5/v6 transactions with native ZIP-244 digests");

    assert_eq!(
        txid.0, expected_txid,
        "native txid must match the {vector_name} test vector"
    );
    assert_eq!(
        auth_digest.0, expected_auth_digest,
        "native auth digest must match the {vector_name} test vector"
    );

    // The separate native entry points must agree with the combined one.
    assert_eq!(crate::transaction::zip244::txid(&tx).expect("v5/v6"), txid);
    assert_eq!(
        crate::transaction::zip244::auth_digest(&tx).expect("v5/v6"),
        auth_digest
    );

    Ok(())
}

fn assert_native_zip244_matches_librustzcash_for_test_vector(
    tx_bytes: &[u8],
    vector_name: &str,
) -> Result<()> {
    let tx = tx_bytes
        .zcash_deserialize_into::<Transaction>()
        .wrap_err_with(|| format!("failed to deserialize {vector_name}"))?;

    let (native_txid, native_auth_digest) = crate::transaction::zip244::txid_and_auth_digest(&tx)
        .expect("test vectors are v6 transactions with native ZIP-244 digests");
    let (librustzcash_txid, librustzcash_auth_digest) =
        crate::primitives::zcash_primitives::txid_and_auth_digest_via_librustzcash(&tx);

    assert_eq!(
        native_txid, librustzcash_txid,
        "native txid must match librustzcash for the {vector_name} test vector"
    );
    assert_eq!(
        native_auth_digest, librustzcash_auth_digest,
        "native auth digest must match librustzcash for the {vector_name} test vector"
    );

    // The separate native entry points must agree with the combined one.
    assert_eq!(
        crate::transaction::zip244::txid(&tx).expect("v6"),
        native_txid
    );
    assert_eq!(
        crate::transaction::zip244::auth_digest(&tx).expect("v6"),
        native_auth_digest
    );

    Ok(())
}

#[test]
fn test_vec143_1() -> Result<()> {
    let _init_guard = zebra_test::init();

    let transaction = ZIP143_1.zcash_deserialize_into::<Transaction>()?;

    let hasher = SigHasher::new(
        &transaction,
        NetworkUpgrade::Overwinter,
        Arc::new(Vec::new()),
    )
    .expect("network upgrade is valid for tx");

    let hash = hasher.sighash(HashType::ALL, None);
    let expected = "a1f1a4e5cd9bd522322d661edd2af1bf2a7019cfab94ece18f4ba935b0a19073";
    let result = hex::encode(hash);
    let span = tracing::span!(
        tracing::Level::ERROR,
        "compare_final",
        expected.len = expected.len(),
        buf.len = result.len()
    );
    let _guard = span.enter();
    assert_eq!(expected, result);

    Ok(())
}

#[test]
fn test_vec143_2() -> Result<()> {
    let _init_guard = zebra_test::init();

    let transaction = ZIP143_2.zcash_deserialize_into::<Transaction>()?;

    let value = hex::decode("2f6e04963b4c0100")?.zcash_deserialize_into::<Amount<_>>()?;
    let lock_script = Script::new(&hex::decode("53")?);
    let input_ind = 1;
    let output = transparent::Output {
        value,
        lock_script: lock_script.clone(),
    };
    let all_previous_outputs = mock_pre_v5_output_list(output, input_ind);

    let hasher = SigHasher::new(
        &transaction,
        NetworkUpgrade::Overwinter,
        Arc::new(all_previous_outputs),
    )
    .expect("network upgrade is valid for tx");

    let hash = hasher.sighash(
        HashType::SINGLE,
        Some((input_ind, lock_script.as_raw_bytes().to_vec())),
    );
    let expected = "23652e76cb13b85a0e3363bb5fca061fa791c40c533eccee899364e6e60bb4f7";
    let result: &[u8] = hash.as_ref();
    let result = hex::encode(result);
    let span = tracing::span!(
        tracing::Level::ERROR,
        "compare_final",
        expected.len = expected.len(),
        buf.len = result.len()
    );
    let _guard = span.enter();
    assert_eq!(expected, result);

    Ok(())
}

#[test]
fn test_vec243_1() -> Result<()> {
    let _init_guard = zebra_test::init();

    let transaction = ZIP243_1.zcash_deserialize_into::<Transaction>()?;

    let hasher = SigHasher::new(&transaction, NetworkUpgrade::Sapling, Arc::new(Vec::new()))
        .expect("network upgrade is valid for tx");

    let hash = hasher.sighash(HashType::ALL, None);
    let expected = "63d18534de5f2d1c9e169b73f9c783718adbef5c8a7d55b5e7a37affa1dd3ff3";
    let result = hex::encode(hash);
    let span = tracing::span!(
        tracing::Level::ERROR,
        "compare_final",
        expected.len = expected.len(),
        buf.len = result.len()
    );
    let _guard = span.enter();
    assert_eq!(expected, result);

    let precomputed_tx_data =
        PrecomputedTxData::new(&transaction, NetworkUpgrade::Sapling, Arc::new(Vec::new()))
            .expect("network upgrade is valid for tx");
    let alt_sighash =
        crate::primitives::zcash_primitives::sighash(&precomputed_tx_data, HashType::ALL, None);
    let result = hex::encode(alt_sighash);
    assert_eq!(expected, result);

    Ok(())
}

#[test]
fn test_vec243_2() -> Result<()> {
    let _init_guard = zebra_test::init();

    let transaction = ZIP243_2.zcash_deserialize_into::<Transaction>()?;

    let value = hex::decode("adedf02996510200")?.zcash_deserialize_into::<Amount<_>>()?;
    let lock_script = Script::new(&[]);
    let input_ind = 1;
    let output = transparent::Output {
        value,
        lock_script: lock_script.clone(),
    };
    let all_previous_outputs = mock_pre_v5_output_list(output, input_ind);

    let hasher = SigHasher::new(
        &transaction,
        NetworkUpgrade::Sapling,
        Arc::new(all_previous_outputs),
    )
    .expect("network upgrade is valid for tx");

    let hash = hasher.sighash(
        HashType::NONE,
        Some((input_ind, lock_script.as_raw_bytes().to_vec())),
    );
    let expected = "bbe6d84f57c56b29b914c694baaccb891297e961de3eb46c68e3c89c47b1a1db";
    let result = hex::encode(hash);
    let span = tracing::span!(
        tracing::Level::ERROR,
        "compare_final",
        expected.len = expected.len(),
        buf.len = result.len()
    );
    let _guard = span.enter();
    assert_eq!(expected, result);

    let lock_script = Script::new(&[]);
    let prevout = transparent::Output {
        value,
        lock_script: lock_script.clone(),
    };
    let index = input_ind;
    let all_previous_outputs = mock_pre_v5_output_list(prevout, input_ind);

    let precomputed_tx_data = PrecomputedTxData::new(
        &transaction,
        NetworkUpgrade::Sapling,
        Arc::new(all_previous_outputs),
    )
    .expect("network upgrade is valid for tx");
    let alt_sighash = crate::primitives::zcash_primitives::sighash(
        &precomputed_tx_data,
        HashType::NONE,
        Some((index, lock_script.as_raw_bytes().to_vec())),
    );
    let result = hex::encode(alt_sighash);
    assert_eq!(expected, result);

    Ok(())
}

#[test]
fn test_vec243_3() -> Result<()> {
    let _init_guard = zebra_test::init();

    let transaction = ZIP243_3.zcash_deserialize_into::<Transaction>()?;

    let value = hex::decode("80f0fa0200000000")?.zcash_deserialize_into::<Amount<_>>()?;
    let lock_script = Script::new(&hex::decode(
        "76a914507173527b4c3318a2aecd793bf1cfed705950cf88ac",
    )?);
    let input_ind = 0;
    let all_previous_outputs = vec![transparent::Output {
        value,
        lock_script: lock_script.clone(),
    }];

    let hasher = SigHasher::new(
        &transaction,
        NetworkUpgrade::Sapling,
        Arc::new(all_previous_outputs),
    )
    .expect("network upgrade is valid for tx");

    let hash = hasher.sighash(
        HashType::ALL,
        Some((input_ind, lock_script.as_raw_bytes().to_vec())),
    );
    let expected = "f3148f80dfab5e573d5edfe7a850f5fd39234f80b5429d3a57edcc11e34c585b";
    let result = hex::encode(hash);
    let span = tracing::span!(
        tracing::Level::ERROR,
        "compare_final",
        expected.len = expected.len(),
        buf.len = result.len()
    );
    let _guard = span.enter();
    assert_eq!(expected, result);

    let lock_script = Script::new(&hex::decode(
        "76a914507173527b4c3318a2aecd793bf1cfed705950cf88ac",
    )?);
    let prevout = transparent::Output {
        value,
        lock_script: lock_script.clone(),
    };
    let index = input_ind;

    let all_previous_outputs = vec![prevout];
    let precomputed_tx_data = PrecomputedTxData::new(
        &transaction,
        NetworkUpgrade::Sapling,
        Arc::new(all_previous_outputs),
    )
    .expect("network upgrade is valid for tx");
    let alt_sighash = crate::primitives::zcash_primitives::sighash(
        &precomputed_tx_data,
        HashType::ALL,
        Some((index, lock_script.as_raw_bytes().to_vec())),
    );
    let result = hex::encode(alt_sighash);
    assert_eq!(expected, result);

    Ok(())
}

#[test]
fn zip143_sighash() -> Result<()> {
    let _init_guard = zebra_test::init();

    for (i, test) in zip0143::TEST_VECTORS.iter().enumerate() {
        let transaction = test.tx.zcash_deserialize_into::<Transaction>()?;
        let (input_index, output) = match test.transparent_input {
            Some(transparent_input) => (
                Some(transparent_input as usize),
                Some(transparent::Output {
                    value: test.amount.try_into()?,
                    lock_script: transparent::Script::new(test.script_code.as_ref()),
                }),
            ),
            None => (None, None),
        };
        let all_previous_outputs: Vec<_> = match output.clone() {
            Some(output) => mock_pre_v5_output_list(output, input_index.unwrap()),
            None => vec![],
        };
        let result = hex::encode(
            transaction
                .sighash(
                    NetworkUpgrade::try_from(test.consensus_branch_id).expect("network upgrade"),
                    HashType::from_bits(test.hash_type).expect("must be a valid HashType"),
                    Arc::new(all_previous_outputs),
                    input_index.map(|input_index| {
                        (
                            input_index,
                            output.unwrap().lock_script.as_raw_bytes().to_vec(),
                        )
                    }),
                )
                .expect("network upgrade is valid for tx"),
        );
        let expected = hex::encode(test.sighash);
        assert_eq!(expected, result, "test #{i}: sighash does not match");
    }

    Ok(())
}

#[test]
fn zip243_sighash() -> Result<()> {
    let _init_guard = zebra_test::init();

    for (i, test) in zip0243::TEST_VECTORS.iter().enumerate() {
        let transaction = test.tx.zcash_deserialize_into::<Transaction>()?;
        let (input_index, output) = match test.transparent_input {
            Some(transparent_input) => (
                Some(transparent_input as usize),
                Some(transparent::Output {
                    value: test.amount.try_into()?,
                    lock_script: transparent::Script::new(test.script_code.as_ref()),
                }),
            ),
            None => (None, None),
        };
        let all_previous_outputs: Vec<_> = match output.clone() {
            Some(output) => mock_pre_v5_output_list(output, input_index.unwrap()),
            None => vec![],
        };
        let result = hex::encode(
            transaction
                .sighash(
                    NetworkUpgrade::try_from(test.consensus_branch_id).expect("network upgrade"),
                    HashType::from_bits(test.hash_type).expect("must be a valid HashType"),
                    Arc::new(all_previous_outputs),
                    input_index.map(|input_index| {
                        (
                            input_index,
                            output.unwrap().lock_script.as_raw_bytes().to_vec(),
                        )
                    }),
                )
                .expect("network upgrade is valid for tx"),
        );
        let expected = hex::encode(test.sighash);
        assert_eq!(expected, result, "test #{i}: sighash does not match");
    }

    Ok(())
}

#[test]
fn zip244_sighash() -> Result<()> {
    let _init_guard = zebra_test::init();

    for (i, test) in zip0244::TEST_VECTORS.iter().enumerate() {
        let transaction = test.tx.zcash_deserialize_into::<Transaction>()?;

        let all_previous_outputs: Arc<Vec<_>> = Arc::new(
            test.amounts
                .iter()
                .zip(test.script_pubkeys.iter())
                .map(|(amount, script_pubkey)| transparent::Output {
                    value: (*amount).try_into().unwrap(),
                    lock_script: transparent::Script::new(script_pubkey.as_ref()),
                })
                .collect(),
        );

        let result = hex::encode(
            transaction
                .sighash(
                    NetworkUpgrade::Nu5,
                    HashType::ALL,
                    all_previous_outputs.clone(),
                    None,
                )
                .expect("network upgrade is valid for tx"),
        );
        let expected = hex::encode(test.sighash_shielded);
        assert_eq!(expected, result, "test #{i}: sighash does not match");

        if let Some(sighash_all) = test.sighash_all {
            let result = hex::encode(
                transaction
                    .sighash(
                        NetworkUpgrade::Nu5,
                        HashType::ALL,
                        all_previous_outputs,
                        test.transparent_input
                            .map(|idx| (idx as _, test.script_pubkeys[idx as usize].clone())),
                    )
                    .expect("network upgrade is valid for tx"),
            );
            let expected = hex::encode(sighash_all);
            assert_eq!(expected, result, "test #{i}: sighash does not match");
        }
    }

    Ok(())
}

/// Real Orchard proofs from mined transactions must have the canonical size, and padding
/// a proof with trailing bytes must make it non-canonical (GHSA-jfw5-j458-pfv6). This
/// also cross-checks `expected_proof_size` against real proofs produced by the chain.
#[test]
fn orchard_proof_size_is_canonical() {
    let mut checked = 0;

    for net in Network::iter() {
        for tx in v5_transactions(net.block_iter()) {
            let Some(shielded_data) = tx.orchard_shielded_data() else {
                continue;
            };

            // A real, mined Orchard proof has the canonical length for its actions.
            assert!(
                shielded_data.proof_size_is_canonical(),
                "a real Orchard proof should be canonically sized"
            );

            // Padding the proof with trailing data must break canonicity.
            let mut padded = shielded_data.clone();
            padded.proof.0.push(0);
            assert!(
                !padded.proof_size_is_canonical(),
                "a padded Orchard proof must not be considered canonical"
            );

            checked += 1;
        }
    }

    assert!(
        checked > 0,
        "expected at least one Orchard transaction in the test vectors"
    );
}

#[test]
fn consensus_branch_id() {
    for net in Network::iter() {
        for tx in v5_transactions(net.block_iter()).filter(|tx| {
            !tx.has_transparent_inputs() && tx.has_shielded_data() && tx.network_upgrade().is_some()
        }) {
            let tx_nu = tx
                .network_upgrade()
                .expect("this test shouldn't use txs without a network upgrade");

            let any_other_nu = NetworkUpgrade::iter()
                .filter(|&nu| nu != tx_nu)
                .choose(&mut thread_rng())
                .expect("there must be a network upgrade other than the tx one");

            // All computations should succeed under the tx nu.

            tx.to_librustzcash(tx_nu)
                .expect("tx is convertible under tx nu");
            PrecomputedTxData::new(&tx, tx_nu, Arc::new(Vec::new()))
                .expect("network upgrade is valid for tx");
            sighash::SigHasher::new(&tx, tx_nu, Arc::new(Vec::new()))
                .expect("network upgrade is valid for tx");
            tx.sighash(tx_nu, HashType::ALL, Arc::new(Vec::new()), None)
                .expect("network upgrade is valid for tx");

            // All computations should fail under an nu other than the tx one.

            tx.to_librustzcash(any_other_nu)
                .expect_err("tx is not convertible under nu other than the tx one");

            let err = PrecomputedTxData::new(&tx, any_other_nu, Arc::new(Vec::new())).unwrap_err();
            assert!(
                matches!(err, crate::Error::InvalidConsensusBranchId),
                "precomputing tx sighash data errors under nu other than the tx one"
            );

            let err = sighash::SigHasher::new(&tx, any_other_nu, Arc::new(Vec::new())).unwrap_err();
            assert!(
                matches!(err, crate::Error::InvalidConsensusBranchId),
                "creating the sighasher errors under nu other than the tx one"
            );

            let err = tx
                .sighash(any_other_nu, HashType::ALL, Arc::new(Vec::new()), None)
                .unwrap_err();
            assert!(
                matches!(err, crate::Error::InvalidConsensusBranchId),
                "the sighash computation errors under nu other than the tx one"
            );
        }
    }
}

#[test]
fn binding_signatures() {
    let _init_guard = zebra_test::init();

    for net in Network::iter() {
        let sapling_activation_height = NetworkUpgrade::Sapling
            .activation_height(&net)
            .expect("a valid height")
            .0;

        let mut at_least_one_v4_checked = false;
        let mut at_least_one_v5_checked = false;

        for (height, block) in net
            .block_iter()
            .skip_while(|(height, _)| **height < sapling_activation_height)
        {
            let nu = NetworkUpgrade::current(&net, Height(*height));

            for tx in block
                .zcash_deserialize_into::<Block>()
                .expect("a valid block")
                .transactions
            {
                match &*tx {
                    Transaction::V1 { .. } | Transaction::V2 { .. } | Transaction::V3 { .. } => (),
                    Transaction::V4 {
                        sapling_shielded_data,
                        ..
                    } => {
                        if let Some(sapling_shielded_data) = sapling_shielded_data {
                            let sighash = tx
                                .sighash(nu, HashType::ALL, Arc::new(Vec::new()), None)
                                .expect("network upgrade is valid for tx");

                            let bvk = redjubjub::VerificationKey::try_from(
                                sapling_shielded_data
                                    .binding_verification_key()
                                    .expect("test transaction has valid value commitments"),
                            )
                            .expect("a valid redjubjub::VerificationKey");

                            bvk.verify(sighash.as_ref(), &sapling_shielded_data.binding_sig)
                                .expect("must pass verification");

                            at_least_one_v4_checked = true;
                        }
                    }
                    Transaction::V5 {
                        sapling_shielded_data,
                        ..
                    } => {
                        if let Some(sapling_shielded_data) = sapling_shielded_data {
                            // V5 txs have the outputs spent by their transparent inputs hashed into
                            // their SIGHASH, so we need to exclude txs with transparent inputs.
                            //
                            // References:
                            //
                            // <https://zips.z.cash/zip-0244#s-2c-amounts-sig-digest>
                            // <https://zips.z.cash/zip-0244#s-2d-scriptpubkeys-sig-digest>
                            if tx.has_transparent_inputs() {
                                continue;
                            }

                            let sighash = tx
                                .sighash(nu, HashType::ALL, Arc::new(Vec::new()), None)
                                .expect("network upgrade is valid for tx");

                            let bvk = redjubjub::VerificationKey::try_from(
                                sapling_shielded_data
                                    .binding_verification_key()
                                    .expect("test transaction has valid value commitments"),
                            )
                            .expect("a valid redjubjub::VerificationKey");

                            bvk.verify(sighash.as_ref(), &sapling_shielded_data.binding_sig)
                                .expect("verification passes");

                            at_least_one_v5_checked = true;
                        }
                    }
                    Transaction::V6 {
                        sapling_shielded_data,
                        ..
                    } => {
                        if let Some(sapling_shielded_data) = sapling_shielded_data {
                            // V6 txs have the outputs spent by their transparent inputs hashed into
                            // their SIGHASH, so we need to exclude txs with transparent inputs.
                            //
                            // References:
                            //
                            // <https://zips.z.cash/zip-0244#s-2c-amounts-sig-digest>
                            // <https://zips.z.cash/zip-0244#s-2d-scriptpubkeys-sig-digest>
                            if tx.has_transparent_inputs() {
                                continue;
                            }

                            let sighash = tx
                                .sighash(nu, HashType::ALL, Arc::new(Vec::new()), None)
                                .expect("network upgrade is valid for tx");

                            let bvk = redjubjub::VerificationKey::try_from(
                                sapling_shielded_data
                                    .binding_verification_key()
                                    .expect("test transaction has valid value commitments"),
                            )
                            .expect("a valid redjubjub::VerificationKey");

                            bvk.verify(sighash.as_ref(), &sapling_shielded_data.binding_sig)
                                .expect("verification passes");
                        }
                    }
                }
            }
        }

        assert!(at_least_one_v4_checked);
        assert!(at_least_one_v5_checked);
    }
}

#[test]
fn test_coinbase_script() -> Result<()> {
    let _init_guard = zebra_test::init();

    let tx = hex::decode("0400008085202f89010000000000000000000000000000000000000000000000000000000000000000ffffffff0503b0e72100ffffffff04e8bbe60e000000001976a914ba92ff06081d5ff6542af8d3b2d209d29ba6337c88ac40787d010000000017a914931fec54c1fea86e574462cc32013f5400b891298738c94d010000000017a914c7a4285ed7aed78d8c0e28d7f1839ccb4046ab0c87286bee000000000017a914d45cb1adffb5215a42720532a076f02c7c778c908700000000b0e721000000000000000000000000").unwrap();

    let transaction = tx.zcash_deserialize_into::<Transaction>()?;

    let recoded_tx = transaction.zcash_serialize_to_vec().unwrap();
    assert_eq!(tx, recoded_tx);

    let data = transaction.inputs()[0].coinbase_script().unwrap();
    let expected = hex::decode("03b0e72100").unwrap();
    assert_eq!(data, expected);

    Ok(())
}

#[test]
fn v6_transactions_accept_nu6_3_and_later_branch_ids() {
    use crate::parameters::TX_V6_VERSION_GROUP_ID;

    let _init_guard = zebra_test::init();

    let empty_v6_transaction_bytes = |branch_id| {
        let mut tx_bytes = Vec::new();
        tx_bytes.extend_from_slice(&((1u32 << 31) | 6).to_le_bytes());
        tx_bytes.extend_from_slice(&TX_V6_VERSION_GROUP_ID.to_le_bytes());
        tx_bytes.extend_from_slice(&u32::from(branch_id).to_le_bytes());
        tx_bytes.extend_from_slice(&0u32.to_le_bytes());
        tx_bytes.extend_from_slice(&0u32.to_le_bytes());
        tx_bytes.extend_from_slice(&[0, 0, 0, 0, 0, 0]);
        tx_bytes
    };

    let empty_v6_transaction = |network_upgrade| Transaction::V6 {
        network_upgrade,
        lock_time: LockTime::unlocked(),
        expiry_height: Height(0),
        inputs: Vec::new(),
        outputs: Vec::new(),
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: None,
    };

    for network_upgrade in NetworkUpgrade::iter() {
        let Some(branch_id) = network_upgrade.branch_id() else {
            continue;
        };

        let tx_bytes = empty_v6_transaction_bytes(branch_id);

        if network_upgrade < NetworkUpgrade::Nu6_3 {
            let error = Transaction::zcash_deserialize(&tx_bytes[..])
                .expect_err("V6 transactions must use a NU6.3 or later branch ID");

            assert!(
                matches!(error, SerializationError::Parse(message) if message.contains("NU6.3")),
                "unexpected V6 branch ID parse error for {network_upgrade:?}: {error:?}"
            );

            let error = empty_v6_transaction(network_upgrade)
                .zcash_serialize_to_vec()
                .expect_err("unsupported V6 transaction branch IDs must not serialize");
            assert_eq!(error.kind(), ErrorKind::InvalidData);
            assert!(
                error.to_string().contains("NU6.3"),
                "unexpected V6 branch ID serialization error for {network_upgrade:?}: {error:?}"
            );
        } else {
            let tx = Transaction::zcash_deserialize(&tx_bytes[..])
                .expect("V6 transaction with a NU6.3 or later branch ID must deserialize");

            assert_eq!(tx.version(), 6);
            assert_eq!(tx.network_upgrade(), Some(network_upgrade));
            assert_eq!(tx.zcash_serialize_to_vec().unwrap(), tx_bytes);
        }
    }
}

#[test]
fn v6_txid_commits_to_ironwood_digest() {
    use proptest::{
        prelude::any,
        strategy::{Strategy, ValueTree},
        test_runner::TestRunner,
    };

    use crate::{
        at_least_one,
        ironwood::{self, tree},
        orchard::Flags,
        primitives::Halo2Proof,
    };

    let _init_guard = zebra_test::init();

    let tx_without_ironwood = Transaction::V6 {
        network_upgrade: NetworkUpgrade::Nu6_3,
        lock_time: LockTime::unlocked(),
        expiry_height: Height(1),
        inputs: Vec::new(),
        outputs: Vec::new(),
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: None,
    };

    let mut runner = TestRunner::default();
    let action = any::<ironwood::Action>()
        .new_tree(&mut runner)
        .expect("test action strategy creates a value")
        .current();

    let ironwood_shielded_data = ironwood::ShieldedData {
        flags: Flags::ENABLE_SPENDS | Flags::ENABLE_OUTPUTS,
        value_balance: crate::amount::Amount::try_from(0).expect("zero is a valid amount"),
        shared_anchor: tree::Root::default(),
        proof: Halo2Proof(vec![0; ::orchard::Proof::expected_proof_size(1)]),
        actions: at_least_one![ironwood::AuthorizedAction {
            action,
            spend_auth_sig: [0u8; 64].into(),
        }],
        binding_sig: [0u8; 64].into(),
    };

    let mut tx_with_ironwood = tx_without_ironwood.clone();
    let Transaction::V6 {
        ironwood_shielded_data: tx_ironwood_shielded_data,
        ..
    } = &mut tx_with_ironwood
    else {
        unreachable!("test transaction is V6");
    };
    *tx_ironwood_shielded_data = Some(ironwood_shielded_data);

    assert_ne!(
        tx_with_ironwood.hash(),
        tx_without_ironwood.hash(),
        "V6 txid must commit to Ironwood shielded data"
    );
}

#[test]
fn v6_ironwood_anchor_changes_auth_digest_not_txid() {
    use proptest::{
        prelude::any,
        strategy::{Strategy, ValueTree},
        test_runner::TestRunner,
    };

    use crate::{
        at_least_one,
        ironwood::{self, tree},
        orchard::Flags,
        primitives::Halo2Proof,
    };

    fn test_anchor(byte: u8) -> tree::Root {
        let mut bytes = [0u8; 32];
        bytes[0] = byte;
        tree::Root::try_from(bytes).expect("test anchor must be canonical")
    }

    let _init_guard = zebra_test::init();

    let mut runner = TestRunner::default();
    let action = any::<ironwood::Action>()
        .new_tree(&mut runner)
        .expect("test action strategy creates a value")
        .current();

    let ironwood_shielded_data = ironwood::ShieldedData {
        flags: Flags::ENABLE_SPENDS | Flags::ENABLE_OUTPUTS,
        value_balance: crate::amount::Amount::try_from(0).expect("zero is a valid amount"),
        shared_anchor: test_anchor(1),
        proof: Halo2Proof(vec![0; ::orchard::Proof::expected_proof_size(1)]),
        actions: at_least_one![ironwood::AuthorizedAction {
            action,
            spend_auth_sig: [0u8; 64].into(),
        }],
        binding_sig: [0u8; 64].into(),
    };

    let tx_a = Transaction::V6 {
        network_upgrade: NetworkUpgrade::Nu6_3,
        lock_time: LockTime::unlocked(),
        expiry_height: Height(1),
        inputs: Vec::new(),
        outputs: Vec::new(),
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: Some(ironwood_shielded_data.clone()),
    };

    let mut tx_b = tx_a.clone();
    let Transaction::V6 {
        ironwood_shielded_data: Some(ironwood_shielded_data),
        ..
    } = &mut tx_b
    else {
        unreachable!("test transaction is V6 with Ironwood shielded data");
    };
    ironwood_shielded_data.shared_anchor = test_anchor(2);

    assert_eq!(
        tx_a.hash(),
        tx_b.hash(),
        "V6 txid must not commit to the Ironwood anchor"
    );
    assert_ne!(
        tx_a.auth_digest(),
        tx_b.auth_digest(),
        "V6 auth digest must commit to the Ironwood anchor"
    );
}

#[test]
fn v6_padded_orchard_proof_is_rejected_by_librustzcash_conversion() {
    let _init_guard = zebra_test::init();

    let orchard_shielded_data = Network::iter()
        .flat_map(|network| v5_transactions(network.block_iter()))
        .find_map(|transaction| transaction.orchard_shielded_data().cloned())
        .expect("test vectors include an Orchard transaction");

    let make_tx = |orchard_shielded_data| Transaction::V6 {
        network_upgrade: NetworkUpgrade::Nu6_3,
        lock_time: LockTime::unlocked(),
        expiry_height: Height(1),
        inputs: Vec::new(),
        outputs: Vec::new(),
        sapling_shielded_data: None,
        orchard_shielded_data: Some(orchard_shielded_data),
        ironwood_shielded_data: None,
    };

    // Control: the same tx with a canonical proof round-trips and converts
    // cleanly, so any conversion failure below is attributable to the padding,
    // not the test vector.
    let canonical_bytes = make_tx(orchard_shielded_data.clone())
        .zcash_serialize_to_vec()
        .expect("serialize");
    let canonical_tx = Transaction::zcash_deserialize(&canonical_bytes[..])
        .expect("v6 tx with a canonical Orchard proof round-trips");
    canonical_tx
        .to_librustzcash(NetworkUpgrade::Nu6_3)
        .expect("v6 tx with a canonical Orchard proof converts to librustzcash");

    // The `librustzcash` conversion is deferred out of deserialization to
    // consensus verification, so a padded (non-canonical) Orchard proof now
    // deserializes successfully instead of being rejected at parse time.
    let mut padded = orchard_shielded_data;
    padded.proof.0.push(0);
    let padded_bytes = make_tx(padded).zcash_serialize_to_vec().expect("serialize");
    let padded_tx = Transaction::zcash_deserialize(&padded_bytes[..])
        .expect("padded Orchard proof deserializes once librustzcash validation is deferred");

    // The deferred conversion that consensus verification relies on still
    // rejects the padded proof, so the malformed transaction cannot be verified.
    padded_tx
        .to_librustzcash(NetworkUpgrade::Nu6_3)
        .expect_err("v6 transaction with a padded Orchard proof must fail librustzcash conversion");
}

/// Companion to `v6_padded_orchard_proof_is_rejected_by_librustzcash_conversion`
/// covering the Ironwood bundle, the other net-new V6 shielded pool. Ironwood
/// reuses Orchard's variable-length Halo2 proof encoding, so the same
/// non-canonical padding must be rejected. This guards the deferred-validation
/// boundary for Ironwood: Zebra's parser is more permissive than `librustzcash`,
/// so malformed Ironwood proofs must still be caught by the conversion consensus
/// relies on rather than slipping through.
#[test]
fn v6_padded_ironwood_proof_is_rejected_by_librustzcash_conversion() {
    let _init_guard = zebra_test::init();

    // Ironwood shielded data has the same shape as Orchard, so a real Orchard
    // bundle is a valid Ironwood bundle for encoding/conversion purposes.
    let ironwood_shielded_data = Network::iter()
        .flat_map(|network| v5_transactions(network.block_iter()))
        .find_map(|transaction| transaction.orchard_shielded_data().cloned())
        .expect("test vectors include an Orchard-shaped bundle");

    let make_tx = |ironwood_shielded_data| Transaction::V6 {
        network_upgrade: NetworkUpgrade::Nu6_3,
        lock_time: LockTime::unlocked(),
        expiry_height: Height(1),
        inputs: Vec::new(),
        outputs: Vec::new(),
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: Some(ironwood_shielded_data),
    };

    // Control: the same tx with a canonical proof round-trips and converts
    // cleanly, so any conversion failure below is attributable to the padding,
    // not the bundle.
    let canonical_bytes = make_tx(ironwood_shielded_data.clone())
        .zcash_serialize_to_vec()
        .expect("serialize");
    let canonical_tx = Transaction::zcash_deserialize(&canonical_bytes[..])
        .expect("v6 tx with a canonical Ironwood proof round-trips");
    canonical_tx
        .to_librustzcash(NetworkUpgrade::Nu6_3)
        .expect("v6 tx with a canonical Ironwood proof converts to librustzcash");

    // The padded (non-canonical) Ironwood proof deserializes successfully once
    // librustzcash validation is deferred to consensus verification.
    let mut padded = ironwood_shielded_data;
    padded.proof.0.push(0);
    let padded_bytes = make_tx(padded).zcash_serialize_to_vec().expect("serialize");
    let padded_tx = Transaction::zcash_deserialize(&padded_bytes[..])
        .expect("padded Ironwood proof deserializes once librustzcash validation is deferred");

    // The deferred conversion that consensus verification relies on still
    // rejects the padded proof, so the malformed transaction cannot be verified.
    padded_tx.to_librustzcash(NetworkUpgrade::Nu6_3).expect_err(
        "v6 transaction with a padded Ironwood proof must fail librustzcash conversion",
    );
}

/// Regression test for the Orchard `rk` identity-point DoS vulnerability.
///
/// A transaction whose Orchard action has `rk = [0u8; 32]` (the Pallas
/// identity point) **deserializes successfully** unless Zebra validates it
/// using the corresponding librustzcash transaction parser before returning.
///
/// When the same transaction is subsequently fed to the Orchard Halo2 batch
/// verifier via [`orchard::bundle::BatchValidator::add_bundle`], the call
/// chain reaches `orchard::circuit::to_halo2_instance()`, which calls
/// `.coordinates().unwrap()` on the identity point.  `coordinates()` returns
/// `None` for the identity, so the `unwrap` **panics**, crashing the node.
///
/// ## Root cause
///
/// `zebra-chain/src/orchard/action.rs:83` reads `rk` as raw bytes with no
/// identity-point check: `reader.read_32_bytes()?.into()`.  The upstream
/// `orchard` crate defers validation to signature verification, but
/// `to_halo2_instance()` unwraps the coordinate extraction unconditionally.
///
/// An analogous identity check already exists for `ephemeral_key`
/// (`zebra-chain/src/orchard/keys.rs:225-238`), demonstrating the correct
/// pattern.
#[test]
fn orchard_rk_identity_point_rejected_during_deserialization() {
    use group::prime::PrimeCurveAffine;
    use reddsa::Signature;

    use crate::{
        at_least_one,
        block::Height,
        orchard::{
            keys::EphemeralPublicKey, tree, Action, AuthorizedAction, EncryptedNote, Flags,
            NoteCommitment, Nullifier, ShieldedData, ValueCommitment, WrappedNoteKey,
        },
        primitives::Halo2Proof,
        serialization::ZcashSerialize,
    };
    use halo2::pasta::pallas;

    let _init_guard = zebra_test::init();

    // Construct an Orchard action with rk = [0u8; 32] (identity point).
    // Other fields use the Pallas generator or the identity as appropriate.
    let action = Action {
        // cv can be any valid Pallas point; identity is accepted here.
        cv: ValueCommitment(pallas::Affine::identity()),
        nullifier: Nullifier(pallas::Base::zero()),
        // rk = identity point — this is the vulnerability trigger.
        rk: [0u8; 32].into(),
        // cm_x is the x-coordinate of the note commitment.
        cm_x: NoteCommitment(pallas::Affine::identity()).extract_x(),
        // ephemeral_key must be non-identity; use the generator.
        ephemeral_key: EphemeralPublicKey(pallas::Affine::generator()),
        enc_ciphertext: EncryptedNote([0u8; 580]),
        out_ciphertext: WrappedNoteKey([0u8; 80]),
    };

    let shielded_data = ShieldedData {
        flags: Flags::ENABLE_SPENDS | Flags::ENABLE_OUTPUTS,
        value_balance: crate::amount::Amount::try_from(0).expect("zero is a valid amount"),
        shared_anchor: tree::Root::default(),
        // An empty proof is accepted at deserialization time.
        proof: Halo2Proof(vec![]),
        actions: at_least_one![AuthorizedAction {
            action,
            spend_auth_sig: Signature::from([0u8; 64]),
        }],
        binding_sig: Signature::from([0u8; 64]),
    };

    let v5_tx = Transaction::V5 {
        network_upgrade: NetworkUpgrade::Nu5,
        lock_time: LockTime::unlocked(),
        expiry_height: Height(0),
        inputs: vec![],
        outputs: vec![],
        sapling_shielded_data: None,
        orchard_shielded_data: Some(shielded_data),
    };

    let v5_tx_bytes = v5_tx
        .zcash_serialize_to_vec()
        .expect("crafted V5 transaction must serialize without error");

    Transaction::zcash_deserialize(&v5_tx_bytes[..]).expect_err("V5 rk = identity should fail");

    {
        let Transaction::V5 {
            orchard_shielded_data,
            ..
        } = v5_tx
        else {
            unreachable!("test transaction is V5");
        };

        let v6_tx = Transaction::V6 {
            network_upgrade: NetworkUpgrade::Nu6_3,
            lock_time: LockTime::unlocked(),
            expiry_height: Height(0),
            inputs: vec![],
            outputs: vec![],
            sapling_shielded_data: None,
            orchard_shielded_data,
            ironwood_shielded_data: None,
        };

        let v6_tx_bytes = v6_tx
            .zcash_serialize_to_vec()
            .expect("crafted V6 transaction must serialize without error");

        Transaction::zcash_deserialize(&v6_tx_bytes[..]).expect_err("V6 rk = identity should fail");
    }
}

/// Lazy Sapling `cv` / `ephemeral_key` deserialization stays consensus-safe:
/// deferring the not-small-order check is caught later by librustzcash.
///
/// Every untrusted transaction is converted via `to_librustzcash` before it is
/// accepted, and librustzcash enforces the same rules the deferred check would:
///
/// - `cv` is rejected at *read* — `read_value_commitment` uses
///   `ValueCommitment::from_bytes_not_small_order`, so a small-order `cv` fails
///   the conversion.
/// - `epk` is rejected at *verify* — `check_output` checks `epk.is_small_order()`.
///
/// So this test asserts both the deferral (deserialization now accepts a
/// small-order `cv`/`epk`) and the safety net (`to_librustzcash` rejects the
/// small-order `cv`; the small-order `epk` is flagged by the verifier's check).
#[test]
fn sapling_small_order_cv_epk_deferred_but_caught_by_librustzcash() {
    use group::Group;

    use crate::{
        amount::Amount,
        at_least_one,
        block::Height,
        parameters::NetworkUpgrade,
        primitives::{
            redjubjub::{Binding, Signature},
            Groth16Proof,
        },
        sapling::{
            self,
            keys::EphemeralPublicKey,
            shielded_data::{ShieldedData, TransferData},
            EncryptedNote, Output, ValueCommitment, WrappedNoteKey,
        },
        serialization::{ZcashDeserializeInto, ZcashSerialize},
        transaction::{LockTime, Transaction},
    };

    let _init_guard = zebra_test::init();

    // The Jubjub identity point is a valid encoding but small order, so the
    // not-small-order check must reject it.
    let small_order_bytes = jubjub::AffinePoint::from(jubjub::ExtendedPoint::identity()).to_bytes();

    // The exact library functions the semantic/mempool path uses must detect it.
    assert!(
        bool::from(
            sapling_crypto::value::ValueCommitment::from_bytes_not_small_order(&small_order_bytes)
                .is_none()
        ),
        "from_bytes_not_small_order (used by librustzcash read_value_commitment) must reject \
         the small-order cv",
    );
    assert!(
        bool::from(
            jubjub::AffinePoint::from_bytes(small_order_bytes)
                .unwrap()
                .is_small_order()
        ),
        "is_small_order (used by the Sapling verifier check_output) must flag the small-order epk",
    );

    // A valid, non-small-order point (the Jubjub generator), used to isolate the
    // `epk` case from the `cv` case below.
    let valid_cv_bytes = jubjub::AffinePoint::from(jubjub::ExtendedPoint::generator()).to_bytes();
    assert!(
        bool::from(
            sapling_crypto::value::ValueCommitment::from_bytes_not_small_order(&valid_cv_bytes)
                .is_some()
        ),
        "the Jubjub generator is a valid non-small-order cv",
    );

    // Build a minimal V5 transaction with one Sapling output using the given cv
    // and ephemeral_key bytes, round-trip it through the lazy deserializer, and
    // return whether `to_librustzcash` accepts it.
    let build_and_convert = |cv_bytes: [u8; 32], epk_bytes: [u8; 32]| -> bool {
        let output = Output {
            cv: ValueCommitment(cv_bytes),
            cm_u: sapling_crypto::note::ExtractedNoteCommitment::from_bytes(&[0u8; 32]).unwrap(),
            ephemeral_key: EphemeralPublicKey(epk_bytes),
            enc_ciphertext: EncryptedNote([0u8; 580]),
            out_ciphertext: WrappedNoteKey([0u8; 80]),
            zkproof: Groth16Proof([0u8; 192]),
        };

        let shielded_data: ShieldedData<sapling::SharedAnchor> = ShieldedData {
            value_balance: Amount::try_from(0).expect("zero is a valid amount"),
            transfers: TransferData::JustOutputs {
                outputs: at_least_one![output],
            },
            binding_sig: Signature::<Binding>::from([0u8; 64]),
        };

        let tx = Transaction::V5 {
            network_upgrade: NetworkUpgrade::Nu5,
            lock_time: LockTime::unlocked(),
            expiry_height: Height(0),
            inputs: vec![],
            outputs: vec![],
            sapling_shielded_data: Some(shielded_data),
            orchard_shielded_data: None,
        };

        let bytes = tx
            .zcash_serialize_to_vec()
            .expect("crafted transaction must serialize");

        // Deferral: deserialization now accepts a small-order cv/epk.
        let tx: Transaction = bytes
            .zcash_deserialize_into()
            .expect("lazy deserialization accepts a small-order cv/epk; validation is deferred");

        tx.to_librustzcash(NetworkUpgrade::Nu5).is_ok()
    };

    // cv is enforced at *read*: `read_value_commitment` uses
    // `from_bytes_not_small_order`, so `to_librustzcash` rejects a small-order cv.
    assert!(
        !build_and_convert(small_order_bytes, valid_cv_bytes),
        "to_librustzcash must reject a small-order Sapling cv at read",
    );

    // epk is enforced at *verify*, not read: a small-order epk (with a valid cv)
    // passes `to_librustzcash`, then the verifier's `check_output` rejects it via
    // `epk.is_small_order()` (asserted above).
    //
    // A full end-to-end verifier test is omitted because mutating epk also breaks
    // the SigHash and binding signature, and the output proof can't be forged, so
    // the rejection would be confounded. The `is_small_order` assertion above
    // covers the exact librustzcash code path that rejects it.
    assert!(
        build_and_convert(valid_cv_bytes, small_order_bytes),
        "to_librustzcash must accept a small-order epk (it is enforced at verify, not read)",
    );
}

/// Edge cases for lazy Sapling `cv` / `ephemeral_key` deserialization. Beyond the
/// small-order case, this checks that:
/// - an off-curve `cv` is also rejected by `to_librustzcash`, so the safety net
///   covers every invalid encoding, not just small-order points;
/// - the lazy types round-trip byte-for-byte through serialize/deserialize — the
///   txid and merkle root hash these bytes, so any change would break consensus;
/// - `cv.commitment()` decompresses a valid encoding back to the same point;
/// - Sapling `rk` was not made lazy, so a small-order `rk` is still rejected at
///   deserialization.
#[test]
fn sapling_lazy_cv_epk_edge_cases() {
    use group::Group;

    use crate::{
        amount::Amount,
        at_least_one,
        block::Height,
        parameters::NetworkUpgrade,
        primitives::{
            redjubjub::{Binding, Signature},
            Groth16Proof,
        },
        sapling::{
            self,
            keys::{EphemeralPublicKey, ValidatingKey},
            shielded_data::{ShieldedData, TransferData},
            EncryptedNote, Output, ValueCommitment, WrappedNoteKey,
        },
        serialization::{ZcashDeserializeInto, ZcashSerialize},
        transaction::{LockTime, Transaction},
    };

    let _init_guard = zebra_test::init();

    // A non-canonical / off-curve 32-byte value: not a valid Jubjub point.
    let off_curve = [0xffu8; 32];
    assert!(
        bool::from(jubjub::AffinePoint::from_bytes(off_curve).is_none()),
        "0xff..ff must not be a valid Jubjub point encoding",
    );
    let valid_cv = jubjub::AffinePoint::from(jubjub::ExtendedPoint::generator()).to_bytes();
    let small_order = jubjub::AffinePoint::from(jubjub::ExtendedPoint::identity()).to_bytes();

    let make_v5 = |cv: [u8; 32], epk: [u8; 32]| -> Transaction {
        let output = Output {
            cv: ValueCommitment(cv),
            cm_u: sapling_crypto::note::ExtractedNoteCommitment::from_bytes(&[0u8; 32]).unwrap(),
            ephemeral_key: EphemeralPublicKey(epk),
            enc_ciphertext: EncryptedNote([0u8; 580]),
            out_ciphertext: WrappedNoteKey([0u8; 80]),
            zkproof: Groth16Proof([0u8; 192]),
        };
        Transaction::V5 {
            network_upgrade: NetworkUpgrade::Nu5,
            lock_time: LockTime::unlocked(),
            expiry_height: Height(0),
            inputs: vec![],
            outputs: vec![],
            sapling_shielded_data: Some(ShieldedData::<sapling::SharedAnchor> {
                value_balance: Amount::try_from(0).expect("zero is a valid amount"),
                transfers: TransferData::JustOutputs {
                    outputs: at_least_one![output],
                },
                binding_sig: Signature::<Binding>::from([0u8; 64]),
            }),
            orchard_shielded_data: None,
        }
    };

    // An off-curve cv is rejected by to_librustzcash, covering invalid encodings
    // beyond small-order points.
    let tx_off_curve_cv: Transaction = make_v5(off_curve, valid_cv)
        .zcash_serialize_to_vec()
        .expect("serializes")
        .zcash_deserialize_into()
        .expect("lazy deserialization accepts an off-curve cv");
    assert!(
        tx_off_curve_cv
            .to_librustzcash(NetworkUpgrade::Nu5)
            .is_err(),
        "to_librustzcash must reject an off-curve cv",
    );

    // Byte-identity: even non-canonical cv/epk bytes survive a
    // serialize -> deserialize -> serialize round-trip unchanged, so the txid and
    // merkle root are unaffected by the lazy representation.
    let bytes_in = make_v5(off_curve, off_curve)
        .zcash_serialize_to_vec()
        .expect("serializes");
    let tx_round: Transaction = bytes_in
        .clone()
        .zcash_deserialize_into()
        .expect("round-trips");
    let bytes_out = tx_round.zcash_serialize_to_vec().expect("re-serializes");
    assert_eq!(
        bytes_in, bytes_out,
        "lazy cv/epk must round-trip byte-for-byte",
    );
    match &tx_round {
        Transaction::V5 {
            sapling_shielded_data: Some(sd),
            ..
        } => {
            let out = sd.outputs().next().expect("one output");
            assert_eq!(out.cv.0, off_curve, "cv bytes preserved exactly");
            assert_eq!(
                out.ephemeral_key.0, off_curve,
                "epk bytes preserved exactly"
            );
        }
        _ => panic!("expected a V5 transaction with Sapling data"),
    }

    // `commitment()` decompresses a valid encoding to the same point.
    assert_eq!(
        ValueCommitment(valid_cv)
            .commitment()
            .expect("the generator is a valid value commitment")
            .to_bytes(),
        valid_cv,
        "commitment() must round-trip a valid value commitment",
    );

    // `rk` was not made lazy: a small-order rk is still rejected at deserialization
    // via `ValidatingKey::try_from`.
    assert!(
        ValidatingKey::try_from(small_order).is_err(),
        "Sapling rk must still reject a small-order point at deserialization",
    );
}

/// The semantic verifier's Sapling cv/epk not-small-order check rejects bad
/// points.
///
/// `Transaction::sapling_point_encodings_are_valid` is the deferred check,
/// relocated from deserialization to the semantic path (the verifier calls it,
/// returning `TransactionError::SmallOrder` on failure). Unlike proof/binding-sig
/// verification it is isolated, so we can exercise it directly: it rejects a
/// small-order or off-curve `cv` and `epk`, and accepts valid points.
#[test]
fn sapling_point_encodings_check_rejects_bad_points() {
    use group::Group;

    use crate::{
        amount::Amount,
        at_least_one,
        block::Height,
        parameters::NetworkUpgrade,
        primitives::{
            redjubjub::{Binding, Signature},
            Groth16Proof,
        },
        sapling::{
            self,
            keys::EphemeralPublicKey,
            shielded_data::{ShieldedData, TransferData},
            EncryptedNote, Output, ValueCommitment, WrappedNoteKey,
        },
        transaction::{LockTime, Transaction},
    };

    let _init_guard = zebra_test::init();

    let valid = jubjub::AffinePoint::from(jubjub::ExtendedPoint::generator()).to_bytes();
    let small_order = jubjub::AffinePoint::from(jubjub::ExtendedPoint::identity()).to_bytes();
    let off_curve = [0xffu8; 32];

    let make_shielded_data = |cv: [u8; 32], epk: [u8; 32]| {
        let output = Output {
            cv: ValueCommitment(cv),
            cm_u: sapling_crypto::note::ExtractedNoteCommitment::from_bytes(&[0u8; 32]).unwrap(),
            ephemeral_key: EphemeralPublicKey(epk),
            enc_ciphertext: EncryptedNote([0u8; 580]),
            out_ciphertext: WrappedNoteKey([0u8; 80]),
            zkproof: Groth16Proof([0u8; 192]),
        };

        ShieldedData::<sapling::SharedAnchor> {
            value_balance: Amount::try_from(0).expect("zero is a valid amount"),
            transfers: TransferData::JustOutputs {
                outputs: at_least_one![output],
            },
            binding_sig: Signature::<Binding>::from([0u8; 64]),
        }
    };

    let make_v5 = |cv: [u8; 32], epk: [u8; 32]| -> Transaction {
        Transaction::V5 {
            network_upgrade: NetworkUpgrade::Nu5,
            lock_time: LockTime::unlocked(),
            expiry_height: Height(0),
            inputs: vec![],
            outputs: vec![],
            sapling_shielded_data: Some(make_shielded_data(cv, epk)),
            orchard_shielded_data: None,
        }
    };

    let check_transaction =
        |version_name: &str, make_transaction: &dyn Fn([u8; 32], [u8; 32]) -> Transaction| {
            // Valid points pass (a dummy proof/binding sig does not affect this check).
            assert!(
                make_transaction(valid, valid).sapling_point_encodings_are_valid(),
                "{version_name} valid cv/epk must pass the encoding check",
            );

            // A small-order cv is rejected.
            assert!(
                !make_transaction(small_order, valid).sapling_point_encodings_are_valid(),
                "{version_name} small-order cv must be rejected",
            );

            // A small-order epk is rejected, independently of proof verification.
            assert!(
                !make_transaction(valid, small_order).sapling_point_encodings_are_valid(),
                "{version_name} small-order epk must be rejected",
            );

            // Off-curve / non-canonical encodings are rejected for both fields.
            assert!(
                !make_transaction(off_curve, valid).sapling_point_encodings_are_valid(),
                "{version_name} off-curve cv must be rejected",
            );
            assert!(
                !make_transaction(valid, off_curve).sapling_point_encodings_are_valid(),
                "{version_name} off-curve epk must be rejected",
            );
        };

    check_transaction("V5", &make_v5);

    let make_v6 = |cv: [u8; 32], epk: [u8; 32]| -> Transaction {
        Transaction::V6 {
            network_upgrade: NetworkUpgrade::Nu6_3,
            lock_time: LockTime::unlocked(),
            expiry_height: Height(0),
            inputs: vec![],
            outputs: vec![],
            sapling_shielded_data: Some(make_shielded_data(cv, epk)),
            orchard_shielded_data: None,
            ironwood_shielded_data: None,
        }
    };

    check_transaction("V6", &make_v6);
}

/// The relocated Sapling `cv` / `epk` not-small-order checks accept exactly the
/// same encodings as the librustzcash functions they mirror.
///
/// If `ValueCommitment::is_valid_not_small_order` or
/// `EphemeralPublicKey::is_valid_not_small_order` ever diverged from librustzcash,
/// Zebra would accept or reject transactions the rest of the network doesn't — a
/// chain split. This pins each Zebra predicate against the library predicate over
/// a corpus covering both verdicts:
///
/// - `cv`: `read_value_commitment` accepts a `cv` iff `from_bytes_not_small_order`
///   returns a point.
/// - `epk`: sapling-crypto decodes `epk` as an `ExtendedPoint` and `check_output`
///   rejects it when `epk.is_small_order()`. Zebra decodes as an `AffinePoint`, so
///   this also guards that the two decoders agree.
#[test]
fn sapling_point_checks_match_librustzcash_predicates() {
    use group::{Group, GroupEncoding};

    use crate::sapling::{keys::EphemeralPublicKey, ValueCommitment};

    let _init_guard = zebra_test::init();

    // The exact predicate librustzcash applies to a `cv` at read.
    let librustzcash_cv_valid = |bytes: [u8; 32]| -> bool {
        bool::from(
            sapling_crypto::value::ValueCommitment::from_bytes_not_small_order(&bytes).is_some(),
        )
    };

    // The exact predicate librustzcash applies to an `epk`: decode as an
    // `ExtendedPoint` (as sapling-crypto's batch verifier does), then reject a
    // small-order point (as `check_output` does).
    let librustzcash_epk_valid = |bytes: [u8; 32]| -> bool {
        match jubjub::ExtendedPoint::from_bytes(&bytes).into_option() {
            Some(point) => !bool::from(point.is_small_order()),
            None => false,
        }
    };

    // A spread of encodings: the three consensus-relevant classes (valid
    // non-small-order, valid small-order, off-curve/non-canonical), a byte-pattern
    // sweep mixing decodable and undecodable encodings, and many prime-order
    // points `[k]·G` to exercise the accepting branch.
    let mut inputs: Vec<[u8; 32]> = vec![
        jubjub::AffinePoint::from(jubjub::ExtendedPoint::generator()).to_bytes(),
        jubjub::AffinePoint::from(jubjub::ExtendedPoint::identity()).to_bytes(),
        [0xffu8; 32],
        [0x00u8; 32],
    ];
    for b in 0u8..=255 {
        inputs.push([b; 32]);
    }
    let mut acc = jubjub::ExtendedPoint::generator();
    for _ in 0..64 {
        inputs.push(jubjub::AffinePoint::from(acc).to_bytes());
        acc += jubjub::ExtendedPoint::generator();
    }

    // Guard against a vacuous comparison: the corpus must contain both accepted
    // and rejected encodings, otherwise an all-accept or all-reject bug could
    // pass the equivalence assertion below.
    assert!(
        inputs.iter().any(|&b| librustzcash_cv_valid(b))
            && inputs.iter().any(|&b| !librustzcash_cv_valid(b)),
        "cv corpus must contain both accepted and rejected encodings",
    );
    assert!(
        inputs.iter().any(|&b| librustzcash_epk_valid(b))
            && inputs.iter().any(|&b| !librustzcash_epk_valid(b)),
        "epk corpus must contain both accepted and rejected encodings",
    );

    for bytes in inputs {
        assert_eq!(
            ValueCommitment(bytes).is_valid_not_small_order(),
            librustzcash_cv_valid(bytes),
            "ValueCommitment::is_valid_not_small_order must match librustzcash \
             read_value_commitment for {bytes:02x?}",
        );
        assert_eq!(
            EphemeralPublicKey(bytes).is_valid_not_small_order(),
            librustzcash_epk_valid(bytes),
            "EphemeralPublicKey::is_valid_not_small_order must match librustzcash \
             check_output for {bytes:02x?}",
        );
    }
}

/// Reproduction for GHSA-rgwx-8r98-p34c:
/// Coinbase Sapling spend vectors allocate before zero-spend consensus rule.
///
/// A V5 coinbase transaction with Sapling spends can be serialized and
/// deserialized — the parser allocates Sapling spend vectors (bounded by
/// `TrustedPreallocate::max_allocation()`) before any coinbase-specific
/// check. The consensus rule rejecting coinbase Sapling spends only runs
/// later in `zebra-consensus`, not during deserialization.
#[test]
fn coinbase_v5_with_sapling_spends_deserializes_successfully() {
    let _init_guard = zebra_test::init();

    let network = Network::Mainnet;

    // Find a real V4 transaction with Sapling spends from the test block vectors.
    let tx_with_spends = arbitrary::test_transactions(&network)
        .find(|(_, tx)| tx.sapling_spends_per_anchor().count() > 0);

    let Some((height, original_tx)) = tx_with_spends else {
        panic!("test block vectors must contain at least one transaction with Sapling spends");
    };

    let original_spend_count = original_tx.sapling_spends_per_anchor().count();
    assert!(
        original_spend_count > 0,
        "source transaction must have Sapling spends"
    );

    // Convert the V4 transaction to a fake V5 — this preserves valid Sapling data.
    let fake_v5 = arbitrary::transaction_to_fake_v5(&original_tx, &network, height);

    // Replace transparent inputs with a single coinbase input.
    let Transaction::V5 {
        lock_time,
        expiry_height,
        outputs,
        sapling_shielded_data,
        orchard_shielded_data,
        ..
    } = fake_v5
    else {
        panic!("transaction_to_fake_v5 must return V5");
    };

    // Confirm the fake V5 still has Sapling spends.
    let sapling_shielded_data =
        sapling_shielded_data.expect("converted V5 must retain Sapling shielded data with spends");

    let coinbase_tx = Transaction::V5 {
        network_upgrade: NetworkUpgrade::Nu5,
        lock_time,
        expiry_height,
        inputs: vec![transparent::Input::Coinbase {
            height,
            data: vec![0x00; 4],
            sequence: 0xFFFF_FFFF,
        }],
        outputs: if outputs.is_empty() {
            vec![transparent::Output {
                value: crate::amount::Amount::zero(),
                lock_script: Script::new(&[0u8; 20]),
            }]
        } else {
            outputs
        },
        sapling_shielded_data: Some(sapling_shielded_data),
        orchard_shielded_data,
    };

    // The constructed transaction must look like a coinbase with Sapling spends.
    assert!(coinbase_tx.is_coinbase(), "transaction must be coinbase");
    assert!(
        coinbase_tx.sapling_spends_per_anchor().count() > 0,
        "coinbase transaction has Sapling spends"
    );

    // Serialize it.
    let serialized = coinbase_tx
        .zcash_serialize_to_vec()
        .expect("coinbase V5 with Sapling spends must serialize");

    // Deserialize it — the parser must now reject coinbase transactions with
    // Sapling spends before allocating spend vectors (GHSA-rgwx-8r98-p34c fix).
    let err = serialized
        .zcash_deserialize_into::<Transaction>()
        .expect_err("coinbase with Sapling spends must be rejected during deserialization");

    assert!(
        err.to_string()
            .contains("coinbase transaction must not have Sapling spends"),
        "unexpected error: {err}"
    );
}
