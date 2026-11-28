//! The `zcash_primitives` known-answer vectors, which an independent implementation of
//! the ZIP 248 digests also answers.

use crate::{serialization::ZcashSerialize, transaction::Transaction};

use super::{
    common_read, encode, native_sighashes, previous_output, vector_transaction,
    vectors::TEST_VECTORS,
};

fn hex32(digest: [u8; 32]) -> String {
    hex::encode(digest)
}

#[test]
fn vectors_round_trip_and_match_known_digests() {
    let _init_guard = zakura_test::init();

    for vector in TEST_VECTORS {
        let bytes = hex::decode(vector.tx).expect("vectors are hex");
        common_read(&bytes).expect("zcash_primitives decodes the vectors");
        let tx = vector_transaction(&bytes);
        assert!(matches!(tx, Transaction::V7 { .. }), "{}", vector.bundles);
        assert_eq!(encode(&tx), bytes, "{}", vector.bundles);
        assert_eq!(tx.zcash_serialized_size(), bytes.len());

        assert_eq!(hex32(tx.hash().0), vector.txid, "txid: {}", vector.bundles);
        let auth_digest = tx.auth_digest().expect("v7 has an auth digest");
        assert_eq!(
            hex32(auth_digest.0),
            vector.auth_digest,
            "{}",
            vector.bundles
        );
        let (txid, auth) = tx.txid_and_auth_digest();
        assert_eq!((txid, auth), (tx.hash(), Some(auth_digest)));

        let previous_outputs = vector
            .amounts
            .iter()
            .zip(vector.script_pubkeys)
            .map(|(value, script)| {
                previous_output(*value, &hex::decode(script).expect("vectors are hex"))
            })
            .collect();
        let digests: Vec<(String, String)> = native_sighashes(&tx, previous_outputs)
            .into_iter()
            .map(|(label, digest)| (label, hex32(digest)))
            .collect();
        let expected: Vec<(String, String)> = vector
            .sighashes
            .iter()
            .map(|(label, digest)| (label.to_string(), digest.to_string()))
            .collect();
        assert_eq!(digests, expected, "{}", vector.bundles);
    }
}
