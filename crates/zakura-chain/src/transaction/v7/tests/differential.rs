//! Differential tests against `zcash_primitives`: both implementations agree on the
//! encoding, the transaction identifier, the authorizing data commitment, and every
//! signature digest of arbitrary v7 transactions.

use proptest::{collection::vec, prelude::*};

use crate::{
    parameters::NetworkUpgrade,
    primitives::zcash_primitives::{
        sighash, v7_shielded_sighash_via_librustzcash, PrecomputedTxData,
    },
    transaction::{HashType, Transaction},
    transparent,
};

use super::{common_read, decode, encode, native_sighashes, sighasher};

/// An arbitrary v7 transaction, and an arbitrary output spent by each of its inputs.
fn tx_with_previous_outputs() -> impl Strategy<Value = (Transaction, Vec<transparent::Output>)> {
    Transaction::v7_strategy().prop_flat_map(|tx| {
        let count = tx.inputs().len();
        (Just(tx), vec(any::<transparent::Output>(), count))
    })
}

/// Computes the digest that `label` names with `zcash_primitives`.
fn common_sighash(precomputed: &PrecomputedTxData, label: &str) -> [u8; 32] {
    let parts: Vec<&str> = label.split(' ').collect();
    let digest = match parts[..] {
        ["t", index] => sighash(
            precomputed,
            HashType::ALL,
            // The script code is unused: variant 0 commits to the spent script instead.
            Some((index.parse().expect("an input index"), Vec::new())),
        ),
        ["s", bundle_type, "b"] => v7_shielded_sighash_via_librustzcash(
            precomputed,
            bundle_type.parse().expect("a bundle type"),
            None,
        )
        .expect("the binding signature exists"),
        ["s", bundle_type, index] => v7_shielded_sighash_via_librustzcash(
            precomputed,
            bundle_type.parse().expect("a bundle type"),
            Some(index.parse().expect("a spend index")),
        )
        .expect("the spend exists"),
        _ => unreachable!("native_sighashes labels every digest"),
    };
    digest.0
}

proptest! {
    // Each case parses the transaction with both implementations and hashes every
    // signature, so fewer cases than the default keep the test fast.
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn v7_matches_zcash_primitives((tx, previous_outputs) in tx_with_previous_outputs()) {
        let _init_guard = zakura_test::init();

        let bytes = encode(&tx);
        prop_assert_eq!(&decode(&bytes).expect("an encoded v7 transaction decodes"), &tx);

        let common = common_read(&bytes).expect("zcash_primitives decodes the encoding");
        let mut common_bytes = Vec::new();
        common.write(&mut common_bytes).expect("writing to a Vec cannot fail");
        prop_assert_eq!(&common_bytes, &bytes);

        prop_assert_eq!(tx.hash().0, *common.txid().as_ref());
        let common_auth: [u8; 32] = common
            .auth_commitment()
            .as_bytes()
            .try_into()
            .expect("the commitment is 32 bytes");
        prop_assert_eq!(tx.auth_digest().expect("v7 has an auth digest").0, common_auth);

        let precomputed = PrecomputedTxData::new(
            &tx,
            NetworkUpgrade::Nu7_1,
            previous_outputs.clone().into(),
        )
        .expect("zcash_primitives accepts the v7 transaction");
        for (label, digest) in native_sighashes(&tx, previous_outputs.clone()) {
            prop_assert_eq!(digest, common_sighash(&precomputed, &label), "{}", label);
        }

        // Variant 0 signs SIGHASH_ALL only.
        let hasher = sighasher(&tx, previous_outputs);
        for hash_type in [
            HashType::NONE,
            HashType::SINGLE,
            HashType::ALL_ANYONECANPAY,
            HashType::NONE_ANYONECANPAY,
            HashType::SINGLE_ANYONECANPAY,
        ] {
            for index in 0..tx.inputs().len() {
                prop_assert_eq!(hasher.v7_transparent_sighash(hash_type, index), None);
            }
        }
        prop_assert_eq!(hasher.v7_transparent_sighash(HashType::ALL, tx.inputs().len()), None);
    }

    /// Spent outputs that do not match the inputs give no signature digests.
    #[test]
    fn v7_sighashes_need_one_spent_output_per_input(
        (tx, mut previous_outputs) in tx_with_previous_outputs(),
        extra in any::<transparent::Output>(),
    ) {
        let _init_guard = zakura_test::init();

        previous_outputs.push(extra);
        let hasher = super::super::V7SighashCache::new(&tx, &previous_outputs)
            .expect("the transaction is v7");
        prop_assert_eq!(hasher.transparent(HashType::ALL, 0), None);
        for bundle in [
            super::ShieldedBundle::Sapling,
            super::ShieldedBundle::Orchard,
            super::ShieldedBundle::Ironwood,
        ] {
            prop_assert_eq!(hasher.shielded(bundle), None);
        }
    }
}
