//! Randomised property tests for transactions.

use proptest::{collection::vec, prelude::*};

use std::io::Cursor;

use zebra_test::prelude::*;

use hex::{FromHex, ToHex};

use super::super::*;

use crate::{
    block::{Block, Height},
    orchard,
    parameters::NetworkUpgrade,
    sapling,
    serialization::{ZcashDeserialize, ZcashDeserializeInto, ZcashSerialize},
    transaction::arbitrary::MAX_ARBITRARY_ITEMS,
    transparent, LedgerState,
};

fn native_zip244_tx_strategy() -> BoxedStrategy<Transaction> {
    prop_oneof![
        v5_tx_strategy(Just(None).boxed(), Just(None).boxed()),
        v5_tx_strategy(
            sapling_outputs_only().prop_map(Some).boxed(),
            Just(None).boxed()
        ),
        v5_tx_strategy(
            sapling_with_spends().prop_map(Some).boxed(),
            Just(None).boxed()
        ),
        v5_tx_strategy(
            Just(None).boxed(),
            any::<orchard::ShieldedData>().prop_map(Some).boxed()
        ),
        v5_tx_strategy(
            any::<sapling::ShieldedData<sapling::SharedAnchor>>()
                .prop_map(Some)
                .boxed(),
            orchard_with_multiple_actions().prop_map(Some).boxed(),
        ),
    ]
    .boxed()
}

fn v5_tx_strategy(
    sapling_shielded_data: BoxedStrategy<Option<sapling::ShieldedData<sapling::SharedAnchor>>>,
    orchard_shielded_data: BoxedStrategy<Option<orchard::ShieldedData>>,
) -> BoxedStrategy<Transaction> {
    (
        nu5_or_later_upgrade(),
        any::<LockTime>(),
        any::<Height>(),
        vec(any_with::<transparent::Input>(None), 0..MAX_ARBITRARY_ITEMS),
        vec(any::<transparent::Output>(), 0..MAX_ARBITRARY_ITEMS),
        sapling_shielded_data,
        orchard_shielded_data,
    )
        .prop_map(
            |(
                network_upgrade,
                lock_time,
                expiry_height,
                inputs,
                outputs,
                sapling_shielded_data,
                orchard_shielded_data,
            )| Transaction::V5 {
                network_upgrade,
                lock_time,
                expiry_height,
                inputs,
                outputs,
                sapling_shielded_data,
                orchard_shielded_data,
            },
        )
        .boxed()
}

fn nu5_or_later_upgrade() -> BoxedStrategy<NetworkUpgrade> {
    prop_oneof![
        Just(NetworkUpgrade::Nu5),
        Just(NetworkUpgrade::Nu6),
        Just(NetworkUpgrade::Nu6_1),
        Just(NetworkUpgrade::Nu6_2),
    ]
    .boxed()
}

fn sapling_outputs_only() -> BoxedStrategy<sapling::ShieldedData<sapling::SharedAnchor>> {
    any::<sapling::ShieldedData<sapling::SharedAnchor>>()
        .prop_filter("Sapling outputs-only bundle", |sapling| {
            sapling.spends().next().is_none() && sapling.outputs().next().is_some()
        })
        .boxed()
}

fn sapling_with_spends() -> BoxedStrategy<sapling::ShieldedData<sapling::SharedAnchor>> {
    any::<sapling::ShieldedData<sapling::SharedAnchor>>()
        .prop_filter("Sapling bundle with spends", |sapling| {
            sapling.spends().next().is_some()
        })
        .boxed()
}

fn orchard_with_multiple_actions() -> BoxedStrategy<orchard::ShieldedData> {
    any::<orchard::ShieldedData>()
        .prop_filter("Orchard bundle with multiple actions", |orchard| {
            orchard.actions().count() > 1
        })
        .boxed()
}

proptest! {
    #[test]
    fn transaction_roundtrip(tx in any::<Transaction>()) {
        let _init_guard = zebra_test::init();

        let has_coinbase_sapling_spends = tx.is_coinbase()
            && tx.sapling_spends_per_anchor().count() > 0;

        let data = tx.zcash_serialize_to_vec().expect("tx should serialize");

        if has_coinbase_sapling_spends {
            // GHSA-rgwx-8r98-p34c fix: the parser now rejects coinbase
            // transactions with Sapling spends before allocating.
            data.zcash_deserialize_into::<Transaction>()
                .expect_err("coinbase with Sapling spends must be rejected");
        } else {
            let tx2 = data.zcash_deserialize_into()
                .expect("randomized tx should deserialize");

            prop_assert_eq![&tx, &tx2];

            let data2 = tx2
                .zcash_serialize_to_vec()
                .expect("vec serialization is infallible");

            prop_assert_eq![data, data2, "data must be equal if structs are equal"];
        }
    }

    #[test]
    fn txid_and_auth_digest_matches_separate(tx in any::<Transaction>()) {
        let _init_guard = zebra_test::init();

        let (txid, auth_digest) = tx.txid_and_auth_digest();

        prop_assert_eq![txid, tx.hash()];
        prop_assert_eq![auth_digest, tx.auth_digest()];
    }

    /// The native ZIP-244 txid + authorizing-data digest implementation
    /// (`transaction::zip244`) must be byte-for-byte identical to the
    /// `librustzcash` conversion it replaces. This is the consensus-critical
    /// correctness proof for the native path, exercised across random v5
    /// transaction shapes: transparent-only, Sapling outputs-only, Sapling
    /// spends, Orchard, combined Sapling+Orchard, and multiple NU5+ branch ids.
    #[test]
    fn native_zip244_matches_librustzcash(tx in native_zip244_tx_strategy()) {
        let _init_guard = zebra_test::init();

        let (native_txid, native_auth) = crate::transaction::zip244::txid_and_auth_digest(&tx)
            .expect("v5 transaction has a native ZIP-244 digest");
        let (ref_txid, ref_auth) =
            crate::primitives::zcash_primitives::txid_and_auth_digest_via_librustzcash(&tx);

        prop_assert_eq!(native_txid, ref_txid, "native txid must match librustzcash");
        prop_assert_eq!(native_auth, ref_auth, "native auth digest must match librustzcash");

        // The separate native entry points must agree with the combined one.
        prop_assert_eq!(crate::transaction::zip244::txid(&tx).expect("v5"), native_txid);
        prop_assert_eq!(
            crate::transaction::zip244::auth_digest(&tx).expect("v5"),
            native_auth
        );
    }

    #[test]
    fn transaction_hash_struct_display_roundtrip(hash in any::<Hash>()) {
        let _init_guard = zebra_test::init();

        let display = format!("{hash}");
        let parsed = display.parse::<Hash>().expect("hash should parse");
        prop_assert_eq!(hash, parsed);
    }

    #[test]
    fn transaction_hash_string_parse_roundtrip(hash in any::<String>()) {
        let _init_guard = zebra_test::init();

        if let Ok(parsed) = hash.parse::<Hash>() {
            let display = format!("{parsed}");
            prop_assert_eq!(hash, display);
        }
    }

    #[test]
    fn transaction_hash_hex_roundtrip(hash in any::<Hash>()) {
        let _init_guard = zebra_test::init();

        let hex_hash: String = hash.encode_hex();
        let new_hash = Hash::from_hex(hex_hash).expect("hex hash should parse");
        prop_assert_eq!(hash, new_hash);
    }

    #[test]
    fn transaction_auth_digest_struct_display_roundtrip(auth_digest in any::<AuthDigest>()) {
        let _init_guard = zebra_test::init();

        let display = format!("{auth_digest}");
        let parsed = display.parse::<AuthDigest>().expect("auth digest should parse");
        prop_assert_eq!(auth_digest, parsed);
    }

    #[test]
    fn transaction_auth_digest_string_parse_roundtrip(auth_digest in any::<String>()) {
        let _init_guard = zebra_test::init();

        if let Ok(parsed) = auth_digest.parse::<AuthDigest>() {
            let display = format!("{parsed}");
            prop_assert_eq!(auth_digest, display);
        }
    }

    #[test]
    fn transaction_wtx_id_struct_display_roundtrip(wtx_id in any::<WtxId>()) {
        let _init_guard = zebra_test::init();

        let display = format!("{wtx_id}");
        let parsed = display.parse::<WtxId>().expect("wide transaction ID should parse");
        prop_assert_eq!(wtx_id, parsed);
    }

    #[test]
    fn transaction_wtx_id_string_parse_roundtrip(wtx_id in any::<String>()) {
        let _init_guard = zebra_test::init();

        if let Ok(parsed) = wtx_id.parse::<WtxId>() {
            let display = format!("{parsed}");
            prop_assert_eq!(wtx_id, display);
        }
    }

    #[test]
    fn locktime_roundtrip(locktime in any::<LockTime>()) {
        let _init_guard = zebra_test::init();

        let mut bytes = Cursor::new(Vec::new());
        locktime.zcash_serialize(&mut bytes)?;

        bytes.set_position(0);
        let other_locktime = LockTime::zcash_deserialize(&mut bytes)?;

        prop_assert_eq![locktime, other_locktime];
    }
}

/// Make sure a transaction version override generates transactions with the specified
/// transaction versions.
#[test]
fn arbitrary_transaction_version_strategy() -> Result<()> {
    let _init_guard = zebra_test::init();

    // Update with new transaction versions as needed
    let strategy = (1..5u32)
        .prop_flat_map(|transaction_version| {
            LedgerState::coinbase_strategy(None, transaction_version, false)
        })
        .prop_flat_map(|ledger_state| Transaction::vec_strategy(ledger_state, MAX_ARBITRARY_ITEMS));

    proptest!(|(transactions in strategy)| {
        let mut version = None;
        for t in transactions {
            if version.is_none() {
                version = Some(t.version());
            } else {
                prop_assert_eq!(Some(t.version()), version);
            }
        }
    });

    Ok(())
}

/// Make sure a transaction valid network upgrade strategy generates transactions
/// with valid network upgrades.
#[test]
fn transaction_valid_network_upgrade_strategy() -> Result<()> {
    let _init_guard = zebra_test::init();

    // Update with new transaction versions as needed
    let strategy = LedgerState::coinbase_strategy(None, 5, true).prop_flat_map(|ledger_state| {
        (
            Just(ledger_state.network.clone()),
            Block::arbitrary_with(ledger_state),
        )
    });

    proptest!(|((network, block) in strategy)| {
        block.check_transaction_network_upgrade_consistency(&network)?;
    });

    Ok(())
}
