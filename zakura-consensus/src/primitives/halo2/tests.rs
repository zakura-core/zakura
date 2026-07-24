//! Tests for the Halo2 Orchard Action verifier.
//!
//! The key correctness property of this module is the **era split**: the Orchard Action circuit
//! (and therefore its verifying key) changed at NU6.2 to fix a variable-base scalar-multiplication
//! soundness bug (GHSA-jfw5-j458-pfv6). A proof produced under one circuit does not verify under
//! the other key. These tests guard that:
//!
//!   * a real pre-NU6.2 Orchard proof verifies under the pre-NU6.2 (insecure) key, so historical
//!     blocks still re-sync;
//!   * the same proof is **rejected** by the post-NU6.2 (fixed) key, so the verifier is not
//!     "fail-open" — it does not accept whatever it is handed regardless of era; and
//!   * [`verifier_for`] routes each network upgrade to the service holding the
//!     matching circuit era's key (pre-NU6.2 insecure, NU6.2-until-NU6.3 fixed, or
//!     NU6.3-onward).

use std::{sync::Arc, time::Duration};

use futures::future::join_all;
use orchard::bundle::{Authorized, Bundle};
use tower::{Service, ServiceExt};
use tower_batch_control::Batch;
use tower_fallback::Fallback;
use zakura_chain::{
    block::Block,
    parameters::NetworkUpgrade,
    serialization::ZcashDeserializeInto,
    transaction::{HashType, SigHash},
    transparent,
};
use zcash_protocol::value::ZatBalance;

use super::{
    lazy_verifier_for, Item, ItemVerifyingKey, OrchardFallback, Verifier, VerifierService,
    VERIFIER_NU6_2, VERIFIER_NU6_3_ONWARD, VERIFIER_PRE_NU6_2, VERIFYING_KEY_NU6_2,
    VERIFYING_KEY_NU6_3_ONWARD, VERIFYING_KEY_PRE_NU6_2,
};

const EXPLICIT_FLUSH_TEST_MAX_BATCH_WEIGHT: usize = 10_000;
const EXPLICIT_FLUSH_TEST_LATENCY: Duration = Duration::from_secs(1000);
const EXPLICIT_FLUSH_TEST_TIMEOUT: Duration = Duration::from_secs(120);

/// Returns one real pre-NU6.2 Orchard bundle and its sighash, extracted from the mainnet test
/// blocks.
///
/// These mainnet blocks are NU5-era Orchard history, mined long before NU6.2, so their proofs
/// were produced by the historical (insecure) circuit and only verify under
/// [`VERIFYING_KEY_PRE_NU6_2`]. Transactions with transparent inputs are skipped because their
/// sighash needs the previous outputs they spend, which are not in the test vectors.
fn pre_nu6_2_bundle_and_sighash() -> (Bundle<Authorized, ZatBalance>, SigHash) {
    for bytes in zakura_test::vectors::MAINNET_BLOCKS.values() {
        let block: Block = bytes
            .zcash_deserialize_into()
            .expect("hard-coded test vector must deserialize");

        for tx in &block.transactions {
            if tx.orchard_shielded_data().is_none() || !tx.inputs().is_empty() {
                continue;
            }

            let all_previous_outputs: Arc<Vec<transparent::Output>> = Arc::new(Vec::new());
            let Ok(sighasher) = tx.sighasher(NetworkUpgrade::Nu5, all_previous_outputs) else {
                continue;
            };
            let Some(bundle) = sighasher.orchard_bundle() else {
                continue;
            };

            let sighash = sighasher.sighash(HashType::ALL, None);
            return (bundle, sighash);
        }
    }

    panic!("mainnet test blocks must contain a transparent-input-free Orchard transaction");
}

fn explicit_flush_verifier(vk: &'static ItemVerifyingKey) -> VerifierService {
    Fallback::new(
        Batch::new(
            Verifier::new(vk),
            EXPLICIT_FLUSH_TEST_MAX_BATCH_WEIGHT,
            1,
            EXPLICIT_FLUSH_TEST_LATENCY,
        ),
        OrchardFallback { vk },
    )
}

async fn assert_explicit_flush_matches_single(vk: &'static ItemVerifyingKey, items: Vec<Item>) {
    let expected_results: Vec<_> = items
        .iter()
        .cloned()
        .map(|item| item.verify_single(vk))
        .collect();
    let mut verifier = explicit_flush_verifier(vk);
    let mut batch_results = Vec::new();

    for item in items {
        verifier
            .ready()
            .await
            .expect("test verifier must become ready");
        batch_results.push(verifier.call(item));
    }

    let mut primary = verifier.primary().clone();
    assert!(
        primary
            .try_flush()
            .expect("explicit test flush must not fail"),
        "explicit test flush must be queued"
    );

    let actual_results: Vec<_> = join_all(batch_results)
        .await
        .into_iter()
        .map(|result| result.is_ok())
        .collect();
    assert_eq!(
        actual_results, expected_results,
        "explicit batch flush plus fallback must match single verification"
    );
}

/// A real pre-NU6.2 Orchard proof verifies under the pre-NU6.2 key and is rejected by the
/// post-NU6.2 key.
///
/// This is the core guard for the era split: it proves the two keys are genuinely different and
/// that selecting the wrong era's key causes a hard verification failure. If the verifier ever
/// "fails open" (e.g. validates everything against a single key, like the rejected zcashd WIP
/// shortcut), the wrong-key assertion below would fail.
#[test]
fn pre_nu6_2_proof_only_verifies_under_pre_nu6_2_key() {
    let (bundle, sighash) = pre_nu6_2_bundle_and_sighash();

    // Correct era key: the historical proof must verify, so pre-NU6.2 history still re-syncs.
    assert!(
        Item::new(bundle.clone(), sighash).verify_single(&VERIFYING_KEY_PRE_NU6_2),
        "a real pre-NU6.2 Orchard proof must verify under the pre-NU6.2 (insecure) key"
    );

    // Wrong era key: the same proof must be rejected. This is the not-fail-open guarantee.
    assert!(
        !Item::new(bundle, sighash).verify_single(&VERIFYING_KEY_NU6_2),
        "a pre-NU6.2 Orchard proof must be REJECTED by the post-NU6.2 (fixed) key; \
         verifying it would mean the era selection is fail-open"
    );
}

/// [`lazy_verifier_for`] routes each upgrade to the service that holds the correct
/// circuit era's key.
///
/// Comparing the `Lazy` handles themselves proves the routing without building
/// any verifying key or starting a batch worker.
#[test]
fn verifier_routes_each_network_upgrade_to_the_correct_key() {
    let pre = &VERIFIER_PRE_NU6_2;
    let nu6_2 = &VERIFIER_NU6_2;
    let nu6_3_onward = &VERIFIER_NU6_3_ONWARD;

    // Everything before NU6.2 (including upgrades from before Orchard existed) routes to the
    // insecure key, which is the only key any pre-NU6.2 Orchard history verifies under.
    for nu in [
        NetworkUpgrade::Nu5,
        NetworkUpgrade::Nu6,
        NetworkUpgrade::Nu6_1,
    ] {
        assert!(
            std::ptr::eq(lazy_verifier_for(nu), pre),
            "{nu:?} must route to the pre-NU6.2 (insecure) verifier"
        );
    }

    // NU6.2 is the only upgrade that uses the fixed key: it is active from the NU6.2 activation
    // height until NU6.3.
    assert!(
        std::ptr::eq(lazy_verifier_for(NetworkUpgrade::Nu6_2), nu6_2),
        "Nu6_2 must route to the NU6.2 (fixed) verifier"
    );

    // NU6.3 onward routes to the NU6.3 circuit, *including in v5 transactions*. The Orchard-pool
    // cross-address restriction is enforced for every Orchard Action from NU6.3 onward regardless
    // of transaction version, "so that it cannot be bypassed by using a version 5 transaction"
    // (ZIP 229), and that restriction lives only in the NU6.3 circuit. Nu7 guards that later
    // upgrades do not fall back to the NU6.2 fixed key.
    for nu in [NetworkUpgrade::Nu6_3, NetworkUpgrade::Nu7] {
        assert!(
            std::ptr::eq(lazy_verifier_for(nu), nu6_3_onward),
            "{nu:?} must route to the NU6.3-onward verifier even for v5 Orchard bundles"
        );
    }

    // v6 Orchard and Ironwood share the NU6.3 circuit, and a v5 Orchard bundle at NU6.3 must use
    // that very same key — selecting the verifier is what binds a bundle to a key, so this is the
    // regression guard against routing v5@NU6.3 to the fixed key.
    assert!(
        std::ptr::eq(lazy_verifier_for(NetworkUpgrade::Nu6_3), nu6_3_onward),
        "a v5 Orchard bundle at NU6.3 must use the same key as v6 Orchard and Ironwood"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn explicit_flush_fallback_matches_single_for_mixed_pre_nu6_2_proofs() {
    let _init_guard = zakura_test::init();
    let (bundle, sighash) = pre_nu6_2_bundle_and_sighash();
    let mut invalid_sighash = sighash;
    invalid_sighash.0[0] ^= 1;

    tokio::time::timeout(
        EXPLICIT_FLUSH_TEST_TIMEOUT,
        assert_explicit_flush_matches_single(
            &VERIFYING_KEY_PRE_NU6_2,
            vec![
                Item::new(bundle.clone(), sighash),
                Item::new(bundle.clone(), invalid_sighash),
                Item::new(bundle, sighash),
            ],
        ),
    )
    .await
    .expect("explicitly flushed Orchard verification must complete");
}

#[tokio::test(flavor = "multi_thread")]
async fn explicit_flush_rejects_single_proof_under_each_wrong_era_key() {
    let _init_guard = zakura_test::init();
    let (bundle, sighash) = pre_nu6_2_bundle_and_sighash();

    for vk in [&*VERIFYING_KEY_NU6_2, &*VERIFYING_KEY_NU6_3_ONWARD] {
        assert!(
            !Item::new(bundle.clone(), sighash).verify_single(vk),
            "the historical proof must be invalid under the wrong era key"
        );

        tokio::time::timeout(
            EXPLICIT_FLUSH_TEST_TIMEOUT,
            assert_explicit_flush_matches_single(vk, vec![Item::new(bundle.clone(), sighash)]),
        )
        .await
        .expect("explicitly flushed Orchard verification must complete");
    }
}
