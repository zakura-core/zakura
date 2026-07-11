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

use std::sync::Arc;

use orchard::bundle::{Authorized, Bundle};
use zakura_chain::{
    block::Block,
    parameters::NetworkUpgrade,
    serialization::ZcashDeserializeInto,
    transaction::{HashType, SigHash},
    transparent,
};
use zcash_protocol::value::ZatBalance;

use super::{
    verifier_for, Item, VERIFIER_NU6_2, VERIFIER_NU6_3_ONWARD, VERIFIER_PRE_NU6_2,
    VERIFYING_KEY_NU6_2, VERIFYING_KEY_PRE_NU6_2,
};

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

/// [`verifier_for`] routes each upgrade to the service that holds the correct
/// circuit era's key.
///
/// We compare service identity by pointer: the routing functions return borrows of global `Lazy`
/// services, so each expected route must alias the matching service. Because the route is what
/// binds an item to a key, routing to the wrong service is exactly routing to the wrong key.
///
/// This is an async test because forcing the global `Lazy` verifiers builds their `Batch` layer,
/// which spawns a worker task and therefore needs a Tokio runtime.
#[tokio::test(flavor = "multi_thread")]
async fn verifier_routes_each_network_upgrade_to_the_correct_key() {
    // Deref each `Lazy` to the inner service it guards, matching what the routing functions
    // return, so the pointer comparisons below compare the same service type.
    let pre: &'static super::VerifierService = &VERIFIER_PRE_NU6_2;
    let nu6_2: &'static super::VerifierService = &VERIFIER_NU6_2;
    let nu6_3_onward: &'static super::VerifierService = &VERIFIER_NU6_3_ONWARD;

    // Everything before NU6.2 (including upgrades from before Orchard existed) routes to the
    // insecure key, which is the only key any pre-NU6.2 Orchard history verifies under.
    for nu in [
        NetworkUpgrade::Nu5,
        NetworkUpgrade::Nu6,
        NetworkUpgrade::Nu6_1,
    ] {
        assert!(
            std::ptr::eq(verifier_for(nu), pre),
            "{nu:?} must route to the pre-NU6.2 (insecure) verifier"
        );
    }

    // NU6.2 is the only upgrade that uses the fixed key: it is active from the NU6.2 activation
    // height until NU6.3.
    assert!(
        std::ptr::eq(verifier_for(NetworkUpgrade::Nu6_2), nu6_2),
        "Nu6_2 must route to the NU6.2 (fixed) verifier"
    );

    // NU6.3 onward routes to the NU6.3 circuit, *including in v5 transactions*. The Orchard-pool
    // cross-address restriction is enforced for every Orchard Action from NU6.3 onward regardless
    // of transaction version, "so that it cannot be bypassed by using a version 5 transaction"
    // (ZIP 229), and that restriction lives only in the NU6.3 circuit. Nu7 guards that later
    // upgrades do not fall back to the NU6.2 fixed key.
    for nu in [NetworkUpgrade::Nu6_3, NetworkUpgrade::Nu7] {
        assert!(
            std::ptr::eq(verifier_for(nu), nu6_3_onward),
            "{nu:?} must route to the NU6.3-onward verifier even for v5 Orchard bundles"
        );
    }

    // v6 Orchard and Ironwood share the NU6.3 circuit, and a v5 Orchard bundle at NU6.3 must use
    // that very same key — selecting the verifier is what binds a bundle to a key, so this is the
    // regression guard against routing v5@NU6.3 to the fixed key.
    assert!(
        std::ptr::eq(verifier_for(NetworkUpgrade::Nu6_3), nu6_3_onward),
        "a v5 Orchard bundle at NU6.3 must use the same key as v6 Orchard and Ironwood"
    );
}
