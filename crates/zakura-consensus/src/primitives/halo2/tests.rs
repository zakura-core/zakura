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

use std::{
    future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};

use futures::future::join_all;
use orchard::{
    builder::{Builder, BundleType},
    bundle::{Authorized, Bundle, BundleVersion, Flags, TxVersion},
    circuit::{OrchardCircuitVersion, ProvingKey},
    keys::{FullViewingKey, Scope, SpendingKey},
    value::NoteValue,
    Anchor,
};
use tower::{Service, ServiceExt};
use tower_batch_control::Batch;
use tower_fallback::Fallback;
use zakura_chain::{
    block::Block,
    parameters::NetworkUpgrade,
    primitives::Halo2Proof,
    serialization::ZcashDeserializeInto,
    transaction::{AuthDigest, Hash, HashType, SigHash, Transaction, WtxId},
    transparent,
};
use zcash_protocol::value::ZatBalance;

use crate::{error::TransactionError, BoxError};

use super::{
    lazy_verifier_for, BatchFallbackService, CacheKey, Cached, CachedItem, Item, ItemVerifyingKey,
    OrchardFallback, Verifier, VERIFIER_NU6_2, VERIFIER_NU6_3_ONWARD, VERIFIER_PRE_NU6_2,
    VERIFYING_KEY_NU6_2, VERIFYING_KEY_NU6_3_ONWARD, VERIFYING_KEY_PRE_NU6_2,
};

/// The `verifier` label the test caches report their metrics under.
///
/// Test caches use their own label so their counts never land in the series
/// the production verifiers report.
const TEST_CACHE_VERIFIER_LABEL: &str = "halo2_test";

const EXPLICIT_FLUSH_TEST_MAX_BATCH_WEIGHT: usize = 10_000;
const EXPLICIT_FLUSH_TEST_LATENCY: Duration = Duration::from_secs(1000);
const EXPLICIT_FLUSH_TEST_TIMEOUT: Duration = Duration::from_secs(120);

/// Returns the real pre-NU6.2 Orchard transactions in the mainnet test blocks.
///
/// These mainnet blocks are NU5-era Orchard history, mined long before NU6.2, so their proofs
/// were produced by the historical (insecure) circuit and only verify under
/// [`VERIFYING_KEY_PRE_NU6_2`]. Transactions with transparent inputs are skipped because their
/// sighash needs the previous outputs they spend, which are not in the test vectors.
fn pre_nu6_2_transactions() -> Vec<Transaction> {
    let mut transactions = Vec::new();

    for bytes in zakura_test::vectors::MAINNET_BLOCKS.values() {
        let block: Block = bytes
            .zcash_deserialize_into()
            .expect("hard-coded test vector must deserialize");

        for tx in &block.transactions {
            if tx.orchard_shielded_data().is_none() || !tx.inputs().is_empty() {
                continue;
            }

            if bundle_and_sighash(tx).is_some() {
                transactions.push(tx.as_ref().clone());
            }
        }
    }

    assert!(
        !transactions.is_empty(),
        "mainnet test blocks must contain a transparent-input-free Orchard transaction"
    );

    transactions
}

/// Returns `tx`'s Orchard bundle and the sighash it is verified against, if it has one.
fn bundle_and_sighash(tx: &Transaction) -> Option<(Bundle<Authorized, ZatBalance>, SigHash)> {
    let all_previous_outputs: Arc<Vec<transparent::Output>> = Arc::new(Vec::new());
    let sighasher = tx
        .sighasher(NetworkUpgrade::Nu5, all_previous_outputs)
        .ok()?;
    let bundle = sighasher.orchard_bundle()?;

    Some((bundle, sighasher.sighash(HashType::ALL, None)))
}

/// Returns one real pre-NU6.2 Orchard bundle and its sighash.
fn pre_nu6_2_bundle_and_sighash() -> (Bundle<Authorized, ZatBalance>, SigHash) {
    let tx = pre_nu6_2_transactions()
        .into_iter()
        .next()
        .expect("there is at least one pre-NU6.2 Orchard transaction");

    bundle_and_sighash(&tx).expect("the transaction was selected for having a bundle")
}

fn explicit_flush_verifier(vk: &'static ItemVerifyingKey) -> BatchFallbackService {
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

// Cache key completeness.
//
// [`Cached`] reuses a previous `Ok` for any item whose key matches, so the key
// must uniquely identify the transaction and its bundle slot.

/// Returns a deterministic witnessed transaction ID for cache behaviour tests.
fn test_wtx_id(tag: u8) -> WtxId {
    WtxId {
        id: Hash([tag; 32]),
        auth_digest: AuthDigest([tag.wrapping_add(1); 32]),
    }
}

/// Returns a cacheable verification item.
fn cacheable_item(
    bundle: &Bundle<Authorized, ZatBalance>,
    sighash: SigHash,
    wtx_id: WtxId,
) -> Item {
    Item::new_with_wtx_id(bundle.clone(), sighash, wtx_id)
}

/// Returns the cache key for `bundle`'s pool, `sighash`, and `wtx_id`.
fn cache_key(bundle: &Bundle<Authorized, ZatBalance>, sighash: SigHash, wtx_id: WtxId) -> CacheKey {
    cacheable_item(bundle, sighash, wtx_id)
        .cache_key()
        .expect("an item constructed with a wtxid is cacheable")
}

/// Returns `tx` with `mutate` applied to its Orchard authorizing data.
///
/// These mutations leave the txid unchanged but change the authorizing-data
/// digest in its [`WtxId`]. That is the shape of CVE-2026-34377: a key derived
/// from the txid alone would collide here.
fn mutated_transaction(
    tx: &Transaction,
    mutate: impl FnOnce(&mut zakura_chain::orchard::ShieldedData),
) -> Transaction {
    let mut mutated = tx.clone();
    mutate(
        mutated
            .orchard_shielded_data_mut()
            .expect("the transaction was selected for having Orchard shielded data"),
    );

    assert_eq!(
        tx.hash(),
        mutated.hash(),
        "mutating authorizing data must leave the txid unchanged, or this test proves nothing"
    );

    mutated
}

#[test]
fn cache_key_is_deterministic() {
    let (bundle, sighash) = pre_nu6_2_bundle_and_sighash();
    let wtx_id = test_wtx_id(1);

    assert_eq!(
        cache_key(&bundle, sighash, wtx_id),
        cache_key(&bundle, sighash, wtx_id),
        "the same wtxid, sighash, and pool must always produce the same key"
    );
}

#[test]
fn cache_key_commits_to_the_txid_and_authorizing_data() {
    let (bundle, sighash) = pre_nu6_2_bundle_and_sighash();
    let original_wtx_id = test_wtx_id(1);
    let original = cache_key(&bundle, sighash, original_wtx_id);

    let mut different_txid = original_wtx_id;
    different_txid.id.0[0] ^= 1;
    assert_ne!(
        original,
        cache_key(&bundle, sighash, different_txid),
        "the transaction ID must be part of the cache key"
    );

    let mut different_authorizing_data = original_wtx_id;
    different_authorizing_data.auth_digest.0[0] ^= 1;
    assert_ne!(
        original,
        cache_key(&bundle, sighash, different_authorizing_data),
        "the authorizing-data digest must be part of the cache key"
    );
}

#[test]
fn cache_key_commits_to_the_sighash() {
    let (bundle, sighash) = pre_nu6_2_bundle_and_sighash();
    let wtx_id = test_wtx_id(1);
    let original = cache_key(&bundle, sighash, wtx_id);

    let mut different_sighash = sighash;
    different_sighash.0[0] ^= 1;

    assert_ne!(
        original,
        cache_key(&bundle, different_sighash, wtx_id),
        "different verification contexts must get different cache keys"
    );
}

/// Every piece of authorizing data verified in the Halo2 batch changes the
/// witnessed transaction ID, and therefore the cache key.
#[test]
fn cache_key_changes_with_every_piece_of_authorizing_data() {
    let tx = pre_nu6_2_transactions()
        .into_iter()
        .next()
        .expect("there is at least one pre-NU6.2 Orchard transaction");
    let (bundle, sighash) = bundle_and_sighash(&tx).expect("the transaction has a bundle");
    let original = cache_key(&bundle, sighash, WtxId::from(&tx));

    for (name, mutate) in [
        (
            "Halo2 proof",
            (|data: &mut zakura_chain::orchard::ShieldedData| {
                data.proof = Halo2Proof(vec![0xDE, 0xAD, 0xBE, 0xEF]);
            }) as fn(&mut zakura_chain::orchard::ShieldedData),
        ),
        ("binding signature", |data| {
            data.binding_sig = [0xFF; 64].into();
        }),
        ("spend authorization signatures", |data| {
            for action in data.actions.iter_mut() {
                action.spend_auth_sig = [0xFF; 64].into();
            }
        }),
    ] {
        let mutated = mutated_transaction(&tx, mutate);
        let (mutated_bundle, mutated_sighash) =
            bundle_and_sighash(&mutated).expect("the mutated transaction still has a bundle");

        assert_ne!(
            original,
            cache_key(&mutated_bundle, mutated_sighash, WtxId::from(&mutated)),
            "mutating the {name} must change the cache key"
        );
    }
}

/// Returns `bundle`'s parts rebuilt under `flags` and `version`.
fn rebuilt_as(
    bundle: &Bundle<Authorized, ZatBalance>,
    flags: Flags,
    version: BundleVersion,
) -> Bundle<Authorized, ZatBalance> {
    Bundle::try_from_parts(
        bundle.actions().clone(),
        flags,
        *bundle.value_balance(),
        *bundle.anchor(),
        bundle.authorization().clone(),
        version,
    )
    .expect("a real mainnet Orchard bundle's parts are representable under the given version")
}

/// The Orchard and Ironwood pools of one v6 transaction never share a cache key.
///
/// A v6 transaction gives both bundles the same [`WtxId`]. Both pools also use
/// the NU6.3 circuit and therefore the same cache, so the value-pool tag names
/// which bundle slot earned an entry.
///
/// The two bundles here are built from identical parts, and with cross-address transfers disabled
/// their flag bytes are identical too, so their consensus encodings are byte-for-byte the same.
/// Only the pool tag tells them apart.
#[test]
fn cache_key_distinguishes_the_orchard_and_ironwood_pools() {
    let (bundle, sighash) = pre_nu6_2_bundle_and_sighash();
    let wtx_id = test_wtx_id(1);

    // Cross-address transfers are disallowed in the NU6.3 Orchard pool and optional in Ironwood,
    // so this is the one flag set both pools can encode — and they encode it identically.
    let orchard = rebuilt_as(
        &bundle,
        Flags::CROSS_ADDRESS_DISABLED,
        BundleVersion::orchard_v3(),
    );
    let ironwood = rebuilt_as(
        &bundle,
        Flags::CROSS_ADDRESS_DISABLED,
        BundleVersion::ironwood_v3(),
    );

    assert_eq!(
        orchard.flag_byte(),
        ironwood.flag_byte(),
        "this test is only meaningful if the two pools encode these flags identically"
    );
    assert_ne!(
        cache_key(&orchard, sighash, wtx_id),
        cache_key(&ironwood, sighash, wtx_id),
        "the Orchard and Ironwood bundles of one transaction must not share a cache key"
    );
}

// Caching behaviour.

/// An inner verification service that counts calls and returns a fixed result.
#[derive(Clone)]
struct CountingVerifier {
    calls: Arc<AtomicUsize>,
    succeeds: bool,
}

impl CountingVerifier {
    fn new(succeeds: bool) -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
            succeeds,
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl Service<Item> for CountingVerifier {
    type Response = ();
    type Error = BoxError;
    type Future = future::Ready<Result<(), BoxError>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _item: Item) -> Self::Future {
        self.calls.fetch_add(1, Ordering::SeqCst);

        future::ready(if self.succeeds {
            Ok(())
        } else {
            Err(TransactionError::Halo2VerificationFailed.into())
        })
    }
}

#[tokio::test]
async fn cache_skips_the_inner_service_for_an_already_verified_item() {
    let (bundle, sighash) = pre_nu6_2_bundle_and_sighash();
    let inner = CountingVerifier::new(true);
    let mut verifier = Cached::new(inner.clone(), 8, TEST_CACHE_VERIFIER_LABEL);

    for _ in 0..3 {
        verifier
            .ready()
            .await
            .expect("the cache must become ready")
            .call(cacheable_item(&bundle, sighash, test_wtx_id(1)))
            .await
            .expect("a valid item must verify");
    }

    assert_eq!(
        inner.calls(),
        1,
        "only the first verification of an item may reach the inner service"
    );
}

#[tokio::test]
async fn items_without_a_wtxid_are_not_cached() {
    let (bundle, sighash) = pre_nu6_2_bundle_and_sighash();
    let inner = CountingVerifier::new(true);
    let mut verifier = Cached::new(inner.clone(), 8, TEST_CACHE_VERIFIER_LABEL);

    for _ in 0..2 {
        verifier
            .ready()
            .await
            .expect("the cache must become ready")
            .call(Item::new(bundle.clone(), sighash))
            .await
            .expect("an item without a wtxid must still verify");
    }

    assert_eq!(
        inner.calls(),
        2,
        "an item without a wtxid must never inherit a cached result"
    );
}

#[tokio::test]
async fn cache_does_not_reuse_a_result_across_items() {
    let (bundle, sighash) = pre_nu6_2_bundle_and_sighash();

    let inner = CountingVerifier::new(true);
    let mut verifier = Cached::new(inner.clone(), 8, TEST_CACHE_VERIFIER_LABEL);

    for wtx_id in [test_wtx_id(1), test_wtx_id(2)] {
        verifier
            .ready()
            .await
            .expect("the cache must become ready")
            .call(cacheable_item(&bundle, sighash, wtx_id))
            .await
            .expect("the inner service accepts everything in this test");
    }

    assert_eq!(
        inner.calls(),
        2,
        "items with different keys must each be verified"
    );
}

/// A failure is never remembered.
///
/// A batch error is not per-item evidence — `Fallback` resolves those by re-verifying singly —
/// and an error can report that the batch worker shut down rather than that a proof is invalid.
/// Remembering either as "invalid" would make the node reject valid blocks.
#[tokio::test]
async fn cache_does_not_remember_failures() {
    let (bundle, sighash) = pre_nu6_2_bundle_and_sighash();
    let inner = CountingVerifier::new(false);
    let mut verifier = Cached::new(inner.clone(), 8, TEST_CACHE_VERIFIER_LABEL);

    for _ in 0..3 {
        verifier
            .ready()
            .await
            .expect("the cache must become ready")
            .call(cacheable_item(&bundle, sighash, test_wtx_id(1)))
            .await
            .expect_err("the inner service rejects everything in this test");
    }

    assert_eq!(
        inner.calls(),
        3,
        "a failed verification must be retried, not remembered"
    );
}

/// Verifies `item` through `verifier`, asserting that it succeeds.
async fn verify_through<S>(verifier: &mut Cached<S>, item: Item)
where
    S: Service<Item, Response = (), Error = BoxError> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    verifier
        .ready()
        .await
        .expect("the cache must become ready")
        .call(item)
        .await
        .expect("the inner service accepts everything in this test");
}

#[tokio::test]
async fn cache_evicts_in_insertion_order_and_stays_correct_when_full() {
    let (bundle, sighash) = pre_nu6_2_bundle_and_sighash();
    let inner = CountingVerifier::new(true);
    let mut verifier = Cached::new(inner.clone(), 2, TEST_CACHE_VERIFIER_LABEL);

    let wtx_ids = [test_wtx_id(1), test_wtx_id(2), test_wtx_id(3)];

    for wtx_id in &wtx_ids {
        verify_through(&mut verifier, cacheable_item(&bundle, sighash, *wtx_id)).await;
    }
    assert_eq!(
        inner.calls(),
        3,
        "three distinct items, three verifications"
    );

    // The two most recent are still remembered.
    for wtx_id in &wtx_ids[1..] {
        verify_through(&mut verifier, cacheable_item(&bundle, sighash, *wtx_id)).await;
    }
    assert_eq!(inner.calls(), 3, "entries within the capacity must be kept");

    // The oldest was evicted, so it is verified again rather than silently mis-answered.
    verify_through(&mut verifier, cacheable_item(&bundle, sighash, wtx_ids[0])).await;
    assert_eq!(inner.calls(), 4, "an evicted entry must be re-verified");
}

/// A remembered result is never visible to another era's cache.
///
/// The cache key deliberately does not name the verifying key. What binds an entry to the key it
/// was produced under is which era's cache holds it — [`batch_verifier`](super::batch_verifier)
/// builds one per era, and [`verifier_routes_each_network_upgrade_to_the_correct_key`] pins the
/// routing. This pins the other half: two cache instances share no state, so an item verified
/// under the pre-NU6.2 insecure key can never be answered from that entry when it is later
/// routed to a different era's verifier.
#[tokio::test]
async fn a_result_cached_under_one_era_is_not_visible_to_another() {
    let (bundle, sighash) = pre_nu6_2_bundle_and_sighash();
    let item = cacheable_item(&bundle, sighash, test_wtx_id(9));

    let inner = CountingVerifier::new(true);
    let mut one_era = Cached::new(inner.clone(), 8, TEST_CACHE_VERIFIER_LABEL);
    let mut another_era = Cached::new(inner.clone(), 8, TEST_CACHE_VERIFIER_LABEL);

    verify_through(&mut one_era, item.clone()).await;
    assert_eq!(inner.calls(), 1, "the first verification must be a miss");

    verify_through(&mut another_era, item).await;
    assert_eq!(
        inner.calls(),
        2,
        "another era's cache must not answer from an entry this one never recorded"
    );
}

/// Clones of a cache answer from the same set of verified proofs.
///
/// Production never calls a global verifier directly: [`super::verifier_for`] hands out a
/// `&'static` handle and every request goes through a fresh `.clone()` of it. A cache that lived
/// in the handle rather than behind the shared `Arc` would be empty for every request, so this
/// pins the sharing that makes the cache reachable at all.
#[tokio::test]
async fn cache_is_shared_between_clones() {
    let (bundle, sighash) = pre_nu6_2_bundle_and_sighash();
    let item = cacheable_item(&bundle, sighash, test_wtx_id(1));

    let inner = CountingVerifier::new(true);
    let verifier = Cached::new(inner.clone(), 8, TEST_CACHE_VERIFIER_LABEL);

    let mut warming_clone = verifier.clone();
    verify_through(&mut warming_clone, item.clone()).await;
    assert_eq!(inner.calls(), 1, "the first verification must be a miss");

    let mut reading_clone = verifier.clone();
    verify_through(&mut reading_clone, item).await;
    assert_eq!(
        inner.calls(),
        1,
        "a clone must answer from the result another clone recorded"
    );
}

/// An inner service that never returns a result, standing in for a verification in flight.
#[derive(Clone)]
struct PendingVerifier {
    calls: Arc<AtomicUsize>,
}

impl PendingVerifier {
    fn new() -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl Service<Item> for PendingVerifier {
    type Response = ();
    type Error = BoxError;
    type Future = future::Pending<Result<(), BoxError>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _item: Item) -> Self::Future {
        self.calls.fetch_add(1, Ordering::SeqCst);
        future::pending()
    }
}

/// A verification that is cancelled before it returns is not remembered.
///
/// The cache records a key from inside the response future, so dropping that future has to leave
/// the cache untouched. Recording on the way in would remember a proof that was never checked:
/// callers drop these futures routinely, because a block or mempool verification abandons its
/// remaining checks as soon as one of them fails.
#[tokio::test]
async fn cancelling_a_verification_does_not_populate_the_cache() {
    let (bundle, sighash) = pre_nu6_2_bundle_and_sighash();
    let item = cacheable_item(&bundle, sighash, test_wtx_id(1));

    let hanging = PendingVerifier::new();
    let mut verifier = Cached::new(hanging.clone(), 8, TEST_CACHE_VERIFIER_LABEL);

    // Start a verification and drop it before the inner service can answer.
    let in_flight = verifier
        .ready()
        .await
        .expect("the cache must become ready")
        .call(item.clone());
    tokio::time::timeout(Duration::from_millis(50), in_flight)
        .await
        .expect_err("the inner service never returns, so the verification cannot complete");
    assert_eq!(
        hanging.calls(),
        1,
        "the cancelled verification must have reached the inner service"
    );

    // Retrying must verify again rather than read an entry the cancelled call never earned.
    let mut verifier = verifier.with_inner(CountingVerifier::new(true));
    let counting = verifier.inner().clone();
    verify_through(&mut verifier, item).await;
    assert_eq!(
        counting.calls(),
        1,
        "a cancelled verification must not be remembered as a success"
    );
}

/// An inner service whose readiness always fails, standing in for a dead batch worker.
///
/// `Batch::poll_ready` reports an error when its worker has exited, panicked, or closed its
/// channel. `call` panics here because it must never be reached: a service that is not ready
/// must not be called, and the tests below are about what happens *before* that point.
#[derive(Clone)]
struct UnreadyVerifier {
    poll_readies: Arc<AtomicUsize>,
}

impl UnreadyVerifier {
    fn new() -> Self {
        Self {
            poll_readies: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn poll_readies(&self) -> usize {
        self.poll_readies.load(Ordering::SeqCst)
    }
}

impl Service<Item> for UnreadyVerifier {
    type Response = ();
    type Error = BoxError;
    type Future = future::Ready<Result<(), BoxError>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_readies.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(Err(BoxError::from("batch worker finished unexpectedly")))
    }

    fn call(&mut self, _item: Item) -> Self::Future {
        unreachable!("a service whose poll_ready failed must not be called")
    }
}

/// A cache hit is answered even when the inner service can no longer become ready.
///
/// `Cached::poll_ready` must not delegate to the inner service. Callers poll readiness before
/// `call`, so delegating would surface a dead batch worker's error for an item whose result the
/// cache already holds — reporting a verified proof as a verification failure, and rejecting a
/// valid block. That is the "an error need not be a verdict" case the module docs are about.
#[tokio::test]
async fn cache_hit_survives_an_inner_service_that_never_becomes_ready() {
    let (bundle, sighash) = pre_nu6_2_bundle_and_sighash();
    let item = cacheable_item(&bundle, sighash, test_wtx_id(1));

    // Warm the cache through a healthy inner service.
    let healthy = CountingVerifier::new(true);
    let mut verifier = Cached::new(healthy.clone(), 8, TEST_CACHE_VERIFIER_LABEL);
    verify_through(&mut verifier, item.clone()).await;
    assert_eq!(healthy.calls(), 1, "the first verification must be a miss");

    // Swap in an inner service that can never become ready, keeping the same cache.
    let dead = UnreadyVerifier::new();
    let mut verifier = verifier.with_inner(dead.clone());

    verifier
        .ready()
        .await
        .expect("the cache must be ready even when the inner service is not")
        .call(item)
        .await
        .expect("a cache hit must be answered from the cache, not from the dead inner service");

    assert_eq!(
        dead.poll_readies(),
        0,
        "a hit must not poll the inner service for readiness at all"
    );
}

/// A miss still propagates an inner readiness failure.
///
/// Moving readiness off `poll_ready` must not make the cache swallow it: an item that is not in
/// the cache has to reach the inner service, and if that service cannot become ready the request
/// must fail rather than be reported as verified.
#[tokio::test]
async fn cache_miss_propagates_an_inner_readiness_failure() {
    let (bundle, sighash) = pre_nu6_2_bundle_and_sighash();
    let dead = UnreadyVerifier::new();
    let mut verifier = Cached::new(dead.clone(), 8, TEST_CACHE_VERIFIER_LABEL);

    verifier
        .ready()
        .await
        .expect("the cache itself is always ready")
        .call(cacheable_item(&bundle, sighash, test_wtx_id(1)))
        .await
        .expect_err("a miss must surface the inner service's readiness failure");

    assert!(
        dead.poll_readies() > 0,
        "a miss must acquire inner readiness"
    );

    // And the failure must not be remembered as a success.
    let mut verifier = verifier.with_inner(CountingVerifier::new(true));
    let counting = verifier.inner().clone();
    verify_through(
        &mut verifier,
        cacheable_item(&bundle, sighash, test_wtx_id(1)),
    )
    .await;
    assert_eq!(
        counting.calls(),
        1,
        "the item must still be verified, so the readiness failure was not recorded as an Ok"
    );
}

/// Builds an output-only NU6.2-era (fixed circuit) Orchard bundle and proves
/// it with `pk`.
///
/// Returns the authorized bundle and the sighash its signatures commit to.
fn prove_shielding_bundle(pk: &ProvingKey) -> (Bundle<Authorized, ZatBalance>, SigHash) {
    let mut rng = rand_10::rng();

    let sk = SpendingKey::from_bytes([7; 32]).expect("hard-coded test key bytes are valid");
    let recipient = FullViewingKey::from(&sk).address_at(0u32, Scope::External);

    let mut builder = Builder::new(
        BundleType::DEFAULT,
        BundleVersion::orchard_v2(),
        Flags::SPENDS_DISABLED,
        Anchor::empty_tree(),
    )
    .expect("spends-disabled flags are valid for an Orchard v2 bundle");
    builder
        .add_output(None, recipient, NoteValue::from_raw(5000), [0u8; 512])
        .expect("adding one output to an empty builder succeeds");

    let (unauthorized, _bundle_meta) = builder
        .build::<ZatBalance>(&mut rng)
        .expect("an output-only bundle builds")
        .expect("the bundle is nonempty: it has an output");

    let sighash: [u8; 32] = unauthorized
        .commitment(TxVersion::V5)
        .expect("spends-disabled flags are representable in a v5 bundle")
        .into();

    let bundle = unauthorized
        .create_proof(pk, &mut rng)
        .expect("proving an output-only bundle succeeds")
        .apply_signatures(&mut rng, sighash, &[])
        .expect("an output-only bundle needs no spend authorizing keys");

    (bundle, SigHash(sighash))
}

/// Arming halo2's prepared-MSM state must not change proving or verification
/// results.
///
/// The production verifying-key statics are armed at construction
/// ([`super::build_verifying_key`]); freshly built keys are the unarmed
/// control. With halo2's opt-in `orbits` feature off (the current build),
/// arming is a documented no-op, so this pins that arming unconditionally is
/// harmless; in a build with the feature on, the same assertions compare the
/// prepared and unprepared paths for real.
#[test]
fn prepared_msm_arming_does_not_change_proving_or_verification() {
    let _init_guard = zakura_test::init();

    // Repeat arming of the already-armed statics is documented as free and
    // must report the same outcome every time.
    assert_eq!(
        VERIFYING_KEY_NU6_2.prepare_batch_validation(),
        VERIFYING_KEY_NU6_2.prepare_batch_validation(),
        "repeat verifier arming must be idempotent",
    );

    // Unarmed control keys, freshly built so no prepared state is cached.
    let unarmed_insecure_vk = ItemVerifyingKey::build(OrchardCircuitVersion::InsecurePreNu6_2);
    let unarmed_fixed_vk = ItemVerifyingKey::build(OrchardCircuitVersion::FixedPostNu6_2);

    // Verification parity on a real mainnet proof: armed and unarmed keys
    // must accept it under its own circuit era and reject it under the wrong
    // era.
    let (bundle, sighash) = pre_nu6_2_bundle_and_sighash();
    assert!(
        Item::new(bundle.clone(), sighash).verify_single(&VERIFYING_KEY_PRE_NU6_2),
        "armed pre-NU6.2 key must accept a real pre-NU6.2 proof",
    );
    assert!(
        Item::new(bundle.clone(), sighash).verify_single(&unarmed_insecure_vk),
        "unarmed pre-NU6.2 key must accept a real pre-NU6.2 proof",
    );
    assert!(
        !Item::new(bundle.clone(), sighash).verify_single(&VERIFYING_KEY_NU6_2),
        "armed NU6.2 key must reject a pre-NU6.2 proof",
    );
    assert!(
        !Item::new(bundle, sighash).verify_single(&unarmed_fixed_vk),
        "unarmed NU6.2 key must reject a pre-NU6.2 proof",
    );

    // Proving parity: prove once with an unarmed proving key, arm it, prove
    // again. Both proofs must verify under both the armed static and the
    // unarmed control key.
    let pk = ProvingKey::build(OrchardCircuitVersion::FixedPostNu6_2);
    let unarmed_prover_result = prove_shielding_bundle(&pk);

    let first_arming = pk.prepare_proving();
    assert_eq!(
        first_arming,
        pk.prepare_proving(),
        "repeat prover arming must be idempotent",
    );

    let armed_prover_result = prove_shielding_bundle(&pk);

    for (bundle, sighash, prover) in [
        (unarmed_prover_result.0, unarmed_prover_result.1, "unarmed"),
        (armed_prover_result.0, armed_prover_result.1, "armed"),
    ] {
        assert!(
            Item::new(bundle.clone(), sighash).verify_single(&VERIFYING_KEY_NU6_2),
            "armed NU6.2 key must accept the proof from the {prover} prover",
        );
        assert!(
            Item::new(bundle, sighash).verify_single(&unarmed_fixed_vk),
            "unarmed NU6.2 key must accept the proof from the {prover} prover",
        );
    }
}
