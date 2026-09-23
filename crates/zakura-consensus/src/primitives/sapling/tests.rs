//! Tests for the Sapling bundle verifier.
//!
//! Most of these are cache-key completeness tests. [`Cached`] reuses a previous `Ok` for any item
//! whose key matches, so a key that misses one of verification's inputs is a consensus bug: it
//! would accept a bundle that was never checked.
//!
//! Sapling keys its entries the same way Halo2 does — transaction ID, sighash and pool — but its
//! bundles also appear in v4 transactions, which have no witnessed ID. These tests pin both
//! forms: that a v4 transaction's legacy ID covers its Sapling proofs and signatures, and that a
//! v5 transaction's authorizing-data digest does, since its txid does not.

use std::{
    future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
};

use tower::{Service, ServiceExt};
use zakura_chain::{
    amount::Amount,
    block::{Block, Height},
    parameters::{Network, NetworkUpgrade},
    primitives::Groth16Proof,
    sapling::{Nullifier, Output, PerSpendAnchor, ShieldedData, Spend, TransferData},
    serialization::{AtLeastOne, ZcashDeserializeInto},
    transaction::{arbitrary::transaction_to_fake_v5, HashType, Transaction},
    transparent,
};

use crate::BoxError;

use super::{sapling_prover, Authorized, Bundle, CacheKey, Cached, CachedItem, Item, ZatBalance};

/// The `verifier` label the test caches report their metrics under.
///
/// Test caches use their own label so their counts never land in the series the production
/// verifier reports.
const TEST_CACHE_VERIFIER_LABEL: &str = "groth16_sapling_test";

#[test]
fn sapling_prover_is_reused() {
    assert!(std::ptr::eq(sapling_prover(), sapling_prover()));
}

/// Returns the mainnet test transactions that carry a Sapling bundle, with the network upgrade
/// each one was mined under.
///
/// Transactions with transparent inputs are skipped, because their sighash needs the previous
/// outputs they spend, which are not in the test vectors.
///
/// The upgrade matters for the tests that actually verify: a V4 sighash commits to the consensus
/// branch id, so a real bundle only verifies under the upgrade its block was mined in. The
/// cache-key tests do not need a valid sighash and pass an upgrade of their own.
fn mined_sapling_transactions() -> Vec<(NetworkUpgrade, Transaction)> {
    let mut transactions = Vec::new();

    for (height, bytes) in zakura_test::vectors::MAINNET_BLOCKS.iter() {
        let block: Block = bytes
            .zcash_deserialize_into()
            .expect("hard-coded test vector must deserialize");

        let nu = NetworkUpgrade::current(&Network::Mainnet, Height(*height));

        for tx in &block.transactions {
            if !tx.inputs().is_empty() {
                continue;
            }

            if item(tx, nu).is_some() {
                transactions.push((nu, tx.as_ref().clone()));
            }
        }
    }

    assert!(
        !transactions.is_empty(),
        "mainnet test blocks must contain a transparent-input-free Sapling transaction"
    );

    transactions
}

/// Returns the mainnet test transactions that carry a Sapling bundle.
fn sapling_transactions() -> Vec<Transaction> {
    mined_sapling_transactions()
        .into_iter()
        .map(|(_, tx)| tx)
        .collect()
}

/// Returns one real mainnet Sapling transaction.
fn sapling_transaction() -> Transaction {
    sapling_transactions()
        .into_iter()
        .next()
        .expect("there is at least one Sapling transaction")
}

/// Returns one real mainnet V4 Sapling transaction that has spends, with the network upgrade it
/// was mined under.
///
/// It has spends so that every field `check_bundle` batches is present to mutate, and it is V4
/// because that is the shape the mutation helpers are written against.
fn mined_v4_sapling_transaction_with_spends() -> (NetworkUpgrade, Transaction) {
    mined_sapling_transactions()
        .into_iter()
        .find(|(_, tx)| {
            matches!(tx, Transaction::V4 { .. }) && tx.sapling_spends_per_anchor().next().is_some()
        })
        .expect("mainnet test blocks must contain a V4 Sapling transaction with spends")
}

/// Returns one real mainnet V4 Sapling transaction that has spends.
fn sapling_transaction_with_spends() -> Transaction {
    mined_v4_sapling_transaction_with_spends().1
}

/// Returns the verification item for `tx`'s Sapling bundle under `nu`, if it has one.
fn item(tx: &Transaction, nu: NetworkUpgrade) -> Option<Item> {
    let all_previous_outputs: Arc<Vec<transparent::Output>> = Arc::new(Vec::new());
    let sighasher = tx.sighasher(nu, all_previous_outputs).ok()?;
    let bundle = sighasher.sapling_bundle()?;

    Some(Item::new(
        bundle,
        sighasher.sighash(HashType::ALL, None),
        tx.unmined_id(),
    ))
}

/// Returns the cache key of `tx`'s Sapling bundle under `nu`.
fn cache_key(tx: &Transaction, nu: NetworkUpgrade) -> CacheKey {
    item(tx, nu)
        .expect("the transaction was selected for having a Sapling bundle")
        .cache_key()
        .expect("every Sapling item carries a cache key")
}

/// Returns `tx` with `mutate` applied to its V4 Sapling shielded data.
///
/// The Sapling test vectors are V4 transactions, so the mutations are written against that shape
/// once; the V5 tests convert the result with [`as_fake_v5`].
fn mutated_transaction(
    tx: &Transaction,
    mutate: impl FnOnce(&mut ShieldedData<PerSpendAnchor>),
) -> Transaction {
    let mut mutated = tx.clone();

    let Transaction::V4 {
        sapling_shielded_data: Some(shielded_data),
        ..
    } = &mut mutated
    else {
        panic!("this fixture is a V4 transaction with Sapling shielded data")
    };
    mutate(shielded_data);

    mutated
}

/// Returns `tx` with `mutate` applied to the first spend of its V4 Sapling shielded data.
fn mutated_spend(tx: &Transaction, mutate: impl FnOnce(&mut Spend<PerSpendAnchor>)) -> Transaction {
    mutated_transaction(tx, |shielded_data| {
        let TransferData::SpendsAndMaybeOutputs { spends, .. } = &mut shielded_data.transfers
        else {
            panic!("the fixture was selected for having spends")
        };

        let mut spends_vec = spends.as_slice().to_vec();
        mutate(&mut spends_vec[0]);
        *spends =
            AtLeastOne::from_vec(spends_vec).expect("replacing a field keeps at least one spend");
    })
}

/// Returns a fingerprint of everything `check_bundle` reads out of `bundle`.
///
/// Used where a test needs "these two transactions carry the same bundle" as a precondition.
/// Comparing the fields directly rather than a `Debug` rendering keeps the assertion meaningful
/// if an upstream `Debug` is ever redacted.
fn bundle_fingerprint(bundle: &Bundle<Authorized, ZatBalance>) -> Vec<Vec<u8>> {
    let mut parts = vec![
        i64::from(*bundle.value_balance()).to_le_bytes().to_vec(),
        <[u8; 64]>::from(bundle.authorization().binding_sig).to_vec(),
    ];

    for spend in bundle.shielded_spends() {
        parts.push(spend.anchor().to_bytes().to_vec());
        parts.push(spend.nullifier().0.to_vec());
        parts.push(spend.zkproof().to_vec());
        parts.push(<[u8; 64]>::from(*spend.spend_auth_sig()).to_vec());
    }

    for output in bundle.shielded_outputs() {
        parts.push(output.cmu().to_bytes().to_vec());
        parts.push(output.ephemeral_key().0.to_vec());
        parts.push(output.zkproof().to_vec());
    }

    parts
}

/// Returns `tx` with `mutate` applied to the first output of its V4 Sapling shielded data.
fn mutated_output(tx: &Transaction, mutate: impl FnOnce(&mut Output)) -> Transaction {
    mutated_transaction(tx, |shielded_data| {
        let TransferData::SpendsAndMaybeOutputs { maybe_outputs, .. } =
            &mut shielded_data.transfers
        else {
            panic!("the fixture was selected for having spends")
        };

        mutate(
            maybe_outputs
                .first_mut()
                .expect("the fixture was selected for having outputs"),
        );
    })
}

/// Returns `tx` as a V5 transaction carrying the same Sapling bundle.
///
/// The Sapling test vectors are V4 transactions, so this is how the V5 side of the key — the
/// ZIP 244 authorizing-data digest — gets exercised over real bundles.
fn as_fake_v5(tx: &Transaction) -> Transaction {
    let network = Network::Mainnet;
    let nu5_height = NetworkUpgrade::Nu5
        .activation_height(&network)
        .expect("NU5 has an activation height on Mainnet");

    let v5 = transaction_to_fake_v5(tx, &network, nu5_height);
    assert!(
        matches!(v5, Transaction::V5 { .. }),
        "the fixture must have been converted to V5"
    );

    v5
}

#[test]
fn cache_key_is_deterministic() {
    let tx = sapling_transaction();

    assert_eq!(
        cache_key(&tx, NetworkUpgrade::Nu5),
        cache_key(&tx, NetworkUpgrade::Nu5),
        "the same transaction, bundle and sighash must always produce the same key"
    );
}

/// Two different Sapling bundles get different keys.
#[test]
fn cache_key_distinguishes_different_transactions() {
    let transactions = sapling_transactions();

    assert!(
        transactions.len() > 1,
        "this test needs at least two Sapling transactions in the test vectors"
    );

    let keys: Vec<_> = transactions
        .iter()
        .map(|tx| cache_key(tx, NetworkUpgrade::Nu5))
        .collect();

    let unique: std::collections::HashSet<_> = keys.iter().collect();
    assert_eq!(
        unique.len(),
        keys.len(),
        "distinct Sapling transactions must not share a cache key"
    );
}

/// The sighash is part of the key.
///
/// It is not a function of the transaction alone: the amounts and scripts of the spent
/// transparent outputs enter it, and those come from the verification context.
#[test]
fn cache_key_commits_to_the_sighash() {
    let tx = sapling_transaction();
    let original = item(&tx, NetworkUpgrade::Nu5).expect("the transaction has a bundle");

    let mut tweaked_sighash = original.sighash;
    tweaked_sighash.0[0] ^= 1;
    let tweaked = Item::new(original.bundle.clone(), tweaked_sighash, tx.unmined_id());

    assert_ne!(
        original.cache_key(),
        tweaked.cache_key(),
        "the sighash is an input to verification, so it must be an input to the key"
    );
}

/// Every piece of authorizing data that verification reads changes a V4 transaction's key.
///
/// `check_bundle` batches each spend's proof and `spendAuthSig`, each output's proof, and the
/// binding signature, and `value_balance` enters the binding verification key. A v1-v4
/// transaction ID is the hash of the whole serialized transaction, so each of those is inside it.
#[test]
fn v4_cache_key_changes_with_every_piece_of_authorizing_data() {
    let tx = sapling_transaction_with_spends();
    let original = cache_key(&tx, NetworkUpgrade::Nu5);

    let mutations = authorizing_data_mutations(&tx).into_iter().chain([
        (
            mutated_value_balance(&tx),
            "the value balance, which enters the binding verification key",
        ),
        (
            mutated_nullifier(&tx),
            "a spend nullifier, which is a public input to its proof",
        ),
    ]);

    for (mutated, what) in mutations {
        assert_ne!(
            original,
            cache_key(&mutated, NetworkUpgrade::Nu5),
            "{what} is verified, so it must be an input to the key"
        );
    }
}

/// The same authorizing data changes a V5 transaction's key, which its txid does not cover.
///
/// This is the shape of CVE-2026-34377: under ZIP 244 a v5 transaction's txid excludes proofs and
/// signatures, so a key derived from the txid alone would collide across all of these mutations.
/// The [`WtxId`](zakura_chain::transaction::WtxId) in the key carries the authorizing-data digest
/// as well, which is what makes it complete.
#[test]
fn v5_cache_key_changes_with_authorizing_data_its_txid_ignores() {
    let v4 = sapling_transaction_with_spends();
    let v5 = as_fake_v5(&v4);
    let original = cache_key(&v5, NetworkUpgrade::Nu5);

    for (mutated, what) in authorizing_data_mutations(&v4) {
        let mutated = as_fake_v5(&mutated);

        assert_eq!(
            v5.hash(),
            mutated.hash(),
            "changing {what} must leave the V5 txid alone, or this test proves nothing"
        );
        assert_ne!(
            original,
            cache_key(&mutated, NetworkUpgrade::Nu5),
            "{what} is verified, so it must be an input to the key"
        );
    }

    // These are inputs to the same verification that a V5 txid does cover.
    for (mutated, what) in [
        (
            mutated_value_balance(&v4),
            "the value balance, which enters the binding verification key",
        ),
        (
            mutated_nullifier(&v4),
            "a spend nullifier, which is a public input to its proof",
        ),
    ] {
        assert_ne!(
            original,
            cache_key(&as_fake_v5(&mutated), NetworkUpgrade::Nu5),
            "{what} is verified, so it must be an input to the key"
        );
    }
}

/// Returns `tx` with each piece of the authorizing data `check_bundle` reads mutated in turn.
///
/// These are the fields ZIP 244 puts in a v5 transaction's authorizing-data digest rather than
/// its txid, so mutating one leaves the v5 txid unchanged.
fn authorizing_data_mutations(tx: &Transaction) -> Vec<(Transaction, &'static str)> {
    vec![
        (
            mutated_spend(tx, |spend| spend.zkproof = Groth16Proof([0xFF; 192])),
            "a spend proof",
        ),
        (
            mutated_spend(tx, |spend| spend.spend_auth_sig = [0xFF; 64].into()),
            "a spend authorization signature",
        ),
        (
            mutated_output(tx, |output| output.zkproof = Groth16Proof([0xFF; 192])),
            "an output proof",
        ),
        (
            mutated_transaction(tx, |shielded_data| {
                shielded_data.binding_sig = [0xFF; 64].into()
            }),
            "the binding signature",
        ),
    ]
}

/// Returns `tx` with the nullifier of its first Sapling spend changed.
///
/// The nullifier is effecting data rather than authorizing data — it is inside every transaction
/// ID including a v5 txid — but it is also a public input to the spend proof, so a key that
/// missed it would reuse one spend's verification for another's.
fn mutated_nullifier(tx: &Transaction) -> Transaction {
    mutated_spend(tx, |spend| spend.nullifier = Nullifier([0xFF; 32].into()))
}

/// Returns `tx` with its Sapling value balance changed.
///
/// `value_balance` is the one input to the binding signature check that is effecting data, so it
/// is inside every transaction ID rather than only the authorizing-data digest.
fn mutated_value_balance(tx: &Transaction) -> Transaction {
    mutated_transaction(tx, |shielded_data| {
        shielded_data.value_balance = (shielded_data.value_balance
            + Amount::try_from(1).expect("one is a valid amount"))
        .expect("the fixture's value balance is not at the maximum");
    })
}

/// A V4 transaction and a V5 carrying the same Sapling bundle do not share a key.
///
/// The two transaction IDs are computed differently and over different bytes, so this is
/// automatic. It is pinned because the two bundles verify identically, which is exactly when a
/// key that named the bundle instead of the transaction could collide.
#[test]
fn cache_key_distinguishes_v4_and_v5_carrying_the_same_bundle() {
    let v4 = sapling_transactions()
        .into_iter()
        .find(|tx| matches!(tx, Transaction::V4 { .. }))
        .expect("mainnet test blocks must contain a V4 Sapling transaction");
    let v5 = as_fake_v5(&v4);

    let v4_item = item(&v4, NetworkUpgrade::Nu5).expect("the V4 transaction has a bundle");
    let v5_item = item(&v5, NetworkUpgrade::Nu5).expect("the V5 transaction has a bundle");

    assert_eq!(
        bundle_fingerprint(&v4_item.bundle),
        bundle_fingerprint(&v5_item.bundle),
        "the conversion must carry the Sapling bundle across unchanged, or this test proves \
         nothing"
    );
    assert_ne!(
        v4_item.cache_key(),
        v5_item.cache_key(),
        "the same bundle in two transaction versions must not share a cache key"
    );
}

/// A verifier that counts calls and always returns the same verdict.
#[derive(Clone)]
struct CountingVerifier {
    calls: Arc<AtomicUsize>,
    accepts: bool,
}

impl CountingVerifier {
    fn new(accepts: bool) -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
            accepts,
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

        future::ready(if self.accepts {
            Ok(())
        } else {
            Err(BoxError::from("rejected"))
        })
    }
}

#[tokio::test]
async fn cache_skips_the_inner_service_for_an_already_verified_bundle() {
    let tx = sapling_transaction();
    let inner = CountingVerifier::new(true);
    let mut verifier = Cached::new(inner.clone(), 8, TEST_CACHE_VERIFIER_LABEL);

    for _ in 0..3 {
        verifier
            .ready()
            .await
            .expect("the cache must become ready")
            .call(item(&tx, NetworkUpgrade::Nu5).expect("the transaction has a bundle"))
            .await
            .expect("a valid item must verify");
    }

    assert_eq!(
        inner.calls(),
        1,
        "only the first verification of a bundle may reach the inner service"
    );
}

#[tokio::test]
async fn cache_does_not_reuse_a_result_across_bundles() {
    let transactions = sapling_transactions();
    let inner = CountingVerifier::new(true);
    let mut verifier = Cached::new(inner.clone(), 8, TEST_CACHE_VERIFIER_LABEL);

    for tx in transactions.iter().take(2) {
        verifier
            .ready()
            .await
            .expect("the cache must become ready")
            .call(item(tx, NetworkUpgrade::Nu5).expect("the transaction has a bundle"))
            .await
            .expect("the inner service accepts everything in this test");
    }

    assert_eq!(
        inner.calls(),
        2,
        "bundles with different keys must each be verified"
    );
}

/// Clearing a cache forces re-verification.
///
/// This is what [`clear_shielded_verification_caches`](crate::clear_shielded_verification_caches)
/// gives the benchmarks: a workload replayed against a cleared cache measures verification, not
/// hits.
#[tokio::test]
async fn clearing_the_cache_forces_reverification() {
    let tx = sapling_transaction();
    let inner = CountingVerifier::new(true);
    let mut verifier = Cached::new(inner.clone(), 8, TEST_CACHE_VERIFIER_LABEL);

    for _ in 0..2 {
        verifier
            .ready()
            .await
            .expect("the cache must become ready")
            .call(item(&tx, NetworkUpgrade::Nu5).expect("the transaction has a bundle"))
            .await
            .expect("a valid item must verify");
    }
    assert_eq!(inner.calls(), 1, "the second verification must be a hit");

    verifier.clear();

    verifier
        .ready()
        .await
        .expect("the cache must become ready")
        .call(item(&tx, NetworkUpgrade::Nu5).expect("the transaction has a bundle"))
        .await
        .expect("a valid item must verify");
    assert_eq!(
        inner.calls(),
        2,
        "a cleared cache must send the bundle back to the inner service"
    );
}

/// A failure is never remembered.
///
/// A batch error is not per-item evidence — `Fallback` resolves those by re-verifying singly —
/// and an error can report that the batch worker shut down rather than that a proof is invalid.
/// Remembering either as "invalid" would make the node reject valid blocks.
#[tokio::test]
async fn cache_does_not_remember_failures() {
    let tx = sapling_transaction();
    let inner = CountingVerifier::new(false);
    let mut verifier = Cached::new(inner.clone(), 8, TEST_CACHE_VERIFIER_LABEL);

    for _ in 0..3 {
        verifier
            .ready()
            .await
            .expect("the cache must become ready")
            .call(item(&tx, NetworkUpgrade::Nu5).expect("the transaction has a bundle"))
            .await
            .expect_err("the inner service rejects everything in this test");
    }

    assert_eq!(
        inner.calls(),
        3,
        "a failed verification must not be remembered"
    );
}

/// An inner service whose readiness always fails, standing in for a dead batch worker.
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

/// A Sapling cache hit is answered even when the inner service can no longer become ready.
///
/// The Sapling verifier shares [`Cached`] with Halo2, so this is the same property
/// `cache_hit_survives_an_inner_service_that_never_becomes_ready` pins there. It is worth pinning
/// per pool as well: the shared `poll_ready` must not start delegating again for either of them,
/// and a `Batch` whose worker has exited would otherwise turn a remembered `Ok` into a
/// verification failure.
#[tokio::test]
async fn cache_hit_survives_an_inner_service_that_never_becomes_ready() {
    let tx = sapling_transaction();

    let healthy = CountingVerifier::new(true);
    let mut verifier = Cached::new(healthy.clone(), 8, TEST_CACHE_VERIFIER_LABEL);
    verifier
        .ready()
        .await
        .expect("the cache must become ready")
        .call(item(&tx, NetworkUpgrade::Nu5).expect("the transaction has a bundle"))
        .await
        .expect("the first verification must succeed");
    assert_eq!(healthy.calls(), 1, "the first verification must be a miss");

    let dead = UnreadyVerifier::new();
    let mut verifier = verifier.with_inner(dead.clone());

    verifier
        .ready()
        .await
        .expect("the cache must be ready even when the inner service is not")
        .call(item(&tx, NetworkUpgrade::Nu5).expect("the transaction has a bundle"))
        .await
        .expect("a cache hit must be answered from the cache, not from the dead inner service");

    assert_eq!(
        dead.poll_readies(),
        0,
        "a hit must not poll the inner service for readiness at all"
    );
}

/// A Sapling cache miss still propagates an inner readiness failure, and does not record it as a
/// success.
#[tokio::test]
async fn cache_miss_propagates_an_inner_readiness_failure() {
    let tx = sapling_transaction();
    let dead = UnreadyVerifier::new();
    let mut verifier = Cached::new(dead.clone(), 8, TEST_CACHE_VERIFIER_LABEL);

    verifier
        .ready()
        .await
        .expect("the cache itself is always ready")
        .call(item(&tx, NetworkUpgrade::Nu5).expect("the transaction has a bundle"))
        .await
        .expect_err("a miss must surface the inner service's readiness failure");

    assert!(
        dead.poll_readies() > 0,
        "a miss must acquire inner readiness"
    );

    let counting = CountingVerifier::new(true);
    let mut verifier = verifier.with_inner(counting.clone());
    verifier
        .ready()
        .await
        .expect("the cache must become ready")
        .call(item(&tx, NetworkUpgrade::Nu5).expect("the transaction has a bundle"))
        .await
        .expect("the retry must succeed");
    assert_eq!(
        counting.calls(),
        1,
        "the bundle must still be verified, so the readiness failure was not recorded as an Ok"
    );
}

/// The real verification stack, behind a cache private to one test.
///
/// The production [`VERIFIER`](super::VERIFIER) is a process-wide `Lazy`, so its cache carries
/// whatever every other test in this binary has already verified. This builds the same batch and
/// fallback stack behind a fresh cache, so a test can prove what a cold cache does with real
/// Sapling verification underneath.
fn uncached_verification_behind_a_fresh_cache() -> Cached<super::BatchFallbackService> {
    Cached::new(
        super::batch_fallback_verifier(),
        8,
        TEST_CACHE_VERIFIER_LABEL,
    )
}

/// A bundle verified under one network upgrade is not reused under another.
///
/// The sighash is the only key component that separates these two verifications: one transaction
/// has one transaction ID whatever height it is verified at, but a V4 sighash commits to the
/// consensus branch id of the block that mines it (ZIP 143 and ZIP 243 put it in the BLAKE2b
/// personalization). So this is the end-to-end evidence for keying on the sighash at all: the
/// same bundle, the same transaction ID, a different branch id, and the remembered `Ok` must not
/// answer it.
///
/// Real verification runs underneath, so the second call is not merely a miss — the mainnet
/// signatures do not verify against another era's sighash, and the rejection proves the bundle
/// was actually checked rather than answered from the cache.
#[tokio::test(flavor = "multi_thread")]
async fn a_bundle_verified_under_one_upgrade_is_not_reused_under_another() {
    use crate::error::TransactionError;

    let _init_guard = zakura_test::init();

    let (mined_upgrade, tx) = mined_v4_sapling_transaction_with_spends();

    // Any other upgrade that accepts V4 transactions will do: only its branch id matters here.
    let other_upgrade = if mined_upgrade == NetworkUpgrade::Nu5 {
        NetworkUpgrade::Canopy
    } else {
        NetworkUpgrade::Nu5
    };

    let mined_item = item(&tx, mined_upgrade).expect("the transaction has a bundle");
    let other_item = item(&tx, other_upgrade).expect("the transaction has a bundle");
    assert_eq!(
        bundle_fingerprint(&mined_item.bundle),
        bundle_fingerprint(&other_item.bundle),
        "the branch id must not change the parsed bundle, or this test proves nothing"
    );
    assert_ne!(
        mined_item.cache_key(),
        other_item.cache_key(),
        "the two sighashes must produce different keys"
    );

    let verifier = uncached_verification_behind_a_fresh_cache();

    verifier
        .clone()
        .oneshot(mined_item)
        .await
        .expect("a real mainnet Sapling bundle must verify under the upgrade that mined it");

    let error = verifier
        .clone()
        .oneshot(other_item)
        .await
        .expect_err("the same bundle must not verify against another upgrade's sighash");

    let error = error
        .downcast::<TransactionError>()
        .expect("the verifier reports a typed transaction error");
    assert!(
        matches!(*error, TransactionError::SaplingVerificationFailed),
        "expected SaplingVerificationFailed, got: {error:?}"
    );
}

/// The cache still rejects an invalid bundle after a valid one, with real verification underneath.
///
/// The unit tests above use a stub inner service, so they prove the cache's own behaviour but not
/// that the key is sound over real bundles. This runs the real batch-and-fallback stack: a valid
/// mainnet bundle is verified (and therefore cached), then the same transaction with a corrupted
/// spend proof is submitted. A key that collided between the two would return the remembered `Ok`
/// and let an unverified proof through.
///
/// The corruption keeps the proof well-formed, so it passes `check_bundle`'s synchronous checks
/// and can only be caught by the batch validation the cache would have skipped.
#[tokio::test(flavor = "multi_thread")]
async fn cached_verifier_still_rejects_a_corrupted_proof_after_verifying_a_valid_one() {
    use crate::error::TransactionError;

    let _init_guard = zakura_test::init();

    let (nu, valid) = mined_v4_sapling_transaction_with_spends();

    let corrupted = mutated_spend(&valid, |spend| {
        // A Groth16 proof is two 48-byte compressed G1 elements around a 96-byte compressed G2
        // element. Swapping the G1 elements keeps the proof well-formed but invalid, so it fails
        // batch validation rather than the synchronous well-formedness checks.
        let (first, rest) = spend.zkproof.0.split_at_mut(48);
        first.swap_with_slice(&mut rest[96..144]);
    });

    let valid_item = item(&valid, nu).expect("the transaction has a bundle");
    let corrupted_item = item(&corrupted, nu).expect("the transaction has a bundle");
    assert_ne!(
        valid_item.cache_key(),
        corrupted_item.cache_key(),
        "the corrupted proof must key differently, or the cache would return the remembered Ok"
    );

    let verifier = uncached_verification_behind_a_fresh_cache();

    verifier
        .clone()
        .oneshot(valid_item)
        .await
        .expect("a real mainnet Sapling bundle must verify");

    let error =
        verifier.clone().oneshot(corrupted_item).await.expect_err(
            "a corrupted Sapling proof must be rejected even after a valid one verified",
        );

    let error = error
        .downcast::<TransactionError>()
        .expect("the verifier reports a typed transaction error");
    assert!(
        matches!(*error, TransactionError::SaplingVerificationFailed),
        "expected SaplingVerificationFailed, got: {error:?}"
    );
}
