//! Tests for the Sapling bundle verifier.
//!
//! Most of these are cache-key completeness tests. [`Memoized`] reuses a previous `Ok` for any
//! item whose key matches, so a key that misses one of verification's inputs is a consensus bug:
//! it would accept a bundle that was never checked.
//!
//! The Sapling key is built differently from the Halo2 one. Halo2 hashes the bundle's own
//! consensus encoding; Sapling has no such encoder available, so it hashes the transaction bytes
//! and consensus branch id the bundle was parsed from — see [`Item::cache_key`]. These tests pin
//! that construction from the outside, over real mainnet bundles, so that they stay meaningful if
//! it is ever replaced by a bundle encoder.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::{
    future,
    task::{Context, Poll},
};

use tower::{Service, ServiceExt};
use zakura_chain::{
    amount::Amount,
    block::{Block, Height},
    parameters::{Network, NetworkUpgrade},
    serialization::ZcashDeserializeInto,
    transaction::{HashType, Transaction},
    transparent,
};

use crate::BoxError;

use super::{sapling_prover, CacheKey, Item, Memoized, MemoizedItem};

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
            if tx.sapling_spends_per_anchor().next().is_none()
                && tx.sapling_outputs().next().is_none()
            {
                continue;
            }

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

/// Returns the verification item for `tx`'s Sapling bundle under `nu`, if it has one.
fn item(tx: &Transaction, nu: NetworkUpgrade) -> Option<Item> {
    let all_previous_outputs: Arc<Vec<transparent::Output>> = Arc::new(Vec::new());
    let sighasher = tx.sighasher(nu, all_previous_outputs).ok()?;
    let (bundle, parse_digest) = sighasher.sapling_bundle_and_parse_digest()?;

    Some(Item::new(
        bundle,
        parse_digest,
        sighasher.sighash(HashType::ALL, None),
    ))
}

/// Returns the cache key of `tx`'s Sapling bundle under `nu`.
fn cache_key(tx: &Transaction, nu: NetworkUpgrade) -> CacheKey {
    item(tx, nu)
        .expect("the transaction was selected for having a Sapling bundle")
        .cache_key()
}

/// Returns one real mainnet Sapling transaction.
fn sapling_transaction() -> Transaction {
    sapling_transactions()
        .into_iter()
        .next()
        .expect("there is at least one Sapling transaction")
}

#[test]
fn cache_key_is_deterministic() {
    let tx = sapling_transaction();

    assert_eq!(
        cache_key(&tx, NetworkUpgrade::Nu5),
        cache_key(&tx, NetworkUpgrade::Nu5),
        "the same bundle, digest and sighash must always produce the same key"
    );
}

#[test]
fn cache_key_changes_with_the_sighash() {
    let tx = sapling_transaction();
    let original = item(&tx, NetworkUpgrade::Nu5).expect("the transaction has a bundle");

    let mut tweaked_sighash = original.sighash;
    tweaked_sighash.0[0] ^= 1;
    let tweaked = Item::new(
        original.bundle.clone(),
        original.parse_digest,
        tweaked_sighash,
    );

    assert_ne!(
        original.cache_key(),
        tweaked.cache_key(),
        "the sighash is an input to verification, so it must be an input to the key"
    );
}

/// Every piece of authorizing data that verification reads changes the key.
///
/// `check_bundle` batches each spend's proof and `spendAuthSig`, each output's proof, and the
/// binding signature, and `value_balance` enters the binding verification key. Each of those is a
/// field a key derived from something coarser could miss — under ZIP 244 the signatures do not
/// even change a V5 transaction's txid, which is the shape of CVE-2026-34377.
#[test]
fn cache_key_changes_with_every_piece_of_authorizing_data() {
    use zakura_chain::{
        primitives::Groth16Proof,
        sapling::{AnchorVariant, TransferData},
        serialization::AtLeastOne,
    };

    fn spend_transfers_mut<A: AnchorVariant + Clone>(
        transfers: &mut TransferData<A>,
    ) -> &mut AtLeastOne<zakura_chain::sapling::Spend<A>> {
        match transfers {
            TransferData::SpendsAndMaybeOutputs { spends, .. } => spends,
            TransferData::JustOutputs { .. } => {
                panic!("this test needs Sapling shielded data with spends")
            }
        }
    }

    let tx = sapling_transactions()
        .into_iter()
        .find(|tx| tx.sapling_spends_per_anchor().next().is_some())
        .expect("mainnet test blocks must contain a Sapling transaction with spends");

    let original = cache_key(&tx, NetworkUpgrade::Nu5);

    let mutated = |mutate: &dyn Fn(&mut Transaction)| -> CacheKey {
        let mut mutated = tx.clone();
        mutate(&mut mutated);
        cache_key(&mutated, NetworkUpgrade::Nu5)
    };

    let with_spend_field = |mutate: &dyn Fn(&mut zakura_chain::sapling::Spend<_>)| -> CacheKey {
        mutated(&|tx: &mut Transaction| {
            let Transaction::V4 {
                sapling_shielded_data: Some(shielded_data),
                ..
            } = tx
            else {
                panic!("this test fixture is a V4 transaction")
            };

            let spends = spend_transfers_mut(&mut shielded_data.transfers);
            let mut spends_vec = spends.as_slice().to_vec();
            mutate(&mut spends_vec[0]);
            *spends = AtLeastOne::from_vec(spends_vec)
                .expect("replacing a field keeps at least one spend");
        })
    };

    assert_ne!(
        original,
        with_spend_field(&|spend| spend.zkproof = Groth16Proof([0xFF; 192])),
        "a spend proof is what is being verified, so it must be an input to the key"
    );
    assert_ne!(
        original,
        with_spend_field(&|spend| spend.spend_auth_sig = [0xFF; 64].into()),
        "spend authorization signatures are batch-verified too, so they must be in the key"
    );
    assert_ne!(
        original,
        mutated(&|tx: &mut Transaction| {
            let Transaction::V4 {
                sapling_shielded_data: Some(shielded_data),
                ..
            } = tx
            else {
                panic!("this test fixture is a V4 transaction")
            };
            shielded_data.binding_sig = [0xFF; 64].into();
        }),
        "the binding signature is batch-verified alongside the proofs, so it must be in the key"
    );
    assert_ne!(
        original,
        mutated(&|tx: &mut Transaction| {
            let Transaction::V4 {
                sapling_shielded_data: Some(shielded_data),
                ..
            } = tx
            else {
                panic!("this test fixture is a V4 transaction")
            };
            shielded_data.value_balance = (shielded_data.value_balance
                + Amount::try_from(1).expect("one is a valid amount"))
            .expect("the fixture's value balance is not at the maximum");
        }),
        "value_balance enters the binding verification key, so it must be in the key"
    );
}

/// Two different Sapling bundles get different keys.
#[test]
fn cache_key_distinguishes_different_transactions() {
    let transactions = sapling_transactions();
    let keys: Vec<_> = transactions
        .iter()
        .map(|tx| cache_key(tx, NetworkUpgrade::Nu5))
        .collect();

    assert!(
        transactions.len() > 1,
        "this test needs at least two Sapling transactions in the test vectors"
    );

    let unique: std::collections::HashSet<_> = keys.iter().collect();
    assert_eq!(
        unique.len(),
        keys.len(),
        "distinct Sapling transactions must not share a cache key"
    );
}

/// The consensus branch id is part of the key.
///
/// A V4 transaction's serialization does not contain `nConsensusBranchId`, so the same bytes are
/// parsed under whichever branch id the caller supplies. [`ParseDigest`] therefore commits to it
/// explicitly rather than relying on the bytes.
///
/// [`ParseDigest`]: zakura_chain::transaction::ParseDigest
#[test]
fn cache_key_changes_with_the_consensus_branch_id() {
    let tx = sapling_transaction();

    assert!(
        tx.network_upgrade().is_none(),
        "a V4 transaction must not pin its own network upgrade, or this test proves nothing"
    );

    assert_ne!(
        cache_key(&tx, NetworkUpgrade::Nu5),
        cache_key(&tx, NetworkUpgrade::Nu6),
        "the branch id selects how the bundle is parsed, so it must be an input to the key"
    );
}

/// The key over-commits to the whole transaction, not just the Sapling bundle.
///
/// Two transactions with byte-identical Sapling bundles but different transparent sections get
/// different keys. That is a deliberate consequence of keying on the transaction bytes: it costs
/// a miss that a bundle-scoped key would have hit, and it can never cause a false hit. Pinned so
/// the behaviour is recorded as intended rather than discovered later as a surprise.
#[test]
fn cache_key_over_commits_to_the_transparent_section() {
    let tx = sapling_transaction();

    let mut with_extra_output = tx.clone();
    with_extra_output.outputs_mut().push(transparent::Output {
        value: Amount::try_from(1).expect("one is a valid amount"),
        lock_script: transparent::Script::new(&[0x51]),
    });

    let original = item(&tx, NetworkUpgrade::Nu5).expect("the transaction has a bundle");
    let modified =
        item(&with_extra_output, NetworkUpgrade::Nu5).expect("the transaction still has a bundle");

    assert_eq!(
        format!("{:?}", original.bundle),
        format!("{:?}", modified.bundle),
        "adding a transparent output must leave the Sapling bundle unchanged, or this test \
         proves nothing"
    );
    assert_ne!(
        original.cache_key(),
        modified.cache_key(),
        "keying on the transaction bytes means an unrelated transparent change also changes the \
         key"
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
async fn memo_skips_the_inner_service_for_an_already_verified_bundle() {
    let tx = sapling_transaction();
    let inner = CountingVerifier::new(true);
    let mut verifier = Memoized::new(inner.clone(), 8, "groth16_sapling_test");

    for _ in 0..3 {
        verifier
            .ready()
            .await
            .expect("the memo must become ready")
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
async fn memo_does_not_reuse_a_result_across_bundles() {
    let transactions = sapling_transactions();
    let inner = CountingVerifier::new(true);
    let mut verifier = Memoized::new(inner.clone(), 8, "groth16_sapling_test");

    for tx in transactions.iter().take(2) {
        verifier
            .ready()
            .await
            .expect("the memo must become ready")
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

/// A failure is never remembered.
///
/// A batch error is not per-item evidence — `Fallback` resolves those by re-verifying singly —
/// and an error can report that the batch worker shut down rather than that a proof is invalid.
/// Remembering either as "invalid" would make the node reject valid blocks.
#[tokio::test]
async fn memo_does_not_remember_failures() {
    let tx = sapling_transaction();
    let inner = CountingVerifier::new(false);
    let mut verifier = Memoized::new(inner.clone(), 8, "groth16_sapling_test");

    for _ in 0..3 {
        verifier
            .ready()
            .await
            .expect("the memo must become ready")
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

/// A V4 transaction and a V5 carrying the same Sapling bundle do not share a key.
///
/// Under the transaction-bytes construction this is automatic — the two serializations differ, so
/// the digests differ. It is pinned anyway, because it is the property a bundle-scoped key could
/// silently break: `zcash_primitives` has separate v4 and v5 Sapling readers, and a v5 bundle
/// encoder would not be injective over v4-origin bundles if the two versions encode anchors
/// differently.
#[test]
fn cache_key_distinguishes_v4_and_v5_carrying_the_same_bundle() {
    use zakura_chain::transaction::arbitrary;

    let network = Network::Mainnet;
    let nu5_height = NetworkUpgrade::Nu5
        .activation_height(&network)
        .expect("NU5 has an activation height on Mainnet");

    let v4 = sapling_transactions()
        .into_iter()
        .find(|tx| matches!(tx, Transaction::V4 { .. }))
        .expect("mainnet test blocks must contain a V4 Sapling transaction");

    let v5 = arbitrary::transaction_to_fake_v5(&v4, &network, nu5_height);
    assert!(
        matches!(v5, Transaction::V5 { .. }),
        "the fixture must have been converted to V5"
    );

    let v4_item = item(&v4, NetworkUpgrade::Nu5).expect("the V4 transaction has a bundle");
    let v5_item = item(&v5, NetworkUpgrade::Nu5).expect("the V5 transaction has a bundle");

    assert_eq!(
        format!("{:?}", v4_item.bundle),
        format!("{:?}", v5_item.bundle),
        "the conversion must carry the Sapling bundle across unchanged, or this test proves \
         nothing"
    );
    assert_ne!(
        v4_item.cache_key(),
        v5_item.cache_key(),
        "the same bundle in two transaction versions must not share a memo key"
    );
}

/// The production memoized verifier still rejects an invalid bundle after a valid one.
///
/// The unit tests above use a stub inner service, so they prove the memo's own behaviour but not
/// that [`VERIFIER`] is wired up with a sound key. This drives the real global service: a valid
/// mainnet bundle is verified (and therefore memoized), then the same transaction with a
/// corrupted spend proof is submitted. A key that collided between the two would return the
/// remembered `Ok` and let an unverified proof through.
///
/// The corruption keeps the proof well-formed, so it passes `check_bundle`'s synchronous checks
/// and can only be caught by the batch validation the memo would have skipped.
///
/// [`VERIFIER`]: super::VERIFIER
#[tokio::test(flavor = "multi_thread")]
async fn memoized_verifier_still_rejects_a_corrupted_proof_after_verifying_a_valid_one() {
    use zakura_chain::{sapling::TransferData, serialization::AtLeastOne};

    use crate::error::TransactionError;

    let _init_guard = zakura_test::init();

    let (nu, valid) = mined_sapling_transactions()
        .into_iter()
        .find(|(_, tx)| tx.sapling_spends_per_anchor().next().is_some())
        .expect("mainnet test blocks must contain a Sapling transaction with spends");

    let mut corrupted = valid.clone();
    let Transaction::V4 {
        sapling_shielded_data: Some(shielded_data),
        ..
    } = &mut corrupted
    else {
        panic!("this test fixture is a V4 transaction")
    };
    let TransferData::SpendsAndMaybeOutputs { spends, .. } = &mut shielded_data.transfers else {
        panic!("the fixture was selected for having spends")
    };
    let mut spends_vec = spends.as_slice().to_vec();
    // A Groth16 proof is two 48-byte compressed G1 elements around a 96-byte compressed G2
    // element. Swapping the G1 elements keeps the proof well-formed but invalid, so it fails
    // batch validation rather than the synchronous well-formedness checks.
    let (first, rest) = spends_vec[0].zkproof.0.split_at_mut(48);
    first.swap_with_slice(&mut rest[96..144]);
    *spends = AtLeastOne::from_vec(spends_vec).expect("replacing a proof keeps at least one spend");

    let valid_item = item(&valid, nu).expect("the transaction has a bundle");
    let corrupted_item = item(&corrupted, nu).expect("the transaction has a bundle");
    assert_ne!(
        valid_item.cache_key(),
        corrupted_item.cache_key(),
        "the corrupted proof must key differently, or the memo would return the remembered Ok"
    );

    super::VERIFIER
        .clone()
        .oneshot(valid_item)
        .await
        .expect("a real mainnet Sapling bundle must verify");

    let error = super::VERIFIER
        .clone()
        .oneshot(corrupted_item)
        .await
        .expect_err("a corrupted Sapling proof must be rejected even after a valid one verified");

    let error = error
        .downcast::<TransactionError>()
        .expect("the verifier reports a typed transaction error");
    assert!(
        matches!(*error, TransactionError::SaplingVerificationFailed),
        "expected SaplingVerificationFailed, got: {error:?}"
    );
}
