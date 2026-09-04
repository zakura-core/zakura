//! Tests for V7 (tachyon) transaction verification.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use chrono::Utc;
use tower::{service_fn, ServiceExt};

use zakura_chain::{
    block::Height,
    parameters::{testnet::ConfiguredActivationHeights, Network, NetworkUpgrade},
    transaction::{HashType, LockTime, Transaction},
};
use zcash_tachyon::{
    action, bundle, effect,
    entropy::ActionEntropy,
    keys::private,
    note::{CommitmentTrapdoor, Note},
    nullifier, value, PointerStamp, ProofStamp, Tachygram, TachygramSetPoly, TachyonBundle,
};

use crate::{
    error::TransactionError,
    transaction::{check, Request, Response, Verifier},
};

/// A regtest network with NuTachyon (tachyon) scheduled, and NuTachyon's activation height.
fn nutachyon_network() -> (Network, Height) {
    let network = Network::new_regtest(
        ConfiguredActivationHeights {
            canopy: Some(1),
            nu5: Some(2),
            nu6: Some(3),
            nu6_1: Some(4),
            nu6_2: Some(5),
            nu6_3: Some(6),
            nu7: Some(8),
            nu_tachyon: Some(10),
            ..Default::default()
        }
        .into(),
    );
    let height = NetworkUpgrade::NuTachyon
        .activation_height(&network)
        .expect("NuTachyon activation height is configured");
    (network, height)
}

/// A V7 transaction with the given tachyon bundle and no other transfers.
fn v7_transaction(
    network_upgrade: NetworkUpgrade,
    tachyon_bundle: Option<TachyonBundle>,
) -> Transaction {
    Transaction::V7 {
        network_upgrade,
        lock_time: LockTime::min_lock_time_timestamp(),
        expiry_height: Height(0),
        inputs: Vec::new(),
        outputs: Vec::new(),
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: None,
        tachyon_shielded_data: tachyon_bundle.map(Into::into),
    }
}

/// A proof stamp with an unasserted covered-actions digest and no proof: tx-level verification
/// never checks the stamp's proof or coverage (those are block-level rules).
fn mock_proof_stamp(tachygrams: Vec<Tachygram>) -> ProofStamp {
    let tachygram_set = tachygrams.iter().copied().collect::<TachygramSetPoly>();
    ProofStamp {
        coverage: [0u8; 32],
        tachygram_set: tachygram_set.commit(),
        tachygrams: tachygrams.into_iter().collect(),
        anchor: zcash_tachyon::Anchor::read(&[0u8; 64][..]).expect("zero anchor reads"),
        proof: Box::new(ragu::Proof::trivial()),
    }
}

/// Computes the covered-actions digest used by a proof stamp.
fn action_descriptor_digest(actions: &[zcash_tachyon::Action]) -> [u8; 32] {
    let mut descriptors: Vec<[u8; 64]> = actions.iter().map(|action| action.descriptor()).collect();
    descriptors.sort_unstable();

    let mut state = blake2b_simd::Params::new()
        .hash_length(32)
        .personal(b"Tachyon-Actions")
        .to_state();
    for descriptor in descriptors {
        state.update(&descriptor);
    }
    state
        .finalize()
        .as_bytes()
        .try_into()
        .expect("hash length is 32")
}

/// A random note worth `value` zatoshis, with its spending key.
fn random_note(rng: &mut impl rand_10::CryptoRng, value: u64) -> (private::SpendingKey, Note) {
    let sk = private::SpendingKey::random(rng);
    let note = Note {
        pk: sk.derive_payment_key(),
        value: value::Positive::try_from(value).expect("value is positive and below MAX_MONEY"),
        psi: nullifier::Trapdoor::random(rng),
        rcm: CommitmentTrapdoor::random(rng),
    };
    (sk, note)
}

/// A spend-only bundle plan worth `value`, with its spend authorizing key.
///
/// A spend contributes its value positively to the transaction value pool, so the resulting
/// transaction has a valid (non-negative) miner fee.
fn spend_bundle_plan(value: u64) -> (bundle::Plan, private::SpendAuthorizingKey) {
    let mut rng = rand_10::rng();

    let (sk, note) = random_note(&mut rng, value);
    let ask = sk.derive_auth_private();

    let theta = ActionEntropy::random(&mut rng);
    let rcv = value::Trapdoor::random(&mut rng);
    let spend_plan = action::Plan::spend(note, theta, rcv, |alpha| {
        ask.derive_action_private(&alpha).derive_action_public()
    });

    (bundle::Plan::new(vec![spend_plan], vec![]), ask)
}

/// A properly-signed spend-only bundle over its own transaction's sighash, in the unproven state.
///
/// The V7 sighash commits to the bundle's effecting data (action descriptors and value balance)
/// but not its signatures or stamp, so the sighash is computed from a draft bundle carrying
/// placeholder signatures, and the returned bundle then signs over it. Attaching the returned
/// bundle (under any stamp) to [`v7_transaction`] yields the same sighash the signatures commit
/// to.
fn signed_spend_bundle(value: u64) -> zcash_tachyon::Bundle<zcash_tachyon::Unproven> {
    let mut rng = rand_10::rng();

    let (plan, ask) = spend_bundle_plan(value);

    let draft = plan
        .sign(&mut rng, &[0u8; 32], &ask)
        .expect("bundle plan has matching signatures and an in-range value balance");
    let draft_tx = v7_transaction(
        NetworkUpgrade::NuTachyon,
        Some(TachyonBundle::Proven(draft.stamp(mock_proof_stamp(vec![])))),
    );
    let sighash = v7_sighash(&draft_tx);

    plan.sign(&mut rng, &sighash, &ask)
        .expect("bundle plan has matching signatures and an in-range value balance")
}

/// The sighash the tachyon bundle's signatures must commit to.
///
/// The V7 sighash commits to the tachyon bundle's effecting data (action descriptors and value
/// balance), but not its signatures or stamp — so a bundle with placeholder signatures produces
/// the same sighash as its properly-signed counterpart under any stamp.
fn v7_sighash(tx: &Transaction) -> [u8; 32] {
    tx.sighash(
        NetworkUpgrade::NuTachyon,
        HashType::ALL,
        Arc::new(Vec::new()),
        None,
    )
    .expect("sighash is computable for a V7 transaction with no transparent inputs")
    .0
}

/// Verifies `tx` through the full transaction verifier with a block request at `height`.
async fn verify_block_transaction(
    network: &Network,
    height: Height,
    tx: Transaction,
) -> Result<Response, TransactionError> {
    // No transparent inputs anywhere in these tests, so the verifier never calls the state.
    let state = service_fn(|_| async { unreachable!("state is not queried") });
    let verifier = Verifier::new_for_tests(network, state);

    verifier
        .oneshot(Request::Block {
            transaction_hash: tx.hash(),
            transaction: Arc::new(tx),
            known_utxos: Arc::new(HashMap::new()),
            known_outpoint_hashes: Arc::new(HashSet::new()),
            height,
            time: Utc::now(),
        })
        .await
}

/// The mempool accepts only proof stamps that cover their own transaction's actions.
#[test]
fn mempool_accepts_only_autonome_tachyon_transactions() {
    let bundle = signed_spend_bundle(100);
    let mut stamp = mock_proof_stamp(vec![]);
    stamp.coverage = action_descriptor_digest(&bundle.actions);
    let autonome = bundle.stamp(stamp);

    let autonome_tx = v7_transaction(
        NetworkUpgrade::NuTachyon,
        Some(TachyonBundle::Proven(autonome.clone())),
    );
    assert_eq!(check::tachyon_bundle_is_autonome(&autonome_tx), Ok(()));

    let mut aggregate = autonome.clone();
    aggregate.stamp.coverage = [0xAA; 32];
    let aggregate_tx = v7_transaction(
        NetworkUpgrade::NuTachyon,
        Some(TachyonBundle::Proven(aggregate)),
    );
    assert_eq!(
        check::tachyon_bundle_is_autonome(&aggregate_tx),
        Err(TransactionError::NonAutonomeTachyon),
    );

    let adjunct_tx = v7_transaction(
        NetworkUpgrade::NuTachyon,
        Some(TachyonBundle::Adjunct(autonome.strip(
            PointerStamp::try_from([0xEE; 64]).expect("nonzero wtxid"),
        ))),
    );
    assert_eq!(
        check::tachyon_bundle_is_autonome(&adjunct_tx),
        Err(TransactionError::NonAutonomeTachyon),
    );
}

/// The V7 sighash commits to the tachyon bundle's effecting data, but not its stamp.
#[tokio::test(flavor = "multi_thread")]
async fn v7_sighash_commits_to_tachyon_bundle() {
    let _init_guard = zakura_test::init();

    let mut rng = rand_10::rng();
    let (plan, ask) = spend_bundle_plan(100);
    let bundle = plan
        .sign(&mut rng, &[0u8; 32], &ask)
        .expect("bundle plan has matching signatures and an in-range value balance");

    let no_bundle_sighash = v7_sighash(&v7_transaction(NetworkUpgrade::NuTachyon, None));
    let proven_sighash = v7_sighash(&v7_transaction(
        NetworkUpgrade::NuTachyon,
        Some(TachyonBundle::Proven(
            bundle.clone().stamp(mock_proof_stamp(vec![])),
        )),
    ));
    let adjunct_sighash = v7_sighash(&v7_transaction(
        NetworkUpgrade::NuTachyon,
        Some(TachyonBundle::Adjunct(
            bundle
                .stamp(mock_proof_stamp(vec![]))
                .strip(PointerStamp::try_from([0xEEu8; 64]).expect("nonzero wtxid")),
        )),
    ));
    let other_bundle_sighash = v7_sighash(&v7_transaction(
        NetworkUpgrade::NuTachyon,
        Some(TachyonBundle::Proven(
            signed_spend_bundle(100).stamp(mock_proof_stamp(vec![])),
        )),
    ));

    // The sighash changes when a bundle is present, and differs between distinct action sets.
    assert_ne!(no_bundle_sighash, proven_sighash);
    assert_ne!(proven_sighash, other_bundle_sighash);

    // The stamp is excluded, so the sighash is invariant across stamping and stripping.
    assert_eq!(proven_sighash, adjunct_sighash);
}

/// A V7 transaction with a properly-signed tachyon bundle passes verification, whether
/// proof-stamped or pointer-stamped: proof coverage is a block-level rule.
#[tokio::test(flavor = "multi_thread")]
async fn v7_with_signed_tachyon_bundle_is_accepted() {
    let _init_guard = zakura_test::init();
    let (network, height) = nutachyon_network();

    // Proof-stamped: the tx-level rules don't verify the proof itself.
    let proven = signed_spend_bundle(100).stamp(mock_proof_stamp(vec![]));
    let tx = v7_transaction(
        NetworkUpgrade::NuTachyon,
        Some(TachyonBundle::Proven(proven)),
    );
    verify_block_transaction(&network, height, tx)
        .await
        .expect("a signed proof-stamped tachyon transaction should verify");

    // Pointer-stamped: signature checks still run, proof coverage is deferred to the block.
    // The sighash excludes the stamp, so stripping a signed bundle keeps its signatures valid.
    let adjunct = signed_spend_bundle(100)
        .stamp(mock_proof_stamp(vec![]))
        .strip(PointerStamp::try_from([0xEEu8; 64]).expect("nonzero wtxid"));
    let tx = v7_transaction(
        NetworkUpgrade::NuTachyon,
        Some(TachyonBundle::Adjunct(adjunct)),
    );
    verify_block_transaction(&network, height, tx)
        .await
        .expect("a signed pointer-stamped tachyon transaction should verify");
}

/// A tachyon bundle whose signatures don't commit to the transaction sighash is rejected.
#[tokio::test(flavor = "multi_thread")]
async fn v7_with_wrong_sighash_signatures_is_rejected() {
    let _init_guard = zakura_test::init();
    let (network, height) = nutachyon_network();

    // Sign over a different message than the transaction's sighash.
    let mut rng = rand_10::rng();
    let (plan, ask) = spend_bundle_plan(100);
    let bundle = plan
        .sign(&mut rng, &[0x42u8; 32], &ask)
        .expect("bundle plan has matching signatures and an in-range value balance")
        .stamp(mock_proof_stamp(vec![]));
    let tx = v7_transaction(
        NetworkUpgrade::NuTachyon,
        Some(TachyonBundle::Proven(bundle)),
    );

    let result = verify_block_transaction(&network, height, tx).await;
    assert!(
        matches!(result, Err(TransactionError::TachyonSignatureInvalid(_))),
        "expected TachyonSignatureInvalid, got: {result:?}",
    );
}

/// A tachyon action whose value commitment is the identity point is rejected.
#[tokio::test(flavor = "multi_thread")]
async fn v7_with_identity_cv_is_rejected() {
    use halo2::pasta::group::CurveAffine;

    let _init_guard = zakura_test::init();
    let (network, height) = nutachyon_network();

    let mut rng = rand_10::rng();

    // A structurally valid action, except its cv is the identity point. The identity check runs
    // before signature verification, so the dummy signature bytes are never checked.
    let (_, note) = random_note(&mut rng, 100);
    let theta = ActionEntropy::random(&mut rng);
    let alpha = theta.randomizer::<effect::Output>(note.commitment());
    let rk = private::ActionSigningKey::new(&alpha).derive_action_public();

    let action = zcash_tachyon::Action {
        cv: value::Commitment::from(halo2::pasta::pallas::Affine::identity()),
        rk,
        sig: action::Signature::read(&[0x01u8; 64][..]).expect("64 bytes"),
    };

    let bundle = zcash_tachyon::Bundle {
        actions: vec![action],
        value_balance: value::Balance::ZERO,
        binding_sig: bundle::Signature::read(&[0x02u8; 64][..]).expect("64 bytes"),
        memo: Vec::new(),
        stamp: mock_proof_stamp(vec![]),
    };
    let tx = v7_transaction(
        NetworkUpgrade::NuTachyon,
        Some(TachyonBundle::Proven(bundle)),
    );

    let result = verify_block_transaction(&network, height, tx).await;
    assert!(
        matches!(result, Err(TransactionError::TachyonIdentityAction(_))),
        "expected TachyonIdentityAction, got: {result:?}",
    );
}

// Distinctness of the tachygrams within one proof stamp is enforced at parse time in
// `zcash_tachyon`: they deserialize into a set, and the wire parser rejects duplicate or
// unsorted entries. A duplicate-tachygram transaction is unrepresentable in Zebra, so there
// is no transaction-level semantic check (or test) for it.

/// V7 transactions are rejected before NuTachyon activation, including under NU7
/// itself (the upstream v6-transaction upgrade).
#[tokio::test(flavor = "multi_thread")]
async fn v7_is_rejected_before_nu_tachyon() {
    let _init_guard = zakura_test::init();
    let (network, _) = nutachyon_network();

    let nu7_height = NetworkUpgrade::Nu7
        .activation_height(&network)
        .expect("NU7 activation height is configured");

    // The bundle's actions let the transaction pass the has-inputs-and-outputs check, so the
    // network-upgrade gate is what rejects it. The gate errors before any signature check runs,
    // so the signatures can be arbitrary.
    let mut rng = rand_10::rng();
    let (plan, ask) = spend_bundle_plan(100);
    let bundle = plan
        .sign(&mut rng, &[0u8; 32], &ask)
        .expect("bundle plan has matching signatures and an in-range value balance")
        .stamp(mock_proof_stamp(vec![]));
    let tx = v7_transaction(
        NetworkUpgrade::NuTachyon,
        Some(TachyonBundle::Proven(bundle)),
    );

    let result = verify_block_transaction(&network, nu7_height, tx).await;
    assert!(
        matches!(result, Err(TransactionError::WrongConsensusBranchId)),
        "expected WrongConsensusBranchId, got: {result:?}",
    );
}
