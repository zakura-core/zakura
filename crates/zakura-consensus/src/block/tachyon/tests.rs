//! Tests for the block-level tachyon consensus rules.

use std::sync::Arc;

use zakura_chain::{
    amount::Amount,
    block::{Block, Height},
    parameters::NetworkUpgrade,
    serialization::ZcashDeserialize,
    transaction::{LockTime, Transaction, WtxId},
};

use zcash_tachyon::{
    action, bundle, effect,
    entropy::ActionEntropy,
    keys::private,
    note::{CommitmentTrapdoor, Note},
    nullifier, value, AggregateId, Anchor, Bundle, PointerStamp, ProofStamp, Tachygram,
    TachygramSetPoly, TachyonBundle,
};

use crate::error::BlockError;

use super::{coherence, AggregateCoverage};

/// Combines the public action and fee coverage commitments.
fn set_coverage(bundle: &mut Bundle<ProofStamp>, adjuncts: &[&Bundle<PointerStamp>]) {
    use zcash_tachyon::digest::blake2b;
    let mut actions: Vec<[u8; 64]> = bundle
        .descriptors()
        .chain(adjuncts.iter().flat_map(|bundle| bundle.descriptors()))
        .collect();
    let mut fees: Vec<[u8; 40]> = std::iter::once(bundle.fee_descriptor())
        .chain(adjuncts.iter().map(|bundle| bundle.fee_descriptor()))
        .map(Into::into)
        .collect();
    actions.sort_unstable();
    fees.sort_unstable();
    bundle.stamp.coverage = blake2b::stamp_coverage_digest(
        &blake2b::action_descriptor_digest(&actions),
        &blake2b::stamp_fees_digest(&fees),
    );
}

/// A structurally valid action with a random `rk` and dummy signature: block-level coherence
/// never checks signatures.
fn dummy_action() -> zcash_tachyon::Action {
    let mut rng = rand_10::rng();

    let sk = private::SpendingKey::random(&mut rng);
    let note = Note {
        pk: sk.derive_payment_key(),
        value: value::Positive::try_from(100u64).expect("valid value"),
        psi: nullifier::Trapdoor::random(&mut rng),
        rcm: CommitmentTrapdoor::random(&mut rng),
    };
    let alpha = ActionEntropy::random(&mut rng).randomizer::<effect::Output>(note.commitment());
    let rk = private::ActionSigningKey::new(&alpha).derive_action_public();
    let cv = value::Trapdoor::random(&mut rng)
        .commit(value::Positive::try_from(100u64).expect("valid value"));

    zcash_tachyon::Action {
        cv,
        rk,
        sig: action::Signature::read(&[0x01u8; 64][..]).expect("64 bytes"),
    }
}

/// The all-zero anchor.
fn zero_anchor() -> Anchor {
    Anchor::read(&[0u8; 32][..]).expect("zero anchor reads")
}

/// A proof-stamped bundle with the given actions, tachygrams, and covered-actions digest.
fn proven_bundle(
    actions: Vec<zcash_tachyon::Action>,
    tachygrams: Vec<Tachygram>,
) -> Bundle<ProofStamp> {
    let tachygram_set = tachygrams.iter().copied().collect::<TachygramSetPoly>();
    let mut bundle = Bundle {
        fee_max_commitment: dummy_action().cv,
        actions,
        value_balance: value::Balance::ZERO,
        binding_sig: bundle::Signature::read(&[0x02u8; 64][..]).expect("64 bytes"),
        memo: Vec::new(),
        stamp: ProofStamp {
            fee: value::Positive::ZERO,
            coverage: [0; 32],
            tachygram_set: tachygram_set.commit(),
            tachygrams: tachygrams.into_iter().collect(),
            anchor: zero_anchor(),
            proof: Box::new(ragu::Proof::bootstrap()),
        },
    };
    set_coverage(&mut bundle, &[]);
    bundle
}

/// A pointer-stamped bundle with the given actions, naming `target` as its covering aggregate.
fn adjunct_bundle(actions: Vec<zcash_tachyon::Action>, target: [u8; 64]) -> Bundle<PointerStamp> {
    Bundle {
        fee_max_commitment: dummy_action().cv,
        actions,
        value_balance: value::Balance::ZERO,
        binding_sig: bundle::Signature::read(&[0x02u8; 64][..]).expect("64 bytes"),
        memo: Vec::new(),
        stamp: PointerStamp {
            wtxid: AggregateId::try_from(target).expect("nonzero wtxid"),
            fee: value::Positive::ZERO,
        },
    }
}

/// A V7 transaction carrying only the given tachyon bundle.
fn v7_transaction(tachyon_bundle: TachyonBundle) -> Arc<Transaction> {
    Arc::new(Transaction::V7 {
        network_upgrade: NetworkUpgrade::NuTachyon,
        lock_time: LockTime::min_lock_time_timestamp(),
        expiry_height: Height(0),
        zip233_amount: Amount::zero(),
        inputs: Vec::new(),
        outputs: Vec::new(),
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: None,
        tachyon_shielded_data: Some(Box::new(tachyon_bundle.into())),
    })
}

/// A block containing the given transactions (the header contents are irrelevant to coherence).
fn block_with(transactions: Vec<Arc<Transaction>>) -> Block {
    let template = Block::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES[..])
        .expect("hardcoded test block deserializes");

    Block {
        header: template.header,
        transactions,
    }
}

fn tachygram(value: u64) -> Tachygram {
    let mut bytes = [0; 32];
    bytes[..8].copy_from_slice(&value.to_le_bytes());
    Tachygram::read(&bytes[..]).expect("a small integer is a canonical field element")
}

/// Rule 1: two proof stamps sharing a tachygram anywhere in the block are rejected.
#[test]
fn duplicate_tachygram_across_block_is_rejected() {
    let _init_guard = zakura_test::init();

    let action_a = dummy_action();
    let action_b = dummy_action();

    let block = block_with(vec![
        v7_transaction(TachyonBundle::Proven(proven_bundle(
            vec![action_a],
            vec![tachygram(1), tachygram(2)],
        ))),
        v7_transaction(TachyonBundle::Proven(proven_bundle(
            vec![action_b],
            vec![tachygram(2), tachygram(3)],
        ))),
    ]);

    assert_eq!(
        coherence(&block).err(),
        Some(BlockError::DuplicateTachygram)
    );
}

/// Rule 2: a pointer stamp naming a wtxid with no proof-stamped transaction in the block is
/// rejected.
#[test]
fn pointer_to_missing_aggregate_is_rejected() {
    let _init_guard = zakura_test::init();

    let block = block_with(vec![v7_transaction(TachyonBundle::Adjunct(
        adjunct_bundle(vec![dummy_action()], [0xEEu8; 64]),
    ))]);

    assert_eq!(
        coherence(&block).err(),
        Some(BlockError::TachyonAggregateNotFound)
    );
}

/// Rule 3: a proof stamp whose carried covered-actions digest doesn't match its actions is
/// rejected.
#[test]
fn coverage_mismatch_is_rejected() {
    let _init_guard = zakura_test::init();

    let mut bundle = proven_bundle(
        vec![dummy_action()],
        vec![tachygram(1), tachygram(2), tachygram(3)],
    );
    bundle.stamp.coverage = [0; 32];
    let block = block_with(vec![v7_transaction(TachyonBundle::Proven(bundle))]);

    assert_eq!(
        coherence(&block).err(),
        Some(BlockError::TachyonCoverageMismatch)
    );
}

/// A proof stamp whose carried Tachygram commitment does not match its published Tachygrams is
/// rejected before proof verification.
#[test]
fn tachygram_set_mismatch_is_rejected() {
    let _init_guard = zakura_test::init();

    let action = dummy_action();
    let mut bundle = proven_bundle(vec![action], vec![tachygram(1), tachygram(2), tachygram(5)]);
    bundle.stamp.tachygram_set = [tachygram(3), tachygram(4)]
        .into_iter()
        .collect::<TachygramSetPoly>()
        .commit();

    let block = block_with(vec![v7_transaction(TachyonBundle::Proven(bundle))]);

    assert_eq!(
        coherence(&block).err(),
        Some(BlockError::TachyonTachygramSetMismatch)
    );
}

/// Rule 3: the same action descriptor covered twice (in the aggregate and in an adjunct naming
/// it) is rejected before comparing coverage digests.
#[test]
fn duplicate_covered_action_is_rejected() {
    let _init_guard = zakura_test::init();

    let action = dummy_action();
    let aggregate_tx = v7_transaction(TachyonBundle::Proven(proven_bundle(
        vec![action],
        vec![tachygram(1)],
    )));

    let aggregate_wtxid = WtxId::from(aggregate_tx.as_ref()).as_bytes();
    let adjunct_tx = v7_transaction(TachyonBundle::Adjunct(adjunct_bundle(
        vec![action],
        aggregate_wtxid,
    )));

    let block = block_with(vec![aggregate_tx, adjunct_tx]);

    assert_eq!(
        coherence(&block).err(),
        Some(BlockError::TachyonDuplicateAction)
    );
}

/// Rules 2-3 positive path: an aggregate covering its own actions plus a pointing adjunct's
/// actions passes coherence, and the returned work item groups the adjunct with its aggregate.
#[test]
fn aggregate_with_adjunct_passes_coherence() {
    let _init_guard = zakura_test::init();

    let own_action = dummy_action();
    let adjunct_action = dummy_action();

    let mut adjunct = adjunct_bundle(vec![adjunct_action], [0xEE; 64]);
    // Two tachygrams per action, plus one fee-change tachygram per covered bundle.
    let mut aggregate = proven_bundle(vec![own_action], (1..=6).map(tachygram).collect());
    set_coverage(&mut aggregate, &[&adjunct]);
    aggregate
        .verify_coverage(
            &adjunct.descriptors().collect::<Vec<_>>(),
            &[adjunct.fee_descriptor()],
        )
        .unwrap();
    let aggregate_tx = v7_transaction(TachyonBundle::Proven(aggregate));
    adjunct.stamp.wtxid =
        AggregateId::try_from(WtxId::from(aggregate_tx.as_ref()).as_bytes()).unwrap();
    let adjunct_tx = v7_transaction(TachyonBundle::Adjunct(adjunct));

    let block = block_with(vec![aggregate_tx, adjunct_tx]);

    let aggregates = coherence(&block).expect("coherent block passes");
    assert_eq!(aggregates.len(), 1, "one proof stamp to verify");
    assert_eq!(
        aggregates[0].adjuncts.len(),
        1,
        "the work item groups the pointing adjunct with its aggregate",
    );
}

/// An autonome (proof-stamped bundle covering only its own actions) passes coherence by itself.
#[test]
fn autonome_passes_coherence() {
    let _init_guard = zakura_test::init();

    let action = dummy_action();

    let block = block_with(vec![v7_transaction(TachyonBundle::Proven(proven_bundle(
        vec![action],
        vec![tachygram(7), tachygram(8), tachygram(9)],
    )))]);

    let aggregates = coherence(&block).expect("coherent block passes");
    assert_eq!(aggregates.len(), 1);
    assert!(aggregates[0].adjuncts.is_empty());
}

/// Rule 4 negative path: a proof stamp with a correct covered-actions digest but an invalid
/// (bootstrap) proof fails proof verification.
#[tokio::test(flavor = "multi_thread")]
async fn bootstrap_proof_fails_verification() {
    let _init_guard = zakura_test::init();

    let action = dummy_action();
    let aggregate = AggregateCoverage {
        bundle: proven_bundle(vec![action], vec![tachygram(1), tachygram(2), tachygram(3)]),
        adjuncts: Vec::new(),
    };

    let result = crate::primitives::tachyon::verify_proof_stamp(aggregate).await;
    assert!(
        matches!(result, Err(BlockError::TachyonProofInvalid(_))),
        "expected TachyonProofInvalid, got: {result:?}",
    );
}

/// Rule 4 positive path: a genuinely proven (mock proof system) output stamp verifies.
#[tokio::test(flavor = "multi_thread")]
async fn real_mock_proof_passes_verification() {
    let _init_guard = zakura_test::init();

    let mut rng = rand_10::rng();

    let sk = private::SpendingKey::random(&mut rng);
    let note = Note {
        pk: sk.derive_payment_key(),
        value: value::Positive::try_from(100u64).expect("valid value"),
        psi: nullifier::Trapdoor::random(&mut rng),
        rcm: CommitmentTrapdoor::random(&mut rng),
    };
    let theta = ActionEntropy::random(&mut rng);
    let rcv = value::Trapdoor::random(&mut rng);
    let output_plan = action::Plan::output(note, theta, rcv);

    let plan = bundle::Plan::new(
        vec![],
        vec![output_plan],
        Note {
            pk: sk.derive_payment_key(),
            value: value::Positive::ZERO,
            psi: nullifier::Trapdoor::random(&mut rng),
            rcm: CommitmentTrapdoor::random(&mut rng),
        },
    );
    let pak = sk.derive_proof_private();
    let ask = sk.derive_auth_private();

    let stamp = plan
        .stamp_plan(zero_anchor(), value::Positive::ZERO)
        .prove(&mut rng, &pak, vec![])
        .expect("output-only stamp plan proves under the mock proof system");

    // Proof verification never checks signatures, so the sighash is arbitrary here.
    let bundle = plan
        .sign(&mut rng, &[0u8; 32], &ask)
        .expect("bundle plan signs")
        .stamp(stamp);

    let aggregate = AggregateCoverage {
        bundle,
        adjuncts: Vec::new(),
    };

    crate::primitives::tachyon::verify_proof_stamp(aggregate)
        .await
        .expect("a genuinely proven stamp verifies");
}

#[test]
fn duplicate_covered_fee_is_rejected() {
    let _guard = zakura_test::init();
    let mut aggregate = proven_bundle(vec![dummy_action()], (1..=6).map(tachygram).collect());
    let mut adjunct = adjunct_bundle(vec![dummy_action()], [0xEE; 64]);
    adjunct.fee_max_commitment = aggregate.fee_max_commitment;
    set_coverage(&mut aggregate, &[&adjunct]);
    let tx = v7_transaction(TachyonBundle::Proven(aggregate));
    adjunct.stamp.wtxid = AggregateId::try_from(WtxId::from(tx.as_ref()).as_bytes()).unwrap();
    let block = block_with(vec![tx, v7_transaction(TachyonBundle::Adjunct(adjunct))]);
    assert_eq!(
        coherence(&block).err(),
        Some(BlockError::TachyonDuplicateFee)
    );
}

#[test]
fn stamp_requires_a_fee_change_tachygram_even_for_zero_fee() {
    let _guard = zakura_test::init();
    let bundle = proven_bundle(vec![dummy_action()], vec![tachygram(1), tachygram(2)]);
    let block = block_with(vec![v7_transaction(TachyonBundle::Proven(bundle))]);
    assert_eq!(
        coherence(&block).err(),
        Some(BlockError::TachyonTachygramArityMismatch)
    );
}
