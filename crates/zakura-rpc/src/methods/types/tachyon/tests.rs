//! Exercise the public feed as a proof-service consumer, including epoch crossings.

use super::*;
use std::sync::Arc;
use zakura_chain::{
    amount::Amount,
    block::{Block, Height},
    parameters::NetworkUpgrade,
    serialization::ZcashDeserializeInto,
    transaction::{LockTime, TachyonShieldedData, Transaction},
};
use zcash_tachyon::{
    bundle::Signature,
    nullifier::Nullifier,
    stamp::proof::{pool, qr, PROOF_SYSTEM},
    witness, Anchor, Bundle, ProofStamp, QrDiscriminant, Tachygram, TachygramSetPoly,
};

fn gram(value: u8) -> Tachygram {
    let mut bytes = [0; 32];
    bytes[0] = value;
    Tachygram::read(&bytes[..]).unwrap()
}

fn transaction(bundle: TachyonBundle) -> Arc<Transaction> {
    Arc::new(Transaction::V7 {
        network_upgrade: NetworkUpgrade::NuTachyon,
        lock_time: LockTime::unlocked(),
        expiry_height: Height(0),
        zip233_amount: Amount::zero(),
        inputs: vec![],
        outputs: vec![],
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: None,
        tachyon_shielded_data: Some(TachyonShieldedData(bundle)),
    })
}

fn proven(members: &[Tachygram]) -> TachyonBundle {
    // Signatures and actions are irrelevant to this already-validated state projection.
    TachyonBundle::Proven(Bundle {
        value_balance: 0i64.try_into().unwrap(),
        actions: vec![],
        binding_sig: Signature::read(&[0; 64][..]).unwrap(),
        memo: vec![],
        stamp: ProofStamp {
            coverage: [0; 32],
            anchor: Anchor::default(),
            tachygram_set: members
                .iter()
                .copied()
                .collect::<TachygramSetPoly>()
                .commit(),
            tachygrams: members.iter().copied().collect(),
            proof: Box::new(ragu::Proof::trivial()),
        },
    })
}

fn data(
    pool_height: u32,
    before: Anchor,
    bundles: Vec<TachyonBundle>,
) -> zakura_state::TachyonBlock {
    let mut block: Block = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
        .zcash_deserialize_into()
        .unwrap();
    block
        .transactions
        .extend(bundles.into_iter().map(transaction));
    let after = tachyon::Anchor::from(before)
        .advance_with_block(pool_height, &block)
        .unwrap();
    zakura_state::TachyonBlock {
        block: Arc::new(block),
        height: Height(10 + pool_height),
        activation_height: Height(10),
        finalized: true,
        anchor_before: before.into(),
        anchor_after: after.post_block,
    }
}

fn wire(data: zakura_state::TachyonBlock) -> GetTachyonBlockResponse {
    let response = GetTachyonBlockResponse::from_state(data).unwrap();
    serde_json::from_str(&serde_json::to_string(&response).unwrap()).unwrap()
}

#[test]
fn tachyon_sync_feed_preserves_stamp_order_and_empty_epoch_crossings() {
    let before = Anchor::default();
    let pointer = TachyonBundle::Adjunct(Bundle {
        value_balance: 0i64.try_into().unwrap(),
        actions: vec![],
        binding_sig: Signature::read(&[0; 64][..]).unwrap(),
        memo: vec![],
        stamp: zcash_tachyon::PointerStamp::try_from([1; 64]).unwrap(),
    });
    let reply = wire(data(
        0,
        before,
        vec![proven(&[gram(1), gram(2)]), pointer, proven(&[gram(3)])],
    ));
    assert_eq!(
        reply
            .stamps
            .iter()
            .map(|stamp| stamp.transaction_index)
            .collect::<Vec<_>>(),
        [1, 3]
    );
    assert_eq!(
        reply.epoch_start_anchor,
        Some(hex::encode(tachyon::Anchor::from(before).0))
    );
    let mut anchor = before;
    for stamp in &reply.stamps {
        let grams: Vec<_> = stamp
            .tachygrams
            .iter()
            .map(|gram| Tachygram::read(&hex::decode(gram).unwrap()[..]).unwrap())
            .collect();
        let commitment = grams.into_iter().collect::<TachygramSetPoly>().commit();
        let mut bytes = [0; 32];
        commitment.write(&mut bytes[..]).unwrap();
        assert_eq!(bytes, stamp.tachygram_set);
        anchor = anchor.next_stamp(EpochIndex::new(0), &commitment).unwrap();
    }
    assert_eq!(tachyon::Anchor::from(anchor).0, reply.anchor_after);

    let empty = wire(data(1, anchor, vec![]));
    assert!(empty.stamps.is_empty());
    assert_eq!(empty.anchor_before, empty.anchor_after);
    assert_eq!(empty.epoch_start_anchor, None);
    let crossing = wire(data(tachyon::EPOCH_LENGTH, anchor, vec![]));
    assert!(crossing.stamps.is_empty());
    assert_eq!(crossing.epoch, 1);
    assert_ne!(crossing.anchor_before, crossing.anchor_after);
    assert_eq!(
        crossing.epoch_start_anchor,
        Some(hex::encode(crossing.anchor_after))
    );

    let mut inconsistent = data(1, anchor, vec![]);
    inconsistent.anchor_after = before.into();
    assert!(GetTachyonBlockResponse::from_state(inconsistent)
        .unwrap_err()
        .contains("stored anchor"));
}

#[test]
fn tachyon_sync_feed_builds_and_composes_unspent_proofs() {
    let rng = &mut rand_10::rng();
    let zero = Anchor::read(&[0; 32][..]).unwrap();
    let entry = Anchor::default();
    let first = wire(data(0, entry, vec![proven(&[gram(1), gram(2)])]));
    let final0 = Anchor::read(&first.anchor_after[..]).unwrap();
    // Epoch one contains no stamps, but still contributes a whole-epoch absence proof.
    let second = wire(data(tachyon::EPOCH_LENGTH, final0, vec![]));
    let final1 = Anchor::read(&second.anchor_after[..]).unwrap();
    let third = wire(data(2 * tachyon::EPOCH_LENGTH, final1, vec![]));
    let replies = [&first, &second];
    let final_prevs = [zero, final0];
    let nullifiers = [gram(90), gram(91)];
    let mut proofs = Vec::new();
    for ((reply, final_prev), nullifier) in replies.into_iter().zip(final_prevs).zip(nullifiers) {
        let entry =
            Anchor::read(&hex::decode(reply.epoch_start_anchor.as_ref().unwrap()).unwrap()[..])
                .unwrap();
        let epoch = EpochIndex::new(reply.epoch);
        let discriminant = QrDiscriminant(gram(100).into());
        let members: Vec<_> = reply
            .stamps
            .iter()
            .flat_map(|stamp| &stamp.tachygrams)
            .map(|gram| Tachygram::read(&hex::decode(gram).unwrap()[..]).unwrap())
            .collect();
        let (intake, ()) = if members.is_empty() {
            PROOF_SYSTEM
                .seed(
                    rng,
                    qr::QrEmptyIntakeSeed,
                    witness::qr_empty_intake_seed(((), ()), entry, epoch, discriminant),
                )
                .unwrap()
        } else {
            PROOF_SYSTEM
                .seed(
                    rng,
                    qr::QrStampIntakeSeed,
                    witness::qr_stamp_intake_seed(((), ()), entry, epoch, discriminant, &members),
                )
                .unwrap()
        };
        let (bucket, ()) = PROOF_SYSTEM
            .fuse(
                rng,
                qr::QrBucketSeal,
                witness::qr_bucket_seal((*intake.data(), ()), final_prev),
                intake,
                ragu::Proof::trivial().carry::<()>(()),
            )
            .unwrap();
        let (unspent, ()) = PROOF_SYSTEM
            .fuse(
                rng,
                qr::QrUnspentInit,
                witness::qr_unspent_init((*bucket.data(), ()), nullifier, &members),
                bucket,
                ragu::Proof::trivial().carry::<()>(()),
            )
            .unwrap();
        proofs.push(unspent);
    }
    let right = proofs.pop().unwrap();
    let left = proofs.pop().unwrap();
    let (composed, ()) = PROOF_SYSTEM
        .fuse(
            rng,
            pool::UnspentFuse,
            witness::unspent_fuse(
                (*left.data(), *right.data()),
                &[Nullifier::from(nullifiers[0])],
                &[Nullifier::from(nullifiers[1])],
            ),
            left,
            right,
        )
        .unwrap();
    let (anchor_start, epoch_start, _, epoch_next, anchor_next) = *composed.data();
    assert_eq!(anchor_start, entry);
    assert_eq!(epoch_start, EpochIndex::new(0));
    assert_eq!(epoch_next, EpochIndex::new(2));
    assert_eq!(tachyon::Anchor::from(anchor_next).0, third.anchor_after);
}
