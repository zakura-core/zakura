//! Batch decompression of Orchard action points.

use group::{ff::PrimeField, Group, GroupEncoding};
use halo2::pasta::pallas;
use sha2::{Digest, Sha256};

use crate::{
    orchard::{keys::EphemeralPublicKey, Action, ActionEncoding, ValueCommitment},
    serialization::{ZcashDeserialize, ZcashSerialize},
};

/// Point encodings covering both verdicts: valid points, the identity (a valid
/// `cv` but not a valid `epk`), off-curve values, and non-canonical encodings.
fn point_encodings() -> Vec<[u8; 32]> {
    let mut encodings = vec![
        pallas::Point::identity().to_bytes(),
        pallas::Point::generator().to_bytes(),
        [0xff; 32],
        [0; 32],
    ];
    // `x = p` is the non-canonical encoding of `x = 0`.
    let mut non_canonical = (-pallas::Base::one()).to_repr();
    non_canonical[0] += 1;
    encodings.push(non_canonical);

    let mut point = pallas::Point::generator();
    for seed in 0u32..64 {
        encodings.push(Sha256::digest(seed.to_le_bytes()).into());
        encodings.push(point.to_bytes());
        point = point.double() + pallas::Point::generator();
    }
    encodings
}

/// The action bytes for `template` with its `cv` and `epk` replaced.
fn action_bytes(template: &[u8], cv: [u8; 32], epk: [u8; 32]) -> Vec<u8> {
    let mut bytes = template.to_vec();
    bytes[..32].copy_from_slice(&cv);
    // `epk` follows `cv`, `nullifier`, `rk` and `cmx`.
    bytes[128..160].copy_from_slice(&epk);
    bytes
}

#[test]
fn batch_decompression_matches_decoding_each_action() {
    let _init_guard = zakura_test::init();

    let template = Action {
        cv: ValueCommitment::try_from(pallas::Point::generator().to_bytes())
            .expect("the generator is a valid value commitment"),
        nullifier: [1u8; 32]
            .try_into()
            .expect("small values are valid nullifiers"),
        rk: [2u8; 32].into(),
        cm_x: pallas::Base::from(3),
        ephemeral_key: EphemeralPublicKey::try_from(pallas::Point::generator().to_bytes())
            .expect("the generator is a valid ephemeral key"),
        enc_ciphertext: [4u8; 580].into(),
        out_ciphertext: [5u8; 80].into(),
    }
    .zcash_serialize_to_vec()
    .expect("vec writes succeed");

    let encodings = point_encodings();
    let pairs: Vec<([u8; 32], [u8; 32])> = encodings
        .iter()
        .flat_map(|&cv| encodings.iter().step_by(7).map(move |&epk| (cv, epk)))
        .collect();

    let mut accepted = 0;
    let mut rejected = 0;
    // Batches of these sizes reach every chunk width `batch_from_bytes` uses.
    for batch_size in [1, 2, 3, 4, 5, 8, 9, 17] {
        for batch in pairs.chunks(batch_size) {
            let one_at_a_time: Result<Vec<Action>, _> = batch
                .iter()
                .map(|&(cv, epk)| {
                    Action::zcash_deserialize(action_bytes(&template, cv, epk).as_slice())
                })
                .collect();

            let read: Vec<ActionEncoding> = batch
                .iter()
                .map(|&(cv, epk)| {
                    ActionEncoding::zcash_deserialize(action_bytes(&template, cv, epk).as_slice())
                        .expect("compressed points are not checked while reading")
                })
                .collect();
            let batched = ActionEncoding::decompress(read);

            match (&one_at_a_time, &batched) {
                (Ok(expected), Ok(actual)) => {
                    assert_eq!(expected, actual);
                    accepted += 1;
                }
                (Err(_), Err(_)) => rejected += 1,
                _ => panic!(
                    "batch of {batch_size} decoded differently: one at a time {:?}, batched {:?}",
                    one_at_a_time.is_ok(),
                    batched.is_ok(),
                ),
            }

            for &(cv, epk) in batch {
                assert_eq!(
                    ValueCommitment::try_from(cv).is_ok()
                        && EphemeralPublicKey::try_from(epk).is_ok(),
                    Action::zcash_deserialize(action_bytes(&template, cv, epk).as_slice()).is_ok(),
                );
            }
        }
    }
    assert!(accepted > 0 && rejected > 0, "both verdicts are covered");
}
