//! The batched Sapling point check agrees with the per-point checks.

use group::GroupEncoding;
use sha2::{Digest, Sha256};

use crate::sapling::{
    all_valid_not_small_order,
    keys::{EphemeralPublicKey, ValidatingKey},
    ValueCommitment,
};

/// Valid points, small-order points, off-curve values and non-canonical encodings.
fn encodings() -> Vec<[u8; 32]> {
    let mut encodings = vec![
        jubjub::AffinePoint::from(jubjub::ExtendedPoint::identity()).to_bytes(),
        [0xff; 32],
        [0; 32],
    ];
    let minus_one = -jubjub::Fr::one();
    for seed in 0u32..96 {
        let candidate: [u8; 32] = Sha256::digest(seed.to_le_bytes()).into();
        encodings.push(candidate);
        if let Some(point) =
            Option::<jubjub::ExtendedPoint>::from(jubjub::ExtendedPoint::from_bytes(&candidate))
        {
            // A small-order point, and a valid point with a small-order component.
            encodings.push((point * minus_one + point).to_bytes());
            encodings.push(point.mul_by_cofactor().to_bytes());
        }
    }
    let mut non_canonical = (-jubjub::Fq::one()).to_bytes();
    non_canonical[0] += 1;
    encodings.push(non_canonical);
    encodings
}

fn each_valid(encodings: &[[u8; 32]]) -> bool {
    encodings.iter().all(|&bytes| {
        let cv = ValueCommitment(bytes).is_valid_not_small_order();
        assert_eq!(
            cv,
            EphemeralPublicKey(bytes).is_valid_not_small_order(),
            "cv and epk use the same predicate"
        );
        assert_eq!(
            cv,
            ValidatingKey::from_bytes_unvalidated(bytes).is_valid_not_small_order(),
            "cv and rk use the same predicate"
        );
        cv
    })
}

#[test]
fn batched_point_check_matches_each_point_check() {
    let _init_guard = zakura_test::init();

    let encodings = encodings();
    let valid: Vec<_> = encodings
        .iter()
        .copied()
        .filter(|&bytes| each_valid(&[bytes]))
        .collect();
    assert!(valid.len() > 10 && valid.len() < encodings.len());

    // Every single encoding, every batch width, and all-valid batches.
    for width in [1, 2, 3, 7, 8, 9, 31] {
        for batch in encodings.chunks(width).chain(valid.chunks(width)) {
            assert_eq!(
                all_valid_not_small_order(batch.iter().copied()),
                each_valid(batch),
                "batch {batch:02x?}"
            );
        }
    }
    assert!(all_valid_not_small_order(std::iter::empty()));
}
