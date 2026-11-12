//! Sapling spend authorization key (`rk`) validation vectors.

use group::GroupEncoding;
use redjubjub::SpendAuth;
use sha2::{Digest, Sha256};

use crate::sapling::keys::ValidatingKey;

/// The `rk` check as it was before the decoded point was reused: decode the
/// key, then decompress the same bytes again for the small-order check.
fn decompress_twice_accepts(bytes: [u8; 32]) -> bool {
    redjubjub::VerificationKey::<SpendAuth>::try_from(bytes).is_ok()
        && !bool::from(
            jubjub::AffinePoint::from_bytes(bytes)
                .expect("redjubjub accepted these bytes, so they decode")
                .is_small_order(),
        )
}

/// Encodings covering every way `rk` can be accepted or rejected: prime-order
/// points, all eight small-order points, points with a small-order component,
/// off-curve values and non-canonical `y` coordinates.
fn rk_encodings() -> Vec<[u8; 32]> {
    let mut encodings = vec![[0xff; 32], [0; 32]];
    // The prime subgroup order times a point leaves only its small-order part:
    // `[r]P = [r - 1]P + P`, and `r - 1` is `-1` in the scalar field.
    let minus_one = -jubjub::Fr::one();

    for seed in 0u32..512 {
        let candidate: [u8; 32] = Sha256::digest(seed.to_le_bytes()).into();
        encodings.push(candidate);

        let Some(point) =
            Option::<jubjub::ExtendedPoint>::from(jubjub::ExtendedPoint::from_bytes(&candidate))
        else {
            continue;
        };
        let small_order_part = point * minus_one + point;
        let prime_order_part = point.mul_by_cofactor();

        for derived in [
            small_order_part,
            prime_order_part,
            prime_order_part + small_order_part,
        ] {
            encodings.push(derived.to_bytes());
        }
    }

    // `y = q` is the non-canonical encoding of `y = 0`. `q - 1` ends in a zero
    // byte, so adding one to its lowest byte cannot carry.
    let mut non_canonical = (-jubjub::Fq::one()).to_bytes();
    non_canonical[0] += 1;
    encodings.push(non_canonical);

    encodings
}

#[test]
fn rk_validation_matches_decompress_twice() {
    let _init_guard = zakura_test::init();

    let encodings = rk_encodings();
    let small_order: std::collections::HashSet<[u8; 32]> = encodings
        .iter()
        .copied()
        .filter(|bytes| {
            Option::<jubjub::AffinePoint>::from(jubjub::AffinePoint::from_bytes(*bytes))
                .is_some_and(|point| bool::from(point.is_small_order()))
        })
        .collect();
    assert_eq!(
        small_order.len(),
        8,
        "the vectors cover all eight small-order Jubjub points",
    );

    let mut accepted = 0;
    for bytes in encodings {
        let expected = decompress_twice_accepts(bytes);
        assert_eq!(
            ValidatingKey::try_from(bytes).is_ok(),
            expected,
            "rk {} acceptance must not change",
            hex::encode(bytes),
        );
        accepted += usize::from(expected);
    }
    assert!(accepted > 0, "the vectors include accepted keys");
}
