//! Sinsemilla hash functions and helpers.
//!
//! These rebuild their domains on every call, so only the test vectors use them.
//! The production `MerkleCRH^Orchard` path in [`crate::orchard::tree`] hashes
//! through a cached domain instead.

use halo2::{arithmetic::CurveExt, pasta::pallas};
use sinsemilla::HashDomain;

/// GroupHash into Pallas, aka _GroupHash^P_
///
/// Produces a random point in the Pallas curve.  The first input element acts
/// as a domain separator to distinguish uses of the group hash for different
/// purposes; the second input element is the message.
///
/// <https://zips.z.cash/protocol/nu5.pdf#concretegrouphashpallasandvesta>
#[allow(non_snake_case)]
pub fn pallas_group_hash(D: &[u8], M: &[u8]) -> pallas::Point {
    let domain_separator = std::str::from_utf8(D).unwrap();

    pallas::Point::hash_to_curve(domain_separator)(M)
}

/// Sinsemilla Hash Function
///
/// "SinsemillaHash is an algebraic hash function with collision resistance (for
/// fixed input length) derived from assumed hardness of the Discrete Logarithm
/// Problem. It is designed by Sean Bowe and Daira Hopwood. The motivation for
/// introducing a new discrete-log-based hash function (rather than using
/// PedersenHash) is to make efficient use of the lookups available in recent
/// proof systems including Halo 2."
///
/// SinsemillaHash: B^Y^\[N\] × B[{0 .. 𝑘·𝑐}] → P_𝑥 ∪ {⊥}
///
/// <https://zips.z.cash/protocol/nu5.pdf#concretesinsemillahash>
#[allow(non_snake_case)]
pub fn sinsemilla_hash(D: &[u8], M: &[bool]) -> Option<pallas::Base> {
    let domain = std::str::from_utf8(D).expect("must be valid UTF-8");
    let hash_domain = HashDomain::new(domain);

    hash_domain.hash(M.iter().copied()).into()
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::orchard::tests::vectors;

    // Checks Sinsemilla hashes to point and to bytes (aka the x-coordinate
    // bytes of a point) with:
    // - One of two domains.
    // - Random message lengths between 0 and 255 bytes.
    // - Random message bits.
    #[test]
    #[allow(non_snake_case)]
    fn sinsemilla_hackworks_test_vectors() {
        use halo2::pasta::group::ff::PrimeField;

        for tv in tests::vectors::SINSEMILLA.iter() {
            let D = tv.domain.as_slice();
            assert_eq!(
                sinsemilla_hash(D, &tv.msg).expect("should not fail per Theorem 5.4.4"),
                pallas::Base::from_repr(tv.hash).unwrap()
            )
        }
    }

    // Checks Pallas group hashes with:
    // - One of two domains.
    // - Random message lengths between 0 and 255 bytes.
    // - Random message contents.
    #[test]
    #[allow(non_snake_case)]
    fn sinsemilla_hackworks_group_hash_test_vectors() {
        use halo2::pasta::group::GroupEncoding;

        for tv in tests::vectors::GROUP_HASHES.iter() {
            let D = tv.domain.as_slice();
            let M = tv.msg.as_slice();

            assert_eq!(
                pallas_group_hash(D, M),
                pallas::Point::from_bytes(&tv.point).unwrap()
            );
        }
    }
}
