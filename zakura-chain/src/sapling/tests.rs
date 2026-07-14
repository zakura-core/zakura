#![allow(clippy::unwrap_in_result)]

use group::Group;
use hex::{FromHex, ToHex};

use crate::serialization::ZcashDeserialize;

mod preallocate;
mod prop;

#[test]
fn transmission_key_rejects_off_curve_bytes() {
    let _init_guard = zakura_test::init();

    assert!(
        super::keys::TransmissionKey::try_from([0xffu8; 32]).is_err(),
        "off-curve Sapling transmission keys must return an error rather than panic",
    );
}

#[test]
fn value_commitment_hex_parse_stays_strict_while_wire_decode_is_lazy() {
    let _init_guard = zakura_test::init();

    let invalid_bytes = [0xff; 32];
    let parsed_wire = super::ValueCommitment::zcash_deserialize(invalid_bytes.as_slice())
        .expect("wire parsing intentionally preserves raw Sapling commitment bytes");

    assert!(
        !parsed_wire.is_valid_not_small_order(),
        "the chosen bytes must remain invalid under the deferred semantic predicate",
    );
    assert!(
        super::ValueCommitment::from_hex(hex::encode(invalid_bytes)).is_err(),
        "the display-oriented hex constructor must not silently create an invalid commitment",
    );

    let valid = super::ValueCommitment::from(jubjub::ExtendedPoint::generator());
    let display_hex = (&valid).encode_hex::<String>();
    assert_eq!(
        super::ValueCommitment::from_hex(display_hex).expect("valid commitment hex should parse"),
        valid,
    );
}

#[test]
#[should_panic(expected = "ValueCommitment::from requires a canonical non-small-order point")]
fn value_commitment_test_helper_rejects_small_order_points() {
    let _ = super::ValueCommitment::from(jubjub::ExtendedPoint::identity());
}
