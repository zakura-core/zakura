use proptest::prelude::*;
use std::io::Cursor;

use crate::{
    orchard::{self, shielded_data::FlagFormat},
    serialization::{ZcashDeserializeInto, ZcashSerialize},
};

proptest! {
    /// Make sure only valid flags deserialize
    #[test]
    fn flag_roundtrip_bytes(flags in any::<u8>()) {

        let mut serialized = Cursor::new(Vec::new());
        flags.zcash_serialize(&mut serialized)?;

        serialized.set_position(0);
        let maybe_deserialized = (&mut serialized).zcash_deserialize_into();

        let pre_nu6_3_allowed = (orchard::Flags::ENABLE_SPENDS | orchard::Flags::ENABLE_OUTPUTS).bits();
        let invalid_bits_mask = !pre_nu6_3_allowed;
        match orchard::Flags::from_bits(flags).filter(|_| flags & invalid_bits_mask == 0) {
            Some(valid_flags) => {
                prop_assert_eq!(maybe_deserialized.ok(), Some(valid_flags));
                prop_assert_eq!(flags & invalid_bits_mask, 0);
            }
            None => {
                prop_assert_eq!(
                    maybe_deserialized.err().unwrap().to_string(),
                    "parse error: invalid reserved orchard flags"
                );
                prop_assert_ne!(flags & invalid_bits_mask, 0);
            }
        }
    }
}

#[test]
fn nu6_3_flags_allow_cross_address_bit() {
    let bits = (orchard::Flags::ENABLE_SPENDS
        | orchard::Flags::ENABLE_OUTPUTS
        | orchard::Flags::ENABLE_CROSS_ADDRESS)
        .bits();
    let mut serialized = Cursor::new(vec![bits]);

    let flags = orchard::Flags::zcash_deserialize_with_format(&mut serialized, FlagFormat::Nu6_3)
        .expect("NU6.3 flag format allows enableCrossAddress");

    assert!(flags.contains(orchard::Flags::ENABLE_SPENDS));
    assert!(flags.contains(orchard::Flags::ENABLE_OUTPUTS));
    assert!(flags.contains(orchard::Flags::ENABLE_CROSS_ADDRESS));
}

#[test]
fn pre_nu6_3_flags_reject_cross_address_bit() {
    let mut serialized = Cursor::new(vec![orchard::Flags::ENABLE_CROSS_ADDRESS.bits()]);

    let error =
        orchard::Flags::zcash_deserialize_with_format(&mut serialized, FlagFormat::PreNu6_3)
            .expect_err("pre-NU6.3 flag format reserves enableCrossAddress");

    assert_eq!(
        error.to_string(),
        "parse error: invalid reserved orchard flags"
    );
}
