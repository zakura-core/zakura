use crate::orchard;

/// Make sure only defined Orchard flag bits are accepted.
#[test]
fn flag_roundtrip_bytes() {
    for flags in u8::MIN..=u8::MAX {
        let nu6_3_allowed = (orchard::Flags::ENABLE_SPENDS
            | orchard::Flags::ENABLE_OUTPUTS
            | orchard::Flags::ENABLE_CROSS_ADDRESS)
            .bits();
        let invalid_bits_mask = !nu6_3_allowed;

        match orchard::Flags::from_bits(flags) {
            Some(valid_flags) => {
                assert_eq!(valid_flags.bits(), flags);
                assert_eq!(flags & invalid_bits_mask, 0);
            }
            None => {
                assert_ne!(flags & invalid_bits_mask, 0);
            }
        }
    }
}
