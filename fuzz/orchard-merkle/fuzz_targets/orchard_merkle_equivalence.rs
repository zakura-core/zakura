#![no_main]

use libfuzzer_sys::fuzz_target;
use zakura_chain::orchard::tree::fuzz_merkle_crh_orchard_equivalence;

const INPUT_BYTES: usize = 65;
const FIELD_BYTES: usize = 32;
const LIMB_BYTES: usize = std::mem::size_of::<u64>();

fn limbs(bytes: &[u8]) -> [u64; 4] {
    std::array::from_fn(|index| {
        let start = index * LIMB_BYTES;
        u64::from_le_bytes(
            bytes[start..start + LIMB_BYTES]
                .try_into()
                .expect("each field limb contains exactly eight bytes"),
        )
    })
}

fuzz_target!(|bytes: &[u8]| {
    if bytes.len() < INPUT_BYTES {
        return;
    }

    let layer = bytes[0] % 32;
    let left = limbs(&bytes[1..1 + FIELD_BYTES]);
    let right = limbs(&bytes[1 + FIELD_BYTES..INPUT_BYTES]);

    fuzz_merkle_crh_orchard_equivalence(layer, left, right);
});
