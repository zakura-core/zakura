//! Independent generated GetBlocks wire acceptance rules.

use super::*;
use proptest::prelude::*;

/// Include well-shaped requests so generated cases exercise acceptance too.
/// Build the bytes directly: the production encoder would reject bad fields.
fn request_payloads() -> impl Strategy<Value = Vec<u8>> {
    let fields = (
        prop_oneof![3 => Just(2u8), 1 => any::<u8>()],
        prop_oneof![0u32..=block::Height::MAX.0, any::<u32>()],
        prop_oneof![3 => 1u32..=128, 1 => any::<u32>()],
    )
        .prop_map(|(tag, start, count)| {
            let mut payload = vec![tag];
            payload.extend_from_slice(&start.to_le_bytes());
            payload.extend_from_slice(&count.to_le_bytes());
            payload
        });
    prop_oneof![fields, prop::collection::vec(any::<u8>(), 0..=16)]
}

proptest! {
    #[test]
    fn get_blocks_decode_accepts_only_exact_valid_requests(
        payload in request_payloads(),
        message_type in prop_oneof![3 => Just(2u16), 1 => any::<u16>()],
        flags in prop_oneof![3 => Just(0u16), 1 => any::<u16>()],
    ) {
        // Independent wire rules: exactly nine bytes, matching GetBlocks
        // tags, no flags, and a nonempty range within supported heights.
        // Wider arithmetic also catches ranges that overflow u32.
        let expected = match payload.as_slice() {
            [2, a, b, c, d, e, f, g, h] if message_type == 2 && flags == 0 => {
                let start = u32::from_le_bytes([*a, *b, *c, *d]);
                let count = u32::from_le_bytes([*e, *f, *g, *h]);
                ((1..=128).contains(&count)
                    && u64::from(start) + u64::from(count)
                        <= u64::from(block::Height::MAX.0) + 1)
                    .then_some((start, count))
            }
            _ => None,
        };
        let policy = GetBlocksPolicy::new(&ZakuraBlockSyncConfig::default());
        // Proptest reports and shrinks any panic from the production decoder.
        let actual = policy.decode(Frame { message_type, flags, payload });
        prop_assert_eq!(
            actual.ok().map(|request| (request.start_height.0, request.count)),
            expected,
        );
    }
}
