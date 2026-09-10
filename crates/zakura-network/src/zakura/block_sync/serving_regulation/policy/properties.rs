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
    fn legal_get_blocks_requests_roundtrip_canonically(
        start_seed in any::<u32>(),
        count in 1u32..=128,
    ) {
        let start = start_seed % (block::Height::MAX.0 - count + 2);
        let frame = BlockSyncMessage::GetBlocks {
            start_height: block::Height(start), count,
        }.encode_frame().unwrap();
        prop_assert_eq!((frame.message_type, frame.flags, frame.payload.len()), (2, 0, 9));
        let decoded = BlockSyncMessage::decode_frame(frame.clone()).unwrap();
        prop_assert!(matches!(&decoded, BlockSyncMessage::GetBlocks { start_height, count: decoded_count }
            if start_height.0 == start && *decoded_count == count), "decoded request fields changed");
        prop_assert_eq!(decoded.encode_frame().unwrap().payload, frame.payload.clone());
        let request = GetBlocksPolicy::new(&ZakuraBlockSyncConfig::default()).decode(frame).unwrap();
        prop_assert_eq!((request.start_height.0, request.count), (start, count));
    }

    #[test]
    fn get_blocks_request_owners_match_shared_model(
        node_capacity in 1usize..=2,
        choices in prop::collection::vec((any::<u8>(), any::<u8>()), 1..160),
    ) {
        crate::zakura::regulation::check_request_owners(
            GetBlocksPolicy::new(&ZakuraBlockSyncConfig::default()),
            tests::frame(block::Height(42), 1),
            node_capacity,
            &choices,
        )?;
    }

    #[test]
    fn get_blocks_admission_waiters_match_shared_model(
        node_capacity in 1usize..=3,
        choices in prop::collection::vec((any::<u8>(), any::<u8>()), 1..160),
    ) {
        crate::zakura::regulation::check_admission_waiters(
            GetBlocksPolicy::new(&ZakuraBlockSyncConfig::default()),
            tests::frame(block::Height(42), 1),
            node_capacity,
            &choices,
        )?;
    }

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
