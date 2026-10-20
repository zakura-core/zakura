//! Check that `inv`, `addr`, and `addrv2` payloads decode the same from a slice
//! as from a stream, and that slices reject counts the input cannot hold.

use std::fmt::Debug;

use zakura_chain::{
    block,
    serialization::{
        CompactSizeMessage, SerializationError, TrustedPreallocate, ZcashDeserialize,
        ZcashSerialize,
    },
    transaction,
};

use crate::protocol::external::{
    addr::{AddrV1, AddrV2},
    types::PeerServices,
    AddrInVersion, InventoryHash,
};

/// A minimal `addrv2` entry: an unknown network ID with an empty address.
const UNSUPPORTED_ADDR_V2: [u8; 9] = [0x29, 0xab, 0x5f, 0x49, 0, 0xff, 0, 0, 0];

/// Decode `payload` from a stream and from a slice, check both agree, and check
/// the slice decoder consumes every byte.
fn decode_both_ways<T: ZcashDeserialize + Debug + PartialEq>(payload: &[u8]) -> T {
    let streamed = T::zcash_deserialize(payload).unwrap();
    let mut bytes = payload;
    let sliced = T::zcash_deserialize_from_slice(&mut bytes).unwrap();
    assert_eq!(sliced, streamed);
    assert!(bytes.is_empty(), "{} bytes left unread", bytes.len());
    sliced
}

/// Declare the largest allowed count of `T` but supply only `entry`, and check
/// the slice decoder rejects the count without allocating.
fn assert_rejects_oversized_count<T: ZcashDeserialize + TrustedPreallocate + Debug>(entry: &[u8]) {
    let count = usize::try_from(T::max_allocation()).unwrap();
    let mut payload = CompactSizeMessage::try_from(count)
        .unwrap()
        .zcash_serialize_to_vec()
        .unwrap();
    payload.extend_from_slice(entry);
    let mut bytes = payload.as_slice();

    let (result, allocations) =
        zakura_test::allocations::measure(|| Vec::<T>::zcash_deserialize_from_slice(&mut bytes));

    assert!(
        matches!(
            result,
            Err(SerializationError::Parse("Vector exceeds available input"))
        ),
        "{result:?}"
    );
    assert_eq!(allocations.peak_live_bytes, 0, "{allocations:?}");
}

#[test]
fn inv_decodes_the_same_from_a_slice() {
    let _init_guard = zakura_test::init();

    let inv = vec![
        InventoryHash::Error,
        InventoryHash::Tx(transaction::Hash([1; 32])),
        InventoryHash::Block(block::Hash([2; 32])),
        InventoryHash::FilteredBlock(block::Hash([3; 32])),
        InventoryHash::Wtx([4; 64].into()),
    ];
    let payload = inv.zcash_serialize_to_vec().unwrap();
    assert_eq!(decode_both_ways::<Vec<InventoryHash>>(&payload), inv);

    // Entries of exactly the minimum size fill the input with no spare bytes.
    let smallest = vec![InventoryHash::Block(block::Hash([5; 32])); 3];
    let payload = smallest.zcash_serialize_to_vec().unwrap();
    assert_eq!(decode_both_ways::<Vec<InventoryHash>>(&payload), smallest);
}

#[test]
fn addr_v1_decodes_the_same_from_a_slice() {
    let _init_guard = zakura_test::init();

    for payload in zakura_test::network_addr::ADDR_V1_IP_VECTORS
        .iter()
        .chain(zakura_test::network_addr::ADDR_V1_EMPTY_VECTORS.iter())
    {
        decode_both_ways::<Vec<AddrV1>>(payload);
    }
}

#[test]
fn addr_v2_decodes_the_same_from_a_slice() {
    let _init_guard = zakura_test::init();

    for payload in zakura_test::network_addr::ADDR_V2_IP_VECTORS
        .iter()
        .chain(zakura_test::network_addr::ADDR_V2_EMPTY_VECTORS.iter())
    {
        decode_both_ways::<Vec<AddrV2>>(payload);
    }

    // Entries of exactly the minimum size fill the input with no spare bytes.
    let mut payload = vec![2];
    payload.extend(UNSUPPORTED_ADDR_V2.repeat(2));
    assert_eq!(
        decode_both_ways::<Vec<AddrV2>>(&payload),
        [AddrV2::Unsupported; 2]
    );

    for payload in zakura_test::network_addr::ADDR_V2_INVALID_VECTORS.iter() {
        assert!(Vec::<AddrV2>::zcash_deserialize(payload.as_slice()).is_err());
        assert!(Vec::<AddrV2>::zcash_deserialize_from_slice(&mut payload.as_slice()).is_err());
    }
}

#[test]
fn version_address_decodes_the_same_from_a_slice() {
    let _init_guard = zakura_test::init();

    let addr = AddrInVersion::new(
        "[2001:db8::1]:8233"
            .parse::<std::net::SocketAddr>()
            .unwrap(),
        PeerServices::NODE_NETWORK,
    );
    let payload = addr.zcash_serialize_to_vec().unwrap();
    assert_eq!(decode_both_ways::<AddrInVersion>(&payload), addr);
}

#[test]
fn oversized_counts_are_rejected_before_allocating() {
    let _init_guard = zakura_test::init();

    let inv = InventoryHash::Block(block::Hash([0; 32]))
        .zcash_serialize_to_vec()
        .unwrap();
    assert_rejects_oversized_count::<InventoryHash>(&inv);

    let addr_v1 = zakura_test::network_addr::ADDR_V1_IP_VECTORS[0][1..].to_vec();
    assert_rejects_oversized_count::<AddrV1>(&addr_v1);

    assert_rejects_oversized_count::<AddrV2>(&UNSUPPORTED_ADDR_V2);
}
